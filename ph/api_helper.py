import os
import logging

import pandas as pd

from ph import elastic_api
from ph.api_classes import OperationDataRq, JobParams
from ph.globals import data_dir, APP_NAME

logger = logging.getLogger(APP_NAME)


def prepare_samples(rq: OperationDataRq, local_jobs):
    """
    samples format: {'flows': [], 'source': [], 'dest': [], 'l7': [], 'dns': []}
    Any key_value element can be missed.
    """
    if rq.max_log_records:
        max_docs = rq.max_log_records
    else:
        max_docs = int(os.getenv('PH_max_docs', 500000))
    job_name = rq.job.name
    data_type = [j.data_type for j in local_jobs if j.name == job_name][0]  # we assume the list always has one element
    source_log = [j.source_log for j in local_jobs if j.name == job_name][0]  # we assume the list always has one element
    data_type2samples = {}
    if rq.data_source.name == 'test_dataset':
        file_name = f'{data_dir}/{job_name}.test_dataset.csv'
        assert os.path.exists(file_name)
        data_type2samples[data_type] = pd.read_csv(file_name, usecols=lambda col: col != 'anomaly',
                                                   low_memory=False).to_dict('records')[:max_docs]
        logger.info(
            f'Loaded "{job_name}" data {len(data_type2samples[data_type]):,} from "{file_name}" for "{data_type}" data_type')
    elif rq.data_source.name == 'logs':
        es_client = elastic_api.ElasticClient()
        start_time, end_time = None, None
        if rq.data:
            start_time, end_time = rq.data.start, rq.data.end
        data_type2samples = es_client.download_and_aggregate_data(start_time=start_time,
                                                                  end_time=end_time,
                                                                  max_docs=max_docs,
                                                                  index_name=source_log
                                                                  )
    elif rq.data_source.name == 'request':
        if source_log != rq.data.log_name.name:
            msg = f"*** Error: {job_name} required '{source_log}' data.log_name field in the request but {rq.data.log_name.name} presented."
            logger.exception(msg)
            raise ValueError(msg)
        if data_type in ['source', 'dest']:
            data_type2samples = elastic_api.aggregate_samples(rq.data.records)
        else:
            data_type2samples = {data_type: rq.data.records}
        data_type2samples = {data_type: samples[:max_docs] for data_type, samples in data_type2samples.items()}
    return data_type2samples


def prepare_all_samples(rq: OperationDataRq, local_jobs):
    """
    Called for the 'job': 'all', 'data_source': 'logs'
    samples format: {'flows': [], 'source': [], 'dest': [], 'l7': [], 'dns': []}
    Any key_value element can be missed.
    """
    assert rq.job.name == 'all'
    assert rq.data_source.name == 'logs'

    if rq.max_log_records:
        max_docs = rq.max_log_records
    else:
        max_docs = int(os.getenv('PH_max_docs', 500000))
    start_time, end_time = None, None
    if rq.data:
        start_time, end_time = rq.data.start, rq.data.end

    es_client = elastic_api.ElasticClient()
    data_type2samples = es_client.download_and_aggregate_data(start_time=start_time,
                                                              end_time=end_time,
                                                              max_docs=max_docs
                                                              )
    return data_type2samples


def set_envvars(rq: JobParams):
    """
    It sets up the environment variables. All environment variable values are strings.

    If the env var does not exist, creates it.

    If the env var does exist, it does not validate the existed value but recreates it.
    It doesn't validate if the env var is related to the global params or to the jobs params.
    All this hierarchy (global or jobs params) is just for presentation. Internally, all env vars
    are in a single list.
    """
    for name, val in rq.params.items():
        os.environ[name] = str(val)
    return


def get_envvars(job_name, local_jobs):
    job2params = {job.name: job.params for job in local_jobs}
    rs = {}
    if not job_name or job_name == 'all' or job_name not in job2params:
        rs = [JobParams(job=job, params={param: os.getenv(param, val) for param, val in params.items()})
              for job, params in job2params.items()
              ]
    else:
        rs = JobParams(job=job_name, params={param: os.getenv(param, val)
                                             for param, val in job2params[job_name].items()})
    return rs


def format_alert_to_anomaly(alerts):
    """
    Reformat the alerts into anomalies. Get rid of the alert-specific info.
    Alert 'record' elements can hold NaN (nan), x != x finds them and replace with 0.
    """
    return [
        {
            "job": al["alert"].split('.')[1],
            "time": al['time'],
            "description": al["description"],
            "data": {k: v if v == v else 0 for k, v in al["record"].items()},
        }
        for al in alerts
    ]
