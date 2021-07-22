import glob
import logging
import os
import random
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

from ph.globals import data_dir, model_dir
from ph.globals import jobs, job_name2job

logger = logging.getLogger('performance_hotspots_fv_test')

local_jobs = jobs()
model_file_pattern = f"{model_dir}/*.model"


def run_all_tests(base_domain='http://localhost', port=8000):
    run_ping(base_domain=base_domain, port=port)

    # train models. Models can be not created at this time.
    run_train_models(base_domain=base_domain, port=port)

    job2samples = read_job2samples()

    # Train endpoints
    run_train_defaults(base_domain=base_domain, port=port)
    run_train_model_with_data_from_request(base_domain=base_domain, port=port, job2samples=job2samples)
    run_train_model_with_test_datasets(base_domain=base_domain, port=port)
    # TODO uncomment when fv works with ES
    # run_train_model_with_logs(base_domain=base_domain, port=port)
    # run_train_all_models_with_logs(base_domain=base_domain, port=port)

    # Detect endpoints
    run_detect_defaults(base_domain=base_domain, port=port)
    run_detect_model_with_data_from_request(base_domain=base_domain, port=port, job2samples=job2samples)
    run_detect_model_with_test_datasets(base_domain=base_domain, port=port)
    # TODO uncomment when fv works with ES
    # run_detect_model_with_logs(base_domain=base_domain, port=port)
    # run_detect_all_models_with_logs(base_domain=base_domain, port=port)

    # Self-Diagnostics endpoints
    run_start_self_diagnostics(base_domain=base_domain, port=port)
    run_get_self_diagnostics_result(base_domain=base_domain, port=port)

    # Configuration endpoints
    run_get_parameters(base_domain=base_domain, port=port)
    run_get_all_parameters(base_domain=base_domain, port=port)
    run_set_parameters(base_domain=base_domain, port=port)


def read_job2samples():
    job2samples = {}
    for job_name in [j.name for j in local_jobs] + ['flows']:
        file_name = f'{data_dir}/{job_name}.test_dataset.csv'
        assert os.path.exists(file_name)
        job2samples[job_name] = pd.read_csv(file_name, usecols=lambda col: col != 'anomaly', low_memory=False).to_dict(
            'records')
        logger.info(f'Loaded {job_name} data {len(job2samples[job_name]):,} from "{file_name}"')
    return job2samples


def run_ping(base_domain, port):
    ts = datetime.utcnow()
    url = f'{base_domain}:{port}/ph/ping'
    response = requests.get(url)
    logger.info(f'{response.url} {response.status_code} {response.text}')
    assert response.status_code == 200
    rs = response.json()
    assert rs['service'] == "performance_hotspots_service"
    ts_svc = datetime.strptime(rs['utcnow'], "%Y-%m-%dT%H:%M:%S.%f")

    assert abs(ts.second - ts_svc.second) < 2


# region OPERATIONS:

# region OPERATIONS: Train:


def run_train_models(base_domain, port):
    """
    Creates models if they where removed by some test.
    """
    # check availability of the new model files: (only dynamic models)
    dynamic_models = [j for j in local_jobs if j.dynamic_model]
    dynamic_models_num = len(dynamic_models)
    delay_sec = 5
    retry = 10
    models_num = len(glob.glob(model_file_pattern))
    logger.info(f"{models_num} dynamic models exist. We need {dynamic_models_num} models for detection.")

    if dynamic_models_num != models_num:
        # recreate all models:
        for job in dynamic_models:
            rq = {
                "job": job.name,
                "data_source": "test_dataset"
            }
            url = f'{base_domain}:{port}/ph/ops/train'
            response = requests.post(url, json=rq)
            logger.info(f'{response.url} {response.status_code} {response.text}')
            if job.dynamic_model:
                assert response.status_code == 202
                assert job.name in response.text

        for i in range(retry):
            logger.info(
                f'{i + 1}/{retry} sleeps. {models_num} models created. We need to recreate {dynamic_models_num}'
                f' models. Wait {delay_sec} seconds.')
            time.sleep(delay_sec)
            models_num = len(glob.glob(model_file_pattern))
            if dynamic_models_num == models_num:
                logger.info(f' {models_num} models created. Everything is OK.')
                break
    assert dynamic_models_num == models_num
    logger.info(f"{dynamic_models_num} dynamic models ready for detection.")
    return 1


def run_train_defaults(base_domain, port):
    # it will connect to the Elasticsearch!
    start = (datetime.utcnow() - timedelta(days=2)).isoformat() + 'Z'
    end = datetime.utcnow().isoformat() + 'Z'
    job = local_jobs[0]
    # TODO uncomment when fv works with ES
    rqs = [
        {
            "job": job.name,
            "data_source": "test_dataset"
        },
        {
            "job": job.name,
            "data_source": "test_dataset",
            "max_log_records": 1000
        },

        # {
        #     "job": job.name,
        #     "data_source": "logs",
        #     "data": {
        #         "start": start,
        #     },
        #     "max_log_records": 1000
        # },
        # {
        #     "job": job.name,
        #     "data_source": "logs",
        #     "data": {
        #         "end": end
        #     },
        #     "max_log_records": 1000
        # },
    ]

    for rq in rqs:
        logger.info(rq)
        url = f'{base_domain}:{port}/ph/ops/train'
        response = requests.post(url, json=rq)
        logger.info(f'{response.url} {response.status_code} {response.text}')
        _assert_train_rs(job, response)


def run_train_model_with_data_from_request(base_domain, port, job2samples, num_samples=2000):
    """
    The requests are filled in with data from the .test_dataset.csv files.
    Some .test_dataset.csv holds the aggregated data (not the log original data)
    If a job require 'source' or 'dest' data_type, we should use the 'flows' log records in the request.
    Here I use a special the flows.test_dataset.csv for such 'flows' record.
    """
    # use num_samples to limit the request size
    _remove_model_files()

    dynamic_jobs = {job.name for job in local_jobs if job.dynamic_model}
    for job in local_jobs:
        if job.data_type in ['source', 'dest']:
            log_name = 'flows'
            records = job2samples['flows'][:num_samples]
        else:
            log_name = job.data_type
            records = job2samples[job.name][:num_samples]
        rq = {
            "job": job.name,
            "data_source": "request",
            "data": {
                "log_name": log_name,
                "records": records
            }
        }
        # bytes_out requires a full dataset. bytes_out model is not created for small datasets.
        if num_samples and job.name != 'bytes_out':
            rq["max_log_records"] = num_samples
        url = f'{base_domain}:{port}/ph/ops/train'
        response = requests.post(url, json=rq)
        logger.info(f'{response.url} {response.status_code} {response.text}')
        _assert_train_rs(job, response)

    _are_model_files_available()


def run_train_model_with_test_datasets(base_domain, port, num_samples=2000):
    # use num_samples to limit the request size
    _remove_model_files()

    for job in local_jobs:
        rq = {
            "job": job.name,
            "data_source": "test_dataset"
        }
        # bytes_out requires a full dataset. bytes_out model is not created for small datasets.
        if num_samples and job.name != 'bytes_out':
            rq["max_log_records"] = num_samples
        url = f'{base_domain}:{port}/ph/ops/train'
        response = requests.post(url, json=rq)
        logger.info(f'{response.url} {response.status_code} {response.text}')
        _assert_train_rs(job, response)

    _are_model_files_available()


def run_train_model_with_logs(base_domain, port, num_samples=2000):
    # use num_samples to limit the request size
    _remove_model_files()

    dynamic_jobs = {job.name for job in local_jobs if job.dynamic_model}
    for job in local_jobs:
        rq = {
            "job": job.name,
            "data_source": "logs"
        }
        # bytes_out requires a full dataset. bytes_out model is not created for small datasets.
        if num_samples and job.name != 'bytes_out':
            rq["max_log_records"] = num_samples
        url = f'{base_domain}:{port}/ph/ops/train'
        response = requests.post(url, json=rq)
        logger.info(f'{response.url} {response.status_code} {response.text}')
        _assert_train_rs(job, response)

    _are_model_files_available()


def run_train_all_models_with_logs(base_domain, port, num_samples=2000):
    # use num_samples to limit the request size
    _remove_model_files()

    rq = {
        "job": "all",
        "data_source": "logs",
        "max_log_records": num_samples
    }
    url = f'{base_domain}:{port}/ph/ops/train'
    response = requests.post(url, json=rq)
    logger.info(f'{response.url} {response.status_code} {response.text}')
    assert response.status_code == 202
    assert "STOP training all models.  Retrained models will replace the old models." in response.text

    _are_model_files_available()


# endregion OPERATIONS: Train:

# region OPERATIONS: Detect:

def run_detect_defaults(base_domain, port):
    # it will connect to the Elasticsearch!
    start = (datetime.utcnow() - timedelta(days=2)).isoformat() + 'Z'
    end = datetime.utcnow().isoformat() + 'Z'
    job = local_jobs[0]
    # TODO uncomment when fv works with ES
    rqs = [
        {
            "job": job.name,
            "data_source": "test_dataset"
        },
        {
            "job": job.name,
            "data_source": "test_dataset",
            "max_log_records": 1000
        },
        # {
        #     "job": job.name,
        #     "data_source": "logs",
        #     "data": {
        #         "start": start,
        #     },
        #     "max_log_records": 1000
        # },
        # {
        #     "job": job.name,
        #     "data_source": "logs",
        #     "data": {
        #         "end": end
        #     },
        #     "max_log_records": 1000
        # },
    ]
    for rq in rqs:
        url = f'{base_domain}:{port}/ph/ops/detect'
        response = requests.post(url, json=rq)
        logger.info(f'{job.name}  {response.url} {response.status_code} {rq}')
        anomalies = response.json()
        logger.info(f"  {len(anomalies)} 0: {anomalies[0]}" if anomalies else "  No Anomalies")
        assert response.status_code == 200
        _assert_anomalies(anomalies, job)


def run_detect_model_with_data_from_request(base_domain, port, job2samples, num_samples=2000):
    """
    The requests are filled in with data from the .test_dataset.csv files.
    Some .test_dataset.csv holds the aggregated data (not the log original data)
    If a job require 'source' or 'dest' data_type, we should use the 'flows' log records in the request.
    Here I use a special the flows.test_dataset.csv for such 'flows' record.
    """
    for job in local_jobs:
        if job.data_type in ['source', 'dest']:
            log_name = 'flows'
            records = job2samples['flows'][:num_samples]
        else:
            log_name = job.data_type
            records = job2samples[job.name][:num_samples]
        rq = {
            "job": job.name,
            "data_source": "request",
            "data": {
                "log_name": log_name,
                "records": records
            }
        }

        url = f'{base_domain}:{port}/ph/ops/detect'
        response = requests.post(url, json=rq)
        rq['data']['records'] = '...'  # for logging
        logger.info(f'{job.name}  {response.url} {response.status_code} {rq}')
        anomalies = response.json()
        logger.info(f"{job.name}  {len(anomalies)} 0: {anomalies[0]}" if anomalies else "  No Anomalies")
        assert response.status_code == 200
        _assert_anomalies(anomalies, job)


def run_detect_model_with_test_datasets(base_domain, port, num_samples=3000):
    # use num_samples to limit the request size. Laptop with 16GB works with num_samples=3000
    for job in local_jobs:
        rq = {
            "job": job.name,
            "data_source": "test_dataset",
            "max_log_records": num_samples
        }
        url = f'{base_domain}:{port}/ph/ops/detect'
        response = requests.post(url, json=rq)
        logger.info(f'{job.name}  {response.url} {response.status_code} {rq}')
        anomalies = response.json()
        logger.info(f"{job.name}  {len(anomalies)} 0: {anomalies[0]}" if anomalies else "  No Anomalies")
        assert response.status_code == 200
        _assert_anomalies(anomalies, job)


def run_detect_dga_model_with_test_datasets(base_domain, port, num_samples=3000):
    # use num_samples to limit the request size. Laptop with 16GB works with num_samples=3000
    for job in [j for j in local_jobs if j.name == 'dga']:
        rq = {
            "job": job.name,
            "data_source": "test_dataset",
            "max_log_records": num_samples
        }
        url = f'{base_domain}:{port}/ph/ops/detect'
        response = requests.post(url, json=rq)
        logger.info(f'{job.name}  {response.url} {response.status_code} {rq}')
        anomalies = response.json()
        logger.info(f"{job.name}  {len(anomalies)} 0: {anomalies[0]}" if anomalies else "  No Anomalies")
        assert response.status_code == 200
        _assert_anomalies(anomalies, job)


def run_detect_model_with_logs(base_domain, port, num_samples=20000):
    for job in local_jobs:
        rq = {
            "job": job.name,
            "data_source": "logs",
            "max_log_records": num_samples
        }
        url = f'{base_domain}:{port}/ph/ops/detect'
        response = requests.post(url, json=rq)
        logger.info(f'{job.name}  {response.url} {response.status_code} {rq}')
        anomalies = response.json()
        logger.info(f"{job.name}  {len(anomalies)} 0: {anomalies[0]}" if anomalies else "  No Anomalies")
        assert response.status_code == 200
        _assert_anomalies(anomalies, job)


def run_detect_all_models_with_logs(base_domain, port, num_samples=2000):
    rq = {
        "job": "all",
        "data_source": "logs",
        "max_log_records": num_samples
    }
    url = f'{base_domain}:{port}/ph/ops/detect'
    response = requests.post(url, json=rq)
    logger.info(f'all  {response.url} {response.status_code} {rq}')
    anomalies = response.json()
    logger.info(f"  {len(anomalies)} 0: {anomalies[0]}" if anomalies else "  No Anomalies")
    assert response.status_code == 200
    _assert_anomalies(anomalies, job=None)


def _assert_anomalies(anomalies, job):
    if anomalies:
        assert type(anomalies) == list
        assert type(anomalies[0]) == dict
        el = anomalies[0]
        for k in el.keys():
            # assert k in ['type', 'alert', 'severity', 'record', 'description', 'time'] # for alerts
            assert k in ['job', 'description', 'time', 'data']
            if job:
                assert el['job'] == job.name
            if 'record' in el:
                assert type(el['data']) == dict


# endregion OPERATIONS: Detect:

# region OPERATIONS: Self-diagnostics
def run_start_self_diagnostics(base_domain, port, num_samples=2000):
    """
    It just starts the SD but not verifies the outcome.
    """
    url = f'{base_domain}:{port}/ph/ops/start_self_diagnostics'
    response = requests.get(url)
    logger.info(f'{response.url} {response.status_code} {response.text}')
    assert response.status_code == 202
    assert len(response.text) >= 36  # 38??? uuid: "4b28397b-16d1-4036-a6ff-0b685fdac9c8"


def run_get_self_diagnostics_result(base_domain, port, num_samples=2000):
    """
    It does not start the SD but verifies the outcome, means the SD should run before this test at last once.
    Test id: -1, uuid (gets from the first test!), something wrong.
    """
    # get the last result
    id = -1
    url = f'{base_domain}:{port}/ph/ops/get_self_diagnostics_result/{id}'
    response = requests.get(url)
    logger.info(f'{response.url} {response.status_code} {response.json().keys()}')
    assert response.status_code == 200
    rs = response.json()
    assert type(rs) == dict
    assert ['id', 'time', 'job', 'result'] == list(rs.keys())
    assert 'result' in rs['result']
    assert rs['result']['result'] in ['Success', 'Failure']
    logger.info(rs['id'])
    last_record_id = rs['id']

    id = last_record_id
    url = f'{base_domain}:{port}/ph/ops/get_self_diagnostics_result/{id}'
    response = requests.get(url)
    logger.info(f'{response.url} {response.status_code} {response.json().keys()}')
    assert response.status_code == 200
    rs = response.json()
    assert type(rs) == dict
    assert ['id', 'time', 'job', 'result'] == list(rs.keys())
    assert 'result' in rs['result']
    assert rs['result']['result'] in ['Success', 'Failure']
    logger.info(rs['id'])

    id = 'abracadabra'
    url = f'{base_domain}:{port}/ph/ops/get_self_diagnostics_result/{id}'
    response = requests.get(url)
    logger.info(f'{response.url} {response.status_code} {response.json().keys()}')
    assert response.status_code == 200
    rs = response.json()
    assert type(rs) == dict
    assert ['id', 'time', 'job', 'result'] == list(rs.keys())
    assert 'result' in rs['result']
    assert rs['result']['result'] in ['Success', 'Failure']
    logger.info(rs['id'])
    assert rs['id'] == last_record_id


# endregion OPERATIONS: Self-diagnostics

# endregion OPERATIONS:


# region CONFIGURATION


def run_get_parameters(base_domain, port):
    # with job_name
    for job in local_jobs:
        url = f'{base_domain}:{port}/ph/conf/get_parameters/{job.name}'
        rs = requests.get(url)
        logger.info(f"{rs.url} {rs.status_code} {rs.json()}")
        assert rs.status_code == 200
        rs = rs.json()
        assert_job_params(job, rs)


def run_get_all_parameters(base_domain, port):
    # with job_name
    url = f'{base_domain}:{port}/ph/conf/get_parameters/all'
    rs = requests.get(url)
    logger.info(f"{rs.url} {rs.status_code} {rs.json()}")
    assert rs.status_code == 200
    rs = rs.json()
    assert type(rs) == list
    for job_rs in rs:
        assert job_rs['job'] in job_name2job
        assert_job_params(job_name2job[job_rs['job']], job_rs)


def run_set_parameters(base_domain, port):
    job = "port_scan"
    param = "AD_port_scan_threshold"

    # get the param. We restore it at the end
    url = f'{base_domain}:{port}/ph/conf/get_parameters/{job}'
    rs = requests.get(url)
    logger.info(f"{rs.url} {rs.status_code} {rs.json()}")
    assert rs.status_code == 200
    param_val_old = rs.json()['params'][param]  # type can be not str

    # set a new value
    param_val_new = random.randint(100, 700)
    rq = {
        "job": job,
        "params": {
            param: str(param_val_new)
        }
    }
    url = f'{base_domain}:{port}/ph/conf/set_parameters'
    rs = requests.put(url, json=rq)
    logger.info(f"{rs.url} {rs.status_code} {rs.json()}")
    assert rs.status_code == 200
    rs = rs.json()
    assert type(rs) == dict
    assert rq == rs

    # double check with get:
    url = f'{base_domain}:{port}/ph/conf/get_parameters/{job}'
    rs = requests.get(url)
    logger.info(f"{rs.url} {rs.status_code} {rs.json()}")
    assert rs.status_code == 200
    assert rq == rs.json()

    # restore old value:
    rq = {
        "job": job,
        "params": {
            param: str(param_val_old)
        }
    }
    url = f'{base_domain}:{port}/ph/conf/set_parameters'
    rs = requests.put(url, json=rq)
    logger.info(f"{rs.url} {rs.status_code} {rs.json()}")
    assert rs.status_code == 200
    rs = rs.json()
    assert type(rs) == dict
    assert rq == rs


def assert_job_params(job, rs):
    assert rs['job'] == job.name
    assert 'params' in rs
    assert len(rs['params']) == len(job.params)
    for par in job.params:
        assert par in rs['params']


# endregion CONFIGURATION


def _assert_train_rs(job, response):
    if job.dynamic_model:
        assert response.status_code == 202
        assert 'STOP training' in response.text
        assert job.name in response.text
        assert 'Retrained model will replace the old model.' in response.text
    else:
        assert response.status_code == 404
        # "* NO training of 'dga'. The model is static and we do not train it here."
        assert '* NO training of' in response.text
        assert job.name in response.text
        assert 'The model is static and we do not train it here.' in response.text


def _are_model_files_available():
    # check availability of the new model files: (only dynamic models)
    dynamic_models_num = len([j for j in local_jobs if j.dynamic_model])
    delay_sec = 5
    retry = 10
    models_num = len(glob.glob(model_file_pattern))
    if dynamic_models_num != models_num:
        for i in range(retry):
            logger.info(
                f'{i + 1}/{retry} sleeps. {models_num} models created. We need to recreate {dynamic_models_num}'
                f' models. Wait {delay_sec} seconds.')
            time.sleep(delay_sec)
            models_num = len(glob.glob(model_file_pattern))
            if dynamic_models_num == models_num:
                logger.info(f' {models_num} models created. Everything is OK.')
                break
    assert dynamic_models_num == models_num


def _remove_model_files():
    # remove the model files:
    _ = [os.remove(f) for f in glob.glob(model_file_pattern)]
    # successful removal of the model files
    assert not list(glob.glob(model_file_pattern))
    return model_file_pattern
