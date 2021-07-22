import sys
import os
import pickle
from datetime import datetime
import pandas as pd
import logging
from collections import namedtuple
from multiprocessing import Lock

from .model_generic import L7LatencyModel

from .globals import APP_NAME, jobs
from . import alert_api
from . import last_timestamp
from . import self_diagnostics

logger = logging.getLogger(APP_NAME)


Params = namedtuple('Params',
                    ['PH_train_start_time',
                     'PH_train_end_time',
                     'PH_search_start_time',
                     'PH_search_end_time',
                     'PH_send_alerts',
                     'PH_max_docs',
                     ])
params = Params(
    os.getenv('PH_train_start_time', None),
    os.getenv('PH_train_end_time', None),
    os.getenv('PH_search_start_time', None),
    os.getenv('PH_search_end_time', None),
    eval(os.getenv('PH_send_alerts', 'True')),
    int(os.getenv('PH_max_docs', 100000000)),
)


model_dir = "./models"
data_dir = './data'

local_jobs = jobs()
job_name2class_name = {job.name: job.model_name for job in local_jobs}
job_name2job = {job.name: job for job in local_jobs}
dynamic_jobs = {job.name for job in local_jobs if job.dynamic_model}


def _save_model(model, aggregators, model_name):
    """
    Save a model and an aggregator in any format.
    Save them even when model or/and aggregators is None.
    Aggregator is used mostly to store average values from the training data to present them in alerts.
    We store model and aggregators together because they both use the training data.
    """
    file_name = f'{model_dir}/{model_name}.model'
    with open(file_name, 'wb') as f:
        out = {'model': model, 'aggregators': aggregators}
        pickle.dump(out, f, pickle.HIGHEST_PROTOCOL)
        logger.info(f'    Model {model_name} saved into "{file_name}"')


def _load_model(model_name):
    """
    Restore a model and aggregator from a local disc.
    """
    file_name = f'{model_dir}/{model_name}.model'
    if os.path.isfile(file_name):
        with open(file_name, 'rb') as f:
            out = pickle.load(f)
            logger.info(f'    Model {model_name} loaded from "{file_name}"')
            return out['model'], out['aggregators']
    else:
        logger.info(f'   Model {model_name} was not created as a "{file_name}" file.')
        return None, None


def _save_data(dct_lst, name, timestamp=True):
    """
    dct_lst: list of dictionaries.
    By default the file name presented with a timestamp, so we do not rewrite the existed file.
    If no data for saving, remove existed file. It needed for the self-diagnostics report. If we do not remove file,
    the previous file data goes into the self-diagnostics report.
    """
    suffix = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    file = f'{data_dir}/{name}.{suffix}.csv' if timestamp else f'{data_dir}/{name}.csv'
    if not dct_lst:
        try:
            os.remove(file)
        except OSError:
            pass
        return
    pd.DataFrame(dct_lst).to_csv(file, index=False)
    logger.info(f'Saved {len(dct_lst):,} into "{file}"')
    return


def aggregate_byte_anomalies(anomalies):
    """
    Aggregate the 'process_bytes' anomalies with the 'bytes_out', 'bytes_in' anomalies in the same
    time intervals.
    Leave only anomalies in the intersections of 'process_bytes' and 'bytes_in',
    and 'process_bytes' and 'bytes_out' anomalies.
    Alert description keep the 'bytes_out' or 'bytes_in' log records.

    anomalies:: a list of dictionaries. anomaly['record'] can be a string not a dictionary.
    """
    def timestamp_from_datetime_str(rec, field):
        return datetime.strptime(rec[field], "%Y-%m-%d %H:%M:%S").timestamp()

    def element_in_interval(rec, tried_dict):
        for k, v in tried_dict.items():
            if timestamp_from_datetime_str(rec, 'start_time') <= k < timestamp_from_datetime_str(rec, 'end_time'):
                return v
        return None

    # get anomalies for 'process_bytes' indexed by 'start_time'
    process_bytes_aa = {}
    for a in anomalies:
        job_name = a['alert'].split('.')[1]
        rec = eval(a['record']) if type(a['record']) == str else a['record']
        if job_name == 'process_bytes':
            process_bytes_aa[rec['start_time']] = rec

    # leave only anomalies found in the intersections of "process_bytes" and "bytes_in",
    #   "process_bytes" and "bytes_out"
    # ['bytes_out', 'bytes_in'] anomalies replaced by the 'process_bytes' anomalies.
    updated_anomalies = []
    for a in anomalies:
        rec = eval(a['record']) if type(a['record']) == str else a['record']
        job_name = a['alert'].split('.')[1]
        if job_name in ['bytes_out', 'bytes_in']:  # replace by 'process_bytes' record
            start_ts = timestamp_from_datetime_str(rec, 'start_time')
            new_record = element_in_interval(rec, process_bytes_aa)
            if new_record:
                if job_name == 'bytes_in':
                    a['description'] = f"[anomaly_detection.{job_name}] {new_record['dest_namespace']}/{new_record['dest_service_name']} has a suspicious input of {int(new_record['bytes_in']):,} bytes with {new_record['confidence']} confidence."
                else:
                    a['description'] = f"[anomaly_detection.{job_name}] {new_record['source_namespace']}/{new_record['source_name_aggr']} has a suspicious output of {int(new_record['bytes_out']):,} bytes with {new_record['confidence']} confidence."
                a['record'] = str(new_record) if type(a['record']) == str else new_record
                updated_anomalies.append(a)
        elif job_name == 'process_bytes':  # remove
            continue
        else:  # leave as it is
            updated_anomalies.append(a)
    return updated_anomalies


def train_job(job_name, samples, lock: Lock = None):
    try:
        # logger.info(f'  Start training {job_name} model, Data: {len(samples):,} samples; columns: {len(samples[0])} {list(samples[0])}')
        model_cls = ModelProcessor.str2class(job_name)()
        model, aggregators = model_cls.train(samples)
        if lock:
            with lock:
                _save_model(model, aggregators, job_name)
        else:
            _save_model(model, aggregators, job_name)
        if not model:
            logger.info(f'No model created for "{job_name}"')
        # logger.info(f'  Stop training {job_name} model.')
    except Exception as ex:
        sample = samples[0] if samples else {}
        msg = f'  *** Exception: "{str(ex)}". Model: {job_name}. Trained with: {len(samples):,} samples; columns: {len(sample)} {list(sample)}'
        logger.error(msg)


def detect(job_name, samples, lock: Lock = None):
    anomalies = []
    try:
        model, aggregators = None, None
        if job_name2job[job_name].dynamic_model:  # reload only dynamic models
            if lock:
                with lock:
                    model, aggregators = _load_model(job_name)
            else:
                model, aggregators = _load_model(job_name)
            if not model:
                logger.info(f'No model created for "{job_name}", so it cannot be used for detection.')
                return []

        model_cls = ModelProcessor.str2class(job_name)()
        anomalies = model_cls.find_anomalies(model, samples, aggregators)
    except Exception as ex:
        sample = samples[0] if samples else {}
        msg = f'  *** Exception: "{str(ex)}". Model: {job_name}. Detection with {len(samples):,} samples; columns: {len(sample)} {list(sample)}'
        logger.error(msg)
    return anomalies



class ModelProcessor():
    def __init__(self, es_client):
        self.es_client = es_client
        logger.info(f'Initialized ModelProcessor with params: {params}')

    def train(self, lock, i, is_test=False):
        """
        Train only dynamic models (models that are highly customer-dependent and time dependent).
        The static models, like the DGA model, do not retrained here.
        A new trained model replaces the existed model. A lock used to avoid collisions with reading a model file
        in the find_anomalies() of a different class instance.
        If the model training fails, we proceed with other models. It is bad but not a critical.
        is_test flag is set up for the self-diagnostic mode when we load data not from the ES but from the prepared files.
        i parameter is used to show the training cycle number. It shows how long the detection works without restarts.
        """
        static_jobs = [j for j in local_jobs if not j.dynamic_model]
        dynamic_jobs = [j for j in local_jobs if j.dynamic_model]
        logger.info(f'START {i:,} training {len(dynamic_jobs)} models {[j.model_name for j in dynamic_jobs]}. '
                    f'Static models {[j.model_name for j in static_jobs]} not retrained here.')
        all_samples = self._load_train_data(is_test)
        if not any(all_samples.values()):
            logger.info(f'* STOP {i:,} training {len(dynamic_jobs)} models. No samples - No training :(')
            return
        for job in dynamic_jobs:
            samples = all_samples[job.name if is_test else job.data_type]
            train_job(job.name, samples, lock)
        logger.info(f'STOP {i:,} training {len(dynamic_jobs)} models.')
        return

    def find_anomalies(self, lock, i, is_test):
        """
        Detects anomalies.
        A lock used to avoid collisions with writing a model file in the train() of a different class instance.
        If the detection fails, we proceed with other models. It is bad but not a critical.
        is_test flag is set up for the self-diagnostic mode when we load data not from the ES but from the prepared files.
        i parameter is used to show the detection cycle number. It shows how long the detection works without restarts.
        if is_test==True or i==0, the anomalies are saved in files for analysis.
        Anomalies also sent as alerts (can be turned off if PH_send_alerts is False).
        """
        logger.info(f'START {i:,} searching anomalies with {len(local_jobs)} models.')
        ts = datetime.utcnow()
        if i == 0: # clean up the last timestamp in the first cycle
            last_timestamp.remove()
        all_anomalies = []
        all_samples = self._load_find_anomalies_data(is_test)

        if any(all_samples.values()):
            for job in local_jobs:
                samples = all_samples[job.name if is_test else job.data_type]
                if not samples:
                    logger.info(f'* STOP {i:,} searching anomalies with {job.model_name} model. No samples - No searching :(')
                    continue
                all_anomalies += detect(job.name, samples, lock)
            all_anomalies = aggregate_byte_anomalies(all_anomalies)
        else:
            logger.info(f'* STOP {i:,} searching anomalies with {len(local_jobs)} models. No samples - No searching :(')

        last_timestamp.save(ts)

        if is_test:
            _save_data(all_anomalies, self_diagnostics.file_all_detected_anomalies_test, timestamp=False)
        else:  # if params.PH_debug or (i == 0 and not is_test):
            _save_data(all_anomalies, self_diagnostics.file_all_detected_anomalies, timestamp=False)
        if all_anomalies and params.PH_send_alerts and i:
            alert_client = alert_api.AlertClient()
            alert_client.send_alerts(all_anomalies)
        logger.info(f'STOP {i:,} searching anomalies with {len(local_jobs)} models.')
        return all_anomalies

    def _load_train_data(self, is_test):
        """
        The output dictionary key is Job.name if is_test else Job.data_type
        """
        if is_test:
            return self_diagnostics.load_train_data()
        else:
            return self.es_client.download_and_aggregate_data(start_time=params.PH_train_start_time,
                                                              end_time=params.PH_train_end_time,
                                                              max_docs=params.PH_max_docs)

    def _load_find_anomalies_data(self, is_test):
        """
        The output dictionary key is Job.name if is_test else Job.data_type
        """
        if is_test:
            return self_diagnostics.load_find_anomalies_data()
        else:
            start_time = params.PH_search_start_time if params.PH_search_start_time else last_timestamp.load()
            return self.es_client.download_and_aggregate_data(start_time=start_time,
                                                              end_time=params.PH_search_end_time,
                                                              max_docs=params.PH_max_docs)

    def str2class(job_name):
        """
        It is a class function.
        Returns a class name symbol.
        """
        return getattr(sys.modules[__name__], job_name2class_name[job_name])
