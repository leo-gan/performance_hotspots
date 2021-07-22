# from bottle import run, get, post, request, BaseRequest
# import json
# import argparse
import datetime
from multiprocessing import Process, Lock
import time
import os
from collections import namedtuple

from . import model_processor
from . import elastic_api

from .globals import APP_NAME

import logging

from .self_diagnostics import output_self_diagnostics_report

logging.basicConfig(level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S",
                    format='%(asctime)s : %(levelname)s : %(message)s')
logger = logging.getLogger(APP_NAME)


def validate_env_variables():
    """
    It validates all environment variables up front. So we don't get any missed variables in the end of the
    long-running process.
    It also keeps all environment variables in one place easy to review.
    It only validates if the required variable presented or not and validates a type of value. Full validation should
    be implemented on the code where this variable used.
    """

    def val_ev(var_name, var_type=None):
        if not os.environ.get(var_name):
            missed_vars.append(var_name)
        elif var_type:
            v = os.environ.get(var_name)
            try:
                var_type(v)
            except ValueError as ex:
                wrong_type_vars.append((var_name, v, var_type.__name__))
        return

    required_typed_vars = [
        # # elastic_api.py:
        # ('ELASTIC_PORT', int)

    ]
    required_vars = [  # with str type
        # elastic_api.py:
        'ELASTIC_USER',
        'ELASTIC_PASSWORD',
    ]
    missed_vars, wrong_type_vars = [], []
    _ = [val_ev(v, None) for v in required_vars]
    _ = [val_ev(v, t) for v, t in required_typed_vars]
    if missed_vars:
        logger.error(f'Missing Environment variables: {", ".join(missed_vars)}')
    if wrong_type_vars:
        msg = ', '.join(f'"{n}"="{v}" should be {t}' for n, v, t in wrong_type_vars)
        logger.error(f'Environment variables with wrong types: {msg}')
    if any([missed_vars, wrong_type_vars]):
        raise Exception('*** Cannot proceed with wrong parameters!')
    return


def train(lock, i, is_test=False):
    es_client = elastic_api.ElasticClient()
    model_proc = model_processor.ModelProcessor(es_client)
    model_proc.train(lock, i, is_test=is_test)


def find_anomalies(lock, i, is_test=False):
    es_client = elastic_api.ElasticClient()
    model_proc = model_processor.ModelProcessor(es_client)
    model_proc.find_anomalies(lock, i, is_test=is_test)


def self_diagnostics(lock, id=0):
    exc = None
    # the first self-diagnostics train+detect cycle on the prepared tests datasets
    train(lock, 0, is_test=True)
    find_anomalies(lock, 0, is_test=True)

    # the second self-diagnostics train+detect cycle on the real-life datasets
    try:
        train(lock, 0)
        find_anomalies(lock, 0)
    except ConnectionError as ex:
        # exception must happen when we run self-diagnostics without connection to the ES.
        # it is OK. We just write this exception in the self-diagnostics report
        exc = ex
    output_self_diagnostics_report(exc, id)


def start():
    """
    It is the main cycle. It starts two processes (train and detection), each with different cycle intervals.
    The train is less frequent (daily) and the detection is more frequent.
    The first two combined cycles (train+detection) represent the Self-Diagnostics (SD).
    The first SD cycle runs on the prepared datasets with known anomalies.
    The second SD cycle runs on the real data from ES with unknown anomalies.
    The SD is successful if the first cycle detects most of the known anomalies and
    if the second cycle finishes without an exception.
    """
    Params = namedtuple('Params', 'PH_train_interval_minutes PH_search_interval_minutes')
    params = Params(
        int(os.getenv('PH_train_interval_minutes', 1440)),
        int(os.getenv('PH_search_interval_minutes', 30)),
    )
    logger.info('Initialized params for the main.py: ' + ', '.join(
        [f'{n}: {el}' for el, n in zip(params, params._fields)]))
    logger.info(f'START: {APP_NAME}')

    validate_env_variables()

    lock = Lock()
    self_diagnostics(lock)
    n, m = 0, 0
    while True:
        cur_minute = int(int(datetime.datetime.utcnow().timestamp()) / 60)
        logger.info(f'Sleep cycle:: Detection {(cur_minute % params.PH_search_interval_minutes)+1:,}/{params.PH_search_interval_minutes:,}, '
                    f'Training {(cur_minute % params.PH_train_interval_minutes)+1:,}/{params.PH_train_interval_minutes:,}')
        if cur_minute % params.PH_train_interval_minutes == 0:
            n += 1
            Process(target=train, args=(lock, n)).start()
        if cur_minute % params.PH_search_interval_minutes == 0:
            m += 1
            Process(target=find_anomalies, args=(lock, m)).start()
        time.sleep(60)
    logger.info(f'STOP: {SERVICE_NAME}')