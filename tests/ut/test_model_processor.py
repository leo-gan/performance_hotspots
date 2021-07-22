import pytest
from pytest_mock import mocker

from multiprocessing import Lock
import glob
import os

from ph.model_processor import ModelProcessor

# NOTE: we have additional dependencies! See imports below.
from ph.elastic_api import ElasticClient
from ph.globals import jobs
from ph.self_diagnostics import data_dir, file_all_detected_anomalies_test, file_all_detected_anomalies

local_jobs = jobs()

# NOTE: run tests not from the current directory but from the test/ directory!
model_pattern = "./models/*.model"
verbose = False

@pytest.fixture
def es_client(mocker):
    return mocker.patch('ph.elastic_api.ElasticClient')

@pytest.fixture
def model_processor(es_client):
    ret = ModelProcessor(es_client)
    if verbose: print(ret)
    return ret


def test_train(model_processor):
    """
    Result of the model_processor.train() is a set of the models
    """
    if verbose: print('START test_model_processor_train()')

    # remove the model files:
    _ = [os.remove(f) for f in glob.glob(model_pattern)]

    # successful removal of the model files
    assert not list(glob.glob(model_pattern))

    # train new models:
    lock = Lock()  # the lock is irrelevant in the test context.
    i = 0
    is_test = True
    model_processor.train(lock, i, is_test=is_test)

    # check the availability of the new model files: (only dynamic models)
    assert len([j for j in local_jobs if j.dynamic_model]) == len(glob.glob(model_pattern))
    if verbose: print('FINISH test_model_processor_train()')


def test_detect_in_test_datasets(model_processor):
    """
    Result of the model_processor.find_anomalies() is the result of the Self Diagnostics
    cycle on the internal datasets.
    """
    if verbose: print('START test_model_processor_find_anomalies()')

    all_detected_anomalies_test_file_name = f'{data_dir}/{file_all_detected_anomalies_test}.csv'

    # all models trained and saved (only dynamic models)
    assert len([j for j in local_jobs if j.dynamic_model]) == len(glob.glob(model_pattern))

    # remove the previous anomalies files:
    if os.path.isfile(all_detected_anomalies_test_file_name):
        os.remove(all_detected_anomalies_test_file_name)
        print(f'Removed "{all_detected_anomalies_test_file_name}" file.')
    # anomaly file should be deleted:
    assert not os.path.isfile(all_detected_anomalies_test_file_name)

    # detect anomalies in the self diagnostics mode in the test cycle:
    lock = Lock()  # the lock is irrelevant in this context.
    i = 0
    is_test = True
    model_processor.find_anomalies(lock, i, is_test=is_test)

    # anomaly file should be created:
    assert os.path.isfile(all_detected_anomalies_test_file_name)

    if verbose: print('FINISH test_model_processor_find_anomalies()')



