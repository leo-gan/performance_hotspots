import pytest
import os
import pandas as pd
from sklearn.ensemble import IsolationForest

from ph.model_l7_latency import L7LatencyModel

# NOTE: run tests not from the current directory but from the test/ directory!
model_pattern = "../models/*.model"
data_dir = './data'
job_name = 'l7_latency'
verbose = False


@pytest.fixture
def model_instance():
    ret = L7LatencyModel()
    if verbose: print(ret)
    return ret


@pytest.fixture
def samples():
    file_name = f'{data_dir}/{job_name}.test_dataset.csv'
    assert os.path.exists(file_name)
    return pd.read_csv(file_name, usecols=lambda col: col != 'anomaly', low_memory=False).to_dict('records')


@pytest.fixture
def model_and_aggregators(model_instance, samples):
    assert samples
    model, aggregators = model_instance.train(samples)
    assert model
    assert aggregators
    return {'model': model, 'aggregators': aggregators}


def test_train(model_instance, samples):
    models, aggregators = model_instance.train(samples)
    assert type(model_instance.model) == IsolationForest


def test_detection(model_instance, model_and_aggregators, samples):
    """
    Test the detected anomaly format.
    """
    anomaly_alerts = model_instance.find_anomalies(model_and_aggregators['model'], samples, model_and_aggregators['aggregators'])
    assert anomaly_alerts
    assert type(anomaly_alerts) == list
    for alert in anomaly_alerts:
        assert type(alert) == dict
        assert set(alert.keys()) == {'alert', 'description', 'record', 'severity', 'time', 'type'}
        assert alert['type'] == 'alert'
        assert alert['alert'] == f"anomaly_detection.{job_name}"
        assert type(alert['severity']) == int
        assert type(alert['record']) == dict
        assert type(alert['description']) == str
        assert type(alert['time']) == int
        assert model_instance.value_field in alert['record'].keys()
