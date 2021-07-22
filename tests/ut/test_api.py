import glob
import os
import time
from datetime import datetime, timedelta

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from ph.api import app
from ph.globals import data_dir, model_dir
from ph.globals import jobs

client = TestClient(app)

local_jobs = jobs()

@pytest.fixture
def job2samples():
    job2samples = {}
    for job_name in [j.name for j in local_jobs] + ['flows']:
        file_name = f'{data_dir}/{job_name}.test_dataset.csv'
        assert os.path.exists(file_name)
        job2samples[job_name] = pd.read_csv(file_name, usecols=lambda col: col != 'anomaly', low_memory=False).to_dict(
            'records')
        print(f'Loaded {job_name} data {len(job2samples[job_name]):,} from "{file_name}"')
    return job2samples


@pytest.fixture
def create_models(job2samples):
    """
    Creates models if they where removed by some test.
    """
    # check availability of the new model files: (only dynamic models)
    dynamic_models = [j for j in local_jobs if j.dynamic_model]
    dynamic_models_num = len(dynamic_models)
    delay_sec = 5
    retry = 10
    models_num = len(glob.glob(model_file_pattern))
    print(f"{models_num} dynamic models exist. We need {dynamic_models_num} models for detection.")

    if dynamic_models_num != models_num:
        # recreate all models:
        for job in dynamic_models:
            rq = {
                "job": job.name,
                "data_source": "test_dataset"
            }
            response = client.post("/ph/ops/train", json=rq)
            print(response.url, response.status_code, response.text)
            if job.dynamic_model:
                assert response.status_code == 202
                assert job.name in response.text

        for i in range(retry):
            print(
                f'{i + 1}/{retry} retries. {models_num} models created. We need to recreate {dynamic_models_num} models.')
            time.sleep(delay_sec)
            models_num = len(glob.glob(model_file_pattern))
            if dynamic_models_num == models_num:
                break
    assert dynamic_models_num == models_num
    print(f"{dynamic_models_num} dynamic models ready for detection.")
    return 1


def test_ping():
    ts = datetime.utcnow()
    response = client.get("/ph/ping")
    print(response.url, response.status_code, response.text)
    assert response.status_code == 200
    rs = response.json()
    assert rs['service'] == "performance_hotspots_service"
    ts_svc = datetime.strptime(rs['utcnow'].replace('Z', ''), "%Y-%m-%dT%H:%M:%S.%f")
    assert abs(ts.second - ts_svc.second) < 2


# region OPERATIONS:

# region OPERATIONS: Train:

# @pytest.mark.skip(reason="temporarily")
def test_train_defaults():
    # it will connect to the Elasticsearch!
    start = (datetime.utcnow() - timedelta(days=2)).isoformat() + 'Z'
    end = datetime.utcnow().isoformat() + 'Z'
    job = local_jobs[0]
    # TODO uncomment when ut works with ES
    requests = [
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

    for rq in requests:
        print(rq)
        response = client.post("/ph/ops/train", json=rq)
        print(response.url, response.status_code, response.text)
        _assert_train_rs(job, response)


# @pytest.mark.skip(reason="temporarily")
def test_train_model_with_data_from_request(job2samples, num_samples=2000):
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
        response = client.post("/ph/ops/train", json=rq)
        print(response.url, response.status_code, response.text)
        _assert_train_rs(job, response)

    _is_model_files_available()


# @pytest.mark.skip(reason="temporarily")
def test_train_model_with_test_datasets(num_samples=2000):
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
        response = client.post("/ph/ops/train", json=rq)
        print(response.url, response.status_code, response.text)
        _assert_train_rs(job, response)

    _is_model_files_available()


# TODO uncomment with ut works with ES
# def test_train_model_with_logs(num_samples=2000):
#     # use num_samples to limit the request size
#     _remove_model_files()
#
#     dynamic_jobs = {job.name for job in jobs if job.dynamic_model}
#     for job in jobs:
#         rq = {
#             "job": job.name,
#             "data_source": "logs"
#         }
#         # bytes_out requires a full dataset. bytes_out model is not created for small datasets.
#         if num_samples and job.name != 'bytes_out':
#             rq["max_log_records"] = num_samples
#         response = client.post("/ph/ops/train", json=rq)
#         print(response.url, response.status_code, response.text)
#         _assert_train_rs(job, response)
#
#     _is_model_files_available()


# TODO uncomment with ut works with ES
# def test_train_all_models_with_logs(num_samples=2000):
#     # use num_samples to limit the request size
#     _remove_model_files()
#
#     rq = {
#         "job": "all",
#         "data_source": "logs",
#         "max_log_records": num_samples
#     }
#     response = client.post("/ph/ops/train", json=rq)
#     print(response.url, response.status_code, response.text)
#     assert response.status_code == 202
#     assert "STOP training all models.  Retrained models will replace the old models." in response.text
#
#     _is_model_files_available()


def test_detect_defaults():
    # it will connect to the Elasticsearch!
    start = (datetime.utcnow() - timedelta(days=2)).isoformat() + 'Z'
    end = datetime.utcnow().isoformat() + 'Z'
    job = local_jobs[0]
    # TODO uncomment with ut works with ES
    requests = [
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
    for rq in requests:
        print()
        print(job.name, end='')
        response = client.post("/ph/ops/detect", json=rq)
        print('  ', response.url, response.status_code, rq)
        anomalies = response.json()
        print('  ', f"{len(anomalies)} 0: {anomalies[0]}" if anomalies else "  No Anomalies")
        assert response.status_code == 200
        _assert_anomalies(anomalies, job)


# endregion OPERATIONS: Train:

# region OPERATIONS: Detect:

# @pytest.mark.skip(reason="temporarily")
def test_detect_model_with_data_from_request(job2samples, num_samples=2000):
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

        print()
        print(job.name, end='')
        response = client.post("/ph/ops/detect", json=rq)
        rq['data']['records'] = '...'  # for printing
        print('  ', response.url, response.status_code, rq)
        anomalies = response.json()
        print('  ', f"{len(anomalies)} 0: {anomalies[0]}" if anomalies else "  No Anomalies")
        assert response.status_code == 200
        _assert_anomalies(anomalies, job)


# @pytest.mark.skip(reason="temporarily")
def test_detect_model_with_test_datasets(create_models, num_samples=3000):
    # use num_samples to limit the request size. Laptop with 16GB works with num_samples=3000
    for job in local_jobs:
        rq = {
            "job": job.name,
            "data_source": "test_dataset",
            "max_log_records": num_samples
        }
        print()
        print(job.name, )
        response = client.post("/ph/ops/detect", json=rq)
        print('  ', response.url, response.status_code)
        anomalies = response.json()
        print('  ', f"{len(anomalies)} 0: {anomalies[0]}" if anomalies else "  No Anomalies")
        assert response.status_code == 200
        _assert_anomalies(anomalies, job)


def test_detect_dga_model_with_test_datasets(create_models, num_samples=3000):
    # use num_samples to limit the request size. Laptop with 16GB works with num_samples=3000
    for job in [j for j in local_jobs if j.name == 'dga']:
        rq = {
            "job": job.name,
            "data_source": "test_dataset",
            "max_log_records": num_samples
        }
        print()
        print(job.name, )
        response = client.post("/ph/ops/detect", json=rq)
        print('  ', response.url, response.status_code)
        anomalies = response.json()
        print('  ', f"{len(anomalies)} 0: {anomalies[0]}" if anomalies else "  No Anomalies")
        assert response.status_code == 200
        _assert_anomalies(anomalies, job)


# TODO uncomment with ut works with ES
# def test_detect_model_with_logs(num_samples=20000):
#     for job in jobs:
#         rq = {
#             "job": job.name,
#             "data_source": "logs",
#             "max_log_records": num_samples
#         }
#         print()
#         print(job.name, end='')
#         response = client.post("/ph/ops/detect", json=rq)
#         print('  ', response.url, response.status_code, rq)
#         anomalies = response.json()
#         print('  ', f"{len(anomalies)} 0: {anomalies[0]}" if anomalies else "  No Anomalies")
#         assert response.status_code == 200
#         _assert_anomalies(anomalies, job)


# TODO uncomment with ut works with ES
# def test_detect_all_models_with_logs(num_samples=2000):
#     rq = {
#         "job": "all",
#         "data_source": "logs",
#         "max_log_records": num_samples
#     }
#     response = client.post("/ph/ops/detect", json=rq)
#     print('  ', response.url, response.status_code, rq)
#     anomalies = response.json()
#     print('  ', f"{len(anomalies)} 0: {anomalies[0]}" if anomalies else "  No Anomalies")
#     assert response.status_code == 200
#     _assert_anomalies(anomalies, job=None)


# @pytest.mark.skip(reason="temporarily")
model_file_pattern = f"{model_dir}/*.model"


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

# TODO uncomment with ut works with ES
# def test_start_self_diagnostics(num_samples=2000):
#     """
#     It just starts the SD but not verifies the outcome.
#     """
#     response = client.get("/ph/ops/start_self_diagnostics")
#     print(response.url, response.status_code, response.text)
#     assert response.status_code == 202
#     assert len(response.text) >= 36  # 38??? uuid: "4b28397b-16d1-4036-a6ff-0b685fdac9c8"


# TODO uncomment with ut works with ES
# def test_get_self_diagnostics_result(num_samples=2000):
#     """
#     It does not start the SD but verifies the outcome, means the SD should run before this test at last once.
#     Test id: -1, uuid (gets from the first test!), something wrong.
#     """
#     # get the last result
#     id = -1
#     response = client.get(f"/ph/ops/get_self_diagnostics_result/{id}")
#     print(response.url, response.status_code, response.json().keys())
#     assert response.status_code == 200
#     rs = response.json()
#     assert type(rs) == dict
#     assert ['id', 'time', 'job', 'result'] == list(rs.keys())
#     assert 'result' in rs['result']
#     assert rs['result']['result'] in ['Success', 'Failure']
#     print(rs['id'])
#     last_record_id = rs['id']
#
#     id = last_record_id
#     response = client.get(f"/ph/ops/get_self_diagnostics_result/{id}")
#     print(response.url, response.status_code, response.json().keys())
#     assert response.status_code == 200
#     rs = response.json()
#     assert type(rs) == dict
#     assert ['id', 'time', 'job', 'result'] == list(rs.keys())
#     assert 'result' in rs['result']
#     assert rs['result']['result'] in ['Success', 'Failure']
#     print(rs['id'])
#
#     id = 'abracadabra'
#     response = client.get(f"/ph/ops/get_self_diagnostics_result/{id}")
#     print(response.url, response.status_code, response.json().keys())
#     assert response.status_code == 200
#     rs = response.json()
#     assert type(rs) == dict
#     assert ['id', 'time', 'job', 'result'] == list(rs.keys())
#     assert 'result' in rs['result']
#     assert rs['result']['result'] in ['Success', 'Failure']
#     print(rs['id'])
#     assert rs['id'] == last_record_id


# endregion OPERATIONS: Self-diagnostics

# endregion OPERATIONS:


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


def _is_model_files_available():
    # check availability of the new model files: (only dynamic models)
    dynamic_models_num = len([j for j in local_jobs if j.dynamic_model])
    delay_sec = 5
    retry = 10
    models_num = len(glob.glob(model_file_pattern))
    if dynamic_models_num != models_num:
        for i in range(retry):
            print(
                f'{i + 1}/{retry} retries. {models_num} models created. We need to recreate {dynamic_models_num} models.')
            time.sleep(delay_sec)
            models_num = len(glob.glob(model_file_pattern))
            if dynamic_models_num == models_num:
                break
    assert dynamic_models_num == models_num


def _remove_model_files():
    # remove the model files:
    _ = [os.remove(f) for f in glob.glob(model_file_pattern)]
    # successful removal of the model files
    assert not list(glob.glob(model_file_pattern))
    return model_file_pattern
