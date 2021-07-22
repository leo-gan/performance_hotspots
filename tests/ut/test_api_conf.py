import random
from datetime import datetime

from fastapi.testclient import TestClient

from ph.api import app
from ph.globals import jobs, job_name2job

client = TestClient(app)

local_jobs = jobs()

def test_ping():
    ts = datetime.utcnow()
    response = client.get("/ph/ping")
    print(response.url, response.status_code, response.text)
    assert response.status_code == 200
    rs = response.json()
    assert rs['service'] == "performance_hotspots_service"
    ts_svc = datetime.strptime(rs['utcnow'].replace('Z', ''), "%Y-%m-%dT%H:%M:%S.%f")
    assert abs(ts.second - ts_svc.second) < 2


def test_get_parameters():
    # with job_name
    for job in local_jobs:
        url = f'/ph/conf/get_parameters/{job.name}'
        rs = client.get(url)
        print(rs.url, rs.status_code, rs.json())
        assert rs.status_code == 200
        rs = rs.json()
        assert_job_params(job, rs)


def test_get_all_parameters():
    # with job_name
    url = f'/ph/conf/get_parameters/all'
    rs = client.get(url)
    print(rs.url, rs.status_code, rs.json())
    assert rs.status_code == 200
    rs = rs.json()
    assert type(rs) == list
    for job_rs in rs:
        assert job_rs['job'] in job_name2job
        assert_job_params(job_name2job[job_rs['job']], job_rs)


def test_set_parameters():
    job = "port_scan"
    param = "AD_port_scan_threshold"

    # get the param. We restore it at the end
    url = f'/ph/conf/get_parameters/{job}'
    rs = client.get(url)
    print(rs.url, rs.status_code, rs.json())
    assert rs.status_code == 200
    param_val_old = rs.json()['params'][param]  # type can be not str

    # set a new value
    url = f'/ph/conf/set_parameters'
    param_val_new = random.randint(100, 700)
    rq = {
        "job": job,
        "params": {
            param: str(param_val_new)
        }
    }
    rs = client.put(url, json=rq)
    print(rs.url, rs.status_code, rs.json())
    assert rs.status_code == 200
    rs = rs.json()
    assert type(rs) == dict
    assert rq == rs

    # double check with get:
    url = f'/ph/conf/get_parameters/{job}'
    rs = client.get(url)
    print(rs.url, rs.status_code, rs.json())
    assert rs.status_code == 200
    assert rq == rs.json()

    # restore old value:
    url = f'/ph/conf/set_parameters'
    rq = {
        "job": job,
        "params": {
            param: str(param_val_old)
        }
    }
    rs = client.put(url, json=rq)
    print(rs.url, rs.status_code, rs.json())
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
