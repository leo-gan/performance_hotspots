import json
import os
import uuid
from shutil import copyfile

from ph.globals import data_dir
from ph.globals import job_name2job
from ph.self_diagnostics import output_self_diagnostics_report, file_all_detected_anomalies_test, \
    file_all_detected_anomalies


def test_output_self_diagnostics_report():
    """
    It needs some preprocessing. It reads two files and we need to take care of the content of these files,
    if they are not created yet.
    We keep copies of these files here, in the test/ut dir.
    Note: we run ut not from the test/ut dir but from app root dir.
    """

    def recreate_file(source_name):
        source_file_name = f'tests/ut/{source_name}.csv'
        dest_file_name = f'{data_dir}/{source_name}.csv'
        assert os.path.exists(source_file_name), f'file "{source_file_name}" should be here.'
        if not os.path.exists(dest_file_name):
            copyfile(source_file_name, dest_file_name)
            print(f'Created "{dest_file_name}" file.')
            return True
        else:
            print(f'"{dest_file_name}" file exist.')
            return False

    def remove_file_if_created(source_name, is_file_all_detected_anomalies_test_created):
        dest_file_name = f'{data_dir}/{source_name}.csv'
        if is_file_all_detected_anomalies_test_created:
            _remove_file(dest_file_name)

    # if file_all_detected_anomalies_test does not exist, create it
    is_file_all_detected_anomalies_test_created = recreate_file(file_all_detected_anomalies_test)

    # if file_all_detected_anomalies does not exist, create it
    is_file_all_detected_anomalies_created = recreate_file(file_all_detected_anomalies)

    # run self_diagnostic. It saves the SD report as a file.
    test_result_file = f'{data_dir}/test_result.json'
    _remove_file(test_result_file)
    id = f'test_{str(uuid.uuid4())}'
    output_self_diagnostics_report(None, id)

    # assert the new SD report format:
    assert os.path.exists(test_result_file)
    with open(test_result_file) as f:
        js = json.load(f)
        _assert_sd_report(js)

    # remove anomalies files if we created them in this test
    remove_file_if_created(file_all_detected_anomalies_test, is_file_all_detected_anomalies_test_created)
    remove_file_if_created(file_all_detected_anomalies, is_file_all_detected_anomalies_created)


def _assert_sd_report(js):
    def assert_result(k, v):
        assert k == 'result'
        assert v in ['Success', 'Failure']

    def assert_job(job):
        assert type(job) == dict
        for el, val in job.items():
            assert el in ['name', 'anomalies', 'statistics']
            if el == 'name':
                assert val in job_name2job
            elif el == 'anomalies':
                assert type(val) == int
            elif el == 'statistics':
                assert type(val) == dict
                for s, v in val.items():
                    assert s in ['TP - Correctly detected', 'FP - Falsely detected', 'FN - Falsely Not detected',
                                 'precision', 'recall', 'F1', 'F1_tolerance', 'result']
                    if s == 'result':
                        assert_result(s, v)
                    else:
                        assert type(v) in [int, float]
            else:
                assert False, 'should never happen'

    def assert_job_report(rep):
        assert type(rep) == dict
        for el, val in rep.items():
            assert el in ['jobs', 'result']
            if el == 'jobs':
                assert type(val) == list
                for job in val:
                    assert_job(job)
            elif el == 'result':
                assert_result(el, val)

    print(type(js), js)
    assert type(js) == dict
    for el, val in js.items():
        assert el in ['tests cycle', 'first training and detection cycle', 'result']
        if el in ['tests cycle', 'first training and detection cycle']:
            assert type(val) == dict
            assert list(val.keys()) == ['description', 'report']
            assert_job_report(val['report'])
        elif el == 'result':
            assert_result(el, val)


def _remove_file(file_name):
    if os.path.exists(file_name):
        os.remove(file_name)
        print(f'Removed the "{file_name}" file.')
        assert not os.path.exists(file_name)
