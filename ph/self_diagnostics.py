import os
import json
from collections import defaultdict, Counter
import pandas as pd

from .globals import jobs

import logging
from .globals import APP_NAME, data_dir
from .history_storage import save_json_in_line

logger = logging.getLogger(APP_NAME)

file_all_detected_anomalies_test = 'all_anomalies_test'
file_all_detected_anomalies = 'all_anomalies'
file_test_result = 'test_result.json'

local_jobs = jobs()


def _load_test_datasets():
    """
    Loads all *.test_dataset.csv datasets.
    It removes the 'anomaly' column when loads.
    """
    return {job.name: pd.read_csv(f'{data_dir}/{job.name}.test_dataset.csv',
                                  usecols=lambda col: col != 'anomaly', low_memory=False).to_dict('records')
            for job in local_jobs}


load_train_data = _load_test_datasets
load_find_anomalies_data = _load_test_datasets


def _format_all_detected_anomalies(all_detected_anomalies, allow_no_anomalies=False):
    """
    return job anomaly statistics with 'result' as the 'Success' or 'Failure'
    Failure - when no anomalies detected at all but with condition.
    """
    if not all_detected_anomalies: return {'jobs': [], 'result': 'Failure' if not allow_no_anomalies else 'Success'}
    anomalies = [{'name': job, 'anomalies': count} for job, count in
                 Counter([a['alert'].split('.')[1] for a in all_detected_anomalies]).items()]
    result = 'Success' if sum([a['anomalies'] for a in anomalies]) else 'Failure'
    return {'jobs': anomalies, 'result': result}


def _report_job_statistics(dct_test, dct_detected, verbose=False):
    """
    Formats the self diagnostics report. It counts all binary metrics for each job, and the results
    for each detection cycle, and compound results (two for two self diagnostics cycles and the final result for
    the whole self diagnostics).
    dct_test: anomalies found in the test datasets.
    dct_test: anomalies found in the real data.
    dct_x = {'job_name': [{k: anomaly}, ...], ...}
    """

    def dga_test_domains(test_anomalies):
        return {an['qname']: an['qname'] for an in test_anomalies.values()}

    def dga_detected_domains(detected_anomalies):
        return {d: d for an in detected_anomalies.values()
                for d in an['domains'].split(',')}

    # jobs with detected anomalies are subsets of the jobs from the tests datasets
    for detected_job_name in dct_detected:
        assert detected_job_name in dct_test

    job2tolerance = {job.name: job.tolerance for job in local_jobs}
    result = {'jobs': []}
    for job_name, test_anomalies in dct_test.items():
        assert job_name in job2tolerance
        if job_name not in dct_detected: continue
        detected_anomalies = dct_detected[job_name]
        if job_name == 'dga':
            # restore aggregated anomalies. They are aggregated for 'dga'.
            test_anomalies = dga_test_domains(test_anomalies)
            detected_anomalies = dga_detected_domains(detected_anomalies)

        res = {'name': job_name}

        TP_samples = [v for k, v in detected_anomalies.items() if k in test_anomalies]
        TP = len(TP_samples)
        if verbose: res['TP_samples'] = TP_samples

        FN_samples = [v for k, v in test_anomalies.items() if k not in detected_anomalies]
        FN = len(FN_samples)
        if verbose: res['FN_samples'] = FN_samples

        FP_samples = [v for k, v in detected_anomalies.items() if k not in test_anomalies]
        FP = len(FP_samples)
        if verbose: res['FP_samples'] = FP_samples

        res['statistics'] = {
            'TP - Correctly detected': TP,
            'FP - Falsely detected': FP,
            'FN - Falsely Not detected': FN,
            'precision': round(TP / (TP + FP), 3),
            'recall': round(TP / (TP + FN), 3),
            'F1': round(2 * TP / (2 * TP + FP + FN), 3),
            'F1_tolerance': job2tolerance[job_name],
            'result': 'Success' if (2 * TP / (2 * TP + FP + FN)) >= job2tolerance[job_name] else "Failure",
        }
        result['jobs'].append(res)
    result['result'] = 'Success' if all([j['statistics']['result'] == 'Success' for j in result['jobs']]) else "Failure"
    return result


def output_self_diagnostics_report(exc, id):
    """
    if exc is None, the preliminary train+detect cycle on the real-life datasets was successful;
    main.find_anomalies() with i=0 and is_test=True saves anomalies in two files:
    The first one is for the tests dataset, the second one is for the real-life dataset.
    !!! if exc is not None: raises an exception with the self-diagnostics report as an exception message.
    """
    # a patch for the port_scan and ip_sweep .test_dataset.csv formats
    def apply_format_start_time(start_time):
        return int(pd.to_datetime(start_time).timestamp()) if type(start_time) == str and start_time.count(
            ':') else start_time

    # read all tests datasets and select all anomalies (labels):
    test_anomalies = {}
    for job in local_jobs:
        df = pd.read_csv(f'{data_dir}/{job.name}.test_dataset.csv')
        if job.name in ['port_scan', 'ip_sweep']:
            # these jobs keep the 'start_time' in different format, so we transform it to default format.
            df['start_time'] = df['start_time'].apply(apply_format_start_time)
        test_anomalies[job.name] = [{k: v for k, v in a.items() if k != 'anomaly'} for a in
                                    df[df['anomaly'] != 0].to_dict('records')]
        test_anomalies[job.name] = {'^'.join([str(v) for k, v in row.items() if k in job.group_fields]): row
                                    for row in test_anomalies[job.name]}

    # read file_all_detected_anomalies_test
    job_names = [job.name for job in local_jobs]
    group_fields = {job.name: job.group_fields for job in local_jobs}
    all_detected_anomalies_test = pd.read_csv(f'{data_dir}/{file_all_detected_anomalies_test}.csv').to_dict('records')
    out = defaultdict(list)
    for a in all_detected_anomalies_test:
        j_name = a['alert'].split('.')[1]
        assert j_name in job_names
        out[j_name].append(eval(a['record'].replace('nan', 'None')))
    detected_test_anomalies = {}
    for k, v in out.items():
        assert k in group_fields
        detected_test_anomalies[k] = {'^'.join([str(vvv) for kkk, vvv in vv.items() if kkk in group_fields[k]]): vv for
                                      vv in v}

    # generate tests report:
    test_report = _report_job_statistics(test_anomalies, detected_test_anomalies, verbose=False)

    # read file_all_detected_anomalies. They are in the form of alerts!
    all_detected_anomalies = []
    file_all_detected_anomalies_file_name = f'{data_dir}/{file_all_detected_anomalies}.csv'
    if os.path.isfile(file_all_detected_anomalies_file_name):
        all_detected_anomalies = pd.read_csv(file_all_detected_anomalies_file_name).to_dict('records')

    # check if all jobs found some anomalies
    if not exc:
        # Self-diagnostics detection on the real-life data can produce no anomalies and it is still 'Success'
        alert_report = _format_all_detected_anomalies(all_detected_anomalies, allow_no_anomalies=True)
    else:
        alert_report = {
            'result': 'Failure',
            'description': 'Exception: ' + str(exc)
        }

    result = 'Success' if all([el['result'] == 'Success' for el in [test_report, alert_report]]) else 'Failure'
    total_result = {
        'tests cycle': {
            'description': 'A training+detection cycle on the tests datasets.',
            'report': test_report
        },
        'first training and detection cycle': {
            'description': 'A training+detection cycle on the real-life datasets.',
            'report': alert_report
        },
        'result': result,
    }
    file_test_result_path = f'{data_dir}/{file_test_result}'
    with open(file_test_result_path, 'w') as f:
        json.dump(total_result, f)
        logger.info(f'Saved test result into "{file_test_result_path}"')
    logger.info(f'Self Diagnostics: {total_result}')
    save_json_in_line('self_diagnostics', job='all', js=total_result, id=id)
    if exc:
        raise Exception(json.dumps(total_result))
    return total_result
