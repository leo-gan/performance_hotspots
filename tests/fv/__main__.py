import json
import logging
import os
import sys
import time
from collections import namedtuple
from multiprocessing import Process
import uvicorn

from ph import api
from ph.ph import start
from . import api_test
from . import bash_api


def assert_test_result(self_diagnostics_result):
    """
    TBD What should be the assertion result?
    :param self_diagnostics_result:
    :return:
    """
    if not self_diagnostics_result or 'result' not in self_diagnostics_result:
        return False
    if self_diagnostics_result['result'] == 'Failure':
        if (self_diagnostics_result['tests cycle']['report']['result'] == 'Success'
                and not params.AD_TEST_USE_ELASTICSEARCH):
            return True
        return False
    else:
        return True


def run_locally_from_source(max_retry=12):
    """
    If the test finished with empty Self Diagnostics result, you probably need to increase the max_retry.
    """
    data_dir = './data'
    file_test_result = 'test_result.json'
    file_test_result_path = f'{data_dir}/{file_test_result}'
    if os.path.isfile(file_test_result_path):
        os.remove(file_test_result_path)
    assert not os.path.isfile(file_test_result_path)

    child_proc = Process(target=start, args=())
    child_proc.start()

    wait_secs = 20
    self_diagnostics_result = {}
    for r in range(max_retry):
        if os.path.isfile(file_test_result_path):
            with open(file_test_result_path) as f:
                self_diagnostics_result = json.load(f)
                logger.info(f'Loaded test result from "{file_test_result_path}"')
            break
        logger.info(f'  {r + 1}/{max_retry} wait {wait_secs} seconds to get the test result.')
        time.sleep(wait_secs)

    child_proc.terminate()
    logger.info(f'**** Finished fv test and TERMINATED the AD jobs.')

    logger.info(f'Self Diagnostics result: {self_diagnostics_result}')
    return self_diagnostics_result


def run_locally():
    self_diagnostics_result = {}
    if params.AD_TEST_CONTAINER == 'None':
        # run the app from the source code:
        self_diagnostics_result = run_locally_from_source()

    else:
        # use container: TODO
        if params.AD_TEST_CONTAINER == 'Test':
            bash_api.container_build_push()
        else:
            # download a manifest
            # find an image in the manifest
            # it should be in the registry!
            pass
        # pull an image from registry:
        # run the app in the container.

    return self_diagnostics_result


def run_in_k8s():
    # # [optional] Deploy a cluster
    # bash_api.run_deploy_cluster()

    # Download the AD job manifest
    bash_api.run_download_file(manifest_file_url, manifest_file)

    # [optional] Configure jobs
    bash_api.run_edit_file(manifest_file, manifest_edited_file, remove_network_policies=True)

    # Deploy an AD job pod
    # manifest_edited_file = '/home/leonid/temp/temp.yaml' # for DEBUG
    bash_api.run_apply_manifest_to_k8s(manifest_edited_file)

    # Read the test anomaly report in the pod log
    self_diagnostics_result = bash_api.run_read_pod_log()

    # Undeploy the AD job pod
    if params.AD_TEST_DELETE_MANIFEST:
        bash_api.run_delete_manifest_from_k8s(manifest_edited_file)

    # # [optional] Undeploy a cluster
    # bash_api.run_undeploy_cluster()

    return self_diagnostics_result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S",
                        format='%(asctime)s : %(levelname)s : %(message)s')
    app_name = 'performance_hotspots_fv_test'
    logger = logging.getLogger(app_name)

    result = 1

    Params = namedtuple('Params', 'AD_TEST_LOCAL AD_TEST_CONTAINER AD_TEST_USE_ELASTICSEARCH '
                                  'AD_TEST_USE_LOCAL_MANIFEST AD_TEST_DELETE_MANIFEST')
    params = Params(
        # True: run tests locally without k8s; False: run tests in the k8s
        eval(os.getenv('AD_TEST_LOCAL', 'True')),

        # one of ["None", "Test", "Current Release"]
        #   None: run app from source code;
        #   Test: run app from "test" container. Build this container right in the test.
        #   Current Release: run app from the container mentioned in the downloaded current release manifest (see doc).
        os.getenv('AD_TEST_CONTAINER', 'None'),

        # True: the ES is used, so the 2nd cycle of the Self Diagnostics should be Successful; False: the opposite
        eval(os.getenv('AD_TEST_USE_ELASTICSEARCH', 'True')),

        # used only when the AD_TEST_LOCAL is False.
        # True: use the test manifest; False: use the manifest from the current release
        eval(os.getenv('AD_TEST_USE_LOCAL_MANIFEST', 'True')),

        # used only when the AD_TEST_LOCAL is False.
        # Remove the app pod from k8s after test. Used only for debugging the issues.
        eval(os.getenv('AD_TEST_DELETE_MANIFEST', 'True')),
    )

    assert params.AD_TEST_CONTAINER in ["None", "Test", "Current Release"]
    logger.info('Initialized params for the main.py: ' + ', '.join(
        [f'{n}: {el}' for el, n in zip(params, params._fields)]))

    # AD_TEST_APP == True : run fv for the Application (as an application that runs the Self-Diagnostics and stop)
    AD_TEST_APP = eval(os.getenv('AD_TEST_APP', 'True'))
    logger.info(f'AD_TEST_APP: {AD_TEST_APP}')
    if AD_TEST_APP:
        logger.info(f'START: {app_name}')

        manifest_file_url = 'https://docs.tigera.io/master/manifests/threatdef/ad-jobs-deployment.yaml'
        manifest_file = './tests/conf/performance-hotspots-deployment.yaml'
        manifest_edited_file = './tests/conf/performance-hotspots-deployment.edited.yaml'

        self_diagnostics_result = run_locally() if params.AD_TEST_LOCAL else run_in_k8s()

        result = assert_test_result(self_diagnostics_result)

        logger.info(f'STOP: {app_name} with {result} result')

    # AD_TEST_API == True : run fv for API:
    AD_TEST_API = eval(os.getenv('AD_TEST_API', 'True'))
    logger.info(f'AD_TEST_API: {AD_TEST_API}')
    if AD_TEST_API:
        logger.info(f'START: API fv')
        # uvicorn.run(api.app, host="0.0.0.0", port=8000)
        kwargs = {'host': "0.0.0.0", 'port': 8000}
        api_proc = Process(target=uvicorn.run, args=(api.app, ), kwargs=kwargs)
        api_proc.start()
        time.sleep(5)

        api_test.run_all_tests()
        logger.info(f'STOP: API fv')
        api_proc.terminate()
        logger.info(f'**** TERMINATED the API process.')
    sys.exit(0 if result else 1)  # 0 means Success, 1 - Failure

