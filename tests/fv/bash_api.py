import subprocess
import os.path
from collections import namedtuple
import yaml
import time

import logging
logger = logging.getLogger('performance_hotspots_fv_test')

Params = namedtuple('Params', 'KUBECONFIG ES_CA_CERT host http_auth PERFORMANCE_HOTSPOTS_IMAGE')
params = Params(
    os.getenv('KUBECONFIG', '/home/leonid/.local/kubeconfig'),
    os.environ.get('ES_CA_CERT'),
    f"https://{os.getenv('ELASTIC_HOST', 'tigera-secure-es-http.tigera-elasticsearch.svc')}:{os.getenv('ELASTIC_PORT', '9200')}",
    (os.environ.get('ELASTIC_USER'), os.environ.get('ELASTIC_PASSWORD')),
    os.getenv('PERFORMANCE_HOTSPOTS_IMAGE', 'gcr.io/unique-caldron-775/cnx/tigera/performance_hotspots:master')
)

logger.info('Initialized ElasticClient with params: ' + ', '.join(
    [f'{n}: {el if n != "http_auth" else "..."}' for el, n in zip(params, params._fields)]))

deployment_name = 'ad-jobs-deployment'


def run_deploy_cluster():
    raise Exception('Not implemented yet.')
    return None


def run_undeploy_cluster():
    raise Exception('Not implemented yet.')
    return None


def run_download_file(from_url, out_file):
    # curl https://docs.tigera.io/master/manifests/threatdef/ad-jobs-deployment.yaml -o
    proc = subprocess.run(['curl', str(from_url), '-o', str(out_file)], check=True, text=True)
    if not os.path.isfile(out_file):
        msg = f'curl {from_url} cannot save file {out_file}'
        logger.info(msg)
        raise Exception(msg)
    else:
        logger.info(f'Downloaded the "{out_file}" file from {from_url}.')
    return


def _image_patch(file_name):
    """
    It works if the image name does not ended on "master"
    :param file_name: this file is rewritten if the patch condition is met.
    """
    correct_image = f'        image: {params.PERFORMANCE_HOTSPOTS_IMAGE}'

    with open(file_name, 'r') as f:
        docs_str = f.read()
        logger.info(f'Loaded "{file_name}" file.')

    out_str = []
    for line in docs_str.splitlines():
        if line.replace(' ', '')[:6] == 'image:' and line.replace(' ', '')[-1] == ':':
            logger.info(f'  Replaced "{line}" --> "{correct_image}"')
            line = correct_image
        out_str.append(line + '\n')

    with open(file_name, 'w') as f:
        f.writelines(out_str)
        logger.info(f'Saved "{file_name}" file.')


def run_edit_file(in_file, out_file, remove_network_policies=False):
    """
    Remove the 'NetworkPolicy' documents from the yaml.
    :param in_file:
    :param out_file:
    :param remove_network_policies:
    :return:
    """
    _image_patch(in_file)

    out_docs = []
    with open(in_file, 'r') as f:
        docs = yaml.load_all(f, Loader=yaml.FullLoader)
        logger.info(f'Loaded "{in_file}" file.')
        for doc in docs:
            if remove_network_policies and doc['kind'] == 'NetworkPolicy':
                continue
            out_docs.append(doc)
    with open(out_file, 'w') as f:
        yaml.dump_all(out_docs, f)
        logger.info(f' Modified yaml file.')
        logger.info(f'Saved "{out_file}" file.')


def _process_self_diagnostics_result(logs):
    """
    See the performance_hotspots.test_utils.output_test_result() for the log format.
    :param logs:
    :return:
    """
    marker = 'Self Diagnostics: '
    result = {}
    for log in logs.split('\n'):
        if marker in log:
            t = log.find(marker)
            result = eval(log[t+len(marker):])
            break
    if not result:
        logger.info(f'"{marker}" is not presented in the logs: ... {logs[-200:]}')
    return result


def _run_command(command):
    cmd = command.split(' ')
    proc = None
    try:
        proc = subprocess.run(cmd, capture_output=True) #, check=True, text=True)
        out_text = proc.stdout
        logger.info(f'$ {" ".join(cmd)}')
        logger.info(f'  output: {out_text}')
        print(proc)
    except subprocess.CalledProcessError as exc:
        print(proc)
        print("Status : FAIL", exc.returncode, exc.output)
        logger.info(f"stdout: {proc.stdout if proc else 'subprocess failed.'}\n")


def _run_command_with_stdin(command, stdin_file):
    cmd = command.split(' ')
    proc = None
    try:
        with open(stdin_file) as f:
            proc = subprocess.run(cmd, capture_output=True, stdin=f) #, check=True, text=True)
        out_text = proc.stdout
        logger.info(f'$ {" ".join(cmd)}')
        logger.info(f'  output: {out_text}')
        print(proc)
    except subprocess.CalledProcessError as exc:
        print(proc)
        print("Status : FAIL", exc.returncode, exc.output)
        logger.info(f"stdout: {proc.stdout if proc else 'subprocess failed.'}\n")


def _run_command_pipe(command1, command2):
    proc = None
    try:
        cmd1 = command1.split(' ')
        proc1 = subprocess.run(cmd1, capture_output=True, check=True, text=True)
        logger.info(command1)
        logger.info(proc1)

        cmd2 = command2.split(' ')
        proc2 = subprocess.run(cmd2, capture_output=True, check=True, text=True)
        proc1.stdout.close()
        logger.info(command2)
        logger.info(proc2)

        out_text = proc2.stdout
        logger.info(f'  output: {out_text}')
    except subprocess.CalledProcessError as exc:
        print(proc)
        print("Status : FAIL", exc.returncode, exc.output)
        logger.info(f"stdout: {proc.stdout if proc else 'subprocess failed.'}\n")


def container_build_push(image='tigera-dev/leo/performance_hotspots', tag='test'):
    # _run_command_pipe('gcloud auth print-access-token',
    #                   'docker login -u oauth2accesstoken --password-stdin https://gcr.io')
    _run_command_with_stdin(f'sudo docker build -t gcr.io/{image}:{tag} .', '/home/leonid/psw.txt')
    _run_command_with_stdin( f'sudo docker push gcr.io/{image}:{tag}', '/home/leonid/psw.txt')


def run_read_pod_log(kubeconfig_file=None):
    """
    It reads the pod log and extracts the Self Diagnostics result if it presented.
    See the performance_hotspots.test_utils.output_test_result() for the log format.
    :param kubeconfig_file:
    :return: The Self Diagnostics result as a dictionary
    """
    # export KUBECONFIG=kubeconfig_file
    # kubectl get pods -n performance_hotspots
    if not kubeconfig_file:
        kubeconfig_file = params.KUBECONFIG
    # kubectl apply -f performance-hotspots-deployment.yaml
    max_retry = 5
    wait_secs = 20
    pod_name, out_text = '', ''
    cmd = ['kubectl', 'get', 'pods', '-n', 'tigera-intrusion-detection']
    for r in range(max_retry):
        proc = subprocess.run(cmd,
                              env=dict(os.environ, KUBECONFIG=kubeconfig_file),
                              capture_output=True, check=True, text=True)
        logger.info(f'$ {" ".join(cmd)}')
        out_text = proc.stdout.split()
        if not out_text:
            logger.info(f'  {r+1}/{max_retry} wait {wait_secs} seconds to get the pod logs.')
            time.sleep(wait_secs)
            continue
        i_pod_name = [(i, el) for i, el in enumerate(out_text) if str(el).startswith(deployment_name)]
        if not len(i_pod_name):
            logger.info('f  *** a pod for the {deployment_name} deployment was not created. Something wrong!')
            return None
        i, pod_name = i_pod_name[0]
        if out_text[i+2] == 'Running':
            logger.info(f'  Pod {pod_name} running.')
            break
        elif out_text[i+2] == 'ContainerCreating':
            logger.info(f'  Pod {pod_name} in {out_text[i+2]} status.')
            logger.info(f'  {r+1}/{max_retry} wait {wait_secs} seconds to get the pod logs.')
            time.sleep(wait_secs)
            continue
        else:
            logger.info(f'  Pod {pod_name} in {out_text[i+2]} status.')
            break
    if not out_text:
        logger.info(f'Waited {max_retry*wait_secs} seconds, but a pod was not created.')
        return None

    self_diagnostics_result = {}
    max_retry = 5
    cmd = ['kubectl', 'logs', pod_name]
    for r in range(max_retry):
        try:
            logger.info(f'$ {" ".join(cmd)}')
            proc = subprocess.run(cmd,
                                  env=dict(os.environ, KUBECONFIG=kubeconfig_file),
                                  capture_output=True, check=True, text=True)
        except subprocess.CalledProcessError as ex:
            logger.info(f'*** {ex}')
            return None
        logs = proc.stdout

        # Investigate the log.
        self_diagnostics_result = _process_self_diagnostics_result(logs)
        if self_diagnostics_result:
            break
        else:
            logger.info(f'  {r + 1}/{max_retry} wait {wait_secs} seconds to get the Self Diagnostics result.')
            time.sleep(wait_secs)
    if not self_diagnostics_result:
        logger.info(f'Waited {max_retry * wait_secs} seconds, but the Self Diagnostics not executed.')
    return self_diagnostics_result


def run_apply_manifest_to_k8s(manifest_file, kubeconfig_file=None):
    _run_k8s_operation_on_manifest('apply', manifest_file, kubeconfig_file=kubeconfig_file)
    return


def run_delete_manifest_from_k8s(manifest_file, kubeconfig_file=None):
    _run_k8s_operation_on_manifest('delete', manifest_file, kubeconfig_file=kubeconfig_file)


def _run_k8s_operation_on_manifest(operation, manifest_file, kubeconfig_file=None):
    if not kubeconfig_file:
        kubeconfig_file = params.KUBECONFIG
    # export KUBECONFIG=kubeconfig_file
    # kubectl apply -f performance-hotspots-deployment.yaml
    cmd = ['kubectl', operation, '-f', str(manifest_file)]
    proc = subprocess.run(cmd,
                          env=dict(os.environ, KUBECONFIG=kubeconfig_file),
                          capture_output=True, check=True, text=True)
    out_text = proc.stdout.split()

    # check if the manifest applied without errors:
    succcess_results = {'apply': ['created', 'configured', 'unchanged'],
                        'delete': ['deleted']}
    res = "SUCCEEDED" if out_text and out_text[-1] in succcess_results[operation] else "FAILED"
    logger.info(f'$ {" ".join(cmd)}')
    logger.info(f'  "{operation}" {res} with the output:')
    logger.info(f'    \"{" ".join(out_text)}\"')
    return
