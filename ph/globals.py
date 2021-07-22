import logging
from collections import namedtuple
import os

APP_NAME = 'performance_hotspots'
logger = logging.getLogger(APP_NAME)


logs = ['flows', 'l7', 'dns']

# all folders here are the sub-folders of the root
data_dir = './data'
model_dir = './models'


Job = namedtuple('Job', 'name params field model_name  data_type source_log group_fields tolerance dynamic_model')
"""
params: the env vars that can be used externally for configuration with the API.
data_type: is not always the source_log, because we can aggregate some log data. For example, 'source' and 'dest' types
produced from the 'flows' log. 
"""
_jobs = [
    Job('l7_latency',
        {'PH_L7Latency_IsolationForest_n_estimators': 100, 'PH_L7Latency_IsolationForest_score_threshold': -0.836},
        'hits.hits._source.duration_mean', 'L7LatencyModel', 'l7', 'l7',
        ['start_time', 'dest_name_aggr', 'dest_namespace', 'dest_service_name', 'src_namespace', 'src_name_aggr',
         'duration_mean'], 0.85, 1),
]


def jobs():
    """Reset a list of the jobs with the PH_DISABLED_DETECTORS environment variable.
    Use `PH_DISABLED_DETECTORS` to opt out some detectors from the detection."""
    disabled = os.getenv('PH_DISABLED_DETECTORS', '')  # TODO document this env variable!
    return [j for j in _jobs if j.name not in disabled.split(',')]


job_name2job = {job.name: job for job in jobs()}


def get_param(job, param_name, param_type=str):
    assert param_type in [str, int, float, bool]
    default_param = job_name2job[job].params[param_name]
    val = os.getenv(param_name, default_param)
    if param_type == str:
        val = str(val)
    elif param_type == int:
        val = int(val)
    elif param_type == float:
        val = float(val)
    elif param_type == bool:
        val = eval(val)
    return val
