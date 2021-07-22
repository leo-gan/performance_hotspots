import itertools
import logging
import uuid
from datetime import datetime
from typing import Optional, List, Union
from multiprocessing import Process
import uvicorn

from fastapi import BackgroundTasks, FastAPI, Response, status

from . import model_processor
from .ph import self_diagnostics
from .api_classes import JobNames, OperationDataRq, Anomaly, JobParams
from .api_helper import prepare_samples, prepare_all_samples, set_envvars, get_envvars, format_alert_to_anomaly
from .globals import APP_NAME, jobs
from .history_storage import read_id_json

logger = logging.getLogger(APP_NAME)

local_jobs = jobs()
job_name2data_type = {job.name: job.data_type for job in local_jobs}
dynamic_jobs = {job.name for job in local_jobs if job.dynamic_model}

app = FastAPI()


def start():
    logger.info(f'START: API')
    kwargs = {'host': "0.0.0.0", 'port': 8000}
    api_proc = Process(target=uvicorn.run, args=(app,), kwargs=kwargs)
    api_proc.start()
    return api_proc


def stop(api_proc):
    api_proc.terminate()
    logger.info(f'STOP: API')


@app.get("/ph/ping")
async def ping():
    """
    Check availability of the service.

    **return:** A name of the service - `performance_hotspots_service` and the ping UTC time.
    """
    return {"service": "performance_hotspots_service", "utcnow": datetime.utcnow()}


# region Operations:
@app.post("/ph/ops/train", tags=["Operations"], status_code=202)
async def train(rq: OperationDataRq, background_tasks: BackgroundTasks, response: Response):
    """
    Train all or a single model.

    It can use training data from the Elasticsearch indexes OR directly from the request OR
    from the internal labeled test datasets.

    - **param rq:** A request. See an example.

    **return:**   Returns status_code=202 `Accepted` because the self-diagnostics operation is a long-running operation and we
    do not wait till the end of the operation.

    """
    job_name = rq.job.name
    samples = prepare_all_samples(rq, local_jobs) if job_name == 'all' else prepare_samples(rq, local_jobs)
    if not samples:
        msg = f"* NO training of '{job_name}'. No samples - No training :("
        response.status_code = status.HTTP_404_NOT_FOUND
    elif job_name == 'all':
        _ = [background_tasks.add_task(model_processor.train_job, job.name, samples[job.data_type])
             for job in local_jobs if job.dynamic_model]
        # _ = [model_processor.train_job(job.name, samples[job.data_type]) for job in jobs if job.dynamic_model]
        msg = f'STOP training all models.  Retrained models will replace the old models.'
    elif job_name not in dynamic_jobs:
        msg = f"* NO training of '{job_name}'. The model is static and we do not train it here."
        response.status_code = status.HTTP_404_NOT_FOUND
    elif job_name2data_type[job_name] not in samples:
        msg = f"* NO training of '{job_name}'. No samples - No training :("
        response.status_code = status.HTTP_404_NOT_FOUND
    else:
        background_tasks.add_task(model_processor.train_job, job_name, samples[job_name2data_type[job_name]])
        # model_processor.train_job(job_name, samples[job_name2data_type[job_name]])
        msg = f"STOP training '{job_name}' model with {len(samples[job_name2data_type[job_name]]):,} " \
              f"samples. Retrained model will replace the old model."
    logger.info(msg)
    return msg


@app.post("/ph/ops/detect", tags=["Operations"], response_model=List[Anomaly])
def detect_anomalies(rq: OperationDataRq, response: Response):
    """
    Detect anomalies using all or just a single model. Use the `job` request field to define it.

    Ust the `data_source` request field to get data from the Elasticsearch indexes OR directly from the request OR
    from the internal labeled test datasets.

    - **param rq:** A request.

    **return:** A list of anomalies.
    """
    anomalies = []
    job_name = rq.job.name
    samples = prepare_all_samples(rq, local_jobs) if job_name == 'all' else prepare_samples(rq, local_jobs)
    if not samples:
        msg = f"* NO detection with '{job_name}'. No samples - No detection :("
        response.status_code = status.HTTP_404_NOT_FOUND
    elif job_name == 'all':
        anomalies = [model_processor.detect(job.name, samples[job.data_type]) for job in local_jobs]
        anomalies = list(itertools.chain(*anomalies))  # flatten List[list] -> list
        anomalies = model_processor.aggregate_byte_anomalies(anomalies)
        msg = f'Stop Detection with all models. {len(anomalies):,} anomalies.'
    elif job_name2data_type[job_name] not in samples:
        msg = f"* NO detection with '{job_name}'. No samples - No training :("
        response.status_code = status.HTTP_404_NOT_FOUND
    else:
        anomalies = model_processor.detect(job_name, samples[job_name2data_type[job_name]])
        msg = f"Stop Detection with '{job_name}' model with {len(samples[job_name2data_type[job_name]]):,} samples. {len(anomalies):,} anomalies."
    logger.info(msg)
    anomalies = format_alert_to_anomaly(anomalies)
    return anomalies


@app.get("/ph/ops/start_self_diagnostics", tags=["Operations"], status_code=202)
async def start_self_diagnostics(background_tasks: BackgroundTasks):
    """
    Run a **self-diagnostics**. It always runs with all jobs.

    It is compound of two cycles, each runs the model training then the performance hotspot detection.

    The first cycle uses the internal labeled test datasets. It uses the labels in the test dataset
    to calculate the performance metrics of the retrained models.

    The second cycle uses the Elasticsearch data. It verifies that performance hotspot detection works properly
    with the data from the Elasticsearch indexes.

    A result of the self-diagnostics is saved. Use the `get_self_diagnostics_result` endpoint
    to get this result.

    **return:** the self-diagnostics `id` (uuid). Use it to get the results of the self-diagnostics.

    Returns status_code=202 "Accepted" because the self-diagnostics operation is a long-running operation and we
    do not wait till the end of the operation.
   """
    id = str(uuid.uuid4())
    background_tasks.add_task(self_diagnostics, lock=None, id=id)
    return id


@app.get("/ph/ops/get_self_diagnostics_result/{id}", tags=["Operations"])
async def get_self_diagnostics_result(id: str = '-1'):
    """
    Return the self-diagnostics result by the self-diagnostics id or by the number of the result,
    starting from the last result.

    - **param id:** The self-diagnostics id or by the number of the result.

    The number of the result is a negative number.

    `-1` means the last self-diagnostics result.
    `-2` means the result before the last result.
     etc.


    **return:** A result of the self-diagnostics operation in **json** format.
    If the provided id not found, returns the last result.
    """
    return read_id_json(op='self_diagnostics', id=id)


# endregion Operations:

# region Configuration:


@app.put("/ph/conf/set_parameters", tags=["Configuration"],
         response_model=Optional[Union[JobParams, List[JobParams]]])
async def set_parameters(rq: JobParams):
    """
    Set the job parameters. Use these parameters to change the detection thresholds and
    increase or decrease the number of the detected anomalies.

    **Note:** The changes do not survive the pod restarts.

    - **param rq:** The configuration parameters for a job.

    **return:** Configuration parameters of the job or of all jobs including the new values.
    If the job_name parameter is 'all' or missed it returns parameters of all jobs.
    """
    set_envvars(rq)
    rs = get_envvars(rq.job.name, local_jobs)
    return rs


@app.get("/ph/conf/get_parameters/{job_name}", tags=["Configuration"],
         response_model=Optional[Union[JobParams, List[JobParams]]])
def get_parameters(job_name: JobNames = None):
    """
    Returns a list of the current job parameters.

    - **param job_name:** A job name or `all`.

    **return:** Configuration parameters of the job or of all jobs. If the job_name parameter is `all` or missed.
    it returns parameters of all jobs.
    """
    rs = get_envvars(job_name.name, local_jobs)
    return rs


# endregion Configuration:


