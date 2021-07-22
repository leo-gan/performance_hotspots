from datetime import datetime
from enum import Enum
from typing import Optional, List, Union

from pydantic import BaseModel, Field

from ph.globals import jobs, logs

JobNames = Enum('JobNames', [(j.name, j.name) for j in jobs()] + [('all', 'all')])
LogNames = Enum('LogNames', [(log, log) for log in logs])
DataSources = Enum('DataSources', [(d, d) for d in ['request', 'logs', 'test_dataset']])


class DtInterval(BaseModel):
    """A datetime interval. Can be open with either side."""
    start: Optional[datetime] = Field(
        None,
        title='A **start** of the datetime interval.',
        description='A missed value means the limit of data defined only by the '
                    '`max_log_records` parameter.')
    end: Optional[datetime] = Field(
        None,
        title='An **end** of the datetime interval.',
        description='A missed value means the last possible datetime.')


class LogData(BaseModel):
    """Data copied from one of the log."""
    log_name: LogNames = Field(
        ...,
        title='A name of the log (which is effectively an Elasticsearch index).',
        description=f'Any of {logs} values')
    records: List[dict] = Field(
        ...,
        title='A list of the log records.',
        description='Each log has a specific format and the records presented as the dictionaries.')


class OperationDataRq(BaseModel):
    """A request for the training or detection operations"""
    job: JobNames = Field(
        ...,
        title='A name of the performance hotspot detector.',
        description='`all` means all jobs. `all` can be used only with the `logs` **data_source**.')
    data_source: DataSources = Field(
        ...,
        title='A source of data for training or detection.',
        description='`logs` - data downloaded from the Elasticsearch indexes. '
                    '`request` - data provided in the "data" field of this request.'
                    '`test_dataset` - data is copied from the internal **[job].test_dataset.csv** file.')
    data: Optional[Union[LogData, DtInterval]] = Field(
        None,
        title='The data used for training or detection.',
        description='It can be the log records OR a datetime interval OR can be missed. '
                    'Log records work for the `request` data_source. '
                    'Log records can hold records only from one log. '
                    'A datetime interval works for the `logs` data_source.'
                    'A datetime interval defines log records to be downloaded '
                    'from the Elasticsearch from the job-related log (index). '
                    'Missed `data` field works for the `test_dataset` data_source. '
                    '`all` job is not permitted with  missed `data` field.')
    max_log_records: Optional[int] = Field(
        None,
        title='A maximum number of the log records used for training or detection.',
        description='If the field missed, the `PH_max_docs` env variable value used instead.')

    class Config:
        schema_extra = {
            "example":
                {
                    "job": "port_scan",
                    "data_source": "test_dataset"
                }
        }


class Anomaly(BaseModel):
    """
    An anomaly detected by the job.
    """
    job: JobNames
    description: str
    time: Union[int, str, datetime]
    data: dict


class JobParams(BaseModel):
    """
    Configuration parameters of the job. Some of them used only for training or detection,
    some for both operations.
    """
    job: JobNames
    params: dict
