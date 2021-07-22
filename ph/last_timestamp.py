import os
from collections import namedtuple
from pathlib import Path
import pickle
from datetime import datetime, timedelta
import logging
from ph.globals import APP_NAME

logger = logging.getLogger(APP_NAME)

file_name = Path('./models') / 'last_timestamp.pkl'

"""
The last_timestamp file is used to store the time of the last detection cycle.
If the detection cycle was not successful then the next cycle runs on the time interval starting
from the last successful detection cycle and we do not miss any anomalies. So in case of the unsuccessful detection 
cycle we do not miss anomalies but only postpone the performance hotspot detection for anomalies from the failed detection time
interval.
If no last_timestamp file existed, we use the doubled PH_search_interval_minutes. 
The files does not exist probably when the app [re]started. To take in account restarting time we double the time.
"""

Params = namedtuple('Params', 'PH_search_interval_minutes')
params = Params(
    int(os.getenv('PH_search_interval_minutes', 30)),
)
logger.info('Initialized params for the last_timestamp.py: ' + ', '.join(
    [f'{n}: {el}' for el, n in zip(params, params._fields)]))


def load():
    """
    Important! we use UTC datetime because logs use it! So it is utcnow() not now()
    """
    if os.path.isfile(file_name):
        with open(file_name, 'rb') as f:
            ts = pickle.load(f)
            logger.info(f'Loaded Last Timestamp {ts} from "{file_name}".')
            assert isinstance(ts, datetime)
            return ts
    else:
        back_minutes = params.PH_search_interval_minutes * 2
        logger.info(f'Last Timestamp is not saved yet. We take {back_minutes} minutes back from now.')
        return datetime.utcnow()-timedelta(minutes=back_minutes)


def save(data):
    with open(file_name, 'wb') as f:
        pickle.dump(data, f, pickle.HIGHEST_PROTOCOL)
        logger.info(f'Saved Last Timestamp {data} into "{file_name}".')


def remove():
    if os.path.exists(file_name):
        os.remove(file_name)
        logger.info(f'Removed the "{file_name}" file.')

