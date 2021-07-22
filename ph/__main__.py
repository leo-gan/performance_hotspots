import os
import logging

from . import ph
from . import api
from .globals import APP_NAME

logger = logging.getLogger(APP_NAME)

if __name__ == "__main__":

    # start API
    PH_NEED_API = eval(os.getenv('PH_NEED_API', 'False'))
    logger.info(f'PH_NEED_API: {PH_NEED_API}')
    if PH_NEED_API:
        api.start()

    # start the job scheduler
    ph.start()
