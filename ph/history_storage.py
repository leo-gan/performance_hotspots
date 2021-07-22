from datetime import datetime
import os
from .globals import data_dir, APP_NAME
import logging
import jsonlines
import uuid

logger = logging.getLogger(APP_NAME)

PH_HISTORY_RETENTION_NUMBER = int(os.getenv("PH_HISTORY_RETENTION_NUMBER", 100))


def get_file_name(op):
    return f'{data_dir}/{op}.jsonl'


def save_json_in_line(op: str, job: str, js: dict, id: str = None):
    """
    Save a json into the history file for an operation and a job.
    Save only the last PH_HISTORY_RETENTION_NUMBER elements.
    @param op: an operation, saved as a file name part. It means several independent files, one file per operation.
    @param job: a name of the job
    @param js: a dictionary (json) to be saved as the 'result' field.
    @param id: an id of the saved record. If None, generate uuid as id.
    @return: id of the saved record.
    """
    file_name = get_file_name(op)
    if not id:
        id = str(uuid.uuid4())
    new_item = {
        'id': id,
        'time': datetime.utcnow().isoformat(),
        'job': job,
        'result': js
    }

    existed_items = read_job_jsons(op) # all json-s
    if existed_items and len(existed_items) >= PH_HISTORY_RETENTION_NUMBER:
        # remove first (oldest) elements
        existed_items = existed_items[-PH_HISTORY_RETENTION_NUMBER:]

    with jsonlines.open(file_name, 'w') as writer:
        for item in existed_items + [new_item]:
            writer.write(item)
    logger.info(f'Saved [op: "{op}", job: "{job}", id: {id}] result into "{file_name}"')
    return id


def read_id_json(op: str, id: str = '-1'):
    """
    Read a json from a file.
    @param op: an operation, saved as a file name part. It means several independent files, one file per operation.
    @param id: id of the previously saved dictionary.
    @return: a dictionary. Can be {} if didn't find any.
    """
    file_name = get_file_name(op)
    if not os.path.exists(file_name):
        return {}
    with jsonlines.open(file_name) as reader:
        records = list(reader)

    if not records:
        return {}

    # if id is an int, we assume id < 0 (a record index back from the tail of the list)
    # any other non-int id we treat as the uuid.
    # if uuid didn't found, we use -1 index.
    # try:
    #     back_id = int(id)
    #     if back_id >= 0:
    #         back_id = -1
    # except (ValueError, TypeError):
    back_id = -1
    if str(id).lstrip('-+').isnumeric():
        back_id = int(id)

    for js in records:
        if str(js['id']) == id:
            return js
    return records[back_id]


def read_job_jsons(op: str, job: str = None):
    """
    Read json-s from a file.
    @param op: an operation, saved as a file name part. It means several independent files, one file per operation.
    @param job: a job name.
    @return: a List[json] if id is None. Can be [] if didn't find any.
    """
    file_name = get_file_name(op)
    if not os.path.exists(file_name):
        return []
    with jsonlines.open(file_name) as reader:
        if not job:
            return list(reader)
        else:
            return [el for el in reader if el['job'] == job]
