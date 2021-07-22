import pytest
import os
from datetime import datetime, timedelta
from time import sleep
from random import randint

from ph.last_timestamp import params, file_name, load, save, remove


def test_params():
    attributes = params._fields
    for at in 'AD_search_interval_minutes'.split(
            ' '):
        assert at in attributes
    assert params
    assert type(params.AD_search_interval_minutes) == int


def test_timestamp_protocol():
    # 1. no timestamp saved; load
    # remove if exist:
    remove()
    assert not os.path.exists(file_name)

    # - load a default value: utcnow() - params.AD_search_interval_minutes*2):
    ts = load()
    assert isinstance(ts, datetime)
    #
    delay = randint(0, 3)
    sleep(delay)
    last_timestamp_now = datetime.utcnow() - timedelta(minutes=params.AD_search_interval_minutes*2)
    assert (last_timestamp_now - ts).seconds == delay

    # 2. save - load:
    last_timestamp_now = datetime.utcnow() - timedelta(minutes=randint(0, 1000000))
    save(last_timestamp_now)
    ts = load()
    assert isinstance(ts, datetime)
    assert ts == last_timestamp_now

    # remove it
    remove()
    assert not os.path.exists(file_name)
