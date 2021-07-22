import logging
import os
from datetime import datetime

from . import elastic_api
from .globals import APP_NAME

logger = logging.getLogger(APP_NAME)


class AlertClient:
    """
    A client class to send alerts to the Elasticsearch
    """
    def __init__(self):
        self.elastic_client = elastic_api.ElasticClient()
        self.removed_fields = os.getenv('PH_ES_FIELDS_NOT_FOR_ALERT', 'host').split(',')
        logger.info(f'Initialized AlertClient.')

    def send_alerts(self, alerts):
        """
        Send alerts to the Elasticsearch.
        It removes fields that can disturb the ES mappings
        Use the PH_ES_FIELDS_NOT_FOR_ALERT environment variable to tune up these removed fields. Encode several fields
        in this variable as string, with ',' as a field separator.
        It also unifies the 'start_time', 'end_time' fields in the alert.record (if presented) to the timestamp format.
        Args:
            alerts: formatted anomalies with description and other alert fields.

        Returns:
            nothing
        """
        if not alerts: return
        for anomaly in alerts:
            anomaly = remove_fields(anomaly, self.removed_fields)
            anomaly = unify_time_format(anomaly, ['start_time', 'end_time'])
            self.elastic_client.write_alert(anomaly)
        logger.info(f'AlertClient: sent {len(alerts):,} alerts with anomalies.')


def unify_time_format(alert, unified_fields):
    """
    Replace unified_fields values with int(value.timestamp()).
    Values can be in the list of predefined datetime formats.
    If format is not found, we use the utcnow().timestamp() instead of the values.
    We unify fields because the ES needs the above format (in our case).
    Args:
        alert: dict. The unified_fields kept inside the alert['record'] dict.
        unified_fields: list of fields that should be unified.

    Returns:
        transformed alert with unified datetime fields.
    """
    if 'record' not in alert:
        return alert
    root_field = alert['record']
    for field_name in unified_fields:
        if field_name not in root_field:
            continue
        field = root_field[field_name]
        if type(field) == int:  # timestamp format, as 1615431612. Do nothing.
            continue
        elif type(field) == str:
            res = int(datetime.utcnow().timestamp())  # if nothing works use now() at last :()
            # formats with Z and T appeared in the python 3.7+ only
            for frmt in ["%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"]:
                try:
                    # strptime doesn't work well with 'T', 'Z' and long msec part.
                    res = int(datetime.strptime(field.replace('T', ' ').replace('Z', '')[:26], frmt).timestamp())
                    break
                except ValueError:
                    continue
            root_field[field_name] = res
    return alert


def remove_fields(alert, removed_fields):
    """
    Removes removed_fields from the 'record' element of the alert.
    Args:
        alert: a dictionary
        removed_fields: a list of the field names

    Returns:
        alert: with removed fields.

    """
    if not alert or 'record' not in alert or not alert['record'] or not removed_fields:
        return alert
    rec_field = alert['record']
    for removed_field in removed_fields:
        if removed_field in rec_field:
            del rec_field[removed_field]
    return alert
