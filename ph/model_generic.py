import numpy as np
import pandas as pd
from operator import itemgetter
from datetime import timedelta, datetime

from sklearn.ensemble import IsolationForest

import logging
from .globals import APP_NAME, get_param

logger = logging.getLogger(APP_NAME)


class L7LatencyModel:
    def __init__(self):
        self.job_name = 'l7_latency'
        self.value_field = 'duration_mean'
        self.model = IsolationForest(n_estimators=get_param(self.job_name, 'PH_L7Latency_IsolationForest_n_estimators', param_type=int), random_state=0)
        self.model_name = 'sklearn.ensemble.IsolationForest'
        logger.info(f'Initialized L7LatencyModel as the "{self.model_name}",')

    def train(self, samples):
        """
        returns: model
        All samples used without any grouping.
        The duration_mean works better than duration_max in this detection.
        """
        logger.info(f'    L7LatencyModel: Start training the {self.model_name} model.')
        x = np.array([s[self.value_field] for s in samples if self.value_field in s]).reshape(-1, 1)
        self.model.fit(x)
        aggregators = self._calc_aggregators(samples)
        logger.info(f'    L7LatencyModel: Stop training the {self.model_name} model.')
        return self.model, aggregators

    def find_anomalies(self, model, samples, aggregators):
        logger.info(f'    L7LatencyModel: Start performance hotspot detection with {self.model_name} model.')
        x = np.array([s[self.value_field] for s in samples if self.value_field in s]).reshape(-1, 1)
        scores = model.score_samples(x)
        assert len(scores) == len(samples)
        anomaly_samples = [{**sample, 'score': score} for sample, score in zip(samples, scores, )
                           if score < get_param(self.job_name, 'PH_L7Latency_IsolationForest_score_threshold', param_type=float)]
        anomaly_samples = self._format_anomalies(anomaly_samples, aggregators)
        logger.info(f'    L7LatencyModel: Detected {len(anomaly_samples):,} anomalies with {self.model_name} model.')
        return anomaly_samples

    def _format_anomalies(self, anomalies, aggregators):
        def calc_confidence(score, score_max):
            return round(min(0.99, abs(score / score_max)), 2)

        if not anomalies:  return []
        alert_name = "anomaly_detection.l7_latency"
        cur_time = int(datetime.utcnow().timestamp())
        agg_str = f' The average value for this latency is {int(aggregators["mean"]/1000):,} μs.' if 'mean' in aggregators else ''
        score_max = min([a['score'] for a in anomalies])  # negative values
        anomalies = sorted(anomalies, key=itemgetter('start_time'))
        return [
            {
                "type": "alert",
                "alert": alert_name,
                "severity": 100,
                "record": {
                    **a,
                    "confidence": calc_confidence(a['score'], score_max),
                },
                "description": f"[{alert_name}] {a['src_namespace']}/{a['src_name_aggr']} has a suspicious latency "
                    f"of {int(a[self.value_field]/1000):,} μs "
                    f"with {calc_confidence(a['score'], score_max):.0%} confidence.{agg_str}",
                "time": cur_time
            } for a in anomalies]

    def _calc_aggregators(self, samples):
        """
        Calculate aggregators of the value_field: sum, count, mean, etc.
        returns: {'sum': 22.0, 'min': 1.0, 'count': 3.0, 'max': 17.0, 'mean': 7.333333333, 'std': 8.504900548}
        """
        df = pd.DataFrame(samples)
        return df.agg({self.value_field: ['count', 'sum', 'min', 'mean', 'max', 'std']}).to_dict()[self.value_field]
