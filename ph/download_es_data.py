import uuid
import os
from operator import itemgetter

from elasticsearch import Elasticsearch
from ssl import create_default_context
import pandas as pd
from datetime import datetime
from collections import namedtuple

import logging
# from .globals import APP_NAME
APP_NAME = 'asdfasdf'
logging.basicConfig(level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S",
                    format='%(asctime)s : %(levelname)s : %(message)s')
logging.getLogger('elasticsearch').setLevel(logging.CRITICAL)
logger = logging.getLogger(APP_NAME)

Params = namedtuple('Params', 'cafile host http_auth indices query_size scroll_time PH_max_docs')
params = Params(
    os.environ.get('ES_CA_CERT'),
    f"https://{os.getenv('ELASTIC_HOST', 'tigera-secure-es-http.tigera-elasticsearch.svc')}:{os.getenv('ELASTIC_PORT', '9200')}",
    (os.environ.get('ELASTIC_USER'), os.environ.get('ELASTIC_PASSWORD')),
    {
        # 'events': f"tigera_secure_ee_events.{os.getenv('CLUSTER_NAME', 'cluster')}",
        'flows': f"tigera_secure_ee_flows.{os.getenv('CLUSTER_NAME', 'cluster')}.*",
        'l7': f"tigera_secure_ee_l7.{os.getenv('CLUSTER_NAME', 'cluster')}.*",
        'dns': f"tigera_secure_ee_dns.{os.getenv('CLUSTER_NAME', 'cluster')}.*",
    },
    int(os.getenv('ES_query_size', 10000)),
    os.getenv('ES_scroll_time', '20s'),
    # int(os.getenv('ES_bucket_size_minutes', 5)),
    eval(os.getenv('PH_max_docs', 20000)),
    # eval(os.getenv('PH_tests', 'False')),
)

filter_paths = {
    'flows': [
        # '@timestamp',
        # 'hits.hits._index',
        # 'hits.hits._id',
        'hits.hits._source.start_time',
        'hits.hits._source.end_time',

        'hits.hits._source.source_ip',
        'hits.hits._source.source_name',
        'hits.hits._source.source_name_aggr',
        'hits.hits._source.source_namespace',
        'hits.hits._source.source_port',
        'hits.hits._source.source_type',
        'hits.hits._source.source_labels',

        'hits.hits._source.dest_ip',
        'hits.hits._source.dest_name',
        'hits.hits._source.dest_name_aggr',
        'hits.hits._source.dest_namespace',
        'hits.hits._source.dest_service_namespace',
        'hits.hits._source.dest_service_name',
        'hits.hits._source.dest_service_port',
        'hits.hits._source.dest_port',
        'hits.hits._source.dest_type',
        'hits.hits._source.dest_labels',

        'hits.hits._source.proto',
        'hits.hits._source.bytes_in',
        'hits.hits._source.bytes_out',
        'hits.hits._source.num_flows',
        'hits.hits._source.num_flows_started',
        'hits.hits._source.num_flows_completed',
        'hits.hits._source.packets_in',
        'hits.hits._source.packets_out',
        'hits.hits._source.http_requests_allowed_in',
        'hits.hits._source.http_requests_denied_in',
        'hits.hits._source.process_name',
        'hits.hits._source.num_process_names',
        'hits.hits._source.process_id',
        'hits.hits._source.num_process_ids',
        'hits.hits._source.original_source_ips',
        'hits.hits._source.num_original_source_ips',
        'hits.hits._source.host',

        '_scroll_id',
    ],
    'dns': [
        'hits.hits._source.start_time',
        'hits.hits._source.end_time',
        'hits.hits._source.qname',
        'hits.hits._source.client_name_aggr',
        'hits.hits._source.client_namespace',
        'hits.hits._source.source_ip',
        'hits.hits._source.dest_name',
        'hits.hits._source.dest_service_name',
        'hits.hits._source.latency_count',
        'hits.hits._source.latency_mean',
        'hits.hits._source.latency_max',
        'hits.hits._source.host',
        '_scroll_id',
    ],
    'l7': [
        'hits.hits._source.start_time',
        'hits.hits._source.end_time',

        'hits.hits._source.duration_mean',
        'hits.hits._source.duration_max',
        'hits.hits._source.bytes_in',
        'hits.hits._source.bytes_out',
        'hits.hits._source.count',

        'hits.hits._source.src_namespace',
        'hits.hits._source.src_name_aggr',
        'hits.hits._source.src_type',

        'hits.hits._source.dest_service_name',
        'hits.hits._source.dest_service_namespace',
        'hits.hits._source.dest_service_port',
        'hits.hits._source.dest_name_aggr',
        'hits.hits._source.dest_namespace',
        'hits.hits._source.dest_type',

        'hits.hits._source.method',
        'hits.hits._source.user_agent',
        'hits.hits._source.url',
        'hits.hits._source.response_code',
        'hits.hits._source.type',
        'hits.hits._source.host',

        '_scroll_id',
    ],
}


class ElasticClient:
    def __init__(self):
        logger.info('Initialized ElasticClient with params: ' + ', '.join(
            [f'{n}: {el if n != "http_auth" else "..."}' for el, n in zip(params, params._fields)]))
        self.log_original_file_name = 'data/log_original.csv'

        using_ssl = params.cafile is not None and len(params.cafile) > 0
        context = create_default_context(cafile=params.cafile) if using_ssl else None
        host = "https://" + params.host if using_ssl else "http://" + params.host
        
        self.es = Elasticsearch(host, ssl_context=context, http_auth=params.http_auth, verify_certs=using_ssl)

    def download_data(self, start_time=None, end_time=None, max_docs=500000):
        def format_dt(dt):
            t = datetime.strptime(dt, '%Y-%m-%d %H:%M:%S') if type(dt) == str else dt
            return t.isoformat()  # requirement for Elasticsearch ?

        query = {"size": params.query_size}
        if not any([start_time, end_time]):
            query['query'] = {"match_all": {}}
        else:
            query['query'] = {'range': {"@timestamp": {}}}
            if start_time:
                query['query']['range']["@timestamp"]["gte"] = format_dt(start_time)
            if end_time:
                query['query']['range']["@timestamp"]["lt"] = format_dt(end_time)

        data_indices = {k: v for k, v in params.indices.items() if k != 'events'}
        for index_name, index in data_indices.items():
            all_docs = []
            resp = self.es.search(
                index=index,
                body=query,
                filter_path=filter_paths[index_name],
                scroll=params.scroll_time  # length of time to keep search context
            )
            if not resp or '_scroll_id' not in resp:  # empty index!
                logger.info(f'Index "{index}" empty.')
                continue
            old_scroll_id = resp['_scroll_id']
            first_resp = True
            while (
                    'hits' in resp
                    and 'hits' in resp['hits']
                    and resp['hits']['hits']
                    and len(all_docs) < max_docs
            ):
                if not first_resp:
                    resp = self.es.scroll(
                        scroll_id=old_scroll_id,
                        filter_path=filter_paths[index_name],
                        scroll=params.scroll_time  # length of time to keep search context
                    )
                first_resp = False
                if old_scroll_id != resp['_scroll_id']:
                    logger.error("*** NEW SCROLL ID:", resp['_scroll_id'])
                old_scroll_id = resp['_scroll_id']

                if 'hits' in resp and 'hits' in resp['hits']:
                    docs = [el['_source'] for el in resp['hits']['hits']]
                    all_docs += docs
                    logger.info(
                        f'  Downloaded {len(docs):,} samples from the "{index}" index.')
            logger.info(
                f'Downloaded {len(all_docs):,} samples from the [{index}] index.')
            self._save_data(all_docs, index_name)
        return

    @staticmethod
    def _save_data(dct_lst, name):
        if not dct_lst: return
        suffix = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        cluster_name = os.getenv('CLUSTER_NAME', 'cluster')
        file = f'../data/{cluster_name}.{name}.{int(len(dct_lst)/1000)}K.{suffix}.csv'
        pd.DataFrame(dct_lst).to_csv(file, index=False)
        logger.info(f'Saved {len(dct_lst):,} into "{file}"')
        return


"""
Use it to download the ES indexes. 
Data saved in files with the cluster name, the index name, the date-time, and the number of rows encoded in the file name.
"""
if __name__ == "__main__":
    es_client = ElasticClient()
    es_client.download_data(max_docs=1000)
