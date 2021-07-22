# import json
import uuid
import os
from operator import itemgetter

from elasticsearch import Elasticsearch
from ssl import create_default_context
import pandas as pd
from datetime import datetime
from collections import namedtuple

import logging
from .globals import APP_NAME

logger = logging.getLogger(APP_NAME)
logging.getLogger('elasticsearch').setLevel(logging.CRITICAL)

Params = namedtuple('Params', 'cafile host http_auth indices query_size scroll_time bucket_size_minutes debug')
params = Params(
    os.environ.get('ES_CA_CERT'),
    f"{os.getenv('ELASTIC_HOST', 'tigera-secure-es-http.tigera-elasticsearch.svc')}:{os.getenv('ELASTIC_PORT', '9200')}",
    (os.environ.get('ELASTIC_USER'), os.environ.get('ELASTIC_PASSWORD')),
    {
        'events': f"tigera_secure_ee_events.{os.getenv('CLUSTER_NAME', 'cluster')}",
        'flows': f"tigera_secure_ee_flows.{os.getenv('CLUSTER_NAME', 'cluster')}.*",
        'l7': f"tigera_secure_ee_l7.{os.getenv('CLUSTER_NAME', 'cluster')}.*",
        'dns': f"tigera_secure_ee_dns.{os.getenv('CLUSTER_NAME', 'cluster')}.*",
    },
    int(os.getenv('ES_query_size', 10000)),
    os.getenv('ES_scroll_time', '20s'),
    int(os.getenv('ES_bucket_size_minutes', 5)),
    False,  # if True, save the download index as a file
)

filter_path_flows = [
    # '@timestamp',
    # 'hits.hits._index',
    # 'hits.hits._id',
    'hits.hits._source.start_time',
    # 'hits.hits._source.end_time',

    # 'hits.hits._source.source_ip',
    # 'hits.hits._source.source_name',
    'hits.hits._source.source_name_aggr',
    'hits.hits._source.source_namespace',
    # 'hits.hits._source.source_port',
    'hits.hits._source.source_type',
    # 'hits.hits._source.source_labels',

    'hits.hits._source.dest_ip',
    'hits.hits._source.dest_name',
    'hits.hits._source.dest_name_aggr',
    'hits.hits._source.dest_namespace',
    'hits.hits._source.dest_service_namespace',
    'hits.hits._source.dest_service_name',
    'hits.hits._source.dest_service_port',
    'hits.hits._source.dest_port',
    'hits.hits._source.dest_type',
    # 'hits.hits._source.dest_labels',

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
    # 'hits.hits._source.original_source_ips',
    'hits.hits._source.num_original_source_ips',
    'hits.hits._source.host',

    '_scroll_id',
]
filter_path_dns_latency = [
    'hits.hits._source.start_time',
    'hits.hits._source.end_time',
    'hits.hits._source.qname',
    'hits.hits._source.qtype',
    'hits.hits._source.rcode',
    'hits.hits._source.client_name_aggr',
    'hits.hits._source.client_namespace',
    # 'hits.hits._source.source_ip',
    # 'hits.hits._source.dest_name',
    # 'hits.hits._source.dest_service_name',
    'hits.hits._source.latency_count',
    'hits.hits._source.latency_mean',
    'hits.hits._source.latency_max',
    # 'hits.hits._source.host',
    '_scroll_id',
]
filter_path_l7_latency = [
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
]
filter_paths = {
    # 'events': filter_path, # read operation (hence filter) is not applicable
    'flows': filter_path_flows,
    'l7': filter_path_l7_latency,
    'dns': filter_path_dns_latency,
}


def _aggregate_bucket(rows, bucket_start):
    if not rows: return []
    start_time = str(datetime.fromtimestamp(bucket_start))
    source_name_aggr2source_namespace = {}
    source_name_aggr2dest_ips = {}
    source_name_aggr2dest_ports = {}
    source_name_aggr2bytes_out = {}

    dest_service_name2dest_namespace = {}
    dest_service_name2bytes_in = {}

    for el in rows:
        if el['source_name_aggr'] not in source_name_aggr2source_namespace:
            # assume name_aggr_to_namespace is M_to_1
            source_name_aggr2source_namespace[el['source_name_aggr']] = el['source_namespace']
        if el['source_name_aggr'] not in source_name_aggr2dest_ips:
            source_name_aggr2dest_ips[el['source_name_aggr']] = set()
        if el['dest_ip']:  # can be None
            source_name_aggr2dest_ips[el['source_name_aggr']].add(el['dest_ip'])
        if el['source_name_aggr'] not in source_name_aggr2dest_ports:
            source_name_aggr2dest_ports[el['source_name_aggr']] = set()
        source_name_aggr2dest_ports[el['source_name_aggr']].add(el['dest_port'])
        if el['source_name_aggr'] not in source_name_aggr2bytes_out:
            source_name_aggr2bytes_out[el['source_name_aggr']] = 0
        source_name_aggr2bytes_out[el['source_name_aggr']] += el['bytes_out']

        if el['dest_service_name'] not in dest_service_name2dest_namespace:
            # assume name_aggr_to_namespace is M_to_1
            dest_service_name2dest_namespace[el['dest_service_name']] = el['dest_namespace']
        if el['dest_service_name'] not in dest_service_name2bytes_in:
            dest_service_name2bytes_in[el['dest_service_name']] = 0
        dest_service_name2bytes_in[el['dest_service_name']] += el['bytes_in']

    source_name_aggr_all = sorted(list(set(
        list(source_name_aggr2source_namespace)
        + list(source_name_aggr2dest_ips)
        + list(source_name_aggr2dest_ports)
        + list(source_name_aggr2bytes_out)
    )))
    dest_service_name_all = sorted(list(set(
        list(dest_service_name2dest_namespace)
        + list(dest_service_name2bytes_in)
    )))
    source_samples = [{
        'start_time': start_time,
        'source_namespace': source_name_aggr2source_namespace[
            source_name_aggr] if source_name_aggr in source_name_aggr2source_namespace else '',
        'source_name_aggr': source_name_aggr,
        'unique_dest_ip_number': len(
            source_name_aggr2dest_ips[source_name_aggr]) if source_name_aggr in source_name_aggr2dest_ips else 0,
        'unique_dest_port_number': len(source_name_aggr2dest_ports[
                                           source_name_aggr]) if source_name_aggr in source_name_aggr2dest_ports else 0,
        'bytes_out': source_name_aggr2bytes_out[
            source_name_aggr] if source_name_aggr in source_name_aggr2bytes_out else 0,

    } for source_name_aggr in source_name_aggr_all]

    dest_samples = [{
        'start_time': start_time,
        'dest_namespace': dest_service_name2dest_namespace[
            dest_service_name] if dest_service_name in dest_service_name2dest_namespace else '',
        'dest_service_name': dest_service_name,
        'bytes_in': dest_service_name2bytes_in[
            dest_service_name] if dest_service_name in dest_service_name2bytes_in else 0,

    } for dest_service_name in dest_service_name_all]
    return source_samples, dest_samples


def _additional_aggregation(aggregated_source_samples, aggregated_dest_samples):
    """some buckets are split into several rows.
    We have to aggregate them into a single row."""
    ss = {}
    for s in aggregated_source_samples:
        k = f'{s["start_time"]}_{s["source_namespace"]}_{s["source_name_aggr"]}'
        if k not in ss:
            ss[k] = s
        else:
            ss[k]['unique_dest_ip_number'] += s['unique_dest_ip_number']
            ss[k]['unique_dest_port_number'] += s['unique_dest_port_number']
            ss[k]['bytes_out'] += s['bytes_out']
    source_out = sorted(ss.values(), key=lambda s: s["start_time"])

    ss = {}
    for s in aggregated_dest_samples:
        k = f'{s["start_time"]}_{s["dest_namespace"]}_{s["dest_service_name"]}'
        if k not in ss:
            ss[k] = s
        else:
            ss[k]['bytes_in'] += s['bytes_in']
    dest_out = sorted(ss.values(), key=lambda s: s["start_time"])
    return source_out, dest_out


def _aggregate_data(docs, bucket_size_minutes):
    if not docs: return [], []
    aggregated_source_samples, aggregated_dest_samples = [], []

    docs = sorted(docs, key=itemgetter('start_time'))
    bucket_size_secs = bucket_size_minutes * 60
    # adjust a bucket_start to an interval
    bucket, bucket_start = [], docs[0]['start_time'] - (docs[0]['start_time'] % bucket_size_secs)
    for i, el in enumerate(docs):
        if el['start_time'] >= bucket_start + bucket_size_secs:
            source_samples, dest_samples = _aggregate_bucket(bucket, bucket_start)
            aggregated_source_samples += source_samples
            aggregated_dest_samples += dest_samples
            bucket, bucket_start = [], el['start_time'] - (el['start_time'] % bucket_size_secs)
        elif el['start_time'] < bucket_start:
            logger.error(i, '**** Should never happen!')
            logger.error('  ', i, bucket_start, el['start_time'], docs[i + 1]['start_time'])
        bucket.append(el)
    source_samples, dest_samples = _aggregate_bucket(bucket, bucket_start)
    aggregated_source_samples += source_samples
    aggregated_dest_samples += dest_samples
    return aggregated_source_samples, aggregated_dest_samples


def aggregate_samples(docs):
    """
    Returns: {'source': source_docs, 'dest': dest_docs}
    Several models require that the time bucket aggregation is precise in terms of the time bucket start-ends
    (say, 0, 5, 10, 15, ...).
    The original data in logs aggregated but the time bucket starts-ends are variable (say, 0, 4, 6, 6, 12, 14, ...).
    """
    source_docs, dest_docs = _aggregate_data(docs, params.bucket_size_minutes)
    source_docs, dest_docs = _additional_aggregation(source_docs, dest_docs)
    logger.info(
        f'Aggregated {len(docs):,} into source: {len(source_docs):,} and  dest: {len(dest_docs):,}.')
    return {'source': source_docs, 'dest': dest_docs}


class ElasticClient:
    def __init__(self):
        logger.info('Initialized ElasticClient with params: ' + ', '.join(
            [f'{n}: {el if n != "http_auth" else "..."}' for el, n in zip(params, params._fields)]))
        using_ssl = params.cafile is not None and len(params.cafile) > 0
        context = create_default_context(cafile=params.cafile) if using_ssl else None
        host = "https://" + params.host if using_ssl else "http://" + params.host

        self.es = Elasticsearch(host, http_auth=params.http_auth, ssl_context=context, verify_certs=using_ssl)

    def write_alert(self, alert):
        return self.es.create(index=params.indices['events'], id=str(uuid.uuid4()), body=alert, doc_type="_doc")

    def download_and_aggregate_data(self, start_time, end_time, max_docs=500000, index_name=None):
        """
        Downloads the ES data from one index in pages with params.query_size size and
        within [start_time, end_time] interval.
        If index_name==None, download all indexes!
        Returns: {'flows': all_flow_docs, 'source': all_source_aggr, 'dest': all_dest_aggr,
        'l7': all_l7_docs, 'dns': all_dns_docs}
        """
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
        logger.info(f'Start downloading ES data: start_time: {start_time} - end_time: {end_time}, max_docs: {max_docs}')
        data_indices = {k: v for k, v in params.indices.items() if k != 'events'}
        if index_name:
            data_indices = {k: v for k, v in params.indices.items() if k == index_name}
        data_type2docs = {}
        for index_name, index in data_indices.items():
            index_data_dict = self._download_and_aggregate_index(index_name, index, query, max_docs=max_docs)
            data_type2docs = {**data_type2docs, **index_data_dict}
        return data_type2docs

    def _download_and_aggregate_index(self, index_name, index, query, max_docs):
        """
        Downloads the ES data from one index in pages.
        The 'flow' index data immediately aggregated by 'source' and 'dest' groups in the time buckets.
        Several models require that the time bucket aggregation is precise in terms of the time bucket start-ends
        (say, 0, 5, 10, 15, ...).
        The original logs aggregate data but the time bucket starts-ends are variable (say, 0, 4, 6, 6, 12, 14, ...).
        If params.debug==True, all downloaded data saved into the files, that can be used for debugging.
        Returns: {'<index_name>': index_records}
          for 'flows' index : {'flows': flow_records, 'source': source_aggr_records, 'dest': dest_aggr_records}
        """
        scroll_id = '_scroll_id'
        resp = self.es.search(
            index=index,
            body=query,
            filter_path=filter_paths[index_name],
            scroll=params.scroll_time  # length of time to keep search context
        )
        if not resp or scroll_id not in resp: # empty index!
            logger.info(f'Index "{index}" empty.')
            return {}
        old_scroll_id = resp[scroll_id]

        all_docs = []
        all_source_docs, all_dest_docs = [], []
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
            if old_scroll_id != resp[scroll_id]:
                logger.error("*** NEW SCROLL ID:", resp[scroll_id])
            old_scroll_id = resp[scroll_id]

            if 'hits' in resp and 'hits' in resp['hits']:
                docs = [el['_source'] for el in resp['hits']['hits']]
                all_docs += docs
                logger.info(f'  Downloaded {len(docs):,} -> {len(all_docs):,} "{index_name}" samples from the "{index}" index.')
                if index_name == 'flows':
                    source_docs, dest_docs = _aggregate_data(docs,  params.bucket_size_minutes)
                    all_source_docs += source_docs
                    all_dest_docs += dest_docs
                    logger.info(
                        f'    Aggregated data:: source: {len(source_docs):,} -> {len(all_source_docs):,}, '
                        f'dest: {len(dest_docs):,} -> {len(all_dest_docs):,}.')

        logger.info(f'Downloaded {len(all_docs):,} "{index_name}" samples from the "{index}" index.')
        data_type2docs = {index_name: all_docs}

        all_source_docs, all_dest_docs = _additional_aggregation(all_source_docs, all_dest_docs)
        if index_name == 'flows':
            data_type2docs['source'] = all_source_docs
            data_type2docs['dest'] = all_dest_docs
            logger.info(f'Aggregated data {len(all_source_docs):,} source, {len(all_dest_docs):,} dest samples.')

        if params.debug:
            suffix = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            self._save_data(all_docs, index_name, suffix)
            if index_name == 'flows':
                self._save_data(all_source_docs, 'source', suffix)
                self._save_data(all_dest_docs, 'dest', suffix)
        return data_type2docs

    @staticmethod
    def _save_data(dct_lst, name, suffix):
        if not dct_lst: return
        file = f'./data/{name}.{suffix}.csv'
        pd.DataFrame(dct_lst).to_csv(file, index=False)
        logger.info(f'Saved {len(dct_lst):,} into "{file}"')
        return

