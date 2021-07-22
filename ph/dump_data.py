import json
import uuid
import os
from operator import itemgetter
import sys
import re

import elasticsearch
from elasticsearch import Elasticsearch
from ssl import create_default_context
import pandas as pd
from datetime import datetime
from collections import namedtuple
import argparse

parser = argparse.ArgumentParser()
optional = parser._action_groups.pop()
required = parser.add_argument_group('required arguments')
required.add_argument('--index', type=str, default='',
                    dest='es_index',
                    help='Provide elasticsearch index/index pattern. Example: python3 dump_data.py --index tigera_secure_ee_dns*')

parser._action_groups.append(optional)
args = parser.parse_args()

Params = namedtuple('Params', 'cafile host http_auth log_index alert_index query_size scroll_time bucket_size_minutes')
params = Params(
    os.environ.get('ES_CA_CERT', './elastic_ca.pem'),
    f"{os.getenv('ELASTIC_HOST', 'tigera-secure-es-http.tigera-elasticsearch.svc')}:{os.getenv('ELASTIC_PORT', '9200')}",
    (os.environ.get('ELASTIC_USER'), os.environ.get('ELASTIC_PASSWORD')),
    os.getenv('ES_LOG_INDEX', args.es_index), #'tigera_secure_ee_flows*'), #'tigera_secure_ee_flows.cluster.20210106'),
    os.getenv('ES_ALERT_INDEX', 'tigera_secure_ee_events.cluster'),
    os.getenv('ES_query_size', 5000),
    os.getenv('ES_scroll_time', '20s'),
    os.getenv('ES_bucket_size_minutes', 5),
)
# dump l7 indices
def get_l7_indices(es, f, all_indices, query):
    filter_path = [
    'hits.hits._source.start_time',
    'hits.hits._source.dest_name_aggr',
    'hits.hits._source.dest_namespace',
    'hits.hits._source.dest_service_name',
    'hits.hits._source.duration_max',
    'hits.hits._source.duration_mean',
    'hits.hits._source.src_name_aggr',
    'hits.hits._source.src_namespace',
    '_scroll_id'
    ]
    all_doc_numb = 0
    all_index_docs_numb = 0

    f.write('start_time,dest_name_aggr,dest_namespace,dest_service_name,duration_max,duration_mean,src_name_aggr,src_namespace' + '\n')

    print ('Indices to be dumped: ', all_indices)
    for index in sorted(all_indices, reverse=True):  # start from the last one
        resp = es.search(
            index=index,
            body=query,
            filter_path=filter_path,
            scroll=params.scroll_time  # length of time to keep search context
        )
        old_scroll_id = resp['_scroll_id']
        print(len(resp['hits']['hits']))
        if 'hits' in resp and 'hits' in resp['hits']:
            docs = [el['_source'] for el in resp['hits']['hits']]
            for doc in docs:
                line = str(doc['start_time']) + ',' + str(doc['dest_name_aggr']) + ',' + str(doc['dest_namespace']) + ',' + str(doc['dest_service_name']) \
                    + ',' + str(doc['duration_max']) + ',' + str(doc['duration_mean']) \
                         + ',' + str(doc['src_name_aggr']) + ',' + str(doc['src_namespace'])
                f.write(line + '\n')
            all_doc_numb += len(docs)
        while (
                'hits' in resp
                and 'hits' in resp['hits']
                and resp['hits']['hits']
        ):
            resp = es.scroll(
                scroll_id=old_scroll_id,
                filter_path=filter_path,
                scroll=params.scroll_time  # length of time to keep search context
            )
            if old_scroll_id != resp['_scroll_id']:
                print("NEW SCROLL ID:", resp['_scroll_id'])
            old_scroll_id = resp['_scroll_id']
            if 'hits' in resp and 'hits' in resp['hits']:
                docs = [el['_source'] for el in resp['hits']['hits']]
                for doc in docs:
                    line = str(doc['start_time']) + ',' + str(doc['dest_name_aggr']) + ',' + str(doc['dest_namespace']) + ',' + str(doc['dest_service_name']) \
                    + ',' + str(doc['duration_max']) + ',' + str(doc['duration_mean']) \
                         + ',' + str(doc['src_name_aggr']) + ',' + str(doc['src_namespace'])
                    f.write(line + '\n')
                all_doc_numb += len(docs)
            print (all_doc_numb)
        all_index_docs_numb += all_doc_numb
        print('Total docs saved : ', index, ' :', all_doc_numb)
    print('Total docs saved to data/all_docs.csv: All indices :', all_doc_numb)

# dump dns indices
def get_dns_indices(es, f, all_indices, query):
    filter_path = [
    'hits.hits._source.start_time',
    'hits.hits._source.client_name_aggr',
    'hits.hits._source.client_namespace',
    'hits.hits._source.qtype',
    'hits.hits._source.rcode',
    'hits.hits._source.qname',
    'hits.hits._source.latency_max',
    'hits.hits._source.latency_mean',
    'hits.hits._source.latency_count',
    '_scroll_id'
    ]
    all_doc_numb = 0
    all_index_docs_numb = 0

    f.write('start_time,client_name_aggr,client_namespace,qname,qtype,rcode,latency_count,latency_mean,latency_max' + '\n')

    print ('Indices to be dumped: ', all_indices)
    for index in sorted(all_indices, reverse=True):  # start from the last one
        resp = es.search(
            index=index,
            body=query,
            filter_path=filter_path,
            scroll=params.scroll_time  # length of time to keep search context
        )
        old_scroll_id = resp['_scroll_id']
        print(len(resp['hits']['hits']))
        if 'hits' in resp and 'hits' in resp['hits']:
            docs = [el['_source'] for el in resp['hits']['hits']]
            for doc in docs:
                line = str(doc['start_time']) + ',' + str(doc['client_name_aggr']) + ',' + str(doc['client_namespace']) + ',' + \
                       str(doc['qname']) + ',' + str(doc['qtype']) + ',' + str(doc['rcode']) + ',' + \
                       str(doc['latency_count']) + ',' + str(doc['latency_mean']) + ',' + str(doc['latency_max'])
                f.write(line + '\n')
            all_doc_numb += len(docs)
        while (
                'hits' in resp
                and 'hits' in resp['hits']
                and resp['hits']['hits']
        ):
            resp = es.scroll(
                scroll_id=old_scroll_id,
                filter_path=filter_path,
                scroll=params.scroll_time  # length of time to keep search context
            )
            if old_scroll_id != resp['_scroll_id']:
                print("NEW SCROLL ID:", resp['_scroll_id'])
            old_scroll_id = resp['_scroll_id']
            if 'hits' in resp and 'hits' in resp['hits']:
                docs = [el['_source'] for el in resp['hits']['hits']]
                for doc in docs:
                    line = str(doc['start_time']) + ',' + str(doc['client_name_aggr']) + ',' + str(doc['client_namespace']) + ',' + \
                       str(doc['qname']) + ',' + str(doc['qtype']) + ',' + str(doc['rcode']) + ',' + \
                       str(doc['latency_count']) + ',' + str(doc['latency_mean']) + ',' + str(doc['latency_max'])
                    f.write(line + '\n')
                all_doc_numb += len(docs)
            print (all_doc_numb)
        all_index_docs_numb += all_doc_numb
        print('Total docs saved : ', index, ' :', all_doc_numb)
    print('Total docs saved to data/all_docs.csv: All indices :', all_doc_numb)

# dump flow log indices
def get_flows_indices(es, f, all_indices, query):
    filter_path = [
    # '@timestamp',
    # 'hits.hits._index',
    # 'hits.hits._id',
    'hits.hits._source.start_time',
    # 'hits.hits._source.end_time',
    # 'hits.hits._source.source_name',
    'hits.hits._source.source_name_aggr',
    'hits.hits._source.source_namespace',
    # 'hits.hits._source.source_ip',
    # 'hits.hits._source.dest_name',
    'hits.hits._source.dest_service_name',
    'hits.hits._source.dest_namespace',
    'hits.hits._source.dest_ip',
    'hits.hits._source.dest_port',
    'hits.hits._source.bytes_in',
    'hits.hits._source.bytes_out',
    # 'hits.hits._source.host',
    '_scroll_id',
    ]
    all_doc_numb = 0
    all_index_docs_numb = 0

    f.write('start_time,source_name_aggr,source_namespace,dest_ip,dest_service_name,dest_namespace,dest_port,bytes_in,bytes_out' + '\n')

    print ('Indices to be dumped: ', all_indices)
    for index in sorted(all_indices, reverse=True):  # start from the last one
        resp = es.search(
            index=index,
            body=query,
            filter_path=filter_path,
            scroll=params.scroll_time  # length of time to keep search context
        )
        old_scroll_id = resp['_scroll_id']
        print(len(resp['hits']['hits']))
        if 'hits' in resp and 'hits' in resp['hits']:
            docs = [el['_source'] for el in resp['hits']['hits']]
            for doc in docs:
                line = str(doc['start_time']) + ',' + str(doc['source_name_aggr']) + ',' + str(
                    doc['source_namespace']) + ',' + \
                       str(doc['dest_ip']) + ',' + str(doc['dest_service_name']) + ',' + str(
                    doc['dest_namespace']) + ',' + \
                       str(doc['dest_port']) + ',' + str(doc['bytes_in']) + ',' + str(doc['bytes_out'])
                f.write(line + '\n')
            all_doc_numb += len(docs)
        while (
                'hits' in resp
                and 'hits' in resp['hits']
                and resp['hits']['hits']
        ):
            resp = es.scroll(
                scroll_id=old_scroll_id,
                filter_path=filter_path,
                scroll=params.scroll_time  # length of time to keep search context
            )
            if old_scroll_id != resp['_scroll_id']:
                print("NEW SCROLL ID:", resp['_scroll_id'])
            old_scroll_id = resp['_scroll_id']
            if 'hits' in resp and 'hits' in resp['hits']:
                docs = [el['_source'] for el in resp['hits']['hits']]
                for doc in docs:
                    line = str(doc['start_time']) + ',' + str(doc['source_name_aggr']) + ',' + str(
                        doc['source_namespace']) + ',' + \
                           str(doc['dest_ip']) + ',' + str(doc['dest_service_name']) + ',' + str(
                        doc['dest_namespace']) + ',' + \
                           str(doc['dest_port']) + ',' + str(doc['bytes_in']) + ',' + str(doc['bytes_out'])
                    f.write(line + '\n')
                all_doc_numb += len(docs)
            print (all_doc_numb)
        all_index_docs_numb += all_doc_numb
        print('Total docs saved : ', index, ' :', all_doc_numb)
    print('Total docs saved to data/all_docs.csv: All indices :', all_doc_numb)

if __name__ == "__main__":
    #prevent script from running without parameters
    if args.es_index:
        pass
    else:
        print ('try : python3 dump_data.py -h\ne.g.: python3 dump_data.py --index tigera_secure_ee_flows*')
        sys.exit()
        
    using_ssl = params.cafile is not None and len(params.cafile) > 0
    context = create_default_context(cafile=params.cafile) if using_ssl else None
    host = "https://" + params.host if using_ssl else "http://" + params.host

    es = Elasticsearch(host, ssl_context=context, http_auth=params.http_auth, verify_certs=verify_certs)
    
    try:
        all_indices = es.indices.get_alias(params.log_index)
    except elasticsearch.exceptions.NotFoundError:
        print ("Given index/pattern doesn't exist: ", args.es_index)
        all_indices = es.indices.get_alias('tigera*')
        print ('Available indices:\n', all_indices.keys())
        sys.exit()
    # query to get all records
    query = {'size': 10000, 'query': {'match_all': {}}}

    # save to a file
    f = open('data/all_docs.csv', 'w')

    # dump matched indices
    if re.match("tigera_secure_ee_f.*", args.es_index):
        get_flows_indices(es, f, all_indices, query)
        sys.exit()
    if re.match("tigera_secure_ee_d.*", args.es_index):
        get_dns_indices(es, f, all_indices, query)
        sys.exit()
    if re.match("tigera_secure_ee_l.*", args.es_index):
        get_l7_indices(es, f, all_indices, query)
        sys.exit()
    print ('Indices dump implemented only for:\ntigera_secure_ee_flows*\ntigera_secure_ee_dns*\ntigera_secure_ee_flows*\ntigera_secure_ee_l7*')