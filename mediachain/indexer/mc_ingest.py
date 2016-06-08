#!/usr/bin/env python
# -*- coding: utf-8 -*-

__doc__ = \
"""
Functions for ingestion of media files into Indexer.

Potential sources include:
- Mediachain blockchain.
- Getty dumps.
- Other media sources.

Scraping / downloading functions also contained here.

Later may be extended to insert media that comes from off-chain into the chain.
"""

from mc_generic import setup_main, group, raw_input_enter, pretty_print

import mc_config

import mc_datasets

from time import sleep
import json
import os
from os.path import exists, join
from os import mkdir, listdir, walk, unlink
from Queue import Queue
from threading import current_thread,Thread

import requests
from random import shuffle
from shutil import copyfile
import sys
from sys import exit

from datetime import datetime
from dateutil import parser as date_parser
from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk
from hashlib import md5

from PIL import Image
from cStringIO import StringIO

import binascii
import base64

import numpy as np

import imagehash
import itertools

    
data_pat = 'data:image/jpeg;base64,'
data_pat_2 = 'data:image/png;base64,'
    
def shrink_and_encode_image(s, size = (150, 150)):
    """
    Resize image to small size & base64 encode it.
    """
    
    img = Image.open(StringIO(s))
    
    if (img.size[0] > size[0]) or (img.size[1] > size[1]):
        f2 = StringIO()
        img.thumbnail(size, Image.ANTIALIAS)
        img.save(f2, "JPEG")
        f2.seek(0)
        s = f2.read()
    
    return data_pat + base64.urlsafe_b64encode(s)

def decode_image(s):

    if s.startswith(data_pat):
        ss = s[len(data_pat):]
        
    elif s.startswith(data_pat_2):
        ss = s[len(data_pat_2):]
        
    else:
        assert False,('BAD_DATA_URL',s[:15])
        
    return base64.urlsafe_b64decode(ss)


def es_connect():
    print ('CONNECTING...')
    es = Elasticsearch()
    print ('CONNECTED')
    return es

        
def ingest_bulk(iter_json = False,
                thread_count = 1,
                index_name = mc_config.MC_INDEX_NAME,
                doc_type = mc_config.MC_DOC_TYPE,
                search_after = False,
                redo_thumbs = True,
                ignore_thumbs = False,
                ):
    """
    Ingest Getty dumps from JSON files.

    Currently does not attempt to import media to the Mediachain chain.
    
    Args:
        iter_json:     Iterable of media objects, with `img_data` containing the raw-bytes image data.
        thread_count:  Number of parallel threads to use for ES insertion.
        index_name:    ES index name to use.
        doc_type:      ES document type to use.
        search_after:  Manually inspect ingested records after. Probably not needed anymore.
        redo_thumbs:   Whether to recalcuate 'image_thumb' from 'img_data'.
        ignore_thumbs: Whether to ignore thumbnail generation entirely.

    Returns:
        Number of inserted records.

    Examples:
        See `mc_test.py`
    """

    if not iter_json:
        iter_json = mc_datasets.iter_json_getty(index_name = index_name,
                                                doc_type = doc_type,
                                                )
    
    es = es_connect()
    
    if es.indices.exists(index_name):
        print ('DELETE_INDEX...', index_name)
        es.indices.delete(index = index_name)
        print ('DELETED')
            
    print ('CREATE_INDEX...',index_name)
    es.indices.create(index = index_name,
                      body = {'settings': {'number_of_shards': mc_config.MC_NUMBER_OF_SHARDS,
                                           'number_of_replicas': mc_config.MC_NUMBER_OF_REPLICAS,                             
                                           },
                              'mappings': {doc_type: {'properties': {'title':{'type':'string'},
                                                                     'artist':{'type':'string'},
                                                                     'collection_name':{'type':'string'},
                                                                     'caption':{'type':'string'},
                                                                     'editorial_source':{'type':'string'},
                                                                     'keywords':{'type':'string', 'index':'not_analyzed'},
                                                                     'created_date':{'type':'date'},
                                                                     'image_thumb':{'type':'string', 'index':'no'},
                                                                     'dedupe_hsh':{'type':'string', 'index':'not_analyzed'},
                                                                     },
                                                      },
                                           },
                              },
                      #ignore = 400, # ignore already existing index
                      )
    
    print('CREATED',index_name)
    
    print('INSERTING...')

    def iter_wrap():
        # Put in parallel_bulk() format:
        
        for hh in iter_json:
            
            xdoc = {'_op_type': 'index',
                    '_index': index_name,
                    '_type': doc_type,
                    }
            
            hh.update(xdoc)

            if not ignore_thumbs:
                if redo_thumbs:
                    # Check existing thumbs meet size & format requirements:

                    if 'img_data' in hh:
                        hh['image_thumb'] = shrink_and_encode_image(decode_image(hh['img_data']))

                    elif 'image_thumb' in hh:
                        hh['image_thumb'] = shrink_and_encode_image(decode_image(hh['image_thumb']))

                    else:
                        assert False,'CANT_GENERATE_THUMBNAILS'

                elif 'image_thumb' not in hh:
                    # Generate thumbs from raw data:

                    if 'img_data' in hh:
                        hh['image_thumb'] = shrink_and_encode_image(decode_image(hh['img_data']))

                    else:
                        assert False,'CANT_GENERATE_THUMBNAILS'

                if 'img_data' in hh:
                    del hh['img_data']

                yield hh
    
    gen = iter_wrap()

    # TODO: parallel_bulk silently eats exceptions. Here's a quick hack to watch for errors:
        
    first = gen.next()
    
    for is_success,res in parallel_bulk(es,
                                        itertools.chain([first], gen),
                                        thread_count = thread_count,
                                        chunk_size = 500,
                                        max_chunk_bytes = 100 * 1024 * 1024, #100MB
                                        ):
        """
        #FORMAT:
        (True,
            {u'index': {u'_id': u'getty_100113781',
                        u'_index': u'getty_test',
                        u'_shards': {u'failed': 0, u'successful': 1, u'total': 1},
                        u'_type': u'image',
                        u'_version': 1,
                        u'status': 201}})
        """
        pass
        
    print ('REFRESHING', index_name)
    es.indices.refresh(index = index_name)
    print ('REFRESHED')
    
    if search_after:
        
        print ('SEARCH...')
        
        q_body = {"query": {'match_all': {}}}
        
        #q_body = {"query" : {"constant_score":{"filter":{"term":
        #                        { "dedupe_hsh" : '87abc00064dc7e780e0683110488a620e9503ceb9bfccd8632d39823fffcffff'}}}}}

        q_body['from'] = 0
        q_body['size'] = 1

        print ('CLUSTER_STATE:')
        print pretty_print(es.cluster.state())

        print ('QUERY:',repr(q_body))

        res = es.search(index = index_name,
                        body = q_body,
                        )

        print ('RESULTS:', res['hits']['total'])

        #print (res['hits']['hits'])

        for hit in res['hits']['hits']:

            doc = hit['_source']#['doc']

            if 'image_thumb' in doc:
                doc['image_thumb'] = '<removed>'

            print 'HIT:'
            print pretty_print(doc)

            raw_input_enter()


    return es.count(index_name)['count']



def ingest_bulk_blockchain(host,
                           port,
                           object_id,
                           ):
    """
    Ingest media from Mediachain blockchain.
    Args:
        host:       Host.
        port:       Port.
        object_id:  ID of the artefact/entity to fetch.
        index_name: Name of Indexer index to populate.
        doc_type:   Name of Indexer doc type.
    Looking at `mediachain-client/mediachain.reader.api.get_object_chain` as the main API call? 
    """

    assert False,'TODO - WIP stub, not ready yet. '
    
    from mediachain.reader import api
    
    aws = {'aws_access_key_id':mc_config.MC_AWS_ACCESS_KEY_ID,
           'aws_secret_access_key':mc_config.MC_AWS_SECRET_ACCESS_KEY,
           'endpoint_url':mc_config.MC_ENDPOINT_URL,
           'region_name':mc_config.MC_REGION_NAME,
           }
    
    def the_gen():
        for object_id in []:

            h = {#'_id': img_id,
                 'title':'Crowd of People Walking',
                 'artist':'test artist',
                 'collection_name':'test collection name',
                 'caption':'test caption',
                 'editorial_source':'test editorial source',
                 'keywords':'test keywords',
                 #'date_created':datetime.datetime.now(),
                 #'img_data':img_uri,
                 }
            
            obj = api.get_object(host, port, object_id, aws)
            
            print 'obj',obj
            
            assert False,'TODO'
            
            h['title'] = obj['title']
            
            yield h
    
    ingest_bulk(iter_json = gen)

    
def ingest_bulk_gettydump(getty_path = 'getty_small/json/images/',
                          index_name = mc_config.MC_INDEX_NAME,
                          doc_type = mc_config.MC_DOC_TYPE,
                          *args,
                          **kw):
    """
    Ingest media from Getty data dumps into Indexer.
    
    Args:
        getty_path: Path to getty image JSON.
        index_name: Name of Indexer index to populate.
        doc_type:   Name of Indexer doc type.
    """
    
    iter_json = mc_datasets.iter_json_getty(getty_path = getty_path,
                                            index_name = index_name,
                                            doc_type = doc_type,
                                            *args,
                                            **kw)

    ingest_bulk(iter_json = iter_json)
    


def config():
    """
    Print current environment variables.
    """    
    for x in dir(mc_config):
        if x.startswith('MC_'):
            print x + '="%s"' % str(getattr(mc_config, x))


functions=['ingest_bulk_blockchain',
           'ingest_bulk_gettydump',
           'config',
           ]

def main():
    setup_main(functions,
               globals(),
                'mediachain-indexer-ingest',
               )

if __name__ == '__main__':
    main()

