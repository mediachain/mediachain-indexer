#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Testing. Currently a quick end-to-end sanity check.
"""

import datetime
import requests

from mc_generic import setup_main, pretty_print

import mc_config
import mc_ingest
import mc_models

from time import sleep

def sanity_check(index_name = mc_config.MC_TEST_INDEX_NAME,
                 doc_type = mc_config.MC_TEST_DOC_TYPE,
                 via_cli = False,
                 ):
    """
    Quick sanity check. WIP while API is being finalized. TODO: full tests.
    
    1. ingest images.
    2. search for images.
    3. find dupes.

    from elasticsearch import Elasticsearch
    es = Elasticsearch()
    es.search(index='mc_test',body = {"query": {'match_all': {}}})
    """
    
    img_uri = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAUAAAAFCAYAAACNbyblAAAAHElEQVQI12P4//8/w38GIAXDIBKE0DHxgljNBAAO9TXL0Y4OHwAAAABJRU5ErkJggg=="
    
    img_id = 'getty_1234'
    
    def test_ingest():
        yield {'_id': img_id,
               'title':'Crowd of People Walking',
               'artist':'test artist',
               'collection_name':'test collection name',
               'caption':'test caption',
               'editorial_source':'test editorial source',
               'keywords':'test keywords',
               'date_created':datetime.datetime.now(),
               'img_data':img_uri,
               }
    
    num_inserted = mc_ingest.ingest_bulk(test_ingest(),
                                         index_name = index_name,
                                         doc_type = doc_type,
                                         )
    
    print ('INSERTED',num_inserted)

    for name in mc_models.VECTORS_MODEL_NAMES:
        mc_models.dedupe_reindex(index_name = index_name,
                                 doc_type = doc_type,
                                 vectors_model = name,
                                 )

    print ('SEARCH_BY_TEXT...')
    
    hh = requests.post(mc_config.MC_TEST_WEB_HOST + '/search',
                       headers = {'User-Agent':'MC_TEST 1.0'},
                       verify = False,
                       json = {"q":'crowd',
                               "limit":5,
                               "include_self": True,
                               "index_name":index_name,
                               "doc_type":doc_type,
                               },
                       ).json()
    
    print pretty_print(hh)
    assert hh['results'][0]['_id'] == img_id,(2, hh)
    
    print ('SEARCH_BY_CONTENT...')
    
    hh = requests.post(mc_config.MC_TEST_WEB_HOST + '/search',
                       headers = {'User-Agent':'MC_TEST 1.0'},
                       verify = False,
                       json = {"q_id":img_uri,
                               "limit":5,
                               "include_self": True,
                               "index_name":index_name,
                               "doc_type":doc_type,
                               },
                       ).json()
    
    print pretty_print(hh)
    assert hh['results'][0]['_id'] == img_id,(3,hh)

    print ('SEARCH_BY_ID...')
    
    hh = requests.post(mc_config.MC_TEST_WEB_HOST + '/search',
                       headers = {'User-Agent':'MC_TEST 1.0'},
                       verify = False,
                       json = {"q_id":img_id,
                               "limit":5,
                               "include_self": True,
                               "index_name":index_name,
                               "doc_type":doc_type,
                               },
                       ).json()
    
    print pretty_print(hh)
    assert hh['results'][0]['_id'] == img_id,(4,hh)
    
    print ('DEDUPE_LOOKUP...')
    
    hh = requests.post(mc_config.MC_TEST_WEB_HOST + '/dupe_lookup',
                       headers = {'User-Agent':'MC_TEST 1.0'},
                       verify = False,
                       #json = {"q_media":img_uri, "limit":5},
                       json = {"q_media":img_id,
                               "limit":5,
                               "include_self": True,
                               "index_name":index_name,
                               "doc_type":doc_type,
                               },
                       ).json()
    
    print pretty_print(hh)
    assert hh['results'][0]['_id'] == img_id,(5,hh)

    print ('PASSED_SANITY_CHECK')

functions = ['sanity_check',
             ]

def main():
    setup_main(functions,
               globals(),
               'mediachain-indexer-test',
               )


if __name__ == '__main__':
    main()
