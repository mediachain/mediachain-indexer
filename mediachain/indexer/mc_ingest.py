#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Functions for ingestion of media files into Indexer.

Potential sources include:
- Mediachain blockchain.
- Getty dumps.
- Other media sources.

Scraping / downloading functions also contained here.

Later may be extended to insert media that comes from off-chain into the chain.
"""

from mc_generic import setup_main, group, raw_input_enter, pretty_print, intget, print_config

import mc_config
import mc_datasets
import mc_neighbors

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
from hashlib import md5

from PIL import Image
from cStringIO import StringIO

import binascii
import base64
import base58

import numpy as np

import imagehash
import itertools


from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk, scan

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
    
    return data_pat + base64.b64encode(s)

def decode_image(s):

    if s.startswith(data_pat):
        ss = s[len(data_pat):]
        
    elif s.startswith(data_pat_2):
        ss = s[len(data_pat_2):]
        
    else:
        assert False,('BAD_DATA_URL',s[:15])
        
    return base64.b64decode(ss)


def ingest_bulk(iter_json = False,
                thread_count = 1,
                index_name = mc_config.MC_INDEX_NAME,
                doc_type = mc_config.MC_DOC_TYPE,
                search_after = False,
                redo_thumbs = True,
                ignore_thumbs = False,
                delete_current = True,
                use_aggressive = True,
                auto_reindex_blocked_wait = 5,
                ):
    """
    Ingest Getty dumps from JSON files.

    Currently does not attempt to import media to the Mediachain chain.
    
    Args:
        iter_json:      Iterable of media objects, with `img_data` containing the raw-bytes image data.
        thread_count:   Number of parallel threads to use for ES insertion.
        index_name:     ES index name to use.
        doc_type:       ES document type to use.
        search_after:   Manually inspect ingested records after. Probably not needed anymore.
        redo_thumbs:    Whether to recalcuate 'image_thumb' from 'img_data'.
        ignore_thumbs:  Whether to ignore thumbnail generation entirely.
        delete_current: Whether to delete current index, if it exists.
        use_aggressive: Use slow inserter that immediately indexes & refreshes after each item.
        auto_reindex_blocked_wait: Since the input iterator may block, if `auto_reindex` is on, and the - TODO.
    
    Returns:
        Number of inserted records.

    Examples:
        See `mc_test.py`
    """
        
    index_settings = {'settings': {'number_of_shards': mc_config.MC_NUMBER_OF_SHARDS_INT,
                                   'number_of_replicas': mc_config.MC_NUMBER_OF_REPLICAS_INT,                             
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
                      }

    if not iter_json:
        iter_json = mc_datasets.iter_json_getty(index_name = index_name,
                                                doc_type = doc_type,
                                                )

    if mc_config.LOW_LEVEL:
        es = mc_neighbors.low_level_es_connect()
    
        if delete_current and es.indices.exists(index_name):
            print ('DELETE_INDEX...', index_name)
            es.indices.delete(index = index_name)
            print ('DELETED')

        if not es.indices.exists(index_name):
            print ('CREATE_INDEX...',index_name)
            es.indices.create(index = index_name,
                              body = index_settings,
                              #ignore = 400, # ignore already existing index
                              )

            print('CREATED',index_name)
    else:
        #NOT LOW_LEVEL:
        nes = mc_neighbors.high_level_connect(index_name = index_name,
                                              doc_type = doc_type,
                                              index_settings = index_settings,
                                              use_custom_parallel_bulk = use_aggressive,
                                              )
        
        if delete_current:
            nes.delete_index()
        
        nes.create_index()
            
    print('INSERTING...')

    def iter_wrap():
        # Put in parallel_bulk() format:
        
        for hh in iter_json:
            
            xdoc = {'_op_type': 'index',
                    '_index': index_name,
                    '_type': doc_type,
                    }
            
            hh.update(xdoc)

            if (hh.get('img_data') == 'NO_IMAGE') or (hh.get('image_thumb') == 'NO_IMAGE'):
                ## One-off ignoring of thumbnail generation via `NO_IMAGE`.

                assert False,('NO_IMAGE',hh)
                
                if 'img_data' in hh:
                    del hh['img_data']
                
                if 'image_thumb' in hh:
                    del hh['image_thumb']
            
            elif not ignore_thumbs:
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
            
            chh = hh.copy()
            if 'image_thumb' in chh:
                del chh['image_thumb']
            print 'INSERTING',index_name,doc_type#,chh
            
            yield hh
    
    gen = iter_wrap()

    def non_parallel_bulk(es,
                          the_iter,
                          *args, **kw):
        """
        Aggressive inserter that inserts & refreshes after every item.
        """

        for hh in the_iter:

            #print 'NON_PARALLEL_BULK',repr(hh)[:100],'...'
            
            xaction = hh['_op_type']
            xindex = hh['_index']
            xtype = hh['_type']
            xid = hh['_id']

            for k,v in hh.items():
                if k.startswith('_'):
                    del hh[k]
            
            assert xaction == 'index',(xaction,)
            
            print 'BODY',hh
            
            res = es.index(index = xindex, doc_type = xtype, id = xid, body = hh)
            
            print 'DONE-NON_PARALLEL_BULK',xaction,xid
            
            yield True,res

            try:
                es.indices.refresh(index = xindex)
            except:
                print 'REFRESH_ERROR'
            
            try:
                import mc_models
                mc_models.dedupe_reindex_all()
            except:
                print '!!! REINDEX_ERROR:'
                import traceback, sys, os
                for line in traceback.format_exception(*sys.exc_info()):
                    print line,
            
            print 'REFRESHED'
        
        print 'EXIT-LOOP_NON_PARALLEL_BULK'

    if use_aggressive:
        use_inserter = non_parallel_bulk
    else:
        use_inserter = parallel_bulk
    
    first = gen.next() ## TODO: parallel_bulk silently eats exceptions. Here's a quick hack to watch for errors.

    if mc_config.LOW_LEVEL:
        ii = use_inserter(es,
                          itertools.chain([first], gen),
                          thread_count = thread_count,
                          chunk_size = 1,
                          max_chunk_bytes = 100 * 1024 * 1024, #100MB
                          )
    else:
        ii = nes.parallel_bulk(itertools.chain([first], gen))
                              
    for is_success,res in ii:
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

    if mc_config.LOW_LEVEL:
        print ('REFRESHING', index_name)
        es.indices.refresh(index = index_name)
        print ('REFRESHED')
        rr = es.count(index_name)['count']
    else:
        nes.refresh_index()
        rr = nes.count()
        
    return rr


"""
EXPECTED ARTEFACT FORMAT:
----

{ 'entity': { u'meta': { u'data': { u'name': u'Randy Brooke'},
                         u'rawRef': { u'@link': '\x12 u\xbb\xdaP\xf6\x1d\x1d\xf4\xff\xcbFD\xac\xe9\x92\xb3,\xf1\x9a;\x08J\r\xd2L\x97\xd0\x8cKY\xd5\x1a'},
                         u'translatedAt': u'2016-06-08T15:25:50.254139',
                         u'translator': u'GettyTranslator/0.1'},
              u'type': u'entity'},
  u'meta': { u'data': { u'_id': u'getty_521396048',
                        u'artist': u'Randy Brooke',
                        u'caption': u'NEW YORK, NY - APRIL 15:  A model walks the runway wearing the Ines Di Santo Bridal Collection Spring 2017 on April 15, 2016 in New York City.  (Photo by Randy Brooke/Getty Images for Ines Di Santo)',
                        u'collection_name': u'Getty Images Entertainment',
                        u'date_created': u'2016-04-15T00:00:00-07:00',
                        u'editorial_source': u'Getty Images North America',
                        u'keywords': [ u'Vertical',
                                       u'Walking',
                                       u'USA',
                                       u'New York City',
                                       u'Catwalk - Stage',
                                       u'Fashion Model',
                                       u'Photography',
                                       u'Arts Culture and Entertainment',
                                       u'Bridal Show'],
                        u'title': u'Ines Di Santo Bridal Collection Spring 2017 - Runway'},
             u'rawRef': { u'@link': "\x12 r\x1a\xed'#\xc8\xbe\xb1'Qu\xadePG\x01@\x19\x88N\x17\xa9\x01a\x1e\xa9v\xc9L\x00\xe6c"},
             u'translatedAt': u'2016-06-08T15:26:12.622240',
             u'translator': u'GettyTranslator/0.1'},
  u'type': u'artefact'}
"""

def ingest_bulk_blockchain(last_block_ref = None,
                           delete_current = True,
                           index_name = mc_config.MC_INDEX_NAME,
                           doc_type = mc_config.MC_DOC_TYPE,
                           auto_reindex = True,
                           force_exit = True,
                           ):
    """
    Ingest media from Mediachain blockchain.
    
    Args:
        last_block_ref:  (Optional) Last block ref to start from.
        index_name:      Name of Indexer index to populate.
        doc_type:        Name of Indexer doc type.
        auto_reindex:    Automatically reindex upon completion. TODO: Reindex periodically instead of waiting for iterator exit?
        force_exit:      Force exit interpreter upon completion. Workaround for gPRC bug that prevents the process from exiting.                           
    """
    
    import mediachain.transactor.client
    from grpc.framework.interfaces.face.face import ExpirationError, AbortionError, CancellationError, ExpirationError, \
        LocalShutdownError, NetworkError, RemoteShutdownError, RemoteError

    grpc_errors = (AbortionError, CancellationError, ExpirationError, LocalShutdownError, \
                   NetworkError, RemoteShutdownError, RemoteError)
    
    from mediachain.datastore.dynamo import set_aws_config
    aws_cfg = {'endpoint_url': mc_config.MC_ENDPOINT_URL,
               'mediachain_table_name': mc_config.MC_DYNAMO_TABLE_NAME,
               'aws_access_key_id': mc_config.MC_AWS_ACCESS_KEY_ID,
               'aws_secret_access_key': mc_config.MC_AWS_SECRET_ACCESS_KEY,
               'region_name': mc_config.MC_REGION_NAME,
               }

    aws_cfg = dict((k, v) for k, v in aws_cfg.iteritems() if v is not None)
    set_aws_config(aws_cfg)
    
    def the_gen():
        
        print 'STREAMING FROM TRANSACTORCLIENT...',(mc_config.MC_TRANSACTOR_HOST, mc_config.MC_TRANSACTOR_PORT_INT)
        
        tc = mediachain.transactor.client.TransactorClient(mc_config.MC_TRANSACTOR_HOST,
                                                           mc_config.MC_TRANSACTOR_PORT_INT,
                                                           )
        for art in tc.canonical_stream(timeout=600):
            try:
                print 'GOT',art.get('type')

                if art['type'] != u'artefact':
                    continue

                meta = art['meta']['data']

                rh = {}
                
                ## Copy these keys in from meta. Use tuples to rename keys. Keys can be repeated:
                
                for kk in [u'caption', u'date_created', u'title', u'artist',
                           u'keywords', u'collection_name', u'editorial_source',
                           '_id',
                           ('_id','getty_id'),
                           ('thumbnail_base64','image_thumb'),
                           ]:

                    if type(kk) == tuple:
                        rh[kk[1]] = meta[kk[0]]
                    elif kk == u'keywords':
                        rh[kk] = ' '.join(meta[kk])
                    else:
                        rh[kk] = meta[kk]

                #TODO: Phase out `rawRef`:
                if 'raw_ref' in art['meta']:
                    raw_ref = art['meta']['raw_ref']
                elif 'rawRef' in art['meta']:
                    raw_ref = art['meta']['rawRef']
                else:
                    assert False,('RAW_REF',repr(art)[:500])
                
                rh['latest_ref'] = base58.b58encode(raw_ref[u'@link'])

                ## TODO - use different created date? Phase out `translatedAt`:
                if 'translated_at' in art['meta']:
                    xx = art['meta']['translated_at']
                elif 'translatedAt' in art['meta']:
                    xx = art['meta']['translatedAt']
                else:
                    assert False,'translatedAt'
                
                rh['date_created'] = date_parser.parse(xx) 

                rhc = rh.copy()
                if 'img_data' in rhc:
                    del rhc['img_data']
                print 'INSERT',rhc
                
                yield rh
            except:
                print '!!!ARTEFACT PARSING ERROR:'
                print repr(art)
                print 'TRACEBACK:'
                import traceback, sys, os
                for line in traceback.format_exception(*sys.exc_info()):
                    print line,
                print 'PRESS ENTER...'
                raw_input()
        
        print 'END ITER'
    
    try:
        nn = ingest_bulk(iter_json = the_gen(),
                         #index_name = index_name,
                         #doc_type = doc_type,
                         delete_current = False,
                         )
        
        print 'GRPC EXITED SUCCESSFULLY...'

    except grpc_errors as e:
        print '!!!CAUGHT gRPC ERROR',e

        import traceback, sys, os
        for line in traceback.format_exception(*sys.exc_info()):
            print line,

        if force_exit:
            ## Force exit due to grpc bug:

            print 'FORCE_EXIT'

            sleep(1)

            os._exit(-1)

            
    except BaseException as e:
        print '!!!CAUGHT OTHER ERROR',e
        
        import traceback, sys, os
        for line in traceback.format_exception(*sys.exc_info()):
            print line,
            
        if force_exit:
            ## Force exit due to grpc bug:

            print 'FORCE_EXIT'

            sleep(1)

            os._exit(-1)

    
    if auto_reindex:
        
        print 'AUTO_REINDEX...'
        
        import mc_models
        mc_models.dedupe_reindex_all()

    print 'DONE_INGEST',nn

    
def ingest_bulk_gettydump(max_num = 100000,
                          getty_path = 'getty_small/json/images/',
                          #getty_path = 'getty_archiv/json/images/',
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
    
    if mc_config.MC_USE_IPFS_INT:
        from mediachain.datastore import set_use_ipfs_for_raw_data
        set_use_ipfs_for_raw_data(True)
    
    iter_json = mc_datasets.iter_json_getty(max_num = max_num,
                                            getty_path = getty_path,
                                            index_name = index_name,
                                            doc_type = doc_type,
                                            *args,
                                            **kw)

    ingest_bulk(iter_json = iter_json)

    ## TODO: automatically do this for now, so we don't forget:
    
    import mc_models
    mc_models.dedupe_reindex_all()



def search_by_image(limit = 5,
                    index_name = mc_config.MC_INDEX_NAME,
                    doc_type = mc_config.MC_DOC_TYPE,
                    ):
    """
    Command-line content-based image search.
    
    Example:
    $ mediachain-indexer-ingest ingest_bulk_gettydump
    $ mediachain-indexer-ingest search_by_image getty_small/downloads/thumb/5/3/1/7/531746924.jpg
    """

    if len(sys.argv) < 3:
        print 'Usage: mediachain-indexer-ingest search_by_image <image_file_name> [limit_num] [index_name] [doc_type]'
        exit(-1)

    fn = sys.argv[2]
    
    if len(sys.argv) >= 4:
        limit = intget(sys.argv[3], 5)
    
    if len(sys.argv) >= 5:
        index_name = sys.argv[4]
        
    if len(sys.argv) >= 6:
        doc_type = sys.argv[5]
    
    if not exists(fn):
        print ('File Not Found:',fn)
        exit(-1)
    
    with open(fn) as f:
        d = f.read()
        
    img_uri = shrink_and_encode_image(d)
    
    hh = requests.post(mc_config.MC_TEST_WEB_HOST + '/search',
                       headers = {'User-Agent':'MC_CLI 1.0'},
                       verify = False,
                       json = {"q_id":img_uri,
                               "limit":limit,
                               "include_self": True,
                               "index_name":index_name,
                               "doc_type":doc_type,
                               },
                       ).json()
    
    print pretty_print(hh)
    

def delete_index(index_name = mc_config.MC_INDEX_NAME):
    print('DELETE_INDEX',index_name)
    
    if mc_config.LOW_LEVEL:
        es = mc_neighbors.low_level_es_connect()
        
        if es.indices.exists(index_name):
            es.indices.delete(index = index_name)
        
    else:
        #NOT LOW_LEVEL:
        nes = mc_neighbors.high_level_connect(index_name = index_name,
                                              doc_type = doc_type,
                                              index_settings = index_settings,
                                              use_custom_parallel_bulk = use_aggressive,
                                              )
        
        nes.delete_index()
    
    print ('DELETED')
        
    


    
    
def config():
    print_config(mc_config.cfg)

functions=['ingest_bulk_blockchain',
           'ingest_bulk_gettydump',
           'search_by_image',
           'config',
           'delete_index',
           ]

def main():
    setup_main(functions,
               globals(),
                'mediachain-indexer-ingest',
               )

if __name__ == '__main__':
    main()

