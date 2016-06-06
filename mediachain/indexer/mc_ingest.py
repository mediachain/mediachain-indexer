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

from mc_generic import setup_main, group, raw_input_enter, pretty_print

import mc_config

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


def getty_create_dumps(INC_SIZE = 100,
                       NUM_WORKERS = 30,
                       ):
    """
    Quick and dirty Getty API scraper.
    """

    if not mc_config.MC_GETTY_KEY:
        print ('ERROR: set GETTY_KEY environment variable.')
        exit(-1)
    
    if len(sys.argv) < 3:
        print ('NOTE: set GETTY_KEY to Getty API key.')
        print ('Usage: python getty_scrape [archiv | entertainment | rf | small]')
        exit(-1)
    
    typ = sys.argv[2]

    assert typ in ['archiv', 'entertainment', 'rf', 'small']
    
    ids = set()
    
    if typ == 'archiv':
        set_archiv = [x.strip() for x in open('Archive IDs.txt').read().split(',')]
        ids.update(set_archiv)
        
    elif typ == 'entertainment':
        set_entertainment = [x.strip() for x in open('Entertainment IDs.txt').read().split(',')]
        ids.update(set_entertainment)
        
    elif typ == 'rf':
        set_rf = [x.strip() for x in open('Royalty-free Creative IDs.txt').readlines()]
        ids.update(set_rf)
        
    elif typ == 'small':
        #small 100-sample portion of `entertainment` dataset.
        set_entertainment = [x.strip() for x in open('Entertainment IDs.txt').read().split(',')[:100]]
        ids.update(set_entertainment)
        
    else:
        assert False,typ
    
    ids = list(ids)
        
    print ('DOING',len(ids))
        
    shuffle(ids)
    
    dd4 = 'getty_' + typ + '/'
    if not exists(dd4):
        mkdir(dd4)
    
    dd3 = dd4 + 'downloads/'    
    if not exists(dd3):
        mkdir(dd3)

    dd = dd4 + 'json/'    
    if not exists(dd):
        mkdir(dd)
    
    for z in ['images/','images_not_found/']:
        if not exists(dd + z):
            mkdir(dd + z)

    good = 0
    bad = 0

    good_i = [0]
    bad_i = [0]

    all_done = [False]
    
    qq = Queue()

    def worker(qq):
        while True:
            
            try:
                h, xh = qq.get(timeout=1)
            except:
                print ('WORKER TIMEOUT',current_thread().name)
                if all_done[0]:
                    return
                else:
                    continue

            print ('WORKER_GOT',h['id'])
                
            xdd = dd3 + (xh['name'] + '/')

            if not exists(xdd):
                mkdir(xdd)

            for bb in xrange(1,5):
                dd2 = dd3 + xh['name'] + '/' + ('/'.join(h['id'][:bb])) + '/'
                if not exists(dd2):
                    mkdir(dd2)

            fn = xdd + ('/'.join(h['id'][:4])) + '/' + h['id'] + '.jpg'

            good_i[0] += 1

            print ('GOOD_i',good_i)

            if exists(fn):
                continue

            print ('GET',c,xh['uri'])

            print ('BDOING',xh['uri'])
            r = requests.get(xh['uri'],
                             verify=False,
                             )

            #Switched to loading whole file into RAM before writing:
            
            rr = ''
            for chunk in r.iter_content(chunk_size=1024 * 64): 
                if chunk: # filter out keep-alive new chunks
                    rr += chunk
            
            with open(fn, 'w') as f:
                f.write(rr)

            print ('WROTE',c,fn)

    tt = []
    for x in xrange(NUM_WORKERS):
        t=Thread(target=worker,
                 args=(qq,)
                 )
        t.daemon=True
        t.start()
        tt.append(t)
            
    for c,xids in enumerate(group(ids, INC_SIZE)):
        
        if True:
            #Skip existing:
            
            for xid in xids[:]:
                fn = dd3 + 'preview/' + ('/'.join(xid[:4])) + '/' + xid + '.jpg'
                fn2 = dd3 + 'comp/' + ('/'.join(xid[:4])) + '/' + xid + '.jpg'
                fn3 = dd3 + 'thumb/' + ('/'.join(xid[:4])) + '/' + xid + '.jpg'
                fn4 = dd + 'images/' + ('/'.join(xid[:4])) + '/' + xid + '.json'
                if exists(fn) and exists(fn2) and exists(fn3) and exists(fn4):
                    xids.remove(xid)
                    continue
                
                fn = dd + 'images_not_found/' + ('/'.join(xid[:4])) + '/' + xid + '.json'
                if exists(fn):
                    #xids.remove(xid)
                    #continue
                    unlink(fn)

        input_size = len(xids)
        
        if not xids:
            print ('SKIPPING',c)
            continue
        
        if qq.qsize() > 500:
            while qq.qsize() > 500:
                sleep(0.5)
                continue

        print ('DOING',c,len(xids))

        while True:

            url = 'https://api.gettyimages.com:443/v3/images?ids=' + (','.join(xids)) + '&fields=allowed_use%2Calternative_ids%2Cartist%2Cartist_title%2Casset_family%2Ccall_for_image%2Ccaption%2Ccity%2Ccollection_code%2Ccollection_id%2Ccollection_name%2Ccolor_type%2Ccomp%2Ccopyright%2Ccountry%2Ccredit_line%2Cdate_camera_shot%2Cdate_created%2Cdate_submitted%2Cdetail_set%2Cdisplay_set%2Cdownload_sizes%2Ceditorial_segments%2Ceditorial_source%2Cevent_ids%2Cgraphical_style%2Cid%2Ckeywords%2Clicense_model%2Clinks%2Cmax_dimensions%2Corientation%2Cpeople%2Cprestige%2Cpreview%2Cproduct_types%2Cquality_rank%2Creferral_destinations%2Cstate_province%2Csummary_set%2Cthumb%2Ctitle%2Curi_oembed'

            print ('ADOING',url)
            hh = requests.get(url,
                              headers={'Api-Key':mc_config.MC_GETTY_KEY},
                              verify=False,
                              ).json()

            if 'images' in hh:
                break
            
            print ('ERROR',hh)
            sleep(1)                

        assert (len(hh['images']) + len(hh['images_not_found'])) == \
            input_size,(len(hh['images']),
                        len(hh['images_not_found']),
                        len(hh['images']) + len(hh['images_not_found']),
                        )
            
        good += len(hh['images'])
        bad += len(hh['images_not_found'])
        
        print ('GOT',c,'good:',good,'bad:',bad)
        
        for h in hh['images']:
            
            for bb in xrange(1,5):
                dd2 = dd + 'images/' + ('/'.join(h['id'][:bb])) + '/'
                if not exists(dd2):
                    mkdir(dd2)

            with open(dd + 'images/' + ('/'.join(h['id'][:4])) + '/' + h['id'] + '.json', 'w') as f:
                f.write(json.dumps(h))
                    
        for bid in hh['images_not_found']:
            
            h = {'id':bid}
            
            for bb in xrange(1,5):
                dd2 = dd + 'images_not_found/' + ('/'.join(h['id'][:bb])) + '/'
                if not exists(dd2):
                    mkdir(dd2)
            
            with open(dd + 'images_not_found/' + ('/'.join(h['id'][:4])) + '/' + h['id'] + '.json', 'w') as f:
                f.write(json.dumps(h))

                
        for h in hh['images']:

            assert h['display_sizes']

            assert len(h['display_sizes']) == 3,h['display_sizes']
            
            for xh in h[u'display_sizes']:

                qq.put((h, xh))
                
    ######
    
    all_done[0] = True
    
    for t in tt:
        t.join()
        print ('JOINED')
        
    print ('DONE ALL')

    
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
    
    if mc_config.MC_ES_URLS:
        
        urls = mc_config.MC_ES_URLS.split(',')
        print ('CONNECTING...', urls)
        es = Elasticsearch(urls)
        
    else:
        print ('CONNECTING... <default>')
        es = Elasticsearch()
    print ('CONNECTED')
    
    return es

    
def iter_json_getty(getty_path,
                    max_num = 0,
                    index_name = mc_config.MC_INDEX_NAME,
                    doc_type = mc_config.MC_DOC_TYPE,                
                    ):

    dd = getty_path
    
    dd3 = dd.replace('/json/images/','/downloads/')
    
    assert exists(dd),repr(dd)

    nn = 0
    for dir_name, subdir_list, file_list in walk(dd):

        for fn in file_list:
            nn += 1

            if max_num and (nn + 1 >= max_num):
                print ('ENDING EARLY...')
                return

            fn = join(dir_name,
                      fn,
                      )

            with open(fn) as f:
                h = json.load(f)

            fn = dd3 + 'thumb/' + ('/'.join(h['id'][:4])) + '/' + h['id'] + '.jpg'

            if not exists(fn):
                print ('NOT_FOUND',fn)
                nn -= 1
                continue

            with open(fn) as f:
                img_data = f.read()

            hh = {'_id':'getty_' + h['id'],
                  'title':h['title'],
                  'artist':h['artist'],
                  'collection_name':h['collection_name'],
                  'caption':h['caption'],
                  'editorial_source':h['editorial_source'].get('name',None),
                  'keywords':' '.join([x['text'] for x in h['keywords'] if 'text' in x]),
                  'date_created':date_parser.parse(h['date_created']),
                  'img_data':img_data,
                  #'artist_id':[md5(x['uri']).hexdigest() for x in h['links'] if x['rel'] == 'artist'][0],                
                  #'dims':h['max_dimensions']
                  #'dims_thumb':[{'width':x['width'],'height':x['height']}
                  #               for x in h['display_sizes']
                  #               if x['name'] == 'thumb'][0],
                  }                

            rr = {'_op_type': 'index',
                  '_index': index_name,
                  '_type': doc_type,
                  '_id': hh['_id'],
                  #'doc': hh,
                  }
            rr.update(hh)

            print ('YIELDING',rr['_id'])

            yield rr

    print ('DONE_YIELD',nn)


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
        
    assert iter_json is not False
    
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

            if ignore_thumbs:
                if 'image_thumb' in hh:
                    del hh['image_thumb']
            
            else:
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
        doc_type:   Name of Indexer doc_type.
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
        doc_type:   Name of Indexer doc_type.
    """
    
    iter_json = iter_json_getty(getty_path = getty_path,
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


functions=['getty_create_dumps',
           'ingest_bulk_blockchain',
           'ingest_bulk_gettydump',
           'config',
           ]

def main():
    setup_main(functions,
               globals(),
               )

if __name__ == '__main__':
    main()

