#!/usr/bin/env python
# -*- coding: utf-8 -*-

__doc__ = \
"""
Run dedupe on all ingested media, to generate two lookup tables:

   {global_image_id:cluster_id}
   {cluster_id:global_image_id}


## Process

Baseline approach uses manual engineered features having minimal flexibility and minimal training supervision used only
for e.g. adjusting the similarity threshold hyperparameter.

Advanced approach relies on more flexible models, trained via supervised training data, and with less reliance on
manual feature engineering or hyper-parameter tuning.

1) [Baseline & Advanced]: For all ingested media, lookup ~100 candidate duplicates. This can be done by:
   - Exact matching on feature-engineered blocking predicates
     (e.g. exact match on stemmed and normalized text strings.)
   - Approximate kNN on unsupervised, feature-engineered hashing descriptors or vector embeddings
     (e.g. pHash, dHash, tf-IDF).
   - Approximate kNN lookup, on vectors created from a model learned via training supervision
     (e.g. Representation learning model trained on a margin ranking triplet-loss.)
2) [Baseline]: It decides there's a match if a pair of media exceeds a similarity threashold.
3) [Advanced]: applies a second layer of dedupe classification on all pairs in each candidate block.
4) [Baseline & Advanced]: Pairwise decisions are fed into an agglomerative clustering, producing a flat clustering.


TODO:
   - Migrate from simple baselines to sophisticated models.
   - Store dedupe lookup tables on something other than ES?

"""

import tornado
import tornado.gen
import json

from mc_generic import setup_main, pretty_print
from mc_ingest import es_connect, decode_image
from elasticsearch.helpers import parallel_bulk, scan

from PIL import Image
from cStringIO import StringIO

import imagehash
import binascii
import numpy as np

import mc_config

data_pat = 'data:image/jpeg;base64,'
data_pat_2 = 'data:image/png;base64,'

def img_to_hsh(img_data_uri):
    """
    Crude method only used for V1 dedupe.
    """
    img = Image.open(StringIO(decode_image(img_data_uri.encode('utf8'))))
    hsh = binascii.b2a_hex(np.packbits(imagehash.dhash(img, hash_size = 16).hash).tobytes())
    return hsh

@tornado.gen.coroutine
def dedupe_lookup_async(media_id,
                        duplicate_mode = 'baseline',
                        incremental = False,
                        include_docs = False,
                        include_self = False,
                        include_thumb = False,
                        es = False,
                        index_name = mc_config.MC_INDEX_NAME,
                        doc_type = mc_config.MC_DOC_TYPE,                
                        ):
    """
    Get list of all duplicates of a media work, from previously-generated duplicate lookup tables from `dedupe_reindex`.
    
    NOTE: Must run `dedupe_reindex` before using this.
    
    TODO: may convert this function to a non-async version with timeouts.
    
    Args:
        q_media:         Media to look up. See `Media Identifiers`.
        duplicate_mode:  Semantic duplicate type or matching mode. Defaults to 'baseline'.
                         - `baseline`: Simple low-recall exact matching on an engineered content-based semantic hash.
                         - `advanced`: TODO.
        incremental:     Attempt to dedupe never-before-seen media file versus pre-ingested media files.
        es:              Database client handle. For `baseline`, it's an elasticsearch client handle.

    Returns:
                         List of matching media IDs of form: [{'id':'ifps://123...'}, {'id':'ifps://456...'},...]
    """
    
    assert duplicate_mode == 'baseline','SELECTED_DUPLICATE_MODE_NOT_IMPLEMENTED'
    
    #raise tornado.gen.Return([])
    
    content_based_search = False
    
    if media_id.startswith(data_pat) or media_id.startswith(data_pat_2):

        #Search based on data URI:
        content_based_search = img_to_hsh(media_id)
    
    elif media_id.startswith('getty_'):
        
        #ID-based search.
        #TODO - identifier format.
        pass
    
    else:
        assert False,'BAD_MEDIA_ID_FORMAT'


    print ('content_based_search',content_based_search,media_id[:40])
    
    if True:
        
        ### Trivial baseline can take a shortcut that uses fewer indexes / fewer writes:
        
        if not content_based_search:
        
            rr = yield es.search(index = index_name,
                                 type = doc_type,
                                 source = {"query":{ "ids":{ "values": [ media_id ] } } }
                                 )

            rr = json.loads(rr.body)

            if not rr['hits']['hits']:
                raise tornado.gen.Return([])
        
            hit = rr['hits']['hits'][0]
            hh = hit['_source']#['doc']
            
            content_based_search = hh['dedupe_hsh']
            
            print ('GOT_HASH',hh['dedupe_hsh'])
        
        rr = yield es.search(index = index_name,
                             type = doc_type,
                             source = {"query" : {"constant_score":{"filter":{"term":{ "dedupe_hsh" : content_based_search}}}}},
                             )

        rr = json.loads(rr.body)
        
        if not rr['hits']['hits']:            
            raise tornado.gen.Return([])
        
        rr = rr['hits']['hits']
        
        if not include_self:
            
            rr = [hit
                  for hit
                  in rr
                  if hit['_id'] != media_id
                  ]
        
        if not include_docs:
            
            rr = [{'_id':hit['_id']}
                  for hit
                  in rr
                  ]
            
        else:
            
            if not include_thumb:
                for x in rr:
                    if 'image_thumb' in x:
                        del x['image_thumb']
        
        raise tornado.gen.Return(rr)
    
    else:

        ### We'll use this method instead, once we move beyond the trivial baseline:
        
        #Lookup cluster ID for media ID:    
        
        rr = yield es.search(index = mc_config.MC_INDEX_NAME_MID_TO_CID,
                             type = mc_config.MC_DOC_TYPE_MID_TO_CID,
                             source = {"query" : {"constant_score":{"filter":{"term":{ "_id" : media_id}}}},
                                       'size':1,
                                       },
                             )
        
        if not rr['hits']['hits']:

            raise tornado.gen.Return([])
        
        else:
            #Lookup all media IDs in that cluster:
            
            hit = rr['hits']['hits'][0]
            hh = hit['_source']#['doc']
            
            rr = yield es.multi_search(index = mc_config.MC_INDEX_NAME_CID_TO_CLUSTER,
                                       type = mc_config.MC_DOC_TYPE_CID_TO_CLUSTER,
                                       source = {"query" : {"constant_score":{"filter":{"term":{ "_id" : hh['c_id']}}}}},
                                       )

            if not rr['hits']['hits']:
                raise tornado.gen.Return([])

            hit = rr['hits']['hits'][0]
            hh = hit['_source']#['doc']

            raise tornado.gen.Return(hh['cluster'])



def dedupe_reindex(duplicate_mode = 'baseline',
                   incremental = False,
                   batch_size = 100,
                   index_name = mc_config.MC_INDEX_NAME,
                   doc_type = mc_config.MC_DOC_TYPE,
                   ):
    """
    Regenerate duplicate lookup tables.
    
    Currently implements V1 - a dumb, simple, greedy, in-memory baseline.
    
    Performance Note:
    Usually it's considerably more efficient to do this in large batches rather than to attempt to dedupe online, for
    each new media file.
    
    Args: 
        duplicate_mode:  Semantic duplicate type. For now, defaults to 'baseline'.
        incremental:     If True, only update clusters affected by newly ingested media. Otherwise, regenerate
                         all dedupe clusters. Note: the more records that are deduped simultaneously, the greater
                         the efficiency.
    
    Returns:
        Check program exit status.
    """
    
    assert duplicate_mode == 'baseline','DUPLICATE_MODE_NOT_IMPLEMENTED'

    def do_commit(rrr):
        print ('COMMITTING BATCH...',len(rrr))
        for is_success,res in parallel_bulk(es,
                                            rrr,
                                            thread_count = 1,
                                            chunk_size = 500,
                                            max_chunk_bytes = 100 * 1024 * 1024, #100MB
                                            ):
            #print is_success,res
            pass
        
        rrr[:] = []
        print ('COMMITTED')
    
    es = es_connect()    
    
    res = scan(client = es,
               index = index_name,
               doc_type = doc_type,
               scroll = '5m', #TODO - hard coded.
               query = {"query": {'match_all': {}
                                 },
                       #'from':0,
                       #'size':1,                           
                       },
               )
    
    print ('(1) Generate baseline image descriptors...')
    
    hash_to_ids = {}
    
    rr = []
    
    for c,hit in enumerate(res):
        
        hh = hit['_source']#['doc']
        #hh['_id'] = hit['_id']
                
        hsh = img_to_hsh(hh['image_thumb'])
        
        if False:            
            ### Can skip this for the trivial baseline:
            
            if hsh not in hash_to_ids:
                hash_to_ids[hsh] = []
            hash_to_ids[hsh].append(hh['_id'])
                
        rr.append({'_op_type': 'update',
                   '_index': index_name,
                   '_type': doc_type,
                   '_id': hit['_id'],
                   'body': {'doc':{'dedupe_hsh': hsh}},
                   })
                
        print ('ADD',c,rr)
        
        if len(rr) >= batch_size:
            do_commit(rr)
        
    if rr:
        do_commit(rr)

    print ('REFRESHING', index_name)
    es.indices.refresh(index = index_name)
    print ('REFRESHED')

    if False:
        
        ### Can skip these for the trivial baseline:

        rr = []

        for c_id, (hsh, cluster) in enumerate(hash_to_ids.iteritems()):

            #Media ID -> Cluster ID:
            for mid in cluster:
                rr.append({'_op_type': 'insert',
                           '_index': mc_config.MC_INDEX_NAME_MID_TO_CID,
                           '_type': mc_config.MC_DOC_TYPE_MID_TO_CID,
                           '_id': mid,
                           'doc': {'c_id': cn},
                           })

            #Cluster ID -> Cluster:
            rr.append({'_op_type': 'insert',
                       '_index': mc_config.MC_INDEX_NAME_CID_TO_CLUSTER,
                       '_type': mc_config.MC_DOC_TYPE_CID_TO_CLUSTER,
                       '_id': c_id,
                       'doc': {'cluster': cluster},
                       })

            if len(rr) >= batch_size:
                do_commit(rr)
        if rr:
            do_commit(rr)

    
    print ('DONE_DEDUPE')
    
    


functions=['dedupe_reindex',
           ]

def main():    
    setup_main(functions,
               globals(),
               )

if __name__ == '__main__':
    main()

