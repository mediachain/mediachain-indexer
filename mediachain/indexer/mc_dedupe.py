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

class model_reps_baseline(object):
    """
    Crudest image-matching model. Low-precision, low-recall.
    Does 1-to-1 matching of engineered feature hashes.
    """
    
    #was img_to_hsh()
    def img_to_terms(self, img_data_uri = False, img_fn = False, hash_size = 16):
        if img_data_uri:
            if type(img_data_uri) is unicode:
                img_data_uri = img_data_uri.encode('utf8')
            img = Image.open(StringIO(decode_image(img_data_uri)))
        else:
            img = Image.open(img_fn)
        hsh = binascii.b2a_hex(np.packbits(imagehash.dhash(img, hash_size = hash_size).hash).tobytes())
        return {'dedupe_hsh': hsh}

    
from sklearn.feature_extraction.image import extract_patches_2d
from math import sqrt

class model_reps_baseline_ng(object):
    """
    Crude image-matching model. Slightly higher recall than `baseline` model.
    Does approximate matching of engineered feature hashes.
    """
    
    def img_to_hsh_bools(self, img_data_uri = False, img_fn = False, hash_size = 16):
        if img_data_uri:
            if type(img_data_uri) is unicode:
                img_data_uri = img_data_uri.encode('utf8')
            img = Image.open(StringIO(decode_image(img_data_uri)))
        else:
            img = Image.open(img_fn)
        hsh = imagehash.dhash(img, hash_size = hash_size).hash
        return hsh

    def hsh_to_patches(self, hsh, patch_size = 4, max_patches = 64):

        pp = extract_patches_2d(hsh.astype(int), (patch_size, patch_size))

        # flatten 2nd and 3rd dimension:

        pp = pp.reshape((pp.shape[0], -1))

        # extract sample of patches:

        max_patches = min(max_patches, pp.shape[0])
        rr = [pp[x] for x in np.linspace(0, pp.shape[0], max_patches, endpoint=False).astype(int)]

        # pack patches into numbers:

        packed = [int(binascii.b2a_hex(''.join(np.packbits(x).view('c'))) or '0', 16) for x in rr]

        return packed

    def patches_to_query(self, packed):
        query = {}
        for c,zz in enumerate(packed):
            query['dedupe_word_' + str(c)] = zz
        return query
    
    def img_to_terms(self, img_data_uri = False, img_fn = False, patch_size = 5, max_patches = 64, hash_size = 16):
        #was img_to_query
        hsh = self.img_to_hsh_bools(img_data_uri = img_data_uri, img_fn = img_fn, hash_size = hash_size)
        patches = self.hsh_to_patches(hsh, patch_size = patch_size, max_patches = max_patches)
        rr = self.patches_to_query(patches)
        return rr


REP_MODEL_NAMES = {'baseline':model_reps_baseline,
                   'baseline_ng':model_reps_baseline_ng,
                   }
    
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



def dedupe_reindex(duplicate_modes = ['baseline', 'baseline_ng'],
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
        duplicate_modes:  List of semantic duplicate types to calculate. Defaults to ['baseline', 'baseline_ng'].
        incremental:      If True, only update clusters affected by newly ingested media. Otherwise, regenerate
                          all dedupe clusters. Note: the more records that are deduped simultaneously, the greater
                          the efficiency.
    
    Returns:
        Check program exit status.
    """
    
    assert not set(duplicate_modes).difference(['baseline', 'baseline_ng']),'DUPLICATE_MODE_NOT_IMPLEMENTED'
    
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
    
    nn = 0
    
    models = [REP_MODEL_NAMES[x]() for x in duplicate_modes]
    
    for c,hit in enumerate(res):

        nn += 1
        
        hh = hit['_source']#['doc']
        #hh['_id'] = hit['_id']
        
        doc_update = {}

        for model in models:
            doc_update.update(model.img_to_terms(hh['image_thumb']))
            
        rr.append({'_op_type': 'update',
                   '_index': index_name,
                   '_type': doc_type,
                   '_id': hit['_id'],
                   'body': {'doc':doc_update},
                   })
                
        #print ('ADD',c) #rr
        
        if len(rr) >= batch_size:
            do_commit(rr)
        
    if rr:
        do_commit(rr)

    print ('UPDATED',nn)

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
    
    return nn
    

functions=['dedupe_reindex',
           ]

def main():    
    setup_main(functions,
               globals(),
               )

if __name__ == '__main__':
    main()

