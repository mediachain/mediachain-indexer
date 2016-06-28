#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Models for search and dedupe.
"""



import tornado
import tornado.gen
import json

from mc_generic import setup_main, pretty_print
from mc_ingest import decode_image
from elasticsearch.helpers import parallel_bulk, scan

import mc_neighbors

from PIL import Image
from cStringIO import StringIO

import imagehash
import binascii
import numpy as np

import mc_config

data_pat = 'data:image/jpeg;base64,'
data_pat_2 = 'data:image/png;base64,'

def simple_check_match(self,
                       query,
                       candidates,
                       score_threshold = 0.5, ## Hyperparameter optimize for this!
                       ):
    """
    Dead-simple classifier based on score thresholds.

    TODO: abstract some of the ES-centric stuff out.

    Hit Format:
        {u'_score': 0.87555695, u'_type': u'hpo_test_doc', u'_id': u'strong_attacked|204403', u'_source': {u'group_num': [u'20440'], u'num_instances_this_object': [22]}, u'_index': u'hpo_test'},
    """
    
    query_id = query['_id']
    
    rr = {}
    
    for cand in candidates:

        cand_id = cand['_id']

        pair_id = tuple(sorted((query_id, cand_id)))

        if cand['_score'] > score_threshold:
            rr[pair_id] = 1
        else:
            rr[pair_id] = 0
        
    return rr    


class VectorsBaseline(object):
    """
    Crudest image-matching model. Low-precision, low-recall.
    Does 1-to-1 matching of engineered feature hashes.
    """
    
    def __init__(self,
                 use_hash = 'dhash',
                 hash_size = 8,
                 ):
        
        self.hash_size = int(hash_size)
        
        if use_hash == 'dhash':
            self.hash_func = imagehash.dhash
        elif use_hash == 'phash':
            self.hash_func = imagehash.phash
        else:
            self.hash_func = use_hash
            
    #was img_to_hsh()
    def img_to_terms(self, img_data_uri = False, img_fn = False):
        if img_data_uri:
            if type(img_data_uri) is unicode:
                img_data_uri = img_data_uri.encode('utf8')
            img = Image.open(StringIO(decode_image(img_data_uri)))
        else:
            img = Image.open(img_fn)
        hsh = binascii.b2a_hex(np.packbits(self.hash_func(img, hash_size = self.hash_size).hash).tobytes())
        return {'dedupe_hsh': hsh}

    def img_to_es_query(self, *args, **kw):
        terms = self.img_to_terms(*args, **kw)
        query = {"query": {"constant_score":{"filter":{"term": terms}}}}
        return query

    def check_match(self,
                    *args,
                    **kw):
        return simple_check_match(*args, **kw)    
    
from sklearn.feature_extraction.image import extract_patches_2d
from math import sqrt

class VectorsBaselineNG(object):
    """
    Crude image-matching model. Slightly higher recall than `baseline` model.
    Does approximate matching of engineered feature hashes.
    
    Attempts to approximate hamming-distance nearest neighbors retrieval from the
    Lucene / Elasticsearch inverted index. 

    Details:
        Word-ngrams are simulated by taking patches of 2d boolean array output by the hashing function
        and converting these "words" to numbers. These numbers are inserted into each image document.
        The documents are queried using the Lucene / ElasticSearch multiterm tf-idf querying method
        via queries of the form: "{"query": {"bool": {"should": [ terms_list ] } } }".
    
    See Also:
        `mc_eval.ScoringTFIDF`
    """

    def __init__(self,
                 use_hash = 'dhash',
                 hash_size = 15,
                 patch_size = 2,
                 max_patches = 512,
                 ):
        if use_hash == 'dhash':
            self.hash_func = imagehash.dhash
        elif use_hash == 'phash':
            self.hash_func = imagehash.phash
        else:
            self.hash_func = use_hash
        
        self.hash_size = int(hash_size)
        
        self.patch_size = int(patch_size)
        self.max_patches = int(max_patches)

    def img_to_hsh_bools(self, img_data_uri = False, img_fn = False):
        if img_data_uri:
            if type(img_data_uri) is unicode:
                img_data_uri = img_data_uri.encode('utf8')
            img = Image.open(StringIO(decode_image(img_data_uri)))
        else:
            img = Image.open(img_fn)
        hsh = self.hash_func(img, hash_size = self.hash_size).hash
        return hsh

    def hsh_to_patches(self, hsh):

        pp = extract_patches_2d(hsh.astype(int), (self.patch_size, self.patch_size))
        
        # flatten 2nd and 3rd dimension:
        
        pp = pp.reshape((pp.shape[0], -1))
        
        # extract sample of patches:
        
        max_patches = min(self.max_patches, pp.shape[0])
        rr = [pp[x] for x in np.linspace(0, pp.shape[0], max_patches, endpoint=False).astype(int)]
        
        # pack patches into numbers:
        
        packed = [int(binascii.b2a_hex(''.join(np.packbits(x).view('c'))) or '0', 16) for x in rr]
        
        return packed

    def patches_to_query(self, packed):
        query = {}
        for c,zz in enumerate(packed):
            query['dedupe_word_' + str(c)] = zz
        return query
    
    def img_to_terms(self, img_data_uri = False, img_fn = False):
        #was img_to_query
        hsh = self.img_to_hsh_bools(img_data_uri = img_data_uri, img_fn = img_fn)
        patches = self.hsh_to_patches(hsh)
        rr = self.patches_to_query(patches)
        return rr
    
    def img_to_es_query(self, *args, **kw):
        terms = self.img_to_terms(*args, **kw)
        query = {'query':{'filtered': {'query': {'bool': {'should': [{'term': {x:y}} for x,y in terms.items()] } } } } }
        return query

    def check_match(self,
                    *args,
                    **kw):
        return simple_check_match(*args, **kw)    

    
@tornado.gen.coroutine
def dedupe_lookup_async(media_id,
                        lookup_name = 'dedupe_hsh', # Identify model by this, instead of by `vectors_model` in v2.
                        incremental = False,
                        include_docs = False,
                        include_self = False,
                        include_thumb = False,
                        es = False,
                        index_name = mc_config.MC_INDEX_NAME,
                        doc_type = mc_config.MC_DOC_TYPE,
                        v1_mode = True,
                        ):
    """
    Get list of all duplicates of a media work, from previously-generated duplicate lookup tables from `dedupe_reindex`.
    
    NOTE: Must run `dedupe_reindex` before using this.
    
    TODO: may convert this function to a non-async version with timeouts.
    
    Args:
        q_media:         Media to look up. See `Media Identifiers`.
        lookup_name:     Name of lookup key for the model you want to use. See `lookup_name` of `dedupe_reindex()`.
                         Note: use 'dedupe_hsh' for v1 model.
        include_self:    Include ID of query document in results.
        include_docs:    Return entire indexed docs, instead of just IDs.
        include_thumb:   Whether to include base64-encoded thumbnails in returned results.
        incremental:     Attempt to dedupe never-before-seen media file versus pre-ingested media files.
        es:              Database client handle. For `baseline`, it's an elasticsearch client handle.
    
    Returns:
                         List of matching media IDs of form: [{'id':'ifps://123...'}, {'id':'ifps://456...'},...]
    """
    
    if v1_mode:
        assert lookup_name == 'dedupe_hsh',("Since v1_mode is on must either use 'dedupe_hsh'. "\
                                            "Otherwise, be sure to turn v1_mode off, both here and for dedupe_reindex()."
                                            )

    else:
        lookup_name = 'lookup_' + lookup_name
    
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
    
    ### Trivial baseline can take a shortcut that uses fewer indexes / fewer writes:

    if not content_based_search:

        ## Query is a media ID. Get cluster ID for it:
        
        rr = yield es.search(index = index_name,
                             type = doc_type,
                             source = {"query":{ "ids":{ "values": [ media_id ] } } }
                             )

        rr = json.loads(rr.body)

        if not rr['hits']['hits']:
            raise tornado.gen.Return([])

        hit = rr['hits']['hits'][0]
        hh = hit['_source']#['doc']

        ## TODO - change following to .get(), to ignore records not yet dedupe-indexed for this lookup_name?:
        
        content_based_search = hh[lookup_name]  
        
        print ('GOT_HASH',content_based_search)
    
    rr = yield es.search(index = index_name,
                         type = doc_type,
                         source = {"query" : {"constant_score":{"filter":{"term":{ lookup_name : content_based_search}}}}},
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

    
def dedupe_train(via_cli = False):
    """
    Train dedupe models. Not needed for unsupervised v1 baseline.
    """
    pass



class GreedyCluster():
    """
    Simple greedy clustering.

    Input data format:
        pair_clf       = {} # {(id_1,id_2):1} or {(id_1,id_2):0}
        cluster_lookup = {} # {image_id:cluster_id}
        clusters = {}       # {cluster_id:[image_id,...]}
        cur_custer_id = [0] # [cluster_id]
    """
    
    def __init__(self,):
        pass
    
    def cluster(self,
                pair_clf,
                clusters,
                cluster_lookup,
                cur_cluster_id,
                ):
        
        for (id_1, id_2), is_match in pair_clf.iteritems():
            
            cn_1 = cluster_lookup[id_1]
            cn_2 = cluster_lookup[id_2]
            
            if is_match and (cn_1 != cn_2):
                ## Merge:
                
                cur_cluster_id[0] += 1
                cluster = tuple(set(clusters[cn_1] + clusters[cn_2]))
                clusters[cur_cluster_id[0]] = cluster

                ## TODO - there are more efficient ways of achieving this:
                
                for xid in cluster:
                    cluster_lookup[xid] = cur_cluster_id[0]
                
            if (not is_match) and (cn_1 == cn_2):
                ## Classifier says they these should be unmerged, but since we're merging greedily, do nothing:
                
                pass
                

VECTORS_MODEL_NAMES = {'baseline':VectorsBaseline,
                       'baseline_ng':VectorsBaselineNG,
                       }

CLUSTER_MODEL_NAMES = {'simple_greedy':GreedyCluster,
                       }

PAIRWISE_MODEL_NAMES = {## For now, reusing the vector models, which have basic `check_match()` functions:
                        'baseline':VectorsBaseline,
                        'baseline_ng':VectorsBaselineNG,
                        }


def dedupe_reindex_all(do_models = ['baseline'],
                       #do_models = VECTORS_MODEL_NAMES,
                       via_cli = False,
                       ):
    """
    Convenience function to re-run all the major models with one call.
    """
    
    for name in do_models:
        dedupe_reindex(vectors_model = name,
                       index_name = mc_config.MC_INDEX_NAME,
                       doc_type = mc_config.MC_DOC_TYPE,
                       )

def dedupe_reindex(lookup_name = False,
                   vectors_model = 'baseline',
                   pairwise_model = False,
                   cluster_model = 'simple_greedy',
                   greedy_updates = True,
                   incremental = False,
                   batch_size = 100,
                   index_name = mc_config.MC_INDEX_NAME,
                   doc_type = mc_config.MC_DOC_TYPE,
                   v1_mode = True,
                   via_cli = False,
                   ):
    """
    Regenerate duplicate lookup tables. Currently implements v1 - a simple, greedy, in-memory baseline.
    
    TODO - Baseline implementation done, more advanced modes still WIP.

    Args:
        lookup_name:    Name by which this particular model configuration can be looked up, in later dedupe lookups.
                        Defaults to just using the name of the passed `vectors_model`.
        vectors_model:  Representation learning model to use. Can be either a string or dict with following forms:
                        String:
                            'baseline'
                        Dictionary with model name as the key, and a sub-dictionary of hyper-parameters to pass
                        to models:
                            {'baseline_ng':{'use_hash':'dhash','patch_size':4}}
        greedy_updates: Whether clustering model should be applied greedily.
        pairwise_model: Pairwise classification model name. Or `False` to use the vectors model's `check_match()` function.
        cluster_model:  Clustering model name.
        incremental:    If True, only update clusters affected by newly ingested media. Otherwise, regenerate
                        all dedupe clusters. Note: the more records that are deduped simultaneously, the greater
                        the efficiency.
    
    Returns:
        Check program exit status.
    
    General steps performed here, depending on chosen models:
    
        1) Generate vectors from artefacts.
        2) Nearest-neighbor blocking from vectors.
        3) Pairwise classify all pairs in each block.
        4) Clustering based on pair classifications.
        5) Output lookup tables for clustering info.
    """
    
    assert vectors_model in VECTORS_MODEL_NAMES,('VECTORS_MODEL_NOT_IMPLEMENTED',vectors_model)
    
    assert (not pairwise_model) or (pairwise_model in PAIRWISE_MODEL_NAMES),('PAIRWISE_MODEL_NOT_IMPLEMENTED',pairwise_model)
    
    assert (not cluster_model) or (cluster_model in CLUSTER_MODEL_NAMES),('CLUSTER_MODEL_NOT_IMPLEMENTED',cluster_model)
    
    assert not incremental,'INCREMENTAL_NOT_IMPLEMENTED' ## And may never be implemented...
    
    #
    ## Step 1) Generate vector embeddings.
    #

    if mc_config.LOW_LEVEL:
        es = mc_neighbors.low_level_es_connect()    

        ## TODO: https://www.elastic.co/guide/en/elasticsearch/reference/current/breaking_21_search_changes.html
        
        res = scan(client = es,
                   index = index_name,
                   doc_type = doc_type,
                   scroll = '100m', #TODO - hard coded.
                   query = {"query": {'match_all': {}
                                     },
                           #'from':0,
                           #'size':1,                           
                           },
                   )
    else:
        nes = mc_neighbors.high_level_connect(index_name = index_name,
                                              doc_type = doc_type,
                                              )
        
        res = nes.scan_all()
        

    ## Instantiate representation learning model:
    
    if type(vectors_model) in [str, unicode]:
        vectors_model_name = vectors_model
        vectors_model = {vectors_model:{}}
        
    elif type(vectors_model) == dict:
        vectors_model_name = vectors_model.keys()[0]

    if not lookup_name:
        lookup_name = vectors_model_name
        
    print repr(vectors_model)
        
    vmodel = VECTORS_MODEL_NAMES[vectors_model_name](**vectors_model[vectors_model_name])
    
    #print ('MODEL',vmodel)

    
    def do_commit(rrr):
        print ('COMMITTING BATCH...',vectors_model_name,len(rrr))
        
        if mc_config.LOW_LEVEL:
            ii = parallel_bulk(es,
                               rrr,
                               thread_count = 1,
                               chunk_size = 500,
                               max_chunk_bytes = 100 * 1024 * 1024, #100MB
                               )
        else:
            ii = nes.parallel_bulk(rrr)
        
        for is_success,res in ii:
            #print ('COMMITTED_VECTORS',vectors_model_name,is_success,res)
            pass
        
        rrr[:] = []
        print ('COMMITTED')

    print ('(1) Generate baseline image descriptors...',vectors_model_name)
    
    hash_to_ids = {}
    
    rr = []
    
    nn = 0
    
    ## Instantiate pairwise classification model:

    if not pairwise_model:
        # Assume that our vectors model also has a `check_match()` function:
        pairwise_model_name = vectors_model_name
    else:
        pairwise_model_name = pairwise_model

    pmodel = PAIRWISE_MODEL_NAMES[pairwise_model_name]

    ## Instantiate clustering model:

    cluster_model_name = cluster_model    
    cmodel = CLUSTER_MODEL_NAMES[cluster_model]
    
    ## These will be needed later:
    
    cluster_lookup = {} # {image_id:cluster_id}
    clusters = {}       # {cluster_id:[image_id,...]}
    cur_cluster_id = [0]
    
    ## Run first pass of dedupe:
    
    
    for c,hit in enumerate(res):
        if c % 1000 == 0:
            print ('INDEXING_IMAGE:',vectors_model_name,c,repr(hit)[:50])

        ## Pre-populate these, for later:
        
        cur_cluster_id[0] += 1
        cluster_lookup[hit['_id']] = cur_cluster_id[0]
        clusters[cur_cluster_id[0]] = [hit['_id']]

        ## First pass of dedupe:
        
        nn += 1
        
        hh = hit['_source']#['doc']
        #hh['_id'] = hit['_id']
        
        doc_update = {}

        if 'image_thumb' in hh:
            doc_update.update(vmodel.img_to_terms(hh['image_thumb']))
            
            ## For now, only bother if there was an image:
            
            rr.append({'_op_type': 'update',
                       '_index': index_name,
                       '_type': doc_type,
                       '_id': hit['_id'],
                       'body': {'doc':doc_update},
                       })
            
            if c % 1000 == 0:
                print ('YES_THUMB_PRESENT',vectors_model_name,)#hit['_source'])
        else:
            
            if c % 1000 == 0:
                print ('NO_THUMB_PRESENT',vectors_model_name,)#hit['_source'])
                
        #print ('ADD',c) #rr
        
        if len(rr) >= batch_size:
            do_commit(rr)
        
    if rr:
        do_commit(rr)
    
    print ('UPDATED',vectors_model_name,nn)

    if mc_config.LOW_LEVEL:
        print ('REFRESHING', vectors_model_name, index_name)
        es.indices.refresh(index = index_name)
        print ('REFRESHED', vectors_model_name)
    else:
        nes.refresh_index()
    
    ### Following code can be skipped for v1 baseline:
    
    if v1_mode:
        print ('DONE_DEDUPE',vectors_model_name)
        return
    
    assert False,"WIP - didn't test yet."
    
    #
    ## Step 2) Create overlapping candidate clusters based on embedding space distance:
    #

    all_clf_pairs = {} # {(id_1,id_2):1} or {(id_1,id_2):0}

    if mc_config.LOW_LEVEL:
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
    else:
        res = nes.scan_all()
    
    nn = 0
    
    for c,hit in enumerate(res):
        
        nn += 1
        
        hh = hit['_source']
        hh['_id'] = hit['_id']
        
        query = vmodel.img_to_es_query(img_data = hh['image_thumb'])
        
        if mc_config.LOW_LEVEL:
            rr = es.search(index = index_name,
                           doc_type = doc_type,
                           body = query,
                           #fields = ['_id',
                           #          ],
                           size = do_optimize_for_recall_at,
                           timeout = '5s',
                           )
        else:
            rr = nes.search_terms(terms = query,
                                  size = do_optimize_for_recall_at,
                                  timeout = '5s',
                                  )

        rr = rr['hits']['hits']
        
        #
        ## Step 3) Pairwise same / different classification:
        #
        
        clf_pairs = pmodel.check_match(hit, rr)

        if greedy_updates:
            #
            ## Greedy Step 4) Create final non-overlapping clusters:
            #
            cmodel.cluster(clf_pairs,
                           clusters,
                           cluster_lookup,
                           cur_cluster_id,
                           )
            
        else:
            
            ## TODO:
            # Uncertain if we should just overwrite previous classifications here.
            # Depends on whether the model is only considering each individual pair in isolation,
            # or if it is also considering outside information e.g. considering at all other pairs in
            # the batch for each pair decision.
            
            all_clf_pairs.update(clf_pairs) 

    
    if not greedy_updates:
        #
        ## Non-Greedy Step 4) Create final non-overlapping clusters:
        #
        
        cmodel.cluster(all_clf_pairs,
                       clusters,
                       cluster_lookup,
                       cur_cluster_id,
                       )
    
    #
    ## Step 5) Update cluster lookup IDs: TODO - Atomic updates?...
    #
    
    rr = []
    nn = 0
    
    for c,(cluster_id,item_ids) in enumerate(clusters.iteritems()):
        
        for item_id in item_ids:
            
            nn += 1
            
            rr.append({'_op_type': 'update',
                       '_index': index_name,
                       '_type': doc_type,
                       '_id': hit['_source']['_id'],
                       'body': {'doc':{'lookup_' + lookup_name: unicode(cluster_id)}},
                       })
            
            if len(rr) >= batch_size:
                do_commit(rr)
    
    if rr:
        do_commit(rr)

    
    print ('DONE_DEDUPE')
    
    return nn



def typeahead_generate():
    """
    Re-generate typeahead search. This consists of a weighted set of completions for every possible query.

    Weighing ideas:
        - query frequency.
        - query results quality / count.
        - language model.
    
    TODO: Consider having the `NearestNeighborsBase` storage create this incrementally? 
          Is that approach really better in a clustered setup?
    """
    
    assert False,'WIP'
    
    if mc_config.LOW_LEVEL:
        es = mc_neighbors.low_level_es_connect()    
        
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
    else:
        nes = mc_neighbors.high_level_connect(index_name = index_name,
                                              doc_type = doc_type,
                                              )
        
        res = nes.scan_all()


functions=['dedupe_train',
           'dedupe_reindex',
           'dedupe_reindex_all',
           'typeahead_generate',
           ]

def main():    
    setup_main(functions,
               globals(),
               'mediachain-indexer-models',
               )

if __name__ == '__main__':
    main()

