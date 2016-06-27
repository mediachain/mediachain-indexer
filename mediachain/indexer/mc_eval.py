#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Evaluate dedupe and search models against labeled datasets. Do hyper-parameter tuning.

For more datasets, see: https://github.com/mediachain/mediachain-indexer/issues/1

Evaluation types:
   - Recall @ k for search.
   - Precision-recall-curve for dedupe.
"""

import matplotlib.pyplot as plt
import numpy as np
from sklearn.metrics import precision_recall_curve, average_precision_score, classification_report
from sklearn.preprocessing import MinMaxScaler
import matplotlib.pyplot as plt

import mc_ingest
import mc_models
import mc_datasets

from mc_generic import group, setup_main, raw_input_enter, download_streamed, tcache, pretty_print
from os import mkdir, listdir, makedirs, walk
from os.path import exists,join
from random import random, shuffle, sample
import requests

from collections import Counter, defaultdict

from hyperopt import hp, space_eval, fmin, tpe
from time import time

import mc_neighbors
import mc_config


def hpo_vector_models(the_gen = mc_datasets.iter_copydays,
                      #the_gen = mc_datasets.iter_ukbench,
                      max_evals = 10,
                      max_records = 160,
                      max_queries = 160,
                      ignore_self = True,
                      recall_at = False,
                      use_simulator = False,
                      index_name = 'hpo_test',
                      doc_type = 'hpo_test_doc',
                      parallel_mode = False,
                      parallel_db = 'mongo://localhost:12345/foo_db/jobs',
                      USE_NN = True,
                      via_cli = False,
                      ):
    """
    Hyper-parameter optimization for the vector representation learning models.

    Args:
        the_gen:        Dataset iterator.
        max_evals:      Important - Number of iterations to run the hyper-parameter optimizer.
        num_records:    Number of images to load.
        max_queries:    Number of test queries to do.
        recall_at:      If False, will automatically optimize for recall @ `sample['num_instances_this_object']`.
                        Set to integer to optimize for recall @ `recall_at`.
        ignore_self:    Whether to ignore query image finding itself in the search results for scoring calculation.
        use_simulator:  Whether to use simulated ES. (TODO - Not yet tested.)
        parallel_mode:  Whether to run parallel across multiple machines. See hyperopt wiki link above.
        parallel_db:    Mongodb database URL needed for parallel mode.
                        NOTE: Use a new database name for each new run.
    
    For parallel mode, see these:
         https://github.com/hyperopt/hyperopt/wiki/Parallelizing-Evaluations-During-Search-via-MongoDB
         https://github.com/hyperopt/hyperopt/issues/248
    
    Example Output:
        BEST ARGS:
        {'patch_size': 2, 'use_hash': 'dhash', 'hash_size': 15.0, 'max_patches': 512, 'rep_model_name': 'baseline_ng'}

        BEST SCORE:
        3.25901%

    TODO:
        Save winning hyper-parameters, for later use.
    """
    
    assert not parallel_mode,'PARALLEL_MODE_NOT_TESTED_YET'
    assert not use_simulator,'SIMULATOR_NOT_TESTED_YET'

    if ignore_self:
        ignore_self_n = 1.0
    else:
        ignore_self_n = 0.0
    
    from hyperopt import Trials
    from hyperopt.mongoexp import MongoTrials

    if parallel_mode:
        trials = MongoTrials(parallel_db, exp_key='exp1')
    else:
        trials = Trials()
    
    t0 = time()

    ## Run once for downloads:
    #the_gen().next()

    if use_simulator:
        es = mc_neighbors.ElasticSearchEmulator()
    else:
        es = mc_neighbors.low_level_es_connect()
    
    if es.indices.exists(index_name):
        print ('DELETE_INDEX...', index_name)
        es.indices.delete(index = index_name)
        print ('DELETED')
    
    print 'INSERTING...',
    
    num_inserted = mc_ingest.ingest_bulk(the_gen(max_records, do_img_data = True),
                                         index_name = index_name,
                                         doc_type = doc_type,
                                         redo_thumbs = False,
                                         use_aggressive = False,
                                         )
    print 'INSERTED',num_inserted

    print 'DEDUPE...'

    for name in mc_models.VECTORS_MODEL_NAMES:
        num_updated = mc_models.dedupe_reindex(index_name = index_name,
                                               doc_type = doc_type,
                                               vectors_model = name,
                                               )
    
    print 'DEDUPE_UPDATED',num_updated

    def objective(oargs):
        """
        Setup optimization objective to be minimizing return value of this function.

        oargs format e.g.: {'type': 'baseline', 'use_hash': 'phash', 'hash_size': 11.0}
        """
        
        oargs_orig = oargs.copy()
        
        rep_model_name = oargs['rep_model_name']
        del oargs['rep_model_name']
        
        #print 'ARGS:'
        #print oargs
        #raw_input_enter()

        ## Instantiate with passed `oargs`:
        
        rep_model = mc_models.VECTORS_MODEL_NAMES[rep_model_name](**oargs)
        
        ## Evaluate recall @ N for on `max_queries` queries.
        
        r_at_n = 0.0
        num_found = 0
        num_possible = 0
        
        for c,hh in enumerate(the_gen(max_queries, do_img_data = False)):
            
            if c % 100 == 0:
                print 'objective',c
            
            ## Either fixed number or dynamic for each sample:
            
            if recall_at:
                do_optimize_for_recall_at = recall_at
            else:
                do_optimize_for_recall_at = hh['num_instances_this_object']
            
            assert do_optimize_for_recall_at > 1,('Dataset must have more than one sample per class.',do_optimize_for_recall_at)
            
            group_num = hh['group_num']
            
            try:
                query = rep_model.img_to_es_query(img_fn = hh['fn'])
            except KeyboardInterrupt:
                raise                         
            except:
                ## Some hyper-parameters combinations are not allowed, e.g.:
                ## Error: 'Height of the patch should be less than the height of the image'
                ## Instead of coding all of those validity constraints into the hyperopt search space,
                ## just return large number here.
                return 0.0

            ## Search!:
            
            rr = es.search(index = index_name,
                           doc_type = doc_type,
                           body = query,
                           fields = ['_id',
                                     'group_num',
                                     'num_instances_this_object',
                                     ],
                           size = do_optimize_for_recall_at,
                           timeout = '5s',
                           )
            
            rr = rr['hits']['hits']

            if rr:
                print 'GOT',hh['_id'],rr
                #raw_input_enter()
            
            ## Record percent of true candidate docs found, ignoring query doc:

            found = 0.0
            for x in rr[:do_optimize_for_recall_at]:
                if ignore_self and (x['_id'] == hh['_id']):
                    continue
                if (x['fields']['group_num'][0] != hh['group_num']):
                    continue
                found += 1.0
            
            r_at_n = found / (do_optimize_for_recall_at - ignore_self_n)
            
            num_found += found
            num_possible += min((do_optimize_for_recall_at - ignore_self_n),
                                (hh['num_instances_this_object'] - ignore_self_n),
                                )


        rr = 0 - (num_found / num_possible)
            
        print 'returning:',rr,'found:',num_found,'of',num_possible
        return rr


    ## Define optimization search space:

    # See: https://github.com/hyperopt/hyperopt/wiki/FMin
    
    space = hp.choice('rep_model',
                      [{'rep_model_name':'baseline',
                        'hash_size':hp.quniform('baseline_hash_size', 2, 64, 1),
                        'use_hash':hp.choice('baseline_use_hash',['dhash','phash']),
                      },
                       {'rep_model_name':'baseline_ng',
                        'hash_size':hp.quniform('baseline_ng_hash_size', 2, 64, 1),
                        'use_hash':hp.choice('baseline_ng_use_hash',['dhash','phash']),
                        'patch_size':hp.choice('baseline_ng_patch_size', [1,2,3,4,5,6,7,8,9,10,11,12,13]),
                        'max_patches':hp.choice('baseline_ng_max_patches', [2,4,8,16,32,64,128,256,512,1024]),
                       },
                      ],
                      )
    
    ## Run optimizer:
    
    print 'RUN OPTIMIZER...'
    
    best_args = fmin(objective,
                     space,
                     algo = tpe.suggest,
                     max_evals = max_evals,
                     trials = trials,
                     )

    print 'RE-RUN BEST PARAMS TO GET BEST SCORE...'
    
    best_score = objective(space_eval(space, best_args))

    print
    print 'BEST ARGS:'
        
    print space_eval(space, best_args)

    print
    print 'BEST SCORE:'
    
    print '%.5f%%' % ((0 - best_score) * 100)
    
    print
    print 'DONE',time() - t0

    
def eval_demo(max_num = 500,
              num_queries = 500,
              the_gen = mc_datasets.iter_ukbench,
              rep_model_names = ['baseline_ng',
                                 'baseline',
                                 #'all_true',
                                 'random_guess',
                                 ],
              ignore_self = False,
              index_name = 'eval_index',
              doc_type = 'eval_doc',
              via_cli = False,
              ):
    """
    Some exploratory visualizations of the representation-learning models.
    
    Consider this like an iPython notebook of quick experiments.
    
    For more practical needs, see `hpo_vector_models`.
    """

    ## Run once for downloads:
    #the_gen().next()

    precision = {}
    recall = {}
    average_precision = {}

    num_true = 0
    num_false = 0

    if mc_config.LOW_LEVEL:
        es = mc_neighbors.low_level_es_connect()

        if es.indices.exists(index_name):
            print ('DELETE_INDEX...', index_name)
            es.indices.delete(index = index_name)
            print ('DELETED')

    else:
        nes = mc_neighbors.high_level_connect(index_name = index_name,
                                              doc_type = doc_type,
                                              )
        nes.delete_index()
    
    print 'INSERTING...'
    
    num_inserted = mc_ingest.ingest_bulk(the_gen(max_num, do_img_data = True),
                                         index_name = index_name,
                                         doc_type = doc_type,
                                         redo_thumbs = False,
                                         use_aggressive = False,
                                         )
    print 'INSERTED',num_inserted
    
    print 'DEDUPE...'

    for name in mc_models.VECTORS_MODEL_NAMES:

        num_updated = mc_models.dedupe_reindex(index_name = index_name,
                                               doc_type = doc_type,
                                               vectors_model = name,
                                               )
    
    print 'DEDUPE_UPDATED',num_updated
    
    # random set of queries
    
    r_at_k = {}

    dedupe_true = {}
    dedupe_prob = {}

    done = {}
        
    for rep_model_name in rep_model_names:

        print 'STARTING',rep_model_name

        done[rep_model_name] = set()
        
        r_at_k[rep_model_name] = {'at_04':0,
                                  'at_10':0,
                                  'at_50':0,
                                  }
        
        if rep_model_name == 'all_true':
            dedupe_true[rep_model_name] = ([1.0] * 500) + ([0.0] * 500)
            dedupe_prob[rep_model_name] = [1.0] * 1000
            continue
        
        if rep_model_name == 'random_guess':
            dedupe_true[rep_model_name] = ([1.0] * 500) + ([0.0] * 500)
            dedupe_prob[rep_model_name] = [random() for x in xrange(1000)]
            continue
        
        rep_model = mc_models.VECTORS_MODEL_NAMES[rep_model_name]()
        
        dedupe_true[rep_model_name] = []
        dedupe_prob[rep_model_name] = []

        # Insert fake zero as shown here? Don't think this actually does much to solve the stability
        #  problems with the P/R curve:
        #  https://github.com/scikit-learn/scikit-learn/issues/4223
        
        dedupe_true[rep_model_name].append(0)
        dedupe_prob[rep_model_name].append(0.0)
        
        for c,hh in enumerate(the_gen(num_queries, do_img_data = False)):
            
            group_num = hh['group_num']
            
            query = rep_model.img_to_es_query(img_fn = hh['fn'])

            if mc_config.LOW_LEVEL:
                rr = es.search(index = index_name,
                               doc_type = doc_type,
                               body = query,
                               fields = ['_id', 'group_num'],
                               size = 100,#max_num, #50,
                               timeout = '5s',
                               )
            else:
                rr = nes.search_terms(body = query,
                                      fields = ['_id', 'group_num'],
                                      size = 100,#max_num, #50,
                                      timeout = '5s',
                                      )
            
            rr = rr['hits']['hits']

            #print ('GOT','query-(id,grp):',(hh['_id'],group_num),'result-(id,grp):',[(x['_id'],x['fields']['group_num'][0]) for x in rr])
            
            at_4 = len([1
                        for x
                        in rr[:4]
                        if (x['fields']['group_num'][0] == hh['group_num']) and (x['_id'] != hh['_id'])
                        ]) / (hh['num_instances_this_object'] - 1.0)
            at_10 = len([1
                         for x
                         in rr[:10]
                         if (x['fields']['group_num'][0] == hh['group_num']) and (x['_id'] != hh['_id'])
                         ]) / (hh['num_instances_this_object'] - 1.0)
            at_50 = len([1
                         for x
                         in rr[:50]
                         if (x['fields']['group_num'][0] == hh['group_num']) and (x['_id'] != hh['_id'])
                         ]) / (hh['num_instances_this_object'] - 1.0)

            r_at_k[rep_model_name]['at_04'] += at_4
            r_at_k[rep_model_name]['at_10'] += at_10
            r_at_k[rep_model_name]['at_50'] += at_50
            
            #print 'at_4',at_4,'at_10',at_10,'at_50',at_50

            #if at_4:
            #    raw_input_enter()
            
            
            ### Dedupe stuff:

            zrr = rr[:]
            shuffle(zrr)
            
            for cc,hh2 in enumerate(zrr):

                #if cc % 50 == 0:
                #    print cc
                
                if (ignore_self) and (hh2['_id'] == hh['_id']):
                    continue

                if ((hh2['_id'], hh['_id']) in done[rep_model_name]) or ((hh2['_id'], hh['_id']) in done[rep_model_name]):
                    continue
                
                
                label = 0
                if hh2['fields']['group_num'][0] == hh['group_num']:
                    label = 1
                
                if True:
                    #attempt to keep these balanced:
                    
                    if num_true > num_false:
                        if label == 1:
                            continue
                    elif num_false > num_true:
                        if label == 0:
                            continue
                
                if label == 1:
                    num_true += 1
                else:
                    num_false += 1

                done[rep_model_name].add((hh2['_id'], hh['_id']))
                
                dedupe_true[rep_model_name].append(label)
                dedupe_prob[rep_model_name].append(hh2['_score'])
            
                
                
            print c, rep_model_name, r_at_k[rep_model_name]

        print 'RECALL_@_K:',rep_model_name,[(x,100 * y / float(num_queries)) for x,y in r_at_k[rep_model_name].items()]

    ### Plot Precision-Recall curves:

    from math import log

    plt.clf()
    
    for mn in rep_model_names:
        
        if len(dedupe_prob[mn]) == 0:
            # Failed to find any matches at all...
            continue
        
        zz = MinMaxScaler(feature_range=(0, 1))
        dedupe_prob[mn] = zz.fit_transform(dedupe_prob[mn])
        
        precision[mn], recall[mn], _ = precision_recall_curve(dedupe_true[mn],
                                                              dedupe_prob[mn],
                                                              pos_label = 1,
                                                              )
        
        average_precision[mn] = average_precision_score(dedupe_true[mn],
                                                        dedupe_prob[mn],
                                                        average="micro",
                                                        )
    
        print mn,'num_true',num_true,'num_false',num_false
        print mn,'TRUE     ',dedupe_true[mn][:10]
        print mn,'PROB     ',dedupe_prob[mn][:10]
        print mn,'precision',list(precision[mn])[:10]
        print mn,'recall   ',list(recall[mn])[:10]

        if False:
            for thresh in np.linspace(0, 1, 20):
                print
                print 'THRESH:',thresh
                print classification_report(dedupe_true[mn],
                                            (np.array(dedupe_prob[mn]) > thresh),
                                            target_names=['diff','same'],
                                            )
                raw_input_enter()
        
        ### Plot Precision-Recall curve for each class:
        
        plt.plot(recall[mn],
                 precision[mn],
                 label='"{0}" model. Average precision = {1:0.2f}'.format(mn,
                                                                          average_precision[mn],
                                                                          ),
                 )

        print 'PLOTTED',mn
    
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.xlabel('Recall')
    plt.ylabel('Precision')
    plt.title('Precision-Recall Curve for Dedupe Task')
    plt.legend(loc="lower right")

    print 'SHOW...'
    
    #plt.show()
    
    fn_out = 'eval.png'
    plt.savefig(fn_out)
    from os import system
    system('open ' + fn_out)
    
    print 'CLEAN_UP...'

    if mc_config.LOW_LEVEL:
        if es.indices.exists(index_name):
            print ('DELETE_INDEX...', index_name)
            es.indices.delete(index = index_name)
            print ('DELETED')
    else:
        nes.delete_index()
            
    print 'PRECISION_@_K:',[(mm,[(x,100 * y / float(num_queries)) for x,y in zz.items()]) for mm,zz in r_at_k.items()]
        
    print 'DONE'



def short_ex(rr):
    """
    Condense ES explains.

    Note: Quick throw-away function. Ignore.
    """

    assert False,'WIP'
    
    paths = []

    def ex(rr, depth, path):
        if depth > 2:
            return
        #print rr['description'].replace(' ','_').replace('(','{').replace(')','}').replace(':','') + '(',
        if not rr['details']:
            path.append('=' + str(rr['value']))
            paths.append(path)

        if 'of:' in rr['description']:
            print ' '.join(rr['description'].split()[-2:-1]) + '(',
        else:
            print '<<' + rr['description'] + '>>',
            return

        #print 'func(',
        for c,a in enumerate(rr['details']):
            ex(a, depth + 1, path[:] + [str(rr['description'].replace('\x01', '').replace('\x00', ''))[:50]])
            if c != len(rr['details']) - 1:
                print ',',
        print '=',rr['value'],
        print ')',

    for hit in rr['hits']['hits']:
        ex(hit['_explanation'], 0, [])
        print
        print

    print 
    print 'PATHS:'
    
    for path in paths:
        print 'path',path
        
    raw_input_enter()

    
def test_scoring_sim(docs = ['the brown fox is nice', 'the house is big', 'nice fox', 'the fish is fat', 'fox'],
                     query = 'fox is big',
                     #docs = ['a','b','c','d','e','e'],
                     #query = 'a b v z',
                     index_name = 'query_test',
                     doc_type = 'query_test_doc',
                     via_cli = False,
                     ):
    """
    Let's see if we can exactly reproduce the Lucene classic scoring function.

    Run this test to evaluate the simulated index vs real ElasticSearch.
    """
    
    from string import lowercase,letters,digits
    from random import choice
    
    #docs = [choice(lowercase)+choice(lowercase) for x in xrange(10)]
    #query = ' '.join([choice(docs) for x in xrange(2)])

    print 'DOCS:'
    for doc in docs:
        print repr(doc)

    print
    print 'QUERY:'
    print repr(query)

    print
    
    ## This is optional:
    
    qq = {}
    qq_rev = {}
    qq_num = [0]
    
    def doc_to_term_counts(zz):
        """
        Avoid potential problems related to ES analyzers by converting term keys from strings to integer IDs.
        
        If you don't want this, just replace with `Counter(zz.split())`.
        """

        if True:
            # Disabled, seems it's not needed for our "should match" queries.
            return Counter(zz.split())
        else:
            rh = {}
            for w,cnt in Counter(zz.split()).items():
                if w not in qq:
                    qq_num[0] += 1
                    qq[w] = qq_num[0]
                    qq_rev[qq_num[0]] = w
                rh[qq[w]] = cnt
            print (zz,'->',{qq_rev[x]:y for x,y in rh.items()})
            print 'aa',rh
            return rh

    
    ## If you want to test the scoring without the full ES emulator:

    if False:    
        xx = mc_neighbors.LuceneScoringClassic()
        
        for x in docs:
            #xx.add_doc(Counter(x.split()))
            xx.add_doc(doc_to_term_counts(x))

        for a,b in list(sorted([xx.score(Counter(query.split()), Counter(x.split()))
                                for x
                                in docs
                                ],
                               reverse = True)):
            print 'SCORE',a,b

    
    ## Try multiple ES versions and compare results:
    
    results = []
    
    for es in [mc_neighbors.ElasticSearchEmulator(),
               mc_neighbors.low_level_es_connect(),
               ]:

        print
        print 'START',es.__class__.__name__
        
        if es.indices.exists(index_name):
            es.indices.delete(index = index_name)

        es.indices.create(index = index_name,
                          body = {'settings': {'number_of_shards': 1, #must be 1, or scoring has problems.
                                               'number_of_replicas': 0,
                                               },
                                  },
                          #ignore = 400,
                          )

        for c,doc in enumerate(docs):
            es.index(index = index_name,
                     doc_type = doc_type,
                     id = str(c),
                     body = doc_to_term_counts(doc),
                     )
        
        es.indices.refresh(index = index_name)
        
        aa = {'query': {'bool': {'should': [{'term': {x:y}} for x,y in doc_to_term_counts(query).items()] } } } 
        
        rr = es.search(index = index_name,
                       doc_type = doc_type,
                       body = aa,
                       explain = True,
                       )

        ## NOTE: Hits with tied scores are returned from ES in unpredictable order. Let's re-sort using IDs as tie-breakers:
        
        rr['hits']['hits'] = [c for (a,b),c in sorted([((x['_score'], x['_id']),x) for x in rr['hits']['hits']], reverse=True)]
        
        #print pretty_print(rr)

        #short_ex(rr)
        
        for hit in rr['hits']['hits']:
            print '-->score:','%.12f' % hit['_score'],'id:',hit['_id'], 'source:',hit['_source']

        
        results.append((es.__class__.__name__,
                        tuple([x['_id'] for x in rr['hits']['hits'] if x['_score'] > 0.0]),
                        ))

    print
    print 'FINAL_RESULTS:'

    ml = max([len(x) for x,y in results])
    
    first = False
    any_failed = False
    for nm, rr in results:

        print '-->',nm.ljust(ml),
        
        if first is False:
            first = rr
            print 'SAME',
        elif (first != rr):
            any_failed = True
            print 'DIFF',
        else:
            print 'SAME',

        print rr
            
    assert not any_failed,'FAILED'

    print
    print 'ALL_PASSED'

functions=['eval_demo',
           'test_scoring_sim',
           'hpo_vector_models',
           ]

def main():
    setup_main(functions,
               globals(),
               'mediachain-indexer-eval',
               )

if __name__ == '__main__':
    main()

