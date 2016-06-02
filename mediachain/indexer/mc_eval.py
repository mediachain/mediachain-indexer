#!/usr/bin/env python
# -*- coding: utf-8 -*-

__doc__ = \
"""
Evaluate dedupe and search models against labeled datasets.

Starting with:
   - Precision @ k for search.
   - Precision-recall-curve for dedupe.
"""

import matplotlib.pyplot as plt
import numpy as np
from sklearn.metrics import precision_recall_curve, average_precision_score, classification_report
from sklearn.preprocessing import MinMaxScaler
import matplotlib.pyplot as plt

import mc_ingest
import mc_dedupe
from mc_generic import group, setup_main, raw_input_enter, download_streamed, tcache
from random import sample
from os import mkdir, listdir, makedirs
from os.path import exists
from random import random, shuffle
import requests
import zipfile


def download_ukbench(fn_out = 'datasets/ukbench/ukbench.zip'):
    """
    Dataset:     ukbench:
    Credit:      D. Nistér and H. Stewénius. Scalable recognition with a vocabulary tree.
    Project URL: http://vis.uky.edu/~stewe/ukbench/
    Description: 4 photos of each object. More difficult semantic-similarity test.
    Stats:       6376 images, 4 images per positive set, 2GB zipped
    """
    
    dir_out = 'datasets/ukbench/'
    dir_out_full = 'datasets/ukbench/full/'

    if not exists(dir_out_full):
        makedirs(dir_out_full)
    
    download_streamed(url = 'http://vis.uky.edu/~stewe/ukbench/ukbench.zip',
                      fn = fn_out,
                      use_temp = True,
                      verbose = True,
                      skip_existing = True,
                      )
    
    if len(listdir(dir_out_full)) == 10200:
        return
    
    print 'EXTRACTING...'
    
    with zipfile.ZipFile(fn_out, 'r') as zf:
        zf.extractall(dir_out)
            
    print 'EXTRACTED',dir_out_full,len(listdir(dir_out_full))
    

def iter_ukbench(max_num = 0,
                 do_img_data = True,
                 ):

    fn_out = 'datasets/ukbench/ukbench.zip'
    download_ukbench(fn_out)

    cache_dir = 'datasets/ukbench/cache/'

    if not exists(cache_dir):
        makedirs(cache_dir)
    
    for group_num,grp in enumerate(group(xrange(10199 + 1), 4)):
        for nn in grp:
            
            if nn % 100 == 0:
                print 'iter_ukbench()',nn
            
            if max_num and (nn == max_num):
                return
            
            fn = 'datasets/ukbench/full/ukbench%05d.jpg' % nn
            #print ('fn',fn)

            with open(fn) as f:
                d = f.read()
            
            hh = {'_id': unicode(nn),
                  'group_num':group_num,
                  'fn':fn,
                  }
            
            if do_img_data:
                
                d2 = tcache(cache_dir + 'cache_%05d' % nn,
                            mc_ingest.shrink_and_encode_image,
                            d,
                            )
                
                hh['image_thumb'] = d2
            
            yield hh


def eval(max_num = 500,
         num_queries = 500,
         the_gen = iter_ukbench,
         rep_model_names = ['baseline_ng',
                            'baseline',
                            #'all_true',
                            'random_guess',
                            ],
         ignore_self = False,
         ):
    """
    Main evaluation script.
    """

    index_name = 'eval_index'
    doc_type = 'eval_doc'

    precision = {}
    recall = {}
    average_precision = {}

    num_true = 0
    num_false = 0
    
    es = mc_ingest.es_connect()
    
    if es.indices.exists(index_name):
        print ('DELETE_INDEX...', index_name)
        es.indices.delete(index = index_name)
        print ('DELETED')
    
    def iter_wrap():
        # Put in ingest_bulk() format:
        
        for hh in the_gen(max_num):
            
            xdoc = {'_op_type': 'index',
                    '_index': index_name,
                    '_type': doc_type,
                    }

            xdoc.update(hh)
                        
            yield xdoc

    print 'INSERTING...'
    
    num_inserted = mc_ingest.ingest_bulk(the_gen(max_num),
                                         index_name = index_name,
                                         doc_type = doc_type,
                                         redo_thumbs = False,
                                         )
    print 'INSERTED',num_inserted
    
    print 'DEDUPE...'
    
    num_updated = mc_dedupe.dedupe_reindex(index_name = index_name,
                                           doc_type = doc_type,
                                           duplicate_modes = ['baseline', 'baseline_ng'],
                                           )
    
    print 'DEDUPE_UPDATED',num_updated
    
    # random set of queries
    
    p_at_k = {}

    dedupe_true = {}
    dedupe_prob = {}

    done = {}
    
    all_ids = set([x['_id'] for x in iter_ukbench()])
    
    for rep_model_name in rep_model_names:

        print 'STARTING',rep_model_name

        done[rep_model_name] = set()
        
        p_at_k[rep_model_name] = {'at_04':0,
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
        
        rep_model = mc_dedupe.REP_MODEL_NAMES[rep_model_name]()
        
        dedupe_true[rep_model_name] = []
        dedupe_prob[rep_model_name] = []

        # Insert fake zero as shown here: https://github.com/scikit-learn/scikit-learn/issues/4223
        dedupe_true[rep_model_name].append(0)
        dedupe_prob[rep_model_name].append(0.0)
        
        for c,hh in enumerate(iter_ukbench(num_queries, do_img_data = False)):
            
            group_num = hh['group_num']
            
            query = rep_model.img_to_es_query(img_fn = hh['fn'])
            
            #print query
            
            rr = es.search(index = index_name,
                           doc_type = doc_type,
                           body = query,
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
                        ]) / 3.0
            at_10 = len([1
                         for x
                         in rr[:10]
                         if (x['fields']['group_num'][0] == hh['group_num']) and (x['_id'] != hh['_id'])
                         ]) / 3.0
            at_50 = len([1
                         for x
                         in rr[:50]
                         if (x['fields']['group_num'][0] == hh['group_num']) and (x['_id'] != hh['_id'])
                         ]) / 3.0

            p_at_k[rep_model_name]['at_04'] += at_4
            p_at_k[rep_model_name]['at_10'] += at_10
            p_at_k[rep_model_name]['at_50'] += at_50

            #print 'at_4',at_4,'at_10',at_10,'at_50',at_50

            #if at_4:
            #    raw_input_enter()


            ### Dedupe stuff:

            unseen_ids = all_ids.copy()

            zrr = rr[:]
            shuffle(zrr)
            
            for cc,hh2 in enumerate(zrr):

                #if cc % 50 == 0:
                #    print cc
                
                unseen_ids.discard(hh2['_id'])

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
            
            if False:
                for xid in unseen_ids:
                    if (int(xid) % 4) == (int(hh['_id']) % 4):
                        label = 1
                    else:
                        label = 0

                    if xid == hh['_id']:
                        continue

                    dedupe_true[rep_model_name].append(label)
                    dedupe_prob[rep_model_name].append(0.0)
                
                
            print c, rep_model_name, p_at_k[rep_model_name]

        print 'PRECISION_@_K:',rep_model_name,[(x,100 * y / float(num_queries)) for x,y in p_at_k[rep_model_name].items()]

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
    
    if es.indices.exists(index_name):
        print ('DELETE_INDEX...', index_name)
        es.indices.delete(index = index_name)
        print ('DELETED')
    
    print 'PRECISION_@_K:',[(mm,[(x,100 * y / float(num_queries)) for x,y in zz.items()]) for mm,zz in p_at_k.items()]
        
    print 'DONE'
    

functions=['eval',
           ]

def main():
    setup_main(functions,
               globals(),
               )

if __name__ == '__main__':
    main()

