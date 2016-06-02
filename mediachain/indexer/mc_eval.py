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
from sklearn.metrics import precision_recall_curve
from sklearn.metrics import average_precision_score

import mc_ingest
import mc_dedupe
from mc_generic import group, setup_main, raw_input_enter, download_streamed, tcache
from random import sample
from os import mkdir, listdir, makedirs
from os.path import exists
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
            
            fn = 'ukbench_full/ukbench%05d.jpg' % nn
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


def eval(max_num = 100,
         num_queries = 10,
         the_gen = iter_ukbench,
         ):
    """
    Main evaluation script.
    """

    index_name = 'eval_index'
    doc_type = 'eval_doc'

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
    
    rh = {'at_4':0,
          'at_10':0,
          'at_50':0,
          }

    rep_model = mc_dedupe.REP_MODEL_NAMES['baseline_ng']()
    
    #for hh in sample(list(iter_ukbench()), num_queries):
    for c,hh in enumerate(iter_ukbench(num_queries, do_img_data = False)):
        
        group_num = hh['group_num']
        
        #print ('EXPECT',group_num)
        
        hsh_ng = rep_model.img_to_terms(img_fn = hh['fn'])
        
        query = {'query':{'filtered': {'query': {'bool': {'should': [{'term': {x:y}} for x,y in hsh_ng.items()] } } } } }

        #print query
        
        rr = es.search(index = index_name,
                       doc_type = doc_type,
                       body = query,
                       fields = ['_id', 'group_num'],
                       size = 50,
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
        
        rh['at_4'] += at_4
        rh['at_10'] += at_4
        rh['at_50'] += at_4
        
        #print 'at_4',at_4,'at_10',at_10,'at_50',at_50
        
        #if at_4:
        #    raw_input_enter()

        print c, rh
    
    print 'FINAL:',rh
    
    if es.indices.exists(index_name):
        print ('DELETE_INDEX...', index_name)
        es.indices.delete(index = index_name)
        print ('DELETED')
    
    print 'DONE'
    

functions=['eval',
           ]

def main():
    setup_main(functions,
               globals(),
               )

if __name__ == '__main__':
    main()

