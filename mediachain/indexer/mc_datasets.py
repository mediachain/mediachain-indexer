#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Contains downloaders and iterators for image datasets.

Most of these are downloaded on-demand when you first use their iterator. Download of datasets that are much larger, or
requiring more setup (e.g. Getty) must be manually initiated by calling their download function.

The 2 dataset types:

   - Training/Evaluation: each sample has 'num_instances_this_object' and 'group_num' keys, indicating that there are
     multiple versions of each image / object in this dataset.

   - Ingestion: without the 'group_num' and 'num_instances_this_object' keys. Instead, each sample
     is in the format expected by `mc_ingest.ingest_bulk`.

Note: All image datasets can be setup for ingestion, but only datasets that have multiple versions of each image / object
can be used for training / evaluation.
"""

import zipfile
import tarfile

import sys

from os import mkdir, listdir, makedirs, walk
from os.path import exists,join
from time import sleep
import json
import os
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

from mc_generic import group, setup_main, raw_input_enter, download_streamed, tcache, pretty_print, tarfile_extract_if_not_exists
import mc_config
import mc_ingest

##
#### START TRAINING/EVALUATION DATASET ITERATORS:
##


copydays_links = {'original':'http://pascal.inrialpes.fr/data/holidays/copydays_original.tar.gz',
                  'cropped':'http://pascal.inrialpes.fr/data/holidays/copydays_crop.tar.gz',
                  'scale_attacked':'http://pascal.inrialpes.fr/data/holidays/copydays_jpeg.tar.gz',
                  'strong_attacked':'http://pascal.inrialpes.fr/data/holidays/copydays_strong.tar.gz',
                  }

def download_copydays(check_extract = True):
    """
    Dataset:     Copydays
    Credit:      Herve Jegou, Matthijs Douze and Cordelia Schmid. Hamming Embedding and Weak geometry consistency for large scale image search.
    Project URL: http://lear.inrialpes.fr/~jegou/data.php#copydays
    Description: Each image has suffered three kinds of artificial attacks: JPEG, cropping and "strong".
    Stats:       Dataset size: 1491 images in total: 500 queries and 991 corresponding relevant images

    Naming Format: E.g. `200001.jpg`. Files that share the same first 5 digits are in the same group.
    """

    try:
        #TODO - better check.
        if len(listdir('datasets/copydays/cropped/crops/80')) == 157:
            check_extract = False
    except KeyboardInterrupt:
        raise                                 
    except:
        pass
    
    for name, url in copydays_links.items():
        
        fn_out = 'datasets/copydays/' + name + '.tar.gz'
        
        dir_out_full = 'datasets/copydays/' + name + '/'
        
        if not exists(dir_out_full):
            makedirs(dir_out_full)
        
        download_streamed(url = url,
                          fn = fn_out,
                          use_temp = True,
                          verbose = True,
                          skip_existing = True,
                          )

        if check_extract:
            print 'CHECK_EXTRACT...',fn_out

            tarfile_extract_if_not_exists(fn_out,
                                          dir_out_full,
                                          )

            print 'EXTRACTED'

cache_groups_copydays = [False]
            
def iter_copydays(max_num = 0,
                  do_img_data = False,
                  check_extract = True,
                  ):
    print 'iter_copydays()'
        
    cache_dir = 'datasets/copydays/cache/'
    
    if not exists(cache_dir):
        makedirs(cache_dir)

    if cache_groups_copydays[0]:
        groups = cache_groups_copydays[0]
        
    else:

        download_copydays(check_extract = check_extract)

        groups = {}
        tot = 0

        for name, url in copydays_links.items():

            dir_out_full = 'datasets/copydays/' + name + '/'

            for dir_name, subdir_list, file_list in walk(dir_out_full):

                for fn in file_list:

                    if not fn.endswith('.jpg'):
                        continue

                    num = fn[:fn.index('.jpg')]

                    int(num)
                    
                    id = name + '|' + num
                    
                    grp = num[:-1]

                    fn = join(dir_name,
                              fn,
                              )

                    if grp not in groups:
                        groups[grp] = []

                    groups[grp].append((id, fn))

                    tot += 1

            #print 'LOADED',name,len(groups),tot

        #print 'LOADED_ALL',len(groups),tot
        
        cache_groups_copydays[0] = groups
        
    nn = 0
    for group_id, grp in groups.iteritems():

        assert len(grp) == len(set(grp)),grp
        
        for id,fn in grp:
            
            if nn % 100 == 0:
                print 'iter_copydays()',len(grp),id,fn
            
            if max_num and (nn == max_num):
                return
                        
            hh = {'_id': unicode(id),
                  'group_num':group_id,
                  'fn':fn,
                  }
            
            if do_img_data:
                with open(fn) as f:
                    d = f.read()
    
                d2 = tcache(cache_dir + 'cache_%s' % id,
                            mc_ingest.shrink_and_encode_image,
                            d,
                            )
                
                hh['image_thumb'] = d2
            
            ## Number of instances of this semantic object:
            
            hh['num_instances_this_object'] = len(grp)
            
            nn += 1
            
            yield hh


def download_ukbench(fn_out = 'datasets/ukbench/ukbench.zip'):
    """
    Dataset:     ukbench:
    Credit:      D. Nistér and H. Stewénius. Scalable recognition with a vocabulary tree.
    Project URL: http://vis.uky.edu/~stewe/ukbench/
    Description: 4 photos of each object. More difficult semantic-similarity test.
    Stats:       6376 images, 4 images per positive set, 2GB zipped

    Naming Format:  E.g. `ukbench00000.jpg`. Files with the same (number % 4) are in the same group.
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
                 do_img_data = False,
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
            
            hh = {'_id': unicode(nn),
                  'group_num':group_num,
                  'fn':fn,
                  }
            
            if do_img_data:
                
                with open(fn) as f:
                    d = f.read()
                    
                d2 = tcache(cache_dir + 'cache_%05d' % nn,
                            mc_ingest.shrink_and_encode_image,
                            d,
                            )
                
                hh['image_thumb'] = d2

            ## Number of instances of this semantic object:
            
            hh['num_instances_this_object'] = 4 
            
            yield hh

##
#### START INGESTION DATASETS ITERATORS:
##


def getty_create_dumps(INC_SIZE = 100,
                       NUM_WORKERS = 30,
                       ):
    """
    Quick and dirty Getty API downloader.
    """

    if not mc_config.MC_GETTY_KEY:
        print ('ERROR: set MC_GETTY_KEY environment variable.')
        exit(-1)
    
    if len(sys.argv) < 3:
        print ('NOTE: set MC_GETTY_KEY to Getty API key.')
        print ('Usage: python mediachain-indexer-ingest getty_create_dumps [archiv | entertainment | rf | small]')
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


def iter_json_getty(max_num = 0,
                    getty_path = 'getty_small/json/images/',
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
                  'img_data':mc_ingest.shrink_and_encode_image(img_data),
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


functions=['getty_create_dumps',
           ]

def main():
    setup_main(functions,
               globals(),
               'mediachain-indexer-datasets',
               )

if __name__ == '__main__':
    main()
