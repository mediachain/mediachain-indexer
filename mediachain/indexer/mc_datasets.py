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
from os.path import exists,join,split as split_path
from time import sleep
import json
import os
from os import mkdir, listdir, walk, unlink, system, walk, rename
from Queue import Queue
from threading import current_thread,Thread

import requests
from random import shuffle, randint
from shutil import copyfile
import sys
from sys import exit

from datetime import datetime
from dateutil import parser as date_parser

from mc_generic import group, setup_main, raw_input_enter, download_streamed, tcache, pretty_print, tarfile_extract_if_not_exists, walk_files
import mc_config
import mc_ingest

import base64
import hashlib

from gzip import GzipFile
from subprocess import check_output
from pipes import quote as pipes_quote
import gzip

##
### Single-file compact sorted format:
##

VERSION_COMPACTSPLIT = '0001'

def convert_to_compactsplit(the_iter = False,
                            dir_out = False,
                            do_sort = True,
                            pre_split_num = 32,
                            max_num_per_split = 1000000,
                            num_digits = 4,
                            delete_existing = True,
                            max_num = 0,
                            confirm_clear = True,
                            via_cli = False,
                            ):
    """
    Post-processing step to convert getty images dataset to the new single-file format. Requires GNU `sort`.
    
    Optionally sort the file to ensure that any contiguous sample of the output file will be representative 
    of the overall dataset.
    
    Args:
        the_iter:           Input iterator that outputs dicts containing, at a minimum, an '_id' key.
        dir_out:            Prefix for output file name. A suffix of the form "-split-0001.gz" will be appended.
                            Can also be a path to a prefix name, e.g. 'output_prefix' or 'a/b/c/output_prefix'.
        getty_path:         Path to getty-formatted directory.
        do_sort:            Sort output files by ID afterward.
        pre_split_num:      Pre-split output into at least `pre_split_num` files, for easy parallel loading. Probably best to
                            error on the high side here.
        max_num_per_split:  If any of the splits have more than `max_num_per_split` records, then multiple files per split will 
                            be created.
        num_digits:         How much to zero-pad numbers in output. Should probably leave this at `4`.
        delete_existing:    Delete any existing files in `dir_out` folder.
        max_num:            Terminate early after `max_num` records.
        confirm_clear:      Prompt for confirmation before clearing output directory.
    """
    
    assert pre_split_num <= (10 ** num_digits - 1),(pre_split_num, num_digits)
    
    if not the_iter:
        
        assert via_cli,('REQUIRED: the_iter',)

        dir_out = 'getty_small_compactsplit'
        
        the_iter = iter_json_getty(getty_path = getty_path,
                                   max_num = max_num,
                                   )
    
    assert the_iter is not False
    assert dir_out is not False
    
    if exists(dir_out) and delete_existing:
        xdd = listdir(dir_out)
        
        for fn in xdd:
            fn = join(dir_out, fn)
            print ('UNLINK',fn)
        
        if len(xdd) and confirm_clear:
            print ('CLEAR FILES FROM EXISTING OUTPUT DIRECTORY?',dir_out)
            print ('PRESS ENTER TO CONFIRM',)
            raw_input()
            
        for fn in xdd:
            fn = join(dir_out, fn)
            unlink(fn)

    the_path, the_dir = split_path(dir_out)
    assert exists(the_path),('PATH_DOES_NOT_EXIST', the_path)
    assert the_dir,('SPECIFY_OUTPUT_DIR',the_dir)
    if not exists(dir_out):
        makedirs(dir_out)
    
    fn_out = join(dir_out, the_dir)
    
    fn_out_temp = fn_out + '-tempfile' + str(randint(1,1000000000000))
    fn_out_temp_2 = fn_out_temp + '-2'
    fn_out_temp_3 = fn_out_temp + '-3'
    
    try:
        with open(fn_out_temp, 'w') as f:
            
            for hh in the_iter:

                xid = hh['_id']
                if type(xid) == unicode:
                    xid = xid.encode('utf8')
                
                new_id = hashlib.md5(xid).hexdigest()
                
                #assert len(new_id) == 24,new_id ## Fixed-length makes sorting easier.
                assert '\t' not in new_id
                assert '\n' not in new_id            
                
                dd = json.dumps(hh, separators=(',', ':'))
                
                assert '\t' not in dd
                assert '\n' not in dd

                f.write(new_id + '\t' + dd + '\n')

        if not do_sort:
            rename(fn_out_temp, fn_out_temp_2)
        
        else:

            assert exists(fn_out_temp),(fn_out_temp,)
            
            ## Sort via gnu `sort`:
            
            print ('FILES', fn_out_temp, fn_out_temp_2)
            
            cmd = "LC_ALL=C sort --temporary-directory=%s %s > %s" % (pipes_quote(dir_out),
                                                                      pipes_quote(fn_out_temp),
                                                                      pipes_quote(fn_out_temp_2))
            
            print ('SORTING',cmd)
            
            rr = check_output(cmd,
                              shell = True,
                              executable = "/bin/bash",
                              )
            
            unlink(fn_out_temp)

        assert exists(fn_out_temp_2),(fn_out_temp_2,)
        
        print ('DONE_STEP_1', fn_out_temp_2)
        
        if pre_split_num == 1:
            print ('WRITE_AND_COMPRESS')
            
            with open(fn_out_temp_2) as src, gzip.open(fn_out_temp_3, 'wb') as dst:
                dst.writelines(src)

            unlink(fn_out_temp_2)
            
            rename(fn_out_temp_3, fn_out + (('-split-%0' + str(int(num_digits)) + 'd.gz') % 1))
            
        else:

            print ('SPLITTING_AND_COMPRESS',pre_split_num)

            
            
            hh = {x:[False, x, 0, 0] ## [output_file, this_split_num, file_num_this_split, record_count_this_split]
                  for x
                  in xrange(pre_split_num)
                  }
            
            with open(fn_out_temp_2) as f:
                for c,line in enumerate(f):
                    
                    xx = hh[c % pre_split_num]
                    
                    if (xx[0] is False) or (xx[3] > max_num_per_split):
                        if xx[0] is not False:
                            xx[0].close()
                        fn = fn_out + (('-compactsplit-v' + VERSION_COMPACTSPLIT + \
                                        '-%0' + str(int(num_digits)) + 'd-%0' + \
                                        str(int(num_digits)) + 'd.gz') % (xx[1], xx[2]))
                        print ('NEW_FILE',fn)
                        xx[0] = GzipFile(fn, 'w')
                        xx[2] += 1
                        xx[3] = 0
                    
                    xx[0].write(line)
                    xx[3] += 1
            
            unlink(fn_out_temp_2)
            
            for xx in hh.values():
                if xx[0] is not False:
                    print ('CLOSING_FILE',xx[0])
                    xx[0].close()
        
        print ('DONE',dir_out)
    
    finally:
        try: unlink(fn_out_temp)
        except: pass
        try: unlink(fn_out_temp_2)
        except: pass
        try: unlink(fn_out_temp_3)
        except: pass
        try: unlink(fn_out)
        except: pass


def iter_compactsplit(fn_in_glob = 'getty_small_compactsplit',
                      max_num = 0,
                      ):
    """
    Iterate records from files of the format created by `convert_getty_to_compact`.

    Args:
        fn_in_glob:   Input file(s) to read from. Wildcards allowed.
    """
    
    from os.path import expanduser, isfile
    from glob import glob
    
    fn_in_glob = expanduser(fn_in_glob)
    
    lst = list(glob(fn_in_glob))
    
    assert lst,('NO_FILES_FOUND',fn_in_glob)

    nn = 0
    
    for fn_in in lst:
        
        if isfile(fn_in):
            fns = [fn_in]
        else:
            fns = walk_files(fn_in)
        
        for fn in fns:
            
            if '-tempfile' in fn:
                continue
            
            with GzipFile(fn) as f:
                for line in f:
                    if nn % 100 == 0:
                        print ('iter_compactsplit', nn, max_num)
                    
                    new_id, dd = line.strip('\n').split('\t', 1)
                    yield json.loads(dd)
                    nn += 1

                    if max_num and (nn >= max_num):
                        break
                    

                
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
                       typ = False,
                       custom_ids = False,
                       via_cli = False,
                       ):
    """
    Quick and dirty Getty API downloader.
    
    Args:
        INC_SIZE:    Number of image IDs to include per Getty API call. (Seems the maximum allowed is 100.)
        NUM_WORKERS: Number of worker threads to use.
        typ:         Name of getty image IDs file to use.
        custom_ids:  List of getty image IDs to download.
    """

    if not mc_config.MC_GETTY_KEY:
        print ('ERROR: set MC_GETTY_KEY environment variable.')
        exit(-1)
    
    if len(sys.argv) < 3:
        print ('NOTE: set MC_GETTY_KEY to Getty API key.')
        print ('Usage: mediachain-indexer-datastes getty_create_dumps [archiv | entertainment | rf | small | custom] [custom-id] [custom-id] ...')
        print ('Example: mediachain-indexer-datasets getty_create_dumps arciv')
        print ('Example: mediachain-indexer-datasets getty_create_dumps custom JD6484-001 JD6484-002 JD6484-003')
        exit(-1)

    if via_cli:
        typ = sys.argv[2]
    else:
        if custom_ids:
            typ = 'custom'
        assert typ

    assert typ in ['archiv', 'entertainment', 'rf', 'small', 'custom']
    
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
        ## small 100-sample portion of `entertainment` dataset.
        set_entertainment = [x.strip() for x in open('Entertainment IDs.txt').read().split(',')[:100]]
        ids.update(set_entertainment)

    elif typ == 'custom':
        ## Specify IDs on command line:
        if via_cli:
            set_entertainment = sys.argv[3:]
        else:
            set_entertainment = custom_ids
        print 'DOING_CUSTOM',set_entertainment
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
        print ('JOINED',)

    if typ == 'custom':
        print 'DONE_CUSTOM',set_entertainment,'->',dd4
    print ('DONE ALL',)


def iter_json_getty(max_num = 0,
                    getty_path = 'getty_small/json/images/',
                    ):

    dd = getty_path

    assert '/json/images/' in dd,('Path must contain "/json/images/":',dd)
    
    dd3 = dd.replace('/json/images/','/downloads/')
    
    assert exists(dd),repr(dd)

    nn = 0
    for dir_name, subdir_list, file_list in walk(dd):

        for fn in file_list:

            if nn % 100 == 0:
                print ('iter_json_getty',nn)

            nn += 1

            if max_num and (nn + 1 >= max_num):
                print ('ENDING EARLY...',)
                return

            fn = join(dir_name,
                      fn,
                      )

            with open(fn) as f:
                try:
                    h = json.load(f)
                except:
                    assert False,('BAD_JSON',fn)

            #fn = join(dd3, 'thumb/' + ('/'.join(h['id'][:4])) + '/' + h['id'] + '.jpg')
            fn = join(dd3, 'comp/' + ('/'.join(h['id'][:4])) + '/' + h['id'] + '.jpg')

            if not exists(fn):
                print ('NOT_FOUND',fn)
                nn -= 1
                continue

            with open(fn) as f:
                img_data = f.read()
            
            try:
                img_data_uri = mc_ingest.shrink_and_encode_image(img_data)
            except:
                print ('BAD_IMAGE_FILE',fn)
                nn -= 1
                continue
                
            hh = {'_id':'getty_' + h['id'],
                  'dataset':'getty',
                  'title':h['title'],
                  'artist':h['artist'],
                  'description':h['caption'],
                  'keywords':' '.join([x['text'] for x in h['keywords'] if 'text' in x]),
                  'date_created':date_parser.parse(h['date_created']).isoformat(),  ## Leave as string, just normalize format.
                  'img_data':img_data_uri,
                  'source_record':h,
                  ## Not yet standardized:
                  'editorial_source':h['editorial_source'].get('name',None),
                  'collection_name':h['collection_name'],
                  }                
            
            rr = hh

            #print ('YIELDING',rr['_id'])

            yield rr

    print ('DONE_YIELD',nn)

    
def transforms_1():
    """
    WIP image transforms collection.
    """

    assert False,'TODO'
    
    from PIL import Image, ImageEnhance
    image = Image.open('downloads/jcfeb2011.jpg')

    ## Sharpness - 0.0 gives a blurred image, a factor of 1.0 gives the original image, and a factor of 2.0 gives a sharpened image:
    i2 = ImageEnhance.Sharpness(image).enhance(factor)
    
    ## An enhancement factor of 0.0 gives a black image. A factor of 1.0 gives the original image.
    i2 = ImageEnhance.Brightness(image).enhance(factor)
    
    ## An enhancement factor of 0.0 gives a solid grey image. A factor of 1.0 gives the original image.
    i2 = ImageEnhance.Contrast(image).enhance(factor)

    ## An enhancement factor of 0.0 gives a black and white image. A factor of 1.0 gives the original image.
    i2 = ImageEnhance.Color(image).enhance(factor)

    ## Rotate, -180 to 180, resample=Image.NEAREST, resample=Image.BILINEAR, resample=Image.BICUBIC:
    i2 = i1.rotate(45)

    ## Rotate without cropping:
    i2 = i2.rotate(45, expand=True)

    ## Specify transparent color:
    transparency = im.info['transparency'] 
    im.save('icon.gif', transparency=transparency)

    ## Crop off max 10% from each side:

    width, height = i1.size
    left = width / randint(10, 100)
    top = height / randint(10, 100)
    right = width - (width / randint(10, 100))
    bottom = height - (height / randint(10, 100))
    i2 = i1.crop((left, top, right, bottom))

    ### http://pillow.readthedocs.io/en/3.1.x/reference/ImageOps.html

    ## cutoff – How many percent to cut off from the histogram. ignore – The background pixel value (use None for no background).
    PIL.ImageOps.autocontrast(image, cutoff=0, ignore=None)

    ## The black and white arguments should be RGB tuples;
    PIL.ImageOps.colorize(image, black, white)

    ## Remove border from image. The same amount of pixels are removed from all four sides. 
    PIL.ImageOps.crop(image, border=0)

    ## Applies a non-linear mapping to the input image, in order to create a uniform distribution of grayscale values in the output image.
    PIL.ImageOps.equalize(image, mask=None)

    ## Add border to the image
    PIL.ImageOps.expand(image, border=0, fill=0)

    ## Returns a sized and cropped version of the image, cropped to the requested aspect ratio and size.
    PIL.ImageOps.fit(image, size, method=0, bleed=0.0, centering=(0.5, 0.5))

    ## Convert the image to grayscale.
    PIL.ImageOps.grayscale(image)

    ## Reduce the number of bits for each color channel.
    PIL.ImageOps.posterize(image, bits)
    
    #### Perspective transformation: http://stackoverflow.com/questions/14177744/how-does-perspective-transformation-work-in-pil

    #######
    ## http://cbio.ensmp.fr/~nvaroquaux/formations/scipy-lecture-notes/advanced/image_processing/index.html
    ## http://www.scipy-lectures.org/advanced/image_processing/

    from scipy import ndimage
    from scipy import misc
    lena = misc.imread('lena.png')
    
    ###

    ## Cropping
    lena = misc.lena()
    lx, ly = lena.shape
    crop_lena = lena[lx / 4: - lx / 4, ly / 4: - ly / 4]
    
    ## up <-> down flip
    flip_ud_lena = np.flipud(lena)
    
    ## rotation
    rotate_lena = ndimage.rotate(lena, 45)
    rotate_lena_noreshape = ndimage.rotate(lena, 45, reshape=False)

    
    ## Add noise to image:
    noisy = l + 0.4 * l.std() * np.random.random(l.shape)
    
    ## A Gaussian filter smoothes the noise out... and the edges as well:
    blurred_lena = ndimage.gaussian_filter(lena, sigma=3)
    very_blurred = ndimage.gaussian_filter(lena, sigma=5)

    ##A median filter preserves better the edges:
    med_denoised = ndimage.median_filter(noisy, 3)

    ##Total-variation (TV) denoising. Find a new image so that the total-variation of the image (integral of the norm L1 of the gradient) is minimized, while being close to the measured image:
    from skimage.filter import tv_denoise
    tv_denoised = tv_denoise(noisy, weight=50)

    
    ## Increase the weight of edges by adding an approximation of the Laplacian:
    filter_blurred_l = ndimage.gaussian_filter(blurred_l, 1)
    alpha = 30
    sharpened = blurred_l + alpha * (blurred_l - filter_blurred_l)


def iter_perturb_images(iter_in,
                        num_perturb_per_image = 5,
                        input_chunk_size = 1000,
                        use_uuid_group_nums = False,
                        thumbs_only = True,
                        funcs = [],
                        ):
    """
    Wraps the other dataset iterators and adds noise to the image. Replaces the `image_thumb` values of the input iterator.
    
    Each of the `funcs` should accept 2 arguments: an input file object and output file object.

    If the wrapped iterator has a `group_num` key, then that value is copied to the new perturbed images. Otherwise,
    new group_num values are created for each group.
    
    Reads in `input_chunk_size` images at a time, adds in perturbed versions, then the chunk is shuffled and output.
    
    Example: `input_chunk_size = 1000` and `num_perturb_per_image = 5` => 6000 images which are then shuffled and output.
    
    """
    assert False,'TODO'
    
    cur_group_num = 0
    
    for c,grp in enumerate(grp(iter_in)):
        pass
    

functions=['getty_create_dumps',
           'convert_to_compactsplit',
           ]

def main():
    setup_main(functions,
               globals(),
               'mediachain-indexer-datasets',
               )

if __name__ == '__main__':
    main()
