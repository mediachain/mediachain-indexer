#!/usr/bin/env python


from os import system, walk, rename
from os.path import join, exists
from os import mkdir, listdir, walk, unlink, system, walk, rename, makedirs

import sys
from os import devnull
import subprocess
from threading  import Thread, current_thread
from Queue import Queue, Empty

from uuid import uuid4

import cPickle
import base64
import json

from tornado.httpclient import AsyncHTTPClient
    
from time import time, sleep

import tornado.ioloop
import tornado.gen
import tornado.web

from tempfile import NamedTemporaryFile, mkdtemp

import hashlib

from gzip import GzipFile

from random import random

####

def walk_files(dd, max_num = 0):
    """
    Simpler walking of all files under a directory.
    """
    
    nn = 0
    for dir_name, subdir_list, file_list in walk(dd):
        for fn in file_list:
            nn += 1
            if max_num and (nn > max_num):
                return
            
            fn = join(dir_name,
                      fn,
                      )

            yield fn


def line_tailer(ff,
                ending = '\n',
                sleep_time = 1.0,
                confirm_time = 60,
                confirm_min_attempts = 5,
                verbose = True,
                ):
    """
    Enumerate lines in a file object, and then continue to tail new lines as they arrive. After receiving an EOF,
    continue to try to read lines, until `confirm_time` seconds have elapse without seeing a new line. Useful for
    streaming data between a sequence of non-critical batch job steps, each being run by separate processes.
    
    This is only lightly tested.
    
    Intentionally unsupported features, for now:
        + Does not detect file rotation. May not be possible on file-like objects (vs filenames.)
        + Don't use in cases where the file is truncated and written to again.
        + Dealing with anything other than normal files, on a normal filesystem, on Linux.
    
    Note: If the upstream process writing to the file crashes mid-line, then the line tailer will intentionally wait
    forever, trying to get the rest of the line.
    
    Args:
        ff:                   File-like object.
        ending:               End of line characters. Not tested on anything other than '\n'.
        sleep_time:           Seconds to wait before retrying after receiving partial lines or EOF.
        confirm_time:         Seconds to wait after most recent EOF before giving up.
        confirm_min_attempts: Minimum number of attempts to read more after EOF. Useful for when system gets
                              suspended and resumed much later, or when system is under heavy load.
        verbose:              Verbose printing to stdout.
    """
    
    from time import time,sleep
    
    partial = False
    reached_end = False
    num_attempts = 0
    
    while True:
        
        for c,line in enumerate(ff):

            #print ('LINE_TAILER',c)
            
            reached_end = False
            num_attempts = 0
            
            if line.endswith(ending):
                partial = False
            else:
                partial = True
                ff.seek(ff.tell() -len(line))
                break
            
            yield line

        ff.seek(ff.tell()) ## apparently the way to read more after EOF.
        num_attempts += 1
        
        if partial:
            
            if verbose:
                print ('LINE_TAILER_PARTIAL_LINE...',
                       'sleep_time:', sleep_time,
                       )
            sleep(sleep_time)
        
        else:
            
            if reached_end is False:
                reached_end = time()

            tm = time() - reached_end
                
            if verbose:
                print ('LINE_TAILER_CONFIRMING_END...',
                       'tell:',ff.tell(),
                       'tm:', '%.3f' % tm,
                       'confirm_time:', confirm_time,
                       'num_attempts:', num_attempts,
                       'confirm_min_attempts:', confirm_min_attempts,
                       'sleep_time:', sleep_time,
                       )
                sleep(sleep_time)
            
            if (tm >= confirm_time) and (num_attempts >= confirm_min_attempts):
                if verbose:
                    print ('LINE_TAILER_FINISHED',)
                break


from ujson import loads as u_loads
from time import time

def _inner_iter_compactsplit(line, skip_callback):
    #print ('CSLINE',line[:40])
    #t0 = time()
    new_id, dd = line.strip('\n').split('\t', 1)
    
    rec = u_loads(dd)
    
    if skip_callback is not False:
        ## Return False to skip:
        sk = skip_callback(rec['_id'])
        if sk is False:
            ## todo re-add fastforward
            return False

        got_any = True

        #print ('INNER',(time() - t0) * 1000, 'ms')
        
        #yield sk, rec
        return sk, rec
    else:
        #print ('INNER',(time() - t0) * 1000, 'ms')
        #yield rec
        return rec
    
            
def iter_compactsplit(fn_in_glob,
                      do_tail = True,
                      skip_num = 0,
                      max_num = 0,
                      skip_callback = False,
                      do_binary_search = False,
                      do_fastforward = True,
                      num_threads = 16,
                      ):
    """
    Iterate records from files of the format created by `convert_getty_to_compact`.

    Will read from `-tempfile` inputs only if they're directly pointed to.
    
    Args:
        fn_in_glob:   Input file(s) to read from. Wildcards allowed.
        do_tail:      Read all records and then tail file to continue to read new records as they arrive.
    """

    assert not skip_num, 'todo'
    
    from os.path import expanduser, isfile
    from glob import glob
    from ujson import loads

    from multiprocessing.pool import ThreadPool

    skip_pool = ThreadPool(num_threads)

    #allow_tempfile = False
    #if '-tempfile' in fn_in_glob:
    #    allow_tempfile = True

    allow_tempfile = True
    
    fn_in_glob = expanduser(fn_in_glob)
    
    lst = list(glob(fn_in_glob))
    
    assert lst,('NO_FILES_FOUND',fn_in_glob)

    nn = 0
    tot_c = -1

    got_any = False

    #last_5 = {}
    
    ffs = []
    
    for fn_in in lst:
        
        if isfile(fn_in):
            fns = [fn_in]
        else:
            fns = walk_files(fn_in)
        
        for fn in list(fns):

            if not allow_tempfile:
                if '-tempfile' in fn:
                    continue
            
            if allow_tempfile and ('tempfile' in fn):
                ctx = open
            else:
                ctx = GzipFile
            
            #with ctx(fn) as f:
            
            f = ctx(fn)
            
            if do_tail:
                f = line_tailer(f)
            
            ffs.append((fn, f))

            #last_5[len(ffs)] = []
            
    t0 = time()
    
    while True:

        for cl, (fn, f) in enumerate(ffs[:]):

            batch = []

            for x in xrange(10):
                try:
                    line = f.next()
                    batch.append(skip_pool.apply_async(_inner_iter_compactsplit, (line, skip_callback)))
                    #batch.append(_inner_iter_compactsplit(line, skip_callback))
                except StopIteration as e:
                    del ffs[cl]
                    break
            
            if not batch:
                continue
            
            for xx in batch[:50]:
                xx = xx.get()
                if xx is False:
                    continue
                nn += 1
                #print ('yielding',xx)
                yield xx
            
            tot_c += len(batch)
            
            print ('iter_compactsplit', 'tot_c', tot_c, 'nn', nn, 'max_num', max_num,
                   'c_per_sec: %.2f' % (tot_c / (time() - t0)),
                   'nn_per_sec: %.2f' % (nn / (time() - t0)),
                   )
            
            if max_num and (nn >= max_num):
                break


                    
def shrink_and_encode_image(s = False,
                            img = False,
                            size = (1024, 1024),
                            to_base64 = True,
                            ):
    """
    Resize image to small size & base64 encode it.
    """
    if s is not False:
        img = Image.open(StringIO(s))
    else:
        assert img is not False,'Required: s or img.'
    
    print ('shrink_and_encode_image',img.size,'->',size)
    
    f2 = StringIO()
    if (img.size[0] > size[0]) or (img.size[1] > size[1]):
        img.thumbnail(size, Image.ANTIALIAS)
    img.convert('RGB').save(f2, "JPEG")
    f2.seek(0)
    s = f2.read()
    
    if to_base64:
        assert False,'todo'
        return data_pat + base64.b64encode(s)
    
    else:
        return s

from PIL import Image
from cStringIO import StringIO

data_pat = 'data:image/jpeg;base64,'
data_pat_2 = 'data:image/png;base64,'

def verify_img(buf):
    """
    Verify image.
    """
    
    sbuf = StringIO(buf)
    
    try:
        ## Basic check:
        img = Image.open(sbuf)
        img.verify()

        ## Detect truncated:
        img = Image.open(sbuf)
        img.load()
    except KeyboardInterrupt:
        raise
    except:
        print ('VERIFY_IMG_FAILED', buf[:100])
        return False
    return img


def decode_image(s):

    if s.startswith(data_pat):
        ss = s[len(data_pat):]
        
    elif s.startswith(data_pat_2):
        ss = s[len(data_pat_2):]
        
    else:
        assert False,('BAD_DATA_URL',s[:15])
    
    rr = base64.b64decode(ss)
    
    return rr

####

REDO_REDO = False

import errno

#curl -XGET 'http://localhost:9200/getty_test/image/_count?q=order_model_3:PLACEHOLDER&query_cache=false'


def iter_es(task_id = False,
            skip_callback = False,
            only_datasets = ['flickr100mm'],#['500px'], # 'flickr100mm', 
            max_num = 0,
            SORT_PRETTY = True,
            SHORT_SIZE = 1000,
            ):
    print ('ITER_ES',task_id, only_datasets)

    #if task_id == 'order_model_3':
    #    SHORT_SIZE = 100
    
    assert task_id, (task_id,)
    
    field_name = VALID_TASKS[task_id]['field_name']
    #fn_out, rec
    
    from mediachain.indexer import mc_neighbors
    from mediachain.indexer import mc_config
    from elasticsearch.helpers import parallel_bulk, scan
    from collections import Counter
    from time import sleep
    
    index_name = mc_config.MC_INDEX_NAME
    doc_type = mc_config.MC_DOC_TYPE
    
    es = mc_neighbors.low_level_es_connect()
    
    def do_commit(rrr):
        print ('COMMITTING BATCH...',len(rrr))

        #print ('SERVER_RESULTS_SAMPLE', rrr[5:])
        #raw_input_enter()
        
        from elasticsearch.helpers import parallel_bulk, scan

        ii = parallel_bulk(es,
                           rrr,
                           thread_count = 1,
                           chunk_size = 500,
                           max_chunk_bytes = 100 * 1024 * 1024, #100MB
                           )

        for is_success,res in ii:
            #print ('COMMITTED',is_success,res)
            pass

        rrr[:] = []
        print ('COMMITTED')

    rr_es = []
    
    from random import choice
    ww = set()
    words = Counter()
    with open('/datasets/datasets/retrain/deploy/order_model_3/typeahead_4.tsv') as f:
        for c, line in enumerate(f):
            if c == 10000:
                break
            xx = line.split('\t')[1]
            words.update(xx.split())
            ww.add(xx)
    ww = list(ww)
    #ww = [x for x,y in words.most_common()[:40000]]
    #ww = [x for x in words if x.endswith('y')]
    #ww = ['glacier national park']
    
    while True:

        w = False
        
        if SHORT_SIZE:

            #query = {"query": {"multi_match": {"query":  'backpacking',
            #                                   "fields": [ "*" ],
            #                                   "type":   "cross_fields"
            #                                   },
            #                       },
            #             }
            
            w = choice(ww)
            
            inner_part = [{"multi_match": {"query":  w,
                                          "fields": [ "*" ],
                                           "type":   "cross_fields",
                                           "minimum_should_match": "100%",
                                          },
                          }]
            
            inner_part += [{"term": {"source_dataset": x}}
                           for x
                           in only_datasets
                           ]
            
            if not REDO_REDO:
                inner_part += [{"missing" : { "field" : field_name }}]
                if field_name != 'aesthetics':
                    inner_part += [{"exists" : { "field" : 'aesthetics' }}]

            query = {"query": {"constant_score": {"filter": {"bool": {"must":inner_part}}}},
                     "size": SHORT_SIZE,
                     }
            
            print ('--------CHOSE', w, query)
            #sleep(5)

            query['timeout'] = '20s' ## only timeout for these
            
        elif only_datasets:

            inner_part = [{"term": {"source_dataset": x}}
                          for x
                          in only_datasets
                          ]

            if not REDO_REDO:
                inner_part += [{"missing" : { "field" : field_name }}]

            query = {"query": {"constant_score": {"filter": {"bool": {"must":inner_part}}}}}

        else:
            #query = {"query": {'match_all': {}}}

            assert False
            
            query = {"query": {"constant_score": {"filter": {"bool": {"must":[{"missing" : { "field" : field_name }}]
                                                                     }
                                                             }
                                                  }
                               }
                     }

        if SORT_PRETTY and (field_name != 'aesthetics'):
            assert field_name != 'aesthetics'
            ## https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-sort.html
            query["sort"] = [{"aesthetics.score" : {"order" : "desc"}}]

        
        ######
        
        try:
            
            ## Main loop:

            print ('START_ITER_ES_QUERY',task_id, index_name, doc_type, query)

            t1 = time()

            #raw_input()
            res = scan(client = es,
                       index = index_name,
                       doc_type = doc_type,
                       scroll = '100m', #TODO - hard coded.
                       query = query,
                       preserve_order = (SORT_PRETTY and SHORT_SIZE),
                       size = (SHORT_SIZE and SHORT_SIZE or 10000), ## size (per shard) of the batch send at each iteration.
                       request_timeout = 60.0 * 60,
                       )
            print ('SCAN_CALLED', time() - t1)

            nn = 0

            rr = []

            common_prefixes = Counter()
            skipped_prefixes = Counter()

            only_datasets = set(only_datasets)

            nn = 0
            t0 = time()
            
            for c, rec in enumerate(res):
                
                if SORT_PRETTY and (field_name != 'aesthetics'):
                    print ('SORT_PRETTY', c, rec['_source'].get('aesthetics',{}).get('score', None), w, rec['_id'])

                if SHORT_SIZE and (c >= SHORT_SIZE):
                    break
                    
                rec['_id'] = rec['_source']['native_id']

                assert '_' in rec['_id']

                #print ('RECREC',rec)

                if c % 100 == 0:
                    print ('iter_es', task_id, field_name,
                           'c', c, 'nn', nn, 'max_num', max_num,
                           'c_per_sec: %.2f' % (c / (time() - t0)),
                           'nn_per_sec: %.2f' % (nn / (time() - t0)),
                           )

                assert skip_callback
                
                if skip_callback is not False:
                    ## Return False to skip:
                    sk = skip_callback(rec['_source']['native_id'])
                    if sk is False:

                        print ('SKIP_2', rec['_source']['native_id'])
                        
                        ## todo re-add fastforward

                        #######
                        
                        ## Mark those with files already generated in ES so we don't try again:
                        
                        if (es is not False) and (task_id == 'order_model_3'):
                            
                            assert task_id == 'order_model_3'

                            assert '_' in rec['_id'], repr(rec['_id'])
                                                
                            hsh = hashlib.md5(str(rec['_id'])).hexdigest()
                    
                            field_name = VALID_TASKS[task_id]['field_name']

                            doc_update = {}

                            doc_update[field_name] = 'PLACEHOLDER' ## DONT PUSH FULL RECORDS TO ES FOR ORDER_MODEL_3

                            rr_es.append({'_op_type': 'update',
                                          '_index': index_name,
                                          '_type': doc_type,
                                          '_id': hsh,
                                          'body': {'doc':doc_update},
                                          })
                            
                            if len(rr_es) >= 100:
                                do_commit(rr_es)

                        #######
                        
                        continue
                    yield sk, rec
                    nn += 1

                else:
                    assert False, 'todo'
                    yield rec
                    nn += 1


            print ('DONE_ITER_ES', 'num_seen', nn, 'common:', common_prefixes.most_common(50), 'skipped:', skipped_prefixes.most_common(50))
            
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print ('------EXCEPTION',e)
        
        #sleep(60)

        print ('MAIN_LOOP_FINISHED', time() - t1)
            

def get_fn_out(_id, the_task, mode, id_already_hashed = False):
    assert not id_already_hashed
    assert '_' in _id,('_id',_id)
    xid = hashlib.md5(_id).hexdigest()
    dirs = '/datasets/datasets/' + the_task + '/' + xid[:3] + '/'
    if mode == 'w':
        if not exists(dirs):
            makedirs(dirs)
    return dirs + xid + '.json'


def image_worker_compactsplit(rec):
    the_id = rec['_id']

    image_bytes = decode_image(rec['img_data'])
    
    img = verify_img(image_bytes)
    
    if img is False:
        return False
    
    img_bytes_out = shrink_and_encode_image(img = img,
                                            size = (300, 300),
                                            to_base64 = False,
                                            )
    
    rec = {'_id':the_id,
           'data':img_bytes_out,
           }

    return rec

from time import sleep
from cStringIO import StringIO

def image_worker_es(rec):

    t3 = time()
    
    native_id = rec['_id']

    assert '_' in native_id,('native_id',native_id)
    _id = hashlib.md5(native_id).hexdigest()
    
    dr1 = '/datasets/datasets/indexer_cache/images/' + 'hh_' + '1024x1024' + '/'
    
    dr2 = dr1 + _id[:3] + '/'
    
    fn_cache = dr2 + _id + '.jpg'
    
    try:
        with open(fn_cache) as f:
            image_bytes = f.read()    
    except IOError as e:
        if e.errno != errno.ENOENT:
            raise
        print ('NOT_FOUND_image_worker_es',fn_cache)
        return False

    t3 = time()
    #print ('image_worker_es_AA',time() - t3)
    
    img = verify_img(image_bytes)
    
    if img is False:
        print ('VERIFY_FAILED_image_worker_es', native_id, fn_cache)
        return False
    
    img_bytes_out = shrink_and_encode_image(img = img,
                                            size = (299, 299),
                                            to_base64 = False,
                                            )
    
    rec = {'_id':native_id,
           'data':img_bytes_out,
           }

    #print ('image_worker_es_BB',time() - t3)
    
    return rec

import numpy as np
import numpy

#SERVER_HOST = 'http://52.23.88.183' #OLD
#SERVER_HOST = 'http://54.87.157.158'        #NEW
SERVER_HOST = 'http://54.85.138.129'

def calc_global_score(score):
    assert False, 'todo'
    if ii['_source']['source']['name'] == 'unsplash.com':
        score += 0.2
    
    if ii['_source']['source_dataset'] == 'pexels':
        score += 1.0
    
    if ii['_source']['source_dataset'] == '500px':
        score += 0.1
    
    if ii['_source']['source_dataset'] == 'dpla':
        score -= 1.0
    
    return score

def start_server(fns_in = False,
                 batch_size = 100,
                 skip_resize = False,
                 TASK_ID = False,
                 SERVER_PORT = False,
                 use_compactsplit_iter = False,
                 via_cli = False,
                 ):
    
    if via_cli:
        print sys.argv
        
        TASK_ID = sys.argv[2]        
        assert TASK_ID in VALID_TASKS,(TASK_ID, 'not in', VALID_TASKS)
        
        fns_in = sys.argv[3:]

        SERVER_PORT = VALID_TASKS[TASK_ID]['port']
        
    if (use_compactsplit_iter and not fns_in) or (not TASK_ID):
        print 'USAGE: ', sys.argv[0], sys.argv[1],'task_id','[compactsplit_dir]','[compactsplit_dir]','...'
        print 'VALID_TASK_IDS', VALID_TASKS
        exit(1)

    env_out = False

    #assert TASK_ID == 'order_model_3', TASK_ID

    if TASK_ID in ['order_model_3', 'aes_out', 'aesthetics_2', 'aesthetics_3']:
        from mediachain.indexer import mc_config
        from elasticsearch.helpers import parallel_bulk, scan, bulk
        from mediachain.indexer import mc_neighbors
        
        index_name = mc_config.MC_INDEX_NAME
        doc_type = mc_config.MC_DOC_TYPE

        es = mc_neighbors.low_level_es_connect() ## write back to DB live now

    if TASK_ID == 'order_model_3':
        
        import lmdb
        
        lmdb_path_out = '/datasets/datasets/retrain/deploy/order_model_3/order_model_3_image_vectors.lmdb'

        assert exists(lmdb_path_out), repr(lmdb_path_out)
        
        env_out = lmdb.open(lmdb_path_out,
                            create = False,
                            )

        print ('WRITING TO LMDB, PRESS ENTER', lmdb_path_out)
        #raw_input()
    
    def skip_callback(x_id, id_already_hashed = False):
        #return False to skip
        fn_out = get_fn_out(x_id, TASK_ID, 'r', id_already_hashed = id_already_hashed)
        if (not REDO_REDO) and exists(fn_out):  ##TEMPORARY - redo getty / pexels stuff.
            print ('SKIP',fn_out)
            return False
        return fn_out
    
    if use_compactsplit_iter:
        the_iter = lambda: iter_compactsplit(fns_in,
                                             skip_callback = skip_callback,
                                             )
        image_worker = image_worker_compactsplit
    
    else:
        the_iter = lambda: iter_es(task_id = TASK_ID,
                                   skip_callback = skip_callback,
                                   )
        image_worker = image_worker_es

    print ('STARTING',fns_in)
        
    qq_in = Queue()
    qq_out = Queue()
    pending_batches = {}
    
    class handle_get_batch(tornado.web.RequestHandler):
        
        @tornado.gen.coroutine
        def get(self):
        
            try:
                batch_d = qq_in.get_nowait()
                print ('SENT_TASK',)
            except Empty:
                #batch_d = base64.b64encode(cPickle.dumps(False))
                batch_d = 'STAwCi4='
                print ('NO_TASKS_AVAILABLE',)
                
            self.write(batch_d)

    
    class handle_finish_batch(tornado.web.RequestHandler):
        
        @tornado.gen.coroutine
        def post(self):
            
            d = self.request.body
            
            rbatch = json.loads(d)
            
            print ('FINISH_BATCH', rbatch['batch_id'], len(rbatch['batch']))

            rr_e = []
            rr_es = []
            
            for rec in rbatch['batch']:
                fn_out = get_fn_out(rec['_id'], rec['task_id'], 'w')
                with open(fn_out, 'w') as f_out:
                    f_out.write(json.dumps(rec))
                print ('WROTE', fn_out)
                
                if (rec['task_id'] == 'order_model_3') and (env_out is not False):
                    iv = rec['image_vectors'][0]
                    
                    assert '_' in rec['_id'], repr(rec['_id'])
                    
                    hsh = hashlib.md5(str(rec['_id'])).hexdigest()
                    
                    assert len(iv) in [1024, 128], ('UNKNOWN_VECTOR_SIZE', len(iv))
                    
                    iv = np.array(iv, dtype=np.float16)
                    
                    ft = StringIO()

                    np.save(ft,iv)
                    
                    d2 = ft.getvalue()
                    
                    rr_e.append((hsh, d2))

                    if (es is not False):
                        
                        assert rec['task_id'] == 'order_model_3'
                        
                        field_name = VALID_TASKS[rec['task_id']]['field_name']
                        
                        doc_update = {}
                        
                        doc_update[field_name] = 'PLACEHOLDER' ## DONT PUSH FULL RECORDS TO ES FOR ORDER_MODEL_3
                        
                        rr_es.append({'_op_type': 'update',
                                      '_index': index_name,
                                      '_type': doc_type,
                                      '_id': hsh,
                                      'body': {'doc':doc_update},
                                      })

                if (rec['task_id'] in ['aes_out', 'aesthetics_2', 'aesthetics_3']):
                    
                    #assert 'rule_of_thirds' in rec, repr(rec)
                    
                    assert '_' in rec['_id'], repr(rec['_id'])
                    
                    hsh = hashlib.md5(str(rec['_id'])).hexdigest()
                    
                    doc_update = {}
                    
                    field_name = VALID_TASKS[rec['task_id']]['field_name']
                    
                    doc_update[field_name] = rec

                    #if rec['task_id'] == 'aes_out':
                    #    assert 'score' in rec, repr(rec)[:500]
                    #    global_score = calc_global_score(rec['score'])
                    #    if global_score:
                    #        doc_update['score_global'] = global_score
                    #else:
                    #    assert 'score' not in rec, repr(rec)[:500]
                    
                    if (type(rec) == dict) and ('score' in rec):
                        doc_update['score_' + field_name] = rec['score']
                    
                    rr_es.append({'_op_type': 'update',
                                  '_index': index_name,
                                  '_type': doc_type,
                                  '_id': hsh,
                                  'body': {'doc':doc_update},
                                  #'_source': doc_update,
                                  })

            if rr_es:
                print ('COMMITTING BATCH TO ES...', len(rr_es),)# rr_es[:1])
                
                #print ('SERVER_RESULTS_SAMPLE', rr_es[5:])
                #raw_input_enter()
        
                ii = bulk(es,
                          rr_es,
                          #thread_count = 1,
                          #chunk_size = 500,
                          #max_chunk_bytes = 100 * 1024 * 1024, #100MB
                          )

                #print ('RESULT', list(ii)[:5])

                            
            if (env_out is not False) and (rr_e):
                print ('WRITE_LMDB', env_out.info())
                t1 = time()

                success = False
                while not success:
                    txn = env_out.begin(write = True)
                    curs = txn.cursor()
                    try:
                        curs.putmulti(rr_e,
                                      dupdata = False,
                                      )
                        txn.commit()
                        success = True
                    except lmdb.MapFullError:
                        txn.abort()
                        curr_limit = env_out.info()['map_size']
                        new_limit = curr_limit * 2
                        #try:
                        print ('>>> Doubling LMDB map size to ', new_limit >> 20, 'MB')
                        env_out.set_mapsize(new_limit)
                        #except:
                        #    print ('ERROR - INSUFFICIENT FREE RAM. Free up some RAM, then press enter to retry.')
                        #    exit()
                        
                        print ('RETRYING...')
                    
                    del txn
                    del curs
                    

                print ('WROTE_LMDB', time() - t1, lmdb_path_out)
                        
                
            qq_out.put(rbatch['batch_id'])


    
    if not exists('aes_out/'):
        mkdir('aes_out/')


    import base58
    import multiprocessing
    
    image_pool = multiprocessing.Pool(max(30, multiprocessing.cpu_count() - 1))
    
    def iter_batches(size):

        #for cfn, fn_in in enumerate(fns_in):
        if True:
            #'{"rule_of_thirds": -0.02248, "vivid_color": -0.64457, "symmetry": 0.11528, "color_harmony": 0.0013, "depth_of_field": -0.12935, "object": -0.509, "content": -0.17186, "score": 0.37013, "lighting": -0.23806, "balance": 0.03836, "repetition": 0.1986, "_id": "getty_3273331", "motion_blur": -0.09343}'
                        
            buf = []
            for fn_out, rec in the_iter():
                
                ##fn_out = get_fn_out(rec['_id'], 'r')
                ##
                ##if exists(fn_out):
                ##    continue

                #the_id = rec['_id']

                #image_bytes = decode_image(rec['img_data'])
                #
                #img = verify_img(image_bytes)
                #
                #if img is False:
                #    continue
                
                #if skip_resize:
                #    img_bytes_out = image_bytes
                #    print 'skip_resize'
                #else:                
                #    img_bytes_out = shrink_and_encode_image(img = img,
                #                                            size = (300, 300),
                #                                            to_base64 = False,
                #                                            )                
                #rec = {'_id':the_id,
                #       'data':img_bytes_out,
                #       }
                
                #buf.append(rec)

                print ('BATCH_ADD')
                buf.append(image_pool.apply_async(image_worker, (rec,)))
                
                if len(buf) == size:
                    print ('YIELD_BATCH',len(buf))
                    
                    #buf = image_pool.map(image_worker, buf)
                    #buf = [x for x in buf if x is not False]
                    #assert len(buf[0].keys()) == 2, buf[0].keys()

                    t2 = time()
                    print ('START_BATCH',len(buf))
                    buf = [x.get() for x in buf]
                    print ('END_BATCH',len(buf), time()-t2)

                    buf = [x for x in buf if x is not False]
                    if buf:
                        assert len(buf[0].keys()) == 2, buf[0].keys()
                        yield buf[:]
                        buf[:] = []
            if buf:
                print ('YIELD_BATCH',len(buf))
                
                #buf = image_pool.map(image_worker, buf)
                #buf = [x for x in buf if x is not False]
                #assert len(buf[0].keys()) == 2, buf[0].keys()

                buf = [x.get() for x in buf]
                buf = [x for x in buf if x is not False]
                if buf:
                    assert len(buf[0].keys()) == 2, buf[0].keys()
                
                    yield buf[:]
                    buf[:] = []
            
            print ('DONE_iter_batches',)

        
    def load_batches():
        print 'START_LOAD_BATCHES()'
        #import traceback, sys
        #traceback.print_stack(file=sys.stdout)

        done_batches = []
        
        try:
            for batch in iter_batches(batch_size):

                batch_id = uuid4().hex
                
                batch = {'batch_id':batch_id,
                         'batch':batch,
                         'task_id':TASK_ID,
                         }
                batch = base64.b64encode(cPickle.dumps(batch))
                
                while True:
                    
                    while True:

                        print ('qq_in.qsize()', qq_in.qsize(),
                               'len(pending_batches)', len(pending_batches),
                               #'done_batches',done_batches,
                               )
                        
                        try:
                            x_batch_id = qq_out.get_nowait()
                        except Empty:
                            break

                        done_batches.append(x_batch_id)
                        
                        print ('BATCH_DONE', x_batch_id)
                        if x_batch_id in pending_batches:
                            del pending_batches[x_batch_id]
                        else:
                            print ('GOT_OLD_BATCH??', x_batch_id)
                    
                    if qq_in.qsize() > 10:
                        while qq_in.qsize():
                            sleep(0.1)
                        continue

                    if len(pending_batches) >= 50:
                        for old_batch_id, (tt, old_batch) in pending_batches.items():
                            if time() - tt > 120:
                                pending_batches[old_batch_id] = [time(), old_batch]
                                del pending_batches[old_batch_id]
                                qq_in.put(old_batch)
                        sleep(1.0)
                        continue
                    
                    break
                
                pending_batches[batch_id] = [time(), batch]
                qq_in.put(batch)
                
        except:
            import sys, traceback, os
            exc_info = sys.exc_info()
            traceback.print_exception(*exc_info)
            os._exit(-1)
    
    t = Thread(target = load_batches)
    t.daemon = True
    t.start()

    print 'STARTING_TORNADO'
    
    # Create the web server 
    application = tornado.web.Application([(r'/get_batch', handle_get_batch),
                                           (r'/finish_batch', handle_finish_batch),
                                           ],
                                          debug = False,
                                          )
    
    application.listen(SERVER_PORT)
    tornado.ioloop.IOLoop.instance().set_blocking_log_threshold(0.5)
    tornado.ioloop.IOLoop.instance().start()



from time import sleep


def start_client(TASK_ID = False,
                 SERVER_PORT = False,
                 SAVE_LOCALLY = True,
                 via_cli = False,
                 ):

    if via_cli:
        print sys.argv
        
        TASK_ID = sys.argv[2]        
        assert TASK_ID in VALID_TASKS,(TASK_ID, 'not in', VALID_TASKS)

        SERVER_PORT = VALID_TASKS[TASK_ID]['port']
    
    if (not TASK_ID) or (not SERVER_PORT):
        print 'USAGE: ', sys.argv[0], sys.argv[1],'task_id'
        print 'VALID_TASK_IDS',VALID_TASKS
        exit(1)
    
    the_client_worker = VALID_TASKS[TASK_ID]['func']
        
    qq_input = Queue()
    qq_output = Queue()
    qq_shutdown = Queue()
        
    @tornado.gen.coroutine
    def get_batches():
                
        while True:

            while qq_input.qsize():
                yield tornado.gen.sleep(0.25)
            
            url = SERVER_HOST  + ':' + SERVER_PORT + '/get_batch'

            print ('URL',url)
            
            while True:
                try:
                    response = yield AsyncHTTPClient().fetch(url,
                                                             connect_timeout = 20,
                                                             request_timeout = 30,
                                                             )
                    d = response.body
                except Exception as e:
                    print ('CONNECT_FAIL', url, e)
                    yield tornado.gen.sleep(1)
                    continue
                break
            
            batch = cPickle.loads(base64.b64decode(d))
            
            if batch is False:
                print ('NO_TASKS_AVAILABLE',)
                yield tornado.gen.sleep(1.0)
                continue
            
            qq_input.put(batch)
            
            batch_id = batch['batch_id']
            
            @tornado.gen.coroutine
            def body_producer(write):
                while True:                                
                    try:
                        batch = qq_output.get_nowait()
                    except Empty:
                        print ('ERROR? - trying to /finish_batch but none to send.',)
                        yield tornado.gen.sleep(1.0)
                        continue
                    break
                
                print ('WRITING_BATCH',len(batch))

                if SAVE_LOCALLY:
                    for cc, rec in enumerate(json.loads(batch)['batch']):
                        try:
                            fn_out = get_fn_out(rec['_id'], rec['task_id'], 'w')
                            if cc == 0:
                                print ('SAVE_LOCAL_COPY', fn_out)
                            with open(fn_out, 'w') as f_out:
                                f_out.write(json.dumps(rec))
                        except Exception as e:
                            print ('EXCEPTION_SAVING', e)
                
                yield write(batch)


            url = SERVER_HOST + ':' + SERVER_PORT + '/finish_batch'
            
            while qq_output.qsize():
                try:
                    response = yield AsyncHTTPClient().fetch(url,
                                                             connect_timeout = 20,
                                                             request_timeout = 200,
                                                             body_producer = body_producer,
                                                             method='POST',
                                                             #allow_nonstandard_methods = True, ## body for GET
                                                             )
                except Exception as e:
                    print ('CONNECT_FAIL', url, e)
                    yield tornado.gen.sleep(1)
                    continue
                break

            if qq_shutdown.qsize():
                ## shutdown after batch is written:
                print ('SHUTDOWN1',)
                sleep(3)
                print ('SHUTDOWN2',)
                import os
                os._exit(-1)

    
    t = Thread(target = the_client_worker,
               args = (qq_input,
                       qq_output,
                       qq_shutdown,
                       TASK_ID,
                       ),
               )
    t.daemon = True
    t.start()
    
    print 'STARTING_TORNADO'
    
    tornado.ioloop.IOLoop.current().spawn_callback(get_batches)
    
    tornado.ioloop.IOLoop.instance().set_blocking_log_threshold(0.5)
    tornado.ioloop.IOLoop.instance().start()




def usage(functions):
    print 'Usage: ' + sys.argv[0] + ' function_name'
    print 'Functions:'
    for xx in functions:
        print xx
    sys.exit(1)


def set_console_title(title):
    from os import system
    cmd = "printf '\033k%s\033\\'" % title
    system(cmd)
        

def main():
    import sys
    
    functions=['start_server',
               'start_client',
               ]
    
    if len(sys.argv) < 2:
        usage(functions)
        return
    
    f = sys.argv[1]
    
    if f not in functions:
        print 'FUNCTION NOT FOUND:',f
        usage(functions)
        return
    
    title = sys.argv[0] + ' ' + f
    set_console_title(title)

    print ('STARTING', f)
    
    try:
        ff=globals()[f]
    except KeyboardInterrupt:
        raise
    
    ff(via_cli = True)


VALID_TASKS = {'aes_out':{'port':'6008', 'func':'client_worker_mlb.client_worker_mlb', 'field_name':'aesthetics'},
               'aes_unsplash_out_v1':{'port':'6009', 'func_name':'client_worker_tf.client_worker_tf', 'field_name':'aes_unsplash_out_v1'},
               'order_model':{'port':'6010',
                              'func_name':'client_worker_order.client_worker_order',
                              'field_name':'order_model',
                              'order_model_path':'/zdrive/order-embedding/snapshots/order',
                              },
               'order_model_2':{'port':'6011',
                                'func_name':'client_worker_order.client_worker_order',
                                'field_name':'order_model_2',
                                'order_model_path':'/zdrive/order-embedding/snapshots_coco_new/coco_new_2097000_order_snap_1',
                                },
               'order_model_3':{'port':'6012',
                                'func_name':'client_worker_order.client_worker_order',
                                'field_name':'order_model_3',
                                'order_model_path':'/zdrive/order-embedding/snapshots_coco_new/order_model_3',
                                },
               'aesthetics_2':{'port':'6013', 'func_name':'client_worker_finetune.client_worker_finetune', 'field_name':'aesthetics_2', 'desc':'vgg19_unsplash'},
               'aesthetics_3':{'port':'6014', 'func_name':'client_worker_finetune.client_worker_finetune', 'field_name':'aesthetics_3', 'desc':'vgg19_500px'},
              }

import importlib

for name, hh in VALID_TASKS.items():
    if 'func' not in hh:
        hh['func'] = importlib.import_module(hh['func_name'])

if __name__ == '__main__':
    main()

