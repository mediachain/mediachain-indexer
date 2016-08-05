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

from mc_generic import setup_main, group, raw_input_enter, pretty_print, intget, print_config, sleep_loud

import mc_config
import mc_datasets
import mc_neighbors

from time import sleep,time
import json
import os
from os.path import exists, join
from os import mkdir, listdir, makedirs, walk, rename, unlink

from Queue import Queue
from threading import current_thread,Thread

import requests
from random import shuffle
from shutil import copyfile
import sys
from sys import exit

from datetime import datetime
from dateutil import parser as date_parser
from hashlib import md5

from PIL import Image
from cStringIO import StringIO

import binascii
import base64
import base58

import numpy as np

import imagehash
import itertools

import hashlib

import elasticsearch.exceptions
from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk, scan

data_pat = 'data:image/jpeg;base64,'
data_pat_2 = 'data:image/png;base64,'


def verify_img(buf):
    """
    Verify image.
    """
    from PIL import Image
    from cStringIO import StringIO

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
        return False
    return True


def shrink_and_encode_image(s, size = (1024, 1024), to_base64 = True):
    """
    Resize image to small size & base64 encode it.
    """
    img = Image.open(StringIO(s))
    
    print ('shrink_and_encode_image',img.size,'->',size)
    
    if (img.size[0] > size[0]) or (img.size[1] > size[1]):
        f2 = StringIO()
        img.thumbnail(size, Image.ANTIALIAS)
        img.convert('RGB').save(f2, "JPEG")
        f2.seek(0)
        s = f2.read()
    
    else:
        ## Detect truncated and throw error:
        img.load()

    if to_base64:        
        return data_pat + base64.b64encode(s)
    
    else:
        return s


def decode_image(s):

    if s.startswith(data_pat):
        ss = s[len(data_pat):]
        
    elif s.startswith(data_pat_2):
        ss = s[len(data_pat_2):]
        
    else:
        assert False,('BAD_DATA_URL',s[:15])

    try:
        rr = base64.b64decode(ss)
    except KeyboardInterrupt:
        raise
    except:
        ## Temporary workaround for broken encoder:
        rr = base64.urlsafe_b64decode(ss)

    return rr


def get_image_cache_url(_id,
                        image_cache_host,
                        image_cache_dir,
                        ):
    """
    Replaced by ``.

    Temporary solution to high-res image caching on the Indexer, for use by front-end. Currently reuses crawler caches.
    
    twistd -n web -p 6008 --path /datasets/datasets/indexer_cache/
    
    TODO: Temporary, as we work the best way to pass high-res images.
    """
    
    if not image_cache_host.endswith('/'):
        image_cache_host = image_cache_host + '/'
    
    import hashlib
    
    #print ('make_cache_url',_id)

    #print ('get_cache_url()', _id)

    if _id.startswith('pexels_'):
        
        _id = _id.split('_')[-1]
        
        xid = hashlib.md5(_id).hexdigest()

        fn = ('/'.join(xid[:4])) + '/' + xid + '.jpg'

        #normalizer_names['pexels']['dir_cache']
        
        real_fn = '/datasets/datasets/pexels/images_1920_1280/' + fn

        #assert exists(real_fn),real_fn
                
        r = image_cache_host + 'pe/' + fn
        
        #print ('get_cache_url result:', r)
        
        return r

    elif _id.startswith('getty_'):
        
        xid = _id.split('_')[-1]
        
        fn = ('/'.join(xid[:4])) + '/' + xid + '.jpg'

        #ln -s /datasets2/datasets/getty_unpack/getty_all_images/ /datasets/datasets/indexer_cache/gg
        
        return image_cache_host + 'gg/' + fn
        
        base = '/datasets/datasets/indexer_cache/images/'
        
        for xdir in ['ga/',
                     'ge/',
                     'gr/',
                     ]:
            
            xfn = base + xdir + fn
            
            if exists(xfn):

                #print 'CACHE_FOUND',xfn
                
                return image_cache_host + xdir + fn
            
        #print '!! CACHE_FAILED',xfn
        return None
    
    elif _id.startswith('eyeem_'):
        
        xid = hashlib.md5(_id.split('_')[-1]).hexdigest()
        
        fn = ('/'.join(xid[:4])) + '/' + xid + '.jpg'
        
        return image_cache_host + 'ey/' + fn
    
    else:
        return None
        #assert False,repr(_id)


def lookup_cached_image(_id,
                        do_sizes = ['1024x1024','256x256'], #'original', 
                        return_as_urls = True,
                        check_exists = False,
                        image_hash_sha256 = False,
                        image_cache_dir = mc_config.MC_IMAGE_CACHE_DIR,
                        image_cache_host = mc_config.MC_IMAGE_CACHE_HOST,
                        ):
    """
    Retrieves latest cached version of this image.
    
    See also: `cache_image()`
    
    Args:
        _id:               Note - Assumed to be already cryptographically hashed, for even distribution.
        do_sizes:          Output resized versions, with these sizes.
        return_as_urls:    Return as URLs, otherwise return filenames.
        check_exists:      Check that the files actually exist. Can considerably slow down lookups.
                           TODO: auto-generate lower res versions from the higher-res versions, if needed?
        image_hash_sha256: TODO: Optionally verify that retrieved original matches this hash?
    """
    
    if '_' in _id:
        ## TODO: either md5 of native_id or not:
        _id = hashlib.md5(_id).hexdigest()
    
    if not image_cache_dir.endswith('/'):
        image_cache_dir = image_cache_dir + '/'
    
    if not image_cache_host.endswith('/'):
        image_cache_host = image_cache_host + '/'
    
    rh = {}
    
    for size in do_sizes:
        
        dr1 = image_cache_dir + 'hh_' + size + '/'
        
        dr2 = dr1 + _id[:3] + '/'
        
        fn_cache = dr2 + _id + '.jpg'

        ## TODO: handle cache misses here?
        
        if check_exists:
            assert exists(fn_cache), fn_cache
        
        if return_as_urls:
            rh[size] = image_cache_host + 'hh_' + size + '/' + _id[:3] + '/' + _id + '.jpg'
        
        else:
            
            rh[size] = fn_cache

    #print ('lookup_cached_image',rh)
    
    return rh


def cache_image(_id,
                image_hash_sha256 = False,
                image_func = False,
                image_bytes = False,
                do_sizes = ['1024x1024','256x256'], #'original', 
                return_as_urls = True,
                image_cache_dir = mc_config.MC_IMAGE_CACHE_DIR,
                image_cache_host = mc_config.MC_IMAGE_CACHE_HOST,
                ):
    """
    NOTE: unfinished draft of an API shape, quickly sketched up at 3am. Consider this unfinished and untested.
    Ping me before rewriting.
    
    Cache an image for later use by other stages of the pipeline.
    
    Uses plain files for now, because that's what works the best for HTTP serving of image files to the Frontend.
    
    Args:
       _id:            Note - Assumed to be already cryptographically hashed, for even distribution.
       image_hash_sha256:     SHA256 hash of image bytes content. If you want to do lazy-loading with `image_func`,
                       you should pass this hash. Otherwise `image_func` must always be called.
       image_func:     When called, returns an open file ready for reading. Used for lazy image retrieval.
       image_bytes:    Image content bytes.
       do_sizes:       Output resized versions with these sizes.
       return_as_urls: Return as URLs, otherwise return filenames.
    
    Process:
       1) Check if hash of image file for this `_id` has changed.
       2) Store (_id -> content_hash) and (_id -> image_content)
    
    Components using this cache:
       - Ingestion via Indexer.
       - Content-based vector calculation.
       - HTTP server for cached images for Frontend.
    
    TODO:
       - Clear out all sizes of outdated images (upon hash change), instead of just the replacing the sizes specified 
         during the `cache_image()` call. See partial implementation below.
       - Unlikely to be perfectly atomic. Look deeper into whether the failure scenarios are acceptable.
    
    Open questions, not too important for now:
       - Expiration? Intentionally delaying a decision on this for now.
       - flush()'s required to make this more likely to be atomic on more filesystems?
       - Add file locking to avoid stampeeds from multiple threads?
    
    See also: `lookup_cached_image()`
    """
    
    if '_' in _id:
        ## TODO: either md5 of native_id or not:
        print ('WARN: Should pass hashed IDs to `cache_image()`, so we get an even distribution between dirs.')
        _id = hashlib.md5(_id).hexdigest()
    
    assert (image_func is not False) or (image_bytes is not False),'Must pass either image_func or image_bytes.'
    assert not ((image_func is not False) and (image_bytes is not False)),'Cannot pass both image_func and image_bytes.'
    assert (image_func is False) or \
           ((image_func is not False) and (image_hash_sha256 is not False)),'image_hash_sha256 must be passed if image_func is passed.'
    
    if not image_cache_dir.endswith('/'):
        image_cache_dir = image_cache_dir + '/'
    
    if not image_cache_host.endswith('/'):
        image_cache_host = image_cache_host + '/'
    
    
    ## Check previously recorded hash of input file for this ID, vs expected hash:
    
    dr1_b = image_cache_dir + 'hh_hash/'
    
    fn_h = dr1_b + _id + '.hash'
    
    current_cached = False
    if exists(fn_h):
        with open(fn_h) as f:
            xx = f.read()
        xx = xx.split('\t')
        old_hsh = xx[-1]
        old_sizes = xx[:-1]
        if old_hsh == image_hash_sha256:
            current_cached = True
        else:
            ## TODO delete all old sizes, to be safer.
            pass
        

    ## Output resized images:
    
    rh = {}
        
    for size in do_sizes:

        assert '\t' not in size, repr(size)
        
                
        dr1 = image_cache_dir + 'hh_' + size + '/'
                
        dr2 = dr1 + _id[:3] + '/'
        
        fn_cache = dr2 + _id + '.jpg'
        
        url = image_cache_host + 'hh_' + size + '/' + _id[:3] + '/' + _id + '.jpg'
        
        ## Check if file is cached, and has not changed for this ID:
        
        if (not current_cached) or (not exists(fn_cache)):
            
            if not exists(dr1):
                mkdir(dr1)
            
            if not exists(dr1_b):
                mkdir(dr1_b)
            
            if not exists(dr2):
                mkdir(dr2)

            ## Retrieve image content from image_func, if needed:
            
            if (image_bytes is False) and (image_func is not False):                
                ## TODO - crash hard here on failure? Expect image_func to manage retries?:
                image_bytes = image_func().read()
            else:
                assert image_bytes is not False
            
            if image_hash_sha256 is False:
                ## When image_bytes is passed but not image_hash_sha256:
                image_hash_sha256 = hashlib.sha256(image_bytes).hexdigest()
            
                
            if size != 'original':
                iw, ih = size.split('x')
                iw, ih = int(iw), int(ih)
            
            try:
                bytes_out = shrink_and_encode_image(image_bytes,
                                                    size = (iw, ih),
                                                    to_base64 = False,
                                                    )
            except KeyboardInterrupt:
                raise
            except:
                ## TODO: bubbling up exceptions for now:
                raise
                print ('BAD_IMAGE_FILE',len(image_bytes),image_bytes[:100])
                continue
            
            with open(fn_cache, 'w') as f:
                f.write(bytes_out)
        
        if return_as_urls:
            rh[size] = url
        
        else:
            rh[size] = fn_cache
    
    
    ## Finally write out the hash file, after all image sizes have been written out to disk:
    ## Also record sizes, so they can hopefully be used to delete all sizes of old images upon next update.
    
    with open(fn_h + '.temp', 'w') as f:
        f.write(('\t'.join(do_sizes)) + '\t' + image_hash_sha256)
    
    rename(fn_h + '.temp',
           fn_h,
           )
    
    #print ('cache_image',rh)
    
    return rh


def aes_backfill(batch_size = 100,
                 index_name = mc_config.MC_INDEX_NAME,
                 doc_type = mc_config.MC_DOC_TYPE,
                 via_cli = False,
                 ):

    import ujson
    
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
        
    def do_commit(rrr):
        print ('COMMITTING BATCH...',len(rrr))
        
        if mc_config.LOW_LEVEL:
            from elasticsearch.helpers import parallel_bulk, scan
            
            ii = parallel_bulk(es,
                               rrr,
                               thread_count = 1,
                               chunk_size = 500,
                               max_chunk_bytes = 100 * 1024 * 1024, #100MB
                               )
        else:
            ii = nes.parallel_bulk(rrr)
        
        for is_success,res in ii:
            #print ('COMMITTED',is_success,res)
            pass
        
        rrr[:] = []
        print ('COMMITTED')
    
    def get_fn_out(_id, mode = 'w'):
        #print ('get_fn_out', _id)
        xid = hashlib.md5(_id).hexdigest()
        dd = '/datasets/datasets/aes_out/' + xid[:3] + '/'
        if mode == 'w':
            if not exists(dd):
                mkdir(dd)
        return dd + xid + '.json'
    
    ## Main loop:
    
    nn = 0

    rr = []
    
    for c,hit in enumerate(res):

        doc_update = {}

        native_id = hit['_source'].get('native_id') or hit['_id']
        
        assert '_' in native_id, hit
        
        if c % 1000 == 0:
            print ('LOOP:', c, native_id)
        
        fn_aes = get_fn_out(native_id, 'r')

        if not exists(fn_aes):
            continue

        with open(fn_aes) as f:
            aes = ujson.loads(f.read())
        
        doc_update['aesthetics'] = aes
        
        ## For now, only bother if there was an image:
        
        rr.append({'_op_type': 'update',
                   '_index': index_name,
                   '_type': doc_type,
                   '_id': hit['_id'],
                   'body': {'doc':doc_update},
                   })
        
        if nn % 50 == 0:
            print ('YES_AES', nn, 'of', c, hit['_source'].get('source_dataset'), aes)
        
        nn += 1
        
        #print ('ADD',c) #rr
        
        if len(rr) >= batch_size:
            do_commit(rr)
        
    if rr:
        do_commit(rr)
    
    print ('AES_ENRICHED',nn)
    
    if mc_config.LOW_LEVEL:
        print ('REFRESHING', index_name)
        es.indices.refresh(index = index_name)
        print ('REFRESHED',)
    else:
        nes.refresh_index()
    
    print ('DONE_ALL',)


def test_image_cache(via_cli = False):
    """
    Basic sanity check for image caching. TODO: more testing, instead of just running the code.
    """
    
    image_base64 = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAUAAAAFCAYAAACNbyblAAAAHElEQVQI12P4"\
                   "//8/w38GIAXDIBKE0DHxgljNBAAO9TXL0Y4OHwAAAABJRU5ErkJggg=="
    
    image_bytes = decode_image(image_base64)

    image_hash = hashlib.sha256(image_bytes).hexdigest()

    _id = hashlib.md5('test_123').hexdigest()

    def image_func():
        from cStringIO import StringIO
        ff = StringIO(image_bytes)
        return ff
    
    for as_urls in [True, False]:
        
        for use_image_func in [True, False]:
            
            rh1 = cache_image(_id,
                              image_hash_sha256 = image_hash if use_image_func else False,
                              image_func = image_func if use_image_func else False,
                              image_bytes = image_bytes if not use_image_func else False,
                              do_sizes = ['1024x1024','256x256','original'],
                              return_as_urls = as_urls,
                              )
            
            if not as_urls:
                for k,v in rh1.items():
                    assert exists(v),(as_urls, use_image_func, v)
            
            rh2 = lookup_cached_image(_id,
                                      do_sizes = ['1024x1024','256x256', 'original'],
                                      return_as_urls = as_urls,
                                      )

            if not as_urls:
                for k,v in rh2.items():
                    assert exists(v),(as_urls, use_image_func, v)

            for k,v in rh1.items():
                assert rh1[k] == rh2[k],(k, rh1[k], rh2[k])
                
    print ('FINISHED')
    

def ingest_bulk(iter_json = False,
                thread_count = 1,
                index_name = mc_config.MC_INDEX_NAME,
                doc_type = mc_config.MC_DOC_TYPE,
                search_after = False,
                redo_thumbs = True,
                ignore_thumbs = False,
                use_aggressive = True,
                refresh_after = True,
                thumbs_elsewhere = True,
                gen_dedupe_vectors = True,
                ):
    """
    Ingest Getty dumps from JSON files.
    
    Currently does not attempt to import media to the Mediachain chain.
    
    Args:
        iter_json:      Iterable of media objects, with `img_data` containing the raw-bytes image data.
        thread_count:   Number of parallel threads to use for ES insertion.
        index_name:     ES index name to use.
        doc_type:       ES document type to use.
        search_after:   Manually inspect ingested records after. Probably not needed anymore.
        redo_thumbs:    Whether to recalcuate 'image_thumb' from 'img_data'.
        ignore_thumbs:  Whether to ignore thumbnail generation entirely.
        use_aggressive: Use slow inserter that immediately indexes & refreshes after each item.
        
        auto_reindex_inactive:   Auto-reindex after `auto_reindex_inactive` seconds of inactivity.
        auto_reindex_max:        Auto-reindex at least every `auto_reindex_max` seconds, regardless of ingestion activity.

        thumbs_elsewhere: Don't store thumbs in ES database. TODO: store thumbs via new shared disk cache system.

        gen_dedupe_vectors: Continuously generate dedupe vectors.
    
    Returns:
        Number of inserted records.
    
    Examples:
        See `mc_test.py`
    """

    if gen_dedupe_vectors:
        import mc_models
        vmodel = mc_models.VectorsBaseline()
    
    index_settings = {'settings': {'number_of_shards': mc_config.MC_NUMBER_OF_SHARDS_INT,
                                   'number_of_replicas': mc_config.MC_NUMBER_OF_REPLICAS_INT,                             
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
                                                             ## Reserving space for a bunch of these now, since adding them 
                                                             ## later would require destroying the ES Index...:
                                                             'canonical_id':{'type':'string', 'index':'not_analyzed'},
                                                             'dedupe_id':{'type':'string', 'index':'not_analyzed'},
                                                             'native_id':{'type':'string', 'index':'not_analyzed'},
                                                             'source_id':{'type':'string', 'index':'not_analyzed'},
                                                             'license_id':{'type':'string', 'index':'not_analyzed'},
                                                             'content_id':{'type':'string', 'index':'not_analyzed'},
                                                             },
                                              },
                                   },
                      }
    
    if not iter_json:
        iter_json = mc_datasets.iter_json_getty()
    
    if mc_config.LOW_LEVEL:
        es = mc_neighbors.low_level_es_connect()
        
        if not es.indices.exists(index_name):
            print ('CREATE_INDEX...',index_name)
            es.indices.create(index = index_name,
                              body = index_settings,
                              #ignore = 400, # ignore already existing index
                              )
            
            print('CREATED',index_name)
    else:
        #NOT LOW_LEVEL:
        nes = mc_neighbors.high_level_connect(index_name = index_name,
                                              doc_type = doc_type,
                                              index_settings = index_settings,
                                              use_custom_parallel_bulk = use_aggressive,
                                              )
                
        nes.create_index()
            
    print('INSERTING...')

    def iter_wrap():
        # Put in parallel_bulk() format:

        nnn = 0 

        t0 = time()
        
        for hh in iter_json:
            
            if nnn % 100 == 0:
                print 'YIELDING_FOR_INSERT','num:',nnn, 'index_name:',index_name, 'doc_type:',doc_type,'per_second:',nnn / (time() - t0)
            
            xdoc = {'_op_type': 'index',
                    '_index': index_name,
                    '_type': doc_type,
                    }
            
            hh.update(xdoc)
            
            assert '_id' in hh,hh.keys()
            
            do_lazy = False ## TODO: temporarily disabling lazy image retrieval, for datasets ingested without `image_hash_sha256`.

            try:
                if (('thumbnail' in hh) and (('image_hash_sha256' in hh) or (do_lazy is False))) or ('img_data' in hh):
                    
                    ## Mediachain metadata formatted 'thumbnail':
                    
                    from cStringIO import StringIO
                    
                    def get_asset():
                        from mediachain.reader.api import open_binary_asset
                        with open_binary_asset(hh['thumbnail']) as f:
                            return StringIO(f.read())

                    if 'img_data' in hh:
                        fns = cache_image(_id = hh['_id'],
                                          image_bytes = decode_image(hh['img_data']),
                                          do_sizes = ['1024x1024','256x256'],
                                          return_as_urls = False,
                                          )

                    elif do_lazy:
                        fns = cache_image(_id = hh['_id'],
                                          image_hash_sha256 = hh['image_hash_sha256'],
                                          image_func = lambda: get_asset(),
                                          do_sizes = ['1024x1024','256x256'],
                                          return_as_urls = False,
                                          )

                    else:
                        
                        print ('NON-LAZY_IMAGE_RETRIEVAL',)
                        fns = cache_image(_id = hh['_id'],
                                          image_bytes = get_asset().read(),
                                          do_sizes = ['1024x1024','256x256'],
                                          return_as_urls = False,
                                          )

                    assert fns
                    
                    if gen_dedupe_vectors:
                        fn = fns['256x256']
                        with open(fn) as f:
                            d = f.read()
                        
                        vv = vmodel.img_to_terms(img_bytes = d)

                        print ('VECTORS',repr(vv)[:500])
                        
                        hh.update(vv)
                    
                    ## Add aspect ratio for all images:

                    with open(fns['256x256']) as f:
                        img = Image.open(f)
                        hh['aspect_ratio'] = img.size[0] / float(img.size[1])


                elif (hh.get('img_data') == 'NO_IMAGE') or (hh.get('image_thumb') == 'NO_IMAGE'):

                    ## One-off ignoring of thumbnail generation via `NO_IMAGE`:

                    if 'img_data' in hh:
                        del hh['img_data']

                    if 'image_thumb' in hh:
                        del hh['image_thumb']

                    ## Add aspect ratio for all images:

                    if 'aspect_ratio' not in hh:
                        hh['aspect_ratio'] = None

                else:
                    assert False, ('No thumbnail detected, and "NO_IMAGE" not used.',hh.keys())
                    
            except KeyboardInterrupt:
                raise
            except:
                ## TODO: a fix has been applied upstream of this step. This won't be needed
                ## once the datasets are reprocessed.
                print ('ERROR_RESIZING_IMAGE /datasets/datasets/error_record',)
                #with open('/datasets/datasets/error_record','w') as f:
                #    f.write(json.dumps(hh, indent=4))
                #raw_input()
                continue
            
            ## TODO: get dedupe to read from cache, because we're not saving `img_data` anymore?
            if 'img_data' in hh:
                del hh['img_data']

            chh = hh.copy()
            if 'image_thumb' in chh:
                del chh['image_thumb']
            
            nnn += 1
            
            yield hh
    
    gen = iter_wrap()
    
    def non_parallel_bulk(es,
                          the_iter,
                          *args, **kw):
        """
        Aggressive inserter that inserts & refreshes after every item.
        """
        print 'USING: NON_PARALLEL_BULK'
        
        for c,hh in enumerate(the_iter):
            
            #print 'NON_PARALLEL_BULK',repr(hh)[:100],'...'
            
            xaction = hh['_op_type']
            xindex = hh['_index']
            xtype = hh['_type']
            xid = hh['_id']
            
            for k,v in hh.items():
                if k.startswith('_'):
                    del hh[k]
            
            assert xaction == 'index',(xaction,)
            
            #print 'BODY',hh
            
            ## Retry, ignoring likey-transient errors:
            
            while True:
                try:
                    res = es.index(index = xindex, doc_type = xtype, id = xid, body = hh) ## TODO - re-add batching
                except elasticsearch.exceptions.ConnectionTimeout:
                    print ('elasticsearch.exceptions.ConnectionTimeout',)
                    sleep_loud(1)
                    continue
                except elasticsearch.exceptions.ConnectionError:
                    print ('elasticsearch.exceptions.ConnectionError',)
                    sleep_loud(1)
                    continue
                break
            
            #print 'DONE-NON_PARALLEL_BULK',xaction,xid
            
            yield True,res
            
            if (c > 0) and (c % 1000 == 0):
                t1 = time()
                print ('REFRESH-NON_PARALLEL_BULK',c)
                try:
                    es.indices.refresh(index = xindex)
                except KeyboardInterrupt:
                    raise
                except:
                    print 'REFRESH_ERROR'
                print 'REFRESHED',time() - t1
                
                if False:
                    try:
                        import mc_models
                        mc_models.dedupe_reindex_all()
                    except KeyboardInterrupt:
                        raise
                    except:
                        print '!!! REINDEX_ERROR:'
                        import traceback, sys, os
                        for line in traceback.format_exception(*sys.exc_info()):
                            print line,
                            
        print ('REFRESH-NON_PARALLEL_BULK',c)
        try:
            es.indices.refresh(index = xindex)
        except KeyboardInterrupt:
            raise
        except:
            print 'REFRESH_ERROR'
        print 'REFRESHED'
        
        print 'EXIT-LOOP_NON_PARALLEL_BULK'
        
        
    if use_aggressive:
        use_inserter = non_parallel_bulk
    else:
        use_inserter = parallel_bulk

    is_empty = True
    
    try:
        first = gen.next() ## TODO: parallel_bulk silently eats exceptions. Here's a quick hack to watch for errors.
        is_empty = False
    except StopIteration:
        print '!!!WARN: GOT EMPTY INPUT ITERATOR'

    if not is_empty:
        if mc_config.LOW_LEVEL:
            ii = use_inserter(es,
                              itertools.chain([first], gen),
                              thread_count = thread_count,
                              chunk_size = 1,
                              max_chunk_bytes = 100 * 1024 * 1024, #100MB
                              )
        else:
            ii = nes.parallel_bulk(itertools.chain([first], gen))

        for is_success,res in ii:
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

    rr = False
    
    if refresh_after:
        if mc_config.LOW_LEVEL:
            print ('REFRESHING', index_name)
            es.indices.refresh(index = index_name)
            print ('REFRESHED')
            rr = es.count(index_name)['count']
        else:
            nes.refresh_index()
            rr = nes.count()
        
    return rr


def tail_blockchain(via_cli = False):
    """
    Debugging tool - Watch blocks arrive from blockchain. 
    """
    from mc_simpleclient import SimpleClient

    cur = SimpleClient()
    
    for art in cur.get_artefacts():
        print ('ART:',time(),art)
    

def get_last_known_block_ref():
    last_block_file = os.path.join(os.path.expanduser('~'), '.mediachain',
                                   'last-known-block')
    try:
        with open(last_block_file) as f:
            return f.read().strip()
    except IOError:
        raise


def save_last_known_block_ref(ref):
    mediachain_dir = os.path.join(os.path.expanduser('~'), '.mediachain')
    last_block_file = os.path.join(mediachain_dir, 'last-known-block')
    try:
        os.makedirs(mediachain_dir)
        with open(last_block_file) as f:
            f.write(ref)
    except os.error:
        pass
    except IOError as e:
        print('ERROR SAVING BLOCK REF', e)
        pass


def receive_blockchain_into_indexer(last_block_ref = None,
                                    index_name = mc_config.MC_INDEX_NAME,
                                    doc_type = mc_config.MC_DOC_TYPE,
                                    via_cli = False,
                                    ):
    """
    Read media from Mediachain blockchain and write it into Indexer.
    
    Args:
        last_block_ref:  (Optional) Last block ref to start from.
        index_name:      Name of Indexer index to populate.
        doc_type:        Name of Indexer doc type.
    """
    
    from mc_simpleclient import SimpleClient
    
    cur = SimpleClient()

    catchup = ('--disable-catchup' not in sys.argv)

    if last_block_ref is None:
        last_block_ref = get_last_known_block_ref()

    def the_gen():
        ## Convert from blockchain format to Indexer format:

        for obj_info in cur.get_artefacts(catchup_blockchain=catchup,
                                          last_known_block_ref=last_block_ref,
                                          force_exit = via_cli): ## Force exit after loop is complete, if CLI.
            ref = obj_info['canonical_id']
            art = obj_info['record']

            # persist block ref for next run
            block_ref = obj_info['prev_block_ref']
            save_last_known_block_ref(block_ref)

            try:
                print 'GOT',art.get('type')
                
                if art['type'] != u'artefact':
                    continue
                
                ## TODO: Use all the same post-blockchain normalization / translation
                ## code as the non-blockchain pipeline:
                
                rh = art['meta']['data']
                
                ## TODO - Add normalizers here?
                
                if 'raw_ref' in art['meta']:
                    raw_ref = art['meta']['raw_ref']
                else:
                    assert False,('RAW_REF',repr(art)[:500])
                
                rh['ingested_indexer_utc'] = time()
                rh['latest_ref'] = base58.b58encode(raw_ref[u'@link'])
                rh['canonical_id'] = ref
                
                rh['old_id'] = rh['_id']
                
                rh['_id'] = rh['canonical_id']
                
                if 'translated_at' in art['meta']:
                    rh['date_translated'] = date_parser.parse(art['meta']['translated_at'])
                
                rhc = rh.copy()
                
                if 'img_data' in rhc:
                    del rhc['img_data']
                    
                if 'thumbnail_base64' in rhc:
                    del rhc['thumbnail_base64']
                
                print 'INSERT',rhc
                
                yield rh
                
            except KeyboardInterrupt:
                raise
            except:
                raise
                print ('!!!ARTEFACT PARSING ERROR:',)
                print repr(art)
                print 'TRACEBACK:'
                import traceback, sys, os
                for line in traceback.format_exception(*sys.exc_info()):
                    print line,
                exit(-1)
                
        print 'END ITER'
    
    ## Do the ingestion:
    
    nn = ingest_bulk(iter_json = the_gen(),
                     #index_name = index_name,
                     #doc_type = doc_type,
                     )
    
    print 'GRPC EXITED SUCCESSFULLY...'

    
    print 'DONE_INGEST',nn


def send_compactsplit_to_blockchain(path_glob = False,
                                    max_num = 5,
                                    normalizer_name = False,
                                    via_cli = False,
                                    ):
    """
    Read in from compactsplit dumps, write to blockchain.
    
    Why this endpoint instead of the `mediachain.client` endpoint? This endpoint allows us to do sophisticated
    dedupe analysis prior to sending media to the blockchain.
    
    Args:
        path_glob:             Directory containing compactsplit files.
        max_num:               End ingestion early after `max_num` records. For testing.
        index_name:            Name of Indexer index to populate.
        doc_type:              Name of Indexer doc type.
        normalizer_name:       Name or function for applying normalization / translation to records.
    """
    
    import sys
    
    from mc_datasets import iter_compactsplit
    from mc_generic import set_console_title
    from mc_normalize import apply_normalizer, normalizer_names
    
    from mc_simpleclient import SimpleClient
    
    if via_cli: 
        if (len(sys.argv) < 4):
            print ('Usage: mediachain-indexer-ingest' + sys.argv[1] + ' directory_containing_compactsplit_files [normalizer_name or auto]')
            print ('Normalizer names:', normalizer_names.keys())
            exit(-1)
        
        path_glob = sys.argv[2]

        normalizer_name = sys.argv[3]

        if normalizer_name not in normalizer_names:
            print ('INVALID:',normalizer_name)
            print ('Normalizer names:', normalizer_names.keys())
            exit(-1)

        set_console_title(sys.argv[0] + ' ' + sys.argv[1] + ' ' + sys.argv[2] + ' ' + sys.argv[3] + ' ' + str(max_num))
    
    else:
        assert path_glob
    
    ## Simple:

    the_iter = lambda : iter_compactsplit(path_glob, max_num = max_num)
    
    iter_json = apply_normalizer(iter_json,
                                 normalizer_name,
                                 )
    
    cur = SimpleClient()
    cur.write_artefacts(the_iter)        
    
    ## NOTE - May not reach here due to gRPC hang bug.
    
    print ('DONE ALL',)

    
def send_compactsplit_to_indexer(path_glob = False,
                                 max_num = 0,
                                 index_name = mc_config.MC_INDEX_NAME,
                                 doc_type = mc_config.MC_DOC_TYPE,
                                 auto_dedupe = False,
                                 extra_translator_func = False,
                                 #resize_images_again = False, ## already resized during compactsplit creation?
                                 via_cli = False,
                                 ):
    """
    [TESTING_ONLY] Read from compactsplit dumps, write directly to Indexer. (Without going through blockchain.)
    
    Args:
        path_glob:             Directory containing compactsplit files.
        max_num:               End ingestion early after `max_num` records. For testing.
        index_name:            Name of Indexer index to populate.
        doc_type:              Name of Indexer doc type.
        extra_translator_func: Function, or name of function, that applies normalization / translation to records.
    """
    
    from mc_datasets import iter_compactsplit
    from mc_generic import set_console_title
    from mc_normalize import apply_normalizer, normalizer_names
    
    if via_cli:
        if (len(sys.argv) < 4):
            print ('Usage: mediachain-indexer-ingest'  + ' ' + sys.argv[1] + ' directory_containing_compactsplit_files [normalizer_name or auto]')
            print ('Normalizer names:', normalizer_names.keys())
            exit(-1)
        
        path_glob = sys.argv[2]

        normalizer_name = sys.argv[3]

        if normalizer_name not in normalizer_names:
            print ('INVALID:',normalizer_name)
            print ('Normalizer names:', normalizer_names.keys())
            exit(-1)
        
        set_console_title(sys.argv[0] + ' ' + sys.argv[1] + ' ' + sys.argv[2] + ' ' + sys.argv[3] + ' ' + str(max_num))        
    else:
        assert path_glob
        
    iter_json = lambda : iter_compactsplit(path_glob,
                                           max_num = max_num,
                                           #resize_images_again = resize_images_again,
                                           )
    
    iter_json = apply_normalizer(iter_json,
                                 normalizer_name,
                                 )

    
    rr = ingest_bulk(iter_json = iter_json)


    if auto_dedupe:
        ## TODO: automatically do this for now, so we don't forget:
        import mc_models
        mc_models.dedupe_reindex_all()
    else:
        print 'NOT AUTOMATICALLY RUNNING DEDUPE.'

    return rr

def send_gettydump_to_indexer(max_num = 0,
                              getty_path = False,
                              index_name = mc_config.MC_INDEX_NAME,
                              doc_type = mc_config.MC_DOC_TYPE,
                              auto_dedupe = False,
                              via_cli = False,
                              *args,
                              **kw):
    """
    [DEPRECATED] Read Getty dumps, write directly to Indexer. (Without going through blockchain.)
    
    Args:
        getty_path: Path to getty image JSON. `False` to get path from command line args.
        index_name: Name of Indexer index to populate.
        doc_type:   Name of Indexer doc type.
    """

    print ('!!!DEPRECATED: Use `ingest_compactsplit_indexer` now instead.')
    
    if via_cli:
        if len(sys.argv) < 3:
            print 'Usage: ' + sys.argv[0] + ' ' + sys.argv[1] + ' getty_small/json/images/'
            exit(-1)
        
        getty_path = sys.argv[2]
    else:
        assert getty_path
    
    iter_json = mc_datasets.iter_json_getty(max_num = max_num,
                                            getty_path = getty_path,
                                            index_name = index_name,
                                            doc_type = doc_type,
                                            *args,
                                            **kw)

    ingest_bulk(iter_json = iter_json)

    if auto_dedupe:
        ## TODO: automatically do this for now, so we don't forget:
        import mc_models
        mc_models.dedupe_reindex_all()
    else:
        print 'NOT AUTOMATICALLY RUNNING DEDUPE.'




def search_by_image(fn = False,
                    limit = 5,
                    index_name = mc_config.MC_INDEX_NAME,
                    doc_type = mc_config.MC_DOC_TYPE,
                    via_cli = False,
                    ):
    """
    Command-line content-based image search.
    
    Example:
    $ mediachain-indexer-ingest ingest_gettydump
    $ mediachain-indexer-ingest search_by_image getty_small/downloads/thumb/5/3/1/7/531746924.jpg
    """
    
    if via_cli:
        if len(sys.argv) < 3:
            print 'Usage: ' + sys.argv[0] + ' ' + sys.argv[1] + ' <image_file_name> [limit_num] [index_name] [doc_type]'
            exit(-1)
        
        fn = sys.argv[2]
        
        if len(sys.argv) >= 4:
            limit = intget(sys.argv[3], 5)
        
        if len(sys.argv) >= 5:
            index_name = sys.argv[4]
        
        if len(sys.argv) >= 6:
            doc_type = sys.argv[5]
        
        if not exists(fn):
            print ('File Not Found:',fn)
            exit(-1)
    else:
        assert fn,'File name required.'
    
    with open(fn) as f:
        d = f.read()
    
    img_uri = shrink_and_encode_image(d)
    
    hh = requests.post(mc_config.MC_TEST_WEB_HOST + '/search',
                       headers = {'User-Agent':'MC_CLI 1.0'},
                       verify = False,
                       json = {"q_id":img_uri,
                               "limit":limit,
                               "include_self": True,
                               "index_name":index_name,
                               "doc_type":doc_type,
                               },
                       ).json()
    
    print pretty_print(hh)


def delete_index(index_name = mc_config.MC_INDEX_NAME,
                 doc_type = mc_config.MC_DOC_TYPE,
                 via_cli = False,
                 ):
    """
    Delete an Indexer index.
    """
    
    print('DELETE_INDEX',index_name)
    
    if mc_config.LOW_LEVEL:
        es = mc_neighbors.low_level_es_connect()
        
        if es.indices.exists(index_name):
            es.indices.delete(index = index_name)
        
    else:
        #NOT LOW_LEVEL:
        nes = mc_neighbors.high_level_connect(index_name = index_name,
                                              doc_type = doc_type,
                                              index_settings = index_settings,
                                              use_custom_parallel_bulk = use_aggressive,
                                              )
        
        nes.delete_index()
    
    print ('DELETED',index_name)


def refresh_index(index_name = mc_config.MC_INDEX_NAME,
                  via_cli = False,
                  ):
    """
    Refresh an Indexer index. NOTE: newly inserted / updated items are not searchable until index is refreshed.
    """
    
    if mc_config.LOW_LEVEL:
        print ('REFRESHING', index_name)
        es.indices.refresh(index = index_name)
        print ('REFRESHED')
        rr = es.count(index_name)['count']
    else:
        nes.refresh_index()
        rr = nes.count()

def refresh_index_repeating(index_name = mc_config.MC_INDEX_NAME,
                            repeat_interval = 600,
                            via_cli = False,
                            ):
    """
    Repeatedly refresh Indexer indexes at specified interval.

    TODO: delay refresh if a refresh was already called elsewhere.
    """
    
    while True:
        refresh_index(index_name = index_name)
        sleep_loud(repeat_interval)


def config(via_cli = False):
    """
    Print config.
    """
    
    print_config(mc_config.cfg)


functions=['receive_blockchain_into_indexer',
           'send_compactsplit_to_blockchain',
           'send_compactsplit_to_indexer',
           'send_gettydump_to_indexer',
           'delete_index',
           'refresh_index',
           'refresh_index_repeating',
           'search_by_image',
           'config',
           'tail_blockchain',
           'test_image_cache',
           'aes_backfill',
           ]

def main():
    setup_main(functions,
               globals(),
                'mediachain-indexer-ingest',
               )

if __name__ == '__main__':
    main()

