#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Prototype REST Indexer interface for search / dedupe.

The following apply to all REST API functions in this package:
    
    Media Identifiers:

       Media works can be identified by strings in any of the following formats:
       - IPFS ID string, starting with 'ipfs://'
       - Base64-encoded PNG or JPG file, starting with 'base64://'

    Input format:
       Body of POST is JSON-encoded string. Keys and values are as specified below.
    
    Returns on success:
       results:       List of results.
       next_page:     Pagination link.
       prev_page:     Pagination link.
    
    Returns on error:
       error:         Error code.
       error_message: Error message.


TODO:
    - Expose additional admin functionality, e.g. start dedupe batch jobs over REST?

"""


import json
import tornado.ioloop
import tornado.web
from time import time
from tornadoes import ESConnection

import tornado
import tornado.options
import tornado.web
import tornado.template
import tornado.gen
import tornado.auth
from tornado.web import RequestHandler
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
from tornado.options import define, options

from os import mkdir, listdir, makedirs, walk, rename, unlink
from os.path import exists,join,split,realpath,splitext,dirname

from mc_generic import setup_main, pretty_print, intget
import mc_models
import mc_config
import mc_normalize

data_pat = 'data:image/jpeg;base64,'
data_pat_2 = 'data:image/png;base64,'


class Application(tornado.web.Application):
    def __init__(self,
                 ):
        
        handlers = [(r'/',handle_front,),
                    (r'/ping',handle_ping,),
                    (r'/search',handle_search,),
                    (r'/list_facets',handle_list_facets),
                    (r'/get_embed_url',handle_get_embed_url,),
                    (r'/dupe_lookup',handle_dupe_lookup,),
                    (r'/score',handle_score,),
                    (r'/record_dupes',handle_record_dupes,),
                    (r'/record_relevance',handle_record_relevance,),
                    #(r'.*', handle_notfound,),
                    ]
        
        settings = {'template_path':join(dirname(__file__), 'templates_mc'),
                    'static_path':join(dirname(__file__), 'static_mc'),
                    'xsrf_cookies':False,
                    }
        
        tornado.web.Application.__init__(self, handlers, **settings)
        
        self.INDEX_NAME = mc_config.MC_INDEX_NAME
        self.DOC_TYPE = mc_config.MC_DOC_TYPE


class BaseHandler(tornado.web.RequestHandler):
    
    def __init__(self, application, request, **kwargs):
        RequestHandler.__init__(self, application, request, **kwargs)
        
        self._current_user=False
        
        self.loader=tornado.template.Loader('templates_mc/')
    
    @property
    def io_loop(self,
                ):
        if not hasattr(self.application,'io_loop'):
            self.application.io_loop = IOLoop.instance()
        return self.application.io_loop
        
    def get_current_user(self,):
        return self._current_user
    
    @property
    def es(self):
        if not hasattr(self.application,'es'):
            self.application.es = ESConnection("localhost", 9200)
        return self.application.es
    
    @tornado.gen.engine
    def render_template(self,template_name, kwargs):
        """
        Central point to customize what variables get passed to templates.        
        """
        
        t0 = time()
        
        if 'self' in kwargs:
            kwargs['handler'] = kwargs['self']
            del kwargs['self']
        else:
            kwargs['handler'] = self
        
        r = self.loader.load(template_name).generate(**kwargs)
        
        print ('TEMPLATE TIME',(time()-t0)*1000)
        
        self.write(r)
        self.finish()
    
    def render_template_s(self,template_s,kwargs):
        """
        Render template from string.
        """
        
        t=Template(template_s)
        r=t.generate(**kwargs)
        self.write(r)
        self.finish()
        
    def write_json(self,
                   hh,
                   sort_keys = True,
                   indent = 4, #Set to None to do without newlines.
                   pretty = False,
                   max_indent_depth = False,
                   ):
        """
        Central point where we can customize the JSON output.
        """
        print 'PRETTY',pretty
        self.set_header("Content-Type", "application/json")
        
        if pretty:
            self.write(pretty_print(hh,
                                    indent = indent,
                                    max_indent_depth = max_indent_depth,
                                    ).replace('\n','\r\n') + '\n')
        
        else:
            self.write(json.dumps(hh,
                                  sort_keys = sort_keys,
                                  ) + '\n')
        self.finish()
        

    def write_error(self,
                    status_code,
                    **kw):

        rr = {'error':'INTERNAL_ERROR',
              'error_message':'Unexpected server error.',
              }

        self.write_json(rr)
    


class handle_front(BaseHandler):
    
    @tornado.gen.coroutine
    def get(self):
        
        #TODO -
        
        self.write('FRONT_PAGE')
        self.finish()


class handle_ping(BaseHandler):
    
    @tornado.gen.coroutine
    def post(self):
        
        rr = yield self.es.ping()
        
        self.write_json({'results':[rr]})

from urllib import urlencode

from mc_rerank import ReRankingBasic

try:
    from mc_crawlers import get_remote_search
except:
    get_remote_search = False


def get_cache_url(_id,
                  image_cache_host = mc_config.MC_IMAGE_CACHE_HOST,
                  image_cache_dir = mc_config.MC_IMAGE_CACHE_DIR,
                  ):
    """
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
        
        base = '/datasets/datasets/indexer_cache/'
        
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
    
    else:
        return None
        #assert False,repr(_id)


class handle_get_embed_url(BaseHandler):
    #disable XSRF checking for this URL:
    def check_xsrf_cookie(self): 
        pass

    @tornado.gen.coroutine
    def post(self):
        
        try:
            from mc_crawlers import send_to_cdn
        except:
            self.write_json({'error':'IMPORT FAILED',
                             'error_message':'from mc_crawlers import get_embed_url',
                            })
            return

        d = self.request.body

        #print ('BODY',d)
        
        ii = self.get_argument('image_url','')

        if not ii:
                    
            try:
                data = json.loads(d)
            except:
                self.set_status(500)
                self.write_json({'error':'JSON_PARSE_ERROR',
                                 'error_message':'Could not parse JSON request.',
                                })
                return

            if not data.get('image_url'):
                self.set_status(500)
                self.write_json({'error':'MISSING ARGUMENT',
                                 'error_message':'required: image_url',
                                })
                return
        
            ii = data['image_url']
        
        print ('handle_get_embed_url()',ii)
        
        try:
            r = send_to_cdn(ii)
        except:
            raise
            self.set_status(500)
            self.write_json({'error':'INTERNAL_ERROR',
                             'error_message':'Internal error.',
                            })
            return

        print ('GOTXXX',r)
        
        self.write_json(r)


from time import time


   
def query_cache_lookup(key,
                       max_age = 60 * 60, # 1 hour
                       query_cache_dir = mc_config.MC_QUERY_CACHE_DIR,
                       skip_query_cache = False,
                       allow_skip_query_cache = mc_config.MC_ALLOW_SKIP_QUERY_CACHE_INT,
                       ):
    """
    Simple file-based cache.
    """

    if skip_query_cache and allow_skip_query_cache:
        print ('!!!SKIP_QUERY_CACHE',)
        return False
    
    dir_out_2 = join(query_cache_dir,
                     ('/'.join(key[:4])) + '/',
                     )
    
    fn_out = dir_out_2 + key + '.json'
    
    if exists(fn_out):
        try:
            with open(fn_out) as f:
                rh = json.loads(f.read())
        except:
            return False
        
        if time() - rh['time'] > max_age:
            return False

        rh['data']['cache_hit'] = True
        
        return rh['data']
    
    return False


def query_cache_save(key,
                     hh,
                     query_cache_dir = mc_config.MC_QUERY_CACHE_DIR,
                     ):
    """
    Simple file-based cache.
    """

    dir_out_2 = join(query_cache_dir,
                     ('/'.join(key[:4])) + '/',
                     )
    
    fn_out = dir_out_2 + key + '.json'
    
    if not exists(dir_out_2):
        try:
            makedirs(dir_out_2)
        except OSError, e:
            if e.errno != 17: ## 17 == File Exists, caused by concurrent threads.
                raise
    
    rh = {'data':hh,
          'time':int(time()),
          }
        
    with open(fn_out + '.temp', 'w') as f:
        f.write(json.dumps(rh))
    
    rename(fn_out + '.temp',
           fn_out,
           )


class handle_list_facets(BaseHandler):
    
    #disable XSRF checking for this URL:
    def check_xsrf_cookie(self): 
        pass
    
    @tornado.gen.coroutine
    def get(self):
        """
        Return a list of filterable facets. Facets are filtered by "id".

        Todo: Periodically or continously update these lists, based on ES database.
        """
        
        licenses = {"CC0":{"id":"CC0",
                           "name":"Creative Commons Zero (CC0)",
                           "url":None,
                           },
                    "Non-Commercial Use":{"id":"Non-Commercial Use",
                                          "name":"Getty Embed",
                                          "url":"http://www.gettyimages.com/Corporate/LicenseAgreements.aspx#RF",
                                          },
                    }


        ## Abbreviated list for now. See mc_normalizers.py for complete list:
        
        source_list = ['gettyimages.com',
                       'pexels.com',
                       'stock.tookapic.com',
                       'unsplash.com',
                       'pixabay.com',
                       'kaboompics.com',
                       'lifeofpix.com',
                       'skitterphoto.com',
                       'snapwiresnaps.tumblr.com',
                       'freenaturestock.com',
                       'negativespace.co',
                       'jaymantri.com',
                       'jeshoots.com',
                       'splitshire.com',
                       'stokpic.com',
                       'gratisography.com',
                       'picography.co',
                       'startupstockphotos.com',
                       'littlevisuals.co',
                       'gratisography.com',
                       'spacex.com',
                       'creativevix.com',
                       'photos.oliur.com',
                       'photos.uncoated.uk',
                       'tinyography.com',
                       'splashofrain.com',
                       'commons.wikimedia.org',
                       'dp.la',
                       ]
        
        sources = {x:{"id":x,
                      "name":x,
                      "url":None,
                      }
                   for x in source_list
                   }

        rr = {"licenses":licenses,
              "sources":sources,
              }
        
        self.write_json(rr)


from mc_generic import consistent_json_hash
import hashlib

class handle_search(BaseHandler):
    
    #disable XSRF checking for this URL:
    def check_xsrf_cookie(self): 
        pass
    
    @tornado.gen.coroutine
    def post(self):
        """
        Search for images based on text query, a media work, or a combination of both.
        
        Args, as JSON-encoded POST body:
            q:                Query text.
            q_id:             Query media. See `Media Identifiers`.
            
            limit:            Maximum number of results to return.
            include_self:     Include ID of query document in results.
            include_docs:     Return entire indexed docs, instead of just IDs.
            include_thumb:    Whether to include base64-encoded thumbnails in returned results.
            
            model_name:       Model name to use.
            rerank_eq:        Override the re-ranking equation.
            
            pretty:           Pretty-print JSON output.
            max_indent_depth: Maximum depth at which to indent for pretty-printed JSON output.

            filter_licenses:  List of allowable licenses. Empty list to allow all licenses. See `/list_facets`.
            filter_sources:   List of allowable sources. Empty list to allow all sources. See `/list_facets`.
        
        Returns:
            List of image IDs, possibly with relevancy scores.
        
        Example:
           in:
               curl "http://127.0.0.1:23456/search" -d '{"q":"crowd", "limit":5}'

           out:
                {
                    "next_page": null, 
                    "prev_page": null, 
                    "results": [
                        {
                            "_id": "getty_531746924", 
                            "_index": "getty_test", 
                            "_score": 0.08742375, 
                            "_source": {
                                "artist": "Tristan Fewings", 
                                "caption": "CANNES:  A policeman watches the crowd in front of the Palais des Festival", 
                                "collection_name": "Getty Images Entertainment", 
                                "date_created": "2016-05-16T00:00:00-07:00", 
                                "dedupe_hsh": "d665691fe66393d81c078ae1ff1467cf18f78070900e23ff87c98704cc007c00", 
                                "editorial_source": "Getty Images Europe", 
                                "keywords": "People Vertical Crowd Watching France Police Force Cannes", 
                                "title": "'Loving' - Red Carpet Arrivals - The 69th Annual Cannes Film Festival"
                            }, 
                            "_type": "image"
                        }
                    ]
                }
        
            in:
                curl "http://127.0.0.1:23456/search" -d '{"q_id":"data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAUAAAAFCAYAAACNbyblAAAAHElEQVQI12P4//8/w38GIAXDIBKE0DHxgljNBAAO9TXL0Y4OHwAAAABJRU5ErkJggg==", "limit":5, "index_name":"mc_test", "doc_type":"mc_test_image"}' 
            out:
                 <see_previous_example>
        """
        
        d = self.request.body

        if d.startswith('{'):
            print ('OLD PARAMETERS FORMAT',d[:20])
            
        else:
            print ('NEW PARAMETERS FORMAT',)
            d = self.get_argument('json','{}')            
                
        try:
            data = json.loads(d)
        except:
            self.set_status(500)
            self.write_json({'error':'JSON_PARSE_ERROR',
                             'error_message':'Could not parse JSON request.',
                            })
            return

        if 'help' in data:
            ## Plain-text help:
            from inspect import getdoc, getframeinfo, currentframe
            self.set_header("Content-Type", "text/plain")
            self.write(getdoc(getattr(self, getframeinfo(currentframe()).function)).replace('\n','\r\n') + '\r\n')
            self.finish()
            return
        
        q_text = data.get('q','')
        q_id = data.get('q_id','')
        q_id_file = None
        offset = max(0, intget(data.get('offset'), 0))
        limit = intget(data.get('limit'), 15)
        index_name = data.get('index_name', mc_config.MC_INDEX_NAME)
        doc_type = data.get('doc_type', mc_config.MC_DOC_TYPE)
        include_docs = data.get('include_docs', True)
        include_thumb = data.get('include_thumb', True)
        rerank_eq = data.get('rerank_eq', None)
        filter_licenses = data.get('filter_license', None) or data.get('filter_licenses', None)
        filter_sources = data.get('filter_sources', None)
        skip_query_cache = data.get('skip_query_cache', None)
        
        if isinstance(filter_licenses, basestring):
            filter_licenses = [filter_licenses]
        
        if isinstance(filter_sources, basestring):
            filter_sources = [filter_sources]
        
        
        ## TODO: need multiple image upload support?:
        
        try:
            fileinfo = self.request.files['file'][0]
            print ("FILE UPLOAD", fileinfo['filename'])
            q_id_file = fileinfo['body']
        except:
            pass
        
        if q_id_file and q_id:
            self.set_status(500)
            self.write_json({'error':'PARAMS',
                             'error_message':'Use either q_id or HTTP image upload, but not both.',
                             })
            return                
        
        include_thumb = False ## Always disabled, for now
        full_limit = 200
        
        remote_ids = []
        if q_text and (get_remote_search is not False):
            t1 = time()
            remote_ids = get_remote_search(q_text)
            print ('REMOTE_IDS','time:',time()-t1,len(remote_ids))

        
        ## Cache & token lookup:
        
        input_token = data.get('token', None)

        the_token = input_token
        
        rr = False

        
        if the_token:
            rr = query_cache_lookup(the_token, skip_query_cache = skip_query_cache)

            if rr is False:
                self.set_status(500)
                self.write_json({'error':'EXPIRED_TOKEN',
                                 'error_message':the_token,
                                 })
                return                

        else:
            ## core args for looking up this query again:
            query_args = {'q':q_text,
                          'q_id':q_id,
                          'q_id_file_hash': q_id_file and hashlib.md5(q_id_file).hexdigest() or q_id_file,
                          #'limit':limit,
                          #'offset':offset,
                          #'index_name':index_name,
                          #'doc_type':doc_type,
                          'include_docs':include_docs,
                          'include_thumb':include_thumb,
                          'rerank_eq':rerank_eq,
                          'filter_licenses':filter_licenses,
                          'filter_sources':filter_sources,
                          }
            print ('QUERY_ARGS',query_args)

            ## ignore those with default args:
            for k,v in query_args.items():
                if v is None:
                    del query_args[k]
            
            the_token = consistent_json_hash(query_args)
            
            rr = query_cache_lookup(the_token, skip_query_cache = skip_query_cache)
        
        if rr is not False:
            print ('CACHE_OR_TOKEN_HIT_QUERY','offset:',offset,'limit:',limit,'len(results)',len(rr['results']))
            
            results_count = len(rr['results'])
                                                
            if offset + limit >= len(rr['results']):
                rr['next_page'] = None
            else:
                rr['next_page'] = {'token':the_token, 'offset':offset + limit, 'limit':limit}

            if offset == 0:
                rr['prev_page'] = None
            else:
                rr['prev_page'] = {'token':the_token, 'offset':max(0, offset - limit), 'limit':limit}

            rr['results'] = rr['results'][offset:offset + limit]

            self.write_json(rr,
                            pretty = data.get('pretty', True),
                            max_indent_depth = data.get('max_indent_depth', False),
                            )
            
            return
        
        if not (q_text or q_id or q_id_file):
            #self.set_status(500)
            #self.write_json({'error':'BAD_QUERY',
            #                 'error_message':'Either `q` or `q_id` is required.',
            #                 })
            
            ## Match all, skip cache:
            
            rr = yield self.es.search(index = index_name,
                                      type = doc_type,
                                      source = {"query": {"match_all": {}}, "size":20}
                                      )
            
            rr = json.loads(rr.body)

            if 'error' in hh:
                self.set_status(500)
                self.write_json({'error':'ELASTICSEARCH_ERROR',
                                 'error_message':repr(hh)[:1000],
                                 })
                return

            
            rr = rr['hits']['hits']
            rr = {'results':rr,
                  'next_page':None,
                  'prev_page':None,
                  'results_count':('{:,}'.format(full_limit)) + '+',
                  }
            self.write_json(rr)
            return

        
        if (q_text and q_id):
            self.set_status(500)
            self.write_json({'error':'BAD_QUERY',
                             'error_message':'Simultaneous `q` and `q_id` not yet implemented.',
                             })
            return

        
        elif q_id or q_id_file:
            
            if q_id_file or (q_id.startswith(data_pat) or q_id.startswith(data_pat_2)):
                
                #Resolve ID(s) for query based on content.
                #Note that this is similar to `/dupe_lookup` with `include_docs` = True:
                
                print ('CONTENT-BASED-SEARCH',)
                
                model = mc_models.VECTORS_MODEL_NAMES['baseline']()
                
                if (q_id.startswith(data_pat) or q_id.startswith(data_pat_2)):
                    print ('GOT_DATA_URI')
                    terms = model.img_to_terms(img_data_uri = q_id)
                else:
                    print ('GOT_RAW_BYTES')
                    assert q_id_file
                    terms = model.img_to_terms(img_bytes = q_id_file)
                
                print ('TERMS',repr(terms)[:100])
                
                
                rr = yield self.es.search(index = index_name,
                                          type = doc_type,
                                          source = {"query": {"constant_score":{"filter":{"term": terms}}}},
                                          )
                #print ('GOT',repr(rr.body)[:100])
                
                rr = json.loads(rr.body)

                if 'error' in hh:
                    self.set_status(500)
                    self.write_json({'error':'ELASTICSEARCH_ERROR',
                                     'error_message':repr(hh)[:1000],
                                     })
                    return

                
                rr = [x['_id'] for x in rr['hits']['hits']]
                
                query = {"query":{ "ids": { "values": rr } } }
                
            else:
                #ID-based search:

                print ('ID-BASED-SEARCH', q_id)
                query = {"query":{ "ids": { "values": [ q_id ] } } }
        
        
        elif q_text:

            #text-based search:
            query = {"query": {"multi_match": {"query":    q_text,
                                               "fields": [ "*" ],
                                               "type":     "cross_fields"
                                               },
                               },
                     }

        remote_hits = []
        
        #assert remote_ids,remote_ids
        if remote_ids:
            t1 = time()
            rr = yield self.es.search(index = index_name,
                                      type = doc_type,
                                      source = {"query":{ "ids": { "values": remote_ids } } },
                                      )
        
            print ('GOT','time:',time() - t1,repr(rr.body)[:100])
        
            hh = json.loads(rr.body)

            if 'error' in hh:
                self.set_status(500)
                self.write_json({'error':'ELASTICSEARCH_ERROR',
                                 'error_message':repr(hh)[:1000],
                                 })
                return


            remote_hits = hh['hits']['hits']

            print ('GOT_REMOTE_HITS',len(remote_hits))
            
            for xx in remote_hits:
                xx['_score'] = 10.0
                xx['_source']['boosted'] = 1
                
            for xx in remote_hits:
                assert xx['_source']['boosted'] == 1
        
        query['from'] = offset
        query['size'] = full_limit
        
        print ('QUERY',query)

        t1 = time()
        rr = yield self.es.search(index = index_name,
                                  type = doc_type,
                                  source = query,
                                  )
        
        print ('GOT','time:',time() - t1, repr(rr.body)[:100])
        
        hh = json.loads(rr.body)

        if 'error' in hh:
            self.set_status(500)
            self.write_json({'error':'ELASTICSEARCH_ERROR',
                             'error_message':repr(hh)[:1000],
                             })
            return
        
        rr = hh['hits']['hits']

        ## Prepend remote hits, filter ID dupes:
        
        rr = remote_hits + rr

        done = set()
        r2 = []
        for xx in rr:
            if xx['_id'] in [u'pexels_85601']:
                continue
            
            if xx['_id'] in done:
                continue
            done.add(xx['_id'])
            r2.append(xx)
        rr = r2
        
        ## Remove inline thumbnail data URIs:
        if not include_thumb:
            for x in rr:
                if 'image_thumb' in x['_source']:
                    del x['_source']['image_thumb']


        ## Remove `dedupe_*` fields:

        for xx in rr:
            xx['_source']  = {x:y
                              for x,y
                              in xx['_source'].iteritems()
                              if not x.startswith('dedupe_')
                              }
        
        
        ## Add in frontend image cached preview images:
        ## Skip items without preview:
        ## TODO: rework `get_cache_url()` and ensure `native_id`s for all records.
        
        image_cache_failed = False
        r2 = []
        for ii in rr:

            try:
                #print ('ZZ',ii['_source']['native_id'],ii['_source']['source'])

                url = get_cache_url(ii['_source']['native_id'])

                if not url:
                    print 'FILTER_SKIP',ii['_source']['native_id']
                    continue

                ii['_source']['url_direct_cache'] = {'url':url}
            except:
                image_cache_failed = True
                ii['_source']['url_direct_cache'] = None
            
            r2.append(ii)
        
        if image_cache_failed:
            print ('!!!IMAGE_CACHE_FAILED', mc_config.MC_IMAGE_CACHE_DIR)
            #raise
        
        rr = r2
        
        ## Re-rank:
        
        rrm = ReRankingBasic(eq_name = rerank_eq)        
        rr = rrm.rerank(rr)

        
        ## Apply post-ingestion normalizers, if there are any:
        
        mc_normalize.apply_post_ingestion_normalizers(rr)

        
        ## Note: swapping these for ES queries shortly:
        
        if filter_licenses:
            filter_licenses_s = set(filter_licenses)
            r2 = []
            for ii in rr:                
                if filter_licenses_s.intersection(ii['_source'].get('license_tags',[])):
                    r2.append(ii)
            
            print ('FILTER_LICENSES',filter_licenses,len(rr),'->',len(r2))
            rr = r2
        
        if filter_sources:
            filter_sources_s = set(filter_sources)
            r2 = []
            for ii in rr:                
                if filter_sources_s.intersection(ii['_source'].get('source_tags',[])):
                    r2.append(ii)
            
            print ('FILTER_SOURCES',filter_licenses,len(rr),'->',len(r2))
            rr = r2
        
        
        ## Include or don't include full docs:
        
        if not include_docs:
            
            rr = [{'_id':hit['_id']}
                  for hit
                  in rr
                  ]


        ## Cache:

        results_count = len(rr)
        
        rr = {'results':rr,
              'results_count':(results_count >= full_limit * 0.8) and \
                               (('{:,}'.format(full_limit)) + '+') or \
                               ('{:,}'.format(results_count)),
              }
        
        query_cache_save(the_token, rr)

        ## Wrap in pagination:
        
        if offset + limit >= len(rr['results']):
            rr['next_page'] = None
        else:
            rr['next_page'] = {'token':the_token, 'offset':offset + limit, 'limit':limit}

        if offset == 0:
            rr['prev_page'] = None
        else:
            rr['prev_page'] = {'token':the_token, 'offset':max(0, offset - limit), 'limit':limit}

        ## Trim:
        
        rr['results'] = rr['results'][offset:offset + limit]
        
        ## Output:
                
        self.write_json(rr,
                        pretty = data.get('pretty', True),
                        max_indent_depth = data.get('max_indent_depth', False),
                        )


class handle_dupe_lookup(BaseHandler):
    
    #disable XSRF checking for this URL:
    def check_xsrf_cookie(self): 
        pass
    
    @tornado.gen.coroutine
    def post(self):
        """
        Find all known duplicates of a media work.
        
        Args - passed as JSON-encoded body:
            q_media:          Media to query for. See `Media Identifiers`.
            lookup_name:      Name of lookup key for the model you want to use. See `lookup_name` of `dedupe_reindex()`.
                              Note: must use 'dedupe_hsh' as lookup_name if v1_mode is True.
            incremental:      If True, only update clusters affected by newly ingested media. Otherwise, regenerate
                              all dedupe clusters. Note: the more records that are deduped simultaneously, the greater
                              the efficiency.
            include_self:     Include ID of query document in results.
            include_docs:     Return entire indexed docs, instead of just IDs.
            include_thumb:    Whether to include base64-encoded thumbnails in returned results.
            incremental:      Attempt to dedupe never-before-seen media file versus all pre-ingested media files.
                              NOTE: potentially inefficient. More efficient to pre-calculate for all known images in
                              background.
             
            pretty:              Pretty-print JSON output.
            max_indent_depth:    Maximum depth at which to indent for pretty-printed JSON output.

        
        Returns: 
             See `mc_models.dedupe_lookup_async`.       

        Example:
             in: 
                 curl "http://127.0.0.1:23456/dupe_lookup" -d '{"q_media":"getty_531746790"}'
             out:
                 {"next_page": null, "results": [{'_id':"getty_9283423"}, {'_id':"getty_2374230"}], "prev_page": null}
        """

        d = self.request.body
        
        try:
            data = json.loads(d)
        except:
            self.set_status(500)
            self.write_json({'error':'JSON_PARSE_ERROR',
                             'error_message':'Could not parse JSON request.',
                            })
            return

        if 'help' in data:
            ## Plain-text help:
            from inspect import getdoc, getframeinfo, currentframe
            self.set_header("Content-Type", "text/plain")
            self.write(getdoc(getattr(self, getframeinfo(currentframe()).function)).replace('\n','\r\n') + '\r\n')
            self.finish()
            return
        
        if not data.get('q_media'):
            self.set_status(500)
            self.write_json({'error':'BAD_ARGUMENTS',
                             'error_message':'Missing required `q_media` argument.',
                            })
            return

        rr = yield mc_models.dedupe_lookup_async(media_id = data['q_media'],
                                                 lookup_name = data.get('lookup_name', 'dedupe_hsh'),
                                                 include_docs = data.get('include_docs'),
                                                 include_self = data.get('include_self'),
                                                 include_thumb = data.get('include_thumb'),
                                                 index_name = data.get('index_name', mc_config.MC_INDEX_NAME),
                                                 doc_type = data.get('doc_type', mc_config.MC_DOC_TYPE),
                                                 es = self.es,
                                                 )
        
        self.write_json({'results':rr,
                         'next_page':None,
                         'prev_page':None,
                         },
                        pretty = data.get('pretty', False),
                        max_indent_depth = data.get('max_indent_depth', False),
                        )


class handle_score(BaseHandler):
    
    #disable XSRF checking for this URL:
    def check_xsrf_cookie(self): 
        pass
    
    @tornado.gen.coroutine
    def post(self):
        """
        NOTE: These functions are mostly contained in other API endpoints now. May remove this endpoint.
        
        Admin tool for peering deeper into the similarity / relevance measurements that are the basis of
        dedupe / search calculations. Useful for e.g. getting a feel for why an image didn't show up in
        the top 100 results for a query, or why a pair of images weren't marked as duplicates.
        
        Takes a "query" and list of "candidate" media, and does 1-vs-all score calculations for
        all "candidate" media versus the "query".
        
        Args - passed as JSON-encoded POST body:
            q:         Query text.
            q_id:      Query media. See `Media Identifiers`.
            
            c_ids:     List of candidate media. See `Media Identifiers`.
            
            mode:      Type of similarity to measure. Should be one of:
                       'search' - Search relevance score.
                       'dupe'   - Duplicate probability.
            
            level:     Level of the model at which to measure the similarity.
                       'similarity' - Similarity in the embedding space(s) (0.0 to 1.0).
                                      May return multiple similarities, if multiple
                                      similarity or duplicate types are in use.
                       'score'     - Final relevance score for "search" mode (1.0 to 5.0),
                                     or final dupe probability score for "dupe" mode (0.0 to 1.0).

            pretty:              Pretty-print JSON output.
            max_indent_depth:    Maximum depth at which to indent for pretty-printed JSON output.
        
        Returns:
            List of similarities or duplicate probabilities, one per similarity or duplicate type.

        Example:
            in: 
                curl "http://127.0.0.1:23456/score" -d '{"mode":"search", "level":"distance", "q_text":"girl with baloon", "c_ids":["ifps://123..."]}'
            out:
                {'results':[{'id':'ifps://123...', 'score':0.044}]}

            in: 
                curl "http://127.0.0.1:23456/score" -d '{"mode":"dupe", "level":"score", "q_id":"ifps://123...", "c_ids":["ifps://123..."]}'
            out:
                {'results':[{'id':'ifps://123...', 'score':0.321}]}
        """

        d = self.request.body
        
        try:
            data = json.loads(d)
        except:
            self.set_status(500)
            self.write_json({'error':'JSON_PARSE_ERROR',
                             'error_message':'Could not parse JSON request.',
                            })
            return

        if 'help' in data:
            ## Plain-text help:
            from inspect import getdoc, getframeinfo, currentframe
            self.set_header("Content-Type", "text/plain")
            self.write(getdoc(getattr(self, getframeinfo(currentframe()).function)).replace('\n','\r\n') + '\r\n')
            self.finish()
            return
        
        q_text = data.get('q','')
        
        query = {"query": {"match_all": {}}}
        
        rr = yield self.es.search(index = mc_config.MC_INDEX_NAME,
                                  type = mc_config.MC_DOC_TYPE,
                                  source = query,
                                  )
        
        self.content_type = 'application/json'
        self.write_json(json.loads(rr.body),
                        pretty = data.get('pretty', False),
                        max_indent_depth = data.get('max_indent_depth', False),
                        )



class handle_record_dupes(BaseHandler):
    
    #disable XSRF checking for this URL:
    def check_xsrf_cookie(self): 
        pass
    
    @tornado.gen.coroutine
    def post(self):
        """
        Record dupe / non-dupes. May factor this out later.
        """
        self.set_status(500)
        self.write('NOT_IMPLEMENTED')
        self.finish()


class handle_record_relevance(BaseHandler):
    
    #disable XSRF checking for this URL:
    def check_xsrf_cookie(self): 
        pass
    
    @tornado.gen.coroutine
    def post(self):
        """        
        Record relevance feedback. May factor this out later.
        """
        self.set_status(500)
        self.write('NOT_IMPLEMENTED')
        self.finish()


def web(port = 23456,
        via_cli = False,
        ):
    """
    Bind Tornado server to specified port.    
    """
        
    print ('BINDING',port)
    
    try:
        tornado.options.parse_command_line()
        http_server = HTTPServer(Application(),
                                 xheaders=True,
                                 )
        http_server.bind(port)
        http_server.start(0) # Forks multiple sub-processes
        tornado.ioloop.IOLoop.instance().set_blocking_log_threshold(0.5)
        IOLoop.instance().start()
        
    except KeyboardInterrupt:
        print 'Exit'
    
    print ('WEB_STARTED')


functions=['web',
           ]

def main():    
    setup_main(functions,
               globals(),
               'mediachain-indexer-web',
               )

if __name__ == '__main__':
    main()

