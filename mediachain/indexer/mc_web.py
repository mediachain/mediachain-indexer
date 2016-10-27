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
import ujson
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

DO_FORWARDING = mc_config.MC_DO_FORWARDING_INT

print (DO_FORWARDING and 'FORWARDING_ENABLED' or 'FORWARDING_DISABLED')


class Application(tornado.web.Application):
    def __init__(self,
                 ):
        
        handlers = [(r'/',handle_front,),
                    (r'/ping',handle_ping,),
                    (r'/stats',handle_stats,),
                    (r'/stats_annotation',handle_stats_annotation,),
                    (r'/search',handle_search,),
                    (r'/list_facets',handle_list_facets),
                    (r'/get_embed_url',handle_get_embed_url,),
                    (r'/record_relevance',handle_record_relevance,),
                    (r'/random_query',handle_random_query,),
                    #(r'.*', handle_notfound,),
                    ]
        
        settings = {'template_path':join(dirname(__file__), 'templates_mc'),
                    'static_path':join(dirname(__file__), 'static_mc'),
                    'xsrf_cookies':False,
                    }
        
        tornado.web.Application.__init__(self, handlers, **settings)
        
        self.INDEX_NAME = mc_config.MC_INDEX_NAME
        self.DOC_TYPE = mc_config.MC_DOC_TYPE


from mc_alerts import MCAlerts

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
    def order_model_cache(self):
        if not hasattr(self.application,'order_model_cache'):
            self.application.order_model_cache = init_order_model(mc_config.MC_NEURAL_MODEL_NAME)
        return self.application.order_model_cache
    
    @property
    def es(self):
        if not hasattr(self.application,'es'):
            ## TODO: multiple hosts:
            self.application.es = ESConnection(mc_config.MC_ES_HOSTS.split(',')[0], 9200)
        return self.application.es


    @property
    def alerts(self):
        if not hasattr(self.application, 'alerts'):
            self.application.alerts = MCAlerts()
        return self.application.alerts

    @property
    def rand_typeahead(self):
        if not hasattr(self.application,'rand_typeahead'):
            
            total = 0
            rr = []

            total_mwe = 0
            rr_mwe = []
            
            with open(mc_config.MC_TYPEAHEAD_TSV_PATH) as f:
                for c, line in enumerate(f):
                    if c >= 1000:
                        break
                    
                    if not line:
                        break
                    
                    score, query, _ = line.split('\t')
                    score = int(score)

                    if c < 1000:
                        rr.append((score, query))
                        total += score

                    if query.count(' ') > 1:
                        rr_mwe.append((score, query))
                        total_mwe += score

            if False:#order_model:
                print ('USE_WORDDICT',)
                rr = [(1.0, x) for x in order_model['worddict'].keys()]
                rr_mwe = rr
                        
            self.application.rand_typeahead_total = total
            self.application.rand_typeahead = rr
            
            self.application.rand_typeahead_mwe_total = total_mwe
            self.application.rand_typeahead_mwe = rr_mwe
            
        return self.application.rand_typeahead
    
    @property
    def rand_typeahead_total(self):
        if not hasattr(self.application,'rand_typeahead_total'):
            self.rand_typeahead
        return self.application.rand_typeahead_total
            
    @property
    def rand_typeahead_mwe(self):
        if not hasattr(self.application,'rand_typeahead_mwe'):
            self.rand_typeahead
        return self.application.rand_typeahead_mwe

    @property
    def rand_typeahead_mwe_total(self):
        if not hasattr(self.application,'rand_typeahead_mwe_total'):
            self.rand_typeahead
        return self.application.rand_typeahead_mwe_total
    
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
                   sort_keys = False,
                   indent = 4, #Set to None to do without newlines.
                   pretty = False,
                   max_indent_depth = False,
                   ):
        """
        Central point where we can customize the JSON output.
        """
        if 'error' in hh:
            print ('ERROR',hh)
        
        self.set_header("Content-Type", "application/json")

        self.write(json.dumps(hh,
                              sort_keys = sort_keys,
                              indent = 4,
                              ) + '\n')
        if False:
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
    def get(self):
        """
        """
        
        #rr = yield self.es.ping()
        #self.write_json({'results':rr})
        
        self.write_json({'pong':1})

from collections import Counter

class handle_stats_annotation(BaseHandler):
    @tornado.gen.coroutine
    def get(self):
        """
        Annotation stats.

        """
        uu = {
            "172.56.34.204": 'dennis', 
            "67.244.113.119": 'jesse', 
            "95.111.63.140": 'tg',
            "75.98.195.186": 'dennis2',
        }
        
        dir_in = '/datasets/datasets/annotate/search_relevance_001/'
        dir_in = '/datasets/datasets/annotate/search_relevance/'
        
        rh = {'users':{}}

        qc = Counter()

        uq = set() #set([(uname,query)])
        
        for fn in listdir(dir_in):

            if fn.startswith('test_'):
                continue
            
            fn = join(dir_in, fn)

            try:
                with open(fn) as f:
                    d = f.read()
                h = json.loads(d)
            except:
                print ('BAD',fn)
                continue

            
            #h['data']['query_info']:
            #{u'query_args': {u'doc_type': u'image', u'skip_query_cache': 0, u'allow_nsfw': 0, u'include_docs': 1, u'schema_variant': u'new', u'q_text': u'women in tech', u'index_name': u'getty_test', u'canonical_id': 0, u'enrich_tags': 1, u'filter_incomplete': 0, u'debug': 1, u'filter_licenses': [u'Creative Commons'], u'full_limit': 200, u'pretty': 1, u'include_thumb': False, u'rerank_eq': u'neural_hybrid', u'q': u'women in tech', u'token': u'5d70d87058a8349e910b7dbda1eda85b', u'filter_sources': u'ALL'}, u'query_elapsed_ms': 5, u'query_time': 1472230434}

            
            #print fn, len(h['data']['data'])
            the_query = h['data']['query_info']['query_args'].get('q')
            
            qc[the_query] += 1
                        
            #{u'ratings': [{u'rating': 1, u'_id': u'Overall'}, {u'rating': 0, u'_id': u'is_bad_nsfw'}, {u'rating': 0, u'_id': u'is_bad_spam'}, {u'rating': 0, u'_id': u'is_bad_watermarks'}], u'id': u'adc4f1634f448095757559005be2d1ab'}
            
            uname = h['user_ip']
            
            uname = uu.get(uname, uname)
            
            if uname not in rh['users']:
                rh['users'][uname] = {'num_tasks':0,
                                      'num_images':0,
                                      'num_queries':0,
                                      'queries':[],
                                      'num_bad':0,
                                      }            

            if not the_query:
                rh['users'][uname]['num_bad'] += 1
                continue
            
            rh['users'][uname]['queries'] = list(sorted(set(rh['users'][uname]['queries'] + [the_query])))
            #if uname == 'tg':
            #    print ('tg', fn)
            
            """
            h['data']['data'][0] =
              {u'id': u'b2e8bf36899934df9b57841a722b3556',
               u'ratings': [{u'_id': u'Overall', u'rating': 1},{u'_id': u'is_bad_nsfw', u'rating': 0},{u'_id': u'is_bad_spam', u'rating': 0},{u'_id': u'is_bad_watermarks', u'rating': 0}]}
            """
            for answers_set in h['data']['data']:
                
                ratings = answers_set['ratings']
                
                overall = [x for x in ratings if x['_id'] == 'Overall']

                if overall:
                    overall = overall[0]['rating']
                else:
                    overall = -1
                
                if overall < 1:
                    continue
                    
                rh['users'][uname]['num_images'] += 1
            
            if not rh['users'][uname]['num_images']:
                ## ignore tasks with 0 images rated.
                continue

            rh['users'][uname]['num_tasks'] += 1

            k = (uname,the_query)
            if k not in uq:
                rh['users'][uname]['num_queries'] += 1
            uq.add(k)

        print qc.most_common()
        
        self.write_json(rh)
        

        
from tornado.httpclient import AsyncHTTPClient

class handle_stats(BaseHandler):
    @tornado.gen.coroutine
    def get(self):
        """
        System stats.
        """
        
        rh = {}
        
        with open('/datasets/datasets/stats_stage_1.json') as f:
            d = f.read()
        
        h = json.loads(d)
        
        #rh['stage_001_crawl'] = h

        rh.update(h)
        
        url = 'http://10.99.0.44:9200/getty_test/_count'

        h2 = {'error':'ES_CONNECTION_ERROR',
              'message':url,
              }
        
        try:
            response = yield AsyncHTTPClient().fetch(url,
                                                     connect_timeout = 5,
                                                     request_timeout = 5,
                                                     )
            d = response.body

            print ('ES_GOT',d)

            h2 = json.loads(d)
            
        except Exception as e:
            print ('CONNECT_FAIL', url, e)
        
        rh['stage_005_elasticsearch'] = h2
        
        self.write_json(rh)


@tornado.gen.coroutine
def post(self):

    rr = yield self.es.ping()

    self.write_json({'results':[rr]})


        
from urllib import urlencode

from mc_rerank import ReRankingBasic, ranking_prebuilt_equations

try:
    from mc_crawlers import get_remote_search, get_enriched_tags
except KeyboardInterrupt:
    raise
except:
    get_remote_search = False
    get_enriched_tags = False

try:
    import mc_crawlers
    from mc_crawlers import get_neural_relevance, init_order_model, relevance_ann_query_to_concepts, reverse_image_lookup_index
except Exception as e:
    raise
    print ('IMPORT_ERROR',e)
    get_neural_relevance = False
    relevance_ann_query_to_concepts = False

print ('get_neural_relevance',get_neural_relevance)

order_model = False

if get_neural_relevance:# and (not DO_FORWARDING):
    #omc = init_order_model()
    #assert mc_crawlers.order_model_cache[0]
    #order_model = omc.get('order_model', False)
    pass

class handle_get_embed_url(BaseHandler):
    #disable XSRF checking for this URL:
    def check_xsrf_cookie(self): 
        pass

    @tornado.gen.coroutine
    def post(self):
        
        try:
            from mc_crawlers import send_to_cdn
        except KeyboardInterrupt:
            raise
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
            except KeyboardInterrupt:
                raise
            except:
                #self.set_status(500)
                self.write_json({'error':'JSON_PARSE_ERROR',
                                 'error_message':'Could not parse JSON request.',
                                })
                return

            if not data.get('image_url'):
                #self.set_status(500)
                self.write_json({'error':'MISSING ARGUMENT',
                                 'error_message':'required: image_url',
                                })
                return
        
            ii = data['image_url']
        
        print ('handle_get_embed_url()',ii)
        
        try:
            r = send_to_cdn(ii)
        except KeyboardInterrupt:
            raise
        except:
            raise
            #self.set_status(500)
            self.write_json({'error':'INTERNAL_ERROR',
                             'error_message':'Internal error.',
                            })
            return

        print ('GOTXXX',r)
        
        self.write_json(r)


from time import time


   
def query_cache_lookup(key,
                       max_age = 60 * 60 * 24 * 7, # 7 days
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
        except KeyboardInterrupt:
            raise
        except:
            return False
        
        if time() - rh['time'] > max_age:
            return False

        rh['data']['cache_hit'] = True
        
        #assert rh['data']['query_info']['query_args'].get('q'), rh['data']['query_info']['query_args'].keys()
        
        return rh['data']
    
    return False


def query_cache_save(key,
                     hh,
                     query_cache_dir = mc_config.MC_QUERY_CACHE_DIR,
                     ):
    """
    Simple file-based cache.
    """

    from random import randint
    
    #assert hh['query_info']['query_args'].get('q'), hh['query_info']['query_args'].keys()
    
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

    fn_out_temp = fn_out + '.temp' + str(randint(1,10000000))
    
    with open(fn_out_temp, 'w') as f:
        f.write(json.dumps(rh))

    rename(fn_out_temp,
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

source_names = {'500px.com':'500px',
                'flickr.com':'flickr100mm',
                'pexels.com':'pexels',
                'dp.la':'dpla',
                }

source_urls = ['500px.com',
               'flickr.com',
               'pexels.com',
               'dp.la',
               'gettyimages.com',
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
               'spacex.com',
               'creativevix.com',
               'photos.oliur.com',
               'photos.uncoated.uk',
               'tinyography.com',
               'splashofrain.com',
               'commons.wikimedia.org',
               ]
        
sources = {x:{"id":x,
              "name":x,
              "url":None,
              }
           for x in source_urls
           }

default_options = [{'name':'filter_licenses',
                    'description':'List of zero-or-more allowable licenses. Select nothing or use "ALL" to allow all licenses.',
                    'default':'Creative Commons',
                    'type':'list-of-strings',
                    'options':['Creative Commons','ALL'],
                   },
                   {'name':'filter_sources',
                    'description':'List of zero-or-more allowable sources. Select nothing or use "ALL" to allow all licenses.',
                    'default':'ALL',
                    'type':['list-of-strings'],
                    'options':['ALL'] + source_urls,
                   },
                   ]

debug_options = [{'name':'q',
                  'description':'Query by text.',
                  'default':None,
                  'type':'text',
                  'options':None,
                  },
                 {'name':'exclusive_to_ann',
                  'description':'Return only results that were exclusively found by the ANN method.',
                  'default':0,
                  'type':'number',
                  'options':[0, 1],
                  },
                 {'name':'exclusive_to_text',
                  'description':'Return only results that were exclusively found by the textual method.',
                  'default':0,
                  'type':'number',
                  'options':[0, 1],
                  },
                 {'name':'q_id',
                  'description':'Query by media. See Media Identifiers.',
                  'default':None,
                  'type':'text',
                  'options':None,
                  },
                 {'name':'canonical_id',
                  'description':'Query by canonical_id. TODO: Probably merging into q_id with prefixes.',
                  'default':0,
                  'type':'text',
                  'options':None,
                  },
                 {'name':'limit',
                  'description':'Items per page.',
                  'default':15,
                  'type':'number',
                  'options':None,
                  },
                 {'name':'offset',
                  'description':'Page offset.',
                  'default':0,
                  'type':'number',
                  'options':None,
                  },
                 {'name':'debug',
                  'description':'Enable advanced mode.',
                  'default':0,
                  'type':'number',
                  'options':[0, 1],
                 },
                   {'name':'include_docs',
                  'description':'Return entire indexed docs, instead of just IDs.',
                  'default':1,
                  'type':'number',
                  'options':[1, 0],
                  },
                 {'name':'pretty',
                  'description':'Indent and pretty-print JSON output.',
                  'default':1,
                  'type':'number',
                  'options':[1, 0],
                  },
                 {'name':'filter_incomplete',
                  'description':"Filter documents for which all features haven't been generated / ingested yet.",
                  'default':0,
                  'type':'number',
                  'options':[0, 1],
                  },
                 {'name':'allow_nsfw',
                  'description':'Include adult images.',
                  'default':0,
                  'type':'number',
                  'options':[0, 1],
                  },
                 {'name':'skip_query_cache',
                  'description':'Bypass the query cache.',
                  'default':0,
                  'type':'number',
                  'options':[0, 1]
                  },
                 {'name':'schema_variant',
                  'description':'Select schema variant postprocessing version.',
                  'default':'new',
                  'type':'text',
                  'options':['new','old'],
                  },
                 {'name':'rerank_eq',
                  'description':'Name of reranking equation, or custom reranking equation string.',
                  'default':'neural_hybrid_switch',#'neural_hybrid',#'aesthetics',
                  'type':'text',
                  'options':['aesthetics'] + list(sorted([x for x in ranking_prebuilt_equations if x != 'aesthetics'])),
                 },
                 {'name':'enrich_tags',
                  'description':'Use external API for tag enrichment on individual image pages.',
                  'default':1,
                  'type':'number',
                  'options':[1, 0],
                  },
                 {'name':'token',
                  'description':'Token ID used to refer to previous search sessions.',
                  'default':[],
                  'type':'text',
                  'options':None,
                 },
                 {'name':'pingback_token',
                  'description':'Pingback token.',
                  'default':[],
                  'type':'text',
                  'options':None,
                 },
                 {'name':'show_default_options',
                  'description':'Include these options in JSON response.',
                  'default':0,
                  'type':'number',
                  'options':[0, 1],
                 },
                 {'name':'reverse_search_by_text',
                  'description':'Reverse image search by text.',
                  'default':'',
                  'type':'text',
                  'options':None,
                 },
                 {'name':'reverse_search_by_image_url',
                  'description':'Reverse image search by image URL.',
                  'default':'',
                  'type':'text',
                  'options':None,
                 },
                 {'name':'reverse_search_by_id',
                  'description':'Reverse image search by ID.',
                  'default':'',
                  'type':'text',
                  'options':None,
                 },
                 ]

from mc_generic import space_pad

rating_options = [{"_id":"Overall",
                   "name":"Overall (1-5)",
                   "description":"Overall relevance rating.",
                   "default":0,
                   "is_ordinal":1,
                   "options":[[1,''], [2,''], [3,''], [4,''], [5,''],],
                   },
                  {"_id":"query_relevance",
                   "name":"Relevance (1-5)",
                   "description":"Relevance of this image to the query.",
                   "default":0,
                   "is_ordinal":1,
                   "options":[[1,''], [2,''], [3,''], [4,''], [5,''],],
                   },
                  {"_id":"aesthetics",
                   "name":"Aesthetics (1-5)",
                   "description":"Quality of the image in general, ignoring the query.",
                   "default":0,
                   "is_ordinal":1,
                   "options":[[1,'very bad'], [2,''], [3,''], [4,''], [5,''],],
                   },
                  #{"_id":"is_bad",
                  # "name":"Report Image",
                  # "description":'Fill in if you selected "very bad" above.',
                  # "default":0,
                  # "is_ordinal":0,
                  # "options":[[1, "pure spam"], [2, "keyword stuffing"], [3, "nsfw"], [4, "other"],],
                  # },
                  {"_id":"is_bad_nsfw",
                   "name":"NSFW (n/y)",
                   "description":'',
                   "default":0,
                   "is_ordinal":0,
                   "options":[[1, "no"], [2, "yes"]],
                   },
                  {"_id":"is_bad_spam",
                   "name":"Spam (n/y)",
                   "description":'',
                   "default":0,
                   "is_ordinal":0,
                   "options":[[1, "no"], [2, "yes"]],
                   },
                  {"_id":"is_bad_watermarks",
                   "name":"Watermark (n/y)",
                   "description":'',
                   "default":0,
                   "is_ordinal":0,
                   "options":[[1, "no"], [2, "yes"]],
                   },
                  #{"_id":"amateur_hour",
                  # "name":"Amateur Hour (1-5)",
                  # "description":"Looks like an amateur made it.",
                  # "default":0,
                  # "is_ordinal":1,
                  # "options":[[1,''], [2,''], [3,''], [4,''], [5,''],],
                  # },
                  #{"_id":"is_adult",
                  # "name":"Adult",
                  # "description":"Image contains adult material.",
                  # "default":0,
                  # "is_ordinal":1,
                  # "options":[[1, "safe"], [2, "risque"], [3, "explicit"]],
                  # },
                  ]

for xx in rating_options:
    aa = xx['name'][:xx['name'].index('(')].strip()
    bb = xx['name'][xx['name'].index('('):].strip()
    xx['name'] = space_pad(aa.upper(), ch='_', n=15) + bb
    xx['name'] = xx['name'].replace('WATERMARK___' ,'WATERMARK')


def do_beam(graph, max_beam = 5):
    """
    Example:
    
        do_beam([[(5,'a'), (4, 'b'), (3, 'c')], [(5,'d'), (4, 'e'), (3, 'f')], [(5,'g'), (4, 'h'), (3, 'i')],])

        -> [(125, ['a', 'd', 'g']), (100, ['a', 'e', 'g']), (100, ['a', 'd', 'h']), (80, ['a', 'e', 'h']), (75, ['a', 'f', 'g'])]
    """
    
    if (not graph) or (not graph[0]):
        return []
    
    longest_path = 0.0
    
    w0 = 1
    p0 = []
    for xx in graph:
        w0 *= xx[0][0]
        p0.append(xx[0][1])
    
    keep = [(w0, p0)]

    #print ('START_KEEP',keep)
    
    graph = [list(sorted(x, reverse=True)) for x in graph]
    
    queue = [(graph[0][x][0], 0, [graph[0][x][1]]) for x in xrange(len(graph[0]))]
    
    while queue:
        
        zz = queue.pop(0)
        
        #print ('POP',zz)
        
        (cur_weight, c, path) = zz
        
        if c + 1 == len(graph):
            continue
        
        for c2, (cur2_weight, cur2_obj) in enumerate(graph[c + 1]): #[:max_depth]

            #print ('VIS',c + 1, c2)
            
            new_weight = cur_weight * cur2_weight
            new_path = path + [cur2_obj]
            longest_path = max(float(len(new_path)), longest_path)

            if len(new_path) == len(graph):
                keep.append((new_weight, new_path))

            queue.append((new_weight, c + 1, new_path))
        
        q2 = [(longest_path / len(x[-1]), x) for x in queue]
        
        q2 = list(sorted(q2, reverse=True))[:max_beam]

        queue = [y for x,y in q2]

        keep.sort(reverse = True)
        keep = keep[:max_beam]
    
    return keep



def query_correct(query, order_model, num = 4, cutoff = 0.5):
    """
    Query correction.
    
    TODO, substantial performance increases are possible here, via methods including:
    - Inform probabilities based on a real language model.
    - Word segmentation based on a language model.
    - Vector-space suggestions of more frequent synonyms for rare words.
    
    input:
         'brown hotdog'

    Output:    
        [{"query":"brown hotdog", highlighted":[["brown ", 1],  ["hotdog ", 1], ]}]
    """

    if query.startswith('search_'):
        return []

    query = query.lower() ## ADDED
    
    from difflib import get_close_matches
    from editdistance import eval as eval_dist
    
    if not order_model:
        return []
    
    word_dict = order_model['worddict']
    
    in_orig = []
    cand = []
    num_found = 0

    #query_s = special_split(query)
    query_s = query.split()
    
    for w in query_s:
        if w in word_dict:
            in_orig.append(1)
            cand.append([(1.0, w)])
            num_found += 1
        else:
            in_orig.append(0)
            cand.append([(max(0,(100.0 - eval_dist(w, w2))) / 100.0, w2) for w2 in get_close_matches(w, word_dict, n = num, cutoff = cutoff)] \
                        # + [(0.0000001, w)]
                        )
    
    if num_found == len(query_s):
        print ('YES_FOUND_ALL',query_s)
        return []

    print ('NOT_FOUND_ALL',query_s, cand)

    try:
        rr = do_beam(cand, max_beam = num)
    except Exception as e:
        print ('BEAM_ERROR', e)
        rr = []

    yy3 = []
    for xx,yy in rr:
        yy2 = []
        for c, x in enumerate(yy):
            if c != len(yy) - 1:
                x += ' '
            yy2.append(x)
        yy3.append((xx, yy2))
    rr = yy3
    
    print ('SUGGEST',in_orig, rr)

    r2 = [{'query':' '.join(y), 'highlighted':zip(in_orig, y)} for x, y in rr]
    
    return r2


DO_XANN = False

class handle_search(BaseHandler):
    
    #disable XSRF checking for this URL:
    def check_xsrf_cookie(self): 
        pass
    
    @tornado.gen.coroutine
    def post(self, verbose = False):
        """
        Search for images.
        
        See: `/list_facets` endpoint, or `debug_options` field in returned the JSON response, for argument details.
        
        Example:
            $ curl "http://127.0.0.1:23456/search" -d '{"q":"crowd", "limit":1}'
            
            {
                "next_page": {
                    "limit": 15, 
                    "offset": 15, 
                    "token": "10908a245779ba1f1d371c40596ce487"
                 },
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
                            "editorial_source": "Getty Images Europe", 
                            "keywords": "People Vertical Crowd Watching France Police Force Cannes", 
                            "title": "'Loving' - Red Carpet Arrivals - The 69th Annual Cannes Film Festival"
                        }, 
                        "_type": "image"
                    }
                ]
            }
        """

        the_query_seg = ''
        
        tt0 = time()
        
        d = self.request.body

        if d.startswith('{'):
            if verbose:
                print ('OLD PARAMETERS FORMAT',d[:20])

        else:
            if verbose:
                print ('NEW PARAMETERS FORMAT',)
            d = self.get_argument('json','{}')            
        
        data = json.loads(d)
        
        is_debug_mode = intget(self.get_cookie('debug')) or intget(data.get('debug')) or intget(self.get_argument('debug','0'))
        
        if data.get('reconcile_task'):#data.get('rerank_eq', '').endswith('|RECONCILE_TASK'):
            
            #tt = data['rerank_eq']
            #tt = tt[:-len('|RECONCILE_TASK')]
            #ht = json.loads(tt)

            #print ('AAA',data['reconcile_task'])
            
            ht = json.loads(data['reconcile_task'])
            
            #{'queryk':queryk, 'rating_type':rating_type, 'user_id':user_id}
            
            
            print ('RECONCILE_TASK', ht)
            
            with open('/datasets/datasets/annotate/search_relevance_002/phase002_2-way.json') as f:
                hr = json.loads(f.read())
            
            qq = json.loads(ht['queryk'])

            task_images = hr[ht['user_id']][ht['queryk']][ht['rating_type']]

            print ('task_images',task_images)
            
            #{u'next_page': None, u'prev_page': None, u'query_info': {u'query_args': {u'doc_type': u'image', u'skip_query_cache': 0, u'allow_nsfw': 0, u'pretty': 1, u'q_text': u'donalds', u'rerank_eq': u'annotation_mode', u'index_name': u'getty_test', u'schema_variant': u'new', u'enrich_tags': 1, u'filter_incomplete': 0, u'include_thumb': False, u'canonical_id': 0, u'full_limit': 600, u'include_docs': 1, u'debug': 1, u'show_default_options': 1, u'q': u'donalds', u'token': [], u'filter_licenses': [u'Creative Commons'], u'filter_sources': u'ALL'}, u'query_elapsed_ms': 119, u'query_time': 1473322887}, u'results_count': u'RECONCILE: ITERATION 1', u'results': [{u'_source': {u'license': None, u'title': u'RECONCILE', u'sizes': {}, u'artist_name': None, u'source': None, u'image_url': None, u'keywords': []}, u'_score': -1.0, u'title': u'Previous: 1d37f4afa8124d2584957a7892dff48a=1, 7754d70c-9edc-4b30-9ae7-ca55508823a9=2', u'_previous_ratings': [[u'1d37f4afa8124d2584957a7892dff48a', 1], [u'7754d70c-9edc-4b30-9ae7-ca55508823a9', 2]], u'_id': u'f745d5493075b0d93ce1b25934a94cef', u'_has_conflict': False}, {u'_source': {u'license': None, u'title': u'RECONCILE', u'sizes': {}, u'artist_name': None, u'source': None, u'image_url': None, u'keywords': []}, u'_score': -1.0, u'title': u'Previous: 1d37f4afa8124d2584957a7892dff48a=1, 7754d70c-9edc-4b30-9ae7-ca55508823a9=3', u'_previous_ratings': [[u'1d37f4afa8124d2584957a7892dff48a', 1], [u'7754d70c-9edc-4b30-9ae7-ca55508823a9', 3]], u'_id': u'a076583d36107460cfd304a1728de6b1', u'_has_conflict': False}], u'default_options': [], u'reconcile_info': {u'min_voters': 2, u'queryk': u'{"q": "nba"}', u'set_key': u'2_1171821989289380032_Overall_1d37f4afa8124d2584957a7892dff48a', u'iteration': 1, u'rating_type': u'Overall'}, u'debug_options': []}

            """
            1) get images for i_id's
            2) add debug_info for all relevant debug types
            """

            if False:
                assert False, 'TODO: would need to add more info into the images for this to work.'
                
                xrr = yield self.es.search(index = mc_config.MC_INDEX_NAME,
                                          type = mc_config.MC_DOC_TYPE,
                                          source = {"query":{ "ids": { "values": task_images['results'] } } },
                                          )

                xrr = json.loads(rr.body)

                if 'error' in xrr:
                    #self.set_status(500)
                    self.write_json({'error':'ELASTICSEARCH_ERROR',
                                     'error_message':repr(xrr)[:1000],
                                     })
                    return

                task_images['results'] = xrr['hits']['hits']

            #####
            
            if is_debug_mode == 2:
                task_images['debug_options'] = debug_options

            #print ('MMM',ht['rating_type'], rating_options)
            
            task_images['rating_options'] = [x for x in rating_options if x['_id'] == ht['rating_type']]
            
            #######
            
            self.write_json(task_images)
            
            return

        
            
        if DO_FORWARDING:
            forward_url = mc_config.MC_DO_FORWARDING_URL
            if verbose:
                print ('FORWARDING', len(d), '->', forward_url,'headers:',dict(self.request.headers))
            response = yield AsyncHTTPClient().fetch(forward_url,
                                                     method = 'POST',
                                                     connect_timeout = 30,
                                                     request_timeout = 30,
                                                     body = d,
                                                     headers = dict(self.request.headers),
                                                     #allow_nonstandard_methods = True,
                                                     )
            d2 = response.body
            h2 = json.loads(d2)
            if verbose:
                print ('FORWARDING_RECEIVED', len(d2))
            #self.write(d2)
            #self.finish()
            self.write_json(h2)
            return
        
        try:
            data = json.loads(d)
        except KeyboardInterrupt:
            raise
        except:
            #self.set_status(500)
            self.write_json({'error':'JSON_PARSE_ERROR',
                             'error_message':'Could not parse JSON request.',
                            })
            return

        
        #is_debug_mode = intget(self.get_cookie('debug')) or intget(data.get('debug')) or intget(self.get_argument('debug','0'))
        
        #print ('DEBUG_MODE?', is_debug_mode)
        
        ###

        if 'pingdom' not in self.request.headers.get("User-Agent",'').lower():
            
            ak = {'0_endpoint':'/search',
                  '1_query': data.get('q'),
                  '2_real_ip':self.request.headers.get('X-Real-Ip'),
                  '4_api-key':self.request.headers.get('API-KEY'),
                  }
            msg = ak.copy()
            msg['3_connecting_ip'] = self.request.remote_ip
            msg['5_headers'] = dict(self.request.headers)
            msg = 'API_CALL:\n' + json.dumps(msg, indent=4, sort_keys=True)
            
            ## curl "http://api.mediachainlabs.com/search" -d '{"q":"cat", "limit": 20}' -H 'access-token: 075a3e36a0a52dcbc568c05788e8a713'

            access_token = self.request.headers.get("Access-Token")
            
            if access_token:
                msg = 'MEDIUM query: ' + repr(data.get('q')) + ' with key: '+ str(access_token) +'\n' + msg
                
                for xuser in ['tg', 'denisnazarov']:
                    tornado.ioloop.IOLoop.current().spawn_callback(self.alerts.send_alert_tornado,
                                                                   message = msg,
                                                                   alert_key = ak,
                                                                   channel = '@' + xuser
                                                                   )

                
            tornado.ioloop.IOLoop.current().spawn_callback(self.alerts.send_alert_tornado,
                                                           message = msg,
                                                           alert_key = ak,
                                                           )
        
        ###
        
        if 'help' in data:
            ## TODO: switch to using the "options" further below instead:
            ## Plain-text help:
            from inspect import getdoc, getframeinfo, currentframe
            self.set_header("Content-Type", "text/plain")
            self.write(getdoc(getattr(self, getframeinfo(currentframe()).function)).replace('\n','\r\n') + '\r\n')
            self.finish()
            return

        
        #diff = set(data).difference(['q', 'q_id', 'q_id_file', 'offset', 'limit',
        #                            'index_name', 'doc_type', 'include_docs', 'include_thumb', 'rerank_eq',
        #                            'filter_licenses', 'filter_sources', 'skip_query_cache', 'filter_incomplete',
        #                            'schema_variant', 'enrich_tags', 'token', 'canonical_id', 'allow_nsfw',
        #                            'pretty',
        #                            ])

        diff = set(data).difference([x['name'] for x in default_options + debug_options + [{'name':'reconcile_task'}]])
        if diff:
            #self.set_status(500)
            self.write_json({'error':'UNKNOWN_ARGS',
                             'error_message':repr(list(diff)),
                            })
            return

        
        ## New method:

        the_input = {}

        if is_debug_mode:
            the_input['debug'] = is_debug_mode
        else:
            the_input['debug'] = is_debug_mode


                    
        for arg in default_options + debug_options:
            
            the_default = arg['default']

            ## Override some defaults for debug mode:
            
            if (is_debug_mode == 1) and (arg['name'] == 'rerank_eq'):
                the_default = 'annotation_mode'
            
            ## Do it:

            if arg['type'] == 'number':
                the_input[arg['name']] = intget(data.get(arg['name'], 'BAD'), the_default)
            else:
                the_input[arg['name']] = data.get(arg['name'], the_default)
            
            #if arg['name'] == 'rerank_eq':
            #    print ('---------DEBUG_MODE_RANKING', the_input[arg['name']])
                
            ## For convenience:
            
            if arg['type'] == 'list-of-strings':
                if isinstance(the_input[arg['name']], basestring):
                    the_input[arg['name']] = [the_input[arg['name']]]
                    

        if the_input['q']:
            the_input['q'] = the_input['q'].strip()
                    
        the_input['q_text'] = the_input['q'] ## for reverse compatibility.
        
        ## Disallow user modification of these, for now:
        
        the_input['index_name'] = data.get('index_name', mc_config.MC_INDEX_NAME)
        the_input['doc_type'] = data.get('doc_type', mc_config.MC_DOC_TYPE)
        the_input['include_thumb'] = False
        the_input['full_limit'] = 1000
        
        
        ## TODO: need multiple image upload support?:

        q_id_file = False
        
        try:
            fileinfo = self.request.files['file'][0]
            print ("FILE UPLOAD", fileinfo['filename'])
            q_id_file = fileinfo['body']
        except KeyboardInterrupt:
            raise
        except:
            pass

        if q_id_file:
            the_input['q_id_file_hash'] = hashlib.md5(q_id_file).hexdigest()
        
        if q_id_file and the_input['q_id']:
            #self.set_status(500)
            self.write_json({'error':'PARAMS',
                             'error_message':'Use either q_id or HTTP image upload, but not both.',
                             })
            return                


        ## Remote ranking hints:
        
        remote_ids = []
        
        if False:
            if the_input['q_text'] and (get_remote_search is not False):
                t1 = time()
                remote_ids = get_remote_search(the_input['q_text'])
                if verbose:
                    print ('REMOTE_IDS','time:',time()-t1,len(remote_ids))

        ## Neural reverse image search by image / text / id:

        neural_vectors_mode = False
        
        if the_input['reverse_search_by_text']:
            neural_vectors_mode = True
            rr = reverse_image_lookup_index(q_text = the_input['reverse_search_by_text'],
                                            #num_k = the_input['limit'],
                                            )
            self.write_json({'ids':rr})
            return

        if (the_input['q_text'] or '').startswith('search_text:'):
            neural_vectors_mode = True
            rr = reverse_image_lookup_index(q_text = the_input['q_text'][len('search_text:'):],
                                            #num_k = the_input['limit'],
                                            )
            remote_ids = rr        
        
        if the_input['reverse_search_by_id']:
            neural_vectors_mode = True
            rr = reverse_image_lookup_index(q_image_id = the_input['reverse_search_by_id'],
                                            #num_k = the_input['limit'],
                                            )
            self.write_json({'ids':rr})
            return

        q_text_orig = ''
        
        if the_input['q_text']:
            q_text_orig = the_input['q_text'].strip()
            the_input['q_text'] = the_input['q_text'].replace('search_id:', 'related:')
        
        if (the_input['q_text'] or '').startswith('related:'):
            neural_vectors_mode = True
            rr = reverse_image_lookup_index(q_image_id = the_input['q_text'][len('related:'):],
                                            #num_k = the_input['limit'],
                                            )
            remote_ids = rr
        
        if False:#the_input['reverse_search_by_url']:
            neural_vectors_mode = True
            assert False, 'todo'
            response = yield AsyncHTTPClient().fetch(the_input['reverse_search_by_url'],
                                                     method = 'POST',
                                                     connect_timeout = 10,
                                                     request_timeout = 10,
                                                     body = d,
                                                     headers = dict(self.request.headers),
                                                     #allow_nonstandard_methods = True,
                                                     )
            d3 = response.body
            rr = reverse_image_lookup_index(q_image_bytes = d3,
                                            #num_k = the_input['limit'],
                                            )
            self.write_json({'ids':rr})
            return
        
        ## Cache & token lookup:
        
        input_token = data.get('token', None)

        the_token = input_token
        
        rr = False

        
        ## New method:

        query_args = the_input.copy()

        for kk in ['limit','offset',]:
            if kk in query_args:
                del query_args[kk]

        query_args['debug'] = intget(self.get_cookie('debug')) or intget(data.get('debug')) or intget(self.get_argument('debug','0'))

        if verbose:
            print ('QUERY_ARGS',query_args)
        
        ## ignore those with default args:
        for k,v in query_args.items():
            if v is None:
                del query_args[k]

        
        if the_token:
            assert 'debug' in the_input,the_input
            
            rr = query_cache_lookup(the_token, skip_query_cache = the_input['skip_query_cache'])

            if rr is False:
                #self.set_status(500)
                self.write_json({'error':'EXPIRED_TOKEN',
                                 'error_message':the_token,
                                 })
                return                

        else:
            the_token = consistent_json_hash(query_args)
            
            rr = query_cache_lookup(the_token, skip_query_cache = the_input['skip_query_cache'])

            
        if rr is not False:
            
            # [u'query_info', u'results_count', u'results', 'cache_hit']
            if verbose:
                print ('QUERY_CACHE_LOOKUP', rr.keys())#['query_info']['query_args'])
            #return
            #raw_input()

            if verbose:
                print ('CACHE_OR_TOKEN_HIT_QUERY','offset:', the_input['offset'], 'limit:', the_input['limit'], 'len(results)',len(rr['results']))
            
            
            results_count = len(rr['results'])
                                                
            if the_input['offset'] + the_input['limit'] >= len(rr['results']):
                rr['next_page'] = None
            else:
                rr['next_page'] = {'token':the_token, 'pingback_token':the_token, 'offset':the_input['offset'] + the_input['limit'], 'limit':the_input['limit']}

            if the_input['offset'] == 0:
                rr['prev_page'] = None
            else:
                rr['prev_page'] = {'token':the_token, 'pingback_token':the_token, 'offset':max(0, the_input['offset'] - the_input['limit']), 'limit':the_input['limit']}

            rr['results'] = rr['results'][the_input['offset']:the_input['offset'] + the_input['limit']]

            if the_input['show_default_options']:
                rr['default_options'] = default_options
            
            if is_debug_mode == 2:
                rr['debug_options'] = debug_options

            if is_debug_mode:                
                rr['rating_options'] = rating_options
            
            #rr['query_info'] = {'query_args':query_args, 'query_time':int(tt0), 'query_elapsed_ms': int((time() - tt0) * 1000)}

            #assert (the_input['q_text'] or the_input['q_id'] or q_id_file or the_input['canonical_id']), ('NO QUERY?', the_input)
            #assert (rr['query_info']['query_args']['q_text'] or rr['query_info']['query_args']['q_id'] or q_id_file or rr['query_info']['query_args']['canonical_id']), ('NO QUERY?', rr['query_info']['query_args'])

            if verbose:
                print ('TOP_LEVEL_KEYS_1',rr.keys(), rr['query_info']['query_args'].get('q'))
            self.write_json(rr,
                            pretty = the_input['pretty'],
                            max_indent_depth = data.get('max_indent_depth', False),
                            )
            
            return

        if verbose and rr:
            print ('ZZZ',rr['query_info'])
        
            #assert (rr['query_info']['query_args']['q_text'] or rr['query_info']['query_args']['q_id'] or q_id_file or rr['query_info']['query_args']['canonical_id']), ('NO QUERY?', rr['query_info']['query_args'])

        is_id_search = False
        
        if not (the_input['q_text'] or the_input['q_id'] or q_id_file or the_input['canonical_id']):
            
            ## Match all mode, skip cache:
            
            rr = yield self.es.search(index = the_input['index_name'],
                                      type = the_input['doc_type'],
                                      source = {"query": {"match_all": {}}, "size":20}
                                      )

            
            try:
                rr = json.loads(rr.body)
            except Exception as e:
                #self.set_status(500)
                self.write_json({'error':'ELASTICSEARCH_JSON_ERROR',
                                 'error_message':'Elasticsearch down or timeout? - ' + repr(rr.body)[:1000],
                                 })
                return

            if 'error' in rr:
                #self.set_status(500)
                self.write_json({'error':'ELASTICSEARCH_ERROR',
                                 'error_message':repr(rr)[:1000],
                                 })
                return

            
            rr = rr['hits']['hits']
            rr = {'results':rr,
                  'next_page':None,
                  'prev_page':None,
                  'results_count':'>' + ('{:,}'.format(the_input['full_limit'])),
                  }
            
            if the_input['show_default_options']:
                rr['default_options'] = default_options
            
            if is_debug_mode == 2:
                rr['debug_options'] = debug_options

            if is_debug_mode:
                rr['rating_options'] = rating_options

            rr['query_info'] = {'query_args':query_args, 'query_time':int(tt0), 'query_elapsed_ms': int((time() - tt0) * 1000)}

            if verbose:
                print ('TOP_LEVEL_KEYS_2',rr.keys(), rr['query_info']['query_args'].get('q'))
            self.write_json(rr)
            return

        
        if (the_input['q_text'] and the_input['q_id']):
            #self.set_status(500)
            self.write_json({'error':'BAD_QUERY',
                             'error_message':'Simultaneous `q` and `q_id` not yet implemented.',
                             })
            return

        
        elif the_input['q_id'] or q_id_file or the_input['canonical_id']:
            
            if the_input['canonical_id']:
                ## Search by canonical_id:

                if verbose:
                    print ('CANONICAL_ID_SEARCH', the_input['canonical_id'])
                
                query = {"query": {"constant_score": {"filter": {"term": {"canonical_id": the_input['canonical_id']}}}}}
            
            elif q_id_file or (the_input['q_id'].startswith(data_pat) or the_input['q_id'].startswith(data_pat_2)):
                
                #Resolve ID(s) for query based on content.
                #Note that this is similar to `/dupe_lookup` with `include_docs` = True:
                
                if True:
                    if verbose:
                        print ('NEURAL-CONTENT-BASED-SEARCH',)

                    rr = reverse_image_lookup_index(q_image_bytes = q_id_file,
                                                    #num_k = the_input['limit'],
                                                    )
                    
                    #query = {"query":{ "ids": { "values": rr } } }
                    query = {}
                    remote_ids = rr
                    
                else:
                    if verbose:
                        print ('OLD-CONTENT-BASED-SEARCH',)
                    
                    model = mc_models.VECTORS_MODEL_NAMES['baseline']()

                    if the_input['q_id'] and  (the_input['q_id'].startswith(data_pat) or the_input['q_id'].startswith(data_pat_2)):
                        if verbose:
                            print ('GOT_DATA_URI')
                        terms = model.img_to_terms(img_data_uri = the_input['q_id'])
                    else:
                        if verbose:
                            print ('GOT_RAW_BYTES')
                        assert q_id_file
                        terms = model.img_to_terms(img_bytes = q_id_file)

                    if verbose:
                        print ('TERMS',repr(terms)[:100])


                    rr = yield self.es.search(index = the_input['index_name'],
                                              type = the_input['doc_type'],
                                              source = {"query": {"constant_score":{"filter":{"term": terms}}}},
                                              )
                    if verbose:
                        print ('GOT_Q_ID_FILE_OR_Q_ID',repr(rr.body)[:100])

                    try:
                        rr = json.loads(rr.body)
                    except Exception as e:
                        #self.set_status(500)
                        self.write_json({'error':'ELASTICSEARCH_JSON_ERROR',
                                         'error_message':'Elasticsearch down or timeout? - ' + repr(rr.body)[:1000],
                                         })
                        return


                    if 'error' in rr:
                        #self.set_status(500)
                        self.write_json({'error':'ELASTICSEARCH_ERROR',
                                         'error_message':repr(rr)[:1000],
                                         })
                        return


                    rr = [x['_id'] for x in rr['hits']['hits']]

                    query = {"query":{ "ids": { "values": rr } } }
                
            else:
                #ID-based search:
                
                is_id_search = True

                if verbose:
                    print ('ID-BASED-SEARCH', the_input['q_id'])
                query = {"query":{ "ids": { "values": [ the_input['q_id'] ] } } }
        
        
        elif the_input['q_text']:
            
            the_q_text = the_input['q_text']

            #the_q_text = ' '.join([x for x in the_q_text.split() if not x.startswith('xann')])
            
            ## text-based search:
            
            #query = {"query": {"multi_match": {"query":    the_q_text,
            #                                   "fields": [ "*" ],
            #                                   "type":     "cross_fields",
            #                                   "minimum_should_match": "100%",
            #                                   },
            #                   },
            #         }

            if the_input['rerank_eq'] == 'neural_hybrid_switch_old':
                factor_field = "aesthetics.score"
            else:
                factor_field = "score_global"
                
            query ={"query": {"function_score": {"query": {"multi_match": {"query":    the_q_text,
                                                                           "fields": [ "*" ],
                                                                           "type":     "cross_fields",
                                                                           "minimum_should_match": "100%",
                                                                           },
                                                           },
                                                 "functions": [{"field_value_factor": {"field": factor_field,
                                                                                       "missing":0.1,
                                                                                       }
                                                                }]
                                                 },
                              },
                    }

            if (len(q_text_orig) == 32) and (not set(q_text_orig).difference('0123456789abcdef')):

                ## Auto-detect ID search:
                
                query = {"query":{ "ids": { "values": [q_text_orig] } },
                         "size": 50,
                         }
            
            elif the_input['filter_sources'] and ('ALL' not in the_input['filter_sources']) and (not is_id_search) and (not neural_vectors_mode):

                if verbose:
                    print ('FILTER_SOURCES', the_input['filter_sources'])
                
                assert isinstance(the_input['filter_sources'], basestring)

                if the_input['filter_sources'] in source_names:
                    the_input['filter_sources'] = source_names[the_input['filter_sources']]
                
                inner_part = [{"multi_match": {"query":    the_q_text,
                                              "fields": [ "*" ],
                                              "type":     "cross_fields",
                                              "minimum_should_match": "100%",
                                              },
                              }]
                
                inner_part += [{"term": {"source_dataset": the_input['filter_sources']}}]

                if the_input['rerank_eq'] == 'neural_hybrid_switch_old':
                    factor_field = "aesthetics.score"
                else:
                    factor_field = "score_global"
                
                query = {"query": {"function_score": {"query": {"constant_score": {"filter": {"bool": {"must":inner_part}}}},
                                                      "functions": [{"field_value_factor": {"field": factor_field,
                                                                                            "missing":0.1,
                                                                                            }
                                                                }]
                                                      },
                                   
                                   },
                         }

                #query["sort"] = [{"aesthetics.score" : {"order" : "desc"}}]

        
        remote_hits = []
        
        #assert remote_ids,remote_ids
        if remote_ids:
            t1 = time()

            rrr = []
            remote_hits = []
            
            for ccc in xrange(10):

                print ('LOOP_REMOTE_HITS', ccc)
                
                xx_remote_ids = remote_ids[ccc * 50:(ccc + 1) * 50]

                if not xx_remote_ids:
                    break
                
                rr = yield self.es.search(index = the_input['index_name'],
                                          type = the_input['doc_type'],
                                          source = {"query":{ "ids": { "values": xx_remote_ids } },
                                                    "size": 50,
                                                    },
                                          )
                #print ('GOT_REMOTE_HITS','time:',time() - t1,repr(rr.body)[:100])

                hh = False

                try:
                    hh = json.loads(rr.body)
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    #print ('BAD_BODY',rr)
                    #self.set_status(500)
                    self.write_json({'error':'ELASTICSEARCH_JSON_ERROR_REMOTE_IDS',
                                     'error_message':'Elasticsearch down or timeout? - remote_ids ' + repr(rr.body)[:1000],
                                     })
                    return

                if 'error' in hh:
                    #self.set_status(500)
                    self.write_json({'error':'ELASTICSEARCH_ERROR',
                                     'error_message':repr(hh)[:1000],
                                     })
                    return

                print ('GOT_REMOTE_HITS', time() - t1, len(hh['hits']['hits']))

                if hh['hits']['hits']:
                    remote_hits.extend(hh['hits']['hits'])
            

            if verbose:
                print ('GOT_REMOTE_HITS',len(remote_ids), '->', len(remote_hits), [x['_id'] for x in remote_hits[:10]])

            ## Fix ordering...:

            remote_hits = [x for x in remote_hits if x]
            
            #print ('REMOTE_IDS', remote_ids)
            
            tmp = {x['_id']:x for x in remote_hits}
            r2 = []
            for xx in remote_ids:
                aa = tmp.get(xx)
                if (aa is not False) and (aa is not None):
                    r2.append(aa)
            remote_hits = r2
            
            for xx in remote_hits:
                #print ('RHIT', xx)
                xx['_score'] = 10.0
                xx['_source']['boosted'] = 1
                
            for xx in remote_hits:
                assert xx['_source']['boosted'] == 1


        xann_hits = []
        
        if DO_XANN and the_input['q_text'] and (not the_input['exclusive_to_text']):

            ## enrich for image content-based search:

            #assert mc_crawlers.order_model_cache[0], 'BAD_DDD'

            wd = self.order_model_cache['order_model']['worddict']

            xann_any_good = False
            the_q_text_2 = False
            
            for w in the_input['q_text'].split():
                if w.startswith('xann'):
                    xann_any_good = True
                if w in wd:
                    xann_any_good = True
            

            if xann_any_good:
                the_q_text_2 = relevance_ann_query_to_concepts(the_input['q_text'],
                                                               the_order_model_cache = self.order_model_cache,
                                                               )
                
                #the_q_text_2 = the_q_text_2.split() + [x for x in the_input['q_text'].split() if x.startswith('xann')]
            
            if the_q_text_2:
                query2 = {"query": {"multi_match": {"query": the_q_text_2,
                                                   "fields": [ "xann" ],
                                                   "type":   "most_fields"
                                                   },
                                   },
                          "size": 300,
                         }


                t1 = time()
                rr = yield self.es.search(index = the_input['index_name'],
                                          type = the_input['doc_type'],
                                          source = query2,
                                          )
                
                try:
                    hh = json.loads(rr.body)
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    #self.set_status(500)
                    self.write_json({'error':'ELASTICSEARCH_JSON_ERROR',
                                     'error_message':'Elasticsearch down or timeout? - ' + repr(rr.body)[:1000],
                                     })
                    return
                
                xann_hits = hh['hits']['hits']

                for ii in xann_hits:
                    ii['_score'] *= 0.1

                if verbose:
                    print ('XANN_GOT','time:',time() - t1, repr(rr.body)[:100])
                
                
        query['from'] = the_input['offset']
        query['size'] = the_input['full_limit']
        query['timeout'] = '5s'   ## TODO - RETURNS PARTIALLY ACCUMULATED HITS WHEN TIMEOUT OCCURS

        if verbose:
            print ('QUERY',query)

        if neural_vectors_mode or q_id_file:
            rr = []

        else:
            t1 = time()
            rr = yield self.es.search(index = the_input['index_name'],
                                      type = the_input['doc_type'],
                                      source = query,
                                      )

            if verbose:
                print ('GOT','time:',time() - t1, repr(rr.body)[:100])

            hh = False

            try:
                hh = json.loads(rr.body)
            except KeyboardInterrupt:
                raise
            except Exception as e:
                #self.set_status(500)
                self.write_json({'error':'ELASTICSEARCH_JSON_ERROR',
                                 'error_message':'Elasticsearch down or timeout? - ' + repr(rr.body)[:1000],
                                 })
                return

            if 'error' in hh:
                #self.set_status(500)
                self.write_json({'error':'ELASTICSEARCH_ERROR',
                                 'error_message':repr(hh)[:1000],
                                 })
                return

            rr = hh['hits']['hits']
            
            
        if neural_vectors_mode or q_id_file:
            rr = remote_hits
        elif the_input['exclusive_to_ann']:
            rr = xann_hits
        else:
            rr = xann_hits + remote_hits + rr
        
        #print ('HITS_A', len(rr))
        
        ## NSFW filtering:

        if (not is_id_search):# and (not neural_vectors_mode):
            if not the_input['allow_nsfw']:

                r2 = []
                for xx in rr:
                    if not xx['_source'].get('nsfw'):
                        r2.append(xx)
                rr = r2

        #print ('HITS_B', len(rr))

        
        ## Prepend remote hits, filter ID dupes:

        done = set()
        r2 = []
        for xx in rr:
            if xx['_id'] in [u'pexels_85601',
                             'feb4186c66cd5255e34ba2fdce5a386c',
                             'f148a4e1c3a985871a3e529755d93202',
                             '3aced17eabda5ac3afaf0e3e0cb7786e',
                             'f0dda8781292b1e3f1ccbf764a9dee94',
                             '1ab346ea9420f2bb62f0cae82bf9d32b',
                             ]:
                continue
            
            if xx['_id'] in done:
                continue
            done.add(xx['_id'])
            r2.append(xx)
        rr = r2

        #print ('HITS_D', len(rr))

        ## Remove inline thumbnail data URIs:
        if not the_input['include_thumb']:
            for x in rr:
                if 'image_thumb' in x['_source']:
                    del x['_source']['image_thumb']
                if 'thumbnail_base64' in x['_source']:
                    del x['_source']['thumbnail_base64']


        ## Remove `dedupe_*` fields:

        if False:
            for xx in rr:
                xx['_source']  = {x:y
                                  for x,y
                                  in xx['_source'].iteritems()
                                  if not x.startswith('dedupe_')
                                  }
        
        
        ## Debug info:
        
        if is_debug_mode == 2:
            for ii in rr:
                
                ## add debug info dict:
            
                ii['debug_info'] = {}
                ii['debug_info_show'] = []
            
                ii['debug_info']['native_id'] = ii['_source'].get('native_id', False)                
                ii['debug_info']['width'] = ii['_source'].get('sizes') and ii['_source'].get('sizes')[0].get('width', False)
            if verbose:
                print ('YES_DEBUG', is_debug_mode)
        else:
            if verbose:
                print ('NO_DEBUG', is_debug_mode)
                

        ## Add in frontend image cached preview images:
        ## Skip items without preview:

        from mc_ingest import lookup_cached_image
        
        image_cache_failed = False
        
        r2 = []
        for ii in rr:

            ## From inline thumbnail:
            
            urls = lookup_cached_image(_id = ii['_id'],
                                       do_sizes = ['1024x1024',],
                                       )
            
            ii['_source']['url_direct_cache'] = {'url':urls['1024x1024']}
            
            ## Tag enrichment, using the image cache URLs:
            
            the_input['enrich_tags'] = False ## TEMPORARILY DISABLE
            
            if the_input['enrich_tags'] and is_id_search and get_enriched_tags:
                
                etags = []
                try:
                    etags = get_enriched_tags([urls['1024x1024']])[0]
                except KeyboardInterrupt:
                    raise
                except:
                    if verbose:
                        print ('ENRICH_TAGS_FAILED - API KEYS?',)
                
                if not ii['_source'].get('keywords'):
                    ii['_source']['keywords'] = []
                
                #ii['_source']['keywords'].extend(etags)
                
                if is_debug_mode:
                    #ii['debug_info']['external_tags'] = [(a.strip(),b.strip()) for a,b in [x.split('@') for x in etags]]
                    ii['debug_info']['external_tags'] = ', '.join(etags)
                
            r2.append(ii)

            continue
        
        rr = r2
        
        #print ('HITS_E', len(rr))
        
        ## Apply post-ingestion normalizers, if there are any:
                
        mc_normalize.apply_post_ingestion_normalizers(rr, schema_variant = the_input['schema_variant'])

        frontend_required = [('artist_name',None),
                             ('keywords',[]),
                             #('id',None),
                             ('license',None),
                             ('title',None),
                             ('source',None),
                             ('sizes',{}),
                             ('image_url',None)
                             ]
        
        for ii in rr:
            for k,v in frontend_required:
                if k not in ii['_source']:
                    if verbose:
                        print ('ADDED_FOR_FRONTEND',k,'=',v)
                    ii['_source'][k] = v
        

        ## Filter to only those exclusive to ann:
                    
        if the_input['exclusive_to_ann']:
            if verbose:
                print ('EXCLUSIVE_TO_ANN', len(rr))
            
            ann_tags = set(x.replace('xann','') for x in the_input['q_text'].split() if x.startswith('xann'))

            if verbose:
                print ('ANN_TAGS', ann_tags)
            
            r2 = []
            for ii in rr:
                if ann_tags.intersection([x.lower() for x in ii['_source']['keywords']]):
                    continue
                if ann_tags.intersection((ii['_source']['title'] or '').lower().split()):
                    continue
                if ann_tags.intersection((ii['_source']['artist_name'] or '').lower().split()):
                    continue
                if ann_tags.intersection((ii['_source'].get('description') or '').lower().split()):
                    continue
                r2.append(ii)
            
            rr = r2

            the_input['q_text'] = ' '.join([(x.startswith('xann') and x.replace('xann','') or x) for x in the_input['q_text'].split()])
            
        #print ('HITS_C', len(rr))

        ## Neural query relevance model:

        neural_rel_scores = False
        score_at_1 = False
        score_at_10 = False
        
        try:
            if (get_neural_relevance is not False) and the_input['q_text'] and (not neural_vectors_mode) and (not q_id_file): #is_debug_mode and
                #assert mc_crawlers.order_model_cache[0], 'BAD_CCC'
                xx = self.order_model_cache
                neural_rel_scores, the_query_seg, score_at_1, score_at_10 = \
                        get_neural_relevance(the_input['q_text'],
                                             rr,
                                             order_model = xx['order_model'],
                                             )
                
                ## moved into get_neural_relevance:
                for c, xx in enumerate(neural_rel_scores['result_scores']):
                    #assert '_neural_rel_score' in rr[c], 'MISSING_KEY'
                    #assert xx is not False, repr(xx)
                    rr[c]['_neural_rel_score'] = xx
                
        except Exception as e2:
            raise
            print ('ERROR_NERUAL_RELEVANCE_OUTER',e2)

        ## Add xann to keywords:

        if DO_XANN:
            for ii in rr:
                if 'xann' in ii['_source']:
                    ii['_source']['keywords'].extend(ii['_source'].get('xann',[]))

                        
        ## Re-rank:

        if (not is_id_search) and (not neural_vectors_mode) and (not q_id_file):
            
            for ii in rr:
                ii['_source']['artist_name_orig'] = ii['_source']['artist_name'] ## Back this up

            rrm = ReRankingBasic(eq_name = the_input['rerank_eq'])
            rr = rrm.rerank(q_text_orig, rr, is_debug_mode)


        ## Diversity penalty:

        if (the_input['rerank_eq'] != 'annotation_mode') and (not is_id_search) and (not neural_vectors_mode) and (not q_id_file):

            if verbose:
                print ('------DIVERSITY_PENALTY',the_input['rerank_eq'])
            
            seen_artists = Counter()
            r2 = []
            for cc, ii in enumerate(rr):
                xx = ii['_source']['artist_name_orig']
                if xx in seen_artists:
                    #print ('DIVERSITY_PENALTY', cc, xx)
                    ii['_score'] *=  (0.0000001 ** seen_artists[xx])
                if xx and ('simply mad' in xx.lower()):
                    #print ('FOUND BAD',xx, ii['_score'])
                    ii['_score'] *=  0.001
                seen_artists[xx] += 1
                r2.append((ii['_score'], ii))

            rr = [y for x,y in sorted(r2, reverse = True)]
                        
        ## Debug stats on frontend:
        
        if is_debug_mode == 2:
            for ii in rr:
                sc = ii['_score']#ii['_source'].get('aes_unsplash_out_v1',{}).get('like_unsplash',-1)
                if sc is None:
                    xx = 'None'
                else:
                    xx = '%0.3f' % sc
                    if sc <= 0:
                        xx = '(' + xx + ')'

                sc2 = ii['score_old']
                xx2 = '%0.3f' % sc2
                if sc2 <= 0:
                    xx2 = '(' + xx2 + ')'

                #ii['_source']['artist_name'] = 'Score: ' + xx + ' TFIDF: ' + xx2
                
                yy = []
                #for nm, kk in [('tfidf','_norm_score'), ('neur_rel','_norm_neural_rel_score'), ('aes','_norm_aesthetics_score')]:
                #    yy.append(nm + (': %.3f' % ii.get(kk,-100)))

                yy = []
                for kk,vv in [('score', ii.get('_score',False)),
                              ('tfidf', ii.get('score_old',False)),
                              ('ss', ii.get('_switch_score',False)),
                              ('n', ii.get('_neural_rel_score',False)),
                              ('g', ii['_source'].get('score_global',False)),
                              ('a1', ii.get('_aesthetics_score',False)),
                              ('a2', ii['_source'].get('score_aesthetics_2',False)),
                              ('a3', ii['_source'].get('score_aesthetics_3',False)),
                              ]:
                            if vv is False:
                                yy.append(kk + u': \u2718')
                            else:
                                yy.append(kk + (': %.3f' % vv))

                ii['_source']['artist_name'] = ', '.join(yy) + '.'

        
        ## Status checkboxes in names:
        if False:#is_debug_mode == 2:

            for ii in rr:

                if ('_aesthetics_score' in ii): ## not present for id-based searches.

                    if ('_aesthetics_score' in ii) and (ii['_aesthetics_score'] is not False):
                        ii['_source']['artist_name'] = (ii['_source']['artist_name'] or '') +  u" a1=\u2713"
                    else:
                        ii['_source']['artist_name'] = (ii['_source']['artist_name'] or '') +  u" a1=\u2718"

                    if ('_neural_rel_score' in ii) and (ii['_neural_rel_score'] is not False):
                        ii['_source']['artist_name'] = (ii['_source']['artist_name'] or '') + u" n=\u2713"
                    else:
                        ii['_source']['artist_name'] = (ii['_source']['artist_name'] or '') +  u" n=\u2718"
                    
                    if ('_switch_score' in ii):
                        ii['_source']['artist_name'] = (ii['_source']['artist_name'] or '') + (u" ss=%.3f" % ii['_switch_score'])
                    else:
                        ii['_source']['artist_name'] = (ii['_source']['artist_name'] or '') +  u" ss=\u2718"
                    
                    if ('score_aesthetics_2' in ii['_source']) and (ii['_source']['score_aesthetics_2'] is not False):
                        ii['_source']['artist_name'] = (ii['_source']['artist_name'] or '') +  u" a2=\u2713"
                    else:
                        ii['_source']['artist_name'] = (ii['_source']['artist_name'] or '') +  u" a2=\u2718"
                    
                    if ('score_aesthetics_3' in ii['_source']) and (ii['_source']['score_aesthetics_3'] is not False):
                        ii['_source']['artist_name'] = (ii['_source']['artist_name'] or '') +  u" a3=\u2713"
                    else:
                        ii['_source']['artist_name'] = (ii['_source']['artist_name'] or '') +  u" a3=\u2718"
                    
                    if ii.get('_neural_rel_score',0) < 1.70:
                        ii['_source']['artist_name'] = (ii['_source']['artist_name'] or '') +  u' n=LR'
                    
                    #ii['_source']['artist_name'] += ' ' + ', '.join([('%s=%.2f' % (k,v)) for k,v in ii['_source'].get('aesthetics',{}).items() if type(v) == float])

        for ii in rr:
            #ii['_source']['keywords'].append('related:' + ii['_id'])

            ii['_source']['keywords'] = [x for x in ii['_source']['keywords'] if x.strip()]
                 
        ## Note: swapping these for ES queries shortly:

        if isinstance(the_input['filter_licenses'], basestring):
            the_input['filter_licenses'] = [the_input['filter_licenses']]
        if isinstance(the_input['filter_sources'], basestring):
            the_input['filter_sources'] = [the_input['filter_sources']]

        if verbose:
            print ('filter_licenses_in',the_input['filter_licenses'])
        
        if the_input['filter_licenses'] and ('ALL' not in the_input['filter_licenses']) and (not is_id_search) and (not neural_vectors_mode) and (not q_id_file):

            dl = Counter()
            
            filter_licenses_s = set(the_input['filter_licenses'])
            r2 = []
            for ii in rr:
                
                native_id = ii['_source'].get('native_id', '')
                
                ii['_source']['license_tags'] = ii['_source'].get('license_tags') or []
                
                assert type(ii['_source']['license_tags']) == list, ii['_source']['license_tags']

                if 'Creative Commons' in the_input['filter_licenses']:
                    ## The currently-ingested, except for these 2 datasets, should be all open-licensed:
                    if ('getty_' not in native_id) and ('eyeem_' not in native_id):
                        ii['_source']['license_tags'].append('Creative Commons') 
                
                ## ANY match:
                if filter_licenses_s.intersection(ii['_source'].get('license_tags',[])):
                    r2.append(ii)
                else:
                    dl[ii['_source']['source']['name']] += 1

            if verbose:
                print ('FILTER_LICENSES', the_input['filter_licenses'], len(rr),'->',len(r2), dl.most_common())
            rr = r2

        if verbose:
            print ('filter_sources_in',the_input['filter_sources'])

        if False: #the_input['filter_sources'] and ('ALL' not in the_input['filter_sources']) and (not is_id_search):
            filter_sources_s = set(the_input['filter_sources'])
            r2 = []
            for ii in rr:
                if filter_sources_s.intersection(ii['_source'].get('source_tags',[])):
                    r2.append(ii)
            if verbose:
                print ('FILTER_SOURCES', the_input['filter_sources'], len(rr),'->',len(r2))
            rr = r2
        
        
        ## Include or don't include full docs:
        
        if not the_input['include_docs']:
            
            rr = [{'_id':hit['_id']}
                  for hit
                  in rr
                  ]

        
        ## Now safe to add rank_number:

        for c, ii in enumerate(rr):
            ii['rank'] = c
        
        
        ## Cache:
        
        results_count = len(rr)

        #assert 'q' in query_args, query_args.keys()

        rct = (results_count >= the_input['full_limit'] * 0.8) and \
              (('{:,}'.format(the_input['full_limit'])) + '+') or \
              ('{:,}'.format(results_count))

        rct = (results_count >= the_input['full_limit'] * 0.8) and \
              ('>' + ('{:,}'.format(the_input['full_limit']))) or \
              ('{:,}'.format(results_count))

        if is_debug_mode == 2:
            rct += the_query_seg


        if verbose:
            print ('HITS_G', len(rr))
        
        rr = {'results':rr,
              'results_count':rct,
              'query_info': {'query_args':query_args,
                             'query_time':int(tt0),
                             'query_elapsed_ms': int((time() - tt0) * 1000),
                             }
              }

        if the_input['q_text']:
            xx = self.order_model_cache
            rr['query_suggestions'] = query_correct(the_input['q_text'], xx['order_model'])
        else:
            rr['query_suggestions'] = []
        
        query_cache_save(the_token, rr)
        
        ## Wrap in pagination:
        
        if the_input['offset'] + the_input['limit'] >= len(rr['results']):
            rr['next_page'] = None
        else:
            rr['next_page'] = {'token':the_token, 'pingback_token':the_token, 'offset':the_input['offset'] + the_input['limit'], 'limit':the_input['limit']}
        
        if the_input['offset'] == 0:
            rr['prev_page'] = None
        else:
            rr['prev_page'] = {'token':the_token, 'pingback_token':the_token, 'offset':max(0, offset - the_input['limit']), 'limit':the_input['limit']}
        
        ## Trim:
        
        rr['results'] = rr['results'][the_input['offset']:the_input['offset'] + the_input['limit']]
        
        ## Output:

        if the_input['show_default_options']:
            rr['default_options'] = default_options
        
        if is_debug_mode == 2:
            rr['debug_options'] = debug_options

        if is_debug_mode:
            rr['rating_options'] = rating_options

        #rr['query_info'] = {'query_args':query_args, 'query_time':int(tt0), 'query_elapsed_ms': int((time() - tt0) * 1000)}

        if verbose:
            print ('TOP_LEVEL_KEYS_3',rr.keys(), rr['query_info']['query_args'].get('q'))
        self.write_json(rr,
                        pretty = the_input['pretty'],
                        max_indent_depth = data.get('max_indent_depth', False),
                        )


from random import uniform, randint, choice, random, shuffle

def weighted_choice(choices, total):
    while True:
        r = uniform(0, total)
        cur = 0
        for w, val in choices:
            if cur + w >= r:
                return val
            cur += w
   

class handle_random_query(BaseHandler):
    
    #disable XSRF checking for this URL:
    def check_xsrf_cookie(self): 
        pass
    
    @tornado.gen.coroutine
    def get(self):
        """        
        Return or redirect to a random search query.

        Query Args:
            as_url:      Redirect to a URL instead of returning JSON, if "1".
            only_mwe:    Only multi-word queries, if "1",
            user_id:     User id for reconciliation mode.
        """

        from urllib import quote
        
        user_id = self.get_argument('user_id', False)
        
        if False:#user_id:
            
            print ('YES_USER_ID', user_id)
            
            fn = '/datasets/datasets/annotate/search_relevance_002/phase002_2-way.json'# % user_id
            
            has_f = False
            try:
                with open(fn) as f:
                    d = f.read()
                has_f = True
            except:
                pass
            
            if not has_f:
                print ('RECONCILE_MODE_NO')
                                
            else:
                print ('RECONCILE_MODE_YES')
                
                hh = json.loads(d)

                
                if user_id not in hh:
                    print ('USER_NOT_KNOWN', user_id, hh.keys())

                else:
                    
                    print ('USER_IS_KNOWN', user_id)
                    
                    has_any = False
                    the_task = False
                    
                    for queryk, rating_type_results_set in hh[user_id].iteritems():

                        xitems = rating_type_results_set.items()
                        shuffle(xitems)

                        print ('PENDING_TASKS',len(xitems))
                        
                        for rating_type, results_set in xitems:
                            
                            the_task = {'queryk':queryk, 'rating_type':rating_type, 'user_id':user_id}
                            
                            set_key = results_set['reconcile_info']['set_key']
                            
                            fn_3 = '/datasets/datasets/annotate/search_relevance_004/phase004_done_%s.json' % set_key
                            
                            if not exists(fn_3):
                                has_any = True
                                break
                        
                        if has_any:
                            break
                    
                    if has_any:

                        the_query = json.loads(queryk)['q']

                        #the_query = set_key
                        
                        print ('FOUND_RECONCILE_TASK', set_key, the_query)
                                                
                        if intget(self.get_argument('as_url', 0)):

                            #args = 'reconcile_task=' + quote(json.dumps(the_task))
                            
                            self.redirect('http://images.mediachainlabs.com/search/' + quote(the_query) \
                                           + '?debug=1&rerank_eq=' + json.dumps(the_task) + '|RECONCILE_TASK',
                                          permanent = False,
                                          )

                        else:

                            self.write_json({'q':the_query,
                                             'reconcile_task':json.dumps(the_task),
                                             })

                        return
                    
        
        if not exists(mc_config.MC_TYPEAHEAD_TSV_PATH):
            #self.set_status(500)
            self.write_json({'error':'TSV_FILE_NOT_FOUND',
                             'error_message':'Please correct MC_TYPEAHEAD_TSV_PATH: ' + repr(mc_config.MC_TYPEAHEAD_TSV_PATH),
                             })
            return
        
        from urllib import quote

        bb = False
        
        if False:
            q = choice(['technology', 'design', 'social media', 'privacy', 'bitcoin', 'internet of things', 'self driving cars', 'movies', 'television', 'music', 'gaming', 'politics', 'government', '2016 election', 'business', 'finance', 'economics', 'investing', 'creativity', 'ideas', 'humor', 'future', 'inspiration', 'travel', 'photography', 'architecture', 'art', 'climate change', 'transportation', 'sustainability', 'energy', 'health', 'mental health', 'psychology', 'science', 'education', 'history', 'space', 'virtual reality', 'artificial intelligence', 'feminism', 'women in tech', 'sports', 'nba', 'nfl', 'life lessons', 'productivity', 'self improvement', 'parenting', 'advice', 'startup', 'venture capital', 'entrepreneurship', 'leadership', 'culture', 'fashion', 'life', 'reading', 'relationships', 'this happened to me', 'diversity', 'racism', 'lgbtq', 'blacklivesmatter', 'fiction', 'books', 'poetry', 'satire', 'short story', 'food', 'future of food', 'cooking', 'writing', 'innovation', 'journalism'])

        else:
            if intget(self.get_argument('only_mwe', 1)):
                print ('YES_MWE')
                aa, bb = self.rand_typeahead_mwe, self.rand_typeahead_mwe_total
            else:
                print ('NO_MWE')
                aa, bb = self.rand_typeahead, self.rand_typeahead_total

            if False:#choice([0, 0, 0, 0, 0, 1]):
                ## Sometimes weighted:
                q = weighted_choice(aa, bb)
            else:
                ## Sometimes totally random:
                q = choice(aa)[1]
        
        print ('random_query', q, bb)
        
        if intget(self.get_argument('as_url', 0)):
            ## TODO: temporary for convenience:
            self.redirect('http://images.mediachainlabs.com/search/' + quote(q) + '&q=' + quote(q) + '?rerank_eq=' + choice(['neural_relevance','tfidf','neural_hybrid','aesthetics']), permanent = False)
        else:
            self.write_json({'q':q,
                             #'rerank_eq':choice(['neural_relevance','tfidf','neural_hybrid','aesthetics']),
                             })
        
        
from uuid import uuid4

def annotation_create_phase_2_tasks(via_cli = False):
    pass

class handle_record_relevance(BaseHandler):
    
    #disable XSRF checking for this URL:
    def check_xsrf_cookie(self): 
        pass
    
    @tornado.gen.coroutine
    def post(self):
        """        
        Record relevance feedback. 
        
        Input - JSON POST body containing the following fields:
            user_id:        Random hex string used as user ID.
            query_info:     Copy from  the `query_info` key returned by the `/search` endpoint.
            data:           Annotation data dict.
            nonce:          Random nonce used to prevent accidential double-submits. Only hex characters allowed.
        
        Example:
            $ curl "http://127.0.0.1:23456/record_relevance" -d '{"user_id":"test", "data":{}, "query_info":{}}'
        """
        
        ## TODO: Switch to real auth system:
                
        dir_out = join(mc_config.MC_ANNOTATE_DIR,
                       'search_relevance',
                       )
        
        if not exists(dir_out):
            makedirs(dir_out)
            
        d = self.request.body
        hh = json.loads(d)
        
        diff = set(['query_info', 'data']).difference(hh.keys())
        
        if diff:
            #self.set_status(500)
            self.write_json({'error':'MISSING_ARGS',
                             'error_message':repr(list(diff)),
                             })
            return
        
        print ('RECORD_RELEVANCE', hh)

        #assert hh['query_info']['query_args'].get('q'), ('NO_QUERY?',hh['query_info']['query_args'].keys())
        
        user_id = hh.get('user_id') or self.get_cookie('user_id','')
        
        if not user_id:
            hh['user_id'] = uuid4().hex
            self.set_cookie('user_id', hh['user_id'])
        
        user_id = hh['user_id']
        diff = set(user_id).difference('1234567890abcdef-')
        if diff and (user_id != 'test'):
            #self.set_status(500)
            self.write_json({'error':'INVALID_USER_ID',
                             'message':'user_id string must only contain hex characters (0-9 a-f). Got: ' + repr(user_id),
                             })
            return

        nonce = hh.get('nonce') or uuid4().hex
        diff = set(nonce).difference('1234567890abcdef-')
        if diff and (nonce != 'test'):
            #self.set_status(500)
            self.write_json({'error':'INVALID_NONCE',
                             'message':'nonce string must only contain hex characters (0-9 a-f). Got: ' + repr(nonce),
                             })
            return

        fn_out = join(dir_out,
                      user_id + '_' + nonce + '.json',
                      )

        if exists(fn_out):
            #self.set_status(500)
            self.write_json({'error':'NONCE_USED',
                             'message':'Already recorded a response under provided nonce. Accidental double submit?',
                             })
            return

        
        rh = {'created':int(time()),
              'user_id':user_id,
              'data':hh,
              'nonce':nonce,
              'user_ip':self.request.headers.get('X-Real-Ip'),
              'headers':dict(self.request.headers),
              }
                
        with open(fn_out, 'w') as f:
            f.write(json.dumps(rh))
        
        print ('WROTE', fn_out, rh)
        
        self.write_json({'success':True,
                         'fn':fn_out,
                         'message':'Success.'
                         })


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
        http_server.start(16) # Forks multiple sub-processes
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

