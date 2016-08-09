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
    
    @property
    def rand_typeahead(self):
        if not hasattr(self.application,'rand_typeahead'):
            
            total = 0
            rr = []

            total_mwe = 0
            rr_mwe = []
            
            with open(mc_config.MC_TYPEAHEAD_TSV_PATH) as f:
                for c, line in enumerate(f):
                    if c >= 10000:
                        break
                    
                    if not line:
                        break
                    
                    score, query, _ = line.split('\t')
                    score = int(score)

                    if c < 1000:
                        rr.append((score, query))
                        total += score

                    if ' ' in query:
                        rr_mwe.append((score, query))
                        total_mwe += score
                    
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
                   sort_keys = True,
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
    from mc_crawlers import get_remote_search, get_enriched_tags
except KeyboardInterrupt:
    raise
except:
    get_remote_search = False
    get_enriched_tags = False


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
        except KeyboardInterrupt:
            raise
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
        except KeyboardInterrupt:
            raise
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

source_urls = ['gettyimages.com',
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
           for x in source_urls
           }

default_options = [{'name':'q',
                  'description':'Query by text.',
                  'default':None,
                  'type':'text',
                  'options':None,
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
                   ]

debug_options = [{'name':'include_docs',
                  'description':'Return entire indexed docs, instead of just IDs.',
                  'default':1,
                  'type':'number',
                  'options':['1', '0'],
                  },
                 {'name':'pretty',
                  'description':'Indent and pretty-print JSON output.',
                  'default':1,
                  'type':'number',
                  'options':['1','0'],
                  },
                 {'name':'filter_incomplete',
                  'description':"Filter documents for which all features haven't been generated / ingested yet.",
                  'default':0,
                  'type':'number',
                  'options':['0','1'],
                  },
                 {'name':'allow_nsfw',
                  'description':'Include adult images.',
                  'default':0,
                  'type':'number',
                  'options':['0','1'],
                  },
                 {'name':'skip_query_cache',
                  'description':'Bypass the query cache.',
                  'default':0,
                  'type':'number',
                  'options':['0','1']
                  },
                 {'name':'schema_variant',
                  'description':'Select schema variant postprocessing version.',
                  'default':'new',
                  'type':'text',
                  'options':['new','old'],
                  },
                 {'name':'rerank_eq',
                  'description':'Name of reranking equation, or custom reranking equation string.',
                  'default':'aesthetics',
                  'type':'text',
                  'options':['aesthetics', 'tfidf', 'boost_pexels',],
                 },
                 {'name':'enrich_tags',
                  'description':'Use external API for tag enrichment on individual image pages.',
                  'default':1,
                  'type':'number',
                  'options':['1','0'],
                  },
                 {'name':'filter_licenses',
                  'description':'List of zero-or-more allowable licenses. Select nothing or use "ALL" to allow all licenses.',
                  'default':['Creative Commons'],
                  'type':'list-of-strings',
                  'options':['Creative Commons','ALL'],
                 },
                 {'name':'filter_sources',
                  'description':'List of zero-or-more allowable sources. Select nothing or use "ALL" to allow all licenses.',
                  'default':[],
                  'type':['list-of-strings'],
                  'options':source_urls + ['ALL'],
                 },
                 {'name':'token',
                  'description':'Token ID used to refer to previous search sessions.',
                  'default':[],
                  'type':'text',
                  'options':None,
                 },
                 ]

class handle_search(BaseHandler):
    
    #disable XSRF checking for this URL:
    def check_xsrf_cookie(self): 
        pass
    
    @tornado.gen.coroutine
    def post(self):
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
        
        d = self.request.body

        if d.startswith('{'):
            print ('OLD PARAMETERS FORMAT',d[:20])
            
        else:
            print ('NEW PARAMETERS FORMAT',)
            d = self.get_argument('json','{}')            
                
        try:
            data = json.loads(d)
        except KeyboardInterrupt:
            raise
        except:
            self.set_status(500)
            self.write_json({'error':'JSON_PARSE_ERROR',
                             'error_message':'Could not parse JSON request.',
                            })
            return

        
        is_debug_mode = intget(self.get_cookie('debug')) or intget(data.get('debug'))
                
        print ('DEBUG_MODE?', is_debug_mode, dict(self.request.cookies))
        

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

        diff = set(data).difference([x['name'] for x in default_options + debug_options])
        if diff:
            self.set_status(500)
            self.write_json({'error':'UNKNOWN_ARGS',
                             'error_message':repr(list(diff)),
                            })
            return

        
        ## New method:

        the_input = {}
        for arg in default_options + debug_options:

            if arg['type'] == 'number':
                the_input[arg['name']] = intget(data.get(arg['name'], 'BAD'), arg['default'])
            else:
                the_input[arg['name']] = data.get(arg['name'], arg['default'])

            ## For convenience:
            
            if arg['type'] == 'list-of-strings':
                if isinstance(the_input[arg['name']], basestring):
                    the_input[arg['name']] = [the_input[arg['name']]]
                    

        the_input['q_text'] = the_input['q'] ## for reverse compatibility.

        
        ## Disallow user modification of these, for now:
        
        the_input['index_name'] = data.get('index_name', mc_config.MC_INDEX_NAME)
        the_input['doc_type'] = data.get('doc_type', mc_config.MC_DOC_TYPE)
        the_input['include_thumb'] = False
        the_input['full_limit'] = 300
        
        
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
        
        if q_id_file and the_input['q_id']:
            self.set_status(500)
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
                print ('REMOTE_IDS','time:',time()-t1,len(remote_ids))

        
        ## Cache & token lookup:
        
        input_token = data.get('token', None)

        the_token = input_token
        
        rr = False

        
        if the_token:
            rr = query_cache_lookup(the_token, skip_query_cache = the_input['skip_query_cache'])

            if rr is False:
                self.set_status(500)
                self.write_json({'error':'EXPIRED_TOKEN',
                                 'error_message':the_token,
                                 })
                return                

        else:
            ## New method:
            
            query_args = the_input.copy()

            for kk in ['limit','offset',]:
                if kk in query_args:
                    del query_args[kk]
                
            print ('QUERY_ARGS',query_args)

            ## ignore those with default args:
            for k,v in query_args.items():
                if v is None:
                    del query_args[k]
            
            the_token = consistent_json_hash(query_args)
            
            rr = query_cache_lookup(the_token, skip_query_cache = the_input['skip_query_cache'])
        
        if rr is not False:
            print ('CACHE_OR_TOKEN_HIT_QUERY','offset:', the_input['offset'], 'limit:', the_input['limit'], 'len(results)',len(rr['results']))
            
            results_count = len(rr['results'])
                                                
            if the_input['offset'] + the_input['limit'] >= len(rr['results']):
                rr['next_page'] = None
            else:
                rr['next_page'] = {'token':the_token, 'offset':the_input['offset'] + the_input['limit'], 'limit':the_input['limit']}

            if the_input['offset'] == 0:
                rr['prev_page'] = None
            else:
                rr['prev_page'] = {'token':the_token, 'offset':max(0, the_input['offset'] - the_input['limit']), 'limit':the_input['limit']}

            rr['results'] = rr['results'][the_input['offset']:the_input['offset'] + the_input['limit']]

            rr['default_options'] = default_options
            
            if is_debug_mode:
                rr['debug_options'] = debug_options
            
            self.write_json(rr,
                            pretty = the_input['pretty'],
                            max_indent_depth = data.get('max_indent_depth', False),
                            )
            
            return

        is_id_search = False
        
        if not (the_input['q_text'] or the_input['q_id'] or q_id_file or the_input['canonical_id']):
            
            ## Match all mode, skip cache:
            
            rr = yield self.es.search(index = the_input['index_name'],
                                      type = the_input['doc_type'],
                                      source = {"query": {"match_all": {}}, "size":20}
                                      )

            
            rr = json.loads(rr.body)

            if 'error' in rr:
                self.set_status(500)
                self.write_json({'error':'ELASTICSEARCH_ERROR',
                                 'error_message':repr(rr)[:1000],
                                 })
                return

            
            rr = rr['hits']['hits']
            rr = {'results':rr,
                  'next_page':None,
                  'prev_page':None,
                  'results_count':('{:,}'.format(the_input['full_limit'])) + '+',
                  }
            
            rr['default_options'] = default_options
            
            if is_debug_mode:
                rr['debug_options'] = debug_options
            
            self.write_json(rr)
            return

        
        if (the_input['q_text'] and the_input['q_id']):
            self.set_status(500)
            self.write_json({'error':'BAD_QUERY',
                             'error_message':'Simultaneous `q` and `q_id` not yet implemented.',
                             })
            return

        
        elif the_input['q_id'] or q_id_file or the_input['canonical_id']:
            
            if the_input['canonical_id']:
                ## Search by canonical_id:
                
                print ('CANONICAL_ID_SEARCH', the_input['canonical_id'])
                
                query = {"query": {"constant_score": {"filter": {"term": {"canonical_id": the_input['canonical_id']}}}}}
            
            elif q_id_file or (the_input['q_id'].startswith(data_pat) or the_input['q_id'].startswith(data_pat_2)):
                
                #Resolve ID(s) for query based on content.
                #Note that this is similar to `/dupe_lookup` with `include_docs` = True:
                
                print ('CONTENT-BASED-SEARCH',)
                
                model = mc_models.VECTORS_MODEL_NAMES['baseline']()
                
                if (the_input['q_id'].startswith(data_pat) or the_input['q_id'].startswith(data_pat_2)):
                    print ('GOT_DATA_URI')
                    terms = model.img_to_terms(img_data_uri = the_input['q_id'])
                else:
                    print ('GOT_RAW_BYTES')
                    assert q_id_file
                    terms = model.img_to_terms(img_bytes = q_id_file)
                
                print ('TERMS',repr(terms)[:100])
                
                
                rr = yield self.es.search(index = the_input['index_name'],
                                          type = the_input['doc_type'],
                                          source = {"query": {"constant_score":{"filter":{"term": terms}}}},
                                          )
                print ('GOT_Q_ID_FILE_OR_Q_ID',repr(rr.body)[:100])
                
                rr = json.loads(rr.body)
                
                if 'error' in rr:
                    self.set_status(500)
                    self.write_json({'error':'ELASTICSEARCH_ERROR',
                                     'error_message':repr(rr)[:1000],
                                     })
                    return

                
                rr = [x['_id'] for x in rr['hits']['hits']]
                
                query = {"query":{ "ids": { "values": rr } } }
                
            else:
                #ID-based search:

                is_id_search = True
                
                print ('ID-BASED-SEARCH', the_input['q_id'])
                query = {"query":{ "ids": { "values": [ the_input['q_id'] ] } } }
        
        
        elif the_input['q_text']:

            #text-based search:
            query = {"query": {"multi_match": {"query":    the_input['q_text'],
                                               "fields": [ "*" ],
                                               "type":     "cross_fields"
                                               },
                               },
                     }

        remote_hits = []
        
        #assert remote_ids,remote_ids
        if remote_ids:
            t1 = time()
            rr = yield self.es.search(index = the_input['index_name'],
                                      type = the_input['doc_type'],
                                      source = {"query":{ "ids": { "values": remote_ids } } },
                                      )
        
            print ('GOT','time:',time() - t1,repr(rr.body)[:100])

            hh = False
            
            try:
                hh = json.loads(rr.body)
            except KeyboardInterrupt:
                raise
            except:
                self.set_status(500)
                self.write_json({'error':'ELASTICSEARCH_JSON_ERROR',
                                 'error_message':'Elasticsearch down or timeout? - ' + repr(hh)[:1000],
                                 })
                return

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
        
        query['from'] = the_input['offset']
        query['size'] = the_input['full_limit']
        
        print ('QUERY',query)

        t1 = time()
        rr = yield self.es.search(index = the_input['index_name'],
                                  type = the_input['doc_type'],
                                  source = query,
                                  )
        
        print ('GOT','time:',time() - t1, repr(rr.body)[:100])

        hh = False
        
        try:
            hh = json.loads(rr.body)
        except KeyboardInterrupt:
            raise
        except:
            self.set_status(500)
            self.write_json({'error':'ELASTICSEARCH_JSON_ERROR',
                             'error_message':'Elasticsearch down or timeout? - ' + repr(hh)[:1000],
                             })
            return
        
        if 'error' in hh:
            self.set_status(500)
            self.write_json({'error':'ELASTICSEARCH_ERROR',
                             'error_message':repr(hh)[:1000],
                             })
            return
        
        rr = hh['hits']['hits']

        
        ## NSFW filtering:
        
        if not the_input['allow_nsfw']:

            r2 = []
            for xx in rr:
                if not xx['_source'].get('nsfw'):
                    r2.append(xx)
            rr = r2

        
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

            if the_input['enrich_tags'] and is_id_search and get_enriched_tags:

                etags = []
                try:
                    etags = get_enriched_tags([urls['1024x1024']])[0]
                except KeyboardInterrupt:
                    raise
                except:
                    print ('ENRICH_TAGS_FAILED - API KEYS?',)

                if not ii['_source'].get('keywords'):
                    ii['_source']['keywords'] = []

                ii['_source']['keywords'].extend(etags)

            r2.append(ii)

            continue
        
        rr = r2

        
        ## Apply post-ingestion normalizers, if there are any:
                
        mc_normalize.apply_post_ingestion_normalizers(rr, schema_variant = the_input['schema_variant'])

        
        ## Re-rank:
        
        rrm = ReRankingBasic(eq_name = the_input['rerank_eq'])
        rr = rrm.rerank(rr)

        
        ## AES for debug:

        for ii in rr:

            if not ii['_source'].get('keywords'):
                ii['_source']['keywords'] = []

            sc = ii['_source'].get('aesthetics', {}).get('score', False)

            if sc is False:
                ii['_source']['keywords'].append('aes=False')
            else:
                ii['_source']['keywords'].append('aes=%.4f' % sc)
                            
            ii['_source']['keywords'].append('ni=' + ii['_source'].get('native_id', 'False'))

            ii['_source']['keywords'].append('width=' + str(ii['_source'].get('sizes') and ii['_source'].get('sizes')[0].get('width', False)))
            
            ii['_source']['keywords'].append('tfidf=%.4f' % ii['_old_score'])
            ii['_source']['keywords'].append('score=%.4f' % ii['_score'])
        
        
        ## Note: swapping these for ES queries shortly:
        
        if the_input['filter_licenses'] and ('ALL' not in the_input['filter_licenses']):
            
            filter_licenses_s = set(the_input['filter_licenses'])
            r2 = []
            for ii in rr:
                
                native_id = ii['_source'].get('native_id', '')
                
                ii['_source']['license_tags'] = ii['_source'].get('license_tags') or []
                
                assert type(ii['_source']['license_tags']) == list, ii['_source']['license_tags']
                
                ## The currently-ingested, except for these 2 datasets, should be all open-licensed:
                if ('getty_' not in native_id) and ('eyeem_' not in native_id):
                    ii['_source']['license_tags'].append('Creative Commons') 
                
                ## ANY match:
                if filter_licenses_s.intersection(ii['_source'].get('license_tags',[])):
                    r2.append(ii)
            
            print ('FILTER_LICENSES', the_input['filter_licenses'], len(rr),'->',len(r2))
            rr = r2
                    
        if False: #the_input['filter_sources']:
            filter_sources_s = set(the_input['filter_sources'])
            r2 = []
            for ii in rr:                
                if filter_sources_s.intersection(ii['_source'].get('source_tags',[])):
                    r2.append(ii)
            
            print ('FILTER_SOURCES', the_input['filter_sources'], len(rr),'->',len(r2))
            rr = r2
        
        
        ## Include or don't include full docs:
        
        if not the_input['include_docs']:
            
            rr = [{'_id':hit['_id']}
                  for hit
                  in rr
                  ]
        
        ## Cache:
        
        results_count = len(rr)
        
        rr = {'results':rr,
              'results_count':(results_count >= the_input['full_limit'] * 0.8) and \
                               (('{:,}'.format(the_input['full_limit'])) + '+') or \
                               ('{:,}'.format(results_count)),
              }
        
        query_cache_save(the_token, rr)
        
        ## Wrap in pagination:
        
        if the_input['offset'] + the_input['limit'] >= len(rr['results']):
            rr['next_page'] = None
        else:
            rr['next_page'] = {'token':the_token, 'offset':the_input['offset'] + the_input['limit'], 'limit':the_input['limit']}
        
        if the_input['offset'] == 0:
            rr['prev_page'] = None
        else:
            rr['prev_page'] = {'token':the_token, 'offset':max(0, offset - the_input['limit']), 'limit':the_input['limit']}
        
        ## Trim:
        
        rr['results'] = rr['results'][the_input['offset']:the_input['offset'] + the_input['limit']]
        
        ## Output:

        rr['default_options'] = default_options
        
        if is_debug_mode:
            rr['debug_options'] = debug_options
        
        self.write_json(rr,
                        pretty = the_input['pretty'],
                        max_indent_depth = data.get('max_indent_depth', False),
                        )


from random import uniform, randint, choice

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
        """
        
        if not exists(mc_config.MC_TYPEAHEAD_TSV_PATH):
            self.set_status(500)
            self.write_json({'error':'TSV_FILE_NOT_FOUND',
                             'error_message':'Please correct MC_TYPEAHEAD_TSV_PATH: ' + repr(mc_config.MC_TYPEAHEAD_TSV_PATH),
                             })
            return
        
        from urllib import quote

        if intget(self.get_argument('only_mwe', 0)):
            aa, bb = self.rand_typeahead_mwe, self.rand_typeahead_mwe_total
        else:
            aa, bb = self.rand_typeahead, self.rand_typeahead_total

        if choice([0, 0, 0, 0, 0, 1]):
            ## Sometimes weighted:
            q = weighted_choice(aa, bb)
        else:
            ## Sometimes totally random:
            q = choice(aa)[1]

        print ('random_query', q, bb)
        
        if intget(self.get_argument('as_url', 0)):
            ## TODO: temporary for convenience:
            self.redirect('http://images.mediachainlabs.com/search/' + quote(q), permanent = False)
        else:
            self.write_json({'query':q})
        
        
from uuid import uuid4

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
            query:          Query args dict, as passed to `/search` endpoint.
            data:           Annotation data dict.
            nonce:          Random nonce used to prevent accidential double-submits. Only hex characters allowed.
        
        Example:
            $ curl "http://127.0.0.1:23456/record_relevance" -d '{"user_id":"012345", "data":{}, "query":{}}'
        
        TODO:
        Million other features. Intentionally neglecting those features, because having anything here is far better
        than nothing. Goal is just to have a basic evaluation test and to record the results, instead of repeatedly
        doing it all mentally.
        
        """
        
        ## TODO: Switch to real auth system:
                
        dir_out = join(mc_config.MC_ANNOTATE_DIR,
                       'search_relevance',
                       )

        if not exists(dir_out):
            makedirs(dir_out)
            
        d = self.request.body
        hh = json.loads(d)

        diff = set(['user_id', 'query', 'data']).difference(hh.keys())
        
        if diff:
            self.set_status(500)
            self.write_json({'error':'MISSING_ARGS',
                             'error_message':repr(list(diff)),
                             })
            return
        
        print ('RECORD_RELEVANCE', hh)
        
        user_id = hh['user_id']
        diff = set(user_id).difference('1234567890abcdef')
        if diff:
            self.set_status(500)
            self.write_json({'error':'INVALID_USER_ID',
                             'error_message':'user_id string must only contain hex characters (0-9 a-f). Got: ' + repr(user_id),
                             })
            return

        nonce = hh.get('nonce') or uuid4().hex
        diff = set(nonce).difference('1234567890abcdef')
        if diff:
            self.set_status(500)
            self.write_json({'error':'INVALID_NONCE',
                             'error_message':'nonce string must only contain hex characters (0-9 a-f). Got: ' + repr(nonce),
                             })
            return

        fn_out = join(dir_out,
                      user_id + '_' + nonce + '.json',
                      )

        if exists(fn_out):
            self.set_status(500)
            self.write_json({'error':'NONCE_USED',
                             'error_message':'Already recorded a response under provided nonce. Accidental double submit?',
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

