#!/usr/bin/env python
# -*- coding: utf-8 -*-

__doc__ = \
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

from os import mkdir,rename,unlink,listdir
from os.path import exists,join,split,realpath,splitext,dirname

from mc_generic import setup_main, pretty_print
import mc_dedupe
import mc_config


class Application(tornado.web.Application):
    def __init__(self,
                 ):
        
        handlers = [(r'/',handle_front,),
                    (r'/ping',handle_ping,),
                    (r'/search',handle_search,),
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
        
        self.INDEX_NAME = mc_config.INDEX_NAME
        self.DOC_TYPE = mc_config.DOC_TYPE


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
                   ):
        """
        Central point where we can customize the JSON output.
        """
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps(hh,
                              #sort_keys = True,
                              #indent = 4,
                              ) + '\n')
        self.finish()

    def write_json_pretty(self,
                          hh,
                          indent = 4,
                          max_indent_depth = False,
                          ):
        
        self.set_header("Content-Type", "text/plain")
        self.write(pretty_print(hh,
                                indent = indent,
                                max_indent_depth = max_indent_depth,
                                ).replace('\n','\r\n'))
        self.finish()


class handle_front(BaseHandler):
    
    @tornado.gen.coroutine
    def get(self):
        
        #TODO - 
        
        self.write('FRONT_PAGE')
        self.finish()


class handle_ping(BaseHandler):
    
    @tornado.gen.coroutine
    def post(self):
        self.write("{'results':['pong']}")
        self.finish()


class handle_search(BaseHandler):
    
    #disable XSRF checking for this URL:
    def check_xsrf_cookie(self): 
        pass
    
    @tornado.gen.coroutine
    def post(self):
        """
        Search for images based on text query, a media work, or a combination of both.
        
        Args, as JSON-encoded POST body:
           q:             Query text.
           q_id:          Query media. See `Media Identifiers`.
           
           limit:         Maximum number of results to return.
           inline_images: Whether to include base64-encoded thumbnails of images directly in the results.

        Returns:
            List of image IDs, possibly with relevancy scores.
        
        Example:
           {'q_text':'girl holding a balloon', 'limit':5}  
        
        Example response:
           {'data': [{'addr':'ifps://1234...',
                      'title':'An Image',
                     },
                    ],
            'next_page': null,
           }
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

        q_text = data.get('q','') or self.get_argument('q','')
        
        limit = intget(data.get('q') or self.get_argument('limit',False), 10)
        
        if not q_text:
            self.set_status(500)
            self.write_json({'error':'MISSING_QUERY',
                             'error_message':'`q` is required.',
                             })
            return
        
        query = {"query": {"multi_match": {"query":    q_text,
                                           "fields": [ "*" ],
                                           "type":     "cross_fields"
                                           },
                           },
                 'from':0,
                 'size':limit,
                 }
        
        #query = {"query": {'match_all': {}},'from':0,'size':1,}
        
        rr = yield self.es.search(index = mc_config.INDEX_NAME,
                                  type = mc_config.DOC_TYPE,
                                  source = query,
                                  )
    
        hh = json.loads(rr.body)
        
        print pretty_print(hh)
        
        rr = {'results':hh['hits']['hits'],
              'next_page':None,
              'prev_page':None,
              }
        
        self.write_json_pretty(rr)

        

class handle_dupe_lookup(BaseHandler):
    
    #disable XSRF checking for this URL:
    def check_xsrf_cookie(self): 
        pass
    
    @tornado.gen.coroutine
    def get(self):
        """
        Find all known duplicates of a media work.
        
        Args:
            q_media:         Media file to query for..
            duplicate_mode:  Semantic duplicate type or matching mode. For now, defaults to 'baseline'.
             
            incremental:     Attempt to dedupe never-before-seen media file versus all pre-ingested media files.
                             NOTE: potentially inefficient. More efficient to pre-calculate for all known images in
                             background.

        Returns: 
             See `mc_dedupe.dedupe_lookup_async`.            
        """

        if False:
            d = self.request.body

            try:
                data = json.loads(d)
            except:
                self.set_status(500)
                self.write_json({'error':'JSON_PARSE_ERROR',
                                 'error_message':'Could not parse JSON request.',
                                })
                return

        data = {'q_media':'getty_531746790'}
        
        rr = yield mc_dedupe.dedupe_lookup_async(media_id = data['q_media'],
                                                 duplicate_mode = 'baseline',
                                                 es = self.es,
                                                 )
        
        self.write_json({'results':rr,
                         'next_page':None,
                         'prev_page':None,
                         })


class handle_score(BaseHandler):
    
    #disable XSRF checking for this URL:
    def check_xsrf_cookie(self): 
        pass
    
    @tornado.gen.coroutine
    def post(self):
        """
        Admin tool for peering deeper into the similarity / relevance measurements that are the basis of
        dedupe / search calculations. Useful for e.g. getting a feel for why an image didn't show up in
        the top 100 results for a query, or why a pair of images weren't marked as duplicates.
        
        Takes a "query" and list of "candidate" media, and does 1-vs-all score calculations for
        all "candidate" media versus the "query".
        
        Args, as JSON-encoded POST body:
            q_text:    Query text.
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
        
        Returns:
            List of similarities or duplicate probabilities, one per similarity or duplicate type.

        Examples:
            in:        {'mode':'search', 'level':'distance', 'q_text':'girl with baloon', 'c_ids':['ifps://123...']}
            out:       {'results':[{'id':'ifps://123...', 'score':0.044}]}

            in:        {'mode':'dupe', 'level':'score', 'q_id':'ifps://123...', 'c_ids':['ifps://123...']}
            out:       {'results':[{'id':'ifps://123...', 'score':0.321}]}

        """
        
        q_text = self.get_argument('q_text','')
        
        query = {"query": {"match_all": {}}}
        
        rr = yield self.es.search(index = mc_config.INDEX_NAME,
                                  type = mc_config.DOC_TYPE,
                                  source = query,
                                  )
        
        self.content_type = 'application/json'
        self.write_json(json.loads(rr.body))



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
               )

if __name__ == '__main__':
    main()

