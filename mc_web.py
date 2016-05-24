#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Prototype REST Indexer interface for search / dedupe.
"""

import json
import tornado.ioloop
import tornado.web
from time import time
from tornadoes import ESConnection
from mc_generic import setup_main


class Application(tornado.web.Application):
    def __init__(self,
                 ):
        
        handlers = [(r'/',handle_front,),
                    (r'/search',handle_search,),
                    (r'/distance',handle_distance,),
                    (r'/ping',handle_ping,),
                    (r'/(favicon.ico)', tornado.web.RedirectHandler,{'url':'/static/favicon.png?v=1','permanent':True}),
                    (r'/robots.txt', tornado.web.RedirectHandler,{'url':'/static/robots.txt','permanent':True}),
                    (r'.*', handle_notfound,),
                    ]
        
        settings = {'template_path':join(dirname(__file__), 'templates_mc'),
                    'static_path':join(dirname(__file__), 'static_mc'),
                    'xsrf_cookies':False,
                    }
        tornado.web.Application.__init__(self, handlers, **settings)
        

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
        t=Template(template_s)
        r=t.generate(**kwargs)
        self.write(r)
        self.finish()
        
    def write_json(self,
                   hh,
                   ):
        self.set_header("Content-Type", "application/json")
        self.write(json.dumps(hh,
                              #sort_keys = True,
                              #indent = 4,
                              ) + '\n')
        self.finish()


INDEX_NAME = 'getty_test'
DOC_TYPE = 'image'

class handle_search(BaseHandler):
    
    #disable XSRF checking for this URL:
    def check_xsrf_cookie(self): 
        pass
    
    @tornado.gen.coroutine
    def post(self):
        """Search for images based on text query, media content, or a combination of both.
           Parameters are encoded as JSON and sent as body of POST request.
           
           JSON-encoded      body arguments:
              q_text:        text query for text-based query.
              q_id:          IFPS media work address for content-based search.
              q_base64:      base64-encoded media object.
              q_facets:      TODO - dictionary for querying against specific fields.
              limit:         maximum number of results to return.
              inline_images: whether to include base64-encoded thumbnails of images directly in the results.
           
           Returns on success:
              data:          list of found media objects.
              next_page:     pagination link.

           Returns on error:
              error:         error code.
              error_message: error message.

           Example request body:
              {'q_text':'girl holding a balloon', 'limit':5}  

           Example response:
              {'data': [{'addr':'ifps://1234...',
                         'title':'An Image'},
                       ],
               'next_page': null,
              }
        """
        
        d = self.request.body
        
        try:
            data = json.loads(d)
        except:
            self.set_status('503')
            self.write_json({'error':'PARSE_ERROR',
                             'error_message':'Could not parse JSON request.',
                            })
            return

        q_text = data.get('q_text','')
        
        if not q_text:
            self.set_status('503')
            self.write_json({'error':'MISSING_QUERY',
                             'error_message':'`q_text` is only query type supported for now.',
                             })
            return
        
        query = {"query": {"multi_match": {"query":    q_text,
                                           "fields": [ "t_*" ],
                                           "type":     "cross_fields"
                                           },
                           },
                 'from':0,
                 'size':10,
                 }
        
        response = yield self.es.search(index = INDEX_NAME,
                                        type = DOC_TYPE,
                                        source = query,
                                        )

        hh = json.loads(response.body)
        
        self.write_json(hh['hits']['hits'])
        

class handle_distance(BaseHandler):
    
    #disable XSRF checking for this URL:
    def check_xsrf_cookie(self): 
        pass
    
    @tornado.gen.coroutine
    def post(self):
        """Metric distance between all pairs of media objects, in the embedded space.
        
           JSON-Encoded POST Body:
               q_ids:     comma-separated list of IFPS media content addresses.
               q_base64s: comman-separated list of base64-encoded media objects.
        """
        
        q_text = self.get_argument('q_text','')
        
        query = {"query": {"match_all": {}}}
        
        response = yield self.es.search(index = INDEX_NAME,
                                        type = DOC_TYPE,
                                        source = query,
                                        )
        
        self.content_type = 'application/json'
        self.write(json.loads(response.body))
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

