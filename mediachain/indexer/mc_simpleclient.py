#!/usr/bin/env python

"""
Simple blockchain client interface.
"""


import mc_config

def consistent_json_dumper(h):
    """
    Consistent JSON dumps, for comparing dumps by hash.
    """
    import json
    return json.dumps(h, separators=(',', ':'), sort_keys=True)


def consistent_json_hash(h):
    """
    Consistent JSON hash.
    """
    import hashlib
    return hashlib.md5(consistent_json_dumper(h)).hexdigest()


class SimpleClient(object):
    """
    Simple blockchain client interface.
    """
    
    def __init__(self,
                 datastore_host = mc_config.MC_DATASTORE_HOST,
                 datastore_port = mc_config.MC_DATASTORE_PORT_INT,
                 transactor_host = mc_config.MC_TRANSACTOR_HOST,
                 transactor_port = mc_config.MC_TRANSACTOR_PORT_INT,
                 use_ipfs = mc_config.MC_USE_IPFS_INT,
                 ):
        """
        """
        
        if not datastore_host:
            datastore_host = transactor_host
        
        print ('SimpleClient',
               'datastore_host:',datastore_host,'datastore_port:',datastore_port,
               'transactor_host:',transactor_host,'transactor_port:',transactor_port,
               'use_ipfs:',use_ipfs,
               )
        
        assert datastore_host,('datastore_host',datastore_host,)
        assert datastore_port,('datastore_port',datastore_port,)
        assert transactor_host,('transactor_host',transactor_host,)
        assert transactor_port,('transactor_port',transactor_port,)
        
        from mediachain.transactor.client import TransactorClient
        import mediachain.datastore.rpc
        from mediachain.datastore.rpc import set_rpc_datastore_config
        from mediachain.datastore import set_use_ipfs_for_raw_data

        self.datastore_host = datastore_host
        self.datastore_port = datastore_port
        self.transactor_host = transactor_host
        self.transactor_port = transactor_port
        self.use_ipfs = use_ipfs
        
        set_use_ipfs_for_raw_data(self.use_ipfs and True)
        
        set_rpc_datastore_config({'host': self.datastore_host,
                                  'port': self.datastore_port,
                                  })
        
        self.transactor = TransactorClient(self.transactor_host,
                                           self.transactor_port,
                                           )

    
    def _simple_translator(self,
                           the_iter,
                           ):
        """
        Magic.
        """
        
        import copy
        
        def inner():
            for hh in the_iter:
                rh = {}
                rh['raw_content'] = consistent_json_dumper(hh)
                rh['parsed'] = hh
                rh['local_assets'] = {'thumbnail': {'__mediachain_asset__': True,
                                                    'uri': None, ## TODO file URL
                                                    },
                                      }

                hh = copy.deepcopy(hh)

                hh['thumbnail'] = {'__mediachain_asset__': True,
                                   'uri': hh.get('img_data'),
                                   }

                rh['translated'] = {'chain': [{'__mediachain_object__': True,
                                               'type': 'artefactCreatedBy',
                                               'meta': {},
                                               'entity': {'__mediachain_object__': True,
                                                          'type': 'entity',
                                                          'meta': {'data': {'name': hh['artist']}}
                                                          }
                                               },
                                              ],
                                    'canonical': {'__mediachain_object__': True,
                                                  'type': 'artefact',
                                                  'meta': {'data': hh}
                                                  },
                                    }

                yield rh
        
        gen = inner()

        class C2(object):
            class C1:
                def translator_id(self):
                    return 'SimpleTranslator1.0'
            translator = C1()
            def __iter__(self):
                return self
            def next(self):
                return gen.next()
                
        return C2()

    
    def write_artefacts(self,
                        the_iter,
                        ):
        """
        Write artefacts to blockchain.

        Args:
            the_iter:   Iterates dicts containing the following keys at the top level:
                        - '_id'
                        - 'artist'
                        - 'img_data'
        """
        from mediachain.writer import Writer
        
        iterator = self._simple_translator(the_iter)
        
        writer = Writer(self.transactor,
                        download_remote_assets = False,
                        )
        
        writer.write_dataset(iterator)

    
    def read_artefacts(self,
                       timeout = 600,
                       force_exit = True,
                       ):
        """
        Read artefacts from blockchain.
        """
        
        from grpc.framework.interfaces.face.face import ExpirationError, AbortionError, CancellationError, ExpirationError, \
            LocalShutdownError, NetworkError, RemoteShutdownError, RemoteError
        
        grpc_errors = (AbortionError, CancellationError, ExpirationError, LocalShutdownError, \
                       NetworkError, RemoteShutdownError, RemoteError)
        
        print ('STREAMING FROM TRANSACTOR...', self.transactor_host, self.transactor_port)
        
        try:

            for art in self.transactor.canonical_stream(timeout = timeout):
                yield art
        
        except grpc_errors as e:
            print ('!!!CAUGHT gRPC ERROR',e)            
            from time import sleep
            import traceback, sys, os
            for line in traceback.format_exception(*sys.exc_info()):
                print line,
            os._exit(-1)
        
        except BaseException as e:
            print ('!!!CAUGHT OTHER ERROR',e)

            import traceback, sys, os
            for line in traceback.format_exception(*sys.exc_info()):
                print line,
            os._exit(-1)
        
        if force_exit:
            ## Force exit due to grpc bug:
            print ('FORCE_EXIT_ON_COMPLETION',)
            sleep(1)
            os._exit(-1)


def test():
    """
    Demo.
    """
    
    from uuid import uuid4
    
    cur = SimpleClient()
    
    cur.write_artefacts([{'_id':'test_' + uuid4().hex,
                          'artist':'Test Artist',
                          'img_data':'data:image/png;base64,iVBORw0KGgoAAAAN'\
                                     'SUhEUgAAAAUAAAAFCAYAAACNbyblAAAAHElEQV'\
                                     'QI12P4//8/w38GIAXDIBKE0DHxgljNBAAO9TXL'\
                                     '0Y4OHwAAAABJRU5ErkJggg==',
                          'description':'Test Description',
                          }])
    print 'WROTE'


