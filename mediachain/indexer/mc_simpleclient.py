#!/usr/bin/env python


"""
Simple blockchain client interface.

Beware of bugs related to unicode_literals.
"""


import mc_config
import mc_ingest

from mc_generic import setup_main

from os import mkdir, rename, unlink, listdir
from os.path import exists, join, split, lexists

from tempfile import NamedTemporaryFile

from mediachain.translation.translator import Translator
from mediachain.ingestion.dataset_iterator import DatasetIterator
from mediachain.transactor.client import TransactorClient
from mediachain.writer import Writer
import mediachain.datastore.rpc
from mediachain.datastore.rpc import set_rpc_datastore_config
from mediachain.datastore import set_use_ipfs_for_raw_data
from mediachain.datastore import set_use_ipfs_for_raw_data
from mediachain.datastore.ipfs import set_ipfs_config
from mediachain.datastore.rpc import set_rpc_datastore_config, close_db



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


def force_all_keys_unicode(h):
    ## TODO - prevent future unicode_literals bugs
    pass


class SimpleTranslator(Translator):
    
    @staticmethod
    def translator_id():
        return 'SimpleTranslator/0.1'
    
    @staticmethod
    def translate(parsed_metadata):
        getty_json = parsed_metadata

        # extract artist Entity
        artist_name = getty_json['artist']
        
        artist_entity = {
            u'__mediachain_object__': True,
            u'type': u'entity',
            u'meta': {
                u'data': {
                    u'name': artist_name
                }
            }
        }

        # extract artwork Artefact

        if True:
            
            ## TODO
            
            data = getty_json
            
            
        else: 
            
            data = {u'_id': u'getty_' + getty_json['id'],
                    u'title': getty_json['title'],
                    u'artist': getty_json['artist'],
                    u'collection_name': getty_json['collection_name'],
                    u'caption': getty_json['caption'],
                    u'editorial_source':
                        getty_json['editorial_source'].get('name', None),
                    u'keywords':
                        [x['text'] for x in getty_json['keywords'] if 'text' in x],
                    u'date_created': getty_json['date_created']
                    }

        try:
            thumb_uri = [i['uri'] for i in parsed_metadata['display_sizes']
                         if i['name'] == 'thumb'].pop()
            data[u'thumbnail'] = {
                u'__mediachain_asset__': True,
                u'uri': thumb_uri
            }
        except:
            pass
        
        artwork_artefact = {
            u'__mediachain_object__': True,
            u'type': u'artefact',
            u'meta': {'data': data}
        }

        return {
            u'canonical': artwork_artefact,
            u'chain': [
                {u'__mediachain_object__': True,
                 u'type': u'artefactCreatedBy',
                 u'meta': {},
                 u'entity': artist_entity
                 }
            ]
        }

    @staticmethod
    def can_translate_file(file_path):
        return True




class SimpleIterator(DatasetIterator):
    
    def __init__(self, the_iter, translator,):
        
        super(SimpleIterator, self).__init__(translator)
        
        self.the_iter = the_iter

    def __iter__(self):
        nn = 0
        
        for hh in self.the_iter:

            try:
                
                with NamedTemporaryFile(delete = True,
                                        prefix = 'simpleclient_',
                                        suffix = '.jpg'
                                        ) as ft:
                        
                    uri_temp = 'file://' + ft.name

                    if 'img_data' in hh:
                        ft.write(mc_ingest.decode_image(hh['img_data']))
                        ft.flush()
                        del hh['img_data']
                    else:
                        assert False,('NO_IMG_DATA',)
                    
                    parsed = hh['source_record']
                    
                    translated = self.translator.translate(parsed)
                    
                    local_assets = {'thumbnail': {'__mediachain_asset__': True,
                                                  'uri': uri_temp
                                                  }
                                    }
                    
                    yield {'translated': translated,
                           'raw_content': consistent_json_dumper(parsed),
                           'parsed': parsed,
                           'local_assets': local_assets
                           }
            
            finally:
                                
                try:
                    unlink(ft.name)
                except:
                    pass
            
            nn += 1

    

class SimpleClient(object):
    """
    Simple blockchain client interface.
    """
    
    def __init__(self,
                 ## Hard Code for now:
                 datastore_host = '107.23.23.184',
                 datastore_port = 10002,
                 transactor_host = '107.23.23.184',
                 transactor_port = 10001,
                 use_ipfs = False,
                 #datastore_host = mc_config.MC_DATASTORE_HOST,
                 #datastore_port = mc_config.MC_DATASTORE_PORT_INT,
                 #transactor_host = mc_config.MC_TRANSACTOR_HOST,
                 #transactor_port = mc_config.MC_TRANSACTOR_PORT_INT,
                 #use_ipfs = mc_config.MC_USE_IPFS_INT,
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
                
        self.datastore_host = datastore_host
        self.datastore_port = datastore_port
        self.transactor_host = transactor_host
        self.transactor_port = transactor_port
        self.use_ipfs = use_ipfs
        
        self.transactor = TransactorClient(self.transactor_host,
                                           self.transactor_port,
                                           )
        
        set_rpc_datastore_config({'host': datastore_host, 'port': datastore_port})
        set_ipfs_config({'host': 'http://localhost:8000', 'port': 5001})
        set_use_ipfs_for_raw_data(True)
        
    
    def write_artefacts(self,
                        the_iter,
                        ):
        """
        Write artefacts to blockchain.

        Args:
            the_iter:   Iterates dicts containing, at least, the following keys at the top level:
                        - '_id'
                        - 'img_data'
        """
        
        iterator = SimpleIterator(the_iter,
                                  SimpleTranslator,
                                  )
        
        writer = Writer(self.transactor,
                        download_remote_assets=False,
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
            raise
            #print ('!!!CAUGHT OTHER ERROR',e)

            #import traceback, sys, os
            #for line in traceback.format_exception(*sys.exc_info()):
            #    print line,
            #os._exit(-1)
        
        if force_exit:
            ## Force exit due to grpc bug:
            print ('FORCE_EXIT_ON_COMPLETION',)
            sleep(1)
            os._exit(-1)


def test_blockchain(via_cli = False):
    """
    Simple round-trip test.
    """
    
    from uuid import uuid4
    
    cur = SimpleClient()

    test_id = 'test_' + uuid4().hex
    
    cur.write_artefacts([{'source_record':{'_id':test_id,
                                           'artist':'Test Artist',
                                           'description':'Test Description',
                                           },
                          'img_data':'data:image/png;base64,iVBORw0KGgoAAAAN'\
                                     'SUhEUgAAAAUAAAAFCAYAAACNbyblAAAAHElEQV'\
                                     'QI12P4//8/w38GIAXDIBKE0DHxgljNBAAO9TXL'\
                                     '0Y4OHwAAAABJRU5ErkJggg==',
                                           
                          }])

    for art in cur.read_artefacts():
        print 'ART',
        try:
            xid = art['meta']['data']['_id']
        except:
            continue

        if xid == test_id:
            print ('SUCCESS',test_id)

    print ('EXIT')


functions=['test_blockchain']

def main():
    setup_main(functions,
               globals(),
                'mediachain-indexer-simpleclient',
               )

if __name__ == '__main__':
    main()

