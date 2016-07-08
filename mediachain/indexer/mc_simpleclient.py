#!/usr/bin/env python


"""
Simple blockchain client interface.

TODO: Much of this should be pushed up to `mediachain.client`.
"""


import mc_config
import mc_ingest

from mc_generic import setup_main

from os import mkdir, rename, unlink, listdir
from os.path import exists, join, split, lexists

from tempfile import NamedTemporaryFile

from mediachain.reader import api

#from mediachain.translation.simple.translator import SimpleTranslator
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

from grpc.framework.interfaces.face.face import (ExpirationError, AbortionError, 
    CancellationError, ExpirationError, LocalShutdownError, NetworkError, 
    RemoteShutdownError, RemoteError)
        
grpc_errors = (AbortionError, CancellationError, ExpirationError, 
               LocalShutdownError, NetworkError, RemoteShutdownError, 
               RemoteError,)


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
    """
    Minimal pass-through artefact translator.
    
    - `artist_names` string used to generate attribution blockchain links.
    - `img_data` - data URI for image thumbnail that will be inserted in blockchain.
    - Rest of artefact metadata is passed through as-is.
    
    Fields required by external apps built on top of the blockchain:
        Indexer:
            - `_id`      - globally-unique ID for image.
    
    TODO: Support multiple attribution links (multiple artists.)
    """
    
    @staticmethod
    def translator_id():
        return 'SimpleTranslator/0.1'
    
    @staticmethod
    def translate(parsed_metadata):
        simple_json = parsed_metadata

        ## Create artist Entity
        
        artist_name = simple_json.get('artist_names')

        if artist_name:
            artist_entity = {
                u'__mediachain_object__': True,
                u'type': u'entity',
                u'meta': {
                    u'data': {
                        u'name': artist_name
                    }
                }
            }
        else:
            artist_entity = None

        
        ## Create thumbnail object:
            
        thumb_uri = parsed_metadata['img_data']

        data[u'thumbnail'] = {
            u'__mediachain_asset__': True,
            u'uri': thumb_uri
        }

        
        ## Pass through rest of metadata as-is:
        
        data = simple_json

        
        ## Finish creating chain:
        
        artwork_artefact = {
            u'__mediachain_object__': True,
            u'type': u'artefact',
            u'meta': {'data': data}
        }

        chain = []
        if artist_entity is not None:
            chain.append({u'__mediachain_object__': True,
                          u'type': u'artefactCreatedBy',
                          u'meta': {},
                          u'entity': artist_entity
                          })

        return {
            u'canonical': artwork_artefact,
            u'chain': chain
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
        
        for obh in self.the_iter:
            
            metadata = obh['metadata']
            raw_source = obh['raw']
            
            try:
                
                with NamedTemporaryFile(delete = True,
                                        prefix = 'simpleclient_',
                                        suffix = '.jpg'
                                        ) as ft:
                        
                    uri_temp = 'file://' + ft.name
                    
                    if 'img_data' in metadata:
                        ft.write(mc_ingest.decode_image(metadata['img_data']))
                        ft.flush()
                        del metadata['img_data']
                    else:
                        assert False,('NO_IMG_DATA',)
                    
                    translated = self.translator.translate(metadata)
                    
                    local_assets = {'thumbnail': {'__mediachain_asset__': True,
                                                  'uri': uri_temp
                                                  }
                                    }
                    
                    yield {'parsed': metadata,
                           'translated': translated,
                           'raw_content': raw_source,
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
    
    
    def write_artefacts(self, *args, **kw):
        """ Convenience wrapper for `write_objects` """
        kw['object_type'] = 'artefacts'
        for x in self.write_objects(*args, **kw):
            yield x
    
    
    def write_objects(self,
                      the_iter,
                      object_type = False,
                      ):
        """
        Write objects to blockchain. Yields back blockchain IDs.

        Args:
            the_iter:   Yields tuples of (metadata_dict, raw_byte_string), defined as:
                        
                        metadata_dict: Must contain the following keys
                            '_id'      - ID of object.
                            'img_data' - data URI of image. TODO: Supporting other types.
                        
                        raw_byte_string:
                            Raw byte string, recorded as the source of this record's metadata.
        """
        
        ## Generally required args:
        assert object_type in ['artefacts', 'entities']
        
        ## Not yet implemented:
        if object_type != 'artefacts':
            raise NotImplemented
        
        ## Write:
        
        if object_type == 'artefacts':
            
            iterator = SimpleIterator(the_iter,
                                      SimpleTranslator,
                                      )
            
            writer = Writer(self.transactor,
                            download_remote_assets=False,
                            )

            ## Broken TODO - How to get the blockchain IDs that were generated?:
            
            for x in writer.write_dataset(iterator):
                yield x
            
        else:
            assert False
    
    
    def get_artefacts(self, *args, **kw):
        """ Convenience wrapper for `get_objects` """
        kw['object_type'] = 'artefacts'
        for x in self.get_objects(*args, **kw):
            yield x

            
    def get_entities(self, *args, **kw):
        """ Convenience wrapper for `get_objects` """
        kw['object_type'] = 'entities'
        for x in self.get_objects(*args, **kw):
            yield x
    
        
    def get_objects(self,
                    start_id = False,
                    end_id = False,
                    only_ids = False,
                    artist_ids = False,
                    fetch_images = False,
                    reverse = False,
                    timeout = 600,
                    force_exit_on_grpc_error = True,
                    object_type = False
                    ):
        """
        Get Artefacts or Entities from the blockchain.
        
        Items will be returned in "blockchain order" - TODO: exact definition of "blockchain order".
        
        The following args behave as filters, and most combinations of these args are supported:
        `start_id`, `end_id`, `only_art_ids`, `by_entity_ids`.
        
        Filtering Args for All Object Types:
            start_id:    (Object ID) Starting iteration from this Object ID.        
            end_id:      (Object ID) Iterate all artefacts, until and including this Object ID.
            only_ids:    (List of Object IDs) Filter to only these Object IDs.
        
        Artefact-Specific Filtering:
            art_by_ent_ids:  (List of Entity IDs) Return Artefacts connected to these Entities.
        
        Entity-Specific Filtering:
            ent_by_art_ids:  (List of Entity IDs) Return Entities connected to these Artefacts.
        
        Sorting Args:
            reverse:      (Boolean) Return results in reverse of specified sorting order type.
                                    (Currently only the "blockchain order" sorting type is supported.)
        Other Args:
            fetch_images: (Boolean) Whether to include thumbnail images as data URIs in the responses.
            timeout:      (Integer) Seconds before underlying GRPc timeout. TODO: should have a deeper
                                    look at this. Allow explicit resource freeing instead of timeout-based?
                
        General TODOs:
            - Batch up requests for `only_ids`, instead of 1 by 1.
            - `transactor.canonical_stream` can block indefinitely. Not appropriate for some use cases.
        """
        
        ## Generally required args:
        assert object_type in ['artefacts', 'entities']
        
        ## Invalid combinations:
        assert not (only_ids and (start_id or end_id)),'Conflicting combination or args.'
        
        ## Not yet implemented:        
        if object_type != 'artefacts':
            raise NotImplemented
        
        if reverse:
            raise NotImplemented
        
        ## Collect results:
        
        if only_ids:

            ## Filter by object IDs:
            
            for oid in only_ids:
                
                obj = api.get_object(self.transactor,
                                     object_id = oid,
                                     fetch_images = fetch_images,
                                     )
                yield obj
        
        else:

            ## No filtering:
            
            print ('STREAMING FROM TRANSACTOR...', self.transactor_host, self.transactor_port)
            
            try:
                started = False
                
                for obj in self.transactor.canonical_stream(timeout = timeout):
                    
                    if (start_id is not False) and (not started):
                        if obj['data']['_id'] == start_id:
                            started = True
                        else:
                            continue
                    
                    yield art
                    
                    if (end_id is not False):
                        if obj['data']['_id'] == end_id:
                            break
                    
            
            except grpc_errors as e:
                
                print ('!!!CAUGHT gRPC ERROR',e)
                
                if force_exit_on_grpc_error:
                    print ('FORCING EXIT',)
                    from time import sleep
                    import traceback, sys, os
                    for line in traceback.format_exception(*sys.exc_info()):
                        print line,
                    os._exit(-1)
                        


def test_blockchain(via_cli = False):
    """
    Simple round-trip test.

    TODO:
        This code assumes that once `write_artefacts` completes, the
        object is guarenteed to be immediately retrievable.
        
        Should either:
            - Make `write_objects` block until the object is retrievable.
            - Make `get_objects` block until the object is available.
            - Modify / clarify the blockchain's consistency guarantees.
    """
    
    ## Create client cursor:
    
    cur = SimpleClient()
    
    ## Test object:
    
    from uuid import uuid4
    
    test_id = 'test_' + uuid4().hex
    
    original_meta = {'_id': test_id,
                     'artist': 'Test Artist',
                     'description': 'Test Description',
                     'img_data': 'data:image/png;base64,iVBORw0KGgoAAAAN'\
                                 'SUhEUgAAAAUAAAAFCAYAAACNbyblAAAAHElEQV'\
                                 'QI12P4//8/w38GIAXDIBKE0DHxgljNBAAO9TXL'\
                                 '0Y4OHwAAAABJRU5ErkJggg==',
                     }

    raw_meta = consistent_json_dumper(original_meta)
    
    ## Write object:
    
    blockchain_ids = cur.write_artefacts([{'metadata':original_meta,
                                           'raw':raw_meta,
                                           },
                                          ])

    b_ids = list(blockchain_ids)

    print ('b_id',b_ids)
    
    ## Check that object of same `_id` is returned:
    
    got_obj = False
    
    for art in cur.get_artefacts(only_ids = b_ids,
                                 fetch_images = True,
                                 ):
        print ('GOT_ART')
        
        returned_meta = art['meta']['data']
        
        if returned_meta['_id'] == original_meta['_id']:
            print ('PASSED_ROUND_TRIP',
                   original_meta['_id'],
                   returned_meta['_id'],
                   )
            got_obj = True
        
        else:
            assert False,('MISMATCHED_ID',
                          original_meta['_id'],
                          returned_meta['_id'],
                          )
    
    assert got_obj,'NO_OBJECTS_RETURNED'

    
    ## Check metadata, excluding images / thumbnails:
    
    del returned_meta['img_data']
    del original_meta['thumbnail']
    
    returned_d = consistent_json_dumper(returned_meta)
    original_d = consistent_json_dumper(original_meta)
    
    assert returned_d == original_d,('RETURNED_META_DIFFERS')
    
    print ('PASSED_ALL')
    

    
functions=['test_blockchain']

def main():
    setup_main(functions,
               globals(),
                'mediachain-indexer-simpleclient',
               )

if __name__ == '__main__':
    main()

