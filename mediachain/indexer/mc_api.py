#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Top-level Indexer API.
"""

import mc_config

class IndexerAPI(object):
    """
    Top-level Indexer API. Basically a further-abstracted version of `NearestNeighborsBase`.
    """
    
    def __init__(self,
                 models_config = mc_config.MC_MODELS_JSON,
                 ):
        self.models = {}
        
        for model_name, model_config in models_config:
            
            self.create_index(model_name,
                              model_config,
                              )
        
    def create_index(self, *args, **kw):
        raise NotImplementedError
    
    def delete_index(self, *args, **kw):
        raise NotImplementedError

    def refresh_index(self, *args, **kw):
        raise NotImplementedError
    
    def scan_all(self, *args, **kw):
        raise NotImplementedError
    
    def parallel_bulk(self, *args, **kw):
        raise NotImplementedError
            
    def search_full_text(self, *args, **kw):
        raise NotImplementedError

    def search_terms(self, *args, **kw):
        raise NotImplementedError

    def search_ids(self, *args, **kw):
        raise NotImplementedError

    def count(self, *args, **kw):
        raise NotImplementedError

    

