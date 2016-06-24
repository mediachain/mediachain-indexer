#!/usr/bin/env python

"""
Each config value will be automatically converted to one of the following types, based on the suffix of the variable's name:

   '_INT'     = integer
   '_FLOAT'   = float
   '_JSON'    = JSON
   '_FJSON'   = JSON loaded from filename.
      *       = string

See `mediachain.indexer.mc_generic.config_env` for more details on parsing of this config.

Config structure in `mediachain.indexer.mc_config`:

   {'section_title':{var_name:(var_value,'var_description')}}

   or

   {'section_title':{var_name:(var_value,['var_description', 'more_description'])}}

"""


cfg = {'1. Model Settings. NOTE - WIP. These settings are not enabled yet.':
           {'MC_LOOKUP_MODEL_TEXT':('mediachain.indexer.neighbors.ElasticSearchNN',
                                    ['Built-in options: ',
                                     '`mediachain.indexer.neighbors.AnnoyNN`, ',
                                     '`mediachain.indexer.neighbors.ElasticSearchNN`',
                                    ]
                                    ),
            'MC_LOOKUP_MODEL_SPARSE_VECTORS':('mediachain.indexer.neighbors.ElasticSearchNN',
                                              ['Built-in options: ',
                                               '`mediachain.indexer.neighbors.PysparnnNN`, ',
                                               '`mediachain.indexer.neighbors.ElasticSearchNN`',
                                               ],
                                              ),  
            'MC_NN_MODEL_DENSE_VECTORS':('mediachain.indexer.neighbors.ElasticSearchNN',
                                         'Model to use for dense vectors nearest-neighbors search.',
                                         ),
            },
       '2. Elasticsearch Settings':
           {'MC_ES_URLS':('',
                          ['One or more comma-separated RFC-1738 formatted URLs.',
                           '\ne.g. "http://user:secret@localhost:9200/,https://user:secret@other_host:443/production"']
                          ),
            'MC_NUMBER_OF_SHARDS_INT':('1', ''),
            'MC_NUMBER_OF_REPLICAS_INT':('0', ''),
            'MC_INDEX_NAME':('getty_test', ''),
            'MC_DOC_TYPE':('image', ''),
           },
       '3. Ingestion Settings':
           {'MC_GETTY_KEY':('', 'Getty key, for creating local dump of getty images'),
            'MC_AWS_ACCESS_KEY_ID':(None, ''),
            'MC_AWS_SECRET_ACCESS_KEY':(None, ''),
            'MC_DYNAMO_TABLE_NAME':('Mediachain', ''),
            'MC_REGION_NAME':(None, 'AWS region of DynamoDB instance'),
            'MC_ENDPOINT_URL':(None, 'AWS endpoint of DynamoDB instance'),
            'MC_USE_IPFS_INT':(1, 'Use IPFS for image ingestion.'),
           },
       '4. Settings for Automated Tests':
           {'MC_TEST_WEB_HOST':('http://127.0.0.1:23456', ''),
            'MC_TEST_INDEX_NAME':('mc_test', ''),
            'MC_TEST_DOC_TYPE':('mc_test_image', 'Document type, required for some neighbors models.'),
           },
           '5. Transactor Settings':
           {'MC_TRANSACTOR_HOST':('127.0.0.1', ''),
            'MC_TRANSACTOR_PORT_INT':('10001', ''),
           },
       }

import mc_generic
mc_generic.config_env(cfg, globals())


LOW_LEVEL = False ## Temporary - For transitioning from old approach to new.
