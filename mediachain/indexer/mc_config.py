#!/usr/bin/env python

"""
Config of format:

   {'section_title':{var_name:(var_value,'var_description')}}

   or

   {'section_title':{var_name:(var_value,['var_description', 'more_description'])}}

NOTE: Use the following suffixes on variable names for typing. See mc_generic.config_env for more details:

   '_INT'     = integer
   '_FLOAT'   = float
      *       = string
"""


cfg = {'1. Elasticsearch Settings':
       {'MC_ES_URLS':('',
                      ['One or more comma-separated RFC-1738 formatted URLs.',
                       '\ne.g. "http://user:secret@localhost:9200/,https://user:secret@other_host:443/production"']
                      ),
        'MC_INDEX_NAME':('getty_test', ''),
        'MC_DOC_TYPE':('image', ''),
        'MC_NUMBER_OF_SHARDS_INT':('1', ''),
        'MC_NUMBER_OF_REPLICAS_INT':('0', ''),
       },
       '2. Ingestion Settings':
       {'MC_GETTY_KEY':('', 'Getty key, for creating local dump of getty images'),
        'MC_AWS_ACCESS_KEY_ID':(None, ''),
        'MC_AWS_SECRET_ACCESS_KEY':(None, ''),
        'MC_DYNAMO_TABLE_NAME':('Mediachain', ''),
        'MC_REGION_NAME':(None, 'AWS region of DynamoDB instance'),
        'MC_ENDPOINT_URL':(None, 'AWS endpoint of DynamoDB instance'),
        'MC_USE_IPFS':(True, 'Use IPFS for image ingestion.'),
       },
       '3. Settings for Automated Tests':
       {'MC_TEST_WEB_HOST':('http://127.0.0.1:23456', ''),
        'MC_TEST_INDEX_NAME':('mc_test', ''),
        'MC_TEST_DOC_TYPE':('mc_test_image', ''),
       },
       '4. Transactor settings':
       {'MC_TRANSACTOR_HOST':('127.0.0.1', ''),
        'MC_TRANSACTOR_PORT_INT':('10001', ''),
       },
       }

import mc_generic
mc_generic.config_env(cfg, globals())


LOW_LEVEL = True ## Temporary - For transitioning from old approach to new.
