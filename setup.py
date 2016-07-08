#!/usr/bin/env python

from setuptools import setup, find_packages
from os.path import join, realpath, dirname

reqs_file = join(dirname(realpath(__file__)), 'requirements.txt')

with open(reqs_file) as f:
    reqs = f.readlines()
    
setup(version = '0.0.10',
      name = 'mediachain-indexer',
      description = 'Mediachain Indexer - Search, Dedupe, Ingestion.',
      author = 'Mediachain Labs',
      packages = find_packages('.'),
      entry_points = {'console_scripts': [## Long versions:
                                          'mediachain-indexer-models = mediachain.indexer.mc_models:main',
                                          'mediachain-indexer-ingest = mediachain.indexer.mc_ingest:main',
                                          'mediachain-indexer-web = mediachain.indexer.mc_web:main',
                                          'mediachain-indexer-test = mediachain.indexer.mc_test:main',
                                          'mediachain-indexer-eval = mediachain.indexer.mc_eval:main',
                                          'mediachain-indexer-datasets = mediachain.indexer.mc_datasets:main',
                                          'mediachain-indexer-simpleclient = mediachain.indexer.mc_simpleclient:main',
                                          ## Short versions:
                                          'mci-models = mediachain.indexer.mc_models:main',
                                          'mci-ingest = mediachain.indexer.mc_ingest:main',
                                          'mci-web = mediachain.indexer.mc_web:main',
                                          'mci-test = mediachain.indexer.mc_test:main',
                                          'mci-eval = mediachain.indexer.mc_eval:main',
                                          'mci-datasets = mediachain.indexer.mc_datasets:main',
                                          'mci-simpleclient = mediachain.indexer.mc_simpleclient:main'
                                          ]
                      },
    url = 'http://mediachain.io',
    install_requires = reqs,
)
