#!/usr/bin/env python


#Elasticsearch index & doc type for image docs (identified by Media IDs):

INDEX_NAME = 'getty_test'
DOC_TYPE = 'image'


#Elasticsearch index & doc type resolving media IDs to cluster IDs:

INDEX_NAME_MID_TO_CID = 'clustering_baseline_mid_to_cid'
DOC_TYPE_MID_TO_CID = 'cluster_nums'


#Elasticsearch index & doc type resolving cluster IDs to clusters (list of media IDs):

INDEX_NAME_CID_TO_CLUSTER = 'clustering_baseline_cid_to_cluster'
DOC_TYPE_CID_TO_CLUSTER = 'clusters'
