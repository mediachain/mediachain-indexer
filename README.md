
## Search and Dedupe for Mediachain

Entry Point                 |  Info
----------------------------|---------------------
mediachain-indexer-datasets | Download training and ingestion datasets.
mediachain-indexer-ingest   | Ingest media from local sources or Mediachain API, for search & dedupe.
mediachain-indexer-models   | Search and deduplication models. (Re-)Generate dedupe lookup tables.
mediachain-indexer-eval     | Hyper-parameter optimization and evaluation of models.
mediachain-indexer-web      | Search & dedupe REST API.
mediachain-indexer-test     | Tests and sanity checks.


## Getting Started

#### Core Setup

1) Install Elasticsearch. Version 2.3.2 or higher recommended:
  - [General Instructions](https://www.elastic.co/guide/en/elasticsearch/reference/current/_installation.html).
  - OSX: `brew install elasticsearch`
  - Linux: Check with your distribution.

2) Launch Elasticsearch server:

```
$ elasticsearch
```

3) Install Indexer:

```
$ pip install git+https://github.com/mediachain/mediachain-indexer.git
```

4) Run any of the entry points list above, to see sub-command details:

```
$ mediachain-indexer-ingest

USAGE: mediachain-indexer-ingest <function_name>

Available Functions:
ingest_bulk_blockchain.................. Ingest media from Mediachain blockchain.
ingest_bulk_gettydump................... Ingest media from Getty data dumps into Indexer.
config.................................. Print current environment variables.
```

5) Inspect environment variables and adjust as necessary:

```
$ mediachain-indexer-ingest config

### CONFIG:

## 1. Elasticsearch Settings:

  MC_NUMBER_OF_SHARDS_INT  = 1                           <INT>
  MC_NUMBER_OF_REPLICAS_INT= 0                           <INT>
  MC_INDEX_NAME            = 'getty_test'                <STR>
  MC_DOC_TYPE              = 'image'                     <STR>

  # One or more comma-separated RFC-1738 formatted URLs.
  # e.g. "http://user:secret@localhost:9200/,https://user:secret@other_host:443/production":
  MC_ES_URLS               = ''                          <STR>

## 2. Ingestion Settings:

  # AWS region of DynamoDB instance:
  MC_REGION_NAME           = None                        <STR>
  MC_AWS_SECRET_ACCESS_KEY = None                        <STR>
  MC_AWS_ACCESS_KEY_ID     = None                        <STR>
  MC_DYNAMO_TABLE_NAME     = 'Mediachain'                <STR>

  # Getty key, for creating local dump of getty images:
  MC_GETTY_KEY             = ''                          <STR>

  # AWS endpoint of DynamoDB instance:
  MC_ENDPOINT_URL          = None                        <STR>

## 3. Settings for Automated Tests:

  MC_TEST_WEB_HOST         = 'http://127.0.0.1:23456'    <STR>
  MC_TEST_INDEX_NAME       = 'mc_test'                   <STR>
  MC_TEST_DOC_TYPE         = 'mc_test_image'             <STR>

## 4. Transactor settings:

  MC_TRANSACTOR_HOST       = '127.0.0.1'                 <STR>
  MC_TRANSACTOR_PORT_INT   = 10001                       <INT>
```


#### Quick Test

6) Run a basic sanity check:

```
$ mediachain-indexer-test sanity_check
```

#### Full Setup

7) Alternatively, grab small Getty testing dataset:

```
$ MC_GETTY_KEY="<your_getty_key>" mediachain-indexer-datasets getty_create_dumps
```

8) Ingest Getty dataset:

```
$ mediachain-indexer-ingest ingest_bulk_gettydump
```

Or, ingest from Mediachain blockchain:

```
$ mediachain-indexer-ingest ingest_bulk_blockchain
```


9) Deduplicate ingested media:

```
$ mediachain-indexer-models dedupe_reindex
```

10) Start REST API server:

```
$ mediachain-indexer-web web
```

11) Query the [REST API](https://github.com/mediachain/mediachain/blob/master/rfc/mediachain-rfc-3.md#rest-api-overview):

Search by text:

```
$ curl "http://127.0.0.1:23456/search" -d '{"q":"crowd", "limit":5}'
{
    "next_page": null,
    "prev_page": null,
    "results": [
        {
            "_id": "getty_531746924",
            "_index": "getty_test",
            "_score": 0.08742375,
            "_source": {
                "artist": "Tristan Fewings",
                "caption": "CANNES, FRANCE - MAY 16:  A policeman watches the crowd in front of the Palais des Festival during the red carpet arrivals of the 'Loving' premiere during the 69th annual Cannes Film Festival on May 16, 2016 in Cannes, France.  (Photo by Tristan Fewings/Getty Images)",
                "collection_name": "Getty Images Entertainment",
                "date_created": "2016-05-16T00:00:00-07:00",
                "editorial_source": "Getty Images Europe",
                "keywords": "People Vertical Crowd Watching France Police Force Cannes Film Premiere Premiere Arrival Photography Film Industry Red Carpet Event Arts Culture and Entertainment International Cannes Film Festival Celebrities Annual Event Palais des Festivals et des Congres 69th International Cannes Film Festival Loving - 2016 Film",
                "title": "'Loving' - Red Carpet Arrivals - The 69th Annual Cannes Film Festival"
            },
            "_type": "image"
        }
    ]
}
```

Search by media content:

```
$ curl "http://127.0.0.1:23456/search" -d '{"q_id":"data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAUAAAAFCAYAAACNbyblAAAAHElEQVQI12P4//8/w38GIAXDIBKE0DHxgljNBAAO9TXL0Y4OHwAAAABJRU5ErkJggg==", "limit":5, "index_name":"mc_test", "doc_type":"mc_test_image"}'
{
    "next_page": null,
    "prev_page": null,
    "results": [
        {
            "_id": "getty_1234",
            "_index": "mc_test",
            "_score": 1.0,
            "_source": {
                "artist": "test",
                "caption": "test",
                "collection_name": "test",
                "date_created": "2016-05-31T17:41:06.929234",
                "editorial_source": "",
                "keywords": "test",
                "title": "Crowd of people walking"
            },
            "_type": "mc_test_image"
        }
    ]
}
```

Search by ID:

```
$ curl "http://127.0.0.1:23456/search" -d '{"q_id":"getty_1234", "limit":5, "index_name":"mc_test", "doc_type":"mc_test_image"}'
{
    "next_page": null,
    "prev_page": null,
    "results": [
        {
            "_id": "getty_1234",
            "_index": "mc_test",
            "_score": 1.0,
            "_source": {
                "artist": "test artist",
                "caption": "test caption",
                "collection_name": "test collection name",
                "date_created": "2016-06-01T15:21:57.796894",
                "editorial_source": "test editorial source",
                "keywords": "test keywords",
                "title": "Crowd of People Walking"
            },
            "_type": "mc_test_image"
        }
    ]
}
```

Duplicate lookup by ID:

```
$ curl "http://127.0.0.1:23456/dupe_lookup" -d '{"q_media":"getty_531746790"}'
{
    "next_page": null,
    "prev_page": null,
    "results": [
        {
            "_id": "getty_1234"
        }
    ]
}
```

Pass `{"help":1}` to any endpoint, to get a plain-text help string:

```
$ curl "http://127.0.0.1:23456/dupe_lookup" -d '{"help":1}' | head

Find all known duplicates of a media work.

Args - passed as JSON-encoded body:
    q_media:          Media to query for. See `Media Identifiers`.
    lookup_name:      Name of lookup key for the model you want to use. See `lookup_name` of `dedupe_reindex()`.
                      Note: must use 'dedupe_hsh' as lookup_name if v1_mode is True.
    incremental:      If True, only update clusters affected by newly ingested media. Otherwise, regenerate
                      all dedupe clusters. Note: the more records that are deduped simultaneously, the greater
                      the efficiency.
    include_self:     Include ID of query document in results.
[...]
```


## Code Organization


```
                      INGESTION:               SEARCH:                       DEDUPE:

                   +--------------+   +-------------------------+    +-------------------------+
                   |  Transactors |   |   End-User Web Browser  |    |     Transactors         |
                   +------+-------+   +---+-------------^-------+    +----------^--------------+
                          |               |             |                       |
                          v               v             ^                       ^
                     (copycat/gRPC)  (JSON/REST)   (JSON/REST)            (copycat/gRPC)
                          |               |             |                       |
                +---------)---------------+-------------+-----------------------)-----------------+
             /  |         |               |             |                       |                 |
            |   |         |           +---v-------------+-------+               |                 |
 mediachain<    |         |           | Javascript/HTML Web App |               |                 |
 -frontend  |   |         |           +---+-------------^-------+               |                 |
             \  |         |               |             |                       |                 |
                ----------)---------------+-------------+-----------------------)-----------------+
                          |               |             |                       |
                          v               v             ^                       ^
                     (copycat/gRPC)  (JSON/REST)   (JSON/REST)            (copycat/gRPC)
                          |               |             |                       |
                +---------+---------------)-------------)-----------------------+-----------------+
             /  |         |               |             |                       |                 |
            |   |  +------v--------+      |             |            +----------+--------------+  |
 mediachain<    |  | Client Reader |      v             ^            |    Client Writer        |  |
 -client    |   |  +------+--------+      |             |            +----------^--------------+  |
             \  |         |               |             |                       |                 |
                ----------+---------------)-------------)-----------------------+-----------------+
                          |               |             |                       |
                          v               v             ^                       ^
                     (copycat/gRPC)  (JSON/REST)   (JSON/REST)         (Artefact-Linkage)
                          |               |             |                       |
                +---------+---------------+-------------+-----------------------+-----------------+
              / |         |               |             |                       |                 |
             |  |  +------v----------+    |             |                       |                 |
mc_ingest.py<   |  | Media Ingestion |    v             ^                       ^                 |
             |  |  +------+----------+    |             |                       |                 |
              \ |         |               |             |                       |                 |
                |         |          (Raw Media)   (Media IDs)         (Artefact-Linkage)         |
              / |         |          (/Text Query)      |                       |                 |
             |            |               |             |                       |                 | 
             |  |         |         +-----v-------------+----------+            |                 | 
 mc_web.py  <   |         v         |      HTTP Search API         |            ^                 | 
             |  |         |         +-----+-------------^----------+            |                 | 
              \ |         |               |             |                       |                 | 
                |         |               |        (Media IDs)                  |                 | 
              / |         |               |             |                       |                 | 
             |  |         |               |    +--------+----------+ +----------+--------------+  | 
             |  |         |               |    | Search Override   | |   Dedupe Staging        |  | 
             |  |         |               |    +--------^----------+ +----------^--------------+  | 
             |  |         |               |             |                       |                 | 
             |  |    (Raw Media)     (Raw Media)   (Media IDs)           (Artefact-Linkage)       | 
             |  |    (& Metadata)    (/Text Query)      |                       |                 | 
             |  |         |               |             |                       |                 | 
             |  |         |               |    +--------+----------+ +----------+--------------+  | 
             |  |         |               |    | Search Re-Ranking | |    Dedupe Clustering    |  | 
             |  |         |               |    +--------^----------+ +----------^--------------+  | 
             |  |         |               |             |                       |                 | 
mc_models.py<   |         |               |       (Media IDs)           (Pair IDs+Split/Merge)    | 
             |  |         |               |       (& Scores)                    |                 | 
             |  |         v               v             |                       |                 | 
             |  |         |               |    +--------+----------+ +----------+--------------+  | 
             |  |         |               |    |  Personalization  | |  Dedupe Pairwise Model  |  | 
             |  |         |               |    +--------^----------+ +----------^--------------+  | 
             |  |         |               |             |                       |                 | 
             |  |    (Raw Media)     (Raw Media)   (Media IDs)        (IDs for Candidate Groups)  | 
             |  |    (& Metadata)    (/Text Query)      |                       |                 | 
             |  |         |               |             |                       |                 | 
             |  |         |               |             |            +----------+--------------+  | 
             |  |         |               |             |            |   Dedupe All-vs-All NN  |  | 
             |  |         |               |             |            +-----+------------^------+  | 
             |  |         |               |             |                  |            |         | 
              \ |         |               |             |                  |            |         | 
                |         v               v             ^                  v            ^         | 
              / |         |               |             |                  |            |         | 
             |  |  +------v---------------v--+          |                  |            |         | 
             |  |  |   Generate Features     |          |                  |            |         | 
             |  |  +------+---------------+--+          |                  |            |         | 
             |  |         |               |             |                  |            |         | 
             |  |  (Descriptors)   (Descriptors)   (Media IDs)        (Media IDs)  (Media IDs)    |
             |  |         |               |             |                  |            |         |
             |  |  +------v---------------v--+          |                  |            |         |
mc_neighbors<   |  |   Feature Compacting    |          |                  |            |         |
   .py       |  |  +------+---------------+--+          ^                  v            ^         |
             |  |         |               |             |                  |            |         |
             |  |    (Binary Codes) (Binary Codes)      |                  |            |         |
             |  |         |               |             |                  |            |         |
             |  |  +------v---------------v-------------+------------------v------------+------+  |
             |  |  |                                KNN Index                                  |  |
              \ |  +---------------------------------------------------------------------------+  |
                |                                                                                 |
                |                             --Mediachain Indexer--                              |
                |                                                                                 |
                +---------------------------------------------------------------------------------+
   
```