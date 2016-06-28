#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Nearest-neighbors storage backends (subclassing `NearestNeighborsBase`), along with testing simulators.

TODO:
- Async vs blocking variants?
- Complete integration of this new abstract approach.
"""

from collections import Counter
from math import sqrt, log, floor, ceil

import struct

import mc_config

from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk as es_parallel_bulk, scan as es_scan


class LuceneSmallFloat():
    """
    Emulate Lucene's `SmallFloat`.
    
    See: https://lucene.apache.org/core/4_3_0/core/org/apache/lucene/util/SmallFloat.html
    """
    
    @classmethod
    def floatToRawIntBits(cls, b):
        'Simulate Java library function - convert ieee 754 floating point number to integer:'
        return struct.unpack('>l', struct.pack('>f', b))[0]

    @classmethod
    def intBitsToFloat(cls, b):
        'Simulate Java library function - convert integer to ieee 754 floating point'
        if (b < 2147483647) or (b > -2147483648):
            return struct.unpack('>f', struct.pack('>l', b))[0]
        else:
            assert False,'todo?'
            #return struct.unpack('d', struct.pack('Q', int(bin(b), 0)))[0]

    @classmethod
    def byteToFloat(cls, b, numMantissaBits, zeroExp):
        'Converts an 8 bit float to a 32 bit float.'
        if (b == 0):
            return 0.0
        bits = (b & 0xff) << (24 - numMantissaBits)
        bits += (63 - zeroExp) << 24
        return cls.intBitsToFloat(bits)

    @classmethod
    def floatToByte(cls, f, numMantissaBits, zeroExp):
        'Converts an 8 bit float to a 32 bit float.'
        
        bits = cls.floatToRawIntBits(f)
        smallfloat = bits >> (24 - numMantissaBits)

        if (smallfloat <= ((63 - zeroExp) << numMantissaBits)):
            return (bits <= 0) and 0 or 1

        if (smallfloat >= ((63 - zeroExp) << numMantissaBits) + 0x100):
            return -1

        return smallfloat - ((63 - zeroExp) << numMantissaBits)
    
    @classmethod
    def floatToByte315(cls, f):
        'Simulate Lucene function.'
        
        return cls.floatToByte(f, numMantissaBits=3, zeroExp=15)
        
        bits = cls.floatToRawIntBits(f)
        smallfloat = bits >> (24 - 3)

        if (smallfloat <= ((63 - 15) << 3)):
            return (bits <= 0) and 0 or 1

        if (smallfloat >= ((63 - 15) << 3) + 0x100):
            return -1

        return smallfloat - ((63 - 15) << 3)

    @classmethod
    def byte315ToFloat(cls, b):
        'Simulate Lucene function.'

        return cls.byteToFloat(b, numMantissaBits=3, zeroExp=15)
        
        if (b == 0):
            return 0.0
        bits = (b & 0xff) << (24 - 3)
        bits += (63 - 15) << 24
        return cls.intBitsToFloat(bits)

    @classmethod
    def byte52ToFloat(cls, b):
        return cls.byteToFloat(b, numMantissaBits=5, zeroExp=2)

    @classmethod
    def floatToByte52(cls, f):
        return cls.floatToByte(f, numMantissaBits=5, zeroExp=2) 
    
    @classmethod
    def float_round_trip_315(cls, x):
        return cls.byte315ToFloat(cls.floatToByte315(x))

    @classmethod
    def float_round_trip_52(cls, x):
        return cls.byte52ToFloat(cls.floatToByte52(x))

    @classmethod
    def test(cls):
        ' Test from: http://www.openjems.com/tag/querynorm/'
        from math import sqrt
        assert LuceneSmallFloat.float_round_trip_315(1 / sqrt(13)) == 0.25
        print 'PASSED'

class LuceneScoringClassic():
    """
    Emulate Lucene's classic tf-IDF-based relevance scoring formula.
    """
    
    def __init__(self):
        self.df = Counter()
        self.num_docs = 0
        self.docs = {}
        
    def add_doc(self,
                doc,
                id = False,
                ):
        """
        Args:
            doc: Dict of form {term: count}
        """
        self.df.update({x:1 for x in doc})
        self.num_docs += 1
        
        if id is not False:
            self.docs[id] = doc
        
    def score(self,
              query,
              doc,
              query_boost = 1.0,
              verbose = False,
              ):
        """
        Attempt to exactly replicate Lucene's classic relevance scoring formula:
        
        score(q,d)  =  
                queryNorm(q)  
              · coord(q,d)    
              · ∑ (           
                    tf(t in d)   
                  · idf(t)²      
                  · t.getBoost() 
                  · norm(t,d)    
                ) (t in q)    
    
        Args:
            query:  Dict of form {term: count, ...}
            doc:    Dict of form {term: count, ...}

        See Also:
            No one quite gets the formula right, but have a look at these:
            https://www.elastic.co/guide/en/elasticsearch/guide/master/practical-scoring-function.html
            http://www.openjems.com/tag/querynorm/
        """
        
        assert self.num_docs,'First `add_doc()`.'

        is_hit = len(set(query).intersection(doc)) and True

        if not is_hit:
            return 0.0
        
        ## Computes a score factor based on a term or phrase's frequency in a document:
        query_norm = 1.0 / sqrt(sum([(1.0 + log(self.num_docs / (self.df.get(term, 0.0) + 1.0))) ** 2
                                     for term
                                     in query
                                     ]))

        if verbose:
            print 'query_norm',query_norm,'self.num_docs',self.num_docs
        
        # Rewards documents that contain a higher percentage of the query terms:
        coord = len(set(query).intersection(doc)) / float(len(query))
        
        xx = 0.0
        
        for term in query:
            ## Term frequency of this term in this document:
            tf = sqrt(doc.get(term, 0.0))
            
            ## Inverse of frequency of this term in all documents:
            idf  = (1.0 + log(self.num_docs / (self.df.get(term, 0.0) + 1.0))) ** 2
            
            ## Query-level boost:
            boost = query_boost
            
            ## Field-length norm (number of terms in field), combined with the field-level boost, if any:
            
            if True:
                ## !!! For our needs, we can just assume field_norm is 1.0:
                
                field_norm = 1.0
                
            else:
                ## Full field_norm calculation. Everything here is tricky. Luckily we can ignore it for our needs:
                
                field_length = 1 ## See docs / source for details on this. For our query forms, assume 1.
                
                field_norm = LuceneSmallFloat.float_round_trip_315(boost / sqrt(field_length))
            
            xx += tf * idf * field_norm # * boost + (idf * query_norm)
            
            if verbose:
                print '->tf',tf,'idf(%d,%d)' % (self.num_docs, self.df.get(term,0)),'idf',idf,
                print 'boost',boost,
                print 'field_length',field_length,'field_norm',field_norm,'=xx',tf * idf * boost * field_norm
            
        rr = xx * query_norm * coord

        #rr = floor(rr * 1e8) / 1e8
        
        if verbose:
            print 'xx',xx,'query_norm',query_norm,'coord',coord,'=',rr
        #raw_input_enter()
        
        return rr


class ElasticSearchEmulator():
    """
    Attempts to exactly reproduce the particular subset of ES functionality we use.
    
    Useful for indexer system design without having to make assuptions about Lucene / ES black-box scoring formulas,
    for testing, model evaluation, and hyper-parameter optimization.
    """
    
    def __init__(self,
                 scoring = LuceneScoringClassic(),
                 *args,
                 **kw):

        class indices():
            def exists(self,
                       *args,
                        **kw):
                return False
            
            def delete(self,
                       *args,
                        **kw):
                pass
            
            def create(self,
                       index,
                       body,
                       *args,
                        **kw):
                pass
            
            def refresh(self,
                        index,
                        *args,
                        **kw):
                pass
        
        
        self.indices = indices()

        self.scoring = scoring
        
    def index(self,
              index,
              doc_type,
              id,
              body,
              *args,
              **kw):
        """
        Accept documents of the form:
        
            {'word1':1, 'word2':2, 'word3':3}
        """
        assert id
        assert body
        
        terms = {x:y for x,y in body.items() if not x.startswith('_')}
        
        self.scoring.add_doc(terms,
                             id = id,
                             )
    
    def search(self,
               index,
               doc_type,
               body,
               explain = False,
               *args,
               **kw):
        """
        Accepts only queries of the forms:
        
            {'query': {'bool': {'should': [{'term':{'word1':1}}, {'term':{'word2':2}} ] } } } 

        Or:

            {'query': {'constant_score': {'filter': {'term': {'dedupe_hsh': 'a6935e549289a7a55ce45c98662db700'}}}}}

        Or:
            {'query':{'filtered': {'query': {'bool': {'should': [{'term': {x:y}} for x,y in terms.items()] } } } } }
        """
        
        ## Get query back into same format as docs:
        
        if body.get('query',{}).get('filtered',{}).get('query',{}).get('bool'):
            vv = body['query']['filtered']['query']['bool']['should']
            
        elif body.get('query',{}).get('bool'):
            vv = body['query']['bool']['should']
            
        else:
            vv = [body['query']['constant_score']['filter']]

        qterms = {}
        for xx in [x['term'] for x in vv]:
            qterms.update(xx)
        
        rh = {'hits':{'hits':[]}}
        
        ## Score and sort docs:
        
        xx = []
        for doc_id,doc in self.scoring.docs.iteritems():

            xdoc = doc.copy()
            xdoc['_id'] = doc_id
            
            xx.append((self.scoring.score(qterms,
                                          doc,
                                          ),
                       xdoc
                       ),
                      )
        
        ## Apparently ES moves stuff around like this:
        
        for sc,doc in sorted(xx, reverse = True):
            
            h = {'_id':unicode(doc['_id']),
                 '_type':unicode(doc_type),
                 '_index':unicode(index),
                 '_source':doc,
                 '_score':sc,
                 }
            
            del doc['_id']
            
            if explain:
                h['_explanation'] = {'details':[], 'description':'EMULATOR_NOT_IMPLEMENTED', 'value':sc}
            
            rh['hits']['hits'].append(h)
        
        return rh


class NearestNeighborsBase(object):
    """
    Base class for nearest neighbor index implementations.
    """
    def create_index(self, *args, **kw):
        pass
    
    def delete_index(self, *args, **kw):
        pass

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


if False:
    """
    TODO:
       - Add as git submodule - https://github.com/facebookresearch/pysparnn.git
       - Due to threading issues, maybe this should run as a service in its own process?
    """
    
    import pysparnn as snn
    import numpy as np
    import multiprocessing
    from scipy.sparse import csr_matrix

    def do_batch(cp, i, len_dids, batch_size, num_k, i1, xquery):
        """
        Note: must be at top level of module due to multiprocessing.
        """
        print ('batch',i)
        
        IX = cp.search(xquery,
                       k = num_k,
                       return_distance = False,
                       )

        IX = np.array(IX)
        
        return (i, i1, IX)

    class PysparnnNN(NearestNeighborsBase):
        def __init__(self):
            assert False,'WIP'

            self.num_cores = 0
            
            self.X_ids_idx_num = 0
            self.X_ids_idx = {}
            self.X_ids = []
            self.X_terms_raw = []
            
        def create_index(self, *args, **kw):
            pass
        
        def delete_index(self, *args, **kw):
            pass
        
        def refresh_index(self, *args, **kw):
            """
            Actually index the data.
            """
            
            ## TODO vectorization. This vectorization could be moved to parallel_bulk.:
            X_terms_vect = vect.partial_fit_transform(X_terms_raw)
            
            print ('Creating index...')
            t1 = time()
            self.index = snn.ClusterIndex(X_terms_vect, range(X_terms_vect.shape[0]))
            print ('Created index.',time() - t1)
        
        def scan_all(self, *args, **kw):
            raise NotImplementedError
        
        def parallel_bulk(self,
                          the_iter,
                          *args, **kw):
            """
            Parallel inserting interface, reminiscent of the elasticsearch interface.

            TODO: support plain matrix batches too.
            """

            for hh in the_iter:
                
                print 'NON_PARALLEL_BULK',hh
                
                xaction = hh['_op_type']
                xindex = hh['_index']
                xtype = hh['_type']
                xid = hh['_id']
                
                for k,v in hh.items():
                    if k.startswith('_'):
                        del hh[k]
                
                print 'BODY',hh
                
                if xaction == 'index':
                    self.X_ids_idx_num += 1
                    self.X_ids_idx[hh['_id']] = self.X_ids_idx_num
                    self.X_ids.append(hh['_id'])
                    self.X_terms_raw.append([x['term'] for x in hh.items() if hh.startswith('term_')]) ## TODO: data format
                else:
                    raise NotImplementedError
                
                print 'DONE-NON_PARALLEL_BULK',xaction,xid

                
        def search_terms(self, *args, **kw):
            assert False,'WIP'
            
            if self.num_cores == 0:
                self.num_cores = multiprocessing.cpu_count()
            
            pool = multiprocessing.Pool(self.num_cores)

            tbtr = [pool.apply_async(do_batch,
                                     [self.index,
                                      i,
                                      len(dids),
                                      batch_size,
                                      num_k,
                                      min(len(dids), i + batch_size),
                                      X[i:min(len(dids), i + batch_size)],
                                     ],
                                     )
                    for i
                    in xrange(0, len(dids), batch_size)
                    ]
            
            btr = [r.get() for r in tbtr]

            for i, i1, IX in btr:
                for j in xrange(i1-i):
                    
                    yield (dids[i + j], [[dids[x]] for x in IX[j] if x != i + j])
                    
                print 'done %d/%d...' % (min(len(dids), i+batch_size), len(dids))
                    
            pool.terminate()

            
        def search_full_text(self, *args, **kw):
            raise NotImplementedError
        
        def search_ids(self, *args, **kw):
            raise NotImplementedError

        def count(self, *args, **kw):
            raise NotImplementedError


if False:
    
    ## TODO: WIP
    
    from annoy import AnnoyIndex

    class AnnoyNN(NearestNeighborsBase):
        """
        Dense nearest-neighbors lookup index.
        """
        def __init__(self):
            assert False,'WIP'
        
        def create_index(self, n_dims):
            """
            args:
                n_dims:  Integer dimensionality of the dense vectors. E.g. 300.
            """
            self.ann = AnnoyIndex(n_dims)
        
        def delete_index(self, *args, **kw):
            pass
        
        def refresh_index(self, *args, **kw):
            self.ann.build(-1)
        
        def scan_all(self, *args, **kw):
            raise NotImplementedError
        
        def parallel_bulk(self,
                          the_matrix,
                          *args, **kw):
            """
            Parallel inserting interface, reminiscent of the elasticsearch interface.

            TODO: support plain matrix batches too.
            """

            assert False,'WIP'
            
            for hh in the_iter:

                print 'NON_PARALLEL_BULK',hh

                xaction = hh['_op_type']
                xindex = hh['_index']
                xtype = hh['_type']
                xid = hh['_id']

                for k,v in hh.items():
                    if k.startswith('_'):
                        del hh[k]

                print 'BODY',hh

                if xaction == 'index':
                    self.ann.add_item(0, [1, 0, 0])
                else:
                    raise NotImplementedError

                print 'DONE-NON_PARALLEL_BULK',xaction,xid

                yield True,res

                try:
                    self.es.indices.refresh(index = xindex)
                except:
                    print 'REFRESH_ERROR'

        def search_full_text(self, *args, **kw):
            raise NotImplementedError

        def search_terms(self, *args, **kw):
            raise NotImplementedError

        def search_ids(self, *args, **kw):
            raise NotImplementedError

        def count(self, *args, **kw):
            raise NotImplementedError

    
class ElasticSearchNN(NearestNeighborsBase):
    """
    ElasticSearch-based nearest neighbors index.
    """
    def __init__(self,
                 use_simulator = False,
                 use_custom_parallel_bulk = False,
                 index_name = mc_config.MC_TEST_INDEX_NAME,
                 doc_type = mc_config.MC_TEST_DOC_TYPE,
                 index_settings = {}
                 ):
        """
        Args:
            use_simulator:             Whether to use our simulator. TODO: not yet feature-complete.
            use_custom_parallel_bulk:  Whether to use our customized parallel_bulk function, or the default ES function.
            index_name:                ES index name.
            doc_type:                  ES doc type.
        """
        
        if use_simulator:
            self.es = ElasticSearchEmulator()
        else:
            self.es = Elasticsearch()

        ## TODO: be more flexible:
        try:
            assert self.es.cluster.stats()['nodes']['versions'][0] == u'2.3.2'
        except:
            raise 'INCORRECT_ES_VERSION - Requires ElasticSearch Version 2.3.2'
        
        self.index_name = index_name
        self.doc_type = doc_type

        self.use_custom_parallel_bulk = use_custom_parallel_bulk
    
    def _create_index(self,
                      ):
        """
        Create and / or setup new index. For ES, this involves schema mapping setup.
        """
        self.es.indices.create(index = self.index_name,
                               body = self.index_settings,
                               #ignore = 400, # ignore already existing index
                               )

    
    def delete_index(self,):
        """
        Delete index if exists.
        """
        if self.es.indices.exists(self.index_name):
            print ('DELETE_INDEX...', self.index_name)
            self.es.indices.delete(index = self.index_name)
            print ('DELETED')

    def refresh_index(self,):
        self.es.indices.refresh(index = self.index_name)

    def _non_parallel_bulk(self,
                           the_iter,
                           *args,
                           **kw):
        """
        Custom bulk inserter which immediately inserts & refreshes the index. Unlike the default `parallel_bulk`,
        which does not flush until `buf_size` records have been yielded from the iterator.
        
        TODO: scale better.
        """
        
        for hh in the_iter:

            print 'NON_PARALLEL_BULK',hh
            
            xaction = hh['_op_type']
            xindex = hh['_index']
            xtype = hh['_type']
            xid = hh['_id']

            for k,v in hh.items():
                if k.startswith('_'):
                    del hh[k]
                        
            print 'BODY',hh

            if xaction == 'index':
                res = self.es.index(index = xindex, doc_type = xtype, id = xid, body = hh)
            elif xaction == 'update':
                res = self.es.update(index = xindex, doc_type = xtype, id = xid, body = hh)
            else:
                assert False,repr(xaction)
            
            print 'DONE-NON_PARALLEL_BULK',xaction,xid
            
            yield True,res

            try:
                self.es.indices.refresh(index = xindex)
            except:
                print 'REFRESH_ERROR'
            
            try:
                import mc_models
                for name in mc_models.VECTORS_MODEL_NAMES:
                    mc_models.dedupe_reindex(index_name = xindex,
                                             doc_type = xtype,
                                             vectors_model = name,
                                             )
            except:
                print 'REINDEX_ERROR'
            
            print 'REFRESHED'
        
        print 'EXIT-LOOP_NON_PARALLEL_BULK'
    
    def parallel_bulk(self,
                      the_iter,
                      *args,
                      **kw):
        
        if self.use_custom_parallel_bulk:
            return self._non_parallel_bulk(the_iter,
                                           *args,
                                           **kw)
        
        else:
            return es_parallel_bulk(self.es,
                                    the_iter,
                                    *args,
                                    **kw)


    def scan_all(self,
                 scroll = '5m', #TODO - hard coded timeout.
                 ):
        """
        Most efficient way to scan all documents.
        """

        rr = es_scan(client = self.es,
                     index = self.index_name,
                     doc_type = self.doc_type,
                     scroll = scroll,
                     query = {"query": {'match_all': {}}},
                     )

        return rr

        
    def search_full_text(self,
                         q_text,
                         ):
        """
        Full text search.

        Args:
            q_text: text string to search for.
        """
        
        query = {"query": {"multi_match": {"query":    q_text,
                                           "fields": [ "*" ],
                                           "type":     "cross_fields"
                                           },
                           },
                 }
        
        rr = self.es.search(index = self.index_name,
                            type = self.doc_type,
                            source = {"query": query},
                            )
        
        return rr
        
    def search_terms(self,
                     terms,
                     **kw):
        """
        Search based on terms, without applying the full text search analyzers.
        Does use tf-IDF for ranking.
        
        Args:
            terms: Dict of the form `{'field_name': 'field_value', ...}`.
        """
        
        query = {"constant_score":{"filter":{"term": terms}}}
        
        rr = self.es.search(index = self.index_name,
                            type = self.doc_type,
                            source = {"query": query},
                            **kw)
        
        return rr

    def search_ids(self,
                   ids,
                   ):
        """
        Search based on list of `_id`s.
        
        Args:
            ids: list of string ids.
        
        """
        
        query = {"query":{ "ids": { "values": ids } } }

        rr = self.es.search(index = self.index_name,
                            type = self.doc_type,
                            source = {"query": query},
                            )
        
        return rr
    
    def count(self):
        return self.es.count(self.index_name)['count']


def fake_async_decorator(ocls):
    """
    Wrap all non-private methods of a class with the tornado Task decorator.
    
    Used to simulate the interface of async methods, but with methods that actually block.
    
    TODO: Test.
    """
    import tornado
    import tornado.gen
    
    cls = type(ocls.__name__ + 'Async', ocls.__bases__, dict(ocls.__dict__))
    
    for attr in cls.__dict__:
        if callable(getattr(cls, attr)) and (not attr.startswith('_')):
            
            def fake(*args, **kw):
                yield tornado.gen.Task(getattr(cls, attr)(*args, **kw))
            
            setattr(cls, attr, fake)
    
    return cls    

NearestNeighborsBaseAsync = fake_async_decorator(NearestNeighborsBase)



def low_level_es_connect():
    """
    Central point for creating new index-backend connections.
    
    Note: Only hyper-parameter optimization should use this low-level interface now.
          Everything else should use `high_level_connect`.
    """
    
    print ('LOW_LEVEL_CONNECTING...')
    es = Elasticsearch()
    print ('LOW_LEVEL_CONNECTED')
    return es


def high_level_connect(**kw):
    """
    Central point for creating new index-backend connections.
    """
    
    print ('HIGH_LEVEL_CONNECTING...')
    
    if ('async' in kw) and (kw['async']):
        nes = NearestNeighborsESAsync(**kw)
    else:
        nes = NearestNeighborsES(**kw)
        
    print ('HIGH_LEVEL_CONNECTED')
    
    return nes
