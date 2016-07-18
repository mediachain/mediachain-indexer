#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Models for search re-ranking.

NOTE: See here for the restrictions / features of asteval: https://newville.github.io/asteval/basics.html

Equation `eq` can use any features provided by asteval, in addition to numpy via `np`.
"""

from mc_generic import setup_main, group, raw_input_enter, pretty_print, intget, print_config, sleep_loud

from asteval import Interpreter
import numpy as np
import math
from time import time

## Re-ranking equations that have names:

ranking_basic_equations = {'noop':"item['_score']",
                           'harmonic_mean_score_comments':"(item['_score'] * item['num_comments']) / (item['_score'] + item['num_comments'])",
                           'boost_pexels':"item['_score'] * (item['_source'].get('native_id','').startswith('pexels') and 2 or 1) * item['_source'].get('boosted', 0.1)",
                           }

class ReRankingBasic():
    """
    Basic search results re-ranking model. Allows you to specify a simple custom re-ranking equation.
    
    NOTE: See here for the restrictions / features of asteval: https://newville.github.io/asteval/basics.html
    
    Equation `eq` can use any features provided by asteval, in addition to numpy via `np`.
    """
    
    def __init__(self,
                 eq_name = None,
                 ):
        
        if not eq_name:
            eq_name = 'boost_pexels'
        
        if eq_name in ranking_basic_equations:
            eq = ranking_basic_equations[eq_name]
        else:
            eq = eq_name
                    
        self.eq = eq
        
        self.aeval = Interpreter()
        
        ## Access all functions of these modules:
        
        ## Turns out that asteval already imports all of `math`.
        
        for mod in []:
            for attr in dir(mod):
                if attr.startswith('_'):
                    continue
                self.aeval.symtable[attr] = getattr(math, attr)

        ## numpy:
        
        self.aeval.symtable['np'] = np
    
    def rerank(self, items, **kw):
        """
        Re-rank items according to new score output by `self.eq`.
        """
        t0 = time()
        self.aeval.symtable['items'] = items

        rr = []
        
        for item in items:
            self.aeval.symtable['item'] = item
            new_score = self.aeval(self.eq)
            rr.append((new_score, item))

        rrr = []
        
        for c,(new_score,item) in enumerate(sorted(rr, reverse = True)):
            item['_score'], item['_old_score'] = new_score, item['_score']

            #if c <= 20:
            #    print ('RERANK', item['_old_score'], item['_source'].get('boosted'),item['_score'],item['_source'].get('native_id'),item['_source'].get('title'))
            
            rrr.append(item)
        
        ## TODO: normalize `_score`s?

        print ('RE-RANK TIME',time() - t0)
        
        return rrr



def test_reranking(via_cli = False):

    items = [{'name':'a', '_score':1, 'num_comments':5},
             {'name':'b', '_score':2, 'num_comments':4 },
             {'name':'c', '_score':3, 'num_comments':3 },
             {'name':'d', '_score':4, 'num_comments':2 },
             ]
    
    mm = ReRankingBasic(eq = "(item['_score'] * item['num_comments']) / (item['_score'] + item['num_comments'])")
    
    print mm.rerank(items)



functions=['test_reranking',
           ]

def main():
    setup_main(functions,
               globals(),
                'mediachain-indexer-ingest',
               )

if __name__ == '__main__':
    main()

