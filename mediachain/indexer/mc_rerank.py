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

class ReRankingBasic():
    """
    Basic search results re-ranking model. Allows you to specify a simple custom re-ranking equation.
    
    NOTE: See here for the restrictions / features of asteval: https://newville.github.io/asteval/basics.html
    
    Equation `eq` can use any features provided by asteval, in addition to numpy via `np`.
    """
    
    def __init__(self,
                 eq = "item['_score']",
                 ):
        
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

        self.aeval.symtable['items'] = items

        rr = []
        
        for item in items:
            self.aeval.symtable['item'] = item
            new_score = self.aeval(self.eq)
            rr.append((new_score, item))

        rrr = []
        
        for score,item in sorted(rr, reverse = True):
            item['_score'], item['_old_score'] = score, item['_score']
            rrr.append(item)
        
        ## TODO: normalize scores?
            
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

