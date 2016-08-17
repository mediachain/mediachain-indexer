#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Models for search re-ranking.

Equation `eq_name` can use any features provided by asteval, in addition to numpy via `np`.

NOTE: Certain names cannot be used within the asteval interpreter:

    and, as, assert, break, class, continue, def, del, elif, else, except, exec, finally, for, from, global,
    if, import, in, is, lambda, not, or, pass, print, raise, return, try, while, with, True, False, None, eval,
    execfile, __import__, __package__

See: https://newville.github.io/asteval/basics.html
"""

from mc_generic import setup_main, group, raw_input_enter, pretty_print, intget, print_config, sleep_loud

from asteval import Interpreter
import numpy as np
import math
from time import time
from collections import Counter


#### Re-ranking equations that have names:

## Harmonic mean of (tf-IDF query relevance) and (general aesthetics) scores. Higher is better -
## 
## NOTES:
##   - 0.4 was chosen for the not-yet-aesthetically analyzed items, because that emperically puts it
##     above the peaks of the bad library datasets but below the high-aesthetics datasets. See the histograms.
##
## TODO:
##   - Remap scores to percentiles? May be bad idea.
##   - Revert back to putting aesthetically scored images at the very bottom, instead of at 0.4?
##   - Consider image resolution

aes_func = \
"""
asc = (item['_source'].get('aesthetics', {}).get('score', -1.0) + 1) * 3.0
rsc = (asc * item['_score']) / (asc + item['_score'])
rsc *= (item['_source'].get('max_width', 0) > 300) and 10 or 1
rsc
"""

#rsc *= ((item['_source'].get('native_id','').startswith('dpla') and 1 or 2))


ranking_prebuilt_equations = {'tfidf':"item['_score']",
                              #'harmonic_mean_score_comments':"(item['_score'] * item['num_comments']) / (item['_score'] + item['num_comments'])",
                              'boost_pexels':"item['_score'] * (item['_source'].get('native_id','').startswith('pexels') and 2 or 1) * item['_source'].get('boosted', 0.1)",
                              'aesthetics':aes_func,
                              'aesthetics_pure':"item['_source'].get('aesthetics', {}).get('score', -1.0) + 1",
                              'balance':"""(item['_source'].get('aesthetics', {}).get('balance', -1.0) + 1) """,
                              'color_harmony':"""(item['_source'].get('aesthetics', {}).get('color_harmony', -1.0) + 1) """,
                              'lighting':"""(item['_source'].get('aesthetics', {}).get('lighting', -1.0) + 1) """,
                              'motion':"""(item['_source'].get('aesthetics', {}).get('motion_blur', -1.0) + 1) """,
                              'repetition':"""(item['_source'].get('aesthetics', {}).get('repetition', -1.0) + 1) """,
                              'vivid_color':"""(item['_source'].get('aesthetics', {}).get('vivid_color', -1.0) + 1) """,
                              'symmetry':"""(item['_source'].get('aesthetics', {}).get('symmetry', -1.0) + 1) """,
                              'uses_depth_of_field':"""(item['_source'].get('aesthetics', {}).get('depth_of_field', -1.0) + 1) """,
                              'object_focus':"""(item['_source'].get('aesthetics', {}).get('object', -1.0) + 1) """,
                              'aes_unsplash_out_v1':"""(item['_source'].get('aes_unsplash_out_v1', {}).get('like_unsplash', -1.0))""",
                              }


class ReRankingBasic():
    """ See: __init__() """
    
    def __init__(self,
                 first_pass_eq_name = None,
                 eq_name = None,
                 default_eq_name = 'aesthetics',
                 ):
        """
        Basic search results re-ranking model. Allows you to specify a simple custom re-ranking equation.

        NOTE: See here for the restrictions / features of asteval: https://newville.github.io/asteval/basics.html

        Equation `eq_name` can use any features provided by asteval, in addition to numpy via `np`.

        Args:
            first_pass_eq_name:  (Optional) Equation run for first-pass, which can view the whole dataset.
            eq_name:             String name (TODO or python callable?) to use as the re-ranking equation. Operates per-item.

        TODO: Intentionally not supporting python callables for now. Strings only.
        """

        self.first_pass_eq_name = first_pass_eq_name
        
        if eq_name is None:
            ## Done this way, instead of default args on the function, so that mc_web can pass in `None` to indicate default:
            eq_name = default_eq_name

        print ('RERANK_EQUATION', first_pass_eq_name, eq_name)
        
        if eq_name in ranking_prebuilt_equations:
            eq = ranking_prebuilt_equations[eq_name]
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
        
            
    def rerank(self, items, is_debug_mode = False, **kw):
        """
        Re-rank items according to new score output by `self.eq`.
        """
        t0 = time()
        self.aeval.symtable['items'] = items

        ## First_pass:
        
        ## setup some storage containers, to help pass data to the 2nd pass:
        
        self.aeval.symtable['buf'] = []
        self.aeval.symtable['hh'] = {}
        self.aeval.symtable['cnt'] = Counter()
        
        self.aeval.symtable['items'] = items
        
        if self.first_pass_eq_name:
            self.aeval(self.first_pass_eq_name)
        
        ## Second pass:
        
        rr = []
        
        for item in items:
            self.aeval.symtable['item'] = item
            new_score = self.aeval(self.eq)
            rr.append((new_score, item))

        rrr = []
        
        for c,(new_score,item) in enumerate(sorted(rr, reverse = True)):
            item['_score'], item['score_old'] = new_score, item['_score']
            
            if is_debug_mode:
                item['debug_info']['score_old'] = item['score_old']
                
                item['debug_info']['score_post_rerank'] = item['_score']
                
                item['debug_info'].update({('aes_' + x):y for x,y in item['_source'].get('aesthetics', {}).items()})

            else:
                item['_score'] = new_score
                        
            if c <= 20:
                #print ('RERANK', item['_old_score'], item['_source'].get('boosted'),item['_score'],item['_source'].get('native_id'),item['_source'].get('title'))
                print ('RERANK',
                       'aes:', item['_source'].get('aesthetics', {}).get('score', None),
                       'tfidf:', item['score_old'],
                       'width:', item['_source'].get('max_width'), #item['_source'].get('sizes') and item['_source'].get('sizes')[0].get('width',0),
                       'final:', item['_score'],
                )
            
                
            rrr.append(item)
        
        ## TODO: normalize `_score`s?

        ## Delete temporary vars:
        
        del self.aeval.symtable['buf']
        del self.aeval.symtable['hh']
        del self.aeval.symtable['cnt']
        del self.aeval.symtable['items']
        
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

