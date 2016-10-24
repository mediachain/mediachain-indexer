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
from random import shuffle

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

ranking_prebuilt_equations = {
    'tfidf':"item['_score']",
    #'harmonic_mean_score_comments':"(item['_score'] * item['num_comments']) / (item['_score'] + item['num_comments'])",
    'boost_pexels':"item['_score'] * (item['_source'].get('native_id','').startswith('pexels') and 2 or 1) * item['_source'].get('boosted', 0.1)",
    #'aesthetics':aes_func,
    'pure_object':"(item['_source'].get('aesthetics', {}).get('object', -1.0) + 1) * ((item['_source'].get('max_width', 0) > 300) and 10 or 1)",
    'pure_balance':"(item['_source'].get('aesthetics', {}).get('balance', -1.0) + 1) * ((item['_source'].get('max_width', 0) > 300) and 10 or 1)",
    'pure_color_harmony':"(item['_source'].get('aesthetics', {}).get('color_harmony', -1.0) + 1) * ((item['_source'].get('max_width', 0) > 300) and 10 or 1)",
    'pure_content':"(item['_source'].get('aesthetics', {}).get('content', -1.0) + 1) * ((item['_source'].get('max_width', 0) > 300) and 10 or 1)",
    'pure_depth_of_field':"(item['_source'].get('aesthetics', {}).get('depth_of_field', -1.0) + 1) * ((item['_source'].get('max_width', 0) > 300) and 10 or 1)",
    'pure_lighting':"(item['_source'].get('aesthetics', {}).get('lighting', -1.0) + 1) * ((item['_source'].get('max_width', 0) > 300) and 10 or 1)",
    'pure_motion_blur':"(item['_source'].get('aesthetics', {}).get('motion_blur', -1.0) + 1) * ((item['_source'].get('max_width', 0) > 300) and 10 or 1)",
    'pure_repetition':"(item['_source'].get('aesthetics', {}).get('repetition', -1.0) + 1) * ((item['_source'].get('max_width', 0) > 300) and 10 or 1)",
    'pure_rule_of_thirds':"(item['_source'].get('aesthetics', {}).get('rule_of_thirds', -1.0) + 1) * ((item['_source'].get('max_width', 0) > 300) and 10 or 1)",
    'pure_symmetry':"(item['_source'].get('aesthetics', {}).get('symmetry', -1.0) + 1) * ((item['_source'].get('max_width', 0) > 300) and 10 or 1)",
    'pure_vivid_color':"(item['_source'].get('aesthetics', {}).get('vivid_color', -1.0) + 1) * ((item['_source'].get('max_width', 0) > 300) and 10 or 1)",
    'pure_aesthetics_g':"(item['_source'].get('score_global', -1.0) + 1) * ((item['_source'].get('max_width', 0) > 300) and 10 or 1)",
    'pure_aesthetics_1':"(item['_source'].get('aesthetics', {}).get('score', -1.0) + 1) * ((item['_source'].get('max_width', 0) > 300) and 10 or 1)",
    'pure_aesthetics_2':"(item['_source'].get('score_aesthetics_2', -1.0)) * ((item['_source'].get('max_width', 0) > 300) and 10 or 1)",
    'pure_aesthetics_3':"(item['_source'].get('score_aesthetics_3', -1.0)) * ((item['_source'].get('max_width', 0) > 300) and 10 or 1)",
    'pure_aesthetics_1+2':"((item['_source'].get('score_global', 0.0)) * (item['_source'].get('score_aesthetics_2', -1.0))) + ((item['_source'].get('score_global', 0.0)) + (item['_source'].get('score_aesthetics_2', -1.0)) + 0.0000001)",
    'pure_aesthetics_1+3':"((item['_source'].get('score_global', 0.0)) * (item['_source'].get('score_aesthetics_3', -1.0))) + ((item['_source'].get('score_global', 0.0)) + (item['_source'].get('score_aesthetics_3', -1.0)) + 0.0000001)",
    'pure_aesthetics_1+2+3':"((item['_source'].get('score_global', 0.0)) * (item['_source'].get('score_aesthetics_2', 0.0)) * (item['_source'].get('score_aesthetics_3', 0.0))) / ((item['_source'].get('score_global', 0.0)) + (item['_source'].get('score_aesthetics_2', 0.0)) + (item['_source'].get('score_aesthetics_3', 0.0)))",
    #'balance':"""(item['_source'].get('aesthetics', {}).get('balance', -1.0) + 1) * ((item['_source'].get('max_width', 0) > 300) and 10 or 1)""",
    #'color_harmony':"""(item['_source'].get('aesthetics', {}).get('color_harmony', -1.0) + 1) * ((item['_source'].get('max_width', 0) > 300) and 10 or 1)""",
    #'lighting':"""(item['_source'].get('aesthetics', {}).get('lighting', -1.0) + 1)  * ((item['_source'].get('max_width', 0) > 300) and 10 or 1)""",
    #'motion':"""(item['_source'].get('aesthetics', {}).get('motion_blur', -1.0) + 1)  * ((item['_source'].get('max_width', 0) > 300) and 10 or 1)""",
    #'repetition':"""(item['_source'].get('aesthetics', {}).get('repetition', -1.0) + 1) * ((item['_source'].get('max_width', 0) > 300) and 10 or 1)""",
    #'vivid_color':"""(item['_source'].get('aesthetics', {}).get('vivid_color', -1.0) + 1) * ((item['_source'].get('max_width', 0) > 300) and 10 or 1)""",
    #'symmetry':"""(item['_source'].get('aesthetics', {}).get('symmetry', -1.0) + 1) * ((item['_source'].get('max_width', 0) > 300) and 10 or 1)""",
    #'uses_depth_of_field':"""(item['_source'].get('aesthetics', {}).get('depth_of_field', -1.0) + 1) * ((item['_source'].get('max_width', 0) > 300) and 10 or 1)""",
    #'object_focus':"""(item['_source'].get('aesthetics', {}).get('object', -1.0) + 1) * ((item['_source'].get('max_width', 0) > 300) and 10 or 1)""",
    'like_unsplash_v1':"""(item['_source'].get('aes_unsplash_out_v1', {}).get('like_unsplash', -1.0)) * ((item['_source'].get('max_width', 0) > 300) and 10 or 1)""",
    'like_flickr_v1':"""(item['_source'].get('aes_unsplash_out_v1', {}).get('like_flickr', -1.0)) * ((item['_source'].get('max_width', 0) > 300) and 10 or 1)""",
    'neural_relevance':"""item.get('_neural_rel_score', 0)""",
    'neural_hybrid':"""(item.get('_neural_rel_score', 0) * ((item['_source'].get('score_global', -1.0) + 1) / 2.0)) / (item.get('_neural_rel_score', 0) * 0.5 + ((item['_source'].get('score_global', -1.0) + 1) / 2.0) + 0.0000001) + (item['_score'] / 50)""",
    'neural_hybrid_switch_old':"""(item['_switch_score'] * ((item['_source'].get('aesthetics',{}).get('score', -1.0) + 1) / 2.0)) / (item.get('_switch_score', 0) * 1.0 + ((item['_source'].get('aesthetics',{}).get('score', -1.0) + 1) / 2.0) + 0.0000001)""",
    'neural_hybrid_switch':"""(item['_switch_score'] * ((item['_source'].get('score_global', -1.0) + 1) / 2.0)) / (item.get('_switch_score', 0) * 1.0 + ((item['_source'].get('score_global', -1.0) + 1) / 2.0) + 0.0000001)""",
    'neural_hybrid_switch_1+2':"(item['_switch_score'] * (((item['_source'].get('score_global', 0.0)) * (item['_source'].get('score_aesthetics_2', 0.0))) / ((item['_source'].get('score_global', 0.0)) + (item['_source'].get('score_aesthetics_2', 0.0))))) / (item.get('_switch_score', 0) * 1.0 + (((item['_source'].get('score_global', 0.0)) * (item['_source'].get('score_aesthetics_2', 0.0))) / ((item['_source'].get('score_global', 0.0)) + (item['_source'].get('score_aesthetics_2', 0.0)))) + 0.0000001)",
    'neural_hybrid_switch_1+3':"(item['_switch_score'] * (((item['_source'].get('score_global', 0.0)) * (item['_source'].get('score_aesthetics_3', 0.0))) / ((item['_source'].get('score_global', 0.0)) + (item['_source'].get('score_aesthetics_3', 0.0))))) / (item.get('_switch_score', 0) * 1.0 + (((item['_source'].get('score_global', 0.0)) * (item['_source'].get('score_aesthetics_3', 0.0))) / ((item['_source'].get('score_global', 0.0)) + (item['_source'].get('score_aesthetics_3', 0.0)))) + 0.0000001)",
    'neural_hybrid_2':"""(item['_norm_score'] * item['_norm_neural_rel_score'] * item['_norm_aesthetics_score']) / ( item['_norm_score'] + item['_norm_neural_rel_score'] + item['_norm_aesthetics_score'] + 0.0000001)""",
    'neural_hybrid_3':"""(item['_total_rel'] * item['_norm_aesthetics_score']) / ( item['_total_rel'] + item['_norm_aesthetics_score'] + 0.0000001)""",
    'annotation_mode':'annotation_mode',
}

"""
((item['_source'].get('score_global', -1.0) + 1) / 2.0)
((item['_source'].get('score_global', -1.0) + 1) / 2.0)

(item['_switch_score'] * (xx)) / (item.get('_switch_score', 0) * 1.0 + (yy) + 0.0000001)

(item['_switch_score'] * (((item['_source'].get('score_global', 0.0)) * (item['_source'].get('score_aesthetics_2', 0.0))) / ((item['_source'].get('score_global', 0.0)) + (item['_source'].get('score_aesthetics_2', 0.0))))) / (item.get('_switch_score', 0) * 1.0 + (((item['_source'].get('score_global', 0.0)) * (item['_source'].get('score_aesthetics_2', 0.0))) / ((item['_source'].get('score_global', 0.0)) + (item['_source'].get('score_aesthetics_2', 0.0)))) + 0.0000001)

=======



"""
import scipy.stats


class ReRankingBasic():
    """ See: __init__() """
    
    def __init__(self,
                 first_pass_eq_name = None,
                 eq_name = None,
                 default_eq_name = 'aesthetics',
                 verbose = False,
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

        self.eq_name = eq_name
            
        if verbose:
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
        
            
    def rerank(self,
               the_query,
               items,
               is_debug_mode = False,
               skip_incomplete = False,
               verbose = False,
               **kw):
        """
        Re-rank items according to new score output by `self.eq`.

        Args:
            items:            Input items, in elasticsearch JSON format.
            is_debug_mode:    Save more detailed stats.
            skip_incomplete:  Skip items for which there's incomplete data.
        """
        t0 = time()
                            

        
        ## Switch point scores:
        
        if True:
            if not (self.eq_name or '').startswith('pure_'):
                switch_point = 1.70

                if not items:
                    maxtfidf = 1.0 ##TODO??
                    mintfidf = 0.0
                else:
                    max_tfidf = max([x['_score'] for x in items]) or 0.000000001
                
                for c, item in enumerate(items):
                    nrs = item.get('_neural_rel_score',-1)
                    #if 'score_global' in item['_source']:
                    #    print ('FIX')
                    #    item['_source']['score_global'] -= item['_source'].get('score_global_boost', 0.0)
                    
                    if nrs > switch_point:
                        item['_switch_score'] = nrs
                        if (verbose and (c < 10)):# or (item['_id'] == '938a86d2f8cafbb9f0c9d70339b3eede'):
                            print ('NO_SWITCH',item['_switch_score'], 'nrs:', nrs, item['_id'])
                    else:
                        ## Gap puts all highly-relevant ahead of lower-relevant with good aesthetics:
                        item['_switch_score'] = (item['_score'] or 0.0) * (switch_point / max_tfidf) - 0.2 - item['_source'].get('score_global_boost', 0.0)
                        assert item['_switch_score'] <= switch_point, (item['_score'], max_tfidf)
                        if (verbose and (c < 10)):# or (item['_id'] == '938a86d2f8cafbb9f0c9d70339b3eede'):
                            print ('YES_SWITCH',item['_switch_score'], self.eq_name, 'nrs:', nrs, item['_id'])
                            
                
        ## Skip incomplete
        if skip_incomplete:
            
            rr = []
            for item in items:
                if '_neural_rel_score' not in item:
                    if verbose:
                        print 'NO_REL'
                    continue
                #if 'score' not in item['_source'].get('aesthetics',{}):
                if 'global_score' not in item['_source']:
                    if verbose:
                        print 'NO_SCORE'
                    continue
                #if 'max_width' not in item['_source']:
                #    continue
                rr.append(item)

            if verbose:
                print ('SKIP_INCOMPLETE',len(items), '->', len(rr))
            
            items = rr
            

        
        self.aeval.symtable['items'] = items
        
        ## Some default first-pass stuff:

        #for item in items:
        #    ## NEW, set aesthetics to at least 0.5:
        #    if item['_source'].get('aesthetics', {}).get('score', False) is False:
        #        item['_source']['aesthetics'] = {'score':0}
        #    item['_source']['aesthetics']['score'] = max(0.5, item['_source']['aesthetics']['score'])

        
        if False: ## use score_global now instead:
            ## aesthetics rerank:
            items_sorted = [b for a,b in sorted([((x['_source'].get('score_global', 0), x.get('_score',0)), x) for x in items], reverse = True)]
            
            num_promoted = 0
            rr = []
            for item in items_sorted:
                if ('aesthetics' in item['_source']) and ('score' in item['_source']['aesthetics']):
                    if num_promoted >= 10:
                            rr.append(item)
                            continue
                    
                    if item['_source']['source_dataset'] in ['500px', 'pexels']:
                        item['_source']['aesthetics']['score'] += 0.1
                        num_promoted += 1
                        
                    if item['_source']['source']['name'] == 'unsplash.com':
                        #print ('--------BOOST_UNSPLASH')
                        item['_source']['aesthetics']['score'] += 0.1
                        #num_promoted += 1
                        
                    if item['_source']['source_dataset'] in ['dpla']:
                        item['_source']['aesthetics']['score'] -= 1.0

                    rr.append(item)

        if False:
            ## aesthetics rerank:
            """
            1) Get aesthetics @ 20 for biggest source.
            2) Re-size all others so their @20 is the same
            3) Sort.
            """
            if len(items) > 15:
                
                use_score_at = 20
                
                items_sorted = [b for a,b in sorted([((x['_source'].get('score_global', 0), x.get('_score',0)), x) for x in items], reverse = True)]
                
                sources = Counter([x['_source']['source_dataset'] for x in items_sorted])
                top_source = sources.most_common(1)[0]
                by_source = {x:[] for x in sources}
                scores_at_20 = {}
                
                for item in items_sorted:
                    by_source[item['_source']['source_dataset']].append(item)
                
                ## get score @ 20 of all sources, or closest to 20 as possible:
                
                top_score_20 = False
                
                for the_source_name, the_source in by_source.iteritems():
                    for aa in range(use_score_at, use_score_at / 2, -1):
                        xx = the_source[aa]
                        if 'aesthetics' in xx:
                            xscore = xx['_source']['aesthetics']['score']
                            scores_at_20[the_source] = xscore
                            if the_source == top_source:
                                top_score_20 = xscore
                            break
                
                if top_score_20 is not False:

                    ## rerank all by score @ 20 of top:

                    for the_source_name, the_source in by_source.iteritems():
                        pass
                    
                    
                    
                    
                        
        for item in items:
            item['_aesthetics_score'] = item['_source'].get('aesthetics',{}).get('score', 'EMPTY')
            #item['_aesthetics_score'] = item['_source'].get('score_global', 'EMPTY')
            item['_neural_rel_score'] = item.get('_neural_rel_score', 'EMPTY')

            
        for score_key in ['_score', '_neural_rel_score', '_aesthetics_score']:
            
            max_tfidf = -1000
            min_tfidf = 1000
            
            got_any = False
            
            for item in items:
                if item[score_key] == 'EMPTY':
                    continue
                min_tfidf = min(item[score_key], min_tfidf)
                max_tfidf = max(item[score_key], max_tfidf)
                got_any = True

            #if not got_any:
            #    continue

            #print ('BB', score_key, min_tfidf, max_tfidf)
                
            min_tfidf = float(min_tfidf)
            max_tfidf = float(max_tfidf)

            self.aeval.symtable['min_tfidf'] = min_tfidf
            self.aeval.symtable['max_tfidf'] = max_tfidf

            for item in items:

                new_max = 1.0
                new_min = 0.0

                ## less important:
                #if score_key != '_aesthetics_score':
                #    new_min = 0.5
                
                if item[score_key] == 'EMPTY':
                    item[score_key] = False #min_tfidf
                
                #print ('AA', item[score_key], min_tfidf, max_tfidf)
                
                if (max_tfidf - min_tfidf) == 0:
                    item['_norm' + score_key] = 0.0 # was 1.0
                else:
                    item['_norm' + score_key] = (((item[score_key] - min_tfidf) * (new_max - new_min)) / (max_tfidf - min_tfidf)) + min_tfidf

        for item in items:
            #print ('CC', [(score_key, item[score_key]) for score_key in ['_score', '_neural_rel_score', '_aesthetics_score']])
            item['_total_rel'] = max(item['_norm_score'], item['_norm_neural_rel_score'])
                    
                    
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
        
        if self.eq == 'annotation_mode':
            
            ### Debug mode:
            ## 1) Get top-50 and bottom-50 from: tfidf, neural_hybrid, neural_relevance, aesthetics_pure
            ## 2) Mixrank
            ## 3) Filter dupes
                        
            rr4 = []
            for xeq_name in ['tfidf', 'neural_hybrid', 'neural_relevance', 'pure_aesthetics']:
                
                xeq = ranking_prebuilt_equations[xeq_name]
                
                rr = []
                
                for item in items:
                    self.aeval.symtable['item'] = item
                    rr.append((self.aeval(xeq), item))
                
                rr = list(sorted(rr, reverse = True))
                
                rr4.append(rr[:50])
                rr4.append(rr[-50:][::-1])

            shuffle(rr4)
                
            rr = [item for sublist in zip(*rr4) for item in sublist]
                        
            r5 = []
            done_ids = set()
            for new_score, item in rr:
                if item['_id'] in done_ids:
                    continue
                r5.append((new_score, item))
                done_ids.add(item['_id'])
                #if len(r5) == 50:
                #    break
            rr = r5
                    
        else:
            
            ## Normal mode:

            if self.eq_name == 'neural_hybrid_switch':                
                rr = [((item['_switch_score'] * ((item['_source'].get('score_global', -1.0) + 1) / 2.0)) / (item.get('_switch_score', 0) * 1.0 + ((item['_source'].get('score_global', -1.0) + 1) / 2.0) + 0.0000001), item) for item in items]
                
            else:
                for item in items:
                    self.aeval.symtable['item'] = item
                    new_score = self.aeval(self.eq)
                    rr.append((new_score, item))


        if the_query.lower().strip() in ['technology','design','social media','privacy','bitcoin','internet of things','self driving cars','movies','television','music','gaming','politics','government','2016 election','business','finance','economics','investing','creativity','ideas','humor','future','inspiration','travel','photography','architecture','art','climate change','transportation','sustainability','energy','health','mental health','psychology','science','education','history','space','virtual reality','artificial intelligence','feminism','women in tech','sports','nba','nfl','life lessons','productivity','self improvement','parenting','advice','startup','startups','venture capital','entrepreneurship','leadership','culture','fashion','life','reading','relationships','this happened to me','diversity','racism','lgbtq','blacklivesmatter','fiction','books','poetry','satire','short story','food','future of food','cooking','writing','innovation','journalism']:
            rr2 = []
            for xscore, xitem in rr:
                if xitem['_source']['source_dataset'] in ['pexels']:
                    xscore += 1000
                    #print ('BOOST_PEX',xscore, xitem['_id'])
                    #sleep(1)
                rr2.append((xscore, xitem))
            rr = rr2
        #else:
        #    assert False, repr(the_query)
                    

        rrr = []

        if self.eq == 'annotation_mode':
            #shuffle(rr)
            pass
        else:
            rr = sorted(rr, reverse = True)
        
        for c,(new_score,item) in enumerate(rr):
            item['_score'], item['score_old'] = new_score, item['_score']
            
            if is_debug_mode == 2:
                item['debug_info']['score_old'] = item['score_old']
                
                item['debug_info']['score_post_rerank'] = item['_score']
                
                item['debug_info'].update({('aes_' + x):y for x,y in item['_source'].get('aesthetics', {}).items()})

            else:
                item['_score'] = new_score
                        
            if verbose and (c <= 10):
                #print ('RERANK', item['_old_score'], item['_source'].get('boosted'),item['_score'],item['_source'].get('native_id'),item['_source'].get('title'))
                print ('RERANK',
                       item['_id'],
                       'glob:', item['_source'].get('score_global', None),
                       'aes:', item['_source'].get('aesthetics', {}).get('score', None),
                       'rel:', item['_neural_rel_score'],
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

        if verbose:
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

