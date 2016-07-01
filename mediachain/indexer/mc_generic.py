#!/usr/bin/env python

"""
Generic helper functions.
"""

import json
import fcntl, termios, struct
import sys
from time import time
import os
from os import system, walk, rename
from os.path import join, exists
import requests

def tcache(fn,
           func,
           *args,
           **kw):
    """
    Cache function output. Use temporary file to be semi-atomic.
    
    Usage:
        To cache output of `str(1234)` in file 'cache_file', do the following:
        
        d = tcache('cache_file', str, 1234)
    """
    
    if exists(fn):
        with open(fn) as f:
            d = f.read()
    
    else:
        fn_temp = fn + '.temp'
        
        d = func(*args, **kw)

        with open(fn_temp, 'w') as f:
            f.write(d)

        rename(fn_temp, fn)
    
    return d


def download_streamed(url,
                      fn,
                      chunk_size = 1024 * 1024,
                      headers = {'User-Agent':'Mediachain Indexer 1.0'},
                      use_temp = True,
                      skip_existing = True,
                      verbose = False,
                      ):
    """
    Streaming download with requests, memoization, progress, and temporary file.
    """
    
    if skip_existing and exists(fn):
        return
    
    if verbose:
        print 'DOWNLOAD',url,'->',fn
        
    if use_temp:
        xfn = fn + '.temp'
    else:
        xfn = fn

    r = requests.get(url,
                     verify = False,
                     headers = headers,
                     stream = True,
                     )

    nn = 0
    with open(xfn, 'w') as f:
        for chunk in r.iter_content(chunk_size = chunk_size): 
            if chunk: # filter out keep-alive new chunks
                nn += len(chunk)
                
                if verbose:
                    print 'WROTE','%.2fMB' % (nn / (1024 * 1024.0)),'->',xfn
                    
                f.write(chunk)

    if use_temp:
        print 'DONE_DOWNLOAD',xfn,'->',fn
        rename(xfn,
               fn,
               )


def walk_files(dd, max_num = 0):
    """
    Simpler walking of all files under a directory.
    """
    
    nn = 0
    for dir_name, subdir_list, file_list in walk(dd):
        for fn in file_list:
            nn += 1
            if max_num and (nn > max_num):
                return
            
            fn = join(dir_name,
                      fn,
                      )

            yield fn


def tarfile_extract_if_not_exists(fn_in,
                                  directory,
                                  ):
    """
    Extract files from tarfile, but skip files that already exist.
    """
    import tarfile
    
    with tarfile.open(fn_in) as tar:
        for name in tar.getnames():
            if exists(join(directory, name)):
                #print ('exists',name)
                pass
            else:
                tar.extract(name, path=directory)

def group(seq, size):
    """
    Group sequence `seq` into chunks of size `size`.
    """
    
    lenseq=len(seq)
    if not hasattr(seq, 'next'):
        seq = iter(seq)
    
    while True:
        if lenseq > size:
            yield [seq.next() for i in xrange(size)]
            lenseq-=size
        else:
            yield [i for i in seq]
            break


def pretty_print(val,
                 indent = 4,
                 max_indent_depth = False,
                 ):
    """
    Pretty print JSON, indenting up to a specified maximum depth.

    Args:
        val:              JSON input.
        indent:           Number of additional spaces to indent per layer of depth.
        max_indent_depth: Beyond this depth in the JSON structure, indentation doesn't get any greater.
    
    Use `max_indent_depth` for a middle-ground between no indentation, and excessive
    indentation of very deep JSON that overflows the width of the screen.
    """
        
    try:
        r = json.dumps(val,
                       indent = indent,
                       sort_keys = True,
                       )
    except:
        raise
        r = repr(val)
    
    if max_indent_depth is False:
        return r
    
    zz = ' ' * (indent * (max_indent_depth))
    
    xx = ' ' * (indent * (max_indent_depth + 1))
    
    rrr = []
    rr = []
    for line in r.splitlines():
        
        if line.startswith(xx) or (line.startswith(zz) and line.endswith('[')) \
           or (line.startswith(zz) and line.endswith('{')):
            
            if rr:
                line = line.strip()
            rr.append(line)            
        else:                        
            if rr:
                 line = line.strip()
            rr.append(line)            
            rrr.append(' '.join(rr))
            rr = []

    return '\n'.join(rrr)

def intget(x,
           default = False,
           ):
    try:
        return int(x)
    except:
        return default

def floatget(x,
             default = False,
             ):
    try:
        return float(x)
    except:
        return default

    
def raw_input_enter():
    print 'PRESS ENTER...'
    raw_input()


def ellipsis_cut(s,
                 n=60,
                 ):
    s=unicode(s)
    if len(s)>n+1:
        return s[:n].rstrip()+u"..."
    else:
        return s


def space_pad(s,
              n=20,
              center=False,
              ch = '.'
              ):
    if center:
        return space_pad_center(s,n,ch)    
    s = unicode(s)
    #assert len(s) <= n,(n,s)
    return s + (ch * max(0,n-len(s)))


def load_config(cfg,
                fn = False,
                hh = False,
                clear = False,
                ):
    """
    Load config from file_name or dict.
    """
    if fn:
        with open(fn) as f:
            hh = json.loads(f.read())
    if clear:
        cfg.clear()
    cfg.update(hh)


    
def config_env(cfg, glb):
    """
    Update config from environment variables. Convert types according suffixes:    
    
      '_INT'     = integer
      '_FLOAT'   = float
      '_JSON'    = JSON
      '_FJSON'   = JSON loaded from filename.
         *       = string

    Note: `_FJSON` values will override `_JSON` values.

    Also updates the passed globals(). Don't pass locals().
    """
    rh = {}
    for kg,vg in cfg.items():
        for k,(v,d) in vg.items():
            xx = os.environ.get(k,v)
            if k.endswith('_INT'):
                xx = intget(xx, v)
            elif k.endswith('_FLOAT'):
                xx = floatget(xx, v)
            elif k.endswith('_JSON'):
                xx = json.loads(xx) if xx else v
            elif k.endswith('_FJSON'):
                if xx:
                    with open(xx) as f:
                        dd = f.read()
                    xx = json.loads(dd.strip())
                else:
                    xx = v
            cfg[kg][k] = (xx,d)
            rh[k] = xx

    ## Override with values from files, if filenames passed:
    
    for kg,vg in cfg.items():
        for k,(v,d) in vg.items():
            xx = os.environ.get(k,v)
            if k.endswith('_FJSON'):
                if xx:
                    cfg[kg][k.replace('_FJSON','_JSON')] = 'OVERRIDDEN_FROM_FJSON:' + cfg[kg][k]
                    rh[k.replace('_FJSON','_JSON')] = rh[k]
    
    glb.update(rh)

    ## Load client config:
        
def print_config(cfg):
    """
    Pretty-print config of format {'section_title':{var_name:(var_value,'var_description')}}.
    """
    
    import numbers

    try:
        tw,th = terminal_size()
    except:
        tw,th = 80,40
    
    print
    print '### CONFIG:'

    max_name = min(50, max([len(y) for x in cfg.values() for y,z in x.items()]))
    max_val = min(50, max([len(repr(y)) for x in cfg.values() for y,z in x.items()]))
    
    for kg,vg in sorted(cfg.items()):
        
        print
        print '##',kg + ':'
        print
        
        for cc,(k,(v,d)) in enumerate(vg.items()):
            
            if d:
                if isinstance(d, (tuple, list)):
                    d = ' '.join(d)
                if cc:
                    print
                d += ':'
                for z in d.split('\n'):
                    print '  # ' + ('...\n  # '.join([''.join(x) for x in group(z, tw - 3)]))
                    
            
            print '  ' + space_pad(k, n=max_name, ch=' ') + '=',
            print space_pad(repr(v), n=max_val, ch=' '),
            if isinstance(v, numbers.Integral):
                print '<INT>  ',
            elif isinstance(v, float):
                print '<FLOAT>',
            elif True: #isinstance(v, basestring):
                print '<STR>  ',
            print
    print


def shell_source(fn_glob,
                 allow_unset = False,
                 ):
    """
    Source bash variables from file. Input filename can use globbing patterns.
    
    Returns changed vars.
    """
    import os
    from os.path import expanduser
    from glob import glob
    from subprocess import check_output
    from pipes import quote
    
    orig = set(os.environ.items())
    
    for fn in glob(fn_glob):
        
        fn = expanduser(fn)
        
        print ('SOURCING',fn)
        
        rr = check_output("source %s; env -0" % quote(fn),
                          shell = True,
                          executable = "/bin/bash",
                          )
        
        env = dict(line.split('=',1) for line in rr.split('\0'))
        
        changed = [x for x in env.items() if x not in orig]
        
        print ('CHANGED',fn,changed)

        if allow_unset:
            os.environ.clear()
        
        os.environ.update(env)
        print env
    
    all_changed = [x for x in os.environ.items() if x not in orig]
    return all_changed
    

def terminal_size():
    """
    Get terminal size.
    """
    h, w, hp, wp = struct.unpack('HHHH',fcntl.ioctl(0,
                                                    termios.TIOCGWINSZ,
                                                    struct.pack('HHHH', 0, 0, 0, 0),
                                                    ))
    return w, h

def usage(functions,
          glb,
          entry_point_name = False,
          ):
    """
    Print usage of all passed functions.
    """
    try:
        tw,th = terminal_size()
    except:
        tw,th = 80,40
                   
    print
    
    print 'USAGE:',(entry_point_name or ('python ' + sys.argv[0])) ,'<function_name>'
        
    print
    print 'Available Functions:'
    
    for f in functions:
        ff = glb[f]
        
        dd = (ff.__doc__ or '').strip() or 'NO_DOCSTRING'
        if '\n' in dd:
            dd = dd[:dd.index('\n')].strip()

        ee = space_pad(f,ch='.',n=40)
        print ee,
        print ellipsis_cut(dd, max(0,tw - len(ee) - 5))
    
    sys.exit(1)

def set_console_title(title):
    """
    Helpers for auto-setting console titles. Uses a few different methods to be sure.
    """
    try:
        title = title.replace("'",' ').replace('"',' ').replace('\\',' ')
        #cmd = r"echo -ne '\ek%s\e\\' > /dev/null" % title
        #system(cmd)
        cmd = "printf '\033k%s\033\\'" % title
        system(cmd)
        #cmd = 'screen -X title "%s" 2> /dev/null' % title
        #system(cmd)
    except:
        pass

def get_version(check = ['mediachain-indexer',
                         'mediachain-cli',
                         'ipfs-api',
                         ],
                ):
    """
    Output most important version info. TODO: incomplete.
    """
    
    if False:
        if sys.argv[0].startswith('mediachain-'):
            ## Indexer is installed:

            try:
                from mediachain.indexer.version import __version__
                return 'installed=' + __version__
            except:
                return 'could_not_get_version'
        else:
            ## Indexer running from local directory:
            
            import subprocess
            try:
                top_path = subprocess.check_output('git rev-parse --show-toplevel'.split()).strip().strip('"')
                assert 'mediachain-indexer' in top_path
            except:
                return 'unknown_version'

            try:
                exec(open(top_path + '/mediachain/indexer/version.py').read())

                return __version__
            except:
                return 'could_not_get_version'
        
    rr = []
    
    try:
        import pkg_resources
        
        for xx in check:
            try:
                rr.append(xx + '=' + pkg_resources.get_distribution(xx).version)
            except:
                pass
    except:
        pass
    
    return ' '.join(rr)

def setup_main(functions,
               glb,
               entry_point_name = False,
               ):
    """
    Helper for invoking functions from command-line.
    """
    
    print 'VERSION_INFO:',get_version()
    
    if len(sys.argv) < 2:
        usage(functions,
              glb,
              entry_point_name = entry_point_name,
              )
        return

    f=sys.argv[1]
    
    if f not in functions:
        print 'FUNCTION NOT FOUND:',f
        usage(functions,
              glb,
              entry_point_name = entry_point_name,
              )
        return

    title = (entry_point_name or sys.argv[0]) + ' '+f + ' #' + get_version(['mediachain-indexer'])
    
    set_console_title(title)
    
    print 'STARTING ',f + '()'

    ff=glb[f]

    ff(via_cli = True) ## New: make it easier for the functions to have dual CLI / API use.


from random import randint,choice,shuffle
from time import time,sleep

def sleep_loud(a, b = False):
    """
    Noisy sleeper. Set b == False to sleep a non-random amount of time.
    
    Args:
        a:  Minimum seconds to sleep.
        b:  Maximum seconds to sleep. `False` to disable randomization.
    """
    
    if b is False:
        x = a
    else:
        a = int(1000 * a)
        b = int(1000 * b)
        x = randint(a, b)
        x = x / 1000.0
    
    print ('SLEEPING %.3f' % x)
    sleep(x)
    print ('SLEEP_DONE')


class Remaining:
    """
    Remaining time estimates.
    """
    
    def __init__(self,
                tot,
                start_at = 0,
                ):
        self.t0 = time()
        self.t1 = self.t0
        self.tot = tot
        self.start_at = start_at
        self.prev = self.start_at
    
    def remaining(self,
                  cur,
                  since_prev = True,
                  is_post = True,
                  ):
        
        if is_post:
            is_post = 1
        else:
            is_post = 0

        if since_prev:
            #since last call:
            tt = self.t1
            prev = self.prev
        else:
            #since beginning:
            tt = self.t0
            prev = self.start_at
        
        now = time()
                
        self.t1 = now
        self.prev = cur
                
        try:
            rr = ((now - tt) / (cur - prev) * (self.tot - prev - is_post) - (now - tt)) / 60 / 60 / 24
        except:
            return '--computing--'
        
        if rr > 1:
            return '%.2f days' % rr
        
        rr *= 24
        if rr > 1:
            return '%.2f hours' % rr
        
        rr *= 60
        if rr > 1:
            return '%.2f min' % rr
        
        rr *= 60
        return '%.2f secs' % rr

