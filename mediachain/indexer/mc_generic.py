#!/usr/bin/env python

"""
Generic helper functions.
"""

import json
import fcntl, termios, struct
import sys
from time import time
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

def terminal_size():
    h, w, hp, wp = struct.unpack('HHHH',
        fcntl.ioctl(0, termios.TIOCGWINSZ,
        struct.pack('HHHH', 0, 0, 0, 0)))
    return w, h

def usage(functions,
          glb,
          entry_point_name = False,
          ):
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
    Helpers for auto-setting console titles.
    """
    try:
        title = title.replace("'",' ').replace('"',' ').replace('\\',' ')
        cmd = r"echo -ne '\ek%s\e\\' > /dev/null" % title
        system(cmd)
        cmd = 'screen -X title "%s" 2> /dev/null' % title
        system(cmd)
    except:
        pass

def get_version(check = ['mediachain-indexer',
                         'mediachain-cli',
                         'ipfs-api',
                         ],
                ):
    """
    Output most important version info. Only works for installed packages.
    """
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
    
    return rr

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

    title = (entry_point_name or sys.argv[0]) + ' '+f
    
    set_console_title(title)
    
    print 'STARTING ',f + '()'

    ff=glb[f]

    ff()


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

