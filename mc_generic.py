#!/usr/bin/env python

"""
Generic helper functions.
"""

import json
import fcntl, termios, struct
import sys
from time import time
from os import system, walk
from os.path import join


def walk_files(dd, max_num = 0):
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
        
        if line.startswith(xx) or (line.startswith(zz) and line.endswith('[')) or (line.startswith(zz) and line.endswith('{')):
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
          ):
    try:
        tw,th = terminal_size()
    except:
        tw,th = 80,40
                   
    print
    print 'USAGE:','python',sys.argv[0],'<function_name>'
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
    title = title.replace("'",' ').replace('"',' ').replace('\\',' ')
    cmd = r"echo -ne '\ek%s\e\\'" % title
    print 'COMMAND',cmd
    system(cmd)


def setup_main(functions,
               glb,
               ):
    """
    Helper for invoking functions from command-line.
    """
    
    if len(sys.argv) < 2:
        usage(functions,
              glb,
              )
        return

    f=sys.argv[1]
    
    if f not in functions:
        print 'FUNCTION NOT FOUND:',f
        usage(functions,
              glb,
              )
        return

    title = sys.argv[0] + ' '+f
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

