#!/usr/bin/env python
# -*- Python -*-
#
# poor man's diskcache
#
# a self-contained python program that scans directories for frames
from bisect import bisect_left, insort
from itertools import chain
from os import kill, walk
from os.path import join as os_path_join, getmtime
from sys import argv, exit, stdout

import cPickle as pickle
import gc
import shelve
import signal

VERSION=(1,1,0)
__version__='.'.join(str(v for v in VERSION))
__author__="Jeffery Kline"
__copyright__="Copyright 2013, Jeffery Kline"
__license__="GPL"
__email__="jeffery.kline@gmail.com"

# human-readable constants for the dc key. For Python versions>2.6 use a 
# named tuple instead.
# see also http://code.activestate.com/recipes/500261/
DIRNAME=0
SITE=1
FT=2
DUR=3
EXT=4

pickle_dumps = pickle.dumps
pickle_loads = pickle.loads
pickle_load = pickle.load
pickle_dump = pickle.dump

def segment_add(seg, sl):
    """
    add segment seg = [start, end] to segment list in place. Segment
    list sl described by list of pairs [[s0, e0], ...]

    NB. This assumes sl is sorted and start < end (these conditions
    are not checked)

    This is (essentially) copied from Kipp Cannon's original GPL'ed
    glue.segments

    It is also is a very expensive operation.
    """
    idx = bisect_left(sl, seg)
    sl.insert(idx, seg)
    n = len(sl)

    # assuming sl was created using segment_add(!), we need to
    # consider elements starting at the left-neighbor of seg to the
    # end. The left-neighbor could have a very large end time, and we
    # should coalesce it.
    i = j = max(0, idx-1)
    while j < n:
        lo, hi = sl[j]
        j += 1
        while j < n and hi >= sl[j][0]:
            hi = max(hi, sl[j][1])
            j += 1

        if lo < hi:
            sl[i] = (lo, hi)
            i += 1
    del sl[i:]

def parse_lfn(lfn):
    "return tuple (site, frametype, (int) gpsstart, (int) dur)"
    site,frametype,gpsstart_s,dur_ext = lfn.split('-')
    gpsstart = int(gpsstart_s)
    dur_s, ext = dur_ext.split('.')
    dur = int(dur_s)
    return (site, frametype, gpsstart, dur, ext)

def parallel_update_dc(dc, args, hot, concurrency):
    """Update dc in parallel by forking off 1 'pmdc.py' process per
    entry in args, running at most 'concurrency' processes at one
    time.

    ipc with the forked processes is done by writing to temporary
    files on disk.
    """
    from shutil import rmtree
    from subprocess import PIPE, Popen
    from tempfile import mkdtemp
    from time import sleep
    import atexit
    
    # the process list
    proc_l = []
    # each process writes to a file.
    fname_l = []

    # Block until fewer than 'nalive' processes are alive.  Raise
    # error if any proces exited abnormally.
    def proc_l_pause(concurrency=0):
        alive_l = lambda: [p for p in proc_l if p.poll() is None]
        failed_l = lambda: [p for p in proc_l if p.poll()]
        max_alive = max(concurrency-1, 0)

        while len(alive_l()) > max_alive:
            # catch any subprocesses that exited with an error
            for failed in failed_l():
                so, se = failed.communicate()
                raise RuntimeError(se)
            sleep(0.125)

    # remove files created, destroy lingering processes
    tempd = mkdtemp()
    def proc_l_kill():
        for p in proc_l:
            try: kill(p.pid, signal.SIGKILL)
            except OSError: pass

    atexit.register(rmtree, tempd)
    atexit.register(proc_l_kill)

    # create the subprocesses
    for _d in args:
        fname_l.append(os_path_join(tempd, str(len(proc_l))))
        # call pmdc with same cache and 1 directory argument. Write
        # new information to ipc file.
        cmd = [argv[0], argv[1], _d, "-i", fname_l[-1]]
        proc_l.append(Popen(cmd, stdout=PIPE, stderr=PIPE))
        # block until some subprocess exits
        proc_l_pause(concurrency)
    # block until all subprocesses exit
    proc_l_pause()

    # update dc and hot using ipc_file
    for fname in fname_l:
        _cache = pickle_load(open(fname, 'r'))
        hot.update(_cache["hot"])
        # dc is a shelve object; this loop is required
        for key, val in _cache["dc"].iteritems():
            dc[key] = val

        # catch anything the needs deleting
        for key in _cache["delete"]:
            del dc[key]

def update_dc(dc, d, hot):
    """
    modify the dictionary dc to reflect the "diskcache content of
    d". Skip 'hot' directories that have not been updated.  
    
    Calls sem.release() before returning

    NB: calls os.walk(d)
    """
    # ignore hot directories that have not changed
    try: 
        if getmtime(d) <= hot[d]: 
            return
    except KeyError: 
        pass

    # store all new information in keys_seen. Modify dc at the very
    # end.  dc might be shelve object; keys must be strings, and
    # updating should be explicit.
    keys_seen = {}
    for dirpath, dirname_l, filename_l in walk(d):
        # remove hot directories from the list to traverse
        for _dir in list(dirname_l):
            _fulldir = os_path_join(dirpath, _dir)
            try:
                # remove from list to  skip _dir on next iteration
                if getmtime(_fulldir) <= hot[_fulldir]:
                    dirname_l.remove(_dir)
            except KeyError:
                pass

        # catch empty 'hot' directories
        if not dirname_l and not filename_l:
            hot[dirpath] = getmtime(dirpath)
        for f in filename_l:
            try:
                (site, frametype, gpsstart, dur, ext) = parse_lfn(f)
            except:
                continue

            key = pickle_dumps((dirpath, site, frametype, dur, ext), -1)
            seg = (gpsstart, gpsstart + dur)
            # On the initial visit to 'key', initialize
            # segmentlist. On later passes, extend segmentlist.
            if key not in keys_seen:
                keys_seen[key] = []
                hot[dirpath] = getmtime(dirpath)
            segment_add(seg, keys_seen[key])        
    for key in keys_seen:
        dc[key] = keys_seen[key]

def write_dc(dc, hot, protocol, ext, fh):
    "write the diskcache to filehandle fh in various formats"
    if protocol == "ldas":
        ldas = []
        for keyp, segmentlist in dc.iteritems():
            key = pickle_loads(keyp)
            dur = key[DUR]
            if key[EXT] not in ext:
                continue
            # mimic the ldas format
            _key = list(key[:-1])
            _key.insert(3,'1')
            key_str = ','.join(map(str, _key))
            mtime_s = str(int(hot[key[DIRNAME]]))
            nfile_s = str(sum((s1-s0) for s0, s1 in segmentlist)/dur)
            val_str_l = [key_str, mtime_s, nfile_s, 
                         '{' + ' '.join(map(str, chain(*segmentlist))) + '}']
            ldas.append(' '.join(val_str_l))
        ldas.sort()
        fh.write('\n'.join(ldas))
        fh.write('\n')
    elif protocol == "pmdc":
        pmdc = []
        for keyp, segmentlist in dc.iteritems():
            key = pickle_loads(keyp)
            dur = key[DUR]
            if key[EXT] not in ext:
                continue
            # mimic the ldas format
            _key = list(key)
            _key.insert(3,'x')
            key_str = ','.join(map(str, _key))
            mtime_s = str(int(hot[key[DIRNAME]]))
            nfile_s = str(sum((seg[1]-seg[0])/dur for seg in segmentlist))
            val_str_l = [key_str, mtime_s, nfile_s, 
                         '{', ' '.join(map(str, chain(*segmentlist))), '}']
            pmdc.append(' '.join(val_str_l))
        pmdc.sort()
        fh.write('\n'.join(pmdc))
        fh.write('\n')
    elif protocol == "dcfs":
        # structures useful for building/browsing the 'dcfs'.  These
        # should be defaultdicts but we need python 2.4 compatibility
        
        # browse high levels of dcfs tree with meta_hi
        meta_hi = {}
        # browse middle levels of dcfs tree with meta_mid
        meta_mid = {}
        # this is the complete listing, a sorted list
        meta_lo = {}
        for keyp, seglist in dc.iteritems():
            key = pickle_loads(keyp)
            ext = key[EXT]
            ft = key[FT]
            site = key[SITE]

            try: meta_hi[ext].add(ft)
            except: meta_hi[ext] = set([ft])

            try: meta_mid[(ext,ft)].add([site])
            except: meta_mid[(ext, ft)] = set([site])
            
            v = (seglist, key[DUR], key[DIRNAME], hot[key[DIRNAME]])
            k = (ext, ft, site)
            try: meta_lo[k].append(v)
            except: meta_lo[k] = [v]
        try:
            gc.disable()
            pickle_dump(meta_hi, fh, -1)
            pickle_dump(meta_mid, fh, -1)
            pickle_dump(meta_lo, fh, -1)
        finally:
            gc.enable()
    else:
        raise ValueError("Unknown protocol %s" % str(protocol))

if __name__=="__main__":
    from optparse import OptionParser
    usage = "usage: %prog <cache> <directory list> [options]"
    parser = OptionParser(usage=usage)
    extension_help = ' '.join("""[[]] Scan for files ending with
                               ".EXTENSION". Passing "--extension gwf"
                               is equivalent to not passing an
                               extension flag.  Use multiple times for
                               multiple extensions. The dot is not
                               part of EXTENSION, do not include it.""".split())
    parser.add_option("-e", "--extension", action="append", default=[],
                      help=extension_help)

    output_help = ' '.join(
        """[-] Name of file to write diskcache output to.  Use '-' for
           stdout. Only applies if used in conjuction with -p
           flag.""".split())
    parser.add_option("-o", "--output", default="-", help=output_help)

    ipc_help = """[None] Name of file for internal process
    communication. You probably do not need this."""
    parser.add_option("-i", "--ipc-file", default=None, help=ipc_help)

    protocol_choices = ("dcfs", "ldas", "pmdc")
    protocol_help = ' '.join(
        """[None] Protocol of output to write. Choose from %s.  Use
        'ldas' for compatibility with ldas-tools.  Use 'pmdc' for an
        extended protocol that includes file extension information.
        Formats 'ldas' and 'pmdc' are plain text and results are
        sorted. 'dcfs' is a format useful for the diskcache
        filesystem.""".split() ) % str(protocol_choices)
    parser.add_option("-p", "--protocol", choices=protocol_choices,
                      help=protocol_help)
    concurrency_help = ' '.join(
        """[5] Maximum number of concurrent scan processes. Only
           applicable if more than 1 directory is passed as a
           positional argument.""".split())
    parser.add_option("-r", "--concurrency", type="int", default=5,
                      help=concurrency_help)

    (options, args) = parser.parse_args()

    # args requires cache_file and a list of 1 or more directories.
    # Branch depending on the case at hand.  If directory list has 1
    # element then run in main thread.  If directory list is longer,
    # fork one process for each entry.
    if not args:
        parser.error("No cache provided")
        exit(1)

    if len(args) == 1:
        parser.error("No directories listed")
        exit(1)
    cache_file = args[0]

    # the cache_file holds the 'hot' dict (and some other stuff).
    #
    # (dict) hot:
    #    key: dirpath
    #    value: mtime(dirpath)
    # 'hot' dirs are directories that have been completely
    # indexed. This includes empty dirs.
    #
    # The value of mtime was sampled when (approximately) the last new
    # dc key containing dirpath was added.
    try: hot = pickle_load(open(cache_file, 'r'))["hot"]
    except: hot = {}

    # If using interprocess communication, then dc must be
    # empty. Otherwise, populate dc from cache.  
    # 
    # (shelve,dict) dc:
    #   key: pickle.dumps((dirpath, site, frametype, dur, ext)) 
    #   value: segmentlist associated with key
    if options.ipc_file is None:
        dc = shelve.open(cache_file + '.shlv')
    else:
        dc = {}

    if len(args) == 2:
        update_dc(dc, args[1], hot)
    else:
        parallel_update_dc(dc, args[1:], hot, options.concurrency)

    # write alternative output formats if requested to desired output
    if options.protocol is not None:
        if options.output == "-":
            fh = stdout
        else:
            fh = open(options.output, 'w')
        extension = set(options.extension)
        if not extension: 
            extension.add('gwf')
        write_dc(dc, hot, options.protocol, extension, fh)

    # all done now save state.
    #
    # If using ipc, save dc as part of meta information to ipc_file.
    # Otherwise, save dc shelve object and save remaining meta to
    # cache_file.
    meta = {"version": VERSION,
            "args": args,
            "options": options}
    if options.ipc_file is None:
        dc.close()
        meta_f = cache_file
        meta["hot"] = hot
    else:
        # save only new information as recorded in 'dc'
        _hot = {}
        _del = set()

        _dc =  shelve.open(cache_file + '.shlv', 'r')
        for keyp in dc:
            key = pickle_loads(keyp)
            _hot[key[DIRNAME]] = hot[key[DIRNAME]]

            # get list of keys to delete from dc: all keys with the
            # same dirname but different key.
            dirname = key[DIRNAME]
            d_keys = set([k for k in _dc if pickle_loads(k)[DIRNAME] == dirname])
            g_keys = set([k for k in dc if pickle_loads(k)[DIRNAME] == dirname])
            for k in d_keys:
                if k not in g_keys:
                    _del.add(k)

        meta_f = options.ipc_file
        meta["dc"] = dc
        meta["hot"] = _hot
        meta["delete"] = _del

    pickle_dump(meta, open(meta_f, 'w'), -1)
