#!/usr/bin/env python
# -*- Python -*-
#
# poor man's diskcache
#
# a self-contained python program that scans directories for frames
from bisect import bisect_left, insort
from itertools import chain
from os import walk
from os.path import join as os_path_join, getmtime
from sys import argv, exit, stdout

import cPickle as pickle
import gc

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

# potentially unsafe optimizations (safe only if no self-referental
# structures are used)
def pickle_load(*args, **kwargs):
    gc.disable()
    try:
        return pickle.load(*args, **kwargs)
    finally:
        gc.enable()

def pickle_dump(*args, **kwargs):
    gc.disable()
    try:
        return pickle.dump(*args, **kwargs)
    finally:
        gc.enable()

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

    tempd = mkdtemp()
    atexit.register(rmtree, tempd)
    def proc_l_kill():
        for p in proc_l:
            try: p.kill()
            except OSError: pass
    atexit.register(proc_l_kill)

    # create the subprocesses
    for _d in args:
        fname_l.append(os_path_join(tempd, str(len(proc_l))))
        # call pmdc with 1 argument, write to fname and communicate
        # with ipc protocol
        cmd = [argv[0], _d, "-p", "ipc", "-o", fname_l[-1]]
        if options.cache_file:
            cmd.extend(["-c", options.cache_file])
        proc_l.append(Popen(cmd, stdout=PIPE, stderr=PIPE))
        # block until some subprocess exits
        proc_l_pause(concurrency)

    # block until all subprocesses exit
    proc_l_pause()

    # update dc and hot using ipc protocol
    for fname in fname_l:
        _cache = pickle_load(open(fname, 'r'))
        dc.update(_cache["dc"])
        hot.update(_cache["hot"])

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

    keys_seen = set()
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
                # FIXME: this is a good place to record errors
                continue
            key = (dirpath, site, frametype, dur, ext)
            seg = (gpsstart, gpsstart + dur)
            # modify dc here (cpython GIL means no required lock).
            # On the initial visit to 'key', initialize
            # segmentlist. On later passes, extend segmentlist.
            if key not in keys_seen:
                keys_seen.add(key)
                dc[key] = []
                hot[dirpath] = getmtime(dirpath)
            segment_add(seg, dc[key])

def print_dc(dc, hot, protocol, ext, fh):
    "write the diskcache to filehandle fh in various formats"
    if protocol == "ipc":
        pickle_dump({"dc": dc, "hot": hot}, fh, -1)
    elif protocol == "ldas":
        ldas = []
        for key, val in dc.iteritems():
            segmentlist = dc[key]
            dur = key[DUR]
            if key[EXT] not in ext: 
                continue
            _key = list(key[:-1])
            _key.insert(3, 1)
            key_str = ','.join([str(s) for s in _key])
            mtime_s = str(int(hot[key[DIRNAME]]))
            nfile_s = str(sum((seg[1]-seg[0])/dur for seg in segmentlist))
            # each line of the ascii dump from the original diskcache
            # terminates with '[0-9]}' and not '[0-9] }' (note lack of
            # whitespace. This makes doing diffs for sanity checks easier.
            val_str_l = [key_str, mtime_s, nfile_s,
                         '{' + ' '.join(str(s) for s in chain(*segmentlist)) + '}']
            ldas.append(' '.join(val_str_l))
        fh.write('\n'.join(ldas))
        fh.write('\n')
    elif protocol == "pmdc":
        pmdc = []
        for key, val in dc.iteritems():
            segmentlist = dc[key]
            dur = key[DUR]
            if key[EXT] not in ext:
                continue
            # mimic the ldas format
            _key = list(key)
            _key.insert(3,'x')
            key_str = ','.join([str(s) for s in _key])
            mtime_s = str(int(hot[key[DIRNAME]]))
            nfile_s = str(sum((seg[1]-seg[0])/dur for seg in segmentlist))
            val_str_l = [key_str, mtime_s, nfile_s, 
                         '{', ' '.join(str(s) for s in chain(*segmentlist)), '}']
            ldas.append(' '.join(val_str_l))
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
        for key, seglist in dc.iteritems():
            try: meta_hi[key[EXT]].add(key[FT])
            except: meta_hi[key[EXT]] = set([key[FT]])

            try: meta_mid[(key[EXT],key[FT])].add([key[SITE]])
            except: meta_mid[(key[EXT],key[FT])] = set([key[SITE]])
            
            v = (seglist, key[DUR], key[DIRNAME], hot[key[DIRNAME]])
            k = (key[EXT], key[FT], key[SITE])
            try:
                insort(meta_lo[k], v)
            except:
                meta_lo[k] = [v]
                
        pickle_dump(meta_hi, fh, -1)
        pickle_dump(meta_mid, fh, -1)
        pickle_dump(meta_lo, fh, -1)
    else:
        raise ValueError("Unknown protocol %s" % str(protocol))

if __name__=="__main__":
    from optparse import OptionParser
    usage = "usage: %prog [options] dir0 dir1 ..."
    parser = OptionParser(usage=usage)
    parser.add_option("-c", "--cache-file",
                      help="[None] Name of cache-file to read during initialzation.")

    extension_help = ' '.join("""[[] (empty list)] Scan for files
                               ending with ".EXTENSION". No
                               "--extension" flag is equivalent to
                               passing "--extension gwf".  Use
                               multiple times for multiple
                               extensions. The dot is not part of
                               EXTENSION, do not include it.""".split())
    parser.add_option("-e", "--extension", action="append", default=[],
                      help=extension_help)

    new_cache_file_help = ' '.join(
        """[None] Name of cache file to save for use with -c flag next
        time. If CACHE_FILE and NEW_CACHE_FILE are the same,
        CACHE_FILE will be overwriten just prior to a successful exit.""".split())
    parser.add_option("-n", "--new-cache-file", help=new_cache_file_help)

    output_help = ' '.join(
        """[-] Name of file to write diskcache output to. 
           Use '-' for stdout.""".split())
    parser.add_option("-o", "--output", default="-", help=output_help)

    concurrency_help = ' '.join(
        """[5] Maximum number of concurrent scan processes. Only
           applicable if more than 1 directory is passed as a
           positional argument.""".split())
    parser.add_option("-r", "--concurrency", type="int", default=5,
                      help=concurrency_help)

    protocol_choices = ("dcfs", "ipc", "ldas", "pmdc")
    protocol_help = ' '.join(
        """[ldas] Protocol of output to write. Valid values are %s.
                     Use 'ldas' for compatibility with ldas-tools.
                     Use 'pmdc' for an extended protocol that includes
                     file extension information.  'ipc' is used for
                     internal communication and also uses the cache
                     differently. Formats 'ldas' and 'pmdc' are plain
                     text and results are sorted. 'dcfs' is a format
                     useful for the diskcache filesystem.""".split()
        ) % str(protocol_choices)
    parser.add_option("-p", "--protocol", default='ldas',
                      choices=protocol_choices, help=protocol_help)
    (options, args) = parser.parse_args()

    # cache file format: 3 pickled dicts.
    # 
    # (dict) header: 
    #   header has a 'version' key (and several keys with cli
    #   arguments, etc.)
    #
    # (dict) hot:
    #    key: dirpath
    #    value: mtime(dirpath)
    # 'hot' dirs are directories that have been completely
    # indexed. This includes empty dirs.
    #
    # NB: the value of mtime was sampled when the last new dc key
    # containing dirpath was added.
    # 
    # (dict) dc:
    #     key: (dirpath, site, frametype, dur, ext)
    #     value: segmentlist associated with key
    dc = {}
    hot = {}
    header ={}
    if options.cache_file is not None:
        # python 2.4; this should be in a context manager
        fh = open(options.cache_file, 'r')

        header = pickle_load(fh)
        if header['version'] != VERSION:
            raise RuntimeError("Cache file version mismatch")

        hot = pickle_load(fh)

        # All protocols (except 'ipc') should populate dc with cached
        # information.
        if options.protocol != 'ipc':
            dc = pickle_load(fh)
        fh.close()

    # args has length 0, 1 or more than 1. Branch depending on the
    # case at hand.  If args has len 1, then run in main thread.  If
    # args has length >1, fork one process for each argument.
    if not args:
        parser.error("No directories listed")
        exit(1)
    elif len(args) == 1:
        update_dc(dc, args[0], hot)
    else:
        parallel_update_dc(dc, args, hot, options.concurrency)

    if options.output == "-":
        fh = stdout
    else:
        fh = open(options.output, 'w')

    extension = set(options.extension)
    if not extension: extension.add('gwf')
    print_dc(dc, hot, options.protocol, extension, fh)

    # write new cache file: header, hot and dc in that order.
    if options.new_cache_file is not None:
        fh = open(options.new_cache_file, 'w')
        header = { "options": options, "args": args, 
                   "version": VERSION }
        pickle_dump(header, fh, -1)
        pickle_dump(hot, fh, -1)
        pickle_dump(dc, fh, -1)
        fh.close()
