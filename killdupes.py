#!/usr/bin/env python
#
# Author: Martin Matusiak <numerodix@gmail.com>
# Licensed under the GNU Public License, version 3.
#
# <desc> Kill duplicate files, finding partial files as well </desc>
#
# revision 3 - Sort by smallest size before reading files in bucket
# revision 2 - Add dashboard display
# revision 1 - Add total byte count


from __future__ import with_statement
import glob
import hashlib
import os
import sys
import time


CHUNK = 1024*100
BYTES_READ = 0

_units = { 0: "B", 1: "KB", 2: "MB", 3: "GB", 4: "TB", 5: "PB", 6: "EB"}

class Record(object):
    def __init__(self, filename, data=None, eof=False):
        self.filename = filename
        self.data = data
        self.eof = eof

def format_size(size):
    if size == None:
        size = -1

    c = 0
    while size > 999:
        size = size / 1024.
        c += 1
    r = "%3.1f" % size
    u = "%s" % _units[c]
    return r.rjust(5) + " " + u.ljust(2)

def format_date(date):
    return time.strftime("%d.%m.%Y %H:%M:%S", time.gmtime(date))

def format_file(filename):
    st = os.stat(filename)
    return ("%s  %s  %s" % 
          (format_size(st.st_size), format_date(st.st_mtime), filename))

def write(s):
    sys.stdout.write(s)
    sys.stdout.flush()

def clear():
    write(79*" "+"\r")

def write_fileline(prefix, filename):
    write("%s %s\n" % (prefix, format_file(filename)))

def get_hash(idx, data):
    m = hashlib.md5()
    m.update(str(idx) + data)
    return m.hexdigest()

def get_filelist(pattern=None, lst=None):
    files = []
    it = lst or glob.iglob(pattern)
    for file in it:
        file = file.strip()
        if os.path.isfile(file) and not os.path.islink(file):
            files.append(Record(file))
    return files

def get_chunk(offset, length, filename):
    try:
        with open(filename, 'r') as f:
            f.seek(max(offset,0))
            data = f.read(length)
            ln = len(data)
            global BYTES_READ
            BYTES_READ += ln
            return ln, data
    except IOError, e:
        write("%s\n" % e)
        return 0, ""

def short_name(lst):
    lst.sort(cmp=lambda x, y: cmp((len(x), x), (len(y), y)))
    return lst

def rev_file_size(lst):
    lst.sort(reverse=True,
             cmp=lambda x, y: cmp(os.path.getsize(x), os.path.getsize(y)))
    return lst

def rec_file_size(lst):
    lst.sort(cmp=lambda x, y: cmp(os.path.getsize(x.filename),
                                  os.path.getsize(y.filename)))
    return lst

def compute(pattern=None, lst=None):
    zerosized = []
    incompletes = {}
    duplicates = {}

    offsets = {}
    offsets[0] = {}
    key = get_hash(0, "")

    write("Building file list..\r")
    offsets[0][key] = get_filelist(pattern=pattern, lst=lst)

    offsets_keys = offsets.keys()
    for offset in offsets_keys:
        offset_hashes = [(h,r) for (h,r) in offsets[offset].items() if len(r) > 1]
        buckets = len(offset_hashes)
        for (hid, (hash, rs)) in enumerate(offset_hashes):
            rs = rec_file_size(rs) # sort by shortest to not read redundant data
            reads = []
            readsize = CHUNK
            for (rid, record) in enumerate(rs):
                ln, data = get_chunk(offset, readsize, record.filename)
                s = ("%s | Offs %s | Buck %s/%s | File %s/%s | Rs %s" % 
                      (format_size(BYTES_READ),
                       format_size(offset),
                       hid+1,
                       buckets,
                       rid+1,
                       len(rs),
                       format_size(readsize)
                      )).ljust(79)
                write("%s\r" % s)
                if ln == 0:
                    record.eof = True
                else:
                    r = Record(record.filename, data)
                    if ln < readsize:
                        readsize = ln
                    reads.append(r)
            
            if reads:
                new_offset = offset+readsize
                if new_offset not in offsets:
                    offsets[new_offset] = {}
                    offsets_keys.append(new_offset)
                    offsets_keys.sort()

            for r in reads:
                new_hash = get_hash(new_offset, hash+r.data[:readsize])
                r.data = None
                if new_hash not in offsets[new_offset]:
                    offsets[new_offset][new_hash] = []
                offsets[new_offset][new_hash].append(r)
    clear() # terminate offset output

    offsets_keys = offsets.keys()
    offsets_keys.sort(reverse=True)
    for offset in offsets_keys:
        offset_hashes = offsets[offset]
        for (hash, rs) in offset_hashes.items():
            if offset == 0:
                zerosized = [r.filename for r in rs if r.eof]
            else:
                if len(rs) > 1:
                    eofs = [r for r in rs if r.eof]
                    n_eofs = [r for r in rs if not r.eof]
                    if len(eofs) >= 2 and len(n_eofs) == 0:
                        duplicates[eofs[0].filename] = [r.filename for r in eofs[1:]]
                    if len(eofs) >= 1 and len(n_eofs) >= 1:
                        key = rev_file_size([r.filename for r in n_eofs])[0]
                        if not key in incompletes:
                            incompletes[key] = []
                        for r in eofs:
                            if r.filename not in incompletes[key]:
                                incompletes[key].append(r.filename)

    return zerosized, incompletes, duplicates

def main(pattern=None, lst=None):
    zerosized, incompletes, duplicates = compute(pattern=pattern, lst=lst)
    if zerosized or incompletes or duplicates:

        kill = " X "
        keep = " = "

        q_zero = []
        q_inc  = []
        q_dupe = []

        if zerosized:
            write("Empty files:\n")
            for f in zerosized: 
                q_zero.append(f)
                write_fileline(kill, f)

        if incompletes:
            write("Incompletes:\n")
            for (idx, (f, fs)) in enumerate(incompletes.items()):
                fs.append(f)
                fs = rev_file_size(fs)
                for (i, f) in enumerate(fs):
                    prefix = keep
                    if os.path.getsize(f) < os.path.getsize(fs[0]):
                        q_inc.append(f)
                        prefix = kill
                    write_fileline(prefix, f)
                if idx < len(incompletes) - 1:
                    write('\n')

        if duplicates:
            write("Duplicates:\n")
            for (idx, (f, fs)) in enumerate(duplicates.items()):
                fs.append(f)
                fs = short_name(fs)
                for (i, f) in enumerate(fs):
                    prefix = keep
                    if i > 0:
                        q_dupe.append(f)
                        prefix = kill
                    write_fileline(prefix, f)
                if idx < len(duplicates) - 1:
                    write('\n')

        inp = raw_input("Kill files? (all/empty/incompletes/duplicates) [a/e/i/d/N] ")

        if "e" in inp or "a" in inp:
            for f in q_zero: os.unlink(f)
        if "i" in inp or "a" in inp:
            for f in q_inc: os.unlink(f)
        if "d" in inp or "a" in inp:
            for f in q_dupe: os.unlink(f)

if __name__ == "__main__":
    pat = '*'
    if len(sys.argv) > 1:
        if sys.argv[1] == "-h":
            write("Usage:  %s ['<glob pattern>'|--file <file>]\n" %
                  os.path.basename(sys.argv[0]))
            sys.exit(2)
        elif sys.argv[1] == "--file":
            lst = open(sys.argv[2], 'r').readlines()
            main(lst=lst)
        else:
            pat = sys.argv[1]
            main(pattern=pat)
    else:
        main(pattern='*')
