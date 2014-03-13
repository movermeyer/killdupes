killdupes
=========

Python version support: CPython 2.6, 2.7, 3.2, 3.3 and PyPy.


How it works
------------

``killdupes`` scans your filesystem to find duplicate files, partial files
and empty files.

Performs n:n comparison of files through md5 hashing and heavy use of
hashtables.
Execute with wildcard, or input file containing file names to check.

The method:

1. Scan all files, find the smallest.
2. Read ``read size`` amount of bytes (equal to the remaining size of the
   smallest file, or at most ``CHUNK`` size) from all files into ``records``.
3. Hash all records, use hashes as keys into ``offsets[current_offset]`` dict.
4. Files in the same bucket are known to be equal up to this offset.
5. Continue until at least two files remain that are still equal at all
   offsets.
6. Equal files are either a duplicate case (if they are the same size), or
   one is partial relative to the other (if not the same size).

Memory consumption should not exceed ``files_in_bucket * read_size``

The algorithm adapts to file changes; it will read all files until eof regardless
of the filesize as recorded at startup.


Usage
-----

.. code:: bash

    $ killdupes.py *
    176.1 KB | Offs   0.0  B | Buck 1/1 | File 193868/600084 | Rs   1.0  B

The dashboard fields:

1. Total bytes read
2. Current offset of reading
3. Current number of buckets
4. File/files in this bucket
5. Readsize at this offset
