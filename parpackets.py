#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
# Find and print sizes of packets in a par2 file.

import sys
import struct
import locale
import codecs
import logging
from collections import defaultdict

### fix stdout to utf-8 (or whatever)
# http://pymotw.com/2/codecs/
locale.setlocale(locale.LC_ALL, '')
lang, encoding = locale.getdefaultlocale()
# http://stackoverflow.com/a/4374457
sys.stdout = codecs.getwriter(encoding)(sys.stdout.detach())
#####

## Yields each header in a par2 file.
# See http://parchive.sourceforge.net/docs/specifications/parity-volume-spec/article-spec.html#i__134603784_354
def headers_in_par2_file(f):
    log = logging.getLogger('headers_in_par2_file')
    x = '????'
    while len(x) > 0:
        x = f.read(4)
        while len(x) > 0 and x != b'PAR2':
            log.info('skipping looking for header at {}'.format(f.tell()))
            x = f.read(4)
        x = f.read(4)
        if x == b'\0PKT':
            length = struct.unpack('<Q', f.read(8))[0]
            f.seek(f.tell()+16+16)
            type = f.read(16).decode('UTF-8').replace('\0', '·')
            yield (type, length)
            f.seek(f.tell()+length-16-16-16-8-8)

if __name__ == '__main__':
    stats = []
    stats.append(('nfiles', 'nbytes', 'data', 'overhead', 'recovery_length', 'nifsc', 'nmain', 'nrecvslic'))
    for fn in sys.argv[1:]:
        overhead = 0
        data = 0
        types = defaultdict(lambda: 0)
        filesizes = []
        filesizes = {}
        with open(fn, 'rb') as f:
            recovery_length = 0
            for type, length in headers_in_par2_file(f):
                types[type] += 1
                if 'RecvSlic' in type:
                    payload = length - 8 - 8 - 16 - 16 - 16
                    data += payload
                    recovery_length = payload
                    overhead += 8+8+16+16+16
                else:
                    overhead += length
                if 'FileDesc' in type:
                    where = f.tell()
                    id = f.read(16)
                    f.read(16)
                    f.read(16)
                    filesize = struct.unpack('<Q', f.read(8))[0]
                    f.seek(where)
                    filesizes[id] = filesize
            total = data + overhead
            overhead_pct = overhead / total * 100
            stats.append((len(filesizes), sum(filesizes.values()),
                          data, overhead, recovery_length, types['PAR 2.0·IFSC····'], types['PAR 2.0·Main····'], types['PAR 2.0·RecvSlic']))
            print('{}: {} bytes of data, {} of overhead ({:5.3f}%), {} total'.format(
                fn, data, overhead, overhead/(data+overhead)*100, data+overhead))
    print(types)
    for row in stats:
        print(','.join(map(str, row)))

