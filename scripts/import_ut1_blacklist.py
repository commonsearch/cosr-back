#!/usr/bin/env python

#
# This scripts imports the UT1 blacklists
# https://dsi.ut-capitole.fr/blacklists/index_en.php
#


import rocksdb
import os
from collections import defaultdict


os.system("rm -rf local-data/ut1-blacklist/ && mkdir -p local-data/ut1-blacklist/")

if os.getenv("COSR_TESTDATA"):
  os.system("cp -R tests/testdata/ut1_blacklists local-data/ut1-blacklist/blacklists")
else:
  os.system("curl ftp://ftp.ut-capitole.fr/pub/reseau/cache/squidguard_contrib/blacklists.tar.gz > local-data/ut1-blacklist/blacklists.tar.gz")
  os.system("cd local-data/ut1-blacklist/ && tar zxf blacklists.tar.gz && rm blacklists.tar.gz")

os.system("rm -rf local-data/ut1-blacklist/blacklist-rocksdb")
os.system("mkdir -p /tmp/cosr/local-data/ut1-blacklist/blacklist-rocksdb")

# TODO optimize http://pyrocksdb.readthedocs.org/en/v0.3/tutorial/index.html
# We must create the DB in /tmp because vboxsf doesn't support fsync() on directories
db = rocksdb.DB("/tmp/cosr/local-data/ut1-blacklist/blacklist-rocksdb", rocksdb.Options(create_if_missing=True))

data = defaultdict(list)

for fp in os.listdir("local-data/ut1-blacklist/blacklists"):
  fullpath = "local-data/ut1-blacklist/blacklists/%s" % fp

  if os.path.isdir(fullpath) and not os.path.islink(fullpath):

    cnt = 0

    with open(fullpath + "/domains", 'r') as f:
      for line in f.readlines():
        # print line.strip(), fp
        data[line.strip()].append(fp)
        cnt += 1

    if os.path.isfile(fullpath + "/urls"):
      with open(fullpath + "/urls", 'r') as f:
        for line in f.readlines():
          # print line.strip(), fp
          data[line.strip()].append(fp)
          cnt += 1

    print "Done %s (%s entries)" % (fp, cnt)

wb = rocksdb.WriteBatch()
for k, v in data.iteritems():
  wb.put(k, " ".join(v))

db.write(wb, sync=True)
db.compact_range()

os.system("mv /tmp/cosr/local-data/ut1-blacklist/blacklist-rocksdb local-data/ut1-blacklist/")
os.system("rm -rf local-data/ut1-blacklist/blacklists/")
