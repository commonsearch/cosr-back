#!/usr/bin/env python

#
# This script import the Harmonic Centrality rankings from webdatacommons
# http://webdatacommons.org/hyperlinkgraph/2012-08/download.html
#

import rocksdb
import csv
import gzip
import os

os.system("mkdir -p local-data/webdatacommons-hyperlinkgraph/")

if os.getenv("COSR_TESTDATA"):
    os.system("cp tests/testdata/webdatacommons-hyperlinkgraph-h.tsv local-data/webdatacommons-hyperlinkgraph/hostgraph-h.tsv")
    os.system("cd local-data/webdatacommons-hyperlinkgraph/ && gzip --force hostgraph-h.tsv")
else:
    os.system("curl http://data.dws.informatik.uni-mannheim.de/hyperlinkgraph/2012-08/ranking/hostgraph-h.tsv.gz > local-data/webdatacommons-hyperlinkgraph/hostgraph-h.tsv.gz")

# https://plyvel.readthedocs.org/en/latest/api.html#database
os.system("rm -rf local-data/webdatacommons-hyperlinkgraph/harmonic-rocksdb")
os.system("mkdir -p /tmp/cosr/local-data/webdatacommons-hyperlinkgraph/harmonic-rocksdb")

# TODO optimize http://pyrocksdb.readthedocs.org/en/v0.3/tutorial/index.html
# We must create the DB in /tmp because vboxsf doesn't support fsync() on directories
db = rocksdb.DB("/tmp/cosr/local-data/webdatacommons-hyperlinkgraph/harmonic-rocksdb", rocksdb.Options(create_if_missing=True))

with gzip.open('local-data/webdatacommons-hyperlinkgraph/hostgraph-h.tsv.gz', 'r') as csvfile:
    reader = csv.reader(csvfile, delimiter='\t')
    wb = rocksdb.WriteBatch()
    for i, row in enumerate(reader):
        wb.put(row[0], str(float(row[1])))
        if i % 1000000 == 0:
          db.write(wb, sync=True)
          wb = rocksdb.WriteBatch()
          print "Done %s" % i
    print "Done %s" % i
    db.write(wb, sync=True)

db.compact_range()

os.system("mv /tmp/cosr/local-data/webdatacommons-hyperlinkgraph/harmonic-rocksdb local-data/webdatacommons-hyperlinkgraph/")
os.system("rm local-data/webdatacommons-hyperlinkgraph/hostgraph-h.tsv.gz")
