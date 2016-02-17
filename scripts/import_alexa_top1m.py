#!/usr/bin/env python

#
# This scripts imports Alexa data
#


import rocksdb
import os
import csv

os.system("mkdir -p local-data/alexa/")

if os.getenv("COSR_TESTDATA"):
    os.system("cp tests/testdata/alexa-top1m.csv local-data/alexa/top-1m.csv")
else:
    os.system("curl http://s3.amazonaws.com/alexa-static/top-1m.csv.zip > local-data/alexa/top-1m.csv.zip")
    os.system("cd local-data/alexa/ && unzip -o top-1m.csv.zip && rm top-1m.csv.zip")

os.system("rm -rf local-data/alexa/top-1m-rocksdb")
os.system("mkdir -p /tmp/cosr/local-data/alexa/top-1m-rocksdb")

# TODO optimize http://pyrocksdb.readthedocs.org/en/v0.3/tutorial/index.html
# We must create the DB in /tmp because vboxsf doesn't support fsync() on directories
db = rocksdb.DB("/tmp/cosr/local-data/alexa/top-1m-rocksdb", rocksdb.Options(
    create_if_missing=True
))

with open('local-data/alexa/top-1m.csv', 'r') as csvfile:
    reader = csv.reader(csvfile)
    wb = rocksdb.WriteBatch()
    for row in reader:
        wb.put(row[1], row[0])
    db.write(wb, sync=True)

db.compact_range()
os.system("cd local-data/alexa/ && rm top-1m.csv")
os.system("mv /tmp/cosr/local-data/alexa/top-1m-rocksdb local-data/alexa/")
