#!/usr/bin/env python

#
# This scripts imports DMOZ dumps
# http://www.dmoz.org/docs/en/rdf.html
#


import rocksdb
import os
import sys
import gzip
import ujson as json
from xml.sax import make_parser, handler

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from cosrlib.url import URL


os.system("mkdir -p local-data/dmoz/")

if os.getenv("COSR_TESTDATA"):
    os.system("cp tests/testdata/dmoz.rdf local-data/dmoz/content")
    os.system("cd local-data/dmoz/ && gzip --force content")
else:
    os.system("curl http://rdf.dmoz.org/rdf/content.rdf.u8.gz > local-data/dmoz/content.gz")

os.system("rm -rf local-data/dmoz/urls-rocksdb")
os.system("rm -rf local-data/dmoz/domains-rocksdb")

os.system("mkdir -p /tmp/cosr/local-data/dmoz/urls-rocksdb")
os.system("mkdir -p /tmp/cosr/local-data/dmoz/domains-rocksdb")

# We must create the DB in /tmp because vboxsf doesn't support fsync() on directories
db_urls = rocksdb.DB("/tmp/cosr/local-data/dmoz/urls-rocksdb", rocksdb.Options(create_if_missing=True))
db_domains = rocksdb.DB("/tmp/cosr/local-data/dmoz/domains-rocksdb", rocksdb.Options(create_if_missing=True))
db_urls_wb = rocksdb.WriteBatch()
db_domains_wb = rocksdb.WriteBatch()


class DmozHandler(handler.ContentHandler):
    def __init__(self):
        self._current_page = None
        self._current_key = None
        self.count = 0

    def startElement(self, name, attrs):
        if name == "ExternalPage" and attrs.get("about"):
            self._current_page = {
                "url": attrs.get("about")
            }
        elif self._current_page:
            self._current_key = name

    def endElement(self, name):
        if name == "ExternalPage":
            url = URL(self._current_page["url"])

            # TODO: Import "cool" flag, like IMDb has in http://www.dmoz.org/Arts/Movies/Databases/
            db_urls_wb.put(url.normalized.encode("utf-8"), json.dumps([
                self._current_page.get("d:Title"),
                self._current_page.get("d:Description")
                # , self._current_page.get("topic")
            ]).encode("utf-8"))
            self._current_page = None

            db_domains_wb.put(url.normalized_domain.encode("utf-8"), "1")  # TODO put total count?

            self.count += 1
            if self.count % 100000 == 0:
                print "Done %s" % self.count

        self._current_key = None

    def characters(self, content):
        if self._current_key:
            self._current_page[self._current_key] = content

    def endDocument(self):
        print "Done %s" % self.count

parser = make_parser()
parser.setContentHandler(DmozHandler())

with gzip.open('local-data/dmoz/content.gz', 'r') as xmlfile:
    parser.parse(xmlfile)

db_domains.write(db_domains_wb, sync=True)
db_domains.compact_range()

db_urls.write(db_urls_wb, sync=True)
db_urls.compact_range()

os.system("mv /tmp/cosr/local-data/dmoz/* local-data/dmoz/")
os.system("rm local-data/dmoz/content.gz")
