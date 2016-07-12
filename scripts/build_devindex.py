#!/usr/bin/env python

#
# The dev index is a small index of sample pages used to make developement in cosr-front easier
#

# We get the top N domains from alexa
TOP_ALEXA_DOMAINS = 1000

# Plus the following hardcoded documents
# Format is: url => {"static_rank": static rank from 1 to 0, URL}
DOCUMENTS = {

    "https://about.commonsearch.org/": {"static_rank": 1},
    "https://en.wikipedia.org/wiki/Maceo_Parker": {"static_rank": 0.98},  # Significance of this rank left to the listener...

}




import gevent.monkey
gevent.monkey.patch_all()

from gevent.pool import Pool
import sys
import os
sys.path.insert(-1, os.path.dirname(os.path.dirname(__file__)))

import itertools
import requests
from cosrlib.sources.webarchive import create_warc_from_corpus
from urlserver.datasources import list_datasources
from cosrlib.indexer import Indexer
from cosrlib.config import config

request_pool = Pool(20)


def get_documents():
    """ Get the complete list of documents to be crawled """
    # Add alexa top domains to the documents
    if TOP_ALEXA_DOMAINS > 0:
        alexa_datasource = list_datasources()["alexa_top1m"]
        print "Fetching top %s domains from Alexa" % TOP_ALEXA_DOMAINS
        for row in itertools.islice(alexa_datasource.iter_dump(), 0, TOP_ALEXA_DOMAINS):
            alexa_rank = int(row[0])
            domain = row[1]
            static_rank = 0.5 - (0.0001 * alexa_rank)
            DOCUMENTS["http://%s" % domain] = {"static_rank": static_rank}

    return DOCUMENTS


def crawl(url):
    """ Crawl a document """

    print "Crawling %s..." % url

    try:
        res = requests.get(url, timeout=30)
    except Exception, e:
        print "[ERROR] %s failed: %s" % (url, e)
        return None

    if res.status_code != 200:
        print "[WARNING] %s had status=%s" % (url, res.status_code)
        return None

    return {
        "content": res.content,  # text.encode("utf-8"),
        "url": url,
        "headers": res.headers
    }


def generate_corpus():
    """ # Crawl all the documents """

    for res in request_pool.imap_unordered(crawl, get_documents().iterkeys()):
        if res is not None:
            res["url_metadata_extra"] = {
                "url": {
                    "rank": DOCUMENTS[res["url"]]["static_rank"]
                }
            }
            yield res


if "--warc" in sys.argv:

    # Generate a WARC file
    devindex_dir = os.path.join(config["PATH_LOCALDATA"], "devindex")
    if not os.path.isdir(devindex_dir):
        os.makedirs(devindex_dir)
    warc_file = os.path.join(devindex_dir, "crawl.warc")

    create_warc_from_corpus(generate_corpus(), filename=warc_file)

    print "Created WARC file:", warc_file

elif "--index" in sys.argv:

    indexer = Indexer()
    if "--empty" in sys.argv:
        indexer.empty()
    docs = indexer.index_corpus(generate_corpus(), flush=True, refresh=True)
    print "Indexed %s documents." % len(docs)

else:
    print "Usage: python build_devindex.py [--warc | --index]"
    sys.exit(1)
