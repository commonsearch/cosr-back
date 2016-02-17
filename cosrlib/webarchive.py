import os
import tempfile

import warc
from boto.s3.key import Key
import boto
try:
    from http_parser.parser import HttpParser
except ImportError:
    from http_parser.pyparser import HttpParser

from gzipstream import GzipStreamFile

from .url import URL
from .config import config


def list_commoncrawl_warc_filenames(limit=None, skip=0):
    """ List Common Crawl filenames """

    warc_paths = os.path.join(config["PATH_BACK"], "local-data/common-crawl/warc.paths.txt")
    with open(warc_paths, "r") as f:
        warc_files = [x.strip() for x in f.readlines()]

    return warc_files[skip:(limit or len(warc_files)) + skip]


def open_warc_file(filename, from_commoncrawl=True):
    """ Opens a WARC file from local-data or S3 for Common Crawl files """

    local_data_file = os.path.join(config["PATH_BACK"], 'local-data/%s' % filename)

    if not from_commoncrawl:
        filereader = open(filename, "rb")
    elif os.path.isfile(local_data_file):
        filereader = open(local_data_file, "rb")
    else:
        conn = boto.connect_s3(anon=True)
        pds = conn.get_bucket('aws-publicdatasets')
        filereader = Key(pds)
        filereader.key = filename

    if filename.endswith(".warc"):
        return warc.WARCFile(fileobj=filereader)
    else:
        # TODO: investigate how we could use cloudflare's zlib
        return warc.WARCFile(fileobj=GzipStreamFile(filereader))


def iter_warc_records(warc_file, domain_whitelist=None, only_homepages=None):
    """ Selective iterator over records in a WARC file """

    for _, record in enumerate(warc_file):

        if not record.url:
            continue

        if record['Content-Type'] != 'application/http; msgtype=response':
            continue

        url = URL(record.url, check_encoding=True)

        if domain_whitelist is not None:
            if url.domain not in domain_whitelist:
                continue

        elif only_homepages:
            if url.parsed.path != "/" or url.parsed.query != "":
                continue

        payload = record.payload.read()
        parser = HttpParser()
        parser.execute(payload, len(payload))

        headers = parser.get_headers()

        if 'text/html' not in headers.get("content-type", ""):
            # print "Not HTML?", record.url, headers
            continue

        yield url, headers, parser.recv_body()


def create_warc_from_corpus(documents, filename=None):
    """ Used mainly in tests to generate small .warc files """

    if filename is None:
        fd, filename = tempfile.mkstemp(suffix=".warc")
        os.close(fd)

    f = warc.open(filename, "w")

    for doc in documents:

        payload = (
            "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Type: text/html\r\n\r\n" +
            doc["content"]
        )
        record = warc.WARCRecord(payload=payload, headers={
            "Content-Type": "application/http; msgtype=response",
            "WARC-Type": "response",
            "WARC-Target-URI": doc["url"]
        })
        f.write_record(record)

    f.close()

    return filename
