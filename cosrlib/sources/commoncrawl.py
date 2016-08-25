import os
import gzip
import StringIO

from boto.s3.key import Key
import boto.s3.connection
import boto
import requests

from cosrlib.config import config
from .webarchive import WebarchiveSource


def list_commoncrawl_warc_filenames(limit=None, skip=0, version=None):
    """ List Common Crawl filenames """

    version = version or "latest"

    # Version that was previously imported with ./scripts/import_commoncrawl.sh
    if version == "local":
        warc_paths = os.path.join(config["PATH_BACK"], "local-data/common-crawl/warc.paths.txt")
        with open(warc_paths, "r") as f:
            warc_files = [x.strip() for x in f.readlines()]

    else:

        if version == "latest":
            # TODO: how to get the latest version automatically?
            version = "CC-MAIN-2016-30"

        r = requests.get("https://commoncrawl.s3.amazonaws.com/crawl-data/%s/warc.paths.gz" % version)
        data = gzip.GzipFile(fileobj=StringIO.StringIO(r.content), mode="rb").read()
        warc_files = [x.strip() for x in data.split("\n") if x.strip()]

    print "Using Common Crawl version %s with %d files" % (version, len(warc_files))

    return warc_files[int(skip or 0):int(limit or len(warc_files)) + int(skip or 0)], version


class CommoncrawlSource(WebarchiveSource):
    """ CommonCrawl-specific .warc Source """

    def get_partitions(self):
        """ Returns a list of Common Crawl segments """

        partitions, version = list_commoncrawl_warc_filenames(
            limit=self.args.get("limit"),
            skip=self.args.get("skip"),
            version=self.args.get("version")
        )

        return [{
            "partition": partition,
            "source": "commoncrawl:%s" % version
        } for partition in partitions]

    def open_warc_stream(self, filepath):
        """ Creates a WARC record iterator from the filepath given to the Source """

        # Test is the file is cached on the local disk
        local_data_file = os.path.join(config["PATH_BACK"], 'local-data/common-crawl/%s' % filepath)

        if os.path.isfile(local_data_file):
            filereader = open(local_data_file, "rb")
        else:
            conn = boto.s3.connect_to_region(
                "us-east-1",
                anon=True,
                calling_format=boto.s3.connection.OrdinaryCallingFormat(),
                is_secure=False
            )

            pds = conn.get_bucket('commoncrawl')
            filereader = Key(pds)
            filereader.key = filepath

        return self._warc_reader_from_file(filereader, filepath)
