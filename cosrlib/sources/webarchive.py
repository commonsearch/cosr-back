import os
import tempfile
from cosrlib.sources import Source
from cosrlib.url import URL

import warc
from gzipstream import GzipStreamFile
try:
    from http_parser.parser import HttpParser
except ImportError:
    from http_parser.pyparser import HttpParser


class WebarchiveSource(Source):
    """ Generic .warc Source """

    def _warc_reader_from_file(self, filereader, filepath):
        """ Creates a WARC record iterator from a file reader """

        if filepath.endswith(".warc"):
            return warc.WARCFile(fileobj=filereader)
        else:
            # TODO: investigate how we could use cloudflare's zlib
            return warc.WARCFile(fileobj=GzipStreamFile(filereader))

    def open_warc_stream(self):
        """ Creates a WARC record iterator from the filepath given to the Source """

        filereader = open(self.args["file"], "rb")
        return self._warc_reader_from_file(filereader, self.args["file"])

    def iter_items(self):
        """ Yields objects in the source's native format """

        warc_stream = self.open_warc_stream()

        for record in warc_stream:

            if not record.url:
                continue

            if record['Content-Type'] != 'application/http; msgtype=response':
                continue

            url = URL(record.url, check_encoding=True)

            do_parse, index_level = self.filter_url(url)

            if not do_parse:
                continue

            payload = record.payload.read()
            parser = HttpParser()
            parser.execute(payload, len(payload))

            headers = parser.get_headers()

            if 'text/html' not in headers.get("content-type", ""):
                # print "Not HTML?", record.url, headers
                continue

            yield url, headers, "html", index_level, parser.recv_body()


def create_warc_from_corpus(documents, filename=None):
    """ Used mainly in tests to generate small .warc files """

    if filename is None:
        fd, filename = tempfile.mkstemp(suffix=".warc")
        os.close(fd)

    f = warc.open(filename, "w")

    for doc in documents:

        headers = "Connection: close\r\nContent-Type: text/html"
        if "headers" in doc:
            headers = "\r\n".join(["%s: %s" % (k, v) for k, v in doc["headers"].iteritems()])

        payload = "HTTP/1.1 200 OK\r\n" + headers + "\r\n\r\n" + doc["content"]

        record = warc.WARCRecord(payload=payload, headers={
            "Content-Type": "application/http; msgtype=response",
            "WARC-Type": "response",
            "WARC-Target-URI": doc["url"]
        })
        f.write_record(record)

    f.close()

    return filename
