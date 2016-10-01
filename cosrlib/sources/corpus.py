from __future__ import absolute_import, division, print_function, unicode_literals

import json

from cosrlib.sources import Source
from cosrlib.url import URL


class CorpusSource(Source):
    """ Source that yields documents from a static corpus. Mostly used in tests """

    def get_partitions(self):

        if self.args.get("path"):
            return [{
                "path": self.args["path"]
            }]
        else:
            return [{
                "doc": doc
            } for doc in self.args.get("docs") or []]

    def iter_items(self, partition):
        """ Partition can be either a single raw document, or a filepath to a JSON file """

        if partition.get("path"):
            with open(partition["path"], "r") as f:
                docs = json.load(f)
        else:
            docs = [partition["doc"]]

        for doc in docs:

            url = URL(doc["url"].encode("utf-8"))

            do_parse, index_level = self.qualify_url(url)

            if do_parse:

                yield (
                    url,
                    {"Content-Type": "text/html"},
                    "html",
                    index_level,
                    doc["content"].encode("utf-8")
                )
