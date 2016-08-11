import json

from cosrlib.sources import Source
from cosrlib.url import URL


class CorpusSource(Source):
    """ Source that yields documents from a static corpus. Mostly used in tests """

    def get_partitions(self):

        if self.args.get("path"):
            return [self.args["path"]]
        else:
            return self.args.get("docs") or []

    def iter_items(self, partition):
        """ Partition can be either a single raw document, or a filepath to a JSON file """

        if isinstance(partition, dict):
            docs = [partition]

        else:
            with open(partition, "r") as f:
                docs = json.load(f)

        for doc in docs:

            url = URL(doc["url"].encode("utf-8"))

            do_parse, index_level = self.filter_url(url)

            if do_parse:

                yield (
                    url,
                    {"Content-Type": "text/html"},
                    "html",
                    index_level,
                    doc["content"].encode("utf-8")
                )
