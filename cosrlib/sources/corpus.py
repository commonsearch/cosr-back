import json

from cosrlib.sources import Source
from cosrlib.url import URL


class CorpusSource(Source):
    """ Source that yields documents from a static corpus. Mostly used in tests """

    def iter_items(self):

        if self.args.get("path"):
            with open(self.args["path"], "r") as f:
                docs = json.load(f)
        else:
            docs = self.args["docs"]

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
