from cosrlib.sources import Source
from cosrlib.url import URL


class CorpusSource(Source):
    """ Source that yields documents from a static corpus. Mostly used in tests """

    def iter_items(self):

        for doc in self.args["docs"]:

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
