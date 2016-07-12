from cosrlib.sources import Source


class CorpusSource(Source):
    """ Source that yields documents from a static corpus. Mostly used in tests """

    def iter_items(self):

        for doc in self.args["docs"]:
            yield (
                doc["url"].encode("utf-8"),
                {"Content-Type": "text/html"},
                "html",
                doc["content"].encode("utf-8")
            )
