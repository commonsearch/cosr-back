from cosrlib.plugins import Plugin


class Words(Plugin):
    """ Finds documents containing a list of words in their indexable text (visible, non-boilerplate) """

    hooks = frozenset(["document_post_index", "spark_pipeline_collect"])

    def init(self):
        if not self.args.get("words"):
            raise Exception("grep.Words plugin needs words!")

        self.words = frozenset(self.args["words"].split(" "))

    def document_post_index(self, document, spark_response):
        """ Filters a document post-indexing """

        doc_words = document.get_all_words()

        match = doc_words.intersection(self.words)
        if len(match) > 0:
            print "WORD MATCH", match, document.source_url.url
            spark_response["grep_words"] = match

    def spark_pipeline_collect(self, sc, rdd, indexer):

        rdd = rdd \
            .filter(lambda row: "grep_words" in row) \
            .map(lambda row: "%s %s" % (",".join(sorted(row["grep_words"])), row["url"].url))

        if self.args.get("coalesce"):
            rdd = rdd.coalesce(int(self.args["coalesce"]), shuffle=bool(self.args.get("shuffle")))

        rdd.saveAsTextFile(self.args["dir"])
