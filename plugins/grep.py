from pyspark.sql import types as SparkTypes
from cosrlib.plugins import Plugin


class Words(Plugin):
    """ Finds documents containing a list of words in their indexable text (visible, non-boilerplate) """

    hooks = frozenset(["document_post_index", "spark_pipeline_action", "document_schema"])

    def init(self):
        if not self.args.get("words"):
            raise Exception("grep.Words plugin needs words!")

        self.words = frozenset(self.args["words"].split(" "))

    def document_schema(self, schema):
        schema.append(SparkTypes.StructField(
            "grep_words",
            SparkTypes.ArrayType(SparkTypes.StringType()),
            nullable=True
        ))

    def document_post_index(self, document, metadata):
        """ Filters a document post-indexing """

        doc_words = document.get_all_words()

        match = doc_words.intersection(self.words)
        if len(match) > 0:
            print "WORD MATCH", match, document.source_url.url
            metadata["grep_words"] = list(match)

    def spark_pipeline_action(self, sc, sqlc, rdd, document_schema, indexer):

        rdd = rdd \
            .flatMap(lambda row: (
                ["%s %s" % (",".join(sorted(row["grep_words"])), row["url"])]
                if "grep_words" in row else []
            ))

        if self.args.get("coalesce"):
            rdd = rdd.coalesce(int(self.args["coalesce"]), shuffle=bool(self.args.get("shuffle")))

        rdd.saveAsTextFile(self.args["path"])


class TextRegex(Plugin):
    """ Finds documents matching a regex on their visible text """
    pass


class HTMLRegex(Plugin):
    """ Finds documents matching a regex on their raw HTML """
    pass
