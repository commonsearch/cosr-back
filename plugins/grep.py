from pyspark.sql import types as SparkTypes
from cosrlib.plugins import Plugin


class Words(Plugin):
    """ Finds documents containing a list of words in their indexable text (visible, non-boilerplate) """

    hooks = frozenset(["document_post_index", "spark_pipeline_action", "spark_pipeline_init"])

    def init(self):
        if not self.args.get("words"):
            raise Exception("grep.Words plugin needs words!")

        self.words = frozenset(self.args["words"].split(" "))

    def spark_pipeline_init(self, sc, sqlc, schema, indexer):
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

    def spark_pipeline_action(self, sc, sqlc, df, indexer):

        # TODO: we might be able to do that in pure Spark SQL
        def fmt(row):
            if not row or not row["grep_words"]:
                return []
            return ["%s %s" % (",".join(sorted(row["grep_words"])), row["url"])]

        rdd = df.rdd \
            .flatMap(fmt)

        if self.args.get("coalesce"):
            rdd = rdd.coalesce(int(self.args["coalesce"]), shuffle=bool(self.args.get("shuffle")))

        rdd.saveAsTextFile(self.args["path"])

        return True


class TextRegex(Plugin):
    """ Finds documents matching a regex on their visible text """
    pass


class HTMLRegex(Plugin):
    """ Finds documents matching a regex on their raw HTML """
    pass
