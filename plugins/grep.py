from __future__ import absolute_import, division, print_function, unicode_literals

from pyspark.sql import types as SparkTypes
from cosrlib.spark import sql, SparkPlugin


class Words(SparkPlugin):
    """ Finds documents containing a list of words in their indexable text (visible, non-boilerplate) """

    def init(self):
        if not self.args.get("words"):
            raise Exception("grep.Words plugin needs words!")

        self.words = frozenset(self.args["words"].split(" "))

    def hook_spark_pipeline_init(self, sc, sqlc, schema, indexer):
        schema.append(SparkTypes.StructField(
            "grep_words",
            SparkTypes.ArrayType(SparkTypes.StringType()),
            nullable=True
        ))

    def hook_document_post_index(self, document, metadata):
        """ Filters a document post-indexing """

        doc_words = document.get_all_words()

        match = doc_words.intersection(self.words)
        if len(match) > 0:
            print("WORD MATCH", match, document.source_url.url)
            metadata["grep_words"] = list(match)

    def hook_spark_pipeline_action(self, sc, sqlc, df, indexer):

        lines_df = sql(sqlc, """
            SELECT CONCAT(CONCAT_WS(",", SORT_ARRAY(grep_words)), " ", url) r
            FROM df
            WHERE size(grep_words) > 0
        """, {"df": df})

        self.save_dataframe(lines_df, "text")

        return True


# class TextRegex(SparkPlugin):
#     """ Finds documents matching a regex on their visible text """
#     pass


# class HTMLRegex(SparkPlugin):
#     """ Finds documents matching a regex on their raw HTML """
#     pass
