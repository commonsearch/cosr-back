import os
import shutil

from pyspark.sql import types as SparkTypes

from cosrlib.spark import sql, SparkPlugin


class MostExternallyLinkedPages(SparkPlugin):
    """ Saves a list of most externally linked pages on a domain """

    def hook_spark_pipeline_init(self, sc, sqlc, schema, indexer):
        schema.append(SparkTypes.StructField("external_links", SparkTypes.ArrayType(SparkTypes.StructType([
            SparkTypes.StructField("href", SparkTypes.StringType(), nullable=False)
            # TODO: link text
        ])), nullable=True))

        final_directory = self.args["path"]
        if final_directory and os.path.isdir(final_directory):
            shutil.rmtree(final_directory)

    def hook_document_post_index(self, document, metadata):
        """ Filters a document post-indexing """

        metadata["external_links"] = [
            {"href": row["href"].url} for row in document.get_external_hyperlinks()
        ]

    def hook_spark_pipeline_action(self, sc, sqlc, df, indexer):

        domain = self.args["domain"]

        if self.args.get("shuffle_partitions"):
            sqlc.setConf("spark.sql.shuffle.partitions", self.args["shuffle_partitions"])

        lines_df = sql(sqlc, """
            SELECT
                CONCAT(
                    regexp_replace(url_to, "^http(s?)://", ""),
                    " ",
                    COUNT(*),
                    " ",
                    CONCAT_WS(" ", COLLECT_LIST(url_from))
                ) r
            FROM (
                SELECT url url_from, EXPLODE(external_links.href) url_to
                FROM df
                WHERE size(external_links) > 0
            ) links
            WHERE SUBSTRING(
                PARSE_URL(links.url_to, "HOST"),
                LENGTH(PARSE_URL(links.url_to, "HOST")) - %s,
                %s
            ) == "%s"
            GROUP BY regexp_replace(url_to, "^http(s?)://", "")
            ORDER BY COUNT(*) DESC
        """ % (len(domain) - 1, len(domain), domain), {"df": df})

        if self.args.get("limit"):
            lines_df = lines_df.limit(int(self.args["limit"]))

        if self.args.get("partitions"):
            lines_df = lines_df.coalesce(int(self.args["partitions"]))
            lines_df.persist()
            print "Number of destination URLs: %s" % lines_df.count()

        self.save_dataframe(lines_df, "text")

        return True
