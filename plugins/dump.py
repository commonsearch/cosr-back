from cosrlib.plugins import Plugin, PLUGIN_HOOK_ABORT


class DocumentMetadataParquet(Plugin):
    """ Stores intermediate documents in parquet format """

    hooks = frozenset(["spark_pipeline_action"])

    def spark_pipeline_action(self, sc, sqlc, rdd, document_schema, indexer):

        doc_df = sqlc.createDataFrame(rdd, document_schema)

        if self.args.get("coalesce"):
            doc_df = doc_df.coalesce(int(self.args["coalesce"]))

        doc_df.write.parquet(self.args["path"])

        if self.args.get("abort"):
            return PLUGIN_HOOK_ABORT
