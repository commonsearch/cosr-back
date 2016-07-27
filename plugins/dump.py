import os
import shutil

from cosrlib.plugins import Plugin, PLUGIN_HOOK_ABORT


class DocumentMetadataParquet(Plugin):
    """ Stores intermediate documents in parquet format """

    hooks = frozenset(["spark_pipeline_action"])

    def init(self):
        if self.args.get("path") and os.path.isdir(self.args["path"]):
            shutil.rmtree(self.args["path"])

    def spark_pipeline_action(self, sc, sqlc, doc_df, indexer):

        if self.args.get("coalesce"):
            doc_df = doc_df.coalesce(int(self.args["coalesce"]))

        doc_df.write.parquet(self.args["path"])

        if self.args.get("abort"):
            return PLUGIN_HOOK_ABORT
