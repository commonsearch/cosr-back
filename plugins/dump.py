from __future__ import absolute_import, division, print_function, unicode_literals

import os
import shutil

from cosrlib.plugins import PLUGIN_HOOK_ABORT
from cosrlib.spark import SparkPlugin


class DocumentMetadata(SparkPlugin):
    """ Stores intermediate documents in parquet format """

    def init(self):
        if self.args.get("path") and os.path.isdir(self.args["path"]):
            shutil.rmtree(self.args["path"])

        self.format = self.args.get("format") or "parquet"

    def hook_spark_pipeline_action(self, sc, sqlc, doc_df, indexer):

        self.save_dataframe(doc_df, self.format)

        if self.args.get("abort"):
            return PLUGIN_HOOK_ABORT

        return True
