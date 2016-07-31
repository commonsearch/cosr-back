import os
import sys

# Add the base cosr-back directory to the Python import path
# pylint: disable=wrong-import-position
if os.environ.get("COSR_PATH_BACK"):
    sys.path.insert(-1, os.environ.get("COSR_PATH_BACK"))
elif os.path.isdir("/cosr/back"):
    sys.path.insert(-1, "/cosr/back")

from cosrlib.spark import SparkJob, sql


class WebGraphJob(SparkJob):
    """ Common operations on WebGraph files """

    name = "Common Search WebGraph tools"

    def add_arguments(self, parser):

        parser.add_argument("--input_parquet", default=None, type=str,
                            help="Path to parquet directories for edges & vertices")

        parser.add_argument("--output_txt", default=None, type=str,
                            help="Path to txt output directory")

        parser.add_argument("--gzip", default=False, action="store_true",
                            help="Save dump as gzip")

        parser.add_argument("--coalesce", default=1, type=int,
                            help="Number of partitions to save")

        # TODO option to rewrite IDs (maybe with row_number()) ?

    def run_job(self, sc, sqlc):

        if self.args.input_parquet:
            edge_df = sqlc.read.load(os.path.join(self.args.input_parquet, "edges"))
            vertex_df = sqlc.read.load(os.path.join(self.args.input_parquet, "vertices"))
        else:
            raise Exception("No input given!")

        if self.args.output_txt:
            vertices = sql(sqlc, """
                SELECT CONCAT(id, " ", domain) r
                FROM vertices
            """, {"vertices": vertex_df})

            edges = sql(sqlc, """
                SELECT CONCAT(src, " ", dst) r
                FROM edges
            """, {"edges": edge_df})

            vertices.coalesce(self.args.coalesce).write.text(
                os.path.join(self.args.output_txt, "vertices"),
                compression="gzip" if self.args.gzip else "none"
            )

            edges.coalesce(self.args.coalesce).write.text(
                os.path.join(self.args.output_txt, "edges"),
                compression="gzip" if self.args.gzip else "none"
            )

if __name__ == "__main__":
    job = WebGraphJob()
    job.run()
