import os
import sys

# Add the base cosr-back directory to the Python import path
# pylint: disable=wrong-import-position
if os.environ.get("COSR_PATH_BACK"):
    sys.path.insert(-1, os.environ.get("COSR_PATH_BACK"))
elif os.path.isdir("/cosr/back"):
    sys.path.insert(-1, "/cosr/back")

from graphframes import GraphFrame  # pylint: disable=import-error

from cosrlib.spark import SparkJob


class PageRankJob(SparkJob):
    """ Compute PageRank from a WebGraph dump """

    name = "Common Search PageRank"

    def add_arguments(self, parser):

        parser.add_argument("--edges", default=None, type=str,
                            help="Link to a parquet file containing the edges")

        parser.add_argument("--vertices", default=None, type=str,
                            help="Link to a parquet file containing the vertices")

        parser.add_argument("--maxiter", default=5, type=int,
                            help="Maximum iterations for the PageRank algorithm")

        parser.add_argument("--dump", default=None, type=str,
                            help="Directory for storing list of pageranks by domain")

        parser.add_argument("--gzip", default=False, action="store_true",
                            help="Save dump as gzip")

    def run_job(self, sc, sqlc):

        edge_df = sqlc.read.load(self.args.edges)
        vertex_df = sqlc.read.load(self.args.vertices)

        graph = GraphFrame(vertex_df, edge_df)

        withPageRank = graph.pageRank(maxIter=self.args.maxiter)
        rdd = withPageRank.vertices.sort(withPageRank.vertices.pagerank.desc()).map(
            lambda x: "%s %s" % (x.domain, x.pagerank)
        ).coalesce(1)

        if self.args.dump:

            codec = None
            if self.args.gzip:
                codec = "org.apache.hadoop.io.compress.GzipCodec"

            rdd.saveAsTextFile(self.args.dump, codec)

        else:
            print rdd.collect()

if __name__ == "__main__":
    job = PageRankJob()
    job.run()
