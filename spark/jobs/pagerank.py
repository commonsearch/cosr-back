import os
import sys

# Add the base cosr-back directory to the Python import path
# pylint: disable=wrong-import-position
if os.environ.get("COSR_PATH_BACK"):
    sys.path.insert(-1, os.environ.get("COSR_PATH_BACK"))
elif os.path.isdir("/cosr/back"):
    sys.path.insert(-1, "/cosr/back")

from cosrlib.spark import SparkJob, sql


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

        parser.add_argument("--maxedges", default=0, type=int,
                            help="Maximum number of edges to consider")

        parser.add_argument("--maxvertices", default=0, type=int,
                            help="Maximum number of vertices to consider")

        parser.add_argument("--dump", default=None, type=str,
                            help="Directory for storing list of pageranks by domain")

        parser.add_argument("--shuffle_partitions", default=10, type=int,
                            help="Number of shuffle partitions to use in the Spark pipeline")

        parser.add_argument("--gzip", default=False, action="store_true",
                            help="Save dump as gzip")

    def run_job(self, sc, sqlc):

        self.custom_pagerank(sc, sqlc)

    def custom_pagerank(self, sc, sqlc):
        """ Our own PageRank implementation """

        edge_df = sqlc.read.load(self.args.edges)

        if self.args.maxedges:
            edge_df = edge_df.limit(self.args.maxedges)

        vertex_df = sqlc.read.load(self.args.vertices)

        if self.args.maxvertices:
            vertex_df = vertex_df.limit(self.args.maxvertices)

        sqlc.setConf("spark.sql.shuffle.partitions", str(self.args.shuffle_partitions))

        # TODO: bootstrap with previous pageranks to accelerate convergence?
        ranks_df = sql(sqlc, """
            SELECT id, cast(1.0 as float) rank
            FROM vertices
        """, {"vertices": vertex_df})

        for _ in range(self.args.maxiter):

            contribs_df = sql(sqlc, """
                SELECT edges.dst id, cast(sum(ranks.rank * COALESCE(edges.weight, 0)) as float) contrib
                FROM edges
                LEFT OUTER JOIN ranks ON edges.src = ranks.id
                GROUP BY edges.dst
            """, {"edges": edge_df, "ranks": ranks_df})

            ranks_df = sql(sqlc, """
                SELECT ranks.id id, cast(0.15 + 0.85 * COALESCE(contribs.contrib, 0) as float) rank
                FROM ranks
                LEFT OUTER JOIN contribs ON contribs.id = ranks.id
            """, {"ranks": ranks_df, "contribs": contribs_df})

            ranks_df.persist()

        final_df = sql(sqlc, """
            SELECT CONCAT(names.domain, ' ', ranks.rank) r
            FROM ranks
            JOIN names ON names.id = ranks.id
            ORDER BY ranks.rank DESC
        """, {"names": vertex_df, "ranks": ranks_df})

        if self.args.dump:

            final_df.repartition(1).write.text(
                self.args.dump,
                compression="gzip" if self.args.gzip else "none"
            )

        else:
            print final_df.rdd.collect()

    def graphframes_pagerank(self, sc, sqlc):
        """ Use GraphFrame's PageRank implementation """

        from graphframes import GraphFrame  # pylint: disable=import-error

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
