from __future__ import absolute_import, division, print_function, unicode_literals

import os
import sys
import shutil
import urlparse
import time

import boto

# Add the base cosr-back directory to the Python import path
# pylint: disable=wrong-import-position
if os.environ.get("COSR_PATH_BACK"):
    sys.path.insert(-1, os.environ.get("COSR_PATH_BACK"))
elif os.path.isdir("/cosr/back"):
    sys.path.insert(-1, "/cosr/back")

from cosrlib.spark import SparkJob, sql
from pyspark.storagelevel import StorageLevel
# from pyspark.sql import types as SparkTypes


class PageRankJob(SparkJob):
    """ Compute PageRank from a WebGraph dump """

    name = "Common Search PageRank"

    def add_arguments(self, parser):

        parser.add_argument("--webgraph", default=None, type=str,
                            help="Link to a parquet directory with edges and vertices subdirectories")

        parser.add_argument("--maxiter", default=5, type=int,
                            help="Maximum iterations for the PageRank algorithm")

        parser.add_argument("--tol", default=0.001, type=float,
                            help="Tolerance for max rank diffs at each iteration. -1 to disable.")

        parser.add_argument("--precision", default=0.000001, type=float,
                            help="Don't transmit PageRank when there was less than this diff")

        parser.add_argument("--maxedges", default=0, type=int,
                            help="Maximum number of edges to consider")

        parser.add_argument("--maxvertices", default=0, type=int,
                            help="Maximum number of vertices to consider")

        parser.add_argument("--dump", default=None, type=str,
                            help="Directory for storing list of pageranks by domain")

        parser.add_argument("--include_orphans", default=False, action="store_true",
                            help="Add orphan vertices, not linked to by any other one.")

        parser.add_argument("--shuffle_partitions", default=10, type=int,
                            help="Number of shuffle partitions to use in the Spark pipeline")

        parser.add_argument("--gzip", default=False, action="store_true",
                            help="Save dump as gzip")

        parser.add_argument("--tmpdir", default="/tmp/cosr_spark_pagerank", type=str,
                            help="Temporary directory for storing iterations of the graph.")

        parser.add_argument("--stats", default=5, type=int,
                            help="Run stats every N iteration")

        parser.add_argument("--top_diffs", default=0, type=int,
                            help="Print top N pagerank diffs at each stats iteration")

    def run_job(self, sc, sqlc):

        self.clean_tmpdir()

        try:

            self.custom_pagerank(sc, sqlc)
            # self.custom_pagerank_2(sc, sqlc)
            # self.graphframes_pagerank(sc, sqlc)
        finally:
            self.clean_tmpdir()

    def clean_tmpdir(self, directory=None):
        """ Delete a folder with temporary results. If no folder passed, delete the whole tmpdir path """

        tmpdir = directory or self.args.tmpdir

        if not tmpdir:
            print("No tmpdir configured! Will probably run out of memory.")
            return

        # Local filepath
        if tmpdir.startswith("/") and os.path.isdir(tmpdir):
            shutil.rmtree(tmpdir)

        # S3
        elif tmpdir.startswith("s3a://"):
            parsed = urlparse.urlparse(tmpdir)
            conn = boto.connect_s3(os.getenv("AWS_ACCESS_KEY_ID"), os.getenv("AWS_SECRET_ACCESS_KEY"))
            bucket = conn.get_bucket(parsed.netloc)
            path = parsed.path or "/cosr_spark_pagerank"
            delete_key_list = list(bucket.list(prefix=path[1:]))  # No leading slash
            if len(delete_key_list) > 0:
                try:
                    bucket.delete_keys(delete_key_list)
                except Exception, e:  # pylint: disable=broad-except
                    print("Exception when cleaning tmpdir: %s" % e)

        # TODO
        elif tmpdir.startswith("hdfs://"):
            pass

    def wait_for_tmpdir(self, tmpdir):
        if tmpdir.startswith("s3a://"):
            parsed = urlparse.urlparse(tmpdir)
            conn = boto.connect_s3(os.getenv("AWS_ACCESS_KEY_ID"), os.getenv("AWS_SECRET_ACCESS_KEY"))
            bucket = conn.get_bucket(parsed.netloc)

            _success = os.path.join(parsed.path, "_SUCCESS")

            while not bucket.get_key(_success):
                print("Waiting for %s ..." % _success)
                time.sleep(10)

    def custom_pagerank(self, sc, sqlc):
        """ Our own PageRank implementation, based on Spark SQL and Pregel-like behaviour """
        # pylint: disable=too-many-statements

        # sc.setCheckpointDir("/tmp/spark-checkpoints")

        edge_df = sqlc.read.load(os.path.join(self.args.webgraph, "edges"))

        if self.args.maxedges:
            edge_df = edge_df.limit(self.args.maxedges)

        vertex_df = sqlc.read.load(os.path.join(self.args.webgraph, "vertices"))

        if self.args.maxvertices:
            vertex_df = vertex_df.limit(self.args.maxvertices)

        sqlc.setConf("spark.sql.shuffle.partitions", str(self.args.shuffle_partitions))

        edge_df.persist(StorageLevel.MEMORY_AND_DISK)
        vertex_df.persist(StorageLevel.MEMORY_AND_DISK)

        print("Starting iterations. %s edges, %s vertices." % (edge_df.count(), vertex_df.count()))

        # TODO: bootstrap with previous pageranks to accelerate convergence?
        ranks_df = sql(sqlc, """
            SELECT vertices.id id, cast(1.0 as float) rank
            FROM vertices
            JOIN edges ON edges.dst = vertices.id
            GROUP BY vertices.id
        """, {"vertices": vertex_df, "edges": edge_df})

        # TODO: optimize further by taking out outDegree=0 vertices and computing their pagerank
        # as a post-filter.
        # LEFT OUTER JOIN edges edges_src on edges_src.src  = vertices.id
        # WHERE edges_src.src IS NOT NULL

        iteration_tmpdir = None

        for iteration in range(self.args.maxiter):

            changed_ranks_df = sql(sqlc, """
                SELECT
                    edges.dst id,
                    cast(
                        0.15 + 0.85 * sum(COALESCE(ranks_src.rank, 0.15) * edges.weight)
                        as float
                    ) rank_new,
                    first(ranks_dst.rank) rank_old
                FROM edges
                LEFT OUTER JOIN ranks_src ON edges.src = ranks_src.id
                LEFT OUTER JOIN ranks_dst ON edges.dst = ranks_dst.id
                GROUP BY edges.dst
                HAVING ABS(rank_old - rank_new) > %s
            """ % self.args.precision, {"ranks_src": ranks_df, "ranks_dst": ranks_df, "edges": edge_df})

            # Every N iterations, we check if we got below the tolerance level.
            if (self.args.tol >= 0 or self.args.stats > 0) and (iteration % self.args.stats == 0):

                changed_ranks_df.persist(StorageLevel.MEMORY_AND_DISK)

                stats_df = sql(sqlc, """
                    SELECT
                        sum(abs(rank_new - rank_old)) as sum_diff,
                        count(*) as count_diff,
                        min(abs(rank_new - rank_old)) as min_diff,
                        max(abs(rank_new - rank_old)) as max_diff,
                        avg(abs(rank_new - rank_old)) as avg_diff,
                        stddev(abs(rank_new - rank_old)) as stddev_diff
                    FROM changes
                """, {"changes": changed_ranks_df})

                stats = stats_df.collect()[0]

                print("Iteration %s, %s changed ranks" % (iteration, stats["count_diff"]))
                print("Stats: %s" % repr(stats))

                if (stats["count_diff"] == 0) or (stats["max_diff"] <= self.args.tol):
                    print("Max diff was below tolerance: stopping iterations!")
                    break

                if self.args.top_diffs > 0:

                    top_changes_df = sql(sqlc, """
                        SELECT
                            (rank_new - rank_old) diff,
                            rank_old,
                            rank_new,
                            names.domain domain
                        FROM changes
                        JOIN names ON names.id = changes.id
                        ORDER BY abs(rank_new - rank_old) DESC
                    """, {"changes": changed_ranks_df, "names": vertex_df})

                    print("Top %s diffs" % self.args.top_changes)
                    print("\n".join([
                        "%3.3f (%3.3f => %3.3f) %s " % x
                        for x in top_changes_df.limit(self.args.top_diffs).collect()
                    ]))

                    top_changes_df.unpersist()

            new_ranks_df = sql(sqlc, """
                SELECT ranks.id id, COALESCE(changed_ranks.rank_new, ranks.rank) rank
                FROM ranks
                LEFT JOIN changed_ranks ON changed_ranks.id = ranks.id
            """, {"ranks": ranks_df, "changed_ranks": changed_ranks_df})

            if (iteration + 1) % 5 != 0:

                new_ranks_df.persist(StorageLevel.MEMORY_AND_DISK)

                new_ranks_df.count()  # Materialize the RDD

                print("Iteration %s cached" % (iteration, ))

                ranks_df.unpersist()
                changed_ranks_df.unpersist()
                ranks_df = new_ranks_df

            # At this point we need to break the RDD dependency chain
            # Writing & loading Parquet seems to be more efficient than checkpointing the RDD.
            else:

                print("Iteration %s, saving to parquet" % iteration)

                iteration_tmpdir_previous = iteration_tmpdir
                iteration_tmpdir = os.path.join(self.args.tmpdir, "iter_%s" % iteration)

                new_ranks_df.write.parquet(iteration_tmpdir)

                # S3 in us-east-1 should support read-after-write consistency since 2015
                # but we still have transient errors
                self.wait_for_tmpdir(iteration_tmpdir)

                new_ranks_df.unpersist()
                ranks_df.unpersist()
                changed_ranks_df.unpersist()

                ranks_df = sqlc.read.load(iteration_tmpdir)

                if iteration_tmpdir_previous is not None:
                    self.clean_tmpdir(directory=iteration_tmpdir_previous)

        if self.args.include_orphans:

            ranks_df = ranks_df.unionAll(sql(sqlc, """
                SELECT vertices.id id, cast(0.15 as float) rank
                FROM vertices
                LEFT OUTER JOIN edges ON edges.dst  = vertices.id
                WHERE edges.dst is NULL
            """, {"vertices": vertex_df, "edges": edge_df}))

        # No more need for the edges after iterations
        edge_df.unpersist()

        final_df = sql(sqlc, """
            SELECT CONCAT(names.domain, ' ', ranks.rank) r
            FROM ranks
            JOIN names ON names.id = ranks.id
            ORDER BY ranks.rank DESC
        """, {"names": vertex_df, "ranks": ranks_df})

        if self.args.dump:

            final_df.coalesce(1).write.text(
                self.args.dump,
                compression="gzip" if self.args.gzip else "none"
            )

        else:
            print(final_df.rdd.collect())

    def custom_pagerank_2(self, sc, sqlc):
        """ Alternative PageRank implementation, with fixed number of steps """

        sc.setCheckpointDir("/tmp/spark-checkpoints")

        # ranks_schema = SparkTypes.StructType([
        #     SparkTypes.StructField("id", SparkTypes.LongType(), nullable=False),
        #     SparkTypes.StructField("rank", SparkTypes.FloatType(), nullable=False)
        # ])

        edge_df = sqlc.read.load(os.path.join(self.args.webgraph, "edges"))

        if self.args.maxedges:
            edge_df = edge_df.limit(self.args.maxedges)

        vertex_df = sqlc.read.load(os.path.join(self.args.webgraph, "vertices"))

        if self.args.maxvertices:
            vertex_df = vertex_df.limit(self.args.maxvertices)

        sqlc.setConf("spark.sql.shuffle.partitions", str(self.args.shuffle_partitions))

        # TODO: bootstrap with previous pageranks to accelerate convergence?
        ranks_df = sql(sqlc, """
            SELECT id, cast(1.0 as float) rank
            FROM vertices
        """, {"vertices": vertex_df})

        edge_df.persist()
        vertex_df.persist()
        print("Starting iterations. %s edges, %s vertices." % (edge_df.count(), vertex_df.count()))

        iteration_tmpdir = None

        for iteration in range(self.args.maxiter):

            new_ranks_df = sql(sqlc, """
                SELECT ranks.id id, cast(0.15 + 0.85 * COALESCE(contribs.contrib, 0) as float) rank
                FROM ranks
                LEFT OUTER JOIN (
                    SELECT edges.dst id, cast(sum(ranks.rank * COALESCE(edges.weight, 0)) as float) contrib
                    FROM edges
                    LEFT OUTER JOIN ranks ON edges.src = ranks.id
                    GROUP BY edges.dst
                ) contribs ON contribs.id = ranks.id
            """, {"ranks": ranks_df, "edges": edge_df})

            # At this point we need to break the RDD dependency chain
            # Writing & loading Parquet seems to be more efficient than checkpointing the RDD.

            iteration_tmpdir_previous = iteration_tmpdir
            iteration_tmpdir = os.path.join(self.args.tmpdir, "iter_%s" % iteration)

            # Every N iterations, we check if we got below the tolerance level.
            if (self.args.tol >= 0 or self.args.stats > 0) and (iteration % self.args.stats == 0):

                new_ranks_df.persist()
                ranks_df.persist()
                vertex_df.persist()

                stats_df = sql(sqlc, """
                    SELECT
                        sum(diff) as sum_diff,
                        count(*) as count_diff,
                        min(diff) as min_diff,
                        max(diff) as max_diff,
                        avg(diff) as avg_diff,
                        stddev(diff) as stddev_diff
                    FROM (
                        SELECT ABS(old_ranks.rank - new_ranks.rank) diff
                        FROM old_ranks
                        JOIN new_ranks ON old_ranks.id = new_ranks.id
                        WHERE old_ranks.rank != new_ranks.rank
                    ) diffs
                """, {"old_ranks": ranks_df, "new_ranks": new_ranks_df})

                stats = stats_df.collect()[0]
                print("Max diff at iteration %s : %s" % (iteration, stats["max_diff"]))
                print("Other stats: %s" % repr(stats))

                if (stats["count_diff"] == 0) or (stats["max_diff"] <= self.args.tol):
                    print("Max diff was below tolerance: stopping iterations!")
                    break

                top_diffs_df = sql(sqlc, """
                    SELECT
                        (new_ranks.rank - old_ranks.rank) diff,
                        old_ranks.rank old_rank,
                        new_ranks.rank new_rank,
                        names.domain domain
                    FROM old_ranks
                    JOIN new_ranks ON old_ranks.id = new_ranks.id
                    JOIN names ON names.id = old_ranks.id
                    WHERE old_ranks.rank != new_ranks.rank
                    ORDER BY ABS(diff) DESC
                """, {"old_ranks": ranks_df, "new_ranks": new_ranks_df, "names": vertex_df})

                print("Top 100 diffs")
                print("\n".join(["%3.3f %3.3f %3.3f %s " % x for x in top_diffs_df.limit(100).collect()]))

            new_ranks_df.write.parquet(iteration_tmpdir)

            # S3 in us-east-1 should support read-after-write consistency since 2015
            # but we still have transient errors
            self.wait_for_tmpdir(iteration_tmpdir)

            new_ranks_df.unpersist()
            ranks_df.unpersist()

            ranks_df = sqlc.read.load(iteration_tmpdir)

            if iteration_tmpdir_previous is not None:
                self.clean_tmpdir(directory=iteration_tmpdir_previous)

        # No more need for the edges after iterations
        edge_df.unpersist()

        final_df = sql(sqlc, """
            SELECT CONCAT(names.domain, ' ', ranks.rank) r
            FROM ranks
            JOIN names ON names.id = ranks.id
            ORDER BY ranks.rank DESC
        """, {"names": vertex_df, "ranks": ranks_df})

        if self.args.dump:

            final_df.coalesce(1).write.text(
                self.args.dump,
                compression="gzip" if self.args.gzip else "none"
            )

        else:
            print(final_df.rdd.collect())

    def graphframes_pagerank(self, sc, sqlc):
        """ GraphFrame's PageRank implementation """

        from graphframes import GraphFrame  # pylint: disable=import-error

        edge_df = sqlc.read.load(os.path.join(self.args.webgraph, "edges"))
        vertex_df = sqlc.read.load(os.path.join(self.args.webgraph, "vertices"))

        graph = GraphFrame(vertex_df, edge_df)

        withPageRank = graph.pageRank(maxIter=self.args.maxiter)

        final_df = sql(sqlc, """
            SELECT CONCAT(ranks.domain, ' ', ranks.pagerank) r
            FROM ranks
            ORDER BY ranks.pagerank DESC
        """, {"ranks": withPageRank.vertices})

        if self.args.dump:

            final_df.coalesce(1).write.text(
                self.args.dump,
                compression="gzip" if self.args.gzip else "none"
            )

        else:
            print(final_df.rdd.collect())

if __name__ == "__main__":
    job = PageRankJob()
    job.run()
