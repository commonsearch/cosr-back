import os
import sys
import argparse

# Add the base cosr-back directory to the Python import path
# pylint: disable=wrong-import-position
if os.environ.get("COSR_PATH_BACK"):
    sys.path.insert(-1, os.environ.get("COSR_PATH_BACK"))
elif os.path.isdir("/cosr/back"):
    sys.path.insert(-1, "/cosr/back")
else:
    sys.path.insert(-1, os.path.normpath(os.path.join(__file__, "../../../")))

from pyspark import SparkContext, SparkConf  # pylint: disable=import-error
from pyspark.sql import SQLContext  # pylint: disable=import-error

from cosrlib.config import config

from graphframes import GraphFrame  # pylint: disable=import-error


def get_args():
    """ Returns the parsed arguments from the command line """

    parser = argparse.ArgumentParser(description='Compute PageRank from a WebGraph dump')

    parser.add_argument("--edges", default=None, type=str,
                        help="Link to a parquet file containing the edges")

    parser.add_argument("--vertices", default=None, type=str,
                        help="Link to a parquet file containing the vertices")

    parser.add_argument("--maxiter", default=5, type=int,
                        help="Maximum iterations for the PageRank algorithm")

    parser.add_argument("--dump", default=None, type=str,
                        help="Directory for storing list of pageranks by domain")

    parser.add_argument("--profile", action='store_true',
                        help="Profile Python usage")

    return parser.parse_args()

# Shared variables while indexing
args = get_args()


def spark_execute(sc, sqlc):

    edge_df = sqlc.read.load(args.edges)
    vertex_df = sqlc.read.load(args.vertices)

    graph = GraphFrame(vertex_df, edge_df)

    withPageRank = graph.pageRank(maxIter=args.maxiter)
    rdd = withPageRank.vertices.sort(withPageRank.vertices.pagerank.desc()).map(
        lambda x: "%s %s" % (x.domain, x.pagerank)
    ).coalesce(1)

    if args.dump:
        rdd.saveAsTextFile(args.dump)
    else:
        print rdd.collect()


def spark_main():
    """ Main Spark entry point """

    conf = SparkConf().setAll((
        ("spark.python.profile", "true" if args.profile else "false"),
        ("spark.ui.enabled", "false" if config["ENV"] in ("local", "ci") else "false"),
        ("spark.task.maxFailures", "20")
    ))

    # TODO could this be set somewhere in cosr-ops instead?
    executor_environment = {
        "_SPARK_IS_WORKER": "1"
    }
    if config["ENV"] == "prod":
        executor_environment.update({
            "PYTHONPATH": "/cosr/back",
            "PYSPARK_PYTHON": "/cosr/back/venv/bin/python",
            "LD_LIBRARY_PATH": "/usr/local/lib"
        })

    sc = SparkContext(appName="Common Search PageRank", conf=conf, environment=executor_environment)
    sqlc = SQLContext(sc)

    spark_execute(sc, sqlc)

    if args.profile:
        sc.show_profiles()

    sc.stop()


if __name__ == "__main__":
    spark_main()
