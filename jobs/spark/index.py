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

from cosrlib.sources import load_source
from cosrlib.sources.commoncrawl import list_commoncrawl_warc_filenames
from cosrlib.indexer import Indexer
from cosrlib.utils import ignore_exceptions_generator  # ignore_exceptions
from cosrlib.config import config
from cosrlib.plugins import load_plugins, exec_hook, parse_plugin_cli_args


def get_args():
    """ Returns the parsed arguments from the command line """

    parser = argparse.ArgumentParser(description='Index documents from diverse sources to Elasticsearch')

    parser.add_argument("--source", default=None, type=str, action="append",
                        help="Source to index. May be specified multiple times.")

    parser.add_argument("--plugin", type=str, action="append",
                        help="Plugin to execute in the pipeline. May be specified multiple times.")

    parser.add_argument("--only_homepages", action='store_true',
                        help="Only index homepages")

    parser.add_argument("--domain_whitelist", default=None, type=str,
                        help="Comma-separated list of domains to fully index")

    parser.add_argument("--domain_blacklist", default=None, type=str,
                        help="Comma-separated list of domains not to index")

    parser.add_argument("--profile", action='store_true',
                        help="Profile Python usage")

    return parser.parse_args()

# Shared variables while indexing
args = get_args()
indexer = Indexer()
urlclient = indexer.urlclient

if not args.source:
    raise Exception("You didn't specify any Document Sources with --source ! Nothing to index.")


def index_from_source(source, _indexer, **kwargs):
    """ Indexes all documents from a source """

    plugins = load_plugins(args.plugin)

    for document in source.iter_documents():

        print "Indexing", document.source_url.url

        resp = {}

        exec_hook(plugins, "document_pre_index", document, resp)

        resp.update(_indexer.index_document(document, **kwargs))

        exec_hook(plugins, "document_post_index", document, resp)

        yield resp


@ignore_exceptions_generator
def index_documents(documentsource):
    """ Indexes documents from a source """

    # Must do that because we may be in a different process!
    # TODO: there might be a bug between spark and mprpc's Cython, this shouldn't be necessary.
    indexer.connect()

    print "Now working on %s" % documentsource

    for resp in index_from_source(documentsource, indexer):
        yield resp

    indexer.flush()

    # When in tests, we want results to be available immediately!
    if config["ENV"] != "prod":
        indexer.refresh()


def print_rows(row):
    print "Indexed", row["url"].url


def spark_execute(sc):
    """ Execute our indexing pipeline with a Spark Context """

    plugins = load_plugins(args.plugin)

    filters = {
        "domain_whitelist": args.domain_whitelist,
        "domain_blacklist": args.domain_blacklist,
        "only_homepages": args.only_homepages
    }

    # Spark RDD containing everything we indexed
    all_indexed_documents = sc.emptyRDD()

    for source_spec in args.source:

        source_name, source_args = parse_plugin_cli_args(source_spec)

        if source_name == "commoncrawl":
            partitions = list_commoncrawl_warc_filenames(
                limit=source_args.get("limit"),
                skip=source_args.get("skip")
            )

            def index_partition(filename):
                ds = load_source("commoncrawl", {"file": filename, "filters": filters})
                return index_documents(ds)

        elif source_name == "warc":

            # We have been given a .txt file with a list of WARC file paths
            if source_args["file"].endswith(".txt"):
                with open(source_args["file"], "rb") as f:
                    partitions = [x.strip() for x in f.readlines()]

            # Single WARC file path
            else:
                partitions = [source_args["file"]]

            def index_partition(filename):
                ds = load_source("webarchive", {"file": filename, "filters": filters})
                return index_documents(ds)

        elif source_name == "wikidata":

            partitions = ["__wikidata_single_dump__"]

            def index_partition(_):
                ds = load_source("wikidata", {})
                return index_documents(ds)

        elif source_name == "corpus":

            partitions = source_args["docs"]

            def index_partition(doc):
                ds = load_source("corpus", {"docs": [doc]})
                return index_documents(ds)

        elif source_name == "url":

            partitions = source_args.get("urls") or [source_args["url"]]

            def index_partition(url):
                ds = load_source("url", {"urls": [url]})
                return index_documents(ds)

        # Split indexing of each partition in Spark workers
        indexed_documents = sc \
            .parallelize(partitions, len(partitions)) \
            .flatMap(index_partition)

        # This .count() call is what triggers the Spark pipeline so far
        print "Source %s indexed %s documents" % (source_name, indexed_documents.count())

        all_indexed_documents = all_indexed_documents.union(indexed_documents)

    exec_hook(plugins, "spark_pipeline_collect", sc, all_indexed_documents, indexer)


def spark_main():
    """ Main Spark entry point """

    conf = SparkConf().setAll((
        ("spark.python.profile", "true" if args.profile else "false"),
        ("spark.ui.enabled", "false" if config["ENV"] in ("local", "ci") else "false"),
        ("spark.task.maxFailures", "20")
    ))

    # TODO could this be set somewhere in cosr-ops instead?
    executor_environment = {}
    if config["ENV"] == "prod":
        executor_environment = {
            "PYTHONPATH": "/cosr/back",
            "PYSPARK_PYTHON": "/cosr/back/venv/bin/python",
            "LD_LIBRARY_PATH": "/usr/local/lib"
        }

    sc = SparkContext(appName="Common Search Indexing", conf=conf, environment=executor_environment)

    spark_execute(sc)

    if args.profile:
        sc.show_profiles()

    sc.stop()


if __name__ == "__main__":
    spark_main()
