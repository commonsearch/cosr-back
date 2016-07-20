import os
import sys

# Add the base cosr-back directory to the Python import path
# pylint: disable=wrong-import-position
if os.environ.get("COSR_PATH_BACK"):
    sys.path.insert(-1, os.environ.get("COSR_PATH_BACK"))
elif os.path.isdir("/cosr/back"):
    sys.path.insert(-1, "/cosr/back")

# from pyspark.sql import types as SparkTypes
from pyspark.storagelevel import StorageLevel

from cosrlib.sources import load_source
from cosrlib.sources.commoncrawl import list_commoncrawl_warc_filenames
from cosrlib.indexer import Indexer
from cosrlib.utils import ignore_exceptions_generator  # ignore_exceptions
from cosrlib.config import config
from cosrlib.plugins import load_plugins, exec_hook, parse_plugin_cli_args
from cosrlib.spark import SparkJob, setup_spark_worker


indexer = Indexer()
urlclient = indexer.urlclient


class IndexJob(SparkJob):
    """ Index documents from diverse sources to Elasticsearch """

    name = "Common Search Indexer"

    plugins = None
    accumulator_indexed = None

    def add_arguments(self, parser):

        parser.add_argument("--source", required=True, default=None, type=str, action="append",
                            help="Source to index. May be specified multiple times.")

        parser.add_argument("--plugin", type=str, action="append",
                            help="Plugin to execute in the pipeline. May be specified multiple times.")

    def run_job(self, sc, sqlc):

        """ Execute our indexing pipeline with a Spark Context """

        self.plugins = load_plugins(self.args.plugin)
        self.accumulator_indexed = sc.accumulator(0)

        maxdocs = {}

        # What fields will be sent to Spark
        # document_schema_columns = [
        #     SparkTypes.StructField("id", SparkTypes.LongType(), nullable=False),
        #     SparkTypes.StructField("url", SparkTypes.StringType(), nullable=False)
        # ]

        # # Some plugins need to add new fields to the schema
        # exec_hook(plugins, "document_schema", document_schema_columns)

        # document_schema = SparkTypes.StructType(document_schema_columns)

        # Spark DataFrame containing everything we indexed

        all_indexed_documents = sc.emptyRDD()  # sqlc.createDataFrame(sc.emptyRDD(), document_schema)

        for source_spec in self.args.source:

            source_name, source_args = parse_plugin_cli_args(source_spec)
            maxdocs[source_spec] = source_args.get("maxdocs")

            if source_name == "commoncrawl":
                partitions = list_commoncrawl_warc_filenames(
                    limit=source_args.get("limit"),
                    skip=source_args.get("skip"),
                    version=source_args.get("version")
                )

                def index_partition(filename):
                    ds = load_source("commoncrawl", {
                        "file": filename,
                        "plugins": self.plugins,
                        "maxdocs": maxdocs[source_spec]  # pylint: disable=cell-var-from-loop
                    })
                    return self.index_documents(ds)

            elif source_name == "warc":

                # We have been given a .txt file with a list of WARC file paths
                if source_args["file"].endswith(".txt"):
                    with open(source_args["file"], "rb") as f:
                        partitions = [x.strip() for x in f.readlines()]

                # Single WARC file path
                else:
                    partitions = [source_args["file"]]

                def index_partition(filename):
                    ds = load_source("webarchive", {
                        "file": filename,
                        "plugins": self.plugins,
                        "maxdocs": maxdocs[source_spec]  # pylint: disable=cell-var-from-loop
                    })
                    return self.index_documents(ds)

            elif source_name == "wikidata":

                partitions = ["__wikidata_single_dump__"]

                def index_partition(_):
                    ds = load_source("wikidata", {
                        "maxdocs": maxdocs[source_spec],  # pylint: disable=cell-var-from-loop
                        "plugins": self.plugins
                    })
                    return self.index_documents(ds)

            elif source_name == "corpus":

                partitions = source_args["docs"]

                def index_partition(doc):

                    ds = load_source("corpus", {
                        "maxdocs": maxdocs[source_spec],  # pylint: disable=cell-var-from-loop
                        "docs": [doc],
                        "plugins": self.plugins
                    })
                    return self.index_documents(ds)

            elif source_name == "url":

                partitions = source_args.get("urls") or [source_args["url"]]

                def index_partition(url):
                    ds = load_source("url", {
                        "urls": [url],
                        "plugins": self.plugins
                    })
                    return self.index_documents(ds)

            # Split indexing of each partition in Spark workers
            indexed_documents = sc \
                .parallelize(partitions, len(partitions)) \
                .flatMap(index_partition)

            indexed_documents.persist(StorageLevel.MEMORY_AND_DISK)

            # This .count() call is what triggers the Spark pipeline so far
            print "Source %s indexed %s documents (acculumator=%s)" % (
                source_name, indexed_documents.count(), self.accumulator_indexed.value
            )

            all_indexed_documents = all_indexed_documents.union(indexed_documents)

        exec_hook(self.plugins, "spark_pipeline_collect", sc, sqlc, all_indexed_documents, indexer)

    def index_from_source(self, source, _indexer, **kwargs):
        """ Indexes all documents from a source """

        # plugins = load_plugins(plugin_list)

        for document in source.iter_documents():

            print "Indexing", document.source_url.url

            resp = {}

            exec_hook(self.plugins, "document_pre_index", document, resp)

            resp.update(_indexer.index_document(document, **kwargs))

            exec_hook(self.plugins, "document_post_index", document, resp)

            yield resp

    @ignore_exceptions_generator
    def index_documents(self, documentsource):
        """ Indexes documents from a source """

        setup_spark_worker()

        indexer.connect()

        print "Now working on %s" % documentsource

        for resp in self.index_from_source(documentsource, indexer):
            self.accumulator_indexed += 1
            yield resp

        indexer.flush()

        # When in tests, we want results to be available immediately!
        if config["ENV"] != "prod":
            indexer.refresh()

if __name__ == "__main__":
    job = IndexJob()
    job.run()
