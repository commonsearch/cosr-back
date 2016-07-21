import os
import sys

# Add the base cosr-back directory to the Python import path
# pylint: disable=wrong-import-position
if os.environ.get("COSR_PATH_BACK"):
    sys.path.insert(-1, os.environ.get("COSR_PATH_BACK"))
elif os.path.isdir("/cosr/back"):
    sys.path.insert(-1, "/cosr/back")

from pyspark.sql import types as SparkTypes
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
        document_schema_columns = [
            SparkTypes.StructField("id", SparkTypes.LongType(), nullable=False),
            SparkTypes.StructField("url", SparkTypes.StringType(), nullable=False)
        ]

        # Some plugins need to add new fields to the schema
        exec_hook(self.plugins, "document_schema", document_schema_columns)

        document_schema = SparkTypes.StructType(document_schema_columns)

        # Spark DataFrame containing everything we indexed

        all_documents = None  # sqlc.createDataFrame(sc.emptyRDD(), document_schema)

        executed_pipeline = False

        for source_spec in self.args.source:

            source_documents = None

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

            elif source_name == "parquet":

                # Read an intermediate dump of document metadata generated by
                # --plugin plugins.dump.DocumentMetadataParquet
                df = sqlc.read.parquet(source_args["path"])
                source_documents = df.rdd

            # Split indexing of each partition in Spark workers
            if source_documents is None:

                executed_pipeline = False
                source_documents = sc \
                    .parallelize(partitions, len(partitions)) \
                    .flatMap(index_partition)

            if source_args.get("persist") == "1":
                source_documents.persist(StorageLevel.MEMORY_AND_DISK)

            # The count() here will execute the pipeline so far to allow for sources to be done sequentially
            if source_args.get("block") == "1":
                executed_pipeline = True
                print "Source %s done, indexed %s documents (%s total so far)" % (
                    source_name, source_documents.count(), self.accumulator_indexed.value
                )

            if all_documents is None:
                all_documents = source_documents
            else:
                all_documents = all_documents.union(source_documents)

        actions = exec_hook(
            self.plugins, "spark_pipeline_action", sc, sqlc, all_documents, document_schema, indexer
        )

        # If no action was done, we need to do a count() to actually execute the spark pipeline
        # TODO: is there a better way to check that?
        if len(actions) > 0:
            executed_pipeline = True

        if not executed_pipeline:
            print "Total documents: %s" % all_documents.count()

    def index_from_source(self, source, _indexer, **kwargs):
        """ Indexes all documents from a source """

        # plugins = load_plugins(plugin_list)

        for document in source.iter_documents():

            print "Indexing", document.source_url.url

            metadata = {}

            exec_hook(self.plugins, "document_pre_index", document, metadata)

            metadata.update(_indexer.index_document(document, **kwargs))

            exec_hook(self.plugins, "document_post_index", document, metadata)

            yield metadata

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
