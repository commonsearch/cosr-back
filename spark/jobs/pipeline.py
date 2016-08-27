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
from cosrlib.spark import SparkJob, setup_spark_worker, createDataFrame


indexer = Indexer()
urlclient = indexer.urlclient


class IndexJob(SparkJob):
    """ Run a data pipeline on documents from diverse sources, through plugins """

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

        # What fields will be sent to Spark
        document_schema_columns = [
            SparkTypes.StructField("id", SparkTypes.LongType(), nullable=False),
            SparkTypes.StructField("url", SparkTypes.StringType(), nullable=False),
            SparkTypes.StructField("rank", SparkTypes.FloatType(), nullable=False)
        ]

        # Some plugins need to add new fields to the schema
        exec_hook(
            self.plugins, "spark_pipeline_init", sc, sqlc, document_schema_columns, indexer
        )

        exec_hook(self.plugins, "document_schema", document_schema_columns)

        document_schema = SparkTypes.StructType(document_schema_columns)

        # Spark DataFrame containing everything we indexed
        all_documents = None

        executed_pipeline = False

        for source_spec in self.args.source:

            source_name, source_args = parse_plugin_cli_args(source_spec)

            ds = load_source(source_name, source_args, plugins=self.plugins)

            if ds.already_parsed:

                # Some sources return already parsed documents
                source_documents = ds.get_documents(sqlc)

            else:

                partitions = ds.get_partitions()

                executed_pipeline = False

                rdd = sc \
                    .parallelize(partitions, len(partitions)) \
                    .flatMap(lambda partition: self.index_documents(ds, partition))

                source_documents = createDataFrame(sqlc, rdd, document_schema)

            #
            # At this point, we have a DataFrame with every document from this source.
            #

            if source_args.get("persist") == "1":
                source_documents.persist(StorageLevel.MEMORY_AND_DISK)

            # The count() here will execute the pipeline so far to allow for sources to be done sequentially
            if source_args.get("block") == "1":
                executed_pipeline = True
                print "Source %s done, indexed %s documents (%s total so far)" % (
                    source_name, source_documents.rdd.count(), self.accumulator_indexed.value
                )

            if all_documents is None:
                all_documents = source_documents
            else:
                all_documents = all_documents.unionAll(source_documents)

        #
        # At this point, we have a DataFrame with all documents from all sources.
        #

        done_actions = exec_hook(
            self.plugins, "spark_pipeline_action", sc, sqlc, all_documents, indexer
        )

        # If no action was done, we need to do a count() to actually
        # execute ("materialize") the spark pipeline
        if any(done_actions):
            executed_pipeline = True

        if not executed_pipeline:
            print "Total documents: %s" % all_documents.rdd.count()

    def index_from_source(self, source, partition, _indexer, **kwargs):
        """ Indexes all documents from a source """

        for document in source.iter_documents(partition):

            print "Indexing", document.source_url.url

            metadata = {}

            exec_hook(self.plugins, "document_pre_index", document, metadata)

            metadata.update(_indexer.index_document(document, **kwargs))

            exec_hook(self.plugins, "document_post_index", document, metadata)

            yield metadata

    @ignore_exceptions_generator
    def index_documents(self, documentsource, partition):
        """ Indexes documents from a source """

        setup_spark_worker()

        indexer.connect()

        print "Now working on %s" % documentsource

        for resp in self.index_from_source(documentsource, partition, indexer):
            self.accumulator_indexed += 1
            yield resp

        indexer.flush()

        # When in tests, we want results to be available immediately!
        if config["ENV"] != "prod":
            indexer.refresh()

if __name__ == "__main__":
    job = IndexJob()
    job.run()
