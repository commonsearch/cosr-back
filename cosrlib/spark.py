from __future__ import absolute_import, division, print_function, unicode_literals

import os
import argparse
import time

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.dataframe import DataFrame
from pyspark.rdd import RDD

from cosrlib.config import config
from cosrlib.plugins import Plugin


def setup_spark_worker(*a, **kw):
    """ Function used to execute some code only once on each worker. Is there a better way to do that? """

    if "_cosr_pyspark_setup_done" not in __builtins__:

        if os.getenv("COV_CORE_SOURCE"):
            from pytest_cov.embed import init
            cov = init()
            __builtins__["_cosr_pyspark_coverage"] = cov
            print("Coverage started for PID", os.getpid())

        __builtins__["_cosr_pyspark_setup_done"] = True


def teardown_spark_worker(*a, **kw):
    """ Used in tests, runs in each worker when the task is finished """
    if "_cosr_pyspark_coverage" in __builtins__:
        print("Coverage stop for PID", os.getpid())
        cov = __builtins__["_cosr_pyspark_coverage"]
        cov.stop()
        cov.save()
        del __builtins__["_cosr_pyspark_coverage"]


def createDataFrame(sqlc, data, schema, samplingRatio=None):
    """ Our own version of spark.sql.session.createDataFrame which doesn't validate the schema.
        See https://issues.apache.org/jira/browse/SPARK-16700
    """
    # pylint: disable=protected-access

    self = sqlc.sparkSession

    if isinstance(data, RDD):
        rdd, schema = self._createFromRDD(data, schema, samplingRatio)
    else:
        rdd, schema = self._createFromLocal(data, schema)

    jrdd = self._jvm.SerDeUtil.toJavaArray(rdd._to_java_object_rdd())
    jdf = self._jsparkSession.applySchemaToPythonRDD(jrdd.rdd(), schema.json())
    df = DataFrame(jdf, self._wrapped)
    df._schema = schema
    return df


def sql(sqlc, query, tables=None):
    """ Helper that runs a Spark SQL query with a list of temporary tables """

    for key, df in (tables or {}).items():
        sqlc.registerDataFrameAsTable(df, key)

    ret = sqlc.sql(query)

    return ret


class SparkJob(object):

    name = "Common Search generic job"
    args = None

    def __init__(self):
        pass

    def parse_arguments(self):
        """ Returns the parsed arguments from the command line """

        arg_parser = argparse.ArgumentParser(description=self.name)

        arg_parser.add_argument("--profile", action='store_true',
                                help="Profile Python usage")

        arg_parser.add_argument("--stop_delay", action='store', type=int, default=0,
                                help="Seconds to wait before stopping the spark context")

        self.add_arguments(arg_parser)

        args = arg_parser.parse_args()

        self.validate_arguments(args)

        return args

    def add_arguments(self, parser):
        pass

    def validate_arguments(self, args):
        return True

    def setup_spark_context(self):

        # http://spark.apache.org/docs/latest/configuration.html
        conf = SparkConf().setAll((
            ("spark.python.profile", "true" if self.args.profile else "false"),

            # Protect against memory leaks (which we seem to have at the moment)
            ("spark.python.worker.reuse", "true" if config["ENV"] in ("ci", ) else "false"),

            ("spark.ui.enabled", "false" if config["ENV"] in ("ci", ) else "true"),

            ("spark.task.maxFailures", "10"),
            ("spark.locality.wait", "10s"),
            ("spark.locality.wait.node", "10s"),
            ("spark.locality.wait.process", "10s"),
            ("spark.locality.wait.rack", "10s"),

            ("spark.sql.warehouse.dir", "/tmp/spark-warehouse"),

            # http://deploymentzone.com/2015/12/20/s3a-on-spark-on-aws-ec2/
            # https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html
            ("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"),
            ("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "")),
            ("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "")),
            ("spark.hadoop.fs.s3a.buffer.dir", "/tmp"),
            ("spark.hadoop.fs.s3a.connection.maximum", "100"),
            ("spark.hadoop.fs.s3a.endpoint", "s3-external-1.amazonaws.com"),  # us-east-1
            # ("spark.hadoop.fs.s3a.fast.upload", "true"),  # Buffer directly from memory to S3

            ("spark.sql.parquet.mergeSchema", "false"),
            ("spark.sql.parquet.cacheMetadata", "true"),
            ("spark.sql.parquet.compression.codec", "gzip"),  # snappy, lzo
            ("spark.hadoop.parquet.enable.summary-metadata", "false"),
            ("spark.hadoop.parquet.metadata.read.parallelism", "100"),
            ("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2"),
            ("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "true"),

            ("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),

            ("spark.speculation", "false"),

            # ("spark.sql.parquet.enableVectorizedReader", "false")

            # TODO https://groups.google.com/forum/#!topic/spark-users/YnAlw7dVdQA ?
            # set("spark.akka.frameSize", "128")
        ))

        executor_environment = {
            "IS_SPARK_EXECUTOR": "1"
        }
        if config["ENV"] == "prod":
            executor_environment.update({
                "PYTHONPATH": "/cosr/back",
                "PYSPARK_PYTHON": "/cosr/back/venv/bin/python",
                "LD_LIBRARY_PATH": "/usr/local/lib"
            })

        from pyspark.serializers import MarshalSerializer

        sc = SparkContext(
            appName=self.name,
            conf=conf,
            environment=executor_environment,
            serializer=MarshalSerializer()
        )

        sqlc = SQLContext(sc)

        if config["ENV"] != "prod":
            sc.parallelize(list(range(4)), 4).foreach(setup_spark_worker)

        return sc, sqlc

    def teardown_spark_context(self, sc, sqlc):

        if config["ENV"] != "prod":
            sc.parallelize(list(range(4)), 4).foreach(teardown_spark_worker)

        if self.args.profile:
            sc.show_profiles()

        if self.args.stop_delay:
            try:
                print()
                print("Spark job finished! You can still browse the UI " + \
                      "for %s seconds, or do Ctrl-C to exit." % self.args.stop_delay)
                print()
                time.sleep(self.args.stop_delay)
            except KeyboardInterrupt:
                pass

        sc.stop()

    def run_job(self, sc, sqlc):
        pass

    def run(self):
        """ Main Spark entry point """

        self.args = self.parse_arguments()

        sc, sqlc = self.setup_spark_context()

        self.run_job(sc, sqlc)

        self.teardown_spark_context(sc, sqlc)


class SparkPlugin(Plugin):

    def save_dataframe(self, df, fileformat):
        """ Saves a dataframe with common options """

        coalesce = int(self.args.get("coalesce", 1) or 0)
        if coalesce > 0:
            df = df.coalesce(coalesce)

        if fileformat == "text":
            df.write.text(
                self.args["path"],
                compression="gzip" if self.args.get("gzip") else "none"
            )

        elif fileformat == "json":
            df.write.json(
                self.args["path"],
                compression="gzip" if self.args.get("gzip") else "none"
            )

        elif fileformat == "parquet":
            df.write.parquet(self.args["path"])

        else:
            raise Exception("Unknown format %s" % fileformat)
