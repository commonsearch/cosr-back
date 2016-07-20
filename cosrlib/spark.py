import os
import argparse
import time

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

from cosrlib.config import config


def setup_spark_worker(*a, **kw):
    """ Function used to execute some code only once on each worker. Is there a better way to do that? """

    if "_cosr_pyspark_setup_done" not in __builtins__:

        if os.getenv("COV_CORE_SOURCE"):
            from pytest_cov.embed import init
            cov = init()
            __builtins__["_cosr_pyspark_coverage"] = cov
            print "Coverage started for PID", os.getpid()

        __builtins__["_cosr_pyspark_setup_done"] = True


def teardown_spark_worker(*a, **kw):
    """ Used in tests, runs in each worker when the task is finished """
    if "_cosr_pyspark_coverage" in __builtins__:
        print "Coverage stop for PID", os.getpid()
        cov = __builtins__["_cosr_pyspark_coverage"]
        cov.stop()
        cov.save()
        del __builtins__["_cosr_pyspark_coverage"]


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

        conf = SparkConf().setAll((
            ("spark.python.profile", "true" if self.args.profile else "false"),
            ("spark.ui.enabled", "false" if config["ENV"] in ("ci", ) else "true"),
            ("spark.task.maxFailures", "20")
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

        sc = SparkContext(appName=self.name, conf=conf, environment=executor_environment)
        sqlc = SQLContext(sc)

        if config["ENV"] != "prod":
            sc.parallelize(range(4), 4).foreach(setup_spark_worker)

        return sc, sqlc

    def teardown_spark_context(self, sc, sqlc):

        if config["ENV"] != "prod":
            sc.parallelize(range(4), 4).foreach(teardown_spark_worker)

        if self.args.profile:
            sc.show_profiles()

        if self.args.stop_delay:
            time.sleep(self.args.stop_delay)

        sc.stop()

    def run_job(self, sc, sqlc):
        pass

    def run(self):
        """ Main Spark entry point """

        self.args = self.parse_arguments()

        sc, sqlc = self.setup_spark_context()

        self.run_job(sc, sqlc)

        self.teardown_spark_context(sc, sqlc)
