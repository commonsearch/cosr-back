import os
import sys
import argparse

#
# Main Spark job! Index Common Crawl files.
#


# Add the base cosr-back directory to the Python import path
# pylint: disable=wrong-import-position
if os.environ.get("COSR_PATH_BACK"):
    sys.path.insert(-1, os.environ.get("COSR_PATH_BACK"))
elif os.path.isdir("/cosr/back"):
    sys.path.insert(-1, "/cosr/back")
else:
    sys.path.insert(-1, os.path.normpath(os.path.join(__file__, "../../../")))

from pyspark import SparkContext, SparkConf  # pylint: disable=import-error

from cosrlib.webarchive import open_warc_file, iter_warc_records, list_commoncrawl_warc_filenames
from cosrlib.indexer import Indexer
from cosrlib.utils import ignore_exceptions, ignore_exceptions_generator
from cosrlib.config import config


def get_args():
    """ Returns the parsed arguments from the command line """

    parser = argparse.ArgumentParser(description='CommonSearch indexing from WARC files')

    parser.add_argument("--warc_limit", default=None, type=int, required=False,
                        help="Number of Common Crawl WARC files to index")

    parser.add_argument("--warc_skip", default=0, type=int,
                        help="Number of Common Crawl WARC files to skip before indexing")

    parser.add_argument("--warc_files", default=None, type=str,
                        help="Alternative list of WARC files or path to a list in .txt format")

    parser.add_argument("--only_homepages", action='store_true',
                        help="Only index homepages")

    parser.add_argument("--domain_whitelist", default=None, type=str,
                        help="Comma-separated list of domains to fully index")

    parser.add_argument("--save_linkgraph_domains", default=False, type=str,
                        help="Save a linkgraph domain file to this path")

    parser.add_argument("--profile", action='store_true',
                        help="Profile Python usage")

    return parser.parse_args()

# Shared variables while indexing
args = get_args()
indexer = Indexer()
urlclient = indexer.urlclient


def list_warc_filenames():
    """ Return a list of all indexable WARC files """

    if args.warc_files:
        if args.warc_files.endswith(".txt"):
            with open(args.warc_files, "rb") as f:
                warc_files = [x.strip() for x in f.readlines()]
        else:
            warc_files = [x.strip() for x in args.warc_files.split(",")]

    else:
        warc_files = list_commoncrawl_warc_filenames(limit=args.warc_limit, skip=args.warc_skip)

    return warc_files


@ignore_exceptions_generator
def iter_records(filename):
    """ Yields indexed records for this WARC file """

    # Must do that because we may be in a different process!
    # TODO: there might be a bug between spark and mprpc's Cython, this shouldn't be necessary.
    indexer.connect()

    print "Now working on %s" % filename

    warc_file = open_warc_file(filename, from_commoncrawl=(not args.warc_files))

    warc_filters = {
        "domain_whitelist": args.domain_whitelist,
        "only_homepages": args.only_homepages
    }

    for (url, headers, body) in iter_warc_records(warc_file, **warc_filters):

        print "Indexing", url.url

        resp = indexer.index_document(body, url=url, headers=headers, links=bool(args.save_linkgraph_domains))

        yield resp

    indexer.flush()

    # When in tests, we want results to be available immediately!
    if config["ENV"] != "prod":
        indexer.refresh()


@ignore_exceptions([])
def iter_links_domain(record):
    """ Returns all the parsed links in this record as (from_domain, to_domain) tuples  """

    record_domain = urlclient.get_domain_ids([record["url"]])[0]
    domains = list(set([link["href"].homepage for link in record["links"]]))
    link_ids = set(urlclient.get_domain_ids(domains))
    link_ids.discard(record_domain)

    return [(record_domain, d) for d in link_ids]


def print_rows(row):
    print "Indexed", row["url"].url


def spark_main():
    """ Main Spark entry point """

    conf = SparkConf().setAll((
        ("spark.python.profile", "true" if args.profile else "false"),
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

    sc = SparkContext(appName="Common Search Index", conf=conf, environment=executor_environment)

    # First, generate a list of all WARC files
    warc_filenames = list_warc_filenames()

    # Then split their indexing in Spark workers
    warc_records = sc.parallelize(warc_filenames, len(warc_filenames)).flatMap(iter_records)

    if args.save_linkgraph_domains:

        # Here we begin using the real power of Spark: get all unique (from, to) tuples
        # from all the links in all the pages
        warc_links = warc_records.flatMap(iter_links_domain).distinct().map(
            lambda row: "%s %s" % row
        ).coalesce(1)

        # warc_links.foreach(print_rows)

        warc_links.saveAsTextFile(args.save_linkgraph_domains)

    else:

        # This .count() call is what triggers the whole Spark pipeline
        print "Indexed %s WARC records" % warc_records.count()

    if args.profile:
        sc.show_profiles()

    sc.stop()


if __name__ == "__main__":
    spark_main()
