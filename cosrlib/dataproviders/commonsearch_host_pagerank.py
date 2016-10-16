from __future__ import absolute_import, division, print_function, unicode_literals

from . import BaseDataProvider


class DataProvider(BaseDataProvider):
    """ Return the PageRank score from CommonSearch.
    """

    dump_testdata = "tests/testdata/commonsearch_host_pagerank.txt"
    dump_url = "https://dumps.commonsearch.org/webgraph/201606/host-level/pagerank/pagerank.txt.gz"
    dump_compression = "gz"
    dump_format = "txt"
    dump_batch_size = 1000000
    dump_count_estimate = 101717775

    def import_row(self, i, row):
        """ Maps a raw data row into a list of (key, values) pairs """
        return [(row[0], {"commonsearch_host_pagerank": float(row[1])})]
