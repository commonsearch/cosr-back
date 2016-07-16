from . import BaseDataSource


class DataSource(BaseDataSource):
    """ Return the Harmonic Centrality score from the WebDataCommons project.
    """

    dump_testdata = "tests/testdata/webdatacommons-hyperlinkgraph-h.tsv"
    dump_url = "http://data.dws.informatik.uni-mannheim.de/hyperlinkgraph/2012-08/ranking/hostgraph-h.tsv.gz"
    dump_compression = "gz"
    dump_format = "tsv"
    dump_batch_size = 1000000
    dump_count_estimate = 101717775

    def import_row(self, i, row):
        """ Maps a raw data row into a list of (key, values) pairs """
        return [(row[0], {"webdatacommons_hc": float(row[1])})]
