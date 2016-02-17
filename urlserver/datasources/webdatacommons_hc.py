from .alexa_top1m import DataSource as AlexaDataSource


class DataSource(AlexaDataSource):
    """ Return the Harmonic Centrality score from the WebDataCommons project.
    """

    db_path = "local-data/webdatacommons-hyperlinkgraph/harmonic-rocksdb"
