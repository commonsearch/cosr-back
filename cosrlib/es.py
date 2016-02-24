import time
import logging

from elasticsearch import Elasticsearch, helpers
from elasticsearch.client import IndicesClient
from elasticsearch.exceptions import ConnectionTimeout

from .config import config
from .es_mappings import ES_MAPPINGS, ES_SIMILARITIES


class ElasticsearchBulkIndexer(object):
    """ Bulk indexer for Elasticsearch """

    servers = {
        "docs": [config["ELASTICSEARCHDOCS"]],
        "text": [config["ELASTICSEARCHTEXT"]]
    }

    def __init__(self, index_name, batch_size=500):
        self.index_name = index_name
        self.buffer = []
        self.batch_size = batch_size
        self.total_size = 0
        self.connected = False
        self.client = None

    def connect(self):
        """ Establish the ES connection if not already done """
        if self.connected:
            return
        self.connected = True
        self.client = Elasticsearch(self.servers[self.index_name])

    def index(self, _id, hit):
        """ Queue one document for indexing. """

        if not self.connected:
            self.connect()

        self.buffer.append({
            "_index": self.index_name,
            "_type": "page",
            "_id": _id,
            "_source": hit
        })

        if len(self.buffer) >= self.batch_size:
            self.flush()

    def empty(self):
        """ Empty the ES index. Dangerous operation! """

        if config["ENV"] not in ("local", "ci"):
            raise Exception("Empty() not allowed in env %s" % config["ENV"])

        if self.indices().exists(index=self.index_name):
            self.indices().delete(index=self.index_name)

    def refresh(self):
        """ Sends a "refresh" to the ES index, forcing the actual indexing of what was sent up until now """

        if config["ENV"] not in ("local", "ci"):
            raise Exception("Refresh() not allowed in env %s" % config["ENV"])

        self.indices().refresh(index=self.index_name)

    def flush(self, retries=10):
        """ Sends the current indexing batch to ES """

        if not self.connected:
            self.connect()

        self.total_size += len(self.buffer)

        logging.debug(
            "ES: Flushing %s docs to index=%s (total: %s)",
            len(self.buffer), self.index_name, self.total_size
        )

        try:
            helpers.bulk(self.client, self.buffer, chunk_size=len(self.buffer))
        except ConnectionTimeout, e:
            if retries == 0:
                raise e
            time.sleep(60)
            return self.flush(retries=retries - 1)

        self.buffer = []

    def indices(self):
        """ Returns an elasticsearch.client.IndicesClient instance """

        if not self.connected:
            self.connect()

        return IndicesClient(self.client)

    def create(self, empty=False):
        """ Creates the ES index """

        if empty:
            self.empty()

        mappings = ES_MAPPINGS[self.index_name]

        self.indices().create(index=self.index_name, body={
            "settings": {

                # TODO: this configuration should be set somewhere else! (cosr-ops?)
                "number_of_shards": 5,
                "number_of_replicas": 0,

                # If 1: The index will need to be refreshed manually,
                # but this optimizes bulk insertions
                "refresh_interval": "-1",

                "similarity": ES_SIMILARITIES
            },
            "mappings": mappings
        })
