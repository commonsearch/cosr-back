from __future__ import absolute_import, division, print_function, unicode_literals

import time
import logging
import ujson as json

from elasticsearch import Elasticsearch
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
        self.client = Elasticsearch(self.servers[self.index_name], timeout=60)

    def index(self, _id, hit):
        """ Queue one document for indexing. """

        if not self.connected:
            self.connect()

        # https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
        self.buffer.append('{"index":{"_id":"%s"}}\n%s\n' % (
            _id,
            json.dumps(hit)  # pylint: disable=no-member
        ))

        if len(self.buffer) >= self.batch_size:
            self.flush()

    def empty(self):
        """ Empty the ES index. Dangerous operation! """

        if config["ENV"] not in ("local", "ci"):
            raise Exception("empty() not allowed in env %s" % config["ENV"])

        if self.indices().exists(index=self.index_name):
            self.indices().delete(index=self.index_name)

    def refresh(self):
        """ Sends a "refresh" to the ES index, forcing the actual indexing of what was sent up until now """

        if not self.connected:
            return

        if config["ENV"] not in ("local", "ci"):
            raise Exception("refresh() not allowed in env %s" % config["ENV"])

        self.indices().refresh(index=self.index_name)

    def flush(self, retries=10):
        """ Sends the current indexing batch to ES """

        if len(self.buffer) == 0:
            return

        if not self.connected:
            self.connect()

        self.total_size += len(self.buffer)

        logging.debug(
            "ES: Flushing %s docs to index=%s (total: %s)",
            len(self.buffer), self.index_name, self.total_size
        )

        try:
            self.bulk_index()
        except ConnectionTimeout, e:
            if retries == 0:
                raise e
            time.sleep(60)
            return self.flush(retries=retries - 1)

        self.buffer = []

    def bulk_index(self):
        """ Indexes the current buffer to Elasticsearch, bypassing the bulk() helper for performance """

        connection = self.client.transport.get_connection()

        bulk_url = "/%s/page/_bulk" % self.index_name

        body = "".join(self.buffer)

        # TODO retries
        # status, headers, data
        status, _, _ = connection.perform_request("POST", bulk_url, body=body)

        if status != 200:
            raise Exception("Elasticsearch returned status=%s" % status)

        # TODO: look for errors there?
        # parsed = json.loads(data)

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

                # In prod we don't refresh manually so this is the only setting
                # that will make ES periodically refresh to avoid storing only in temporary files
                # as we index
                "refresh_interval": "60s",

                "similarity": ES_SIMILARITIES
            },
            "mappings": mappings
        })
