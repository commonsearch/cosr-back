from ._rocksdb import RocksdbDataSource
from . import BaseDataSource
from cosrlib import re


class DataSource(BaseDataSource, RocksdbDataSource):
    """ Return the rank in the top 1 million domains from Alexa
    """

    db_path = "local-data/alexa/top-1m-rocksdb"

    def rank(self, url):
        if not self.db:
            return None

        rank = self.db.get(url.domain)
        if rank is not None:
            return rank

        rank = self.db.get(url.normalized_domain)
        if rank is not None:
            return rank

        # Dirty hack to fix wikipedia
        # Domains like images.google.com are currently None :/
        # TODO: detect domains like tumblr/blogpost that generate lots of unrelated subdomains?
        domain = re.sub(r"^[a-z]{2}\.", "", url.normalized_domain)

        rank = self.db.get(domain)
        if rank is not None:
            return rank

        return None
