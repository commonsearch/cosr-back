from ._rocksdb import RocksdbDataSource
from . import BaseDataSource


class DataSource(BaseDataSource, RocksdbDataSource):
    """ Return the domain presence in DMOZ
    """

    db_path = "local-data/dmoz/domains-rocksdb"

    def exists(self, url):
        if not self.db:
            return None

        # TODO return a float rank depending on the number of occurences in DMOZ?
        rank = self.db.get(url.normalized_domain)
        if rank is not None:
            return True

        return False
