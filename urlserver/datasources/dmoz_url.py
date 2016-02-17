from ._rocksdb import RocksdbDataSource
from . import BaseDataSource


class DataSource(BaseDataSource, RocksdbDataSource):
    """ Return the URL presence in DMOZ
    """

    db_path = "local-data/dmoz/urls-rocksdb"

    def exists(self, url):
        if not self.db:
            return None

        # TODO return a float rank depending on longevity in DMOZ by importing several dumps
        rank = self.db.get(url.normalized)
        if rank is not None:
            return True

        return False
