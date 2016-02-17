from ._rocksdb import RocksdbDataSource
from . import BaseDataSource


class DataSource(BaseDataSource, RocksdbDataSource):
    """ Return the UT1 categories in which the URL belongs.

        https://dsi.ut-capitole.fr/blacklists/index_en.php
    """

    db_path = "local-data/ut1-blacklist/blacklist-rocksdb"

    def classes(self, url):
        ret = set()

        # We indexed both the domains and the URLs
        # TODO should all substrings be considered? In which case RocksDB can't be used
        # and we should build some kind of matching tree instead.
        keys = list(set([
            url.domain,
            url.pld,
            url.normalized,
            url.normalized_without_query,
            url.url
        ]))

        found = self.db.multi_get(keys)

        for categ in found.itervalues():
            if categ:
                for c in categ.split(" "):
                    ret.add(c)

        return list(ret)
