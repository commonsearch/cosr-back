import mmh3

from . import BaseDataSource


class DataSource(BaseDataSource):
    """ Return int64 IDs for URLs or domains """

    def domain_id(self, url):
        return mmh3.hash64(url.normalized_domain)[0]

    def url_id(self, url):
        return mmh3.hash64(url.normalized)[0]
