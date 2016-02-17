from mprpc import RPCClient

from .config import config
from .url import URL


URLSERVER_FIELDNAMES = {
    "get_metadata": (
        "url_id",
        "domain_id",
        "alexa_top1m_rank",
        "dmoz_url_exists",
        "dmoz_domain_exists",
        "ut1_blacklist_classes",
        "webdatacommons_hc_rank"
    )
}


class URLClientRemote(object):
    """ The URLClient takes URLs and sends them to the URLServer to get their IDs or fields

        We should migrate to something more efficient (but more complex!) like gRPC in the future.

    """

    urlserver = None

    def connect(self):
        self.urlserver = RPCClient(
            config["URLSERVER"].split(":")[0],
            int(config["URLSERVER"].split(":")[1])
        )

    def empty(self):
        pass

    def get_id(self, url):
        """ Simple API to get the ID of an URL """
        return self.get_ids([url])[0]

    def get_domain_id(self, url):
        """ Simple API to get the ID of a domain """
        return self.get_domain_ids([url])[0]

    def _rpc(self, method, *args):
        return self.urlserver.call(method, *args)

    def get_ids(self, urls):
        """ Returns an array of 64-bit integers for each URL """

        if len(urls) == 0:
            return []

        if isinstance(urls[0], URL):
            urls = [u.url for u in urls]

        return self._rpc("get_ids", urls)

    def get_domain_ids(self, urls):
        """ Returns an array of 64-bit integers for each domain """

        if len(urls) == 0:
            return []

        if isinstance(urls[0], URL):
            urls = [u.url for u in urls]

        return self._rpc("get_domain_ids", urls)

    def get_metadata(self, urls):
        """ Returns an array of complete fields for each URL """

        if len(urls) == 0:
            return []

        if isinstance(urls[0], URL):
            urls = [u.url for u in urls]

        res = self._rpc("get_metadata", urls)
        ret = []
        for row in res:
            d = {}
            for i, f in enumerate(URLSERVER_FIELDNAMES["get_metadata"]):
                d[f] = row[i]
            ret.append(d)

        return ret


class URLClientLocal(URLClientRemote):
    """ Local version that directly imports from the urlserver/ directory, in the same process """

    def connect(self):
        from urlserver.server import URLServer
        self.urlserver = URLServer()

    def _rpc(self, method, *args):
        return getattr(self.urlserver, method)(*args)


if config["URLSERVER"] == "local":
    URLClient = URLClientLocal
else:
    URLClient = URLClientRemote
