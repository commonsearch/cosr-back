from __future__ import absolute_import, division, print_function, unicode_literals

from mprpc import RPCClient

from urlserver.protos import urlserver_pb2
from .config import config
from .url import URL
from . import py2_xrange


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
        """ Returns an array of complete fields for each URL, for each main variation of those URLs """

        if len(urls) == 0:
            return []

        if not isinstance(urls[0], URL):
            urls = [URL(u) for u in urls]

        # Multiple variations of the URLs to be tested. It is slightly inefficient, but we
        # send the 4 variations in the same request and split them just below.
        urls_variations = []
        for url in urls:
            urls_variations.extend([
                url.normalized,
                url.normalized_without_query,
                url.normalized_domain,
                url.pld
            ])

        res = self._rpc("get_metadata", urls_variations)
        ret = []

        for url_i in py2_xrange(len(res) // 4):

            url_metadata = {}
            for i, key in ((0, "url"), (1, "url_without_query"), (2, "domain"), (3, "pld")):
                url_metadata[key] = urlserver_pb2.UrlMetadata()
                url_metadata[key].ParseFromString(res[(url_i * 4) + i])
            ret.append(url_metadata)

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
