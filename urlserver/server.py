import os
import sys

if __name__ == "__main__":
    from gevent.server import StreamServer

from mprpc import RPCServer

sys.path.insert(-1, os.path.normpath(os.path.join(__file__, "../../")))
from cosrlib.url import URL
from urlserver.id_generator import make_url_id, make_domain_id
from urlserver.storage import Storage
from urlserver.protos import urlserver_pb2

db = Storage(read_only=True)


class URLServer(RPCServer):
    """ RPC server for getting static metadata about URLs.

        For simplicity we currently use mprpc, but we should migrate to gRPC or similar to
        be able to send protobufs directly (and avoid re-encoding them as MessagePacks!)

    """

    def get_ids(self, urls):
        """ Return a list of IDs for these URLs """
        ret = []
        for u in urls:
            url = URL(u)
            ret.append(make_url_id(url))
        return ret

    def get_domain_ids(self, urls):
        """ Return a list of domain IDs for these URLs """
        ret = []
        for u in urls:
            url = URL(u)
            ret.append(make_domain_id(url))
        return ret

    def get_metadata(self, urls):
        """ Return a list of tuples of metadata for these *normalized* URLs """

        ret = []
        for url in urls:

            data = db.get(url)

            # If the URL has been in none of our static databases, we still want to return an ID
            if data is None:
                obj = urlserver_pb2.UrlMetadata()
                obj.id = make_url_id(URL(url))
                data = obj.SerializeToString()

            ret.append(data)

        return ret


if __name__ == "__main__":

    server = StreamServer(('0.0.0.0', 9702), URLServer())
    server.serve_forever()
