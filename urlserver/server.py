import os
import sys
import importlib

if __name__ == "__main__":
    from gevent.server import StreamServer

from mprpc import RPCServer

sys.path.insert(-1, os.path.normpath(os.path.join(__file__, "../../")))
from cosrlib.url import URL

datasources = {}

datasource_dir = os.path.join(os.path.dirname(__file__), "datasources/")
for datasource in os.listdir(datasource_dir):
    if not datasource.startswith("_") and datasource.endswith(".py"):
        ds_name = datasource.replace(".py", "")
        ds = importlib.import_module(".datasources.%s" % ds_name, package="urlserver").DataSource()
        datasources[ds_name] = ds


class URLServer(RPCServer):
    """ RPC server for getting static metadata about URLs """

    def get_ids(self, urls):
        """ Return a list of IDs for these URLs """
        ret = []
        for u in urls:
            url = URL(u)
            ret.append(datasources["ids"].url_id(url))
        return ret

    def get_domain_ids(self, urls):
        """ Return a list of domain IDs for these URLs """
        ret = []
        for u in urls:
            url = URL(u)
            ret.append(datasources["ids"].domain_id(url))
        return ret

    def get_metadata(self, urls):
        """ Return a list of tuples of metadata for these URLs """

        ret = []
        for u in urls:

            url = URL(u)
            ret.append((
                datasources["ids"].url_id(url),
                datasources["ids"].domain_id(url),
                datasources["alexa_top1m"].rank(url),
                datasources["dmoz_url"].exists(url),
                datasources["dmoz_domain"].exists(url),
                datasources["ut1_blacklist"].classes(url),
                datasources["webdatacommons_hc"].rank(url)
            ))

        return ret


if __name__ == "__main__":

    server = StreamServer(('0.0.0.0', 9702), URLServer())
    server.serve_forever()
