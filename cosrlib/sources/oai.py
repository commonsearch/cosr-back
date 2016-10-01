from __future__ import absolute_import, division, print_function, unicode_literals

import xml.etree.ElementTree as ET

from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry, oai_dc_reader
import requests

from cosrlib.sources import Source


class OaiSource(Source):
    """ Source that fetches metadata from OAI-PMH endpoints. """

    def get_partitions(self):

        if self.args.get("list"):
            if self.args["list"] == "openarchives":
                list_url = "http://www.openarchives.org/pmh/registry/ListFriends"
            else:
                list_url = self.args["list"]

            req = requests.get(list_url)
            root = ET.fromstring(req.content)

            if root.tag == "BaseURLs":
                return [elt.text for elt in root if elt.tag == "baseURL"]

            return []

        elif self.args.get("urls"):
            return self.args["urls"]
        else:
            return [self.args["url"]]

    def iter_items(self, partition):
        """ Partition is an OAI-PMH endpoint """

        # source = "oai:%s" % partition

        registry = MetadataRegistry()
        registry.registerReader('oai_dc', oai_dc_reader)
        client = Client(partition, registry)

        for record in client.listRecords(metadataPrefix='oai_dc'):
            header, metadata, _ = record

            if header.isDeleted():
                continue

            # _id = header.identifier()
            # date = header.datestamp()

            meta = metadata.getMap()

            # TODO: there are much validation and heuristics to be done here!

            # format0 = (meta.get("format") or [None])[0]
            # if not format0:
            #     continue

            # if format0 not in ("application/pdf", ):
            #     continue

            url0 = (meta.get("identifier") or [None])[0]

            if not url0:
                continue

            title0 = (meta.get("title") or [""])[0].encode("utf-8")
            desc0 = (meta.get("description") or [""])[0].encode("utf-8")

            # TODO: validate that the url0 is not on another domain?!
            yield url0, {}, "html", 2, """
                <html><head><title>%s</title></head><body>%s</body></html>
            """ % (title0, desc0)
