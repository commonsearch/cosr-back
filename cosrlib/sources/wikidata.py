from cosrlib.sources import Source
from cosrlib.document import Document
from cosrlib.dataproviders import load_dataprovider


class WikidataSource(Source):
    """ Source that reads 'fake' documents from the WikiData dump """

    def get_partitions(self):

        return ["__wikidata_single_dump__"]

    def iter_documents(self, partition):

        dataprovider = load_dataprovider("wikidata")

        i = 0
        maxdocs = int(self.args.get("maxdocs") or 0)

        for key, _ in dataprovider.iter_rows():

            doc = Document(None, url="http://%s" % key)  # TODO get the original URL instead?

            # Summary & title will be inferred from the Wikidata *dataprovider* via url_metadata
            # doc._title = values["wikidata_title"]

            yield doc

            i += 1
            if i >= maxdocs > 0:
                return
