from cosrlib.sources import Source
from cosrlib.document import Document
from urlserver.datasources import load_datasource


class WikidataSource(Source):
    """ Source that reads 'fake' documents from the WikiData dump """

    def iter_documents(self):

        datasource = load_datasource("wikidata")

        i = 0
        maxdocs = int(self.args.get("maxdocs") or 9999999999)

        for key, _ in datasource.iter_rows():

            doc = Document(None, url="http://%s" % key)  # TODO get the original URL instead?

            # Summary & title will be inferred from the Wikidata *datasource* via url_metadata
            # doc._title = values["wikidata_title"]

            yield doc

            i += 1
            if i > maxdocs:
                return
