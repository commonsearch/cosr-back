from . import BaseSignal


class Signal(BaseSignal):
    """ Ranking signal based on the URL presence in Wikidata as official website
    """

    def get_value(self, document, url_metadata):

        return (
            float(bool(url_metadata["url"].wikidata_title)) or
            float(bool(url_metadata["url_without_query"].wikidata_title))
        )
