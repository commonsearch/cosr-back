import math

from . import BaseSignal


class Signal(BaseSignal):
    """ Ranking signal based on the URL presence in Wikidata as official website
    """

    def get_value(self, document, url_metadata):

        # Number of pages on Wikimedia projects is a rough approximation of the importance of the entity
        number_of_sitelinks = max(
            url_metadata["url"].wikidata_sitelinks,
            url_metadata["url_without_query"].wikidata_sitelinks
        )

        max_sitelinks = 200

        # http://www.wolframalpha.com/input/?i=sqrt(x%2F200)+from+0+to+200
        score = min(1., math.sqrt(float(number_of_sitelinks) / max_sitelinks))

        return score
