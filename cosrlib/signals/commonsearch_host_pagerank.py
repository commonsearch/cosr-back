from __future__ import absolute_import, division, print_function, unicode_literals

from . import BaseSignal


class Signal(BaseSignal):
    """ Ranking signal based on the Harmonic Centrality domain ranking
        from the WebDataCommons project.
    """

    def get_value(self, document, url_metadata):

        rank = url_metadata["domain"].commonsearch_host_pagerank
        if not rank:
            return None

        return float(rank)
