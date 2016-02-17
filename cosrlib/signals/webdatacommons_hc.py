from . import BaseSignal


class Signal(BaseSignal):
    """ Ranking signal based on the Harmonic Centrality domain ranking
        from the WebDataCommons project.
    """

    def get_value(self, document, url_metadata):

        rank = url_metadata["webdatacommons_hc_rank"]
        if not rank:
            return None

        return max(0, min(1, float(rank) / 26214400.0))
