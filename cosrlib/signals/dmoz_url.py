from . import BaseSignal


class Signal(BaseSignal):
    """ Ranking signal based on the URL presence in DMOZ
    """

    def get_value(self, document, url_metadata):

        return float(url_metadata["dmoz_url_exists"])
