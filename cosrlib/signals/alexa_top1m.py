from . import BaseSignal


class Signal(BaseSignal):
    """ Ranking signal based on the list of the top 1 million domains from Alexa
    """

    def get_value(self, document, url_metadata):

        alexa_rank = url_metadata["alexa_top1m_rank"]
        if not alexa_rank:
            return None

        return (1000001.0 - int(alexa_rank)) / 1000000
