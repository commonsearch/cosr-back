import math

from . import BaseSignal


class Signal(BaseSignal):
    """ Ranking signal based on the list of the top 1 million domains from Alexa
    """

    def get_value(self, document, url_metadata):

        alexa_rank = url_metadata["alexa_top1m_rank"]
        if not alexa_rank:
            return None

        max_rank = 1000001

        # Attenuate differencies in the top100
        k1 = 2

        # http://www.wolframalpha.com/input/?i=1+-+(ln(x%2B1)%2Fln(1000000))%5E2+with+x+from+0+to+1000
        # http://www.wolframalpha.com/input/?i=1+-+(ln(x%2B1)%2Fln(1000000))%5E2+with+x+from+0+to+1000000

        return 1 - (math.log(float(alexa_rank), max_rank)) ** k1
