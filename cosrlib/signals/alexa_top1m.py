import math

from . import BaseSignal


class Signal(BaseSignal):
    """ Ranking signal based on the list of the top 1 million domains from Alexa
    """

    def get_value(self, document, url_metadata):

        factor = 1.0
        alexa_rank = url_metadata["domain"].alexa_top1m

        # Sub-domain matches have a slight penalty
        # This works well for en.wikipedia.org but not for xxxx.tumblr.com
        # Ideally we should divide it by the number of known subdomains...
        if not alexa_rank:
            factor = 0.7
            alexa_rank = url_metadata["pld"].alexa_top1m

        if not alexa_rank:
            return None

        max_rank = 1000001

        # Attenuate differencies in the top100
        k1 = 2

        # http://www.wolframalpha.com/input/?i=1+-+(ln(x%2B1)%2Fln(1000000))%5E2+with+x+from+0+to+1000
        # http://www.wolframalpha.com/input/?i=1+-+(ln(x%2B1)%2Fln(1000000))%5E2+with+x+from+0+to+1000000

        return (1 - (math.log(float(alexa_rank), max_rank)) ** k1) * factor
