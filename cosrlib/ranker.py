from __future__ import absolute_import, division, print_function, unicode_literals

from .signals import load_signal
from .document import Document


class Ranker(object):
    """ Class that gives a static rank (popularity measure) to every URL """

    def __init__(self, urlclient):
        self.urlclient = urlclient

    def connect(self):
        pass

    def empty(self):
        pass

    def get_global_document_rank(self, document, url_metadata):
        """ Gets a merged document rank from all available signals

            This is a float between 0.0 (unknown) and 1.0 (most popular page on the web)
        """

        signal_weights = {

            # TODO: should this be a part of the same rank? Or should we split popularity & url simplicity?
            "url_total_length": 0.01,
            "url_path_length": 0.01,
            "url_subdomain": 0.1,

            "alexa_top1m": 5,

            "wikidata_url": 3,

            "dmoz_domain": 1,
            "dmoz_url": 1,

            "webdatacommons_hc": 1,

            "commonsearch_host_pagerank": 1
        }

        sum_ranks = 0.
        sum_weights = 0.

        signals = {}

        for signal_name, weight in signal_weights.items():

            signals[signal_name] = {
                "value": load_signal(signal_name).get_value(document, url_metadata),
                "weight": weight
            }

            # print signal_name, signals[signal_name]

            sum_weights += weight
            sum_ranks += weight * (signals[signal_name]["value"] or 0)

        global_rank = sum_ranks / sum_weights

        # Presence in some blacklists impacts the rank
        global_rank *= self._get_blacklist_weight(document, url_metadata)

        return global_rank, signals

    def _get_blacklist_weight(self, document, url_metadata):  # pylint: disable=no-self-use
        """ Return the weight due to the presence in blacklists. 1.0 if none """

        # This should (and will) be debated openly
        blacklist_weights = {
            "adult": 0.01,  # TODO: flag instead to make a safesearch setting, not a blacklist
            "agressif": 0.001,
            "dangerous_material": 0.001,
            "phishing": 0.001,
            "malware": 0.001,
            "ddos": 0.001
        }

        ut1_classes = load_signal("ut1_blacklist").get_value(document, url_metadata)

        for c in ut1_classes:
            if c in blacklist_weights:
                return blacklist_weights[c]

        return 1.

    def get_global_url_rank(self, url):
        """ Gets the same document rank from URL only. Used in tests """

        url_metadata = self.urlclient.get_metadata([url])[0]
        return self.get_global_document_rank(Document(None, url=url), url_metadata)

    def get_signal_value(self, signal_name, document):
        """ Gets one signal value from a document. Used in tests """

        sig = load_signal(signal_name)
        url_metadata = None
        if sig.uses_url_metadata:
            url_metadata = self.urlclient.get_metadata([document.get_url()])[0]
        return sig.get_value(document, url_metadata)

    def get_signal_value_from_url(self, signal_name, url):
        """ Gets one signal value from URL only. Used in tests """

        sig = load_signal(signal_name)
        url_metadata = None
        if sig.uses_url_metadata:
            url_metadata = self.urlclient.get_metadata([url])[0]
        return sig.get_value(Document(None, url=url), url_metadata)
