from __future__ import absolute_import, division, print_function, unicode_literals

from . import BaseSignal


class Signal(BaseSignal):
    """ Simple ranking signal based on the presence of a subdomain in the URL """

    uses_url_metadata = False

    def get_value(self, document, url_metadata):

        url = document.get_url()
        if url.normalized_subdomain == "":
            return 1.0

        return 0.0
