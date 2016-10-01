from __future__ import absolute_import, division, print_function, unicode_literals

from . import BaseSignal


class Signal(BaseSignal):
    """ Ranking signal based on the URL presence in DMOZ
    """

    def get_value(self, document, url_metadata):

        return (
            float(bool(url_metadata["url"].dmoz_title)) or
            float(bool(url_metadata["url_without_query"].dmoz_title))
        )
