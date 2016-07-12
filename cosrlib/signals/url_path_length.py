from . import BaseSignal


class Signal(BaseSignal):
    """ Simple ranking signal based on the length of the path in the document URL """

    uses_url_metadata = False

    def get_value(self, document, url_metadata):
        url = document.get_url()
        length = (
            len(url.parsed.path) +
            len(url.parsed.query) +
            len(url.parsed.fragment)
        )
        return max(0, min(1, (101.0 - length) / 100))
