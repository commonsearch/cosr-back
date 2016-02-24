from . import BaseSignal


class Signal(BaseSignal):
    """ Simple ranking signal based on the length of the document's URL """

    uses_url_metadata = False

    def get_value(self, document, url_metadata):
        return max(0, min(1, (111.0 - len(document.get_url().normalized)) / 100))
