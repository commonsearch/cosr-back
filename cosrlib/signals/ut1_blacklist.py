from . import BaseSignal


class Signal(BaseSignal):
    """ Classifier signal detecting the UT1 categories: https://dsi.ut-capitole.fr/blacklists/index_en.php """

    def get_value(self, document, url_metadata):

        return {
            k: 1. for k in (url_metadata["ut1_blacklist_classes"] or [])
        }
