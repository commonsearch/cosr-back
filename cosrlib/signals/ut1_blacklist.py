from . import BaseSignal


class Signal(BaseSignal):
    """ Classifier signal detecting the UT1 categories: https://dsi.ut-capitole.fr/blacklists/index_en.php """

    def get_value(self, document, url_metadata):

        classes = {}
        for data in url_metadata.itervalues():
            classes.update({
                k: 1. for k in (data.ut1_blacklist)
            })
        return classes
