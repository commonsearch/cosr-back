import cld2


from . import BaseSignal


class Signal(BaseSignal):
    """ Classifier signal detecting the language of the page """

    uses_url_metadata = False

    def detect_from_document(self, document, html=None):

        return cld2.detect(
            html or document.source_data,
            isPlainText=False,
            useFullLangTables=True,
            hintTopLevelDomain=document.get_url().suffix,  # TODO: doesn't seem to have any influence?
            hintLanguage=None,
            hintLanguageHTTPHeaders=None,  # TODO from headers
            hintEncoding=None,  # TODO from headers
            returnVectors=False,
            debugScoreAsQuads=False,
            debugHTML=False,
            bestEffort=True,
            debugCR=False,
            debugVerbose=False,
            debugQuiet=True,
            debugEcho=False
        )

    def get_value(self, document, url_metadata):

        # https://github.com/GregBowyer/cld2-cffi/blob/master/cld2/__init__.py#L221

        # TODO: don't pass HTML but our own parsed list of visible words
        try:
            is_reliable, text_bytes_found, details = self.detect_from_document(document)  # pylint: disable=unused-variable
        except ValueError:
            # There was an encoding error. Force a cleanup of the utf-8.
            html = document.source_data.decode("utf-8", "ignore").encode("utf-8", "ignore")
            try:
                is_reliable, text_bytes_found, details = self.detect_from_document(document, html=html)  # pylint: disable=unused-variable
            except ValueError:
                # If there still is an error, we can't do much else than working harder
                # on the encoding detection
                return {}

        # print '  reliable: %s' % (is_reliable != 0)
        # print '  textBytes: %s' % text_bytes_found
        # print '  details: %s' % str(details)
        # print '  vectors: %s' % str(vectors)

        ret = {}

        if not is_reliable:
            return ret

        for lang in details:
            if lang[1] != "un":  # Unknown
                ret[lang[1]] = float(lang[2]) / 100

        return ret
