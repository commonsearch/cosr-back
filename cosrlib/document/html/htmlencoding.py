import cgi

import webencodings
import cchardet

from cosrlib import re
from .parsers import GUMBOCY_PARSER_HEAD


_RE_XML_ENCODING = re.compile(r'^\s*\<\?xml\s+version\="1\.0"\s+encoding\="([^"]+)"\?\>')


def get_encoding_from_content_type(content_type):
    _, params = cgi.parse_header(content_type.decode("ascii", "ignore"))
    if params.get("charset"):
        detected = webencodings.lookup(params["charset"])
        if detected:
            return detected.codec_info


class HTMLEncoding(object):
    """ This class deals with the many different encoding and quirks found in pages on the web,
        and tries to normalize everything in UTF-8 """

    def __init__(self, document):
        self.doc = document
        self.parser = None
        self.detected = None

    def ensure_utf8(self):
        """ Makes sure we have only UTF-8 in source_data """

        self.detected = self.detect()

        # No encoding detected, let's hope we are utf-8
        if self.detected is None or self.detected.name == "utf-8":
            return

        # If the source_encoding is not utf-8, reencode the source_data!
        self.doc.source_data = self.detected.decode(
            self.doc.source_data, "ignore"
        )[0].encode("utf-8", "ignore")

    def detect(self):
        """ Returns the python-compatible encoding of the doc as a codecs.CodecInfo object

        We attempt to detect the encoding in the following order:
         - BOM presence (https://en.wikipedia.org/wiki/Byte_order_mark)
         - HTTP header
         - meta charset or meta http-equiv
         - XML prolog
         - Auto-detection with chardet
         - utf-8
        """

        # See http://html5lib.readthedocs.org/en/latest/movingparts.html#encoding-discovery for more

        bom_encoding = self.detect_bom()
        if bom_encoding:
            return bom_encoding

        if self.doc.source_headers.get("content-type"):
            header_encoding = get_encoding_from_content_type(self.doc.source_headers["content-type"])
            if header_encoding:
                return header_encoding

        # Create a temporary parser to look in meta tags
        GUMBOCY_PARSER_HEAD.parse(self.doc.source_data)

        meta_encoding = self.detect_meta_charset()
        if meta_encoding:
            return meta_encoding

        xml_encoding = self.detect_xml_encoding()
        if xml_encoding:
            return xml_encoding

        # Use slow auto-detection!
        guess = self.guess_encoding()
        return guess

    def guess_encoding(self):
        """ Makes an expensive guess of the charset with the chardet library """

        # TODO: would it be faster to look only in the first N thousand bytes?
        detected = cchardet.detect(self.doc.source_data)
        if detected.get("encoding"):
            c = webencodings.lookup(detected.get("encoding"))
            if c:
                return c.codec_info

    def detect_meta_charset(self):
        """ Returns the encoding found in meta tags in the doc """

        for node in GUMBOCY_PARSER_HEAD.listnodes():
            if node[1] == "meta" and len(node) > 2:
                if node[2].get("charset"):
                    detected = webencodings.lookup(node[2]["charset"])
                    if detected:
                        return detected.codec_info
                elif node[2].get("http-equiv", "").lower().strip() == "content-type":
                    meta_encoding = get_encoding_from_content_type(node[2].get("content", ""))
                    if meta_encoding:
                        return meta_encoding

    def detect_xml_encoding(self):
        """ Detects an encoding from an XML prolog """

        match = _RE_XML_ENCODING.search(self.doc.source_data)
        if match:
            detected = webencodings.lookup(match.group(1))
            if detected:
                return detected.codec_info

    def detect_bom(self):
        """ Returns the encoding if a BOM was found. """

        if self.doc.source_data.startswith(b'\xFF\xFE'):
            return webencodings._UTF16LE.codec_info  # pylint: disable=protected-access
        elif self.doc.source_data.startswith(b'\xFE\xFF'):
            return webencodings._UTF16BE.codec_info  # pylint: disable=protected-access
        elif self.doc.source_data.startswith(b'\xEF\xBB\xBF'):
            return webencodings.UTF8.codec_info
        # TODO utf-32?
        return None
