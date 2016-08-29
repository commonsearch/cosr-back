from __future__ import absolute_import, division, print_function, unicode_literals

from cosrlib.url import URL
from . import BaseDataProvider


class DataProvider(BaseDataProvider):
    """ Return the title and description from DMOZ
    """

    dump_testdata = "tests/testdata/dmoz.rdf"
    dump_url = "http://rdf.dmoz.org/rdf/content.rdf.u8.gz"
    dump_compression = "gz"
    dump_format = "xml"
    dump_batch_size = 100000
    dump_count_estimate = 3000000

    def import_row(self, i, row):
        """ Returns a (key, value) pair for this row from the dump file """

        event, elem = row

        if event != "end":
            return

        if elem.tag == "{http://dmoz.org/rdf/}ExternalPage":
            url = URL(elem.attrib["about"].encode("utf-8")).normalized
            title = elem.find("{http://purl.org/dc/elements/1.0/}Title")
            description = elem.find("{http://purl.org/dc/elements/1.0/}Description")

            if url:
                yield url, {
                    "dmoz_title": (title.text or "") if (title is not None) else "",
                    "dmoz_description": (description.text or "") if (description is not None) else ""
                }

            self.clear_xml_elements(title, description, elem)

        elif elem.tag not in (
                "{http://purl.org/dc/elements/1.0/}Title",
                "{http://purl.org/dc/elements/1.0/}Description"
        ):
            self.clear_xml_elements(elem)
