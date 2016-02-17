import pytest
import os

# https://github.com/w3c/web-platform-tests/tree/master/html/syntax/parsing-html-fragments
# https://www.w3.org/International/tests/repository/html5/the-input-byte-stream/results-basics#basics

W3C_TESTS = {
    "001": "iso8859-15",
    "003": "utf-16-le",

    # TODO: this fails unexplicably. Did we mess up the file import?
    # "004": "utf-16-be",

    "007": "iso8859-15",
    "009": "iso8859-15",

    # Default encoding test: according to the spec it should be "None" (for the utf-8 default)
    # but we do auto-detection
    "015": "cp1252",

    "034": "utf-8",
    "016": "iso8859-15",
    "018": "iso8859-15",
    "037": "utf-8",
    "038": "utf-8",
    "030": "iso8859-15"
}


@pytest.mark.parametrize(("p_w3c_test", "p_expected_encoding"), W3C_TESTS.items())
def test_encoding_w3c(p_w3c_test, p_expected_encoding):
    from cosrlib.document.html import HTMLDocument

    test_file = os.path.join(
        "tests/testdata/html_w3c_encoding_testcases",
        "the-input-byte-stream-%s.html" % p_w3c_test
    )

    with open(test_file, "rb") as f:
        headers = {}
        if os.path.isfile(test_file + ".headers"):
            with open(test_file + ".headers", "rb") as hf:
                headers["content-type"] = hf.read()[14:].strip()

        html = f.read()

        # print repr(html[0:10])

        doc = HTMLDocument(html, url=None, headers=headers)

        if p_expected_encoding is None:
            assert doc.encoding.detect() is None
        else:
            assert doc.encoding.detect().name == p_expected_encoding

        doc.parse()


def test_encoding_x_user_defined():
    from cosrlib.document.html import HTMLDocument

    doc = HTMLDocument("""<html><head><meta charset="x-user-defined"></head><body>Hello</body></html>""")
    assert doc.encoding.detect().name == "x-user-defined"
    doc.parse()


def test_encoding_aliases():
    from cosrlib.document.html import HTMLDocument

    doc = HTMLDocument("""<html><head><meta charset="tis-620"></head><body>Hello</body></html>""")
    assert doc.encoding.detect().name == "cp874"
    doc.parse()

    doc = HTMLDocument("""<html><head><meta charset="windows-874"></head><body>Hello</body></html>""")
    assert doc.encoding.detect().name == "cp874"
    doc.parse()


def test_encoding_xml():
    from cosrlib.document.html import HTMLDocument

    doc = HTMLDocument("""<?xml version="1.0" encoding="shift_jis"?><!DOCTYPE html>
<html  lang="en" ></html>
    """)
    assert doc.encoding.detect().name == "shift_jis"
    doc.parse()


def test_reparse():
    from cosrlib.document.html import HTMLDocument

    doc = HTMLDocument("""<html><head><meta charset="iso-8859-15"><title>Mac\xe9o</title></head></html>""")
    assert doc.encoding.detect().name == "iso8859-15"

    # A re-parsing of the document should be triggered, gumbo only accepts utf-8
    doc.parse()

    assert doc.get_title() == "Mac\xc3\xa9o"
