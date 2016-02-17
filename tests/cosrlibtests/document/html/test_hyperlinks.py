from cosrlib.document.html import HTMLDocument
import pytest


def _links(html, url=None):
    return HTMLDocument(html, url=url).parse().get_hyperlinks()


def test_get_hyperlinks():
    links = _links("""<html><head><title>Test title</title></head><body>x</body></html>""")
    assert len(links) == 0

    links = _links("""<html><head><title>Test title</title></head><body>
        <a name="x">Y</a>
    </body></html>""")
    assert len(links) == 0

    links = _links("""<html><head><title>Test title</title></head><body>
        <a href="">Y</a>
    </body></html>""")
    assert len(links) == 0

    links = _links("""<html><head><title>Test title</title></head><body>
        <a href="ftp://test.com">Y</a>
    </body></html>""")
    assert len(links) == 0

    links = _links("""<html><head><title>Test title</title></head><body>
        <a href="http://sub.test.com/page1?q=2&a=b#xxx">Y</a>
    </body></html>""")
    assert len(links) == 1
    assert links[0]["href"].url == "http://sub.test.com/page1?q=2&a=b#xxx"
    assert links[0]["words"] == ["Y"]

    links = _links("""<html><head><title>Test title</title></head><body>
        <a href="/page1?q=2&a=b#xxx">Y</a>
    </body></html>""", url="http://sub.test.com/page2")
    assert len(links) == 1
    assert links[0]["href"].url == "http://sub.test.com/page1?q=2&a=b#xxx"
    assert links[0]["words"] == ["Y"]

    links = _links("""<html><head><title>Test title</title></head><body>
        <a href="../page1?q=2&a=b#xxx">Y</a>
    </body></html>""", url="http://sub.test.com/page2/x.html")
    assert len(links) == 1
    assert links[0]["href"].url == "http://sub.test.com/page1?q=2&a=b#xxx"
    assert links[0]["words"] == ["Y"]


def test_get_hyperlinks_base_tag():

    links = _links("""<html><head><base href="https://example.com/d1/d2/" /><title>Test title</title></head><body>
        <a href="../page1?q=2&a=b#xxx">Y</a>
    </body></html>""", url="http://sub.test.com/page2/x.html")
    assert len(links) == 1
    assert links[0]["href"].url == "https://example.com/d1/page1?q=2&a=b#xxx"
    assert links[0]["words"] == ["Y"]
