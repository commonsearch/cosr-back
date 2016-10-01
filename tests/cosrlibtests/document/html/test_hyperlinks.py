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
        <a href="http://sub.test.com/page1?q=2&a=b#xxx">Y </a>
    </body></html>""")
    assert len(links) == 1
    assert links[0]["href"].url == "http://sub.test.com/page1?q=2&a=b#xxx"
    assert links[0]["text"] == "Y"

    links = _links("""<html><head><title>Test title</title></head><body>
        <a href="/page1?q=2&a=b#xxx">Y x</a>
    </body></html>""", url="http://sub.test.com/page2")
    assert len(links) == 1
    assert links[0]["href"].url == "http://sub.test.com/page1?q=2&a=b#xxx"
    assert links[0]["text"] == "Y x"

    links = _links("""<html><head><title>Test title</title></head><body>
        <a href="../page1?q=2&a=b#xxx">Y_</a>
    </body></html>""", url="http://sub.test.com/page2/x.html")
    assert len(links) == 1
    assert links[0]["href"].url == "http://sub.test.com/page1?q=2&a=b#xxx"
    assert links[0]["text"] == "Y_"

    links = _links("""<html><head><title>Test title</title></head><body>
        <a href="http://UPPER.CASE.coM/PATH?QUERY=V">*Y</a>
    </body></html>""", url="http://sub.test.com/page2/x.html")
    assert len(links) == 1
    assert links[0]["href"].url == "http://upper.case.com/PATH?QUERY=V"
    assert links[0]["text"] == "*Y"

    links = _links("""<html><head><title>Test title</title></head><body>
        <a href="//UPPER.CASE.coM/PATH?QUERY=V">Y</a>
    </body></html>""", url="http://sub.test.com/page2/x.html")
    assert len(links) == 1
    assert links[0]["href"].url == "http://upper.case.com/PATH?QUERY=V"
    assert links[0]["text"] == "Y"

    # We do not index links behind any kind of auth
    links = _links("""<html><head><title>Test title</title></head><body>
        <a href="http://user@domain.com">Y</a>
    </body></html>""", url="http://sub.test.com/page2/x.html")
    assert len(links) == 0

    # Looks like a forgotten mailto:, don't index
    links = _links("""<html><head><title>Test title</title></head><body>
        <a href="user@domain.com">Y</a>
    </body></html>""", url="http://sub.test.com/page2/x.html")
    assert len(links) == 0

    # Invalid URL should be filtered
    links = _links("""<html><head><title>Test title</title></head><body>
        <a href="http://www.[wsj-ticker ticker=">Y</a>
    </body></html>""", url="http://sub.test.com/page2/x.html")
    assert len(links) == 0

    links = _links("""<html><head><title>Test title</title></head><body>
        <a href="<object width=">Y</a>
    </body></html>""", url="http://sub.test.com/page2/x.html")
    assert len(links) == 0

    links = _links("""<html><head><title>Test title</title></head><body>
        <a href="http://<object width=">Y</a>
    </body></html>""", url="http://sub.test.com/page2/x.html")
    assert len(links) == 0

    # We don't index TLDs either
    links = _links("""<html><head><title>Test title</title></head><body>
        <a href="http://com/x">Y</a>
    </body></html>""", url="http://sub.test.com/page2/x.html")
    assert len(links) == 0

    links = _links("""<html><head><title>Test title</title></head><body>
        <a href="http://newunknowntldxx/x">Y</a>
    </body></html>""", url="http://sub.test.com/page2/x.html")
    assert len(links) == 0


def test_get_hyperlinks_base_tag():

    links = _links("""<html><head><base href="https://example.com/d1/d2/" /><title>Test title</title></head><body>
        <a href="../page1?q=2&a=b#xxx">Y</a>
    </body></html>""", url="http://sub.test.com/page2/x.html")
    assert len(links) == 1
    assert links[0]["href"].url == "https://example.com/d1/page1?q=2&a=b#xxx"
    assert links[0]["text"] == "Y"
