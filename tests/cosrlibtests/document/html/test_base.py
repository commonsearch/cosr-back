from cosrlib.document.html import HTMLDocument


def test_get_title():
    assert HTMLDocument(
        """<html><head><title>Test title</title></head><body>x</body></html>"""
    ).parse().get_title() == "Test title"

    assert HTMLDocument(
        """<html><title>Test title</title>XX</html>"""
    ).parse().get_title() == "Test title"

    assert HTMLDocument(
        """<html><head><title>Test title</title></head><body><title>x</title></body></html>"""
    ).parse().get_title() == "Test title"


def test_get_url_words():

    doc = HTMLDocument("", url="http://www.nytimes.com/2011/10/06/arts/music/maceo-parker.html?print=true#hash").parse()
    assert doc.get_url_words() == [
        "nytimes", "com", "2011", "10", "06", "arts", "music", "maceo", "parker", "html"
    ]

    doc = HTMLDocument("", url="https://en.wikipedia.org/wiki/Nine_Inch_Nails").parse()
    assert doc.get_url_words() == [
        "en", "wikipedia", "org", "wiki", "nine", "inch", "nails"
    ]


def test_get_domain_paid_words():

    doc = HTMLDocument("", url="http://www.bbc.co.uk/2011/10/06/arts/music/maceo-parker.html?print=true")
    assert doc.get_domain_paid_words() == ["bbc"]


def test_get_url():

    # When none is given, we take the URL
    html = """<html><head></head><body>x</body></html>"""
    page = HTMLDocument(html, url="http://example.com/page.html").parse()
    assert page.get_url().url == "http://example.com/page.html"

    # But when a tag is present, it has precedence
    html = """<html><head><link rel="canonical" href="http://example.com/page2.html" /></head><body>x</body></html>"""
    page = HTMLDocument(html, url="http://example.com/page.html").parse()
    assert page.get_url().url == "http://example.com/page2.html"

    # Including with strange caps
    html = """<htmL><heaD><linK reL="CANonical" hreF="http://example.com/Page2.html" /></head><body>x</body></html>"""
    page = HTMLDocument(html, url="http://example.com/page.html").parse()
    assert page.get_url().url == "http://example.com/Page2.html"


def test_get_canonical_url():

    html = """<html><head></head><body>x</body></html>"""
    page = HTMLDocument(html, url="http://example.com/page.html").parse()
    assert page.parse_canonical_url() is None

    html = """<html><head><link rel="canonical" href="" /></head><body>x</body></html>"""
    page = HTMLDocument(html, url="http://example.com/page.html").parse()
    assert page.parse_canonical_url() is None

    html = """<html><head><link rel="canonical" href="http://example.com/page2.html" /></head><body>x</body></html>"""
    page = HTMLDocument(html, url="http://example.com/page.html").parse()
    assert page.parse_canonical_url().url == "http://example.com/page2.html"

    html = """<html><head><linK reL="caNonical" hreF="http://example.com/page2.html" /></head><body>x</body></html>"""
    page = HTMLDocument(html, url="http://example.com/page.html").parse()
    assert page.parse_canonical_url().url == "http://example.com/page2.html"

    # Cross domain blocked for now
    html = """<html><head><linK reL="caNonical" hreF="http://example2.com/page2.html" /></head><body>x</body></html>"""
    page = HTMLDocument(html, url="http://example.com/page.html").parse()
    assert page.parse_canonical_url() is None

    # Relative URLs
    html = """<html><head><linK reL="caNonical" hreF="/dir2/page2.html" /></head><body>x</body></html>"""
    page = HTMLDocument(html, url="http://example.com/dir/page.html").parse()
    assert page.parse_canonical_url().url == "http://example.com/dir2/page2.html"

    html = """<html><head><linK reL="caNonical" hreF="dir2/page2.html" /></head><body>x</body></html>"""
    page = HTMLDocument(html, url="http://example.com/dir/page.html").parse()
    assert page.parse_canonical_url().url == "http://example.com/dir/dir2/page2.html"

    html = """<html><head><linK reL="caNonical" hreF="//example.com/dir2/page2.html" /></head><body>x</body></html>"""
    page = HTMLDocument(html, url="http://example.com/dir/page.html").parse()
    assert page.parse_canonical_url().url == "http://example.com/dir2/page2.html"


def test_hidden_text():

    html = """<html><head></head><body>
        <script> hello(); </script>
        <style> style { good } </style>
        <!-- comment -->
        text
        <p>p</p>
        <div style='display: none;'>hidden by display</div>
        <div hidden>hidden by html5 attribute</div>
        <div aria-hidden="true">hidden by aria</div>
        <div aria-hidden="false">not_aria</div>
        <div style='visibility: hidden;'>hidden by visibility</div>
    </body></html>"""
    page = HTMLDocument(html).parse()

    assert page.get_all_words() == set(["text", "p", "not_aria"])


def test_get_hyperlinks():

    # When none is given, we take the URL
    html = """<html><head></head><body>before <a href="http://example.com/page1">link text</a> after</body></html>"""
    page = HTMLDocument(html, url="http://example.com/page.html").parse()

    links = page.get_hyperlinks()
    assert len(links) == 1
    assert links[0]["href"].url == "http://example.com/page1"
    assert links[0]["words"] == ["link", "text"]
