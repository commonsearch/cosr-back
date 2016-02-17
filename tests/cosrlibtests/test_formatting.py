from cosrlib.formatting import format_title, format_summary
from cosrlib.document.html import HTMLDocument


def test_format_title():

    def format_html_title(title, url=None):
        doc = HTMLDocument("""
            <html><head><title>%s</title></head><body>Hello</body></html>
        """ % title, url=url)
        doc.parse()
        return format_title(doc, {})

    assert format_html_title("A Title!") == "A Title!"
    assert format_html_title("  A  \n Title\t \t!  ") == "A Title !"
    assert format_html_title("a" * 100) == ("a" * 70) + "..."

    assert format_html_title(("a" * 60) + " 2345678 1234567") == ("a" * 60) + " 2345678..."
    assert format_html_title(("a" * 60) + " 234567890 1234567") == ("a" * 60) + " 234567890..."
    assert format_html_title(("a" * 60) + " 2345678901 1234567") == ("a" * 60) + "..."

    # Test domain fallback
    assert format_html_title("  ", url="http://www.example.com/hello.html") == "Example"

    # Test blacklist
    assert format_html_title("  home ", url="http://www.example.com/hello.html") == "Example"

    # Test OGP
    html = """<html>
        <head><meta property="og:title" content="Open graph title  " /></head>
        <body>This is &lt;body&gt; text</body>
    </html>"""

    page = HTMLDocument(html).parse()
    assert format_title(page, {}) == "Open graph title"


def test_format_summary():

    html = """<html>
        <head><meta name="Description" content=" This   is a &lt;summary&gt;!" /></head>
        <body>This is &lt;body&gt; text</body>
    </html>"""

    page = HTMLDocument(html).parse()
    assert format_summary(page, {}) == "This is a <summary>!"

    html = """<html>
        <head><meta property="og:description" content=" This   is a &lt;summary&gt;!" /></head>
        <body>This is &lt;body&gt; text</body>
    </html>"""

    page = HTMLDocument(html).parse()
    assert format_summary(page, {}) == "This is a <summary>!"

    html = """<html>
        <head><meta name="Description" content="" /></head>
        <body> <div>This is &lt;body&gt; text, very detailed, very long xxxxxxxxx! </div></body>
    </html>"""

    page = HTMLDocument(html).parse()
    assert format_summary(page, {}) == "This is <body> text, very detailed, very long xxxxxxxxx!"

    html = """<html>
        <head><meta name="Description" content="" /></head>
        <body> 
            <div>This is &lt;body&gt; text, very detailed, very long xxxxxxxxx! </div>
            <h1>But there is a more informative title! Use it</h1>
        </body>
    </html>"""

    page = HTMLDocument(html).parse()
    assert format_summary(page, {}) == "But there is a more informative title! Use it"
