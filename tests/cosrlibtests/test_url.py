import pytest
from cosrlib.url import URL
from cosrlib.document import Document


def test_url():
    assert URL("http://nourd.gouv.fr").normalized == "nourd.gouv.fr"
    assert URL("https://www.test.com").normalized == "test.com"
    assert URL("https://www.test.com?").normalized == "test.com"
    assert URL("https://www.test.com?").normalized_domain == "test.com"
    assert URL("https://www.test.com?").domain == "www.test.com"

    assert URL("http://sub.test.com/?x=a#b").normalized == "sub.test.com/?x=a"
    assert URL("http://sub.test.co.uk?x=a#b").normalized == "sub.test.co.uk/?x=a"

    assert URL("http://sub.test.co.uk/page1?x=a#b").normalized == "sub.test.co.uk/page1?x=a"
    assert URL("http://sub.test.co.uk/page1/?x=a#b").normalized == "sub.test.co.uk/page1/?x=a"

    assert URL("http://sub.test.co.uk?x=a#b").normalized_without_query == "sub.test.co.uk"

    assert URL("http://sub.test.co.uk?x=a#b").suffix == "co.uk"
    assert URL("http://sub.test.co.uk?x=a#b").pld == "test.co.uk"

    assert URL("http://www.sub.test.co.uk?x=a#b").subdomain == "www.sub"
    assert URL("http://www.sub.test.co.uk?x=a#b").normalized_subdomain == "sub"

    assert URL("http://sub.test.co.uk/azerza/azer.html?x=a#b").homepage == "http://sub.test.co.uk"

    assert URL('http://dc.weber.edu/\xc3\xaf\xc2\xbf\xc2\xbd/field/?a=b&c=d&e=\xc3\xaf\xc2\xbf\xc2\xbd#qq', check_encoding=True).url == "http://dc.weber.edu/%C3%AF%C2%BF%C2%BD/field/?a=b&c=d&e=%C3%AF%C2%BF%C2%BD#qq"


@pytest.mark.parametrize('url,normalized_domain,normalized', [
    ('http://example.org:8080/foo', 'example.org:8080', 'example.org:8080/foo'),
    ('http://example.org:80/foo', 'example.org', 'example.org/foo'),
    ('https://example.org:443', 'example.org', 'example.org')
    ])
def test_normalize(url, normalized_domain, normalized):
    _url = URL(url)
    assert _url.normalized_domain == normalized_domain
    assert _url.normalized == normalized

def test_get_domain_words():
    url = "http://nourd.gouv.fr"
    d = Document([], url=URL(url))
    assert d.get_domain_words() == ["nourd", "gouv", "fr"]
