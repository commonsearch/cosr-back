import pytest
from cosrlib.url import URL, tld_extract
import pickle


def test_url():
    assert URL("https://www.test.com").normalized == "test.com"
    assert URL("https://www.test.com?").normalized == "test.com"
    assert URL("https://www.test.com?").normalized_domain == "test.com"
    assert URL("https://www.test.com?").domain == "www.test.com"

    assert URL(u"https://www.test.com?").domain == "www.test.com"
    assert type(URL(u"https://www.test.com?").domain) == str
    assert type(URL(u"https://www.test.com?").normalized_domain) == str

    assert URL("https://.test.com").domain == ".test.com"
    assert URL("https://test.com.").domain == "test.com."
    assert URL("https://.test.com.").domain == ".test.com."

    assert URL("https://www.test.com.").normalized_domain == "test.com"
    assert URL("https://.www.test.com").normalized_domain == "test.com"
    assert URL("https://www.www.test.com").normalized_domain == "test.com"
    assert URL("https://.www.www.test.com").normalized_domain == "test.com"

    assert URL("https://.test.com").normalized_subdomain == ""
    assert URL("https://.www.test.com").normalized_subdomain == ""
    assert URL("https://.example.test.com").normalized_subdomain == "example"

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

    assert URL("http://nord.gouv.fr").normalized == "nord.gouv.fr"


def test_tld_extract():
    assert tld_extract("sub.test.com") == ("sub", "test", "com")
    assert tld_extract(".test.com") == ("", "test", "com")
    assert tld_extract(".test.com.") == ("", "test", "com")
    assert tld_extract(".www.test.com.") == ("www", "test", "com")

    assert tld_extract(u".www.test.com.") == ("www", "test", "com")
    assert [type(x) for x in tld_extract(u".www.test.com.")] == [str, str, str]


@pytest.mark.parametrize('url,normalized_domain,normalized', [
    ('http://example.org:8080/foo', 'example.org:8080', 'example.org:8080/foo'),
    ('http://example.org:80/foo', 'example.org', 'example.org/foo'),
    ('https://example.org:443', 'example.org', 'example.org')
])
def test_normalize(url, normalized_domain, normalized):
    _url = URL(url)
    assert _url.normalized_domain == normalized_domain
    assert _url.normalized == normalized


def test_pickling():

    dumped = pickle.dumps(URL("http://sub.test.co.uk/azerza/azer.html?x=a#b"))
    url = pickle.loads(dumped)
    assert url.url == "http://sub.test.co.uk/azerza/azer.html?x=a#b"
