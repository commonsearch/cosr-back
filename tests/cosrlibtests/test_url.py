
def test_url():
    from cosrlib.url import URL

    assert URL("https://www.test.com").normalized == "test.com/"
    assert URL("https://www.test.com?").normalized == "test.com/"
    assert URL("https://www.test.com?").normalized_domain == "test.com"
    assert URL("https://www.test.com?").domain == "www.test.com"

    assert URL("http://sub.test.com/?x=a#b").normalized == "sub.test.com/?x=a"
    assert URL("http://sub.test.co.uk?x=a#b").normalized == "sub.test.co.uk/?x=a"

    assert URL("http://sub.test.co.uk?x=a#b").normalized_without_query == "sub.test.co.uk/"

    assert URL("http://sub.test.co.uk?x=a#b").suffix == "co.uk"
    assert URL("http://sub.test.co.uk?x=a#b").pld == "test.co.uk"

    assert URL("http://sub.test.co.uk/azerza/azer.html?x=a#b").homepage == "http://sub.test.co.uk/"

    assert URL('http://dc.weber.edu/\xc3\xaf\xc2\xbf\xc2\xbd/field/?a=b&c=d&e=\xc3\xaf\xc2\xbf\xc2\xbd#qq', check_encoding=True).url == "http://dc.weber.edu/%C3%AF%C2%BF%C2%BD/field/?a=b&c=d&e=%C3%AF%C2%BF%C2%BD#qq"
