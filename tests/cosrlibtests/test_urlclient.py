def test_urlclient_getids(urlclient):
    # TODO proper fixture with reset at each test for local dev!

    client = urlclient.client

    # Test basic hashes being different
    wp_homepage = client.get_id("http://www.wordpress.org")
    wp_com_homepage = client.get_id("http://www.wordpress.com")
    assert wp_homepage != wp_com_homepage

    # Test all variations of the same URL
    assert wp_homepage == client.get_id("http://www.wordpress.org")
    assert wp_homepage == client.get_id("http://www.wordpress.org/")
    assert wp_homepage == client.get_id("https://www.wordpress.org/")
    assert wp_homepage == client.get_id("http://wordpress.org")
    assert wp_homepage == client.get_id("http://wordpress.org/")
    assert wp_homepage == client.get_id("https://wordpress.org/")
    assert wp_homepage == client.get_id("https://wordpress.org/#")
    assert wp_homepage == client.get_id("https://wordpress.org/#hello")
    assert wp_homepage == client.get_id("https://wordpress.org/?")

    # Different pages
    wp_new_page = client.get_id("http://www.wordpress.org/new_unknown_url.html")
    wp_new_page2 = client.get_id("http://www.wordpress.org/new_unknown_url2.html")
    wp_new_page3 = client.get_id("http://en.wordpress.org/new_unknown_url2.html")
    wp_new_page4 = client.get_id("http://en.wordpress.org/new_unknown_url2.html?a=a")

    # Must all be different
    assert len(set([
        wp_new_page,
        wp_new_page2,
        wp_new_page3,
        wp_new_page4,
        wp_homepage,
        wp_com_homepage
    ])) == 6

    # Test multiple urls at once
    res = list(client.get_ids([
        "http://www.wordpress.org",
        "http://www.wordpress.org/new_unknown_url.html"
    ]))
    assert res == [wp_homepage, wp_new_page]

    assert wp_homepage == client.get_domain_id("http://www.wordpress.org")
    assert wp_homepage == client.get_domain_id("http://wordpress.org")
    assert wp_homepage == client.get_domain_id("https://wordpress.org/?")
    assert wp_homepage == client.get_domain_id("https://wordpress.org/page")
    assert wp_homepage != client.get_domain_id("https://en.wordpress.org/page")


def test_urlclient_getmetadata(urlclient):

    client = urlclient.client

    meta = client.get_metadata(["http://www.wordpress.org/index.html"])
    print meta
