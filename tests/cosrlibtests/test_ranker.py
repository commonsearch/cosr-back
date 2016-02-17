
def test_get_global_document_rank_url(ranker):

    def rank(url):
        r, _ = ranker.client.get_global_url_rank(url)
        return r

    assert 1 >= rank("http://google.com") > 0.9

    # Facebook isn't in our DMOZ dump
    assert rank("http://google.com") > rank("http://facebook.com") > 0.1

    assert rank("http://www.non-existing-domain.local") > rank("http://www.xxxxx-non-existing-domain.local")
    assert rank("http://www.xxxxx-non-existing-domain.local") > rank("http://www.xxxxx-non-existing-domain.local/a-specific-page.html")
