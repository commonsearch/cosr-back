
def test_signal_alexa_top1m(ranker):
    rank = lambda url: ranker.client.get_signal_value_from_url("alexa_top1m", url)

    assert 1 >= rank("http://www.google.com") > 0.99999
    assert 1 >= rank("http://www.facebook.com") > 0.99999
    assert rank("http://www.google.com") != rank("http://www.facebook.com")
    assert rank("http://www.xxxxx-non-existing-domain.local") is None
