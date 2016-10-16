
def test_signal_commonsearch_host_pagerank(ranker):
    rank = lambda url: ranker.client.get_signal_value_from_url("commonsearch_host_pagerank", url)

    assert 0 < rank("twitter.com") < 1
    assert 0 < rank("facebook.com") < 1
    assert 0 < rank("iheart.com") < 1
    assert 0 < rank("youtube.com") < 1
