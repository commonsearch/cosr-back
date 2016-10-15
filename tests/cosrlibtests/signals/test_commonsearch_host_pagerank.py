
def test_signal_commonsearch_host_pagerank(ranker):
    rank = lambda url: ranker.client.get_signal_value_from_url("commonsearch_host_pagerank", url)

    assert rank("twitter.com") > 1.9
    assert 1.9 > rank("facebook.com") > 1.4
    assert rank("iheart.com") < 0.9
    assert rank("youtube.com") < 0.6
