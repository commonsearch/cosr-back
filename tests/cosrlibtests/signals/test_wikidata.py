
def test_signal_wikidata_url(ranker):
    rank = lambda url: ranker.client.get_signal_value_from_url("wikidata_url", url)

    assert rank("http://www.douglasadams.com") > 0.5
    assert rank("http://www.douglasadams.com/?a=b") > 0.5
    assert rank("http://www.douglasadams.com/page2") == 0.  # TODO, check domain?
    assert rank("http://www.paulherbert.com/") == 0.
