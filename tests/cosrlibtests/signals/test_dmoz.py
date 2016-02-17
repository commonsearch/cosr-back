
def test_signal_dmoz_domain(ranker):
    rank = lambda url: ranker.client.get_signal_value_from_url("dmoz_domain", url)

    assert rank("http://www.neoanime.org") == 1.
    assert rank("http://www.neoanime.org/") == 1.
    assert rank("http://www.neoanime.org/non-existing-page") == 1.
    assert rank("http://www.non-existing-domain.com") == 0.


def test_signal_dmoz_url(ranker):
    rank = lambda url: ranker.client.get_signal_value_from_url("dmoz_url", url)

    assert rank("http://www.neoanime.org") == 1.
    assert rank("http://www.neoanime.org/?") == 1.
    assert rank("http://www.hcs.harvard.edu/~anime/") == 1.
    assert rank("http://www.non-existent-domain.com") == 0.
    assert rank("http://www.hcs.harvard.edu/non-existing-page") == 0.
