
def test_signal_url_total_length(ranker):
  rank = lambda url: ranker.client.get_signal_value_from_url("url_total_length", url)

  rank_long = rank("http://www.verrrryyyylongdomain.com/very-long-url-xxxxxxxxxxxxxx.html")
  rank_short = rank("https://en.wikipedia.org/wiki/Maceo_Parker")
  rank_min = rank("http://t.co")

  assert 0 <= rank_long < rank_short < rank_min <= 1


def test_signal_url_path_length(ranker):
  rank = lambda url: ranker.client.get_signal_value_from_url("url_path_length", url)

  rank_hp = rank("http://www.longdomain.com")
  rank_hp2 = rank("http://www.domain.com/")

  assert rank_hp == rank_hp2

  rank_subpage = rank("http://t.co/p")

  assert rank_subpage < rank_hp

  rank_subpage_query = rank("http://t.co/p?q=1")

  assert rank_subpage_query < rank_subpage
