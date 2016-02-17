# -*- coding: utf-8 -*-

SAMPLES = {
  "nytimes-article-2006.html": {
    "url": "http://www.nytimes.com/2006/06/16/arts/music/16prince.web.html",
    "title": "Prince a Surprise Guest at Celebrate Brooklyn  - New York Times",
    "assert_words_missing": ["servedbyopenx", "newspaper", "technology"],
    "assert_words": ["prince", "maceo"]
  },
  "nytimes-article-2011.html": {
    "url": "http://www.nytimes.com/2011/10/06/arts/music/fred-wesley-pee-wee-ellis-and-maceo-parker-reunite.html",
    "title": "Fred Wesley, Pee Wee Ellis and Maceo Parker Reunite - The New York Times",
    "assert_words_missing": ["sections", "advertisement", "tweet", "browser"],
    "assert_words": ["godsons", "maceo", "wesley"]
  },
  "wordpress-2014.html": {
    "url": "https://thesunbreak.com/2014/06/23/live-show-review-maceo-parker-at-jazz-alley/",
    "title": "Live Show Review: Maceo Parker at Jazz Alley | The SunBreak",
    "assert_words_missing": ["weather", "primary", "notifications", "timberfest"],
    "assert_words": ["sunbreak", "maceo", "believe"]
  },
  "wikipedia-2015.html": {
    "url": "https://en.wikipedia.org/wiki/Nine_Inch_Nails",
    "title": "Nine Inch Nails - Wikipedia, the free encyclopedia",
    "assert_words_missing": ["random", "nederlands", "developers", "logged"],
    "assert_words": ["nine", "kiss", "velvet", "rockbeat", "snapped"]
  },
  "nutch-nested-spider-trap.html": {
    "url": "http://svn.apache.org/viewvc/nutch/trunk/src/testresources/fetch-test-site/nested_spider_trap.html?revision=1175075&view=co",
    "title": "nested spider trap",
    "assert_words_missing": ["b", "i"],
    "assert_words": ["nutch", "fetcher", "test", "page"]
  }
}
