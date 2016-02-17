
def test_signal_ut1blacklist(ranker):
    from cosrlib.document import Document
    detect = lambda doc: ranker.client.get_signal_value("ut1_blacklist", doc)

    # No match
    for i in range(2):

        assert detect(Document(None, url="http://unclassified-domain.com")) == {}

        # Domain match
        assert detect(Document(None, url="http://dict.cc")) == {"translation": 1}
        assert detect(Document(None, url="http://www.dict.cc")) == {"translation": 1}
        assert detect(Document(None, url="https://www.dict.cc")) == {"translation": 1}
        assert detect(Document(None, url="https://www.dict.cc/")) == {"translation": 1}
        assert detect(Document(None, url="https://www.dict.cc/page1")) == {"translation": 1}

        bing_translate = {"translation": 1, "liste_bu": 1}

        # URL no match

        assert detect(Document(None, url="https://bing.com/translat")) != bing_translate
        assert detect(Document(None, url="https://bing.com/translatorx")) != bing_translate
        assert detect(Document(None, url="http://subdomain.bing.com/translator")) != bing_translate

        # URL match
        assert detect(Document(None, url="https://bing.com/translator")) == bing_translate
        assert detect(Document(None, url="http://bing.com/translator")) == bing_translate
        assert detect(Document(None, url="http://www.bing.com/translator")) == bing_translate
        assert detect(Document(None, url="http://bing.com/translator?q=1#a=b")) == bing_translate
