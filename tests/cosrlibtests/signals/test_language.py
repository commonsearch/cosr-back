

def _make_document(url, text, title=""):
    from cosrlib.document.html import HTMLDocument
    html = "<html><head><title>%s</title></head><body>%s</body></html>" % (
        title, text
    )
    return HTMLDocument(html, url=url).parse()


def test_signal_language(ranker):
    detect = lambda doc: ranker.client.get_signal_value("language", doc)

    langs = detect(_make_document("http://example.com", "This is obviously english text"))
    assert len(langs) == 1
    assert langs["en"]

    langs = detect(_make_document("http://example.com", "Ceci est du francais, biensur."))
    assert len(langs) == 1
    assert langs["fr"]

    langs = detect(_make_document("http://example.com", "Esto es espanol, por seguro"))
    assert len(langs) == 1
    assert langs["es"]

    langs = detect(_make_document("http://example.com", "Deutsch ist die meistverbreitete Muttersprache"))
    assert len(langs) == 1
    assert langs["de"]

    # Test a mixed lang document
    langs = detect(_make_document(
        "http://example.fr",
        "Die Standardsprache Standarddeutsch setzt sich aus den Standardvarietaten zusammen und einer Vielzahl von hochdeutschen und niederdeutschen Mundarten, die in einem Dialektkontinuum miteinander verbunden sind. Die Standardvarietaten entstanden aus deutschen Mundarten und Kanzleisprachen. " +
        "Du fait de ses nombreux dialectes, l'allemand constitue dans une certaine mesure une langue-toit. Ceci est du francais, biensur. Nous pourrions continuer mais cela suffit."
    ))
    assert len(langs) == 2
    assert langs["de"]
    assert langs["fr"]
    assert langs["de"] > langs["fr"]
