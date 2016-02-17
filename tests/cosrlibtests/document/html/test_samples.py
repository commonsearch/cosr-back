import pytest
from cosrlib.document.html import HTMLDocument
from tests.testdata.html_page_samples import SAMPLES


@pytest.mark.parametrize(("sample_name", ), [[name] for name in SAMPLES.iterkeys()])
def test_parsing_samples(sample_name):
    metadata = SAMPLES[sample_name]

    sample_file = "tests/testdata/html_page_samples/%s" % sample_name
    with open(sample_file, "r") as f:
        html = f.read()

        page = HTMLDocument(html).parse()

        if "title" in metadata:
            assert metadata["title"] == page.get_title()

        if "summary" in metadata:
            assert metadata["summary"] == page.get_summary()

        # for k, g in sorted(page.get_word_groups().items()):
        #   print k, g

        words = page.get_all_words()
        lower_words_set = set([w.lower() for w in words])

        # Uncomment this to debug
        if metadata.get("debug"):
            print words

        for word in metadata.get("assert_words_missing", []):
            assert word not in lower_words_set

        for word in metadata.get("assert_words", []):
            assert word in lower_words_set
