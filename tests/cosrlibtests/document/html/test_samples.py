import pytest
import os
import json
from cosrlib.document.html import HTMLDocument
from tests.testdata.html_page_samples import SAMPLES

NEWSPAPER_DIR = "tests/testdata/html_newspaper_testcases"
NEWSPAPER_TESTCASES = os.listdir("%s/html/" % NEWSPAPER_DIR)

MOZILLA_READABILITY_DIR = "tests/testdata/html_mozilla_readability_testcases"
MOZILLA_READABILITY_TESTCASES = os.listdir(MOZILLA_READABILITY_DIR)


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

        # Uncomment this to debug
        if metadata.get("debug"):
            print words

        for word in metadata.get("assert_words_missing", []):
            assert word not in words

        for word in metadata.get("assert_words", []):
            assert word in words


@pytest.mark.parametrize("test_name", NEWSPAPER_TESTCASES)
def _test_newspaper_testcases(test_name):
    with open("%s/html/%s" % (NEWSPAPER_DIR, test_name)) as fhtml:
        with open("%s/text/%s" % (NEWSPAPER_DIR, test_name.replace(".html", ".txt"))) as ftxt:
            html = fhtml.read()
            txt = ftxt.read()
            word_groups = txt.split("\n\n")

            doc = HTMLDocument(html).parse()

            doc_word_groups = []
            for wg in doc.analysis["word_groups"]:
                doc_word_groups.append(wg[0])

            assert doc_word_groups == word_groups


# TODO: also use these in formatting.py for the titles in the metadata files
@pytest.mark.parametrize("test_name", MOZILLA_READABILITY_TESTCASES)
def _test_mozilla_readability_testcases(test_name):
    with open("%s/%s/source.html" % (MOZILLA_READABILITY_DIR, test_name)) as fhtml:
        with open("%s/%s/expected.html" % (MOZILLA_READABILITY_DIR, test_name)) as fsimple:
            html = fhtml.read()
            simple = fsimple.read()

            doc = HTMLDocument(html).parse()
            doc_simple = HTMLDocument(simple).parse()

            doc_word_groups = []
            for wg in doc.analysis["word_groups"]:
                doc_word_groups.append(wg[0])

            doc_simple_word_groups = []
            for wg in doc_simple.analysis["word_groups"]:
                doc_simple_word_groups.append(wg[0])

            assert doc_word_groups == doc_simple_word_groups
