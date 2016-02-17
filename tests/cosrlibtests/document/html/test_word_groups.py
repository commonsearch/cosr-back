from cosrlib.document.html import HTMLDocument
import pytest


SAMPLES = [
    {
        "html": """ <p>hello</p> """,
        "groups": [
            ["p", "hello"]
        ]
    },

    # A <body> is automatically added
    {
        "html": """ nobody """,
        "groups": [
            ["body", "nobody"]
        ]
    },

    # span
    {
        "html": """ <p>pre <span>link</span> post</p> """,
        "groups": [
            ["p", "pre link post"]
        ]
    },

    # a
    {
        "html": """ <p>pre <a href="#">link</a> post</p> """,
        "groups": [
            ["p", "pre link post"]
        ]
    },

    # mid p
    {
        "html": """ <p>pre </p><ul><li>li1 x</li></ul> mid <p> post </p> """,
        "groups": [
            ["p", "pre"],
            ["li", "li1 x"],
            ["body", "mid"],
            ["p", "post"]
        ]
    },

    # Lists
    {
        "html": """ pre <ul><li>li1</li><li>li2</li></ul> post """,
        "groups": [
            ["body", "pre"],
            ["li", "li1"],
            ["li", "li2"],
            ["body", "post"]
        ]
    },

    # HR with illegal <p>. "post" is actually part of <body>.
    {
        "html": """ <p>pre <hr/> post</p>""",
        "groups": [
            ["p", "pre"],
            ["body", "post"]
        ]
    },

    # Non-closed p tag.
    {
        "html": """ pre <p> post""",
        "groups": [
            ["body", "pre"],
            ["p", "post"]
        ]
    },

    # BR
    {
        "html": """ <p>pre <br/> post </p>""",
        "groups": [
            ["p", "pre"],
            ["p", "post"]
        ]
    },

    # IMG filename + alt
    {
        "html": """ <p> pre <img src="/test/dir/maceo_parker.jpg" alt="james brown"> post </p>""",
        "groups": [
            ["p", "pre"],
            ["img", "james brown"],
            ["img", "maceo parker"],
            ["p", "post"]
        ]
    },
]


# TODO: good coverage of http://www.w3.org/html/wg/drafts/html/master/syntax.html
@pytest.mark.parametrize(("sample"), SAMPLES)
def test_get_word_groups(sample):

    page = HTMLDocument(sample["html"]).parse()

    word_groups = page.get_word_groups()

    for i, group in enumerate(word_groups):
        sample_group = sample["groups"][i]
        assert group["tag"] == sample_group[0]
        assert group["words"] == sample_group[1]

    assert len(word_groups) == len(sample["groups"])
