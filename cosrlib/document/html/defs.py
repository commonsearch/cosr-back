
# Which attributes we extract (with gumbocy) from the HTML.
# All the others are ignored
ATTRIBUTES_WHITELIST = frozenset([
    "id",
    "class",
    "role",
    "content",
    "name",
    "href",
    "alt",
    "src",
    "hidden",
    "aria-hidden",
    "style",
    "rel",
    "property"
])

# The meta names or properties we parse in the <head>
METAS_WHITELIST = frozenset([
    "description",
    "og:description",
    "og:title"
])


# https://en.wikipedia.org/wiki/Noindex
# These tags are completely ignored, and not indexed at all.
TAGS_NOINDEX = frozenset([
    "noindex",
    "button",
    "input",
    "script",
    "style",
    "iframe",
    "noscript",
    "svg",
    "canvas"
])

# CSS classes which result in non-indexed elements
CLASSES_NOINDEX = frozenset([

    # https://support.google.com/customsearch/answer/2364585?hl=en
    "nocontent",

    # https://en.wikipedia.org/wiki/Noindex
    "robots-nocontent"
])

# HTML Element IDs which result in non-indexed elements
IDS_NOINDEX = frozenset([
    "notice-cookie-block",
    "cookiebar",
    "cookies_consent"
])

# Tags that contain boilerplate like navigation or footer.
# Links will be analyzed/followed but content won't be indexed.
# TODO: copy some ideas from:
# https://github.com/buriy/python-readability/blob/master/readability/readability.py
TAGS_BOILERPLATE = frozenset([
    "nav",
    "header",
    "footer"
])

# HTML Element IDs that suggest boilerplate elements
IDS_BOILERPLATE = frozenset([
    "navigation",
    "nav",
    "footer",
    "header",
    "expanded-nav",
    "mobile-nav",
    "sign_in",
    "user-bar"
])

# HTML Element classes that suggest boilerplate elements
CLASSES_BOILERPLATE = frozenset([
    "twitter-follow-button",
    "sharetools",
    "login"
])

# Values for the "role" attribute that suggest boilerplate elements
# http://www.w3.org/1999/xhtml/vocab
ROLES_BOILERPLATE = frozenset([
    "navigation",
    "search",
    "button"
])

# Tags that we still want to index even if they're in a boilerplate container
# Common case is a title in the header
TAGS_BOILERPLATE_BYPASS = frozenset([
    "h1", "h2", "h3", "h4", "h5", "h6"
])


# Classes that usually result in hidden elements that won't be indexed.
# Links inside will still be followed (it might be a dynamic menu)
CLASSES_HIDDEN = frozenset([

    # Common "hidden" css classes
    "visually-hidden",
    "visuallyhidden",
    "hidden",
    "invisible",
    "element-invisible",
    "element-hidden",
    "hide",

    # Cookies
    "cookie-message",
    "cookieNoticeTxt",
    "cookiebar",
    "g-cookiebar",
    "cookieContainer"
])

# IDs that usually result in hidden elements
IDS_HIDDEN = frozenset([
])


# Tags which can separate word groups on the page
TAGS_SEPARATORS = frozenset([
    "body",

    # http://www.w3.org/TR/html5/grouping-content.html#grouping-content
    "p", "pre", "blockquote", "ul", "ol", "li", "dl", "dt", "dd", "figure", "figcaption",

    "br", "img",

    "h1", "h2", "h3", "h4", "h5", "h6"
])

# Gumbocy options for regular parsing of the whole page
GUMBOCY_OPTIONS = {
    "tags_ignore": TAGS_NOINDEX,
    "classes_ignore": CLASSES_NOINDEX,
    "ids_ignore": IDS_NOINDEX,
    "classes_hidden": CLASSES_HIDDEN,
    "ids_hidden": IDS_HIDDEN,
    "attributes_whitelist": ATTRIBUTES_WHITELIST,
    "ids_boilerplate": IDS_BOILERPLATE,
    "tags_boilerplate": TAGS_BOILERPLATE,
    "tags_boilerplate_bypass": TAGS_BOILERPLATE_BYPASS,
    "classes_boilerplate": CLASSES_BOILERPLATE,
    "roles_boilerplate": ROLES_BOILERPLATE,
    "tags_separators": TAGS_SEPARATORS,
    "metas_whitelist": METAS_WHITELIST,
    "nesting_limit": 300
}


# Gumbocy options for quick parsing of the <head> (to look for meta tags for instance)
GUMBOCY_OPTIONS_HEAD = {
    "tags_ignore": TAGS_NOINDEX,
    "attributes_whitelist": frozenset([
        "http-equiv",
        "charset",
        "content"
    ]),
    "nesting_limit": 5,
    "head_only": True
}
