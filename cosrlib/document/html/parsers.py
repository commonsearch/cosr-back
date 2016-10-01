from __future__ import absolute_import, division, print_function, unicode_literals

import gumbocy
from . import defs

GUMBOCY_PARSER = gumbocy.HTMLParser(defs.GUMBOCY_OPTIONS)
GUMBOCY_PARSER_HEAD = gumbocy.HTMLParser(defs.GUMBOCY_OPTIONS_HEAD)
