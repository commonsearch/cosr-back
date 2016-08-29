from __future__ import absolute_import, division, print_function, unicode_literals

try:
    import re2 as re
except ImportError:
    import re

import sys


if sys.version_info[0] < 3:
    py2_xrange = xrange  # pylint: disable=xrange-builtin
    py2_long = long  # pylint: disable=long-builtin
    py2_unicode = unicode  # pylint: disable=unicode-builtin

    def is_basestring(x):
        return isinstance(x, basestring)  # pylint: disable=basestring-builtin

else:
    py2_xrange = range  # pylint: disable=redefined-variable-type
    py2_long = int
    py2_unicode = str

    def is_basestring(x):
        return isinstance(x, (str, bytes))
