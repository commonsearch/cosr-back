from __future__ import absolute_import, division, print_function, unicode_literals

import os
import sys
import time

sys.path.insert(-1, os.path.normpath(os.path.join(__file__, "../../")))
from cosrlib.dataproviders import list_dataproviders

if len(sys.argv) > 1:
    to_import = sys.argv[1:]
else:
    to_import = [
        "alexa_top1m",
        "ut1_blacklist",
        "dmoz",
        "webdatacommons_hc",
        "commonsearch_host_pagerank",
        "wikidata"
    ]

dataproviders = list_dataproviders()

for dataprovider in to_import:
    ds = dataproviders[dataprovider]

    start_time = time.time()
    print("Importing %s..." % dataprovider)

    ds.import_dump()

    print("Import of %s finished. Took %ss" % (dataprovider, int(time.time() - start_time)))
    print()
