import os
import sys
import time

sys.path.insert(-1, os.path.normpath(os.path.join(__file__, "../../")))
from urlserver.datasources import list_datasources

if len(sys.argv) > 1:
    to_import = sys.argv[1:]
else:
    to_import = ["alexa_top1m", "ut1_blacklist", "dmoz", "webdatacommons_hc", "wikidata"]

datasources = list_datasources()

for datasource in to_import:
    ds = datasources[datasource]

    start_time = time.time()
    print "Importing %s..." % datasource

    ds.import_dump()

    print "Import of %s finished. Took %ss" % (datasource, int(time.time() - start_time))
    print
