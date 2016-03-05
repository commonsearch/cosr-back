import os
import tempfile
from collections import defaultdict
import shutil

from . import BaseDataSource
from cosrlib.config import config
from cosrlib.url import URL


class DataSource(BaseDataSource):
    """ Return the UT1 categories in which the URL belongs.

        https://dsi.ut-capitole.fr/blacklists/index_en.php
    """

    dump_testdata = "tests/testdata/ut1_blacklists"
    dump_url = "ftp://ftp.ut-capitole.fr/pub/reseau/cache/squidguard_contrib/blacklists.tar.gz"
    dump_batch_size = None

    def iter_dump(self):
        if config["TESTDATA"] == "1":
            extract_dir = self.dump_testdata
            clean = False
        else:
            extract_dir = tempfile.mkdtemp(suffix="cosr-ut1-import")
            clean = True

            os.system("curl %s > %s/blacklists.tar.gz" % (self.dump_url, extract_dir))
            os.system("cd %s && tar zxf blacklists.tar.gz" % extract_dir)
            extract_dir += "/blacklists"

        data = defaultdict(list)

        for fp in os.listdir(extract_dir):
            fullpath = os.path.join(extract_dir, fp)

            if os.path.isdir(fullpath) and not os.path.islink(fullpath):

                cnt = 0

                with open(fullpath + "/domains", 'r') as f:
                    for line in f.readlines():
                        url = URL(line.strip()).normalized
                        if url:
                            data[url].append(fp)
                            cnt += 1

                if os.path.isfile(fullpath + "/urls"):
                    with open(fullpath + "/urls", 'r') as f:
                        for line in f.readlines():
                            url = URL(line.strip()).normalized
                            if url:
                                data[url].append(fp)
                                cnt += 1

                print "Done %s (%s entries)" % (fp, cnt)

        if clean:
            shutil.rmtree(os.path.dirname(extract_dir))

        return data.iteritems()

    def import_row(self, key, value):
        """ Maps a raw data row into a list of (key, values) pairs """
        return [(key, {"ut1_blacklist": value})]
