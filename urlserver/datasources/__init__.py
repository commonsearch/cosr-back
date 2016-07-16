import csv
import zipfile
import StringIO
import urllib2
import os
import importlib
import random
import time
from xml.etree import cElementTree as ElementTree

import ujson as json
from gzipstream import GzipStreamFile

from cosrlib.config import config
from cosrlib.url import URL
from urlserver.storage import Storage
from urlserver.protos import urlserver_pb2
from urlserver.id_generator import make_url_id


def list_datasources():
    """ Returns a dict of all available datasources """
    datasources = {}

    datasource_dir = os.path.join(os.path.dirname(__file__))
    for datasource in os.listdir(datasource_dir):
        if not datasource.startswith("_") and datasource.endswith(".py"):
            ds_name = datasource.replace(".py", "")
            datasources[ds_name] = load_datasource(ds_name)

    return datasources


def load_datasource(name):
    return importlib.import_module(".%s" % name, package="urlserver.datasources").DataSource(name)


class BaseDataSource(object):
    """ Base DataSource class """

    dump_testdata = None
    dump_url = None
    dump_compression = None
    dump_compression_params = None
    dump_format = None
    dump_batch_size = None
    dump_count_estimate = None

    def __init__(self, name):
        self.name = name
        self.xml_root = None

    def import_row(self, i, row):
        """ Maps a raw data row into a list of (key, values) pairs """
        return []

    def import_dump(self):
        """ Read a dump from an URL or a local file, and merge its data in RocksDB """

        db = Storage(read_only=False)

        write_batch = db.write_batch(None)
        batch_time = time.time()

        done = 0

        for url, values in self.iter_rows():

            # TODO: RocksDB merge operator?
            existing_value = db.get(url)
            existing_pb = urlserver_pb2.UrlMetadata()
            if existing_value is not None:
                existing_pb.ParseFromString(existing_value)
            else:
                # In order to send the protobuf message untouched via RPC, we pre-compute the ID
                existing_pb.id = make_url_id(URL(url))

            for k, v in values.iteritems():
                if k in ("ut1_blacklist", ):
                    for elt in v:
                        existing_pb.ut1_blacklist.append(elt)  # pylint: disable=no-member
                else:
                    setattr(existing_pb, k, v)

            # print "IMPORT", key, existing_pb

            write_batch.put(url, existing_pb.SerializeToString())

            done += 1

            if self.dump_batch_size and (done % self.dump_batch_size) == 0:

                eta = 0
                if self.dump_count_estimate:
                    eta = float(
                        self.dump_count_estimate - done
                    ) / (
                        3600.0 * done / (time.time() - batch_time)
                    )

                print "Done %s (%s/s, ~%0.2f%%, ETA %0.2fh)" % (
                    done,
                    int(done / (time.time() - batch_time)),
                    (float(done * 100) / self.dump_count_estimate) if self.dump_count_estimate else 0,
                    eta
                )
                write_batch = db.write_batch(write_batch)
                batch_time = time.time()

        print "Total rows: %s" % done
        db.write_batch(write_batch)
        db.close()

    def iter_dump(self):
        """ Iterates over the lines of the dump """

        f = self.open_dump()

        if self.dump_format == "csv":
            reader = csv.reader(f)

        elif self.dump_format == "tsv":
            reader = csv.reader(f, delimiter="\t")

        elif self.dump_format == "xml":
            reader = ElementTree.iterparse(f, events=("start", "end"))
            _, self.xml_root = reader.next()

        elif self.dump_format == "json-lines":
            def _reader():
                for line in f:
                    if not line.startswith("{"):
                        continue
                    yield json.loads(line.strip(",\n"))  # pylint: disable=no-member
            reader = _reader()

        return reader

    def iter_rows(self):
        """ Iterates over the formatted rows of the dump """

        for i, row in enumerate(self.iter_dump()):
            for key, values in self.import_row(i, row):
                yield key, values

    def open_dump(self):
        """ Returns a file-like object for the dump """

        if config["TESTDATA"] == "1":
            return open(self.dump_testdata, "rb")
        else:
            f = urllib2.urlopen(self.dump_url)

            if self.dump_compression == "zip":

                file_name = self.dump_compression_params[0]

                # TODO: is there a more efficient way of doing this? the file object passed to ZipFile
                # need to support .seek()
                zfile = zipfile.ZipFile(StringIO.StringIO(f.read()))
                return StringIO.StringIO(zfile.read(file_name))

            elif self.dump_compression == "gz":

                f.__dict__["closed"] = False  # Hack for GzipStreamFile
                return GzipStreamFile(f)

            else:
                return f

    def clear_xml_elements(self, *elements):
        """ Clear some XML elements during our iterative parsing, with all their references.

            See http://effbot.org/zone/element-iterparse.htm
        """

        for elem in elements:
            if elem is None:
                continue
            elem.clear()

            if random.randint(1, 1000) == 1:
                self.xml_root.clear()
