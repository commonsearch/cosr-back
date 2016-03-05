import os

import rocksdb
from cosrlib.config import config


class Storage(object):
    """ Key-value storage class using a RocksDB database on disk as a source. """

    _rocksdb_options_readonly = {
        "db_log_dir": "/dev/null",  # TODO: Is this the right way of disabling logs?
        "max_open_files": 100,
        "max_background_compactions": 0,
        "max_background_flushes": 0,
        "keep_log_file_num": 0,
        "disable_auto_compactions": True,
        "advise_random_on_open": True
    }

    _db_dir = os.path.join(config["PATH_LOCALDATA"], "urlserver-rocksdb")

    def __init__(self, read_only=True):
        self.read_only = read_only
        self.db = None

        if self.read_only:
            if os.path.isdir(self._db_dir):
                self.db = rocksdb.DB(
                    self._db_dir,
                    rocksdb.Options(**self._rocksdb_options_readonly),
                    read_only=True
                )
            else:
                print "WARNING: RocksDB data not found (%s). Run make import_local_data" % self._db_dir
        else:
            self.db = rocksdb.DB(self._db_dir, rocksdb.Options(create_if_missing=True), read_only=False)

    def get(self, key):
        """ Returns the value of a key """
        return self.db.get(key)

    def close(self):
        """ Closes the connection to the DB """
        if self.db is not None:
            del self.db

    def write_batch(self, batch):
        """ Write a batch of data to the DB and return a new empty one """
        if batch is not None:
            self.db.write(batch, sync=True)
            self.db.compact_range()
        return rocksdb.WriteBatch()
