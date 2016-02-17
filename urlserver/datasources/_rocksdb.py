import os

import rocksdb
from cosrlib.config import config


rocksdb_options = {
    "db_log_dir": "/dev/null",  # TODO: Is this the right way of disabling logs?
    "max_open_files": 100,
    "max_background_compactions": 0,
    "max_background_flushes": 0,
    "keep_log_file_num": 0,
    "disable_auto_compactions": True,
    "advise_random_on_open": True
}


class RocksdbDataSource(object):
    """ Base DataSource class using a RocksDB database on disk as a source """

    db_path = None
    _db = None

    @property
    def db(self):
        if not self._db:

            db_dir = os.path.join(config["PATH_BACK"], self.db_path)
            if os.path.isdir(db_dir):
                self._db = rocksdb.DB(db_dir, rocksdb.Options(**rocksdb_options), read_only=True)
            else:
                print "WARNING: RocksDB data not found (%s). See scripts/import_*.py" % self.db_path

        return self._db
