# readers/reader_manager.py

from typing import List
import logging

from dailyprophet.readers.reader import Reader
from dailyprophet.mongodb_service import MongoDBService

logger = logging.getLogger(__name__)


class ReaderManager:
    DEFAULT_USER = "PUBLIC"
    KEY_FIELD = "userId"

    def __init__(self):
        self._readers = {}
        self.db = MongoDBService()
        self.load()

    def load(self):
        key_field = ReaderManager.KEY_FIELD
        default_user = ReaderManager.DEFAULT_USER
        try:
            reader_records = self.db.read_all()
        except Exception as e:
            logger.error(e)
            logger.error("Failed to load DB reader records!!!")
            reader_records = [
                {
                    key_field: default_user,
                }
            ]

        for record in reader_records:
            reader_name = record[key_field]
            if reader_name not in self._readers:
                self._readers[reader_name] = Reader(reader_name, record=record)

    def __getitem__(self, id: str):
        if id is None:
            logger.debug("Null reader Id. Use default reader.")
            return self.get_default()
        elif id in self._readers:
            logger.debug(f"Existing reader {id}")
            return self._readers[id]
        else:
            logger.debug(f"Create new reader {id}")
            reader = self.create_reader(id)
            return reader

    def get_default(self):
        return self._readers[ReaderManager.DEFAULT_USER]

    def create_reader(self, id: str):
        key_field = ReaderManager.KEY_FIELD
        new_reader = Reader(id, record=None)
        self._readers[id] = new_reader  # in-memory

        portfolio = new_reader.portfolio.get_setting()
        record = {key_field: id, "portfolio": portfolio}

        self.db.save(id, record, key_field=key_field)  # persist
        return new_reader

    def serialize(self):
        reader_dict = {}
        for name, reader in self._readers.items():
            portfolio = reader.portfolio.get_setting()
            new_item = {"userId": name, "portfolio": portfolio}
            reader_dict[name] = new_item
        return reader_dict

    def sync(self):
        key_field = ReaderManager.KEY_FIELD

        reader_dict = self.serialize()
        for id, record in reader_dict.items():
            self.db.save(id, record, key_field=key_field)  # persist
        logger.info("Reader Manager sync done!")


if __name__ == "__main__":
    rm = ReaderManager()
    print(rm._readers)

    r1 = rm["TEST1"]
    r2 = rm["TEST2"]
    r3 = rm["TEST3"]
    r4 = rm["TEST4"]
    print(rm._readers)

    reader_dict = rm.serialize()
    print(reader_dict)
    rm.sync()
