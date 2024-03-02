# readers/reader_manager.py

from typing import List
import os
import csv

from dailyprophet.readers.reader import Reader


class ReaderManager:
    DEFAULT_USER = "PUBLIC"

    def __init__(self):
        self.csv_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "../data/readers.csv",
        )
        self._readers = {}
        self.load()

    def load(self):
        reader_names = self.load_reader_names()
        for reader_name in reader_names:
            self._readers[reader_name] = Reader(reader_name)

    def load_reader_names(self) -> List[str]:
        reader_names = []
        with open(self.csv_path, "r", newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                reader_names.append(row["name"])
        return reader_names

    def __getitem__(self, id):
        if id is None:
            return self.get_default()
        elif id in self._readers:
            return self._readers[id]
        else:
            return self.create_reader(id)

    def get_default(self):
        return self._readers[ReaderManager.DEFAULT_USER]

    def create_reader(self, id: str):
        with open(self.csv_file_path, "a", newline="") as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow([id])
        new_reader = Reader(id)
        self._readers[id] = new_reader
        return new_reader


if __name__ == "__main__":
    reader_manager = ReaderManager()
    print(reader_manager._readers)
