# readers/reader_manager.py

from typing import List
import os

import pandas as pd

from dailyprophet.readers.reader import Reader


class ReaderManager:
    DEFAULT_USER = "PUBLIC"

    def __init__(self):
        self.csv_file_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "../data/readers.csv",
        )
        self._readers = {}
        self.load()

    def load(self):
        reader_names = self.load_reader_names()
        for reader_name in reader_names:
            if reader_name not in self._readers:
                self._readers[reader_name] = Reader(reader_name)

    def load_reader_names(self) -> List[str]:
        df = pd.read_csv(self.csv_file_path)
        return df.name.tolist()

    def __getitem__(self, id):
        if id is None:
            return self.get_default()
        elif id in self._readers:
            return self._readers[id]
        else:
            reader = self.create_reader(id)
            self._readers[id] = reader
            return reader

    def get_default(self):
        return self._readers[ReaderManager.DEFAULT_USER]

    def create_reader(self, id: str):
        reader_names = self.load_reader_names()
        if id not in reader_names:
            reader_names.append(id)
            df = pd.DataFrame(reader_names, columns=["name"])
            df.to_csv(self.csv_file_path, index=False)
        return Reader(id)

if __name__ == "__main__":
    reader_manager = ReaderManager()
    print(reader_manager._readers)

    r1 = reader_manager["asdf"]
    print(reader_manager._readers)

    r2 = reader_manager["qwer"]
    print(reader_manager._readers)
