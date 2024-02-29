import os
from random import choices
import concurrent.futures
import logging

import requests
import pandas as pd

from dailyprophet.feeds.feed import Feed
from dailyprophet.configs import LICHESS_API_TOKEN

logger = logging.getLogger(__name__)


class LichessPuzzleFeed(Feed):
    def __init__(self, minimum_rating: int = 2000):
        super().__init__()
        self.api_token = LICHESS_API_TOKEN
        self.csv_file_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "../data/lichess_db_puzzle_filtered.csv",
        )
        self.minimum_rating = int(minimum_rating)
        self.puzzle_ids = self.filter_puzzle_ids_from_csv()

    def filter_puzzle_ids_from_csv(self):
        df = pd.read_csv(self.csv_file_path)
        puzzle_ids = df.loc[df["Rating"] >= self.minimum_rating, "PuzzleId"].tolist()
        return puzzle_ids

    def parse(self, puzzle):
        id = puzzle["puzzle"]["id"]
        return {
            "type": "lichess",
            "id": id,
            "rating": puzzle["puzzle"]["rating"],
            "url": f"https://lichess.org/training/{id}",
        }

    def fetch(self, n: int):
        sampled_ids = choices(self.puzzle_ids, k=n)

        result = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_puzzle = {
                executor.submit(self.fetch_puzzle, puzzle_id): puzzle_id
                for puzzle_id in sampled_ids
            }
            for future in concurrent.futures.as_completed(future_to_puzzle):
                puzzle_id = future_to_puzzle[future]
                try:
                    puzzle_data = future.result()
                    parsed_puzzle = self.parse(puzzle_data)
                    result.append(parsed_puzzle)
                except Exception as e:
                    logger.error(f"Error fetching puzzle {puzzle_id}: {e}")

        return result

    def fetch_puzzle(self, puzzle_id):
        response = requests.get(
            f"https://lichess.org/api/puzzle/{puzzle_id}",
            headers={"Authorization": f"Bearer {self.api_token}"},
        )
        response.raise_for_status()
        return response.json()
