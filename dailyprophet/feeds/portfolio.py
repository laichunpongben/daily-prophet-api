# portfolio.py

import os
import json

from typing import List
import logging

logger = logging.getLogger(__name__)


class FeedPortfolio:
    DEFAULT_SETTING = [
        ["reddit", "programming", 0.4],
        ["youtube", "UCqECaJ8Gagnn7YCbPEzWH6g", 0.4],
        ["openweathermap", "Hong Kong", 0.2],
    ]

    def __init__(self, name: str) -> None:
        self.name = name
        self._portfolio = {}
        try:
            self.load_setting_from_file(verison=1)  # eager
        except FileNotFoundError:
            self.save_setting_to_file(version=0)
            self.save_setting_to_file(version=1)
            self.load_setting_from_file(verison=1)

    def format_setting_file_path(self, version: int = 0):
        assert version is not None
        return os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            f"../data/portfolio_{self.name}_{version}.json",
        )

    def add(self, feed_type: str, name: str, weight: float):
        key = f"{feed_type}/{name}"
        self._portfolio[key] = float(weight)

    def load_setting_from_file(self, verison: int = 1):
        self._portfolio = {}
        with open(self.format_setting_file_path(version=verison), "r") as f:
            setting = json.load(f)
            for feed_type, name, weight in setting:
                self.add(feed_type, name, weight)

    def load_setting(self, setting: List):
        self._portfolio = {}
        for feed_type, name, weight in setting:
            self.add(feed_type, name, weight)

    def get_setting(self):
        setting = []
        for key, weight in self._portfolio.items():
            feed_type, name = key.split("/")
            line = [feed_type, name, weight]
            setting.append(line)
        return setting

    def save_setting_to_file(self, version: int = 1):
        setting = self.get_setting()
        if len(setting) == 0:
            setting = FeedPortfolio.DEFAULT_SETTING
        with open(self.format_setting_file_path(version=version), "w") as f:
            json.dump(setting, f)

    def generate_key_weight(self):
        for key, weight in self._portfolio.items():
            yield key, weight
