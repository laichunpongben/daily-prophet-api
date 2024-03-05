# portfolio.py

import os
import json

from typing import List, Optional
import logging

logger = logging.getLogger(__name__)


class FeedPortfolio:
    DEFAULT_SETTING = [
        ["reddit", "programming", 0.4],
        ["youtube", "UCqECaJ8Gagnn7YCbPEzWH6g", 0.4],
        ["openweathermap", "Hong Kong", 0.2],
    ]
    DEFAULT_USER = "PUBLIC"

    def __init__(self, name: str, setting: Optional[List] = None) -> None:
        self.name = name
        self._portfolio = {}

        if setting is not None:
            logger.debug(f"Portfolio setting provided by {name}.")
            self.add_setting(setting)
        else:
            logger.warning(
                f"No portfolio setting provided by {name}. Loading from default."
            )
            self.load_default()

    def _format_setting_file_path(self, name, version: int = 0):
        assert version is not None
        assert name is not None
        return os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            f"../data/portfolio_{name}_{version}.json",
        )

    def _load_setting_from_file(self, name, verison: int = 1):
        self._portfolio = {}
        with open(self._format_setting_file_path(name, version=verison), "r") as f:
            setting = json.load(f)
            self.add_setting(setting)

    def load_default(self):
        self._load_setting_from_file(FeedPortfolio.DEFAULT_USER, verison=0)

    def add_setting(self, setting: List):
        for feed_type, name, weight in setting:
            self.add(feed_type, name, weight)

    def add(self, feed_type: str, name: str, weight: float):
        key = f"{feed_type}/{name}"
        self._portfolio[key] = float(weight)

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

    def generate_key_weight(self):
        for key, weight in self._portfolio.items():
            yield key, weight
