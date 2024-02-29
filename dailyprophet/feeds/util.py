import random
from typing import List


def expo_decay_weighted_sample(data: List, k: int, decaying_factor: float = 0.98):
    weights = [decaying_factor**i for i in range(len(data))]
    return random.choices(data, weights=weights, k=k)


def flatten_dict(d, parent_key="", sep="_"):
    items = []
    counter = {}

    if d is None:
        return {}

    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if new_key in counter:
            counter[new_key] += 1
            new_key = f"{new_key}{sep}{counter[new_key]}"
        else:
            counter[new_key] = 0

        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            for i, item in enumerate(v):
                items.extend(flatten_dict(item, f"{new_key}{sep}{i}", sep=sep).items())
        else:
            items.append((new_key, v))

    return dict(items)
