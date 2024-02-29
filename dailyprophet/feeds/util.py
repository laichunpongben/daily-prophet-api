import random
from typing import List


def expo_decay_weighted_sample(data: List, k: int, decaying_factor: float = 0.98):
    weights = [decaying_factor**i for i in range(len(data))]
    return random.choices(data, weights=weights, k=k)
