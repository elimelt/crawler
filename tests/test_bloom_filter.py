import math
import random
from typing import List

import numpy as np
import pytest

from crawlerlib import bloom_filter as bf_module


def test_calculate_optimal_params_valid():
    m, k = bf_module.calculate_optimal_params(1000, 0.01)
    assert m > 1000
    assert 1 <= k < 32


@pytest.mark.parametrize("n,p", [(0, 0.01), (-1, 0.01), (100, 0), (100, 1), (100, 1.0)])
def test_calculate_optimal_params_invalid(n: int, p: float):
    with pytest.raises(ValueError):
        bf_module.calculate_optimal_params(n, p)


@pytest.mark.parametrize("use_packed", [True, False])
def test_add_and_contains_single(monkeypatch: pytest.MonkeyPatch, use_packed: bool):
    monkeypatch.setattr(bf_module, "USE_PACKED_BITS", use_packed, raising=False)
    # Very low FPR to avoid random flakiness on negative checks
    bf = bf_module.BloomFilter(expected_items=1000, false_positive_rate=1e-6)
    items = [f"url://example/{i}" for i in range(50)]
    not_added = [f"url://other/{i}" for i in range(10)]

    for x in items:
        bf.add(x)

    # No false negatives
    for x in items:
        assert bf.contains(x)
        assert x in bf

    # Extremely unlikely to be true due to very low FPR
    negatives_true = sum(bf.contains(x) for x in not_added)
    assert negatives_true == 0


@pytest.mark.parametrize("use_packed", [True, False])
def test_add_batch_and_contains_batch(monkeypatch: pytest.MonkeyPatch, use_packed: bool):
    monkeypatch.setattr(bf_module, "USE_PACKED_BITS", use_packed, raising=False)
    # FPR ~1e-3 for 1000 items keeps memory small and test robust
    bf = bf_module.BloomFilter(expected_items=1000, false_positive_rate=1e-3)

    added: List[str] = [f"item:{i}" for i in range(400)]
    other: List[str] = [f"other:{i}" for i in range(400)]

    bf.add_batch(added)
    res_added = bf.contains_batch(added)
    res_other = bf.contains_batch(other)

    # No false negatives for inserted items
    assert res_added.dtype == np.bool_
    assert res_added.all()

    # False positives possible; keep a generous upper bound to avoid flakiness
    # With p=1e-3 and 400 trials, we expect ~0.4 positives; allow up to 10
    false_positives = int(res_other.sum())
    assert false_positives <= 10


@pytest.mark.parametrize("use_packed", [True, False])
def test_get_stats_and_memory(monkeypatch: pytest.MonkeyPatch, use_packed: bool):
    monkeypatch.setattr(bf_module, "USE_PACKED_BITS", use_packed, raising=False)
    bf = bf_module.BloomFilter(expected_items=1000, false_positive_rate=1e-3)

    # Add a handful to set some bits
    bf.add_batch([f"x{i}" for i in range(50)])
    stats = bf.get_stats()

    for key in [
        "items_added",
        "size_bits",
        "num_hashes",
        "memory_mb",
        "fill_ratio",
        "estimated_fpr",
        "set_bits",
    ]:
        assert key in stats

    assert stats["items_added"] == 50
    assert stats["size_bits"] == bf.m
    assert stats["num_hashes"] == bf.k
    assert 0.0 <= stats["fill_ratio"] <= 1.0
    assert stats["set_bits"] >= 1

    # Memory calculation should match implementation
    if use_packed:
        expected_mb = bf.bit_array.nbytes / (1024 * 1024)
    else:
        expected_mb = bf.t.nbytes / (1024 * 1024)
    assert math.isclose(stats["memory_mb"], expected_mb, rel_tol=1e-9, abs_tol=1e-12)


def test_len_and_contains_dunder(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(bf_module, "USE_PACKED_BITS", True, raising=False)
    bf = bf_module.BloomFilter(expected_items=256, false_positive_rate=1e-4)

    assert len(bf) == 0
    bf.add("hello")
    bf.add("world")
    assert len(bf) == 2
    assert "hello" in bf
    assert "world" in bf


def test_create_bloom_filter_uses_params(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(bf_module, "USE_PACKED_BITS", True, raising=False)
    n = 1234
    p = 0.02
    exp_m, exp_k = bf_module.calculate_optimal_params(n, p)
    bf = bf_module.create_bloom_filter(expected_urls=n, false_positive_rate=p)
    assert bf.m == exp_m
    assert bf.k == exp_k


