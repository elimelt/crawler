import numpy as np
import mmh3
from typing import List, Optional
import math

TARGET_FPR = 0.001
MAX_EXPECTED_ITEMS = 10_000_000
BATCH_SIZE = 10_000
USE_PACKED_BITS = True  # Pack 8 bits per byte


def calculate_optimal_params(n: int, p: float) -> tuple[int, int]:
    """Calculate optimal m (bits) and k (hash functions)."""
    if n <= 0 or p <= 0 or p >= 1:
        raise ValueError("Invalid parameters")
    
    m = -n * math.log(p) / (math.log(2) ** 2)
    k = (m / n) * math.log(2)
    
    return int(math.ceil(m)), int(math.ceil(k))


class BloomFilter:
    def __init__(self, 
                 expected_items: Optional[int] = None,
                 false_positive_rate: Optional[float] = None,
                 m: Optional[int] = None, 
                 k: Optional[int] = None):
        if expected_items is not None and false_positive_rate is not None:
            self.m, self.k = calculate_optimal_params(expected_items, false_positive_rate)
        elif m is not None and k is not None:
            self.m = m
            self.k = k
        else:
            self.m, self.k = calculate_optimal_params(MAX_EXPECTED_ITEMS, TARGET_FPR)
        
        self.n_added = 0
        
        if USE_PACKED_BITS:
            self.bytes_needed = (self.m + 7) // 8
            self.bit_array = np.zeros(self.bytes_needed, dtype=np.uint8)
        else:
            self.t = np.zeros((self.k, self.m), dtype=np.bool_)
    
    def _hash_all(self, x: str) -> np.ndarray:
        """Generate k hashes using double hashing: h(i) = h1(x) + i*h2(x)"""
        h1 = mmh3.hash(x, 0, signed=False)
        h2 = mmh3.hash(x, h1, signed=False)
        
        hashes = np.zeros(self.k, dtype=np.uint32)
        for i in range(self.k):
            hashes[i] = (h1 + i * h2) % self.m
        
        return hashes
    
    def add(self, x: str) -> None:
        if USE_PACKED_BITS:
            positions = self._hash_all(x)
            for pos in positions:
                byte_idx = pos // 8
                bit_idx = pos % 8
                self.bit_array[byte_idx] |= (1 << bit_idx)
        else:
            for i in range(self.k):
                self.t[i, mmh3.hash(x, i, signed=False) % self.m] = True
        
        self.n_added += 1
    
    def add_batch(self, items: List[str]) -> None:
        if USE_PACKED_BITS:
            for item in items:
                positions = self._hash_all(item)
                byte_indices = positions // 8
                bit_indices = positions % 8
                masks = 1 << bit_indices
                
                for byte_idx, mask in zip(byte_indices, masks):
                    self.bit_array[byte_idx] |= mask
        else:
            for item in items:
                for i in range(self.k):
                    self.t[i, mmh3.hash(item, i, signed=False) % self.m] = True
        
        self.n_added += len(items)
    
    def contains(self, x: str) -> bool:
        if USE_PACKED_BITS:
            positions = self._hash_all(x)
            for pos in positions:
                byte_idx = pos // 8
                bit_idx = pos % 8
                if not (self.bit_array[byte_idx] & (1 << bit_idx)):
                    return False
            return True
        else:
            return all(self.t[i, mmh3.hash(x, i, signed=False) % self.m] 
                      for i in range(self.k))
    
    def contains_batch(self, items: List[str]) -> np.ndarray:
        results = np.ones(len(items), dtype=bool)
        
        if USE_PACKED_BITS:
            for idx, item in enumerate(items):
                positions = self._hash_all(item)
                for pos in positions:
                    byte_idx = pos // 8
                    bit_idx = pos % 8
                    if not (self.bit_array[byte_idx] & (1 << bit_idx)):
                        results[idx] = False
                        break
        else:
            for idx, item in enumerate(items):
                for i in range(self.k):
                    if not self.t[i, mmh3.hash(item, i, signed=False) % self.m]:
                        results[idx] = False
                        break
        
        return results
    
    def get_stats(self) -> dict:
        if USE_PACKED_BITS:
            set_bits = np.unpackbits(self.bit_array).sum()
            fill_ratio = set_bits / self.m if self.m > 0 else 0
        else:
            set_bits = self.t.sum()
            fill_ratio = set_bits / (self.k * self.m) if self.m > 0 else 0
        
        estimated_fpr = (fill_ratio ** self.k) if self.n_added > 0 else 0
        memory_mb = self.bit_array.nbytes / (1024 * 1024) if USE_PACKED_BITS else self.t.nbytes / (1024 * 1024)
        
        return {
            'items_added': self.n_added,
            'size_bits': self.m,
            'num_hashes': self.k,
            'memory_mb': memory_mb,
            'fill_ratio': fill_ratio,
            'estimated_fpr': estimated_fpr,
            'set_bits': int(set_bits)
        }
    
    def __contains__(self, x: str) -> bool:
        return self.contains(x)
    
    def __len__(self) -> int:
        return self.n_added


def create_bloom_filter(expected_urls: int = MAX_EXPECTED_ITEMS,
                       false_positive_rate: float = TARGET_FPR) -> BloomFilter:
    return BloomFilter(
        expected_items=expected_urls,
        false_positive_rate=false_positive_rate
    )