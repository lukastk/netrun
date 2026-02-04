"""Node functions demonstrating CPU-bound work for pool comparison.

This module contains functions that perform CPU-intensive calculations.
When run in a thread pool, they are limited by Python's GIL (Global Interpreter Lock).
When run in a multiprocess pool, they can utilize multiple CPU cores in parallel.
"""

import hashlib


def compute_hash(data: str, iterations: int, print) -> dict:
    """Compute a hash iteratively (CPU-bound work).

    This simulates CPU-intensive work by repeatedly hashing a value.
    """
    print(f"Starting hash computation with {iterations} iterations")

    result = data.encode()
    for i in range(iterations):
        result = hashlib.sha256(result).digest()
        if (i + 1) % (iterations // 4) == 0:
            print(f"Progress: {(i + 1) * 100 // iterations}%")

    hex_result = result.hex()[:16]
    print(f"Completed: {hex_result}...")

    return {"input": data, "iterations": iterations, "hash": hex_result}


def is_prime(n: int) -> bool:
    """Check if a number is prime."""
    if n < 2:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False
    for i in range(3, int(n**0.5) + 1, 2):
        if n % i == 0:
            return False
    return True


def find_primes(start: int, count: int, print) -> list[int]:
    """Find prime numbers starting from a given number (CPU-bound work).

    This demonstrates CPU-intensive work that benefits from parallel execution.
    """
    print(f"Finding {count} primes starting from {start}")

    primes = []
    current = start
    checked = 0

    while len(primes) < count:
        if is_prime(current):
            primes.append(current)
        current += 1
        checked += 1
        if checked % 10000 == 0:
            print(f"Checked {checked} numbers, found {len(primes)} primes")

    print(f"Found {len(primes)} primes: {primes[0]}...{primes[-1]}")
    return primes


def aggregate_results(results: list[dict], print) -> dict:
    """Aggregate results from multiple computations."""
    print(f"Aggregating {len(results)} results")

    summary = {
        "count": len(results),
        "inputs": [r.get("input") or r.get("start") for r in results],
    }

    print(f"Summary: {summary}")
    return summary
