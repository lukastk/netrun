def _is_prime(n: int) -> bool:
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

def find_primes(print, start: int, stop: int) -> list[int]:
    """Find prime numbers starting from a given number."""
    print(f"Finding all primes between {start} and {stop}")

    primes = []
    current = start
    checked = 0

    while current < stop:
        if _is_prime(current):
            primes.append(current)
        current += 1
        checked += 1
        if checked % 10000 == 0:
            print(f"Checked {checked} numbers, found {len(primes)} primes")

    print(f"Found {len(primes)} primes: {primes[0]}...{primes[-1]}")
    return primes

def aggregate_primes(print, primes: list[int]) -> int:
    pass