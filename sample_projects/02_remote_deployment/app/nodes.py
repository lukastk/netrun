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
    """Find prime numbers in the range [start, stop)."""
    print(f"Finding primes in [{start}, {stop})")

    primes = [n for n in range(start, stop) if _is_prime(n)]

    print(f"Found {len(primes)} primes")
    return primes
