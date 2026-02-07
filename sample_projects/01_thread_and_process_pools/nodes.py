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

def find_primes(print, ctx, start: int, stop: int) -> list[int]:
    """Find prime numbers in a range.

    Uses ctx.vars to access node variables (label, verbose).
    """
    label = ctx.vars.get("label", "primes")
    verbose = ctx.vars.get("verbose", False)
    print(f"[{label}] Finding all primes between {start} and {stop}")

    primes = []
    current = start
    checked = 0

    while current < stop:
        if _is_prime(current):
            primes.append(current)
        current += 1
        checked += 1
        if verbose and checked % 10000 == 0:
            print(f"[{label}] Checked {checked} numbers, found {len(primes)} primes")

    print(f"[{label}] Found {len(primes)} primes: {primes[0]}...{primes[-1]}")
    return primes


def count_primes(print, start: int, stop: int) -> int:
    """Count primes in a range. Demonstrates max_epochs and print_echo_stdout."""
    print(f"Counting primes between {start} and {stop}")
    count = sum(1 for n in range(start, stop) if _is_prime(n))
    print(f"Found {count} primes")
    return count
