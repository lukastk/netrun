"""Resource semaphore demonstration.

Three "GPU jobs" that could in principle run concurrently on a thread pool.
But the net declares only ONE `gpu` slot, and each job requires one. So
they end up running serially — the second job blocks until the first
finishes and releases the slot, and so on.

Compare timestamps in the output: the start of each job should follow the
end of the previous one, not be concurrent.
"""

import time


def gpu_job_1(trigger, ctx) -> str:
    started = time.time()
    ctx.print(f"gpu_job_1: started")
    time.sleep(0.2)
    ctx.print(f"gpu_job_1: finished after {time.time() - started:.2f}s")
    return "job_1 done"


def gpu_job_2(trigger, ctx) -> str:
    started = time.time()
    ctx.print(f"gpu_job_2: started")
    time.sleep(0.2)
    ctx.print(f"gpu_job_2: finished after {time.time() - started:.2f}s")
    return "job_2 done"


def gpu_job_3(trigger, ctx) -> str:
    started = time.time()
    ctx.print(f"gpu_job_3: started")
    time.sleep(0.2)
    ctx.print(f"gpu_job_3: finished after {time.time() - started:.2f}s")
    return "job_3 done"
