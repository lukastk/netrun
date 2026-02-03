# MultiprocessPool Code Review

**Date**: 2026-01-19
**Scope**: Review of `MultiprocessPool` for potential tech debt introduced while fixing exception propagation/hanging issues (commits since `ea2dca7d`)

## Summary

The exception propagation fix is working, but there are several areas of concern that could cause issues or represent tech debt.

---

## Issues

### 1. Race Condition in `_dead_processes` Check

**Location**: `02_multiprocess.pct.py` lines 756-761

```python
except RecvTimeout:
    if process_idx in self._dead_processes:
        break
    continue
```

**Issue**: There's a potential race condition between:
- The monitor task adding to `_dead_processes`
- The recv_loop checking `_dead_processes`
- The monitor putting `POOL_UP_ERROR_CRASHED` messages in the queue

If recv_loop breaks before the monitor has finished putting all the CRASHED messages in the queue, some messages could be lost or the order could be unexpected.

**Risk Level**: Low-Medium. The monitor also closes the channel which should trigger the recv_loop to exit anyway.

---

### 2. Duplicate Error Reporting Paths

**Location**: Lines 616-643 (monitor) vs 767-775 (recv_loop)

The same process crash can be reported through two different paths:

1. **Monitor task** (lines 616-643): Detects `proc.exitcode is not None` and puts `POOL_UP_ERROR_CRASHED` messages
2. **recv_loop** (lines 767-775): Handles `MP_UP_SUBPROCESS_ERROR` and puts `POOL_UP_ERROR_EXCEPTION` messages

**Issue**: If a subprocess both sends `MP_UP_SUBPROCESS_ERROR` AND then crashes, the parent could receive both:
- N `POOL_UP_ERROR_EXCEPTION` messages (from recv_loop handling MP_UP_SUBPROCESS_ERROR)
- N `POOL_UP_ERROR_CRASHED` messages (from monitor detecting exit code)

This is partially intentional (belt-and-suspenders), but the caller may see duplicate errors for the same failure.

**Risk Level**: Low. Duplicate errors are better than missing errors.

---

### 3. `response_forwarder` Thread Uses Infinite Timeout

**Location**: Lines 295-307

```python
def response_forwarder():
    while not shutdown:
        try:
            msg = response_queue.get(timeout=None)  # <-- INFINITE
```

**Issue**: The `timeout=None` means this thread will block forever on `response_queue.get()`. It relies on receiving a `None` sentinel to wake up. If workers die without the router sending `None`, this thread could hang forever (though threads are daemon threads, so they'd die with the process).

**Risk Level**: Low. Daemon threads will be killed when the process exits.

---

### 4. Shutdown Sequence Complexity

**Location**: Lines 646-727

The shutdown sequence is complex with multiple interacting components:
1. Start recv tasks BEFORE setting `_running = False`
2. Cancel monitor task
3. Send `MP_DOWN_SHUTDOWN` to each subprocess
4. Wait for `SHUTDOWN_COMPLETE` from all processes
5. Close channels
6. Cancel recv tasks
7. Join processes
8. Clean up state

**Issue**: The order matters greatly and there are timeout fallbacks (`shutdown_timeout = timeout if timeout is not None else 30.0`), but if anything goes wrong in the sequence, state could be left inconsistent.

**Tech Debt**: Consider breaking this into smaller named methods for each phase.

---

### 5. `_start_recv_tasks` Idempotency Check is Weak

**Location**: Lines 740-798

```python
def _start_recv_tasks(self) -> None:
    if self._recv_tasks:
        return
```

**Issue**: This check only prevents creating new tasks if the list is non-empty. But tasks can fail/be cancelled. If all tasks have crashed but the list is non-empty with completed/failed tasks, new tasks won't be created.

**Risk Level**: Medium. Could cause `recv()` to hang if all recv tasks have died.

---

### 6. `try_recv` Has Two Different Code Paths

**Location**: Lines 828-859

```python
async def try_recv(self) -> WorkerMessage | None:
    if self._recv_tasks:
        # Path 1: Read from queue
        ...
    # Path 2: Read directly from channels
    for process_idx, channel in enumerate(self._channels):
        result = await channel.try_recv()
```

**Issue**: Path 2 (reading directly from channels) doesn't handle `MP_UP_SUBPROCESS_ERROR`, `MP_UP_STDOUT_BUFFER`, or `MP_UP_SHUTDOWN_COMPLETE` - it only handles `MP_UP_RESPONSE`. If these messages are read via `try_recv` before recv_tasks are started, they'll be silently dropped.

**Risk Level**: Medium. Could lose error notifications if `try_recv` is called before `recv`.

---

### 7. `_OutputCapture.fileno()` Raises OSError

**Location**: Lines 186-188

```python
def fileno(self) -> int:
    raise OSError("_OutputCapture does not have a file descriptor")
```

**Issue**: Some libraries (e.g., subprocess, certain logging handlers) check `fileno()` to determine if output is a real file. Raising `OSError` is correct but could cause issues with libraries that don't expect this.

**Risk Level**: Low. Most code doesn't call `fileno()` directly.

---

### 8. Thread Worker Error Pickling Fallback

**Location**: Lines 468-481

```python
try:
    pickle.dumps(e)  # Test if pickleable
    response_queue.put((worker_id, POOL_UP_ERROR_EXCEPTION, e))
except Exception:
    # Fallback to dict with error info
    response_queue.put((worker_id, POOL_UP_ERROR_EXCEPTION, {...}))
```

**Issue**: This creates two different data formats for the same error key. The receiver (`WorkerException` in base.py lines 193-201) handles both, but it's a subtle contract that could be broken if someone assumes exceptions are always `Exception` objects.

**Risk Level**: Low. The receiver handles both cases.

---

### 9. No Cleanup of `_flush_events` on Error

**Location**: Lines 898-911

```python
self._flush_events[process_idx] = event
await self._channels[process_idx].send(MP_DOWN_FLUSH_STDOUT, None)
try:
    await asyncio.wait_for(event.wait(), timeout=timeout)
except TimeoutError:
    raise RecvTimeout(...)
finally:
    self._flush_events.pop(process_idx, None)
```

**Issue**: If `send()` raises before we enter the `try` block, the event stays in `_flush_events`. On next `flush_stdout` call for the same process, a stale event could be overwritten without issue, but there's cleanup inconsistency.

**Risk Level**: Very Low. The event gets overwritten on next call.

---

### 10. `shutdown_event` is Not Used Consistently

**Location**: Lines 293, 322-328, 374-375

In `_subprocess_main_inner`:
```python
shutdown_event = threading.Event()

def output_flusher():
    while not shutdown:
        shutdown_event.wait(timeout=output_flush_interval)  # Uses event
        if shutdown:
            break
```

But `response_forwarder`:
```python
def response_forwarder():
    while not shutdown:  # Just checks boolean
        msg = response_queue.get(timeout=None)  # No event
```

**Issue**: `response_forwarder` doesn't use `shutdown_event`, so it can't be woken up early. It relies on receiving a `None` sentinel from the queue. This is fine but inconsistent.

**Risk Level**: Very Low. Current design works, just inconsistent patterns.

---

## Summary of Recommendations

| Priority | Issue | Recommendation |
|----------|-------|----------------|
| **High** | `try_recv` doesn't handle all message types | Add handling for `MP_UP_SUBPROCESS_ERROR` etc. in the direct-read path, or always ensure recv_tasks are started first |
| **Medium** | `_start_recv_tasks` doesn't check task health | Check if existing tasks are still alive before returning early |
| **Medium** | Duplicate error reporting | Document this behavior or consolidate to single path |
| **Low** | Shutdown complexity | Consider refactoring into smaller methods |
| **Low** | Inconsistent shutdown patterns in subprocess | Minor consistency fix |

---

## Conclusion

The code is fundamentally sound and the exception propagation fix is working correctly. The main areas of concern are:

1. The `try_recv` message handling gap (could silently drop error messages)
2. The weak task health check in `_start_recv_tasks` (could cause hangs)

These should be addressed before the code is considered production-ready.
