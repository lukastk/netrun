from __future__ import annotations

import itertools
import multiprocessing as mp
import queue
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

# req:  (req_id, kind, payload)
# resp: (req_id, ok, payload_or_error_string)
# sig:  (kind, payload)

Signal = tuple[str, Any]


@dataclass
class WorkerRPC:
    """
    Two-way RPC bridge between worker <-> main, with concurrency support:

    Worker process:
      - call(kind, payload, timeout=None) -> response
      - emit(kind, payload) -> None
      - start_worker_side() / stop_worker_side() to manage background demux thread

    Main process:
      - serve(handler, stop_when=None, poll_interval=0.05) -> None
      - recv_signal(timeout=0.0) -> optional (kind, payload)

    This version supports multiple concurrent call() invocations from multiple threads
    *within the worker process* by demultiplexing responses.
    """
    _req_q: mp.Queue
    _resp_q: mp.Queue
    _sig_q: mp.Queue
    _id_prefix: str = "w"
    _counter_start: int = 0

    # -------------------------
    # Worker-side implementation
    # -------------------------

    def start_worker_side(self) -> None:
        """
        Call this inside the worker process before using call() concurrently.
        Starts a background thread that reads responses and routes them to waiters.
        """
        # These exist only in the worker process runtime (not pickled state).
        if getattr(self, "_demux_started", False):
            return

        self._demux_started = True
        self._pending: dict[str, queue.Queue[tuple[bool, Any]]] = {}
        self._pending_lock = threading.Lock()
        self._stop_evt = threading.Event()

        self._counter = itertools.count(self._counter_start)
        self._resp_thread = threading.Thread(target=self._resp_demux_loop, daemon=True)
        self._resp_thread.start()

    def stop_worker_side(self) -> None:
        """Stop background response demux thread (worker process only)."""
        if not getattr(self, "_demux_started", False):
            return
        self._stop_evt.set()
        # Nudge the demux thread by sending a special response (safe no-op)
        try:
            self._resp_q.put(("__wake__", True, None))
        except Exception:
            pass
        self._resp_thread.join(timeout=1)

        # Fail any still-pending callers
        with self._pending_lock:
            pending_items = list(self._pending.items())
            self._pending.clear()

        for _, waiter_q in pending_items:
            try:
                waiter_q.put((False, "WorkerRPC stopped while waiting"))
            except Exception:
                pass

        self._demux_started = False

    def _next_id(self) -> str:
        # Called in worker process only.
        return f"{self._id_prefix}-{next(self._counter)}"

    def emit(self, kind: str, payload: Any) -> None:
        """One-way signal to main (progress/log/telemetry)."""
        self._sig_q.put((kind, payload))

    def call(self, kind: str, payload: Any, timeout: float | None = None) -> Any:
        """
        Worker -> main request/response. Safe to call from multiple threads
        once start_worker_side() has been called.
        """
        if not getattr(self, "_demux_started", False):
            # Auto-start for convenience; still safe.
            self.start_worker_side()

        req_id = self._next_id()
        waiter_q: queue.Queue[tuple[bool, Any]] = queue.Queue(maxsize=1)

        with self._pending_lock:
            self._pending[req_id] = waiter_q

        # Send request to main
        self._req_q.put((req_id, kind, payload))

        # Wait for routed response
        try:
            ok, resp_payload = waiter_q.get(timeout=timeout)
        except queue.Empty as e:
            # Clean up pending map
            with self._pending_lock:
                self._pending.pop(req_id, None)
            raise TimeoutError(f"Timed out waiting for response to {kind!r}") from e

        if ok:
            return resp_payload
        raise RuntimeError(f"Main handler error for {kind!r}: {resp_payload!r}")

    def _resp_demux_loop(self) -> None:
        """
        Worker-side background thread: reads all responses and routes by req_id.
        """
        while not self._stop_evt.is_set():
            try:
                req_id, ok, payload = self._resp_q.get(timeout=0.1)
            except queue.Empty:
                continue

            if req_id == "__wake__":
                continue

            with self._pending_lock:
                waiter_q = self._pending.pop(req_id, None)

            if waiter_q is not None:
                try:
                    waiter_q.put((ok, payload))
                except Exception:
                    pass
            else:
                # Stray response: no waiter. Could log/ignore.
                pass

    def close(self, timeout: float = 2.0) -> None:
        """
        Convenience: ask main server to stop, if it is running.
        """
        try:
            self.call("__close__", None, timeout=timeout)
        except Exception:
            pass

    # -----------------------
    # Main-side implementation
    # -----------------------

    def serve(
        self,
        handler: Callable[[str, Any], Any],
        *,
        stop_when: Callable[[], bool] | None = None,
        poll_interval: float = 0.05,
    ) -> None:
        """
        Main loop: handles worker requests and replies.
        """
        while True:
            if stop_when and stop_when():
                return

            try:
                req_id, kind, payload = self._req_q.get(timeout=poll_interval)
            except queue.Empty:
                continue

            if kind == "__close__":
                self._resp_q.put((req_id, True, "closing"))
                return

            try:
                result = handler(kind, payload)
                self._resp_q.put((req_id, True, result))
            except Exception as e:
                # Keep error payload picklable
                self._resp_q.put((req_id, False, f"{type(e).__name__}: {e}"))

    def recv_signal(self, timeout: float = 0.0) -> Signal | None:
        """Main: receive one-way signals from worker."""
        try:
            return self._sig_q.get(timeout=timeout)
        except queue.Empty:
            return None


def make_worker_rpc(*, ctx: mp.context.BaseContext | None = None) -> WorkerRPC:
    ctx = ctx or mp.get_context()
    return WorkerRPC(
        _req_q=ctx.Queue(),
        _resp_q=ctx.Queue(),
        _sig_q=ctx.Queue(),
        _id_prefix="w",
    )


# ------------------ Demo ------------------

def worker_main(rpc: WorkerRPC) -> None:
    # Important: start demux so multiple threads can rpc.call() concurrently
    rpc.start_worker_side()
    rpc.emit("status", "worker started")

    def do_calls(thread_id: int) -> None:
        for x in range(3):
            # concurrent calls from multiple threads
            resp = rpc.call("compute", {"thread": thread_id, "x": x})
            rpc.emit("thread_done", {"thread": thread_id, "x": x, "resp": resp})

    threads = [threading.Thread(target=do_calls, args=(i,), daemon=True) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    rpc.emit("status", "worker finished")
    rpc.stop_worker_side()


if __name__ == "__main__":
    mp.set_start_method("spawn", force=True)

    rpc = make_worker_rpc()

    p = mp.Process(target=worker_main, args=(rpc,), daemon=True)
    p.start()

    # main handler
    def handler(kind: str, payload: Any) -> Any:
        if kind == "compute":
            # pretend main has access to some shared resource/db/config
            return {"y": payload["x"] * 10, "thread": payload["thread"]}
        raise ValueError(f"unknown kind {kind!r}")

    # Run server in a thread so main can also consume signals
    server = threading.Thread(
        target=rpc.serve,
        args=(handler,),
        kwargs={"stop_when": lambda: (p.exitcode is not None)},
        daemon=True,
    )
    server.start()

    # Print signals until worker exits
    while p.is_alive() or (rpc.recv_signal(timeout=0.05) is not None):
        sig = rpc.recv_signal(timeout=0.05)
        if sig:
            print("MAIN signal:", sig)

    p.join()
    server.join(timeout=1)
    print("worker exitcode:", p.exitcode)
