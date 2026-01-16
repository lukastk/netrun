#!/usr/bin/env python3
import threading
import time
import rpyc
from rpyc.utils.server import ThreadedServer


class CallbackServerService(rpyc.Service):
    """
    Server-side service that stores a client callback, and can call it later.
    """

    def on_connect(self, conn):
        # Per-connection state
        self._cb = None
        self._stop = threading.Event()
        self._event_thread = None
        print("[server] client connected")

    def on_disconnect(self, conn):
        print("[server] client disconnected")
        self._stop.set()
        if self._event_thread and self._event_thread.is_alive():
            self._event_thread.join(timeout=1)

    def exposed_register_callback(self, cb):
        """
        Client passes in a callback function/object.
        Server stores it and can invoke it later (server -> client).
        """
        self._cb = cb
        print("[server] callback registered:", cb)
        return True

    def exposed_start_events(self, interval_s=1.0):
        """
        Start a background thread that calls the callback periodically.
        """
        if self._cb is None:
            raise ValueError("No callback registered")

        if self._event_thread and self._event_thread.is_alive():
            return "already running"

        interval_s = float(interval_s)
        self._stop.clear()

        def loop():
            i = 0
            print("[server] event loop started")
            while not self._stop.is_set():
                i += 1
                payload = {"i": i, "ts": time.time()}

                try:
                    # This call crosses back to the client process.
                    # If the client is doing blocking calls, it must run BgServingThread.
                    ack = self._cb("tick", payload)
                    print("[server] callback ack:", ack)
                except Exception as e:
                    print("[server] callback failed:", repr(e))
                    # If callback fails (client died), stop emitting.
                    break

                time.sleep(interval_s)

            print("[server] event loop stopped")

        self._event_thread = threading.Thread(target=loop, daemon=True)
        self._event_thread.start()
        return "started"

    def exposed_stop_events(self):
        self._stop.set()
        return "stopping"

    def exposed_echo(self, msg):
        """
        Simple server method the client can call.
        """
        return f"echo: {msg}"


if __name__ == "__main__":
    # ThreadedServer: one thread per connection (works on Windows/macOS/Linux)
    server = ThreadedServer(
        CallbackServerService,
        port=18861,
        protocol_config={
            # These are common defaults; adjust for your security model.
            "allow_public_attrs": True,
            "allow_all_attrs": True,
        },
    )
    print("[server] listening on port 18861")
    server.start()
