#!/usr/bin/env python3
import time

import rpyc
from rpyc.utils.helpers import BgServingThread


def on_server_event(kind, payload):
    """
    This function runs in the CLIENT process, but is invoked by the SERVER.
    """
    print(f"[client] server event: {kind} payload={payload}")
    return "ack-from-client"


if __name__ == "__main__":
    conn = rpyc.connect("localhost", 18861)

    # Critical: keeps the client serving incoming requests while it's busy
    # (so the server can invoke callbacks even during blocking client calls)
    bg = BgServingThread(conn)  # :contentReference[oaicite:1]{index=1}

    # Register callback with server
    ok = conn.root.register_callback(on_server_event)
    print("[client] register_callback:", ok)

    # Tell server to start emitting callbacks
    print("[client] start_events:", conn.root.start_events(0.5))

    # While events are coming in, we also do normal RPC calls to the server
    for i in range(5):
        reply = conn.root.echo(f"hello {i}")
        print("[client] echo reply:", reply)
        time.sleep(0.8)

    # Stop events and clean up
    print("[client] stop_events:", conn.root.stop_events())
    time.sleep(1.0)

    bg.stop()
    conn.close()
    print("[client] done")
