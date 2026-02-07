"""Run the error handling examples.

Demonstrates:
1. Retries with retry_wait and on_node_failure callback
2. max_epochs=1 (node runs once, second trigger queues exception)
3. Epoch cancellation via ctx.cancel_epoch()
4. Runtime type checking (PacketTypeMismatch)
5. Non-propagating exceptions (propagate_exceptions=False)
6. Timeout enforcement (thread pool + timeout=0.5s)
7. Dead letter queue
"""

import asyncio
from pathlib import Path

from netrun.core import Net, NetConfig


async def run_net(net):
    """Run the net until no more progress can be made."""
    while True:
        await net.run_until_blocked()
        startable = net.get_startable_epochs()
        if not startable:
            break
        for epoch_id in startable:
            await net.execute_epoch(epoch_id)


async def main():
    config_path = Path(__file__).parent / "main.netrun.json"
    config = NetConfig.from_file(config_path)

    async with Net(config) as net:
        # --- Phase 1: Run all nodes ---
        # 1. Flaky node: fails twice, succeeds on third attempt (retries=3)
        net.inject_data("flaky", "data", ["hello"])

        # 2. Once-only node: first invocation succeeds
        net.inject_data("once_only", "data", ["first"])

        # 3. Cancelling node: "normal" produces output, "cancel" triggers ctx.cancel_epoch()
        net.inject_data("canceller", "data", ["normal", "cancel"])

        # 4. Type-checked node: returns str instead of int -> PacketTypeMismatch
        net.inject_data("type_checked", "value", [42])

        # 5. Quiet failure: exception is queued, not propagated
        net.inject_data("quiet", "data", ["test_data"])

        # 6. Slow node: times out after 0.5s (runs on thread pool)
        net.inject_data("slow", "data", ["will_timeout"])

        await run_net(net)

        # --- Phase 2: Second once_only invocation exceeds max_epochs ---
        net.inject_data("once_only", "data", ["second"])
        await run_net(net)

        # --- Report results ---
        print("=" * 60)

        # 1. Flaky node results
        results = net.flush_output_queue(node="flaky", port="out")
        print(f"1. Flaky node result: {results}")

        # 2. Once-only node
        once_results = net.flush_output_queue(node="once_only", port="out")
        print(f"2. Once-only node result: {once_results}")

        # 3. Canceller results (only "normal" produces output)
        cancel_results = net.flush_output_queue(node="canceller", port="out")
        print(f"3. Canceller results (cancel discarded): {cancel_results}")

        # 4. Type-checked node - failed due to type mismatch
        type_results = net.flush_output_queue(node="type_checked", port="out")
        print(f"4. Type-checked result (type mismatch): {type_results}")

        # 5. Quiet failure - no output
        quiet_results = net.flush_output_queue(node="quiet", port="out")
        print(f"5. Quiet failure result: {quiet_results}")

        # 6. Slow node - timed out
        slow_results = net.flush_output_queue(node="slow", port="out")
        print(f"6. Slow node result (timed out): {slow_results}")

        # Exception queue
        exceptions = net.exception_queue
        print(f"\nException queue ({len(exceptions)} queued):")
        for i, err in enumerate(exceptions):
            print(f"  [{i}] {type(err).__name__}: {err}")
            if err.__cause__:
                print(f"       cause: {type(err.__cause__).__name__}: {err.__cause__}")

        # Dead letter queue
        dlq = net.dead_letter_queue
        print(f"\nDead letter queue ({len(dlq)} entries):")
        for entry in dlq:
            print(f"  node={entry['node_name']}, error={type(entry['error']).__name__}: {entry['error']}")

        # Logs
        print("\nNode Logs:")
        net.print_all_logs()


if __name__ == "__main__":
    asyncio.run(main())
