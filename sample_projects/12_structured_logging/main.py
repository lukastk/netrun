"""Demonstrates netrun's structured logging / wide events.

This example showcases:
1. ctx.log() — structured key=value logging inside node functions
2. In-memory retention — accessing EpochLogs and SimActionLogs after execution
3. Epoch log stdout echo — real-time epoch summaries printed to terminal
4. JSONL backend — persisting logs to JSONL files
5. SQLite backend — persisting logs to a SQLite database
6. NodeInfo.epoch_logs — per-node log access

Pipeline:  fetch_data -> process -> format_report -> [output queue]
"""

import asyncio
from pathlib import Path

from netrun.core import Net, NetConfig
from netrun.logging._backends import JsonlEpochLogger, JsonlSimActionLogger, SqliteLogger


async def run_pipeline(net: Net, url: str):
    """Inject a URL and run the pipeline to completion."""
    net.inject_data("fetch_data", "url", [url])
    await net.run_until_blocked()


async def main():
    config_path = Path(__file__).parent / "main.netrun.json"

    # ==================================================================
    # 1. ctx.log() AND EPOCH LOG ECHO
    # ==================================================================
    print("=" * 60)
    print("1. ctx.log() WITH EPOCH LOG ECHO")
    print("=" * 60)
    print()
    print("Each node uses ctx.log() to emit structured fields.")
    print("epoch_log_echo_stdout=True prints a summary line per epoch.")
    print()

    config = NetConfig.from_file(config_path)
    config.epoch_log_echo_stdout = True
    config.retain_epoch_logs = True

    async with Net(config) as net:
        await run_pipeline(net, "https://example.com/data")
        print()

        # Show the merged user_fields from each epoch
        print("Epoch user_fields (merged from all ctx.log() calls):")
        for epoch_log in net.epoch_logs.values():
            print(f"  [{epoch_log.node_name}] {epoch_log.user_fields}")

    # ==================================================================
    # 2. IN-MEMORY RETENTION — EPOCH LOGS
    # ==================================================================
    print()
    print("=" * 60)
    print("2. IN-MEMORY EPOCH LOGS")
    print("=" * 60)
    print()
    print("With retain_epoch_logs=True, all EpochLogs are kept in memory.")
    print()

    config = NetConfig.from_file(config_path)
    config.retain_epoch_logs = True
    config.print_echo_stdout = False

    async with Net(config) as net:
        await run_pipeline(net, "https://example.com/alpha")
        await run_pipeline(net, "https://example.com/beta")

        print(f"Total epoch logs retained: {len(net.epoch_logs)}")
        print()
        for eid, el in net.epoch_logs.items():
            print(f"  {el.node_name:15s}  outcome={el.outcome:8s}  "
                  f"duration={el.duration_ms:.0f}ms  "
                  f"in_ports={el.in_salvo_ports}  "
                  f"log_entries={len(el.node_log_entries)}")

    # ==================================================================
    # 3. IN-MEMORY RETENTION — SIM ACTION LOGS
    # ==================================================================
    print()
    print("=" * 60)
    print("3. IN-MEMORY SIM ACTION LOGS")
    print("=" * 60)
    print()
    print("With retain_sim_action_logs=True, every netsim action is tracked.")
    print()

    config = NetConfig.from_file(config_path)
    config.retain_sim_action_logs = True
    config.print_echo_stdout = False

    async with Net(config) as net:
        await run_pipeline(net, "https://example.com/data")

        print(f"Total sim actions recorded: {len(net.sim_action_log)}")
        print()

        # Group by action_kind
        from collections import Counter
        kinds = Counter(a.action_kind for a in net.sim_action_log)
        print("Action kinds:")
        for kind, count in kinds.most_common():
            print(f"  {kind}: {count}")

        # Show event counts per action
        print()
        actions_with_events = [a for a in net.sim_action_log if a.events]
        print(f"Actions that produced events: {len(actions_with_events)}")
        for a in actions_with_events[:5]:
            event_kinds = [e.kind for e in a.events]
            print(f"  {a.action_kind} -> {event_kinds}")

    # ==================================================================
    # 4. NODE INFO EPOCH LOGS
    # ==================================================================
    print()
    print("=" * 60)
    print("4. NODE INFO EPOCH LOGS")
    print("=" * 60)
    print()
    print("Access epoch logs per-node via net.nodes[name].epoch_logs.")
    print()

    config = NetConfig.from_file(config_path)
    config.retain_epoch_logs = True
    config.print_echo_stdout = False

    async with Net(config) as net:
        await run_pipeline(net, "https://example.com/first")
        await run_pipeline(net, "https://example.com/second")

        for name in ["fetch_data", "process", "format_report"]:
            node_logs = net.nodes[name].epoch_logs
            print(f"  {name}: {len(node_logs)} epoch(s)")
            for el in node_logs:
                entries = "; ".join(
                    f"{e.message} ({', '.join(f'{k}={v}' for k, v in e.fields.items())})"
                    for e in el.node_log_entries
                )
                print(f"    -> {entries}")

    # ==================================================================
    # 5. JSONL BACKEND
    # ==================================================================
    print()
    print("=" * 60)
    print("5. JSONL BACKEND")
    print("=" * 60)
    print()
    print("Persist epoch logs and sim actions to JSONL files.")
    print()

    output_dir = Path(__file__).parent / "output"
    output_dir.mkdir(exist_ok=True)

    epoch_path = output_dir / "epochs.jsonl"
    actions_path = output_dir / "sim_actions.jsonl"

    epoch_logger = JsonlEpochLogger(epoch_path)
    action_logger = JsonlSimActionLogger(actions_path)

    config = NetConfig.from_file(config_path)
    config.print_echo_stdout = False

    async with Net(config) as net:
        net.on_epoch_end(epoch_logger)
        net.on_sim_actions(action_logger)

        await run_pipeline(net, "https://example.com/alpha")
        await run_pipeline(net, "https://example.com/beta")

    epoch_logger.close()
    action_logger.close()

    # Load back and inspect
    loaded_epochs = JsonlEpochLogger.load(epoch_path)
    loaded_actions = JsonlSimActionLogger.load(actions_path)

    print(f"Wrote {epoch_path.stat().st_size} bytes to {epoch_path.name}")
    print(f"Wrote {actions_path.stat().st_size} bytes to {actions_path.name}")
    print()
    print(f"Loaded {len(loaded_epochs)} epoch logs back from JSONL:")
    for el in loaded_epochs:
        print(f"  [{el.node_name}] outcome={el.outcome}  "
              f"duration={el.duration_ms:.0f}ms  "
              f"fields={el.user_fields}")
    print()
    print(f"Loaded {len(loaded_actions)} sim action logs back from JSONL")

    # ==================================================================
    # 6. SQLITE BACKEND
    # ==================================================================
    print()
    print("=" * 60)
    print("6. SQLITE BACKEND")
    print("=" * 60)
    print()
    print("Persist both log types to a single SQLite database.")
    print()

    db_path = output_dir / "run.db"
    logger = SqliteLogger(db_path)

    config = NetConfig.from_file(config_path)
    config.print_echo_stdout = False

    async with Net(config) as net:
        net.on_epoch_end(logger.epoch_handler)
        net.on_sim_actions(logger.sim_action_handler)

        await run_pipeline(net, "https://example.com/alpha")
        await run_pipeline(net, "https://example.com/beta")

    # Load back and inspect
    epoch_logs = logger.load_epoch_logs()
    sim_actions = logger.load_sim_action_logs()
    logger.close()

    print(f"SQLite database: {db_path.name} ({db_path.stat().st_size} bytes)")
    print()
    print(f"Epoch logs table: {len(epoch_logs)} rows")
    for el in epoch_logs:
        print(f"  [{el.node_name}] outcome={el.outcome}  "
              f"duration={el.duration_ms:.0f}ms  "
              f"fields={el.user_fields}")
    print()
    print(f"Sim action logs table: {len(sim_actions)} rows")

    # Show how you could query the SQLite directly
    import sqlite3
    conn = sqlite3.connect(str(db_path))
    cursor = conn.execute(
        "SELECT node_name, outcome, duration_ms FROM epoch_logs ORDER BY timestamp"
    )
    print()
    print("Direct SQL query (SELECT node_name, outcome, duration_ms):")
    for row in cursor.fetchall():
        print(f"  {row[0]:15s}  {row[1]:8s}  {row[2]:.0f}ms")
    conn.close()

    # ==================================================================
    # 7. run_step() / run_until_blocked() RETURN VALUES
    # ==================================================================
    print()
    print("=" * 60)
    print("7. run_step() RETURN VALUES")
    print("=" * 60)
    print()
    print("run_step() and run_until_blocked() return sim_actions and epoch_logs")
    print("directly, so you can process them inline without callbacks.")
    print()

    config = NetConfig.from_file(config_path)
    config.print_echo_stdout = False

    async with Net(config) as net:
        net.inject_data("fetch_data", "url", ["https://example.com/inline"])
        while True:
            made_progress, sim_actions, epoch_logs = await net.run_step()

            if epoch_logs:
                for el in epoch_logs:
                    print(f"  Epoch completed: [{el.node_name}] "
                          f"outcome={el.outcome}  "
                          f"sim_actions={len(el.sim_actions)}")

            if not made_progress:
                break

    print()
    print("=" * 60)
    print("Done! All structured logging features demonstrated.")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
