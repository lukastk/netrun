"""Node functions for the controls and signals example.

Demonstrates:
- A worker node whose epoch_finished signal triggers a monitor
- A generator node triggered on demand via start_epoch control
- A processor node that can be enabled/disabled at runtime
"""


def generate(print) -> str:
    """Source node that produces a value each time it is triggered.

    Has no regular input ports — triggered via `start_epoch` control.
    Tracks how many times it has been called.
    """
    if not hasattr(generate, "_count"):
        generate._count = 0
    generate._count += 1
    count = generate._count
    print(f"Generator call #{count}")
    return f"item_{count}"


def process(data: str, print) -> str:
    """Worker node that transforms data.

    Configured with epoch_finished signal and enable/disable controls.
    """
    result = data.upper()
    print(f"Processing: {data} -> {result}")
    return result


def monitor(trigger, print) -> str:
    """Monitor node that logs lifecycle signals from upstream nodes.

    Receives EpochSignalValue packets on its 'trigger' port, which
    contain the signal type, originating node name, and epoch ID.
    """
    signal_type = trigger.signal
    node_name = trigger.node_name
    epoch_id = str(trigger.epoch_id)
    print(f"Signal received: {signal_type} from {node_name} (epoch {epoch_id[:8]}...)")
    return f"{signal_type}@{node_name}"
