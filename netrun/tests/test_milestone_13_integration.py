"""
Milestone 14: Integration and Polish - Integration Tests

End-to-end tests demonstrating real-world patterns and combining
multiple features from all previous milestones.
"""

import pytest
import time
import tempfile
from pathlib import Path
from typing import Any, Dict, List

from netrun import (
    Graph, Node, Edge, Port, PortType, PortRef, PortState,
    MaxSalvos, SalvoCondition, SalvoConditionTerm, PortSlotSpec,
    Net, NetState, NodeConfig,
    parse_toml_string, net_config_to_toml,
)


# ============================================================================
# Test Fixtures
# ============================================================================

@pytest.fixture
def producer_consumer_graph():
    """
    Producer-Consumer Pattern:

    Producer --> Buffer --> Consumer

    - Producer generates items at a rate
    - Buffer has limited capacity
    - Consumer processes items
    """
    producer = Node(
        name="Producer",
        out_ports={"out": Port()},
        out_salvo_conditions={
            "produce": SalvoCondition(
                MaxSalvos.infinite(),
                "out",
                SalvoConditionTerm.port("out", PortState.non_empty())
            )
        }
    )

    buffer = Node(
        name="Buffer",
        in_ports={"in": Port(PortSlotSpec.finite(5))},  # Limited buffer
        out_ports={"out": Port()},
        in_salvo_conditions={
            "receive": SalvoCondition(
                MaxSalvos.finite(1),
                "in",
                SalvoConditionTerm.port("in", PortState.non_empty())
            )
        },
        out_salvo_conditions={
            "forward": SalvoCondition(
                MaxSalvos.infinite(),
                "out",
                SalvoConditionTerm.port("out", PortState.non_empty())
            )
        }
    )

    consumer = Node(
        name="Consumer",
        in_ports={"in": Port()},
        in_salvo_conditions={
            "consume": SalvoCondition(
                MaxSalvos.finite(1),
                "in",
                SalvoConditionTerm.port("in", PortState.non_empty())
            )
        }
    )

    edges = [
        Edge(PortRef("Producer", PortType.Output, "out"), PortRef("Buffer", PortType.Input, "in")),
        Edge(PortRef("Buffer", PortType.Output, "out"), PortRef("Consumer", PortType.Input, "in")),
    ]

    return Graph([producer, buffer, consumer], edges)


@pytest.fixture
def fan_out_fan_in_graph():
    r"""
    Fan-Out/Fan-In Pattern:

          /--> Worker1 --\
    Source --> Worker2 --> Aggregator
          \--> Worker3 --/

    - Source distributes work to multiple workers
    - Workers process in parallel
    - Aggregator collects results
    """
    source = Node(
        name="Source",
        out_ports={
            "out1": Port(),
            "out2": Port(),
            "out3": Port(),
        },
        out_salvo_conditions={
            "distribute": SalvoCondition(
                MaxSalvos.infinite(),
                ["out1", "out2", "out3"],
                SalvoConditionTerm.and_([
                    SalvoConditionTerm.port("out1", PortState.non_empty()),
                    SalvoConditionTerm.port("out2", PortState.non_empty()),
                    SalvoConditionTerm.port("out3", PortState.non_empty()),
                ])
            )
        }
    )

    workers = []
    for i in range(1, 4):
        worker = Node(
            name=f"Worker{i}",
            in_ports={"in": Port()},
            out_ports={"out": Port()},
            in_salvo_conditions={
                "receive": SalvoCondition(
                    MaxSalvos.finite(1),
                    "in",
                    SalvoConditionTerm.port("in", PortState.non_empty())
                )
            },
            out_salvo_conditions={
                "send": SalvoCondition(
                    MaxSalvos.infinite(),
                    "out",
                    SalvoConditionTerm.port("out", PortState.non_empty())
                )
            }
        )
        workers.append(worker)

    aggregator = Node(
        name="Aggregator",
        in_ports={
            "in1": Port(),
            "in2": Port(),
            "in3": Port(),
        },
        in_salvo_conditions={
            "collect": SalvoCondition(
                MaxSalvos.finite(1),
                ["in1", "in2", "in3"],
                SalvoConditionTerm.and_([
                    SalvoConditionTerm.port("in1", PortState.non_empty()),
                    SalvoConditionTerm.port("in2", PortState.non_empty()),
                    SalvoConditionTerm.port("in3", PortState.non_empty()),
                ])
            )
        }
    )

    edges = [
        Edge(PortRef("Source", PortType.Output, "out1"), PortRef("Worker1", PortType.Input, "in")),
        Edge(PortRef("Source", PortType.Output, "out2"), PortRef("Worker2", PortType.Input, "in")),
        Edge(PortRef("Source", PortType.Output, "out3"), PortRef("Worker3", PortType.Input, "in")),
        Edge(PortRef("Worker1", PortType.Output, "out"), PortRef("Aggregator", PortType.Input, "in1")),
        Edge(PortRef("Worker2", PortType.Output, "out"), PortRef("Aggregator", PortType.Input, "in2")),
        Edge(PortRef("Worker3", PortType.Output, "out"), PortRef("Aggregator", PortType.Input, "in3")),
    ]

    return Graph([source] + workers + [aggregator], edges)


@pytest.fixture
def pipeline_with_error_handling_graph():
    """
    Pipeline with Error Handling:

    Input --> Validate --> Process --> Output
                |
                v
            ErrorHandler

    - Validate can reject items
    - Process can fail and retry
    - ErrorHandler receives failed items
    """
    input_node = Node(
        name="Input",
        out_ports={"out": Port()},
        out_salvo_conditions={
            "send": SalvoCondition(
                MaxSalvos.infinite(),
                "out",
                SalvoConditionTerm.port("out", PortState.non_empty())
            )
        }
    )

    validate = Node(
        name="Validate",
        in_ports={"in": Port()},
        out_ports={"valid": Port(), "invalid": Port()},
        in_salvo_conditions={
            "receive": SalvoCondition(
                MaxSalvos.finite(1),
                "in",
                SalvoConditionTerm.port("in", PortState.non_empty())
            )
        },
        out_salvo_conditions={
            "send_valid": SalvoCondition(
                MaxSalvos.infinite(),
                "valid",
                SalvoConditionTerm.port("valid", PortState.non_empty())
            ),
            "send_invalid": SalvoCondition(
                MaxSalvos.infinite(),
                "invalid",
                SalvoConditionTerm.port("invalid", PortState.non_empty())
            )
        }
    )

    process = Node(
        name="Process",
        in_ports={"in": Port()},
        out_ports={"out": Port()},
        in_salvo_conditions={
            "receive": SalvoCondition(
                MaxSalvos.finite(1),
                "in",
                SalvoConditionTerm.port("in", PortState.non_empty())
            )
        },
        out_salvo_conditions={
            "send": SalvoCondition(
                MaxSalvos.infinite(),
                "out",
                SalvoConditionTerm.port("out", PortState.non_empty())
            )
        }
    )

    output_node = Node(
        name="Output",
        in_ports={"in": Port()},
        in_salvo_conditions={
            "receive": SalvoCondition(
                MaxSalvos.finite(1),
                "in",
                SalvoConditionTerm.port("in", PortState.non_empty())
            )
        }
    )

    error_handler = Node(
        name="ErrorHandler",
        in_ports={"in": Port()},
        in_salvo_conditions={
            "receive": SalvoCondition(
                MaxSalvos.finite(1),
                "in",
                SalvoConditionTerm.port("in", PortState.non_empty())
            )
        }
    )

    edges = [
        Edge(PortRef("Input", PortType.Output, "out"), PortRef("Validate", PortType.Input, "in")),
        Edge(PortRef("Validate", PortType.Output, "valid"), PortRef("Process", PortType.Input, "in")),
        Edge(PortRef("Validate", PortType.Output, "invalid"), PortRef("ErrorHandler", PortType.Input, "in")),
        Edge(PortRef("Process", PortType.Output, "out"), PortRef("Output", PortType.Input, "in")),
    ]

    return Graph([input_node, validate, process, output_node, error_handler], edges)


# ============================================================================
# Producer-Consumer Pattern Tests
# ============================================================================

class TestProducerConsumerPattern:
    """Test producer-consumer pattern with rate limiting and buffering."""

    def test_basic_producer_consumer(self, producer_consumer_graph):
        """Test basic producer-consumer flow."""
        net = Net(producer_consumer_graph)

        items_produced = []
        items_consumed = []

        def producer_exec(ctx, packets):
            for i in range(3):
                pkt = ctx.create_packet({"item": i, "produced_at": time.time()})
                ctx.load_output_port("out", pkt)
                ctx.send_output_salvo("produce")
                items_produced.append(i)

        def buffer_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    # Add buffer timestamp
                    value["buffered_at"] = time.time()
                    out_pkt = ctx.create_packet(value)
                    ctx.load_output_port("out", out_pkt)
                    ctx.send_output_salvo("forward")

        def consumer_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    value["consumed_at"] = time.time()
                    items_consumed.append(value)

        net.set_node_exec("Producer", producer_exec)
        net.set_node_exec("Buffer", buffer_exec)
        net.set_node_exec("Consumer", consumer_exec)

        net.inject_source_epoch("Producer")
        net.start()

        assert len(items_produced) == 3
        assert len(items_consumed) == 3

        # Verify timing chain
        for item in items_consumed:
            assert item["produced_at"] <= item["buffered_at"]
            assert item["buffered_at"] <= item["consumed_at"]

    def test_producer_consumer_with_rate_limit(self, producer_consumer_graph):
        """Test producer-consumer with rate limiting on consumer."""
        net = Net(producer_consumer_graph)

        consume_times = []

        def producer_exec(ctx, packets):
            for i in range(3):
                pkt = ctx.create_packet({"item": i})
                ctx.load_output_port("out", pkt)
                ctx.send_output_salvo("produce")

        def buffer_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    out_pkt = ctx.create_packet(value)
                    ctx.load_output_port("out", out_pkt)
                    ctx.send_output_salvo("forward")

        def consumer_exec(ctx, packets):
            consume_times.append(time.time())
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    ctx.consume_packet(pkt)

        net.set_node_exec("Producer", producer_exec)
        net.set_node_exec("Buffer", buffer_exec)
        net.set_node_exec("Consumer", consumer_exec)
        net.set_node_config("Consumer", rate_limit_per_second=10)  # Max 10/sec

        net.inject_source_epoch("Producer")
        net.start()

        assert len(consume_times) == 3


# ============================================================================
# Fan-Out/Fan-In Pattern Tests
# ============================================================================

class TestFanOutFanInPattern:
    """Test fan-out/fan-in pattern with parallel workers."""

    def test_basic_fan_out_fan_in(self, fan_out_fan_in_graph):
        """Test basic fan-out/fan-in flow."""
        net = Net(fan_out_fan_in_graph)

        worker_results = {1: [], 2: [], 3: []}
        aggregated_results = []

        def source_exec(ctx, packets):
            # Distribute work to all workers
            for i in range(1, 4):
                pkt = ctx.create_packet({"task": f"task_{i}", "worker": i})
                ctx.load_output_port(f"out{i}", pkt)
            ctx.send_output_salvo("distribute")

        def worker_exec(ctx, packets):
            worker_num = int(ctx.node_name[-1])
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    value["processed_by"] = ctx.node_name
                    value["processed_at"] = time.time()
                    worker_results[worker_num].append(value)

                    out_pkt = ctx.create_packet(value)
                    ctx.load_output_port("out", out_pkt)
                    ctx.send_output_salvo("send")

        def aggregator_exec(ctx, packets):
            collected = []
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    collected.append(ctx.consume_packet(pkt))
            aggregated_results.append(collected)

        net.set_node_exec("Source", source_exec)
        for i in range(1, 4):
            net.set_node_exec(f"Worker{i}", worker_exec)
        net.set_node_exec("Aggregator", aggregator_exec)

        net.inject_source_epoch("Source")
        net.start()

        # All workers should have processed one item
        assert len(worker_results[1]) == 1
        assert len(worker_results[2]) == 1
        assert len(worker_results[3]) == 1

        # Aggregator should have collected all results
        assert len(aggregated_results) == 1
        assert len(aggregated_results[0]) == 3


# ============================================================================
# Pipeline with Error Handling Tests
# ============================================================================

class TestPipelineWithErrorHandling:
    """Test pipeline pattern with validation and error handling."""

    def test_valid_items_pass_through(self, pipeline_with_error_handling_graph):
        """Test that valid items pass through the pipeline."""
        net = Net(pipeline_with_error_handling_graph)

        output_items = []
        error_items = []

        def input_exec(ctx, packets):
            # Send valid items
            for i in range(3):
                pkt = ctx.create_packet({"value": i * 10, "valid": True})
                ctx.load_output_port("out", pkt)
                ctx.send_output_salvo("send")

        def validate_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    if value.get("valid"):
                        out_pkt = ctx.create_packet(value)
                        ctx.load_output_port("valid", out_pkt)
                        ctx.send_output_salvo("send_valid")
                    else:
                        out_pkt = ctx.create_packet(value)
                        ctx.load_output_port("invalid", out_pkt)
                        ctx.send_output_salvo("send_invalid")

        def process_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    value["processed"] = True
                    out_pkt = ctx.create_packet(value)
                    ctx.load_output_port("out", out_pkt)
                    ctx.send_output_salvo("send")

        def output_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    output_items.append(ctx.consume_packet(pkt))

        def error_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    error_items.append(ctx.consume_packet(pkt))

        net.set_node_exec("Input", input_exec)
        net.set_node_exec("Validate", validate_exec)
        net.set_node_exec("Process", process_exec)
        net.set_node_exec("Output", output_exec)
        net.set_node_exec("ErrorHandler", error_exec)

        net.inject_source_epoch("Input")
        net.start()

        assert len(output_items) == 3
        assert len(error_items) == 0
        assert all(item["processed"] for item in output_items)

    def test_invalid_items_routed_to_error_handler(self, pipeline_with_error_handling_graph):
        """Test that invalid items are routed to the error handler."""
        net = Net(pipeline_with_error_handling_graph)

        output_items = []
        error_items = []

        def input_exec(ctx, packets):
            # Send mix of valid and invalid items
            for i in range(3):
                valid = i % 2 == 0  # Items 0 and 2 are valid
                pkt = ctx.create_packet({"value": i, "valid": valid})
                ctx.load_output_port("out", pkt)
                ctx.send_output_salvo("send")

        def validate_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    if value.get("valid"):
                        out_pkt = ctx.create_packet(value)
                        ctx.load_output_port("valid", out_pkt)
                        ctx.send_output_salvo("send_valid")
                    else:
                        out_pkt = ctx.create_packet(value)
                        ctx.load_output_port("invalid", out_pkt)
                        ctx.send_output_salvo("send_invalid")

        def process_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    value["processed"] = True
                    out_pkt = ctx.create_packet(value)
                    ctx.load_output_port("out", out_pkt)
                    ctx.send_output_salvo("send")

        def output_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    output_items.append(ctx.consume_packet(pkt))

        def error_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    error_items.append(ctx.consume_packet(pkt))

        net.set_node_exec("Input", input_exec)
        net.set_node_exec("Validate", validate_exec)
        net.set_node_exec("Process", process_exec)
        net.set_node_exec("Output", output_exec)
        net.set_node_exec("ErrorHandler", error_exec)

        net.inject_source_epoch("Input")
        net.start()

        # 2 valid items (0 and 2), 1 invalid (1)
        assert len(output_items) == 2
        assert len(error_items) == 1
        assert error_items[0]["value"] == 1

    def test_process_with_retries(self, pipeline_with_error_handling_graph):
        """Test that process node retries on failure."""
        net = Net(pipeline_with_error_handling_graph, on_error="continue")

        output_items = []
        process_attempts = []

        def input_exec(ctx, packets):
            pkt = ctx.create_packet({"value": 42, "valid": True})
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def validate_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    out_pkt = ctx.create_packet(value)
                    ctx.load_output_port("valid", out_pkt)
                    ctx.send_output_salvo("send_valid")

        def process_exec(ctx, packets):
            process_attempts.append(ctx.retry_count)
            if ctx.retry_count < 2:
                raise ValueError("Simulated failure")

            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    value["processed"] = True
                    out_pkt = ctx.create_packet(value)
                    ctx.load_output_port("out", out_pkt)
                    ctx.send_output_salvo("send")

        def output_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    output_items.append(ctx.consume_packet(pkt))

        def error_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    ctx.consume_packet(pkt)

        net.set_node_exec("Input", input_exec)
        net.set_node_exec("Validate", validate_exec)
        net.set_node_exec("Process", process_exec)
        net.set_node_exec("Output", output_exec)
        net.set_node_exec("ErrorHandler", error_exec)
        net.set_node_config("Process", retries=3, defer_net_actions=True)

        net.inject_source_epoch("Input")
        net.start()

        # Should have retried 3 times (attempts 0, 1, 2)
        assert len(process_attempts) == 3
        assert output_items[0]["processed"] == True


# ============================================================================
# Checkpoint and Resume Tests
# ============================================================================

class TestCheckpointAndResume:
    """Test checkpoint save/load with complex flows."""

    def test_checkpoint_mid_pipeline(self, producer_consumer_graph):
        """Test checkpointing and resuming a pipeline mid-execution."""
        with tempfile.TemporaryDirectory() as tmpdir:
            checkpoint_path = Path(tmpdir) / "checkpoint"

            # First run: produce items, pause before consumer processes all
            net1 = Net(producer_consumer_graph)
            items_consumed_1 = []

            produced_count = [0]

            def producer_exec_1(ctx, packets):
                for i in range(5):
                    pkt = ctx.create_packet({"item": i})
                    ctx.load_output_port("out", pkt)
                    ctx.send_output_salvo("produce")
                    produced_count[0] += 1

            def buffer_exec_1(ctx, packets):
                for port_name, pkts in packets.items():
                    for pkt in pkts:
                        value = ctx.consume_packet(pkt)
                        out_pkt = ctx.create_packet(value)
                        ctx.load_output_port("out", out_pkt)
                        ctx.send_output_salvo("forward")

            def consumer_exec_1(ctx, packets):
                for port_name, pkts in packets.items():
                    for pkt in pkts:
                        items_consumed_1.append(ctx.consume_packet(pkt))

            net1.set_node_exec("Producer", producer_exec_1)
            net1.set_node_exec("Buffer", buffer_exec_1)
            net1.set_node_exec("Consumer", consumer_exec_1)

            # Start and immediately pause
            net1.inject_source_epoch("Producer")
            net1.start()
            net1.pause()

            # Save checkpoint
            net1.save_checkpoint(checkpoint_path)

            items_before_checkpoint = len(items_consumed_1)

            # Second run: load checkpoint and continue
            net2 = Net.load_checkpoint(checkpoint_path, resolve_funcs=False)
            items_consumed_2 = []

            def consumer_exec_2(ctx, packets):
                for port_name, pkts in packets.items():
                    for pkt in pkts:
                        items_consumed_2.append(ctx.consume_packet(pkt))

            net2.set_node_exec("Producer", producer_exec_1)  # Won't run again
            net2.set_node_exec("Buffer", buffer_exec_1)
            net2.set_node_exec("Consumer", consumer_exec_2)

            # Resume
            net2.start()

            # Total consumed should be 5
            total_consumed = items_before_checkpoint + len(items_consumed_2)
            assert total_consumed == 5


# ============================================================================
# TOML DSL Integration Tests
# ============================================================================

class TestTOMLDSLIntegration:
    """Test TOML DSL with complex configurations."""

    def test_round_trip_complex_graph(self, fan_out_fan_in_graph):
        """Test that a complex graph survives TOML round-trip."""
        from netrun.dsl import graph_to_dict, dict_to_graph

        # Convert to dict and back
        graph_dict = graph_to_dict(fan_out_fan_in_graph)
        restored_graph = dict_to_graph(graph_dict)

        # Verify nodes
        orig_nodes = fan_out_fan_in_graph.nodes()
        rest_nodes = restored_graph.nodes()
        assert set(orig_nodes.keys()) == set(rest_nodes.keys())

        # Verify edges
        orig_edges = fan_out_fan_in_graph.edges()
        rest_edges = restored_graph.edges()
        assert len(orig_edges) == len(rest_edges)

    def test_toml_with_all_features(self):
        """Test parsing TOML with pools, configs, and factories."""
        toml_content = '''
[net]
on_error = "continue"

[net.thread_pools.compute]
size = 4

[nodes.Source]
out_ports = { out = {} }

[nodes.Source.out_salvo_conditions.send]
max_salvos = "infinite"
ports = "out"
when = "nonempty(out)"

[nodes.Processor]
in_ports = { in = {} }
out_ports = { out = {} }

[nodes.Processor.in_salvo_conditions.receive]
max_salvos = 1
ports = "in"
when = "nonempty(in)"

[nodes.Processor.out_salvo_conditions.send]
max_salvos = "infinite"
ports = "out"
when = "nonempty(out)"

[nodes.Processor.options]
pool = "compute"
retries = 3
defer_net_actions = true

[nodes.Sink]
in_ports = { in = {} }

[nodes.Sink.in_salvo_conditions.receive]
max_salvos = 1
ports = "in"
when = "nonempty(in)"

[[edges]]
from = "Source.out"
to = "Processor.in"

[[edges]]
from = "Processor.out"
to = "Sink.in"
'''

        config = parse_toml_string(toml_content)

        # Verify net config
        assert config.on_error == "continue"
        assert "compute" in config.thread_pools
        assert config.thread_pools["compute"]["size"] == 4

        # Verify nodes
        nodes = config.graph.nodes()
        assert "Source" in nodes
        assert "Processor" in nodes
        assert "Sink" in nodes

        # Verify node config
        assert "Processor" in config.node_configs
        assert config.node_configs["Processor"]["pool"] == "compute"
        assert config.node_configs["Processor"]["retries"] == 3


# ============================================================================
# Combined Feature Tests
# ============================================================================

class TestCombinedFeatures:
    """Test combining multiple features in a single net."""

    def test_pools_with_rate_limiting_and_history(self, producer_consumer_graph):
        """Test using pools, rate limiting, and history together."""
        with tempfile.TemporaryDirectory() as tmpdir:
            history_file = Path(tmpdir) / "history.jsonl"

            net = Net(
                producer_consumer_graph,
                thread_pools={"workers": {"size": 2}},
                history_file=str(history_file),
            )

            items_produced = []
            items_consumed = []

            def producer_exec(ctx, packets):
                for i in range(3):
                    pkt = ctx.create_packet({"item": i})
                    ctx.load_output_port("out", pkt)
                    ctx.send_output_salvo("produce")
                    items_produced.append(i)

            def buffer_exec(ctx, packets):
                for port_name, pkts in packets.items():
                    for pkt in pkts:
                        value = ctx.consume_packet(pkt)
                        out_pkt = ctx.create_packet(value)
                        ctx.load_output_port("out", out_pkt)
                        ctx.send_output_salvo("forward")

            def consumer_exec(ctx, packets):
                for port_name, pkts in packets.items():
                    for pkt in pkts:
                        items_consumed.append(ctx.consume_packet(pkt))

            net.set_node_exec("Producer", producer_exec)
            net.set_node_exec("Buffer", buffer_exec)
            net.set_node_exec("Consumer", consumer_exec)
            net.set_node_config("Buffer", pool="workers")
            net.set_node_config("Consumer", pool="workers", rate_limit_per_second=100)

            net.inject_source_epoch("Producer")
            net.start()

            assert len(items_consumed) == 3

            # Verify history object was created with correct config
            assert net._event_history is not None
            assert net._event_history._file_path == history_file
