"""
Milestone 14: Integration and Polish - Performance Tests

Basic performance tests to establish baseline metrics for:
- Packet throughput in single-threaded mode
- Packet throughput with thread pools
- Memory usage under load
"""

import pytest
import time
from typing import List

from netrun import (
    Graph, Node, Edge, Port, PortType, PortRef, PortState,
    MaxSalvos, SalvoCondition, SalvoConditionTerm,
    Net, NetState,
)


# ============================================================================
# Test Fixtures
# ============================================================================

@pytest.fixture
def simple_pipeline_graph():
    """Create a simple Source -> Processor -> Sink pipeline for throughput tests."""
    source = Node(
        name="Source",
        out_ports={"out": Port()},
        out_salvo_conditions={
            "send": SalvoCondition(
                MaxSalvos.infinite(),
                "out",
                SalvoConditionTerm.port("out", PortState.non_empty())
            )
        }
    )

    processor = Node(
        name="Processor",
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

    sink = Node(
        name="Sink",
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
        Edge(PortRef("Source", PortType.Output, "out"), PortRef("Processor", PortType.Input, "in")),
        Edge(PortRef("Processor", PortType.Output, "out"), PortRef("Sink", PortType.Input, "in")),
    ]

    return Graph([source, processor, sink], edges)


# ============================================================================
# Throughput Tests
# ============================================================================

class TestPacketThroughput:
    """Test packet throughput in different configurations."""

    def test_single_threaded_throughput_100_packets(self, simple_pipeline_graph):
        """Measure throughput for 100 packets in single-threaded mode."""
        net = Net(simple_pipeline_graph)

        packet_count = 100
        processed_packets: List[dict] = []

        def source_exec(ctx, packets):
            for i in range(packet_count):
                pkt = ctx.create_packet({"index": i})
                ctx.load_output_port("out", pkt)
                ctx.send_output_salvo("send")

        def processor_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    out_pkt = ctx.create_packet(value)
                    ctx.load_output_port("out", out_pkt)
                    ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    processed_packets.append(ctx.consume_packet(pkt))

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Processor", processor_exec)
        net.set_node_exec("Sink", sink_exec)

        net.inject_source_epoch("Source")

        start_time = time.perf_counter()
        net.start()
        elapsed = time.perf_counter() - start_time

        assert len(processed_packets) == packet_count
        throughput = packet_count / elapsed

        # Just verify we processed all packets correctly
        # Throughput varies by machine, so we don't assert a minimum
        print(f"\nSingle-threaded throughput: {throughput:.0f} packets/sec ({elapsed:.3f}s for {packet_count} packets)")

    def test_single_threaded_throughput_1000_packets(self, simple_pipeline_graph):
        """Measure throughput for 1000 packets in single-threaded mode."""
        net = Net(simple_pipeline_graph)

        packet_count = 1000
        processed_packets: List[dict] = []

        def source_exec(ctx, packets):
            for i in range(packet_count):
                pkt = ctx.create_packet({"index": i})
                ctx.load_output_port("out", pkt)
                ctx.send_output_salvo("send")

        def processor_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    out_pkt = ctx.create_packet(value)
                    ctx.load_output_port("out", out_pkt)
                    ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    processed_packets.append(ctx.consume_packet(pkt))

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Processor", processor_exec)
        net.set_node_exec("Sink", sink_exec)

        net.inject_source_epoch("Source")

        start_time = time.perf_counter()
        net.start()
        elapsed = time.perf_counter() - start_time

        assert len(processed_packets) == packet_count
        throughput = packet_count / elapsed

        print(f"\nSingle-threaded throughput: {throughput:.0f} packets/sec ({elapsed:.3f}s for {packet_count} packets)")


class TestThreadPoolThroughput:
    """Test throughput with thread pools."""

    def test_thread_pool_throughput_100_packets(self, simple_pipeline_graph):
        """Measure throughput for 100 packets with thread pool."""
        net = Net(
            simple_pipeline_graph,
            thread_pools={"workers": {"size": 4}},
        )

        packet_count = 100
        processed_packets: List[dict] = []

        def source_exec(ctx, packets):
            for i in range(packet_count):
                pkt = ctx.create_packet({"index": i})
                ctx.load_output_port("out", pkt)
                ctx.send_output_salvo("send")

        def processor_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    out_pkt = ctx.create_packet(value)
                    ctx.load_output_port("out", out_pkt)
                    ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    processed_packets.append(ctx.consume_packet(pkt))

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Processor", processor_exec)
        net.set_node_exec("Sink", sink_exec)
        net.set_node_config("Processor", pool="workers")

        net.inject_source_epoch("Source")

        start_time = time.perf_counter()
        net.start()
        elapsed = time.perf_counter() - start_time

        assert len(processed_packets) == packet_count
        throughput = packet_count / elapsed

        print(f"\nThread pool throughput: {throughput:.0f} packets/sec ({elapsed:.3f}s for {packet_count} packets)")


class TestMemoryUsage:
    """Test memory usage under load."""

    def test_memory_with_consumed_storage(self, simple_pipeline_graph):
        """Test memory usage with consumed packet storage enabled."""
        net = Net(
            simple_pipeline_graph,
            consumed_packet_storage=True,
            consumed_packet_storage_limit=500,
        )

        packet_count = 1000
        processed_count = [0]

        def source_exec(ctx, packets):
            for i in range(packet_count):
                # Create packets with some data
                pkt = ctx.create_packet({
                    "index": i,
                    "data": "x" * 100,  # 100 bytes of data per packet
                })
                ctx.load_output_port("out", pkt)
                ctx.send_output_salvo("send")

        def processor_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    out_pkt = ctx.create_packet(value)
                    ctx.load_output_port("out", out_pkt)
                    ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    ctx.consume_packet(pkt)
                    processed_count[0] += 1

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Processor", processor_exec)
        net.set_node_exec("Sink", sink_exec)

        net.inject_source_epoch("Source")
        net.start()

        assert processed_count[0] == packet_count

        # Verify consumed storage limit was respected
        consumed_count = len(net._value_store._consumed_values)
        assert consumed_count <= 500

        print(f"\nProcessed {packet_count} packets, consumed storage: {consumed_count} (limit: 500)")

    def test_memory_without_consumed_storage(self, simple_pipeline_graph):
        """Test memory usage without consumed packet storage."""
        net = Net(
            simple_pipeline_graph,
            consumed_packet_storage=False,
        )

        packet_count = 1000
        processed_count = [0]

        def source_exec(ctx, packets):
            for i in range(packet_count):
                pkt = ctx.create_packet({
                    "index": i,
                    "data": "x" * 100,
                })
                ctx.load_output_port("out", pkt)
                ctx.send_output_salvo("send")

        def processor_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    out_pkt = ctx.create_packet(value)
                    ctx.load_output_port("out", out_pkt)
                    ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    ctx.consume_packet(pkt)
                    processed_count[0] += 1

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Processor", processor_exec)
        net.set_node_exec("Sink", sink_exec)

        net.inject_source_epoch("Source")
        net.start()

        assert processed_count[0] == packet_count

        # Verify no consumed packets were stored
        consumed_count = len(net._value_store._consumed_values)
        assert consumed_count == 0

        print(f"\nProcessed {packet_count} packets, consumed storage: {consumed_count}")


class TestDeferredActionsThroughput:
    """Test throughput with deferred actions (used for retries)."""

    def test_deferred_actions_throughput(self, simple_pipeline_graph):
        """Measure throughput with deferred actions enabled."""
        net = Net(simple_pipeline_graph)

        packet_count = 100
        processed_packets: List[dict] = []

        def source_exec(ctx, packets):
            for i in range(packet_count):
                pkt = ctx.create_packet({"index": i})
                ctx.load_output_port("out", pkt)
                ctx.send_output_salvo("send")

        def processor_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    value = ctx.consume_packet(pkt)
                    out_pkt = ctx.create_packet(value)
                    ctx.load_output_port("out", out_pkt)
                    ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    processed_packets.append(ctx.consume_packet(pkt))

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Processor", processor_exec)
        net.set_node_exec("Sink", sink_exec)
        net.set_node_config("Processor", defer_net_actions=True)

        net.inject_source_epoch("Source")

        start_time = time.perf_counter()
        net.start()
        elapsed = time.perf_counter() - start_time

        assert len(processed_packets) == packet_count
        throughput = packet_count / elapsed

        print(f"\nDeferred actions throughput: {throughput:.0f} packets/sec ({elapsed:.3f}s for {packet_count} packets)")
