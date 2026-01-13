"""
Tests for Milestone 6: Thread and Process Pools
"""

import pytest
import threading
import time
from concurrent.futures import Future

from netrun import (
    Graph, Node, Edge, Port, PortType, PortRef, PortState,
    MaxSalvos, SalvoCondition, SalvoConditionTerm,
    Net, NetState, NodeConfig,
    # Pool types
    PoolType, PoolConfig, PoolInitMode, WorkerState, ManagedPool, PoolManager,
    BackgroundNetRunner,
)


# ============================================================================
# Test Fixtures
# ============================================================================

@pytest.fixture
def simple_graph():
    """Create a simple Source -> Sink graph."""
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
        Edge(
            PortRef("Source", PortType.Output, "out"),
            PortRef("Sink", PortType.Input, "in")
        )
    ]
    return Graph([source, sink], edges)


# ============================================================================
# PoolConfig Tests
# ============================================================================

class TestPoolConfig:
    """Test PoolConfig class."""

    def test_valid_config(self):
        """Test creating a valid pool config."""
        config = PoolConfig(name="test", pool_type=PoolType.THREAD, size=4)
        assert config.name == "test"
        assert config.pool_type == PoolType.THREAD
        assert config.size == 4

    def test_invalid_size_zero(self):
        """Test that size=0 raises ValueError."""
        with pytest.raises(ValueError, match="at least 1"):
            PoolConfig(name="test", pool_type=PoolType.THREAD, size=0)

    def test_invalid_size_negative(self):
        """Test that negative size raises ValueError."""
        with pytest.raises(ValueError, match="at least 1"):
            PoolConfig(name="test", pool_type=PoolType.THREAD, size=-1)


# ============================================================================
# WorkerState Tests
# ============================================================================

class TestWorkerState:
    """Test WorkerState class."""

    def test_initial_state(self):
        """Test initial worker state."""
        worker = WorkerState(worker_id=0, pool_name="test")
        assert worker.worker_id == 0
        assert worker.pool_name == "test"
        assert worker.active_tasks == 0
        assert len(worker.initialized_nodes) == 0

    def test_acquire_release_task(self):
        """Test task tracking."""
        worker = WorkerState(worker_id=0, pool_name="test")
        assert worker.active_tasks == 0

        worker.acquire_task()
        assert worker.active_tasks == 1

        worker.acquire_task()
        assert worker.active_tasks == 2

        worker.release_task()
        assert worker.active_tasks == 1

        worker.release_task()
        assert worker.active_tasks == 0

    def test_node_initialization_tracking(self):
        """Test node initialization tracking."""
        worker = WorkerState(worker_id=0, pool_name="test")

        assert not worker.is_node_initialized("NodeA")

        worker.mark_node_initialized("NodeA")
        assert worker.is_node_initialized("NodeA")
        assert not worker.is_node_initialized("NodeB")


# ============================================================================
# ManagedPool Tests
# ============================================================================

class TestManagedPool:
    """Test ManagedPool class."""

    def test_create_thread_pool(self):
        """Test creating a thread pool."""
        config = PoolConfig(name="test", pool_type=PoolType.THREAD, size=2)
        pool = ManagedPool(config)
        assert pool.name == "test"
        assert pool.is_thread_pool
        assert not pool.is_process_pool
        assert pool.size == 2

    def test_create_process_pool(self):
        """Test creating a process pool."""
        config = PoolConfig(name="test", pool_type=PoolType.PROCESS, size=2)
        pool = ManagedPool(config)
        assert pool.name == "test"
        assert not pool.is_thread_pool
        assert pool.is_process_pool

    def test_start_stop(self):
        """Test starting and stopping a pool."""
        config = PoolConfig(name="test", pool_type=PoolType.THREAD, size=2)
        pool = ManagedPool(config)

        # Initially not started
        pool.start()
        assert pool._started

        # Can start multiple times (idempotent)
        pool.start()
        assert pool._started

        pool.stop()
        assert not pool._started

    def test_submit_to_started_pool(self):
        """Test submitting tasks to a started pool."""
        config = PoolConfig(name="test", pool_type=PoolType.THREAD, size=2)
        pool = ManagedPool(config)
        pool.start()

        try:
            result = []

            def task(x):
                result.append(x * 2)
                return x * 2

            future = pool.submit(task, 5)
            assert isinstance(future, Future)
            assert future.result() == 10
            assert result == [10]
        finally:
            pool.stop()

    def test_submit_to_unstarted_pool_raises(self):
        """Test that submitting to an unstarted pool raises."""
        config = PoolConfig(name="test", pool_type=PoolType.THREAD, size=2)
        pool = ManagedPool(config)

        with pytest.raises(RuntimeError, match="not started"):
            pool.submit(lambda: None)

    def test_worker_tracking(self):
        """Test worker availability tracking."""
        config = PoolConfig(name="test", pool_type=PoolType.THREAD, size=2)
        pool = ManagedPool(config)
        pool.start()

        try:
            assert pool.get_available_workers() == 2
            assert pool.get_total_active_tasks() == 0

            worker = pool.get_least_busy_worker()
            assert worker is not None
            assert worker.active_tasks == 0
        finally:
            pool.stop()

    def test_global_node_initialization(self):
        """Test global node initialization tracking."""
        config = PoolConfig(name="test", pool_type=PoolType.THREAD, size=2)
        pool = ManagedPool(config)

        assert not pool.is_node_globally_initialized("NodeA")

        pool.mark_node_globally_initialized("NodeA")
        assert pool.is_node_globally_initialized("NodeA")


# ============================================================================
# PoolManager Tests
# ============================================================================

class TestPoolManager:
    """Test PoolManager class."""

    def test_create_empty_manager(self):
        """Test creating an empty pool manager."""
        manager = PoolManager()
        assert not manager.has_pools()
        assert manager.pool_names == []

    def test_create_with_thread_pools(self):
        """Test creating manager with thread pools."""
        manager = PoolManager(
            thread_pools={
                "workers": {"size": 4},
                "io": {"size": 2},
            }
        )
        assert manager.has_pools()
        assert "workers" in manager.pool_names
        assert "io" in manager.pool_names

        workers = manager.get_pool("workers")
        assert workers is not None
        assert workers.is_thread_pool
        assert workers.size == 4

    def test_create_with_process_pools(self):
        """Test creating manager with process pools."""
        manager = PoolManager(
            process_pools={
                "compute": {"size": 2},
            }
        )
        assert manager.has_pools()
        compute = manager.get_pool("compute")
        assert compute is not None
        assert compute.is_process_pool

    def test_start_stop_all_pools(self):
        """Test starting and stopping all pools."""
        manager = PoolManager(
            thread_pools={"a": {"size": 2}},
            process_pools={"b": {"size": 2}},
        )
        manager.start()

        pool_a = manager.get_pool("a")
        pool_b = manager.get_pool("b")
        assert pool_a._started
        assert pool_b._started

        manager.stop()
        assert not pool_a._started
        assert not pool_b._started

    def test_get_pools_multiple(self):
        """Test getting multiple pools by name."""
        manager = PoolManager(
            thread_pools={
                "a": {"size": 2},
                "b": {"size": 2},
                "c": {"size": 2},
            }
        )
        pools = manager.get_pools(["a", "c"])
        assert len(pools) == 2

    def test_select_pool_least_busy(self):
        """Test pool selection with least_busy algorithm."""
        manager = PoolManager(
            thread_pools={
                "a": {"size": 2},
                "b": {"size": 4},
            }
        )
        manager.start()

        try:
            # Pool b has more workers, so should be selected
            selected = manager.select_pool(["a", "b"], algorithm="least_busy")
            assert selected is not None
            assert selected.name == "b"
        finally:
            manager.stop()

    def test_select_pool_invalid_algorithm(self):
        """Test that invalid algorithm raises."""
        manager = PoolManager(thread_pools={"a": {"size": 2}})
        manager.start()

        try:
            with pytest.raises(ValueError, match="Unknown pool selection"):
                manager.select_pool(["a"], algorithm="invalid")
        finally:
            manager.stop()

    def test_get_nonexistent_pool(self):
        """Test getting a non-existent pool."""
        manager = PoolManager()
        assert manager.get_pool("nonexistent") is None


# ============================================================================
# BackgroundNetRunner Tests
# ============================================================================

class TestBackgroundNetRunner:
    """Test BackgroundNetRunner class."""

    def test_runner_basic(self, simple_graph):
        """Test basic background runner functionality."""
        net = Net(simple_graph)

        # Simple exec funcs
        results = []

        def source_exec(ctx, packets):
            pkt = ctx.create_packet("data")
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    results.append(ctx.consume_packet(pkt))

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Sink", sink_exec)
        net.inject_source_epoch("Source")

        runner = BackgroundNetRunner(net)
        assert not runner.is_running

        runner.start()
        assert runner.is_running

        # Wait for completion
        blocked = runner.wait_until_blocked(timeout=5.0)
        assert blocked

        runner.stop()
        assert not runner.is_running
        assert results == ["data"]

    def test_runner_poll(self, simple_graph):
        """Test polling the runner."""
        net = Net(simple_graph)

        def source_exec(ctx, packets):
            pkt = ctx.create_packet("data")
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    ctx.consume_packet(pkt)

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Sink", sink_exec)
        net.inject_source_epoch("Source")

        runner = BackgroundNetRunner(net)
        runner.start()

        # Poll until blocked
        while not runner.poll():
            time.sleep(0.01)

        runner.stop()

    def test_runner_pause(self, simple_graph):
        """Test pausing through the runner."""
        net = Net(simple_graph)

        def source_exec(ctx, packets):
            pkt = ctx.create_packet("data")
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    ctx.consume_packet(pkt)

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Sink", sink_exec)
        net.inject_source_epoch("Source")

        runner = BackgroundNetRunner(net)
        runner.start()
        runner.wait_until_blocked(timeout=5.0)
        runner.pause()
        runner.stop()

        assert net.state == NetState.PAUSED


# ============================================================================
# Net Pool Integration Tests
# ============================================================================

class TestNetPoolIntegration:
    """Test pool integration with Net."""

    def test_net_creates_pool_manager(self, simple_graph):
        """Test that Net creates a pool manager."""
        net = Net(
            simple_graph,
            thread_pools={"workers": {"size": 4}},
        )
        assert net.pool_manager is not None
        assert net.pool_manager.has_pools()
        assert net.pool_manager.get_pool("workers") is not None

    def test_net_start_starts_pools(self, simple_graph):
        """Test that Net.start() starts pools."""
        net = Net(
            simple_graph,
            thread_pools={"workers": {"size": 2}},
        )

        def source_exec(ctx, packets):
            pkt = ctx.create_packet("data")
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    ctx.consume_packet(pkt)

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Sink", sink_exec)
        net.inject_source_epoch("Source")

        # After start(), pools should have started and stopped
        net.start()

        # Pools are stopped after start() completes
        pool = net.pool_manager.get_pool("workers")
        assert not pool._started

    def test_net_start_threaded(self, simple_graph):
        """Test Net.start(threaded=True) returns BackgroundNetRunner."""
        net = Net(simple_graph)

        results = []

        def source_exec(ctx, packets):
            pkt = ctx.create_packet("threaded_data")
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    results.append(ctx.consume_packet(pkt))

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Sink", sink_exec)
        net.inject_source_epoch("Source")

        runner = net.start(threaded=True)
        assert isinstance(runner, BackgroundNetRunner)
        assert runner.is_running

        runner.wait_until_blocked(timeout=5.0)
        runner.stop()

        assert results == ["threaded_data"]

    def test_net_stop_stops_background_runner(self, simple_graph):
        """Test that Net.stop() stops the background runner."""
        net = Net(simple_graph)

        def source_exec(ctx, packets):
            pkt = ctx.create_packet("data")
            ctx.load_output_port("out", pkt)
            ctx.send_output_salvo("send")

        def sink_exec(ctx, packets):
            for port_name, pkts in packets.items():
                for pkt in pkts:
                    ctx.consume_packet(pkt)

        net.set_node_exec("Source", source_exec)
        net.set_node_exec("Sink", sink_exec)
        net.inject_source_epoch("Source")

        runner = net.start(threaded=True)
        runner.wait_until_blocked(timeout=5.0)

        net.stop()
        assert net.state == NetState.STOPPED
        assert not runner.is_running


# ============================================================================
# PoolInitMode Tests
# ============================================================================

class TestPoolInitMode:
    """Test PoolInitMode enum."""

    def test_per_worker_mode(self):
        """Test PER_WORKER mode value."""
        assert PoolInitMode.PER_WORKER.value == "per_worker"

    def test_global_mode(self):
        """Test GLOBAL mode value."""
        assert PoolInitMode.GLOBAL.value == "global"
