# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp net._net

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %%
#|export
import asyncio

import netrun_sim
from netrun.pool.thread import ThreadPool
from netrun.pool.multiprocess import MultiprocessPool
from netrun.pool.aio import SingleWorkerPool
from netrun.pool.remote import RemotePoolClient
from netrun.net.config import NetConfig
from netrun.execution_manager import PoolType

# %%
#|export
class NodeExecutionContext:
    pass

class NodeFailureContext:
    pass

# %%
#|export
class Net:
    def __init__(self, config: NetConfig):
        self._config: NetConfig = config
        self._graph: netrun_sim.Graph = self.config.graph.get_graph()
        self._netsim = netrun_sim.NetSim(self._graph)
        self._started: bool = False

        _exec_manager_config = {}
        for pool_name, pool_config in self.config.pools.items():
            match pool_config.spec.type:
                case "main": pool_type = SingleWorkerPool
                case "thread": pool_type = ThreadPool
                case "multiprocess": pool_type = MultiprocessPool
                case "remote": pool_type = RemotePoolClient
                case _: raise ValueError(f"Invalid pool type: {pool_config.spec.type}")
            _init_kwargs = pool_config.spec.model_dump()
            _init_kwargs.pop("type")
            _exec_manager_config[pool_name] = (pool_type, _init_kwargs)
        self._execution_manager = ExecutionManager(_exec_manager_config)

    @property
    def config(self) -> NetConfig:
        return self._config

    @property
    def graph(self) -> netrun_sim.Graph:
        return self._graph

    @property
    def pools(self) -> list[tuple[str, type[PoolType]]]:
        return self._execution_manager.pools

    @property
    def started(self) -> bool:
        return self._started

    def start_pools(self):
        """
        Can be used before `Net.start`, to initialise the pools before starting the Net itself.
        """
        if self._execution_manager.started:
            raise RuntimeError("Execution manager already started")
        self._execution_manager.start()

    def start(self):
        if self._started:
            raise RuntimeError("Net already started")
        if not self.start_pools():
            self.start_pools()
        self._execution_manager.start()

    def stop(self):
        self._execution_manager.stop()

    # send_packet_to_input_port
    # execute_epoch

    async def async_run_step(self):
        self.netsim

    def run_step(self):
        asyncio.run(self.async_run_step())

# %%
await net.async_run_step()
await net.async_start()
net.start()
net.run_step()

net.start(threaded=True)
net.run_step(threaded=True)

net.wait_until_blocked()       # Block until net is blocked
await net.async_wait_until_blocked()
net.poll()                      # Check status (blocked, running, etc.)
net.pause()                     # Finish current epochs, don't start new ones
await net.async_pause()
net.stop()                      # Stop the net entirely
await net.async_stop()
