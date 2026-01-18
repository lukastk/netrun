# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp execution_manager

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %% [markdown]
# # ExecutionManager
#
# The execution manager layer deals with the execution of functions inside the pools.
#
# It uses a round

# %%
#|export
from contextlib import contextmanager
from typing import Any
from collections.abc import Callable, Awaitable
import datetime
import builtins
from netrun._iutils import get_timestamp_utc
from datetime import datetime
import asyncio
from enum import Enum
from dataclasses import dataclass
import importlib
import uuid
import pickle
import random
2
from netrun.rpc.base import RPCChannel
from netrun.pool.thread import ThreadPool
from netrun.pool.multiprocess import MultiprocessPool
from netrun.pool.aio import SingleWorkerPool
from netrun.pool.remote import RemotePoolClient

# %% [markdown]
# ## `_override_builtins_print`
#
# Overrides print in a function. Used to keep the print statements in each function execution tracked and separate.

# %%
#|exporti
@contextmanager
def _override_builtins_print(func, new_print):
    g = func.__globals__
    old = g.get("print", builtins.print)
    g["print"] = new_print
    try:
        yield
    finally:
        # restore whatever was there before
        if old is builtins.print and "print" in g:
            # keep func globals clean if it didn't define print originally
            del g["print"]
        else:
            g["print"] = old

# %% [markdown]
# ## Function runners
#
# Helpers for the execution manager to run functions.

# %%
#|exporti
async def _async_func_runner(
    channel: RPCChannel,
    func: Callable[..., Awaitable[Any]],
    send_channel: bool,
    print_callback: Callable[[str], None],
    args: tuple,
    kwargs: dict,
) -> Any:
    with _override_builtins_print(func, print_callback):
        if asyncio.iscoroutinefunction(func):
            if send_channel:
                return await func(channel, *args, **kwargs)
            else:
                return await func(*args, **kwargs)
        else:
            if send_channel:
                return func(channel, *args, **kwargs)
            else:
                return func(*args, **kwargs)

# %%
#|exporti
def _func_runner(
    channel: RPCChannel,
    func: Callable[..., Any],
    send_channel: bool,
    print_callback: Callable[[str], None],
    args: tuple,
    kwargs: dict,
    event_loop: asyncio.AbstractEventLoop,
) -> Any:
    with _override_builtins_print(func, print_callback):
        if asyncio.iscoroutinefunction(func):
            if send_channel:
                return event_loop.run_until_complete(func(channel, *args, **kwargs))
            else:
                return event_loop.run_until_complete(func(*args, **kwargs))
        else:
            if send_channel:
                return func(channel, *args, **kwargs)
            else:
                return func(*args, **kwargs)

# %% [markdown]
# ## Helpers

# %%
#|exporti
def _convert_to_str_if_not_serializable(obj: Any) -> tuple[bool, Any]:
    """Convert an object to string if it's not pickle-serializable.

    Returns:
        Tuple of (was_converted, result)
    """
    try:
        pickle.dumps(obj)
        return (False, obj)
    except (pickle.PicklingError, TypeError, AttributeError):
        return (True, str(obj))

# %% [markdown]
# ## Workers
#
# Worker functions for the execution manager's pools

# %%
#|exporti
class ExecutionManagerProtocolKeys(Enum):
    RUN = "exec-manager:run"
    """
    Run a function.
    Args: msg_id, func_import_path_or_key, run_id, send_channel, args, kwargs
    """

    UP_RUN_STARTED = "exec-manager-up:run-started"
    """
    Notification from the worker that a run has been submitted and started. 
    Args: msg_id, timestamp_utc_started
    """

    UP_RUN_RESPONSE = "exec-manager-up:run-response"
    """
    Response to RUN from the worker.
    Args: msg_id, converted_to_str, _res
    """

    SEND_FUNCTION = "exec-manager:send-function"
    """
    Used to send a function object to the worker, which can then be run using RUN.
    Args: msg_id, func_key, func
    """

    UP_SEND_FUNCTION_RESPONSE = "exec-manager-up:send-function-response"
    """
    Response to SEND_FUNCTION from the worker, to confirm that the function was received.
    Args: msg_id
    """

    FLUSH_PRINT_BUFFER = "exec-manager:flush-print-buffer"
    """
    Flushes the print buffer of a given run_id.
    Args: msg_id, run_id
    """

    UP_FLUSH_PRINT_BUFFER_RESPONSE = "exec-manager-up:flush-print-buffer-response"
    """
    Response to FLUSH_PRINT_BUFFER from the worker, with the flushed buffer.
    Args: msg_id, run_id, _buffer
    """

# %%
#|exporti
def _thread_worker_func(channel, worker_id):
    func_print_buffer: dict[str, list[tuple[datetime, str]]] = {}
    event_loop = asyncio.new_event_loop()
    registered_functions: dict[str, Callable[..., Awaitable] | Callable[..., None]] = {}

    while True:
        key, data = channel.recv()
        # RUN
        if key == ExecutionManagerProtocolKeys.RUN.value:
            msg_id, func_import_path_or_key, run_id, send_channel, args, kwargs = data
            if func_import_path_or_key in registered_functions:
                func = registered_functions[func_import_path_or_key]
            else:
                module_path, func_name = func_import_path_or_key.rsplit(".", 1)
                module = importlib.import_module(module_path)
                func = getattr(module, func_name)
            func_print_buffer[run_id] = []
            timestamp_utc_started = get_timestamp_utc()
            channel.send(ExecutionManagerProtocolKeys.UP_RUN_STARTED.value, (msg_id, timestamp_utc_started))
            res = _func_runner(
                channel=channel,
                func=func,
                send_channel=send_channel,
                print_callback=lambda s: func_print_buffer[run_id].append((get_timestamp_utc(), s)),
                args=args,
                kwargs=kwargs,
                event_loop=event_loop,
            )
            timestamp_utc_completed = get_timestamp_utc()
            converted_to_str, _res = _convert_to_str_if_not_serializable(res)
            _buffer = func_print_buffer.pop(run_id)
            channel.send(ExecutionManagerProtocolKeys.UP_RUN_RESPONSE.value, (msg_id, timestamp_utc_started, timestamp_utc_completed, converted_to_str, _res))
        # SEND_FUNCTION
        elif key == ExecutionManagerProtocolKeys.SEND_FUNCTION.value:
            msg_id, func_key, func = data
            registered_functions[func_key] = func
            channel.send(ExecutionManagerProtocolKeys.UP_SEND_FUNCTION_RESPONSE.value, (msg_id,))
        # FLUSH_PRINT_BUFFER
        elif key == ExecutionManagerProtocolKeys.FLUSH_PRINT_BUFFER.value:
            msg_id, run_id = data
            if run_id not in func_print_buffer:
                raise ValueError(f"Run ID '{run_id}' not found in print buffer")
            _buffer = func_print_buffer.pop(run_id)
            channel.send(ExecutionManagerProtocolKeys.UP_FLUSH_PRINT_BUFFER_RESPONSE.value, (msg_id, run_id, _buffer))
        else:
            raise ValueError(f"Unknown execution manager protocol key: '{key}'.")

# %%
#|exporti
async def _async_worker_func(channel, worker_id):
    func_print_buffer: dict[str, list[tuple[datetime, str]]] = {}
    registered_functions: dict[str, Callable[..., Awaitable] | Callable[..., None]] = {}

    while True:
        key, data = await channel.recv()
        # RUN
        if key == ExecutionManagerProtocolKeys.RUN.value:
            msg_id, func_import_path_or_key, run_id, send_channel, args, kwargs = data
            if func_import_path_or_key in registered_functions:
                func = registered_functions[func_import_path_or_key]
            else:
                module_path, func_name = func_import_path_or_key.rsplit(".", 1)
                module = importlib.import_module(module_path)
                func = getattr(module, func_name)
            func_print_buffer[run_id] = []
            timestamp_utc_started = get_timestamp_utc()
            await channel.send(ExecutionManagerProtocolKeys.UP_RUN_STARTED.value, (msg_id, timestamp_utc_started))
            res = await _async_func_runner(
                channel=channel,
                func=func,
                send_channel=send_channel,
                print_callback=lambda s: func_print_buffer[run_id].append((get_timestamp_utc(), s)),
                args=args,
                kwargs=kwargs,
            )
            timestamp_utc_completed = get_timestamp_utc()
            converted_to_str, _res = _convert_to_str_if_not_serializable(res)
            _buffer = func_print_buffer.pop(run_id)
            await channel.send(ExecutionManagerProtocolKeys.UP_RUN_RESPONSE.value, (msg_id, timestamp_utc_started, timestamp_utc_completed, converted_to_str, _res))
        # SEND_FUNCTION
        elif key == ExecutionManagerProtocolKeys.SEND_FUNCTION.value:
            msg_id, func_key, func = data
            registered_functions[func_key] = func
            await channel.send(ExecutionManagerProtocolKeys.UP_SEND_FUNCTION_RESPONSE.value, (msg_id,))
        # FLUSH_PRINT_BUFFER
        elif key == ExecutionManagerProtocolKeys.FLUSH_PRINT_BUFFER.value:
            msg_id, run_id = data
            if run_id not in func_print_buffer:
                raise ValueError(f"Run ID '{run_id}' not found in print buffer")
            _buffer = func_print_buffer.pop(run_id)
            await channel.send(ExecutionManagerProtocolKeys.UP_FLUSH_PRINT_BUFFER_RESPONSE.value, (msg_id, run_id, _buffer))
        else:
            raise ValueError(f"Unknown execution manager protocol key: '{key}'.")

# %% [markdown]
# ## ExecutionManager

# %%
#|export
@dataclass
class JobResult:
    """Result of a job execution."""
    timestamp_utc_submitted: datetime
    timestamp_utc_started: datetime
    timestamp_utc_completed: datetime
    func_import_path_or_key: str
    pool_id: str
    worker_id: int
    converted_to_str: bool
    result: Any

@dataclass
class SubmittedJobInfo:
    """Information about a submitted job."""
    timestamp_utc_submitted: datetime
    timestamp_utc_started: datetime | None
    func_import_path_or_key: str
    pool_id: str
    worker_id: int

class RunAllocationMethod(Enum):
    """Method for allocating a job to a worker."""
    ROUND_ROBIN = "round-robin"
    RANDOM = "random"
    LEAST_BUSY = "least-busy"

# %%
#|export
PoolType = ThreadPool | MultiprocessPool | SingleWorkerPool | RemotePoolClient

class ExecutionManager:
    def __init__(self, pool_configs: dict[str, tuple[str, dict[str, Any]]]):
        """
        Create an ExecutionManager with the given pool configurations.

        Args:
            pool_configs: A dictionary mapping pool_id to (pool_type, pool_init_kwargs).
                pool_type can be "thread", "multiprocess", "remote", or "main".
                pool_init_kwargs are passed to the pool constructor (excluding worker_fn).
        """
        self._pool_configs = pool_configs
        self._pools: dict[str, PoolType] = {}
        self._msg_recv_tasks: dict[str, asyncio.Task] = {}
        self._msgs: dict[str, dict[str, asyncio.Queue]] = {}
        self._started = False

        self._worker_jobs: dict[tuple[str, str], list[SubmittedJobInfo]] = {}  # (pool_id, worker_id) -> list of SubmittedJobInfo
        self._worker_round_robin_lst: list[tuple[str, str]] = []

    async def start(self) -> None:
        """Start all pools and initialize the execution manager."""
        if self._started:
            raise RuntimeError("ExecutionManager is already started.")

        for pool_id, (pool_type, pool_init_kwargs) in self._pool_configs.items():
            if 'worker_fn' in pool_init_kwargs:
                raise ValueError("The 'worker_fn' argument should not be specified in the pool config.")

            if pool_type == "thread":
                self._pools[pool_id] = ThreadPool(**pool_init_kwargs, worker_fn=_thread_worker_func)
            elif pool_type == "multiprocess":
                self._pools[pool_id] = MultiprocessPool(**pool_init_kwargs, worker_fn=_thread_worker_func)
            elif pool_type == "remote":
                self._pools[pool_id] = RemotePoolClient(**pool_init_kwargs)
            elif pool_type == "main":
                self._pools[pool_id] = SingleWorkerPool(**pool_init_kwargs, worker_fn=_async_worker_func)
            else:
                raise ValueError(f"Unknown pool type: '{pool_type}'.")

            self._msgs[pool_id] = {}

        # Start all pools
        for pool_id, pool in self._pools.items():
            await pool.start()

        # Initialize worker jobs tracking for each worker
        for pool_id, pool in self._pools.items():
            for worker_id in range(pool.num_workers):
                self._worker_jobs[(pool_id, worker_id)] = []

        # Start message receiver tasks after pools are started
        for pool_id in self._pools:
            self._msg_recv_tasks[pool_id] = asyncio.create_task(self._msg_recv_task_func(pool_id))

        self._started = True

    async def _msg_recv_task_func(self, pool_id: str):
        pool = self._pools[pool_id]
        while True:
            msg = await pool.recv()
            msg_id = msg.data[0]
            msg.data = msg.data[1:]
            await self._msgs[pool_id][msg_id].put(msg)

    async def _send_msg(self, pool_id: str, worker_id: str, key: str, data: Any) -> str:
        pool = self._pools[pool_id]
        msg_id = str(uuid.uuid4())

        # Check if the message receiver task for the pool is running, else propagate its exception
        msg_recv_task = self._msg_recv_tasks.get(pool_id)
        if msg_recv_task is not None and msg_recv_task.done():
            exc = msg_recv_task.exception()
            if exc is not None:
                raise exc

        await pool.send(
            worker_id=worker_id,
            key=key,
            data=(msg_id, *data),
        )
        self._msgs[pool_id][msg_id] = asyncio.Queue()

        return msg_id

    async def _recv_msg(self, pool_id: str, msg_id: str, expect: ExecutionManagerProtocolKeys, close_msg_queue: bool) -> tuple[str, Any]:
        # Check if the message receiver task for the pool is running, else propagate its exception
        msg_recv_task = self._msg_recv_tasks.get(pool_id)
        if msg_recv_task is not None and msg_recv_task.done():
            exc = msg_recv_task.exception()
            if exc is not None:
                raise exc

        msg = await self._msgs[pool_id][msg_id].get()
        if close_msg_queue:
            del self._msgs[pool_id][msg_id]

        if msg.key != expect.value:
            raise ValueError(f"Expected message key '{expect.value}', got '{msg.key}'.")

        return msg

    async def run(self, pool_id: str, worker_id: str, func_import_path_or_key: str, send_channel: bool, func_args, func_kwargs) -> JobResult:
        """
        Run a function in a pool.

        Args:
            pool_id: The ID of the pool to run the function in.
            worker_id: The ID of the worker to run the function on.
            func_import_path_or_key: T  he import path or key of the function to run (for the latter, use send_function to register the function first)
            send_channel: Whether to send the worker RPC channel to the function.
            func_args: The arguments to pass to the function.
            func_kwargs: The keyword arguments to pass to the function.

        Returns:
            The result of the function.
        """
        pool = self._pools[pool_id]

        run_id = str(uuid.uuid4())
        timestamp_utc_submitted = get_timestamp_utc()
        msg_id = await self._send_msg(
            pool_id=pool_id,
            worker_id=worker_id,
            key=ExecutionManagerProtocolKeys.RUN.value,
            data=(func_import_path_or_key, run_id, send_channel, func_args, func_kwargs),
        )
        job_info = SubmittedJobInfo(
            timestamp_utc_submitted=timestamp_utc_submitted,
            timestamp_utc_started=None,
            func_import_path_or_key=func_import_path_or_key,
            pool_id=pool_id,
            worker_id=worker_id,
        )
        self._worker_jobs[(pool_id, worker_id)].append(job_info)
        if (pool_id, worker_id) in self._worker_round_robin_lst:
            self._worker_round_robin_lst.remove((pool_id, worker_id))
        self._worker_round_robin_lst.append((pool_id, worker_id))
        started_msg = await self._recv_msg(pool_id, msg_id, expect=ExecutionManagerProtocolKeys.UP_RUN_STARTED, close_msg_queue=False)
        job_info.timestamp_utc_started = started_msg.data[0]  # timestamp_utc_started
        msg = await self._recv_msg(pool_id, msg_id, expect=ExecutionManagerProtocolKeys.UP_RUN_RESPONSE, close_msg_queue=True)
        self._worker_jobs[(pool_id, worker_id)].remove(job_info)

        timestamp_utc_started, timestamp_utc_completed, converted_to_str, _res = msg.data
        return JobResult(
            timestamp_utc_submitted=job_info.timestamp_utc_submitted,
            timestamp_utc_started=job_info.timestamp_utc_started,
            timestamp_utc_completed=timestamp_utc_completed,
            func_import_path_or_key=job_info.func_import_path_or_key,
            pool_id=job_info.pool_id,
            worker_id=job_info.worker_id,
            converted_to_str=converted_to_str,
            result=_res,
        )

    async def run_allocate(
        self,
        pool_worker_ids: list[str | tuple[str, str]],
        allocation_method: RunAllocationMethod,
        func_import_path_or_key: str,
        send_channel: bool,
        func_args,
        func_kwargs,
    ) -> JobResult:
        worker_ids: list[tuple[str, int]] = []
        # Convert pool_worker_ids to a list of (pool_id, worker_id) tuples
        for _id in pool_worker_ids:
            if isinstance(_id, str):
                pool = self._pools[_id]
                for worker_id in range(pool.num_workers):
                    worker_ids.append((_id, worker_id))
            else:
                pool_id, worker_id = _id
                worker_ids.append((pool_id, worker_id))

        if not worker_ids:
            raise ValueError("No workers available for allocation.")

        # Select worker based on allocation method
        if allocation_method == RunAllocationMethod.ROUND_ROBIN:
            round_robin_lst = [p for p in self._worker_round_robin_lst if p in worker_ids]
            not_in_round_robin_lst = [p for p in worker_ids if p not in round_robin_lst]
            if not_in_round_robin_lst:
                pool_id, worker_id = not_in_round_robin_lst[0]
            else:
                pool_id, worker_id = round_robin_lst[0]

        elif allocation_method == RunAllocationMethod.RANDOM:
            pool_id, worker_id = random.choice(worker_ids)

        elif allocation_method == RunAllocationMethod.LEAST_BUSY:
            # Choose the worker with the fewest active jobs (jobs that have been submitted
            # but not yet completed). A worker with no recorded jobs is preferred.
            def active_job_count(pool_worker: tuple[str, str]) -> int:
                key = pool_worker
                jobs = self._worker_jobs.get(key, [])
                # Active = submitted but not yet completed; here we treat presence in the
                # list as active, and completion will remove the job from this structure.
                return len(jobs)

            pool_id, worker_id = min(worker_ids, key=active_job_count)

        else:
            raise ValueError(f"Unknown allocation method: '{allocation_method}'.")

        return await self.run(
            pool_id=pool_id,
            worker_id=worker_id,
            func_import_path_or_key=func_import_path_or_key,
            send_channel=send_channel,
            func_args=func_args,
            func_kwargs=func_kwargs,
        )

    async def send_function(self, pool_id: str, worker_id: str, func_key: str, func: Callable[..., Any]|Callable[..., Awaitable[Any]]) -> None:
        """
        Send a function to a worker in a pool, such that it can be run using the given key using `ExecutionManager.run`.

        Args:
            pool_id: The ID of the pool to send the function to.
            worker_id: The ID of the worker to send the function to.
            func_key: The key of the function to send. 
            func: The function to send.
        """
        # If the pool is a multiprocess pool or remote pool, the function needs to be pickleable.
        pool = self._pools[pool_id]
        if isinstance(pool, (MultiprocessPool, RemotePoolClient)):
            try:
                pickle.dumps(func)
            except pickle.PicklingError:
                raise ValueError(f"Function {func} (key = '{func_key}') is not pickleable. Cannot send to worker in pool '{pool_id}'.")

        msg_id = await self._send_msg(
            pool_id=pool_id,
            worker_id=worker_id,
            key=ExecutionManagerProtocolKeys.SEND_FUNCTION.value,
            data=(func_key, func),
        )
        await self._recv_msg(pool_id, msg_id, expect=ExecutionManagerProtocolKeys.UP_SEND_FUNCTION_RESPONSE, close_msg_queue=True)

    async def send_function_to_pool(self, pool_id: str, func_key: str, func: Callable[..., Any]|Callable[..., Awaitable[Any]]) -> None:
        """
        Send a function to all workers in a pool.

        Args:
            pool_id: The ID of the pool to send the function to.
            func_key: The key of the function to send.
            func: The function to send.
        """
        tasks = [asyncio.create_task(self.send_function(pool_id, worker_id, func_key, func)) for worker_id in range(self._pools[pool_id].num_workers)]
        await asyncio.gather(*tasks)

    async def close(self):
        """

        Close the execution manager and all its pools.
        """
        for pool in self._pools.values():
            await pool.close()
        for task in self._msg_recv_tasks.values():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                print(f"Error in msg recv task: {e}")

    @property
    def pool_ids(self) -> list[str]:
        """Get list of pool IDs."""
        return list(self._pools.keys())

    def get_num_workers(self, pool_id: str) -> int:
        """Get the number of workers in a pool."""
        return self._pools[pool_id].num_workers

    def get_worker_ids(self, pool_id: str) -> list[str]:
        """Get the list of worker IDs for a pool."""
        return [f"{pool_id}_{i}" for i in range(self._pools[pool_id].num_workers)]

    def get_worker_jobs(self, pool_id: str, worker_id: int) -> list[SubmittedJobInfo]:
        """Get the list of currently submitted jobs for a worker."""
        return list(self._worker_jobs.get((pool_id, worker_id), []))

    async def __aenter__(self) -> "ExecutionManager":
        """Context manager entry - starts the manager."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit - closes the manager."""
        await self.close()

# %% [markdown]
# ## Examples
#
# The following examples demonstrate how to use the ExecutionManager.

# %% [markdown]
# ### Example 1: Basic usage with ThreadPool
#
# This example shows the basic workflow of running a function on a worker.

# %%
def example_add(a: int, b: int) -> int:
    """A simple function that adds two numbers."""
    print(f"Adding {a} + {b}")
    return a + b

# %%
print("=" * 50)
print("Example: Basic ExecutionManager Usage")
print("=" * 50)

# Create an ExecutionManager with a thread pool
manager = ExecutionManager({
    "workers": ("thread", {"num_workers": 2}),
})

async with manager:
    print(f"Pool IDs: {manager.pool_ids}")
    print(f"Workers in 'workers' pool: {manager.get_num_workers('workers')}")

    # Send a function to all workers in the pool
    await manager.send_function_to_pool("workers", "add", example_add)

    # Run the function on worker 0
    result = await manager.run(
        pool_id="workers",
        worker_id=0,
        func_import_path_or_key="add",
        send_channel=False,
        func_args=(3, 4),
        func_kwargs={},
    )

    print(f"\nResult: {result.result}")
    print(f"Submitted at: {result.timestamp_utc_submitted}")
    print(f"Started at: {result.timestamp_utc_started}")
    print(f"Completed at: {result.timestamp_utc_completed}")
    print(f"Was converted to str: {result.converted_to_str}")

print("\nDone!")

# %% [markdown]
# ### Example 2: Running multiple jobs with allocation
#
# This example shows how to use the allocation methods to distribute work.

# %%
def example_multiply(x: int, y: int) -> int:
    """Multiply two numbers."""
    import time
    time.sleep(0.1)  # Simulate some work
    print(f"Multiplying {x} * {y}")
    return x * y

print("=" * 50)
print("Example: Job Allocation")
print("=" * 50)

manager = ExecutionManager({
    "compute": ("thread", {"num_workers": 3}),
})

async with manager:
    # Send the multiply function to all workers
    await manager.send_function_to_pool("compute", "multiply", example_multiply)

    # Run multiple jobs using round-robin allocation
    print("\nRunning 6 jobs with ROUND_ROBIN allocation:")
    tasks = []
    for i in range(6):
        task = asyncio.create_task(
            manager.run_allocate(
                pool_worker_ids=["compute"],  # Use all workers in "compute" pool
                allocation_method=RunAllocationMethod.ROUND_ROBIN,
                func_import_path_or_key="multiply",
                send_channel=False,
                func_args=(i, i + 1),
                func_kwargs={},
            )
        )
        tasks.append(task)

    results = await asyncio.gather(*tasks)
    for i, result in enumerate(results):
        print(f"  Job {i}: {result.result} (worker {result.worker_id})")

print("\nDone!")

# %% [markdown]
# ### Example 3: Functions with print capture
#
# The ExecutionManager captures print statements from executed functions.

# %%
def example_verbose_function(name: str) -> str:
    """A function that prints during execution."""
    print(f"Hello, {name}!")
    print("Processing...")
    print("Done processing.")
    return f"Greeted {name}"

print("=" * 50)
print("Example: Print Capture")
print("=" * 50)

manager = ExecutionManager({
    "pool": ("thread", {"num_workers": 1}),
})

async with manager:
    await manager.send_function_to_pool("pool", "greet", example_verbose_function)

    result = await manager.run(
        pool_id="pool",
        worker_id=0,
        func_import_path_or_key="greet",
        send_channel=False,
        func_args=("World",),
        func_kwargs={},
    )

    print(f"\nFunction result: {result.result}")
    # Note: Print output is captured per-run and can be flushed
    # using flush_print_buffer if the function is still running
