# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # set_node_func_args
#
# Inject a node's expected inputs into the caller's namespace for interactive
# notebook development. Allows developing and testing node functions cell-by-cell
# with realistic data.

# %%
#|default_exp dev.node_args

# %%
#|export
import builtins
import sys
from pathlib import Path

from netrun.core import Net, NetConfig
from netrun.net._net._context import NodeExecutionContext

from netrun_utils.dev._helpers import (
    _load_config,
    _resolve_node_name,
    _get_node_func,
    _get_merged_node_vars,
    _get_input_salvo,
    _run_async,
)

# %%
#|export
def set_node_func_args(
    node_name: str,
    config_path: str | Path,
    *,
    global_node_vars: dict | None = None,
    node_vars: dict | None = None,
    return_args: bool = False,
    verbose: bool = True,
) -> dict | None:
    """Set up a node's function arguments for interactive development.

    Loads the config, resolves the node's inputs (from cache or by running
    upstream), imports the node function, and either injects the function's
    arguments into the caller's globals or returns them as a dict.

    Args:
        node_name: The node name (bare name is resolved against subgraph prefixes).
        config_path: Path to the netrun config file (.json or .toml).
        global_node_vars: Optional net-level variable overrides (passed to NetConfig.from_file).
        node_vars: Optional per-node variable overrides (passed to NetConfig.from_file).
        return_args: If True, return the args dict instead of injecting into globals.
        verbose: If True, print status messages during execution.

    Returns:
        If return_args=True, a dict of {param_name: value}.
        Otherwise None (values are injected into the caller's globals).
    """
    # Load and resolve config
    config = _load_config(config_path, global_node_vars=global_node_vars, node_vars=node_vars)
    full_name = _resolve_node_name(config, node_name)

    if verbose and full_name != node_name:
        print(f"Resolved '{node_name}' -> '{full_name}'")

    # Get the node function and detect special parameters
    func, special_params = _get_node_func(config, full_name)

    # Get input data (from cache or by running upstream)
    async def _run():
        net = Net(config, run_source_nodes=False)
        await net.start(run_source_nodes=False)
        try:
            return await _get_input_salvo(net, full_name, verbose=verbose)
        finally:
            await net.stop()

    input_data = _run_async(_run())

    # Build the arguments dict
    args = dict(input_data)

    # Add special parameters
    if "ctx" in special_params:
        merged = _get_merged_node_vars(config, full_name)
        node_var_objects = {name: var for name, (var, _source) in merged.items()}
        ctx = NodeExecutionContext(
            epoch_id="dev-0",
            node_name=full_name,
            _node_vars=node_var_objects if node_var_objects else None,
        )
        args["ctx"] = ctx

    if "print" in special_params:
        args["print"] = builtins.print

    if "log" in special_params and "ctx" in args:
        args["log"] = args["ctx"].log

    if return_args:
        return args

    # Inject into caller's globals
    caller_globals = sys._getframe(1).f_globals
    for key, value in args.items():
        caller_globals[key] = value

    if verbose:
        print(f"Injected: {', '.join(sorted(args.keys()))}")

    return None
