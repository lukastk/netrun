# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # nodes.transform
#
# Transforms text based on the configured `transform_mode` node variable.
# Receives text from `fetch_text` upstream.

# %%
#|default_exp transform
#|export_as_func true

# %%
#|set_func_signature
def main(text: str, ctx, print) -> str:
    """Transform text based on configured mode."""
    ...

# %% [markdown]
# ## Dev setup
#
# `set_node_func_args` will run the upstream `fetch_text` node (or use cached
# data) to populate `text`, and create a `ctx` with the resolved node variables.

# %%
from netrun_utils.dev import set_node_func_args, show_node_vars
from pathlib import Path

config_path = Path("../..") / "main.netrun.json"
set_node_func_args("transform", config_path)
show_node_vars("transform", config_path)

# %% [markdown]
# Now `text`, `ctx`, and `print` are available as globals. We can develop the
# function body cell by cell, inspecting intermediate results as we go.

# %%
text  # Inspect the input we received from upstream

# %% [markdown]
# ## Function body

# %%
#|export
import time

mode = ctx.vars.get("transform_mode", "upper")
print(f"Transforming with mode={mode}")
ctx.log("Transforming", mode=mode, input_length=len(text))
time.sleep(2)  # Simulate processing

# %%
#|export
if mode == "upper":
    result = text.upper()
elif mode == "reverse":
    result = text[::-1]
elif mode == "title":
    result = text.title()
else:
    result = text

# %%
#|export
ctx.log("Transform complete", output_length=len(result))
print(f"Output: {result[:50]}...")

# %%
result  # Inspect the output before returning

# %%
#|export
result #|func_return_line
