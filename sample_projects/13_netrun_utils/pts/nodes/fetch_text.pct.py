# ---
# jupyter:
#   kernelspec:
#     display_name: netrun-utils-demo (3.13.5)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # nodes.fetch_text
#
# Source node that fetches text data. The text source is configured via the
# `text_source` node variable.

# %%
#|default_exp fetch_text
#|export_as_func true

# %%
#|top_export
import time

# %%
#|set_func_signature
def main(ctx, print) -> str:
    """Fetch text data from a configured source."""
    ...

# %% [markdown]
# ## Dev setup
#
# Call `set_node_func_args` to populate this notebook's namespace with the
# inputs this node would receive in a real run. Since `fetch_text` is a source
# node (no input ports), only `ctx` and `print` are injected.

# %%
from netrun_utils.dev import set_node_func_args, show_node_vars
from pathlib import Path

config_path = Path("../..") / "main.netrun.json"
set_node_func_args("fetch_text", config_path)
show_node_vars("fetch_text", config_path)

# %% [markdown]
# ## Function body

# %%
#|export
source = ctx.vars.get("text_source", "default")
print(f"Fetching text from source: {source}")
ctx.log("Fetching text", source=source)

# %%
#|export
time.sleep(2)  # Simulate I/O

texts = {
    "greeting": "Hello world! This is a sample text for processing.",
    "lorem": "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
    "default": "The quick brown fox jumps over the lazy dog.",
}
result = texts.get(source, texts["default"])

# %%
#|export
ctx.log("Fetch complete", length=len(result))
print(f"Fetched {len(result)} characters")

# %%
#|export
result #|func_return_line
