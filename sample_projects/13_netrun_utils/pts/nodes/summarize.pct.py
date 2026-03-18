# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # nodes.summarize
#
# Produces a summary and statistics for the transformed text.
# Has two output ports: `summary` and `stats`.

# %%
#|default_exp summarize
#|export_as_func true

# %%
#|set_func_signature
def main(text: str, ctx, print) -> {"summary": str, "stats": str}:
    """Produce a summary and statistics for the text."""
    ...

# %% [markdown]
# ## Dev setup
#
# `set_node_func_args` runs the full upstream chain (`fetch_text` -> `transform`)
# to produce realistic input for this node.

# %%
from netrun_utils.dev import set_node_func_args, show_node_vars
from pathlib import Path

config_path = Path("../..") / "main.netrun.json"
set_node_func_args("summarize", config_path)
show_node_vars("summarize", config_path)

# %%
text  # Inspect: this is the transformed text from upstream

# %% [markdown]
# ## Function body

# %%
#|export
print(f"Summarizing text of length {len(text)}")
ctx.log("Summarizing", input_length=len(text))

# %%
#|export
word_count = len(text.split())
char_count = len(text)
summary = f"[{word_count} words] {text[:60]}..."
stats = f"chars={char_count}, words={word_count}, avg_word_len={char_count / max(word_count, 1):.1f}"

# %%
print(f"Summary: {summary}")
print(f"Stats: {stats}")

# %%
#|export
ctx.log("Summary complete", word_count=word_count, char_count=char_count)

# %%
#|export
{"summary": summary, "stats": stats} #|func_return_line
