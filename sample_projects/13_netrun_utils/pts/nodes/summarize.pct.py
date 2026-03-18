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
# Produces a summary of the transformed text.

# %%
#|default_exp summarize
#|export_as_func true

# %%
#|set_func_signature
async def main(text: str, ctx, print) -> str:
    """Produce a summary of the text."""
    ...

# %% [markdown]
# ## Function body

# %%
#|export
import asyncio

print(f"Summarizing {len(text)} chars")
ctx.log("Summarizing", input_length=len(text))
await asyncio.sleep(1.5)  # Simulate processing

# %%
#|export
word_count = len(text.split())
result = f"[{word_count} words] {text[:60]}..."
ctx.log("Summary complete", word_count=word_count)
print(f"Summary: {result}")

# %%
#|export
result #|func_return_line
