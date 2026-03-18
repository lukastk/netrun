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
# Transforms text to uppercase.

# %%
#|default_exp transform
#|export_as_func true

# %%
#|set_func_signature
async def main(text: str, ctx, print) -> str:
    """Transform text to uppercase."""
    ...

# %% [markdown]
# ## Function body

# %%
#|export
import asyncio

print(f"Uppercasing {len(text)} chars")
ctx.log("Uppercasing", input_length=len(text))
await asyncio.sleep(1)  # Simulate processing

# %%
#|export
result = text.upper()
ctx.log("Uppercase complete")
print(f"Done: {result[:40]}...")

# %%
#|export
result #|func_return_line
