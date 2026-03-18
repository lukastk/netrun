# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # nodes.reverse
#
# Reverses the input text.

# %%
#|default_exp reverse
#|export_as_func true

# %%
#|set_func_signature
def main(text: str, ctx, print) -> str:
    """Reverse the input text."""
    ...

# %% [markdown]
# ## Function body

# %%
#|export
import time

print(f"Reversing {len(text)} chars")
ctx.log("Reversing", input_length=len(text))
time.sleep(2)  # Simulate processing

# %%
#|export
result = text[::-1]
ctx.log("Reverse complete")
print(f"Done: {result[:40]}...")

# %%
#|export
result #|func_return_line
