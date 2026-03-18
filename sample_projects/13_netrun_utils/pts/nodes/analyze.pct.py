# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # nodes.analyze
#
# Analyzes text and produces statistics.

# %%
#|default_exp analyze
#|export_as_func true

# %%
#|set_func_signature
async def main(text: str, ctx, print) -> str:
    """Analyze text and produce statistics."""
    ...

# %% [markdown]
# ## Function body

# %%
#|export
import asyncio

print(f"Analyzing {len(text)} chars")
ctx.log("Analyzing", input_length=len(text))
await asyncio.sleep(3)  # Simulate heavier processing

# %%
#|export
words = text.split()
word_count = len(words)
char_count = len(text)
avg_word = char_count / max(word_count, 1)
unique_words = len(set(w.lower() for w in words))

result = f"chars={char_count}, words={word_count}, unique={unique_words}, avg_len={avg_word:.1f}"
ctx.log("Analysis complete", word_count=word_count, unique_words=unique_words)
print(f"Stats: {result}")

# %%
#|export
result #|func_return_line
