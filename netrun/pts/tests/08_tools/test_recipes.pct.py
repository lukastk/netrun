# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp tools.test_recipes

# %%
#|hide
from nblite import nbl_export; nbl_export();

# %%
#|export
import pytest
import tempfile
from pathlib import Path
from netrun.tools._recipes import (
    load_recipe_module,
    get_recipe_prompts,
    execute_recipe,
)

# %%
#|export
def test_load_recipe_module_not_found():
    with pytest.raises(FileNotFoundError):
        load_recipe_module("/nonexistent/recipe.py")


def test_load_recipe_module_not_python():
    with tempfile.NamedTemporaryFile(suffix=".txt") as f:
        with pytest.raises(ValueError, match=".py"):
            load_recipe_module(f.name)


def test_load_recipe_module_basic():
    with tempfile.NamedTemporaryFile(suffix=".py", mode="w", delete=False) as f:
        f.write("VALUE = 42\n")
        f.flush()
        module = load_recipe_module(f.name)
        assert module.VALUE == 42
    Path(f.name).unlink()


def test_get_recipe_prompts_no_function():
    with tempfile.NamedTemporaryFile(suffix=".py", mode="w", delete=False) as f:
        f.write("# no get_prompts\n")
        f.flush()
        prompts = get_recipe_prompts(f.name, {})
        assert prompts == []
    Path(f.name).unlink()


def test_get_recipe_prompts_with_function():
    with tempfile.NamedTemporaryFile(suffix=".py", mode="w", delete=False) as f:
        f.write(
            "def get_prompts(config):\n"
            "    return [{'name': 'count', 'label': 'Count'}]\n"
        )
        f.flush()
        prompts = get_recipe_prompts(f.name, {})
        assert len(prompts) == 1
        assert prompts[0].name == "count"
    Path(f.name).unlink()


def test_execute_recipe_basic():
    with tempfile.NamedTemporaryFile(suffix=".py", mode="w", delete=False) as f:
        f.write(
            "def run(config, inputs):\n"
            "    config['modified'] = True\n"
            "    return config\n"
        )
        f.flush()
        result = execute_recipe(f.name, {"nodes": []}, {})
        assert result["modified"] is True
        assert result["nodes"] == []
    Path(f.name).unlink()


def test_execute_recipe_no_run():
    with tempfile.NamedTemporaryFile(suffix=".py", mode="w", delete=False) as f:
        f.write("# no run function\n")
        f.flush()
        with pytest.raises(AttributeError, match="run"):
            execute_recipe(f.name, {}, {})
    Path(f.name).unlink()


def test_execute_recipe_bad_return():
    with tempfile.NamedTemporaryFile(suffix=".py", mode="w", delete=False) as f:
        f.write("def run(config, inputs): return 42\n")
        f.flush()
        with pytest.raises(TypeError, match="dict"):
            execute_recipe(f.name, {}, {})
    Path(f.name).unlink()
