# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp _iutils.pickling

# %%
#|hide
from nblite import nbl_export, show_doc; nbl_export();
import netrun._iutils.pickling as this_module

# %%
#|export
import pickle
import pickletools
from enum import Enum
from typing import Any

# %%
#|hide
show_doc(this_module.PicklingMethod)

# %%
#|export
class PicklingMethod(Enum):
    """Pickling method to use for serialization."""
    pickle = "pickle"
    dill = "dill"
    cloudpickle = "cloudpickle"

# %%
#|hide
show_doc(this_module.pickle_data)

# %%
#|export
def pickle_data(data: Any, method: PicklingMethod = PicklingMethod.pickle, **kwargs) -> bytes:
    """Serialize data to bytes using the specified pickling method.

    Args:
        data: The data to serialize.
        method: The pickling method to use.
        **kwargs: Additional arguments passed to the pickler.
            For pickle: ``protocol`` (default 4), ``optimize`` (default True).
            For dill/cloudpickle: passed through to ``dumps()``.

    Returns:
        The serialized bytes.

    Raises:
        ImportError: If dill or cloudpickle is not installed.
    """
    if method == PicklingMethod.pickle:
        protocol = kwargs.pop("protocol", 4)
        optimize = kwargs.pop("optimize", True)
        result = pickle.dumps(data, protocol=protocol, **kwargs)
        if optimize:
            result = pickletools.optimize(result)
        return result

    elif method == PicklingMethod.dill:
        try:
            import dill
        except ImportError:
            raise ImportError(
                "dill is required for PicklingMethod.dill. "
                "Install it with: pip install dill  (or: pip install netrun[serialization])"
            )
        return dill.dumps(data, **kwargs)

    elif method == PicklingMethod.cloudpickle:
        try:
            import cloudpickle
        except ImportError:
            raise ImportError(
                "cloudpickle is required for PicklingMethod.cloudpickle. "
                "Install it with: pip install cloudpickle  (or: pip install netrun[serialization])"
            )
        return cloudpickle.dumps(data, **kwargs)

    else:
        raise ValueError(f"Unknown pickling method: {method}")

# %%
#|hide
show_doc(this_module.unpickle_data)

# %%
#|export
def unpickle_data(data: bytes, method: PicklingMethod = PicklingMethod.pickle) -> Any:
    """Deserialize bytes back to a Python object.

    Args:
        data: The bytes to deserialize.
        method: The pickling method that was used for serialization.
            For cloudpickle, stdlib pickle.loads() is used (cloudpickle output
            is stdlib-compatible).

    Returns:
        The deserialized Python object.

    Raises:
        ImportError: If dill is not installed and method is dill.
    """
    if method == PicklingMethod.pickle or method == PicklingMethod.cloudpickle:
        return pickle.loads(data)

    elif method == PicklingMethod.dill:
        try:
            import dill
        except ImportError:
            raise ImportError(
                "dill is required for PicklingMethod.dill. "
                "Install it with: pip install dill  (or: pip install netrun[serialization])"
            )
        return dill.loads(data)

    else:
        raise ValueError(f"Unknown pickling method: {method}")

# %% [markdown]
# ## Quick test

# %%
data = {"hello": "world", "numbers": [1, 2, 3]}
for method in [PicklingMethod.pickle]:
    serialized = pickle_data(data, method)
    deserialized = unpickle_data(serialized, method)
    assert deserialized == data
    print(f"{method.value}: {len(serialized)} bytes")
