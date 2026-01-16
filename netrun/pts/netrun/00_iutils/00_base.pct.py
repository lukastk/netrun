# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp _iutils._base

# %%
#|hide
from nblite import nbl_export, show_doc; nbl_export();
import netrun._iutils._base as this_module

# %%
#|hide
show_doc(this_module.patch_to)

# %%
#|export
def patch_to(cls):
    """Patch a class to add a method."""

    def decorator(func):
        setattr(cls, func.__name__, func)
        return func

    return decorator
