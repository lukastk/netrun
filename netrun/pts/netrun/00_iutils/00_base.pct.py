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
#|export
import datetime

# %%
#|hide
show_doc(this_module.get_timestamp_utc)

# %%
#|export
def get_timestamp_utc():
    """Get the current timestamp in UTC."""
    return datetime.datetime.now(datetime.UTC)
