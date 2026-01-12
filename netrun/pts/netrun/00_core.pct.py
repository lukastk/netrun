# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %%
#|default_exp core

# %%
#|hide
from nblite import nbl_export, show_doc; nbl_export();

# %%
#|export
from typing import Callable, Optional
from dataclasses import dataclass

import netrun_sim
from netrun_sim import Port, PortType, PortRef, SalvoConditionTerm, SalvoCondition, Edge
