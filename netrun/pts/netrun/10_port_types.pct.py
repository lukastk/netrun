# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Port Types
#
# Type checking for packet values on input and output ports.

# %%
#|default_exp port_types

# %%
#|hide
from nblite import nbl_export, show_doc; nbl_export();

# %%
#|export
from dataclasses import dataclass
from typing import Any, Dict, Optional, Type, Union


@dataclass
class PortTypeSpec:
    """
    Specification for a port's expected value type.

    Supports multiple forms of type checking:
    - Class name string: checks value.__class__.__name__ == name
    - Class object: isinstance check by default
    - Dict with options for exact type match or subclass check
    """
    type_class: Optional[Type] = None
    type_name: Optional[str] = None
    use_isinstance: bool = True
    use_subclass: bool = False

    @classmethod
    def from_spec(cls, spec: Union[str, Type, Dict, None]) -> Optional["PortTypeSpec"]:
        """
        Create a PortTypeSpec from various input formats.

        Args:
            spec: One of:
                - None: No type checking
                - str: Class name to match against __class__.__name__
                - Type: Class to check with isinstance (default)
                - dict: {"class": Type, "isinstance": bool} or
                        {"class": Type, "subclass": bool}

        Returns:
            PortTypeSpec or None if no type checking needed
        """
        if spec is None:
            return None

        if isinstance(spec, str):
            # String: check class name
            return cls(type_name=spec)

        if isinstance(spec, type):
            # Class: isinstance check
            return cls(type_class=spec, use_isinstance=True)

        if isinstance(spec, dict):
            type_class = spec.get("class")
            if type_class is None:
                return None

            # Check for isinstance option (default True)
            use_isinstance = spec.get("isinstance", True)

            # Check for subclass option
            use_subclass = spec.get("subclass", False)

            return cls(
                type_class=type_class,
                use_isinstance=use_isinstance,
                use_subclass=use_subclass,
            )

        return None

    def check(self, value: Any) -> bool:
        """
        Check if a value matches this type specification.

        Args:
            value: The value to check

        Returns:
            True if the value matches, False otherwise
        """
        if self.type_name is not None:
            # String-based class name check
            return value.__class__.__name__ == self.type_name

        if self.type_class is not None:
            if self.use_subclass:
                # Subclass check
                return issubclass(type(value), self.type_class)
            elif self.use_isinstance:
                # isinstance check (default)
                return isinstance(value, self.type_class)
            else:
                # Exact type match
                return type(value) == self.type_class

        # No type specification
        return True

    def get_expected_type_str(self) -> str:
        """Get a string representation of the expected type."""
        if self.type_name is not None:
            return self.type_name

        if self.type_class is not None:
            type_name = self.type_class.__name__
            if self.use_subclass:
                return f"subclass of {type_name}"
            elif self.use_isinstance:
                return f"instance of {type_name}"
            else:
                return f"exactly {type_name}"

        return "any"


def check_value_type(
    value: Any,
    type_spec: Union[str, Type, Dict, None],
) -> tuple[bool, str, str]:
    """
    Check if a value matches a type specification.

    Args:
        value: The value to check
        type_spec: The type specification (see PortTypeSpec.from_spec)

    Returns:
        Tuple of (matches, expected_type_str, actual_type_str)
    """
    spec = PortTypeSpec.from_spec(type_spec)

    if spec is None:
        return (True, "any", type(value).__name__)

    matches = spec.check(value)
    expected = spec.get_expected_type_str()
    actual = type(value).__name__

    return (matches, expected, actual)

# %%
#|export
class PortTypeRegistry:
    """
    Registry for port types in a Net.

    Stores type specifications for input and output ports and provides
    type checking methods.
    """

    def __init__(self):
        # Maps (node_name, port_name) -> type_spec for input ports
        self._input_port_types: Dict[tuple[str, str], Any] = {}
        # Maps (node_name, port_name) -> type_spec for output ports
        self._output_port_types: Dict[tuple[str, str], Any] = {}

    def set_input_port_type(
        self,
        node_name: str,
        port_name: str,
        type_spec: Union[str, Type, Dict, None],
    ) -> None:
        """Set the type specification for an input port."""
        self._input_port_types[(node_name, port_name)] = type_spec

    def set_output_port_type(
        self,
        node_name: str,
        port_name: str,
        type_spec: Union[str, Type, Dict, None],
    ) -> None:
        """Set the type specification for an output port."""
        self._output_port_types[(node_name, port_name)] = type_spec

    def get_input_port_type(
        self,
        node_name: str,
        port_name: str,
    ) -> Optional[Any]:
        """Get the type specification for an input port."""
        return self._input_port_types.get((node_name, port_name))

    def get_output_port_type(
        self,
        node_name: str,
        port_name: str,
    ) -> Optional[Any]:
        """Get the type specification for an output port."""
        return self._output_port_types.get((node_name, port_name))

    def check_input_port_value(
        self,
        node_name: str,
        port_name: str,
        value: Any,
    ) -> tuple[bool, str, str]:
        """
        Check if a value matches the input port's type.

        Returns:
            Tuple of (matches, expected_type_str, actual_type_str)
        """
        type_spec = self.get_input_port_type(node_name, port_name)
        return check_value_type(value, type_spec)

    def check_output_port_value(
        self,
        node_name: str,
        port_name: str,
        value: Any,
    ) -> tuple[bool, str, str]:
        """
        Check if a value matches the output port's type.

        Returns:
            Tuple of (matches, expected_type_str, actual_type_str)
        """
        type_spec = self.get_output_port_type(node_name, port_name)
        return check_value_type(value, type_spec)

    def clear(self) -> None:
        """Clear all type specifications."""
        self._input_port_types.clear()
        self._output_port_types.clear()
