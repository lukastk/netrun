# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Tests for Net Config Module

# %%
#|default_exp net.test_config

# %%
#|export
import pytest
from netrun.net.config import (
    # Port slot spec
    PortSlotSpecInfiniteConfig,
    PortSlotSpecFiniteConfig,
    PortConfig,
    # Port state
    PortStateEmptyConfig,
    PortStateFullConfig,
    PortStateNonEmptyConfig,
    PortStateNonFullConfig,
    PortStateEqualsConfig,
    PortStateLessThanConfig,
    PortStateGreaterThanConfig,
    PortStateEqualsOrLessThanConfig,
    PortStateEqualsOrGreaterThanConfig,
    # Packet count
    PacketCountAllConfig,
    PacketCountNConfig,
    # Max salvos
    MaxSalvosInfiniteConfig,
    MaxSalvosFiniteConfig,
    # Salvo condition term
    SalvoConditionTermTrueConfig,
    SalvoConditionTermFalseConfig,
    SalvoConditionTermPortConfig,
    SalvoConditionTermAndConfig,
    SalvoConditionTermOrConfig,
    SalvoConditionTermNotConfig,
    # Salvo condition
    SalvoConditionConfig,
    # Edge and node
    PortRefConfig,
    EdgeConfig,
    NodeGraphConfig,
    # Graph
    GraphConfig,
)
import netrun_sim

# %% [markdown]
# ## Port Slot Spec Tests

# %%
#|export
def test_port_slot_spec_infinite_config():
    """Test PortSlotSpecInfiniteConfig creation and conversion."""
    config = PortSlotSpecInfiniteConfig()
    assert config.type == "infinite"

    # Test conversion to netrun_sim
    result = config.to_netrun_sim()
    assert result == netrun_sim.PortSlotSpec.Infinite

# %%
test_port_slot_spec_infinite_config();

# %%
#|export
def test_port_slot_spec_finite_config():
    """Test PortSlotSpecFiniteConfig creation and conversion."""
    config = PortSlotSpecFiniteConfig(capacity=5)
    assert config.type == "finite"
    assert config.capacity == 5

    # Test conversion to netrun_sim
    result = config.to_netrun_sim()
    assert result.capacity == 5

# %%
test_port_slot_spec_finite_config();

# %%
#|export
def test_port_config_default():
    """Test PortConfig with default (infinite) slot spec."""
    config = PortConfig()
    assert config.slots_spec.type == "infinite"

    port = config.to_netrun_sim()
    assert port.slots_spec == netrun_sim.PortSlotSpec.Infinite

# %%
test_port_config_default();

# %%
#|export
def test_port_config_finite():
    """Test PortConfig with finite slot spec."""
    config = PortConfig(slots_spec=PortSlotSpecFiniteConfig(capacity=10))
    assert config.slots_spec.type == "finite"
    assert config.slots_spec.capacity == 10

    port = config.to_netrun_sim()
    assert port.slots_spec.capacity == 10

# %%
test_port_config_finite();

# %% [markdown]
# ## Port State Tests

# %%
#|export
def test_port_state_empty_config():
    """Test PortStateEmptyConfig."""
    config = PortStateEmptyConfig()
    assert config.type == "empty"
    result = config.to_netrun_sim()
    assert result == netrun_sim.PortState.Empty

# %%
test_port_state_empty_config();

# %%
#|export
def test_port_state_full_config():
    """Test PortStateFullConfig."""
    config = PortStateFullConfig()
    assert config.type == "full"
    result = config.to_netrun_sim()
    assert result == netrun_sim.PortState.Full

# %%
test_port_state_full_config();

# %%
#|export
def test_port_state_non_empty_config():
    """Test PortStateNonEmptyConfig."""
    config = PortStateNonEmptyConfig()
    assert config.type == "non_empty"
    result = config.to_netrun_sim()
    assert result == netrun_sim.PortState.NonEmpty

# %%
test_port_state_non_empty_config();

# %%
#|export
def test_port_state_non_full_config():
    """Test PortStateNonFullConfig."""
    config = PortStateNonFullConfig()
    assert config.type == "non_full"
    result = config.to_netrun_sim()
    assert result == netrun_sim.PortState.NonFull

# %%
test_port_state_non_full_config();

# %%
#|export
def test_port_state_equals_config():
    """Test PortStateEqualsConfig."""
    config = PortStateEqualsConfig(value=3)
    assert config.type == "equals"
    assert config.value == 3
    result = config.to_netrun_sim()
    assert result.kind == "equals"
    assert result.value == 3

# %%
test_port_state_equals_config();

# %%
#|export
def test_port_state_less_than_config():
    """Test PortStateLessThanConfig."""
    config = PortStateLessThanConfig(value=5)
    assert config.type == "less_than"
    assert config.value == 5
    result = config.to_netrun_sim()
    assert result.kind == "less_than"
    assert result.value == 5

# %%
test_port_state_less_than_config();

# %%
#|export
def test_port_state_greater_than_config():
    """Test PortStateGreaterThanConfig."""
    config = PortStateGreaterThanConfig(value=2)
    assert config.type == "greater_than"
    assert config.value == 2
    result = config.to_netrun_sim()
    assert result.kind == "greater_than"
    assert result.value == 2

# %%
test_port_state_greater_than_config();

# %%
#|export
def test_port_state_equals_or_less_than_config():
    """Test PortStateEqualsOrLessThanConfig."""
    config = PortStateEqualsOrLessThanConfig(value=4)
    assert config.type == "equals_or_less_than"
    assert config.value == 4
    result = config.to_netrun_sim()
    assert result.kind == "equals_or_less_than"
    assert result.value == 4

# %%
test_port_state_equals_or_less_than_config();

# %%
#|export
def test_port_state_equals_or_greater_than_config():
    """Test PortStateEqualsOrGreaterThanConfig."""
    config = PortStateEqualsOrGreaterThanConfig(value=1)
    assert config.type == "equals_or_greater_than"
    assert config.value == 1
    result = config.to_netrun_sim()
    assert result.kind == "equals_or_greater_than"
    assert result.value == 1

# %%
test_port_state_equals_or_greater_than_config();

# %% [markdown]
# ## Packet Count Tests

# %%
#|export
def test_packet_count_all_config():
    """Test PacketCountAllConfig."""
    config = PacketCountAllConfig()
    assert config.type == "all"
    result = config.to_netrun_sim()
    assert result == netrun_sim.PacketCount.All

# %%
test_packet_count_all_config();

# %%
#|export
def test_packet_count_n_config():
    """Test PacketCountNConfig."""
    config = PacketCountNConfig(count=5)
    assert config.type == "count"
    assert config.count == 5
    result = config.to_netrun_sim()
    assert result.count == 5

# %%
test_packet_count_n_config();

# %% [markdown]
# ## Max Salvos Tests

# %%
#|export
def test_max_salvos_infinite_config():
    """Test MaxSalvosInfiniteConfig."""
    config = MaxSalvosInfiniteConfig()
    assert config.type == "infinite"
    result = config.to_netrun_sim()
    assert result == netrun_sim.MaxSalvos.Infinite

# %%
test_max_salvos_infinite_config();

# %%
#|export
def test_max_salvos_finite_config():
    """Test MaxSalvosFiniteConfig."""
    config = MaxSalvosFiniteConfig(max=3)
    assert config.type == "finite"
    assert config.max == 3
    result = config.to_netrun_sim()
    assert result.max == 3

# %%
test_max_salvos_finite_config();

# %% [markdown]
# ## Salvo Condition Term Tests

# %%
#|export
def test_salvo_condition_term_true_config():
    """Test SalvoConditionTermTrueConfig."""
    config = SalvoConditionTermTrueConfig()
    assert config.type == "true"
    result = config.to_netrun_sim()
    assert result.kind == "True"

# %%
test_salvo_condition_term_true_config();

# %%
#|export
def test_salvo_condition_term_false_config():
    """Test SalvoConditionTermFalseConfig."""
    config = SalvoConditionTermFalseConfig()
    assert config.type == "false"
    result = config.to_netrun_sim()
    assert result.kind == "False"

# %%
test_salvo_condition_term_false_config();

# %%
#|export
def test_salvo_condition_term_port_config():
    """Test SalvoConditionTermPortConfig."""
    config = SalvoConditionTermPortConfig(
        port_name="in",
        state=PortStateNonEmptyConfig()
    )
    assert config.type == "port"
    assert config.port_name == "in"
    assert config.state.type == "non_empty"

    result = config.to_netrun_sim()
    assert result.kind == "Port"
    assert result.get_port_name() == "in"

# %%
test_salvo_condition_term_port_config();

# %%
#|export
def test_salvo_condition_term_and_config():
    """Test SalvoConditionTermAndConfig."""
    config = SalvoConditionTermAndConfig(
        terms=[
            SalvoConditionTermPortConfig(port_name="in1", state=PortStateNonEmptyConfig()),
            SalvoConditionTermPortConfig(port_name="in2", state=PortStateNonEmptyConfig()),
        ]
    )
    assert config.type == "and"
    assert len(config.terms) == 2

    result = config.to_netrun_sim()
    assert result.kind == "And"
    terms = result.get_terms()
    assert len(terms) == 2

# %%
test_salvo_condition_term_and_config();

# %%
#|export
def test_salvo_condition_term_or_config():
    """Test SalvoConditionTermOrConfig."""
    config = SalvoConditionTermOrConfig(
        terms=[
            SalvoConditionTermPortConfig(port_name="in1", state=PortStateNonEmptyConfig()),
            SalvoConditionTermPortConfig(port_name="in2", state=PortStateNonEmptyConfig()),
        ]
    )
    assert config.type == "or"
    assert len(config.terms) == 2

    result = config.to_netrun_sim()
    assert result.kind == "Or"
    terms = result.get_terms()
    assert len(terms) == 2

# %%
test_salvo_condition_term_or_config();

# %%
#|export
def test_salvo_condition_term_not_config():
    """Test SalvoConditionTermNotConfig."""
    config = SalvoConditionTermNotConfig(
        term=SalvoConditionTermPortConfig(port_name="in", state=PortStateEmptyConfig())
    )
    assert config.type == "not"

    result = config.to_netrun_sim()
    assert result.kind == "Not"
    inner = result.get_inner()
    assert inner.kind == "Port"

# %%
test_salvo_condition_term_not_config();

# %%
#|export
def test_salvo_condition_term_nested():
    """Test nested salvo condition terms."""
    config = SalvoConditionTermAndConfig(
        terms=[
            SalvoConditionTermOrConfig(
                terms=[
                    SalvoConditionTermPortConfig(port_name="a", state=PortStateNonEmptyConfig()),
                    SalvoConditionTermPortConfig(port_name="b", state=PortStateNonEmptyConfig()),
                ]
            ),
            SalvoConditionTermNotConfig(
                term=SalvoConditionTermPortConfig(port_name="c", state=PortStateFullConfig())
            ),
        ]
    )
    assert config.type == "and"
    assert config.terms[0].type == "or"
    assert config.terms[1].type == "not"

    result = config.to_netrun_sim()
    assert result.kind == "And"

# %%
test_salvo_condition_term_nested();

# %% [markdown]
# ## Salvo Condition Tests

# %%
#|export
def test_salvo_condition_config():
    """Test SalvoConditionConfig."""
    config = SalvoConditionConfig(
        max_salvos=MaxSalvosFiniteConfig(max=1),
        ports={"in": PacketCountAllConfig()},
        term=SalvoConditionTermPortConfig(port_name="in", state=PortStateNonEmptyConfig()),
    )
    assert config.max_salvos.type == "finite"
    assert "in" in config.ports
    assert config.term.type == "port"

    result = config.to_netrun_sim()
    assert result.max_salvos.max == 1
    assert "in" in result.ports

# %%
test_salvo_condition_config();

# %%
#|export
def test_salvo_condition_config_multiple_ports():
    """Test SalvoConditionConfig with multiple ports."""
    config = SalvoConditionConfig(
        max_salvos=MaxSalvosFiniteConfig(max=1),
        ports={
            "in1": PacketCountAllConfig(),
            "in2": PacketCountNConfig(count=2),
        },
        term=SalvoConditionTermAndConfig(
            terms=[
                SalvoConditionTermPortConfig(port_name="in1", state=PortStateNonEmptyConfig()),
                SalvoConditionTermPortConfig(port_name="in2", state=PortStateEqualsOrGreaterThanConfig(value=2)),
            ]
        ),
    )
    assert len(config.ports) == 2

    result = config.to_netrun_sim()
    assert len(result.ports) == 2

# %%
test_salvo_condition_config_multiple_ports();

# %% [markdown]
# ## Port Ref and Edge Tests

# %%
#|export
def test_port_ref_config():
    """Test PortRefConfig."""
    config = PortRefConfig(node_name="A", port_type="output", port_name="out")
    assert config.node_name == "A"
    assert config.port_type == "output"
    assert config.port_name == "out"

    result = config.to_netrun_sim()
    assert result.node_name == "A"
    assert result.port_type == netrun_sim.PortType.Output
    assert result.port_name == "out"

# %%
test_port_ref_config();

# %%
#|export
def test_port_ref_config_input():
    """Test PortRefConfig for input port."""
    config = PortRefConfig(node_name="B", port_type="input", port_name="in")
    result = config.to_netrun_sim()
    assert result.port_type == netrun_sim.PortType.Input

# %%
test_port_ref_config_input();

# %%
#|export
def test_edge_config_full_form():
    """Test EdgeConfig with full PortRefConfig objects."""
    config = EdgeConfig(
        source=PortRefConfig(node_name="A", port_type="output", port_name="out"),
        target=PortRefConfig(node_name="B", port_type="input", port_name="in"),
    )
    source = config.get_source()
    target = config.get_target()
    assert source.node_name == "A"
    assert target.node_name == "B"

    result = config.to_netrun_sim()
    assert result.source.node_name == "A"
    assert result.target.node_name == "B"

# %%
test_edge_config_full_form();

# %%
#|export
def test_edge_config_shorthand():
    """Test EdgeConfig with shorthand string notation."""
    config = EdgeConfig(source_str="A.out", target_str="B.in")
    source = config.get_source()
    target = config.get_target()
    assert source.node_name == "A"
    assert source.port_name == "out"
    assert source.port_type == "output"
    assert target.node_name == "B"
    assert target.port_name == "in"
    assert target.port_type == "input"

    result = config.to_netrun_sim()
    assert result.source.node_name == "A"
    assert result.target.node_name == "B"

# %%
test_edge_config_shorthand();

# %%
#|export
def test_edge_config_validation_neither():
    """Test EdgeConfig raises error when neither form provided."""
    with pytest.raises(ValueError, match="Must provide either"):
        EdgeConfig()

# %%
test_edge_config_validation_neither();

# %%
#|export
def test_edge_config_validation_both():
    """Test EdgeConfig raises error when both forms provided."""
    with pytest.raises(ValueError, match="Cannot provide both"):
        EdgeConfig(
            source=PortRefConfig(node_name="A", port_type="output", port_name="out"),
            target=PortRefConfig(node_name="B", port_type="input", port_name="in"),
            source_str="A.out",
            target_str="B.in",
        )

# %%
test_edge_config_validation_both();

# %%
#|export
def test_edge_config_invalid_shorthand():
    """Test EdgeConfig raises error for invalid shorthand format."""
    config = EdgeConfig(source_str="invalid", target_str="B.in")
    with pytest.raises(ValueError, match="Invalid port string"):
        config.get_source()

# %%
test_edge_config_invalid_shorthand();

# %% [markdown]
# ## Node Graph Config Tests

# %%
#|export
def test_node_graph_config_minimal():
    """Test NodeGraphConfig with minimal configuration."""
    config = NodeGraphConfig(name="A")
    assert config.name == "A"
    assert config.in_ports == {}
    assert config.out_ports == {}
    assert config.in_salvo_conditions == {}
    assert config.out_salvo_conditions == {}

    result = config.to_netrun_sim()
    assert result.name == "A"

# %%
test_node_graph_config_minimal();

# %%
#|export
def test_node_graph_config_with_ports():
    """Test NodeGraphConfig with ports."""
    config = NodeGraphConfig(
        name="B",
        in_ports={"in1": PortConfig(), "in2": PortConfig(slots_spec=PortSlotSpecFiniteConfig(capacity=5))},
        out_ports={"out": PortConfig()},
    )
    assert len(config.in_ports) == 2
    assert len(config.out_ports) == 1

    result = config.to_netrun_sim()
    assert len(result.in_ports) == 2
    assert len(result.out_ports) == 1

# %%
test_node_graph_config_with_ports();

# %%
#|export
def test_node_graph_config_with_salvo_conditions():
    """Test NodeGraphConfig with salvo conditions."""
    config = NodeGraphConfig(
        name="C",
        in_ports={"in": PortConfig()},
        out_ports={"out": PortConfig()},
        in_salvo_conditions={
            "default": SalvoConditionConfig(
                max_salvos=MaxSalvosFiniteConfig(max=1),
                ports={"in": PacketCountAllConfig()},
                term=SalvoConditionTermPortConfig(port_name="in", state=PortStateNonEmptyConfig()),
            ),
        },
        out_salvo_conditions={
            "send": SalvoConditionConfig(
                max_salvos=MaxSalvosInfiniteConfig(),
                ports={"out": PacketCountAllConfig()},
                term=SalvoConditionTermPortConfig(port_name="out", state=PortStateNonEmptyConfig()),
            ),
        },
    )
    assert len(config.in_salvo_conditions) == 1
    assert len(config.out_salvo_conditions) == 1

    result = config.to_netrun_sim()
    assert len(result.in_salvo_conditions) == 1
    assert len(result.out_salvo_conditions) == 1

# %%
test_node_graph_config_with_salvo_conditions();

# %% [markdown]
# ## Graph Config Tests

# %%
#|export
def test_graph_config_simple():
    """Test GraphConfig with simple A -> B graph."""
    config = GraphConfig(
        nodes=[
            NodeGraphConfig(name="A", out_ports={"out": PortConfig()}),
            NodeGraphConfig(
                name="B",
                in_ports={"in": PortConfig()},
                in_salvo_conditions={
                    "default": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={"in": PacketCountAllConfig()},
                        term=SalvoConditionTermPortConfig(port_name="in", state=PortStateNonEmptyConfig()),
                    ),
                },
            ),
        ],
        edges=[EdgeConfig(source_str="A.out", target_str="B.in")],
    )
    assert len(config.nodes) == 2
    assert len(config.edges) == 1

    graph = config.get_graph()
    assert len(graph.nodes()) == 2
    assert len(graph.edges()) == 1
    assert len(graph.validate()) == 0

# %%
test_graph_config_simple();

# %%
#|export
def test_graph_config_no_edges():
    """Test GraphConfig with no edges (disconnected nodes)."""
    config = GraphConfig(
        nodes=[
            NodeGraphConfig(name="A", out_ports={"out": PortConfig()}),
            NodeGraphConfig(name="B", in_ports={"in": PortConfig()}),
        ],
        edges=[],
    )
    graph = config.get_graph()
    assert len(graph.nodes()) == 2
    assert len(graph.edges()) == 0

# %%
test_graph_config_no_edges();

# %%
#|export
def test_graph_config_complex():
    """Test GraphConfig with multiple nodes and edges."""
    config = GraphConfig(
        nodes=[
            NodeGraphConfig(name="Source", out_ports={"out": PortConfig()}),
            NodeGraphConfig(
                name="Processor",
                in_ports={"in1": PortConfig(), "in2": PortConfig()},
                out_ports={"out": PortConfig()},
                in_salvo_conditions={
                    "both_ready": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={"in1": PacketCountAllConfig(), "in2": PacketCountAllConfig()},
                        term=SalvoConditionTermAndConfig(
                            terms=[
                                SalvoConditionTermPortConfig(port_name="in1", state=PortStateNonEmptyConfig()),
                                SalvoConditionTermPortConfig(port_name="in2", state=PortStateNonEmptyConfig()),
                            ]
                        ),
                    ),
                },
            ),
            NodeGraphConfig(
                name="Sink",
                in_ports={"in": PortConfig()},
                in_salvo_conditions={
                    "default": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={"in": PacketCountAllConfig()},
                        term=SalvoConditionTermPortConfig(port_name="in", state=PortStateNonEmptyConfig()),
                    ),
                },
            ),
        ],
        edges=[
            EdgeConfig(source_str="Source.out", target_str="Processor.in1"),
            EdgeConfig(source_str="Processor.out", target_str="Sink.in"),
        ],
    )

    graph = config.get_graph()
    assert len(graph.nodes()) == 3
    assert len(graph.edges()) == 2
    assert len(graph.validate()) == 0

# %%
test_graph_config_complex();

# %%
#|export
def test_graph_config_validates_correctly():
    """Test that GraphConfig produces graphs that validate correctly."""
    # This config has a valid graph
    valid_config = GraphConfig(
        nodes=[
            NodeGraphConfig(name="A", out_ports={"out": PortConfig()}),
            NodeGraphConfig(name="B", in_ports={"in": PortConfig()}),
        ],
        edges=[EdgeConfig(source_str="A.out", target_str="B.in")],
    )
    assert len(valid_config.get_graph().validate()) == 0

# %%
test_graph_config_validates_correctly();

# %% [markdown]
# ## JSON Serialization Tests

# %%
#|export
def test_port_slot_spec_json_roundtrip():
    """Test PortSlotSpec configs JSON roundtrip."""
    configs = [
        PortSlotSpecInfiniteConfig(),
        PortSlotSpecFiniteConfig(capacity=10),
    ]
    for config in configs:
        json_str = config.model_dump_json()
        loaded = type(config).model_validate_json(json_str)
        assert loaded == config

# %%
test_port_slot_spec_json_roundtrip();

# %%
#|export
def test_port_state_json_roundtrip():
    """Test PortState configs JSON roundtrip."""
    configs = [
        PortStateEmptyConfig(),
        PortStateFullConfig(),
        PortStateNonEmptyConfig(),
        PortStateNonFullConfig(),
        PortStateEqualsConfig(value=5),
        PortStateLessThanConfig(value=3),
        PortStateGreaterThanConfig(value=2),
        PortStateEqualsOrLessThanConfig(value=4),
        PortStateEqualsOrGreaterThanConfig(value=1),
    ]
    for config in configs:
        json_str = config.model_dump_json()
        loaded = type(config).model_validate_json(json_str)
        assert loaded == config

# %%
test_port_state_json_roundtrip();

# %%
#|export
def test_packet_count_json_roundtrip():
    """Test PacketCount configs JSON roundtrip."""
    configs = [
        PacketCountAllConfig(),
        PacketCountNConfig(count=5),
    ]
    for config in configs:
        json_str = config.model_dump_json()
        loaded = type(config).model_validate_json(json_str)
        assert loaded == config

# %%
test_packet_count_json_roundtrip();

# %%
#|export
def test_max_salvos_json_roundtrip():
    """Test MaxSalvos configs JSON roundtrip."""
    configs = [
        MaxSalvosInfiniteConfig(),
        MaxSalvosFiniteConfig(max=3),
    ]
    for config in configs:
        json_str = config.model_dump_json()
        loaded = type(config).model_validate_json(json_str)
        assert loaded == config

# %%
test_max_salvos_json_roundtrip();

# %%
#|export
def test_salvo_condition_term_json_roundtrip():
    """Test SalvoConditionTerm configs JSON roundtrip."""
    configs = [
        SalvoConditionTermTrueConfig(),
        SalvoConditionTermFalseConfig(),
        SalvoConditionTermPortConfig(port_name="in", state=PortStateNonEmptyConfig()),
        SalvoConditionTermAndConfig(
            terms=[
                SalvoConditionTermPortConfig(port_name="a", state=PortStateNonEmptyConfig()),
                SalvoConditionTermPortConfig(port_name="b", state=PortStateNonEmptyConfig()),
            ]
        ),
        SalvoConditionTermOrConfig(
            terms=[
                SalvoConditionTermPortConfig(port_name="a", state=PortStateNonEmptyConfig()),
                SalvoConditionTermPortConfig(port_name="b", state=PortStateNonEmptyConfig()),
            ]
        ),
        SalvoConditionTermNotConfig(
            term=SalvoConditionTermPortConfig(port_name="in", state=PortStateEmptyConfig())
        ),
    ]
    for config in configs:
        json_str = config.model_dump_json()
        loaded = type(config).model_validate_json(json_str)
        assert loaded == config

# %%
test_salvo_condition_term_json_roundtrip();

# %%
#|export
def test_graph_config_json_roundtrip():
    """Test GraphConfig complete JSON roundtrip."""
    config = GraphConfig(
        nodes=[
            NodeGraphConfig(name="A", out_ports={"out": PortConfig()}),
            NodeGraphConfig(
                name="B",
                in_ports={"in": PortConfig(slots_spec=PortSlotSpecFiniteConfig(capacity=5))},
                in_salvo_conditions={
                    "default": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={"in": PacketCountAllConfig()},
                        term=SalvoConditionTermPortConfig(port_name="in", state=PortStateNonEmptyConfig()),
                    ),
                },
            ),
        ],
        edges=[EdgeConfig(source_str="A.out", target_str="B.in")],
    )

    json_str = config.model_dump_json()
    loaded = GraphConfig.model_validate_json(json_str)

    # Verify structure matches
    assert len(loaded.nodes) == len(config.nodes)
    assert len(loaded.edges) == len(config.edges)

    # Verify the loaded config produces a valid graph
    graph = loaded.get_graph()
    assert len(graph.validate()) == 0

# %%
test_graph_config_json_roundtrip();

# %%
#|export
def test_graph_config_json_roundtrip_complex():
    """Test GraphConfig JSON roundtrip with complex nested terms."""
    config = GraphConfig(
        nodes=[
            NodeGraphConfig(name="Source", out_ports={"out": PortConfig()}),
            NodeGraphConfig(
                name="Processor",
                in_ports={"in1": PortConfig(), "in2": PortConfig()},
                out_ports={"out": PortConfig()},
                in_salvo_conditions={
                    "complex": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={"in1": PacketCountAllConfig(), "in2": PacketCountNConfig(count=2)},
                        term=SalvoConditionTermAndConfig(
                            terms=[
                                SalvoConditionTermOrConfig(
                                    terms=[
                                        SalvoConditionTermPortConfig(port_name="in1", state=PortStateNonEmptyConfig()),
                                        SalvoConditionTermPortConfig(port_name="in1", state=PortStateFullConfig()),
                                    ]
                                ),
                                SalvoConditionTermNotConfig(
                                    term=SalvoConditionTermPortConfig(port_name="in2", state=PortStateEmptyConfig())
                                ),
                            ]
                        ),
                    ),
                },
                out_salvo_conditions={
                    "send": SalvoConditionConfig(
                        max_salvos=MaxSalvosInfiniteConfig(),
                        ports={"out": PacketCountAllConfig()},
                        term=SalvoConditionTermPortConfig(port_name="out", state=PortStateNonEmptyConfig()),
                    ),
                },
            ),
        ],
        edges=[EdgeConfig(source_str="Source.out", target_str="Processor.in1")],
    )

    json_str = config.model_dump_json(indent=2)
    loaded = GraphConfig.model_validate_json(json_str)

    # Verify structure matches
    assert len(loaded.nodes) == len(config.nodes)

    # Verify the nested term structure
    processor = next(n for n in loaded.nodes if n.name == "Processor")
    complex_cond = processor.in_salvo_conditions["complex"]
    assert complex_cond.term.type == "and"
    assert len(complex_cond.term.terms) == 2
    assert complex_cond.term.terms[0].type == "or"
    assert complex_cond.term.terms[1].type == "not"

# %%
test_graph_config_json_roundtrip_complex();

# %% [markdown]
# ## Integration Tests

# %%
#|export
def test_config_to_netrun_sim_integration():
    """Test that configs properly integrate with netrun_sim."""
    config = GraphConfig(
        nodes=[
            NodeGraphConfig(name="A", out_ports={"out": PortConfig()}),
            NodeGraphConfig(
                name="B",
                in_ports={"in": PortConfig()},
                in_salvo_conditions={
                    "default": SalvoConditionConfig(
                        max_salvos=MaxSalvosFiniteConfig(max=1),
                        ports={"in": PacketCountAllConfig()},
                        term=SalvoConditionTermPortConfig(port_name="in", state=PortStateNonEmptyConfig()),
                    ),
                },
            ),
        ],
        edges=[EdgeConfig(source_str="A.out", target_str="B.in")],
    )

    graph = config.get_graph()

    # Create a NetSim and verify it works
    net = netrun_sim.NetSim(graph)

    # Create a packet
    response, events = net.do_action(netrun_sim.NetAction.create_packet())
    packet_id = response.packet_id

    # Place it on the edge
    edge = graph.edges()[0]
    net.do_action(netrun_sim.NetAction.transport_packet_to_location(
        packet_id,
        netrun_sim.PacketLocation.edge(edge)
    ))

    # Run the network
    net.do_action(netrun_sim.NetAction.run_step())

    # Check that an epoch was created
    startable = net.get_startable_epochs()
    assert len(startable) == 1

# %%
test_config_to_netrun_sim_integration();
