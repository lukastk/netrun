# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Example 11: Complete Application - Data Processing Pipeline
#
# This example demonstrates a complete, real-world data processing pipeline that
# combines all features from the previous milestones:
#
# - **TOML DSL**: Define the network structure in a human-readable format
# - **Thread Pools**: Parallel processing with configurable pools
# - **Rate Limiting**: Control processing rate to avoid overwhelming downstream systems
# - **Error Handling**: Retries with dead letter queue for failed items
# - **Data Types**: Structured data with dataclasses
# - **History/Logging**: Track all operations for debugging
# - **Checkpointing**: Save and resume pipeline state
#
# ## The Application: ETL Data Pipeline
#
# We'll build an Extract-Transform-Load (ETL) pipeline that:
# 1. **Extracts** data records from a source
# 2. **Validates** records (routing invalid ones to error handling)
# 3. **Transforms** valid records (enrichment, normalization)
# 4. **Loads** transformed records to a destination
#
# ```
#                    /--> Transformer --> Loader
# Extractor --> Validator
#                    \--> ErrorHandler
# ```

# %%
#|default_exp 11_complete_application

# %%
#|export
import tempfile
import time
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from netrun import (
    # Graph building
    Graph,
    Node,
    Edge,
    Port,
    PortType,
    PortRef,
    PortState,
    PortSlotSpec,
    MaxSalvos,
    SalvoCondition,
    SalvoConditionTerm,
    # Net and configuration
    Net,
    NetState,
    NodeConfig,
    # DSL
    parse_toml_string,
    net_config_to_toml,
)

# %% [markdown]
# ## Part 1: Define the Pipeline Using TOML DSL
#
# We define our ETL pipeline using the TOML DSL format for clarity and portability.

# %%
#|export
ETL_PIPELINE_TOML = '''
[net]
on_error = "continue"

[net.thread_pools.workers]
size = 4

[net.thread_pools.io]
size = 2

# =============================================================================
# Extractor Node - Generates data records
# =============================================================================
[nodes.Extractor]
out_ports = { out = {} }

[nodes.Extractor.out_salvo_conditions.send]
max_salvos = "infinite"
ports = "out"
when = "nonempty(out)"

# =============================================================================
# Validator Node - Validates records, routes to transform or error
# =============================================================================
[nodes.Validator]
in_ports = { in = {} }
out_ports = { valid = {}, invalid = {} }

[nodes.Validator.in_salvo_conditions.receive]
max_salvos = 1
ports = "in"
when = "nonempty(in)"

[nodes.Validator.out_salvo_conditions.send_valid]
max_salvos = "infinite"
ports = "valid"
when = "nonempty(valid)"

[nodes.Validator.out_salvo_conditions.send_invalid]
max_salvos = "infinite"
ports = "invalid"
when = "nonempty(invalid)"

[nodes.Validator.options]
pool = "workers"

# =============================================================================
# Transformer Node - Enriches and normalizes data
# =============================================================================
[nodes.Transformer]
in_ports = { in = { slots = 10 } }
out_ports = { out = {} }

[nodes.Transformer.in_salvo_conditions.receive]
max_salvos = 1
ports = "in"
when = "nonempty(in)"

[nodes.Transformer.out_salvo_conditions.send]
max_salvos = "infinite"
ports = "out"
when = "nonempty(out)"

[nodes.Transformer.options]
pool = "workers"
retries = 3
defer_net_actions = true

# =============================================================================
# Loader Node - Loads data to destination
# =============================================================================
[nodes.Loader]
in_ports = { in = {} }

[nodes.Loader.in_salvo_conditions.receive]
max_salvos = 1
ports = "in"
when = "nonempty(in)"

[nodes.Loader.options]
pool = "io"
rate_limit_per_second = 100

# =============================================================================
# ErrorHandler Node - Handles invalid/failed records
# =============================================================================
[nodes.ErrorHandler]
in_ports = { in = {} }

[nodes.ErrorHandler.in_salvo_conditions.receive]
max_salvos = 1
ports = "in"
when = "nonempty(in)"

# =============================================================================
# Edges
# =============================================================================
[[edges]]
from = "Extractor.out"
to = "Validator.in"

[[edges]]
from = "Validator.valid"
to = "Transformer.in"

[[edges]]
from = "Validator.invalid"
to = "ErrorHandler.in"

[[edges]]
from = "Transformer.out"
to = "Loader.in"
'''

# Parse the TOML definition
config = parse_toml_string(ETL_PIPELINE_TOML)
print("Pipeline configuration loaded from TOML:")
print(f"  Nodes: {list(config.graph.nodes().keys())}")
print(f"  Edges: {len(config.graph.edges())}")
print(f"  Thread pools: {list(config.thread_pools.keys())}")

# %% [markdown]
# ## Part 2: Define Data Types
#
# We use dataclasses to represent our data records with type checking.

# %%
#|export
@dataclass
class RawRecord:
    """A raw data record from the source."""
    id: int
    name: str
    value: float
    category: Optional[str] = None


@dataclass
class ValidatedRecord:
    """A validated record ready for transformation."""
    id: int
    name: str
    value: float
    category: str
    validation_timestamp: float


@dataclass
class TransformedRecord:
    """A fully transformed record ready for loading."""
    id: int
    name: str
    normalized_value: float
    category: str
    enriched_data: Dict[str, Any]
    transform_timestamp: float


@dataclass
class ErrorRecord:
    """A record that failed processing."""
    original_record: Any
    error_type: str
    error_message: str
    timestamp: float

# %% [markdown]
# ## Part 3: Create the Net and Define Execution Functions
#
# We'll create execution functions for each node demonstrating different features.

# %%
#|export
# Create a temporary directory for history and checkpoints
temp_dir = tempfile.mkdtemp(prefix="etl_pipeline_")
history_file = Path(temp_dir) / "history.jsonl"
checkpoint_dir = Path(temp_dir) / "checkpoint"

print(f"Working directory: {temp_dir}")

# Create the Net from the parsed config
net = Net(
    config.graph,
    on_error=config.on_error,
    thread_pools=config.thread_pools,
    history_file=str(history_file),
    consumed_packet_storage=True,
    consumed_packet_storage_limit=1000,
)

# Apply node configs from TOML
for node_name, node_config in config.node_configs.items():
    net.set_node_config(node_name, **node_config)

# Storage for results and metrics
loaded_records: List[TransformedRecord] = []
error_records: List[ErrorRecord] = []
metrics = {
    "extracted": 0,
    "validated": 0,
    "invalid": 0,
    "transformed": 0,
    "loaded": 0,
    "errors": 0,
}

# %% [markdown]
# ### Extractor Node
# Generates sample data records for the pipeline.

# %%
#|export
def extractor_exec(ctx, packets):
    """Extract data records from source."""
    # Simulate extracting records from a data source
    sample_records = [
        RawRecord(1, "Alice", 100.5, "A"),
        RawRecord(2, "Bob", 200.0, "B"),
        RawRecord(3, "Charlie", -50.0, None),  # Invalid: negative value
        RawRecord(4, "Diana", 150.75, "A"),
        RawRecord(5, "Eve", 0.0, ""),  # Invalid: empty category
        RawRecord(6, "Frank", 300.25, "C"),
        RawRecord(7, "Grace", 175.0, "B"),
        RawRecord(8, "Henry", 999.99, "A"),
    ]

    print(f"[Extractor] Extracting {len(sample_records)} records...")

    for record in sample_records:
        pkt = ctx.create_packet(record)
        ctx.load_output_port("out", pkt)
        ctx.send_output_salvo("send")
        metrics["extracted"] += 1

    print(f"[Extractor] Sent {len(sample_records)} records to validation")

net.set_node_exec("Extractor", extractor_exec)

# %% [markdown]
# ### Validator Node
# Validates records and routes them to either transformation or error handling.

# %%
#|export
def validator_exec(ctx, packets):
    """Validate incoming records."""
    for port_name, pkts in packets.items():
        for pkt in pkts:
            record: RawRecord = ctx.consume_packet(pkt)

            # Validation rules
            errors = []
            if record.value < 0:
                errors.append("Value must be non-negative")
            if not record.category:
                errors.append("Category is required")

            if errors:
                # Invalid record - send to error handler
                error_record = ErrorRecord(
                    original_record=record,
                    error_type="validation_error",
                    error_message="; ".join(errors),
                    timestamp=time.time(),
                )
                out_pkt = ctx.create_packet(error_record)
                ctx.load_output_port("invalid", out_pkt)
                ctx.send_output_salvo("send_invalid")
                metrics["invalid"] += 1
                print(f"[Validator] Record {record.id} invalid: {errors}")
            else:
                # Valid record - send to transformer
                validated = ValidatedRecord(
                    id=record.id,
                    name=record.name,
                    value=record.value,
                    category=record.category,
                    validation_timestamp=time.time(),
                )
                out_pkt = ctx.create_packet(validated)
                ctx.load_output_port("valid", out_pkt)
                ctx.send_output_salvo("send_valid")
                metrics["validated"] += 1
                print(f"[Validator] Record {record.id} validated")

net.set_node_exec("Validator", validator_exec)

# %% [markdown]
# ### Transformer Node
# Transforms records with enrichment and normalization.

# %%
#|export
def transformer_exec(ctx, packets):
    """Transform and enrich validated records."""
    for port_name, pkts in packets.items():
        for pkt in pkts:
            record: ValidatedRecord = ctx.consume_packet(pkt)

            # Normalize value (scale to 0-1 range, assuming max of 1000)
            normalized_value = min(record.value / 1000.0, 1.0)

            # Enrich with additional computed data
            enriched_data = {
                "value_tier": "high" if record.value > 200 else "medium" if record.value > 100 else "low",
                "category_code": ord(record.category[0]) if record.category else 0,
                "processing_node": ctx.node_name,
            }

            transformed = TransformedRecord(
                id=record.id,
                name=record.name,
                normalized_value=normalized_value,
                category=record.category,
                enriched_data=enriched_data,
                transform_timestamp=time.time(),
            )

            out_pkt = ctx.create_packet(transformed)
            ctx.load_output_port("out", out_pkt)
            ctx.send_output_salvo("send")
            metrics["transformed"] += 1
            print(f"[Transformer] Record {record.id} transformed (tier: {enriched_data['value_tier']})")

net.set_node_exec("Transformer", transformer_exec)

# %% [markdown]
# ### Loader Node
# Loads transformed records to the destination.

# %%
#|export
def loader_exec(ctx, packets):
    """Load transformed records to destination."""
    for port_name, pkts in packets.items():
        for pkt in pkts:
            record: TransformedRecord = ctx.consume_packet(pkt)
            loaded_records.append(record)
            metrics["loaded"] += 1
            print(f"[Loader] Record {record.id} loaded to destination")

net.set_node_exec("Loader", loader_exec)

# %% [markdown]
# ### Error Handler Node
# Handles invalid and failed records.

# %%
#|export
def error_handler_exec(ctx, packets):
    """Handle error records."""
    for port_name, pkts in packets.items():
        for pkt in pkts:
            record: ErrorRecord = ctx.consume_packet(pkt)
            error_records.append(record)
            metrics["errors"] += 1
            print(f"[ErrorHandler] Captured error for record: {record.error_message}")

net.set_node_exec("ErrorHandler", error_handler_exec)

# %% [markdown]
# ## Part 4: Run the Pipeline

# %%
#|export
print("\n" + "="*60)
print("Starting ETL Pipeline")
print("="*60 + "\n")

# Inject the extractor epoch to start the pipeline
net.inject_source_epoch("Extractor")

# Run the pipeline
start_time = time.time()
net.start()
elapsed = time.time() - start_time

print("\n" + "="*60)
print("Pipeline Complete")
print("="*60)

# %% [markdown]
# ## Part 5: Review Results

# %%
#|export
print("\n--- Pipeline Metrics ---")
for metric, value in metrics.items():
    print(f"  {metric}: {value}")
print(f"  elapsed_time: {elapsed:.3f}s")

print("\n--- Successfully Loaded Records ---")
for record in loaded_records:
    print(f"  ID={record.id}, Name={record.name}, "
          f"NormValue={record.normalized_value:.3f}, "
          f"Tier={record.enriched_data['value_tier']}")

print("\n--- Error Records ---")
for record in error_records:
    orig = record.original_record
    print(f"  ID={orig.id}, Error: {record.error_message}")

# %% [markdown]
# ## Part 6: Demonstrate Checkpointing
#
# We can save the pipeline state and configuration for later resumption.

# %%
#|export
print("\n--- Demonstrating Checkpointing ---")

# Pause the net before saving checkpoint
net.pause()
print(f"Net paused. State: {net.state}")

# Save the net definition (without runtime state)
definition_path = Path(temp_dir) / "pipeline_definition.toml"
net.save_definition(definition_path)
print(f"Definition saved to: {definition_path}")

# Read back and display a snippet
with open(definition_path) as f:
    toml_content = f.read()
print(f"\nSaved TOML definition ({len(toml_content)} chars):")
print(toml_content[:500] + "..." if len(toml_content) > 500 else toml_content)

# %% [markdown]
# ## Part 7: Verify History Recording
#
# The pipeline records all events for debugging and auditing.

# %%
#|export
print("\n--- Event History ---")
history = net._event_history
print(f"Total entries in memory: {len(history)}")

# Show recent entries
entries = history.get_entries(limit=5)
if entries:
    print("Recent entries:")
    for entry in entries:
        print(f"  [{entry.entry_type}] {entry.event_type or entry.action_type}: {entry.id[:8]}...")

# %% [markdown]
# ## Summary
#
# This complete application demonstrated:
#
# 1. **TOML DSL**: Defined the entire pipeline structure in a readable TOML format
# 2. **Thread Pools**: Used separate pools for workers (CPU-bound) and I/O operations
# 3. **Rate Limiting**: Controlled the Loader node's throughput
# 4. **Error Handling**: Validation errors routed to a dedicated error handler
# 5. **Retries**: Transformer configured with automatic retries on failure
# 6. **Data Types**: Used dataclasses with type annotations
# 7. **History**: Tracked all operations for debugging
# 8. **Checkpointing**: Saved pipeline definition for portability
#
# Key patterns demonstrated:
# - Separation of valid/invalid data flows
# - Parallel processing with thread pools
# - Configuration-driven pipeline design
# - Graceful error handling without pipeline failure
# - ETL pipeline architecture

# %%
#|export
# Cleanup (optional - comment out to inspect files)
import shutil
# shutil.rmtree(temp_dir)
print(f"\nTemp files available at: {temp_dir}")
