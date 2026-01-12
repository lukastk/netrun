# Example 00: Basic Setup

This example demonstrates the foundational components of `netrun`:

- Creating a network graph with nodes and edges
- Creating a `Net` instance
- Configuring nodes with execution functions
- Setting node configuration options
- Using the `PacketValueStore` for packet value management

## What This Example Shows

1. **Graph Creation**: How to define nodes with input/output ports and salvo conditions
2. **Net Construction**: Creating a Net from a graph with various configuration options
3. **Node Configuration**: Using `set_node_exec()` and `set_node_config()` to configure nodes
4. **Value Storage**: Direct usage of `PacketValueStore` for storing and retrieving values

## Note

This example only demonstrates setup and configuration. Actual execution of the network
(running nodes, flowing packets) will be shown in later examples after Milestone 3 is complete.

## Running

```bash
./run_example.sh
```
