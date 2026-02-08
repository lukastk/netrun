/**
 * Central field registration file.
 *
 * Every field of NetConfig, NodeConfig, and NodeExecutionConfig must appear
 * here. When a new field is added to a Python model:
 *   1. The test in tests/test_config_schema.py will fail
 *   2. Add the field to KNOWN_FIELDS in that test
 *   3. Register it here with the appropriate strategy
 *
 * Strategies:
 *   'auto'   — AutoConfigFields renders it based on schema category
 *   'custom' — A specific component handles it manually
 *   'ignore' — Not shown in the UI (complex internal fields, etc.)
 */
import { registerField } from '$lib/stores/schemaStore';

// ==========================================================================
// NetConfig
// ==========================================================================

registerField('NetConfig', 'project_root', 'custom', 'Sidebar.svelte');
registerField('NetConfig', 'pools', 'custom', 'PoolsSection.svelte');
registerField('NetConfig', 'graph', 'ignore');
registerField('NetConfig', 'extra', 'ignore');
registerField('NetConfig', 'default_pool_allocation_method', 'auto');
registerField('NetConfig', 'node_vars', 'custom', 'NodeVariablesSection.svelte');
registerField('NetConfig', 'dead_letter_queue', 'auto');
registerField('NetConfig', 'dead_letter_path', 'auto');
registerField('NetConfig', 'dead_letter_callback', 'custom', 'NetSettingsSection.svelte');
registerField('NetConfig', 'output_queues', 'ignore');
registerField('NetConfig', 'error_on_undeclared_output', 'auto');
registerField('NetConfig', 'type_checking_enabled', 'auto');
registerField('NetConfig', 'propagate_exceptions', 'auto');
registerField('NetConfig', 'print_exceptions', 'auto');

// ==========================================================================
// NodeConfig
// ==========================================================================

registerField('NodeConfig', 'type', 'ignore');
registerField('NodeConfig', 'name', 'custom', 'Sidebar.svelte');
registerField('NodeConfig', 'in_ports', 'custom', 'Sidebar.svelte');
registerField('NodeConfig', 'out_ports', 'custom', 'Sidebar.svelte');
registerField('NodeConfig', 'in_salvo_conditions', 'custom', 'SalvoConditionsSection.svelte');
registerField('NodeConfig', 'out_salvo_conditions', 'custom', 'SalvoConditionsSection.svelte');
registerField('NodeConfig', 'execution_config', 'custom', 'NodeExecutionSection.svelte');
registerField('NodeConfig', 'extra', 'ignore');
registerField('NodeConfig', 'factory', 'custom', 'Sidebar.svelte');
registerField('NodeConfig', 'factory_args', 'custom', 'Sidebar.svelte');

// ==========================================================================
// NodeExecutionConfig
// ==========================================================================

registerField('NodeExecutionConfig', 'pools', 'custom', 'NodeExecutionSection.svelte');
registerField('NodeExecutionConfig', 'exec_node_func', 'custom', 'NodeExecutionSection.svelte');
registerField('NodeExecutionConfig', 'start_node_func', 'custom', 'NodeExecutionSection.svelte');
registerField('NodeExecutionConfig', 'stop_node_func', 'custom', 'NodeExecutionSection.svelte');
registerField('NodeExecutionConfig', 'on_node_failure', 'custom', 'NodeExecutionSection.svelte');
registerField('NodeExecutionConfig', 'defer_startup', 'auto');
registerField('NodeExecutionConfig', 'max_parallel_epochs', 'auto');
registerField('NodeExecutionConfig', 'max_epochs', 'auto');
registerField('NodeExecutionConfig', 'rate_limit_per_second', 'auto');
registerField('NodeExecutionConfig', 'defer_net_actions', 'auto');
registerField('NodeExecutionConfig', 'retries', 'auto');
registerField('NodeExecutionConfig', 'retry_wait', 'auto');
registerField('NodeExecutionConfig', 'timeout', 'auto');
registerField('NodeExecutionConfig', 'capture_prints', 'auto');
registerField('NodeExecutionConfig', 'print_flush_interval', 'auto');
registerField('NodeExecutionConfig', 'print_buffer_max_size', 'auto');
registerField('NodeExecutionConfig', 'print_echo_stdout', 'auto');
registerField('NodeExecutionConfig', 'pool_allocation_method', 'auto');
registerField('NodeExecutionConfig', 'node_vars', 'custom', 'NodeVariablesSection.svelte');
registerField('NodeExecutionConfig', 'type_checking_enabled', 'auto');
registerField('NodeExecutionConfig', 'propagate_exceptions', 'auto');
registerField('NodeExecutionConfig', 'print_exceptions', 'auto');
