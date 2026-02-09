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
registerField('NetConfig', 'output_queues', 'custom', 'OutputQueuesSection.svelte');
registerField('NetConfig', 'error_on_undeclared_output', 'auto');
registerField('NetConfig', 'type_checking_enabled', 'auto');
registerField('NetConfig', 'propagate_exceptions', 'auto');
registerField('NetConfig', 'print_exceptions', 'auto');
registerField('NetConfig', 'storage', 'custom', 'StorageSection.svelte');

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
registerField('NodeExecutionConfig', 'storage', 'custom', 'NodeStorageSection.svelte');
registerField('NodeExecutionConfig', 'run_on_startup', 'auto');

// ==========================================================================
// StorageConfig
// ==========================================================================

registerField('StorageConfig', 'backends', 'custom', 'StorageSection.svelte');
registerField('StorageConfig', 'cache', 'custom', 'StorageSection.svelte');
registerField('StorageConfig', 'file_storage_metadata_path', 'auto');

// ==========================================================================
// CacheConfig
// ==========================================================================

registerField('CacheConfig', 'enabled', 'auto');
registerField('CacheConfig', 'version', 'auto');
registerField('CacheConfig', 'storage_path', 'auto');
registerField('CacheConfig', 'include_nodes', 'custom', 'StorageSection.svelte');
registerField('CacheConfig', 'exclude_nodes', 'custom', 'StorageSection.svelte');
registerField('CacheConfig', 'include_all_nodes', 'auto');
registerField('CacheConfig', 'cache_what', 'auto');
registerField('CacheConfig', 'hash_method', 'auto');
registerField('CacheConfig', 'pickling_method', 'auto');
registerField('CacheConfig', 'pickling_args', 'custom', 'StorageSection.svelte');
registerField('CacheConfig', 'evaluate_lazy_value_for_cache', 'auto');
registerField('CacheConfig', 'sample_size', 'auto');

// ==========================================================================
// NodeStorageConfig
// ==========================================================================

registerField('NodeStorageConfig', 'cache', 'custom', 'NodeStorageSection.svelte');
registerField('NodeStorageConfig', 'file_storage', 'custom', 'NodeStorageSection.svelte');

// ==========================================================================
// NodeCacheConfig
// ==========================================================================

registerField('NodeCacheConfig', 'enabled', 'auto');
registerField('NodeCacheConfig', 'version', 'auto');
registerField('NodeCacheConfig', 'cache_what', 'auto');
registerField('NodeCacheConfig', 'hash_method', 'auto');
registerField('NodeCacheConfig', 'pickling_method', 'auto');
registerField('NodeCacheConfig', 'pickling_args', 'custom', 'NodeStorageSection.svelte');
registerField('NodeCacheConfig', 'evaluate_lazy_value_for_cache', 'auto');
registerField('NodeCacheConfig', 'sample_size', 'auto');

// ==========================================================================
// NodeFileStorageConfig
// ==========================================================================

registerField('NodeFileStorageConfig', 'enabled', 'auto');
registerField('NodeFileStorageConfig', 'backend', 'custom', 'NodeStorageSection.svelte');
registerField('NodeFileStorageConfig', 'serialization', 'auto');
registerField('NodeFileStorageConfig', 'pickling_method', 'auto');
registerField('NodeFileStorageConfig', 'pickling_args', 'custom', 'NodeStorageSection.svelte');
registerField('NodeFileStorageConfig', 'compression', 'auto');
registerField('NodeFileStorageConfig', 'on_hash_change', 'auto');
registerField('NodeFileStorageConfig', 'store_inputs', 'auto');
registerField('NodeFileStorageConfig', 'hash_method', 'auto');
registerField('NodeFileStorageConfig', 'hash_pickling_method', 'auto');
registerField('NodeFileStorageConfig', 'hash_pickling_args', 'custom', 'NodeStorageSection.svelte');
registerField('NodeFileStorageConfig', 'evaluate_lazy_value_for_hash', 'auto');
registerField('NodeFileStorageConfig', 'bundle', 'auto');
registerField('NodeFileStorageConfig', 'bundle_format', 'auto');
registerField('NodeFileStorageConfig', 'output_names', 'custom', 'NodeStorageSection.svelte');
registerField('NodeFileStorageConfig', 'version', 'auto');

// ==========================================================================
// Backend configs
// ==========================================================================

registerField('LocalBackendConfig', 'type', 'ignore');
registerField('LocalBackendConfig', 'base_path', 'auto');

registerField('S3BackendConfig', 'type', 'ignore');
registerField('S3BackendConfig', 'bucket', 'auto');
registerField('S3BackendConfig', 'prefix', 'auto');
registerField('S3BackendConfig', 'region', 'auto');
registerField('S3BackendConfig', 'endpoint_url', 'auto');
registerField('S3BackendConfig', 'access_key', 'auto');
registerField('S3BackendConfig', 'secret_key', 'auto');

registerField('GCSBackendConfig', 'type', 'ignore');
registerField('GCSBackendConfig', 'bucket', 'auto');
registerField('GCSBackendConfig', 'prefix', 'auto');
registerField('GCSBackendConfig', 'credentials_path', 'auto');

registerField('SSHBackendConfig', 'type', 'ignore');
registerField('SSHBackendConfig', 'host', 'auto');
registerField('SSHBackendConfig', 'base_path', 'auto');
registerField('SSHBackendConfig', 'port', 'auto');
registerField('SSHBackendConfig', 'username', 'auto');
registerField('SSHBackendConfig', 'key_path', 'auto');
registerField('SSHBackendConfig', 'password', 'auto');

registerField('RcloneBackendConfig', 'type', 'ignore');
registerField('RcloneBackendConfig', 'remote', 'auto');
registerField('RcloneBackendConfig', 'config_path', 'auto');

// ==========================================================================
// OutputQueueConfig
// ==========================================================================

registerField('OutputQueueConfig', 'ports', 'custom', 'OutputQueuesSection.svelte');
