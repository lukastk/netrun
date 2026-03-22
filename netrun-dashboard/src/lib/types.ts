/** Mirrors netrun_utils.observe.models.NetStatus */
export interface NetStatus {
	started: boolean;
	paused: boolean;
	is_blocked: boolean;
	node_names: string[];
	edge_count: number;
	total_epochs: number;
	busy_nodes: string[];
	idle_nodes: string[];
	startable_epoch_count: number;
	running_epoch_count: number;
	dead_letter_count: number;
	exception_count: number;
	output_queues: Record<string, number>;
}

/** Mirrors netrun_utils.observe.models.NodeStatus */
export interface NodeStatus {
	name: string;
	enabled: boolean;
	epoch_count: number;
	is_busy: boolean;
	running_epoch_ids: string[];
	startable_epoch_ids: string[];
	in_port_names: string[];
	out_port_names: string[];
	input_port_packet_counts: Record<string, number>;
	pools: string[];
	factory: string | null;
}

/** Mirrors netrun_utils.observe.models.EdgeStatus */
export interface EdgeStatus {
	source_node: string;
	source_port: string;
	target_node: string;
	target_port: string;
	packet_count: number;
}

/** Mirrors netrun_utils.observe.models.StructuredLogEntry */
export interface StructuredLogEntry {
	timestamp: string;
	message: string | null;
	level: string;
	fields: Record<string, unknown>;
}

/** Mirrors netrun_utils.observe.models.EpochInfo */
export interface EpochInfo {
	epoch_id: string;
	node_name: string;
	state: string;
	created_at: string;
	started_at: string | null;
	ended_at: string | null;
	duration_ms: number | null;
	queue_time_ms: number | null;
	outcome: string | null;
	error: string | null;
	error_type: string | null;
	error_traceback: string | null;
	pool_id: string | null;
	worker_id: number | null;
	was_cache_hit: boolean | null;
	was_file_storage_hit: boolean | null;
	retry_count: number | null;
	factory: string | null;
	in_salvo_ports: string[];
	in_salvo_packet_count: number;
	out_salvo_count: number;
	orphaned_packet_count: number;
	destroyed_packet_count: number;
	node_log_entries: StructuredLogEntry[];
}

/** Mirrors netrun_utils.observe.models.LogEntry */
export interface LogEntry {
	timestamp: string;
	message: string;
	node_name: string | null;
	epoch_id: string | null;
}

/** WebSocket message from ObserveServer */
export interface ObserveState {
	status: NetStatus;
	nodes: NodeStatus[];
	edges: EdgeStatus[];
	epochs: EpochInfo[];
	logs: LogEntry[];
}

/** Entry in the dashboard registry */
export interface RegisteredNet {
	name: string;
	url: string;
	last_heartbeat: number;
	healthy: boolean;
}
