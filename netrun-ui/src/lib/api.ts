/**
 * API client for netrun-ui backend
 */

const API_BASE = 'http://127.0.0.1:8000/api';

export interface PortInfo {
	name: string;
	type?: string | null;
}

export interface UINodeData {
	label: string;
	nodeType: 'regular' | 'factory' | 'subgraph';
	inPorts: PortInfo[];
	outPorts: PortInfo[];
	factory?: string;
	factoryArgs?: Record<string, unknown>;
	isValid?: boolean;
	validationErrors?: string[];
	_config?: Record<string, unknown>;
	// Subgraph-specific
	source?: string;
	nodeCount?: number;
	_subgraphConfig?: Record<string, unknown>;
}

export interface UINode {
	id: string;
	type: string;
	position: { x: number; y: number };
	data: UINodeData;
}

export interface UIEdge {
	id: string;
	source: string;
	target: string;
	sourceHandle?: string;
	targetHandle?: string;
	type?: string;
}

export interface FileReadResponse {
	path: string;
	format: 'json' | 'toml';
	nodes: UINode[];
	edges: UIEdge[];
	meta?: Record<string, unknown>;
	extra_data?: Record<string, unknown>;  // Non-graph data (pools, etc.)
}

export interface FileSaveResponse {
	success: boolean;
	path: string;
}

export interface FactoryParameter {
	name: string;
	type: string | null;
	default: unknown;
	has_default: boolean;
}

export interface FactorySignatureResponse {
	factory_path: string;
	parameters: FactoryParameter[];
	docstring: string | null;
}

export interface FactoryPortInfo {
	name: string;
	port_type: string | null;
}

export interface FactoryPreviewResponse {
	factory_path: string;
	name: string;
	in_ports: FactoryPortInfo[];
	out_ports: FactoryPortInfo[];
	has_in_salvo_conditions: boolean;
	has_out_salvo_conditions: boolean;
	error: string | null;
}

export interface BuiltinFactory {
	import_path: string;
	name: string;
	docstring: string | null;
}

export interface ListBuiltinFactoriesResponse {
	factories: BuiltinFactory[];
	errors: string[];
}

export interface ApiError {
	detail: string;
}

export interface FileEntry {
	name: string;
	path: string;
	is_dir: boolean;
	is_netrun_file: boolean;
}

export interface DirectoryListResponse {
	path: string;
	parent: string | null;
	entries: FileEntry[];
}

// Subgraph interfaces
export interface SubgraphLoadResponse {
	nodes: UINode[];
	edges: UIEdge[];
	exposed_in_ports: Record<string, unknown>;
	exposed_out_ports: Record<string, unknown>;
	source: string;
}

export interface SubgraphCreateRequest {
	subgraph_name: string;
	selected_node_ids: string[];
	all_nodes: UINode[];
	all_edges: UIEdge[];
}

export interface SubgraphCreateResponse {
	subgraph_node: UINode;
	remaining_nodes: UINode[];
	remaining_edges: UIEdge[];
	internal_edges: UIEdge[];
}

// Action execution interfaces
export interface ExecuteActionRequest {
	command: string;
	working_directory?: string;
	env?: Record<string, string>;
	node_env?: Record<string, string>;  // Node-level variable overrides
	node_name?: string;
	node_id?: string;
	net_file_path?: string;
	project_root?: string;
	default_cmd?: string;
}

export interface ExecuteActionResponse {
	success: boolean;
	exit_code: number;
	stdout: string;
	stderr: string;
	resolved_command: string;
}

export interface ResolveTemplateResponse {
	resolved: string;
}

export interface ValidationError {
	loc: (string | number)[];
	msg: string;
	type: string;
}

export interface ValidateConfigResponse {
	valid: boolean;
	errors: ValidationError[];
	netrun_available: boolean;
}

class ApiClient {
	private baseUrl: string;

	constructor(baseUrl: string = API_BASE) {
		this.baseUrl = baseUrl;
	}

	private async request<T>(
		endpoint: string,
		options: RequestInit = {}
	): Promise<T> {
		const url = `${this.baseUrl}${endpoint}`;
		const response = await fetch(url, {
			...options,
			headers: {
				'Content-Type': 'application/json',
				...options.headers,
			},
		});

		if (!response.ok) {
			const error: ApiError = await response.json().catch(() => ({
				detail: `HTTP ${response.status}: ${response.statusText}`,
			}));
			throw new Error(error.detail);
		}

		return response.json();
	}

	/**
	 * Read a .netrun.json or .netrun.toml file
	 */
	async readFile(path: string): Promise<FileReadResponse> {
		return this.request<FileReadResponse>('/files/read', {
			method: 'POST',
			body: JSON.stringify({ path }),
		});
	}

	/**
	 * Save to a .netrun.json or .netrun.toml file
	 */
	async saveFile(
		path: string,
		format: 'json' | 'toml',
		nodes: UINode[],
		edges: UIEdge[],
		meta?: Record<string, unknown>,
		extra_data?: Record<string, unknown>  // Non-graph data (pools, etc.)
	): Promise<FileSaveResponse> {
		return this.request<FileSaveResponse>('/files/save', {
			method: 'POST',
			body: JSON.stringify({ path, format, nodes, edges, meta, extra_data }),
		});
	}

	/**
	 * Convert between JSON and TOML formats
	 */
	async convertFormat(
		content: string,
		fromFormat: 'json' | 'toml',
		toFormat: 'json' | 'toml'
	): Promise<{ content: string }> {
		return this.request<{ content: string }>('/files/convert', {
			method: 'POST',
			body: JSON.stringify({
				content,
				from_format: fromFormat,
				to_format: toFormat,
			}),
		});
	}

	/**
	 * Get factory function signature
	 */
	async getFactorySignature(
		factoryPath: string
	): Promise<FactorySignatureResponse> {
		return this.request<FactorySignatureResponse>('/factories/signature', {
			method: 'POST',
			body: JSON.stringify({ factory_path: factoryPath }),
		});
	}

	/**
	 * Preview factory-generated config
	 */
	async previewFactory(
		factoryPath: string,
		factoryArgs: Record<string, unknown> = {}
	): Promise<FactoryPreviewResponse> {
		return this.request<FactoryPreviewResponse>('/factories/preview', {
			method: 'POST',
			body: JSON.stringify({
				factory_path: factoryPath,
				factory_args: factoryArgs,
			}),
		});
	}

	/**
	 * Validate an import path
	 */
	async validateImport(
		importPath: string
	): Promise<{ valid: boolean; error: string | null; is_factory: boolean }> {
		return this.request('/factories/validate-import', {
			method: 'POST',
			body: JSON.stringify({ import_path: importPath }),
		});
	}

	/**
	 * List built-in factories from netrun.node_factories
	 */
	async listBuiltinFactories(): Promise<ListBuiltinFactoriesResponse> {
		return this.request<ListBuiltinFactoriesResponse>('/factories/builtin', {
			method: 'GET',
		});
	}

	/**
	 * Check if the backend is available
	 */
	async healthCheck(): Promise<boolean> {
		try {
			const response = await fetch(`${this.baseUrl.replace('/api', '')}/health`);
			return response.ok;
		} catch {
			return false;
		}
	}

	/**
	 * List directory contents
	 */
	async listDirectory(
		path: string,
		includeHidden: boolean = false
	): Promise<DirectoryListResponse> {
		return this.request<DirectoryListResponse>('/files/list', {
			method: 'POST',
			body: JSON.stringify({ path, include_hidden: includeHidden }),
		});
	}

	/**
	 * Load subgraph content for editing
	 * @param path Path to external file (optional)
	 * @param inlineConfig Inline subgraph configuration (optional)
	 * @param basePath Base path for resolving relative paths (optional)
	 */
	async loadSubgraph(
		path?: string,
		inlineConfig?: Record<string, unknown>,
		basePath?: string
	): Promise<SubgraphLoadResponse> {
		return this.request<SubgraphLoadResponse>('/files/subgraph/load', {
			method: 'POST',
			body: JSON.stringify({
				path: path || null,
				base_path: basePath || null,
				inline_config: inlineConfig || null,
			}),
		});
	}

	/**
	 * Create a subgraph from selected nodes
	 */
	async createSubgraph(
		subgraphName: string,
		selectedNodeIds: string[],
		allNodes: UINode[],
		allEdges: UIEdge[]
	): Promise<SubgraphCreateResponse> {
		return this.request<SubgraphCreateResponse>('/files/subgraph/create', {
			method: 'POST',
			body: JSON.stringify({
				subgraph_name: subgraphName,
				selected_node_ids: selectedNodeIds,
				all_nodes: allNodes,
				all_edges: allEdges,
			}),
		});
	}

	/**
	 * Execute an action command with template variable resolution
	 */
	async executeAction(request: ExecuteActionRequest): Promise<ExecuteActionResponse> {
		return this.request<ExecuteActionResponse>('/actions/execute', {
			method: 'POST',
			body: JSON.stringify(request),
		});
	}

	/**
	 * Resolve template variables without executing
	 */
	async resolveTemplate(
		template: string,
		options: {
			node_name?: string;
			node_id?: string;
			net_file_path?: string;
			project_root?: string;
			default_cmd?: string;
			env?: Record<string, string>;
			node_env?: Record<string, string>;  // Node-level variable overrides
		} = {}
	): Promise<ResolveTemplateResponse> {
		return this.request<ResolveTemplateResponse>('/actions/resolve', {
			method: 'POST',
			body: JSON.stringify({
				template,
				...options,
			}),
		});
	}

	/**
	 * Validate config against NetConfig/GraphConfig Pydantic models
	 */
	async validateConfig(
		nodes: UINode[],
		edges: UIEdge[],
		meta?: Record<string, unknown>,
		extra_data?: Record<string, unknown>
	): Promise<ValidateConfigResponse> {
		return this.request<ValidateConfigResponse>('/files/validate', {
			method: 'POST',
			body: JSON.stringify({ nodes, edges, meta, extra_data }),
		});
	}
}

export const api = new ApiClient();
