/**
 * API client for netrun-ui backend
 */

// In production, API is served from same origin. In dev, it's on port 8000.
export const API_BASE = import.meta.env.DEV ? 'http://127.0.0.1:8000/api' : '/api';

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
	description?: string;
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
	width?: number;
	height?: number;
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
	extra?: Record<string, unknown>;
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
	enum_options?: string[] | null;
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
	description: string | null;
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
	detail: string | Array<{ loc?: string[]; msg?: string; type?: string }>;
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
	net_file_path?: string;
	project_root?: string;
	default_cmd?: string;
	node_config?: string;  // JSON-serialized node config
}

export interface ExecuteActionResponse {
	success: boolean;
	exit_code: number;
	stdout: string;
	stderr: string;
	resolved_command: string;
	timed_out?: boolean;
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
}

export interface ServerConfigResponse {
	working_dir: string;
	first_netrun_file: string | null;
}

// Recipe interfaces
export interface RecipePrompt {
	name: string;
	label: string;
	type: 'text' | 'number' | 'select' | 'checkbox';
	default?: unknown;
	options?: string[];  // For select type
}

export interface GetRecipePromptsResponse {
	prompts: RecipePrompt[];
}

export interface ExecuteRecipeResponse {
	config: Record<string, unknown>;
	stdout: string;
}

// Config schema types
export type FieldCategory =
	| 'bool' | 'str' | 'int' | 'float' | 'enum'
	| 'bool_or_null' | 'str_or_null' | 'int_or_null' | 'float_or_null' | 'enum_or_null'
	| 'complex';

export interface FieldSchema {
	name: string;
	category: FieldCategory;
	default: unknown;
	description: string | null;
	enum_values: string[] | null;
	required: boolean;
	env_var_supported: boolean;
}

export interface ModelSchema {
	model_name: string;
	fields: FieldSchema[];
}

export interface ConfigSchemaResponse {
	models: Record<string, ModelSchema>;
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
			const detail = error.detail;
			const message = Array.isArray(detail)
				? detail.map(d => d.msg ?? JSON.stringify(d)).join('; ')
				: (typeof detail === 'string' ? detail : JSON.stringify(detail));
			throw new Error(message);
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
		extra?: Record<string, unknown>,
		extra_data?: Record<string, unknown>  // Non-graph data (pools, etc.)
	): Promise<FileSaveResponse> {
		return this.request<FileSaveResponse>('/files/save', {
			method: 'POST',
			body: JSON.stringify({ path, format, nodes, edges, extra, extra_data }),
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
		factoryPath: string,
		projectRoot?: string
	): Promise<FactorySignatureResponse> {
		return this.request<FactorySignatureResponse>('/factories/signature', {
			method: 'POST',
			body: JSON.stringify({
				factory_path: factoryPath,
				...(projectRoot ? { project_root: projectRoot } : {}),
			}),
		});
	}

	/**
	 * Preview factory-generated config
	 */
	async previewFactory(
		factoryPath: string,
		factoryArgs: Record<string, unknown> = {},
		projectRoot?: string
	): Promise<FactoryPreviewResponse> {
		return this.request<FactoryPreviewResponse>('/factories/preview', {
			method: 'POST',
			body: JSON.stringify({
				factory_path: factoryPath,
				factory_args: factoryArgs,
				...(projectRoot ? { project_root: projectRoot } : {}),
			}),
		});
	}

	/**
	 * Validate an import path
	 */
	async validateImport(
		importPath: string,
		projectRoot?: string
	): Promise<{ valid: boolean; error: string | null; is_factory: boolean }> {
		return this.request('/factories/validate-import', {
			method: 'POST',
			body: JSON.stringify({
				import_path: importPath,
				...(projectRoot ? { project_root: projectRoot } : {}),
			}),
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
	 * Shut down the application (closes pywebview window or stops server)
	 */
	async shutdown(): Promise<void> {
		try {
			await fetch(`${this.baseUrl.replace('/api', '')}/api/shutdown`, { method: 'POST' });
		} catch {
			// Connection may be lost before response arrives — that's expected
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
	 * @param projectRoot Explicit project root for resolving relative paths (optional)
	 */
	async loadSubgraph(
		path?: string,
		inlineConfig?: Record<string, unknown>,
		basePath?: string,
		projectRoot?: string
	): Promise<SubgraphLoadResponse> {
		return this.request<SubgraphLoadResponse>('/files/subgraph/load', {
			method: 'POST',
			body: JSON.stringify({
				path: path || null,
				base_path: basePath || null,
				project_root: projectRoot || null,
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
			net_file_path?: string;
			project_root?: string;
			default_cmd?: string;
			env?: Record<string, string>;
			node_env?: Record<string, string>;  // Node-level variable overrides
			node_config?: string;  // JSON-serialized node config
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
		extra?: Record<string, unknown>,
		extra_data?: Record<string, unknown>,
		file_path?: string
	): Promise<ValidateConfigResponse> {
		return this.request<ValidateConfigResponse>('/files/validate', {
			method: 'POST',
			body: JSON.stringify({ nodes, edges, extra, extra_data, file_path }),
		});
	}

	/**
	 * Get server configuration including working directory
	 */
	async getServerConfig(): Promise<ServerConfigResponse> {
		// Note: /api/config is served directly, not under /api/files etc.
		const url = this.baseUrl.replace('/api', '') + '/api/config';
		const response = await fetch(url);
		if (!response.ok) {
			throw new Error(`Failed to get server config: ${response.statusText}`);
		}
		return response.json();
	}

	/**
	 * Get prompts for a recipe
	 */
	async getRecipePrompts(
		recipePath: string,
		config: Record<string, unknown>
	): Promise<GetRecipePromptsResponse> {
		return this.request<GetRecipePromptsResponse>('/recipes/get-prompts', {
			method: 'POST',
			body: JSON.stringify({ recipe_path: recipePath, config }),
		});
	}

	/**
	 * Execute a recipe with inputs
	 */
	async executeRecipe(
		recipePath: string,
		config: Record<string, unknown>,
		inputs: Record<string, unknown>
	): Promise<ExecuteRecipeResponse> {
		return this.request<ExecuteRecipeResponse>('/recipes/execute', {
			method: 'POST',
			body: JSON.stringify({ recipe_path: recipePath, config, inputs }),
		});
	}

	/**
	 * Get config field schemas for NetConfig, NodeConfig, and NodeExecutionConfig
	 */
	async getConfigSchema(): Promise<ConfigSchemaResponse> {
		return this.request<ConfigSchemaResponse>('/config/schema', {
			method: 'GET',
		});
	}
}

export const api = new ApiClient();
