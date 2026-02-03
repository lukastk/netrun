/**
 * API client for netrun-ui backend
 */

const API_BASE = 'http://127.0.0.1:8000/api';

export interface PortInfo {
	name: string;
	type?: string | null;
}

export interface UINode {
	id: string;
	type: string;
	position: { x: number; y: number };
	data: {
		label: string;
		nodeType: 'regular' | 'factory';
		inPorts: PortInfo[];
		outPorts: PortInfo[];
		factory?: string;
		factoryArgs?: Record<string, unknown>;
		isValid?: boolean;
		validationErrors?: string[];
		_config?: Record<string, unknown>;
	};
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

export interface ApiError {
	detail: string;
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
		meta?: Record<string, unknown>
	): Promise<FileSaveResponse> {
		return this.request<FileSaveResponse>('/files/save', {
			method: 'POST',
			body: JSON.stringify({ path, format, nodes, edges, meta }),
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
}

export const api = new ApiClient();
