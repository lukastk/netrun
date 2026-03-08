import * as vscode from 'vscode';
import { NetrunEditorProvider } from './editorProvider';
import { ChildProcess, spawn } from 'child_process';
import * as fs from 'fs';
import * as http from 'http';
import * as net from 'net';
import * as path from 'path';

let backendProcess: ChildProcess | null = null;
let backendPort: number = 0;
let outputChannel: vscode.OutputChannel;

export async function activate(context: vscode.ExtensionContext) {
	outputChannel = vscode.window.createOutputChannel('netrun-ui');
	context.subscriptions.push(outputChannel);

	try {
		backendPort = await startBackend(context);
		outputChannel.appendLine(`Backend started on port ${backendPort}`);
	} catch (err) {
		const msg = err instanceof Error ? err.message : String(err);
		vscode.window.showErrorMessage(
			`netrun-ui: Failed to start backend. ${msg}. ` +
			`Ensure Python with netrun-ui-backend is installed and configured in settings.`
		);
		outputChannel.appendLine(`Backend startup failed: ${msg}`);
		outputChannel.show();
		return;
	}

	const provider = new NetrunEditorProvider(context, backendPort);
	context.subscriptions.push(
		vscode.window.registerCustomEditorProvider(
			'netrun-ui.flowEditor',
			provider,
			{ supportsMultipleEditorsPerDocument: false }
		)
	);
}

export function deactivate() {
	if (backendProcess) {
		backendProcess.kill();
		backendProcess = null;
	}
}

async function findFreePort(): Promise<number> {
	return new Promise((resolve, reject) => {
		const server = net.createServer();
		server.listen(0, '127.0.0.1', () => {
			const addr = server.address();
			if (addr && typeof addr === 'object') {
				const port = addr.port;
				server.close(() => resolve(port));
			} else {
				server.close(() => reject(new Error('Could not determine port')));
			}
		});
		server.on('error', reject);
	});
}

async function waitForServer(
	url: string,
	processExited: Promise<{ code: number | null; stderr: string }>,
	timeoutMs: number = 30000,
): Promise<void> {
	const start = Date.now();
	while (Date.now() - start < timeoutMs) {
		// Check if the process exited early (e.g. missing module, bad python)
		const exitResult = await Promise.race([
			processExited.then(r => r),
			new Promise<null>(resolve => setTimeout(() => resolve(null), 0)),
		]);
		if (exitResult !== null) {
			const detail = exitResult.stderr
				? `\n${exitResult.stderr.slice(0, 500)}`
				: '';
			throw new Error(
				`Backend process exited with code ${exitResult.code} before becoming ready.${detail}`
			);
		}

		try {
			await new Promise<void>((resolve, reject) => {
				const req = http.get(url, (res) => {
					res.resume();
					if (res.statusCode === 200) {
						resolve();
					} else {
						reject(new Error(`Status ${res.statusCode}`));
					}
				});
				req.on('error', reject);
				req.setTimeout(1000, () => {
					req.destroy();
					reject(new Error('timeout'));
				});
			});
			return;
		} catch {
			await new Promise(r => setTimeout(r, 200));
		}
	}
	throw new Error(`Backend did not start within ${timeoutMs / 1000}s`);
}

/**
 * Resolve the Python interpreter path. Priority:
 * 1. netrun-ui.pythonPath setting (if explicitly set by user)
 * 2. VS Code Python extension's active interpreter
 * 3. Workspace .venv/bin/python (if it exists)
 * 4. Fallback to "python"
 */
async function resolvePythonPath(): Promise<string> {
	const config = vscode.workspace.getConfiguration('netrun-ui');
	const configured = config.get<string>('pythonPath', 'python');

	// If the user explicitly set a non-default path, use it
	const inspect = config.inspect<string>('pythonPath');
	const isExplicitlySet = inspect?.workspaceValue !== undefined
		|| inspect?.workspaceFolderValue !== undefined
		|| inspect?.globalValue !== undefined;

	if (isExplicitlySet) {
		outputChannel.appendLine(`Using configured Python path: ${configured}`);
		return configured;
	}

	// Try to get the interpreter from VS Code's Python extension
	try {
		const pythonExt = vscode.extensions.getExtension('ms-python.python');
		if (pythonExt) {
			if (!pythonExt.isActive) {
				await pythonExt.activate();
			}
			const api = pythonExt.exports;
			const envPath = api?.environments?.getActiveEnvironmentPath?.();
			if (envPath?.path) {
				// The path may point to the env folder or the python binary.
				// resolveEnvironment gives us the full details.
				const resolved = await api.environments.resolveEnvironment(envPath);
				const execPath = resolved?.executable?.uri?.fsPath;
				if (execPath) {
					outputChannel.appendLine(`Using Python from VS Code Python extension: ${execPath}`);
					return execPath;
				}
				// Fall back to the raw path if resolve didn't give us an executable
				outputChannel.appendLine(`Using Python environment path: ${envPath.path}`);
				return envPath.path;
			}
		}
	} catch (err) {
		outputChannel.appendLine(`Could not get interpreter from Python extension: ${err}`);
	}

	// Try workspace .venv
	const workspaceFolder = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
	if (workspaceFolder) {
		const venvPython = path.join(workspaceFolder, '.venv', 'bin', 'python');
		if (fs.existsSync(venvPython)) {
			outputChannel.appendLine(`Using workspace .venv Python: ${venvPython}`);
			return venvPython;
		}
	}

	outputChannel.appendLine(`Falling back to default Python: ${configured}`);
	return configured;
}

async function startBackend(context: vscode.ExtensionContext): Promise<number> {
	const pythonPath = await resolvePythonPath();
	const config = vscode.workspace.getConfiguration('netrun-ui');
	let port = config.get<number>('backendPort', 0);

	if (port === 0) {
		port = await findFreePort();
	}

	const workspaceFolder = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;

	const args = [
		'-m', 'netrun_ui_backend.cli',
		'--server',
		'--port', String(port),
	];

	if (workspaceFolder) {
		args.push('-C', workspaceFolder);
	}

	outputChannel.appendLine(`Starting backend: ${pythonPath} ${args.join(' ')}`);

	backendProcess = spawn(pythonPath, args, {
		env: {
			...process.env,
			NETRUN_UI_ALLOW_ALL_ORIGINS: '1',
		},
		stdio: ['ignore', 'pipe', 'pipe'],
	});

	// Collect stderr for error reporting and track process exit
	let stderrOutput = '';
	const processExited = new Promise<{ code: number | null; stderr: string }>((resolve) => {
		backendProcess!.stderr?.on('data', (data: Buffer) => {
			const text = data.toString();
			stderrOutput += text;
			outputChannel.appendLine(text.trimEnd());
		});

		backendProcess!.stdout?.on('data', (data: Buffer) => {
			outputChannel.appendLine(data.toString().trimEnd());
		});

		backendProcess!.on('exit', (code) => {
			outputChannel.appendLine(`Backend process exited with code ${code}`);
			resolve({ code, stderr: stderrOutput });
			backendProcess = null;
		});
	});

	await waitForServer(`http://127.0.0.1:${port}/health`, processExited);

	return port;
}
