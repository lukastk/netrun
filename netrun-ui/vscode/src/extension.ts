import * as vscode from 'vscode';
import { NetrunEditorProvider } from './editorProvider';
import { ChildProcess, spawn, execFileSync } from 'child_process';
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

	// Register the select-environment command
	context.subscriptions.push(
		vscode.commands.registerCommand(
			'netrun-ui.selectPythonEnvironment',
			() => selectPythonEnvironment(context),
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
 * 2. Workspace .venv/bin/python (if it exists and has netrun_ui_backend)
 * 3. VS Code Python extension's active interpreter
 * 4. Workspace .venv/bin/python (even without netrun_ui_backend)
 * 5. Fallback to "python"
 *
 * The workspace .venv is preferred over the VS Code Python extension because
 * it contains the project's own dependencies (editable installs, etc.) which
 * are needed for factory resolution.
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

	// Try workspace .venv first — it has the project's dependencies
	const workspaceFolder = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
	if (workspaceFolder) {
		const venvPython = path.join(workspaceFolder, '.venv', 'bin', 'python');
		if (fs.existsSync(venvPython)) {
			// Check if the venv has netrun_ui_backend installed
			try {
				execFileSync(venvPython, ['-c', 'import netrun_ui_backend'], {
					timeout: 5000,
					stdio: 'ignore',
				});
				outputChannel.appendLine(`Using workspace .venv Python: ${venvPython}`);
				return venvPython;
			} catch {
				outputChannel.appendLine(
					`Workspace .venv exists but lacks netrun_ui_backend, trying other sources`
				);
			}
		}
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

	// Last resort: workspace .venv even without netrun_ui_backend
	if (workspaceFolder) {
		const venvPython = path.join(workspaceFolder, '.venv', 'bin', 'python');
		if (fs.existsSync(venvPython)) {
			outputChannel.appendLine(`Using workspace .venv Python (fallback): ${venvPython}`);
			return venvPython;
		}
	}

	outputChannel.appendLine(`Falling back to default Python: ${configured}`);
	return configured;
}

/**
 * Extract the virtual environment root from a Python executable path.
 * E.g. "/path/to/.venv/bin/python" -> "/path/to/.venv"
 * Returns undefined if the path doesn't look like it's inside a venv.
 */
function extractVenvPath(pythonPath: string): string | undefined {
	// Typical venv layout: <venv>/bin/python (Unix) or <venv>/Scripts/python.exe (Windows)
	const normalized = path.resolve(pythonPath);
	const dir = path.dirname(normalized);
	const dirName = path.basename(dir);

	if (dirName === 'bin' || dirName === 'Scripts') {
		const candidate = path.dirname(dir);
		// Verify it looks like a venv by checking for pyvenv.cfg
		if (fs.existsSync(path.join(candidate, 'pyvenv.cfg'))) {
			return candidate;
		}
	}
	return undefined;
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

	// Build subprocess environment. If the Python path is inside a venv,
	// set VIRTUAL_ENV so the subprocess picks up the venv's site-packages.
	const env: Record<string, string | undefined> = {
		...process.env,
		NETRUN_UI_ALLOW_ALL_ORIGINS: '1',
	};

	const venvPath = extractVenvPath(pythonPath);
	if (venvPath) {
		env.VIRTUAL_ENV = venvPath;
		// Prepend the venv's bin dir to PATH so child processes also resolve
		// to the venv's Python and tools.
		const venvBin = path.join(venvPath, process.platform === 'win32' ? 'Scripts' : 'bin');
		env.PATH = `${venvBin}${path.delimiter}${process.env.PATH ?? ''}`;
		outputChannel.appendLine(`Activating venv: VIRTUAL_ENV=${venvPath}`);
	}

	backendProcess = spawn(pythonPath, args, {
		env,
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

/**
 * Check if a Python binary has netrun_ui_backend installed.
 */
function hasBackend(pythonPath: string): boolean {
	try {
		execFileSync(pythonPath, ['-c', 'import netrun_ui_backend'], {
			timeout: 5000,
			stdio: 'ignore',
		});
		return true;
	} catch {
		return false;
	}
}

/**
 * Discover candidate Python environments for the quick pick.
 */
function discoverEnvironments(): Array<{ label: string; description: string; pythonPath: string }> {
	const candidates: Array<{ label: string; description: string; pythonPath: string }> = [];
	const workspaceFolder = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;

	if (workspaceFolder) {
		// Check .venv in workspace
		const venvPython = path.join(workspaceFolder, '.venv', 'bin', 'python');
		if (fs.existsSync(venvPython)) {
			const has = hasBackend(venvPython);
			candidates.push({
				label: '.venv (workspace)',
				description: has ? venvPython : `${venvPython} (missing netrun_ui_backend)`,
				pythonPath: venvPython,
			});
		}

		// Check venv in workspace
		const venvPython2 = path.join(workspaceFolder, 'venv', 'bin', 'python');
		if (fs.existsSync(venvPython2)) {
			const has = hasBackend(venvPython2);
			candidates.push({
				label: 'venv (workspace)',
				description: has ? venvPython2 : `${venvPython2} (missing netrun_ui_backend)`,
				pythonPath: venvPython2,
			});
		}
	}

	return candidates;
}

/**
 * Stop the running backend process (if any).
 */
async function stopBackend(): Promise<void> {
	if (backendProcess) {
		backendProcess.kill();
		// Wait for process to exit
		await new Promise<void>((resolve) => {
			if (!backendProcess) {
				resolve();
				return;
			}
			backendProcess.on('exit', () => resolve());
			setTimeout(resolve, 3000); // Timeout after 3s
		});
		backendProcess = null;
	}
}

/**
 * Command: Select Python Environment.
 * Shows a quick pick with discovered venvs and a "Browse..." option,
 * saves the choice to workspace settings, and restarts the backend.
 */
async function selectPythonEnvironment(context: vscode.ExtensionContext): Promise<void> {
	const discovered = discoverEnvironments();

	interface EnvItem extends vscode.QuickPickItem {
		pythonPath?: string;
		action?: 'browse' | 'auto';
	}

	const items: EnvItem[] = [];

	// "Auto-detect" option to clear the explicit setting
	items.push({
		label: '$(search) Auto-detect',
		description: 'Use automatic Python resolution (default)',
		action: 'auto',
	});

	// Discovered environments
	for (const env of discovered) {
		items.push({
			label: env.label,
			description: env.description,
			pythonPath: env.pythonPath,
		});
	}

	// Browse option
	items.push({
		label: '$(folder-opened) Browse...',
		description: 'Select a Python binary manually',
		action: 'browse',
	});

	const selected = await vscode.window.showQuickPick(items, {
		placeHolder: 'Select Python environment for netrun-ui backend',
	});

	if (!selected) {
		return; // Cancelled
	}

	const config = vscode.workspace.getConfiguration('netrun-ui');
	let chosenPath: string | undefined;

	if (selected.action === 'auto') {
		// Clear the explicit setting so auto-detection kicks in
		await config.update('pythonPath', undefined, vscode.ConfigurationTarget.Workspace);
		outputChannel.appendLine('Python path reset to auto-detect');
	} else if (selected.action === 'browse') {
		const result = await vscode.window.showOpenDialog({
			canSelectFiles: true,
			canSelectFolders: false,
			canSelectMany: false,
			title: 'Select Python binary',
			openLabel: 'Select',
		});
		if (!result || result.length === 0) {
			return; // Cancelled
		}
		chosenPath = result[0].fsPath;
	} else if (selected.pythonPath) {
		chosenPath = selected.pythonPath;
	}

	if (chosenPath) {
		// Validate the chosen Python has the backend
		if (!hasBackend(chosenPath)) {
			const install = await vscode.window.showWarningMessage(
				`The selected Python (${chosenPath}) does not have netrun_ui_backend installed. Use it anyway?`,
				'Use anyway', 'Cancel',
			);
			if (install !== 'Use anyway') {
				return;
			}
		}
		await config.update('pythonPath', chosenPath, vscode.ConfigurationTarget.Workspace);
		outputChannel.appendLine(`Python path set to: ${chosenPath}`);
	}

	// Restart the backend with the new Python
	outputChannel.appendLine('Restarting backend...');
	await stopBackend();
	try {
		backendPort = await startBackend(context);
		outputChannel.appendLine(`Backend restarted on port ${backendPort}`);
		vscode.window.showInformationMessage('netrun-ui: Backend restarted with new Python environment');
	} catch (err) {
		const msg = err instanceof Error ? err.message : String(err);
		vscode.window.showErrorMessage(`netrun-ui: Failed to restart backend. ${msg}`);
		outputChannel.appendLine(`Backend restart failed: ${msg}`);
		outputChannel.show();
	}
}
