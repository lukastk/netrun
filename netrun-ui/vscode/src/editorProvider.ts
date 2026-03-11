import * as vscode from 'vscode';
import * as path from 'path';

function getNonce(): string {
	let text = '';
	const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
	for (let i = 0; i < 32; i++) {
		text += chars.charAt(Math.floor(Math.random() * chars.length));
	}
	return text;
}

interface Action {
	id: string;
	label: string;
	command: string;
}

export class NetrunEditorProvider implements vscode.CustomTextEditorProvider {
	private activeWebview: vscode.Webview | null = null;

	constructor(
		private readonly context: vscode.ExtensionContext,
		private readonly backendPort: number,
	) {
		// Register commands
		context.subscriptions.push(
			vscode.commands.registerCommand('netrun-ui.save', () => {
				if (this.activeWebview) {
					this.activeWebview.postMessage({ type: 'save' });
				}
			}),
			vscode.commands.registerCommand('netrun-ui.commandPalette', () => {
				if (this.activeWebview) {
					this.activeWebview.postMessage({ type: 'commandPalette' });
				}
			}),
			vscode.commands.registerCommand('netrun-ui.openNode', () => {
				this.showOpenNodePicker();
			}),
			vscode.commands.registerCommand('netrun-ui.openConfig', () => {
				this.showOpenConfigPicker();
			})
		);
	}

	private get apiBase(): string {
		return `http://127.0.0.1:${this.backendPort}/api`;
	}

	private async findNetrunFiles(): Promise<vscode.Uri[]> {
		return vscode.workspace.findFiles(
			'**/{*.netrun.json,*.netrun.toml,netrun.json,netrun.toml}',
			'**/node_modules/**'
		);
	}

	private async pickNetrunFile(placeHolder: string): Promise<vscode.Uri | undefined> {
		const files = await this.findNetrunFiles();

		if (files.length === 0) {
			vscode.window.showInformationMessage('No netrun config files found in workspace');
			return undefined;
		}

		if (files.length === 1) {
			return files[0];
		}

		const fileItems = files.map(f => ({
			label: vscode.workspace.asRelativePath(f),
			uri: f,
		}));
		const picked = await vscode.window.showQuickPick(fileItems, { placeHolder });
		return picked?.uri;
	}

	private async showOpenConfigPicker(): Promise<void> {
		const file = await this.pickNetrunFile('Select a netrun config to open');
		if (file) {
			await vscode.commands.executeCommand('vscode.openWith', file, 'netrun-ui.flowEditor');
		}
	}

	private async showOpenNodePicker(): Promise<void> {
		const selectedFile = await this.pickNetrunFile('Select a netrun config file');
		if (!selectedFile) return;

		// Load file via backend API
		let fileData: any;
		try {
			const resp = await fetch(`${this.apiBase}/files/read`, {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ path: selectedFile.fsPath }),
			});
			if (!resp.ok) {
				throw new Error(`Backend returned ${resp.status}`);
			}
			fileData = await resp.json();
		} catch (e) {
			vscode.window.showErrorMessage(`Failed to load file: ${(e as Error).message}`);
			return;
		}

		const uiNodes: any[] = fileData.nodes || [];
		const nonDecorationNodes = uiNodes.filter(
			(n: any) => n.data?.nodeType !== 'decoration'
		);

		if (nonDecorationNodes.length === 0) {
			vscode.window.showInformationMessage('No nodes in selected config');
			return;
		}

		// Extract graph-level extra for project actions and settings
		const graphExtra = fileData.extra || {};
		const graphUi = graphExtra.ui || {};
		const projectActions: Action[] = graphUi.actions || [];
		const projectOpenAction = projectActions.find((a: Action) => a.label === 'open');

		// Build QuickPick items
		const items = nonDecorationNodes.map((n: any) => {
			const config = n.data?._config || {};
			const extra = config.extra || {};
			const sourcePath = extra.source_path as string | undefined;
			const factory = n.data?.factory as string | undefined;
			return {
				label: n.data?.label || n.id,
				description: factory || n.data?.nodeType || '',
				detail: sourcePath,
				nodeData: n,
			};
		});

		const picked = await vscode.window.showQuickPick(items, {
			placeHolder: 'Select a node to open',
			matchOnDescription: true,
			matchOnDetail: true,
		});

		if (!picked) return;

		const nodeData = picked.nodeData;
		const config = nodeData.data?._config || {};
		const extra = config.extra || {};
		const nodeUi = extra.ui || {};
		const nodeActions: Action[] = nodeUi.actions || [];
		const nodeOpenAction = nodeActions.find((a: Action) => a.label === 'open');

		const openAction = nodeOpenAction || projectOpenAction;

		if (openAction) {
			// Execute the open action via backend API
			try {
				const extraData = fileData.extra_data || {};
				await fetch(`${this.apiBase}/actions/execute`, {
					method: 'POST',
					headers: { 'Content-Type': 'application/json' },
					body: JSON.stringify({
						command: openAction.command,
						node_name: nodeData.data?.label,
						net_file_path: selectedFile.fsPath,
						project_root: extraData.project_root_override || graphUi.projectRoot,
						default_cmd: graphUi.defaultCmd,
						env: graphUi.env,
					}),
				});
			} catch (e) {
				vscode.window.showErrorMessage(`Failed to execute open action: ${(e as Error).message}`);
			}
		} else {
			// Fall back to opening source_path in VS Code
			const sourcePath = extra.source_path as string | undefined;
			if (sourcePath) {
				let resolvedPath = sourcePath;
				if (!path.isAbsolute(sourcePath)) {
					const extraData = fileData.extra_data || {};
					const prOverride = extraData.project_root_override || graphUi.projectRoot;
					const configDir = path.dirname(selectedFile.fsPath);
					const basePath = prOverride
						? (path.isAbsolute(prOverride) ? prOverride : path.resolve(configDir, prOverride))
						: configDir;
					resolvedPath = path.resolve(basePath, sourcePath);
				}
				const fileUri = vscode.Uri.file(resolvedPath);
				await vscode.window.showTextDocument(fileUri);
			} else {
				vscode.window.showInformationMessage(
					`No 'open' action or extra.source_path defined on node "${nodeData.data?.label}"`
				);
			}
		}
	}

	async resolveCustomTextEditor(
		document: vscode.TextDocument,
		webviewPanel: vscode.WebviewPanel,
		_token: vscode.CancellationToken,
	): Promise<void> {
		const distWebview = vscode.Uri.joinPath(this.context.extensionUri, 'dist', 'webview');

		webviewPanel.webview.options = {
			enableScripts: true,
			localResourceRoots: [distWebview],
		};

		webviewPanel.webview.html = this.getHtmlForWebview(webviewPanel.webview, distWebview);

		// Track the active webview for save command
		if (webviewPanel.active) {
			this.activeWebview = webviewPanel.webview;
		}
		webviewPanel.onDidChangeViewState(() => {
			if (webviewPanel.active) {
				this.activeWebview = webviewPanel.webview;
			} else if (this.activeWebview === webviewPanel.webview) {
				this.activeWebview = null;
			}
		});

		// Send initial configuration to webview once it's ready
		const messageHandler = webviewPanel.webview.onDidReceiveMessage(async (message) => {
			switch (message.type) {
				case 'ready':
					webviewPanel.webview.postMessage({
						type: 'init',
						filePath: document.uri.fsPath,
						apiBase: `http://127.0.0.1:${this.backendPort}/api`,
					});
					break;

				case 'saved':
					// File was saved by the webview via the backend.
					// Revert the text document to pick up changes from disk.
					await vscode.commands.executeCommand(
						'workbench.action.files.revert'
					);
					break;

				case 'openFile': {
					const fileUri = vscode.Uri.file(message.filePath);
					await vscode.window.showTextDocument(fileUri, {
						viewColumn: message.beside ? vscode.ViewColumn.Beside : vscode.ViewColumn.Active,
					});
					break;
				}
			}
		});

		webviewPanel.onDidDispose(() => {
			messageHandler.dispose();
			if (this.activeWebview === webviewPanel.webview) {
				this.activeWebview = null;
			}
		});
	}

	private getHtmlForWebview(
		webview: vscode.Webview,
		distWebview: vscode.Uri,
	): string {
		const scriptUri = webview.asWebviewUri(
			vscode.Uri.joinPath(distWebview, 'main.js')
		);
		const styleUri = webview.asWebviewUri(
			vscode.Uri.joinPath(distWebview, 'index.css')
		);
		const nonce = getNonce();

		return /* html */ `<!DOCTYPE html>
<html lang="en">
<head>
	<meta charset="UTF-8">
	<meta http-equiv="Content-Security-Policy"
		content="default-src 'none';
			style-src ${webview.cspSource} 'unsafe-inline';
			script-src 'nonce-${nonce}';
			connect-src http://127.0.0.1:*;
			img-src ${webview.cspSource} http://127.0.0.1:* data:;
			font-src ${webview.cspSource};">
	<meta name="viewport" content="width=device-width, initial-scale=1.0">
	<link rel="stylesheet" href="${styleUri}">
	<title>netrun-ui</title>
</head>
<body>
	<div id="app"></div>
	<script nonce="${nonce}" src="${scriptUri}"></script>
</body>
</html>`;
	}
}
