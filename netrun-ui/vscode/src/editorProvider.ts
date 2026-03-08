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
			})
		);
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
						viewColumn: vscode.ViewColumn.Beside,
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
