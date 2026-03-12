#!/usr/bin/env node
/**
 * CLI tool to export a .netrun.json config as a standalone HTML file.
 *
 * Usage:
 *   npx netrun-vis-export input.netrun.json -o output.html
 *   npx netrun-vis-export input.netrun.json  # outputs to stdout
 */
import { readFileSync, writeFileSync, existsSync, readdirSync } from 'fs';
import { resolve, dirname, join } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

function usage() {
	console.error(`Usage: netrun-vis-export <input.netrun.json> [-o output.html]

Converts a .netrun.json config file into a standalone HTML file
that renders an interactive graph visualization.

Options:
  -o, --output <file>   Output HTML file (default: stdout)
  -h, --help            Show this help message`);
	process.exit(1);
}

// Parse args
const args = process.argv.slice(2);
let inputFile = null;
let outputFile = null;

for (let i = 0; i < args.length; i++) {
	if (args[i] === '-h' || args[i] === '--help') {
		usage();
	} else if (args[i] === '-o' || args[i] === '--output') {
		outputFile = args[++i];
	} else if (!inputFile) {
		inputFile = args[i];
	}
}

if (!inputFile) {
	console.error('Error: No input file specified.');
	usage();
}

// Read and validate input
const inputPath = resolve(inputFile);
if (!existsSync(inputPath)) {
	console.error(`Error: File not found: ${inputPath}`);
	process.exit(1);
}

let config;
try {
	const raw = readFileSync(inputPath, 'utf-8');
	config = JSON.parse(raw);
} catch (err) {
	console.error(`Error: Failed to parse ${inputPath}: ${err.message}`);
	process.exit(1);
}

// Read the built app files
const appDir = resolve(__dirname, '../dist/app');
const assetsDir = join(appDir, 'assets');

if (!existsSync(appDir) || !existsSync(assetsDir)) {
	console.error('Error: Built app not found. Run "npm run build:app" in netrun-ui-vis first.');
	console.error(`  Expected: ${appDir}`);
	process.exit(1);
}

// Find the JS and CSS files in assets
const assetFiles = readdirSync(assetsDir);
const jsFile = assetFiles.find(f => f.endsWith('.js'));
const cssFile = assetFiles.find(f => f.endsWith('.css'));

if (!jsFile) {
	console.error('Error: No JS bundle found in dist/app/assets/');
	process.exit(1);
}

const jsContent = readFileSync(join(assetsDir, jsFile), 'utf-8');
const cssContent = cssFile ? readFileSync(join(assetsDir, cssFile), 'utf-8') : '';

// Extract title from config
const extra = config.extra || {};
const description = extra.description || 'Netrun Graph';

// Build the single-file HTML
const configJson = JSON.stringify(config);
const html = `<!DOCTYPE html>
<html lang="en">
<head>
	<meta charset="UTF-8" />
	<meta name="viewport" content="width=device-width, initial-scale=1.0" />
	<title>Netrun: ${escapeHtml(description)}</title>
	<style>
		html, body {
			margin: 0;
			padding: 0;
			width: 100%;
			height: 100%;
			overflow: hidden;
			background: #1a1a1a;
			font-family: 'SF Mono', Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace;
		}
		#app {
			width: 100%;
			height: 100%;
		}
	</style>
${cssContent ? `	<style>${cssContent}</style>` : ''}
</head>
<body>
	<script>window.__NETRUN_CONFIG__ = ${configJson};</script>
	<div id="app"></div>
	<script type="module">${jsContent}</script>
</body>
</html>`;

// Output
if (outputFile) {
	writeFileSync(resolve(outputFile), html, 'utf-8');
	console.error(`Wrote ${resolve(outputFile)}`);
} else {
	process.stdout.write(html);
}

function escapeHtml(str) {
	return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}
