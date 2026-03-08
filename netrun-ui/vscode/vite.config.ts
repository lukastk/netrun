import { svelte } from '@sveltejs/vite-plugin-svelte';
import { defineConfig } from 'vite';
import path from 'path';

export default defineConfig({
	plugins: [svelte()],
	resolve: {
		alias: {
			'$lib': path.resolve(__dirname, '../src/lib'),
		},
	},
	build: {
		outDir: 'dist/webview',
		emptyOutDir: true,
		rollupOptions: {
			input: path.resolve(__dirname, 'src/webview/index.html'),
			output: {
				entryFileNames: 'main.js',
				chunkFileNames: '[name].js',
				assetFileNames: '[name][extname]',
			},
		},
	},
	define: {
		// Provide import.meta.env.DEV as false for production webview build
		__APP_VERSION__: JSON.stringify('vscode'),
	},
});
