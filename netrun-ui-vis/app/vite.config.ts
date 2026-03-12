import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';
import { resolve } from 'path';

export default defineConfig({
	plugins: [
		svelte(),
	],
	root: resolve(__dirname),
	build: {
		outDir: resolve(__dirname, '../dist/app'),
		emptyOutDir: true,
		// Inline all assets to produce a self-contained HTML
		assetsInlineLimit: Infinity,
		rollupOptions: {
			output: {
				// Single chunk for everything
				inlineDynamicImports: true,
			},
		},
	},
	resolve: {
		alias: {
			// Resolve vis package imports to source
			'netrun-ui-vis': resolve(__dirname, '../src'),
		},
	},
});
