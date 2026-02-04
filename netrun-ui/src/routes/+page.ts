/**
 * Page load function for extracting URL query parameters
 *
 * Supports:
 * - ?file=/path/to/file.netrun.json - Open a specific file
 * - ?file=path1&file=path2 - Open multiple files
 * - ?node=NodeName - Select a specific node (after file loads)
 */
import type { PageLoad } from './$types';

export const load: PageLoad = ({ url }) => {
	// Get all file parameters (supports multiple files)
	const fileParams = url.searchParams.getAll('file').filter(f => f.length > 0);

	// Get optional node selection parameter
	const nodeParam = url.searchParams.get('node');

	return {
		initialFiles: fileParams,
		initialNode: nodeParam,
	};
};
