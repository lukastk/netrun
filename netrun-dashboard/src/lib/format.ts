/** Format a duration in milliseconds to a human-readable string. */
export function formatDuration(ms: number | null): string {
	if (ms === null) return '-';
	if (ms < 1000) return `${ms.toFixed(0)}ms`;
	return `${(ms / 1000).toFixed(2)}s`;
}

/** Format an ISO timestamp to HH:MM:SS. */
export function formatTime(iso: string): string {
	try {
		const d = new Date(iso);
		return d.toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
	} catch {
		return iso;
	}
}

/** Format an ISO timestamp to HH:MM:SS.mmm. */
export function formatTimeMs(iso: string): string {
	try {
		const d = new Date(iso);
		return d.toLocaleTimeString('en-US', {
			hour12: false,
			hour: '2-digit',
			minute: '2-digit',
			second: '2-digit',
			fractionalSecondDigits: 3,
		});
	} catch {
		return iso;
	}
}

/** Format an arbitrary value for display. */
export function formatFieldValue(v: unknown): string {
	if (v === null || v === undefined) return 'null';
	if (typeof v === 'string') return v;
	return JSON.stringify(v);
}

/** CSS class for epoch state badge. */
export function stateClass(state: string): string {
	switch (state) {
		case 'finished': return 'state-finished';
		case 'running': return 'state-running';
		case 'startable': return 'state-startable';
		case 'cancelled': return 'state-cancelled';
		default: return '';
	}
}

/** CSS class for epoch outcome badge. */
export function outcomeClass(outcome: string | null): string {
	if (!outcome) return '';
	if (outcome === 'success') return 'outcome-success';
	if (outcome === 'error') return 'outcome-error';
	if (outcome === 'cancelled') return 'outcome-cancelled';
	return '';
}
