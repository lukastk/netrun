/**
 * Svelte action that shows a tooltip on hover.
 * Renders into document.body so it escapes overflow clipping.
 *
 * Usage: <span use:tooltip={"Description text"}>?</span>
 */
export function tooltip(node: HTMLElement, text: string | undefined) {
	let el: HTMLDivElement | null = null;

	function show() {
		if (!text) return;
		el = document.createElement('div');
		el.className = 'global-tooltip';
		el.textContent = text;
		document.body.appendChild(el);
		position();
	}

	function position() {
		if (!el) return;
		const rect = node.getBoundingClientRect();
		// Place above the icon, centered horizontally
		el.style.left = `${rect.left + rect.width / 2}px`;
		el.style.top = `${rect.top - 6}px`;
	}

	function hide() {
		if (el) {
			el.remove();
			el = null;
		}
	}

	node.addEventListener('mouseenter', show);
	node.addEventListener('mouseleave', hide);

	return {
		update(newText: string | undefined) {
			text = newText;
			if (el) {
				if (!text) {
					hide();
				} else {
					el.textContent = text;
					position();
				}
			}
		},
		destroy() {
			hide();
			node.removeEventListener('mouseenter', show);
			node.removeEventListener('mouseleave', hide);
		}
	};
}
