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
		const centerX = rect.left + rect.width / 2;
		const tooltipRect = el.getBoundingClientRect();
		const margin = 8;

		// Default: centered above the icon via CSS transform translateX(-50%)
		let left = centerX;
		let transform = 'translateX(-50%) translateY(-100%)';

		// Clamp to viewport edges
		const halfWidth = tooltipRect.width / 2;
		if (centerX + halfWidth > window.innerWidth - margin) {
			// Would overflow right — anchor right edge to viewport
			left = window.innerWidth - margin;
			transform = 'translateX(-100%) translateY(-100%)';
		} else if (centerX - halfWidth < margin) {
			// Would overflow left — anchor left edge to viewport
			left = margin;
			transform = 'translateY(-100%)';
		}

		el.style.left = `${left}px`;
		el.style.top = `${rect.top - 6}px`;
		el.style.transform = transform;
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
