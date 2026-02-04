<script lang="ts">
	import {
		commandPaletteOpen,
		closeCommandPalette,
		commands,
		searchCommands,
		executeCommand,
		recentCommands,
		categoryLabels,
		type Command,
		type CommandCategory
	} from '$lib/stores/commandStore';
	import { getShortcutForCommand } from '$lib/stores/keyboardStore';

	let searchQuery = $state('');
	let selectedIndex = $state(0);
	let inputRef = $state<HTMLInputElement | null>(null);

	// Filtered commands based on search
	const filteredCommands = $derived.by(() => {
		if (searchQuery.trim()) {
			return searchCommands(searchQuery);
		}
		// Show all commands grouped by category when no search
		return $commands;
	});

	// Get recent commands for display
	const recentForDisplay = $derived.by(() => {
		if (searchQuery.trim()) return [];
		return $recentCommands.slice(0, 5);
	});

	// Group commands by category for display
	const groupedCommands = $derived.by(() => {
		if (searchQuery.trim()) {
			// When searching, show flat list
			return null;
		}
		const grouped = new Map<CommandCategory, Command[]>();
		const categoryOrder: CommandCategory[] = ['file', 'edit', 'view', 'node', 'subgraph', 'tab'];

		for (const category of categoryOrder) {
			grouped.set(category, []);
		}

		for (const cmd of filteredCommands) {
			const list = grouped.get(cmd.category) || [];
			list.push(cmd);
			grouped.set(cmd.category, list);
		}

		// Remove empty categories
		for (const [category, cmds] of grouped) {
			if (cmds.length === 0) {
				grouped.delete(category);
			}
		}

		return grouped;
	});

	// All visible commands in order (for keyboard navigation)
	const visibleCommands = $derived.by(() => {
		const result: Command[] = [];

		// Add recent commands first (if not searching)
		if (!searchQuery.trim() && recentForDisplay.length > 0) {
			result.push(...recentForDisplay);
		}

		if (searchQuery.trim()) {
			// Flat list when searching
			result.push(...filteredCommands);
		} else if (groupedCommands) {
			// Grouped list otherwise
			for (const [, cmds] of groupedCommands) {
				result.push(...cmds);
			}
		}

		return result;
	});

	// Reset selection when search changes
	$effect(() => {
		searchQuery; // Track dependency
		selectedIndex = 0;
	});

	// Focus input when palette opens
	$effect(() => {
		if ($commandPaletteOpen && inputRef) {
			searchQuery = '';
			selectedIndex = 0;
			// Use setTimeout to ensure the element is visible
			setTimeout(() => inputRef?.focus(), 10);
		}
	});

	function handleKeydown(event: KeyboardEvent) {
		if (event.key === 'Escape') {
			event.preventDefault();
			closeCommandPalette();
			return;
		}

		if (event.key === 'ArrowDown') {
			event.preventDefault();
			selectedIndex = Math.min(selectedIndex + 1, visibleCommands.length - 1);
			return;
		}

		if (event.key === 'ArrowUp') {
			event.preventDefault();
			selectedIndex = Math.max(selectedIndex - 1, 0);
			return;
		}

		if (event.key === 'Enter') {
			event.preventDefault();
			const cmd = visibleCommands[selectedIndex];
			if (cmd && (!cmd.enabled || cmd.enabled())) {
				closeCommandPalette();
				executeCommand(cmd.id);
			}
			return;
		}
	}

	function handleCommandClick(cmd: Command) {
		if (cmd.enabled && !cmd.enabled()) return;
		closeCommandPalette();
		executeCommand(cmd.id);
	}

	function handleBackdropClick(event: MouseEvent) {
		if (event.target === event.currentTarget) {
			closeCommandPalette();
		}
	}

	function isCommandEnabled(cmd: Command): boolean {
		return !cmd.enabled || cmd.enabled();
	}

	function getCommandShortcut(cmd: Command): string | null {
		return cmd.shortcut || getShortcutForCommand(cmd.id);
	}

	// Highlight matching text in search results
	function highlightMatch(text: string, query: string): string {
		if (!query.trim()) return text;
		const regex = new RegExp(`(${query.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`, 'gi');
		return text.replace(regex, '<mark>$1</mark>');
	}

	// Track which command is at which index for selection
	let commandIndexMap = $derived.by(() => {
		const map = new Map<string, number>();
		visibleCommands.forEach((cmd, idx) => {
			map.set(cmd.id, idx);
		});
		return map;
	});
</script>

{#if $commandPaletteOpen}
	<!-- svelte-ignore a11y_click_events_have_key_events -->
	<!-- svelte-ignore a11y_no_static_element_interactions -->
	<div class="backdrop" onclick={handleBackdropClick}>
		<div class="palette" role="dialog" aria-label="Command Palette">
			<div class="search-container">
				<span class="search-icon">
					<svg
						width="16"
						height="16"
						viewBox="0 0 24 24"
						fill="none"
						stroke="currentColor"
						stroke-width="2"
					>
						<circle cx="11" cy="11" r="8" />
						<path d="m21 21-4.35-4.35" />
					</svg>
				</span>
				<input
					bind:this={inputRef}
					bind:value={searchQuery}
					onkeydown={handleKeydown}
					type="text"
					placeholder="Search commands..."
					class="search-input"
					aria-label="Search commands"
				/>
			</div>

			<div class="commands-list">
				{#if visibleCommands.length === 0}
					<div class="empty-state">No commands found</div>
				{:else}
					<!-- Recent commands -->
					{#if !searchQuery.trim() && recentForDisplay.length > 0}
						<div class="category-header">Recent</div>
						{#each recentForDisplay as cmd}
							{@const idx = commandIndexMap.get(cmd.id) ?? -1}
							{@const isSelected = idx === selectedIndex}
							{@const isEnabled = isCommandEnabled(cmd)}
							{@const shortcut = getCommandShortcut(cmd)}
							<!-- svelte-ignore a11y_click_events_have_key_events -->
							<!-- svelte-ignore a11y_no_static_element_interactions -->
							<div
								class="command-item"
								class:selected={isSelected}
								class:disabled={!isEnabled}
								onclick={() => handleCommandClick(cmd)}
							>
								<span class="command-label">{cmd.label}</span>
								{#if shortcut}
									<span class="command-shortcut">{shortcut}</span>
								{/if}
							</div>
						{/each}
						<div class="category-separator"></div>
					{/if}

					<!-- Search results (flat list) -->
					{#if searchQuery.trim()}
						{#each filteredCommands as cmd}
							{@const idx = commandIndexMap.get(cmd.id) ?? -1}
							{@const isSelected = idx === selectedIndex}
							{@const isEnabled = isCommandEnabled(cmd)}
							{@const shortcut = getCommandShortcut(cmd)}
							<!-- svelte-ignore a11y_click_events_have_key_events -->
							<!-- svelte-ignore a11y_no_static_element_interactions -->
							<div
								class="command-item"
								class:selected={isSelected}
								class:disabled={!isEnabled}
								onclick={() => handleCommandClick(cmd)}
							>
								<span class="command-label">
									{@html highlightMatch(cmd.label, searchQuery)}
								</span>
								<span class="command-category">{categoryLabels[cmd.category]}</span>
								{#if shortcut}
									<span class="command-shortcut">{shortcut}</span>
								{/if}
							</div>
						{/each}
					{:else if groupedCommands}
						<!-- Grouped commands -->
						{#each [...groupedCommands] as [category, cmds]}
							<div class="category-header">{categoryLabels[category]}</div>
							{#each cmds as cmd}
								{@const idx = commandIndexMap.get(cmd.id) ?? -1}
								{@const isSelected = idx === selectedIndex}
								{@const isEnabled = isCommandEnabled(cmd)}
								{@const shortcut = getCommandShortcut(cmd)}
								<!-- svelte-ignore a11y_click_events_have_key_events -->
								<!-- svelte-ignore a11y_no_static_element_interactions -->
								<div
									class="command-item"
									class:selected={isSelected}
									class:disabled={!isEnabled}
									onclick={() => handleCommandClick(cmd)}
								>
									<span class="command-label">{cmd.label}</span>
									{#if shortcut}
										<span class="command-shortcut">{shortcut}</span>
									{/if}
								</div>
							{/each}
						{/each}
					{/if}
				{/if}
			</div>

			<div class="palette-footer">
				<span class="hint">
					<kbd>↑↓</kbd> navigate
					<kbd>↵</kbd> select
					<kbd>esc</kbd> close
				</span>
			</div>
		</div>
	</div>
{/if}

<style>
	.backdrop {
		position: fixed;
		top: 0;
		left: 0;
		right: 0;
		bottom: 0;
		background: rgba(0, 0, 0, 0.5);
		backdrop-filter: blur(2px);
		display: flex;
		justify-content: center;
		padding-top: 15vh;
		z-index: 1000;
	}

	.palette {
		width: 500px;
		max-width: 90vw;
		max-height: 60vh;
		background: var(--bg-secondary, #242424);
		border: 1px solid var(--border-color, #404040);
		border-radius: 8px;
		box-shadow:
			0 16px 48px rgba(0, 0, 0, 0.4),
			0 4px 16px rgba(0, 0, 0, 0.2);
		display: flex;
		flex-direction: column;
		overflow: hidden;
	}

	.search-container {
		display: flex;
		align-items: center;
		padding: 12px 16px;
		border-bottom: 1px solid var(--border-color, #404040);
		gap: 10px;
	}

	.search-icon {
		color: var(--text-secondary, #a0a0a0);
		display: flex;
		align-items: center;
	}

	.search-input {
		flex: 1;
		background: transparent;
		border: none;
		color: var(--text-primary, #fff);
		font-size: 14px;
		outline: none;
	}

	.search-input::placeholder {
		color: var(--text-secondary, #a0a0a0);
	}

	.commands-list {
		flex: 1;
		overflow-y: auto;
		padding: 8px 0;
	}

	.category-header {
		padding: 8px 16px 4px;
		font-size: 11px;
		font-weight: 600;
		text-transform: uppercase;
		color: var(--text-secondary, #a0a0a0);
		letter-spacing: 0.05em;
	}

	.category-separator {
		height: 1px;
		background: var(--border-color, #404040);
		margin: 8px 16px;
	}

	.command-item {
		display: flex;
		align-items: center;
		padding: 8px 16px;
		cursor: pointer;
		gap: 12px;
	}

	.command-item:hover,
	.command-item.selected {
		background: var(--bg-tertiary, #2d2d2d);
	}

	.command-item.selected {
		background: var(--accent-color, #3b82f6);
	}

	.command-item.disabled {
		opacity: 0.5;
		cursor: not-allowed;
	}

	.command-label {
		flex: 1;
		color: var(--text-primary, #fff);
		font-size: 13px;
	}

	.command-label :global(mark) {
		background: var(--accent-color, #3b82f6);
		color: inherit;
		padding: 0 2px;
		border-radius: 2px;
	}

	.command-category {
		font-size: 11px;
		color: var(--text-secondary, #a0a0a0);
		background: var(--bg-tertiary, #2d2d2d);
		padding: 2px 6px;
		border-radius: 3px;
	}

	.command-shortcut {
		font-size: 12px;
		color: var(--text-secondary, #a0a0a0);
		font-family: inherit;
	}

	.empty-state {
		padding: 24px 16px;
		text-align: center;
		color: var(--text-secondary, #a0a0a0);
		font-size: 13px;
	}

	.palette-footer {
		padding: 8px 16px;
		border-top: 1px solid var(--border-color, #404040);
		display: flex;
		justify-content: center;
	}

	.hint {
		font-size: 11px;
		color: var(--text-secondary, #a0a0a0);
		display: flex;
		gap: 12px;
	}

	.hint kbd {
		background: var(--bg-tertiary, #2d2d2d);
		padding: 2px 6px;
		border-radius: 3px;
		font-family: inherit;
		font-size: 10px;
		margin-right: 4px;
	}
</style>
