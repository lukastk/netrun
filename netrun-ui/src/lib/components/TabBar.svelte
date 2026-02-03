<script lang="ts">
	import {
		tabs,
		activeTabId,
		switchTab,
		closeTab,
		createTab,
	} from '$lib/stores/flowStore';

	// Handle tab click
	function handleTabClick(tabId: string) {
		switchTab(tabId);
	}

	// Handle tab close (prevent event bubbling)
	function handleTabClose(event: MouseEvent, tabId: string) {
		event.stopPropagation();
		closeTab(tabId);
	}

	// Handle middle-click to close
	function handleTabMouseDown(event: MouseEvent, tabId: string) {
		if (event.button === 1) {
			// Middle click
			event.preventDefault();
			closeTab(tabId);
		}
	}

	// Handle new tab
	function handleNewTab() {
		createTab();
	}
</script>

<div class="tab-bar">
	<div class="tabs-container">
		{#each $tabs as tab (tab.id)}
			<div
				class="tab"
				class:active={tab.id === $activeTabId}
				onclick={() => handleTabClick(tab.id)}
				onmousedown={(e) => handleTabMouseDown(e, tab.id)}
				onkeydown={(e) => e.key === 'Enter' && handleTabClick(tab.id)}
				title={tab.filePath || 'Untitled'}
				role="tab"
				tabindex="0"
				aria-selected={tab.id === $activeTabId}
			>
				<span class="tab-name">
					{tab.fileName}
					{#if tab.isDirty}<span class="dirty-indicator">*</span>{/if}
				</span>
				<button
					class="close-btn"
					onclick={(e) => handleTabClose(e, tab.id)}
					title="Close tab"
				>
					×
				</button>
			</div>
		{/each}
	</div>

	<button class="new-tab-btn" onclick={handleNewTab} title="New tab (Cmd+T)">
		+
	</button>
</div>

<style>
	.tab-bar {
		display: flex;
		align-items: center;
		height: 32px;
		background: var(--bg-primary, #1a1a1a);
		border-bottom: 1px solid var(--border-color, #404040);
		padding: 0 4px;
		gap: 4px;
		overflow: hidden;
	}

	.tabs-container {
		display: flex;
		align-items: center;
		gap: 2px;
		overflow-x: auto;
		flex: 1;
		scrollbar-width: none;
	}

	.tabs-container::-webkit-scrollbar {
		display: none;
	}

	.tab {
		display: flex;
		align-items: center;
		gap: 6px;
		height: 28px;
		padding: 0 8px 0 12px;
		background: var(--bg-secondary, #242424);
		border: 1px solid transparent;
		border-radius: 6px 6px 0 0;
		color: var(--text-secondary, #a0a0a0);
		font-size: 12px;
		cursor: pointer;
		white-space: nowrap;
		transition: all 0.1s ease;
		min-width: 0;
		max-width: 180px;
	}

	.tab:hover {
		background: var(--bg-tertiary, #2d2d2d);
		color: var(--text-primary, #fff);
	}

	.tab.active {
		background: var(--bg-secondary, #242424);
		border-color: var(--border-color, #404040);
		border-bottom-color: var(--bg-secondary, #242424);
		color: var(--text-primary, #fff);
		margin-bottom: -1px;
		height: 29px;
	}

	.tab-name {
		overflow: hidden;
		text-overflow: ellipsis;
		white-space: nowrap;
		flex: 1;
	}

	.dirty-indicator {
		color: var(--accent-color, #3b82f6);
		margin-left: 2px;
	}

	.close-btn {
		display: flex;
		align-items: center;
		justify-content: center;
		width: 16px;
		height: 16px;
		padding: 0;
		background: transparent;
		border: none;
		border-radius: 3px;
		color: var(--text-secondary, #a0a0a0);
		font-size: 14px;
		line-height: 1;
		cursor: pointer;
		opacity: 0;
		transition: opacity 0.1s ease, background 0.1s ease;
	}

	.tab:hover .close-btn,
	.tab.active .close-btn {
		opacity: 1;
	}

	.close-btn:hover {
		background: var(--border-color, #404040);
		color: var(--text-primary, #fff);
	}

	.new-tab-btn {
		display: flex;
		align-items: center;
		justify-content: center;
		width: 24px;
		height: 24px;
		padding: 0;
		background: transparent;
		border: 1px solid transparent;
		border-radius: 4px;
		color: var(--text-secondary, #a0a0a0);
		font-size: 16px;
		cursor: pointer;
		flex-shrink: 0;
		transition: all 0.1s ease;
	}

	.new-tab-btn:hover {
		background: var(--bg-tertiary, #2d2d2d);
		color: var(--text-primary, #fff);
	}
</style>
