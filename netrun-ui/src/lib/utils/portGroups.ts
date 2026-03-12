/**
 * Port group utilities.
 *
 * Re-exports from netrun-ui-vis for backwards compatibility.
 */
export {
	type PortGroupTree,
	type PortLeaf,
	type PortDisplayItem,
	ROOT_GROUP_PATH,
	makeGroupHandleId,
	parseGroupHandleId,
	isGroupHandle,
	buildPortTree,
	getLeafPorts,
	getAllLeafPorts,
	getGroupLeafSuffixes,
	findGroupInTree,
	areGroupsCompatible,
	getGroupConnectionPairs,
	getPortGroupPath,
	getGroupPortNames,
	countVisibleRows,
	getVisibleHandleIds,
	getAllHandleIds,
} from 'netrun-ui-vis';
