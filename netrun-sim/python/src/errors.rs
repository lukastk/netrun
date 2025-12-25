use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

// Base exception for all netrun errors
create_exception!(netrun_sim, NetrunError, PyException);

// Exception hierarchy matching NetActionError variants
create_exception!(netrun_sim, PacketNotFoundError, NetrunError);
create_exception!(netrun_sim, EpochNotFoundError, NetrunError);
create_exception!(netrun_sim, EpochNotRunningError, NetrunError);
create_exception!(netrun_sim, EpochNotStartableError, NetrunError);
create_exception!(netrun_sim, CannotFinishNonEmptyEpochError, NetrunError);
create_exception!(netrun_sim, PacketNotInNodeError, NetrunError);
create_exception!(netrun_sim, OutputPortNotFoundError, NetrunError);
create_exception!(netrun_sim, OutputPortFullError, NetrunError);
create_exception!(netrun_sim, SalvoConditionNotFoundError, NetrunError);
create_exception!(netrun_sim, SalvoConditionNotMetError, NetrunError);
create_exception!(netrun_sim, MaxSalvosExceededError, NetrunError);
create_exception!(netrun_sim, NodeNotFoundError, NetrunError);
create_exception!(netrun_sim, PacketNotAtInputPortError, NetrunError);
create_exception!(netrun_sim, InputPortNotFoundError, NetrunError);
create_exception!(netrun_sim, InputPortFullError, NetrunError);
create_exception!(netrun_sim, CannotMovePacketFromRunningEpochError, NetrunError);
create_exception!(netrun_sim, CannotMovePacketIntoRunningEpochError, NetrunError);
create_exception!(netrun_sim, EdgeNotFoundError, NetrunError);
create_exception!(netrun_sim, UnconnectedOutputPortError, NetrunError);
create_exception!(netrun_sim, GraphValidationError, NetrunError);

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("NetrunError", m.py().get_type::<NetrunError>())?;
    m.add("PacketNotFoundError", m.py().get_type::<PacketNotFoundError>())?;
    m.add("EpochNotFoundError", m.py().get_type::<EpochNotFoundError>())?;
    m.add("EpochNotRunningError", m.py().get_type::<EpochNotRunningError>())?;
    m.add("EpochNotStartableError", m.py().get_type::<EpochNotStartableError>())?;
    m.add("CannotFinishNonEmptyEpochError", m.py().get_type::<CannotFinishNonEmptyEpochError>())?;
    m.add("PacketNotInNodeError", m.py().get_type::<PacketNotInNodeError>())?;
    m.add("OutputPortNotFoundError", m.py().get_type::<OutputPortNotFoundError>())?;
    m.add("OutputPortFullError", m.py().get_type::<OutputPortFullError>())?;
    m.add("SalvoConditionNotFoundError", m.py().get_type::<SalvoConditionNotFoundError>())?;
    m.add("SalvoConditionNotMetError", m.py().get_type::<SalvoConditionNotMetError>())?;
    m.add("MaxSalvosExceededError", m.py().get_type::<MaxSalvosExceededError>())?;
    m.add("NodeNotFoundError", m.py().get_type::<NodeNotFoundError>())?;
    m.add("PacketNotAtInputPortError", m.py().get_type::<PacketNotAtInputPortError>())?;
    m.add("InputPortNotFoundError", m.py().get_type::<InputPortNotFoundError>())?;
    m.add("InputPortFullError", m.py().get_type::<InputPortFullError>())?;
    m.add("CannotMovePacketFromRunningEpochError", m.py().get_type::<CannotMovePacketFromRunningEpochError>())?;
    m.add("CannotMovePacketIntoRunningEpochError", m.py().get_type::<CannotMovePacketIntoRunningEpochError>())?;
    m.add("EdgeNotFoundError", m.py().get_type::<EdgeNotFoundError>())?;
    m.add("UnconnectedOutputPortError", m.py().get_type::<UnconnectedOutputPortError>())?;
    m.add("GraphValidationError", m.py().get_type::<GraphValidationError>())?;
    Ok(())
}

/// Convert a NetActionError to the appropriate Python exception
pub fn net_action_error_to_py_err(err: netrun_sim::net::NetActionError) -> PyErr {
    use netrun_sim::net::NetActionError;
    match err {
        NetActionError::PacketNotFound { packet_id } => {
            PacketNotFoundError::new_err(format!("Packet not found: {}", packet_id))
        }
        NetActionError::EpochNotFound { epoch_id } => {
            EpochNotFoundError::new_err(format!("Epoch not found: {}", epoch_id))
        }
        NetActionError::EpochNotRunning { epoch_id } => {
            EpochNotRunningError::new_err(format!("Epoch not running: {}", epoch_id))
        }
        NetActionError::EpochNotStartable { epoch_id } => {
            EpochNotStartableError::new_err(format!("Epoch not startable: {}", epoch_id))
        }
        NetActionError::CannotFinishNonEmptyEpoch { epoch_id } => {
            CannotFinishNonEmptyEpochError::new_err(format!(
                "Cannot finish epoch with packets remaining: {}",
                epoch_id
            ))
        }
        NetActionError::PacketNotInNode { packet_id, epoch_id } => {
            PacketNotInNodeError::new_err(format!(
                "Packet {} not in epoch {}",
                packet_id, epoch_id
            ))
        }
        NetActionError::OutputPortNotFound { epoch_id, port_name } => {
            OutputPortNotFoundError::new_err(format!(
                "Output port '{}' not found in epoch {}",
                port_name, epoch_id
            ))
        }
        NetActionError::OutputPortFull { epoch_id, port_name } => {
            OutputPortFullError::new_err(format!(
                "Output port '{}' is full in epoch {}",
                port_name, epoch_id
            ))
        }
        NetActionError::OutputSalvoConditionNotFound { epoch_id, condition_name } => {
            SalvoConditionNotFoundError::new_err(format!(
                "Salvo condition '{}' not found for epoch {}",
                condition_name, epoch_id
            ))
        }
        NetActionError::MaxOutputSalvosReached { epoch_id, condition_name } => {
            MaxSalvosExceededError::new_err(format!(
                "Max salvos exceeded for condition '{}' in epoch {}",
                condition_name, epoch_id
            ))
        }
        NetActionError::SalvoConditionNotMet { epoch_id, condition_name } => {
            SalvoConditionNotMetError::new_err(format!(
                "Salvo condition '{}' not met for epoch {}",
                condition_name, epoch_id
            ))
        }
        NetActionError::NodeNotFound { node_name } => {
            NodeNotFoundError::new_err(format!("Node not found: {}", node_name))
        }
        NetActionError::PacketNotAtInputPort { packet_id, port_name, node_name } => {
            PacketNotAtInputPortError::new_err(format!(
                "Packet {} not at input port '{}' on node '{}'",
                packet_id, port_name, node_name
            ))
        }
        NetActionError::InputPortNotFound { node_name, port_name } => {
            InputPortNotFoundError::new_err(format!(
                "Input port '{}' not found on node {}",
                port_name, node_name
            ))
        }
        NetActionError::InputPortFull { node_name, port_name } => {
            InputPortFullError::new_err(format!(
                "Input port '{}' is full on node {}",
                port_name, node_name
            ))
        }
        NetActionError::CannotMovePacketFromRunningEpoch { packet_id, epoch_id } => {
            CannotMovePacketFromRunningEpochError::new_err(format!(
                "Cannot move packet {} from running epoch {}",
                packet_id, epoch_id
            ))
        }
        NetActionError::CannotMovePacketIntoRunningEpoch { packet_id, epoch_id } => {
            CannotMovePacketIntoRunningEpochError::new_err(format!(
                "Cannot move packet {} into running epoch {}",
                packet_id, epoch_id
            ))
        }
        NetActionError::EdgeNotFound { edge_ref } => {
            EdgeNotFoundError::new_err(format!("Edge not found: {}", edge_ref))
        }
        NetActionError::CannotPutPacketIntoUnconnectedOutputPort { port_name, node_name } => {
            UnconnectedOutputPortError::new_err(format!(
                "Output port '{}' on node '{}' is not connected to any edge",
                port_name, node_name
            ))
        }
    }
}
