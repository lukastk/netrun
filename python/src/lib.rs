use pyo3::prelude::*;

mod errors;
mod graph;
mod net;

/// Python bindings for netrun-core, a flow-based development runtime simulation engine.
#[pymodule]
fn netrun_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Register exception types
    errors::register(m)?;

    // Register graph types
    graph::register(m)?;

    // Register net types
    net::register(m)?;

    Ok(())
}
