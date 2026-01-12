use super::*;

// Helper functions for tests
fn simple_port() -> Port {
    Port {
        slots_spec: PortSlotSpec::Infinite,
    }
}

fn simple_node(name: &str, in_ports: Vec<&str>, out_ports: Vec<&str>) -> Node {
    let in_ports_map: HashMap<PortName, Port> = in_ports
        .iter()
        .map(|p| (p.to_string(), simple_port()))
        .collect();
    let out_ports_map: HashMap<PortName, Port> = out_ports
        .iter()
        .map(|p| (p.to_string(), simple_port()))
        .collect();

    // Default input salvo condition
    let mut in_salvo_conditions = HashMap::new();
    if !in_ports.is_empty() {
        in_salvo_conditions.insert(
            "default".to_string(),
            SalvoCondition {
                max_salvos: MaxSalvos::Finite(1),
                ports: in_ports
                    .iter()
                    .map(|s| (s.to_string(), PacketCount::All))
                    .collect(),
                term: SalvoConditionTerm::Port {
                    port_name: in_ports[0].to_string(),
                    state: PortState::NonEmpty,
                },
            },
        );
    }

    Node {
        name: name.to_string(),
        in_ports: in_ports_map,
        out_ports: out_ports_map,
        in_salvo_conditions,
        out_salvo_conditions: HashMap::new(),
    }
}

fn edge(src_node: &str, src_port: &str, tgt_node: &str, tgt_port: &str) -> Edge {
    Edge {
        source: PortRef {
            node_name: src_node.to_string(),
            port_type: PortType::Output,
            port_name: src_port.to_string(),
        },
        target: PortRef {
            node_name: tgt_node.to_string(),
            port_type: PortType::Input,
            port_name: tgt_port.to_string(),
        },
    }
}

#[test]
fn test_valid_graph_passes_validation() {
    let nodes = vec![
        simple_node("A", vec![], vec!["out"]),
        simple_node("B", vec!["in"], vec![]),
    ];
    let edges = vec![edge("A", "out", "B", "in")];
    let graph = Graph::new(nodes, edges);

    let errors = graph.validate();
    assert!(errors.is_empty(), "Expected no errors, got: {:?}", errors);
}

#[test]
fn test_edge_references_nonexistent_source_node() {
    let nodes = vec![simple_node("B", vec!["in"], vec![])];
    // Edge from nonexistent node "A"
    let edges = vec![edge("A", "out", "B", "in")];
    let graph = Graph::new(nodes, edges);

    let errors = graph.validate();
    assert_eq!(errors.len(), 1);
    match &errors[0] {
        GraphValidationError::EdgeReferencesNonexistentNode { missing_node, .. } => {
            assert_eq!(missing_node, "A");
        }
        _ => panic!(
            "Expected EdgeReferencesNonexistentNode, got: {:?}",
            errors[0]
        ),
    }
}

#[test]
fn test_edge_references_nonexistent_target_node() {
    let nodes = vec![simple_node("A", vec![], vec!["out"])];
    // Edge to nonexistent node "B"
    let edges = vec![edge("A", "out", "B", "in")];
    let graph = Graph::new(nodes, edges);

    let errors = graph.validate();
    assert_eq!(errors.len(), 1);
    match &errors[0] {
        GraphValidationError::EdgeReferencesNonexistentNode { missing_node, .. } => {
            assert_eq!(missing_node, "B");
        }
        _ => panic!(
            "Expected EdgeReferencesNonexistentNode, got: {:?}",
            errors[0]
        ),
    }
}

#[test]
fn test_edge_references_nonexistent_source_port() {
    let nodes = vec![
        simple_node("A", vec![], vec!["out"]),
        simple_node("B", vec!["in"], vec![]),
    ];
    // Edge from nonexistent port "wrong_port"
    let edges = vec![edge("A", "wrong_port", "B", "in")];
    let graph = Graph::new(nodes, edges);

    let errors = graph.validate();
    assert_eq!(errors.len(), 1);
    match &errors[0] {
        GraphValidationError::EdgeReferencesNonexistentPort { missing_port, .. } => {
            assert_eq!(missing_port.port_name, "wrong_port");
        }
        _ => panic!(
            "Expected EdgeReferencesNonexistentPort, got: {:?}",
            errors[0]
        ),
    }
}

#[test]
fn test_edge_references_nonexistent_target_port() {
    let nodes = vec![
        simple_node("A", vec![], vec!["out"]),
        simple_node("B", vec!["in"], vec![]),
    ];
    // Edge to nonexistent port "wrong_port"
    let edges = vec![edge("A", "out", "B", "wrong_port")];
    let graph = Graph::new(nodes, edges);

    let errors = graph.validate();
    assert_eq!(errors.len(), 1);
    match &errors[0] {
        GraphValidationError::EdgeReferencesNonexistentPort { missing_port, .. } => {
            assert_eq!(missing_port.port_name, "wrong_port");
        }
        _ => panic!(
            "Expected EdgeReferencesNonexistentPort, got: {:?}",
            errors[0]
        ),
    }
}

#[test]
fn test_edge_source_must_be_output_port() {
    let nodes = vec![
        simple_node("A", vec!["in"], vec!["out"]),
        simple_node("B", vec!["in"], vec![]),
    ];
    // Edge from input port (wrong type)
    let edges = vec![Edge {
        source: PortRef {
            node_name: "A".to_string(),
            port_type: PortType::Input, // Wrong!
            port_name: "in".to_string(),
        },
        target: PortRef {
            node_name: "B".to_string(),
            port_type: PortType::Input,
            port_name: "in".to_string(),
        },
    }];
    let graph = Graph::new(nodes, edges);

    let errors = graph.validate();
    assert!(
        errors
            .iter()
            .any(|e| matches!(e, GraphValidationError::EdgeSourceNotOutputPort { .. }))
    );
}

#[test]
fn test_edge_target_must_be_input_port() {
    let nodes = vec![
        simple_node("A", vec![], vec!["out"]),
        simple_node("B", vec!["in"], vec!["out"]),
    ];
    // Edge to output port (wrong type)
    let edges = vec![Edge {
        source: PortRef {
            node_name: "A".to_string(),
            port_type: PortType::Output,
            port_name: "out".to_string(),
        },
        target: PortRef {
            node_name: "B".to_string(),
            port_type: PortType::Output, // Wrong!
            port_name: "out".to_string(),
        },
    }];
    let graph = Graph::new(nodes, edges);

    let errors = graph.validate();
    assert!(
        errors
            .iter()
            .any(|e| matches!(e, GraphValidationError::EdgeTargetNotInputPort { .. }))
    );
}

#[test]
fn test_input_salvo_condition_must_have_max_salvos_finite_1() {
    let mut node = simple_node("A", vec!["in"], vec![]);
    // Set max_salvos to something other than Finite(1)
    node.in_salvo_conditions
        .get_mut("default")
        .unwrap()
        .max_salvos = MaxSalvos::Finite(2);

    let graph = Graph::new(vec![node], vec![]);

    let errors = graph.validate();
    assert_eq!(errors.len(), 1);
    match &errors[0] {
        GraphValidationError::InputSalvoConditionInvalidMaxSalvos { max_salvos, .. } => {
            assert_eq!(*max_salvos, MaxSalvos::Finite(2));
        }
        _ => panic!(
            "Expected InputSalvoConditionInvalidMaxSalvos, got: {:?}",
            errors[0]
        ),
    }
}

#[test]
fn test_salvo_condition_ports_must_exist() {
    let mut node = simple_node("A", vec!["in"], vec![]);
    // Reference nonexistent port in condition.ports
    node.in_salvo_conditions.get_mut("default").unwrap().ports =
        [("nonexistent".to_string(), PacketCount::All)]
            .into_iter()
            .collect();

    let graph = Graph::new(vec![node], vec![]);

    let errors = graph.validate();
    assert!(errors.iter().any(|e| matches!(
        e,
        GraphValidationError::SalvoConditionReferencesNonexistentPort { missing_port, .. }
        if missing_port == "nonexistent"
    )));
}

#[test]
fn test_salvo_condition_term_ports_must_exist() {
    let mut node = simple_node("A", vec!["in"], vec![]);
    // Reference nonexistent port in condition.term
    node.in_salvo_conditions.get_mut("default").unwrap().term = SalvoConditionTerm::Port {
        port_name: "nonexistent".to_string(),
        state: PortState::NonEmpty,
    };

    let graph = Graph::new(vec![node], vec![]);

    let errors = graph.validate();
    assert!(errors.iter().any(|e| matches!(
        e,
        GraphValidationError::SalvoConditionTermReferencesNonexistentPort { missing_port, .. }
        if missing_port == "nonexistent"
    )));
}

#[test]
fn test_output_salvo_condition_ports_must_exist() {
    let mut node = simple_node("A", vec![], vec!["out"]);
    // Add output salvo condition referencing nonexistent port
    node.out_salvo_conditions.insert(
        "test".to_string(),
        SalvoCondition {
            max_salvos: MaxSalvos::Infinite,
            ports: [("nonexistent".to_string(), PacketCount::All)]
                .into_iter()
                .collect(),
            term: SalvoConditionTerm::Port {
                port_name: "out".to_string(),
                state: PortState::NonEmpty,
            },
        },
    );

    let graph = Graph::new(vec![node], vec![]);

    let errors = graph.validate();
    assert!(errors.iter().any(|e| matches!(
        e,
        GraphValidationError::SalvoConditionReferencesNonexistentPort {
            is_input_condition: false,
            missing_port,
            ..
        } if missing_port == "nonexistent"
    )));
}

#[test]
fn test_complex_salvo_condition_term_validation() {
    let mut node = simple_node("A", vec!["in1", "in2"], vec![]);
    // Create complex term with And/Or/Not that references nonexistent port
    node.in_salvo_conditions.get_mut("default").unwrap().term = SalvoConditionTerm::And(vec![
        SalvoConditionTerm::Port {
            port_name: "in1".to_string(),
            state: PortState::NonEmpty,
        },
        SalvoConditionTerm::Or(vec![
            SalvoConditionTerm::Port {
                port_name: "in2".to_string(),
                state: PortState::NonEmpty,
            },
            SalvoConditionTerm::Not(Box::new(SalvoConditionTerm::Port {
                port_name: "nonexistent".to_string(), // This should be caught
                state: PortState::Empty,
            })),
        ]),
    ]);

    let graph = Graph::new(vec![node], vec![]);

    let errors = graph.validate();
    assert!(errors.iter().any(|e| matches!(
        e,
        GraphValidationError::SalvoConditionTermReferencesNonexistentPort { missing_port, .. }
        if missing_port == "nonexistent"
    )));
}

#[test]
fn test_empty_graph_is_valid() {
    let graph = Graph::new(vec![], vec![]);
    let errors = graph.validate();
    assert!(errors.is_empty());
}

#[test]
fn test_node_without_ports_is_valid() {
    let node = Node {
        name: "A".to_string(),
        in_ports: HashMap::new(),
        out_ports: HashMap::new(),
        in_salvo_conditions: HashMap::new(),
        out_salvo_conditions: HashMap::new(),
    };
    let graph = Graph::new(vec![node], vec![]);
    let errors = graph.validate();
    assert!(errors.is_empty());
}
