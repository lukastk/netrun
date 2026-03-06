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
    let mut in_salvo_conditions = IndexMap::new();
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
        out_salvo_conditions: IndexMap::new(),
        dependency_request_config: None,
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
        in_salvo_conditions: IndexMap::new(),
        out_salvo_conditions: IndexMap::new(),
        dependency_request_config: None,
    };
    let graph = Graph::new(vec![node], vec![]);
    let errors = graph.validate();
    assert!(errors.is_empty());
}

#[test]
fn test_multiple_edges_to_same_input_port_is_valid() {
    // Fan-in: multiple edges can connect to the same input port
    let nodes = vec![
        simple_node("A", vec![], vec!["out"]),
        simple_node("B", vec![], vec!["out"]),
        simple_node("C", vec!["in"], vec![]),
    ];
    let edges = vec![edge("A", "out", "C", "in"), edge("B", "out", "C", "in")];
    let graph = Graph::new(nodes, edges);

    let errors = graph.validate();
    assert!(
        errors.is_empty(),
        "Fan-in should be valid, got: {:?}",
        errors
    );

    // Verify get_edges_by_head returns both edges
    let c_in_ref = PortRef {
        node_name: "C".to_string(),
        port_type: PortType::Input,
        port_name: "in".to_string(),
    };
    let incoming_edges = graph.get_edges_by_head(&c_in_ref);
    assert_eq!(incoming_edges.len(), 2, "Should have 2 incoming edges");
}

#[test]
fn test_multiple_edges_from_same_output_port_is_invalid() {
    // Fan-out: only one edge allowed per output port
    let nodes = vec![
        simple_node("A", vec![], vec!["out"]),
        simple_node("B", vec!["in"], vec![]),
        simple_node("C", vec!["in"], vec![]),
    ];
    let edges = vec![edge("A", "out", "B", "in"), edge("A", "out", "C", "in")];
    let graph = Graph::new(nodes, edges);

    let errors = graph.validate();
    assert_eq!(errors.len(), 1, "Should have exactly one error");
    match &errors[0] {
        GraphValidationError::MultipleEdgesFromOutputPort {
            output_port,
            edge_count,
        } => {
            assert_eq!(output_port.node_name, "A");
            assert_eq!(output_port.port_name, "out");
            assert_eq!(*edge_count, 2);
        }
        _ => panic!("Expected MultipleEdgesFromOutputPort, got: {:?}", errors[0]),
    }
}

#[test]
fn test_get_edges_by_head_returns_all_incoming_edges() {
    // Create a graph with multiple edges to the same input port
    let nodes = vec![
        simple_node("A", vec![], vec!["out"]),
        simple_node("B", vec![], vec!["out"]),
        simple_node("C", vec![], vec!["out"]),
        simple_node("D", vec!["in"], vec![]),
    ];
    let edges = vec![
        edge("A", "out", "D", "in"),
        edge("B", "out", "D", "in"),
        edge("C", "out", "D", "in"),
    ];
    let graph = Graph::new(nodes, edges);

    let d_in_ref = PortRef {
        node_name: "D".to_string(),
        port_type: PortType::Input,
        port_name: "in".to_string(),
    };
    let incoming_edges = graph.get_edges_by_head(&d_in_ref);
    assert_eq!(incoming_edges.len(), 3, "Should have 3 incoming edges");

    // Verify all sources are present
    let sources: Vec<&str> = incoming_edges
        .iter()
        .map(|e| e.source.node_name.as_str())
        .collect();
    assert!(sources.contains(&"A"));
    assert!(sources.contains(&"B"));
    assert!(sources.contains(&"C"));
}

#[test]
fn test_get_edges_by_head_returns_empty_for_unconnected_port() {
    let nodes = vec![simple_node("A", vec!["in"], vec!["out"])];
    let graph = Graph::new(nodes, vec![]);

    let a_in_ref = PortRef {
        node_name: "A".to_string(),
        port_type: PortType::Input,
        port_name: "in".to_string(),
    };
    let incoming_edges = graph.get_edges_by_head(&a_in_ref);
    assert!(
        incoming_edges.is_empty(),
        "Unconnected port should have no incoming edges"
    );
}

#[test]
#[should_panic(expected = "Duplicate node name: 'A'")]
fn test_duplicate_node_names_panics() {
    let nodes = vec![
        simple_node("A", vec![], vec!["out"]),
        simple_node("A", vec!["in"], vec![]),
    ];
    Graph::new(nodes, vec![]);
}

// ========== Dependency Edges & Cascade Backward Tests ==========

fn make_edge(src_node: &str, src_port: &str, tgt_node: &str, tgt_port: &str) -> Edge {
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
fn test_dependency_edges_validation_valid() {
    let nodes = vec![
        simple_node("A", vec![], vec!["out"]),
        simple_node("B", vec!["in"], vec![]),
    ];
    let e = make_edge("A", "out", "B", "in");
    let graph = Graph::new(nodes, vec![e.clone()]).with_dependency_edges(vec![e]);
    assert!(graph.validate().is_empty());
}

#[test]
fn test_dependency_edge_not_in_graph_is_invalid() {
    let nodes = vec![
        simple_node("A", vec![], vec!["out"]),
        simple_node("B", vec!["in"], vec![]),
    ];
    let e = make_edge("A", "out", "B", "in");
    // Create graph without the edge but mark it as dependency edge
    let graph = Graph::new(nodes, vec![]).with_dependency_edges(vec![e]);
    let errors = graph.validate();
    assert!(errors.iter().any(|e| matches!(
        e,
        GraphValidationError::DependencyEdgeNotInGraph { .. }
    )));
}

#[test]
fn test_dependency_request_config_without_dependency_edges_is_invalid() {
    let mut node_b = simple_node("B", vec!["in"], vec![]);
    node_b.dependency_request_config = Some(DependencyRequestConfig {
        triggers: vec![DependencyRequestTrigger::OnStartup],
        label: "test".to_string(),
    });
    let nodes = vec![simple_node("A", vec![], vec!["out"]), node_b];
    let e = make_edge("A", "out", "B", "in");
    // Edge exists but is NOT a dependency edge
    let graph = Graph::new(nodes, vec![e]);
    let errors = graph.validate();
    assert!(errors.iter().any(|e| matches!(
        e,
        GraphValidationError::DependencyRequestConfigWithoutDependencyEdges { .. }
    )));
}

#[test]
fn test_cascade_backward_linear() {
    // Source -> Mid -> Sink, dependency edge Mid->Sink
    let nodes = vec![
        simple_node("Source", vec![], vec!["out"]),
        simple_node("Mid", vec!["in"], vec!["out"]),
        simple_node("Sink", vec!["in"], vec![]),
    ];
    let e1 = make_edge("Source", "out", "Mid", "in");
    let e2 = make_edge("Mid", "out", "Sink", "in");
    let graph = Graph::new(nodes, vec![e1, e2.clone()]).with_dependency_edges(vec![e2.clone()]);
    assert!(graph.validate().is_empty());

    let result = graph
        .cascade_backward(&[e2.target.clone()])
        .expect("cascade should succeed");
    assert_eq!(result.source_nodes, vec!["Source".to_string()]);
    assert!(result.visited_nodes.contains(&"Mid".to_string()));
}

#[test]
fn test_cascade_backward_diamond() {
    // Source -> B -> Sink, Source -> C -> Sink
    let nodes = vec![
        simple_node("Source", vec![], vec!["out1", "out2"]),
        simple_node("B", vec!["in"], vec!["out"]),
        simple_node("C", vec!["in"], vec!["out"]),
        simple_node("Sink", vec!["in1", "in2"], vec![]),
    ];
    let edges = vec![
        make_edge("Source", "out1", "B", "in"),
        make_edge("Source", "out2", "C", "in"),
        make_edge("B", "out", "Sink", "in1"),
        make_edge("C", "out", "Sink", "in2"),
    ];
    let dep_edges = vec![edges[2].clone(), edges[3].clone()];
    let graph = Graph::new(nodes, edges).with_dependency_edges(dep_edges.clone());
    assert!(graph.validate().is_empty());

    let start_ports: Vec<PortRef> = dep_edges.iter().map(|e| e.target.clone()).collect();
    let result = graph
        .cascade_backward(&start_ports)
        .expect("cascade should succeed");
    // Source is the only node with no input ports
    assert_eq!(result.source_nodes, vec!["Source".to_string()]);
}

#[test]
fn test_cascade_backward_fan_in() {
    // A -> C, B -> C (both dep edges)
    let nodes = vec![
        simple_node("A", vec![], vec!["out"]),
        simple_node("B", vec![], vec!["out"]),
        simple_node("C", vec!["in1", "in2"], vec![]),
    ];
    let edges = vec![
        make_edge("A", "out", "C", "in1"),
        make_edge("B", "out", "C", "in2"),
    ];
    let graph = Graph::new(nodes, edges.clone()).with_dependency_edges(edges.clone());

    let start_ports: Vec<PortRef> = edges.iter().map(|e| e.target.clone()).collect();
    let result = graph
        .cascade_backward(&start_ports)
        .expect("cascade should succeed");
    // Both A and B are source nodes
    assert_eq!(result.source_nodes.len(), 2);
    assert!(result.source_nodes.contains(&"A".to_string()));
    assert!(result.source_nodes.contains(&"B".to_string()));
}

#[test]
fn test_cascade_backward_unconnected_port_error() {
    // B has an input port "in" with no incoming edge
    let nodes = vec![simple_node("B", vec!["in"], vec![])];
    let graph = Graph::new(nodes, vec![]);

    let port = PortRef {
        node_name: "B".to_string(),
        port_type: PortType::Input,
        port_name: "in".to_string(),
    };
    let result = graph.cascade_backward(&[port]);
    assert!(matches!(
        result,
        Err(CascadeError::UnconnectedInputPort { .. })
    ));
}

#[test]
fn test_is_dependency_edge() {
    let nodes = vec![
        simple_node("A", vec![], vec!["out"]),
        simple_node("B", vec!["in"], vec!["out"]),
        simple_node("C", vec!["in"], vec![]),
    ];
    let e1 = make_edge("A", "out", "B", "in");
    let e2 = make_edge("B", "out", "C", "in");
    let graph = Graph::new(nodes, vec![e1.clone(), e2.clone()]).with_dependency_edges(vec![e2.clone()]);

    assert!(!graph.is_dependency_edge(&e1));
    assert!(graph.is_dependency_edge(&e2));
}
