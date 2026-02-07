# Missing features

## 1. `undeclared_output_behavior`

Currently it's either "discard" or "error", but this is a bit in conflict with the fact that the default behaviour is to have an output queue for each unconnected port if `output_queues` is `None`. So I think we should change this to a boolean that, if `True`, means there should be an error if a packet gets sent through an unconnected output port (and that port does not have an output queue). The use case for this is just to unambiguously enforce that all output ports must either be connected or have an output queue.

I think we can also remove `catch_all_output_queue`, as it's a bit redundant considering that if `output_queues` is `None` then we have an output queue for each unconnected port.

## 2. Add feature to `netrun.node_factories.from_function`

I think we should add a feature to `netrun.node_factories.from_function` that can make it so that we can select certain return values of the functions to instead be packets instead of the actual values. This should be done in the return annotation of the function, where you can specify the `PortConfig` and perhaps additional metadata. Please propose what this should look like.

Also, there should be more documentation in `get_node_config` and in `_factory_desc` on the features of `from_function`. For example, it should say how it infers what output ports the functions have from the return annotation (see `_parse_return_annotation`).

## 3. Disable sending packets in `from_function`

I want an additional argument in the `from_function` factory that, if `True` (and it should be `False` by default), disables the node from sending outputs returned by the function (in fact, it should raise an exception if anything but `None` is returned by the function). The use case for this is mainly for when a `from_function` factory node has the `ctx: NodeExecutionContext`, and wants to send off its packets on its own. In which case, it should not return anything.

## 4. `NodeExecutionConfig.max_parallel_epochs`

Limit concurrent running epochs per node. Please implement this.

## 5. `start_node_func` and `stop_node_func`

Function called when a node starts up, and function called when a node shuts down. These configs are parsed but never called during the Net lifecycle.

And additionally, you should thus also implement the `defer_startup` config.