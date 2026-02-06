# ---
# jupyter:
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Worker Functions for Net Tests
#
# These node execution functions are defined in a separate module so they can be
# properly pickled and sent to remote pool workers.

# %%
#|default_exp net.workers

# %%
#|export
def doubler_node(ctx, packets):
    """Node that doubles the input value and sends it to 'out'."""
    for port_name, packet_ids in packets.items():
        for packet_id in packet_ids:
            value = ctx.consume_packet(packet_id)
            out_id = ctx.create_packet(value * 2)
            ctx.load_output_port("out", out_id)
    ctx.send_output_salvo("send")


def echo_node(ctx, packets):
    """Node that consumes input packets and prints their values."""
    for port_name, packet_ids in packets.items():
        for packet_id in packet_ids:
            value = ctx.consume_packet(packet_id)
            ctx.print(f"echo: {value}")
