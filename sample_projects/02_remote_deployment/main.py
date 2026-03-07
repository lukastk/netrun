"""Run the remote deployment example.

This demonstrates:
1. Creating a Hetzner Cloud server (idempotent)
2. Deploying the app/ folder (code + deps + auto-delete watchdog)
3. Starting the pool server + SSH tunnel
4. Running a network with a remote pool
5. Stopping the pool server and closing the SSH tunnel

The server auto-deletes after 10 minutes idle (no pool server running).

Prerequisites:
    - hcloud CLI installed and authenticated
    - SSH key registered in Hetzner Cloud
    - .env file with HCLOUD_SSH_KEY_NAME, SSH_PRIVATE_KEY_PATH, HCLOUD_API_TOKEN
"""

import asyncio
import sys
from pathlib import Path

from dotenv import load_dotenv
import os

from deploy_to_hetzner import (
    create_hetzner_server,
    deploy_to_server,
    check_deployed,
    start_pool_server,
    stop_pool_server,
)

from netrun.core import Net, NetConfig

load_dotenv()

# --- Loaded from .env ---
HCLOUD_SSH_KEY_NAME = os.environ["HCLOUD_SSH_KEY_NAME"]
SSH_PRIVATE_KEY_PATH = os.environ["SSH_PRIVATE_KEY_PATH"]
HCLOUD_API_TOKEN = os.environ["HCLOUD_API_TOKEN"]

# --- Server settings ---
SERVER_NAME = "netrun-demo"
SERVER_TYPE = "cpx22"          # 2 vCPU, 4 GB RAM
SERVER_IMAGE = "ubuntu-24.04"
SERVER_LOCATION = "fsn1"       # Falkenstein, DE

# --- Deployment settings ---
REMOTE_DIR = "/opt/netrun-app"
POOL_SERVER_PORT = 8765
APP_DIR = str(Path(__file__).parent / "app")


async def main():
    # 1. Create server (idempotent)
    print("=" * 60)
    print("1. CREATE SERVER")
    print("=" * 60)
    ip = create_hetzner_server(
        server_name=SERVER_NAME,
        ssh_key_name=HCLOUD_SSH_KEY_NAME,
        server_type=SERVER_TYPE,
        server_image=SERVER_IMAGE,
        server_location=SERVER_LOCATION,
    )
    print(f"Server IP: {ip}")

    # 2. Deploy (skip if already deployed)
    print()
    print("=" * 60)
    print("2. DEPLOY")
    print("=" * 60)
    if not check_deployed(host=ip, ssh_private_key_path=SSH_PRIVATE_KEY_PATH, remote_dir=REMOTE_DIR):
        deploy_to_server(
            host=ip,
            ssh_private_key_path=SSH_PRIVATE_KEY_PATH,
            local_folder=APP_DIR,
            remote_dir=REMOTE_DIR,
            net_source="netrun.toml",
            pool_server_port=POOL_SERVER_PORT,
            python_version="3.11",
            pre_commands=["apt-get update -qq && apt-get install -y -qq build-essential > /dev/null 2>&1"],
            exclude=["*.pyc"],
            exclude_dir=["__pycache__", "*/__pycache__", ".venv"],
            enable_watchdog=True,
            hcloud_api_token=HCLOUD_API_TOKEN,
            auto_delete_idle_minutes=10,
            auto_delete_start_delay_minutes=15,
        )
    else:
        print("Already deployed — skipping.")

    # 3. Start pool server + SSH tunnel
    print()
    print("=" * 60)
    print("3. START POOL SERVER")
    print("=" * 60)
    handle = start_pool_server(
        host=ip,
        ssh_private_key_path=SSH_PRIVATE_KEY_PATH,
        remote_dir=REMOTE_DIR,
        pool_server_port=POOL_SERVER_PORT,
    )
    print(f"Pool: {handle.pool_server_url}")

    # 4. Run the network
    print()
    print("=" * 60)
    print("4. RUN NETWORK")
    print("=" * 60)

    # Add app/ to path so the client can resolve the function factory
    sys.path.insert(0, APP_DIR)

    config = NetConfig.from_file(f"{APP_DIR}/netrun.toml")
    config.pools["remote"].spec.url = handle.pool_server_url
    config.pools["remote"].spec.worker_name = "execution_manager"

    async with Net(config) as net:
        net.inject_data("double", "x", [5])
        net.inject_data("add", "b", [10])

        while True:
            await net.run_until_blocked()
            startable = net.get_startable_epochs()
            if not startable:
                break
            for epoch_id in startable:
                await net.execute_epoch(epoch_id)

        results = net.flush_output_queue("results")
        print("Result:")
        for value in results:
            print(f"  {value}")

        print()
        print("Node Logs:")
        net.print_all_logs()

        await net.request_pool_shutdown("remote")

    # 5. Stop
    print()
    print("=" * 60)
    print("5. STOP")
    print("=" * 60)
    handle.close_tunnel()
    stop_pool_server(host=ip, ssh_private_key_path=SSH_PRIVATE_KEY_PATH, remote_dir=REMOTE_DIR)


if __name__ == "__main__":
    asyncio.run(main())
