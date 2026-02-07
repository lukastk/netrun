#!/bin/sh
export PATH="$HOME/.local/bin:$PATH"
cd __REMOTE_DIR__

# Skip if already running
PID_FILE=".netrun_serve_pool.pid"
if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "Pool server already running (pid $(cat "$PID_FILE"))"
    exit 0
fi

# Use forwarded SSH agent if available
AGENT_SOCK="__REMOTE_DIR__/.ssh_agent.sock"
if [ -S "$AGENT_SOCK" ]; then
    export SSH_AUTH_SOCK="$AGENT_SOCK"
fi

nohup __RUN_PREFIX__python .netrun_serve_pool.py > pool_server_stdout.log 2>&1 &
echo $! > "$PID_FILE"
