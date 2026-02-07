#!/bin/bash
# netrun auto-delete watchdog
PID_FILE="__REMOTE_DIR__/.netrun_serve_pool.pid"
IDLE_FILE="__REMOTE_DIR__/.netrun_idle_since"
CREATED_FILE="__REMOTE_DIR__/.netrun_watchdog_created"
IDLE_TIMEOUT=__IDLE_TIMEOUT__
START_DELAY=__START_DELAY__

# Record creation time on first run
if [ ! -f "$CREATED_FILE" ]; then
    date +%s > "$CREATED_FILE"
fi

# Skip checks until start delay has elapsed
CREATED_AT=$(cat "$CREATED_FILE")
NOW=$(date +%s)
if [ $((NOW - CREATED_AT)) -lt "$START_DELAY" ]; then
    exit 0
fi

# Check if pool server is running
if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    # Server is running — reset idle timer
    rm -f "$IDLE_FILE"
    exit 0
fi

# Server is NOT running — track idle time
if [ ! -f "$IDLE_FILE" ]; then
    date +%s > "$IDLE_FILE"
    exit 0
fi

IDLE_SINCE=$(cat "$IDLE_FILE")
NOW=$(date +%s)
ELAPSED=$((NOW - IDLE_SINCE))

if [ "$ELAPSED" -ge "$IDLE_TIMEOUT" ]; then
    logger -t netrun-watchdog "Idle for ${ELAPSED}s (limit: ${IDLE_TIMEOUT}s). Deleting server."
    TOKEN=$(cat __REMOTE_DIR__/.hcloud_token)
    SERVER_ID=$(curl -sf http://169.254.169.254/hetzner/v1/metadata/instance-id)
    curl -sf -X DELETE \
        -H "Authorization: Bearer $TOKEN" \
        "https://api.hetzner.cloud/v1/servers/$SERVER_ID"
fi
