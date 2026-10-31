#!/usr/bin/env bash
# Manage a local Kafka broker for the kafka-random-numbers demo.
#
# Usage: kafka.sh <start|stop|status|wait>
#
# Environment overrides:
#   KAFKA_BOOTSTRAP_SERVERS  broker address      (default localhost:9092)
#   KAFKA_SERVER_CONFIG      broker properties   (default /opt/homebrew/etc/kafka/server.properties)
#   KAFKA_READY_TIMEOUT      seconds to wait for readiness (default 90)

set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
SERVER_CONFIG="${KAFKA_SERVER_CONFIG:-/opt/homebrew/etc/kafka/server.properties}"
READY_TIMEOUT="${KAFKA_READY_TIMEOUT:-90}"

HOST="${BOOTSTRAP%%:*}"
PORT="${BOOTSTRAP##*:}"

log()  { printf '%s\n' "$*"; }
fail() { printf 'Error: %s\n' "$*" >&2; exit 1; }

usage() {
  sed -n '2,10p' "$0" | sed 's/^# \{0,1\}//'
}

# Run a command with a hard timeout (macOS has no coreutils `timeout` by default).
with_timeout() {
  local secs="$1" rc=0
  shift
  "$@" &
  local cmd_pid=$!
  ( sleep "$secs"; kill -KILL "$cmd_pid" 2>/dev/null ) &
  local watchdog_pid=$!
  wait "$cmd_pid" 2>/dev/null || rc=$?
  kill "$watchdog_pid" 2>/dev/null || true
  wait "$watchdog_pid" 2>/dev/null || true
  return "$rc"
}

port_open() {
  nc -z -w 2 "$HOST" "$PORT" >/dev/null 2>&1
}

# Exercises the broker's metadata service. This goes beyond the TCP listener,
# which opens before the broker is actually ready to serve clients.
probe() {
  command -v kafka-topics >/dev/null 2>&1 || return 1
  with_timeout 10 kafka-topics --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1
}

is_local() {
  [[ "$HOST" == "localhost" || "$HOST" == "127.0.0.1" || "$HOST" == "::1" ]]
}

wait_for_ready() {
  printf 'Waiting for Kafka at %s (up to %ss) ' "$BOOTSTRAP" "$READY_TIMEOUT"
  local deadline=$((SECONDS + READY_TIMEOUT))
  while (( SECONDS < deadline )); do
    if probe; then
      printf ' ready\n'
      return 0
    fi
    printf '.'
    sleep 2
  done
  printf '\n'
  return 1
}

broker_log_hint() {
  local prefix log_file
  prefix="$(brew --prefix kafka 2>/dev/null || true)"
  log_file="$prefix/libexec/logs/server.log"
  if [[ -n "$prefix" && -f "$log_file" ]]; then
    printf '\nLast lines of %s:\n' "$log_file" >&2
    tail -n 20 "$log_file" >&2
  fi
}

do_start() {
  if probe; then
    log "Kafka is already running and ready at $BOOTSTRAP."
    return 0
  fi

  is_local || fail "Cannot auto-start a non-local broker ($HOST). Start it yourself, then re-run."

  command -v kafka-server-start >/dev/null 2>&1 \
    || fail "kafka-server-start not found. Install Kafka (e.g. 'brew install kafka') or start a broker manually."

  [[ -f "$SERVER_CONFIG" ]] || fail "Kafka config not found: $SERVER_CONFIG (set KAFKA_SERVER_CONFIG)."

  if port_open; then
    log "Kafka port $PORT is already open; waiting for the broker to become ready..."
  else
    log "Starting Kafka ($SERVER_CONFIG) ..."
    kafka-server-start -daemon "$SERVER_CONFIG"
  fi

  if ! wait_for_ready; then
    broker_log_hint
    fail "Kafka did not become ready within ${READY_TIMEOUT}s."
  fi
}

do_stop() {
  if ! port_open; then
    log "Kafka is not running at $BOOTSTRAP."
    return 0
  fi

  command -v kafka-server-stop >/dev/null 2>&1 || fail "kafka-server-stop not found."

  log "Stopping Kafka ..."
  kafka-server-stop >/dev/null 2>&1 || true

  local deadline=$((SECONDS + 90))
  while (( SECONDS < deadline )); do
    if ! port_open; then
      log "Kafka stopped."
      return 0
    fi
    sleep 1
  done
  fail "Kafka did not stop within 90s."
}

do_status() {
  if ! port_open; then
    log "Kafka is not reachable at $BOOTSTRAP."
    return 1
  fi
  if probe; then
    log "Kafka is ready at $BOOTSTRAP."
  else
    log "Kafka port $PORT is open, but the broker is still starting up or not responding."
    return 1
  fi
}

case "${1:-status}" in
  start)          do_start ;;
  stop)           do_stop ;;
  status)         do_status ;;
  wait)           wait_for_ready || fail "Kafka did not become ready within ${READY_TIMEOUT}s." ;;
  -h|--help|help) usage ;;
  *)              usage; exit 2 ;;
esac
