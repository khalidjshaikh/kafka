#!/usr/bin/env bash
# Build (if needed) and run the kafka-random-numbers app, starting a local
# Kafka broker first if it is not already reachable.
#
# Usage: run.sh [options] [bootstrap-servers] [topic]
#
# Options:
#   --no-build        skip the Maven build even if sources changed
#   --no-start-kafka  fail if the broker is unreachable instead of auto-starting a local one
#   -h, --help        show this help
#
# Environment overrides: KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

log()  { printf '%s\n' "$*"; }
fail() { printf 'Error: %s\n' "$*" >&2; exit 1; }

usage() {
  sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'
}

BUILD=1
START_KAFKA=1
POSITIONAL=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-build)       BUILD=0; shift ;;
    --no-start-kafka) START_KAFKA=0; shift ;;
    -h|--help)        usage; exit 0 ;;
    *)                POSITIONAL+=("$1"); shift ;;
  esac
done

BOOTSTRAP="${POSITIONAL[0]:-${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}}"
TOPIC="${POSITIONAL[1]:-${KAFKA_TOPIC:-random-numbers}}"

# --- Java ---------------------------------------------------------------------

command -v java >/dev/null 2>&1 || fail "java not found on PATH."

JAVA_INFO="$(java -version 2>&1 | head -n 1)"
if [[ "$JAVA_INFO" =~ version\ \"1\.([0-9]+) ]]; then
  JAVA_MAJOR="${BASH_REMATCH[1]}"
elif [[ "$JAVA_INFO" =~ version\ \"([0-9]+) ]]; then
  JAVA_MAJOR="${BASH_REMATCH[1]}"
else
  JAVA_MAJOR=""
fi
if [[ -n "$JAVA_MAJOR" && "$JAVA_MAJOR" -lt 17 ]]; then
  fail "Java 17 or newer is required (found: $JAVA_INFO)."
fi

# --- Build --------------------------------------------------------------------

VERSION="$(sed -n 's/.*<version>\([^<]*\)<\/version>.*/\1/p' pom.xml | head -n 1)"
[[ -n "$VERSION" ]] || fail "Could not determine the project version from pom.xml."
JAR="target/kafka-random-numbers-$VERSION.jar"

needs_build() {
  [[ -f "$JAR" ]] || return 0
  [[ -n "$(find pom.xml src -newer "$JAR" -print -quit 2>/dev/null)" ]]
}

if [[ "$BUILD" -eq 1 ]] && needs_build; then
  command -v mvn >/dev/null 2>&1 || fail "Maven (mvn) is required to build the jar."
  log "Building $JAR ..."
  mvn -q -DskipTests package
fi

[[ -f "$JAR" ]] || fail "Jar not found: $JAR (do not use --no-build before the first build)."

# --- Broker --------------------------------------------------------------------

if [[ "$START_KAFKA" -eq 1 ]]; then
  KAFKA_BOOTSTRAP_SERVERS="$BOOTSTRAP" "$SCRIPT_DIR/kafka.sh" start
else
  HOST="${BOOTSTRAP%%:*}"
  PORT="${BOOTSTRAP##*:}"
  if ! nc -z -w 2 "$HOST" "$PORT" >/dev/null 2>&1; then
    fail "Kafka is not reachable at $BOOTSTRAP (--no-start-kafka was given)."
  fi
fi

# --- Run ----------------------------------------------------------------------

log "Kafka: $BOOTSTRAP | topic: $TOPIC"
log "Press Ctrl+C to stop."
exec java -jar "$JAR" ${POSITIONAL[@]+"${POSITIONAL[@]}"}
