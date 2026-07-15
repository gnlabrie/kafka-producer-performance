#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
CONFIG=${1:-}

if ! command -v java >/dev/null 2>&1; then
  echo "Java is required but was not found on PATH." >&2
  exit 1
fi
if [ -z "$CONFIG" ] || [ ! -r "$CONFIG" ]; then
  echo "Usage: $0 <readable-run-config.yaml>" >&2
  exit 2
fi

JAR="$PROJECT_DIR/target/kafka-producer-test-tool.jar"
if [ ! -f "$JAR" ]; then
  JAR=
  for candidate in "$PROJECT_DIR"/target/kafka-producer-test-tool-*.jar; do
    case "$candidate" in
      *.original) ;;
      *) [ -f "$candidate" ] && JAR=$candidate ;;
    esac
  done
fi
if [ -z "${JAR:-}" ] || [ ! -f "$JAR" ]; then
  echo "Application JAR not found. Build it with mvn package." >&2
  exit 1
fi

exec java -jar "$JAR" "--spring.config.additional-location=file:$CONFIG"
