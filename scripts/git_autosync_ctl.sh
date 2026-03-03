#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="${2:-/root/discordbot}"
PID_FILE="$REPO_DIR/.git_autosync.pid"
LOG_FILE="$REPO_DIR/logs/git_autosync.log"
RUNNER="$REPO_DIR/scripts/git_autosync.sh"

cmd="${1:-status}"

start() {
  if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "autosync already running (pid $(cat "$PID_FILE"))"
    return 0
  fi
  mkdir -p "$REPO_DIR/logs"
  nohup "$RUNNER" "$REPO_DIR" >>"$LOG_FILE" 2>&1 &
  local pid="$!"
  echo "$pid" > "$PID_FILE"
  sleep 1
  if kill -0 "$pid" 2>/dev/null; then
    echo "autosync started (pid $pid)"
    return 0
  fi
  echo "autosync failed to start"
  tail -n 40 "$LOG_FILE" 2>/dev/null || true
  rm -f "$PID_FILE"
  return 1
}

stop() {
  if [[ ! -f "$PID_FILE" ]]; then
    echo "autosync not running"
    return 0
  fi
  pid="$(cat "$PID_FILE")"
  if kill -0 "$pid" 2>/dev/null; then
    kill "$pid" || true
    sleep 1
  fi
  rm -f "$PID_FILE"
  echo "autosync stopped"
}

status() {
  if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "running (pid $(cat "$PID_FILE"))"
  else
    echo "stopped"
    return 1
  fi
}

logs() {
  tail -n 80 "$LOG_FILE" 2>/dev/null || true
}

case "$cmd" in
  start) start ;;
  stop) stop ;;
  restart) stop; start ;;
  status) status ;;
  logs) logs ;;
  *)
    echo "usage: $0 {start|stop|restart|status|logs} [repo_dir]"
    exit 2
    ;;
esac
