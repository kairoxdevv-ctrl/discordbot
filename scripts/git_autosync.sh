#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="${1:-/root/discordbot}"
BRANCH="${GIT_SYNC_BRANCH:-main}"

log() {
  printf '[%s] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*"
}

read_env() {
  local key="$1"
  local file="$2"
  awk -F= -v k="$key" '$1==k {sub(/^[^=]*=/,""); print; exit}' "$file"
}

cd "$REPO_DIR"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  log "not a git repo: $REPO_DIR"
  exit 1
fi

REMOTE_URL="$(git remote get-url origin)"
ENV_FILE="$REPO_DIR/.env"

if [[ ! -f "$ENV_FILE" ]]; then
  log "missing .env"
  exit 1
fi

TOKEN="$(read_env GITHUB_TOKEN "$ENV_FILE")"
INTERVAL_RAW="$(read_env GITHUB_SYNC_INTERVAL "$ENV_FILE")"
INTERVAL="${INTERVAL_RAW:-20}"
QUIET_RAW="$(read_env GITHUB_SYNC_QUIET_SEC "$ENV_FILE")"
QUIET_SEC="${QUIET_RAW:-120}"
MIN_GAP_RAW="$(read_env GITHUB_SYNC_MIN_COMMIT_GAP_SEC "$ENV_FILE")"
MIN_COMMIT_GAP_SEC="${MIN_GAP_RAW:-600}"
MAX_WAIT_RAW="$(read_env GITHUB_SYNC_MAX_WAIT_SEC "$ENV_FILE")"
MAX_WAIT_SEC="${MAX_WAIT_RAW:-1800}"

if [[ -z "${TOKEN}" ]]; then
  log "GITHUB_TOKEN is empty in .env"
  exit 1
fi

if ! [[ "$INTERVAL" =~ ^[0-9]+$ ]]; then INTERVAL=20; fi
if ! [[ "$QUIET_SEC" =~ ^[0-9]+$ ]]; then QUIET_SEC=120; fi
if ! [[ "$MIN_COMMIT_GAP_SEC" =~ ^[0-9]+$ ]]; then MIN_COMMIT_GAP_SEC=600; fi
if ! [[ "$MAX_WAIT_SEC" =~ ^[0-9]+$ ]]; then MAX_WAIT_SEC=1800; fi

AUTH_B64="$(printf 'x-access-token:%s' "$TOKEN" | base64 -w0)"
HEADER_KEY="http.${REMOTE_URL}.extraheader"

PENDING_SINCE=0
LAST_CHANGE_TS=0
LAST_COMMIT_TS=0
LAST_TREE_SIG=""

reload_env() {
  local now_token now_interval now_quiet now_min_gap now_max_wait
  now_token="$(read_env GITHUB_TOKEN "$ENV_FILE")"
  now_interval="$(read_env GITHUB_SYNC_INTERVAL "$ENV_FILE")"
  now_quiet="$(read_env GITHUB_SYNC_QUIET_SEC "$ENV_FILE")"
  now_min_gap="$(read_env GITHUB_SYNC_MIN_COMMIT_GAP_SEC "$ENV_FILE")"
  now_max_wait="$(read_env GITHUB_SYNC_MAX_WAIT_SEC "$ENV_FILE")"

  if [[ "$now_token" != "$TOKEN" && -n "$now_token" ]]; then
    TOKEN="$now_token"
    AUTH_B64="$(printf 'x-access-token:%s' "$TOKEN" | base64 -w0)"
    log "reloaded token from .env"
  fi
  if [[ "$now_interval" =~ ^[0-9]+$ ]]; then INTERVAL="$now_interval"; fi
  if [[ "$now_quiet" =~ ^[0-9]+$ ]]; then QUIET_SEC="$now_quiet"; fi
  if [[ "$now_min_gap" =~ ^[0-9]+$ ]]; then MIN_COMMIT_GAP_SEC="$now_min_gap"; fi
  if [[ "$now_max_wait" =~ ^[0-9]+$ ]]; then MAX_WAIT_SEC="$now_max_wait"; fi
}

fetch_remote() {
  git -c "$HEADER_KEY=AUTHORIZATION: basic $AUTH_B64" fetch origin "$BRANCH" --quiet
}

rebase_if_needed() {
  if [[ -n "$(git status --porcelain)" ]]; then
    return 0
  fi
  if ! git merge-base --is-ancestor "origin/$BRANCH" HEAD; then
    if ! git rebase "origin/$BRANCH" >/dev/null 2>&1; then
      git rebase --abort >/dev/null 2>&1 || true
      log "rebase conflict, skipped"
      return 1
    fi
    log "rebased on origin/$BRANCH"
  fi
  return 0
}

push_if_ahead() {
  if ! git merge-base --is-ancestor HEAD "origin/$BRANCH"; then
    if git -c "$HEADER_KEY=AUTHORIZATION: basic $AUTH_B64" push origin "$BRANCH" --quiet; then
      log "pushed local commits"
    else
      log "push failed"
    fi
  fi
}

maybe_commit_local() {
  local now status_sig should_commit=0
  now="$(date +%s)"
  status_sig="$(git status --porcelain)"

  if [[ -z "$status_sig" ]]; then
    PENDING_SINCE=0
    LAST_CHANGE_TS=0
    LAST_TREE_SIG=""
    return 0
  fi

  if [[ "$status_sig" != "$LAST_TREE_SIG" ]]; then
    LAST_TREE_SIG="$status_sig"
    LAST_CHANGE_TS="$now"
    if [[ "$PENDING_SINCE" -eq 0 ]]; then
      PENDING_SINCE="$now"
    fi
  fi

  # Debounce: wait for quiet window after last local change.
  if (( now - LAST_CHANGE_TS >= QUIET_SEC )); then
    should_commit=1
  fi
  # Hard cap: if changes keep coming, still batch-commit eventually.
  if (( PENDING_SINCE > 0 && now - PENDING_SINCE >= MAX_WAIT_SEC )); then
    should_commit=1
  fi
  # Prevent commit storms.
  if (( LAST_COMMIT_TS > 0 && now - LAST_COMMIT_TS < MIN_COMMIT_GAP_SEC )); then
    should_commit=0
  fi

  if (( should_commit == 1 )); then
    git add -A
    if ! git diff --cached --quiet; then
      git commit -m "chore(sync): local batch update $(date -u +'%Y-%m-%dT%H:%M:%SZ')" >/dev/null || true
      LAST_COMMIT_TS="$now"
      log "local changes committed (batched)"
      PENDING_SINCE=0
      LAST_CHANGE_TS=0
      LAST_TREE_SIG=""
    fi
  fi
}

sync_once() {
  reload_env

  if ! fetch_remote; then
    log "fetch failed"
    return
  fi
  rebase_if_needed || true

  maybe_commit_local

  if ! fetch_remote; then
    log "fetch failed (post-commit)"
    return
  fi
  rebase_if_needed || true
  push_if_ahead
}

log "autosync started (repo=$REPO_DIR branch=$BRANCH interval=${INTERVAL}s quiet=${QUIET_SEC}s min_gap=${MIN_COMMIT_GAP_SEC}s max_wait=${MAX_WAIT_SEC}s)"
while true; do
  sync_once
  sleep "$INTERVAL"
done
