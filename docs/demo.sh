#!/usr/bin/env bash
# The README's demo, recorded against a running stack after the quickstart:
#   asciinema rec --cols 104 --rows 32 -c "bash docs/demo.sh" /tmp/demo.cast
#   agg --theme monokai --font-size 15 --speed 1.3 /tmp/demo.cast docs/demo.gif
# Run from the repository root with the package installed (pip install '.[warehouse]').
set -uo pipefail
export TRIBUTARY_LOG_LEVEL=WARNING

run() {
  printf '\n\033[1;32m$\033[0m '
  local text="$*"
  for ((i = 0; i < ${#text}; i++)); do printf '%s' "${text:i:1}"; sleep 0.02; done
  printf '\n'
  eval "$@"
  sleep 2.5
}

note() { printf '\n\033[2m# %s\033[0m' "$*"; sleep 1.2; }

note "Kafka, Spark, PostgreSQL and MinIO, after docker compose up -d --build"
run "docker compose ps --format '{{.Service}}\t{{.Status}}' | grep -v Exited | column -t -s $'\\t'"
note "the 50,000 sample events, landed once each"
run "docker compose exec -T postgres psql -U tributary -tAc 'SELECT count(*), count(DISTINCT event_id) FROM raw.recentchange'"
note "SQL through the guard and the agent's read-only role"
run "tributary sql 'SELECT wiki, edits FROM marts.dim_wiki ORDER BY edits DESC LIMIT 3'"
run "tributary sql 'DELETE FROM marts.dim_wiki'"
note "a function call disguised as a column: PostgreSQL would run pg_terminate_backend(p)"
run "tributary sql 'SELECT p.pg_terminate_backend FROM unnest(ARRAY[42]) AS p'"
note "a question in English (replaying the recorded qwen2.5-coder:7b answer; --provider ollama runs it live)"
run "tributary ask --provider fake 'Which 5 wikis had the most edits (including page creations)? Return the wiki and its number of edits.'"
sleep 2
