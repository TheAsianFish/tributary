# recentchange sample

`recentchange.jsonl.gz`: 50,000 consecutive events from Wikimedia's public
EventStreams feed (`https://stream.wikimedia.org/v2/stream/recentchange`),
captured 2026-10-16 23:45 to 2026-10-17 00:21 UTC with

```bash
make demo-data        # uv run tributary sample capture --count 50000
```

One event per line, in the order the stream delivered them (that order is what
the lateness figures in RESULTS.md are measured on). Each event is trimmed to
an allowlist of factual fields (`tributary/events.py`: ids, timestamps, wiki,
type, namespace, title, user name, bot/minor/patrolled flags, page lengths,
revision ids, log type/action). Edit summaries and other free text
(`comment`, `parsedcomment`, `log_action_comment`, `log_params`) are never
written. Two canary events were dropped during the capture; the stream
connection was closed by the server three times and resumed with
`Last-Event-ID`.

sha256 `f62faabc8a4c1662baf7b5882323e45e19aa5a18a323fb5dcb19ff9f93a1f1e5`, 3.4 MB.
