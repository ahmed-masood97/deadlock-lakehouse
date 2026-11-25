# ADR 0004: Write-audit-publish on Iceberg branches for gold

**Status:** accepted

## Context
Consumers query gold directly. A bad run (an upstream bug, a broken join) must never become visible, and when it is stopped we want the bad data kept for debugging rather than lost.

## Decision
All gold tables set `write.wap.enabled=true`. A run creates branch `wap_<run_id>` on every gold table, sets `spark.wap.branch`, and writes and audits there. Only if no blocking check fails does it fast-forward `main` to the branch, in dependency order. Watermarks for gold jobs are committed only after a publish. Backfills use the same mechanism with branch `backfill_<run_id>`, plus a main-vs-branch diff, and can stop before cutover for human review.

## Consequences
- Readers only ever see audited data, and a failed audit is a non-event for them (stale, not wrong).
- Fast-forward refuses if main moved. That surfaces concurrent writers instead of losing a commit.
- Multi-commit operations (delete then insert) look atomic to readers, because both commits sit on the branch until publish.
- Publish is per table. Ordering avoids dangling keys, but cross-table snapshot consistency is not guaranteed.
