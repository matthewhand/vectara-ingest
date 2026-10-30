# vectara-ingest — dormant deployment note

## Status and history

This fork was previously described as dormant in July 2026, after the legacy
Vectara v1 integration and associated account/scheduled job reportedly stopped
working. The historical account and cron-job status have **not** been rechecked.
This note does not establish whether a deployment is currently running.

**The repository code has since changed.** The upstream changes incorporated
into this fork in October 2026 include Vectara **v2 API** endpoints. The
checked-in implementation is therefore not v1-only; remaining compatibility
work, if any, still requires integration testing.

## Current code (source-inspected, not live-tested)

- `core/indexer.py` uses v2 document upload, indexing and deletion endpoints,
  including `/v2/corpora/{corpus_key}/documents` and
  `/v2/corpora/{corpus_key}/documents/{encoded_doc_id}` (the document ID is
  URL-encoded before use).
- `ingest.py` implements v2 corpus creation at `/v2/corpora` and resetting at
  `/v2/corpora/{corpus_key}/reset`.
- Vectara API-key authentication is used by the indexer; corpus creation/reset
  also have OAuth client-credential paths.
- The repository includes RSS/feed crawling, document parsing and `run.sh`
  orchestration. `run.sh` is already executable in the fork's `main` branch.

These are source-code observations, **not** evidence of a successful v2 API
request, working credentials, a running deployment or compatible live data.

## Before reviving or scheduling

1. Confirm whether any installation, container, job or cron schedule is active;
   do not re-enable an old task without checking its current owner and target.
2. Obtain a valid Vectara account, permitted credentials and a **test** corpus.
   Reconcile local configuration and secret names with the current source; never
   commit credentials or assume historical customer IDs remain usable.
3. Resolve outstanding security advisories separately. At the upstream sync,
   `authlib==1.6.11` was below the 1.9.0 fix for a critical advisory.
4. Run dependency-resolution and bounded local tests, then separately authorised
   end-to-end checks against the test corpus. Check any remaining API and auth
   incompatibilities before claiming a working migration.
5. Only after explicit deployment approval, review the current `run.sh`,
   container settings and scheduling configuration before enabling operation.

No deployment, live endpoint check or scheduler change was performed as part of
this documentation update.
