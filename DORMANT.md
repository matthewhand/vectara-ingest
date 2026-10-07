# vectara-ingest — DORMANT (July 2026)

## Why dormant
Vectara retired its v1 REST API in August 2025. This project uses v1 endpoints exclusively, and the customer account (`customer_id: <customer_id>`) has been purged. The cron job has been failing silently since at least July 2026.

## What this project does
Crawls RSS feeds (configured in `config/pg-rss.yaml`), processes articles through an AI pipeline, and indexes them into a Vectara corpus for semantic search.

## What was working
- RSS feed crawling pipeline (postgres sources, discourse, etc.)
- Document parsing via `unstructured.io`
- JWT-based auth against Vectara v1 API
- Dockerized execution via `run.sh`

## What needs doing to revive

### 1. Vectara account
- Sign up at https://console.vectara.com/signup (30-day free trial)
- Create a new corpus, get: `api_key`, `customer_id`, `corpus_key`

### 2. v1 → v2 API migration (required)
The v1 API is retired. Code changes needed in `core/indexer.py` and `ingest.py`:

| v1 (current) | v2 (needed) |
|---|---|
| `/v1/index` | `/v2/corpora/{corpus_key}/documents` |
| `/v1/delete-doc` | `/v2/corpora/{corpus_key}/documents/{doc_id}` |
| `/v1/reset-corpus` | `/v2/corpora/{corpus_key}/documents?delete=true` |
| JWT OAuth2 auth | `x-api-key` header (simpler!) |
| `customer_id` header | Not needed for v2 (except password auth) |
| `corpus_id: 1` (int) | `corpus_key` (string) |
| `num_results`, `start` | `limit`, `offset` |

### 3. Update configs
- `secrets.toml`: new `api_key`
- `config/pg-rss.yaml`: new `customer_id` + `corpus_key`

### 4. Re-enable
```bash
chmod +x run.sh  # Already done
# Uncomment the cron entry in crontab
```

## Files
- Repo: https://github.com/matthewhand/vectara-ingest
- Cron was: `0 0,12 * * * cd ~/vectara-ingest && bash -l -c './run.sh'` (disabled July 2026)
- Config: `config/pg-rss.yaml`, `secrets.toml`
