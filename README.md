# WTT – GCP Streaming (live scoring → Vertex AI → BigQuery)

**What:** End-to-end streaming pipeline that ingests live table-tennis scores, enriches them into features, gets real-time win predictions from Vertex AI, and lands scored events into BigQuery for Looker Studio dashboards.

**Why:** Demonstrates a minimal, production-minded pattern for Pub/Sub + Cloud Run + Vertex AI + BigQuery.

**How (quickstart):**
1. Deploy Cloud Run scorer (`cloudrun/`) – receives Pub/Sub via Eventarc and writes to `wtt_ml.live_preds`.
2. Publish a test message (`scripts/publish_test.sh`) using `cloudrun/Publish on Pub&Sub.json`.
3. Confirm rows in BigQuery with `sql/queries/failed_rows.sql` and `SELECT * FROM wtt_ml.live_preds`.
