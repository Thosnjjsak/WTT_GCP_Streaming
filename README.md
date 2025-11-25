 # WTT – GCP Streaming (live scoring → Vertex AI → BigQuery)

## What this repo contains
- **notebooks/**
  - `new_live_pull_getmatchcard.ipynb` – pulls live match snapshots into BigQuery (`wtt_ingest.match_snapshots`)
  - `live_2_transform.ipynb` – transforms snapshots to model features (and can publish to Pub/Sub)
- **cloud-run/**
  - `main.py` – Cloud Run handler that receives Pub/Sub → calls Vertex AI → writes to `wtt_ml.live_preds`
  - `requirements.txt`
- **sql/** – optional schema helpers
- **infra/** – notes for wiring Pub/Sub, Eventarc, Cloud Run

## High-level pipeline

<img width="1536" height="1024" alt="ChatGPT Image Nov 25, 2025, 12_53_47 PM" src="https://github.com/user-attachments/assets/0c6c6413-7d9c-4ce4-9e12-af3318c0be80" />


## Quick map
1. `match_snapshots` (raw) → `live_2_transform` → feature dict
2. Publish to Pub/Sub topic `wtt-live`
3. Cloud Run (`entry_point`) reads Pub/Sub, calls Vertex AI endpoint, inserts into `wtt_ml.live_preds`

## Run Book
- Publish a manual test: GCP Console → Pub/Sub → Topic `wtt-live` → **Publish message** → (JSON body from `live_2_transform`)
- Watch predictions: BigQuery → `wtt_ml.live_preds` (check `model_score`, `model_prediction`, `error`)

