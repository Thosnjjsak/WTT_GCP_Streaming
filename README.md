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
<img width="1600" height="1000" alt="WTT_streaming_architecture" src="https://github.com/user-attachments/assets/e87b5baf-1fad-4138-8f55-60a8a2c74d59" />


## Quick map
1. `match_snapshots` (raw) → `live_2_transform` → feature dict
2. Publish to Pub/Sub topic `wtt-live`
3. Cloud Run (`entry_point`) reads Pub/Sub, calls Vertex AI endpoint, inserts into `wtt_ml.live_preds`

## Run Book
- Publish a manual test: GCP Console → Pub/Sub → Topic `wtt-live` → **Publish message** → (JSON body from `live_2_transform`)
- Watch predictions: BigQuery → `wtt_ml.live_preds` (check `model_score`, `model_prediction`, `error`)

