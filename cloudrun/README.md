# Cloud Run: live feature scorer

## Deploy (no-Docker, from source)
```bash
gcloud run deploy wtt-live-feature \
  --source . \
  --entry-point entry_point \
  --region us-central1 \
  --allow-unauthenticated
