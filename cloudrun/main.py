# -*- coding: utf-8 -*-
"""
Cloud Run (Functions Framework) service:
- Triggered by Pub/Sub via Eventarc (CloudEvent)
- Validates/normalizes message -> Vertex AI Online Prediction
- Inserts result rows into BigQuery table: {PROJECT}.wtt_ml.live_preds

Entry point: entry_point
"""

import base64
import datetime as dt
import json
import re
from typing import Any, Dict, List, Optional, Tuple

from google.cloud import aiplatform, bigquery

# ──────────────────────────────────────────────────────────────────────────────
# CONFIG — put your real IDs here (or read from env if you prefer)
# ──────────────────────────────────────────────────────────────────────────────
PROJECT = "project-36a10255-b110-4164-8f8"
REGION = "us-central1"
ENDPOINT_ID = (
    "projects/project-36a10255-b110-4164-8f8/locations/us-central1/endpoints/5185920259684564992"
)

BQ_DATASET = "wtt_ml"
BQ_TABLE = "live_preds"
TABLE_ID = f"{PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

# Columns your online model expects (names must match training schema)
REQUIRED = [
    "avg_point_diff", "avg_points_scored_a", "avg_points_scored_b", "total_points",
    "tight_games", "clutch_games",
    "country_a", "country_b", "player_a", "player_b",
    "tournament_country", "yr", "round",
    "game_1_a", "game_1_b", "game_2_a", "game_2_b", "game_3_a", "game_3_b",
    "game_4_a", "game_4_b", "game_5_a", "game_5_b", "game_6_a", "game_6_b",
    "game_7_a", "game_7_b",
    # model requires the column to exist in the instance; value can be null/empty
    "games",
    "games_tuples",
]

# Don’t send the target/label to the model (leakage)
BLOCKLIST = {"winner"}

# per your Vertex “Test your model” view
NUMERIC_ONLY = {"avg_point_diff", "avg_points_scored_a", "avg_points_scored_b"}
STRING_ONLY = set(REQUIRED) - NUMERIC_ONLY

# clients
bq = bigquery.Client(project=PROJECT)

_num_re = re.compile(r"^-?\d+(\.\d+)?$")


# ──────────────────────────────────────────────────────────────────────────────
# Utilities
# ──────────────────────────────────────────────────────────────────────────────
def _coerce(v: Any) -> Any:
    """'11'->11, '11.0'->11.0, 'null'/''->None, leave strings otherwise."""
    if v is None:
        return None
    if isinstance(v, (int, float, bool)):
        return v
    if isinstance(v, str):
        s = v.strip()
        if s == "" or s.lower() == "null":
            return None
        if _num_re.match(s):
            return float(s) if "." in s else int(s)
        return s
    return v


def _as_int_or_none(v) -> Optional[int]:
    try:
        if v is None:
            return None
        if isinstance(v, str):
            s = v.strip().lower()
            if s in ("", "null"):
                return None
            v = float(s)
        f = float(v)
        i = int(f)
        return i if f.is_integer() else int(round(f))
    except Exception:
        return None


def _sanitize_instance(d: Dict[str, Any]) -> Dict[str, Any]:
    """Drop blocked keys, light coercion, force 'yr' to string for model."""
    out: Dict[str, Any] = {}
    for k, v in d.items():
        if k in BLOCKLIST:
            continue
        if k == "yr":
            out[k] = None if v is None else str(v).strip()
            continue
        out[k] = _coerce(v)
    return out


def _backfill_if_possible(inst: Dict[str, Any]) -> None:
    """
    If per-game scores exist but engineered features are missing, derive them.
    Does NOT overwrite values already provided by upstream transform.
    """
    pairs: List[Tuple[Optional[int], Optional[int]]] = []
    for i in range(1, 8):
        a = _as_int_or_none(inst.get(f"game_{i}_a"))
        b = _as_int_or_none(inst.get(f"game_{i}_b"))
        if a is not None and b is not None:
            pairs.append((a, b))
    if not pairs:
        return

    pts_a = sum(a for a, _ in pairs)
    pts_b = sum(b for _, b in pairs)
    n = len(pairs)

    inst.setdefault("total_points", pts_a + pts_b)
    inst.setdefault("avg_points_scored_a", pts_a / n)
    inst.setdefault("avg_points_scored_b", pts_b / n)
    inst.setdefault("avg_point_diff", (pts_a / n) - (pts_b / n))
    inst.setdefault("tight_games", sum(1 for a, b in pairs if abs(a - b) <= 2))
    inst.setdefault("clutch_games", 1 if n in (5, 7) else 0)


def _enforce_types(inst: Dict[str, Any]) -> None:
    """Match the model’s feature dtypes exactly."""
    for k in NUMERIC_ONLY:
        if k in inst and inst[k] is not None:
            v = inst[k]
            if isinstance(v, str):
                v = v.strip()
                if v == "" or v.lower() == "null":
                    inst[k] = None
                else:
                    inst[k] = float(v) if "." in v else float(v)
            elif isinstance(v, (int, float)):
                inst[k] = float(v)
            else:
                inst[k] = None

    for k in STRING_ONLY:
        if k in inst and inst[k] is not None:
            inst[k] = str(inst[k])


def _insert(row: Dict[str, Any]) -> None:
    bq.insert_rows_json(TABLE_ID, [row])


def _predict(instance: Dict[str, Any]) -> Optional[float]:
    aiplatform.init(project=PROJECT, location=REGION)
    ep = aiplatform.Endpoint(endpoint_name=ENDPOINT_ID)
    pred = ep.predict(instances=[instance]).predictions
    if not pred:
        return None
    p0 = pred[0]

    # Common shapes
    if isinstance(p0, dict):
        if isinstance(p0.get("scores"), list) and p0["scores"]:
            return float(p0["scores"][0])
        if "score" in p0:
            return float(p0["score"])
    if isinstance(p0, list) and p0:
        try:
            return float(p0[0])
        except Exception:
            return None
    try:
        return float(p0)
    except Exception:
        return None


def _validate_or_record(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    inst = _sanitize_instance(_pick_instance(payload))
    _backfill_if_possible(inst)
    _enforce_types(inst)

    missing = [c for c in REQUIRED if c not in inst]
    if missing:
        _insert({
            "ingest_ts": dt.datetime.utcnow().isoformat() + "Z",
            "match_id": payload.get("match_id"),
            "yr": str(payload.get("yr")) if payload.get("yr") is not None else None,
            "round": str(payload.get("round")) if payload.get("round") is not None else None,
            "country_a": payload.get("country_a"),
            "country_b": payload.get("country_b"),
            "player_a": payload.get("player_a"),
            "player_b": payload.get("player_b"),
            "tournament_country": payload.get("tournament_country"),
            "endpoint": ENDPOINT_ID,
            "model_score": None,
            "model_prediction": None,
            "raw_request": json.dumps(payload),
            "error": f"Missing features: {', '.join(missing)}",
        })
        return None
    return inst


def _pick_instance(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Accept either flat dict or {'instance': {...}}."""
    if isinstance(payload.get("instance"), dict):
        return payload["instance"]
    return payload


def _extract_payload_from_cloudevent(cloud_event) -> dict:
    """
    Return the FINAL dict the model needs, regardless of Eventarc shape:
    - raw bytes of a JSON object
    - dict envelope: {"message":{"data": "<base64>" , "attributes":{...}}}
    - nested shapes (decode until we get the feature dict)
    """
    ce = getattr(cloud_event, "data", None)

    def _to_obj(b: bytes) -> dict:
        return json.loads(b.decode("utf-8"))

    # Start with bytes or dict
    if isinstance(ce, (bytes, bytearray)):
        obj = _to_obj(ce)
    elif isinstance(ce, dict):
        obj = ce
    else:
        obj = _to_obj(json.dumps(ce).encode("utf-8"))

    # Peel Pub/Sub envelope (once or twice if needed)
    for _ in range(2):
        if isinstance(obj, dict) and "message" in obj and isinstance(obj["message"], dict):
            raw = obj["message"].get("data")
            if isinstance(raw, str):
                obj = _to_obj(base64.b64decode(raw))
                continue
            if isinstance(raw, (bytes, bytearray)):
                obj = _to_obj(raw)
                continue
        if isinstance(obj, dict) and "data" in obj and isinstance(obj["data"], str):
            try:
                obj = _to_obj(base64.b64decode(obj["data"]))
                continue
            except Exception:
                pass
        break

    if not isinstance(obj, dict):
        raise ValueError("Decoded payload is not a JSON object")
    return obj


# ──────────────────────────────────────────────────────────────────────────────
# Cloud Run / Functions Framework entry
# ──────────────────────────────────────────────────────────────────────────────
def entry_point(cloud_event):
    try:
        payload = _extract_payload_from_cloudevent(cloud_event)

        inst = _validate_or_record(payload)
        if inst is None:  # already logged the error row
            return

        score: Optional[float] = None
        err: Optional[str] = None
        try:
            score = _predict(inst)
        except Exception as e:
            err = f"predict error: {str(e)}"

        row = {
            "ingest_ts": dt.datetime.utcnow().isoformat() + "Z",
            "match_id": payload.get("match_id"),
            "yr": str(payload.get("yr")) if payload.get("yr") is not None else None,
            "round": str(payload.get("round")) if payload.get("round") is not None else None,
            "country_a": payload.get("country_a"),
            "country_b": payload.get("country_b"),
            "player_a": payload.get("player_a"),
            "player_b": payload.get("player_b"),
            "tournament_country": payload.get("tournament_country"),
            "endpoint": ENDPOINT_ID,
            "model_score": score,
            "model_prediction": (score is not None and score >= 0.5),
            "raw_request": json.dumps(_pick_instance(payload)),
            "error": err,
        }
        _insert(row)

    except Exception as e:
        _insert({
            "ingest_ts": dt.datetime.utcnow().isoformat() + "Z",
            "match_id": None,
            "endpoint": ENDPOINT_ID,
            "model_score": None,
            "model_prediction": None,
            "raw_request": None,
            "error": str(e),
        })
