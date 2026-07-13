"""
generate_call_stream.py
=======================
Simulates a live JSON call-event feed for the Celebal Sentiment Pipeline.

What it produces
----------------
Each record is a nested JSON object matching the bronze_calls schema:
  {
    "call_id":            str (UUID v4),
    "customer_id":        int (drawn from the real customer_cdc_data_final.csv IDs),
    "agent_id":           str ("AGT-NNN"),
    "epoch_timestamp":    int (Unix seconds),
    "duration_sec":       int,
    "transcript_segments":[{"speaker": "agent|customer", "text": str, "start_sec": float}],
    "metadata": {
      "call_type":  "support|billing|technical",
      "channel":    "phone|chat|callback",
      "region":     str (city name)
    }
  }

Deliberate injections to prove pipeline features
-------------------------------------------------
  - ~10% late-arriving records  → epoch_timestamp 10-30 min in the past (proves watermarking)
  - ~5%  malformed records      → missing call_id or null customer_id (proves quarantine)
  - Sentiment keywords seeded   → so silver sentiment scoring produces non-trivial results

Usage
-----
  python generate_call_stream.py [--records N] [--out-dir ./output/calls] [--batch-size 20] [--delay 2]

  --records    Total records to generate          (default: 500)
  --out-dir    Directory to write JSON files to   (default: ./output/calls)
  --batch-size Records per JSON file              (default: 20)
  --delay      Seconds between batches (0=no delay)(default: 0)
"""

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Customer IDs present in customer_cdc_data_final.csv
CUSTOMER_IDS = [4, 5, 12, 14, 15, 18, 26, 28, 29, 30,
                32, 36, 55, 65, 70, 72, 76, 78, 82, 87, 95]

CITIES = ["Delhi", "Mumbai", "Bangalore", "Chennai", "Hyderabad"]
CALL_TYPES = ["support", "billing", "technical"]
CHANNELS = ["phone", "chat", "callback"]
AGENT_IDS = [f"AGT-{n:03d}" for n in range(1, 21)]

# Positive sentiment words → positive score
POSITIVE_PHRASES = [
    "Thank you so much", "great service", "very helpful", "excellent support",
    "resolved my issue", "satisfied", "appreciate your help", "problem solved",
    "works perfectly now", "amazing experience", "very happy", "quick resolution",
]

# Negative sentiment words → negative score
NEGATIVE_PHRASES = [
    "very disappointed", "frustrated", "unacceptable", "this is terrible",
    "waste of time", "not working", "still broken", "no help at all",
    "keep getting disconnected", "worst experience", "I want a refund",
    "escalate this", "manager please", "this is ridiculous",
]

NEUTRAL_PHRASES = [
    "Can you help me with", "I have a question about", "I need information",
    "When will this be available", "What is the status of my",
    "I received a notification", "I would like to check",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def now_epoch() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp())


def build_transcript(sentiment_bias: str) -> list:
    """Build a list of transcript segments seeded with sentiment phrases."""
    segments = []
    t = 0.0
    turns = random.randint(4, 10)

    for i in range(turns):
        speaker = "agent" if i % 2 == 0 else "customer"

        if sentiment_bias == "positive" and i > turns // 2:
            text = random.choice(POSITIVE_PHRASES) + "."
        elif sentiment_bias == "negative" and i > turns // 2:
            text = random.choice(NEGATIVE_PHRASES) + "."
        else:
            text = random.choice(NEUTRAL_PHRASES) + " my account."

        segments.append({"speaker": speaker, "text": text, "start_sec": round(t, 1)})
        t += random.uniform(8, 30)

    return segments


def make_valid_record(ts_offset_sec: int = 0) -> dict:
    """Generate a well-formed call event."""
    sentiment_bias = random.choices(
        ["positive", "negative", "neutral"],
        weights=[0.45, 0.35, 0.20]
    )[0]

    customer_id = random.choice(CUSTOMER_IDS)
    city = random.choice(CITIES)

    return {
        "call_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "agent_id": random.choice(AGENT_IDS),
        "epoch_timestamp": now_epoch() - ts_offset_sec,
        "duration_sec": random.randint(30, 600),
        "transcript_segments": build_transcript(sentiment_bias),
        "metadata": {
            "call_type": random.choice(CALL_TYPES),
            "channel": random.choice(CHANNELS),
            "region": city,
        },
    }


def make_late_record() -> dict:
    """Record with epoch_timestamp 10–30 minutes in the past (late arrival)."""
    late_by = random.randint(600, 1800)  # 10–30 min
    rec = make_valid_record(ts_offset_sec=late_by)
    rec["_injection_type"] = "late_arriving"  # debug marker, stripped by bronze
    return rec


def make_malformed_record() -> dict:
    """Record with deliberate structural defects to test quarantine logic."""
    rec = make_valid_record()
    defect = random.choice(["missing_call_id", "null_customer_id", "negative_duration"])
    if defect == "missing_call_id":
        del rec["call_id"]
    elif defect == "null_customer_id":
        rec["customer_id"] = None
    elif defect == "negative_duration":
        rec["duration_sec"] = -random.randint(1, 100)
    rec["_injection_type"] = f"malformed_{defect}"
    return rec


# ---------------------------------------------------------------------------
# Main generator
# ---------------------------------------------------------------------------

def generate(records: int, out_dir: str, batch_size: int, delay: float):
    os.makedirs(out_dir, exist_ok=True)

    total_late = 0
    total_malformed = 0
    total_valid = 0
    batch_num = 0

    print(f"Generating {records} records -> {out_dir} (batch_size={batch_size}, delay={delay}s)")

    for start in range(0, records, batch_size):
        batch = []
        end = min(start + batch_size, records)

        for _ in range(end - start):
            roll = random.random()
            if roll < 0.05:                  # 5% malformed
                batch.append(make_malformed_record())
                total_malformed += 1
            elif roll < 0.15:               # 10% late-arriving (5+10)
                batch.append(make_late_record())
                total_late += 1
            else:
                batch.append(make_valid_record())
                total_valid += 1

        ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S")
        filename = os.path.join(out_dir, f"calls_{ts}_{batch_num:04d}.json")

        # Write one JSON object per line (newline-delimited JSON = Auto Loader friendly)
        with open(filename, "w", encoding="utf-8") as f:
            for rec in batch:
                f.write(json.dumps(rec) + "\n")

        batch_num += 1
        print(f"  [{batch_num}] Wrote {len(batch)} records -> {filename}")

        if delay > 0:
            time.sleep(delay)

    print(f"\nDone. Valid={total_valid}, Late={total_late}, Malformed={total_malformed}")
    print(f"Total files: {batch_num}")
    print("\nNext step: upload the files from")
    print(f"  {os.path.abspath(out_dir)}")
    print("to your Databricks Volume at:")
    print("  /Volumes/celebal_catalog/sentiment_pipeline/landing/calls/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Call Stream Generator for Celebal Pipeline")
    parser.add_argument("--records",    type=int,   default=500,          help="Total records to generate")
    parser.add_argument("--out-dir",    type=str,   default="./output/calls", help="Output directory")
    parser.add_argument("--batch-size", type=int,   default=20,           help="Records per file")
    parser.add_argument("--delay",      type=float, default=0.0,          help="Delay (seconds) between batches")
    args = parser.parse_args()

    generate(
        records=args.records,
        out_dir=args.out_dir,
        batch_size=args.batch_size,
        delay=args.delay,
    )
