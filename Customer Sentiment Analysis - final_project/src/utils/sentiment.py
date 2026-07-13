"""
sentiment.py — Shared sentiment scoring utility
================================================
Provides score_text(text) → float in [-1.0, +1.0] and label → str.

Design: keyword-based scoring for zero-dependency portability on any
Databricks runtime. To swap in VADER or a transformer model, only replace
score_text() — all DLT table definitions remain unchanged.

Usage from DLT notebooks:
    from src.utils.sentiment import score_text, score_label, SENTIMENT_UDF

    # As a plain function
    score = score_text("I am frustrated with the service")

    # As a Spark UDF (registered in pipeline.py)
    df = df.withColumn("sentiment_score", SENTIMENT_UDF(col("text")))
"""

import re
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType, StringType

# ---------------------------------------------------------------------------
# Keyword dictionaries  (extend as needed)
# ---------------------------------------------------------------------------

POSITIVE_WORDS = {
    "excellent", "great", "happy", "satisfied", "resolved", "helpful",
    "perfect", "amazing", "wonderful", "fantastic", "appreciate", "thanks",
    "thank you", "pleased", "good", "nice", "love", "impressed", "awesome",
    "outstanding", "efficient", "quick", "fast", "smooth", "easy", "works",
    "fixed", "sorted", "solved", "glad", "delighted", "superb", "brilliant",
}

NEGATIVE_WORDS = {
    "frustrated", "disappointed", "terrible", "awful", "horrible", "bad",
    "worst", "waste", "broken", "failed", "unacceptable", "angry", "upset",
    "disgusted", "useless", "incompetent", "never", "always", "escalate",
    "refund", "cancel", "complaint", "problem", "issue", "error", "bug",
    "disconnected", "dropped", "slow", "delayed", "missing", "wrong", "lie",
    "lied", "cheated", "scam", "fraud", "ridiculous", "absurd",
}

# Intensifiers that double the weight of the next matched word
INTENSIFIERS = {"very", "extremely", "absolutely", "completely", "totally"}


# ---------------------------------------------------------------------------
# Core scoring function
# ---------------------------------------------------------------------------

def score_text(text: str) -> float:
    """
    Compute a sentiment score for a piece of text.

    Returns
    -------
    float in [-1.0, +1.0]
      +1.0 = strongly positive
       0.0 = neutral / no signal
      -1.0 = strongly negative
    """
    if not text:
        return 0.0

    tokens = re.findall(r"[a-z']+", text.lower())
    score = 0.0
    weight = 1.0

    for token in tokens:
        if token in INTENSIFIERS:
            weight = 2.0
            continue
        if token in POSITIVE_WORDS:
            score += weight
        elif token in NEGATIVE_WORDS:
            score -= weight
        weight = 1.0  # reset after consumption

    # Normalize to [-1, +1].
    # Use (abs(raw) + 1) so that intensifiers (weight=2) always produce
    # a strictly different value from the un-intensified (weight=1) case.
    if score == 0:
        return 0.0
    norm = score / (abs(score) + 1.0)
    return round(max(-1.0, min(1.0, norm)), 4)


def score_label(score: float) -> str:
    """Convert a numeric score to a categorical label."""
    if score > 0.1:
        return "positive"
    elif score < -0.1:
        return "negative"
    return "neutral"


# ---------------------------------------------------------------------------
# Spark UDFs (registered when imported inside a Spark session)
# ---------------------------------------------------------------------------

SENTIMENT_SCORE_UDF = udf(score_text, FloatType())
SENTIMENT_LABEL_UDF = udf(score_label, StringType())
