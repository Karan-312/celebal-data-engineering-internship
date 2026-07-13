# Data Dictionary — Customer Sentiment Monitoring Pipeline

> Complete column-level documentation for all tables across Bronze, Silver, and Gold layers.

---

## Bronze Layer

### `bronze_calls`

| Column | Type | Description |
|---|---|---|
| `call_id` | string | UUID v4 uniquely identifying the call. May be NULL for malformed records (warn-only). |
| `customer_id` | integer | Foreign key to customer dimension. |
| `agent_id` | string | Format: `AGT-NNN`. Identifies the support agent. |
| `epoch_timestamp` | long | Unix timestamp (seconds) when the call event occurred. Used to derive `event_ts` in Silver. |
| `duration_sec` | integer | Call duration in seconds. Negative values are malformed; flagged by @dlt.expect. |
| `transcript_segments` | array\<struct\> | Nested array of spoken turns. Each element: `{speaker, text, start_sec}`. |
| `metadata` | struct | Nested: `{call_type: string, channel: string, region: string}`. |
| `source_file` | string | Full path of the file that contributed this record (from Auto Loader `_metadata.file_path`). |
| `ingestion_timestamp` | timestamp | When the record was ingested into the Bronze layer. |

### `bronze_customers`

| Column | Type | Description |
|---|---|---|
| `customer_id` | integer | Natural key for the customer. |
| `city` | string | Customer's city of residence. Values: Delhi, Mumbai, Bangalore, Chennai, Hyderabad. |
| `subscription_type` | string | Plan tier. Values: Basic, Premium. |
| `age` | integer | Customer age in years. |
| `signup_date` | date | Date the customer signed up for the service. |
| `operation` | string | CDC operation type: INSERT, UPDATE, or DELETE. |
| `update_ts` | timestamp | When this CDC event was generated at the source system. Used for SCD2 sequencing. |
| `source_file` | string | Source file path from Auto Loader. |
| `ingestion_timestamp` | timestamp | When the record was ingested. |

---

## Silver Layer

### `silver_calls_clean`

One row per **transcript segment** (exploded from `transcript_segments` array). Aggregate on `call_id` to get call-level metrics.

| Column | Type | Description |
|---|---|---|
| `call_id` | string | UUID. |
| `customer_id` | integer | FK to customer dimension. |
| `agent_id` | string | Supporting agent. |
| `event_ts` | timestamp | Derived from `epoch_timestamp`. Used as the event-time anchor for watermarking. |
| `duration_sec` | integer | Call duration (≥ 0 enforced by @dlt.expect_or_drop). |
| `call_type` | string | Flattened from `metadata.call_type`. |
| `channel` | string | Flattened from `metadata.channel`. |
| `region` | string | Flattened from `metadata.region` (call-side city, may differ from customer SCD2 city). |
| `speaker` | string | `agent` or `customer`. |
| `text` | string | The spoken text for this segment turn. |
| `start_sec` | float | Offset (seconds) from call start. |
| `sentiment_score` | float | Score in [-1.0, +1.0]. Positive = good experience. Computed by `score_text()`. |
| `sentiment_label` | string | `positive` (score > 0.1), `negative` (score < -0.1), or `neutral`. |
| `source_file` | string | Lineage: originating file. |
| `ingestion_timestamp` | timestamp | Lineage: when ingested. |

### `silver_customers_clean`

| Column | Type | Description |
|---|---|---|
| `customer_id` | integer | Natural key. |
| `city` | string | Customer city for this CDC event. |
| `subscription_type` | string | Plan tier for this CDC event. |
| `age` | integer | Customer age. |
| `signup_date` | date | Signup date. |
| `operation` | string | CDC operation. |
| `update_ts` | timestamp | Source-system change timestamp (used for SCD2 sequencing). |
| `source_file` | string | Lineage. |
| `ingestion_timestamp` | timestamp | Lineage. |

### `silver_customers_rejects`

| Column | Type | Description |
|---|---|---|
| *(all bronze_customers columns)* | — | All original columns preserved for audit. |
| `reject_reason` | string | Why the row was quarantined: `null_customer_id`, `invalid_operation`, `null_update_ts`, or `unknown`. |

### `silver_customers_scd2`

SCD Type 2 materialized view maintained by `dlt.apply_changes()`.

| Column | Type | Description |
|---|---|---|
| `customer_id` | integer | Natural key. |
| `city` | string | Customer city for this historical period. |
| `subscription_type` | string | Plan tier for this period. |
| `age` | integer | Age at time of record. |
| `signup_date` | date | Signup date. |
| `__start_at` | timestamp | When this version of the customer record became active. |
| `__end_at` | timestamp | When this version was superseded. NULL = currently active row. |

**To get current attributes:** `WHERE __end_at IS NULL`  
**To get attributes at a point in time T:** `WHERE __start_at <= T AND (__end_at IS NULL OR __end_at > T)`

---

## Gold Layer

### `gold_sentiment_daily`

| Column | Type | Description |
|---|---|---|
| `date` | date | Calendar date. |
| `total_calls` | long | Distinct calls received on this date. |
| `positive_calls` | long | Calls with `call_label = 'positive'`. |
| `negative_calls` | long | Calls with `call_label = 'negative'`. |
| `neutral_calls` | long | Calls with `call_label = 'neutral'`. |
| `avg_sentiment_score` | double | Mean sentiment score across all calls. |
| `sentiment_ratio` | double | `positive_calls / total_calls`. Higher = better day. |

### `gold_call_volume_daily`

| Column | Type | Description |
|---|---|---|
| `date` | date | Calendar date. |
| `total_calls` | long | Total distinct calls. |
| `avg_duration_sec` | double | Average call duration in seconds. |
| `max_duration_sec` | long | Longest call of the day. |
| `min_duration_sec` | long | Shortest call of the day. |

### `gold_high_risk_customers`

| Column | Type | Description |
|---|---|---|
| `customer_id` | integer | Customer identifier. |
| `city` | string | Current city (from SCD2 active row). |
| `subscription_type` | string | Current plan tier. |
| `age` | integer | Customer age. |
| `negative_call_count_7d` | long | Negative-sentiment calls in the trailing 7 days. |
| `total_calls_7d` | long | All calls (any sentiment) in the trailing 7 days. |
| `last_negative_call_ts` | timestamp | Timestamp of the most recent negative call. |
| `avg_sentiment_score_7d` | double | Average sentiment over 7 days. |

Threshold: `negative_call_count_7d >= 3`.

### `gold_region_sentiment`

| Column | Type | Description |
|---|---|---|
| `city` | string | Customer's current city. |
| `date` | date | Calendar date. |
| `total_calls` | long | Distinct calls from this city on this date. |
| `avg_sentiment_score` | double | Mean sentiment score. |
| `positive_calls` | long | Positive-label call count. |
| `negative_calls` | long | Negative-label call count. |
| `neutral_calls` | long | Neutral-label call count. |
| `sentiment_ratio` | double | `positive_calls / total_calls`. |

### `gold_subscription_sentiment`

| Column | Type | Description |
|---|---|---|
| `subscription_type` | string | `Basic` or `Premium`. |
| `date` | date | Calendar date. |
| `total_calls` | long | Distinct calls for this tier on this date. |
| `avg_sentiment_score` | double | Mean sentiment score. |
| `positive_calls` | long | Positive-label call count. |
| `negative_calls` | long | Negative-label call count. |
| `neutral_calls` | long | Neutral-label call count. |
| `sentiment_ratio` | double | `positive_calls / total_calls`. |
