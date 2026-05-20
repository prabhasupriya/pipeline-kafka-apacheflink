# Real-Time Feature Engineering Analysis

## 1. Windowing Strategy
* **User Engagement (Tumbling):** We implemented a `TUMBLE(row_time, INTERVAL '1' HOUR)`. This ensures each user event belongs to exactly one hour-long bucket, preventing data overlap for hourly metrics and ensuring computational efficiency.
* **Content Engagement (Sliding):** We utilized `HOP(row_time, INTERVAL '5' MINUTE, INTERVAL '15' MINUTE)`. This provides a rolling 15-minute average updated every 5 minutes, which is optimal for capturing high-velocity viral content trends in real-time.

## 2. Event-Time & Watermarking
* **Watermarks:** A 30-second watermark (`INTERVAL '30' SECOND`) was applied to the `user_events` stream. This allows the pipeline to handle late-arriving data caused by network latency or producer delays, ensuring the system remains robust against real-world ingestion issues without dropping critical events.

## 3. Data Consistency
* **Log Compaction:** Both the `content-metadata` and `feature-store` topics are configured with `cleanup.policy=compact`.
    * **For Metadata:** This ensures Flink always has access to the latest category information for any given `content_id`.
    * **For the Feature Store:** This allows the topic to act as a permanent Key-Value lookup, where only the freshest version of a calculated feature value is retained.

## 4. Enrichment
* **Stream-Table Join:** We performed a join between the high-velocity `user_events` stream and the `content_metadata` reference table. This process enriches raw interaction data with high-level category information (e.g., "Sports", "Tech"), enabling the computation of advanced category affinity features.