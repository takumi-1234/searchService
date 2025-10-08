# Operational Readiness: Staging to Production

This guide covers the minimum operational preparations required before promoting `searchService` from staging to production. It documents how logging, metrics, alerting, monitoring, rollback, and incident response must be implemented and validated.

## Logging

- **Format**: JSON structured logs emitted by `zap` (`pkg/logger`). Every entry must include `timestamp`, `severity`, `service`, `trace_id`, `span_id`, `request_id`, `tenant`, and `message`.
- **Level policy**: `info` in production, `debug` allowed in staging. Sensitive payloads (documents, vectors) must be redacted prior to logging.
- **Sink**:
  1. Containers write to `stdout`.
  2. Fluent Bit (or the cluster logging agent) ships logs to the centralized stack. Preferred targets are **Cloud Logging** (GKE) or the ELK stack (self-managed).
  3. Logs are retained for 30 days; error logs for 90 days to support incident analysis.
- **Indices / labels**: When shipping to ELK, use index pattern `search-service-%{+YYYY.MM.dd}` and label each entry with `environment`, `version`, and `cluster`.
- **Verification**: During staging bake time, ensure log entries appear in the target sink with trace correlation working end to end (OpenTelemetry trace IDs must match).

## Metrics & Alerting

- **Exporter**: Prometheus endpoint exposed on `observability.metrics.address` (default `:9464`). Enable scrape via ServiceMonitor in staging and production.
- **Instrumentation**:
  - gRPC unary interceptor captures request count, latency histogram (`search_grpc_request_duration_seconds`), and error counts.
  - Kafka consumer emits `search_kafka_consumer_lag` (gauge) and processing duration histogram.
  - Elasticsearch/Qdrant clients emit call duration and error counters via OpenTelemetry metrics.
- **Key SLI metrics**:
  | Metric | Type | Description |
  | --- | --- | --- |
  | `search_grpc_requests_total{code}` | Counter | gRPC request volume split by status code. |
  | `search_grpc_request_duration_seconds_bucket` | Histogram | Latency distribution (P50, P95, P99). |
  | `search_grpc_errors_total{code}` | Counter | Error volume by canonical code. |
  | `search_hybrid_result_merge_seconds` | Histogram | Time spent merging ES/Qdrant results. |
  | `search_kafka_consumer_lag` | Gauge | Latest committed offset lag per partition. |
  | `search_kafka_retries_total` | Counter | Count of retry attempts in the Kafka consumer. |
  | `search_index_updates_total{action}` | Counter | Successful indexing operations (UPSERT, DELETE). |
- **SLO targets**:
  - Availability: 99.9% success (`code == "OK"`) over rolling 30 days.
  - Latency: 95th percentile < 500 ms, 99th percentile < 900 ms.
  - Kafka freshness: `search_kafka_consumer_lag < 1_000` messages for 99% of samples.
- **Alert rules (Prometheus)**:
  ```yaml
  groups:
    - name: search-service.slo
      rules:
        - alert: SearchAvailabilityBudgetBurn
          expr: |
            (1 - rate(search_grpc_requests_total{code="OK"}[5m]) / rate(search_grpc_requests_total[5m])) > 0.001
          for: 10m
          labels:
            severity: critical
          annotations:
            summary: "searchService availability error budget burn"
            description: "Error rate exceeded 0.1% over the last 10 minutes."

        - alert: SearchLatencyBudgetBurn
          expr: |
            histogram_quantile(0.95, sum(rate(search_grpc_request_duration_seconds_bucket[5m])) by (le)) > 0.5
          for: 10m
          labels:
            severity: warning
          annotations:
            summary: "searchService high latency (P95)"
            description: "P95 latency exceeded 500ms for 10 minutes."

        - alert: SearchKafkaLagHigh
          expr: search_kafka_consumer_lag > 2000
          for: 15m
          labels:
            severity: warning
          annotations:
            summary: "Kafka consumer lag high"
            description: "Lag stayed above 2000 messages for 15 minutes."
  ```
- Validate alert delivery paths (PagerDuty/Slack) in staging prior to go-live.

## Monitoring Dashboards

Provision a shared Grafana dashboard (`searchService / Production Overview`) with the following panels:

- **API Overview**:
  - Request rate: `sum(rate(search_grpc_requests_total[1m]))` grouped by method.
  - Error rate: `1 - (rate(search_grpc_requests_total{code="OK"}[5m]) / rate(search_grpc_requests_total[5m]))`.
  - Latency: P50/P95/P99 derived from the latency histogram.
- **Hybrid Search Internals**:
  - Elasticsearch vs Qdrant call latency split.
  - Hybrid merge duration histogram percentiles.
  - Result score distributions (via custom metric or log-based panel).
- **Kafka Processing**:
  - Consumer lag per partition.
  - Retry attempt rate (`search_kafka_retries_total`).
  - DLQ throughput (number of messages routed to the DLQ topic).
- **Infrastructure**:
  - Pod CPU/memory (via kube-state-metrics).
  - gRPC connection pool saturation (OpenTelemetry metric if enabled).
- **Alert summary panel** (Grafana alert list) to mirror live alerts.

Dashboards must be parameterized by `environment` and `tenant` labels to support staging vs. production comparisons.

## Deployment Gates

1. **Staging verification**
   - `make ci` and `make test-integration` green.
   - Canary deployment in staging (>=30 minutes) with no alerts firing.
   - Observability smoke test: confirm metrics scrape, logs ingested, traces flowing to Jaeger.
2. **Change review**
   - Architecture review for migrations or infra-impacting changes.
   - Security review if new dependencies or data paths are introduced.
3. **Production approval**
   - Release manager confirms change log, rollback plan, and monitoring baseline.
   - Schedule within maintenance window when applicable.

## Rollback Procedure

1. **Identify target version**: Determine last known good image tag (e.g., `registry/search-service:2024-05-15-1`). Keep at least two prior versions available.
2. **Application rollback**:
   - Helm: `helm rollback search-service <revision>` or
   - Raw deployment: `kubectl rollout undo deployment/search-service`.
   - Monitor `kubectl rollout status` until completion.
3. **Config rollback**: Revert ConfigMap or Secret via `kubectl rollout undo` or reapply the previous manifest.
4. **Database / store rollback**:
   - No RDB migrations are used, but Elasticsearch/Qdrant index changes must be reverted if schema-altering changes were shipped.
   - If new index mappings were introduced, reapply the previous mapping and reindex from the last successful snapshot.
5. **Kafka DLQ reconciliation**: Replay DLQ messages once the service stabilizes by producing them back into the primary topic in controlled batches.
6. **Post-rollback validation**:
   - Ensure alert backlog clears and dashboards return to baseline.
   - Record the incident in the runbook log with root cause summary.

## Runbook

### General Flow

1. **Detection**: Alert received via PagerDuty/Slack.
2. **Triage**: Check Grafana dashboard for the impacted SLI. Correlate with logs and traces (filter by `trace_id` from alerts when possible).
3. **Mitigation**: Apply the relevant scenario response (below).
4. **Communication**: Update incident channel and stakeholder mailing list every 15 minutes.
5. **Resolution & Review**: After mitigation, create a post-incident report within 24 hours.

### Scenario: Elevated Error Rate

- **Symptoms**: `SearchAvailabilityBudgetBurn` alert firing, `code != OK` spikes.
- **Immediate actions**:
  - Inspect recent deploys; consider rolling back if deployment occurred within the last hour.
  - Check Elasticsearch and Qdrant health endpoints (`/_cluster/health`, `/healthz`).
- **Diagnostics**:
  - Use structured logs filtered by `severity=error` and `request_id`.
  - Review gRPC traces in Jaeger to pinpoint the failing dependency.
- **Mitigations**:
  - Toggle feature flags if a new feature caused the regression.
  - Fallback to keyword-only search by flipping the `search.hybrid.enabled` flag (set in ConfigMap) if Qdrant is degraded.

### Scenario: Latency SLO Breach

- **Symptoms**: `SearchLatencyBudgetBurn` alert, P95 > 500 ms.
- **Diagnostics**:
  - Grafana: compare latency between Elasticsearch and Qdrant calls.
  - Check resource utilization; ensure HPA scaled out.
  - Review Kafka backlog for upstream contention.
- **Mitigations**:
  - Manually scale deployment: `kubectl scale deployment/search-service --replicas=<n>`.
  - Increase Elasticsearch threadpool or adjust Qdrant shard replicas if bottlenecked.
  - Enable caching for hot queries via API Gateway rate limiting.

### Scenario: Kafka Consumer Lag

- **Symptoms**: `SearchKafkaLagHigh` alert, growing lag metrics.
- **Diagnostics**:
  - Inspect consumer logs for processing failures or DLQ events.
  - Validate Kafka broker health and partition distribution.
- **Mitigations**:
  - Scale consumer replicas (increase deployment replicas).
  - If a specific payload causes repeated failures, confirm DLQ ingestion and notify data producers.
  - After recovery, replay DLQ messages in batches (`kafka-replay --source dlq --target search.indexing.requests --batch-size 100`).

### Scenario: Infrastructure Outage

- **Symptoms**: Pod restarts, readiness probe failures.
- **Diagnostics**:
  - `kubectl describe pod` for events.
  - Check node capacity and Kubernetes cluster health.
- **Mitigations**:
  - Restart failing pods: `kubectl rollout restart deployment/search-service`.
  - Coordinate with platform team if cluster-level outage is involved.

## Artifact Checklist Before Go-Live

- Logging verified in Cloud Logging / ELK with trace correlation.
- Prometheus scrape and Grafana dashboard configured in staging and production namespaces.
- Alert rules deployed and notification channels tested.
- Rollback command snippets stored in the incident response repo.
- Runbook owners assigned and on-call rotation updated in PagerDuty.

Maintaining this document is a release gate item: update it whenever observability architecture, alert thresholds, or deployment mechanics change.
