#!/usr/bin/env bash
# Creates the pipeline topics idempotently. Runs once as the `kafka-init` compose service.
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP_SERVERS:-kafka-1:19092}"
TOPICS=/opt/kafka/bin/kafka-topics.sh

# Main event stream.
#   6 partitions       -> up to 6 parallel consumers in the processor group; keyed by pickup zone
#   RF 3 / min ISR 2   -> survives the loss of one broker without losing acknowledged writes
#   7 days retention   -> enough to replay / backfill after a downstream incident
#   lz4 compression    -> keep whatever the producer sent (producer compresses batches with lz4)
$TOPICS --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic taxi-trips-stream \
    --partitions 6 \
    --replication-factor 3 \
    --config min.insync.replicas=2 \
    --config retention.ms=604800000 \
    --config cleanup.policy=delete \
    --config compression.type=producer \
    --config max.message.bytes=1048576

# Dead-letter queue for events that fail validation; kept longer for investigation.
$TOPICS --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic taxi-trips-dlq \
    --partitions 3 \
    --replication-factor 3 \
    --config min.insync.replicas=2 \
    --config retention.ms=1209600000

$TOPICS --bootstrap-server "$BOOTSTRAP" --describe --topic taxi-trips-stream
$TOPICS --bootstrap-server "$BOOTSTRAP" --describe --topic taxi-trips-dlq
echo "topics ready"
