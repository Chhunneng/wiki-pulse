"""Wikimedia recentchange SSE -> Kafka.

Reference for the event payload shape: wiki-apidoc-for-stream.json at repo root.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import threading
import time
from dataclasses import dataclass

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
from sseclient import SSEClient as EventSource

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-server:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "wiki.event.raw")
SSE_URL = os.environ.get(
    "SSE_STREAM_URL",
    "https://stream.wikimedia.org/v2/stream/recentchange",
)
USER_AGENT = os.environ.get(
    "SSE_USER_AGENT",
    "WikiPulseAnalyticsProducer/1.0 (education; kafka producer; bot policy "
    "https://foundation.wikimedia.org/wiki/Policy:Wikimedia_Reuse)",
)
T_CONNECT = float(os.environ.get("SSE_CONNECT_TIMEOUT_SEC", "15"))
# Per-read timeout on the SSE body (seconds). Prevents silent hang if the socket stays open
# but no bytes arrive. Set SSE_READ_TIMEOUT_SEC=0 to disable (not recommended).
_SSE_READ_RAW = os.environ.get("SSE_READ_TIMEOUT_SEC", "120").strip().lower()
if _SSE_READ_RAW in ("", "0", "none", "false", "off"):
    SSE_READ_TIMEOUT: float | None = None
else:
    SSE_READ_TIMEOUT = float(_SSE_READ_RAW)
BACKOFF_0 = float(os.environ.get("SSE_RECONNECT_BASE_SEC", "2"))
BACKOFF_CAP = float(os.environ.get("SSE_RECONNECT_MAX_SEC", "120"))

# Heartbeat log every N seconds while events flow. Set to 0 to silence.
HEARTBEAT_SECS = float(os.environ.get("PRODUCER_HEARTBEAT_SECS", "30"))
# Bounded loss window: flush buffered records every N seconds.
FLUSH_EVERY_SECS = float(os.environ.get("PRODUCER_FLUSH_EVERY_SECS", "10"))
# If > 0, a daemon thread logs when no SSE iterator progress for this many seconds (observability).
SSE_STALL_LOG_SEC = float(os.environ.get("SSE_STALL_LOG_SEC", "0"))

# Durability + throughput knobs. Defaults give acks=all + ordered delivery (idempotence-lite)
# at the cost of throughput. Override KAFKA_ENABLE_IDEMPOTENCE=false on a real cluster.
KAFKA_ACKS = os.environ.get("KAFKA_ACKS", "all")
KAFKA_RETRIES = int(os.environ.get("KAFKA_RETRIES", "10"))
KAFKA_LINGER_MS = int(os.environ.get("KAFKA_LINGER_MS", "20"))
KAFKA_BATCH_SIZE = int(os.environ.get("KAFKA_BATCH_SIZE", "32768"))
# Snappy is materially cheaper on CPU than gzip at similar ratios for JSON payloads.
KAFKA_COMPRESSION = os.environ.get("KAFKA_COMPRESSION", "snappy") or None
KAFKA_REQUEST_TIMEOUT_MS = int(os.environ.get("KAFKA_REQUEST_TIMEOUT_MS", "30000"))
KAFKA_DELIVERY_TIMEOUT_MS = int(os.environ.get("KAFKA_DELIVERY_TIMEOUT_MS", "120000"))
KAFKA_ENABLE_IDEMPOTENCE = os.environ.get("KAFKA_ENABLE_IDEMPOTENCE", "true").lower() in (
    "1",
    "true",
    "yes",
)


@dataclass
class DeliveryStats:
    """Tracks async Kafka delivery outcomes. Bound to a single Producer lifecycle."""

    failures: int = 0

    def on_error(self, exc: BaseException) -> None:
        self.failures += 1
        # Log loudly for the first few failures and then every 100th, so a burst of errors
        # is visible without flooding the logs forever.
        if self.failures <= 5 or self.failures % 100 == 0:
            log.error("Kafka delivery failed (#%d): %s", self.failures, exc)


def _new_producer() -> KafkaProducer:
    p = KafkaProducer(
        bootstrap_servers=BOOTSTRAP.split(","),
        acks=KAFKA_ACKS,
        retries=KAFKA_RETRIES,
        linger_ms=KAFKA_LINGER_MS,
        batch_size=KAFKA_BATCH_SIZE,
        compression_type=KAFKA_COMPRESSION,
        request_timeout_ms=KAFKA_REQUEST_TIMEOUT_MS,
        # kafka-python 2.0.2 doesn't expose enable_idempotence; emulate it by limiting in-flight
        # requests to 1 so retries can't reorder records (the only producer guarantee we need).
        max_in_flight_requests_per_connection=1 if KAFKA_ENABLE_IDEMPOTENCE else 5,
        value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
    )
    if not p.bootstrap_connected():
        p.close()
        raise RuntimeError(f"Kafka not reachable: {BOOTSTRAP}")
    log.info(
        "Kafka producer connected bootstrap=%s topic=%s acks=%s retries=%s compression=%s",
        BOOTSTRAP,
        TOPIC,
        KAFKA_ACKS,
        KAFKA_RETRIES,
        KAFKA_COMPRESSION,
    )
    return p


def _event_key(payload: dict) -> bytes | None:
    meta = payload.get("meta") or {}
    rid = meta.get("id") or payload.get("id")
    return str(rid).encode("utf-8") if rid else None


def run() -> None:
    read_t = SSE_READ_TIMEOUT
    log.info(
        "Producer running topic=%s bootstrap=%s sse_read_timeout=%s",
        TOPIC,
        BOOTSTRAP,
        f"{read_t}s" if read_t is not None else "disabled (unsafe)",
    )
    if read_t is None:
        log.warning(
            "SSE_READ_TIMEOUT_SEC is off — a stalled SSE connection can block forever with no logs"
        )

    session = requests.Session()
    headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream"}
    prod: KafkaProducer | None = None
    stats = DeliveryStats()
    backoff = BACKOFF_0
    sent = 0
    sent_at_last_heartbeat = 0
    last_flush = time.monotonic()
    last_heartbeat = time.monotonic()

    last_sse_activity = {"t": time.monotonic()}
    stall_warned = False

    def _touch_sse_activity() -> None:
        last_sse_activity["t"] = time.monotonic()
        nonlocal stall_warned
        stall_warned = False

    def _stall_watchdog() -> None:
        nonlocal stall_warned
        interval = max(10.0, SSE_STALL_LOG_SEC / 2.0)
        while True:
            time.sleep(interval)
            idle = time.monotonic() - last_sse_activity["t"]
            if idle >= SSE_STALL_LOG_SEC:
                if not stall_warned:
                    log.warning(
                        "No SSE iterator progress for %.0fs (connection may be stale; "
                        "read timeout should still break the wait)",
                        idle,
                    )
                    stall_warned = True

    if SSE_STALL_LOG_SEC > 0:
        threading.Thread(target=_stall_watchdog, name="sse-stall-watchdog", daemon=True).start()
        log.info("SSE stall watchdog enabled warn_after=%ss", SSE_STALL_LOG_SEC)

    def reconnect_wait(reason: str) -> None:
        nonlocal backoff
        log.warning("%s - reconnecting in %ss", reason, backoff)
        time.sleep(min(backoff, BACKOFF_CAP))
        backoff = min(backoff * 2, BACKOFF_CAP)

    while True:
        if prod is None:
            try:
                prod = _new_producer()
                backoff = BACKOFF_0
            except Exception as e:
                log.error("Kafka connect failed %s retry in %ss", e, backoff)
                time.sleep(min(backoff, BACKOFF_CAP))
                backoff = min(backoff * 2, BACKOFF_CAP)
                continue

        try:
            req_timeout: float | tuple[float, float | None] = (
                (T_CONNECT, read_t) if read_t is not None else (T_CONNECT, None)
            )
            with session.get(
                SSE_URL, stream=True, headers=headers, timeout=req_timeout
            ) as resp:
                resp.raise_for_status()
                backoff = BACKOFF_0
                log.info("SSE connected (HTTP %s) - reading events", resp.status_code)
                _touch_sse_activity()

                for ev in EventSource(resp).events():
                    _touch_sse_activity()
                    if (ev.event or "").strip() not in ("", "message"):
                        continue
                    raw = (ev.data or "").strip()
                    if not raw or raw == "[DONE]":
                        continue
                    try:
                        msg = json.loads(raw)
                    except (json.JSONDecodeError, ValueError):
                        continue

                    try:
                        future = prod.send(TOPIC, key=_event_key(msg), value=msg)
                        future.add_errback(stats.on_error)
                        sent += 1
                        if sent == 1:
                            log.info("First event queued to Kafka topic=%s", TOPIC)

                        now = time.monotonic()
                        if HEARTBEAT_SECS > 0 and (now - last_heartbeat) >= HEARTBEAT_SECS:
                            elapsed = max(now - last_heartbeat, 1e-6)
                            delta = sent - sent_at_last_heartbeat
                            log.info(
                                "heartbeat sent=%d (+%d in %.1fs = %.1f ev/s) "
                                "delivery_failures=%d topic=%s",
                                sent,
                                delta,
                                elapsed,
                                delta / elapsed,
                                stats.failures,
                                TOPIC,
                            )
                            last_heartbeat = now
                            sent_at_last_heartbeat = sent
                        if FLUSH_EVERY_SECS > 0 and (now - last_flush) >= FLUSH_EVERY_SECS:
                            prod.flush(timeout=KAFKA_DELIVERY_TIMEOUT_MS / 1000.0)
                            last_flush = now
                    except KafkaError as e:
                        log.error("Kafka send failed %s - recycling producer", e)
                        try:
                            prod.flush(timeout=10)
                        except Exception:
                            pass
                        prod.close()
                        prod = None
                        break
                else:
                    prod.flush()
                    reconnect_wait("SSE stream ended")
        except requests.ReadTimeout as e:
            reconnect_wait(f"SSE read timed out (no data for {read_t}s): {e}")
        except (requests.RequestException, OSError) as e:
            reconnect_wait(f"SSE disconnected: {e}")
        except Exception:
            log.exception("Unexpected error in SSE loop")
            reconnect_wait("error")


if __name__ == "__main__":
    # Non-zero exit on unhandled error so Docker's restart policy kicks in.
    try:
        run()
    except KeyboardInterrupt:
        log.info("Producer interrupted - exiting cleanly")
        sys.exit(0)
    except Exception:
        log.exception("Producer crashed - exiting non-zero so Docker restarts the container")
        sys.exit(1)
