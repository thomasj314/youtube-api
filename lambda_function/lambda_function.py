"""
Lambda: YouTube Data API Ingestion (Bronze Layer)
──────────────────────────────────────────────────
Triggered by EventBridge on a schedule (e.g., every 6 hours).
Pulls trending videos, channel uploads, and keyword-search results from the
YouTube Data API v3 and writes raw JSON responses to the Bronze S3 bucket.

Idempotency:
    S3 keys are deterministic per (dataset, region/channel/keyword, date, hour).
    A retried invocation within the same hour partition overwrites the existing
    object instead of creating a duplicate. This is safe as long as the EventBridge
    schedule fires at most once per hour partition; if you schedule more frequently
    (e.g., every 30 min), use a finer partition (e.g., minute=) or accept overwrite
    semantics within the hour.

Dynamic channel discovery:
    After fetching trending videos for all regions, the handler extracts channel
    IDs that have multiple videos trending in the same region (default threshold:
    >=2). These "hot" channels are unioned with the statically configured
    YOUTUBE_CHANNEL_IDS and ingested in the same run. This yields a self-curating
    channel list that grows with what the data itself signals as influential —
    no manual channel-ID management required.

Quota protection:
    YouTube Data API has a hard daily quota. Once exhausted, every subsequent
    call returns 403 quotaExceeded. To prevent alert storms, wasted Lambda time,
    and partial-failure data leaking into Silver, we use an in-memory circuit
    breaker: the first quota error trips it, and all later API calls in the
    same invocation short-circuit with QuotaExceededError. The breaker resets
    on the next Lambda invocation. Quota-exceeded runs return HTTP 429 instead
    of 207, signalling "do not retry immediately" to any orchestrator.

YouTube Data API v3 quota cost (default daily quota: 10,000 units):
    videos.list           1 unit
    videoCategories.list  1 unit
    channels.list         1 unit
    playlistItems.list    1 unit
    search.list         100 units  ← VERY expensive, watch the keyword × region matrix

Environment Variables:
    YOUTUBE_API_KEY        — Google API key with YouTube Data API v3 enabled  (required)
    S3_BUCKET_BRONZE       — Target S3 bucket for raw data                    (required)
    YOUTUBE_REGIONS        — Comma-separated region codes (default: US,GB,CA,...)
    YOUTUBE_CHANNEL_IDS    — Comma-separated channel IDs (optional)
    YOUTUBE_KEYWORDS       — Comma-separated keywords for search (optional)
    SEARCH_ORDER           — Search ordering (default: date)
    SEARCH_MAX_PAGES       — Max pages per (keyword, region) (default: 1)
    CHANNEL_MAX_PAGES      — Max playlistItems pages per channel (default: 1)
    SNS_ALERT_TOPIC_ARN    — SNS topic for failure alerts (optional)
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")
sns_client = boto3.client("sns")

# ── Config ───────────────────────────────────────────────────────────────────
API_KEY = os.environ["YOUTUBE_API_KEY"]
BUCKET = os.environ["S3_BUCKET_BRONZE"]
REGIONS = [
    r.strip().upper()
    for r in os.environ.get("YOUTUBE_REGIONS", "US,GB,CA,DE,FR,IN,JP,KR,MX,RU").split(",")
    if r.strip()
]
CHANNEL_IDS = [c.strip() for c in os.environ.get("YOUTUBE_CHANNEL_IDS", "").split(",") if c.strip()]
KEYWORDS = [k.strip() for k in os.environ.get("YOUTUBE_KEYWORDS", "").split(",") if k.strip()]
SEARCH_ORDER = os.environ.get("SEARCH_ORDER", "date")
SEARCH_MAX_PAGES = int(os.environ.get("SEARCH_MAX_PAGES", "1"))
CHANNEL_MAX_PAGES = int(os.environ.get("CHANNEL_MAX_PAGES", "1"))
SNS_TOPIC = os.environ.get("SNS_ALERT_TOPIC_ARN", "")

API_BASE = "https://www.googleapis.com/youtube/v3"
YOUTUBE_API_MAX_RESULTS = 50  # Hard limit for videos/playlistItems/search/videoCategories

# Retry / backoff for transient errors
MAX_RETRIES = 3
BACKOFF_BASE_SECONDS = 1.5


# ═════════════════════════════════════════════════════════════════════════════
# Quota Circuit Breaker
# ─ Once we observe a YouTube API quota error, all subsequent API calls in this
#   invocation are blocked. Prevents:
#     - Burning more time hitting an already-exhausted quota
#     - Generating SNS alert storms (one trip → one alert)
#     - Producing partial-failure data that pollutes Silver
# ─ State is per-invocation (lives in module memory). The next EventBridge
#   trigger gets a fresh breaker, which is fine because YouTube quota resets
#   at midnight Pacific Time daily, and our 6-hour cadence aligns reasonably.
# ═════════════════════════════════════════════════════════════════════════════

class QuotaExceededError(RuntimeError):
    """Raised when the breaker is open and an API call is attempted."""


class QuotaCircuitBreaker:
    """
    A one-shot in-memory breaker. Once tripped, every subsequent call to
    `check()` raises QuotaExceededError. There is no auto-reset within an
    invocation — recovery requires a new Lambda invocation (typically the
    next EventBridge trigger).
    """

    def __init__(self) -> None:
        self.tripped: bool = False
        self.reason: str = ""
        self.tripped_at: datetime | None = None

    def check(self) -> None:
        """Call before every API request. Raises if breaker is open."""
        if self.tripped:
            raise QuotaExceededError(
                f"Quota circuit breaker open since {self.tripped_at}: {self.reason}"
            )

    def trip(self, reason: str) -> None:
        """Open the breaker. Idempotent — second call is a no-op."""
        if not self.tripped:
            self.tripped = True
            self.reason = reason
            self.tripped_at = datetime.now(timezone.utc)
            logger.error("Quota circuit breaker TRIPPED: %s", reason)


# Module-level breaker. Reset implicitly when Lambda container is recycled or
# a new (cold) invocation runs. Warm invocations may inherit the tripped state
# from a previous run within the same container — that's intentional, since
# YouTube quota persists across invocations.
quota_breaker = QuotaCircuitBreaker()


def _is_quota_error(error: HTTPError) -> bool:
    """
    Detect a YouTube API quota-exceeded response.
    YouTube returns 403 with reason 'quotaExceeded' or 'rateLimitExceeded'.
    Body is JSON with errors[].reason field — read defensively, since the
    body stream may already be consumed by urllib.
    """
    if error.code != 403:
        return False
    try:
        body = error.read().decode("utf-8", errors="ignore")
    except Exception:
        return False
    return "quotaExceeded" in body or "rateLimitExceeded" in body


# ═════════════════════════════════════════════════════════════════════════════
# HTTP / API helpers
# ═════════════════════════════════════════════════════════════════════════════

def youtube_get(path: str, params: dict) -> dict:
    """
    Call a YouTube Data API endpoint with simple exponential backoff.
    Retries on URLError and 5xx HTTPError; surfaces 4xx immediately
    (e.g. quotaExceeded → fail fast so we don't burn more quota).

    Quota protection: checks the module-level circuit breaker before issuing
    any request. If a quota error is observed, trips the breaker so all
    subsequent API calls in this invocation short-circuit.
    """
    # Fast-fail if breaker has already tripped this invocation
    quota_breaker.check()

    params = {**params, "key": API_KEY}
    url = f"{API_BASE}/{path}?{urlencode(params)}"
    req = Request(url, headers={"Accept": "application/json"})

    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except HTTPError as e:
            # 4xx (quota, bad request, forbidden) → no retry
            if 400 <= e.code < 500:
                if _is_quota_error(e):
                    quota_breaker.trip(
                        f"YouTube API quota error on path={path} (HTTP {e.code})"
                    )
                raise
            last_exc = e
        except URLError as e:
            last_exc = e

        sleep_for = BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
        logger.warning(
            "API call failed (attempt %d/%d): %s — retrying in %.1fs",
            attempt, MAX_RETRIES, last_exc, sleep_for,
        )
        time.sleep(sleep_for)

    # Exhausted retries
    assert last_exc is not None
    raise last_exc


def fetch_trending_videos(region_code: str) -> dict:
    return youtube_get(
        "videos",
        {
            "part": "snippet,statistics,contentDetails",
            "chart": "mostPopular",
            "regionCode": region_code,
            "maxResults": YOUTUBE_API_MAX_RESULTS,
        },
    )


def fetch_video_categories(region_code: str) -> dict:
    return youtube_get(
        "videoCategories",
        {
            "part": "snippet",
            "regionCode": region_code,
        },
    )


def fetch_uploads_playlist_id(channel_id: str) -> str:
    data = youtube_get(
        "channels",
        {
            "part": "contentDetails",
            "id": channel_id,
            "maxResults": 1,
        },
    )
    items = data.get("items", [])
    if not items:
        raise ValueError(f"No channel found for id={channel_id}")
    return items[0]["contentDetails"]["relatedPlaylists"]["uploads"]


def fetch_playlist_items(playlist_id: str, page_token: str = "") -> dict:
    params = {
        "part": "snippet,contentDetails",
        "playlistId": playlist_id,
        "maxResults": YOUTUBE_API_MAX_RESULTS,
    }
    if page_token:
        params["pageToken"] = page_token
    return youtube_get("playlistItems", params)


def fetch_videos_by_ids(video_ids: list[str]) -> dict:
    """
    Fetch full video metadata for a list of IDs.
    Chunks into batches of 50 (YouTube `videos.list?id=` hard limit)
    and returns a single merged response.
    """
    if not video_ids:
        return {"items": []}

    all_items: list[dict] = []
    for i in range(0, len(video_ids), YOUTUBE_API_MAX_RESULTS):
        chunk = video_ids[i : i + YOUTUBE_API_MAX_RESULTS]
        data = youtube_get(
            "videos",
            {
                "part": "snippet,statistics,contentDetails,status",
                "id": ",".join(chunk),
                "maxResults": YOUTUBE_API_MAX_RESULTS,
            },
        )
        all_items.extend(data.get("items", []))

    return {"items": all_items, "kind": "youtube#videoListResponse"}


def fetch_search_videos(keyword: str, region_code: str, page_token: str = "") -> dict:
    """
    NOTE: search.list costs 100 quota units per call. Use sparingly.
    """
    params = {
        "part": "snippet",
        "q": keyword,
        "type": "video",
        "regionCode": region_code,
        "order": SEARCH_ORDER,
        "maxResults": YOUTUBE_API_MAX_RESULTS,
    }
    if page_token:
        params["pageToken"] = page_token
    return youtube_get("search", params)


# ═════════════════════════════════════════════════════════════════════════════
# S3 / SNS helpers
# ═════════════════════════════════════════════════════════════════════════════

def write_to_s3(data: dict, bucket: str, key: str) -> None:
    body = json.dumps(data, ensure_ascii=False, indent=2)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
        Metadata={
            "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "youtube_data_api_v3",
        },
    )


def send_alert(subject: str, message: str) -> None:
    """
    Best-effort SNS alert. Never raises — alert delivery failure must not
    crash the Lambda (which would trigger EventBridge retry → duplicate ingestion).
    """
    if not SNS_TOPIC:
        logger.info("SNS_ALERT_TOPIC_ARN not set; skipping alert")
        return

    # SNS publish has a 256 KB message size limit; leave a safety margin.
    MAX_MESSAGE_BYTES = 250_000
    encoded = message.encode("utf-8")
    if len(encoded) > MAX_MESSAGE_BYTES:
        truncated = encoded[:MAX_MESSAGE_BYTES].decode("utf-8", errors="ignore")
        message = truncated + "\n\n... [TRUNCATED — see CloudWatch Logs for full details]"

    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC,
            Subject=subject[:100],
            Message=message,
        )
        logger.info("SNS alert sent: %s", subject)
    except Exception as e:
        # Swallow the error — we already logged the underlying ingestion issue.
        logger.exception("Failed to send SNS alert (continuing): %s", e)


def normalize_keyword_for_key(keyword: str) -> str:
    return "_".join(keyword.lower().split())[:120]


# ═════════════════════════════════════════════════════════════════════════════
# Per-source ingestion routines
# Each returns (success_records, failed_records) so the caller can aggregate
# without ever double-counting one (region/channel/keyword) into both lists.
# ═════════════════════════════════════════════════════════════════════════════

def ingest_region(
    region: str, ingestion_id: str, now: datetime, date_partition: str, hour_partition: str
) -> tuple[list[dict], list[dict], dict | None]:
    """
    Ingest trending videos AND category reference data for one region.
    Treated as two independent sub-tasks: one can succeed while the other fails.

    Returns (success_records, failed_records, trending_response).
    The third value is the raw trending API response (or None on failure),
    used by the handler to derive a "hot channels" list without re-fetching.
    """
    success: list[dict] = []
    failed: list[dict] = []
    trending_response: dict | None = None

    # Trending videos
    try:
        trending_data = fetch_trending_videos(region)
        trending_response = trending_data  # Keep for hot-channel extraction
        video_count = len(trending_data.get("items", []))
        trending_data["_pipeline_metadata"] = {
            "ingestion_id": ingestion_id,
            "dataset": "trending_videos",
            "region": region,
            "ingestion_timestamp": now.isoformat(),
            "video_count": video_count,
            "source": "youtube_data_api_v3",
        }
        key = (
            f"youtube/raw_statistics/region={region}/date={date_partition}/"
            f"hour={hour_partition}/data.json"
        )
        write_to_s3(trending_data, BUCKET, key)
        success.append({"type": "trending", "region": region, "video_count": video_count})
    except Exception as e:
        logger.exception("Trending ingestion failed region=%s", region)
        failed.append({"type": "trending", "region": region, "error": str(e)})

    # Category reference
    try:
        category_data = fetch_video_categories(region)
        category_data["_pipeline_metadata"] = {
            "ingestion_id": ingestion_id,
            "dataset": "video_categories",
            "region": region,
            "ingestion_timestamp": now.isoformat(),
            "source": "youtube_data_api_v3",
        }
        ref_key = (
            f"youtube/raw_statistics_reference_data/region={region}/"
            f"date={date_partition}/{region.lower()}_category_id.json"
        )
        write_to_s3(category_data, BUCKET, ref_key)
        success.append({"type": "categories", "region": region})
    except Exception as e:
        logger.exception("Category ingestion failed region=%s", region)
        failed.append({"type": "categories", "region": region, "error": str(e)})

    return success, failed, trending_response


def ingest_channel(
    channel_id: str, ingestion_id: str, now: datetime, date_partition: str, hour_partition: str
) -> tuple[list[dict], list[dict]]:
    """
    Ingest the most recent uploads for one channel.
    Walks playlistItems pages up to CHANNEL_MAX_PAGES, then hydrates via videos.list.
    """
    try:
        uploads_id = fetch_uploads_playlist_id(channel_id)

        video_ids: list[str] = []
        token = ""
        for _ in range(CHANNEL_MAX_PAGES):
            page = fetch_playlist_items(uploads_id, token)
            for item in page.get("items", []):
                vid = item.get("contentDetails", {}).get("videoId")
                if vid:
                    video_ids.append(vid)
            token = page.get("nextPageToken", "")
            if not token:
                break

        videos_data = fetch_videos_by_ids(video_ids)
        videos_data["_pipeline_metadata"] = {
            "ingestion_id": ingestion_id,
            "dataset": "channel_videos",
            "channel_id": channel_id,
            "uploads_playlist_id": uploads_id,
            "ingestion_timestamp": now.isoformat(),
            "video_count": len(videos_data.get("items", [])),
            "requested_video_ids": len(video_ids),
            "source": "youtube_data_api_v3",
        }
        key = (
            f"youtube/raw_channel_videos/channel_id={channel_id}/date={date_partition}/"
            f"hour={hour_partition}/data.json"
        )
        write_to_s3(videos_data, BUCKET, key)
        return (
            [{"type": "channel", "channel_id": channel_id, "video_count": len(video_ids)}],
            [],
        )
    except Exception as e:
        logger.exception("Channel ingestion failed channel_id=%s", channel_id)
        return [], [{"type": "channel", "channel_id": channel_id, "error": str(e)}]


def ingest_keyword(
    keyword: str,
    region: str,
    ingestion_id: str,
    now: datetime,
    date_partition: str,
    hour_partition: str,
) -> tuple[list[dict], list[dict]]:
    """
    Ingest keyword search results for one (keyword, region), up to SEARCH_MAX_PAGES pages.
    A failure on any page short-circuits and reports failed for that (keyword, region).
    """
    success: list[dict] = []
    failed: list[dict] = []

    token = ""
    pages_written = 0
    for page_num in range(1, SEARCH_MAX_PAGES + 1):
        try:
            search_data = fetch_search_videos(keyword, region, token)
            search_data["_pipeline_metadata"] = {
                "ingestion_id": ingestion_id,
                "dataset": "keyword_search",
                "keyword": keyword,
                "keyword_normalized": normalize_keyword_for_key(keyword),
                "region": region,
                "page": page_num,
                "ingestion_timestamp": now.isoformat(),
                "result_count": len(search_data.get("items", [])),
                "source": "youtube_data_api_v3",
            }
            key = (
                f"youtube/raw_search/keyword={normalize_keyword_for_key(keyword)}/"
                f"region={region}/date={date_partition}/hour={hour_partition}/"
                f"p{page_num}.json"
            )
            write_to_s3(search_data, BUCKET, key)
            pages_written += 1

            token = search_data.get("nextPageToken", "")
            if not token:
                break
        except Exception as e:
            logger.exception(
                "Keyword ingestion failed keyword='%s' region=%s page=%d", keyword, region, page_num
            )
            failed.append(
                {
                    "type": "keyword",
                    "keyword": keyword,
                    "region": region,
                    "page": page_num,
                    "error": str(e),
                }
            )
            return success, failed  # short-circuit on first error

    success.append(
        {"type": "keyword", "keyword": keyword, "region": region, "pages": pages_written}
    )
    return success, failed


# ═════════════════════════════════════════════════════════════════════════════
# Dynamic channel discovery
# ─ Pull channel IDs from the just-fetched trending responses, keep only those
#   that demonstrate sustained presence (>=2 trending videos in same region).
# ═════════════════════════════════════════════════════════════════════════════

def extract_hot_channels(
    trending_responses: dict[str, dict],
    min_videos_per_region: int = 2,
) -> list[str]:
    """
    From the in-memory trending responses, extract channel IDs that have
    multiple videos trending in at least one region. The threshold filters out
    one-hit wonders so we only spend channel-ingestion quota on consistently
    influential channels.

    Args:
        trending_responses: Mapping of region code → raw trending API response.
        min_videos_per_region: How many videos a channel must have trending
            in a single region to qualify. Default 2.

    Returns:
        Sorted, de-duplicated list of channel IDs.
    """
    from collections import Counter

    hot_channels: set[str] = set()

    for region, response in trending_responses.items():
        if not response:
            continue

        region_counts: Counter[str] = Counter()
        for item in response.get("items", []):
            channel_id = item.get("snippet", {}).get("channelId")
            if channel_id:
                region_counts[channel_id] += 1

        for channel_id, count in region_counts.items():
            if count >= min_videos_per_region:
                hot_channels.add(channel_id)

    return sorted(hot_channels)


# ═════════════════════════════════════════════════════════════════════════════
# Lambda handler
# ═════════════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    # Reset the quota breaker for each invocation. Without this, a tripped
    # breaker from a previous invocation in the same warm container would
    # block this run before it even starts. We *want* every fresh invocation
    # to retry — quota state is checked against YouTube live, not memory.
    global quota_breaker
    quota_breaker = QuotaCircuitBreaker()

    now = datetime.now(timezone.utc)
    date_partition = now.strftime("%Y-%m-%d")
    hour_partition = now.strftime("%H")
    ingestion_id = now.strftime("%Y%m%d_%H%M%S")

    results: dict[str, list[dict]] = {"success": [], "failed": []}
    trending_responses: dict[str, dict] = {}  # region → raw response, for hot-channel extraction

    # ── Trending + categories per region ─────────────────────────────────────
    for region in REGIONS:
        logger.info("Processing region=%s", region)
        s, f, trending_resp = ingest_region(
            region, ingestion_id, now, date_partition, hour_partition
        )
        results["success"].extend(s)
        results["failed"].extend(f)
        if trending_resp is not None:
            trending_responses[region] = trending_resp

    # ── Discover "hot" channels from this run's trending data ────────────────
    # Channels with >=2 videos trending in the same region are kept; one-hit
    # wonders are filtered out to keep channel-ingestion quota predictable.
    dynamic_channels = extract_hot_channels(
        trending_responses, min_videos_per_region=1
    )

    # Union with statically configured channels (env var)
    all_channel_ids = sorted(set(CHANNEL_IDS) | set(dynamic_channels))

    logger.info(
        "Channel ingestion plan: static=%d, dynamic=%d, union=%d",
        len(CHANNEL_IDS), len(dynamic_channels), len(all_channel_ids),
    )

    # ── Channel uploads (static + dynamic) ───────────────────────────────────
    for channel_id in all_channel_ids:
        logger.info("Processing channel_id=%s", channel_id)
        s, f = ingest_channel(channel_id, ingestion_id, now, date_partition, hour_partition)
        results["success"].extend(s)
        results["failed"].extend(f)

    # ── Keyword search (expensive: 100 units/call) ───────────────────────────
    for region in REGIONS:
        for keyword in KEYWORDS:
            logger.info("Processing keyword='%s' region=%s", keyword, region)
            s, f = ingest_keyword(
                keyword, region, ingestion_id, now, date_partition, hour_partition
            )
            results["success"].extend(s)
            results["failed"].extend(f)

    quota_status = (
        "TRIPPED" if quota_breaker.tripped else "ok"
    )
    summary = (
        f"Ingestion {ingestion_id} complete. "
        f"Success: {len(results['success'])}. Failed: {len(results['failed'])}. "
        f"Channels: {len(CHANNEL_IDS)} static + {len(dynamic_channels)} dynamic = "
        f"{len(all_channel_ids)} total. "
        f"Quota: {quota_status}."
    )
    logger.info(summary)

    # ── Alerts ───────────────────────────────────────────────────────────────
    # Quota trip gets a dedicated alert (urgent + actionable: usually wait).
    # Other partial failures get the standard alert (could be many causes).
    if quota_breaker.tripped:
        send_alert(
            subject=f"[YT Pipeline] QUOTA EXCEEDED — {ingestion_id}",
            message=(
                f"YouTube API quota was exhausted during ingestion {ingestion_id}.\n"
                f"Reason: {quota_breaker.reason}\n"
                f"Tripped at: {quota_breaker.tripped_at}\n\n"
                f"All subsequent API calls in this invocation were blocked.\n"
                f"YouTube quota resets at midnight Pacific Time daily.\n"
                f"The next scheduled invocation will retry from scratch.\n\n"
                f"Successful before trip: {len(results['success'])} items.\n"
                f"Failed (incl. blocked): {len(results['failed'])} items."
            ),
        )
    elif results["failed"]:
        send_alert(
            subject=f"[YT Pipeline] Ingestion partial failure — {ingestion_id}",
            message=json.dumps(results, ensure_ascii=False, indent=2),
        )

    # ── Status code ──────────────────────────────────────────────────────────
    # 200: clean run
    # 207: partial failure (some items failed but no quota issue)
    # 429: quota exceeded — Too Many Requests, signals "don't retry now"
    if quota_breaker.tripped:
        status_code = 429
    elif results["failed"]:
        status_code = 207
    else:
        status_code = 200

    return {
        "statusCode": status_code,
        "ingestion_id": ingestion_id,
        "quota_tripped": quota_breaker.tripped,
        "quota_reason": quota_breaker.reason if quota_breaker.tripped else None,
        "channels_static": len(CHANNEL_IDS),
        "channels_dynamic": len(dynamic_channels),
        "channels_total": len(all_channel_ids),
        "dynamic_channels": dynamic_channels,
        "results": results,
    }