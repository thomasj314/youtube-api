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


def youtube_get(path: str, params: dict) -> dict:
    """
    Call a YouTube Data API endpoint with simple exponential backoff.
    Retries on URLError and 5xx HTTPError; surfaces 4xx immediately
    (e.g. quotaExceeded → fail fast so we don't burn more quota).
    """
    params = {**params, "key": API_KEY}
    url = f"{API_BASE}/{path}?{urlencode(params)}"
    req = Request(url, headers={"Accept": "application/json"})

    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except HTTPError as e:
            if 400 <= e.code < 500:
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

    if last_exc is None:
        raise RuntimeError("YouTube API call failed without exception details")
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

    all_items = []
    for i in range(0, len(video_ids), YOUTUBE_API_MAX_RESULTS):
        chunk = video_ids[i:i + YOUTUBE_API_MAX_RESULTS]
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

    max_message_bytes = 250_000
    encoded = message.encode("utf-8")
    if len(encoded) > max_message_bytes:
        truncated = encoded[:max_message_bytes].decode("utf-8", errors="ignore")
        message = truncated + "\n\n... [TRUNCATED — see CloudWatch Logs for full details]"

    try:
        sns_client.publish(
            TopicArn=SNS_TOPIC,
            Subject=subject[:100],
            Message=message,
        )
        logger.info("SNS alert sent: %s", subject)
    except Exception as e:
        logger.exception("Failed to send SNS alert (continuing): %s", e)


def normalize_keyword_for_key(keyword: str) -> str:
    return "_".join(keyword.lower().split())[:120]


def ingest_region(region: str, ingestion_id: str, now: datetime, date_partition: str, hour_partition: str):
    success = []
    failed = []

    try:
        trending_data = fetch_trending_videos(region)
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

    return success, failed


def ingest_channel(channel_id: str, ingestion_id: str, now: datetime, date_partition: str, hour_partition: str):
    try:
        uploads_id = fetch_uploads_playlist_id(channel_id)

        video_ids = []
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
        return ([{"type": "channel", "channel_id": channel_id, "video_count": len(video_ids)}], [])
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
):
    success = []
    failed = []

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
            return success, failed

    success.append({"type": "keyword", "keyword": keyword, "region": region, "pages": pages_written})
    return success, failed


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    date_partition = now.strftime("%Y-%m-%d")
    hour_partition = now.strftime("%H")
    ingestion_id = now.strftime("%Y%m%d_%H%M%S")

    results = {"success": [], "failed": []}

    for region in REGIONS:
        logger.info("Processing region=%s", region)
        s, f = ingest_region(region, ingestion_id, now, date_partition, hour_partition)
        results["success"].extend(s)
        results["failed"].extend(f)

    for channel_id in CHANNEL_IDS:
        logger.info("Processing channel_id=%s", channel_id)
        s, f = ingest_channel(channel_id, ingestion_id, now, date_partition, hour_partition)
        results["success"].extend(s)
        results["failed"].extend(f)

    for region in REGIONS:
        for keyword in KEYWORDS:
            logger.info("Processing keyword='%s' region=%s", keyword, region)
            s, f = ingest_keyword(keyword, region, ingestion_id, now, date_partition, hour_partition)
            results["success"].extend(s)
            results["failed"].extend(f)

    summary = (
        f"Ingestion {ingestion_id} complete. "
        f"Success: {len(results['success'])}. Failed: {len(results['failed'])}."
    )
    logger.info(summary)

    if results["failed"]:
        send_alert(
            subject=f"[YT Pipeline] Ingestion partial failure — {ingestion_id}",
            message=json.dumps(results, ensure_ascii=False, indent=2),
        )

    return {
        "statusCode": 200 if not results["failed"] else 207,
        "ingestion_id": ingestion_id,
        "results": results,
    }
