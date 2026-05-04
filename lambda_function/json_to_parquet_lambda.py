import json
import logging
import os
import traceback
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional

import awswrangler as wr
import boto3
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")
sns_client = boto3.client("sns")
boto3_session = boto3.Session()

SNS_TOPIC = os.environ.get("SNS_ALERT_TOPIC_ARN", "")
BRONZE_BUCKET = os.environ["S3_BUCKET_BRONZE"]
SILVER_BUCKET = os.environ["S3_BUCKET_SILVER"]
GLUE_DB_SILVER = os.environ["GLUE_DB_SILVER"]
GOLD_BUCKET = os.environ.get("S3_BUCKET_GOLD", "")
GLUE_DB_GOLD = os.environ.get("GLUE_DB_GOLD", "")
ENABLE_SILVER = os.environ.get("ENABLE_SILVER", "true").lower() == "true"
ENABLE_GOLD = os.environ.get("ENABLE_GOLD", "false").lower() == "true"


@dataclass
class DatasetConfig:
    name: str
    bronze_prefix: str
    silver_subpath: str
    silver_table: str
    parser_fn: Callable[[dict], list[dict]]
    silver_transform_fn: Callable[[pd.DataFrame], pd.DataFrame]
    partition_cols: list[str] = field(default_factory=list)


@dataclass
class LayerConfig:
    name: str
    enabled: bool


def send_sns_alert(subject: str, message: str) -> None:
    if not SNS_TOPIC:
        logger.info("SNS_ALERT_TOPIC_ARN not set; skipping alert")
        return
    max_message_bytes = 250_000
    encoded = message.encode("utf-8")
    if len(encoded) > max_message_bytes:
        truncated = encoded[:max_message_bytes].decode("utf-8", errors="ignore")
        message = truncated + "\n\n... [TRUNCATED — see CloudWatch Logs for full details]"
    try:
        sns_client.publish(TopicArn=SNS_TOPIC, Subject=subject[:100], Message=message)
    except Exception as e:
        logger.exception("Failed to send SNS alert (continuing): %s", e)


def scan_json_files(bucket: str, prefix: str) -> list[str]:
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(".json"):
                keys.append(key)
    return keys


def read_json_object(bucket: str, key: str) -> dict:
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read())


def extract_partitions_from_key(key: str) -> dict[str, str]:
    parts = {}
    for segment in key.split("/"):
        if "=" in segment:
            k, v = segment.split("=", 1)
            parts[k] = v
    return parts


def parse_categories(raw: dict) -> list[dict]:
    rows = []
    for item in raw.get("items", []):
        snippet = item.get("snippet", {})
        rows.append(
            {
                "category_id": item.get("id", ""),
                "category_title": snippet.get("title", ""),
                "assignable": snippet.get("assignable"),
                "channel_id": snippet.get("channelId", ""),
            }
        )
    return rows


def _parse_video_item(item: dict) -> dict:
    snippet = item.get("snippet", {})
    stats = item.get("statistics", {})
    content = item.get("contentDetails", {})
    return {
        "video_id": item.get("id", ""),
        "title": snippet.get("title", ""),
        "description": snippet.get("description", ""),
        "channel_id": snippet.get("channelId", ""),
        "channel_title": snippet.get("channelTitle", ""),
        "category_id": snippet.get("categoryId", ""),
        "published_at": snippet.get("publishedAt", ""),
        "tags": snippet.get("tags", []),
        "default_language": snippet.get("defaultLanguage", ""),
        "default_audio_language": snippet.get("defaultAudioLanguage", ""),
        "view_count": stats.get("viewCount"),
        "like_count": stats.get("likeCount"),
        "comment_count": stats.get("commentCount"),
        "duration": content.get("duration", ""),
        "definition": content.get("definition", ""),
        "caption": content.get("caption", ""),
        "licensed_content": content.get("licensedContent"),
    }


def parse_trending(raw: dict) -> list[dict]:
    return [_parse_video_item(item) for item in raw.get("items", [])]


def parse_channel_videos(raw: dict) -> list[dict]:
    return [_parse_video_item(item) for item in raw.get("items", [])]


def parse_search(raw: dict) -> list[dict]:
    rows = []
    for item in raw.get("items", []):
        snippet = item.get("snippet", {})
        item_id = item.get("id", {})
        rows.append(
            {
                "video_id": item_id.get("videoId", "") if isinstance(item_id, dict) else "",
                "title": snippet.get("title", ""),
                "description": snippet.get("description", ""),
                "channel_id": snippet.get("channelId", ""),
                "channel_title": snippet.get("channelTitle", ""),
                "published_at": snippet.get("publishedAt", ""),
                "live_broadcast_content": snippet.get("liveBroadcastContent", ""),
            }
        )
    return rows


def _strip_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].map(lambda v: v.strip() if isinstance(v, str) else v)
    return df


def transform_categories(df: pd.DataFrame) -> pd.DataFrame:
    df = _strip_string_columns(df)
    if "category_id" in df.columns:
        df["category_id"] = pd.to_numeric(df["category_id"], errors="coerce").astype("Int64")
    if "assignable" in df.columns:
        df["assignable"] = df["assignable"].astype("boolean")
    return df


def transform_video(df: pd.DataFrame) -> pd.DataFrame:
    df = _strip_string_columns(df)
    for numeric_col in ("view_count", "like_count", "comment_count"):
        if numeric_col in df.columns:
            df[numeric_col] = pd.to_numeric(df[numeric_col], errors="coerce").astype("Int64")
    if "category_id" in df.columns:
        df["category_id"] = pd.to_numeric(df["category_id"], errors="coerce").astype("Int64")
    if "published_at" in df.columns:
        df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce", utc=True)
    if "licensed_content" in df.columns:
        df["licensed_content"] = df["licensed_content"].astype("boolean")
    return df


def transform_search(df: pd.DataFrame) -> pd.DataFrame:
    df = _strip_string_columns(df)
    if "published_at" in df.columns:
        df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce", utc=True)
    return df


DATASETS = [
    DatasetConfig("categories", "youtube/raw_statistics_reference_data/", "youtube/categories/", "silver_categories", parse_categories, transform_categories, ["region", "date"]),
    DatasetConfig("trending", "youtube/raw_statistics/", "youtube/trending/", "silver_trending", parse_trending, transform_video, ["region", "date", "hour"]),
    DatasetConfig("channel_videos", "youtube/raw_channel_videos/", "youtube/channel_videos/", "silver_channel_videos", parse_channel_videos, transform_video, ["channel_id", "date", "hour"]),
    DatasetConfig("search", "youtube/raw_search/", "youtube/search/", "silver_search", parse_search, transform_search, ["keyword", "region", "date", "hour"]),
]


def bronze_json_to_dataframe(bucket: str, key: str, dataset: DatasetConfig) -> Optional[pd.DataFrame]:
    raw = read_json_object(bucket, key)
    raw.pop("_pipeline_metadata", None)
    rows = dataset.parser_fn(raw)
    if not rows:
        return None
    df = pd.DataFrame(rows)
    parts = extract_partitions_from_key(key)
    for col, val in parts.items():
        df[col] = val
    missing = [c for c in dataset.partition_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing partition columns {missing} for dataset={dataset.name} key={key}")
    return dataset.silver_transform_fn(df)


def write_parquet(df: pd.DataFrame, table_root_uri: str, glue_db: str, glue_table: str, partition_cols: list[str]) -> None:
    wr.s3.to_parquet(
        df=df,
        path=table_root_uri,
        dataset=True,
        database=glue_db,
        table=glue_table,
        partition_cols=partition_cols,
        mode="overwrite_partitions",
        boto3_session=boto3_session,
    )


def _group_keys_by_partition(src_keys: list[str], partition_cols: list[str]) -> dict[tuple, list[str]]:
    groups = defaultdict(list)
    for key in src_keys:
        parts = extract_partitions_from_key(key)
        partition_key = tuple(parts.get(col, "") for col in partition_cols)
        groups[partition_key].append(key)
    return groups


def run_silver_dataset(dataset: DatasetConfig) -> dict:
    silver_path = f"s3://{SILVER_BUCKET}/{dataset.silver_subpath}"
    try:
        src_keys = scan_json_files(BRONZE_BUCKET, dataset.bronze_prefix)
    except Exception:
        msg = f"[Silver/{dataset.name}] scan failed:\n{traceback.format_exc()}"
        send_sns_alert(f"[YT Pipeline] Silver/{dataset.name} scan failed", msg)
        return {"dataset": dataset.name, "status": "scan_error", "results": []}
    if not src_keys:
        return {"dataset": dataset.name, "status": "no_files", "results": []}

    partition_groups = _group_keys_by_partition(src_keys, dataset.partition_cols)
    results = []
    errors = []
    for partition_key, keys_in_partition in partition_groups.items():
        dfs = []
        for src_key in keys_in_partition:
            try:
                df = bronze_json_to_dataframe(BRONZE_BUCKET, src_key, dataset)
                if df is None:
                    results.append({"status": "empty", "src_key": src_key})
                    continue
                dfs.append(df)
            except Exception as exc:
                errors.append({"src_key": src_key, "error": str(exc), "traceback": traceback.format_exc()})
                results.append({"status": "error", "src_key": src_key, "error": str(exc)})

        if not dfs:
            continue

        try:
            combined = pd.concat(dfs, ignore_index=True)
            write_parquet(combined, silver_path, GLUE_DB_SILVER, dataset.silver_table, dataset.partition_cols)
            for src_key in keys_in_partition:
                if not any(r["src_key"] == src_key for r in results):
                    results.append({"status": "converted", "src_key": src_key, "partition": str(partition_key)})
        except Exception as exc:
            errors.append({"src_key": f"<partition write: {partition_key}>", "error": str(exc), "traceback": traceback.format_exc()})
            for src_key in keys_in_partition:
                results.append({"status": "error", "src_key": src_key, "error": f"partition write: {exc}"})

    if errors:
        send_sns_alert(
            f"[YT Pipeline][Silver/{dataset.name}] {len(errors)} file(s) failed",
            "\n\n".join(e["traceback"] for e in errors[:5]),
        )

    return {
        "dataset": dataset.name,
        "status": "error" if errors else "ok",
        "converted": sum(1 for r in results if r["status"] == "converted"),
        "empty": sum(1 for r in results if r["status"] == "empty"),
        "failed": len(errors),
        "results": results,
    }


def run_silver_layer() -> list[dict]:
    if not ENABLE_SILVER:
        return [{"layer": "Silver", "status": "disabled"}]
    return [run_silver_dataset(ds) for ds in DATASETS]


def run_gold_layer() -> list[dict]:
    if not ENABLE_GOLD:
        return [{"layer": "Gold", "status": "disabled"}]
    if not GOLD_BUCKET or not GLUE_DB_GOLD:
        return [{"layer": "Gold", "status": "misconfigured"}]
    raise NotImplementedError("Gold layer transforms are not implemented yet")


def lambda_handler(event, context):
    silver_results = run_silver_layer()
    gold_results = run_gold_layer()
    all_results = {"silver": silver_results, "gold": gold_results}
    has_error = any(r.get("status") == "error" for layer_results in all_results.values() for r in layer_results)
    return {
        "statusCode": 207 if has_error else 200,
        "body": json.dumps({"has_error": has_error, "results": all_results}, ensure_ascii=False, default=str),
    }
