{{
  config(
    materialized='incremental',
    table_type=var('youtube_trending_table_type', 'iceberg'),
    incremental_strategy='insert_overwrite',
    unique_key='trend_record_id',
    partitioned_by=['base_ym'],
    on_schema_change='sync_all_columns',
    tags=['gold', 'trend_mart', 'incremental']
  )
}}

with trending as (
    select
        cast(date_format(cast(t.collection_date as date), '%Y%m') as varchar) as base_ym,
        cast(t.collection_date as date) as collection_date,
        t.region_code,
        t.video_id,
        t.channel_id,
        t.category_id,
        t.title,
        t.view_count,
        t.like_count,
        t.comment_count,
        t.published_at,
        t.ingested_at
    from {{ source('yt_pipeline_silver_dev', 'silver_trending') }} t
    {% if is_incremental() %}
      where cast(date_format(cast(t.collection_date as date), '%Y%m') as varchar) >=
            (select coalesce(max(base_ym), '190001') from {{ this }})
    {% endif %}
),

deduped_trending as (
    select *
    from (
      select
          *,
          row_number() over (
            partition by collection_date, region_code, video_id
            order by ingested_at desc
          ) as rn
      from trending
    ) x
    where rn = 1
),

categories as (
    select
      category_id,
      category_title
    from {{ source('yt_pipeline_silver_dev', 'silver_categories') }}
),

channel_latest as (
    select
      channel_id,
      channel_title,
      subscriber_count,
      row_number() over (
        partition by channel_id
        order by ingested_at desc
      ) as rn
    from {{ source('yt_pipeline_silver_dev', 'silver_channel_videos') }}
),

search_signal as (
    select
      video_id,
      max(search_term) as last_search_term,
      max(cast(ingested_at as timestamp)) as last_search_at
    from {{ source('yt_pipeline_silver_dev', 'silver_search') }}
    group by 1
)

select
    md5(concat_ws('||', cast(d.collection_date as varchar), d.region_code, d.video_id)) as trend_record_id,
    d.base_ym,
    d.collection_date,
    d.region_code,
    d.video_id,
    d.channel_id,
    cl.channel_title,
    cl.subscriber_count,
    d.category_id,
    c.category_title,
    d.title as video_title,
    d.published_at,
    d.view_count,
    d.like_count,
    d.comment_count,
    coalesce(d.like_count * 1.0 / nullif(d.view_count, 0), 0.0) as like_rate,
    coalesce(d.comment_count * 1.0 / nullif(d.view_count, 0), 0.0) as comment_rate,
    ss.last_search_term,
    ss.last_search_at,
    d.ingested_at as silver_ingested_at,
    current_timestamp as mart_loaded_at
from deduped_trending d
left join categories c
  on d.category_id = c.category_id
left join channel_latest cl
  on d.channel_id = cl.channel_id and cl.rn = 1
left join search_signal ss
  on d.video_id = ss.video_id
