"""Bronze ingestion scaffold for Reddit API."""

import os
from datetime import datetime, timezone

from src.api_clients.reddit_client import RedditApiClient
from src.utils.bronze_writer import write_bronze_payloads, write_bronze_structured_records
from src.utils.idempotency import compact_ids_with_retirement, load_state, save_state
from src.utils.io_paths import build_raw_path, build_retired_base_path, build_state_path
from src.utils.quality import record_ingestion_event


def run(source_name: str = "reddit") -> dict:
    """Ingest subreddit posts/comments and land immutable raw JSON into Bronze."""
    run_ts = datetime.now(timezone.utc)
    target_path = build_raw_path(source_name=source_name, run_ts=run_ts)
    scope = os.getenv("SECRETS_SCOPE", "kv_databricks_scope")
    subreddit = os.getenv("REDDIT_SUBREDDIT", "leagueoflegends")
    dbutils_ref = globals().get("dbutils")
    spark_ref = globals().get("spark")
    state_path = build_state_path(source_name=source_name)
    retired_base_path = build_retired_base_path(source_name=source_name)
    state = load_state(state_path)
    seen_post_ids = set(state.get("seen_post_ids", []))
    seen_comment_ids = set(state.get("seen_comment_ids", []))

    client = RedditApiClient.from_secrets(secret_scope=scope, dbutils=dbutils_ref)
    hot_posts = client.get_hot_posts(subreddit=subreddit, limit=25)
    patch_posts = client.search_patch_posts(subreddit=subreddit, query="patch", limit=25)

    posts_by_id: dict[str, dict] = {}
    for post in hot_posts + patch_posts:
        post_id = post.get("id")
        if post_id:
            posts_by_id[post_id] = post
    merged_posts = [post for post in posts_by_id.values() if post.get("id") not in seen_post_ids]

    post_ids = {post.get("id") for post in merged_posts if post.get("id")}
    comments: list[dict] = []
    for post_id in post_ids:
        comments.extend(client.get_comments(subreddit=subreddit, post_id=post_id, limit=50))

    new_comments = [comment for comment in comments if comment.get("id") not in seen_comment_ids]

    posts_write = write_bronze_payloads(
        source_name=source_name,
        entity_name="bronze_reddit_posts_raw",
        records=merged_posts,
        base_target_path=target_path,
        run_ts=run_ts,
        spark=spark_ref,
        prefer_parquet=True,
    )
    comments_write = write_bronze_payloads(
        source_name=source_name,
        entity_name="bronze_reddit_comments_raw",
        records=new_comments,
        base_target_path=target_path,
        run_ts=run_ts,
        spark=spark_ref,
        prefer_parquet=True,
    )

    updated_post_ids, retired_post_ids, new_retired_post_ids = compact_ids_with_retirement(
        list(seen_post_ids) + [post.get("id") for post in merged_posts if post.get("id")],
        keep_last=5000,
        retired_records=state.get("retired_post_ids", []),
        retired_date=run_ts.date().isoformat(),
        primary_key_prefix="reddit_post_retired",
    )
    updated_comment_ids, retired_comment_ids, new_retired_comment_ids = compact_ids_with_retirement(
        list(seen_comment_ids) + [comment.get("id") for comment in new_comments if comment.get("id")],
        keep_last=20000,
        retired_records=state.get("retired_comment_ids", []),
        retired_date=run_ts.date().isoformat(),
        primary_key_prefix="reddit_comment_retired",
    )
    save_state(
        state_path,
        {
            "seen_post_ids": updated_post_ids,
            "seen_comment_ids": updated_comment_ids,
            "retired_post_ids": retired_post_ids,
            "retired_comment_ids": retired_comment_ids,
            "last_run_ts": run_ts.isoformat(),
        },
    )

    retired_posts_write = write_bronze_structured_records(
        source_name=source_name,
        entity_name="bronze_reddit_post_ids_retired",
        records=new_retired_post_ids,
        base_target_path=retired_base_path,
        run_ts=run_ts,
        spark=spark_ref,
        prefer_parquet=True,
    )
    retired_comments_write = write_bronze_structured_records(
        source_name=source_name,
        entity_name="bronze_reddit_comment_ids_retired",
        records=new_retired_comment_ids,
        base_target_path=retired_base_path,
        run_ts=run_ts,
        spark=spark_ref,
        prefer_parquet=True,
    )

    record_ingestion_event(
        source_name=source_name,
        target_path=target_path,
        run_ts=run_ts,
        record_count=len(merged_posts) + len(new_comments),
        metadata={
            "hot_posts": len(hot_posts),
            "patch_posts": len(patch_posts),
            "merged_posts": len(merged_posts),
            "comments": len(new_comments),
            "retired_post_ids": len(retired_post_ids),
            "retired_comment_ids": len(retired_comment_ids),
            "new_retired_post_ids": len(new_retired_post_ids),
            "new_retired_comment_ids": len(new_retired_comment_ids),
            "landing_formats": {
                "bronze_reddit_posts_raw": posts_write["format"],
                "bronze_reddit_comments_raw": comments_write["format"],
                "bronze_reddit_post_ids_retired": retired_posts_write["format"],
                "bronze_reddit_comment_ids_retired": retired_comments_write["format"],
            },
        },
    )

    return {
        "source": source_name,
        "target_path": target_path,
        "run_ts": run_ts.isoformat(),
        "hot_posts": len(hot_posts),
        "patch_posts": len(patch_posts),
        "merged_posts": len(merged_posts),
        "comments": len(new_comments),
        "retired_post_ids": len(retired_post_ids),
        "retired_comment_ids": len(retired_comment_ids),
        "new_retired_post_ids": len(new_retired_post_ids),
        "new_retired_comment_ids": len(new_retired_comment_ids),
        "landing": {
            "bronze_reddit_posts_raw": posts_write,
            "bronze_reddit_comments_raw": comments_write,
            "bronze_reddit_post_ids_retired": retired_posts_write,
            "bronze_reddit_comment_ids_retired": retired_comments_write,
        },
    }


if __name__ == "__main__":
    print(run())
