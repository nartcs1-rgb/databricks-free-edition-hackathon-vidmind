# Databricks notebook source
pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client

# COMMAND ----------

from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
import json

# 1️⃣ Define OAuth scope
SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]

# 2️⃣ Load credentials (must be created earlier using OAuth flow)
# creds = Credentials.from_authorized_user_file("/Volumes/hackathon/input_data/input_volume/token/token2.json", SCOPES)

creds = Credentials.from_authorized_user_file("/Volumes/hackathon/input_data/input_volume/token/token4_all_access.json", SCOPES)


# 3️⃣ Initialize YouTube API client
youtube = build("youtube", "v3", credentials=creds)

# 4️⃣ Input: list of YouTube video IDs
video_ids = ['V7Gi26Bumoo', 'K0Dhy_1S-48', 'wjcLsJAZFM8', '-Mu3dQf_GqU', 'X1I8Vlw1qNc', 'AGNRtuWF80I', 'xTXcMK2Ktww', 't-42lC167k4', '5pysgDToGFA', 'Zw70GStboO8', 'gKzLzxUEjuY', 'iubjoX2s4lM', 'x0HO0aIyKEc', 'iDWVHdyoNuo', 'zMDLrq4Ymf0', '1E2MFv7CUOQ', 'DRIBTpjxP5M', 'XITlubJoR6M', 'TIJ5uwq4G-E', 'KJQl9Utr8f0', 'NnRYsyGHaAU', 'TdwRNeoSl5Q', 'vhcvcxwjgzg', 'hkmUDAsPrDE', 'qnPzvSzfiok', 'NgQP08BsxkI', 'FXwqc4zJBhw', 'XH6Dtdo_5wA', 'LR8Hmysqnbg', 'SIRf3kNytyg', 'FP_NGg6KtUs', 'H05jmJ5wRlo', 'QH-3skWNtQo', 'YvY7Twf0l2k', 'H1_vx-OApgY', 'uTuPI-6X_po', 'yiZ6hY2SM4M', 'hCGbFa4DIWs', 'doyHQWbEs3A', 'yPQzKt037lI', 'Nm3HTvy_s44', 'l-tdSGl6Wdw', 'Dr0g3GbqQ9Y', '6YLWbfVXYMI', 'N0IaHY5nsx0', 'yHjdxebqV3A', 'QSgVyDNqR_A']


# COMMAND ----------

import pandas as pd
from googleapiclient.discovery import build

def get_video_details(youtube, video_ids):
    """
    Fetch video metadata and statistics for a list of YouTube video IDs.
    
    Args:
        youtube: Authenticated YouTube API client (from googleapiclient.discovery.build).
        video_ids (list): List of YouTube video IDs.
    
    Returns:
        pandas.DataFrame: DataFrame with video id, title, description, tags, publishedAt, viewCount, likeCount, favoriteCount, commentCount, duration.
    """
    all_videos = []

    # Process in batches of 50 (API limit per request)
    for i in range(0, len(video_ids), 50):
        batch_ids = video_ids[i:i + 50]

        response = youtube.videos().list(
            part="snippet,statistics,contentDetails",
            id=",".join(batch_ids)
        ).execute()

        for item in response.get("items", []):
            snippet = item.get("snippet", {})
            stats = item.get("statistics", {})
            content = item.get("contentDetails", {})

            all_videos.append({
                "video_id": item["id"],
                "title": snippet.get("title"),
                "description": snippet.get("description"),
                "tags": snippet.get("tags", []),
                "publishedAt": snippet.get("publishedAt"),
                "viewCount": int(stats.get("viewCount", 0)),
                "likeCount": int(stats.get("likeCount", 0)),
                "favoriteCount": int(stats.get("favoriteCount", 0)),
                "commentCount": int(stats.get("commentCount", 0)),
                "duration": content.get("duration")
            })

    return pd.DataFrame(all_videos)


# ---------------- Example Usage ----------------
# assuming you already have an authenticated YouTube API object like:
# youtube = build("youtube", "v3", credentials=creds)

# video_ids = [
#     "dQw4w9WgXcQ",  # example video IDs
#     "M7FIvfx5J10",
#     "abc123XYZ"
# ]

df_videos = get_video_details(youtube, video_ids)
print(df_videos.head())


# COMMAND ----------

display(df_videos)

# COMMAND ----------

spark_df_videos = spark.createDataFrame(df_videos)
display(spark_df_videos)
spark_df_videos.write.mode("overwrite").saveAsTable("hackathon.youtube.videos_data")

# COMMAND ----------

import pandas as pd
import time

def get_video_comments(youtube, video_ids, max_comments_per_video=200):
    """
    Fetch comments for a list of YouTube videos.
    
    Args:
        youtube: Authenticated YouTube API client.
        video_ids (list): List of YouTube video IDs.
        max_comments_per_video (int): Limit to avoid excessive API usage (default=200).
    
    Returns:
        pandas.DataFrame: DataFrame with video_id, comment_id, author, text, likes, published_at, reply_count.
    """
    all_comments = []

    for video_id in video_ids:
        print(f"Fetching comments for video: {video_id}")
        next_page_token = None
        total_fetched = 0

        while True:
            try:
                response = youtube.commentThreads().list(
                    part="snippet,replies",
                    videoId=video_id,
                    maxResults=100,
                    pageToken=next_page_token,
                    textFormat="plainText"
                ).execute()

                for item in response.get("items", []):
                    top_comment = item["snippet"]["topLevelComment"]["snippet"]

                    all_comments.append({
                        "video_id": video_id,
                        "comment_id": item["id"],
                        "author": top_comment.get("authorDisplayName"),
                        "text": top_comment.get("textDisplay"),
                        "likes": int(top_comment.get("likeCount", 0)),
                        "published_at": top_comment.get("publishedAt"),
                        "reply_count": item["snippet"].get("totalReplyCount", 0)
                    })

                    # add replies if available
                    for reply in item.get("replies", {}).get("comments", []):
                        reply_snippet = reply["snippet"]
                        all_comments.append({
                            "video_id": video_id,
                            "comment_id": reply["id"],
                            "author": reply_snippet.get("authorDisplayName"),
                            "text": reply_snippet.get("textDisplay"),
                            "likes": int(reply_snippet.get("likeCount", 0)),
                            "published_at": reply_snippet.get("publishedAt"),
                            "reply_count": 0
                        })

                    total_fetched += 1
                    if total_fetched >= max_comments_per_video:
                        break

                if total_fetched >= max_comments_per_video:
                    break

                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    break

                time.sleep(0.2)  # polite pause to avoid quota bursts

            except Exception as e:
                print(f"Error fetching comments for {video_id}: {e}")
                break

    return pd.DataFrame(all_comments)


# ---------------- Example Usage ----------------
# assuming you already have an authenticated YouTube API object like:
# youtube = build("youtube", "v3", credentials=creds)

# video_ids = ["dQw4w9WgXcQ", "M7FIvfx5J10"]  # example video IDs

df_comments = get_video_comments(youtube, video_ids)
print(df_comments.head())


# COMMAND ----------

display(df_comments)

# COMMAND ----------

spark_df_comments = spark.createDataFrame(df_comments)
# display(spark_df_videos)
spark_df_comments.write.mode("overwrite").saveAsTable("hackathon.youtube.videos_comments")