# Databricks notebook source
dbutils.widgets.text("video_id", "")
video_id = dbutils.widgets.get("video_id")

dbutils.widgets.text("email_id", "")
email_id = dbutils.widgets.get("email_id")

print(f"Video ID received: {video_id}")

# COMMAND ----------

video_metadata_path=f"/Volumes/hackathon/youtube/files/{video_id}/{video_id}_metadata.json"
video_file = f"/Volumes/hackathon/youtube/files/{video_id}/{video_id}.mp4"
thumbnail_path=f"/Volumes/hackathon/youtube/files/{video_id}/{video_id}_thumbnail_compressed.png"

# COMMAND ----------

pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client

# COMMAND ----------

from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials

SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]
creds = Credentials.from_authorized_user_file("/Volumes/hackathon/input_data/input_volume/token/token_channel_2_force_ssl_3.json", SCOPES)
youtube = build("youtube", "v3", credentials=creds)

# COMMAND ----------


with open(video_metadata_path, "r") as file:
    video_metadata = file.read()
import json
video_metadata = json.loads(video_metadata)
title=video_metadata.get("title")
description=video_metadata.get("description")
tags=video_metadata.get("tags").split(",")
print(title)
print(description)
print(tags)

# COMMAND ----------

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
body = {
    "snippet": {
        "title": title,
        "description": description,
        "tags": tags,
        "categoryId": "28"  # Category number, e.g., 22 for People & Blogs
    },
    "status": {
        "privacyStatus": "public"  # or "private", "unlisted"
    }
}

# Prepare media file with resumable upload support
media = MediaFileUpload(video_file, chunksize=-1, resumable=True)

# Create and execute request to upload video
request = youtube.videos().insert(
    part="snippet,status",
    body=body,
    media_body=media
)

response = None
while response is None:
    status, response = request.next_chunk()
    if status:
        print(f"Upload progress: {int(status.progress() * 100)}%")
print(f"Video uploaded. Video ID: {response['id']}")

# COMMAND ----------

youtube_video_id=response['id']
print(f"Video ID ={youtube_video_id}")

# COMMAND ----------

# # youtube_video_id='N02R7qumNOo'
# from googleapiclient.http import MediaFileUpload

# COMMAND ----------

youtube.thumbnails().set(
        videoId=youtube_video_id,
        media_body=MediaFileUpload(thumbnail_path)
    ).execute()

# COMMAND ----------

transcript_path = f"/Volumes/hackathon/youtube/files/{video_id}/{video_id}_transcript.txt"
with open(transcript_path, "r") as file:
    transcript_text = file.read()

# COMMAND ----------

import io
import csv
import pandas as pd

def parse_transcript(csv_text: str):
    """
    Parse CSV transcript text into structured rows.
    Each row has: start, end, text.
    Handles quoted fields and commas inside text.
    """
    f = io.StringIO(csv_text.strip())
    reader = csv.DictReader(f)
    rows = []
    for r in reader:
        text = r["transcript"].strip()
        if text.startswith('"') and text.endswith('"'):
            text = text[1:-1]
        rows.append({
            "start": r["start_time"].strip(),
            "end": r["end_time"].strip(),
            "text": text
        })
    return rows


def chunk_transcript_rows(rows, chunk_size=1200, overlap=200):
    """
    Combine transcript rows into chunks of approximately `chunk_size` characters.
    Keeps ~`overlap` characters overlap between chunks.
    """
    chunks = []
    current_text = ""
    current_start = None
    current_end = None
    current_rows = []

    for row in rows:
        piece = row["text"].strip()
        if not piece:
            continue

        # Start a new chunk if needed
        if current_start is None:
            current_start = row["start"]

        # If we can still fit this piece into the current chunk
        if len(current_text) + len(piece) + 1 <= chunk_size:
            current_text += (" " + piece) if current_text else piece
            current_end = row["end"]
            current_rows.append(row)
        else:
            # Save current chunk
            chunks.append({
                "text": current_text.strip(),
                "start": current_start,
                "end": current_end
            })

            # Prepare overlap
            overlap_text = current_text[-overlap:] if overlap < len(current_text) else current_text
            current_text = (overlap_text + " " + piece).strip()
            current_start = current_rows[-1]["start"] if current_rows else row["start"]
            current_end = row["end"]
            current_rows = [row]

    # Add final chunk
    if current_text:
        chunks.append({
            "text": current_text.strip(),
            "start": current_start,
            "end": current_end
        })

    return chunks


def create_transcript_chunks(csv_text, chunk_size=1200, overlap=200):
    """
    High-level function: parse + chunk transcript.
    Returns DataFrame with chunk_id, start, end, text, char_length.
    """
    rows = parse_transcript(csv_text)
    chunks = chunk_transcript_rows(rows, chunk_size, overlap)

    df = pd.DataFrame([
        {
            "chunk_id": i,
            "start": c["start"],
            "end": c["end"],
            "char_length": len(c["text"]),
            "text": c["text"]
        }
        for i, c in enumerate(chunks)
    ])
    return df

# COMMAND ----------

df_chunks = spark.createDataFrame(create_transcript_chunks(transcript_text))
display(df_chunks)

# COMMAND ----------

from pyspark.sql import Row

data = [Row(
    uid=video_id,
    youtube_video_id=youtube_video_id,
    title=title,
    description=description,
    tags=tags
)]

df_video = spark.createDataFrame(data)
# df.write.format("delta").mode("append").saveAsTable("hackathon.youtube.videos")
# display(df)

# COMMAND ----------

df_final=df_video.crossJoin(df_chunks)
display(df_final)
from pyspark.sql.functions import expr
df_final=df_final.withColumn("uid",expr("concat(uid,'_',chunk_id)"))\
    .withColumnRenamed("start","start_time")\
    .withColumnRenamed("end","end_time")\
    .withColumnRenamed("text","chunk_text")\
# df_final.write.format("delta").mode("append")

# COMMAND ----------

display(df_final)

# COMMAND ----------

df_final.write.format("delta").mode("append").saveAsTable("hackathon.youtube.videos_uploaded")

# COMMAND ----------

# spark.sql("ALTER TABLE hackathon.youtube.videos SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# COMMAND ----------

pip install dotenv

# COMMAND ----------

from dotenv import load_dotenv
import os
load_dotenv()

# COMMAND ----------

import os
gmail_app_password= os.getenv("gmail_app_password")
# print(gmail_app_password)

# COMMAND ----------

# youtube_video_id='HT2Idn4PeS0'

# COMMAND ----------

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Gmail account credentials
EMAIL = "databeli13@gmail.com"
APP_PASSWORD = gmail_app_password

# Email details
to_email = email_id
subject = "Video Uploaded to Youtube DatabricksFreeAccountHackathon Channel"
body = f"""Dear Sender,

Thank you for uploading a video to the DatabricksFreeAccountHackathon YouTube channel. 
Your video has been uploaded successfully at the following URL:

https://www.youtube.com/watch?v={youtube_video_id}

Please reach out to databeli13@gmail.com in case any suuport is required.

Thanks
Narender Kumar
"""

# Create message
msg = MIMEMultipart()
msg["From"] = EMAIL
msg["To"] = to_email
msg["Subject"] = subject
msg.attach(MIMEText(body, "plain"))

# Send email via Gmail SMTP
try:
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()  # Secure the connection
        server.login(EMAIL, APP_PASSWORD)
        server.send_message(msg)
    print("✅ Email sent successfully!")
except Exception as e:
    print("❌ Error:", e)


# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch
# MAGIC %restart_python

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
client = VectorSearchClient()
index = client.get_index(index_name="hackathon.youtube.videos_uploaded_index")
index.sync()