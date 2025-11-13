# Databricks notebook source
dbutils.widgets.text("video_id", "")
video_id = dbutils.widgets.get("video_id")
print(f"Video ID received: {video_id}")

# COMMAND ----------

# MAGIC %pip install moviepy

# COMMAND ----------

mp4_path = f"/Volumes/hackathon/youtube/files/{video_id}/{video_id}.mp4"
wav_path = f"/Volumes/hackathon/youtube/files/{video_id}/{video_id}.wav"

# COMMAND ----------

from moviepy import VideoFileClip
video = VideoFileClip(mp4_path)
video.audio.write_audiofile(wav_path)