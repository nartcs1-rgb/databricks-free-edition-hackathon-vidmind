# Databricks notebook source
dbutils.widgets.text("video_id", "")
video_id = dbutils.widgets.get("video_id")
print(f"Video ID received: {video_id}")

# COMMAND ----------

pip install dotenv

# COMMAND ----------

from dotenv import load_dotenv
import os

load_dotenv()

# COMMAND ----------

video_metadata_path=f"/Volumes/hackathon/youtube/files/{video_id}/{video_id}_metadata.json"
with open(video_metadata_path, "r") as file:
    video_metadata = file.read()
import json
video_metadata = json.loads(video_metadata)
title=video_metadata.get("title")
description=video_metadata.get("description")
print(title)

# COMMAND ----------

prompt=f"""Create a YouTube thumbnail for a video with provided title and description.
It should look professional, eye-catching, and optimized for click-through.
Use bright colors, bold typography, and a tech/futuristic background
Make it in YouTube thumbnail dimensions.

Below are title and description:
titled= {title} 
description = {description}.
"""

# COMMAND ----------

image_output_path=f"/Volumes/hackathon/youtube/files/{video_id}/{video_id}_thumbnail.png"

# COMMAND ----------

pip install openai

# COMMAND ----------

from openai import OpenAI
import base64

client = OpenAI()

result = client.images.generate(
    model="gpt-image-1",       # or "dall-e-3"
    prompt=f"Create a YouTube thumbnail for the video titled {title}",
    size="1536x1024"          # perfect YouTube thumbnail size
)

image_base64 = result.data[0].b64_json
image_bytes = base64.b64decode(image_base64)

with open(image_output_path, "wb") as f:
    f.write(image_bytes)

print("✅ Thumbnail saved as youtube_thumbnail.png")


# COMMAND ----------

pip install pillow

# COMMAND ----------

compressed_image_path=f"/Volumes/hackathon/youtube/files/{video_id}/{video_id}_thumbnail_compressed.png"

# COMMAND ----------

from PIL import Image
import os

def resize_and_compress(input_path, output_path,max_size_mb=2):
    img = Image.open(input_path)
    img = img.resize((1280, 720))
    img.save(output_path, optimize=True, quality=85)


# COMMAND ----------

resize_and_compress(image_output_path, compressed_image_path)