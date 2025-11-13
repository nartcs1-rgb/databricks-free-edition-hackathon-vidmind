# Databricks notebook source
dbutils.widgets.text("video_id", "")
video_id = dbutils.widgets.get("video_id")
print(f"Video ID received: {video_id}")

# COMMAND ----------

transcript_path = f"/Volumes/hackathon/youtube/files/{video_id}/{video_id}_transcript.txt"
output_metadata_path = f"/Volumes/hackathon/youtube/files/{video_id}/{video_id}_metadata.json"

# COMMAND ----------

with open(transcript_path, "r") as file:
    transcript_text = file.read()

# COMMAND ----------

print(transcript_text)

# COMMAND ----------

pip install openai

# COMMAND ----------

from openai import OpenAI
import os

# How to get your Databricks token: https://docs.databricks.com/en/dev-tools/auth/pat.html
# DATABRICKS_TOKEN = os.environ.get('DATABRICKS_TOKEN')
# Alternatively in a Databricks notebook you can use this:
DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url="https://dbc-3f1c04d0-60d3.cloud.databricks.com/serving-endpoints"
)

response = client.chat.completions.create(
    model="databricks-gpt-oss-120b",
    messages=[
        {
            "role": "system",
            "content": """You are an assistant to generate Description for a youtube video based on provided transcript of the video.
            The provided transcript will have start time in minutes:seconds, end time in minutes:seconds and the transcript text as comma sepereated values.

            Generated description should be simple text without any markdown and should have the following sections:
            1. Introduction -> Provide a brief introduction to the topic
            2. Chapters -> Devide the entire video into logical sections/chapters.
            Any chapter shall not be less than 1 minute. There shall not be more than 5 chapters.
            For each section/chapter,provide a list of chapters in following format:

            00:00 Name of Chapter 1
            03:04 Name of Chapter 2
            05:00 Name of Chapter 3
            07:00 Name of Chapter 4
            10:00 Name of Chapter 5

            3. Put a message to like,comment and subscribe to the channel
            4. Put 4 tags with #tag1 #tag2 #tga3 #tag4

            Output shall not contain section headings like Introduction, Chapters, Conclusion, etc.
            """
        },
        {
            "role": "user",
            "content": transcript_text
        }
    ],
    max_tokens=5000
)

print(response.choices[0].message.content)

# COMMAND ----------

description=''
for answer in response.choices[0].message.content:
    if answer.get("type")=="text":
        description=answer.get("text")
        print(answer.get("text"))

# COMMAND ----------

from openai import OpenAI
import os

# How to get your Databricks token: https://docs.databricks.com/en/dev-tools/auth/pat.html
# DATABRICKS_TOKEN = os.environ.get('DATABRICKS_TOKEN')
# Alternatively in a Databricks notebook you can use this:
DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url="https://dbc-3f1c04d0-60d3.cloud.databricks.com/serving-endpoints"
)

response = client.chat.completions.create(
    model="databricks-gpt-oss-120b",
    messages=[
        {
            "role": "system",
            "content": """You are an assistant to generate Title for a youtube video based on provided description of the video.
            The provided description will have Introduction, Chapters, Conclusion and other sections.
            Generated title should be simple text without any markdown.
            The title should be short and catchy and should be related to the description.
            The title should not contain any hashtags or mentions.
            The title should not contain any section headings like Introduction, Chapters, Conclusion, etc.
            """
        },
        {
            "role": "user",
            "content": description
        }
    ],
    max_tokens=5000
)

print(response.choices[0].message.content)

# COMMAND ----------

title=''
for answer in response.choices[0].message.content:
    if answer.get("type")=="text":
        title=answer.get("text")
        print(answer.get("text"))

# COMMAND ----------

from openai import OpenAI
import os

# How to get your Databricks token: https://docs.databricks.com/en/dev-tools/auth/pat.html
# DATABRICKS_TOKEN = os.environ.get('DATABRICKS_TOKEN')
# Alternatively in a Databricks notebook you can use this:
DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

client = OpenAI(
    api_key=DATABRICKS_TOKEN,
    base_url="https://dbc-3f1c04d0-60d3.cloud.databricks.com/serving-endpoints"
)

response = client.chat.completions.create(
    model="databricks-gpt-oss-120b",
    messages=[
        {
            "role": "system",
            "content": """You are an assistant to generate SEO tags for a youtube video.
            You will be provided a description of the video with sections like Introduction, Chapters etc.
            Based on thsi desciption generate the seo tags a simple comma seperated string without any markdown.
            The seo tags should be related to the description and should be short and catchy.
            The seo tags should not contain any hashtags or mentions.
            The total lenght of all tags shall not exceed 450 characters
            """
        },
        {
            "role": "user",
            "content": description
        }
    ],
    max_tokens=5000
)

print(response.choices[0].message.content)

# COMMAND ----------

tags=''
for answer in response.choices[0].message.content:
    if answer.get("type")=="text":
        tags=answer.get("text")
        print(answer.get("text"))

# COMMAND ----------

video_metadata = {
    "title": title,
    "description": description, "tags": tags
}


# COMMAND ----------

import json
with open(output_metadata_path, "w") as f:
    json.dump(video_metadata, f)