# Databricks notebook source
dbutils.widgets.text("video_id", "")
video_id = dbutils.widgets.get("video_id")
print(f"Video ID received: {video_id}")

# COMMAND ----------

wav_path = f"/Volumes/hackathon/youtube/files/{video_id}/{video_id}.wav"

# COMMAND ----------

# Install ffmpeg binary via pip (user-level)
%pip install git+https://github.com/openai/whisper.git
%pip install torch imageio[ffmpeg]

# COMMAND ----------

import os, imageio_ffmpeg, whisper

# get actual ffmpeg binary path
ffmpeg_path = imageio_ffmpeg.get_ffmpeg_exe()
print("FFmpeg binary found at:", ffmpeg_path)

# Force Whisper to use this exact binary
os.environ["FFMPEG_BINARY"] = ffmpeg_path
os.environ["PATH"] += os.pathsep + os.path.dirname(ffmpeg_path)

# Optional: verify within Python (not shell)
from subprocess import run, PIPE
print(run([ffmpeg_path, "-version"], stdout=PIPE, text=True).stdout.split("\n")[0])

# COMMAND ----------

import os
import whisper
import imageio_ffmpeg
import subprocess
from pathlib import Path


# Get ffmpeg binary path from imageio
ffmpeg_path = imageio_ffmpeg.get_ffmpeg_exe()
print("✅ Using ffmpeg binary:", ffmpeg_path)

# Monkey-patch Whisper’s internal audio load to call full ffmpeg path
import whisper.audio

def custom_load_audio(file: str, sr: int = 16000):
    """
    Replacement for whisper.audio.load_audio that uses explicit ffmpeg path.
    """
    cmd = [
        ffmpeg_path,
        "-nostdin",
        "-threads", "0",
        "-i", file,
        "-f", "s16le",
        "-ac", "1",
        "-acodec", "pcm_s16le",
        "-ar", str(sr),
        "-"
    ]
    out = subprocess.run(cmd, capture_output=True, check=True)
    import numpy as np
    import io
    return np.frombuffer(out.stdout, np.int16).astype(np.float32) / 32768.0

# Replace the default Whisper load_audio with ours
whisper.audio.load_audio = custom_load_audio

# Now load model and transcribe
model = whisper.load_model("small")
result = model.transcribe(wav_path, fp16=False)

print("\n📝 Transcript:\n", result["text"])


# COMMAND ----------

import re
import pandas as pd

# result = model.transcribe(wav_path, fp16=False)
segments = result["segments"]

sentence_level = []
current_text = []
current_start = None
current_end = None

# sentence enders
ENDERS = re.compile(r'[.?!]\s*$')

for seg in segments:
    seg_text = seg["text"].strip()
    seg_start = seg["start"]
    seg_end = seg["end"]

    if current_start is None:
        current_start = seg_start

    current_text.append(seg_text)
    current_end = seg_end

    # if this segment looks like it ends a sentence, flush
    if ENDERS.search(seg_text):
        sentence_level.append({
            "start": int(current_start),
            "end": int(current_end),
            "text": " ".join(current_text).strip()
        })
        # reset
        current_text = []
        current_start = None
        current_end = None

# leftover (last sentence without punctuation)
if current_text:
    sentence_level.append({
        "start": int(current_start),
        "end": int(current_end),
        "text": " ".join(current_text).strip()
    })

# df_sentences = pd.DataFrame(sentence_level)
# display(df_sentences)
csv_string = pd.DataFrame(sentence_level).rename(columns={
    "start": "start_seconds",
    "end": "end_seconds",
    "text": "transcript"
}).to_csv(index=False)
print(csv_string)


# COMMAND ----------

df = pd.DataFrame(sentence_level)
df["start_minutes"] = df["start"].apply(lambda s: f"{s//60}:{s%60:02d}")
df["end_minutes"] = df["end"].apply(lambda s: f"{s//60}:{s%60:02d}")
csv_string = df.rename(columns={
    "start_minutes": "start_time",
    "end_minutes": "end_time",
    "text": "transcript"
})[["start_time", "end_time", "transcript"]].to_csv(index=False)

# COMMAND ----------

print(csv_string)

# COMMAND ----------

with open(wav_path.replace(".wav", "_transcript.txt"), "w") as f:
    f.write(csv_string)