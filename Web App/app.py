import streamlit as st
import os
import uuid
import requests
from openai import OpenAI
from databricks.vector_search.client import VectorSearchClient
from dotenv import load_dotenv
load_dotenv()

# ---------------- CONFIG ----------------
# VOLUME_PATH = "/Volumes/hackathon/input_data/input_volume/input_video/"
VOLUME_PATH = os.getenv("VOLUME_PATH")
# VOLUME_PATH = "/dbfs"+VOLUME_PATH
DATABRICKS_HOST = "https://dbc-3f1c04d0-60d3.cloud.databricks.com"
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")
JOB_ID = 924159400275240  # Hardcoded Job ID
# ---------------------------------------

def extract_results(results):
    columns = [col["name"] for col in results["manifest"]["columns"]]
    data = results["result"]["data_array"]
    parsed = [dict(zip(columns, row)) for row in data]
    return [
        {
            "video_id": r["youtube_video_id"],
            "start_time": r["start_time"],
            "text": r["chunk_text"]
        }
        for r in parsed
    ]

def youtube_url(video_id: str, start_time: str) -> str:
    parts = [int(p) for p in start_time.split(":")]
    if len(parts) == 3:
        hours, minutes, seconds = parts
    elif len(parts) == 2:
        hours, minutes, seconds = 0, parts[0], parts[1]
    else:
        hours, minutes, seconds = 0, 0, parts[0]
    total_seconds = hours * 3600 + minutes * 60 + seconds
    return f"https://www.youtube.com/watch?v={video_id}&t={total_seconds}s"

def summarize_result(query, results):

    client = OpenAI(
        api_key=DATABRICKS_TOKEN,
        base_url="https://dbc-3f1c04d0-60d3.cloud.databricks.com/serving-endpoints"
    )

    response = client.chat.completions.create(
        model="databricks-gpt-oss-120b",
        messages=[
            {
                "role": "system",
                "content": """You are an assistant that summarizes the search results clearly and concisely.
                Do not include any code snippets in your response.
                You will be provided the query shared by user and the search results from a knowledge base.
                Your task is to generate a summary that directly addresses the user's query based on the search results.
                If the search results do not contain relevant information, respond with 'No relevant information found in the search results.
                Do not add any additional information beyond what is provided in the search results.
                Result shall be mardown only.
                """
            },
            {
                "role": "user",
                "content": f"Do not include any code in resutlt. User Query: {query}\nSearch Results: {results}"
            }
        ],
        max_tokens=1000
    )

    # Handle both structured (list) and string response types
    content = response.choices[0].message.content
    if isinstance(content, list):
        # extract only text parts and join them
        text_blocks = [c["text"] for c in content if c.get("type") == "text"]
        summary = "\n".join(text_blocks)
    elif isinstance(content, str):
        summary = content
    else:
        summary = str(content)

    return summary.strip()


# ---------------- Streamlit UI ----------------
st.set_page_config(page_title="VidMind", layout="wide")

# 🎨 Custom CSS for dropdown styling
st.markdown("""
    <style>
    .stApp {
        background-color: #e0f7f6;
    }
    .persona-container {
        display: flex;
        justify-content: center;
        margin-bottom: 25px;
    }
    </style>
""", unsafe_allow_html=True)

# Default persona = Knowledge Explorer
if "persona" not in st.session_state:
    st.session_state["persona"] = "Knowledge Explorer"

# Persona Dropdown
st.markdown('<div class="persona-container" style="background-color: teal;">', unsafe_allow_html=True)

st.markdown(
    '<div style="font-size:44px; font-weight:700; margin-bottom:6px; text-align:center; color: teal;">VidMind</div>',
    unsafe_allow_html=True
)

st.markdown(
    '<div style="font-size:22px; font-weight:700; margin-bottom:6px; text-align:center;">👤 Select Role:</div>',
    unsafe_allow_html=True
)
persona_choice = st.selectbox(
    "Knowledge Explorer",
    options=["Knowledge Explorer", "Media Creator"],
    index=0 if st.session_state["persona"] == "Knowledge Explorer" else 1,
    key="persona_dropdown",
    label_visibility="collapsed"
)

if persona_choice != st.session_state["persona"]:
    st.session_state["persona"] = persona_choice
    st.rerun()

st.markdown('</div>', unsafe_allow_html=True)

# ===================== Media Creator VIEW =====================
if st.session_state["persona"] == "Media Creator":
    st.subheader("📤 Upload Video to Youtube and Knowledge Base")

    email = st.text_input("Enter your Email ID for notification:", placeholder="you@example.com")

    uploaded_file = st.file_uploader("Upload an MP4 file", type=["mp4"])
    st.info("ℹ️ After uploading, the video processing job will be triggered and you will be notified via email on completion.")
    if uploaded_file is not None:

        if not email:
            st.warning("⚠️ Please enter your email before uploading.")
        else:
            st.info("⏳ Uploading video...")
            # Generate unique ID
            unique_id = str(uuid.uuid4()).replace("-", "")
            folder_path = os.path.join(VOLUME_PATH, unique_id)
            os.makedirs(folder_path, exist_ok=True)

            # Save uploaded video
            video_path = folder_path+ f"/{unique_id}.mp4"
            with open(video_path, "wb") as f:
                f.write(uploaded_file.read())

            st.success(f"✅ Video is uploaded successfully ")

            # Trigger Databricks job
            st.info("🚀 Triggering the Video Proessing Job...")

            trigger_url = f"{DATABRICKS_HOST}/api/2.1/jobs/run-now"
            payload = {
                "job_id": JOB_ID,
                "job_parameters": {"video_id": unique_id, "email_id": email}
            }
            headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}

            response = requests.post(trigger_url, json=payload, headers=headers)

            if response.status_code == 200:
                run_id = response.json().get("run_id")
                st.success(f"🎯 Job triggered successfully! Run ID: {run_id}")
                st.success(f"🎯 You will receive an Email once the Job is completed and your video is published on YouTube.")
            else:
                st.error(f"❌ Failed to trigger job: {response.text}")

# ===================== Knowledge Explorer VIEW =====================
else:
    st.subheader("💬 Search Videos Knowledge Base")

    query = st.text_input("Type your search query:")

    if query:
        st.info(f"Searching for: **{query}** ...")

        def perform_search(query):
            

            vsc = VectorSearchClient(
                workspace_url=DATABRICKS_HOST,
                personal_access_token=DATABRICKS_TOKEN
            )

            index = vsc.get_index(
                endpoint_name="hackathon_vector_search_index",
                index_name="hackathon.youtube.videos_uploaded_index"
            )

            results = index.similarity_search(
                num_results=1,
                columns=["youtube_video_id","start_time","chunk_text"],
                query_text=query,
                query_type="HYBRID"
            )

            extracted_results = extract_results(results)
            if not extracted_results:
                return "No matching results found."

            extracted = extracted_results
            video_url = youtube_url(extracted[0]["video_id"], extracted[0]["start_time"])
            summary = summarize_result(query, extracted_results)

            return summary + f"\n\n🎥 Watch on YouTube : {video_url}"

        results = perform_search(query)
        st.success("✅ Search completed!")
        st.write(results)
