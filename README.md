# **VidMind – AI-Powered YouTube Automation & Knowledge Intelligence Platform**

VidMind is an end-to-end solution built on **Databricks Free Edition** for the virtual company **DataTuber**, which publishes technical demo content on YouTube.  
The solution automates the YouTube publishing workflow, builds an intelligent video knowledge base, and provides business insights through dashboards and conversational analytics.

---

## 🚀 **Solution Overview**

VidMind streamlines the entire video-to-knowledge lifecycle:

- **Creators** upload a video through the Databricks Web App.  
- The system automatically extracts audio, creates transcripts, generates metadata, produces thumbnails, and publishes the video to YouTube.  
- All transcripts are embedded and stored in **Databricks Vector Search**, enabling natural-language Q&A.  
- YouTube metrics and comment sentiments are stored in Delta Tables to power analytics.  
- Business owners access dashboards and Genie-powered insights via **Databricks One**.

---

## 🧩 **Architecture Workflow**

1. **Upload Video** → User uploads via Web App.  
2. **Databricks Job Triggered** → End-to-end processing starts.  
3. **Audio Extraction** → Convert video to audio using MoviePy.  
4. **Transcription** → Whisper model generates the text.  
5. **Metadata Generation** → LLM produces title, description, tags.  
6. **Thumbnail Generation** → GPT Image Model creates thumbnail.  
7. **Publish Video** → Upload automatically to YouTube.  
8. **Data Storage** → Save transcripts, metadata, embeddings, comments, sentiments.  
9. **Q&A Search** → Vector search + LLM summarization.  
10. **Dashboards & Genie** → Business owners explore data visually and via NLQ.

---

## 🛠️ **Technologies & Services Used**

### **User Experience**
- **Web UI for Creators & Explorers** → *Databricks Web App*  
- **Unified UI for Business Owners** → *Databricks One*  

### **Orchestration**
- **Automated video-processing pipeline** → *Databricks Jobs*  

### **Video Processing**
- **Convert video to audio** → *MoviePy*  
- **Generate transcript** → *OpenAI Whisper Model*  
- **Generate title, description & tags** → *Databricks Foundation Model Serving – gpt-oss-120b*  
- **Create thumbnail** → *OpenAI gpt-image-1*  
- **Auto-upload & fetch views/likes/comments** → *YouTube Data API*  

### **Storage**
- **Videos, audio & temporary files** → *Databricks Volumes*  
- **Structured YouTube data** → *Unity Catalog Delta Tables*  

### **Notifications**
- **Send email alerts** → *Gmail SMTP Service*  

### **Knowledge Base / Vector Search**
- **Generate embeddings for transcript chunks** → *Databricks Foundation Model Serving – gpt-large-en*  
- **Vector storage & similarity search** → *Databricks Vector Search*  
- **Summarize user queries + search results** → *Databricks FM – gpt-oss-120b*  

### **Analytics & Intelligence**
- **Sentiment analysis on comments** → *Databricks SQL (ai_analyze_sentiment)*  
- **Business dashboards** → *Databricks Dashboards*  
- **Natural-language analytics** → *Databricks AI/BI Genie*  
- **AI-assisted coding** → *Databricks AI Assistant*  

---

## 🎯 **Personas Supported**

### **1. Media Creators**
- Upload videos  
- Receive generated title/description/tags  
- AI-generated thumbnail  
- Auto-published to YouTube  
- Email notification on completion  

### **2. Knowledge Explorers**
- Ask natural-language questions  
- Receive summarized answers  
- Get precise video timestamps  

### **3. Business Owners**
- View channel performance  
- Dashboards for views, engagement, sentiment  
- Conversational insights via Genie  

---

## 📊 **Key Features**

- Fully automated YouTube publishing workflow  
- AI-driven metadata & thumbnail generation  
- Vector search–powered knowledge explorer  
- Sentiment analytics on YouTube comments  
- Dashboard-driven business insights  
- Natural-language analytics via Genie  

---

## 🚀 **Demo Highlights**

- Upload a video → pipeline triggers → video published to YouTube  
- Ask “How to configure OpenAI in Databricks?” → Explorer returns exact timestamp  
- Dashboard shows views, sentiment, most-watched video & monthly trends  

---

## 🙌 **Thank You**

VidMind demonstrates how Databricks can act as a unified platform for **AI + Data + Analytics + Automation**.