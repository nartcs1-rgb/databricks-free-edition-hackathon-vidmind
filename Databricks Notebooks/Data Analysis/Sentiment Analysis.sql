-- Databricks notebook source


-- COMMAND ----------

create or replace table hackathon.youtube.videos_comments_sentiments
as select comment_id,ai_analyze_sentiment(text) as sentiment from hackathon.youtube.videos_comments

-- COMMAND ----------

select c.text,s.sentiment from hackathon.youtube.videos_comments_sentiments as s
join  hackathon.youtube.videos_comments c on s.comment_id=c.comment_id