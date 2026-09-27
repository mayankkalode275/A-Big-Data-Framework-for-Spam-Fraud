"""
PySpark Data Preprocessing & Feature Extraction Module
Semester 7 Mumbai University Computer Engineering Big Data Project
"""

from pyspark.sql import functions as F

# Key high-risk suspicious terms indicative of Spam or Fraud
SUSPICIOUS_KEYWORDS_REGEX = r"(urgent|alert|verify|bank|kyc|blocked|suspended|won|winner|jackpot|loan|otp|refund|claim|offer|free|cash|discount|unauthorized|login|paytm|hdfc|sbi|kbc|income|tax|court|arrest|password|debit|credit)"

def preprocess_communications_dataframe(df):
    """
    Applies distributed PySpark cleaning, text normalization, and feature extraction.
    Uses native Spark SQL functions for fast Catalyst execution without Python UDF overhead.
    """
    print("[SPARK PREPROCESSING] Cleaning & handling missing values...")
    
    # 1. Deduplication & Null handling
    cleaned_df = df.dropDuplicates(["message_id"]).dropna(subset=["message_id", "message", "communication_type"])
    
    # 2. Text Normalization
    cleaned_df = cleaned_df.withColumn("clean_text", F.lower(F.col("message")))
    cleaned_df = cleaned_df.withColumn("sanitized_text", F.regexp_replace(F.col("clean_text"), r"[^a-z0-9\s]", " "))
    
    # 3. Feature Extraction
    print("[SPARK PREPROCESSING] Extracting Big Data statistical features...")
    
    # Message length
    cleaned_df = cleaned_df.withColumn("msg_length", F.length(F.col("message")))
    
    # Digit count using regex replace count
    cleaned_df = cleaned_df.withColumn("digit_count", 
        F.length(F.regexp_replace(F.col("message"), r"[^\d]", ""))
    )
    
    # Special character count (!, $, %, #, ?, etc.)
    cleaned_df = cleaned_df.withColumn("special_char_count",
        F.length(F.regexp_replace(F.col("message"), r"[a-zA-Z0-9\s]", ""))
    )
    
    # URL presence flag
    url_pattern = r"(http://|https://|www\.|[a-zA-Z0-9-]+\.(xyz|net|com|org|info))"
    cleaned_df = cleaned_df.withColumn("has_url",
        F.when(F.col("message").rlike(url_pattern), 1).otherwise(0)
    )
    
    # Repeated punctuation pattern flag (e.g., !!! or ??? or $$$)
    cleaned_df = cleaned_df.withColumn("has_repeated_chars",
        F.when(F.col("message").rlike(r"(!{2,}|\?{2,}|\${2,})"), 1).otherwise(0)
    )
    
    # Word count
    cleaned_df = cleaned_df.withColumn("raw_tokens", F.split(F.trim(F.col("sanitized_text")), r"\s+"))
    cleaned_df = cleaned_df.withColumn("word_count", F.size(F.col("raw_tokens")))
    
    # Count suspicious keywords using regex matching on cleaned text
    cleaned_df = cleaned_df.withColumn("suspicious_keyword_count",
        F.size(F.split(F.regexp_replace(F.col("clean_text"), r".*?(" + SUSPICIOUS_KEYWORDS_REGEX + r").*?", r"$1 "), r"\s+")) - 1
    )
    cleaned_df = cleaned_df.withColumn("suspicious_keyword_count",
        F.when(F.col("suspicious_keyword_count") < 0, 0).otherwise(F.col("suspicious_keyword_count"))
    )
    
    # Token array for keyword frequency analysis
    cleaned_df = cleaned_df.withColumn("clean_tokens", F.col("raw_tokens"))
    
    # 4. Rule-Based Classification (Explainable Viva Model)
    print("[SPARK PREPROCESSING] Applying explainable classification rules...")
    cleaned_df = cleaned_df.withColumn("rule_pred_label",
        F.when(
            (F.col("has_url") == 1) & (F.col("suspicious_keyword_count") >= 2) & (F.col("digit_count") >= 3), "Fraud"
        ).when(
            (F.col("has_url") == 1) | (F.col("suspicious_keyword_count") >= 2) | (F.col("has_repeated_chars") == 1), "Spam"
        ).when(
            (F.col("suspicious_keyword_count") >= 1) & (F.col("digit_count") >= 4), "Fraud"
        ).otherwise("Normal")
    )
    
    return cleaned_df
