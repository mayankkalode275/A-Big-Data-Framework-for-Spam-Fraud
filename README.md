# A Big Data Framework for Spam & Fraud Detection

A distributed Big Data framework for processing, analyzing, and detecting spam and fraudulent communications across multi-channel networks (SMS, Email, WhatsApp, VoIP). The system combines Hadoop HDFS storage, Apache Spark (PySpark) distributed processing, Spark MLlib machine learning, explainable rule-based feature extraction, and an interactive R Shiny dashboard with real-time message detection.

---

## Table of Contents

- [Overview](#overview)
- [Live Demo](#live-demo)
- [Problem Statement](#problem-statement)
- [Objectives](#objectives)
- [Technologies Used](#technologies-used)
- [System Architecture](#system-architecture)
- [Dataset Specifications](#dataset-specifications)
- [Data Preprocessing](#data-preprocessing)
- [Extracted Features](#extracted-features)
- [Detection Rule Logic](#detection-rule-logic)
- [Machine Learning Classification](#machine-learning-classification)
- [Interactive Dashboard](#interactive-dashboard)
- [Individual Message Detection](#individual-message-detection)
- [Project Structure](#project-structure)
- [How to Run](#how-to-run)
- [HDFS Storage Integration](#hdfs-storage-integration)
- [Generated Output Artifacts](#generated-output-artifacts)
- [Limitations & Future Scope](#limitations--future-scope)
- [Academic Context](#academic-context)

---

## Overview

Modern communication channels are subject to high volumes of unsolicited spam and high-risk fraudulent attacks (e.g., bank phishing, OTP theft, lottery scams, legal threats). Traditional centralized security systems struggle to handle the high volume, velocity, and variety of multi-channel message streams. 

This project demonstrates an end-to-end Big Data pipeline designed to ingest, sanitize, analyze, and classify communications into **NORMAL**, **SPAM**, or **FRAUD**. It integrates distributed block storage (Hadoop HDFS), in-memory distributed data processing (Apache Spark PySpark), machine learning classification (Spark MLlib), and a web dashboard with live message detection (R Shiny).

---

## Live Demo

🚀 **Live Deployed Web Application**: [https://mayankkalode.shinyapps.io/big-data-spam-fraud-detection/](https://mayankkalode.shinyapps.io/big-data-spam-fraud-detection/)

The interactive R Shiny dashboard is deployed live on **shinyapps.io**, providing full access to dataset visualizations, model metrics, confusion matrices, data tables, and live real-time message detection.

> **Note on Architecture**: The public cloud dashboard runs in a decoupled deployment model. Local batch ingestion and model training utilize Hadoop HDFS and PySpark, while the live dashboard and real-time message detection engine run natively in R on shinyapps.io. No shinyapps.io account tokens or secret credentials are stored in this repository.

---

## Problem Statement

Communicating over SMS, Email, WhatsApp, and VoIP presents several security challenges:
- **High Volume & Velocity**: Millions of messages sent per hour require scalable, distributed processing.
- **Heterogeneous Variety**: Unstructured textual streams spanning multiple communication protocols.
- **Financial & Identity Threats**: Fraudulent attempts targeting personal identification numbers, OTPs, and bank credentials.

This project addresses these challenges by building a scalable, distributed processing architecture that provides both machine learning classification and explainable rule-based detection.

---

## Objectives

1. **Distributed Storage**: Store multi-channel communication records reliably in Hadoop HDFS (`/HadoopProject/input/`).
2. **Distributed Preprocessing**: Clean, sanitize, and tokenize large-scale message data using Apache Spark PySpark DataFrames.
3. **Feature Extraction**: Calculate statistical, textual, and indicator-based features (character length, word count, digit frequency, URL flags, link phrases, and threat keywords).
4. **Machine Learning Classification**: Train and evaluate a scalable Spark MLlib Logistic Regression classifier on held-out test data.
5. **Explainable Rule-Based Detection**: Provide deterministic, transparent threat classification with dynamic explanation summaries.
6. **Interactive Visualization**: Present real-time aggregations, hourly velocity trends, keyword frequencies, and confusion matrices using R Shiny and `ggplot2`.
7. **Individual Message Detection**: Allow users to evaluate custom messages in real time with an execution decision trace.

---

## Technologies Used

- **Distributed File System**: Hadoop HDFS (Distributed Block Storage)
- **Processing Engine**: Apache Spark 3.5 / PySpark (In-Memory Distributed Computing)
- **Machine Learning**: Spark MLlib (`LogisticRegression`, `VectorAssembler`, `MulticlassClassificationEvaluator`)
- **Dashboard & UI**: R 4.x, R Shiny, `shinydashboard`, `ggplot2`, `DT`, `dplyr`
- **Programming Languages**: Python 3.x, R 4.x
- **Data Formats**: CSV, JSON

---

## System Architecture

```
[ Raw Multi-Channel Communications Dataset ]
       │
       ▼ (Ingestion & Staging)
[ Hadoop HDFS Storage ] ──► hdfs://localhost:9000/HadoopProject/input/sample_communications.csv
       │
       ▼
[ Apache Spark / PySpark Distributed Engine ]
  ├─► Data Cleaning & Deduplication (dropDuplicates, dropna)
  ├─► Text Sanitization & Tokenization (Regex, Lowercasing)
  ├─► Feature Extraction (Lengths, Digits, URLs, Link Phrases, Keywords)
  └─► Spark MLlib Logistic Regression & Explainable Rule Classifier
       │
       ▼
[ Generated CSV & JSON Summaries ] ──► Exported to output/ directory
       │
       ▼
[ R Shiny Interactive Dashboard ]
  ├─► Home / System Environment Status
  ├─► Dataset Explorer & Raw Preview
  ├─► Preprocessing & Spark Transformations Pipeline
  ├─► Spam & Fraud Analysis (Metrics & Confusion Matrix)
  ├─► Visualizations (ggplot2 Distribution & Hourly Velocity)
  ├─► Processed Data Explorer (DT Filtering)
  ├─► Message Communication Detection (Live Analysis & Decision Trace)
  └─► About Project Specifications
```

---

## Dataset Specifications

The dataset represents a multi-channel communication stream containing 3,000 records structured with the following schema:

| Field | Data Type | Description |
| :--- | :--- | :--- |
| `message_id` | String | Unique record identifier (e.g., `MSG10001`) |
| `sender` | String | Originating phone number or email address |
| `receiver` | String | Recipient phone number or email address |
| `message` | String | Text body of the communication |
| `timestamp` | Datetime | Communication timestamp (`YYYY-MM-DD HH:MM:SS`) |
| `communication_type` | String | Communication channel (`SMS`, `Email`, `WhatsApp`, `VoIP`) |
| `label` | String | Target ground truth classification (`Normal`, `Spam`, `Fraud`) |

---

## Data Preprocessing

Spark applies distributed PySpark DataFrame transformations for fast execution:

1. **Deduplication**: `dropDuplicates(["message_id"])` eliminates redundant communication records.
2. **Null Handling**: `dropna(subset=["message_id", "message", "communication_type"])` filters incomplete records.
3. **Text Normalization**: Lowercasing and regex sanitization (`[^a-z0-9\s]`) clean text for tokenization.
4. **Tokenization**: Splitting sanitized text into word arrays for word counting and keyword matching.
5. **Distributed Feature Extraction**: Native Spark SQL functions extract numerical counts and boolean indicators without UDF performance overhead.

---

## Extracted Features

| Feature Name | Type | Description |
| :--- | :--- | :--- |
| `msg_length` | Integer | Total character length of the raw message |
| `word_count` | Integer | Total word count after sanitization |
| `digit_count` | Integer | Count of numerical digits (`0-9`) in message body |
| `special_char_count` | Integer | Count of non-alphanumeric, non-whitespace characters (`!`, `$`, `%`, `#`, `?`, etc.) |
| `has_url` | Binary (1/0) | Detects actual web URLs and domains (`http://`, `https://`, `www.`, `.xyz`, `.net`, `.com`, etc.) |
| `has_link_phrase` | Binary (1/0) | Detects explicit link call-to-action phrases (`"click on this link"`, `"click here"`, `"open link"`) |
| `suspicious_keyword_count` | Integer | Total count of matched high-risk keywords across promotional, financial, and urgency categories |
| `has_repeated_chars` | Binary (1/0) | Detects repeated threat punctuation patterns (`!!`, `??`, `$$`) |
| `promotional_phrase_detected` | Binary (1/0) | Detects promotional solicitation phrases (`"get 50000 money"`, `"win money"`, `"claim reward"`) |
| `financial_request_detected` | Binary (1/0) | Detects financial or security credential requests (`"give me otp"`, `"share otp"`, `"verify kyc"`) |

---

## Detection Rule Logic

The framework enforces a strict, explainable rule hierarchy to classify messages into threat categories:

### 1. FRAUD Criteria (High Risk)
Classified as **FRAUD** if any of the following high-risk conditions are met:
- **Rule F1**: Actual URL present AND suspicious keyword count $\ge 2$ AND digit count $\ge 3$.
- **Rule F2**: Financial/OTP/KYC credential request detected AND combined with an actual URL, link phrase, keyword, or digit activity.
- **Rule F3**: Actual URL present with financial terms (`bank`, `account`, `debit`, `credit`) AND digit or special character anomalies.
- **Rule F4**: Financial/urgency terms combined with significant digit activity ($\ge 4$ digits, e.g., account #, tax amount, or OTP).

### 2. SPAM Criteria (Medium Risk)
Classified as **SPAM** if not Fraud and any of the following promotional conditions are met:
- **Rule S1**: Promotional phrase detected OR call-to-action link phrase combined with promotional keywords or digit activity.
- **Rule S2**: Call-to-action link phrase combined with suspicious keywords or repeated threat punctuation.
- **Rule S3**: Actual URL present with promotional content.
- **Rule S4**: Multiple promotional keywords present OR repeated threat punctuation (`!!`, `??`, `$$`).

### 3. NORMAL Criteria (Low Risk)
- **Rule N1**: Assigned when zero threat or promotional triggers are met.

---

## Machine Learning Classification

The Spark processing pipeline trains and evaluates a multiclass classifier using Spark MLlib:

- **Model Architecture**: Spark MLlib `LogisticRegression` (`family="multinomial"`, `maxIter=20`, `regParam=0.01`).
- **Feature Vector**: Assembled using `VectorAssembler` over `msg_length`, `word_count`, `digit_count`, `special_char_count`, `has_url`, `has_repeated_chars`, and `suspicious_keyword_count`.
- **Train/Test Methodology**: Deterministic 80% Training (~2,400 records) / 20% Held-out Test (~600 records) split (`seed=42`).
- **Evaluation Engine**: PySpark `MulticlassClassificationEvaluator`.

### Model Metrics Output (`output/model_metrics.csv`)

| Evaluation Metric | Value |
| :--- | :--- |
| **Accuracy** | **86.31%** |
| **Precision** | **85.96%** |
| **Recall** | **86.31%** |
| **F1 Score** | **85.66%** |

*Note: Evaluation metrics are calculated strictly on unseen held-out test predictions.*

---

## Interactive Dashboard

The dashboard is built with R Shiny, `shinydashboard`, `ggplot2`, and `DT`. It features 8 tabs:

1. **Home / Overview**: System environment status, HDFS storage indicator, architecture diagram, and summary cards.
2. **Dataset Overview**: Raw dataset schema definitions and interactive data preview.
3. **Preprocessing & Spark**: Detailed breakdown of PySpark transformations and feature extraction rules.
4. **Spam & Fraud Analysis**: Model evaluation performance table, train/test split parameters, test set confusion matrix, and explainable rule matrix.
5. **Visualizations**: `ggplot2` charts showing classification breakdowns, channel distributions, top suspicious keyword frequencies, and hourly volume trends.
6. **Processed Data Table**: `DT` interactive explorer allowing filtering by classification label and channel type.
7. **Message Communication Detection**: Real-time detection interface for user-entered messages with decision trace.
8. **About Project**: Specifications, technological stack, and data processing pipeline details.

---

## Individual Message Detection

The **Message Communication Detection** page allows evaluating individual messages against the detection pipeline:

1. **Input**: Prominent text box with **Detect Communication** and **Clear** controls.
2. **Detection Result**: Displays the classification (`NORMAL`, `SPAM`, `FRAUD`), risk level (`LOW`, `MEDIUM`, `HIGH`), reason summary, and extracted indicator badges.
3. **Detection Analysis**: Provides a feature breakdown table and a dynamic explanation (`"Why was this classified?"`).
4. **Detection Decision Trace**: Displays an end-to-end execution path:
   `Input Received` → `Features Extracted` → `Indicators & Patterns Detected` → `Triggered Rule` → `Final Classification & Risk Assessment`.

---

## Project Structure

```
A-Big-Data-Framework-for-Spam-Fraud/
├── data/
│   ├── generate_dataset.py
│   └── sample_communications.csv
├── hadoop/
│   ├── hdfs_commands.cmd
│   └── hdfs_commands.sh
├── spark/
│   ├── model_training.py
│   ├── preprocessing.py
│   ├── spark_pipeline.py
│   └── utils.py
├── r_dashboard/
│   ├── app.R
│   ├── global.R
│   ├── server.R
│   ├── ui.R
│   └── www/
│       └── custom.css
├── output/
│   ├── aggregated_summary.csv
│   ├── class_distribution.csv
│   ├── communication_type_summary.csv
│   ├── confusion_matrix.csv
│   ├── hourly_velocity_summary.csv
│   ├── keyword_frequency.csv
│   ├── ml_evaluation_info.json
│   ├── model_metrics.csv
│   ├── processed_communications.csv
│   └── storage_info.json
├── .gitignore
├── README.md
├── requirements.txt
├── run_pipeline.py
└── run_r_dashboard.R
```

---

## How to Run

### 1. Requirements & Setup

#### Python Dependencies
Install required Python packages:
```bash
pip install -r requirements.txt
```

#### R Dependencies
Install required R packages in R or RStudio:
```R
install.packages(c("shiny", "shinydashboard", "ggplot2", "dplyr", "readr", "DT", "scales"))
```

### 2. Running the PySpark Pipeline

To generate/verify the dataset, sync to Hadoop HDFS, run PySpark feature extraction, train the ML model, and export dashboard summary files:
```bash
python run_pipeline.py
```

### 3. Launching the R Shiny Dashboard

Run via Rscript command line:
```bash
Rscript run_r_dashboard.R
```
Or launch from RStudio:
```R
shiny::runApp("r_dashboard", port = 8080)
```
Open your browser at `http://127.0.0.1:8080`.

---

## HDFS Storage Integration

The framework connects to Hadoop HDFS:
- **Local Hadoop Project Staging**: `D:\Downloads\HadoopProject\input\sample_communications.csv`
- **HDFS Target URI**: `hdfs://localhost:9000/HadoopProject/input/sample_communications.csv`
- **HDFS Command Helper Scripts**: Shell and batch scripts (`hadoop/hdfs_commands.sh` and `hadoop/hdfs_commands.cmd`) handle directory initialization and HDFS `-put` uploads.

---

## Generated Output Artifacts

Running the pipeline generates CSV and JSON artifacts in the `output/` directory:
- `aggregated_summary.csv`: Summary totals, class counts, percentages, and storage status.
- `class_distribution.csv`: Class breakdown across Normal, Spam, and Fraud.
- `communication_type_summary.csv`: Distribution across SMS, Email, WhatsApp, and VoIP.
- `confusion_matrix.csv`: Test set confusion matrix comparing ground truth vs. predictions.
- `hourly_velocity_summary.csv`: Hourly message volume trends.
- `keyword_frequency.csv`: Suspicious keyword frequency counts.
- `model_metrics.csv`: Spark MLlib Accuracy, Precision, Recall, and F1 Score metrics.
- `processed_communications.csv`: Full processed dataset with extracted features and model predictions.
- `storage_info.json`: System storage status and active paths.

---

## Limitations & Future Scope

### Limitations
- **Synthetic Dataset**: The dataset uses structured patterns designed for course project demonstration. Real-world message streams exhibit higher noise and colloquial variations.
- **Batch Pipeline Focus**: Processing currently runs in batch mode using PySpark DataFrames.

### Future Scope
- **Streaming Ingestion**: Integration with Apache Kafka and Spark Streaming for real-time stream processing.
- **Advanced NLP**: Integrating transformer-based language models (e.g., BERT) for semantic contextual embeddings.
- **Adaptive False-Positive Tuning**: User feedback integration to update detection threshold parameters dynamically.

---

## Academic Context

Developed as a Semester 7 Computer Engineering Big Data course project demonstrating distributed storage (Hadoop HDFS), distributed compute (Apache Spark PySpark), machine learning (Spark MLlib), and interactive web visualization (R Shiny).

---

## Deployment Architecture

The framework supports a decoupled deployment model distinguishing between local development compute and public cloud hosting:

### Local Development Environment
- **Hadoop HDFS**: Manages distributed block storage (`hdfs://localhost:9000/HadoopProject/input/`) locally during batch ingestion.
- **Apache Spark / PySpark**: Executes distributed DataFrame cleaning, feature extraction, and Spark MLlib model training (`python run_pipeline.py`).
- **Summary Generation**: Exports processed results to the `output/` directory.

### Public Cloud Deployment Model
- **R Shiny Dashboard**: Can be deployed to cloud hosting platforms such as **shinyapps.io**, **POSIT Connect**, **Hugging Face Spaces**, or containerized with **Docker**.
- **Path Resolution**: `r_dashboard/global.R` features deployment-safe multi-path resolution (`c("output", "../output", "r_dashboard/output", "./output")`), enabling the UI to start independently on any cloud host.
- **Message Communication Detection Engine**: Executes `extract_message_features()` in native R, ensuring full real-time message detection, indicator extraction, and decision tracing without requiring a Java, Hadoop, or Spark installation on the cloud host.

---

## Deployment Checklist

- [x] **Shiny Entry Point Verified**: Validated `r_dashboard/app.R` and `run_r_dashboard.R`.
- [x] **Required R Packages Identified**: `shiny`, `shinydashboard`, `ggplot2`, `dplyr`, `readr`, `DT`, `scales`.
- [x] **Required Data Files Packaged**: Output CSV/JSON summaries bundled in `output/` and `r_dashboard/output/`.
- [x] **Local Windows Paths Removed from Runtime**: Dashboard runtime uses deployment-safe relative path fallback in `global.R`.
- [x] **Local Hadoop Runtime Excluded**: Local Hadoop/Spark logs and cache directories excluded via `.gitignore`.
- [x] **Secrets & Credentials Excluded**: Verified no API keys, private tokens, or credentials exist.
- [x] **Dashboard Starts Independently**: Decoupled R Shiny architecture runs without local Hadoop daemon requirements.
- [x] **Detection Dependency Verified**: `extract_message_features()` engine runs 100% in R without external cloud dependencies.
- [x] **GitHub Repository Ready**: Configured for `https://github.com/mayankkalode275/A-Big-Data-Framework-for-Spam-Fraud`.
- [x] **Cloud Deployment Live**: Successfully deployed on shinyapps.io at [https://mayankkalode.shinyapps.io/big-data-spam-fraud-detection/](https://mayankkalode.shinyapps.io/big-data-spam-fraud-detection/).