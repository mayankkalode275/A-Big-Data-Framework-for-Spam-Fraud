#!/bin/bash
# ==============================================================================
# HDFS Management Script for Big Data Spam and Fraud Identification Project
# Mumbai University Semester 7 Computer Engineering
# ==============================================================================

echo "======================================================================"
echo " Big Data Framework for Identifying Spam and Fraudulent Communications"
echo " Hadoop HDFS Setup & Initialization Script"
echo "======================================================================"

# Step 1: Verify Hadoop Installation & Daemon Status
echo -e "\n[STEP 1] Checking HDFS availability..."
if command -v hdfs &> /dev/null; then
    echo "[INFO] HDFS CLI found. Testing connection to NameNode..."
    hdfs dfs -ls / > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo "[SUCCESS] HDFS NameNode is active and responsive."
    else
        echo "[WARNING] Unable to connect to HDFS NameNode. Ensure Hadoop daemons (start-dfs.sh) are running."
    fi
else
    echo "[NOTICE] 'hdfs' command not found in PATH. Project will fall back to local file storage for standalone demo mode."
fi

# Step 2: Create HDFS Directory Structure
echo -e "\n[STEP 2] Creating HDFS directory structure..."
hdfs dfs -mkdir -p /bigdata_project/raw
hdfs dfs -mkdir -p /bigdata_project/processed
hdfs dfs -mkdir -p /bigdata_project/output

echo "[INFO] Current HDFS directory layout:"
hdfs dfs -ls -R /bigdata_project

# Step 3: Upload Raw Communications Dataset to HDFS
echo -e "\n[STEP 3] Uploading raw dataset to HDFS /bigdata_project/raw/ ..."
if [ -f "data/sample_communications.csv" ]; then
    hdfs dfs -put -f data/sample_communications.csv /bigdata_project/raw/
    echo "[SUCCESS] Upload complete! HDFS Raw Directory Contents:"
    hdfs dfs -ls /bigdata_project/raw
else
    echo "[ERROR] Local dataset 'data/sample_communications.csv' not found. Run dataset generator first."
fi

# Step 4: Verify File Preview in HDFS
echo -e "\n[STEP 4] Previewing first 5 lines of raw dataset stored in HDFS:"
hdfs dfs -cat /bigdata_project/raw/sample_communications.csv | head -n 5

echo -e "\n======================================================================"
echo " HDFS Initialization Completed Successfully!"
echo " Raw Data Path: hdfs://localhost:9000/bigdata_project/raw/sample_communications.csv"
echo "======================================================================"
