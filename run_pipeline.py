"""
Master Pipeline Runner Script
Semester 7 Mumbai University Computer Engineering Big Data Project

Executes:
 1. Communications Dataset Generation & Staging (data/ & D:\Downloads\HadoopProject\input\)
 2. HDFS Directory Sync & Verification (/HadoopProject/input/sample_communications.csv)
 3. PySpark Distributed Processing, Cleaning, Feature Extraction & ML Classification
 4. Output Summary CSV Export for R Shiny Dashboard
"""

import sys
import os
import shutil
import subprocess

HADOOP_HOME = r"D:\Downloads\hadoop-3.5.0\hadoop-3.5.0"
HDFS_CMD = os.path.join(HADOOP_HOME, "bin", "hdfs.cmd")
LOCAL_HADOOP_PROJECT = r"D:\Downloads\HadoopProject\input"

def sync_to_hadoop_and_hdfs(local_csv):
    """
    Syncs local dataset to D:\\Downloads\\HadoopProject\\input\\ and uploads to HDFS /HadoopProject/input/
    """
    print("\n[HADOOP STAGING & HDFS SYNC]")
    # 1. Local Hadoop Project Staging Directory Sync
    os.makedirs(LOCAL_HADOOP_PROJECT, exist_ok=True)
    target_local_path = os.path.join(LOCAL_HADOOP_PROJECT, "sample_communications.csv")
    shutil.copy2(local_csv, target_local_path)
    print(f"  [SYNC] Copied '{local_csv}' -> '{target_local_path}'")

    # 2. HDFS Directory Upload Check
    cmd_to_run = HDFS_CMD if os.path.exists(HDFS_CMD) else "hdfs"
    try:
        # Create HDFS input folder if needed
        subprocess.run([cmd_to_run, "dfs", "-mkdir", "-p", "/HadoopProject/input"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5)
        # Upload / overwrite file in HDFS
        res = subprocess.run([cmd_to_run, "dfs", "-put", "-f", target_local_path, "/HadoopProject/input/"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
        
        # Verify HDFS Upload
        check_res = subprocess.run([cmd_to_run, "dfs", "-ls", "/HadoopProject/input/sample_communications.csv"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5)
        if check_res.returncode == 0:
            print("  [HDFS SUCCESS] Verified dataset in HDFS at: 'hdfs://localhost:9000/HadoopProject/input/sample_communications.csv'")
            return True
        else:
            print("  [HDFS WARNING] HDFS upload executed, but list check returned non-zero.")
    except Exception as e:
        print(f"  [HDFS SYNC NOTE] HDFS upload note: {e}")
        
    return False

def run_project_pipeline():
    print("=" * 80)
    print(" MUMBAI UNIVERSITY SEMESTER 7 COMPUTER ENGINEERING BIG DATA COURSE PROJECT")
    print(" Title: A Big Data Framework for Identifying Spam and Fraudulent Communications")
    print(" Stack: Hadoop HDFS | Apache Spark (PySpark) | R Shiny Dashboard")
    print("=" * 80 + "\n")
    
    # Step 1: Dataset Verification / Generation
    csv_path = "data/sample_communications.csv"
    if not os.path.exists(csv_path):
        print("[STEP 1] Generating sample communications dataset...")
        from data.generate_dataset import generate_communications_dataset
        generate_communications_dataset()
    else:
        print(f"[STEP 1] Raw dataset verified at '{csv_path}'.")

    # Step 1B: Sync to Local Hadoop Project & Upload to HDFS
    sync_to_hadoop_and_hdfs(csv_path)

    # Step 2: Run PySpark Pipeline
    print("\n[STEP 2] Launching Apache Spark Distributed Processing Engine...")
    from spark.spark_pipeline import main as run_spark
    run_spark()
    
    # Step 3: Verify Output Files
    print("\n[STEP 3] Verifying Generated Big Data Output Summaries...")
    output_files = [
        "output/aggregated_summary.csv",
        "output/communication_type_summary.csv",
        "output/hourly_velocity_summary.csv",
        "output/keyword_frequency.csv",
        "output/model_metrics.csv",
        "output/processed_communications.csv"
    ]
    
    all_ok = True
    for out_file in output_files:
        if os.path.exists(out_file):
            size_kb = round(os.path.getsize(out_file) / 1024, 2)
            print(f"  [OK] Found output file '{out_file}' ({size_kb} KB)")
        else:
            print(f"  [MISSING] Output file '{out_file}' not found!")
            all_ok = False
            
    if all_ok:
        print("\n" + "=" * 80)
        print(" PIPELINE EXECUTION COMPLETED SUCCESSFULLY!")
        print(" Next Step: Open RStudio and run 'shiny::runApp(\"r_dashboard\")' or launch via R script.")
        print("=" * 80)
    else:
        print("\n[WARNING] Some output files were missing. Please re-run the pipeline.")

if __name__ == "__main__":
    run_project_pipeline()
