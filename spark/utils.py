"""
Spark & HDFS Helper Utilities with Environment Auditor
Semester 7 Mumbai University Computer Engineering Big Data Project
"""

import os
import sys
import subprocess
import json

HADOOP_HOME = r"D:\Downloads\hadoop-3.5.0\hadoop-3.5.0"
HADOOP_CONF_DIR = r"D:\Downloads\hadoop-3.5.0\hadoop-3.5.0\etc\hadoop"
HDFS_CMD = os.path.join(HADOOP_HOME, "bin", "hdfs.cmd")

def auto_detect_java():
    """
    Auto-detects Java installation and configures JAVA_HOME and HADOOP_HOME.
    """
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # Configure Hadoop Environment Variables
    if os.path.exists(HADOOP_HOME):
        os.environ["HADOOP_HOME"] = HADOOP_HOME
        os.environ["HADOOP_CONF_DIR"] = HADOOP_CONF_DIR
        hadoop_bin = os.path.join(HADOOP_HOME, "bin")
        if hadoop_bin not in os.environ["PATH"]:
            os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ["PATH"]

    if "JAVA_HOME" in os.environ and os.path.exists(os.environ["JAVA_HOME"]):
        return True

    possible_paths = [
        r"C:\Program Files\Java\jdk-17",
        r"C:\Program Files\Android\Android Studio\jbr",
        r"C:\Program Files\JetBrains\PyCharm 2025.3.3\jbr",
        r"C:\Program Files\Java\jdk1.8.0_301",
        r"C:\Program Files\Java\jdk-11",
        r"C:\Program Files\Java\jdk-21"
    ]
    
    for path in possible_paths:
        if os.path.exists(path) and os.path.exists(os.path.join(path, "bin", "java.exe" if os.name == "nt" else "java")):
            os.environ["JAVA_HOME"] = path
            os.environ["PATH"] = os.path.join(path, "bin") + os.pathsep + os.environ["PATH"]
            print(f"[JAVA AUDITOR] Auto-detected JAVA_HOME at: '{path}'")
            return True

    print("[JAVA AUDITOR WARNING] Java installation not found in standard paths.")
    return False

def check_hdfs_status():
    """
    Verifies if Hadoop HDFS CLI is available and NameNode daemon is active on RPC port 9000.
    Returns (is_available, hdfs_path_or_local_fallback, storage_mode_label)
    """
    hdfs_url = "hdfs://localhost:9000/HadoopProject/input/sample_communications.csv"
    local_path = "data/sample_communications.csv"
    
    # Resolve executable command
    cmd_to_run = HDFS_CMD if os.path.exists(HDFS_CMD) else "hdfs"
    
    try:
        res = subprocess.run([cmd_to_run, "dfs", "-ls", "hdfs://localhost:9000/HadoopProject"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=5)
        if res.returncode == 0:
            print("[HDFS AUDITOR] HDFS NameNode is ACTIVE and responsive on hdfs://localhost:9000/HadoopProject")
            return True, hdfs_url, "HDFS Distributed Storage Mode (hdfs://localhost:9000/HadoopProject)"
        else:
            print("[HDFS AUDITOR] HDFS RPC check returned code:", res.returncode)
    except Exception as e:
        print(f"[HDFS AUDITOR] HDFS command execution check note: {e}")
        
    print(f"[HDFS AUDITOR] Falling back to Local Development Storage Mode ('{local_path}')")
    return False, local_path, "Local Fallback Storage Mode (Development/Testing)"

def get_spark_session(app_name="BigData_SpamFraud_Analyzer"):
    """
    Initializes and returns a PySpark SparkSession.
    """
    auto_detect_java()
    from pyspark.sql import SparkSession
    
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.master", "local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "2g")
    )
    
    spark = builder.get_OrCreate() if hasattr(builder, 'get_OrCreate') else builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def resolve_paths_and_mode():
    """
    Returns (input_path, output_dir, is_hdfs_active, storage_mode_label)
    """
    is_hdfs, input_path, mode_label = check_hdfs_status()
    output_dir = "output/"
    os.makedirs(output_dir, exist_ok=True)
    
    info = {
        "is_hdfs_active": is_hdfs,
        "storage_mode": mode_label,
        "processing_engine": "Apache Spark 3.5 (PySpark)",
        "input_source": input_path
    }
    with open(os.path.join(output_dir, "storage_info.json"), "w") as f:
        json.dump(info, f, indent=2)
        
    return input_path, output_dir, is_hdfs, mode_label
