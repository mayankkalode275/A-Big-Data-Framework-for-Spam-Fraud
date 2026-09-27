@echo off
REM ==============================================================================
REM HDFS Management Script for Big Data Spam and Fraud Identification Project
REM Mumbai University Semester 7 Computer Engineering (Windows CMD)
REM Target Hadoop Installation: D:\Downloads\hadoop-3.5.0\hadoop-3.5.0
REM ==============================================================================

SET HADOOP_BIN=D:\Downloads\hadoop-3.5.0\hadoop-3.5.0\bin\hdfs.cmd

echo ======================================================================
echo  Big Data Framework for Identifying Spam and Fraudulent Communications
echo  Hadoop HDFS Setup ^& Directory Inspector Script (Windows)
echo ======================================================================

echo.
echo [STEP 1] Checking HDFS Root and /HadoopProject Structure...
call "%HADOOP_BIN%" dfs -mkdir -p /HadoopProject/input
call "%HADOOP_BIN%" dfs -mkdir -p /HadoopProject/output

echo.
echo [STEP 2] Listing HDFS /HadoopProject Contents...
call "%HADOOP_BIN%" dfs -ls /HadoopProject

echo.
echo [STEP 3] Uploading Dataset to HDFS /HadoopProject/input ...
IF EXIST "data\sample_communications.csv" (
    call "%HADOOP_BIN%" dfs -put -f data\sample_communications.csv /HadoopProject/input/
    echo [SUCCESS] Dataset uploaded to /HadoopProject/input/sample_communications.csv
) ELSE IF EXIST "D:\Downloads\HadoopProject\input\sample_communications.csv" (
    call "%HADOOP_BIN%" dfs -put -f D:\Downloads\HadoopProject\input\sample_communications.csv /HadoopProject/input/
    echo [SUCCESS] Dataset uploaded from D:\Downloads\HadoopProject\input\ to /HadoopProject/input/
) ELSE (
    echo [NOTICE] Run python run_pipeline.py first to generate the dataset.
)

echo.
echo [STEP 4] Verifying Uploaded Files in HDFS...
call "%HADOOP_BIN%" dfs -ls /HadoopProject/input

echo.
echo [STEP 5] Previewing First 5 Lines from HDFS File...
call "%HADOOP_BIN%" dfs -cat /HadoopProject/input/sample_communications.csv | findstr /V "^$" | cmd /c "more +1"

echo.
echo ======================================================================
echo  HDFS Commands Completed Successfully.
echo  Active Target: hdfs://localhost:9000/HadoopProject/input/
echo ======================================================================
