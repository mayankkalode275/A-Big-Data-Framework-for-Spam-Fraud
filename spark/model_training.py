"""
PySpark ML Classification & Evaluation Module
Semester 7 Mumbai University Computer Engineering Big Data Project
"""

from itertools import chain
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import functions as F
import pandas as pd

def train_and_evaluate_spark_ml(df):
    """
    Trains a Spark ML Logistic Regression classifier on extracted communication features,
    calculates Accuracy, Precision, Recall, F1 Score on a held-out test split (20%),
    and generates Confusion Matrix & Class Distribution data.
    """
    print("[SPARK ML] Assembling feature vectors for Spark ML Pipeline...")
    
    feature_cols = [
        "msg_length", "word_count", "digit_count", 
        "special_char_count", "has_url", "suspicious_keyword_count", "has_repeated_chars"
    ]
    
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    ml_df = assembler.transform(df)
    
    # Index label column
    indexer = StringIndexer(inputCol="label", outputCol="indexed_label")
    indexer_model = indexer.fit(ml_df)
    indexed_df = indexer_model.transform(ml_df)
    labels_arr = list(indexer_model.labels)
    
    # 1. Dataset / Class Distribution Verification
    total_count = indexed_df.count()
    label_counts = indexed_df.groupBy("label").count().collect()
    class_dist_list = []
    for r in label_counts:
        c_name = r["label"]
        c_cnt = r["count"]
        c_pct = round((c_cnt / total_count) * 100, 2) if total_count > 0 else 0.0
        class_dist_list.append({"Class": c_name, "Count": c_cnt, "Percentage": f"{c_pct}%"})
    class_dist_pd = pd.DataFrame(class_dist_list)
    
    print(f"\n[DATASET VERIFICATION]")
    print(f"  Total Ingested Records: {total_count}")
    for item in class_dist_list:
        print(f"  Class '{item['Class']}': {item['Count']} records ({item['Percentage']})")
        
    # 2. Train / Test Split (80% Train, 20% Held-Out Test)
    train_df, test_df = indexed_df.randomSplit([0.8, 0.2], seed=42)
    train_count = train_df.count()
    test_count = test_df.count()
    print(f"  Train Set Records (80%): {train_count}")
    print(f"  Test Set Records (20%):  {test_count}")
    
    # 3. Model Training (Trained ONLY on train_df)
    lr = LogisticRegression(featuresCol="features", labelCol="indexed_label", maxIter=20)
    lr_model = lr.fit(train_df)
    
    # 4. Predictions on Unseen Test Set
    predictions = lr_model.transform(test_df)
    predictions.cache()
    
    # 5. Multiclass Evaluation Metrics
    evaluator_acc = MulticlassClassificationEvaluator(labelCol="indexed_label", predictionCol="prediction", metricName="accuracy")
    evaluator_f1 = MulticlassClassificationEvaluator(labelCol="indexed_label", predictionCol="prediction", metricName="f1")
    evaluator_prec = MulticlassClassificationEvaluator(labelCol="indexed_label", predictionCol="prediction", metricName="weightedPrecision")
    evaluator_rec = MulticlassClassificationEvaluator(labelCol="indexed_label", predictionCol="prediction", metricName="weightedRecall")
    
    accuracy = float(evaluator_acc.evaluate(predictions))
    f1_score = float(evaluator_f1.evaluate(predictions))
    precision = float(evaluator_prec.evaluate(predictions))
    recall = float(evaluator_rec.evaluate(predictions))
    
    print(f"\n[SPARK ML EVALUATION RESULTS - HELD-OUT TEST SET]")
    print(f"  Accuracy:  {accuracy * 100:.2f}%")
    print(f"  Precision: {precision * 100:.2f}%")
    print(f"  Recall:    {recall * 100:.2f}%")
    print(f"  F1 Score:  {f1_score * 100:.2f}%")
    
    metrics_pd = pd.DataFrame({
        "Metric": ["Accuracy", "Precision", "Recall", "F1 Score"],
        "Value": [f"{accuracy * 100:.2f}%", f"{precision * 100:.2f}%", f"{recall * 100:.2f}%", f"{f1_score * 100:.2f}%"]
    })
    
    # 6. Confusion Matrix Computation
    cm_rows = predictions.groupBy("indexed_label", "prediction").count().collect()
    cm_dict = {lbl: {p_lbl: 0 for p_lbl in labels_arr} for lbl in labels_arr}
    
    for row in cm_rows:
        act_idx = int(row["indexed_label"])
        pred_idx = int(row["prediction"])
        if act_idx < len(labels_arr) and pred_idx < len(labels_arr):
            act_lbl = labels_arr[act_idx]
            pred_lbl = labels_arr[pred_idx]
            cm_dict[act_lbl][pred_lbl] = row["count"]
            
    confusion_list = []
    for act_lbl in labels_arr:
        row_dict = {"Actual_Label": act_lbl}
        for pred_lbl in labels_arr:
            row_dict[f"Pred_{pred_lbl}"] = cm_dict[act_lbl].get(pred_lbl, 0)
        row_dict["Total_Test_Records"] = sum(cm_dict[act_lbl].values())
        confusion_list.append(row_dict)
        
    confusion_pd = pd.DataFrame(confusion_list)
    
    print("\n[CONFUSION MATRIX - TEST SET]")
    print(confusion_pd.to_string(index=False))
    
    return indexed_df, metrics_pd, confusion_pd, class_dist_pd, train_count, test_count
