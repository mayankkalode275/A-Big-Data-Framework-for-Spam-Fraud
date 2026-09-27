# ==============================================================================
# Shiny UI Definition for Big Data Spam and Fraud Identification Dashboard
# Semester 7 Computer Engineering Course Project
# Title: A Big Data Framework for Spam & Fraud Detection
# ==============================================================================

library(shiny)
library(shinydashboard)
library(DT)

ui <- dashboardPage(
  skin = "blue",
  
  dashboardHeader(
    title = "A Big Data Framework for Spam & Fraud Detection",
    titleWidth = 460
  ),
  
  dashboardSidebar(
    width = 280,
    sidebarMenu(
      id = "tabs",
      menuItem("1. Home / Overview", tabName = "home", icon = icon("home")),
      menuItem("2. Dataset Overview", tabName = "dataset", icon = icon("database")),
      menuItem("3. Preprocessing & Spark", tabName = "preprocessing", icon = icon("cogs")),
      menuItem("4. Spam & Fraud Analysis", tabName = "analysis", icon = icon("shield-alt")),
      menuItem("5. Visualizations", tabName = "visualizations", icon = icon("chart-bar")),
      menuItem("6. Processed Data Table", tabName = "processed_data", icon = icon("table")),
      menuItem("7. Detection", tabName = "detection", icon = icon("search")),
      menuItem("8. About Project", tabName = "about", icon = icon("info-circle"))
    )
  ),
  
  dashboardBody(
    tags$head(
      tags$link(rel = "stylesheet", type = "text/css", href = "custom.css")
    ),
    
    tabItems(
      # ------------------------------------------------------------------------
      # TAB 1: HOME / OVERVIEW
      # ------------------------------------------------------------------------
      tabItem(
        tabName = "home",
        fluidRow(
          box(
            width = 12,
            title = "A Big Data Framework for Spam & Fraud Detection",
            status = "primary",
            solidHeader = TRUE,
            h4("Big Data Course Project"),
            p("A distributed Big Data framework for identifying spam and fraudulent communications across multi-channel communication networks using Hadoop HDFS, Apache Spark (PySpark), and R Shiny.")
          )
        ),
        
        # Summary Value Cards
        fluidRow(
          valueBoxOutput("card_total_msgs", width = 3),
          valueBoxOutput("card_normal_msgs", width = 3),
          valueBoxOutput("card_spam_msgs", width = 3),
          valueBoxOutput("card_fraud_msgs", width = 3)
        ),
        
        # System Environment Audit Status Card
        fluidRow(
          box(
            width = 12,
            title = "Environment & System Architecture Status",
            status = "warning",
            solidHeader = TRUE,
            fluidRow(
              column(4, tags$b("Storage Layer:"), div(style = "margin-top: 5px;", htmlOutput("text_storage_mode"))),
              column(4, tags$b("Processing Engine:"), p(style = "margin-top: 5px;", "Apache Spark 3.5 / PySpark (In-Memory DataFrames)")),
              column(4, tags$b("Visualization Layer:"), p(style = "margin-top: 5px;", "R Shiny & ggplot2 (Decoupled Dashboard UI)"))
            )
          )
        ),
        
        # Pipeline & Concepts
        fluidRow(
          box(
            width = 7,
            title = "Big Data System Architecture Pipeline",
            status = "info",
            solidHeader = TRUE,
            div(
              class = "architecture-box",
              pre(
"[ RAW COMMUNICATION DATASET ]
        │
        ▼ (Data Ingestion & Staging)
[ HADOOP HDFS STORAGE ]  ──► URI: hdfs://localhost:9000/HadoopProject/input/sample_communications.csv
                         ──► Local: D:\\Downloads\\HadoopProject\\input\\
        │
        ▼
[ APACHE SPARK / PYSPARK PROCESSING ]
  ├─► Data Cleaning & Deduplication (dropDuplicates, dropna)
  ├─► Text Tokenization & Sanitization (Lowercasing, Regex)
  ├─► Spark Feature Extraction (Length, Digits, URLs, Keywords)
  ├─► Rule Engine & Spark MLlib Logistic Regression
  └─► Distributed Aggregation (GroupBy, Count, Velocity)
        │
        ▼
[ OUTPUT CSV FILES ]      ──► Path: output/*.csv & /HadoopProject/output/
        │
        ▼
[ R SHINY DASHBOARD ]     ──► Visualizations, ggplot2 Charts, Data Explorer"
              )
            )
          ),
          
          box(
            width = 5,
            title = "Big Data Concepts Demonstrated",
            status = "success",
            solidHeader = TRUE,
            tags$ul(
              tags$li(tags$b("Volume:"), " Ingestion and analysis of multi-channel communication datasets."),
              tags$li(tags$b("Velocity:"), " High-throughput hourly communication trend and pattern monitoring."),
              tags$li(tags$b("Variety:"), " Heterogeneous message streams across SMS, Email, WhatsApp, and VoIP."),
              tags$li(tags$b("Distributed Storage:"), " Hadoop HDFS (/HadoopProject/input & /HadoopProject/output)."),
              tags$li(tags$b("Distributed Engine:"), " Apache Spark PySpark DataFrames and Spark MLlib.")
            )
          )
        )
      ),
      
      # ------------------------------------------------------------------------
      # TAB 2: DATASET OVERVIEW
      # ------------------------------------------------------------------------
      tabItem(
        tabName = "dataset",
        fluidRow(
          box(
            width = 12,
            title = "Raw Communication Dataset Information",
            status = "primary",
            solidHeader = TRUE,
            p("The dataset consists of multi-channel communications stored in Hadoop HDFS at /HadoopProject/input/sample_communications.csv. Below is the schema specification and sample data preview.")
          )
        ),
        fluidRow(
          box(
            width = 4,
            title = "Dataset Schema Description",
            status = "info",
            tags$table(
              class = "table table-bordered table-striped",
              tags$thead(tags$tr(tags$th("Field"), tags$th("Type"), tags$th("Description"))),
              tags$tbody(
                tags$tr(tags$td("message_id"), tags$td("String"), tags$td("Unique ID (e.g. MSG10001)")),
                tags$tr(tags$td("sender"), tags$td("String"), tags$td("Sender phone number / email")),
                tags$tr(tags$td("receiver"), tags$td("String"), tags$td("Recipient phone / email")),
                tags$tr(tags$td("message"), tags$td("String"), tags$td("Text content of message")),
                tags$tr(tags$td("timestamp"), tags$td("Datetime"), tags$td("Communication timestamp")),
                tags$tr(tags$td("communication_type"), tags$td("String"), tags$td("SMS, Email, WhatsApp, VoIP")),
                tags$tr(tags$td("label"), tags$td("String"), tags$td("Ground truth: Normal, Spam, Fraud"))
              )
            )
          ),
          box(
            width = 8,
            title = "Raw Communications Sample Data",
            status = "success",
            DT::dataTableOutput("table_raw_preview")
          )
        )
      ),
      
      # ------------------------------------------------------------------------
      # TAB 3: PREPROCESSING & SPARK PIPELINE
      # ------------------------------------------------------------------------
      tabItem(
        tabName = "preprocessing",
        fluidRow(
          box(
            width = 12,
            title = "Apache Spark Distributed Preprocessing Stage",
            status = "primary",
            solidHeader = TRUE,
            p("Spark applies scalable DataFrame transformations to sanitize text, handle missing values, and extract statistical features required for classification.")
          )
        ),
        fluidRow(
          box(
            width = 6,
            title = "Spark Transformations Pipeline",
            status = "info",
            tags$ol(
              tags$li(tags$b("Deduplication:"), " dropDuplicates(['message_id']) removes duplicate records."),
              tags$li(tags$b("Null Handling:"), " dropna() removes incomplete messages."),
              tags$li(tags$b("Text Normalization:"), " Lowercasing and regex special character removal."),
              tags$li(tags$b("Tokenization:"), " Splitting text into word tokens."),
              tags$li(tags$b("Stop Words Removal:"), " Filtering out common non-informative English stop words.")
            )
          ),
          box(
            width = 6,
            title = "Extracted Big Data Features",
            status = "warning",
            tags$ul(
              tags$li(tags$b("msg_length:"), " Total character length of message."),
              tags$li(tags$b("word_count:"), " Total word count after sanitization."),
              tags$li(tags$b("digit_count:"), " Number of digits present (bank accounts, OTPs, amounts)."),
              tags$li(tags$b("special_char_count:"), " Count of special characters (!, $, %, #, ?)."),
              tags$li(tags$b("has_url:"), " Binary flag (1/0) for phishing / promotional URLs."),
              tags$li(tags$b("suspicious_keyword_count:"), " Matches high-risk terms (urgent, bank, verify, kyc, won, otp).")
            )
          )
        )
      ),
      
      # ------------------------------------------------------------------------
      # TAB 4: SPAM & FRAUD ANALYSIS
      # ------------------------------------------------------------------------
      tabItem(
        tabName = "analysis",
        fluidRow(
          box(
            width = 12,
            title = "Spam & Fraud Identification Analysis",
            status = "primary",
            solidHeader = TRUE,
            p("Comparison between Spark MLlib classification on a held-out 20% test split and explainable rule-based threat identification.")
          )
        ),
        
        # Row 1: Spark ML Performance Metrics & Verification Info Box
        fluidRow(
          box(
            width = 6,
            title = "Spark ML Classification Model Performance",
            status = "success",
            solidHeader = TRUE,
            tableOutput("table_model_metrics"),
            p(tags$i("Calculated using PySpark MulticlassClassificationEvaluator strictly on held-out test predictions."))
          ),
          box(
            width = 6,
            title = "Model Evaluation Verification Info",
            status = "warning",
            solidHeader = TRUE,
            tags$ul(
              tags$li(tags$b("Evaluation Method:"), " 80 / 20 Train-Test Split"),
              tags$li(tags$b("Test Set Split:"), " 20% held-out unseen test data (~600 records)"),
              tags$li(tags$b("Training Set Split:"), " 80% training data (~2,400 records)"),
              tags$li(tags$b("Random Seed:"), " 42 (Deterministic reproducibility)"),
              tags$li(tags$b("Model Architecture:"), " Spark MLlib Logistic Regression Classifier"),
              tags$li(tags$b("Evaluation Engine:"), " PySpark MulticlassClassificationEvaluator"),
              tags$li(tags$b("Evaluation Note:"), " High feature separability is observed due to structured pattern alignment in the synthetic dataset templates.")
            )
          )
        ),
        
        # Row 2: Test Set Confusion Matrix & Explainable Rule Matrix
        fluidRow(
          box(
            width = 6,
            title = "Test Set Confusion Matrix (Actual vs Predicted)",
            status = "info",
            solidHeader = TRUE,
            tableOutput("table_confusion_matrix"),
            p(tags$i("Breakdown of Actual vs Predicted test samples for NORMAL, SPAM, and FRAUD."))
          ),
          box(
            width = 6,
            title = "Explainable Rule Classification Matrix",
            status = "primary",
            solidHeader = TRUE,
            tags$table(
              class = "table table-bordered",
              tags$thead(tags$tr(tags$th("Category"), tags$th("Rule Criteria"))),
              tags$tbody(
                tags$tr(tags$td(tags$span(class = "badge-fraud", "FRAUD")), tags$td("URL Flag == 1 AND Suspicious Keywords >= 2 AND Digit Count >= 3")),
                tags$tr(tags$td(tags$span(class = "badge-spam", "SPAM")), tags$td("URL Flag == 1 OR Suspicious Keywords >= 2 OR Repeated Chars == 1")),
                tags$tr(tags$td(tags$span(class = "badge-normal", "NORMAL")), tags$td("All standard communications meeting zero threat criteria"))
              )
            )
          )
        )
      ),
      
      # ------------------------------------------------------------------------
      # TAB 5: VISUALIZATIONS
      # ------------------------------------------------------------------------
      tabItem(
        tabName = "visualizations",
        fluidRow(
          box(
            width = 6,
            title = "Communication Classification Breakdown",
            status = "primary",
            plotOutput("plot_classification_breakdown", height = 300)
          ),
          box(
            width = 6,
            title = "Communication Channel Distribution",
            status = "info",
            plotOutput("plot_comm_type", height = 300)
          )
        ),
        fluidRow(
          box(
            width = 6,
            title = "Top Suspicious Keywords Frequency",
            status = "warning",
            plotOutput("plot_top_keywords", height = 320)
          ),
          box(
            width = 6,
            title = "Message Velocity Trend Over Time",
            status = "success",
            plotOutput("plot_velocity_trend", height = 320)
          )
        )
      ),
      
      # ------------------------------------------------------------------------
      # TAB 6: PROCESSED DATA TABLE
      # ------------------------------------------------------------------------
      tabItem(
        tabName = "processed_data",
        fluidRow(
          box(
            width = 12,
            title = "Interactive Processed Communications Explorer",
            status = "primary",
            solidHeader = TRUE,
            fluidRow(
              column(4, selectInput("filter_label", "Filter by Classification:", choices = c("All", "Normal", "Spam", "Fraud"), selected = "All")),
              column(4, selectInput("filter_comm_type", "Filter by Communication Type:", choices = c("All", "SMS", "Email", "WhatsApp", "VoIP"), selected = "All"))
            ),
            hr(),
            DT::dataTableOutput("table_processed_data")
          )
        )
      ),
      
      # ------------------------------------------------------------------------
      # TAB 7: REAL-TIME COMMUNICATION DETECTION
      # ------------------------------------------------------------------------
      tabItem(
        tabName = "detection",
        fluidRow(
          box(
            width = 12,
            title = "Message Communication Detection",
            status = "primary",
            solidHeader = TRUE,
            p("Fraud detection and feature extraction interface for evaluating individual messages against the project's Big Data spam and fraud identification rules.")
          )
        ),
        fluidRow(
          box(
            width = 12,
            title = "Communication Detection",
            status = "info",
            solidHeader = TRUE,
            textAreaInput(
              "detection_input",
              label = "Enter Communication Message:",
              value = "",
              placeholder = "Enter an SMS, email, WhatsApp message, or other communication...",
              rows = 4,
              width = "100%"
            ),
            div(
              style = "margin-top: 10px;",
              actionButton("btn_detect", "Detect Communication", icon = icon("search"), class = "btn-primary", style = "margin-right: 10px; font-weight: 600;"),
              actionButton("btn_clear", "Clear", icon = icon("eraser"), class = "btn-default", style = "font-weight: 600;")
            )
          )
        ),
        uiOutput("detection_results_ui")
      ),
      
      # ------------------------------------------------------------------------
      # TAB 8: ABOUT PROJECT
      # ------------------------------------------------------------------------
      tabItem(
        tabName = "about",
        fluidRow(
          box(
            width = 12,
            title = "Project Overview & Specifications",
            status = "primary",
            solidHeader = TRUE,
            h3("A Big Data Framework for Spam & Fraud Detection"),
            h5("Big Data Course Project"),
            hr(),
            p(tags$b("Project Purpose:"), " Designed to process and analyze large-scale multi-channel communication datasets to detect unsolicited spam and high-risk fraudulent communications (e.g., bank phishing, OTP theft, lottery scams)."),
            p(tags$b("Communication Channels Supported:"), " SMS, Email, WhatsApp, VoIP.")
          )
        ),
        fluidRow(
          box(
            width = 6,
            title = "Technology Stack",
            status = "info",
            solidHeader = TRUE,
            tags$ul(
              tags$li(tags$b("Hadoop HDFS:"), " Distributed File System for fault-tolerant storage (/HadoopProject/input/ & /HadoopProject/output/)."),
              tags$li(tags$b("Apache Spark / PySpark:"), " Distributed in-memory data processing, text sanitization, and feature extraction."),
              tags$li(tags$b("Spark MLlib:"), " Scalable Logistic Regression classification engine."),
              tags$li(tags$b("R Shiny & ggplot2:"), " Interactive dashboard presentation layer.")
            )
          ),
          box(
            width = 6,
            title = "End-to-End Data Processing Pipeline",
            status = "warning",
            solidHeader = TRUE,
            tags$ol(
              tags$li("Raw Communication Dataset Ingestion"),
              tags$li("Hadoop HDFS Storage Sync (/HadoopProject/input/)"),
              tags$li("Apache Spark Distributed DataFrame Preprocessing"),
              tags$li("Feature Extraction (Lengths, Digits, URLs, Keywords)"),
              tags$li("Rule Engine & Spark MLlib Classification"),
              tags$li("Big Data Aggregations & CSV Results Generation"),
              tags$li("Interactive R Shiny Dashboard Visualization")
            )
          )
        ),
        fluidRow(
          box(
            width = 12,
            title = "Big Data Concepts Demonstrated",
            status = "success",
            solidHeader = TRUE,
            tags$ul(
              tags$li(tags$b("Volume:"), " Large-scale communication records stored and queried across distributed blocks."),
              tags$li(tags$b("Velocity:"), " High-throughput real-time tracking of message volume trends and hourly spikes."),
              tags$li(tags$b("Variety:"), " Heterogeneous text streams across SMS, Email, WhatsApp, and VoIP channels."),
              tags$li(tags$b("Distributed Storage:"), " Hadoop HDFS distributed file system."),
              tags$li(tags$b("Distributed Processing:"), " Apache Spark PySpark DataFrames & Catalyst Optimizer."),
              tags$li(tags$b("Machine Learning:"), " Spark MLlib Logistic Regression classification."),
              tags$li(tags$b("Data Visualization:"), " Interactive decoupled R Shiny dashboard.")
            )
          )
        )
      )
    )
  )
)
