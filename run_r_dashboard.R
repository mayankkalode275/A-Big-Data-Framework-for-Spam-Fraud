# Helper R Script to Launch the Big Data Spam & Fraud Shiny Dashboard
# Usage: Rscript run_r_dashboard.R

if (!require("shiny")) install.packages("shiny")
if (!require("shinydashboard")) install.packages("shinydashboard")
if (!require("ggplot2")) install.packages("ggplot2")
if (!require("dplyr")) install.packages("dplyr")
if (!require("readr")) install.packages("readr")
if (!require("DT")) install.packages("DT")

message("======================================================================")
message(" Launching Mumbai University Big Data Spam & Fraud Dashboard")
message(" Open your browser at: http://127.0.0.1:8080")
message("======================================================================")

shiny::runApp("r_dashboard", port = 8080, launch.browser = TRUE)
