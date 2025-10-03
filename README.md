📊 End-to-End Data Pipeline on GCP


📝 Problem Statement:

Banks often face challenges in managing transaction adjustments due to issues like duplicate entries, processing errors, and fraudulent activities. With high transaction volumes flowing in from multiple sources, the lack of a unified data platform makes it difficult to gain a comprehensive view of customer transactions. This limitation prevents banks from effectively analyzing both historical and real-time data, thereby restricting their ability to generate predictive insights, manage risks, and optimize product development. A robust Data Lake architecture can centralize data handling, support real-time access, and enable advanced analytics to drive better decision-making and business growth.

💡 Solution:

This project implements an end-to-end data pipeline on Google Cloud Platform (GCP) to address these challenges. The pipeline ingests data from multiple sources, applies incremental and CDC-based processing, and manages historical data with SCD Type 1 & Type 2 approaches. Orchestration is handled using Cloud Composer (Airflow), ensuring seamless scheduling and data flow, while curated datasets are made available for advanced analytics and visualization in Power BI.


### Architecture Diagram
![Archi](https://github.com/shruthilakshmii/Banking-end-to-end-pipeline-in-gcp/blob/main/Banking%20ETL.png)

🔹 Features:

---  Data ingestion from Cloud SQL, REST APIs, and GCS

---  Supports incremental ingestion, CDC, and partitioning

---  Raw and curated layers built in BigQuery with SCD Type 1 & Type 2 handling

---  Dataproc (Spark)  for scalable ingestion and transformation

---  Cloud Composer (Airflow) for orchestration and automation

--- Final datasets integrated with Power BI for reporting and insights
