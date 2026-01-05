## Workflow Pharmacy Machine Learning Engineer end-to-end Mini Project 💊
![alt text](images/84B05E67-588D-474E-9AFD-3B599294806A.png)

This project are integrating Between Machine learning Engineer and Data engineer.

Business solution for complicated busineess in real world are using Artificial Intelligence by using ML Engineer and Data Engineer workflow set.

## Project purpose 🎯

- Increase sales revenue
- Reduced outlier distribution which made the company loss
- Define profit in growth sales
- Handle risk management and risk score
- Fraud detection in customer about the transaction

## Log project progress 👨‍💻

- Build structural workflow project in miro using tools
    - PostgreSQL
    - Kafka
    - Airflow
    - Minio
    - Machine learning alogrithm
    - Streamlit
- Ingest SQL database from big Pharmacy Sales project including SCHEMA raw, features, labels.
- Building Kafka producer and consumer for ingesting kafka data topic
- ETL airflow connection to kafka and ingesting to MinIO storage
- Build ML Engineer training data from gold kafka already data cleaned for catching
- Fundamental reliable, reproducible, and compatible for MLOps are
    
    **Infra → Data → Streaming → Orchestration → Feature → ML → Analytics**
    
- Run producer in kafka first for receiving data in kafka consumer
- Kafka has received a lot rows data from kafka producer
    
    ![alt text](images/1E55B2FA-4424-4654-B99B-F7858B430695_4_5005_c.jpeg)
    
- Integrating airflow dags about 2 data list airflow/dags/sales_feature_pipeline.py and airflow/dags/sales_feature_pipeline_v2.py
- Success integrating airflow for sales_feature_pipeline
- Kafka to Bronze
    
    ![alt text](images/804216AF-EEEC-4813-BCED-5C3BDE11EC10.png)
    
- Kafka Bronze to Silver
    
    ![alt text](images/2EFE6C81-B250-444E-8198-0E790A9CDE86.png)
    
- Kafka Silver to Gold
    
    ![alt text](images/A060B3FA-9599-41CD-88BC-25C1868C0A10.png)
    
- Ingesting data from airflow in scheduling ETL was ingested to MinIO
- Bronze data transfered
    
    ![alt text](images/50532F3C-5276-48E1-9B60-53792019FF82.png)
    
- Silver data transfered
    
    ![alt text](images/29C60C6E-A53F-4E59-9C3E-3BFE1136C601.png)
    
- Gold data transfered
    
    ![alt text](images/5CBF82A4-4573-4B20-9FDF-D2B60FCCEE9F.png)
    
- Analytics data transfered
    
    ![alt text](images/F5F6EED4-718C-4CA5-A5FC-5045852DC2D8.png)