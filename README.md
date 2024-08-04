YouTube ETL Pipeline

This project demonstrates an ETL (Extract, Transform, Load) pipeline that extracts data from YouTube using Python, runs the script using Airflow on Amazon EC2, and loads the data into AWS S3.

The primary goal of this project is to extract data from a YouTube channel, process it, and store it in AWS S3. This pipeline automates the extraction of channel statistics and video details, and schedules the process using Airflow on an EC2 instance.

The Youtube_ETL.py script extracts data from the YouTube Data API and saves it to AWS S3.

The youtube_dag.py defines the DAG for running the YouTube ETL script.

Conclusion
This project showcases an automated ETL pipeline for extracting and processing YouTube data, scheduled and managed by Airflow on an EC2 instance, with data storage in AWS S3. This setup can be extended to include more data sources and processing steps as needed.
