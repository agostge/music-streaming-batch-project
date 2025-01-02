# Music Streaming Data Pipeline

## Introduction

This project simulates the processing of fake music streaming data. It generates data, stores it in MinIO storage, performs analytics using pandas, and is orchestrated by Apache Airflow. The processed results are then uploaded into a PostgreSQL database. The purpose of this project is to showcase a batch processing pipeline for handling datasets in a scalable and fault-tolerant manner.


![Flowchart Description](https://github.com/agostge/music-streaming-batch-project/blob/main/assets/flowchart.png)




## Tech Stack
- **Airflow**: Data orchestration & transformation
- **MinIO**: Storage layer (compatible with Amazon S3)
- **PostgreSQL**: Data warehouse for storage and reporting

## Pipeline Overview

1. **Data Generation:**
   - The project uses fake data representing music streaming activity. The `scripts/generate_data.py` script generates fake music streaming data using the Faker Python package. You can run the script manually to create a batch of fake data, which is then stored locally as CSV files:
   
     ```bash
     python scripts/generate_data.py
     ```

2. **Upload Data to MinIO:**
   - The generated files are uploaded to MinIO, an object storage system that is S3-compatible. You can upload the files using the following script:
   
     ```bash
     python scripts/upload_to_minio.py
     ```

3. **Data Orchestration with Apache Airflow:**
   - Apache Airflow is used to orchestrate the entire ETL process. The pipeline's DAG (Directed Acyclic Graph) will:
     - Pull data from MinIO object storage.
     - Perform basic analytics using pandas (e.g., KPIs based on genre and hourly listens).
     - Load the results into a PostgreSQL database for further analysis.

4. **Basic Analytics:**
   - The project performs simple analytics using pandas, such as calculating KPIs based on genre and hourly listens.

5. **Upload to PostgreSQL:**
   - After transformation, the data is uploaded into a PostgreSQL database for potential further analysis and reporting.

## Requirements
- Docker
- Python 3.x with the required packages installed (see `requirements.txt`)

## Environment Setup

1. **Build the Docker Image:**
   - Navigate to the project folder and build the necessary containers (MinIO and PostgreSQL) using Docker Compose:
   
     ```bash
     docker compose up -d
     ```

   - This will start the MinIO object storage service and the PostgreSQL database, both with their respective UIs.

2. **Set Up Python Virtual Environment:**
   - Create and activate a Python virtual environment:
   
     ```bash
     python -m venv <environment_name>
     ```
   
   - For Linux/macOS:
   
     ```bash
     source <environment_name>/bin/activate
     ```

   - For Windows:
   
     ```bash
     <environment_name>\Scripts\activate
     ```

3. **Install Dependencies:**
   - Install the required Python dependencies:
   
     ```bash
     pip install -r requirements.txt
     ```

4. **Set Up and Start Apache Airflow:**
   - Initialize the Airflow database and start the webserver and scheduler:
   
     ```bash
     airflow db init
     airflow webserver -p 8080
     airflow scheduler
     ```

## Run Instructions

1. **Generate Fake Data:**
   - Generate the data by running the following script:
   
     ```bash
     python scripts/generate_data.py
     ```

2. **Upload Data to MinIO Storage:**
   - Upload the resulting CSV files to MinIO:
   
     ```bash
     python scripts/upload_to_minio.py
     ```

3. **Trigger the Airflow DAG:**
   - The DAG can be triggered manually either via the Airflow UI or using the following command:
   
     ```bash
     airflow dags trigger dag_song_kpi_calculations
     ```
