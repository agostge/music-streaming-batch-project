
Introduction


This project simulates the processing of fake music streaming data. It generates data, stores it in MinIO storage, performs analytics using pandas, orchestrated by Apache Airflow, and uploads the results into a PostgreSQL database. The purpose of this project is to showcase a batch processing pipeline for handling datasets in a scalable, fault-tolerant manner.


Tech Stack


Airflow - Data orchestration & Transformation
MinIO - Storage Layer
Postgres - Data Warehouse


Pipeline




1. The project uses fake data representing music streaming activity. The scripts/generate_data.py script generates fake music streaming data using the faker Python package. The following script can be run manually to create a batch of fake data, then store it locally in the form of CSV files:

python scripts/generate_data.py


2. The generated files are then uploaded to MinIO, an object storage system, compatible with Amazon S3 using the following script.

python scripts/upload_to_minio.py


3. Apache Airflow is used to orchestrate the entire ETL  process. The DAG used in the pipeline will pull data from the object storage, perform some basic analysis on it, then load it into a postgres database.


4. The project performs basic analytics using pandas, such as different KPIs based on genre and hourly listens.



5. The transformed data is uploaded into PostgreSQL for potential further analysis and reporting.


Requirements


Docker 
Python with the requirements installed

Environment Setup


1. Build the docker image using docker compose file by navigating to the project folder and running the following command

docker compose up -d

This will create the necessary MinIO object storage and the Postgres database with a UI.

2. Create a python virtual environment 

python -m venv <environment_name>

source <environment_name>/bin/activate

3. Install the required dependencies

pip install -r requirements.txt

4. Set up and start Airflow with the following commands:

airflow db init
airflow webserver -p 8080
airflow scheduler



Run Instructions


1. Generate the data by running the following script:

python scripts/generate_data.py

2. Upload the resulting files to the S3 storage

python scripts/upload_to_minio.py

3. The DAG can be triggered manually via the UI or using the following command:

airflow dags trigger dag_song_kpi_calculations.py
