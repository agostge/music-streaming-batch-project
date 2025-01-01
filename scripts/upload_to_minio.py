import os
import boto3
from botocore.exceptions import NoCredentialsError

# Set up MinIO client with Boto3
def create_minio_client():
    s3_client = boto3.client('s3',
                             endpoint_url='http://localhost:9000',  # MinIO endpoint URL
                             aws_access_key_id='minioadmin',
                             aws_secret_access_key='minioadmin',
                             region_name='us-east-1')  # Adjust region if needed
    return s3_client

# Upload a file to MinIO
def upload_file_to_minio(file_path, bucket_name, object_name):
    s3_client = create_minio_client()
    
    try:
        # Upload the file
        s3_client.upload_file(file_path, bucket_name, object_name)
        print(f"File {file_path} uploaded to {bucket_name}/{object_name}")
    except NoCredentialsError:
        print("Credentials not available.")
    except Exception as e:
        print(f"Error uploading file: {e}")

# Upload multiple CSV files from a folder to MinIO
def upload_csv_files_from_folder(folder_path, bucket_name):
    # Get all CSV files from the folder
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.csv'):
            file_path = os.path.join(folder_path, file_name)
            # Upload each CSV file with the same file name in MinIO bucket
            upload_file_to_minio(file_path, bucket_name, file_name)

if __name__ == '__main__':
    folder_path = '/home/karesz/Desktop/data-engineering-practice-main/music_streaming_batch_project/data/raw'  # Path to your folder containing CSV files
    bucket_name = 'streaming-data'  # Your MinIO bucket name
    
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    if len(csv_files) == 3:
        upload_csv_files_from_folder(folder_path, bucket_name)
    else:
        print(f"There should be exactly 3 CSV files in the folder. Found {len(csv_files)}.")
