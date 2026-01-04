import boto3
import logging
import os

# --- CONFIG (UPDATE THESE THREE VALUES) ---
AWS_ACCESS_KEY = "Upload your access key here"  # <-- REQUIRED
AWS_SECRET_KEY = "Upload your secret key here"  # <-- REQUIRED
S3_BUCKET = "sp500-data-lake-project"  # <-- REQUIRED: Use your actual bucket name
AWS_REGION = "us-east-1" # <-- Change this to your region (e.g., 'eu-west-1')
S3_PREFIX = "sp500/"
# ------------------------------------------

logging.basicConfig(level=logging.INFO)

def upload_to_s3(local_file_path: str):
    """
    Uploads a local file to S3 using explicitly defined credentials.
    """
    if not os.path.exists(local_file_path):
        logging.error(f"Local file not found: {local_file_path}")
        return

    key = f"{S3_PREFIX}raw/{os.path.basename(local_file_path)}"
    
    try:
        # Initialize Boto3 S3 client using inline credentials
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=AWS_REGION
        )
        
        logging.info(f"Starting upload of {local_file_path} to s3://{S3_BUCKET}/{key}...")
        
        # Upload the file
        s3_client.upload_file(local_file_path, S3_BUCKET, key)
        
        s3_url = f"s3://{S3_BUCKET}/{key}"
        logging.info(f"Upload successful! S3 Path: {s3_url}")
        return s3_url
        
    except Exception as e:
        logging.error(f"Failed to upload to S3: {e}")
        return None

# Example usage (Replace the placeholder with the actual file path from Milestone 2)
# You must get the exact full path of the CSV file created in the previous step.
raw_csv_path = "D:/CDE Learnings Journey/SMIT CDE LEARNING B2 2025/Project 08 - SP500 Stock Data Orchestration using Airflow/sp500_raw_20251118_231424.csv" # <-- **UPDATE THIS PATH**

if raw_csv_path.startswith("D:"): # Check if the path looks real
    upload_to_s3(raw_csv_path)
else:
    logging.warning("Please update raw_csv_path with the actual file path and provide AWS credentials.")