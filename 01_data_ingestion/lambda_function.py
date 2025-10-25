import os
import json
import zipfile
import boto3

# Set Kaggle config dir BEFORE import
os.environ['KAGGLE_CONFIG_DIR'] = '/tmp'

from kaggle.api.kaggle_api_extended import KaggleApi


def lambda_handler(event, context):
    # Environment variables
    kaggle_username = os.environ['KAGGLE_USERNAME']
    kaggle_key = os.environ['KAGGLE_KEY']
    s3_bucket = os.environ['S3_BUCKET']

    # Write kaggle.json to /tmp
    kaggle_config = {
        "username": kaggle_username,
        "key": kaggle_key
    }
    with open('/tmp/kaggle.json', 'w') as f:
        json.dump(kaggle_config, f)

    # Initialize Kaggle API
    api = KaggleApi()
    api.authenticate()

    # Dataset and download path
    dataset = 'ukveteran/adventure-works'  
    download_path = '/tmp/adventure-works.zip'
    extract_path = '/tmp/adventure-works_data'

    # Step 1: Download dataset
    print("Downloading dataset from Kaggle...")
    api.dataset_download_files(dataset, path='/tmp', unzip=False)

    # Step 2: Unzip
    print("Extracting files...")
    os.makedirs(extract_path, exist_ok=True)
    with zipfile.ZipFile(download_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)

    # Step 3: Fix encoding and upload to S3
    print("Processing and uploading files...")
    s3 = boto3.client('s3')
    
    for root, dirs, files in os.walk(extract_path):
        for filename in files:
            if filename.endswith('.csv'):
                local_path = os.path.join(root, filename)
                
                try:
                    # Try reading with different encodings
                    content = None
                    for encoding in ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252']:
                        try:
                            with open(local_path, 'r', encoding=encoding) as f:
                                content = f.read()
                            print(f"✓ {filename} read successfully with {encoding}")
                            break
                        except UnicodeDecodeError:
                            continue
                    
                    if content is None:
                        print(f"⚠ Could not decode {filename}, uploading as-is")
                        s3_key = f"raw/{filename}"
                        s3.upload_file(local_path, s3_bucket, s3_key)
                        continue
                    
                    # Remove BOM if present
                    if content.startswith('\ufeff'):
                        content = content[1:]
                        print(f"  Removed BOM from {filename}")
                    
                    # Write as clean UTF-8
                    clean_path = f'/tmp/clean_{filename}'
                    with open(clean_path, 'w', encoding='utf-8', newline='') as f:
                        f.write(content)
                    
                    # Upload to S3
                    s3_key = f"raw/{filename}"
                    s3.upload_file(clean_path, s3_bucket, s3_key)
                    print(f"✓ Uploaded {filename} (clean UTF-8)")
                    
                except Exception as e:
                    print(f"❌ Error processing {filename}: {str(e)}")
                    # Upload original if all else fails
                    s3_key = f"raw/{filename}"
                    s3.upload_file(local_path, s3_bucket, s3_key)

    return {
        "status": "success", 
        "message": "Dataset uploaded with clean UTF-8 encoding"
    }
