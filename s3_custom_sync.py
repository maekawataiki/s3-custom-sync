import os
import boto3
import argparse
import concurrent.futures
import hashlib
from datetime import datetime
import fnmatch

# Set up S3 client
s3 = boto3.client('s3')

# Parse command line arguments
parser = argparse.ArgumentParser()
parser.add_argument('--directory', help='the directory to search in')
parser.add_argument('--date', help='the date to check against in yyyy-mm-dd format')
parser.add_argument('--bucket', help='the name of the S3 bucket to upload to')
parser.add_argument('--prefix', help='the prefix to use for uploaded files in S3')
parser.add_argument('--threads', type=int, default=8, help='the number of threads to use for processing files (default: 8)')
parser.add_argument('--ignore', type=str, help='file containing a list of file paths to ignore')
args = parser.parse_args()

# Convert date string to datetime object
check_date = datetime.strptime(args.date, '%Y-%m-%d')

# Get list of files to ignore
ignore_files = []
if args.ignore:
    with open(args.ignore, 'r') as f:
        ignore_files = [line.strip() for line in f]

# Retrieve metadata for all files in the specified prefix at once
    s3_objects = s3.list_objects_v2(Bucket=args.bucket, Prefix=args.prefix)['Contents']
    s3_files = {}
    for obj in s3_objects:
        s3_files[obj['Key']] = obj['LastModified'].replace(tzinfo=None), obj['ETag'].replace('"', '')

# Function to process a batch of files in parallel
def process_files(files):
    # Check if each file was modified after the specified date and upload if necessary
    for file in files:
        # Check if file should be ignored
        if any(fnmatch.fnmatch(file, pattern) for pattern in ignore_files):
            continue
        mod_time = os.path.getmtime(file)
        if datetime.fromtimestamp(mod_time) > check_date:
            s3_key = os.path.join(args.prefix, file.replace(args.directory, '').lstrip('/'))
            file_hash = hashlib.md5(open(file, 'rb').read()).hexdigest()
            if s3_key in s3_files and s3_files[s3_key][0] >= datetime.fromtimestamp(mod_time) and s3_files[s3_key][1] == file_hash:
                print(f"{file} already exists in S3 and has the same hash. Skipping upload.")
                continue
            s3.upload_file(file, args.bucket, s3_key, ExtraArgs={'Metadata': {'hash': file_hash}})
            print(f"{file} uploaded to S3 as s3://{args.bucket}/{s3_key}")

# Retrieve a list of all files in the specified directory and its subdirectories
all_files = []
for root, dirs, files in os.walk(args.directory):
    for file in files:
        all_files.append(os.path.join(root, file))

# Split the list of files into batches and process each batch in parallel
batch_size = len(all_files) // args.threads + 1
file_batches = [all_files[i:i+batch_size] for i in range(0, len(all_files), batch_size)]

# Process each batch of files in parallel
with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as executor:
    futures = []
    for file_batch in file_batches:
        futures.append(executor.submit(process_files, file_batch))
    for future in concurrent.futures.as_completed(futures):
        future.result()
