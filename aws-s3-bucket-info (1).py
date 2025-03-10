import boto3
from datetime import datetime
import humanize
import csv
import os

def get_s3_bucket_info():
    """
    Lists all S3 buckets with their size, and last modified file information.
    
    Returns a list of dictionaries with bucket information including:
    - Bucket name
    - Total size (bytes)
    - Total size (human readable)
    - Last modified file name
    - Last modified date
    """
    # Initialize S3 client
    s3_client = boto3.client('s3')
    
    # Get list of all buckets
    response = s3_client.list_buckets()
    
    # Store results
    results = []
    
    # Process each bucket
    for bucket in response['Buckets']:
        bucket_name = bucket['Name']
        print(f"Processing bucket: {bucket_name}")
        
        try:
            # Variables to track bucket stats
            total_size = 0
            last_modified_date = None
            last_modified_file = None
            
            # List all objects in the bucket
            paginator = s3_client.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(Bucket=bucket_name):
                # Skip if bucket is empty
                if 'Contents' not in page:
                    continue
                    
                for obj in page['Contents']:
                    # Add to total size
                    total_size += obj['Size']
                    
                    # Check if this is the most recently modified file
                    if last_modified_date is None or obj['LastModified'] > last_modified_date:
                        last_modified_date = obj['LastModified']
                        last_modified_file = obj['Key']
            
            # Format the results
            bucket_info = {
                'bucket_name': bucket_name,
                'total_size_bytes': total_size,
                'total_size_human': humanize.naturalsize(total_size),
                'last_modified_file': last_modified_file if last_modified_file else 'N/A',
                'last_modified_date': last_modified_date.strftime('%Y-%m-%d %H:%M:%S') if last_modified_date else 'N/A'
            }
            
            results.append(bucket_info)
            
        except Exception as e:
            print(f"Error processing bucket {bucket_name}: {str(e)}")
            # Add bucket with error information
            results.append({
                'bucket_name': bucket_name,
                'total_size_bytes': 0,
                'total_size_human': 'Error',
                'last_modified_file': 'Error',
                'last_modified_date': f'Error: {str(e)}'
            })
    
    return results

def write_to_csv(bucket_info, filename='s3_bucket_info.csv'):
    """
    Writes the bucket information to a CSV file.
    
    Args:
        bucket_info: List of dictionaries containing bucket data
        filename: Name of the output CSV file
    """
    # Order the buckets by total size (descending)
    ordered_bucket_info = sorted(bucket_info, key=lambda x: x['total_size_bytes'], reverse=True)
    
    # Define CSV headers
    headers = ['Bucket Name', 'Total Size', 'Last Modified Date', 'Last Modified File']
    
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        
        for bucket in ordered_bucket_info:
            writer.writerow([
                bucket['bucket_name'],
                bucket['total_size_human'],
                bucket['last_modified_date'],
                bucket['last_modified_file']
            ])
    
    # Print the absolute path of the created file
    abs_path = os.path.abspath(filename)
    print(f"CSV file created successfully: {abs_path}")
    return abs_path

def main():
    """Main function to run the script and output results to CSV."""
    print("Retrieving S3 bucket information...")
    bucket_info = get_s3_bucket_info()
    
    # Write the results to a CSV file and get the file path
    csv_path = write_to_csv(bucket_info)
    
    print(f"Script completed. S3 bucket information has been saved to: {csv_path}")

if __name__ == "__main__":
    main()
