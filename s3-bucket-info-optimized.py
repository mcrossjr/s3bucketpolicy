#!/usr/bin/env python3
import boto3
import datetime
import csv
import os
import sys
import time

def format_size(size_in_bytes):
    """
    Convert bytes to human-readable format without external dependencies
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if size_in_bytes < 1024.0 or unit == 'PB':
            break
        size_in_bytes /= 1024.0
    return f"{size_in_bytes:.2f} {unit}"

def get_bucket_size_sample(bucket_name, sample_limit=1000):
    """
    Estimate bucket size based on a sample of objects to avoid memory issues
    """
    s3_client = boto3.client('s3')
    total_size = 0
    object_count = 0
    continuation_token = None
    
    try:
        # Use pagination to avoid loading all objects into memory at once
        while True:
            if continuation_token:
                response = s3_client.list_objects_v2(
                    Bucket=bucket_name, 
                    MaxKeys=sample_limit,
                    ContinuationToken=continuation_token
                )
            else:
                response = s3_client.list_objects_v2(
                    Bucket=bucket_name, 
                    MaxKeys=sample_limit
                )
                
            # Process objects
            if 'Contents' in response:
                for obj in response['Contents']:
                    total_size += obj['Size']
                    object_count += 1
            
            # Check if there are more objects
            if not response.get('IsTruncated'):
                break
                
            continuation_token = response.get('NextContinuationToken')
            
            # Optional: Sleep briefly to avoid API throttling
            time.sleep(0.1)
            
        return total_size, object_count
    except Exception as e:
        print(f"Error getting size for bucket {bucket_name}: {str(e)}")
        return 0, 0

def get_latest_objects(bucket_name, count=3):
    """
    Get the latest objects from a bucket without loading all objects into memory
    """
    s3_client = boto3.client('s3')
    latest_objects = []
    
    try:
        # List objects sorted by date (newest first)
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            MaxKeys=100  # Fetch enough to likely include newest objects
        )
        
        if 'Contents' in response:
            # Sort objects by last modified date (newest first)
            sorted_objects = sorted(
                response['Contents'],
                key=lambda obj: obj['LastModified'],
                reverse=True
            )
            
            # Get the top 'count' objects
            for obj in sorted_objects[:count]:
                latest_objects.append({
                    'key': obj['Key'],
                    'size': format_size(obj['Size']),
                    'size_bytes': obj['Size'],
                    'last_modified': obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
                })
        
        # Ensure we have exactly 'count' items
        while len(latest_objects) < count:
            latest_objects.append({
                'key': "N/A",
                'size': "N/A",
                'size_bytes': 0,
                'last_modified': "N/A"
            })
            
        return latest_objects
    except Exception as e:
        print(f"Error getting latest objects for bucket {bucket_name}: {str(e)}")
        # Return placeholder objects on error
        return [{'key': "Error", 'size': "Error", 'size_bytes': 0, 'last_modified': "Error"} for _ in range(count)]

def get_access_times(bucket_name, count=3):
    """
    Get the last access times for a bucket
    """
    access_times = []
    
    try:
        cloudtrail = boto3.client('cloudtrail')
        
        # Look up the last 'count' access events
        events = cloudtrail.lookup_events(
            LookupAttributes=[
                {
                    'AttributeKey': 'ResourceName',
                    'AttributeValue': bucket_name
                }
            ],
            MaxResults=count
        )
        
        for event in events.get('Events', []):
            access_times.append(event['EventTime'].strftime('%Y-%m-%d %H:%M:%S'))
        
    except Exception as e:
        # CloudTrail lookup might fail if not enabled
        access_times = ["CloudTrail access info not available"]
    
    # Ensure we have exactly 'count' items
    while len(access_times) < count:
        access_times.append("N/A")
        
    return access_times

def process_buckets_and_write_csv(csv_filename):
    """
    Process each bucket individually and write directly to CSV to avoid memory issues
    """
    s3_client = boto3.client('s3')
    
    # Get all bucket names
    response = s3_client.list_buckets()
    buckets = [bucket['Name'] for bucket in response['Buckets']]
    
    # Initialize CSV file
    with open(csv_filename, 'w', newline='') as csvfile:
        fieldnames = [
            'name', 'size', 'size_bytes', 'object_count',
            'access_time_1', 'access_time_2', 'access_time_3',
            'latest_object_1_key', 'latest_object_1_size', 'latest_object_1_date',
            'latest_object_2_key', 'latest_object_2_size', 'latest_object_2_date',
            'latest_object_3_key', 'latest_object_3_size', 'latest_object_3_date'
        ]
        
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        # Process each bucket individually
        for bucket_name in buckets:
            print(f"Processing bucket: {bucket_name}")
            
            try:
                # Get bucket size and object count
                total_size, object_count = get_bucket_size_sample(bucket_name)
                
                # Get latest objects
                latest_objects = get_latest_objects(bucket_name, 3)
                
                # Get access times
                access_times = get_access_times(bucket_name, 3)
                
                # Create bucket info
                bucket_info = {
                    'name': bucket_name,
                    'size': format_size(total_size),
                    'size_bytes': total_size,
                    'object_count': object_count,
                    'access_time_1': access_times[0],
                    'access_time_2': access_times[1],
                    'access_time_3': access_times[2],
                    'latest_object_1_key': latest_objects[0]['key'],
                    'latest_object_1_size': latest_objects[0]['size'],
                    'latest_object_1_date': latest_objects[0]['last_modified'],
                    'latest_object_2_key': latest_objects[1]['key'],
                    'latest_object_2_size': latest_objects[1]['size'],
                    'latest_object_2_date': latest_objects[1]['last_modified'],
                    'latest_object_3_key': latest_objects[2]['key'],
                    'latest_object_3_size': latest_objects[2]['size'],
                    'latest_object_3_date': latest_objects[2]['last_modified']
                }
                
                # Print summary to console
                print(f"  Size: {bucket_info['size']} ({object_count} objects)")
                print(f"  Latest object: {bucket_info['latest_object_1_key']}")
                print(f"  Last access: {bucket_info['access_time_1']}")
                print("-" * 40)
                
                # Write to CSV immediately
                writer.writerow(bucket_info)
                csvfile.flush()  # Force write to disk
                
            except Exception as e:
                print(f"Error processing bucket {bucket_name}: {str(e)}")
                
                # Write error entry to CSV
                error_info = {
                    'name': bucket_name,
                    'size': "Error retrieving data",
                    'size_bytes': 0,
                    'object_count': 0,
                    'access_time_1': "Error",
                    'access_time_2': "Error",
                    'access_time_3': "Error",
                    'latest_object_1_key': "Error",
                    'latest_object_1_size': "Error",
                    'latest_object_1_date': "Error",
                    'latest_object_2_key': "Error",
                    'latest_object_2_size': "Error",
                    'latest_object_2_date': "Error",
                    'latest_object_3_key': "Error",
                    'latest_object_3_size': "Error",
                    'latest_object_3_date': "Error"
                }
                writer.writerow(error_info)
                csvfile.flush()  # Force write to disk
            
            # Free memory
            del latest_objects
            del access_times
            
    return os.path.abspath(csv_filename)

def main():
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"s3_bucket_info_{timestamp}.csv"
    
    print("\n" + "="*50)
    print("S3 BUCKET INFORMATION GATHERING")
    print("="*50 + "\n")
    print("This script will process all S3 buckets and export information to CSV.")
    print("Large buckets will be sampled to avoid memory issues.\n")
    
    # Process buckets and write directly to CSV
    csv_path = process_buckets_and_write_csv(csv_filename)
    
    print("\n" + "="*50)
    print("PROCESS COMPLETED")
    print("="*50)
    print(f"\nCSV export saved as: {csv_path}")

if __name__ == "__main__":
    main()
