#!/usr/bin/env python3
"""
S3 Bucket Cleanup Script
Deletes objects older than 90 days from a specified S3 bucket
Suitable for AWS CloudShell and Lambda deployment
Exports file details to CSV for audit purposes
"""

import boto3
from datetime import datetime, timezone, timedelta
import sys
import os
import csv
import io

def list_old_objects(bucket_name, days_threshold=90):
    """
    List objects older than specified days from an S3 bucket
    
    Args:
        bucket_name (str): Name of the S3 bucket
        days_threshold (int): Age threshold in days (default: 90)
        
    Returns:
        list: List of objects to be deleted
    """
    # Initialize S3 client
    s3_client = boto3.client('s3')
    
    # Calculate the cutoff date
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_threshold)
    
    # Objects to be deleted
    objects_to_delete = []
    
    try:
        # List all objects in the bucket
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket_name)
        
        print(f"Scanning bucket '{bucket_name}' for objects older than {days_threshold} days...")
        print(f"Cutoff date: {cutoff_date.strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
        
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    # Check if object is older than threshold
                    if obj['LastModified'] < cutoff_date:
                        objects_to_delete.append({
                            'Key': obj['Key'],
                            'LastModified': obj['LastModified'],
                            'Size': obj['Size']
                        })
        
        return objects_to_delete
        
    except Exception as e:
        print(f"Error accessing bucket '{bucket_name}': {str(e)}")
        sys.exit(1)

def export_to_csv(objects_to_delete, csv_filename=None):
    """
    Export the list of objects to a CSV file
    
    Args:
        objects_to_delete (list): List of objects to export
        csv_filename (str, optional): Name of CSV file. If None, a default name with timestamp will be created
        
    Returns:
        str: Path to the created CSV file
    """
    if not csv_filename:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_filename = f"s3_cleanup_{timestamp}.csv"
    
    try:
        with open(csv_filename, 'w', newline='') as csvfile:
            # Define CSV columns
            fieldnames = ['Object_Key', 'Last_Modified', 'Size_Bytes', 'Size_KB', 'Size_MB']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write header
            writer.writeheader()
            
            # Write data rows
            for obj in objects_to_delete:
                writer.writerow({
                    'Object_Key': obj['Key'],
                    'Last_Modified': obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S UTC'),
                    'Size_Bytes': obj['Size'],
                    'Size_KB': round(obj['Size'] / 1024, 2),
                    'Size_MB': round(obj['Size'] / (1024 * 1024), 4)
                })
        
        print(f"Exported list of objects to: {csv_filename}")
        return csv_filename
        
    except Exception as e:
        print(f"Error exporting to CSV: {str(e)}")
        return None

def export_csv_to_s3(s3_client, bucket_name, local_csv_path, prefix="cleanup_logs/"):
    """
    Upload the CSV to S3 (useful for Lambda execution)
    
    Args:
        s3_client: Boto3 S3 client
        bucket_name (str): S3 bucket name
        local_csv_path (str): Local path to CSV file
        prefix (str): Prefix for the S3 key
        
    Returns:
        str: S3 path where the CSV was uploaded
    """
    try:
        # Get just the filename from the path
        csv_filename = os.path.basename(local_csv_path)
        
        # Create S3 key with prefix
        s3_key = f"{prefix}{csv_filename}"
        
        # Upload to S3
        s3_client.upload_file(local_csv_path, bucket_name, s3_key)
        
        print(f"Uploaded CSV to s3://{bucket_name}/{s3_key}")
        return f"s3://{bucket_name}/{s3_key}"
        
    except Exception as e:
        print(f"Error uploading CSV to S3: {str(e)}")
        return None

def display_objects(objects_to_delete):
    """
    Display objects in a formatted table
    
    Args:
        objects_to_delete (list): List of objects to display
    """
    if not objects_to_delete:
        print("No objects found older than 90 days.")
        return
    
    # Calculate total size
    total_size_bytes = sum(obj['Size'] for obj in objects_to_delete)
    total_size_mb = total_size_bytes / (1024 * 1024)
    
    print(f"Found {len(objects_to_delete)} objects to delete (Total size: {total_size_mb:.2f} MB):\n")
    print("=" * 85)
    print(f"{'Object Key':<50} {'Last Modified':<25} {'Size (KB)':<10}")
    print("=" * 85)
    
    for obj in objects_to_delete:
        key = obj['Key']
        # Truncate long keys for display
        display_key = key if len(key) <= 50 else key[:47] + "..."
        last_modified = obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S UTC')
        size_kb = obj['Size'] / 1024
        print(f"{display_key:<50} {last_modified:<25} {size_kb:>9.2f}")
    
    print("=" * 85)

def delete_objects(s3_client, bucket_name, objects_to_delete):
    """
    Delete the specified objects from S3 bucket
    
    Args:
        s3_client: Boto3 S3 client
        bucket_name (str): Name of the S3 bucket
        objects_to_delete (list): List of objects to delete
        
    Returns:
        int: Number of successfully deleted objects
    """
    print("\nDeleting objects...")
    
    # Delete objects in batches (S3 allows max 1000 objects per delete request)
    batch_size = 1000
    deleted_count = 0
    
    for i in range(0, len(objects_to_delete), batch_size):
        batch = objects_to_delete[i:i + batch_size]
        delete_keys = [{'Key': obj['Key']} for obj in batch]
        
        try:
            response = s3_client.delete_objects(
                Bucket=bucket_name,
                Delete={
                    'Objects': delete_keys,
                    'Quiet': False
                }
            )
            
            # Count successful deletions
            if 'Deleted' in response:
                deleted_count += len(response['Deleted'])
            
            # Report any errors
            if 'Errors' in response:
                for error in response['Errors']:
                    print(f"Error deleting {error['Key']}: {error['Message']}")
            
        except Exception as e:
            print(f"Error during batch deletion: {str(e)}")
    
    return deleted_count

def main(interactive=True):
    """
    Main function
    
    Args:
        interactive (bool): Whether to run in interactive mode (ask for confirmation)
    """
    # S3 bucket name
    bucket_name = os.environ.get('S3_BUCKET_NAME', 'testme')
    
    # Days threshold (objects older than this will be deleted)
    days_threshold = int(os.environ.get('DAYS_THRESHOLD', '90'))
    
    # CSV export options
    export_csv = True
    csv_filename = os.environ.get('CSV_FILENAME', None)  # Use default timestamp if not provided
    
    # Whether to upload CSV to S3 (useful for Lambda)
    upload_csv_to_s3 = os.environ.get('UPLOAD_CSV_TO_S3', 'false').lower() == 'true'
    
    # List old objects
    objects_to_delete = list_old_objects(bucket_name, days_threshold)
    
    # Display objects
    display_objects(objects_to_delete)
    
    if not objects_to_delete:
        return
    
    # Export to CSV
    if export_csv:
        csv_path = export_to_csv(objects_to_delete, csv_filename)
        
        # Upload CSV to S3 if requested
        if upload_csv_to_s3 and csv_path:
            s3_client = boto3.client('s3')
            export_csv_to_s3(s3_client, bucket_name, csv_path)
    
    # In interactive mode, ask for confirmation
    if interactive:
        response = input(f"\nDo you want to delete these {len(objects_to_delete)} objects? (yes/no): ")
        
        if response.lower() != 'yes':
            print("Deletion cancelled.")
            return
    
    # Delete objects
    s3_client = boto3.client('s3')
    deleted_count = delete_objects(s3_client, bucket_name, objects_to_delete)
    
    print(f"\nSuccessfully deleted {deleted_count} objects.")

# Lambda handler for future use
def lambda_handler(event, context):
    """
    Lambda handler function
    """
    # Set environment variables from Lambda event if provided
    if 'bucket_name' in event:
        os.environ['S3_BUCKET_NAME'] = event['bucket_name']
    if 'days_threshold' in event:
        os.environ['DAYS_THRESHOLD'] = str(event['days_threshold'])
        
    # For Lambda, always upload CSV to S3
    os.environ['UPLOAD_CSV_TO_S3'] = 'true'
    
    # Run in non-interactive mode for Lambda
    main(interactive=False)
    
    return {
        'statusCode': 200,
        'body': 'S3 cleanup completed successfully'
    }

if __name__ == "__main__":
    # Check if running in Lambda
    is_lambda = os.environ.get('AWS_LAMBDA_FUNCTION_NAME') is not None
    
    # Run in interactive mode if not in Lambda
    main(interactive=not is_lambda)
