#!/usr/bin/env python3
"""
S3 Bucket Cleanup Script
Deletes objects older than 90 days from a specified S3 bucket
Avoids early deletion fees for Glacier and Glacier Deep Archive storage classes
Suitable for AWS CloudShell and Lambda deployment
Exports file details to CSV in current directory for audit purposes
"""

import boto3
from datetime import datetime, timezone, timedelta
import sys
import os
import csv
import io

def list_old_objects(bucket_name, days_threshold=90, glacier_min_days=91, deep_archive_min_days=181):
    """
    List objects older than specified days from an S3 bucket
    Checks storage class to avoid early deletion fees for Glacier objects
    
    Args:
        bucket_name (str): Name of the S3 bucket
        days_threshold (int): Age threshold in days for standard objects (default: 90)
        glacier_min_days (int): Minimum age in days for Glacier objects (default: 91)
        deep_archive_min_days (int): Minimum age in days for Glacier Deep Archive objects (default: 181)
        
    Returns:
        tuple: (objects_to_delete, skipped_glacier_objects, skipped_deep_archive_objects)
    """
    # Initialize S3 client
    s3_client = boto3.client('s3')
    
    # Calculate the cutoff dates
    standard_cutoff = datetime.now(timezone.utc) - timedelta(days=days_threshold)
    glacier_cutoff = datetime.now(timezone.utc) - timedelta(days=glacier_min_days)
    deep_archive_cutoff = datetime.now(timezone.utc) - timedelta(days=deep_archive_min_days)
    
    # Objects to be deleted and objects to be skipped
    objects_to_delete = []
    skipped_glacier_objects = []
    skipped_deep_archive_objects = []
    
    try:
        # List all objects in the bucket
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket_name)
        
        print(f"Scanning bucket '{bucket_name}' for objects older than {days_threshold} days...")
        print(f"Standard cutoff date: {standard_cutoff.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"Glacier minimum age: {glacier_min_days} days (cutoff: {glacier_cutoff.strftime('%Y-%m-%d %H:%M:%S UTC')})")
        print(f"Glacier Deep Archive minimum age: {deep_archive_min_days} days (cutoff: {deep_archive_cutoff.strftime('%Y-%m-%d %H:%M:%S UTC')})\n")
        
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    # Get the storage class (default to STANDARD if not specified)
                    storage_class = obj.get('StorageClass', 'STANDARD')
                    
                    # Determine if object should be deleted based on storage class and age
                    if (storage_class in ['GLACIER', 'GLACIER_IR'] and obj['LastModified'] < glacier_cutoff) or \
                       (storage_class == 'DEEP_ARCHIVE' and obj['LastModified'] < deep_archive_cutoff) or \
                       (storage_class not in ['GLACIER', 'GLACIER_IR', 'DEEP_ARCHIVE'] and obj['LastModified'] < standard_cutoff):
                        # Safe to delete - meets the age requirements
                        objects_to_delete.append({
                            'Key': obj['Key'],
                            'LastModified': obj['LastModified'],
                            'Size': obj['Size'],
                            'StorageClass': storage_class
                        })
                    elif storage_class in ['GLACIER', 'GLACIER_IR'] and obj['LastModified'] < standard_cutoff:
                        # Glacier object old enough to meet standard threshold but not glacier threshold
                        skipped_glacier_objects.append({
                            'Key': obj['Key'],
                            'LastModified': obj['LastModified'],
                            'Size': obj['Size'],
                            'StorageClass': storage_class
                        })
                    elif storage_class == 'DEEP_ARCHIVE' and obj['LastModified'] < standard_cutoff:
                        # Deep Archive object old enough to meet standard threshold but not deep archive threshold
                        skipped_deep_archive_objects.append({
                            'Key': obj['Key'],
                            'LastModified': obj['LastModified'],
                            'Size': obj['Size'],
                            'StorageClass': storage_class
                        })
        
        return (objects_to_delete, skipped_glacier_objects, skipped_deep_archive_objects)
        
    except Exception as e:
        print(f"Error accessing bucket '{bucket_name}': {str(e)}")
        sys.exit(1)

def export_to_csv(objects_to_delete, skipped_glacier_objects=None, skipped_deep_archive_objects=None, csv_filename=None):
    """
    Export the list of objects to a CSV file
    In Lambda environment, creates the file in /tmp directory
    
    Args:
        objects_to_delete (list): List of objects to export
        skipped_glacier_objects (list): List of Glacier objects skipped to avoid early deletion fees
        skipped_deep_archive_objects (list): List of Deep Archive objects skipped to avoid early deletion fees
        csv_filename (str, optional): Name of CSV file. If None, a default name with timestamp will be created
        
    Returns:
        str: Path to the created CSV file
    """
    if not csv_filename:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_filename = f"s3_cleanup_{timestamp}.csv"
    
    # In Lambda, we need to use /tmp directory for file operations
    if os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
        csv_path = f"/tmp/{csv_filename}"
    else:
        # For local execution, use current working directory
        csv_path = os.path.join(os.getcwd(), csv_filename)
    
    try:
        with open(csv_path, 'w', newline='') as csvfile:
            # Define CSV columns
            fieldnames = ['Object_Key', 'Last_Modified', 'Size_Bytes', 'Size_KB', 'Size_MB', 'Storage_Class', 'Age_Days', 'Action', 'Notes']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write header
            writer.writeheader()
            
            # Calculate age for each object
            now = datetime.now(timezone.utc)
            
            # Write data rows for objects to delete
            for obj in objects_to_delete:
                age_days = (now - obj['LastModified']).days
                writer.writerow({
                    'Object_Key': obj['Key'],
                    'Last_Modified': obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S UTC'),
                    'Size_Bytes': obj['Size'],
                    'Size_KB': round(obj['Size'] / 1024, 2),
                    'Size_MB': round(obj['Size'] / (1024 * 1024), 4),
                    'Storage_Class': obj.get('StorageClass', 'STANDARD'),
                    'Age_Days': age_days,
                    'Action': 'DELETE',
                    'Notes': ''
                })
            
            # Write data rows for skipped Glacier objects
            if skipped_glacier_objects:
                for obj in skipped_glacier_objects:
                    age_days = (now - obj['LastModified']).days
                    remaining_days = 91 - age_days
                    writer.writerow({
                        'Object_Key': obj['Key'],
                        'Last_Modified': obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S UTC'),
                        'Size_Bytes': obj['Size'],
                        'Size_KB': round(obj['Size'] / 1024, 2),
                        'Size_MB': round(obj['Size'] / (1024 * 1024), 4),
                        'Storage_Class': obj.get('StorageClass', 'GLACIER'),
                        'Age_Days': age_days,
                        'Action': 'SKIPPED',
                        'Notes': f'Glacier early deletion fee would apply. Needs {remaining_days} more days to reach 91 days.'
                    })
            
            # Write data rows for skipped Deep Archive objects
            if skipped_deep_archive_objects:
                for obj in skipped_deep_archive_objects:
                    age_days = (now - obj['LastModified']).days
                    remaining_days = 181 - age_days
                    writer.writerow({
                        'Object_Key': obj['Key'],
                        'Last_Modified': obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S UTC'),
                        'Size_Bytes': obj['Size'],
                        'Size_KB': round(obj['Size'] / 1024, 2),
                        'Size_MB': round(obj['Size'] / (1024 * 1024), 4),
                        'Storage_Class': obj.get('StorageClass', 'DEEP_ARCHIVE'),
                        'Age_Days': age_days,
                        'Action': 'SKIPPED',
                        'Notes': f'Deep Archive early deletion fee would apply. Needs {remaining_days} more days to reach 181 days.'
                    })
        
        print(f"Exported list of objects to: {csv_path}")
        return csv_path
        
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

def display_objects(objects_to_delete, skipped_glacier_objects=None, skipped_deep_archive_objects=None):
    """
    Display objects in a formatted table
    
    Args:
        objects_to_delete (list): List of objects to display
        skipped_glacier_objects (list): List of Glacier objects skipped
        skipped_deep_archive_objects (list): List of Deep Archive objects skipped
    """
    if not objects_to_delete and not skipped_glacier_objects and not skipped_deep_archive_objects:
        print("No objects found older than the specified threshold.")
        return
    
    # Calculate total size for objects to delete
    if objects_to_delete:
        total_size_bytes = sum(obj['Size'] for obj in objects_to_delete)
        total_size_mb = total_size_bytes / (1024 * 1024)
        
        print(f"Found {len(objects_to_delete)} objects to delete (Total size: {total_size_mb:.2f} MB):\n")
        print("=" * 105)
        print(f"{'Object Key':<50} {'Last Modified':<25} {'Size (KB)':<10} {'Storage Class':<10} {'Age (Days)':<10}")
        print("=" * 105)
        
        now = datetime.now(timezone.utc)
        for obj in objects_to_delete:
            key = obj['Key']
            # Truncate long keys for display
            display_key = key if len(key) <= 50 else key[:47] + "..."
            last_modified = obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S UTC')
            age_days = (now - obj['LastModified']).days
            size_kb = obj['Size'] / 1024
            storage_class = obj.get('StorageClass', 'STANDARD')
            print(f"{display_key:<50} {last_modified:<25} {size_kb:>9.2f} {storage_class:<10} {age_days:>10}")
        
        print("=" * 105)
    
    # Display skipped Glacier objects
    if skipped_glacier_objects:
        g_total_size_bytes = sum(obj['Size'] for obj in skipped_glacier_objects)
        g_total_size_mb = g_total_size_bytes / (1024 * 1024)
        
        print(f"\nSkipping {len(skipped_glacier_objects)} Glacier objects to avoid early deletion fees (Total size: {g_total_size_mb:.2f} MB):")
        print("=" * 105)
        print(f"{'Object Key':<50} {'Last Modified':<25} {'Size (KB)':<10} {'Storage Class':<10} {'Age (Days)':<10}")
        print("=" * 105)
        
        now = datetime.now(timezone.utc)
        for obj in skipped_glacier_objects:
            key = obj['Key']
            display_key = key if len(key) <= 50 else key[:47] + "..."
            last_modified = obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S UTC')
            age_days = (now - obj['LastModified']).days
            size_kb = obj['Size'] / 1024
            storage_class = obj.get('StorageClass', 'GLACIER')
            print(f"{display_key:<50} {last_modified:<25} {size_kb:>9.2f} {storage_class:<10} {age_days:>10}")
        
        print("=" * 105)
    
    # Display skipped Deep Archive objects
    if skipped_deep_archive_objects:
        da_total_size_bytes = sum(obj['Size'] for obj in skipped_deep_archive_objects)
        da_total_size_mb = da_total_size_bytes / (1024 * 1024)
        
        print(f"\nSkipping {len(skipped_deep_archive_objects)} Glacier Deep Archive objects to avoid early deletion fees (Total size: {da_total_size_mb:.2f} MB):")
        print("=" * 105)
        print(f"{'Object Key':<50} {'Last Modified':<25} {'Size (KB)':<10} {'Storage Class':<10} {'Age (Days)':<10}")
        print("=" * 105)
        
        now = datetime.now(timezone.utc)
        for obj in skipped_deep_archive_objects:
            key = obj['Key']
            display_key = key if len(key) <= 50 else key[:47] + "..."
            last_modified = obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S UTC')
            age_days = (now - obj['LastModified']).days
            size_kb = obj['Size'] / 1024
            storage_class = obj.get('StorageClass', 'DEEP_ARCHIVE')
            print(f"{display_key:<50} {last_modified:<25} {size_kb:>9.2f} {storage_class:<10} {age_days:>10}")
        
        print("=" * 105)

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
    if not objects_to_delete:
        print("\nNo objects to delete.")
        return 0
        
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
    
    # Glacier minimum age (to avoid early deletion fees)
    glacier_min_days = int(os.environ.get('GLACIER_MIN_DAYS', '91'))
    
    # Deep Archive minimum age (to avoid early deletion fees)
    deep_archive_min_days = int(os.environ.get('DEEP_ARCHIVE_MIN_DAYS', '181'))
    
    # CSV export options
    export_csv = True
    csv_filename = os.environ.get('CSV_FILENAME', None)  # Use default timestamp if not provided
    
    # Whether to upload CSV to S3 (useful for Lambda)
    upload_csv_to_s3 = os.environ.get('UPLOAD_CSV_TO_S3', 'false').lower() == 'true'
    
    # List old objects
    objects_to_delete, skipped_glacier_objects, skipped_deep_archive_objects = list_old_objects(
        bucket_name, 
        days_threshold, 
        glacier_min_days, 
        deep_archive_min_days
    )
    
    # Display objects
    display_objects(objects_to_delete, skipped_glacier_objects, skipped_deep_archive_objects)
    
    # Export to CSV (including skipped objects for reference)
    if export_csv:
        csv_path = export_to_csv(
            objects_to_delete, 
            skipped_glacier_objects, 
            skipped_deep_archive_objects, 
            csv_filename
        )
        
        # Upload CSV to S3 if requested
        if upload_csv_to_s3 and csv_path:
            s3_client = boto3.client('s3')
            export_csv_to_s3(s3_client, bucket_name, csv_path)
    
    # Exit if no objects to delete
    if not objects_to_delete:
        return
    
    # In interactive mode, ask for confirmation
    if interactive:
        response = input(f"\nDo you want to delete {len(objects_to_delete)} objects? (yes/no): ")
        
        if response.lower() != 'yes':
            print("Deletion cancelled.")
            return
    
    # Delete objects
    s3_client = boto3.client('s3')
    deleted_count = delete_objects(s3_client, bucket_name, objects_to_delete)
    
    print(f"\nSuccessfully deleted {deleted_count} objects.")
    
    # Print summary of skipped objects
    if skipped_glacier_objects:
        print(f"Skipped {len(skipped_glacier_objects)} Glacier objects to avoid early deletion fees.")
    if skipped_deep_archive_objects:
        print(f"Skipped {len(skipped_deep_archive_objects)} Glacier Deep Archive objects to avoid early deletion fees.")
    
    # Print location of CSV for reference
    if export_csv and 'csv_path' in locals():
        print(f"\nA record of processed objects is available in: {csv_path}")

def lambda_handler(event, context):
    """
    Lambda handler function
    
    Args:
        event (dict): Lambda event data, can contain configuration overrides:
            - bucket_name: S3 bucket to clean up
            - days_threshold: Age threshold for standard objects
            - glacier_min_days: Minimum age for Glacier objects
            - deep_archive_min_days: Minimum age for Deep Archive objects
            - report_prefix: S3 prefix for report uploads (default: "cleanup_logs/")
            
        context: Lambda context
        
    Returns:
        dict: Execution results
    """
    print(f"S3 Cleanup Lambda started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    
    # Set environment variables from Lambda event if provided
    if 'bucket_name' in event:
        os.environ['S3_BUCKET_NAME'] = event['bucket_name']
    if 'days_threshold' in event:
        os.environ['DAYS_THRESHOLD'] = str(event['days_threshold'])
    if 'glacier_min_days' in event:
        os.environ['GLACIER_MIN_DAYS'] = str(event['glacier_min_days'])
    if 'deep_archive_min_days' in event:
        os.environ['DEEP_ARCHIVE_MIN_DAYS'] = str(event['deep_archive_min_days'])
    
    # Configure report upload
    os.environ['UPLOAD_CSV_TO_S3'] = 'true'
    
    # Set report prefix if provided
    if 'report_prefix' in event:
        report_prefix = event['report_prefix']
        if not report_prefix.endswith('/'):
            report_prefix += '/'
    else:
        report_prefix = "cleanup_logs/"
    
    # Get bucket name for logging
    bucket_name = os.environ.get('S3_BUCKET_NAME', 'testme')
    
    # Run in non-interactive mode for Lambda
    try:
        main(interactive=False)
        execution_status = "success"
        message = f"S3 cleanup completed successfully for bucket {bucket_name}"
    except Exception as e:
        execution_status = "error"
        message = f"Error during S3 cleanup for bucket {bucket_name}: {str(e)}"
        print(message)
    
    result = {
        'statusCode': 200 if execution_status == "success" else 500,
        'body': {
            'status': execution_status,
            'message': message,
            'bucket': bucket_name,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')
        }
    }
    
    print(f"S3 Cleanup Lambda completed with status: {execution_status}")
    return result

if __name__ == "__main__":
    # Check if running in Lambda
    is_lambda = os.environ.get('AWS_LAMBDA_FUNCTION_NAME') is not None
    
    # Run in interactive mode if not in Lambda
    main(interactive=not is_lambda)