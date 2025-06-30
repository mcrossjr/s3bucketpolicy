#!/usr/bin/env python3
"""
S3 Bucket Cleanup Script
Deletes objects older than 15 days from a specified S3 bucket
EXCEPT objects created on Sunday or Wednesday (preserves them)
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

def list_old_objects(bucket_name, days_threshold=15, excluded_prefixes=None):
    """
    List objects older than specified days from an S3 bucket
    Only considers objects with STANDARD storage class
    EXCLUDES objects created on Sunday (weekday 6) or Wednesday (weekday 2)
    
    Args:
        bucket_name (str): Name of the S3 bucket
        days_threshold (int): Age threshold in days for standard objects (default: 15)
        excluded_prefixes (list): List of prefixes to exclude from cleanup (default: None)
        
    Returns:
        tuple: (objects_to_delete, other_storage_class_objects, excluded_objects, day_protected_objects)
    """
    # Initialize S3 client
    s3_client = boto3.client('s3')
    
    # Calculate the cutoff dates
    standard_cutoff = datetime.now(timezone.utc) - timedelta(days=days_threshold)
    
    # Objects to be deleted and objects to be skipped
    objects_to_delete = []
    other_storage_class_objects = []
    excluded_objects = []  # Objects excluded due to prefix
    day_protected_objects = []  # Objects protected due to creation day (Sunday/Wednesday)
    
    # Prepare the excluded prefixes list
    if excluded_prefixes is None:
        excluded_prefixes = []
    
    try:
        # List all objects in the bucket
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket_name)
        
        print(f"Scanning bucket '{bucket_name}' for STANDARD objects older than {days_threshold} days...")
        print(f"Standard cutoff date: {standard_cutoff.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"NOTE: Objects with non-STANDARD storage classes will be skipped.")
        print(f"NOTE: Objects created on Sunday or Wednesday will be preserved regardless of age.")
        
        if excluded_prefixes:
            print(f"Excluding objects with the following prefixes: {', '.join(excluded_prefixes)}")
        print("")
        
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    # Check if object should be excluded based on prefix
                    object_key = obj['Key']
                    if any(object_key.startswith(prefix) for prefix in excluded_prefixes):
                        excluded_objects.append({
                            'Key': object_key,
                            'LastModified': obj['LastModified'],
                            'Size': obj['Size'],
                            'StorageClass': obj.get('StorageClass', 'STANDARD')
                        })
                        continue
                    
                    # Get the storage class (default to STANDARD if not specified)
                    storage_class = obj.get('StorageClass', 'STANDARD')
                    
                    # Only consider STANDARD objects for deletion
                    if storage_class == 'STANDARD' and obj['LastModified'] < standard_cutoff:
                        # Check if the object was created on Sunday (6) or Wednesday (2)
                        creation_weekday = obj['LastModified'].weekday()
                        
                        if creation_weekday in [2, 6]:  # Wednesday=2, Sunday=6
                            # This object is protected due to creation day
                            day_protected_objects.append({
                                'Key': object_key,
                                'LastModified': obj['LastModified'],
                                'Size': obj['Size'],
                                'StorageClass': storage_class,
                                'CreationDay': obj['LastModified'].strftime('%A')  # Day name for display
                            })
                        else:
                            # This is a STANDARD object older than the threshold and not protected
                            objects_to_delete.append({
                                'Key': object_key,
                                'LastModified': obj['LastModified'],
                                'Size': obj['Size'],
                                'StorageClass': storage_class
                            })
                    else:
                        # Either non-STANDARD storage class or not old enough
                        if storage_class != 'STANDARD':
                            # This is a non-STANDARD object
                            other_storage_class_objects.append({
                                'Key': object_key,
                                'LastModified': obj['LastModified'],
                                'Size': obj['Size'],
                                'StorageClass': storage_class
                            })
        
        return (objects_to_delete, other_storage_class_objects, excluded_objects, day_protected_objects)
        
    except Exception as e:
        print(f"Error accessing bucket '{bucket_name}': {str(e)}")
        sys.exit(1)

def export_to_csv(objects_to_delete, other_storage_class_objects=None, excluded_objects=None, day_protected_objects=None, csv_filename=None):
    """
    Export the list of objects to a CSV file
    In Lambda environment, creates the file in /tmp directory
    
    Args:
        objects_to_delete (list): List of objects to export
        other_storage_class_objects (list): List of objects with non-STANDARD storage class
        excluded_objects (list): List of objects excluded from cleanup due to prefix rules
        day_protected_objects (list): List of objects protected due to creation day
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
            fieldnames = ['Object_Key', 'Last_Modified', 'Size_Bytes', 'Size_KB', 'Size_MB', 'Storage_Class', 'Age_Days', 'Creation_Day', 'Action', 'Notes']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write header
            writer.writeheader()
            
            # Calculate age for each object
            now = datetime.now(timezone.utc)
            
            # Write data rows for objects to delete
            for obj in objects_to_delete:
                age_days = (now - obj['LastModified']).days
                creation_day = obj['LastModified'].strftime('%A')
                writer.writerow({
                    'Object_Key': obj['Key'],
                    'Last_Modified': obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S UTC'),
                    'Size_Bytes': obj['Size'],
                    'Size_KB': round(obj['Size'] / 1024, 2),
                    'Size_MB': round(obj['Size'] / (1024 * 1024), 4),
                    'Storage_Class': obj.get('StorageClass', 'STANDARD'),
                    'Age_Days': age_days,
                    'Creation_Day': creation_day,
                    'Action': 'DELETE',
                    'Notes': ''
                })
            
            # Write data rows for day-protected objects
            if day_protected_objects:
                for obj in day_protected_objects:
                    age_days = (now - obj['LastModified']).days
                    creation_day = obj['LastModified'].strftime('%A')
                    writer.writerow({
                        'Object_Key': obj['Key'],
                        'Last_Modified': obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S UTC'),
                        'Size_Bytes': obj['Size'],
                        'Size_KB': round(obj['Size'] / 1024, 2),
                        'Size_MB': round(obj['Size'] / (1024 * 1024), 4),
                        'Storage_Class': obj.get('StorageClass', 'STANDARD'),
                        'Age_Days': age_days,
                        'Creation_Day': creation_day,
                        'Action': 'PROTECTED',
                        'Notes': f'Protected - created on {creation_day}'
                    })
            
            # Write data rows for non-STANDARD storage class objects
            if other_storage_class_objects:
                for obj in other_storage_class_objects:
                    age_days = (now - obj['LastModified']).days
                    creation_day = obj['LastModified'].strftime('%A')
                    writer.writerow({
                        'Object_Key': obj['Key'],
                        'Last_Modified': obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S UTC'),
                        'Size_Bytes': obj['Size'],
                        'Size_KB': round(obj['Size'] / 1024, 2),
                        'Size_MB': round(obj['Size'] / (1024 * 1024), 4),
                        'Storage_Class': obj.get('StorageClass', 'STANDARD'),
                        'Age_Days': age_days,
                        'Creation_Day': creation_day,
                        'Action': 'SKIPPED',
                        'Notes': f'Non-STANDARD storage class: {obj.get("StorageClass", "Unknown")}'
                    })
                    
            # Write data rows for excluded objects
            if excluded_objects:
                for obj in excluded_objects:
                    age_days = (now - obj['LastModified']).days
                    creation_day = obj['LastModified'].strftime('%A')
                    writer.writerow({
                        'Object_Key': obj['Key'],
                        'Last_Modified': obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S UTC'),
                        'Size_Bytes': obj['Size'],
                        'Size_KB': round(obj['Size'] / 1024, 2),
                        'Size_MB': round(obj['Size'] / (1024 * 1024), 4),
                        'Storage_Class': obj.get('StorageClass', 'STANDARD'),
                        'Age_Days': age_days,
                        'Creation_Day': creation_day,
                        'Action': 'EXCLUDED',
                        'Notes': 'Object in excluded prefix - skipped from cleanup'
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

def display_objects(objects_to_delete, other_storage_class_objects=None, excluded_objects=None, day_protected_objects=None):
    """
    Display objects in a formatted table
    
    Args:
        objects_to_delete (list): List of objects to display
        other_storage_class_objects (list): List of objects with non-STANDARD storage class
        excluded_objects (list): List of objects excluded due to prefix rules
        day_protected_objects (list): List of objects protected due to creation day
    """
    if not objects_to_delete and not other_storage_class_objects and not excluded_objects and not day_protected_objects:
        print("No objects found older than the specified threshold.")
        return
    
    # Calculate total size for objects to delete
    if objects_to_delete:
        total_size_bytes = sum(obj['Size'] for obj in objects_to_delete)
        total_size_mb = total_size_bytes / (1024 * 1024)
        
        print(f"Found {len(objects_to_delete)} STANDARD objects to delete (Total size: {total_size_mb:.2f} MB):\n")
        print("=" * 120)
        print(f"{'Object Key':<50} {'Last Modified':<25} {'Size (KB)':<10} {'Storage Class':<10} {'Creation Day':<12} {'Age (Days)':<10}")
        print("=" * 120)
        
        now = datetime.now(timezone.utc)
        for obj in objects_to_delete:
            key = obj['Key']
            # Truncate long keys for display
            display_key = key if len(key) <= 50 else key[:47] + "..."
            last_modified = obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S UTC')
            age_days = (now - obj['LastModified']).days
            size_kb = obj['Size'] / 1024
            storage_class = obj.get('StorageClass', 'STANDARD')
            creation_day = obj['LastModified'].strftime('%A')
            print(f"{display_key:<50} {last_modified:<25} {size_kb:>9.2f} {storage_class:<10} {creation_day:<12} {age_days:>10}")
        
        print("=" * 120)
    
    # Display day-protected objects
    if day_protected_objects:
        protected_total_size_bytes = sum(obj['Size'] for obj in day_protected_objects)
        protected_total_size_mb = protected_total_size_bytes / (1024 * 1024)
        
        print(f"\nProtected {len(day_protected_objects)} objects created on Sunday/Wednesday (Total size: {protected_total_size_mb:.2f} MB):")
        print("=" * 120)
        print(f"{'Object Key':<50} {'Last Modified':<25} {'Size (KB)':<10} {'Storage Class':<10} {'Creation Day':<12} {'Age (Days)':<10}")
        print("=" * 120)
        
        now = datetime.now(timezone.utc)
        for obj in day_protected_objects:
            key = obj['Key']
            display_key = key if len(key) <= 50 else key[:47] + "..."
            last_modified = obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S UTC')
            age_days = (now - obj['LastModified']).days
            size_kb = obj['Size'] / 1024
            storage_class = obj.get('StorageClass', 'STANDARD')
            creation_day = obj['CreationDay']
            print(f"{display_key:<50} {last_modified:<25} {size_kb:>9.2f} {storage_class:<10} {creation_day:<12} {age_days:>10}")
        
        print("=" * 120)
    
    # Display objects with non-STANDARD storage class
    if other_storage_class_objects:
        other_total_size_bytes = sum(obj['Size'] for obj in other_storage_class_objects)
        other_total_size_mb = other_total_size_bytes / (1024 * 1024)
        
        print(f"\nSkipping {len(other_storage_class_objects)} objects with non-STANDARD storage class (Total size: {other_total_size_mb:.2f} MB):")
        print("=" * 120)
        print(f"{'Object Key':<50} {'Last Modified':<25} {'Size (KB)':<10} {'Storage Class':<10} {'Creation Day':<12} {'Age (Days)':<10}")
        print("=" * 120)
        
        now = datetime.now(timezone.utc)
        for obj in other_storage_class_objects:
            key = obj['Key']
            display_key = key if len(key) <= 50 else key[:47] + "..."
            last_modified = obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S UTC')
            age_days = (now - obj['LastModified']).days
            size_kb = obj['Size'] / 1024
            storage_class = obj.get('StorageClass', 'STANDARD')
            creation_day = obj['LastModified'].strftime('%A')
            print(f"{display_key:<50} {last_modified:<25} {size_kb:>9.2f} {storage_class:<10} {creation_day:<12} {age_days:>10}")
        
        print("=" * 120)
    
    # Display excluded objects
    if excluded_objects:
        ex_total_size_bytes = sum(obj['Size'] for obj in excluded_objects)
        ex_total_size_mb = ex_total_size_bytes / (1024 * 1024)
        
        print(f"\nExcluded {len(excluded_objects)} objects from cleanup due to prefix rules (Total size: {ex_total_size_mb:.2f} MB):")
        print("=" * 120)
        print(f"{'Object Key':<50} {'Last Modified':<25} {'Size (KB)':<10} {'Storage Class':<10} {'Creation Day':<12} {'Age (Days)':<10}")
        print("=" * 120)
        
        now = datetime.now(timezone.utc)
        for obj in excluded_objects:
            key = obj['Key']
            display_key = key if len(key) <= 50 else key[:47] + "..."
            last_modified = obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S UTC')
            age_days = (now - obj['LastModified']).days
            size_kb = obj['Size'] / 1024
            storage_class = obj.get('StorageClass', 'STANDARD')
            creation_day = obj['LastModified'].strftime('%A')
            print(f"{display_key:<50} {last_modified:<25} {size_kb:>9.2f} {storage_class:<10} {creation_day:<12} {age_days:>10}")
        
        print("=" * 120)

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

def main(interactive=True, dry_run=False):
    """
    Main function
    
    Args:
        interactive (bool): Whether to run in interactive mode (ask for confirmation)
        dry_run (bool): Whether to run in dry-run mode (no actual deletions)
    """
    # S3 bucket name
    bucket_name = os.environ.get('S3_BUCKET_NAME', 'testme')
    
    # Days threshold (objects older than this will be deleted, except Sunday/Wednesday)
    days_threshold = int(os.environ.get('DAYS_THRESHOLD', '15'))
    
    # Excluded prefixes (to protect specific folders, especially those with lifecycle transitions)
    excluded_prefixes_str = os.environ.get('EXCLUDED_PREFIXES', '')
    excluded_prefixes = [prefix.strip() for prefix in excluded_prefixes_str.split(',') if prefix.strip()]
    
    # Dry run mode
    dry_run = dry_run or os.environ.get('DRY_RUN', 'false').lower() == 'true'
    
    # CSV export options
    export_csv = True
    csv_filename = os.environ.get('CSV_FILENAME', None)  # Use default timestamp if not provided
    
    # Whether to upload CSV to S3 (useful for Lambda)
    upload_csv_to_s3 = os.environ.get('UPLOAD_CSV_TO_S3', 'false').lower() == 'true'
    
    if dry_run:
        print("\n" + "="*60)
        print("DRY RUN MODE - NO OBJECTS WILL BE DELETED")
        print("="*60)
    
    # List old objects
    objects_to_delete, other_storage_class_objects, excluded_objects, day_protected_objects = list_old_objects(
        bucket_name, 
        days_threshold, 
        excluded_prefixes
    )
    
    # Display objects
    display_objects(objects_to_delete, other_storage_class_objects, excluded_objects, day_protected_objects)
    
    # Export to CSV (including skipped objects for reference)
    if export_csv:
        csv_path = export_to_csv(
            objects_to_delete, 
            other_storage_class_objects, 
            excluded_objects,
            day_protected_objects,
            csv_filename
        )
        
        # Upload CSV to S3 if requested
        if upload_csv_to_s3 and csv_path:
            s3_client = boto3.client('s3')
            export_csv_to_s3(s3_client, bucket_name, csv_path)
    
    # Exit if no objects to delete
    if not objects_to_delete:
        if dry_run:
            print("\nDRY RUN COMPLETE - No objects would be deleted.")
        return
    
    # In dry run mode, skip actual deletion
    if dry_run:
        print(f"\nDRY RUN COMPLETE - Would delete {len(objects_to_delete)} STANDARD objects.")
        if other_storage_class_objects:
            print(f"Would skip {len(other_storage_class_objects)} objects with non-STANDARD storage class.")
        if excluded_objects:
            print(f"Would exclude {len(excluded_objects)} objects due to prefix rules.")
        if day_protected_objects:
            print(f"Would protect {len(day_protected_objects)} objects created on Sunday or Wednesday.")
        return
    
    # In interactive mode, ask for confirmation
    if interactive:
        response = input(f"\nDo you want to delete {len(objects_to_delete)} STANDARD objects? (yes/no): ")
        
        if response.lower() != 'yes':
            print("Deletion cancelled.")
            return
    
    # Delete objects
    s3_client = boto3.client('s3')
    deleted_count = delete_objects(s3_client, bucket_name, objects_to_delete)
    
    print(f"\nSuccessfully deleted {deleted_count} STANDARD objects.")
    
    # Print summary of skipped objects
    if other_storage_class_objects:
        print(f"Skipped {len(other_storage_class_objects)} objects with non-STANDARD storage class.")
    if excluded_objects:
        print(f"Excluded {len(excluded_objects)} objects due to prefix rules.")
    if day_protected_objects:
        print(f"Protected {len(day_protected_objects)} objects created on Sunday or Wednesday.")
    
    # Print location of CSV for reference
    if export_csv and 'csv_path' in locals():
        print(f"\nA record of processed objects is available in: {csv_path}")

def lambda_handler(event, context):
    """
    Lambda handler function
    
    Args:
        event (dict): Lambda event data, can contain configuration overrides:
            - bucket_name: S3 bucket to clean up
            - days_threshold: Age threshold for standard objects (default: 15)
            - excluded_prefixes: Comma-separated list of prefixes to exclude from cleanup
            - report_prefix: S3 prefix for report uploads (default: "cleanup_logs/")
            - dry_run: Boolean to enable dry-run mode (default: False)
            
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
    if 'excluded_prefixes' in event:
        if isinstance(event['excluded_prefixes'], list):
            os.environ['EXCLUDED_PREFIXES'] = ','.join(event['excluded_prefixes'])
        else:
            os.environ['EXCLUDED_PREFIXES'] = event['excluded_prefixes']
    if 'dry_run' in event:
        os.environ['DRY_RUN'] = str(event['dry_run']).lower()
    
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
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='S3 Bucket Cleanup Script')
    parser.add_argument('--dry-run', action='store_true', 
                       help='Run in dry-run mode (no actual deletions)')
    parser.add_argument('--non-interactive', action='store_true',
                       help='Run in non-interactive mode (no confirmation prompts)')
    args = parser.parse_args()
    
    # Check if running in Lambda
    is_lambda = os.environ.get('AWS_LAMBDA_FUNCTION_NAME') is not None
    
    # Set dry run from command line argument
    if args.dry_run:
        os.environ['DRY_RUN'] = 'true'
    
    # Run in interactive mode if not in Lambda and not explicitly set to non-interactive
    interactive_mode = not is_lambda and not args.non_interactive
    main(interactive=interactive_mode, dry_run=args.dry_run)
