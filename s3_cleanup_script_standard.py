#!/usr/bin/env python3
"""
S3 Object Cleanup Script
Supports both AWS Lambda and CloudShell execution
Deletes objects older than specified days with dry-run capability
"""

import boto3
import json
import logging
import argparse
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional
import sys
import os
from botocore.exceptions import ClientError, NoCredentialsError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class S3ObjectCleaner:
    def __init__(self, bucket_name: str, region_name: Optional[str] = None):
        """Initialize S3 client and set bucket name"""
        try:
            self.s3_client = boto3.client('s3', region_name=region_name)
            self.bucket_name = bucket_name
            self.region_name = region_name
        except NoCredentialsError:
            logger.error("AWS credentials not found. Please configure your credentials.")
            raise
        except Exception as e:
            logger.error(f"Failed to initialize S3 client: {str(e)}")
            raise

    def validate_bucket_access(self) -> bool:
        """Validate that the bucket exists and we have access"""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Successfully validated access to bucket: {self.bucket_name}")
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.error(f"Bucket {self.bucket_name} does not exist")
            elif error_code == '403':
                logger.error(f"Access denied to bucket {self.bucket_name}")
            else:
                logger.error(f"Error accessing bucket {self.bucket_name}: {str(e)}")
            return False

    def get_objects_to_delete(self, days_old: int, prefix: str = '') -> List[Dict]:
        """Get list of objects older than specified days"""
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_old)
        objects_to_delete = []
        
        logger.info(f"Searching for objects older than {cutoff_date.isoformat()}")
        
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            total_objects = 0
            for page in page_iterator:
                if 'Contents' not in page:
                    continue
                    
                for obj in page['Contents']:
                    total_objects += 1
                    if obj['LastModified'] < cutoff_date:
                        objects_to_delete.append({
                            'Key': obj['Key'],
                            'Size': obj['Size'],
                            'LastModified': obj['LastModified'].isoformat(),
                            'ETag': obj['ETag']
                        })
            
            logger.info(f"Found {len(objects_to_delete)} objects to delete out of {total_objects} total objects")
            return objects_to_delete
            
        except ClientError as e:
            logger.error(f"Error listing objects: {str(e)}")
            return []

    def export_deletion_list(self, objects_to_delete: List[Dict], 
                           export_to_s3: bool = False, 
                           export_prefix: str = 'deletion-reports/') -> str:
        """Export list of files to be deleted"""
        if not objects_to_delete:
            logger.info("No objects to export")
            return ""
        
        # Create export data
        export_data = {
            'export_timestamp': datetime.now(timezone.utc).isoformat(),
            'bucket_name': self.bucket_name,
            'total_objects_to_delete': len(objects_to_delete),
            'total_size_bytes': sum(obj['Size'] for obj in objects_to_delete),
            'objects': objects_to_delete
        }
        
        # Generate filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"s3_deletion_list_{self.bucket_name}_{timestamp}.json"
        
        if export_to_s3:
            # Export to S3
            try:
                s3_key = f"{export_prefix}{filename}"
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Body=json.dumps(export_data, indent=2, default=str),
                    ContentType='application/json'
                )
                export_location = f"s3://{self.bucket_name}/{s3_key}"
                logger.info(f"Deletion list exported to S3: {export_location}")
                return export_location
            except ClientError as e:
                logger.error(f"Failed to export to S3: {str(e)}")
                return ""
        else:
            # Export to local file
            try:
                with open(filename, 'w') as f:
                    json.dump(export_data, f, indent=2, default=str)
                export_location = os.path.abspath(filename)
                logger.info(f"Deletion list exported locally: {export_location}")
                return export_location
            except IOError as e:
                logger.error(f"Failed to export locally: {str(e)}")
                return ""

    def delete_objects(self, objects_to_delete: List[Dict], dry_run: bool = True) -> Dict:
        """Delete objects from S3 (supports dry run)"""
        if not objects_to_delete:
            logger.info("No objects to delete")
            return {'deleted_count': 0, 'failed_count': 0, 'errors': []}
        
        if dry_run:
            logger.info(f"DRY RUN: Would delete {len(objects_to_delete)} objects")
            total_size = sum(obj['Size'] for obj in objects_to_delete)
            logger.info(f"DRY RUN: Would free up {total_size:,} bytes ({total_size / (1024**3):.2f} GB)")
            return {'deleted_count': len(objects_to_delete), 'failed_count': 0, 'errors': []}
        
        logger.info(f"Starting deletion of {len(objects_to_delete)} objects")
        
        deleted_count = 0
        failed_count = 0
        errors = []
        
        # Delete in batches of 1000 (S3 limit)
        batch_size = 1000
        for i in range(0, len(objects_to_delete), batch_size):
            batch = objects_to_delete[i:i + batch_size]
            delete_objects = [{'Key': obj['Key']} for obj in batch]
            
            try:
                response = self.s3_client.delete_objects(
                    Bucket=self.bucket_name,
                    Delete={
                        'Objects': delete_objects,
                        'Quiet': False
                    }
                )
                
                # Count successful deletions
                if 'Deleted' in response:
                    deleted_count += len(response['Deleted'])
                    logger.info(f"Successfully deleted batch of {len(response['Deleted'])} objects")
                
                # Handle errors
                if 'Errors' in response:
                    failed_count += len(response['Errors'])
                    for error in response['Errors']:
                        error_msg = f"Failed to delete {error['Key']}: {error['Code']} - {error['Message']}"
                        logger.error(error_msg)
                        errors.append(error_msg)
                        
            except ClientError as e:
                error_msg = f"Batch deletion failed: {str(e)}"
                logger.error(error_msg)
                failed_count += len(batch)
                errors.append(error_msg)
        
        logger.info(f"Deletion complete: {deleted_count} succeeded, {failed_count} failed")
        return {'deleted_count': deleted_count, 'failed_count': failed_count, 'errors': errors}

def lambda_handler(event, context):
    """AWS Lambda handler"""
    try:
        # Extract parameters from event
        bucket_name = event.get('bucket_name')
        days_old = int(event.get('days_old', 30))
        dry_run = event.get('dry_run', True)
        export_to_s3 = event.get('export_to_s3', True)
        prefix = event.get('prefix', '')
        region_name = event.get('region_name')
        
        if not bucket_name:
            raise ValueError("bucket_name parameter is required")
        
        logger.info(f"Lambda execution started - Bucket: {bucket_name}, Days: {days_old}, Dry Run: {dry_run}")
        
        # Initialize cleaner
        cleaner = S3ObjectCleaner(bucket_name, region_name)
        
        # Validate bucket access
        if not cleaner.validate_bucket_access():
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Cannot access specified bucket'})
            }
        
        # Get objects to delete
        objects_to_delete = cleaner.get_objects_to_delete(days_old, prefix)
        
        # Export deletion list
        export_location = ""
        if objects_to_delete:
            export_location = cleaner.export_deletion_list(objects_to_delete, export_to_s3)
        
        # Delete objects
        deletion_result = cleaner.delete_objects(objects_to_delete, dry_run)
        
        # Prepare response
        response = {
            'statusCode': 200,
            'body': json.dumps({
                'bucket_name': bucket_name,
                'days_old': days_old,
                'dry_run': dry_run,
                'objects_found': len(objects_to_delete),
                'export_location': export_location,
                'deletion_result': deletion_result
            }, default=str)
        }
        
        logger.info("Lambda execution completed successfully")
        return response
        
    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def main():
    """Main function for CloudShell execution"""
    parser = argparse.ArgumentParser(description='S3 Object Cleanup Script')
    parser.add_argument('bucket_name', help='S3 bucket name')
    parser.add_argument('--days-old', type=int, default=30, 
                       help='Delete objects older than X days (default: 30)')
    parser.add_argument('--dry-run', action='store_true', default=False,
                       help='Perform a dry run without actually deleting objects')
    parser.add_argument('--export-to-s3', action='store_true', default=False,
                       help='Export deletion list to S3 instead of local file')
    parser.add_argument('--prefix', default='', 
                       help='Only process objects with this prefix')
    parser.add_argument('--region', help='AWS region name')
    
    args = parser.parse_args()
    
    try:
        logger.info(f"Starting S3 cleanup - Bucket: {args.bucket_name}, Days: {args.days_old}, Dry Run: {args.dry_run}")
        
        # Initialize cleaner
        cleaner = S3ObjectCleaner(args.bucket_name, args.region)
        
        # Validate bucket access
        if not cleaner.validate_bucket_access():
            logger.error("Exiting due to bucket access issues")
            sys.exit(1)
        
        # Get objects to delete
        objects_to_delete = cleaner.get_objects_to_delete(args.days_old, args.prefix)
        
        if not objects_to_delete:
            logger.info("No objects found matching criteria. Nothing to do.")
            return
        
        # Export deletion list
        export_location = cleaner.export_deletion_list(objects_to_delete, args.export_to_s3)
        
        # Confirm deletion if not dry run
        if not args.dry_run:
            total_size = sum(obj['Size'] for obj in objects_to_delete)
            print(f"\nWARNING: About to delete {len(objects_to_delete)} objects ({total_size / (1024**3):.2f} GB)")
            print(f"Export location: {export_location}")
            
            confirm = input("Are you sure you want to proceed? (yes/no): ").lower().strip()
            if confirm != 'yes':
                logger.info("Deletion cancelled by user")
                return
        
        # Delete objects
        deletion_result = cleaner.delete_objects(objects_to_delete, args.dry_run)
        
        # Summary
        logger.info(f"Operation completed:")
        logger.info(f"  Objects processed: {len(objects_to_delete)}")
        logger.info(f"  Successfully deleted: {deletion_result['deleted_count']}")
        logger.info(f"  Failed deletions: {deletion_result['failed_count']}")
        if export_location:
            logger.info(f"  Export location: {export_location}")
        
        if deletion_result['errors']:
            logger.error("Errors encountered during deletion:")
            for error in deletion_result['errors']:
                logger.error(f"  {error}")
                
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Script execution failed: {str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()
