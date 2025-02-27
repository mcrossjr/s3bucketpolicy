#!/usr/bin/env python3
import boto3
import datetime
import csv
from tabulate import tabulate
import humanize
import os

def get_s3_bucket_info():
    """
    Retrieves information about all S3 buckets including:
    - Bucket name
    - Last 3 access times
    - Bucket size
    - Last 3 objects added
    """
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    
    # Get all buckets
    response = s3_client.list_buckets()
    buckets = response['Buckets']
    
    bucket_info = []
    
    for bucket in buckets:
        bucket_name = bucket['Name']
        print(f"Processing bucket: {bucket_name}")
        
        try:
            # Get bucket size
            total_size = 0
            object_count = 0
            
            bucket_obj = s3_resource.Bucket(bucket_name)
            
            # Get all objects sorted by last modified date (newest first)
            objects = sorted(
                bucket_obj.objects.all(),
                key=lambda obj: obj.last_modified,
                reverse=True
            )
            
            # Calculate total size
            for obj in objects:
                total_size += obj.size
                object_count += 1
            
            # Get the last 3 objects
            latest_objects = []
            for obj in objects[:3]:
                latest_objects.append({
                    'key': obj.key,
                    'size': humanize.naturalsize(obj.size),
                    'size_bytes': obj.size,
                    'last_modified': obj.last_modified.strftime('%Y-%m-%d %H:%M:%S')
                })
            
            # Get CloudTrail events for bucket access (if available)
            access_times = []
            try:
                cloudtrail = boto3.client('cloudtrail')
                
                # Look up the last 3 access events
                events = cloudtrail.lookup_events(
                    LookupAttributes=[
                        {
                            'AttributeKey': 'ResourceName',
                            'AttributeValue': bucket_name
                        }
                    ],
                    MaxResults=3
                )
                
                for event in events.get('Events', []):
                    access_times.append(event['EventTime'].strftime('%Y-%m-%d %H:%M:%S'))
                
            except Exception as e:
                # CloudTrail lookup might fail if not enabled
                access_times = ["CloudTrail access info not available"]
            
            # Ensure we have exactly 3 items for access times (or placeholders)
            while len(access_times) < 3:
                access_times.append("N/A")
            
            # Ensure we have exactly 3 items for latest objects (or placeholders)
            while len(latest_objects) < 3:
                latest_objects.append({
                    'key': "N/A",
                    'size': "N/A",
                    'size_bytes': 0,
                    'last_modified': "N/A"
                })
            
            # Create bucket info entry
            bucket_info.append({
                'name': bucket_name,
                'size': humanize.naturalsize(total_size),
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
            })
            
        except Exception as e:
            print(f"Error processing bucket {bucket_name}: {str(e)}")
            bucket_info.append({
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
            })
    
    return bucket_info

def display_bucket_info(bucket_info):
    """
    Formats and displays the bucket information in a readable format
    """
    print("\n" + "="*100)
    print("S3 BUCKET INFORMATION SUMMARY")
    print("="*100 + "\n")
    
    for bucket in bucket_info:
        print(f"BUCKET: {bucket['name']}")
        print(f"Total Size: {bucket['size']} ({bucket['object_count']} objects)")
        print("\nLast 3 Access Times:")
        print(f"  1. {bucket['access_time_1']}")
        print(f"  2. {bucket['access_time_2']}")
        print(f"  3. {bucket['access_time_3']}")
        
        print("\nLast 3 Objects Added:")
        object_data = [
            [bucket['latest_object_1_key'], bucket['latest_object_1_size'], bucket['latest_object_1_date']],
            [bucket['latest_object_2_key'], bucket['latest_object_2_size'], bucket['latest_object_2_date']],
            [bucket['latest_object_3_key'], bucket['latest_object_3_size'], bucket['latest_object_3_date']]
        ]
        
        print(tabulate(object_data, headers=["Object Key", "Size", "Last Modified"], tablefmt="grid"))
        print("\n" + "-"*100 + "\n")

def export_to_csv(bucket_info, filename="s3_bucket_info.csv"):
    """
    Exports the bucket information to a CSV file
    """
    try:
        with open(filename, 'w', newline='') as csvfile:
            # Define the CSV headers
            fieldnames = [
                'name', 'size', 'size_bytes', 'object_count',
                'access_time_1', 'access_time_2', 'access_time_3',
                'latest_object_1_key', 'latest_object_1_size', 'latest_object_1_date',
                'latest_object_2_key', 'latest_object_2_size', 'latest_object_2_date',
                'latest_object_3_key', 'latest_object_3_size', 'latest_object_3_date'
            ]
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            # Write the data
            for bucket in bucket_info:
                writer.writerow(bucket)
                
        print(f"\nCSV export completed. File saved as: {os.path.abspath(filename)}")
        return True
    except Exception as e:
        print(f"Error exporting to CSV: {str(e)}")
        return False

def main():
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"s3_bucket_info_{timestamp}.csv"
    
    print("Starting S3 bucket information gathering...")
    bucket_info = get_s3_bucket_info()
    display_bucket_info(bucket_info)
    export_to_csv(bucket_info, csv_filename)
    print("Completed!")

if __name__ == "__main__":
    main()
