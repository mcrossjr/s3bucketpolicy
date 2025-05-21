# AWS Lambda Deployment Guide for S3 Cleanup Script

This guide walks through the process of deploying the S3 Cleanup Script as an AWS Lambda function that runs daily.

## 1. Prepare the Deployment Package

First, let's create a deployment package for Lambda:

```bash
# Create a directory for the package
mkdir s3_cleanup_lambda
cd s3_cleanup_lambda

# Copy the script into the directory
# (Assuming you saved the script as s3_cleanup.py)
cp /path/to/s3_cleanup.py .

# Create the deployment package
zip s3_cleanup_lambda.zip s3_cleanup.py
```

## 2. Create an IAM Role for the Lambda Function

The Lambda function needs permissions to access S3 buckets and write logs to CloudWatch.

### Create the IAM Role:

1. Go to the AWS Management Console
2. Navigate to IAM > Roles > Create role
3. Select "AWS service" as the trusted entity and "Lambda" as the use case
4. Click "Next: Permissions"
5. Attach the following policies:
   - `AmazonS3ReadOnlyAccess` (for listing bucket objects)
   - `AWSLambdaBasicExecutionRole` (for CloudWatch Logs)
6. Click "Next: Tags" (add optional tags)
7. Click "Next: Review"
8. Name the role (e.g., "S3CleanupLambdaRole")
9. Click "Create role"

### Add Custom S3 Permissions:

1. Go to the newly created role
2. Click "Add inline policy"
3. Select the JSON tab and paste the following policy:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:DeleteObject",
                "s3:PutObject"
            ],
            "Resource": [
                "arn:aws:s3:::testme/*",
                "arn:aws:s3:::testme"
            ]
        }
    ]
}
```

4. Replace "testme" with your actual bucket name
5. Click "Review policy"
6. Name the policy (e.g., "S3CleanupBucketAccess")
7. Click "Create policy"

## 3. Create the Lambda Function

Now create the Lambda function:

1. Go to the AWS Management Console
2. Navigate to Lambda > Functions > Create function
3. Choose "Author from scratch"
4. Enter a function name (e.g., "S3BucketCleanup")
5. Select "Python 3.9" for the runtime
6. Under "Permissions", expand "Change default execution role"
7. Select "Use an existing role" and choose the role you created
8. Click "Create function"

## 4. Configure the Lambda Function

After the function is created:

1. In the "Code" tab, upload your deployment package:
   - Click "Upload from" and select ".zip file"
   - Upload your `s3_cleanup_lambda.zip` file
   - Click "Save"

2. Set the handler:
   - Under "Runtime settings", click "Edit"
   - Set the Handler to "s3_cleanup.lambda_handler"
   - Click "Save"

3. Configure environment variables:
   - Click on the "Configuration" tab
   - Select "Environment variables" from the left menu
   - Click "Edit"
   - Add the following environment variables:
     - `S3_BUCKET_NAME`: Your bucket name (e.g., "testme")
     - `DAYS_THRESHOLD`: 90 (or your preferred value)
     - `GLACIER_MIN_DAYS`: 91
     - `DEEP_ARCHIVE_MIN_DAYS`: 181
     - `UPLOAD_CSV_TO_S3`: true
   - Click "Save"

4. Configure function timeout:
   - In the "Configuration" tab, select "General configuration"
   - Click "Edit"
   - Set Timeout to 5 minutes (300 seconds)
   - Set Memory to 256 MB (increase if you have large buckets)
   - Click "Save"

## 5. Set Up Daily Scheduling with EventBridge (CloudWatch Events)

Now, set up the Lambda function to run daily:

1. Go to the AWS Management Console
2. Navigate to Amazon EventBridge > Rules > Create rule
3. For the rule type, select "Schedule"
4. Enter a name for the rule (e.g., "DailyS3Cleanup")
5. Add an optional description
6. Under "Schedule pattern", select "Cron expression"
7. Enter a cron expression for daily execution:
   - `0 3 * * ? *` (Runs at 3:00 AM UTC every day)

8. Click "Next"

9. Under "Select targets":
   - Choose "Lambda function" from the target dropdown
   - Select your Lambda function from the list
   - Optionally, you can configure input by selecting "Configure input"
   - If you want to override function parameters, select "Constant (JSON text)" and provide:

```json
{
  "bucket_name": "testme",
  "days_threshold": 90,
  "glacier_min_days": 91,
  "deep_archive_min_days": 181,
  "report_prefix": "cleanup_logs/"
}
```

10. Click "Next" through the remaining steps
11. Click "Create rule"

## 6. Testing the Lambda Function

Test your Lambda function:

1. Go back to your Lambda function in the console
2. Click the "Test" tab
3. Create a new test event:
   - Name: "TestEvent"
   - JSON payload (you can use the same JSON as above)
4. Click "Test"
5. Check the execution results and logs

## 7. Monitoring and Logs

Monitor your Lambda function:

1. In the Lambda console, go to the "Monitor" tab
2. View invocation metrics and logs
3. Click "View logs in CloudWatch" to see detailed logs
4. Set up CloudWatch Alarms if you want notifications on failures

## 8. Checking the Reports

The script will save a CSV report of deleted and skipped objects to:

- `s3://your-bucket/cleanup_logs/s3_cleanup_YYYYMMDD_HHMMSS.csv`

You can review these reports to keep track of what was deleted and what was skipped.

## Advanced Configuration Options

### Multiple Buckets

To clean up multiple buckets:

1. Create multiple EventBridge rules targeting the same Lambda function
2. Configure each rule with a different input JSON specifying different bucket names

### Custom Thresholds

Adjust environment variables or the EventBridge input to customize:
- `days_threshold`: Standard objects older than this will be deleted (default: 90)
- `glacier_min_days`: Minimum age for Glacier objects (default: 91)
- `deep_archive_min_days`: Minimum age for Deep Archive objects (default: 181)

### Enhanced Monitoring

For more detailed monitoring:
1. Set up CloudWatch Dashboards to visualize metrics
2. Create CloudWatch Alarms to notify you of issues
3. Use AWS X-Ray for tracing (requires additional configuration)