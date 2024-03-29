Project Description

This repository hosts demo scripts for our data warehousing project centered on traffic, pedestrian, and enforcement dynamics analysis in Basel. The project integrates various AWS services and Python scripting, covering aspects from data collection to storage. The provided scripts primarily serve as demonstration models to outline our project's approach and methodologies.

Note on Original Code

Regrettably, the original code for the Lambda function and Step Function integral to this project was lost due to an account limitation exceeded on our AWS student account. Therefore, the scripts included here represent a conceptual demonstration, albeit simplified, of the intended functionality and should be regarded as illustrative examples.

Scripts Overview

01_lambda.py: A demo version of a Lambda function for fetching non-motorized traffic data from Basel's open data portal, subsequently storing it in an S3 bucket.
02_stepfunction.json: A sample AWS Step Function configuration that demonstrates looping the Lambda function until reaching the current date.
03_load_from_clue_and_transform.py: A Python script designed to load data from AWS Glue, perform necessary transformations, and store the processed data in S3.
04_analysis.py: This script handles the analytical aspect, focusing on data filtering and correlation matrix computation.
05_load_to_rds.py: A script intended for loading transformed data into designated tables within Amazon RDS.

Configuration and Usage

Requirements

Active AWS account with configured access to services like Lambda, S3, Glue, Athena, and RDS.
A Python setup equipped with libraries such as boto3, pandas, requests, and sqlalchemy.

Configuration Steps

1. Setting up Lambda Function (01_lambda.py):

Create an AWS Lambda function and ensure it has the necessary IAM roles for S3 access and API interactions.
Modify S3_BUCKET_NAME and BASE_API_URL to reflect your specific bucket name and API endpoint.

2. AWS Step Function Setup (02_stepfunction.json):

Implement this Step Function in AWS, utilizing the provided JSON structure.
Ensure the Lambda ARN in the resource field is correctly updated.

3. Data Loading and Transformation (03_load_from_clue_and_transform.py):

Input your AWS credentials (aws_access_key_id, aws_secret_access_key, aws_session_token).
Define your AWS region_name.
Specify your s3_output for Athena query results.

4. Performing Data Analysis (04_analysis.py):

Make sure combined_df_path is correctly pointing to your DataFrame.
Execute the script to proceed with data analysis and generate the correlation matrix.

5. Data Loading to RDS (05_load_to_rds.py):

Update with the necessary database credentials (host, port, dbname, user, password).
Ensure combined_df_path directs to your DataFrame.
Execute the script for data insertion into RDS.

Execution Instructions

Execute each script as per the sequence detailed above. Prior to running these scripts, confirm the readiness and proper configuration of all necessary AWS services. Note that modifications might be required for these demo scripts to suit full-scale project deployment.