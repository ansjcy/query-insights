#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SageMaker continuous training script for Query Insights performance prediction.
This script handles loading data from CSV files, preprocessing, model training,
and setting up continuous retraining as new data becomes available.
"""

import os
import glob
import argparse
import pandas as pd
import numpy as np
import boto3
import json
import time
import logging
from datetime import datetime, timedelta
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# SageMaker settings
REGION = 'us-east-1'  # Replace with your AWS region
ROLE_ARN = 'YOUR_SAGEMAKER_ROLE_ARN'  # Replace with your SageMaker role ARN
BUCKET_NAME = 'YOUR_S3_BUCKET'  # Replace with your S3 bucket name
PREFIX = 'query-insights'  # S3 prefix for storing data and models
MODEL_NAME = 'query-insights-model'  # Base name for the model

# Feature configuration
NUMERIC_FEATURES = [
    'timestamp_ms', 'indices_count', 'total_shards', 'aggregations_count', 
    'sort_count', 'source_includes_count', 'source_excludes_count', 'query_depth',
    'size_value', 'from_value', 'active_nodes_count', 'indices_status',
    'avg_cpu_utilization', 'avg_jvm_heap_used_percent', 'avg_disk_used_percent',
    'avg_docs_count', 'max_docs_count', 'avg_index_size_bytes',
    'time_of_day_hour', 'day_of_week'
]

CATEGORICAL_FEATURES = [
    'search_type', 'query_type', 'cluster_status',
    'has_aggregations', 'has_sort', 'has_source_filtering',
    'has_query', 'has_size', 'has_from'
]

BINARY_FEATURES = [
    feature for feature in CATEGORICAL_FEATURES 
    if feature.startswith('has_')
]

TARGET_FEATURES = [
    'latency_ms', 'cpu_nanos', 'memory_bytes'
]

# Function to load and preprocess the CSV data
def load_data(data_dir, days_to_include=7):
    """
    Load CSV files from a directory, optionally filtering by age.
    
    Args:
        data_dir: Directory containing CSV files
        days_to_include: How many days of recent data to include
        
    Returns:
        DataFrame containing combined data
    """
    logger.info(f"Loading data from {data_dir}")
    
    # Get list of CSV files in the directory
    pattern = os.path.join(data_dir, "query_features_*.csv")
    all_files = glob.glob(pattern)
    
    # Filter files by date if requested
    if days_to_include:
        cutoff_date = datetime.now() - timedelta(days=days_to_include)
        filtered_files = []
        for file_path in all_files:
            # Extract date from filename (format: query_features_YYYY-MM-DD.csv)
            filename = os.path.basename(file_path)
            date_str = filename.replace('query_features_', '').replace('.csv', '')
            try:
                file_date = datetime.strptime(date_str, '%Y-%m-%d')
                if file_date >= cutoff_date:
                    filtered_files.append(file_path)
            except ValueError:
                # If filename doesn't match expected format, include it anyway
                filtered_files.append(file_path)
        files_to_read = filtered_files
    else:
        files_to_read = all_files
    
    if not files_to_read:
        raise ValueError(f"No CSV files found in {data_dir} for the specified time range")
        
    logger.info(f"Found {len(files_to_read)} files to process")
    
    # Read and combine all CSV files
    dfs = []
    for file in files_to_read:
        logger.info(f"Reading file: {file}")
        try:
            df = pd.read_csv(file)
            dfs.append(df)
        except Exception as e:
            logger.warning(f"Error reading file {file}: {e}")
    
    if not dfs:
        raise ValueError("No valid data found in the specified files")
    
    # Combine all dataframes
    combined_df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Loaded {len(combined_df)} records from {len(dfs)} files")
    
    return combined_df

def preprocess_data(df):
    """
    Preprocess the data for training.
    
    Args:
        df: DataFrame with raw data
        
    Returns:
        X: Features for training
        y: Target variables
    """
    logger.info("Preprocessing data")
    
    # Remove duplicates
    df = df.drop_duplicates()
    
    # Handle missing values
    df = df.replace([np.inf, -np.inf], np.nan)
    
    # For binary features, convert to 0/1
    for col in BINARY_FEATURES:
        if col in df.columns:
            df[col] = df[col].astype(int)
    
    # Extract features and targets
    X = df.drop(TARGET_FEATURES, axis=1, errors='ignore')
    
    # If targets are not present (prediction mode), return None for y
    if all(target in df.columns for target in TARGET_FEATURES):
        y = df[TARGET_FEATURES]
    else:
        y = None
    
    logger.info(f"Preprocessed data shape: X={X.shape}, y={None if y is None else y.shape}")
    return X, y

def upload_data_to_s3(df, bucket, prefix, filename):
    """
    Upload a DataFrame to S3 as a CSV file.
    
    Args:
        df: DataFrame to upload
        bucket: S3 bucket name
        prefix: S3 prefix
        filename: Filename to use in S3
        
    Returns:
        S3 URI of the uploaded file
    """
    logger.info(f"Uploading data to S3 bucket {bucket}/{prefix}/{filename}")
    
    # Create a temporary file
    local_path = f"/tmp/{filename}"
    df.to_csv(local_path, index=False)
    
    # Upload the file to S3
    s3_client = boto3.client('s3', region_name=REGION)
    s3_client.upload_file(local_path, bucket, f"{prefix}/{filename}")
    
    # Remove the temporary file
    os.remove(local_path)
    
    # Return the S3 URI
    s3_uri = f"s3://{bucket}/{prefix}/{filename}"
    logger.info(f"Data uploaded to {s3_uri}")
    return s3_uri

def create_sagemaker_training_job(train_data_s3_uri, job_name=None):
    """
    Create a SageMaker training job.
    
    Args:
        train_data_s3_uri: S3 URI for the training data
        job_name: Optional job name (generated if not provided)
        
    Returns:
        Job name
    """
    if job_name is None:
        timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        job_name = f"{MODEL_NAME}-{timestamp}"
    
    logger.info(f"Creating SageMaker training job: {job_name}")
    
    # Create SageMaker client
    sagemaker_client = boto3.client('sagemaker', region_name=REGION)
    
    # Define output path for the model artifacts
    output_path = f"s3://{BUCKET_NAME}/{PREFIX}/models/{job_name}/output"
    
    # Define hyperparameters for XGBoost model
    hyperparameters = {
        "objective": "reg:squarederror",
        "num_round": "100",
        "max_depth": "6",
        "eta": "0.2",
        "subsample": "0.8",
        "colsample_bytree": "0.8"
    }
    
    # Create the training job
    sagemaker_client.create_training_job(
        TrainingJobName=job_name,
        AlgorithmSpecification={
            'TrainingImage': f"683313688378.dkr.ecr.{REGION}.amazonaws.com/sagemaker-xgboost:1.5-1",
            'TrainingInputMode': 'File'
        },
        RoleArn=ROLE_ARN,
        InputDataConfig=[
            {
                'ChannelName': 'train',
                'DataSource': {
                    'S3DataSource': {
                        'S3DataType': 'S3Prefix',
                        'S3Uri': train_data_s3_uri,
                        'S3DataDistributionType': 'FullyReplicated'
                    }
                },
                'ContentType': 'text/csv',
                'CompressionType': 'None'
            }
        ],
        OutputDataConfig={
            'S3OutputPath': output_path
        },
        ResourceConfig={
            'InstanceType': 'ml.m5.xlarge',
            'InstanceCount': 1,
            'VolumeSizeInGB': 10
        },
        StoppingCondition={
            'MaxRuntimeInSeconds': 86400  # 24 hours max runtime
        },
        HyperParameters=hyperparameters
    )
    
    logger.info(f"Training job {job_name} started. Output will be stored at {output_path}")
    return job_name

def wait_for_training_job(job_name):
    """
    Wait for a SageMaker training job to complete.
    
    Args:
        job_name: Name of the training job
        
    Returns:
        Status of the job ('Completed', 'Failed', etc.)
    """
    logger.info(f"Waiting for training job {job_name} to complete...")
    
    sagemaker_client = boto3.client('sagemaker', region_name=REGION)
    
    while True:
        response = sagemaker_client.describe_training_job(TrainingJobName=job_name)
        status = response['TrainingJobStatus']
        
        if status in ['Completed', 'Failed', 'Stopped']:
            logger.info(f"Training job {job_name} finished with status: {status}")
            return status
        
        logger.info(f"Training job status: {status}")
        time.sleep(60)  # Check every minute

def create_model(job_name):
    """
    Create a SageMaker model from a completed training job.
    
    Args:
        job_name: Name of the completed training job
        
    Returns:
        Name of the created model
    """
    logger.info(f"Creating model from training job {job_name}")
    
    sagemaker_client = boto3.client('sagemaker', region_name=REGION)
    
    # Get model artifact S3 URI from the training job
    response = sagemaker_client.describe_training_job(TrainingJobName=job_name)
    model_artifact = response['ModelArtifacts']['S3ModelArtifacts']
    
    # Create the model
    model_name = f"{MODEL_NAME}-{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
    
    sagemaker_client.create_model(
        ModelName=model_name,
        PrimaryContainer={
            'Image': f"683313688378.dkr.ecr.{REGION}.amazonaws.com/sagemaker-xgboost:1.5-1",
            'ModelDataUrl': model_artifact
        },
        ExecutionRoleArn=ROLE_ARN
    )
    
    logger.info(f"Model {model_name} created")
    return model_name

def create_endpoint_config(model_name):
    """
    Create a SageMaker endpoint configuration.
    
    Args:
        model_name: Name of the model
        
    Returns:
        Name of the endpoint configuration
    """
    logger.info(f"Creating endpoint configuration for model {model_name}")
    
    sagemaker_client = boto3.client('sagemaker', region_name=REGION)
    
    # Create endpoint configuration
    endpoint_config_name = f"{model_name}-config"
    
    sagemaker_client.create_endpoint_config(
        EndpointConfigName=endpoint_config_name,
        ProductionVariants=[
            {
                'VariantName': 'default',
                'ModelName': model_name,
                'InitialInstanceCount': 1,
                'InstanceType': 'ml.m5.large'
            }
        ]
    )
    
    logger.info(f"Endpoint configuration {endpoint_config_name} created")
    return endpoint_config_name

def create_or_update_endpoint(endpoint_name, endpoint_config_name):
    """
    Create or update a SageMaker endpoint.
    
    Args:
        endpoint_name: Name of the endpoint
        endpoint_config_name: Name of the endpoint configuration
        
    Returns:
        None
    """
    logger.info(f"Creating or updating endpoint {endpoint_name} with config {endpoint_config_name}")
    
    sagemaker_client = boto3.client('sagemaker', region_name=REGION)
    
    # Check if the endpoint already exists
    try:
        sagemaker_client.describe_endpoint(EndpointName=endpoint_name)
        # If the endpoint exists, update it
        logger.info(f"Endpoint {endpoint_name} exists, updating...")
        sagemaker_client.update_endpoint(
            EndpointName=endpoint_name,
            EndpointConfigName=endpoint_config_name
        )
    except sagemaker_client.exceptions.ClientError:
        # If the endpoint doesn't exist, create it
        logger.info(f"Endpoint {endpoint_name} doesn't exist, creating...")
        sagemaker_client.create_endpoint(
            EndpointName=endpoint_name,
            EndpointConfigName=endpoint_config_name
        )
    
    logger.info(f"Endpoint {endpoint_name} creation/update initiated")

def wait_for_endpoint(endpoint_name):
    """
    Wait for a SageMaker endpoint to be in service.
    
    Args:
        endpoint_name: Name of the endpoint
        
    Returns:
        Status of the endpoint
    """
    logger.info(f"Waiting for endpoint {endpoint_name} to be in service...")
    
    sagemaker_client = boto3.client('sagemaker', region_name=REGION)
    
    while True:
        response = sagemaker_client.describe_endpoint(EndpointName=endpoint_name)
        status = response['EndpointStatus']
        
        if status == 'InService':
            logger.info(f"Endpoint {endpoint_name} is now in service")
            return status
        elif status in ['Failed', 'OutOfService']:
            logger.error(f"Endpoint {endpoint_name} status: {status}")
            return status
        
        logger.info(f"Endpoint status: {status}")
        time.sleep(60)  # Check every minute

def prepare_data_for_training(df):
    """
    Prepare the data for XGBoost training.
    
    Args:
        df: DataFrame with raw data
        
    Returns:
        DataFrame ready for training
    """
    logger.info("Preparing data for XGBoost training")
    
    # Separate features and targets
    X, y = preprocess_data(df)
    
    if y is None:
        raise ValueError("Target features not found in the data")
    
    # Create a preprocessing pipeline
    numeric_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='median')),
        ('scaler', StandardScaler())
    ])
    
    categorical_transformer = Pipeline(steps=[
        ('imputer', SimpleImputer(strategy='most_frequent')),
        ('onehot', OneHotEncoder(handle_unknown='ignore'))
    ])
    
    # Identify which columns are present in the data
    numeric_features = [f for f in NUMERIC_FEATURES if f in X.columns]
    categorical_features = [f for f in CATEGORICAL_FEATURES if f in X.columns]
    
    # Create the column transformer
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', numeric_transformer, numeric_features),
            ('cat', categorical_transformer, categorical_features)
        ],
        remainder='drop'  # Drop any columns not specified
    )
    
    # Apply preprocessing
    X_processed = preprocessor.fit_transform(X)
    
    # For XGBoost on SageMaker, we need to convert to a CSV format without headers
    # Convert sparse matrix to dense if needed
    if hasattr(X_processed, 'toarray'):
        X_processed = X_processed.toarray()
    
    # Combine features and targets
    # XGBoost expects target in the first column for multi-target regression
    train_data = pd.DataFrame(X_processed)
    
    # Add targets as separate columns at the beginning
    for i, target in enumerate(TARGET_FEATURES):
        train_data.insert(i, target, y[target].values)
    
    logger.info(f"Prepared data shape: {train_data.shape}")
    return train_data

def main():
    parser = argparse.ArgumentParser(description='Train a model for Query Insights performance prediction')
    parser.add_argument('--data-dir', required=True, help='Directory containing feature embedding CSV files')
    parser.add_argument('--days', type=int, default=7, help='Number of days of data to include')
    parser.add_argument('--endpoint-name', default='query-insights-endpoint', help='Name for the SageMaker endpoint')
    parser.add_argument('--s3-bucket', help='S3 bucket to use (overrides default)')
    parser.add_argument('--s3-prefix', help='S3 prefix to use (overrides default)')
    parser.add_argument('--region', help='AWS region (overrides default)')
    parser.add_argument('--role-arn', help='SageMaker IAM role ARN (overrides default)')
    
    args = parser.parse_args()
    
    # Override defaults if provided
    global BUCKET_NAME, PREFIX, REGION, ROLE_ARN
    if args.s3_bucket:
        BUCKET_NAME = args.s3_bucket
    if args.s3_prefix:
        PREFIX = args.s3_prefix
    if args.region:
        REGION = args.region
    if args.role_arn:
        ROLE_ARN = args.role_arn
    
    try:
        # Load and preprocess the data
        df = load_data(args.data_dir, args.days)
        
        # Prepare data for XGBoost
        train_data = prepare_data_for_training(df)
        
        # Upload the data to S3
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        train_data_s3_uri = upload_data_to_s3(
            train_data, 
            BUCKET_NAME, 
            f"{PREFIX}/data/{timestamp}", 
            "train.csv"
        )
        
        # Create a training job
        job_name = create_sagemaker_training_job(train_data_s3_uri)
        
        # Wait for the training job to complete
        job_status = wait_for_training_job(job_name)
        
        if job_status == 'Completed':
            # Create a model from the training job
            model_name = create_model(job_name)
            
            # Create an endpoint configuration
            endpoint_config_name = create_endpoint_config(model_name)
            
            # Create or update the endpoint
            create_or_update_endpoint(args.endpoint_name, endpoint_config_name)
            
            # Wait for the endpoint to be in service
            endpoint_status = wait_for_endpoint(args.endpoint_name)
            
            if endpoint_status == 'InService':
                logger.info(f"Model deployed successfully to endpoint {args.endpoint_name}")
            else:
                logger.error(f"Endpoint deployment failed with status {endpoint_status}")
        else:
            logger.error(f"Training job failed with status {job_status}")
    
    except Exception as e:
        logger.error(f"Error in training pipeline: {e}")
        raise

# Continuous training function
def setup_continuous_training(data_dir, endpoint_name, interval_hours=24):
    """
    Set up continuous training that runs at regular intervals.
    
    Args:
        data_dir: Directory containing feature embedding CSV files
        endpoint_name: Name for the SageMaker endpoint
        interval_hours: How often to run training (in hours)
        
    Returns:
        None
    """
    logger.info(f"Setting up continuous training to run every {interval_hours} hours")
    
    while True:
        try:
            # Run the main training pipeline
            main_args = ["--data-dir", data_dir, "--endpoint-name", endpoint_name]
            args = parser.parse_args(main_args)
            
            global BUCKET_NAME, PREFIX, REGION, ROLE_ARN
            if args.s3_bucket:
                BUCKET_NAME = args.s3_bucket
            if args.s3_prefix:
                PREFIX = args.s3_prefix
            if args.region:
                REGION = args.region
            if args.role_arn:
                ROLE_ARN = args.role_arn
                
            # Run the main function
            main()
            
            logger.info(f"Training completed successfully. Next run in {interval_hours} hours")
        except Exception as e:
            logger.error(f"Error in continuous training: {e}")
        
        # Sleep until the next training run
        time.sleep(interval_hours * 3600)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Train a model for Query Insights performance prediction')
    parser.add_argument('--data-dir', required=True, help='Directory containing feature embedding CSV files')
    parser.add_argument('--days', type=int, default=7, help='Number of days of data to include')
    parser.add_argument('--endpoint-name', default='query-insights-endpoint', help='Name for the SageMaker endpoint')
    parser.add_argument('--s3-bucket', help='S3 bucket to use (overrides default)')
    parser.add_argument('--s3-prefix', help='S3 prefix to use (overrides default)')
    parser.add_argument('--region', help='AWS region (overrides default)')
    parser.add_argument('--role-arn', help='SageMaker IAM role ARN (overrides default)')
    parser.add_argument('--continuous', action='store_true', help='Run in continuous mode')
    parser.add_argument('--interval', type=int, default=24, help='Interval between training jobs (hours)')
    
    args = parser.parse_args()
    
    if args.continuous:
        setup_continuous_training(args.data_dir, args.endpoint_name, args.interval)
    else:
        main() 