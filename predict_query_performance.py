#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script for making predictions with the SageMaker model trained on query insights data.
This can be used to predict the performance (latency, CPU, memory) of a query before execution.
"""

import os
import json
import boto3
import argparse
import pandas as pd
import numpy as np
import logging
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Feature lists (must match the ones used for training)
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

class QueryPerformancePredictor:
    """Class for predicting query performance using a SageMaker endpoint."""
    
    def __init__(self, endpoint_name, region='us-east-1'):
        """
        Initialize the predictor.
        
        Args:
            endpoint_name: Name of the SageMaker endpoint
            region: AWS region
        """
        self.endpoint_name = endpoint_name
        self.region = region
        self.sagemaker_runtime = boto3.client('sagemaker-runtime', region_name=region)
        logger.info(f"Initialized predictor for endpoint {endpoint_name} in region {region}")
        
        # Initialize preprocessing pipeline (this should match what was used in training)
        self.preprocessor = self._create_preprocessor()
    
    def _create_preprocessor(self):
        """Create preprocessing pipeline for features."""
        numeric_transformer = Pipeline(steps=[
            ('imputer', SimpleImputer(strategy='median')),
            ('scaler', StandardScaler())
        ])
        
        categorical_transformer = Pipeline(steps=[
            ('imputer', SimpleImputer(strategy='most_frequent')),
            ('onehot', OneHotEncoder(handle_unknown='ignore'))
        ])
        
        return ColumnTransformer(
            transformers=[
                ('num', numeric_transformer, NUMERIC_FEATURES),
                ('cat', categorical_transformer, CATEGORICAL_FEATURES)
            ],
            remainder='drop'
        )
    
    def preprocess_features(self, features_df):
        """
        Preprocess features for prediction.
        
        Args:
            features_df: DataFrame containing query features
            
        Returns:
            Preprocessed features ready for prediction
        """
        logger.info("Preprocessing features for prediction")
        
        # Handle missing values
        features_df = features_df.replace([np.inf, -np.inf], np.nan)
        
        # Convert binary features to integers
        for col in BINARY_FEATURES:
            if col in features_df.columns:
                features_df[col] = features_df[col].astype(int)
        
        # Fit and transform the data
        # In production, you would save and load a pre-fitted preprocessor from training
        features_processed = self.preprocessor.fit_transform(features_df)
        
        # Convert to CSV format expected by SageMaker XGBoost
        if hasattr(features_processed, 'toarray'):
            features_processed = features_processed.toarray()
            
        return features_processed
        
    def predict(self, query_features):
        """
        Predict query performance.
        
        Args:
            query_features: DataFrame with query features
            
        Returns:
            Dictionary with predicted latency, CPU, and memory usage
        """
        if not isinstance(query_features, pd.DataFrame):
            raise TypeError("query_features must be a pandas DataFrame")
        
        logger.info("Preparing query features for prediction")
        
        # Preprocess the features
        features_processed = self.preprocess_features(query_features)
        
        # Convert to CSV format
        csv_data = pd.DataFrame(features_processed).to_csv(header=False, index=False).encode('utf-8')
        
        logger.info(f"Invoking SageMaker endpoint {self.endpoint_name}")
        
        # Invoke the endpoint
        try:
            # For multi-output XGBoost, we need to explicitly request all targets
            response = self.sagemaker_runtime.invoke_endpoint(
                EndpointName=self.endpoint_name,
                ContentType='text/csv',
                Body=csv_data,
                # Add Accept header to ensure we get all prediction values
                Accept='text/csv'
            )
            
            # Parse the result
            result = response['Body'].read().decode('utf-8')
            predictions = np.fromstring(result, sep=',')
            
            if len(predictions) != len(TARGET_FEATURES):
                logger.warning(f"Expected {len(TARGET_FEATURES)} predictions, got {len(predictions)}")
                # If we got fewer predictions than expected, handle it gracefully
                # This could happen if the model wasn't properly trained for multi-output
                
                # Create a dictionary with the predictions we have
                prediction_dict = {}
                
                # Add predictions for available targets
                for i, target in enumerate(TARGET_FEATURES):
                    if i < len(predictions):
                        prediction_dict[target] = float(predictions[i])
                    else:
                        # If we're missing this target, set it to None
                        logger.warning(f"Missing prediction for {target}, setting to None")
                        prediction_dict[target] = None
            else:
                # If we got the expected number of predictions, create the dictionary
                prediction_dict = {
                    TARGET_FEATURES[i]: float(predictions[i]) 
                    for i in range(len(TARGET_FEATURES))
                }
            
            logger.info(f"Prediction successful: {prediction_dict}")
            return prediction_dict
            
        except Exception as e:
            logger.error(f"Error invoking endpoint: {e}")
            raise

def extract_features_from_query(query_json, cluster_info=None):
    """
    Extract features from a query for prediction.
    
    Args:
        query_json: The query in JSON format
        cluster_info: Dictionary with cluster information
        
    Returns:
        DataFrame with query features
    """
    logger.info("Extracting features from query")
    
    # Initialize features dictionary
    features = {}
    
    # Add timestamp
    import time
    features['timestamp_ms'] = int(time.time() * 1000)
    
    # Parse query
    if isinstance(query_json, str):
        try:
            query = json.loads(query_json)
        except json.JSONDecodeError:
            logger.error("Invalid JSON query")
            raise
    else:
        query = query_json
    
    # Extract indices information
    indices = query.get('index', [])
    if isinstance(indices, str):
        indices = [indices]
    features['indices_count'] = len(indices)
    
    # Extract search info
    if 'body' in query:
        body = query['body']
    else:
        body = query
    
    # Total shards (default to 1 if not available)
    features['total_shards'] = cluster_info.get('total_shards', 1) if cluster_info else 1
    
    # Search type
    features['search_type'] = query.get('search_type', 'query_then_fetch')
    
    # Aggregations
    aggs = body.get('aggs', body.get('aggregations', {}))
    features['has_aggregations'] = 1 if aggs else 0
    features['aggregations_count'] = len(aggs) if aggs else 0
    
    # Sort
    sort = body.get('sort', [])
    features['has_sort'] = 1 if sort else 0
    features['sort_count'] = len(sort) if isinstance(sort, list) else 1 if sort else 0
    
    # Source filtering
    source = body.get('_source', None)
    has_source_filtering = False
    source_includes_count = 0
    source_excludes_count = 0
    
    if source is not None:
        if isinstance(source, dict):
            includes = source.get('includes', [])
            excludes = source.get('excludes', [])
            source_includes_count = len(includes) if isinstance(includes, list) else 1 if includes else 0
            source_excludes_count = len(excludes) if isinstance(excludes, list) else 1 if excludes else 0
            has_source_filtering = source_includes_count > 0 or source_excludes_count > 0
        elif isinstance(source, list):
            source_includes_count = len(source)
            has_source_filtering = source_includes_count > 0
        elif source is False:
            has_source_filtering = True
    
    features['has_source_filtering'] = 1 if has_source_filtering else 0
    features['source_includes_count'] = source_includes_count
    features['source_excludes_count'] = source_excludes_count
    
    # Query
    query_obj = body.get('query', None)
    features['has_query'] = 1 if query_obj else 0
    
    if query_obj:
        # Simplified query type extraction
        query_type = next(iter(query_obj)) if isinstance(query_obj, dict) else 'unknown'
        features['query_type'] = query_type
        
        # Query depth
        query_str = json.dumps(query_obj)
        features['query_depth'] = calculate_query_depth(query_str)
    else:
        features['query_type'] = 'none'
        features['query_depth'] = 0
    
    # Size and From
    size = body.get('size', 10)  # Default is 10
    features['has_size'] = 1 if 'size' in body else 0
    features['size_value'] = size
    
    from_val = body.get('from', 0)  # Default is 0
    features['has_from'] = 1 if 'from' in body else 0
    features['from_value'] = from_val
    
    # Cluster information
    if cluster_info:
        features['active_nodes_count'] = cluster_info.get('active_nodes_count', 1)
        features['cluster_status'] = cluster_info.get('cluster_status', 'green')
        features['indices_status'] = cluster_info.get('indices_status', 2)  # Default to green (2)
        features['avg_cpu_utilization'] = cluster_info.get('avg_cpu_utilization', 50)
        features['avg_jvm_heap_used_percent'] = cluster_info.get('avg_jvm_heap_used_percent', 50)
        features['avg_disk_used_percent'] = cluster_info.get('avg_disk_used_percent', 50)
        features['avg_docs_count'] = cluster_info.get('avg_docs_count', 1000)
        features['max_docs_count'] = cluster_info.get('max_docs_count', 1000)
        features['avg_index_size_bytes'] = cluster_info.get('avg_index_size_bytes', 1000000)
    else:
        # Default values
        features['active_nodes_count'] = 1
        features['cluster_status'] = 'green'
        features['indices_status'] = 2
        features['avg_cpu_utilization'] = 50
        features['avg_jvm_heap_used_percent'] = 50
        features['avg_disk_used_percent'] = 50
        features['avg_docs_count'] = 1000
        features['max_docs_count'] = 1000
        features['avg_index_size_bytes'] = 1000000
    
    # Time-based features
    from datetime import datetime
    now = datetime.now()
    features['time_of_day_hour'] = now.hour
    features['day_of_week'] = now.weekday() + 1  # 1-7 for Monday-Sunday
    
    # Convert to DataFrame
    df = pd.DataFrame([features])
    
    logger.info(f"Extracted features: {df.shape[1]} columns")
    return df

def calculate_query_depth(query_string):
    """
    Calculate the depth of a query by counting nested levels.
    
    Args:
        query_string: The query as a string
        
    Returns:
        Depth of the query
    """
    if not query_string:
        return 0
    
    max_depth = 0
    current_depth = 0
    
    for c in query_string:
        if c == '{' or c == '[':
            current_depth += 1
            max_depth = max(max_depth, current_depth)
        elif c == '}' or c == ']':
            current_depth -= 1
    
    return max_depth

def main():
    """Main function to handle command line arguments and predict query performance."""
    parser = argparse.ArgumentParser(description='Predict query performance using a SageMaker model')
    parser.add_argument('--endpoint-name', required=True, help='Name of the SageMaker endpoint')
    parser.add_argument('--region', default='us-east-1', help='AWS region')
    parser.add_argument('--query-file', help='File containing the query JSON')
    parser.add_argument('--query', help='Query JSON as a string')
    parser.add_argument('--cluster-info-file', help='File containing cluster information JSON')
    
    args = parser.parse_args()
    
    # Load query
    if args.query_file:
        with open(args.query_file, 'r') as f:
            query_json = json.load(f)
    elif args.query:
        query_json = args.query
    else:
        logger.error("Either --query-file or --query must be provided")
        return 1
    
    # Load cluster information
    cluster_info = None
    if args.cluster_info_file:
        with open(args.cluster_info_file, 'r') as f:
            cluster_info = json.load(f)
    
    try:
        # Extract features
        features_df = extract_features_from_query(query_json, cluster_info)
        
        # Initialize predictor
        predictor = QueryPerformancePredictor(args.endpoint_name, args.region)
        
        # Make prediction
        predictions = predictor.predict(features_df)
        
        # Format for readability
        formatted_predictions = {
            'latency_ms': f"{predictions['latency_ms']:.2f} ms",
            'cpu_nanos': f"{predictions['cpu_nanos']:.2f} ns (≈ {predictions['cpu_nanos']/1e9:.6f} s)",
            'memory_bytes': f"{predictions['memory_bytes']:.2f} bytes (≈ {predictions['memory_bytes']/1024/1024:.2f} MB)"
        }
        
        # Print results
        print("\nQuery Performance Predictions:")
        print(json.dumps(formatted_predictions, indent=2))
        
        return 0
    
    except Exception as e:
        logger.error(f"Error predicting query performance: {e}")
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main()) 