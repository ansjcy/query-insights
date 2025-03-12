#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Natural Language Query Performance Predictor.

This script provides a bridge between natural language query descriptions and
performance predictions using a combination of Amazon Bedrock LLMs for query generation
and a trained SageMaker model for performance prediction.
"""

import os
import json
import logging
import argparse
import pandas as pd
from typing import Dict, Any, Optional, Union, List

# Import local modules
from bedrock_query_generator import BedrockQueryGenerator
from predict_query_performance import QueryPerformancePredictor, extract_features_from_query

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NLQueryPredictor:
    """
    Natural Language Query Performance Predictor.
    
    This class combines an LLM-based query generator and a performance prediction model
    to predict the performance of OpenSearch queries described in natural language.
    """
    
    def __init__(
        self,
        sagemaker_endpoint: str,
        bedrock_model_id: str = "anthropic.claude-3-sonnet-20240229-v1:0",
        region: str = "us-east-1",
        cluster_info_path: Optional[str] = None
    ):
        """
        Initialize the NLQueryPredictor.
        
        Args:
            sagemaker_endpoint: Name of the SageMaker endpoint for performance prediction
            bedrock_model_id: Bedrock model ID for query generation
            region: AWS region for both SageMaker and Bedrock
            cluster_info_path: Path to a JSON file with cluster information
        """
        # Initialize query generator
        logger.info(f"Initializing Bedrock query generator with model {bedrock_model_id}")
        self.query_generator = BedrockQueryGenerator(
            model_id=bedrock_model_id,
            region=region
        )
        
        # Initialize performance predictor
        logger.info(f"Initializing query performance predictor with endpoint {sagemaker_endpoint}")
        self.performance_predictor = QueryPerformancePredictor(
            endpoint_name=sagemaker_endpoint,
            region=region
        )
        
        # Load cluster information if provided
        self.cluster_info = None
        if cluster_info_path:
            try:
                with open(cluster_info_path, 'r') as f:
                    self.cluster_info = json.load(f)
                logger.info(f"Loaded cluster information from {cluster_info_path}")
            except Exception as e:
                logger.warning(f"Failed to load cluster information: {e}")
                logger.warning("Will use default cluster information")
        
        # Use default cluster info if not provided
        if not self.cluster_info:
            self.cluster_info = {
                "active_nodes_count": 3,
                "cluster_status": "green",
                "indices_status": 2,
                "avg_cpu_utilization": 50,
                "avg_jvm_heap_used_percent": 50,
                "avg_disk_used_percent": 50,
                "avg_docs_count": 1000000,
                "max_docs_count": 2000000,
                "avg_index_size_bytes": 500000000,
                "total_shards": 10
            }
    
    def predict_from_nl(
        self, 
        query_description: str,
        override_cluster_info: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Predict performance metrics for a query described in natural language.
        
        Args:
            query_description: Natural language description of the query
            override_cluster_info: Optional dictionary to override default cluster info
            
        Returns:
            Dictionary with query, features, and predictions
        """
        logger.info(f"Processing natural language query: '{query_description}'")
        
        # Step 1: Generate structured query from natural language
        query = self.query_generator.generate_query(query_description)
        logger.info(f"Generated structured query")
        
        # Step 2: Use the cluster info (default or override)
        cluster_info = override_cluster_info if override_cluster_info else self.cluster_info
        
        # Step 3: Extract features from the query
        features_df = extract_features_from_query(query, cluster_info)
        logger.info(f"Extracted {features_df.shape[1]} features from query")
        
        # Step 4: Predict performance metrics
        predictions = self.performance_predictor.predict(features_df)
        logger.info(f"Generated performance predictions")
        
        # Format and return results
        result = {
            "query_description": query_description,
            "structured_query": query,
            "cluster_info": cluster_info,
            "predictions": {
                "latency_ms": predictions["latency_ms"],
                "cpu_nanos": predictions["cpu_nanos"],
                "memory_bytes": predictions["memory_bytes"]
            },
            "human_readable": {
                "latency": f"{predictions['latency_ms']:.2f} ms",
                "cpu": f"{predictions['cpu_nanos']:.2f} ns (≈ {predictions['cpu_nanos']/1e9:.6f} s)",
                "memory": f"{predictions['memory_bytes']:.2f} bytes (≈ {predictions['memory_bytes']/1024/1024:.2f} MB)"
            }
        }
        
        return result
    
    def generate_explanation(
        self, 
        prediction_result: Dict[str, Any],
        explain_model_id: Optional[str] = None
    ) -> str:
        """
        Generate a natural language explanation of the prediction results.
        
        Args:
            prediction_result: Result from predict_from_nl
            explain_model_id: Optional different model ID for explanations
            
        Returns:
            Natural language explanation of the performance predictions
        """
        # Use the same model or a specified one for explanations
        model_id = explain_model_id or self.query_generator.model_id
        
        # Create a dedicated generator for explanations if needed
        if explain_model_id:
            explainer = BedrockQueryGenerator(model_id=explain_model_id)
        else:
            explainer = self.query_generator
        
        # Create a prompt for the explanation
        query_desc = prediction_result["query_description"]
        query_json = json.dumps(prediction_result["structured_query"], indent=2)
        latency = prediction_result["human_readable"]["latency"]
        cpu = prediction_result["human_readable"]["cpu"]
        memory = prediction_result["human_readable"]["memory"]
        
        prompt = f"""
        I need you to explain the performance predictions for a query in simple terms.
        
        QUERY DESCRIPTION:
        {query_desc}
        
        GENERATED QUERY:
        {query_json}
        
        PREDICTED PERFORMANCE:
        - Latency: {latency}
        - CPU Usage: {cpu}
        - Memory Usage: {memory}
        
        Please explain these predictions in a way that would be understandable to someone
        who is not familiar with OpenSearch. Include:
        1. What factors in the query might impact performance
        2. Any concerns about the predicted resource usage
        3. Suggestions for optimizing the query (if needed)
        4. How these predictions compare to typical queries (is this expensive or cheap?)
        
        Keep your explanation concise and focus on practical insights.
        """
        
        # Use a custom system prompt for the explanation
        system_prompt = """
        You are an expert at explaining OpenSearch query performance. 
        Your goal is to help users understand their query performance predictions in simple terms.
        Be concise but informative, with a focus on practical insights.
        """
        
        # Override the system prompt temporarily
        original_system_prompt = explainer.system_prompt
        explainer.system_prompt = system_prompt
        
        # Generate the explanation using the LLM
        body = explainer._prepare_claude_body(prompt)
        
        try:
            response = explainer.bedrock_runtime.invoke_model(
                modelId=model_id,
                body=json.dumps(body)
            )
            response_body = json.loads(response["body"].read().decode("utf-8"))
            
            # Process response based on model
            if "anthropic.claude" in model_id:
                explanation = response_body["content"][0]["text"]
            elif "amazon.titan" in model_id:
                explanation = response_body["results"][0]["outputText"]
            elif "meta.llama" in model_id:
                explanation = response_body["generation"]
            else:
                explanation = str(response_body)
                
            # Restore the original system prompt
            explainer.system_prompt = original_system_prompt
            
            return explanation
            
        except Exception as e:
            logger.error(f"Error generating explanation: {e}")
            # Restore the original system prompt
            explainer.system_prompt = original_system_prompt
            return "Unable to generate explanation due to an error."

def main():
    """Command-line interface for the natural language query predictor."""
    parser = argparse.ArgumentParser(
        description="Predict OpenSearch query performance from natural language descriptions"
    )
    
    parser.add_argument(
        "--description", 
        required=True, 
        help="Natural language description of the query"
    )
    parser.add_argument(
        "--sagemaker-endpoint", 
        required=True, 
        help="SageMaker endpoint for performance prediction"
    )
    parser.add_argument(
        "--bedrock-model", 
        default="anthropic.claude-3-sonnet-20240229-v1:0", 
        help="Bedrock model ID for query generation"
    )
    parser.add_argument(
        "--region", 
        default="us-east-1", 
        help="AWS region"
    )
    parser.add_argument(
        "--cluster-info", 
        help="Path to a JSON file with cluster information"
    )
    parser.add_argument(
        "--explain", 
        action="store_true", 
        help="Generate a natural language explanation of the predictions"
    )
    parser.add_argument(
        "--output", 
        help="Path to save the results as JSON"
    )
    
    args = parser.parse_args()
    
    # Initialize predictor
    predictor = NLQueryPredictor(
        sagemaker_endpoint=args.sagemaker_endpoint,
        bedrock_model_id=args.bedrock_model,
        region=args.region,
        cluster_info_path=args.cluster_info
    )
    
    # Predict performance
    result = predictor.predict_from_nl(args.description)
    
    # Generate explanation if requested
    if args.explain:
        explanation = predictor.generate_explanation(result)
        result["explanation"] = explanation
    
    # Print the results
    print("\n===== GENERATED QUERY =====")
    print(json.dumps(result["structured_query"], indent=2))
    
    print("\n===== PERFORMANCE PREDICTIONS =====")
    print(f"Latency: {result['human_readable']['latency']}")
    print(f"CPU Usage: {result['human_readable']['cpu']}")
    print(f"Memory Usage: {result['human_readable']['memory']}")
    
    if args.explain:
        print("\n===== EXPLANATION =====")
        print(result["explanation"])
    
    # Save to file if requested
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(result, f, indent=2)
        print(f"\nResults saved to {args.output}")

if __name__ == "__main__":
    main() 