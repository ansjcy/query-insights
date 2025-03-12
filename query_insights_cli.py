#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Query Insights CLI.

A command-line interface for interacting with the Query Insights system,
allowing users to generate queries from natural language, predict performance,
and manage model training and deployment.
"""

import os
import json
import logging
import argparse
import subprocess
import sys
from typing import Optional, Dict, Any, List

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_parser() -> argparse.ArgumentParser:
    """Set up the command-line parser with all subcommands."""
    parser = argparse.ArgumentParser(
        description="Query Insights CLI - A natural language interface for OpenSearch query performance prediction",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Predict performance from natural language
  python query_insights_cli.py predict --description "Find all log entries with ERROR status in the last 24 hours" 
  
  # Generate an OpenSearch query from natural language
  python query_insights_cli.py generate --description "Find users who purchased more than 5 items last month"
  
  # Train a new performance prediction model
  python query_insights_cli.py train --data-path "data/query_features.csv" --model-name "query-perf-model"
  
  # Start interactive mode
  python query_insights_cli.py interactive
"""
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Predict command
    predict_parser = subparsers.add_parser("predict", help="Predict query performance from natural language")
    predict_parser.add_argument("--description", required=True, help="Natural language description of the query")
    predict_parser.add_argument("--sagemaker-endpoint", help="SageMaker endpoint name")
    predict_parser.add_argument("--bedrock-model", default="anthropic.claude-3-sonnet-20240229-v1:0", 
                               help="Bedrock model ID for query generation")
    predict_parser.add_argument("--region", default="us-east-1", help="AWS region")
    predict_parser.add_argument("--cluster-info", help="Path to a JSON file with cluster information")
    predict_parser.add_argument("--explain", action="store_true", help="Generate a natural language explanation")
    predict_parser.add_argument("--output", help="Save results to a JSON file")
    
    # Generate command
    generate_parser = subparsers.add_parser("generate", help="Generate an OpenSearch query from natural language")
    generate_parser.add_argument("--description", required=True, help="Natural language description of the query")
    generate_parser.add_argument("--bedrock-model", default="anthropic.claude-3-sonnet-20240229-v1:0", 
                                help="Bedrock model ID for query generation")
    generate_parser.add_argument("--region", default="us-east-1", help="AWS region")
    generate_parser.add_argument("--output", help="Save generated query to a JSON file")
    
    # Train command
    train_parser = subparsers.add_parser("train", help="Train a query performance prediction model")
    train_parser.add_argument("--data-path", required=True, help="Path to the feature dataset (CSV)")
    train_parser.add_argument("--model-name", required=True, help="Name for the trained model")
    train_parser.add_argument("--s3-bucket", help="S3 bucket for storing model artifacts")
    train_parser.add_argument("--region", default="us-east-1", help="AWS region")
    train_parser.add_argument("--instance-type", default="ml.c5.xlarge", help="SageMaker training instance type")
    train_parser.add_argument("--deploy", action="store_true", help="Deploy model after training")
    
    # Deploy command
    deploy_parser = subparsers.add_parser("deploy", help="Deploy a trained model to a SageMaker endpoint")
    deploy_parser.add_argument("--model-name", required=True, help="Name of the trained model to deploy")
    deploy_parser.add_argument("--endpoint-name", help="Name for the SageMaker endpoint")
    deploy_parser.add_argument("--instance-type", default="ml.c5.large", help="SageMaker deployment instance type")
    deploy_parser.add_argument("--region", default="us-east-1", help="AWS region")
    
    # Interactive mode
    interactive_parser = subparsers.add_parser("interactive", help="Start interactive shell for query insights")
    interactive_parser.add_argument("--sagemaker-endpoint", help="SageMaker endpoint name")
    interactive_parser.add_argument("--bedrock-model", default="anthropic.claude-3-sonnet-20240229-v1:0", 
                                   help="Bedrock model ID for query generation")
    interactive_parser.add_argument("--region", default="us-east-1", help="AWS region")
    interactive_parser.add_argument("--cluster-info", help="Path to a JSON file with cluster information")
    
    # Config command
    config_parser = subparsers.add_parser("config", help="Set default configuration values")
    config_parser.add_argument("--sagemaker-endpoint", help="Default SageMaker endpoint name")
    config_parser.add_argument("--bedrock-model", help="Default Bedrock model ID")
    config_parser.add_argument("--region", help="Default AWS region")
    config_parser.add_argument("--cluster-info", help="Default cluster info file path")
    config_parser.add_argument("--s3-bucket", help="Default S3 bucket")
    config_parser.add_argument("--show", action="store_true", help="Show current configuration")
    
    return parser

def load_config() -> Dict[str, Any]:
    """Load the configuration file if it exists."""
    config_path = os.path.expanduser("~/.query_insights_config.json")
    
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load config file: {e}")
    
    return {
        "sagemaker_endpoint": None,
        "bedrock_model": "anthropic.claude-3-sonnet-20240229-v1:0",
        "region": "us-east-1",
        "cluster_info": None,
        "s3_bucket": None,
    }

def save_config(config: Dict[str, Any]) -> None:
    """Save the configuration to a file."""
    config_path = os.path.expanduser("~/.query_insights_config.json")
    
    try:
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
        logger.info(f"Configuration saved to {config_path}")
    except Exception as e:
        logger.error(f"Failed to save configuration: {e}")

def handle_predict(args: argparse.Namespace, config: Dict[str, Any]) -> None:
    """Handle the predict command."""
    # Import the necessary module
    try:
        from nl_query_predictor import NLQueryPredictor
    except ImportError:
        logger.error("Failed to import NLQueryPredictor. Make sure nl_query_predictor.py is in the current directory.")
        return
    
    # Use provided args or fall back to config
    sagemaker_endpoint = args.sagemaker_endpoint or config.get("sagemaker_endpoint")
    if not sagemaker_endpoint:
        logger.error("No SageMaker endpoint specified. Please provide --sagemaker-endpoint or set a default with 'config'.")
        return
        
    bedrock_model = args.bedrock_model or config.get("bedrock_model")
    region = args.region or config.get("region")
    cluster_info = args.cluster_info or config.get("cluster_info")
    
    try:
        # Initialize the predictor
        predictor = NLQueryPredictor(
            sagemaker_endpoint=sagemaker_endpoint,
            bedrock_model_id=bedrock_model,
            region=region,
            cluster_info_path=cluster_info
        )
        
        # Make the prediction
        result = predictor.predict_from_nl(args.description)
        
        # Generate explanation if requested
        if args.explain:
            explanation = predictor.generate_explanation(result)
            result["explanation"] = explanation
        
        # Display results
        print("\n===== GENERATED QUERY =====")
        print(json.dumps(result["structured_query"], indent=2))
        
        print("\n===== PERFORMANCE PREDICTIONS =====")
        print(f"Latency: {result['human_readable']['latency']}")
        print(f"CPU Usage: {result['human_readable']['cpu']}")
        print(f"Memory Usage: {result['human_readable']['memory']}")
        
        if args.explain and "explanation" in result:
            print("\n===== EXPLANATION =====")
            print(result["explanation"])
        
        # Save to file if requested
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(result, f, indent=2)
            print(f"\nResults saved to {args.output}")
            
    except Exception as e:
        logger.error(f"Error during prediction: {e}")

def handle_generate(args: argparse.Namespace, config: Dict[str, Any]) -> None:
    """Handle the generate command."""
    try:
        from bedrock_query_generator import BedrockQueryGenerator
    except ImportError:
        logger.error("Failed to import BedrockQueryGenerator. Make sure bedrock_query_generator.py is in the current directory.")
        return
    
    bedrock_model = args.bedrock_model or config.get("bedrock_model")
    region = args.region or config.get("region")
    
    try:
        # Initialize the generator
        generator = BedrockQueryGenerator(
            model_id=bedrock_model,
            region=region
        )
        
        # Generate the query
        query = generator.generate_query(args.description)
        
        # Display results
        print("\n===== GENERATED QUERY =====")
        print(json.dumps(query, indent=2))
        
        # Save to file if requested
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(query, f, indent=2)
            print(f"\nQuery saved to {args.output}")
            
    except Exception as e:
        logger.error(f"Error during query generation: {e}")

def handle_train(args: argparse.Namespace, config: Dict[str, Any]) -> None:
    """Handle the train command."""
    s3_bucket = args.s3_bucket or config.get("s3_bucket")
    if not s3_bucket:
        logger.error("No S3 bucket specified. Please provide --s3-bucket or set a default with 'config'.")
        return
    
    region = args.region or config.get("region")
    
    try:
        # Call the training script
        cmd = [
            "python", "train_query_insights_model.py",
            "--data-path", args.data_path,
            "--model-name", args.model_name,
            "--s3-bucket", s3_bucket,
            "--region", region,
            "--instance-type", args.instance_type
        ]
        
        if args.deploy:
            cmd.append("--deploy")
            
        logger.info(f"Running command: {' '.join(cmd)}")
        subprocess.run(cmd, check=True)
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Error during model training: {e}")
    except FileNotFoundError:
        logger.error("Train script not found. Make sure train_query_insights_model.py is in the current directory.")

def handle_deploy(args: argparse.Namespace, config: Dict[str, Any]) -> None:
    """Handle the deploy command."""
    region = args.region or config.get("region")
    endpoint_name = args.endpoint_name or args.model_name
    
    try:
        # Import necessary AWS modules
        import boto3
        sagemaker = boto3.client('sagemaker', region_name=region)
        
        # Create model configuration
        model_data_url = f"s3://{config.get('s3_bucket')}/{args.model_name}/model.tar.gz"
        
        # Create model
        model_name = f"{args.model_name}-model"
        logger.info(f"Creating model: {model_name}")
        
        try:
            sagemaker.create_model(
                ModelName=model_name,
                PrimaryContainer={
                    'Image': f"683313688378.dkr.ecr.{region}.amazonaws.com/sagemaker-xgboost:1.7-1",
                    'ModelDataUrl': model_data_url,
                },
                ExecutionRoleArn=config.get("sagemaker_role") or sagemaker.get_execution_role()
            )
        except Exception as e:
            logger.warning(f"Error creating model (it may already exist): {e}")
        
        # Create endpoint configuration
        config_name = f"{endpoint_name}-config"
        logger.info(f"Creating endpoint configuration: {config_name}")
        
        try:
            sagemaker.create_endpoint_config(
                EndpointConfigName=config_name,
                ProductionVariants=[{
                    'VariantName': 'AllTraffic',
                    'ModelName': model_name,
                    'InstanceType': args.instance_type,
                    'InitialInstanceCount': 1
                }]
            )
        except Exception as e:
            logger.warning(f"Error creating endpoint config (it may already exist): {e}")
        
        # Create or update endpoint
        try:
            # Check if endpoint exists
            endpoints = sagemaker.list_endpoints()
            endpoint_exists = any(e['EndpointName'] == endpoint_name for e in endpoints['Endpoints'])
            
            if endpoint_exists:
                logger.info(f"Updating existing endpoint: {endpoint_name}")
                sagemaker.update_endpoint(
                    EndpointName=endpoint_name,
                    EndpointConfigName=config_name
                )
            else:
                logger.info(f"Creating new endpoint: {endpoint_name}")
                sagemaker.create_endpoint(
                    EndpointName=endpoint_name,
                    EndpointConfigName=config_name
                )
                
            logger.info(f"Endpoint deployment initiated. It may take a few minutes to complete.")
            print(f"\nEndpoint '{endpoint_name}' deployment initiated.")
            print("It may take a few minutes for the endpoint to become available.")
            print(f"You can use this endpoint with: --sagemaker-endpoint {endpoint_name}")
            
        except Exception as e:
            logger.error(f"Error deploying endpoint: {e}")
        
    except ImportError:
        logger.error("Missing required AWS packages. Please install boto3: pip install boto3")

def handle_interactive(args: argparse.Namespace, config: Dict[str, Any]) -> None:
    """Handle the interactive command to start an interactive shell."""
    try:
        from nl_query_predictor import NLQueryPredictor
        from bedrock_query_generator import BedrockQueryGenerator
    except ImportError:
        logger.error("Failed to import required modules. Make sure all Python files are in the current directory.")
        return
    
    # Use provided args or fall back to config
    sagemaker_endpoint = args.sagemaker_endpoint or config.get("sagemaker_endpoint")
    if not sagemaker_endpoint:
        logger.error("No SageMaker endpoint specified. Please provide --sagemaker-endpoint or set a default with 'config'.")
        return
        
    bedrock_model = args.bedrock_model or config.get("bedrock_model")
    region = args.region or config.get("region")
    cluster_info = args.cluster_info or config.get("cluster_info")
    
    try:
        # Initialize the predictor
        predictor = NLQueryPredictor(
            sagemaker_endpoint=sagemaker_endpoint,
            bedrock_model_id=bedrock_model,
            region=region,
            cluster_info_path=cluster_info
        )
        
        print("\n===== QUERY INSIGHTS INTERACTIVE MODE =====")
        print("Type 'exit' or 'quit' to exit the interactive shell.")
        print("Type 'explain' before your query to get an explanation of the predictions.")
        print("Example: 'find all logs with error status in the last hour'")
        print("Example: 'explain find users who made purchases over $1000'")
        
        while True:
            try:
                # Get user input
                user_input = input("\n> ").strip()
                
                # Check for exit commands
                if user_input.lower() in ('exit', 'quit'):
                    break
                    
                # Check for explanation flag
                explain = False
                if user_input.lower().startswith('explain '):
                    explain = True
                    user_input = user_input[8:].strip()
                
                if not user_input:
                    continue
                
                # Process the query
                result = predictor.predict_from_nl(user_input)
                
                # Generate explanation if requested
                if explain:
                    explanation = predictor.generate_explanation(result)
                    result["explanation"] = explanation
                
                # Display results
                print("\n===== GENERATED QUERY =====")
                print(json.dumps(result["structured_query"], indent=2))
                
                print("\n===== PERFORMANCE PREDICTIONS =====")
                print(f"Latency: {result['human_readable']['latency']}")
                print(f"CPU Usage: {result['human_readable']['cpu']}")
                print(f"Memory Usage: {result['human_readable']['memory']}")
                
                if explain and "explanation" in result:
                    print("\n===== EXPLANATION =====")
                    print(result["explanation"])
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Error: {e}")
                
        print("\nExiting interactive mode.")
        
    except Exception as e:
        logger.error(f"Error in interactive mode: {e}")

def handle_config(args: argparse.Namespace, config: Dict[str, Any]) -> None:
    """Handle the config command to set default configuration values."""
    if args.show:
        print("\n===== CURRENT CONFIGURATION =====")
        for key, value in config.items():
            print(f"{key}: {value}")
        return
    
    # Update config with provided values
    if args.sagemaker_endpoint:
        config["sagemaker_endpoint"] = args.sagemaker_endpoint
    if args.bedrock_model:
        config["bedrock_model"] = args.bedrock_model
    if args.region:
        config["region"] = args.region
    if args.cluster_info:
        config["cluster_info"] = args.cluster_info
    if args.s3_bucket:
        config["s3_bucket"] = args.s3_bucket
    
    # Save the updated config
    save_config(config)
    print("Configuration updated successfully.")

def main():
    """Main entry point for the Query Insights CLI."""
    parser = setup_parser()
    args = parser.parse_args()
    
    # Load config
    config = load_config()
    
    # Handle commands
    if args.command == "predict":
        handle_predict(args, config)
    elif args.command == "generate":
        handle_generate(args, config)
    elif args.command == "train":
        handle_train(args, config)
    elif args.command == "deploy":
        handle_deploy(args, config)
    elif args.command == "interactive":
        handle_interactive(args, config)
    elif args.command == "config":
        handle_config(args, config)
    else:
        parser.print_help()

if __name__ == "__main__":
    main() 