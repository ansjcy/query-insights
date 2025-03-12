#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Query Insights Demo.

This script demonstrates the natural language interface for OpenSearch query
performance prediction. It includes examples of generating queries from natural
language and predicting their performance.
"""

import os
import json
import time
import argparse
from typing import Optional, List, Dict, Any
import pandas as pd
from rich.console import Console
from rich.panel import Panel
from rich.markdown import Markdown
from rich.table import Table
from rich.syntax import Syntax

# Import local modules
try:
    from bedrock_query_generator import BedrockQueryGenerator
    from nl_query_predictor import NLQueryPredictor
except ImportError:
    print("ERROR: Required modules not found. Make sure bedrock_query_generator.py "
          "and nl_query_predictor.py are in the current directory.")
    exit(1)

# Initialize console for rich output
console = Console()

# Example queries for demonstration
EXAMPLE_QUERIES = [
    "Find all log entries with ERROR status in the last 24 hours sorted by timestamp",
    "Show me users who purchased more than 5 items last month",
    "Find documents where field 'category' contains 'electronics' AND price is greater than 100, sorted by rating",
    "Perform a fuzzy search for 'elasticsearch' with minimum score of 0.7 across all fields",
    "Return the count of documents per status field aggregated by day for the last week",
    "Get the 10 most recent transactions with amount greater than $1000 that have status 'completed'",
    "Find documents matching 'server crash' using phrase matching with a slop of 2",
    "Perform a nested query to find orders where at least one item has category 'books' and price less than $20"
]

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Query Insights Demo")
    
    parser.add_argument(
        "--sagemaker-endpoint", 
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
        default="demo_cluster_info.json",
        help="Path to a JSON file with cluster information"
    )
    parser.add_argument(
        "--interactive", 
        action="store_true",
        help="Run in interactive mode instead of using example queries"
    )
    
    return parser.parse_args()

def create_demo_cluster_info() -> Dict[str, Any]:
    """Create a sample cluster info file for demo purposes."""
    cluster_info = {
        "active_nodes_count": 3,
        "cluster_status": "green",
        "indices_status": 2,
        "avg_cpu_utilization": 45,
        "avg_jvm_heap_used_percent": 62,
        "avg_disk_used_percent": 58,
        "avg_docs_count": 5000000,
        "max_docs_count": 8000000,
        "avg_index_size_bytes": 2000000000,
        "total_shards": 15
    }
    
    # Save to file
    with open("demo_cluster_info.json", "w") as f:
        json.dump(cluster_info, f, indent=2)
    
    return cluster_info

def display_intro() -> None:
    """Display introduction to the demo."""
    intro_text = """
    # Query Insights Demo
    
    This demo shows how the Query Insights system can:
    
    1. Convert natural language descriptions into structured OpenSearch queries
    2. Predict performance metrics for those queries
    3. Provide explanations of query performance in plain language
    
    The system uses:
    * Amazon Bedrock for natural language understanding
    * A machine learning model trained on query performance data
    * Feature extraction to identify query characteristics
    """
    
    console.print(Panel(Markdown(intro_text), title="Welcome to Query Insights"))
    console.print()

def display_query_results(
    query_description: str,
    structured_query: Dict[str, Any],
    predictions: Dict[str, Any],
    explanation: Optional[str] = None
) -> None:
    """Display the results of query generation and performance prediction."""
    # Display query description
    console.print(Panel(query_description, title="Natural Language Query"))
    
    # Display generated query
    query_json = json.dumps(structured_query, indent=2)
    console.print(Panel(Syntax(query_json, "json"), title="Generated OpenSearch Query"))
    
    # Display performance predictions
    table = Table(title="Performance Predictions")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")
    
    table.add_row("Latency", predictions["human_readable"]["latency"])
    table.add_row("CPU Usage", predictions["human_readable"]["cpu"])
    table.add_row("Memory Usage", predictions["human_readable"]["memory"])
    
    console.print(table)
    
    # Display explanation if available
    if explanation:
        console.print(Panel(Markdown(explanation), title="Performance Explanation"))
    
    console.print("\n" + "-" * 80 + "\n")

def run_demo(
    predictor: NLQueryPredictor,
    query_list: List[str],
    with_explanations: bool = True
) -> None:
    """Run the demo with the provided query list."""
    for i, query in enumerate(query_list, 1):
        console.print(f"[bold cyan]Example {i}/{len(query_list)}[/bold cyan]")
        
        # Generate query and predict performance
        try:
            result = predictor.predict_from_nl(query)
            
            # Generate explanation
            explanation = None
            if with_explanations:
                explanation = predictor.generate_explanation(result)
            
            # Display results
            display_query_results(
                query,
                result["structured_query"],
                result["predictions"],
                explanation
            )
            
            # Small pause between examples
            if i < len(query_list):
                time.sleep(1)
                
        except Exception as e:
            console.print(f"[bold red]Error processing query:[/bold red] {e}")
    
    console.print("[bold green]Demo completed![/bold green]")

def interactive_mode(predictor: NLQueryPredictor) -> None:
    """Run the demo in interactive mode."""
    console.print("[bold]Interactive Mode[/bold]")
    console.print("Enter your query descriptions or type 'exit' to quit.")
    console.print("Type 'explain' before your query to get an explanation.\n")
    
    while True:
        try:
            # Get user input
            query = console.input("[bold cyan]> [/bold cyan]").strip()
            
            if query.lower() in ('exit', 'quit'):
                break
                
            # Check for explanation flag
            explain = False
            if query.lower().startswith('explain '):
                explain = True
                query = query[8:].strip()
            
            if not query:
                continue
            
            # Process the query
            result = predictor.predict_from_nl(query)
            
            # Generate explanation if requested
            explanation = None
            if explain:
                explanation = predictor.generate_explanation(result)
            
            # Display results
            display_query_results(
                query,
                result["structured_query"],
                result["predictions"],
                explanation
            )
            
        except KeyboardInterrupt:
            break
        except Exception as e:
            console.print(f"[bold red]Error:[/bold red] {e}")

def main() -> None:
    """Main function to run the demo."""
    args = parse_args()
    
    # Display intro
    display_intro()
    
    # Create demo cluster info if needed
    if not os.path.exists(args.cluster_info):
        console.print("[yellow]Creating demo cluster info file...[/yellow]")
        create_demo_cluster_info()
    
    # Check if SageMaker endpoint is provided
    if not args.sagemaker_endpoint:
        console.print("[bold red]Error:[/bold red] SageMaker endpoint is required.")
        console.print("Please provide it using the --sagemaker-endpoint argument.")
        return
    
    # Initialize the predictor
    try:
        console.print("[yellow]Initializing Query Insights system...[/yellow]")
        predictor = NLQueryPredictor(
            sagemaker_endpoint=args.sagemaker_endpoint,
            bedrock_model_id=args.bedrock_model,
            region=args.region,
            cluster_info_path=args.cluster_info
        )
        console.print("[green]Initialization complete![/green]\n")
    except Exception as e:
        console.print(f"[bold red]Error initializing system:[/bold red] {e}")
        return
    
    # Run demo or interactive mode
    if args.interactive:
        interactive_mode(predictor)
    else:
        run_demo(predictor, EXAMPLE_QUERIES)

if __name__ == "__main__":
    main() 