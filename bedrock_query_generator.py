#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Amazon Bedrock client for generating OpenSearch queries from natural language inputs.
This script provides a class that can translate a user's natural language query description
into a structured OpenSearch query.
"""

import json
import logging
import boto3
import os
from typing import Dict, Any, Optional, List, Union

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BedrockQueryGenerator:
    """Client for generating OpenSearch queries using Amazon Bedrock LLMs."""
    
    # Default system prompt for query generation
    DEFAULT_SYSTEM_PROMPT = """
    You are an expert OpenSearch query generator. 
    Your task is to convert natural language query descriptions into valid JSON structured OpenSearch queries.
    
    Guidelines:
    - Always return valid JSON that follows the OpenSearch Query DSL format
    - Include both the index and body parts of the query
    - Add appropriate sort, size, and pagination options when mentioned
    - Include aggregations when statistical analysis is requested
    - Use match, term, range, and bool queries appropriately based on the request
    - Use source filtering when specific fields are requested
    - Be conservative with resource usage - don't use large size values unless specified
    
    Return only the JSON query without any explanation or markdown formatting.
    """
    
    def __init__(
        self, 
        model_id: str = "anthropic.claude-3-sonnet-20240229-v1:0", 
        region: str = "us-east-1",
        max_tokens: int = 2000,
        temperature: float = 0.1,
        system_prompt: Optional[str] = None
    ):
        """
        Initialize the BedrockQueryGenerator.
        
        Args:
            model_id: The Bedrock model ID to use
            region: AWS region for Bedrock
            max_tokens: Maximum tokens to generate
            temperature: Temperature for generation (lower = more deterministic)
            system_prompt: Custom system prompt (uses default if None)
        """
        self.model_id = model_id
        self.region = region
        self.max_tokens = max_tokens
        self.temperature = temperature
        self.system_prompt = system_prompt or self.DEFAULT_SYSTEM_PROMPT
        
        # Initialize Bedrock client
        try:
            self.bedrock_runtime = boto3.client(
                service_name="bedrock-runtime",
                region_name=self.region
            )
            logger.info(f"Initialized Bedrock client with model {model_id} in region {region}")
        except Exception as e:
            logger.error(f"Failed to initialize Bedrock client: {e}")
            raise
    
    def generate_query(self, query_description: str) -> Dict[str, Any]:
        """
        Generate an OpenSearch query from a natural language description.
        
        Args:
            query_description: Natural language description of the query
            
        Returns:
            Dictionary containing the generated OpenSearch query
        """
        logger.info(f"Generating query from description: {query_description}")
        
        # Prepare the prompt
        prompt = self._prepare_prompt(query_description)
        
        # Select appropriate invocation body based on model
        if "anthropic.claude" in self.model_id:
            # Claude 3 models
            body = self._prepare_claude_body(prompt)
        elif "amazon.titan" in self.model_id:
            # Amazon Titan models
            body = self._prepare_titan_body(prompt)
        elif "meta.llama" in self.model_id:
            # Llama 2 models
            body = self._prepare_llama_body(prompt)
        else:
            raise ValueError(f"Unsupported model ID: {self.model_id}")
        
        # Call Bedrock API
        try:
            response = self.bedrock_runtime.invoke_model(
                modelId=self.model_id,
                body=json.dumps(body)
            )
            response_body = json.loads(response["body"].read().decode("utf-8"))
            
            # Process response based on model
            if "anthropic.claude" in self.model_id:
                output = response_body["content"][0]["text"]
            elif "amazon.titan" in self.model_id:
                output = response_body["results"][0]["outputText"]
            elif "meta.llama" in self.model_id:
                output = response_body["generation"]
            else:
                output = str(response_body)
            
            # Extract the JSON query from the output
            opensearch_query = self._extract_json_query(output)
            logger.info(f"Successfully generated query")
            return opensearch_query
            
        except Exception as e:
            logger.error(f"Error generating query: {e}")
            raise
    
    def _prepare_prompt(self, query_description: str) -> str:
        """
        Prepare the prompt for the LLM.
        
        Args:
            query_description: Natural language description of the query
            
        Returns:
            Full prompt for the LLM
        """
        # Add specific instructions to get a proper JSON response
        prompt = f"""
        Convert the following natural language query description into a valid OpenSearch query:
        
        QUERY DESCRIPTION:
        {query_description}
        
        Generate a complete OpenSearch query in JSON format that includes both the 'index' and 'body' parts.
        """
        return prompt
    
    def _prepare_claude_body(self, prompt: str) -> Dict[str, Any]:
        """
        Prepare the request body for Claude models.
        
        Args:
            prompt: The prompt for the model
            
        Returns:
            Request body for Claude API
        """
        return {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": self.max_tokens,
            "temperature": self.temperature,
            "system": self.system_prompt,
            "messages": [
                {
                    "role": "user",
                    "content": prompt
                }
            ]
        }
    
    def _prepare_titan_body(self, prompt: str) -> Dict[str, Any]:
        """
        Prepare the request body for Titan models.
        
        Args:
            prompt: The prompt for the model
            
        Returns:
            Request body for Titan API
        """
        return {
            "inputText": f"<system>{self.system_prompt}</system>\n\n<user>{prompt}</user>",
            "textGenerationConfig": {
                "maxTokenCount": self.max_tokens,
                "temperature": self.temperature,
                "topP": 0.9
            }
        }
    
    def _prepare_llama_body(self, prompt: str) -> Dict[str, Any]:
        """
        Prepare the request body for Llama models.
        
        Args:
            prompt: The prompt for the model
            
        Returns:
            Request body for Llama API
        """
        return {
            "prompt": f"<system>{self.system_prompt}</system>\n\n<user>{prompt}</user>",
            "max_gen_len": self.max_tokens,
            "temperature": self.temperature,
            "top_p": 0.9
        }
    
    def _extract_json_query(self, text: str) -> Dict[str, Any]:
        """
        Extract a JSON query from the model's response text.
        
        Args:
            text: Raw text from the model response
            
        Returns:
            Extracted JSON query as a dictionary
        """
        # Strip out any markdown code block formatting
        text = text.replace("```json", "").replace("```", "").strip()
        
        # Try to parse the JSON
        try:
            query = json.loads(text)
            return query
        except json.JSONDecodeError:
            # Try to extract JSON from the text
            import re
            pattern = r'(\{.*\})'
            matches = re.search(pattern, text, re.DOTALL)
            if matches:
                try:
                    query = json.loads(matches.group(1))
                    return query
                except json.JSONDecodeError:
                    raise ValueError(f"Could not parse JSON from response: {text}")
            else:
                raise ValueError(f"Could not find JSON in response: {text}")


# Example usage
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate OpenSearch queries from natural language")
    parser.add_argument("--description", required=True, help="Natural language description of the query")
    parser.add_argument("--model", default="anthropic.claude-3-sonnet-20240229-v1:0", help="Bedrock model ID")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    
    args = parser.parse_args()
    
    generator = BedrockQueryGenerator(model_id=args.model, region=args.region)
    query = generator.generate_query(args.description)
    
    print(json.dumps(query, indent=2)) 