# OpenSearch Query Insights

## Introduction
OpenSearch stands as a versatile, scalable, open-source solution designed for diverse data exploration needs, ranging from interactive log analytics to real-time application monitoring. Despite its capabilities, OpenSearch users and administrators often encounter challenges in ensuring optimal search performance due to limited expertise or OpenSearch's current constraints in providing comprehensive data points on query executions. Common questions include:

* "What are the top queries with highest latency/CPU usages in the last 1 hour" (Identification of top queries by certain resource usages within a specific timeframe).
* "How do I associate queries to users" (Profiling users with the highest search query volumes).
* "Why my search queries are so slow" (Concerns about slow search queries).
* "Why there was a spike in my search latency chart" (Spikes in query latency).

The overarching objective of the Query Insights initiative is to address these issues by building frameworks, APIs, and dashboards, with minimal performance impact, to offer profound insights, metrics and recommendations into query executions, empowering users to better understand search query characteristics, patterns, and system behavior during query execution stages. Query Insights will facilitate enhanced detection, diagnosis, and prevention of query performance issues, ultimately improving query processing performance, user experience, and overall system resilience.

Query Insights and this plugin project was originally proposed in the [OpenSearch Query Insights RFC](https://github.com/opensearch-project/OpenSearch/issues/11429).

## Get Started
### Installing the Plugin

To get started, install the plugin into OpenSearch with the following command:

```
bin/opensearch-plugin install query-insights
```
For information about installing plugins, see [Installing plugins](https://opensearch.org/docs/latest/install-and-configure/plugins/).

### Enabling top N query monitoring

When you install the `query-insights` plugin, top N query monitoring is enabled by default. To disable top N query monitoring, update the dynamic cluster settings for the desired metric types. For example, to disable monitoring top N queries by latency, update the `search.insights.top_queries.latency.enabled` setting:

```
PUT _cluster/settings
{
  "persistent" : {
    "search.insights.top_queries.latency.enabled" : false
  }
}
```
### Monitoring the top N queries

You can use the Insights API endpoint to obtain top N queries:

```
GET /_insights/top_queries
```

### Export top N query data

You can configure your desired exporter to export top N query data to different sinks, allowing for better monitoring and analysis of your OpenSearch queries.

A local index exporter allows you to export the top N queries to local OpenSearch indexes. To configure the local index exporter for the top N queiries by latency, send the following request:

```
PUT _cluster/settings
{
  "persistent" : {
    "search.insights.top_queries.exporter.type" : "local_index"
  }
}
```
You can refer to the [official document](https://opensearch.org/docs/latest/observing-your-data/query-insights/index/) for more detailed usage of query-insights plugin.

## Development
If you find bugs or want to request a feature, please create [a new issue](https://github.com/opensearch-project/query-insights/issues/new/choose). For questions or to discuss how Query Insights works, please find us in the [OpenSearch Slack](https://opensearch.org/slack.html) in the `#plugins` channel.

### Building and Testing

The plugin can be built using Gradle:

```
./gradlew build
```

To test and debug, run the plugin with OpenSearch in debug mode:

```
./gradlew run --debug-jvm
```

## Project Style Guidelines

The [OpenSearch Project style guidelines](https://github.com/opensearch-project/documentation-website/blob/main/STYLE_GUIDE.md) and [OpenSearch terms](https://github.com/opensearch-project/documentation-website/blob/main/TERMS.md) documents provide style standards and terminology to be observed when creating OpenSearch Project content.

## Getting Help

* For questions or help getting started, please find us in the [OpenSearch Slack](https://opensearch.org/slack.html) in the `#plugins` channel.
* For bugs or feature requests, please create [a new issue](https://github.com/opensearch-project/query-insights/issues/new/choose).

## Code of Conduct

This project has adopted the [Amazon Open Source Code of Conduct](CODE_OF_CONDUCT.md). For more information see the [Code of Conduct FAQ](https://aws.github.io/code-of-conduct-faq), or contact [opensource-codeofconduct@amazon.com](mailto:opensource-codeofconduct@amazon.com) with any additional questions or comments.

## License

This project is licensed under the [Apache v2.0 License](LICENSE.txt).

## Copyright

Copyright OpenSearch Contributors. See [NOTICE](NOTICE.txt) for details.

# Query Insights Performance Prediction

This project provides tools for training machine learning models on Amazon SageMaker to predict OpenSearch query performance metrics (latency, CPU usage, and memory consumption) based on query characteristics and cluster metrics.

## Overview

The system consists of several components:

1. **Feature Generation**: Custom feature embeddings from the OpenSearch Query Insights plugin, which collects a rich set of features including query shape, metadata, and cluster state.

2. **Data Collection**: Each query execution generates a feature vector that is written to CSV files, organized by date.

3. **Model Training**: A Python script that processes these CSV files, prepares the data, and trains an XGBoost model on SageMaker.

4. **Continuous Training**: The ability to automatically retrain the model as new data becomes available.

5. **Prediction**: A Python client that can extract features from a query and predict its performance metrics.

## Setup

### Prerequisites

- OpenSearch cluster with the Query Insights plugin installed and configured (with feature embedding enabled)
- AWS account with SageMaker access
- Python 3.7+ with required packages
- AWS CLI configured with appropriate credentials

### Installation

1. Clone this repository:
   ```
   git clone https://github.com/your-repo/query-insights-prediction.git
   cd query-insights-prediction
   ```

2. Install required Python packages:
   ```
   pip install -r requirements.txt
   ```

3. Configure the OpenSearch Query Insights plugin to enable feature embedding generation:

   ```yaml
   # Add to opensearch.yml or configure via API
   search.insights.feature_embedding.enabled: true
   search.insights.feature_embedding.output_directory: /path/to/outputs
   search.insights.feature_embedding.file_prefix: query_features
   search.insights.feature_embedding.export_interval: 5m
   ```

## Data Collection

The Query Insights plugin will automatically collect feature vectors for each search query and store them in CSV files based on the configured output directory. Each file follows the format `query_features_YYYY-MM-DD.csv`.

The collected features include:

- Query attributes (shape, size, aggregations, etc.)
- Cluster metrics (CPU, memory, disk usage)
- Index metrics (document count, size)
- Performance metrics (latency, CPU usage, memory usage)

## Training a Model

### Initial Setup

1. Create an S3 bucket to store training data and model artifacts:
   ```
   aws s3 mb s3://your-bucket-name
   ```

2. Create an IAM role for SageMaker with the necessary permissions:
   - AmazonSageMakerFullAccess
   - AmazonS3FullAccess

3. Note the ARN of the created role for use in the training script.

### Training Procedure

To train a model using the collected data, run the training script:

```bash
python train_query_insights_model.py \
  --data-dir /path/to/feature/embeddings \
  --days 7 \
  --endpoint-name query-insights-endpoint \
  --s3-bucket your-bucket-name \
  --region us-east-1 \
  --role-arn arn:aws:iam::123456789012:role/SageMakerRole
```

This will:
1. Load the CSV files from the specified directory
2. Preprocess the data
3. Upload the preprocessed data to S3
4. Create a SageMaker training job
5. Deploy the resulting model to a SageMaker endpoint

### Continuous Training

To set up continuous retraining, you can:

1. Run the script in continuous mode:
   ```bash
   python train_query_insights_model.py \
     --data-dir /path/to/feature/embeddings \
     --endpoint-name query-insights-endpoint \
     --s3-bucket your-bucket-name \
     --region us-east-1 \
     --role-arn arn:aws:iam::123456789012:role/SageMakerRole \
     --continuous \
     --interval 24
   ```

2. Or schedule the script using cron or a task scheduler.

3. For production workloads, consider using AWS Step Functions or Lambda to orchestrate the training pipeline.

## Making Predictions

Once your model is deployed to a SageMaker endpoint, you can use the prediction script to estimate query performance:

```bash
python predict_query_performance.py \
  --endpoint-name query-insights-endpoint \
  --region us-east-1 \
  --query-file ./examples/query.json \
  --cluster-info-file ./examples/cluster_info.json
```

You can also incorporate the prediction functionality into your application:

```python
from predict_query_performance import QueryPerformancePredictor, extract_features_from_query

# Initialize the predictor
predictor = QueryPerformancePredictor('query-insights-endpoint')

# Extract features from a query
query = {
    "index": "my-index",
    "body": {
        "query": {
            "match": {"field": "value"}
        },
        "size": 10
    }
}

cluster_info = {
    "active_nodes_count": 3,
    "cluster_status": "green",
    "avg_cpu_utilization": 45.2,
    # Other cluster metrics...
}

# Extract features
features_df = extract_features_from_query(query, cluster_info)

# Make prediction
predictions = predictor.predict(features_df)

print(f"Predicted latency: {predictions['latency_ms']} ms")
print(f"Predicted CPU usage: {predictions['cpu_nanos']} ns")
print(f"Predicted memory usage: {predictions['memory_bytes']} bytes")
```

## Monitoring and Maintenance

### Monitoring Model Performance

- Monitor metrics in SageMaker through CloudWatch
- Set up alerts for endpoint errors or performance degradation
- Periodically validate the model accuracy against real query performance

### Improving Model Accuracy

1. **Collect more data**: The model improves with more training examples.

2. **Feature engineering**: Add more features to the feature embedding generator.

3. **Hyperparameter tuning**: Use SageMaker hyperparameter tuning to optimize the model.

4. **Model evaluation**: Implement a feedback loop by comparing predictions with actual performance.

## Troubleshooting

### Common Issues

1. **Training data not found**: Ensure the CSV files exist in the specified directory and follow the expected format.

2. **Missing features**: If the feature extraction fails, check the OpenSearch Query Insights plugin logs.

3. **SageMaker endpoint errors**: Check CloudWatch logs for the endpoint to diagnose issues.

4. **Poor prediction accuracy**: This can occur with insufficient training data or if the cluster environment changes significantly.

### Debugging Tips

- Enable DEBUG level logging to get more detailed information
- Use `--debug` flag with the AWS CLI for troubleshooting AWS-related issues
- Inspect CSV files manually to ensure data quality
- Test predictions with known queries to establish baseline accuracy

## Example Files

### Example Query JSON

```json
{
  "index": "my-index",
  "body": {
    "query": {
      "bool": {
        "must": [
          { "match": { "field1": "value1" } },
          { "range": { "field2": { "gte": 10, "lte": 100 } } }
        ]
      }
    },
    "size": 20,
    "from": 0,
    "sort": [
      { "field3": "desc" }
    ]
  }
}
```

### Example Cluster Info JSON

```json
{
  "active_nodes_count": 5,
  "cluster_status": "green",
  "indices_status": 2,
  "avg_cpu_utilization": 60.5,
  "avg_jvm_heap_used_percent": 70.2,
  "avg_disk_used_percent": 45.8,
  "avg_docs_count": 1000000,
  "max_docs_count": 2500000,
  "avg_index_size_bytes": 500000000,
  "total_shards": 15
}
```

## License

Apache License 2.0

# Query Insights

A tool for collecting, analyzing, and predicting the performance of OpenSearch queries using machine learning and natural language processing.

## Overview

Query Insights is a comprehensive solution that collects metrics from OpenSearch queries, uses machine learning to predict query performance, and provides a natural language interface for generating and analyzing queries.

The system consists of several components:
1. A data collection mechanism that captures query metrics and feature embeddings
2. A machine learning pipeline for training performance prediction models on SageMaker
3. A natural language interface that allows users to describe queries in plain English

## Features

- **Data Collection**: Automatically collects metrics from OpenSearch queries including latency, CPU usage, and memory consumption
- **Feature Embeddings**: Generates vector embeddings of queries for ML model training
- **Performance Prediction**: Uses trained models to predict how a query will perform before execution
- **Natural Language Interface**: Describe queries in plain English and get structured OpenSearch queries and performance predictions
- **Interactive Mode**: Conversational interface for exploring query performance

## Setup

### Prerequisites
- Python 3.8+
- AWS account with permissions for SageMaker and Bedrock
- OpenSearch cluster for data collection

### Installation

1. Clone this repository:
```bash
git clone https://github.com/yourusername/query-insights.git
cd query-insights
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure AWS credentials:
```bash
aws configure
```

4. Set default configuration:
```bash
python query_insights_cli.py config --sagemaker-endpoint your-endpoint --bedrock-model anthropic.claude-3-sonnet-20240229-v1:0 --s3-bucket your-bucket
```

## Data Collection

The system collects data from OpenSearch queries through the `QueryInsightsListener` class. This listener captures metrics such as:
- Query latency
- CPU utilization
- Memory usage
- Query structure features
- Feature embeddings for machine learning

To enable data collection, configure the `QueryInsightsPlugin` in your OpenSearch setup.

## Model Training

Train a performance prediction model using the collected data:

```bash
python query_insights_cli.py train --data-path data/query_features.csv --model-name query-perf-model --deploy
```

This will:
1. Prepare your data for training
2. Upload it to S3
3. Launch a SageMaker training job
4. Deploy the model to a SageMaker endpoint (if --deploy is specified)

## Natural Language Interface

### Generating Queries from Natural Language

Convert natural language descriptions into structured OpenSearch queries:

```bash
python query_insights_cli.py generate --description "Find all log entries with ERROR status from the last 24 hours sorted by timestamp"
```

### Predicting Query Performance

Predict the performance of a query described in natural language:

```bash
python query_insights_cli.py predict --description "Find documents where the field 'category' contains 'electronics' and price is greater than 100, sorted by rating"
```

This will:
1. Generate a structured OpenSearch query from your description
2. Extract features from the generated query
3. Predict performance metrics (latency, CPU, memory)
4. Display the results

Add `--explain` to get a natural language explanation of the predictions:

```bash
python query_insights_cli.py predict --description "..." --explain
```

### Interactive Mode

For an interactive experience, use the shell mode:

```bash
python query_insights_cli.py interactive
```

This provides a conversational interface where you can:
- Enter natural language query descriptions
- Get instant query translations and performance predictions
- Get explanations of the predictions
- Explore different query formulations

## Example Use Cases

1. **Query Performance Optimization**:
   - Identify poorly performing queries before execution
   - Test alternative query formulations for better performance

2. **Capacity Planning**:
   - Predict resource requirements for new query workloads
   - Identify potential bottlenecks before they occur

3. **Developer Assistance**:
   - Help developers write efficient OpenSearch queries
   - Provide natural language interface for non-experts

## Architecture

The system uses:
- **Amazon Bedrock**: For natural language understanding and query generation
- **Amazon SageMaker**: For training and hosting prediction models
- **XGBoost**: As the machine learning algorithm for performance prediction
- **Feature Embeddings**: Vector representations of queries for better prediction accuracy

## Troubleshooting

### Common Issues

- **Authentication Errors**: Ensure your AWS credentials are properly configured
- **Missing Dependencies**: Verify all required packages are installed
- **SageMaker Endpoint Not Found**: Check that your endpoint is deployed and available
- **Bedrock API Errors**: Verify your region has the selected Bedrock model available

### Logs

Check application logs for detailed error information:

```bash
cat ~/.query_insights_logs.log
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
