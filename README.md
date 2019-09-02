# Graphalytics Gelly Platform Extension

## Getting Started
Please refer to the documentation of the [Graphalytics Repository](https://github.com/tudelft-atlarge/graphalytics) for an introduction to using Graphalytics. For setting up Apache Flink, please refer to the [Flink setup guide](https://ci.apache.org/projects/flink/flink-docs-release-1.0/quickstart/setup_quickstart.html).

## Supported Algorithms
- Breadth-First Search
- Label Propagation
- Local Clustering Coefficient
- PageRank
- Single-Source Shortest Paths
- Weakly Connected Components

## Gelly-specific Benchmark Configuration
Gelly-specific configuration options should be set in the *gelly.properties* file.

- *hadoop.config.home*: The hadoop configuration path
- *gelly.hdfs.host*: The address and port of the HDFS namenode
- *gelly.job.parallelism*: The Flink environment default parallelism
- *gelly.job.manager.address*: The Flink job manager address
- *gelly.job.manager.port*: The Flink job manager port
- *gelly.job.taskmanager.memory*: The available memory per task manager (in MB)
- *gelly.graphalytics.jar*: The graphalytics-gelly jar path

## References
- [Apache Flink](https://flink.apache.org/)
- [Gelly Guide](https://ci.apache.org/projects/flink/flink-docs-master/dev/libs/gelly/index.html)
- [Graphalytics paper](https://dl.acm.org/citation.cfm?id=2764954)
- [Graphalytics Repository](https://github.com/tudelft-atlarge/graphalytics)
