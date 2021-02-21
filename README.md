# ForecastingDemand

The goal of this project is to :

1. Understanding Big data in Retail domain
2. Implementing a Lambda architecture to ingest and process data both for streaming and batch source
3. Creating robust pipelines using modern storage and processing options

## Technology stack

1. Storage
	- AWS S3 : Inbound for batch files
	- Apache Kafka : Buffer for streaming events
	- HDFS : Data lake
	- Cassandra : Landing zone for streaming data
	- AWS Redshift : Data warehouse
	
2. Processing 
	- Spark : Batch and Stream processing
	- Hive : Batch processing

3. Language 
	- Python
	- Unix scripting
	
4. Others 
	- VCS : GitHub
	- Oozie : Schedulling