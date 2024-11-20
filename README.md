# Incremental_data_load
This pipeline is designed to process only unprocessed files, load them into the target system, and then update a control table in Hive with the filenames that were successfully processed.

## Introduction
Data Pipeline Description:
This pipeline is designed to process only unprocessed files, load them into the target system, and then update a control table in Hive with the filenames that were successfully processed.

### Key Features:
Pipeline for Processing Only Unprocessed Files

### Overview
This pipeline is designed to efficiently load only unprocessed files from the source to the target system, ensuring that processed files are ignored. This selective processing helps to reduce costs by avoiding the reprocessing of already processed data.

### The pipeline performs the following steps:

File Detection: It checks the source location for files that are unprocessed (i.e., new or previously failed to process).
Selective Processing: Only unprocessed files are loaded into the target system, significantly reducing unnecessary data processing and storage costs.
Hive Control Table Update: After processing, the pipeline updates a Hive control table with the list of successfully processed file names to ensure future files are processed efficiently.
Key Benefits:

Cost-Efficiency: By processing only unprocessed files, the pipeline reduces resource usage and cost, especially on cloud platforms.
Optimized Data Flow: Prevents unnecessary data reprocessing, improving overall efficiency.
Automated Workflow: The pipeline is fully automated, running on a daily schedule with minimal intervention.
Technologies Used:

Apache Airflow: Used to orchestrate and schedule the pipeline tasks.
Google Cloud Dataproc: Manages the processing of files using PySpark.
Apache Hive: Used for metadata management and control table updates.
Google Cloud Storage (GCS): Stores source files that are processed by the pipeline.


