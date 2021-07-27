# AWS Guided-Capstone Step 5: Pipeline Orchestration

### Description
Now that all of the functionality has been implemented, it is time to orchestrate my pipeline, connecting all of the individual components into an end-to-end data pipeline. In the real-world, data processing jobs are typically organized into automated workflows.

There are many ways to define a workflow and it's scope, but typically workflows are repeatable pipeline units which can be run independently. In cloud architecture, a workflow can be run in an elastic Hadoop cluster w/ input and output data that are persistent on cloud storage such as:
1. Preprocessing Workflow: Data Ingestion and Batch Load
2. Analytical Workflow: Analytical ETL job

In this project, you'll launch your Spark job using a command line shell script in each workflow.

### Learning Objectives:
By the end of this step, I will:
1. Write shell script to submit Spark jobs.
2. Execute Spark jobs in Azure Elastic Clusters (in my case, AWS EMR Clusters).
3. Design job status tracker.

### Prerequisites:
1. AWS Cluster
2. Knowledge of command line

### Outline of Steps:
1. Preprocessing Workflow - Use a shell script to call a data ingestion Spark job.
2. Use shell script to call an analytical ETL job.
3. Job Status Tracking
  a. Define a Job Status Table
  b. Define a Job Management Class
  c. Populate Job Status Table During the Job Run




