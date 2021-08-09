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
1. Use Shell Script to run both Run_Data_Ingestion.py and Run_Reporter_ETL.py scripts via ```spark-submit```.
2. Create Tracker class which will be used to track the job status of the "Data Ingestion" job and store data into MySQL Database.
3. Create Reporter class which will write desired output of Step 4 to desired location.
4. Use a separate Python script using ```configparser``` package to store and protect important information. This is highly important for real-world when I need to protect AWS credentials such as access and secret access keys.

### Issue:
I was unable to SSH into EMR Cluster, nor create access and secret access keys based on AWS Educate account.
Yet, I ended up instead trying to complete the work locally while researching what I would for real-world application.
Basically, I can try to SSH into EMR Cluster to try and build a concurrent data orchestration pipeline using Amazon EMR and Apache Livy (which enables easy interaction w/ a Spark cluster over a REST interface). The cloud architecture would include AWS CloudFormation, where I build a CloudFormation stack that includes an Airflow instance, RDS Instance based on PostgreSQL, S3 bucket, and the Airflow instance would be triggered as needed for the EMR Cluster to run w/ Apache Spark and Apache Livy.

The following is a link for more information: https://aws.amazon.com/blogs/big-data/build-a-concurrent-data-orchestration-pipeline-using-amazon-emr-and-apache-livy/







