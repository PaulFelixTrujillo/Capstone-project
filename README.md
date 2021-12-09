# Capstone project : Data Engineering Bootcamp

## Paul Ricardo Félix Trujillo

## Exploratory Analysis

The Customer Service team wants to analyze the products for their clients using their data to create user behavior metrics based on their reviews.

## Objective

Make an automation process create an infrastructure in a cloud
service and run a pipeline to process raw databases and storage in a data warehouse.

![GENERAL DIAGRAM.svg](Capstone%20project%20Data%20Engineering%20Bootcamp%206086dc7bcba345fc9fa643fb05574103/GENERAL_DIAGRAM.svg)

## Architecture

![MAIN ARCHITECTURE.svg](Capstone%20project%20Data%20Engineering%20Bootcamp%206086dc7bcba345fc9fa643fb05574103/MAIN_ARCHITECTURE.svg)

### The project can be divided into 3 phases:

- Set the configurations and settings for the cloud infrastructure, in this case GCP.
- Create  the  infrastructure in a new project in the cloud service to prepare our pipeline.
- Upload the code in a platform, in this case airflow, to schedule and monitor the workflow.

## Phase 1 terraform

![terraform.svg](Capstone%20project%20Data%20Engineering%20Bootcamp%206086dc7bcba345fc9fa643fb05574103/terraform.svg)

For the first phase, I use terraform as a tool to build the infrastructure through code. This allow to create, change or destroy the infrastructure automatically saving time and money with preconstructed blocks.

## Phase 2 GCP

![gcp.svg](Capstone%20project%20Data%20Engineering%20Bootcamp%206086dc7bcba345fc9fa643fb05574103/gcp.svg)

The second phase after I already set up the needed services for the project in GCP, consist in desing the architecture to process the data.

Starting with the data source,  I stored the csv.files in a raw bucket, then create the tables needed for the analytics in postgres. For the file movie_review.csv that required transformations, I set up a cluster Dataproc to create a spark job that insert a new table in postgres with the information of good reviews.

Finally, I use Bigquery as my data Wharehouse  to create a external query job using the table in posgres to insert the user behavior metrics in a new data table.

## Phase 3 Airflow / DAGs

![MAIN ARCHITECTURE.svg](Capstone%20project%20Data%20Engineering%20Bootcamp%206086dc7bcba345fc9fa643fb05574103/MAIN_ARCHITECTURE%201.svg)

![DAGS.svg](Capstone%20project%20Data%20Engineering%20Bootcamp%206086dc7bcba345fc9fa643fb05574103/DAGS.svg)

The last phase use the GKE cloud service to integrate airflow as a scheduler. In this platform I

 created the DAGs for each part of the data pipeline to schedule and monitor the workflow.

The first two DAGs store the cvs files (`user_pruchase.csv movie_review.csv`) in postgres as tables.

The third DAG submit the spark job in dataproc to create a new table, `reviews`, with the information of  positive reviews.

The final dag insert a job in BigQuery to create the `user_behavior_metrics` table using a external query for the tables in postgres.

## Lesson Learned and next steps

![lesson.svg](Capstone%20project%20Data%20Engineering%20Bootcamp%206086dc7bcba345fc9fa643fb05574103/lesson.svg)

## *I want to thank the mentors and the wizeline team for giving me the opportunity to be part of this course and I hope to keep in touch for new courses or projects.*
