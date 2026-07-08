# STEDI Human Balance Analytics

## Project Overview

This project builds a secure data lakehouse solution on AWS for the STEDI Step Trainer application. The objective is to ingest raw sensor and customer data into Amazon S3, transform the data using AWS Glue Studio, and create trusted and curated datasets for machine learning while protecting customer privacy.

The project demonstrates how to build an end-to-end ETL pipeline using AWS services including Amazon S3, AWS Glue, AWS Glue Data Catalog, Amazon Athena, and AWS IAM.

---

# Architecture

```
Landing Zone (Amazon S3)
        │
        ▼
AWS Glue Data Catalog
        │
        ▼
Amazon Athena (Validation)
        │
        ▼
AWS Glue Studio ETL Jobs
        │
        ▼
Trusted Zone (Amazon S3)
        │
        ▼
AWS Glue Crawlers
        │
        ▼
Amazon Athena (Validation)
        │
        ▼
AWS Glue Studio ETL Jobs
        │
        ▼
Curated Zone (Amazon S3)
        │
        ▼
Amazon Athena
```

---

# Technologies Used

* Amazon S3
* AWS Glue Studio
* AWS Glue Data Catalog
* AWS Glue Crawlers
* Amazon Athena
* AWS IAM
* SQL
* Python

# Landing Zone

The landing zone stores the raw JSON files uploaded to Amazon S3.

### Landing Tables

* customer_landing
* accelerometer_landing
* step_trainer_landing


The `customer_landing` table also contains records where `shareWithResearchAsOfDate` is blank, representing customers who have not provided research consent.

# Trusted Zone

The trusted zone removes customer records without research consent and filters accelerometer data to include only readings belonging to consented customers.

## Glue Jobs

### customer_landing_to_trusted

* Reads customer landing data
* Filters customers with non-empty `shareWithResearchAsOfDate`
* Writes trusted customer dataset

### accelerometer_landing_to_trusted

* Reads accelerometer landing data
* Joins with customer trusted data using email
* Outputs only accelerometer columns

### step_trainer_trusted

* Reads step trainer landing data
* Writes trusted step trainer dataset

### Athena Validation
The `customer_trusted` table contains no rows with blank `shareWithResearchAsOfDate`.
# Curated Zone

The curated zone prepares data for downstream analytics and machine learning.

## Glue Jobs

### customer_trusted_to_curated

* Joins customer trusted and accelerometer trusted data using email
* Outputs only customer columns

### step_trainer_trusted

* Joins step trainer landing data with customer curated data using serial number

### machine_learning_curated

* Joins step trainer trusted and accelerometer trusted datasets using timestamp and sensor reading time
* Produces the final machine learning dataset


# How to Run

1. Upload the JSON files to the Landing Zone in Amazon S3.
2. Create the Glue database.
3. Create Glue tables for the landing data.
4. Validate landing tables using Amazon Athena.
5. Execute `customer_landing_to_trusted`.
6. Run the Glue crawler for `customer_trusted`.
7. Execute `accelerometer_landing_to_trusted`.
8. Run the Glue crawler for `accelerometer_trusted`.
9. Execute `step_trainer_trusted`.
10. Run the Glue crawler for `step_trainer_trusted`.
11. Execute `customer_trusted_to_curated`.
12. Run the Glue crawler for `customer_curated`.
13. Execute `machine_learning_curated`.
14. Run the Glue crawler for `machine_learning_curated`.
15. Validate the trusted and curated tables using Amazon Athena and capture the required screenshots.

---

# Results

The project successfully:

* Ingested raw JSON data into Amazon S3.
* Created Glue Data Catalog tables.
* Queried datasets using Amazon Athena.
* Built ETL pipelines using AWS Glue Studio.
* Generated trusted datasets by enforcing research consent.
* Produced curated datasets for machine learning.
* Implemented a privacy-aware data lakehouse architecture.

---
Conclusion

This is an End to End data pipline project where i have seeked,understood knowledge on the concepts including data lakes , ETL and Data processing.
the row count in the produced table is verified using Athena 

Landing Customer: 956 Accelerometer: 81273 Step Trainer: 28680
Trusted Customer: 482 Accelerometer: 40981 Step Trainer: 14460
Curated Customer: 482 Machine Learning: 43681


