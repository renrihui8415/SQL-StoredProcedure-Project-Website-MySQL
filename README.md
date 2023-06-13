# This is for the project to create my own website. I share this so as to keep track of the website building.

# Architecture overview

Please refer to _Architecture_Diagram_Website_with_AWS.png

# aws-website building

The repository provides Data Analysing with SQL.

## Requirements

* MySQL Workbench 
* AWS RDS (MySQL)
* SQL Query (Stored Procedures for DataBase Init, Data Loading, Report Data Generating)

## Deploying
(ECS (AWS Elastic Container Service) and Lambda are used to execute all the procedures in the shared files automatically.)
1) Once the .sql files are uploaded into s3 bucket, SQS will trigger lambda to check the files.
2) The files info will be passed in to ECS containers.
3) ECS will use MySQL Command Line to connect/execute all SQL scripts in the file at once.
4) MySQL Database in the same VPC will be set up completely.
