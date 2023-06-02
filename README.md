# This is for the project to create my own website. I share this so as to keep track of the website building.

# Architecture overview

Please refer to _Architecture_Diagram_Website_with_AWS.png

# aws-website building

The repository provides Data Analysing by AWS RDS (MySQL).

## Requirements

* MySQL Workbench 
* AWS RDS (MySQL)
* SQL Query (Stored Procedures for DataBase Init, Data Loading, Report Data Generating)

## Deploying
(Terraform is used to execute all the queries in the shared files automatically.)
terraform init
terraform validate
terraform plan
terraform apply
terraform destroy
