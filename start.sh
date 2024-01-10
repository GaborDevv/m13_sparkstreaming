#!/bin/bash

# Set up terraform
cd terraform
terraform init
terraform plan -out terraform.plan
terraform apply terraform.plan
cd ..
