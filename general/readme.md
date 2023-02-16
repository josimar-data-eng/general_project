
# requirements.txt  
Create Venv
pip3 freeze > requirements.txt 		--> Create a requirement.txt file
pip3 install -r requirements.txt  	--> Run all the libraries in the venv environment


# Create Venv
venv directory   : python3 -m venv env01
venv directory   : source env01/bin/activate  → activate the vm
deactivate the vm: deactivate

python3 -m venv data-eng-gcp && source data-eng-gcp/bin/activate 

# Run bash command
Run bash command: bash file


# Example of how to deploy cloud function 1st generation
deploy cloud function 1st generation
gcloud functions deploy function-get-flight-data \
--source=/Users/josimardossantosjunior/Code/DataEngineeringOnGCP/cloud-function-test \
--runtime python310 \
--region=us-central1 \
--allow-unauthenticated \
--entry-point=load \
--trigger-resource flights-data-out \
--trigger-event google.storage.object.finalize

# Example of how to deploy cloud function 2nd generation
deploy cloud function 2nd generation
gcloud functions deploy test-function \
--gen2 \
--runtime=python310 \
--region=us-central1 \
--source=. \
--allow-unauthenticated \
--entry-point=hello_gcss \
--trigger-bucket flights-data-out \
--trigger-location us-central1


# Delete a file
rm /Users/josimardossantosjunior/Code/DataEngineeringOnGCP/cloud-function/cloud-function.zip && \


# Deployment of cloud-function


## Start cloud SDK throug gcloud init
export PATH="/opt/homebrew/Caskroom/google-cloud-sdk/latest/google-cloud-sdk/bin:$PATH" && gcloud init


## Cloud Function
-- Zip file: Remove zip file
-- Zip it again (update) running zip_unzip.py script
-- Provision Cloud Function through "gcloud functions deploy" command

rm /Users/josimardossantosjunior/Code/general_project/cloud-function/cloud-function.zip && \
cd /Users/josimardossantosjunior/Code/general_project/cloud-function && \
python3 /Users/josimardossantosjunior/Code/general_project/general/zip_unzip.py && \
gcloud functions deploy function-flight-data-1sgen \
--source=/Users/josimardossantosjunior/Code/general_project/cloud-function \
--runtime python310 \
--region=us-central1 \
--allow-unauthenticated \
--entry-point=load \
--trigger-resource flights-data-out \
--trigger-event google.storage.object.finalize

## As cloud-function get file from output bucket, we need to sendo it to there as a test via gsutil
gsutil cp /Users/josimardossantosjunior/Downloads/avg.json gs://flights-data-out


# How to set runtime environment variables in cloud-functions via Cloud SSK - 
-set-env-vars FOO=bar,BAZ=boo FLAGS...
https://cloud.google.com/functions/docs/configuring/env-var#gcloud

### PULUMI ###

Configure Pulumi to access your Google Cloud account
1 - Configure SDK first

2 - pulumi config set gcp:project dbt-learning-377715 && export GOOGLE_PROJECT=dbt-learning-377715