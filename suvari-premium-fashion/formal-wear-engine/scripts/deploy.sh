#!/bin/bash

echo "Deploying Suvari Formal Wear Analytics Agent..."

gcloud functions deploy formal-wear-query \
    --gen2 \
    --runtime=python311 \
    --region=europe-west1 \
    --source=. \
    --entry-point=query_formal_wear \
    --trigger-http \
    --allow-unauthenticated \
    --env-vars-file=.env.yaml \
    --memory=512MB \
    --timeout=60s \
    --max-instances=10

echo "Deployment completed!"
echo "Function URL: https://europe-west1-agentspace-ngc.cloudfunctions.net/formal-wear-query"