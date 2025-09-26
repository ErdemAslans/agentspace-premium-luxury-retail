#!/bin/bash

# Suvari Store Performance Cloud Function Deployment Script

PROJECT_ID="agentspace-ngc"
FUNCTION_NAME="store-performance-query"
REGION="europe-west1"
RUNTIME="python311"
MEMORY="512MB"
TIMEOUT="60s"
ENTRY_POINT="store_performance_query"

echo "🚀 Deploying Suvari Store Performance Function..."

# Set project
gcloud config set project $PROJECT_ID

# Deploy function
gcloud functions deploy $FUNCTION_NAME \
  --gen2 \
  --runtime=$RUNTIME \
  --region=$REGION \
  --source=. \
  --entry-point=$ENTRY_POINT \
  --trigger-http \
  --allow-unauthenticated \
  --memory=$MEMORY \
  --timeout=$TIMEOUT \
  --env-vars-file=.env.yaml \
  --max-instances=10 \
  --min-instances=0 \
  --ingress-settings=all

# Get function URL
echo "✅ Deployment complete!"
echo "📍 Function URL:"
gcloud functions describe $FUNCTION_NAME \
  --region=$REGION \
  --format="value(serviceConfig.uri)"

# Test the function
echo "🧪 Testing function..."
curl -X POST \
  "https://$REGION-$PROJECT_ID.cloudfunctions.net/$FUNCTION_NAME" \
  -H "Content-Type: application/json" \
  -d '{
    "question": "Bugün en iyi performansı hangi mağaza gösterdi?",
    "query_type": "daily_performance",
    "date_range": 1
  }'