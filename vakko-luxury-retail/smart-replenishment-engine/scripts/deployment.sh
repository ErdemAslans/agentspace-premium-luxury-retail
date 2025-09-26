#!/bin/bash

# Vakko Smart Replenishment Cloud Functions Deployment
# =====================================================

echo "🚀 Vakko Smart Replenishment Deployment Starting..."
echo "=================================================="

# Configuration
PROJECT_ID="agentspace-ngc"
REGION="europe-west1"
RUNTIME="python310"
DATASET_ID="vakko_replenishment"

# Set the project
gcloud config set project $PROJECT_ID

# Check if requirements.txt exists
if [ ! -f "requirements.txt" ]; then
    echo "📝 Creating requirements.txt..."
    cat > requirements.txt << 'EOF'
google-cloud-bigquery==3.13.0
google-cloud-storage==2.10.0
functions-framework==3.5.0
numpy==1.24.3
pandas==2.0.3
pytz==2023.3
EOF
    echo "✅ requirements.txt created"
else
    echo "✅ requirements.txt already exists"
fi

echo ""
echo "📦 Deploying 12 Smart Replenishment Functions to $REGION..."
echo "Dataset: $DATASET_ID"
echo ""

# Counter for tracking deployments
DEPLOYED=0
FAILED=0

# Function deployment with --gen2 flag
deploy_function() {
    local func_name=$1
    local func_number=$2
    echo "[$func_number/12] 🔧 Deploying $func_name..."
    
    gcloud functions deploy $func_name \
        --gen2 \
        --runtime $RUNTIME \
        --trigger-http \
        --allow-unauthenticated \
        --entry-point $func_name \
        --region $REGION \
        --memory 512MB \
        --timeout 60s \
        --source . \
        --set-env-vars PROJECT_ID=$PROJECT_ID,DATASET_ID=$DATASET_ID \
        --max-instances 10 \
        --min-instances 0 \
        --quiet
    
    if [ $? -eq 0 ]; then
        echo "✅ [$func_number/12] $func_name deployed successfully"
        echo "🔗 https://$REGION-$PROJECT_ID.cloudfunctions.net/$func_name"
        ((DEPLOYED++))
    else
        echo "❌ [$func_number/12] Failed to deploy $func_name"
        ((FAILED++))
    fi
    echo ""
}

# Start deployment
START_TIME=$(date +%s)

echo "===== UTILITY FUNCTIONS (3) ====="
deploy_function "list_tables" 1
deploy_function "get_table_schema" 2
deploy_function "sql_query" 3

echo "===== CORE REPLENISHMENT FUNCTIONS (3) ====="
deploy_function "smart_replenishment" 4
deploy_function "stockout_prediction" 5
deploy_function "replenishment_schedule" 6

echo "===== OPTIMIZATION FUNCTIONS (3) ====="
deploy_function "inventory_optimization" 7
deploy_function "transfer_recommendation" 8
deploy_function "critical_stock_alerts" 9

echo "===== ANALYTICS FUNCTIONS (3) ====="
deploy_function "sales_velocity_analysis" 10
deploy_function "warehouse_summary" 11
deploy_function "demand_trends" 12

# Calculate deployment time
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
MINUTES=$((DURATION / 60))
SECONDS=$((DURATION % 60))

echo ""
echo "======================================"
echo "📊 DEPLOYMENT SUMMARY"
echo "======================================"
echo "✅ Successfully deployed: $DEPLOYED functions"
echo "❌ Failed deployments: $FAILED functions"
echo "⏱️  Total deployment time: ${MINUTES}m ${SECONDS}s"
echo ""

if [ $DEPLOYED -eq 12 ]; then
    echo "🎉 All 12 functions deployed successfully!"
else
    echo "⚠️  Some deployments failed. Please check the logs."
fi

echo ""
echo "📊 Test Your Functions:"
echo "======================================"
echo ""
echo "1️⃣ Smart Replenishment (5 günden önce bitecek stoklar):"
echo "curl -X POST https://$REGION-$PROJECT_ID.cloudfunctions.net/smart_replenishment \\"
echo "  -H 'Content-Type: application/json' \\"
echo "  -d '{\"critical_days\": 5}'"
echo ""
echo "2️⃣ Stockout Prediction (7 günlük tahmin):"
echo "curl -X POST https://$REGION-$PROJECT_ID.cloudfunctions.net/stockout_prediction \\"
echo "  -H 'Content-Type: application/json' \\"
echo "  -d '{\"forecast_days\": 7}'"
echo ""
echo "3️⃣ Transfer Recommendation:"
echo "curl -X POST https://$REGION-$PROJECT_ID.cloudfunctions.net/transfer_recommendation"
echo ""
echo "4️⃣ Critical Alerts Dashboard:"
echo "curl -X POST https://$REGION-$PROJECT_ID.cloudfunctions.net/critical_stock_alerts"
echo ""
echo "5️⃣ List Tables:"
echo "curl -X POST https://$REGION-$PROJECT_ID.cloudfunctions.net/list_tables"
echo ""
echo "======================================"
echo "🎯 Demo Prompt:"
echo "   'Mont stoklarını incele. Satış hızına göre hangi"
echo "   mağaza stokları 5 günden önce bitecek? Replenishment önerilerini ver.'"
echo ""
echo "📋 Configuration:"
echo "   Project: $PROJECT_ID"
echo "   Dataset: $DATASET_ID"
echo "   Region: $REGION"
echo "   Runtime: $RUNTIME"
echo ""
echo "✅ Deployment script completed at: $(date)"