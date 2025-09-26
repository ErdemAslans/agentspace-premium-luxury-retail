# main_allocation.py - Vakko Smart Allocation Cloud Functions

import functions_framework
import json
from google.cloud import bigquery
from datetime import datetime
import logging
import os
from typing import Dict, List, Any

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = os.getenv("PROJECT_ID", "agentspace-ngc")
DATASET_ID = os.getenv("DATASET_ID", "vakko_allocation")

# Initialize BigQuery client
bq_client = bigquery.Client(project=PROJECT_ID)

# =====================================
# HELPER FUNCTIONS
# =====================================

def execute_query(query: str) -> List[Dict]:
    """Execute BigQuery query and return results"""
    try:
        logger.info(f"Executing query: {query[:200]}...")
        query_job = bq_client.query(query)
        results = query_job.result()
        
        data = []
        for row in results:
            data.append(dict(row))
        
        logger.info(f"Query returned {len(data)} rows")
        return data
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise

def format_response(data: List[Dict], status: str = "success", message: str = "") -> tuple:
    """Format JSON response"""
    response = {
        "status": status,
        "data": data,
        "row_count": len(data),
        "message": message,
        "timestamp": datetime.utcnow().isoformat()
    }
    
    return json.dumps(response, default=str), 200, {'Content-Type': 'application/json'}

def error_response(error: str) -> tuple:
    """Format error response"""
    return json.dumps({
        "status": "error",
        "error": str(error),
        "timestamp": datetime.utcnow().isoformat()
    }), 500, {'Content-Type': 'application/json'}

# =====================================
# 1. CALCULATE ALLOCATION
# =====================================
@functions_framework.http
def calculate_allocation(request):
    """
    Merkez depodan mağazalara akıllı dağıtım hesaplama
    """
    try:
        request_json = request.get_json(silent=True) or {}
        sku = request_json.get('sku', 'KBN-001')
        quantity = request_json.get('quantity', 1000)
        
        query = f"""
        WITH demand_scores AS (
            -- Mağaza talep skorları
            SELECT 
                store_id,
                store_name,
                city,
                avg_daily_sales_30d,
                sales_trend_percentage,
                weather_adjusted_demand,
                stock_turnover_rate,
                current_stock_level,
                customer_traffic_index,
                -- Kompozit skor hesaplama
                (
                    avg_daily_sales_30d * 0.3 +  -- Mevcut satış hacmi
                    weather_adjusted_demand * 0.25 + -- Hava durumu etkisi
                    (avg_daily_sales_30d * (1 + sales_trend_percentage/100)) * 0.25 + -- Trend etkisi
                    (100 - current_stock_level) * 0.1 + -- Stok ihtiyacı
                    customer_traffic_index * 0.1  -- Müşteri trafiği
                ) as allocation_score
            FROM `{PROJECT_ID}.{DATASET_ID}.store_demand_metrics`
            WHERE product_category = 'Kaban'
                AND store_id IN ('VKK001', 'VKK002', 'VKK003', 'VKK004', 'VKK005')
        ),
        allocation_calc AS (
            -- Oransal dağıtım hesaplama
            SELECT 
                store_id,
                store_name,
                city,
                allocation_score,
                SUM(allocation_score) OVER() as total_score,
                allocation_score / SUM(allocation_score) OVER() as allocation_ratio
            FROM demand_scores
        )
        SELECT 
            store_id,
            store_name,
            city,
            ROUND({quantity} * allocation_ratio) as ai_recommended_qty,
            {quantity} / 5 as erp_suggested_qty,  -- Basit eşit dağıtım
            ROUND({quantity} * allocation_ratio) - ({quantity} / 5) as difference,
            CASE 
                WHEN allocation_ratio > 0.25 THEN 'Yüksek talep ve trend'
                WHEN allocation_ratio > 0.20 THEN 'Normal talep'
                WHEN allocation_ratio > 0.15 THEN 'Orta seviye talep'
                ELSE 'Düşük talep'
            END as allocation_reason,
            ROUND(allocation_score, 2) as score
        FROM allocation_calc
        ORDER BY ai_recommended_qty DESC
        """
        
        data = execute_query(query)
        
        # Toplam kontrolü ve düzeltme
        total_allocated = sum(d['ai_recommended_qty'] for d in data)
        if total_allocated != quantity and data:
            diff = quantity - total_allocated
            data[0]['ai_recommended_qty'] += diff
            data[0]['difference'] += diff
        
        return format_response(data, 
            message=f"{quantity} adet {sku} için AI tabanlı dağıtım önerisi")
        
    except Exception as e:
        return error_response(e)

# =====================================
# 2. COMPARE ALLOCATION METHODS
# =====================================
@functions_framework.http
def compare_allocation_methods(request):
    """
    AI vs ERP allocation karşılaştırması
    """
    try:
        request_json = request.get_json(silent=True) or {}
        sku = request_json.get('sku', 'KBN-001')
        
        query = f"""
        SELECT 
            ar.store_id,
            ar.store_name,
            ar.ai_recommended_qty,
            ar.erp_suggested_qty,
            ar.difference,
            ar.allocation_reason,
            ar.expected_sellthrough_7d,
            ar.expected_revenue,
            -- Demand metrics ekleme
            dm.avg_daily_sales_30d,
            dm.sales_trend_percentage,
            dm.stock_turnover_rate,
            dm.current_stock_level,
            -- Performans tahmini
            ROUND(ar.ai_recommended_qty / NULLIF(dm.avg_daily_sales_30d, 0), 1) as days_of_supply_ai,
            ROUND(ar.erp_suggested_qty / NULLIF(dm.avg_daily_sales_30d, 0), 1) as days_of_supply_erp
        FROM `{PROJECT_ID}.{DATASET_ID}.allocation_recommendations` ar
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.store_demand_metrics` dm
            ON ar.store_id = dm.store_id 
            AND dm.product_category = 'Kaban'
        WHERE ar.sku = '{sku}'
        ORDER BY ar.ai_recommended_qty DESC
        """
        
        data = execute_query(query)
        
        # Özet istatistikler
        if data:
            total_ai = sum(d['ai_recommended_qty'] for d in data)
            total_erp = sum(d['erp_suggested_qty'] for d in data)
            
            summary = {
                "total_ai_allocation": total_ai,
                "total_erp_allocation": total_erp,
                "ai_efficiency_score": calculate_efficiency_score(data, 'ai'),
                "erp_efficiency_score": calculate_efficiency_score(data, 'erp')
            }
            
            return format_response({
                "comparison": data,
                "summary": summary
            }, message=f"AI vs ERP allocation karşılaştırması - {sku}")
        
        return format_response(data, message="Karşılaştırma verisi bulunamadı")
        
    except Exception as e:
        return error_response(e)

# =====================================
# 3. STOCK TRANSFER RECOMMENDATION
# =====================================
@functions_framework.http
def transfer_recommendation(request):
    """
    Mağazalar arası transfer önerisi
    """
    try:
        query = f"""
        WITH store_balance AS (
            -- Her mağazanın stok dengesi
            SELECT 
                dm.store_id,
                dm.store_name,
                dm.city,
                dm.current_stock_level,
                dm.avg_daily_sales_30d,
                dm.weather_adjusted_demand,
                -- İdeal stok seviyesi (14 günlük)
                ROUND(dm.weather_adjusted_demand * 14) as ideal_stock,
                -- Stok fazlası veya eksikliği
                dm.current_stock_level - ROUND(dm.weather_adjusted_demand * 14) as stock_balance
            FROM `{PROJECT_ID}.{DATASET_ID}.store_demand_metrics` dm
            WHERE dm.product_category = 'Kaban'
        ),
        transfer_matrix AS (
            -- Transfer önerileri
            SELECT 
                s1.store_id as from_store_id,
                s1.store_name as from_store_name,
                s2.store_id as to_store_id,
                s2.store_name as to_store_name,
                s1.stock_balance as excess_stock,
                ABS(s2.stock_balance) as stock_needed,
                LEAST(s1.stock_balance, ABS(s2.stock_balance)) as transfer_qty,
                s1.city as from_city,
                s2.city as to_city
            FROM store_balance s1
            CROSS JOIN store_balance s2
            WHERE s1.stock_balance > 20  -- Fazla stok (20+ adet)
                AND s2.stock_balance < -10  -- Eksik stok (10+ adet eksik)
                AND s1.store_id != s2.store_id
        )
        SELECT 
            from_store_name,
            to_store_name,
            transfer_qty,
            from_city,
            to_city,
            CASE 
                WHEN from_city = to_city THEN 'Şehir içi transfer'
                ELSE 'Şehirler arası transfer'
            END as transfer_type,
            CASE 
                WHEN transfer_qty > 50 THEN 'Yüksek öncelik'
                WHEN transfer_qty > 20 THEN 'Orta öncelik'
                ELSE 'Düşük öncelik'
            END as priority
        FROM transfer_matrix
        WHERE transfer_qty > 0
        ORDER BY transfer_qty DESC
        LIMIT 10
        """
        
        data = execute_query(query)
        return format_response(data, 
            message="Mağazalar arası stok transfer önerileri")
        
    except Exception as e:
        return error_response(e)

# =====================================
# 4. WAREHOUSE STOCK SUMMARY
# =====================================
@functions_framework.http
def warehouse_summary(request):
    """
    Merkez depo stok özeti
    """
    try:
        query = f"""
        SELECT 
            category,
            subcategory,
            COUNT(DISTINCT sku) as product_count,
            SUM(total_quantity) as total_units,
            SUM(total_quantity * unit_cost) as total_value,
            AVG(unit_cost) as avg_unit_cost,
            -- Sezon dağılımı
            SUM(CASE WHEN season = 'Kış' THEN total_quantity ELSE 0 END) as winter_stock,
            SUM(CASE WHEN season = 'Bahar' THEN total_quantity ELSE 0 END) as spring_stock,
            SUM(CASE WHEN season = 'Yaz' THEN total_quantity ELSE 0 END) as summer_stock,
            SUM(CASE WHEN season = 'Sonbahar' THEN total_quantity ELSE 0 END) as autumn_stock,
            -- Öncelik dağılımı
            SUM(CASE WHEN priority_level = 'Yüksek' THEN total_quantity ELSE 0 END) as high_priority,
            SUM(CASE WHEN priority_level = 'Orta' THEN total_quantity ELSE 0 END) as medium_priority,
            SUM(CASE WHEN priority_level = 'Düşük' THEN total_quantity ELSE 0 END) as low_priority
        FROM `{PROJECT_ID}.{DATASET_ID}.central_warehouse_stock`
        GROUP BY category, subcategory
        ORDER BY total_value DESC
        """
        
        data = execute_query(query)
        return format_response(data, message="Merkez depo stok özeti")
        
    except Exception as e:
        return error_response(e)

# =====================================
# 5. DEMAND TREND ANALYSIS
# =====================================
@functions_framework.http
def demand_trends(request):
    """
    Mağaza talep trend analizi
    """
    try:
        query = f"""
        WITH trend_analysis AS (
            SELECT 
                store_id,
                store_name,
                city,
                product_category,
                avg_daily_sales_30d,
                sales_trend_percentage,
                weather_adjusted_demand,
                stock_turnover_rate,
                customer_traffic_index,
                CASE 
                    WHEN sales_trend_percentage > 20 THEN 'Güçlü Artış ↑↑'
                    WHEN sales_trend_percentage > 10 THEN 'Artış ↑'
                    WHEN sales_trend_percentage > -10 THEN 'Stabil →'
                    WHEN sales_trend_percentage > -20 THEN 'Düşüş ↓'
                    ELSE 'Güçlü Düşüş ↓↓'
                END as trend_category,
                CASE 
                    WHEN stockout_days_last_30d > 5 THEN 'Kritik stoksuzluk'
                    WHEN stockout_days_last_30d > 0 THEN 'Ara sıra stoksuzluk'
                    ELSE 'Stok yeterli'
                END as stock_status
            FROM `{PROJECT_ID}.{DATASET_ID}.store_demand_metrics`
            WHERE product_category IN ('Kaban', 'Mont', 'Dış Giyim')
        )
        SELECT 
            store_name,
            city,
            product_category,
            ROUND(avg_daily_sales_30d, 1) as avg_daily_sales,
            ROUND(sales_trend_percentage, 1) as trend_pct,
            trend_category,
            ROUND(weather_adjusted_demand, 1) as weather_adjusted,
            customer_traffic_index,
            stock_status,
            ROUND(stock_turnover_rate, 2) as turnover_rate
        FROM trend_analysis
        ORDER BY sales_trend_percentage DESC
        LIMIT 20
        """
        
        data = execute_query(query)
        return format_response(data, 
            message="Mağaza talep trend analizi - Son 30 gün")
        
    except Exception as e:
        return error_response(e)

# =====================================
# 6. ALLOCATION PERFORMANCE
# =====================================
@functions_framework.http
def allocation_performance(request):
    """
    AI allocation performans analizi
    """
    try:
        query = f"""
        WITH performance_metrics AS (
            SELECT 
                ar.store_id,
                ar.store_name,
                ar.sku,
                ar.product_name,
                ar.ai_recommended_qty,
                ar.erp_suggested_qty,
                ar.expected_sellthrough_7d,
                ar.expected_revenue,
                -- Verimlilik hesaplama
                ar.expected_revenue / NULLIF(ar.ai_recommended_qty * cw.unit_cost, 0) as roi_ai,
                ar.expected_sellthrough_7d / NULLIF(ar.ai_recommended_qty, 0) * 100 as sellthrough_rate_ai,
                dm.sales_trend_percentage,
                dm.stock_turnover_rate
            FROM `{PROJECT_ID}.{DATASET_ID}.allocation_recommendations` ar
            LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.central_warehouse_stock` cw
                ON ar.sku = cw.sku
            LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.store_demand_metrics` dm
                ON ar.store_id = dm.store_id
            WHERE ar.sku = 'KBN-001'
        )
        SELECT 
            store_name,
            product_name,
            ai_recommended_qty,
            expected_sellthrough_7d,
            ROUND(sellthrough_rate_ai, 1) as sellthrough_pct,
            expected_revenue,
            ROUND(roi_ai, 2) as roi_index,
            CASE 
                WHEN roi_ai > 1.5 THEN 'Çok Yüksek Verimlilik'
                WHEN roi_ai > 1.2 THEN 'Yüksek Verimlilik'
                WHEN roi_ai > 1.0 THEN 'Normal Verimlilik'
                ELSE 'Düşük Verimlilik'
            END as efficiency_level
        FROM performance_metrics
        ORDER BY roi_ai DESC
        """
        
        data = execute_query(query)
        return format_response(data, 
            message="AI allocation performans metrikleri")
        
    except Exception as e:
        return error_response(e)

# =====================================
# HELPER FUNCTION
# =====================================
def calculate_efficiency_score(data: List[Dict], method: str) -> float:
    """Calculate efficiency score for allocation method"""
    try:
        total_score = 0
        for store in data:
            if method == 'ai':
                days_supply = store.get('days_of_supply_ai', 0)
                qty = store.get('ai_recommended_qty', 0)
            else:
                days_supply = store.get('days_of_supply_erp', 0)
                qty = store.get('erp_suggested_qty', 0)
            
            # İdeal 14 gün stok
            if days_supply > 0:
                efficiency = 1 - abs(days_supply - 14) / 14
                total_score += efficiency * qty
        
        return round(total_score / sum(d.get('ai_recommended_qty', 0) for d in data) * 100, 2)
    except:
        return 0

# =====================================
# MAIN ROUTER (for testing)
# =====================================
@functions_framework.http
def main(request):
    """Main router for testing"""
    path = request.path
    
    if path == "/calculate_allocation":
        return calculate_allocation(request)
    elif path == "/compare_methods":
        return compare_allocation_methods(request)
    elif path == "/transfer":
        return transfer_recommendation(request)
    elif path == "/warehouse":
        return warehouse_summary(request)
    elif path == "/trends":
        return demand_trends(request)
    elif path == "/performance":
        return allocation_performance(request)
    else:
        return format_response({
            "available_endpoints": [
                "/calculate_allocation - Akıllı dağıtım hesaplama",
                "/compare_methods - AI vs ERP karşılaştırma",
                "/transfer - Transfer önerileri",
                "/warehouse - Depo stok özeti",
                "/trends - Talep trend analizi",
                "/performance - Performans metrikleri"
            ]
        }, message="Vakko Smart Allocation API v1.0")