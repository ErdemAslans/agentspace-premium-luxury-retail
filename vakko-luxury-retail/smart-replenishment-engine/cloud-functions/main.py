# main.py - Vakko Smart Replenishment Cloud Functions
# Production-ready BigQuery integration for Replenishment System

import functions_framework
import json
from google.cloud import bigquery
from datetime import datetime, timedelta
import logging
import os
from typing import Dict, List, Any, Optional

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = os.getenv("PROJECT_ID", "agentspace-ngc")
DATASET_ID = os.getenv("DATASET_ID", "vakko_replenishment")

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
# 1. LIST TABLES
# =====================================
@functions_framework.http
def list_tables(request):
    """List all Replenishment tables in the dataset"""
    try:
        dataset_ref = bq_client.dataset(DATASET_ID)
        tables = bq_client.list_tables(dataset_ref)
        
        table_list = []
        for table in tables:
            table_ref = bq_client.dataset(DATASET_ID).table(table.table_id)
            table_obj = bq_client.get_table(table_ref)
            
            table_list.append({
                "table_id": table.table_id,
                "full_table_id": f"{PROJECT_ID}.{DATASET_ID}.{table.table_id}",
                "row_count": table_obj.num_rows,
                "size_mb": round(table_obj.num_bytes / (1024*1024), 2) if table_obj.num_bytes else 0
            })
        
        return format_response(table_list, message=f"Found {len(table_list)} replenishment tables")
        
    except Exception as e:
        return error_response(e)

# =====================================
# 2. GET TABLE SCHEMA
# =====================================
@functions_framework.http
def get_table_schema(request):
    """Get schema and sample data for a replenishment table"""
    try:
        request_json = request.get_json(silent=True) or {}
        table_id = request_json.get('table_id', 'current_store_inventory')
        
        table_ref = bq_client.dataset(DATASET_ID).table(table_id)
        table_obj = bq_client.get_table(table_ref)
        
        schema = []
        for field in table_obj.schema:
            schema.append({
                "name": field.name,
                "type": field.field_type,
                "mode": field.mode,
                "description": field.description or ""
            })
        
        # Get sample data
        query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{table_id}` LIMIT 5"
        sample_data = execute_query(query)
        
        return format_response({
            "table_name": table_id,
            "schema": schema,
            "sample_data": sample_data,
            "total_rows": table_obj.num_rows
        }, message=f"Schema for {table_id}")
        
    except Exception as e:
        return error_response(e)

# =====================================
# 3. SQL QUERY
# =====================================
@functions_framework.http
def sql_query(request):
    """Execute custom SQL query on replenishment data"""
    try:
        request_json = request.get_json(silent=True) or {}
        query = request_json.get('query')
        
        if not query:
            return error_response("Query is required")
        
        # Safety check
        if "DROP" in query.upper() or "DELETE" in query.upper() or "TRUNCATE" in query.upper():
            return error_response("Destructive operations not allowed")
        
        data = execute_query(query)
        return format_response(data, message="Query executed successfully")
        
    except Exception as e:
        return error_response(e)

# =====================================
# 4. SMART REPLENISHMENT
# =====================================
@functions_framework.http
def smart_replenishment(request):
    """
    Kritik stok seviyelerini tespit edip replenishment önerileri
    Demo Prompt: 'Mont stoklarını incele. Hangi mağaza stokları 5 günden önce bitecek?'
    """
    try:
        request_json = request.get_json(silent=True) or {}
        critical_days = request_json.get('critical_days', 5)
        store_id = request_json.get('store_id')
        
        where_clause = ""
        if store_id:
            where_clause = f"AND i.store_id = '{store_id}'"
        
        query = f"""
        WITH sales_velocity AS (
            -- Son 7 günlük ortalama satış hızı
            SELECT 
                store_id,
                sku,
                size,
                color,
                AVG(daily_units_sold) as avg_daily_sales,
                MAX(daily_units_sold) as max_daily_sales,
                MIN(daily_units_sold) as min_daily_sales
            FROM `{PROJECT_ID}.{DATASET_ID}.daily_sales_velocity`
            WHERE date >= DATE_SUB('2025-07-12', INTERVAL 7 DAY)
            GROUP BY store_id, sku, size, color
        ),
        stock_analysis AS (
            SELECT 
                i.store_id,
                i.store_name,
                i.city,
                i.sku,
                i.product_name,
                i.size,
                i.color,
                i.current_stock,
                COALESCE(sv.avg_daily_sales, 0) as avg_daily_sales,
                -- Stok tükenme süresi
                CASE 
                    WHEN sv.avg_daily_sales > 0 
                    THEN ROUND(i.current_stock / sv.avg_daily_sales, 1)
                    ELSE 999
                END as days_until_stockout,
                r.min_order_qty,
                r.order_multiple,
                r.lead_time_days,
                r.priority_level,
                r.target_stock_days,
                r.safety_stock_days
            FROM `{PROJECT_ID}.{DATASET_ID}.current_store_inventory` i
            LEFT JOIN sales_velocity sv 
                ON i.store_id = sv.store_id 
                AND i.sku = sv.sku 
                AND i.size = sv.size 
                AND i.color = sv.color
            LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.replenishment_rules` r 
                ON i.store_id = r.store_id 
                AND i.sku = r.sku
            WHERE i.date = '2025-07-12'
                {where_clause}
        )
        SELECT 
            store_name,
            city,
            sku,
            product_name,
            size,
            color,
            current_stock,
            ROUND(avg_daily_sales, 1) as daily_sales,
            days_until_stockout as days_remaining,
            CASE 
                WHEN days_until_stockout <= 1.5 THEN '🔴 ÇOK KRİTİK'
                WHEN days_until_stockout <= 3 THEN '🔴 KRİTİK'
                WHEN days_until_stockout <= 5 THEN '🟠 UYARI'
                WHEN days_until_stockout <= 7 THEN '🟡 YAKINDA'
                ELSE '🟢 YETERLİ'
            END as urgency_level,
            -- Sipariş miktarı hesaplama
            CASE
                WHEN days_until_stockout > 7 THEN 0
                WHEN (target_stock_days * avg_daily_sales - current_stock) < min_order_qty 
                    THEN min_order_qty
                ELSE CEILING((target_stock_days * avg_daily_sales - current_stock) / order_multiple) * order_multiple
            END as suggested_order_qty,
            lead_time_days,
            CASE
                WHEN days_until_stockout <= lead_time_days THEN 'ACİL SİPARİŞ VER!'
                WHEN days_until_stockout <= lead_time_days + 1 THEN 'BUGÜN SİPARİŞ VER'
                WHEN days_until_stockout <= lead_time_days + safety_stock_days THEN 'BU HAFTA SİPARİŞ'
                ELSE 'İZLEMEDE TUT'
            END as action_required
        FROM stock_analysis
        WHERE avg_daily_sales > 0 
            AND days_until_stockout <= {critical_days}
        ORDER BY days_until_stockout ASC, priority_level DESC
        LIMIT 30
        """
        
        data = execute_query(query)
        return format_response(data, 
            message=f"{critical_days} gün içinde tükenecek ürünler ve replenishment önerileri")
        
    except Exception as e:
        return error_response(e)

# =====================================
# 5. STOCKOUT PREDICTION
# =====================================
@functions_framework.http
def stockout_prediction(request):
    """
    Stok tükenme tahmin modeli
    """
    try:
        request_json = request.get_json(silent=True) or {}
        forecast_days = request_json.get('forecast_days', 7)
        
        query = f"""
        WITH velocity_trend AS (
            -- Son 7 gün satış hızı analizi
            SELECT 
                store_id,
                sku,
                size,
                AVG(daily_units_sold) as avg_daily_sales,
                MAX(daily_units_sold) as max_daily_sales,
                MIN(daily_units_sold) as min_daily_sales,
                -- Hafta sonu faktörü
                AVG(CASE WHEN was_weekend = 1 THEN daily_units_sold END) / 
                    NULLIF(AVG(CASE WHEN was_weekend = 0 THEN daily_units_sold END), 0) as weekend_factor
            FROM `{PROJECT_ID}.{DATASET_ID}.daily_sales_velocity`
            WHERE date >= DATE_SUB('2025-07-12', INTERVAL 7 DAY)
            GROUP BY store_id, sku, size
        ),
        stockout_forecast AS (
            SELECT 
                i.store_id,
                i.store_name,
                i.city,
                i.sku,
                i.product_name,
                i.size,
                i.current_stock,
                vt.avg_daily_sales,
                vt.max_daily_sales,
                -- Güvenli stok tükenme tahmini (max satışa göre)
                ROUND(i.current_stock / NULLIF(vt.max_daily_sales, 0), 1) as pessimistic_days,
                -- Normal tahmin
                ROUND(i.current_stock / NULLIF(vt.avg_daily_sales, 0), 1) as expected_days,
                -- İyimser tahmin
                ROUND(i.current_stock / NULLIF(vt.min_daily_sales, 0), 1) as optimistic_days,
                -- Tahmini tükenme tarihi
                DATE_ADD('2025-07-12', INTERVAL 
                    CAST(i.current_stock / NULLIF(vt.avg_daily_sales, 0) AS INT64) DAY
                ) as predicted_stockout_date
            FROM `{PROJECT_ID}.{DATASET_ID}.current_store_inventory` i
            JOIN velocity_trend vt 
                ON i.store_id = vt.store_id 
                AND i.sku = vt.sku 
                AND i.size = vt.size
            WHERE i.date = '2025-07-12'
                AND vt.avg_daily_sales > 0
        )
        SELECT 
            store_name,
            city,
            sku,
            product_name,
            size,
            current_stock,
            ROUND(avg_daily_sales, 1) as daily_avg_sales,
            pessimistic_days as worst_case_days,
            expected_days as expected_days,
            predicted_stockout_date,
            CASE
                WHEN expected_days <= 2 THEN '🔴 2 gün içinde tükenecek'
                WHEN expected_days <= 3 THEN '🔴 3 gün içinde tükenecek'
                WHEN expected_days <= 5 THEN '🟠 5 gün içinde tükenecek'
                WHEN expected_days <= 7 THEN '🟡 1 hafta içinde kritik'
                ELSE '🟢 Stok yeterli'
            END as risk_level,
            CASE
                WHEN pessimistic_days <= 2 THEN 'En kötü senaryoda 2 gün!'
                ELSE ''
            END as warning
        FROM stockout_forecast
        WHERE expected_days <= {forecast_days}
        ORDER BY expected_days ASC
        LIMIT 25
        """
        
        data = execute_query(query)
        return format_response(data, 
            message=f"Önümüzdeki {forecast_days} gün içinde tükenecek ürün tahminleri")
        
    except Exception as e:
        return error_response(e)

# =====================================
# 6. REPLENISHMENT SCHEDULE
# =====================================
@functions_framework.http
def replenishment_schedule(request):
    """
    Haftalık replenishment takvimi ve transfer planı
    """
    try:
        query = f"""
        WITH stock_velocity AS (
            SELECT 
                i.store_id,
                i.store_name,
                i.city,
                i.sku,
                i.product_name,
                i.size,
                i.current_stock,
                COALESCE(v.avg_daily_sales, 0) as avg_daily_sales,
                r.lead_time_days,
                r.min_order_qty,
                r.order_multiple,
                r.priority_level,
                r.target_stock_days,
                -- Stok tükenme günü
                CASE 
                    WHEN v.avg_daily_sales > 0 
                    THEN ROUND(i.current_stock / v.avg_daily_sales, 0)
                    ELSE 999
                END as days_until_stockout
            FROM `{PROJECT_ID}.{DATASET_ID}.current_store_inventory` i
            LEFT JOIN (
                SELECT 
                    store_id, sku, size,
                    AVG(daily_units_sold) as avg_daily_sales
                FROM `{PROJECT_ID}.{DATASET_ID}.daily_sales_velocity`
                WHERE date >= DATE_SUB('2025-07-12', INTERVAL 7 DAY)
                GROUP BY store_id, sku, size
            ) v ON i.store_id = v.store_id AND i.sku = v.sku AND i.size = v.size
            LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.replenishment_rules` r
                ON i.store_id = r.store_id AND i.sku = r.sku
            WHERE i.date = '2025-07-12'
        ),
        replenishment_plan AS (
            SELECT 
                store_id,
                store_name,
                city,
                sku,
                product_name,
                size,
                current_stock,
                avg_daily_sales,
                days_until_stockout,
                lead_time_days,
                priority_level,
                -- Sipariş verme günü
                CASE 
                    WHEN days_until_stockout - lead_time_days <= 0 THEN 'BUGÜN'
                    WHEN days_until_stockout - lead_time_days <= 1 THEN 'YARIN'
                    WHEN days_until_stockout - lead_time_days <= 3 THEN 'BU HAFTA'
                    ELSE 'GELECEk HAFTA'
                END as order_timing,
                -- Sipariş miktarı
                CEILING((target_stock_days * avg_daily_sales - current_stock) / order_multiple) * order_multiple as order_qty
            FROM stock_velocity
            WHERE avg_daily_sales > 0
                AND days_until_stockout <= 10
        )
        SELECT 
            store_name,
            city,
            COUNT(DISTINCT CONCAT(sku, '-', size)) as critical_products,
            SUM(CASE WHEN order_timing = 'BUGÜN' THEN 1 ELSE 0 END) as urgent_today,
            SUM(CASE WHEN order_timing = 'YARIN' THEN 1 ELSE 0 END) as tomorrow,
            SUM(CASE WHEN order_timing = 'BU HAFTA' THEN 1 ELSE 0 END) as this_week,
            STRING_AGG(
                CASE WHEN order_timing = 'BUGÜN' 
                THEN CONCAT(product_name, ' ', size) 
                END LIMIT 3
            ) as urgent_products,
            SUM(order_qty) as total_units_needed,
            COUNT(CASE WHEN priority_level = 'High' THEN 1 END) as high_priority_items,
            CASE 
                WHEN SUM(CASE WHEN order_timing = 'BUGÜN' THEN 1 ELSE 0 END) > 5 THEN '🚨 KRİTİK DURUM'
                WHEN SUM(CASE WHEN order_timing = 'BUGÜN' THEN 1 ELSE 0 END) > 2 THEN '🔴 ACİL'
                WHEN SUM(CASE WHEN order_timing IN ('BUGÜN', 'YARIN') THEN 1 ELSE 0 END) > 3 THEN '🟠 ÖNCELİKLİ'
                ELSE '🟡 NORMAL'
            END as store_status
        FROM replenishment_plan
        GROUP BY store_name, city
        ORDER BY urgent_today DESC, tomorrow DESC
        """
        
        data = execute_query(query)
        return format_response(data, 
            message="Mağaza bazlı haftalık replenishment planlama özeti")
        
    except Exception as e:
        return error_response(e)

# =====================================
# 7. INVENTORY OPTIMIZATION
# =====================================
@functions_framework.http
def inventory_optimization(request):
    """
    Envanter optimizasyon önerileri - Fazla ve eksik stoklar
    """
    try:
        query = f"""
        WITH inventory_metrics AS (
            SELECT 
                i.store_id,
                i.store_name,
                i.city,
                i.sku,
                i.product_name,
                i.size,
                i.current_stock,
                i.min_stock_level,
                i.max_stock_level,
                COALESCE(v.avg_sales, 0) as avg_sales,
                -- Stok devir hızı (aylık)
                CASE 
                    WHEN i.current_stock > 0 
                    THEN (v.avg_sales * 30) / i.current_stock
                    ELSE 0
                END as turnover_rate,
                -- Optimal stok seviyesi
                r.safety_stock_days * v.avg_sales + r.lead_time_days * v.avg_sales as optimal_stock,
                r.priority_level,
                r.cost_per_unit
            FROM `{PROJECT_ID}.{DATASET_ID}.current_store_inventory` i
            LEFT JOIN (
                SELECT 
                    store_id, sku, size,
                    AVG(daily_units_sold) as avg_sales
                FROM `{PROJECT_ID}.{DATASET_ID}.daily_sales_velocity`
                WHERE date >= DATE_SUB('2025-07-12', INTERVAL 7 DAY)
                GROUP BY store_id, sku, size
            ) v ON i.store_id = v.store_id AND i.sku = v.sku AND i.size = v.size
            LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.replenishment_rules` r
                ON i.store_id = r.store_id AND i.sku = r.sku
            WHERE i.date = '2025-07-12'
        )
        SELECT 
            store_name,
            city,
            sku,
            product_name,
            size,
            current_stock,
            ROUND(optimal_stock, 0) as recommended_stock,
            current_stock - ROUND(optimal_stock, 0) as stock_difference,
            CASE 
                WHEN avg_sales = 0 AND current_stock > 50 THEN '💀 ÖLMÜŞ STOK'
                WHEN current_stock > optimal_stock * 2 THEN '📦 FAZLA STOK'
                WHEN current_stock > optimal_stock * 1.5 THEN '🔵 Yüksek stok'
                WHEN current_stock < optimal_stock * 0.3 THEN '🔴 KRİTİK DÜŞÜK'
                WHEN current_stock < optimal_stock * 0.5 THEN '🟠 Düşük stok'
                WHEN current_stock BETWEEN optimal_stock * 0.8 AND optimal_stock * 1.2 THEN '🟢 OPTİMAL'
                ELSE '🟡 İzlemede'
            END as stock_status,
            ROUND(turnover_rate, 2) as monthly_turnover,
            ROUND(current_stock * cost_per_unit * 0.02, 2) as monthly_holding_cost_tl,
            CASE
                WHEN avg_sales = 0 AND current_stock > 50 THEN 'Transfer veya outlet'
                WHEN current_stock > optimal_stock * 2 THEN 'Transfer düşün'
                WHEN current_stock < optimal_stock * 0.3 THEN 'ACİL sipariş'
                WHEN current_stock < optimal_stock * 0.5 THEN 'Sipariş ver'
                WHEN turnover_rate < 1 THEN 'Yavaş hareket - promosyon?'
                ELSE 'Normal takip'
            END as recommendation
        FROM inventory_metrics
        WHERE ABS(current_stock - optimal_stock) > 10
            OR (avg_sales = 0 AND current_stock > 20)
        ORDER BY 
            CASE 
                WHEN avg_sales = 0 AND current_stock > 50 THEN 1
                WHEN current_stock < optimal_stock * 0.3 THEN 2
                ELSE 3
            END,
            ABS(current_stock - optimal_stock) DESC
        LIMIT 30
        """
        
        data = execute_query(query)
        return format_response(data, 
            message="Envanter optimizasyon önerileri - Fazla ve eksik stoklar")
        
    except Exception as e:
        return error_response(e)

# =====================================
# 8. TRANSFER RECOMMENDATION
# =====================================
@functions_framework.http
def transfer_recommendation(request):
    """
    Mağazalar arası transfer önerileri
    """
    try:
        query = f"""
        WITH store_stock_status AS (
            SELECT 
                i.store_id,
                i.store_name,
                i.city,
                i.sku,
                i.product_name,
                i.size,
                i.current_stock,
                COALESCE(v.avg_daily_sales, 0) as avg_daily_sales,
                CASE 
                    WHEN v.avg_daily_sales > 0 
                    THEN i.current_stock / v.avg_daily_sales
                    ELSE 999
                END as days_of_supply,
                r.target_stock_days
            FROM `{PROJECT_ID}.{DATASET_ID}.current_store_inventory` i
            LEFT JOIN (
                SELECT 
                    store_id, sku, size,
                    AVG(daily_units_sold) as avg_daily_sales
                FROM `{PROJECT_ID}.{DATASET_ID}.daily_sales_velocity`
                WHERE date >= DATE_SUB('2025-07-12', INTERVAL 7 DAY)
                GROUP BY store_id, sku, size
            ) v ON i.store_id = v.store_id AND i.sku = v.sku AND i.size = v.size
            LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.replenishment_rules` r
                ON i.store_id = r.store_id AND i.sku = r.sku
            WHERE i.date = '2025-07-12'
        ),
        transfer_pairs AS (
            SELECT 
                -- Fazla stoklu mağaza
                excess.store_name as from_store,
                excess.city as from_city,
                excess.sku,
                excess.product_name,
                excess.size,
                excess.current_stock as from_stock,
                excess.days_of_supply as from_days,
                -- Az stoklu mağaza
                shortage.store_name as to_store,
                shortage.city as to_city,
                shortage.current_stock as to_stock,
                shortage.days_of_supply as to_days,
                -- Transfer miktarı
                LEAST(
                    excess.current_stock - (excess.avg_daily_sales * excess.target_stock_days),
                    (shortage.avg_daily_sales * shortage.target_stock_days) - shortage.current_stock
                ) as transfer_qty,
                -- Transfer tipi
                CASE 
                    WHEN excess.city = shortage.city THEN 'Şehir içi'
                    ELSE 'Şehirler arası'
                END as transfer_type
            FROM store_stock_status excess
            INNER JOIN store_stock_status shortage
                ON excess.sku = shortage.sku 
                AND excess.size = shortage.size
                AND excess.store_id != shortage.store_id
            WHERE excess.days_of_supply > 30  -- Fazla stok
                AND shortage.days_of_supply < 7  -- Az stok
                AND excess.avg_daily_sales > 0
                AND shortage.avg_daily_sales > 0
        )
        SELECT 
            from_store,
            from_city,
            to_store,
            to_city,
            sku,
            product_name,
            size,
            from_stock,
            to_stock,
            ROUND(from_days, 1) as from_days_supply,
            ROUND(to_days, 1) as to_days_supply,
            ROUND(transfer_qty, 0) as suggested_transfer_qty,
            transfer_type,
            CASE 
                WHEN to_days < 3 THEN '🔴 Yüksek öncelik'
                WHEN to_days < 5 THEN '🟠 Orta öncelik'
                ELSE '🟡 Düşük öncelik'
            END as priority,
            CASE 
                WHEN transfer_type = 'Şehir içi' THEN '1 gün teslimat'
                ELSE '2-3 gün teslimat'
            END as delivery_time
        FROM transfer_pairs
        WHERE transfer_qty > 5
        ORDER BY to_days ASC, transfer_qty DESC
        LIMIT 20
        """
        
        data = execute_query(query)
        return format_response(data, 
            message="Mağazalar arası transfer önerileri - Fazla stoktan eksik stoğa")
        
    except Exception as e:
        return error_response(e)

# =====================================
# 9. CRITICAL STOCK ALERTS
# =====================================
@functions_framework.http
def critical_stock_alerts(request):
    """
    Kritik stok uyarıları dashboard
    """
    try:
        query = f"""
        WITH store_alerts AS (
            SELECT 
                i.store_id,
                i.store_name,
                i.city,
                COUNT(DISTINCT CONCAT(i.sku, '-', i.size)) as total_skus,
                COUNT(DISTINCT CASE 
                    WHEN i.current_stock = 0 THEN CONCAT(i.sku, '-', i.size) 
                END) as out_of_stock,
                COUNT(DISTINCT CASE 
                    WHEN i.current_stock > 0 AND i.current_stock / NULLIF(v.avg_sales, 0) <= 2 
                    THEN CONCAT(i.sku, '-', i.size) 
                END) as critical_2days,
                COUNT(DISTINCT CASE 
                    WHEN i.current_stock > 0 AND i.current_stock / NULLIF(v.avg_sales, 0) <= 5 
                    THEN CONCAT(i.sku, '-', i.size) 
                END) as warning_5days,
                SUM(i.current_stock) as total_stock_units,
                SUM(i.current_stock * r.cost_per_unit) as total_stock_value,
                AVG(CASE 
                    WHEN v.avg_sales > 0 
                    THEN i.current_stock / v.avg_sales 
                    ELSE NULL 
                END) as avg_days_of_supply
            FROM `{PROJECT_ID}.{DATASET_ID}.current_store_inventory` i
            LEFT JOIN (
                SELECT 
                    store_id, sku, size,
                    AVG(daily_units_sold) as avg_sales
                FROM `{PROJECT_ID}.{DATASET_ID}.daily_sales_velocity`
                WHERE date >= DATE_SUB('2025-07-12', INTERVAL 7 DAY)
                GROUP BY store_id, sku, size
            ) v ON i.store_id = v.store_id AND i.sku = v.sku AND i.size = v.size
            LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.replenishment_rules` r
                ON i.store_id = r.store_id AND i.sku = r.sku
            WHERE i.date = '2025-07-12'
            GROUP BY i.store_id, i.store_name, i.city
        )
        SELECT 
            store_name,
            city,
            total_skus,
            out_of_stock,
            critical_2days,
            warning_5days,
            ROUND((critical_2days * 100.0 / NULLIF(total_skus, 0)), 1) as critical_pct,
            total_stock_units,
            ROUND(total_stock_value, 0) as stock_value_tl,
            ROUND(avg_days_of_supply, 1) as avg_coverage_days,
            CASE
                WHEN critical_2days > 5 THEN '🚨 KRİTİK DURUM'
                WHEN critical_2days > 2 THEN '🔴 Yüksek risk'
                WHEN warning_5days > 5 THEN '🟠 Dikkat gerekli'
                WHEN out_of_stock > 2 THEN '🟡 Stoksuzluk var'
                ELSE '🟢 Normal'
            END as store_status,
            CASE
                WHEN critical_2days > 5 THEN 'Acil replenishment gerekli!'
                WHEN critical_2days > 2 THEN 'Bugün sipariş ver'
                WHEN warning_5days > 5 THEN 'Haftalık plan yap'
                WHEN out_of_stock > 0 THEN 'Stoksuz ürünleri kontrol et'
                ELSE 'Rutin takip'
            END as action
        FROM store_alerts
        ORDER BY critical_2days DESC, warning_5days DESC, out_of_stock DESC
        """
        
        data = execute_query(query)
        return format_response(data, 
            message="Mağaza bazlı kritik stok durumu özeti")
        
    except Exception as e:
        return error_response(e)

# =====================================
# 10. SALES VELOCITY ANALYSIS
# =====================================
@functions_framework.http
def sales_velocity_analysis(request):
    """
    Satış hızı trend analizi - Hangi ürünler hızlanıyor/yavaşlıyor
    """
    try:
        query = f"""
        WITH daily_aggregates AS (
            SELECT 
                date,
                store_id,
                sku,
                size,
                SUM(daily_units_sold) as total_daily_sales,
                AVG(weather_temp) as avg_temp,
                MAX(was_promotion) as had_promo
            FROM `{PROJECT_ID}.{DATASET_ID}.daily_sales_velocity`
            GROUP BY date, store_id, sku, size
        ),
        velocity_comparison AS (
            SELECT 
                i.store_name,
                i.city,
                i.sku,
                i.product_name,
                i.size,
                -- İlk 3 gün ortalaması
                AVG(CASE 
                    WHEN da.date BETWEEN '2025-07-06' AND '2025-07-08'
                    THEN da.total_daily_sales 
                END) as first_period_avg,
                -- Son 3 gün ortalaması
                AVG(CASE 
                    WHEN da.date BETWEEN '2025-07-10' AND '2025-07-12'
                    THEN da.total_daily_sales 
                END) as last_period_avg,
                -- Genel ortalama
                AVG(da.total_daily_sales) as overall_avg,
                -- Promosyon günleri
                SUM(da.had_promo) as promo_days,
                -- Hava durumu korelasyonu
                CORR(da.avg_temp, da.total_daily_sales) as temp_correlation
            FROM `{PROJECT_ID}.{DATASET_ID}.current_store_inventory` i
            JOIN daily_aggregates da
                ON i.store_id = da.store_id 
                AND i.sku = da.sku 
                AND i.size = da.size
            WHERE i.date = '2025-07-12'
            GROUP BY i.store_name, i.city, i.sku, i.product_name, i.size
        )
        SELECT 
            store_name,
            city,
            sku,
            product_name,
            size,
            ROUND(first_period_avg, 1) as early_week_avg,
            ROUND(last_period_avg, 1) as late_week_avg,
            ROUND(overall_avg, 1) as week_avg,
            ROUND((last_period_avg - first_period_avg) / NULLIF(first_period_avg, 0) * 100, 1) as change_pct,
            CASE
                WHEN (last_period_avg - first_period_avg) / NULLIF(first_period_avg, 0) > 0.5 THEN '🚀 Hızlı artış'
                WHEN (last_period_avg - first_period_avg) / NULLIF(first_period_avg, 0) > 0.2 THEN '📈 Artış'
                WHEN (last_period_avg - first_period_avg) / NULLIF(first_period_avg, 0) < -0.3 THEN '📉 Düşüş'
                WHEN (last_period_avg - first_period_avg) / NULLIF(first_period_avg, 0) < -0.1 THEN '⬇️ Hafif düşüş'
                ELSE '➡️ Stabil'
            END as trend,
            CASE 
                WHEN promo_days > 3 THEN 'Promosyonlu'
                ELSE 'Normal'
            END as promo_status,
            ROUND(temp_correlation, 2) as weather_sensitivity
        FROM velocity_comparison
        WHERE overall_avg > 0
        ORDER BY ABS(change_pct) DESC
        LIMIT 30
        """
        
        data = execute_query(query)
        return format_response(data, 
            message="Satış hızı trend analizi - Haftalık değişimler")
        
    except Exception as e:
        return error_response(e)

# =====================================
# 11. WAREHOUSE SUMMARY
# =====================================
@functions_framework.http
def warehouse_summary(request):
    """
    Merkez depo özet raporu
    """
    try:
        # Warehouse'da ne var simulasyonu (mağaza stoklarının toplamı)
        query = f"""
        WITH warehouse_simulation AS (
            SELECT 
                i.sku,
                i.product_name,
                i.category,
                -- Toplam mağaza stokları (warehouse simülasyonu)
                SUM(i.current_stock) as total_in_stores,
                -- Ortalama satış hızı
                AVG(v.avg_daily_sales) as avg_daily_velocity,
                -- Kaç mağazada var
                COUNT(DISTINCT i.store_id) as store_coverage,
                -- En düşük stoklu mağaza
                MIN(i.current_stock) as min_store_stock,
                MAX(i.current_stock) as max_store_stock,
                -- Ortalama maliyet
                AVG(r.cost_per_unit) as avg_cost
            FROM `{PROJECT_ID}.{DATASET_ID}.current_store_inventory` i
            LEFT JOIN (
                SELECT 
                    store_id, sku,
                    AVG(daily_units_sold) as avg_daily_sales
                FROM `{PROJECT_ID}.{DATASET_ID}.daily_sales_velocity`
                GROUP BY store_id, sku
            ) v ON i.store_id = v.store_id AND i.sku = v.sku
            LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.replenishment_rules` r
                ON i.store_id = r.store_id AND i.sku = r.sku
            WHERE i.date = '2025-07-12'
            GROUP BY i.sku, i.product_name, i.category
        )
        SELECT 
            category,
            sku,
            product_name,
            total_in_stores as total_units,
            store_coverage,
            ROUND(avg_daily_velocity * 5, 1) as weekly_velocity,
            ROUND(total_in_stores / NULLIF(avg_daily_velocity * 5, 0), 1) as weeks_of_supply,
            min_store_stock,
            max_store_stock,
            ROUND(total_in_stores * avg_cost, 0) as inventory_value_tl,
            CASE 
                WHEN total_in_stores / NULLIF(avg_daily_velocity * 5, 0) > 8 THEN '📦 Fazla stok'
                WHEN total_in_stores / NULLIF(avg_daily_velocity * 5, 0) < 2 THEN '🔴 Az stok'
                ELSE '🟢 Normal'
            END as stock_health,
            CASE 
                WHEN min_store_stock = 0 THEN 'Bazı mağazalarda stoksuz!'
                WHEN min_store_stock < 10 THEN 'Dengesiz dağılım'
                ELSE 'Dengeli dağılım'
            END as distribution_status
        FROM warehouse_simulation
        ORDER BY inventory_value_tl DESC
        LIMIT 20
        """
        
        data = execute_query(query)
        return format_response(data, 
            message="Ürün bazlı toplam envanter durumu (tüm mağazalar)")
        
    except Exception as e:
        return error_response(e)

# =====================================
# 12. DEMAND TRENDS
# =====================================
@functions_framework.http
def demand_trends(request):
    """
    Talep trendleri ve hava durumu etkisi
    """
    try:
        query = f"""
        WITH demand_analysis AS (
            SELECT 
                v.store_id,
                i.store_name,
                i.city,
                i.category,
                -- Haftalık ortalamalar
                AVG(v.daily_units_sold) as avg_daily_demand,
                -- Hava durumu grupları
                AVG(CASE WHEN v.weather_temp < 10 THEN v.daily_units_sold END) as cold_weather_sales,
                AVG(CASE WHEN v.weather_temp >= 10 AND v.weather_temp < 20 THEN v.daily_units_sold END) as mild_weather_sales,
                AVG(CASE WHEN v.weather_temp >= 20 THEN v.daily_units_sold END) as warm_weather_sales,
                -- Hafta sonu vs hafta içi
                AVG(CASE WHEN v.was_weekend = 1 THEN v.daily_units_sold END) as weekend_avg,
                AVG(CASE WHEN v.was_weekend = 0 THEN v.daily_units_sold END) as weekday_avg,
                -- Promosyon etkisi
                AVG(CASE WHEN v.was_promotion = 1 THEN v.daily_units_sold END) as promo_avg,
                AVG(CASE WHEN v.was_promotion = 0 THEN v.daily_units_sold END) as regular_avg,
                -- Müşteri trafiği
                AVG(v.customer_count) as avg_traffic
            FROM `{PROJECT_ID}.{DATASET_ID}.daily_sales_velocity` v
            JOIN (
                SELECT DISTINCT store_id, store_name, city, category
                FROM `{PROJECT_ID}.{DATASET_ID}.current_store_inventory`
                WHERE date = '2025-07-12'
            ) i ON v.store_id = i.store_id
            GROUP BY v.store_id, i.store_name, i.city, i.category
        )
        SELECT 
            store_name,
            city,
            category,
            ROUND(avg_daily_demand, 1) as avg_daily_sales,
            ROUND((weekend_avg - weekday_avg) / NULLIF(weekday_avg, 0) * 100, 1) as weekend_lift_pct,
            ROUND((promo_avg - regular_avg) / NULLIF(regular_avg, 0) * 100, 1) as promo_lift_pct,
            CASE 
                WHEN cold_weather_sales > warm_weather_sales * 1.5 THEN '❄️ Soğuk hava ürünü'
                WHEN warm_weather_sales > cold_weather_sales * 1.5 THEN '☀️ Sıcak hava ürünü'
                ELSE '🌤️ Hava bağımsız'
            END as weather_sensitivity,
            ROUND(avg_traffic, 0) as daily_traffic,
            CASE 
                WHEN weekend_avg > weekday_avg * 1.3 THEN 'Hafta sonu yoğun'
                WHEN weekday_avg > weekend_avg * 1.3 THEN 'Hafta içi yoğun'
                ELSE 'Dengeli'
            END as sales_pattern,
            CASE 
                WHEN promo_avg > regular_avg * 1.5 THEN '🎯 Promosyona çok duyarlı'
                WHEN promo_avg > regular_avg * 1.2 THEN '📢 Promosyona duyarlı'
                ELSE '💰 Fiyat dengeli'
            END as price_sensitivity
        FROM demand_analysis
        WHERE avg_daily_demand > 0
        ORDER BY avg_daily_demand DESC
        LIMIT 20
        """
        
        data = execute_query(query)
        return format_response(data, 
            message="Mağaza bazlı talep trendleri ve duyarlılık analizi")
        
    except Exception as e:
        return error_response(e)

# =====================================
# MAIN ROUTER
# =====================================
@functions_framework.http
def main(request):
    """
    Main entry point for routing requests
    """
    try:
        path = request.path
        
        # Route to appropriate function
        routes = {
            "/list_tables": list_tables,
            "/get_table_schema": get_table_schema,
            "/sql_query": sql_query,
            "/smart_replenishment": smart_replenishment,
            "/stockout_prediction": stockout_prediction,
            "/replenishment_schedule": replenishment_schedule,
            "/inventory_optimization": inventory_optimization,
            "/transfer_recommendation": transfer_recommendation,
            "/critical_stock_alerts": critical_stock_alerts,
            "/sales_velocity_analysis": sales_velocity_analysis,
            "/warehouse_summary": warehouse_summary,
            "/demand_trends": demand_trends
        }
        
        if path in routes:
            return routes[path](request)
        else:
            return format_response({
                "available_endpoints": list(routes.keys()),
                "description": {
                    "/list_tables": "Tüm replenishment tablolarını listele",
                    "/get_table_schema": "Tablo şeması ve örnek veri",
                    "/sql_query": "Özel SQL sorgusu çalıştır",
                    "/smart_replenishment": "Kritik stok ve sipariş önerileri",
                    "/stockout_prediction": "Stok tükenme tahminleri",
                    "/replenishment_schedule": "Haftalık sipariş takvimi",
                    "/inventory_optimization": "Optimal stok seviyeleri",
                    "/transfer_recommendation": "Mağazalar arası transfer",
                    "/critical_stock_alerts": "Kritik stok uyarıları",
                    "/sales_velocity_analysis": "Satış hızı analizi",
                    "/warehouse_summary": "Toplam envanter durumu",
                    "/demand_trends": "Talep trendleri"
                }
            }, message="Vakko Smart Replenishment API v1.0")
            
    except Exception as e:
        return error_response(e)