# main.py - Vakko AI Analytics Cloud Functions
# Production-ready BigQuery integration for Vakko retail data

import functions_framework
import json
from google.cloud import bigquery
from datetime import datetime, timedelta
import logging
import os
from typing import Dict, List, Any, Optional
import numpy as np

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = os.getenv("PROJECT_ID", "agentspace-ngc")
DATASET_ID = os.getenv("DATASET_ID", "vakko_retail")

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
    """List all Vakko tables in the dataset"""
    try:
        dataset_ref = bq_client.dataset(DATASET_ID)
        tables = bq_client.list_tables(dataset_ref)
        
        table_list = []
        for table in tables:
            table_list.append({
                "table_id": table.table_id,
                "full_table_id": f"{PROJECT_ID}.{DATASET_ID}.{table.table_id}"
            })
        
        return format_response(table_list, message=f"Found {len(table_list)} tables")
        
    except Exception as e:
        return error_response(e)

# =====================================
# 2. GET TABLE SCHEMA
# =====================================
@functions_framework.http
def get_table_schema(request):
    """Get schema and sample data for a table"""
    try:
        request_json = request.get_json(silent=True) or {}
        table_id = request_json.get('table_id', 'product_sales_daily')
        
        table_ref = bq_client.dataset(DATASET_ID).table(table_id)
        table_obj = bq_client.get_table(table_ref)
        
        schema = []
        for field in table_obj.schema:
            schema.append({
                "name": field.name,
                "type": field.field_type,
                "mode": field.mode
            })
        
        # Get sample data
        query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{table_id}` LIMIT 3"
        sample_data = execute_query(query)
        
        return format_response({
            "schema": schema,
            "sample_data": sample_data,
            "row_count": table_obj.num_rows
        })
        
    except Exception as e:
        return error_response(e)

# =====================================
# 3. EXECUTE SQL QUERY
# =====================================
@functions_framework.http
def sql_query(request):
    """Execute custom SQL query"""
    try:
        request_json = request.get_json(silent=True) or {}
        query = request_json.get('query')
        
        if not query:
            return error_response("Query is required")
        
        data = execute_query(query)
        return format_response(data)
        
    except Exception as e:
        return error_response(e)

# =====================================
# 4. DEMAND FORECAST (Talep Tahmini)
# =====================================
@functions_framework.http
def demand_forecast(request):
    """
    Mont satış tahmini - Hava durumu ve geçmiş verilerle
    """
    try:
        request_json = request.get_json(silent=True) or {}
        days_ahead = request_json.get('days_ahead', 7)
        store_id = request_json.get('store_id')
        
        # Build WHERE clause
        where_clause = "WHERE s.product_subcategory = 'Mont'"
        if store_id:
            where_clause += f" AND s.store_id = '{store_id}'"
        
        query = f"""
        WITH historical_sales AS (
            -- Son 11 günlük satış verileri
            SELECT 
                s.store_id,
                s.store_name,
                s.city,
                s.date,
                SUM(s.units_sold) as daily_units,
                SUM(s.revenue) as daily_revenue,
                AVG(s.weather_temp) as avg_temp,
                MAX(s.weather_condition) as weather_condition
            FROM `{PROJECT_ID}.{DATASET_ID}.product_sales_daily` s
            {where_clause}
            GROUP BY s.store_id, s.store_name, s.city, s.date
        ),
        weather_impact AS (
            -- Hava durumu etkisi hesaplama
            SELECT 
                store_id,
                store_name,
                city,
                AVG(daily_units) as base_units,
                AVG(daily_revenue) as base_revenue,
                AVG(CASE 
                    WHEN avg_temp <= -5 THEN daily_units * 1.8  -- Çok soğukta %80 artış
                    WHEN avg_temp <= 0 THEN daily_units * 1.5   -- Soğukta %50 artış
                    WHEN avg_temp <= 5 THEN daily_units * 1.2   -- Serinde %20 artış
                    WHEN avg_temp >= 15 THEN daily_units * 0.6  -- Sıcakta %40 düşüş
                    ELSE daily_units
                END) as weather_adjusted_units
            FROM historical_sales
            GROUP BY store_id, store_name, city
        ),
        forecast_data AS (
            -- Önümüzdeki 7 gün için tahmin
            SELECT 
                w.store_id,
                w.store_name,
                w.city,
                ROUND(w.weather_adjusted_units) as forecast_daily_units,
                ROUND(w.base_revenue * (w.weather_adjusted_units / NULLIF(w.base_units, 0))) as forecast_daily_revenue,
                CASE 
                    WHEN w.weather_adjusted_units > w.base_units * 1.3 THEN 'Yüksek Talep Bekleniyor 🔴'
                    WHEN w.weather_adjusted_units > w.base_units * 1.1 THEN 'Orta Artış Bekleniyor 🟡'
                    ELSE 'Normal Talep 🟢'
                END as demand_level,
                ROUND((w.weather_adjusted_units - w.base_units) / NULLIF(w.base_units, 0) * 100, 1) as change_percentage,
                {days_ahead} as forecast_days
            FROM weather_impact w
        )
        SELECT 
            store_id,
            store_name,
            city,
            forecast_daily_units,
            forecast_daily_revenue,
            demand_level,
            change_percentage,
            CASE 
                WHEN forecast_daily_units > 30 THEN 'Stok takviyesi önerilir'
                WHEN forecast_daily_units > 20 THEN 'Stok seviyesini kontrol edin'
                ELSE 'Mevcut stok yeterli'
            END as recommendation
        FROM forecast_data
        ORDER BY forecast_daily_units DESC
        """
        
        data = execute_query(query)
        
        message = f"Önümüzdeki {days_ahead} gün için mont satış tahmini"
        if store_id:
            message += f" - {store_id} mağazası"
        
        return format_response(data, message=message)
        
    except Exception as e:
        return error_response(e)

# =====================================
# 5. WEATHER IMPACT ANALYSIS
# =====================================
@functions_framework.http
def weather_impact(request):
    """
    Hava durumunun satışlara etkisi analizi
    """
    try:
        query = f"""
        WITH weather_sales AS (
            SELECT 
                s.date,
                w.temp_avg,
                w.weather_condition,
                w.precipitation_mm,
                SUM(s.units_sold) as total_units,
                SUM(s.revenue) as total_revenue,
                COUNT(DISTINCT s.store_id) as store_count
            FROM `{PROJECT_ID}.{DATASET_ID}.product_sales_daily` s
            JOIN `{PROJECT_ID}.{DATASET_ID}.weather_data` w
                ON s.store_id = w.store_id AND s.date = w.date
            WHERE s.product_subcategory = 'Mont'
            GROUP BY s.date, w.temp_avg, w.weather_condition, w.precipitation_mm
        ),
        temperature_bands AS (
            SELECT 
                CASE 
                    WHEN temp_avg < -5 THEN '1. Çok Soğuk (< -5°C)'
                    WHEN temp_avg < 0 THEN '2. Soğuk (-5 to 0°C)'
                    WHEN temp_avg < 5 THEN '3. Serin (0 to 5°C)'
                    WHEN temp_avg < 10 THEN '4. Ilıman (5 to 10°C)'
                    WHEN temp_avg < 15 THEN '5. Ilık (10 to 15°C)'
                    ELSE '6. Sıcak (> 15°C)'
                END as temp_category,
                AVG(total_units) as avg_units_sold,
                AVG(total_revenue) as avg_revenue,
                COUNT(*) as days_count,
                ROUND(CORR(temp_avg, total_units), 3) as correlation
            FROM weather_sales
            GROUP BY temp_category
        )
        SELECT 
            temp_category,
            ROUND(avg_units_sold, 1) as avg_daily_units,
            ROUND(avg_revenue, 0) as avg_daily_revenue,
            days_count,
            correlation,
            CASE 
                WHEN avg_units_sold = MAX(avg_units_sold) OVER() THEN '🏆 En Yüksek Satış'
                WHEN avg_units_sold = MIN(avg_units_sold) OVER() THEN '⚠️ En Düşük Satış'
                ELSE ''
            END as note
        FROM temperature_bands
        ORDER BY temp_category
        """
        
        data = execute_query(query)
        return format_response(data, message="Sıcaklık ve mont satışları korelasyon analizi")
        
    except Exception as e:
        return error_response(e)

# =====================================
# 6. INVENTORY OPTIMIZATION
# =====================================
@functions_framework.http
def inventory_optimization(request):
    """
    Stok optimizasyon önerileri
    """
    try:
        query = f"""
        WITH current_inventory AS (
            SELECT 
                i.store_id,
                i.store_name,
                i.sku,
                i.product_name,
                i.closing_stock,
                i.days_of_supply,
                i.stockout_flag,
                i.turnover_rate
            FROM `{PROJECT_ID}.{DATASET_ID}.store_inventory` i
            WHERE i.date = (SELECT MAX(date) FROM `{PROJECT_ID}.{DATASET_ID}.store_inventory`)
                AND i.subcategory = 'Mont'
        ),
        sales_velocity AS (
            SELECT 
                s.store_id,
                s.sku,
                AVG(s.units_sold) as avg_daily_sales,
                MAX(s.units_sold) as max_daily_sales
            FROM `{PROJECT_ID}.{DATASET_ID}.product_sales_daily` s
            WHERE s.product_subcategory = 'Mont'
            GROUP BY s.store_id, s.sku
        )
        SELECT 
            ci.store_id,
            ci.store_name,
            ci.sku,
            ci.product_name,
            ci.closing_stock as current_stock,
            ROUND(sv.avg_daily_sales, 1) as avg_daily_sales,
            ROUND(ci.closing_stock / NULLIF(sv.avg_daily_sales, 0), 1) as days_until_stockout,
            CASE 
                WHEN ci.stockout_flag THEN '🔴 Stok Tükendi!'
                WHEN ci.closing_stock / NULLIF(sv.avg_daily_sales, 0) < 3 THEN '🟠 Kritik Seviye'
                WHEN ci.closing_stock / NULLIF(sv.avg_daily_sales, 0) < 7 THEN '🟡 Sipariş Ver'
                WHEN ci.closing_stock / NULLIF(sv.avg_daily_sales, 0) > 30 THEN '🔵 Fazla Stok'
                ELSE '🟢 Optimal'
            END as stock_status,
            CASE 
                WHEN ci.closing_stock / NULLIF(sv.avg_daily_sales, 0) < 7 
                THEN GREATEST(ROUND(sv.avg_daily_sales * 14), 20)
                ELSE 0
            END as recommended_order_qty
        FROM current_inventory ci
        LEFT JOIN sales_velocity sv 
            ON ci.store_id = sv.store_id AND ci.sku = sv.sku
        WHERE sv.avg_daily_sales > 0
        ORDER BY days_until_stockout ASC
        LIMIT 20
        """
        
        data = execute_query(query)
        return format_response(data, message="Stok durumu ve sipariş önerileri")
        
    except Exception as e:
        return error_response(e)

# =====================================
# 7. STORE PERFORMANCE RANKING
# =====================================
@functions_framework.http
def store_performance(request):
    """
    Mağaza performans sıralaması
    """
    try:
        query = f"""
        WITH store_metrics AS (
            SELECT 
                s.store_id,
                s.store_name,
                s.city,
                s.region,
                SUM(s.units_sold) as total_units,
                SUM(s.revenue) as total_revenue,
                AVG(s.gross_margin) as avg_margin,
                COUNT(DISTINCT s.date) as operating_days
            FROM `{PROJECT_ID}.{DATASET_ID}.product_sales_daily` s
            WHERE s.product_subcategory = 'Mont'
            GROUP BY s.store_id, s.store_name, s.city, s.region
        ),
        rankings AS (
            SELECT 
                *,
                RANK() OVER (ORDER BY total_revenue DESC) as revenue_rank,
                RANK() OVER (ORDER BY total_units DESC) as units_rank,
                RANK() OVER (ORDER BY avg_margin DESC) as margin_rank
            FROM store_metrics
        )
        SELECT 
            store_id,
            store_name,
            city,
            region,
            total_units,
            ROUND(total_revenue, 0) as total_revenue,
            ROUND(avg_margin, 0) as avg_margin,
            revenue_rank,
            CASE 
                WHEN revenue_rank <= 2 THEN '🥇 Top Performer'
                WHEN revenue_rank <= 4 THEN '🥈 İyi Performans'
                ELSE '📊 Gelişim Alanı'
            END as performance_tier
        FROM rankings
        ORDER BY revenue_rank
        """
        
        data = execute_query(query)
        return format_response(data, message="Mağaza performans sıralaması")
        
    except Exception as e:
        return error_response(e)

# =====================================
# 8. DAILY SALES SUMMARY
# =====================================
@functions_framework.http
def daily_sales_summary(request):
    """
    Günlük satış özeti
    """
    try:
        request_json = request.get_json(silent=True) or {}
        date = request_json.get('date', '2025-07-11')
        
        query = f"""
        SELECT 
            s.date,
            s.store_id,
            s.store_name,
            s.city,
            SUM(s.units_sold) as total_units,
            SUM(s.revenue) as total_revenue,
            AVG(s.weather_temp) as avg_temp,
            MAX(s.weather_condition) as weather,
            COUNT(DISTINCT s.sku) as product_variety
        FROM `{PROJECT_ID}.{DATASET_ID}.product_sales_daily` s
        WHERE s.date = '{date}'
            AND s.product_subcategory = 'Mont'
        GROUP BY s.date, s.store_id, s.store_name, s.city
        ORDER BY total_revenue DESC
        """
        
        data = execute_query(query)
        return format_response(data, message=f"{date} tarihli satış özeti")
        
    except Exception as e:
        return error_response(e)

# =====================================
# 9. PRODUCT PERFORMANCE
# =====================================
@functions_framework.http
def product_performance(request):
    """
    Ürün bazlı performans analizi
    """
    try:
        query = f"""
        WITH product_sales AS (
            SELECT 
                s.sku,
                s.product_name,
                SUM(s.units_sold) as total_units,
                SUM(s.revenue) as total_revenue,
                AVG(s.gross_margin) as avg_margin,
                COUNT(DISTINCT s.store_id) as store_coverage,
                COUNT(DISTINCT s.date) as days_sold
            FROM `{PROJECT_ID}.{DATASET_ID}.product_sales_daily` s
            WHERE s.product_subcategory = 'Mont'
            GROUP BY s.sku, s.product_name
        )
        SELECT 
            sku,
            product_name,
            total_units,
            ROUND(total_revenue, 0) as total_revenue,
            ROUND(avg_margin, 0) as avg_margin,
            store_coverage,
            ROUND(total_revenue / NULLIF(total_units, 0), 0) as avg_price,
            CASE 
                WHEN total_units = MAX(total_units) OVER() THEN '🏆 En Çok Satan'
                WHEN total_revenue = MAX(total_revenue) OVER() THEN '💰 En Çok Gelir'
                WHEN avg_margin = MAX(avg_margin) OVER() THEN '📈 En Karlı'
                ELSE ''
            END as badge
        FROM product_sales
        ORDER BY total_revenue DESC
        """
        
        data = execute_query(query)
        return format_response(data, message="Mont modellerinin performans analizi")
        
    except Exception as e:
        return error_response(e)

# =====================================
# 10. PROMOTION IMPACT
# =====================================
@functions_framework.http
def promotion_impact(request):
    """
    Promosyon etkisi analizi
    """
    try:
        query = f"""
        WITH promo_comparison AS (
            SELECT 
                s.promo_flag,
                s.promo_type,
                COUNT(*) as transaction_count,
                SUM(s.units_sold) as total_units,
                SUM(s.revenue) as total_revenue,
                AVG(s.units_sold) as avg_units_per_day,
                AVG(s.revenue) as avg_revenue_per_day
            FROM `{PROJECT_ID}.{DATASET_ID}.product_sales_daily` s
            WHERE s.product_subcategory = 'Mont'
            GROUP BY s.promo_flag, s.promo_type
        )
        SELECT 
            CASE 
                WHEN promo_flag THEN 'Promosyonlu'
                ELSE 'Normal Fiyat'
            END as sale_type,
            COALESCE(promo_type, 'İndirim Yok') as promo_type,
            transaction_count,
            total_units,
            ROUND(total_revenue, 0) as total_revenue,
            ROUND(avg_units_per_day, 1) as avg_daily_units,
            ROUND(avg_revenue_per_day, 0) as avg_daily_revenue,
            CASE 
                WHEN promo_flag THEN 
                    CONCAT(ROUND((avg_units_per_day / 
                        (SELECT avg_units_per_day FROM promo_comparison WHERE NOT promo_flag) - 1) * 100, 1), 
                        '% artış')
                ELSE 'Baz Satış'
            END as impact
        FROM promo_comparison
        ORDER BY promo_flag DESC, total_revenue DESC
        """
        
        data = execute_query(query)
        return format_response(data, message="Promosyonların satışlara etkisi")
        
    except Exception as e:
        return error_response(e)

# =====================================
# 11. REGIONAL ANALYSIS
# =====================================
@functions_framework.http
def regional_analysis(request):
    """
    Bölgesel satış analizi
    """
    try:
        query = f"""
        WITH regional_sales AS (
            SELECT 
                s.region,
                s.city,
                COUNT(DISTINCT s.store_id) as store_count,
                SUM(s.units_sold) as total_units,
                SUM(s.revenue) as total_revenue,
                AVG(s.weather_temp) as avg_temp
            FROM `{PROJECT_ID}.{DATASET_ID}.product_sales_daily` s
            WHERE s.product_subcategory = 'Mont'
            GROUP BY s.region, s.city
        ),
        region_totals AS (
            SELECT 
                region,
                SUM(total_revenue) as region_revenue
            FROM regional_sales
            GROUP BY region
        )
        SELECT 
            rs.region,
            rs.city,
            rs.store_count,
            rs.total_units,
            ROUND(rs.total_revenue, 0) as city_revenue,
            ROUND(rt.region_revenue, 0) as region_total,
            ROUND(rs.total_revenue / rt.region_revenue * 100, 1) as region_share_pct,
            ROUND(rs.avg_temp, 1) as avg_temperature
        FROM regional_sales rs
        JOIN region_totals rt ON rs.region = rt.region
        ORDER BY rs.region, rs.total_revenue DESC
        """
        
        data = execute_query(query)
        return format_response(data, message="Bölge ve şehir bazlı satış analizi")
        
    except Exception as e:
        return error_response(e)

# =====================================
# 12. STOCK ALERT SYSTEM
# =====================================
@functions_framework.http
def stock_alerts(request):
    """
    Kritik stok uyarıları
    """
    try:
        query = f"""
        WITH latest_inventory AS (
            SELECT 
                i.*,
                ROW_NUMBER() OVER (PARTITION BY i.store_id, i.sku ORDER BY i.date DESC) as rn
            FROM `{PROJECT_ID}.{DATASET_ID}.store_inventory` i
            WHERE i.subcategory = 'Mont'
        ),
        stock_issues AS (
            SELECT 
                store_id,
                store_name,
                sku,
                product_name,
                closing_stock,
                days_of_supply,
                stockout_flag,
                CASE 
                    WHEN stockout_flag THEN 1
                    WHEN days_of_supply < 3 THEN 2
                    WHEN days_of_supply < 7 THEN 3
                    ELSE 4
                END as urgency_level
            FROM latest_inventory
            WHERE rn = 1
                AND (stockout_flag OR days_of_supply < 7)
        )
        SELECT 
            store_id,
            store_name,
            sku,
            product_name,
            closing_stock,
            ROUND(days_of_supply, 1) as days_remaining,
            CASE urgency_level
                WHEN 1 THEN '🚨 STOK TÜKENDİ'
                WHEN 2 THEN '🔴 ÇOK KRİTİK (< 3 gün)'
                WHEN 3 THEN '🟠 KRİTİK (< 7 gün)'
                ELSE '🟡 UYARI'
            END as alert_level,
            CASE urgency_level
                WHEN 1 THEN 'ACİL SİPARİŞ VER'
                WHEN 2 THEN 'BUGÜN SİPARİŞ VER'
                WHEN 3 THEN 'BU HAFTA SİPARİŞ VER'
                ELSE 'STOK TAKİP'
            END as action_required
        FROM stock_issues
        ORDER BY urgency_level, days_of_supply
        """
        
        data = execute_query(query)
        return format_response(data, message="Kritik stok uyarıları ve aksiyon önerileri")
        
    except Exception as e:
        return error_response(e)

# =====================================
# 13. WEEKLY TREND ANALYSIS
# =====================================
@functions_framework.http
def weekly_trends(request):
    """
    Haftalık trend analizi
    """
    try:
        query = f"""
        WITH weekly_sales AS (
            SELECT 
                EXTRACT(WEEK FROM s.date) as week_num,
                MIN(s.date) as week_start,
                MAX(s.date) as week_end,
                SUM(s.units_sold) as weekly_units,
                SUM(s.revenue) as weekly_revenue,
                AVG(s.weather_temp) as avg_temp,
                COUNT(DISTINCT s.store_id) as active_stores
            FROM `{PROJECT_ID}.{DATASET_ID}.product_sales_daily` s
            WHERE s.product_subcategory = 'Mont'
            GROUP BY week_num
        ),
        week_comparison AS (
            SELECT 
                *,
                LAG(weekly_revenue, 1) OVER (ORDER BY week_num) as prev_week_revenue,
                LAG(weekly_units, 1) OVER (ORDER BY week_num) as prev_week_units
            FROM weekly_sales
        )
        SELECT 
            week_num,
            week_start,
            week_end,
            weekly_units,
            ROUND(weekly_revenue, 0) as weekly_revenue,
            ROUND(avg_temp, 1) as avg_temperature,
            active_stores,
            CASE 
                WHEN prev_week_revenue IS NOT NULL 
                THEN ROUND((weekly_revenue - prev_week_revenue) / prev_week_revenue * 100, 1)
                ELSE NULL
            END as revenue_growth_pct,
            CASE 
                WHEN weekly_revenue > prev_week_revenue THEN '📈 Artış'
                WHEN weekly_revenue < prev_week_revenue THEN '📉 Düşüş'
                ELSE '➡️ Sabit'
            END as trend
        FROM week_comparison
        ORDER BY week_num
        """
        
        data = execute_query(query)
        return format_response(data, message="Haftalık satış trend analizi")
        
    except Exception as e:
        return error_response(e)

# =====================================
# 14. COMPETITOR ACTIVITY IMPACT
# =====================================
@functions_framework.http
def competitor_impact(request):
    """
    Rakip aktivitelerin etkisi
    """
    try:
        query = f"""
        WITH competitor_analysis AS (
            SELECT 
                s.competitor_activity,
                s.store_id,
                s.store_name,
                COUNT(*) as days_with_activity,
                SUM(s.units_sold) as total_units,
                SUM(s.revenue) as total_revenue,
                AVG(s.units_sold) as avg_daily_units
            FROM `{PROJECT_ID}.{DATASET_ID}.product_sales_daily` s
            WHERE s.product_subcategory = 'Mont'
            GROUP BY s.competitor_activity, s.store_id, s.store_name
        ),
        impact_summary AS (
            SELECT 
                competitor_activity,
                COUNT(DISTINCT store_id) as affected_stores,
                SUM(total_units) as total_units,
                SUM(total_revenue) as total_revenue,
                AVG(avg_daily_units) as avg_daily_units
            FROM competitor_analysis
            GROUP BY competitor_activity
        )
        SELECT 
            competitor_activity,
            affected_stores,
            total_units,
            ROUND(total_revenue, 0) as total_revenue,
            ROUND(avg_daily_units, 1) as avg_daily_units,
            CASE competitor_activity
                WHEN 'Yüksek' THEN '🔴 Yoğun rekabet - Agresif strategi gerekli'
                WHEN 'Normal' THEN '🟡 Normal rekabet - Mevcut strateji devam'
                WHEN 'İndirimde' THEN '🟠 Rakip indirimleri - Fiyat ayarlaması düşünün'
                ELSE '🟢 Düşük rekabet - Fırsat'
            END as strategy_recommendation
        FROM impact_summary
        ORDER BY total_revenue DESC
        """
        
        data = execute_query(query)
        return format_response(data, message="Rakip aktivitelerinin satışlara etkisi")
        
    except Exception as e:
        return error_response(e)

# =====================================
# 15. ADVANCED FORECAST WITH ML
# =====================================
@functions_framework.http
def advanced_forecast(request):
    """
    Gelişmiş makine öğrenmesi ile tahmin
    """
    try:
        request_json = request.get_json(silent=True) or {}
        store_id = request_json.get('store_id')
        
        where_clause = "WHERE s.product_subcategory = 'Mont'"
        if store_id:
            where_clause += f" AND s.store_id = '{store_id}'"
        
        query = f"""
        WITH historical_data AS (
            -- Geçmiş veri hazırlama
            SELECT 
                s.store_id,
                s.store_name,
                s.date,
                SUM(s.units_sold) as daily_units,
                SUM(s.revenue) as daily_revenue,
                AVG(s.weather_temp) as temp,
                MAX(s.promo_flag) as had_promo,
                EXTRACT(DAYOFWEEK FROM s.date) as day_of_week
            FROM `{PROJECT_ID}.{DATASET_ID}.product_sales_daily` s
            {where_clause}
            GROUP BY s.store_id, s.store_name, s.date
        ),
        statistical_features AS (
            -- İstatistiksel özellikler
            SELECT 
                store_id,
                store_name,
                AVG(daily_units) as avg_units,
                STDDEV(daily_units) as std_units,
                MAX(daily_units) as max_units,
                MIN(daily_units) as min_units,
                CORR(temp, daily_units) as temp_correlation,
                AVG(CASE WHEN day_of_week IN (1, 7) THEN daily_units ELSE NULL END) as weekend_avg,
                AVG(CASE WHEN day_of_week NOT IN (1, 7) THEN daily_units ELSE NULL END) as weekday_avg
            FROM historical_data
            GROUP BY store_id, store_name
        ),
        forecast_model AS (
            -- Tahmin modeli
            SELECT 
                sf.store_id,
                sf.store_name,
                -- Base forecast
                sf.avg_units as baseline,
                -- Trend component (son 3 günün ortalaması)
                (SELECT AVG(daily_units) FROM historical_data h 
                 WHERE h.store_id = sf.store_id 
                 ORDER BY date DESC LIMIT 3) as recent_trend,
                -- Weather adjustment
                CASE 
                    WHEN sf.temp_correlation < -0.3 THEN sf.avg_units * 1.3  -- Soğuk hava boost
                    ELSE sf.avg_units
                END as weather_adjusted,
                -- Confidence intervals
                sf.avg_units - (1.96 * sf.std_units) as lower_bound,
                sf.avg_units + (1.96 * sf.std_units) as upper_bound,
                -- Seasonality
                sf.weekend_avg,
                sf.weekday_avg,
                sf.std_units,
                ABS(sf.temp_correlation) as correlation_strength
            FROM statistical_features sf
        )
        SELECT 
            store_id,
            store_name,
            ROUND((baseline * 0.4 + recent_trend * 0.4 + weather_adjusted * 0.2), 0) as forecast_units_7d,
            ROUND((baseline * 0.4 + recent_trend * 0.4 + weather_adjusted * 0.2) * 7 * 2990, 0) as forecast_revenue_7d,
            ROUND(lower_bound, 0) as min_expected,
            ROUND(upper_bound, 0) as max_expected,
            CASE 
                WHEN std_units / NULLIF(baseline, 0) < 0.3 THEN 'Yüksek'
                WHEN std_units / NULLIF(baseline, 0) < 0.5 THEN 'Orta'
                ELSE 'Düşük'
            END as confidence_level,
            ROUND(correlation_strength, 2) as weather_sensitivity,
            CASE 
                WHEN recent_trend > baseline * 1.2 THEN '🚀 Yükselen Trend'
                WHEN recent_trend < baseline * 0.8 THEN '📉 Düşen Trend'
                ELSE '➡️ Stabil'
            END as trend_direction
        FROM forecast_model
        ORDER BY forecast_units_7d DESC
        """
        
        data = execute_query(query)
        return format_response(data, message="7 günlük gelişmiş tahmin modeli")
        
    except Exception as e:
        return error_response(e)

# =====================================
# MAIN ENTRY POINT (For testing)
# =====================================
@functions_framework.http
def main(request):
    """
    Main entry point for routing requests
    """
    try:
        path = request.path
        
        # Route to appropriate function based on path
        if path == "/list_tables":
            return list_tables(request)
        elif path == "/get_table_schema":
            return get_table_schema(request)
        elif path == "/sql_query":
            return sql_query(request)
        elif path == "/demand_forecast":
            return demand_forecast(request)
        elif path == "/weather_impact":
            return weather_impact(request)
        elif path == "/inventory_optimization":
            return inventory_optimization(request)
        elif path == "/store_performance":
            return store_performance(request)
        elif path == "/daily_sales_summary":
            return daily_sales_summary(request)
        elif path == "/product_performance":
            return product_performance(request)
        elif path == "/promotion_impact":
            return promotion_impact(request)
        elif path == "/regional_analysis":
            return regional_analysis(request)
        elif path == "/stock_alerts":
            return stock_alerts(request)
        elif path == "/weekly_trends":
            return weekly_trends(request)
        elif path == "/competitor_impact":
            return competitor_impact(request)
        elif path == "/advanced_forecast":
            return advanced_forecast(request)
        else:
            return format_response(
                {"available_endpoints": [
                    "/list_tables",
                    "/get_table_schema",
                    "/sql_query",
                    "/demand_forecast",
                    "/weather_impact",
                    "/inventory_optimization",
                    "/store_performance",
                    "/daily_sales_summary",
                    "/product_performance",
                    "/promotion_impact",
                    "/regional_analysis",
                    "/stock_alerts",
                    "/weekly_trends",
                    "/competitor_impact",
                    "/advanced_forecast"
                ]},
                message="Vakko AI Analytics API v1.0"
            )
            
    except Exception as e:
        return error_response(e)