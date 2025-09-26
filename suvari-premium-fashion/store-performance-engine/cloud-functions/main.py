# main.py - Suvari Store Performance Analytics
# Store Performance Agent - Dedicated Cloud Function
# Version: 1.0

import os
import json
import logging
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
import statistics
import functions_framework
from google.cloud import bigquery

# =======================
# Configuration
# =======================
PROJECT_ID = "agentspace-ngc"
BQ_LOCATION = "europe-west1"
DATASET = "suvari_store_performance"

# Table References
TABLES = {
   "daily_sales": f"`{PROJECT_ID}.{DATASET}.daily_sales`",
   "traffic_data": f"`{PROJECT_ID}.{DATASET}.traffic_data`",
   "staff_shifts": f"`{PROJECT_ID}.{DATASET}.staff_shifts`",
   "targets_kpi": f"`{PROJECT_ID}.{DATASET}.targets_kpi`"
}

# Business Constants
@dataclass
class SuvariMetrics:
   # Store codes
   STORE_PREFIX = "SV"  # SV001-SV010
   
   # Geographic
   COUNTRIES = ["Turkey", "Germany", "Russia", "Kazakhstan", "Ukraine", "Romania"]
   
   # Performance thresholds
   CONVERSION_EXCELLENT = 28.0  # %28+
   CONVERSION_GOOD = 23.0       # %23-28
   CONVERSION_POOR = 20.0       # <%20
   
   # Staff productivity
   SALES_PER_HOUR_TARGET = 3000  # TL/hour
   TRAINING_SCORE_MIN = 8.5
   
   # KPI thresholds
   KPI_ACHIEVEMENT_EXCELLENT = 105
   KPI_ACHIEVEMENT_GOOD = 95
   KPI_ACHIEVEMENT_POOR = 85

logger = logging.getLogger("suvari-store-performance")
logging.basicConfig(level=logging.INFO)

# =======================
# Query Type Detection
# =======================
def detect_query_type(question: str) -> str:
   """Detect query intent from natural language"""
   
   q = (question or "").lower()
   
   # Patterns for each analysis type
   patterns = {
       "daily_performance": ["bugün", "dün", "today", "yesterday", "günlük", "daily", "performans"],
       "store_comparison": ["karşılaştır", "compare", "mağaza", "store", "vs", "fark"],
       "staff_analysis": ["personel", "çalışan", "staff", "employee", "vardiya", "shift", "satış danışmanı"],
       "traffic_conversion": ["trafik", "ziyaretçi", "conversion", "dönüşüm", "footfall", "müşteri"],
       "kpi_tracking": ["hedef", "kpi", "target", "başarı", "achievement", "fiscal"],
       "trend_analysis": ["trend", "değişim", "change", "artış", "azalış", "growth", "decline"],
       "regional_performance": ["bölge", "ülke", "region", "country", "city", "şehir"],
       "franchise_analysis": ["franchise", "owned", "bayi", "sahiplik"],
       "hourly_patterns": ["saat", "hour", "peak", "yoğun", "pattern", "desen"],
       "weekend_analysis": ["hafta sonu", "weekend", "cumartesi", "pazar", "saturday", "sunday"]
   }
   
   for qtype, keywords in patterns.items():
       if any(k in q for k in keywords):
           logger.info(f"Detected query type: {qtype}")
           return qtype
   
   return "daily_performance"  # Default

# =======================
# Core SQL Queries
# =======================

def sql_daily_performance(store_id: Optional[str] = None, date_range: int = 7) -> Tuple[str, List]:
   """Daily store performance with all metrics"""
   
   sql = f"""
    WITH daily_metrics AS (
        SELECT 
            ds.date,
            ds.store_id,
            ds.country,
            ds.city,
            ds.mall_name,
            ds.franchise_flag,
            -- Sales metrics
            ds.total_revenue,
            ds.net_revenue,
            ds.transactions,
            ds.aov,
            ds.suits_qty,
            ds.shirts_qty,
            ds.accessories_qty,
            -- Traffic metrics
            td.store_entries,
            td.store_exits,
            td.conversion_rate,
            td.avg_dwell_time_min,
            td.peak_hour,
            td.weather_impact,
            td.competitor_activity,
            -- Staff metrics
            ds.staff_count,
            ds.opening_hours,
            ROUND(ds.net_revenue / NULLIF(ds.staff_count * ds.opening_hours, 0), 2) as revenue_per_staff_hour,
            -- Calculated metrics
            ds.suits_qty + ds.shirts_qty + ds.accessories_qty as total_items,
            ROUND(ds.suits_qty * 100.0 / NULLIF(ds.suits_qty + ds.shirts_qty + ds.accessories_qty, 0), 1) as suit_mix_pct,
            ROUND(td.store_entries / NULLIF(ds.opening_hours, 0), 1) as visitors_per_hour
        FROM {TABLES['daily_sales']} ds
        LEFT JOIN {TABLES['traffic_data']} td 
            ON ds.store_id = td.store_id AND ds.date = td.date
        WHERE ds.date BETWEEN DATE('2024-12-09') AND DATE('2024-12-15')  -- SABİT TARİH ARALIĞI
            AND (@store_id IS NULL OR ds.store_id = @store_id)
    ),
   performance_ranking AS (
       SELECT 
           *,
           RANK() OVER (PARTITION BY date ORDER BY net_revenue DESC) as daily_revenue_rank,
           RANK() OVER (PARTITION BY date ORDER BY conversion_rate DESC) as daily_conversion_rank,
           AVG(net_revenue) OVER (PARTITION BY store_id ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as ma7_revenue,
           LAG(net_revenue, 1) OVER (PARTITION BY store_id ORDER BY date) as prev_day_revenue,
           LAG(net_revenue, 7) OVER (PARTITION BY store_id ORDER BY date) as same_day_last_week
       FROM daily_metrics
   )
   SELECT 
       FORMAT_DATE('%Y-%m-%d', date) as date,
       store_id,
       country,
       city,
       mall_name,
       franchise_flag,
       -- Revenue metrics
       ROUND(total_revenue, 2) as total_revenue,
       ROUND(net_revenue, 2) as net_revenue,
       transactions,
       ROUND(aov, 2) as avg_basket_value,
       -- Product mix
       suits_qty,
       shirts_qty,
       accessories_qty,
       total_items,
       suit_mix_pct as suit_percentage,
       -- Traffic & conversion
       store_entries,
       ROUND(conversion_rate, 1) as conversion_rate,
       ROUND(avg_dwell_time_min, 1) as avg_dwell_time,
       peak_hour,
       visitors_per_hour,
       -- Staff productivity
       staff_count,
       opening_hours,
       revenue_per_staff_hour,
       -- Rankings
       daily_revenue_rank,
       daily_conversion_rank,
       -- Trends
       ROUND(ma7_revenue, 2) as moving_avg_7d,
       ROUND(100.0 * (net_revenue - prev_day_revenue) / NULLIF(prev_day_revenue, 0), 1) as day_over_day_change,
       ROUND(100.0 * (net_revenue - same_day_last_week) / NULLIF(same_day_last_week, 0), 1) as week_over_week_change,
       -- Context
       weather_impact,
       competitor_activity,
       -- Performance indicators
       CASE 
           WHEN conversion_rate >= {SuvariMetrics.CONVERSION_EXCELLENT} THEN '🟢 Excellent'
           WHEN conversion_rate >= {SuvariMetrics.CONVERSION_GOOD} THEN '🟡 Good'
           WHEN conversion_rate >= {SuvariMetrics.CONVERSION_POOR} THEN '🟠 Fair'
           ELSE '🔴 Poor'
       END as conversion_status,
       CASE 
           WHEN revenue_per_staff_hour >= {SuvariMetrics.SALES_PER_HOUR_TARGET} THEN '⭐ Top Performer'
           WHEN revenue_per_staff_hour >= {SuvariMetrics.SALES_PER_HOUR_TARGET} * 0.8 THEN '✅ On Track'
           ELSE '⚠️ Below Target'
       END as productivity_status,
       CASE
           WHEN net_revenue > ma7_revenue * 1.2 THEN '📈 Spike Day'
           WHEN net_revenue < ma7_revenue * 0.8 THEN '📉 Low Day'
           ELSE '➡️ Normal'
       END as performance_flag
   FROM performance_ranking
   ORDER BY date DESC, net_revenue DESC
   LIMIT @limit
   """
   
   params = [
       bigquery.ScalarQueryParameter("store_id", "STRING", store_id),
       bigquery.ScalarQueryParameter("date_range", "INT64", date_range),
       bigquery.ScalarQueryParameter("limit", "INT64", None)
   ]
   
   return sql, params

def sql_store_comparison() -> Tuple[str, List]:
   """Compare stores with comprehensive metrics"""
   
   sql = f"""
   WITH store_summary AS (
       SELECT 
           ds.store_id,
           ds.country,
           ds.city,
           ds.franchise_flag,
           COUNT(DISTINCT ds.date) as operating_days,
           -- Revenue metrics
           SUM(ds.net_revenue) as total_revenue,
           AVG(ds.net_revenue) as avg_daily_revenue,
           STDDEV(ds.net_revenue) as revenue_volatility,
           -- Sales metrics
           SUM(ds.transactions) as total_transactions,
           AVG(ds.aov) as avg_basket_value,
           SUM(ds.suits_qty) as suits_sold,
           SUM(ds.shirts_qty) as shirts_sold,
           SUM(ds.accessories_qty) as accessories_sold,
           -- Efficiency
           AVG(ds.staff_count) as avg_staff,
           SUM(ds.net_revenue) / NULLIF(SUM(ds.staff_count * ds.opening_hours), 0) as revenue_per_staff_hour
       FROM {TABLES['daily_sales']} ds
       WHERE ds.date BETWEEN DATE('2024-12-09') AND DATE('2024-12-15')
       GROUP BY ds.store_id, ds.country, ds.city, ds.franchise_flag
   ),
   traffic_summary AS (
       SELECT 
           td.store_id,
           AVG(td.conversion_rate) as avg_conversion_rate,
           SUM(td.store_entries) as total_visitors,
           AVG(td.avg_dwell_time_min) as avg_dwell_time,
           APPROX_TOP_COUNT(td.peak_hour, 1)[OFFSET(0)].value as most_common_peak_hour
       FROM {TABLES['traffic_data']} td
       WHERE td.date BETWEEN DATE('2024-12-09') AND DATE('2024-12-15')
       GROUP BY td.store_id
   ),
   kpi_summary AS (
       SELECT 
           tk.store_id,
           AVG(tk.achievement_pct) as avg_kpi_achievement,
           SUM(CASE WHEN tk.achievement_pct >= 100 THEN 1 ELSE 0 END) as kpis_achieved,
           COUNT(*) as total_kpis
       FROM {TABLES['targets_kpi']} tk
       WHERE tk.fiscal_week IN ('2024-W49', '2024-W50')
       GROUP BY tk.store_id
   ),
   rankings AS (
       SELECT 
           ss.*,
           ts.avg_conversion_rate,
           ts.total_visitors,
           ts.avg_dwell_time,
           ts.most_common_peak_hour,
           ks.avg_kpi_achievement,
           ks.kpis_achieved,
           ks.total_kpis,
           -- Rankings
           RANK() OVER (ORDER BY ss.total_revenue DESC) as revenue_rank,
           RANK() OVER (ORDER BY ts.avg_conversion_rate DESC) as conversion_rank,
           RANK() OVER (ORDER BY ss.revenue_per_staff_hour DESC) as productivity_rank,
           RANK() OVER (ORDER BY ks.avg_kpi_achievement DESC) as kpi_rank
       FROM store_summary ss
       LEFT JOIN traffic_summary ts ON ss.store_id = ts.store_id
       LEFT JOIN kpi_summary ks ON ss.store_id = ks.store_id
   )
   SELECT 
       store_id,
       country,
       city,
       franchise_flag,
       -- Performance metrics
       ROUND(total_revenue, 2) as total_revenue_30d,
       ROUND(avg_daily_revenue, 2) as avg_daily_revenue,
       ROUND(revenue_volatility, 2) as revenue_stability,
       total_transactions,
       ROUND(avg_basket_value, 2) as avg_basket_value,
       -- Product performance
       suits_sold,
       shirts_sold,
       accessories_sold,
       ROUND(suits_sold * 100.0 / NULLIF(suits_sold + shirts_sold + accessories_sold, 0), 1) as suit_mix_pct,
       -- Traffic & conversion
       ROUND(avg_conversion_rate, 1) as conversion_rate,
       total_visitors,
       ROUND(avg_dwell_time, 1) as avg_dwell_time_min,
       most_common_peak_hour,
       -- Efficiency
       ROUND(avg_staff, 1) as avg_staff_count,
       ROUND(revenue_per_staff_hour, 2) as revenue_per_staff_hour,
       -- KPI Achievement
       ROUND(avg_kpi_achievement, 1) as kpi_achievement_pct,
       CONCAT(kpis_achieved, '/', total_kpis) as kpis_achieved_ratio,
       -- Rankings
       revenue_rank,
       conversion_rank,
       productivity_rank,
       kpi_rank,
       -- Overall assessment
       CASE 
           WHEN revenue_rank <= 3 AND conversion_rank <= 3 THEN '⭐ Star Store'
           WHEN revenue_rank <= 5 OR conversion_rank <= 5 THEN '🏆 High Performer'
           WHEN productivity_rank <= 3 THEN '💪 Efficient Store'
           WHEN kpi_rank <= 3 THEN '🎯 Target Achiever'
           WHEN revenue_rank > 7 THEN '⚠️ Needs Attention'
           ELSE '📊 Average Performer'
       END as store_category,
       CASE 
           WHEN franchise_flag = 'owned' THEN 'Company Owned'
           ELSE 'Franchise'
       END as ownership_type
   FROM rankings
   ORDER BY revenue_rank
   """
   
   params = []
   return sql, params

def sql_staff_performance() -> Tuple[str, List]:
   """Staff productivity and performance analysis"""
   
   sql = f"""
   WITH staff_metrics AS (
       SELECT 
           ss.shift_date,
           ss.store_id,
           ss.employee_id,
           ss.role,
           ss.actual_hours,
           ss.suits_sold,
           ss.shirts_sold,
           ss.accessories_sold,
           ss.total_sales_attributed,
           ss.customer_interactions,
           ss.alterations_booked,
           ss.training_score,
           -- Calculated metrics
           ss.suits_sold + ss.shirts_sold + ss.accessories_sold as total_items_sold,
           ROUND(ss.total_sales_attributed / NULLIF(ss.actual_hours, 0), 2) as sales_per_hour,
           ROUND(ss.total_sales_attributed / NULLIF(ss.customer_interactions, 0), 2) as avg_transaction_value,
           ROUND(ss.customer_interactions / NULLIF(ss.actual_hours, 0), 1) as customers_per_hour
       FROM {TABLES['staff_shifts']} ss
       WHERE ss.shift_date BETWEEN DATE('2024-12-14') AND DATE('2024-12-15')
   ),
   employee_summary AS (
       SELECT 
           employee_id,
           store_id,
           APPROX_TOP_COUNT(role, 1)[OFFSET(0)].value as primary_role,
           COUNT(*) as shifts_worked,
           SUM(actual_hours) as total_hours,
           -- Sales metrics
           SUM(suits_sold) as total_suits,
           SUM(shirts_sold) as total_shirts,
           SUM(accessories_sold) as total_accessories,
           SUM(total_items_sold) as total_items,
           SUM(total_sales_attributed) as total_sales,
           -- Productivity metrics
           AVG(sales_per_hour) as avg_sales_per_hour,
           AVG(avg_transaction_value) as avg_transaction_value,
           AVG(customers_per_hour) as avg_customers_per_hour,
           SUM(customer_interactions) as total_customers,
           SUM(alterations_booked) as total_alterations,
           -- Performance
           AVG(training_score) as avg_training_score,
           MAX(training_score) as best_training_score
       FROM staff_metrics
       GROUP BY employee_id, store_id
   ),
   role_benchmarks AS (
       SELECT 
           primary_role,
           AVG(avg_sales_per_hour) as role_avg_sales_per_hour,
           APPROX_QUANTILES(avg_sales_per_hour, 100)[OFFSET(75)] as role_p75_sales_per_hour,
           APPROX_QUANTILES(avg_sales_per_hour, 100)[OFFSET(90)] as role_p90_sales_per_hour
       FROM employee_summary
       GROUP BY primary_role
   )
   SELECT 
       es.employee_id,
       es.store_id,
       es.primary_role,
       es.shifts_worked,
       ROUND(es.total_hours, 1) as total_hours_worked,
       -- Sales performance
       es.total_suits as suits_sold,
       es.total_shirts as shirts_sold,
       es.total_accessories as accessories_sold,
       es.total_items as total_items_sold,
       ROUND(es.total_sales, 2) as total_sales_value,
       -- Productivity
       ROUND(es.avg_sales_per_hour, 2) as avg_sales_per_hour,
       ROUND(es.avg_transaction_value, 2) as avg_transaction_value,
       ROUND(es.avg_customers_per_hour, 1) as customers_per_hour,
       es.total_customers as total_customers_served,
       es.total_alterations as alterations_booked,
       -- Training & development
       ROUND(es.avg_training_score, 1) as avg_training_score,
       ROUND(es.best_training_score, 1) as best_training_score,
       -- Benchmarking
       ROUND(rb.role_avg_sales_per_hour, 2) as role_average,
       ROUND(100.0 * es.avg_sales_per_hour / NULLIF(rb.role_avg_sales_per_hour, 0), 1) as performance_vs_average,
       -- Rankings
       RANK() OVER (PARTITION BY es.primary_role ORDER BY es.avg_sales_per_hour DESC) as role_rank,
       RANK() OVER (ORDER BY es.avg_sales_per_hour DESC) as overall_rank,
       -- Performance categories
       CASE 
           WHEN es.avg_sales_per_hour >= rb.role_p90_sales_per_hour THEN '⭐ Top 10%'
           WHEN es.avg_sales_per_hour >= rb.role_p75_sales_per_hour THEN '🏆 Top 25%'
           WHEN es.avg_sales_per_hour >= rb.role_avg_sales_per_hour THEN '✅ Above Average'
           ELSE '📈 Development Needed'
       END as performance_tier,
       CASE 
           WHEN es.avg_training_score >= {SuvariMetrics.TRAINING_SCORE_MIN} THEN '🎓 Certified'
           ELSE '📚 Training Required'
       END as training_status,
       CASE
           WHEN es.total_suits > es.total_shirts THEN '🤵 Suit Specialist'
           WHEN es.total_alterations > 10 THEN '✂️ Alteration Expert'
           WHEN es.avg_customers_per_hour > 5 THEN '👥 Customer Champion'
           ELSE '📊 Balanced Performer'
       END as specialty
   FROM employee_summary es
   LEFT JOIN role_benchmarks rb ON es.primary_role = rb.primary_role
   ORDER BY es.avg_sales_per_hour DESC
   LIMIT @limit
   """
   
   params = [
       bigquery.ScalarQueryParameter("limit", "INT64", None)
   ]
   
   return sql, params

def sql_traffic_conversion() -> Tuple[str, List]:
   """Traffic patterns and conversion analysis"""
   
   sql = f"""
   WITH traffic_patterns AS (
       SELECT 
           td.date,
           td.store_id,
           EXTRACT(DAYOFWEEK FROM td.date) as day_of_week,
           CASE 
               WHEN EXTRACT(DAYOFWEEK FROM td.date) IN (1, 7) THEN 'Weekend'
               ELSE 'Weekday'
           END as day_type,
           td.mall_traffic,
           td.store_entries,
           td.store_exits,
           td.conversion_rate,
           td.avg_dwell_time_min,
           td.peak_hour,
           td.weather_impact,
           td.nearby_events,
           td.competitor_activity,
           -- Calculated metrics
           ROUND(100.0 * td.store_entries / NULLIF(td.mall_traffic, 0), 2) as capture_rate,
           td.store_entries - td.store_exits as net_visitors,
           CASE 
               WHEN td.weather_impact IN ('rain', 'snow', 'cold') THEN 'Adverse'
               WHEN td.weather_impact IN ('sunny', 'warm', 'mild') THEN 'Favorable'
               ELSE 'Neutral'
           END as weather_category
       FROM {TABLES['traffic_data']} td
       WHERE td.date BETWEEN DATE('2024-12-09') AND DATE('2024-12-15')
   ),
   hourly_patterns AS (
       SELECT 
           store_id,
           peak_hour,
           COUNT(*) as frequency,
           AVG(conversion_rate) as avg_conversion_at_peak,
           AVG(store_entries) as avg_entries_at_peak
       FROM traffic_patterns
       GROUP BY store_id, peak_hour
   ),
   best_peak_hours AS (
       SELECT 
           store_id,
           ARRAY_AGG(
               STRUCT(peak_hour, frequency)
               ORDER BY frequency DESC
               LIMIT 3
           ) as top_peak_hours
       FROM hourly_patterns
       GROUP BY store_id
   ),
   conversion_analysis AS (
       SELECT 
           tp.store_id,
           -- Overall metrics
           AVG(tp.conversion_rate) as avg_conversion_rate,
           STDDEV(tp.conversion_rate) as conversion_volatility,
           MIN(tp.conversion_rate) as min_conversion,
           MAX(tp.conversion_rate) as max_conversion,
           -- By day type
           AVG(CASE WHEN tp.day_type = 'Weekend' THEN tp.conversion_rate END) as weekend_conversion,
           AVG(CASE WHEN tp.day_type = 'Weekday' THEN tp.conversion_rate END) as weekday_conversion,
           -- By weather
           AVG(CASE WHEN tp.weather_category = 'Favorable' THEN tp.conversion_rate END) as favorable_weather_conversion,
           AVG(CASE WHEN tp.weather_category = 'Adverse' THEN tp.conversion_rate END) as adverse_weather_conversion,
           -- Traffic metrics
           SUM(tp.store_entries) as total_visitors,
           AVG(tp.store_entries) as avg_daily_visitors,
           AVG(tp.capture_rate) as avg_capture_rate,
           AVG(tp.avg_dwell_time_min) as avg_dwell_time,
           -- Context
           COUNT(DISTINCT CASE WHEN tp.competitor_activity != 'none' THEN tp.date END) as days_with_competition,
           COUNT(DISTINCT CASE WHEN tp.nearby_events != 'none' THEN tp.date END) as days_with_events
       FROM traffic_patterns tp
       GROUP BY tp.store_id
   )
   SELECT 
       ca.store_id,
       -- Conversion metrics
       ROUND(ca.avg_conversion_rate, 1) as avg_conversion_rate,
       ROUND(ca.conversion_volatility, 1) as conversion_stability,
       ROUND(ca.min_conversion, 1) as worst_conversion,
       ROUND(ca.max_conversion, 1) as best_conversion,
       ROUND(ca.max_conversion - ca.min_conversion, 1) as conversion_range,
       -- Pattern analysis
       ROUND(ca.weekend_conversion, 1) as weekend_conversion_rate,
       ROUND(ca.weekday_conversion, 1) as weekday_conversion_rate,
       ROUND(ca.weekend_conversion - ca.weekday_conversion, 1) as weekend_lift,
       -- Weather impact
       ROUND(ca.favorable_weather_conversion, 1) as good_weather_conversion,
       ROUND(ca.adverse_weather_conversion, 1) as bad_weather_conversion,
       ROUND(ca.favorable_weather_conversion - ca.adverse_weather_conversion, 1) as weather_impact,
       -- Traffic metrics
       ca.total_visitors as total_visitors_30d,
       ROUND(ca.avg_daily_visitors, 0) as avg_daily_visitors,
       ROUND(ca.avg_capture_rate, 1) as mall_capture_rate,
       ROUND(ca.avg_dwell_time, 1) as avg_dwell_time_min,
       -- Context
       ca.days_with_competition,
       ca.days_with_events,
       -- Performance indicators
       CASE 
           WHEN ca.avg_conversion_rate >= {SuvariMetrics.CONVERSION_EXCELLENT} THEN '🏆 Excellent Converter'
           WHEN ca.avg_conversion_rate >= {SuvariMetrics.CONVERSION_GOOD} THEN '✅ Good Converter'
           WHEN ca.avg_conversion_rate >= {SuvariMetrics.CONVERSION_POOR} THEN '📊 Average Converter'
           ELSE '⚠️ Poor Converter'
       END as conversion_tier,
       CASE 
           WHEN ca.weekend_conversion > ca.weekday_conversion * 1.2 THEN '🎯 Weekend Performer'
           WHEN ca.weekday_conversion > ca.weekend_conversion * 1.2 THEN '💼 Weekday Performer'
           ELSE '⚖️ Balanced Performance'
       END as performance_pattern,
       CASE
           WHEN ca.avg_capture_rate > 5 THEN '🧲 High Mall Capture'
           WHEN ca.avg_capture_rate > 3 THEN '📍 Good Location'
           ELSE '📌 Low Visibility'
       END as location_effectiveness
   FROM conversion_analysis ca
   ORDER BY ca.avg_conversion_rate DESC
   """
   
   params = []
   return sql, params

def sql_kpi_achievement() -> Tuple[str, List]:
   """KPI achievement and target analysis"""
   
   sql = f"""
   WITH kpi_performance AS (
       SELECT 
           tk.fiscal_week,
           tk.store_id,
           tk.kpi_name,
           tk.target_value,
           tk.actual_value,
           tk.achievement_pct,
           tk.threshold_warn,
           tk.threshold_crit,
           -- Status calculation
           CASE 
               WHEN tk.achievement_pct >= 100 THEN 'Achieved'
               WHEN tk.achievement_pct >= tk.threshold_warn * 100 THEN 'Warning'
               WHEN tk.achievement_pct >= tk.threshold_crit * 100 THEN 'Critical'
               ELSE 'Failed'
           END as kpi_status,
           -- Gap analysis
           tk.actual_value - tk.target_value as absolute_gap,
           (100 - tk.achievement_pct) as percentage_gap
       FROM {TABLES['targets_kpi']} tk
   ),
   store_kpi_summary AS (
       SELECT 
           store_id,
           fiscal_week,
           COUNT(*) as total_kpis,
           SUM(CASE WHEN kpi_status = 'Achieved' THEN 1 ELSE 0 END) as kpis_achieved,
           SUM(CASE WHEN kpi_status = 'Warning' THEN 1 ELSE 0 END) as kpis_warning,
           SUM(CASE WHEN kpi_status = 'Critical' THEN 1 ELSE 0 END) as kpis_critical,
           AVG(achievement_pct) as avg_achievement,
           -- By KPI type
           AVG(CASE WHEN kpi_name = 'Revenue' THEN achievement_pct END) as revenue_achievement,
           AVG(CASE WHEN kpi_name = 'Conversion Rate' THEN achievement_pct END) as conversion_achievement,
           AVG(CASE WHEN kpi_name = 'AOV' THEN achievement_pct END) as aov_achievement,
           AVG(CASE WHEN kpi_name = 'Suits Sold' THEN achievement_pct END) as suits_achievement
       FROM kpi_performance
       GROUP BY store_id, fiscal_week
   ),
   week_comparison AS (
       SELECT 
           s1.store_id,
           s1.fiscal_week as current_week,
           s1.avg_achievement as current_achievement,
           s2.avg_achievement as previous_achievement,
           s1.avg_achievement - s2.avg_achievement as week_over_week_change
       FROM store_kpi_summary s1
       LEFT JOIN store_kpi_summary s2 
           ON s1.store_id = s2.store_id 
           AND s2.fiscal_week = '2024-W49'
       WHERE s1.fiscal_week = '2024-W50'
   )
   SELECT 
       wc.store_id,
       wc.current_week,
       -- Overall performance
       ROUND(wc.current_achievement, 1) as overall_achievement_pct,
       CONCAT(sks.kpis_achieved, '/', sks.total_kpis) as kpis_achieved_ratio,
       sks.kpis_warning as warning_kpis,
       sks.kpis_critical as critical_kpis,
       -- Trend
       ROUND(wc.previous_achievement, 1) as previous_week_achievement,
       ROUND(wc.week_over_week_change, 1) as wow_change,
       -- Individual KPIs
       ROUND(sks.revenue_achievement, 1) as revenue_achievement_pct,
       ROUND(sks.conversion_achievement, 1) as conversion_achievement_pct,
       ROUND(sks.aov_achievement, 1) as aov_achievement_pct,
       ROUND(sks.suits_achievement, 1) as suits_achievement_pct,
       -- Rankings
       RANK() OVER (ORDER BY wc.current_achievement DESC) as achievement_rank,
       RANK() OVER (ORDER BY sks.kpis_achieved DESC, wc.current_achievement DESC) as success_rank,
       -- Performance classification
       CASE 
           WHEN wc.current_achievement >= {SuvariMetrics.KPI_ACHIEVEMENT_EXCELLENT} THEN '⭐ Exceeding Targets'
           WHEN wc.current_achievement >= {SuvariMetrics.KPI_ACHIEVEMENT_GOOD} THEN '✅ Meeting Targets'
           WHEN wc.current_achievement >= {SuvariMetrics.KPI_ACHIEVEMENT_POOR} THEN '⚠️ Below Targets'
           ELSE '🔴 Missing Targets'
       END as performance_level,
       CASE 
           WHEN wc.week_over_week_change > 5 THEN '📈 Improving'
           WHEN wc.week_over_week_change < -5 THEN '📉 Declining'
           ELSE '➡️ Stable'
       END as trend_direction,
       CASE
           WHEN sks.revenue_achievement < 90 THEN 'Focus on revenue generation'
           WHEN sks.conversion_achievement < 90 THEN 'Improve conversion tactics'
           WHEN sks.aov_achievement < 90 THEN 'Increase basket value'
           WHEN sks.suits_achievement < 90 THEN 'Push formal wear sales'
           ELSE 'Maintain performance'
       END as primary_focus_area
   FROM week_comparison wc
   LEFT JOIN store_kpi_summary sks 
       ON wc.store_id = sks.store_id 
       AND sks.fiscal_week = wc.current_week
   ORDER BY wc.current_achievement DESC
   """
   
   params = []
   return sql, params

# =======================
# Business Logic Layer
# =======================

def analyze_results(rows: List[Dict], query_type: str) -> Dict[str, Any]:
   """Generate insights from query results"""
   
   insights = []
   recommendations = []
   alerts = []
   
   if not rows:
       return {
           "insights": ["Belirtilen kriterlere uygun veri bulunamadı"],
           "recommendations": ["Tarih aralığını genişletmeyi deneyin"],
           "alerts": []
       }
   
   try:
       if query_type == "daily_performance":
           # Top performers
           top_stores = [r for r in rows if r.get('daily_revenue_rank') == 1]
           if top_stores:
               insights.append(f"🏆 Günün lideri: {top_stores[0]['store_id']} - {top_stores[0]['net_revenue']:,.0f} TL")
           
           # Conversion excellence
           excellent_conversion = [r for r in rows if '🟢 Excellent' in str(r.get('conversion_status', ''))]
           if excellent_conversion:
               insights.append(f"🎯 {len(excellent_conversion)} mağaza mükemmel dönüşüm oranı (>%28) yakaladı")
           
           # Performance trends
           improving = [r for r in rows if r.get('week_over_week_change', 0) > 10]
           declining = [r for r in rows if r.get('week_over_week_change', 0) < -10]
           
           if improving:
               insights.append(f"📈 {len(improving)} mağaza geçen haftaya göre %10+ büyüme gösterdi")
           if declining:
               alerts.append(f"📉 {len(declining)} mağaza geçen haftaya göre %10+ düşüş yaşadı")
           
           # Weather impact
           weather_affected = [r for r in rows if r.get('weather_impact') in ['rain', 'snow', 'cold']]
           if weather_affected:
               insights.append(f"🌧️ {len(weather_affected)} mağaza olumsuz hava koşullarından etkilendi")
           
           # Suit mix analysis
           high_suit_mix = [r for r in rows if r.get('suit_percentage', 0) > 50]
           if high_suit_mix:
               insights.append(f"🤵 {len(high_suit_mix)} mağazada takım elbise satışları %50+ pay aldı")
               recommendations.append("Yüksek takım elbise satışı olan mağazalara ek stok gönderin")
       
       elif query_type == "store_comparison":
           # Star stores
           star_stores = [r for r in rows if '⭐ Star Store' in str(r.get('store_category', ''))]
           if star_stores:
               insights.append(f"⭐ {len(star_stores)} yıldız mağaza: " + 
                             ", ".join([s['store_id'] for s in star_stores[:3]]))
           
           # Regional performance
           country_groups = {}
           for row in rows:
               country = row.get('country', 'Unknown')
               if country not in country_groups:
                   country_groups[country] = []
               country_groups[country].append(row.get('total_revenue_30d', 0))
           
           for country, revenues in country_groups.items():
               avg_revenue = sum(revenues) / len(revenues)
               insights.append(f"🌍 {country} ortalama gelir: {avg_revenue:,.0f} TL ({len(revenues)} mağaza)")
           
           # Franchise vs Owned
           franchise_stores = [r for r in rows if r.get('franchise_flag') == 'franchise']
           owned_stores = [r for r in rows if r.get('franchise_flag') == 'owned']
           
           if franchise_stores and owned_stores:
               franchise_avg = sum(r.get('total_revenue_30d', 0) for r in franchise_stores) / len(franchise_stores)
               owned_avg = sum(r.get('total_revenue_30d', 0) for r in owned_stores) / len(owned_stores)
               
               if owned_avg > franchise_avg:
                   insights.append(f"🏢 Şirket mağazaları %{((owned_avg/franchise_avg - 1) * 100):.0f} daha iyi performans gösteriyor")
               else:
                   insights.append(f"🤝 Franchise mağazalar %{((franchise_avg/owned_avg - 1) * 100):.0f} daha iyi performans gösteriyor")
           
           # Efficiency champions
           efficient = [r for r in rows if r.get('productivity_rank', 999) <= 3]
           if efficient:
               insights.append(f"💪 En verimli mağazalar: " + 
                             ", ".join([f"{s['store_id']} ({s['revenue_per_staff_hour']:.0f} TL/saat)" 
                                      for s in efficient]))
       
       elif query_type == "staff_analysis":
           # Top performers
           top_performers = [r for r in rows if '⭐ Top 10%' in str(r.get('performance_tier', ''))]
           if top_performers:
               insights.append(f"⭐ {len(top_performers)} çalışan üst %10'luk dilimde performans gösteriyor")
               for performer in top_performers[:3]:
                   insights.append(f"  • {performer['employee_id']}: {performer['avg_sales_per_hour']:.0f} TL/saat")
           
           # Training needs
           training_needed = [r for r in rows if '📚 Training Required' in str(r.get('training_status', ''))]
           if training_needed:
               alerts.append(f"📚 {len(training_needed)} çalışan için eğitim gerekli (skor < 8.5)")
               recommendations.append("Eğitim skorları düşük personel için acil eğitim planı oluşturun")
           
           # Role performance
           roles_data = {}
           for row in rows:
               role = row.get('primary_role', 'Unknown')
               if role not in roles_data:
                   roles_data[role] = []
               roles_data[role].append(row.get('avg_sales_per_hour', 0))
           
           for role, sales in roles_data.items():
               if sales:
                   avg_sales = sum(sales) / len(sales)
                   insights.append(f"👔 {role} ortalama: {avg_sales:.0f} TL/saat ({len(sales)} kişi)")
           
           # Specialties
           suit_specialists = [r for r in rows if '🤵 Suit Specialist' in str(r.get('specialty', ''))]
           if suit_specialists:
               insights.append(f"🤵 {len(suit_specialists)} takım elbise uzmanı tespit edildi")
       
       elif query_type == "traffic_conversion":
           # Conversion champions
           excellent = [r for r in rows if '🏆 Excellent' in str(r.get('conversion_tier', ''))]
           poor = [r for r in rows if '⚠️ Poor' in str(r.get('conversion_tier', ''))]
           
           if excellent:
               insights.append(f"🏆 {len(excellent)} mağaza mükemmel dönüşüm oranına sahip (>%28)")
           if poor:
               alerts.append(f"⚠️ {len(poor)} mağaza düşük dönüşüm oranında (<%20)")
               recommendations.append("Düşük dönüşümlü mağazalarda müşteri deneyimi denetimi yapın")
           
           # Weekend vs Weekday
           weekend_performers = [r for r in rows if '🎯 Weekend Performer' in str(r.get('performance_pattern', ''))]
           if weekend_performers:
               insights.append(f"🎯 {len(weekend_performers)} mağaza hafta sonu güçlü performans gösteriyor")
               recommendations.append("Hafta sonu güçlü mağazalara ek personel atayın")
           
           # Weather impact
           high_weather_impact = [r for r in rows if r.get('weather_impact', 0) > 5]
           if high_weather_impact:
               insights.append(f"🌤️ {len(high_weather_impact)} mağazada hava durumu dönüşümü %5+ etkiliyor")
           
           # Mall capture
           high_capture = [r for r in rows if r.get('mall_capture_rate', 0) > 5]
           if high_capture:
               insights.append(f"🧲 {len(high_capture)} mağaza AVM trafiğinin %5+'ını yakalıyor")
       
       elif query_type == "kpi_tracking":
           # Achievement levels
           exceeding = [r for r in rows if '⭐ Exceeding' in str(r.get('performance_level', ''))]
           missing = [r for r in rows if '🔴 Missing' in str(r.get('performance_level', ''))]
           
           if exceeding:
               insights.append(f"⭐ {len(exceeding)} mağaza hedefleri aşıyor (>%105)")
           if missing:
               alerts.append(f"🔴 {len(missing)} mağaza hedefleri tutturamıyor (<%85)")
           
           # Trends
           improving = [r for r in rows if '📈 Improving' in str(r.get('trend_direction', ''))]
           declining = [r for r in rows if '📉 Declining' in str(r.get('trend_direction', ''))]
           
           if improving:
               insights.append(f"📈 {len(improving)} mağaza KPI performansı yükselişte")
           if declining:
               alerts.append(f"📉 {len(declining)} mağaza KPI performansı düşüşte")
           
           # Focus areas
           revenue_focus = [r for r in rows if 'revenue generation' in str(r.get('primary_focus_area', ''))]
           conversion_focus = [r for r in rows if 'conversion tactics' in str(r.get('primary_focus_area', ''))]
           
           if revenue_focus:
               recommendations.append(f"{len(revenue_focus)} mağaza için gelir artırıcı kampanyalar başlatın")
           if conversion_focus:
               recommendations.append(f"{len(conversion_focus)} mağaza için dönüşüm optimizasyonu yapın")
   
   except Exception as e:
       logger.error(f"Error in analyze_results: {str(e)}")
   
   return {
       "insights": insights[:10],
       "recommendations": recommendations[:5],
       "alerts": alerts[:5]
   }

def calculate_summary(rows: List[Dict], query_type: str) -> Dict[str, Any]:
   """Calculate summary statistics"""
   
   if not rows:
       return {"total_records": 0}
   
   summary = {
       "total_records": len(rows),
       "query_type": query_type,
       "timestamp": datetime.utcnow().isoformat()
   }
   
   try:
       if query_type == "daily_performance":
           summary.update({
               "total_revenue": sum(r.get('net_revenue', 0) for r in rows),
               "total_transactions": sum(r.get('transactions', 0) for r in rows),
               "avg_conversion": sum(r.get('conversion_rate', 0) for r in rows) / len(rows) if rows else 0,
               "total_visitors": sum(r.get('store_entries', 0) for r in rows),
               "unique_stores": len(set(r.get('store_id') for r in rows)),
               "date_range": f"{rows[-1].get('date')} to {rows[0].get('date')}" if rows else ""
           })
       
       elif query_type == "store_comparison":
           summary.update({
               "total_stores": len(rows),
               "star_stores": len([r for r in rows if '⭐ Star Store' in str(r.get('store_category', ''))]),
               "total_revenue_30d": sum(r.get('total_revenue_30d', 0) for r in rows),
               "avg_conversion": sum(r.get('conversion_rate', 0) for r in rows) / len(rows) if rows else 0,
               "franchise_count": len([r for r in rows if r.get('franchise_flag') == 'franchise']),
               "owned_count": len([r for r in rows if r.get('franchise_flag') == 'owned'])
           })
       
       elif query_type == "staff_analysis":
           summary.update({
               "total_employees": len(rows),
               "top_performers": len([r for r in rows if '⭐ Top 10%' in str(r.get('performance_tier', ''))]),
               "avg_sales_per_hour": sum(r.get('avg_sales_per_hour', 0) for r in rows) / len(rows) if rows else 0,
               "total_sales": sum(r.get('total_sales_value', 0) for r in rows),
               "training_required": len([r for r in rows if '📚 Training Required' in str(r.get('training_status', ''))])
           })
   
   except Exception as e:
       logger.error(f"Error in calculate_summary: {str(e)}")
   
   return summary

# =======================
# Main HTTP Handler
# =======================

@functions_framework.http
def store_performance_query(request):
   """Suvari Store Performance Analytics Engine"""
   
   try:
       # Parse request
       body = request.get_json(silent=True) or {}
       
       # Extract parameters
       question = body.get("question", "")
       query_type = body.get("query_type") or detect_query_type(question)
       store_id = body.get("store_id")
       date_range = body.get("date_range", 7)
       limit = min(int(body.get("limit", 100)), 500)
       
       logger.info(f"Processing query - Type: {query_type}, Store: {store_id}, Range: {date_range} days")
       
       # Query function mapping
       query_functions = {
           "daily_performance": lambda: sql_daily_performance(store_id, date_range),
           "store_comparison": sql_store_comparison,
           "staff_analysis": sql_staff_performance,
           "staff_performance": sql_staff_performance,
           "traffic_conversion": sql_traffic_conversion,
           "traffic_analysis": sql_traffic_conversion,
           "kpi_tracking": sql_kpi_achievement,
           "kpi_achievement": sql_kpi_achievement,
           "trend_analysis": lambda: sql_daily_performance(store_id, 30),
           "regional_performance": sql_store_comparison,
           "franchise_analysis": sql_store_comparison,
           "hourly_patterns": sql_traffic_conversion,
           "weekend_analysis": sql_traffic_conversion
       }
       
       # Get appropriate query
       query_func = query_functions.get(query_type, lambda: sql_daily_performance(store_id, date_range))
       sql, params = query_func()
       
       # Fill parameters
       filled_params = []
       for p in params:
           if p.name == "limit":
               filled_params.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))
           else:
               filled_params.append(p)
       
       # Execute query
       client = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)
       job_config = bigquery.QueryJobConfig(
           query_parameters=filled_params,
           maximum_bytes_billed=5 * 1024 * 1024 * 1024,  # 5GB limit
           labels={"agent": "store_performance", "query_type": query_type}
       )
       
       job = client.query(sql, job_config=job_config)
       rows = [dict(r) for r in job.result()]
       
       # Analyze results
       analysis = analyze_results(rows, query_type)
       summary = calculate_summary(rows, query_type)
       
       # Build response
       response = {
           "success": True,
           "agent": "store_performance",
           "query_type": query_type,
           "parameters": {
               "store_id": store_id,
               "date_range": date_range,
               "limit": limit
           },
           "summary": summary,
           "insights": analysis["insights"],
           "recommendations": analysis["recommendations"],
           "alerts": analysis["alerts"],
           "row_count": len(rows),
           "rows": rows,
           "metadata": {
               "bytes_processed": job.total_bytes_processed,
               "bytes_billed": job.total_bytes_billed,
               "cache_hit": job.cache_hit,
               "dataset": DATASET,
               "location": BQ_LOCATION
           }
       }
       
       return (json.dumps(response, ensure_ascii=False, default=str), 200, {"Content-Type": "application/json"})
       
   except Exception as e:
       logger.exception(f"Store performance query failed: {str(e)}")
       error_response = {
           "success": False,
           "error": str(e),
           "error_type": type(e).__name__,
           "agent": "store_performance",
           "query_type": query_type if 'query_type' in locals() else "unknown"
       }
       return (json.dumps(error_response, ensure_ascii=False), 500, {"Content-Type": "application/json"})