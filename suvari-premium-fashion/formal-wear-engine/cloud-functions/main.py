# main.py - Suvari Advanced Analytics Engine
# Autonomous LLM-Powered Business Intelligence System
# Version: 4.0 - Full Featured Production System
import os
import json
import logging
import re
from typing import List, Optional, Dict, Any, Tuple, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from collections import Counter  # ✅ BURAYA EKLENDİ
import functions_framework
from google.cloud import bigquery
# Kullanılmayan importları kaldırın:
# import hashlib  # KULLANILMIYOR
# from concurrent.futures import ThreadPoolExecutor  # KULLANILMIYOR

# =======================
# Configuration & Setup
# =======================
PROJECT_ID = "agentspace-ngc"
BQ_LOCATION = "europe-west1"
DATASET = "suvari_formal_wear"

# Enhanced logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("suvari-ai")

# =======================
# Data Models & Enums
# =======================
class QueryType(Enum):
    SUIT_SALES = "suit_sales"
    BUNDLE_ANALYSIS = "bundle_analysis"
    SIZE_ANALYSIS = "size_analysis"
    SEASONAL_TRENDS = "seasonal_trends"
    CROSS_ANALYSIS = "cross_analysis"
    PREDICTIVE = "predictive"
    ANOMALY = "anomaly"

class InsightLevel(Enum):
    CRITICAL = "🚨"
    HIGH = "🔴"
    MEDIUM = "⚠️"
    LOW = "ℹ️"
    SUCCESS = "✅"
    EXCELLENCE = "🏆"

@dataclass
class BusinessMetrics:
    """Dynamic business thresholds that adapt based on data"""
    # Performance thresholds
    alteration_rate_excellent: float = 10.0
    alteration_rate_target: float = 15.0
    alteration_rate_critical: float = 25.0
    
    return_rate_excellent: float = 3.0
    return_rate_threshold: float = 5.0
    return_rate_critical: float = 10.0
    
    satisfaction_excellent: float = 4.7
    satisfaction_target: float = 4.5
    satisfaction_minimum: float = 4.0
    
    margin_premium: float = 70.0
    margin_target: float = 60.0
    margin_minimum: float = 50.0
    
    bundle_attach_excellent: float = 75.0
    bundle_attach_target: float = 65.0
    bundle_attach_minimum: float = 50.0
    
    demand_index_exceptional: float = 9.0
    demand_index_high: float = 8.0
    demand_index_good: float = 7.0
    demand_index_low: float = 6.0

@dataclass
class QueryContext:
    """Enhanced context for intelligent query processing"""
    original_question: str
    detected_intent: QueryType
    entities: Dict[str, Any] = field(default_factory=dict)
    time_range: Dict[str, str] = field(default_factory=dict)
    filters: Dict[str, Any] = field(default_factory=dict)
    user_language: str = "tr"
    confidence_score: float = 0.0
    suggested_queries: List[str] = field(default_factory=list)

# =======================
# Advanced NLP & Intent Detection
# =======================
class IntentDetector:
    """Advanced intent detection with entity extraction"""
    
    def __init__(self):
        self.patterns = self._initialize_patterns()
        self.entity_patterns = self._initialize_entity_patterns()
        
    def _initialize_patterns(self) -> Dict[QueryType, List[str]]:
        return {
            QueryType.SUIT_SALES: [
                r"takım\s*elbise|suit|business|wedding|luxury|executive|casual",
                r"satış|sales|revenue|gelir|ciro|kar|profit|margin|marj",
                r"kategori|category|type|tür|segment"
            ],
            QueryType.BUNDLE_ANALYSIS: [
                r"bundle|paket|set|combo|kampanya|campaign",
                r"discount|indirim|tasarruf|savings|value",
                r"attach|ekleme|birlikte|together"
            ],
            QueryType.SIZE_ANALYSIS: [
                r"beden|size|ölçü|measurement|fit|kalıp",
                r"tadilat|alteration|düzeltme|adjustment",
                r"iade|return|memnuniyet|satisfaction"
            ],
            QueryType.SEASONAL_TRENDS: [
                r"sezon|season|mevsim|trend|eğilim",
                r"talep|demand|popüler|popular",
                r"renk|color|kumaş|fabric|stil|style"
            ],
            QueryType.CROSS_ANALYSIS: [
                r"karşılaştır|compare|versus|vs|kıyas",
                r"benchmark|fark|difference|arasında|between"
            ],
            QueryType.PREDICTIVE: [
                r"tahmin|predict|forecast|öngörü|gelecek|future",
                r"trend|eğilim|potansiyel|potential"
            ],
            QueryType.ANOMALY: [
                r"anomali|anomaly|anormal|unusual|garip|strange",
                r"outlier|sapma|deviation|beklenmedik|unexpected"
            ]
        }
    
    def _initialize_entity_patterns(self) -> Dict[str, str]:
        return {
            "country": r"(türkiye|turkey|almanya|germany|rusya|russia|kazakistan|kazakhstan)",
            "store": r"(sv\d{3}|mağaza\s*\d+|store\s*\d+)",
            "suit_type": r"(business|wedding|luxury|casual|executive)",
            "bundle_type": r"(complete professional|premium wedding|business essential|weekend smart|luxury executive)",
            "size": r"(4[6-9]|5[0-8]|60)\b",
            "season": r"(kış|winter|yaz|summer|bahar|spring|sonbahar|fall|autumn)",
            "time_period": r"(bugün|today|dün|yesterday|bu hafta|this week|geçen hafta|last week|bu ay|this month)",
            "percentage": r"(\d+(?:\.\d+)?%|yüzde\s*\d+)",  # ✅ DÜZELTME
            "amount": r"(\d+(?:\.\d+)?(?:\s*(?:tl|usd|euro))?)"
        }

    def detect(self, question: str) -> QueryContext:
        """Detect intent and extract entities from question"""
        q_lower = question.lower()
        context = QueryContext(
            original_question=question,
            detected_intent=QueryType.SUIT_SALES,
            confidence_score=0.0
        )
        
        # Score each intent
        intent_scores = {}
        for intent, patterns in self.patterns.items():
            score = 0
            for pattern in patterns:
                if re.search(pattern, q_lower):
                    score += 1
            intent_scores[intent] = score
        
        # Select best intent
        if intent_scores:
            best_intent = max(intent_scores, key=intent_scores.get)
            max_score = intent_scores[best_intent]
            if max_score > 0:
                context.detected_intent = best_intent
                context.confidence_score = min(max_score / 3.0, 1.0)
        
        # Extract entities
        for entity_type, pattern in self.entity_patterns.items():
            matches = re.findall(pattern, q_lower)
            if matches:
                context.entities[entity_type] = matches[0] if len(matches) == 1 else matches
        
        # Detect language
        turkish_indicators = ["mı", "mi", "mu", "mü", "ğ", "ş", "ı", "ö", "ü", "ç"]
        context.user_language = "tr" if any(char in question for char in turkish_indicators) else "en"
        
        # Generate suggested follow-up queries
        context.suggested_queries = self._generate_suggestions(context)
        
        return context
    
    def _generate_suggestions(self, context: QueryContext) -> List[str]:
        """Generate intelligent follow-up query suggestions"""
        suggestions = []
        
        if context.detected_intent == QueryType.SUIT_SALES:
            suggestions = [
                "En karlı suit kategorisi hangisi?",
                "Hangi mağaza en yüksek suit satışı yapıyor?",
                "Suit satışlarında ülke karşılaştırması"
            ]
        elif context.detected_intent == QueryType.BUNDLE_ANALYSIS:
            suggestions = [
                "En popüler bundle kombinasyonu nedir?",
                "Bundle attach rate analizi",
                "Bundle müşteri tasarruf analizi"
            ]
        elif context.detected_intent == QueryType.SIZE_ANALYSIS:
            suggestions = [
                "En çok iade edilen bedenler hangileri?",
                "Beden bazlı müşteri memnuniyeti",
                "Tadilat gerektirmeyen bedenler"
            ]
        elif context.detected_intent == QueryType.SEASONAL_TRENDS:
            suggestions = [
                "Sezonluk renk trendleri",
                "En yüksek talep gören sezon",
                "Fiyat hassasiyeti analizi"
            ]
        
        return suggestions[:3]

# =======================
# SQL Query Builder
# =======================
class QueryBuilder:
    """Dynamic SQL query builder with optimization"""
    
    def __init__(self, metrics: BusinessMetrics):
        self.metrics = metrics
        self.tables = {
            "suit_sales": f"`{PROJECT_ID}.{DATASET}.suit_sales`",
            "bundle_analysis": f"`{PROJECT_ID}.{DATASET}.bundle_analysis`",
            "size_fit": f"`{PROJECT_ID}.{DATASET}.size_fit`",
            "seasonal_trends": f"`{PROJECT_ID}.{DATASET}.seasonal_trends`"
        }
    
    def build_suit_sales_query(self, context: QueryContext) -> Tuple[str, List]:
        """Build optimized suit sales query"""
        
        # Dynamic date range
        date_filter = self._build_date_filter(context)
        
        # Dynamic filters based on entities
        where_conditions = ["ss.date BETWEEN DATE('2024-12-14') AND DATE('2024-12-15')"]
        
        if "country" in context.entities:
            country = context.entities["country"]
            where_conditions.append(f"LOWER(ss.country) = '{country}'")
        
        if "suit_type" in context.entities:
            suit_type = context.entities["suit_type"]
            where_conditions.append(f"LOWER(ss.suit_type) = '{suit_type}'")
        
        if "store" in context.entities:
            store = context.entities["store"]
            where_conditions.append(f"ss.store_id = '{store.upper()}'")
        
        where_clause = " AND ".join(where_conditions)
        
        sql = f"""
        WITH suit_performance AS (
            SELECT 
                ss.date,
                ss.store_id,
                ss.country,
                ss.suit_type,
                ss.fit_type,
                ss.color,
                ss.fabric,
                ss.price_tier,
                ss.bundle_flag,
                ss.customer_occasion,
                ss.age_group,
                ss.size,
                ss.sale_price,
                ss.cost_price,
                ss.margin_pct,
                -- Fixed: Handle boolean properly
                CASE WHEN ss.alteration_needed THEN 1 ELSE 0 END as needs_alteration
            FROM {self.tables['suit_sales']} ss
            WHERE {where_clause}
        ),
        suit_aggregated AS (
            SELECT 
                store_id,
                country,
                suit_type,
                COUNT(*) as units_sold,
                SUM(sale_price) as gross_revenue,
                SUM(sale_price - cost_price) as profit,
                AVG(margin_pct) as avg_margin,
                SUM(needs_alteration) as alterations_needed,
                COUNT(DISTINCT customer_occasion) as occasion_variety,
                COUNT(DISTINCT age_group) as age_groups_served,
                COUNT(DISTINCT color) as color_variety,
                COUNT(DISTINCT fabric) as fabric_variety,
                COUNT(DISTINCT size) as size_variety,
                ROUND(100.0 * SUM(needs_alteration) / NULLIF(COUNT(*), 0), 1) as alteration_rate,
                ROUND(AVG(sale_price), 2) as avg_sale_price,
                ROUND(STDDEV(sale_price), 2) as price_std_dev,
                MIN(sale_price) as min_price,
                MAX(sale_price) as max_price,
                ROUND(100.0 * COUNT(CASE WHEN bundle_flag THEN 1 END) / NULLIF(COUNT(*), 0), 1) as bundle_attach_rate

            FROM suit_performance
            GROUP BY store_id, country, suit_type
        ),
        performance_rankings AS (
            SELECT 
                *,
                RANK() OVER (ORDER BY gross_revenue DESC) as revenue_rank,
                RANK() OVER (ORDER BY units_sold DESC) as volume_rank,
                RANK() OVER (ORDER BY avg_margin DESC) as margin_rank,
                RANK() OVER (ORDER BY alteration_rate ASC) as fit_rank,
                RANK() OVER (PARTITION BY country ORDER BY gross_revenue DESC) as country_revenue_rank,
                PERCENTILE_CONT(gross_revenue, 0.5) OVER() as median_revenue,
                PERCENTILE_CONT(avg_margin, 0.75) OVER() as q3_margin
            FROM suit_aggregated
        )
        SELECT 
            store_id,
            country,
            suit_type,
            units_sold,
            ROUND(gross_revenue, 2) as gross_revenue,
            ROUND(profit, 2) as profit,
            ROUND(avg_margin, 1) as avg_margin_pct,
            alteration_rate,
            ROUND(avg_sale_price, 2) as avg_sale_price,
            ROUND(price_std_dev, 2) as price_volatility,
            min_price,
            max_price,
            bundle_attach_rate,
            occasion_variety,
            age_groups_served,
            color_variety,
            fabric_variety,
            size_variety,
            revenue_rank,
            volume_rank,
            margin_rank,
            fit_rank,
            country_revenue_rank,
            ROUND(median_revenue, 2) as market_median_revenue,
            ROUND(q3_margin, 1) as market_q3_margin,
            
            -- Advanced performance categorization
            CASE 
                WHEN revenue_rank <= 3 AND margin_rank <= 5 THEN '🏆 Star Performer'
                WHEN revenue_rank <= 5 THEN '💰 Revenue Champion'
                WHEN volume_rank <= 5 THEN '📊 Volume Leader'
                WHEN margin_rank <= 3 THEN '💎 Margin Excellence'
                WHEN country_revenue_rank = 1 THEN '🌍 Country Leader'
                WHEN gross_revenue > median_revenue THEN '✅ Above Average'
                ELSE '📈 Growth Opportunity'
            END as performance_tier,
            
            CASE
                WHEN alteration_rate <= {self.metrics.alteration_rate_excellent} THEN '🏆 Perfect Fit'
                WHEN alteration_rate <= {self.metrics.alteration_rate_target} THEN '✅ Good Fit'
                WHEN alteration_rate <= {self.metrics.alteration_rate_critical} THEN '⚠️ Fit Issues'
                ELSE '🔴 Critical Fit Problem'
            END as fit_quality,
            
            CASE
                WHEN avg_margin >= {self.metrics.margin_premium} THEN '💎 Premium Margin'
                WHEN avg_margin >= {self.metrics.margin_target} THEN '💰 Target Margin'
                WHEN avg_margin >= {self.metrics.margin_minimum} THEN '📊 Standard Margin'
                ELSE '🔴 Below Target Margin'
            END as margin_health,
            
            CASE
                WHEN bundle_attach_rate >= {self.metrics.bundle_attach_excellent} THEN '🎯 Excellent Attach'
                WHEN bundle_attach_rate >= {self.metrics.bundle_attach_target} THEN '✅ Good Attach'
                WHEN bundle_attach_rate >= {self.metrics.bundle_attach_minimum} THEN '📊 Average Attach'
                ELSE '📉 Low Attach Rate'
            END as bundle_performance,
            
            -- Opportunity scoring
            ROUND(
                (CASE WHEN avg_margin < q3_margin THEN (q3_margin - avg_margin) * 0.3 ELSE 0 END) +
                (CASE WHEN alteration_rate > {self.metrics.alteration_rate_target} THEN (alteration_rate - {self.metrics.alteration_rate_target}) * 0.2 ELSE 0 END) +
                (CASE WHEN bundle_attach_rate < {self.metrics.bundle_attach_target} THEN ({self.metrics.bundle_attach_target} - bundle_attach_rate) * 0.2 ELSE 0 END) +
                (CASE WHEN gross_revenue < median_revenue THEN 10 ELSE 0 END)
            , 1) as improvement_opportunity_score
            
        FROM performance_rankings
        ORDER BY gross_revenue DESC
        LIMIT @limit
        """
        
        params = [
            bigquery.ScalarQueryParameter("limit", "INT64", 100)
        ]
        
        return sql, params
    
    def build_bundle_analysis_query(self, context: QueryContext) -> Tuple[str, List]:
        """Build bundle analysis query with advanced metrics"""
        
        sql = f"""
        WITH bundle_metrics AS (
            SELECT 
                ba.date,
                ba.store_id,
                ba.bundle_type,
                ba.customer_type,
                ba.suit_price,
                ba.shirt_price,
                ba.tie_price,
                ba.belt_price,
                ba.shoes_price,
                ba.total_bundle_price,
                ba.individual_price_sum,
                ba.discount_amount,
                ba.discount_pct,
                ba.quantity_sold,
                ba.revenue_total,
                ba.margin_total,
                -- Additional calculations
                ba.individual_price_sum - ba.total_bundle_price as customer_savings,
                ROUND(100.0 * ba.discount_amount / NULLIF(ba.individual_price_sum, 0), 1) as true_discount_rate,
                ROUND(ba.revenue_total / NULLIF(ba.quantity_sold, 0), 2) as avg_transaction_value
            FROM {self.tables['bundle_analysis']} ba
            WHERE ba.date BETWEEN DATE('2024-12-14') AND DATE('2024-12-15')
        ),
        bundle_aggregated AS (
            SELECT 
                store_id,
                bundle_type,
                SUM(quantity_sold) as total_units,
                SUM(revenue_total) as total_revenue,
                SUM(margin_total) as total_profit,
                AVG(discount_pct) as avg_discount,
                AVG(total_bundle_price) as avg_price,
                SUM(customer_savings) as total_customer_savings,
                COUNT(DISTINCT customer_type) as customer_segments,
                AVG(true_discount_rate) as effective_discount,
                AVG(avg_transaction_value) as avg_transaction_size,
                STDDEV(total_bundle_price) as price_volatility,
                MIN(total_bundle_price) as min_bundle_price,
                MAX(total_bundle_price) as max_bundle_price,
                ROUND(100.0 * SUM(margin_total) / NULLIF(SUM(revenue_total), 0), 1) as profit_margin,
                COUNT(DISTINCT DATE(date)) as active_days
            FROM bundle_metrics
            GROUP BY store_id, bundle_type
        ),
        bundle_rankings AS (
            SELECT 
                *,
                RANK() OVER (ORDER BY total_revenue DESC) as revenue_rank,
                RANK() OVER (ORDER BY total_units DESC) as volume_rank,
                RANK() OVER (ORDER BY profit_margin DESC) as margin_rank,
                RANK() OVER (PARTITION BY bundle_type ORDER BY total_revenue DESC) as type_rank,
                PERCENTILE_CONT(total_revenue, 0.5) OVER() as median_revenue,
                PERCENTILE_CONT(profit_margin, 0.75) OVER() as q3_margin,
                AVG(total_revenue) OVER() as avg_market_revenue
            FROM bundle_aggregated
        )
        SELECT 
            store_id,
            bundle_type,
            total_units,
            ROUND(total_revenue, 2) as total_revenue,
            ROUND(total_profit, 2) as total_profit,
            ROUND(avg_discount, 1) as avg_discount_pct,
            ROUND(avg_price, 2) as avg_bundle_price,
            ROUND(total_customer_savings, 2) as customer_value_created,
            customer_segments,
            ROUND(effective_discount, 1) as real_discount_rate,
            ROUND(avg_transaction_size, 2) as avg_basket_size,
            ROUND(price_volatility, 2) as price_consistency,
            min_bundle_price,
            max_bundle_price,
            profit_margin as margin_pct,
            active_days,
            revenue_rank,
            volume_rank,
            margin_rank,
            type_rank,
            ROUND(median_revenue, 2) as market_median,
            ROUND(q3_margin, 1) as market_q3_margin,
            
            -- Strategic categorization
            CASE 
                WHEN revenue_rank <= 3 AND margin_rank <= 5 THEN '⭐ Bundle Superstar'
                WHEN type_rank = 1 THEN '🏆 Category Champion'
                WHEN total_revenue > avg_market_revenue * 1.5 THEN '💰 High Performer'
                WHEN volume_rank <= 5 THEN '📊 Volume Driver'
                WHEN margin_rank <= 3 THEN '💎 Margin Leader'
                ELSE '📈 Standard Bundle'
            END as bundle_status,
            
            CASE
                WHEN effective_discount >= 20 THEN '🎁 Ultra Value'
                WHEN effective_discount >= 15 THEN '💰 High Value'
                WHEN effective_discount >= 10 THEN '✅ Good Value'
                ELSE '📊 Standard Value'
            END as value_tier,
            
            CASE
                WHEN customer_segments >= 4 THEN '🌟 Universal Appeal'
                WHEN customer_segments >= 3 THEN '🎯 Broad Market'
                WHEN customer_segments >= 2 THEN '👥 Dual Market'
                ELSE '🔍 Niche Focus'
            END as market_coverage,
            
            -- Optimization score
            ROUND(
                (profit_margin / NULLIF(q3_margin, 0) * 25) +
                (total_revenue / NULLIF(median_revenue, 0) * 25) +
                (customer_segments * 10) +
                (CASE WHEN effective_discount BETWEEN 12 AND 18 THEN 20 ELSE 10 END) +
                (active_days * 2)
            , 1) as bundle_effectiveness_score
            
        FROM bundle_rankings
        ORDER BY bundle_effectiveness_score DESC, total_revenue DESC
        LIMIT @limit
        """
        
        params = [
            bigquery.ScalarQueryParameter("limit", "INT64", 100)
        ]
        
        return sql, params
    
    def build_size_analysis_query(self, context: QueryContext) -> Tuple[str, List]:
        """Build size and fit analysis query"""
        
        sql = f"""
        WITH size_data AS (
            SELECT 
                sf.date,
                sf.store_id,
                sf.country,
                sf.size,
                sf.fit_type,
                sf.quantity_sold,
                sf.alteration_requested,
                sf.alteration_type,
                CASE WHEN sf.return_flag THEN 1 ELSE 0 END as is_returned,
                sf.return_reason,
                sf.customer_height_cm,
                sf.customer_weight_kg,
                sf.satisfaction_score,
                sf.regional_preference
            FROM {self.tables['size_fit']} sf
            WHERE sf.date BETWEEN DATE('2024-12-14') AND DATE('2024-12-15')
        ),
        size_aggregated AS (
            SELECT 
                store_id,
                country,
                size,
                fit_type,
                SUM(quantity_sold) as total_units,
                SUM(alteration_requested) as total_alterations,
                SUM(is_returned) as total_returns,
                AVG(satisfaction_score) as avg_satisfaction,
                STDDEV(satisfaction_score) as satisfaction_variance,
                AVG(customer_height_cm) as avg_height,
                AVG(customer_weight_kg) as avg_weight,
                STDDEV(customer_height_cm) as height_variance,
                STDDEV(customer_weight_kg) as weight_variance,
                COUNT(DISTINCT alteration_type) as alteration_types,
                COUNT(DISTINCT return_reason) as return_reasons,
                COUNT(DISTINCT regional_preference) as style_variations,
                ROUND(100.0 * SUM(alteration_requested) / NULLIF(SUM(quantity_sold), 0), 1) as alteration_rate,
                ROUND(100.0 * SUM(is_returned) / NULLIF(SUM(quantity_sold), 0), 1) as return_rate,
                -- BMI calculation for fit analysis
                ROUND(AVG(customer_weight_kg) / POWER(AVG(customer_height_cm) / 100, 2), 1) as avg_bmi
            FROM size_data
            GROUP BY store_id, country, size, fit_type
        ),
        size_rankings AS (
            SELECT 
                *,
                RANK() OVER (ORDER BY total_units DESC) as popularity_rank,
                RANK() OVER (ORDER BY avg_satisfaction DESC) as satisfaction_rank,
                RANK() OVER (ORDER BY alteration_rate ASC) as fit_quality_rank,
                RANK() OVER (ORDER BY return_rate ASC) as return_performance_rank,
                RANK() OVER (PARTITION BY country ORDER BY total_units DESC) as country_popularity_rank,
                PERCENTILE_CONT(alteration_rate, 0.25) OVER() as q1_alteration,
                PERCENTILE_CONT(return_rate, 0.25) OVER() as q1_return,
                PERCENTILE_CONT(avg_satisfaction, 0.75) OVER() as q3_satisfaction
            FROM size_aggregated
        )
        SELECT 
            store_id,
            country,
            size,
            fit_type,
            total_units,
            total_alterations,
            total_returns,
            alteration_rate,
            return_rate,
            ROUND(avg_satisfaction, 2) as avg_satisfaction_score,
            ROUND(satisfaction_variance, 2) as satisfaction_consistency,
            ROUND(avg_height, 1) as avg_customer_height,
            ROUND(avg_weight, 1) as avg_customer_weight,
            ROUND(avg_bmi, 1) as avg_customer_bmi,
            ROUND(height_variance, 1) as height_variation,
            ROUND(weight_variance, 1) as weight_variation,
            alteration_types,
            return_reasons,
            style_variations,
            popularity_rank,
            satisfaction_rank,
            fit_quality_rank,
            return_performance_rank,
            country_popularity_rank,
            
            -- Advanced categorization
            CASE 
                WHEN popularity_rank <= 3 AND avg_satisfaction >= {self.metrics.satisfaction_excellent} THEN '🌟 Perfect Match'
                WHEN popularity_rank <= 5 THEN '🔥 Hot Size'
                WHEN total_units >= 20 THEN '📊 High Demand'
                WHEN total_units >= 10 THEN '✅ Standard Demand'
                ELSE '📉 Low Demand'
            END as demand_status,
            
            CASE
                WHEN alteration_rate <= q1_alteration AND return_rate <= q1_return THEN '🏆 Perfect Fit'
                WHEN alteration_rate <= {self.metrics.alteration_rate_target} THEN '✅ Good Fit'
                WHEN alteration_rate <= {self.metrics.alteration_rate_critical} THEN '⚠️ Fit Challenges'
                ELSE '🔴 Critical Fit Issues'
            END as fit_assessment,
            
            CASE
                WHEN avg_satisfaction >= q3_satisfaction THEN '⭐ Exceptional Satisfaction'
                WHEN avg_satisfaction >= {self.metrics.satisfaction_target} THEN '✅ High Satisfaction'
                WHEN avg_satisfaction >= {self.metrics.satisfaction_minimum} THEN '📊 Acceptable'
                ELSE '🔴 Low Satisfaction'
            END as satisfaction_tier,
            
            CASE
                WHEN return_rate <= {self.metrics.return_rate_excellent} THEN '🏆 Minimal Returns'
                WHEN return_rate <= {self.metrics.return_rate_threshold} THEN '✅ Low Returns'
                WHEN return_rate <= {self.metrics.return_rate_critical} THEN '⚠️ Moderate Returns'
                ELSE '🔴 High Return Risk'
            END as return_risk,
            
            -- Body type classification
            CASE
                WHEN avg_bmi < 18.5 THEN 'Slim Build'
                WHEN avg_bmi BETWEEN 18.5 AND 24.9 THEN 'Regular Build'
                WHEN avg_bmi BETWEEN 25 AND 29.9 THEN 'Athletic Build'
                ELSE 'Large Build'
            END as typical_body_type,
            
            -- Quality score calculation
            ROUND(
                (avg_satisfaction / 5.0 * 30) +
                (CASE WHEN alteration_rate <= {self.metrics.alteration_rate_target} 
                    THEN (1 - alteration_rate/100) * 30 ELSE 0 END) +
                (CASE WHEN return_rate <= {self.metrics.return_rate_threshold} 
                    THEN (1 - return_rate/100) * 20 ELSE 0 END) +
                (LEAST(total_units / 10, 10)) +
                (style_variations * 2)
            , 1) as size_quality_score
            
        FROM size_rankings
        ORDER BY size_quality_score DESC, total_units DESC
        LIMIT @limit
        """
        
        params = [
            bigquery.ScalarQueryParameter("limit", "INT64", 100)
        ]
        
        return sql, params
    
    def build_seasonal_trends_query(self, context: QueryContext) -> Tuple[str, List]:
        """Build seasonal trends and demand analysis query"""
        
        sql = f"""
        WITH seasonal_data AS (
            SELECT 
                st.date,
                st.store_id,
                st.country,
                st.season,
                st.occasion_type,
                st.demand_index,
                st.color_preference,
                st.fabric_preference,
                st.style_trend,
                st.price_sensitivity,
                st.weather_factor,
                st.cultural_event
            FROM {self.tables['seasonal_trends']} st
            WHERE st.date BETWEEN DATE('2024-12-14') AND DATE('2024-12-15')
        ),
        trend_aggregated AS (
            SELECT 
                store_id,
                country,
                season,
                occasion_type,
                AVG(demand_index) as avg_demand,
                STDDEV(demand_index) as demand_volatility,
                MIN(demand_index) as min_demand,
                MAX(demand_index) as max_demand,
                COUNT(DISTINCT color_preference) as color_variety,
                COUNT(DISTINCT fabric_preference) as fabric_variety,
                COUNT(DISTINCT style_trend) as style_variety,
                COUNT(DISTINCT cultural_event) as cultural_events,
                ARRAY_AGG(DISTINCT color_preference ORDER BY color_preference LIMIT 3) as top_colors,
                ARRAY_AGG(DISTINCT fabric_preference ORDER BY fabric_preference LIMIT 3) as top_fabrics,
                APPROX_TOP_COUNT(price_sensitivity, 1)[OFFSET(0)].value as price_sensitivity_mode,
                APPROX_TOP_COUNT(weather_factor, 1)[OFFSET(0)].value as dominant_weather,
                COUNT(*) as data_points
            FROM seasonal_data
            GROUP BY store_id, country, season, occasion_type
        ),
        trend_rankings AS (
            SELECT 
                *,
                RANK() OVER (ORDER BY avg_demand DESC) as demand_rank,
                RANK() OVER (PARTITION BY country ORDER BY avg_demand DESC) as country_rank,
                RANK() OVER (PARTITION BY season ORDER BY avg_demand DESC) as season_rank,
                PERCENTILE_CONT(avg_demand, 0.75) OVER() as q3_demand,
                PERCENTILE_CONT(avg_demand, 0.50) OVER() as median_demand,
                AVG(avg_demand) OVER (PARTITION BY country) as country_avg_demand
            FROM trend_aggregated
        )
        SELECT 
            store_id,
            country,
            season,
            occasion_type,
            ROUND(avg_demand, 2) as avg_demand_index,
            ROUND(demand_volatility, 2) as demand_stability,
            ROUND(min_demand, 1) as demand_floor,
            ROUND(max_demand, 1) as demand_ceiling,
            color_variety,
            fabric_variety,
            style_variety,
            cultural_events,
            ARRAY_TO_STRING(top_colors, ', ') as trending_colors,
            ARRAY_TO_STRING(top_fabrics, ', ') as trending_fabrics,
            price_sensitivity_mode,
            dominant_weather,
            data_points,
            demand_rank,
            country_rank,
            season_rank,
            ROUND(q3_demand, 1) as market_q3_demand,
            ROUND(median_demand, 1) as market_median_demand,
            ROUND(country_avg_demand, 1) as country_average,
            
            -- Strategic categorization
            CASE 
                WHEN avg_demand >= {self.metrics.demand_index_exceptional} THEN '🚀 Explosive Demand'
                WHEN avg_demand >= {self.metrics.demand_index_high} THEN '🔥 High Demand'
                WHEN avg_demand >= {self.metrics.demand_index_good} THEN '✅ Strong Demand'
                WHEN avg_demand >= {self.metrics.demand_index_low} THEN '📊 Moderate Demand'
                ELSE '📉 Low Demand'
            END as demand_level,
            
            CASE
                WHEN price_sensitivity_mode = 'Low' THEN '💎 Premium Market'
                WHEN price_sensitivity_mode = 'Medium' THEN '💰 Value Market'
                ELSE '💸 Price Sensitive'
            END as market_type,
            
            CASE
                WHEN color_variety >= 5 AND fabric_variety >= 4 THEN '🎨 High Fashion Diversity'
                WHEN color_variety >= 3 AND fabric_variety >= 3 THEN '🎯 Balanced Selection'
                WHEN color_variety >= 2 OR fabric_variety >= 2 THEN '📊 Standard Range'
                ELSE '📌 Limited Options'
            END as product_diversity,
            
            CASE
                WHEN demand_volatility <= 1.0 THEN '⚖️ Very Stable'
                WHEN demand_volatility <= 2.0 THEN '📊 Stable'
                WHEN demand_volatility <= 3.0 THEN '〰️ Variable'
                ELSE '⚡ Volatile'
            END as market_stability,
            
            CASE
                WHEN cultural_events >= 3 THEN '🎉 Event Driven'
                WHEN cultural_events >= 2 THEN '🎊 Moderate Events'
                WHEN cultural_events >= 1 THEN '📅 Some Events'
                ELSE '📆 Regular Season'
            END as event_influence,
            
            -- Opportunity scoring
            ROUND(
                (avg_demand / NULLIF(median_demand, 0) * 20) +
                (CASE WHEN price_sensitivity_mode IN ('Low', 'Medium') THEN 15 ELSE 5 END) +
                (color_variety * 3) +
                (fabric_variety * 3) +
                (CASE WHEN demand_volatility <= 2 THEN 10 ELSE 5 END) +
                (cultural_events * 5) +
                (CASE WHEN avg_demand > country_avg_demand THEN 10 ELSE 0 END)
            , 1) as market_opportunity_score
            
        FROM trend_rankings
        ORDER BY market_opportunity_score DESC, avg_demand DESC
        LIMIT @limit
        """
        
        params = [
            bigquery.ScalarQueryParameter("limit", "INT64", 100)
        ]
        
        return sql, params
    
    def build_cross_analysis_query(self, context: QueryContext) -> Tuple[str, List]:
        """Build cross-dimensional analysis combining multiple tables"""
        
        sql = f"""
        WITH combined_metrics AS (
            SELECT 
                'suit_sales' as source,
                ss.store_id,
                ss.country,
                ss.date,
                ss.sale_price as transaction_value,
                ss.margin_pct,
                CASE WHEN ss.bundle_flag = 'TRUE' THEN 1 ELSE 0 END as is_bundle,
                CASE WHEN ss.alteration_needed THEN 1 ELSE 0 END as needs_alteration,
                ss.suit_type as product_type,
                ss.customer_occasion,
                ss.age_group,
                NULL as satisfaction_score
            FROM {self.tables['suit_sales']} ss
            WHERE ss.date BETWEEN DATE('2024-12-14') AND DATE('2024-12-15')
            
            UNION ALL
            
            SELECT 
                'bundle' as source,
                ba.store_id,
                NULL as country,
                ba.date,
                ba.revenue_total / NULLIF(ba.quantity_sold, 0) as transaction_value,
                100.0 * ba.margin_total / NULLIF(ba.revenue_total, 0) as margin_pct,
                1 as is_bundle,
                0 as needs_alteration,
                ba.bundle_type as product_type,
                NULL as customer_occasion,
                NULL as age_group,
                NULL as satisfaction_score
            FROM {self.tables['bundle_analysis']} ba
            WHERE ba.date BETWEEN DATE('2024-12-14') AND DATE('2024-12-15')
            
            UNION ALL
            
            SELECT 
                'size_fit' as source,
                sf.store_id,
                sf.country,
                sf.date,
                NULL as transaction_value,
                NULL as margin_pct,
                0 as is_bundle,
                sf.alteration_requested as needs_alteration,
                CONCAT(sf.size, '-', sf.fit_type) as product_type,
                NULL as customer_occasion,
                NULL as age_group,
                sf.satisfaction_score
            FROM {self.tables['size_fit']} sf
            WHERE sf.date BETWEEN DATE('2024-12-14') AND DATE('2024-12-15')
        ),
        cross_analysis AS (
            SELECT 
                store_id,
                COUNT(DISTINCT source) as data_sources,
                COUNT(*) as total_transactions,
                AVG(transaction_value) as avg_transaction_value,
                AVG(margin_pct) as avg_margin,
                AVG(satisfaction_score) as avg_satisfaction,
                SUM(is_bundle) as bundle_count,
                SUM(needs_alteration) as alteration_count,
                COUNT(DISTINCT product_type) as product_variety,
                COUNT(DISTINCT customer_occasion) as occasion_variety,
                COUNT(DISTINCT age_group) as demographic_diversity,
                ROUND(100.0 * SUM(is_bundle) / NULLIF(COUNT(*), 0), 1) as bundle_rate,
                ROUND(100.0 * SUM(needs_alteration) / NULLIF(COUNT(*), 0), 1) as alteration_rate
            FROM combined_metrics
            GROUP BY store_id
        )
        SELECT 
            store_id,
            data_sources,
            total_transactions,
            ROUND(avg_transaction_value, 2) as avg_transaction_value,
            ROUND(avg_margin, 1) as avg_margin_pct,
            ROUND(avg_satisfaction, 2) as avg_satisfaction_score,
            bundle_count,
            alteration_count,
            product_variety,
            occasion_variety,
            demographic_diversity,
            bundle_rate,
            alteration_rate,
            
            -- Holistic performance assessment
            CASE 
                WHEN avg_margin >= 60 AND avg_satisfaction >= 4.5 AND alteration_rate <= 15 THEN '⭐ Excellence'
                WHEN avg_margin >= 55 AND avg_satisfaction >= 4.3 THEN '🏆 High Performance'
                WHEN avg_margin >= 50 AND avg_satisfaction >= 4.0 THEN '✅ Good Performance'
                ELSE '📈 Improvement Needed'
            END as overall_performance,
            
            -- Store complexity level
            CASE
                WHEN product_variety >= 20 AND demographic_diversity >= 3 THEN '🌈 High Complexity'
                WHEN product_variety >= 15 THEN '🎯 Moderate Complexity'
                ELSE '📊 Standard Operations'
            END as operational_complexity,
            
            -- Overall health score
            ROUND(
                COALESCE(avg_margin / 100 * 30, 15) +
                COALESCE(avg_satisfaction / 5 * 30, 15) +
                (CASE WHEN bundle_rate >= 50 THEN 15 ELSE bundle_rate / 50 * 15 END) +
                (CASE WHEN alteration_rate <= 20 THEN 15 ELSE 0 END) +
                (product_variety / 2)
            , 1) as store_health_score
            
        FROM cross_analysis
        ORDER BY store_health_score DESC
        LIMIT @limit
        """
        
        params = [
            bigquery.ScalarQueryParameter("limit", "INT64", 100)
        ]
        
        return sql, params
    
    def _build_date_filter(self, context: QueryContext) -> str:
        """Build dynamic date filter based on context"""
        # Default date range
        start_date = "2024-12-14"
        end_date = "2024-12-15"
        
        if "time_period" in context.entities:
            period = context.entities["time_period"]
            if "bugün" in period or "today" in period:
                start_date = end_date = "2024-12-15"
            elif "dün" in period or "yesterday" in period:
                start_date = end_date = "2024-12-14"
            # Add more time period handling as needed
        
        return f"DATE('{start_date}') AND DATE('{end_date}')"

# =======================
# Advanced Analytics Engine
# =======================
class AnalyticsEngine:
    """Advanced analytics with ML-ready insights"""
    
    def __init__(self, metrics: BusinessMetrics):
        self.metrics = metrics
    
    def analyze_results(self, rows: List[Dict], query_type: QueryType, context: QueryContext) -> Dict[str, Any]:
        """Generate advanced insights from query results"""
        
        if not rows:
            return self._empty_results_response(context)
        
        insights = []
        recommendations = []
        alerts = []
        metrics_summary = {}
        
        try:
            if query_type == QueryType.SUIT_SALES:
                analysis = self._analyze_suit_sales(rows)
            elif query_type == QueryType.BUNDLE_ANALYSIS:
                analysis = self._analyze_bundles(rows)
            elif query_type == QueryType.SIZE_ANALYSIS:
                analysis = self._analyze_sizes(rows)
            elif query_type == QueryType.SEASONAL_TRENDS:
                analysis = self._analyze_trends(rows)
            elif query_type == QueryType.CROSS_ANALYSIS:
                analysis = self._analyze_cross_dimensional(rows)
            else:
                analysis = self._generic_analysis(rows)
            
            insights = analysis.get("insights", [])
            recommendations = analysis.get("recommendations", [])
            alerts = analysis.get("alerts", [])
            metrics_summary = analysis.get("metrics", {})
            
            # Add contextual insights based on entities
            if context.entities:
                contextual = self._add_contextual_insights(rows, context)
                insights.extend(contextual)
            
            # Generate predictive insights if applicable
            if len(rows) >= 10:
                predictive = self._generate_predictive_insights(rows, query_type)
                insights.extend(predictive)
            
        except Exception as e:
            logger.error(f"Analysis error: {str(e)}")
            alerts.append(f"Analysis partially completed: {str(e)}")
        
        return {
            "insights": insights[:15],  # More insights
            "recommendations": recommendations[:8],
            "alerts": alerts[:5],
            "metrics_summary": metrics_summary,
            "confidence_score": context.confidence_score,
            "suggested_queries": context.suggested_queries
        }
    
    def _analyze_suit_sales(self, rows: List[Dict]) -> Dict[str, Any]:
        """Deep analysis of suit sales data"""
        insights = []
        recommendations = []
        alerts = []
        metrics = {}
        
        # Performance leaders
        star_performers = [r for r in rows if 'Star Performer' in str(r.get('performance_tier', ''))]
        if star_performers:
            top = star_performers[0]
            insights.append(f"⭐ Star performer: {top['store_id']} - {top['suit_type']} "
                          f"(Revenue: ${top['gross_revenue']:,.0f}, Margin: {top['avg_margin_pct']}%)")
        
        # Country analysis
        countries = {}
        for row in rows:
            country = row.get('country')
            if country:
                if country not in countries:
                    countries[country] = {
                        'revenue': 0, 'units': 0, 'margin_sum': 0, 'count': 0,
                        'alteration_sum': 0
                    }
                countries[country]['revenue'] += row.get('gross_revenue', 0)
                countries[country]['units'] += row.get('units_sold', 0)
                countries[country]['margin_sum'] += row.get('avg_margin_pct', 0)
                countries[country]['alteration_sum'] += row.get('alteration_rate', 0)
                countries[country]['count'] += 1
        
        if countries:
            best_country = max(countries.items(), key=lambda x: x[1]['revenue'])
            insights.append(f"🌍 Top market: {best_country[0]} with ${best_country[1]['revenue']:,.0f} revenue")
            
            for country, data in countries.items():
                avg_margin = data['margin_sum'] / data['count'] if data['count'] > 0 else 0
                avg_alteration = data['alteration_sum'] / data['count'] if data['count'] > 0 else 0
                
                if avg_margin >= self.metrics.margin_premium:
                    insights.append(f"💎 {country} shows premium margin performance ({avg_margin:.1f}%)")
                
                if avg_alteration > self.metrics.alteration_rate_critical:
                    alerts.append(f"🔴 {country} has critical alteration rate ({avg_alteration:.1f}%)")
                    recommendations.append(f"Review sizing standards for {country} market")
        
        # Fit issues detection
        fit_problems = [r for r in rows if 'Critical Fit Problem' in str(r.get('fit_quality', ''))]
        if fit_problems:
            problem_categories = list(set(r['suit_type'] for r in fit_problems))
            alerts.append(f"🔴 Critical fit issues in: {', '.join(problem_categories)}")
            recommendations.append("Conduct urgent fit review for problematic categories")
        
        # Bundle attachment analysis
        low_attach = [r for r in rows if r.get('bundle_attach_rate', 0) < self.metrics.bundle_attach_minimum]
        if low_attach:
            insights.append(f"📉 {len(low_attach)} categories show low bundle attachment (<50%)")
            recommendations.append("Develop targeted bundle promotions for low-attachment categories")
        
        # Opportunity identification
        high_opportunity = [r for r in rows if r.get('improvement_opportunity_score', 0) > 15]
        if high_opportunity:
            top_opp = max(high_opportunity, key=lambda x: x.get('improvement_opportunity_score', 0))
            recommendations.append(f"🎯 Priority improvement: {top_opp['store_id']} - {top_opp['suit_type']} "
                                 f"(Opportunity score: {top_opp['improvement_opportunity_score']})")
        
        # Calculate summary metrics
        total_revenue = sum(r.get('gross_revenue', 0) for r in rows)
        total_units = sum(r.get('units_sold', 0) for r in rows)
        avg_margin = sum(r.get('avg_margin_pct', 0) for r in rows) / len(rows) if rows else 0
        
        metrics = {
            'total_revenue': total_revenue,
            'total_units': total_units,
            'average_margin': round(avg_margin, 1),
            'countries_analyzed': len(countries),
            'star_performers': len(star_performers),
            'fit_issues': len(fit_problems)
        }
        
        return {
            "insights": insights,
            "recommendations": recommendations,
            "alerts": alerts,
            "metrics": metrics
        }
    
    def _analyze_bundles(self, rows: List[Dict]) -> Dict[str, Any]:
        """Deep analysis of bundle performance"""
        insights = []
        recommendations = []
        alerts = []
        
        # Bundle champions
        superstars = [r for r in rows if 'Bundle Superstar' in str(r.get('bundle_status', ''))]
        if superstars:
            insights.append(f"⭐ {len(superstars)} bundle superstars identified")
            for star in superstars[:3]:
                insights.append(f"  • {star['bundle_type']} at {star['store_id']} "
                              f"(Score: {star.get('bundle_effectiveness_score', 0):.1f})")
        
        # Value analysis
        ultra_value = [r for r in rows if 'Ultra Value' in str(r.get('value_tier', ''))]
        if ultra_value:
            total_savings = sum(r.get('customer_value_created', 0) for r in ultra_value)
            insights.append(f"🎁 Ultra value bundles created ${total_savings:,.0f} in customer savings")
        
        # Market coverage
        universal = [r for r in rows if 'Universal Appeal' in str(r.get('market_coverage', ''))]
        if universal:
            insights.append(f"🌟 {len(universal)} bundles have universal market appeal (4+ segments)")
        
        # Margin analysis
        high_margin = [r for r in rows if r.get('margin_pct', 0) >= 40]
        low_margin = [r for r in rows if r.get('margin_pct', 0) < 30]
        
        if high_margin:
            insights.append(f"💎 {len(high_margin)} bundles operate at 40%+ margin")
        
        if low_margin:
            alerts.append(f"⚠️ {len(low_margin)} bundles have margin below 30%")
            recommendations.append("Review pricing strategy for low-margin bundles")
        
        # Effectiveness scoring
        high_effectiveness = [r for r in rows if r.get('bundle_effectiveness_score', 0) > 80]
        if high_effectiveness:
            best = max(high_effectiveness, key=lambda x: x.get('bundle_effectiveness_score', 0))
            insights.append(f"🏆 Most effective bundle: {best['bundle_type']} "
                          f"(Score: {best['bundle_effectiveness_score']:.1f}/100)")
        
        return {
            "insights": insights,
            "recommendations": recommendations,
            "alerts": alerts,
            "metrics": {
                'total_bundles': len(rows),
                'superstars': len(superstars),
                'high_margin_bundles': len(high_margin),
                'universal_appeal': len(universal)
            }
        }
    
    def _analyze_sizes(self, rows: List[Dict]) -> Dict[str, Any]:
        """Deep analysis of size and fit data"""
        insights = []
        recommendations = []
        alerts = []
        
        # Perfect matches
        perfect_matches = [r for r in rows if 'Perfect Match' in str(r.get('demand_status', ''))]
        if perfect_matches:
            insights.append(f"🌟 Perfect size matches found:")
            for match in perfect_matches[:3]:
                insights.append(f"  • Size {match['size']} {match['fit_type']} "
                              f"(Score: {match.get('size_quality_score', 0):.1f})")
        
        # Fit issues
        critical_fit = [r for r in rows if 'Critical Fit Issues' in str(r.get('fit_assessment', ''))]
        if critical_fit:
            problem_sizes = [f"{r['size']}-{r['fit_type']}" for r in critical_fit]
            alerts.append(f"🔴 Critical fit issues: {', '.join(problem_sizes[:5])}")
            recommendations.append("Urgent pattern review needed for problematic sizes")
        
        # Return risk
        high_returns = [r for r in rows if 'High Return Risk' in str(r.get('return_risk', ''))]
        if high_returns:
            total_returns = sum(r.get('total_returns', 0) for r in high_returns)
            alerts.append(f"🔴 High return risk: {total_returns} returns from problematic sizes")
            recommendations.append("Implement enhanced size guides and virtual fitting")
        
        # Satisfaction analysis
        exceptional_sat = [r for r in rows if 'Exceptional Satisfaction' in str(r.get('satisfaction_tier', ''))]
        low_sat = [r for r in rows if 'Low Satisfaction' in str(r.get('satisfaction_tier', ''))]
        
        if exceptional_sat:
            insights.append(f"⭐ {len(exceptional_sat)} size/fit combinations show exceptional satisfaction")
        
        if low_sat:
            alerts.append(f"📉 {len(low_sat)} size/fit combinations have low satisfaction")
            recommendations.append("Customer feedback review for low-satisfaction sizes")
        
        # Body type insights
        body_types = {}
        for row in rows:
            body_type = row.get('typical_body_type', 'Unknown')
            if body_type not in body_types:
                body_types[body_type] = []
            body_types[body_type].append(row)
        
        for body_type, type_rows in body_types.items():
            if len(type_rows) >= 3:
                avg_alteration = sum(r.get('alteration_rate', 0) for r in type_rows) / len(type_rows)
                insights.append(f"👤 {body_type}: {len(type_rows)} sizes, "
                              f"{avg_alteration:.1f}% alteration rate")
        
        return {
            "insights": insights,
            "recommendations": recommendations,
            "alerts": alerts,
            "metrics": {
                'perfect_matches': len(perfect_matches),
                'critical_fit_issues': len(critical_fit),
                'high_return_risk': len(high_returns),
                'body_types_analyzed': len(body_types)
            }
        }
    
    def _analyze_trends(self, rows: List[Dict]) -> Dict[str, Any]:
        """Deep analysis of seasonal trends"""
        insights = []
        recommendations = []
        alerts = []
        
        # Demand analysis
        explosive = [r for r in rows if 'Explosive Demand' in str(r.get('demand_level', ''))]
        if explosive:
            insights.append(f"🚀 Explosive demand detected:")
            for item in explosive[:3]:
                insights.append(f"  • {item['occasion_type']} in {item['country']} "
                              f"(Index: {item['avg_demand_index']})")
            recommendations.append("Maximize inventory for explosive demand categories")
        
        # Market type analysis
        premium_markets = [r for r in rows if 'Premium Market' in str(r.get('market_type', ''))]
        price_sensitive = [r for r in rows if 'Price Sensitive' in str(r.get('market_type', ''))]
        
        if premium_markets:
            premium_countries = list(set(r['country'] for r in premium_markets))
            insights.append(f"💎 Premium markets: {', '.join(premium_countries)}")
            recommendations.append("Focus on quality and exclusivity in premium markets")
        
        if price_sensitive:
            sensitive_countries = list(set(r['country'] for r in price_sensitive))
            insights.append(f"💸 Price-sensitive markets: {', '.join(sensitive_countries)}")
            recommendations.append("Develop value bundles for price-sensitive segments")
        
        # Fashion diversity
        high_fashion = [r for r in rows if 'High Fashion Diversity' in str(r.get('product_diversity', ''))]
        if high_fashion:
            insights.append(f"🎨 {len(high_fashion)} categories show high fashion diversity")
            
        # Trending colors and fabrics
        all_colors = []
        all_fabrics = []
        for row in rows:
            if row.get('trending_colors'):
                all_colors.extend(row['trending_colors'].split(', '))
            if row.get('trending_fabrics'):
                all_fabrics.extend(row['trending_fabrics'].split(', '))
        
        if all_colors:
            from collections import Counter
            color_counts = Counter(all_colors)
            top_colors = color_counts.most_common(3)
            insights.append(f"🎨 Trending colors: {', '.join([c[0] for c in top_colors])}")
        
        if all_fabrics:
            fabric_counts = Counter(all_fabrics)
            top_fabrics = fabric_counts.most_common(3)
            insights.append(f"🧵 Trending fabrics: {', '.join([f[0] for f in top_fabrics])}")
        
        # Market stability
        volatile = [r for r in rows if 'Volatile' in str(r.get('market_stability', ''))]
        if volatile:
            alerts.append(f"⚡ {len(volatile)} volatile market segments detected")
            recommendations.append("Implement flexible inventory strategies for volatile segments")
        
        # Event influence
        event_driven = [r for r in rows if 'Event Driven' in str(r.get('event_influence', ''))]
        if event_driven:
            insights.append(f"🎉 {len(event_driven)} categories are event-driven")
            recommendations.append("Align marketing campaigns with cultural events")
        
        return {
            "insights": insights,
            "recommendations": recommendations,
            "alerts": alerts,
            "metrics": {
                'explosive_demand': len(explosive),
                'premium_markets': len(premium_markets),
                'price_sensitive': len(price_sensitive),
                'event_driven': len(event_driven)
            }
        }
    
    def _analyze_cross_dimensional(self, rows: List[Dict]) -> Dict[str, Any]:
        """Cross-dimensional analysis insights"""
        insights = []
        recommendations = []
        alerts = []
        
        # Excellence identification
        excellence = [r for r in rows if 'Excellence' in str(r.get('overall_performance', ''))]
        if excellence:
            insights.append(f"⭐ {len(excellence)} stores showing excellence")
            for store in excellence[:3]:
                insights.append(f"  • {store['store_id']} (Health Score: {store.get('store_health_score', 0):.1f})")
        
        # Complexity analysis
        high_complexity = [r for r in rows if 'High Complexity' in str(r.get('operational_complexity', ''))]
        if high_complexity:
            insights.append(f"🌈 {len(high_complexity)} stores manage high complexity operations")
            recommendations.append("Provide additional support for high-complexity stores")
        
        # Health scoring
        if rows:
            avg_health = sum(r.get('store_health_score', 0) for r in rows) / len(rows)
            insights.append(f"📊 Average store health score: {avg_health:.1f}/100")
            
            low_health = [r for r in rows if r.get('store_health_score', 0) < 50]
            if low_health:
                alerts.append(f"🔴 {len(low_health)} stores have health scores below 50")
                recommendations.append("Immediate intervention needed for low-health stores")

class ResponseFormatter:
    """Format responses for different audiences and languages"""
    
    def __init__(self):
        self.templates = self._initialize_templates()
    
    def _initialize_templates(self) -> Dict[str, Dict[str, str]]:
        return {
            "tr": {
                "executive_summary": "🎯 YÖNETİCİ ÖZETİ",
                "key_findings": "📊 ANA BULGULAR",
                "recommendations": "💡 ÖNERİLER",
                "alerts": "⚠️ UYARILAR",
                "metrics": "📈 METRİKLER",
                "next_steps": "➡️ SONRAKİ ADIMLAR"
            },
            "en": {
                "executive_summary": "🎯 EXECUTIVE SUMMARY",
                "key_findings": "📊 KEY FINDINGS",
                "recommendations": "💡 RECOMMENDATIONS",
                "alerts": "⚠️ ALERTS",
                "metrics": "📈 METRICS",
                "next_steps": "➡️ NEXT STEPS"
            }
        }
    
    def format_response(self, analysis: Dict, context: QueryContext, summary: Dict) -> Dict[str, Any]:
        """Format the complete response"""
        
        lang = context.user_language
        templates = self.templates.get(lang, self.templates["en"])
        
        # Build formatted sections
        formatted_insights = self._format_insights(
            analysis.get("insights", []),
            templates["key_findings"]
        )
        
        formatted_recommendations = self._format_recommendations(
            analysis.get("recommendations", []),
            templates["recommendations"]
        )
        
        formatted_alerts = self._format_alerts(
            analysis.get("alerts", []),
            templates["alerts"]
        )
        
        # Add executive summary if high confidence
        executive_summary = ""
        if context.confidence_score >= 0.7 and analysis.get("metrics_summary"):
            executive_summary = self._create_executive_summary(
                analysis["metrics_summary"],
                templates["executive_summary"],
                lang
            )
        
        # Combine all formatted sections
        formatted_response = {
            "executive_summary": executive_summary,
            "insights": formatted_insights,
            "recommendations": formatted_recommendations,
            "alerts": formatted_alerts,
            "metrics_summary": analysis.get("metrics_summary", {}),
            "confidence_score": context.confidence_score,
            "suggested_queries": context.suggested_queries,
            "query_interpretation": {
                "detected_intent": context.detected_intent.value,
                "detected_entities": context.entities,
                "language": context.user_language
            }
        }
        
        return formatted_response
    
    def _format_insights(self, insights: List[str], header: str) -> List[str]:
        """Format insights section"""
        if not insights:
            return []
        
        formatted = [header]
        formatted.extend(insights)
        return formatted
    
    def _format_recommendations(self, recommendations: List[str], header: str) -> List[str]:
        """Format recommendations with priority"""
        if not recommendations:
            return []
        
        formatted = [header]
        for i, rec in enumerate(recommendations, 1):
            priority = "🔴" if i <= 2 else "🟡" if i <= 5 else "🟢"
            formatted.append(f"{priority} {rec}")
        return formatted
    
    def _format_alerts(self, alerts: List[str], header: str) -> List[str]:
        """Format alerts section"""
        if not alerts:
            return []
        
        formatted = [header]
        formatted.extend(alerts)
        return formatted
    
    def _create_executive_summary(self, metrics: Dict, header: str, lang: str) -> str:
        """Create executive summary from metrics"""
        summary_parts = [header]
        
        if "total_revenue" in metrics:
            revenue = metrics["total_revenue"]
            summary_parts.append(
                f"Total Revenue: ${revenue:,.0f}" if lang == "en" 
                else f"Toplam Gelir: {revenue:,.0f} TL"
            )
        
        if "total_units" in metrics:
            units = metrics["total_units"]
            summary_parts.append(
                f"Units Sold: {units:,}" if lang == "en"
                else f"Satılan Adet: {units:,}"
            )
        
        if "average_margin" in metrics:
            margin = metrics["average_margin"]
            summary_parts.append(
                f"Average Margin: {margin:.1f}%" if lang == "en"
                else f"Ortalama Kar Marjı: %{margin:.1f}"
            )
        
        return " | ".join(summary_parts)

# =======================
# Main HTTP Handler - CRITICAL: This MUST be at the end
# =======================
@functions_framework.http
def formal_wear_query(request):
    """Advanced Suvari Analytics Engine - Main Entry Point"""
    
    start_time = datetime.utcnow()
    query_type = None
    
    try:
        # Parse request
        body = request.get_json(silent=True) or {}
        question = body.get("question", "")
        limit = min(int(body.get("limit", 100)), 500)
        
        # Initialize components
        metrics = BusinessMetrics()
        detector = IntentDetector()
        builder = QueryBuilder(metrics)
        engine = AnalyticsEngine(metrics)
        formatter = ResponseFormatter()
        
        # Detect intent and extract context
        context = detector.detect(question)
        
        # Override with explicit query_type if provided
        if body.get("query_type"):
            try:
                context.detected_intent = QueryType(body["query_type"])
                context.confidence_score = 1.0
            except ValueError:
                pass
        
        query_type = context.detected_intent
        
        logger.info(f"Processing query - Type: {query_type.value}, Confidence: {context.confidence_score:.2f}")
        logger.info(f"Detected entities: {context.entities}")
        
        # Build appropriate query
        if query_type == QueryType.SUIT_SALES:
            sql, params = builder.build_suit_sales_query(context)
        elif query_type == QueryType.BUNDLE_ANALYSIS:
            sql, params = builder.build_bundle_analysis_query(context)
        elif query_type == QueryType.SIZE_ANALYSIS:
            sql, params = builder.build_size_analysis_query(context)
        elif query_type == QueryType.SEASONAL_TRENDS:
            sql, params = builder.build_seasonal_trends_query(context)
        elif query_type == QueryType.CROSS_ANALYSIS:
            sql, params = builder.build_cross_analysis_query(context)
        else:
            sql, params = builder.build_suit_sales_query(context)
        
        # Update limit parameter
        for i, param in enumerate(params):
            if param.name == "limit":
                params[i] = bigquery.ScalarQueryParameter("limit", "INT64", limit)
        
        # Execute query
        client = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)
        job_config = bigquery.QueryJobConfig(
            query_parameters=params,
            maximum_bytes_billed=10 * 1024 * 1024 * 1024,  # 10GB limit
            labels={
                "agent": "suvari_advanced",
                "query_type": query_type.value,
                "confidence": str(int(context.confidence_score * 100))
            }
        )
        
        job = client.query(sql, job_config=job_config)
        rows = [dict(r) for r in job.result()]
        
        # Analyze results
        analysis = engine.analyze_results(rows, query_type, context)
        
        # Calculate summary
        summary = {
            "total_records": len(rows),
            "query_type": query_type.value,
            "execution_time_ms": int((datetime.utcnow() - start_time).total_seconds() * 1000),
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Format response
        formatted = formatter.format_response(analysis, context, summary)
        
        # Build final response
        response = {
            "success": True,
            "agent": "suvari_advanced_analytics",
            "version": "4.0",
            "query_type": query_type.value,
            "parameters": {
                "question": question,
                "limit": limit,
                "detected_entities": context.entities
            },
            "summary": summary,
            "insights": formatted["insights"],
            "recommendations": formatted["recommendations"],
            "alerts": formatted["alerts"],
            "metrics_summary": formatted["metrics_summary"],
            "confidence_score": formatted["confidence_score"],
            "suggested_queries": formatted["suggested_queries"],
            "query_interpretation": formatted["query_interpretation"],
            "row_count": len(rows),
            "rows": rows,
            "metadata": {
                "bytes_processed": job.total_bytes_processed,
                "bytes_billed": job.total_bytes_billed,
                "cache_hit": job.cache_hit,
                "slot_millis": job.slot_millis if hasattr(job, 'slot_millis') else None,
                "dataset": DATASET,
                "location": BQ_LOCATION,
                "execution_time_ms": int((datetime.utcnow() - start_time).total_seconds() * 1000)
            }
        }
        
        # Log success
        logger.info(f"Query completed successfully - {len(rows)} rows, "
                   f"{job.total_bytes_processed/1024/1024:.2f}MB processed")
        
        return (json.dumps(response, ensure_ascii=False, default=str), 
                200, 
                {"Content-Type": "application/json; charset=utf-8"})
        
    except Exception as e:
        logger.exception(f"Query failed: {str(e)}")
        
        error_response = {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__,
            "agent": "suvari_advanced_analytics",
            "version": "4.0",
            "query_type": query_type.value if query_type else "unknown",
            "timestamp": datetime.utcnow().isoformat(),
            "suggestion": "Please check your query syntax or try a different question"
        }
        
        return (json.dumps(error_response, ensure_ascii=False), 
                500, 
                {"Content-Type": "application/json; charset=utf-8"})
