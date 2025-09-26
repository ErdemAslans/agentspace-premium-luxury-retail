# main.py - Suvari Franchise Operations Analytics
# Functions Framework based Cloud Function
# Version: 2.0 - Production Ready

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
DATASET = "suvari_franchise_ops"

# Corrected Table References (matching BigQuery exactly)
TABLES = {
    "partner_performance": f"`{PROJECT_ID}.{DATASET}.partner_performance`",
    "store_network": f"`{PROJECT_ID}.{DATASET}.store_network`", 
    "expansion_pipeline": f"`{PROJECT_ID}.{DATASET}.expansion_pipeline`",
    "support_tickets": f"`{PROJECT_ID}.{DATASET}.support_tickets`"
}

# Business Constants
@dataclass
class SuvariFranchiseMetrics:
    # Partner and performance tiers from actual data
    PERFORMANCE_TIERS = ['A+', 'A', 'B+', 'B', 'C']
    PARTNER_TYPES = ['Owned', 'Master Franchise', 'International Franchise', 'Regional Franchise']
    REGIONS = ['Marmara', 'Central Anatolia', 'Central Europe', 'Eastern Europe', 'Aegean', 'Central Asia', 'Mediterranean', 'Southeastern Europe']
    
    # Performance thresholds
    PERFORMANCE_SCORE_EXCELLENT = 95.0
    COMPLIANCE_SCORE_TARGET = 95.0
    SATISFACTION_TARGET = 4.5
    REVENUE_ACHIEVEMENT_TARGET = 100.0
    SUPPORT_RESOLUTION_TARGET = 24.0  # hours
    
    # Expansion criteria
    FEASIBILITY_HIGH_THRESHOLD = 8.5
    ROI_EXCELLENT_THRESHOLD = 20.0
    INVESTMENT_MAJOR_THRESHOLD = 500000.0  # USD
    
    # Support metrics
    TICKET_RESOLUTION_EXCELLENT = 95.0  # %
    CRITICAL_ISSUE_THRESHOLD = 3

logger = logging.getLogger("suvari-franchise-ops")
logging.basicConfig(level=logging.INFO)

# =======================
# Query Intent Detection
# =======================
def detect_query_intent(question: str) -> str:
    """Detect franchise operations query intent"""
    
    q = (question or "").lower()
    
    patterns = {
        "partner_performance": [
            r"partner|franchise|başarı|performance", r"evaluation|değerlendirme", r"score|puan",
            r"revenue|gelir", r"compliance|uygunluk", r"satisfaction|memnuniyet"
        ],
        "store_network": [
            r"network|ağ|mağaza", r"coverage|kapsam", r"region|bölge", 
            r"distribution|dağılım", r"market|pazar", r"geographic|coğrafi"
        ],
        "expansion_analysis": [
            r"expansion|genişleme|büyüme", r"pipeline|hat", r"opportunity|fırsat",
            r"investment|yatırım", r"feasibility|fizibilite", r"roi|kar"
        ],
        "support_tickets": [
            r"support|destek|ticket", r"issue|sorun|problem", r"resolution|çözüm",
            r"help|yardım", r"technical|teknik", r"escalation|yükseltme"
        ]
    }
    
    for intent, pattern_list in patterns.items():
        for pattern in pattern_list:
            if any(word in q for word in pattern.split("|")):
                logger.info(f"Detected intent: {intent}")
                return intent
    
    return "partner_performance"  # Default

# =======================
# SQL Query Functions
# =======================

def sql_partner_performance() -> Tuple[str, List]:
    """Comprehensive franchise partner performance analysis"""
    
    sql = f"""
    WITH partner_metrics AS (
        SELECT 
            pp.evaluation_date,
            pp.store_id,
            pp.franchise_partner,
            pp.performance_score,
            pp.revenue_achievement_pct,
            pp.inventory_management,
            pp.customer_satisfaction,
            pp.staff_training_score,
            pp.marketing_compliance,
            pp.brand_standards,
            pp.financial_health,
            pp.support_requests,
            pp.critical_issues,
            pp.improvement_areas
        FROM {TABLES['partner_performance']} pp
        WHERE pp.evaluation_date BETWEEN DATE('2024-12-14') AND DATE('2024-12-15')
    ),
    partner_summary AS (
        SELECT 
            franchise_partner,
            COUNT(DISTINCT store_id) as stores_managed,
            AVG(performance_score) as avg_performance_score,
            AVG(revenue_achievement_pct) as avg_revenue_achievement,
            AVG(customer_satisfaction) as avg_customer_satisfaction,
            AVG(staff_training_score) as avg_training_score,
            AVG(marketing_compliance) as avg_marketing_compliance,
            AVG(brand_standards) as avg_brand_standards,
            AVG(financial_health) as avg_financial_health,
            SUM(support_requests) as total_support_requests,
            SUM(critical_issues) as total_critical_issues,
            COUNT(DISTINCT improvement_areas) as improvement_areas_count,
            -- Calculate performance tier
            CASE 
                WHEN AVG(performance_score) >= 95.0 THEN 'A+'
                WHEN AVG(performance_score) >= 90.0 THEN 'A'
                WHEN AVG(performance_score) >= 85.0 THEN 'B+'
                WHEN AVG(performance_score) >= 80.0 THEN 'B'
                ELSE 'C'
            END as calculated_tier
        FROM partner_metrics
        GROUP BY franchise_partner
    ),
    partner_rankings AS (
        SELECT 
            *,
            RANK() OVER (ORDER BY avg_performance_score DESC) as performance_rank,
            RANK() OVER (ORDER BY avg_revenue_achievement DESC) as revenue_rank,
            RANK() OVER (ORDER BY avg_customer_satisfaction DESC) as satisfaction_rank
        FROM partner_summary
    )
    SELECT 
        pm.store_id,
        pm.franchise_partner,
        pm.performance_score,
        pm.revenue_achievement_pct,
        pm.customer_satisfaction,
        pm.staff_training_score,
        pm.marketing_compliance,
        pm.brand_standards,
        pm.financial_health,
        pm.support_requests,
        pm.critical_issues,
        pm.improvement_areas,
        ps.stores_managed,
        ROUND(ps.avg_performance_score, 1) as avg_performance_score,
        ROUND(ps.avg_revenue_achievement, 1) as avg_revenue_achievement,
        ROUND(ps.avg_customer_satisfaction, 1) as avg_customer_satisfaction,
        ps.total_support_requests,
        ps.total_critical_issues,
        ps.calculated_tier,
        ps.performance_rank,
        ps.revenue_rank,
        ps.satisfaction_rank,
        -- Performance assessments
        CASE 
            WHEN ps.avg_performance_score >= {SuvariFranchiseMetrics.PERFORMANCE_SCORE_EXCELLENT} THEN '🏆 Excellent Partner'
            WHEN ps.avg_performance_score >= 90.0 THEN '✅ Good Partner'
            WHEN ps.avg_performance_score >= 85.0 THEN '📊 Average Partner'
            ELSE '⚠️ Needs Improvement'
        END as partner_tier_assessment,
        CASE
            WHEN ps.avg_revenue_achievement >= {SuvariFranchiseMetrics.REVENUE_ACHIEVEMENT_TARGET} THEN '💰 Revenue Target Met'
            WHEN ps.avg_revenue_achievement >= 90.0 THEN '📈 Good Revenue Performance'
            ELSE '🔴 Revenue Below Target'
        END as revenue_performance,
        CASE
            WHEN ps.total_critical_issues = 0 THEN '✅ No Critical Issues'
            WHEN ps.total_critical_issues <= {SuvariFranchiseMetrics.CRITICAL_ISSUE_THRESHOLD} THEN '⚠️ Minor Issues'
            ELSE '🔴 Multiple Critical Issues'
        END as issue_status,
        CASE
            WHEN ps.avg_customer_satisfaction >= {SuvariFranchiseMetrics.SATISFACTION_TARGET} THEN '⭐ High Customer Satisfaction'
            WHEN ps.avg_customer_satisfaction >= 4.0 THEN '✅ Good Customer Satisfaction'
            ELSE '📈 Satisfaction Improvement Needed'
        END as satisfaction_level
    FROM partner_metrics pm
    JOIN partner_summary ps ON pm.franchise_partner = ps.franchise_partner
    JOIN partner_rankings pr ON ps.franchise_partner = pr.franchise_partner
    ORDER BY ps.avg_performance_score DESC, pm.store_id
    LIMIT @limit
    """
    
    params = [
        bigquery.ScalarQueryParameter("limit", "INT64", None)
    ]
    
    return sql, params

def sql_store_network() -> Tuple[str, List]:
    """Store network coverage and regional analysis"""
    
    sql = f"""
    WITH network_overview AS (
        SELECT 
            sn.store_id,
            sn.country,
            sn.city,
            sn.region,
            sn.franchise_partner,
            sn.partner_type,
            sn.open_date,
            sn.store_size_m2,
            sn.monthly_rent_usd,
            sn.performance_tier,
            sn.compliance_score,
            sn.local_manager,
            sn.contract_end,
            sn.expansion_potential,
            sn.market_maturity,
            -- Calculate operational metrics
            DATE_DIFF(CURRENT_DATE(), PARSE_DATE('%Y-%m-%d', sn.open_date), DAY) / 365.25 as years_operating,
            DATE_DIFF(PARSE_DATE('%Y-%m-%d', sn.contract_end), CURRENT_DATE(), DAY) / 365.25 as years_remaining_contract
        FROM {TABLES['store_network']} sn
    ),
    regional_metrics AS (
        SELECT 
            region,
            country,
            COUNT(*) as stores_in_region,
            AVG(store_size_m2) as avg_store_size,
            AVG(monthly_rent_usd) as avg_monthly_rent,
            AVG(compliance_score) as avg_compliance_score,
            COUNT(CASE WHEN performance_tier IN ('A+', 'A') THEN 1 END) as high_performing_stores,
            COUNT(CASE WHEN expansion_potential = 'High' THEN 1 END) as high_expansion_potential,
            COUNT(DISTINCT franchise_partner) as unique_partners,
            COUNT(CASE WHEN years_remaining_contract < 1 THEN 1 END) as contracts_expiring_soon
        FROM network_overview
        GROUP BY region, country
    ),
    network_rankings AS (
        SELECT 
            *,
            RANK() OVER (ORDER BY avg_compliance_score DESC) as compliance_rank,
            RANK() OVER (ORDER BY stores_in_region DESC) as region_size_rank,
            RANK() OVER (ORDER BY high_performing_stores DESC) as performance_rank
        FROM regional_metrics
    )
    SELECT 
        no.store_id,
        no.country,
        no.city,
        no.region,
        no.franchise_partner,
        no.partner_type,
        no.open_date,
        no.store_size_m2,
        no.monthly_rent_usd,
        no.performance_tier,
        no.compliance_score,
        no.local_manager,
        no.contract_end,
        no.expansion_potential,
        no.market_maturity,
        ROUND(no.years_operating, 1) as years_operating,
        ROUND(no.years_remaining_contract, 1) as years_remaining_contract,
        rm.stores_in_region,
        ROUND(rm.avg_store_size, 0) as regional_avg_store_size,
        ROUND(rm.avg_monthly_rent, 0) as regional_avg_rent,
        ROUND(rm.avg_compliance_score, 1) as regional_avg_compliance,
        rm.high_performing_stores,
        rm.high_expansion_potential,
        rm.unique_partners,
        rm.contracts_expiring_soon,
        nr.compliance_rank,
        nr.region_size_rank,
        nr.performance_rank,
        -- Performance assessments
        CASE 
            WHEN no.compliance_score >= {SuvariFranchiseMetrics.COMPLIANCE_SCORE_TARGET} THEN '✅ Fully Compliant'
            WHEN no.compliance_score >= 90.0 THEN '⚠️ Minor Compliance Issues'
            ELSE '🔴 Compliance Attention Needed'
        END as compliance_status,
        CASE
            WHEN no.performance_tier IN ('A+', 'A') THEN '🏆 Top Performer'
            WHEN no.performance_tier IN ('B+', 'B') THEN '📊 Standard Performer'
            ELSE '📈 Performance Improvement Needed'
        END as performance_classification,
        CASE
            WHEN no.years_remaining_contract < 1 THEN '⏰ Contract Renewal Urgent'
            WHEN no.years_remaining_contract < 2 THEN '📅 Contract Renewal Due Soon'
            ELSE '✅ Contract Status Stable'
        END as contract_status,
        CASE
            WHEN no.expansion_potential = 'High' AND no.performance_tier IN ('A+', 'A') THEN '🚀 Prime Expansion Candidate'
            WHEN no.expansion_potential = 'High' THEN '📈 Good Expansion Potential'
            WHEN no.expansion_potential = 'Medium' THEN '📊 Moderate Expansion Potential'
            ELSE '📉 Limited Expansion Potential'
        END as expansion_assessment
    FROM network_overview no
    JOIN regional_metrics rm ON no.region = rm.region AND no.country = rm.country
    JOIN network_rankings nr ON rm.region = nr.region AND rm.country = nr.country
    ORDER BY no.performance_tier, no.compliance_score DESC, no.store_id
    LIMIT @limit
    """
    
    params = [
        bigquery.ScalarQueryParameter("limit", "INT64", None)
    ]
    
    return sql, params

def sql_expansion_analysis() -> Tuple[str, List]:
    """Expansion pipeline and market opportunity analysis"""
    
    sql = f"""
    WITH expansion_pipeline AS (
        SELECT 
            ep.pipeline_id,
            ep.target_country,
            ep.target_city,
            ep.market_size,
            ep.demographics_score,
            ep.competition_level,
            ep.rental_cost_usd,
            ep.partner_interest,
            ep.feasibility_score,
            ep.timeline_months,
            ep.investment_required_usd,
            ep.roi_projection_pct,
            ep.risk_level,
            ep.priority_rank
        FROM {TABLES['expansion_pipeline']} ep
    ),
    market_analysis AS (
        SELECT 
            target_country,
            COUNT(*) as opportunities_identified,
            AVG(feasibility_score) as avg_feasibility_score,
            SUM(investment_required_usd) as total_investment_needed,
            AVG(roi_projection_pct) as avg_roi_projection,
            AVG(timeline_months) as avg_timeline_months,
            COUNT(CASE WHEN feasibility_score >= {SuvariFranchiseMetrics.FEASIBILITY_HIGH_THRESHOLD} THEN 1 END) as high_feasibility_opportunities,
            COUNT(CASE WHEN roi_projection_pct >= {SuvariFranchiseMetrics.ROI_EXCELLENT_THRESHOLD} THEN 1 END) as high_roi_opportunities,
            COUNT(CASE WHEN investment_required_usd >= {SuvariFranchiseMetrics.INVESTMENT_MAJOR_THRESHOLD} THEN 1 END) as major_investments,
            MIN(timeline_months) as fastest_timeline,
            MAX(roi_projection_pct) as best_roi_opportunity
        FROM expansion_pipeline
        GROUP BY target_country
    ),
    opportunity_rankings AS (
        SELECT 
            ep.*,
            ma.opportunities_identified,
            ma.avg_feasibility_score,
            ma.total_investment_needed,
            ma.avg_roi_projection,
            ma.avg_timeline_months,
            ma.high_feasibility_opportunities,
            ma.high_roi_opportunities,
            ma.major_investments,
            ma.fastest_timeline,
            ma.best_roi_opportunity,
            RANK() OVER (ORDER BY ep.feasibility_score DESC, ep.roi_projection_pct DESC) as overall_opportunity_rank,
            RANK() OVER (PARTITION BY ep.target_country ORDER BY ep.feasibility_score DESC) as country_opportunity_rank
        FROM expansion_pipeline ep
        JOIN market_analysis ma ON ep.target_country = ma.target_country
    )
    SELECT 
        pipeline_id,
        target_country,
        target_city,
        market_size,
        demographics_score,
        competition_level,
        rental_cost_usd,
        partner_interest,
        feasibility_score,
        timeline_months,
        investment_required_usd,
        roi_projection_pct,
        risk_level,
        priority_rank,
        opportunities_identified,
        ROUND(avg_feasibility_score, 1) as country_avg_feasibility,
        ROUND(total_investment_needed, 0) as country_total_investment,
        ROUND(avg_roi_projection, 1) as country_avg_roi,
        ROUND(avg_timeline_months, 1) as country_avg_timeline,
        high_feasibility_opportunities,
        high_roi_opportunities,
        major_investments,
        fastest_timeline,
        ROUND(best_roi_opportunity, 1) as country_best_roi,
        overall_opportunity_rank,
        country_opportunity_rank,
        -- Opportunity assessments
        CASE 
            WHEN feasibility_score >= {SuvariFranchiseMetrics.FEASIBILITY_HIGH_THRESHOLD} AND roi_projection_pct >= {SuvariFranchiseMetrics.ROI_EXCELLENT_THRESHOLD} THEN '🚀 Premium Opportunity'
            WHEN feasibility_score >= 7.5 AND roi_projection_pct >= 15.0 THEN '⭐ High Priority Opportunity'
            WHEN feasibility_score >= 6.5 AND roi_projection_pct >= 10.0 THEN '📊 Medium Priority Opportunity'
            ELSE '📉 Low Priority Opportunity'
        END as opportunity_classification,
        CASE
            WHEN investment_required_usd >= {SuvariFranchiseMetrics.INVESTMENT_MAJOR_THRESHOLD} THEN '💰 Major Investment Required'
            WHEN investment_required_usd >= 250000 THEN '💸 Significant Investment'
            WHEN investment_required_usd >= 100000 THEN '💵 Standard Investment'
            ELSE '💳 Low Investment Required'
        END as investment_level,
        CASE
            WHEN timeline_months <= 6 THEN '⚡ Fast Track (6 months)'
            WHEN timeline_months <= 12 THEN '🏃 Standard Timeline (12 months)'
            WHEN timeline_months <= 18 THEN '🚶 Extended Timeline (18 months)'
            ELSE '🐌 Long-term Project (18+ months)'
        END as timeline_assessment,
        CASE
            WHEN risk_level = 'Low' AND feasibility_score >= 8.0 THEN '💎 Low Risk High Reward'
            WHEN risk_level = 'Low' THEN '✅ Low Risk Investment'
            WHEN risk_level = 'Medium' THEN '⚠️ Moderate Risk'
            ELSE '🔴 High Risk Investment'
        END as risk_assessment
    FROM opportunity_rankings
    ORDER BY feasibility_score DESC, roi_projection_pct DESC
    LIMIT @limit
    """
    
    params = [
        bigquery.ScalarQueryParameter("limit", "INT64", None)
    ]
    
    return sql, params

def sql_support_tickets() -> Tuple[str, List]:
    """Support ticket analysis and resolution performance"""
    
    sql = f"""
    WITH support_analysis AS (
        SELECT 
            st.ticket_date,
            st.store_id,
            st.franchise_partner,
            st.ticket_category,
            st.priority_level,
            st.issue_description,
            st.resolution_time_hours,
            st.satisfaction_rating,
            st.resolved_flag,
            st.escalation_required,
            st.support_agent,
            st.follow_up_needed,
            st.cost_impact_usd,
            st.business_impact_level
        FROM {TABLES['support_tickets']} st
        WHERE st.ticket_date BETWEEN DATE('2024-12-14') AND DATE('2024-12-15')
    ),
    support_summary AS (
        SELECT 
            franchise_partner,
            COUNT(*) as total_tickets,
            AVG(resolution_time_hours) as avg_resolution_time_hours,
            AVG(satisfaction_rating) as avg_satisfaction_rating,
            COUNT(CASE WHEN resolved_flag = 'TRUE' THEN 1 END) as resolved_tickets,
            COUNT(CASE WHEN priority_level = 'High' THEN 1 END) as high_priority_tickets,
            COUNT(CASE WHEN priority_level = 'Critical' THEN 1 END) as critical_tickets,
            COUNT(CASE WHEN escalation_required = 'TRUE' THEN 1 END) as escalated_tickets,
            SUM(cost_impact_usd) as total_cost_impact,
            COUNT(DISTINCT ticket_category) as issue_categories,
            COUNT(DISTINCT support_agent) as agents_involved,
            -- Calculate resolution rate
            ROUND(COUNT(CASE WHEN resolved_flag = 'TRUE' THEN 1 END) * 100.0 / COUNT(*), 1) as resolution_rate_pct
        FROM support_analysis
        GROUP BY franchise_partner
    ),
    support_rankings AS (
        SELECT 
            *,
            RANK() OVER (ORDER BY resolution_rate_pct DESC, avg_resolution_time_hours ASC) as support_performance_rank,
            RANK() OVER (ORDER BY avg_satisfaction_rating DESC) as satisfaction_rank,
            RANK() OVER (ORDER BY total_cost_impact ASC) as cost_efficiency_rank
        FROM support_summary
    )
    SELECT 
        sa.store_id,
        sa.franchise_partner,
        sa.ticket_category,
        sa.priority_level,
        sa.issue_description,
        sa.resolution_time_hours,
        sa.satisfaction_rating,
        sa.resolved_flag,
        sa.escalation_required,
        sa.support_agent,
        sa.follow_up_needed,
        sa.cost_impact_usd,
        sa.business_impact_level,
        ss.total_tickets,
        ROUND(ss.avg_resolution_time_hours, 1) as partner_avg_resolution_time,
        ROUND(ss.avg_satisfaction_rating, 1) as partner_avg_satisfaction,
        ss.resolved_tickets,
        ss.high_priority_tickets,
        ss.critical_tickets,
        ss.escalated_tickets,
        ROUND(ss.total_cost_impact, 2) as partner_total_cost_impact,
        ss.issue_categories,
        ss.agents_involved,
        ss.resolution_rate_pct,
        sr.support_performance_rank,
        sr.satisfaction_rank,
        sr.cost_efficiency_rank,
        -- Performance assessments
        CASE 
            WHEN ss.avg_resolution_time_hours <= {SuvariFranchiseMetrics.SUPPORT_RESOLUTION_TARGET} AND ss.resolution_rate_pct >= {SuvariFranchiseMetrics.TICKET_RESOLUTION_EXCELLENT} THEN '🏆 Excellent Support Performance'
            WHEN ss.avg_resolution_time_hours <= 48 AND ss.resolution_rate_pct >= 90.0 THEN '✅ Good Support Performance'
            WHEN ss.avg_resolution_time_hours <= 72 THEN '📊 Standard Support Performance'
            ELSE '⚠️ Support Improvement Needed'
        END as support_performance_tier,
        CASE
            WHEN sa.priority_level = 'Critical' AND sa.resolution_time_hours > 4 THEN '🚨 Critical Delay'
            WHEN sa.priority_level = 'High' AND sa.resolution_time_hours > 24 THEN '⏰ High Priority Delay'
            WHEN sa.escalation_required = 'TRUE' THEN '⬆️ Escalation Required'
            ELSE '✅ Normal Processing'
        END as ticket_urgency_status,
        CASE
            WHEN sa.cost_impact_usd >= 10000 THEN '💰 High Cost Impact'
            WHEN sa.cost_impact_usd >= 5000 THEN '💸 Moderate Cost Impact'
            WHEN sa.cost_impact_usd >= 1000 THEN '💵 Low Cost Impact'
            ELSE '📝 No Financial Impact'
        END as cost_impact_level,
        CASE
            WHEN ss.critical_tickets > 0 THEN '🔴 Has Critical Issues'
            WHEN ss.high_priority_tickets > 3 THEN '⚠️ High Support Load'
            WHEN ss.total_tickets <= 2 THEN '✅ Low Support Burden'
            ELSE '📊 Standard Support Load'
        END as support_load_assessment
    FROM support_analysis sa
    JOIN support_summary ss ON sa.franchise_partner = ss.franchise_partner
    JOIN support_rankings sr ON ss.franchise_partner = sr.franchise_partner
    ORDER BY sa.ticket_date DESC, sa.priority_level DESC, sa.resolution_time_hours DESC
    LIMIT @limit
    """
    
    params = [
        bigquery.ScalarQueryParameter("limit", "INT64", None)
    ]
    
    return sql, params

# =======================
# Business Logic Layer
# =======================

def analyze_franchise_results(rows: List[Dict], query_type: str) -> Dict[str, Any]:
    """Generate franchise-specific insights"""
    
    insights = []
    recommendations = []
    alerts = []
    
    if not rows:
        return {
            "insights": ["Belirtilen kriterlere uygun franchise verisi bulunamadı"],
            "recommendations": ["Tarih aralığını veya kriterleri genişletin"],
            "alerts": []
        }
    
    try:
        if query_type == "partner_performance":
            # Performance tier analysis
            excellent_partners = [r for r in rows if '🏆 Excellent Partner' in str(r.get('partner_tier_assessment', ''))]
            needs_improvement = [r for r in rows if '⚠️ Needs Improvement' in str(r.get('partner_tier_assessment', ''))]
            
            if excellent_partners:
                top_performer = excellent_partners[0]
                insights.append(f"🏆 En başarılı partner: {top_performer['franchise_partner']} (Skor: {top_performer['avg_performance_score']})")
            
            if needs_improvement:
                alerts.append(f"⚠️ {len(needs_improvement)} partner gelişim desteği gerektiriyor")
                recommendations.append("Düşük performanslı partnerler için gelişim programları başlatın")
            
            # Revenue performance
            revenue_below_target = [r for r in rows if '🔴 Revenue Below Target' in str(r.get('revenue_performance', ''))]
            if revenue_below_target:
                alerts.append(f"🔴 {len(revenue_below_target)} partner gelir hedeflerinin altında")
                recommendations.append("Gelir hedeflerinin altındaki partnerlerle birebir review yapın")
            
            # Critical issues
            critical_issues = [r for r in rows if '🔴 Multiple Critical Issues' in str(r.get('issue_status', ''))]
            if critical_issues:
                alerts.append(f"🚨 {len(critical_issues)} partnerde çoklu kritik sorunlar tespit edildi")
                recommendations.append("Kritik sorunları öncelikle çözüme kavuşturun")
            
            # Overall performance insight
            avg_performance = sum(r.get('avg_performance_score', 0) for r in rows) / len(rows)
            insights.append(f"📊 Ortalama partner performans skoru: {avg_performance:.1f}/100")
        
        elif query_type == "store_network":
            # Regional coverage analysis
            regions = list(set(r.get('region', '') for r in rows))
            top_performers = [r for r in rows if '🏆 Top Performer' in str(r.get('performance_classification', ''))]
            
            insights.append(f"🌍 Analiz edilen bölge sayısı: {len(regions)}")
            if top_performers:
                insights.append(f"🏆 {len(top_performers)} mağaza top performer kategorisinde")
            
            # Compliance analysis
            compliance_issues = [r for r in rows if '🔴 Compliance Attention Needed' in str(r.get('compliance_status', ''))]
            if compliance_issues:
                alerts.append(f"🔴 {len(compliance_issues)} mağaza uygunluk sorunu yaşıyor")
                recommendations.append("Uygunluk sorunları olan mağazalar için acil düzeltici eylemler alın")
            
            # Contract renewal urgency
            urgent_renewals = [r for r in rows if '⏰ Contract Renewal Urgent' in str(r.get('contract_status', ''))]
            if urgent_renewals:
                alerts.append(f"⏰ {len(urgent_renewals)} mağaza kontratı acil yenileme gerektiriyor")
                recommendations.append("Acil kontrat yenilemeleri için partner görüşmeleri planlayın")
            
            # Expansion potential
            prime_expansion = [r for r in rows if '🚀 Prime Expansion Candidate' in str(r.get('expansion_assessment', ''))]
            if prime_expansion:
                insights.append(f"🚀 {len(prime_expansion)} mağaza birinci sınıf genişleme adayı")
                recommendations.append("Prime expansion adayları ile genişleme planları geliştirin")
        
        elif query_type == "expansion_analysis":
            # High potential opportunities
            premium_opportunities = [r for r in rows if '🚀 Premium Opportunity' in str(r.get('opportunity_classification', ''))]
            high_priority = [r for r in rows if '⭐ High Priority Opportunity' in str(r.get('opportunity_classification', ''))]
            
            if premium_opportunities:
                top_opportunity = premium_opportunities[0]
                insights.append(f"🚀 En iyi fırsat: {top_opportunity['target_city']}, {top_opportunity['target_country']} (ROI: %{top_opportunity['roi_projection_pct']})")
            
            insights.append(f"⭐ {len(premium_opportunities + high_priority)} yüksek öncelikli genişleme fırsatı")
            
            # Investment analysis
            total_investment = sum(r.get('investment_required_usd', 0) for r in rows)
            avg_roi = sum(r.get('roi_projection_pct', 0) for r in rows) / len(rows)
            
            insights.append(f"💰 Toplam yatırım gereksinimi: ${total_investment:,.0f}")
            insights.append(f"📈 Ortalama ROI projeksiyonu: %{avg_roi:.1f}")
            
            # Timeline insights
            fast_track = [r for r in rows if '⚡ Fast Track' in str(r.get('timeline_assessment', ''))]
            if fast_track:
                insights.append(f"⚡ {len(fast_track)} fırsat hızlı implementasyon için uygun")
                recommendations.append("Fast track fırsatları öncelikle değerlendirin")
        
        elif query_type == "support_tickets":
            # Support performance analysis
            excellent_support = [r for r in rows if '🏆 Excellent Support Performance' in str(r.get('support_performance_tier', ''))]
            needs_improvement = [r for r in rows if '⚠️ Support Improvement Needed' in str(r.get('support_performance_tier', ''))]
            
            if excellent_support:
                insights.append(f"🏆 {len(excellent_support)} partner mükemmel destek performansı gösteriyor")
            
            if needs_improvement:
                alerts.append(f"⚠️ {len(needs_improvement)} partner destek iyileştirmesi gerektiriyor")
                recommendations.append("Destek süreçlerini optimize edin ve yanıt sürelerini kısaltın")
            
            # Critical issues tracking
            critical_delays = [r for r in rows if '🚨 Critical Delay' in str(r.get('ticket_urgency_status', ''))]
            high_cost_impact = [r for r in rows if '💰 High Cost Impact' in str(r.get('cost_impact_level', ''))]
            
            if critical_delays:
                alerts.append(f"🚨 {len(critical_delays)} kritik ticket gecikme yaşıyor")
                recommendations.append("Kritik ticket'lar için acil müdahale protokolü başlatın")
            
            if high_cost_impact:
                alerts.append(f"💰 {len(high_cost_impact)} ticket yüksek maliyet etkisi yaratıyor")
            
            # Resolution insights
            avg_resolution_time = sum(r.get('partner_avg_resolution_time', 0) for r in rows) / len(rows)
            avg_satisfaction = sum(r.get('partner_avg_satisfaction', 0) for r in rows) / len(rows)
            
            insights.append(f"⏱️ Ortalama çözüm süresi: {avg_resolution_time:.1f} saat")
            insights.append(f"⭐ Ortalama destek memnuniyeti: {avg_satisfaction:.1f}/5.0")
    
    except Exception as e:
        logger.error(f"Error in analyze_franchise_results: {str(e)}")
    
    return {
        "insights": insights[:10],
        "recommendations": recommendations[:5],
        "alerts": alerts[:5]
    }

def calculate_franchise_summary(rows: List[Dict], query_type: str) -> Dict[str, Any]:
    """Calculate franchise operations summary statistics"""
    
    if not rows:
        return {"total_records": 0}
    
    summary = {
        "total_records": len(rows),
        "query_type": query_type,
        "timestamp": datetime.utcnow().isoformat()
    }
    
    try:
        if query_type == "partner_performance":
            summary.update({
                "unique_partners": len(set(r.get('franchise_partner') for r in rows)),
                "avg_performance_score": sum(r.get('avg_performance_score', 0) for r in rows) / len(rows) if rows else 0,
                "total_stores_managed": sum(r.get('stores_managed', 0) for r in rows),
                "total_critical_issues": sum(r.get('total_critical_issues', 0) for r in rows)
            })
        
        elif query_type == "store_network":
            summary.update({
                "total_stores": len(set(r.get('store_id') for r in rows)),
                "unique_regions": len(set(r.get('region') for r in rows)),
                "unique_countries": len(set(r.get('country') for r in rows)),
                "avg_compliance_score": sum(r.get('compliance_score', 0) for r in rows) / len(rows) if rows else 0
            })
        
        elif query_type == "expansion_analysis":
            summary.update({
                "total_opportunities": len(rows),
                "target_countries": len(set(r.get('target_country') for r in rows)),
                "total_investment_required": sum(r.get('investment_required_usd', 0) for r in rows),
                "avg_roi_projection": sum(r.get('roi_projection_pct', 0) for r in rows) / len(rows) if rows else 0
            })
        
        elif query_type == "support_tickets":
            summary.update({
                "total_tickets": len(rows),
                "unique_partners": len(set(r.get('franchise_partner') for r in rows)),
                "avg_resolution_time": sum(r.get('partner_avg_resolution_time', 0) for r in rows) / len(rows) if rows else 0,
                "resolution_rate": sum(r.get('resolution_rate_pct', 0) for r in rows) / len(rows) if rows else 0
            })
    
    except Exception as e:
        logger.error(f"Error in calculate_franchise_summary: {str(e)}")
    
    return summary

# =======================
# Main HTTP Handler
# =======================

@functions_framework.http
def franchise_ops_query(request):
    """Suvari Franchise Operations Analytics Engine"""
    
    try:
        # Parse request
        body = request.get_json(silent=True) or {}
        
        # Extract parameters
        question = body.get("question", "")
        query_type = body.get("query_type") or detect_query_intent(question)
        limit = min(int(body.get("limit", 100)), 500)
        
        logger.info(f"Processing franchise ops query - Type: {query_type}")
        
        # Query function mapping
        query_functions = {
            "partner_performance": sql_partner_performance,
            "store_network": sql_store_network,
            "network_analysis": sql_store_network,
            "expansion_analysis": sql_expansion_analysis,
            "expansion_pipeline": sql_expansion_analysis,
            "support_tickets": sql_support_tickets,
            "support_analysis": sql_support_tickets
        }
        
        # Get appropriate query
        query_func = query_functions.get(query_type, sql_partner_performance)
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
            labels={"agent": "franchise_ops", "query_type": query_type}
        )
        
        job = client.query(sql, job_config=job_config)
        rows = [dict(r) for r in job.result()]
        
        # Analyze results
        analysis = analyze_franchise_results(rows, query_type)
        summary = calculate_franchise_summary(rows, query_type)
        
        # Build response
        response = {
            "success": True,
            "agent": "franchise_ops",
            "query_type": query_type,
            "parameters": {
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
        logger.exception(f"Franchise ops query failed: {str(e)}")
        error_response = {
            "success": False,
            "error": str(e),
            "error_type": type(e).__name__,
            "agent": "franchise_ops",
            "query_type": query_type if 'query_type' in locals() else "unknown"
        }
        return (json.dumps(error_response, ensure_ascii=False), 500, {"Content-Type": "application/json"})