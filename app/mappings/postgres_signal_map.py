# Signal map: (schema.table) → (signal_event_type, source_node_label)
# Covers both Postgres (goglo_etl) and CRM MySQL tables.

POSTGRES_SIGNAL_TABLE_MAP = {
    # ── Trademo / Trade Intelligence (Postgres goglo_etl) ─────────────────────
    'raw.trademo_shipment_bl':              ('shipment_detected',                'TradeShipmentSource'),
    'raw.trademo_buyer_supplier_list':      ('active_importer_detected',         'TradeShipmentSource'),
    'raw.trademo_company_master':           ('trade_company_profile_detected',   'TradeShipmentSource'),
    'raw.trademo_company_matcher':          ('trade_company_matched',            'TradeShipmentSource'),
    'raw.trademo_company_import_partner':   ('import_partner_detected',          'TradeShipmentSource'),
    'raw.trademo_company_export_partner':   ('export_partner_detected',          'TradeShipmentSource'),
    'raw.trademo_company_imported_hs':      ('import_hs_interest_detected',      'TradeShipmentSource'),
    'raw.trademo_company_exported_hs':      ('export_hs_activity_detected',      'TradeShipmentSource'),
    'raw.trademo_company_imported_keyword': ('import_keyword_interest_detected', 'TradeShipmentSource'),
    'raw.trademo_company_exported_keyword': ('export_keyword_activity_detected', 'TradeShipmentSource'),
    'raw.trademo_company_country_import':   ('import_country_activity_detected', 'TradeShipmentSource'),
    'raw.trademo_company_country_export':   ('export_country_activity_detected', 'TradeShipmentSource'),
    'raw.trademo_company_ports_lading':     ('export_port_activity_detected',    'TradeShipmentSource'),
    'raw.trademo_company_ports_unlading':   ('import_port_activity_detected',    'TradeShipmentSource'),
    'raw.trademo_hs_classifier':            ('hs_classification_detected',       'TradeShipmentSource'),
    'raw.company_hs_code':                  ('company_hs_profile_detected',      'TradeShipmentSource'),
    'raw.company_products':                 ('company_product_profile_detected',  'TradeShipmentSource'),

    # ── ZoomInfo / Third-party Intelligence (Postgres goglo_etl) ─────────────
    'zoominfo.company_search':              ('company_discovered',               'ThirdPartyIntelligenceSource'),
    'zoominfo.contact_search':              ('contact_discovered',               'ThirdPartyIntelligenceSource'),
    'zoominfo.contact_enrich':              ('contact_enriched',                 'EnrichmentSource'),
    'zoominfo.corporate_hierarchy':         ('firmographic_update_detected',     'ThirdPartyIntelligenceSource'),
    'zoominfo.intent_search':              ('third_party_intent_detected',       'ThirdPartyIntelligenceSource'),
    'zoominfo.news_search':                ('news_trigger_detected',             'ThirdPartyIntelligenceSource'),
    'zoominfo.scoop_search':               ('business_event_detected',           'ThirdPartyIntelligenceSource'),
    'skg.intent_signal':                   ('commercial_intent_detected',        'ThirdPartyIntelligenceSource'),
    'skg.intent_score':                    ('intent_score_updated',              'ThirdPartyIntelligenceSource'),
    'skg.interaction':                     ('interaction_detected',              'WebsiteBehaviorSource'),
    'skg.lead':                            ('external_lead_detected',            'CRMSource'),

    # ── CRM MySQL — Website Behavior (scenarios A, C, D, H, M, T, BB, FF) ────
    'crm.click_tracking':                  ('commercial_click_detected',         'WebsiteBehaviorSource'),
    'crm.page_event_trackings':            ('commercial_action_detected',        'WebsiteBehaviorSource'),
    'crm.page_visits':                     ('page_visit_detected',               'WebsiteBehaviorSource'),
    'crm.scroll_depths':                   ('scroll_depth_recorded',             'WebsiteBehaviorSource'),
    'crm.session_engagements':             ('session_engagement_detected',       'WebsiteBehaviorSource'),

    # ── CRM MySQL — RFQ / Quote (scenarios F, G, H, FF) ─────────────────────
    'crm.rfqs':                            ('rfq_detected',                      'CRMSource'),
    'crm.rfq_items':                       ('rfq_item_detected',                 'CRMSource'),
    'crm.rfq_files':                       ('rfq_file_attached',                 'CRMSource'),
    'crm.rfq_distributions':               ('rfq_distributed',                   'CRMSource'),
    'crm.rfq_quotes':                      ('quote_submitted',                   'CRMSource'),
    'crm.quotation':                       ('quote_request_detected',            'CRMSource'),

    # ── CRM MySQL — Deals / Orders (scenarios Z, u) ───────────────────────────
    'crm.deals':                           ('deal_detected',                     'CRMSource'),
    'crm.deal_stages':                     ('deal_stage_changed',                'CRMSource'),
    'crm.deal_stage_histories':            ('deal_stage_history_recorded',       'CRMSource'),

    # ── CRM MySQL — Contacts / Emails (scenarios K, II, JJ, AA) ─────────────
    'crm.crm_leads':                       ('crm_lead_detected',                 'CRMSource'),
    'crm.crm_contacts':                    ('contact_detected',                  'CRMSource'),
    'crm.crm_emails':                      ('email_interaction_detected',        'CRMSource'),
    'crm.auto_quote_email_events':         ('email_event_detected',              'CRMSource'),
    'crm.crm_lead_product_items':          ('lead_product_interest_detected',    'CRMSource'),

    # ── CRM MySQL — Lead Engine (scenarios PP, architecture) ─────────────────
    'crm.lead_master':                     ('lead_master_updated',               'CRMSource'),
    'crm.lead_evidence':                   ('lead_evidence_recorded',            'CRMSource'),
    'crm.raw_source_payload':              ('raw_source_ingested',               'CRMSource'),
    'crm.signal_registry':                 ('signal_registered',                 'NormalizedSource'),
    'crm.source_registry':                 ('source_registered',                 'NormalizedSource'),
    'crm.lead_action_log':                 ('lead_action_logged',                'CRMSource'),
    'crm.lead_suppression_log':            ('lead_suppressed',                   'CRMSource'),
    'crm.lead_distribution_log':           ('lead_distributed',                  'CRMSource'),

    # ── CRM MySQL — Identity / Compliance (scenarios EE, LL, x, OO) ─────────
    'crm.account_identity':                ('identity_resolved',                 'IdentitySource'),
    'crm.account_risk_flags':              ('compliance_flag_detected',          'ComplianceSource'),
    'crm.identity_resolution_log':         ('identity_resolution_logged',        'IdentitySource'),
    'crm.account_state':                   ('account_state_changed',             'CRMSource'),
    'crm.person_identity':                 ('person_identity_detected',          'IdentitySource'),

    # ── CRM MySQL — Partner / Channel (scenario HH) ───────────────────────────
    'crm.partner_registry':                ('partner_detected',                  'CRMSource'),
    'crm.engagement_with_sellers':         ('seller_engagement_detected',        'CRMSource'),
    'crm.trade_relationship':              ('crm_trade_relationship_detected',   'TradeShipmentSource'),

    # ── CRM MySQL — Scoring / Analytics ───────────────────────────────────────
    'crm.scoring_outcomes':                ('scoring_outcome_recorded',          'CRMSource'),
    'crm.scoring_rule_evaluation':         ('scoring_rule_evaluated',            'CRMSource'),
    'crm.build_lead_score':                ('lead_score_built',                  'CRMSource'),
    'crm.seller_user_metrics':             ('seller_metric_updated',             'CRMSource'),

    # ── GoGlo staging / messaging ─────────────────────────────────────────────
    'goglo.messages':                      ('message_sent_detected',             'CRMSource'),
    'goglo.bot_detection_logs':            ('bot_detected',                      'BotDetectionSource'),
    'goglo_staging.enquiries':             ('enquiry_detected',                  'CRMSource'),
    'goglo_staging.tracking_sessions':     ('session_detected',                  'WebsiteBehaviorSource'),
    'goglo_staging.tracking_click_events': ('click_detected',                    'WebsiteBehaviorSource'),
    'goglo_staging.tracking_page_views':   ('page_view_detected',                'WebsiteBehaviorSource'),
}


def signal_mapping(schema: str, table: str):
    key = f'{schema}.{table}'.lower()
    if key in POSTGRES_SIGNAL_TABLE_MAP:
        return POSTGRES_SIGNAL_TABLE_MAP[key]
    # Fallback heuristics
    if any(x in key for x in ['trademo', 'shipment', 'import', 'export', 'hs_code']):
        return ('trade_intelligence_detected', 'TradeShipmentSource')
    if any(x in key for x in ['zoominfo', 'intent_search', 'scoop']):
        return ('third_party_company_intelligence_detected', 'ThirdPartyIntelligenceSource')
    if any(x in key for x in ['click', 'page_visit', 'session', 'scroll', 'page_event']):
        return ('web_behavior_detected', 'WebsiteBehaviorSource')
    if any(x in key for x in ['rfq', 'quotation', 'deal', 'lead', 'contact', 'email']):
        return ('crm_event_detected', 'CRMSource')
    if 'intent' in key:
        return ('commercial_intent_detected', 'ThirdPartyIntelligenceSource')
    return None
