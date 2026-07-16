POSTGRES_SIGNAL_TABLE_MAP={
 'raw.trademo_shipment_bl':('shipment_detected','TradeShipmentSource'), 'raw.trademo_buyer_supplier_list':('active_importer_detected','TradeShipmentSource'),
 'raw.trademo_company_master':('trade_company_profile_detected','TradeShipmentSource'), 'raw.trademo_company_matcher':('trade_company_matched','TradeShipmentSource'),
 'raw.trademo_company_import_partner':('import_partner_detected','TradeShipmentSource'), 'raw.trademo_company_export_partner':('export_partner_detected','TradeShipmentSource'),
 'raw.trademo_company_imported_hs':('import_hs_interest_detected','TradeShipmentSource'), 'raw.trademo_company_exported_hs':('export_hs_activity_detected','TradeShipmentSource'),
 'raw.trademo_company_imported_keyword':('import_keyword_interest_detected','TradeShipmentSource'), 'raw.trademo_company_exported_keyword':('export_keyword_activity_detected','TradeShipmentSource'),
 'raw.trademo_company_country_import':('import_country_activity_detected','TradeShipmentSource'), 'raw.trademo_company_country_export':('export_country_activity_detected','TradeShipmentSource'),
 'raw.trademo_company_ports_lading':('export_port_activity_detected','TradeShipmentSource'), 'raw.trademo_company_ports_unlading':('import_port_activity_detected','TradeShipmentSource'),
 'raw.trademo_hs_classifier':('hs_classification_detected','TradeShipmentSource'),
 'zoominfo.company_search':('company_discovered','ThirdPartyIntelligenceSource'), 'zoominfo.contact_enrich':('contact_enriched','EnrichmentSource'),
 'zoominfo.corporate_hierarchy':('firmographic_update_detected','ThirdPartyIntelligenceSource'), 'zoominfo.intent_search':('third_party_intent_detected','ThirdPartyIntelligenceSource'),
 'zoominfo.news_search':('news_trigger_detected','ThirdPartyIntelligenceSource'), 'zoominfo.scoop_search':('business_event_detected','ThirdPartyIntelligenceSource'),
 'skg.intent_signal':('commercial_intent_detected','ThirdPartyIntelligenceSource'), 'skg.intent_score':('intent_score_updated','ThirdPartyIntelligenceSource'),
 'skg.interaction':('interaction_detected','WebsiteBehaviorSource'), 'skg.lead':('external_lead_detected','CRMSource'),
 'raw.company_hs_code':('company_hs_profile_detected','ThirdPartyIntelligenceSource'), 'raw.company_products':('company_product_profile_detected','ThirdPartyIntelligenceSource')}
def signal_mapping(schema,table):
    key=f'{schema}.{table}'.lower()
    if key in POSTGRES_SIGNAL_TABLE_MAP: return POSTGRES_SIGNAL_TABLE_MAP[key]
    if any(x in key for x in ['trademo','shipment','import','export']): return ('trade_intelligence_detected','TradeShipmentSource')
    if any(x in key for x in ['zoominfo','contact','company']): return ('third_party_company_intelligence_detected','ThirdPartyIntelligenceSource')
    if 'intent' in key: return ('commercial_intent_detected','ThirdPartyIntelligenceSource')
    return None
