from pathlib import Path
from app.core.logger import ok
class InitEngine:
    def __init__(self,neo,settings): self.neo=neo; self.settings=settings
    def run(self):
        root=Path(__file__).resolve().parents[1]
        self.neo.run_file(str(root/'cypher'/'constraints.cypher')); self.neo.run_file(str(root/'cypher'/'indexes.cypher')); self.neo.run_file(str(root/'cypher'/'switch_schema.cypher'))
        source_families=['WebsiteBehaviorSource','RFQInquirySource','CRMSource','EmailCommunicationSource','MarketplaceSource','TradeShipmentSource','EnrichmentSource','ThirdPartyIntelligenceSource','OfflineEventSource','IdentityDeAnonymizationSource','CommercialProgressionSource']
        lead_types=['visit_only','intent_only','hot_in_market','rfq_submitted','quote_ready','deal_ready','active_deal','market_opportunity','strategic_account','event_lead','blocked']
        evidence_categories=['intent','behavior','engagement','trade','risk','identity','opportunity','commitment','enrichment','offline','commercial','cross_source']
        self.neo.run('UNWIND $x AS v MERGE (:SourceFamily {name:v})', {'x':source_families})
        self.neo.run('UNWIND $x AS v MERGE (:LeadType {type_name:v})', {'x':lead_types})
        self.neo.run('UNWIND $x AS v MERGE (:EvidenceCategory {name:v})', {'x':evidence_categories})
        self.neo.run("MERGE (sys:System {system_id:'lead_engine_v25'}) SET sys.scoring_model_version=$m, sys.updated_at=toString(datetime())", {'m':self.settings.runtime.scoring_model_version})
        ok('Init complete')
