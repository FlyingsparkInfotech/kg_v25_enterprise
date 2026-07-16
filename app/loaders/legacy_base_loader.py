from pathlib import Path
import importlib.util, sys
from app.core.logger import ok, warn
class LegacyBaseLoader:
    def __init__(self,settings): self.settings=settings
    def run(self, skip_governance=False, skip_ui=False, skip_crm=False):
        base_file=Path(self.settings.runtime.legacy_base_file)
        if not base_file.exists(): warn(f'Legacy base file not found: {base_file}; skipping base loader'); return
        spec=importlib.util.spec_from_file_location('legacy_base_injector', str(base_file)); base=importlib.util.module_from_spec(spec); assert spec and spec.loader; sys.modules[spec.name]=base; spec.loader.exec_module(base)
        gov=base.GovernanceConfig(); gov.STORE_VALIDATION_RESULTS=not skip_governance
        neo=base.Neo4jWriter(self.settings.neo4j.uri,self.settings.neo4j.user,self.settings.neo4j.password)
        ui=base.MySQL(base.MySQLConnInfo(self.settings.mysql_ui.host,self.settings.mysql_ui.port,self.settings.mysql_ui.user,self.settings.mysql_ui.password,self.settings.mysql_ui.database))
        crm=None if skip_crm else base.MySQL(base.MySQLConnInfo(self.settings.mysql_crm.host,self.settings.mysql_crm.port,self.settings.mysql_crm.user,self.settings.mysql_crm.password,self.settings.mysql_crm.database))
        try:
            inj=base.InjectorV5(neo=neo, ui=ui, crm=crm, batch_size=self.settings.runtime.batch_size, governance_config=gov)
            inj.upsert_meta_from_xlsx(entity_xlsx=self.settings.xlsx.entity, rel_xlsx=self.settings.xlsx.relationships, taxonomy_xlsx=self.settings.xlsx.taxonomy, conditional_xlsx=self.settings.xlsx.conditional)
            inj.upsert_hat_rbac_governance_metadata(); inj.upsert_provenance_and_advanced_governance_metadata(); inj.neo.create_runtime_constraints()
            if not skip_ui:
                inj.inject_users_persons_accounts_orgs(run_governance=False); inj.inject_categories(); inj.inject_brands(); inj.inject_units(run_governance=False); inj.inject_products(run_governance=False); inj.inject_product_applications(run_governance=False); inj.inject_use_cases_and_keywords(run_governance=False); inj.inject_master_keywords(); inj.inject_facilities(); inj.inject_feature_packages(); inj.inject_hat_system(run_governance=False); inj.inject_rbac_abac(run_governance=False)
            if crm is not None:
                inj.inject_crm_pipeline(); inj.inject_crm_core(); inj.inject_crm_activity()
            try: inj.inject_lead_source_provenance(run_governance=False); inj.inject_advanced_intelligence_pipelines(run_governance=False)
            except Exception as e: warn(f'Advanced legacy metadata skipped: {e}')
            ok('Legacy base governance + UI + CRM + G-I loaded')
        finally:
            ui.close(); neo.close();
            if crm: crm.close()
