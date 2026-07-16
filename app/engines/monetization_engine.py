"""
MonetizationEngine — Credit-based subscription tier system.

Subscription tiers:
  basic:        100 credits/month,  lead access: low+medium
  professional: 500 credits/month,  lead access: low+medium+high
  enterprise:   unlimited credits,  lead access: all priorities

Credit costs per lead priority (aligned with DistributionEngine.CREDIT_COST):
  critical: 5 credits
  high:     3 credits
  medium:   1 credit
  low:      0 credits

Credit operations:
  - allocate_monthly_credits()  : refill credits at start of billing cycle
  - deduct_credits()            : deduct on lead unlock (called by DistributionEngine)
  - top_up_credits()            : ad-hoc credit purchase
  - get_balance()               : current balance for a seller

Config (config.yaml):
  monetization:
    enabled: true
    default_tier: basic
    credit_cost_critical: 5
    credit_cost_high:     3
    credit_cost_medium:   1
    credit_cost_low:      0
    monthly_credits_basic:        100
    monthly_credits_professional: 500
    monthly_credits_enterprise:   -1     # -1 = unlimited
"""

from app.core.logger import info, ok, warn, banner

# Tier → monthly credit allotment (-1 = unlimited)
_TIER_MONTHLY_CREDITS = {
    'basic':        100,
    'professional': 500,
    'enterprise':   -1,   # unlimited
}

# Tier → which lead priorities can be accessed
_TIER_PRIORITY_ACCESS = {
    'basic':        {'low', 'medium'},
    'professional': {'low', 'medium', 'high'},
    'enterprise':   {'low', 'medium', 'high', 'critical'},
}

_UPSERT_ACCOUNT_QUERY = """
MERGE (sa:SellerAccount {account_id: 'acct:' + $seller_id})
SET sa.seller_id           = $seller_id,
    sa.tier                = coalesce($tier, sa.tier, 'basic'),
    sa.credits_available   = coalesce(sa.credits_available, 0),
    sa.credits_used_month  = coalesce(sa.credits_used_month, 0),
    sa.credits_total_month = coalesce($monthly_allot, sa.credits_total_month, 100),
    sa.billing_cycle_start = coalesce(sa.billing_cycle_start, toString(date())),
    sa.created_at          = coalesce(sa.created_at, toString(datetime())),
    sa.updated_at          = toString(datetime())
RETURN sa.credits_available AS balance
"""

_DEDUCT_QUERY = """
MATCH (sa:SellerAccount {account_id: 'acct:' + $seller_id})
WHERE sa.credits_available >= $cost OR sa.credits_total_month = -1
SET sa.credits_available  = CASE WHEN sa.credits_total_month = -1 THEN sa.credits_available
                                  ELSE sa.credits_available - $cost END,
    sa.credits_used_month = sa.credits_used_month + $cost,
    sa.updated_at         = toString(datetime())
MERGE (tx:CreditTransaction {
    tx_id: 'tx:' + $seller_id + ':' + toString(datetime())
})
SET tx.seller_id   = $seller_id,
    tx.type        = 'deduct',
    tx.amount      = $cost,
    tx.lead_id     = $lead_id,
    tx.reason      = $reason,
    tx.recorded_at = toString(datetime())
MERGE (sa)-[:HAS_TRANSACTION]->(tx)
RETURN sa.credits_available AS balance
"""

_TOPUP_QUERY = """
MATCH (sa:SellerAccount {account_id: 'acct:' + $seller_id})
SET sa.credits_available  = sa.credits_available + $amount,
    sa.updated_at         = toString(datetime())
MERGE (tx:CreditTransaction {
    tx_id: 'tx:topup:' + $seller_id + ':' + toString(datetime())
})
SET tx.seller_id   = $seller_id,
    tx.type        = 'topup',
    tx.amount      = $amount,
    tx.reason      = $reason,
    tx.recorded_at = toString(datetime())
MERGE (sa)-[:HAS_TRANSACTION]->(tx)
RETURN sa.credits_available AS balance
"""

_BALANCE_QUERY = """
MATCH (sa:SellerAccount {account_id: 'acct:' + $seller_id})
RETURN sa.tier               AS tier,
       sa.credits_available  AS available,
       sa.credits_used_month AS used_month,
       sa.credits_total_month AS total_month,
       sa.billing_cycle_start AS cycle_start
"""

_MONTHLY_RESET_QUERY = """
MATCH (sa:SellerAccount)
WHERE sa.billing_cycle_start IS NOT NULL
  AND duration.inDays(date(sa.billing_cycle_start), date()).days >= 30
SET sa.credits_used_month  = 0,
    sa.credits_available   = CASE WHEN sa.credits_total_month = -1
                                  THEN sa.credits_available
                                  ELSE sa.credits_total_month END,
    sa.billing_cycle_start = toString(date()),
    sa.updated_at          = toString(datetime())
RETURN count(sa) AS c
"""


class MonetizationEngine:

    def __init__(self, neo, settings):
        self.neo = neo
        cfg      = getattr(settings, 'monetization', None)
        self.enabled          = bool(cfg and getattr(cfg, 'enabled', True))
        self.default_tier     = getattr(cfg, 'default_tier', 'basic') if cfg else 'basic'
        self.cost_critical    = int(getattr(cfg, 'credit_cost_critical', 5)  if cfg else 5)
        self.cost_high        = int(getattr(cfg, 'credit_cost_high',     3)  if cfg else 3)
        self.cost_medium      = int(getattr(cfg, 'credit_cost_medium',   1)  if cfg else 1)
        self.cost_low         = int(getattr(cfg, 'credit_cost_low',      0)  if cfg else 0)

    # ── credit cost lookup ────────────────────────────────────────────────────

    def credit_cost(self, priority: str) -> int:
        return {
            'critical': self.cost_critical,
            'high':     self.cost_high,
            'medium':   self.cost_medium,
            'low':      self.cost_low,
        }.get(priority, 0)

    def priority_accessible(self, seller_tier: str, lead_priority: str) -> bool:
        return lead_priority in _TIER_PRIORITY_ACCESS.get(seller_tier, set())

    # ── account management ────────────────────────────────────────────────────

    def ensure_account(self, seller_id: str, tier: str = None) -> dict:
        tier      = tier or self.default_tier
        allotment = _TIER_MONTHLY_CREDITS.get(tier, 100)
        rows = self.neo.run(_UPSERT_ACCOUNT_QUERY, {
            'seller_id':    seller_id,
            'tier':         tier,
            'monthly_allot': allotment,
        }) or []
        return {'balance': rows[0]['balance'] if rows else 0}

    def allocate_monthly_credits(self) -> int:
        """Reset billing cycle for all accounts that are >= 30 days old."""
        rows = self.neo.run(_MONTHLY_RESET_QUERY) or [{'c': 0}]
        count = int(rows[0].get('c', 0))
        if count:
            ok(f'MonetizationEngine: reset billing cycle for {count} accounts')
        return count

    def deduct_credits(self, seller_id: str, cost: int, lead_id: str, reason: str = 'lead_unlock') -> dict:
        if not self.enabled or cost == 0:
            return {'balance': None, 'success': True}
        rows = self.neo.run(_DEDUCT_QUERY, {
            'seller_id': seller_id,
            'cost':      cost,
            'lead_id':   lead_id,
            'reason':    reason,
        }) or []
        if not rows:
            warn(f'MonetizationEngine: deduct failed for seller {seller_id} — insufficient credits or account missing')
            return {'balance': None, 'success': False}
        return {'balance': rows[0].get('balance'), 'success': True}

    def top_up_credits(self, seller_id: str, amount: int, reason: str = 'purchase') -> dict:
        rows = self.neo.run(_TOPUP_QUERY, {
            'seller_id': seller_id,
            'amount':    amount,
            'reason':    reason,
        }) or []
        balance = rows[0].get('balance') if rows else None
        ok(f'MonetizationEngine: topped up {amount} credits for {seller_id}, balance={balance}')
        return {'balance': balance}

    def get_balance(self, seller_id: str) -> dict:
        rows = self.neo.run(_BALANCE_QUERY, {'seller_id': seller_id}) or []
        if not rows:
            return {'error': 'account_not_found'}
        r = rows[0]
        unlimited = r.get('total_month') == -1
        return {
            'seller_id':       seller_id,
            'tier':            r.get('tier', 'basic'),
            'credits_available': r.get('available', 0) if not unlimited else 'unlimited',
            'credits_used':    r.get('used_month', 0),
            'credits_monthly': r.get('total_month'),
            'cycle_start':     r.get('cycle_start'),
            'unlimited':       unlimited,
        }

    def run(self) -> dict:
        """Batch operation: reset billing cycles for all due accounts."""
        if not self.enabled:
            return {'reset': 0}
        banner('MonetizationEngine: Monthly credit allocation')
        reset = self.allocate_monthly_credits()
        return {'reset': reset}
