# Kafka integration layer for KG V25.2
# Producer  — publishes KG output events after each pipeline write
# Consumer  — receives raw CRM / web / trade events from upstream topics
# Outbox    — writes to kg_outbox in Postgres; Debezium picks it up
