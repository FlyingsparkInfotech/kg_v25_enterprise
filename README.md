# KG V25.2 Enterprise Framework

This version adds progress-safe batching and optional MLflow tracking.

## Why v25.1
The first v25 version could appear stuck after Postgres because the next stage (`derive-signals`) ran a large Neo4j transaction with very little progress logging. V25.2 splits UI/CRM signal derivation and every factory stage into small repeatable batches.

## Run order

```bash
python main.py init --config config.yaml
python main.py derive-signals --config config.yaml
python main.py factory-only --config config.yaml
python main.py validate --config config.yaml
```

For full run:

```bash
python main.py run-all --config config.yaml
```

If base and Postgres are already loaded, avoid reloading them:

```bash
python main.py run-all --config config.yaml --skip-base --skip-postgres
```

## Optional MLflow

In `config.yaml`:

```yaml
mlflow:
  enabled: true
  tracking_uri: file:./mlruns
  experiment_name: kg_v25_enterprise
```

Start UI:

```bash
mlflow ui --backend-store-uri ./mlruns --host 0.0.0.0 --port 5000
```

## Important config for slow Neo4j

Use smaller batches first:

```yaml
runtime:
  signal_batch: 1000
  max_rows_per_step: 50000
```

Increase `signal_batch` after the first successful run.
