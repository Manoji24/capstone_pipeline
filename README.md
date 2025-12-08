# 📊 Project Overview

This repository implements a **Databricks-based ETL pipeline** following the **Medallion Architecture** (Bronze → Silver → Gold) for a capstone dataset containing **products, transactions, and customers**.

---

## 🔧 Key Features
- **Bronze Layer (Raw Ingestion)**
  - Uses **Auto Loader (cloudFiles)** to ingest raw CSV/JSON data.
  - Applies schemas and creates Bronze Delta tables.
  - Writes audit log entries for each ingest.

- **Silver Layer (Transformations)**
  - Cleans and normalizes transactions (timestamps, currency, numeric types).
  - Filters corrupt records and writes Silver transactions table.
  - Cleans product metadata (title casing) and writes Silver products table.
  - Builds **Slowly Changing Dimension Type 2 (SCD2)** customers table with merges/inserts.
  - Calls `audit_log` after each write.

- **Gold Layer (Analytics)**
  - Joins Silver dimensions and transactions to create enriched **daily_sales_fact** table.
  - Aggregates fact data into **daily_sales_summary** metrics for reporting and dashboards.
  - Calls `audit_log` after table creation and inserts.

- **Audit Logging**
  - Lightweight, append‑only Delta table capturing pipeline metadata.
  - Records batch identifiers, row counts, timestamps, status, and optional error messages.
  - Centralized audit location ensures traceability across ingest, transform, and merge steps.
  - Helper functions provided in `capstone_pipeline.main`.

---

## 📓 Notebooks
Located under `src/`:

- `01_Bronze_Ingestion.ipynb` — Raw ingestion with Auto Loader → Bronze tables.
- `02_Silver_Transaction.ipynb` — Transaction cleaning → Silver transactions.
- `02_Silver_Products.ipynb` — Product metadata cleaning → Silver products.
- `02_Silver_Customers_SCD2.ipynb` — SCD2 merge logic → Silver customers.
- `03_Gold_Fact_Table.ipynb` — Dimension joins → Gold fact table.
- `04_Gold_Aggregations.ipynb` — Aggregations → Gold summary metrics.
- `tests/nb_pytest_demo.ipynb` — Run pytest suite inside cluster for quick verification.

---

## ▶️ How to Run
1. Use **Databricks** to run notebooks interactively or orchestrate via **Jobs/Workflows**.
2. Configure `log_path` and base paths in each notebook to point to your storage/mount points.
3. For **streaming ingestion**, ensure checkpoint locations are accessible and durable.

---

## 🚀 CI/CD Pipeline

This repository uses **GitHub Actions** to automate testing and deployment on every push to the `main` branch.

### Workflow: `.github/workflows/cicd.yml`

The CI/CD pipeline includes three main stages:

1. **Test Stage**
   - Runs on every push and pull request
   - Installs Python dependencies
   - Executes pytest suite (`tests/main_test.py`)
   - Generates and uploads coverage reports to Codecov

2. **Deploy to Dev**
   - Runs only on pushes to `main` (after tests pass)
   - Uses `databricks bundle deploy --target dev`
   - Validates deployment configuration

3. **Deploy to Prod**
   - Runs only on pushes to `main` (after dev deployment succeeds)
   - Uses `databricks bundle deploy --target prod`
   - Validates production deployment

### Required GitHub Secrets

Add these secrets to your GitHub repository settings (Settings > Secrets and variables > Actions):

- `DATABRICKS_HOST` — Your Databricks workspace URL (e.g., `https://your-instance.cloud.databricks.com`)
- `DATABRICKS_TOKEN` — Personal access token for Databricks authentication

### Triggering the Workflow

- **Push to `main`**: Runs all stages (test → dev → prod)
- **Pull Request to `main`**: Runs tests only (no deployment)

### Monitoring Builds

- View workflow runs: GitHub repo → **Actions** tab
- Failed workflows block merges to `main`
- Coverage reports available via Codecov integration

---

## ▶️ Databricks CLI

0. Install UV: https://docs.astral.sh/uv/getting-started/installation/

1. Install the Databricks CLI from https://docs.databricks.com/dev-tools/cli/databricks-cli.html

2. Authenticate to your Databricks workspace, if you have not done so already:
    ```
    $ databricks configure
    ```

3. To deploy a development copy of this project, type:
    ```
    $ databricks bundle deploy --target dev
    ```
    (Note that "dev" is the default target, so the `--target` parameter
    is optional here.)

    This deploys everything that's defined for this project.
    For example, the default template would deploy a job called
    `[dev yourname] capstone_pipeline_job` to your workspace.
    You can find that job by opening your workpace and clicking on **Workflows**.

4. Similarly, to deploy a production copy, type:
   ```
   $ databricks bundle deploy --target prod
   ```

   Note that the default job from the template has a schedule that runs every day
   (defined in resources/capstone_pipeline.job.yml). The schedule
   is paused when deploying in development mode (see
   https://docs.databricks.com/dev-tools/bundles/deployment-modes.html).

5. To run a job or pipeline, use the "run" command:
   ```
   $ databricks bundle run
   ```
6. Optionally, install the Databricks extension for Visual Studio code for local development from
   https://docs.databricks.com/dev-tools/vscode-ext.html. It can configure your
   virtual environment and setup Databricks Connect for running unit tests locally.
   When not using these tools, consult your development environment's documentation
   and/or the documentation for Databricks Connect for manually setting up your environment
   (https://docs.databricks.com/en/dev-tools/databricks-connect/python/index.html).

7. For documentation on the Databricks asset bundles format used
   for this project, and for CI/CD configuration, see
   https://docs.databricks.com/dev-tools/bundles/index.html.