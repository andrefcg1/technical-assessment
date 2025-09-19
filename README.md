# Technical Assessment - dbt + BigQuery + Google Cloud Composer

This repository contains a complete data pipeline implementation using dbt, BigQuery, and Google Cloud Composer for the Technical Assessment.

## Architecture

- **dbt**: Data transformation and modeling
- **BigQuery**: Data warehouse
- **Google Cloud Composer**: Managed Airflow for orchestration
- **Python**: Data processing and pipeline automation

## Project Structure

```
├─airflow/
│ └─dags/
│   └─bigquery_pipeline.py          # Main DAG for Cloud Composer
├─dbt/
│ ├─models/                         # dbt models
│ │ ├─model_1_daily_player_transactions.sql
│ │ ├─model_2_kyc_discord_deposits.sql
│ │ ├─model_3_top_three_deposits.sql
│ │ └─schema.yml                    # Model documentation and tests
│ ├─seeds/                          # CSV data files
│ │ ├─affiliates_extended.csv
│ │ ├─players_extended.csv
│ │ └─transactions_extended.csv
│ ├─snapshots/                      # dbt snapshots
│ │ └─player_kyc_status_snapshot.sql
│ ├─tests/                          # Custom dbt tests
│ │ ├─test_deposits_are_positive.sql
│ │ ├─test_kyc_discord_filtering.sql
│ │ └─test_withdrawals_are_negative.sql
│ ├─dbt_project.yml                 # dbt project configuration
│ ├─profiles.yml                    # dbt profiles for BigQuery
│ └─packages.yml                    # dbt packages (dbt_utils)
├─README.md
└─requirements.txt                  # Python dependencies for Composer
```

## Quick Start

### Prerequisites

1. **Google Cloud Project** with the following APIs enabled:
   - BigQuery API
   - Cloud Composer API
   - Cloud Storage API

2. **Service Account** with the following roles:
   - BigQuery Admin
   - BigQuery Job User
   - Storage Admin
   - Composer Worker

3. **Google Cloud Composer Environment** (Airflow 2.x)


## Data Models

### Seeds (Raw Data)
- **affiliates_extended**
- **players_extended**
- **transactions_extended**

### Models
1. **model_1_daily_player_transactions**: Daily transaction aggregations per player
2. **model_2_kyc_discord_deposits**: Discord players with KYC approval and deposits per country
3. **model_3_top_three_deposits**: Top 3 largest deposits per player

### Snapshots
- **player_kyc_status_snapshot**: Tracks KYC status changes over time

## Configuration

### dbt Configuration
- **Target**: BigQuery (`assessment-project-aeta`)
- **Dataset**: `technical_assessment`
- **Location**: US
- **Authentication**: Service Account JSON key

### Airflow DAG
- **Schedule**: Daily
- **Tasks**: debug → deps → seed → snapshot → run → test

## Testing

The pipeline includes comprehensive testing:

### dbt Tests
- **Generic tests**: unique, not_null constraints
- **Custom tests**: 
  - Deposits are positive
  - Withdrawals are negative
  - KYC Discord filtering logic

### Data Quality
- Schema validation
- Data type consistency
- Business rule validation
