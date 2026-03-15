# HazardCast

**AI-powered disaster prediction engine for US counties.**

Predicts which US counties will receive FEMA disaster declarations up to 90 days in advance, using an XGBoost model trained on 42 engineered features from 7 federal data sources spanning 60+ years.

## What This Is

A full ML pipeline — from raw federal data to deployed predictions:

1. **Ingests** data from FEMA, USGS, NOAA, Census Bureau, US Drought Monitor, NIFC, and NFIP (all free, no API keys)
2. **Engineers** 42 features per county per month across 10 domains, including multi-hazard cascade interaction terms
3. **Trains** XGBoost binary classifier with temporal train/val/test split (2000–2021 / 2022 / 2023–2024)
4. **Serves** real-time predictions via REST API with top risk factors, seasonality context, and data freshness warnings

## Data Sources

| Source | Records | Timespan | What |
|--------|---------|----------|------|
| FEMA Disaster Declarations | ~65,000 | 1953–present | Every federal disaster declaration by county |
| USGS Earthquakes | ~500,000 | 1964–present | M2.5+ seismic events with coordinates |
| NOAA Storm Events | ~1,500,000 | 2000–present | Tornadoes, floods, hurricanes, hail + casualties/damage |
| US Census | 3,143 | Current | County demographics, housing, economics |
| US Drought Monitor | ~3,000,000 | 2000–present | Weekly drought severity by county (D0–D4) |
| NIFC Wildfires | ~100,000 | 2000–present | Wildfire incidents with acres burned |
| NFIP Flood Claims | ~2,500,000 | 1978–present | Flood insurance claims and payouts |

## Feature Vector (42 features per county-month)

| Domain | Count | Features |
|--------|-------|----------|
| **FEMA History** | 7 | Declarations in 1/3/5/10yr windows, total, months since last, major disaster ratio, IA program ratio |
| **Storm Events** | 10 | Event count 1yr/5yr, deaths, injuries, property/crop damage, tornado/flood/hail counts, max F-scale |
| **Socioeconomic** | 5 | Population, housing units, median home value, density, land area |
| **Drought** | 4 | Severity avg/max 5yr, severe drought weeks, D4 max percentage |
| **Wildfire** | 4 | Count 1yr/5yr, acres burned 5yr, max acres |
| **NFIP** | 3 | Claim count 5yr, total payout, avg payout |
| **Spatial** | 2 | Neighbor county avg, state avg declarations |
| **Cascade** | 7 | Drought×fire, fire×flood, hurricane×flood, earthquake×landslide interactions, storm compound count, active chains, max chain length |
| **Target** | 1 | FEMA declaration in next 90 days (binary) |

> **Note:** Temporal features (month of year, season flags) were evaluated but excluded — ablation showed they cause seasonal overfitting (+0.018 AUC when removed). The cascade interaction features already capture relevant seasonal dynamics.

### Cascade Interaction Features

Unlike single-domain features, cascade features capture **multiplicative co-occurrence** of hazard precursors:

- **Drought × Wildfire**: `drought_severity_6mo × log1p(wildfire_acres_1yr)` — only high when dry fuel meets active fire
- **Wildfire × Flood**: `log1p(burn_scar_acres_18mo) × flood_events_1yr` — post-fire debris flow risk
- **Hurricane × Flood**: `hurricane_declarations_60d × flood_events_30d` — inland flood amplification
- **Earthquake × Landslide**: `significant_quakes_90d × severe_storms_30d` — seismic + saturated slope failure
- **Storm Compound**: Rapid-succession severe storms in 30 days

## Quick Start

```bash
# Start Postgres
docker compose up -d

# Run with Postgres profile
./gradlew bootRun --args='--spring.profiles.active=postgres'

# Or use H2 (zero setup, data persists to ./data/)
./gradlew bootRun
```

### Run the Pipeline

```bash
# Ingest all data sources (~10 minutes)
curl -X POST "http://localhost:8080/api/pipeline/ingest?startYear=2000&endYear=2024"

# Compute features for a month
curl -X POST "http://localhost:8080/api/pipeline/features?yearMonth=2024-01"

# Export to Parquet for ML training
curl -X POST http://localhost:8080/api/pipeline/export

# Or run everything end-to-end
curl -X POST "http://localhost:8080/api/pipeline/run?startYear=2000&endYear=2024"
```

### Train the Model

```bash
cd ml/
pip install -r requirements.txt

# Train XGBoost with temporal split + Optuna tuning
python train_xgboost.py --input ../output/us-county-hazard-features.parquet --tune

# Compare against FEMA's National Risk Index
python compare_nri.py --input ../output/us-county-hazard-features.parquet
```

### Query Predictions

```bash
# Predict for Los Angeles County (FIPS 06037)
curl http://localhost:8080/api/predict/06037

# National heatmap data
curl http://localhost:8080/api/predict/national?yearMonth=2024-06
```

Example response:
```json
{
  "fips": "06037",
  "county": "Los Angeles",
  "state": "CA",
  "probability": 0.73,
  "yearMonth": "2024-06",
  "modelType": "xgboost_v4",
  "topFactors": {
    "declarations_5yr": 12,
    "storm_property_damage_5yr": 850000,
    "cascade_drought_fire_risk": 341.2
  },
  "seasonality": {
    "isWildfireSeason": true,
    "monthOfYear": 6
  }
}
```

## Key Results

| Metric | Value |
|--------|-------|
| **XGBoost ROC-AUC** | 0.9006 [0.8976, 0.9035] |
| **vs FEMA NRI** | 0.9421 vs 0.5462 (county-level AUC) |
| **Temporal stability** | mean 0.9135, std 0.023 across 24 months |
| **Best per-type recall** | Tornado 0.82, Flood 0.79 |
| **Cascade ablation** | -0.0033 AUC when removed |

### Extended Analyses

- **Ablation study**: FEMA declaration history provides 99% of discriminative power (AUC drops from 0.89 to 0.63 without it)
- **No-FEMA model variant**: Predicts from hazard signals alone (AUC 0.60) — useful for counties with no declaration history
- **SHAP interaction analysis**: Feature interaction strengths via TreeExplainer, confirming FEMA history as dominant signal
- **Declaration equity**: Using the no-FEMA model, low-income counties (Q1) show significantly higher prediction residuals than wealthy counties (Q4), p < 10^-252 — suggesting structural inequities in federal disaster declarations
- **NRI comparison**: HazardCast outperforms FEMA's static National Risk Index by 72% in county-level AUC (0.9421 vs 0.5462)

## Project Structure

```
src/main/java/com/hazardcast/
├── model/                  # JPA entities (County, DisasterDeclaration, StormEvent, etc.)
├── repository/             # Spring Data repositories with custom JPQL queries
├── ingestion/              # Federal API clients (FEMA, USGS, NOAA, USDM, NIFC, NFIP)
├── pipeline/
│   ├── feature/            # Feature computers (one per domain, strategy pattern)
│   ├── FeatureEngineer     # Orchestrates all feature computers
│   └── ParquetExporter     # Exports to ML-ready Parquet
├── scoring/                # XGBoost4J inference (MachineLearningScorer)
├── api/                    # REST controllers (prediction, pipeline)
└── config/                 # App configuration

ml/
├── train_xgboost.py        # XGBoost training with temporal split + ablation study
├── compare_nri.py          # Benchmark against FEMA National Risk Index
└── requirements.txt
```

## Tech Stack

- **Java 17** + Spring Boot 3.3.5
- **XGBoost4J** for native Java model inference
- **PipelineOrchestrator** for data pipeline orchestration
- **Spring Data JPA** with PostgreSQL (or H2 for dev)
- **WebClient** for async HTTP to federal APIs
- **Apache Parquet** for ML-ready dataset export
- **Testcontainers** for integration tests
- **Python** (XGBoost, pandas, scikit-learn) for model training

## Requirements

- Java 17+
- Docker (for Postgres) or use embedded H2
- Gradle 9+
- Python 3.10+ (for ML training only)

## Tests

```bash
./gradlew test    # 30 tests — unit + Spring integration
```

Test coverage includes:
- Feature computation (all 11 domains including cascade interactions)
- Prediction API (404s, stale data warnings, missing features)
- ML scorer (feature count validation, null handling)
- Data ingestion (FEMA window queries, deduplication)

## License

Apache 2.0
