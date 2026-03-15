"""
HazardCast — Compare against FEMA National Risk Index (NRI)

Downloads NRI county-level composite risk scores and correlates them
with HazardCast's XGBoost predictions. This acknowledges FEMA's prior
art and measures how our predictions complement or overlap with NRI.

Usage:
    python compare_nri.py --input ../output/us-county-hazard-features.parquet --model model/hazardcast_xgb.json

Output:
    - results/nri_comparison.json   (correlation stats + analysis)
    - results/nri_scatter.png       (scatter plot: NRI vs HazardCast)
    - results/nri_by_rating.png     (box plot: our scores by NRI rating)
"""

import argparse
import json
import os
import warnings

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import xgboost as xgb
from sklearn.metrics import roc_auc_score
from scipy import stats

warnings.filterwarnings("ignore", category=UserWarning)

# NRI ArcGIS endpoint — county-level composite + per-hazard scores
NRI_URL = (
    "https://services.arcgis.com/XG15cJAlne2vxtgt/arcgis/rest/services/"
    "National_Risk_Index_Counties/FeatureServer/0/query"
    "?where=1%3D1"
    "&outFields=STCOFIPS,RISK_SCORE,RISK_RATNG,EAL_SCORE,SOVI_SCORE,RESL_SCORE"
    "&returnGeometry=false&f=json"
    "&resultOffset={offset}&resultRecordCount=2000"
)

# Must exactly match FEATURE_COLS in train_xgboost.py (42 features)
# Imported from training script to avoid drift
from train_xgboost import FEATURE_COLS


def download_nri() -> pd.DataFrame:
    """Download all NRI county scores via ArcGIS REST API."""
    print("Downloading FEMA NRI county scores...")
    all_records = []
    offset = 0

    while True:
        url = NRI_URL.format(offset=offset)
        resp = requests.get(url, timeout=30)
        data = resp.json()

        features = data.get("features", [])
        if not features:
            break

        for f in features:
            attrs = f["attributes"]
            all_records.append(attrs)

        print(f"  Fetched {len(all_records)} counties...")
        if len(features) < 2000:
            break
        offset += 2000

    df = pd.DataFrame(all_records)
    df = df.rename(columns={"STCOFIPS": "fips"})
    print(f"  Total NRI counties: {len(df)}")
    return df


def get_hazardcast_predictions(parquet_path: str, model_path: str) -> pd.DataFrame:
    """Get HazardCast average prediction per county (most recent year)."""
    print(f"Loading HazardCast data from {parquet_path}...")
    df = pd.read_parquet(parquet_path)

    # Use most recent 12 months for comparison
    recent = df[df["year_month"] >= "2024-01"].copy()
    print(f"  Using {len(recent)} records from 2024+")

    # Load model
    model = xgb.XGBClassifier()
    model.load_model(model_path)

    available = [c for c in FEATURE_COLS if c in recent.columns]
    X = recent[available].fillna(0).values
    probs = model.predict_proba(X)[:, 1]
    recent["hazardcast_prob"] = probs

    # Average prediction per county
    county_avg = recent.groupby("fips").agg(
        hazardcast_prob=("hazardcast_prob", "mean"),
        actual_rate=("declaration_next_90d", "mean"),
    ).reset_index()

    print(f"  Counties with predictions: {len(county_avg)}")
    return county_avg


def analyze(nri: pd.DataFrame, hc: pd.DataFrame, results_dir: str):
    """Merge and analyze NRI vs HazardCast."""
    merged = pd.merge(hc, nri, on="fips", how="inner")
    merged = merged.dropna(subset=["RISK_SCORE", "hazardcast_prob"])
    print(f"\nMerged counties (with valid NRI scores): {len(merged)}")

    # Correlations
    pearson_r, pearson_p = stats.pearsonr(merged["RISK_SCORE"], merged["hazardcast_prob"])
    spearman_r, spearman_p = stats.spearmanr(merged["RISK_SCORE"], merged["hazardcast_prob"])

    # NRI vs actual
    nri_actual_r, _ = stats.pearsonr(merged["RISK_SCORE"], merged["actual_rate"])

    # HazardCast vs actual
    hc_actual_r, _ = stats.pearsonr(merged["hazardcast_prob"], merged["actual_rate"])

    print(f"\n--- Correlation Analysis ---")
    print(f"  NRI ↔ HazardCast:  Pearson r={pearson_r:.4f} (p={pearson_p:.2e}), "
          f"Spearman ρ={spearman_r:.4f}")
    print(f"  NRI ↔ Actual rate: Pearson r={nri_actual_r:.4f}")
    print(f"  HC  ↔ Actual rate: Pearson r={hc_actual_r:.4f}")

    # ROC-AUC comparison: use median actual_rate as binary threshold
    binary_actual = (merged["actual_rate"] > 0).astype(int)
    if binary_actual.sum() > 0 and binary_actual.sum() < len(binary_actual):
        nri_auc = roc_auc_score(binary_actual, merged["RISK_SCORE"])
        hc_auc = roc_auc_score(binary_actual, merged["hazardcast_prob"])
        print(f"\n  Predicting 'any declaration in 2024' (county-level):")
        print(f"    NRI ROC-AUC:        {nri_auc:.4f}")
        print(f"    HazardCast ROC-AUC: {hc_auc:.4f}")
    else:
        nri_auc = hc_auc = None

    # By NRI rating category
    print(f"\n--- HazardCast scores by NRI Rating ---")
    rating_order = ["Very Low", "Relatively Low", "Relatively Moderate",
                    "Relatively High", "Very High"]
    existing_ratings = [r for r in rating_order if r in merged["RISK_RATNG"].values]

    print(f"  {'NRI Rating':<22} {'Count':>6} {'HC Prob':>8} {'Actual':>8}")
    print("  " + "-" * 50)
    rating_stats = {}
    for rating in existing_ratings:
        subset = merged[merged["RISK_RATNG"] == rating]
        hc_mean = subset["hazardcast_prob"].mean()
        actual_mean = subset["actual_rate"].mean()
        rating_stats[rating] = {
            "count": len(subset),
            "hazardcast_mean": round(hc_mean, 4),
            "actual_rate_mean": round(actual_mean, 4),
        }
        print(f"  {rating:<22} {len(subset):>6} {hc_mean:>8.4f} {actual_mean:>8.4f}")

    # --- Plots ---

    # 1. Scatter: NRI vs HazardCast
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    ax = axes[0]
    ax.scatter(merged["RISK_SCORE"], merged["hazardcast_prob"],
               alpha=0.3, s=8, c="#2196F3")
    ax.set_xlabel("FEMA NRI Risk Score (0-100)")
    ax.set_ylabel("HazardCast Predicted Probability")
    ax.set_title(f"FEMA NRI vs HazardCast (r={pearson_r:.3f})")
    # Add trend line
    z = np.polyfit(merged["RISK_SCORE"], merged["hazardcast_prob"], 1)
    p = np.poly1d(z)
    x_line = np.linspace(merged["RISK_SCORE"].min(), merged["RISK_SCORE"].max(), 100)
    ax.plot(x_line, p(x_line), "r--", linewidth=1.5, alpha=0.7)

    # 2. Box plot: HC scores by NRI rating
    ax = axes[1]
    box_data = [merged[merged["RISK_RATNG"] == r]["hazardcast_prob"].values
                for r in existing_ratings]
    bp = ax.boxplot(box_data, tick_labels=[r.replace("Relatively ", "Rel.\n") for r in existing_ratings],
                    patch_artist=True)
    colors = ["#4CAF50", "#8BC34A", "#FFC107", "#FF9800", "#F44336"]
    for patch, color in zip(bp["boxes"], colors[:len(existing_ratings)]):
        patch.set_facecolor(color)
        patch.set_alpha(0.6)
    ax.set_ylabel("HazardCast Predicted Probability")
    ax.set_title("HazardCast Predictions by NRI Rating")

    plt.tight_layout()
    plt.savefig(f"{results_dir}/nri_comparison.png", dpi=150)
    plt.close()
    print(f"\n  Saved: {results_dir}/nri_comparison.png")

    # --- Save report ---
    report = {
        "description": "Comparison of HazardCast predictions vs FEMA National Risk Index (NRI)",
        "nri_version": "December 2025 v1.20",
        "counties_compared": len(merged),
        "correlation": {
            "nri_vs_hazardcast_pearson": round(pearson_r, 4),
            "nri_vs_hazardcast_spearman": round(spearman_r, 4),
            "nri_vs_actual_pearson": round(nri_actual_r, 4),
            "hazardcast_vs_actual_pearson": round(hc_actual_r, 4),
        },
        "county_level_auc": {
            "nri_roc_auc": round(nri_auc, 4) if nri_auc else None,
            "hazardcast_roc_auc": round(hc_auc, 4) if hc_auc else None,
        },
        "by_nri_rating": rating_stats,
        "interpretation": [
            "NRI is a static, expert-weighted composite index (18 hazard types, social vulnerability, resilience).",
            "HazardCast is a dynamic, ML-based temporal prediction (probability of FEMA declaration in next 90 days).",
            "Moderate correlation is expected: both capture hazard exposure, but HazardCast adds temporal dynamics.",
            "HazardCast predictions should increase monotonically with NRI rating (higher NRI = higher risk).",
            "The two systems are complementary: NRI for long-term planning, HazardCast for short-term prediction.",
        ],
    }

    class NumpyEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, (np.floating, np.float32, np.float64)):
                return float(obj)
            if isinstance(obj, (np.integer, np.int64)):
                return int(obj)
            return super().default(obj)

    with open(f"{results_dir}/nri_comparison.json", "w") as f:
        json.dump(report, f, indent=2, cls=NumpyEncoder)
    print(f"  Saved: {results_dir}/nri_comparison.json")

    return report


def main():
    parser = argparse.ArgumentParser(description="Compare HazardCast vs FEMA NRI")
    parser.add_argument("--input", required=True, help="Path to Parquet feature file")
    parser.add_argument("--model", default="model/hazardcast_xgb.json", help="XGBoost model path")
    parser.add_argument("--results-dir", default="results", help="Output directory")
    args = parser.parse_args()

    os.makedirs(args.results_dir, exist_ok=True)

    nri = download_nri()
    hc = get_hazardcast_predictions(args.input, args.model)
    report = analyze(nri, hc, args.results_dir)

    print(f"\n{'=' * 60}")
    print("NRI COMPARISON COMPLETE")
    print(f"{'=' * 60}")
    r = report["correlation"]
    print(f"  NRI ↔ HazardCast: r={r['nri_vs_hazardcast_pearson']}")
    print(f"  NRI ↔ Actual:     r={r['nri_vs_actual_pearson']}")
    print(f"  HC  ↔ Actual:     r={r['hazardcast_vs_actual_pearson']}")
    if report["county_level_auc"]["nri_roc_auc"]:
        print(f"  NRI AUC:          {report['county_level_auc']['nri_roc_auc']}")
        print(f"  HC  AUC:          {report['county_level_auc']['hazardcast_roc_auc']}")


if __name__ == "__main__":
    main()
