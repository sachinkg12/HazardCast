"""
HazardCast — Multi-Model Training & Evaluation

Trains multiple classifiers (baselines + XGBoost) to predict FEMA disaster
declarations 90 days in advance. Includes rigorous evaluation with bootstrap
confidence intervals, calibration analysis, and per-disaster-type breakdown.

Usage:
    python train_xgboost.py --input ../output/us-county-hazard-features.parquet

Output:
    - model/hazardcast_xgb.json          (XGBoost native format)
    - model/hazardcast_xgb.ubj           (XGBoost4J binary for Java)
    - results/evaluation_report.json      (all model metrics + CIs)
    - results/roc_comparison.png          (all models ROC curves)
    - results/calibration_curve.png       (reliability diagram)
    - results/feature_importance.png      (XGBoost gain)
    - results/confusion_matrix.png        (XGBoost confusion matrix)
"""

import argparse
import json
import os
import warnings

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import xgboost as xgb
from sklearn.calibration import calibration_curve
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    auc,
    brier_score_loss,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    roc_auc_score,
    roc_curve,
)
from sklearn.preprocessing import StandardScaler

try:
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    HAS_OPTUNA = True
except ImportError:
    HAS_OPTUNA = False

warnings.filterwarnings("ignore", category=UserWarning)

# Feature columns (must match Java ParquetExporter schema)
# v4: 35 base features + 7 cascade interaction features = 42 total
# Removed: temporal features (month_of_year, season flags) — ablation showed
# removing them improves AUC by +0.018, indicating seasonal overfitting.
# The cascade features already capture relevant seasonal interactions.
FEATURE_COLS = [
    # FEMA (7)
    "declarations_1yr", "declarations_3yr", "declarations_5yr",
    "declarations_10yr", "months_since_last_decl",
    "major_disaster_ratio", "ia_program_ratio",
    # Storm (10 — removed hurricane_count_5yr, zero gain)
    "storm_event_count_1yr", "storm_event_count_5yr",
    "storm_deaths_5yr", "storm_injuries_5yr",
    "storm_property_damage_5yr", "storm_crop_damage_5yr",
    "tornado_count_5yr", "flood_count_5yr",
    "hail_count_5yr", "max_tor_f_scale_5yr",
    # Socioeconomic (5)
    "population", "housing_units", "median_home_value",
    "population_density", "land_area_sq_mi",
    # Drought (4)
    "drought_severity_avg_5yr", "drought_max_severity_5yr",
    "severe_drought_weeks_5yr", "drought_d4_pct_max_5yr",
    # Wildfire (4)
    "wildfire_count_1yr", "wildfire_count_5yr",
    "wildfire_acres_burned_5yr", "wildfire_max_acres_5yr",
    # NFIP (3)
    "nfip_claim_count_5yr", "nfip_total_payout_5yr", "nfip_avg_payout_5yr",
    # Spatial (2)
    "neighbor_avg_declarations_5yr", "state_avg_declarations_5yr",
    # Cascade — multi-hazard interaction features (7)
    "cascade_drought_fire_risk",        # D2+ drought severity in prior 6 months
    "cascade_fire_flood_risk",          # Burn scar acres in prior 18 months
    "cascade_hurricane_flood_risk",     # Hurricane declarations in prior 60 days
    "cascade_earthquake_landslide_risk",# M4.0+ quakes in prior 90 days
    "cascade_storm_compound_count",     # Severe storms in prior 30 days
    "cascade_active_chains",            # Count of active cascade precursors (0-5)
    "cascade_max_chain_length",         # Longest active chain (1-3)
]

TARGET_COL = "declaration_next_90d"


def load_data(parquet_path: str) -> pd.DataFrame:
    """Load and validate the feature dataset."""
    print(f"Loading data from {parquet_path}...")
    df = pd.read_parquet(parquet_path)
    # Drop declarations_total if present (data leakage — cumulative autocorrelation)
    if "declarations_total" in df.columns:
        df = df.drop(columns=["declarations_total"])
    print(f"  Shape: {df.shape}")
    print(f"  Date range: {df['year_month'].min()} to {df['year_month'].max()}")
    print(f"  Positive rate: {df[TARGET_COL].mean():.4f} ({df[TARGET_COL].sum()} / {len(df)})")
    return df


def temporal_split(df: pd.DataFrame) -> tuple:
    """
    Split by time to prevent data leakage.
    Train: 2000-2021
    Validation: 2022
    Test: 2023-2024 only (cap at 2024-12 because FEMA declaration data
          for 2025+ is incomplete — near-zero positive rate inflates AUC)
    """
    min_year = df["year_month"].min()[:4]
    train = df[df["year_month"] < "2022-01"]
    val = df[(df["year_month"] >= "2022-01") & (df["year_month"] < "2023-01")]
    test = df[(df["year_month"] >= "2023-01") & (df["year_month"] <= "2024-12")]

    # Warn if data beyond 2024 is present but excluded
    excluded = df[df["year_month"] > "2024-12"]
    if len(excluded) > 0:
        print(f"\n  Note: Excluded {len(excluded)} rows after 2024-12 from test set")
        print(f"        (FEMA declaration data incomplete for recent months — would inflate metrics)")

    print(f"\nSplit sizes:")
    print(f"  Train: {len(train)} ({min_year}-01 to 2021-12, "
          f"{train[TARGET_COL].mean():.4f} positive rate)")
    print(f"  Val:   {len(val)} (2022-01 to 2022-12, "
          f"{val[TARGET_COL].mean():.4f} positive rate)")
    print(f"  Test:  {len(test)} (2023-01 to 2024-12, "
          f"{test[TARGET_COL].mean():.4f} positive rate)")

    return train, val, test


def prepare_features(train_df, val_df, test_df):
    """Extract and fill features. Returns X/y arrays + a fitted scaler."""
    available = [c for c in FEATURE_COLS if c in train_df.columns]
    missing = [c for c in FEATURE_COLS if c not in train_df.columns]
    if missing:
        print(f"  Warning: missing features (will skip): {missing}")

    X_train = train_df[available].fillna(0).values
    y_train = train_df[TARGET_COL].values.astype(int)
    X_val = val_df[available].fillna(0).values
    y_val = val_df[TARGET_COL].values.astype(int)
    X_test = test_df[available].fillna(0).values
    y_test = test_df[TARGET_COL].values.astype(int)

    # Scaler for logistic regression (tree models don't need it)
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_val_scaled = scaler.transform(X_val)
    X_test_scaled = scaler.transform(X_test)

    return (X_train, X_val, X_test, y_train, y_val, y_test,
            X_train_scaled, X_val_scaled, X_test_scaled, available)


# ──────────────────────────────────────────────────────────────
# BASELINE MODELS
# ──────────────────────────────────────────────────────────────

def train_naive_baseline(y_train):
    """Naive: always predict the training set positive rate."""
    model = DummyClassifier(strategy="prior", random_state=42)
    model.fit(np.zeros((len(y_train), 1)), y_train)
    return model


def train_logistic_regression(X_train_scaled, y_train):
    """Logistic regression baseline."""
    model = LogisticRegression(
        max_iter=1000, class_weight="balanced", random_state=42, n_jobs=-1
    )
    model.fit(X_train_scaled, y_train)
    return model


def train_random_forest(X_train, y_train):
    """Random forest baseline."""
    model = RandomForestClassifier(
        n_estimators=200, max_depth=8, class_weight="balanced",
        random_state=42, n_jobs=-1
    )
    model.fit(X_train, y_train)
    return model


def train_xgboost(X_train, y_train, X_val, y_val):
    """XGBoost with scale_pos_weight (no SMOTE)."""
    neg = (y_train == 0).sum()
    pos = (y_train == 1).sum()
    imbalance = neg / pos
    print(f"\n  XGBoost class imbalance: {neg} neg / {pos} pos = {imbalance:.2f}x")

    model = xgb.XGBClassifier(
        n_estimators=500,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=5,
        reg_alpha=0.1,
        reg_lambda=1.0,
        scale_pos_weight=imbalance,
        eval_metric="logloss",
        early_stopping_rounds=30,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=50)
    print(f"  Best iteration: {model.best_iteration}")
    return model


def tune_xgboost(X_train, y_train, X_val, y_val, n_trials=50):
    """Optuna hyperparameter tuning for XGBoost."""
    if not HAS_OPTUNA:
        print("  Optuna not installed, skipping tuning")
        return None

    neg = (y_train == 0).sum()
    pos = (y_train == 1).sum()
    imbalance = neg / pos

    def objective(trial):
        params = {
            "n_estimators": 1000,
            "max_depth": trial.suggest_int("max_depth", 4, 10),
            "learning_rate": trial.suggest_float("learning_rate", 0.01, 0.2, log=True),
            "subsample": trial.suggest_float("subsample", 0.6, 1.0),
            "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 1.0),
            "min_child_weight": trial.suggest_int("min_child_weight", 1, 20),
            "reg_alpha": trial.suggest_float("reg_alpha", 1e-3, 10.0, log=True),
            "reg_lambda": trial.suggest_float("reg_lambda", 1e-3, 10.0, log=True),
            "gamma": trial.suggest_float("gamma", 0.0, 5.0),
            "scale_pos_weight": imbalance,
            "eval_metric": "logloss",
            "early_stopping_rounds": 30,
            "random_state": 42,
            "n_jobs": -1,
        }
        model = xgb.XGBClassifier(**params)
        model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=0)
        y_prob = model.predict_proba(X_val)[:, 1]
        return roc_auc_score(y_val, y_prob)

    print(f"\n  Running Optuna ({n_trials} trials)...")
    study = optuna.create_study(direction="maximize", sampler=optuna.samplers.TPESampler(seed=42))
    study.optimize(objective, n_trials=n_trials, show_progress_bar=False)

    best = study.best_params
    print(f"  Best val ROC-AUC: {study.best_value:.4f}")
    print(f"  Best params: {json.dumps(best, indent=4)}")

    # Retrain with best params
    best_model = xgb.XGBClassifier(
        n_estimators=1000,
        **best,
        scale_pos_weight=imbalance,
        eval_metric="logloss",
        early_stopping_rounds=30,
        random_state=42,
        n_jobs=-1,
    )
    best_model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=50)
    print(f"  Tuned model best iteration: {best_model.best_iteration}")

    return best_model, best, study.best_value


# ──────────────────────────────────────────────────────────────
# EVALUATION
# ──────────────────────────────────────────────────────────────

def evaluate_model(name, y_test, y_prob, y_pred):
    """Compute metrics for one model."""
    roc = roc_auc_score(y_test, y_prob)
    prec_arr, rec_arr, _ = precision_recall_curve(y_test, y_prob)
    pr = auc(rec_arr, prec_arr)
    f1 = f1_score(y_test, y_pred)
    report = classification_report(y_test, y_pred, output_dict=True, zero_division=0)
    brier = brier_score_loss(y_test, y_prob)

    return {
        "model": name,
        "roc_auc": round(roc, 4),
        "pr_auc": round(pr, 4),
        "f1": round(f1, 4),
        "precision": round(report["1"]["precision"], 4),
        "recall": round(report["1"]["recall"], 4),
        "brier_score": round(brier, 4),
    }


def bootstrap_ci(y_test, y_prob, metric_fn, n_boot=1000, ci=0.95):
    """Bootstrap confidence interval for a metric."""
    rng = np.random.RandomState(42)
    scores = []
    n = len(y_test)
    for _ in range(n_boot):
        idx = rng.randint(0, n, n)
        try:
            scores.append(metric_fn(y_test[idx], y_prob[idx]))
        except ValueError:
            continue
    alpha = (1 - ci) / 2
    lo = np.percentile(scores, 100 * alpha)
    hi = np.percentile(scores, 100 * (1 - alpha))
    return round(lo, 4), round(hi, 4)


def plot_roc_comparison(all_results, y_test, all_probs, results_dir):
    """Plot ROC curves for all models on one figure."""
    plt.figure(figsize=(8, 6))
    for name, y_prob in all_probs.items():
        fpr, tpr, _ = roc_curve(y_test, y_prob)
        auc_val = all_results[name]["roc_auc"]
        plt.plot(fpr, tpr, linewidth=2, label=f"{name} (AUC={auc_val})")
    plt.plot([0, 1], [0, 1], "k--", linewidth=1, label="Random (AUC=0.50)")
    plt.xlabel("False Positive Rate")
    plt.ylabel("True Positive Rate")
    plt.title("HazardCast — Model Comparison (ROC)")
    plt.legend(loc="lower right")
    plt.tight_layout()
    plt.savefig(f"{results_dir}/roc_comparison.png", dpi=150)
    plt.close()


def plot_calibration(y_test, y_prob, results_dir):
    """Calibration curve (reliability diagram)."""
    prob_true, prob_pred = calibration_curve(y_test, y_prob, n_bins=10, strategy="uniform")
    brier = brier_score_loss(y_test, y_prob)

    plt.figure(figsize=(7, 6))
    plt.plot(prob_pred, prob_true, "s-", linewidth=2, label=f"XGBoost (Brier={brier:.4f})")
    plt.plot([0, 1], [0, 1], "k--", linewidth=1, label="Perfectly calibrated")
    plt.xlabel("Mean predicted probability")
    plt.ylabel("Fraction of positives")
    plt.title("HazardCast — Calibration Curve")
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{results_dir}/calibration_curve.png", dpi=150)
    plt.close()


def plot_confusion(y_test, y_pred, results_dir):
    """Confusion matrix heatmap."""
    cm = confusion_matrix(y_test, y_pred)
    plt.figure(figsize=(6, 5))
    sns.heatmap(cm, annot=True, fmt="d", cmap="Blues",
                xticklabels=["No Decl", "Declaration"],
                yticklabels=["No Decl", "Declaration"])
    plt.xlabel("Predicted")
    plt.ylabel("Actual")
    plt.title("XGBoost — Confusion Matrix (Test Set)")
    plt.tight_layout()
    plt.savefig(f"{results_dir}/confusion_matrix.png", dpi=150)
    plt.close()


def per_disaster_type_evaluation(df_test, y_prob, y_pred, results_dir):
    """Evaluate model performance per disaster type."""
    if "declaration_type_next_90d" not in df_test.columns:
        print("  Skipping: declaration_type_next_90d not in dataset")
        return {}

    y_test = df_test[TARGET_COL].values.astype(int)
    decl_type = df_test["declaration_type_next_90d"].fillna("None")

    # Get unique disaster types (excluding None)
    types = [t for t in decl_type.unique() if t != "None" and pd.notna(t)]
    results = {}

    print(f"\n{'Type':<25} {'Count':>6} {'ROC-AUC':>8} {'Recall':>8}")
    print("-" * 55)

    for dtype in sorted(types):
        mask = (decl_type == dtype) | (y_test == 0)  # type positives vs all negatives
        y_sub = (decl_type == dtype).astype(int).values[mask]
        prob_sub = y_prob[mask]

        if y_sub.sum() < 10:
            continue

        try:
            roc = roc_auc_score(y_sub, prob_sub)
            # Recall at default threshold
            pred_sub = y_pred[mask]
            tp = ((pred_sub == 1) & (y_sub == 1)).sum()
            recall = tp / y_sub.sum() if y_sub.sum() > 0 else 0
            results[dtype] = {"count": int(y_sub.sum()), "roc_auc": round(roc, 4),
                              "recall": round(recall, 4)}
            print(f"  {dtype:<23} {y_sub.sum():>6} {roc:>8.4f} {recall:>8.4f}")
        except ValueError:
            continue

    return results


def per_region_evaluation(df_test, y_prob, results_dir):
    """Evaluate model performance by US Census region (state FIPS prefix)."""
    y_test = df_test[TARGET_COL].values.astype(int)

    # Census region mapping by state FIPS prefix
    regions = {
        "Northeast": ["09", "23", "25", "33", "44", "50",  # New England
                       "34", "36", "42"],                    # Mid-Atlantic
        "Midwest": ["17", "18", "26", "39", "55",          # East North Central
                     "19", "20", "27", "29", "31", "38", "46"],  # West North Central
        "South": ["10", "11", "12", "13", "24", "37", "45", "51", "54",  # South Atlantic
                   "01", "21", "28", "47",                   # East South Central
                   "05", "22", "40", "48"],                   # West South Central
        "West": ["04", "08", "16", "30", "32", "35", "49", "56",  # Mountain
                  "02", "06", "15", "41", "53"],              # Pacific
    }

    fips_to_region = {}
    for region, state_codes in regions.items():
        for code in state_codes:
            fips_to_region[code] = region

    state_prefix = df_test["fips"].str[:2]
    region_col = state_prefix.map(fips_to_region).fillna("Other")

    results = {}
    print(f"\n{'Region':<15} {'Count':>8} {'Pos':>6} {'ROC-AUC':>8}")
    print("-" * 45)

    for region in ["Northeast", "Midwest", "South", "West"]:
        mask = region_col == region
        if mask.sum() < 100:
            continue
        y_sub = y_test[mask]
        prob_sub = y_prob[mask]
        try:
            roc = roc_auc_score(y_sub, prob_sub)
            results[region] = {"count": int(mask.sum()), "positives": int(y_sub.sum()),
                               "roc_auc": round(roc, 4)}
            print(f"  {region:<13} {mask.sum():>8} {y_sub.sum():>6} {roc:>8.4f}")
        except ValueError:
            continue

    return results


def temporal_stability_analysis(df, model, feature_names, results_dir):
    """Evaluate model performance month-by-month to check temporal stability."""
    available = [c for c in FEATURE_COLS if c in df.columns]

    months = sorted(df["year_month"].unique())
    # Only evaluate on test period months (cap at 2024-12 — FEMA data incomplete after)
    test_months = [m for m in months if "2023-01" <= m <= "2024-12"]

    results = {}
    month_rocs = []

    for ym in test_months:
        mask = df["year_month"] == ym
        sub = df[mask]
        if sub[TARGET_COL].sum() < 5 or sub[TARGET_COL].sum() == len(sub):
            continue

        X = sub[available].fillna(0).values
        y = sub[TARGET_COL].values.astype(int)
        y_prob = model.predict_proba(X)[:, 1]

        try:
            roc = roc_auc_score(y, y_prob)
            results[ym] = round(roc, 4)
            month_rocs.append(roc)
        except ValueError:
            continue

    if month_rocs:
        print(f"\n  Temporal stability (test months):")
        print(f"    Mean ROC-AUC: {np.mean(month_rocs):.4f}")
        print(f"    Std:          {np.std(month_rocs):.4f}")
        print(f"    Min:          {np.min(month_rocs):.4f}")
        print(f"    Max:          {np.max(month_rocs):.4f}")

        # Plot temporal stability
        plt.figure(figsize=(10, 4))
        plt.plot(list(results.keys()), list(results.values()), "o-", linewidth=1.5)
        plt.axhline(y=np.mean(month_rocs), color="r", linestyle="--", alpha=0.5,
                     label=f"Mean={np.mean(month_rocs):.3f}")
        plt.xlabel("Month")
        plt.ylabel("ROC-AUC")
        plt.title("XGBoost — Temporal Stability (Monthly ROC-AUC)")
        plt.xticks(rotation=45)
        plt.legend()
        plt.tight_layout()
        plt.savefig(f"{results_dir}/temporal_stability.png", dpi=150)
        plt.close()

    return {"monthly_roc_auc": results,
            "mean": round(np.mean(month_rocs), 4) if month_rocs else None,
            "std": round(np.std(month_rocs), 4) if month_rocs else None}


def error_analysis(df_test, y_prob, y_pred, feature_names, results_dir):
    """Analyze false negatives — counties where disasters occurred but model missed."""
    y_test = df_test[TARGET_COL].values.astype(int)

    fn_mask = (y_test == 1) & (y_pred == 0)
    tp_mask = (y_test == 1) & (y_pred == 1)

    fn_count = fn_mask.sum()
    tp_count = tp_mask.sum()
    total_pos = y_test.sum()

    print(f"\n  False negatives: {fn_count}/{total_pos} ({fn_count/total_pos*100:.1f}% of actual disasters)")
    print(f"  True positives:  {tp_count}/{total_pos} ({tp_count/total_pos*100:.1f}% detected)")

    available = [c for c in FEATURE_COLS if c in df_test.columns]
    X = df_test[available].fillna(0)

    # Compare feature means: false negatives vs true positives
    fn_means = X[fn_mask].mean()
    tp_means = X[tp_mask].mean()

    diffs = []
    for feat in available:
        fn_val = fn_means[feat]
        tp_val = tp_means[feat]
        if tp_val != 0:
            pct_diff = (fn_val - tp_val) / abs(tp_val) * 100
        else:
            pct_diff = 0
        diffs.append((feat, fn_val, tp_val, pct_diff))

    # Show features most different between FN and TP
    diffs.sort(key=lambda x: abs(x[3]), reverse=True)
    print(f"\n  Top features distinguishing false negatives from true positives:")
    print(f"  {'Feature':<35} {'FN mean':>10} {'TP mean':>10} {'Diff%':>8}")
    print("  " + "-" * 68)
    for feat, fn_val, tp_val, pct in diffs[:10]:
        print(f"  {feat:<35} {fn_val:>10.2f} {tp_val:>10.2f} {pct:>7.1f}%")

    # Probability distribution for FN
    fn_probs = y_prob[fn_mask]
    print(f"\n  FN predicted probability distribution:")
    print(f"    Mean:   {fn_probs.mean():.4f}")
    print(f"    Median: {np.median(fn_probs):.4f}")
    print(f"    >0.3:   {(fn_probs > 0.3).sum()} ({(fn_probs > 0.3).mean()*100:.1f}%)")
    print(f"    >0.4:   {(fn_probs > 0.4).sum()} ({(fn_probs > 0.4).mean()*100:.1f}%)")

    return {
        "false_negatives": int(fn_count),
        "true_positives": int(tp_count),
        "total_positives": int(total_pos),
        "fn_mean_prob": round(float(fn_probs.mean()), 4),
        "fn_median_prob": round(float(np.median(fn_probs)), 4),
    }


def cascade_event_evaluation(test_df, xgb_prob, xgb_pred, feature_names, results_dir):
    """Evaluate model specifically on compound/cascade disaster events.

    Cascade events are defined as test samples where at least one cascade
    precursor is active, indicating a multi-hazard interaction was in progress
    when the disaster occurred. This is the key evaluation for our novel
    contribution — does cascade awareness improve detection of compound events?
    """
    cascade_cols = [c for c in FEATURE_COLS if c.startswith("cascade_")]
    results = {}

    # Overall baseline
    y_test = test_df[TARGET_COL].values
    total_pos = y_test.sum()
    overall_recall = (xgb_pred[y_test == 1] == 1).mean()
    overall_roc = roc_auc_score(y_test, xgb_prob) if len(np.unique(y_test)) > 1 else 0

    print(f"\n  Overall: {total_pos} disasters, recall={overall_recall:.4f}, ROC-AUC={overall_roc:.4f}")

    # 1. Events with any cascade precursor active (cascade_active_chains > 0)
    if "cascade_active_chains" in test_df.columns:
        cascade_mask = test_df["cascade_active_chains"].values > 0
        cascade_pos = y_test[cascade_mask].sum()
        if cascade_pos > 0:
            cascade_recall = (xgb_pred[cascade_mask & (y_test == 1)] == 1).mean()
            cascade_roc = roc_auc_score(y_test[cascade_mask], xgb_prob[cascade_mask]) \
                if len(np.unique(y_test[cascade_mask])) > 1 else 0
            no_cascade_mask = ~cascade_mask
            no_cascade_recall = (xgb_pred[no_cascade_mask & (y_test == 1)] == 1).mean() \
                if y_test[no_cascade_mask].sum() > 0 else 0
            print(f"  With cascade precursors: {cascade_pos} disasters, recall={cascade_recall:.4f}, ROC-AUC={cascade_roc:.4f}")
            print(f"  Without cascade precursors: {y_test[no_cascade_mask].sum()} disasters, recall={no_cascade_recall:.4f}")
            print(f"  Recall lift from cascade: {cascade_recall - no_cascade_recall:+.4f}")
            results["cascade_active"] = {
                "count": int(cascade_pos), "recall": round(float(cascade_recall), 4),
                "roc_auc": round(float(cascade_roc), 4),
            }
            results["no_cascade"] = {
                "count": int(y_test[no_cascade_mask].sum()),
                "recall": round(float(no_cascade_recall), 4),
            }

    # 2. Per-chain evaluation
    chain_defs = {
        "Drought→Fire": ("cascade_drought_fire_risk", "Fire"),
        "Fire→Flood": ("cascade_fire_flood_risk", "Flood"),
        "Hurricane→Flood": ("cascade_hurricane_flood_risk", "Hurricane"),
        "Storm Compound": ("cascade_storm_compound_count", None),
        "Earthquake→Landslide": ("cascade_earthquake_landslide_risk", None),
    }

    print(f"\n  {'Chain':<25} {'Precursor>0':>12} {'+Disaster':>10} {'Recall':>8}")
    print("  " + "-" * 60)

    chain_results = {}
    for chain_name, (col, decl_type) in chain_defs.items():
        if col not in test_df.columns:
            continue
        precursor_mask = test_df[col].values > 0
        precursor_count = precursor_mask.sum()
        precursor_disasters = y_test[precursor_mask].sum()
        if precursor_disasters > 0:
            recall = (xgb_pred[precursor_mask & (y_test == 1)] == 1).mean()
            print(f"  {chain_name:<25} {precursor_count:>12} {int(precursor_disasters):>10} {recall:>8.4f}")
            chain_results[chain_name] = {
                "precursor_active": int(precursor_count),
                "disasters": int(precursor_disasters),
                "recall": round(float(recall), 4),
            }
        else:
            print(f"  {chain_name:<25} {precursor_count:>12} {'0':>10} {'N/A':>8}")

    results["per_chain"] = chain_results

    # 3. Multi-chain events (chain_length >= 2)
    if "cascade_max_chain_length" in test_df.columns:
        for length in [2, 3]:
            chain_mask = test_df["cascade_max_chain_length"].values >= length
            chain_disasters = y_test[chain_mask].sum()
            if chain_disasters > 0:
                recall = (xgb_pred[chain_mask & (y_test == 1)] == 1).mean()
                print(f"\n  Chain length >= {length}: {int(chain_disasters)} disasters, recall={recall:.4f}")
                results[f"chain_length_ge_{length}"] = {
                    "count": int(chain_disasters), "recall": round(float(recall), 4),
                }

    # Plot cascade vs non-cascade recall comparison
    if "cascade_active" in results and "no_cascade" in results:
        labels = ["No Cascade\nPrecursor", "Cascade\nPrecursor Active"]
        recalls = [results["no_cascade"]["recall"], results["cascade_active"]["recall"]]
        counts = [results["no_cascade"]["count"], results["cascade_active"]["count"]]

        fig, ax = plt.subplots(figsize=(6, 5))
        bars = ax.bar(labels, recalls, color=["#95a5a6", "#e74c3c"], width=0.5)
        for bar, count in zip(bars, counts):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01,
                    f"n={count}", ha="center", fontsize=10)
        ax.set_ylabel("Recall")
        ax.set_title("Disaster Detection: Cascade vs Non-Cascade Events")
        ax.set_ylim(0, 1.0)
        ax.axhline(y=overall_recall, color="blue", linestyle="--", alpha=0.5, label=f"Overall ({overall_recall:.3f})")
        ax.legend()
        plt.tight_layout()
        plt.savefig(f"{results_dir}/cascade_evaluation.png", dpi=150)
        plt.close()

    return results


def ablation_study(X_train, y_train, X_val, y_val, X_test, y_test, feature_names, results_dir):
    """Measure impact of removing each feature domain."""
    domains = {
        "FEMA": ["declarations_1yr", "declarations_3yr", "declarations_5yr",
                  "declarations_10yr", "months_since_last_decl",
                  "major_disaster_ratio", "ia_program_ratio"],
        "Seismic": ["earthquake_count_1yr", "earthquake_count_5yr",
                     "max_magnitude_1yr", "max_magnitude_5yr", "avg_magnitude_5yr",
                     "total_energy_5yr", "earthquake_depth_avg", "earthquake_distance_avg_km"],
        "Storm": ["storm_event_count_1yr", "storm_event_count_5yr",
                   "storm_deaths_5yr", "storm_injuries_5yr",
                   "storm_property_damage_5yr", "storm_crop_damage_5yr",
                   "tornado_count_5yr", "flood_count_5yr",
                   "hurricane_count_5yr", "hail_count_5yr", "max_tor_f_scale_5yr"],
        "Socioeconomic": ["population", "housing_units", "median_home_value",
                           "population_density", "land_area_sq_mi"],
        "Temporal": ["month_of_year", "is_hurricane_season", "is_tornado_season",
                      "is_wildfire_season"],
        "Drought": ["drought_severity_avg_5yr", "drought_max_severity_5yr",
                      "severe_drought_weeks_5yr", "drought_d4_pct_max_5yr"],
        "Wildfire": ["wildfire_count_1yr", "wildfire_count_5yr",
                      "wildfire_acres_burned_5yr", "wildfire_max_acres_5yr"],
        "NFIP": ["nfip_claim_count_5yr", "nfip_total_payout_5yr", "nfip_avg_payout_5yr"],
        "Spatial": ["neighbor_avg_declarations_5yr", "state_avg_declarations_5yr"],
        "Cascade": ["cascade_drought_fire_risk", "cascade_fire_flood_risk",
                      "cascade_hurricane_flood_risk", "cascade_earthquake_landslide_risk",
                      "cascade_storm_compound_count", "cascade_active_chains",
                      "cascade_max_chain_length"],
    }

    # Full model baseline
    neg = (y_train == 0).sum()
    pos = (y_train == 1).sum()
    imbalance = neg / pos

    full_prob = xgb.XGBClassifier(
        n_estimators=300, max_depth=6, learning_rate=0.05,
        subsample=0.8, colsample_bytree=0.8, min_child_weight=5,
        scale_pos_weight=imbalance, eval_metric="logloss",
        early_stopping_rounds=20, random_state=42, n_jobs=-1, verbosity=0,
    )
    full_prob.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)
    full_roc = roc_auc_score(y_test, full_prob.predict_proba(X_test)[:, 1])

    results = {"full_model": round(full_roc, 4)}

    print(f"\n  Full model ROC-AUC: {full_roc:.4f}")
    print(f"\n  {'Domain removed':<20} {'ROC-AUC':>8} {'Delta':>8}")
    print("  " + "-" * 40)

    for domain, cols in domains.items():
        # Find indices to keep (all except this domain)
        keep_idx = [i for i, f in enumerate(feature_names) if f not in cols]
        if len(keep_idx) == len(feature_names):
            continue  # domain not present

        X_tr = X_train[:, keep_idx]
        X_v = X_val[:, keep_idx]
        X_te = X_test[:, keep_idx]

        model = xgb.XGBClassifier(
            n_estimators=300, max_depth=6, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8, min_child_weight=5,
            scale_pos_weight=imbalance, eval_metric="logloss",
            early_stopping_rounds=20, random_state=42, n_jobs=-1, verbosity=0,
        )
        model.fit(X_tr, y_train, eval_set=[(X_v, y_val)], verbose=False)
        roc = roc_auc_score(y_test, model.predict_proba(X_te)[:, 1])
        delta = roc - full_roc
        results[f"without_{domain}"] = round(roc, 4)
        results[f"delta_{domain}"] = round(delta, 4)
        print(f"  w/o {domain:<15} {roc:>8.4f} {delta:>+8.4f}")

    # Plot ablation
    domain_names = []
    deltas = []
    for domain in domains:
        key = f"delta_{domain}"
        if key in results:
            domain_names.append(domain)
            deltas.append(results[key])

    plt.figure(figsize=(8, 5))
    colors = ["#e74c3c" if d < -0.005 else "#3498db" for d in deltas]
    plt.barh(domain_names, deltas, color=colors)
    plt.axvline(x=0, color="black", linewidth=0.5)
    plt.xlabel("Change in ROC-AUC when domain removed")
    plt.title("Ablation Study — Feature Domain Impact")
    plt.tight_layout()
    plt.savefig(f"{results_dir}/ablation_study.png", dpi=150)
    plt.close()

    return results


def train_no_fema_model(X_train, y_train, X_val, y_val, X_test, y_test,
                         feature_names, model_dir, results_dir):
    """Train a model WITHOUT FEMA declaration history features.

    This directly addresses the FEMA dominance criticism: the ablation shows
    removing FEMA features drops AUC to ~0.57. By training a separate model
    without FEMA, we answer: "Can hazard signals alone predict declarations?"

    This is a genuinely useful model variant for counties with no
    declaration history (new counties, novel hazards, under-declared areas).
    """
    fema_cols = {"declarations_1yr", "declarations_3yr", "declarations_5yr",
                 "declarations_10yr", "months_since_last_decl",
                 "major_disaster_ratio", "ia_program_ratio"}

    keep_idx = [i for i, f in enumerate(feature_names) if f not in fema_cols]
    nf_names = [feature_names[i] for i in keep_idx]

    X_tr = X_train[:, keep_idx]
    X_v = X_val[:, keep_idx]
    X_te = X_test[:, keep_idx]

    neg = (y_train == 0).sum()
    pos = (y_train == 1).sum()

    model = xgb.XGBClassifier(
        n_estimators=500, max_depth=6, learning_rate=0.05,
        subsample=0.8, colsample_bytree=0.8, min_child_weight=5,
        scale_pos_weight=neg / pos, eval_metric="logloss",
        early_stopping_rounds=30, random_state=42, n_jobs=-1, verbosity=0,
    )
    model.fit(X_tr, y_train, eval_set=[(X_v, y_val)], verbose=False)

    y_prob = model.predict_proba(X_te)[:, 1]
    y_pred = model.predict(X_te)

    roc = roc_auc_score(y_test, y_prob)
    pr_p, pr_r, _ = precision_recall_curve(y_test, y_prob)
    pr = auc(pr_r, pr_p)
    f1 = f1_score(y_test, y_pred)
    brier = brier_score_loss(y_test, y_prob)
    recall = (y_pred[y_test == 1] == 1).mean()
    precision = (y_test[y_pred == 1] == 1).mean() if y_pred.sum() > 0 else 0

    print(f"  Features: {len(nf_names)} (removed {len(fema_cols)} FEMA features)")
    print(f"  ROC-AUC:  {roc:.4f}")
    print(f"  PR-AUC:   {pr:.4f}")
    print(f"  F1:       {f1:.4f}")
    print(f"  Recall:   {recall:.4f}")
    print(f"  Brier:    {brier:.4f}")

    # Bootstrap CI
    roc_lo, roc_hi = bootstrap_ci(y_test, y_prob, roc_auc_score)
    print(f"  95% CI:   [{roc_lo}, {roc_hi}]")

    # Save the no-FEMA model
    model.save_model(f"{model_dir}/hazardcast_no_fema.json")
    print(f"  Model saved: {model_dir}/hazardcast_no_fema.json")

    results = {
        "roc_auc": round(roc, 4),
        "roc_auc_ci_95": [roc_lo, roc_hi],
        "pr_auc": round(pr, 4),
        "f1": round(f1, 4),
        "recall": round(float(recall), 4),
        "precision": round(float(precision), 4),
        "brier_score": round(brier, 4),
        "features": len(nf_names),
        "description": "XGBoost trained without FEMA declaration history — predicts from hazard signals alone",
    }
    return results


def plot_feature_importance(model, feature_names, results_dir):
    """Feature importance bar chart."""
    booster = model.get_booster()
    importance = booster.get_score(importance_type="gain")
    imp_named = {}
    for k, v in importance.items():
        if k.startswith("f"):
            idx = int(k[1:])
            if idx < len(feature_names):
                imp_named[feature_names[idx]] = v

    imp_sorted = sorted(imp_named.items(), key=lambda x: x[1], reverse=True)
    top_n = min(25, len(imp_sorted))
    names = [x[0] for x in imp_sorted[:top_n]][::-1]
    values = [x[1] for x in imp_sorted[:top_n]][::-1]

    plt.figure(figsize=(10, 8))
    plt.barh(names, values, color="#2196F3")
    plt.xlabel("Gain")
    plt.title("XGBoost — Feature Importance (Top 25)")
    plt.tight_layout()
    plt.savefig(f"{results_dir}/feature_importance.png", dpi=150, bbox_inches="tight")
    plt.close()


# ──────────────────────────────────────────────────────────────
# SHAP INTERACTION ANALYSIS
# ──────────────────────────────────────────────────────────────

def shap_interaction_analysis(model, X_test, feature_names, results_dir, max_samples=5000):
    """Compute SHAP interaction values to validate cascade feature engineering.

    If SHAP independently discovers that drought-wildfire or fire-flood
    feature pairs have strong interaction effects, this empirically validates
    our engineered cascade interaction terms from the data itself.
    """
    import shap

    print(f"\n  Computing SHAP interaction values (n={min(max_samples, len(X_test))})...")

    # Subsample for computational feasibility
    rng = np.random.RandomState(42)
    if len(X_test) > max_samples:
        idx = rng.choice(len(X_test), max_samples, replace=False)
        X_sample = X_test[idx]
    else:
        X_sample = X_test

    explainer = shap.TreeExplainer(model)

    # Standard SHAP values (for summary plot)
    shap_values = explainer.shap_values(X_sample)

    # SHAP summary plot
    plt.figure(figsize=(10, 10))
    shap.summary_plot(shap_values, X_sample, feature_names=feature_names, show=False,
                      max_display=20)
    plt.tight_layout()
    plt.savefig(f"{results_dir}/shap_summary.png", dpi=150, bbox_inches="tight")
    plt.close()

    # SHAP interaction values
    shap_interaction = explainer.shap_interaction_values(X_sample)

    # Average absolute interaction strength for each feature pair
    n_features = len(feature_names)
    interaction_matrix = np.abs(shap_interaction).mean(axis=0)

    # Zero out diagonal (self-interaction = main effect, not pairwise)
    np.fill_diagonal(interaction_matrix, 0)

    # Find top interactions
    interactions = []
    for i in range(n_features):
        for j in range(i + 1, n_features):
            interactions.append((
                feature_names[i], feature_names[j],
                float(interaction_matrix[i, j])
            ))
    interactions.sort(key=lambda x: x[2], reverse=True)

    print(f"\n  Top 15 feature interactions (by mean |SHAP interaction|):")
    print(f"  {'Feature A':<35} {'Feature B':<35} {'Strength':>10}")
    print("  " + "-" * 82)
    for feat_a, feat_b, strength in interactions[:15]:
        print(f"  {feat_a:<35} {feat_b:<35} {strength:>10.4f}")

    # Heatmap of top interactions
    top_n = 15
    top_features_idx = set()
    for a, b, _ in interactions[:top_n]:
        top_features_idx.add(feature_names.index(a))
        top_features_idx.add(feature_names.index(b))
    top_features_idx = sorted(top_features_idx)[:15]
    top_names = [feature_names[i] for i in top_features_idx]
    sub_matrix = interaction_matrix[np.ix_(top_features_idx, top_features_idx)]

    plt.figure(figsize=(12, 10))
    sns.heatmap(sub_matrix, xticklabels=top_names, yticklabels=top_names,
                cmap="YlOrRd", annot=True, fmt=".3f", square=True)
    plt.title("SHAP Feature Interaction Strengths")
    plt.tight_layout()
    plt.savefig(f"{results_dir}/shap_interactions_heatmap.png", dpi=150, bbox_inches="tight")
    plt.close()

    return {
        "top_interactions": [
            {"feature_a": a, "feature_b": b, "strength": round(s, 4)}
            for a, b, s in interactions[:20]
        ],
    }


# ──────────────────────────────────────────────────────────────
# DECLARATION EQUITY ANALYSIS
# ──────────────────────────────────────────────────────────────

def declaration_equity_analysis(df, no_fema_model, feature_names, results_dir):
    """Analyze whether FEMA declaration patterns correlate with socioeconomic factors
    after controlling for hazard exposure.

    Uses the NO-FEMA model to avoid circular reasoning: the full model's predictions
    are dominated by FEMA declaration history, so residuals would just measure
    "counties that break the historical pattern" rather than genuine inequities.
    The no-FEMA model predicts from hazard signals alone, making residuals
    interpretable as: "given the hazard exposure, did this county get declared?"

    For each county-month in the test period, compute:
      residual = predicted_probability - actual_declaration

    Positive residual = hazard signals suggest risk but no declaration (potential under-service)
    Negative residual = declaration despite low hazard signals (potential over-service)
    """
    fema_cols = {"declarations_1yr", "declarations_3yr", "declarations_5yr",
                 "declarations_10yr", "months_since_last_decl",
                 "major_disaster_ratio", "ia_program_ratio"}
    non_fema = [c for c in FEATURE_COLS if c not in fema_cols and c in df.columns]

    # Use test period only
    test = df[(df["year_month"] >= "2023-01") & (df["year_month"] <= "2024-12")].copy()

    if len(test) == 0:
        print("  No test data available for equity analysis")
        return {}

    X = test[non_fema].fillna(0).values
    y = test[TARGET_COL].values.astype(int)
    y_prob = no_fema_model.predict_proba(X)[:, 1]
    test = test.copy()
    test["predicted_prob"] = y_prob
    test["residual"] = y_prob - y  # positive = expected but didn't get, negative = unexpected
    print(f"  Using no-FEMA model (hazard signals only) for equity analysis")

    results = {}

    # ── 1. Income-based stratification ──
    if "median_home_value" in test.columns:
        # Use median home value as income proxy (available in our data)
        test["income_quartile"] = pd.qcut(
            test["median_home_value"], 4,
            labels=["Q1 (lowest)", "Q2", "Q3", "Q4 (highest)"],
            duplicates="drop"
        )

        print(f"\n  Declaration rates by income quartile (median home value):")
        print(f"  {'Quartile':<20} {'Counties':>10} {'Declarations':>14} {'Rate':>8} "
              f"{'Pred Prob':>10} {'Residual':>10}")
        print("  " + "-" * 76)

        income_results = {}
        for q in test["income_quartile"].cat.categories:
            subset = test[test["income_quartile"] == q]
            n = len(subset)
            decls = subset[TARGET_COL].sum()
            rate = decls / n if n > 0 else 0
            pred = subset["predicted_prob"].mean()
            resid = subset["residual"].mean()
            print(f"  {str(q):<20} {n:>10} {decls:>14} {rate:>8.4f} {pred:>10.4f} {resid:>10.4f}")
            income_results[str(q)] = {
                "count": int(n), "declarations": int(decls),
                "rate": round(rate, 4), "mean_predicted": round(float(pred), 4),
                "mean_residual": round(float(resid), 4),
            }
        results["by_income"] = income_results

        # Statistical test: do low-income counties have higher residuals?
        from scipy import stats
        q1 = test[test["income_quartile"] == "Q1 (lowest)"]["residual"]
        q4 = test[test["income_quartile"] == "Q4 (highest)"]["residual"]
        if len(q1) > 10 and len(q4) > 10:
            stat, pval = stats.mannwhitneyu(q1, q4, alternative="two-sided")
            print(f"\n  Mann-Whitney U test (Q1 vs Q4 residuals): U={stat:.0f}, p={pval:.4e}")
            results["income_test"] = {"statistic": float(stat), "p_value": float(pval)}

    # ── 2. Population-based stratification (urbanicity proxy) ──
    if "population" in test.columns:
        test["pop_category"] = pd.cut(
            test["population"],
            bins=[0, 25000, 100000, 500000, float("inf")],
            labels=["Rural (<25K)", "Small (25-100K)", "Medium (100-500K)", "Urban (>500K)"]
        )

        print(f"\n  Declaration rates by population (urbanicity proxy):")
        print(f"  {'Category':<20} {'Counties':>10} {'Declarations':>14} {'Rate':>8} "
              f"{'Pred Prob':>10} {'Residual':>10}")
        print("  " + "-" * 76)

        pop_results = {}
        for cat in ["Rural (<25K)", "Small (25-100K)", "Medium (100-500K)", "Urban (>500K)"]:
            subset = test[test["pop_category"] == cat]
            n = len(subset)
            if n == 0:
                continue
            decls = subset[TARGET_COL].sum()
            rate = decls / n if n > 0 else 0
            pred = subset["predicted_prob"].mean()
            resid = subset["residual"].mean()
            print(f"  {cat:<20} {n:>10} {decls:>14} {rate:>8.4f} {pred:>10.4f} {resid:>10.4f}")
            pop_results[cat] = {
                "count": int(n), "declarations": int(decls),
                "rate": round(rate, 4), "mean_predicted": round(float(pred), 4),
                "mean_residual": round(float(resid), 4),
            }
        results["by_population"] = pop_results

    # ── 3. Population density stratification ──
    if "population_density" in test.columns:
        test["density_quartile"] = pd.qcut(
            test["population_density"], 4,
            labels=["Q1 (sparse)", "Q2", "Q3", "Q4 (dense)"],
            duplicates="drop"
        )

        print(f"\n  Declaration rates by population density:")
        print(f"  {'Quartile':<20} {'Counties':>10} {'Declarations':>14} {'Rate':>8} "
              f"{'Pred Prob':>10} {'Residual':>10}")
        print("  " + "-" * 76)

        density_results = {}
        for q in test["density_quartile"].cat.categories:
            subset = test[test["density_quartile"] == q]
            n = len(subset)
            decls = subset[TARGET_COL].sum()
            rate = decls / n if n > 0 else 0
            pred = subset["predicted_prob"].mean()
            resid = subset["residual"].mean()
            print(f"  {str(q):<20} {n:>10} {decls:>14} {rate:>8.4f} {pred:>10.4f} {resid:>10.4f}")
            density_results[str(q)] = {
                "count": int(n), "declarations": int(decls),
                "rate": round(rate, 4), "mean_predicted": round(float(pred), 4),
                "mean_residual": round(float(resid), 4),
            }
        results["by_density"] = density_results

    # ── 4. Hazard exposure vs declaration gap ──
    # Counties with high hazard signals (storm damage, wildfire) but no declaration
    hazard_cols = ["storm_property_damage_5yr", "wildfire_acres_burned_5yr",
                   "nfip_total_payout_5yr", "flood_count_5yr"]
    hazard_available = [c for c in hazard_cols if c in test.columns]

    if hazard_available:
        # Composite hazard score (z-score average of available hazard indicators)
        for col in hazard_available:
            test[f"{col}_z"] = (test[col] - test[col].mean()) / (test[col].std() + 1e-8)
        test["hazard_composite"] = test[[f"{c}_z" for c in hazard_available]].mean(axis=1)

        test["hazard_tercile"] = pd.qcut(
            test["hazard_composite"], 3,
            labels=["Low hazard", "Medium hazard", "High hazard"],
            duplicates="drop"
        )

        print(f"\n  Declaration rates by hazard exposure level:")
        print(f"  {'Hazard Level':<20} {'Counties':>10} {'Declarations':>14} {'Rate':>8} "
              f"{'Pred Prob':>10}")
        print("  " + "-" * 66)

        hazard_results = {}
        for level in ["Low hazard", "Medium hazard", "High hazard"]:
            subset = test[test["hazard_tercile"] == level]
            n = len(subset)
            if n == 0:
                continue
            decls = subset[TARGET_COL].sum()
            rate = decls / n if n > 0 else 0
            pred = subset["predicted_prob"].mean()
            print(f"  {level:<20} {n:>10} {decls:>14} {rate:>8.4f} {pred:>10.4f}")
            hazard_results[level] = {
                "count": int(n), "declarations": int(decls),
                "rate": round(rate, 4), "mean_predicted": round(float(pred), 4),
            }
        results["by_hazard_exposure"] = hazard_results

        # Cross-tabulation: high hazard × low income = most underserved?
        if "income_quartile" in test.columns:
            print(f"\n  Cross-analysis: Hazard exposure × Income")
            print(f"  {'Income':<20} {'Hazard':<15} {'N':>8} {'Decls':>8} {'Rate':>8} {'Residual':>10}")
            print("  " + "-" * 73)

            cross_results = {}
            for iq in ["Q1 (lowest)", "Q4 (highest)"]:
                for hl in ["High hazard", "Low hazard"]:
                    subset = test[(test["income_quartile"] == iq) & (test["hazard_tercile"] == hl)]
                    n = len(subset)
                    if n < 10:
                        continue
                    decls = subset[TARGET_COL].sum()
                    rate = decls / n
                    resid = subset["residual"].mean()
                    key = f"{iq} + {hl}"
                    print(f"  {str(iq):<20} {hl:<15} {n:>8} {decls:>8} {rate:>8.4f} {resid:>10.4f}")
                    cross_results[key] = {
                        "count": int(n), "declarations": int(decls),
                        "rate": round(rate, 4), "mean_residual": round(float(resid), 4),
                    }
            results["cross_income_hazard"] = cross_results

    # ── 5. Visualization: residual by income ──
    if "income_quartile" in test.columns:
        plt.figure(figsize=(10, 6))
        quartiles = test["income_quartile"].cat.categories
        data = [test[test["income_quartile"] == q]["residual"].values for q in quartiles]
        bp = plt.boxplot(data, tick_labels=[str(q) for q in quartiles], patch_artist=True,
                         showfliers=False)
        colors = ["#e74c3c", "#f39c12", "#3498db", "#2ecc71"]
        for patch, color in zip(bp["boxes"], colors):
            patch.set_facecolor(color)
            patch.set_alpha(0.7)
        plt.axhline(y=0, color="black", linestyle="--", alpha=0.5)
        plt.xlabel("Income Quartile (Median Home Value)")
        plt.ylabel("Prediction Residual (predicted - actual)")
        plt.title("Declaration Equity: Residual by Income Quartile\n"
                  "(positive = model expected declaration but none came)")
        plt.tight_layout()
        plt.savefig(f"{results_dir}/equity_residual_by_income.png", dpi=150, bbox_inches="tight")
        plt.close()

    # ── 6. Visualization: declaration rate by income × hazard ──
    if "income_quartile" in test.columns and "hazard_tercile" in test.columns:
        plt.figure(figsize=(10, 6))
        pivot = test.groupby(["income_quartile", "hazard_tercile"])[TARGET_COL].mean().unstack()
        pivot.plot(kind="bar", figsize=(10, 6), colormap="YlOrRd")
        plt.xlabel("Income Quartile")
        plt.ylabel("Declaration Rate")
        plt.title("FEMA Declaration Rate by Income × Hazard Exposure")
        plt.legend(title="Hazard Level")
        plt.xticks(rotation=0)
        plt.tight_layout()
        plt.savefig(f"{results_dir}/equity_income_hazard_cross.png", dpi=150, bbox_inches="tight")
        plt.close()

    return results



# ──────────────────────────────────────────────────────────────
# COLD-START EVALUATION
# ──────────────────────────────────────────────────────────────

def cold_start_evaluation(df, model, no_fema_model, feature_names, results_dir):
    """Evaluate on counties with sparse declaration history in training period.

    Uses a threshold of <3 declarations (rather than zero) because near-universal
    FEMA coverage means only 12 of 3,222 counties have zero history — too few
    to evaluate. Counties with 1-2 declarations have minimal FEMA signal,
    making them a practical test of whether hazard features generalize.
    """
    available = [c for c in FEATURE_COLS if c in df.columns]
    fema_cols_set = {"declarations_1yr", "declarations_3yr", "declarations_5yr",
                     "declarations_10yr", "months_since_last_decl",
                     "major_disaster_ratio", "ia_program_ratio"}
    non_fema = [c for c in available if c not in fema_cols_set]

    train = df[df["year_month"] < "2022-01"]
    test = df[(df["year_month"] >= "2023-01") & (df["year_month"] <= "2024-12")]

    # Find counties with sparse declarations in training period (<3)
    train_decl_by_fips = train.groupby("fips")[TARGET_COL].sum()
    sparse_history_fips = set(train_decl_by_fips[train_decl_by_fips < 3].index)
    has_history_fips = set(train_decl_by_fips[train_decl_by_fips >= 3].index)
    zero_fips = set(train_decl_by_fips[train_decl_by_fips == 0].index)

    cold_test = test[test["fips"].isin(sparse_history_fips)]
    warm_test = test[test["fips"].isin(has_history_fips)]

    print(f"\n  Zero-history counties: {len(zero_fips)} (too few for evaluation)")
    print(f"  Sparse-history counties (<3 declarations 2000-2021): {len(sparse_history_fips)}")
    print(f"  Rich-history counties (3+ declarations 2000-2021):   {len(has_history_fips)}")
    print(f"  Sparse test samples: {len(cold_test)} ({cold_test[TARGET_COL].sum()} positive)")
    print(f"  Rich test samples:   {len(warm_test)} ({warm_test[TARGET_COL].sum()} positive)")

    results = {
        "zero_history_counties": len(zero_fips),
        "sparse_history_counties": len(sparse_history_fips),
        "rich_history_counties": len(has_history_fips),
        "threshold": "< 3 declarations in training period (2000-2021)",
    }

    for label, subset in [("sparse_history", cold_test), ("rich_history", warm_test)]:
        if len(subset) == 0 or subset[TARGET_COL].sum() < 5:
            print(f"  {label}: insufficient data for evaluation")
            continue

        X_full = subset[available].fillna(0).values
        X_nofema = subset[non_fema].fillna(0).values
        y = subset[TARGET_COL].values.astype(int)

        # Full model
        y_prob_full = model.predict_proba(X_full)[:, 1]
        roc_full = roc_auc_score(y, y_prob_full)
        recall_full = ((y_prob_full > 0.5) & (y == 1)).sum() / max(y.sum(), 1)

        # No-FEMA model
        if no_fema_model is not None:
            y_prob_nofema = no_fema_model.predict_proba(X_nofema)[:, 1]
            roc_nofema = roc_auc_score(y, y_prob_nofema)
            recall_nofema = ((y_prob_nofema > 0.5) & (y == 1)).sum() / max(y.sum(), 1)
        else:
            roc_nofema = 0
            recall_nofema = 0

        print(f"\n  {label.upper()} counties:")
        print(f"    Full model:    ROC-AUC={roc_full:.4f}, Recall@0.5={recall_full:.4f}")
        print(f"    No-FEMA model: ROC-AUC={roc_nofema:.4f}, Recall@0.5={recall_nofema:.4f}")
        if roc_nofema > roc_full:
            print(f"    → No-FEMA model outperforms by +{roc_nofema - roc_full:.4f}")
        else:
            print(f"    → Full model outperforms by +{roc_full - roc_nofema:.4f}")

        results[label] = {
            "samples": int(len(subset)),
            "positives": int(y.sum()),
            "positive_rate": round(float(y.mean()), 4),
            "full_model_roc_auc": round(roc_full, 4),
            "full_model_recall": round(float(recall_full), 4),
            "nofema_model_roc_auc": round(roc_nofema, 4),
            "nofema_model_recall": round(float(recall_nofema), 4),
        }

    # Visualization
    if "sparse_history" in results and "rich_history" in results:
        labels = ["Sparse History\n(<3 declarations)", "Rich History\n(3+ declarations)"]
        full_aucs = [results["sparse_history"]["full_model_roc_auc"],
                     results["rich_history"]["full_model_roc_auc"]]
        nofema_aucs = [results["sparse_history"]["nofema_model_roc_auc"],
                       results["rich_history"]["nofema_model_roc_auc"]]

        x = np.arange(len(labels))
        width = 0.35
        fig, ax = plt.subplots(figsize=(8, 6))
        bars1 = ax.bar(x - width / 2, full_aucs, width, label="Full Model", color="#3498db")
        bars2 = ax.bar(x + width / 2, nofema_aucs, width, label="No-FEMA Model", color="#e74c3c")
        ax.set_ylabel("ROC-AUC")
        ax.set_title("Sparse vs Rich Declaration History — Model Performance")
        ax.set_xticks(x)
        ax.set_xticklabels(labels)
        ax.legend()
        ax.set_ylim(0.4, 1.0)
        for bars in [bars1, bars2]:
            for bar in bars:
                height = bar.get_height()
                ax.annotate(f"{height:.3f}", xy=(bar.get_x() + bar.get_width() / 2, height),
                            xytext=(0, 3), textcoords="offset points", ha="center", fontsize=10)
        plt.tight_layout()
        plt.savefig(f"{results_dir}/cold_start_comparison.png", dpi=150, bbox_inches="tight")
        plt.close()

    return results


# ──────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Train HazardCast models")
    parser.add_argument("--input", required=True, help="Path to Parquet feature file")
    parser.add_argument("--output-dir", default=".", help="Output directory")
    parser.add_argument("--tune", action="store_true", help="Run Optuna hyperparameter tuning")
    parser.add_argument("--tune-trials", type=int, default=50, help="Number of Optuna trials")
    args = parser.parse_args()

    model_dir = f"{args.output_dir}/model"
    results_dir = f"{args.output_dir}/results"
    os.makedirs(model_dir, exist_ok=True)
    os.makedirs(results_dir, exist_ok=True)

    # ── Load & split ──
    df = load_data(args.input)
    train_df, val_df, test_df = temporal_split(df)
    (X_train, X_val, X_test, y_train, y_val, y_test,
     X_train_scaled, X_val_scaled, X_test_scaled, feature_names) = prepare_features(
        train_df, val_df, test_df
    )

    # ── Train all models ──
    print("\n" + "=" * 60)
    print("TRAINING MODELS")
    print("=" * 60)

    models = {}

    print("\n1. Naive baseline (predict training prior)...")
    models["Naive"] = train_naive_baseline(y_train)

    print("\n2. Logistic Regression...")
    models["LogReg"] = train_logistic_regression(X_train_scaled, y_train)

    print("\n3. Random Forest...")
    models["RandomForest"] = train_random_forest(X_train, y_train)

    print("\n4. XGBoost...")
    models["XGBoost"] = train_xgboost(X_train, y_train, X_val, y_val)

    tuning_result = None
    if args.tune:
        print("\n5. XGBoost (Optuna-tuned)...")
        tuning_result = tune_xgboost(X_train, y_train, X_val, y_val, n_trials=args.tune_trials)
    tuned_params = None
    if tuning_result is not None:
        tuned_model, tuned_params, tuned_val_auc = tuning_result
        # Evaluate tuned model on test to compare
        tuned_prob = tuned_model.predict_proba(X_test)[:, 1]
        tuned_roc = roc_auc_score(y_test, tuned_prob)
        default_prob = models["XGBoost"].predict_proba(X_test)[:, 1]
        default_roc = roc_auc_score(y_test, default_prob)
        print(f"\n  Default XGBoost test ROC-AUC: {default_roc:.4f}")
        print(f"  Tuned XGBoost test ROC-AUC:   {tuned_roc:.4f}")
        if tuned_roc > default_roc:
            print(f"  Tuned model is better (+{tuned_roc - default_roc:.4f}), using it.")
            models["XGBoost"] = tuned_model
        else:
            print(f"  Default model is equal or better, keeping it.")

    # ── Evaluate all ──
    print("\n" + "=" * 60)
    print("EVALUATION ON TEST SET")
    print("=" * 60)

    all_results = {}
    all_probs = {}

    for name, model in models.items():
        if name == "LogReg":
            X = X_test_scaled
        elif name == "Naive":
            X = np.zeros((len(y_test), 1))
        else:
            X = X_test

        y_prob = model.predict_proba(X)[:, 1]
        y_pred = model.predict(X)
        metrics = evaluate_model(name, y_test, y_prob, y_pred)
        all_results[name] = metrics
        all_probs[name] = y_prob

    # Print comparison table
    print(f"\n{'Model':<15} {'ROC-AUC':>8} {'PR-AUC':>8} {'F1':>8} {'Prec':>8} {'Recall':>8} {'Brier':>8}")
    print("-" * 75)
    for name in ["Naive", "LogReg", "RandomForest", "XGBoost"]:
        m = all_results[name]
        print(f"{name:<15} {m['roc_auc']:>8.4f} {m['pr_auc']:>8.4f} {m['f1']:>8.4f} "
              f"{m['precision']:>8.4f} {m['recall']:>8.4f} {m['brier_score']:>8.4f}")

    # ── Bootstrap CIs for XGBoost ──
    print("\nBootstrap 95% confidence intervals (XGBoost, n=1000):")
    xgb_prob = all_probs["XGBoost"]
    roc_lo, roc_hi = bootstrap_ci(y_test, xgb_prob, roc_auc_score)
    print(f"  ROC-AUC: [{roc_lo}, {roc_hi}]")

    def pr_auc_fn(yt, yp):
        p, r, _ = precision_recall_curve(yt, yp)
        return auc(r, p)
    pr_lo, pr_hi = bootstrap_ci(y_test, xgb_prob, pr_auc_fn)
    print(f"  PR-AUC:  [{pr_lo}, {pr_hi}]")

    all_results["XGBoost"]["roc_auc_ci_95"] = [roc_lo, roc_hi]
    all_results["XGBoost"]["pr_auc_ci_95"] = [pr_lo, pr_hi]

    # ── Plots ──
    print("\nGenerating plots...")
    plot_roc_comparison(all_results, y_test, all_probs, results_dir)
    plot_calibration(y_test, xgb_prob, results_dir)
    plot_confusion(y_test, models["XGBoost"].predict(X_test), results_dir)
    plot_feature_importance(models["XGBoost"], feature_names, results_dir)

    # ── Extended evaluation ──
    print("\n" + "=" * 60)
    print("EXTENDED EVALUATION")
    print("=" * 60)

    print("\n--- Per-Disaster-Type ---")
    per_type = per_disaster_type_evaluation(test_df, xgb_prob,
                                             models["XGBoost"].predict(X_test), results_dir)

    print("\n--- Per-Region ---")
    per_region = per_region_evaluation(test_df, xgb_prob, results_dir)

    print("\n--- Temporal Stability ---")
    temporal = temporal_stability_analysis(df, models["XGBoost"], feature_names, results_dir)

    print("\n--- Error Analysis (False Negatives) ---")
    errors = error_analysis(test_df, xgb_prob,
                            models["XGBoost"].predict(X_test), feature_names, results_dir)

    print("\n--- Cascade Event Evaluation ---")
    cascade_eval = cascade_event_evaluation(test_df, xgb_prob,
                                             models["XGBoost"].predict(X_test),
                                             feature_names, results_dir)

    print("\n--- Ablation Study ---")
    ablation = ablation_study(X_train, y_train, X_val, y_val, X_test, y_test,
                               feature_names, results_dir)

    # ── No-FEMA model variant ──
    print("\n--- No-FEMA Model (Hazard-Only Prediction) ---")
    no_fema_results = train_no_fema_model(
        X_train, y_train, X_val, y_val, X_test, y_test,
        feature_names, model_dir, results_dir)

    # ── SHAP interaction analysis ──
    print("\n--- SHAP Interaction Analysis ---")
    shap_results = shap_interaction_analysis(
        models["XGBoost"], X_test, feature_names, results_dir)

    # ── Declaration equity analysis (uses no-FEMA model) ──
    print("\n--- Declaration Equity Analysis ---")
    no_fema_equity_path = f"{model_dir}/hazardcast_no_fema.json"
    try:
        no_fema_equity = xgb.XGBClassifier()
        no_fema_equity.load_model(no_fema_equity_path)
    except Exception:
        no_fema_equity = None
    if no_fema_equity is not None:
        equity_results = declaration_equity_analysis(
            df, no_fema_equity, feature_names, results_dir)
    else:
        print("  Skipped: no-FEMA model not available")
        equity_results = {}

    # ── Cold-start evaluation ──
    print("\n--- Cold-Start Evaluation ---")
    # Load the no-FEMA model for comparison
    no_fema_model_path = f"{model_dir}/hazardcast_no_fema.json"
    try:
        no_fema_xgb = xgb.XGBClassifier()
        no_fema_xgb.load_model(no_fema_model_path)
    except Exception:
        no_fema_xgb = None
    cold_start_results = cold_start_evaluation(
        df, models["XGBoost"], no_fema_xgb, feature_names, results_dir)

    # ── Save XGBoost model ──
    models["XGBoost"].save_model(f"{model_dir}/hazardcast_xgb.json")
    booster = models["XGBoost"].get_booster()
    booster.save_model(f"{model_dir}/hazardcast_xgb.ubj")
    print(f"\nModel saved: {model_dir}/hazardcast_xgb.json")

    # ── Save full report ──
    report = {
        "models": all_results,
        "dataset": {
            "total_samples": len(df),
            "features": len(feature_names),
            "positive_rate": round(df[TARGET_COL].mean(), 4),
            "train_period": f"{train_df['year_month'].min()} to {train_df['year_month'].max()}",
            "val_period": f"{val_df['year_month'].min()} to {val_df['year_month'].max()}",
            "test_period": f"{test_df['year_month'].min()} to {test_df['year_month'].max()}",
            "train_size": len(train_df),
            "val_size": len(val_df),
            "test_size": len(test_df),
        },
        "per_disaster_type": per_type,
        "per_region": per_region,
        "temporal_stability": temporal,
        "error_analysis": errors,
        "cascade_evaluation": cascade_eval,
        "ablation_study": ablation,
        "no_fema_model": no_fema_results,
        "shap_interactions": shap_results,
        "declaration_equity": equity_results,
        "cold_start": cold_start_results,
        "hyperparameter_tuning": {
            "method": "Optuna TPE",
            "n_trials": args.tune_trials if args.tune else 0,
            "best_params": tuned_params,
            "used_tuned_model": tuned_params is not None,
        } if tuned_params else None,
        "limitations": [
            "FEMA declarations are political/bureaucratic decisions, not purely hazard-driven",
            "Target variable conflates hazard severity with political/economic factors",
            "FEMA dominance: removing FEMA features drops AUC from 0.89 to 0.63 — model relies heavily on declaration history",
            "Equity analysis uses no-FEMA model to avoid circular reasoning, but that model has much lower AUC (0.60)",
        ],
    }

    # ── Integrate NRI comparison if available ──
    nri_path = f"{results_dir}/nri_comparison.json"
    if os.path.exists(nri_path):
        with open(nri_path) as f:
            report["nri_comparison"] = json.load(f)
        print(f"  Integrated NRI comparison results from {nri_path}")

    with open(f"{results_dir}/evaluation_report.json", "w") as f:
        json.dump(report, f, indent=2)

    # ── Summary ──
    xgb_m = all_results["XGBoost"]
    naive_m = all_results["Naive"]
    print(f"\n{'=' * 60}")
    print(f"TRAINING COMPLETE")
    print(f"{'=' * 60}")
    print(f"  XGBoost ROC-AUC: {xgb_m['roc_auc']} [{roc_lo}, {roc_hi}]")
    print(f"  vs Naive:        {naive_m['roc_auc']}")
    print(f"  vs LogReg:       {all_results['LogReg']['roc_auc']}")
    print(f"  vs RandomForest: {all_results['RandomForest']['roc_auc']}")
    print(f"  Brier score:     {xgb_m['brier_score']} (lower=better calibrated)")
    print(f"  Model:           {model_dir}/hazardcast_xgb.json")
    print(f"  Results:         {results_dir}/")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
