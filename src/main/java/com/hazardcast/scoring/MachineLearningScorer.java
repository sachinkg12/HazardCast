package com.hazardcast.scoring;

import com.hazardcast.model.CountyFeatureVector;
import lombok.extern.slf4j.Slf4j;
import ml.dmlc.xgboost4j.java.Booster;
import ml.dmlc.xgboost4j.java.DMatrix;
import ml.dmlc.xgboost4j.java.XGBoost;

/**
 * ML-based risk scorer using a trained XGBoost model via XGBoost4J.
 *
 * Loads the JSON model exported by train_xgboost.py and runs native
 * inference — no ONNX or Python runtime needed.
 */
@Slf4j
public class MachineLearningScorer implements RiskScorer {

    private final Booster booster;

    /** Feature count must match FEATURE_COLS in train_xgboost.py (v4: 35 base + 7 cascade = 42) */
    private static final int NUM_FEATURES = 42;

    public MachineLearningScorer(String modelPath) {
        try {
            this.booster = XGBoost.loadModel(modelPath);
            log.info("XGBoost model loaded from {}", modelPath);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load XGBoost model: " + modelPath, e);
        }
    }

    @Override
    public double score(CountyFeatureVector fv) {
        try {
            float[] features = extractFeatures(fv);
            DMatrix dmat = new DMatrix(features, 1, NUM_FEATURES, Float.NaN);
            float[][] preds = booster.predict(dmat);
            // XGBoost binary classifier returns probability of class 1
            return preds[0][0];
        } catch (Exception e) {
            log.warn("ML prediction failed for {}, falling back to 0.0: {}",
                    fv.getFips(), e.getMessage());
            return 0.0;
        }
    }

    @Override
    public String modelName() { return "xgboost_v4"; }

    /**
     * Extract features in the exact order matching FEATURE_COLS in train_xgboost.py.
     */
    private float[] extractFeatures(CountyFeatureVector fv) {
        // Null handling: use 0 for missing values, matching ParquetExporter (fixes train/serve skew)
        return new float[] {
                // FEMA (7)
                safe(fv.getDeclarations1yr()), safe(fv.getDeclarations3yr()),
                safe(fv.getDeclarations5yr()), safe(fv.getDeclarations10yr()),
                fv.getMonthsSinceLastDecl() != null ? fv.getMonthsSinceLastDecl() : -1f,
                safeD(fv.getMajorDisasterRatio()), safeD(fv.getIaProgramRatio()),
                // Storm (10)
                safe(fv.getStormEventCount1yr()), safe(fv.getStormEventCount5yr()),
                safe(fv.getStormDeaths5yr()), safe(fv.getStormInjuries5yr()),
                safeD(fv.getStormPropertyDamage5yr()), safeD(fv.getStormCropDamage5yr()),
                safe(fv.getTornadoCount5yr()), safe(fv.getFloodCount5yr()),
                safe(fv.getHailCount5yr()), safe(fv.getMaxTorFScale5yr()),
                // Socioeconomic (5)
                fv.getPopulation() != null ? fv.getPopulation() : 0f,
                fv.getHousingUnits() != null ? fv.getHousingUnits() : 0f,
                fv.getMedianHomeValue() != null ? fv.getMedianHomeValue() : 0f,
                safeD(fv.getPopulationDensity()), safeD(fv.getLandAreaSqMi()),
                // Drought (4)
                safeD(fv.getDroughtSeverityAvg5yr()), safeD(fv.getDroughtMaxSeverity5yr()),
                safe(fv.getSevereDroughtWeeks5yr()), safeD(fv.getDroughtD4PctMax5yr()),
                // Wildfire (4)
                safe(fv.getWildfireCount1yr()), safe(fv.getWildfireCount5yr()),
                safeD(fv.getWildfireAcresBurned5yr()), safeD(fv.getWildfireMaxAcres5yr()),
                // NFIP (3)
                safe(fv.getNfipClaimCount5yr()), safeD(fv.getNfipTotalPayout5yr()),
                safeD(fv.getNfipAvgPayout5yr()),
                // Spatial (2)
                safeD(fv.getNeighborAvgDeclarations5yr()), safeD(fv.getStateAvgDeclarations5yr()),
                // Cascade (7)
                safeD(fv.getCascadeDroughtFireRisk()), safeD(fv.getCascadeFireFloodRisk()),
                safeD(fv.getCascadeHurricaneFloodRisk()), safeD(fv.getCascadeEarthquakeLandslideRisk()),
                safe(fv.getCascadeStormCompoundCount()), safe(fv.getCascadeActiveChains()),
                safe(fv.getCascadeMaxChainLength()),
        };
    }

    private float safe(Integer val) { return val != null ? val : 0f; }
    private float safeD(Double val) { return val != null ? val.floatValue() : 0f; }
}
