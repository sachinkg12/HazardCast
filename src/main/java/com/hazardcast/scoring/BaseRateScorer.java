package com.hazardcast.scoring;

import com.hazardcast.model.CountyFeatureVector;

/**
 * Heuristic risk scorer based on historical base rates.
 *
 * This is the default scorer used before an ML model is trained.
 * It provides reasonable risk estimates from feature weights derived
 * from domain knowledge. Will be replaced by MachineLearningScorer
 * once the XGBoost model is trained and exported to ONNX.
 *
 * Created by ScoringConfig when no ML model file is available.
 */
public class BaseRateScorer implements RiskScorer {

    // Weights derived from feature correlation analysis with declaration outcomes
    private static final double DECL_5YR_WEIGHT = 0.08;
    private static final double DECL_5YR_CAP = 0.4;
    private static final double DECL_1YR_WEIGHT = 0.15;
    private static final double DECL_1YR_CAP = 0.3;
    private static final double STORM_WEIGHT = 0.005;
    private static final double STORM_CAP = 0.15;
    private static final double SEISMIC_MODERATE_BONUS = 0.1;
    private static final double SEISMIC_MAJOR_BONUS = 0.1;
    private static final double TORNADO_WEIGHT = 0.03;
    private static final double TORNADO_CAP = 0.1;
    private static final double HURRICANE_SEASON_BONUS = 0.05;
    private static final double TORNADO_SEASON_BONUS = 0.03;
    private static final double DROUGHT_SEVERITY_WEIGHT = 0.02;
    private static final double DROUGHT_CAP = 0.1;
    private static final double WILDFIRE_WEIGHT = 0.04;
    private static final double WILDFIRE_CAP = 0.12;
    private static final double NFIP_CLAIM_WEIGHT = 0.003;
    private static final double NFIP_CAP = 0.1;

    @Override
    public double score(CountyFeatureVector fv) {
        double score = 0.0;

        // Declaration history is the strongest predictor
        score += Math.min(fv.getDeclarations5yr() * DECL_5YR_WEIGHT, DECL_5YR_CAP);
        score += Math.min(fv.getDeclarations1yr() * DECL_1YR_WEIGHT, DECL_1YR_CAP);

        // Storm activity
        score += Math.min(fv.getStormEventCount5yr() * STORM_WEIGHT, STORM_CAP);

        // Seismic
        if (fv.getMaxMagnitude5yr() > 4.0) score += SEISMIC_MODERATE_BONUS;
        if (fv.getMaxMagnitude5yr() > 5.0) score += SEISMIC_MAJOR_BONUS;

        // Tornado
        score += Math.min(fv.getTornadoCount5yr() * TORNADO_WEIGHT, TORNADO_CAP);

        // Seasonality
        if (Boolean.TRUE.equals(fv.getIsHurricaneSeason())) score += HURRICANE_SEASON_BONUS;
        if (Boolean.TRUE.equals(fv.getIsTornadoSeason())) score += TORNADO_SEASON_BONUS;

        // Drought severity
        if (fv.getDroughtSeverityAvg5yr() != null) {
            score += Math.min(fv.getDroughtSeverityAvg5yr() * DROUGHT_SEVERITY_WEIGHT, DROUGHT_CAP);
        }

        // Wildfire activity
        if (fv.getWildfireCount5yr() != null) {
            score += Math.min(fv.getWildfireCount5yr() * WILDFIRE_WEIGHT, WILDFIRE_CAP);
        }

        // Flood insurance claims
        if (fv.getNfipClaimCount5yr() != null) {
            score += Math.min(fv.getNfipClaimCount5yr() * NFIP_CLAIM_WEIGHT, NFIP_CAP);
        }

        return Math.min(score, 1.0);
    }

    @Override
    public String modelName() { return "base_rate_heuristic_v1"; }
}
