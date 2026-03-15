package com.hazardcast.scoring;

import com.hazardcast.model.CountyFeatureVector;

/**
 * Strategy interface for computing risk scores from feature vectors.
 *
 * Implementations can range from simple heuristics to trained ML models.
 * The active scorer is injected into the prediction controller via Spring.
 *
 * GoF Pattern: Strategy
 * Allows hot-swapping scoring logic (heuristic → XGBoost → LSTM)
 * without modifying the controller or API contract.
 */
public interface RiskScorer {

    /**
     * Compute a risk score between 0.0 (no risk) and 1.0 (certain declaration).
     */
    double score(CountyFeatureVector fv);

    /**
     * Name of this scoring model (returned in API responses for transparency).
     */
    String modelName();
}
