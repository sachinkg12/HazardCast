package com.hazardcast.pipeline.feature;

import com.hazardcast.model.County;
import com.hazardcast.model.CountyFeatureVector;

import java.time.LocalDate;

/**
 * Strategy interface for computing a domain of features.
 *
 * Each implementation handles one feature domain (FEMA, seismic, storm, etc.)
 * and populates the relevant fields on the feature vector.
 */
public interface FeatureComputer {

    /**
     * Name of this feature domain (for logging and metrics).
     */
    String domain();

    /**
     * Compute features for a single county at a given point in time.
     *
     * @param fv       the feature vector to populate (mutable)
     * @param county   the county reference data
     * @param asOfDate the reference date for rolling windows
     */
    void compute(CountyFeatureVector fv, County county, LocalDate asOfDate);

    /**
     * Execution order (lower = first). Target computation must run first.
     */
    default int order() { return 100; }
}
