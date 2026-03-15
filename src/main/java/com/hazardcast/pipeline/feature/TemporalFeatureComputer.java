package com.hazardcast.pipeline.feature;

import com.hazardcast.model.County;
import com.hazardcast.model.CountyFeatureVector;
import org.springframework.stereotype.Component;

import java.time.LocalDate;

/**
 * Computes temporal features: month of year and hazard season flags.
 */
@Component
public class TemporalFeatureComputer implements FeatureComputer {

    @Override
    public String domain() { return "temporal"; }

    @Override
    public int order() { return 50; }

    @Override
    public void compute(CountyFeatureVector fv, County county, LocalDate asOfDate) {
        int month = asOfDate.getMonthValue();
        fv.setMonthOfYear(month);
        fv.setIsHurricaneSeason(month >= 6 && month <= 11);
        fv.setIsTornadoSeason(month >= 3 && month <= 6);
        fv.setIsWildfireSeason(month >= 6 && month <= 10);
    }
}
