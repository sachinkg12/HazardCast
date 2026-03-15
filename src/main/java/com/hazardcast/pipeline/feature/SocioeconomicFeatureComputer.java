package com.hazardcast.pipeline.feature;

import com.hazardcast.model.County;
import com.hazardcast.model.CountyFeatureVector;
import org.springframework.stereotype.Component;

import java.time.LocalDate;

/**
 * Computes socioeconomic features from Census county data:
 * population, housing, density, median home value.
 */
@Component
public class SocioeconomicFeatureComputer implements FeatureComputer {

    @Override
    public String domain() { return "socioeconomic"; }

    @Override
    public int order() { return 40; }

    @Override
    public void compute(CountyFeatureVector fv, County county, LocalDate asOfDate) {
        fv.setPopulation(county.getPopulation());
        fv.setHousingUnits(county.getHousingUnits());
        fv.setMedianHomeValue(county.getMedianHomeValue());
        fv.setLandAreaSqMi(county.getLandAreaSqMi());

        fv.setPopulationDensity(
                (county.getPopulation() != null && county.getLandAreaSqMi() != null
                        && county.getLandAreaSqMi() > 0)
                ? county.getPopulation() / county.getLandAreaSqMi() : null);
    }
}
