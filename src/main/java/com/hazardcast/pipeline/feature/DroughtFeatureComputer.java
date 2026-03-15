package com.hazardcast.pipeline.feature;

import com.hazardcast.model.County;
import com.hazardcast.model.CountyFeatureVector;
import com.hazardcast.repository.DroughtIndicatorRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.LocalDate;

/**
 * Computes drought features from USDA Drought Monitor data:
 * severity averages, max severity, severe drought week counts.
 */
@Component
@RequiredArgsConstructor
public class DroughtFeatureComputer implements FeatureComputer {

    private final DroughtIndicatorRepository repository;

    @Override
    public String domain() { return "drought"; }

    @Override
    public int order() { return 35; }

    @Override
    public void compute(CountyFeatureVector fv, County county, LocalDate asOfDate) {
        String fips = county.getFips();
        LocalDate fiveYearsAgo = asOfDate.minusYears(5);

        fv.setDroughtSeverityAvg5yr(orZero(repository.avgSeverityByFipsSince(fips, fiveYearsAgo)));
        fv.setDroughtMaxSeverity5yr(orZero(repository.maxSeverityByFipsSince(fips, fiveYearsAgo)));
        fv.setSevereDroughtWeeks5yr(repository.countSevereDroughtWeeks(fips, fiveYearsAgo));

        // Max D4 (exceptional drought) percentage in rolling window
        var recent = repository.findByFipsSince(fips, fiveYearsAgo);
        double maxD4 = recent.stream()
                .mapToDouble(d -> d.getD4Pct() != null ? d.getD4Pct() : 0.0)
                .max().orElse(0.0);
        fv.setDroughtD4PctMax5yr(maxD4);
    }

    private double orZero(Double val) { return val != null ? val : 0.0; }
}
