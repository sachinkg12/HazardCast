package com.hazardcast.pipeline.feature;

import com.hazardcast.model.County;
import com.hazardcast.model.CountyFeatureVector;
import com.hazardcast.repository.WildfireEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.LocalDate;

/**
 * Computes wildfire features from NIFC fire perimeter data:
 * fire counts, total/max acres burned.
 */
@Component
@RequiredArgsConstructor
public class WildfireFeatureComputer implements FeatureComputer {

    private final WildfireEventRepository repository;

    @Override
    public String domain() { return "wildfire"; }

    @Override
    public int order() { return 45; }

    @Override
    public void compute(CountyFeatureVector fv, County county, LocalDate asOfDate) {
        String fips = county.getFips();
        LocalDate oneYearAgo = asOfDate.minusYears(1);
        LocalDate fiveYearsAgo = asOfDate.minusYears(5);

        fv.setWildfireCount1yr(repository.countByFipsSince(fips, oneYearAgo));
        fv.setWildfireCount5yr(repository.countByFipsSince(fips, fiveYearsAgo));
        fv.setWildfireAcresBurned5yr(repository.totalAcresByFipsSince(fips, fiveYearsAgo));
        fv.setWildfireMaxAcres5yr(orZero(repository.maxAcresByFipsSince(fips, fiveYearsAgo)));
    }

    private double orZero(Double val) { return val != null ? val : 0.0; }
}
