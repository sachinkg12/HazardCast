package com.hazardcast.pipeline.feature;

import com.hazardcast.model.County;
import com.hazardcast.model.CountyFeatureVector;
import com.hazardcast.repository.SeismicEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

/**
 * Computes seismic features: earthquake counts, magnitude stats,
 * cumulative energy (Gutenberg-Richter), depth, distance.
 */
@Component
@RequiredArgsConstructor
public class SeismicFeatureComputer implements FeatureComputer {

    private final SeismicEventRepository repository;

    @Override
    public String domain() { return "seismic"; }

    @Override
    public int order() { return 20; }

    @Override
    public void compute(CountyFeatureVector fv, County county, LocalDate asOfDate) {
        String fips = county.getFips();
        Instant oneYearAgo = asOfDate.minusYears(1).atStartOfDay(ZoneOffset.UTC).toInstant();
        Instant fiveYearsAgo = asOfDate.minusYears(5).atStartOfDay(ZoneOffset.UTC).toInstant();

        fv.setEarthquakeCount1yr(repository.countByFipsSince(fips, oneYearAgo));
        fv.setEarthquakeCount5yr(repository.countByFipsSince(fips, fiveYearsAgo));
        fv.setMaxMagnitude1yr(nullToZero(repository.maxMagnitudeByFipsSince(fips, oneYearAgo)));
        fv.setMaxMagnitude5yr(nullToZero(repository.maxMagnitudeByFipsSince(fips, fiveYearsAgo)));
        fv.setAvgMagnitude5yr(nullToZero(repository.avgMagnitudeByFipsSince(fips, fiveYearsAgo)));

        // Cumulative seismic energy using Gutenberg-Richter relation: E = 10^(1.5M + 4.8)
        var recentQuakes = repository.findByFipsSince(fips, fiveYearsAgo);
        double totalEnergy = recentQuakes.stream()
                .mapToDouble(e -> Math.pow(10, 1.5 * e.getMagnitude() + 4.8))
                .sum();
        fv.setTotalEnergy5yr(totalEnergy > 0 ? Math.log10(totalEnergy) : 0.0);

        fv.setEarthquakeDepthAvg(recentQuakes.stream()
                .filter(e -> e.getDepthKm() != null)
                .mapToDouble(e -> e.getDepthKm())
                .average().orElse(0.0));

        fv.setEarthquakeDistanceAvgKm(recentQuakes.stream()
                .filter(e -> e.getDistanceKm() != null)
                .mapToDouble(e -> e.getDistanceKm())
                .average().orElse(0.0));
    }

    private double nullToZero(Double val) {
        return val != null ? val : 0.0;
    }
}
