package com.hazardcast.pipeline.feature;

import com.hazardcast.model.County;
import com.hazardcast.model.CountyFeatureVector;
import com.hazardcast.repository.CountyFeatureVectorRepository;
import com.hazardcast.repository.CountyRepository;
import com.hazardcast.repository.DisasterDeclarationRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.YearMonth;
import java.util.List;

/**
 * Computes spatial features: neighboring county averages and state-level averages.
 * Uses a bounding-box approximation for neighbor lookup.
 *
 * State average is computed from raw declaration data (not feature vectors)
 * to avoid circular dependency.
 */
@Component
@RequiredArgsConstructor
public class SpatialFeatureComputer implements FeatureComputer {

    private final CountyRepository countyRepository;
    private final CountyFeatureVectorRepository featureRepository;
    private final DisasterDeclarationRepository declarationRepository;

    private static final double NEIGHBOR_RADIUS_DEG = 0.5; // ~50km

    @Override
    public String domain() { return "spatial"; }

    @Override
    public int order() { return 60; } // Run last — may depend on other counties' features

    @Override
    public void compute(CountyFeatureVector fv, County county, LocalDate asOfDate) {
        String yearMonth = YearMonth.from(asOfDate).toString();
        LocalDate fiveYearsAgo = asOfDate.minusYears(5);

        // Neighbor average (bounding box approximation)
        if (county.getLatitude() != null && county.getLongitude() != null) {
            List<County> neighbors = countyRepository.findInBoundingBox(
                    county.getLatitude() - NEIGHBOR_RADIUS_DEG,
                    county.getLatitude() + NEIGHBOR_RADIUS_DEG,
                    county.getLongitude() - NEIGHBOR_RADIUS_DEG,
                    county.getLongitude() + NEIGHBOR_RADIUS_DEG);

            List<String> neighborFips = neighbors.stream()
                    .map(County::getFips)
                    .filter(f -> !f.equals(county.getFips()))
                    .toList();

            if (!neighborFips.isEmpty()) {
                Double avg = featureRepository.avgNeighborDeclarations5yr(neighborFips, yearMonth);
                fv.setNeighborAvgDeclarations5yr(avg != null ? avg : 0.0);
            } else {
                fv.setNeighborAvgDeclarations5yr(0.0);
            }
        } else {
            fv.setNeighborAvgDeclarations5yr(0.0);
        }

        // State average — computed from raw declarations, not feature vectors
        String stateFips = county.getStateFips();
        Double stateAvg = declarationRepository.avgDeclarationsPerCountyInState(stateFips, fiveYearsAgo);
        fv.setStateAvgDeclarations5yr(stateAvg != null ? stateAvg : 0.0);
    }
}
