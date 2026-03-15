package com.hazardcast.api;

import com.hazardcast.model.CountyFeatureVector;
import com.hazardcast.repository.CountyFeatureVectorRepository;
import com.hazardcast.repository.CountyRepository;
import com.hazardcast.scoring.RiskScorer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.YearMonth;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * REST API for disaster risk predictions.
 *
 * Scoring logic is delegated to RiskScorer (Strategy pattern).
 * Controller handles only HTTP concerns — no domain logic.
 */
@RestController
@RequestMapping("/api/predict")
@RequiredArgsConstructor
public class PredictionController {

    private final CountyFeatureVectorRepository featureRepository;
    private final CountyRepository countyRepository;
    private final RiskScorer riskScorer;

    /**
     * Predict disaster risk for a county.
     * GET /api/predict/{fips}
     */
    @GetMapping("/{fips}")
    public ResponseEntity<Map<String, Object>> predict(@PathVariable String fips) {
        var county = countyRepository.findById(fips);
        if (county.isEmpty()) {
            return ResponseEntity.notFound().build();
        }

        Optional<CountyFeatureVector> features = resolveFeatures(fips);
        if (features.isEmpty()) {
            return ResponseEntity.ok(Map.of(
                    "fips", fips,
                    "county", county.get().getCountyName(),
                    "state", county.get().getStateAbbr(),
                    "status", "no_features_computed",
                    "message", "No prediction data available for this county yet."
            ));
        }

        CountyFeatureVector fv = features.get();
        double score = riskScorer.score(fv);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("fips", fips);
        response.put("county", county.get().getCountyName());
        response.put("state", county.get().getStateAbbr());
        response.put("yearMonth", fv.getYearMonth());
        response.put("riskScore", Math.round(score * 100.0) / 100.0);
        response.put("modelType", riskScorer.modelName());

        if (isStale(fv)) {
            long monthsOld = java.time.temporal.ChronoUnit.MONTHS.between(
                    YearMonth.parse(fv.getYearMonth()), YearMonth.now());
            response.put("dataAge", monthsOld + " months old");
            response.put("warning", "Prediction is based on data from " + fv.getYearMonth()
                    + " (" + monthsOld + " months ago). Accuracy may be reduced.");
        }

        response.put("topFactors", Map.of(
                "declarations_5yr", fv.getDeclarations5yr(),
                "storm_events_5yr", fv.getStormEventCount5yr(),
                "earthquake_count_5yr", fv.getEarthquakeCount5yr(),
                "max_magnitude_5yr", fv.getMaxMagnitude5yr(),
                "tornado_count_5yr", fv.getTornadoCount5yr(),
                "flood_count_5yr", fv.getFloodCount5yr(),
                "property_damage_5yr", fv.getStormPropertyDamage5yr()
        ));

        response.put("seasonality", Map.of(
                "hurricaneSeason", fv.getIsHurricaneSeason(),
                "tornadoSeason", fv.getIsTornadoSeason(),
                "wildfireSeason", fv.getIsWildfireSeason()
        ));

        return ResponseEntity.ok(response);
    }

    /**
     * National heatmap data for all counties.
     * GET /api/predict/national?yearMonth=2024-06
     */
    @GetMapping("/national")
    public ResponseEntity<?> national(
            @RequestParam(required = false) String yearMonth) {
        String ym = yearMonth != null ? yearMonth : YearMonth.now().toString();
        var vectors = featureRepository.findAllByYearMonth(ym);

        var results = vectors.stream().map(fv -> {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("fips", fv.getFips());
            entry.put("riskScore", Math.round(riskScorer.score(fv) * 100.0) / 100.0);
            entry.put("declarations5yr", fv.getDeclarations5yr());
            entry.put("stormEvents5yr", fv.getStormEventCount5yr());
            entry.put("declarationNext90d", fv.getDeclarationNext90d());
            return entry;
        }).toList();

        return ResponseEntity.ok(Map.of(
                "yearMonth", ym,
                "modelType", riskScorer.modelName(),
                "counties", results.size(),
                "data", results
        ));
    }

    private Optional<CountyFeatureVector> resolveFeatures(String fips) {
        String currentMonth = YearMonth.now().toString();
        var features = featureRepository.findByFipsAndYearMonth(fips, currentMonth);
        if (features.isPresent()) return features;

        // Fall back to latest available
        var all = featureRepository.findByFipsOrderByYearMonth(fips);
        return all.isEmpty() ? Optional.empty() : Optional.of(all.get(all.size() - 1));
    }

    private boolean isStale(CountyFeatureVector fv) {
        YearMonth fvMonth = YearMonth.parse(fv.getYearMonth());
        return fvMonth.isBefore(YearMonth.now().minusMonths(2));
    }
}
