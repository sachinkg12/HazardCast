package com.hazardcast.pipeline;

import com.hazardcast.model.County;
import com.hazardcast.model.CountyFeatureVector;
import com.hazardcast.pipeline.feature.FeatureComputer;
import com.hazardcast.repository.CountyFeatureVectorRepository;
import com.hazardcast.repository.CountyRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.*;
import java.util.*;

/**
 * Orchestrates feature computation using composable FeatureComputer strategies.
 *
 * All feature domain logic is delegated to individual FeatureComputer implementations,
 * which are auto-discovered via Spring component scanning. Adding a new feature domain
 * requires only implementing FeatureComputer — no changes here.
 */
@Service
@Slf4j
public class FeatureEngineer {

    private final List<FeatureComputer> featureComputers;
    private final CountyRepository countyRepository;
    private final CountyFeatureVectorRepository featureRepository;

    public FeatureEngineer(List<FeatureComputer> featureComputers,
                           CountyRepository countyRepository,
                           CountyFeatureVectorRepository featureRepository) {
        this.featureComputers = featureComputers.stream()
                .sorted(Comparator.comparingInt(FeatureComputer::order))
                .toList();
        this.countyRepository = countyRepository;
        this.featureRepository = featureRepository;

        log.info("Feature engineer initialized with {} computers: {}",
                featureComputers.size(),
                this.featureComputers.stream().map(FeatureComputer::domain).toList());
    }

    /**
     * Compute feature vectors for all counties for a specific year-month.
     * Saves in batches of 500 for performance on H2.
     */
    public int computeForMonth(YearMonth yearMonth) {
        String ym = yearMonth.toString();
        log.info("Computing features for {} ({} domains)", ym, featureComputers.size());

        List<County> counties = countyRepository.findAll();
        int computed = 0;
        List<CountyFeatureVector> batch = new java.util.ArrayList<>();

        for (County county : counties) {
            try {
                CountyFeatureVector fv = computeCountyMonth(county, yearMonth);
                batch.add(fv);

                if (batch.size() >= 500) {
                    featureRepository.saveAll(batch);
                    computed += batch.size();
                    batch.clear();
                    log.debug("Features {}: {}/{} counties", ym, computed, counties.size());
                }
            } catch (Exception e) {
                log.warn("Failed to compute features for {} {}: {}",
                        county.getFips(), ym, e.getMessage());
            }
        }

        if (!batch.isEmpty()) {
            featureRepository.saveAll(batch);
            computed += batch.size();
        }

        log.info("Computed {} feature vectors for {}", computed, ym);

        // Data drift monitoring: log key feature distribution stats
        logFeatureStats(ym);

        return computed;
    }

    private void logFeatureStats(String yearMonth) {
        try {
            var vectors = featureRepository.findAllByYearMonth(yearMonth);
            if (vectors.isEmpty()) return;

            double avgDecl = vectors.stream()
                    .mapToInt(v -> v.getDeclarations5yr() != null ? v.getDeclarations5yr() : 0).average().orElse(0);
            double avgStorm = vectors.stream()
                    .mapToInt(v -> v.getStormEventCount5yr() != null ? v.getStormEventCount5yr() : 0).average().orElse(0);
            double positiveRate = vectors.stream()
                    .filter(v -> Boolean.TRUE.equals(v.getDeclarationNext90d())).count() / (double) vectors.size();
            long failedCount = vectors.stream()
                    .filter(v -> v.getFailedDomains() != null).count();

            log.info("Feature stats {}: avg_decl_5yr={}, avg_storm_5yr={}, pos_rate={}, failed_vectors={}",
                    yearMonth, String.format("%.2f", avgDecl), String.format("%.2f", avgStorm),
                    String.format("%.4f", positiveRate), failedCount);
        } catch (Exception e) {
            // Non-critical — don't fail feature computation for stats
        }
    }

    /**
     * Compute all months in a range for all counties.
     */
    public long computeRange(YearMonth start, YearMonth end) {
        long total = 0;
        YearMonth current = start;
        while (!current.isAfter(end)) {
            total += computeForMonth(current);
            current = current.plusMonths(1);
        }
        return total;
    }

    /**
     * Backfill a single feature domain on existing vectors (avoids recomputing all domains).
     * Used when adding new feature domains to existing dataset.
     */
    public long backfillDomain(String domainName, YearMonth start, YearMonth end) {
        FeatureComputer target = featureComputers.stream()
                .filter(c -> c.domain().equals(domainName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown domain: " + domainName));

        List<County> counties = countyRepository.findAll();
        Map<String, County> countyMap = new java.util.HashMap<>();
        for (County c : counties) countyMap.put(c.getFips(), c);

        long total = 0;
        YearMonth current = start;
        while (!current.isAfter(end)) {
            String ym = current.toString();
            log.info("Backfilling domain '{}' for {} ({} counties)", domainName, ym, counties.size());

            var vectors = featureRepository.findAllByYearMonth(ym);
            List<CountyFeatureVector> batch = new ArrayList<>();

            for (CountyFeatureVector fv : vectors) {
                County county = countyMap.get(fv.getFips());
                if (county == null) continue;

                try {
                    target.compute(fv, county, current.atDay(1));
                    batch.add(fv);

                    if (batch.size() >= 500) {
                        featureRepository.saveAll(batch);
                        total += batch.size();
                        batch.clear();
                    }
                } catch (Exception e) {
                    log.warn("Backfill '{}' failed for {} {}: {}",
                            domainName, fv.getFips(), ym, e.getMessage());
                }
            }

            if (!batch.isEmpty()) {
                featureRepository.saveAll(batch);
                total += batch.size();
            }
            log.info("Backfilled {} vectors for {}", vectors.size(), ym);
            current = current.plusMonths(1);
        }
        return total;
    }

    private CountyFeatureVector computeCountyMonth(County county, YearMonth yearMonth) {
        String fips = county.getFips();
        LocalDate asOfDate = yearMonth.atDay(1);

        // Retrieve existing or create new (idempotent)
        Optional<CountyFeatureVector> existing =
                featureRepository.findByFipsAndYearMonth(fips, yearMonth.toString());
        CountyFeatureVector fv = existing.orElse(new CountyFeatureVector());

        fv.setFips(fips);
        fv.setYearMonth(yearMonth.toString());

        // Delegate to each feature computer in order, track failures
        List<String> failures = new ArrayList<>();
        for (FeatureComputer computer : featureComputers) {
            try {
                computer.compute(fv, county, asOfDate);
            } catch (Exception e) {
                failures.add(computer.domain());
                log.warn("Feature computer '{}' failed for {} {}: {}",
                        computer.domain(), fips, yearMonth, e.getMessage());
            }
        }

        fv.setComputedAt(Instant.now());
        fv.setFailedDomains(failures.isEmpty() ? null : String.join(",", failures));
        return fv;
    }
}
