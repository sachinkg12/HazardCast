package com.hazardcast.pipeline;

import com.hazardcast.ingestion.DataProvider;
import com.hazardcast.ingestion.IngestionResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.File;
import java.time.Instant;
import java.time.YearMonth;
import java.util.*;

/**
 * Orchestrates the full data pipeline: ingest → features → export.
 *
 * Depends only on the DataProvider interface — new data sources are auto-discovered
 * via Spring component scanning. No modification needed to add providers.
 */
@Service
@Slf4j
public class PipelineOrchestrator {

    private final List<DataProvider> providers;
    private final FeatureEngineer featureEngineer;
    private final ParquetExporter parquetExporter;

    /**
     * Spring auto-injects all DataProvider implementations, sorted by priority.
     */
    public PipelineOrchestrator(List<DataProvider> providers,
                                FeatureEngineer featureEngineer,
                                ParquetExporter parquetExporter) {
        this.providers = providers.stream()
                .sorted(Comparator.comparingInt(DataProvider::priority))
                .toList();
        this.featureEngineer = featureEngineer;
        this.parquetExporter = parquetExporter;

        log.info("Pipeline initialized with {} data providers: {}",
                providers.size(),
                providers.stream().map(DataProvider::getName).toList());
    }

    /**
     * Run the full pipeline end-to-end.
     */
    public PipelineReport runFull(int startYear, int endYear) {
        log.info("=== HazardCast Pipeline: Full Run {}-{} ===", startYear, endYear);
        Instant start = Instant.now();
        PipelineReport report = new PipelineReport();

        // Step 1: Ingest all providers (in priority order)
        log.info("--- Step 1/{}: Data Ingestion ({} providers) ---",
                3, providers.size());
        report.setIngestionResults(runIngestion(startYear, endYear));

        // Step 2: Feature Engineering
        log.info("--- Step 2/3: Feature Engineering ---");
        YearMonth startMonth = YearMonth.of(startYear, 1);
        YearMonth endMonth = YearMonth.of(endYear, 12);
        report.setFeatureVectorsComputed(featureEngineer.computeRange(startMonth, endMonth));

        // Step 3: Export
        log.info("--- Step 3/3: Parquet Export ---");
        try {
            File parquetFile = parquetExporter.export();
            report.setParquetFile(parquetFile.getAbsolutePath());
            report.setParquetSizeMb(parquetFile.length() / (1024.0 * 1024.0));
        } catch (Exception e) {
            log.error("Parquet export failed: {}", e.getMessage());
            report.setExportError(e.getMessage());
        }

        report.setDurationSeconds((Instant.now().toEpochMilli() - start.toEpochMilli()) / 1000.0);
        log.info("=== Pipeline complete in {}s ===", report.durationSeconds);
        return report;
    }

    /**
     * Run only ingestion for all registered providers.
     */
    public Map<String, IngestionResult> runIngestion(int startYear, int endYear) {
        Map<String, IngestionResult> results = new LinkedHashMap<>();
        for (DataProvider provider : providers) {
            results.put(provider.getName(), runSafely(provider, startYear, endYear));
        }
        return results;
    }

    /**
     * Run ingestion for a single named provider.
     */
    public IngestionResult runSingleProvider(String name, int startYear, int endYear) {
        return providers.stream()
                .filter(p -> p.getName().equals(name))
                .findFirst()
                .map(p -> runSafely(p, startYear, endYear))
                .orElse(null);
    }

    private IngestionResult runSafely(DataProvider provider, int startYear, int endYear) {
        try {
            return provider.ingest(startYear, endYear);
        } catch (Exception e) {
            log.error("{} ingestion failed: {}", provider.getName(), e.getMessage());
            IngestionResult failed = new IngestionResult(provider.getName());
            failed.setError(e.getMessage());
            failed.complete();
            return failed;
        }
    }

    @lombok.Data
    public static class PipelineReport {
        private Map<String, IngestionResult> ingestionResults = new LinkedHashMap<>();
        private long featureVectorsComputed;
        private String parquetFile;
        private double parquetSizeMb;
        private String exportError;
        private double durationSeconds;
    }
}
