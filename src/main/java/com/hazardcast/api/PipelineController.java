package com.hazardcast.api;

import com.hazardcast.ingestion.IngestionResult;
import com.hazardcast.pipeline.FeatureEngineer;
import com.hazardcast.pipeline.ParquetExporter;
import com.hazardcast.pipeline.PipelineOrchestrator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.YearMonth;
import java.util.Map;

@RestController
@RequestMapping("/api/pipeline")
@RequiredArgsConstructor
@Slf4j
public class PipelineController {

    private final PipelineOrchestrator orchestrator;
    private final FeatureEngineer featureEngineer;
    private final ParquetExporter parquetExporter;

    /**
     * Run the full pipeline: ingest → features → export.
     * POST /api/pipeline/run?startYear=2000&endYear=2024
     */
    @PostMapping("/run")
    public ResponseEntity<PipelineOrchestrator.PipelineReport> runFull(
            @RequestParam(defaultValue = "2000") int startYear,
            @RequestParam(defaultValue = "2024") int endYear) {
        log.info("Pipeline triggered: {}-{}", startYear, endYear);
        var report = orchestrator.runFull(startYear, endYear);
        return ResponseEntity.ok(report);
    }

    /**
     * Run only data ingestion.
     * POST /api/pipeline/ingest?startYear=2000&endYear=2024
     */
    @PostMapping("/ingest")
    public ResponseEntity<Map<String, IngestionResult>> ingest(
            @RequestParam(defaultValue = "2000") int startYear,
            @RequestParam(defaultValue = "2024") int endYear) {
        var results = orchestrator.runIngestion(startYear, endYear);
        return ResponseEntity.ok(results);
    }

    /**
     * Run ingestion for a single provider by name.
     * POST /api/pipeline/ingest/nifc_wildfires?startYear=2000&endYear=2024
     */
    @PostMapping("/ingest/{providerName}")
    public ResponseEntity<?> ingestSingle(
            @PathVariable String providerName,
            @RequestParam(defaultValue = "2000") int startYear,
            @RequestParam(defaultValue = "2024") int endYear) {
        var result = orchestrator.runSingleProvider(providerName, startYear, endYear);
        if (result == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "Unknown provider: " + providerName));
        }
        return ResponseEntity.ok(result);
    }

    /**
     * Compute features for a specific month.
     * POST /api/pipeline/features?yearMonth=2024-01
     */
    @PostMapping("/features")
    public ResponseEntity<Map<String, Object>> computeFeatures(
            @RequestParam String yearMonth) {
        YearMonth ym = YearMonth.parse(yearMonth);
        int count = featureEngineer.computeForMonth(ym);
        return ResponseEntity.ok(Map.of(
                "yearMonth", yearMonth,
                "featuresComputed", count
        ));
    }

    /**
     * Compute features for a range of months.
     * POST /api/pipeline/features/range?start=2020-01&end=2024-12
     */
    @PostMapping("/features/range")
    public ResponseEntity<Map<String, Object>> computeFeaturesRange(
            @RequestParam String start,
            @RequestParam String end) {
        YearMonth from = YearMonth.parse(start);
        YearMonth to = YearMonth.parse(end);
        long count = featureEngineer.computeRange(from, to);
        return ResponseEntity.ok(Map.of(
                "start", start,
                "end", end,
                "featuresComputed", count
        ));
    }

    /**
     * Backfill a single feature domain on existing vectors.
     * POST /api/pipeline/backfill?domain=cascade&start=2000-01&end=2024-12
     */
    @PostMapping("/backfill")
    public ResponseEntity<Map<String, Object>> backfillDomain(
            @RequestParam String domain,
            @RequestParam String start,
            @RequestParam String end) {
        YearMonth from = YearMonth.parse(start);
        YearMonth to = YearMonth.parse(end);
        long count = featureEngineer.backfillDomain(domain, from, to);
        return ResponseEntity.ok(Map.of(
                "domain", domain,
                "start", start,
                "end", end,
                "vectorsUpdated", count
        ));
    }

    /**
     * Export feature vectors to Parquet.
     * POST /api/pipeline/export
     */
    @PostMapping("/export")
    public ResponseEntity<Map<String, Object>> export() {
        try {
            var file = parquetExporter.export();
            return ResponseEntity.ok(Map.of(
                    "file", file.getAbsolutePath(),
                    "sizeMb", file.length() / (1024.0 * 1024.0)
            ));
        } catch (Exception e) {
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", e.getMessage()));
        }
    }
}
