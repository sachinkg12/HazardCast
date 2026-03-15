package com.hazardcast.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazardcast.model.DroughtIndicator;
import com.hazardcast.repository.CountyRepository;
import com.hazardcast.repository.DroughtIndicatorRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.Year;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Ingests USDA Drought Monitor data via their statistics API.
 *
 * The USDM API requires county FIPS in the `aoi` parameter and supports
 * comma-separated batches. We query in batches of 50 counties across
 * quarterly windows.
 *
 * Source: https://droughtmonitor.unl.edu/
 * Free, no key required.
 */
@Component
@Slf4j
public class DroughtIngestionService implements DataProvider {

    private final WebClient webClient;
    private final DroughtIndicatorRepository repository;
    private final CountyRepository countyRepository;
    private final ObjectMapper objectMapper;

    private static final int BATCH_SIZE = 50;

    public DroughtIngestionService(WebClient webClient, DroughtIndicatorRepository repository,
                                   CountyRepository countyRepository, ObjectMapper objectMapper) {
        this.webClient = webClient;
        this.repository = repository;
        this.countyRepository = countyRepository;
        this.objectMapper = objectMapper;
    }

    @Override
    public String getName() { return "usda_drought"; }

    @Override
    public String getDescription() { return "USDA Drought Monitor county statistics (2000-present)"; }

    @Override
    public int priority() { return 40; }

    @Override
    public IngestionResult ingest(int startYear, int endYear) {
        log.info("Starting Drought Monitor ingestion: {}-{}...", startYear, endYear);
        var result = new IngestionResult(getName());
        var inserted = new AtomicInteger(0);
        var skipped = new AtomicInteger(0);

        // Get all county FIPS from seeded counties
        List<String> allFips = countyRepository.findAllFips();
        if (allFips.isEmpty()) {
            log.warn("No counties seeded — skipping drought ingestion");
            result.setError("No counties seeded");
            result.complete();
            return result;
        }

        // Partition counties into batches of 50
        List<List<String>> batches = new ArrayList<>();
        for (int i = 0; i < allFips.size(); i += BATCH_SIZE) {
            batches.add(allFips.subList(i, Math.min(i + BATCH_SIZE, allFips.size())));
        }

        log.info("Drought: {} counties in {} batches, {}-{}", allFips.size(), batches.size(), startYear, endYear);

        for (int year = Math.max(startYear, 2000); year <= Math.min(endYear, Year.now().getValue()); year++) {
            // Query in quarterly chunks
            for (int quarter = 0; quarter < 4; quarter++) {
                int startMonth = quarter * 3 + 1;
                int endMonth = startMonth + 2;
                // USDM API expects MM/DD/YYYY format
                String start = String.format("%d/01/%d", startMonth, year);
                String end = String.format("%d/28/%d", endMonth, year);

                int batchNum = 0;
                for (List<String> batch : batches) {
                    batchNum++;
                    try {
                        String fipsParam = String.join(",", batch);
                        log.debug("Drought: Q{} batch {}/{} ({} FIPS)", quarter + 1, batchNum, batches.size(), batch.size());
                        fetchBatch(fipsParam, start, end, inserted, skipped);
                    } catch (Exception e) {
                        log.debug("Drought: batch {} failed: {}", batchNum, e.getMessage());
                    }
                }
                log.info("Drought: Q{} {} complete, total inserted = {}", quarter + 1, year, inserted.get());
            }
            log.info("Drought year {}: total inserted = {}", year, inserted.get());
        }

        result.setRecordsInserted(inserted.get());
        result.setRecordsSkipped(skipped.get());
        result.complete();
        log.info("Drought ingestion complete: {} inserted, {} skipped", inserted.get(), skipped.get());
        return result;
    }

    private void fetchBatch(String fipsParam, String start, String end,
                            AtomicInteger inserted, AtomicInteger skipped) {
        // Use HttpURLConnection directly — WebClient's URI handling
        // encodes commas in the FIPS list, breaking the USDM API
        String body;
        try {
            String rawUrl = "https://usdmdataservices.unl.edu/api/CountyStatistics/" +
                    "GetDroughtSeverityStatisticsByAreaPercent" +
                    "?aoi=" + fipsParam +
                    "&startdate=" + URLEncoder.encode(start, StandardCharsets.UTF_8) +
                    "&enddate=" + URLEncoder.encode(end, StandardCharsets.UTF_8) +
                    "&statisticsType=1";
            var conn = (java.net.HttpURLConnection) new java.net.URL(rawUrl).openConnection();
            conn.setRequestProperty("Accept", "application/json");
            conn.setRequestProperty("User-Agent", "HazardCast/0.1");
            conn.setConnectTimeout(15_000);
            conn.setReadTimeout(30_000);
            if (conn.getResponseCode() != 200) return;
            body = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            conn.disconnect();
            log.debug("Drought: HTTP {} body={} chars", conn.getResponseCode(), body.length());
        } catch (Exception e) {
            log.warn("Drought batch fetch failed: {}", e.getMessage());
            return;
        }

        if (body == null || body.isBlank() || body.equals("[]")) return;

        JsonNode response;
        try {
            response = objectMapper.readTree(body);
        } catch (Exception e) {
            return;
        }

        if (!response.isArray()) return;

        List<DroughtIndicator> records = new ArrayList<>();

        for (JsonNode record : response) {
            try {
                String fips = textOrNull(record, "fips");
                if (fips == null || fips.length() < 5) { skipped.incrementAndGet(); continue; }
                fips = String.format("%05d", Integer.parseInt(fips));

                String dateStr = textOrNull(record, "mapDate");
                if (dateStr == null) { skipped.incrementAndGet(); continue; }
                LocalDate reportDate = LocalDate.parse(dateStr.substring(0, 10));

                if (repository.existsByFipsAndReportDate(fips, reportDate)) {
                    skipped.incrementAndGet();
                    continue;
                }

                double d0 = doubleOr(record, "d0", 0);
                double d1 = doubleOr(record, "d1", 0);
                double d2 = doubleOr(record, "d2", 0);
                double d3 = doubleOr(record, "d3", 0);
                double d4 = doubleOr(record, "d4", 0);

                records.add(DroughtIndicator.builder()
                        .fips(fips)
                        .reportDate(reportDate)
                        .d0Pct(d0).d1Pct(d1).d2Pct(d2).d3Pct(d3).d4Pct(d4)
                        .severityScore(d1 + 2 * d2 + 3 * d3 + 4 * d4)
                        .build());

                if (records.size() >= 500) {
                    repository.saveAll(records);
                    inserted.addAndGet(records.size());
                    records.clear();
                }
            } catch (Exception e) {
                skipped.incrementAndGet();
            }
        }

        if (!records.isEmpty()) {
            repository.saveAll(records);
            inserted.addAndGet(records.size());
        }
    }

    private String textOrNull(JsonNode node, String field) {
        JsonNode val = node.get(field);
        return (val != null && !val.isNull()) ? val.asText() : null;
    }

    private double doubleOr(JsonNode node, String field, double def) {
        JsonNode val = node.get(field);
        return (val != null && !val.isNull()) ? val.asDouble(def) : def;
    }
}
