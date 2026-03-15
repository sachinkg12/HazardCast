package com.hazardcast.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Template Method base class for data providers.
 *
 * Encapsulates the shared ingestion flow: paginate → parse → deduplicate → save.
 * Subclasses override only the parts that vary per data source.
 *
 * GoF Pattern: Template Method
 * Eliminates duplicated fetch/parse/save logic across providers.
 */
@Slf4j
public abstract class AbstractDataProvider implements DataProvider {

    protected final WebClient webClient;

    protected AbstractDataProvider(WebClient webClient) {
        this.webClient = webClient;
    }

    /**
     * Template method — defines the invariant ingestion algorithm.
     * Subclasses customize via hook methods.
     */
    @Override
    public final IngestionResult ingest(int startYear, int endYear) {
        log.info("Starting {} ingestion: {}-{}...", getName(), startYear, endYear);
        var result = new IngestionResult(getName());
        var inserted = new AtomicInteger(0);
        var skipped = new AtomicInteger(0);

        try {
            List<PageRequest> pages = buildPageRequests(startYear, endYear);

            for (PageRequest page : pages) {
                boolean hasData = processPage(page, inserted, skipped);
                if (!hasData && stopOnEmptyPage()) {
                    log.info("{}: empty page at {}, stopping pagination", getName(), page.label());
                    break;
                }
            }
        } catch (Exception e) {
            log.error("{} ingestion failed: {}", getName(), e.getMessage());
            result.setError(e.getMessage());
        }

        result.setRecordsInserted(inserted.get());
        result.setRecordsSkipped(skipped.get());
        result.complete();
        log.info("{} ingestion complete: {} inserted, {} skipped",
                getName(), inserted.get(), skipped.get());
        return result;
    }

    /**
     * Build the list of page requests to execute.
     * Override for custom pagination (e.g., USGS chunks by 5-year ranges).
     */
    protected abstract List<PageRequest> buildPageRequests(int startYear, int endYear);

    /**
     * Process a single page: fetch → parse → dedupe → save.
     * Returns true if the page had data, false if empty (for early termination).
     */
    private boolean processPage(PageRequest page, AtomicInteger inserted, AtomicInteger skipped) {
        JsonNode response = fetchPage(page);
        if (response == null) return false;

        JsonNode records = extractRecords(response);
        if (records == null || !records.isArray() || records.isEmpty()) return false;

        List<Object> batch = new ArrayList<>();

        for (JsonNode record : records) {
            try {
                if (isDuplicate(record)) {
                    skipped.incrementAndGet();
                    continue;
                }
                Object entity = parseRecord(record);
                if (entity != null) {
                    batch.add(entity);
                } else {
                    skipped.incrementAndGet();
                }
            } catch (Exception e) {
                log.warn("Skipping malformed {} record: {}", getName(), e.getMessage());
                skipped.incrementAndGet();
            }
        }

        if (!batch.isEmpty()) {
            saveBatch(batch);
            inserted.addAndGet(batch.size());
            log.info("{}: inserted {} records (total: {})", getName(), batch.size(), inserted.get());
        }
        return true;
    }

    /**
     * Whether to stop pagination when an empty page is encountered.
     * Override to return false for APIs where gaps are expected (e.g., NOAA by year).
     */
    protected boolean stopOnEmptyPage() {
        return false;
    }

    /**
     * Fetch a single page of data from the API.
     */
    protected JsonNode fetchPage(PageRequest page) {
        try {
            // Use URI.create() to prevent double-encoding of special chars (e.g., $ in OData params)
            return webClient.get()
                    .uri(java.net.URI.create(page.url()))
                    .retrieve()
                    .bodyToMono(JsonNode.class)
                    .block();
        } catch (Exception e) {
            log.warn("{} fetch failed for {}: {}", getName(), page.label(), e.getMessage());
            return null;
        }
    }

    /**
     * Extract the records array from the API response.
     * Override if the response structure differs per provider.
     */
    protected abstract JsonNode extractRecords(JsonNode response);

    /**
     * Check if a record already exists (deduplication).
     */
    protected abstract boolean isDuplicate(JsonNode record);

    /**
     * Parse a single JSON record into a JPA entity.
     * Return null to skip the record.
     */
    protected abstract Object parseRecord(JsonNode record);

    /**
     * Persist a batch of parsed entities.
     */
    protected abstract void saveBatch(List<Object> batch);

    // ---- Shared parsing helpers ----

    protected String textOrNull(JsonNode node, String field) {
        JsonNode val = node.get(field);
        return (val != null && !val.isNull()) ? val.asText() : null;
    }

    protected int intOrZero(JsonNode node, String field) {
        JsonNode val = node.get(field);
        return (val != null && !val.isNull()) ? val.asInt(0) : 0;
    }

    protected Double doubleOrNull(JsonNode node, String field) {
        JsonNode val = node.get(field);
        return (val != null && !val.isNull() && !val.asText().isEmpty()) ? val.asDouble() : null;
    }

    protected Boolean boolOrNull(JsonNode node, String field) {
        JsonNode val = node.get(field);
        return (val != null && !val.isNull()) ? val.asBoolean() : null;
    }

    protected String buildFips(String stateFips, String countyFips) {
        if (stateFips == null || countyFips == null) return null;
        try {
            return String.format("%02d%03d",
                    Integer.parseInt(stateFips), Integer.parseInt(countyFips));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Represents a single page/chunk to fetch.
     */
    public record PageRequest(String url, String label) {}
}
