package com.hazardcast.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.hazardcast.model.NfipClaim;
import com.hazardcast.repository.NfipClaimRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Ingests FEMA National Flood Insurance Program (NFIP) claims data
 * from OpenFEMA API v2.
 *
 * Source: https://www.fema.gov/api/open/v2/FimaNfipClaims
 * Contains all NFIP flood insurance claims. Free, no key.
 *
 * Key field mappings (from actual API response):
 *   countyCode  → 5-digit FIPS (e.g., "12011")
 *   dateOfLoss  → ISO 8601 (e.g., "1991-10-08T00:00:00.000Z")
 *   amountPaidOnBuildingClaim, amountPaidOnContentsClaim → dollar amounts
 *   ratedFloodZone → flood zone designation
 *   yearOfLoss  → integer year
 */
@Component
@Slf4j
public class NfipIngestionService implements DataProvider {

    private final WebClient webClient;
    private final NfipClaimRepository repository;

    private static final String NFIP_URL =
            "https://www.fema.gov/api/open/v2/FimaNfipClaims?$skip=%d&$top=%d";

    private static final int PAGE_SIZE = 1000;
    private static final int MAX_RECORDS = 500_000; // Cap to avoid endless pagination

    public NfipIngestionService(WebClient webClient, NfipClaimRepository repository) {
        this.webClient = webClient;
        this.repository = repository;
    }

    @Override
    public String getName() { return "fema_nfip_claims"; }

    @Override
    public String getDescription() { return "FEMA NFIP flood insurance claims"; }

    @Override
    public int priority() { return 60; }

    @Override
    public IngestionResult ingest(int startYear, int endYear) {
        log.info("Starting NFIP claims ingestion...");
        var result = new IngestionResult(getName());
        var inserted = new AtomicInteger(0);
        var skipped = new AtomicInteger(0);

        int skip = 0;
        int emptyPages = 0;

        while (emptyPages < 3 && skip < MAX_RECORDS) {
            String url = String.format(NFIP_URL, skip, PAGE_SIZE);
            log.debug("NFIP: fetching skip={}", skip);

            JsonNode response;
            try {
                response = webClient.get()
                        .uri(java.net.URI.create(url))
                        .retrieve()
                        .bodyToMono(JsonNode.class)
                        .block();
            } catch (Exception e) {
                log.warn("NFIP fetch failed at skip={}: {}", skip, e.getMessage());
                break;
            }

            if (response == null || !response.has("FimaNfipClaims")) break;
            JsonNode records = response.get("FimaNfipClaims");
            if (!records.isArray() || records.isEmpty()) {
                emptyPages++;
                skip += PAGE_SIZE;
                continue;
            }
            emptyPages = 0;

            List<NfipClaim> batch = new ArrayList<>();

            for (JsonNode record : records) {
                try {
                    // countyCode is already a 5-digit FIPS (e.g., "12011")
                    String fips = textOrNull(record, "countyCode");
                    if (fips == null || fips.length() < 4) { skipped.incrementAndGet(); continue; }
                    fips = String.format("%05d", Integer.parseInt(fips));

                    LocalDate dateOfLoss = parseDate(record, "dateOfLoss");
                    if (dateOfLoss == null) { skipped.incrementAndGet(); continue; }

                    // Filter by year range
                    if (dateOfLoss.getYear() < startYear || dateOfLoss.getYear() > endYear) {
                        skipped.incrementAndGet();
                        continue;
                    }

                    double buildingPaid = doubleOr(record, "amountPaidOnBuildingClaim", 0);
                    double contentsPaid = doubleOr(record, "amountPaidOnContentsClaim", 0);
                    double amountPaid = buildingPaid + contentsPaid;

                    batch.add(NfipClaim.builder()
                            .fips(fips)
                            .dateOfLoss(dateOfLoss)
                            .amountPaid(amountPaid)
                            .buildingDamage(buildingPaid)
                            .contentsDamage(contentsPaid)
                            .floodZone(textOrNull(record, "ratedFloodZone"))
                            .yearOfLoss(dateOfLoss.getYear())
                            .build());

                    if (batch.size() >= 500) {
                        repository.saveAll(batch);
                        inserted.addAndGet(batch.size());
                        batch.clear();
                    }
                } catch (Exception e) {
                    skipped.incrementAndGet();
                }
            }

            if (!batch.isEmpty()) {
                repository.saveAll(batch);
                inserted.addAndGet(batch.size());
            }

            if (inserted.get() % 10_000 < PAGE_SIZE) {
                log.info("NFIP: {} inserted, {} skipped so far", inserted.get(), skipped.get());
            }

            if (records.size() < PAGE_SIZE) break;
            skip += PAGE_SIZE;
        }

        result.setRecordsInserted(inserted.get());
        result.setRecordsSkipped(skipped.get());
        result.complete();
        log.info("NFIP ingestion complete: {} inserted, {} skipped", inserted.get(), skipped.get());
        return result;
    }

    private LocalDate parseDate(JsonNode record, String field) {
        String raw = textOrNull(record, field);
        if (raw == null || raw.isEmpty()) return null;
        try { return LocalDate.parse(raw.substring(0, 10)); }
        catch (Exception e) { return null; }
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
