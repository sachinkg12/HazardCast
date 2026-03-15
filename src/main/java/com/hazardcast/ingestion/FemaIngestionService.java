package com.hazardcast.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.hazardcast.config.ApiProperties;
import com.hazardcast.model.DisasterDeclaration;
import com.hazardcast.repository.DisasterDeclarationRepository;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * Ingests FEMA disaster declarations from OpenFEMA API v2.
 * ~65,000+ records since 1953. Paginated at 1000/page.
 */
@Component
public class FemaIngestionService extends AbstractDataProvider {

    private final ApiProperties properties;
    private final DisasterDeclarationRepository repository;

    public FemaIngestionService(WebClient webClient, ApiProperties properties,
                                DisasterDeclarationRepository repository) {
        super(webClient);
        this.properties = properties;
        this.repository = repository;
    }

    @Override
    public String getName() { return "fema_disasters"; }

    @Override
    public String getDescription() { return "FEMA disaster declarations (1953-present)"; }

    @Override
    public int priority() { return 10; } // Run first — other providers reference declarations

    @Override
    protected List<PageRequest> buildPageRequests(int startYear, int endYear) {
        // FEMA API doesn't filter well by year, so we paginate through all records
        // and let deduplication handle already-ingested ones.
        List<PageRequest> pages = new ArrayList<>();
        int pageSize = properties.getApi().getFema().getPageSize();
        String baseUrl = properties.getApi().getFema().getBaseUrl();

        // ~65k records / 1000 per page = ~65 pages. No $select/$orderby — FEMA API
        // returns 0 results when combining these with $skip on some endpoints.
        for (int skip = 0; skip < 100_000; skip += pageSize) {
            String url = String.format(
                    "%s/DisasterDeclarationsSummaries?$skip=%d&$top=%d",
                    baseUrl, skip, pageSize);
            pages.add(new PageRequest(url, "skip=" + skip));
        }
        return pages;
    }

    @Override
    protected boolean stopOnEmptyPage() { return true; } // Stop when FEMA runs out of records

    @Override
    protected JsonNode extractRecords(JsonNode response) {
        return response.get("DisasterDeclarationsSummaries");
    }

    @Override
    protected boolean isDuplicate(JsonNode record) {
        String fips = buildFips(
                textOrNull(record, "fipsStateCode"),
                textOrNull(record, "fipsCountyCode"));
        if (fips == null) return true; // skip records without FIPS
        int disasterNumber = record.get("disasterNumber").asInt();
        return repository.existsByDisasterNumberAndFips(disasterNumber, fips);
    }

    @Override
    protected Object parseRecord(JsonNode record) {
        String fips = buildFips(
                textOrNull(record, "fipsStateCode"),
                textOrNull(record, "fipsCountyCode"));
        if (fips == null) return null;

        return DisasterDeclaration.builder()
                .disasterNumber(record.get("disasterNumber").asInt())
                .declarationDate(parseDate(record, "declarationDate"))
                .disasterType(textOrNull(record, "disasterType"))
                .incidentType(textOrNull(record, "incidentType"))
                .title(textOrNull(record, "declarationTitle"))
                .state(textOrNull(record, "state"))
                .fips(fips)
                .incidentBeginDate(parseDate(record, "incidentBeginDate"))
                .incidentEndDate(parseDate(record, "incidentEndDate"))
                .declarationType(textOrNull(record, "declarationType"))
                .ihProgram(boolOrNull(record, "ihProgramDeclared"))
                .iaProgram(boolOrNull(record, "iaProgramDeclared"))
                .paProgram(boolOrNull(record, "paProgramDeclared"))
                .hmProgram(boolOrNull(record, "hmProgramDeclared"))
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void saveBatch(List<Object> batch) {
        repository.saveAll(batch.stream()
                .map(o -> (DisasterDeclaration) o)
                .toList());
    }

    private LocalDate parseDate(JsonNode record, String field) {
        String raw = textOrNull(record, field);
        if (raw == null || raw.isEmpty()) return null;
        try {
            return LocalDate.parse(raw.substring(0, 10));
        } catch (Exception e) {
            return null;
        }
    }
}
