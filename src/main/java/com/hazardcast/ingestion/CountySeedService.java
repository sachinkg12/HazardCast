package com.hazardcast.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.hazardcast.model.County;
import com.hazardcast.repository.CountyRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Seeds the counties table with all 3,143 US counties from Census Bureau.
 * Uses the Census Bureau ACS 5-Year Data API (free, no key for small queries).
 *
 * This runs as a DataProvider with highest priority — must execute before
 * any providers that need county lat/lon for FIPS mapping.
 */
@Component
@Slf4j
public class CountySeedService implements DataProvider {

    private final WebClient webClient;
    private final CountyRepository repository;

    // Census Bureau ACS API — county-level population + housing (no AREALAND — not an ACS var)
    private static final String CENSUS_API =
            "https://api.census.gov/data/2022/acs/acs5" +
            "?get=NAME,B01003_001E,B25001_001E,B25077_001E" +
            "&for=county:*&in=state:*";

    // Census Gazetteer ZIP — county centroids (lat/lon) + land area
    private static final String GAZETTEER_URL =
            "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2024_Gazetteer/2024_Gaz_counties_national.zip";

    public CountySeedService(WebClient webClient, CountyRepository repository) {
        this.webClient = webClient;
        this.repository = repository;
    }

    @Override
    public String getName() { return "census_counties"; }

    @Override
    public String getDescription() { return "US Census Bureau county reference data (3,143 counties)"; }

    @Override
    public int priority() { return 1; } // Run first — everything depends on counties

    @Override
    public IngestionResult ingest(int startYear, int endYear) {
        // Skip if already seeded
        long existing = repository.count();
        if (existing > 3000) {
            log.info("Counties already seeded ({} records), skipping", existing);
            var result = new IngestionResult(getName());
            result.setRecordsSkipped((int) existing);
            result.complete();
            return result;
        }

        log.info("Seeding counties table...");
        var result = new IngestionResult(getName());
        var inserted = new AtomicInteger(0);

        try {
            // Step 1: Get county population/housing from ACS API
            seedFromCensusApi(inserted);

            // Step 2: Overlay lat/lon from Gazetteer
            overlayCoordinates();

        } catch (Exception e) {
            log.error("County seeding failed: {}", e.getMessage());
            result.setError(e.getMessage());
        }

        result.setRecordsInserted(inserted.get());
        result.complete();
        log.info("County seeding complete: {} counties", inserted.get());
        return result;
    }

    private void seedFromCensusApi(AtomicInteger inserted) {
        log.info("Fetching county data from Census ACS API...");

        JsonNode response;
        try {
            response = webClient.get()
                    .uri(java.net.URI.create(CENSUS_API))
                    .retrieve()
                    .bodyToMono(JsonNode.class)
                    .block();
        } catch (Exception e) {
            log.error("Census API failed: {}", e.getMessage());
            return;
        }

        if (response == null || !response.isArray() || response.size() < 2) {
            log.error("Census API returned unexpected response");
            return;
        }

        List<County> batch = new ArrayList<>();

        // First row is headers, skip it
        for (int i = 1; i < response.size(); i++) {
            JsonNode row = response.get(i);
            try {
                // Columns: NAME, B01003_001E, B25001_001E, B25077_001E, state, county
                String name = row.get(0).asText(); // "County Name, State"
                long population = parseLong(row.get(1).asText());
                long housingUnits = parseLong(row.get(2).asText());
                long medianHomeValue = parseLong(row.get(3).asText());

                String stateFips = row.get(4).asText();
                String countyFips = row.get(5).asText();
                String fips = stateFips + countyFips;

                String[] nameParts = name.split(",\\s*");
                String countyName = nameParts.length > 0 ? nameParts[0] : name;
                String stateName = nameParts.length > 1 ? nameParts[1] : "";

                County county = County.builder()
                        .fips(fips)
                        .stateFips(stateFips)
                        .countyName(countyName)
                        .stateName(stateName)
                        .stateAbbr(stateAbbr(stateFips))
                        .population(population)
                        .housingUnits(housingUnits)
                        .medianHomeValue(medianHomeValue > 0 ? medianHomeValue : null)
                        .build();

                batch.add(county);

                if (batch.size() >= 500) {
                    repository.saveAll(batch);
                    inserted.addAndGet(batch.size());
                    batch.clear();
                }
            } catch (Exception e) {
                log.warn("Skipping county row {}: {}", i, e.getMessage());
            }
        }

        if (!batch.isEmpty()) {
            repository.saveAll(batch);
            inserted.addAndGet(batch.size());
        }

        log.info("Seeded {} counties from Census ACS", inserted.get());
    }

    private void overlayCoordinates() {
        log.info("Fetching county coordinates from Gazetteer ZIP...");

        byte[] zipBytes;
        try {
            zipBytes = webClient.get()
                    .uri(java.net.URI.create(GAZETTEER_URL))
                    .retrieve()
                    .bodyToMono(byte[].class)
                    .block();
        } catch (Exception e) {
            log.warn("Gazetteer fetch failed: {}. Lat/lon will be missing.", e.getMessage());
            return;
        }

        if (zipBytes == null) return;

        // Extract TSV from ZIP
        String tsv = extractFirstFileFromZip(zipBytes);
        if (tsv == null) {
            log.warn("Could not extract Gazetteer data from ZIP");
            return;
        }

        String[] lines = tsv.split("\n");
        int updated = 0;

        // Gazetteer columns: USPS, GEOID, ANSICODE, NAME, ALAND, AWATER, ALAND_SQMI, AWATER_SQMI, INTPTLAT, INTPTLONG
        for (int i = 1; i < lines.length; i++) { // skip header
            try {
                String[] cols = lines[i].split("\t");
                if (cols.length < 10) continue;

                String geoid = cols[1].trim(); // GEOID = FIPS
                double landAreaSqMi = parseDouble(cols[6].trim()); // ALAND_SQMI
                double lat = Double.parseDouble(cols[8].trim()); // INTPTLAT
                double lon = Double.parseDouble(cols[9].trim()); // INTPTLONG

                repository.findById(geoid).ifPresent(county -> {
                    county.setLatitude(lat);
                    county.setLongitude(lon);
                    if (landAreaSqMi > 0) county.setLandAreaSqMi(landAreaSqMi);
                    repository.save(county);
                });
                updated++;
            } catch (Exception e) {
                // skip malformed lines
            }
        }
        log.info("Updated {} counties with lat/lon + land area from Gazetteer", updated);
    }

    private String extractFirstFileFromZip(byte[] zipBytes) {
        try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipBytes))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (!entry.isDirectory()) {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    byte[] buffer = new byte[8192];
                    int len;
                    while ((len = zis.read(buffer)) > 0) {
                        baos.write(buffer, 0, len);
                    }
                    return baos.toString("UTF-8");
                }
            }
        } catch (Exception e) {
            log.error("ZIP extraction failed: {}", e.getMessage());
        }
        return null;
    }

    private double parseDouble(String val) {
        if (val == null || val.isEmpty()) return 0.0;
        try { return Double.parseDouble(val); }
        catch (NumberFormatException e) { return 0.0; }
    }

    private long parseLong(String val) {
        if (val == null || val.isEmpty() || val.equals("null")) return 0;
        try { return Long.parseLong(val); }
        catch (NumberFormatException e) { return 0; }
    }

    /** Map state FIPS to abbreviation. Could use a full lookup but top states suffice for display. */
    private String stateAbbr(String stateFips) {
        return switch (stateFips) {
            case "01" -> "AL"; case "02" -> "AK"; case "04" -> "AZ"; case "05" -> "AR";
            case "06" -> "CA"; case "08" -> "CO"; case "09" -> "CT"; case "10" -> "DE";
            case "11" -> "DC"; case "12" -> "FL"; case "13" -> "GA"; case "15" -> "HI";
            case "16" -> "ID"; case "17" -> "IL"; case "18" -> "IN"; case "19" -> "IA";
            case "20" -> "KS"; case "21" -> "KY"; case "22" -> "LA"; case "23" -> "ME";
            case "24" -> "MD"; case "25" -> "MA"; case "26" -> "MI"; case "27" -> "MN";
            case "28" -> "MS"; case "29" -> "MO"; case "30" -> "MT"; case "31" -> "NE";
            case "32" -> "NV"; case "33" -> "NH"; case "34" -> "NJ"; case "35" -> "NM";
            case "36" -> "NY"; case "37" -> "NC"; case "38" -> "ND"; case "39" -> "OH";
            case "40" -> "OK"; case "41" -> "OR"; case "42" -> "PA"; case "44" -> "RI";
            case "45" -> "SC"; case "46" -> "SD"; case "47" -> "TN"; case "48" -> "TX";
            case "49" -> "UT"; case "50" -> "VT"; case "51" -> "VA"; case "53" -> "WA";
            case "54" -> "WV"; case "55" -> "WI"; case "56" -> "WY"; case "60" -> "AS";
            case "66" -> "GU"; case "69" -> "MP"; case "72" -> "PR"; case "78" -> "VI";
            default -> "??";
        };
    }
}
