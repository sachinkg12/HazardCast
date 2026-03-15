package com.hazardcast.ingestion;

import com.hazardcast.model.StormEvent;
import com.hazardcast.repository.StormEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.*;
import java.time.LocalDate;
import java.time.Year;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

/**
 * Ingests NOAA Storm Events Database from NCEI bulk CSV files.
 *
 * Storm Events data (tornadoes, floods, hurricanes, hail, etc.) is published
 * as gzipped CSV files by year at:
 * https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/
 *
 * This provider downloads and parses these files directly, bypassing the
 * non-functional REST API.
 */
@Component
@Slf4j
public class NoaaStormIngestionService implements DataProvider {

    private final WebClient webClient;
    private final StormEventRepository repository;

    private static final String BASE_URL =
            "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/";

    public NoaaStormIngestionService(WebClient webClient, StormEventRepository repository) {
        this.webClient = webClient;
        this.repository = repository;
    }

    @Override
    public String getName() { return "noaa_storm_events"; }

    @Override
    public String getDescription() { return "NOAA Storm Events Database (bulk CSV, 2000-present)"; }

    @Override
    public int priority() { return 30; }

    @Override
    public IngestionResult ingest(int startYear, int endYear) {
        log.info("Starting NOAA Storm Events ingestion: {}-{}...", startYear, endYear);
        var result = new IngestionResult(getName());
        var inserted = new AtomicInteger(0);
        var skipped = new AtomicInteger(0);

        // First, discover available files from the directory listing
        String listing;
        try {
            listing = webClient.get()
                    .uri(java.net.URI.create(BASE_URL))
                    .retrieve()
                    .bodyToMono(String.class)
                    .block();
        } catch (Exception e) {
            log.error("Failed to fetch NOAA file listing: {}", e.getMessage());
            result.setError(e.getMessage());
            result.complete();
            return result;
        }

        for (int year = startYear; year <= Math.min(endYear, Year.now().getValue()); year++) {
            String filename = findFilename(listing, year);
            if (filename == null) {
                log.warn("No Storm Events file found for year {}", year);
                continue;
            }

            try {
                processYear(filename, year, inserted, skipped);
            } catch (Exception e) {
                log.warn("Failed to process Storm Events for year {}: {}", year, e.getMessage());
            }
        }

        result.setRecordsInserted(inserted.get());
        result.setRecordsSkipped(skipped.get());
        result.complete();
        log.info("NOAA Storm ingestion complete: {} inserted, {} skipped",
                inserted.get(), skipped.get());
        return result;
    }

    /**
     * Find the CSV filename for a given year from the directory listing HTML.
     */
    private String findFilename(String html, int year) {
        // Pattern: StormEvents_details-ftp_v1.0_dYYYY_cDATE.csv.gz
        String prefix = "StormEvents_details-ftp_v1.0_d" + year;
        int idx = html.indexOf(prefix);
        if (idx < 0) return null;
        int end = html.indexOf(".csv.gz", idx);
        if (end < 0) return null;
        return html.substring(idx, end + 7); // include ".csv.gz"
    }

    private void processYear(String filename, int year,
                             AtomicInteger inserted, AtomicInteger skipped) throws Exception {
        String url = BASE_URL + filename;
        log.info("NOAA: downloading {}", filename);

        byte[] gzipData = webClient.get()
                .uri(java.net.URI.create(url))
                .retrieve()
                .bodyToMono(byte[].class)
                .block();

        if (gzipData == null || gzipData.length == 0) {
            log.warn("Empty response for {}", filename);
            return;
        }

        log.info("NOAA: parsing {} ({} KB)", filename, gzipData.length / 1024);

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                new GZIPInputStream(new ByteArrayInputStream(gzipData))))) {

            String headerLine = reader.readLine();
            if (headerLine == null) return;

            String[] headers = parseCsvLine(headerLine);
            int eventIdIdx = indexOf(headers, "EVENT_ID");
            int eventTypeIdx = indexOf(headers, "EVENT_TYPE");
            int beginDateIdx = indexOf(headers, "BEGIN_DATE_TIME");
            int endDateIdx = indexOf(headers, "END_DATE_TIME");
            int stateIdx = indexOf(headers, "STATE");
            int stateFipsIdx = indexOf(headers, "STATE_FIPS");
            int countyFipsIdx = indexOf(headers, "CZ_FIPS");
            int czTypeIdx = indexOf(headers, "CZ_TYPE");
            int injDirIdx = indexOf(headers, "INJURIES_DIRECT");
            int injIndIdx = indexOf(headers, "INJURIES_INDIRECT");
            int deathDirIdx = indexOf(headers, "DEATHS_DIRECT");
            int deathIndIdx = indexOf(headers, "DEATHS_INDIRECT");
            int dmgPropIdx = indexOf(headers, "DAMAGE_PROPERTY");
            int dmgCropIdx = indexOf(headers, "DAMAGE_CROPS");
            int magIdx = indexOf(headers, "MAGNITUDE");
            int magTypeIdx = indexOf(headers, "MAGNITUDE_TYPE");
            int floodCauseIdx = indexOf(headers, "FLOOD_CAUSE");
            int torScaleIdx = indexOf(headers, "TOR_F_SCALE");

            if (eventIdIdx < 0 || eventTypeIdx < 0 || beginDateIdx < 0) {
                log.warn("Missing required columns in {}", filename);
                return;
            }

            List<StormEvent> batch = new ArrayList<>();
            String line;

            while ((line = reader.readLine()) != null) {
                try {
                    String[] cols = parseCsvLine(line);

                    String eventId = safeGet(cols, eventIdIdx);
                    if (eventId == null || eventId.isEmpty()) { skipped.incrementAndGet(); continue; }
                    if (repository.existsByEventId(eventId)) { skipped.incrementAndGet(); continue; }

                    // Only include county-level events (CZ_TYPE = "C")
                    String czType = safeGet(cols, czTypeIdx);
                    if (czType != null && !czType.equals("C")) { skipped.incrementAndGet(); continue; }

                    String stateFips = safeGet(cols, stateFipsIdx);
                    String countyFips = safeGet(cols, countyFipsIdx);
                    String fips = buildFips(stateFips, countyFips);

                    LocalDate beginDate = parseDateTime(safeGet(cols, beginDateIdx));
                    if (beginDate == null) { skipped.incrementAndGet(); continue; }

                    StormEvent event = StormEvent.builder()
                            .eventId(eventId)
                            .eventType(safeGet(cols, eventTypeIdx))
                            .beginDate(beginDate)
                            .endDate(parseDateTime(safeGet(cols, endDateIdx)))
                            .state(safeGet(cols, stateIdx))
                            .stateFips(stateFips)
                            .fips(fips)
                            .injuriesDirect(safeInt(cols, injDirIdx))
                            .injuriesIndirect(safeInt(cols, injIndIdx))
                            .deathsDirect(safeInt(cols, deathDirIdx))
                            .deathsIndirect(safeInt(cols, deathIndIdx))
                            .damageProperty(parseDamage(safeGet(cols, dmgPropIdx)))
                            .damageCrops(parseDamage(safeGet(cols, dmgCropIdx)))
                            .magnitude(safeDouble(cols, magIdx))
                            .magnitudeType(safeGet(cols, magTypeIdx))
                            .floodCause(safeGet(cols, floodCauseIdx))
                            .torFScale(safeGet(cols, torScaleIdx))
                            .build();

                    batch.add(event);

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
        }

        log.info("NOAA year {}: total inserted so far = {}", year, inserted.get());
    }

    /**
     * Simple CSV parser that handles quoted fields with commas.
     */
    private String[] parseCsvLine(String line) {
        List<String> fields = new ArrayList<>();
        boolean inQuotes = false;
        StringBuilder current = new StringBuilder();

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                fields.add(current.toString().trim());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        fields.add(current.toString().trim());
        return fields.toArray(new String[0]);
    }

    private int indexOf(String[] headers, String name) {
        for (int i = 0; i < headers.length; i++) {
            if (headers[i].trim().equalsIgnoreCase(name)) return i;
        }
        return -1;
    }

    private String safeGet(String[] cols, int idx) {
        if (idx < 0 || idx >= cols.length) return null;
        String val = cols[idx].trim();
        return val.isEmpty() ? null : val;
    }

    private int safeInt(String[] cols, int idx) {
        String val = safeGet(cols, idx);
        if (val == null) return 0;
        try { return Integer.parseInt(val); }
        catch (NumberFormatException e) { return 0; }
    }

    private Double safeDouble(String[] cols, int idx) {
        String val = safeGet(cols, idx);
        if (val == null) return null;
        try { return Double.parseDouble(val); }
        catch (NumberFormatException e) { return null; }
    }

    private String buildFips(String stateFips, String countyFips) {
        if (stateFips == null || countyFips == null) return null;
        try {
            return String.format("%02d%03d",
                    Integer.parseInt(stateFips), Integer.parseInt(countyFips));
        } catch (NumberFormatException e) { return null; }
    }

    /**
     * Parse NOAA datetime: "01-JAN-23 00:00:00" or "15-MAR-2023 14:30:00"
     */
    private LocalDate parseDateTime(String val) {
        if (val == null) return null;
        try {
            // Try "dd-MMM-yy HH:mm:ss" format
            String datePart = val.split(" ")[0];
            String[] parts = datePart.split("-");
            if (parts.length != 3) return null;

            int day = Integer.parseInt(parts[0]);
            String monthStr = parts[1].substring(0, 3).toUpperCase();
            int month = monthNumber(monthStr);
            int year = Integer.parseInt(parts[2]);
            if (year < 100) year += 2000;

            return LocalDate.of(year, month, day);
        } catch (Exception e) {
            return null;
        }
    }

    private int monthNumber(String abbr) {
        return switch (abbr) {
            case "JAN" -> 1; case "FEB" -> 2; case "MAR" -> 3;
            case "APR" -> 4; case "MAY" -> 5; case "JUN" -> 6;
            case "JUL" -> 7; case "AUG" -> 8; case "SEP" -> 9;
            case "OCT" -> 10; case "NOV" -> 11; case "DEC" -> 12;
            default -> 1;
        };
    }

    /**
     * Parse NOAA damage strings: "25K" → 25000, "1.5M" → 1500000.
     */
    private double parseDamage(String val) {
        if (val == null || val.isEmpty()) return 0.0;
        val = val.trim().toUpperCase();
        try {
            if (val.endsWith("K")) return Double.parseDouble(val.replace("K", "")) * 1_000;
            if (val.endsWith("M")) return Double.parseDouble(val.replace("M", "")) * 1_000_000;
            if (val.endsWith("B")) return Double.parseDouble(val.replace("B", "")) * 1_000_000_000;
            return Double.parseDouble(val);
        } catch (NumberFormatException e) { return 0.0; }
    }
}
