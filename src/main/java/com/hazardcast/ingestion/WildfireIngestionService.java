package com.hazardcast.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.hazardcast.model.County;
import com.hazardcast.model.WildfireEvent;
import com.hazardcast.repository.CountyRepository;
import com.hazardcast.repository.WildfireEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Ingests NIFC (National Interagency Fire Center) wildfire data
 * from the ArcGIS REST service.
 *
 * Source: https://data-nifc.opendata.arcgis.com/
 * Historical perimeters + current year fire points. Free, no key.
 *
 * Field names (actual API response):
 *   Year-to-date: attr_IncidentName, attr_FireDiscoveryDateTime, attr_ContainmentDateTime,
 *                 poly_GISAcres, attr_FireCause, attr_POOState, attr_InitialLatitude, attr_InitialLongitude
 *   Historical:   incidentname, perimeterdatetime, gisacres, state (lowercase, no lat/lon fields)
 */
@Component
@Slf4j
public class WildfireIngestionService implements DataProvider {

    private final WebClient webClient;
    private final WildfireEventRepository repository;
    private final CountyRepository countyRepository;

    // NIFC Year-to-Date Perimeters (current year) — updated field names with attr_/poly_ prefix
    private static final String NIFC_URL =
            "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/" +
            "WFIGS_Interagency_Perimeters_YearToDate/FeatureServer/0/query" +
            "?where=attr_FireDiscoveryDateTime+IS+NOT+NULL" +
            "&outFields=OBJECTID,attr_IncidentName,attr_FireDiscoveryDateTime," +
            "attr_ContainmentDateTime,poly_GISAcres,attr_FireCause,attr_POOState," +
            "attr_InitialLatitude,attr_InitialLongitude" +
            "&f=json&resultOffset=%d&resultRecordCount=1000";

    // All fire perimeters (2000-present) — has attr_* fields + centroids, 35K+ records
    private static final String NIFC_ALL_PERIMETERS_URL =
            "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/" +
            "WFIGS_Interagency_Perimeters/FeatureServer/0/query" +
            "?where=1%%3D1&outFields=OBJECTID,poly_IncidentName,poly_GISAcres," +
            "attr_FireDiscoveryDateTime,attr_POOState,attr_InitialLatitude,attr_InitialLongitude" +
            "&returnGeometry=false&returnCentroid=true&f=json&resultOffset=%d&resultRecordCount=1000";

    public WildfireIngestionService(WebClient webClient, WildfireEventRepository repository,
                                    CountyRepository countyRepository) {
        this.webClient = webClient;
        this.repository = repository;
        this.countyRepository = countyRepository;
    }

    @Override
    public String getName() { return "nifc_wildfires"; }

    @Override
    public String getDescription() { return "NIFC wildfire perimeters (2000-present)"; }

    @Override
    public int priority() { return 50; }

    @Override
    public IngestionResult ingest(int startYear, int endYear) {
        log.info("Starting NIFC wildfire ingestion...");
        var result = new IngestionResult(getName());
        var inserted = new AtomicInteger(0);
        var skipped = new AtomicInteger(0);

        // Ingest historical fires (2000-2018)
        ingestHistorical(inserted, skipped);
        // Ingest recent fires (current year)
        ingestYearToDate(inserted, skipped);

        result.setRecordsInserted(inserted.get());
        result.setRecordsSkipped(skipped.get());
        result.complete();
        log.info("NIFC ingestion complete: {} inserted, {} skipped", inserted.get(), skipped.get());
        return result;
    }

    private void ingestYearToDate(AtomicInteger inserted, AtomicInteger skipped) {
        int offset = 0;
        while (true) {
            String url = String.format(NIFC_URL, offset);
            log.debug("NIFC YTD: fetching offset={}", offset);

            JsonNode response = fetchJson(url);
            if (response == null || !response.has("features")) break;
            JsonNode features = response.get("features");
            if (!features.isArray() || features.isEmpty()) break;

            List<WildfireEvent> batch = new ArrayList<>();

            for (JsonNode feature : features) {
                try {
                    JsonNode attrs = feature.get("attributes");
                    String fireId = "R-" + attrs.get("OBJECTID").asText();

                    if (repository.existsByFireId(fireId)) { skipped.incrementAndGet(); continue; }

                    double lat = doubleOr(attrs, "attr_InitialLatitude", 0);
                    double lon = doubleOr(attrs, "attr_InitialLongitude", 0);
                    if (lat == 0 || lon == 0) { skipped.incrementAndGet(); continue; }

                    LocalDate discovery = parseEpochMs(attrs.get("attr_FireDiscoveryDateTime"));

                    // State is "US-XX" format — extract abbreviation
                    String stateRaw = textOrNull(attrs, "attr_POOState");
                    String state = stateRaw != null && stateRaw.startsWith("US-") ?
                            stateRaw.substring(3) : stateRaw;

                    String nearestFips = findNearestCountyFips(lat, lon);

                    batch.add(WildfireEvent.builder()
                            .fireId(fireId)
                            .fireName(textOrNull(attrs, "attr_IncidentName"))
                            .discoveryDate(discovery)
                            .containmentDate(parseEpochMs(attrs.get("attr_ContainmentDateTime")))
                            .latitude(lat)
                            .longitude(lon)
                            .acresBurned(doubleOr(attrs, "poly_GISAcres", 0))
                            .state(state)
                            .nearestFips(nearestFips)
                            .distanceKm(nearestFips != null ? computeDistance(lat, lon, nearestFips) : null)
                            .build());
                } catch (Exception e) {
                    skipped.incrementAndGet();
                }
            }

            if (!batch.isEmpty()) {
                repository.saveAll(batch);
                inserted.addAndGet(batch.size());
                log.info("NIFC YTD: inserted {} (total: {})", batch.size(), inserted.get());
            }

            if (features.size() < 1000) break;
            offset += 1000;
        }
    }

    private void ingestHistorical(AtomicInteger inserted, AtomicInteger skipped) {
        int offset = 0;
        while (true) {
            String url = String.format(NIFC_ALL_PERIMETERS_URL, offset);
            log.debug("NIFC Perimeters: fetching offset={}", offset);

            JsonNode response = fetchJson(url);
            if (response == null || !response.has("features")) break;
            JsonNode features = response.get("features");
            if (!features.isArray() || features.isEmpty()) break;

            List<WildfireEvent> batch = new ArrayList<>();

            for (JsonNode feature : features) {
                try {
                    JsonNode attrs = feature.get("attributes");
                    String fireId = "P-" + attrs.get("OBJECTID").asText();

                    if (repository.existsByFireId(fireId)) { skipped.incrementAndGet(); continue; }

                    // Try attr_InitialLatitude/Longitude first, fall back to centroid
                    double lat = doubleOr(attrs, "attr_InitialLatitude", 0);
                    double lon = doubleOr(attrs, "attr_InitialLongitude", 0);

                    if (lat == 0 || lon == 0) {
                        JsonNode centroid = feature.get("centroid");
                        if (centroid != null) {
                            lon = centroid.has("x") ? centroid.get("x").asDouble(0) : 0;
                            lat = centroid.has("y") ? centroid.get("y").asDouble(0) : 0;
                        }
                    }

                    if (lat == 0 || lon == 0) { skipped.incrementAndGet(); continue; }

                    LocalDate discovery = parseEpochMs(attrs.get("attr_FireDiscoveryDateTime"));

                    String stateRaw = textOrNull(attrs, "attr_POOState");
                    String state = stateRaw != null && stateRaw.startsWith("US-") ?
                            stateRaw.substring(3) : stateRaw;

                    String nearestFips = findNearestCountyFips(lat, lon);

                    batch.add(WildfireEvent.builder()
                            .fireId(fireId)
                            .fireName(textOrNull(attrs, "poly_IncidentName"))
                            .discoveryDate(discovery)
                            .latitude(lat)
                            .longitude(lon)
                            .acresBurned(doubleOr(attrs, "poly_GISAcres", 0))
                            .state(state)
                            .nearestFips(nearestFips)
                            .distanceKm(nearestFips != null ? computeDistance(lat, lon, nearestFips) : null)
                            .build());
                } catch (Exception e) {
                    skipped.incrementAndGet();
                }
            }

            if (!batch.isEmpty()) {
                repository.saveAll(batch);
                inserted.addAndGet(batch.size());
                log.info("NIFC Perimeters: inserted {} (total: {})", batch.size(), inserted.get());
            }

            if (features.size() < 1000) break;
            offset += 1000;
        }
    }

    private JsonNode fetchJson(String url) {
        try {
            return webClient.get()
                    .uri(java.net.URI.create(url))
                    .retrieve()
                    .bodyToMono(JsonNode.class)
                    .block();
        } catch (Exception e) {
            log.warn("NIFC fetch failed: {}", e.getMessage());
            return null;
        }
    }

    private String findNearestCountyFips(double lat, double lon) {
        double delta = 0.5;
        var counties = countyRepository.findInBoundingBox(
                lat - delta, lat + delta, lon - delta, lon + delta);
        if (counties.isEmpty()) return null;
        return counties.stream()
                .min((a, b) -> Double.compare(
                        haversine(lat, lon, a.getLatitude(), a.getLongitude()),
                        haversine(lat, lon, b.getLatitude(), b.getLongitude())))
                .map(County::getFips)
                .orElse(null);
    }

    private Double computeDistance(double lat, double lon, String fips) {
        return countyRepository.findById(fips)
                .map(c -> haversine(lat, lon, c.getLatitude(), c.getLongitude()))
                .orElse(null);
    }

    private LocalDate parseEpochMs(JsonNode node) {
        if (node == null || node.isNull()) return null;
        long ms = node.asLong(0);
        if (ms == 0) return null;
        return Instant.ofEpochMilli(ms).atZone(ZoneOffset.UTC).toLocalDate();
    }

    static double haversine(double lat1, double lon1, double lat2, double lon2) {
        double R = 6371.0;
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                   Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                   Math.sin(dLon / 2) * Math.sin(dLon / 2);
        return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
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
