package com.hazardcast.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.hazardcast.config.ApiProperties;
import com.hazardcast.model.County;
import com.hazardcast.model.SeismicEvent;
import com.hazardcast.repository.CountyRepository;
import com.hazardcast.repository.SeismicEventRepository;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Ingests USGS earthquake data via the FDSN Event Web Service.
 * Fetches all M2.5+ earthquakes in the US, mapped to nearest county FIPS.
 */
@Component
public class UsgsIngestionService extends AbstractDataProvider {

    private final ApiProperties properties;
    private final SeismicEventRepository repository;
    private final CountyRepository countyRepository;

    private static final double MIN_LAT = 18.0, MAX_LAT = 72.0;
    private static final double MIN_LON = -180.0, MAX_LON = -65.0;
    private static final int CHUNK_YEARS = 5;

    public UsgsIngestionService(WebClient webClient, ApiProperties properties,
                                SeismicEventRepository repository,
                                CountyRepository countyRepository) {
        super(webClient);
        this.properties = properties;
        this.repository = repository;
        this.countyRepository = countyRepository;
    }

    @Override
    public String getName() { return "usgs_earthquakes"; }

    @Override
    public String getDescription() { return "USGS M2.5+ earthquakes (1964-present)"; }

    @Override
    public int priority() { return 20; }

    @Override
    protected List<PageRequest> buildPageRequests(int startYear, int endYear) {
        List<PageRequest> pages = new ArrayList<>();
        String baseUrl = properties.getApi().getUsgs().getBaseUrl();
        double minMag = properties.getApi().getUsgs().getMinMagnitude();

        for (int year = startYear; year < endYear; year += CHUNK_YEARS) {
            int chunkEnd = Math.min(year + CHUNK_YEARS, endYear);
            String url = String.format(
                    "%s/query?format=geojson&starttime=%d-01-01&endtime=%d-01-01" +
                    "&minmagnitude=%.1f&minlatitude=%.1f&maxlatitude=%.1f" +
                    "&minlongitude=%.1f&maxlongitude=%.1f&limit=20000&orderby=time",
                    baseUrl, year, chunkEnd, minMag,
                    MIN_LAT, MAX_LAT, MIN_LON, MAX_LON);
            pages.add(new PageRequest(url, year + "-" + chunkEnd));
        }
        return pages;
    }

    @Override
    protected JsonNode extractRecords(JsonNode response) {
        return response.get("features");
    }

    @Override
    protected boolean isDuplicate(JsonNode record) {
        String eventId = record.get("id").asText();
        return repository.existsByEventId(eventId);
    }

    @Override
    protected Object parseRecord(JsonNode feature) {
        JsonNode props = feature.get("properties");
        JsonNode coords = feature.get("geometry").get("coordinates");

        double lon = coords.get(0).asDouble();
        double lat = coords.get(1).asDouble();
        double depth = coords.get(2).asDouble();
        long timeMs = props.get("time").asLong();

        String nearestFips = findNearestCountyFips(lat, lon);

        return SeismicEvent.builder()
                .eventId(feature.get("id").asText())
                .eventTime(Instant.ofEpochMilli(timeMs))
                .latitude(lat)
                .longitude(lon)
                .depthKm(depth)
                .magnitude(props.get("mag").asDouble())
                .magType(textOrNull(props, "magType"))
                .place(textOrNull(props, "place"))
                .eventType(textOrNull(props, "type"))
                .nearestFips(nearestFips)
                .distanceKm(nearestFips != null ? computeDistance(lat, lon, nearestFips) : null)
                .build();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void saveBatch(List<Object> batch) {
        repository.saveAll(batch.stream()
                .map(o -> (SeismicEvent) o)
                .toList());
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

    static double haversine(double lat1, double lon1, double lat2, double lon2) {
        double R = 6371.0;
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
                   Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                   Math.sin(dLon / 2) * Math.sin(dLon / 2);
        return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }
}
