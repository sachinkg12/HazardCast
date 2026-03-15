package com.hazardcast.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "hazardcast")
@Getter @Setter
public class ApiProperties {

    private Api api = new Api();
    private Pipeline pipeline = new Pipeline();

    @Getter @Setter
    public static class Api {
        private FemaConfig fema = new FemaConfig();
        private UsgsConfig usgs = new UsgsConfig();
        private NoaaConfig noaa = new NoaaConfig();
        private CensusConfig census = new CensusConfig();
    }

    @Getter @Setter
    public static class FemaConfig {
        private String baseUrl = "https://www.fema.gov/api/open/v2";
        private int pageSize = 1000;
    }

    @Getter @Setter
    public static class UsgsConfig {
        private String baseUrl = "https://earthquake.usgs.gov/fdsnws/event/1";
        private double minMagnitude = 2.5;
    }

    @Getter @Setter
    public static class NoaaConfig {
        private String baseUrl = "https://www.ncei.noaa.gov/cdo-web/api/v2";
    }

    @Getter @Setter
    public static class CensusConfig {
        private String baseUrl = "https://geocoding.geo.census.gov/geocoder";
    }

    @Getter @Setter
    public static class Pipeline {
        private int predictionHorizonDays = 90;
        private String parquetOutputDir = "./output";
    }
}
