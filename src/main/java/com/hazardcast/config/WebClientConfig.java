package com.hazardcast.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class WebClientConfig {

    @Bean
    public WebClient webClient() {
        return WebClient.builder()
                .defaultHeader(HttpHeaders.USER_AGENT, "HazardCast/0.1 (disaster-prediction-research)")
                .exchangeStrategies(ExchangeStrategies.builder()
                        .codecs(config -> config.defaultCodecs()
                                .maxInMemorySize(16 * 1024 * 1024)) // 16MB for large FEMA responses
                        .build())
                .build();
    }
}
