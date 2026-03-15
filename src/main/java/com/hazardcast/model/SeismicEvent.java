package com.hazardcast.model;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;

@Entity
@Table(name = "seismic_events", indexes = {
        @Index(name = "idx_seismic_fips_time", columnList = "nearest_fips, event_time")
})
@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class SeismicEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", unique = true, nullable = false, length = 50)
    private String eventId;

    @Column(name = "event_time", nullable = false)
    private Instant eventTime;

    @Column(nullable = false)
    private Double latitude;

    @Column(nullable = false)
    private Double longitude;

    @Column(name = "depth_km")
    private Double depthKm;

    @Column(nullable = false)
    private Double magnitude;

    @Column(name = "mag_type", length = 10)
    private String magType;

    @Column(length = 255)
    private String place;

    @Column(name = "event_type", length = 50)
    private String eventType;

    @Column(name = "nearest_fips", length = 5)
    private String nearestFips;

    @Column(name = "distance_km")
    private Double distanceKm;
}
