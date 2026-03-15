package com.hazardcast.model;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDate;

@Entity
@Table(name = "wildfire_events", indexes = {
        @Index(name = "idx_wildfire_fips_date", columnList = "nearest_fips, discovery_date")
})
@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class WildfireEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "fire_id", unique = true, length = 50)
    private String fireId;

    @Column(name = "fire_name", length = 200)
    private String fireName;

    @Column(name = "discovery_date")
    private LocalDate discoveryDate;

    @Column(name = "containment_date")
    private LocalDate containmentDate;

    private Double latitude;
    private Double longitude;

    @Column(name = "acres_burned")
    private Double acresBurned;

    @Column(name = "fire_cause", length = 100)
    private String fireCause;

    @Column(length = 50)
    private String state;

    @Column(name = "nearest_fips", length = 5)
    private String nearestFips;

    @Column(name = "distance_km")
    private Double distanceKm;
}
