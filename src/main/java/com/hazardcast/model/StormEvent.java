package com.hazardcast.model;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDate;

@Entity
@Table(name = "storm_events", indexes = {
        @Index(name = "idx_storm_fips_date", columnList = "fips, begin_date")
})
@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class StormEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", unique = true, nullable = false, length = 20)
    private String eventId;

    @Column(name = "event_type", nullable = false, length = 100)
    private String eventType;

    @Column(name = "begin_date", nullable = false)
    private LocalDate beginDate;

    @Column(name = "end_date")
    private LocalDate endDate;

    @Column(length = 50)
    private String state;

    @Column(name = "state_fips", length = 2)
    private String stateFips;

    @Column(length = 5)
    private String fips;

    @Column(name = "injuries_direct")
    private Integer injuriesDirect;

    @Column(name = "injuries_indirect")
    private Integer injuriesIndirect;

    @Column(name = "deaths_direct")
    private Integer deathsDirect;

    @Column(name = "deaths_indirect")
    private Integer deathsIndirect;

    @Column(name = "damage_property")
    private Double damageProperty;

    @Column(name = "damage_crops")
    private Double damageCrops;

    private Double magnitude;

    @Column(name = "magnitude_type", length = 10)
    private String magnitudeType;

    @Column(name = "flood_cause", length = 100)
    private String floodCause;

    @Column(name = "tor_f_scale", length = 5)
    private String torFScale;
}
