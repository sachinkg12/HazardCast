package com.hazardcast.model;

import jakarta.persistence.*;
import lombok.*;

@Entity
@Table(name = "counties")
@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class County {

    @Id
    @Column(length = 5)
    private String fips;

    @Column(name = "state_fips", nullable = false, length = 2)
    private String stateFips;

    @Column(name = "county_name", nullable = false, length = 100)
    private String countyName;

    @Column(name = "state_name", nullable = false, length = 50)
    private String stateName;

    @Column(name = "state_abbr", nullable = false, length = 2)
    private String stateAbbr;

    private Double latitude;
    private Double longitude;
    private Long population;

    @Column(name = "housing_units")
    private Long housingUnits;

    @Column(name = "median_home_value")
    private Long medianHomeValue;

    @Column(name = "land_area_sq_mi")
    private Double landAreaSqMi;
}
