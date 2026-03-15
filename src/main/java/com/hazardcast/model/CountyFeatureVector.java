package com.hazardcast.model;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;

/**
 * One row per county per month. This is the ML-ready feature table.
 * Each row contains rolling-window aggregates from all data sources
 * plus the target variable (declaration in next 90 days).
 */
@Entity
@Table(name = "county_feature_vectors",
       uniqueConstraints = @UniqueConstraint(columnNames = {"fips", "year_month"}))
@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class CountyFeatureVector {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(length = 5, nullable = false)
    private String fips;

    @Column(name = "year_month", length = 7, nullable = false)
    private String yearMonth;

    // ---- Target ----
    @Column(name = "declaration_next_90d", nullable = false)
    private Boolean declarationNext90d;

    @Column(name = "declaration_type_next_90d", length = 100)
    private String declarationTypeNext90d;

    // ---- FEMA features ----
    @Column(name = "declarations_1yr") private Integer declarations1yr;
    @Column(name = "declarations_3yr") private Integer declarations3yr;
    @Column(name = "declarations_5yr") private Integer declarations5yr;
    @Column(name = "declarations_10yr") private Integer declarations10yr;
    @Column(name = "declarations_total") private Integer declarationsTotal;
    @Column(name = "months_since_last_decl") private Integer monthsSinceLastDecl;
    @Column(name = "major_disaster_ratio") private Double majorDisasterRatio;
    @Column(name = "ia_program_ratio") private Double iaProgramRatio;

    // ---- Seismic features ----
    @Column(name = "earthquake_count_1yr") private Integer earthquakeCount1yr;
    @Column(name = "earthquake_count_5yr") private Integer earthquakeCount5yr;
    @Column(name = "max_magnitude_1yr") private Double maxMagnitude1yr;
    @Column(name = "max_magnitude_5yr") private Double maxMagnitude5yr;
    @Column(name = "avg_magnitude_5yr") private Double avgMagnitude5yr;
    @Column(name = "total_energy_5yr") private Double totalEnergy5yr;
    @Column(name = "earthquake_depth_avg") private Double earthquakeDepthAvg;
    @Column(name = "earthquake_distance_avg_km") private Double earthquakeDistanceAvgKm;

    // ---- Storm features ----
    @Column(name = "storm_event_count_1yr") private Integer stormEventCount1yr;
    @Column(name = "storm_event_count_5yr") private Integer stormEventCount5yr;
    @Column(name = "storm_deaths_5yr") private Integer stormDeaths5yr;
    @Column(name = "storm_injuries_5yr") private Integer stormInjuries5yr;
    @Column(name = "storm_property_damage_5yr") private Double stormPropertyDamage5yr;
    @Column(name = "storm_crop_damage_5yr") private Double stormCropDamage5yr;
    @Column(name = "tornado_count_5yr") private Integer tornadoCount5yr;
    @Column(name = "flood_count_5yr") private Integer floodCount5yr;
    @Column(name = "hurricane_count_5yr") private Integer hurricaneCount5yr;
    @Column(name = "hail_count_5yr") private Integer hailCount5yr;
    @Column(name = "max_tor_f_scale_5yr") private Integer maxTorFScale5yr;

    // ---- Socioeconomic ----
    private Long population;
    @Column(name = "housing_units") private Long housingUnits;
    @Column(name = "median_home_value") private Long medianHomeValue;
    @Column(name = "population_density") private Double populationDensity;
    @Column(name = "land_area_sq_mi") private Double landAreaSqMi;

    // ---- Temporal ----
    @Column(name = "month_of_year", nullable = false) private Integer monthOfYear;
    @Column(name = "is_hurricane_season") private Boolean isHurricaneSeason;
    @Column(name = "is_tornado_season") private Boolean isTornadoSeason;
    @Column(name = "is_wildfire_season") private Boolean isWildfireSeason;

    // ---- Drought features ----
    @Column(name = "drought_severity_avg_5yr") private Double droughtSeverityAvg5yr;
    @Column(name = "drought_max_severity_5yr") private Double droughtMaxSeverity5yr;
    @Column(name = "severe_drought_weeks_5yr") private Integer severeDroughtWeeks5yr;
    @Column(name = "drought_d4_pct_max_5yr") private Double droughtD4PctMax5yr;

    // ---- Wildfire features ----
    @Column(name = "wildfire_count_1yr") private Integer wildfireCount1yr;
    @Column(name = "wildfire_count_5yr") private Integer wildfireCount5yr;
    @Column(name = "wildfire_acres_burned_5yr") private Double wildfireAcresBurned5yr;
    @Column(name = "wildfire_max_acres_5yr") private Double wildfireMaxAcres5yr;

    // ---- NFIP (Flood Insurance) features ----
    @Column(name = "nfip_claim_count_5yr") private Integer nfipClaimCount5yr;
    @Column(name = "nfip_total_payout_5yr") private Double nfipTotalPayout5yr;
    @Column(name = "nfip_avg_payout_5yr") private Double nfipAvgPayout5yr;

    // ---- Spatial ----
    @Column(name = "neighbor_avg_declarations_5yr") private Double neighborAvgDeclarations5yr;
    @Column(name = "state_avg_declarations_5yr") private Double stateAvgDeclarations5yr;

    // ---- Cascade features (multi-hazard interaction effects) ----
    /** D2+ drought severity in prior 6 months — wildfire precursor */
    @Column(name = "cascade_drought_fire_risk") private Double cascadeDroughtFireRisk;
    /** Wildfire acres burned in prior 18 months — post-fire flood precursor (burn scars → debris flows) */
    @Column(name = "cascade_fire_flood_risk") private Double cascadeFireFloodRisk;
    /** Hurricane-type activity in prior 60 days — inland flood trigger */
    @Column(name = "cascade_hurricane_flood_risk") private Double cascadeHurricaneFloodRisk;
    /** Seismic energy in prior 90 days — landslide precursor */
    @Column(name = "cascade_earthquake_landslide_risk") private Double cascadeEarthquakeLandslideRisk;
    /** Severe storm events in prior 30 days — compound infrastructure stress */
    @Column(name = "cascade_storm_compound_count") private Integer cascadeStormCompoundCount;
    /** Number of active cascade precursor conditions (0-5) */
    @Column(name = "cascade_active_chains") private Integer cascadeActiveChains;
    /** Longest active cascade chain length (e.g., drought→fire→flood = 3) */
    @Column(name = "cascade_max_chain_length") private Integer cascadeMaxChainLength;

    @Column(name = "computed_at")
    private Instant computedAt;

    /** Comma-separated list of feature domains that failed during computation. Null = all succeeded. */
    @Column(name = "failed_domains")
    private String failedDomains;
}
