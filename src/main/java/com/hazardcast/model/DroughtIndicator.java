package com.hazardcast.model;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDate;

@Entity
@Table(name = "drought_indicators", indexes = {
        @Index(name = "idx_drought_fips_date", columnList = "fips, report_date")
})
@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class DroughtIndicator {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(length = 5, nullable = false)
    private String fips;

    @Column(name = "report_date", nullable = false)
    private LocalDate reportDate;

    /** Percentage of county in D0 (Abnormally Dry) or worse */
    @Column(name = "d0_pct")
    private Double d0Pct;

    /** Percentage in D1 (Moderate Drought) or worse */
    @Column(name = "d1_pct")
    private Double d1Pct;

    /** Percentage in D2 (Severe Drought) or worse */
    @Column(name = "d2_pct")
    private Double d2Pct;

    /** Percentage in D3 (Extreme Drought) or worse */
    @Column(name = "d3_pct")
    private Double d3Pct;

    /** Percentage in D4 (Exceptional Drought) */
    @Column(name = "d4_pct")
    private Double d4Pct;

    /** Weighted severity score: 1*D1 + 2*D2 + 3*D3 + 4*D4 */
    @Column(name = "severity_score")
    private Double severityScore;
}
