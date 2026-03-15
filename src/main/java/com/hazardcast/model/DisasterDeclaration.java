package com.hazardcast.model;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDate;

@Entity
@Table(name = "disaster_declarations", indexes = {
        @Index(name = "idx_disaster_fips_date", columnList = "fips, declaration_date")
})
@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class DisasterDeclaration {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "disaster_number", nullable = false)
    private Integer disasterNumber;

    @Column(name = "declaration_date", nullable = false)
    private LocalDate declarationDate;

    @Column(name = "disaster_type", length = 50)
    private String disasterType;

    @Column(name = "incident_type", length = 100)
    private String incidentType;

    @Column(length = 500)
    private String title;

    @Column(length = 50)
    private String state;

    @Column(length = 5)
    private String fips;

    @Column(name = "incident_begin_date")
    private LocalDate incidentBeginDate;

    @Column(name = "incident_end_date")
    private LocalDate incidentEndDate;

    @Column(name = "declaration_type", length = 10)
    private String declarationType;

    @Column(name = "ih_program")
    private Boolean ihProgram;

    @Column(name = "ia_program")
    private Boolean iaProgram;

    @Column(name = "pa_program")
    private Boolean paProgram;

    @Column(name = "hm_program")
    private Boolean hmProgram;
}
