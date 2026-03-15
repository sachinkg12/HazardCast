package com.hazardcast.model;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDate;

@Entity
@Table(name = "nfip_claims", indexes = {
        @Index(name = "idx_nfip_fips_date", columnList = "fips, date_of_loss")
})
@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class NfipClaim {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(length = 5)
    private String fips;

    @Column(name = "date_of_loss")
    private LocalDate dateOfLoss;

    @Column(name = "amount_paid")
    private Double amountPaid;

    @Column(name = "building_damage")
    private Double buildingDamage;

    @Column(name = "contents_damage")
    private Double contentsDamage;

    @Column(name = "flood_zone", length = 10)
    private String floodZone;

    @Column(name = "year_of_loss")
    private Integer yearOfLoss;
}
