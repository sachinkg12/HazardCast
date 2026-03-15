package com.hazardcast.repository;

import com.hazardcast.model.NfipClaim;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;

@Repository
public interface NfipClaimRepository extends JpaRepository<NfipClaim, Long> {

    @Query("""
        SELECT COUNT(c) FROM NfipClaim c
        WHERE c.fips = :fips AND c.dateOfLoss >= :since
        """)
    int countByFipsSince(String fips, LocalDate since);

    @Query("""
        SELECT COALESCE(SUM(c.amountPaid), 0) FROM NfipClaim c
        WHERE c.fips = :fips AND c.dateOfLoss >= :since
        """)
    double totalPayoutByFipsSince(String fips, LocalDate since);

    @Query("""
        SELECT AVG(c.amountPaid) FROM NfipClaim c
        WHERE c.fips = :fips AND c.dateOfLoss >= :since
        """)
    Double avgPayoutByFipsSince(String fips, LocalDate since);

    boolean existsByFipsAndDateOfLossAndAmountPaid(String fips, LocalDate dateOfLoss, Double amountPaid);
}
