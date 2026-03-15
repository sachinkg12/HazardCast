package com.hazardcast.repository;

import com.hazardcast.model.DroughtIndicator;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.List;

@Repository
public interface DroughtIndicatorRepository extends JpaRepository<DroughtIndicator, Long> {

    boolean existsByFipsAndReportDate(String fips, LocalDate reportDate);

    @Query("""
        SELECT d FROM DroughtIndicator d
        WHERE d.fips = :fips AND d.reportDate >= :since
        ORDER BY d.reportDate DESC
        """)
    List<DroughtIndicator> findByFipsSince(String fips, LocalDate since);

    @Query("""
        SELECT AVG(d.severityScore) FROM DroughtIndicator d
        WHERE d.fips = :fips AND d.reportDate >= :since
        """)
    Double avgSeverityByFipsSince(String fips, LocalDate since);

    @Query("""
        SELECT MAX(d.severityScore) FROM DroughtIndicator d
        WHERE d.fips = :fips AND d.reportDate >= :since
        """)
    Double maxSeverityByFipsSince(String fips, LocalDate since);

    @Query("""
        SELECT COUNT(d) FROM DroughtIndicator d
        WHERE d.fips = :fips AND d.reportDate >= :since AND d.d2Pct > 0
        """)
    int countSevereDroughtWeeks(String fips, LocalDate since);

    /** Average D2+ percentage in window — cascade precursor for wildfire risk */
    @Query("""
        SELECT AVG(d.d2Pct) FROM DroughtIndicator d
        WHERE d.fips = :fips AND d.reportDate >= :since AND d.reportDate < :until
        """)
    Double avgD2PctBetween(String fips, LocalDate since, LocalDate until);
}
