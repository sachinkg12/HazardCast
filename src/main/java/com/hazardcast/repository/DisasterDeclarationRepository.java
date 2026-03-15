package com.hazardcast.repository;

import com.hazardcast.model.DisasterDeclaration;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.List;

@Repository
public interface DisasterDeclarationRepository extends JpaRepository<DisasterDeclaration, Long> {

    List<DisasterDeclaration> findByFips(String fips);

    List<DisasterDeclaration> findByFipsAndDeclarationDateBetween(
            String fips, LocalDate start, LocalDate end);

    @Query("""
        SELECT COUNT(d) FROM DisasterDeclaration d
        WHERE d.fips = :fips AND d.declarationDate >= :since
        """)
    int countByFipsSince(String fips, LocalDate since);

    @Query("""
        SELECT MAX(d.declarationDate) FROM DisasterDeclaration d
        WHERE d.fips = :fips
        """)
    LocalDate findLatestDeclarationDate(String fips);

    @Query("""
        SELECT COUNT(d) FROM DisasterDeclaration d
        WHERE d.fips = :fips AND d.declarationType = 'DR'
        """)
    int countMajorDisasters(String fips);

    @Query("SELECT MAX(d.disasterNumber) FROM DisasterDeclaration d")
    Integer findMaxDisasterNumber();

    boolean existsByDisasterNumberAndFips(Integer disasterNumber, String fips);

    @Query("""
        SELECT COUNT(d) * 1.0 / (SELECT COUNT(DISTINCT d2.fips) FROM DisasterDeclaration d2
            WHERE d2.fips LIKE CONCAT(:stateFips, '%') AND LENGTH(d2.fips) = 5)
        FROM DisasterDeclaration d
        WHERE d.fips LIKE CONCAT(:stateFips, '%') AND LENGTH(d.fips) = 5
        AND d.declarationDate >= :since
        """)
    Double avgDeclarationsPerCountyInState(String stateFips, LocalDate since);

    /** Count hurricane-type declarations in window — cascade trigger for inland flooding */
    @Query("""
        SELECT COUNT(d) FROM DisasterDeclaration d
        WHERE d.fips = :fips AND d.declarationDate >= :since AND d.declarationDate < :until
        AND d.incidentType IN ('Hurricane', 'Tropical Storm', 'Typhoon')
        """)
    int countHurricaneDeclarationsBetween(String fips, LocalDate since, LocalDate until);
}
