package com.hazardcast.repository;

import com.hazardcast.model.WildfireEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.List;

@Repository
public interface WildfireEventRepository extends JpaRepository<WildfireEvent, Long> {

    boolean existsByFireId(String fireId);

    @Query("""
        SELECT w FROM WildfireEvent w
        WHERE w.nearestFips = :fips AND w.discoveryDate >= :since
        """)
    List<WildfireEvent> findByFipsSince(String fips, LocalDate since);

    @Query("""
        SELECT COUNT(w) FROM WildfireEvent w
        WHERE w.nearestFips = :fips AND w.discoveryDate >= :since
        """)
    int countByFipsSince(String fips, LocalDate since);

    @Query("""
        SELECT COALESCE(SUM(w.acresBurned), 0) FROM WildfireEvent w
        WHERE w.nearestFips = :fips AND w.discoveryDate >= :since
        """)
    double totalAcresByFipsSince(String fips, LocalDate since);

    @Query("""
        SELECT MAX(w.acresBurned) FROM WildfireEvent w
        WHERE w.nearestFips = :fips AND w.discoveryDate >= :since
        """)
    Double maxAcresByFipsSince(String fips, LocalDate since);

    /** Total acres burned in window — cascade precursor for post-fire flooding */
    @Query("""
        SELECT COALESCE(SUM(w.acresBurned), 0) FROM WildfireEvent w
        WHERE w.nearestFips = :fips AND w.discoveryDate >= :since AND w.discoveryDate < :until
        """)
    double totalAcresBetween(String fips, LocalDate since, LocalDate until);
}
