package com.hazardcast.repository;

import com.hazardcast.model.StormEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.List;

@Repository
public interface StormEventRepository extends JpaRepository<StormEvent, Long> {

    boolean existsByEventId(String eventId);

    @Query("""
        SELECT s FROM StormEvent s
        WHERE s.fips = :fips AND s.beginDate >= :since
        """)
    List<StormEvent> findByFipsSince(String fips, LocalDate since);

    @Query("""
        SELECT COUNT(s) FROM StormEvent s
        WHERE s.fips = :fips AND s.beginDate >= :since
        """)
    int countByFipsSince(String fips, LocalDate since);

    @Query("""
        SELECT COUNT(s) FROM StormEvent s
        WHERE s.fips = :fips AND s.beginDate >= :since AND s.eventType = :eventType
        """)
    int countByFipsAndTypeSince(String fips, LocalDate since, String eventType);

    @Query("""
        SELECT COALESCE(SUM(s.deathsDirect + s.deathsIndirect), 0) FROM StormEvent s
        WHERE s.fips = :fips AND s.beginDate >= :since
        """)
    int totalDeathsByFipsSince(String fips, LocalDate since);

    @Query("""
        SELECT COALESCE(SUM(s.damageProperty), 0) FROM StormEvent s
        WHERE s.fips = :fips AND s.beginDate >= :since
        """)
    double totalPropertyDamageByFipsSince(String fips, LocalDate since);

    /** Count severe storm events in short window — cascade compound stress indicator */
    @Query("""
        SELECT COUNT(s) FROM StormEvent s
        WHERE s.fips = :fips AND s.beginDate >= :since AND s.beginDate < :until
        AND (s.damageProperty > 10000 OR s.deathsDirect > 0 OR s.injuriesDirect > 0
             OR s.eventType IN ('Tornado', 'Hurricane', 'Tropical Storm'))
        """)
    int countSevereStormsBetween(String fips, LocalDate since, LocalDate until);

    /** Count flood events in window — for cascade chain detection */
    @Query("""
        SELECT COUNT(s) FROM StormEvent s
        WHERE s.fips = :fips AND s.beginDate >= :since AND s.beginDate < :until
        AND s.eventType IN ('Flash Flood', 'Flood', 'Coastal Flood', 'Lakeshore Flood')
        """)
    int countFloodEventsBetween(String fips, LocalDate since, LocalDate until);
}
