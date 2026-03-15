package com.hazardcast.repository;

import com.hazardcast.model.SeismicEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
public interface SeismicEventRepository extends JpaRepository<SeismicEvent, Long> {

    boolean existsByEventId(String eventId);

    @Query("""
        SELECT s FROM SeismicEvent s
        WHERE s.nearestFips = :fips AND s.eventTime >= :since
        """)
    List<SeismicEvent> findByFipsSince(String fips, Instant since);

    @Query("""
        SELECT COUNT(s) FROM SeismicEvent s
        WHERE s.nearestFips = :fips AND s.eventTime >= :since
        """)
    int countByFipsSince(String fips, Instant since);

    @Query("""
        SELECT MAX(s.magnitude) FROM SeismicEvent s
        WHERE s.nearestFips = :fips AND s.eventTime >= :since
        """)
    Double maxMagnitudeByFipsSince(String fips, Instant since);

    @Query("""
        SELECT AVG(s.magnitude) FROM SeismicEvent s
        WHERE s.nearestFips = :fips AND s.eventTime >= :since
        """)
    Double avgMagnitudeByFipsSince(String fips, Instant since);

    @Query("SELECT MAX(s.eventTime) FROM SeismicEvent s")
    Instant findLatestEventTime();

    /** Sum of 10^magnitude (proxy for energy) in window — cascade landslide precursor */
    @Query("""
        SELECT COUNT(s) FROM SeismicEvent s
        WHERE s.nearestFips = :fips AND s.eventTime >= :since AND s.eventTime < :until
        AND s.magnitude >= 4.0
        """)
    int countSignificantQuakesBetween(String fips, Instant since, Instant until);
}
