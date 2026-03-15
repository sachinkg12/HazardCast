package com.hazardcast.repository;

import com.hazardcast.model.County;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface CountyRepository extends JpaRepository<County, String> {

    List<County> findByStateFips(String stateFips);

    @Query("SELECT c.fips FROM County c")
    List<String> findAllFips();

    @Query("""
        SELECT c FROM County c
        WHERE c.latitude BETWEEN :minLat AND :maxLat
        AND c.longitude BETWEEN :minLon AND :maxLon
        """)
    List<County> findInBoundingBox(double minLat, double maxLat, double minLon, double maxLon);
}
