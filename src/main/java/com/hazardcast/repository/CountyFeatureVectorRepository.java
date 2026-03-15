package com.hazardcast.repository;

import com.hazardcast.model.CountyFeatureVector;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface CountyFeatureVectorRepository extends JpaRepository<CountyFeatureVector, Long> {

    Optional<CountyFeatureVector> findByFipsAndYearMonth(String fips, String yearMonth);

    List<CountyFeatureVector> findByFipsOrderByYearMonth(String fips);

    @Query("""
        SELECT cfv FROM CountyFeatureVector cfv
        WHERE cfv.yearMonth = :yearMonth
        ORDER BY cfv.fips
        """)
    List<CountyFeatureVector> findAllByYearMonth(String yearMonth);

    @Query("SELECT COUNT(cfv) FROM CountyFeatureVector cfv")
    long countAll();

    @Query("""
        SELECT AVG(cfv.declarations5yr) FROM CountyFeatureVector cfv
        WHERE cfv.fips IN :neighborFips AND cfv.yearMonth = :yearMonth
        """)
    Double avgNeighborDeclarations5yr(List<String> neighborFips, String yearMonth);
}
