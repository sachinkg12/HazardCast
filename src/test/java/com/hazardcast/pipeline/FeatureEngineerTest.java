package com.hazardcast.pipeline;

import com.hazardcast.model.County;
import com.hazardcast.model.DisasterDeclaration;
import com.hazardcast.model.SeismicEvent;
import com.hazardcast.model.StormEvent;
import com.hazardcast.repository.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
class FeatureEngineerTest {

    @Autowired private FeatureEngineer featureEngineer;
    @Autowired private CountyRepository countyRepository;
    @Autowired private DisasterDeclarationRepository declarationRepository;
    @Autowired private SeismicEventRepository seismicRepository;
    @Autowired private StormEventRepository stormRepository;
    @Autowired private CountyFeatureVectorRepository featureRepository;

    private static final String TEST_FIPS = "06037"; // LA County

    @BeforeEach
    void setUp() {
        featureRepository.deleteAll();
        declarationRepository.deleteAll();
        seismicRepository.deleteAll();
        stormRepository.deleteAll();
        countyRepository.deleteAll();

        countyRepository.save(County.builder()
                .fips(TEST_FIPS)
                .stateFips("06")
                .countyName("Los Angeles")
                .stateName("California")
                .stateAbbr("CA")
                .latitude(34.0522)
                .longitude(-118.2437)
                .population(9_721_138L)
                .housingUnits(3_500_000L)
                .medianHomeValue(681_000L)
                .landAreaSqMi(4_058.0)
                .build());
    }

    @Test
    void shouldComputeTargetVariable() {
        // Declaration 30 days from now (within 90-day horizon)
        declarationRepository.save(DisasterDeclaration.builder()
                .disasterNumber(5000)
                .fips(TEST_FIPS)
                .declarationDate(LocalDate.of(2024, 2, 1))
                .declarationType("DR")
                .build());

        int count = featureEngineer.computeForMonth(YearMonth.of(2024, 1));
        assertThat(count).isEqualTo(1);

        var fv = featureRepository.findByFipsAndYearMonth(TEST_FIPS, "2024-01");
        assertThat(fv).isPresent();
        assertThat(fv.get().getDeclarationNext90d()).isTrue();
    }

    @Test
    void shouldComputeNegativeTarget() {
        // No declarations near the target month
        declarationRepository.save(DisasterDeclaration.builder()
                .disasterNumber(5001)
                .fips(TEST_FIPS)
                .declarationDate(LocalDate.of(2020, 6, 1))
                .declarationType("DR")
                .build());

        featureEngineer.computeForMonth(YearMonth.of(2024, 1));

        var fv = featureRepository.findByFipsAndYearMonth(TEST_FIPS, "2024-01");
        assertThat(fv).isPresent();
        assertThat(fv.get().getDeclarationNext90d()).isFalse();
    }

    @Test
    void shouldComputeRollingWindows() {
        declarationRepository.save(DisasterDeclaration.builder()
                .disasterNumber(5010).fips(TEST_FIPS)
                .declarationDate(LocalDate.of(2023, 6, 1)).declarationType("DR").build());
        declarationRepository.save(DisasterDeclaration.builder()
                .disasterNumber(5011).fips(TEST_FIPS)
                .declarationDate(LocalDate.of(2021, 3, 1)).declarationType("DR").build());
        declarationRepository.save(DisasterDeclaration.builder()
                .disasterNumber(5012).fips(TEST_FIPS)
                .declarationDate(LocalDate.of(2015, 1, 1)).declarationType("EM").build());

        featureEngineer.computeForMonth(YearMonth.of(2024, 1));

        var fv = featureRepository.findByFipsAndYearMonth(TEST_FIPS, "2024-01").get();
        assertThat(fv.getDeclarations1yr()).isEqualTo(1);  // 2023-06
        assertThat(fv.getDeclarations3yr()).isEqualTo(2);  // 2023-06 + 2021-03
        assertThat(fv.getDeclarations5yr()).isEqualTo(2);
        assertThat(fv.getDeclarations10yr()).isEqualTo(3);
    }

    @Test
    void shouldComputeSeasonalityFlags() {
        featureEngineer.computeForMonth(YearMonth.of(2024, 7)); // July

        var fv = featureRepository.findByFipsAndYearMonth(TEST_FIPS, "2024-07").get();
        assertThat(fv.getIsHurricaneSeason()).isTrue();   // Jun-Nov
        assertThat(fv.getIsTornadoSeason()).isFalse();    // Mar-Jun
        assertThat(fv.getIsWildfireSeason()).isTrue();     // Jun-Oct
        assertThat(fv.getMonthOfYear()).isEqualTo(7);
    }

    @Test
    void shouldComputeSocioeconomicFeatures() {
        featureEngineer.computeForMonth(YearMonth.of(2024, 1));

        var fv = featureRepository.findByFipsAndYearMonth(TEST_FIPS, "2024-01").get();
        assertThat(fv.getPopulation()).isEqualTo(9_721_138L);
        assertThat(fv.getMedianHomeValue()).isEqualTo(681_000L);
        assertThat(fv.getPopulationDensity()).isGreaterThan(2000.0); // LA is dense
    }

    @Test
    void shouldBeIdempotent() {
        featureEngineer.computeForMonth(YearMonth.of(2024, 1));
        featureEngineer.computeForMonth(YearMonth.of(2024, 1)); // re-run

        // Should update, not duplicate
        var all = featureRepository.findByFipsOrderByYearMonth(TEST_FIPS);
        long jan2024 = all.stream()
                .filter(fv -> "2024-01".equals(fv.getYearMonth()))
                .count();
        assertThat(jan2024).isEqualTo(1);
    }
}
