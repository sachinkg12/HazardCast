package com.hazardcast.pipeline.feature;

import com.hazardcast.model.*;
import com.hazardcast.repository.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
class CascadeFeatureComputerTest {

    @Autowired private CascadeFeatureComputer cascadeComputer;
    @Autowired private CountyRepository countyRepository;
    @Autowired private DroughtIndicatorRepository droughtRepo;
    @Autowired private WildfireEventRepository wildfireRepo;
    @Autowired private StormEventRepository stormRepo;
    @Autowired private DisasterDeclarationRepository declarationRepo;
    @Autowired private SeismicEventRepository seismicRepo;

    private static final String FIPS = "06037";
    private County county;

    @BeforeEach
    void setUp() {
        droughtRepo.deleteAll();
        wildfireRepo.deleteAll();
        stormRepo.deleteAll();
        declarationRepo.deleteAll();
        seismicRepo.deleteAll();
        countyRepository.deleteAll();

        county = countyRepository.save(County.builder()
                .fips(FIPS).stateFips("06")
                .countyName("Los Angeles").stateName("California").stateAbbr("CA")
                .latitude(34.05).longitude(-118.24)
                .population(9_700_000L).housingUnits(3_500_000L)
                .medianHomeValue(681_000L).landAreaSqMi(4058.0)
                .build());
    }

    @Test
    void noDataProducesAllZeros() {
        CountyFeatureVector fv = new CountyFeatureVector();
        fv.setFips(FIPS);

        cascadeComputer.compute(fv, county, LocalDate.of(2024, 1, 1));

        assertThat(fv.getCascadeDroughtFireRisk()).isEqualTo(0.0);
        assertThat(fv.getCascadeFireFloodRisk()).isEqualTo(0.0);
        assertThat(fv.getCascadeHurricaneFloodRisk()).isEqualTo(0.0);
        assertThat(fv.getCascadeEarthquakeLandslideRisk()).isEqualTo(0.0);
        assertThat(fv.getCascadeStormCompoundCount()).isEqualTo(0);
        assertThat(fv.getCascadeActiveChains()).isEqualTo(0);
        assertThat(fv.getCascadeMaxChainLength()).isEqualTo(1);
    }

    @Test
    void droughtFireInteractionRequiresBothConditions() {
        // Drought alone should give 0 interaction (no wildfire)
        droughtRepo.save(DroughtIndicator.builder()
                .fips(FIPS).reportDate(LocalDate.of(2023, 10, 1))
                .d0Pct(80.0).d1Pct(60.0).d2Pct(40.0).d3Pct(10.0).d4Pct(0.0)
                .severityScore(200.0)
                .build());

        CountyFeatureVector fv = new CountyFeatureVector();
        fv.setFips(FIPS);
        cascadeComputer.compute(fv, county, LocalDate.of(2024, 1, 1));

        // D2% is 40, but no wildfire → log1p(0) = 0 → interaction = 0
        assertThat(fv.getCascadeDroughtFireRisk()).isEqualTo(0.0);
        assertThat(fv.getCascadeActiveChains()).isEqualTo(0);
    }

    @Test
    void droughtFireInteractionPositiveWhenBothPresent() {
        // Both drought and wildfire → positive interaction
        droughtRepo.save(DroughtIndicator.builder()
                .fips(FIPS).reportDate(LocalDate.of(2023, 10, 1))
                .d0Pct(80.0).d1Pct(60.0).d2Pct(40.0).d3Pct(10.0).d4Pct(0.0)
                .severityScore(200.0)
                .build());
        wildfireRepo.save(WildfireEvent.builder()
                .fireId("FIRE001").nearestFips(FIPS)
                .discoveryDate(LocalDate.of(2023, 11, 1))
                .acresBurned(5000.0)
                .build());

        CountyFeatureVector fv = new CountyFeatureVector();
        fv.setFips(FIPS);
        cascadeComputer.compute(fv, county, LocalDate.of(2024, 1, 1));

        // D2% ≈ 40 * log1p(5000) ≈ 40 * 8.52 ≈ 340
        assertThat(fv.getCascadeDroughtFireRisk()).isGreaterThan(0.0);
        assertThat(fv.getCascadeActiveChains()).isGreaterThanOrEqualTo(1);
        assertThat(fv.getCascadeMaxChainLength()).isGreaterThanOrEqualTo(2);
    }

    @Test
    void fireFloodInteractionRequiresBothConditions() {
        // Wildfire alone, no floods → interaction = 0
        wildfireRepo.save(WildfireEvent.builder()
                .fireId("FIRE002").nearestFips(FIPS)
                .discoveryDate(LocalDate.of(2023, 6, 1))
                .acresBurned(10000.0)
                .build());

        CountyFeatureVector fv = new CountyFeatureVector();
        fv.setFips(FIPS);
        cascadeComputer.compute(fv, county, LocalDate.of(2024, 1, 1));

        // Burn scars present but no flood events → interaction = log1p(10000) * 0 = 0
        assertThat(fv.getCascadeFireFloodRisk()).isEqualTo(0.0);
    }

    @Test
    void fireFloodInteractionPositiveWhenBothPresent() {
        // Wildfire followed by flood → post-fire debris flow signal
        wildfireRepo.save(WildfireEvent.builder()
                .fireId("FIRE003").nearestFips(FIPS)
                .discoveryDate(LocalDate.of(2023, 6, 1))
                .acresBurned(10000.0)
                .build());
        stormRepo.save(StormEvent.builder()
                .eventId("FLOOD001").fips(FIPS)
                .eventType("Flash Flood")
                .beginDate(LocalDate.of(2023, 12, 1))
                .deathsDirect(0).deathsIndirect(0)
                .injuriesDirect(0).injuriesIndirect(0)
                .damageProperty(50000.0).damageCrops(0.0)
                .build());

        CountyFeatureVector fv = new CountyFeatureVector();
        fv.setFips(FIPS);
        cascadeComputer.compute(fv, county, LocalDate.of(2024, 1, 1));

        assertThat(fv.getCascadeFireFloodRisk()).isGreaterThan(0.0);
    }

    @Test
    void tripleChainDroughtFireFlood() {
        // All three: drought → wildfire → flood
        droughtRepo.save(DroughtIndicator.builder()
                .fips(FIPS).reportDate(LocalDate.of(2023, 7, 1))
                .d0Pct(90.0).d1Pct(70.0).d2Pct(50.0).d3Pct(20.0).d4Pct(5.0)
                .severityScore(300.0)
                .build());
        wildfireRepo.save(WildfireEvent.builder()
                .fireId("FIRE004").nearestFips(FIPS)
                .discoveryDate(LocalDate.of(2023, 8, 1))
                .acresBurned(20000.0)
                .build());
        stormRepo.save(StormEvent.builder()
                .eventId("FLOOD002").fips(FIPS)
                .eventType("Flood")
                .beginDate(LocalDate.of(2023, 11, 1))
                .deathsDirect(0).deathsIndirect(0)
                .injuriesDirect(0).injuriesIndirect(0)
                .damageProperty(100000.0).damageCrops(0.0)
                .build());

        CountyFeatureVector fv = new CountyFeatureVector();
        fv.setFips(FIPS);
        cascadeComputer.compute(fv, county, LocalDate.of(2024, 1, 1));

        assertThat(fv.getCascadeDroughtFireRisk()).isGreaterThan(0.0);
        assertThat(fv.getCascadeFireFloodRisk()).isGreaterThan(0.0);
        assertThat(fv.getCascadeMaxChainLength()).isEqualTo(3);
        assertThat(fv.getCascadeActiveChains()).isGreaterThanOrEqualTo(2);
    }

    @Test
    void stormCompoundCountsRecentSevereStorms() {
        // 3 severe storms in 30 days
        for (int i = 0; i < 3; i++) {
            stormRepo.save(StormEvent.builder()
                    .eventId("STORM" + i).fips(FIPS)
                    .eventType("Tornado")
                    .beginDate(LocalDate.of(2023, 12, 10 + i))
                    .deathsDirect(0).deathsIndirect(0)
                    .injuriesDirect(0).injuriesIndirect(0)
                    .damageProperty(0.0).damageCrops(0.0)
                    .build());
        }

        CountyFeatureVector fv = new CountyFeatureVector();
        fv.setFips(FIPS);
        cascadeComputer.compute(fv, county, LocalDate.of(2024, 1, 1));

        assertThat(fv.getCascadeStormCompoundCount()).isEqualTo(3);
        assertThat(fv.getCascadeActiveChains()).isGreaterThanOrEqualTo(1);
    }
}
