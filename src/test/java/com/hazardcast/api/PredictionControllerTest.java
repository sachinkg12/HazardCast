package com.hazardcast.api;

import com.hazardcast.model.County;
import com.hazardcast.model.CountyFeatureVector;
import com.hazardcast.repository.CountyFeatureVectorRepository;
import com.hazardcast.repository.CountyRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.time.YearMonth;

import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
class PredictionControllerTest {

    @Autowired private MockMvc mockMvc;
    @Autowired private CountyRepository countyRepository;
    @Autowired private CountyFeatureVectorRepository featureRepository;

    private static final String FIPS = "48201";

    @BeforeEach
    void setUp() {
        featureRepository.deleteAll();
        countyRepository.deleteAll();

        countyRepository.save(County.builder()
                .fips(FIPS).stateFips("48")
                .countyName("Harris County").stateName("Texas").stateAbbr("TX")
                .latitude(29.76).longitude(-95.37)
                .population(4_700_000L).housingUnits(1_700_000L)
                .medianHomeValue(200_000L).landAreaSqMi(1777.0)
                .build());
    }

    @Test
    void returns404ForUnknownCounty() throws Exception {
        mockMvc.perform(get("/api/predict/99999"))
                .andExpect(status().isNotFound());
    }

    @Test
    void returnsNoFeaturesMessageWhenNoneComputed() throws Exception {
        mockMvc.perform(get("/api/predict/" + FIPS))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("no_features_computed"))
                .andExpect(jsonPath("$.message").value("No prediction data available for this county yet."))
                .andExpect(jsonPath("$.message", not(containsString("POST"))));
    }

    @Test
    void returnsPredictionForCurrentMonth() throws Exception {
        String currentMonth = YearMonth.now().toString();
        saveFeatureVector(FIPS, currentMonth);

        mockMvc.perform(get("/api/predict/" + FIPS))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.fips").value(FIPS))
                .andExpect(jsonPath("$.county").value("Harris County"))
                .andExpect(jsonPath("$.state").value("TX"))
                .andExpect(jsonPath("$.yearMonth").value(currentMonth))
                .andExpect(jsonPath("$.modelType").value("xgboost_v4"))
                .andExpect(jsonPath("$.warning").doesNotExist());
    }

    @Test
    void fallsBackToLatestMonthWhenCurrentMissing() throws Exception {
        saveFeatureVector(FIPS, "2024-06");

        mockMvc.perform(get("/api/predict/" + FIPS))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.yearMonth").value("2024-06"));
    }

    @Test
    void showsWarningForStaleData() throws Exception {
        // Data from 6 months ago — should trigger stale warning
        String staleMonth = YearMonth.now().minusMonths(6).toString();
        saveFeatureVector(FIPS, staleMonth);

        mockMvc.perform(get("/api/predict/" + FIPS))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.warning").exists())
                .andExpect(jsonPath("$.warning", containsString("Accuracy may be reduced")))
                .andExpect(jsonPath("$.warning", not(containsString("POST"))))
                .andExpect(jsonPath("$.dataAge").exists());
    }

    @Test
    void warningDoesNotExposeInternalEndpoints() throws Exception {
        saveFeatureVector(FIPS, "2023-01");

        mockMvc.perform(get("/api/predict/" + FIPS))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.warning", not(containsString("/api/pipeline"))))
                .andExpect(jsonPath("$.warning", not(containsString("POST"))));
    }

    @Test
    void includesTopFactorsAndSeasonality() throws Exception {
        saveFeatureVector(FIPS, YearMonth.now().toString());

        mockMvc.perform(get("/api/predict/" + FIPS))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.topFactors").exists())
                .andExpect(jsonPath("$.seasonality").exists())
                .andExpect(jsonPath("$.topFactors.declarations_5yr").isNumber());
    }

    private void saveFeatureVector(String fips, String yearMonth) {
        CountyFeatureVector fv = CountyFeatureVector.builder()
                .fips(fips)
                .yearMonth(yearMonth)
                .declarationNext90d(false)
                .declarations1yr(3).declarations3yr(8)
                .declarations5yr(15).declarations10yr(30)
                .monthsSinceLastDecl(2)
                .majorDisasterRatio(0.6).iaProgramRatio(0.4)
                .stormEventCount1yr(10).stormEventCount5yr(50)
                .stormDeaths5yr(0).stormInjuries5yr(5)
                .stormPropertyDamage5yr(100000.0).stormCropDamage5yr(0.0)
                .tornadoCount5yr(3).floodCount5yr(5)
                .hurricaneCount5yr(1).hailCount5yr(8)
                .maxTorFScale5yr(2)
                .earthquakeCount1yr(0).earthquakeCount5yr(0)
                .maxMagnitude1yr(0.0).maxMagnitude5yr(0.0)
                .avgMagnitude5yr(0.0).totalEnergy5yr(0.0)
                .earthquakeDepthAvg(0.0).earthquakeDistanceAvgKm(0.0)
                .population(4_700_000L).housingUnits(1_700_000L)
                .medianHomeValue(200_000L)
                .populationDensity(2644.0).landAreaSqMi(1777.0)
                .monthOfYear(Integer.parseInt(yearMonth.split("-")[1]))
                .isHurricaneSeason(true).isTornadoSeason(false).isWildfireSeason(false)
                .droughtSeverityAvg5yr(10.0).droughtMaxSeverity5yr(50.0)
                .severeDroughtWeeks5yr(20).droughtD4PctMax5yr(5.0)
                .wildfireCount1yr(0).wildfireCount5yr(2)
                .wildfireAcresBurned5yr(500.0).wildfireMaxAcres5yr(300.0)
                .nfipClaimCount5yr(100).nfipTotalPayout5yr(5000000.0)
                .nfipAvgPayout5yr(50000.0)
                .neighborAvgDeclarations5yr(10.0).stateAvgDeclarations5yr(12.0)
                .cascadeDroughtFireRisk(0.0).cascadeFireFloodRisk(0.0)
                .cascadeHurricaneFloodRisk(0.0).cascadeEarthquakeLandslideRisk(0.0)
                .cascadeStormCompoundCount(0).cascadeActiveChains(0)
                .cascadeMaxChainLength(1)
                .computedAt(Instant.now())
                .build();
        featureRepository.save(fv);
    }
}
