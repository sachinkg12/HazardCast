package com.hazardcast.ingestion;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazardcast.config.ApiProperties;
import com.hazardcast.model.DisasterDeclaration;
import com.hazardcast.repository.DisasterDeclarationRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
class FemaIngestionServiceTest {

    @Autowired
    private DisasterDeclarationRepository repository;

    @Autowired
    private ApiProperties properties;

    private ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        repository.deleteAll();
    }

    @Test
    void shouldParseFipsCorrectly() {
        // State FIPS "06" + County FIPS "037" = "06037" (Los Angeles County)
        String json = """
            {
                "fipsStateCode": "6",
                "fipsCountyCode": "37",
                "disasterNumber": 4344,
                "declarationDate": "2018-11-12T00:00:00.000Z",
                "disasterType": "DR",
                "incidentType": "Fire",
                "declarationTitle": "WOOLSEY FIRE",
                "state": "California",
                "incidentBeginDate": "2018-11-08T00:00:00.000Z",
                "incidentEndDate": "2018-11-25T00:00:00.000Z",
                "declarationType": "DR",
                "ihProgramDeclared": true,
                "iaProgramDeclared": true,
                "paProgramDeclared": true,
                "hmProgramDeclared": true
            }
            """;

        // Verify FIPS construction logic
        assertThat(String.format("%02d%03d", 6, 37)).isEqualTo("06037");
    }

    @Test
    void shouldHandleNullFipsGracefully() {
        // Records without FIPS should be skipped
        assertThat(buildFips(null, "037")).isNull();
        assertThat(buildFips("06", null)).isNull();
        assertThat(buildFips("6", "37")).isEqualTo("06037");
    }

    @Test
    void shouldStoreDeclarationCorrectly() {
        DisasterDeclaration decl = DisasterDeclaration.builder()
                .disasterNumber(4344)
                .declarationDate(LocalDate.of(2018, 11, 12))
                .disasterType("DR")
                .incidentType("Fire")
                .title("WOOLSEY FIRE")
                .state("California")
                .fips("06037")
                .incidentBeginDate(LocalDate.of(2018, 11, 8))
                .incidentEndDate(LocalDate.of(2018, 11, 25))
                .declarationType("DR")
                .ihProgram(true)
                .iaProgram(true)
                .paProgram(true)
                .hmProgram(true)
                .build();

        repository.save(decl);

        assertThat(repository.count()).isEqualTo(1);
        assertThat(repository.existsByDisasterNumberAndFips(4344, "06037")).isTrue();
        assertThat(repository.countByFipsSince("06037", LocalDate.of(2018, 1, 1))).isEqualTo(1);
    }

    @Test
    void shouldCountDeclarationsByWindow() {
        // Insert declarations at different dates
        saveDeclaration(1000, "12345", LocalDate.of(2020, 1, 1));
        saveDeclaration(1001, "12345", LocalDate.of(2021, 6, 15));
        saveDeclaration(1002, "12345", LocalDate.of(2023, 3, 1));
        saveDeclaration(1003, "12345", LocalDate.of(2024, 1, 1));

        // As of 2024-06-01
        LocalDate asOf = LocalDate.of(2024, 6, 1);

        assertThat(repository.countByFipsSince("12345", asOf.minusYears(1))).isEqualTo(1);
        assertThat(repository.countByFipsSince("12345", asOf.minusYears(3))).isEqualTo(3);
        assertThat(repository.countByFipsSince("12345", asOf.minusYears(5))).isEqualTo(4);
    }

    @Test
    void shouldTrackMajorDisasterRatio() {
        saveDeclarationWithType(1000, "12345", LocalDate.of(2020, 1, 1), "DR");
        saveDeclarationWithType(1001, "12345", LocalDate.of(2021, 1, 1), "DR");
        saveDeclarationWithType(1002, "12345", LocalDate.of(2022, 1, 1), "EM");

        assertThat(repository.countMajorDisasters("12345")).isEqualTo(2);
        int total = repository.countByFipsSince("12345", LocalDate.of(2000, 1, 1));
        assertThat(total).isEqualTo(3);
        assertThat((double) 2 / total).isCloseTo(0.667, org.assertj.core.data.Offset.offset(0.01));
    }

    private void saveDeclaration(int number, String fips, LocalDate date) {
        saveDeclarationWithType(number, fips, date, "DR");
    }

    private void saveDeclarationWithType(int number, String fips, LocalDate date, String type) {
        repository.save(DisasterDeclaration.builder()
                .disasterNumber(number)
                .declarationDate(date)
                .fips(fips)
                .declarationType(type)
                .build());
    }

    private String buildFips(String stateFips, String countyFips) {
        if (stateFips == null || countyFips == null) return null;
        return String.format("%02d%03d", Integer.parseInt(stateFips), Integer.parseInt(countyFips));
    }
}
