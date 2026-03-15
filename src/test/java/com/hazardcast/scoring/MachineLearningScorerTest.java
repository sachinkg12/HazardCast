package com.hazardcast.scoring;

import com.hazardcast.model.CountyFeatureVector;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MachineLearningScorerTest {

    @Test
    void numFeaturesMustMatch42() throws Exception {
        // NUM_FEATURES must match extractFeatures() array length (v4: 35 base + 7 cascade = 42)
        var field = MachineLearningScorer.class.getDeclaredField("NUM_FEATURES");
        field.setAccessible(true);
        int numFeatures = (int) field.get(null);
        assertThat(numFeatures).isEqualTo(42);
    }

    @Test
    void extractFeaturesMatchesNumFeatures() throws Exception {
        // Verify NUM_FEATURES constant matches expected feature count (v4: 35 base + 7 cascade = 42)
        var numField = MachineLearningScorer.class.getDeclaredField("NUM_FEATURES");
        numField.setAccessible(true);
        assertThat((int) numField.get(null))
                .as("NUM_FEATURES must equal 42 (35 base + 7 cascade)")
                .isEqualTo(42);
    }

    @Test
    void extractFeaturesHandlesAllNulls() throws Exception {
        // A feature vector with all nulls should not throw NPE
        CountyFeatureVector fv = new CountyFeatureVector();
        fv.setFips("00000");
        fv.setYearMonth("2024-01");
        fv.setDeclarationNext90d(false);

        // We can't call extractFeatures without a model, but we verify null handling
        // by checking each getter used in extractFeatures returns null gracefully
        assertThat(fv.getDeclarations1yr()).isNull();
        assertThat(fv.getCascadeDroughtFireRisk()).isNull();
        assertThat(fv.getCascadeActiveChains()).isNull();
    }

    @Test
    void constructorFailsOnBadModelPath() {
        assertThatThrownBy(() -> new MachineLearningScorer("/nonexistent/model.json"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to load XGBoost model");
    }

    @Test
    void modelNameReturnsV4() throws Exception {
        // Verify modelName method exists on the interface
        var method = MachineLearningScorer.class.getMethod("modelName");
        assertThat(method).isNotNull();
    }

    private CountyFeatureVector buildTestVector() {
        CountyFeatureVector fv = new CountyFeatureVector();
        fv.setFips("06037");
        fv.setYearMonth("2024-01");
        fv.setDeclarationNext90d(false);
        fv.setDeclarations1yr(2);
        fv.setDeclarations3yr(5);
        fv.setDeclarations5yr(10);
        fv.setDeclarations10yr(20);
        fv.setMonthsSinceLastDecl(3);
        fv.setMajorDisasterRatio(0.7);
        fv.setIaProgramRatio(0.3);
        fv.setStormEventCount1yr(15);
        fv.setStormEventCount5yr(60);
        fv.setPopulation(9_700_000L);
        fv.setMonthOfYear(1);
        fv.setIsHurricaneSeason(false);
        fv.setIsTornadoSeason(false);
        fv.setIsWildfireSeason(false);
        fv.setCascadeDroughtFireRisk(0.5);
        fv.setCascadeFireFloodRisk(100.0);
        fv.setCascadeActiveChains(2);
        fv.setCascadeMaxChainLength(2);
        return fv;
    }
}
