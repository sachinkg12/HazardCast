package com.hazardcast.pipeline.feature;

import com.hazardcast.config.ApiProperties;
import com.hazardcast.model.County;
import com.hazardcast.model.CountyFeatureVector;
import com.hazardcast.repository.DisasterDeclarationRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.LocalDate;

/**
 * Computes the target variable: will this county have a FEMA declaration
 * within the prediction horizon (default 90 days)?
 */
@Component
@RequiredArgsConstructor
public class TargetFeatureComputer implements FeatureComputer {

    private final DisasterDeclarationRepository declarationRepository;
    private final ApiProperties properties;

    @Override
    public String domain() { return "target"; }

    @Override
    public int order() { return 0; } // Must run first

    @Override
    public void compute(CountyFeatureVector fv, County county, LocalDate asOfDate) {
        int horizonDays = properties.getPipeline().getPredictionHorizonDays();
        LocalDate horizonEnd = asOfDate.plusDays(horizonDays);

        var futureDeclarations = declarationRepository
                .findByFipsAndDeclarationDateBetween(county.getFips(), asOfDate, horizonEnd);

        fv.setDeclarationNext90d(!futureDeclarations.isEmpty());
        fv.setDeclarationTypeNext90d(futureDeclarations.isEmpty() ? null :
                futureDeclarations.get(0).getIncidentType());
    }
}
