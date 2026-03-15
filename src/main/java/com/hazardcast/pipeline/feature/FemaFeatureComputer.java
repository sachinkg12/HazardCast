package com.hazardcast.pipeline.feature;

import com.hazardcast.model.County;
import com.hazardcast.model.CountyFeatureVector;
import com.hazardcast.repository.DisasterDeclarationRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

/**
 * Computes FEMA disaster declaration features: rolling window counts,
 * recency, severity ratios.
 */
@Component
@RequiredArgsConstructor
public class FemaFeatureComputer implements FeatureComputer {

    private final DisasterDeclarationRepository repository;

    @Override
    public String domain() { return "fema"; }

    @Override
    public int order() { return 10; }

    @Override
    public void compute(CountyFeatureVector fv, County county, LocalDate asOfDate) {
        String fips = county.getFips();

        fv.setDeclarations1yr(repository.countByFipsSince(fips, asOfDate.minusYears(1)));
        fv.setDeclarations3yr(repository.countByFipsSince(fips, asOfDate.minusYears(3)));
        fv.setDeclarations5yr(repository.countByFipsSince(fips, asOfDate.minusYears(5)));
        fv.setDeclarations10yr(repository.countByFipsSince(fips, asOfDate.minusYears(10)));
        fv.setDeclarationsTotal(repository.countByFipsSince(fips, LocalDate.of(1953, 1, 1)));

        LocalDate lastDecl = repository.findLatestDeclarationDate(fips);
        fv.setMonthsSinceLastDecl(lastDecl != null ?
                (int) ChronoUnit.MONTHS.between(lastDecl, asOfDate) : null);

        int totalDecl = fv.getDeclarationsTotal();
        int majorDecl = repository.countMajorDisasters(fips);
        fv.setMajorDisasterRatio(totalDecl > 0 ? (double) majorDecl / totalDecl : 0.0);

        var allDeclarations = repository.findByFips(fips);
        long iaCount = allDeclarations.stream()
                .filter(d -> Boolean.TRUE.equals(d.getIaProgram()))
                .count();
        fv.setIaProgramRatio(totalDecl > 0 ? (double) iaCount / totalDecl : 0.0);
    }
}
