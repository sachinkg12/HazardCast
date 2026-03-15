package com.hazardcast.pipeline.feature;

import com.hazardcast.model.County;
import com.hazardcast.model.CountyFeatureVector;
import com.hazardcast.repository.NfipClaimRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.LocalDate;

/**
 * Computes NFIP flood insurance features: claim counts, total/average payouts.
 */
@Component
@RequiredArgsConstructor
public class NfipFeatureComputer implements FeatureComputer {

    private final NfipClaimRepository repository;

    @Override
    public String domain() { return "nfip"; }

    @Override
    public int order() { return 55; }

    @Override
    public void compute(CountyFeatureVector fv, County county, LocalDate asOfDate) {
        String fips = county.getFips();
        LocalDate fiveYearsAgo = asOfDate.minusYears(5);

        fv.setNfipClaimCount5yr(repository.countByFipsSince(fips, fiveYearsAgo));
        fv.setNfipTotalPayout5yr(repository.totalPayoutByFipsSince(fips, fiveYearsAgo));
        fv.setNfipAvgPayout5yr(orZero(repository.avgPayoutByFipsSince(fips, fiveYearsAgo)));
    }

    private double orZero(Double val) { return val != null ? val : 0.0; }
}
