package com.hazardcast.pipeline.feature;

import com.hazardcast.model.County;
import com.hazardcast.model.CountyFeatureVector;
import com.hazardcast.repository.StormEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.LocalDate;

/**
 * Computes storm features: event counts by type, casualties,
 * property/crop damage, tornado F-scale.
 */
@Component
@RequiredArgsConstructor
public class StormFeatureComputer implements FeatureComputer {

    private final StormEventRepository repository;

    @Override
    public String domain() { return "storm"; }

    @Override
    public int order() { return 30; }

    @Override
    public void compute(CountyFeatureVector fv, County county, LocalDate asOfDate) {
        String fips = county.getFips();
        LocalDate oneYearAgo = asOfDate.minusYears(1);
        LocalDate fiveYearsAgo = asOfDate.minusYears(5);

        fv.setStormEventCount1yr(repository.countByFipsSince(fips, oneYearAgo));
        fv.setStormEventCount5yr(repository.countByFipsSince(fips, fiveYearsAgo));
        fv.setStormDeaths5yr(repository.totalDeathsByFipsSince(fips, fiveYearsAgo));
        fv.setStormPropertyDamage5yr(repository.totalPropertyDamageByFipsSince(fips, fiveYearsAgo));

        // Storm type breakdowns
        fv.setTornadoCount5yr(repository.countByFipsAndTypeSince(fips, fiveYearsAgo, "Tornado"));
        fv.setFloodCount5yr(repository.countByFipsAndTypeSince(fips, fiveYearsAgo, "Flood"));
        fv.setHurricaneCount5yr(repository.countByFipsAndTypeSince(fips, fiveYearsAgo, "Hurricane"));
        fv.setHailCount5yr(repository.countByFipsAndTypeSince(fips, fiveYearsAgo, "Hail"));

        // Injuries + crop damage + max tornado scale from individual events
        var recentStorms = repository.findByFipsSince(fips, fiveYearsAgo);

        fv.setStormInjuries5yr(recentStorms.stream()
                .mapToInt(s -> orZero(s.getInjuriesDirect()) + orZero(s.getInjuriesIndirect()))
                .sum());

        fv.setStormCropDamage5yr(recentStorms.stream()
                .mapToDouble(s -> s.getDamageCrops() != null ? s.getDamageCrops() : 0.0)
                .sum());

        fv.setMaxTorFScale5yr(recentStorms.stream()
                .filter(s -> s.getTorFScale() != null)
                .mapToInt(s -> parseFScale(s.getTorFScale()))
                .max().orElse(0));
    }

    private int orZero(Integer val) { return val != null ? val : 0; }

    private int parseFScale(String fScale) {
        String cleaned = fScale.replaceAll("[^0-9]", "");
        try { return Integer.parseInt(cleaned); }
        catch (NumberFormatException e) { return 0; }
    }
}
