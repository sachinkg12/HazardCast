package com.hazardcast.pipeline.feature;

import com.hazardcast.model.County;
import com.hazardcast.model.CountyFeatureVector;
import com.hazardcast.repository.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

/**
 * Computes multi-hazard cascade INTERACTION features.
 *
 * Unlike single-domain features that measure one hazard independently,
 * cascade features capture the multiplicative effect of co-occurring
 * hazard precursors. A high drought score alone is already captured by
 * DroughtFeatureComputer. A high wildfire score alone is captured by
 * WildfireFeatureComputer. But the INTERACTION (drought × wildfire)
 * is only high when both conditions are simultaneously present —
 * this is the signal that single-domain features cannot represent.
 *
 * Cascade chains modeled:
 *   1. Drought × Wildfire: drought dries fuel → wildfire ignition risk
 *   2. Wildfire × Flood: burn scars → post-fire debris flows
 *   3. Hurricane × Flood: hurricane remnants → inland flood amplification
 *   4. Earthquake × Landslide: seismic shaking → slope failure
 *   5. Storm Compound: rapid-succession storms → infrastructure collapse
 *   6. Drought × Heat: extended drought in summer → compound effects
 *
 * Implementation: multiplicative interaction terms and conditional
 * co-occurrence indicators, NOT raw hazard values.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class CascadeFeatureComputer implements FeatureComputer {

    private final DroughtIndicatorRepository droughtRepo;
    private final WildfireEventRepository wildfireRepo;
    private final DisasterDeclarationRepository declarationRepo;
    private final StormEventRepository stormRepo;
    private final SeismicEventRepository seismicRepo;

    @Override
    public String domain() { return "cascade"; }

    @Override
    public int order() { return 200; } // Run after all single-domain computers

    @Override
    public void compute(CountyFeatureVector fv, County county, LocalDate asOfDate) {
        String fips = county.getFips();

        // ── Interaction terms (multiplicative — only high when BOTH hazards co-occur) ──

        // Chain 1: Drought × Wildfire interaction
        // drought_severity_6mo * wildfire_acres_1yr — captures the joint condition
        // where dry fuel load (drought) meets recent fire activity
        double droughtSev = safeD(droughtRepo.avgD2PctBetween(fips,
                asOfDate.minusMonths(6), asOfDate));
        double fireAcres = wildfireRepo.totalAcresBetween(fips,
                asOfDate.minusYears(1), asOfDate);
        // Normalize: drought is 0-100%, fire acres can be huge → log-scale
        double droughtFireInteraction = droughtSev * Math.log1p(fireAcres);
        fv.setCascadeDroughtFireRisk(droughtFireInteraction);

        // Chain 2: Wildfire × Flood interaction (post-fire debris flow risk)
        // burn_scar_acres_18mo * flood_events_1yr — burn scars only cause floods
        // when there IS subsequent rainfall/flooding
        double burnScars = wildfireRepo.totalAcresBetween(fips,
                asOfDate.minusMonths(18), asOfDate);
        int floodEvents = stormRepo.countFloodEventsBetween(fips,
                asOfDate.minusYears(1), asOfDate);
        double fireFloodInteraction = Math.log1p(burnScars) * floodEvents;
        fv.setCascadeFireFloodRisk(fireFloodInteraction);

        // Chain 3: Hurricane × Flood interaction
        // hurricane_declarations_60d * flood_events_30d — captures inland flood
        // amplification specifically triggered by hurricane remnants
        int hurricaneDecls = declarationRepo.countHurricaneDeclarationsBetween(fips,
                asOfDate.minusDays(60), asOfDate);
        int recentFloods = stormRepo.countFloodEventsBetween(fips,
                asOfDate.minusDays(30), asOfDate);
        double hurricaneFloodInteraction = (double) hurricaneDecls * recentFloods;
        fv.setCascadeHurricaneFloodRisk(hurricaneFloodInteraction);

        // Chain 4: Earthquake × Landslide interaction
        // significant_quakes_90d * storm_property_damage_30d — seismic destabilization
        // is only dangerous for landslides when combined with saturating rainfall
        Instant until = asOfDate.atStartOfDay(ZoneOffset.UTC).toInstant();
        Instant since90 = asOfDate.minusDays(90).atStartOfDay(ZoneOffset.UTC).toInstant();
        int sigQuakes = seismicRepo.countSignificantQuakesBetween(fips, since90, until);
        int severeStorms = stormRepo.countSevereStormsBetween(fips,
                asOfDate.minusDays(30), asOfDate);
        double quakeLandslideInteraction = (double) sigQuakes * severeStorms;
        fv.setCascadeEarthquakeLandslideRisk(quakeLandslideInteraction);

        // Chain 5: Storm compound — rapid succession storms in 30 days
        // This is inherently an interaction (multiple events compounding)
        fv.setCascadeStormCompoundCount(severeStorms);

        // ── Meta-features: co-occurrence indicators ──

        // Count of active cascade INTERACTIONS (not raw precursors)
        int activeChains = 0;
        if (droughtFireInteraction > 0) activeChains++;
        if (fireFloodInteraction > 0) activeChains++;
        if (hurricaneFloodInteraction > 0) activeChains++;
        if (quakeLandslideInteraction > 0) activeChains++;
        if (severeStorms >= 3) activeChains++;
        fv.setCascadeActiveChains(activeChains);

        // Max chain length — detect triple chains
        int maxChainLength = 1;
        // Drought → Fire → Flood (length 3): all three legs active
        if (droughtFireInteraction > 0 && fireFloodInteraction > 0) {
            maxChainLength = 3;
        } else if (activeChains > 0) {
            maxChainLength = 2;
        }
        fv.setCascadeMaxChainLength(maxChainLength);
    }

    private double safeD(Double val) { return val != null ? val : 0.0; }
}
