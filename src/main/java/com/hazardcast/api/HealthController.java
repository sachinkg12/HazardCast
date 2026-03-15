package com.hazardcast.api;

import com.hazardcast.repository.CountyFeatureVectorRepository;
import com.hazardcast.repository.CountyRepository;
import com.hazardcast.repository.DisasterDeclarationRepository;
import com.hazardcast.repository.DroughtIndicatorRepository;
import com.hazardcast.repository.NfipClaimRepository;
import com.hazardcast.repository.SeismicEventRepository;
import com.hazardcast.repository.StormEventRepository;
import com.hazardcast.repository.WildfireEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequiredArgsConstructor
public class HealthController {

    private final CountyRepository countyRepository;
    private final DisasterDeclarationRepository declarationRepository;
    private final SeismicEventRepository seismicRepository;
    private final StormEventRepository stormRepository;
    private final DroughtIndicatorRepository droughtRepository;
    private final WildfireEventRepository wildfireRepository;
    private final NfipClaimRepository nfipRepository;
    private final CountyFeatureVectorRepository featureRepository;

    @GetMapping("/")
    public Map<String, Object> index() {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("name", "HazardCast");
        info.put("version", "0.1.0");
        info.put("description", "AI-powered disaster prediction engine for US counties");
        info.put("endpoints", Map.of(
                "predict", "GET /api/predict/{fips}",
                "national", "GET /api/predict/national",
                "pipeline", "POST /api/pipeline/run",
                "ingest", "POST /api/pipeline/ingest",
                "features", "POST /api/pipeline/features?yearMonth=2024-01",
                "export", "POST /api/pipeline/export",
                "health", "GET /actuator/health"
        ));

        Map<String, Long> stats = new LinkedHashMap<>();
        stats.put("counties", countyRepository.count());
        stats.put("disasterDeclarations", declarationRepository.count());
        stats.put("seismicEvents", seismicRepository.count());
        stats.put("stormEvents", stormRepository.count());
        stats.put("droughtIndicators", droughtRepository.count());
        stats.put("wildfireEvents", wildfireRepository.count());
        stats.put("nfipClaims", nfipRepository.count());
        stats.put("featureVectors", featureRepository.countAll());
        info.put("dataStats", stats);

        return info;
    }
}
