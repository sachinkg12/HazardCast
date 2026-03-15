package com.hazardcast.config;

import com.hazardcast.scoring.BaseRateScorer;
import com.hazardcast.scoring.MachineLearningScorer;
import com.hazardcast.scoring.RiskScorer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Value;

import java.io.File;

/**
 * Configures the active RiskScorer implementation.
 *
 * When hazardcast.model.path points to an existing file, the XGBoost
 * MachineLearningScorer is created. Otherwise, falls back to
 * BaseRateScorer (heuristic-based).
 */
@Configuration
@Slf4j
public class ScoringConfig {

    @Bean
    public RiskScorer riskScorer(
            @Value("${hazardcast.model.path:}") String modelPath) {
        if (!modelPath.isEmpty()) {
            File modelFile = new File(modelPath);
            if (modelFile.exists()) {
                log.info("Activating ML scorer with model: {}", modelPath);
                return new MachineLearningScorer(modelPath);
            }
            log.warn("Model file not found at {}, falling back to BaseRateScorer", modelPath);
        }
        log.info("Using BaseRateScorer (heuristic)");
        return new BaseRateScorer();
    }
}
