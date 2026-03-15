package com.hazardcast.config;

import com.hazardcast.scoring.MachineLearningScorer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Value;

import java.io.File;

/**
 * Configures the active RiskScorer implementation.
 *
 * When hazardcast.model.path is set and the file exists, the XGBoost
 * MachineLearningScorer is created. Otherwise, Spring falls back to
 * BaseRateScorer via @ConditionalOnMissingBean.
 */
@Configuration
@Slf4j
public class ScoringConfig {

    @Bean
    @ConditionalOnProperty("hazardcast.model.path")
    public MachineLearningScorer machineLearningScorer(
            @Value("${hazardcast.model.path}") String modelPath) {
        File modelFile = new File(modelPath);
        if (!modelFile.exists()) {
            log.warn("Model file not found at {}, falling back to BaseRateScorer", modelPath);
            return null;
        }
        log.info("Activating ML scorer with model: {}", modelPath);
        return new MachineLearningScorer(modelPath);
    }
}
