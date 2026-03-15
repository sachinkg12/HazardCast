package com.hazardcast.pipeline.event;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Listens to pipeline events for logging and metrics.
 *
 * GoF Pattern: Observer (concrete listener)
 * Additional listeners can be added (e.g., Slack notifications,
 * Prometheus metrics) without modifying the pipeline.
 */
@Component
@Slf4j
public class PipelineEventListener {

    @EventListener
    public void onPipelineEvent(PipelineEvent event) {
        switch (event.getType()) {
            case PIPELINE_STARTED -> log.info("Pipeline started: {}", event.getDetail());
            case PIPELINE_COMPLETED -> log.info("Pipeline completed: {}", event.getDetail());
            case INGESTION_STARTED -> log.info("Ingestion started: {}", event.getDetail());
            case INGESTION_COMPLETED -> log.info("Ingestion completed: {}", event.getDetail());
            case FEATURES_STARTED -> log.info("Feature computation started: {}", event.getDetail());
            case FEATURES_COMPLETED -> log.info("Feature computation completed: {}", event.getDetail());
            case EXPORT_STARTED -> log.info("Export started: {}", event.getDetail());
            case EXPORT_COMPLETED -> log.info("Export completed: {}", event.getDetail());
        }
    }
}
