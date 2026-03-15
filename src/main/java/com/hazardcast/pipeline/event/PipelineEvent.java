package com.hazardcast.pipeline.event;

import lombok.Getter;
import org.springframework.context.ApplicationEvent;

/**
 * Domain events emitted during pipeline execution.
 *
 * GoF Pattern: Observer (via Spring's ApplicationEvent)
 * Decouples pipeline execution from cross-cutting concerns
 * (logging, metrics, notifications) without modifying pipeline code.
 */
@Getter
public class PipelineEvent extends ApplicationEvent {

    public enum Type {
        INGESTION_STARTED, INGESTION_COMPLETED,
        FEATURES_STARTED, FEATURES_COMPLETED,
        EXPORT_STARTED, EXPORT_COMPLETED,
        PIPELINE_STARTED, PIPELINE_COMPLETED
    }

    private final Type type;
    private final String detail;

    public PipelineEvent(Object source, Type type, String detail) {
        super(source);
        this.type = type;
        this.detail = detail;
    }
}
