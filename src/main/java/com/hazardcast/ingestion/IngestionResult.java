package com.hazardcast.ingestion;

import lombok.Data;

import java.time.Instant;

@Data
public class IngestionResult {
    private final String source;
    private int recordsInserted;
    private int recordsSkipped;
    private String error;
    private final Instant startedAt = Instant.now();
    private Instant completedAt;

    public IngestionResult(String source) {
        this.source = source;
    }

    public void complete() {
        this.completedAt = Instant.now();
    }

    public long durationMs() {
        Instant end = completedAt != null ? completedAt : Instant.now();
        return end.toEpochMilli() - startedAt.toEpochMilli();
    }
}
