package com.hazardcast.ingestion;

/**
 * Strategy interface for data source ingestion.
 *
 * Each federal data source implements this interface, allowing the pipeline
 * to discover and execute providers without knowing their concrete types.
 * New data sources can be added by implementing this interface and annotating
 * with @Component — no changes to the orchestrator required.
 */
public interface DataProvider {

    /**
     * Unique identifier for this data source (e.g., "fema_disasters", "usgs_earthquakes").
     */
    String getName();

    /**
     * Human-readable description of what this provider ingests.
     */
    String getDescription();

    /**
     * Execute ingestion for the given year range.
     * Implementations must be idempotent — safe to re-run.
     *
     * @param startYear inclusive start year
     * @param endYear   inclusive end year
     * @return result summary with counts and timing
     */
    IngestionResult ingest(int startYear, int endYear);

    /**
     * Priority order for execution (lower = first).
     * FEMA should run first as other providers may reference declarations.
     */
    default int priority() {
        return 100;
    }
}
