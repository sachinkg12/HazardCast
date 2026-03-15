-- ============================================================
-- HazardCast Schema
-- Stores raw ingested federal data + engineered feature vectors
-- ============================================================

-- Counties reference table (3,143 US counties)
CREATE TABLE IF NOT EXISTS counties (
    fips        VARCHAR(5)  PRIMARY KEY,
    state_fips  VARCHAR(2)  NOT NULL,
    county_name VARCHAR(100) NOT NULL,
    state_name  VARCHAR(50) NOT NULL,
    state_abbr  VARCHAR(2)  NOT NULL,
    latitude    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION,
    population  BIGINT,
    housing_units BIGINT,
    median_home_value BIGINT,
    land_area_sq_mi DOUBLE PRECISION,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_counties_state ON counties(state_fips);

-- FEMA disaster declarations (raw)
CREATE TABLE IF NOT EXISTS disaster_declarations (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    disaster_number     INTEGER NOT NULL,
    declaration_date    DATE NOT NULL,
    disaster_type       VARCHAR(50),
    incident_type       VARCHAR(100),
    title               VARCHAR(500),
    state               VARCHAR(50),
    fips                VARCHAR(5),
    incident_begin_date DATE,
    incident_end_date   DATE,
    declaration_type    VARCHAR(10),
    ih_program          BOOLEAN DEFAULT FALSE,
    ia_program          BOOLEAN DEFAULT FALSE,
    pa_program          BOOLEAN DEFAULT FALSE,
    hm_program          BOOLEAN DEFAULT FALSE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dd_fips ON disaster_declarations(fips);
CREATE INDEX IF NOT EXISTS idx_dd_date ON disaster_declarations(declaration_date);
CREATE INDEX IF NOT EXISTS idx_dd_type ON disaster_declarations(incident_type);

-- USGS seismic events (raw)
CREATE TABLE IF NOT EXISTS seismic_events (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id    VARCHAR(50) UNIQUE NOT NULL,
    event_time  TIMESTAMP NOT NULL,
    latitude    DOUBLE PRECISION NOT NULL,
    longitude   DOUBLE PRECISION NOT NULL,
    depth_km    DOUBLE PRECISION,
    magnitude   DOUBLE PRECISION NOT NULL,
    mag_type    VARCHAR(10),
    place       VARCHAR(255),
    event_type  VARCHAR(50),
    nearest_fips VARCHAR(5),
    distance_km DOUBLE PRECISION,
    created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_se_fips ON seismic_events(nearest_fips);
CREATE INDEX IF NOT EXISTS idx_se_time ON seismic_events(event_time);
CREATE INDEX IF NOT EXISTS idx_se_mag ON seismic_events(magnitude);

-- NOAA storm events (raw)
CREATE TABLE IF NOT EXISTS storm_events (
    id              BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id        VARCHAR(20) UNIQUE NOT NULL,
    event_type      VARCHAR(100) NOT NULL,
    begin_date      DATE NOT NULL,
    end_date        DATE,
    state           VARCHAR(50),
    state_fips      VARCHAR(2),
    fips            VARCHAR(5),
    injuries_direct INTEGER DEFAULT 0,
    injuries_indirect INTEGER DEFAULT 0,
    deaths_direct   INTEGER DEFAULT 0,
    deaths_indirect INTEGER DEFAULT 0,
    damage_property DOUBLE PRECISION DEFAULT 0,
    damage_crops    DOUBLE PRECISION DEFAULT 0,
    magnitude       DOUBLE PRECISION,
    magnitude_type  VARCHAR(10),
    flood_cause     VARCHAR(100),
    tor_f_scale     VARCHAR(5),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_storm_fips ON storm_events(fips);
CREATE INDEX IF NOT EXISTS idx_storm_date ON storm_events(begin_date);
CREATE INDEX IF NOT EXISTS idx_storm_type ON storm_events(event_type);

-- Engineered feature vectors (one row per county per month)
CREATE TABLE IF NOT EXISTS county_feature_vectors (
    id                          BIGINT AUTO_INCREMENT PRIMARY KEY,
    fips                        VARCHAR(5) NOT NULL,
    year_month                  VARCHAR(7) NOT NULL,  -- "2024-01"

    -- Target variable
    declaration_next_90d        BOOLEAN NOT NULL DEFAULT FALSE,
    declaration_type_next_90d   VARCHAR(100),

    -- FEMA features (rolling windows)
    declarations_1yr            INTEGER DEFAULT 0,
    declarations_3yr            INTEGER DEFAULT 0,
    declarations_5yr            INTEGER DEFAULT 0,
    declarations_10yr           INTEGER DEFAULT 0,
    declarations_total          INTEGER DEFAULT 0,
    months_since_last_decl      INTEGER,
    major_disaster_ratio        DOUBLE PRECISION DEFAULT 0,
    ia_program_ratio            DOUBLE PRECISION DEFAULT 0,

    -- Seismic features (rolling 5yr)
    earthquake_count_1yr        INTEGER DEFAULT 0,
    earthquake_count_5yr        INTEGER DEFAULT 0,
    max_magnitude_1yr           DOUBLE PRECISION DEFAULT 0,
    max_magnitude_5yr           DOUBLE PRECISION DEFAULT 0,
    avg_magnitude_5yr           DOUBLE PRECISION DEFAULT 0,
    total_energy_5yr            DOUBLE PRECISION DEFAULT 0,  -- log10 scale
    earthquake_depth_avg        DOUBLE PRECISION,
    earthquake_distance_avg_km  DOUBLE PRECISION,

    -- Storm features (rolling 5yr)
    storm_event_count_1yr       INTEGER DEFAULT 0,
    storm_event_count_5yr       INTEGER DEFAULT 0,
    storm_deaths_5yr            INTEGER DEFAULT 0,
    storm_injuries_5yr          INTEGER DEFAULT 0,
    storm_property_damage_5yr   DOUBLE PRECISION DEFAULT 0,
    storm_crop_damage_5yr       DOUBLE PRECISION DEFAULT 0,
    tornado_count_5yr           INTEGER DEFAULT 0,
    flood_count_5yr             INTEGER DEFAULT 0,
    hurricane_count_5yr         INTEGER DEFAULT 0,
    hail_count_5yr              INTEGER DEFAULT 0,
    max_tor_f_scale_5yr         INTEGER DEFAULT 0,

    -- Socioeconomic features
    population                  BIGINT,
    housing_units               BIGINT,
    median_home_value           BIGINT,
    population_density          DOUBLE PRECISION,
    land_area_sq_mi             DOUBLE PRECISION,

    -- Temporal features
    month_of_year               INTEGER NOT NULL,
    is_hurricane_season         BOOLEAN DEFAULT FALSE,  -- Jun-Nov
    is_tornado_season           BOOLEAN DEFAULT FALSE,  -- Mar-Jun
    is_wildfire_season          BOOLEAN DEFAULT FALSE,  -- Jun-Oct

    -- Spatial features
    neighbor_avg_declarations_5yr DOUBLE PRECISION DEFAULT 0,
    state_avg_declarations_5yr   DOUBLE PRECISION DEFAULT 0,

    computed_at                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(fips, year_month)
);

CREATE INDEX IF NOT EXISTS idx_cfv_fips ON county_feature_vectors(fips);
CREATE INDEX IF NOT EXISTS idx_cfv_month ON county_feature_vectors(year_month);
CREATE INDEX IF NOT EXISTS idx_cfv_target ON county_feature_vectors(declaration_next_90d);
