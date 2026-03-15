package com.hazardcast.pipeline;

import com.hazardcast.config.ApiProperties;
import com.hazardcast.model.CountyFeatureVector;
import com.hazardcast.repository.CountyFeatureVectorRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Exports feature vectors to Parquet format for ML training and dataset publishing.
 * Output is directly usable by pandas, scikit-learn, PyTorch, and XGBoost.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class ParquetExporter {

    private final CountyFeatureVectorRepository featureRepository;
    private final ApiProperties properties;

    private static final Schema SCHEMA = buildSchema();

    public File export() throws IOException {
        String outputDir = properties.getPipeline().getParquetOutputDir();
        new File(outputDir).mkdirs();

        String filename = outputDir + "/us-county-hazard-features.parquet";
        File outputFile = new File(filename);

        log.info("Exporting feature vectors to {}", filename);

        long count = featureRepository.countAll();
        log.info("Total feature vectors to export: {}", count);

        Configuration hadoopConf = new Configuration();
        Path path = new Path(outputFile.getAbsolutePath());

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(path)
                .withSchema(SCHEMA)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withConf(hadoopConf)
                .build()) {

            // Stream in batches to avoid OOM
            int page = 0;
            int pageSize = 10_000;
            long exported = 0;

            while (true) {
                var batch = featureRepository.findAll(
                        org.springframework.data.domain.PageRequest.of(page, pageSize));

                if (batch.isEmpty()) break;

                for (CountyFeatureVector fv : batch) {
                    writer.write(toRecord(fv));
                    exported++;
                }

                page++;
                if (exported % 100_000 == 0) {
                    log.info("Exported {}/{} records", exported, count);
                }

                if (!batch.hasNext()) break;
            }

            log.info("Export complete: {} records to {}", exported, filename);
        }

        return outputFile;
    }

    private GenericRecord toRecord(CountyFeatureVector fv) {
        GenericRecord record = new GenericData.Record(SCHEMA);

        record.put("fips", fv.getFips());
        record.put("year_month", fv.getYearMonth());
        record.put("declaration_next_90d", fv.getDeclarationNext90d());
        record.put("declaration_type_next_90d", fv.getDeclarationTypeNext90d());

        // FEMA
        record.put("declarations_1yr", orZero(fv.getDeclarations1yr()));
        record.put("declarations_3yr", orZero(fv.getDeclarations3yr()));
        record.put("declarations_5yr", orZero(fv.getDeclarations5yr()));
        record.put("declarations_10yr", orZero(fv.getDeclarations10yr()));
        record.put("months_since_last_decl", orNeg(fv.getMonthsSinceLastDecl()));
        record.put("major_disaster_ratio", orZeroD(fv.getMajorDisasterRatio()));
        record.put("ia_program_ratio", orZeroD(fv.getIaProgramRatio()));

        // Storm
        record.put("storm_event_count_1yr", orZero(fv.getStormEventCount1yr()));
        record.put("storm_event_count_5yr", orZero(fv.getStormEventCount5yr()));
        record.put("storm_deaths_5yr", orZero(fv.getStormDeaths5yr()));
        record.put("storm_injuries_5yr", orZero(fv.getStormInjuries5yr()));
        record.put("storm_property_damage_5yr", orZeroD(fv.getStormPropertyDamage5yr()));
        record.put("storm_crop_damage_5yr", orZeroD(fv.getStormCropDamage5yr()));
        record.put("tornado_count_5yr", orZero(fv.getTornadoCount5yr()));
        record.put("flood_count_5yr", orZero(fv.getFloodCount5yr()));
        record.put("hail_count_5yr", orZero(fv.getHailCount5yr()));
        record.put("max_tor_f_scale_5yr", orZero(fv.getMaxTorFScale5yr()));

        // Socioeconomic
        record.put("population", fv.getPopulation() != null ? fv.getPopulation() : 0L);
        record.put("housing_units", fv.getHousingUnits() != null ? fv.getHousingUnits() : 0L);
        record.put("median_home_value", fv.getMedianHomeValue() != null ? fv.getMedianHomeValue() : 0L);
        record.put("population_density", orZeroD(fv.getPopulationDensity()));
        record.put("land_area_sq_mi", orZeroD(fv.getLandAreaSqMi()));

        // Temporal
        record.put("month_of_year", fv.getMonthOfYear());
        record.put("is_hurricane_season", fv.getIsHurricaneSeason());
        record.put("is_tornado_season", fv.getIsTornadoSeason());
        record.put("is_wildfire_season", fv.getIsWildfireSeason());

        // Drought
        record.put("drought_severity_avg_5yr", orZeroD(fv.getDroughtSeverityAvg5yr()));
        record.put("drought_max_severity_5yr", orZeroD(fv.getDroughtMaxSeverity5yr()));
        record.put("severe_drought_weeks_5yr", orZero(fv.getSevereDroughtWeeks5yr()));
        record.put("drought_d4_pct_max_5yr", orZeroD(fv.getDroughtD4PctMax5yr()));

        // Wildfire
        record.put("wildfire_count_1yr", orZero(fv.getWildfireCount1yr()));
        record.put("wildfire_count_5yr", orZero(fv.getWildfireCount5yr()));
        record.put("wildfire_acres_burned_5yr", orZeroD(fv.getWildfireAcresBurned5yr()));
        record.put("wildfire_max_acres_5yr", orZeroD(fv.getWildfireMaxAcres5yr()));

        // NFIP
        record.put("nfip_claim_count_5yr", orZero(fv.getNfipClaimCount5yr()));
        record.put("nfip_total_payout_5yr", orZeroD(fv.getNfipTotalPayout5yr()));
        record.put("nfip_avg_payout_5yr", orZeroD(fv.getNfipAvgPayout5yr()));

        // Spatial
        record.put("neighbor_avg_declarations_5yr", orZeroD(fv.getNeighborAvgDeclarations5yr()));
        record.put("state_avg_declarations_5yr", orZeroD(fv.getStateAvgDeclarations5yr()));

        // Cascade
        record.put("cascade_drought_fire_risk", orZeroD(fv.getCascadeDroughtFireRisk()));
        record.put("cascade_fire_flood_risk", orZeroD(fv.getCascadeFireFloodRisk()));
        record.put("cascade_hurricane_flood_risk", orZeroD(fv.getCascadeHurricaneFloodRisk()));
        record.put("cascade_earthquake_landslide_risk", orZeroD(fv.getCascadeEarthquakeLandslideRisk()));
        record.put("cascade_storm_compound_count", orZero(fv.getCascadeStormCompoundCount()));
        record.put("cascade_active_chains", orZero(fv.getCascadeActiveChains()));
        record.put("cascade_max_chain_length", orZero(fv.getCascadeMaxChainLength()));

        return record;
    }

    private static Schema buildSchema() {
        return SchemaBuilder.record("CountyHazardFeatures")
                .namespace("com.hazardcast")
                .fields()
                .requiredString("fips")
                .requiredString("year_month")
                .requiredBoolean("declaration_next_90d")
                .optionalString("declaration_type_next_90d")
                // FEMA
                .requiredInt("declarations_1yr")
                .requiredInt("declarations_3yr")
                .requiredInt("declarations_5yr")
                .requiredInt("declarations_10yr")
                .requiredInt("months_since_last_decl")
                .requiredDouble("major_disaster_ratio")
                .requiredDouble("ia_program_ratio")
                // Storm
                .requiredInt("storm_event_count_1yr")
                .requiredInt("storm_event_count_5yr")
                .requiredInt("storm_deaths_5yr")
                .requiredInt("storm_injuries_5yr")
                .requiredDouble("storm_property_damage_5yr")
                .requiredDouble("storm_crop_damage_5yr")
                .requiredInt("tornado_count_5yr")
                .requiredInt("flood_count_5yr")
                .requiredInt("hail_count_5yr")
                .requiredInt("max_tor_f_scale_5yr")
                // Socioeconomic
                .requiredLong("population")
                .requiredLong("housing_units")
                .requiredLong("median_home_value")
                .requiredDouble("population_density")
                .requiredDouble("land_area_sq_mi")
                // Temporal
                .requiredInt("month_of_year")
                .requiredBoolean("is_hurricane_season")
                .requiredBoolean("is_tornado_season")
                .requiredBoolean("is_wildfire_season")
                // Drought
                .requiredDouble("drought_severity_avg_5yr")
                .requiredDouble("drought_max_severity_5yr")
                .requiredInt("severe_drought_weeks_5yr")
                .requiredDouble("drought_d4_pct_max_5yr")
                // Wildfire
                .requiredInt("wildfire_count_1yr")
                .requiredInt("wildfire_count_5yr")
                .requiredDouble("wildfire_acres_burned_5yr")
                .requiredDouble("wildfire_max_acres_5yr")
                // NFIP
                .requiredInt("nfip_claim_count_5yr")
                .requiredDouble("nfip_total_payout_5yr")
                .requiredDouble("nfip_avg_payout_5yr")
                // Spatial
                .requiredDouble("neighbor_avg_declarations_5yr")
                .requiredDouble("state_avg_declarations_5yr")
                // Cascade
                .requiredDouble("cascade_drought_fire_risk")
                .requiredDouble("cascade_fire_flood_risk")
                .requiredDouble("cascade_hurricane_flood_risk")
                .requiredDouble("cascade_earthquake_landslide_risk")
                .requiredInt("cascade_storm_compound_count")
                .requiredInt("cascade_active_chains")
                .requiredInt("cascade_max_chain_length")
                .endRecord();
    }

    private int orZero(Integer val) { return val != null ? val : 0; }
    private int orNeg(Integer val) { return val != null ? val : -1; }
    private double orZeroD(Double val) { return val != null ? val : 0.0; }
}
