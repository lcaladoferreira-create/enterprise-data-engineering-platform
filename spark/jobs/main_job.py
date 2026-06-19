import sys
import yaml
from pyspark.sql import SparkSession
from spark.transformations.bronze_layer import process_bronze_layer
from spark.transformations.silver_layer import process_silver_layer
from spark.transformations.gold_layer import create_gold_star_schema
from spark.utils.logger import get_logger

logger = get_logger(__name__)


def load_entity_config():
    """Load entity mappings from external YAML."""
    try:
        with open("config/entities.yml", "r") as f:
            return yaml.safe_load(f).get("entities", {})
    except Exception as e:
        logger.error(f"Failed to load entity config: {str(e)}")
        return {}


def main():
    """
    Enterprise Data Pipeline Orchestrator.
    Handles the execution flow for Bronze, Silver, and Gold layers.
    """
    if len(sys.argv) < 2:
        logger.error("Usage: main_job.py <layer> [entity]")
        sys.exit(1)

    layer = sys.argv[1]
    entity_configs = load_entity_config()

    spark = SparkSession.builder \
        .appName(f"Enterprise_Data_Pipeline_{layer}") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()

    try:
        if layer == "bronze":
            if len(sys.argv) < 3:
                logger.error("Bronze layer requires an entity argument.")
                sys.exit(1)
            entity = sys.argv[2]
            process_bronze_layer(spark, entity)

        elif layer == "silver":
            if len(sys.argv) < 3:
                logger.error("Silver layer requires an entity argument.")
                sys.exit(1)
            entity = sys.argv[2]

            cfg = entity_configs.get(entity, {"pk": "id"})
            process_silver_layer(
                spark,
                entity,
                cfg.get("pk", "id"),
                cfg.get("pii"),
                cfg.get("critical")
            )

        elif layer == "gold":
            create_gold_star_schema(spark)

        else:
            logger.error(f"Unsupported layer: {layer}")
            sys.exit(1)

        logger.info(f"Successfully completed processing for layer: {layer}")

    except Exception as e:
        logger.error(f"Pipeline failure in layer {layer}: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
