import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict

import great_expectations as ge
from loguru import logger
from pyspark.sql import DataFrame


class DataQualityException(Exception):
    """Custom exception for data quality failures."""

    pass


class DataQualityCheckpoint:
    """
    Reusable Data Quality Checkpoint using Great Expectations.
    """

    def __init__(self, suite_name: str):
        self.suite_name = suite_name
        self.suite_path = f"data_quality/expectations/{suite_name}.json"
        self._load_suite()

    def _load_suite(self) -> None:
        if not os.path.exists(self.suite_path):
            raise FileNotFoundError(f"Expectation suite not found at {self.suite_path}")

        with open(self.suite_path, "r") as f:
            self.expectation_suite = json.load(f)
        logger.info(f"Loaded expectation suite: {self.suite_name}")

    def validate(self, df: DataFrame, evaluation_parameters: Dict[str, Any] = None) -> None:
        """
        Validates a Spark DataFrame against the loaded expectation suite.
        """
        logger.info(f"Starting DQ validation for suite: {self.suite_name}")

        # Convert Spark DF to GE Spark Dataset
        ge_df = ge.dataset.SparkDFDataset(df)

        # Set default evaluation parameters if not provided (e.g., for freshness check)
        if not evaluation_parameters:
            now = datetime.now()
            yesterday = now - timedelta(hours=24)
            evaluation_parameters = {"now_minus_24h": yesterday.strftime("%Y-%m-%d %H:%M:%S")}

        # Run validation
        results = ge_df.validate(expectation_suite=self.expectation_suite, evaluation_parameters=evaluation_parameters)

        if not results["success"]:
            failed_expectations = [
                res["expectation_config"]["expectation_type"] for res in results["results"] if not res["success"]
            ]
            error_msg = f"Data Quality validation failed for {self.suite_name}. Failed: {failed_expectations}"
            logger.error(error_msg)
            # Log detailed results for debugging
            logger.debug(json.dumps(results.to_json_dict(), indent=2))
            raise DataQualityException(error_msg)

        logger.info(f"Data Quality validation passed for {self.suite_name}")
