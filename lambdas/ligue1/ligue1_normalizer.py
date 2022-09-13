import os
import boto3
import pandas as pd
from common_libs.normalizer.base_normalizer import BaseNormalizer


class Ligue1Normalizer(BaseNormalizer):
    def __init__(
        self,
        s3_client,
        raw_bucket,
        normalized_bucket,
        config_path
    ):
        super().__init__(
            s3_client,
            raw_bucket,
            normalized_bucket,
            config_path
        )

    def normalize_data(self, json_dict):
        df = pd.json_normalize(json_dict)

        df.rename(columns=self.config["relevant_columns"], inplace=True)

        df.loc[df["winner"] == "H", "winner"] = df.loc[df["winner"] == "H", "home_team"]
        df.loc[df["winner"] == "A", "winner"] = df.loc[df["winner"] == "A", "away_team"]
        df.loc[df["winner"] == "D", "winner"] = "<draw>"

        df = df.reindex(columns=self.config["final_schema"])

        return df


if not bool(os.environ.get("TEST_FLAG", False)):
    lambda_handler = Ligue1Normalizer(
        boto3.client("s3"),
        os.environ["S3_RAW_BUCKET_NAME"],
        os.environ["S3_NORMALIZED_BUCKET_NAME"],
        os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "config",
            "normalizer_config.yaml",
        )
    )
else:
    lambda_handler = None
