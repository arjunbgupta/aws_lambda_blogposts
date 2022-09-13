import abc
from io import StringIO
import json
import yaml
import logging
from uuid import uuid4
from datetime import datetime, timezone


class BaseNormalizer(metaclass=abc.ABCMeta):
    """A normalizer class"""

    def __init__(
        self,
        s3_client,
        raw_bucket,
        normalized_bucket,
        config_path
    ):
        # Setup logger
        self._logger = logging.getLogger()
        self._logger.setLevel(logging.INFO)

        self.s3_client = s3_client
        self.raw_bucket = raw_bucket
        self.normalized_bucket = normalized_bucket

        # Load config file
        with open(config_path, "r") as stream:
            self.config = yaml.safe_load(stream)

    def read_s3_json(self, bucket, json_file_key):
        """
        Read a Json file stored in s3
        Args:
            bucket (string): the bucket to read from.
            json_file_key (string): key of the file we want to copy.
        """
        obj_indiv = self.s3_client.get_object(Bucket=bucket, Key=json_file_key)
        file_content = obj_indiv["Body"].read().decode("utf-8")
        return json.loads(file_content)

    def save_csv_to_s3(self, df, bucket, file_key):
        """
        Save a pandas.DataFrame to S3 as csv.
        Args:
            df (pandas.DataFrame): a dataframe we want to save to S3.
            bucket (string): The name of the bucket where
            file_key (string): The key of the file that will be saved in the normalized bucket.
        """
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        self.s3_client.put_object(
            Body=csv_buffer.getvalue(), Bucket=bucket, Key=file_key
        )

    def copy_to_raw(self, bucket, file_key, raw_file_key):
        """
        Copy a file from a bucket to raw bucket (used to copy from transient to raw).
        Args:
            bucket (string): the source bucket to copy from (should be the transient).
            file_key (string): a key of the file to copy.
            raw_file_key (string): the key that the file will have in raw.
        """
        self.s3_client.copy_object(
            Bucket=self.raw_bucket, CopySource=bucket + "/" + file_key, Key=raw_file_key
        )

    def delete_file(self, bucket, file_key):
        """
        Copy a file from a bucket to raw bucket (used to copy from transient to raw).
        Args:
            bucket (string): the source bucket to copy from (should be the transient).
            file_key (string): a key of the file to copy.
        """
        self.s3_client.delete_object(Bucket=bucket, Key=file_key)

    @abc.abstractmethod
    def normalize_data(self, json_dict):
        """
        Abstract method that needs to be implemented by any class inheriting from
        BaseNormalizer.
        Args:
            json_dict (dict): The input json file as a python dictionary.
        """
        raise NotImplementedError("Abstract class has not been implemented.")

    def add_metadata(self, df):
        """
        Adds metadata columns to a DataFrame.
        Args:
            df (pd.DataFrame): The DataFrame for which we want to add metadata fields.
        Returns:
            normalized_file_key (str): The key of the file we stored in the normalized bucket.
            raw_file_key (str): The key of the file we stored in the raw bucket.
        """
        datetime_now = datetime.now(timezone.utc)
        uuid_tag = str(uuid4())[:8]

        df["division"] = self.config["division"]
        df["normalization_datetime"] = datetime_now
        df["normalization_uuid"] = uuid_tag

        normalized_file_key = f"{datetime_now.strftime('%Y%m%d')}/{uuid_tag}.csv"
        raw_file_key = f"{datetime_now.strftime('%Y%m%d')}/{uuid_tag}.json"

        return normalized_file_key, raw_file_key

    def __call__(self, event, context):
        transient_bucket = event["Records"][0]["s3"]["bucket"]["name"]
        file_key = event["Records"][0]["s3"]["object"]["key"]
        # Read json files from s3 transient bucket
        json_content = self.read_s3_json(transient_bucket, file_key)

        df = self.normalize_data(json_content)

        # Add meta-data
        normalized_file_key, raw_file_key = self.add_metadata(df)

        # Save to normalized bucket
        self.save_csv_to_s3(df, self.normalized_bucket, normalized_file_key)

        # Copy the transient file to raw
        self.copy_to_raw(transient_bucket, file_key, raw_file_key)

        # Delete file from transient
        self.delete_file(transient_bucket, file_key)

        self._logger.info("Successful normalization.")
