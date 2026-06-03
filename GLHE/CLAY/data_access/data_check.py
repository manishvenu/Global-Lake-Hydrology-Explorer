import json
import logging
from pathlib import Path

import boto3
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

_LOCAL_DATA_DIR = Path(__file__).parents[1] / "LocalData"
_BUCKET = "glhe"


def check_data_and_download_missing_data_or_files() -> None:
    """Verify all required data is accessible — locally cached or on S3."""
    _LOCAL_DATA_DIR.mkdir(exist_ok=True)

    for product in _load_products():
        name = product["name"]
        code = product["access_code"]

        if code == "local":
            local_path = _LOCAL_DATA_DIR / product["local_path"]
            if local_path.exists():
                logger.info(f"Found {name} locally")
                continue
            cloud_path = product["cloud"]
            if s3_object_or_folder_exists(_BUCKET, cloud_path):
                logger.info(f"Found {name} in S3")
            else:
                raise RuntimeError(
                    f"{name} not found locally or in S3 (s3://{_BUCKET}/{cloud_path})"
                )

        elif code == "api":
            script = product["api_access_script"]
            script_path = Path(__file__).parent / (script + ".py")
            if script_path.exists():
                logger.info(f"Found {name} api access script")
            else:
                raise RuntimeError(
                    f"Missing api access script for {name}: {script_path}"
                )

    logger.info("Finished checking and/or downloading required data & files")


def download_for_offline() -> None:
    """Download all S3 data products to LocalData so CLAY can run without internet.

    Call this once before going offline:
        from GLHE.CLAY.data_access.data_check import download_for_offline
        download_for_offline()
    """
    _LOCAL_DATA_DIR.mkdir(exist_ok=True)

    for product in _load_products():
        if product["access_code"] != "local":
            continue
        local_path = _LOCAL_DATA_DIR / product["local_path"]
        if local_path.exists():
            logger.info(f"{product['name']} already cached locally")
            continue
        cloud_path = product["cloud"]
        logger.info(f"Downloading {product['name']} from s3://{_BUCKET}/{cloud_path} ...")
        _download_from_s3(_BUCKET, cloud_path, local_path)
        logger.info(f"Downloaded {product['name']}")


def _load_products() -> list:
    config_path = Path(__file__).parent / "data_access_config" / "input_data.json"
    with open(config_path) as f:
        return json.load(f)["data_products"]


def _download_from_s3(bucket: str, s3_path: str, local_path: Path) -> None:
    """Download a file or directory (zarr store, shapefile folder) from S3."""
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    prefix = s3_path.rstrip("/") + "/"

    objects = [
        obj
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=prefix
        )
        for obj in page.get("Contents", [])
    ]

    if not objects:
        # Single file
        local_path.parent.mkdir(parents=True, exist_ok=True)
        s3.download_file(bucket, s3_path, str(local_path))
        return

    logger.info(f"  {len(objects)} files to download")
    for obj in objects:
        key = obj["Key"]
        rel = key[len(prefix):]
        if not rel:
            continue
        dest = local_path / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        s3.download_file(bucket, key, str(dest))


def s3_object_or_folder_exists(bucket_name: str, s3_path: str) -> bool:
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    try:
        s3.head_object(Bucket=bucket_name, Key=s3_path)
        return True
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code not in ("404", "403", "NoSuchKey"):
            raise

    if s3_path.endswith(".zarr"):
        try:
            s3.head_object(Bucket=bucket_name, Key=s3_path + "/.zmetadata")
            return True
        except ClientError:
            pass

    try:
        result = s3.list_objects_v2(
            Bucket=bucket_name, Prefix=s3_path, Delimiter="/"
        )
        return "Contents" in result or "CommonPrefixes" in result
    except ClientError as e:
        if e.response["Error"]["Code"] == "AccessDenied":
            logger.warning(
                "s3:ListBucket denied for %s/%s — assuming it exists",
                bucket_name,
                s3_path,
            )
            return True
        raise
