import os
import shutil
from pathlib import Path
import hashlib
import time

from src.ubs_landing_zone.az_copy import AzCopy
from loguru import logger

class Pipeline:
    def __init__(
        self, 
        az_copy: AzCopy,
        checksum_extension: str, 
        algorithm: str, 
        failed_dir: Path
    ):
        self._az_copy: AzCopy = az_copy
        self._checksum_extension: str = checksum_extension
        self._algorithm: str = algorithm
        self._failed_dir: Path = failed_dir

    def run(self, feed: Path) -> None:
        start_time = time.time()
        
        try:
            self._verify_checksum(feed)
            self._upload(feed)
        except Exception as e:
            logger.debug(f"Error processing feed: {feed.name}, moving to failed directory: {self._failed_dir}.")
            
            self._failed_dir.mkdir(parents=True, exist_ok=True)
            feed.rename(self._failed_dir / feed.name)
            
            md5_file = feed.with_suffix(self._checksum_extension)
            if md5_file.exists():
                md5_file.rename(self._failed_dir / md5_file.name)
            
            raise
        
        processing_time = time.time() - start_time
        logger.info(f"Successfully proceeded feed: {feed.name} in {processing_time:.1f}s, deleting local copy.")
        self._delete_path(feed)
        self._delete_path(feed.with_suffix(self._checksum_extension))

    def _verify_checksum(self, feed: Path) -> None:
        logger.debug(f"Verifying checksum of {feed.name}.")
        expected_checksum_file: Path = feed.with_suffix(self._checksum_extension)
        expected_checksum: str
        try:
            with open(expected_checksum_file, 'r') as f:
                expected_checksum = f.read()
        except Exception as e:
            logger.error(f"Cannot read checksum file: {expected_checksum_file}, error: {e}")
            raise

        h = hashlib.new(self._algorithm)
        with open(feed, 'rb') as f:
            h.update(f.read())
            if h.hexdigest() != expected_checksum:
                msg = f"Checksum did not match, feed: {feed}, expected: {expected_checksum}, calculated:  {h.hexdigest()}"
                logger.error(msg)
                raise ValueError(msg) 
        logger.debug(f"Checksum match for feed: {feed}")

    def _upload(self, feed: Path) -> None:
        self._az_copy.upload(feed)

    @staticmethod
    def _delete_path(path: Path) -> None:
        if os.path.exists(path):
            logger.debug(f"Deleting path {path} ...")

            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.remove(path)

            while os.path.exists(path):
                time.sleep(0.1)
            logger.debug(f"Path {path} deleted.")

        else:
            logger.warning(f"Path {path} does not exist, cannot delete.")
