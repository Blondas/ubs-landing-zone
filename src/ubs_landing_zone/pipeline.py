import os
import shutil
import tarfile
from pathlib import Path
import hashlib
import time
import tempfile

from .az_copy import AzCopy
from loguru import logger

class Pipeline:
    def __init__(
        self, 
        az_copy: AzCopy,
        checksum_extension: str, 
        algorithm: str, 
        failed_dir: Path,
        processing_dir: Path
    ):
        self._az_copy: AzCopy = az_copy
        self._checksum_extension: str = checksum_extension
        self._algorithm: str = algorithm
        self._failed_dir: Path = failed_dir
        self._processing_dir: Path = processing_dir

    def run(self, feed: Path) -> None:
        start_time = time.time()
        unpacked_dir: Path = None
        
        try:
            logger.debug(f"Processing feed: {feed.name}")
            self._verify_checksum(feed)
            unpacked_dir = self._unpack(feed)
            self._verify_feed_content(unpacked_dir, feed)
            ordered_feed_content: list[Path] = self._order_feed_content(unpacked_dir, feed)
            self._upload(ordered_feed_content, feed)
                
        except Exception as e:
            logger.debug(f"Moving feed and checksum to failed directory, feed: {feed.name}, failed_dir: {self._failed_dir}")
            
            if feed and feed.exists():    
                self._failed_dir.mkdir(parents=True, exist_ok=True)
                feed.rename(self._failed_dir / feed.name)
            
            md5_file = feed.with_suffix(self._checksum_extension)
            if md5_file and md5_file.exists():
                md5_file.rename(self._failed_dir / md5_file.name)
                
            if unpacked_dir and unpacked_dir.exists():
                self._delete_path(unpacked_dir)
            raise
        
        processing_time = time.time() - start_time
        logger.info(f"Successfully proceeded feed: {feed.name} in {processing_time:.1f}s, deleting local copy")
        
        try:
            logger.debug(f"Deleting started...")
            self._delete_path(feed)
            self._delete_path(feed.with_suffix(self._checksum_extension))
            self._delete_path(unpacked_dir)
        except Exception as e:
            msg: str = f"Error deleting local files for feed: {feed.name}"
            logger.error(f"{msg}, error: {e}")
            raise IOError(msg) from e

    def _verify_checksum(self, feed: Path) -> None:
        logger.debug(f"Verifying checksum, feed: {feed.name}")
        expected_checksum_file: Path = feed.with_suffix(self._checksum_extension)
        expected_checksum: str
        try:
            with open(expected_checksum_file, 'r') as f:
                expected_checksum = f.read().strip()
        except Exception as e:
            msg: str =  f"Checksum file missing: {expected_checksum_file}"
            logger.error(f"{msg}, error: {e}")
            raise IOError(msg) from e

        h = hashlib.new(self._algorithm)
        with open(feed, 'rb') as f:
            h.update(f.read())
            if h.hexdigest() != expected_checksum:
                msg = f"Checksum doesn't match, feed: {feed}, expected: '{expected_checksum}', calculated: '{h.hexdigest().strip()}'"
                logger.error(msg)
                raise ValueError(msg) 
            
        logger.debug(f"Checksum match for feed: {feed}")
        
    def _unpack(self, feed: Path) -> Path:
        logger.debug(f"Unpacking feed: {feed.name}")
        try: 
            with tarfile.open(feed, "r") as tar:
                self._processing_dir.mkdir(parents=True, exist_ok=True)
                temp_dir = tempfile.mkdtemp(dir=self._processing_dir)
                logger.debug(f"Extracting {feed.name} to {temp_dir}")
                tar.extractall(temp_dir, filter='data')

                return Path(temp_dir)
        except Exception as e:
            msg: str = f"Corrupted archive (feed), cannot extract {feed.name} to processing dir: {self._processing_dir}"
            logger.error(f"{msg}, error: {e}")
            raise IOError(msg) from e
    
    def _verify_feed_content(self, unpacked_dir: Path, feed: Path) -> None:
        control_file_ext: str = ".control"
        logger.debug(f"Verifying feed content, feed: {feed}, unpacked in: {unpacked_dir}")
        logger.debug(f"DUPA 0")
        
        matching_file: str = next((f for f in os.listdir(unpacked_dir) if f.endswith(control_file_ext)), None)

        if matching_file:
            logger.debug(f"Control file '{matching_file}' found if feed: {feed}, unpacked to: {unpacked_dir}.")
            return
        
        msg: str = f"No control file: '{control_file_ext}' in {feed}"
        logger.error(msg)
        raise ValueError(msg)
    
    def _order_feed_content(self, unpacked_dir: Path, feed: Path) -> list[Path]:
        file_list: list[str] = os.listdir(unpacked_dir)
        for i, e in enumerate(file_list):
            if e.endswith(".control"):
                tmp = file_list[-1]
                file_list[-1] = file_list[i]
                file_list[i] = tmp
                break
        
        if not file_list or not file_list[-1].endswith(".control"):
            msg: str = f"Last file is not a *.control file in {unpacked_dir} for feed {feed}."
            logger.error(msg)
            raise ValueError(msg)
        return [Path(f) for f in file_list]

    def _upload(self, ordered_feed_content: list[Path], feed: Path) -> None:
        err_list: list[(Path, str)] = []
        for file in ordered_feed_content:
            try:
                self._az_copy.upload(file)
                logger.debug(f"Upload succeeded for {file} from feed {feed.name}")
            except Exception as e:
                msg: str = f"Upload failed for {file}, in feed: {feed}"
                logger.error(f"{msg}, error: {e}")
                raise IOError(msg) from e

    @staticmethod
    def _delete_path(path: Path) -> None:
        if path and path.exists():
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
