import os
import sys
from datetime import datetime

from dotenv import load_dotenv, dotenv_values
from pathlib import Path
from .pipeline import Pipeline
from .az_copy import AzCopy
from .executor import Executor
from loguru import logger

def config_logging(log_level: str) -> None:
    timestamp: str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_filename: str = f"logs/app_{timestamp}.log"
    format: str = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{thread.name}</cyan> | "
        "<level>{message}</level>"
    )
    
    logger.remove()
    logger.add(sys.stderr, format=format, level=log_level)
    logger.add(
        log_filename, 
        level=log_level,
        format=format
    )
    
def main() -> None:
    load_dotenv()
    log_level: str = os.getenv("UBS_LANDING_ZONE_LOG_LEVEL")
    
    config_logging(log_level)
    
    preserve_source_feeds: bool = bool(os.getenv("UBS_LANDING_ZONE_PRESERVE_SOURCE_FEEDS"))
    azcopy_binary: str = os.getenv("UBS_LANDING_ZONE_AZCOPY_BINARY")
    vault_binary: str = os.getenv("UBS_LANDING_ZONE_VAULT_BINARY")
    az_copy_dry_run: bool = bool(os.getenv("UBS_LANDING_ZONE_AZCOPY_DRY_RUN"))
    az_copy_destination_url: str = os.getenv("UBS_LANDING_ZONE_AZCOPY_DESTINATION_URL")
    dir: str = os.getenv("UBS_LANDING_ZONE_DIR")
    dir_processing: str = os.getenv("UBS_LANDING_ZONE_DIR_PROCESSING")
    dir_failed: str = os.getenv("UBS_LANDING_ZONE_DIR_FAILED")
    pattern: str = os.getenv("UBS_LANDING_ZONE_FEED_PATTERN")
    checksum_extension: str = os.getenv("UBS_LANDING_ZONE_CHECKSUM_EXTENSION")
    checksum_algorithm: str = os.getenv("UBS_LANDING_ZONE_CHECKSUM_ALGORITHM")
    parallelism: str = os.getenv("UBS_LANDING_ZONE_PARALLELISM")
    
    logger.debug("== Environment Variables ==")
    logger.debug(f"landing zone log level: {log_level}")
    logger.debug(f"preserve source feeds: {preserve_source_feeds}")
    logger.debug(f"azcopy binary: {azcopy_binary}")
    logger.debug(f"vault binary: {vault_binary}")
    logger.debug(f"preserve source feeds: {preserve_source_feeds}")
    logger.debug(f"azcopy --dry-run: {az_copy_dry_run}")
    logger.debug(f"azcopy destination url: {az_copy_destination_url}")
    logger.debug(f"landing zone directory: {dir}")
    logger.debug(f"processing directory: {dir_processing}")
    logger.debug(f"failed directory: {dir_failed}")
    logger.debug(f"file pattern: {pattern}")
    logger.debug(f"checksum extension: {checksum_extension}")
    logger.debug(f"checksum algorithm: {checksum_algorithm}")
    logger.debug(f"parallelism: {parallelism}")
    
    logger.debug("== Starting UBS Landing Zone processing... ==")
    
    az_copy: AzCopy = AzCopy(
        az_copy_binary=Path(azcopy_binary),
        az_copy_destination_url=az_copy_destination_url, 
        dry_run=az_copy_dry_run
    )
    pipeline: Pipeline = Pipeline(
        az_copy=az_copy,
        checksum_extension=checksum_extension,
        algorithm=checksum_algorithm,
        failed_dir=Path(dir_failed),
        processing_dir=Path(dir_processing)
    )
    executor: Executor = Executor(
        pipeline=pipeline,
        directory=Path(dir),
        file_pattern=pattern,
        parallelism=int(parallelism)
    )
    
    try:
        executor.execute_parallel()
    except ExceptionGroup as eg:    
        msg: str = "\n\t- ".join(str(e) for e in eg.exceptions)
        
        logger.error(f"Execution failed, {len(eg.exceptions)} error(s): \n\t- {msg}")
        sys.exit(msg)
    except Exception as e:
        msg: str = f"Execution failed, unexpected error: {e}"
        logger.error(msg)
        sys.exit(msg)
        
    logger.info("Execution succeed, all feeds processed successfully.")

if __name__ == "__main__":
    main()