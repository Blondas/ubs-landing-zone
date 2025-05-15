import os
import sys
from datetime import datetime

from dotenv import load_dotenv, dotenv_values
from pathlib import Path
from pipeline import Pipeline
from src.ubs_landing_zone.az_copy import AzCopy
from src.ubs_landing_zone.executor import Executor
from loguru import logger
import json

def config_logging() -> None:
    level: str = "INFO"
    timestamp: str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_filename: str = f"logs/app_{timestamp}.log"
    format: str = (
        "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>{thread.name}</cyan> | "
        "<level>{message}</level>"
    )
    
    logger.remove()
    logger.add(sys.stderr, format=format, level=level)
    logger.add(
        log_filename, 
        level=level,
        format=format
    )
    
def main():
    config_logging()
    
    load_dotenv()
    logger.debug(f"Environment variables: \n{json.dumps(dotenv_values(), indent=4)}")
    
    dir: str = os.getenv("UBS_LANDING_ZONE_DIR")
    dir_failed: str = os.getenv("UBS_LANDING_ZONE_DIR_FAILED")
    pattern: str = os.getenv("UBS_LANDING_ZONE_FEED_PATTERN")
    checksum_extension: str = os.getenv("UBS_LANDING_ZONE_CHECKSUM_EXTENSION")
    checksum_algorithm: str = os.getenv("UBS_LANDING_ZONE_CHECKSUM_ALGORITHM")
    parallelism: str = os.getenv("UBS_LANDING_ZONE_PARALLELISM")
    
    
    
    az_copy: AzCopy = AzCopy()
    pipeline: Pipeline = Pipeline(
        az_copy=az_copy,
        checksum_extension=checksum_extension,
        algorithm=checksum_algorithm,
        failed_dir=Path(dir)
    )
    executor: Executor = Executor(
        pipeline=pipeline,
        directory=Path(dir),
        file_pattern=pattern,
        parallelism=int(parallelism)
    )
    
    try:
        executor.execute_parallel()
    except Exception:    
        logger.error(f"An error occurred during execution, at least one feed failed")
        sys.exit(1)
    logger.info("All feeds processed successfully.")

if __name__ == "__main__":
    main()