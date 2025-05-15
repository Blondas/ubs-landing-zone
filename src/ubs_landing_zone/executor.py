import os
import re

from pathlib import Path
from src.ubs_landing_zone.pipeline import Pipeline
from concurrent.futures import ThreadPoolExecutor
from loguru import logger

class Executor:
    def __init__(
        self, 
        pipeline: Pipeline,
        directory: Path,
        file_pattern: str,
        parallelism: int
    ):
        self._pipeline: Pipeline = pipeline
        self._directory: Path = directory
        self._file_pattern: str = file_pattern
        self._parallelism: int = parallelism
    
    def execute_parallel(self):
        logger.info("Executor started")
        
        with ThreadPoolExecutor(max_workers=self._parallelism, thread_name_prefix="executor") as executor:
            return list(executor.map(self._process, self._files()))

    def _files(self) -> list[Path]:
        feeds: list[Path] = [
            Path(self._directory / f) 
            for f in os.listdir(self._directory) 
            if re.match(self._file_pattern, f.lower())
        ]
        if not feeds:
            logger.warning(f"No mathing feeds in dir:{self._directory} with pattern:{self._file_pattern}")
        else:
            logger.debug(f"{len(feeds)} Matching feeds found in dir:{self._directory}")
        return feeds
    
    def _process(self, feed: Path):
        return self._pipeline.run(feed)