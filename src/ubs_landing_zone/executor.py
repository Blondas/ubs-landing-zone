import os
import re

from pathlib import Path
from typing import Any

from .pipeline import Pipeline
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    
    def execute_parallel(self) -> None:
        logger.info("Executor started")
        
        with ThreadPoolExecutor(max_workers=self._parallelism, thread_name_prefix="executor") as executor:
            futures =  [
                executor.submit(self._process, f) 
                for f in self._files()
            ]
            
            exceptions: list[Exception] = []
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    exceptions.append(e)

            if exceptions:
                raise ExceptionGroup("Tasks failed", exceptions)
            

    def _files(self) -> list[Path]:
        list_dir: list[str] = os.listdir(self._directory)
        feeds: list[Path] = [
            Path(self._directory / f) 
            for f in list_dir
            if re.match(self._file_pattern, f.lower())
        ]
        if len(feeds) > len(list_dir) / 2:
            logger.warning(f"UBS_LANDING_ZONE_FEED_PATTERN env var too inclusive, or missing checksum files. Found {len(feeds)} feeds, in {len(list_dir)} all files in directory. ")
        
        if not feeds:
            logger.warning(f"0 mathing feeds in dir:{self._directory} with pattern:'{self._file_pattern}'")
        else:
            logger.debug(f"{len(feeds)} Matching feeds found in dir:{self._directory}, feeds: {list(map(lambda x: x.name, feeds))}")
        return feeds
    
    def _process(self, feed: Path):
        return self._pipeline.run(feed)