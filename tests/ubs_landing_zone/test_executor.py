import os

import pytest

from src.ubs_landing_zone.executor import Executor
from unittest.mock import Mock
from src.ubs_landing_zone.pipeline import Pipeline
from pathlib import Path

class TestExecutor:
    def test_executor(self, monkeypatch):
        pipeline_mock: Pipeline = Mock(Pipeline)
        pipeline_mock.run = lambda feed: None

        monkeypatch.setattr(
            os,
            'listdir',
            lambda feed: [f"feed{i}.tar" for i in range(1, 6)]
        )

        directory: Path = Path("test_dir")

        executor = Executor(
            pipeline=pipeline_mock,
            directory=directory,
            file_pattern=".*",
            parallelism=3
        )

        executor.execute_parallel()

    def test_executor_failed(self, monkeypatch):
        pipeline_mock: Pipeline = Mock(Pipeline)
        def mock_run(feed):
            if feed.name == "feed1.tar" or feed.name == "feed5.tar":
                raise IOError("Test error feed1")
            return None
        pipeline_mock.run = mock_run

        monkeypatch.setattr(
            os,
            'listdir',
            lambda feed: [f"feed{i}.tar" for i in range(1, 6)]
        )

        directory: Path = Path("test_dir")

        executor = Executor(
            pipeline=pipeline_mock,
            directory=directory,
            file_pattern=".*",
            parallelism=3
        )
        
        with pytest.raises(ExceptionGroup) as exc_info:
            executor.execute_parallel()
            
        assert "Tasks failed (2 sub-exceptions)" in str(exc_info.value)
        assert len(exc_info.value.exceptions) == 2
