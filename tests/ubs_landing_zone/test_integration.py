import os
from pathlib import Path
import pytest
from src.ubs_landing_zone.__main__ import main
import shutil

class TestIntegration:
    resource_dir: Path = Path(os.path.abspath(__file__)).parent.parent / "resources"

    @pytest.fixture
    def base_dirs(self, tmp_path: Path):
        landing_zone = tmp_path / "landing_zone"
        processing = tmp_path / "processing"
        failed = tmp_path / "failed"
        
        landing_zone.mkdir(parents=True, exist_ok=True)

        return {
            "landing_zone": landing_zone,
            "processing": processing,
            "failed": failed,
        }
    
    @pytest.fixture
    def env_vars(self, monkeypatch):
        monkeypatch.setenv("UBS_LANDING_ZONE_LOG_LEVEL", "INFO")
        monkeypatch.setenv("UBS_LANDING_ZONE_FEED_PATTERN", r".+\.tar")
        monkeypatch.setenv("UBS_LANDING_ZONE_CHECKSUM_EXTENSION", ".md5")
        monkeypatch.setenv("UBS_LANDING_ZONE_CHECKSUM_ALGORITHM", "MD5")
        monkeypatch.setenv("UBS_LANDING_ZONE_PARALLELISM", "1")
        
    def test_ok_10_feeds(self, env_vars, base_dirs, monkeypatch):
        shutil.copytree(
            self.resource_dir / "OK_10_feeds", 
            base_dirs["landing_zone"],
            dirs_exist_ok=True
        )

        monkeypatch.setenv("UBS_LANDING_ZONE_DIR", str(base_dirs["landing_zone"]))
        monkeypatch.setenv("UBS_LANDING_ZONE_DIR_PROCESSING", str(base_dirs["processing"]))
        monkeypatch.setenv("UBS_LANDING_ZONE_DIR_FAILED", str(base_dirs["failed"]))
        
        main()

    def test_ok_100_feeds(self, env_vars, base_dirs, monkeypatch):
        shutil.copytree(
            self.resource_dir / "OK_100_feeds",
            base_dirs["landing_zone"],
            dirs_exist_ok=True
        )

        monkeypatch.setenv("UBS_LANDING_ZONE_DIR", str(base_dirs["landing_zone"]))
        monkeypatch.setenv("UBS_LANDING_ZONE_DIR_PROCESSING", str(base_dirs["processing"]))
        monkeypatch.setenv("UBS_LANDING_ZONE_DIR_FAILED", str(base_dirs["failed"]))

        main()

    def test_ok_3_feeds_not_ok_3_feeds(self, env_vars, base_dirs, monkeypatch):
        shutil.copytree(
            self.resource_dir / "OK_3_feeds_NOT_OK_3_feeds",
            base_dirs["landing_zone"],
            dirs_exist_ok=True
        )

        monkeypatch.setenv("UBS_LANDING_ZONE_DIR", str(base_dirs["landing_zone"]))
        monkeypatch.setenv("UBS_LANDING_ZONE_DIR_PROCESSING", str(base_dirs["processing"]))
        monkeypatch.setenv("UBS_LANDING_ZONE_DIR_FAILED", str(base_dirs["failed"]))

        with pytest.raises(SystemExit) as exc_info:
            main()
        assert "No control file" in exc_info.value.code
        assert "Corrupted archive" in exc_info.value.code
        assert "Checksum file missing" in exc_info.value.code
        
    def test_not_ok_3_missing_checksum_corrupted_tar_missing_control(self, env_vars, base_dirs, monkeypatch):
        shutil.copytree(
            self.resource_dir / "NOT_OK_missing_checksum_corrupted_tar_missing_control",
            base_dirs["landing_zone"],
            dirs_exist_ok=True
        )

        monkeypatch.setenv("UBS_LANDING_ZONE_DIR", str(base_dirs["landing_zone"]))
        monkeypatch.setenv("UBS_LANDING_ZONE_DIR_PROCESSING", str(base_dirs["processing"]))
        monkeypatch.setenv("UBS_LANDING_ZONE_DIR_FAILED", str(base_dirs["failed"]))

        with pytest.raises(SystemExit) as exc_info:
            main()
        assert "No control file" in exc_info.value.code
        assert "Corrupted archive" in exc_info.value.code
        assert "Checksum file missing" in exc_info.value.code