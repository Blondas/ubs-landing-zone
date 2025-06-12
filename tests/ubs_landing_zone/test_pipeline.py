import os
from pathlib import Path
from unittest.mock import Mock, mock_open
import pytest
import tarfile
from src.ubs_landing_zone.az_copy import AzCopy
from src.ubs_landing_zone.pipeline import Pipeline
import hashlib

checksum_algorithm: str = "md5"

@pytest.fixture
def pipeline(
    tmp_path: Path,
    checksum_extension: str = ".md5",
    algorithm: str = checksum_algorithm,
) -> Pipeline:
    return Pipeline(
            az_copy=Mock(),
            checksum_extension=checksum_extension,
            algorithm=algorithm,
            failed_dir=tmp_path / "failed",
            processing_dir=tmp_path / "processing",
            preserve_source_feeds=False
        )

class TestPipeline:
    @pytest.fixture
    def az_copy_mock(self):
        return Mock(AzCopy)

    @pytest.fixture
    def base_dirs(self, tmp_path: Path):
        failed_dir = tmp_path / "failed"
        processing_dir = tmp_path / "processing"
        feeds_dir = tmp_path / "feeds_dir"
        feeds_dir.mkdir(parents=True, exist_ok=True)

        return {
            "failed_dir": failed_dir,
            "processing_dir": processing_dir,
            "feeds_dir": feeds_dir,
        }
    
    @staticmethod
    def _prepare_valid_feed(feed_dir: Path) -> Path:
        sample_xml: Path = feed_dir / "sample.xml"
        sample_xml.write_text('<?xml version="1.0"?><root/>')
        sample_csv: Path = feed_dir / "sample.csv"
        sample_csv.write_text('col1,col2\nval1,val2')
        control_file: Path = feed_dir / "control.control"
        control_file.touch()

        feed_tar: Path = feed_dir / "feed.tar"

        with tarfile.open(feed_tar, 'w') as tar:
            tar.add(sample_xml, arcname=sample_xml.name)
            tar.add(sample_csv, arcname=sample_csv.name)
            tar.add(control_file, arcname=control_file.name)

        h = hashlib.new(checksum_algorithm)
        with open(feed_tar, 'rb') as f:
            h.update(f.read())
        feed_tar.with_suffix(".md5").write_text(h.hexdigest())

        assert  feed_tar.exists() and  feed_tar.with_suffix(".md5").exists() and feed_tar.with_suffix(".md5").stat().st_size > 0, "'_prepare_valid_feed failed' to create feed"

        return feed_tar

    def test_verify_checksum_successful(self, pipeline, monkeypatch):
        feed_path = Path("test_feed.tar")
        checksum_path = feed_path.with_suffix(".md5")
        test_content = b"test content"
        expected_checksum = hashlib.md5(test_content).hexdigest()

        def mock_open_handler(file_path, mode='r', **kwargs):
            if mode == 'r':
                return mock_open(read_data=expected_checksum)()
            else:  # mode == 'rb'
                return mock_open(read_data=test_content)()

        monkeypatch.setattr("builtins.open", mock_open_handler)
        monkeypatch.setattr(Path, "with_suffix", lambda self, suffix: checksum_path)

        pipeline._verify_checksum(feed_path)

    def test_verify_checksum_cannot_read_checksum_file_error(self, pipeline, monkeypatch):
        feed_path = Path("test_feed.tar")
        checksum_path = feed_path.with_suffix(".md5")
        test_content = b"test content"

        def mock_open_handler(file_path, mode='r', **kwargs):
                raise ValueError("FooError")

        monkeypatch.setattr(Path, 'exists', lambda x: True)
        monkeypatch.setattr("builtins.open", mock_open_handler)
        monkeypatch.setattr(Path, "with_suffix", lambda self, suffix: checksum_path)

        with pytest.raises(IOError) as exc_info:
            pipeline._verify_checksum(feed_path)
        assert "Checksum file cannot be open: test_feed.md5" in str(exc_info.value)

    def test_verify_checksum_checksum_not_match_error(self, pipeline, monkeypatch):
        feed_path = Path("test_feed.tar")
        checksum_path = feed_path.with_suffix(".md5")
        test_content = b"test content"
        expected_checksum = "aaaa"

        def mock_open_handler(file_path, mode='r', **kwargs):
            if mode == 'r':
                return mock_open(read_data=expected_checksum)()
            else:  # mode == 'rb'
                return mock_open(read_data=test_content)()

        monkeypatch.setattr(Path, 'exists', lambda x: True)
        monkeypatch.setattr("builtins.open", mock_open_handler)
        monkeypatch.setattr(Path, "with_suffix", lambda self, suffix: checksum_path)

        with pytest.raises(ValueError) as exc_info:
            pipeline._verify_checksum(feed_path)
        assert "Checksum doesn't match" in str(exc_info.value)

    @pytest.mark.parametrize(
        "checksum_extension, algorithm",
        [
            (".md5", "MD5"),
        ]
    )
    def test_unpack_successful(self, az_copy_mock, base_dirs, checksum_extension, algorithm):
        pipeline = Pipeline(
            az_copy=az_copy_mock,
            checksum_extension=checksum_extension,
            algorithm=algorithm,
            failed_dir=base_dirs["failed_dir"],
            processing_dir=base_dirs["processing_dir"],
            preserve_source_feeds=False
        )

        valid_feed_tar: Path = self._prepare_valid_feed(base_dirs["feeds_dir"])

        temp_dir: Path = pipeline._unpack(valid_feed_tar)
        assert temp_dir.exists()
        assert temp_dir.is_dir()
        assert len(list(temp_dir.iterdir())) == 3

    @pytest.mark.parametrize(
        "checksum_extension, algorithm",
        [
            (".md5", "MD5"),
        ]
    )
    def test_unpack_failed(self, az_copy_mock, base_dirs, checksum_extension, algorithm):
        pipeline = Pipeline(
            az_copy=az_copy_mock,
            checksum_extension=checksum_extension,
            algorithm=algorithm,
            failed_dir=base_dirs["failed_dir"],
            processing_dir=base_dirs["processing_dir"],
            preserve_source_feeds=False
        )

        invalid_feed_tar: Path = base_dirs["feeds_dir"] / "invalid_feed.tar"
        invalid_feed_tar.touch()

        with pytest.raises(IOError) as exc_info:
            pipeline._unpack(invalid_feed_tar)

        assert "Corrupted archive (feed), cannot extract" in str(exc_info.value)

    @pytest.mark.parametrize(
        "file_list",
        [
            ["file1.xml", "file2.csv", "control.control"],
            ["control.control", "file2.csv"],
            ["control.control"]
        ]
    )
    def test_order_feed_content_successful(self, pipeline, monkeypatch, file_list):
        monkeypatch.setattr(os, "listdir", lambda path: file_list)
        feed_path = Path("test_feed.tar")
        unpacked_dir = Path("/path/to/unpacked")

        ret: list[Path] = pipeline._order_feed_content(unpacked_dir, feed_path)

        assert len(ret) == len(file_list)
        assert ret[-1].suffix == ".control"

    @pytest.mark.parametrize(
        "file_list",
        [
            ["file1.xml", "file2.csv"],
            []
        ]
    )
    def test_order_feed_content_error(self, pipeline, monkeypatch, file_list):
        monkeypatch.setattr(os, "listdir", lambda path: file_list)
        feed_path = Path("test_feed.tar")
        unpacked_dir = Path("/path/to/unpacked")

        with pytest.raises(ValueError) as exc_info:
            pipeline._order_feed_content(unpacked_dir, feed_path)
        assert "Last file is not a *.control file in" in str(exc_info.value)

    @pytest.mark.parametrize(
        "file_list",
        [
            [],
            [Path("file1"), Path("file2")]
        ]
    )
    def test_upload_successful(self, pipeline, file_list):
        feed_path = Path("test_feed.tar")
        pipeline._az_copy.upload.return_value = None

        pipeline._upload(file_list, feed_path)

        assert pipeline._az_copy.upload.call_count == len(file_list)

    def test_upload_failed(self, pipeline):
        file_list: list[Path] = [Path("file1"), Path("file2")]
        feed_path: Path = Path("test_feed.tar")
        pipeline._az_copy.upload.side_effect = lambda file: exec('raise IOError(f"FOO-ERROR")')

        with pytest.raises(IOError) as exc_info:
            pipeline._upload(file_list, feed_path)
        assert f"Upload failed for {file_list[0]}, in feed: {feed_path}" in str(exc_info.value)

    def test_run_failed_first_step(self, pipeline, monkeypatch):
        feed_path = Path("test_feed.tar")
        
        with pytest.raises(IOError) as exc_info:
            pipeline.run(feed_path)
        assert f"Corrupted archive (feed), cannot extract test_feed.tar to processing dir" in str(exc_info.value)
        
        assert not feed_path.exists() 
        assert not feed_path.with_suffix(pipeline._checksum_extension).exists() 
        assert not pipeline._processing_dir.exists()

    def test_run_failed_last_step(self, az_copy_mock, base_dirs, monkeypatch):
        feed_path: Path = self._prepare_valid_feed(base_dirs["feeds_dir"])
        az_copy_mock.upload.side_effect = lambda file: exec('raise IOError(f"FOO-ERROR")')
        
        pipeline = Pipeline(
            az_copy=az_copy_mock,
            checksum_extension=".md5",
            algorithm=checksum_algorithm,
            failed_dir=base_dirs["failed_dir"],
            processing_dir=base_dirs["processing_dir"],
            preserve_source_feeds=False
        )

        with pytest.raises(IOError) as exc_info:
            pipeline.run(feed_path)
        assert f"Upload failed for" in str(exc_info.value)
        assert not ".control" in str(exc_info.value)

        assert not feed_path.exists()
        assert not feed_path.with_suffix(pipeline._checksum_extension).exists()
        assert not any(pipeline._processing_dir.iterdir())

        
