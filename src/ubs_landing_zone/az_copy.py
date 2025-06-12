from pathlib import Path
import subprocess
from subprocess import CalledProcessError
import json

from loguru import logger

class AzCopy:
    def __init__(
        self,
        az_copy_binary: Path,
        az_copy_destination_url: str, 
        dry_run: bool = False
    ):
        self._az_copy_binary = az_copy_binary
        self._az_copy_destination_url = az_copy_destination_url
        self._dry_run = dry_run
        
        if not az_copy_binary.exists():
            raise FileNotFoundError(f"AZCopy binary not found at {az_copy_binary}")
        if not self._az_copy_destination_url:
            raise ValueError("AZCopy destination URL must be provided.")
    
    def upload(self, file: Path) -> None:
        cmd = [
            str(self._az_copy_binary),
            "copy",
            str(file),
            self._az_copy_destination_url,
            "--output-type",
            "json",
            "--log-level",
            "NONE"
        ]
        
        if self._dry_run:
            cmd.append("--dry-run")
            cmd.append("--from-to")
            cmd.append("LocalBlob")
        else:
            cmd.append("--output-level")
            cmd.append("essential")
            
        try:
            logger.debug(f"Executing azcopy cmd: '{' '.join(cmd)}'")  

            result = subprocess.run(
                cmd,
                capture_output=True,
                check=True,
                text=True
            )

            if result.stderr:
                logger.warning(f"Upload completed with warnings: {result.stderr}")
            
            logger.debug(f"Upload completed successfully for {file.name}")
        
        except CalledProcessError as e: 
            err_arr = [
                json.loads(line)['MessageContent'] 
                for line in e.output.splitlines() 
                if line.strip()
            ]
            
            msg: str = f"AZCopy command failed, file: {file.name}, command: '{' '.join(cmd)}', cmd errors: {" | ".join(err_arr)}"
            
            raise IOError(msg) from e

        except Exception as e: 
            msg: str = f"AZCopy command failed, file: {file.name}, command: {' '.join(cmd)}"
            logger.error(f"{msg}, error: {e}")
            raise IOError(msg) from e