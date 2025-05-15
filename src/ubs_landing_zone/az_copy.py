from pathlib import Path
import subprocess
from loguru import logger

class AzCopy:
    # def __init__(self):
    #     ...
    
    def upload(self, feed: Path):
        cmd = [
            "echo", "azcopy",
            str(feed),
            f"destination",
            "--overwrite"
        ]

        try:
            logger.debug(f"Executing command: {' '.join(cmd)}")  

            result = subprocess.run(
                cmd,
                capture_output=True,
                check=True,
                text=True
            )

            if result.stderr:
                logger.warning(f"Upload completed with warnings: {result.stderr}")
            logger.debug(f"Upload completed successfully for {feed.name}")

        except Exception as e: 
            logger.error(f"Upload failed, error: {e}")
            raise