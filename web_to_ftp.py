import sys
import json
import time
import logging
import concurrent.futures
import schedule
import pandas as pd
from os import environ, remove
from pathlib import Path
from ftplib import FTP_TLS, error_perm
from contextlib import contextmanager
from typing import Dict, Any, Optional
from dataclasses import dataclass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_project.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class FTPConfig:
    """FTP configuration data class"""
    host: str
    user: str
    password: str
    target_dir: str = "/home/psyger/ftp/new"

@dataclass
class ProcessResult:
    """Result of processing a single data source"""
    source_name: str
    success: bool
    message: str
    file_size: Optional[int] = None
    processing_time: Optional[float] = None

class ETLPipeline:
    def __init__(self, config_file: str = "config.json"):
        self.config_file = config_file
        self.ftp_config = self._load_ftp_config()
        
    def _load_ftp_config(self) -> FTPConfig:
        """Load FTP configuration from environment variables"""
        try:
            return FTPConfig(
                host=environ["FTPHOST"],
                user=environ["FTPUSER"],
                password=environ["FTPPASS"],
                target_dir=environ.get("FTPTARGET", "/home/psyger/ftp/new")
            )
        except KeyError as e:
            logger.error(f"Missing environment variable: {e}")
            raise
    
    @contextmanager
    def get_ftp_connection(self):
        """Context manager for FTP connections with proper cleanup"""
        ftp = None
        try:
            logger.info(f"Connecting to FTP server: {self.ftp_config.host}")
            ftp = FTP_TLS(self.ftp_config.host)
            ftp.login(self.ftp_config.user, self.ftp_config.password)
            ftp.prot_p()  # Enable protection for data channel
            logger.info("FTP connection established")
            yield ftp
        except Exception as e:
            logger.error(f"FTP connection failed: {e}")
            raise
        finally:
            if ftp:
                try:
                    ftp.quit()
                    logger.info("FTP connection closed")
                except:
                    ftp.close()  # Force close if quit fails
    
    def read_csv_with_retry(self, config: Dict[str, Any], max_retries: int = 3) -> pd.DataFrame:
        """Read CSV with retry logic for network issues"""
        url = config["URL"]
        params = config.get("PARAMS", {})
        
        for attempt in range(max_retries):
            try:
                logger.info(f"Attempting to read CSV from {url} (attempt {attempt + 1})")
                df = pd.read_csv(url, **params)
                logger.info(f"Successfully read {len(df)} rows from {url}")
                return df
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    logger.error(f"Failed to read CSV after {max_retries} attempts")
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff
    
    def ensure_ftp_directory(self, ftp: FTP_TLS, target_dir: str) -> bool:
        """Ensure FTP directory exists, create if necessary"""
        try:
            ftp.cwd(target_dir)
            logger.info(f"Successfully navigated to {target_dir}")
            return True
        except error_perm:
            try:
                # Try to create the directory structure
                dirs = target_dir.strip('/').split('/')
                current_path = '/'
                for dir_name in dirs:
                    current_path = f"{current_path}{dir_name}/"
                    try:
                        ftp.cwd(current_path)
                    except error_perm:
                        ftp.mkd(current_path)
                        ftp.cwd(current_path)
                logger.info(f"Created and navigated to {target_dir}")
                return True
            except Exception as e:
                logger.error(f"Failed to create/navigate to directory {target_dir}: {e}")
                return False
    
    def upload_to_ftp(self, ftp: FTP_TLS, file_path: Path) -> bool:
        """Upload file to FTP server with error handling"""
        try:
            if not self.ensure_ftp_directory(ftp, self.ftp_config.target_dir):
                return False
            
            file_size = file_path.stat().st_size
            logger.info(f"Uploading {file_path.name} ({file_size} bytes)")
            
            with open(file_path, "rb") as fp:
                ftp.storbinary(f"STOR {file_path.name}", fp)
            
            logger.info(f"Successfully uploaded {file_path.name}")
            return True
        except Exception as e:
            logger.error(f"Failed to upload {file_path.name}: {e}")
            return False
    
    def safe_delete_file(self, file_path: Path) -> bool:
        """Safely delete file with error handling"""
        try:
            if file_path.exists():
                remove(file_path)
                logger.info(f"Deleted local file: {file_path.name}")
                return True
            return True
        except Exception as e:
            logger.error(f"Failed to delete {file_path.name}: {e}")
            return False
    
    def process_single_source(self, source_name: str, source_config: Dict[str, Any]) -> ProcessResult:
        """Process a single data source"""
        start_time = time.time()
        file_path = Path(f"{source_name}.csv")
        
        try:
            # Read CSV data
            df = self.read_csv_with_retry(source_config)
            
            # Save to local file
            df.to_csv(file_path, index=False)
            file_size = file_path.stat().st_size
            logger.info(f"Saved {source_name}.csv ({file_size} bytes)")
            
            # Upload to FTP
            with self.get_ftp_connection() as ftp:
                upload_success = self.upload_to_ftp(ftp, file_path)
                if not upload_success:
                    return ProcessResult(
                        source_name=source_name,
                        success=False,
                        message="Failed to upload to FTP",
                        file_size=file_size,
                        processing_time=time.time() - start_time
                    )
            
            # Clean up local file
            self.safe_delete_file(file_path)
            
            processing_time = time.time() - start_time
            return ProcessResult(
                source_name=source_name,
                success=True,
                message="Successfully processed",
                file_size=file_size,
                processing_time=processing_time
            )
            
        except Exception as e:
            # Ensure cleanup on error
            self.safe_delete_file(file_path)
            return ProcessResult(
                source_name=source_name,
                success=False,
                message=f"Error: {str(e)}",
                processing_time=time.time() - start_time
            )
    
    def load_config(self) -> Dict[str, Any]:
        """Load configuration from JSON file"""
        try:
            with open(self.config_file, 'r') as fp:
                config = json.load(fp)
                logger.info(f"Loaded configuration for {len(config)} sources")
                return config
        except Exception as e:
            logger.error(f"Failed to load config file: {e}")
            raise
    
    def run_sequential(self) -> Dict[str, ProcessResult]:
        """Run pipeline sequentially"""
        logger.info("Starting sequential processing")
        config = self.load_config()
        results = {}
        
        for source_name, source_config in config.items():
            logger.info(f"Processing source: {source_name}")
            result = self.process_single_source(source_name, source_config)
            results[source_name] = result
            
            if result.success:
                logger.info(f"✓ {source_name}: {result.message} ({result.processing_time:.2f}s)")
            else:
                logger.error(f"✗ {source_name}: {result.message}")
        
        return results
    
    def run_parallel(self, max_workers: int = 4) -> Dict[str, ProcessResult]:
        """Run pipeline in parallel with controlled concurrency"""
        logger.info(f"Starting parallel processing with {max_workers} workers")
        config = self.load_config()
        results = {}
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_source = {
                executor.submit(self.process_single_source, source_name, source_config): source_name
                for source_name, source_config in config.items()
            }
            
            # Collect results
            for future in concurrent.futures.as_completed(future_to_source):
                source_name = future_to_source[future]
                try:
                    result = future.result()
                    results[source_name] = result
                    
                    if result.success:
                        logger.info(f"✓ {source_name}: {result.message} ({result.processing_time:.2f}s)")
                    else:
                        logger.error(f"✗ {source_name}: {result.message}")
                        
                except Exception as e:
                    logger.error(f"✗ {source_name}: Unexpected error: {e}")
                    results[source_name] = ProcessResult(
                        source_name=source_name,
                        success=False,
                        message=f"Unexpected error: {str(e)}"
                    )
        
        return results
    
    def print_summary(self, results: Dict[str, ProcessResult]):
        """Print summary of processing results"""
        successful = sum(1 for r in results.values() if r.success)
        total = len(results)
        total_size = sum(r.file_size or 0 for r in results.values() if r.success)
        total_time = sum(r.processing_time or 0 for r in results.values())
        
        logger.info("=" * 50)
        logger.info("PROCESSING SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Total sources: {total}")
        logger.info(f"Successful: {successful}")
        logger.info(f"Failed: {total - successful}")
        logger.info(f"Total data processed: {total_size:,} bytes")
        logger.info(f"Total processing time: {total_time:.2f} seconds")
        logger.info("=" * 50)

def main():
    if len(sys.argv) < 2:
        print("Usage: python web_to_ftp.py [manual|schedule] [sequential|parallel]")
        sys.exit(1)
    
    execution_mode = sys.argv[1]
    processing_mode = sys.argv[2] if len(sys.argv) > 2 else "sequential"
    
    pipeline = ETLPipeline()
    
    def run_pipeline():
        try:
            if processing_mode == "parallel":
                results = pipeline.run_parallel(max_workers=4)
            else:
                results = pipeline.run_sequential()
            
            pipeline.print_summary(results)
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            raise
    
    if execution_mode == "manual":
        logger.info("Running pipeline manually")
        run_pipeline()
        
    elif execution_mode == "schedule":
        logger.info("Scheduling pipeline to run daily at 22:00")
        schedule.every().day.at("22:00").do(run_pipeline)
        
        logger.info("Scheduler started. Press Ctrl+C to stop.")
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute instead of every second
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")
    
    else:
        print("Invalid execution mode. Use 'manual' or 'schedule'")
        sys.exit(1)

if __name__ == "__main__":
    main()
