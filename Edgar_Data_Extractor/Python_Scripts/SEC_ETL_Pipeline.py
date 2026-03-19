import re
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Tuple, Optional, Set
from XBRLToPostgresLoader import XBRLToPostgresLoader
from SECScraper import SECScraper

class SECFilingPipeline:
    def __init__(self, cik: str, base_save_dir: str, db_config: dict, years: Set[int], count: int = 100):
        """
        Initialize the complete SEC filing pipeline
        
        Args:
            cik: Company CIK number (with leading zeros)
            base_save_dir: Base directory for storing filings (e.g., 'filings')
            db_config: Database connection parameters
            years: Set of years to process
            count: Number of filings to fetch per request
        """
        self.cik = cik
        self.base_save_dir = base_save_dir
        self.db_config = db_config
        self.years = years
        self.count = count
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        """Configure and return a logger instance"""
        logger = logging.getLogger('SECFilingPipeline')
        logger.setLevel(logging.INFO)
        
        # Create file handler
        log_file = Path(self.base_save_dir) / self.cik / 'sec_pipeline.log'
        log_file.parent.mkdir(parents=True, exist_ok=True)  # Ensure directory exists
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        
        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # Add handlers
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger
    
    def get_filing_dir(self, filing_type: str, period: str) -> Path:
        """Get the full directory path for a specific filing"""
        return Path(self.base_save_dir) / self.cik / filing_type / period
    
    def scrape_filings(self) -> None:
        """Scrape SEC filings using SECScraper"""
        
        self.logger.info(f"Starting SEC scraping for CIK {self.cik}")
        
        scraper = SECScraper(
            cik=self.cik,
            count=self.count,
            requests_per_second=8.0,
            years=self.years,
            # base_save_dir=self.base_save_dir  # Pass the base directory to the scraper
        )
        
        try:
            filing_types = ['10-K', '10-Q']
            for filing_type in filing_types:
                self.logger.info(f"Scraping {filing_type} filings...")
                scraper.filing_type = filing_type
                scraper.scrape()
                self.logger.info(f"Completed {filing_type} scraping")
        except KeyboardInterrupt:
            self.logger.warning("Scraping interrupted by user")
        except Exception as e:
            self.logger.error(f"Scraping failed: {str(e)}", exc_info=True)
            raise
        finally:
            del scraper
            self.logger.info("Scraping completed")
    
    def find_xbrl_files(self) -> List[Tuple[str, str]]:
        """Find all instance and submission file pairs in the directory structure"""
        file_pairs = []
        base_dir = Path(self.base_save_dir) / self.cik
        self.logger.info(f"Locating XBRL files in: {base_dir}")
        
        for filing_type in ['10-K', '10-Q']:
            filing_type_dir = base_dir / filing_type
            if not filing_type_dir.exists():
                self.logger.warning(f"Directory not found: {filing_type_dir}")
                continue
                
            self.logger.info(f"Processing {filing_type} filings...")
            
            for period_dir in filing_type_dir.iterdir():
                if not period_dir.is_dir():
                    continue
                    
                instance_file, submission_file = self._find_files_in_period_dir(period_dir)
                
                if instance_file and submission_file:
                    file_pairs.append((str(instance_file), str(submission_file)))
                    self.logger.debug(f"Found file pair in {period_dir.name}")
                else:
                    self.logger.warning(f"Incomplete file pair in {period_dir.name}")
        
        self.logger.info(f"Found {len(file_pairs)} complete XBRL file pairs")
        return file_pairs
    
    def _find_files_in_period_dir(self, period_dir: Path) -> Tuple[Optional[Path], Optional[Path]]:
        """Find instance and submission files in a single period directory"""
        instance_file = None
        submission_file = None
        
        for file in period_dir.iterdir():
            file_lower = file.name.lower()
            
            if re.search(r'(?<!_cal)(?<!_def)(?<!_lab)(?<!_pre)(_htm)?\.xml$', file_lower):
                instance_file = file
            elif file_lower.endswith('.txt'):
                submission_file = file
        
        return instance_file, submission_file
    
    def process_xbrl_files(self, file_pairs: List[Tuple[str, str]]) -> None:
        """Process all XBRL file pairs with the loader"""
        
        total_files = len(file_pairs)
        success_count = 0
        failure_count = 0
        start_time = datetime.now()
        
        self.logger.info(f"Starting processing of {total_files} file pairs")
        
        loader = XBRLToPostgresLoader(db_config=self.db_config)
        
        try:
            for idx, (instance_file, submission_file) in enumerate(file_pairs, 1):
                file_prefix = f"[{idx}/{total_files}]"
                self.logger.info(f"{file_prefix} Processing: {Path(instance_file).name}")
                
                try:
                    if loader.load_xbrl_file(instance_file):
                        company_id = loader.load_metadata(submission_file)
                        fact_count = loader._process_standard_facts(company_id=company_id)
                        
                        self.logger.info(
                            f"{file_prefix} Processed {fact_count} facts from "
                            f"{Path(instance_file).name}"
                        )
                        success_count += 1
                    else:
                        self.logger.error(f"{file_prefix} Failed to load XBRL file")
                        failure_count += 1
                except Exception as e:
                    self.logger.error(
                        f"{file_prefix} Error processing file: {str(e)}",
                        exc_info=True
                    )
                    failure_count += 1
                    continue
        finally:
            loader.close()
            elapsed = datetime.now() - start_time
            self.logger.info(
                f"Processing completed. Success: {success_count}, "
                f"Failures: {failure_count}, Elapsed: {elapsed}"
            )
    
    def run_pipeline(self) -> None:
        """Run the complete pipeline: scraping → file discovery → processing"""
        try:
            # Step 1: Scrape filings from SEC
            self.scrape_filings()
            
            # Step 2: Find all XBRL files
            file_pairs = self.find_xbrl_files()
            
            if not file_pairs:
                self.logger.warning("No XBRL file pairs found to process")
                return
            
            # Step 3: Process all XBRL files
            self.process_xbrl_files(file_pairs)
            
            self.logger.info("SEC filing pipeline completed successfully")
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
            raise

if __name__ == "__main__":
    # Configuration
    config = {
        'cik': '0001001838',
        'base_save_dir': 'filings',  # This is now the base directory
        'db_config': {
            'dbname': 'finhub',
            'user': 'finhub_admin',
            'password': 'pass@123',
            'host': 'localhost',
            'port': '5432'
        },
        'years': {2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025},
        'count': 100
    }
    
    # Initialize and run pipeline
    pipeline = SECFilingPipeline(**config)
    pipeline.run_pipeline()