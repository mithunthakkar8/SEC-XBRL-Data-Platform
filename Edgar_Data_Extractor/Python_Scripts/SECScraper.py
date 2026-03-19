import httpx
from lxml import html
import os
import time
import logging
from typing import Optional, List, Dict, Tuple, Set
from urllib.parse import urljoin
import re
import random
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sec_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SECScraper:
    def __init__(self, cik: str, filing_type: str = "10-K", count: int = 100, 
                 requests_per_second: float = 9.0, years: Optional[Set[int]] = None):
        self.cik = cik
        self.filing_type = filing_type
        self.count = count
        self.base_url = "https://www.sec.gov"
        self.min_request_interval = 1.0 / requests_per_second
        self.years = years if years else None
        self.client = httpx.Client(
            headers={
                "User-Agent": "Mithun Thakkar (thakkarmithun26@gmail.com)", 
                "Accept-Encoding": "gzip, deflate",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Connection": "keep-alive",
            },
            timeout=30.0,
            follow_redirects=True
        )
        self.last_request_time = 0
        
    def _rate_limit(self):
        """Ensure we respect SEC's rate limits with jitter"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_request_interval:
            jitter = random.uniform(0, self.min_request_interval * 0.1)
            sleep_time = self.min_request_interval - elapsed + jitter
            logger.debug(f"Rate limiting - sleeping for {sleep_time:.3f} seconds")
            time.sleep(sleep_time)
        self.last_request_time = time.time()
    
    def _get_with_retry(self, url: str, max_retries: int = 3) -> Optional[httpx.Response]:
        """Get request with retry logic and exponential backoff"""
        for attempt in range(max_retries):
            try:
                self._rate_limit()
                response = self.client.get(url)
                response.raise_for_status()
                return response
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 429:
                    wait_time = min(2 ** attempt + random.uniform(0, 1), 60)
                    logger.warning(f"Rate limited. Waiting {wait_time:.1f} seconds before retry {attempt + 1}/{max_retries}")
                    time.sleep(wait_time)
                    continue
                logger.error(f"HTTP error {e.response.status_code} for URL {url}")
                return None
            except httpx.RequestError as e:
                logger.error(f"Request failed: {e}")
                if attempt == max_retries - 1:
                    return None
                time.sleep(1 + attempt * 2)
        return None
    
    def _extract_filing_date(self, row) -> Optional[datetime]:
        """Extract filing date from a table row"""
        date_cell = row.xpath('./td[4]')  # Filing date is typically in the 4th column
        if not date_cell:
            return None
        
        date_str = date_cell[0].text_content().strip()
        try:
            return datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            try:
                # Try alternative format if primary fails
                return datetime.strptime(date_str, '%m/%d/%Y')
            except ValueError:
                logger.warning(f"Could not parse date string: {date_str}")
                return None
        
    def get_filing_links(self) -> Dict[str, List[str]]:
        """Get dictionary of filing document links grouped by filing type (10-K or 10-K/A)"""
        base_url = f"{self.base_url}/cgi-bin/browse-edgar?action=getcompany&CIK={self.cik}&type={self.filing_type}&count={self.count}"
        if self.filing_type == '10-K':
            doc_links = {"10-K": [], "10-K/A": []}  # Separate lists for regular and amended filings
        else:
            doc_links = {self.filing_type: []}
        current_url = base_url
        
        while current_url:
            logger.info(f"Fetching page: {current_url}")
            response = self._get_with_retry(current_url)
            if not response:
                logger.error("Failed to fetch filing index page")
                break
            
            tree = html.fromstring(response.text)
            rows = tree.xpath('//table[@class="tableFile2"]/tr[position()>1]')
            
            for row in rows:
                filing_date = self._extract_filing_date(row)
                if not filing_date:
                    continue
                    
                if self.years and filing_date.year not in self.years:
                    continue
                
                # Extract filing type from the same row (typically in the 1st column)
                filing_type_cell = row.xpath('./td[1]/text()')
                current_filing_type = filing_type_cell[0].strip() if filing_type_cell else self.filing_type
                
                doc_link = row.xpath('./td[2]/a[contains(translate(text(), "DOCUMENTS", "documents"), "documents")]/@href')
                # Normalize filing type (handle variations)
                doc_links[current_filing_type].append(urljoin(self.base_url, doc_link[0]))
            
            # Pagination logic remains the same
            next_button = tree.xpath('//input[contains(@value, "Next") and @type="button"]')
            current_url = self._get_next_page_url(tree, next_button) if next_button else None
        
        logger.info(f"Found {sum(len(v) for v in doc_links.values())} {self.filing_type} documents")
        return doc_links
    
    def extract_period_of_report(self, tree) -> Optional[str]:
        """Extract period of report from the document page with multiple fallback strategies"""
        xpaths = [
            "//div[contains(translate(., 'PERIODOFREPORT', 'periodofreport'), 'period of report')]/following-sibling::div",
            "//div[contains(., 'Period of Report')]/following-sibling::div",
            "//div[contains(@class, 'formGrouping') and contains(., 'Period of Report')]//div[@class='info']",
            "//div[contains(., 'Filing Date')]/following-sibling::div"
        ]
        
        for xpath in xpaths:
            elements = tree.xpath(xpath)
            if elements:
                period_text = elements[0].text_content().strip()
                period_text = re.sub(r'[^0-9-]', '', period_text)
                if len(period_text) == 8:
                    period_text = f"{period_text[:4]}-{period_text[4:6]}-{period_text[6:8]}"
                if period_text:
                    return period_text
        
        logger.warning("Could not find period of report on document page")
        return None
    
    def find_xbrl_files(self, tree) -> Dict[str, Tuple[str, str]]:
        """Find all XBRL-related files on the document page"""
        xbrl_files = {}
        xbrl_patterns = ['cal.xml', 'def.xml', 'lab.xml', 'pre.xml', '.xsd']
        
        for pattern in xbrl_patterns:
            links = tree.xpath(f'//a[contains(translate(@href, "XML", "xml"), "{pattern}")]/@href')
            if links:
                file_name = links[0].split('/')[-1]
                xbrl_files[pattern.replace('.', '')] = (file_name, urljoin(self.base_url, links[0]))
        
        # Instance file
        instance_link = tree.xpath(
            '''//tr[
                td[contains(translate(text(), "INSTANCE", "instance"), "instance")
                or contains(translate(text(), ".INS", ".ins"), ".ins")]
            ]//a/@href'''
        )
        if instance_link:
            file_name = instance_link[0].split('/')[-1]
            xbrl_files['instance'] = (file_name, urljoin(self.base_url, instance_link[0]))
        
        # Submission text file
        submission_row = tree.xpath('//tr[contains(translate(., "SUBMISSION", "submission"), "submission")]')
        if submission_row:
            submission_link = submission_row[0].xpath('.//a/@href')
            if submission_link:
                file_name = submission_link[0].split('/')[-1]
                xbrl_files['submission'] = (file_name, urljoin(self.base_url, submission_link[0]))

        # HTML filing document (look for .htm or .html links with filing type in text)
        html_link = tree.xpath(f'''
        //tr[.//td[contains(translate(text(), "{self.filing_type.upper()}", "{self.filing_type.lower()}"), 
         "{self.filing_type.lower()}")]]//a[contains(@href, ".htm") or contains(@href, ".html")]/@href''')
        if html_link:
            # Take the first .htm/.html link found
            html_url = html_link[0]
            if html_url.startswith('/ix?doc=/'):
                html_url = html_url.replace('/ix?doc=/', '/')
            file_name = html_url.split('/')[-1]
            xbrl_files['html_filing'] = (file_name, urljoin(self.base_url, html_url))
        
        logger.info(f"Found {len(xbrl_files)} files (XBRL+HTML)")
        return xbrl_files
    
    def download_file(self, url: str, save_path: str) -> bool:
        """Download a file only if it doesn't already exist"""
        if os.path.exists(save_path):
            logger.info(f"File already exists, skipping download: {save_path}")
            return True
            
        try:
            self._rate_limit()
            response = self._get_with_retry(url)
            if not response:
                return False
            
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            
            with open(save_path, 'wb') as f:
                f.write(response.content)
            
            logger.info(f"Successfully downloaded {url} to {save_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to download {url}: {e}")
            return False
    
    def process_document_page(self, doc_url: str, filing_type: str, base_save_dir: str = "filings") -> bool:
        """Process a single document page with filing type awareness"""
        logger.info(f"Processing {filing_type} document page: {doc_url}")
        
        response = self._get_with_retry(doc_url)
        if not response:
            return False
        
        tree = html.fromstring(response.text)
        period = self.extract_period_of_report(tree)
        if not period:
            logger.warning(f"Could not determine period for {doc_url}, skipping")
            return False
        
        # Create directory path with filing type subfolder
        period_dir = os.path.join(base_save_dir, self.cik, filing_type, period)
    
        # Find all files (XBRL + HTML)
        files = self.find_xbrl_files(tree)
        if not files:
            logger.warning(f"No files found for {doc_url}")
            return False
        
        success = True
        for file_type, (file_name, file_url) in files.items():
            save_path = os.path.join(period_dir, file_name)
            if not self.download_file(file_url, save_path):
                success = False
                logger.warning(f"Failed to download {file_type} file")
        
        return success
    
    def scrape(self):
        """Main method to execute the scraping process"""
        logger.info(f"Starting SEC scraping for CIK: {self.cik}")
        if self.years:
            logger.info(f"Filtering for years: {sorted(self.years)}")
        
        try:
            # Get document links grouped by filing type
            doc_links = self.get_filing_links()
            if not any(doc_links.values()):
                logger.error("No document links found, exiting")
                return
            
            success_count = 0
            total_count = 0
            
            # Process each filing type separately
            for filing_type, links in doc_links.items():
                if not links:
                    logger.info(f"No {filing_type} filings found")
                    continue
                    
                logger.info(f"Processing {len(links)} {filing_type} filings")
                total_count += len(links)
                
                for i, doc_link in enumerate(links, 1):
                    logger.info(f"Processing {filing_type} document {i}/{len(links)}: {doc_link}")
                    if self.process_document_page(doc_link, filing_type):
                        success_count += 1
            
            logger.info(f"Completed. Successfully processed {success_count}/{total_count} documents across all filing types")
        except Exception as e:
            logger.error(f"Fatal error during scraping: {e}", exc_info=True)
            raise
    
    def __del__(self):
        """Clean up the HTTP client"""
        if hasattr(self, 'client'):
            self.client.close()

if __name__ == "__main__":
    scraper = SECScraper(
        cik="0001001838",
        # filing_type="10-K",
        count=100,  # Higher count reduces pagination requests
        requests_per_second=8.0,
        years={2017, 2018, 2019}  # Only fetch filings from these years
    )
    try:
        filing_types = ['10-K']
        for filing_type in filing_types:
            scraper.filing_type = filing_type
            scraper.scrape()
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    finally:
        del scraper