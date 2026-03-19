import requests
import os
import pycountry
from yahooquery import Ticker
import re
from lxml import html


def extract_year(xbrl_file_path, logger):
    """Extract year from XSD filename (handles both hyphenated and compact dates)"""
    try:
        logger.debug(f"Attempting to extract year from: {xbrl_file_path}")
        filename = os.path.basename(xbrl_file_path)
        year_match = re.search(r'\d{4}', filename)
        
        if year_match:
            year = int(year_match.group(0))
            logger.info(f"Successfully extracted year {year} from filename")
            return year
                
        raise ValueError(f"No valid year found in filename: {xbrl_file_path}")
        
    except Exception as e:
        logger.error(f"Year extraction failed for {xbrl_file_path}: {str(e)}", exc_info=True)
        return None


def get_lei_by_name(extracted_data, logger):
    """Fetch LEI using company name from GLEIF API with fallback mechanisms"""
    logger.info(f"Starting LEI lookup for: {extracted_data['company_name']}")
    url = "https://api.gleif.org/api/v1/lei-records"
    params = {"filter[entity.legalName]": extracted_data['company_name']}
    headers = {
        "Accept": "application/vnd.api+json",
        "User-Agent": "Mithun Thakkar (mithun.thakkar8@gmail.com)"
    }
    
    try:
        logger.debug(f"Making initial GLEIF API request for: {extracted_data['company_name']}")
        response = requests.get(url, headers=headers, params=params, timeout=10)
        response.raise_for_status()

        if response.ok and len(response.json().get("data", [])) > 0:
            lei = response.json()["data"][0]["id"]
            logger.info(f"Found LEI {lei} for primary company name")
            return lei
        
        lei = ''
        logger.debug("No direct match found, attempting simplified name")

        # Fallback 1: Simplified name
        simplified_name = re.sub(r"\b(INC|CORP|LLC|PLC|LTD|CO)\b", "", extracted_data['company_name'], flags=re.IGNORECASE).strip()
        if simplified_name != extracted_data['company_name']:
            logger.debug(f"Trying simplified name: {simplified_name}")
            params = {"filter[entity.legalName]": simplified_name}
            response = requests.get(url, headers=headers, params=params)
            if response.ok and len(response.json().get("data", [])) > 0:
                lei = response.json()["data"][0]["id"]
                logger.info(f"Found LEI {lei} with simplified name")
        
        # Fallback 2: Former name
        if not lei and extracted_data.get('former_name'):
            logger.debug(f"Trying former name: {extracted_data['former_name']}")
            simplified_name = re.sub(r"\b(INC|CORP|LLC|PLC|LTD|CO)\b", "", extracted_data['former_name'], flags=re.IGNORECASE).strip()
            if simplified_name != extracted_data['former_name']:
                params = {"filter[entity.legalName]": simplified_name}
                response = requests.get(url, headers=headers, params=params)
                if response.ok and len(response.json().get("data", [])) > 0:
                    lei = response.json()["data"][0]["id"]
                    logger.info(f"Found LEI {lei} with former name")

        if lei:
            return lei
        else:
            logger.warning(f"No LEI found for company: {extracted_data['company_name']}")
            return None

    except requests.exceptions.RequestException as e:
        logger.error(f"GLEIF API request failed for {extracted_data['company_name']}: {str(e)}", exc_info=True)
        return None


def get_iso_code(country_name, logger):
    """Convert country name to ISO 2-letter code with fuzzy matching"""
    try:
        logger.debug(f"Looking up ISO code for country: {country_name}")
        country = pycountry.countries.search_fuzzy(country_name)[0]
        iso_code = country.alpha_2
        logger.info(f"Found ISO code {iso_code} for {country_name}")
        return iso_code
    except LookupError:
        logger.warning(f"No ISO code found for country: {country_name}")
        return None
    except Exception as e:
        logger.error(f"Error in ISO code lookup for {country_name}: {str(e)}", exc_info=True)
        return None

def get_ticker_from_cik(cik: str, logger):
    """Lookup ticker symbol using SEC API"""
    try:
        logger.info(f"Looking up ticker for CIK: {cik}")
        url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        headers = {
            "User-Agent": "Mithun Thakkar (thakkarmithun26@gmail.com)",
            "Accept-Encoding": "gzip, deflate",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Connection": "keep-alive"
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        tickers = data.get("tickers", [])
        
        if tickers:
            logger.info(f"Found ticker {tickers[0]} for CIK {cik}")
            return tickers[0]
        else:
            logger.warning(f"No tickers found for CIK {cik}")
            return None
            
    except Exception as e:
        logger.error(f"Ticker lookup failed for CIK {cik}: {str(e)}", exc_info=True)
        return None

def query_yahoo(ticker_symbol, logger):
    """Fetch company data from Yahoo Finance"""
    try:
        logger.info(f"Querying Yahoo Finance for: {ticker_symbol}")
        ticker = Ticker(ticker_symbol)
        summary = ticker.asset_profile.get(ticker_symbol, {})
        quote_type = ticker.quote_type.get(ticker_symbol, {})
        
        yahoo_results = {
            "country": summary.get("country"),
            "industry": summary.get("industry"),
            "sector": summary.get("sector"),
            "exchange_code": quote_type.get("exchange")  
        }
        
        logger.debug(f"Yahoo results for {ticker_symbol}: {yahoo_results}")
        return yahoo_results
    
    except Exception as e:
        logger.error(f"Error fetching Yahoo data for {ticker_symbol}: {e}", exc_info=True)
        return None

def extract_text_from_tags(raw_html, logger):
    """Extract clean text from HTML tags"""
    try:
        tree = html.fromstring(raw_html)
        font_texts = tree.xpath(".//font//text()|.//span//text()")
        clean_text = " ".join(t.strip() for t in font_texts if t.strip())
        return clean_text
    except Exception as e:
        logger.warning(f"HTML parsing failed, returning raw text: {str(e)}")
        return raw_html  # fallback to raw if parsing fails

def get_cleaned_value(fact_value, logger):
    """Clean and normalize fact values"""
    try:
        if isinstance(fact_value, str) and ("<font" in fact_value.lower() or "<span" in fact_value.lower()):
            logger.debug("Cleaning HTML-containing value")
            value = extract_text_from_tags(fact_value, logger)
            logger.debug(f"Cleaned value: {value[:100]}...")
            return value
        return fact_value
    except Exception as e:
        logger.error(f"Error cleaning value: {str(e)}", exc_info=True)
        return fact_value
    
