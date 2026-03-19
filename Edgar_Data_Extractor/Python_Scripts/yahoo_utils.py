import yahooquery as yq

def fetch_company_metadata(ticker_symbol):
    """Fetches company sector and industry from Yahoo Finance."""
    ticker = yq.Ticker(ticker_symbol)
    profile = ticker.asset_profile.get(ticker_symbol, {})

    return {
        "sector": profile.get("sector", "Unknown"),
        "industry": profile.get("industry", "Unknown")
    }
