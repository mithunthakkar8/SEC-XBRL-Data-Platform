import yfinance as yf
import yahooquery as yq
import pandas as pd
pd.set_option("display.max_columns", None)


# Define the stock ticker symbol 
ticker = "SCCO"

# Fetch data
stock = yf.Ticker(ticker)

# Get historical market data (e.g., last 1 year of daily data)
price_data = stock.history(period="15y", auto_adjust = True)

# get historical analyst downgrades and upgrades data from 2012 to current time
upgrades_downgrades = stock.upgrades_downgrades

# get copper industry company metrics
# Define the industry name
industry = "Copper"  

# Fetch companies in the industry
s = yq.Screener()
screen = s.get_screeners("copper", count=100)

df = pd.DataFrame(screen)

industry_companies_df = pd.DataFrame(df.loc['quotes'].iloc[0]).dropna(axis=1, how="all")

filtered_companies_df=industry_companies_df[['symbol', 'averageAnalystRating', 'dividendYield', 'epsTrailingTwelveMonths', 'epsForward'
                       , 'epsCurrentYear', 'marketCap', 'forwardPE', 'priceToBook']]


