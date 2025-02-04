import os
import requests
import pandas as pd
import yfinance as yf
from datetime import datetime
from typing import Literal

def get_option_price(
    exchange: str,
    ticker: str,
    contract_type: Literal["call", "put"],
    strike_price: float,
    expiration_date: str
) -> float:
    """
    Get the current price of an American-style option contract.
    
    Args:
        exchange (str): Exchange where the underlying asset is traded
        ticker (str): Exchange ticker symbol of the underlying asset
        contract_type (str): Type of option contract ("call" or "put")
        strike_price (float): Strike price of the option contract
        expiration_date (str): Expiration date in YYYY-MM-DD format
        
    Returns:
        float: Current price of the option contract
        
    Raises:
        ValueError: If contract_type is not "call" or "put"
        ValueError: If expiration_date is invalid or in the past
        ValueError: If ticker is not found
    """
    # Validate contract type
    if contract_type.lower() not in ["call", "put"]:
        raise ValueError("Contract type must be either 'call' or 'put'")
    
    # Validate expiration date
    try:
        exp_date = datetime.strptime(expiration_date, "%Y-%m-%d")
        if exp_date < datetime.now():
            raise ValueError("Expiration date cannot be in the past")
    except ValueError as e:
        raise ValueError(f"Invalid expiration date format. Use YYYY-MM-DD. Error: {str(e)}")
    
    # Get option data using yfinance
    stock = yf.Ticker(ticker)
    
    try:
        # Get all options chain for the expiration date
        options = stock.option_chain(expiration_date)
        
        # Select calls or puts based on contract_type
        chain = options.calls if contract_type.lower() == "call" else options.puts
        
        # Find the contract with matching strike price
        contract = chain[chain['strike'] == strike_price]
        
        if contract.empty:
            raise ValueError(f"No {contract_type} contract found for strike price {strike_price}")
        
        # Return the last traded price
        return float(contract['lastPrice'].iloc[0])
        
    except Exception as e:
        raise ValueError(f"Error fetching option data: {str(e)}")

def get_options_chain_from_yfinance(exchange: str, ticker: str) -> pd.DataFrame:
    """
    Get the complete options chain for an equity asset using yfinance.
    
    Args:
        exchange (str): Exchange where the underlying asset is traded
        ticker (str): Exchange ticker symbol of the underlying asset
        
    Returns:
        pd.DataFrame: DataFrame containing options data with columns:
            - contract_id: str, unique identifier for the option contract
            - type: str, option type ('call' or 'put')
            - strike: float, strike price of the contract
            - expiration: str, expiration date of the contract
            - volume: int, trading volume
            - open_interest: int, open interest
            - implied_volatility: float, implied volatility
            - bid: float, current bid price
            - ask: float, current ask price
        
    Raises:
        ValueError: If ticker is not found or if there's an error fetching options data
    """
    try:
        # Get stock data using yfinance
        stock = yf.Ticker(ticker)
        
        # Get all available expiration dates
        expiration_dates = stock.options
        
        if not expiration_dates:
            raise ValueError(f"No options available for {ticker}")
        
        # Initialize list to store all options data
        all_options = []
        
        # Fetch options data for each expiration date
        for date in expiration_dates:
            try:
                # Get options chain for this expiration
                options = stock.option_chain(date)
                
                # Process calls
                calls_df = options.calls.copy()
                calls_df['type'] = 'call'
                calls_df['expiration'] = date
                
                # Process puts
                puts_df = options.puts.copy()
                puts_df['type'] = 'put'
                puts_df['expiration'] = date
                
                # Select and rename columns for both
                columns = {
                    'contractSymbol': 'contract_id',
                    'openInterest': 'open_interest',
                    'impliedVolatility': 'implied_volatility'
                }
                
                for df in [calls_df, puts_df]:
                    df.rename(columns=columns, inplace=True)
                    selected_df = df[['contract_id', 'type', 'strike', 'expiration', 
                                    'volume', 'open_interest', 'implied_volatility', 
                                    'bid', 'ask']]
                    all_options.append(selected_df)
                
            except Exception as e:
                continue
        
        if not all_options:
            raise ValueError(f"Failed to fetch any valid options data for {ticker}")
            
        # Concatenate all options into a single DataFrame
        options_chain = pd.concat(all_options, ignore_index=True)
        
        return options_chain
        
    except Exception as e:
        raise ValueError(f"Error fetching options chain: {str(e)}")

def get_options_chain_from_alphavantage(exchange: str, ticker: str) -> pd.DataFrame:
    """
    Get option prices using Alpha Vantage API.
    
    Args:
        exchange (str): Exchange where the underlying asset is traded
        ticker (str): Exchange ticker symbol of the underlying asset
        
    Returns:
        pd.DataFrame: DataFrame containing options data with columns:
            - contract_id: str, unique identifier for the option contract
            - type: str, option type ('call' or 'put')
            - strike: float, strike price of the contract
            - expiration: str, expiration date of the contract
            - volume: int, trading volume
            - open_interest: int, open interest
            - implied_volatility: float, implied volatility
            - bid: float, current bid price
            - ask: float, current ask price
        
    Raises:
        ValueError: If API call fails or returns invalid data
    """
    try:
        # Construct the API URL
        api_key =os.environ.get('ALPHAVANTAGE_API_KEY')
        base_url = "https://www.alphavantage.co/query?function="
        function_param = "HISTORICAL_OPTIONS"
        ticker_param = ticker
        api_key_param = api_key
        request_url = base_url + function_param + "&symbol=" + ticker_param + '&apikey=' + api_key_param
        
        # Make API request
        response = requests.get(request_url)
        response.raise_for_status()
        data = response.json()
        
        # Check for error messages
        if "Error Message" in data:
            raise ValueError(f"API Error: {data['Error Message']}")
            
        if not data:
            raise ValueError(f"No options data available for {ticker}")

        # Get the options data
        options_data = data.get('data', [])
        
        # Initialize list to store processed options data
        all_options = []
        
        # Process each option contract
        for contract in options_data:
            try:                
                # Create standardized contract data
                option_data = {
                    'contract_id': contract['contractID'],
                    'type': contract['type'],
                    'strike': float(contract['strike']),
                    'expiration': contract['expiration'],
                    'volume': int(contract.get('volume', 0)),
                    'open_interest': int(contract.get('open_interest', 0)),
                    'implied_volatility': float(contract.get('implied_volatility', 0.0)),
                    'bid': float(contract.get('bid', 0.0)),
                    'ask': float(contract.get('ask', 0.0))
                }
                all_options.append(option_data)
            except (KeyError, ValueError, TypeError) as e:
                # Skip contracts with missing or invalid data
                continue
        
        if not all_options:
            raise ValueError(f"No valid options data found for {ticker}")
        
        # Convert to DataFrame
        options_chain = pd.DataFrame(all_options)
        
        # Ensure columns are in the correct order
        column_order = ['contract_id', 'type', 'strike', 'expiration', 'volume', 
                       'open_interest', 'implied_volatility', 'bid', 'ask']
        options_chain = options_chain[column_order]
        
        return options_chain
        
    except requests.exceptions.RequestException as e:
        raise ValueError(f"API request failed: {str(e)}")
    except (KeyError, ValueError) as e:
        raise ValueError(f"Error processing API response: {str(e)}")
    except Exception as e:
        raise ValueError(f"Unexpected error: {str(e)}")

def get_and_save_aggregate_options_chain(exchange: str, ticker: str, output_dir: str = ".") -> pd.DataFrame:
    """
    Get options chain data from both yfinance and Alpha Vantage, combine them,
    and save the result as a CSV file.
    
    Args:
        exchange (str): Exchange where the underlying asset is traded
        ticker (str): Exchange ticker symbol of the underlying asset
        output_dir (str): Directory where the CSV file will be saved (default: current directory)
        
    Returns:
        pd.DataFrame: Combined DataFrame containing options data from both sources,
                     sorted by contract_id. The following columns are formatted to
                     two decimal places:
                     - strike
                     - implied_volatility
                     - bid
                     - ask
        
    Raises:
        ValueError: If there's an error fetching data from either source
    """
    try:
        # Get current timestamp for filename
        current_time = datetime.now()
        date_str = current_time.strftime("%Y-%m-%d")
        time_str = current_time.strftime("%H-%M-%S")
        
        # Get data from both sources
        yf_data = get_options_chain_from_yfinance(exchange, ticker)
        av_data = get_options_chain_from_alphavantage(exchange, ticker)
        
        # Add source column to identify where each row came from
        yf_data['source'] = 'yfinance'
        av_data['source'] = 'alphavantage'
        
        # Combine the data
        aggregate_data = pd.concat([yf_data, av_data], ignore_index=True)
        
        # Format decimal columns to two decimal places
        decimal_columns = ['strike', 'implied_volatility', 'bid', 'ask']
        for col in decimal_columns:
            aggregate_data[col] = aggregate_data[col].round(2)
        
        # Sort by contract_id
        aggregate_data.sort_values('contract_id', ascending=True, inplace=True)
        
        # Create filename using the specified convention
        filename = f"{exchange}_{ticker}_options_chain_{date_str}_as_of_{time_str}.csv"
        filepath = os.path.join(output_dir, filename)
        
        # Save to CSV, ensuring decimal places are preserved
        aggregate_data.to_csv(filepath, index=False, float_format='%.2f')
        
        return aggregate_data
        
    except Exception as e:
        raise ValueError(f"Error combining options chain data: {str(e)}")

def get_and_save_earliest_expiring_contracts(options_chain_df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters the options chain DataFrame to return only the contracts with the earliest expiration date.

    Args:
        options_chain_df (pd.DataFrame): The DataFrame containing the options chain data,
                                          as returned by `get_and_save_aggregate_options_chain`.

    Returns:
        pd.DataFrame: A DataFrame containing only the options contracts with the earliest expiration date.
                      If the input DataFrame is empty, returns an empty DataFrame.
    """

    if options_chain_df.empty:
        return pd.DataFrame()

    current_time = datetime.now()
    current_date_str = current_time.strftime("%Y-%m-%d")
    earliest_expiration = min(options_chain_df[options_chain_df['expiration'] >= current_date_str]['expiration'])
    earliest_expiring_contracts = options_chain_df[options_chain_df['expiration'] == earliest_expiration]
    
    # Save to CSV
    earliest_expiring_contracts.to_csv('earliest_expiring_contracts.csv', index=False)

    return earliest_expiring_contracts

def get_most_recent_equity_price(equity_exchange: str, equity_ticker: str) -> float:
    """
    Get the latest price of an equity.

    Args:
        equity_exchange (str): Exchange where the equity is traded.
        equity_ticker (str): Exchange ticker symbol of the equity.

    Returns:
        float: The equity's latest price, rounded to 2 decimal places.

    Raises:
        ValueError: If the ticker is not found or if there's an error fetching data.

    """
    try:
        stock = yf.Ticker(equity_ticker)
        data = stock.history(period="1d")
        if data.empty:
            raise ValueError(f"No data found for {equity_ticker}")
        return float(data['Close'][0].round(6))
    except Exception as e:
        raise ValueError(f"Error fetching equity price: {str(e)}")

def get_equity_price_history(equity_exchange: str, equity_ticker: str, period='max') -> pd.DataFrame:
    stock = yf.Ticker(equity_ticker)
    price_history = stock.history(period=period)
    return price_history

def calculate_price_differences(price_df: pd.DataFrame) -> pd.DataFrame:
    # The following values, unless otherwise written can be negative, positive
    # or negative
    price_df['open_price_dtd_abs_diff'] = price_df['Open'].diff().abs()
    price_df['open_price_dtd_pct_diff'] = (price_df['Open'].diff() / price_df['Open'].shift()) * 100
    price_df['close_price_dtd_abs_diff'] = price_df['Close'].diff()
    price_df['close_price_dtd_pct_diff'] = (price_df['Close'].diff() / price_df['Close'].shift()) * 100
    price_df['low_price_dtd_abs_diff'] = price_df['Low'].diff()
    price_df['low_price_dtd_pct_diff'] = (price_df['Low'].diff() / price_df['Low'].shift()) * 100
    price_df['high_price_dtd_abs_diff'] = price_df['High'].diff()
    price_df['high_price_dtd_pct_diff'] = (price_df['High'].diff() / price_df['High'].shift()) * 100
    price_df['open_to_close_abs_diff'] = price_df['Close'] - price_df['Open']
    # The following value is always positive
    price_df['high_to_low_abs_diff'] = price_df['High'] - price_df['Low']
    return price_df

def see_data_structure(equity_exchange: str, equity_ticker: str) -> None:
    stock = yf.Ticker(equity_ticker)
    # print(sorted(stock.info.keys()))
    # print(stock.info['volume'])
    # print(stock.history(period='5d'))  # returns object of type pd.DatFrame
    print(stock.history(period='max').head(1))
    print(stock.history(period='max').shape[0])