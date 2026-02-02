import pandas as pd
import yfinance as yf
import json
from time import sleep
import requests
import os
from pathlib import Path
from io import StringIO
import random

def get_ticker_info(symbol, max_retries=3):
    """Function to retrieve ticker information with retry logic"""
    for attempt in range(max_retries):
        try:
            # ✅ Laisser yfinance gérer la session (pas de paramètre session)
            ticker = yf.Ticker(symbol)
            info = ticker.info
            
            # Vérifier si on a des données valides
            if not info or 'symbol' not in info:
                raise ValueError(f"No data found for {symbol}")
            
            sector = info.get('sector')
            industry = info.get('industry')
            
            if sector and industry and sector != 'Unknown' and industry != 'Unknown':
                return sector, industry
            
            if attempt < max_retries - 1:
                print(f"Attempt {attempt + 1} failed for {symbol} (no sector/industry), retrying in 2s...")
                sleep(2)
            
        except Exception as e:
            if attempt < max_retries - 1:
                # Backoff exponentiel avec un peu d'aléatoire
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                print(f"Error for {symbol} (attempt {attempt + 1}): {e}, retrying in {wait_time:.1f}s...")
                sleep(wait_time)
            else:
                print(f"Final failure for {symbol}: {e}")
    
    return None, None

def process_nasdaq_file():
    """Process NASDAQ symbols and update JSON file with sector/industry data"""
    
    # Get NASDAQ symbols directly from URL
    url = "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqtraded.txt"
    result = {}
    
    # Define path for output file
    script_dir = Path(__file__).parent
    output_file = script_dir / 'ticker_info.json'
    
    # Load existing data if available
    if output_file.exists():
        try:
            with open(output_file, 'r') as f:
                result = json.load(f)
            print(f"Loaded existing data with {len(result)} entries")
        except Exception as e:
            print(f"Error loading existing file: {e}")
            pass
    
    try:
        # Download and process NASDAQ data
        response = requests.get(url)
        response.raise_for_status()
        
        # Read data into DataFrame
        df = pd.read_csv(StringIO(response.text), delimiter='|')
        print(f"Retrieved {len(df)} symbols from NASDAQ")
        
        # ✅ Plus besoin de créer une session pour yfinance
        processed_count = 0
        
        for _, row in df.iterrows():
            symbol = row['Symbol']
            
            # Filtrer les symboles invalides (avec des caractères spéciaux)
            if not symbol or '.' in symbol or '-' in symbol or len(symbol) > 5:
                continue
                
            if symbol not in result:
                sector, industry = get_ticker_info(symbol)
                
                if sector and industry:
                    result[symbol] = {
                        "info": {
                            "industry": industry,
                            "sector": sector
                        }
                    }
                    print(f"Added: {symbol} - {sector}/{industry}")
                    processed_count += 1
                    
                    # Save after each successful addition
                    with open(output_file, 'w') as f:
                        json.dump(result, f, indent=2)
                else:
                    print(f"Skipped (missing data after retries): {symbol}")
                
                # Pause aléatoire entre 1 et 3 secondes pour éviter les rate limits
                sleep(random.uniform(1, 3))
                
                # Pause plus longue tous les 10 symboles
                if processed_count % 10 == 0:
                    print(f"Processed {processed_count} symbols, taking a longer break...")
                    sleep(5)
    
    except Exception as e:
        print(f"Error processing NASDAQ data: {e}")
        raise
    
    print(f"Final dataset contains {len(result)} symbols")
    return result

if __name__ == "__main__":
    process_nasdaq_file()
