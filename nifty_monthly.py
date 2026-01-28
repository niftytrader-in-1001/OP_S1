import os
import time
import pandas as pd
from datetime import datetime, timezone, timedelta
import pyotp
import sys
import requests
import zipfile
import io
import traceback
import logging
import concurrent.futures
from threading import Lock
import numpy as np
import tempfile
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import math


# =========================================================
# LOGGING (reduced noise for GitHub Actions)
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# =========================================================
# ENVIRONMENT VARIABLES (GitHub Secrets)
# =========================================================
ANGEL_API_KEY = os.getenv("ANGEL_API_KEY")
ANGEL_CLIENT_ID = os.getenv("ANGEL_CLIENT_ID")
ANGEL_PIN = os.getenv("ANGEL_PIN")
ANGEL_TOTP = os.getenv("ANGEL_TOTP")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID_NIFTY")

# Allow script to continue even if TOTP is missing for debugging
if not all([ANGEL_API_KEY, ANGEL_CLIENT_ID, ANGEL_PIN]):
    raise RuntimeError("❌ Missing critical Angel One credentials")

# Only warn about TOTP, don't exit
if not ANGEL_TOTP:
    logger.warning("⚠️ ANGEL_TOTP is missing - login may fail")
# =========================================================
# SMART API IMPORT (GitHub compatible)
# =========================================================
sys.path.append(os.getcwd())   # <-- CRITICAL FIX
from SmartApi.smartConnect import SmartConnect

# =========================================================
# CONFIG
# =========================================================
API_SLEEP = 1.0
MAX_RETRIES = 3
MAX_WORKERS = 3
IST = timezone(timedelta(hours=5, minutes=30))

WEEKS_FOR_RANGE = 6

# Symbol configuration
SYMBOL_CONFIG = {
    "BANKNIFTY": {
        "token": "99926009",
        "strike_multiple": 100,
        "round_function": 100,
        "buffer": 1000
    },
    "FINNIFTY": {
        "token": "99926037",
        "strike_multiple": 50,
        "round_function": 50,
        "buffer": 500
    },
    "MIDCPNIFTY": {
        "token": "99926074",
        "strike_multiple": 25,
        "round_function": 25,
        "buffer": 150
    }
}

# =========================================================
# THREAD SAFE GLOBALS
# =========================================================
success_list = []
failed_list = []
failed_details = []
zip_lock = Lock()
counter_lock = Lock()
processed_counter = 0
total_symbols = 0

# =========================================================
# UTILS
# =========================================================
def round_down_to_multiple(price, multiple):
    return math.floor(price / multiple) * multiple

def round_up_to_multiple(price, multiple):
    return math.ceil(price / multiple) * multiple

# =========================================================
# HISTORICAL DATA FOR EACH SYMBOL
# =========================================================
def get_historical_data(smart_api, symbol_token, weeks=6):
    try:
        to_date = datetime.now(IST)
        from_date = to_date - timedelta(weeks=weeks)

        params = {
            "exchange": "NSE",
            "symboltoken": symbol_token,
            "interval": "ONE_DAY",
            "fromdate": from_date.strftime("%Y-%m-%d 09:15"),
            "todate": to_date.strftime("%Y-%m-%d %H:%M"),
        }

        resp = smart_api.getCandleData(params)

        if resp and resp.get("status") and resp.get("data"):
            df = pd.DataFrame(
                resp["data"],
                columns=["Date", "Open", "High", "Low", "Close", "Volume"]
            )
            df["Date"] = pd.to_datetime(df["Date"])
            return {
                "min_low": df["Low"].min(),
                "max_high": df["High"].max(),
                "current_close": df["Close"].iloc[-1]
            }

    except Exception as e:
        logger.error(f"{symbol_token} historical error: {e}")

    return None


def get_ltp(smart_api, symbol_token):
    try:
        params = {
            "exchange": "NSE",
            "symboltoken": symbol_token,
            "interval": "ONE_MINUTE",
            "fromdate": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d 09:15"),
            "todate": datetime.now().strftime("%Y-%m-%d %H:%M"),
        }
        resp = smart_api.getCandleData(params)
        if resp and resp.get("status") and resp.get("data"):
            return resp["data"][-1][4]
    except:
        pass
    return None


def calculate_strike_range(smart_api, symbol_config, buffer=None):
    if buffer is None:
        buffer = symbol_config["buffer"]
    
    hist = get_historical_data(smart_api, symbol_config["token"], WEEKS_FOR_RANGE)
    if not hist:
        logger.error(f"❌ Historical data unavailable for {symbol_config['token']}")
        return None, None

    multiple = symbol_config["round_function"]
    start = round_down_to_multiple(hist["min_low"] - buffer, multiple)
    end = round_up_to_multiple(hist["max_high"] + buffer, multiple)

    return max(0, start), end


# =========================================================
# SYMBOL MASTER
# =========================================================
def load_symbol_master():
    url = "https://api.shoonya.com/NFO_symbols.txt.zip"
    r = requests.get(url, timeout=60)
    r.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        with z.open(z.namelist()[0]) as f:
            content = "\n".join(
                line.rstrip(",") for line in f.read().decode().splitlines()
            )
            return pd.read_csv(io.StringIO(content))


def is_today_expiry(df, symbol):
    today = datetime.now(IST).date()
    df = df[(df["Symbol"] == symbol) & (df["Instrument"] == "OPTIDX")].copy()
    df["ExpiryDate"] = pd.to_datetime(df["Expiry"], format="%d-%b-%Y").dt.date
    return (today in df["ExpiryDate"].values), today


def get_option_symbols(df, symbol, expiry_date, start, end, strike_multiple):
    expiry = expiry_date.strftime("%d-%b-%Y").upper()
    df = df[
        (df["Symbol"] == symbol) &
        (df["Instrument"] == "OPTIDX") &
        (df["Expiry"] == expiry)
    ].copy()

    df["StrikePrice"] = pd.to_numeric(df["StrikePrice"], errors="coerce")

    return df[
        (df["StrikePrice"] >= start) &
        (df["StrikePrice"] <= end) &
        (df["StrikePrice"] % strike_multiple == 0)
    ]


# =========================================================
# TELEGRAM UPLOAD
# =========================================================
def send_zip_to_telegram(zip_bytes, name):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(zip_bytes)
        path = tmp.name

    try:
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=2, status_forcelist=[429,500,502,503])
        session.mount("https://", HTTPAdapter(max_retries=retries))

        with open(path, "rb") as f:
            r = session.post(
                url,
                data={"chat_id": TELEGRAM_CHAT_ID},
                files={"document": (name, f)},
                timeout=(30, 600),
            )
        r.raise_for_status()
        logger.info(f"✅ Sent {name} to Telegram")
        return True
    except Exception as e:
        logger.error(f"Telegram error for {name}: {e}")
        return False
    finally:
        os.remove(path)


# =========================================================
# CANDLE DOWNLOAD
# =========================================================
def get_candles_with_retry(smart, params):
    for i in range(MAX_RETRIES):
        try:
            r = smart.getCandleData(params)
            if r and r.get("status"):
                return r
            time.sleep((i + 1) * 5)
        except:
            time.sleep((i + 1) * 5)
    return None


def download_symbol(args):
    smart, row, FROM, TO = args
    symbol = row["TradingSymbol"]
    token = str(row["Token"])

    params = {
        "exchange": "NFO",
        "symboltoken": token,
        "interval": "ONE_MINUTE",
        "fromdate": FROM,
        "todate": TO,
    }

    r = get_candles_with_retry(smart, params)
    if r and r.get("data"):
        df = pd.DataFrame(
            r["data"],
            columns=["Date","Open","High","Low","Close","Volume"]
        )
        df["Date"] = pd.to_datetime(df["Date"]).dt.tz_localize(None)
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            df.to_excel(w, index=False)
        return symbol, buf.getvalue(), None

    return symbol, None, "No data"


# =========================================================
# PROCESS SINGLE INDEX
# =========================================================
def process_index(smart, df_master, symbol_name, symbol_config):
    """Process a single index and return zip bytes if successful"""
    logger.info(f"🔍 Processing {symbol_name}...")
    
    # Check if today is expiry
    is_expiry, expiry = is_today_expiry(df_master, symbol_name)
    if not is_expiry:
        logger.info(f"📅 Not {symbol_name} expiry day. Skipping.")
        return None
    
    logger.info(f"✅ Today is {symbol_name} expiry day: {expiry}")
    
    # Calculate strike range
    start, end = calculate_strike_range(smart, symbol_config)
    if start is None or end is None:
        logger.error(f"❌ Could not calculate strike range for {symbol_name}")
        return None
    
    logger.info(f"📊 {symbol_name} strike range: {start} to {end}")
    
    # Get option symbols
    df = get_option_symbols(
        df_master, 
        symbol_name, 
        expiry, 
        start, 
        end, 
        symbol_config["strike_multiple"]
    )
    
    if df.empty:
        logger.warning(f"⚠️ No option symbols found for {symbol_name}")
        return None
    
    logger.info(f"📈 Found {len(df)} option symbols for {symbol_name}")
    
    # Prepare date range
    FROM = (expiry - timedelta(days=90)).strftime("%Y-%m-%d 09:15")
    TO = expiry.strftime("%Y-%m-%d 15:30")
    
    # Prepare arguments for parallel download
    args = [(smart, r, FROM, TO) for _, r in df.iterrows()]
    
    # Create zip buffer
    zip_buf = io.BytesIO()
    
    # Track success/failure for this index
    index_success = []
    index_failed = []
    
    # Download symbols in parallel
    with concurrent.futures.ThreadPoolExecutor(MAX_WORKERS) as ex:
        futures = {ex.submit(download_symbol, arg): arg[0] for arg in args}
        
        for future in concurrent.futures.as_completed(futures):
            symbol, data, err = future.result()
            if data:
                with zip_lock:
                    with zipfile.ZipFile(zip_buf, "a") as zf:
                        zf.writestr(f"{symbol}.xlsx", data)
                index_success.append(symbol)
            else:
                index_failed.append(symbol)
                failed_details.append((symbol, err))
    
    # Update global counters
    with counter_lock:
        success_list.extend(index_success)
        failed_list.extend(index_failed)
    
    if index_success:
        zip_buf.seek(0)
        logger.info(f"✅ Downloaded {len(index_success)} symbols for {symbol_name}")
        return zip_buf.read()
    else:
        logger.warning(f"⚠️ No data downloaded for {symbol_name}")
        return None


# =========================================================
# MAIN
# =========================================================
def main():
    # Initialize API
    smart = SmartConnect(api_key=ANGEL_API_KEY)
    totp = pyotp.TOTP(ANGEL_TOTP).now()
    login = smart.generateSession(ANGEL_CLIENT_ID, ANGEL_PIN, totp)
    if not login or not login.get("status"):
        raise RuntimeError("Login failed")
    
    logger.info("✅ Login successful")
    
    # Load symbol master once
    df_master = load_symbol_master()
    logger.info("✅ Symbol master loaded")
    
    # Process each index
    for symbol_name, symbol_config in SYMBOL_CONFIG.items():
        try:
            zip_bytes = process_index(smart, df_master, symbol_name, symbol_config)
            
            if zip_bytes:
                # Send to Telegram
                filename = f"{symbol_name}_expiry_{datetime.now(IST).strftime('%d%m%y')}_1min.zip"
                send_zip_to_telegram(zip_bytes, filename)
                
                # Also save locally for debugging
                with open(filename, "wb") as f:
                    f.write(zip_bytes)
                logger.info(f"💾 Saved {filename} locally")
            else:
                logger.info(f"📭 No data to send for {symbol_name}")
                
        except Exception as e:
            logger.error(f"❌ Error processing {symbol_name}: {e}")
            traceback.print_exc()
            continue
    
    # Summary
    logger.info(f"✅ Script completed. Success: {len(success_list)}, Failed: {len(failed_list)}")
    
    if failed_list:
        logger.warning(f"Failed symbols: {failed_list[:10]}")  # Show first 10
        if len(failed_list) > 10:
            logger.warning(f"... and {len(failed_list) - 10} more")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        traceback.print_exc()
        sys.exit(1)
