import os
import requests
from datetime import datetime, timedelta
import pyotp
from SmartApi.smartConnect import SmartConnect

# =========================
# ENV VARIABLES (GitHub Secrets)
# =========================
API_KEY = os.getenv("ANGEL_API_KEY")
CLIENT_ID = os.getenv("ANGEL_CLIENT_ID")
PIN = os.getenv("ANGEL_PIN")
TOTP_SECRET = os.getenv("ANGEL_TOTP")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID_NIFTY")

# SBI token (NSE)
SBI_TOKEN = "3045"   # SBI EQ token

# =========================
# LOGIN
# =========================
totp = pyotp.TOTP(TOTP_SECRET).now()
smart = SmartConnect(api_key=API_KEY)

login = smart.generateSession(CLIENT_ID, PIN, totp)

if not login or not login.get("status"):
    raise Exception("Login failed")

# =========================
# FETCH 1 DAY OHLC
# =========================
today = datetime.now()
yesterday = today - timedelta(days=5)  # buffer for weekend

params = {
    "exchange": "NSE",
    "symboltoken": SBI_TOKEN,
    "interval": "ONE_DAY",
    "fromdate": yesterday.strftime("%Y-%m-%d 09:15"),
    "todate": today.strftime("%Y-%m-%d %H:%M"),
}

response = smart.getCandleData(params)

if not response or not response.get("data"):
    raise Exception("No data received")

# Get latest candle
candle = response["data"][-1]

date, open_, high, low, close, volume = candle

# =========================
# TELEGRAM MESSAGE
# =========================
message = f"""
📊 SBI Daily OHLC

Date: {date}
Open: {open_}
High: {high}
Low: {low}
Close: {close}
Volume: {volume}
"""

url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

requests.post(url, data={
    "chat_id": CHAT_ID,
    "text": message
})

print("✅ Message sent to Telegram")
