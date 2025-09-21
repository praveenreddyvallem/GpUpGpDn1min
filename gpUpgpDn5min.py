import websocket
import json
import time
import csv
from datetime import datetime, timedelta, timezone
import socket
import logging
import requests

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", filename="btcusd_candles.log")
logger = logging.getLogger(__name__)

# Constants
WEBSOCKET_URLS = ["wss://socket.india.delta.exchange"]
SYMBOL = "BTCUSD"
CSV_FILE = "btcusd_realtime_candles.csv"
MAX_RECONNECT_ATTEMPTS = 5

# Define IST timezone (UTC+5:30)
IST = timezone(timedelta(hours=5, minutes=30))

# Telegram Config
TELEGRAM_TOKEN = "8137164045:AAF7uxGaEFU6BOQkhHHWiHPSOoJOUTbMM1k"
TELEGRAM_CHAT_ID = "5837597618"

# Send alert to Telegram
def send_telegram_alert(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        response = requests.post(url, data=data)
        if response.status_code == 200:
            print("✅ Telegram alert sent successfully")
        else:
            print(f"❌ Failed to send Telegram alert: {response.status_code}, {response.text}")
    except Exception as e:
        print(f"❌ Error sending Telegram alert: {e}")

# Initialize CSV
def init_csv():
    with open(CSV_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Time", "Open", "High", "Low", "Close", "Volume"])

# Append candle to CSV
def append_to_csv(candle):
    with open(CSV_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            datetime.fromtimestamp(candle["timestamp"] / 1_000_000, IST).strftime("%Y-%m-%d %H:%M:%S"),
            candle["open"], candle["high"], candle["low"], candle["close"], candle["volume"]
        ])

# Global Variables
last_candle = None
last_candle_start_time = None
previous_candle = None

# WebSocket message handler
def on_message(ws, message):
    global last_candle, last_candle_start_time, previous_candle
    try:
        data = json.loads(message)
        if data.get("type") == "candlestick_5m":  # 5m candles
            candle = data
            current_candle_start_time = candle["candle_start_time"]

            if last_candle_start_time is None:
                last_candle_start_time = current_candle_start_time
                last_candle = candle

            elif current_candle_start_time != last_candle_start_time:
                # Finalize last candle
                final_time = datetime.fromtimestamp(last_candle["timestamp"] / 1_000_000, IST).strftime("%Y-%m-%d %H:%M:%S")
                latest_open = float(last_candle["open"])
                latest_high = float(last_candle["high"])
                latest_low = float(last_candle["low"])
                latest_close = float(last_candle["close"])
                latest_vol = float(last_candle["volume"])
                ltp = latest_close  # last traded price = close

                print(f"Final 5m candle closed: Time: {final_time}, O:{latest_open}, H:{latest_high}, L:{latest_low}, C:{latest_close}, V:{latest_vol}")

                append_to_csv(last_candle)

                # Check conditions if previous candle exists
                if previous_candle:
                    prev_open = float(previous_candle["open"])
                    prev_close = float(previous_candle["close"])

                    # Bullish Signal
                    if (latest_open > prev_close) and (prev_open > prev_close) and (latest_close > latest_open):
                        alert_msg = (
                            f"🟢 *Bullish Signal Detected*\n\n"
                            f"🕒 Time: {final_time} IST\n"
                            f"💰 LTP: {ltp}\n"
                            f"O:{latest_open}, H:{latest_high}, L:{latest_low}, C:{latest_close}\n"
                            f"Prev O:{prev_open}, Prev C:{prev_close}"
                        )
                        send_telegram_alert(alert_msg)

                    # Bearish Signal
                    elif (latest_open < prev_close) and (prev_open < prev_close) and (latest_close < latest_open):
                        alert_msg = (
                            f"🔴 *Bearish Signal Detected*\n\n"
                            f"🕒 Time: {final_time} IST\n"
                            f"💰 LTP: {ltp}\n"
                            f"O:{latest_open}, H:{latest_high}, L:{latest_low}, C:{latest_close}\n"
                            f"Prev O:{prev_open}, Prev C:{prev_close}"
                        )
                        send_telegram_alert(alert_msg)

                # Update for next iteration
                previous_candle = last_candle
                last_candle_start_time = current_candle_start_time
                last_candle = candle
            else:
                last_candle = candle

        elif data.get("type") == "error":
            print(f"Server error: {data.get('message', 'Unknown error')}")

        elif data.get("type") == "subscriptions":
            print(f"Subscription confirmed: {data}")

    except Exception as e:
        print(f"❌ Error processing message: {e}")

# WebSocket error handler
def on_error(ws, error):
    print(f"Error: {error}")
    ws.close()

# WebSocket close handler
def on_close(ws, close_status_code, close_msg):
    print(f"Connection closed: {close_status_code}, {close_msg}")

# WebSocket open handler
def on_open(ws):
    subscribe_msg = {
        "type": "subscribe",
        "payload": {"channels": [{"name": "candlestick_5m", "symbols": [SYMBOL]}]}
    }
    ws.send(json.dumps(subscribe_msg))
    print(f"✅ Subscribed to 5-minute {SYMBOL} candles")

# Initialize
init_csv()

# WebSocket connection logic
for url in WEBSOCKET_URLS:
    attempt = 0
    while attempt < MAX_RECONNECT_ATTEMPTS:
        try:
            hostname = url.replace("wss://", "").split("/")[0]
            socket.getaddrinfo(hostname, 443)

            ws = websocket.WebSocketApp(
                url, on_message=on_message, on_error=on_error, on_close=on_close, on_open=on_open
            )
            ws.run_forever(ping_interval=60, ping_timeout=10)
            break
        except Exception as e:
            print(f"Connection error: {e}")
            attempt += 1
            if attempt < MAX_RECONNECT_ATTEMPTS:
                time.sleep(10)
