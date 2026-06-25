import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ALCHEMY_WS_URL = os.getenv("ALCHEMY_WS_URL")
ALCHEMY_URL = os.getenv("ALCHEMY_URL")

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "tx_radar")

try:
    TARGET_CONFIRMATIONS = int(os.getenv("TARGET_CONFIRMATIONS", 12))
except ValueError:
    TARGET_CONFIRMATIONS = 12
