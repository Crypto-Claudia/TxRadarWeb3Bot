import asyncio
import logging
import sys
from database import init_db
from bot import build_bot_app
from monitor import TxMonitor
import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

async def main():
    logger.info("Initializing TxRadar Bot...")
    
    # 1. Initialize Database
    try:
        db_pool = await init_db(config)
    except Exception as e:
        logger.critical(f"Failed to initialize database: {e}")
        sys.exit(1)
        
    # 2. Event to notify monitor of address changes
    addresses_changed_event = asyncio.Event()
    
    # 3. Build and initialize Telegram Bot application
    application = build_bot_app(
        token=config.TELEGRAM_BOT_TOKEN,
        db_pool=db_pool,
        addresses_changed_event=addresses_changed_event
    )
    
    await application.initialize()
    await application.start()
    await application.updater.start_polling()
    logger.info("Telegram Bot started and polling.")
    
    # 4. Initialize and start transaction monitor
    monitor = TxMonitor(
        ws_url=config.ALCHEMY_WS_URL,
        http_url=config.ALCHEMY_URL,
        db_pool=db_pool,
        bot=application.bot,
        addresses_changed_event=addresses_changed_event,
        target_confirmations=config.TARGET_CONFIRMATIONS
    )
    await monitor.start()
    
    # Keep the main loop running until cancelled
    stop_event = asyncio.Event()
    
    try:
        # Wait for stop
        await stop_event.wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Shutdown signal received.")
    finally:
        logger.info("Cleaning up and shutting down...")
        
        # Stop Telegram Bot polling and updater
        if application.updater.running:
            await application.updater.stop()
        await application.stop()
        await application.shutdown()
        logger.info("Telegram Bot shut down.")
        
        # Close database pool
        db_pool.close()
        await db_pool.wait_closed()
        logger.info("Database pool closed. Goodbye!")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("TxRadar terminated.")
