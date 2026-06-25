import re
import logging
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from web3 import Web3
from database import register_user, add_monitored_address, remove_monitored_address, get_user_addresses

logger = logging.getLogger(__name__)

# Regex for standard Ethereum Address validation
ETH_ADDRESS_REGEX = re.compile(r"^0x[a-fA-F0-9]{40}$")

def is_valid_eth_address(address: str) -> bool:
    return bool(ETH_ADDRESS_REGEX.match(address))

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    username = update.effective_user.username
    db_pool = context.bot_data['db_pool']
    
    await register_user(db_pool, chat_id, username)
    
    welcome_text = (
        "🛰️ **Welcome to TxRadar!**\n\n"
        "This bot detects real-time deposits and withdrawals of Ethereum wallets and sends alerts.\n"
        "It tracks transactions from the Pending state to being Mined in a block and final Confirmation.\n\n"
        "**Available Commands:**\n"
        "➕ /add `<address>` `[label]` - Add wallet address to monitor (label is optional)\n"
        "➖ /remove `<address>` - Remove wallet address\n"
        "📋 /list - Check your monitored wallet addresses\n"
        "❓ /help - View help guide\n\n"
        "Example: `/add 0xd8da6bf26964af9d7eed9e03e53415d37aa96045 Vitalik`"
    )
    await update.message.reply_text(welcome_text, parse_mode="Markdown")

async def add_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    username = update.effective_user.username
    db_pool = context.bot_data['db_pool']
    addresses_changed_event = context.bot_data['addresses_changed_event']
    
    if not context.args:
        await update.message.reply_text(
            "❌ **Usage**: `/add <address> [label]`\nExample: `/add 0xd8da... Vitalik`",
            parse_mode="Markdown"
        )
        return
        
    address = context.args[0].strip()
    if not is_valid_eth_address(address):
        await update.message.reply_text("❌ Invalid Ethereum wallet address.")
        return
        
    checksum_addr = Web3.to_checksum_address(address)
    label = " ".join(context.args[1:]) if len(context.args) > 1 else None
    
    # Ensure user is registered first
    await register_user(db_pool, chat_id, username)
    
    try:
        await add_monitored_address(db_pool, chat_id, checksum_addr, label)
        # Signal the monitor to refresh addresses and re-subscribe
        addresses_changed_event.set()
        
        lbl_str = f" ({label})" if label else ""
        await update.message.reply_text(
            f"✅ **Address added successfully!**\n"
            f"Address: `{checksum_addr}`{lbl_str}\n"
            f"Real-time deposit/withdrawal alerts will start now.",
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error(f"Error in add_command: {e}")
        await update.message.reply_text("❌ An error occurred while adding the address.")

async def remove_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    db_pool = context.bot_data['db_pool']
    addresses_changed_event = context.bot_data['addresses_changed_event']
    
    if not context.args:
        await update.message.reply_text(
            "❌ **Usage**: `/remove <address>`\nExample: `/remove 0xd8da...`",
            parse_mode="Markdown"
        )
        return
        
    address = context.args[0].strip()
    if not is_valid_eth_address(address):
        await update.message.reply_text("❌ Invalid Ethereum wallet address.")
        return
        
    checksum_addr = Web3.to_checksum_address(address)
    
    try:
        success = await remove_monitored_address(db_pool, chat_id, checksum_addr)
        if success:
            addresses_changed_event.set()
            await update.message.reply_text(
                f"✅ **Address removed from monitoring list!**\nAddress: `{checksum_addr}`",
                parse_mode="Markdown"
            )
        else:
            await update.message.reply_text("❓ This address is not registered or not in your monitoring list.")
    except Exception as e:
        logger.error(f"Error in remove_command: {e}")
        await update.message.reply_text("❌ An error occurred while removing the address.")

async def list_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    db_pool = context.bot_data['db_pool']
    
    try:
        addresses = await get_user_addresses(db_pool, chat_id)
        if not addresses:
            await update.message.reply_text("📋 No monitored addresses registered. Try adding one with `/add`!")
            return
            
        text = "📋 **Monitored Wallets List**\n\n"
        for idx, addr_info in enumerate(addresses, 1):
            addr = addr_info['address']
            label = addr_info['label']
            checksum_addr = Web3.to_checksum_address(addr)
            lbl_str = f" - *{label}*" if label else ""
            text += f"{idx}. `{checksum_addr}`{lbl_str}\n"
            
        await update.message.reply_text(text, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Error in list_command: {e}")
        await update.message.reply_text("❌ An error occurred while loading the list.")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "🛰️ **TxRadar Help**\n\n"
        "Ethereum mainnet real-time deposit/withdrawal alert command guide:\n\n"
        "➕ /add `<address>` `[label]`\n"
        "  - Register a new address for monitoring. Specifying a label makes notifications easier to identify.\n\n"
        "➖ /remove `<address>`\n"
        "  - Cancel monitoring for the registered address.\n\n"
        "📋 /list\n"
        "  - Show all addresses and labels currently being monitored.\n\n"
        "❓ /help\n"
        "  - Show this help guide."
    )
    await update.message.reply_text(help_text, parse_mode="Markdown")

def build_bot_app(token: str, db_pool, addresses_changed_event):
    """
    Builds the Telegram Bot application, registers handlers, and initializes context data.
    """
    app = ApplicationBuilder().token(token).build()
    
    # Store pool and event in application context to be accessible inside commands
    app.bot_data['db_pool'] = db_pool
    app.bot_data['addresses_changed_event'] = addresses_changed_event
    
    # Add Command handlers
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("add", add_command))
    app.add_handler(CommandHandler("remove", remove_command))
    app.add_handler(CommandHandler("list", list_command))
    app.add_handler(CommandHandler("help", help_command))
    
    return app
