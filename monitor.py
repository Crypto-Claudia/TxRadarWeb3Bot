import asyncio
import json
import logging
import html
import websockets
from web3 import AsyncWeb3, AsyncHTTPProvider
from web3 import Web3
from telegram import Bot
from telegram.error import TelegramError
from database import (
    get_all_unique_addresses,
    get_users_by_address,
    transaction_exists,
    get_transaction_status,
    add_tracked_transaction,
    update_transaction_status,
    get_transactions_by_status
)

logger = logging.getLogger(__name__)

# Standard ERC20 ABI interface for metadata querying
ERC20_ABI = [
    {
        "constant": True,
        "inputs": [],
        "name": "symbol",
        "outputs": [{"name": "", "type": "string"}],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": True,
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    }
]

def hex_to_int(val) -> int:
    """Helper to convert hex values (prefixed with 0x) or integer strings to Python integers."""
    if isinstance(val, str) and val.startswith("0x"):
        return int(val, 16)
    return int(val) if val is not None else 0

def escape_html(text) -> str:
    """Safely escape text for HTML parsing in Telegram."""
    if text is None:
        return ""
    return html.escape(str(text))

def pad_address_to_topic(address: str) -> str:
    """Pads a standard 20-byte Ethereum address to a 32-byte WebSocket topic parameter."""
    if not address:
        return "0x" + "0" * 64
    addr_clean = address.lower().replace("0x", "")
    return "0x" + addr_clean.rjust(64, "0")

def decode_address_from_topic(topic: str) -> str:
    """Decodes a 32-byte WebSocket topic parameter back to a standard 20-byte Ethereum address."""
    if not topic:
        return None
    topic_str = str(topic).lower()
    return "0x" + topic_str[-40:]

def parse_erc20_transfer(input_data: str):
    """
    Decodes transaction input data for standard ERC20 transfer methods.
    
    1. transfer(address,uint256) -> Selector: 0xa9059cbb
    2. transferFrom(address,address,uint256) -> Selector: 0x23b872dd
    
    Returns: (parsed_from, parsed_to, value) or None if input does not match selectors.
    """
    if not input_data or len(input_data) < 10:
        return None
        
    selector = input_data[:10].lower()
    
    # transfer(address,uint256)
    # Params: recipient (32 bytes), amount (32 bytes)
    if selector == "0xa9059cbb":
        if len(input_data) < 138: # 10 + 64 + 64
            return None
        recipient = "0x" + input_data[34:74].lower()
        amount = int(input_data[74:138], 16)
        return (None, recipient, amount)
        
    # transferFrom(address,address,uint256)
    # Params: sender (32 bytes), recipient (32 bytes), amount (32 bytes)
    elif selector == "0x23b872dd":
        if len(input_data) < 202: # 10 + 64 + 64 + 64
            return None
        sender = "0x" + input_data[34:74].lower()
        recipient = "0x" + input_data[98:138].lower()
        amount = int(input_data[138:202], 16)
        return (sender, recipient, amount)
        
    return None


class TokenCache:
    """Asynchronous on-chain metadata resolver and cache for ERC20 contracts."""
    def __init__(self, w3: AsyncWeb3):
        self.w3 = w3
        self.cache = {
            "eth": {"symbol": "ETH", "decimals": 18},
            "0xdac17f958d2ee523a2206206994597c13d831ec7": {"symbol": "USDT", "decimals": 6},
            "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": {"symbol": "USDC", "decimals": 6},
            "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": {"symbol": "WBTC", "decimals": 8},
        }
        self.lock = asyncio.Lock()

    async def get_token_info(self, contract_address: str):
        addr_lower = contract_address.lower()
        if addr_lower in self.cache:
            return self.cache[addr_lower]
            
        async with self.lock:
            if addr_lower in self.cache:
                return self.cache[addr_lower]
                
            try:
                checksum_addr = Web3.to_checksum_address(contract_address)
                contract = self.w3.eth.contract(address=checksum_addr, abi=ERC20_ABI)
                
                # Fetch symbol and decimals in parallel
                symbol, decimals = await asyncio.gather(
                    contract.functions.symbol().call(),
                    contract.functions.decimals().call()
                )
                self.cache[addr_lower] = {"symbol": str(symbol), "decimals": int(decimals)}
                logger.info(f"Resolved new ERC20 token: {symbol} ({decimals} decimals) at {contract_address}")
            except Exception as e:
                logger.warning(f"Could not read metadata for contract {contract_address}: {e}. Fallback applied.")
                # Default fallback (uses truncated address as symbol)
                self.cache[addr_lower] = {"symbol": contract_address[:8] + "...", "decimals": 18}
                
            return self.cache[addr_lower]


class TxMonitor:
    def __init__(self, ws_url: str, http_url: str, db_pool, bot: Bot, addresses_changed_event: asyncio.Event, target_confirmations: int):
        self.ws_url = ws_url
        self.http_url = http_url
        self.db_pool = db_pool
        self.bot = bot
        self.addresses_changed_event = addresses_changed_event
        self.target_confirmations = target_confirmations
        self.w3 = AsyncWeb3(AsyncHTTPProvider(http_url))
        self.token_cache = TokenCache(self.w3)
        self.current_addresses = set()
        
    async def start(self):
        """Starts the monitoring loop as a concurrent async task."""
        logger.info("Starting TxMonitor...")
        asyncio.create_task(self.ws_monitor_loop())

    async def ws_monitor_loop(self):
        """Main loop that handles WebSocket connections and subscriptions."""
        while True:
            try:
                # 1. Fetch monitored addresses from DB
                self.current_addresses = await get_all_unique_addresses(self.db_pool)
                if not self.current_addresses:
                    logger.info("No addresses to monitor. Waiting for user registrations...")
                    await self.addresses_changed_event.wait()
                    self.addresses_changed_event.clear()
                    continue
                
                logger.info(f"Connecting to Alchemy WS with {len(self.current_addresses)} unique addresses...")
                async with websockets.connect(self.ws_url) as ws:
                    # 2. Subscribe
                    await self.subscribe(ws, list(self.current_addresses))
                    logger.info("Successfully subscribed to transaction filters and ERC20 event logs.")
                    
                    # 3. Read loop
                    while True:
                        ws_msg_task = asyncio.create_task(ws.recv())
                        addr_change_task = asyncio.create_task(self.addresses_changed_event.wait())
                        
                        done, pending = await asyncio.wait(
                            [ws_msg_task, addr_change_task],
                            return_when=asyncio.FIRST_COMPLETED
                        )
                        
                        # Cancel unused tasks to prevent leaks
                        for task in pending:
                            task.cancel()
                            
                        if addr_change_task in done:
                            self.addresses_changed_event.clear()
                            logger.info("Monitored address updates detected. Re-establishing subscriptions...")
                            break
                            
                        if ws_msg_task in done:
                            try:
                                message_str = ws_msg_task.result()
                                data = json.loads(message_str)
                                await self.handle_ws_message(data)
                            except Exception as e:
                                logger.error(f"Error handling WS message: {e}")
                                
            except (websockets.exceptions.WebSocketException, ConnectionRefusedError, Exception) as e:
                logger.error(f"WS connection error: {e}. Reconnecting in 5 seconds...")
                await asyncio.sleep(5)

    async def subscribe(self, ws, addresses):
        """Helper to batch register addresses on Alchemy stream (both native and ERC20 logs)."""
        chunk_size = 900
        for i in range(0, len(addresses), chunk_size):
            chunk = addresses[i:i + chunk_size]
            
            # A. Native pending transaction monitoring
            pending_req = {
                "jsonrpc": "2.0",
                "id": i + 1000,
                "method": "eth_subscribe",
                "params": [
                    "alchemy_pendingTransactions",
                    {
                        "fromAddress": chunk,
                        "toAddress": chunk,
                        "hashesOnly": False
                    }
                ]
            }
            await ws.send(json.dumps(pending_req))
            
            # B. Native mined transaction monitoring
            addresses_filter = []
            for addr in chunk:
                addresses_filter.append({"from": addr})
                addresses_filter.append({"to": addr})
                
            mined_req = {
                "jsonrpc": "2.0",
                "id": i + 2000,
                "method": "eth_subscribe",
                "params": [
                    "alchemy_minedTransactions",
                    {
                        "addresses": addresses_filter,
                        "hashesOnly": False
                    }
                ]
            }
            await ws.send(json.dumps(mined_req))
            
            # C. ERC20 Transfer logs - monitoring sends (from)
            padded_addresses = [pad_address_to_topic(addr) for addr in chunk]
            
            erc20_send_req = {
                "jsonrpc": "2.0",
                "id": i + 3000,
                "method": "logs",
                "params": [
                    "logs",
                    {
                        "topics": [
                            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef", # Transfer topic
                            padded_addresses,
                            None
                        ]
                    }
                ]
            }
            await ws.send(json.dumps(erc20_send_req))
            
            # D. ERC20 Transfer logs - monitoring receipts (to)
            erc20_recv_req = {
                "jsonrpc": "2.0",
                "id": i + 4000,
                "method": "logs",
                "params": [
                    "logs",
                    {
                        "topics": [
                            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                            None,
                            padded_addresses
                        ]
                    }
                ]
            }
            await ws.send(json.dumps(erc20_recv_req))

    async def handle_ws_message(self, data):
        """Filters stream outputs and redirects them to the transaction processor."""
        if "method" not in data or data["method"] != "eth_subscription":
            return
            
        params = data.get("params", {})
        result = params.get("result", {})
        if not result:
            return
            
        # Detect if it's an event log (ERC20 transfer logs)
        if "topics" in result and "address" in result:
            await self.process_erc20_log(result)
        elif "transaction" in result:
            # Mined transaction (alchemy_minedTransactions wraps it in result.transaction)
            tx = result["transaction"]
            removed = result.get("removed", False)
            if removed:
                return
            await self.process_transaction(tx, status='confirmed')
        else:
            # Pending transaction (alchemy_pendingTransactions returns transaction object in result)
            await self.process_transaction(result, status='pending')

    async def process_erc20_log(self, log):
        """Processes and logs mined ERC20 logs, preventing duplicates."""
        topics = log.get("topics", [])
        if len(topics) < 3:
            return
            
        tx_hash = log.get("transactionHash")
        contract_addr = log.get("address")
        from_addr = decode_address_from_topic(topics[1])
        to_addr = decode_address_from_topic(topics[2])
        value_hex = log.get("data", "0x0")
        block_num_hex = log.get("blockNumber")
        
        if not tx_hash or not contract_addr or not from_addr or not to_addr:
            return
            
        value_wei = hex_to_int(value_hex)
        block_num = hex_to_int(block_num_hex) if block_num_hex else None
        
        # Check database to prevent double-logging from logs vs transaction streams
        current_status = await get_transaction_status(self.db_pool, tx_hash)
        if current_status == 'confirmed':
            return
            
        # Fetch token metadata details
        token_info = await self.token_cache.get_token_info(contract_addr)
        
        # Log to DB
        await add_tracked_transaction(
            self.db_pool,
            tx_hash=tx_hash,
            from_address=from_addr,
            to_address=to_addr,
            value_wei=value_wei,
            status='confirmed',
            block_number=block_num,
            token_symbol=token_info["symbol"],
            token_decimals=token_info["decimals"]
        )
        
        # Notify
        await self.notify_users(
            tx_hash=tx_hash,
            from_addr=from_addr,
            to_addr=to_addr,
            value_wei=value_wei,
            status='confirmed',
            block_num=block_num,
            token_symbol=token_info["symbol"],
            token_decimals=token_info["decimals"]
        )

    async def process_transaction(self, tx, status: str):
        """Saves transaction state in DB and distributes notifications to subscribers."""
        tx_hash = tx.get("hash")
        from_addr = tx.get("from")
        to_addr = tx.get("to")
        value_hex = tx.get("value", "0x0")
        input_data = tx.get("input", "")
        block_num_hex = tx.get("blockNumber")
        
        if not tx_hash or not from_addr:
            return
            
        value_wei = hex_to_int(value_hex)
        block_num = hex_to_int(block_num_hex) if block_num_hex else None
        
        # Evaluate if this is an ERC20 Transfer transaction by parsing input bytes
        erc20_info = parse_erc20_transfer(input_data)
        
        if erc20_info:
            # It's an ERC20 Transfer transaction!
            parsed_from, parsed_to, parsed_value = erc20_info
            
            # Sender address resolution
            sender = parsed_from if parsed_from else from_addr
            recipient = parsed_to
            contract_addr = to_addr # The contract being called is the transaction 'to'
            
            # Check if this ERC20 transaction relates to any of our monitored addresses
            if sender.lower() not in self.current_addresses and recipient.lower() not in self.current_addresses:
                return
                
            # Fetch token metadata details
            token_info = await self.token_cache.get_token_info(contract_addr)
            exists = await transaction_exists(self.db_pool, tx_hash)
            
            if status == 'pending':
                if exists:
                    return
                await add_tracked_transaction(
                    self.db_pool,
                    tx_hash=tx_hash,
                    from_address=sender,
                    to_address=recipient,
                    value_wei=parsed_value,
                    status='pending',
                    token_symbol=token_info["symbol"],
                    token_decimals=token_info["decimals"]
                )
                await self.notify_users(
                    tx_hash=tx_hash,
                    from_addr=sender,
                    to_addr=recipient,
                    value_wei=parsed_value,
                    status='pending',
                    block_num=block_num,
                    token_symbol=token_info["symbol"],
                    token_decimals=token_info["decimals"]
                )
                
            elif status == 'confirmed':
                current_status = await get_transaction_status(self.db_pool, tx_hash)
                if current_status == 'confirmed':
                    return
                    
                await add_tracked_transaction(
                    self.db_pool,
                    tx_hash=tx_hash,
                    from_address=sender,
                    to_address=recipient,
                    value_wei=parsed_value,
                    status='confirmed',
                    block_number=block_num,
                    token_symbol=token_info["symbol"],
                    token_decimals=token_info["decimals"]
                )
                await self.notify_users(
                    tx_hash=tx_hash,
                    from_addr=sender,
                    to_addr=recipient,
                    value_wei=parsed_value,
                    status='confirmed',
                    block_num=block_num,
                    token_symbol=token_info["symbol"],
                    token_decimals=token_info["decimals"]
                )
        else:
            # Native ETH transaction
            # Ensure it touches our monitored addresses
            if from_addr.lower() not in self.current_addresses and (not to_addr or to_addr.lower() not in self.current_addresses):
                return
                
            exists = await transaction_exists(self.db_pool, tx_hash)
            
            if status == 'pending':
                if exists:
                    return
                await add_tracked_transaction(
                    self.db_pool,
                    tx_hash=tx_hash,
                    from_address=from_addr,
                    to_address=to_addr,
                    value_wei=value_wei,
                    status='pending',
                    token_symbol='ETH',
                    token_decimals=18
                )
                await self.notify_users(
                    tx_hash=tx_hash,
                    from_addr=from_addr,
                    to_addr=to_addr,
                    value_wei=value_wei,
                    status='pending',
                    block_num=block_num,
                    token_symbol='ETH',
                    token_decimals=18
                )
                
            elif status == 'confirmed':
                current_status = await get_transaction_status(self.db_pool, tx_hash)
                if current_status == 'confirmed':
                    return
                    
                await add_tracked_transaction(
                    self.db_pool,
                    tx_hash=tx_hash,
                    from_address=from_addr,
                    to_address=to_addr,
                    value_wei=value_wei,
                    status='confirmed',
                    block_number=block_num,
                    token_symbol='ETH',
                    token_decimals=18
                )
                await self.notify_users(
                    tx_hash=tx_hash,
                    from_addr=from_addr,
                    to_addr=to_addr,
                    value_wei=value_wei,
                    status='confirmed',
                    block_num=block_num,
                    token_symbol='ETH',
                    token_decimals=18
                )

    async def notify_users(self, tx_hash: str, from_addr: str, to_addr: str, value_wei: int, status: str, block_num: int = None, token_symbol: str = 'ETH', token_decimals: int = 18):
        """Constructs HTML templates and triggers Telegram messages for each active user."""
        # 1. Fetch users monitoring from_addr and to_addr
        from_users = await get_users_by_address(self.db_pool, from_addr)
        to_users = await get_users_by_address(self.db_pool, to_addr) if to_addr else []
        
        # 2. Map users by chat_id
        # Structure: chat_id -> { "from_info": user_info_dict, "to_info": user_info_dict, "role": 'sender' | 'receiver' | 'both' }
        subscribers = {}
        for user in from_users:
            chat_id = user['chat_id']
            subscribers[chat_id] = {
                "from_info": user,
                "to_info": None,
                "role": "sender"
            }
            
        for user in to_users:
            chat_id = user['chat_id']
            if chat_id in subscribers:
                subscribers[chat_id]["to_info"] = user
                subscribers[chat_id]["role"] = "both"
            else:
                subscribers[chat_id] = {
                    "from_info": None,
                    "to_info": user,
                    "role": "receiver"
                }
                
        if not subscribers:
            return
            
        # 3. Format value and status
        # Divide by 10 ** decimals to get float representation
        value_formatted = value_wei / (10 ** token_decimals)
        if value_formatted.is_integer():
            val_str = f"{int(value_formatted):,}"
        else:
            val_str = f"{value_formatted:,.6f}".rstrip('0').rstrip('.')
            
        # Status header formatting
        if status == 'pending':
            status_header = f"⏳ <b>[Pending] Transaction Detected ({token_symbol})</b>"
        else: # confirmed
            status_header = f"✅ <b>[Confirmed] Transaction Confirmed ({token_symbol})</b>"
            
        tx_link = f'<a href="https://etherscan.io/tx/{tx_hash}">{tx_hash[:10]}...{tx_hash[-8:]}</a>'
        
        # 4. Dispatch messages to each subscriber with their private label details
        for chat_id, data in subscribers.items():
            role = data["role"]
            from_info = data["from_info"]
            to_info = data["to_info"]
            
            # Format text depending on User Role
            if role == "both":
                from_label = f" ({escape_html(from_info['label'])})" if from_info['label'] else ""
                to_label = f" ({escape_html(to_info['label'])})" if to_info['label'] else ""
                msg_body = (
                    f"🔄 <b>Transfer between my wallets detected ({token_symbol})</b>\n"
                    f"From: <code>{Web3.to_checksum_address(from_addr)}</code>{from_label}\n"
                    f"To: <code>{Web3.to_checksum_address(to_addr)}</code>{to_label}"
                )
            elif role == "sender":
                from_label = f" ({escape_html(from_info['label'])})" if from_info['label'] else ""
                dest_str = f"<code>{Web3.to_checksum_address(to_addr)}</code>" if to_addr else "None (Contract Creation)"
                msg_body = (
                    f"💸 <b>{token_symbol} Withdrawal Detected</b>\n"
                    f"From: <code>{Web3.to_checksum_address(from_addr)}</code>{from_label}\n"
                    f"To: {dest_str}"
                )
            else: # receiver
                to_label = f" ({escape_html(to_info['label'])})" if to_info['label'] else ""
                msg_body = (
                    f"📩 <b>{token_symbol} Deposit Detected</b>\n"
                    f"From: <code>{Web3.to_checksum_address(from_addr)}</code>\n"
                    f"To: <code>{Web3.to_checksum_address(to_addr)}</code>{to_label}"
                )
                
            full_message = (
                f"{status_header}\n\n"
                f"{msg_body}\n\n"
                f"💰 <b>Amount:</b> {val_str} {token_symbol}\n"
            )
            if block_num:
                full_message += f"📦 <b>Block Number:</b> {block_num}\n"
            full_message += f"🔗 <b>Tx Hash:</b> {tx_link}"
            
            try:
                await self.bot.send_message(
                    chat_id=chat_id,
                    text=full_message,
                    parse_mode="HTML",
                    disable_web_page_preview=True
                )
            except TelegramError as e:
                logger.error(f"Failed to send Telegram notification to {chat_id}: {e}")
