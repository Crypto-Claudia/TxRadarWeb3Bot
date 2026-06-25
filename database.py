import aiomysql
import logging

logger = logging.getLogger(__name__)

async def init_db(config):
    """
    Initializes the database: ensures the DB exists, creates tables, and returns a connection pool.
    """
    # 1. Connect without specifying db first to ensure it exists
    conn = await aiomysql.connect(
        host=config.MYSQL_HOST,
        port=config.MYSQL_PORT,
        user=config.MYSQL_USER,
        password=config.MYSQL_PASSWORD,
        autocommit=True
    )
    async with conn.cursor() as cur:
        await cur.execute(f"CREATE DATABASE IF NOT EXISTS {config.MYSQL_DB}")
    conn.close()
    await conn.ensure_closed()

    # 2. Connect with db name to create pool
    pool = await aiomysql.create_pool(
        host=config.MYSQL_HOST,
        port=config.MYSQL_PORT,
        user=config.MYSQL_USER,
        password=config.MYSQL_PASSWORD,
        db=config.MYSQL_DB,
        autocommit=True
    )

    # 3. Create tables
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            # Users table
            await cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                chat_id BIGINT PRIMARY KEY,
                username VARCHAR(100) NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """)
            
            # User monitored addresses table
            await cur.execute("""
            CREATE TABLE IF NOT EXISTS user_addresses (
                id INT AUTO_INCREMENT PRIMARY KEY,
                chat_id BIGINT NOT NULL,
                address VARCHAR(42) NOT NULL,
                label VARCHAR(100) NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (chat_id) REFERENCES users(chat_id) ON DELETE CASCADE,
                UNIQUE KEY idx_chat_id_address (chat_id, address),
                INDEX idx_address (address)
            )
            """)
            
            # Tracked transactions table
            await cur.execute("""
            CREATE TABLE IF NOT EXISTS tracked_transactions (
                tx_hash VARCHAR(66) PRIMARY KEY,
                from_address VARCHAR(42) NOT NULL,
                to_address VARCHAR(42) NULL,
                value_wei VARCHAR(78) NOT NULL,
                status VARCHAR(20) NOT NULL, -- 'pending', 'mined', 'confirmed'
                block_number BIGINT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                mined_at DATETIME NULL,
                confirmed_at DATETIME NULL,
                INDEX idx_status_block (status, block_number)
            )
            """)
            
            # Alter table for ERC20 token logging support
            try:
                await cur.execute("ALTER TABLE tracked_transactions ADD COLUMN token_symbol VARCHAR(20) DEFAULT NULL")
            except Exception:
                pass
            try:
                await cur.execute("ALTER TABLE tracked_transactions ADD COLUMN token_decimals INT DEFAULT 18")
            except Exception:
                pass
    logger.info("Database initialized successfully.")
    return pool

async def register_user(pool, chat_id, username):
    """
    Registers a new user or updates their username if it has changed.
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO users (chat_id, username)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE username = %s
                """,
                (chat_id, username, username)
            )

async def add_monitored_address(pool, chat_id, address, label=None):
    """
    Adds an address to monitor for a specific user.
    Address is stored in lowercase.
    """
    normalized_address = address.lower()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO user_addresses (chat_id, address, label)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE label = %s
                """,
                (chat_id, normalized_address, label, label)
            )

async def remove_monitored_address(pool, chat_id, address):
    """
    Removes an address from a user's monitored list.
    Returns True if an address was removed, False otherwise.
    """
    normalized_address = address.lower()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            affected = await cur.execute(
                "DELETE FROM user_addresses WHERE chat_id = %s AND address = %s",
                (chat_id, normalized_address)
            )
            return affected > 0

async def get_user_addresses(pool, chat_id):
    """
    Returns list of addresses monitored by the user.
    """
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                "SELECT address, label, created_at FROM user_addresses WHERE chat_id = %s ORDER BY created_at DESC",
                (chat_id,)
            )
            return await cur.fetchall()

async def get_all_unique_addresses(pool):
    """
    Returns a set of all unique lowercased addresses monitored by any user.
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT DISTINCT address FROM user_addresses")
            results = await cur.fetchall()
            return {row[0] for row in results}

async def get_users_by_address(pool, address):
    """
    Finds all users (with usernames and custom labels) monitoring a specific address.
    """
    if not address:
        return []
    normalized_address = address.lower()
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                SELECT u.chat_id, u.username, ua.address, ua.label
                FROM users u
                JOIN user_addresses ua ON u.chat_id = ua.chat_id
                WHERE ua.address = %s
                """,
                (normalized_address,)
            )
            return await cur.fetchall()

async def transaction_exists(pool, tx_hash):
    """
    Checks if a transaction hash is already tracked.
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT 1 FROM tracked_transactions WHERE tx_hash = %s LIMIT 1",
                (tx_hash,)
            )
            res = await cur.fetchone()
            return res is not None

async def add_tracked_transaction(pool, tx_hash, from_address, to_address, value_wei, status, block_number=None, token_symbol=None, token_decimals=18):
    """
    Saves/Updates a transaction in the database.
    """
    normalized_from = from_address.lower() if from_address else ""
    normalized_to = to_address.lower() if to_address else None
    
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            if status == 'confirmed':
                await cur.execute(
                    """
                    INSERT INTO tracked_transactions (tx_hash, from_address, to_address, value_wei, status, block_number, token_symbol, token_decimals, mined_at, confirmed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON DUPLICATE KEY UPDATE status = %s, block_number = %s, token_symbol = %s, token_decimals = %s, mined_at = COALESCE(mined_at, NOW()), confirmed_at = NOW()
                    """,
                    (tx_hash, normalized_from, normalized_to, str(value_wei), status, block_number, token_symbol, token_decimals,
                     status, block_number, token_symbol, token_decimals)
                )
            else:
                await cur.execute(
                    """
                    INSERT INTO tracked_transactions (tx_hash, from_address, to_address, value_wei, status, block_number, token_symbol, token_decimals)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE status = %s, block_number = %s, token_symbol = %s, token_decimals = %s
                    """,
                    (tx_hash, normalized_from, normalized_to, str(value_wei), status, block_number, token_symbol, token_decimals,
                     status, block_number, token_symbol, token_decimals)
                )

async def update_transaction_status(pool, tx_hash, status, block_number=None):
    """
    Updates the status of a tracked transaction and records timestamp.
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            if status == 'confirmed':
                await cur.execute(
                    """
                    UPDATE tracked_transactions 
                    SET status = %s, block_number = COALESCE(%s, block_number), mined_at = COALESCE(mined_at, NOW()), confirmed_at = NOW() 
                    WHERE tx_hash = %s
                    """,
                    (status, block_number, tx_hash)
                )
            else:
                await cur.execute(
                    "UPDATE tracked_transactions SET status = %s, block_number = COALESCE(%s, block_number) WHERE tx_hash = %s",
                    (status, block_number, tx_hash)
                )

async def get_transactions_by_status(pool, status):
    """
    Retrieves all tracked transactions with a specific status.
    """
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                """
                SELECT tx_hash, from_address, to_address, value_wei, status, block_number, token_symbol, token_decimals, created_at 
                FROM tracked_transactions 
                WHERE status = %s
                """,
                (status,)
            )
            return await cur.fetchall()

async def get_transaction_status(pool, tx_hash):
    """
    Returns the status of a tracked transaction, or None if it doesn't exist.
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT status FROM tracked_transactions WHERE tx_hash = %s",
                (tx_hash,)
            )
            res = await cur.fetchone()
            return res[0] if res else None

