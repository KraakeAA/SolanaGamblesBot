import 'dotenv/config';
import { Pool } from 'pg';
import express from 'express';
import TelegramBot from 'node-telegram-bot-api';
import {
    PublicKey,
    LAMPORTS_PER_SOL,
    Keypair,
    Transaction,
    SystemProgram,
    sendAndConfirmTransaction,
    ComputeBudgetProgram
} from '@solana/web3.js';
import bs58 from 'bs58';
import { randomBytes } from 'crypto';
import PQueue from 'p-queue';
// Assuming RateLimitedConnection is correctly implemented in this path
import RateLimitedConnection from './lib/solana-connection.js';

// --- Enhanced Environment Variable Checks ---
const REQUIRED_ENV_VARS = [
    'BOT_TOKEN',
    'DATABASE_URL',
    'BOT_PRIVATE_KEY', // Coinflip payouts
    'RACE_BOT_PRIVATE_KEY', // Race payouts
    'MAIN_WALLET_ADDRESS', // Coinflip deposits
    'RACE_WALLET_ADDRESS', // Race deposits
    'RPC_URL',
    'FEE_MARGIN' // Lamports buffer for payout fees
];

// Check for Railway-specific variables if deployed there
if (process.env.RAILWAY_ENVIRONMENT) {
    REQUIRED_ENV_VARS.push('RAILWAY_PUBLIC_DOMAIN');
}

// Validate environment variables
let missingVars = false;
REQUIRED_ENV_VARS.forEach((key) => {
    // Skip Railway check if not in Railway environment
    if (key === 'RAILWAY_PUBLIC_DOMAIN' && !process.env.RAILWAY_ENVIRONMENT) return;

    if (!process.env[key]) {
        console.error(`❌ Environment variable ${key} is missing.`);
        missingVars = true;
    }
});

if (missingVars) {
    console.error("⚠️ Please set all required environment variables. Exiting.");
    process.exit(1);
}

// Set default fee margin if not specified (e.g., 0.00005 SOL)
if (!process.env.FEE_MARGIN) {
    process.env.FEE_MARGIN = '5000'; // Default to 5000 lamports
}
console.log(`ℹ️ Using FEE_MARGIN: ${process.env.FEE_MARGIN} lamports`);

// --- Initialize Scalable Components ---
const app = express();

// 1. Enhanced Solana Connection with Rate Limiting
console.log("⚙️ Initializing scalable Solana connection...");
const solanaConnection = new RateLimitedConnection(process.env.RPC_URL, {
    maxConcurrent: 3,       // Max parallel requests
    retryBaseDelay: 600,     // Initial delay for retries (ms)
    commitment: 'confirmed',  // Default commitment level
    httpHeaders: {
        'Content-Type': 'application/json',
        'solana-client': `SolanaGamblesBot/2.0 (${process.env.RAILWAY_ENVIRONMENT ? 'railway' : 'local'})` // Client info
    },
    rateLimitCooloff: 10000,   // Pause duration after hitting rate limits (ms)
    disableRetryOnRateLimit: false // Rely on RateLimitedConnection's internal handling
});
console.log("✅ Scalable Solana connection initialized");

// 2. Message Processing Queue (for handling Telegram messages)
const messageQueue = new PQueue({
    concurrency: 5,   // Max concurrent messages processed
    timeout: 10000    // Max time per message task (ms)
});
console.log("✅ Message processing queue initialized");

// 3. Enhanced PostgreSQL Pool
console.log("⚙️ Setting up optimized PostgreSQL Pool...");
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    max: 15,                    // Max connections in pool
    min: 5,                     // Min connections maintained
    idleTimeoutMillis: 30000,   // Close idle connections after 30s
    connectionTimeoutMillis: 5000, // Timeout for acquiring connection
    ssl: process.env.NODE_ENV === 'production' ? {
        rejectUnauthorized: false // Necessary for some cloud providers like Heroku/Railway
    } : false
});
console.log("✅ PostgreSQL Pool created with optimized settings");

// 4. Simple Performance Monitor
const performanceMonitor = {
    requests: 0,
    errors: 0,
    startTime: Date.now(),
    logRequest(success) {
        this.requests++;
        if (!success) this.errors++;

        // Log stats every 50 requests
        if (this.requests % 50 === 0) {
            const uptime = (Date.now() - this.startTime) / 1000;
            const errorRate = this.requests > 0 ? (this.errors / this.requests * 100).toFixed(1) : 0;
            console.log(`
📊 Performance Metrics:
    - Uptime: ${uptime.toFixed(0)}s
    - Total Requests Handled: ${this.requests}
    - Error Rate: ${errorRate}%
            `);
        }
    }
};

// --- Database Initialization (Updated) ---
async function initializeDatabase() {
    console.log("⚙️ Initializing Database schema...");
    let client;
    try {
        client = await pool.connect();

        // Bets Table: Tracks individual game bets
        await client.query(`
            CREATE TABLE IF NOT EXISTS bets (
                id SERIAL PRIMARY KEY,                          -- Unique bet identifier
                user_id TEXT NOT NULL,                          -- Telegram User ID
                chat_id TEXT NOT NULL,                          -- Telegram Chat ID
                game_type TEXT NOT NULL,                        -- 'coinflip' or 'race'
                bet_details JSONB,                              -- Game-specific details (choice, horse, odds)
                expected_lamports BIGINT NOT NULL,              -- Amount user should send (in lamports)
                memo_id TEXT UNIQUE NOT NULL,                   -- Unique memo for payment tracking
                status TEXT NOT NULL,                           -- Bet status (e.g., 'awaiting_payment', 'completed_win_paid', 'error_...')
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),  -- When the bet was initiated
                expires_at TIMESTAMPTZ NOT NULL,                -- When the payment window closes
                paid_tx_signature TEXT UNIQUE,                  -- Signature of the user's payment transaction
                payout_tx_signature TEXT UNIQUE,                -- Signature of the bot's payout transaction (if win)
                processed_at TIMESTAMPTZ,                       -- When the bet was fully resolved
                fees_paid BIGINT,                               -- Estimated fees buffer associated with this bet
                priority INT DEFAULT 0                          -- Priority for processing (higher first)
            );
        `);

        // Wallets Table: Links Telegram User ID to their Solana wallet address
        await client.query(`
            CREATE TABLE IF NOT EXISTS wallets (
                user_id TEXT PRIMARY KEY,                       -- Telegram User ID
                wallet_address TEXT NOT NULL,                   -- User's Solana wallet address
                linked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),   -- When the wallet was first linked
                last_used_at TIMESTAMPTZ                        -- When the wallet was last used for a bet/payout
            );
        `);

        // Add columns using ALTER TABLE IF NOT EXISTS for backward compatibility
        // This ensures columns are added even if the table was created previously without them.
        await client.query(`ALTER TABLE bets ADD COLUMN IF NOT EXISTS priority INT DEFAULT 0;`);
        await client.query(`ALTER TABLE bets ADD COLUMN IF NOT EXISTS fees_paid BIGINT;`);


        // Add indexes for performance on frequently queried columns
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_status ON bets(status);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_user_id ON bets(user_id);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_expires_at ON bets(expires_at);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_memo_id ON bets(memo_id);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_priority ON bets(priority);`);

        console.log("✅ Database schema initialized/verified");
    } catch (err) {
        console.error("❌ Database initialization error:", err);
        throw err; // Re-throw to prevent startup if DB fails
    } finally {
        if (client) client.release(); // Release client back to the pool
    }
}

// --- Telegram Bot Initialization with Queue ---
console.log("⚙️ Initializing Telegram Bot...");
const bot = new TelegramBot(process.env.BOT_TOKEN, {
    polling: false, // Use webhooks in production (set later)
    request: {      // Adjust request options for stability
        timeout: 10000, // Request timeout: 10s
        agentOptions: {
            keepAlive: true,    // Reuse connections
            timeout: 60000      // Keep-alive timeout: 60s
        }
    }
});

// Wrap all incoming message handlers in the processing queue
bot.on('message', (msg) => {
    messageQueue.add(() => handleMessage(msg))
        .catch(err => {
            console.error("⚠️ Message processing queue error:", err);
            performanceMonitor.logRequest(false); // Log error
        });
});

console.log("✅ Telegram Bot initialized");

// --- Express Setup for Webhook and Health Check ---
app.use(express.json({
    limit: '10kb', // Limit payload size
    verify: (req, res, buf) => { // Keep raw body if needed for signature verification (not used here)
        req.rawBody = buf;
    }
}));

// Health check endpoint
app.get('/', (req, res) => {
    performanceMonitor.logRequest(true);
    res.status(200).json({
        status: 'ok',
        timestamp: new Date().toISOString(),
        version: '2.0.2', // Bot version (incremented for logging change)
        queueStats: { // Report queue status
            pending: messageQueue.size + paymentProcessor.highPriorityQueue.size + paymentProcessor.normalQueue.size, // Combined pending
            active: messageQueue.pending + paymentProcessor.highPriorityQueue.pending + paymentProcessor.normalQueue.pending // Combined active
        }
    });
});

// Webhook handler (listens for updates from Telegram)
const webhookPath = `/bot${process.env.BOT_TOKEN}`;
app.post(webhookPath, (req, res) => {
    // Add webhook processing to the message queue as well
    messageQueue.add(() => {
        try {
            if (!req.body || typeof req.body !== 'object') {
                console.warn("⚠️ Received invalid webhook request body");
                performanceMonitor.logRequest(false);
                return res.status(400).send('Invalid request body');
            }

            // Process the update using the bot instance
            bot.processUpdate(req.body);
            performanceMonitor.logRequest(true);
            res.sendStatus(200); // Acknowledge receipt to Telegram
        } catch (error) {
            console.error("❌ Webhook processing error:", error);
            performanceMonitor.logRequest(false);
            res.status(500).json({ // Send error response
                error: 'Internal server error processing webhook',
                details: error.message
            });
        }
    });
});

// --- State Management & Constants ---

// User command cooldown (prevents spam)
const confirmCooldown = new Map(); // Map<userId, lastCommandTimestamp>
const cooldownInterval = 3000; // 3 seconds

// Cache for linked wallets (reduces DB lookups)
const walletCache = new Map(); // Map<userId, { wallet: address, timestamp: cacheTimestamp }>
const CACHE_TTL = 300000; // Cache wallet links for 5 minutes (300,000 ms)

// Cache of processed transaction signatures during this bot session (prevents double processing)
const processedSignaturesThisSession = new Set(); // Set<signature>
const MAX_PROCESSED_SIGNATURES = 10000; // Reset cache if it gets too large

// *** PAGINATION FIX: Initialize state object for payment monitor pagination ***
const lastProcessedSignature = {}; // Object<walletAddress, lastSignatureString>
// *** END PAGINATION FIX ***

// Game Configuration
const GAME_CONFIG = {
    coinflip: {
        minBet: 0.01,
        maxBet: 1.0,
        expiryMinutes: 15,
        houseEdge: 0.02 // 2% house edge
    },
    race: {
        minBet: 0.01,
        maxBet: 1.0,
        expiryMinutes: 15,
        houseEdge: 0.02 // 2% house edge (applied during payout calculation)
    }
};

// Fee buffer (lamports) - should cover base fee + priority fee estimate
const FEE_BUFFER = BigInt(process.env.FEE_MARGIN);
// Priority fee rate (used to calculate microLamports for ComputeBudgetProgram)
// Example: 0.0001 means 0.01% of the payout amount used as priority fee rate
const PRIORITY_FEE_RATE = 0.0001;

// --- Helper Functions ---

// Generates a unique memo ID for tracking bets
function generateMemoId(prefix = 'BET') {
    const validPrefixes = ['BET', 'CF', 'RA']; // Allowed prefixes
    if (!validPrefixes.includes(prefix)) {
        console.error(`Attempted to generate memo with invalid prefix: ${prefix}`);
        prefix = 'BET'; // Default to BET if invalid
    }
    // Format: PREFIX-HEXSTRING (e.g., CF-A1B2C3D4E5F6)
    return `${prefix}-${randomBytes(6).toString('hex').toUpperCase()}`;
}

// Validates the original expected memo format
function validateOriginalMemoFormat(memo) {
    if (!memo || typeof memo !== 'string') return false;
    const parts = memo.split('-');
    return parts.length === 2 &&
           ['BET', 'CF', 'RA'].includes(parts[0]) &&
           /^[A-F0-9]{12}$/.test(parts[1]); // Original: Exactly 12 hex chars
}

// --- NEW: DeepSeek's normalizeMemo function ---
// Attempts to normalize potentially non-standard memo formats
// WARNING: This logic is speculative and might incorrectly alter valid memos.
// It also uses a less strict hex validation. Use with caution.
function normalizeMemo(rawMemo) {
    if (!rawMemo || typeof rawMemo !== 'string') return null;

    // 1. Remove common prefixes/suffixes wallets might add & trim whitespace
    let memo = rawMemo.trim()
        .replace(/^memo:/i, '')
        .replace(/^text:/i, '')
        .trim();

    // 2. Handle specific JSON/array formats (like Phantom's [1,2,3]) - Highly speculative
    try {
        // Only attempt parse if it looks like JSON array/object
        if (memo.startsWith('[') || memo.startsWith('{')) {
            const parsed = JSON.parse(memo);
            if (Array.isArray(parsed)) {
                // If it's an array, join with hyphens. This likely breaks expected format.
                 console.warn(`Normalized potentially JSON array memo: ${rawMemo} -> ${parsed.join('-')}`);
                memo = parsed.join('-');
            }
            // Ignore parsed objects or other JSON types
        }
    } catch {
        // Ignore parsing errors, proceed with memo as string
    }

    // 3. Standardize separators (allow space, comma, underscore as separator) - Risky
    // This might corrupt valid hex strings if they contain these characters unexpectedly.
    memo = memo.replace(/[\s,_]+/g, '-');

    // 4. Final validation (Less strict than original)
    const parts = memo.split('-');
    if (parts.length >= 1) { // Allow just prefix or prefix-id
        const prefix = parts[0].toUpperCase(); // Standardize prefix case
        const idPart = parts.slice(1).join(''); // Combine remaining parts (allows multiple hyphens)

        // Check if prefix is valid and if the ID part (if exists) contains only hex chars
        if (['BET','CF','RA'].includes(prefix)) {
            // Check if ID part exists and is valid hex OR if only prefix exists
            if ((idPart.length > 0 && /^[A-F0-9]+$/.test(idPart)) || idPart.length === 0) {
                 // Return standardized format (e.g., CF-ABCDEF) or just prefix if no ID part
                 const normalized = idPart.length > 0 ? `${prefix}-${idPart}` : prefix;

                 // *** ADDED CHECK: Compare against original expected format before returning ***
                 // We still want the EXACT format CF-HEX12 preferably.
                 // If the normalized one happens to match the original strict format, return it.
                 // Otherwise, maybe log a warning and return null, or return the less strict format?
                 // Let's return the normalized one for now as requested, but log if it's weird.
                 if (!validateOriginalMemoFormat(normalized)) {
                     console.warn(`Normalized memo "${normalized}" from raw "${rawMemo}" does not match strict format (PREFIX-HEX12). Accepting potentially invalid format.`);
                 }
                 return normalized;
            }
        }
    }

    // Original raw memo didn't normalize to a valid format
    // console.log(`Memo "${rawMemo}" could not be normalized to a valid format.`);
    return null;
}
// --- END NEW ---


// --- Database Operations ---

// Saves a new bet intention to the database
async function savePendingBet(userId, chatId, gameType, details, lamports, memoId, expiresAt, priority = 0) {
    // Use the ORIGINAL strict validation for generating the memo ID
    if (!validateOriginalMemoFormat(memoId)) {
        console.error(`DB: Attempted to save bet with invalid generated memo format: ${memoId}`);
        return { success: false, error: 'Internal error: Invalid memo ID generated' };
    }

    const query = `
        INSERT INTO bets (
            user_id, chat_id, game_type, bet_details,
            expected_lamports, memo_id, status, expires_at, fees_paid, priority
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        RETURNING id;
    `;
    const values = [
        String(userId), // Ensure IDs are strings
        String(chatId),
        gameType,
        details, // JSONB object
        BigInt(lamports), // Ensure lamports are BigInt
        memoId,
        'awaiting_payment', // Initial status
        expiresAt,
        FEE_BUFFER, // Store the fee buffer associated with this bet
        priority
    ];

    try {
        const res = await pool.query(query, values);
        console.log(`DB: Saved pending ${gameType} bet ID ${res.rows[0].id} with memo ${memoId}`);
        return { success: true, id: res.rows[0].id };
    } catch (err) {
        console.error('DB Error saving bet:', err.message);
        // Handle unique constraint violation specifically (memo collision)
        if (err.code === '23505') { // PostgreSQL unique violation error code
             console.warn(`DB: Memo ID collision for ${memoId}. User might be retrying.`);
             return { success: false, error: 'Memo ID already exists. Please try generating the bet again.' };
        }
        return { success: false, error: err.message };
    }
}

// Finds a pending bet by its unique memo ID
async function findBetByMemo(memoId) {
    // Use the ORIGINAL strict validation when searching by memo ID
    if (!validateOriginalMemoFormat(memoId)) {
         // console.warn(`DB: findBetByMemo called with invalid format: ${memoId}`);
         return undefined;
    }

    // Select the highest priority bet first if multiple match (unlikely with unique memo)
    // Use FOR UPDATE SKIP LOCKED to prevent race conditions if multiple monitors pick up the same TX
    const query = `
        SELECT * FROM bets
        WHERE memo_id = $1 AND status = 'awaiting_payment'
        ORDER BY priority DESC, created_at ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1;
    `;
    try {
        const res = await pool.query(query, [memoId]);
        return res.rows[0]; // Return the bet object or undefined
    } catch (err) {
        console.error(`DB Error finding bet by memo ${memoId}:`, err.message);
        return undefined;
    }
}

// Marks a bet as paid after successful transaction verification
async function markBetPaid(betId, signature) {
    const query = `
        UPDATE bets
        SET status = 'payment_verified',
            paid_tx_signature = $1,
            processed_at = NOW() -- Record time payment was verified
        WHERE id = $2 AND status = 'awaiting_payment' -- Ensure status is correct before update
        RETURNING *; -- Return the updated bet row
    `;
    try {
        const res = await pool.query(query, [signature, betId]);
        if (res.rowCount === 0) {
            console.warn(`DB: Attempted to mark bet ${betId} as paid, but status was not 'awaiting_payment' or bet not found.`);
            return { success: false, error: 'Bet not found or already processed' };
        }
        console.log(`DB: Marked bet ${betId} as paid with TX ${signature}`);
        return { success: true, bet: res.rows[0] };
    } catch (err) {
         // Handle potential unique constraint violation on paid_tx_signature
        if (err.code === '23505') {
             console.warn(`DB: Paid TX Signature ${signature} collision for bet ${betId}. Already processed.`);
             return { success: false, error: 'Transaction signature already recorded' };
        }
        console.error(`DB Error marking bet ${betId} paid:`, err.message);
        return { success: false, error: err.message };
    }
}

// Links a Solana wallet address to a Telegram User ID
async function linkUserWallet(userId, walletAddress) {
    const cacheKey = `wallet-${userId}`;
    const query = `
        INSERT INTO wallets (user_id, wallet_address, last_used_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (user_id) -- If user already exists
        DO UPDATE SET       -- Update their wallet address and last used time
            wallet_address = EXCLUDED.wallet_address,
            last_used_at = NOW()
        RETURNING *; -- Return the created/updated wallet entry
    `;
    try {
        const res = await pool.query(query, [String(userId), walletAddress]);
        const linkedWallet = res.rows[0]?.wallet_address;

        if (linkedWallet) {
            console.log(`DB: Linked/Updated wallet for user ${userId} to ${linkedWallet}`);
            // Update cache
            walletCache.set(cacheKey, {
                wallet: linkedWallet,
                timestamp: Date.now()
            });
        }
        return { success: true };
    } catch (err) {
        console.error(`DB Error linking wallet for user ${userId}:`, err.message);
        return { success: false, error: err.message };
    }
}

// Retrieves the linked wallet address for a user (checks cache first)
async function getLinkedWallet(userId) {
    const cacheKey = `wallet-${userId}`;

    // Check cache first
    if (walletCache.has(cacheKey)) {
        const { wallet, timestamp } = walletCache.get(cacheKey);
        if (Date.now() - timestamp < CACHE_TTL) {
            // console.log(`Cache hit for user ${userId} wallet.`); // Optional: Debug log
            return wallet; // Return cached wallet if not expired
        } else {
            walletCache.delete(cacheKey); // Delete expired cache entry
        }
    }

    // If not in cache or expired, query DB
    const query = `SELECT wallet_address FROM wallets WHERE user_id = $1`;
    try {
        const res = await pool.query(query, [String(userId)]);
        const wallet = res.rows[0]?.wallet_address;

        if (wallet) {
            // Add to cache
            walletCache.set(cacheKey, {
                wallet,
                timestamp: Date.now()
            });
        }
        return wallet; // Return wallet address or undefined
    } catch (err) {
        console.error(`DB Error fetching wallet for user ${userId}:`, err.message);
        return undefined; // Indicate error or not found
    }
}

// Updates the status of a specific bet
async function updateBetStatus(betId, status) {
    const query = `UPDATE bets SET status = $1 WHERE id = $2 RETURNING id;`;
    try {
        const res = await pool.query(query, [status, betId]);
        if (res.rowCount > 0) {
             console.log(`DB: Updated status for bet ${betId} to ${status}`);
             return true;
        } else {
             console.warn(`DB: Failed to update status for bet ${betId} (not found?)`);
             return false;
        }
    } catch (err) {
        console.error(`DB Error updating bet ${betId} status to ${status}:`, err.message);
        return false;
    }
}

// Records the payout transaction signature for a completed winning bet
async function recordPayout(betId, status, signature) {
    const query = `
        UPDATE bets
        SET status = $1,
            payout_tx_signature = $2,
            processed_at = NOW() -- Record time payout was completed
        WHERE id = $3 AND status = 'processing_payout' -- Ensure status is correct
        RETURNING id;
    `;
    try {
        const res = await pool.query(query, [status, signature, betId]);
         if (res.rowCount > 0) {
             console.log(`DB: Recorded payout TX ${signature} for bet ${betId} with status ${status}`);
             return true;
         } else {
             console.warn(`DB: Failed to record payout for bet ${betId} (not found or status mismatch?)`);
             return false;
         }
    } catch (err) {
         // Handle potential unique constraint violation on payout_tx_signature
        if (err.code === '23505') {
             console.warn(`DB: Payout TX Signature ${signature} collision for bet ${betId}. Already recorded.`);
             return false;
        }
        console.error(`DB Error recording payout for bet ${betId}:`, err.message);
        return false;
    }
}


// --- Solana Transaction Analysis ---

// --- UPDATED: findMemoInTx using normalizeMemo ---
// Finds and potentially normalizes SPL Memo instruction data in a transaction
function findMemoInTx(tx) {
    if (!tx?.transaction?.message?.instructions) {
        return null;
    }

    try {
        const MEMO_PROGRAM_ID = 'Memo1UhkJRfHyvLMcVuc6beZNRYqUP2VZwW'; // v1
        const MEMO_PROGRAM_ID_V2 = 'MemoSq4gqABAXKb96qnH8TysNcVtrp5GktfD'; // v2

        for (const instruction of tx.transaction.message.instructions) {
            let programId = '';

            if (instruction.programIdIndex !== undefined && tx.transaction.message.accountKeys) {
                const keyInfo = tx.transaction.message.accountKeys[instruction.programIdIndex];
                programId = keyInfo?.pubkey ? keyInfo.pubkey.toBase58() :
                            (typeof keyInfo === 'string' ? new PublicKey(keyInfo).toBase58() : '');
            } else if (instruction.programId) {
                programId = instruction.programId.toBase58 ?
                            instruction.programId.toBase58() :
                            instruction.programId.toString();
            }

            if ((programId === MEMO_PROGRAM_ID || programId === MEMO_PROGRAM_ID_V2) && instruction.data) {
                const rawMemo = bs58.decode(instruction.data).toString('utf-8');
                // Attempt to normalize the raw memo
                const normalizedMemo = normalizeMemo(rawMemo);
                // Return the normalized memo (which could be null if normalization failed)
                // console.log(`Found raw memo "${rawMemo}", normalized to "${normalizedMemo}"`); // Debugging
                return normalizedMemo;
            }
        }
    } catch (e) {
        console.error("Error parsing memo instruction:", e.message);
    }
    return null; // No memo instruction found or parsing failed
}
// --- END UPDATE ---

// Analyzes a transaction to find SOL transfers to the target bot wallet
function analyzeTransactionAmounts(tx, walletType) {
    let transferAmount = 0n; // Use BigInt for lamports
    let payerAddress = null;
    const targetAddress = walletType === 'coinflip'
        ? process.env.MAIN_WALLET_ADDRESS
        : process.env.RACE_WALLET_ADDRESS;

    if (!targetAddress) { // Added check
        console.error(`[AnalyzeTx] Target address for ${walletType} is not defined in environment variables.`);
        return { transferAmount: 0n, payerAddress: null };
    }

    if (tx?.meta?.err) {
        // console.warn(`Skipping amount analysis for failed TX: ${tx.transaction.signatures[0]}`);
        return { transferAmount: 0n, payerAddress: null }; // Ignore failed transactions
    }

    if (tx?.meta && tx?.transaction?.message?.instructions) {
        // Combine top-level and inner instructions for comprehensive analysis
        const instructions = [
            ...(tx.transaction.message.instructions || []),
            ...(tx.meta.innerInstructions || []).flatMap(i => i.instructions)
        ];

        const SYSTEM_PROGRAM_ID = SystemProgram.programId.toBase58();

        for (const inst of instructions) {
             let programId = '';
             // Similar programId resolution as in findMemoInTx
            if (inst.programIdIndex !== undefined && tx.transaction.message.accountKeys) {
                const keyInfo = tx.transaction.message.accountKeys[inst.programIdIndex];
                programId = keyInfo?.pubkey ? keyInfo.pubkey.toBase58() :
                            (typeof keyInfo === 'string' ? new PublicKey(keyInfo).toBase58() : '');
            } else if (inst.programId) {
                programId = inst.programId.toBase58 ?
                            inst.programId.toBase58() :
                            inst.programId.toString();
            }

            // Check for SystemProgram SOL transfers
            if (programId === SYSTEM_PROGRAM_ID && inst.parsed?.type === 'transfer') {
                const transferInfo = inst.parsed.info;
                // Check if the destination is the bot's target wallet
                if (transferInfo.destination === targetAddress) {
                    // Add the amount (handle potential variations in field names)
                    transferAmount += BigInt(transferInfo.lamports || transferInfo.amount || 0);
                    // Capture the source address (likely the user's wallet)
                    if (!payerAddress) payerAddress = transferInfo.source;
                }
            }
        }
    }

    // console.log(`Analyzed TX: Found ${transferAmount} lamports transfer from ${payerAddress || 'Unknown'} to ${targetAddress}`);
    return { transferAmount, payerAddress };
}

// Tries to identify the primary payer of a transaction (heuristic)
function getPayerFromTransaction(tx) {
     if (!tx || !tx.meta || !tx.transaction?.message?.accountKeys) return null;

     const message = tx.transaction.message;
     // Fee payer is always the first account listed
     if (message.accountKeys.length > 0) {
         const feePayerKeyInfo = message.accountKeys[0];
         const feePayerAddress = feePayerKeyInfo?.pubkey ? feePayerKeyInfo.pubkey.toBase58() :
                                  (typeof feePayerKeyInfo === 'string' ? new PublicKey(feePayerKeyInfo).toBase58() : null);
         // console.log(`Identified fee payer as ${feePayerAddress}`); // Optional log
         return feePayerAddress ? new PublicKey(feePayerAddress) : null; // Return PublicKey or null
     }

     // Fallback: Check signers and balance changes (less reliable)
     const preBalances = tx.meta.preBalances;
     const postBalances = tx.meta.postBalances;
     if (!preBalances || !postBalances) return null;

     for (let i = 0; i < message.accountKeys.length; i++) {
         if (i >= preBalances.length || i >= postBalances.length) continue;

         // Check if the account is a signer in the transaction message
         if (message.accountKeys[i]?.signer || message.header?.numRequiredSignatures > 0 && i < message.header.numRequiredSignatures) {
              let key;
               if (message.accountKeys[i].pubkey) {
                   key = message.accountKeys[i].pubkey;
               } else if (typeof message.accountKeys[i] === 'string') {
                   key = new PublicKey(message.accountKeys[i]);
               } else {
                   continue;
               }

               // Check if balance decreased (paid fees or sent funds)
               const balanceDiff = BigInt(preBalances[i] || 0) - BigInt(postBalances[i] || 0);
               if (balanceDiff > 0) {
                   // console.log(`Identified potential payer by balance change: ${key.toBase58()}`); // Optional log
                   return key; // Return the PublicKey object
               }
         }
     }

     // console.warn("Could not definitively identify payer.");
     return null; // Could not determine payer
}

// --- Payment Processing System ---

// Checks if an error is likely retryable (e.g., network/rate limit issues)
function isRetryableError(error) {
    const msg = error?.message?.toLowerCase() || ''; // Defensive check
    return msg.includes('429') || // HTTP 429 Too Many Requests
           msg.includes('timeout') ||
           msg.includes('rate limit') ||
           msg.includes('econnreset') || // Connection reset
           msg.includes('esockettimedout') ||
           msg.includes('eai_again'); // DNS lookup timeout
}


class PaymentProcessor {
    constructor() {
        // Queue for high-priority jobs (e.g., game processing, payouts)
        this.highPriorityQueue = new PQueue({
            concurrency: 3,
        });
        // Queue for normal priority jobs (e.g., initial payment monitoring checks)
        this.normalQueue = new PQueue({
            concurrency: 2,
        });
        this.activeProcesses = new Set(); // Track signatures currently being processed to prevent duplicates
    }

    // Adds a job to the appropriate queue based on priority
    async addPaymentJob(job) {
        // Simulate priority by choosing the queue
        if (job.priority > 0) {
            await this.highPriorityQueue.add(() => this.processJob(job));
        } else {
            await this.normalQueue.add(() => this.processJob(job));
        }
    }

    // Wrapper to handle job execution, retries, and error logging
    async processJob(job) {
        // Prevent processing the same signature concurrently if added multiple times quickly
        if (job.signature && this.activeProcesses.has(job.signature)) { // Check only if signature exists
             // console.warn(`Job for signature ${job.signature} already active, skipping duplicate.`); // Reduce log noise
             return;
        }
        if (job.signature) this.activeProcesses.add(job.signature); // Mark as active if signature exists

        try {
            let result;
            // Route job based on type
            if (job.type === 'monitor_payment') {
                 result = await this._processIncomingPayment(job.signature, job.walletType, job.retries || 0);
            } else if (job.type === 'process_bet') {
                 const bet = await pool.query('SELECT * FROM bets WHERE id = $1', [job.betId]).then(res => res.rows[0]);
                 if (bet) {
                     await processPaidBet(bet); // Trigger game logic processing
                     result = { processed: true }; // Assume success unless exception
                 } else {
                     console.error(`Cannot process bet: Bet ID ${job.betId} not found.`);
                     result = { processed: false, reason: 'bet_not_found' };
                 }
            } else if (job.type === 'payout') {
                 await handlePayoutJob(job); // Trigger payout logic
                 result = { processed: true }; // Assume success unless exception
            } else {
                 console.error(`Unknown job type: ${job.type}`);
                 result = { processed: false, reason: 'unknown_job_type'};
            }

            performanceMonitor.logRequest(true); // Log successful processing attempt
            return result;

        } catch (error) {
            performanceMonitor.logRequest(false); // Log failed processing attempt
            console.error(`Error processing job type ${job.type} for signature ${job.signature || 'N/A'} (Bet ID: ${job.betId || 'N/A'}):`, error.message);

            // Retry logic for retryable errors (only for initial payment check for now)
            if (job.type === 'monitor_payment' && (job.retries || 0) < 3 && isRetryableError(error)) {
                job.retries = (job.retries || 0) + 1;
                console.log(`Retrying job for signature ${job.signature} (Attempt ${job.retries})...`);
                await new Promise(resolve => setTimeout(resolve, 1000 * job.retries)); // Exponential backoff
                // Release active lock before requeueing for retry
                if (job.signature) this.activeProcesses.delete(job.signature);
                await this.addPaymentJob(job); // Re-add the job for retry
                return; // Prevent falling through to finally block immediately after requeueing
            } else {
                // Log final failure or non-retryable error
                 console.error(`Job failed permanently or exceeded retries: ${job.signature || job.betId}`, error);
                 // Potentially update bet status to an error state if applicable
                 if(job.betId && job.type !== 'monitor_payment') {
                     // Check if status is still one that makes sense to mark as error
                     try {
                         const currentStatusRes = await pool.query('SELECT status FROM bets WHERE id = $1', [job.betId]);
                         const currentStatus = currentStatusRes.rows[0]?.status;
                         // Only mark as error if it's in a state where this makes sense (not already completed/errored differently)
                         if (currentStatus && !currentStatus.startsWith('completed_') && !currentStatus.startsWith('error_')) {
                            await updateBetStatus(job.betId, `error_${job.type}_failed`);
                         }
                     } catch (dbErr) {
                        console.error(`Failed to check status before marking error for Bet ID ${job.betId}:`, dbErr.message);
                     }
                 }
            }
        } finally {
             // Always remove signature from active set when processing finishes (success or final fail)
            if (job.signature) {
                this.activeProcesses.delete(job.signature);
            }
        }
    }

    // **** START: _processIncomingPayment with DETAILED LOGGING ****
    async _processIncomingPayment(signature, walletType, attempt = 0) {
        const logPrefix = `[PaymentProc:${signature?.substring(0, 8) || 'SIG N/A'}]`; // Short sig prefix for logs
        console.log(`${logPrefix} Starting processing (Attempt ${attempt + 1}) for wallet type ${walletType}.`);

        try { // Wrap entire function logic in try/catch for better error logging
            // 1. Check session cache first (quickest check)
            if (processedSignaturesThisSession.has(signature)) {
                console.log(`${logPrefix} Skipped: Already processed in this session cache.`);
                return { processed: false, reason: 'already_processed_session' };
            }
            console.log(`${logPrefix} Passed session cache check.`);

            // 2. Check database if already recorded as paid
            const checkQuery = `SELECT id FROM bets WHERE paid_tx_signature = $1 LIMIT 1;`;
            try {
                const processed = await pool.query(checkQuery, [signature]);
                if (processed.rowCount > 0) {
                    console.log(`${logPrefix} Skipped: Signature already exists in DB (Bet ID: ${processed.rows[0].id}).`);
                    processedSignaturesThisSession.add(signature); // Add to session cache too
                    return { processed: false, reason: 'exists_in_db' };
                }
                console.log(`${logPrefix} Passed DB signature check.`);
            } catch (dbError) {
                console.error(`${logPrefix} DB Error checking signature:`, dbError.message);
                 if (isRetryableError(dbError)) throw dbError; // Rethrow retryable DB errors for job retry
                 return { processed: false, reason: 'db_check_error' };
            }

            // 3. Fetch the transaction details from Solana
            console.log(`${logPrefix} Fetching transaction details from Solana...`);
            let tx;
            try {
                tx = await solanaConnection.getParsedTransaction(
                    signature,
                    { maxSupportedTransactionVersion: 0 } // Request parsed format
                );
            } catch (fetchError) {
                console.error(`${logPrefix} Error fetching transaction details:`, fetchError.message);
                if (isRetryableError(fetchError)) throw fetchError; // Rethrow retryable fetch errors
                // Treat non-retryable fetch error as TX not found/invalid for this process run
                tx = null; // Ensure tx is null if non-retryable error
            }

            // 4. Validate transaction
            if (!tx) {
                console.warn(`${logPrefix} Transaction not found or failed to fetch.`);
                // Don't add to processedSignatures here, could be temporary RPC issue unless error was non-retryable
                // If error was non-retryable, it's already logged above. If retryable, it will be retried by processJob.
                // If simply not found, maybe it will appear later? Throw error to allow potential retry?
                 throw new Error(`Transaction ${signature} null or fetch failed`); // Let processJob handle retry
                // return { processed: false, reason: 'tx_fetch_failed' };
            }
            console.log(`${logPrefix} Transaction details fetched.`);

            if (tx.meta?.err) {
                console.log(`${logPrefix} Skipped: Transaction failed on-chain: ${JSON.stringify(tx.meta.err)}`);
                processedSignaturesThisSession.add(signature); // Add failed TX sig to cache
                return { processed: false, reason: 'tx_onchain_error' };
            }
            console.log(`${logPrefix} Passed on-chain success check.`);

            // 5. Find and validate the memo using the updated findMemoInTx
            const memo = findMemoInTx(tx); // Uses normalizeMemo internally
            console.log(`${logPrefix} Memo found/normalized: "${memo}"`);
            if (!memo) {
                console.log(`${logPrefix} Skipped: No valid game memo found after normalization.`);
                // Don't add to cache, could be unrelated transaction
                return { processed: false, reason: 'no_valid_memo' };
            }

            // 5b. Find the *exact* bet using the *strict* memo format check
            const bet = await findBetByMemo(memo); // Uses strict check internally
            if (!bet) {
                console.warn(`${logPrefix} Skipped: No matching *pending* bet found for strictly validated memo "${memo}".`);
                // Don't add to cache, memo might be for an old/processed bet or invalid format despite normalization attempt
                return { processed: false, reason: 'no_matching_bet_strict' };
            }
            console.log(`${logPrefix} Found matching Bet ID: ${bet.id} for memo ${memo}.`);

            // 6. Check bet status (redundant due to findBetByMemo query, but safe)
            if (bet.status !== 'awaiting_payment') {
                console.warn(`${logPrefix} Skipped: Bet ${bet.id} status is ${bet.status}, not 'awaiting_payment'.`);
                processedSignaturesThisSession.add(signature); // Add sig, bet already processed somehow
                return { processed: false, reason: 'bet_already_processed' };
            }
            console.log(`${logPrefix} Passed bet status check ('awaiting_payment').`);

            // 7. Analyze transaction amounts
            const { transferAmount, payerAddress } = analyzeTransactionAmounts(tx, walletType);
            console.log(`${logPrefix} Amount analysis: Found ${transferAmount} lamports transfer to target wallet from ${payerAddress || 'Unknown'}.`);
            if (transferAmount <= 0n) {
                console.warn(`${logPrefix} Skipped: No SOL transfer to target wallet found.`);
                // Don't add to cache, unrelated transaction
                return { processed: false, reason: 'no_transfer_found' };
            }

            // 8. Validate amount sent vs expected (with tolerance)
            const expectedAmount = BigInt(bet.expected_lamports);
            const tolerance = BigInt(5000); // Allow small deviation (e.g., 0.000005 SOL)
            const lowerBound = expectedAmount - tolerance;
            const upperBound = expectedAmount + tolerance;
            console.log(`${logPrefix} Amount Check: Expected=${expectedAmount}, Received=${transferAmount}, Tolerance=${tolerance} (Range: [${lowerBound}, ${upperBound}])`);

            if (transferAmount < lowerBound || transferAmount > upperBound) {
                console.warn(`${logPrefix} Failed: Amount mismatch.`);
                await updateBetStatus(bet.id, 'error_payment_mismatch');
                processedSignaturesThisSession.add(signature); // Add sig, processed with error
                await bot.sendMessage(bet.chat_id, `⚠️ Payment amount mismatch for bet ${memo}. Expected ${Number(expectedAmount)/LAMPORTS_PER_SOL} SOL, received ${Number(transferAmount)/LAMPORTS_PER_SOL} SOL. Bet cancelled.`).catch(e => console.error("TG Send Error:", e.message));
                return { processed: false, reason: 'amount_mismatch' };
            }
            console.log(`${logPrefix} Passed amount check.`);

            // 9. Validate transaction time vs bet expiry
            const txTime = tx.blockTime ? new Date(tx.blockTime * 1000) : new Date(0);
            const betExpiresAt = new Date(bet.expires_at);
            console.log(`${logPrefix} Expiry Check: TxTime=${txTime.toISOString() || 'Unknown'}, ExpiresAt=${betExpiresAt.toISOString()}`);
            if (txTime === 0) {
                 console.warn(`${logPrefix} Could not determine blockTime for TX. Skipping expiry check.`);
            } else if (txTime > betExpiresAt) {
                console.warn(`${logPrefix} Failed: Payment received after expiry.`);
                await updateBetStatus(bet.id, 'error_payment_expired');
                processedSignaturesThisSession.add(signature); // Add sig, processed with error
                await bot.sendMessage(bet.chat_id, `⚠️ Payment for bet ${memo} received after expiry time. Bet cancelled.`).catch(e => console.error("TG Send Error:", e.message));
                return { processed: false, reason: 'expired' };
            }
            console.log(`${logPrefix} Passed expiry check.`);

            // 10. Mark bet as paid in DB
            console.log(`${logPrefix} Attempting to mark bet ${bet.id} as paid in DB...`);
            const markResult = await markBetPaid(bet.id, signature);
            if (!markResult.success) {
                console.error(`${logPrefix} Failed DB update: ${markResult.error}`);
                 processedSignaturesThisSession.add(signature); // Add sig here to prevent retries if DB fails persistently
                return { processed: false, reason: 'db_mark_paid_failed' };
            }
            console.log(`${logPrefix} Successfully marked bet ${bet.id} as 'payment_verified'.`);

            // 11. Link wallet address to user ID
            let actualPayer = payerAddress || getPayerFromTransaction(tx)?.toBase58();
             console.log(`${logPrefix} Attempting to link wallet ${actualPayer || 'Unknown'} to user ${bet.user_id}...`);
            if (actualPayer) {
                await linkUserWallet(bet.user_id, actualPayer);
                console.log(`${logPrefix} Wallet link attempt finished.`);
            } else {
                 console.warn(`${logPrefix} Could not identify payer address to link wallet.`);
            }

            // 12. Add signature to session cache AFTER successful DB update
            processedSignaturesThisSession.add(signature);
            console.log(`${logPrefix} Added signature to session cache.`);
            if (processedSignaturesThisSession.size > MAX_PROCESSED_SIGNATURES) {
                 console.log(`${logPrefix} Clearing processed signatures cache (reached max size)`);
                 processedSignaturesThisSession.clear();
            }

            // 13. Queue the bet for actual game processing (higher priority)
            console.log(`${logPrefix} Queuing bet ${bet.id} for game processing.`);
            await this.addPaymentJob({
                type: 'process_bet',
                betId: bet.id,
                priority: 1, // Higher priority than monitoring
                signature // Pass signature for context/logging if needed
            });

            console.log(`${logPrefix} Processing finished successfully.`);
            return { processed: true };

        } catch (error) { // Catch any unexpected errors during the process
            console.error(`${logPrefix} UNEXPECTED ERROR during processing:`, error);
            // Don't update bet status here, let the job processor handle retries/final error status
            // Re-throw the error so the job processor knows it failed
            throw error;
        }
    }
    // **** END: _processIncomingPayment with DETAILED LOGGING ****
}

const paymentProcessor = new PaymentProcessor();


// --- Payment Monitoring Loop ---
let isMonitorRunning = false; // Flag to prevent concurrent monitor runs
let monitorIntervalSeconds = 30; // Initial interval
let monitorInterval = null; // Holds the setInterval ID

// *** UPDATED FUNCTION with adaptive throttling ***
async function monitorPayments() {
    // <<< Logging Added Here >>>
    // console.log(`[${new Date().toISOString()}] ---- monitorPayments function START ----`); // Reduce log noise

    if (isMonitorRunning) {
        // console.log('[Monitor] Cycle already running, skipping.'); // Reduce log noise
        return;
    }

    isMonitorRunning = true;
    const startTime = Date.now();
    let signaturesFound = 0;

    try {
        // --- START: Adaptive Rate Limiting Logic ---
        const currentLoad = paymentProcessor.highPriorityQueue.pending +
                            paymentProcessor.normalQueue.pending;

        // Calculate delay: 50ms per pending item, max 5000ms (5 seconds)
        // Adjusted delay calculation to be less aggressive (50ms per item)
        const throttleDelay = Math.min(5000, currentLoad * 50);

        if (throttleDelay > 0) {
            console.log(`[Monitor] Queues have ${currentLoad} pending items. Throttling monitor check for ${throttleDelay}ms.`);
            await new Promise(resolve => setTimeout(resolve, throttleDelay));
        }
        // --- END: Adaptive Rate Limiting Logic ---


        // --- Original Monitor Logic Start ---
        const walletsToMonitor = [
            { address: process.env.MAIN_WALLET_ADDRESS, type: 'coinflip', priority: 0 },
            { address: process.env.RACE_WALLET_ADDRESS, type: 'race', priority: 0 },
        ];

        // Process each wallet
        for (const wallet of walletsToMonitor) {
            if (!wallet.address) { // Add check for missing env var
                 console.warn(`[Monitor] Skipping monitoring for ${wallet.type}: Address not defined in environment variables.`);
                 continue;
             }
            try {
                // *** PAGINATION FIX: Get the last signature processed for THIS wallet ***
                const beforeSig = lastProcessedSignature[wallet.address] || null;
                // console.log(`[Monitor] Checking ${wallet.type} wallet (${wallet.address}) for signatures before: ${beforeSig || 'Start'}`); // Optional: More detailed logging

                // Fetch latest signatures for the wallet using rate-limited connection
                const signatures = await solanaConnection.getSignaturesForAddress(
                    new PublicKey(wallet.address),
                    {
                        limit: 15,       // Fetch a reasonable batch size
                        before: beforeSig  // Use the specific 'before' signature for pagination
                    }
                );

                if (!signatures || signatures.length === 0) {
                    continue; // No new signatures for this wallet
                }

                signaturesFound += signatures.length; // Count total found signatures

                // *** PAGINATION FIX: Update the last processed signature state for the *next* query cycle ***
                // Use the *oldest* signature in this batch as the 'before' marker for the next run
                const oldestSignatureInBatch = signatures[signatures.length - 1].signature;
                lastProcessedSignature[wallet.address] = oldestSignatureInBatch;
                // console.log(`[Monitor] Updated last signature for ${wallet.address} to: ${oldestSignatureInBatch}`); // Optional: Logging
                // *** END PAGINATION FIX ***

                // Queue each found signature for processing by the PaymentProcessor
                for (const sigInfo of signatures) {
                    // Double check session cache before queuing
                    if (processedSignaturesThisSession.has(sigInfo.signature)) {
                        continue;
                    }
                    // Queue the job
                    await paymentProcessor.addPaymentJob({
                        type: 'monitor_payment',
                        signature: sigInfo.signature,
                        walletType: wallet.type,
                        priority: wallet.priority, // 0 for monitoring jobs
                        retries: 0
                    });
                }
            } catch (error) {
                console.error(`[Monitor] Error fetching/processing signatures for wallet ${wallet.address}:`, error.message);
                performanceMonitor.logRequest(false);
                 // Maybe reset lastProcessedSignature for this wallet on error? Or rely on retry?
                 // For now, let's rely on the next successful run.
            }
        } // End loop through wallets
        // --- Original Monitor Logic End ---

    } catch (error) {
        console.error("[Monitor] Unexpected error in main monitor loop:", error);
        performanceMonitor.logRequest(false);
    } finally {
        isMonitorRunning = false; // Release the lock
        const duration = Date.now() - startTime;
        if (signaturesFound > 0) { // Only log duration if something happened
             console.log(`[Monitor] Cycle finished in ${duration}ms. Found ${signaturesFound} new signatures.`);
        }

        // Adjust monitoring interval based on activity
        adjustMonitorInterval(signaturesFound);

        // console.log(`[${new Date().toISOString()}] ---- monitorPayments function END ----`); // Reduce log noise
    }
}
// *** END UPDATED FUNCTION ***


// Adjusts the monitor interval based on how many new signatures were found
function adjustMonitorInterval(processedCount) {
    let newInterval = monitorIntervalSeconds;
    if (processedCount > 10) { // High activity, check sooner
        newInterval = Math.max(10, monitorIntervalSeconds - 5); // Min 10s
    } else if (processedCount === 0) { // No activity, check less often
        newInterval = Math.min(120, monitorIntervalSeconds + 10); // Max 120s (2 minutes)
    } // else: Moderate activity, keep interval the same

    if (newInterval !== monitorIntervalSeconds && monitorInterval) {
        monitorIntervalSeconds = newInterval;
        clearInterval(monitorInterval); // Clear existing interval
        monitorInterval = setInterval(() => { // Reschedule with new interval
             // console.log(`[${new Date().toISOString()}] ==== Monitor Interval Fired (${monitorIntervalSeconds}s) ====`); // Reduce log noise
             try {
                 monitorPayments().catch(err => {
                     console.error('[FATAL MONITOR ERROR in setInterval catch]:', err);
                     performanceMonitor.logRequest(false);
                 });
             } catch (syncErr) {
                 console.error('[FATAL MONITOR SYNC ERROR in setInterval try/catch]:', syncErr);
                 performanceMonitor.logRequest(false);
             }
             // console.log(`[${new Date().toISOString()}] ==== Monitor Interval Callback End ====`);
         }, monitorIntervalSeconds * 1000);
        console.log(`ℹ️ Adjusted monitor interval to ${monitorIntervalSeconds}s`);
    }
}


// --- SOL Sending Function ---
async function sendSol(recipientPublicKey, amountLamports, gameType) {
    // Select the correct private key based on the game/wallet being paid from
    const privateKey = gameType === 'coinflip'
        ? process.env.BOT_PRIVATE_KEY
        : process.env.RACE_BOT_PRIVATE_KEY;

    if (!privateKey) {
         console.error(`❌ Cannot send SOL: Missing private key for game type ${gameType}`);
         return { success: false, error: `Missing private key for ${gameType}` };
    }

    const recipientPubKey = (typeof recipientPublicKey === 'string')
        ? new PublicKey(recipientPublicKey)
        : recipientPublicKey;

    const amountSOL = Number(amountLamports) / LAMPORTS_PER_SOL;
    console.log(`💸 Attempting to send ${amountSOL.toFixed(6)} SOL to ${recipientPubKey.toBase58()} for ${gameType}`);

    // Calculate dynamic priority fee based on payout amount (adjust rate as needed)
    const priorityFeeMicroLamports = Math.min(
        1000000, // Max priority fee (in microLamports, e.g., 0.001 SOL)
        Math.max(1000, Math.floor(Number(amountLamports) * PRIORITY_FEE_RATE)) // Min 1000 microLamports
    );

    const amountToSend = BigInt(amountLamports); // Send the exact calculated payout amount

    if (amountToSend <= 0n) {
         console.error(`❌ Calculated payout amount ${amountLamports} is zero or negative. Cannot send.`);
         return { success: false, error: 'Calculated payout amount is zero or negative' };
    }

    // Retry logic for sending transaction
    for (let attempt = 1; attempt <= 3; attempt++) {
        try {
            const payerWallet = Keypair.fromSecretKey(bs58.decode(privateKey));

            // Get latest blockhash before building transaction
            const latestBlockhash = await solanaConnection.getLatestBlockhash(
                 { commitment: 'confirmed' } // Use confirmed blockhash
            );

            if (!latestBlockhash || !latestBlockhash.blockhash) {
                throw new Error('Failed to get latest blockhash');
            }


            const transaction = new Transaction({
                recentBlockhash: latestBlockhash.blockhash,
                feePayer: payerWallet.publicKey
            });

            // Add priority fee instruction
            transaction.add(
                ComputeBudgetProgram.setComputeUnitPrice({
                    microLamports: priorityFeeMicroLamports
                })
            );
            // Add CU limit just in case (simple transfer is low, but good practice)
            transaction.add(
                ComputeBudgetProgram.setComputeUnitLimit({ units: 800 }) // Simple transfer needs ~200 CUs
            );

            // Add the SOL transfer instruction
            transaction.add(
                SystemProgram.transfer({
                    fromPubkey: payerWallet.publicKey,
                    toPubkey: recipientPubKey,
                    lamports: amountToSend // Send the exact calculated amount
                })
            );

            console.log(`Sending TX (Attempt ${attempt})... Amount: ${amountToSend}, Priority Fee: ${priorityFeeMicroLamports} microLamports`);

            // Send and confirm transaction with timeout
            const signature = await Promise.race([
                sendAndConfirmTransaction(
                    solanaConnection, // Use the rate-limited connection
                    transaction,
                    [payerWallet], // Signer
                    {
                        commitment: 'confirmed', // Confirm at 'confirmed' level
                        skipPreflight: false,    // Perform preflight checks
                        maxRetries: 3            // Retries within sendAndConfirm
                    }
                ),
                // Add a timeout for the confirmation process
                new Promise((_, reject) => {
                    setTimeout(() => {
                        reject(new Error(`Transaction confirmation timeout after 45s (Attempt ${attempt})`)); // Increased timeout
                    }, 45000); // 45 second timeout
                })
            ]);

            console.log(`✅ Payout successful! Sent ${Number(amountToSend)/LAMPORTS_PER_SOL} SOL to ${recipientPubKey.toBase58()}. TX: ${signature}`);
            return { success: true, signature };

        } catch (error) {
            console.error(`❌ Payout TX failed (Attempt ${attempt}/3):`, error.message);

            // Check for specific errors that shouldn't be retried
            if (error.message.includes('Invalid param') ||
                error.message.includes('Insufficient funds') || // Bot might be out of SOL
                error.message.includes('blockhash not found') ||
                error.message.includes('custom program error')) { // Catch on-chain program errors early
                 console.error("❌ Non-retryable error encountered. Aborting payout.");
                 return { success: false, error: `Non-retryable error: ${error.message}` }; // Exit retry loop
            }

            // If retryable error and not last attempt, wait before retrying
            if (attempt < 3) {
                await new Promise(resolve => setTimeout(resolve, 2000 * attempt)); // Wait longer each attempt (increased)
            }
        }
    } // End retry loop

    // If all attempts failed
    console.error(`❌ Payout failed after 3 attempts for ${recipientPubKey.toBase58()}`);
    return { success: false, error: `Payout failed after 3 attempts` };
}


// --- Game Processing Logic ---

// Routes a paid bet to the correct game handler
async function processPaidBet(bet) {
    console.log(`⚙️ Processing game for bet ${bet.id} (${bet.game_type})`);
    let client;

    try {
         // Start a transaction and lock the bet row to prevent concurrent processing
         client = await pool.connect();
         await client.query('BEGIN');
         const statusCheck = await client.query(
             'SELECT status FROM bets WHERE id = $1 FOR UPDATE', // Lock the row
             [bet.id]
         );

         // Double-check status before processing
         if (statusCheck.rows[0]?.status !== 'payment_verified') {
             console.warn(`Bet ${bet.id} status is ${statusCheck.rows[0]?.status}, not 'payment_verified'. Aborting game processing.`);
             await client.query('ROLLBACK'); // Release lock
             return;
         }

         // Update status to 'processing_game' within the transaction
         await client.query(
             'UPDATE bets SET status = $1 WHERE id = $2',
             ['processing_game', bet.id]
         );
         await client.query('COMMIT'); // Commit status change and release lock

         // Call the appropriate game handler
         if (bet.game_type === 'coinflip') {
             await handleCoinflipGame(bet);
         } else if (bet.game_type === 'race') {
             await handleRaceGame(bet);
         } else {
             console.error(`❌ Unknown game type '${bet.game_type}' for bet ${bet.id}`);
             await updateBetStatus(bet.id, 'error_unknown_game');
         }
    } catch (error) {
        console.error(`❌ Error during game processing setup for bet ${bet.id}:`, error.message);
        if (client) {
            try { await client.query('ROLLBACK'); } catch (rbErr) { console.error('Rollback failed:', rbErr); }
        }
        await updateBetStatus(bet.id, 'error_processing_setup'); // Mark bet with error
    } finally {
        if (client) client.release(); // Release client back to pool
    }
}


// Handles the Coinflip game logic
async function handleCoinflipGame(bet) {
    const { id: betId, user_id, chat_id, bet_details, expected_lamports } = bet;
    const choice = bet_details.choice; // 'heads' or 'tails'
    const config = GAME_CONFIG.coinflip;

    // Determine outcome using pseudo-randomness with house edge applied
    const playerWinProbability = 0.5 - (config.houseEdge / 2.0);
    const resultRandom = Math.random();
    let result;
    if (choice === 'heads') {
        result = resultRandom < playerWinProbability ? 'heads' : 'tails';
    } else { // choice === 'tails'
        result = resultRandom < playerWinProbability ? 'tails' : 'heads';
    }
    const win = (result === choice);

    // Calculate payout amount if win (uses helper function)
    const payoutLamports = win ? calculatePayout(BigInt(expected_lamports), 'coinflip') : 0n;

    // Get user's display name for messages
    let displayName = await getUserDisplayName(chat_id, user_id);

    if (win) {
        const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
        console.log(`🪙 Coinflip Bet ${betId}: ${displayName} WON ${payoutSOL.toFixed(6)} SOL! Choice: ${choice}, Result: ${result}`);

        // Check if user has a linked wallet
        const winnerAddress = await getLinkedWallet(user_id);
        if (!winnerAddress) {
             console.warn(`Coinflip Bet ${betId}: Winner ${displayName} has no linked wallet.`);
             await bot.sendMessage(chat_id,
                 `🎉 ${displayName}, you won the coinflip (Result: *${result}*) but have no wallet linked!\n` +
                 `Your payout of ${payoutSOL.toFixed(6)} SOL is waiting. Place another bet (any amount) to link your wallet and receive pending payouts.`,
                 { parse_mode: 'Markdown' }
             ).catch(e => console.error("TG Send Error:", e.message));
             await updateBetStatus(betId, 'completed_win_no_wallet'); // Special status
             return;
        }

        // Send "processing payout" message and queue the actual payout
        try {
            await bot.sendMessage(chat_id,
                `🎉 ${displayName}, you won ${payoutSOL.toFixed(6)} SOL!\n` +
                `Result: *${result}*\n\n` +
                `💸 Processing payout to your linked wallet...`,
                { parse_mode: 'Markdown' }
            ).catch(e => console.error("TG Send Error:", e.message));

            // Update bet status before queuing payout
             await updateBetStatus(betId, 'processing_payout');

            // Add payout job to the high priority queue
            await paymentProcessor.addPaymentJob({
                type: 'payout',
                betId,
                recipient: winnerAddress,
                amount: payoutLamports, // Send the calculated payout amount
                gameType: 'coinflip',
                priority: 2, // Higher priority than monitoring/game processing
                chatId: chat_id,
                displayName,
                result
            });

        } catch (e) {
            console.error(`❌ Error queuing payout for coinflip bet ${betId}:`, e);
            await bot.sendMessage(chat_id,
                `⚠️ Error occurred while processing your coinflip win for bet ID ${betId}.\n` +
                `Please contact support.`,
                { parse_mode: 'Markdown' }
            ).catch(e => console.error("TG Send Error:", e.message));
            await updateBetStatus(betId, 'error_payout_queueing');
        }

    } else { // Loss
        console.log(`🪙 Coinflip Bet ${betId}: ${displayName} LOST. Choice: ${choice}, Result: ${result}`);
        await bot.sendMessage(chat_id,
            `❌ ${displayName}, you lost the coinflip!\n` +
            `You guessed *${choice}* but the result was *${result}*. Better luck next time!`,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
        await updateBetStatus(betId, 'completed_loss');
    }
}

// Handles the Race game logic
async function handleRaceGame(bet) {
    const { id: betId, user_id, chat_id, bet_details, expected_lamports } = bet;
    const chosenHorseName = bet_details.horse;
    const config = GAME_CONFIG.race;

    // Ensure probabilities sum close to 1 (adjust if needed)
    const horses = [
        { name: 'Yellow', emoji: '🟡', odds: 1.1, winProbability: 0.25 },
        { name: 'Orange', emoji: '🟠', odds: 2.0, winProbability: 0.20 },
        { name: 'Blue',   emoji: '🔵', odds: 3.0, winProbability: 0.15 },
        { name: 'Cyan',   emoji: '💧', odds: 4.0, winProbability: 0.12 },
        { name: 'White',  emoji: '⚪️', odds: 5.0, winProbability: 0.09 },
        { name: 'Red',    emoji: '🔴', odds: 6.0, winProbability: 0.07 },
        { name: 'Black',  emoji: '⚫️', odds: 7.0, winProbability: 0.05 },
        { name: 'Pink',   emoji: '🌸', odds: 8.0, winProbability: 0.03 },
        { name: 'Purple', emoji: '🟣', odds: 9.0, winProbability: 0.02 },
        { name: 'Green',  emoji: '🟢', odds: 10.0, winProbability: 0.01 },
        { name: 'Silver', emoji: '💎', odds: 15.0, winProbability: 0.01 } // Total: 0.99 - adjust if needed
    ];
    const totalProb = horses.reduce((sum, h) => sum + h.winProbability, 0);
    if (totalProb < 1.0 && totalProb > 0) { // Avoid division by zero if totalProb is 0
        const adjustmentFactor = 1.0 / totalProb;
        horses.forEach(h => h.winProbability *= adjustmentFactor);
        console.log(`[Race:${betId}] Adjusted horse probabilities to sum to 1.`);
    } else if (totalProb !== 1.0) {
        console.warn(`[Race:${betId}] Initial horse probabilities sum to ${totalProb}, might lead to unexpected winner selection.`);
    }


    // Determine the winning horse based on weighted probabilities
    let winningHorse;
    const randomNumber = Math.random();
    let cumulativeProbability = 0;
    for (const horse of horses) {
        cumulativeProbability += horse.winProbability;
        if (randomNumber <= cumulativeProbability) {
            winningHorse = horse;
            break;
        }
    }
    // Fallback just in case (shouldn't be needed if probabilities sum to 1)
    winningHorse = winningHorse || horses[horses.length - 1];

    // Send race commentary messages
    let displayName = await getUserDisplayName(chat_id, user_id);
    try {
        await bot.sendMessage(chat_id, `🐎 Race ${betId} starting! ${displayName} bet on ${chosenHorseName}!`).catch(e => console.error("TG Send Error:", e.message));
        await new Promise(resolve => setTimeout(resolve, 2000)); // Pause
        await bot.sendMessage(chat_id, "🚦 And they're off!").catch(e => console.error("TG Send Error:", e.message));
        await new Promise(resolve => setTimeout(resolve, 3000)); // Pause
        await bot.sendMessage(chat_id,
            `🏆 The winner is... ${winningHorse.emoji} *${winningHorse.name}*! 🏆`,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
    } catch (e) {
        console.error(`Error sending race commentary for bet ${betId}:`, e);
    }

    // Determine win/loss and payout
    const win = (chosenHorseName.toLowerCase() === winningHorse.name.toLowerCase());
    const payoutLamports = win
        ? calculatePayout(BigInt(expected_lamports), 'race', winningHorse) // Pass winning horse for odds
        : 0n;

    if (win) {
        const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
        console.log(`🐎 Race Bet ${betId}: ${displayName} WON ${payoutSOL.toFixed(6)} SOL! Horse: ${chosenHorseName}, Winner: ${winningHorse.name}`);

        // Check for linked wallet
        const winnerAddress = await getLinkedWallet(user_id);
        if (!winnerAddress) {
            console.warn(`Race Bet ${betId}: Winner ${displayName} has no linked wallet.`);
            await bot.sendMessage(chat_id,
                `🎉 ${displayName}, your horse *${chosenHorseName}* won the race!\n` +
                `Your payout of ${payoutSOL.toFixed(6)} SOL is waiting. Place another bet (any amount) to link your wallet and receive pending payouts.`,
                { parse_mode: 'Markdown' }
            ).catch(e => console.error("TG Send Error:", e.message));
            await updateBetStatus(betId, 'completed_win_no_wallet');
            return;
        }

        // Send "processing payout" message and queue the actual payout
        try {
            await bot.sendMessage(chat_id,
                `🎉 ${displayName}, your horse *${chosenHorseName}* won!\n` +
                `Payout: ${payoutSOL.toFixed(6)} SOL\n\n` +
                `💸 Processing payout to your linked wallet...`,
                { parse_mode: 'Markdown' }
            ).catch(e => console.error("TG Send Error:", e.message));

             // Update bet status before queuing payout
             await updateBetStatus(betId, 'processing_payout');

            // Add payout job to the high priority queue
            await paymentProcessor.addPaymentJob({
                type: 'payout',
                betId,
                recipient: winnerAddress,
                amount: payoutLamports, // Send calculated payout amount
                gameType: 'race',
                priority: 2,
                chatId: chat_id,
                displayName,
                horseName: chosenHorseName,
                winningHorse
            });
        } catch (e) {
            console.error(`❌ Error queuing payout for race bet ${betId}:`, e);
             await bot.sendMessage(chat_id,
                 `⚠️ Error occurred while processing your race win for bet ID ${betId}.\n` +
                 `Please contact support.`,
                 { parse_mode: 'Markdown' }
             ).catch(e => console.error("TG Send Error:", e.message));
            await updateBetStatus(betId, 'error_payout_queueing');
        }

    } else { // Loss
        console.log(`🐎 Race Bet ${betId}: ${displayName} LOST. Horse: ${chosenHorseName}, Winner: ${winningHorse.name}`);
        await bot.sendMessage(chat_id,
            `❌ ${displayName}, your horse *${chosenHorseName}* lost the race!\n` +
            `Winner: ${winningHorse.emoji} *${winningHorse.name}*. Better luck next time!`,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
        await updateBetStatus(betId, 'completed_loss');
    }
}


// Handles the actual payout transaction after a win is confirmed
async function handlePayoutJob(job) {
    const { betId, recipient, amount, gameType, chatId, displayName, result, horseName, winningHorse } = job;
    const payoutAmountLamports = BigInt(amount); // Ensure amount is BigInt

    // Ensure payout amount is positive before attempting to send
    if (payoutAmountLamports <= 0n) {
        console.error(`❌ Payout job for bet ${betId} has zero or negative amount (${payoutAmountLamports}). Skipping send.`);
        await updateBetStatus(betId, 'error_payout_zero_amount');
        await bot.sendMessage(chatId,
            `⚠️ There was an issue calculating the payout for bet ID ${betId} (amount was zero). Please contact support.`,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
        return; // Don't proceed with sending 0
    }

    console.log(`💸 Processing payout job for Bet ID: ${betId}, Amount: ${payoutAmountLamports} lamports`);

    try {
        // Attempt to send the SOL payout
        const sendResult = await sendSol(recipient, payoutAmountLamports, gameType);

        if (sendResult.success) {
            console.log(`💸 Payout successful for bet ${betId}, TX: ${sendResult.signature}`);
            // Send success message to user
            await bot.sendMessage(chatId,
                `✅ Payout successful for bet ${betId}!\n` +
                `${(Number(payoutAmountLamports)/LAMPORTS_PER_SOL).toFixed(6)} SOL sent.\n` +
                `TX: \`https://solscan.io/tx/${sendResult.signature}\``, // Add link
                { parse_mode: 'Markdown', disable_web_page_preview: true } // Don't preview Solscan etc.
            ).catch(e => console.error("TG Send Error:", e.message));
            // Record payout signature and final status in DB
            await recordPayout(betId, 'completed_win_paid', sendResult.signature);

        } else {
            // Payout failed after retries
            console.error(`❌ Payout failed permanently for bet ${betId}: ${sendResult.error}`);
            await bot.sendMessage(chatId,
                `⚠️ Payout failed for bet ID ${betId}: ${sendResult.error}\n` +
                `The team has been notified. Please contact support if needed.`,
                { parse_mode: 'Markdown' }
            ).catch(e => console.error("TG Send Error:", e.message));
            // Update status to indicate payout failure
            await updateBetStatus(betId, 'error_payout_failed');
            // TODO: Implement admin notification for failed payouts (Log is already present)
            // notifyAdmin(`Payout failed for Bet ${betId}: ${sendResult.error}`);
        }
    } catch (error) {
        // Catch unexpected errors during payout processing
        console.error(`❌ Unexpected error processing payout job for bet ${betId}:`, error);
        // Update status to reflect exception during payout attempt
        await updateBetStatus(betId, 'error_payout_exception');
        // Notify user of technical error
         await bot.sendMessage(chatId,
             `⚠️ A technical error occurred during payout for bet ID ${betId}.\n` +
             `Please contact support.`,
             { parse_mode: 'Markdown' }
         ).catch(e => console.error("TG Send Error:", e.message));
         // notifyAdmin(`Unexpected error during payout for Bet ${betId}: ${error.message}`);
    }
}

// --- Utility Functions ---

// Calculates payout amount, applying house edge. Returns BigInt lamports.
function calculatePayout(betLamports, gameType, gameDetails = {}) {
    betLamports = BigInt(betLamports); // Ensure input is BigInt
    let payoutLamports = 0n;

    if (gameType === 'coinflip') {
        // Payout is 2x minus house edge (e.g., 2 - 0.02 = 1.98 multiplier)
        const multiplier = 2.0 - GAME_CONFIG.coinflip.houseEdge;
        // Use floating point for intermediate calculation, then floor to BigInt
        payoutLamports = BigInt(Math.floor(Number(betLamports) * multiplier));
    } else if (gameType === 'race' && gameDetails.odds) {
        // Payout is Bet * Odds * (1 - House Edge)
        const multiplier = gameDetails.odds * (1.0 - GAME_CONFIG.race.houseEdge);
        payoutLamports = BigInt(Math.floor(Number(betLamports) * multiplier));
    } else {
         console.error(`Cannot calculate payout: Unknown game type ${gameType} or missing race odds.`);
         return 0n; // Return 0 if calculation fails
    }

    // Return the calculated payout amount. Fee buffer is *not* subtracted here.
    // sendSol handles the sending; the bot needs sufficient balance overall.
    // console.log(`Payout Calc: Bet ${betLamports}, Payout ${payoutLamports}`); // Debug log
    return payoutLamports > 0n ? payoutLamports : 0n; // Ensure payout is not negative
}

// Fetches user's display name (username or first name) from Telegram
async function getUserDisplayName(chat_id, user_id) {
     try {
         const chatMember = await bot.getChatMember(chat_id, user_id);
         // Prefer username if available
         return chatMember.user.username
             ? `@${chatMember.user.username}`
             : chatMember.user.first_name;
     } catch (e) {
         // Log error but return default name if fetching fails
         // Avoid logging common "user not found" errors if expected in some contexts
         if (e.message?.includes('user not found')) {
            // Optionally handle differently or just return default
         } else {
            console.warn(`Couldn't get username for user ${user_id} in chat ${chat_id}:`, e.message);
         }
         return `User ${String(user_id).substring(0, 6)}...`; // Return partial ID as fallback
     }
}

// --- Telegram Bot Command Handlers ---

// Handles polling errors (e.g., conflicts if multiple instances run)
bot.on('polling_error', (error) => {
    console.error(`❌ Polling error: ${error.code} - ${error.message}`);
    // Specific check for conflict error code
    if (error.code === 'ETELEGRAM' && error.message.includes('409 Conflict')) {
        console.error("❌❌❌ FATAL: Conflict detected! Another bot instance might be running with the same token. Exiting.");
        process.exit(1); // Exit immediately to prevent issues
    }
    // Handle other polling errors if needed
});

// Handles general bot errors
bot.on('error', (error) => {
    console.error('❌ General Bot Error:', error);
    performanceMonitor.logRequest(false); // Log as failed request
});


// Central handler for all incoming messages (routed by command)
async function handleMessage(msg) {
    // Ignore messages without text or from other bots
    if (!msg.text || msg.from.is_bot) return;

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const messageText = msg.text || ''; // Ensure messageText is a string

    try {
        // --- User Cooldown Check ---
        const now = Date.now();
        if (confirmCooldown.has(userId)) {
            const lastTime = confirmCooldown.get(userId);
            if (now - lastTime < cooldownInterval) {
                // Optionally send a cooldown message, or just ignore
                // await bot.sendMessage(chatId, "⏱️ Please wait a moment before sending another command.");
                return; // Ignore command if user is on cooldown
            }
        }
        // Apply cooldown only to commands, not general messages
        if (messageText.startsWith('/')) {
             confirmCooldown.set(userId, now); // Update last command time
        }


        // --- Command Routing ---
        const text = messageText.trim(); // Trim whitespace

        if (text.match(/^\/start$/i)) {
            await handleStartCommand(msg);
        }
        else if (text.match(/^\/coinflip$/i)) {
            await handleCoinflipCommand(msg);
        }
        else if (text.match(/^\/wallet$/i)) {
            await handleWalletCommand(msg);
        }
        else if (text.match(/^\/bet (\d+\.?\d*)\s+(heads|tails)/i)) { // Regex for /bet amount choice
            await handleBetCommand(msg);
        }
        else if (text.match(/^\/race$/i)) {
            await handleRaceCommand(msg);
        }
        else if (text.match(/^\/betrace (\d+\.?\d*)\s+(\w+)/i)) { // Regex for /betrace amount horse
            await handleBetRaceCommand(msg);
        }
        else if (text.match(/^\/help$/i)) {
             await handleHelpCommand(msg);
        }
        // Ignore other messages or unrecognized commands silently, or add a default handler

        performanceMonitor.logRequest(true); // Log successful command handling

    } catch (error) {
        console.error(`❌ Error processing message from user ${userId} in chat ${chatId}: "${messageText}"`, error);
        performanceMonitor.logRequest(false); // Log error

        // Send generic error message to user
        try {
             await bot.sendMessage(chatId, "⚠️ An unexpected error occurred while processing your request. Please try again later or contact support if the issue persists.");
        } catch (tgError) {
             console.error("❌ Failed to send error message to chat:", tgError.message);
        }
    } finally {
         // Clean up cooldown map periodically (optional)
         // Example: Remove entries older than 1 minute
         const cutoff = Date.now() - 60000;
         for (const [key, timestamp] of confirmCooldown.entries()) {
             if (timestamp < cutoff) {
                 confirmCooldown.delete(key);
             }
         }
    }
}

// Handles the /start command
async function handleStartCommand(msg) {
    const firstName = msg.from.first_name || 'there';
    try {
        // Try sending with animation/banner first
        await bot.sendAnimation(msg.chat.id, 'https://i.ibb.co/9vDo58q/banner.gif', {
            caption: `👋 Welcome, ${firstName}!\n\n` +
                     `🎰 *Solana Gambles Bot*\n\n` +
                     `Use /coinflip or /race to see game options.\n` +
                     `Use /wallet to view your linked Solana wallet.\n` +
                     `Use /help to see all commands.`,
            parse_mode: 'Markdown'
        }).catch(async (err) => {
             // Fallback if animation fails (e.g., invalid URL, Telegram issue)
             console.warn("⚠️ Failed to send start animation, sending text fallback:", err.message); // Use warn level
             await bot.sendMessage(msg.chat.id,
                 `👋 Welcome, ${firstName}!\n\n` +
                 `🎰 *Solana Gambles Bot*\n\n` +
                 `Use /coinflip or /race to see game options.\n` +
                 `Use /wallet to view your linked Solana wallet.\n` +
                 `Use /help to see all commands.`,
                 { parse_mode: 'Markdown' }
             );
        });
    } catch (error) {
         console.error("Start command error:", error);
         // Ensure some message is sent even if all else fails
         await bot.sendMessage(msg.chat.id, `Welcome, ${firstName}! Use /help to see available commands.`).catch(e=>console.error("TG Send Error:", e.message));
    }
}

// Handles the /coinflip command (shows instructions)
async function handleCoinflipCommand(msg) {
    const config = GAME_CONFIG.coinflip;
    await bot.sendMessage(msg.chat.id,
        `🪙 *Coinflip Game* 🪙\n\n` +
        `Bet on Heads or Tails!\n\n` +
        `*How to play:*\n` +
        `1. Type \`/bet amount heads\` (e.g., \`/bet 0.1 heads\`)\n` +
        `2. Type \`/bet amount tails\` (e.g., \`/bet 0.1 tails\`)\n\n` +
        `*Rules:*\n` +
        `- Min Bet: ${config.minBet} SOL\n` +
        `- Max Bet: ${config.maxBet} SOL\n` +
        `- House Edge: ${(config.houseEdge * 100).toFixed(1)}%\n` +
        `- Payout: ~${(2.0 - config.houseEdge).toFixed(2)}x (after house edge)\n\n` + // Show effective payout
        `You will be given a wallet address and a *unique Memo ID*. Send the *exact* SOL amount with the memo to place your bet.`,
        { parse_mode: 'Markdown' }
    ).catch(e => console.error("TG Send Error:", e.message));
}

// Handles the /race command (shows instructions)
async function handleRaceCommand(msg) {
    const horses = [
         { name: 'Yellow', emoji: '🟡', odds: 1.1 }, { name: 'Orange', emoji: '🟠', odds: 2.0 },
         { name: 'Blue',   emoji: '🔵', odds: 3.0 }, { name: 'Cyan',   emoji: '💧', odds: 4.0 },
         { name: 'White',  emoji: '⚪️', odds: 5.0 }, { name: 'Red',    emoji: '🔴', odds: 6.0 },
         { name: 'Black',  emoji: '⚫️', odds: 7.0 }, { name: 'Pink',   emoji: '🌸', odds: 8.0 },
         { name: 'Purple', emoji: '🟣', odds: 9.0 }, { name: 'Green',  emoji: '🟢', odds: 10.0 },
         { name: 'Silver', emoji: '💎', odds: 15.0 }
    ];

    let raceMessage = `🐎 *Horse Race Game* 🐎\n\nBet on the winning horse!\n\n*Available Horses & Odds:*\n`;
    horses.forEach(horse => {
        // Calculate payout after house edge for display
        const effectiveOdds = (horse.odds * (1.0 - GAME_CONFIG.race.houseEdge)).toFixed(2)
        raceMessage += `- ${horse.emoji} *${horse.name}* (~${effectiveOdds}x Payout)\n`;
    });

    const config = GAME_CONFIG.race;
    raceMessage += `\n*How to play:*\n` +
                   `1. Type \`/betrace amount horse_name\`\n` +
                   `   (e.g., \`/betrace 0.1 Yellow\`)\n\n` +
                   `*Rules:*\n` +
                   `- Min Bet: ${config.minBet} SOL\n` +
                   `- Max Bet: ${config.maxBet} SOL\n` +
                   `- House Edge: ${(config.houseEdge * 100).toFixed(1)}% (applied to winnings)\n\n` +
                   `You will be given a wallet address and a *unique Memo ID*. Send the *exact* SOL amount with the memo to place your bet.`;

    await bot.sendMessage(msg.chat.id, raceMessage, { parse_mode: 'Markdown' }).catch(e => console.error("TG Send Error:", e.message));
}

// Handles the /wallet command (shows linked wallet)
async function handleWalletCommand(msg) {
    const userId = String(msg.from.id);
    const walletAddress = await getLinkedWallet(userId); // Uses cache

    if (walletAddress) {
        await bot.sendMessage(msg.chat.id,
            `🔗 Your linked Solana wallet:\n\`${walletAddress}\`\n\n`+
            `Payouts will be sent here. It's linked automatically when you make your first paid bet.`,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
    } else {
        await bot.sendMessage(msg.chat.id,
            `🔗 No wallet linked yet.\n` +
            `Place a bet and send the required SOL. Your sending wallet will be automatically linked for future payouts.`
        ).catch(e => console.error("TG Send Error:", e.message));
    }
}

// Handles /bet command (for Coinflip)
async function handleBetCommand(msg) {
    const match = msg.text.match(/^\/bet (\d+\.?\d*)\s+(heads|tails)/i);
    if (!match) return; // Should not happen if routing is correct, but safe check

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const config = GAME_CONFIG.coinflip;

    // Validate bet amount
    const betAmount = parseFloat(match[1]);
    if (isNaN(betAmount) || betAmount < config.minBet || betAmount > config.maxBet) {
        await bot.sendMessage(chatId,
            `⚠️ Invalid bet amount. Please bet between ${config.minBet} and ${config.maxBet} SOL.\n` +
            `Example: \`/bet 0.1 heads\``,
             { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
        return;
    }

    const userChoice = match[2].toLowerCase();
    const memoId = generateMemoId('CF');
    const expectedLamports = BigInt(Math.round(betAmount * LAMPORTS_PER_SOL));
    const expiresAt = new Date(Date.now() + config.expiryMinutes * 60 * 1000);

    // Save the pending bet
    const saveResult = await savePendingBet(
        userId, chatId, 'coinflip',
        { choice: userChoice },
        expectedLamports, memoId, expiresAt
    );

    if (!saveResult.success) {
         await bot.sendMessage(chatId, `⚠️ Error registering bet: ${saveResult.error}. Please try the command again.`).catch(e => console.error("TG Send Error:", e.message));
         // Don't throw, just return after notifying user
         return;
    }

    // Send payment instructions
    await bot.sendMessage(chatId,
        `✅ Coinflip bet registered! (ID: ${memoId})\n\n` +
        `You chose: *${userChoice}*\n` +
        `Amount: *${betAmount.toFixed(6)} SOL*\n\n` +
        `➡️ Send *exactly ${betAmount.toFixed(6)} SOL* to:\n` +
        `\`${process.env.MAIN_WALLET_ADDRESS}\`\n\n` + // Ensure this env var is correct
        `📎 *Include MEMO:* \`${memoId}\`\n\n` +
        `⏱️ This request expires in ${config.expiryMinutes} minutes.\n\n` +
        `*IMPORTANT:* Send from your own wallet. Do not send from an exchange. Ensure you include the memo correctly.`,
        { parse_mode: 'Markdown', disable_web_page_preview: true }
    ).catch(e => console.error("TG Send Error:", e.message));
}


// Handles /betrace command
async function handleBetRaceCommand(msg) {
    const match = msg.text.match(/^\/betrace (\d+\.?\d*)\s+(\w+)/i);
     if (!match) return; // Safe check

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const config = GAME_CONFIG.race;

    // Validate bet amount
    const betAmount = parseFloat(match[1]);
    if (isNaN(betAmount) || betAmount < config.minBet || betAmount > config.maxBet) {
        await bot.sendMessage(chatId,
            `⚠️ Invalid bet amount. Please bet between ${config.minBet} and ${config.maxBet} SOL.\n`+
            `Example: \`/betrace 0.1 Yellow\``,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
        return;
    }

    // Validate horse selection
    const chosenHorseNameInput = match[2];
    const horses = [
         { name: 'Yellow', emoji: '🟡', odds: 1.1 }, { name: 'Orange', emoji: '🟠', odds: 2.0 },
         { name: 'Blue',   emoji: '🔵', odds: 3.0 }, { name: 'Cyan',   emoji: '💧', odds: 4.0 },
         { name: 'White',  emoji: '⚪️', odds: 5.0 }, { name: 'Red',    emoji: '🔴', odds: 6.0 },
         { name: 'Black',  emoji: '⚫️', odds: 7.0 }, { name: 'Pink',   emoji: '🌸', odds: 8.0 },
         { name: 'Purple', emoji: '🟣', odds: 9.0 }, { name: 'Green',  emoji: '🟢', odds: 10.0 },
         { name: 'Silver', emoji: '💎', odds: 15.0 }
    ];
    const chosenHorse = horses.find(h => h.name.toLowerCase() === chosenHorseNameInput.toLowerCase());

    if (!chosenHorse) {
        await bot.sendMessage(chatId,
             `⚠️ Invalid horse name: "${chosenHorseNameInput}". Please choose from the list in /race.`
        ).catch(e => console.error("TG Send Error:", e.message));
        return;
    }

    const memoId = generateMemoId('RA');
    const expectedLamports = BigInt(Math.round(betAmount * LAMPORTS_PER_SOL));
    const expiresAt = new Date(Date.now() + config.expiryMinutes * 60 * 1000);

    // Save the pending bet
    const saveResult = await savePendingBet(
        userId, chatId, 'race',
        { horse: chosenHorse.name, odds: chosenHorse.odds },
        expectedLamports, memoId, expiresAt
    );

      if (!saveResult.success) {
         await bot.sendMessage(chatId, `⚠️ Error registering bet: ${saveResult.error}. Please try the command again.`).catch(e => console.error("TG Send Error:", e.message));
         // Don't throw, just return
         return;
      }

    // Calculate potential payout for display
     const potentialPayoutLamports = calculatePayout(expectedLamports, 'race', chosenHorse);
     const potentialPayoutSOL = (Number(potentialPayoutLamports) / LAMPORTS_PER_SOL).toFixed(6);

    // Send payment instructions
    await bot.sendMessage(chatId,
        `✅ Race bet registered! (ID: ${memoId})\n\n` +
        `You chose: ${chosenHorse.emoji} *${chosenHorse.name}*\n` +
        `Amount: *${betAmount.toFixed(6)} SOL*\n` +
        `Potential Payout: ~${potentialPayoutSOL} SOL\n\n`+ // Show potential payout
        `➡️ Send *exactly ${betAmount.toFixed(6)} SOL* to:\n` +
        `\`${process.env.RACE_WALLET_ADDRESS}\`\n\n` + // Ensure this env var is correct
        `📎 *Include MEMO:* \`${memoId}\`\n\n` +
        `⏱️ This request expires in ${config.expiryMinutes} minutes.\n\n` +
        `*IMPORTANT:* Send from your own wallet. Do not send from an exchange. Ensure you include the memo correctly.`,
        { parse_mode: 'Markdown', disable_web_page_preview: true }
    ).catch(e => console.error("TG Send Error:", e.message));
}

// Handles /help command
async function handleHelpCommand(msg) {
     const helpText = `*Solana Gambles Bot Commands* 🎰\n\n` +
                      `/start - Show welcome message\n` +
                      `/help - Show this help message\n\n` +
                      `*Games:*\n` +
                      `/coinflip - Show Coinflip game info & how to bet\n` +
                      `/race - Show Horse Race game info & how to bet\n\n` +
                      `*Betting:*\n` +
                      `/bet <amount> <heads|tails> - Place a Coinflip bet\n` +
                      `/betrace <amount> <horse_name> - Place a Race bet\n\n` +
                      `*Wallet:*\n` +
                      `/wallet - View your linked Solana wallet for payouts\n\n` +
                      `*Support:* If you encounter issues, please contact [Admin/Support Link - Placeholder].`; // Replace placeholder

     await bot.sendMessage(msg.chat.id, helpText, { parse_mode: 'Markdown' }).catch(e => console.error("TG Send Error:", e.message));
}


// --- Server Startup & Shutdown Logic ---

// Main function to initialize and start the bot/server
async function startServer() {
    try {
        await initializeDatabase(); // Ensure DB is ready first
        const PORT = process.env.PORT || 3000; // Use PORT from env or default to 3000

        // Set up Telegram Webhook if in a production environment (e.g., Railway)
        if (process.env.RAILWAY_ENVIRONMENT && process.env.RAILWAY_PUBLIC_DOMAIN) {
            const webhookUrl = `https://${process.env.RAILWAY_PUBLIC_DOMAIN}${webhookPath}`;
            console.log(`Attempting to set webhook to: ${webhookUrl}`);

            let attempts = 0;
            while (attempts < 3) { // Retry webhook setup
                try {
                    await bot.setWebHook(webhookUrl);
                    console.log(`✅ Webhook successfully set to: ${webhookUrl}`);
                    break; // Exit loop on success
                } catch (webhookError) {
                    attempts++;
                    console.error(`❌ Webhook setup attempt ${attempts} failed:`, webhookError.message);
                    if (attempts >= 3) {
                         console.error("❌ Max webhook setup attempts reached. Exiting.");
                         throw webhookError; // Throw error to stop startup
                    }
                    // Wait before retrying
                    await new Promise(resolve => setTimeout(resolve, 3000 * attempts));
                }
            }
        } else {
             console.log("ℹ️ Not in Railway environment or domain not set, webhook not configured.");
        }

        // Start the Express server
        const server = app.listen(PORT, "0.0.0.0", () => { // Listen on all interfaces
            console.log(`🚀 Server running on port ${PORT}`);

            // Start the payment monitor loop AFTER server starts listening
            console.log(`⚙️ Starting payment monitor (Interval: ${monitorIntervalSeconds}s)`);
            // --- Logging Added Around Interval ---
            monitorInterval = setInterval(() => {
                 // console.log(`[${new Date().toISOString()}] ==== Monitor Interval Fired (${monitorIntervalSeconds}s) ====`); // Reduce log noise
                 try {
                      monitorPayments().catch(err => { // Catch async errors from monitorPayments
                          console.error('[FATAL MONITOR ERROR in setInterval catch]:', err);
                          performanceMonitor.logRequest(false);
                      });
                 } catch (syncErr) { // Catch sync errors immediately within the callback
                      console.error('[FATAL MONITOR SYNC ERROR in setInterval try/catch]:', syncErr);
                      performanceMonitor.logRequest(false);
                 }
                 // console.log(`[${new Date().toISOString()}] ==== Monitor Interval Callback End ====`); // Optional: Log end
             }, monitorIntervalSeconds * 1000);
            // --- End Logging Added ---


            // Run monitor once shortly after startup
            setTimeout(() => {
                 console.log("⚙️ Performing initial payment monitor run...");
                 // Also wrap this initial call
                 try {
                      monitorPayments().catch(err => {
                          console.error('[FATAL MONITOR ERROR in initial run catch]:', err);
                          performanceMonitor.logRequest(false);
                      });
                 } catch (syncErr) {
                      console.error('[FATAL MONITOR SYNC ERROR in initial run try/catch]:', syncErr);
                      performanceMonitor.logRequest(false);
                 }
            }, 5000); // Delay initial run slightly

            // Start polling if webhook is NOT used (e.g., local development)
            if (!process.env.RAILWAY_ENVIRONMENT && !process.env.RAILWAY_PUBLIC_DOMAIN) { // Check domain too
                console.log("ℹ️ Starting bot polling for local development...");
                // Delete any existing webhook first to avoid conflicts
                bot.deleteWebHook({ drop_pending_updates: true })
                    .then(() => bot.startPolling())
                    .then(() => console.log("✅ Bot polling started successfully"))
                    .catch(err => {
                         console.error("❌ Error starting polling:", err.message);
                         // Handle specific polling errors if needed
                         if (err.message.includes('409 Conflict')) {
                             console.error("❌❌❌ Conflict detected! Another instance might be polling.");
                             process.exit(1);
                         }
                    });
            }
        });

        // Handle server errors (e.g., port already in use)
        server.on('error', (err) => {
            console.error('❌ Server error:', err);
            if (err.code === 'EADDRINUSE') {
                console.error(`❌❌❌ Port ${PORT} is already in use. Is another instance running? Exiting.`);
                process.exit(1);
            }
            // Handle other server errors if necessary
        });

    } catch (error) {
        // Catch errors during the overall startup process (DB init, webhook setup)
        console.error("🔥🔥🔥 Failed to start application:", error);
        process.exit(1); // Exit if critical startup steps fail
    }
}

// Graceful shutdown handler
const shutdown = async (signal) => {
    console.log(`\n🛑 ${signal} received, shutting down gracefully...`);
    isMonitorRunning = true; // Prevent monitor from starting new cycle during shutdown

    // 1. Stop receiving new events/requests
    console.log("Stopping incoming connections...");
    if (monitorInterval) {
        clearInterval(monitorInterval);
        console.log("- Stopped payment monitor interval.");
    }
    try {
        let webhookDeleted = false;
         if (process.env.RAILWAY_ENVIRONMENT && process.env.RAILWAY_PUBLIC_DOMAIN) {
             try {
                 await bot.deleteWebHook({ drop_pending_updates: true });
                 console.log("- Removed Telegram webhook.");
                 webhookDeleted = true;
             } catch (whErr) {
                 console.error("⚠️ Error removing webhook:", whErr.message);
             }
         }
         // Only stop polling if webhook wasn't set/deleted OR if polling was explicitly started
         if (!webhookDeleted && bot.isPolling()) {
              await bot.stopPolling({ cancel: true }); // Cancel polling
              console.log("- Stopped Telegram polling.");
         }
         // Close Express server? (Usually handled by Railway/Docker itself)
         // server.close(() => console.log("- Express server closed."));
    } catch (e) {
        console.error("⚠️ Error stopping bot listeners:", e.message);
    }

    // 2. Wait for ongoing queue processing to finish (with timeout)
    console.log("Waiting for active jobs to finish...");
    try {
         // Add timeout logic for queue draining
         await Promise.race([
             Promise.all([
                 messageQueue.onIdle(),
                 paymentProcessor.highPriorityQueue.onIdle(),
                 paymentProcessor.normalQueue.onIdle()
             ]),
             new Promise((_, reject) => setTimeout(() => reject(new Error('Queue drain timeout')), 8000)) // 8s timeout
         ]);
         console.log("- All processing queues are idle.");
    } catch (queueError) {
         console.warn("⚠️ Timed out waiting for queues or queue error during shutdown:", queueError.message);
         // Clear queues to prevent processing during shutdown
         messageQueue.clear();
         paymentProcessor.highPriorityQueue.clear();
         paymentProcessor.normalQueue.clear();
    }


    // 3. Close database pool
    console.log("Closing database pool...");
    try {
        await pool.end(); // Close all connections in the pool
        console.log("✅ Database pool closed.");
    } catch (dbErr) {
        console.error("❌ Error closing database pool:", dbErr);
    } finally {
        console.log("🛑 Shutdown complete.");
        process.exit(0); // Exit cleanly
    }
};

// Register signal handlers for graceful shutdown
process.on('SIGINT', () => shutdown('SIGINT')); // Ctrl+C
process.on('SIGTERM', () => shutdown('SIGTERM')); // Termination signal (e.g., from Docker/Railway)

// Handle uncaught exceptions (log and attempt graceful shutdown)
process.on('uncaughtException', (err, origin) => {
    console.error(`🔥🔥🔥 Uncaught Exception at: ${origin}`, err);
    // Attempt graceful shutdown, but exit quickly if it fails
    shutdown('UNCAUGHT_EXCEPTION').catch(() => process.exit(1));
    setTimeout(() => process.exit(1), 10000).unref(); // Force exit after 10s if shutdown hangs
});

// Handle unhandled promise rejections (log them)
process.on('unhandledRejection', (reason, promise) => {
    console.error('🔥🔥🔥 Unhandled Rejection at:', promise, 'reason:', reason);
    // Consider shutting down on unhandled rejections too? Maybe optional.
    // shutdown('UNHANDLED_REJECTION').catch(() => process.exit(1));
    // setTimeout(() => process.exit(1), 10000).unref();
});

// --- Start the Application ---
startServer().then(() => {
    console.log("🚀🚀🚀 Solana Gambles Bot is up and running! 🚀🚀🚀");
}).catch(err => {
    // This catch is for errors thrown *during* the async startServer call itself
    console.error("🔥🔥🔥 Application failed to initialize:", err);
    process.exit(1);
});
