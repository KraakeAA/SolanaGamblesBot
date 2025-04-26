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
// Ensure crypto is imported if not already globally available in your Node version
import * as crypto from 'crypto'; // Import crypto module
import PQueue from 'p-queue';
// Assuming RateLimitedConnection is correctly implemented in this path
import RateLimitedConnection from './lib/solana-connection.js';


console.log("⏳ Starting Solana Gambles Bot... Checking environment variables...");
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
    console.error("❌ Exiting due to missing environment variables."); process.exit(1);
}

// Set default fee margin if not specified (e.g., 0.00005 SOL)
if (!process.env.FEE_MARGIN) {
    process.env.FEE_MARGIN = '5000'; // Default to 5000 lamports
}
console.log(`ℹ️ Using FEE_MARGIN: ${process.env.FEE_MARGIN} lamports`);

// --- NEW: Startup State Tracking ---
let isFullyInitialized = false;

// --- Initialize Scalable Components ---
const app = express();

// --- NEW: Railway Health Check Endpoint (MODIFIED as per Recommendation) ---
// Add this near the top, after app initialization, before other routes
app.get('/health', (req, res) => {
    if (isFullyInitialized) {
        res.status(200).json({
            status: 'ready', // Changed from 'ok'
            uptime: process.uptime() // Added uptime
        });
    } else {
        // Railway specifically looks for 200 during startup
        res.status(200).json({ // Changed from 503
            status: 'starting', // Changed from 'initializing'
            uptime: process.uptime() // Added uptime
        });
    }
});
// --- END NEW ---

// 1. Enhanced Solana Connection with Rate Limiting
console.log("⚙️ Initializing scalable Solana connection...");
const solanaConnection = new RateLimitedConnection(process.env.RPC_URL, {
    maxConcurrent: 3,      // Max parallel requests
    retryBaseDelay: 600,   // Initial delay for retries (ms)
    commitment: 'confirmed', // Default commitment level
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
    timeout: 10000     // Max time per message task (ms)
});
console.log("✅ Message processing queue initialized");

// 3. Enhanced PostgreSQL Pool
console.log("⚙️ Setting up optimized PostgreSQL Pool...");
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    max: 15,                       // Max connections in pool
    min: 5,                        // Min connections maintained
    idleTimeoutMillis: 30000,    // Close idle connections after 30s
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
        await client.query(`ALTER TABLE bets ADD COLUMN IF NOT EXISTS priority INT DEFAULT 0;`);
        await client.query(`ALTER TABLE bets ADD COLUMN IF NOT EXISTS fees_paid BIGINT;`);


        // Add indexes for performance on frequently queried columns
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_status ON bets(status);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_user_id ON bets(user_id);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_expires_at ON bets(expires_at);`);
        // --- Removed old memo index creation here, replaced below ---
        // await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_memo_id ON bets(memo_id);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_priority ON bets(priority);`);

        // --- NEW: Add improved unique index for memo_id ---
        await client.query(`
            CREATE UNIQUE INDEX IF NOT EXISTS idx_bets_memo_id
            ON bets (memo_id)
            INCLUDE (status, expected_lamports, expires_at);
        `);
        // --- END NEW ---

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
    polling: false, // Use webhooks in production (set later in startServer)
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


// [PATCHED: THROTTLED TELEGRAM SEND]
const telegramSendQueue = new PQueue({ concurrency: 1, interval: 1000, intervalCap: 1 });

function safeSendMessage(chatId, message, options = {}) {
    return telegramSendQueue.add(() =>
        bot.sendMessage(chatId, message, options).catch(err => { // Corrected: call bot.sendMessage directly
            console.error("❌ Telegram send error:", err.message);
            // Optionally re-throw or handle specific errors (e.g., 403 Forbidden)
        })
    );
}

console.log("✅ Telegram Bot initialized");

// --- Express Setup for Webhook and Health Check ---
app.use(express.json({
    limit: '10kb', // Limit payload size
    verify: (req, res, buf) => { // Keep raw body if needed for signature verification (not used here)
        req.rawBody = buf;
    }
}));

// Original Health check / Info endpoint (keep for manual checks)
app.get('/', (req, res) => {
    performanceMonitor.logRequest(true);
    res.status(200).json({
        status: 'ok',
        initialized: isFullyInitialized, // Include initialization status
        timestamp: new Date().toISOString(),
        version: '2.0.3', // Bot version (incremented for health check change)
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

// --- START: Enhanced Memo Handling System ---

// 1. Revised Memo Generation with Checksum
function generateMemoId(prefix = 'BET') {
    const validPrefixes = ['BET', 'CF', 'RA'];
    if (!validPrefixes.includes(prefix)) {
        prefix = 'BET';
    }

    // Generate 8-byte random buffer (64-bit) instead of 6-byte
    const randomBytes = crypto.randomBytes(8); // Use crypto directly
    const hexString = randomBytes.toString('hex').toUpperCase();

    // Add simple checksum (last 2 chars of SHA256 hash)
    const checksum = crypto.createHash('sha256') // Use crypto directly
        .update(hexString)
        .digest('hex')
        .slice(-2)
        .toUpperCase();

    return `${prefix}-${hexString}-${checksum}`;
}

// 2. Strict Memo Validation with Checksum Verification
function validateOriginalMemoFormat(memo) {
    if (typeof memo !== 'string') return false;

    // New format: PREFIX-HEX16-CHECKSUM (e.g., CF-A1B2C3D4E5F6A7B8-9F)
    const parts = memo.split('-');
    if (parts.length !== 3) return false;

    const [prefix, hex, checksum] = parts;
    return (
        ['BET', 'CF', 'RA'].includes(prefix) &&
        hex.length === 16 &&
        /^[A-F0-9]{16}$/.test(hex) &&
        checksum.length === 2 &&
        /^[A-F0-9]{2}$/.test(checksum) &&
        validateMemoChecksum(hex, checksum) // Use new checksum validator
    );
}

// New helper function to validate the checksum part
function validateMemoChecksum(hex, checksum) {
    const expectedChecksum = crypto.createHash('sha256') // Use crypto directly
        .update(hex)
        .digest('hex')
        .slice(-2)
        .toUpperCase();
    return expectedChecksum === checksum;
}


// 3. Robust Memo Normalization with Fallbacks
function normalizeMemo(rawMemo) {
    if (typeof rawMemo !== 'string') return null;

    // Cleanup steps
    let memo = rawMemo
        .trim()
        .replace(/^memo[:=\s]*/i, '') // More flexible prefix removal
        .replace(/^text[:=\s]*/i, '') // Handle text prefix
        .replace(/[\u200B-\u200D\uFEFF]/g, '') // Remove zero-width chars often added by wallets
        .replace(/\s+/g, '-') // Replace multiple whitespace chars with a single hyphen
        .replace(/[^a-zA-Z0-9\-]/g, '') // Remove remaining invalid characters aggressively
        .toUpperCase(); // Standardize case early

    // Attempt to parse based on expected format (PREFIX-HEX16-CHECKSUM)
    const parts = memo.split('-').filter(p => p.length > 0); // Split and remove empty segments

    // Ideal case: Exactly 3 parts matching format structure
    if (parts.length === 3) {
        const [prefix, hex, checksum] = parts;
        // Validate structure and checksum
        if (['BET', 'CF', 'RA'].includes(prefix) &&
            hex.length === 16 && /^[A-F0-9]{16}$/.test(hex) &&
            checksum.length === 2 && /^[A-F0-9]{2}$/.test(checksum)) {
            if (validateMemoChecksum(hex, checksum)) {
                return `${prefix}-${hex}-${checksum}`; // Return perfectly formatted memo
            } else {
                console.warn(`Normalized memo "${memo}" failed checksum validation.`);
                return null; // Invalid checksum
            }
        }
    }

    // Recovery Case 1: Missing checksum but valid prefix and 16-char hex?
    if (parts.length === 2) {
        const [prefix, hex] = parts;
        if (['BET', 'CF', 'RA'].includes(prefix) && hex.length === 16 && /^[A-F0-9]{16}$/.test(hex)) {
            console.warn(`Attempting to recover memo "${memo}": Missing checksum. Calculating...`);
            // Calculate the expected checksum
            const expectedChecksum = crypto.createHash('sha256')
                .update(hex)
                .digest('hex')
                .slice(-2)
                .toUpperCase();
            const recoveredMemo = `${prefix}-${hex}-${expectedChecksum}`;
            console.warn(`Recovered memo as: ${recoveredMemo}`);
            // Validate the recovered memo format (should pass) before returning
            return validateOriginalMemoFormat(recoveredMemo) ? recoveredMemo : null;
        }
    }

    // Recovery Case 2: Extra segments? Try to extract valid prefix, hex, checksum
    if (parts.length > 3) {
        const prefix = parts[0];
        if (['BET', 'CF', 'RA'].includes(prefix)) {
            // Assume the hex part might be split, try joining segments
            const potentialHex = parts.slice(1, -1).join('');
            const potentialChecksum = parts.slice(-1)[0]; // Last segment

            // Check if the joined hex is 16 chars and the checksum is 2 chars
            if (potentialHex.length === 16 && /^[A-F0-9]{16}$/.test(potentialHex) &&
                potentialChecksum.length === 2 && /^[A-F0-9]{2}$/.test(potentialChecksum))
            {
                if (validateMemoChecksum(potentialHex, potentialChecksum)) {
                    const recoveredMemo = `${prefix}-${potentialHex}-${potentialChecksum}`;
                    console.warn(`Recovered memo from extra segments "${memo}" to: ${recoveredMemo}`);
                    return recoveredMemo;
                }
            }
        }
    }

    // If no valid format or recovery attempt worked
    // console.log(`Memo "${rawMemo}" could not be normalized to a valid format.`); // Reduce log noise
    return null;
}


// 4. Enhanced Transaction Memo Search
function findMemoInTx(tx) {
    // Added null checks for tx and nested properties
    if (!tx?.transaction?.message?.instructions || !tx?.transaction?.message?.accountKeys) {
        return null;
    }

    try {
        const MEMO_PROGRAM_IDS = new Set([
            'Memo1UhkJRfHyvLMcVuc6beZNRYqUP2VZwW', // v1
            'MemoSq4gqABAXKb96qnH8TysNcVtrp5GktfD'  // v2
        ]);

        // Combine instructions and inner instructions
        const instructions = [
            ...(tx.transaction.message.instructions || []),
            ...(tx.meta?.innerInstructions || []).flatMap(i => i.instructions || [])
        ];

        for (const inst of instructions) {
            // Safely get program ID using index
            const programId = inst.programIdIndex !== undefined
                                ? tx.transaction.message.accountKeys[inst.programIdIndex]?.pubkey?.toBase58()
                                : null; // Handle case where index might be missing or invalid

            if (!programId || !MEMO_PROGRAM_IDS.has(programId)) continue;

            // Safely decode data, handle potential errors
            let rawMemo = null;
            try {
                const data = bs58.decode(inst.data || '');
                rawMemo = data.toString('utf8').replace(/\0/g, ''); // Remove null bytes often padded
            } catch (decodeError) {
                // console.warn(`Error decoding instruction data for potential memo: ${decodeError.message}`); // Debug log
                continue; // Skip if data is invalid
            }

            // Attempt to normalize the raw memo text
            const normalized = normalizeMemo(rawMemo);

            // Return the first valid, normalized, and checksum-verified memo found
            if (normalized && validateOriginalMemoFormat(normalized)) {
                // console.log(`Found and validated memo: ${normalized} from raw: ${rawMemo}`); // Debug log
                return normalized;
            } else if (normalized) {
                // console.warn(`Normalized memo "${normalized}" failed strict validation.`); // Debug if normalized but didn't pass final check
            }
        }
    } catch (e) {
        console.error("Memo parsing error:", e);
    }
    return null; // No valid memo found
}

// --- END: Enhanced Memo Handling System ---


// --- Database Operations ---

// Saves a new bet intention to the database
async function savePendingBet(userId, chatId, gameType, details, lamports, memoId, expiresAt, priority = 0) {
    // Use the STRICT validation for the generated memo ID before saving
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
        if (err.code === '23505' && err.constraint === 'bets_memo_id_key') { // Check constraint name if available
            console.warn(`DB: Memo ID collision for ${memoId}. User might be retrying.`);
            return { success: false, error: 'Memo ID already exists. Please try generating the bet again.' };
        }
        return { success: false, error: err.message };
    }
}

// Finds a pending bet by its unique memo ID
async function findBetByMemo(memoId) {
    // Use the STRICT validation when searching by memo ID
    if (!validateOriginalMemoFormat(memoId)) {
        // console.warn(`DB: findBetByMemo called with invalid format: ${memoId}`); // Reduce noise
        return undefined;
    }

    // Select the highest priority bet first if multiple match (unlikely with unique memo)
    // Use FOR UPDATE SKIP LOCKED to prevent race conditions if multiple monitors pick up the same TX
    // Select only necessary columns initially
    const query = `
        SELECT id, user_id, chat_id, game_type, bet_details, expected_lamports, status, expires_at, fees_paid, priority
        FROM bets
        WHERE memo_id = $1 AND status = 'awaiting_payment'
        ORDER BY priority DESC, created_at ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED;
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
        if (err.code === '23505' && err.constraint === 'bets_paid_tx_signature_key') {
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
        RETURNING wallet_address; -- Return just the address
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
            return { success: true, wallet: linkedWallet };
        } else {
            console.error(`DB: Failed to link/update wallet for user ${userId}. Query returned no address.`);
            return { success: false, error: 'Failed to update wallet in database.' };
        }
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
        if (err.code === '23505' && err.constraint === 'bets_payout_tx_signature_key') {
            console.warn(`DB: Payout TX Signature ${signature} collision for bet ${betId}. Already recorded.`);
            return false;
        }
        console.error(`DB Error recording payout for bet ${betId}:`, err.message);
        return false;
    }
}


// --- Solana Transaction Analysis ---

// Analyzes a transaction to find SOL transfers to the target bot wallet
function analyzeTransactionAmounts(tx, walletType) {
    let transferAmount = 0n; // Use BigInt for lamports
    let payerAddress = null;
    const targetAddress = walletType === 'coinflip'
        ? process.env.MAIN_WALLET_ADDRESS
        : process.env.RACE_WALLET_ADDRESS;

    if (tx?.meta?.err) {
        // console.warn(`Skipping amount analysis for failed TX: ${tx.transaction.signatures[0]}`);
        return { transferAmount: 0n, payerAddress: null }; // Ignore failed transactions
    }

    // Check both pre and post balances for direct transfers to the target address
    if (tx?.meta?.preBalances && tx?.meta?.postBalances && tx?.transaction?.message?.accountKeys) {
        const accountKeys = tx.transaction.message.accountKeys.map(keyInfo =>
            keyInfo?.pubkey ? keyInfo.pubkey.toBase58() : (typeof keyInfo === 'string' ? new PublicKey(keyInfo).toBase58() : null)
        );
        const targetIndex = accountKeys.indexOf(targetAddress);

        if (targetIndex !== -1 && tx.meta.preBalances[targetIndex] !== undefined && tx.meta.postBalances[targetIndex] !== undefined) {
            const balanceChange = BigInt(tx.meta.postBalances[targetIndex]) - BigInt(tx.meta.preBalances[targetIndex]);
            if (balanceChange > 0n) {
                transferAmount = balanceChange;
                // Try to identify payer based on who lost balance
                for (let i = 0; i < accountKeys.length; i++) {
                    if (i === targetIndex || accountKeys[i] === null) continue;
                    const payerBalanceChange = BigInt(tx.meta.postBalances[i] || 0) - BigInt(tx.meta.preBalances[i] || 0);
                    // Consider the fee payer (account 0) as the primary suspect if their balance decreased
                    if (i === 0 && payerBalanceChange < 0n) {
                        payerAddress = accountKeys[i];
                        break; // Assume fee payer is the sender
                    }
                    // Otherwise, look for a signer whose balance decreased appropriately
                    const isSigner = tx.transaction.message.accountKeys[i]?.signer || (tx.transaction.message.header?.numRequiredSignatures > 0 && i < tx.transaction.message.header.numRequiredSignatures);
                    if (isSigner && payerBalanceChange <= -transferAmount) { // Allow for fees
                        payerAddress = accountKeys[i];
                        break;
                    }
                }
                // If payer still not found, fallback to fee payer if their balance decreased
                if (!payerAddress && accountKeys[0] && (BigInt(tx.meta.postBalances[0] || 0) - BigInt(tx.meta.preBalances[0] || 0)) < 0n) {
                    payerAddress = accountKeys[0];
                }
            }
        }
    }

    // Fallback or supplement with instruction parsing (less reliable for exact amount sometimes)
    if (transferAmount === 0n && tx?.transaction?.message?.instructions) {
        const instructions = [
            ...(tx.transaction.message.instructions || []),
            ...(tx.meta?.innerInstructions || []).flatMap(i => i.instructions || [])
        ];
        const SYSTEM_PROGRAM_ID = SystemProgram.programId.toBase58();

        for (const inst of instructions) {
            let programId = '';
            if (inst.programIdIndex !== undefined && tx.transaction.message.accountKeys) {
                const keyInfo = tx.transaction.message.accountKeys[inst.programIdIndex];
                programId = keyInfo?.pubkey ? keyInfo.pubkey.toBase58() : (typeof keyInfo === 'string' ? new PublicKey(keyInfo).toBase58() : '');
            } else if (inst.programId) {
                programId = inst.programId.toBase58 ? inst.programId.toBase58() : inst.programId.toString();
            }

            // Check for SystemProgram SOL transfers
            if (programId === SYSTEM_PROGRAM_ID && inst.parsed?.type === 'transfer') {
                const transferInfo = inst.parsed.info;
                // Check if the destination is the bot's target wallet
                if (transferInfo.destination === targetAddress) {
                    const instructionAmount = BigInt(transferInfo.lamports || transferInfo.amount || 0);
                    if (instructionAmount > 0n) {
                        transferAmount = instructionAmount; // Take the amount from the first relevant transfer instruction
                        payerAddress = transferInfo.source; // Get payer from instruction
                        break; // Assume the first direct transfer is the one we care about
                    }
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

    // Fallback logic remains the same (checking signers and balance changes)
    const preBalances = tx.meta.preBalances;
    const postBalances = tx.meta.postBalances;
    if (!preBalances || !postBalances || preBalances.length !== postBalances.length || preBalances.length !== message.accountKeys.length) {
        console.warn("Inconsistent balance/account key data, cannot reliably determine payer by balance change.");
        return null;
    }

    for (let i = 0; i < message.accountKeys.length; i++) {
        // Check if the account is a signer in the transaction message
        const isSigner = message.accountKeys[i]?.signer || (message.header?.numRequiredSignatures > 0 && i < message.header.numRequiredSignatures);
        if (isSigner) {
            let key;
            try {
                if (message.accountKeys[i].pubkey) {
                    key = message.accountKeys[i].pubkey instanceof PublicKey ? message.accountKeys[i].pubkey : new PublicKey(message.accountKeys[i].pubkey);
                } else if (typeof message.accountKeys[i] === 'string') {
                    key = new PublicKey(message.accountKeys[i]);
                } else {
                    continue;
                }
            } catch (e) { continue; } // Skip if key is invalid

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
    const msg = error?.message?.toLowerCase() || '';
    const code = error?.code?.toLowerCase() || ''; // Include error code check

    // Common HTTP/Network errors
    if (msg.includes('429') || // HTTP 429 Too Many Requests
        msg.includes('timeout') ||
        msg.includes('timed out') ||
        msg.includes('rate limit') ||
        msg.includes('econnreset') || // Connection reset
        msg.includes('esockettimedout') ||
        msg.includes('network error') ||
        msg.includes('fetch') || // Generic fetch errors
        code === 'etimedout')
    {
        return true;
    }

    // Solana specific potentially transient errors (check error codes/messages from web3.js or RPC responses)
    // Note: This needs refinement based on actual observed retryable Solana errors
    // if (msg.includes('blockhash not found') || msg.includes('slot lag')) {
    //     return true; // Example: Blockhash might become valid shortly after
    // }

    // Database potentially transient errors
    if (msg.includes('connection terminated') || code === 'econnrefused') { // Basic examples
        return true;
    }


    return false;
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
        const queue = (job.priority && job.priority > 0) ? this.highPriorityQueue : this.normalQueue;
        // Add job wrapped in error handling
        queue.add(() => this.processJob(job)).catch(queueError => {
            console.error(`Queue error processing job ${job.type} (${job.signature || job.betId || 'N/A'}):`, queueError.message);
            // Handle queue-level errors if needed, e.g., log differently
            performanceMonitor.logRequest(false);
        });
    }

    // Wrapper to handle job execution, retries, and error logging
    async processJob(job) {
        // Prevent processing the same signature concurrently if added multiple times quickly
        const jobIdentifier = job.signature || job.betId; // Use signature or betId as identifier
        if (jobIdentifier && this.activeProcesses.has(jobIdentifier)) { // Check only if identifier exists
            // console.warn(`Job for identifier ${jobIdentifier} already active, skipping duplicate.`); // Reduce log noise
            return;
        }
        if (jobIdentifier) this.activeProcesses.add(jobIdentifier); // Mark as active if identifier exists

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
            console.error(`Error processing job type ${job.type} for identifier ${jobIdentifier || 'N/A'}:`, error.message);

            // Retry logic for retryable errors (only for initial payment check for now)
            if (job.type === 'monitor_payment' && (job.retries || 0) < 3 && isRetryableError(error)) {
                job.retries = (job.retries || 0) + 1;
                console.log(`Retrying job for signature ${job.signature} (Attempt ${job.retries})...`);
                await new Promise(resolve => setTimeout(resolve, 1000 * job.retries)); // Exponential backoff
                // Release active lock before requeueing for retry
                if (jobIdentifier) this.activeProcesses.delete(jobIdentifier);
                // Re-add the job for retry (use await here to ensure it's queued before finally)
                await this.addPaymentJob(job);
                return; // Prevent falling through to finally block immediately after requeueing
            } else {
                // Log final failure or non-retryable error
                console.error(`Job failed permanently or exceeded retries for identifier: ${jobIdentifier}`, error);
                // Potentially update bet status to an error state if applicable and if betId exists
                if(job.betId && job.type !== 'monitor_payment') { // Only update status for non-monitor failures with betId
                    await updateBetStatus(job.betId, `error_${job.type}_failed`);
                }
            }
        } finally {
            // Always remove identifier from active set when processing finishes (success or final fail)
            if (jobIdentifier) {
                this.activeProcesses.delete(jobIdentifier);
            }
        }
    }

    // Core logic to process an incoming payment transaction signature
    async _processIncomingPayment(signature, walletType, attempt = 0) {
        // 1. Check session cache first (quickest check)
        if (processedSignaturesThisSession.has(signature)) {
            // console.log(`Sig ${signature} already processed this session.`); // Reduce noise
            return { processed: false, reason: 'already_processed_session' };
        }

        // 2. Check database if already recorded as paid (using paid_tx_signature)
        const checkQuery = `SELECT id FROM bets WHERE paid_tx_signature = $1 LIMIT 1;`;
        try {
            const processed = await pool.query(checkQuery, [signature]);
            if (processed.rowCount > 0) {
                // console.log(`Sig ${signature} already exists in DB.`); // Reduce noise
                processedSignaturesThisSession.add(signature); // Add to session cache too
                return { processed: false, reason: 'exists_in_db' };
            }
        } catch (dbError) {
            console.error(`DB Error checking signature ${signature}:`, dbError.message);
            if (isRetryableError(dbError) && attempt < 3) throw dbError; // Re-throw retryable errors for the job handler
            return { processed: false, reason: 'db_check_error' };
        }

        // 3. Fetch the transaction details from Solana
        console.log(`Processing transaction details for signature: ${signature} (Attempt ${attempt + 1})`);
        let tx;
        try {
            tx = await solanaConnection.getParsedTransaction(
                signature,
                { maxSupportedTransactionVersion: 0 } // Request parsed format
            );
        } catch (fetchError) {
            console.error(`Failed to fetch TX ${signature}: ${fetchError.message}`);
            if (isRetryableError(fetchError) && attempt < 3) throw fetchError; // Re-throw retryable errors
            processedSignaturesThisSession.add(signature); // Mark as processed if fetch fails permanently
            return { processed: false, reason: 'tx_fetch_failed' };
        }

        // 4. Validate transaction fetched
        if (!tx) {
            console.warn(`Transaction ${signature} not found or null after fetch.`);
            // Consider if this state is permanent or potentially retryable (e.g., RPC lag)
            // For now, treat as permanent failure after retries handled by processJob
            processedSignaturesThisSession.add(signature);
            return { processed: false, reason: 'tx_not_found' };
        }
        if (tx.meta?.err) {
            console.log(`Transaction ${signature} failed on-chain: ${JSON.stringify(tx.meta.err)}`);
            processedSignaturesThisSession.add(signature);
            return { processed: false, reason: 'tx_onchain_error' };
        }

        // 5. Find and validate the memo using the enhanced findMemoInTx
        const memo = findMemoInTx(tx); // Uses normalizeMemo and checksum validation internally now

        // --- NEW: Insert enhanced memo validation checks ---
        if (!memo) { // findMemoInTx now returns null if normalization/validation fails
            // console.log(`Transaction ${signature} does not contain a valid game memo after normalization/validation.`); // Reduce noise
            processedSignaturesThisSession.add(signature);
            return { processed: false, reason: 'no_valid_memo' };
        }
        // Memo format and checksum are already validated by findMemoInTx returning a non-null value
        // The validateOriginalMemoFormat check here is redundant if findMemoInTx implements it correctly.
        // We rely on findMemoInTx to return a strictly validated memo or null.
        // --- END: Insert enhanced memo validation checks ---

        // 6. Find the corresponding bet in the database using the validated memo
        const bet = await findBetByMemo(memo); // Uses strict check internally
        if (!bet) {
            console.warn(`No matching pending bet found for memo "${memo}" from TX ${signature} (strict format search).`);
            processedSignaturesThisSession.add(signature);
            return { processed: false, reason: 'no_matching_bet_strict' };
        }
        // If found, proceed with the bet object we retrieved
        console.log(`Processing payment TX ${signature} with validated memo: ${memo} for Bet ID: ${bet.id}`);

        // 7. Check bet status (already filtered by findBetByMemo, but good defense)
        if (bet.status !== 'awaiting_payment') {
            console.warn(`Bet ${bet.id} found for memo ${memo} but status is ${bet.status}, not 'awaiting_payment'.`);
            processedSignaturesThisSession.add(signature); // Mark sig processed even if bet state is wrong
            return { processed: false, reason: 'bet_already_processed' };
        }

        // 8. Analyze transaction amounts
        const { transferAmount, payerAddress } = analyzeTransactionAmounts(tx, walletType);
        if (transferAmount <= 0n) {
            console.warn(`No SOL transfer to target wallet found in TX ${signature} for memo ${memo}.`);
            // Consider if this TX should be marked processed or if amount analysis could be flaky
            processedSignaturesThisSession.add(signature);
            return { processed: false, reason: 'no_transfer_found' };
        }

        // 9. Validate amount sent vs expected (with tolerance)
        const expectedAmount = BigInt(bet.expected_lamports);
        const tolerance = BigInt(5000); // Allow small deviation (e.g., 0.000005 SOL) - Adjust as needed
        if (transferAmount < (expectedAmount - tolerance) ||
            transferAmount > (expectedAmount + tolerance)) {
            console.warn(`Amount mismatch for bet ${bet.id} (memo ${memo}). Expected ~${expectedAmount}, got ${transferAmount}.`);
            await updateBetStatus(bet.id, 'error_payment_mismatch');
            processedSignaturesThisSession.add(signature);
            await safeSendMessage(bet.chat_id, `⚠️ Payment amount mismatch for bet ${memo}. Expected ${Number(expectedAmount)/LAMPORTS_PER_SOL} SOL, received ${Number(transferAmount)/LAMPORTS_PER_SOL} SOL. Bet cancelled.`).catch(e => console.error("TG Send Error:", e.message));
            return { processed: false, reason: 'amount_mismatch' };
        }

        // 10. Validate transaction time vs bet expiry
        const txTime = tx.blockTime ? new Date(tx.blockTime * 1000) : null;
        if (!txTime) {
            console.warn(`Could not determine blockTime for TX ${signature}. Skipping expiry check.`);
        } else if (txTime > new Date(bet.expires_at)) {
            console.warn(`Payment for bet ${bet.id} (memo ${memo}) received after expiry.`);
            await updateBetStatus(bet.id, 'error_payment_expired');
            processedSignaturesThisSession.add(signature);
            await safeSendMessage(bet.chat_id, `⚠️ Payment for bet ${memo} received after expiry time. Bet cancelled.`).catch(e => console.error("TG Send Error:", e.message));
            return { processed: false, reason: 'expired' };
        }

        // 11. Mark bet as paid in DB
        const markResult = await markBetPaid(bet.id, signature);
        if (!markResult.success) {
            console.error(`Failed to mark bet ${bet.id} as paid: ${markResult.error}`);
            // Check if error was due to signature collision (already processed by another worker)
            if (markResult.error === 'Transaction signature already recorded' || markResult.error === 'Bet not found or already processed') {
                processedSignaturesThisSession.add(signature); // Mark as processed if collision occurred
                return { processed: false, reason: 'db_mark_paid_collision' };
            }
            // For other DB errors, potentially retry if possible, otherwise mark processed to prevent loop
            processedSignaturesThisSession.add(signature);
            return { processed: false, reason: 'db_mark_paid_failed' };
        }

        // 12. Link wallet address to user ID using the determined payer address
        let actualPayer = payerAddress || getPayerFromTransaction(tx)?.toBase58(); // Use determined payer or fallback
        if (actualPayer) {
            // Ensure payer is valid base58 before linking
            try {
                new PublicKey(actualPayer); // Validate address format
                await linkUserWallet(bet.user_id, actualPayer);
            } catch(e) {
                console.warn(`Identified payer address "${actualPayer}" for bet ${bet.id} is invalid. Cannot link wallet.`);
            }
        } else {
            console.warn(`Could not identify valid payer address for bet ${bet.id} to link wallet.`);
        }

        // 13. Add signature to session cache AFTER successful DB update
        processedSignaturesThisSession.add(signature);
        if (processedSignaturesThisSession.size > MAX_PROCESSED_SIGNATURES) {
            console.log('Clearing processed signatures cache (reached max size)');
            processedSignaturesThisSession.clear();
        }

        // 14. Queue the bet for actual game processing (higher priority)
        console.log(`Payment verified for bet ${bet.id}. Queuing for game processing.`);
        // Use await to ensure job is added before function returns
        await this.addPaymentJob({
            type: 'process_bet',
            betId: bet.id,
            priority: 1, // Higher priority than monitoring
            signature // Pass signature for context/logging if needed
        });

        return { processed: true };
    }
}

const paymentProcessor = new PaymentProcessor();


// --- Payment Monitoring Loop ---
let isMonitorRunning = false; // Flag to prevent concurrent monitor runs

// [PATCHED: TRACK BOT START TIME]
const botStartupTime = Math.floor(Date.now() / 1000); // Timestamp in seconds

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
    // Check if app is fully initialized before running monitor
    if (!isFullyInitialized) {
        console.log('[Monitor] Skipping cycle, application not fully initialized yet.');
        return;
    }

    isMonitorRunning = true;
    const startTime = Date.now();
    let signaturesFoundThisCycle = 0; // Renamed for clarity

    try {
        // --- START: Adaptive Rate Limiting Logic ---
        const currentLoad = paymentProcessor.highPriorityQueue.pending +
                            paymentProcessor.normalQueue.pending +
                            paymentProcessor.highPriorityQueue.size + // Include active items too
                            paymentProcessor.normalQueue.size;

        // Calculate delay: Base delay + delay per pending item, max N seconds
        const baseDelay = 500; // Minimum delay between cycles (ms)
        const delayPerItem = 100; // Additional delay per queued/active item (ms)
        const maxThrottleDelay = 10000; // Max delay (10 seconds)

        const throttleDelay = Math.min(maxThrottleDelay, baseDelay + currentLoad * delayPerItem);

        if (throttleDelay > baseDelay) { // Only log if throttling beyond base delay
            console.log(`[Monitor] Queues have ${currentLoad} pending/active items. Throttling monitor check for ${throttleDelay}ms.`);
            await new Promise(resolve => setTimeout(resolve, throttleDelay));
        } else {
            await new Promise(resolve => setTimeout(resolve, baseDelay)); // Always enforce base delay
        }
        // --- END: Adaptive Rate Limiting Logic ---


        // --- Original Monitor Logic Start ---
        const walletsToMonitor = [
            { address: process.env.MAIN_WALLET_ADDRESS, type: 'coinflip', priority: 0 },
            { address: process.env.RACE_WALLET_ADDRESS, type: 'race', priority: 0 },
        ];

        // Process each wallet
        for (const wallet of walletsToMonitor) {
            let signaturesForWallet = [];
            try {
                // *** PAGINATION FIX: Get the last signature processed for THIS wallet ***
                const beforeSig = lastProcessedSignature[wallet.address] || null;
                // console.log(`[Monitor] Checking ${wallet.type} wallet (${wallet.address}) for signatures before: ${beforeSig || 'Start'}`); // Optional: More detailed logging

                // Fetch latest signatures for the wallet using rate-limited connection
                signaturesForWallet = await solanaConnection.getSignaturesForAddress(
                    new PublicKey(wallet.address),
                    {
                        limit: 25,     // Fetch a slightly larger batch
                        before: beforeSig  // Use the specific 'before' signature for pagination
                    }
                );

                if (!signaturesForWallet || signaturesForWallet.length === 0) {
                    continue; // No new signatures for this wallet
                }

                signaturesFoundThisCycle += signaturesForWallet.length; // Count total found signatures

                // *** PAGINATION FIX: Update the last processed signature state for the *next* query cycle ***
                // Use the *oldest* signature in this batch as the 'before' marker for the next run
                const oldestSignatureInBatch = signaturesForWallet[signaturesForWallet.length - 1].signature;
                // Only update if we actually got signatures
                if (oldestSignatureInBatch) {
                    lastProcessedSignature[wallet.address] = oldestSignatureInBatch;
                    // console.log(`[Monitor] Updated last signature for ${wallet.address} to: ${oldestSignatureInBatch}`); // Optional: Logging
                }
                // *** END PAGINATION FIX ***

                // Queue each found signature for processing by the PaymentProcessor
                for (const sigInfo of signaturesForWallet) {
                    // Skip transactions older than bot startup time (with a small buffer)
                    if (sigInfo.blockTime && sigInfo.blockTime < botStartupTime - 60) { // 60 second buffer
                        // console.log(`🕒 Skipping old TX: ${sigInfo.signature} (BlockTime: ${sigInfo.blockTime} < Startup: ${botStartupTime})`); // Reduce noise
                        continue;
                    }

                    // Double check session cache before queuing
                    if (processedSignaturesThisSession.has(sigInfo.signature)) {
                        continue;
                    }
                    // Queue the job (use await to ensure it's added before loop continues, helps backpressure)
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
                // Don't reset lastProcessedSignature on error, rely on next successful run
            }
        } // End loop through wallets
        // --- Original Monitor Logic End ---

    } catch (error) {
        console.error("[Monitor] Unexpected error in main monitor loop:", error);
        performanceMonitor.logRequest(false);
    } finally {
        isMonitorRunning = false; // Release the lock
        const duration = Date.now() - startTime;
        if (signaturesFoundThisCycle > 0) { // Only log duration if something happened
            console.log(`[Monitor] Cycle finished in ${duration}ms. Found ${signaturesFoundThisCycle} new signatures.`);
        }

        // Adjust monitoring interval based on activity
        adjustMonitorInterval(signaturesFoundThisCycle);

        // console.log(`[${new Date().toISOString()}] ---- monitorPayments function END ----`); // Reduce log noise
    }
}
// *** END UPDATED FUNCTION ***


// Adjusts the monitor interval based on how many new signatures were found
function adjustMonitorInterval(processedCount) {
    let newInterval = monitorIntervalSeconds;
    const minInterval = 15; // Minimum interval (seconds)
    const maxInterval = 120; // Maximum interval (seconds)
    const step = 10; // How much to change interval by

    if (processedCount > 10) { // High activity, check sooner
        newInterval = Math.max(minInterval, monitorIntervalSeconds - step);
    } else if (processedCount === 0) { // No activity, check less often
        newInterval = Math.min(maxInterval, monitorIntervalSeconds + step);
    } // else: Moderate activity, keep interval the same

    if (newInterval !== monitorIntervalSeconds) {
        monitorIntervalSeconds = newInterval;
        if (monitorInterval) clearInterval(monitorInterval); // Clear existing interval

        monitorInterval = setInterval(() => { // Reschedule with new interval
            // console.log(`[${new Date().toISOString()}] ==== Monitor Interval Fired (${monitorIntervalSeconds}s) ====`); // Log interval fire
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

    let recipientPubKey;
    try {
        recipientPubKey = (typeof recipientPublicKey === 'string')
            ? new PublicKey(recipientPublicKey)
            : recipientPublicKey;
        if (!(recipientPubKey instanceof PublicKey)) throw new Error("Invalid recipient public key type");
    } catch (e) {
        console.error(`❌ Invalid recipient address format: ${recipientPublicKey}`);
        return { success: false, error: `Invalid recipient address: ${e.message}` };
    }


    const amountSOL = Number(amountLamports) / LAMPORTS_PER_SOL;
    console.log(`💸 Attempting to send ${amountSOL.toFixed(6)} SOL to ${recipientPubKey.toBase58()} for ${gameType}`);

    // Calculate dynamic priority fee based on payout amount (adjust rate as needed)
    // Ensure fee is integer and within reasonable bounds
    const basePriorityFee = 1000; // Minimum fee in microLamports
    const maxPriorityFee = 1000000; // Max fee in microLamports (0.001 SOL)
    const calculatedFee = Math.floor(Number(amountLamports) * PRIORITY_FEE_RATE);
    const priorityFeeMicroLamports = Math.max(basePriorityFee, Math.min(calculatedFee, maxPriorityFee));


    // Ensure amount is positive BigInt
    const amountToSend = BigInt(amountLamports);
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
                { commitment: 'confirmed' } // Use confirmed blockhash for sending
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
            const confirmationTimeoutMs = 45000; // Increased timeout to 45s
            const signature = await Promise.race([
                sendAndConfirmTransaction(
                    solanaConnection, // Use the rate-limited connection
                    transaction,
                    [payerWallet], // Signer
                    {
                        commitment: 'confirmed', // Confirm at 'confirmed' level
                        skipPreflight: false,    // Perform preflight checks
                        maxRetries: 2,         // Retries within sendAndConfirm (lower internal retries)
                        preflightCommitment: 'confirmed' // Match commitment
                    }
                ),
                // Add a timeout for the confirmation process
                new Promise((_, reject) => {
                    setTimeout(() => {
                        reject(new Error(`Transaction confirmation timeout after ${confirmationTimeoutMs/1000}s (Attempt ${attempt})`));
                    }, confirmationTimeoutMs);
                })
            ]);

            console.log(`✅ Payout successful! Sent ${Number(amountToSend)/LAMPORTS_PER_SOL} SOL to ${recipientPubKey.toBase58()}. TX: ${signature}`);
            return { success: true, signature };

        } catch (error) {
            console.error(`❌ Payout TX failed (Attempt ${attempt}/3):`, error.message);
            // Log full error object for more details if available
            // console.error(error);

            // Check for specific non-retryable errors
            const errorMsg = error.message.toLowerCase();
            if (errorMsg.includes('invalid param') ||
                errorMsg.includes('insufficient funds') || // Bot might be out of SOL
                errorMsg.includes('blockhash not found') || // Treat as non-retryable after sendAndConfirm fails
                errorMsg.includes('custom program error') ||
                errorMsg.includes('account not found') || // e.g., recipient address wrong
                errorMsg.includes('invalid recipient')) { // If our validation missed something
                console.error("❌ Non-retryable error encountered. Aborting payout.");
                return { success: false, error: `Non-retryable error: ${error.message}` }; // Exit retry loop
            }

            // If retryable error and not last attempt, wait before retrying
            if (attempt < 3) {
                await new Promise(resolve => setTimeout(resolve, 2000 * attempt)); // Wait longer each attempt
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
        if (!statusCheck.rows[0] || statusCheck.rows[0].status !== 'payment_verified') {
            console.warn(`Bet ${bet.id} status is ${statusCheck.rows[0]?.status ?? 'not found'}, not 'payment_verified'. Aborting game processing.`);
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
            try { await client.query('ROLLBACK'); } catch (rbError) { console.error("Rollback failed:", rbError); }
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

    // --- Determine Outcome (Simplified & Corrected House Edge Application) ---
    // The house wins if Math.random() falls within the houseEdge percentage
    // Otherwise, it's a 50/50 split.
    const houseWinRoll = Math.random();
    let result;
    let isHouseWin = houseWinRoll < config.houseEdge; // e.g., if edge is 0.02, house wins on 0 to 0.0199...

    if (isHouseWin) {
        // House wins, result is the opposite of the player's choice
        result = (choice === 'heads') ? 'tails' : 'heads';
        console.log(`🪙 Coinflip Bet ${betId}: House edge triggered.`);
    } else {
        // Fair 50/50 flip
        result = (Math.random() < 0.5) ? 'heads' : 'tails';
    }
    // --- End Outcome Determination ---

    const win = (result === choice);

    // Calculate payout amount if win (uses helper function)
    // Pass the *original* expected lamports (the bet amount)
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
            await safeSendMessage(chat_id,
                `🎉 ${displayName}, you won the coinflip (Result: *${result}*) but have no wallet linked!\n` +
                `Your payout of ${payoutSOL.toFixed(6)} SOL is waiting. Place another bet (any amount) to link your wallet and receive pending payouts.`,
                { parse_mode: 'Markdown' }
            ).catch(e => console.error("TG Send Error:", e.message));
            await updateBetStatus(betId, 'completed_win_no_wallet'); // Special status
            return;
        }

        // Send "processing payout" message and queue the actual payout
        try {
            await safeSendMessage(chat_id,
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
                amount: payoutLamports.toString(), // Pass amount as string to avoid BigInt issues in queue/JSON
                gameType: 'coinflip',
                priority: 2, // Higher priority than monitoring/game processing
                chatId: chat_id,
                displayName,
                result
            });

        } catch (e) {
            console.error(`❌ Error queuing payout for coinflip bet ${betId}:`, e);
            await safeSendMessage(chat_id,
                `⚠️ Error occurred while processing your coinflip win for bet ID ${betId}.\n` +
                `Please contact support.`,
                { parse_mode: 'Markdown' }
            ).catch(e => console.error("TG Send Error:", e.message));
            await updateBetStatus(betId, 'error_payout_queueing');
        }

    } else { // Loss
        console.log(`🪙 Coinflip Bet ${betId}: ${displayName} LOST. Choice: ${choice}, Result: ${result}`);
        await safeSendMessage(chat_id,
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
        // Probabilities adjusted slightly to incorporate house edge directly
        // Total win probability should be (1 - houseEdge)
        // Example: For 2% edge, total win prob = 0.98
        // Relative probabilities can be kept, then normalized to 0.98
        { name: 'Yellow', emoji: '🟡', odds: 1.1, baseProb: 0.25 }, // Base probabilities sum to 1
        { name: 'Orange', emoji: '🟠', odds: 2.0, baseProb: 0.20 },
        { name: 'Blue',   emoji: '🔵', odds: 3.0, baseProb: 0.15 },
        { name: 'Cyan',   emoji: '💧', odds: 4.0, baseProb: 0.12 },
        { name: 'White',  emoji: '⚪️', odds: 5.0, baseProb: 0.09 },
        { name: 'Red',    emoji: '🔴', odds: 6.0, baseProb: 0.07 },
        { name: 'Black',  emoji: '⚫️', odds: 7.0, baseProb: 0.05 },
        { name: 'Pink',   emoji: '🌸', odds: 8.0, baseProb: 0.03 },
        { name: 'Purple', emoji: '🟣', odds: 9.0, baseProb: 0.02 },
        { name: 'Green',  emoji: '🟢', odds: 10.0, baseProb: 0.01 },
        { name: 'Silver', emoji: '💎', odds: 15.0, baseProb: 0.01 }
    ];

    const totalBaseProb = horses.reduce((sum, h) => sum + h.baseProb, 0); // Should be ~1.0
    const targetTotalProb = 1.0 - config.houseEdge; // e.g., 0.98

    // Calculate adjusted probabilities
    horses.forEach(h => {
        h.winProbability = (h.baseProb / totalBaseProb) * targetTotalProb;
    });
    // Verify adjusted sum (should be close to targetTotalProb)
    // const adjustedSum = horses.reduce((sum, h) => sum + h.winProbability, 0);
    // console.log("Adjusted race probability sum:", adjustedSum);


    // Determine the winning horse based on adjusted weighted probabilities
    let winningHorse = null;
    const randomNumber = Math.random(); // Random number between 0 and 1
    let cumulativeProbability = 0;

    if (randomNumber < targetTotalProb) { // Player wins branch
        const playerWinRoll = randomNumber / targetTotalProb; // Normalize roll to 0-1 range for player win space
        for (const horse of horses) {
            // Use BASE probability for distribution WITHIN the player win chance
            cumulativeProbability += (horse.baseProb / totalBaseProb);
            if (playerWinRoll <= cumulativeProbability) {
                winningHorse = horse;
                break;
            }
        }
        // Fallback within player win branch (should not be needed if baseProbs sum to 1)
        winningHorse = winningHorse || horses[horses.length - 1];
    } else {
        // House wins branch - Pick a random horse as the "winner" but player loses
        // This horse is just for display, the player already lost due to house edge roll.
        const houseWinnerIndex = Math.floor(Math.random() * horses.length);
        winningHorse = horses[houseWinnerIndex];
        console.log(`🐎 Race Bet ${betId}: House edge triggered.`);
    }


    // Send race commentary messages
    let displayName = await getUserDisplayName(chat_id, user_id);
    try {
        await safeSendMessage(chat_id, `🐎 Race ${betId} starting! ${displayName} bet on ${chosenHorseName}!`).catch(e => console.error("TG Send Error:", e.message));
        await new Promise(resolve => setTimeout(resolve, 2000)); // Pause
        await safeSendMessage(chat_id, "🚦 And they're off!").catch(e => console.error("TG Send Error:", e.message));
        await new Promise(resolve => setTimeout(resolve, 3000)); // Pause
        await safeSendMessage(chat_id,
            `🏆 The winner is... ${winningHorse.emoji} *${winningHorse.name}*! 🏆`,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
    } catch (e) {
        console.error(`Error sending race commentary for bet ${betId}:`, e);
    }

    // Determine win/loss and payout
    // Player only wins if house edge didn't trigger AND their chosen horse matches the winning horse
    const win = (randomNumber < targetTotalProb) && (chosenHorseName.toLowerCase() === winningHorse.name.toLowerCase());
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
            await safeSendMessage(chat_id,
                `🎉 ${displayName}, your horse *${chosenHorseName}* won the race!\n` +
                `Your payout of ${payoutSOL.toFixed(6)} SOL is waiting. Place another bet (any amount) to link your wallet and receive pending payouts.`,
                { parse_mode: 'Markdown' }
            ).catch(e => console.error("TG Send Error:", e.message));
            await updateBetStatus(betId, 'completed_win_no_wallet');
            return;
        }

        // Send "processing payout" message and queue the actual payout
        try {
            await safeSendMessage(chat_id,
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
                amount: payoutLamports.toString(), // Pass amount as string
                gameType: 'race',
                priority: 2,
                chatId: chat_id,
                displayName,
                horseName: chosenHorseName,
                winningHorse // Pass full winning horse object if needed later
            });
        } catch (e) {
            console.error(`❌ Error queuing payout for race bet ${betId}:`, e);
            await safeSendMessage(chat_id,
                `⚠️ Error occurred while processing your race win for bet ID ${betId}.\n` +
                `Please contact support.`,
                { parse_mode: 'Markdown' }
            ).catch(e => console.error("TG Send Error:", e.message));
            await updateBetStatus(betId, 'error_payout_queueing');
        }

    } else { // Loss
        console.log(`🐎 Race Bet ${betId}: ${displayName} LOST. Horse: ${chosenHorseName}, Winner: ${winningHorse.name}`);
        await safeSendMessage(chat_id,
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
    const payoutAmountLamports = BigInt(amount); // Ensure amount is BigInt (convert back from string)

    // Ensure payout amount is positive before attempting to send
    if (payoutAmountLamports <= 0n) {
        console.error(`❌ Payout job for bet ${betId} has zero or negative amount (${payoutAmountLamports}). Skipping send.`);
        await updateBetStatus(betId, 'error_payout_zero_amount');
        await safeSendMessage(chatId,
            `⚠️ There was an issue calculating the payout for bet ID ${betId} (amount was zero). Please contact support.`,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
        return; // Don't proceed with sending 0
    }

    console.log(`💸 Processing payout job for Bet ID: ${betId}, Amount: ${payoutAmountLamports} lamports to ${recipient}`);

    try {
        // Attempt to send the SOL payout
        const sendResult = await sendSol(recipient, payoutAmountLamports, gameType);

        if (sendResult.success) {
            console.log(`💸 Payout successful for bet ${betId}, TX: ${sendResult.signature}`);
            // Send success message to user
            await safeSendMessage(chatId,
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
            await safeSendMessage(chatId,
                `⚠️ Payout failed for bet ID ${betId}: ${sendResult.error}\n` +
                `The team has been notified. Please contact support if needed.`,
                { parse_mode: 'Markdown' }
            ).catch(e => console.error("TG Send Error:", e.message));
            // Update status to indicate payout failure
            await updateBetStatus(betId, 'error_payout_failed');
            // TODO: Implement admin notification for failed payouts (Log is already present)
        }
    } catch (error) {
        // Catch unexpected errors during payout processing
        console.error(`❌ Unexpected error processing payout job for bet ${betId}:`, error);
        // Update status to reflect exception during payout attempt
        await updateBetStatus(betId, 'error_payout_exception');
        // Notify user of technical error
        await safeSendMessage(chatId,
            `⚠️ A technical error occurred during payout for bet ID ${betId}.\n` +
            `Please contact support.`,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
    }
}

// --- Utility Functions ---

// Calculates payout amount, applying house edge. Returns BigInt lamports.
function calculatePayout(betLamports, gameType, gameDetails = {}) {
    betLamports = BigInt(betLamports); // Ensure input is BigInt
    let payoutLamports = 0n;

    if (gameType === 'coinflip') {
        // Payout is Bet * (2 - House Edge)
        // Example: Bet * (2 - 0.02) = Bet * 1.98
        const multiplier = 2.0 - GAME_CONFIG.coinflip.houseEdge;
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
        // Prefer username if available and valid
        const username = chatMember.user.username;
        if (username && /^[a-zA-Z0-9_]{5,32}$/.test(username)) { // Basic validation
            return `@${username}`;
        }
        // Fallback to first name
        const firstName = chatMember.user.first_name;
        if (firstName) {
            // Simple sanitization for display
            return firstName.replace(/</g, '&lt;').replace(/>/g, '&gt;');
        }
        // Fallback if neither is available
        return `User ${String(user_id).substring(0, 6)}...`;
    } catch (e) {
        // Log error but return default name if fetching fails
        console.warn(`Couldn't get username/name for user ${user_id} in chat ${chat_id}:`, e.message);
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
        console.error("❌ Exiting due to conflict."); process.exit(1); // Exit immediately to prevent issues
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
    // Ignore messages without 'from' or from other bots
    if (!msg || !msg.from || msg.from.is_bot || !msg.chat) {
        return;
    }

    // Ignore messages without text content (e.g., stickers, photos) for command parsing
    if (!msg.text) {
        return;
    }

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
                // await safeSendMessage(chatId, "⏱️ Please wait a moment before sending another command.");
                return; // Ignore command if user is on cooldown
            }
        }
        // Apply cooldown only to commands, not general messages
        if (messageText.startsWith('/')) {
            confirmCooldown.set(userId, now); // Update last command time
        }


        // --- Command Routing ---
        const text = messageText.trim(); // Trim whitespace

        // Use regex for more robust command matching (allows @BotName suffix)
        const commandMatch = text.match(/^\/(\w+)(?:@\w+)?(?:\s+(.*))?$/); // Extracts command and arguments

        if (!commandMatch) return; // Not a command format

        const command = commandMatch[1].toLowerCase();
        const args = commandMatch[2] || ''; // Arguments string

        switch (command) {
            case 'start':
                await handleStartCommand(msg);
                break;
            case 'coinflip':
                await handleCoinflipCommand(msg);
                break;
            case 'wallet':
                await handleWalletCommand(msg);
                break;
            case 'bet':
                await handleBetCommand(msg, args); // Pass args separately
                break;
            case 'race':
                await handleRaceCommand(msg);
                break;
            case 'betrace':
                await handleBetRaceCommand(msg, args); // Pass args separately
                break;
            case 'help':
                await handleHelpCommand(msg);
                break;
            // Add other commands here
            default:
                // Optional: Handle unknown commands
                // await safeSendMessage(chatId, "❓ Unknown command. Use /help to see available commands.");
                break;
        }

        performanceMonitor.logRequest(true); // Log successful command handling

    } catch (error) {
        console.error(`❌ Error processing message from user ${userId} in chat ${chatId}: "${messageText}"`, error);
        performanceMonitor.logRequest(false); // Log error

        // Send generic error message to user
        try {
            await safeSendMessage(chatId, "⚠️ An unexpected error occurred while processing your request. Please try again later or contact support if the issue persists.");
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
    const sanitizedFirstName = firstName.replace(/</g, '&lt;').replace(/>/g, '&gt;'); // Basic sanitization
    const welcomeText = `👋 Welcome, ${sanitizedFirstName}!\n\n` +
                        `🎰 *Solana Gambles Bot*\n\n` +
                        `Use /coinflip or /race to see game options.\n` +
                        `Use /wallet to view your linked Solana wallet.\n` +
                        `Use /help to see all commands.`;
    const bannerUrl = 'https://i.ibb.co/9vDo58q/banner.gif'; // Keep banner URL

    try {
        // Try sending with animation/banner first
        await bot.sendAnimation(msg.chat.id, bannerUrl, {
            caption: welcomeText,
            parse_mode: 'Markdown'
        });
    } catch (err) {
        // Fallback if animation fails (e.g., invalid URL, Telegram issue)
        console.warn("⚠️ Failed to send start animation, sending text fallback:", err.message); // Use warn level
        await safeSendMessage(msg.chat.id, welcomeText, { parse_mode: 'Markdown' })
            .catch(e => console.error("TG Send Error (start fallback):", e.message)); // Catch fallback error too
    }
}

// Handles the /coinflip command (shows instructions)
async function handleCoinflipCommand(msg) {
    const config = GAME_CONFIG.coinflip;
    await safeSendMessage(msg.chat.id,
        `🪙 *Coinflip Game* 🪙\n\n` +
        `Bet on Heads or Tails!\n\n` +
        `*How to play:*\n` +
        `1. Type \`/bet amount heads\` (e.g., \`/bet 0.1 heads\`)\n` +
        `2. Type \`/bet amount tails\` (e.g., \`/bet 0.1 tails\`)\n\n` +
        `*Rules:*\n` +
        `- Min Bet: ${config.minBet} SOL\n` +
        `- Max Bet: ${config.maxBet} SOL\n` +
        `- House Edge: ${(config.houseEdge * 100).toFixed(1)}%\n` +
        `- Payout: ~${(2.0 * (1.0 - config.houseEdge)).toFixed(2)}x (Win Amount = Bet * ${ (2.0 * (1.0 - config.houseEdge)).toFixed(2)}x )\n\n` + // Clarified payout calculation display
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

    let raceMessage = `🐎 *Horse Race Game* 🐎\n\nBet on the winning horse!\n\n*Available Horses & Approx Payout (Bet x Odds):*\n`;
    horses.forEach(horse => {
        // Calculate payout after house edge for display
        const effectiveMultiplier = (horse.odds * (1.0 - GAME_CONFIG.race.houseEdge)).toFixed(2)
        raceMessage += `- ${horse.emoji} *${horse.name}* (~${effectiveMultiplier}x Payout)\n`;
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

    await safeSendMessage(msg.chat.id, raceMessage, { parse_mode: 'Markdown' }).catch(e => console.error("TG Send Error:", e.message));
}

// Handles the /wallet command (shows linked wallet)
async function handleWalletCommand(msg) {
    const userId = String(msg.from.id);
    const walletAddress = await getLinkedWallet(userId); // Uses cache

    if (walletAddress) {
        await safeSendMessage(msg.chat.id,
            `🔗 Your linked Solana wallet:\n\`${walletAddress}\`\n\n`+
            `Payouts will be sent here. It's linked automatically when you make your first paid bet.`,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
    } else {
        await safeSendMessage(msg.chat.id,
            `🔗 No wallet linked yet.\n` +
            `Place a bet and send the required SOL. Your sending wallet will be automatically linked for future payouts.`
        ).catch(e => console.error("TG Send Error:", e.message));
    }
}

// Handles /bet command (for Coinflip) - Updated to use args
async function handleBetCommand(msg, args) {
    const match = args.trim().match(/^(\d+\.?\d*)\s+(heads|tails)/i); // Match against args string
    if (!match) {
        await safeSendMessage(msg.chat.id,
            `⚠️ Invalid format. Use: \`/bet <amount> <heads|tails>\`\n` +
            `Example: \`/bet 0.1 heads\``,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
        return;
    }

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const config = GAME_CONFIG.coinflip;

    // Validate bet amount
    const betAmount = parseFloat(match[1]);
    if (isNaN(betAmount) || betAmount < config.minBet || betAmount > config.maxBet) {
        await safeSendMessage(chatId,
            `⚠️ Invalid bet amount. Please bet between ${config.minBet} and ${config.maxBet} SOL.\n` +
            `Example: \`/bet 0.1 heads\``,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
        return;
    }

    const userChoice = match[2].toLowerCase();
    const memoId = generateMemoId('CF'); // Uses new enhanced generator
    const expectedLamports = BigInt(Math.round(betAmount * LAMPORTS_PER_SOL));
    const expiresAt = new Date(Date.now() + config.expiryMinutes * 60 * 1000);

    // Save the pending bet
    const saveResult = await savePendingBet(
        userId, chatId, 'coinflip',
        { choice: userChoice },
        expectedLamports, memoId, expiresAt
    );

    if (!saveResult.success) {
        await safeSendMessage(chatId, `⚠️ Error registering bet: ${saveResult.error}. Please try the command again.`).catch(e => console.error("TG Send Error:", e.message));
        // Don't throw, just return after notifying user
        return;
    }

    // Send payment instructions
    await safeSendMessage(chatId,
        `✅ Coinflip bet registered! (ID: ${memoId})\n\n` +
        `You chose: *${userChoice}*\n` +
        `Amount: *${betAmount.toFixed(6)} SOL*\n\n` +
        `➡️ Send *exactly ${betAmount.toFixed(6)} SOL* to:\n` +
        `\`${process.env.MAIN_WALLET_ADDRESS}\`\n\n` +
        `📎 *Include MEMO:* \`${memoId}\`\n\n` + // Display the full new memo
        `⏱️ This request expires in ${config.expiryMinutes} minutes.\n\n` +
        `*IMPORTANT:* Send from your own wallet. Do not send from an exchange. Ensure you include the memo correctly.`,
        { parse_mode: 'Markdown', disable_web_page_preview: true }
    ).catch(e => console.error("TG Send Error:", e.message));
}


// Handles /betrace command - Updated to use args
async function handleBetRaceCommand(msg, args) {
    const match = args.trim().match(/^(\d+\.?\d*)\s+(\w+)/i); // Match against args string
    if (!match) {
        await safeSendMessage(msg.chat.id,
            `⚠️ Invalid format. Use: \`/betrace <amount> <horse_name>\`\n`+
            `Example: \`/betrace 0.1 Yellow\``,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
        return;
    }

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const config = GAME_CONFIG.race;

    // Validate bet amount
    const betAmount = parseFloat(match[1]);
    if (isNaN(betAmount) || betAmount < config.minBet || betAmount > config.maxBet) {
        await safeSendMessage(chatId,
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
        await safeSendMessage(chatId,
            `⚠️ Invalid horse name: "${chosenHorseNameInput}". Please choose from the list in /race.`
        ).catch(e => console.error("TG Send Error:", e.message));
        return;
    }

    const memoId = generateMemoId('RA'); // Uses new enhanced generator
    const expectedLamports = BigInt(Math.round(betAmount * LAMPORTS_PER_SOL));
    const expiresAt = new Date(Date.now() + config.expiryMinutes * 60 * 1000);

    // Save the pending bet
    const saveResult = await savePendingBet(
        userId, chatId, 'race',
        { horse: chosenHorse.name, odds: chosenHorse.odds }, // Store chosen horse name and odds
        expectedLamports, memoId, expiresAt
    );

    if (!saveResult.success) {
        await safeSendMessage(chatId, `⚠️ Error registering bet: ${saveResult.error}. Please try the command again.`).catch(e => console.error("TG Send Error:", e.message));
        // Don't throw, just return
        return;
    }

    // Calculate potential payout for display
    const potentialPayoutLamports = calculatePayout(expectedLamports, 'race', chosenHorse);
    const potentialPayoutSOL = (Number(potentialPayoutLamports) / LAMPORTS_PER_SOL).toFixed(6);

    // Send payment instructions
    await safeSendMessage(chatId,
        `✅ Race bet registered! (ID: ${memoId})\n\n` +
        `You chose: ${chosenHorse.emoji} *${chosenHorse.name}*\n` +
        `Amount: *${betAmount.toFixed(6)} SOL*\n` +
        `Potential Payout: ~${potentialPayoutSOL} SOL\n\n`+ // Show potential payout
        `➡️ Send *exactly ${betAmount.toFixed(6)} SOL* to:\n` +
        `\`${process.env.RACE_WALLET_ADDRESS}\`\n\n` +
        `📎 *Include MEMO:* \`${memoId}\`\n\n` + // Display the full new memo
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

    await safeSendMessage(msg.chat.id, helpText, { parse_mode: 'Markdown' }).catch(e => console.error("TG Send Error:", e.message));
}


// --- Server Startup & Shutdown Logic ---

// --- MODIFIED: startServer function for Railway (Includes Startup Timeout) ---
async function startServer() {
    let server; // Define server variable here to be accessible in error handler

    // --- NEW: Add Startup Timeout Safety (Railway Recommendation) ---
    const startupTimeout = setTimeout(() => {
        if (!isFullyInitialized) {
            console.error("🛑 Startup timed out after 2 minutes. Application failed to initialize.");
            process.exit(1); // Exit if initialization takes too long
        }
    }, 120000); // 2 minutes (120,000 ms)
    // --- END NEW ---

    try {
        // Start the server IMMEDIATELY
        const PORT = process.env.PORT || 3000;
        server = app.listen(PORT, "0.0.0.0", () => {
            console.log(`🚀 Server running on port ${PORT}. Waiting for initialization...`);
            // Note: Initialization happens in the setTimeout below now
        });

        // Handle server startup errors (like EADDRINUSE)
        server.on('error', (err) => {
            console.error('❌ Server startup error:', err);
            if (err.code === 'EADDRINUSE') {
                console.error(`❌❌❌ Port ${PORT} is already in use. Is another instance running? Exiting.`);
                process.exit(1); // Exit immediately if port is taken
            }
            // Handle other potential server errors if needed
            process.exit(1); // Exit on other server errors too
        });

        // Then initialize other components ASYNCHRONOUSLY
        // Using setTimeout 0 to run this block after the current event loop cycle
        setTimeout(async () => {
            try {
                console.log("⚙️ Starting asynchronous initialization...");
                await initializeDatabase(); // Ensure DB is ready first

                // Set up Telegram Webhook if in a production environment (e.g., Railway)
                if (process.env.RAILWAY_ENVIRONMENT && process.env.RAILWAY_PUBLIC_DOMAIN) {
                    const webhookUrl = `https://${process.env.RAILWAY_PUBLIC_DOMAIN}${webhookPath}`;
                    console.log(`Attempting to set webhook to: ${webhookUrl}`);

                    let attempts = 0;
                    while (attempts < 3) { // Retry webhook setup (kept from original)
                        try {
                            // Ensure any existing webhook is deleted before setting a new one
                            await bot.deleteWebHook({ drop_pending_updates: true });
                            await bot.setWebHook(webhookUrl, {
                                max_connections: 20 // Allow more concurrent webhook connections if needed
                            });
                            console.log(`✅ Webhook successfully set to: ${webhookUrl}`);
                            break; // Exit loop on success
                        } catch (webhookError) {
                            attempts++;
                            console.error(`❌ Webhook setup attempt ${attempts} failed:`, webhookError.message);
                            if (attempts >= 3) {
                                console.error("❌ Max webhook setup attempts reached. Continuing without webhook (potential issues)...");
                                // Decide if you want to throw or just log here
                                // throw webhookError; // Option: Throw error to stop initialization
                                break; // Option: Continue without webhook if polling is fallback
                            }
                            // Wait before retrying
                            await new Promise(resolve => setTimeout(resolve, 3000 * attempts));
                        }
                    }
                } else {
                    console.log("ℹ️ Not in Railway environment or domain not set, webhook not configured. Will attempt polling.");
                }

                // Start the payment monitor loop AFTER async initialization is done
                console.log(`⚙️ Starting payment monitor (Initial Interval: ${monitorIntervalSeconds}s)`);
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


                // Run monitor once shortly after initialization completes
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

                // Start polling if webhook is NOT used (e.g., local development or webhook setup failed)
                // Check if bot is already polling or has webhook set
                bot.getWebHookInfo().then(info => {
                    if (!info || !info.url) {
                        console.log("ℹ️ Webhook not set, starting bot polling...");
                        // Delete any existing webhook first just in case
                        bot.deleteWebHook({ drop_pending_updates: true })
                            .then(() => bot.startPolling({ polling: { interval: 300 } })) // Adjust interval if needed
                            .then(() => console.log("✅ Bot polling started successfully"))
                            .catch(err => {
                                console.error("❌ Error starting polling:", err.message);
                                // Handle specific polling errors if needed
                                if (err.message.includes('409 Conflict')) {
                                    console.error("❌❌❌ Conflict detected! Another instance might be polling.");
                                    console.error("❌ Exiting due to conflict."); process.exit(1);
                                }
                            });
                    } else {
                        console.log("ℹ️ Webhook is set, polling will not be started.");
                    }
                }).catch(err => {
                    console.error("❌ Error checking webhook info:", err.message);
                    // Potentially try starting polling as a fallback?
                });

                // --- NEW: Mark initialization as complete AND clear timeout ---
                isFullyInitialized = true;
                clearTimeout(startupTimeout); // Clear the timeout timer as initialization succeeded
                console.log("✅ Asynchronous Initialization Complete. Bot is fully ready.");
                console.log("🚀🚀🚀 Solana Gambles Bot is up and running! 🚀🚀🚀");


            } catch (initError) {
                console.error("🔥🔥🔥 Asynchronous Initialization Error:", initError);
                // Even if initialization fails, the server is running.
                // Keep isFullyInitialized = false. The /health check will report initializing.
                // The startupTimeout timer *will* eventually fire and exit the process.
                // Consider if a failed initialization requires stopping the server earlier.
                // For now, just logging the error. Might need manual intervention.
                // Optionally, try to shut down gracefully here?
                // await shutdown('INITIALIZATION_FAILURE'); // Requires shutdown to be defined
            }
        }, 0); // Use timeout 0 to schedule immediately after current stack clears

    } catch (error) {
        // Catch errors during the *immediate* synchronous part of startup (e.g., Express setup itself)
        console.error("🔥🔥🔥 Failed to start server (immediate error):", error);
        clearTimeout(startupTimeout); // Clear timeout if immediate startup fails
        process.exit(1); // Exit if critical startup steps fail
    }
}
// --- END MODIFIED startServer ---

// Graceful shutdown handler
const shutdown = async (signal) => {
    console.log(`\n🛑 ${signal} received, shutting down gracefully...`);
    isMonitorRunning = true; // Prevent monitor from starting new cycle during shutdown

    // 1. Stop receiving new events/requests
    console.log("Stopping incoming connections and tasks...");
    if (monitorInterval) {
        clearInterval(monitorInterval);
        console.log("- Stopped payment monitor interval.");
    }
    try {
        let webhookDeleted = false;
        // Try deleting webhook regardless of env var, in case it was set manually
        try {
            const webhookInfo = await bot.getWebHookInfo();
            if (webhookInfo && webhookInfo.url) {
                await bot.deleteWebHook({ drop_pending_updates: true });
                console.log("- Removed Telegram webhook.");
                webhookDeleted = true;
            }
        } catch (whErr) {
            console.error("⚠️ Error removing webhook during shutdown:", whErr.message);
        }

        // Stop polling if it was active
        if (bot.isPolling()) {
            await bot.stopPolling({ cancel: true }); // Cancel polling
            console.log("- Stopped Telegram polling.");
        }
        // Close Express server? (Handled by Railway/Docker usually, but good practice)
        // server.close(() => console.log("- Express server closed.")); // Need 'server' in wider scope if used here
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
            new Promise((_, reject) => setTimeout(() => reject(new Error('Queue drain timeout (10s)')), 10000)) // 10s timeout
        ]);
        console.log("- All processing queues are idle.");
    } catch (queueError) {
        console.warn("⚠️ Timed out waiting for queues or queue error during shutdown:", queueError.message);
        // Optionally clear queues if timeout occurs
        // messageQueue.clear();
        // paymentProcessor.highPriorityQueue.clear();
        // paymentProcessor.normalQueue.clear();
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
    // Attempt graceful shutdown, but exit quickly if it fails or takes too long
    shutdown('UNCAUGHT_EXCEPTION').catch(() => process.exit(1));
    setTimeout(() => {
        console.error("Shutdown timed out after uncaught exception. Forcing exit.");
        process.exit(1);
    }, 12000).unref(); // Force exit after 12s if shutdown hangs
});

// Handle unhandled promise rejections (log them - consider if shutdown needed)
process.on('unhandledRejection', (reason, promise) => {
    console.error('🔥🔥🔥 Unhandled Rejection at:', promise, 'reason:', reason);
    // Decide if this warrants a shutdown. Often indicates a programming error.
    // For now, just logging. Could trigger shutdown similar to uncaughtException:
    // shutdown('UNHANDLED_REJECTION').catch(() => process.exit(1));
    // setTimeout(() => process.exit(1), 12000).unref();
});

// --- Start the Application ---
// No need for .then() here anymore as the final success log is inside the async init now
startServer();

// Note: The final "Bot is up and running" log message moved inside the async initialization block
// in startServer to accurately reflect when the bot is *fully* ready.
