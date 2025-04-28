// index.js

// --- All Imports MUST come first ---
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
import * as crypto from 'crypto';
import PQueue from 'p-queue';
import RateLimitedConnection from './lib/solana-connection.js';
import { toByteArray, fromByteArray } from 'base64-js';
import { Buffer } from 'buffer';
// --- End of Imports ---

// --- CORRECTED PLACEMENT for Deployment Check Log ---
console.log(`--- INDEX.JS - DEPLOYMENT CHECK --- ${new Date().toISOString()} ---`);
// --- END CORRECTED PLACEMENT ---


console.log("⏳ Starting Solana Gambles Bot (Multi-RPC)... Checking environment variables...");

// ... (rest of your index.js ---

// --- START: Enhanced Environment Variable Checks (MODIFIED for RPC_URLS) ---
const REQUIRED_ENV_VARS = [
    'BOT_TOKEN',
    'DATABASE_URL',
    'BOT_PRIVATE_KEY', // Coinflip payouts
    'RACE_BOT_PRIVATE_KEY', // Race payouts
    'MAIN_WALLET_ADDRESS', // Coinflip deposits
    'RACE_WALLET_ADDRESS', // Race deposits
    'RPC_URLS',         // <-- CHANGED from RPC_URL
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

// Specific check for RPC_URLS content
if (process.env.RPC_URLS && process.env.RPC_URLS.split(',').map(u => u.trim()).filter(u => u).length === 0) {
     console.error(`❌ Environment variable RPC_URLS is set but contains no valid URLs after parsing.`);
     missingVars = true;
} else if (process.env.RPC_URLS && !process.env.RPC_URLS.includes(',') && process.env.RPC_URLS.split(',').map(u => u.trim()).filter(u => u).length === 1) {
     // Optional: Warn if only one RPC URL is provided, although the new class handles it.
     console.warn(`⚠️ Environment variable RPC_URLS only contains one URL. Multi-RPC features may not be fully utilized.`);
}


if (missingVars) {
    console.error("⚠️ Please set all required environment variables. Exiting.");
    console.error("❌ Exiting due to missing environment variables."); process.exit(1);
}

// Set default fee margin if not specified (e.g., 0.00005 SOL)
if (!process.env.FEE_MARGIN) {
    process.env.FEE_MARGIN = '5000'; // Default to 5000 lamports
}
console.log(`ℹ️ Using FEE_MARGIN: ${process.env.FEE_MARGIN} lamports`);
// --- END: Enhanced Environment Variable Checks ---


// --- Startup State Tracking ---
let isFullyInitialized = false; // Tracks completion of background initialization
let server; // Declare server variable for health check and graceful shutdown

// --- Initialize Scalable Components ---
const app = express();

// --- IMMEDIATE Health Check Endpoint (Critical Fix #1) ---
// This endpoint responds instantly, regardless of background initialization state.
app.get('/health', (req, res) => {
  res.status(200).json({
    status: server ? 'ready' : 'starting', // Report 'ready' once server object exists, otherwise 'starting'
    uptime: process.uptime()
  });
});

// --- Railway-Specific Health Check Endpoint (NEW) ---
// This endpoint can be used by Railway to check readiness after container rotation.
app.get('/railway-health', (req, res) => {
    res.status(200).json({
        status: isFullyInitialized ? 'ready' : 'starting',
        version: '2.1-multi-rpc' // Update version string
    });
});

// --- PreStop hook for Railway graceful shutdown ---
app.get('/prestop', (req, res) => {
    console.log('🚪 Received pre-stop signal from Railway, preparing to shutdown gracefully...');
    res.status(200).send('Shutting down');
});

// --- END IMMEDIATE Health Check Endpoint ---


// --- Initialize Multi-RPC Solana connection ---
console.log("⚙️ Initializing Multi-RPC Solana connection...");

// Parse RPC URLs
const rpcUrls = process.env.RPC_URLS.split(',')
    .map(url => url.trim())
    .filter(url => url.length > 0);

if (rpcUrls.length === 0) {
    console.error("❌ No valid RPC URLs found in RPC_URLS environment variable after filtering. Exiting.");
    process.exit(1);
}

console.log(`ℹ️ Using RPC Endpoints: ${rpcUrls.join(', ')}`);

// --- ONLY ONE solanaConnection created here ---
const solanaConnection = new RateLimitedConnection(rpcUrls, {
    maxConcurrent: parseInt(process.env.RPC_MAX_CONCURRENT || '5', 10),
    retryBaseDelay: parseInt(process.env.RPC_RETRY_BASE_DELAY || '700', 10),
    maxRetries: parseInt(process.env.RPC_MAX_RETRIES || '5', 10),
    rateLimitCooloff: parseInt(process.env.RPC_RATE_LIMIT_COOLOFF || '5000', 10),
    retryMaxDelay: parseInt(process.env.RPC_RETRY_MAX_DELAY || '30000', 10),
    retryJitter: parseFloat(process.env.RPC_RETRY_JITTER || '0.2'),
    commitment: process.env.RPC_COMMITMENT || 'confirmed',
    httpHeaders: {
        'User-Agent': `SolanaGamblesBot/2.1-multi-rpc`
    },
    clientId: `SolanaGamblesBot/2.1-multi-rpc`
});
console.log("✅ Multi-RPC Solana connection initialized");

// --- LIVE QUEUE MONITOR ---
setInterval(() => {
    if (!solanaConnection) return;

    const { totalRequestsEnqueued, totalRequestsSucceeded, totalRequestsFailed, endpointRotations } = solanaConnection.stats;
    const queueSize = solanaConnection.requestQueue.length;
    const activeRequests = solanaConnection.activeRequests;
    const successRate = totalRequestsEnqueued > 0 ? (100 * totalRequestsSucceeded / totalRequestsEnqueued).toFixed(1) : "N/A";

    console.log(`[Queue Monitor] Active: ${activeRequests}, Pending: ${queueSize}, Success Rate: ${successRate}%, Rotations: ${endpointRotations}`);
}, 60000); // Every 60 seconds

// --- EMERGENCY AUTO-ROTATE ---
let lastQueueProcessTime = Date.now();

const originalProcessQueue = solanaConnection._processQueue.bind(solanaConnection);

solanaConnection._processQueue = async function patchedProcessQueue() {
    lastQueueProcessTime = Date.now();
    await originalProcessQueue();
};

setInterval(() => {
    const now = Date.now();
    const stuckDuration = now - lastQueueProcessTime;

    if (stuckDuration > 30000) { // If no progress for 30 seconds
        console.warn(`[Emergency Rotate] No queue movement detected for ${stuckDuration}ms. Forcing endpoint rotation...`);
        solanaConnection._rotateEndpoint();
        lastQueueProcessTime = Date.now();
    }
}, 10000); // Every 10 seconds
// --- (then continue initializing queues and telegram bot as normal)


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
    max: parseInt(process.env.DB_POOL_MAX || '15', 10),             // Max connections in pool
    min: parseInt(process.env.DB_POOL_MIN || '5', 10),              // Min connections maintained
    idleTimeoutMillis: parseInt(process.env.DB_IDLE_TIMEOUT || '30000', 10), // Close idle connections after 30s
    connectionTimeoutMillis: parseInt(process.env.DB_CONN_TIMEOUT || '5000', 10), // Timeout for acquiring connection
    ssl: process.env.NODE_ENV === 'production' || process.env.DB_SSL === 'true' ? { // More flexible SSL check
        rejectUnauthorized: process.env.DB_REJECT_UNAUTHORIZED !== 'false' // Default to true unless explicitly false
    } : false
});

// Add listener for pool errors
pool.on('error', (err, client) => {
    console.error('❌ Unexpected error on idle PostgreSQL client', err);
    // Optionally try to remove the client or log more details
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
             // Include Solana connection stats if available
            const connectionStats = typeof solanaConnection.getRequestStats === 'function'
                ? solanaConnection.getRequestStats()
                : null;
            // Format connection stats more concisely
            const statsString = connectionStats
                ? `| SOL Conn: Q:${connectionStats.status.queueSize} A:${connectionStats.status.activeRequests} E:${connectionStats.status.currentEndpointIndex} RL:${connectionStats.status.consecutiveRateLimits} Rot:${connectionStats.stats.endpointRotations} Fail:${connectionStats.stats.totalRequestsFailed}`
                : '';

            console.log(`
📊 Perf Mon: Uptime:${uptime.toFixed(0)}s | Req:${this.requests} | Err:${errorRate}% ${statsString}
            `);
        }
    }
};

// --- Database Initialization ---
// Incorporates syntax fixes and new indexes from the "ultimate fix"
async function initializeDatabase() {
    console.log("⚙️ Initializing Database schema...");
    let client;
    try {
        client = await pool.connect();

        // Bets Table: Tracks individual game bets
        await client.query(`CREATE TABLE IF NOT EXISTS bets (
            id SERIAL PRIMARY KEY,
            user_id TEXT NOT NULL,
            chat_id TEXT NOT NULL,
            game_type TEXT NOT NULL,
            bet_details JSONB,
            expected_lamports BIGINT NOT NULL,
            memo_id TEXT UNIQUE NOT NULL,
            status TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            expires_at TIMESTAMPTZ NOT NULL,
            paid_tx_signature TEXT UNIQUE,
            payout_tx_signature TEXT UNIQUE,
            processed_at TIMESTAMPTZ,
            fees_paid BIGINT,
            priority INT DEFAULT 0
        );`);

        // Wallets Table: Links Telegram User ID to their Solana wallet address
        await client.query(`CREATE TABLE IF NOT EXISTS wallets (
            user_id TEXT PRIMARY KEY,
            wallet_address TEXT NOT NULL,
            linked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_used_at TIMESTAMPTZ
        );`);

        // Add columns using ALTER TABLE IF NOT EXISTS for backward compatibility
        // Use transaction for potentially multiple ALTER statements
        await client.query('BEGIN');
        await client.query(`ALTER TABLE bets ADD COLUMN IF NOT EXISTS priority INT DEFAULT 0;`);
        await client.query(`ALTER TABLE bets ADD COLUMN IF NOT EXISTS fees_paid BIGINT;`);
        await client.query(`ALTER TABLE bets ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ;`);
        await client.query(`ALTER TABLE wallets ADD COLUMN IF NOT EXISTS last_used_at TIMESTAMPTZ;`);
        await client.query('COMMIT');

        console.log("  - Columns verified/added.");

        // Add indexes for performance on frequently queried columns
        // Use CONCURRENTLY in production if possible, but IF NOT EXISTS is safer for init script
        console.log("  - Creating indexes (if they don't exist)...");
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_status ON bets(status);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_user_id ON bets(user_id);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_expires_at ON bets(expires_at);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_priority ON bets(priority);`);
        // Bets memo index (ensure unique constraint exists)
        await client.query(`DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'bets_memo_id_key') THEN
                ALTER TABLE bets ADD CONSTRAINT bets_memo_id_key UNIQUE (memo_id);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_bets_memo_id ON bets (memo_id) INCLUDE (status, expected_lamports, expires_at);
            END IF;
        END $$;`);

        // Indexes from "Ultimate Fix"
        await client.query(`CREATE INDEX IF NOT EXISTS bets_memo_status_idx ON bets (memo_id, status);`);
        await client.query(`CREATE INDEX IF NOT EXISTS bets_created_at_idx ON bets (created_at DESC);`);
        // Index for faster duplicate signature checks in payment processor
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_paid_tx_signature ON bets(paid_tx_signature) WHERE paid_tx_signature IS NOT NULL;`);
        // Index for faster wallet lookups
        await client.query(`CREATE INDEX IF NOT EXISTS idx_wallets_address ON wallets(wallet_address);`);


        console.log("✅ Database schema initialized/verified.");
    } catch (err) {
        // Log the detailed error if initialization fails
        console.error("❌ Database initialization error:", err);
        if (err.message.includes('already exists') && err.code === '42P07') {
             console.warn("  - Note: Some elements (table/index) already existed, which is usually okay.");
        } else if (err.message.includes('already exists') && err.code === '23505') {
             console.warn("  - Note: Unique constraint likely already existed, which is okay.");
        }
        else {
             await client?.query('ROLLBACK').catch(rbErr => console.error("Rollback failed:", rbErr));
             throw err; // Re-throw other errors to prevent startup
        }
    } finally {
        if (client) client.release(); // Release client back to the pool
    }
}

// --- End of Part 1 ---
// index.js (Updated for Multi-RPC RateLimitedConnection) - Part 2

// --- Telegram Bot Initialization with Queue ---
console.log("⚙️ Initializing Telegram Bot...");
const bot = new TelegramBot(process.env.BOT_TOKEN, {
    polling: false, // Use webhooks in production (set later in startup)
    request: {      // Adjust request options for stability (Fix #4)
        timeout: 10000, // Request timeout: 10s
        agentOptions: {
            keepAlive: true,   // Reuse connections
            timeout: 60000     // Keep-alive timeout: 60s
        }
    }
});

// Wrap all incoming message handlers in the processing queue
bot.on('message', (msg) => {
    // Basic filtering at the entry point
    if (!msg || !msg.from || msg.from.is_bot || !msg.chat || !msg.text) {
        // console.log("Ignoring non-text message or message from bot."); // Optional debug log
        return;
    }
    // Add to queue only if it passes basic checks
    messageQueue.add(() => handleMessage(msg))
        .catch(err => {
            console.error(`⚠️ Message processing queue error for msg ID ${msg?.message_id}:`, err);
            performanceMonitor.logRequest(false); // Log error
        });
});


// [PATCHED: THROTTLED TELEGRAM SEND]
const telegramSendQueue = new PQueue({ concurrency: 1, interval: 1000, intervalCap: 1 });

/**
 * Sends a message via Telegram safely using a rate-limited queue.
 * @param {string|number} chatId - The chat ID to send the message to.
 * @param {string} message - The message text.
 * @param {object} [options={}] - Telegram Bot API message options (e.g., parse_mode).
 * @returns {Promise<TelegramBot.Message|undefined>} A promise resolving with the sent message or undefined on error.
 */
function safeSendMessage(chatId, message, options = {}) {
    // Basic validation
    if (!chatId || !message) {
        console.error("safeSendMessage: Missing chatId or message.");
        return Promise.resolve(undefined); // Return a resolved promise for consistency
    }
    return telegramSendQueue.add(() =>
        bot.sendMessage(chatId, message, options).catch(err => {
            console.error(`❌ Telegram send error to chat ${chatId}:`, err.message);
            // Handle specific errors like "Forbidden: bot was blocked by the user"
            if (err.code === 'ETELEGRAM' && err.message.includes('403 Forbidden')) {
                console.warn(`Bot may be blocked by user in chat ${chatId}.`);
                // Optionally, update user status in DB if needed
            }
            // Return undefined or null to indicate failure without throwing
             return undefined;
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

// Original Health check / Info endpoint (keep for manual checks / detailed status)
app.get('/', (req, res) => {
    performanceMonitor.logRequest(true);
    const processor = paymentProcessor; // Assign to local var for safety
     // Include Solana connection stats if available
    const connectionStats = typeof solanaConnection.getRequestStats === 'function'
        ? solanaConnection.getRequestStats()
        : null;

    res.status(200).json({
        status: 'ok',
        initialized: isFullyInitialized, // Report background initialization status here
        timestamp: new Date().toISOString(),
        version: '2.1-multi-rpc', // Updated version
        queueStats: { // Report queue status
            messageQueuePending: messageQueue.size,
            messageQueueActive: messageQueue.pending,
            telegramSendPending: telegramSendQueue.size,
            telegramSendActive: telegramSendQueue.pending,
            paymentHighPriPending: (processor?.highPriorityQueue?.size || 0),
            paymentHighPriActive: (processor?.highPriorityQueue?.pending || 0),
            paymentNormalPending: (processor?.normalQueue?.size || 0),
            paymentNormalActive: (processor?.normalQueue?.pending || 0)
        },
        solanaConnection: connectionStats // Add connection stats to the main health check
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
                // It's important to still send a 2xx response to Telegram even for bad requests
                // otherwise it might keep retrying. 400 is acceptable.
                return res.status(400).send('Invalid request body');
            }

            // Process the update using the bot instance
            bot.processUpdate(req.body);
            performanceMonitor.logRequest(true);
            res.sendStatus(200); // Acknowledge receipt to Telegram
        } catch (error) {
            console.error("❌ Webhook processing error:", error);
            performanceMonitor.logRequest(false);
            // Send 200 to Telegram to prevent retries, but log the internal error
            res.status(200).json({ warning: 'Internal server error processing webhook, but acknowledged receipt.' });
        }
    }); // <<< Closing parenthesis for messageQueue.add
}); // <<< Closing parenthesis for app.post


// --- State Management & Constants ---

// User command cooldown (prevents spam)
const confirmCooldown = new Map(); // Map<userId, lastCommandTimestamp>
const cooldownInterval = 3000; // 3 seconds

// Cache for linked wallets (reduces DB lookups)
const walletCache = new Map(); // Map<userId, { wallet: address, timestamp: cacheTimestamp }>
const CACHE_TTL = parseInt(process.env.WALLET_CACHE_TTL_MS || '300000', 10); // Cache wallet links for 5 minutes (300,000 ms)

// Cache of processed transaction signatures during this bot session (prevents double processing)
const processedSignaturesThisSession = new Set(); // Set<signature>
const MAX_PROCESSED_SIGNATURES = parseInt(process.env.MAX_PROCESSED_SIG_CACHE || '10000', 10); // Reset cache if it gets too large

// Game Configuration
const GAME_CONFIG = {
    coinflip: {
        minBet: parseFloat(process.env.CF_MIN_BET || '0.01'),
        maxBet: parseFloat(process.env.CF_MAX_BET || '1.0'),
        expiryMinutes: parseInt(process.env.CF_EXPIRY_MINUTES || '15', 10),
        houseEdge: parseFloat(process.env.CF_HOUSE_EDGE || '0.02') // 2% house edge
    },
    race: {
        minBet: parseFloat(process.env.RACE_MIN_BET || '0.01'),
        maxBet: parseFloat(process.env.RACE_MAX_BET || '1.0'),
        expiryMinutes: parseInt(process.env.RACE_EXPIRY_MINUTES || '15', 10),
        houseEdge: parseFloat(process.env.RACE_HOUSE_EDGE || '0.02') // 2% house edge (applied during payout calculation)
    }
};
console.log("ℹ️ Game Config Loaded:", GAME_CONFIG);

// Fee buffer (lamports) - should cover base fee + priority fee estimate
const FEE_BUFFER = BigInt(process.env.FEE_MARGIN);
// Priority fee rate (used to calculate microLamports for ComputeBudgetProgram)
const PRIORITY_FEE_RATE = parseFloat(process.env.PAYOUT_PRIORITY_FEE_RATE || '0.0001'); // Example: 0.01% of payout amount

// --- Helper Functions ---

// <<< DEBUG HELPER FUNCTION - Remains but unused by current findMemoInTx >>>
function debugInstruction(inst, accountKeys) {
    try {
        const programIdKeyInfo = accountKeys[inst.programIdIndex];
        // Handle different potential structures for accountKeys
        const programId = programIdKeyInfo?.pubkey ? new PublicKey(programIdKeyInfo.pubkey) : // VersionedMessage
                            (typeof programIdKeyInfo === 'string' ? new PublicKey(programIdKeyInfo) : // Legacy string
                            (programIdKeyInfo instanceof PublicKey ? programIdKeyInfo : null)); // Direct PublicKey

        const accountPubkeys = inst.accounts?.map(idx => {
            const keyInfo = accountKeys[idx];
            return keyInfo?.pubkey ? new PublicKey(keyInfo.pubkey) :
                   (typeof keyInfo === 'string' ? new PublicKey(keyInfo) :
                   (keyInfo instanceof PublicKey ? keyInfo : null));
        }).filter(pk => pk !== null) // Filter out nulls if parsing failed
          .map(pk => pk.toBase58()); // Convert valid PublicKeys to string

        return {
            programId: programId ? programId.toBase58() : `Invalid Index ${inst.programIdIndex}`,
            data: inst.data ? Buffer.from(inst.data, 'base64').toString('hex') : null,
            accounts: accountPubkeys // Array of Base58 strings or empty array
        };
    } catch (e) {
        console.error("[DEBUG INSTR HELPER] Error:", e); // Log the specific error
        return { error: e.message, programIdIndex: inst.programIdIndex, accountIndices: inst.accounts }; // Return error info
    }
}
// <<< END DEBUG HELPER FUNCTION >>>

// For instruction data (Base64 or Buffer)
const decodeInstructionData = (data) => {
  if (!data) return null;
  try {
    // Handle base64 string input
    if (typeof data === 'string') {
       // Check for common base64 encoding types
       if (/^[A-Za-z0-9+/]*={0,2}$/.test(data)) { // Standard base64 check
           return Buffer.from(data, 'base64').toString('utf8');
       } else if (bs58.decode(data)) { // Check if it's base58 (less likely for memo data, but possible)
           return Buffer.from(bs58.decode(data)).toString('utf8');
       } else {
           // Assume plain string if not obviously encoded, but might be incorrect
           return data;
       }
    }
    // Handle Buffer or Uint8Array input
    else if (Buffer.isBuffer(data) || data instanceof Uint8Array) {
      return Buffer.from(data).toString('utf8');
    }
    // Handle array of numbers (less common)
    else if (Array.isArray(data) && data.every(n => typeof n === 'number')) {
        return Buffer.from(data).toString('utf8');
    }
    console.warn(`[decodeInstructionData] Unhandled data type: ${typeof data}`);
    return null;
  } catch (e) {
    // console.warn("Failed to decode instruction data:", e.message); // Optional: log decoding failures
    return null;
  }
};


// --- START: Updated Memo Handling System ---
// (Memo logic functions: generateMemoId, normalizeMemo, validateOriginalMemoFormat,
// validateMemoChecksum, findMemoInTx, deepScanTransaction remain unchanged internally
// as they don't directly interact with the connection object)

// Define Memo Program IDs
const MEMO_V1_PROGRAM_ID = new PublicKey("Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo");
const MEMO_V2_PROGRAM_ID = new PublicKey("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr");
const MEMO_PROGRAM_IDS = [MEMO_V1_PROGRAM_ID.toBase58(), MEMO_V2_PROGRAM_ID.toBase58()];


// 1. Revised Memo Generation with Checksum (Used for *generating* our memos)
function generateMemoId(prefix = 'BET') {
    const validPrefixes = ['BET', 'CF', 'RA'];
    if (!validPrefixes.includes(prefix)) {
        prefix = 'BET';
    }
    const randomBytes = crypto.randomBytes(8);
    const hexString = randomBytes.toString('hex').toUpperCase();
    const checksum = crypto.createHash('sha256')
        .update(hexString)
        .digest('hex')
        .slice(-2)
        .toUpperCase();
    return `${prefix}-${hexString}-${checksum}`;
}

// *** normalizeMemo function with EXPLICIT TRAILING NEWLINE REMOVAL & STEP LOGGING ***
function normalizeMemo(rawMemo) {
    // Add a check for the rawMemo type and log if it's not a string
    if (typeof rawMemo !== 'string') {
        // console.warn(`[NORMALIZE_MEMO] Received non-string input: ${typeof rawMemo}`, rawMemo); // Reduce noise
        return null;
    }

    // console.log(`[NORMALIZE_MEMO] STEP 0 - Input Raw: "${rawMemo}"`); // Reduce noise

    // STEP 1: Explicitly remove potential SINGLE trailing newline FIRST
    let memo = rawMemo.replace(/\n$/, '');
    // console.log(`[NORMALIZE_MEMO] STEP 1 - After replace(/\\n$/, ''): "${memo}"`); // Reduce noise

    // STEP 2: Remove ALL control characters and trim remaining whitespace
    memo = memo.replace(/[\x00-\x1F\x7F]/g, '').trim();
    // console.log(`[NORMALIZE_MEMO] STEP 2 - After control char replace & trim: "${memo}"`); // Reduce noise

    // STEP 3: Standardize format aggressively (case, prefixes, spaces, invalid chars)
    memo = memo.toUpperCase()
        .replace(/^MEMO[:=\s]*/i, '')
        .replace(/^TEXT[:=\s]*/i, '')
        .replace(/[\u200B-\u200D\uFEFF]/g, '') // Remove zero-width spaces
        .replace(/\s+/g, '-') // Replace spaces with dashes
        .replace(/[^A-Z0-9\-]/g, ''); // Remove remaining non-alphanumeric/dash characters
    // console.log(`[NORMALIZE_MEMO] STEP 3 - Fully Standardized Candidate: "${memo}"`); // Reduce noise

   // STEP 3.5 - Explicitly handle potential trailing 'N' after checksum-like part
    const trailingNPattern = /^(BET|CF|RA)-([A-F0-9]{16})-([A-F0-9]{2})N$/;
    const trailingNMatch = memo.match(trailingNPattern);

    if (trailingNMatch) {
        console.warn(`[NORMALIZE_MEMO] STEP 3.5 - Detected V1-like memo with trailing 'N': "${memo}". Attempting correction by removing 'N'.`);
        memo = memo.slice(0, -1); // Remove the last character ('N')
        // console.log(`[NORMALIZE_MEMO] STEP 3.5 - Corrected candidate for V1 Check: "${memo}"`); // Reduce noise
    }

    // STEP 4: V1 Pattern Match and Checksum logic...
    const v1Pattern = /^(BET|CF|RA)-([A-F0-9]{16})-([A-F0-9]{2})$/;
    const v1Match = memo.match(v1Pattern);

    if (v1Match) {
        // console.log(`[NORMALIZE_MEMO] STEP 4 - V1 Pattern Matched.`); // Reduce noise
        const [/* full match */, prefix, hex, checksum] = v1Match;
        // console.log(`[NORMALIZE_MEMO] Extracted - Prefix: ${prefix}, Hex: ${hex}, Checksum (from input): ${checksum}`); // Reduce noise

        let expectedChecksum;
        try {
            expectedChecksum = crypto.createHash('sha256')
                .update(hex)
                .digest('hex')
                .slice(-2)
                .toUpperCase();
        } catch (hashError) {
             console.error(`[NORMALIZE_MEMO] CRITICAL ERROR calculating checksum for hex "${hex}":`, hashError);
             return null; // Exit if hashing fails
        }

        // console.log(`[NORMALIZE_MEMO] Calculated Expected Checksum: ${expectedChecksum}`); // Reduce noise
        if (checksum !== expectedChecksum) {
             console.warn(`[NORMALIZE_MEMO] Checksum MISMATCH! Input: "${checksum}", Expected: "${expectedChecksum}". Correcting.`);
        } // else {
          //  console.log(`[NORMALIZE_MEMO] Checksum MATCH! Input: "${checksum}", Expected: "${expectedChecksum}".`); // Reduce noise
       // }

        // Return the format with the CORRECT calculated checksum
        const finalMemo = `${prefix}-${hex}-${expectedChecksum}`;
        // console.log(`[NORMALIZE_MEMO] Returning Corrected V1 Memo: "${finalMemo}"`); // Reduce noise
        return finalMemo;
    }

    // STEP 4 - Fallback if V1 Pattern Did NOT Match
    // console.log(`[NORMALIZE_MEMO] STEP 4 - V1 Pattern Did NOT Match candidate "${memo}". Returning cleaned string or null.`); // Reduce noise
    // Basic sanity check for potentially valid non-V1 memos
    if (memo && memo.length >= 3 && memo.length <= 64) { // Example length constraints
         return memo;
    }
    return null; // Return null if memo becomes empty or doesn't meet basic criteria
}
// *** END: normalizeMemo function ***

// 2. Strict Memo Validation with Checksum Verification (Used only for validating OUR generated V1 format)
function validateOriginalMemoFormat(memo) {
    if (typeof memo !== 'string') return false;
    const parts = memo.split('-');
    if (parts.length !== 3) return false;
    const [prefix, hex, checksum] = parts;
    // Helper function to validate the checksum part
    const validateMemoChecksum = (h, cs) => {
        const expected = crypto.createHash('sha256').update(h).digest('hex').slice(-2).toUpperCase();
        return expected === cs;
    };
    return (
        ['BET', 'CF', 'RA'].includes(prefix) &&
        hex.length === 16 &&
        /^[A-F0-9]{16}$/.test(hex) &&
        checksum.length === 2 &&
        /^[A-F0-9]{2}$/.test(checksum) &&
        validateMemoChecksum(hex, checksum) // Validate against calculated checksum
    );
}

// <<< START: findMemoInTx FUNCTION (Adapted - uses latest normalizeMemo, retains structure) >>>
// NOTE: This function internally uses the *new* normalizeMemo defined above.
async function findMemoInTx(tx, signature) { // Added signature for logging
    const startTime = Date.now(); // For MEMO STATS
    const usedMethods = [];      // For MEMO STATS
    let scanDepth = 0;           // For MEMO STATS

    const transactionResponse = tx; // Type assertion

    if (!transactionResponse?.transaction?.message) {
        console.log(`[MEMO DEBUG] Sig ${signature?.slice(0,6)}: Invalid tx structure (missing message)`);
        return null;
    }

    // 1. First try the direct approach using log messages
    scanDepth = 1;
    if (transactionResponse.meta?.logMessages) {
        const memoLogRegex = /Program log: Memo(?: \(len \d+\))?:\s*"?([^"\n]+)"?/; // Explicitly exclude newlines
        for (const log of transactionResponse.meta.logMessages) {
             const match = log.match(memoLogRegex);
             if (match?.[1]) {
                 const rawMemo = match[1].trim();
                 const memo = normalizeMemo(rawMemo); // Use new normalize + trim/remove trailing \n
                 if (memo) {
                     usedMethods.push('LogScanRegex');
                     console.log(`[MEMO DEBUG] Sig ${signature?.slice(0,6)}: Found memo via LogScanRegex: "${memo}" (Raw: "${rawMemo}")`);
                     // MEMO STATS Logging
                     console.log(`[MEMO STATS] TX:${signature?.slice(0,8)}|M:${usedMethods.join(',')}|D:${scanDepth}|T:${Date.now() - startTime}ms`);
                     return memo;
                 }
             }
        }
    }

    // 2. Fallback to instruction parsing if not found in logs
    scanDepth = 2;
    const message = transactionResponse.transaction.message;
    let accountKeys = []; // Ensure initialized

    // Robustly get account keys as strings
    try {
        if (message.getAccountKeys) { // Handle VersionedMessage interface
             const accounts = message.getAccountKeys();
             accountKeys = accounts.staticAccountKeys.map(k => k.toBase58());
             // Note: We might need resolved lookup table accounts for full analysis,
             // but for memo finding, static keys are often enough.
        } else if (message.accountKeys) { // Handle legacy Message format
             accountKeys = message.accountKeys.map(k => k.toBase58 ? k.toBase58() : String(k));
        }
    } catch (e) {
        console.error(`[MEMO DEBUG] Sig ${signature?.slice(0,6)}: Error extracting account keys: ${e.message}`);
        // Proceed with potentially empty accountKeys array
    }

    // Combine regular and inner instructions for parsing
    const allInstructions = [
        ...(message.instructions || []),
        ...(transactionResponse.meta?.innerInstructions || []).flatMap(inner => inner.instructions || [])
    ];

    if (allInstructions.length === 0) {
         // console.log(`[MEMO DEBUG] Sig ${signature?.slice(0,6)}: No instructions found.`); // Reduce noise
    } else {
         for (const [i, inst] of allInstructions.entries()) {
             try {
                 // Resolve program ID string using the mapped accountKeys array
                 const programId = (inst.programIdIndex !== undefined && accountKeys[inst.programIdIndex])
                                    ? accountKeys[inst.programIdIndex]
                                    : null;

                 // Check for memo programs using resolved programId string
                 if (programId && MEMO_PROGRAM_IDS.includes(programId)) {
                     const dataString = decodeInstructionData(inst.data); // Use helper
                      if (dataString) {
                          const memo = normalizeMemo(dataString); // Use *new* normalizeMemo
                          if (memo) {
                              const method = MEMO_PROGRAM_IDS.indexOf(programId) === 0 ? 'InstrParseV1' : 'InstrParseV2';
                              usedMethods.push(method); // Log Method
                              console.log(`[MEMO DEBUG] Sig ${signature?.slice(0,6)}: Found ${method} memo via instruction parse: "${memo}"`);
                              // MEMO STATS Logging
                              console.log(`[MEMO STATS] TX:${signature?.slice(0,8)}|M:${usedMethods.join(',')}|D:${scanDepth}|T:${Date.now() - startTime}ms`);
                              return memo;
                          }
                      }
                 }

                 // Raw pattern matching fallback *within* the instruction loop (safety net)
                 scanDepth = 3;
                 const dataBuffer = inst.data ? (typeof inst.data === 'string' ? Buffer.from(toByteArray(inst.data)) : Buffer.from(inst.data)) : null;

                 if (dataBuffer && dataBuffer.length >= 22) { // Check minimum length for V1 format
                     const potentialMemo = dataBuffer.toString('utf8'); // Decode relevant part
                     const memo = normalizeMemo(potentialMemo); // Try to normalize/validate

                     // The new normalizeMemo handles V1 correction, so just check if it returns a valid V1 format
                     if (memo && validateOriginalMemoFormat(memo)) {
                          usedMethods.push('PatternMatchV1'); // Log Method
                          console.log(`[MEMO DEBUG] Sig ${signature?.slice(0,6)}: Pattern-matched and validated V1 memo: "${memo}" (Raw: "${potentialMemo}")`);
                          // MEMO STATS Logging
                          console.log(`[MEMO STATS] TX:${signature?.slice(0,8)}|M:${usedMethods.join(',')}|D:${scanDepth}|T:${Date.now() - startTime}ms`);
                          return memo;
                     }
                 }
             } catch (e) {
                 console.error(`[MEMO DEBUG] Sig ${signature?.slice(0,6)}: Error processing instruction ${i}:`, e?.message || e);
             }
         } // End for loop
    } // End else

    // Final check for address lookup tables
    if (message.addressTableLookups?.length > 0) {
       console.log(`[MEMO DEBUG] Sig ${signature?.slice(0,6)}: Transaction uses address lookup tables (memo finding may be incomplete).`);
    }

    // Enhanced Fallback Parsing (Log Scan - safety net)
    scanDepth = 4;
    if (transactionResponse.meta?.logMessages) {
        const logString = transactionResponse.meta.logMessages.join('\n');
        const logMemoMatch = logString.match(/(?:Memo|Text|Message|Data|Log):?\s*"?([A-Z0-9\-]{10,64})"?/i); // Broad pattern
        if (logMemoMatch?.[1]) {
            const recoveredMemo = normalizeMemo(logMemoMatch[1]); // Use *new* normalizeMemo
            if (recoveredMemo) {
                usedMethods.push('DeepLogScan'); // Log Method
                console.log(`[MEMO DEBUG] Sig ${signature?.slice(0,6)}: Recovered memo from deep log scan: ${recoveredMemo} (Matched: ${logMemoMatch[1]})`);
                // MEMO STATS Logging
                console.log(`[MEMO STATS] TX:${signature?.slice(0,8)}|M:${usedMethods.join(',')}|D:${scanDepth}|T:${Date.now() - startTime}ms`);
                return recoveredMemo;
            }
        }
    }

    console.log(`[MEMO DEBUG] Sig ${signature?.slice(0,6)}: Exhausted all search methods, no valid memo found.`);
    return null;
}
// <<< END: findMemoInTx FUNCTION >>>


// --- START: deepScanTransaction Function (Kept from original structure) ---
// NOTE: Not typically needed if findMemoInTx is robust, but kept as a deeper fallback.
// It will internally use the *new* normalizeMemo defined above.
async function deepScanTransaction(tx, accountKeys, signature) {
    // ... (deepScanTransaction logic remains the same, relying on normalizeMemo) ...
    // Note: The logic inside this function was already quite comprehensive and
    // relies on normalizeMemo, so no structural changes are needed here.
    // Only adding signature for logging context.
    const startTime = Date.now();
    const usedMethods = ['DeepScanInit'];
    let scanDepth = 5;

    try {
        if (!tx || !tx.meta || !tx.meta.innerInstructions || !accountKeys) {
            // console.log(`[MEMO DEEP SCAN] Sig ${signature?.slice(0,6)}: Insufficient data.`); // Reduce noise
            return null;
        }

        const innerInstructions = tx.meta.innerInstructions.flatMap(i => i.instructions || []);

        for (const inst of innerInstructions) {
             const programId = inst.programIdIndex !== undefined && accountKeys[inst.programIdIndex]
                                 ? accountKeys[inst.programIdIndex]
                                 : null;
             if (programId && MEMO_PROGRAM_IDS.includes(programId)) {
                 const dataString = decodeInstructionData(inst.data);
                 if (dataString) {
                     const memo = normalizeMemo(dataString);
                     if (memo) {
                         usedMethods.push('DeepInnerInstr');
                         console.log(`[MEMO DEEP SCAN] Sig ${signature?.slice(0,6)}: Found memo in inner instruction: ${memo}`);
                         console.log(`[MEMO STATS] TX:${signature?.slice(0,8)}|M:${usedMethods.join(',')}|D:${scanDepth}|T:${Date.now() - startTime}ms`);
                         return memo;
                     }
                 }
             }
        }

        scanDepth = 6;
        for (const inst of innerInstructions) {
             const dataString = decodeInstructionData(inst.data);
             const v1Pattern = /(BET|CF|RA)-([A-F0-9]{16})-([A-F0-9]{2})/;
             if (dataString && v1Pattern.test(dataString)) {
                 const memo = normalizeMemo(dataString);
                 if (memo && validateOriginalMemoFormat(memo)) {
                     usedMethods.push('DeepPatternV1');
                     console.log(`[MEMO DEEP SCAN] Sig ${signature?.slice(0,6)}: Found V1 pattern in inner data: ${memo}`);
                     console.log(`[MEMO STATS] TX:${signature?.slice(0,8)}|M:${usedMethods.join(',')}|D:${scanDepth}|T:${Date.now() - startTime}ms`);
                     return memo;
                 }
             }
        }
    } catch (e) {
        console.error(`[MEMO DEEP SCAN] Deep scan failed for TX ${signature?.slice(0,8)}:`, e.message);
    }
    // console.log(`[MEMO DEEP SCAN] No memo found after deep scan for TX ${signature?.slice(0,8)}.`); // Reduce noise
    return null;
}
// --- END: deepScanTransaction Function ---

// --- END: Updated Memo Handling System ---


// --- Database Operations ---
// (These functions remain structurally the same, relying on the corrected schema)

// Saves a new bet intention to the database
async function savePendingBet(userId, chatId, gameType, details, lamports, memoId, expiresAt, priority = 0) {
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
        if (err.code === '23505' && err.constraint === 'bets_memo_id_key') {
            console.warn(`DB: Memo ID collision for ${memoId}. User might be retrying.`);
            return { success: false, error: 'Memo ID already exists. Please try generating the bet again.' };
        }
        return { success: false, error: `Database error (${err.code || 'N/A'})` }; // More generic error
    }
}

// Finds a pending bet by its unique memo ID (Used internally by payment processor)
// The new GuaranteedPaymentProcessor._findBetGuaranteed handles retries/locking.
// This simpler version might be used elsewhere but isn't the primary lookup path for processing.
async function findBetByMemoBasic(memoId) {
    if (!memoId || typeof memoId !== 'string') {
        return undefined;
    }
    const query = `
        SELECT id, user_id, chat_id, game_type, bet_details, expected_lamports, status, expires_at, fees_paid, priority
        FROM bets
        WHERE memo_id = $1 AND status = 'awaiting_payment'
        ORDER BY priority DESC, created_at ASC
        LIMIT 1;
    `; // No FOR UPDATE here
    try {
        const res = await pool.query(query, [memoId]);
        return res.rows[0]; // Return the bet object or undefined
    } catch (err) {
        console.error(`DB Error finding bet by memo (basic) ${memoId}:`, err.message);
        return undefined;
    }
}


// Links a Solana wallet address to a Telegram User ID
async function linkUserWallet(userId, walletAddress) {
    const cacheKey = `wallet-${userId}`;
    const query = `
        INSERT INTO wallets (user_id, wallet_address, last_used_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (user_id)
        DO UPDATE SET
            wallet_address = EXCLUDED.wallet_address,
            last_used_at = NOW()
        RETURNING wallet_address;
    `;
    try {
        // Basic validation before DB insert
        new PublicKey(walletAddress); // Throws error if invalid address format

        const res = await pool.query(query, [String(userId), walletAddress]);
        const linkedWallet = res.rows[0]?.wallet_address;

        if (linkedWallet) {
            console.log(`DB: Linked/Updated wallet for user ${userId} to ${linkedWallet}`);
            // Update cache
            walletCache.set(cacheKey, { wallet: linkedWallet, timestamp: Date.now() });
            // Set TTL for cache entry deletion
            setTimeout(() => {
                 const current = walletCache.get(cacheKey);
                 // Only delete if it hasn't been refreshed in the meantime
                 if (current && current.wallet === linkedWallet) {
                     walletCache.delete(cacheKey);
                 }
             }, CACHE_TTL);
            return { success: true, wallet: linkedWallet };
        } else {
            console.error(`DB: Failed to link/update wallet for user ${userId}. Query returned no address.`);
            return { success: false, error: 'Failed to update wallet in database.' };
        }
    } catch (err) {
        if (err instanceof Error && err.message.includes('Invalid public key')) {
            console.error(`DB Error linking wallet: Invalid address format provided for user ${userId}: ${walletAddress}`);
            return { success: false, error: 'Invalid wallet address format.' };
        }
        console.error(`DB Error linking wallet for user ${userId}:`, err.message);
        return { success: false, error: `Database error (${err.code || 'N/A'})` };
    }
}

// Retrieves the linked wallet address for a user (checks cache first)
async function getLinkedWallet(userId) {
    const cacheKey = `wallet-${userId}`;

    // Check cache first
    if (walletCache.has(cacheKey)) {
        const { wallet, timestamp } = walletCache.get(cacheKey);
        // No explicit TTL check here needed if using setTimeout on set
        // console.log(`Cache hit for user ${userId} wallet.`); // Optional: Debug log
        return wallet; // Return cached wallet
    }

    // If not in cache, query DB
    const query = `SELECT wallet_address FROM wallets WHERE user_id = $1`;
    try {
        const res = await pool.query(query, [String(userId)]);
        const wallet = res.rows[0]?.wallet_address;

        if (wallet) {
            // Add to cache with TTL
            walletCache.set(cacheKey, { wallet, timestamp: Date.now() });
            setTimeout(() => {
                 const current = walletCache.get(cacheKey);
                 if (current && current.wallet === wallet) {
                    walletCache.delete(cacheKey);
                 }
             }, CACHE_TTL);
        }
        return wallet; // Return wallet address or undefined
    } catch (err) {
        console.error(`DB Error fetching wallet for user ${userId}:`, err.message);
        return undefined; // Indicate error or not found
    }
}

// Updates the status of a specific bet
async function updateBetStatus(betId, status) {
    const query = `UPDATE bets SET status = $1, processed_at = CASE WHEN $1 LIKE 'completed_%' OR $1 LIKE 'error_%' THEN NOW() ELSE processed_at END WHERE id = $2 RETURNING id;`; // Update processed_at on final states
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
            // Check current status if update failed
            const current = await pool.query('SELECT status FROM bets WHERE id = $1', [betId]);
            console.warn(`DB: Failed to record payout for bet ${betId} (not found or status mismatch?). Current status: ${current.rows[0]?.status ?? 'Not Found'}`);
            return false;
        }
    } catch (err) {
        if (err.code === '23505' && err.constraint === 'bets_payout_tx_signature_key') {
            console.warn(`DB: Payout TX Signature ${signature} collision for bet ${betId}. Already recorded.`);
            // Optionally, double-check the status here if this happens
             const current = await pool.query('SELECT status, payout_tx_signature FROM bets WHERE id = $1', [betId]);
             if(current.rows[0]?.payout_tx_signature === signature && current.rows[0]?.status.startsWith('completed_win')){
                 console.log(`DB: Bet ${betId} already correctly recorded with this payout signature. Treating as success.`);
                 return true; // Allow process to continue if already recorded correctly
             }
            return false;
        }
        console.error(`DB Error recording payout for bet ${betId}:`, err.message);
        return false;
    }
}

// --- End of Part 2 ---
// index.js (Updated for Multi-RPC RateLimitedConnection) - Part 3

// --- Solana Transaction Analysis ---
// (These functions remain structurally the same, no direct dependency on the connection type)

/**
 * Analyzes a transaction to find SOL transfers to the target bot wallet.
 * @param {import('@solana/web3.js').ParsedTransactionWithMeta | import('@solana/web3.js').VersionedTransactionResponse | null} tx - The transaction object.
 * @param {'coinflip' | 'race'} walletType - The type of wallet to check against (determines target address).
 * @returns {{ transferAmount: bigint, payerAddress: string | null }} The transfer amount in lamports and the detected payer address.
 */
function analyzeTransactionAmounts(tx, walletType) {
    let transferAmount = 0n; // Use BigInt for lamports
    let payerAddress = null;
    const targetAddress = walletType === 'coinflip'
        ? process.env.MAIN_WALLET_ADDRESS
        : process.env.RACE_WALLET_ADDRESS;

    if (!tx || tx.meta?.err) {
        // console.warn(`Skipping amount analysis for failed or missing TX`); // Reduce noise
        return { transferAmount: 0n, payerAddress: null }; // Ignore failed/missing transactions
    }

    // Check both pre and post balances for direct transfers to the target address
    if (tx.meta?.preBalances && tx.meta?.postBalances && tx.transaction?.message?.accountKeys) {
        const accountKeys = tx.transaction.message.accountKeys.map(keyInfo => {
            // Robust key extraction (handle PublicKey, string, {pubkey: PublicKey}, {pubkey: string})
            try {
                 if (keyInfo instanceof PublicKey) return keyInfo.toBase58();
                 if (keyInfo && typeof keyInfo.pubkey === 'string') return new PublicKey(keyInfo.pubkey).toBase58();
                 if (keyInfo && keyInfo.pubkey instanceof PublicKey) return keyInfo.pubkey.toBase58();
                 if (typeof keyInfo === 'string') return new PublicKey(keyInfo).toBase58();
            } catch { /* Ignore invalid keys */ }
            return null;
           }).filter(Boolean); // Remove nulls


        const targetIndex = accountKeys.indexOf(targetAddress);

        if (targetIndex !== -1 && tx.meta.preBalances[targetIndex] !== undefined && tx.meta.postBalances[targetIndex] !== undefined) {
            const balanceChange = BigInt(tx.meta.postBalances[targetIndex]) - BigInt(tx.meta.preBalances[targetIndex]);
            if (balanceChange > 0n) {
                transferAmount = balanceChange;
                // Try to identify payer (fee payer is primary suspect)
                const feePayerIndex = 0; // Fee payer is always the first account
                if (accountKeys[feePayerIndex]) {
                     const preFeePayerBalance = tx.meta.preBalances[feePayerIndex];
                     const postFeePayerBalance = tx.meta.postBalances[feePayerIndex];
                     if (preFeePayerBalance !== undefined && postFeePayerBalance !== undefined) {
                         const feePayerChange = BigInt(postFeePayerBalance) - BigInt(preFeePayerBalance);
                         // If fee payer's balance decreased by at least the transfer amount (plus fee)
                         if (feePayerChange < 0n) {
                              payerAddress = accountKeys[feePayerIndex];
                         }
                     }
                }

                // Fallback: Look for other signers if fee payer wasn't identified or didn't lose enough
                if (!payerAddress && tx.transaction?.message?.header?.numRequiredSignatures > 1) {
                    for (let i = 1; i < tx.transaction.message.header.numRequiredSignatures; i++) { // Start from 1 to skip fee payer
                         if (!accountKeys[i]) continue;
                         const preBalance = tx.meta.preBalances[i];
                         const postBalance = tx.meta.postBalances[i];
                         if (preBalance === undefined || postBalance === undefined) continue;
                         const signerBalanceChange = BigInt(postBalance) - BigInt(preBalance);
                         // Check if signer and their balance decreased appropriately
                         if (signerBalanceChange <= -transferAmount) { // Simple check: lost at least the transfer amount
                              payerAddress = accountKeys[i];
                              break;
                         }
                    }
                }
            }
        }
    }

    // Fallback or supplement with instruction parsing (less reliable for exact amount sometimes)
    // This part is less critical if balance changes work, but kept as backup
    if (transferAmount === 0n && tx.transaction?.message?.instructions) {
        const instructions = [
            ...(tx.transaction.message.instructions || []),
            ...(tx.meta?.innerInstructions || []).flatMap(i => i.instructions || [])
        ];
        const SYSTEM_PROGRAM_ID = SystemProgram.programId.toBase58();

        // Re-map account keys needed for instruction parsing fallback
        const accountKeysForInstr = tx.transaction.message.accountKeys?.map(keyInfo => {
             try {
                 if (keyInfo instanceof PublicKey) return keyInfo.toBase58();
                 if (keyInfo && typeof keyInfo.pubkey === 'string') return new PublicKey(keyInfo.pubkey).toBase58();
                 if (keyInfo && keyInfo.pubkey instanceof PublicKey) return keyInfo.pubkey.toBase58();
                 if (typeof keyInfo === 'string') return new PublicKey(keyInfo).toBase58();
            } catch { /* Ignore invalid keys */ }
            return null;
           });

        for (const inst of instructions) {
            let programId = '';
            try {
                if (inst.programIdIndex !== undefined && accountKeysForInstr && accountKeysForInstr[inst.programIdIndex]) {
                     programId = accountKeysForInstr[inst.programIdIndex];
                } else if (inst.programId) {
                     programId = inst.programId.toBase58 ? inst.programId.toBase58() : String(inst.programId);
                }
            } catch { /* Ignore errors getting programId */ }

            // Check for SystemProgram SOL transfers using parsed info (if available)
            if (programId === SYSTEM_PROGRAM_ID && inst.parsed?.type === 'transfer' && inst.parsed?.info) {
                const transferInfo = inst.parsed.info;
                if (transferInfo.destination === targetAddress) {
                    const instructionAmount = BigInt(transferInfo.lamports || transferInfo.amount || 0);
                    if (instructionAmount > 0n) {
                        transferAmount = instructionAmount;
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
    if (!tx || !tx.transaction?.message?.accountKeys || tx.transaction.message.accountKeys.length === 0) return null;

    const message = tx.transaction.message;
    // Fee payer is always the first account listed
    const feePayerKeyInfo = message.accountKeys[0];
    let feePayerAddress = null;
    try {
         if (feePayerKeyInfo instanceof PublicKey) {
             feePayerAddress = feePayerKeyInfo.toBase58();
         } else if (feePayerKeyInfo && typeof feePayerKeyInfo.pubkey === 'string') {
             feePayerAddress = new PublicKey(feePayerKeyInfo.pubkey).toBase58();
         } else if (feePayerKeyInfo && feePayerKeyInfo.pubkey instanceof PublicKey) {
             feePayerAddress = feePayerKeyInfo.pubkey.toBase58();
         } else if (typeof feePayerKeyInfo === 'string') {
             feePayerAddress = new PublicKey(feePayerKeyInfo).toBase58();
         }
    } catch (e) {
         console.warn("Could not parse fee payer address:", e.message);
    }

    if (feePayerAddress) {
         try {
             return new PublicKey(feePayerAddress); // Return PublicKey object
         } catch {
             return null; // Return null if address is invalid
         }
    }

    // Fallback logic (less reliable) - check signers and balance changes if meta is available
     if (!tx.meta || !tx.meta.preBalances || !tx.meta.postBalances) {
         return null; // Cannot perform fallback without balances
     }
     const preBalances = tx.meta.preBalances;
     const postBalances = tx.meta.postBalances;

     // Check other required signers if fee payer couldn't be parsed or isn't the only signer
     const numRequiredSignatures = message.header?.numRequiredSignatures || 1;
     for (let i = 0; i < Math.min(numRequiredSignatures, message.accountKeys.length); i++) {
         if (i === 0 && feePayerAddress) continue; // Skip fee payer if already identified

         const keyInfo = message.accountKeys[i];
         let key;
         try { // Extract PublicKey robustly
              if (keyInfo instanceof PublicKey) key = keyInfo;
              else if (keyInfo && typeof keyInfo.pubkey === 'string') key = new PublicKey(keyInfo.pubkey);
              else if (keyInfo && keyInfo.pubkey instanceof PublicKey) key = keyInfo.pubkey;
              else if (typeof keyInfo === 'string') key = new PublicKey(keyInfo);
              else continue;
         } catch (e) { continue; } // Skip if key is invalid

         // Check if balance decreased
         if (preBalances[i] !== undefined && postBalances[i] !== undefined) {
             const balanceDiff = BigInt(preBalances[i]) - BigInt(postBalances[i]);
             if (balanceDiff > 0) { // If balance decreased, likely a payer
                 // console.log(`Identified potential payer by balance change: ${key.toBase58()}`); // Optional log
                 return key;
             }
         }
     }


    // console.warn("Could not definitively identify payer."); // Reduce noise
    return null; // Could not determine payer
}


// --- Payment Processing System ---

// Checks if an error is likely retryable (e.g., network/rate limit issues)
// This is used by the GuaranteedPaymentProcessor internally
function isRetryableError(error) {
    const msg = error?.message?.toLowerCase() || '';
    const code = error?.code?.toLowerCase() || ''; // Include error code check
    const status = error?.response?.status || error?.statusCode; // HTTP status

    // Rate limit
    if (status === 429 || code === '429' || msg.includes('429') || msg.includes('rate limit') || msg.includes('too many requests')) {
        return true;
    }
    // Common HTTP/Network errors (potentially transient server-side or network issues)
    if ([500, 502, 503, 504].includes(Number(status))) {
        return true;
    }
    // Node.js network errors
    if (['timeout', 'timed out', 'econnreset', 'esockettimedout', 'network error', 'fetch', 'socket hang up', 'connection terminated', 'econnrefused', 'failed to fetch'].some(m => msg.includes(m)) ||
        ['etimedout', 'econnreset', 'enetunreach', 'eai_again', 'econnaborted', 'econnrefused'].includes(code)) {
        return true;
    }
    // Database potentially transient errors
    if (msg.includes('connection terminated unexpectedly') || code === 'econnrefused' || code === '57p01' /* admin shutdown */ || code === '57p03' /* cannot connect now */) {
        return true;
    }
    // Solana specific: Transaction simulation failed (might be temporary load)
    // Note: RateLimitedConnection handles this as RPC-specific, so processor might retry
    if (msg.includes('transaction simulation failed') || msg.includes('failed to simulate transaction')) {
         return true;
    }
    // Blockhash not found is generally NOT retryable at this stage (it's expired)
    // if (msg.includes('blockhash not found')) return true;

    return false;
}

// *** START: New GuaranteedPaymentProcessor class from "Ultimate Fix" ***
// (No changes needed here, it uses the RateLimitedConnection passed to it implicitly)
class GuaranteedPaymentProcessor {
    constructor() {
        this.highPriorityQueue = new PQueue({ concurrency: 3 });
        this.normalQueue = new PQueue({ concurrency: 2 });
        this.activeProcesses = new Set();
        this.memoCache = new Map(); // Cache for memo lookups
        this.cacheTTL = 30000; // Cache memo results for 30 seconds
        console.log("✅ Initialized GuaranteedPaymentProcessor"); // Log initialization
    }

    // Adds a job to the appropriate queue based on priority
    async addPaymentJob(job) {
        const queue = (job.priority && job.priority > 0) ? this.highPriorityQueue : this.normalQueue;
        queue.add(() => this.processJob(job)).catch(queueError => {
            console.error(`Queue error processing job ${job.type} (${job.signature || job.betId || 'N/A'}):`, queueError.message);
            performanceMonitor.logRequest(false);
        });
    }

    // Wrapper to handle job execution, retries, and error logging
    async processJob(job) {
        const jobIdentifier = job.signature || job.betId;
        if (jobIdentifier && this.activeProcesses.has(jobIdentifier)) {
            // console.warn(`Job for identifier ${jobIdentifier} already active, skipping duplicate.`); // Reduce noise
            return;
        }
        if (jobIdentifier) this.activeProcesses.add(jobIdentifier);

        try {
            let result;
            if (job.type === 'monitor_payment') {
                result = await this._processIncomingPayment(job.signature, job.walletType);
            } else if (job.type === 'process_bet') {
                // Game processing logic remains separate
                // Ensure we fetch the bet row needed by processPaidBet
                const bet = await pool.query('SELECT * FROM bets WHERE id = $1', [job.betId]).then(res => res.rows[0]);
                if (bet) {
                    await processPaidBet(bet); // Trigger game logic processing
                    result = { processed: true };
                } else {
                    console.error(`Cannot process bet: Bet ID ${job.betId} not found.`);
                    result = { processed: false, reason: 'bet_not_found' };
                }
            } else if (job.type === 'payout') {
                // Payout logic remains separate
                await handlePayoutJob(job); // Trigger payout logic
                result = { processed: true };
            } else {
                console.error(`Unknown job type: ${job.type}`);
                result = { processed: false, reason: 'unknown_job_type'};
            }
            performanceMonitor.logRequest(true);
            return result;
        } catch (error) {
            performanceMonitor.logRequest(false);
            console.error(`Error processing job type ${job.type} for identifier ${jobIdentifier || 'N/A'} in processJob:`, error.message, error.stack);
            // Retries handled internally by _methods
            console.error(`Job failed permanently for identifier: ${jobIdentifier}`, error);
             // Update bet status to an error state if applicable
             if(job.betId && job.type !== 'monitor_payment' && !(error?.reason?.includes('already_processed'))) { // Avoid overwriting status if already handled
                 const errorStatus = `error_${job.type}_failed`;
                 await updateBetStatus(job.betId, errorStatus);
                 console.log(`Set bet ${job.betId} status to ${errorStatus} due to job failure.`);
             }
             return { processed: false, reason: error.message }; // Return failure reason

        } finally {
            if (jobIdentifier) {
                this.activeProcesses.delete(jobIdentifier);
            }
        }
    }

    // --- New Helper Methods from "Ultimate Fix" ---

    // Main entry point for processing a detected signature
    async _processIncomingPayment(signature, walletType) {
        // 1. Session-level duplicate check
        if (processedSignaturesThisSession.has(signature)) {
            // console.log(`Sig ${signature}: Already processed this session.`); // Reduce noise
            return { processed: false, reason: 'already_processed_session' };
        }

        // 2. Database-level duplicate check (checks bets.paid_tx_signature)
        try {
            const exists = await pool.query(
                'SELECT 1 FROM bets WHERE paid_tx_signature = $1 LIMIT 1',
                [signature]
            );
            if (exists.rowCount > 0) {
                // console.log(`Sig ${signature}: Already recorded in DB.`); // Reduce noise
                processedSignaturesThisSession.add(signature); // Cache if found in DB
                return { processed: false, reason: 'exists_in_db' };
            }
        } catch (dbError) {
             console.error(`Sig ${signature}: DB error checking paid_tx_signature: ${dbError.message}`);
             return { processed: false, reason: 'db_check_error', error: dbError }; // Propagate error
        }

        let tx;
        try {
             // 3. Transaction fetching with robust error handling (uses RateLimitedConnection implicitly)
             console.log(`Sig ${signature?.slice(0,6)}: Fetching transaction...`);
             tx = await this._getTransactionWithRetry(signature); // Uses internal retries via connection

             // Check if fetch ultimately failed or TX failed on-chain
             if (!tx) {
                 // This case means retries failed or tx not found after reasonable time
                 // Do not cache signature here, monitor might find it later if it confirms slowly
                 console.log(`Sig ${signature?.slice(0,6)}: Failed to fetch valid transaction after retries.`);
                 // Check if transaction actually failed on chain (if meta exists)
                 const failureReason = tx === null ? 'fetch_failed' : (tx?.meta?.err ? 'onchain_failure' : 'unknown_fetch_issue');
                 if (failureReason === 'onchain_failure') {
                     processedSignaturesThisSession.add(signature); // Cache on-chain failures
                 }
                 return { processed: false, reason: failureReason };
             }
             if (tx.meta?.err){
                console.log(`Sig ${signature?.slice(0,6)}: Transaction failed on-chain, skipping. Error: ${JSON.stringify(tx.meta.err)}`);
                processedSignaturesThisSession.add(signature); // Cache txs that failed on-chain
                return { processed: false, reason: 'onchain_failure' };
             }


             // 4. Memo extraction with guaranteed normalization
             console.log(`Sig ${signature?.slice(0,6)}: Extracting memo...`);
             const memo = await this._extractMemoGuaranteed(tx, signature); // Uses normalizeMemo internally
             if (!memo) {
                 console.log(`Sig ${signature?.slice(0,6)}: No valid memo found after extraction.`);
                 processedSignaturesThisSession.add(signature); // Cache if no memo found
                 return { processed: false, reason: 'no_valid_memo' };
             }
             console.log(`Sig ${signature?.slice(0,6)}: Found memo "${memo}".`);

             // 5. Bet lookup with guaranteed finding (includes retries & cache)
             console.log(`Sig ${signature?.slice(0,6)}: Looking up bet for memo "${memo}"...`);
             const bet = await this._findBetGuaranteed(memo); // Use new bet finding method
             if (!bet) {
                 console.log(`Sig ${signature?.slice(0,6)}: No awaiting_payment bet found for memo "${memo}" after retries/cache check.`);
                  // Check if a bet with this memo exists but isn't awaiting_payment
                 const existingBetStatus = await pool.query('SELECT status FROM bets WHERE memo_id = $1 LIMIT 1', [memo]).then(r=>r.rows[0]?.status);
                 if (existingBetStatus && existingBetStatus !== 'awaiting_payment') {
                     console.log(`Sig ${signature?.slice(0,6)}: Bet for memo "${memo}" exists but status is '${existingBetStatus}'. Caching signature.`);
                     processedSignaturesThisSession.add(signature); // Cache signature if bet is already processed/errored
                     return { processed: false, reason: 'bet_already_processed_or_errored' };
                 }
                 // If no bet found at all, don't cache sig, could be unrelated tx or bet created later
                 return { processed: false, reason: 'no_matching_bet_final' };
             }
             console.log(`Sig ${signature?.slice(0,6)}: Found bet ID ${bet.id} for memo "${memo}".`);

             // 6. Process payment with guaranteed completion (includes DB transaction)
             console.log(`Sig ${signature?.slice(0,6)}: Processing payment for bet ID ${bet.id}...`);
             return await this._processPaymentGuaranteed(bet, signature, walletType, tx); // Pass tx object

        } catch (error) {
             // Catch errors from fetching, memo extraction, bet finding, or processing
              console.error(`Sig ${signature?.slice(0,6)}: Critical error during processing: ${error.message}`, error.stack);
              // Decide whether to cache based on the error type
              const isDefinitiveFailure = !isRetryableError(error) || error.reason === 'onchain_failure' || error.reason === 'no_valid_memo' || error.reason?.includes('_final');
              if (isDefinitiveFailure) {
                  processedSignaturesThisSession.add(signature);
                  console.log(`Sig ${signature?.slice(0,6)}: Caching signature due to non-retryable/definitive error.`);
              }
              // Update bet status if we have a bet ID and it failed during processing payment
              if (error.betId) { // Assuming error might carry betId context
                  await updateBetStatus(error.betId, 'error_processing_exception');
              }
              return { processed: false, reason: `processing_error: ${error.message}` };
        }
    }

    // Helper to get transaction with retries (now simpler, relies on RateLimitedConnection)
    async _getTransactionWithRetry(signature) {
         try {
             // Use the rate-limited connection directly. Retries/rotation handled internally.
             const tx = await solanaConnection.getParsedTransaction(signature, {
                 maxSupportedTransactionVersion: 0, // Fetch as Versioned Tx if possible
                 commitment: 'confirmed' // Ensure we get confirmed transactions
             });
             // Return the result (could be null if not found by RPC after its retries, or the tx object)
              return tx;
         } catch (error) {
             // Log the final error if the RateLimitedConnection gives up
             console.error(`Sig ${signature?.slice(0,6)}: Final error from solanaConnection.getParsedTransaction: ${error.message}`);
             // Return null to indicate failure to the caller
             return null;
         }
     }


    // Helper to extract memo using multiple methods (uses new normalizeMemo)
    async _extractMemoGuaranteed(tx, signature) {
         // ... (logic remains the same - uses findMemoInTx) ...
         if (!tx) return null;
         return findMemoInTx(tx, signature); // Directly use the existing robust function
    }

    // Helper to find bet with retries and caching
    async _findBetGuaranteed(memo, attempts = 3) {
        const cacheKey = `bet-memo-${memo}`;
        const cachedBet = this.memoCache.get(cacheKey);
        if (cachedBet) {
             // Re-verify status before returning from cache
             if (cachedBet.status === 'awaiting_payment') {
                 // console.log(`Memo ${memo}: Cache hit.`); // Reduce noise
                 return cachedBet;
             } else {
                 this.memoCache.delete(cacheKey); // Evict if status changed
             }
        }

        // Database lookup with retries for temporary locks or connection issues
        for (let i = 0; i < attempts; i++) {
            let client;
            try {
                client = await pool.connect();
                // Use FOR UPDATE SKIP LOCKED to handle potential concurrency
                const bet = await client.query(`
                    SELECT id, user_id, chat_id, game_type, bet_details, expected_lamports, status, expires_at, fees_paid, priority, memo_id
                    FROM bets
                    WHERE memo_id = $1 AND status = 'awaiting_payment'
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                `, [memo]).then(r => r.rows[0]);

                if (bet) {
                    // console.log(`Memo ${memo}: DB lookup successful on attempt ${i + 1}.`); // Reduce noise
                    // Add to cache with TTL
                    this.memoCache.set(cacheKey, bet);
                    setTimeout(() => {
                        const currentCached = this.memoCache.get(cacheKey);
                        if (currentCached && currentCached.id === bet.id) {
                            this.memoCache.delete(cacheKey);
                        }
                    }, this.cacheTTL);
                    return bet; // Return the found bet
                }
                // If bet is null, row was skipped (locked) or doesn't exist in awaiting_payment state.
                // console.log(`Memo ${memo}: DB lookup attempt ${i + 1} found no unlocked awaiting_payment bet.`); // Reduce noise

            } catch (error) {
                console.error(`Memo ${memo}: DB lookup attempt ${i + 1} failed: ${error.message}`);
                if (i === attempts - 1 || !isRetryableError(error)) {
                     throw error; // Rethrow final or non-retryable error
                }
                // Wait before retrying DB connection/query
                await new Promise(r => setTimeout(r, 300 * (i + 1)));
            } finally {
                 if (client) client.release(); // Release client for this attempt
            }
            // Small delay even if query succeeded but returned no rows (i.e., skipped locked row or not found)
             if (i < attempts - 1) {
                 await new Promise(r => setTimeout(r, 200 * (i + 1)));
             }
        }
        // console.log(`Memo ${memo}: Bet not found after ${attempts} attempts.`); // Reduce noise
        return null;
    }

    // Helper to process payment within a DB transaction
    async _processPaymentGuaranteed(bet, signature, walletType, tx) {
        const client = await pool.connect();
        try {
            await client.query('BEGIN');

            // 1. Verify bet status again within transaction for atomicity
            const currentBet = await client.query(`
                SELECT status
                FROM bets WHERE id = $1
                FOR UPDATE
            `, [bet.id]).then(r => r.rows[0]);

            if (!currentBet) {
                console.warn(`Sig ${signature?.slice(0,6)}: Bet ID ${bet.id} not found during final lock.`);
                await client.query('ROLLBACK');
                return { processed: false, reason: 'bet_disappeared' };
            }
            if (currentBet.status !== 'awaiting_payment') {
                 console.warn(`Sig ${signature?.slice(0,6)}: Bet ID ${bet.id} status was ${currentBet.status}, not awaiting_payment, during final lock.`);
                 await client.query('ROLLBACK');
                 processedSignaturesThisSession.add(signature); // Cache the signature here
                 return { processed: false, reason: 'bet_already_processed_final' };
            }

            // --- Additional Validation Checks within Transaction ---
            // 8a. Analyze transaction amounts
            const { transferAmount, payerAddress: detectedPayer } = analyzeTransactionAmounts(tx, walletType);
            if (transferAmount <= 0n) {
                console.warn(`Sig ${signature?.slice(0,6)}: Bet ID ${bet.id}. No SOL transfer found in final check.`);
                await client.query(`UPDATE bets SET status = 'error_no_transfer_found', processed_at = NOW() WHERE id = $1`, [bet.id]);
                await client.query('ROLLBACK');
                processedSignaturesThisSession.add(signature); // Cache on definitive validation failure
                return { processed: false, reason: 'no_transfer_found_final' };
            }

            // 8b. Validate amount sent vs expected (with tolerance)
            const expectedAmount = BigInt(bet.expected_lamports);
            const tolerance = BigInt(process.env.PAYMENT_TOLERANCE_LAMPORTS || '5000'); // 0.000005 SOL tolerance default
            if (transferAmount < (expectedAmount - tolerance) || transferAmount > (expectedAmount + tolerance)) {
                console.warn(`Sig ${signature?.slice(0,6)}: Bet ID ${bet.id}. Amount mismatch. Expected ~${expectedAmount}, got ${transferAmount}.`);
                await client.query(`UPDATE bets SET status = 'error_payment_mismatch', processed_at = NOW() WHERE id = $1`, [bet.id]);
                await client.query('COMMIT'); // Commit the error status
                processedSignaturesThisSession.add(signature); // Cache on definitive validation failure
                // Send Telegram message *after* releasing DB connection (COMMIT occurs first)
                safeSendMessage(bet.chat_id, `⚠️ Payment amount mismatch for bet \`${bet.memo_id}\`. Expected ${Number(expectedAmount)/LAMPORTS_PER_SOL} SOL, received ${Number(transferAmount)/LAMPORTS_PER_SOL} SOL. Bet cancelled.`, { parse_mode: 'Markdown' }).catch(e => console.error("TG Send Error:", e.message));
                return { processed: false, reason: 'amount_mismatch_final' };
            }

            // 8c. Validate transaction time vs bet expiry
            const txTime = tx.blockTime ? new Date(tx.blockTime * 1000) : null;
            const expiryTime = new Date(bet.expires_at);
             // Add a grace period to expiry (e.g., 60 seconds) to account for block confirmation time variance
             const expiryGracePeriodMs = 60 * 1000;
             const effectiveExpiryTime = new Date(expiryTime.getTime() + expiryGracePeriodMs);

            if (!txTime) {
                console.warn(`Sig ${signature?.slice(0,6)}: Bet ID ${bet.id}. Could not determine blockTime for final expiry check. Allowing processing.`);
                 // Decide if this is critical - currently allowing processing but logging warning
                 // If critical, uncomment below to reject:
                 /*
                 await client.query(`UPDATE bets SET status = 'error_missing_blocktime', processed_at = NOW() WHERE id = $1`, [bet.id]);
                 await client.query('COMMIT');
                 processedSignaturesThisSession.add(signature);
                 return { processed: false, reason: 'missing_blocktime_final' };
                 */
            } else if (txTime > effectiveExpiryTime) {
                console.warn(`Sig ${signature?.slice(0,6)}: Bet ID ${bet.id}. Payment received at ${txTime.toISOString()} after effective expiry ${effectiveExpiryTime.toISOString()}.`);
                await client.query(`UPDATE bets SET status = 'error_payment_expired', processed_at = NOW() WHERE id = $1`, [bet.id]);
                await client.query('COMMIT'); // Commit error status
                processedSignaturesThisSession.add(signature); // Cache on definitive validation failure
                // Send Telegram message *after* releasing DB connection
                safeSendMessage(bet.chat_id, `⚠️ Payment for bet \`${bet.memo_id}\` received after expiry time. Bet cancelled.`, { parse_mode: 'Markdown' }).catch(e => console.error("TG Send Error:", e.message));
                return { processed: false, reason: 'expired_final' };
            }
            // --- End Additional Validation Checks ---


            // 2. Mark as paid (if all checks passed)
            await client.query(`
                UPDATE bets
                SET status = 'payment_verified',
                    paid_tx_signature = $1,
                    processed_at = NOW() -- Record verification time
                WHERE id = $2
            `, [signature, bet.id]);

            // 3. Link wallet if possible
            const payer = detectedPayer || getPayerFromTransaction(tx)?.toBase58();
            if (payer) {
                 try {
                      // Use the dedicated function which includes validation and caching
                      await linkUserWallet(bet.user_id, payer); // Call the existing helper
                 } catch (walletError) {
                      // linkUserWallet already logs errors, just note it here
                      console.warn(`Sig ${signature?.slice(0,6)}: Bet ID ${bet.id}. Non-critical issue linking wallet for payer "${payer}": ${walletError.message}`);
                 }
            } else {
                 console.warn(`Sig ${signature?.slice(0,6)}: Bet ID ${bet.id}. Could not identify valid payer address to link wallet.`);
            }

            // Commit the transaction (marks paid, links wallet)
            await client.query('COMMIT');
            console.log(`Sig ${signature?.slice(0,6)}: Bet ID ${bet.id} successfully marked as 'payment_verified' and wallet linked (if possible).`);

            // Add signature to session cache ONLY AFTER successful commit
            processedSignaturesThisSession.add(signature);
             if (processedSignaturesThisSession.size > MAX_PROCESSED_SIGNATURES) {
                 console.log(`[CACHE] Clearing processed signatures cache (reached ${processedSignaturesThisSession.size}/${MAX_PROCESSED_SIGNATURES})`);
                 processedSignaturesThisSession.clear();
             }


            // 4. Queue for game processing (after successful commit)
            // Fetch full bet details again to ensure we pass fresh data after commit
            // Note: Use bet.id from the originally found bet object
             const fullBetDetails = await pool.query('SELECT * FROM bets WHERE id = $1', [bet.id]).then(res => res.rows[0]);
             if (fullBetDetails && fullBetDetails.status === 'payment_verified') { // Double-check status after commit
                 await this._queueBetProcessing(fullBetDetails); // Pass the full details
             } else {
                 console.error(`Sig ${signature?.slice(0,6)}: Failed to retrieve full bet details or status mismatch for Bet ID ${bet.id} after commit. Cannot queue game processing. Status: ${fullBetDetails?.status ?? 'Not Found'}`);
                 // This indicates a potential problem, status should be payment_verified
             }
            return { processed: true };

        } catch (error) {
            console.error(`Sig ${signature?.slice(0,6)}: Error during final payment processing TXN for Bet ID ${bet.id}: ${error.message}`, error.stack);
            await client.query('ROLLBACK').catch((rbError) => console.error(`Rollback failed: ${rbError.message}`));
            error.betId = bet.id; // Add context for outer catch
            throw error; // Re-throw error
        } finally {
            client.release();
        }
    }

     // Helper method to queue bet processing
     async _queueBetProcessing(bet) {
        console.log(`Payment verified for bet ${bet.id} (${bet.memo_id}). Queuing for game processing.`);
        await this.addPaymentJob({
            type: 'process_bet',
            betId: bet.id,
            priority: 1, // High priority for game logic
            // signature: bet.paid_tx_signature // Optional context
        });
     }

} // *** END: New GuaranteedPaymentProcessor class ***

// Instantiate the new payment processor
const paymentProcessor = new GuaranteedPaymentProcessor();


// --- Payment Monitoring Loop ---
let isMonitorRunning = false; // Flag to prevent concurrent monitor runs
const botStartupTime = Math.floor(Date.now() / 1000); // Timestamp in seconds
let monitorInterval = null; // Holds the setInterval ID

// *** MONITOR LOGIC (MODIFIED): REMOVED EXPLICIT BACKOFF, RELIES ON RateLimitedConnection ***
async function monitorPayments() {
    if (isMonitorRunning) {
        // console.log('[Monitor] Cycle already running, skipping.'); // Reduce noise
        return;
    }
    if (!isFullyInitialized) {
        // console.log('[Monitor] Skipping cycle, application not fully initialized yet.'); // Reduce noise
        return;
    }

    isMonitorRunning = true;
    const startTime = Date.now();
    let signaturesFoundThisCycle = 0;
    let signaturesQueuedThisCycle = 0;

    try {
        // --- Optional: Throttle based on Payment Processor Queue Size ---
        const paymentQueueLoad = (paymentProcessor.highPriorityQueue.pending + paymentProcessor.normalQueue.pending +
                                 paymentProcessor.highPriorityQueue.size + paymentProcessor.normalQueue.size);
         const monitorThrottleMs = parseInt(process.env.MONITOR_THROTTLE_MS_PER_ITEM || '50', 10);
         const maxMonitorThrottle = parseInt(process.env.MONITOR_MAX_THROTTLE_MS || '5000', 10);
         const throttleDelay = Math.min(maxMonitorThrottle, paymentQueueLoad * monitorThrottleMs);

         if (throttleDelay > 100) { // Only log if throttling significantly
             console.log(`[Monitor] Payment queues have ${paymentQueueLoad} items. Throttling monitor check for ${throttleDelay}ms.`);
             await new Promise(resolve => setTimeout(resolve, throttleDelay));
         }
         // --- End Optional Throttle ---


        // console.log("⚙️ Performing payment monitor run..."); // Reduce noise

        const monitoredWallets = [
             { address: process.env.MAIN_WALLET_ADDRESS, type: 'coinflip', priority: 0 },
             { address: process.env.RACE_WALLET_ADDRESS, type: 'race', priority: 0 },
        ];

        for (const wallet of monitoredWallets) {
            // Add small random delay/jitter before each wallet check
             const jitter = Math.random() * (parseInt(process.env.MONITOR_WALLET_JITTER_MS || '1500', 10)); // 0-1.5s jitter default
             await new Promise(resolve => setTimeout(resolve, jitter));

            const walletAddress = wallet.address;
            let signaturesForWallet = [];

            try {
                // Fetch the latest N signatures, DO NOT use 'before' or 'until'
                 const fetchLimit = parseInt(process.env.MONITOR_FETCH_LIMIT || '25', 10);
                 const options = { limit: fetchLimit };
                 // console.log(`[Monitor] Checking ${wallet.type} wallet (${walletAddress.slice(0,6)}...) for latest ${options.limit} signatures.`); // Reduce noise

                 // Use the RateLimitedConnection directly - it handles retries/errors/rotation
                 signaturesForWallet = await solanaConnection.getSignaturesForAddress(
                     new PublicKey(walletAddress),
                     options
                 );

                 if (!signaturesForWallet || signaturesForWallet.length === 0) {
                     // console.log(`[Monitor] No recent signatures found for ${walletAddress.slice(0,6)}...`); // Reduce noise
                     continue; // Skip to next wallet
                 }

                 signaturesFoundThisCycle += signaturesForWallet.length;

                 // Filter out potentially old transactions (e.g., older than bot startup + buffer)
                 // Also filter out transactions known to have failed on-chain
                  const startupBufferSeconds = 600; // Check 10 mins prior to startup
                  const recentSignatures = signaturesForWallet.filter(sigInfo => {
                      // Filter based on blockTime
                      if (sigInfo.blockTime && sigInfo.blockTime < (botStartupTime - startupBufferSeconds)) {
                          return false;
                      }
                      // Filter based on on-chain error (if present)
                      if (sigInfo.err) {
                           // console.log(`[Monitor] Skipping failed TX ${sigInfo.signature.slice(0,6)} for ${walletAddress.slice(0,6)}...`); // Reduce noise
                           processedSignaturesThisSession.add(sigInfo.signature); // Cache known failures
                           return false;
                      }
                      return true;
                  });


                 if (recentSignatures.length === 0) {
                      continue;
                 }

                 // Process oldest first within the fetched batch by reversing the array
                 recentSignatures.reverse();

                 for (const sigInfo of recentSignatures) {
                     // Check session cache first (quickest check)
                     if (processedSignaturesThisSession.has(sigInfo.signature)) {
                         continue;
                     }

                     // Check if already being processed by another job
                     if (paymentProcessor.activeProcesses.has(sigInfo.signature)) {
                         continue;
                     }

                     // Queue the signature for full processing if not in cache or active
                     console.log(`[Monitor] Queuing signature ${sigInfo.signature.slice(0,10)}... for ${wallet.type} wallet.`);
                     signaturesQueuedThisCycle++;
                     await paymentProcessor.addPaymentJob({
                         type: 'monitor_payment',
                         signature: sigInfo.signature,
                         walletType: wallet.type,
                         priority: wallet.priority, // Normal priority for initial check
                         // Retries handled internally by GuaranteedPaymentProcessor methods now
                     });
                 } // End loop through signatures

            } catch (error) {
                // Catch errors during signature fetching for a specific wallet
                // RateLimitedConnection should handle most retryable RPC errors internally.
                // Log errors that bubble up here, but don't implement specific backoff logic in the monitor.
                console.error(`[Monitor] Error fetching/processing signatures for wallet ${walletAddress}:`, error.message);
                // Don't increase interval here, let RateLimitedConnection manage its state
                performanceMonitor.logRequest(false);
            }
        } // End loop through wallets

    } catch (err) {
        // General error handling for the monitor loop itself
        console.error('❌ MonitorPayments Error in main block:', err);
        performanceMonitor.logRequest(false);
        // No explicit backoff adjustment here

    } finally {
        isMonitorRunning = false; // Release the lock
        const duration = Date.now() - startTime;
        // Only log detailed completion if something was queued or took time
        if (signaturesQueuedThisCycle > 0 || duration > 1000) {
             console.log(`[Monitor] Cycle completed in ${duration}ms. Found:${signaturesFoundThisCycle}. Queued:${signaturesQueuedThisCycle}.`);
        }
    }
}
// *** END UPDATED MONITOR FUNCTION ***


// --- SOL Sending Function ---
// MODIFIED: Removed outer retry loop, relying on sendAndConfirmTransaction + RateLimitedConnection
/**
 * Sends SOL to a recipient, handling priority fees and confirmation.
 * @param {string | PublicKey} recipientPublicKey - The recipient's address.
 * @param {bigint} amountLamports - The amount to send in lamports.
 * @param {'coinflip' | 'race'} gameType - Determines which private key to use for sending.
 * @returns {Promise<{success: boolean, signature?: string, error?: string}>} Result object.
 */
async function sendSol(recipientPublicKey, amountLamports, gameType) {
    const operationId = `sendSol-${gameType}-${Date.now().toString().slice(-6)}`; // Unique ID for logging
    console.log(`[${operationId}] Initiating. Recipient: ${recipientPublicKey}, Amount: ${amountLamports}`);

    // Select the correct private key
    const privateKey = gameType === 'coinflip'
        ? process.env.BOT_PRIVATE_KEY
        : process.env.RACE_BOT_PRIVATE_KEY;

    if (!privateKey) {
        console.error(`[${operationId}] ❌ ERROR: Missing private key for game type ${gameType}.`);
        return { success: false, error: `Missing private key for ${gameType}` };
    }

    // Validate recipient address
    let recipientPubKey;
    try {
        recipientPubKey = (typeof recipientPublicKey === 'string')
            ? new PublicKey(recipientPublicKey)
            : recipientPublicKey;
        if (!(recipientPubKey instanceof PublicKey)) throw new Error("Invalid recipient public key type");
        console.log(`[${operationId}] Recipient address validated: ${recipientPubKey.toBase58()}`);
    } catch (e) {
        console.error(`[${operationId}] ❌ ERROR: Invalid recipient address format: "${recipientPublicKey}". Error: ${e.message}`);
        return { success: false, error: `Invalid recipient address: ${e.message}` };
    }

    // Validate amount
    const amountToSend = BigInt(amountLamports);
    if (amountToSend <= 0n) {
        console.error(`[${operationId}] ❌ ERROR: Payout amount ${amountLamports} is zero or negative.`);
        return { success: false, error: 'Payout amount is zero or negative' };
    }
    const amountSOL = Number(amountToSend) / LAMPORTS_PER_SOL;
    console.log(`[${operationId}] Amount: ${amountToSend} lamports (${amountSOL.toFixed(6)} SOL)`);

    // Calculate dynamic priority fee
    const basePriorityFee = parseInt(process.env.PAYOUT_BASE_PRIORITY_FEE_MICROLAMPORTS || '1000', 10); // Min fee
    const maxPriorityFee = parseInt(process.env.PAYOUT_MAX_PRIORITY_FEE_MICROLAMPORTS || '1000000', 10); // Max fee (0.001 SOL)
    const calculatedFee = Math.floor(Number(amountToSend) * PRIORITY_FEE_RATE);
    const priorityFeeMicroLamports = Math.max(basePriorityFee, Math.min(calculatedFee, maxPriorityFee));
    console.log(`[${operationId}] Calculated priority fee: ${priorityFeeMicroLamports} microLamports`);

    try {
        console.log(`[${operationId}] Loading payer wallet...`);
        const payerWallet = Keypair.fromSecretKey(bs58.decode(privateKey));
        console.log(`[${operationId}] Payer wallet loaded: ${payerWallet.publicKey.toBase58()}`);

        // Get latest blockhash (uses RateLimitedConnection internally)
        console.log(`[${operationId}] Fetching latest blockhash (commitment: confirmed)...`);
        // Use confirmed commitment for blockhash when sending confirmed transactions
        const latestBlockhash = await solanaConnection.getLatestBlockhash('confirmed');
        if (!latestBlockhash || !latestBlockhash.blockhash || !latestBlockhash.lastValidBlockHeight) {
            throw new Error('Failed to get valid latest blockhash object from RPC.');
        }
        console.log(`[${operationId}] Got blockhash: ${latestBlockhash.blockhash}, Last Valid Block Height: ${latestBlockhash.lastValidBlockHeight}`);

        console.log(`[${operationId}] Building transaction...`);
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
        // Optional: Add compute limit if needed, though usually not for simple transfers
        // transaction.add(ComputeBudgetProgram.setComputeUnitLimit({ units: 300 }));

        // Add the SOL transfer instruction
        transaction.add(
            SystemProgram.transfer({
                fromPubkey: payerWallet.publicKey,
                toPubkey: recipientPubKey,
                lamports: amountToSend
            })
        );
        console.log(`[${operationId}] Transaction built.`);

        // Send and confirm transaction with timeout
        // sendAndConfirmTransaction uses the solanaConnection and its internal retries/rotation
        const confirmationTimeoutMs = parseInt(process.env.PAYOUT_CONFIRM_TIMEOUT_MS || '45000', 10); // 45s timeout default
        console.log(`[${operationId}] Calling sendAndConfirmTransaction (Timeout: ${confirmationTimeoutMs / 1000}s)...`);

        const signature = await Promise.race([
            sendAndConfirmTransaction(
                solanaConnection, // Use the rate-limited connection
                transaction,
                [payerWallet],    // Signer
                {
                    commitment: 'confirmed',       // Wait for 'confirmed'
                    skipPreflight: false,          // Perform preflight checks
                    maxRetries: parseInt(process.env.SCT_MAX_RETRIES || '3', 10), // Internal retries within sendAndConfirm (for resending/status check)
                    preflightCommitment: 'confirmed', // Match commitment for preflight
                    lastValidBlockHeight: latestBlockhash.lastValidBlockHeight // Provide block height for better confirmation
                }
            ),
            // Timeout for the entire confirmation process
            new Promise((_, reject) => {
                const timeoutId = setTimeout(() => {
                    console.warn(`[${operationId}] Transaction confirmation TIMEOUT after ${confirmationTimeoutMs / 1000}s.`);
                    reject(new Error(`Transaction confirmation timeout after ${confirmationTimeoutMs / 1000}s`));
                }, confirmationTimeoutMs);
                if (timeoutId && typeof timeoutId.unref === 'function') {
                    timeoutId.unref();
                }
            })
        ]);

        // If sendAndConfirmTransaction succeeded
        console.log(`[${operationId}] SUCCESS! ✅ Payout confirmed! Sent ${amountSOL.toFixed(6)} SOL to ${recipientPubKey.toBase58()}. TX Signature: ${signature}`);
        return { success: true, signature };

    } catch (error) {
        // Log failure
        console.error(`[${operationId}] ❌ FAILED. Error:`, error.message);
        // console.error(error.stack); // Optional: log full stack for debugging

        // Extract simulation logs if available
        if (error.logs) {
            console.error(`[${operationId}] Simulation Logs:`);
            error.logs.forEach(log => console.error(`  -> ${log}`));
        }

        // Determine if the error suggests a permanent failure vs. a temporary one
        const errorMsg = error.message.toLowerCase();
        const isPermanent = errorMsg.includes('invalid param') ||
                            errorMsg.includes('insufficient funds') ||
                            errorMsg.includes('account not found') ||
                            errorMsg.includes('incorrect program id') ||
                            errorMsg.includes('custom program error') || // Usually permanent
                            errorMsg.includes('invalid recipient');

        let returnError = error.message;
        if(isPermanent) {
            returnError = `Permanent send error: ${error.message}`;
        } else if (errorMsg.includes('blockhash not found') || errorMsg.includes('transaction confirmation timeout') || errorMsg.includes('timed out')) {
            returnError = `Confirmation failed (timeout/blockhash): ${error.message}`;
        } else {
            // Assume other errors might be temporary RPC issues handled internally or network problems
            returnError = `Send/Confirm error: ${error.message}`;
        }

        return { success: false, error: returnError };
    }
}


// --- Game Processing Logic ---
// (These functions remain structurally the same, relying on the updated payment flow indirectly)

// Routes a paid bet to the correct game handler
async function processPaidBet(bet) {
    console.log(`⚙️ Processing game for bet ${bet.id} (${bet.game_type}, ${bet.memo_id})`);
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
            console.warn(`Bet ${bet.id} (${bet.memo_id}) status is ${statusCheck.rows[0]?.status ?? 'not found'}, not 'payment_verified'. Aborting game processing.`);
            await client.query('ROLLBACK'); // Release lock
            return;
        }

        // Update status to 'processing_game' within the transaction
        await client.query(
            'UPDATE bets SET status = $1 WHERE id = $2',
            ['processing_game', bet.id]
        );
        await client.query('COMMIT'); // Commit status change and release lock
        console.log(`Bet ${bet.id} (${bet.memo_id}) status set to processing_game.`);

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
        console.error(`❌ Error during game processing setup for bet ${bet.id} (${bet.memo_id}):`, error.message);
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
    const { id: betId, user_id, chat_id, bet_details, expected_lamports, memo_id } = bet;
    const choice = bet_details.choice; // 'heads' or 'tails'
    const config = GAME_CONFIG.coinflip;

    // --- Determine Outcome (Simplified & Corrected House Edge Application) ---
    const houseWinRoll = Math.random();
    let result;
    let isHouseWin = houseWinRoll < config.houseEdge;

    if (isHouseWin) {
        // House wins, result is the opposite of the player's choice
        result = (choice === 'heads') ? 'tails' : 'heads';
        console.log(`🪙 Coinflip Bet ${betId} (${memo_id}): House edge triggered.`);
    } else {
        // Fair 50/50 flip
        result = (Math.random() < 0.5) ? 'heads' : 'tails';
    }
    // --- End Outcome Determination ---

    const win = (result === choice) && !isHouseWin; // Ensure house edge win doesn't count as player win

    // Calculate payout amount if win (uses helper function)
    const payoutLamports = win ? calculatePayout(BigInt(expected_lamports), 'coinflip') : 0n;

    // Get user's display name for messages
    let displayName = await getUserDisplayName(chat_id, user_id);

    if (win) {
        const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
        console.log(`🪙 Coinflip Bet ${betId} (${memo_id}): ${displayName} WON ${payoutSOL.toFixed(6)} SOL! Choice: ${choice}, Result: ${result}`);

        // Check if user has a linked wallet
        const winnerAddress = await getLinkedWallet(user_id);
        if (!winnerAddress) {
            console.warn(`Coinflip Bet ${betId} (${memo_id}): Winner ${displayName} has no linked wallet.`);
            await safeSendMessage(chat_id,
                `🎉 ${displayName}, you won the coinflip (Result: *${result}*) but have no wallet linked!\n` +
                `Your payout of ${payoutSOL.toFixed(6)} SOL is waiting. Place another bet (any amount) to link your wallet and receive pending payouts.`,
                { parse_mode: 'Markdown' }
            );
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
            );

            // Update bet status before queuing payout
            const statusUpdated = await updateBetStatus(betId, 'processing_payout');
            if (!statusUpdated) {
                console.error(`[CRITICAL] Bet ${betId} (${memo_id}): Failed to update status to 'processing_payout' before queueing! Payout job may fail.`);
                // Decide how to handle this - maybe don't queue? For now, log critical error.
                 await safeSendMessage(chat_id, `⚠️ Internal error preparing your payout for bet \`${memo_id}\`. Please contact support.`, { parse_mode: 'Markdown' });
                 return; // Stop before queueing payout
            }

            console.log(`Bet ${betId} (${memo_id}): PRE-QUEUE Payout Job. Status updated to processing_payout.`);
            await paymentProcessor.addPaymentJob({
                type: 'payout',
                betId,
                recipient: winnerAddress,
                amount: payoutLamports.toString(), // Pass amount as string
                gameType: 'coinflip',           // Ensure correct gameType
                priority: 2,                    // Higher priority for payouts
                chatId: chat_id,
                displayName,
                // Include context for payout message if needed
                result,                         // Coinflip result
                memoId                          // Include memo ID for reference
            });
            console.log(`Bet ${betId} (${memo_id}): POST-QUEUE Payout Job successfully added.`);
        } catch (e) {
            console.error(`❌ Error preparing/queueing payout info for coinflip bet ${betId} (${memo_id}):`, e);
            await safeSendMessage(chat_id,
                 `⚠️ Error occurred while processing your coinflip win for bet \`${memo_id}\`.\nPlease contact support.`,
                 { parse_mode: 'Markdown' }
            );
            // Attempt to revert status if queuing failed after update, though may be difficult
            await updateBetStatus(betId, 'error_payout_preparation');
        }

    } else { // Loss (including house edge wins)
        console.log(`🪙 Coinflip Bet ${betId} (${memo_id}): ${displayName} LOST. Choice: ${choice}, Result: ${result}`);
        await safeSendMessage(chat_id,
            `❌ ${displayName}, you lost the coinflip!\n` +
            `You guessed *${choice}* but the result was *${result}*. Better luck next time!`,
            { parse_mode: 'Markdown' }
        );
        await updateBetStatus(betId, 'completed_loss');
    }
}

// Handles the Race game logic
async function handleRaceGame(bet) {
    const { id: betId, user_id, chat_id, bet_details, expected_lamports, memo_id } = bet;
    const chosenHorseName = bet_details.horse;
    const config = GAME_CONFIG.race;

    const horses = [
         { name: 'Yellow', emoji: '🟡', odds: 1.1, baseProb: 0.25 }, { name: 'Orange', emoji: '🟠', odds: 2.0, baseProb: 0.20 },
         { name: 'Blue',   emoji: '🔵', odds: 3.0, baseProb: 0.15 }, { name: 'Cyan',   emoji: '💧', odds: 4.0, baseProb: 0.12 },
         { name: 'White',  emoji: '⚪️', odds: 5.0, baseProb: 0.09 }, { name: 'Red',    emoji: '🔴', odds: 6.0, baseProb: 0.07 },
         { name: 'Black',  emoji: '⚫️', odds: 7.0, baseProb: 0.05 }, { name: 'Pink',   emoji: '🌸', odds: 8.0, baseProb: 0.03 },
         { name: 'Purple', emoji: '🟣', odds: 9.0, baseProb: 0.02 }, { name: 'Green',  emoji: '🟢', odds: 10.0, baseProb: 0.01 },
         { name: 'Silver', emoji: '💎', odds: 15.0, baseProb: 0.01 }
    ];
    const totalBaseProb = horses.reduce((sum, h) => sum + h.baseProb, 0);
    if (Math.abs(totalBaseProb - 1.0) > 0.001) console.warn(`Race base probabilities sum to ${totalBaseProb}, not 1.0.`);

    // Determine Winning Horse (including house edge)
    let winningHorse = null;
    const randomNumber = Math.random();
    const targetTotalProb = 1.0 - config.houseEdge; // Total probability allocated to players winning
    let isHouseWin = randomNumber >= targetTotalProb;

    if (!isHouseWin) { // Player wins branch
        const playerWinRoll = randomNumber / targetTotalProb; // Normalize roll within player win space
        let cumulativeBaseProb = 0;
        for (const horse of horses) {
            cumulativeBaseProb += (horse.baseProb / totalBaseProb); // Normalize base prob
            if (playerWinRoll <= cumulativeBaseProb) {
                winningHorse = horse;
                break;
            }
        }
        if (!winningHorse) winningHorse = horses[horses.length - 1]; // Fallback
    } else { // House wins branch
        // Pick a "winning" horse randomly, but player loses regardless of choice
         const houseWinnerIndex = Math.floor(Math.random() * horses.length);
         winningHorse = horses[houseWinnerIndex];
         console.log(`🐎 Race Bet ${betId} (${memo_id}): House edge triggered.`);
    }

    // Send race commentary messages
    let displayName = await getUserDisplayName(chat_id, user_id);
    try {
        await safeSendMessage(chat_id, `🐎 Race starting for bet \`${memo_id}\`! ${displayName} bet on ${chosenHorseName}!`, { parse_mode: 'Markdown' });
        await new Promise(resolve => setTimeout(resolve, 2000)); // Pause
        await safeSendMessage(chat_id, "🚦 And they're off!");
        await new Promise(resolve => setTimeout(resolve, 3000)); // Pause
        await safeSendMessage(chat_id,
            `🏆 The winner is... ${winningHorse.emoji} *${winningHorse.name}*! 🏆`,
            { parse_mode: 'Markdown' }
        );
    } catch (e) {
        console.error(`Error sending race commentary for bet ${betId} (${memo_id}):`, e);
    }

    // Determine win/loss and payout
    const win = !isHouseWin && (chosenHorseName.toLowerCase() === winningHorse.name.toLowerCase());
    const payoutLamports = win
        ? calculatePayout(BigInt(expected_lamports), 'race', winningHorse) // Pass winning horse for odds
        : 0n;

    if (win) {
        const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
        console.log(`🐎 Race Bet ${betId} (${memo_id}): ${displayName} WON ${payoutSOL.toFixed(6)} SOL! Horse: ${chosenHorseName}, Winner: ${winningHorse.name}`);

        const winnerAddress = await getLinkedWallet(user_id);
        if (!winnerAddress) {
            console.warn(`Race Bet ${betId} (${memo_id}): Winner ${displayName} has no linked wallet.`);
            await safeSendMessage(chat_id,
                `🎉 ${displayName}, your horse *${chosenHorseName}* won the race!\n` +
                `Your payout of ${payoutSOL.toFixed(6)} SOL is waiting. Place another bet (any amount) to link your wallet and receive pending payouts.`,
                { parse_mode: 'Markdown' }
            );
            await updateBetStatus(betId, 'completed_win_no_wallet');
            return;
        }

        try {
            await safeSendMessage(chat_id,
                `🎉 ${displayName}, your horse *${chosenHorseName}* won!\n` +
                `Payout: ${payoutSOL.toFixed(6)} SOL\n\n` +
                `💸 Processing payout to your linked wallet...`,
                { parse_mode: 'Markdown' }
            );

            const statusUpdated = await updateBetStatus(betId, 'processing_payout');
             if (!statusUpdated) {
                 console.error(`[CRITICAL] Bet ${betId} (${memo_id}): Failed to update status to 'processing_payout' before queueing! Payout job may fail.`);
                 await safeSendMessage(chat_id, `⚠️ Internal error preparing your payout for bet \`${memo_id}\`. Please contact support.`, { parse_mode: 'Markdown' });
                 return; // Stop before queueing payout
             }
             console.log(`Bet ${betId} (${memo_id}): PRE-QUEUE Payout Job. Status updated to processing_payout.`);

            await paymentProcessor.addPaymentJob({
                type: 'payout',
                betId,
                recipient: winnerAddress,
                amount: payoutLamports.toString(),
                gameType: 'race',
                priority: 2,
                chatId: chat_id,
                displayName,
                 // Include context for payout message if needed
                horseName: chosenHorseName,
                winningHorseName: winningHorse.name,
                winningHorseEmoji: winningHorse.emoji,
                memoId
            });
            console.log(`Bet ${betId} (${memo_id}): POST-QUEUE Payout Job successfully added.`);
        } catch (e) {
            console.error(`❌ Error queuing payout for race bet ${betId} (${memo_id}):`, e);
            await safeSendMessage(chat_id,
                `⚠️ Error occurred while processing your race win for bet \`${memo_id}\`.\nPlease contact support.`,
                { parse_mode: 'Markdown' }
            );
            await updateBetStatus(betId, 'error_payout_queueing');
        }

    } else { // Loss (including house edge wins)
        console.log(`🐎 Race Bet ${betId} (${memo_id}): ${displayName} LOST. Choice: ${chosenHorseName}, Winner: ${winningHorse.name}`);
        await safeSendMessage(chat_id,
            `❌ ${displayName}, your horse *${chosenHorseName}* lost the race!\n` +
            `Winner: ${winningHorse.emoji} *${winningHorse.name}*. Better luck next time!`,
            { parse_mode: 'Markdown' }
        );
        await updateBetStatus(betId, 'completed_loss');
    }
}


// Handles the actual payout transaction after a win is confirmed
async function handlePayoutJob(job) {
    const { betId, recipient, amount, gameType, chatId, displayName, memoId } = job; // Added memoId
    console.log(`[PAYOUT_JOB] START - Processing job for Bet ID: ${betId} (${memoId}), Recipient: ${recipient}, Amount: ${amount}`);

    const payoutAmountLamports = BigInt(amount);

    if (payoutAmountLamports <= 0n) {
        console.error(`❌ Payout job for bet ${betId} (${memoId}) has zero or negative amount (${payoutAmountLamports}). Skipping send.`);
        await updateBetStatus(betId, 'error_payout_zero_amount');
        await safeSendMessage(chatId, `⚠️ There was an issue calculating the payout for bet \`${memoId}\` (amount was zero). Please contact support.`, { parse_mode: 'Markdown' });
        return;
    }

    console.log(`💸 Processing payout job for Bet ID: ${betId} (${memoId}), Amount: ${payoutAmountLamports} lamports to ${recipient}`);

    try {
        // Attempt to send the SOL payout using the updated sendSol function
        const sendResult = await sendSol(recipient, payoutAmountLamports, gameType);
        console.log(`[PAYOUT_JOB] sendSol completed for Bet ID: ${betId} (${memoId}). Success: ${sendResult.success}`);

        if (sendResult.success && sendResult.signature) {
            console.log(`💸 Payout successful for bet ${betId} (${memoId}), TX: ${sendResult.signature}`);
            // Record payout first before notifying user
            const recorded = await recordPayout(betId, 'completed_win_paid', sendResult.signature);
            if(recorded) {
                await safeSendMessage(chatId,
                    `✅ Payout successful for bet \`${memoId}\`!\n` +
                    `${(Number(payoutAmountLamports)/LAMPORTS_PER_SOL).toFixed(6)} SOL sent.\n` +
                    `TX: \`https://solscan.io/tx/${sendResult.signature}\``,
                    { parse_mode: 'Markdown', disable_web_page_preview: true }
                );
            } else {
                 console.error(`[CRITICAL] Payout for bet ${betId} (${memoId}) SUCCEEDED on chain (TX: ${sendResult.signature}) but FAILED to record in DB! Requires manual investigation.`);
                 await safeSendMessage(chatId,
                    `⚠️ Your payout for bet \`${memoId}\` was sent successfully, but there was an issue recording it.\n` +
                    `TX: \`https://solscan.io/tx/${sendResult.signature}\`\n` +
                    `Please contact support if you have issues.`,
                    { parse_mode: 'Markdown', disable_web_page_preview: true }
                 );
                 // Leave status as 'processing_payout' or set specific error? Set specific error.
                 await updateBetStatus(betId, 'error_payout_record_failed');
            }

        } else {
            // Payout failed
            console.error(`❌ Payout failed permanently for bet ${betId} (${memoId}): ${sendResult.error}`);
            await updateBetStatus(betId, 'error_payout_failed'); // Update status first
            await safeSendMessage(chatId,
                `⚠️ Payout failed for bet \`${memoId}\`: ${sendResult.error}\n` +
                `The team has been notified. Please contact support if needed.`,
                { parse_mode: 'Markdown' }
            );
            // TODO: Implement admin notification for failed payouts
        }
    } catch (error) {
        // Catch unexpected errors during payout processing itself (not sendSol errors)
        console.error(`❌ Unexpected error processing payout job for bet ${betId} (${memoId}):`, error);
        await updateBetStatus(betId, 'error_payout_exception');
        await safeSendMessage(chatId,
            `⚠️ A technical error occurred during payout for bet \`${memoId}\`.\nPlease contact support.`,
            { parse_mode: 'Markdown' }
        );
    }
}

// --- Utility Functions ---

// Calculates payout amount, applying house edge. Returns BigInt lamports.
function calculatePayout(betLamports, gameType, gameDetails = {}) {
    betLamports = BigInt(betLamports);
    let payoutLamports = 0n;

    if (gameType === 'coinflip') {
        // Payout = Bet * (2 - HouseEdge) = Bet * (1 + (1 - HouseEdge))
        // Example: Bet 1 SOL, Edge 0.02. Payout = 1 * (2 - 0.02) = 1.98 SOL (includes stake)
        const multiplier = 2.0 - GAME_CONFIG.coinflip.houseEdge;
        payoutLamports = BigInt(Math.floor(Number(betLamports) * multiplier));
    } else if (gameType === 'race' && gameDetails.odds) {
        // Payout = Bet * Odds * (1 - House Edge)
        // Example: Bet 1 SOL, Odds 5.0, Edge 0.02. Payout = 1 * 5.0 * (1 - 0.02) = 4.9 SOL (includes stake)
        const multiplier = gameDetails.odds * (1.0 - GAME_CONFIG.race.houseEdge);
        payoutLamports = BigInt(Math.floor(Number(betLamports) * multiplier));
    } else {
        console.error(`Cannot calculate payout: Unknown game type "${gameType}" or missing race odds.`);
        return 0n; // Return 0 if calculation fails
    }

    // Ensure payout is not negative (shouldn't happen with positive inputs/multipliers)
    return payoutLamports > 0n ? payoutLamports : 0n;
}

// Fetches user's display name (username or first name) from Telegram
async function getUserDisplayName(chat_id, user_id) {
    try {
        // Use caching for chat members to reduce API calls
        const memberCacheKey = `member-${chat_id}-${user_id}`;
        // let cachedMember = memberCache.get(memberCacheKey); // Requires defining memberCache
        // if (!cachedMember) { ... } // Implement cache logic if needed

        const chatMember = await bot.getChatMember(chat_id, user_id);
        const user = chatMember.user;
        const name = user.username ? `@${user.username}` : (user.first_name || `User ${String(user_id).substring(0, 6)}...`);

        // Basic sanitization for Markdown V2
        const escapeMarkdown = (text) => {
            // Escape characters: _ * [ ] ( ) ~ ` > # + - = | { } . !
             return text.replace(/([_*\[\]()~`>#+\-=|{}.!])/g, '\\$1');
        }
         return escapeMarkdown(name); // Escape the chosen name

    } catch (e) {
        if (e.response && (e.response.statusCode === 400 || e.response.statusCode === 403) && e.response.body?.description?.includes('user not found')) {
             console.warn(`User ${user_id} not found in chat ${chat_id}.`);
        } else if (e.response && e.response.statusCode === 403) {
             console.warn(`Permission error getting user ${user_id} in chat ${chat_id}: ${e.message}`);
        }
        else {
             console.warn(`Couldn't get username/name for user ${user_id} in chat ${chat_id}:`, e.message);
        }
        return `User ${String(user_id).substring(0, 6)}...`; // Fallback
    }
}


// --- Telegram Bot Command Handlers ---

// Handles polling errors
bot.on('polling_error', (error) => {
    console.error(`❌ Polling error: ${error.code} - ${error.message}`);
    if (error.code === 'ETELEGRAM' && error.message.includes('409 Conflict')) {
        console.error("❌❌❌ FATAL: Conflict detected! Another bot instance might be running with the same token. Exiting.");
        console.error("❌ Exiting due to conflict."); process.exit(1);
    } else if (error.code === 'ECONNRESET') {
         console.warn("⚠️ Polling connection reset. Attempting to continue...");
    }
    // Add more specific handling if needed
});

// Handles webhook errors (e.g., connection issues to Telegram endpoint)
bot.on('webhook_error', (error) => {
     console.error(`❌ Webhook error: ${error.code} - ${error.message}`);
     // Consider attempting to switch to polling if webhook fails persistently
});

// Handles general bot errors
bot.on('error', (error) => {
    console.error('❌ General Bot Error:', error);
    performanceMonitor.logRequest(false); // Log as failed request
});


// Central handler for all incoming messages (routed by command)
async function handleMessage(msg) {
    // Basic filtering moved to the 'on message' handler

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const messageText = msg.text;
    const messageId = msg.message_id;

    try {
        // --- User Cooldown Check ---
        const now = Date.now();
        if (confirmCooldown.has(userId)) {
            const lastTime = confirmCooldown.get(userId);
            if (now - lastTime < cooldownInterval) {
                // console.log(`User ${userId} command ignored due to cooldown.`); // Optional debug log
                return; // Ignore command if user is on cooldown
            }
        }
        // Apply cooldown only for actual commands
        if (messageText.startsWith('/')) {
            confirmCooldown.set(userId, now); // Update last command time
        }

        // --- Command Routing ---
        const text = messageText.trim();
        const commandMatch = text.match(/^\/(\w+)(?:@\w+)?(?:\s+(.*))?$/s); // Extracts command and arguments, 's' flag for multiline args

        if (!commandMatch) return; // Not a command format

        const command = commandMatch[1].toLowerCase();
        const args = commandMatch[2]?.trim() || ''; // Arguments string, trimmed

        // Log received command for debugging
        // console.log(`Cmd: /${command} | User:${userId} | Chat:${chatId} | Args:"${args}"`);

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
             // Add admin commands if needed
             case 'botstats':
                 if (process.env.ADMIN_USER_IDS && process.env.ADMIN_USER_IDS.split(',').includes(userId)) {
                     await handleBotStatsCommand(msg);
                 } else {
                      console.log(`User ${userId} attempted unauthorized /botstats command.`);
                 }
                 break;
            default:
                 break; // Ignore unknown commands silently
        }

        performanceMonitor.logRequest(true); // Log successful command handling attempt

    } catch (error) {
        console.error(`❌ Error processing msg ${messageId} from user ${userId} in chat ${chatId}: "${messageText}"`, error);
        performanceMonitor.logRequest(false); // Log error
        try {
            // Use safeSendMessage for error reporting
            await safeSendMessage(chatId, "⚠️ An unexpected error occurred processing your command. Please try again later or contact support.");
        } catch (tgError) {
            // safeSendMessage already logs errors, no need to log again
        }
    } finally {
        // Optional: Periodic cleanup of cooldown map (simple approach)
        if (Math.random() < 0.1) { // Run cleanup occasionally
             const cutoff = Date.now() - 60000; // Remove entries older than 1 minute
             let deletedCount = 0;
             for (const [key, timestamp] of confirmCooldown.entries()) {
                 if (timestamp < cutoff) {
                     confirmCooldown.delete(key);
                     deletedCount++;
                 }
             }
             // if (deletedCount > 0) console.log(`[Cooldown Cleanup] Removed ${deletedCount} expired entries.`);
        }
    }
}

// Handles the /start command
async function handleStartCommand(msg) {
    const firstName = msg.from.first_name || 'there';
    const sanitizedFirstName = firstName.replace(/</g, '&lt;').replace(/>/g, '&gt;'); // Basic HTML escape
    const welcomeText = `👋 Welcome, <b>${sanitizedFirstName}</b>!\n\n` +
                        `🎰 <b>Solana Gambles Bot</b>\n\n` +
                        `Use /coinflip or /race to see game options.\n` +
                        `Use /wallet to view your linked Solana wallet.\n` +
                        `Use /help to see all commands.\n\n` +
                        `<i>Remember to gamble responsibly!</i>`;
    const bannerUrl = 'https://i.ibb.co/9vDo58q/banner.gif'; // Keep banner URL

    try {
        // Try sending animation first
        await bot.sendAnimation(msg.chat.id, bannerUrl, {
            caption: welcomeText,
            parse_mode: 'HTML' // Use HTML for the caption
        }).catch(async (animError) => {
             // If animation fails, send text fallback using safeSendMessage
             console.warn("⚠️ Failed to send start animation, sending text fallback:", animError.message);
             await safeSendMessage(msg.chat.id, welcomeText, { parse_mode: 'HTML' });
        });
    } catch (fallbackError) {
         // Catch errors from the fallback send itself (should be caught by safeSendMessage now)
         // console.error("TG Send Error (start fallback):", fallbackError.message);
    }
}


// Handles the /coinflip command (shows instructions) - Uses HTML formatting
async function handleCoinflipCommand(msg) {
    try {
        const config = GAME_CONFIG.coinflip;
        const payoutMultiplier = (2.0 * (1.0 - config.houseEdge)).toFixed(2);

        // Using HTML tags (<b> for bold, <code> for inline code, <i> for italics)
        const messageText = `🪙 <b>Coinflip Game</b> 🪙\n\n` +
            `Bet on Heads or Tails! Simple and quick.\n\n` +
            `<b>How to play:</b>\n` +
            `1. Type <code>/bet amount heads</code> (e.g., <code>/bet 0.1 heads</code>)\n` +
            `2. Type <code>/bet amount tails</code> (e.g., <code>/bet 0.1 tails</code>)\n\n` +
            `<b>Rules:</b>\n` +
            `- Min Bet: ${config.minBet} SOL\n` +
            `- Max Bet: ${config.maxBet} SOL\n` +
            `- House Edge: ${(config.houseEdge * 100).toFixed(1)}%\n` +
            `- Payout: ~<b>${payoutMultiplier}x</b> <i>(Win Amount = Bet * ${payoutMultiplier})</i>\n\n` +
            `You will be given a wallet address and a <b>unique Memo ID</b>. Send the <b>exact</b> SOL amount with the memo to place your bet.\n\n`+
            `<i>Good luck!</i>`;

        await safeSendMessage(msg.chat.id, messageText, { parse_mode: 'HTML' });
    } catch (error) {
        console.error("Error INSIDE handleCoinflipCommand:", error);
        await safeSendMessage(msg.chat.id, "Sorry, couldn't display Coinflip info right now.");
    }
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

     // Using MarkdownV2 requires escaping special characters: _ * [ ] ( ) ~ ` > # + - = | { } . !
     const escapeMarkdown = (text) => text.replace(/([_*\[\]()~`>#+\-=|{}.!])/g, '\\$1');

     let raceMessage = `🐎 *Horse Race Game* 🐎\\n\\nBet on the winning horse\\!\\n\\n*Available Horses \\& Approx Payout* \\(Bet x Odds After House Edge\\):\\n`;
     horses.forEach(horse => {
         const effectiveMultiplier = (horse.odds * (1.0 - GAME_CONFIG.race.houseEdge)).toFixed(2);
         raceMessage += `\\- ${horse.emoji} *${escapeMarkdown(horse.name)}* \\(\\~${escapeMarkdown(effectiveMultiplier)}x Payout\\)\\n`;
     });

     const config = GAME_CONFIG.race;
     raceMessage += `\\n*How to play:*\\n` +
                       `1\\. Type \`/betrace amount horse\\_name\`\\n` +
                       `   \\(e\\.g\\., \`/betrace 0.1 Yellow\`\\)\\n\\n` +
                       `*Rules:*\\n` +
                       `\\- Min Bet: ${escapeMarkdown(config.minBet.toString())} SOL\\n` +
                       `\\- Max Bet: ${escapeMarkdown(config.maxBet.toString())} SOL\\n` +
                       `\\- House Edge: ${escapeMarkdown((config.houseEdge * 100).toFixed(1))}% \\(applied to winnings\\)\\n\\n` +
                       `You will be given a wallet address and a *unique Memo ID*\\. Send the *exact* SOL amount with the memo to place your bet\\.`;

     await safeSendMessage(msg.chat.id, raceMessage, { parse_mode: 'MarkdownV2' });
}


// Handles the /wallet command (shows linked wallet)
async function handleWalletCommand(msg) {
    const userId = String(msg.from.id);
    const walletAddress = await getLinkedWallet(userId); // Uses cache

    if (walletAddress) {
        await safeSendMessage(msg.chat.id,
            `🔗 Your linked Solana wallet:\n\`${walletAddress}\`\n\n`+ // Use backticks for code block in Markdown
            `Payouts will be sent here\\. It's linked automatically when you make your first paid bet\\.`,
            { parse_mode: 'MarkdownV2' } // Ensure MarkdownV2 is used if escaping characters
        );
    } else {
        await safeSendMessage(msg.chat.id,
            `🔗 No wallet linked yet\\.\n` +
            `Place a bet and send the required SOL\\. Your sending wallet will be automatically linked for future payouts\\.`,
             { parse_mode: 'MarkdownV2' }
        );
    }
}

// Handles /bet command (for Coinflip) - Updated to use args
async function handleBetCommand(msg, args) {
     // Use regex to capture amount and choice, ignoring case for choice
     const match = args.trim().match(/^(\d+\.?\d*)\s+(heads|tails)/i);
     if (!match) {
         await safeSendMessage(msg.chat.id,
             `⚠️ Invalid format\\. Use: \`/bet <amount> <heads|tails>\`\n` +
             `Example: \`/bet 0.1 heads\``,
             { parse_mode: 'MarkdownV2' }
         );
         return;
     }

     const userId = String(msg.from.id);
     const chatId = String(msg.chat.id);
     const config = GAME_CONFIG.coinflip;
     const escapeMarkdown = (text) => text.replace(/([_*\[\]()~`>#+\-=|{}.!])/g, '\\$1'); // For MarkdownV2

     // Validate bet amount
     const betAmount = parseFloat(match[1]);
     if (isNaN(betAmount) || betAmount < config.minBet || betAmount > config.maxBet) {
         await safeSendMessage(chatId,
             `⚠️ Invalid bet amount\\. Please bet between ${escapeMarkdown(config.minBet.toString())} and ${escapeMarkdown(config.maxBet.toString())} SOL\\.`,
             { parse_mode: 'MarkdownV2' }
         );
         return;
     }

     const userChoice = match[2].toLowerCase();
     const memoId = generateMemoId('CF'); // Generates V1 format
     const expectedLamports = BigInt(Math.round(betAmount * LAMPORTS_PER_SOL));
     const expiresAt = new Date(Date.now() + config.expiryMinutes * 60 * 1000);

     // Save the pending bet
     const saveResult = await savePendingBet(
         userId, chatId, 'coinflip',
         { choice: userChoice },
         expectedLamports, memoId, expiresAt
     );

     if (!saveResult.success) {
         await safeSendMessage(chatId, `⚠️ Error registering bet: ${escapeMarkdown(saveResult.error || 'Unknown error')}\\. Please try the command again\\.`, { parse_mode: 'MarkdownV2' });
         return;
     }

     // Format amount precisely
        const betAmountString = betAmount.toFixed(Math.max(2, (betAmount.toString().split('.')[1] || '').length));
        // Escape necessary characters for MarkdownV2

        // --- CORRECTED MESSAGE STRING ---
        const message = `✅ Coinflip bet registered\\! \\(ID: \`${memoId}\`\\)\n\n` + // Removed \\ before \n
                        `You chose: *${escapeMarkdown(userChoice)}*\n` + // Removed \\ before \n
                        `Amount: *${escapeMarkdown(betAmountString)} SOL*\n\n` + // Removed \\ before \n\n
                        `➡️ Send *exactly ${escapeMarkdown(betAmountString)} SOL* to:\n` + // Removed \\ before \n
                        `\`${escapeMarkdown(process.env.MAIN_WALLET_ADDRESS)}\`\n\n` + // Removed \\ before \n\n
                        `📎 *Include MEMO:* \`${memoId}\`\n\n` + // Removed \\ before \n\n (memoId is safe)
                        `⏱️ This request expires in ${config.expiryMinutes} minutes\\.\n\n` + // Removed \\ before \n\n (Escaped .)
                        `*IMPORTANT:* Send from your own wallet \\(not an exchange\\)\\. Ensure you include the memo correctly\\.`; // Escaped () and .

        await safeSendMessage(chatId, message, { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}


// Handles /betrace command - Updated to use args
async function handleBetRaceCommand(msg, args) {
    const match = args.trim().match(/^(\d+\.?\d*)\s+(\w+)/i);
    if (!match) {
        await safeSendMessage(msg.chat.id,
            `⚠️ Invalid format\\. Use: \`/betrace <amount> <horse\\_name>\`\\n`+
            `Example: \`/betrace 0.1 Yellow\``,
            { parse_mode: 'MarkdownV2' }
        );
        return;
    }

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const config = GAME_CONFIG.race;
    const escapeMarkdown = (text) => text.replace(/([_*\[\]()~`>#+\-=|{}.!])/g, '\\$1');

    // Validate bet amount
    const betAmount = parseFloat(match[1]);
    if (isNaN(betAmount) || betAmount < config.minBet || betAmount > config.maxBet) {
        await safeSendMessage(chatId,
            `⚠️ Invalid bet amount\\. Please bet between ${escapeMarkdown(config.minBet.toString())} and ${escapeMarkdown(config.maxBet.toString())} SOL\\.`,
            { parse_mode: 'MarkdownV2' }
        );
        return;
    }

    // Validate horse selection
    const chosenHorseNameInput = match[2];
    const horses = [ // Define horses locally for validation
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
            `⚠️ Invalid horse name: "${escapeMarkdown(chosenHorseNameInput)}"\\. Please choose from the list in /race\\.`, { parse_mode: 'MarkdownV2' }
        );
        return;
    }

    const memoId = generateMemoId('RA'); // Generates V1 format
    const expectedLamports = BigInt(Math.round(betAmount * LAMPORTS_PER_SOL));
    const expiresAt = new Date(Date.now() + config.expiryMinutes * 60 * 1000);

    // Save the pending bet
    const saveResult = await savePendingBet(
        userId, chatId, 'race',
        { horse: chosenHorse.name, odds: chosenHorse.odds }, // Store chosen horse name and odds
        expectedLamports, memoId, expiresAt
    );

    if (!saveResult.success) {
        await safeSendMessage(chatId, `⚠️ Error registering bet: ${escapeMarkdown(saveResult.error || 'Unknown error')}\\. Please try the command again\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    // Calculate potential payout for display
        const potentialPayoutLamports = calculatePayout(expectedLamports, 'race', chosenHorse);
        // Escape necessary characters for MarkdownV2
        const potentialPayoutSOL = escapeMarkdown((Number(potentialPayoutLamports) / LAMPORTS_PER_SOL).toFixed(6));
        const betAmountString = escapeMarkdown(betAmount.toFixed(Math.max(2, (betAmount.toString().split('.')[1] || '').length)));


        // --- CORRECTED MESSAGE STRING ---
        const message = `✅ Race bet registered\\! \\(ID: \`${memoId}\`\\)\n\n` + // Removed \\ before \n
                        `You chose: ${chosenHorse.emoji} *${escapeMarkdown(chosenHorse.name)}*\n` + // Removed \\ before \n
                        `Amount: *${betAmountString} SOL*\n` + // Removed \\ before \n
                        `Potential Payout: \\~${potentialPayoutSOL} SOL\n\n`+ // Removed \\ before \n\n (Keep \~ escaped)
                        `➡️ Send *exactly ${betAmountString} SOL* to:\n` + // Removed \\ before \n
                        `\`${escapeMarkdown(process.env.RACE_WALLET_ADDRESS)}\`\n\n` + // Removed \\ before \n\n
                        `📎 *Include MEMO:* \`${memoId}\`\n\n` + // Removed \\ before \n\n (memoId is safe)
                        `⏱️ This request expires in ${config.expiryMinutes} minutes\\.\n\n` + // Removed \\ before \n\n (Escaped .)
                        `*IMPORTANT:* Send from your own wallet \\(not an exchange\\)\\. Ensure you include the memo correctly\\.`; // Escaped () and .

        await safeSendMessage(chatId, message, { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}

// Handles /help command
async function handleHelpCommand(msg) {
     // Using MarkdownV2 requires escaping special characters
     const helpText = `*Solana Gambles Bot Commands* 🎰\\n\\n` +
                      `/start \\- Show welcome message\\n` +
                      `/help \\- Show this help message\\n\\n` +
                      `*Games:*\\n` +
                      `/coinflip \\- Show Coinflip game info \\& how to bet\\n` +
                      `/race \\- Show Horse Race game info \\& how to bet\\n\\n` +
                      `*Betting:*\\n` +
                      `/bet <amount> <heads|tails> \\- Place a Coinflip bet\\n` +
                      `/betrace <amount> <horse\\_name> \\- Place a Race bet\\n\\n` +
                      `*Wallet:*\\n` +
                      `/wallet \\- View your linked Solana wallet for payouts\\n\\n` +
                      `*Support:* If you encounter issues, please contact support\\.`; // Replace placeholder

     await safeSendMessage(msg.chat.id, helpText, { parse_mode: 'MarkdownV2' });
}

// Example Admin Command: /botstats
async function handleBotStatsCommand(msg) {
     const processor = paymentProcessor;
     const connectionStats = typeof solanaConnection.getRequestStats === 'function' ? solanaConnection.getRequestStats() : null;
     const escapeMarkdown = (text) => String(text).replace(/([_*\[\]()~`>#+\-=|{}.!])/g, '\\$1'); // Ensure input is string

     let statsMsg = `*Bot Statistics* \\(v2\\.1\\-multi\\-rpc\\)\\n\\n`;
     statsMsg += `*Uptime:* ${escapeMarkdown(Math.floor(process.uptime() / 60))} minutes\\n`;
     statsMsg += `*Performance:* Req:${performanceMonitor.requests}, Err:${performanceMonitor.errors}\\n`;
     statsMsg += `*Queues:*\\n`;
     statsMsg += `  \\- Msg: P:${messageQueue.size} A:${messageQueue.pending}\\n`;
     statsMsg += `  \\- TG Send: P:${telegramSendQueue.size} A:${telegramSendQueue.pending}\\n`;
     statsMsg += `  \\- Pay High: P:${processor?.highPriorityQueue?.size || 0} A:${processor?.highPriorityQueue?.pending || 0}\\n`;
     statsMsg += `  \\- Pay Norm: P:${processor?.normalQueue?.size || 0} A:${processor?.normalQueue?.pending || 0}\\n`;
     statsMsg += `*Caches:*\\n`;
     statsMsg += `  \\- Wallets: ${walletCache.size}\\n`;
     statsMsg += `  \\- Processed Sigs: ${processedSignaturesThisSession.size} / ${MAX_PROCESSED_SIGNATURES}\\n`;

     if (connectionStats) {
         statsMsg += `*Solana Connection:*\\n`;
         statsMsg += `  \\- Endpoint: ${escapeMarkdown(connectionStats.status.currentEndpoint)} \\(Idx ${connectionStats.status.currentEndpointIndex}\\)\\n`;
         statsMsg += `  \\- Q:${connectionStats.status.queueSize}, A:${connectionStats.status.activeRequests}\\n`;
         statsMsg += `  \\- Consecutive RL: ${connectionStats.status.consecutiveRateLimits}\\n`;
         statsMsg += `  \\- Last RL: ${escapeMarkdown(connectionStats.status.lastRateLimit)}\\n`;
         statsMsg += `  \\- Total Req: S:${connectionStats.stats.totalRequestsSucceeded}, F:${connectionStats.stats.totalRequestsFailed}\\n`;
         statsMsg += `  \\- RL Events: ${connectionStats.stats.rateLimitEvents}\\n`;
         statsMsg += `  \\- Rotations: ${connectionStats.stats.endpointRotations}\\n`;
         statsMsg += `  \\- Success Rate: ${escapeMarkdown(connectionStats.stats.successRate)}\\n`;
     } else {
         statsMsg += `*Solana Connection:* Stats unavailable\\.\n`;
     }

     await safeSendMessage(msg.chat.id, statsMsg, { parse_mode: 'MarkdownV2' });
}


// --- Server Startup & Shutdown Logic ---

// Encapsulated Webhook Setup Logic
async function setupTelegramWebhook() {
    if (process.env.RAILWAY_ENVIRONMENT && process.env.RAILWAY_PUBLIC_DOMAIN) {
        const webhookUrl = `https://${process.env.RAILWAY_PUBLIC_DOMAIN}${webhookPath}`;
        console.log(`Attempting to set webhook to: ${webhookUrl}`);
        let attempts = 0;
        const maxAttempts = 3;
        while (attempts < maxAttempts) {
            try {
                await bot.deleteWebHook({ drop_pending_updates: true });
                await bot.setWebHook(webhookUrl, {
                     max_connections: parseInt(process.env.WEBHOOK_MAX_CONN || '5', 10), // Configurable max connections
                     allowed_updates: ["message", "callback_query"] // Only process needed updates
                     });
                console.log(`✅ Webhook successfully set to: ${webhookUrl}`);
                return true; // Indicate webhook was set
            } catch (webhookError) {
                attempts++;
                console.error(`❌ Webhook setup attempt ${attempts}/${maxAttempts} failed:`, webhookError.message);
                 if (webhookError.code === 'ETELEGRAM' && webhookError.message.includes('400 Bad Request: URL host is empty')) {
                     console.error("❌❌❌ Webhook URL seems invalid. Check RAILWAY_PUBLIC_DOMAIN.");
                     return false; // Don't retry if URL is fundamentally broken
                 }
                if (attempts >= maxAttempts) {
                    console.error("❌ Max webhook setup attempts reached. Continuing without webhook.");
                    return false; // Indicate webhook failed
                }
                await new Promise(resolve => setTimeout(resolve, 3000 * attempts)); // Exponential backoff
            }
        }
    } else {
        console.log("ℹ️ Not in Railway environment or domain not set, webhook not configured.");
        return false; // Indicate webhook was not set
    }
    return false; // Default return
}

// Encapsulated Polling Setup Logic
async function startPollingIfNeeded() {
    try {
        const info = await bot.getWebHookInfo();
        if (!info || !info.url) { // Start polling only if webhook is not set
             if (bot.isPolling()) {
                 console.log("ℹ️ Bot is already polling.");
                 return;
             }
            console.log("ℹ️ Webhook not set, starting bot polling...");
            await bot.deleteWebHook({ drop_pending_updates: true }); // Ensure no residual webhook
            await bot.startPolling({ polling: { interval: parseInt(process.env.POLLING_INTERVAL_MS || '300', 10) } });
            console.log("✅ Bot polling started successfully");
        } else {
            console.log(`ℹ️ Webhook is set (${info.url}), polling will not be started.`);
             if (bot.isPolling()) {
                 console.log("ℹ️ Stopping polling because webhook is set.");
                 await bot.stopPolling({ cancel: true });
             }
        }
    } catch (err) {
        console.error("❌ Error managing polling state:", err.message);
        if (err.code === 'ETELEGRAM' && err.message.includes('409 Conflict')) {
             console.error("❌❌❌ Conflict detected! Another instance might be polling. Exiting.");
             console.error("❌ Exiting due to conflict."); process.exit(1);
        }
        // Handle other potential errors
    }
}

// Encapsulated Payment Monitor Start Logic
function startPaymentMonitor() {
    if (monitorInterval) {
         console.log("ℹ️ Payment monitor already running.");
         return; // Prevent multiple intervals
    }
     const monitorIntervalSeconds = parseInt(process.env.MONITOR_INTERVAL_SECONDS || '45', 10); // Default 45s
     console.log(`⚙️ Starting payment monitor (Interval: ${monitorIntervalSeconds}s)`);
     monitorInterval = setInterval(() => {
          try {
               monitorPayments().catch(err => {
                    console.error('❌ [FATAL MONITOR ERROR in setInterval catch]:', err);
                    performanceMonitor.logRequest(false);
               });
          } catch (syncErr) {
               console.error('❌ [FATAL MONITOR SYNC ERROR in setInterval try/catch]:', syncErr);
               performanceMonitor.logRequest(false);
          }
     }, monitorIntervalSeconds * 1000);

    // Run monitor once shortly after initialization completes
    setTimeout(() => {
        console.log("⚙️ Performing initial payment monitor run...");
        try {
             monitorPayments().catch(err => {
                  console.error('❌ [FATAL MONITOR ERROR in initial run catch]:', err);
                  performanceMonitor.logRequest(false);
             });
        } catch (syncErr) {
             console.error('❌ [FATAL MONITOR SYNC ERROR in initial run try/catch]:', syncErr);
             performanceMonitor.logRequest(false);
        }
    }, parseInt(process.env.MONITOR_INITIAL_DELAY_MS || '5000', 10)); // 5s default delay
}


// --- Graceful shutdown handler (Modified for Railway Rotations) ---
const shutdown = async (signal, isRailwayRotation = false) => {
    console.log(`\n🛑 ${signal} received, ${isRailwayRotation ? 'container rotation' : 'shutting down gracefully'}...`);

    if (isRailwayRotation) {
        // For Railway rotations, just stop accepting new connections quickly
        console.log("Railway container rotation detected - minimizing disruption.");
        if (server) {
            server.close(() => console.log("- Stopped accepting new server connections for rotation."));
        }
        // Don't explicitly exit process here, Railway handles it.
        return;
    }

    // --- START: Full Shutdown Logic (for non-rotation signals like SIGINT or manual stop) ---
    console.log("Performing full graceful shutdown sequence...");
    isMonitorRunning = true; // Prevent monitor from starting new cycle during shutdown

    // 1. Stop receiving new events/requests
    console.log("Stopping incoming connections and tasks...");
    if (monitorInterval) {
        clearInterval(monitorInterval);
        console.log("- Stopped payment monitor interval.");
    }
    try {
        if (server) {
             await new Promise((resolve, reject) => {
                 server.close((err) => {
                     if (err) {
                         console.error("⚠️ Error closing Express server:", err.message);
                         return reject(err);
                     }
                     console.log("- Express server closed.");
                     resolve(undefined);
                 });
                 setTimeout(() => reject(new Error('Server close timeout')), 5000).unref();
             });
        }

        try {
            const webhookInfo = await bot.getWebHookInfo();
            if (webhookInfo && webhookInfo.url) {
                await bot.deleteWebHook({ drop_pending_updates: false }); // Don't drop updates, let queues handle them
                console.log("- Removed Telegram webhook.");
            }
        } catch (whErr) {
            console.error("⚠️ Error removing webhook during shutdown:", whErr.message);
        }

        if (bot.isPolling()) {
            await bot.stopPolling({ cancel: false }); // Don't cancel, let queues handle pending
            console.log("- Stopped Telegram polling.");
        }
    } catch (e) {
        console.error("⚠️ Error stopping listeners/server:", e.message);
    }

    // 2. Wait for ongoing queue processing to finish (with timeout)
    const queueDrainTimeoutMs = parseInt(process.env.SHUTDOWN_QUEUE_TIMEOUT_MS || '20000', 10); // 20s default
    console.log(`Waiting for active jobs to finish (max ${queueDrainTimeoutMs / 1000}s)...`);
    try {
        await Promise.race([
            Promise.all([
                messageQueue.onIdle(),
                paymentProcessor.highPriorityQueue.onIdle(),
                paymentProcessor.normalQueue.onIdle(),
                telegramSendQueue.onIdle()
            ]),
            new Promise((_, reject) => setTimeout(() => reject(new Error(`Queue drain timeout (${queueDrainTimeoutMs}ms)`)), queueDrainTimeoutMs))
        ]);
        console.log("- All processing queues are idle.");
    } catch (queueError) {
        console.warn("⚠️ Timed out waiting for queues or queue error during shutdown:", queueError.message);
    }

    // 3. Close database pool
    console.log("Closing database pool...");
    try {
        await pool.end();
        console.log("✅ Database pool closed.");
    } catch (dbErr) {
        console.error("❌ Error closing database pool:", dbErr);
    } finally {
        console.log("🛑 Full Shutdown complete.");
        process.exit(0); // Exit cleanly after full shutdown
    }
    // --- END: Full Shutdown Logic ---
};


// --- Signal handlers for graceful shutdown ---
process.on('SIGTERM', () => {
    const isRailwayRotation = !!process.env.RAILWAY_ENVIRONMENT;
    console.log(`SIGTERM received. Railway Environment: ${isRailwayRotation}`);
    shutdown('SIGTERM', isRailwayRotation).catch((err) => {
        console.error("Error during SIGTERM shutdown:", err);
        process.exit(1);
    });
});

process.on('SIGINT', () => { // Ctrl+C remains full shutdown
    console.log(`SIGINT received.`);
    shutdown('SIGINT', false).catch((err) => {
        console.error("Error during SIGINT shutdown:", err);
        process.exit(1);
    });
});

// Handle uncaught exceptions
process.on('uncaughtException', (err, origin) => {
    console.error(`🔥🔥🔥 Uncaught Exception at: ${origin}`, err);
    shutdown('UNCAUGHT_EXCEPTION', false).catch(() => process.exit(1));
    setTimeout(() => {
        console.error("Shutdown timed out after uncaught exception. Forcing exit.");
        process.exit(1);
    }, parseInt(process.env.SHUTDOWN_FAIL_TIMEOUT_MS || '12000', 10)).unref(); // 12s default
});

// Handle unhandled promise rejections
process.on('unhandledRejection', (reason, promise) => {
    console.error('🔥🔥🔥 Unhandled Rejection at:', promise, 'reason:', reason);
    // Optional: Decide if shutdown is needed. For now, just logging.
    // Consider shutdown('UNHANDLED_REJECTION', false) if these are critical.
});

// --- Start the Application (MODIFIED Startup Sequence) ---
const PORT = process.env.PORT || 3000;

// Start server immediately, listen on 0.0.0.0 for container compatibility
server = app.listen(PORT, "0.0.0.0", () => {
    console.log(`🚀 Server running on port ${PORT}`);

    // Start heavy initialization AFTER server is listening
    setTimeout(async () => {
        console.log("⚙️ Starting delayed background initialization...");
        try {
            console.log("  - Initializing Database...");
            await initializeDatabase();
            console.log("  - Setting up Telegram...");
            const webhookSet = await setupTelegramWebhook();
            if (!webhookSet) {
                await startPollingIfNeeded();
            }
            console.log("  - Starting Payment Monitor...");
            startPaymentMonitor(); // Start the monitor loop

            // --- Solana Connection Concurrency Adjustment (Optional) ---
             const concurrencyAdjustDelay = parseInt(process.env.CONCURRENCY_ADJUST_DELAY_MS || '20000', 10);
             const targetConcurrency = parseInt(process.env.RPC_TARGET_CONCURRENT || '8', 10); // Adjust target concurrency
             if (targetConcurrency !== solanaConnection.options.maxConcurrent) {
                 setTimeout(() => {
                      if (solanaConnection && solanaConnection.options) {
                           console.log(`⚡ Adjusting Solana connection concurrency from ${solanaConnection.options.maxConcurrent} to ${targetConcurrency}...`);
                           solanaConnection.options.maxConcurrent = targetConcurrency;
                           console.log(`✅ Solana maxConcurrent adjusted to ${targetConcurrency}`);
                      }
                 }, concurrencyAdjustDelay); // Adjust after specified delay
             }

            isFullyInitialized = true; // Mark as fully initialized
            console.log("✅ Delayed Background Initialization Complete. Bot is fully ready.");
            console.log("🚀🚀🚀 Solana Gambles Bot (Multi-RPC) is up and running! 🚀🚀🚀");

        } catch (initError) {
            console.error("🔥🔥🔥 Delayed Background Initialization Error:", initError);
            console.error("❌ Exiting due to critical initialization failure.");
            await shutdown('INITIALIZATION_FAILURE', false).catch(() => process.exit(1));
             setTimeout(() => {
                  console.error("Shutdown timed out after initialization failure. Forcing exit.");
                  process.exit(1);
             }, parseInt(process.env.SHUTDOWN_FAIL_TIMEOUT_MS || '10000', 10)).unref(); // 10s default
        }
    }, parseInt(process.env.INIT_DELAY_MS || '1000', 10)); // 1s default delay
});

// Handle server startup errors (like EADDRINUSE)
server.on('error', (err) => {
    console.error('❌ Server startup error:', err);
    if (err.code === 'EADDRINUSE') {
        console.error(`❌❌❌ Port ${PORT} is already in use. Exiting.`);
    }
    process.exit(1); // Exit on any server startup error
});

// --- End of Part 3 / End of File ---
