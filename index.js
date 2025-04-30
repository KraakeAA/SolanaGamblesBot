// index.js - Final Version with War (Tie is Push), /betcf, /betwar
// --- CORRECTED FOR SEPARATE PAYOUT KEYS, SKEWED ODDS & OTHER FIXES ---
// --- VERSION: 2.6.0 ---

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
import RateLimitedConnection from './lib/solana-connection.js'; // Assuming this file exists and is corrected
import { toByteArray, fromByteArray } from 'base64-js'; // Keep if used by bs58/other libs indirectly, though Buffer is preferred
import { Buffer } from 'buffer'; // Standard Node.js Buffer
// --- End of Imports ---

// --- CORRECTED PLACEMENT for Deployment Check Log ---
console.log(`--- INDEX.JS - DEPLOYMENT CHECK --- ${new Date().toISOString()} ---`);
// --- END CORRECTED PLACEMENT ---


console.log("⏳ Starting Solana Gambles Bot (Multi-RPC)... Checking environment variables...");

// --- START: Enhanced Environment Variable Checks (MODIFIED for 5 Payout Keys & Separate Deposit Wallets) ---
const REQUIRED_ENV_VARS = [
    'BOT_TOKEN',
    'DATABASE_URL',
    // Payout Private Keys (SECRET) - One for each game
    'CF_BOT_PRIVATE_KEY',       // Coinflip payout key (Renamed from BOT_PRIVATE_KEY)
    'RACE_BOT_PRIVATE_KEY',     // Race payout key
    'SLOTS_BOT_PRIVATE_KEY',    // Slots payout key (NEW)
    'ROULETTE_BOT_PRIVATE_KEY', // Roulette payout key (NEW)
    'WAR_BOT_PRIVATE_KEY',      // War payout key (NEW)
    // Deposit Public Keys (Addresses) - One for each game
    'CF_WALLET_ADDRESS',        // Coinflip deposit address (Renamed from MAIN_WALLET_ADDRESS)
    'RACE_WALLET_ADDRESS',      // Race deposit address
    'SLOTS_WALLET_ADDRESS',     // Slots deposit address
    'ROULETTE_WALLET_ADDRESS',  // Roulette deposit address
    'WAR_WALLET_ADDRESS',       // War deposit address
    // Other Required
    'RPC_URLS',                 // Comma-separated list of RPC URLs
];

// Check for Railway-specific variables if deployed there
if (process.env.RAILWAY_ENVIRONMENT) {
    REQUIRED_ENV_VARS.push('RAILWAY_PUBLIC_DOMAIN');
}

// Optional vars with defaults - Adjusted for Skewed Odds implementation
const OPTIONAL_ENV_DEFAULTS = {
    'FEE_MARGIN': '5000', // Default 5000 lamports (0.000005 SOL) - Applied to bet record, not payout calc
    'PAYOUT_PRIORITY_FEE_RATE': '0.0001', // Default 0.01% - Applied to payout TX fee
    'ADMIN_USER_IDS': '',
    // Game Bet Limits & Expiry
    'CF_MIN_BET': '0.01',
    'CF_MAX_BET': '1.0',
    'CF_EXPIRY_MINUTES': '15',
    'RACE_MIN_BET': '0.01',
    'RACE_MAX_BET': '1.0',
    'RACE_EXPIRY_MINUTES': '15',
    'SLOTS_MIN_BET': '0.01',
    'SLOTS_MAX_BET': '0.5',
    'SLOTS_EXPIRY_MINUTES': '10',
    'ROULETTE_MIN_BET': '0.01', // Per placement
    'ROULETTE_MAX_BET': '1.0',  // Per placement
    'ROULETTE_EXPIRY_MINUTES': '10',
    'WAR_MIN_BET': '0.01',
    'WAR_MAX_BET': '1.0',
    'WAR_EXPIRY_MINUTES': '10',
    // House Edge / Skew Configuration (NEW STRUCTURE)
    'CF_HOUSE_EDGE': '0.65', // Coinflip: Chance house auto-wins (0.0 to < 1.0)
    'RACE_HOUSE_EDGE': '0.50', // Race: Chance house auto-wins (0.0 to < 1.0)
    'SLOTS_HIDDEN_EDGE': '0.10', // Slots: Chance to force a losing spin result (0.0 to < 1.0)
    'ROULETTE_HIDDEN_EDGE': '0.65', // Roulette: Chance to force the result to '0' (0.0 to < 1.0)
    // War skew is hardcoded in dealing logic

    // RPC Connection Options
    'RPC_MAX_CONCURRENT': '8',
    'RPC_RETRY_BASE_DELAY': '600',
    'RPC_MAX_RETRIES': '3',
    'RPC_RATE_LIMIT_COOLOFF': '1500',
    'RPC_RETRY_MAX_DELAY': '15000',
    'RPC_RETRY_JITTER': '0.2',
    'RPC_COMMITMENT': 'confirmed',
    // Queue Options
    'MSG_QUEUE_CONCURRENCY': '5',
    'MSG_QUEUE_TIMEOUT_MS': '10000',
    'PAYMENT_HP_CONCURRENCY': '3',
    'PAYMENT_NP_CONCURRENCY': '2',
    // Monitor Options
    'EMERGENCY_ROTATE_THRESHOLD_MS': '20000',
    'MONITOR_INTERVAL_SECONDS': '35',
    'MONITOR_INITIAL_DELAY_MS': '5000',
    'MONITOR_FETCH_LIMIT': '25',
    'MONITOR_WALLET_JITTER_MS': '1500',
    'MONITOR_THROTTLE_MS_PER_ITEM': '50',
    'MONITOR_MAX_THROTTLE_MS': '5000',
    // DB Pool Options
    'DB_POOL_MAX': '15',
    'DB_POOL_MIN': '5',
    'DB_IDLE_TIMEOUT': '30000',
    'DB_CONN_TIMEOUT': '5000',
    'DB_SSL': 'true', // Assuming production needs SSL by default
    'DB_REJECT_UNAUTHORIZED': 'true',
    // Cache Options
    'USER_COOLDOWN_MS': '3000',
    'WALLET_CACHE_TTL_MS': '300000',
    'MAX_PROCESSED_SIG_CACHE': '10000',
    'PAYMENT_MEMO_CACHE_TTL_MS': '30000',
    // Payout Options
    'PAYOUT_JOB_RETRIES': '3',
    'PAYOUT_JOB_RETRY_DELAY_MS': '5000',
    'PAYOUT_BASE_PRIORITY_FEE_MICROLAMPORTS': '1000',
    'PAYOUT_MAX_PRIORITY_FEE_MICROLAMPORTS': '1000000',
    'PAYOUT_CONFIRM_TIMEOUT_MS': '60000',
    // Startup/Shutdown Options
    'INIT_DELAY_MS': '1000',
    'CONCURRENCY_ADJUST_DELAY_MS': '20000',
    'RPC_TARGET_CONCURRENT': '8', // Target concurrency after startup adjustment
    'RAILWAY_ROTATION_DRAIN_MS': '5000',
    'SHUTDOWN_QUEUE_TIMEOUT_MS': '20000',
    'SHUTDOWN_FAIL_TIMEOUT_MS': '8000',
    // Payment Validation
    'PAYMENT_TOLERANCE_LAMPORTS': '5000',
    'PAYMENT_EXPIRY_GRACE_MS': '60000',
    // Telegram Options
    'WEBHOOK_MAX_CONN': '10' // Default max connections for webhook
};

// Validate required environment variables
let missingVars = false;
REQUIRED_ENV_VARS.forEach((key) => {
    if (key === 'RAILWAY_PUBLIC_DOMAIN' && !process.env.RAILWAY_ENVIRONMENT) return;
    if (!process.env[key]) {
        console.error(`❌ Environment variable ${key} is missing.`);
        missingVars = true;
    }
});

// Assign defaults for optional vars if they are missing
Object.entries(OPTIONAL_ENV_DEFAULTS).forEach(([key, defaultValue]) => {
    if (!process.env[key]) {
        // console.log(`ℹ️ Environment variable ${key} not set, using default: ${defaultValue}`); // Reduce noise
        process.env[key] = defaultValue;
    }
});


// Specific check for RPC_URLS content
if (process.env.RPC_URLS && process.env.RPC_URLS.split(',').map(u => u.trim()).filter(u => u).length === 0) {
     console.error(`❌ Environment variable RPC_URLS is set but contains no valid URLs after parsing.`);
     missingVars = true;
} else if (process.env.RPC_URLS && process.env.RPC_URLS.split(',').map(u => u.trim()).filter(u => u).length === 1) {
     console.warn(`⚠️ Environment variable RPC_URLS only contains one URL. Multi-RPC features may not be fully utilized.`);
}


if (missingVars) {
    console.error("⚠️ Please set all required environment variables. Exiting.");
    process.exit(1);
}

// Log effective fee margin and priority rate after defaults are applied
console.log(`ℹ️ Using FEE_MARGIN: ${process.env.FEE_MARGIN} lamports`);
const PRIORITY_FEE_RATE = parseFloat(process.env.PAYOUT_PRIORITY_FEE_RATE);
if (isNaN(PRIORITY_FEE_RATE) || PRIORITY_FEE_RATE < 0) {
     console.warn(`⚠️ Invalid PAYOUT_PRIORITY_FEE_RATE (${process.env.PAYOUT_PRIORITY_FEE_RATE}), defaulting again to 0.0001 (0.01%).`);
     process.env.PAYOUT_PRIORITY_FEE_RATE = '0.0001'; // Ensure env var reflects validated value
}
console.log(`ℹ️ Using PAYOUT_PRIORITY_FEE_RATE: ${process.env.PAYOUT_PRIORITY_FEE_RATE}`);
// --- END: Enhanced Environment Variable Checks ---


// --- Startup State Tracking ---
let isFullyInitialized = false;
let server;

// --- Initialize Scalable Components ---
const app = express();

// --- IMMEDIATE Health Check Endpoint ---
app.get('/health', (req, res) => {
  res.status(200).json({
    status: server ? 'ready' : 'starting',
    uptime: process.uptime()
  });
});

// --- Railway-Specific Health Check Endpoint ---
app.get('/railway-health', (req, res) => {
    res.status(200).json({
        status: isFullyInitialized ? 'ready' : 'starting',
        version: '2.6.0' // Updated version string
    });
});

// --- PreStop hook for Railway graceful shutdown ---
app.get('/prestop', (req, res) => {
    console.log('🚪 Received pre-stop signal from Railway, preparing to shutdown gracefully...');
    res.status(200).send('Shutting down');
});
// --- END Health Check Endpoints ---


// --- Initialize Multi-RPC Solana connection ---
console.log("⚙️ Initializing Multi-RPC Solana connection...");

const rpcUrls = process.env.RPC_URLS.split(',')
    .map(url => url.trim())
    .filter(url => url.length > 0 && (url.startsWith('http://') || url.startsWith('https://')));

if (rpcUrls.length === 0) {
    console.error("❌ No valid RPC URLs found in RPC_URLS environment variable after filtering. Exiting.");
    process.exit(1);
}

console.log(`ℹ️ Using RPC Endpoints: ${rpcUrls.join(', ')}`);

const solanaConnection = new RateLimitedConnection(rpcUrls, {
    maxConcurrent: parseInt(process.env.RPC_MAX_CONCURRENT, 10),
    retryBaseDelay: parseInt(process.env.RPC_RETRY_BASE_DELAY, 10),
    maxRetries: parseInt(process.env.RPC_MAX_RETRIES, 10),
    rateLimitCooloff: parseInt(process.env.RPC_RATE_LIMIT_COOLOFF, 10),
    retryMaxDelay: parseInt(process.env.RPC_RETRY_MAX_DELAY, 10),
    retryJitter: parseFloat(process.env.RPC_RETRY_JITTER),
    commitment: process.env.RPC_COMMITMENT,
    httpHeaders: {
        'User-Agent': `SolanaGamblesBot/2.6.0` // Updated version
    },
    clientId: `SolanaGamblesBot/2.6.0` // Updated version
});
console.log("✅ Multi-RPC Solana connection instance created.");


// --- LIVE QUEUE MONITOR ---
setInterval(() => {
    if (!solanaConnection?.getRequestStats) return;
    try {
        const statsInfo = solanaConnection.getRequestStats();
        if (!statsInfo?.status || !statsInfo?.stats) return;

        const { totalRequestsSucceeded = 0, totalRequestsFailed = 0 } = statsInfo.stats;
        const { queueSize = 0, activeRequests = 0 } = statsInfo.status;
        const totalProcessed = totalRequestsSucceeded + totalRequestsFailed;
        const successRate = totalProcessed > 0 ? (100 * totalRequestsSucceeded / totalProcessed).toFixed(1) : "N/A";

        // Use statsInfo.stats.endpointRotations for rotations count
        console.log(`[Queue Monitor] Active: ${activeRequests}, Pending: ${queueSize}, Success Rate: ${successRate}%, Rotations: ${statsInfo.stats.endpointRotations ?? 'N/A'}`);
    } catch (e) {
        console.error("[Queue Monitor] Error accessing stats:", e.message);
    }
}, 60000); // Every 60 seconds


// --- EMERGENCY AUTO-ROTATE ---
let lastQueueProcessTime = Date.now();

// Ensure _processQueue exists before attempting to patch
if (typeof solanaConnection?._processQueue === 'function') {
    const originalProcessQueue = solanaConnection._processQueue.bind(solanaConnection);
    solanaConnection._processQueue = async function patchedProcessQueue() {
        lastQueueProcessTime = Date.now();
        try {
            await originalProcessQueue();
        } catch (queueProcessingError) {
            console.error("❌ Error during Solana connection internal queue processing:", queueProcessingError);
        }
    };

    setInterval(() => {
        try {
            const now = Date.now();
            const stuckDuration = now - lastQueueProcessTime;
            const queueSize = solanaConnection.requestQueue?.length || 0;
            const activeRequests = solanaConnection.activeRequests || 0;
            const stuckThreshold = parseInt(process.env.EMERGENCY_ROTATE_THRESHOLD_MS, 10);

            if (stuckDuration > stuckThreshold && (queueSize > 0 || activeRequests > 0)) {
                console.warn(`[Emergency Rotate] No queue movement detected for ${stuckDuration}ms with pending/active requests (${activeRequests} active, ${queueSize} pending). Threshold: ${stuckThreshold}ms. Forcing endpoint rotation...`);
                if (typeof solanaConnection?._rotateEndpoint === 'function') {
                    solanaConnection._rotateEndpoint();
                } else {
                     console.error("[Emergency Rotate] _rotateEndpoint function not found on connection object!");
                }
                lastQueueProcessTime = Date.now(); // Reset timer after forced rotation
            }
        } catch (e) {
             console.error("[Emergency Rotate Check] Error:", e.message);
        }
    }, 10000); // Check every 10 seconds
} else {
     console.warn("[Emergency Rotate] Cannot patch _processQueue: Function not found on connection object.");
}


// --- Initial RPC Connection Test ---
setTimeout(async () => {
  try {
    const currentSlot = await solanaConnection.getSlot();
    console.log(`✅ [Startup Test] RPC call successful! Current Solana Slot: ${currentSlot}`);
  } catch (err) {
    console.warn(`⚠️ [Startup Test] Initial RPC call failed (may resolve shortly):`, err.message);
  }
}, parseInt(process.env.MONITOR_INITIAL_DELAY_MS, 10)); // Use configured delay


// 2. Message Processing Queue
const messageQueue = new PQueue({
    concurrency: parseInt(process.env.MSG_QUEUE_CONCURRENCY, 10),
    timeout: parseInt(process.env.MSG_QUEUE_TIMEOUT_MS, 10)
});
console.log(`✅ Message processing queue initialized (Concurrency: ${messageQueue.concurrency})`);


// 3. Enhanced PostgreSQL Pool
console.log("⚙️ Setting up optimized PostgreSQL Pool...");
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    max: parseInt(process.env.DB_POOL_MAX, 10),
    min: parseInt(process.env.DB_POOL_MIN, 10),
    idleTimeoutMillis: parseInt(process.env.DB_IDLE_TIMEOUT, 10),
    connectionTimeoutMillis: parseInt(process.env.DB_CONN_TIMEOUT, 10),
    ssl: process.env.NODE_ENV === 'production' || process.env.DB_SSL === 'true' ? {
        rejectUnauthorized: process.env.DB_REJECT_UNAUTHORIZED !== 'false'
    } : false
});
pool.on('error', (err, client) => {
    console.error('❌ Unexpected error on idle PostgreSQL client', err);
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
        if (this.requests % 50 === 0) {
             const uptime = (Date.now() - this.startTime) / 1000;
             const errorRate = this.requests > 0 ? (this.errors / this.requests * 100).toFixed(1) : 0;
             let statsString = '';
             try {
                  const connectionStats = (typeof solanaConnection?.getRequestStats === 'function')
                       ? solanaConnection.getRequestStats()
                       : null;
                  statsString = connectionStats?.status && connectionStats?.stats
                       ? `| SOL Conn: Q:${connectionStats.status.queueSize ?? 'N/A'} A:${connectionStats.status.activeRequests ?? 'N/A'} E:${connectionStats.status.currentEndpointIndex ?? 'N/A'} RL:${connectionStats.status.consecutiveRateLimits ?? 'N/A'} Rot:${connectionStats.stats.endpointRotations ?? 'N/A'} Fail:${connectionStats.stats.totalRequestsFailed ?? 'N/A'}`
                       : '';
             } catch (e) {
                  statsString = '| SOL Conn: Error fetching stats';
             }
             console.log(`📊 Perf Mon: Uptime:${uptime.toFixed(0)}s | Req:${this.requests} | Err:${errorRate}% ${statsString}`);
        }
    }
};


// --- Database Initialization ---
async function initializeDatabase() {
    console.log("⚙️ Initializing Database schema...");
    let client;
    try {
        client = await pool.connect();

        // Bets Table
        await client.query(`CREATE TABLE IF NOT EXISTS bets (
            id SERIAL PRIMARY KEY,
            user_id TEXT NOT NULL,
            chat_id TEXT NOT NULL,
            game_type TEXT NOT NULL, -- e.g., 'coinflip', 'race', 'slots', 'roulette', 'war'
            bet_details JSONB, -- Game specific details (e.g., {'choice':'heads'}, {'horse':'Blue'}, {}, {'bets': {'R': 10000000}}, {})
            expected_lamports BIGINT NOT NULL CHECK (expected_lamports >= 0), -- Min bet enforced elsewhere
            memo_id TEXT UNIQUE NOT NULL,
            status TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            expires_at TIMESTAMPTZ NOT NULL,
            paid_tx_signature TEXT UNIQUE,
            payout_tx_signature TEXT UNIQUE,
            processed_at TIMESTAMPTZ,
            fees_paid BIGINT, -- Fee margin buffer recorded at bet creation
            priority INT NOT NULL DEFAULT 0
        );`);

        // Wallets Table
        await client.query(`CREATE TABLE IF NOT EXISTS wallets (
            user_id TEXT PRIMARY KEY,
            wallet_address TEXT NOT NULL,
            linked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_used_at TIMESTAMPTZ
        );`);

        // Add columns idempotently
        await client.query('BEGIN');
        await client.query(`ALTER TABLE bets ADD COLUMN IF NOT EXISTS priority INT NOT NULL DEFAULT 0;`);
        await client.query(`ALTER TABLE bets ADD COLUMN IF NOT EXISTS fees_paid BIGINT;`); // Ensure this exists if needed elsewhere, although payout % cut is removed
        await client.query(`ALTER TABLE bets ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ;`);
        await client.query(`ALTER TABLE wallets ADD COLUMN IF NOT EXISTS last_used_at TIMESTAMPTZ;`);
        await client.query('COMMIT');
        console.log("   - Columns verified/added.");

        // Add indexes idempotently
        console.log("   - Creating indexes (if they don't exist)...");
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_status_created ON bets(status, created_at DESC);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_user_id ON bets(user_id);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_expires_at ON bets(expires_at);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_priority ON bets(priority DESC);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_paid_tx_sig ON bets(paid_tx_signature) WHERE paid_tx_signature IS NOT NULL;`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_payout_tx_sig ON bets(payout_tx_signature) WHERE payout_tx_signature IS NOT NULL;`);
        const constraintCheck = await client.query(`SELECT constraint_name FROM information_schema.table_constraints WHERE table_name = 'bets' AND constraint_name = 'bets_memo_id_key' AND constraint_type = 'UNIQUE';`);
         if (constraintCheck.rowCount === 0) {
             console.log("   - Adding UNIQUE constraint on bets(memo_id)...");
             await client.query(`ALTER TABLE bets ADD CONSTRAINT bets_memo_id_key UNIQUE (memo_id);`);
         }
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_memo_id_pending ON bets (memo_id) WHERE status = 'awaiting_payment';`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_game_type ON bets (game_type);`); // Index on game_type
        await client.query(`CREATE INDEX IF NOT EXISTS idx_wallets_address ON wallets(wallet_address);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_wallets_last_used ON wallets(last_used_at DESC NULLS LAST);`);

        console.log("✅ Database schema initialized/verified.");
    } catch (err) {
        console.error("❌ Database initialization error:", err);
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { console.error("Rollback failed:", rbErr); } }
        if (err.code === '42P07' || err.code === '23505' || err.message.includes('already exists')) {
             console.warn("   - Note: Some elements (table/index/constraint) already existed, which is usually okay.");
        } else {
             throw err;
        }
    } finally {
        if (client) client.release();
    }
}

// --- End of Part 1 ---
// --- Start of Part 2a ---
// index.js - Part 2a (Corrected for Separate Payout Keys & Skewed Odds)
// --- VERSION: 2.6.0 ---

// (Code continues directly from the end of Part 1)

// --- Telegram Bot Initialization with Queue ---
console.log("⚙️ Initializing Telegram Bot...");
const bot = new TelegramBot(process.env.BOT_TOKEN, {
    polling: false, // Use webhooks in production (set later in startup)
    request: {      // Adjust request options for stability
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
        return;
    }
    // Add to queue only if it passes basic checks
    messageQueue.add(() => handleMessage(msg)) // handleMessage defined in Part 3b
        .catch(err => {
            console.error(`⚠️ Message processing queue error for msg ID ${msg?.message_id}:`, err);
            performanceMonitor.logRequest(false); // Log error
        });
});


// [PATCHED: THROTTLED TELEGRAM SEND]
// Queue for sending messages to avoid hitting Telegram rate limits
const telegramSendQueue = new PQueue({ concurrency: 1, interval: 1050, intervalCap: 1 });

/**
 * Sends a message via Telegram safely using a rate-limited queue.
 * IMPORTANT: MarkdownV2 escaping must be done *before* calling this function
 * using the escapeMarkdownV2 helper for dynamic content AND manually escaping
 * reserved characters in static text (e.g., '!' becomes '\\!').
 * @param {string|number} chatId - The chat ID to send the message to.
 * @param {string} message - The message text.
 * @param {TelegramBot.SendMessageOptions} [options={}] - Telegram Bot API message options (e.g., parse_mode).
 * @returns {Promise<TelegramBot.Message|undefined>} A promise resolving with the sent message or undefined on error.
 */
function safeSendMessage(chatId, message, options = {}) {
    if (!chatId || typeof message !== 'string') {
        console.error("safeSendMessage: Missing chatId or invalid message type.");
        return Promise.resolve(undefined);
    }
    if (message.length > 4096) {
        console.warn(`safeSendMessage: Message too long (${message.length} > 4096), truncating.`);
        message = message.substring(0, 4090) + '...'; // Truncate leaving space for ellipsis
    }

    // NOTE: Escaping should be done *before* calling safeSendMessage.

    return telegramSendQueue.add(() =>
        bot.sendMessage(chatId, message, options).catch(err => {
            console.error(`❌ Telegram send error to chat ${chatId}:`, err.message);
            if (err.response && err.response.statusCode === 403) {
                console.warn(`Bot may be blocked or kicked from chat ${chatId}.`);
            } else if (err.response && err.response.statusCode === 400 && err.message.includes("can't parse entities")) {
                 console.error(` -> Telegram Parse Error: ${err.message}. Check escaping in message content being sent.`);
                 // Log the problematic message snippet (first 100 chars) for easier debugging
                 console.error(` -> Message Snippet (Check Escaping!): ${message.substring(0, 100)}...`);
            } else if (err.response && err.response.statusCode === 400 && err.message.includes("message is too long")) {
                 console.error(` -> Confirmed message too long rejection for chat ${chatId}.`);
            } else if (err.response && err.response.statusCode === 429) {
                 console.warn(`Hit Telegram rate limit sending to ${chatId}. Queue should manage this.`);
            }
             return undefined; // Return undefined on error
        })
    );
}

console.log("✅ Telegram Bot initialized");


// --- Express Setup for Webhook and Health Check ---
app.use(express.json({ limit: '10kb' }));

// Original Health check / Info endpoint
app.get('/', (req, res) => {
    performanceMonitor.logRequest(true);
    const processor = paymentProcessor; // Assuming paymentProcessor is defined later
    let connectionStats = null;
    try {
         connectionStats = typeof solanaConnection?.getRequestStats === 'function'
             ? solanaConnection.getRequestStats()
             : null;
    } catch (e) { console.error("Error getting connection stats for /:", e.message); }

    const messageQueueSize = messageQueue?.size || 0;
    const messageQueuePending = messageQueue?.pending || 0;
    const telegramSendQueueSize = telegramSendQueue?.size || 0;
    const telegramSendQueuePending = telegramSendQueue?.pending || 0;
    const paymentHighPriQueueSize = processor?.highPriorityQueue?.size || 0;
    const paymentHighPriQueuePending = processor?.highPriorityQueue?.pending || 0;
    const paymentNormalQueueSize = processor?.normalQueue?.size || 0;
    const paymentNormalQueuePending = processor?.normalQueue?.pending || 0;

    res.status(200).json({
        status: 'ok',
        initialized: isFullyInitialized,
        timestamp: new Date().toISOString(),
        version: '2.6.0', // Updated version
        queueStats: {
            messageQueuePending: messageQueueSize,
            messageQueueActive: messageQueuePending,
            telegramSendPending: telegramSendQueueSize,
            telegramSendActive: telegramSendQueuePending,
            paymentHighPriPending: paymentHighPriQueueSize,
            paymentHighPriActive: paymentHighPriQueuePending,
            paymentNormalPending: paymentNormalQueueSize,
            paymentNormalActive: paymentNormalQueuePending
        },
        solanaConnection: connectionStats
    });
});

// Webhook handler
const webhookPath = `/bot${process.env.BOT_TOKEN}`;
app.post(webhookPath, (req, res) => {
    messageQueue.add(() => {
        try {
            if (!req.body || typeof req.body !== 'object') {
                console.warn("⚠️ Received invalid webhook request body");
                performanceMonitor.logRequest(false);
                // Send status only if headers not already sent
                if (!res.headersSent) {
                   return res.status(400).send('Invalid request body');
                }
                return; // Avoid further processing
            }
            bot.processUpdate(req.body); // Hand off to bot library
            performanceMonitor.logRequest(true);
            // Acknowledge Telegram quickly
            if (!res.headersSent) {
               res.sendStatus(200);
            }
        } catch (error) {
            console.error("❌ Webhook processing error:", error);
            performanceMonitor.logRequest(false);
            if (!res.headersSent) {
                res.sendStatus(500);
            }
        }
    }).catch(queueError => {
        console.error("❌ Error adding webhook update to message queue:", queueError);
        performanceMonitor.logRequest(false);
        // Try to send 500 if possible
        if (!res.headersSent) {
            res.sendStatus(500);
        }
    });
});


// --- State Management & Constants ---

// User command cooldown
const confirmCooldown = new Map();
const cooldownInterval = parseInt(process.env.USER_COOLDOWN_MS, 10);

// Cache for linked wallets
const walletCache = new Map();
const CACHE_TTL = parseInt(process.env.WALLET_CACHE_TTL_MS, 10);

// Cache of processed transaction signatures
const processedSignaturesThisSession = new Set();
const MAX_PROCESSED_SIGNATURES = parseInt(process.env.MAX_PROCESSED_SIG_CACHE, 10);

// --- Game Configuration ---
function validateGameConfig(config) {
     const errors = [];
     for (const game in config) {
         const gc = config[game];
         if (!gc) {
             errors.push(`Config for game "${game}" is missing.`);
             continue;
         }
         if (isNaN(gc.minBet) || gc.minBet <= 0) errors.push(`${game}.minBet invalid (${gc.minBet})`);
         if (isNaN(gc.maxBet) || gc.maxBet <= 0) errors.push(`${game}.maxBet invalid (${gc.maxBet})`);
         if (isNaN(gc.expiryMinutes) || gc.expiryMinutes <= 0) errors.push(`${game}.expiryMinutes invalid (${gc.expiryMinutes})`);
         // Check houseEdge only if it exists (now only for CF/Race)
         if (gc.houseEdge !== undefined && (isNaN(gc.houseEdge) || gc.houseEdge < 0 || gc.houseEdge >= 1)) {
             errors.push(`${game}.houseEdge invalid (${gc.houseEdge})`);
         }
     }
     return errors;
}

// ** MODIFIED GAME_CONFIG: Removed houseEdge for Slots, Roulette, War **
// CF_HOUSE_EDGE and RACE_HOUSE_EDGE now control auto-win probability only.
const GAME_CONFIG = {
    coinflip: {
        minBet: parseFloat(process.env.CF_MIN_BET),
        maxBet: parseFloat(process.env.CF_MAX_BET),
        expiryMinutes: parseInt(process.env.CF_EXPIRY_MINUTES, 10),
        houseEdge: parseFloat(process.env.CF_HOUSE_EDGE || '0.65') // Keep for auto-win logic
    },
    race: {
        minBet: parseFloat(process.env.RACE_MIN_BET),
        maxBet: parseFloat(process.env.RACE_MAX_BET),
        expiryMinutes: parseInt(process.env.RACE_EXPIRY_MINUTES, 10),
        houseEdge: parseFloat(process.env.RACE_HOUSE_EDGE || '0.50') // Keep for auto-win logic
    },
    slots: {
        minBet: parseFloat(process.env.SLOTS_MIN_BET),
        maxBet: parseFloat(process.env.SLOTS_MAX_BET),
        expiryMinutes: parseInt(process.env.SLOTS_EXPIRY_MINUTES, 10),
        // houseEdge removed - using SLOTS_HIDDEN_EDGE in game logic
    },
    roulette: {
        minBet: parseFloat(process.env.ROULETTE_MIN_BET),
        maxBet: parseFloat(process.env.ROULETTE_MAX_BET),
        expiryMinutes: parseInt(process.env.ROULETTE_EXPIRY_MINUTES, 10),
        // houseEdge removed - using ROULETTE_HIDDEN_EDGE in game logic
    },
    war: {
        minBet: parseFloat(process.env.WAR_MIN_BET),
        maxBet: parseFloat(process.env.WAR_MAX_BET),
        expiryMinutes: parseInt(process.env.WAR_EXPIRY_MINUTES, 10),
        // houseEdge removed - skew handled in dealing logic
    }
};
// Validate new structure (houseEdge is optional)
const configErrors = validateGameConfig(GAME_CONFIG);
if (configErrors.length > 0) {
    console.error(`❌ Invalid game configuration values found for: ${configErrors.join(', ')}. Please check corresponding environment variables.`);
    process.exit(1);
}
console.log("ℹ️ Game Config Loaded (Adjusted for Skewed Odds & Full Payouts):", JSON.stringify(GAME_CONFIG));


// Fee buffer (still used for bet record)
const FEE_BUFFER = BigInt(process.env.FEE_MARGIN);

// Helper function for MarkdownV2 escaping (use this consistently for dynamic content)
const escapeMarkdownV2 = (text) => {
     if (typeof text !== 'string') text = String(text); // Convert non-strings
     // Escape characters: _ * [ ] ( ) ~ ` > # + - = | { } . !
     // Ensure backslashes are escaped first if they might already exist
     text = text.replace(/\\/g, '\\\\');
     return text.replace(/([_*\[\]()~`>#+\-=|{}.!])/g, '\\$1');
};


// --- Helper Functions (Existing) ---

function debugInstruction(inst, accountKeys) {
     try {
         const programIdKeyInfo = accountKeys[inst.programIdIndex];
         const programId = programIdKeyInfo?.pubkey ? new PublicKey(programIdKeyInfo.pubkey) :
                           (typeof programIdKeyInfo === 'string' ? new PublicKey(programIdKeyInfo) :
                           (programIdKeyInfo instanceof PublicKey ? programIdKeyInfo : null));

         const accountPubkeys = inst.accounts?.map(idx => {
             const keyInfo = accountKeys[idx];
             return keyInfo?.pubkey ? new PublicKey(keyInfo.pubkey) :
                    (typeof keyInfo === 'string' ? new PublicKey(keyInfo) :
                    (keyInfo instanceof PublicKey ? keyInfo : null));
         }).filter(pk => pk !== null)
           .map(pk => pk.toBase58());

         return {
             programId: programId ? programId.toBase58() : `Invalid Index ${inst.programIdIndex}`,
             data: inst.data ? Buffer.from(inst.data, 'base64').toString('hex') : null,
             accounts: accountPubkeys
         };
     } catch (e) {
         console.error("[DEBUG INSTR HELPER] Error:", e);
         return { error: e.message, programIdIndex: inst.programIdIndex, accountIndices: inst.accounts };
     }
}

const decodeInstructionData = (data) => {
     if (!data) return null;
     try {
         if (typeof data === 'string') {
              // Try base64 first
              if (/^[A-Za-z0-9+/]*={0,2}$/.test(data) && data.length % 4 === 0) {
                  try {
                      const decoded = Buffer.from(data, 'base64').toString('utf8');
                      // Check for non-printable characters common in raw data
                      if (/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/.test(decoded)) return null; // Return null if likely binary
                      return decoded;
                  } catch { /* ignore base64 decode error */ }
              }
              // Try base58 (for short strings that look like keys/hashes but might be memos)
               if (/[1-9A-HJ-NP-Za-km-z]{20,}/.test(data) && data.length < 70) {
                   try {
                       const decoded = Buffer.from(bs58.decode(data)).toString('utf8');
                       if (/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/.test(decoded)) return null;
                       return decoded;
                   } catch { /* ignore base58 decode error */ }
               }
               // If it's not clearly base64 or base58, return as is (might be plain text)
                // Check for non-printable chars again before returning raw string
                if (/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/.test(data)) return null;
                return data;
         }
         else if (Buffer.isBuffer(data) || data instanceof Uint8Array) {
             const decoded = Buffer.from(data).toString('utf8');
             if (/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/.test(decoded)) return null;
             return decoded;
         }
         else if (Array.isArray(data) && data.every(n => typeof n === 'number')) {
             const decoded = Buffer.from(data).toString('utf8');
              if (/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/.test(decoded)) return null;
              return decoded;
         }
         // console.warn(`[decodeInstructionData] Unhandled data type: ${typeof data}`); // Reduce noise
         return null; // Return null for unhandled types or potential binary data
     } catch (e) {
         // console.warn(`[decodeInstructionData] Error decoding data: ${e.message}`); // Reduce noise
         return null; // Return null on any decoding error
     }
};

// --- calculateNetPayout function REMOVED ---

// --- End of Part 2a ---
// --- Start of Part 2b ---
// index.js - Part 2b (Corrected for Separate Payout Keys & Skewed Odds)
// --- VERSION: 2.6.0 ---

// (Code continues directly from the end of Part 2a)

// --- START: Memo Handling System ---
// (Updated for new game prefixes SL, RL, WA)

// Define Memo Program IDs
const MEMO_V1_PROGRAM_ID = new PublicKey("Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo");
const MEMO_V2_PROGRAM_ID = new PublicKey("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr");
const MEMO_PROGRAM_IDS = [MEMO_V1_PROGRAM_ID.toBase58(), MEMO_V2_PROGRAM_ID.toBase58()];

// Allowed memo prefixes for V1 generation/validation
const VALID_MEMO_PREFIXES = ['BET', 'CF', 'RA', 'SL', 'RL', 'WA']; // ADDED SL, RL, WA

// 1. Revised Memo Generation with Checksum
function generateMemoId(prefix = 'BET') {
    if (!VALID_MEMO_PREFIXES.includes(prefix)) {
        console.warn(`[generateMemoId] Invalid prefix "${prefix}", defaulting to BET.`);
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

// 2. Memo Normalization (crucial for matching incoming memos)
function normalizeMemo(rawMemo) {
    if (typeof rawMemo !== 'string') {
        return null;
    }
    let memo = rawMemo.replace(/\n$/, ''); // Remove single trailing newline
    memo = memo.replace(/[\x00-\x1F\x7F]/g, '').trim(); // Remove control chars, trim
    memo = memo.toUpperCase() // Standardize case
        .replace(/^MEMO[:=\s]*/i, '') // Remove common prefixes
        .replace(/^TEXT[:=\s]*/i, '')
        .replace(/[\u200B-\u200D\uFEFF]/g, '') // Remove zero-width spaces
        .replace(/\s+/g, '-') // Replace spaces with dashes
        .replace(/[^A-Z0-9\-]/g, ''); // Remove remaining invalid chars

   // Handle potential trailing 'N' (Phantom bug workaround) - Regex uses updated prefixes
   const trailingNPattern = new RegExp(`^(${VALID_MEMO_PREFIXES.join('|')})-([A-F0-9]{16})-([A-F0-9]{2})N$`);
   const trailingNMatch = memo.match(trailingNPattern);
   if (trailingNMatch) {
        console.warn(`[NORMALIZE_MEMO] Detected V1-like memo with trailing 'N': "${memo}". Correcting.`);
        memo = memo.slice(0, -1);
   }

    // V1 Pattern Match and Checksum validation - Regex uses updated prefixes
    const v1Pattern = new RegExp(`^(${VALID_MEMO_PREFIXES.join('|')})-([A-F0-9]{16})-([A-F0-9]{2})$`);
    const v1Match = memo.match(v1Pattern);
    if (v1Match) {
        const [/* full match */, prefix, hex, checksum] = v1Match;
        let expectedChecksum;
        try {
            expectedChecksum = crypto.createHash('sha256')
                .update(hex)
                .digest('hex')
                .slice(-2)
                .toUpperCase();
        } catch (hashError) {
             console.error(`[NORMALIZE_MEMO] CRITICAL ERROR calculating checksum for hex "${hex}":`, hashError);
             return null; // Return null on critical hashing error
        }
        if (checksum !== expectedChecksum) {
             console.warn(`[NORMALIZE_MEMO] Checksum MISMATCH on V1 memo "${memo}"! Input CS: "${checksum}", Expected: "${expectedChecksum}". Using expected.`);
             // Return the format with the CORRECT calculated checksum
             return `${prefix}-${hex}-${expectedChecksum}`;
        }
        // Checksum matches, return original validated memo
        return memo;
    }

    // Fallback if V1 Pattern Did NOT Match
    if (memo && memo.length >= 3 && memo.length <= 64) { // Basic sanity checks
         // console.warn(`[NORMALIZE_MEMO] Memo "${rawMemo}" did not match V1 format. Returning basic normalized: "${memo}"`); // Optional: Log non-V1 memos
         return memo; // Return whatever is left after basic normalization
    }
    return null;
}


// 3. Strict Memo Validation (Used only for validating OUR generated V1 format)
function validateOriginalMemoFormat(memo) {
     if (typeof memo !== 'string') return false;
     const parts = memo.split('-');
     if (parts.length !== 3) return false;
     const [prefix, hex, checksum] = parts;
     const validateMemoChecksum = (h, cs) => {
         try {
             const expected = crypto.createHash('sha256').update(h).digest('hex').slice(-2).toUpperCase();
             return expected === cs;
         } catch (e) {
             console.error(`[validateMemoChecksum] Hashing error for hex ${h}: ${e.message}`);
             return false;
         }
     };
     // Use updated VALID_MEMO_PREFIXES here
     return (
         VALID_MEMO_PREFIXES.includes(prefix) &&
         hex.length === 16 &&
         /^[A-F0-9]{16}$/.test(hex) &&
         checksum.length === 2 &&
         /^[A-F0-9]{2}$/.test(checksum) &&
         validateMemoChecksum(hex, checksum) // Validate against calculated checksum
     );
}

// 4. Find Memo in Transaction (Uses normalizeMemo)
async function findMemoInTx(tx, signature) {
    const startTime = Date.now();
    const usedMethods = [];
    let scanDepth = 0;
    const transactionResponse = tx;

    if (!transactionResponse?.transaction?.message) {
        // console.warn(`[FindMemo] Sig ${signature?.slice(0,6)}: Invalid TX structure, no message found.`);
        return null;
    }

    // 1. Try log messages first (often the most reliable for simple memo programs)
    scanDepth = 1;
    if (transactionResponse.meta?.logMessages) {
        // Regex to find memo logged by common programs (SPL Memo, Memo V1/V2)
        const memoLogRegex = /Program log: (?:Instruction: (?:Spl Memo|Memo)|Memo) instruction: (.+)/i;
        const memoLogRegexV2 = /Program log: Memo(?: \(len \d+\))?:\s*"?([^"\n]+)"?/; // Original regex, keep as fallback

        for (const log of transactionResponse.meta.logMessages) {
            let rawMemo = null;
            const match1 = log.match(memoLogRegex);
            if (match1?.[1]) {
                 rawMemo = match1[1].trim();
            } else {
                 const match2 = log.match(memoLogRegexV2);
                 if (match2?.[1]) {
                      rawMemo = match2[1].trim();
                 }
            }

            if (rawMemo) {
                 const memo = normalizeMemo(rawMemo);
                 if (memo) {
                      usedMethods.push('LogScanRegex');
                      // console.log(`[FindMemo] Sig ${signature?.slice(0,6)}: Found memo "${memo}" via LogScanRegex (Depth ${scanDepth}, ${Date.now()-startTime}ms)`);
                      return memo;
                 }
            }
        }
    }

    // 2. Fallback to instruction parsing (more complex, handles memos embedded in CPIs)
    scanDepth = 2;
    const message = transactionResponse.transaction.message;
    let accountKeys = [];

    try {
        // Handle both legacy and versioned transaction formats for account keys
        if (message.staticAccountKeys) { // Versioned TX
             accountKeys = message.staticAccountKeys.map(k => k.toBase58());
        } else if (message.accountKeys) { // Legacy TX
             accountKeys = message.accountKeys.map(k => k.toBase58 ? k.toBase58() : String(k));
        } else {
             console.warn(`[FindMemo] Sig ${signature?.slice(0,6)}: Could not extract account keys from message.`);
        }
    } catch (e) {
        console.error(`[FindMemo] Sig ${signature?.slice(0,6)}: Error extracting account keys: ${e.message}`);
    }

    const allInstructions = [
        ...(message.instructions || []),
        ...(transactionResponse.meta?.innerInstructions || []).flatMap(inner => inner.instructions || [])
    ];

    if (allInstructions.length > 0 && accountKeys.length > 0) { // Ensure accountKeys were extracted
        for (const [i, inst] of allInstructions.entries()) {
            try {
                 // Ensure programIdIndex is valid before accessing accountKeys
                 if (inst.programIdIndex === undefined || inst.programIdIndex < 0 || inst.programIdIndex >= accountKeys.length) {
                     // console.warn(`[FindMemo] Sig ${signature?.slice(0,6)}: Invalid programIdIndex ${inst.programIdIndex} for instruction ${i}.`);
                     continue; // Skip this instruction if index is invalid
                 }
                 const programId = accountKeys[inst.programIdIndex];


                 if (programId && MEMO_PROGRAM_IDS.includes(programId)) {
                      const dataString = decodeInstructionData(inst.data); // decodeInstructionData handles base64/utf8 etc.
                       if (dataString) {
                           const memo = normalizeMemo(dataString);
                           if (memo) {
                               const method = MEMO_PROGRAM_IDS.indexOf(programId) === 0 ? 'InstrParseV1' : 'InstrParseV2';
                               usedMethods.push(method);
                               // console.log(`[FindMemo] Sig ${signature?.slice(0,6)}: Found memo "${memo}" via ${method} (Depth ${scanDepth}, ${Date.now()-startTime}ms)`);
                               return memo;
                           }
                       }
                 }

                 // Optional: Raw pattern matching fallback within instruction data (less reliable)
                 // scanDepth = 3; ... if (dataBuffer && ...) ... validateOriginalMemoFormat ...

            } catch (e) {
                console.error(`[FindMemo] Sig ${signature?.slice(0,6)}: Error processing instruction ${i} (ProgramID: ${accountKeys[inst.programIdIndex] ?? 'N/A'}):`, e?.message || e);
            }
        }
    }

    // 3. Deep Log Scan as final fallback (redundant if first log scan is thorough, but kept for safety)
    scanDepth = 4;
    if (transactionResponse.meta?.logMessages && !usedMethods.includes('LogScanRegex')) { // Only run if not already found by regex
        const logString = transactionResponse.meta.logMessages.join('\n');
        // Regex specifically for our V1 format (includes WA)
        const logMemoMatch = logString.match(new RegExp(`(?:${VALID_MEMO_PREFIXES.join('|')})-[A-F0-9]{16}-[A-F0-9]{2}`));
        if (logMemoMatch?.[0]) {
            const recoveredMemo = normalizeMemo(logMemoMatch[0]); // Normalize just in case of extra chars
            if (recoveredMemo && validateOriginalMemoFormat(recoveredMemo)) { // Validate it strictly matches OUR format
                 usedMethods.push('DeepLogScanV1');
                 // console.log(`[FindMemo] Sig ${signature?.slice(0,6)}: Found memo "${recoveredMemo}" via DeepLogScanV1 (Depth ${scanDepth}, ${Date.now()-startTime}ms)`);
                 return recoveredMemo;
            }
        }
    }

    // Lookup tables make direct analysis harder, just log if present and no memo found yet
    if (message.addressTableLookups?.length > 0 && usedMethods.length === 0) {
      console.log(`[FindMemo] Sig ${signature?.slice(0,6)}: Transaction uses address lookup tables. No memo found. (Time: ${Date.now()-startTime}ms)`);
    } else if (usedMethods.length === 0) {
       // console.log(`[FindMemo] Sig ${signature?.slice(0,6)}: No valid memo found after all checks. (Time: ${Date.now()-startTime}ms)`); // Reduce noise unless debugging
    }

    return null; // No valid memo found
}
// --- END: Memo Handling System ---


// --- Database Operations ---
// (Functions remain structurally the same, relying on the corrected schema)

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
        ON CONFLICT (memo_id) DO NOTHING
        RETURNING id;
    `;
    const values = [
        String(userId), String(chatId), gameType, details, BigInt(lamports),
        memoId, 'awaiting_payment', expiresAt, FEE_BUFFER, priority
    ];
    try {
        const res = await pool.query(query, values);
        if (res.rows.length > 0) {
            return { success: true, id: res.rows[0].id };
        } else {
            // Attempt to retrieve the existing bet's status if collision occurs
             const existingBet = await pool.query('SELECT status, id FROM bets WHERE memo_id = $1', [memoId]);
             if (existingBet.rows.length > 0) {
                 console.warn(`DB: Memo ID collision for ${memoId}. Bet ID ${existingBet.rows[0].id} already exists with status: ${existingBet.rows[0].status}.`);
                 return { success: false, error: `Bet with Memo ID ${memoId} already exists (Status: ${existingBet.rows[0].status}). Please wait or try again.` };
             } else {
                  console.error(`DB: Memo ID collision for ${memoId}, but failed to retrieve existing bet.`);
                  return { success: false, error: 'Bet with this Memo ID may already exist. Please wait or try again.' };
             }
        }
    } catch (err) {
        console.error('DB Error saving bet:', err.message, err.code);
        return { success: false, error: `Database error (${err.code || 'N/A'})` };
    }
}

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
        new PublicKey(walletAddress); // Basic validation
        const res = await pool.query(query, [String(userId), walletAddress]);
        const linkedWallet = res.rows[0]?.wallet_address;
        if (linkedWallet) {
            walletCache.set(cacheKey, { wallet: linkedWallet, timestamp: Date.now() });
            // Schedule cache expiration check
            setTimeout(() => {
                const current = walletCache.get(cacheKey);
                if (current && current.wallet === linkedWallet && Date.now() - current.timestamp >= CACHE_TTL) {
                    walletCache.delete(cacheKey);
                }
            }, CACHE_TTL + 1000); // Check slightly after TTL expires
            return { success: true, wallet: linkedWallet };
        } else {
            console.error(`DB: Failed to link/update wallet for user ${userId}.`);
            return { success: false, error: 'Failed to update wallet in database.' };
        }
    } catch (err) {
        if (err instanceof Error && (err.message.includes('Invalid public key') || err.message.includes('Invalid address'))) {
            console.error(`DB Error linking wallet: Invalid address format for user ${userId}: ${walletAddress}`);
            return { success: false, error: 'Invalid wallet address format.' };
        }
        console.error(`DB Error linking wallet for user ${userId}:`, err.message);
        return { success: false, error: `Database error (${err.code || 'N/A'})` };
    }
}

async function getLinkedWallet(userId) {
    const cacheKey = `wallet-${userId}`;
    const cachedEntry = walletCache.get(cacheKey);
    if (cachedEntry && Date.now() - cachedEntry.timestamp < CACHE_TTL) {
        return cachedEntry.wallet;
    } else if (cachedEntry) {
        walletCache.delete(cacheKey); // Delete expired entry
    }
    const query = `SELECT wallet_address FROM wallets WHERE user_id = $1`;
    try {
        const res = await pool.query(query, [String(userId)]);
        const wallet = res.rows[0]?.wallet_address;
        if (wallet) {
            // Update cache
            walletCache.set(cacheKey, { wallet, timestamp: Date.now() });
            // Schedule cache expiration check
            setTimeout(() => {
                const current = walletCache.get(cacheKey);
                 if (current && current.wallet === wallet && Date.now() - current.timestamp >= CACHE_TTL) {
                      walletCache.delete(cacheKey);
                 }
            }, CACHE_TTL + 1000);
        }
        return wallet; // Return found wallet or undefined
    } catch (err) {
        console.error(`DB Error fetching wallet for user ${userId}:`, err.message);
        return undefined;
    }
}

async function updateBetStatus(betId, status) {
    // Ensure status is not overly long or invalid
    const validStatusPattern = /^[a-z0-9_]+$/i; // Simple pattern for valid status chars
    if (typeof status !== 'string' || status.length > 50 || !validStatusPattern.test(status)) {
         console.error(`DB: Invalid status format attempted for bet ${betId}: "${status}"`);
         return false;
    }
    const query = `UPDATE bets SET status = $1, processed_at = CASE WHEN $1 LIKE 'completed_%' OR $1 LIKE 'error_%' OR $1 LIKE 'processing_payout' OR $1 LIKE 'processing_game' THEN NOW() ELSE processed_at END WHERE id = $2 RETURNING id;`;
    // Added processing_payout and processing_game to update processed_at time earlier
    try {
        const res = await pool.query(query, [status, betId]);
        return res.rowCount > 0;
    } catch (err) {
        console.error(`DB Error updating bet ${betId} status to ${status}:`, err.message);
        return false;
    }
}

async function recordPayout(betId, status, signature) {
    // Validate status before DB operation
      const validStatusPattern = /^[a-z0-9_]+$/i;
      if (typeof status !== 'string' || status.length > 50 || !validStatusPattern.test(status)) {
           console.error(`DB: Invalid status format attempted for payout record on bet ${betId}: "${status}"`);
           return false;
      }
      // Validate signature format (basic Solana signature check)
      if (typeof signature !== 'string' || !/^[1-9A-HJ-NP-Za-km-z]{64,88}$/.test(signature)) {
           console.error(`DB: Invalid signature format attempted for payout record on bet ${betId}: "${signature}"`);
           return false;
      }

    const query = `
        UPDATE bets
        SET status = $1,
            payout_tx_signature = $2,
            processed_at = NOW()
        WHERE id = $3 AND status = 'processing_payout' -- Only update if currently processing
        RETURNING id;
    `;
    try {
        const res = await pool.query(query, [status, signature, betId]);
        if (res.rowCount > 0) {
            return true; // Successfully updated
        } else {
            // If rowCount is 0, check the current status to see why it failed
            const current = await pool.query('SELECT status, payout_tx_signature FROM bets WHERE id = $1', [betId]);
            if (current.rows.length === 0) {
                 console.warn(`DB: Failed to record payout for bet ${betId}: Bet not found.`);
            } else if (current.rows[0].status !== 'processing_payout') {
                 console.warn(`DB: Failed to record payout for bet ${betId}: Status mismatch (Expected 'processing_payout', got '${current.rows[0].status}'). Payout might have already been recorded or failed.`);
                 // Check if it was already recorded successfully
                 if(current.rows[0].payout_tx_signature === signature && current.rows[0].status === status) {
                     console.log(`DB: Bet ${betId} payout already recorded correctly. Treating as success.`);
                     return true;
                 }
            } else {
                 console.warn(`DB: Failed to record payout for bet ${betId} for unknown reason (status was 'processing_payout').`);
            }
            return false; // Update failed
        }
    } catch (err) {
        // Handle potential unique constraint violation if TX signature already exists for another bet (highly unlikely but possible)
        if (err.code === '23505' && err.constraint?.includes('payout_tx_signature')) {
            console.error(`DB: CRITICAL - Payout TX Signature ${signature.slice(0,10)}... unique constraint violation for bet ${betId}. This TX may already be linked to another bet!`);
            return false; // Treat constraint violation as failure here
        }
        console.error(`DB Error recording payout for bet ${betId}:`, err.message, err.code);
        return false;
    }
}


// --- Solana Transaction Analysis ---

/**
 * Analyzes a transaction to find SOL transfers to the specified target bot wallet.
 * Checks both top-level and inner instructions for transfers.
 * Uses balance changes as primary source, falls back to instruction parsing.
 * @param {import('@solana/web3.js').ParsedTransactionWithMeta | import('@solana/web3.js').VersionedTransactionResponse | null} tx - The transaction object from getParsedTransaction.
 * @param {string} targetAddress - The specific public key (as a string) of the bot's deposit wallet address to check for incoming funds.
 * @returns {{ transferAmount: bigint, payerAddress: string | null }} The transfer amount in lamports and the detected payer address.
 */
 // ** MODIFIED: Accepts specific targetAddress instead of inferring from gameType **
function analyzeTransactionAmounts(tx, targetAddress) {
    let transferAmount = 0n;
    let payerAddress = null;

    // Validation
    if (!tx || !targetAddress) {
        // console.warn("[analyzeAmounts] Invalid input: Missing tx or targetAddress.");
        return { transferAmount: 0n, payerAddress: null };
    }
    // Basic validation for targetAddress format
    try {
        new PublicKey(targetAddress);
    } catch (e) {
        console.error(`[analyzeAmounts] Invalid targetAddress format provided: ${targetAddress}`);
        return { transferAmount: 0n, payerAddress: null };
    }

     if (tx.meta?.err) {
         // console.log("[analyzeAmounts] Transaction failed on-chain (tx.meta.err present)."); // Reduce noise
         return { transferAmount: 0n, payerAddress: null };
     }

    const getAccountKeysFromTx = (tx) => {
        try {
            if (!tx?.transaction?.message) return [];

            const message = tx.transaction.message;
            if (message.staticAccountKeys) { // Versioned TX
                 return message.staticAccountKeys.map(k => k.toBase58());
            } else if (message.accountKeys) { // Legacy TX
                 // Need to handle potential PublicKey objects or strings
                 return message.accountKeys.map(k => {
                     if (k instanceof PublicKey) return k.toBase58();
                     if (typeof k === 'string') return k; // Already stringified
                     if (k && typeof k.toBase58 === 'function') return k.toBase58(); // Handle objects with toBase58
                     return String(k); // Fallback conversion
                 });
            }
        } catch (e) {
            console.error("[analyzeAmounts] Error parsing account keys:", e.message);
        }
        return [];
    };

    // 1. Use balance changes (most reliable for simple transfers)
    let balanceChange = 0n;
    if (tx.meta?.preBalances && tx.meta?.postBalances && tx.transaction?.message) {
        const accountKeys = getAccountKeysFromTx(tx);
        const targetIndex = accountKeys.indexOf(targetAddress); // Use the provided targetAddress

        if (targetIndex !== -1 &&
            tx.meta.preBalances.length > targetIndex && tx.meta.postBalances.length > targetIndex &&
            tx.meta.preBalances[targetIndex] !== null && tx.meta.postBalances[targetIndex] !== null) {

            balanceChange = BigInt(tx.meta.postBalances[targetIndex]) - BigInt(tx.meta.preBalances[targetIndex]);

            if (balanceChange > 0n) {
                 transferAmount = balanceChange; // Initial assumption

                 // Try to identify payer based on balance changes of signers
                 const signers = tx.transaction.signatures?.length > 0
                     ? (tx.transaction.message.accountKeys || []).slice(0, tx.transaction.message.header?.numRequiredSignatures || 1)
                     : [];
                 const signerKeys = signers.map(s => s.toBase58 ? s.toBase58() : String(s));


                 for (const signerKey of signerKeys) {
                     const signerIndex = accountKeys.indexOf(signerKey);
                     if (signerIndex !== -1 && signerIndex !== targetIndex && // Ensure signer is not the target
                         tx.meta.preBalances.length > signerIndex && tx.meta.postBalances.length > signerIndex &&
                         tx.meta.preBalances[signerIndex] !== null && tx.meta.postBalances[signerIndex] !== null)
                     {
                         const preSignerBalance = BigInt(tx.meta.preBalances[signerIndex]);
                         const postSignerBalance = BigInt(tx.meta.postBalances[signerIndex]);
                         const signerBalanceChange = postSignerBalance - preSignerBalance;
                         const fee = BigInt(tx.meta.fee || 0); // Fee paid by fee payer (usually first signer)

                         // Check if the signer's balance decreased by roughly the transfer amount (allowing for fees if they are the fee payer)
                         const expectedDecrease = -(transferAmount + (signerIndex === 0 ? fee : 0n)); // Signer 0 pays fee
                         const tolerance = BigInt(100); // Small tolerance for other potential small costs

                         if (signerBalanceChange <= expectedDecrease && signerBalanceChange >= expectedDecrease - tolerance) {
                              payerAddress = signerKey;
                              break; // Found likely payer
                         }
                          // Fallback: If signer's balance just decreased by more than the transfer amount (less precise)
                          else if (signerBalanceChange <= -transferAmount) {
                               if (!payerAddress) payerAddress = signerKey; // Tentative payer if none found yet
                          }
                     }
                 }
                 // If no payer identified via balance changes, fee payer is the best guess
                 if (!payerAddress && signerKeys.length > 0) {
                      payerAddress = signerKeys[0];
                 }
            }
        }
    } // End balance check

    // 2. Check instructions if balance change method was inconclusive or amount is 0
    // This helps confirm transfers made via CPIs or complex interactions
    let instructionTransferAmount = 0n;
    let instructionPayer = null;

    if (tx.transaction?.message) {
        const accountKeys = getAccountKeysFromTx(tx);
        const SYSTEM_PROGRAM_ID = SystemProgram.programId.toBase58();
        const topLevelInstructions = tx.transaction.message.instructions || [];
        const innerInstructionsNested = tx.meta?.innerInstructions || [];
        const allInstructions = [
            ...topLevelInstructions.map((inst, index) => ({ ...inst, type: 'topLevel', index })),
            ...innerInstructionsNested.flatMap(innerSet =>
                (innerSet.instructions || []).map((inst, subIndex) => ({ ...inst, type: 'inner', parentIndex: innerSet.index, subIndex }))
            )
        ];

        for (const instWrapper of allInstructions) {
            const inst = instWrapper;
            let programId = ''; let parsedInfo = null; let isParsedTransfer = false;
            try {
                 // Get program ID safely
                  if ('programIdIndex' in inst && accountKeys.length > inst.programIdIndex) {
                       programId = accountKeys[inst.programIdIndex];
                  } else if ('programId' in inst) { // Handle ParsedInstruction format which might have programId directly
                       programId = typeof inst.programId === 'string' ? inst.programId : inst.programId?.toBase58();
                  }

                 if (programId === SYSTEM_PROGRAM_ID) {
                      // Check if it's already parsed as a transfer
                      if (inst.parsed?.type === 'transfer' || inst.parsed?.type === 'transferChecked') {
                           isParsedTransfer = true;
                           parsedInfo = inst.parsed.info;
                      }
                       // Add manual decoding for SystemProgram.transfer if needed (less common with getParsedTransaction)
                 }

                 // Use the provided targetAddress for checking destination
                 if (isParsedTransfer && parsedInfo?.destination === targetAddress) {
                      const currentInstructionAmount = BigInt(parsedInfo.lamports || parsedInfo.amount || parsedInfo.tokenAmount?.amount || 0); // Added tokenAmount check just in case
                       if (currentInstructionAmount > 0n) {
                           instructionTransferAmount += currentInstructionAmount; // Sum transfers if multiple found
                           if (!instructionPayer && parsedInfo.source) {
                               instructionPayer = parsedInfo.source; // Capture first identified source
                           }
                       }
                 }
            } catch(parseError) {
                console.error(`[analyzeAmounts] Error processing instruction (${instWrapper.type} ${instWrapper.index ?? ''}/${instWrapper.subIndex ?? ''}): ${parseError.message}`);
            }
        } // End instruction loop
    } // End instruction check block


     // Final Decision:
     // Prefer balance change amount if positive, otherwise use instruction amount.
     // Prefer payer identified by balance change analysis if available, otherwise instruction payer, otherwise fee payer.
     if (transferAmount > 0n) {
          // We already have transferAmount from balance changes.
          // Use payerAddress derived from balance change analysis if available.
          return { transferAmount, payerAddress: payerAddress ?? instructionPayer ?? null }; // Fallback chain
     } else if (instructionTransferAmount > 0n) {
         // Use instruction data if balance change was zero (e.g., wrapped SOL interaction)
         console.log(`[analyzeAmounts] Using instruction analysis. Amount: ${instructionTransferAmount}, Payer: ${instructionPayer}`);
         return { transferAmount: instructionTransferAmount, payerAddress: instructionPayer };
     } else {
         // No positive transfer detected by either method
         return { transferAmount: 0n, payerAddress: null };
     }
}


// --- Payment Processing System ---

function isRetryableError(error) {
    const msg = error?.message?.toLowerCase() || '';
    const code = error?.code || error?.cause?.code;
    const status = error?.response?.status || error?.statusCode;

    // Rate limits
    if (status === 429 || msg.includes('429') || msg.includes('rate limit') || msg.includes('too many requests')) return true;
    // Server errors (generic)
    if ([500, 502, 503, 504].includes(Number(status)) || msg.includes('server error') || msg.includes('internal server error')) return true;
    // Network/Connection errors
    if (['timeout', 'timed out', 'econnreset', 'esockettimedout', 'network error', 'fetch', 'socket hang up', 'connection terminated', 'econnrefused', 'failed to fetch', 'getaddrinfo enotfound', 'connection refused'].some(m => msg.includes(m)) ||
        ['ETIMEDOUT', 'ECONNRESET', 'ENETUNREACH', 'EAI_AGAIN', 'ECONNABORTED', 'ECONNREFUSED', 'UND_ERR_CONNECT_TIMEOUT', 'UND_ERR_HEADERS_TIMEOUT', 'UND_ERR_BODY_TIMEOUT', 'FETCH_TIMEOUT', 'FETCH_ERROR'].includes(code)) return true;
    // Database specific connection errors
    if (msg.includes('connection terminated unexpectedly') || code === 'ECONNREFUSED' || code === '57P01' || code === '57P03') return true;
    // Solana specific transient errors
      if (msg.includes('transaction simulation failed') ||
          msg.includes('failed to simulate transaction') || // Duplicate but safe
          msg.includes('blockhash not found') ||
          msg.includes('slot leader does not match') ||
          msg.includes('node is behind') ||
          msg.includes("processing transaction") || // Sometimes indicates temporary node load
          msg.includes("block not available") ||
          msg.includes("sending transaction") // Transient RPC issue
          ) return true;
      // Solana connection library specific reasons
      if (error?.reason === 'rpc_error' || error?.reason === 'network_error') return true;

    return false;
}


class GuaranteedPaymentProcessor {
    constructor() {
        this.highPriorityQueue = new PQueue({ concurrency: parseInt(process.env.PAYMENT_HP_CONCURRENCY, 10) });
        this.normalQueue = new PQueue({ concurrency: parseInt(process.env.PAYMENT_NP_CONCURRENCY, 10) });
        this.activeProcesses = new Set();
        this.memoCache = new Map();
        this.cacheTTL = parseInt(process.env.PAYMENT_MEMO_CACHE_TTL_MS, 10);
        console.log(`✅ Initialized GuaranteedPaymentProcessor (HP: ${this.highPriorityQueue.concurrency}, NP: ${this.normalQueue.concurrency})`);
    }

    async addPaymentJob(job) {
        const jobIdentifier = job.signature || job.betId || `job-${Date.now()}`;
        const jobKey = `${job.type}:${jobIdentifier}`;
        const queue = (job.priority && job.priority > 0) ? this.highPriorityQueue : this.normalQueue;

        // Avoid adding duplicate jobs already in the queue or actively processing
        // Simple check on activeProcesses; queue check is harder without iterating
        if (this.activeProcesses.has(jobKey)) {
             // console.log(`[PaymentProcessor] Job ${jobKey} already active. Skipping add.`); // Reduce noise
             return;
        }

        // Add jobKey property to the task function for potential duplicate checking later
        const task = () => this.processJob(job);
        // task.jobKey = jobKey; // Attach the key // Not strictly needed if activeProcesses check works

        queue.add(task).catch(queueError => {
            console.error(`Queue error processing job ${jobKey}:`, queueError.message);
            performanceMonitor.logRequest(false);
            this.activeProcesses.delete(jobKey); // Ensure cleanup on queue error too
        });
    }

    async processJob(job) {
        const jobIdentifier = job.signature || job.betId;
        const jobKey = `${job.type}:${jobIdentifier || crypto.randomUUID()}`; // Use UUID for safety if no identifier

        if (this.activeProcesses.has(jobKey)) {
            // Should ideally be caught by addPaymentJob check, but safety first
             // console.warn(`[PaymentProcessor] Job ${jobKey} processing collision detected!`);
            return;
        }
        this.activeProcesses.add(jobKey);

        try {
            let result;
            if (job.type === 'monitor_payment') {
                // The `job.walletType` here corresponds to the type assigned in `monitorPayments`
                // which matches the key of the env var (e.g., 'slots', 'roulette').
                // _processIncomingPayment will handle determining the actual target address from the bet.
                result = await this._processIncomingPayment(job.signature, job.walletType);
            } else if (job.type === 'process_bet') {
                const bet = await pool.query('SELECT * FROM bets WHERE id = $1', [job.betId]).then(res => res.rows[0]);
                if (bet) {
                    // Ensure bet is actually in a state ready for processing
                    if (bet.status === 'payment_verified') {
                         await processPaidBet(bet); // processPaidBet handles own errors (defined in Part 3a)
                         result = { processed: true };
                    } else {
                         console.warn(`[PROCESS_JOB] Bet ${job.betId} is not in 'payment_verified' state (Status: ${bet.status}). Skipping game processing.`);
                         result = { processed: false, reason: `process_bet_wrong_status: ${bet.status}` };
                    }
                } else {
                    console.error(`[PROCESS_JOB] Cannot process bet: Bet ID ${job.betId} not found.`);
                    result = { processed: false, reason: 'bet_not_found' };
                }
            } else if (job.type === 'payout') {
                 let retries = parseInt(process.env.PAYOUT_JOB_RETRIES, 10);
                 const baseRetryDelay = parseInt(process.env.PAYOUT_JOB_RETRY_DELAY_MS, 10);
                 let attempt = 0;
                 while (attempt <= retries) {
                      attempt++;
                      try {
                           await handlePayoutJob(job); // handlePayoutJob handles own errors (defined in Part 3b)
                           result = { processed: true };
                           break; // Success, exit retry loop
                      } catch (err) {
                           const errorMessage = err?.message || 'Unknown payout error';
                           const isRetryable = isRetryableError(err);
                            // Check current bet status *before* deciding to retry/fail
                            const currentBetStatusResult = await pool.query('SELECT status FROM bets WHERE id = $1', [job.betId]);
                            const currentBetStatus = currentBetStatusResult.rows[0]?.status;

                            // If already completed or errored out, don't retry or send messages
                            if (currentBetStatus?.startsWith('completed_') || currentBetStatus?.startsWith('error_payout_')) {
                                console.warn(`[PAYOUT_JOB_RETRY] Bet ${job.betId} is already in final state '${currentBetStatus}'. Aborting payout attempts.`);
                                result = { processed: false, reason: `payout_already_final_state: ${currentBetStatus}` };
                                break; // Exit retry loop
                            }

                           console.warn(`[PAYOUT_JOB_RETRY] Payout attempt ${attempt}/${retries} failed for bet ${job.betId}. Error: ${errorMessage}. Retryable: ${isRetryable}`);

                           if (!isRetryable || attempt > retries) {
                               const finalErrorStatus = isRetryable ? 'error_payout_job_failed' : 'error_payout_non_retryable';
                               console.error(`[PAYOUT_JOB_FAIL] Final payout attempt failed (or error not retryable) for bet ${job.betId}. Setting status to ${finalErrorStatus}. Error: ${errorMessage}`);
                               await updateBetStatus(job.betId, finalErrorStatus); // Update status to reflect failure
                               // Send user notification about the final failure
                               const safeErrorMsg = escapeMarkdownV2((errorMessage || 'Unknown Error').substring(0, 200));
                                // ** MD ESCAPE APPLIED ** - Escaped `\` `(` `)` `.` twice
                               await safeSendMessage(job.chatId, `⚠️ Payout for bet \`${escapeMarkdownV2(job.memoId)}\` failed after ${attempt} attempt\\(s\\)\\. Error: ${safeErrorMsg}\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
                               result = { processed: false, reason: `${isRetryable ? 'payout_retries_exhausted' : 'payout_non_retryable'}: ${errorMessage}` };
                               break; // Exit retry loop
                           }

                           // Calculate delay with jitter before retrying
                           const delay = baseRetryDelay * Math.pow(2, attempt - 1) + Math.random() * 1000;
                           await new Promise(resolve => setTimeout(resolve, delay));
                      } // end catch
                 } // end while loop
            } else {
                console.error(`[PROCESS_JOB] Unknown job type: ${job.type}`);
                result = { processed: false, reason: 'unknown_job_type' };
            }
            // Log request success/failure based on the outcome
            if (result) performanceMonitor.logRequest(result.processed === true);
            return result; // Return the final result of the job processing
        } catch (error) {
            // Catch any unexpected errors during the job processing itself (outside the specific handlers/retry loops)
            performanceMonitor.logRequest(false);
            console.error(`[PROCESS_JOB] Unhandled error processing job ${jobKey}:`, error.message, error.stack);
            // Attempt to mark the bet with an error if applicable and not already handled
            if (job.betId && job.type !== 'monitor_payment') {
                 const currentStatusCheck = await pool.query('SELECT status FROM bets WHERE id = $1', [job.betId]).then(r => r.rows[0]?.status);
                 if (currentStatusCheck && !currentStatusCheck.startsWith('error_') && !currentStatusCheck.startsWith('completed_')) {
                      const errorStatus = `error_${job.type}_uncaught`;
                      await updateBetStatus(job.betId, errorStatus);
                      console.log(`[PROCESS_JOB] Set bet ${job.betId} status to ${errorStatus} due to uncaught job error.`);
                      // Maybe notify user?
                       // ** MD ESCAPE APPLIED ** - Escaped `\` `.` twice
                      await safeSendMessage(job.chatId || 'admin', `⚠️ Uncaught error processing job for bet \`${escapeMarkdownV2(job.memoId || job.betId)}\`\\. Please contact support\\.`, { parse_mode: 'MarkdownV2'});
                 }
            }
             return { processed: false, reason: `uncaught_job_error: ${error.message}` };
        } finally {
            this.activeProcesses.delete(jobKey); // Ensure the job key is removed from active set
        }
    }

    // Internal method to process an incoming payment signature
    // ** MODIFIED: Determines target wallet based on bet.game_type **
    async _processIncomingPayment(signature, monitoredWalletType) { // monitoredWalletType still passed for context
        const logPrefix = `Sig ${signature.slice(0, 6)}...`;
        if (processedSignaturesThisSession.has(signature)) {
            // console.log(`${logPrefix}: Already processed in this session.`);
            return { processed: false, reason: 'already_processed_session' };
        }
        try {
            // Check DB first
            const exists = await pool.query('SELECT id, status FROM bets WHERE paid_tx_signature = $1 LIMIT 1', [signature]);
            if (exists.rowCount > 0) {
                // console.log(`${logPrefix}: Already exists in DB`);
                processedSignaturesThisSession.add(signature); this._cleanSignatureCache();
                return { processed: false, reason: 'exists_in_db' };
            }
        } catch (dbError) {
             console.error(`${logPrefix}: DB error checking paid_tx_signature: ${dbError.message}`);
             return { processed: false, reason: 'db_check_error', error: dbError };
        }

        let tx;
        try {
             tx = await this._getTransactionWithRetry(signature);
             if (!tx) { return { processed: false, reason: 'fetch_failed_or_not_confirmed' }; }
             if (tx.meta?.err){
                 processedSignaturesThisSession.add(signature); this._cleanSignatureCache();
                 return { processed: false, reason: 'onchain_failure' };
             }

             const memo = await this._extractMemoGuaranteed(tx, signature);
             if (!memo) {
                 processedSignaturesThisSession.add(signature); this._cleanSignatureCache();
                 return { processed: false, reason: 'no_valid_memo' };
             }

             const bet = await this._findBetGuaranteed(memo);
             if (!bet) {
                  const existingBetStatusResult = await pool.query('SELECT status FROM bets WHERE memo_id = $1 LIMIT 1', [memo]);
                  const existingBetStatus = existingBetStatusResult.rows[0]?.status;
                  if (existingBetStatus && existingBetStatus !== 'awaiting_payment') {
                       processedSignaturesThisSession.add(signature); this._cleanSignatureCache();
                       return { processed: false, reason: 'bet_already_processed_or_expired' };
                  } else {
                       processedSignaturesThisSession.add(signature); this._cleanSignatureCache();
                       return { processed: false, reason: 'no_matching_bet_found' };
                  }
             }

             // --- Determine correct TARGET wallet address based on BET's game type ---
             let expectedTargetAddress = null;
             let targetEnvVarName = '';
             switch(bet.game_type) {
                case 'coinflip': targetEnvVarName = 'CF_WALLET_ADDRESS'; break;
                case 'race':     targetEnvVarName = 'RACE_WALLET_ADDRESS'; break;
                case 'slots':    targetEnvVarName = 'SLOTS_WALLET_ADDRESS'; break;
                case 'roulette': targetEnvVarName = 'ROULETTE_WALLET_ADDRESS'; break;
                case 'war':      targetEnvVarName = 'WAR_WALLET_ADDRESS'; break;
                default:
                    console.error(`${logPrefix}: Unknown game_type "${bet.game_type}" for bet ${bet.id}. Cannot determine target wallet.`);
                    await updateBetStatus(bet.id, 'error_unknown_game_type');
                    return { processed: false, reason: 'unknown_game_type_config' };
             }
             expectedTargetAddress = process.env[targetEnvVarName];

             if (!expectedTargetAddress) {
                 console.error(`${logPrefix}: Target wallet address for game_type "${bet.game_type}" (env var ${targetEnvVarName}) is not configured!`);
                 await updateBetStatus(bet.id, 'error_missing_wallet_config');
                 return { processed: false, reason: 'missing_wallet_config' };
             }
             // --- End target wallet determination ---

             // Process the payment details within a DB transaction, passing the specific address
             const processResult = await this._processPaymentGuaranteed(bet, signature, expectedTargetAddress, tx);

              // If payment verification was successful, queue the next step (game processing)
              if (processResult.processed) {
                   const finalBetDetails = await pool.query('SELECT status FROM bets WHERE id = $1', [bet.id]).then(res => res.rows[0]);
                   if (finalBetDetails && finalBetDetails.status === 'payment_verified') {
                       await this._queueBetProcessing(bet); // Queue using the original bet object fetched
                   } else {
                       console.error(`${logPrefix}: CRITICAL! Post-verify status mismatch for Bet ID ${bet.id}. Status: ${finalBetDetails?.status ?? 'Not Found'} instead of 'payment_verified'.`);
                       await updateBetStatus(bet.id, 'error_post_verify_status_mismatch');
                   }
              }
              return processResult; // Return result from _processPaymentGuaranteed

        } catch (error) {
             // Catch errors from the steps within this function (fetch, memo extract, find bet)
             console.error(`${logPrefix}: Error during _processIncomingPayment pipeline: ${error.message}`, error.stack);
             const isDefinitiveFailure = !isRetryableError(error) || ['onchain_failure', 'no_valid_memo', 'no_matching_bet_found', 'bet_already_processed_or_expired'].includes(error.reason);
             if (isDefinitiveFailure && signature) { // Add signature only if available and failure is final
                  processedSignaturesThisSession.add(signature); this._cleanSignatureCache(); // Cache sig if failure is final
             }
             // Try to update bet status if a bet was involved
             if (error.betId && typeof error.betId === 'number') {
                  await updateBetStatus(error.betId, 'error_processing_exception');
             }
             return { processed: false, reason: `processing_pipeline_error: ${error.message}` };
        }
    }

    async _getTransactionWithRetry(signature) {
        let retries = 3; // Simple retry count for fetching
        let lastError = null;
        while(retries > 0) {
            try {
                 const tx = await solanaConnection.getParsedTransaction(signature, {
                     maxSupportedTransactionVersion: 0,
                     commitment: 'confirmed' // Use confirmed to ensure data (like balances) is available
                 });
                  // Check if transaction is actually confirmed (null means not found/confirmed)
                  if (tx === null) {
                      // console.log(`Sig ${signature?.slice(0,6)}: Transaction not found or not confirmed yet.`);
                      lastError = new Error("Transaction not found or not confirmed yet.");
                      retries--;
                      if (retries > 0) await new Promise(r => setTimeout(r, 500 * (3 - retries)));
                      else return null;
                      continue;
                  }
                  return tx;
            } catch (error) {
                 lastError = error;
                 retries--;
                 console.warn(`Sig ${signature?.slice(0,6)}: Error fetching getParsedTransaction (Attempt ${4-retries}/3): ${error.message}`);
                 if (!isRetryableError(error) || retries === 0) {
                     console.error(`Sig ${signature?.slice(0,6)}: Final error fetching transaction: ${error.message}`);
                     return null;
                 }
                 await new Promise(r => setTimeout(r, 500 * (3 - retries)));
            }
        }
        return null; // Retries exhausted
    }

    async _extractMemoGuaranteed(tx, signature) {
        // Wrapper around the improved findMemoInTx
        return findMemoInTx(tx, signature);
    }

    async _findBetGuaranteed(memo) {
        // Check cache first
        const cachedBet = this.memoCache.get(memo);
        if (cachedBet && Date.now() - cachedBet.timestamp < this.cacheTTL) {
             if(cachedBet.bet?.status === 'awaiting_payment'){
                 return cachedBet.bet;
             } else {
                 this.memoCache.delete(memo);
             }
        } else if (cachedBet) {
             this.memoCache.delete(memo);
        }

        // DB lookup with retry logic
        let retries = 3;
        let lastError = null;
        while (retries > 0) {
            try {
                const res = await pool.query(
                    "SELECT * FROM bets WHERE memo_id = $1 AND status = 'awaiting_payment' ORDER BY created_at DESC LIMIT 1",
                    [memo]
                );
                const bet = res.rows[0];
                if (bet) {
                    this.memoCache.set(memo, { bet, timestamp: Date.now() });
                    setTimeout(() => {
                        const current = this.memoCache.get(memo);
                        if (current && current.bet?.id === bet.id && Date.now() - current.timestamp >= this.cacheTTL) {
                            this.memoCache.delete(memo);
                        }
                    }, this.cacheTTL + 1000);
                }
                return bet;
            } catch (dbError) {
                lastError = dbError;
                retries--;
                console.error(`DB error finding bet for memo ${memo} (Attempt ${4-retries}/3): ${dbError.message}`);
                if (!isRetryableError(dbError) || retries === 0) {
                    console.error(`[FIND_BET] Non-retryable DB error or retries exhausted for memo ${memo}.`);
                    return undefined;
                }
                await new Promise(r => setTimeout(r, 500 * (3 - retries)));
            }
        }
        return undefined; // Retries exhausted
    }

    // Performs payment validation and updates DB within a transaction
     // ** MODIFIED: Accepts specific expectedTargetAddress **
    async _processPaymentGuaranteed(bet, signature, expectedTargetAddress, tx) {
        const logPrefix = `Bet ${bet.id} (Memo ${bet.memo_id.slice(0,8)}...)`;
        const client = await pool.connect(); // Get a dedicated client for transaction
        try {
            await client.query('BEGIN');

            // 1. Re-verify bet status inside transaction using FOR UPDATE to lock the row
            const currentStatusRes = await client.query(
                 'SELECT status, expires_at FROM bets WHERE id = $1 FOR UPDATE', // Fetch expiry_at too
                 [bet.id]
            );

             // Check if bet still exists
             if (currentStatusRes.rows.length === 0) {
                 await client.query('ROLLBACK');
                 console.log(`${logPrefix}: Bet not found inside TX. Aborting.`);
                 return { processed: false, reason: `bet_not_found_in_tx` };
             }

            const currentStatus = currentStatusRes.rows[0].status;
            const expiryTime = new Date(currentStatusRes.rows[0].expires_at); // Use expiry from DB

            // Check status
            if (currentStatus !== 'awaiting_payment') {
                await client.query('ROLLBACK');
                if (currentStatus && currentStatus !== 'awaiting_payment') {
                    processedSignaturesThisSession.add(signature); this._cleanSignatureCache();
                }
                return { processed: false, reason: `status_mismatch_in_tx: ${currentStatus}` };
            }

             // 2. Re-check expiry using DB time (more reliable)
             const now = Date.now();
             const expiryGraceMs = parseInt(process.env.PAYMENT_EXPIRY_GRACE_MS || '60000', 10);
             if (now > expiryTime.getTime() + expiryGraceMs) {
                 await client.query(`UPDATE bets SET status = $1, processed_at = NOW() WHERE id = $2`, ['error_expired_payment', bet.id]);
                 await client.query('COMMIT');
                 console.log(`${logPrefix}: Expired before payment confirmed (Expired At: ${expiryTime.toISOString()}, Now: ${new Date(now).toISOString()}).`);
                 processedSignaturesThisSession.add(signature); this._cleanSignatureCache(); // Add sig to cache
                 return { processed: false, reason: 'expired_before_confirmation' };
             }


            // 3. Analyze transaction amount and get payer using the correct target wallet address
            const { transferAmount, payerAddress } = analyzeTransactionAmounts(tx, expectedTargetAddress); // Pass expectedTargetAddress
              if (transferAmount <= 0n) {
                 await client.query(`UPDATE bets SET status = $1, processed_at = NOW() WHERE id = $2`, ['error_payment_amount_zero', bet.id]);
                 await client.query('COMMIT');
                 console.log(`${logPrefix}: No valid transfer amount found in transaction ${signature} to target wallet ${expectedTargetAddress.slice(0,6)}...`);
                 processedSignaturesThisSession.add(signature); this._cleanSignatureCache(); // Add sig to cache
                 return { processed: false, reason: 'transfer_amount_zero' };
              }

            // 4. Verify amount with tolerance
             const expected = BigInt(bet.expected_lamports);
             const tolerance = BigInt(process.env.PAYMENT_TOLERANCE_LAMPORTS || '5000');
             if (transferAmount < expected - tolerance) {
                 await client.query(`UPDATE bets SET status = $1, paid_tx_signature = $2, processed_at = NOW() WHERE id = $3`, ['error_payment_amount_low', signature, bet.id]); // Record sig even on failure
                 await client.query('COMMIT');
                 console.log(`${logPrefix}: Payment amount too low. Expected >=${expected - tolerance}, Got ${transferAmount}.`);
                 // Notify user about incorrect amount
                 // ** MD ESCAPE & DECIMAL APPLIED ** - Escaped `\` `.` twice using toFixed(3)
                 const expectedSOL = (Number(expected)/LAMPORTS_PER_SOL).toFixed(3);
                 const receivedSOL = (Number(transferAmount)/LAMPORTS_PER_SOL).toFixed(3);
                 await safeSendMessage(bet.chat_id, `⚠️ Your payment for bet \`${escapeMarkdownV2(bet.memo_id)}\` was received, but the amount was too low\\. Expected ${escapeMarkdownV2(expectedSOL)} SOL, but received ${escapeMarkdownV2(receivedSOL)} SOL\\. Your bet could not be processed\\.`, { parse_mode: 'MarkdownV2' });
                 processedSignaturesThisSession.add(signature); this._cleanSignatureCache(); // Add sig to cache
                 return { processed: false, reason: 'payment_amount_low' };
             } else if (transferAmount > expected + tolerance) {
                 console.warn(`${logPrefix}: Payment amount slightly higher than expected. Expected ${expected}, Got ${transferAmount}. Processing anyway.`);
             } else {
                  // Amount is within tolerance
             }


            // 5. Update bet status and record signature atomically
            const updateResult = await client.query(
                 'UPDATE bets SET status = $1, paid_tx_signature = $2 WHERE id = $3 AND status = $4', // Add status check again for safety
                 ['payment_verified', signature, bet.id, 'awaiting_payment']
            );

              // Check if the update actually happened
              if (updateResult.rowCount === 0) {
                  await client.query('ROLLBACK');
                  console.warn(`${logPrefix}: Failed to update bet status to 'payment_verified' inside TX. Status might have changed concurrently.`);
                  const finalCheck = await pool.query('SELECT status FROM bets WHERE id = $1', [bet.id]);
                  if(finalCheck.rows[0]?.status !== 'awaiting_payment'){
                     processedSignaturesThisSession.add(signature); this._cleanSignatureCache();
                  }
                  return { processed: false, reason: `update_failed_in_tx_final_status_${finalCheck.rows[0]?.status ?? 'not_found'}` };
              }


            // 6. Link wallet if payer found and different from current linked wallet (still inside TX)
            if (payerAddress) {
                 const currentLinkedWallet = await getLinkedWallet(bet.user_id);
                 if (!currentLinkedWallet || currentLinkedWallet !== payerAddress) {
                     // console.log(`${logPrefix}: Linking payer wallet ${payerAddress.slice(0,6)}... for user ${bet.user_id}.`);
                     await client.query(
                          `INSERT INTO wallets (user_id, wallet_address, last_used_at)
                           VALUES ($1, $2, NOW())
                           ON CONFLICT (user_id)
                           DO UPDATE SET wallet_address = EXCLUDED.wallet_address, last_used_at = NOW()`,
                          [String(bet.user_id), payerAddress]
                     );
                     walletCache.set(`wallet-${bet.user_id}`, { wallet: payerAddress, timestamp: Date.now() });
                     setTimeout(() => {
                         const current = walletCache.get(`wallet-${bet.user_id}`);
                         if (current && current.wallet === payerAddress && Date.now() - current.timestamp >= CACHE_TTL) {
                             walletCache.delete(`wallet-${bet.user_id}`);
                         }
                     }, CACHE_TTL + 1000);
                 }
            }

            // 7. Commit transaction
            await client.query('COMMIT');
            processedSignaturesThisSession.add(signature);
             this._cleanSignatureCache();
            return { processed: true }; // Indicate success

        } catch (error) {
            console.error(`${logPrefix}: Error during DB transaction for payment processing: ${error.message}`);
            try { await client.query('ROLLBACK'); } catch (rollbackError) { console.error(`${logPrefix}: Failed to rollback transaction after error: ${rollbackError.message}`); }
            try {
                 const statusCheck = await pool.query('SELECT status FROM bets WHERE id = $1', [bet.id]);
                 if (statusCheck.rows.length > 0 && statusCheck.rows[0].status === 'awaiting_payment') {
                      await pool.query(`UPDATE bets SET status = $1, processed_at = NOW() WHERE id = $2`, ['error_processing_db', bet.id]);
                 }
            } catch (updateErr) {
                 console.error(`${logPrefix}: Failed to set error status after rollback: ${updateErr.message}`);
            }
            error.betId = bet.id;
            return { processed: false, reason: `db_transaction_error: ${error.message}`, error: error };


        } finally {
            client.release(); // Release client back to pool
        }
    }


    // Helper to queue bet processing after payment verification
    async _queueBetProcessing(bet) {
        // console.log(`Queueing bet ID ${bet.id} (${bet.game_type}) for game logic processing.`); // Reduce noise
        await this.addPaymentJob({
            type: 'process_bet',
            betId: bet.id,
            priority: bet.priority || 0,
            chatId: bet.chat_id, // Pass info for potential error messages
            memoId: bet.memo_id
        });
    }


     // Helper method to clean up the signature cache
     _cleanSignatureCache() {
          if (processedSignaturesThisSession.size > MAX_PROCESSED_SIGNATURES) {
               console.warn(`[SigCache] Reached max size (${MAX_PROCESSED_SIGNATURES}). Clearing oldest ${Math.floor(MAX_PROCESSED_SIGNATURES / 2)} entries.`);
               const entriesToRemove = Math.floor(MAX_PROCESSED_SIGNATURES / 2);
               let i = 0;
               const sigs = Array.from(processedSignaturesThisSession);
               for (const sig of sigs) {
                   if (i >= entriesToRemove) break;
                   processedSignaturesThisSession.delete(sig);
                   i++;
               }
          }
     }

} // End GuaranteedPaymentProcessor Class

// Instantiate the processor
const paymentProcessor = new GuaranteedPaymentProcessor();
console.log("✅ Payment Processor instantiated.");

// --- End of Part 2b ---
// --- Start of Part 3a ---
// index.js - Part 3a (Corrected for Separate Payout Keys & Skewed Odds)
// --- VERSION: 2.6.0 ---

// (Code continues directly from the end of Part 2b)

// --- Payment Monitoring Loop ---
// (Includes stagger delay AND Enhanced Logging for Debugging RPC Errors)
let isMonitorRunning = false;
const botStartupTime = Math.floor(Date.now() / 1000);
let monitorIntervalId = null;

async function monitorPayments() {
    if (isMonitorRunning) return;
    if (!isFullyInitialized) return;

    isMonitorRunning = true;
    const mainStartTime = Date.now();
    let signaturesFoundThisCycle = 0;
    let signaturesQueuedThisCycle = 0;
    const WALLET_CHECK_DELAY_MS = 500; // Delay in ms between checking each wallet

    try {
        // Optional Throttling logic... (kept as before)
        const paymentQueueLoad = (paymentProcessor.highPriorityQueue.size + paymentProcessor.normalQueue.size +
                                  paymentProcessor.highPriorityQueue.pending + paymentProcessor.normalQueue.pending);
        const monitorThrottleMs = parseInt(process.env.MONITOR_THROTTLE_MS_PER_ITEM, 10);
        const maxMonitorThrottle = parseInt(process.env.MONITOR_MAX_THROTTLE_MS, 10);
        const throttleDelay = Math.min(maxMonitorThrottle, paymentQueueLoad * monitorThrottleMs);
        if (throttleDelay > 100) {
            console.log(`[Monitor Debug] Throttling monitor check due to queue load (${paymentQueueLoad}) for ${throttleDelay}ms.`);
            await new Promise(resolve => setTimeout(resolve, throttleDelay));
        }

        const monitoredWallets = [
             { envVar: 'CF_WALLET_ADDRESS',       type: 'coinflip', priority: 0 },
             { envVar: 'RACE_WALLET_ADDRESS',     type: 'race',     priority: 0 },
             { envVar: 'SLOTS_WALLET_ADDRESS',    type: 'slots',    priority: 0 },
             { envVar: 'ROULETTE_WALLET_ADDRESS', type: 'roulette', priority: 0 },
             { envVar: 'WAR_WALLET_ADDRESS',      type: 'war',      priority: 0 },
        ];

        let walletIndex = 0;
        for (const walletInfo of monitoredWallets) {
            const walletAddress = process.env[walletInfo.envVar];
            if (!walletAddress) {
                continue;
            }

            // Stagger delay
            if (walletIndex > 0) {
                await new Promise(resolve => setTimeout(resolve, WALLET_CHECK_DELAY_MS));
            }
            walletIndex++;

             // Jitter (kept as before)
             const jitter = Math.random() * (parseInt(process.env.MONITOR_WALLET_JITTER_MS, 10));
             if (jitter > 0) await new Promise(resolve => setTimeout(resolve, jitter));

            let signaturesForWallet = [];
            const fetchStartTime = Date.now(); // <<< Timing Start
            try {
                const fetchLimit = parseInt(process.env.MONITOR_FETCH_LIMIT, 10);
                const options = { limit: fetchLimit, commitment: 'confirmed' };
                const targetPublicKey = new PublicKey(walletAddress); // <<< Validate PublicKey creation

                // <<< Log before the call >>>
                console.log(`[Monitor Debug] Attempting getSignaturesForAddress: Wallet=${walletInfo.type} (${targetPublicKey.toBase58().slice(0,6)}...), Limit=${fetchLimit}, Commitment=${options.commitment}`);

                signaturesForWallet = await solanaConnection.getSignaturesForAddress(
                    targetPublicKey,
                    options
                );
                const fetchEndTime = Date.now(); // <<< Timing End Success
                // console.log(`[Monitor Debug] Success: getSignaturesForAddress for ${walletInfo.type} took ${fetchEndTime - fetchStartTime}ms. Found ${signaturesForWallet?.length ?? 0}.`);

                // --- Signature Processing Logic (remains the same) ---
                if (!signaturesForWallet || signaturesForWallet.length === 0) continue;
                signaturesFoundThisCycle += signaturesForWallet.length;
                const startupBufferSeconds = 600;
                const recentSignatures = signaturesForWallet.filter(sigInfo => {
                     if (sigInfo.err) { /* ... */ return false; }
                     if (sigInfo.blockTime && sigInfo.blockTime < (botStartupTime - startupBufferSeconds)) { return false; }
                     if (processedSignaturesThisSession.has(sigInfo.signature)) { return false; }
                     const jobKey = `monitor_payment:${sigInfo.signature}`;
                     if (paymentProcessor.activeProcesses.has(jobKey)){ return false; }
                     return true;
                 });
                if (recentSignatures.length === 0) continue;
                recentSignatures.reverse();
                for (const sigInfo of recentSignatures) {
                    if (processedSignaturesThisSession.has(sigInfo.signature)) continue;
                    const jobKey = `monitor_payment:${sigInfo.signature}`;
                    if (paymentProcessor.activeProcesses.has(jobKey)) continue;
                    signaturesQueuedThisCycle++;
                    await paymentProcessor.addPaymentJob({ type: 'monitor_payment', signature: sigInfo.signature, walletType: walletInfo.type, priority: walletInfo.priority });
                }
                // --- End Signature Processing ---

            } catch (error) {
                const fetchFailTime = Date.now(); // <<< Timing End Failure
                console.error(`[Monitor Debug] FAILURE during getSignaturesForAddress for ${walletInfo.type} (${walletAddress.slice(0,6)}...) after ${fetchFailTime - fetchStartTime}ms.`);
                // <<< Log the FULL error object >>>
                console.error('[Monitor Debug] Full Error Object:', error);
                performanceMonitor.logRequest(false);

                // Original logging (kept for context)
                if (error.message.includes('long-term storage')) {
                     console.warn(`[Monitor] RPC Node Storage Error for ${walletInfo.type} wallet. Consider checking RPC node health/history support.`);
                 } else if (!isRetryableError(error)) {
                      console.warn(`[Monitor] Non-retryable RPC error for ${walletInfo.type} wallet. Error: ${error.message}`);
                 } else {
                      console.warn(`[Monitor] Retryable RPC error for ${walletInfo.type} wallet. Error: ${error.message}. Connection library should handle retries.`);
                 }
            }
        } // End loop through wallets

    } catch (err) {
        console.error('❌ MonitorPayments Error in main try block:', err);
        performanceMonitor.logRequest(false);
    } finally {
        isMonitorRunning = false;
        const duration = Date.now() - mainStartTime;
        if (signaturesFoundThisCycle > 0 || duration > (parseInt(process.env.MONITOR_INTERVAL_SECONDS, 10) * 1000 / 2) ) {
             console.log(`[Monitor] Cycle completed in ${duration}ms. Found:${signaturesFoundThisCycle}. Queued:${signaturesQueuedThisCycle}.`);
        }
    }
}


// --- SOL Sending Function ---
/**
 * Sends SOL to a recipient, handling priority fees and confirmation.
 * Relies on RateLimitedConnection for underlying RPC calls.
 * @param {string | PublicKey} recipientPublicKey - The recipient's address.
 * @param {bigint} amountLamports - The amount to send in lamports (MUST be BigInt).
 * @param {'coinflip' | 'race' | 'slots' | 'roulette' | 'war'} payoutWalletType - Determines which private key ENV VAR to use.
 * @returns {Promise<{success: boolean, signature?: string}>} Result object. Throws error on failure.
 */
 // ** MODIFIED: Handles 5 distinct payout private keys **
async function sendSol(recipientPublicKey, amountLamports, payoutWalletType) {
    const operationId = `sendSol-${payoutWalletType}-${Date.now().toString().slice(-6)}`;

    let recipientPubKey;
    try {
        recipientPubKey = (typeof recipientPublicKey === 'string') ? new PublicKey(recipientPublicKey) : recipientPublicKey;
        if (!(recipientPubKey instanceof PublicKey)) throw new Error("Invalid recipient public key type");
    } catch (e) {
        console.error(`[${operationId}] ❌ ERROR: Invalid recipient address format: "${recipientPublicKey}". Error: ${e.message}`);
        throw new Error(`Invalid recipient address: ${e.message}`);
    }

    let amountToSend;
    try {
        amountToSend = BigInt(amountLamports);
        if (amountToSend <= 0n) {
            console.error(`[${operationId}] ❌ ERROR: Payout amount ${amountToSend} is zero or negative.`);
            throw new Error('Payout amount is zero or negative');
        }
    } catch (e) {
        console.error(`[${operationId}] ❌ ERROR: Failed to convert input amountLamports '${amountLamports}' to BigInt. Error: ${e.message}`);
        throw new Error(`Invalid payout amount format: ${e.message}`);
    }

    // ** MODIFIED: Select correct private key based on specific game type **
    let privateKeyEnvVar;
    switch (payoutWalletType) {
        case 'race':     privateKeyEnvVar = 'RACE_BOT_PRIVATE_KEY';     break;
        case 'slots':    privateKeyEnvVar = 'SLOTS_BOT_PRIVATE_KEY';    break;
        case 'roulette': privateKeyEnvVar = 'ROULETTE_BOT_PRIVATE_KEY'; break;
        case 'war':      privateKeyEnvVar = 'WAR_BOT_PRIVATE_KEY';      break;
        case 'coinflip': // Fallthrough intended
        default:         privateKeyEnvVar = 'CF_BOT_PRIVATE_KEY';       break; // Default to CF key
    }

    const privateKey = process.env[privateKeyEnvVar];
    if (!privateKey) {
        console.error(`[${operationId}] ❌ ERROR: Missing private key env var ${privateKeyEnvVar} for payout type ${payoutWalletType}.`);
        throw new Error(`Missing private key for ${payoutWalletType}`);
    }

    const amountSOL = Number(amountToSend) / LAMPORTS_PER_SOL;

    // Calculate Priority Fee dynamically based on settings
    const basePriorityFee = parseInt(process.env.PAYOUT_BASE_PRIORITY_FEE_MICROLAMPORTS, 10);
    const maxPriorityFee = parseInt(process.env.PAYOUT_MAX_PRIORITY_FEE_MICROLAMPORTS, 10);
    const calculatedFee = Math.floor(Number(amountToSend) * PRIORITY_FEE_RATE);
    let priorityFeeMicroLamports = Math.max(basePriorityFee, Math.min(calculatedFee, maxPriorityFee));

    if (isNaN(priorityFeeMicroLamports)) {
         console.error(`[${operationId}] ❌ ERROR: NaN detected during priority fee calculation! base=${basePriorityFee}, max=${maxPriorityFee}, rate=${PRIORITY_FEE_RATE}, amount=${amountToSend}, calc=${calculatedFee}, final=${priorityFeeMicroLamports}`);
         console.warn(`[${operationId}] NaN priority fee, defaulting to base fee: ${basePriorityFee}`);
         priorityFeeMicroLamports = basePriorityFee;
    }

    try {
        const payerWallet = Keypair.fromSecretKey(bs58.decode(privateKey));

        const latestBlockhash = await solanaConnection.getLatestBlockhash('confirmed');
        if (!latestBlockhash || !latestBlockhash.blockhash || !latestBlockhash.lastValidBlockHeight) {
            throw new Error('Failed to get valid latest blockhash object from RPC.');
        }

        const transaction = new Transaction({
            recentBlockhash: latestBlockhash.blockhash,
            feePayer: payerWallet.publicKey
        });

        transaction.add(
            ComputeBudgetProgram.setComputeUnitPrice({ microLamports: priorityFeeMicroLamports })
        );
        // transaction.add(ComputeBudgetProgram.setComputeUnitLimit({ units: 200000 })); // Optional

        transaction.add(
            SystemProgram.transfer({
                fromPubkey: payerWallet.publicKey,
                toPubkey: recipientPubKey,
                lamports: amountToSend
            })
        );

        const confirmationTimeoutMs = parseInt(process.env.PAYOUT_CONFIRM_TIMEOUT_MS, 10);

        const signature = await sendAndConfirmTransaction(
            solanaConnection,
            transaction,
            [payerWallet],
            {
                commitment: 'confirmed',
                skipPreflight: false,
                maxRetries: parseInt(process.env.SCT_MAX_RETRIES || '3', 10),
                preflightCommitment: 'confirmed',
                lastValidBlockHeight: latestBlockhash.lastValidBlockHeight,
            }
        );

        // ** FORMATTING APPLIED ** - Use toFixed(3)
        console.log(`[${operationId}] SUCCESS! ✅ Sent ${amountSOL.toFixed(3)} SOL to ${recipientPubKey.toBase58()} from key ${privateKeyEnvVar}. TX: ${signature.slice(0,10)}...`);
        return { success: true, signature };

    } catch (error) {
        // Error classification and re-throw logic (remains the same)
        console.error(`[${operationId}] ❌ SEND FAILED from key ${privateKeyEnvVar}. Error message:`, error.message);
        if (error.logs) {
             console.error(`[${operationId}] Simulation Logs (if available, last 10):`);
             error.logs.slice(-10).forEach(log => console.error(`   -> ${log}`));
        }
        const errorMsg = error.message.toLowerCase();
        let returnError = error.message;
        if (errorMsg.includes('insufficient lamports') || errorMsg.includes('insufficient funds')) { returnError = 'Insufficient funds in payout wallet.'; }
        else if (errorMsg.includes('blockhash not found') || errorMsg.includes('block height exceeded') || errorMsg.includes('slot advance behavior')) { returnError = 'Transaction expired (blockhash invalid/expired). Retryable.'; }
        else if (errorMsg.includes('transaction was not confirmed') || errorMsg.includes('timed out waiting')) { returnError = `Transaction confirmation timeout (${confirmationTimeoutMs / 1000}s). May succeed later. Retryable.`;}
        else if (errorMsg.includes('custom program error') || errorMsg.includes('invalid account data') || errorMsg.includes('account not found')) { returnError = `Permanent chain error: ${error.message}`; }
        else if (isRetryableError(error)) { returnError = `Temporary network/RPC error: ${error.message}. Retryable.`; }
        else { returnError = `Send/Confirm error: ${error.message}`; }
        error.retryable = isRetryableError(error);
        error.message = returnError;
        throw error;
    }
}


// --- Game Processing Logic ---

// Routes a paid bet to the correct game handler after payment verification
async function processPaidBet(bet) {
    // console.log(`⚙️ Processing game for bet ${bet.id} (${bet.game_type}, ${bet.memo_id})`); // Reduce noise
    let client;
    try {
        // Start a transaction and lock the bet row
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
            return; // Bet already processed or in error state
        }

        // Update status to 'processing_game' within the transaction
        await client.query(
            'UPDATE bets SET status = $1, processed_at = NOW() WHERE id = $2', // Update processed_at here too
            ['processing_game', bet.id]
        );
        await client.query('COMMIT'); // Commit status change and release lock

        // Call the appropriate game handler *after* releasing the lock
        // Add a try-catch around the game handler call itself
        try {
            // ** Get skew parameters from environment **
            const cfEdge = parseFloat(process.env.CF_HOUSE_EDGE || '0.65');
            const raceEdge = parseFloat(process.env.RACE_HOUSE_EDGE || '0.50');
            const slotsHiddenEdge = parseFloat(process.env.SLOTS_HIDDEN_EDGE || '0.10');
            const rouletteHiddenEdge = parseFloat(process.env.ROULETTE_HIDDEN_EDGE || '0.65');
            // War skew is internal to its handler

            if (bet.game_type === 'coinflip') {
                await handleCoinflipGame(bet, cfEdge); // Pass edge for auto-win check
            } else if (bet.game_type === 'race') {
                await handleRaceGame(bet, raceEdge); // Pass edge for auto-win check
            } else if (bet.game_type === 'slots') {
                await handleSlotsGame(bet, slotsHiddenEdge); // Pass hidden edge
            } else if (bet.game_type === 'roulette') {
                await handleRouletteGame(bet, rouletteHiddenEdge); // Pass hidden edge
            } else if (bet.game_type === 'war') {
                await handleWarGame(bet); // Skew logic is internal
            } else {
                console.error(`❌ Unknown game type '${bet.game_type}' for bet ${bet.id}`);
                await updateBetStatus(bet.id, 'error_unknown_game');
            }
        } catch (gameError) {
            console.error(`❌ Error executing game logic for ${bet.game_type} bet ${bet.id}:`, gameError);
            // Mark bet with game processing error status
            await updateBetStatus(bet.id, 'error_game_logic');
            // Notify user potentially
             // ** MD ESCAPE APPLIED ** - Escaped `\` `.` twice
            await safeSendMessage(bet.chat_id, `⚠️ An error occurred while running the game for your bet \`${escapeMarkdownV2(bet.memo_id)}\`\\. Please contact support\\.`, { parse_mode: 'MarkdownV2'});
        }
    } catch (error) {
        console.error(`❌ Error during game processing setup for bet ${bet.id} (${bet.memo_id}):`, error.message);
        if (client) {
            try { await client.query('ROLLBACK'); } catch (rbError) { console.error("Rollback failed:", rbError); }
        }
        // Mark bet with error status - do this outside the failed transaction
         const currentStatusResult = await pool.query('SELECT status FROM bets WHERE id = $1', [bet.id]);
         if (currentStatusResult.rows[0]?.status === 'payment_verified' || currentStatusResult.rows[0]?.status === 'processing_game') {
             await updateBetStatus(bet.id, 'error_processing_setup');
         }
    } finally {
        if (client) client.release(); // Ensure client is always released
    }
}

// --- Utility Functions (Existing) ---

async function getUserDisplayName(chat_id, user_id) {
     try {
         const chatMember = await bot.getChatMember(chat_id, user_id);
         const user = chatMember.user;
         let name = user.first_name || `User_${String(user_id).slice(-4)}`;
         if(user.username) name = `@${user.username}`;
         return escapeMarkdownV2(name);
     } catch (e) {
          if (e.response && e.response.statusCode === 400 && e.message.includes('user not found')) {
               // console.warn(`Couldn't get username/name for user ${user_id} in chat ${chat_id}: User not found.`);
          } else if (e.response && e.response.statusCode === 403) {
               // console.warn(`Couldn't get username/name for user ${user_id} in chat ${chat_id}: Bot blocked or no permission.`);
          } else {
               // console.warn(`Couldn't get username/name for user ${user_id} in chat ${chat_id}:`, e.message);
          }
         const fallbackName = `User_${String(user_id).slice(-4)}`;
         return escapeMarkdownV2(fallbackName);
     }
}


// --- Game Logic Implementation (Modified for Skewed Odds/Full Payouts) ---

// ** MODIFIED: Accepts edge for auto-win, pays full 2x on win **
async function handleCoinflipGame(bet, cfEdge) {
    const { id: betId, user_id, chat_id, bet_details, expected_lamports, memo_id } = bet;
    const choice = bet_details.choice;
    const logPrefix = `CF Bet ${betId} (${memo_id.slice(0, 6)}...)`;

    // --- Skewed Outcome ---
    // 1. Check for house auto-win based on CF_HOUSE_EDGE
    const houseAutoWins = Math.random() < cfEdge;

    let result;
    let win;

    if (houseAutoWins) {
        console.log(`${logPrefix}: House auto-win triggered (Edge: ${cfEdge*100}%).`);
        win = false;
        result = (choice === 'heads') ? 'tails' : 'heads'; // Force loss
    } else {
        // Fair flip if house doesn't auto-win
        result = (Math.random() < 0.5) ? 'heads' : 'tails';
        win = (result === choice);
    }
    // --- End Skewed Outcome ---

    let payoutLamports = 0n;
    if (win) {
        // Calculate full payout (2x stake)
        payoutLamports = BigInt(expected_lamports) * 2n;
    }

    const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
    const displayName = await getUserDisplayName(chat_id, user_id);

    // Process win
    if (win && payoutLamports > 0n) {
        const winnerAddress = await getLinkedWallet(user_id);
        if (!winnerAddress) {
            await updateBetStatus(betId, 'completed_win_no_wallet');
             // ** MD ESCAPE & DECIMAL APPLIED ** - Escaped `\` `(` `)` `!` `.` twice using toFixed(3)
            await safeSendMessage(chat_id,
                `🎉 ${displayName}, you won the coinflip \\(Result: *${escapeMarkdownV2(result)}*\\) but have no wallet linked\\!\n` +
                `Your payout of ${escapeMarkdownV2(payoutSOL.toFixed(3))} SOL is waiting\\. Place another bet \\(any amount\\) to link your wallet and receive pending payouts\\.`,
                { parse_mode: 'MarkdownV2' }
            );
            return;
        }
        // Wallet linked, proceed to payout
        try {
             const statusUpdated = await updateBetStatus(betId, 'processing_payout');
             if (!statusUpdated) {
                  console.error(`${logPrefix}: CRITICAL! Failed to update status from 'processing_game' to 'processing_payout' before queueing! Aborting payout queue.`);
                   // ** MD ESCAPE APPLIED ** - Escaped `\` `.` twice
                  await safeSendMessage(chatId, `⚠️ Internal error preparing your payout for bet \`${escapeMarkdownV2(memo_id)}\`\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
                  await updateBetStatus(betId, 'error_payout_status_update');
                  return;
             }
              // ** MD ESCAPE & DECIMAL APPLIED ** - Escaped `\` `!` `.` three times using toFixed(3)
            await safeSendMessage(chat_id,
                `🎉 ${displayName}, you won ${escapeMarkdownV2(payoutSOL.toFixed(3))} SOL\\!\n` +
                `Result: *${escapeMarkdownV2(result)}*\n\n` +
                `💸 Processing payout to your linked wallet\\.\\.\\.`,
                { parse_mode: 'MarkdownV2' }
            );
             // Queue the payout job - Use correct 'coinflip' type for payout key selection
            await paymentProcessor.addPaymentJob({
                type: 'payout',
                betId,
                recipient: winnerAddress,
                amount: payoutLamports.toString(), // Full payout
                gameType: 'coinflip', // <<< Ensures correct payout key is selected
                priority: 2,
                chatId: chat_id,
                displayName: displayName,
                memoId: memo_id,
            });
        } catch (e) {
            console.error(`${logPrefix}: Error preparing/queueing payout info:`, e);
             await updateBetStatus(betId, 'error_payout_preparation');
              // ** MD ESCAPE APPLIED ** - Escaped `\` `.` twice
            await safeSendMessage(chat_id, `⚠️ Error occurred while processing your coinflip win for bet \`${escapeMarkdownV2(memo_id)}\`\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
        }
    } else { // Process loss
        await updateBetStatus(betId, 'completed_loss');
         // ** MD ESCAPE APPLIED ** - Escaped `\` `!` `.` `!`
        await safeSendMessage(chat_id,
            `❌ ${displayName}, you lost the coinflip\\!\n` +
            `You guessed *${escapeMarkdownV2(choice)}* but the result was *${escapeMarkdownV2(result)}*\\. Better luck next time\\!`,
            { parse_mode: 'MarkdownV2' }
        );
    }
}


// ** MODIFIED: Accepts edge for auto-win, uses skewed weights, pays FINALIZED full odds on win **
async function handleRaceGame(bet, raceEdge) { // raceEdge comes from process.env.RACE_HOUSE_EDGE
    const { id: betId, user_id, chat_id, bet_details, expected_lamports, memo_id } = bet;
    const chosenHorseName = bet_details.horse;
    const logPrefix = `Race Bet ${betId} (${memo_id.slice(0, 6)}...)`;

    // ** MODIFIED: Final odds based on latest request **
    const horses = [
         { name: 'Yellow', emoji: '🟡', odds: 2.0 }, // Exactly 2.0
         { name: 'Orange', emoji: '🟠', odds: 3.0 },
         { name: 'Blue',   emoji: '🔵', odds: 4.0 },
         { name: 'Cyan',   emoji: '💧', odds: 5.0 },
         { name: 'White',  emoji: '⚪️', odds: 6.0 },
         { name: 'Red',    emoji: '🔴', odds: 7.0 },
         { name: 'Black',  emoji: '⚫️', odds: 8.0 },
         { name: 'Pink',   emoji: '🌸', odds: 9.0 },
         { name: 'Purple', emoji: '🟣', odds: 10.0 },
         { name: 'Green',  emoji: '🟢', odds: 15.0 }, // Changed to 15.0
         { name: 'Silver', emoji: '💎', odds: 25.0 } // Changed to 25.0
    ];
    // Internal weights for visual winner selection remain the same (skewed)
    const internalWeights = [
        { name: 'Yellow', weight: 650 }, { name: 'Orange', weight: 180 },
        { name: 'Blue',   weight: 90 },  { name: 'Cyan',   weight: 40 },
        { name: 'White',  weight: 20 },  { name: 'Red',    weight: 10 },
        { name: 'Black',  weight: 5 },   { name: 'Pink',   weight: 2 },
        { name: 'Purple', weight: 1 },   { name: 'Green',  weight: 1 },
        { name: 'Silver', weight: 1 }
    ];
    const totalWeight = internalWeights.reduce((sum, h) => sum + h.weight, 0);

    // --- Skewed Outcome ---
    const houseAutoWins = Math.random() < raceEdge;
    let winningHorse = null;
    let playerWins = false;
    const pickVisualWinner = () => { /* ... pickVisualWinner logic remains the same ... */
        let randomWeight = Math.random() * totalWeight;
        for (const horse of internalWeights) {
            if (randomWeight < horse.weight) {
                return horses.find(h => h.name === horse.name);
            }
            randomWeight -= horse.weight;
        }
        return horses[0]; // Fallback
    };

    if (houseAutoWins) {
        console.log(`${logPrefix}: House auto-win triggered (Edge: ${raceEdge*100}%).`);
        winningHorse = pickVisualWinner();
        playerWins = false;
    } else {
        winningHorse = pickVisualWinner();
        playerWins = (chosenHorseName.toLowerCase() === winningHorse.name.toLowerCase());
    }
     // --- End Skewed Outcome ---

    let payoutLamports = 0n;
    if (playerWins) {
        // Calculate full payout based on FINAL DISPLAYED odds
        const winningHorseInfo = horses.find(h => h.name.toLowerCase() === winningHorse.name.toLowerCase());
        if (winningHorseInfo) {
             payoutLamports = (BigInt(expected_lamports) * BigInt(Math.round(winningHorseInfo.odds * 100))) / 100n;
        } else {
             console.error(`${logPrefix}: Could not find winning horse info for payout calculation? Winner: ${winningHorse?.name}`);
        }
    }

    const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
    const displayName = await getUserDisplayName(chat_id, user_id);

    // Send race commentary messages (remains the same)
    try { /* ... commentary sending ... */
        await safeSendMessage(chat_id, `🐎 Race starting for bet \`${escapeMarkdownV2(memo_id)}\`\\! ${displayName} bet on *${escapeMarkdownV2(chosenHorseName)}*\\!`, { parse_mode: 'MarkdownV2' });
        await new Promise(resolve => setTimeout(resolve, 2000));
        await safeSendMessage(chat_id, "🚦 And they're off\\!", { parse_mode: 'MarkdownV2' });
        await new Promise(resolve => setTimeout(resolve, 3000));
        await safeSendMessage(chat_id, `🏆 The winner is\\.\\.\\. ${winningHorse.emoji} *${escapeMarkdownV2(winningHorse.name)}*\\! 🏆`, { parse_mode: 'MarkdownV2' });
        await new Promise(resolve => setTimeout(resolve, 1000));
    } catch (e) { console.error(`${logPrefix}: Error sending race commentary:`, e); }

    // Process win/loss (remains the same, uses calculated payoutLamports)
    if (playerWins && payoutLamports > 0n) {
        const winnerAddress = await getLinkedWallet(user_id);
        if (!winnerAddress) { /* ... no wallet message ... */
            await updateBetStatus(betId, 'completed_win_no_wallet');
            await safeSendMessage(chat_id,
                `🎉 ${displayName}, your horse *${escapeMarkdownV2(chosenHorseName)}* won the race\\!\n`+
                `Your payout of ${escapeMarkdownV2(payoutSOL.toFixed(3))} SOL is waiting\\. Place another bet \\(any amount\\) to link your wallet and receive pending payouts\\.`,
                { parse_mode: 'MarkdownV2' }
            );
            return;
        }
        try {
             const statusUpdated = await updateBetStatus(betId, 'processing_payout');
             if (!statusUpdated) { /* ... status update error handling ... */ return; }
              await safeSendMessage(chat_id,
                   `🎉 ${displayName}, your horse *${escapeMarkdownV2(chosenHorseName)}* won\\!\n` +
                   `Payout: ${escapeMarkdownV2(payoutSOL.toFixed(3))} SOL\n\n` +
                   `💸 Processing payout to your linked wallet\\.\\.\\.`,
                   { parse_mode: 'MarkdownV2' }
              );
              await paymentProcessor.addPaymentJob({
                  type: 'payout', betId, recipient: winnerAddress,
                  amount: payoutLamports.toString(), gameType: 'race',
                  priority: 2, chatId: chat_id, displayName: displayName, memoId: memo_id,
              });
        } catch (e) { /* ... payout prep error handling ... */ }
    } else { // Loss
        await updateBetStatus(betId, 'completed_loss');
         const lossReason = houseAutoWins
             ? `The house took the win this time\\!`
             : `Your horse *${escapeMarkdownV2(chosenHorseName)}* lost the race\\! Winner: ${winningHorse.emoji} *${escapeMarkdownV2(winningHorse.name)}*\\.`;
        await safeSendMessage(chat_id,
            `❌ ${displayName}, ${lossReason} Better luck next time\\!`,
            { parse_mode: 'MarkdownV2' }
        );
    }
}

// --- Slots Game Logic ---

// ** MODIFIED: Adjusted weights heavily for rarity, payouts adjusted slightly **
const SLOTS_SYMBOLS = {
    // Payout: { num_matches: payout_multiplier_on_stake }
    CHERRY: { emoji: '🍒', weight: 30, payout: { 3: 3 } },  // 3 cherries = 3x stake (+ stake back = 4x total)
    ORANGE: { emoji: '🍊', weight: 20, payout: { 3: 8 } },  // 3 oranges = 8x stake
    BAR:    { emoji: '🍫', weight: 5, payout: { 3: 40 } }, // 3 bars = 40x stake (rarer)
    SEVEN:  { emoji: '7️⃣', weight: 10, payout: { /* Special */ } }, // Pays 2x stake if on first reel
    TRIPLE_SEVEN: { emoji: '🎰', weight: 1, payout: { 3: 750 } },// 3 Jackpot = 750x stake (very rare)
    BLANK:  { emoji: '➖', weight: 50, payout: {} }, // Increased blanks significantly
};
const slotsTotalWeight = Object.values(SLOTS_SYMBOLS).reduce((sum, s) => sum + s.weight, 0); // Recalculate total weight
const SLOTS_REEL_LENGTH = 3;

// Helper to create a weighted reel based on new weights
function createReel() {
    const reel = [];
    for (const symbolKey in SLOTS_SYMBOLS) {
        const symbol = SLOTS_SYMBOLS[symbolKey];
        for (let i = 0; i < symbol.weight; i++) {
            reel.push(symbolKey);
        }
    }
    return reel;
}
const reelStrip = createReel(); // Create the reel strip with new weights

// Helper to get a random symbol from the reel
function spinReel(strip) {
    const randomIndex = Math.floor(Math.random() * strip.length);
    return strip[randomIndex];
}

// ** MODIFIED: Accepts hidden edge, pays full paytable amount on win **
async function handleSlotsGame(bet, slotsHiddenEdge) {
    const { id: betId, user_id, chat_id, expected_lamports, memo_id } = bet;
    const logPrefix = `Slots Bet ${betId} (${memo_id.slice(0, 6)}...)`;
    const betAmountLamports = BigInt(expected_lamports);

    // --- Skewed Outcome ---
    // 1. Check for house force-loss based on SLOTS_HIDDEN_EDGE
    const houseForceLoss = Math.random() < slotsHiddenEdge;

    let results = [];
    if (houseForceLoss) {
        console.log(`${logPrefix}: House hidden edge triggered (Edge: ${slotsHiddenEdge*100}%). Forcing loss.`);
        // Force a non-winning combination
        results = ['BLANK', 'CHERRY', 'ORANGE']; // Example guaranteed loss (no 3-in-a-row, no first reel 7)
        if (Math.random() < 0.5) results[0] = 'ORANGE'; // Avoid first reel 7 reliably
    } else {
        // Fair spin based on (skewed by weights) reelStrip
        for (let i = 0; i < SLOTS_REEL_LENGTH; i++) {
            results.push(spinReel(reelStrip));
        }
    }
    // --- End Skewed Outcome ---

    let resultEmojis = results.map(key => SLOTS_SYMBOLS[key]?.emoji || '❓').join(' \\| ');

    // --- Determine Win (based on paytable) ---
    let winMultiplier = 0; // Multiplier on STAKE
    let winDescription = "No Win";

    if (results.every(s => s === 'TRIPLE_SEVEN')) {
        winMultiplier = SLOTS_SYMBOLS.TRIPLE_SEVEN.payout[3]; // 750
        winDescription = "777 JACKPOT!!!";
    }
    else if (results.every(s => s === 'BAR')) {
        winMultiplier = SLOTS_SYMBOLS.BAR.payout[3]; // 40
        winDescription = "Triple BAR!";
    }
    else if (results.every(s => s === 'ORANGE')) {
        winMultiplier = SLOTS_SYMBOLS.ORANGE.payout[3]; // 8
        winDescription = "Triple Orange!";
    }
    else if (results.every(s => s === 'CHERRY')) {
        winMultiplier = SLOTS_SYMBOLS.CHERRY.payout[3]; // 3
        winDescription = "Triple Cherry!";
    }
    else if (results[0] === 'SEVEN' && winMultiplier === 0) {
        winMultiplier = 2; // Payout for first reel 7 is now 2x Stake
        winDescription = "Seven on First Reel!";
    }

    // --- Calculate Payout (Full Amount) ---
    let payoutLamports = 0n;
    if (winMultiplier > 0) {
         // Full Payout = Stake + (Stake * Multiplier)
         payoutLamports = betAmountLamports + (betAmountLamports * BigInt(winMultiplier));
    }

    const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
    const displayName = await getUserDisplayName(chat_id, user_id);
    const win = payoutLamports > betAmountLamports; // Win only if payout > original bet

    // --- Send Result Message ---
      // ** MD ESCAPE APPLIED ** - Escaped `\` `!`
      let resultMessage = `🎰 *Slots Result* for ${displayName} \\!\n\n` +
                           `*Result:* ${resultEmojis}\n\n`;


    if (win) {
         // ** MD ESCAPE & DECIMAL APPLIED ** - Escaped `\` `!` using toFixed(3)
        resultMessage += `🎉 *${escapeMarkdownV2(winDescription)}* You won ${escapeMarkdownV2(payoutSOL.toFixed(3))} SOL\\!`;
         // console.log(`${logPrefix}: ${displayName} WON ${payoutSOL.toFixed(3)} SOL! Result: ${resultEmojis}, Desc: ${winDescription}`);
    } else {
          // ** MD ESCAPE APPLIED ** - Escaped `\` `.` `\` `!`
        resultMessage += `❌ No win this time\\. Better luck next spin\\!`;
        // console.log(`${logPrefix}: ${displayName} LOST. Result: ${resultEmojis}`);
    }
    await safeSendMessage(chat_id, resultMessage, { parse_mode: 'MarkdownV2' });


    // --- Handle Payout or Update Status ---
    if (win) {
        const winnerAddress = await getLinkedWallet(user_id);
        if (!winnerAddress) {
            await updateBetStatus(betId, 'completed_win_no_wallet');
             // ** MD ESCAPE & DECIMAL APPLIED ** - Escaped `\` `.` twice
             await safeSendMessage(chat_id, `Your payout of ${escapeMarkdownV2(payoutSOL.toFixed(3))} SOL is waiting\\. Place another bet to link your wallet\\.`, { parse_mode: 'MarkdownV2' });
            return;
        }
        try {
             const statusUpdated = await updateBetStatus(betId, 'processing_payout');
             if (!statusUpdated) {
                  console.error(`${logPrefix}: CRITICAL! Failed to update status from 'processing_game' to 'processing_payout' before queueing! Aborting payout queue.`);
                    // ** MD ESCAPE APPLIED ** - Escaped `\` `.` twice
                  await safeSendMessage(chatId, `⚠️ Internal error preparing your payout for bet \`${escapeMarkdownV2(memo_id)}\`\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
                  await updateBetStatus(betId, 'error_payout_status_update');
                  return;
             }
             // Queue payout job - Use correct 'slots' type for payout key selection
             await paymentProcessor.addPaymentJob({
                 type: 'payout',
                 betId,
                 recipient: winnerAddress,
                 amount: payoutLamports.toString(), // Full payout
                 gameType: 'slots', // <<< Ensures correct payout key is selected
                 priority: 1,
                 chatId: chat_id,
                 displayName: displayName,
                 memoId: memo_id,
             });
        } catch (e) {
            console.error(`${logPrefix}: Error preparing/queueing slots payout info:`, e);
            await updateBetStatus(betId, 'error_payout_preparation');
             // ** MD ESCAPE APPLIED ** - Escaped `\` `.` twice
            await safeSendMessage(chat_id, `⚠️ Error occurred while processing your slots win for bet \`${escapeMarkdownV2(memo_id)}\`\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
        }
    } else { // Loss
        await updateBetStatus(betId, 'completed_loss');
    }
}

// --- Roulette Game Logic ---

const ROULETTE_NUMBERS = { /* ... remains the same ... */
     0: { color: 'green', dozen: null, column: null },
     1: { color: 'red', dozen: 1, column: 1 }, 2: { color: 'black', dozen: 1, column: 2 },
     3: { color: 'red', dozen: 1, column: 3 }, 4: { color: 'black', dozen: 1, column: 1 }, 5: { color: 'red', dozen: 1, column: 2 },
     6: { color: 'black', dozen: 1, column: 3 }, 7: { color: 'red', dozen: 1, column: 1 }, 8: { color: 'black', dozen: 1, column: 2 },
     9: { color: 'red', dozen: 1, column: 3 }, 10: { color: 'black', dozen: 1, column: 1 }, 11: { color: 'black', dozen: 1, column: 2 },
     12: { color: 'red', dozen: 1, column: 3 }, 13: { color: 'black', dozen: 2, column: 1 }, 14: { color: 'red', dozen: 2, column: 2 },
     15: { color: 'black', dozen: 2, column: 3 }, 16: { color: 'red', dozen: 2, column: 1 }, 17: { color: 'black', dozen: 2, column: 2 },
     18: { color: 'red', dozen: 2, column: 3 }, 19: { color: 'red', dozen: 2, column: 1 }, 20: { color: 'black', dozen: 2, column: 2 },
     21: { color: 'red', dozen: 2, column: 3 }, 22: { color: 'black', dozen: 2, column: 1 }, 23: { color: 'red', dozen: 2, column: 2 },
     24: { color: 'black', dozen: 2, column: 3 }, 25: { color: 'red', dozen: 3, column: 1 }, 26: { color: 'black', dozen: 3, column: 2 },
     27: { color: 'red', dozen: 3, column: 3 }, 28: { color: 'black', dozen: 3, column: 1 }, 29: { color: 'black', dozen: 3, column: 2 },
     30: { color: 'red', dozen: 3, column: 3 }, 31: { color: 'black', dozen: 3, column: 1 }, 32: { color: 'red', dozen: 3, column: 2 },
     33: { color: 'black', dozen: 3, column: 3 }, 34: { color: 'red', dozen: 3, column: 1 }, 35: { color: 'black', dozen: 3, column: 2 },
     36: { color: 'red', dozen: 3, column: 3 }
};
// Payout odds (N:1) - Standard Fair Odds
const ROULETTE_PAYOUT_ODDS = { /* ... remains the same ... */
     S: 35, R: 1, B: 1, E: 1, O: 1, L: 1, H: 1, D1: 2, D2: 2, D3: 2, C1: 2, C2: 2, C3: 2,
};

// ** MODIFIED: Accepts hidden edge, pays full standard odds on win **
async function handleRouletteGame(bet, rouletteHiddenEdge) {
    const { id: betId, user_id, chat_id, bet_details, expected_lamports, memo_id } = bet;
    const logPrefix = `Roulette Bet ${betId} (${memo_id.slice(0, 6)}...)`;
    const userBets = bet_details.bets;

    // --- Skewed Outcome ---
    let winningNumber = Math.floor(Math.random() * 37); // Initial fair spin (0-36)
    const houseForceZero = Math.random() < rouletteHiddenEdge; // Check if house forces '0'

    if (houseForceZero) {
        console.log(`${logPrefix}: House hidden edge triggered (Edge: ${rouletteHiddenEdge*100}%). Forcing result to 0.`);
        winningNumber = 0; // Force the result
    }
    // --- End Skewed Outcome ---

    const winningInfo = ROULETTE_NUMBERS[winningNumber];
    const winningColorEmoji = winningInfo.color === 'red' ? '🔴' : winningInfo.color === 'black' ? '⚫️' : '🟢';

    // --- Calculate Winnings (Full Odds) ---
    let totalPayoutLamports = 0n; // Includes stake return for winning bets
    let winningBetDescriptions = [];

    for (const betKey in userBets) {
        const betAmountLamports = BigInt(userBets[betKey]);
        if (betAmountLamports <= 0n) continue;

        let betWins = false;
        let payoutOdds = 0; // N in N:1

        const betTypeCode = betKey.charAt(0);
        if (betTypeCode === 'S') { payoutOdds = ROULETTE_PAYOUT_ODDS[betTypeCode] ?? 0; }
        else if (betTypeCode === 'D' || betTypeCode === 'C') { payoutOdds = ROULETTE_PAYOUT_ODDS[betKey] ?? 0; }
        else { payoutOdds = ROULETTE_PAYOUT_ODDS[betTypeCode] ?? 0; }

        const betValue = betKey.length > 1 ? betKey.substring(1) : undefined;

        switch (betTypeCode) {
            case 'S': if (winningNumber === parseInt(betValue, 10)) betWins = true; break;
            case 'R': if (winningInfo.color === 'red') betWins = true; break;
            case 'B': if (winningInfo.color === 'black') betWins = true; break;
            case 'E': if (winningNumber !== 0 && winningNumber % 2 === 0) betWins = true; break;
            case 'O': if (winningNumber !== 0 && winningNumber % 2 !== 0) betWins = true; break;
            case 'L': if (winningNumber >= 1 && winningNumber <= 18) betWins = true; break;
            case 'H': if (winningNumber >= 19 && winningNumber <= 36) betWins = true; break;
            case 'D': if (winningInfo.dozen === parseInt(betValue, 10)) betWins = true; break;
            case 'C': if (winningInfo.column === parseInt(betValue, 10)) betWins = true; break;
        }

        if (betWins) {
            // Calculate Full Payout = Stake + (Stake * Odds)
            const payoutForBet = betAmountLamports + (betAmountLamports * BigInt(payoutOdds));
            totalPayoutLamports += payoutForBet;
            // ** MD ESCAPE & DECIMAL APPLIED ** - Escaped `\` `+` `(` `)` using toFixed(3)
            const winAmountSOL = (Number(payoutForBet) / LAMPORTS_PER_SOL).toFixed(3);
            winningBetDescriptions.push(`\`${betKey}\` \\(\\+${escapeMarkdownV2(winAmountSOL)} SOL\\)`);
        }
    }

    // --- Determine final outcome and message ---
    const payoutLamports = totalPayoutLamports;
    const win = payoutLamports > 0n; // Considered a win if anything is paid out
    const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
    const displayName = await getUserDisplayName(chat_id, user_id);

    // --- Send Result Message ---
      // ** MD ESCAPE APPLIED ** - Escaped `\` `!` `\` `(` `)`
    let resultMessage = `⚪️ *Roulette Result* for ${displayName} \\!\n\n` +
                         `*Winning Number:* ${winningColorEmoji} *${escapeMarkdownV2(winningNumber)}* \\(${escapeMarkdownV2(winningInfo.color)}\\)\n\n`;

    if (win) {
        // ** MD ESCAPE & DECIMAL APPLIED ** - Escaped `\` `!` using toFixed(3)
        resultMessage += `🎉 *You won\\!* Total Payout: ${escapeMarkdownV2(payoutSOL.toFixed(3))} SOL\n`;
        if (winningBetDescriptions.length > 0) {
            resultMessage += `Winning Bets: ${winningBetDescriptions.join(', ')}\n`; // Items are already escaped
        }
    } else {
         // ** MD ESCAPE APPLIED ** - Escaped `\` `.` `\` `!`
        resultMessage += `❌ No winning bets this time\\. Better luck next spin\\!`;
    }
    await safeSendMessage(chat_id, resultMessage, { parse_mode: 'MarkdownV2' });

    // --- Handle Payout or Update Status ---
    if (win) {
        const winnerAddress = await getLinkedWallet(user_id);
        if (!winnerAddress) {
            await updateBetStatus(betId, 'completed_win_no_wallet');
             // ** MD ESCAPE & DECIMAL APPLIED ** - Escaped `\` `.` twice
            await safeSendMessage(chat_id, `Your payout of ${escapeMarkdownV2(payoutSOL.toFixed(3))} SOL is waiting\\. Place another bet to link your wallet\\.`, { parse_mode: 'MarkdownV2' });
            return;
        }
        try {
            const statusUpdated = await updateBetStatus(betId, 'processing_payout');
            if (!statusUpdated) {
                console.error(`${logPrefix}: CRITICAL! Failed to update status from 'processing_game' to 'processing_payout' before queueing! Aborting payout queue.`);
                 // ** MD ESCAPE APPLIED ** - Escaped `\` `.` twice
                await safeSendMessage(chatId, `⚠️ Internal error preparing your payout for bet \`${escapeMarkdownV2(memo_id)}\`\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
                await updateBetStatus(betId, 'error_payout_status_update');
                return;
            }
            // Send brief processing message
             // ** MD ESCAPE & DECIMAL APPLIED ** - Escaped `\` `.` three times using toFixed(3)
             await safeSendMessage(chat_id, `💸 Processing payout of ${escapeMarkdownV2(payoutSOL.toFixed(3))} SOL to your linked wallet\\.\\.\\.`, { parse_mode: 'MarkdownV2' });

             // Queue payout job - Use correct 'roulette' type for payout key selection
            await paymentProcessor.addPaymentJob({
                type: 'payout',
                betId,
                recipient: winnerAddress,
                amount: payoutLamports.toString(), // Full payout
                gameType: 'roulette', // <<< Ensures correct payout key is selected
                priority: 1,
                chatId: chat_id,
                displayName: displayName,
                memoId: memo_id,
            });
        } catch (e) {
            console.error(`${logPrefix}: Error preparing/queueing roulette payout info:`, e);
            await updateBetStatus(betId, 'error_payout_preparation');
             // ** MD ESCAPE APPLIED ** - Escaped `\` `.` twice
            await safeSendMessage(chat_id, `⚠️ Error occurred while processing your roulette win for bet \`${escapeMarkdownV2(memo_id)}\`\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
        }
    } else { // Loss
        await updateBetStatus(betId, 'completed_loss');
    }
}


// --- Casino War Game Logic --- (NEW - Updated with Suits & Skewed Dealing)
// ** MODIFIED: Implements ~65% house win rate via dealing bias, pays full 2x on win **
async function handleWarGame(bet) {
    const { id: betId, user_id, chat_id, expected_lamports, memo_id } = bet;
    const logPrefix = `War Bet ${betId} (${memo_id.slice(0, 6)}...)`;

    // Define ranks and suits
    const cardValues = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]; // J=11, Q=12, K=13, A=14
    const suits = ['♠️', '♥️', '♦️', '♣️'];

    function cardRankToString(rank) {
        if (rank <= 10) return rank.toString();
        if (rank === 11) return 'J'; if (rank === 12) return 'Q';
        if (rank === 13) return 'K'; if (rank === 14) return 'A';
        return '?';
    }

    // --- Skewed Dealing (Target: ~65% House Win Rate) ---
    const playerCardRank = cardValues[Math.floor(Math.random() * cardValues.length)];
    const playerSuit = suits[Math.floor(Math.random() * suits.length)];
    const playerCardStr = cardRankToString(playerCardRank) + playerSuit;

    // Determine if the house should win this non-push round
    const forceDealerWin = Math.random() < 0.65; // 65% target house win rate

    let dealerCardRank;
    let dealerSuit;

    if (forceDealerWin) {
        // Try to deal a card higher than the player's
        const higherRanks = cardValues.filter(rank => rank > playerCardRank);
        if (higherRanks.length > 0) {
            dealerCardRank = higherRanks[Math.floor(Math.random() * higherRanks.length)];
            console.log(`${logPrefix}: House bias forcing dealer win.`);
        } else {
            // Player has Ace, dealer cannot win. Force a Push.
            dealerCardRank = playerCardRank;
             console.log(`${logPrefix}: House bias attempted win, but player has Ace. Forcing Push.`);
        }
    } else {
        // Allow player to win or push - deal a card less than or equal to player's
        const lowerOrEqualRanks = cardValues.filter(rank => rank <= playerCardRank);
         if (lowerOrEqualRanks.length > 0) {
            dealerCardRank = lowerOrEqualRanks[Math.floor(Math.random() * lowerOrEqualRanks.length)];
        } else {
            // Fallback (shouldn't happen with standard deck)
            dealerCardRank = cardValues[Math.floor(Math.random() * cardValues.length)];
        }
    }

    // Assign dealer suit, avoiding exact card match only if ranks are equal
    do {
        dealerSuit = suits[Math.floor(Math.random() * suits.length)];
    } while (playerCardRank === dealerCardRank && playerSuit === dealerSuit);
    const dealerCardStr = cardRankToString(dealerCardRank) + dealerSuit;
    // --- End Skewed Dealing ---

    // Determine outcome & payout (based on ranks only, full payout)
    let outcome = '';
    let payoutLamports = 0n; // Includes stake return
    let playerWins = false;
    let isPush = false;

    if (playerCardRank > dealerCardRank) {
        outcome = 'win';
        playerWins = true;
        payoutLamports = BigInt(expected_lamports) * 2n; // Win 1:1 -> 2x stake back
    } else if (dealerCardRank > playerCardRank) {
        outcome = 'loss';
        // payoutLamports remains 0n
    } else {
        outcome = 'push'; // Tie is a Push
        isPush = true;
        payoutLamports = BigInt(expected_lamports); // Get stake back (1x)
    }

    const winOrPushRequiresPayout = (playerWins || isPush) && payoutLamports > 0n;
    const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
    const displayName = await getUserDisplayName(chat_id, user_id);

    // --- Send Result Message ---
     // ** MD ESCAPE APPLIED ** - Escaped `\` `!`
    let resultMessage = `🃏 *Casino War Result* for ${displayName} \\!\n\n` +
                         `Player Card: *${escapeMarkdownV2(playerCardStr)}*\n` +
                         `Dealer Card: *${escapeMarkdownV2(dealerCardStr)}*\n\n`;

    if (playerWins) {
          // ** MD ESCAPE & DECIMAL APPLIED ** - Escaped `\` `!` using toFixed(3)
         resultMessage += `🎉 *You Win\\!* Payout: ${escapeMarkdownV2(payoutSOL.toFixed(3))} SOL`;
    } else if (isPush) {
          // ** MD ESCAPE & DECIMAL APPLIED ** - Escaped `\` `(` `)` `!` using toFixed(3)
         resultMessage += `🤝 *Push \\(Tie\\)!* Bet returned: ${escapeMarkdownV2(payoutSOL.toFixed(3))} SOL`;
    } else { // Loss
          // ** MD ESCAPE APPLIED ** - Escaped `\` `!` `\` `.`
         resultMessage += `❌ *Dealer Wins\\!* Better luck next time\\.`;
    }
    await safeSendMessage(chat_id, resultMessage, { parse_mode: 'MarkdownV2' });

    // --- Handle Payout or Update Status ---
    if (winOrPushRequiresPayout) {
        const winnerAddress = await getLinkedWallet(user_id);
        if (!winnerAddress) {
            await updateBetStatus(betId, 'completed_win_no_wallet'); // Use generic status
             // ** MD ESCAPE & DECIMAL APPLIED ** - Escaped `\` `.` twice using toFixed(3)
            await safeSendMessage(chat_id, `Your ${outcome === 'win' ? 'winnings' : 'returned bet'} of ${escapeMarkdownV2(payoutSOL.toFixed(3))} SOL is waiting\\. Place another bet to link your wallet\\.`, { parse_mode: 'MarkdownV2' });
            return; // Exit early, no payout job needed yet
        }
        // Wallet is linked, proceed to payout
        try {
             const statusUpdated = await updateBetStatus(betId, 'processing_payout');
             if (!statusUpdated) {
                 console.error(`${logPrefix}: CRITICAL! Failed to update status from 'processing_game' to 'processing_payout' before queueing! Aborting payout queue.`);
                  // ** MD ESCAPE APPLIED ** - Escaped `\` `.` twice
                 await safeSendMessage(chatId, `⚠️ Internal error preparing your payout for bet \`${escapeMarkdownV2(memo_id)}\`\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
                 await updateBetStatus(betId, 'error_payout_status_update'); // Mark with specific error
                 return;
             }

             // Queue the payout job - Use correct 'war' type for payout key selection
             await paymentProcessor.addPaymentJob({
                 type: 'payout',
                 betId: betId,
                 recipient: winnerAddress,
                 amount: payoutLamports.toString(), // Full payout amount
                 gameType: 'war', // <<< Ensures correct payout key is selected
                 priority: 1,
                 chatId: chat_id,
                 displayName: displayName,
                 memoId: memo_id,
             });
        } catch (e) {
             console.error(`${logPrefix}: Error preparing/queueing war payout info:`, e);
             await updateBetStatus(betId, 'error_payout_preparation');
              // ** MD ESCAPE APPLIED ** - Escaped `\` `.` twice
             await safeSendMessage(chatId, `⚠️ Error occurred while processing your war win/push for bet \`${escapeMarkdownV2(memo_id)}\`\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
        }
    } else if (outcome === 'loss') {
         await updateBetStatus(betId, 'completed_loss');
    } else {
         // This case should ideally not be hit if payoutLamports is calculated correctly for push
         console.warn(`${logPrefix}: Outcome was ${outcome} but winOrPushRequiresPayout is false. PayoutLamports: ${payoutLamports}. Marking as completed.`);
         await updateBetStatus(betId, isPush ? 'completed_push_zero_payout' : 'completed_loss'); // Assume loss if not push with payout
    }
}
// --- End of Game Logic Implementation ---

// --- End of Part 3a ---
// --- Start of Part 3b ---
// index.js - Part 3b (Corrected for Separate Payout Keys & Skewed Odds)
// --- VERSION: 2.6.0 ---

// (Code continues directly from the end of Part 3a)

// --- Payout Job Handler ---
// ** MODIFIED: Handles 5 distinct payout key types based on gameType **
async function handlePayoutJob(job) {
    const { betId, recipient, amount, gameType, chatId, displayName, memoId } = job;
    const logPrefix = `Payout Job Bet ${betId} (${memoId?.slice(0, 6)}...)`;

    let payoutAmountLamports;
    try {
         payoutAmountLamports = BigInt(amount);
         if (payoutAmountLamports <= 0n) {
             console.error(`${logPrefix}: ❌ Payout amount is zero or negative (${amount}). Skipping.`);
             await updateBetStatus(betId, 'error_payout_zero_amount');
              // ** MD ESCAPE APPLIED ** - Escaped `\` `(` `)` `\` `.` twice
             await safeSendMessage(chatId, `⚠️ There was an issue calculating the payout for bet \`${escapeMarkdownV2(memoId)}\` \\(amount was zero\\)\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
             return;
         }
    } catch (e) {
        console.error(`${logPrefix}: ❌ Invalid payout amount format received in job: '${amount}'. Error: ${e.message}`);
        await updateBetStatus(betId, 'error_payout_invalid_amount');
         // ** MD ESCAPE APPLIED ** - Escaped `\` `(` `)` `\` `.` twice
        await safeSendMessage(chatId, `⚠️ Technical error processing payout for bet \`${escapeMarkdownV2(memoId)}\` \\(invalid amount\\)\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    // ** MODIFIED: Determine correct payoutWalletType based on job.gameType **
    let payoutWalletType;
    switch (job.gameType) {
        case 'race':     payoutWalletType = 'race';     break;
        case 'slots':    payoutWalletType = 'slots';    break;
        case 'roulette': payoutWalletType = 'roulette'; break;
        case 'war':      payoutWalletType = 'war';      break;
        case 'coinflip': // Fallthrough intended
        default:
             payoutWalletType = 'coinflip'; // Default or specifically coinflip
             if (job.gameType !== 'coinflip') {
                 console.warn(`${logPrefix}: Unexpected gameType "${job.gameType}" encountered in payout job. Defaulting to 'coinflip' payout key.`);
             }
             break;
    }
    // console.log(`${logPrefix}: Determined payoutWalletType: ${payoutWalletType} from gameType: ${job.gameType}`); // Debug log

    // Call sendSol to perform the transaction, passing the determined type
    let retries = parseInt(process.env.PAYOUT_JOB_RETRIES, 10);
    const baseRetryDelay = parseInt(process.env.PAYOUT_JOB_RETRY_DELAY_MS, 10);
    let attempt = 0;
    while (attempt <= retries) {
        attempt++;
        try {
             // Pass the determined payoutWalletType to sendSol
             const sendResult = await sendSol(recipient, payoutAmountLamports, payoutWalletType);

             // Success path (sendSol throws on failure)
             if (sendResult.success && sendResult.signature) {
                 const recorded = await recordPayout(betId, 'completed_win_paid', sendResult.signature);
                 if (recorded) {
                     console.log(`[PAYOUT_JOB_SUCCESS] Bet ${job.betId} payout logged successfully.`);
                     // Payout success message to user removed
                     return; // Exit function on success
                 } else {
                     console.error(`${logPrefix}: 🆘 CRITICAL! Payout sent (TX: ${sendResult.signature}) but FAILED to record in DB! Requires manual investigation.`);
                     await updateBetStatus(betId, 'error_payout_record_failed');
                      // ** MD ESCAPE APPLIED ** - Escaped `\` `.` twice
                     await safeSendMessage(chatId,
                         `⚠️ Your payout for bet \`${escapeMarkdownV2(memoId)}\` was sent successfully, but there was an issue recording it\\. Please contact support and provide this TX ID\\.\n` +
                         `TX: \`https://solscan.io/tx/${sendResult.signature}\``,
                         { parse_mode: 'MarkdownV2', disable_web_page_preview: true }
                     );
                     return; // Exit function even though recording failed
                 }
             } else {
                  // Should not be reached if sendSol throws correctly
                  console.error(`${logPrefix}: ❌ Payout failed via sendSol but did not throw error? Result:`, sendResult);
                  throw new Error(sendResult.error || 'sendSol failed without throwing');
             }
        } catch (error) {
            // Handle errors thrown by sendSol or recordPayout issues
            const errorMessage = error?.message || 'Unknown payout error';
            const isRetryable = isRetryableError(error);
            const currentBetStatusResult = await pool.query('SELECT status FROM bets WHERE id = $1', [job.betId]);
            const currentBetStatus = currentBetStatusResult.rows[0]?.status;

            // If already completed or errored out, don't retry or send messages
            if (currentBetStatus?.startsWith('completed_') || currentBetStatus?.startsWith('error_payout_')) {
                 console.warn(`[PAYOUT_JOB_RETRY] Bet ${job.betId} is already in final state '${currentBetStatus}'. Aborting payout attempts.`);
                 return; // Exit function
            }

            console.warn(`[PAYOUT_JOB_RETRY] Payout attempt ${attempt}/${retries} failed for bet ${job.betId}. Type:${payoutWalletType}. Error: ${errorMessage}. Retryable: ${isRetryable}`);

            if (!isRetryable || attempt > retries) {
                 const finalErrorStatus = isRetryable ? 'error_payout_job_failed' : 'error_payout_non_retryable';
                 console.error(`[PAYOUT_JOB_FAIL] Final payout attempt failed (or error not retryable) for bet ${job.betId}. Setting status to ${finalErrorStatus}. Error: ${errorMessage}`);
                 await updateBetStatus(job.betId, finalErrorStatus);
                 const safeErrorMsg = escapeMarkdownV2((errorMessage || 'Unknown Error').substring(0, 200));
                 // ** MD ESCAPE APPLIED ** - Escaped `\` `(` `)` `.` twice
                 await safeSendMessage(job.chatId, `⚠️ Payout for bet \`${escapeMarkdownV2(job.memoId)}\` failed after ${attempt} attempt\\(s\\)\\. Error: ${safeErrorMsg}\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
                 return; // Exit function after final failure
            }

            // Calculate delay with jitter before retrying
            const delay = baseRetryDelay * Math.pow(2, attempt - 1) + Math.random() * 1000;
            await new Promise(resolve => setTimeout(resolve, delay));
        } // end catch
    } // end while loop
}


// --- Telegram Bot Command Handlers ---

bot.on('polling_error', (error) => {
    console.error(`❌ Polling error: ${error.code} - ${error.message}`);
    if (error.code === 'ETELEGRAM' && error.message.includes('409 Conflict')) {
        console.error("❌❌❌ FATAL: Conflict detected! Another bot instance might be running. Exiting.");
         shutdown('POLLING_CONFLICT', false).catch(() => process.exit(1));
         setTimeout(() => process.exit(1), 5000).unref();
    } else if (error.code === 'ECONNRESET') {
         console.warn("⚠️ Polling connection reset. Attempting to continue...");
    } else if (error.response && error.response.statusCode === 401) {
         console.error("❌❌❌ FATAL: Unauthorized (401). Check BOT_TOKEN. Exiting.");
         shutdown('BOT_TOKEN_INVALID', false).catch(() => process.exit(1));
         setTimeout(() => process.exit(1), 5000).unref();
    } else {
         console.error(`Unhandled Polling Error: Code ${error.code}, Status ${error.response?.statusCode}`);
    }
});

bot.on('webhook_error', (error) => {
    console.error(`❌ Webhook error: ${error.code} - ${error.message}`);
      if (error.message.includes('ETIMEDOUT') || error.message.includes('ECONNRESET')) {
          console.warn("Webhook connection issue detected.");
      } else {
          console.error(`Unhandled Webhook Error: Code ${error.code}`);
      }
});

bot.on('error', (error) => { // General errors from the library
    console.error('❌ General Bot Error:', error);
    performanceMonitor.logRequest(false);
});


/**
 * Central handler for all incoming messages. Routes to command handlers.
 */
async function handleMessage(msg) {
    // Basic filtering done in 'on message' listener
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const messageText = msg.text;
    const messageId = msg.message_id;

    try {
        // Cooldown Check - Apply only to command messages
        if (messageText.startsWith('/')) {
            const now = Date.now();
            if (confirmCooldown.has(userId)) {
                const lastTime = confirmCooldown.get(userId);
                if (now - lastTime < cooldownInterval) {
                    return; // Ignore command during cooldown
                }
            }
            // Set cooldown *before* executing handler logic
            confirmCooldown.set(userId, now);
        }

        // Command Routing
        const commandMatch = messageText.trim().match(/^\/(\w+)(?:@\w+)?(?:\s+(.*))?$/s);
        if (!commandMatch) return; // Not a command

        const command = commandMatch[1].toLowerCase();
        const args = commandMatch[2]?.trim() || ''; // Arguments string

        // --- Command Handler Map --- (War Info now uses HTML) ---
        const commandHandlers = {
            'start': handleStartCommand,
            'war': handleWarInfoCommand, // Uses HTML now
            'coinflip': handleCoinflipCommand,
            'betcf': (msg, args) => handleBetCommand(msg, args),
            'race': handleRaceCommand,
            'betrace': (msg, args) => handleBetRaceCommand(msg, args),
            'slots': handleSlotsCommand,
            'betslots': (msg, args) => handleBetSlotsCommand(msg, args),
            'roulette': handleRouletteCommand,
            'betroulette': (msg, args) => handleBetRouletteCommand(msg, args),
            'betwar': (msg, args) => handleBetWarCommand(msg, args),
            'wallet': handleWalletCommand,
            'help': handleHelpCommand,
            'botstats': handleBotStatsCommand, // Admin
        };


        const handler = commandHandlers[command];

        if (handler) {
            // Admin Check
            if (command === 'botstats') {
                const adminIds = (process.env.ADMIN_USER_IDS || '').split(',').map(id=>id.trim()).filter(id => id);
                if (adminIds.includes(userId)) {
                    await handler(msg);
                } else {
                     // Silently ignore or send a generic "Unknown command" message
                }
            } else {
                 // Regular command execution
                 if (typeof handler === 'function') {
                     if (handler.length === 2) { // Handler expects msg and args
                          await handler(msg, args);
                     } else { // Handler expects only msg
                          await handler(msg);
                     }
                     performanceMonitor.logRequest(true); // Log success only if handler doesn't throw
                 } else {
                     console.warn(`Handler for command /${command} is not a function.`);
                 }
            }
        } else {
             // Unknown command
        }

    } catch (error) {
        console.error(`❌ Error processing msg ${messageId} from user ${userId} ("${messageText}"):`, error);
        performanceMonitor.logRequest(false);
        try {
             // ** MD ESCAPE APPLIED ** - Escaped `\` `.` twice
            await safeSendMessage(chatId, "⚠️ An unexpected error occurred processing your request\\. Please try again later or contact support if the issue persists\\.", { parse_mode: 'MarkdownV2'});
        } catch (tgError) { /* safeSendMessage already logs its own errors */ }
    } finally {
         if (Math.random() < 0.05) { // Cleanup cooldown map occasionally
             const cutoff = Date.now() - 300000;
             for (const [key, timestamp] of confirmCooldown.entries()) {
                 if (timestamp < cutoff) confirmCooldown.delete(key);
             }
         }
    }
}


// --- Specific Command Handler Implementations ---
// (Ensure HTML/Markdown escaping is correct in each)

// /start command (HTML) - Remains HTML
async function handleStartCommand(msg) {
    const firstName = msg.from.first_name || 'there';
    const sanitizedFirstName = firstName.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    const welcomeText = `👋 Welcome, <b>${sanitizedFirstName}</b>!\n\n` +
                         `🎰 <b>Solana Gambles Bot</b>\n\n` +
                         `Use the commands below to play:\n` +
                         `/coinflip - Simple Heads/Tails\n` +
                         `/race - Bet on Horse Races\n` +
                         `/slots - Play the Slot Machine\n` +
                         `/roulette - Play European Roulette\n` +
                         `/war - Play Casino War (Tie is Push)\n\n` +
                         `/wallet - View/Link your Solana wallet\n` +
                         `/help - See all commands & rules\n\n` +
                         `<i>Remember to gamble responsibly!</i>`;
    try {
        await safeSendMessage(msg.chat.id, welcomeText, { parse_mode: 'HTML' });
    } catch (error) {
        console.error("Error in handleStartCommand:", error);
    }
}


// /coinflip command (HTML) - Remains HTML
async function handleCoinflipCommand(msg) {
    try {
        const config = GAME_CONFIG.coinflip;
        const payoutMultiplier = '2.00'; // Fixed 2x
        const houseEdgePercent = (config.houseEdge * 100).toFixed(1);

        const messageText = `🪙 <b>Coinflip Game</b> 🪙\n\n` +
             `Bet on Heads or Tails! Simple and quick.\n\n` +
             `<b>How to play:</b>\n` +
             `1. Type <code>/betcf amount heads</code> (e.g., <code>/betcf 0.1 heads</code>)\n` +
             `2. Type <code>/betcf amount tails</code> (e.g., <code>/betcf 0.1 tails</code>)\n\n` +
             `<b>Rules:</b>\n` +
             `- Min Bet: ${config.minBet} SOL\n` +
             `- Max Bet: ${config.maxBet} SOL\n` +
             `- House Edge: Approx ${houseEdgePercent}% house auto-win chance\n` +
             `- Payout on Win: <b>${payoutMultiplier}x</b> <i>(Stake returned + 1x Stake Won)</i>\n\n` +
             `You will be given the Coinflip deposit address and a <b>unique Memo ID</b>. Send the <b>exact</b> SOL amount with the memo to place your bet.\n\n`+
             `<i>Good luck!</i>`;
        await safeSendMessage(msg.chat.id, messageText, { parse_mode: 'HTML' });
    } catch (error) {
        console.error("Error in handleCoinflipCommand:", error);
        await safeSendMessage(msg.chat.id, "Sorry, couldn't display Coinflip info right now.");
    }
}

// /race command (MarkdownV2) - Text only by design
async function handleRaceCommand(msg) {
    // ** MODIFIED: Use final odds for display **
    const horses = [
         { name: 'Yellow', emoji: '🟡', odds: 2.0 }, { name: 'Orange', emoji: '🟠', odds: 3.0 },
         { name: 'Blue',   emoji: '🔵', odds: 4.0 }, { name: 'Cyan',   emoji: '💧', odds: 5.0 },
         { name: 'White',  emoji: '⚪️', odds: 6.0 }, { name: 'Red',    emoji: '🔴', odds: 7.0 },
         { name: 'Black',  emoji: '⚫️', odds: 8.0 }, { name: 'Pink',   emoji: '🌸', odds: 9.0 },
         { name: 'Purple', emoji: '🟣', odds: 10.0 }, { name: 'Green',  emoji: '🟢', odds: 15.0 }, // Updated Green
         { name: 'Silver', emoji: '💎', odds: 25.0 }  // Updated Silver
    ];
    let raceMessage = `🐎 *Horse Race Game* 🐎\n\nBet on the winning horse\\!\n\n*Available Horses \\& Payout Multiplier* \\(Stake \\* Multiplier\\):\n`;
    horses.forEach(horse => {
        const displayMultiplier = horse.odds.toFixed(2);
        raceMessage += `\\- ${horse.emoji} *${escapeMarkdownV2(horse.name)}* \\(${escapeMarkdownV2(displayMultiplier)}x Payout\\)\n`;
    });
    const config = GAME_CONFIG.race;
    const houseEdgePercent = (config.houseEdge * 100).toFixed(1);
    raceMessage += `\n*How to play:*\n` +
                         `1\\. Type \`/betrace amount horse_name\`\n` +
                         `  \\(e\\.g\\., \`/betrace 0\\.1 Yellow\`\\)\n\n` +
                         `*Rules:*\n` +
                         `\\- Min Bet: ${escapeMarkdownV2(config.minBet)} SOL\n` +
                         `\\- Max Bet: ${escapeMarkdownV2(config.maxBet)} SOL\n` +
                         `\\- House Edge: Applied via win probability \\(Approx ${escapeMarkdownV2(houseEdgePercent)}% house auto\\-win chance \\+ skewed horse weights\\)\n` +
                         `\\- Payout on Win: Stake \\* Horse Odds\n\n`+
                         `You will be given the Race deposit address and a *unique Memo ID*\\. Send the *exact* SOL amount with the memo to place your bet\\.`;
    await safeSendMessage(msg.chat.id, raceMessage, { parse_mode: 'MarkdownV2' });
}

// /slots command (MarkdownV2) - Remains MarkdownV2 (verified escapes)
async function handleSlotsCommand(msg) {
    const config = GAME_CONFIG.slots;
    const hiddenEdgePercent = parseFloat(process.env.SLOTS_HIDDEN_EDGE || '0.10') * 100;
    const paylines = [
         `🍒 Cherry \\| 🍒 Cherry \\| 🍒 Cherry \\= ${escapeMarkdownV2(SLOTS_SYMBOLS.CHERRY.payout[3])}x Stake`,
         `🍊 Orange \\| 🍊 Orange \\| 🍊 Orange \\= ${escapeMarkdownV2(SLOTS_SYMBOLS.ORANGE.payout[3])}x Stake`,
         `🍫 BAR \\| 🍫 BAR \\| 🍫 BAR \\= ${escapeMarkdownV2(SLOTS_SYMBOLS.BAR.payout[3])}x Stake`,
         `7️⃣ Seven \\| \\(Any\\) \\| \\(Any\\) \\= 2x Stake`, // Updated payout from prev round
         `🎰 777 \\| 🎰 777 \\| 🎰 777 \\= ${escapeMarkdownV2(SLOTS_SYMBOLS.TRIPLE_SEVEN.payout[3])}x Stake \\(Jackpot\\!\\)`
    ];

    // ** Ensured all static MarkdownV2 characters like ! ( ) . - % + & * are escaped **
    const message = `🎰 *777 Slots Game* 🎰\n\n` +
                   `Spin the 3 reels and match symbols on the center line\\!\n\n`+
                   `*Symbols:*\n`+
                   `🍒 Cherry, 🍊 Orange, 🍫 BAR, 7️⃣ Seven, 🎰 777, ➖ Blank\n\n` +
                   `*Payouts \\(Win Amount / Bet Amount\\):*\n` + // Escaped -
                   paylines.map(line => `\\- ${line}`).join('\n') + `\n\n` + // Escaped -
                   `*How to Play:*\n` +
                   `\\- Type \`/betslots amount\` \\(e\\.g\\., \`/betslots 0\\.05\`\\)\n\n` + // Escaped . ( ) .
                   `*Rules:*\n` +
                   `\\- Min Bet: ${escapeMarkdownV2(config.minBet)} SOL\n` +
                   `\\- Max Bet: ${escapeMarkdownV2(config.maxBet)} SOL\n` +
                   `\\- House Edge: Applied via symbol weights \\& hidden ${escapeMarkdownV2(hiddenEdgePercent.toFixed(1))}% forced loss chance\\.\n`+ // Escaped & % .
                   `\\- Payout on Win: Stake \\+ \\(Stake \\* Multiplier\\)\n\n`+ // Escaped + ( ) *
                   `You will be given the Slots deposit address and a *unique Memo ID*\\. Send the *exact* SOL amount with the memo to spin\\.`; // Escaped .

    await safeSendMessage(msg.chat.id, message, { parse_mode: 'MarkdownV2' });
}

// /roulette command (MarkdownV2) - Remains MarkdownV2 (verified escapes)
async function handleRouletteCommand(msg) {
    const config = GAME_CONFIG.roulette;
    const hiddenEdgePercent = parseFloat(process.env.ROULETTE_HIDDEN_EDGE || '0.65') * 100;
    // ** Ensured all static MarkdownV2 characters like ! ( ) . - % + & * are escaped **
    const message = `⚪️ *European Roulette Game* ⚪️

Place bets on the outcome of the wheel spin \\(numbers 0\\-36\\)\\.

Bet Types & Payouts \\(Odds N:1\\) \\- Payout is Stake \\* \\(N\\+1\\)*:
\\- *Straight* \\(\`S<number>\`, e\\.g\\. \`S17\`\\): 35:1 \\(Pays 36x Stake\\)
\\- *Red* \\(\`R\`\\) \\/ *Black* \\(\`B\`\\): 1:1 \\(Pays 2x Stake\\)
\\- *Even* \\(\`E\`\\) \\/ *Odd* \\(\`O\`\\): 1:1 \\(Pays 2x Stake\\)
\\- *Low* \\(\`L\`, numbers 1\\-18\\) \\/ *High* \\(\`H\`, numbers 19\\-36\\): 1:1 \\(Pays 2x Stake\\)
\\- *Dozens* \\(e\\.g\\. \`D1\` for 1\\-12, \`D2\` for 13\\-24, \`D3\` for 25\\-36\\): 2:1 \\(Pays 3x Stake\\)
\\- *Columns* \\(e\\.g\\. \`C1\` for 1,4,7\\.\\.\\.34\\): 2:1 \\(Pays 3x Stake\\)

*How to Play*:
\\- Type \`/betroulette <amount> <bet\\_spec>\`
    *\\(e\\.g\\., \`/betroulette 0\\.1 R\`\\)*
    *\\(e\\.g\\., \`/betroulette 0\\.05 S17\`\\)*
    *\\(e\\.g\\., \`/betroulette 0\\.2 D1\`\\)*

*Rules*:
\\- Min Bet \\(per placement\\): ${escapeMarkdownV2(config.minBet)} SOL
\\- Max Bet \\(per placement\\): ${escapeMarkdownV2(config.maxBet)} SOL
\\- House Edge: Applied via win probability \\(Approx ${escapeMarkdownV2(hiddenEdgePercent.toFixed(1))}% chance of forced '0' result\\)\\.
\\- Payout on Win: Standard Roulette Payouts \\(see above\\)

You will be given the Roulette deposit address and a *unique Memo ID*\\. Send the *exact* SOL amount with the memo\\.`;

    await safeSendMessage(msg.chat.id, message, { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}

// /war command (HTML) - Use HTML version to avoid parsing issues
async function handleWarInfoCommand(msg) {
    const config = GAME_CONFIG.war;
    const minBetHtml = String(config.minBet).replace(/</g, '&lt;').replace(/>/g, '&gt;');
    const maxBetHtml = String(config.maxBet).replace(/</g, '&lt;').replace(/>/g, '&gt;');
    // Message using HTML tags
    const message = `🃏 <b>Casino War Game</b> 🃏\n\n` +
                    `Place your bet. You and the dealer each get one card. Highest card wins (Ace high)!\n\n` +
                    `<b>Rules:</b>\n` +
                    `- If your card is higher, you win 1:1 (double your bet back).\n` +
                    `- If the dealer's card is higher, you lose your bet.\n` +
                    `- If cards <i>Tie</i>, it's a <i>Push</i> - your bet is returned to you.\n\n` +
                    `<b>How to Play:</b>\n` +
                    `- Type <code>/betwar &lt;amount&gt;</code> (e.g., <code>/betwar 0.1</code>)\n\n` +
                    `<b>Limits:</b>\n` +
                    `- Min Bet: ${minBetHtml} SOL\n` +
                    `- Max Bet: ${maxBetHtml} SOL\n` +
                    `- House Edge: Applied via biased card dealing (House wins approx 65% of non-push rounds).\n` + // Removed problematic ~
                    `- Payout on Win: 2x Stake. Push returns 1x Stake.\n\n` +
                    `You will be given the War deposit address and a <b>unique Memo ID</b>. Send the <b>exact</b> SOL amount with the memo to play.`;
    await safeSendMessage(msg.chat.id, message, { parse_mode: 'HTML' });
}

// /wallet command (MarkdownV2) - Remains MarkdownV2 (verified escapes)
async function handleWalletCommand(msg) {
    const userId = String(msg.from.id);
    const walletAddress = await getLinkedWallet(userId);
    if (walletAddress) {
         // ** MD ESCAPE APPLIED ** - Escaped `\` `.` twice
        await safeSendMessage(msg.chat.id,
            `🔗 Your linked Solana wallet:\n\`${escapeMarkdownV2(walletAddress)}\`\n\n`+
            `Payouts will be sent here\\. It's linked automatically when you make your first paid bet\\.`,
            { parse_mode: 'MarkdownV2' }
        );
    } else {
          // ** MD ESCAPE APPLIED ** - Escaped `\` `.` twice
        await safeSendMessage(msg.chat.id,
            `🔗 No wallet linked yet\\.\n` +
            `Place a bet and send the required SOL\\. Your sending wallet will be automatically linked for future payouts\\.`,
             { parse_mode: 'MarkdownV2' }
        );
    }
}


// /betcf command (MarkdownV2) - Verified escapes & uses CF_WALLET_ADDRESS
async function handleBetCommand(msg, args) {
     const match = args.trim().match(/^(\d+\.?\d*)\s+(heads|tails)/i);
     if (!match) {
           // ** MD ESCAPE APPLIED ** - Escaped `\` `.` twice
         await safeSendMessage(msg.chat.id,
              `⚠️ Invalid format\\. Use: \`/betcf <amount> <heads|tails>\`\n` +
              `Example: \`/betcf 0\\.100 heads\``,
              { parse_mode: 'MarkdownV2' }
         );
         return;
     }
     const userId = String(msg.from.id);
     const chatId = String(msg.chat.id);
     const config = GAME_CONFIG.coinflip;
     const betAmount = parseFloat(match[1]);
     if (isNaN(betAmount) || betAmount < config.minBet || betAmount > config.maxBet) {
            // ** MD ESCAPE APPLIED ** - Escaped `\` `.` twice using toFixed(3)
         await safeSendMessage(chatId,
              `⚠️ Invalid bet amount\\. Please bet between ${escapeMarkdownV2(config.minBet.toFixed(3))} and ${escapeMarkdownV2(config.maxBet.toFixed(3))} SOL\\.`,
              { parse_mode: 'MarkdownV2' }
         );
         return;
     }
     const userChoice = match[2].toLowerCase();
     const memoId = generateMemoId('CF');
     const expectedLamports = BigInt(Math.round(betAmount * LAMPORTS_PER_SOL));
     const expiresAt = new Date(Date.now() + config.expiryMinutes * 60 * 1000);
     const saveResult = await savePendingBet( userId, chatId, 'coinflip', { choice: userChoice }, expectedLamports, memoId, expiresAt );
     if (!saveResult.success) {
          // ** MD ESCAPE APPLIED ** - Escaped `\` `.` twice
         await safeSendMessage(chatId, `⚠️ Error registering bet: ${escapeMarkdownV2(saveResult.error || 'Unknown')}\\. Please try the command again\\.`, { parse_mode: 'MarkdownV2' });
         return;
     }
     const depositAddress = process.env.CF_WALLET_ADDRESS;
     if (!depositAddress) {
          console.error("CRITICAL: CF_WALLET_ADDRESS environment variable is not set!");
           // ** MD ESCAPE APPLIED ** - Escaped `\` `.` twice
          await safeSendMessage(chatId, `⚠️ Bot configuration error: Coinflip deposit address not set\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
          return;
     }
      // ** FORMATTING & MD ESCAPE APPLIED ** - Use toFixed(3), escaped `\` `!` `(` `)` `.` twice `(` `)` `.`
     const betAmountString = escapeMarkdownV2(betAmount.toFixed(3));
     const message = `✅ Coinflip bet registered\\! \\(ID: \`${memoId}\`\\)\n\n` +
                      `You chose: *${escapeMarkdownV2(userChoice)}*\n` +
                      `Amount: *${betAmountString} SOL*\n\n` +
                      `➡️ Send *exactly ${betAmountString} SOL* to \\(Coinflip Deposit\\):\n` +
                      `\`${escapeMarkdownV2(depositAddress)}\`\n\n` +
                      `📎 *Include MEMO:* \`${memoId}\`\n\n` +
                      `⏱️ This request expires in ${config.expiryMinutes} minutes\\.\n\n` +
                      `*IMPORTANT:* Send from your own wallet \\(not an exchange\\)\\. Ensure you include the memo correctly\\.`;
     await safeSendMessage(chatId, message, { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}

// /betrace command (MarkdownV2)
async function handleBetRaceCommand(msg, args) {
     const match = args.trim().match(/^(\d+\.?\d*)\s+([\w\s]+)/i);
     if (!match) { /* ... error handling ... */ return; }
     const userId = String(msg.from.id);
     const chatId = String(msg.chat.id);
     const config = GAME_CONFIG.race;
     const betAmount = parseFloat(match[1]);
     if (isNaN(betAmount) || betAmount < config.minBet || betAmount > config.maxBet) { /* ... error handling ... */ return; }

      const chosenHorseNameInput = match[2].trim();
      // ** MODIFIED: Use final odds here too **
      const horses = [
          { name: 'Yellow', emoji: '🟡', odds: 2.0 }, { name: 'Orange', emoji: '🟠', odds: 3.0 },
          { name: 'Blue',   emoji: '🔵', odds: 4.0 }, { name: 'Cyan',   emoji: '💧', odds: 5.0 },
          { name: 'White',  emoji: '⚪️', odds: 6.0 }, { name: 'Red',    emoji: '🔴', odds: 7.0 },
          { name: 'Black',  emoji: '⚫️', odds: 8.0 }, { name: 'Pink',   emoji: '🌸', odds: 9.0 },
          { name: 'Purple', emoji: '🟣', odds: 10.0 }, { name: 'Green',  emoji: '🟢', odds: 15.0 }, // Updated Green
          { name: 'Silver', emoji: '💎', odds: 25.0 }  // Updated Silver
      ];
      const chosenHorse = horses.find(h => h.name.toLowerCase() === chosenHorseNameInput.toLowerCase());

     if (!chosenHorse) { /* ... error handling ... */ return; }

     const memoId = generateMemoId('RA');
     const expectedLamports = BigInt(Math.round(betAmount * LAMPORTS_PER_SOL));
     const expiresAt = new Date(Date.now() + config.expiryMinutes * 60 * 1000);

     // Calculate potential payout for display using FINALIZED odds
     const potentialPayoutLamports = (expectedLamports * BigInt(Math.round(chosenHorse.odds * 100))) / 100n;
     const potentialPayoutSOL = escapeMarkdownV2((Number(potentialPayoutLamports) / LAMPORTS_PER_SOL).toFixed(3));
     const betAmountString = escapeMarkdownV2(betAmount.toFixed(3));

     const saveResult = await savePendingBet( userId, chatId, 'race', { horse: chosenHorse.name, odds: chosenHorse.odds }, expectedLamports, memoId, expiresAt );
     if (!saveResult.success) { /* ... error handling ... */ return; }

     const depositAddress = process.env.RACE_WALLET_ADDRESS;
     if (!depositAddress) { /* ... error handling ... */ return; }

     // Message uses potentialPayoutSOL calculated with final odds
     const message = `✅ Race bet registered\\! \\(ID: \`${memoId}\`\\)\n\n` +
                      `You chose: ${chosenHorse.emoji} *${escapeMarkdownV2(chosenHorse.name)}*\n` +
                      `Amount: *${betAmountString} SOL*\n` +
                      `Potential Payout: ${potentialPayoutSOL} SOL \\(Stake \\* ${escapeMarkdownV2(chosenHorse.odds.toFixed(2))}x\\)\n\n`+ // Escaped . in odds display
                      `➡️ Send *exactly ${betAmountString} SOL* to \\(Race Deposit\\):\n` +
                      `\`${escapeMarkdownV2(depositAddress)}\`\n\n` +
                      `📎 *Include MEMO:* \`${memoId}\`\n\n` +
                      `⏱️ This request expires in ${config.expiryMinutes} minutes\\.\n\n` +
                      `*IMPORTANT:* Send from your own wallet \\(not an exchange\\)\\. Ensure you include the memo correctly\\.`;
     await safeSendMessage(chatId, message, { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}

// /betslots command (MarkdownV2) - Verified escapes & uses SLOTS_WALLET_ADDRESS
async function handleBetSlotsCommand(msg, args) {
    const match = args.trim().match(/^(\d+\.?\d*)$/);
    if (!match) {
          // ** MD ESCAPE APPLIED ** - Escaped `\` `.` twice
        await safeSendMessage(msg.chat.id,
            `⚠️ Invalid format\\. Use: \`/betslots <amount>\`\n` +
            `Example: \`/betslots 0\\.050\``,
            { parse_mode: 'MarkdownV2' }
        );
        return;
    }
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const config = GAME_CONFIG.slots;
    const betAmount = parseFloat(match[1]);
    if (isNaN(betAmount) || betAmount < config.minBet || betAmount > config.maxBet) {
          // ** MD ESCAPE APPLIED ** - Escaped `\` `.` twice using toFixed(3)
        await safeSendMessage(chatId,
            `⚠️ Invalid bet amount\\. Please bet between ${escapeMarkdownV2(config.minBet.toFixed(3))} and ${escapeMarkdownV2(config.maxBet.toFixed(3))} SOL\\.`,
            { parse_mode: 'MarkdownV2' }
        );
        return;
    }
    const memoId = generateMemoId('SL');
    const expectedLamports = BigInt(Math.round(betAmount * LAMPORTS_PER_SOL));
    const expiresAt = new Date(Date.now() + config.expiryMinutes * 60 * 1000);
    const betDetails = { betAmountSOL: betAmount };
    const saveResult = await savePendingBet( userId, chatId, 'slots', betDetails, expectedLamports, memoId, expiresAt );
    if (!saveResult.success) {
         // ** MD ESCAPE APPLIED ** - Escaped `\` `.` twice
        await safeSendMessage(chatId, `⚠️ Error registering bet: ${escapeMarkdownV2(saveResult.error || 'Unknown')}\\. Please try again\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    const depositAddress = process.env.SLOTS_WALLET_ADDRESS;
    if (!depositAddress) {
         console.error("CRITICAL: SLOTS_WALLET_ADDRESS environment variable is not set!");
           // ** MD ESCAPE APPLIED ** - Escaped `\` `.` twice
         await safeSendMessage(chatId, `⚠️ Bot configuration error: Slots deposit address not set\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
         return;
    }
      // ** FORMATTING & MD ESCAPE APPLIED ** - Use toFixed(3), escaped `\` `!` `(` `)` twice `\` `.` twice `\` `(` `)` `\` `.`
    const betAmountString = escapeMarkdownV2(betAmount.toFixed(3));
    const message = `✅ Slots bet registered\\! \\(ID: \`${memoId}\`\\)\n\n` +
                     `Spin Amount: *${betAmountString} SOL*\n\n` +
                     `➡️ Send *exactly ${betAmountString} SOL* to \\(Slots Deposit\\):\n` +
                     `\`${escapeMarkdownV2(depositAddress)}\`\n\n` +
                     `📎 *Include MEMO:* \`${memoId}\`\n\n` +
                     `⏱️ This request expires in ${config.expiryMinutes} minutes\\.\n\n` +
                     `*IMPORTANT:* Send from your own wallet \\(not an exchange\\)\\. Ensure you include the memo correctly\\.`;
    await safeSendMessage(chatId, message, { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}


// /betroulette command (MarkdownV2) - Verified escapes & uses ROULETTE_WALLET_ADDRESS
async function handleBetRouletteCommand(msg, args) {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const config = GAME_CONFIG.roulette;
    const match = args.trim().match(/^(\d+\.?\d*)\s+(.+)$/i);
    if (!match) {
         // ** MD ESCAPE APPLIED ** - Escaped `\` `.` `\` `\` `\` etc.
        await safeSendMessage(chatId,
            `⚠️ Invalid format\\. Use: \`/betroulette <amount> <bet_spec>\`\n` +
            `Bet Spec Codes: \`R\`, \`B\`, \`E\`, \`O\`, \`L\`, \`H\`, \`D1\`/\`D2\`/\`D3\`, \`C1\`/\`C2\`/\`C3\`, \`S<0-36>\`\n`+
            `Examples: \`/betroulette 0\\.100 R\` \\| \`/betroulette 0\\.020 S17\` \\| \`/betroulette 0\\.200 D1\``,
            { parse_mode: 'MarkdownV2', disable_web_page_preview: true }
        );
        return;
    }
    const betAmount = parseFloat(match[1]);
    const betSpec = match[2].toUpperCase();
     if (isNaN(betAmount) || betAmount <= 0) {
           // ** MD ESCAPE APPLIED ** - Escaped `\` `.`
         await safeSendMessage(chatId, `⚠️ Invalid bet amount specified: "${escapeMarkdownV2(match[1] || args)}"\\. Amount must be positive\\.`, { parse_mode: 'MarkdownV2' });
         return;
     }
    if (betAmount < config.minBet || betAmount > config.maxBet) {
          // ** MD ESCAPE & DECIMAL APPLIED ** - Escaped `\` `(` `\` `-` `\` `)` `.` using toFixed(3)
        await safeSendMessage(chatId,
            `⚠️ Bet amount out of range for a single placement \\(${escapeMarkdownV2(config.minBet.toFixed(3))} \\- ${escapeMarkdownV2(config.maxBet.toFixed(3))} SOL\\)\\.`,
            { parse_mode: 'MarkdownV2' }
        );
        return;
    }
    let betKey = ''; let betType = ''; let betValue = undefined;
    if (/^(R|B|E|O|L|H)$/.test(betSpec)) { betKey = betSpec; betType = betSpec; }
    else if (/^D([1-3])$/.test(betSpec)) { betKey = betSpec; betType = 'D'; betValue = betSpec.substring(1); }
    else if (/^C([1-3])$/.test(betSpec)) { betKey = betSpec; betType = 'C'; betValue = betSpec.substring(1); }
    else if (/^S(0|[1-9]|[12]\d|3[0-6])$/.test(betSpec)) { betKey = betSpec; betType = 'S'; betValue = betSpec.substring(1); }
    else {
           // ** MD ESCAPE APPLIED ** - Escaped `\` `.` twice `\` `\` etc. `\` `.`
         await safeSendMessage(chatId,
            `⚠️ Invalid Bet Specification: \`${escapeMarkdownV2(betSpec)}\`\\.\n` +
            `Use codes like \`R\`, \`B\`, \`E\`, \`O\`, \`L\`, \`H\`, \`D1\`/\`D2\`/\`D3\`, \`C1\`/\`C2\`/\`C3\`, \`S<0-36>\`\\. See /roulette\\.`,
            { parse_mode: 'MarkdownV2', disable_web_page_preview: true }
         );
        return;
    }
    const memoId = generateMemoId('RL');
    const expectedLamports = BigInt(Math.round(betAmount * LAMPORTS_PER_SOL));
    const expiresAt = new Date(Date.now() + config.expiryMinutes * 60 * 1000);
    const betDetails = { betAmountSOL: betAmount, bets: { [betKey]: expectedLamports.toString() } };
    const saveResult = await savePendingBet( userId, chatId, 'roulette', betDetails, expectedLamports, memoId, expiresAt );
    if (!saveResult.success) {
         // ** MD ESCAPE APPLIED ** - Escaped `\` `.` twice
        await safeSendMessage(chatId, `⚠️ Error registering bet: ${escapeMarkdownV2(saveResult.error || 'Unknown')}\\. Please try again\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    const depositAddress = process.env.ROULETTE_WALLET_ADDRESS;
    if (!depositAddress) {
         console.error("CRITICAL: ROULETTE_WALLET_ADDRESS environment variable is not set!");
           // ** MD ESCAPE APPLIED ** - Escaped `\` `.` twice
         await safeSendMessage(chatId, `⚠️ Bot configuration error: Roulette deposit address not set\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
         return;
    }
      // ** FORMATTING & MD ESCAPE APPLIED ** - Use toFixed(3), escaped `\` `!` `(` `)` `\` `(` `)` `\` `.` twice `\` `(` `)` `\` `.`
    const betAmountString = escapeMarkdownV2(betAmount.toFixed(3));
    const message = `✅ Roulette bet registered\\! \\(ID: \`${memoId}\`\\)\n\n` +
                     `Bet Placed: *${escapeMarkdownV2(betKey)}*\n` +
                     `Amount: *${betAmountString} SOL*\n\n` +
                     `➡️ Send *exactly ${betAmountString} SOL* to \\(Roulette Deposit\\):\n` +
                     `\`${escapeMarkdownV2(depositAddress)}\`\n\n` +
                     `📎 *Include MEMO:* \`${memoId}\`\n\n` +
                     `⏱️ This request expires in ${config.expiryMinutes} minutes\\.\n\n` +
                     `*IMPORTANT:* Send from your own wallet \\(not an exchange\\)\\. Ensure you include the memo correctly\\.`;
    await safeSendMessage(chatId, message, { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}

// /betwar command (MarkdownV2) - Verified escapes & uses WAR_WALLET_ADDRESS
async function handleBetWarCommand(msg, args) {
    const match = args.trim().match(/^(\d+\.?\d*)$/);
    if (!match) {
          // ** MD ESCAPE APPLIED ** - Escaped `\` `.` twice
        await safeSendMessage(msg.chat.id,
            `⚠️ Invalid format\\. Use: \`/betwar <amount>\`\n` +
            `Example: \`/betwar 0\\.100\``,
            { parse_mode: 'MarkdownV2' }
        );
        return;
    }
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const config = GAME_CONFIG.war;
    const betAmount = parseFloat(match[1]);
    if (isNaN(betAmount) || betAmount < config.minBet || betAmount > config.maxBet) {
          // ** MD ESCAPE APPLIED ** - Escaped `\` `.` twice using toFixed(3)
        await safeSendMessage(chatId,
            `⚠️ Invalid bet amount\\. Please bet between ${escapeMarkdownV2(config.minBet.toFixed(3))} and ${escapeMarkdownV2(config.maxBet.toFixed(3))} SOL for War\\.`,
            { parse_mode: 'MarkdownV2' }
        );
        return;
    }
    const memoId = generateMemoId('WA');
    const expectedLamports = BigInt(Math.round(betAmount * LAMPORTS_PER_SOL));
    const expiresAt = new Date(Date.now() + config.expiryMinutes * 60 * 1000);
    const saveResult = await savePendingBet( userId, chatId, 'war', {}, expectedLamports, memoId, expiresAt );
    if (!saveResult.success) {
         // ** MD ESCAPE APPLIED ** - Escaped `\` `.` twice
        await safeSendMessage(chatId, `⚠️ Error registering War bet: ${escapeMarkdownV2(saveResult.error || 'Unknown')}\\. Please try again\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    const depositAddress = process.env.WAR_WALLET_ADDRESS;
    if (!depositAddress) {
         console.error("CRITICAL: WAR_WALLET_ADDRESS environment variable is not set!");
           // ** MD ESCAPE APPLIED ** - Escaped `\` `.` twice
         await safeSendMessage(chatId, `⚠️ Bot configuration error: War deposit address not set\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
         return;
    }
      // ** FORMATTING & MD ESCAPE APPLIED ** - Use toFixed(3), escaped `\` `!` `(` `)` `\` `.` twice `\` `(` `)` `\` `.`
    const betAmountString = escapeMarkdownV2(betAmount.toFixed(3));
    const message = `✅ Casino War bet registered\\! \\(ID: \`${memoId}\`\\)\n\n` +
                     `Bet Amount: *${betAmountString} SOL*\n\n` +
                     `➡️ Send *exactly ${betAmountString} SOL* to \\(War Deposit\\):\n` +
                     `\`${escapeMarkdownV2(depositAddress)}\`\n\n` +
                     `📎 *Include MEMO:* \`${memoId}\`\n\n` +
                     `⏱️ This request expires in ${config.expiryMinutes} minutes\\.\n\n` +
                     `*IMPORTANT:* Send from your own wallet \\(not an exchange\\)\\. Ensure you include the memo correctly\\.`;
    await safeSendMessage(chatId, message, { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}


// /help command (MarkdownV2) - Verified escapes
async function handleHelpCommand(msg) {
     // ** MD ESCAPE APPLIED ** - Escaped `\` `-` multiple times `\` `&` `\` `(` `)` `\` `.`
    const helpText = `*Solana Gambles Bot Commands* 🎰\n\n` +
                      `/start \\- Show welcome message\n` +
                      `/help \\- Show this help message\n\n` +
                      `*Games:*\n` +
                      `/coinflip \\- Coinflip info \\& how to bet\n` +
                      `/race \\- Horse Race info \\& how to bet\n` +
                      `/slots \\- Slots info \\& how to bet\n` +
                      `/roulette \\- Roulette info \\& how to bet\n` +
                      `/war \\- Casino War info \\& how to bet\n\n` +
                      `*Betting:*\n` +
                      `/betcf <amount> <heads|tails> \\- Place a Coinflip bet\n` +
                      `/betrace <amount> <horse\\_name> \\- Place a Race bet\n` +
                      `/betslots <amount> \\- Place a Slots bet\n` +
                      `/betroulette <amount> <bet_spec> \\- Place a Roulette bet \\(see /roulette\\)\n` +
                      `/betwar <amount> \\- Place a Casino War bet\n\n` +
                      `*Wallet:*\n` +
                      `/wallet \\- View your linked Solana wallet for payouts\n\n` +
                      `*Support:* If you encounter issues, please contact support \\(details not provided here\\)\\.`;

    await safeSendMessage(msg.chat.id, helpText, { parse_mode: 'MarkdownV2' });
}


// /botstats command (Admin - MarkdownV2) - Updated Version
async function handleBotStatsCommand(msg) {
    // Note: Assumes ADMIN_USER_IDS check happened in handleMessage
    const processor = paymentProcessor;
    let connectionStats = null;
    try {
        connectionStats = typeof solanaConnection?.getRequestStats === 'function' ? solanaConnection.getRequestStats() : null;
    } catch(e){ console.error("Error getting conn stats for botstats:", e.message); }

    const messageQueueSize = messageQueue?.size || 0;
    const messageQueuePending = messageQueue?.pending || 0;
    const telegramSendQueueSize = telegramSendQueue?.size || 0;
    const telegramSendQueuePending = telegramSendQueue?.pending || 0;
    const paymentHighPriQueueSize = processor?.highPriorityQueue?.size || 0;
    const paymentHighPriQueuePending = processor?.highPriorityQueue?.pending || 0;
    const paymentNormalQueueSize = processor?.normalQueue?.size || 0;
    const paymentNormalQueuePending = processor?.normalQueue?.pending || 0;

      // ** MD ESCAPE APPLIED & Version Updated ** - Escaped `\` `(` `)` `\` `-` multiple times `\` `/` `\` `.` `\` `%`
    let statsMsg = `*Bot Statistics* \\(v${escapeMarkdownV2('2.6.0')}\\)\n\n`; // Updated version
    statsMsg += `*Uptime:* ${escapeMarkdownV2(Math.floor(process.uptime() / 60))} minutes\n`;
    statsMsg += `*Performance:* Req:${performanceMonitor.requests}, Err:${performanceMonitor.errors}\n`;
    statsMsg += `*Queues:*\n`;
    statsMsg += `  \\- Msg: P:${messageQueueSize} A:${messageQueuePending}\n`;
    statsMsg += `  \\- TG Send: P:${telegramSendQueueSize} A:${telegramSendQueuePending}\n`;
    statsMsg += `  \\- Pay High: P:${paymentHighPriQueueSize} A:${paymentHighPriQueuePending}\n`;
    statsMsg += `  \\- Pay Norm: P:${paymentNormalQueueSize} A:${paymentNormalQueuePending}\n`;
    statsMsg += `*Caches:*\n`;
    statsMsg += `  \\- Wallets: ${walletCache.size}\n`;
    statsMsg += `  \\- Processed Sigs: ${processedSignaturesThisSession.size} / ${MAX_PROCESSED_SIGNATURES}\n`;
    statsMsg += `  \\- Memo Cache: ${paymentProcessor.memoCache.size}\n`;

    if (connectionStats?.status && connectionStats?.stats) {
        statsMsg += `*Solana Connection:*\n`;
        statsMsg += `  \\- Endpoint: ${escapeMarkdownV2(connectionStats.status.currentEndpoint || 'N/A')} \\(Idx ${connectionStats.status.currentEndpointIndex ?? 'N/A'}\\)\n`;
        statsMsg += `  \\- Q:${connectionStats.status.queueSize ?? 'N/A'}, A:${connectionStats.status.activeRequests ?? 'N/A'}\n`;
        statsMsg += `  \\- Consecutive RL: ${connectionStats.status.consecutiveRateLimits ?? 'N/A'}\n`;
        statsMsg += `  \\- Last RL: ${escapeMarkdownV2(connectionStats.status.lastRateLimitTimestamp ? new Date(connectionStats.status.lastRateLimitTimestamp).toISOString() : 'None')}\n`;
        statsMsg += `  \\- Tot Req: S:${connectionStats.stats.totalRequestsSucceeded ?? 'N/A'}, F:${connectionStats.stats.totalRequestsFailed ?? 'N/A'}\n`;
        statsMsg += `  \\- RL Events: ${connectionStats.stats.rateLimitEvents ?? 'N/A'}\n`;
        statsMsg += `  \\- Rotations: ${connectionStats.stats.endpointRotations ?? 'N/A'}\n`;
        statsMsg += `  \\- Success Rate: ${escapeMarkdownV2(connectionStats.stats.successRate ? connectionStats.stats.successRate.toFixed(1) + '%' : 'N/A')}\n`;
    } else {
        statsMsg += `*Solana Connection:* Stats unavailable\\.\n`;
    }

    try {
        statsMsg += `*DB Pool:*\n`;
        statsMsg += `  \\- Total: ${pool.totalCount}, Idle: ${pool.idleCount}, Waiting: ${pool.waitingCount}\n`;
    } catch (e) {
        statsMsg += `*DB Pool:* Stats unavailable\\.\n`;
    }

    await safeSendMessage(msg.chat.id, statsMsg, { parse_mode: 'MarkdownV2' });
}


// --- Server Startup & Shutdown Logic ---
// (Moved to Part 4)

// --- End of Part 3b ---
// --- Start of Part 4 ---
// index.js - Part 4 (Corrected for Separate Payout Keys & Skewed Odds)
// --- VERSION: 2.6.0 ---

// (Code continues directly from the end of Part 3b)

// --- Server Startup & Shutdown Logic ---

// Encapsulated Webhook Setup Logic
async function setupTelegramWebhook() {
    if (process.env.RAILWAY_ENVIRONMENT && process.env.RAILWAY_PUBLIC_DOMAIN) {
        const webhookUrl = `https://${process.env.RAILWAY_PUBLIC_DOMAIN}${webhookPath}`; // webhookPath defined earlier
        console.log(`Attempting to set webhook to: ${webhookUrl}`);
        let attempts = 0;
        const maxAttempts = 3;
        while (attempts < maxAttempts) {
            try {
                 // Ensure any existing webhook is cleared first, ignore errors
                 await bot.deleteWebHook({ drop_pending_updates: true }).catch(e => console.warn(`Ignoring error during pre-webhook delete: ${e.message}`));

                await bot.setWebHook(webhookUrl, {
                    max_connections: parseInt(process.env.WEBHOOK_MAX_CONN || '10', 10), // Allow configuration
                    allowed_updates: ["message"], // Only process message updates via webhook
                    // drop_pending_updates: true // Consider if restarts are frequent
                });
                console.log(`✅ Webhook successfully set to: ${webhookUrl}`);
                return true; // Indicate webhook was set successfully
            } catch (webhookError) {
                attempts++;
                console.error(`❌ Webhook setup attempt ${attempts}/${maxAttempts} failed:`, webhookError.message);
                 // Check for specific, non-retryable errors
                 if (webhookError.code === 'ETELEGRAM') {
                     if (webhookError.message.includes('URL host is empty')) {
                         console.error("❌❌❌ Webhook URL seems invalid. Check RAILWAY_PUBLIC_DOMAIN.");
                         return false; // Don't retry if URL is fundamentally broken
                     } else if (webhookError.message.includes('Unauthorized')) {
                         console.error("❌❌❌ Webhook setup failed (401 Unauthorized). Check BOT_TOKEN.");
                         return false; // Don't retry auth errors
                     }
                 }
                  // Check if max attempts reached
                 if (attempts >= maxAttempts) {
                     console.error("❌ Max webhook setup attempts reached. Continuing without webhook.");
                     return false; // Indicate webhook setup failed
                 }
                  // Wait before retrying
                 await new Promise(resolve => setTimeout(resolve, 3000 * attempts)); // Exponential backoff
            }
        }
    } else {
        // Not in Railway env or domain not set
        // console.log("ℹ️ Not in Railway environment or domain not set, webhook not configured."); // Reduce noise
        return false; // Indicate webhook was not configured
    }
    return false; // Default return if logic somehow falls through
}

// Encapsulated Polling Setup Logic
async function startPollingIfNeeded() {
    try {
        // Check if webhook is set first
        const info = await bot.getWebHookInfo().catch(e => {
             console.error("Error fetching webhook info:", e.message);
             // Assume no webhook if check fails, proceed to poll
             return null;
        });

        if (!info || !info.url) { // Start polling only if webhook is not set OR check failed
             if (bot.isPolling()) {
                 // console.log("ℹ️ Bot is already polling."); // Reduce noise
                 return; // Already polling, do nothing
             }
            console.log("ℹ️ Webhook not set (or check failed), starting bot polling...");
            // Ensure no residual webhook before starting polling
            await bot.deleteWebHook({ drop_pending_updates: true }).catch(e => console.warn(`Ignoring error during pre-polling delete: ${e.message}`));

            // Start polling
             await bot.startPolling({ /* options can be added here if needed */ });
            console.log("✅ Bot polling started successfully");
        } else {
            // Webhook IS set, ensure polling is stopped
            // console.log(`ℹ️ Webhook is set (${info.url}), polling will not be started.`); // Reduce noise
             if (bot.isPolling()) {
                 console.log("ℹ️ Stopping existing polling because webhook is now set.");
                 // Cancel pending updates during stop might be safer if switching modes
                 await bot.stopPolling({ cancel: true }).catch(e => console.warn(`Ignoring error during polling stop: ${e.message}`));
             }
        }
    } catch (err) {
        console.error("❌ Error managing polling state:", err.message);
        // Handle specific critical errors like conflicts or auth issues
        if (err.code === 'ETELEGRAM' && err.message.includes('409 Conflict')) {
            console.error("❌❌❌ Conflict detected during polling setup! Another instance might be running. Exiting.");
             shutdown('POLLING_CONFLICT_STARTUP', false).catch(() => process.exit(1));
             setTimeout(() => { console.error("Shutdown timed out after polling conflict. Forcing exit."); process.exit(1); }, 5000).unref();
        } else if (err.response && err.response.statusCode === 401) {
             console.error("❌❌❌ FATAL: Unauthorized (401) during polling setup. Check BOT_TOKEN. Exiting.");
             shutdown('BOT_TOKEN_INVALID_STARTUP', false).catch(() => process.exit(1));
             setTimeout(() => { console.error("Shutdown timed out after auth error. Forcing exit."); process.exit(1); }, 5000).unref();
        }
         // Log other errors but might allow the bot to continue if less critical
    }
}

// Encapsulated Payment Monitor Start Logic
function startPaymentMonitor() {
    if (monitorIntervalId) {
        // console.log("ℹ️ Payment monitor already running."); // Reduce noise
        return;
    }
    const monitorIntervalSeconds = parseInt(process.env.MONITOR_INTERVAL_SECONDS, 10);
    if (isNaN(monitorIntervalSeconds) || monitorIntervalSeconds < 10) {
        console.warn(`⚠️ Invalid or low MONITOR_INTERVAL_SECONDS (${process.env.MONITOR_INTERVAL_SECONDS}), defaulting to 30s.`);
        process.env.MONITOR_INTERVAL_SECONDS = '30'; // Set validated default back to env
    }
    const intervalMs = parseInt(process.env.MONITOR_INTERVAL_SECONDS, 10) * 1000;

    console.log(`⚙️ Starting payment monitor (Interval: ${intervalMs / 1000}s)`);
    monitorIntervalId = setInterval(() => {
        // Wrap monitorPayments call in try-catch to prevent interval from stopping on error
        try {
            monitorPayments().catch(err => { // Catch async errors from monitorPayments
                 console.error('❌ [MONITOR ASYNC ERROR - Caught in setInterval]:', err);
                 performanceMonitor.logRequest(false);
            });
        } catch (syncErr) { // Catch synchronous errors (less likely)
            console.error('❌ [MONITOR SYNC ERROR - Caught in setInterval]:', syncErr);
            performanceMonitor.logRequest(false);
        }
    }, intervalMs);

    // Run monitor once shortly after initialization completes
    const initialDelay = parseInt(process.env.MONITOR_INITIAL_DELAY_MS, 10);
    console.log(`⚙️ Scheduling initial payment monitor run in ${initialDelay / 1000}s...`);
    setTimeout(() => {
        console.log("⚙️ Performing initial payment monitor run...");
        try {
             monitorPayments().catch(err => {
                  console.error('❌ [MONITOR ASYNC ERROR - Initial Run]:', err);
                  performanceMonitor.logRequest(false);
             });
        } catch (syncErr) {
             console.error('❌ [MONITOR SYNC ERROR - Initial Run]:', syncErr);
             performanceMonitor.logRequest(false);
        }
    }, initialDelay);
}


// --- Graceful shutdown handler ---
let isShuttingDown = false; // Flag to prevent duplicate shutdown triggers
const shutdown = async (signal, isRailwayRotation = false) => {
    if (isShuttingDown) {
        console.log("Shutdown already in progress...");
        return;
    }
    isShuttingDown = true; // Set flag immediately

    console.log(`\n🛑 ${signal} received, ${isRailwayRotation ? 'initiating container rotation procedure' : 'shutting down gracefully'}...`);

    // --- Special handling for Railway container rotation ---
    if (isRailwayRotation) {
        console.log("Railway container rotation: Closing server, stopping monitor, allowing brief queue drain.");
         if (monitorIntervalId) { clearInterval(monitorIntervalId); monitorIntervalId = null; }
         if (server) { server.close(() => console.log("- Server closed for rotation.")); }
         // Allow a very short time for critical in-flight requests before Railway likely terminates
         const drainTimeout = parseInt(process.env.RAILWAY_ROTATION_DRAIN_MS, 10);
         console.log(`- Allowing ${drainTimeout}ms for queue drain...`);
         try {
             await Promise.race([
                 Promise.all([
                     paymentProcessor.highPriorityQueue.onIdle(),
                     paymentProcessor.normalQueue.onIdle()
                     // Don't necessarily wait for messageQueue or telegramSendQueue on quick rotation
                 ]),
                 new Promise(resolve => setTimeout(resolve, drainTimeout))
             ]);
             console.log("- Queue drain period ended.");
         } catch(e) {
             console.warn(`Ignoring error/timeout during rotation drain (${drainTimeout}ms):`, e.message);
         }
        console.log("Rotation procedure complete from app perspective.");
        // Don't explicitly exit process here for rotations. Railway handles termination.
        return; // Exit shutdown function early for rotations
    }

    // --- START: Full Shutdown Logic (for non-rotation signals like SIGINT/SIGTERM/Errors) ---
    console.log("Performing full graceful shutdown sequence...");
    isFullyInitialized = false; // Prevent monitor/new jobs
    isMonitorRunning = true; // Prevent monitor from starting new cycle during shutdown

    // 1. Stop receiving new events/requests
    console.log("Stopping incoming connections and tasks...");
    if (monitorIntervalId) {
        clearInterval(monitorIntervalId);
        monitorIntervalId = null;
        console.log("- Stopped payment monitor interval.");
    }
    try {
        // Close Express server first to stop new HTTP requests
        if (server) {
            console.log("- Closing Express server...");
            await new Promise((resolve, reject) => {
                const serverTimeout = setTimeout(() => reject(new Error('Server close timeout (5s)')), 5000);
                 if (serverTimeout.unref) serverTimeout.unref(); // Allow process to exit if only timer remains
                server.close((err) => {
                    clearTimeout(serverTimeout); // Clear timeout if close completes
                    if (err) {
                        console.error("⚠️ Error closing Express server:", err.message);
                        return reject(err); // Propagate error if needed
                    }
                    console.log("- Express server closed.");
                    resolve(undefined);
                });
            });
        }

        // Stop Telegram listeners gracefully
         console.log("- Stopping Telegram listeners...");
         if (bot.isPolling()) {
             // Allow pending updates to process before stopping fully? Maybe cancel=false is better?
             await bot.stopPolling({ cancel: true }).catch(e => console.warn("Ignoring error during polling stop:", e.message));
             console.log("- Stopped Telegram polling.");
         }
         // Attempt to remove webhook if it was set
         try {
             const webhookInfo = await bot.getWebHookInfo().catch(() => null);
             if (webhookInfo && webhookInfo.url) {
                 // Don't drop pending updates here if server is already closed
                 await bot.deleteWebHook({ drop_pending_updates: false }).catch(e => console.warn("Ignoring error during webhook delete:", e.message));
                 console.log("- Removed Telegram webhook.");
             }
         } catch (whErr) { console.warn("⚠️ Error checking/removing webhook:", whErr.message); }

    } catch (e) {
        console.error("⚠️ Error stopping listeners/server:", e.message);
        // Continue shutdown process even if stopping listeners fails
    }

    // 2. Wait for ongoing queue processing to finish (with timeout)
    const queueDrainTimeoutMs = parseInt(process.env.SHUTDOWN_QUEUE_TIMEOUT_MS, 10);
    console.log(`Waiting for active jobs to finish (max ${queueDrainTimeoutMs / 1000}s)...`);
    try {
        // Create a timeout promise
         const timeoutPromise = new Promise((_, reject) => {
             const timeout = setTimeout(() => reject(new Error(`Queue drain timeout (${queueDrainTimeoutMs}ms)`)), queueDrainTimeoutMs);
              if (timeout.unref) timeout.unref();
         });

         // Wait for all queues or timeout
         await Promise.race([
             Promise.all([
                 messageQueue.onIdle(),
                 paymentProcessor.highPriorityQueue.onIdle(),
                 paymentProcessor.normalQueue.onIdle(),
                 telegramSendQueue.onIdle() // Wait for outgoing TG messages too
             ]),
             timeoutPromise
         ]);
        console.log("- All processing queues are idle.");
    } catch (queueError) {
        // This catch block now specifically handles the timeout error
        console.warn(`⚠️ ${queueError.message}. Proceeding with shutdown.`);
        // Log queue sizes on timeout for debugging
         console.warn(`  Queue sizes on timeout: MsgQ=${messageQueue.size}, PayHP=${paymentProcessor.highPriorityQueue.size}, PayNP=${paymentProcessor.normalQueue.size}, TGSend=${telegramSendQueue.size}`);
    }

    // 3. Close database pool
    console.log("Closing database pool...");
    try {
        await pool.end(); // pool.end() waits for clients to be released
        console.log("✅ Database pool closed.");
    } catch (dbErr) {
        console.error("❌ Error closing database pool:", dbErr);
    } finally {
        console.log("🛑 Full Shutdown sequence complete.");
        // Exit cleanly after full shutdown attempt
        process.exit(signal === 'SIGINT' || signal === 'SIGTERM' ? 0 : 1);
    }
    // --- END: Full Shutdown Logic ---
};


// --- Signal handlers for graceful shutdown ---
process.on('SIGTERM', () => { // Commonly used by process managers (like Docker, systemd, Railway)
    const isRailway = !!process.env.RAILWAY_ENVIRONMENT;
    console.log(`SIGTERM received. Railway Environment: ${isRailway}`);
    // Pass isRailway flag to shutdown to potentially use rotation logic if applicable
    shutdown('SIGTERM', isRailway).catch((err) => {
        console.error("Error during SIGTERM shutdown:", err);
        process.exit(1); // Exit with error code if shutdown fails
    });
});

process.on('SIGINT', () => { // Ctrl+C in terminal
    console.log(`SIGINT (Ctrl+C) received.`);
    // SIGINT is usually manual, treat as full shutdown (not rotation)
    shutdown('SIGINT', false).catch((err) => {
        console.error("Error during SIGINT shutdown:", err);
        process.exit(1);
    });
});

// Handle uncaught exceptions - Last resort safeguard
process.on('uncaughtException', (err, origin) => {
    console.error(`🔥🔥🔥 Uncaught Exception at: ${origin}`, err);
    // Attempt a very quick shutdown, then force exit
    if (!isShuttingDown) { // Prevent recursive shutdown calls
         console.log("Attempting emergency shutdown due to uncaught exception...");
         shutdown('UNCAUGHT_EXCEPTION', false).catch(() => {}); // Ignore shutdown errors here
    }
    // Force exit after a short delay regardless of shutdown progress
    const emergencyExitTimeout = parseInt(process.env.SHUTDOWN_FAIL_TIMEOUT_MS, 10) || 5000;
    setTimeout(() => {
        console.error(`Forcing exit (${emergencyExitTimeout}ms) after uncaught exception.`);
        process.exit(1); // Exit with non-zero code
    }, emergencyExitTimeout).unref(); // unref allows node to exit if this timer is the only thing left
});

// Handle unhandled promise rejections
process.on('unhandledRejection', (reason, promise) => {
    console.error('🔥🔥🔥 Unhandled Rejection at:', promise, 'reason:', reason);
    // Optionally trigger shutdown for specific critical rejections
    // For now, just log. Add shutdown trigger if needed for specific reasons.
});


// --- Start the Application ---
const PORT = process.env.PORT || 3000;

// Start server immediately to respond to health checks quickly
server = app.listen(PORT, "0.0.0.0", () => { // Listen on 0.0.0.0 for container/network compatibility
    console.log(`🚀 Server listening on port ${PORT}...`);

    // Start heavy initialization *after* server starts listening
    const initDelay = parseInt(process.env.INIT_DELAY_MS, 10);
    console.log(`⚙️ Scheduling background initialization in ${initDelay / 1000}s...`);

    setTimeout(async () => {
        console.log("⚙️ Starting delayed background initialization...");
        try {
            console.log("  - Initializing Database...");
            await initializeDatabase();

            console.log("  - Setting up Telegram connection (Webhook/Polling)...");
            const webhookSet = await setupTelegramWebhook();
            if (!webhookSet) {
                await startPollingIfNeeded(); // Start polling only if webhook failed/not applicable
            }
            // Verify bot connection after setup attempt
             try {
                 const me = await bot.getMe();
                 console.log(`✅ Connected to Telegram as bot: @${me.username}`);
             } catch (tgError) {
                 console.error(`❌ Failed to verify Telegram connection after setup: ${tgError.message}`);
                 // If it's an auth error (401), it's fatal.
                 if (tgError.response && tgError.response.statusCode === 401) {
                     throw new Error("Invalid BOT_TOKEN detected during getMe check."); // Throw to trigger shutdown
                 }
                 // Treat other getMe errors as potentially serious connection issues
                 throw tgError;
             }

            console.log("  - Starting Payment Monitor...");
            startPaymentMonitor(); // Starts the interval and initial run

            // Optional: Adjust Solana Connection Concurrency after initial startup load
            const concurrencyAdjustDelay = parseInt(process.env.CONCURRENCY_ADJUST_DELAY_MS, 10);
            const targetConcurrency = parseInt(process.env.RPC_TARGET_CONCURRENT, 10);
            const initialConcurrency = parseInt(process.env.RPC_MAX_CONCURRENT, 10);
            // Check if adjustment is needed and configured
            if (solanaConnection?.options && targetConcurrency !== initialConcurrency && !isNaN(targetConcurrency) && !isNaN(initialConcurrency) && targetConcurrency > 0 && concurrencyAdjustDelay > 0) {
                 console.log(`⚙️ Scheduling Solana connection concurrency adjustment in ${concurrencyAdjustDelay / 1000}s...`);
                 setTimeout(() => {
                     if (solanaConnection?.options) { // Check again if connection exists
                         console.log(`⚡ Adjusting Solana connection concurrency from ${solanaConnection.options.maxConcurrent} to ${targetConcurrency}...`);
                         solanaConnection.options.maxConcurrent = targetConcurrency; // Directly modify the option
                         console.log(`✅ Solana maxConcurrent adjusted to ${targetConcurrency}`);
                     }
                 }, concurrencyAdjustDelay);
            }

            isFullyInitialized = true; // Mark as fully initialized *before* final ready message
            console.log("✅ Background Initialization Complete.");
            // Use process.env.npm_package_version if running via npm start and it's defined in package.json
            // ** VERSION UPDATED **
            const botVersion = process.env.npm_package_version || '2.6.0';
            console.log(`🚀🚀🚀 Solana Gambles Bot (v${botVersion}) is fully operational! 🚀🚀🚀`);

        } catch (initError) { // Catch errors from DB init, Telegram setup, etc.
            console.error("🔥🔥🔥 Delayed Background Initialization Failed:", initError);
            console.error("❌ Exiting due to critical initialization failure.");
            // Attempt graceful shutdown even on init error, then force exit
             if (!isShuttingDown) await shutdown('INITIALIZATION_FAILURE', false).catch(() => {});
             // Force exit after timeout
             const failTimeout = parseInt(process.env.SHUTDOWN_FAIL_TIMEOUT_MS, 10) || 5000;
             setTimeout(() => {
                 console.error(`Shutdown timed out after initialization failure. Forcing exit.`);
                 process.exit(1); // Force exit if shutdown hangs
             }, failTimeout).unref();
        }
    }, initDelay);
});

// Handle server startup errors (e.g., port already in use) immediately
server.on('error', (err) => {
    console.error('❌ Server startup error:', err);
    if (err.code === 'EADDRINUSE') {
        console.error(`❌❌❌ Port ${PORT} is already in use. Is another instance running? Exiting.`);
    } else {
        console.error("❌ Exiting due to unrecoverable server startup error.");
    }
    process.exit(1); // Exit immediately on critical server startup errors
});

// --- End of Part 4 / End of File ---
