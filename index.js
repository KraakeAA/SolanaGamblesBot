// index.js - Revised Part 1 (With Slots & Roulette additions)

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

// --- START: Enhanced Environment Variable Checks (MODIFIED for RPC_URLS & New Games) ---
const REQUIRED_ENV_VARS = [
    'BOT_TOKEN',
    'DATABASE_URL',
    'BOT_PRIVATE_KEY', // Coinflip, Slots, Roulette payouts (assuming reuse)
    'RACE_BOT_PRIVATE_KEY', // Race payouts
    'MAIN_WALLET_ADDRESS', // Coinflip, Slots, Roulette deposits (assuming reuse)
    'RACE_WALLET_ADDRESS', // Race deposits
    'RPC_URLS',           // Comma-separated list of RPC URLs
    // Optional vars with defaults are handled below
];

// Check for Railway-specific variables if deployed there
if (process.env.RAILWAY_ENVIRONMENT) {
    REQUIRED_ENV_VARS.push('RAILWAY_PUBLIC_DOMAIN');
}

// Optional vars with defaults - ADDED SLOTS & ROULETTE
const OPTIONAL_ENV_DEFAULTS = {
    'FEE_MARGIN': '5000', // Default 5000 lamports (0.000005 SOL)
    'PAYOUT_PRIORITY_FEE_RATE': '0.0001', // Default 0.01%
    'ADMIN_USER_IDS': '',
    'CF_MIN_BET': '0.01',
    'CF_MAX_BET': '1.0',
    'CF_EXPIRY_MINUTES': '15',
    'CF_HOUSE_EDGE': '0.02', // 2%
    'RACE_MIN_BET': '0.01',
    'RACE_MAX_BET': '1.0',
    'RACE_EXPIRY_MINUTES': '15',
    'RACE_HOUSE_EDGE': '0.02', // 2%
    'SLOTS_MIN_BET': '0.01', // Default for Slots
    'SLOTS_MAX_BET': '0.5',  // Default for Slots
    'SLOTS_EXPIRY_MINUTES': '10',  // Default for Slots
    'SLOTS_HOUSE_EDGE': '0.04', // Default 4% for Slots
    'ROULETTE_MIN_BET': '0.01', // Default for Roulette (per bet placement)
    'ROULETTE_MAX_BET': '1.0',  // Default for Roulette (total bet per spin)
    'ROULETTE_EXPIRY_MINUTES': '10',  // Default for Roulette
    'ROULETTE_HOUSE_EDGE': '0.027', // Default 2.7% (single zero wheel edge)
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
    'PAYMENT_EXPIRY_GRACE_MS': '60000'
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
        version: '2.3-slots-roulette' // Updated version string
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
        'User-Agent': `SolanaGamblesBot/2.3-slots-roulette`
    },
    clientId: `SolanaGamblesBot/2.3-slots-roulette`
});
console.log("✅ Multi-RPC Solana connection instance created.");


// --- LIVE QUEUE MONITOR ---
setInterval(() => {
    if (!solanaConnection?.getRequestStats) return;
    try {
        const statsInfo = solanaConnection.getRequestStats();
        if (!statsInfo?.status || !statsInfo?.stats) return;

        const { totalRequestsSucceeded = 0, totalRequestsFailed = 0 } = statsInfo.stats;
        const { queueSize = 0, activeRequests = 0 } = statsInfo.status; // Removed endpointRotations here as it's in stats.stats
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
                lastQueueProcessTime = Date.now();
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
            game_type TEXT NOT NULL, -- e.g., 'coinflip', 'race', 'slots', 'roulette'
            bet_details JSONB, -- Game specific details (e.g., {'choice':'heads'}, {'horse':'Blue'}, {}, {'bets': {'R': 10000000, 'S17': 5000000}})
            expected_lamports BIGINT NOT NULL CHECK (expected_lamports >= 0), -- Min bet enforced elsewhere
            memo_id TEXT UNIQUE NOT NULL,
            status TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            expires_at TIMESTAMPTZ NOT NULL,
            paid_tx_signature TEXT UNIQUE,
            payout_tx_signature TEXT UNIQUE,
            processed_at TIMESTAMPTZ,
            fees_paid BIGINT,
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
        await client.query(`ALTER TABLE bets ADD COLUMN IF NOT EXISTS fees_paid BIGINT;`);
        await client.query(`ALTER TABLE bets ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ;`);
        await client.query(`ALTER TABLE wallets ADD COLUMN IF NOT EXISTS last_used_at TIMESTAMPTZ;`);
        await client.query('COMMIT');
        console.log("  - Columns verified/added.");

        // Add indexes idempotently
        console.log("  - Creating indexes (if they don't exist)...");
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
             console.warn("  - Note: Some elements (table/index/constraint) already existed, which is usually okay.");
        } else {
             throw err;
        }
    } finally {
        if (client) client.release();
    }
}

// --- End of Part 1 ---
// index.js - Revised Part 2a (With Slots & Roulette additions)

// (Code continues directly from the end of Part 1)

// --- Telegram Bot Initialization with Queue ---
console.log("⚙️ Initializing Telegram Bot...");
const bot = new TelegramBot(process.env.BOT_TOKEN, {
    polling: false, // Use webhooks in production (set later in startup)
    request: {      // Adjust request options for stability
        timeout: 10000, // Request timeout: 10s
        agentOptions: {
            keepAlive: true,   // Reuse connections
            timeout: 60000    // Keep-alive timeout: 60s
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
    messageQueue.add(() => handleMessage(msg)) // handleMessage defined in Part 3
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
        message = message.substring(0, 4090) + '...';
    }

    // Automatically escape MarkdownV2 if specified
    if (options.parse_mode === 'MarkdownV2') {
        // Basic escaping function - adjust as needed for more complex cases
        const escapeMarkdownV2 = (text) => {
             // Escape characters: _ * [ ] ( ) ~ ` > # + - = | { } . !
             // Note: Already escaped characters (e.g., \\.) should ideally not be double-escaped.
             // This simple replace might double-escape if source text already contains escapes.
             // A more robust solution might use a regex that avoids double-escaping.
             // For now, applying basic escaping:
             return text.replace(/([_*\[\]()~`>#+\-=|{}.!])/g, '\\$1');
        };
        // Apply escaping ONLY IF parse_mode is MarkdownV2 in options
        // message = escapeMarkdownV2(message); // Let's apply escaping selectively in command handlers instead
    }


    return telegramSendQueue.add(() =>
        bot.sendMessage(chatId, message, options).catch(err => {
            console.error(`❌ Telegram send error to chat ${chatId}:`, err.message);
            if (err.response && err.response.statusCode === 403) {
                console.warn(`Bot may be blocked or kicked from chat ${chatId}.`);
            } else if (err.response && err.response.statusCode === 400 && err.message.includes("can't parse entities")) {
                 console.error(` -> Telegram Parse Error: ${err.message}. Check escaping in message content being sent.`);
            } else if (err.response && err.response.statusCode === 400 && err.message.includes("message is too long")) {
                console.error(` -> Confirmed message too long rejection for chat ${chatId}.`);
            } else if (err.response && err.response.statusCode === 429) {
                 console.warn(`Hit Telegram rate limit sending to ${chatId}. Queue should manage this.`);
            }
             return undefined;
        })
    );
}

console.log("✅ Telegram Bot initialized");


// --- Express Setup for Webhook and Health Check ---
app.use(express.json({ limit: '10kb' }));

// Original Health check / Info endpoint
app.get('/', (req, res) => {
    performanceMonitor.logRequest(true);
    const processor = paymentProcessor;
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
        version: '2.3-slots-roulette', // Updated version
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
                return res.status(400).send('Invalid request body');
            }
            bot.processUpdate(req.body);
            performanceMonitor.logRequest(true);
            res.sendStatus(200);
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
function validateGameConfig(config) { // Ensure this function is defined before use
     const errors = [];
     for (const game in config) {
         const gc = config[game];
         if (isNaN(gc.minBet) || gc.minBet <= 0) errors.push(`${game}.minBet`);
         if (isNaN(gc.maxBet) || gc.maxBet <= 0) errors.push(`${game}.maxBet`);
         if (isNaN(gc.expiryMinutes) || gc.expiryMinutes <= 0) errors.push(`${game}.expiryMinutes`);
         if (isNaN(gc.houseEdge) || gc.houseEdge < 0 || gc.houseEdge >= 1) errors.push(`${game}.houseEdge`);
     }
     return errors;
}

const GAME_CONFIG = {
    coinflip: {
        minBet: parseFloat(process.env.CF_MIN_BET),
        maxBet: parseFloat(process.env.CF_MAX_BET),
        expiryMinutes: parseInt(process.env.CF_EXPIRY_MINUTES, 10),
        houseEdge: parseFloat(process.env.CF_HOUSE_EDGE)
    },
    race: {
        minBet: parseFloat(process.env.RACE_MIN_BET),
        maxBet: parseFloat(process.env.RACE_MAX_BET),
        expiryMinutes: parseInt(process.env.RACE_EXPIRY_MINUTES, 10),
        houseEdge: parseFloat(process.env.RACE_HOUSE_EDGE)
    },
    slots: {
        minBet: parseFloat(process.env.SLOTS_MIN_BET),
        maxBet: parseFloat(process.env.SLOTS_MAX_BET),
        expiryMinutes: parseInt(process.env.SLOTS_EXPIRY_MINUTES, 10),
        houseEdge: parseFloat(process.env.SLOTS_HOUSE_EDGE)
    },
    roulette: {
        minBet: parseFloat(process.env.ROULETTE_MIN_BET),
        maxBet: parseFloat(process.env.ROULETTE_MAX_BET),
        expiryMinutes: parseInt(process.env.ROULETTE_EXPIRY_MINUTES, 10),
        houseEdge: parseFloat(process.env.ROULETTE_HOUSE_EDGE)
    }
};
const configErrors = validateGameConfig(GAME_CONFIG);
if (configErrors.length > 0) {
    console.error(`❌ Invalid game configuration values found for: ${configErrors.join(', ')}. Please check corresponding environment variables.`);
    process.exit(1);
}
console.log("ℹ️ Game Config Loaded:", JSON.stringify(GAME_CONFIG)); // Log loaded config


// Fee buffer
const FEE_BUFFER = BigInt(process.env.FEE_MARGIN);
// Priority fee rate
const PRIORITY_FEE_RATE = parseFloat(process.env.PAYOUT_PRIORITY_FEE_RATE); // Already validated

// Helper function for MarkdownV2 escaping (use this consistently)
const escapeMarkdownV2 = (text) => {
     if (typeof text !== 'string') text = String(text); // Convert non-strings
     // Escape characters: _ * [ ] ( ) ~ ` > # + - = | { } . !
     // Ensure backslashes are escaped first if they might already exist
     text = text.replace(/\\/g, '\\\\');
     return text.replace(/([_*\[\]()~`>#+\-=|{}.!])/g, '\\$1');
};


// --- Helper Functions (Existing) ---

function debugInstruction(inst, accountKeys) {
    // ... (implementation remains unchanged) ...
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
    // ... (implementation remains unchanged) ...
     if (!data) return null;
     try {
         if (typeof data === 'string') {
            if (/^[A-Za-z0-9+/]*={0,2}$/.test(data) && data.length % 4 === 0) {
                try { return Buffer.from(data, 'base64').toString('utf8'); } catch { /* ignore */ }
            }
             if (/[1-9A-HJ-NP-Za-km-z]{32,}/.test(data) && data.length < 60) {
                 try { return Buffer.from(bs58.decode(data)).toString('utf8'); } catch { /* ignore */ }
             }
             return data;
         }
         else if (Buffer.isBuffer(data) || data instanceof Uint8Array) {
           return Buffer.from(data).toString('utf8');
         }
         else if (Array.isArray(data) && data.every(n => typeof n === 'number')) {
            return Buffer.from(data).toString('utf8');
         }
         console.warn(`[decodeInstructionData] Unhandled data type: ${typeof data}`);
         return null;
     } catch (e) {
         return null;
     }
};


// --- End of Part 2a ---
// index.js - Revised Part 2b (With Slots & Roulette additions)

// (Code continues directly from the end of Part 2a)

// --- START: Memo Handling System ---
// (Updated for new game prefixes SL, RL)

// Define Memo Program IDs
const MEMO_V1_PROGRAM_ID = new PublicKey("Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo");
const MEMO_V2_PROGRAM_ID = new PublicKey("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr");
const MEMO_PROGRAM_IDS = [MEMO_V1_PROGRAM_ID.toBase58(), MEMO_V2_PROGRAM_ID.toBase58()];

// Allowed memo prefixes for V1 generation/validation
const VALID_MEMO_PREFIXES = ['BET', 'CF', 'RA', 'SL', 'RL']; // ADDED SL, RL

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
             return null;
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
        return null;
    }

    // 1. Try log messages first
    scanDepth = 1;
    if (transactionResponse.meta?.logMessages) {
        const memoLogRegex = /Program log: Memo(?: \(len \d+\))?:\s*"?([^"\n]+)"?/;
        for (const log of transactionResponse.meta.logMessages) {
            const match = log.match(memoLogRegex);
             if (match?.[1]) {
                 const rawMemo = match[1].trim();
                 const memo = normalizeMemo(rawMemo);
                 if (memo) {
                     usedMethods.push('LogScanRegex');
                     return memo;
                 }
             }
        }
    }

    // 2. Fallback to instruction parsing
    scanDepth = 2;
    const message = transactionResponse.transaction.message;
    let accountKeys = [];

    try {
        if (message.getAccountKeys) {
            const accounts = message.getAccountKeys();
            accountKeys = accounts.staticAccountKeys.map(k => k.toBase58());
        } else if (message.accountKeys) {
            accountKeys = message.accountKeys.map(k => k.toBase58 ? k.toBase58() : String(k));
        }
    } catch (e) {
        console.error(`[MEMO DEBUG] Sig ${signature?.slice(0,6)}: Error extracting account keys: ${e.message}`);
    }

    const allInstructions = [
        ...(message.instructions || []),
        ...(transactionResponse.meta?.innerInstructions || []).flatMap(inner => inner.instructions || [])
    ];

    if (allInstructions.length > 0) {
        for (const [i, inst] of allInstructions.entries()) {
            try {
                const programId = (inst.programIdIndex !== undefined && accountKeys[inst.programIdIndex])
                                      ? accountKeys[inst.programIdIndex]
                                      : null;

                if (programId && MEMO_PROGRAM_IDS.includes(programId)) {
                    const dataString = decodeInstructionData(inst.data);
                     if (dataString) {
                         const memo = normalizeMemo(dataString);
                         if (memo) {
                             const method = MEMO_PROGRAM_IDS.indexOf(programId) === 0 ? 'InstrParseV1' : 'InstrParseV2';
                             usedMethods.push(method);
                             return memo;
                         }
                     }
                }

                // Raw pattern matching fallback
                scanDepth = 3;
                let dataBuffer = null;
                if (inst.data) {
                  if (typeof inst.data === 'string') {
                      try { dataBuffer = Buffer.from(toByteArray(inst.data)); } catch { /* ignore */ }
                  } else if (Buffer.isBuffer(inst.data) || inst.data instanceof Uint8Array || Array.isArray(inst.data)) {
                      try { dataBuffer = Buffer.from(inst.data); } catch { /* ignore */ }
                  }
                }

                if (dataBuffer && dataBuffer.length >= 22) {
                    const potentialMemo = dataBuffer.toString('utf8');
                    const normalizedMemo = normalizeMemo(potentialMemo);

                    // Check if normalization resulted in a valid V1 format WE generate
                    if (normalizedMemo && validateOriginalMemoFormat(normalizedMemo)) {
                        usedMethods.push('PatternMatchV1');
                        return normalizedMemo;
                    }
                }
            } catch (e) {
                console.error(`[MEMO DEBUG] Sig ${signature?.slice(0,6)}: Error processing instruction ${i}:`, e?.message || e);
            }
        }
    }

    // 3. Deep Log Scan as final fallback
    scanDepth = 4;
    if (transactionResponse.meta?.logMessages) {
        const logString = transactionResponse.meta.logMessages.join('\n');
        // Updated regex to include SL, RL
        const logMemoMatch = logString.match(new RegExp(`(?:${VALID_MEMO_PREFIXES.join('|')})-[A-F0-9]{16}-[A-F0-9]{2}`));
        if (logMemoMatch?.[0]) {
            const recoveredMemo = normalizeMemo(logMemoMatch[0]);
            if (recoveredMemo) {
                usedMethods.push('DeepLogScanV1');
                return recoveredMemo;
            }
        }
    }

    if (message.addressTableLookups?.length > 0 && usedMethods.length === 0) {
      console.log(`[MEMO DEBUG] Sig ${signature?.slice(0,6)}: Transaction uses address lookup tables. No memo found via logs or instructions.`);
    }

    return null;
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
            console.warn(`DB: Memo ID collision for ${memoId}. Bet likely already exists.`);
            return { success: false, error: 'Bet with this Memo ID already exists. Please wait or try again.' };
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
            setTimeout(() => {
                const current = walletCache.get(cacheKey);
                if (current && current.wallet === linkedWallet && Date.now() - current.timestamp >= CACHE_TTL) {
                    walletCache.delete(cacheKey);
                }
            }, CACHE_TTL + 1000);
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
        walletCache.delete(cacheKey);
    }
    const query = `SELECT wallet_address FROM wallets WHERE user_id = $1`;
    try {
        const res = await pool.query(query, [String(userId)]);
        const wallet = res.rows[0]?.wallet_address;
        if (wallet) {
            walletCache.set(cacheKey, { wallet, timestamp: Date.now() });
            setTimeout(() => {
                const current = walletCache.get(cacheKey);
                 if (current && current.wallet === wallet && Date.now() - current.timestamp >= CACHE_TTL) {
                     walletCache.delete(cacheKey);
                 }
            }, CACHE_TTL + 1000);
        }
        return wallet;
    } catch (err) {
        console.error(`DB Error fetching wallet for user ${userId}:`, err.message);
        return undefined;
    }
}

async function updateBetStatus(betId, status) {
    const query = `UPDATE bets SET status = $1, processed_at = CASE WHEN $1 LIKE 'completed_%' OR $1 LIKE 'error_%' THEN NOW() ELSE processed_at END WHERE id = $2 RETURNING id;`;
    try {
        const res = await pool.query(query, [status, betId]);
        return res.rowCount > 0;
    } catch (err) {
        console.error(`DB Error updating bet ${betId} status to ${status}:`, err.message);
        return false;
    }
}

async function recordPayout(betId, status, signature) {
    const query = `
        UPDATE bets
        SET status = $1,
            payout_tx_signature = $2,
            processed_at = NOW()
        WHERE id = $3 AND status = 'processing_payout'
        RETURNING id;
    `;
    try {
        const res = await pool.query(query, [status, signature, betId]);
        if (res.rowCount > 0) {
            return true;
        } else {
            const current = await pool.query('SELECT status FROM bets WHERE id = $1', [betId]);
            console.warn(`DB: Failed to record payout for bet ${betId} (not found or status mismatch?). Current status: ${current.rows[0]?.status ?? 'Not Found'}`);
            return false;
        }
    } catch (err) {
         if (err.code === '23505' && err.constraint?.includes('payout_tx_signature')) {
             console.warn(`DB: Payout TX Signature ${signature.slice(0,10)}... collision for bet ${betId}. Already recorded.`);
             const current = await pool.query('SELECT status, payout_tx_signature FROM bets WHERE id = $1', [betId]);
              if(current.rows[0]?.payout_tx_signature === signature && current.rows[0]?.status.startsWith('completed_win')){
                  console.log(`DB: Bet ${betId} already correctly recorded with this payout signature. Treating as success.`);
                  return true;
              }
             return false;
         }
        console.error(`DB Error recording payout for bet ${betId}:`, err.message);
        return false;
    }
}


// --- Solana Transaction Analysis ---

/**
 * Analyzes a transaction to find SOL transfers to the target bot wallet.
 * Checks both top-level and inner instructions for transfers.
 * Uses balance changes as primary source, falls back to instruction parsing.
 * @param {import('@solana/web3.js').ParsedTransactionWithMeta | import('@solana/web3.js').VersionedTransactionResponse | null} tx - The transaction object from getParsedTransaction.
 * @param {'coinflip' | 'race' | 'slots' | 'roulette'} gameType - The type of game this payment relates to (determines target deposit address).
 * @returns {{ transferAmount: bigint, payerAddress: string | null }} The transfer amount in lamports and the detected payer address.
 */
function analyzeTransactionAmounts(tx, gameType) {
    let transferAmount = 0n;
    let payerAddress = null;

    // Determine target deposit address based on game type
    // Assume slots/roulette use MAIN_WALLET_ADDRESS like coinflip
    let targetAddress;
    if (gameType === 'race') {
        targetAddress = process.env.RACE_WALLET_ADDRESS;
    } else { // Coinflip, Slots, Roulette, etc.
        targetAddress = process.env.MAIN_WALLET_ADDRESS;
    }

    // Validation
    if (!tx || tx.meta?.err || !targetAddress) {
        return { transferAmount: 0n, payerAddress: null };
    }

    const getAccountKeys = (message) => {
        try {
            if (!message) return [];
            if (message.getAccountKeys) {
                const accounts = message.getAccountKeys();
                return accounts.staticAccountKeys.map(k => k.toBase58());
            } else if (message.accountKeys) {
                return message.accountKeys.map(k => k.toBase58 ? k.toBase58() : String(k));
            }
        } catch (e) {
            console.error("[analyzeAmounts] Error parsing account keys:", e.message);
        }
        return [];
    };

    // 1. Use balance changes
    let balanceChange = 0n;
    if (tx.meta?.preBalances && tx.meta?.postBalances && tx.transaction?.message) {
        const accountKeys = getAccountKeys(tx.transaction.message);
        const targetIndex = accountKeys.indexOf(targetAddress);

        if (targetIndex !== -1 && tx.meta.preBalances.length > targetIndex && tx.meta.postBalances.length > targetIndex && tx.meta.preBalances[targetIndex] !== null && tx.meta.postBalances[targetIndex] !== null) {
            balanceChange = BigInt(tx.meta.postBalances[targetIndex]) - BigInt(tx.meta.preBalances[targetIndex]);
            if (balanceChange > 0n) {
                transferAmount = balanceChange;
                const feePayerIndex = 0;
                if (accountKeys[feePayerIndex] && tx.meta.preBalances.length > feePayerIndex && tx.meta.postBalances.length > feePayerIndex && tx.meta.preBalances[feePayerIndex] !== null && tx.meta.postBalances[feePayerIndex] !== null) {
                    const preFeePayerBalance = BigInt(tx.meta.preBalances[feePayerIndex]);
                    const postFeePayerBalance = BigInt(tx.meta.postBalances[feePayerIndex]);
                    const feePayerChange = postFeePayerBalance - preFeePayerBalance;
                    const fee = BigInt(tx.meta.fee || 5000n);
                    if (feePayerChange <= -(transferAmount - fee) || feePayerChange <= -transferAmount) {
                        payerAddress = accountKeys[feePayerIndex];
                    }
                }
                if (!payerAddress && tx.transaction?.message?.header?.numRequiredSignatures > 1) {
                    for (let i = 1; i < tx.transaction.message.header.numRequiredSignatures; i++) {
                        if (!accountKeys[i] || !(tx.meta.preBalances.length > i && tx.meta.postBalances.length > i && tx.meta.preBalances[i] !== null && tx.meta.postBalances[i] !== null)) continue;
                        const preBalance = BigInt(tx.meta.preBalances[i]);
                        const postBalance = BigInt(tx.meta.postBalances[i]);
                        const signerBalanceChange = postBalance - preBalance;
                        if (signerBalanceChange <= -transferAmount) {
                            payerAddress = accountKeys[i];
                            break;
                        }
                    }
                }
            }
        }
    } // End balance check

    // 2. Check instructions
    if (tx.transaction?.message) {
        const accountKeys = getAccountKeys(tx.transaction.message);
        const SYSTEM_PROGRAM_ID = SystemProgram.programId.toBase58();
        const topLevelInstructions = tx.transaction.message.instructions || [];
        const innerInstructionsNested = tx.meta?.innerInstructions || [];
        const allInstructions = [
            ...topLevelInstructions.map(inst => ({ ...inst, type: 'topLevel' })),
            ...innerInstructionsNested.flatMap(innerSet =>
                (innerSet.instructions || []).map(inst => ({ ...inst, type: 'inner', parentIndex: innerSet.index }))
            )
        ];

        for (const instWrapper of allInstructions) {
            const inst = instWrapper;
            let programId = ''; let parsedInfo = null; let isParsedTransfer = false;
            try {
                if ('programId' in inst) {
                    programId = typeof inst.programId === 'string' ? inst.programId : inst.programId.toBase58();
                } else if ('programIdIndex' in inst && accountKeys.length > inst.programIdIndex) {
                    programId = accountKeys[inst.programIdIndex];
                }
                if (programId === SYSTEM_PROGRAM_ID) {
                    if ('parsed' in inst && inst.parsed?.type === 'transfer') {
                        isParsedTransfer = true;
                        parsedInfo = inst.parsed.info;
                    }
                }
                if (isParsedTransfer && parsedInfo?.destination === targetAddress) {
                    const instructionAmount = BigInt(parsedInfo.lamports || parsedInfo.amount || 0);
                    if (instructionAmount > 0n) {
                        if (transferAmount === 0n || instructionAmount === balanceChange) {
                            transferAmount = instructionAmount;
                            payerAddress = parsedInfo.source;
                            return { transferAmount, payerAddress }; // Found definitive transfer
                        } else if (transferAmount > 0n && instructionAmount !== balanceChange) {
                            console.warn(`[analyzeAmounts] Ambiguous amounts for ${targetAddress}. BalanceΔ: ${balanceChange}, InstrAmt: ${instructionAmount}. Using balance Δ.`);
                            if (!payerAddress && parsedInfo.source) payerAddress = parsedInfo.source;
                            return { transferAmount, payerAddress }; // Return potentially ambiguous result
                        }
                    }
                }
            } catch(parseError) {
                console.error(`[analyzeAmounts] Error processing instruction: ${parseError.message}`);
            }
        } // End instruction loop
    } // End instruction check block

    return { transferAmount, payerAddress };
}


// --- Payment Processing System ---

function isRetryableError(error) {
    const msg = error?.message?.toLowerCase() || '';
    const code = error?.code || error?.cause?.code;
    const status = error?.response?.status || error?.statusCode;

    if (status === 429 || msg.includes('429') || msg.includes('rate limit') || msg.includes('too many requests')) return true;
    if ([500, 502, 503, 504].includes(Number(status)) || msg.includes('server error')) return true;
    if (['timeout', 'timed out', 'econnreset', 'esockettimedout', 'network error', 'fetch', 'socket hang up', 'connection terminated', 'econnrefused', 'failed to fetch', 'getaddrinfo enotfound'].some(m => msg.includes(m)) ||
        ['ETIMEDOUT', 'ECONNRESET', 'ENETUNREACH', 'EAI_AGAIN', 'ECONNABORTED', 'ECONNREFUSED', 'UND_ERR_CONNECT_TIMEOUT', 'UND_ERR_HEADERS_TIMEOUT', 'UND_ERR_BODY_TIMEOUT', 'FETCH_TIMEOUT', 'FETCH_ERROR'].includes(code)) return true;
    if (msg.includes('connection terminated unexpectedly') || code === 'ECONNREFUSED' || code === '57P01' || code === '57P03') return true;
     if (msg.includes('transaction simulation failed') ||
         msg.includes('failed to simulate transaction') ||
         msg.includes('blockhash not found') ||
         msg.includes('slot leader does not match') ||
         msg.includes('node is behind')) return true;
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
        queue.add(() => this.processJob(job)).catch(queueError => {
            console.error(`Queue error processing job ${jobKey}:`, queueError.message);
            performanceMonitor.logRequest(false);
        });
    }

    async processJob(job) {
        const jobIdentifier = job.signature || job.betId;
        const jobKey = `${job.type}:${jobIdentifier || crypto.randomUUID()}`;

        if (this.activeProcesses.has(jobKey)) {
            return;
        }
        this.activeProcesses.add(jobKey);

        try {
            let result;
            if (job.type === 'monitor_payment') {
                // Determine correct walletType based on bet.game_type stored in DB
                // This requires fetching the bet based on a potential memo, which happens inside _processIncomingPayment
                // We pass the monitored walletType ('coinflip' or 'race') initially.
                result = await this._processIncomingPayment(job.signature, job.walletType); // Pass monitored wallet type
            } else if (job.type === 'process_bet') {
                const bet = await pool.query('SELECT * FROM bets WHERE id = $1', [job.betId]).then(res => res.rows[0]);
                if (bet) {
                    await processPaidBet(bet); // processPaidBet handles own errors
                    result = { processed: true };
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
                         await handlePayoutJob(job); // handlePayoutJob handles own errors
                         result = { processed: true };
                         break;
                     } catch (err) {
                         const errorMessage = err?.message || 'Unknown payout error';
                          const currentBetStatus = await pool.query('SELECT status FROM bets WHERE id = $1', [job.betId]).then(r => r.rows[0]?.status);
                          if (currentBetStatus?.startsWith('error_payout_') || currentBetStatus === 'completed_win_paid') {
                              result = { processed: false, reason: `payout_already_final_state: ${currentBetStatus}` };
                              break;
                          }
                          console.warn(`[PAYOUT_JOB_RETRY] Payout attempt ${attempt}/${retries} failed for bet ${job.betId}. Error: ${errorMessage}`);
                          if (attempt >= retries) {
                              console.error(`[PAYOUT_JOB_FAIL] Final payout attempt failed for bet ${job.betId}. Error: ${errorMessage}`);
                               if (!currentBetStatus?.startsWith('error_')) {
                                   await updateBetStatus(job.betId, 'error_payout_job_failed');
                                   await safeSendMessage(job.chatId, `⚠️ Payout for bet \`${escapeMarkdownV2(job.memoId)}\` failed after multiple retries\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
                               }
                              result = { processed: false, reason: `payout_retries_exhausted: ${errorMessage}` };
                              break;
                          }
                          if (!isRetryableError(err)) {
                              console.error(`[PAYOUT_JOB_FAIL] Error not deemed retryable for bet ${job.betId}. Aborting retries. Error: ${errorMessage}`);
                               if (!currentBetStatus?.startsWith('error_')) {
                                   await updateBetStatus(job.betId, 'error_payout_non_retryable');
                                   const safeErrorMsg = escapeMarkdownV2((errorMessage || 'Unknown Error').substring(0, 200)); // Escape and truncate
                                   await safeSendMessage(job.chatId, `⚠️ Payout for bet \`${escapeMarkdownV2(job.memoId)}\` failed with a non\\-retryable error: ${safeErrorMsg}\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
                               }
                              result = { processed: false, reason: `payout_non_retryable: ${errorMessage}` };
                              break;
                          }
                          const delay = baseRetryDelay * Math.pow(2, attempt - 1) + Math.random() * 1000;
                          await new Promise(resolve => setTimeout(resolve, delay));
                     }
                 }
            } else {
                console.error(`[PROCESS_JOB] Unknown job type: ${job.type}`);
                result = { processed: false, reason: 'unknown_job_type' };
            }
            if (result) performanceMonitor.logRequest(result.processed !== false);
            return result;
        } catch (error) {
            performanceMonitor.logRequest(false);
            console.error(`[PROCESS_JOB] Unhandled error processing job ${jobKey}:`, error.message, error.stack);
            if (job.betId && job.type !== 'monitor_payment') {
                 const currentStatusCheck = await pool.query('SELECT status FROM bets WHERE id = $1', [job.betId]).then(r => r.rows[0]?.status);
                 if (!currentStatusCheck?.startsWith('error_') && !currentStatusCheck?.startsWith('completed_')) {
                     const errorStatus = `error_${job.type}_uncaught`;
                     await updateBetStatus(job.betId, errorStatus);
                     console.log(`[PROCESS_JOB] Set bet ${job.betId} status to ${errorStatus} due to uncaught job error.`);
                 }
            }
             return { processed: false, reason: `uncaught_job_error: ${error.message}` };
        } finally {
            this.activeProcesses.delete(jobKey);
        }
    }

    // Internal method to process an incoming payment signature
    async _processIncomingPayment(signature, monitoredWalletType) { // Renamed param
        const logPrefix = `Sig ${signature.slice(0, 6)}...`;
        if (processedSignaturesThisSession.has(signature)) {
            return { processed: false, reason: 'already_processed_session' };
        }
        try {
            const exists = await pool.query('SELECT 1 FROM bets WHERE paid_tx_signature = $1 LIMIT 1', [signature]);
            if (exists.rowCount > 0) {
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
             if (!tx) {
                 return { processed: false, reason: 'fetch_failed_or_not_confirmed' };
             }
             if (tx.meta?.err){
                 console.log(`${logPrefix}: Transaction failed on-chain, skipping. Error: ${JSON.stringify(tx.meta.err)}`);
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
                 const existingBetStatus = await pool.query('SELECT status FROM bets WHERE memo_id = $1 LIMIT 1', [memo]).then(r=>r.rows[0]?.status);
                 if (existingBetStatus && existingBetStatus !== 'awaiting_payment') {
                     processedSignaturesThisSession.add(signature); this._cleanSignatureCache();
                     return { processed: false, reason: 'bet_already_processed_or_errored' };
                 }
                 return { processed: false, reason: 'no_matching_bet_found' };
             }

             // --- Determine correct walletType for analysis based on BET's game type ---
             let depositWalletType = bet.game_type; // Use game_type from the found bet
             if (bet.game_type === 'slots' || bet.game_type === 'roulette') {
                  depositWalletType = 'coinflip'; // Map to 'coinflip' if using MAIN wallet
             } else if (bet.game_type === 'race') {
                  depositWalletType = 'race'; // Use 'race' if it's a race bet
             }
             // --- End wallet type determination ---

             const processResult = await this._processPaymentGuaranteed(bet, signature, depositWalletType, tx);

              if (processResult.processed) {
                    const finalBetDetails = await pool.query('SELECT * FROM bets WHERE id = $1', [bet.id]).then(res => res.rows[0]);
                    if (finalBetDetails && finalBetDetails.status === 'payment_verified') {
                        await this._queueBetProcessing(finalBetDetails);
                    } else {
                        console.error(`${logPrefix}: CRITICAL! Failed post-verify fetch/status mismatch for Bet ID ${bet.id}. Status: ${finalBetDetails?.status ?? 'Not Found'}`);
                        await updateBetStatus(bet.id, 'error_post_verify_fetch');
                    }
              }
             return processResult;

        } catch (error) {
              console.error(`${logPrefix}: Critical error during _processIncomingPayment pipeline: ${error.message}`, error.stack);
              const isDefinitiveFailure = !isRetryableError(error) || error.reason === 'onchain_failure' || error.reason === 'no_valid_memo' || error.reason?.includes('_final');
              if (isDefinitiveFailure) {
                  processedSignaturesThisSession.add(signature); this._cleanSignatureCache();
              }
              if (error.betId && typeof error.betId === 'number') {
                   await updateBetStatus(error.betId, 'error_processing_exception');
              }
              return { processed: false, reason: `processing_pipeline_error: ${error.message}` };
        }
    }

    async _getTransactionWithRetry(signature) {
        try {
            const tx = await solanaConnection.getParsedTransaction(signature, {
                maxSupportedTransactionVersion: 0,
                commitment: 'confirmed'
            });
             return tx;
        } catch (error) {
             console.error(`Sig ${signature?.slice(0,6)}: Final error from getParsedTransaction: ${error.message}`);
             return null;
        }
    }

    async _extractMemoGuaranteed(tx, signature) {
        // Simple wrapper, actual logic is in findMemoInTx
        return findMemoInTx(tx, signature);
    }

    async _findBetGuaranteed(memo) {
        // Check cache first
        const cachedBet = this.memoCache.get(memo);
        if (cachedBet && Date.now() - cachedBet.timestamp < this.cacheTTL) {
            // console.log(`[MEMO CACHE] Hit for memo ${memo}`); // Reduce noise
            // Check if status is still awaiting_payment before returning from cache
             if(cachedBet.bet?.status === 'awaiting_payment'){
                  return cachedBet.bet;
             } else {
                 this.memoCache.delete(memo); // Remove if status changed
                 // console.log(`[MEMO CACHE] Removed memo ${memo} due to status change.`); // Reduce noise
                 // Fall through to DB query
             }
        } else if (cachedBet) {
             this.memoCache.delete(memo); // Delete expired entry
             // console.log(`[MEMO CACHE] Expired memo ${memo}`); // Reduce noise
        }

        // DB lookup with retry logic for potential transient connection issues
        let retries = 3;
        let lastError = null;
        while (retries > 0) {
            try {
                const res = await pool.query(
                    "SELECT * FROM bets WHERE memo_id = $1 AND status = 'awaiting_payment' ORDER BY created_at DESC LIMIT 1", // Ensure we get the latest if somehow duplicated
                    [memo]
                );
                const bet = res.rows[0];
                if (bet) {
                    // Cache the found bet
                    this.memoCache.set(memo, { bet, timestamp: Date.now() });
                    // Set TTL for cache entry deletion
                    setTimeout(() => {
                        const current = this.memoCache.get(memo);
                        if (current && current.bet?.id === bet.id && Date.now() - current.timestamp >= this.cacheTTL) {
                            this.memoCache.delete(memo);
                        }
                    }, this.cacheTTL + 1000);
                }
                return bet; // Return bet object or undefined
            } catch (dbError) {
                lastError = dbError;
                retries--;
                console.error(`DB error finding bet for memo ${memo} (Attempt ${4-retries}/3): ${dbError.message}`);
                if (!isRetryableError(dbError) || retries === 0) {
                     console.error(`[FIND_BET] Non-retryable DB error or retries exhausted for memo ${memo}.`);
                     return undefined; // Give up
                }
                await new Promise(r => setTimeout(r, 500 * (3 - retries))); // Short backoff
            }
        }
        // Should not be reached if loop logic is correct, but satisfy TS/ESLint
        return undefined;
    }

    // Performs payment validation and updates DB within a transaction
    async _processPaymentGuaranteed(bet, signature, walletType, tx) { // Accept tx object
        const logPrefix = `Bet ${bet.id} (Memo ${bet.memo_id.slice(0,8)}...)`;
        const client = await pool.connect(); // Get a dedicated client for transaction
        try {
            await client.query('BEGIN');

            // 1. Re-verify bet status inside transaction to prevent race conditions
            const currentStatusRes = await client.query(
                 'SELECT status FROM bets WHERE id = $1 FOR UPDATE', // Lock row
                 [bet.id]
            );
            const currentStatus = currentStatusRes.rows[0]?.status;
            if (!currentStatus || currentStatus !== 'awaiting_payment') {
                 await client.query('ROLLBACK');
                 // console.log(`${logPrefix}: Status check failed (Expected 'awaiting_payment', got '${currentStatus}').`); // Reduce noise
                 return { processed: false, reason: `status_mismatch: ${currentStatus}` };
            }

             // 2. Re-check expiry (use DB expiry time)
             const expiryTime = new Date(bet.expires_at);
             const now = Date.now();
             const expiryGraceMs = parseInt(process.env.PAYMENT_EXPIRY_GRACE_MS || '60000', 10); // 1 min grace default
             if (now > expiryTime.getTime() + expiryGraceMs) { // Allow grace period
                 await client.query(`UPDATE bets SET status = $1, processed_at = NOW() WHERE id = $2`, ['error_expired_payment', bet.id]);
                 await client.query('COMMIT');
                 console.log(`${logPrefix}: Expired before payment confirmed (Expired At: ${expiryTime.toISOString()}, Now: ${new Date(now).toISOString()}).`);
                 return { processed: false, reason: 'expired_before_confirmation' };
             }


            // 3. Analyze transaction amount and get payer
            const { transferAmount, payerAddress } = analyzeTransactionAmounts(tx, walletType); // Pass walletType
             if (transferAmount <= 0n) {
                 await client.query(`UPDATE bets SET status = $1, processed_at = NOW() WHERE id = $2`, ['error_payment_amount_zero', bet.id]);
                 await client.query('COMMIT');
                 console.log(`${logPrefix}: No valid transfer amount found in transaction ${signature}.`);
                 return { processed: false, reason: 'transfer_amount_zero' };
             }

            // 4. Verify amount with tolerance
             const expected = BigInt(bet.expected_lamports);
             const tolerance = BigInt(process.env.PAYMENT_TOLERANCE_LAMPORTS || '5000'); // Allow small tolerance
             if (transferAmount < expected - tolerance) {
                 await client.query(`UPDATE bets SET status = $1, processed_at = NOW() WHERE id = $2`, ['error_payment_amount_low', bet.id]);
                 await client.query('COMMIT');
                 console.log(`${logPrefix}: Payment amount too low. Expected >=${expected - tolerance}, Got ${transferAmount}.`);
                  // Notify user about incorrect amount
                  await safeSendMessage(bet.chat_id, `⚠️ Your payment for bet \`${escapeMarkdownV2(bet.memo_id)}\` was received, but the amount was too low\\. Expected ${escapeMarkdownV2((Number(expected)/LAMPORTS_PER_SOL).toFixed(6))} SOL, but received ${escapeMarkdownV2((Number(transferAmount)/LAMPORTS_PER_SOL).toFixed(6))} SOL\\. Your bet could not be processed\\.`, { parse_mode: 'MarkdownV2' });
                 return { processed: false, reason: 'payment_amount_low' };
             } else if (transferAmount > expected + tolerance) {
                 console.warn(`${logPrefix}: Payment amount slightly higher than expected. Expected ${expected}, Got ${transferAmount}. Processing anyway.`);
                 // Log overpayment but continue processing
             } else {
                  // console.log(`${logPrefix}: Payment amount verified.`); // Reduce noise
             }


            // 5. Update bet status and record signature
            await client.query(
                'UPDATE bets SET status = $1, paid_tx_signature = $2 WHERE id = $3',
                ['payment_verified', signature, bet.id]
            );

            // 6. Link wallet if payer found and different from current linked wallet
            if (payerAddress) {
                 const currentLinkedWallet = await getLinkedWallet(bet.user_id); // Use helper to check cache
                 if (currentLinkedWallet !== payerAddress) {
                     // console.log(`${logPrefix}: Linking payer wallet ${payerAddress.slice(0,6)}... for user ${bet.user_id}.`); // Reduce noise
                      await client.query(
                          `INSERT INTO wallets (user_id, wallet_address, last_used_at)
                           VALUES ($1, $2, NOW())
                           ON CONFLICT (user_id)
                           DO UPDATE SET wallet_address = EXCLUDED.wallet_address, last_used_at = NOW()`,
                          [String(bet.user_id), payerAddress]
                      );
                      // Update cache immediately
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
            // console.log(`${logPrefix}: Payment processing committed successfully.`); // Reduce noise
            processedSignaturesThisSession.add(signature); // Add to session cache AFTER successful commit
             this._cleanSignatureCache();
            return { processed: true }; // Indicate success

        } catch (error) {
            // console.error(`${logPrefix}: Error during DB transaction for payment processing: ${error.message}`); // Reduce noise
            await client.query('ROLLBACK'); // Rollback on any error
            // Attempt to set a generic processing error status if possible
            try {
                 await pool.query(`UPDATE bets SET status = $1, processed_at = NOW() WHERE id = $2 AND status = 'awaiting_payment'`, ['error_processing_db', bet.id]);
            } catch (updateErr) {
                 console.error(`${logPrefix}: Failed to set error status after rollback: ${updateErr.message}`);
            }
            // Add betId context to the error for higher-level handling if needed
            error.betId = bet.id;
            throw error; // Re-throw the error to be caught by processJob

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
            // Pass necessary details if needed by processJob later, though it refetches
            // gameType: bet.game_type,
            priority: bet.priority || 0
        });
    }


     // Helper method to clean up the signature cache periodically or when it grows large
     _cleanSignatureCache() {
         if (processedSignaturesThisSession.size > MAX_PROCESSED_SIGNATURES) {
              console.warn(`[SigCache] Reached max size (${MAX_PROCESSED_SIGNATURES}). Clearing cache.`);
              processedSignaturesThisSession.clear();
         }
         // Optionally implement time-based cleanup if needed
     }

} // End GuaranteedPaymentProcessor Class

// Instantiate the processor
const paymentProcessor = new GuaranteedPaymentProcessor();
console.log("✅ Payment Processor instantiated.");

// --- End of Part 2b ---
// index.js - Revised Part 3 (With Slots & Roulette additions)

// (Code continues directly from the end of Part 2b)

// --- Payment Monitoring Loop ---
let isMonitorRunning = false;
const botStartupTime = Math.floor(Date.now() / 1000);
let monitorIntervalId = null;

async function monitorPayments() {
    if (isMonitorRunning) return;
    if (!isFullyInitialized) return;

    isMonitorRunning = true;
    const startTime = Date.now();
    let signaturesFoundThisCycle = 0;
    let signaturesQueuedThisCycle = 0;

    try {
        // Optional Throttling based on Payment Processor Queue Load
        const paymentQueueLoad = (paymentProcessor.highPriorityQueue.size + paymentProcessor.normalQueue.size +
                                 paymentProcessor.highPriorityQueue.pending + paymentProcessor.normalQueue.pending);
        const monitorThrottleMs = parseInt(process.env.MONITOR_THROTTLE_MS_PER_ITEM, 10);
        const maxMonitorThrottle = parseInt(process.env.MONITOR_MAX_THROTTLE_MS, 10);
        const throttleDelay = Math.min(maxMonitorThrottle, paymentQueueLoad * monitorThrottleMs);
        if (throttleDelay > 100) {
            console.log(`[Monitor] Payment queues have ${paymentQueueLoad} items. Throttling monitor check for ${throttleDelay}ms.`);
            await new Promise(resolve => setTimeout(resolve, throttleDelay));
        }
        // --- End Optional Throttle ---

        // Define wallets to monitor - Use 'coinflip' type for MAIN wallet as it maps to correct payout key if needed
        const monitoredWallets = [
             { address: process.env.MAIN_WALLET_ADDRESS, type: 'coinflip', priority: 0 }, // Monitors MAIN for coinflip, slots, roulette
             { address: process.env.RACE_WALLET_ADDRESS, type: 'race', priority: 0 },     // Monitors RACE for races
        ];

        for (const wallet of monitoredWallets) {
            const walletAddress = wallet.address;
            if (!walletAddress) {
                // console.warn(`[Monitor] Skipping wallet type ${wallet.type} - address not configured.`); // Reduce noise
                continue;
            }

             // Add jitter
             const jitter = Math.random() * (parseInt(process.env.MONITOR_WALLET_JITTER_MS, 10));
             await new Promise(resolve => setTimeout(resolve, jitter));

            let signaturesForWallet = [];
            try {
                const fetchLimit = parseInt(process.env.MONITOR_FETCH_LIMIT, 10);
                const options = { limit: fetchLimit, commitment: 'confirmed' };

                signaturesForWallet = await solanaConnection.getSignaturesForAddress(
                    new PublicKey(walletAddress),
                    options
                );

                if (!signaturesForWallet || signaturesForWallet.length === 0) {
                    continue;
                }

                signaturesFoundThisCycle += signaturesForWallet.length;

                // Filter out old/failed TXs
                const startupBufferSeconds = 600;
                const recentSignatures = signaturesForWallet.filter(sigInfo => {
                    if (sigInfo.err) {
                        if (!processedSignaturesThisSession.has(sigInfo.signature)) {
                             processedSignaturesThisSession.add(sigInfo.signature);
                             paymentProcessor._cleanSignatureCache();
                        }
                        return false;
                    }
                    if (sigInfo.blockTime && sigInfo.blockTime < (botStartupTime - startupBufferSeconds)) {
                        return false;
                    }
                    return true;
                });

                if (recentSignatures.length === 0) continue;

                // Process oldest first
                recentSignatures.reverse();

                for (const sigInfo of recentSignatures) {
                    if (processedSignaturesThisSession.has(sigInfo.signature)) continue;
                    const jobKey = `monitor_payment:${sigInfo.signature}`;
                     if (paymentProcessor.activeProcesses.has(jobKey)) continue;

                    signaturesQueuedThisCycle++;
                    // Pass the monitored wallet type ('coinflip' or 'race') to the job
                    await paymentProcessor.addPaymentJob({
                        type: 'monitor_payment',
                        signature: sigInfo.signature,
                        walletType: wallet.type, // Pass the type associated with the address being monitored
                        priority: wallet.priority,
                    });
                } // End loop through signatures

            } catch (error) {
                console.error(`[Monitor] Error fetching/processing signatures for wallet ${walletAddress}:`, error.message);
                performanceMonitor.logRequest(false);
            }
        } // End loop through wallets

    } catch (err) {
        console.error('❌ MonitorPayments Error in main try block:', err);
        performanceMonitor.logRequest(false);
    } finally {
        isMonitorRunning = false; // Release the lock
        const duration = Date.now() - startTime;
        if (signaturesFoundThisCycle > 0 || duration > 1500) {
             // console.log(`[Monitor] Cycle completed in ${duration}ms. Found:${signaturesFoundThisCycle}. Queued:${signaturesQueuedThisCycle}.`); // Reduce noise
        }
    }
}


// --- SOL Sending Function ---
/**
 * Sends SOL to a recipient, handling priority fees and confirmation.
 * Relies on RateLimitedConnection for underlying RPC calls.
 * @param {string | PublicKey} recipientPublicKey - The recipient's address.
 * @param {bigint} amountLamports - The amount to send in lamports (MUST be BigInt).
 * @param {'coinflip' | 'race'} payoutWalletType - Determines which private key to use ('coinflip' uses BOT_PRIVATE_KEY, 'race' uses RACE_BOT_PRIVATE_KEY).
 * @returns {Promise<{success: boolean, signature?: string, error?: string}>} Result object.
 */
async function sendSol(recipientPublicKey, amountLamports, payoutWalletType) {
    const operationId = `sendSol-${payoutWalletType}-${Date.now().toString().slice(-6)}`;
    // console.log(`DEBUG_SENDSOL: ${operationId} - Entering function.`); // Reduce noise

    // Determine correct private key based on payoutWalletType
    const privateKeyEnvVar = payoutWalletType === 'race' ? 'RACE_BOT_PRIVATE_KEY' : 'BOT_PRIVATE_KEY';
    const privateKey = process.env[privateKeyEnvVar];

    if (!privateKey) {
        console.error(`[${operationId}] ❌ ERROR: Missing private key env var ${privateKeyEnvVar}.`);
        return { success: false, error: `Missing private key for ${payoutWalletType}` };
    }

    let recipientPubKey;
    try {
        recipientPubKey = (typeof recipientPublicKey === 'string') ? new PublicKey(recipientPublicKey) : recipientPublicKey;
        if (!(recipientPubKey instanceof PublicKey)) throw new Error("Invalid recipient public key type");
    } catch (e) {
        console.error(`[${operationId}] ❌ ERROR: Invalid recipient address format: "${recipientPublicKey}". Error: ${e.message}`);
        return { success: false, error: `Invalid recipient address: ${e.message}` };
    }

    // Validate amount (should already be BigInt if called correctly)
    let amountToSend;
    try {
        amountToSend = BigInt(amountLamports); // Ensure it's BigInt
        if (amountToSend <= 0n) {
            console.error(`[${operationId}] ❌ ERROR: Payout amount ${amountToSend} is zero or negative.`);
            return { success: false, error: 'Payout amount is zero or negative' };
        }
    } catch (e) {
        console.error(`[${operationId}] ❌ ERROR: Failed to convert input amountLamports '${amountLamports}' to BigInt. Error: ${e.message}`);
        return { success: false, error: `Invalid payout amount format: ${e.message}` };
    }

    const amountSOL = Number(amountToSend) / LAMPORTS_PER_SOL;

    // Calculate Priority Fee
    const basePriorityFee = parseInt(process.env.PAYOUT_BASE_PRIORITY_FEE_MICROLAMPORTS, 10);
    const maxPriorityFee = parseInt(process.env.PAYOUT_MAX_PRIORITY_FEE_MICROLAMPORTS, 10);
    const calculatedFee = Math.floor(Number(amountToSend) * PRIORITY_FEE_RATE); // PRIORITY_FEE_RATE is global
    const priorityFeeMicroLamports = Math.max(basePriorityFee, Math.min(calculatedFee, maxPriorityFee));

    if (isNaN(priorityFeeMicroLamports)) {
         console.error(`[${operationId}] ❌ ERROR: NaN detected during priority fee calculation! base=${basePriorityFee}, max=${maxPriorityFee}, calc=${calculatedFee}, final=${priorityFeeMicroLamports}`);
         return { success: false, error: 'Failed to calculate priority fee (NaN result)' };
    }
    // console.log(`DEBUG_SENDSOL: ${operationId} - Using Priority Fee: ${priorityFeeMicroLamports} microLamports`); // Reduce noise


    try {
        // console.log(`DEBUG_SENDSOL: ${operationId} - Entering transaction build/send block...`); // Reduce noise
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

        transaction.add(
            SystemProgram.transfer({
                fromPubkey: payerWallet.publicKey,
                toPubkey: recipientPubKey,
                lamports: amountToSend
            })
        );

        const confirmationTimeoutMs = parseInt(process.env.PAYOUT_CONFIRM_TIMEOUT_MS, 10);
        // console.log(`DEBUG_SENDSOL: ${operationId} - Calling sendAndConfirmTransaction...`); // Reduce noise

        // Use the main solanaConnection instance which handles rate limiting/rotation
        const signature = await sendAndConfirmTransaction(
            solanaConnection, // Use the RateLimitedConnection instance
            transaction,
            [payerWallet],
            {
                commitment: 'confirmed',
                skipPreflight: false, // Keep preflight for safety unless causing issues
                maxRetries: parseInt(process.env.SCT_MAX_RETRIES || '3', 10), // Retries for sendAndConfirm itself
                preflightCommitment: 'confirmed',
                lastValidBlockHeight: latestBlockhash.lastValidBlockHeight,
            }
        );

        console.log(`[${operationId}] SUCCESS! ✅ Sent ${amountSOL.toFixed(6)} SOL to ${recipientPubKey.toBase58()}. TX: ${signature.slice(0,10)}...`);
        return { success: true, signature };

    } catch (error) {
        console.error(`[${operationId}] ❌ SEND FAILED. Error message:`, error.message);
        if (error.logs) {
             console.error(`[${operationId}] Simulation Logs (last 10):`);
             error.logs.slice(-10).forEach(log => console.error(`   -> ${log}`));
        }

        const errorMsg = error.message.toLowerCase();
        let returnError = error.message; // Default to original message

        // Classify error for retry logic / user message
        if (errorMsg.includes('insufficient funds')) { returnError = 'Insufficient funds in payout wallet.'; }
        else if (errorMsg.includes('blockhash not found') || errorMsg.includes('block height exceeded')) { returnError = 'Transaction expired (blockhash invalid/expired). Retryable.'; } // Added block height exceeded
        else if (errorMsg.includes('transaction was not confirmed') || errorMsg.includes('timed out')) { returnError = `Transaction confirmation timeout (${confirmationTimeoutMs / 1000}s). May succeed later. Retryable.`;}
        else if (errorMsg.includes('custom program error') || errorMsg.includes('invalid account data')) { returnError = `Permanent chain error: ${error.message}`; }
        else if (isRetryableError(error)) { returnError = `Temporary network/RPC error: ${error.message}. Retryable.`; } // Check using helper
        else { returnError = `Send/Confirm error: ${error.message}`; } // Default for other errors

        return { success: false, error: returnError };
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
            return;
        }

        // Update status to 'processing_game' within the transaction
        await client.query(
            'UPDATE bets SET status = $1 WHERE id = $2',
            ['processing_game', bet.id]
        );
        await client.query('COMMIT'); // Commit status change and release lock

        // Call the appropriate game handler *after* releasing the lock
        if (bet.game_type === 'coinflip') {
            await handleCoinflipGame(bet);
        } else if (bet.game_type === 'race') {
            await handleRaceGame(bet);
        } else if (bet.game_type === 'slots') { // ADDED SLOTS ROUTE
            await handleSlotsGame(bet);
        } else if (bet.game_type === 'roulette') { // ADDED ROULETTE ROUTE
            await handleRouletteGame(bet);
        } else {
            console.error(`❌ Unknown game type '${bet.game_type}' for bet ${bet.id}`);
            await updateBetStatus(bet.id, 'error_unknown_game'); // Update status outside transaction
        }
    } catch (error) {
        console.error(`❌ Error during game processing setup for bet ${bet.id} (${bet.memo_id}):`, error.message);
        if (client) {
            try { await client.query('ROLLBACK'); } catch (rbError) { console.error("Rollback failed:", rbError); }
        }
        // Mark bet with error status - do this outside the failed transaction
        await updateBetStatus(bet.id, 'error_processing_setup');
    } finally {
        if (client) client.release(); // Ensure client is always released
    }
}

// --- Utility Functions (Existing) ---

// Calculates payout amount based on winnings and house edge (MODIFIED)
// Now expects gross winnings calculated by game logic, applies house edge
function calculateNetPayout(grossWinningsLamports, gameType) {
    try {
        const winnings = BigInt(grossWinningsLamports); // Ensure input is BigInt
        if (winnings <= 0n) return 0n; // No payout if no gross win

        const config = GAME_CONFIG[gameType];
        if (!config || typeof config.houseEdge !== 'number' || config.houseEdge < 0 || config.houseEdge >= 1) {
            console.error(`[calculateNetPayout] Invalid house edge configuration for game type "${gameType}". Returning 0 payout.`);
            return 0n;
        }

        // Calculate the house cut (as a BigInt to avoid precision loss later)
        // Multiply first, then divide to maintain precision with integers
        const houseCut = (winnings * BigInt(Math.round(config.houseEdge * 10000))) / 10000n;

        const netPayout = winnings - houseCut;

        // console.log(`DEBUG_PAYOUT_CALC: Gross=${winnings}, HE=${config.houseEdge}, Cut=${houseCut}, Net=${netPayout}`); // Debug log

        return netPayout > 0n ? netPayout : 0n; // Ensure payout is not negative

    } catch (e) {
        console.error(`[calculateNetPayout] Error calculating net payout for ${gameType} (Input: ${grossWinningsLamports}): ${e.message}`);
        return 0n; // Return 0 on error
    }
}

async function getUserDisplayName(chat_id, user_id) {
     try {
         const chatMember = await bot.getChatMember(chat_id, user_id);
         const user = chatMember.user;
         // Prioritize username, fallback to first name, finally use partial ID
         const name = user.username ? `@${user.username}` : (user.first_name || `User_${String(user_id).slice(-4)}`);
         return escapeMarkdownV2(name); // Use central escape function
     } catch (e) {
         // Handle user not found or other errors
         // console.warn(`Couldn't get username/name for user ${user_id} in chat ${chat_id}:`, e.message); // Reduce noise
          const fallbackName = `User_${String(user_id).slice(-4)}`;
          return escapeMarkdownV2(fallbackName); // Escape fallback too
     }
}


// --- Game Logic Implementation (Existing & New) ---

async function handleCoinflipGame(bet) {
    const { id: betId, user_id, chat_id, bet_details, expected_lamports, memo_id } = bet;
    const choice = bet_details.choice;
    const config = GAME_CONFIG.coinflip;
    const logPrefix = `CF Bet ${betId} (${memo_id.slice(0, 6)}...)`;

    const houseWinRoll = Math.random();
    let result;
    let isHouseWin = houseWinRoll < config.houseEdge;

    if (isHouseWin) {
        result = (choice === 'heads') ? 'tails' : 'heads';
        // console.log(`${logPrefix}: House edge triggered.`); // Reduce noise
    } else {
        result = (Math.random() < 0.5) ? 'heads' : 'tails';
    }

    const win = (result === choice); // Player wins if result matches choice

    let payoutLamports = 0n;
    if (win) {
        // Coinflip payout is typically 2x minus house edge on the *winnings portion*
        // Gross win = Bet * 2; House cut = (Gross Win - Original Bet) * HE; Net Payout = Gross Win - House Cut
        const grossWin = BigInt(expected_lamports) * 2n;
        const profit = grossWin - BigInt(expected_lamports); // = expected_lamports
        const houseCut = (profit * BigInt(Math.round(config.houseEdge * 10000))) / 10000n;
        payoutLamports = grossWin - houseCut; // Net payout including original stake
        payoutLamports = payoutLamports > 0n ? payoutLamports : 0n; // Ensure not negative
    }

    const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
    let displayName = await getUserDisplayName(chat_id, user_id);

    if (win && payoutLamports > 0n) {
        // console.log(`${logPrefix}: ${displayName} WON ${payoutSOL.toFixed(6)} SOL! Choice: ${choice}, Result: ${result}`); // Reduce noise
        const winnerAddress = await getLinkedWallet(user_id);
        if (!winnerAddress) {
            await updateBetStatus(betId, 'completed_win_no_wallet');
            await safeSendMessage(chat_id,
                `🎉 ${displayName}, you won the coinflip \\(Result: *${escapeMarkdownV2(result)}*\\) but have no wallet linked\\!\n` +
                `Your payout of ${escapeMarkdownV2(payoutSOL.toFixed(6))} SOL is waiting\\. Place another bet \\(any amount\\) to link your wallet and receive pending payouts\\.`,
                { parse_mode: 'MarkdownV2' }
            );
            return;
        }
        try {
            const statusUpdated = await updateBetStatus(betId, 'processing_payout');
            if (!statusUpdated) {
                 console.error(`${logPrefix}: CRITICAL! Failed to update status to 'processing_payout' before queueing! Aborting payout queue.`);
                 await safeSendMessage(chat_id, `⚠️ Internal error preparing your payout for bet \`${escapeMarkdownV2(memo_id)}\`\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
                 await updateBetStatus(betId, 'error_payout_status_update');
                 return;
            }
            await safeSendMessage(chat_id,
                `🎉 ${displayName}, you won ${escapeMarkdownV2(payoutSOL.toFixed(6))} SOL\\!\n` +
                `Result: *${escapeMarkdownV2(result)}*\n\n` +
                `💸 Processing payout to your linked wallet\\.\\.\\.`,
                { parse_mode: 'MarkdownV2' }
            );
            await paymentProcessor.addPaymentJob({
                type: 'payout',
                betId,
                recipient: winnerAddress,
                amount: payoutLamports.toString(), // Pass calculated net payout
                gameType: 'coinflip', // Use 'coinflip' to select BOT_PRIVATE_KEY
                priority: 2, // High priority for payouts
                // Context for payout message
                chatId: chat_id,
                displayName: displayName, // Already escaped
                memoId: memo_id,
            });
        } catch (e) {
            console.error(`${logPrefix}: Error preparing/queueing payout info:`, e);
            await updateBetStatus(betId, 'error_payout_preparation');
            await safeSendMessage(chat_id, `⚠️ Error occurred while processing your coinflip win for bet \`${escapeMarkdownV2(memo_id)}\`\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
        }
    } else { // Loss
        // console.log(`${logPrefix}: ${displayName} LOST. Choice: ${choice}, Result: ${result}`); // Reduce noise
        await updateBetStatus(betId, 'completed_loss');
        await safeSendMessage(chat_id,
            `❌ ${displayName}, you lost the coinflip\\!\n` +
            `You guessed *${escapeMarkdownV2(choice)}* but the result was *${escapeMarkdownV2(result)}*\\. Better luck next time\\!`,
            { parse_mode: 'MarkdownV2' }
        );
    }
}

async function handleRaceGame(bet) {
    const { id: betId, user_id, chat_id, bet_details, expected_lamports, memo_id } = bet;
    const chosenHorseName = bet_details.horse;
    const config = GAME_CONFIG.race;
    const logPrefix = `Race Bet ${betId} (${memo_id.slice(0, 6)}...)`;

    // Define horses within the function to ensure consistency
    const horses = [
         { name: 'Yellow', emoji: '🟡', odds: 1.1, baseProb: 0.25 }, { name: 'Orange', emoji: '🟠', odds: 2.0, baseProb: 0.20 },
         { name: 'Blue',   emoji: '🔵', odds: 3.0, baseProb: 0.15 }, { name: 'Cyan',   emoji: '💧', odds: 4.0, baseProb: 0.12 },
         { name: 'White',  emoji: '⚪️', odds: 5.0, baseProb: 0.09 }, { name: 'Red',    emoji: '🔴', odds: 6.0, baseProb: 0.07 },
         { name: 'Black',  emoji: '⚫️', odds: 7.0, baseProb: 0.05 }, { name: 'Pink',   emoji: '🌸', odds: 8.0, baseProb: 0.03 },
         { name: 'Purple', emoji: '🟣', odds: 9.0, baseProb: 0.02 }, { name: 'Green',  emoji: '🟢', odds: 10.0, baseProb: 0.01 },
         { name: 'Silver', emoji: '💎', odds: 15.0, baseProb: 0.01 }
     ];
     // Ensure probabilities sum close to 1 for weighted selection
     const totalProbSum = horses.reduce((sum, h) => sum + h.baseProb, 0);
     // if (Math.abs(totalProbSum - 1.0) > 0.001) console.warn(`Race base probabilities sum to ${totalProbSum}, not 1.0.`); // Optional check

    // --- Determine Winning Horse (Weighted selection + House Edge) ---
    // This logic simulates house edge by having a chance the player loses regardless of pick
    let winningHorse = null;
    const houseEdgeTarget = 1.0 - config.houseEdge; // e.g., 0.98
    const randomRoll = Math.random(); // 0.0 to < 1.0
    const isHouseWin = randomRoll >= houseEdgeTarget;

    // Pick a visual winner using weighted probabilities regardless of house edge outcome
     let cumulativeProb = 0;
     const visualWinnerRoll = Math.random();
     if (totalProbSum <= 0) { // Avoid division by zero
          winningHorse = horses[0]; // Default to first horse on error
          console.error(`${logPrefix}: Total horse probability is zero or negative! Defaulting winner.`);
     } else {
          for (const horse of horses) {
              cumulativeProb += (horse.baseProb / totalProbSum); // Use normalized probability
              if (visualWinnerRoll <= cumulativeProb) {
                  winningHorse = horse;
                  break;
              }
          }
          // Fallback if float precision issue occurs
          if (!winningHorse) winningHorse = horses[horses.length - 1];
     }

     if (isHouseWin) {
          // console.log(`${logPrefix}: House edge triggered. Visual Winner: ${winningHorse.name}`); // Reduce noise
     }
    // --- End Winning Horse Determination ---

    const playerWins = !isHouseWin && (chosenHorseName.toLowerCase() === winningHorse.name.toLowerCase());

    let payoutLamports = 0n;
    if (playerWins) {
        // Calculate gross winnings based on odds (Odds are "X to 1", so payout is Bet * (Odds + 1))
        // But our odds seem defined as multiplier (1.1x, 2.0x etc.)? Let's assume odds are payout multiplier *including stake*
        // Gross Winnings = Bet Amount * Winning Horse Odds
        const grossWinningsLamports = (BigInt(expected_lamports) * BigInt(Math.round(winningHorse.odds * 100))) / 100n; // Multiply odds safely
        payoutLamports = calculateNetPayout(grossWinningsLamports, 'race'); // Apply house edge to gross winnings
    }

    const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
    let displayName = await getUserDisplayName(chat_id, user_id);

    // Send race commentary messages
    try {
        await safeSendMessage(chat_id, `🐎 Race starting for bet \`${escapeMarkdownV2(memo_id)}\`\\! ${displayName} bet on *${escapeMarkdownV2(chosenHorseName)}*\\!`, { parse_mode: 'MarkdownV2' });
        await new Promise(resolve => setTimeout(resolve, 2000));
        await safeSendMessage(chat_id, "🚦 And they're off\\!", { parse_mode: 'MarkdownV2' });
        await new Promise(resolve => setTimeout(resolve, 3000));
        await safeSendMessage(chat_id, `🏆 The winner is\\.\\.\\. ${winningHorse.emoji} *${escapeMarkdownV2(winningHorse.name)}*\\! 🏆`, { parse_mode: 'MarkdownV2' });
        await new Promise(resolve => setTimeout(resolve, 1000));
    } catch (e) {
        console.error(`${logPrefix}: Error sending race commentary:`, e);
    }

    // Process win/loss
    if (playerWins && payoutLamports > 0n) {
        // console.log(`${logPrefix}: ${displayName} WON ${payoutSOL.toFixed(6)} SOL! Horse: ${chosenHorseName}, Winner: ${winningHorse.name}`); // Reduce noise
        const winnerAddress = await getLinkedWallet(user_id);
        if (!winnerAddress) {
            await updateBetStatus(betId, 'completed_win_no_wallet');
            await safeSendMessage(chat_id,
                `🎉 ${displayName}, your horse *${escapeMarkdownV2(chosenHorseName)}* won the race\\!\n`+
                `Your payout of ${escapeMarkdownV2(payoutSOL.toFixed(6))} SOL is waiting\\. Place another bet \\(any amount\\) to link your wallet and receive pending payouts\\.`,
                { parse_mode: 'MarkdownV2' }
            );
            return;
        }
        try {
             const statusUpdated = await updateBetStatus(betId, 'processing_payout');
             if (!statusUpdated) {
                 console.error(`${logPrefix}: CRITICAL! Failed to update status to 'processing_payout' before queueing! Aborting payout queue.`);
                 await safeSendMessage(chat_id, `⚠️ Internal error preparing your payout for bet \`${escapeMarkdownV2(memo_id)}\`\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
                 await updateBetStatus(betId, 'error_payout_status_update');
                 return;
             }
             await safeSendMessage(chat_id,
                 `🎉 ${displayName}, your horse *${escapeMarkdownV2(chosenHorseName)}* won\\!\n` +
                 `Payout: ${escapeMarkdownV2(payoutSOL.toFixed(6))} SOL\n\n` +
                 `💸 Processing payout to your linked wallet\\.\\.\\.`,
                 { parse_mode: 'MarkdownV2' }
             );
             await paymentProcessor.addPaymentJob({
                 type: 'payout',
                 betId,
                 recipient: winnerAddress,
                 amount: payoutLamports.toString(),
                 gameType: 'race', // Use 'race' to select RACE_BOT_PRIVATE_KEY
                 priority: 2,
                 chatId: chat_id,
                 displayName: displayName,
                 memoId: memo_id,
             });
        } catch (e) {
            console.error(`${logPrefix}: Error preparing/queueing race payout info:`, e);
            await updateBetStatus(betId, 'error_payout_preparation');
            await safeSendMessage(chat_id, `⚠️ Error occurred while processing your race win for bet \`${escapeMarkdownV2(memo_id)}\`\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
        }
    } else { // Loss
        // console.log(`${logPrefix}: ${displayName} LOST. Choice: ${chosenHorseName}, Winner: ${winningHorse.name}`); // Reduce noise
        await updateBetStatus(betId, 'completed_loss');
        await safeSendMessage(chat_id,
            `❌ ${displayName}, your horse *${escapeMarkdownV2(chosenHorseName)}* lost the race\\!\n` +
            `Winner: ${winningHorse.emoji} *${escapeMarkdownV2(winningHorse.name)}*\\. Better luck next time\\!`,
            { parse_mode: 'MarkdownV2' }
        );
    }
}

// --- NEW GAME: Slots ---

const SLOTS_SYMBOLS = {
    CHERRY: { emoji: '🍒', weight: 10, payout: { 2: 1, 3: 19 } }, // Payout for 2 or 3
    ORANGE: { emoji: '🍊', weight: 8, payout: { 3: 49 } },
    BAR: { emoji: '🍫', weight: 6, payout: { 3: 99 } }, // Using chocolate bar emoji for BAR
    SEVEN: { emoji: '7️⃣', weight: 4, payout: { /* Special handling for first reel */ } },
    TRIPLE_SEVEN: { emoji: '🎰', weight: 2, payout: { 3: 499 } }, // Using slot machine for 777
    BLANK: { emoji: '➖', weight: 15, payout: {} }, // Blank symbol
};
const SLOTS_REEL_LENGTH = 3; // 3 reels

// Helper to create a weighted reel based on symbols
function createReel() {
    const reel = [];
    for (const symbolKey in SLOTS_SYMBOLS) {
        const symbol = SLOTS_SYMBOLS[symbolKey];
        for (let i = 0; i < symbol.weight; i++) {
            reel.push(symbolKey); // Add symbol key based on weight
        }
    }
    return reel;
}
const reelStrip = createReel(); // Single reel strip used for all reels

// Helper to get a random symbol from the reel
function spinReel(strip) {
     const randomIndex = Math.floor(Math.random() * strip.length);
     return strip[randomIndex];
}

async function handleSlotsGame(bet) {
    const { id: betId, user_id, chat_id, expected_lamports, memo_id } = bet;
    const config = GAME_CONFIG.slots;
    const logPrefix = `Slots Bet ${betId} (${memo_id.slice(0, 6)}...)`;
    const betAmountLamports = BigInt(expected_lamports);

    // --- Simulate Spin ---
    const results = [];
    for (let i = 0; i < SLOTS_REEL_LENGTH; i++) {
        results.push(spinReel(reelStrip)); // Get 3 symbol keys
    }
    const resultEmojis = results.map(key => SLOTS_SYMBOLS[key].emoji).join(' | '); // e.g., 🍒 | 7️⃣ | ➖
    console.log(`${logPrefix}: Spin result: ${resultEmojis} (${results.join(',')})`);

    // --- Determine Win ---
    let winMultiplier = 0; // Base payout multiplier (0 = loss)
    let winDescription = "No Win";

    // Check for 3 x TRIPLE_SEVEN (Highest Payout)
    if (results.every(s => s === 'TRIPLE_SEVEN')) {
        winMultiplier = SLOTS_SYMBOLS.TRIPLE_SEVEN.payout[3]; // 499
        winDescription = "Triple 777 Jackpot!";
    }
    // Check for 3 x BAR
    else if (results.every(s => s === 'BAR')) {
        winMultiplier = SLOTS_SYMBOLS.BAR.payout[3]; // 99
        winDescription = "Triple BAR!";
    }
    // Check for 3 x ORANGE
    else if (results.every(s => s === 'ORANGE')) {
        winMultiplier = SLOTS_SYMBOLS.ORANGE.payout[3]; // 49
        winDescription = "Triple Orange!";
    }
    // Check for 3 x CHERRY
    else if (results.every(s => s === 'CHERRY')) {
        winMultiplier = SLOTS_SYMBOLS.CHERRY.payout[3]; // 19
        winDescription = "Triple Cherry!";
    }
    // Check for 7 on first reel (only if no higher win)
    else if (results[0] === 'SEVEN' && winMultiplier === 0) {
         winMultiplier = 4; // 4:1 payout
         winDescription = "Seven on First Reel!";
    }
    // Check for 2 Cherries on first two reels (only if no higher win)
    // User specified "2 cherries = 1:1". Assume first two reels for simplicity.
    else if (results[0] === 'CHERRY' && results[1] === 'CHERRY' && winMultiplier === 0) {
        winMultiplier = 1; // 1:1 payout
        winDescription = "Two Cherries!";
    }

    // --- Calculate Payout ---
    let payoutLamports = 0n;
    if (winMultiplier > 0) {
         // Gross winnings = Bet * Multiplier (e.g., Bet * 1, Bet * 19)
         const grossWinningsLamports = betAmountLamports * BigInt(winMultiplier);
         payoutLamports = calculateNetPayout(grossWinningsLamports, 'slots'); // Apply house edge
    }

    const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
    let displayName = await getUserDisplayName(chat_id, user_id);
    const win = payoutLamports > 0n;

    // --- Send Result Message ---
     let resultMessage = `🎰 *Slots Result* for ${displayName} \\!\n\n` +
                         `*Result:* ${resultEmojis}\n\n`;

    if (win) {
        resultMessage += `🎉 *${escapeMarkdownV2(winDescription)}* You won ${escapeMarkdownV2(payoutSOL.toFixed(6))} SOL\\!`;
         // console.log(`${logPrefix}: ${displayName} WON ${payoutSOL.toFixed(6)} SOL! Result: ${resultEmojis}, Desc: ${winDescription}`); // Reduce noise
    } else {
        resultMessage += `❌ No win this time\\. Better luck next spin\\!`;
        // console.log(`${logPrefix}: ${displayName} LOST. Result: ${resultEmojis}`); // Reduce noise
    }
    await safeSendMessage(chat_id, resultMessage, { parse_mode: 'MarkdownV2' });


    // --- Handle Payout or Update Status ---
    if (win) {
        const winnerAddress = await getLinkedWallet(user_id);
        if (!winnerAddress) {
            await updateBetStatus(betId, 'completed_win_no_wallet');
             await safeSendMessage(chat_id, `Your payout of ${escapeMarkdownV2(payoutSOL.toFixed(6))} SOL is waiting\\. Place another bet to link your wallet\\.`, { parse_mode: 'MarkdownV2' });
            return;
        }
        try {
             const statusUpdated = await updateBetStatus(betId, 'processing_payout');
             if (!statusUpdated) {
                 console.error(`${logPrefix}: CRITICAL! Failed to update status to 'processing_payout' before queueing! Aborting payout queue.`);
                 await safeSendMessage(chat_id, `⚠️ Internal error preparing your payout for bet \`${escapeMarkdownV2(memo_id)}\`\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
                 await updateBetStatus(betId, 'error_payout_status_update');
                 return;
             }
             // Don't send another message immediately, result message was sent above
             await paymentProcessor.addPaymentJob({
                 type: 'payout',
                 betId,
                 recipient: winnerAddress,
                 amount: payoutLamports.toString(),
                 gameType: 'coinflip', // Use main payout key
                 priority: 1, // Normal priority for game payouts unless specified otherwise
                 chatId: chat_id,
                 displayName: displayName,
                 memoId: memo_id,
             });
        } catch (e) {
            console.error(`${logPrefix}: Error preparing/queueing slots payout info:`, e);
            await updateBetStatus(betId, 'error_payout_preparation');
            await safeSendMessage(chat_id, `⚠️ Error occurred while processing your slots win for bet \`${escapeMarkdownV2(memo_id)}\`\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
        }
    } else { // Loss
        await updateBetStatus(betId, 'completed_loss');
    }
}

// --- NEW GAME: Roulette ---

const ROULETTE_NUMBERS = {
     0: { color: 'green' }, 1: { color: 'red', dozen: 1, column: 1 }, 2: { color: 'black', dozen: 1, column: 2 },
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
const ROULETTE_PAYOUTS = { // Payout *multipliers* (e.g., 35 means 35:1, pays 36x stake)
    S: 35, // Straight up
    R: 1, B: 1, // Red/Black
    E: 1, O: 1, // Even/Odd
    L: 1, H: 1, // Low (1-18)/High (19-36)
    D1: 2, D2: 2, D3: 2, // Dozens (1-12, 13-24, 25-36)
    C1: 2, C2: 2, C3: 2, // Columns (1,4,..34), (2,5,..35), (3,6,..36)
};

async function handleRouletteGame(bet) {
    const { id: betId, user_id, chat_id, bet_details, expected_lamports, memo_id } = bet;
    const config = GAME_CONFIG.roulette;
    const logPrefix = `Roulette Bet ${betId} (${memo_id.slice(0, 6)}...)`;
    const userBets = bet_details.bets; // e.g., { 'R': 10000000, 'S17': 5000000 }

    // --- Spin the Wheel (European: 0-36) ---
    const winningNumber = Math.floor(Math.random() * 37); // 0 to 36 inclusive
    const winningInfo = ROULETTE_NUMBERS[winningNumber];
    const winningColorEmoji = winningInfo.color === 'red' ? '🔴' : winningInfo.color === 'black' ? '⚫️' : '_0_'; // Using underscore for green 0

    console.log(`${logPrefix}: Spin result: ${winningNumber} (${winningInfo.color})`);

    // --- Calculate Winnings ---
    let totalGrossWinningsLamports = 0n;
    let winningBetDescriptions = [];

    for (const betTypeKey in userBets) {
        const betAmountLamports = BigInt(userBets[betTypeKey]);
        let betWins = false;
        let payoutMultiplier = ROULETTE_PAYOUTS[betTypeKey] ?? 0; // Get payout multiplier (e.g., 35 for S)

        // Deconstruct betTypeKey (e.g., 'S17' -> type='S', value='17'; 'R' -> type='R', value=undefined)
        const betType = betTypeKey.charAt(0);
        const betValue = betTypeKey.length > 1 ? betTypeKey.substring(1) : undefined;

        switch (betType) {
            case 'S': // Straight up
                if (winningNumber === parseInt(betValue, 10)) betWins = true;
                break;
            case 'R': // Red
                if (winningInfo.color === 'red') betWins = true;
                break;
            case 'B': // Black
                if (winningInfo.color === 'black') betWins = true;
                break;
            case 'E': // Even
                if (winningNumber !== 0 && winningNumber % 2 === 0) betWins = true;
                break;
            case 'O': // Odd
                if (winningNumber !== 0 && winningNumber % 2 !== 0) betWins = true;
                break;
            case 'L': // Low (1-18)
                if (winningNumber >= 1 && winningNumber <= 18) betWins = true;
                break;
            case 'H': // High (19-36)
                if (winningNumber >= 19 && winningNumber <= 36) betWins = true;
                break;
            case 'D': // Dozen
                const dozen = parseInt(betValue, 10); // Expect D1, D2, D3
                if (winningInfo.dozen === dozen) betWins = true;
                break;
            case 'C': // Column
                const column = parseInt(betValue, 10); // Expect C1, C2, C3
                if (winningInfo.column === column) betWins = true;
                break;
             // Add more bet types here (Split, Street, Corner etc.) if needed later
            default:
                console.warn(`${logPrefix}: Unknown bet type key "${betTypeKey}" in user bets.`);
                break;
        }

        if (betWins) {
            // Calculate payout: Stake * (Odds + 1) = Stake * PayoutMultiplier
            // e.g., Straight: 100 * (35 + 1) = 3600; Red: 100 * (1 + 1) = 200
            const grossWinForBet = betAmountLamports * BigInt(payoutMultiplier + 1);
            totalGrossWinningsLamports += grossWinForBet;
            winningBetDescriptions.push(`${betTypeKey} (+${escapeMarkdownV2((Number(grossWinForBet) / LAMPORTS_PER_SOL).toFixed(4))} SOL)`);
        }
    }

    // --- Calculate Net Payout (Apply House Edge) ---
    const payoutLamports = calculateNetPayout(totalGrossWinningsLamports, 'roulette');
    const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
    const win = payoutLamports > 0n;
    let displayName = await getUserDisplayName(chat_id, user_id);

    // --- Send Result Message ---
    let resultMessage = `⚪️ *Roulette Result* for ${displayName} \\!\n\n` +
                        `*Winning Number:* ${winningColorEmoji} ${winningNumber}\n\n`;

    if (win) {
        resultMessage += `🎉 *You won\\!* Payout: ${escapeMarkdownV2(payoutSOL.toFixed(6))} SOL\n`;
        resultMessage += `Winning Bets: ${winningBetDescriptions.join(', ')}\n`;
        // console.log(`${logPrefix}: ${displayName} WON ${payoutSOL.toFixed(6)} SOL! Winning No: ${winningNumber}`); // Reduce noise
    } else {
        resultMessage += `❌ No winning bets this time\\. Better luck next spin\\!`;
        // console.log(`${logPrefix}: ${displayName} LOST. Winning No: ${winningNumber}`); // Reduce noise
    }
    await safeSendMessage(chat_id, resultMessage, { parse_mode: 'MarkdownV2' });

    // --- Handle Payout or Update Status ---
    if (win) {
        const winnerAddress = await getLinkedWallet(user_id);
        if (!winnerAddress) {
            await updateBetStatus(betId, 'completed_win_no_wallet');
            await safeSendMessage(chat_id, `Your payout of ${escapeMarkdownV2(payoutSOL.toFixed(6))} SOL is waiting\\. Place another bet to link your wallet\\.`, { parse_mode: 'MarkdownV2' });
            return;
        }
        try {
            const statusUpdated = await updateBetStatus(betId, 'processing_payout');
            if (!statusUpdated) {
                console.error(`${logPrefix}: CRITICAL! Failed to update status to 'processing_payout' before queueing! Aborting payout queue.`);
                await safeSendMessage(chat_id, `⚠️ Internal error preparing your payout for bet \`${escapeMarkdownV2(memo_id)}\`\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
                await updateBetStatus(betId, 'error_payout_status_update');
                return;
            }
            // Send brief processing message
             await safeSendMessage(chat_id, `💸 Processing payout of ${escapeMarkdownV2(payoutSOL.toFixed(6))} SOL to your linked wallet\\.\\.\\.`, { parse_mode: 'MarkdownV2' });

            await paymentProcessor.addPaymentJob({
                type: 'payout',
                betId,
                recipient: winnerAddress,
                amount: payoutLamports.toString(),
                gameType: 'coinflip', // Use main payout key
                priority: 1,
                chatId: chat_id,
                displayName: displayName,
                memoId: memo_id,
            });
        } catch (e) {
            console.error(`${logPrefix}: Error preparing/queueing roulette payout info:`, e);
            await updateBetStatus(betId, 'error_payout_preparation');
            await safeSendMessage(chat_id, `⚠️ Error occurred while processing your roulette win for bet \`${escapeMarkdownV2(memo_id)}\`\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
        }
    } else { // Loss
        await updateBetStatus(betId, 'completed_loss');
    }
}


// --- Payout Job Handler ---
// Handles the actual payout transaction after a win is confirmed.
async function handlePayoutJob(job) {
    const { betId, recipient, amount, gameType, chatId, displayName, memoId } = job;
    const logPrefix = `Payout Job Bet ${betId} (${memoId?.slice(0, 6)}...)`;

    let payoutAmountLamports;
    try {
         payoutAmountLamports = BigInt(amount); // Amount comes as string from queue job
         if (payoutAmountLamports <= 0n) {
             console.error(`${logPrefix}: ❌ Payout amount is zero or negative (${amount}). Skipping.`);
             await updateBetStatus(betId, 'error_payout_zero_amount');
             await safeSendMessage(chatId, `⚠️ There was an issue calculating the payout for bet \`${escapeMarkdownV2(memoId)}\` \\(amount was zero\\)\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
             // Do not throw here, this is a final state error handled by status update
             return; // Exit function, job is considered "done" (failed)
         }
    } catch (e) {
        console.error(`${logPrefix}: ❌ Invalid payout amount format received in job: '${amount}'. Error: ${e.message}`);
        await updateBetStatus(betId, 'error_payout_invalid_amount');
        await safeSendMessage(chatId, `⚠️ Technical error processing payout for bet \`${escapeMarkdownV2(memoId)}\` \\(invalid amount\\)\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
        return; // Exit function
    }

    // console.log(`${logPrefix}: Processing payout of ${Number(payoutAmountLamports) / LAMPORTS_PER_SOL} SOL to ${recipient}`); // Reduce noise

    // Determine which payout key to use ('coinflip' uses main, 'race' uses race key)
    const payoutWalletType = (gameType === 'race') ? 'race' : 'coinflip';

    const sendResult = await sendSol(recipient, payoutAmountLamports, payoutWalletType);

    if (sendResult.success && sendResult.signature) {
        // console.log(`${logPrefix}: ✅ Payout successful! TX: ${sendResult.signature.slice(0, 10)}...`); // Reduce noise
        const recorded = await recordPayout(betId, 'completed_win_paid', sendResult.signature);
        if (recorded) {
            // Escape amount for Telegram message
            const payoutAmountSOLString = (Number(payoutAmountLamports)/LAMPORTS_PER_SOL).toFixed(6);
            const escapedAmount = payoutAmountSOLString.replace('.', '\\.'); // Escape decimal point

            await safeSendMessage(chatId,
                `✅ Payout successful for bet \`${escapeMarkdownV2(memoId)}\`\\!\n` +
                `${escapedAmount} SOL sent\\.\n` +
                `TX: \`https://solscan.io/tx/${sendResult.signature}\``, // Signature doesn't need escaping in code block
                { parse_mode: 'MarkdownV2', disable_web_page_preview: true }
            );
             console.log(`[PAYOUT_JOB_SUCCESS] Bet ${job.betId} payout logged successfully.`); // Log final success
        } else {
            console.error(`${logPrefix}: 🆘 CRITICAL! Payout sent (TX: ${sendResult.signature}) but FAILED to record in DB! Requires manual investigation.`);
            await updateBetStatus(betId, 'error_payout_record_failed');
            await safeSendMessage(chatId,
                `⚠️ Your payout for bet \`${escapeMarkdownV2(memo_id)}\` was sent successfully, but there was an issue recording it\\. Please contact support\\.\n` +
                `TX: \`https://solscan.io/tx/${sendResult.signature}\``,
                { parse_mode: 'MarkdownV2', disable_web_page_preview: true }
            );
            // Do not throw; error state is recorded.
        }
        // Payout sent (and hopefully recorded) successfully, DO NOT throw error.
        return; // Signal successful completion of job attempt

    } else {
        // Payout failed via sendSol
        console.error(`${logPrefix}: ❌ Payout failed via sendSol. Error: ${sendResult.error}`);
        // Re-throw the error message from sendSol so processJob retry logic can catch it
        throw new Error(sendResult.error || 'sendSol failed');
    }
    // Catch block in processJob will handle status updates for failed retries/non-retryable
}


// --- Telegram Bot Command Handlers ---

bot.on('polling_error', (error) => {
    console.error(`❌ Polling error: ${error.code} - ${error.message}`);
    if (error.code === 'ETELEGRAM' && error.message.includes('409 Conflict')) {
        console.error("❌❌❌ FATAL: Conflict detected! Another bot instance might be running. Exiting.");
         shutdown('POLLING_CONFLICT', false).catch(() => process.exit(1)); // Attempt graceful, then exit
         setTimeout(() => process.exit(1), 5000).unref(); // Force exit if shutdown hangs
    } else if (error.code === 'ECONNRESET') {
         console.warn("⚠️ Polling connection reset. Attempting to continue...");
    } else if (error.response && error.response.statusCode === 401) {
         console.error("❌❌❌ FATAL: Unauthorized (401). Check BOT_TOKEN. Exiting.");
         shutdown('BOT_TOKEN_INVALID', false).catch(() => process.exit(1));
         setTimeout(() => process.exit(1), 5000).unref();
    }
    // Add more specific handling if needed
});

bot.on('webhook_error', (error) => {
    console.error(`❌ Webhook error: ${error.code} - ${error.message}`);
});

bot.on('error', (error) => {
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
        // Cooldown Check
        const now = Date.now();
        if (confirmCooldown.has(userId)) {
            const lastTime = confirmCooldown.get(userId);
            if (now - lastTime < cooldownInterval) {
                return; // Ignore command during cooldown
            }
        }
        if (messageText.startsWith('/')) {
            confirmCooldown.set(userId, now); // Apply cooldown
        }

        // Command Routing
        const commandMatch = messageText.trim().match(/^\/(\w+)(?:@\w+)?(?:\s+(.*))?$/s);
        if (!commandMatch) return; // Not a command

        const command = commandMatch[1].toLowerCase();
        const args = commandMatch[2]?.trim() || ''; // Arguments string

        // --- Command Handler Map --- ADDED SLOTS/ROULETTE
        const commandHandlers = {
            'start': handleStartCommand,
            'coinflip': handleCoinflipCommand,
            'bet': (msg, args) => handleBetCommand(msg, args), // Coinflip bet
            'race': handleRaceCommand,
            'betrace': (msg, args) => handleBetRaceCommand(msg, args),
            'slots': handleSlotsCommand, // NEW
            'betslots': (msg, args) => handleBetSlotsCommand(msg, args), // NEW
            'roulette': handleRouletteCommand, // NEW
            'betroulette': (msg, args) => handleBetRouletteCommand(msg, args), // NEW
            'wallet': handleWalletCommand,
            'help': handleHelpCommand,
            'botstats': handleBotStatsCommand, // Admin
        };

        const handler = commandHandlers[command];

        if (handler) {
            // Admin Check
            if (command === 'botstats') {
                const adminIds = (process.env.ADMIN_USER_IDS || '').split(',').filter(id => id); // Filter empty strings
                if (adminIds.includes(userId)) {
                    await handler(msg); // Pass only msg if needed
                } else {
                     console.log(`User ${userId} attempted unauthorized /${command} command.`);
                     // Optionally send a silent fail or generic message
                }
            } else {
                 // Regular command execution
                 if (typeof handler === 'function') {
                      // Check arity (number of expected arguments) to pass args correctly
                      if (handler.length === 2) {
                           await handler(msg, args); // Handler expects msg and args
                      } else {
                           await handler(msg); // Handler expects only msg
                      }
                      performanceMonitor.logRequest(true);
                 } else {
                      console.warn(`Handler for command /${command} is not a function.`);
                 }
            }
        } else {
            // Unknown command - optionally notify user
            // await safeSendMessage(chatId, `Unknown command: \`/${command}\``, { parse_mode: 'MarkdownV2'});
        }

    } catch (error) {
        console.error(`❌ Error processing msg ${messageId} from user ${userId} ("${messageText}"):`, error);
        performanceMonitor.logRequest(false);
        try {
            await safeSendMessage(chatId, "⚠️ An unexpected error occurred\\. Please try again later or contact support\\.", { parse_mode: 'MarkdownV2'});
        } catch (tgError) { /* safeSendMessage already logs */ }
    } finally {
        // Optional: Cooldown map cleanup
         if (Math.random() < 0.05) { // ~5% chance per message
             const cutoff = Date.now() - 300000; // 5 minutes ago
             for (const [key, timestamp] of confirmCooldown.entries()) {
                 if (timestamp < cutoff) confirmCooldown.delete(key);
             }
         }
    }
}


// --- Specific Command Handler Implementations ---

// /start command (HTML)
async function handleStartCommand(msg) {
    const firstName = msg.from.first_name || 'there';
    const sanitizedFirstName = firstName.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    const welcomeText = `👋 Welcome, <b>${sanitizedFirstName}</b>!\n\n` +
                         `🎰 <b>Solana Gambles Bot</b>\n\n` +
                         `Use the commands below to play:\n` +
                         `/coinflip - Simple Heads/Tails\n` +
                         `/race - Bet on Horse Races\n` +
                         `/slots - Play the Slot Machine\n` + // Added
                         `/roulette - Play European Roulette\n\n` + // Added
                         `/wallet - View/Link your Solana wallet\n` +
                         `/help - See all commands & rules\n\n` +
                         `<i>Remember to gamble responsibly!</i>`;
    const bannerUrl = 'https://i.ibb.co/9vDo58q/banner.gif'; // Keep existing banner

    try {
        await bot.sendAnimation(msg.chat.id, bannerUrl, {
            caption: welcomeText,
            parse_mode: 'HTML'
        }).catch(async (animError) => {
             console.warn("⚠️ Failed to send start animation, sending text fallback:", animError.message);
             await safeSendMessage(msg.chat.id, welcomeText, { parse_mode: 'HTML' });
        });
    } catch (fallbackError) { /* safeSendMessage handles logging */ }
}


// /coinflip command (HTML)
async function handleCoinflipCommand(msg) {
    try {
        const config = GAME_CONFIG.coinflip;
        const payoutMultiplier = (2.0 * (1.0 - config.houseEdge)).toFixed(2);
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
        console.error("Error in handleCoinflipCommand:", error);
        await safeSendMessage(msg.chat.id, "Sorry, couldn't display Coinflip info right now.");
    }
}

// /race command (MarkdownV2)
async function handleRaceCommand(msg) {
    const horses = [ // Keep consistent definition
        { name: 'Yellow', emoji: '🟡', odds: 1.1 }, { name: 'Orange', emoji: '🟠', odds: 2.0 }, { name: 'Blue', emoji: '🔵', odds: 3.0 }, { name: 'Cyan', emoji: '💧', odds: 4.0 },
        { name: 'White', emoji: '⚪️', odds: 5.0 }, { name: 'Red', emoji: '🔴', odds: 6.0 }, { name: 'Black', emoji: '⚫️', odds: 7.0 }, { name: 'Pink', emoji: '🌸', odds: 8.0 },
        { name: 'Purple', emoji: '🟣', odds: 9.0 }, { name: 'Green', emoji: '🟢', odds: 10.0 }, { name: 'Silver', emoji: '💎', odds: 15.0 }
    ];
    let raceMessage = `🐎 *Horse Race Game* 🐎\n\nBet on the winning horse\\!\n\n*Available Horses \\& Approx Payout* \\(Bet x Odds After House Edge\\):\n`; // Escape !()
    horses.forEach(horse => {
        const effectiveMultiplier = (horse.odds * (1.0 - GAME_CONFIG.race.houseEdge)).toFixed(2);
        raceMessage += `\\- ${horse.emoji} *${escapeMarkdownV2(horse.name)}* \\(\\~${escapeMarkdownV2(effectiveMultiplier)}x Payout\\)\n`; // Escape -()~
    });
    const config = GAME_CONFIG.race;
    raceMessage += `\n*How to play:*\n` +
                    `1\\. Type \`/betrace amount horse_name\`\n` + // Escape .
                    `   \\(e\\.g\\., \`/betrace 0\\.1 Yellow\`\\)\n\n` + // Escape .()
                    `*Rules:*\n` +
                    `\\- Min Bet: ${escapeMarkdownV2(config.minBet)} SOL\n` + // Escape -
                    `\\- Max Bet: ${escapeMarkdownV2(config.maxBet)} SOL\n` + // Escape -
                    `\\- House Edge: ${escapeMarkdownV2((config.houseEdge * 100).toFixed(1))}% \\(applied to winnings\\)\n\n` + // Escape -()
                    `You will be given a wallet address and a *unique Memo ID*\\. Send the *exact* SOL amount with the memo to place your bet\\.`; // Escape .
    await safeSendMessage(msg.chat.id, raceMessage, { parse_mode: 'MarkdownV2' });
}

// --- NEW: /slots command (MarkdownV2) ---
async function handleSlotsCommand(msg) {
    const config = GAME_CONFIG.slots;
    const paylines = [ // Define paylines - using emojis for symbols
        `🍒 Cherry \\| 🍒 Cherry \\| \\(Any\\) = ${escapeMarkdownV2(SLOTS_SYMBOLS.CHERRY.payout[2])}:1`,
        `7️⃣ Seven \\| \\(Any\\) \\| \\(Any\\) = 4:1`,
        `🍒 Cherry \\| 🍒 Cherry \\| 🍒 Cherry = ${escapeMarkdownV2(SLOTS_SYMBOLS.CHERRY.payout[3])}:1`,
        `🍊 Orange \\| 🍊 Orange \\| 🍊 Orange = ${escapeMarkdownV2(SLOTS_SYMBOLS.ORANGE.payout[3])}:1`,
        `🍫 BAR \\| 🍫 BAR \\| 🍫 BAR = ${escapeMarkdownV2(SLOTS_SYMBOLS.BAR.payout[3])}:1`,
        `🎰 777 \\| 🎰 777 \\| 🎰 777 = ${escapeMarkdownV2(SLOTS_SYMBOLS.TRIPLE_SEVEN.payout[3])}:1`
    ];

    const message = `🎰 *777 Slots Game* 🎰\n\n` +
                    `Spin the 3 reels and match symbols on the center line\\!\n\n`+
                    `*Symbols:*\n`+
                    `🍒 Cherry, 🍊 Orange, 🍫 BAR, 7️⃣ Seven, 🎰 777, ➖ Blank\n\n` +
                    `*Payouts (Odds:1):*\n` +
                    paylines.map(line => `\\- ${line}`).join('\n') + `\n\n` + // Use map for payouts
                    `*How to Play:*\n` +
                    `\\- Type \`/betslots amount\` \\(e\\.g\\., \`/betslots 0\\.05\`\\)\n\n` + // Simplified bet command
                    `*Rules:*\n` +
                    `\\- Min Bet: ${escapeMarkdownV2(config.minBet)} SOL\n` +
                    `\\- Max Bet: ${escapeMarkdownV2(config.maxBet)} SOL\n` +
                    `\\- House Edge: ${escapeMarkdownV2((config.houseEdge * 100).toFixed(1))}% \\(applied to winnings\\)\n\n`+
                    `You will be given a wallet address and a *unique Memo ID*\\. Send the *exact* SOL amount with the memo to spin\\.`;

    await safeSendMessage(msg.chat.id, message, { parse_mode: 'MarkdownV2' });
}

// --- NEW: /roulette command (MarkdownV2) ---
async function handleRouletteCommand(msg) {
    const config = GAME_CONFIG.roulette;
    // Define payouts clearly (X:1 format)
    const payouts = [
        `Straight \\(1 number\\): 35:1`,
        `Red / Black: 1:1`,
        `Even / Odd: 1:1`,
        `Low \\(1\\-18\\) / High \\(19\\-36\\): 1:1`,
        `Dozen \\(1st, 2nd, 3rd 12\\): 2:1`,
        `Column \\(1st, 2nd, 3rd\\): 2:1`
    ];
     // Define bet types for instructions
     const betTypes = [
         '`S <number>` \\(Straight Up, e\\.g\\., `S 17`\\)',
         '`R` \\(Red\\)', '`B` \\(Black\\)',
         '`E` \\(Even\\)', '`O` \\(Odd\\)',
         '`L` \\(Low 1\\-18\\)', '`H` \\(High 19\\-36\\)',
         '`D1` \\(Dozen 1\\-12\\)', '`D2` \\(Dozen 13\\-24\\)', '`D3` \\(Dozen 25\\-36\\)',
         '`C1` \\(Column 1\\)', '`C2` \\(Column 2\\)', '`C3` \\(Column 3\\)'
     ];

    const message = `⚪️ *European Roulette Game* ⚪️ \\(Single Zero\\)\n\n`+
                    `Place your bets and spin the wheel\\!\n\n`+
                    `*Available Bet Types & Payouts (Odds:1):*\n` +
                    payouts.map(p => `\\- ${p}`).join('\n') + `\n\n` +
                    `*How to Play (One Bet Type per Command):*\n`+
                    `\\- Type \`/betroulette amount bet_type [value]\`\n`+
                    `   \\(e\\.g\\., \`/betroulette 0\\.1 R\` to bet 0\\.1 SOL on Red\\)\n`+
                    `   \\(e\\.g\\., \`/betroulette 0\\.02 S 17\` to bet 0\\.02 SOL on Straight 17\\)\n\n` +
                    `*Bet Type Codes:*\n`+
                    betTypes.map(t => `\\- ${t}`).join('\n') + `\n\n` +
                    `*Rules:*\n` +
                    `\\- Min Bet: ${escapeMarkdownV2(config.minBet)} SOL \\(per bet type placed\\)\n` +
                    `\\- Max Bet: ${escapeMarkdownV2(config.maxBet)} SOL \\(total allowed per spin request\\)\n` +
                    `\\- House Edge: ${escapeMarkdownV2((config.houseEdge * 100).toFixed(1))}% \\(applied to winnings\\)\n\n`+
                    `You will be given a wallet address and a *unique Memo ID*\\. Send the *exact* SOL amount with the memo to place your bet\\.`;

    await safeSendMessage(msg.chat.id, message, { parse_mode: 'MarkdownV2' });
}


// /wallet command (MarkdownV2)
async function handleWalletCommand(msg) {
    const userId = String(msg.from.id);
    const walletAddress = await getLinkedWallet(userId);
    if (walletAddress) {
        await safeSendMessage(msg.chat.id,
            `🔗 Your linked Solana wallet:\n\`${escapeMarkdownV2(walletAddress)}\`\n\n`+
            `Payouts will be sent here\\. It's linked automatically when you make your first paid bet\\.`,
            { parse_mode: 'MarkdownV2' }
        );
    } else {
        await safeSendMessage(msg.chat.id,
            `🔗 No wallet linked yet\\.\n` +
            `Place a bet and send the required SOL\\. Your sending wallet will be automatically linked for future payouts\\.`,
             { parse_mode: 'MarkdownV2' }
        );
    }
}


// /bet command (Coinflip - MarkdownV2)
async function handleBetCommand(msg, args) {
     const match = args.trim().match(/^(\d+\.?\d*)\s+(heads|tails)/i);
     if (!match) {
         await safeSendMessage(msg.chat.id,
             `⚠️ Invalid format\\. Use: \`/bet <amount> <heads|tails>\`\n` +
             `Example: \`/bet 0\\.1 heads\``,
             { parse_mode: 'MarkdownV2' }
         );
         return;
     }

     const userId = String(msg.from.id);
     const chatId = String(msg.chat.id);
     const config = GAME_CONFIG.coinflip;

     const betAmount = parseFloat(match[1]);
     if (isNaN(betAmount) || betAmount < config.minBet || betAmount > config.maxBet) {
         await safeSendMessage(chatId,
             `⚠️ Invalid bet amount\\. Please bet between ${escapeMarkdownV2(config.minBet)} and ${escapeMarkdownV2(config.maxBet)} SOL\\.`,
             { parse_mode: 'MarkdownV2' }
         );
         return;
     }

     const userChoice = match[2].toLowerCase();
     const memoId = generateMemoId('CF');
     const expectedLamports = BigInt(Math.round(betAmount * LAMPORTS_PER_SOL));
     const expiresAt = new Date(Date.now() + config.expiryMinutes * 60 * 1000);

     const saveResult = await savePendingBet(
         userId, chatId, 'coinflip', { choice: userChoice }, expectedLamports, memoId, expiresAt
     );

     if (!saveResult.success) {
         await safeSendMessage(chatId, `⚠️ Error registering bet: ${escapeMarkdownV2(saveResult.error || 'Unknown error')}\\. Please try the command again\\.`, { parse_mode: 'MarkdownV2' });
         return;
     }

     const betAmountString = escapeMarkdownV2(betAmount.toFixed(Math.max(2, (betAmount.toString().split('.')[1] || '').length)));
     const message = `✅ Coinflip bet registered\\! \\(ID: \`${memoId}\`\\)\n\n` +
                     `You chose: *${escapeMarkdownV2(userChoice)}*\n` +
                     `Amount: *${betAmountString} SOL*\n\n` +
                     `➡️ Send *exactly ${betAmountString} SOL* to:\n` +
                     `\`${escapeMarkdownV2(process.env.MAIN_WALLET_ADDRESS)}\`\n\n` +
                     `📎 *Include MEMO:* \`${memoId}\`\n\n` +
                     `⏱️ This request expires in ${config.expiryMinutes} minutes\\.\n\n` +
                     `*IMPORTANT:* Send from your own wallet \\(not an exchange\\)\\. Ensure you include the memo correctly\\.`;

     await safeSendMessage(chatId, message, { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}

// /betrace command (MarkdownV2)
async function handleBetRaceCommand(msg, args) {
    const match = args.trim().match(/^(\d+\.?\d*)\s+([\w\s]+)/i); // Allow spaces in name
    if (!match) {
        await safeSendMessage(msg.chat.id,
            `⚠️ Invalid format\\. Use: \`/betrace <amount> <horse_name>\`\n`+
            `Example: \`/betrace 0\\.1 Yellow\``,
            { parse_mode: 'MarkdownV2' }
        );
        return;
    }

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const config = GAME_CONFIG.race;

    const betAmount = parseFloat(match[1]);
    if (isNaN(betAmount) || betAmount < config.minBet || betAmount > config.maxBet) {
        await safeSendMessage(chatId,
            `⚠️ Invalid bet amount\\. Please bet between ${escapeMarkdownV2(config.minBet)} and ${escapeMarkdownV2(config.maxBet)} SOL\\.`,
            { parse_mode: 'MarkdownV2' }
        );
        return;
    }

     const chosenHorseNameInput = match[2].trim();
     const horses = [ // Use same list as in handleRaceGame
        { name: 'Yellow', emoji: '🟡', odds: 1.1 }, { name: 'Orange', emoji: '🟠', odds: 2.0 }, { name: 'Blue', emoji: '🔵', odds: 3.0 }, { name: 'Cyan', emoji: '💧', odds: 4.0 },
        { name: 'White', emoji: '⚪️', odds: 5.0 }, { name: 'Red', emoji: '🔴', odds: 6.0 }, { name: 'Black', emoji: '⚫️', odds: 7.0 }, { name: 'Pink', emoji: '🌸', odds: 8.0 },
        { name: 'Purple', emoji: '🟣', odds: 9.0 }, { name: 'Green', emoji: '🟢', odds: 10.0 }, { name: 'Silver', emoji: '💎', odds: 15.0 }
     ];
     const chosenHorse = horses.find(h => h.name.toLowerCase() === chosenHorseNameInput.toLowerCase());

    if (!chosenHorse) {
        await safeSendMessage(chatId, `⚠️ Invalid horse name: "${escapeMarkdownV2(chosenHorseNameInput)}"\\. Please choose from the list in /race\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    const memoId = generateMemoId('RA');
    const expectedLamports = BigInt(Math.round(betAmount * LAMPORTS_PER_SOL));
    const expiresAt = new Date(Date.now() + config.expiryMinutes * 60 * 1000);

    // Calculate potential payout for display (using net payout function for consistency)
    // Gross win = Bet * Odds; Net Payout = calculateNetPayout(Gross Win)
    const potentialGrossWin = (expectedLamports * BigInt(Math.round(chosenHorse.odds * 100))) / 100n;
    const potentialNetPayout = calculateNetPayout(potentialGrossWin, 'race');
    const potentialPayoutSOL = escapeMarkdownV2((Number(potentialNetPayout) / LAMPORTS_PER_SOL).toFixed(6));
    const betAmountString = escapeMarkdownV2(betAmount.toFixed(Math.max(2, (betAmount.toString().split('.')[1] || '').length)));

    const saveResult = await savePendingBet(
        userId, chatId, 'race',
        { horse: chosenHorse.name, odds: chosenHorse.odds }, // Store chosen horse name and odds
        expectedLamports, memoId, expiresAt
    );

    if (!saveResult.success) {
        await safeSendMessage(chatId, `⚠️ Error registering bet: ${escapeMarkdownV2(saveResult.error || 'Unknown error')}\\. Please try the command again\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    const message = `✅ Race bet registered\\! \\(ID: \`${memoId}\`\\)\n\n` +
                    `You chose: ${chosenHorse.emoji} *${escapeMarkdownV2(chosenHorse.name)}*\n` +
                    `Amount: *${betAmountString} SOL*\n` +
                    `Potential Payout: \\~${potentialPayoutSOL} SOL \\(after house edge\\)\n\n`+ // Make clear it's net
                    `➡️ Send *exactly ${betAmountString} SOL* to:\n` +
                    `\`${escapeMarkdownV2(process.env.RACE_WALLET_ADDRESS)}\`\n\n` +
                    `📎 *Include MEMO:* \`${memoId}\`\n\n` +
                    `⏱️ This request expires in ${config.expiryMinutes} minutes\\.\n\n` +
                    `*IMPORTANT:* Send from your own wallet \\(not an exchange\\)\\. Ensure you include the memo correctly\\.`;

    await safeSendMessage(chatId, message, { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}

// --- NEW: /betslots command (MarkdownV2) ---
async function handleBetSlotsCommand(msg, args) {
    const match = args.trim().match(/^(\d+\.?\d*)$/); // Only expects amount now
    if (!match) {
        await safeSendMessage(msg.chat.id,
            `⚠️ Invalid format\\. Use: \`/betslots <amount>\`\n` +
            `Example: \`/betslots 0\\.05\``,
            { parse_mode: 'MarkdownV2' }
        );
        return;
    }

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const config = GAME_CONFIG.slots;

    const betAmount = parseFloat(match[1]);
    if (isNaN(betAmount) || betAmount < config.minBet || betAmount > config.maxBet) {
        await safeSendMessage(chatId,
            `⚠️ Invalid bet amount\\. Please bet between ${escapeMarkdownV2(config.minBet)} and ${escapeMarkdownV2(config.maxBet)} SOL\\.`,
            { parse_mode: 'MarkdownV2' }
        );
        return;
    }

    const memoId = generateMemoId('SL'); // Slots memo prefix
    const expectedLamports = BigInt(Math.round(betAmount * LAMPORTS_PER_SOL));
    const expiresAt = new Date(Date.now() + config.expiryMinutes * 60 * 1000);

    // bet_details can be empty for simple slots, or store bet amount if needed later
    const betDetails = { betAmountSOL: betAmount };

    const saveResult = await savePendingBet(
        userId, chatId, 'slots', betDetails, expectedLamports, memoId, expiresAt
    );

    if (!saveResult.success) {
        await safeSendMessage(chatId, `⚠️ Error registering bet: ${escapeMarkdownV2(saveResult.error || 'Unknown error')}\\. Please try the command again\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    const betAmountString = escapeMarkdownV2(betAmount.toFixed(Math.max(2, (betAmount.toString().split('.')[1] || '').length)));
    const message = `✅ Slots bet registered\\! \\(ID: \`${memoId}\`\\)\n\n` +
                    `Spin Amount: *${betAmountString} SOL*\n\n` +
                    `➡️ Send *exactly ${betAmountString} SOL* to:\n` +
                    `\`${escapeMarkdownV2(process.env.MAIN_WALLET_ADDRESS)}\` \\(Shared Deposit Address\\)\n\n` + // Clarify shared address
                    `📎 *Include MEMO:* \`${memoId}\`\n\n` +
                    `⏱️ This request expires in ${config.expiryMinutes} minutes\\.\n\n` +
                    `*IMPORTANT:* Send from your own wallet \\(not an exchange\\)\\. Ensure you include the memo correctly\\.`;

    await safeSendMessage(chatId, message, { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}

// --- NEW: /betroulette command (MarkdownV2) ---
async function handleBetRouletteCommand(msg, args) {
    // Expects: amount type [value] e.g. "0.1 R", "0.05 S 17", "0.2 D1"
    const match = args.trim().match(/^(\d+\.?\d*)\s+([A-Z])(?:([1-3])|(?:([1-9]|[12]\d|3[0-6])))?$/i); // Complex regex to capture type and optional value
    // Groups: 1: amount, 2: type (S,R,B,E,O,L,H,D,C), 3: dozen/column value (1-3), 4: straight up value (1-36, group 4 used for S)

    if (!match) {
        await safeSendMessage(msg.chat.id,
            `⚠️ Invalid format\\. Use: \`/betroulette <amount> <type> [number]\`\n` +
            `Type Codes: \`R\`, \`B\`, \`E\`, \`O\`, \`L\`, \`H\`, \`D1\`/\`D2\`/\`D3\`, \`C1\`/\`C2\`/\`C3\`, \`S <0-36>\`\n`+
            `Examples: \`/betroulette 0\\.1 R\` \\| \`/betroulette 0\\.02 S 17\` \\| \`/betroulette 0\\.2 D1\``,
            { parse_mode: 'MarkdownV2', disable_web_page_preview: true }
        );
        return;
    }

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const config = GAME_CONFIG.roulette;

    const betAmount = parseFloat(match[1]);
    const betType = match[2].toUpperCase();
    let betValue = undefined; // For Straight up, Dozen, Column

    if (betType === 'S') {
         // Special case for S 0 - needs separate regex or check, allow 0 for straight up
         const straightMatch = args.trim().match(/^(\d+\.?\d*)\s+S\s+(0|[1-9]|[12]\d|3[0-6])$/i);
         if (!straightMatch) {
             await safeSendMessage(chatId, `⚠️ Invalid Straight Up Bet\\. Use \`/betroulette <amount> S <number 0-36>\``, { parse_mode: 'MarkdownV2' });
             return;
         }
         betValue = straightMatch[2]; // Get the number 0-36
    } else if (betType === 'D' || betType === 'C') {
         betValue = match[3]; // Group 3 for Dozen/Column (1-3)
         if (!betValue) {
              await safeSendMessage(chatId, `⚠️ Invalid Dozen/Column Bet\\. Use \`D1\`, \`D2\`, \`D3\` or \`C1\`, \`C2\`, \`C3\``, { parse_mode: 'MarkdownV2' });
              return;
         }
    } else if (match[3] || match[4]) {
         // Value provided for a type that doesn't need it (e.g., /betroulette 0.1 R 5)
         await safeSendMessage(chatId, `⚠️ Invalid Bet Type\\. Type \`${escapeMarkdownV2(betType)}\` does not require a number\\. See /roulette for examples\\.`, { parse_mode: 'MarkdownV2' });
         return;
    }

     // Validate bet type
     const validTypes = ['S', 'R', 'B', 'E', 'O', 'L', 'H', 'D', 'C'];
     if (!validTypes.includes(betType)) {
         await safeSendMessage(chatId, `⚠️ Invalid bet type: \`${escapeMarkdownV2(betType)}\`\\. See /roulette for valid types\\.`, { parse_mode: 'MarkdownV2' });
         return;
     }

     // Construct full bet key (e.g., "S17", "D1", "R")
     const betKey = betType + (betValue || '');


    // Validate bet amount
    if (isNaN(betAmount) || betAmount < config.minBet || betAmount > config.maxBet) {
        // Note: Max bet here applies to this single bet placement, not necessarily the total for the spin yet
        // A user could place multiple bets up to the total max (this check is slightly simplified)
        await safeSendMessage(chatId,
            `⚠️ Invalid bet amount for this bet type\\. Please bet between ${escapeMarkdownV2(config.minBet)} and ${escapeMarkdownV2(config.maxBet)} SOL\\.`,
            { parse_mode: 'MarkdownV2' }
        );
        return;
    }

    const memoId = generateMemoId('RL'); // Roulette memo prefix
    const expectedLamports = BigInt(Math.round(betAmount * LAMPORTS_PER_SOL));
    const expiresAt = new Date(Date.now() + config.expiryMinutes * 60 * 1000);

    // Store the single bet details
    // Structure: { bets: { 'BET_KEY': lamports } } e.g. {'bets': {'S17': 5000000}}
    const betDetails = {
         betAmountSOL: betAmount, // Store original SOL amount for reference
         bets: {
             [betKey]: expectedLamports.toString() // Store lamports as string in JSON
         }
    };

    // Save the bet
    const saveResult = await savePendingBet(
        userId, chatId, 'roulette', betDetails, expectedLamports, memoId, expiresAt
    );

    if (!saveResult.success) {
        await safeSendMessage(chatId, `⚠️ Error registering bet: ${escapeMarkdownV2(saveResult.error || 'Unknown error')}\\. Please try the command again\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    // Send instructions
    const betAmountString = escapeMarkdownV2(betAmount.toFixed(Math.max(2, (betAmount.toString().split('.')[1] || '').length)));
    const message = `✅ Roulette bet registered\\! \\(ID: \`${memoId}\`\\)\n\n` +
                    `Bet Placed: *${escapeMarkdownV2(betKey)}*\n` +
                    `Amount: *${betAmountString} SOL*\n\n` +
                    `➡️ Send *exactly ${betAmountString} SOL* to:\n` +
                    `\`${escapeMarkdownV2(process.env.MAIN_WALLET_ADDRESS)}\` \\(Shared Deposit Address\\)\n\n` +
                    `📎 *Include MEMO:* \`${memoId}\`\n\n` +
                    `⏱️ This request expires in ${config.expiryMinutes} minutes\\.\n\n` +
                    `*IMPORTANT:* Send from your own wallet \\(not an exchange\\)\\. Ensure you include the memo correctly\\.`;

    await safeSendMessage(chatId, message, { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}


// /help command (MarkdownV2) - UPDATED
async function handleHelpCommand(msg) {
    const helpText = `*Solana Gambles Bot Commands* 🎰\n\n` +
                      `/start \\- Show welcome message\n` +
                      `/help \\- Show this help message\n\n` +
                      `*Games:*\n` +
                      `/coinflip \\- Coinflip info \\& how to bet\n` +
                      `/race \\- Horse Race info \\& how to bet\n` +
                      `/slots \\- Slots info \\& how to bet\n` + // Added
                      `/roulette \\- Roulette info \\& how to bet\n\n` + // Added
                      `*Betting:*\n` +
                      `/bet <amount> <heads|tails> \\- Place a Coinflip bet\n` +
                      `/betrace <amount> <horse\\_name> \\- Place a Race bet\n` +
                      `/betslots <amount> \\- Place a Slots bet\n` + // Added
                      `/betroulette <amount> <type> [value] \\- Place a Roulette bet \\(see /roulette\\)\n\n` + // Added
                      `*Wallet:*\n` +
                      `/wallet \\- View your linked Solana wallet for payouts\n\n` +
                      `*Support:* If you encounter issues, please contact support \\(details not provided here\\)\\.`; // Added placeholder

    await safeSendMessage(msg.chat.id, helpText, { parse_mode: 'MarkdownV2' });
}


// /botstats command (Admin - MarkdownV2)
async function handleBotStatsCommand(msg) {
    // Note: Assumes ADMIN_USER_IDS check happened in handleMessage
    const processor = paymentProcessor;
    let connectionStats = null;
    try {
        connectionStats = typeof solanaConnection?.getRequestStats === 'function' ? solanaConnection.getRequestStats() : null;
    } catch(e){ console.error("Error getting conn stats for botstats:", e.message); }

    // Safely access queue sizes
    const messageQueueSize = messageQueue?.size || 0;
    const messageQueuePending = messageQueue?.pending || 0;
    const telegramSendQueueSize = telegramSendQueue?.size || 0;
    const telegramSendQueuePending = telegramSendQueue?.pending || 0;
    const paymentHighPriQueueSize = processor?.highPriorityQueue?.size || 0;
    const paymentHighPriQueuePending = processor?.highPriorityQueue?.pending || 0;
    const paymentNormalQueueSize = processor?.normalQueue?.size || 0;
    const paymentNormalQueuePending = processor?.normalQueue?.pending || 0;

    let statsMsg = `*Bot Statistics* \\(v${escapeMarkdownV2('2.3-slots-roulette')}\\)\n\n`; // Escape version too
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

    if (connectionStats?.status && connectionStats?.stats) {
        statsMsg += `*Solana Connection:*\n`;
        statsMsg += `  \\- Endpoint: ${escapeMarkdownV2(connectionStats.status.currentEndpoint || 'N/A')} \\(Idx ${connectionStats.status.currentEndpointIndex ?? 'N/A'}\\)\n`;
        statsMsg += `  \\- Q:${connectionStats.status.queueSize ?? 'N/A'}, A:${connectionStats.status.activeRequests ?? 'N/A'}\n`;
        statsMsg += `  \\- Consecutive RL: ${connectionStats.status.consecutiveRateLimits ?? 'N/A'}\n`;
        statsMsg += `  \\- Last RL: ${escapeMarkdownV2(connectionStats.status.lastRateLimit || 'None')}\n`;
        statsMsg += `  \\- Tot Req: S:${connectionStats.stats.totalRequestsSucceeded ?? 'N/A'}, F:${connectionStats.stats.totalRequestsFailed ?? 'N/A'}\n`;
        statsMsg += `  \\- RL Events: ${connectionStats.stats.rateLimitEvents ?? 'N/A'}\n`;
        statsMsg += `  \\- Rotations: ${connectionStats.stats.endpointRotations ?? 'N/A'}\n`;
        statsMsg += `  \\- Success Rate: ${escapeMarkdownV2(connectionStats.stats.successRate || 'N/A')}\n`;
    } else {
        statsMsg += `*Solana Connection:* Stats unavailable\\.\n`;
    }

    await safeSendMessage(msg.chat.id, statsMsg, { parse_mode: 'MarkdownV2' });
}


// --- Server Startup & Shutdown Logic ---
// (Moved to Part 4)

// --- End of Part 3 ---
// index.js - Revised Part 4 (With Slots & Roulette additions)

// (Code continues directly from the end of Part 3)

// --- Server Startup & Shutdown Logic ---

// Encapsulated Webhook Setup Logic
async function setupTelegramWebhook() {
    if (process.env.RAILWAY_ENVIRONMENT && process.env.RAILWAY_PUBLIC_DOMAIN) {
        const webhookUrl = `https://${process.env.RAILWAY_PUBLIC_DOMAIN}${webhookPath}`; // webhookPath defined in Part 2a
        console.log(`Attempting to set webhook to: ${webhookUrl}`);
        let attempts = 0;
        const maxAttempts = 3;
        while (attempts < maxAttempts) {
            try {
                // Ensure any existing webhook is cleared first
                 await bot.deleteWebHook({ drop_pending_updates: true }).catch(e => console.warn(`Ignoring error during pre-webhook delete: ${e.message}`)); // Ignore delete errors

                await bot.setWebHook(webhookUrl, {
                    max_connections: parseInt(process.env.WEBHOOK_MAX_CONN || '10', 10), // Allow configuration
                    allowed_updates: ["message"], // Only process message updates via webhook for now
                    // drop_pending_updates: true // Optionally drop updates queued while bot was down
                });
                console.log(`✅ Webhook successfully set to: ${webhookUrl}`);
                return true; // Indicate webhook was set
            } catch (webhookError) {
                attempts++;
                console.error(`❌ Webhook setup attempt ${attempts}/${maxAttempts} failed:`, webhookError.message);
                 if (webhookError.code === 'ETELEGRAM' && webhookError.message.includes('URL host is empty')) {
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
        const info = await bot.getWebHookInfo().catch(e => { // Catch potential errors during getWebhookInfo
             console.error("Error fetching webhook info:", e.message);
             // If fetching info fails (e.g., network issue, bad token), assume no webhook is set
             return null;
        });

        if (!info || !info.url) { // Start polling only if webhook is not set or check failed
             if (bot.isPolling()) {
                 // console.log("ℹ️ Bot is already polling."); // Reduce noise
                 return;
             }
            console.log("ℹ️ Webhook not set (or check failed), starting bot polling...");
            await bot.deleteWebHook({ drop_pending_updates: true }).catch(e => console.warn(`Ignoring error during pre-polling delete: ${e.message}`)); // Ensure no residual webhook

            // Start polling - ensure options are passed correctly if needed
             await bot.startPolling({ /* polling: { interval: 300 } - Handled by default options? */ }); // Simplified startPolling call
            console.log("✅ Bot polling started successfully");
        } else {
            // console.log(`ℹ️ Webhook is set (${info.url}), polling will not be started.`); // Reduce noise
             if (bot.isPolling()) {
                 console.log("ℹ️ Stopping existing polling because webhook is now set.");
                 await bot.stopPolling({ cancel: true }).catch(e => console.warn(`Ignoring error during polling stop: ${e.message}`));
             }
        }
    } catch (err) {
        console.error("❌ Error managing polling state:", err.message);
        if (err.code === 'ETELEGRAM' && err.message.includes('409 Conflict')) {
            console.error("❌❌❌ Conflict detected! Another instance might be polling. Exiting.");
             shutdown('POLLING_CONFLICT_STARTUP', false).catch(() => process.exit(1));
             setTimeout(() => { console.error("Shutdown timed out after polling conflict. Forcing exit."); process.exit(1); }, 5000).unref();
        }
         if (err.response && err.response.statusCode === 401) {
             console.error("❌❌❌ FATAL: Unauthorized (401) checking webhook/polling. Check BOT_TOKEN. Exiting.");
             shutdown('BOT_TOKEN_INVALID_STARTUP', false).catch(() => process.exit(1));
             setTimeout(() => { console.error("Shutdown timed out after auth error. Forcing exit."); process.exit(1); }, 5000).unref();
        }
    }
}

// Encapsulated Payment Monitor Start Logic
function startPaymentMonitor() {
    if (monitorIntervalId) {
        // console.log("ℹ️ Payment monitor already running."); // Reduce noise
        return;
    }
    const monitorIntervalSeconds = parseInt(process.env.MONITOR_INTERVAL_SECONDS, 10);
    if (monitorIntervalSeconds < 10) { // Warn if interval seems too low
        console.warn("⚠️ MONITOR_INTERVAL_SECONDS seems very low (< 10s). Ensure your RPC can handle this frequency.");
    }
    console.log(`⚙️ Starting payment monitor (Interval: ${monitorIntervalSeconds}s)`);
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
    }, monitorIntervalSeconds * 1000);

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

    if (isRailwayRotation) {
        // For Railway rotations: stop accepting new connections quickly, allow existing to finish briefly.
        console.log("Railway container rotation: Closing server, stopping monitor, allowing brief queue drain.");
         if (monitorIntervalId) { clearInterval(monitorIntervalId); monitorIntervalId = null; }
         if (server) { server.close(() => console.log("- Server closed for rotation.")); }
         // Allow a very short time for critical in-flight requests before Railway likely terminates
         const drainTimeout = parseInt(process.env.RAILWAY_ROTATION_DRAIN_MS, 10);
         await Promise.race([
              Promise.all([ // Only wait for payment queues briefly
                   paymentProcessor.highPriorityQueue.onIdle(),
                   paymentProcessor.normalQueue.onIdle()
              ]),
              new Promise(resolve => setTimeout(resolve, drainTimeout))
         ]).catch(e => console.warn(`Ignoring error/timeout during rotation drain (${drainTimeout}ms):`, e.message));
        console.log("Rotation procedure complete from app perspective.");
        // Don't explicitly exit process here for rotations. Railway handles termination.
        isShuttingDown = false; // Reset flag if rotation didn't exit
        return;
    }

    // --- START: Full Shutdown Logic (for non-rotation signals like SIGINT/SIGTERM) ---
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
        if (server) {
            console.log("- Closing Express server...");
            await new Promise((resolve, reject) => {
                server.close((err) => {
                    if (err) {
                        console.error("⚠️ Error closing Express server:", err.message);
                        return reject(err);
                    }
                    console.log("- Express server closed.");
                    resolve(undefined);
                });
                 // Add a timeout for server closing
                  setTimeout(() => reject(new Error('Server close timeout (5s)')), 5000).unref();
            });
        }

        // Stop Telegram listeners gracefully
         console.log("- Stopping Telegram listeners...");
         if (bot.isPolling()) {
             await bot.stopPolling({ cancel: false }).catch(e => console.warn("Ignoring error during polling stop:", e.message)); // Let queued updates process
             console.log("- Stopped Telegram polling.");
         }
         try {
             const webhookInfo = await bot.getWebHookInfo().catch(() => null);
             if (webhookInfo && webhookInfo.url) {
                 await bot.deleteWebHook({ drop_pending_updates: false }).catch(e => console.warn("Ignoring error during webhook delete:", e.message));
                 console.log("- Removed Telegram webhook.");
             }
         } catch (whErr) { console.warn("⚠️ Error checking/removing webhook:", whErr.message); }

    } catch (e) {
        console.error("⚠️ Error stopping listeners/server:", e.message);
    }

    // 2. Wait for ongoing queue processing to finish (with timeout)
    const queueDrainTimeoutMs = parseInt(process.env.SHUTDOWN_QUEUE_TIMEOUT_MS, 10);
    console.log(`Waiting for active jobs to finish (max ${queueDrainTimeoutMs / 1000}s)...`);
    try {
        await Promise.race([
            Promise.all([
                messageQueue.onIdle(),
                paymentProcessor.highPriorityQueue.onIdle(),
                paymentProcessor.normalQueue.onIdle(),
                telegramSendQueue.onIdle() // Wait for outgoing TG messages too
            ]),
            new Promise((_, reject) => {
                const timeout = setTimeout(() => reject(new Error(`Queue drain timeout (${queueDrainTimeoutMs}ms)`)), queueDrainTimeoutMs);
                 if (timeout.unref) timeout.unref(); // Don't let timeout keep process alive
            })
        ]);
        console.log("- All processing queues are idle.");
    } catch (queueError) {
        console.warn("⚠️ Timed out waiting for queues or queue error during shutdown:", queueError.message);
        // Log queue sizes on timeout for debugging
         console.warn(`  Queue sizes on timeout: MsgQ=${messageQueue.size}, PayHP=${paymentProcessor.highPriorityQueue.size}, PayNP=${paymentProcessor.normalQueue.size}, TGSend=${telegramSendQueue.size}`);
    }

    // 3. Close database pool
    console.log("Closing database pool...");
    try {
        await pool.end();
        console.log("✅ Database pool closed.");
    } catch (dbErr) {
        console.error("❌ Error closing database pool:", dbErr);
    } finally {
        console.log("🛑 Full Shutdown sequence complete.");
        process.exit(0); // Exit cleanly after full shutdown attempt
    }
    // --- END: Full Shutdown Logic ---
};


// --- Signal handlers for graceful shutdown ---
process.on('SIGTERM', () => {
    const isRailway = !!process.env.RAILWAY_ENVIRONMENT;
    console.log(`SIGTERM received. Railway Environment: ${isRailway}`);
    shutdown('SIGTERM', isRailway).catch((err) => { // Pass isRailway flag
        console.error("Error during SIGTERM shutdown:", err);
        process.exit(1);
    });
});

process.on('SIGINT', () => { // Ctrl+C
    console.log(`SIGINT received.`);
    shutdown('SIGINT', false).catch((err) => { // Not a railway rotation
        console.error("Error during SIGINT shutdown:", err);
        process.exit(1);
    });
});

// Handle uncaught exceptions - last resort
process.on('uncaughtException', (err, origin) => {
    console.error(`🔥🔥🔥 Uncaught Exception at: ${origin}`, err);
    // Attempt a very quick shutdown, then force exit
    if (!isShuttingDown) { // Prevent recursive shutdown calls
         shutdown('UNCAUGHT_EXCEPTION', false).catch(() => {}); // Ignore shutdown errors here
    }
    setTimeout(() => {
        console.error("Forcing exit after uncaught exception.");
        process.exit(1);
    }, parseInt(process.env.SHUTDOWN_FAIL_TIMEOUT_MS, 10)).unref();
});

// Handle unhandled promise rejections
process.on('unhandledRejection', (reason, promise) => {
    console.error('🔥🔥🔥 Unhandled Rejection at:', promise, 'reason:', reason);
    // Depending on the severity, you might trigger a shutdown or just log
    // Example: if reason indicates database connection failure, maybe shutdown
    // if (reason instanceof Error && reason.message.includes("database connection")) {
    //     if (!isShuttingDown) shutdown('UNHANDLED_DB_REJECTION', false).catch(() => process.exit(1));
    // }
});


// --- Start the Application ---
const PORT = process.env.PORT || 3000;

// Start server immediately
server = app.listen(PORT, "0.0.0.0", () => { // Listen on 0.0.0.0 for container compatibility
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
                 // If it's an auth error, trigger shutdown
                 if (tgError.response && tgError.response.statusCode === 401) {
                     throw new Error("Invalid BOT_TOKEN detected during getMe check.");
                 }
                 // Otherwise, log warning but maybe continue? Or throw? Let's throw.
                 throw tgError;
             }

            console.log("  - Starting Payment Monitor...");
            startPaymentMonitor(); // Starts the interval and initial run

            // Optional: Adjust Solana Connection Concurrency after initial startup load
            const concurrencyAdjustDelay = parseInt(process.env.CONCURRENCY_ADJUST_DELAY_MS, 10);
            const targetConcurrency = parseInt(process.env.RPC_TARGET_CONCURRENT, 10);
            if (solanaConnection?.options && targetConcurrency !== solanaConnection.options.maxConcurrent && concurrencyAdjustDelay > 0) {
                 console.log(`⚙️ Scheduling Solana connection concurrency adjustment in ${concurrencyAdjustDelay / 1000}s...`);
                 setTimeout(() => {
                     if (solanaConnection?.options) { // Check again if connection exists
                         console.log(`⚡ Adjusting Solana connection concurrency from ${solanaConnection.options.maxConcurrent} to ${targetConcurrency}...`);
                         solanaConnection.options.maxConcurrent = targetConcurrency;
                         console.log(`✅ Solana maxConcurrent adjusted to ${targetConcurrency}`);
                     }
                 }, concurrencyAdjustDelay);
            }

            isFullyInitialized = true; // Mark as fully initialized *before* final ready message
            console.log("✅ Background Initialization Complete.");
            console.log("🚀🚀🚀 Solana Gambles Bot (v2.3-slots-roulette) is fully operational! 🚀🚀🚀"); // Updated version

        } catch (initError) { // Catch errors from DB init, Telegram setup, etc.
            console.error("🔥🔥🔥 Delayed Background Initialization Failed:", initError);
            console.error("❌ Exiting due to critical initialization failure.");
            // Attempt graceful shutdown even on init error
             if (!isShuttingDown) await shutdown('INITIALIZATION_FAILURE', false).catch(() => {});
             setTimeout(() => {
                 console.error("Shutdown timed out after initialization failure. Forcing exit.");
                 process.exit(1); // Force exit if shutdown hangs
             }, parseInt(process.env.SHUTDOWN_FAIL_TIMEOUT_MS, 10)).unref();
        }
    }, initDelay);
});

// Handle server startup errors (e.g., port already in use)
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
