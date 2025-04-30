// index.js - Final Version with War (Tie is Push), /betcf, /betwar

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

// --- START: Enhanced Environment Variable Checks (MODIFIED for RPC_URLS & New Games: War) ---
const REQUIRED_ENV_VARS = [
    'BOT_TOKEN',
    'DATABASE_URL',
    'BOT_PRIVATE_KEY',      // Coinflip, Slots, Roulette, War payouts (assuming reuse)
    'RACE_BOT_PRIVATE_KEY', // Race payouts
    'MAIN_WALLET_ADDRESS',  // Coinflip, Slots, Roulette, War deposits (assuming reuse)
    'RACE_WALLET_ADDRESS',  // Race deposits
    'RPC_URLS',             // Comma-separated list of RPC URLs
    // Optional vars with defaults are handled below
];

// Check for Railway-specific variables if deployed there
if (process.env.RAILWAY_ENVIRONMENT) {
    REQUIRED_ENV_VARS.push('RAILWAY_PUBLIC_DOMAIN');
}

// Optional vars with defaults - ADDED SLOTS, ROULETTE, WAR
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
    'SLOTS_MIN_BET': '0.01',
    'SLOTS_MAX_BET': '0.5',
    'SLOTS_EXPIRY_MINUTES': '10',
    'SLOTS_HOUSE_EDGE': '0.04', // 4% for Slots
    'ROULETTE_MIN_BET': '0.01', // Per placement
    'ROULETTE_MAX_BET': '1.0',  // Per placement
    'ROULETTE_EXPIRY_MINUTES': '10',
    'ROULETTE_HOUSE_EDGE': '0.027', // 2.7% (single zero wheel edge)
    'WAR_MIN_BET': '0.01', // Default for War
    'WAR_MAX_BET': '1.0',  // Default for War
    'WAR_EXPIRY_MINUTES': '10', // Default for War
    'WAR_HOUSE_EDGE': '0.0', // Default 0% for War base game (Tie is Push)
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
        version: '2.4-war' // Updated version string
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
        'User-Agent': `SolanaGamblesBot/2.4-war`
    },
    clientId: `SolanaGamblesBot/2.4-war`
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
// index.js - Part 2 (Final Version with War, /betcf, /betwar)

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

    // NOTE: Automatic MarkdownV2 escaping was removed here.
    // Escaping should be done *before* calling safeSendMessage
    // using the escapeMarkdownV2 helper function where needed.

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
        version: '2.4-war', // Updated version
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
            bot.processUpdate(req.body);
            performanceMonitor.logRequest(true);
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
         if (!gc) { // Check if game config itself exists
            errors.push(`Config for game "${game}" is missing.`);
            continue;
         }
         if (isNaN(gc.minBet) || gc.minBet <= 0) errors.push(`${game}.minBet invalid (${gc.minBet})`);
         if (isNaN(gc.maxBet) || gc.maxBet <= 0) errors.push(`${game}.maxBet invalid (${gc.maxBet})`);
         if (isNaN(gc.expiryMinutes) || gc.expiryMinutes <= 0) errors.push(`${game}.expiryMinutes invalid (${gc.expiryMinutes})`);
         // Allow houseEdge to be 0
         if (isNaN(gc.houseEdge) || gc.houseEdge < 0 || gc.houseEdge >= 1) errors.push(`${game}.houseEdge invalid (${gc.houseEdge})`);
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
    },
    war: { // Added War configuration
        minBet: parseFloat(process.env.WAR_MIN_BET),
        maxBet: parseFloat(process.env.WAR_MAX_BET),
        expiryMinutes: parseInt(process.env.WAR_EXPIRY_MINUTES, 10),
        houseEdge: parseFloat(process.env.WAR_HOUSE_EDGE) // Default 0.0
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
    // ... (implementation remains unchanged from original document) ...
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
    // ... (implementation remains unchanged from original document) ...
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

// --- End of Part 2a ---
// index.js - Part 2b (Final Version with War, /betcf, /betwar)

// (Code continues directly from the end of Part 2a - provided previously)

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
 * Analyzes a transaction to find SOL transfers to the target bot wallet.
 * Checks both top-level and inner instructions for transfers.
 * Uses balance changes as primary source, falls back to instruction parsing.
 * @param {import('@solana/web3.js').ParsedTransactionWithMeta | import('@solana/web3.js').VersionedTransactionResponse | null} tx - The transaction object from getParsedTransaction.
 * @param {'coinflip' | 'race' | 'slots' | 'roulette' | 'war'} gameTypeOrTargetWalletType - The type of game or target wallet type ('coinflip'/'slots'/'roulette'/'war' for main, 'race' for race).
 * @returns {{ transferAmount: bigint, payerAddress: string | null }} The transfer amount in lamports and the detected payer address.
 */
function analyzeTransactionAmounts(tx, gameTypeOrTargetWalletType) {
    let transferAmount = 0n;
    let payerAddress = null;

    // Determine target deposit address based on game type or explicitly passed type
    let targetAddress;
    if (gameTypeOrTargetWalletType === 'race') {
        targetAddress = process.env.RACE_WALLET_ADDRESS;
    } else { // Coinflip, Slots, Roulette, War, or if 'coinflip' passed directly use MAIN
        targetAddress = process.env.MAIN_WALLET_ADDRESS;
    }

    // Validation
    if (!tx || !targetAddress) {
        // console.warn("[analyzeAmounts] Invalid input: Missing tx or targetAddress.");
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
        const targetIndex = accountKeys.indexOf(targetAddress);

        if (targetIndex !== -1 &&
            tx.meta.preBalances.length > targetIndex && tx.meta.postBalances.length > targetIndex &&
            tx.meta.preBalances[targetIndex] !== null && tx.meta.postBalances[targetIndex] !== null) {

            balanceChange = BigInt(tx.meta.postBalances[targetIndex]) - BigInt(tx.meta.preBalances[targetIndex]);

            if (balanceChange > 0n) {
                transferAmount = balanceChange; // Initial assumption

                // Try to identify payer based on balance changes of signers
                // Handle different ways signatures might be represented
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
                // We pass the monitored walletType ('coinflip' or 'race') initially.
                // _processIncomingPayment will determine the *actual* target wallet type based on the bet's gameType
                result = await this._processIncomingPayment(job.signature, job.walletType);
            } else if (job.type === 'process_bet') {
                const bet = await pool.query('SELECT * FROM bets WHERE id = $1', [job.betId]).then(res => res.rows[0]);
                if (bet) {
                    // Ensure bet is actually in a state ready for processing
                    if (bet.status === 'payment_verified') {
                         await processPaidBet(bet); // processPaidBet handles own errors (defined in Part 3)
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
                         await handlePayoutJob(job); // handlePayoutJob handles own errors (defined in Part 3)
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
                              await safeSendMessage(job.chatId, `⚠️ Payout for bet \`${escapeMarkdownV2(job.memoId)}\` failed after ${attempt} attempt(s)\\. Error: ${safeErrorMsg}\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
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
                     await safeSendMessage(job.chatId || 'admin', `⚠️ Uncaught error processing job for bet \`${escapeMarkdownV2(job.memoId || job.betId)}\`\\. Please contact support\\.`, { parse_mode: 'MarkdownV2'});
                 }
            }
             return { processed: false, reason: `uncaught_job_error: ${error.message}` };
        } finally {
            this.activeProcesses.delete(jobKey); // Ensure the job key is removed from active set
        }
    }

    // Internal method to process an incoming payment signature
    async _processIncomingPayment(signature, monitoredWalletType) {
        const logPrefix = `Sig ${signature.slice(0, 6)}...`;
        if (processedSignaturesThisSession.has(signature)) {
            // console.log(`${logPrefix}: Already processed in this session.`); // Reduce noise
            return { processed: false, reason: 'already_processed_session' };
        }
        try {
            // Check DB first to avoid fetching TX for already processed signatures
            const exists = await pool.query('SELECT id, status FROM bets WHERE paid_tx_signature = $1 LIMIT 1', [signature]);
            if (exists.rowCount > 0) {
                // console.log(`${logPrefix}: Already exists in DB (Bet ID: ${exists.rows[0].id}, Status: ${exists.rows[0].status}).`); // Reduce noise
                processedSignaturesThisSession.add(signature); this._cleanSignatureCache();
                return { processed: false, reason: 'exists_in_db' };
            }
        } catch (dbError) {
             console.error(`${logPrefix}: DB error checking paid_tx_signature: ${dbError.message}`);
             // Potentially retryable? For now, treat as failure for this attempt.
             return { processed: false, reason: 'db_check_error', error: dbError };
        }

        let tx;
        try {
             tx = await this._getTransactionWithRetry(signature);
             if (!tx) {
                 // console.log(`${logPrefix}: Failed to fetch transaction or not confirmed after retries.`); // Reduce noise
                 return { processed: false, reason: 'fetch_failed_or_not_confirmed' };
             }
             if (tx.meta?.err){
                 // console.log(`${logPrefix}: Transaction failed on-chain, skipping. Error: ${JSON.stringify(tx.meta.err)}`); // Reduce noise
                 processedSignaturesThisSession.add(signature); this._cleanSignatureCache(); // Add failed TX sig to avoid re-processing
                 return { processed: false, reason: 'onchain_failure' };
             }

             const memo = await this._extractMemoGuaranteed(tx, signature);
             if (!memo) {
                 // console.log(`${logPrefix}: No valid memo found in transaction.`); // Reduce noise
                 processedSignaturesThisSession.add(signature); this._cleanSignatureCache(); // Add sig with no memo to avoid re-processing
                 return { processed: false, reason: 'no_valid_memo' };
             }

             const bet = await this._findBetGuaranteed(memo);
             if (!bet) {
                  // Check if a bet with this memo exists but isn't 'awaiting_payment'
                  const existingBetStatusResult = await pool.query('SELECT status FROM bets WHERE memo_id = $1 LIMIT 1', [memo]);
                  const existingBetStatus = existingBetStatusResult.rows[0]?.status;
                  if (existingBetStatus && existingBetStatus !== 'awaiting_payment') {
                       // console.log(`${logPrefix}: Bet for memo ${memo} exists but status is ${existingBetStatus}. Payment likely already processed or expired.`); // Reduce noise
                       processedSignaturesThisSession.add(signature); this._cleanSignatureCache(); // Add sig for already handled bet
                       return { processed: false, reason: 'bet_already_processed_or_expired' };
                  } else {
                       // console.log(`${logPrefix}: No bet found awaiting payment for memo ${memo}.`); // Reduce noise
                       processedSignaturesThisSession.add(signature); this._cleanSignatureCache(); // Add sig for unknown memo
                       return { processed: false, reason: 'no_matching_bet_found' };
                  }
             }

             // --- Determine correct target wallet type for amount analysis based on BET's game type ---
             let targetWalletType;
             if (bet.game_type === 'race') {
                 targetWalletType = 'race';
             } else { // Coinflip, Slots, Roulette, War use the main wallet
                 targetWalletType = 'coinflip'; // Use 'coinflip' as the key for the main wallet logic
             }
             // --- End wallet type determination ---

             // Process the payment details within a DB transaction
             const processResult = await this._processPaymentGuaranteed(bet, signature, targetWalletType, tx);

              // If payment verification was successful, queue the next step (game processing)
              if (processResult.processed) {
                   const finalBetDetails = await pool.query('SELECT status FROM bets WHERE id = $1', [bet.id]).then(res => res.rows[0]);
                   if (finalBetDetails && finalBetDetails.status === 'payment_verified') {
                       await this._queueBetProcessing(bet); // Queue using the original bet object fetched
                   } else {
                       // This case should be rare due to transaction locking in _processPaymentGuaranteed
                       console.error(`${logPrefix}: CRITICAL! Post-verify status mismatch for Bet ID ${bet.id}. Status: ${finalBetDetails?.status ?? 'Not Found'} instead of 'payment_verified'.`);
                       // Attempt to set an error status, though the cause is unclear
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
                      // Treat as potentially temporary, could retry outside if needed
                      lastError = new Error("Transaction not found or not confirmed yet."); // Set error for logging if retries fail
                      retries--; // Decrement retry count
                      if (retries > 0) await new Promise(r => setTimeout(r, 500 * (3 - retries))); // Backoff before next try
                      else return null; // Retries exhausted
                      continue; // Go to next iteration
                  }
                  return tx; // Return the transaction object
            } catch (error) {
                 lastError = error;
                 retries--;
                 console.warn(`Sig ${signature?.slice(0,6)}: Error fetching getParsedTransaction (Attempt ${4-retries}/3): ${error.message}`);
                 if (!isRetryableError(error) || retries === 0) {
                     console.error(`Sig ${signature?.slice(0,6)}: Final error fetching transaction: ${error.message}`);
                     return null; // Failed to fetch
                 }
                 await new Promise(r => setTimeout(r, 500 * (3 - retries))); // Backoff
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
            // console.log(`[MEMO CACHE] Hit for memo ${memo}`); // Reduce noise
            // Ensure the cached bet is still awaiting payment
             if(cachedBet.bet?.status === 'awaiting_payment'){
                  return cachedBet.bet;
             } else {
                 // Status changed, remove from cache and query DB
                 this.memoCache.delete(memo);
                 // console.log(`[MEMO CACHE] Removed memo ${memo} due to status change (${cachedBet.bet?.status}).`);
             }
        } else if (cachedBet) {
             // Expired cache entry
             this.memoCache.delete(memo);
             // console.log(`[MEMO CACHE] Expired memo ${memo}`);
        }

        // DB lookup with retry logic for transient connection issues
        let retries = 3;
        let lastError = null;
        while (retries > 0) {
            try {
                const res = await pool.query(
                    // Explicitly look for 'awaiting_payment' status
                    "SELECT * FROM bets WHERE memo_id = $1 AND status = 'awaiting_payment' ORDER BY created_at DESC LIMIT 1",
                    [memo]
                );
                const bet = res.rows[0];
                if (bet) {
                    // Cache the found bet (only if status is correct)
                    this.memoCache.set(memo, { bet, timestamp: Date.now() });
                    // Schedule cache expiration check
                    setTimeout(() => {
                        const current = this.memoCache.get(memo);
                        if (current && current.bet?.id === bet.id && Date.now() - current.timestamp >= this.cacheTTL) {
                            this.memoCache.delete(memo);
                        }
                    }, this.cacheTTL + 1000);
                }
                return bet; // Return bet object or undefined if not found/wrong status
            } catch (dbError) {
                lastError = dbError;
                retries--;
                console.error(`DB error finding bet for memo ${memo} (Attempt ${4-retries}/3): ${dbError.message}`);
                if (!isRetryableError(dbError) || retries === 0) {
                    console.error(`[FIND_BET] Non-retryable DB error or retries exhausted for memo ${memo}.`);
                    // Do not throw here, just return undefined
                    return undefined;
                }
                await new Promise(r => setTimeout(r, 500 * (3 - retries))); // Short backoff
            }
        }
        return undefined; // Retries exhausted
    }

    // Performs payment validation and updates DB within a transaction
    async _processPaymentGuaranteed(bet, signature, targetWalletType, tx) { // Accept targetWalletType and tx object
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
                // console.log(`${logPrefix}: Status check failed inside TX (Expected 'awaiting_payment', got '${currentStatus}'). Aborting.`); // Reduce noise
                // Add signature to processed cache if status mismatch indicates it was handled already
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


            // 3. Analyze transaction amount and get payer using the correct target wallet type
            const { transferAmount, payerAddress } = analyzeTransactionAmounts(tx, targetWalletType);
             if (transferAmount <= 0n) {
                 // This might happen if the TX didn't contain a direct transfer
                 await client.query(`UPDATE bets SET status = $1, processed_at = NOW() WHERE id = $2`, ['error_payment_amount_zero', bet.id]);
                 await client.query('COMMIT');
                 console.log(`${logPrefix}: No valid transfer amount found in transaction ${signature} to target wallet.`);
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
                 await safeSendMessage(bet.chat_id, `⚠️ Your payment for bet \`${escapeMarkdownV2(bet.memo_id)}\` was received, but the amount was too low\\. Expected ${escapeMarkdownV2((Number(expected)/LAMPORTS_PER_SOL).toFixed(6))} SOL, but received ${escapeMarkdownV2((Number(transferAmount)/LAMPORTS_PER_SOL).toFixed(6))} SOL\\. Your bet could not be processed\\.`, { parse_mode: 'MarkdownV2' });
                 processedSignaturesThisSession.add(signature); this._cleanSignatureCache(); // Add sig to cache
                 return { processed: false, reason: 'payment_amount_low' };
             } else if (transferAmount > expected + tolerance) {
                 console.warn(`${logPrefix}: Payment amount slightly higher than expected. Expected ${expected}, Got ${transferAmount}. Processing anyway.`);
                 // Log overpayment but continue processing
             } else {
                  // Amount is within tolerance
                  // console.log(`${logPrefix}: Payment amount verified.`); // Reduce noise
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
                  // Check the status again to see what happened
                  const finalCheck = await pool.query('SELECT status FROM bets WHERE id = $1', [bet.id]);
                  // Add signature to processed if already handled by another process
                  if(finalCheck.rows[0]?.status !== 'awaiting_payment'){
                     processedSignaturesThisSession.add(signature); this._cleanSignatureCache();
                  }
                  return { processed: false, reason: `update_failed_in_tx_final_status_${finalCheck.rows[0]?.status ?? 'not_found'}` };
              }


            // 6. Link wallet if payer found and different from current linked wallet (still inside TX)
            if (payerAddress) {
                 const currentLinkedWallet = await getLinkedWallet(bet.user_id); // Use helper (checks cache first)
                 if (!currentLinkedWallet || currentLinkedWallet !== payerAddress) {
                      // console.log(`${logPrefix}: Linking payer wallet ${payerAddress.slice(0,6)}... for user ${bet.user_id}.`); // Reduce noise
                      await client.query(
                           `INSERT INTO wallets (user_id, wallet_address, last_used_at)
                            VALUES ($1, $2, NOW())
                            ON CONFLICT (user_id)
                            DO UPDATE SET wallet_address = EXCLUDED.wallet_address, last_used_at = NOW()`,
                           [String(bet.user_id), payerAddress]
                      );
                      // Update cache immediately after successful DB link/update
                      walletCache.set(`wallet-${bet.user_id}`, { wallet: payerAddress, timestamp: Date.now() });
                      // Schedule cache expiration check
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
            console.error(`${logPrefix}: Error during DB transaction for payment processing: ${error.message}`);
            try { // Attempt rollback safely
                 await client.query('ROLLBACK');
            } catch (rollbackError) {
                 console.error(`${logPrefix}: Failed to rollback transaction after error: ${rollbackError.message}`);
            }
            // Attempt to set a generic processing error status outside the failed transaction
            try {
                 // Check current status before overwriting with error
                 const statusCheck = await pool.query('SELECT status FROM bets WHERE id = $1', [bet.id]);
                 if (statusCheck.rows.length > 0 && statusCheck.rows[0].status === 'awaiting_payment') {
                      await pool.query(`UPDATE bets SET status = $1, processed_at = NOW() WHERE id = $2`, ['error_processing_db', bet.id]);
                 }
            } catch (updateErr) {
                 console.error(`${logPrefix}: Failed to set error status after rollback: ${updateErr.message}`);
            }
            // Add betId context to the error for higher-level handling if needed
            error.betId = bet.id;
            // Do not throw here, let processJob handle based on retry logic
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
            // Pass necessary details if needed by processJob later (e.g., priority)
            priority: bet.priority || 0,
            // Include info for potential user messages if processJob fails later
            chatId: bet.chat_id,
            memoId: bet.memo_id
        });
    }


     // Helper method to clean up the signature cache
     _cleanSignatureCache() {
         if (processedSignaturesThisSession.size > MAX_PROCESSED_SIGNATURES) {
              console.warn(`[SigCache] Reached max size (${MAX_PROCESSED_SIGNATURES}). Clearing oldest ${Math.floor(MAX_PROCESSED_SIGNATURES / 2)} entries.`);
              // Simple strategy: remove half the entries (assumes Set iteration order is insertion order)
              const entriesToRemove = Math.floor(MAX_PROCESSED_SIGNATURES / 2);
              let i = 0;
              // Use Array.from to create a stable array for iteration before deletion
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
// index.js - Part 3 (Corrected Again - Final Version with War, /betcf, /betwar)

// (Code continues directly from the end of Part 2b)

// --- Payment Monitoring Loop ---
let isMonitorRunning = false;
const botStartupTime = Math.floor(Date.now() / 1000);
let monitorIntervalId = null;

async function monitorPayments() {
    if (isMonitorRunning) return;
    if (!isFullyInitialized) return; // Don't run if bot isn't ready

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

        // Define wallets to monitor
        // 'coinflip' type maps to MAIN_WALLET for coinflip, slots, roulette, war deposits
        const monitoredWallets = [
             { address: process.env.MAIN_WALLET_ADDRESS, type: 'coinflip', priority: 0 },
             { address: process.env.RACE_WALLET_ADDRESS, type: 'race', priority: 0 },
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
                // Fetch only confirmed signatures to align with payment processing checks
                const options = { limit: fetchLimit, commitment: 'confirmed' };

                signaturesForWallet = await solanaConnection.getSignaturesForAddress(
                    new PublicKey(walletAddress),
                    options
                );

                if (!signaturesForWallet || signaturesForWallet.length === 0) {
                    continue; // No signatures found for this wallet
                }

                signaturesFoundThisCycle += signaturesForWallet.length;

                // Filter out old/failed TXs more reliably
                const startupBufferSeconds = 600; // 10 minutes buffer
                const recentSignatures = signaturesForWallet.filter(sigInfo => {
                    // Skip if signature has an error object
                    if (sigInfo.err) {
                        if (!processedSignaturesThisSession.has(sigInfo.signature)) {
                             processedSignaturesThisSession.add(sigInfo.signature);
                             paymentProcessor._cleanSignatureCache(); // Add failed sigs to cache to prevent re-processing
                        }
                        return false;
                    }
                    // Skip if blockTime is older than bot startup time (minus buffer)
                    if (sigInfo.blockTime && sigInfo.blockTime < (botStartupTime - startupBufferSeconds)) {
                        return false;
                    }
                    // Skip if signature already processed in this session
                    if (processedSignaturesThisSession.has(sigInfo.signature)) {
                       return false;
                    }
                    // Skip if signature is currently being processed
                    const jobKey = `monitor_payment:${sigInfo.signature}`;
                    if (paymentProcessor.activeProcesses.has(jobKey)){
                       return false;
                    }

                    return true; // Keep signature if recent, not failed, not processed/processing
                });


                if (recentSignatures.length === 0) continue;

                // Process oldest first within the fetched batch to maintain order
                recentSignatures.reverse();

                for (const sigInfo of recentSignatures) {
                    // Double check cache/active just before adding
                    if (processedSignaturesThisSession.has(sigInfo.signature)) continue;
                    const jobKey = `monitor_payment:${sigInfo.signature}`;
                    if (paymentProcessor.activeProcesses.has(jobKey)) continue;

                    signaturesQueuedThisCycle++;
                    // Pass the monitored wallet type ('coinflip' or 'race') to the job
                    await paymentProcessor.addPaymentJob({
                        type: 'monitor_payment',
                        signature: sigInfo.signature,
                        walletType: wallet.type, // Pass type associated with the monitored address
                        priority: wallet.priority,
                    });
                } // End loop through signatures

            } catch (error) {
                console.error(`[Monitor] Error fetching/processing signatures for wallet ${walletAddress}:`, error.message);
                performanceMonitor.logRequest(false); // Log error for monitoring
                 // Check if the error is retryable or suggests an RPC issue
                 if (!isRetryableError(error)) {
                     console.warn(`[Monitor] Non-retryable error for wallet ${walletAddress}. Consider checking RPC health.`);
                 }
            }
        } // End loop through wallets

    } catch (err) {
        console.error('❌ MonitorPayments Error in main try block:', err);
        performanceMonitor.logRequest(false);
    } finally {
        isMonitorRunning = false; // Release the lock
        const duration = Date.now() - startTime;
        if (signaturesFoundThisCycle > 0 || duration > 1500) { // Log if work was done or took long
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
 * @param {'coinflip' | 'race'} payoutWalletType - Determines which private key to use ('coinflip' uses BOT_PRIVATE_KEY for main wallet games, 'race' uses RACE_BOT_PRIVATE_KEY).
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

    // Validate amount (should already be BigInt if called correctly by handlePayoutJob)
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

    // Calculate Priority Fee dynamically based on settings
    const basePriorityFee = parseInt(process.env.PAYOUT_BASE_PRIORITY_FEE_MICROLAMPORTS, 10);
    const maxPriorityFee = parseInt(process.env.PAYOUT_MAX_PRIORITY_FEE_MICROLAMPORTS, 10);
    // Use PAYOUT_PRIORITY_FEE_RATE (e.g., 0.0001 for 0.01%)
    const calculatedFee = Math.floor(Number(amountToSend) * PRIORITY_FEE_RATE);
    // Clamp the fee between base and max
    let priorityFeeMicroLamports = Math.max(basePriorityFee, Math.min(calculatedFee, maxPriorityFee));


    if (isNaN(priorityFeeMicroLamports)) {
         console.error(`[${operationId}] ❌ ERROR: NaN detected during priority fee calculation! base=${basePriorityFee}, max=${maxPriorityFee}, rate=${PRIORITY_FEE_RATE}, amount=${amountToSend}, calc=${calculatedFee}, final=${priorityFeeMicroLamports}`);
          console.warn(`[${operationId}] NaN priority fee, defaulting to base fee: ${basePriorityFee}`);
          priorityFeeMicroLamports = basePriorityFee; // Use base as fallback
    }
    // console.log(`DEBUG_SENDSOL: ${operationId} - Using Priority Fee: ${priorityFeeMicroLamports} microLamports`); // Reduce noise


    try {
        // console.log(`DEBUG_SENDSOL: ${operationId} - Entering transaction build/send block...`); // Reduce noise
        const payerWallet = Keypair.fromSecretKey(bs58.decode(privateKey));

        // Fetch latest blockhash just before sending
        const latestBlockhash = await solanaConnection.getLatestBlockhash('confirmed');
        if (!latestBlockhash || !latestBlockhash.blockhash || !latestBlockhash.lastValidBlockHeight) {
            throw new Error('Failed to get valid latest blockhash object from RPC.');
        }
        // console.log(`[${operationId}] Using blockhash: ${latestBlockhash.blockhash}, lastValidHeight: ${latestBlockhash.lastValidBlockHeight}`); // More debug

        const transaction = new Transaction({
            recentBlockhash: latestBlockhash.blockhash,
            feePayer: payerWallet.publicKey
        });

        // Add priority fee instruction
        transaction.add(
            ComputeBudgetProgram.setComputeUnitPrice({ microLamports: priorityFeeMicroLamports })
        );

        // Optional: Add compute limit if needed, but usually default is fine for simple transfers
        // transaction.add(ComputeBudgetProgram.setComputeUnitLimit({ units: 200000 })); // Example limit

        // Add the transfer instruction
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
            [payerWallet], // Signer
            {
                commitment: 'confirmed', // Wait for 'confirmed' commitment
                skipPreflight: false, // Keep preflight enabled for safety
                maxRetries: parseInt(process.env.SCT_MAX_RETRIES || '3', 10), // Retries for sendAndConfirm itself
                preflightCommitment: 'confirmed', // Preflight commitment level
                lastValidBlockHeight: latestBlockhash.lastValidBlockHeight, // Include last valid block height
            }
        );

        console.log(`[${operationId}] SUCCESS! ✅ Sent ${amountSOL.toFixed(6)} SOL to ${recipientPubKey.toBase58()}. TX: ${signature.slice(0,10)}...`);
        return { success: true, signature };

    } catch (error) {
        console.error(`[${operationId}] ❌ SEND FAILED. Error message:`, error.message);
        if (error.logs) {
             console.error(`[${operationId}] Simulation Logs (if available, last 10):`);
             error.logs.slice(-10).forEach(log => console.error(`   -> ${log}`));
        }

        const errorMsg = error.message.toLowerCase();
        let returnError = error.message; // Default to original message

        // Classify error for retry logic / user message
        if (errorMsg.includes('insufficient lamports') || errorMsg.includes('insufficient funds')) { returnError = 'Insufficient funds in payout wallet.'; }
        else if (errorMsg.includes('blockhash not found') || errorMsg.includes('block height exceeded') || errorMsg.includes('slot advance behavior')) { returnError = 'Transaction expired (blockhash invalid/expired). Retryable.'; }
        else if (errorMsg.includes('transaction was not confirmed') || errorMsg.includes('timed out waiting')) { returnError = `Transaction confirmation timeout (${confirmationTimeoutMs / 1000}s). May succeed later. Retryable.`;}
        else if (errorMsg.includes('custom program error') || errorMsg.includes('invalid account data') || errorMsg.includes('account not found')) { returnError = `Permanent chain error: ${error.message}`; }
        else if (isRetryableError(error)) { returnError = `Temporary network/RPC error: ${error.message}. Retryable.`; } // Check using helper
        else { returnError = `Send/Confirm error: ${error.message}`; } // Default for other errors

        // Attach retryable status to the error object if possible
        error.retryable = isRetryableError(error);

        // Ensure the error object passed back contains the classified message
        error.message = returnError;

        // Rethrow the modified error object so processJob retry logic gets the classified reason
        throw error;
        // return { success: false, error: returnError }; // Old way: return object
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
            if (bet.game_type === 'coinflip') {
                await handleCoinflipGame(bet);
            } else if (bet.game_type === 'race') {
                await handleRaceGame(bet);
            } else if (bet.game_type === 'slots') {
                await handleSlotsGame(bet);
            } else if (bet.game_type === 'roulette') {
                await handleRouletteGame(bet);
            } else if (bet.game_type === 'war') { // ADDED WAR ROUTE
                await handleWarGame(bet);
            } else {
                console.error(`❌ Unknown game type '${bet.game_type}' for bet ${bet.id}`);
                await updateBetStatus(bet.id, 'error_unknown_game');
            }
        } catch (gameError) {
             console.error(`❌ Error executing game logic for ${bet.game_type} bet ${bet.id}:`, gameError);
             // Mark bet with game processing error status
             await updateBetStatus(bet.id, 'error_game_logic');
             // Notify user potentially
             await safeSendMessage(bet.chat_id, `⚠️ An error occurred while running the game for your bet \`${escapeMarkdownV2(bet.memo_id)}\`\\. Please contact support\\.`, { parse_mode: 'MarkdownV2'});
        }
    } catch (error) {
        console.error(`❌ Error during game processing setup for bet ${bet.id} (${bet.memo_id}):`, error.message);
        if (client) {
            try { await client.query('ROLLBACK'); } catch (rbError) { console.error("Rollback failed:", rbError); }
        }
        // Mark bet with error status - do this outside the failed transaction
         // Check status first to avoid overwriting a more specific error
         const currentStatusResult = await pool.query('SELECT status FROM bets WHERE id = $1', [bet.id]);
         if (currentStatusResult.rows[0]?.status === 'payment_verified' || currentStatusResult.rows[0]?.status === 'processing_game') {
             await updateBetStatus(bet.id, 'error_processing_setup');
         }
    } finally {
        if (client) client.release(); // Ensure client is always released
    }
}

// --- Utility Functions (Existing) ---

// Calculates payout amount based on winnings and house edge
function calculateNetPayout(grossWinningsLamports, gameType) {
    try {
        const winnings = BigInt(grossWinningsLamports); // Ensure input is BigInt
        if (winnings <= 0n) return 0n; // No payout if no gross win

        const config = GAME_CONFIG[gameType];
        if (!config || typeof config.houseEdge !== 'number' || config.houseEdge < 0 || config.houseEdge >= 1) {
            console.error(`[calculateNetPayout] Invalid house edge configuration for game type "${gameType}". Returning 0 payout.`);
            return 0n;
        }
        // If house edge is 0, return gross winnings directly
        if (config.houseEdge === 0) {
             // console.log(`DEBUG_PAYOUT_CALC: Game=${gameType}, HE=0, Net=${winnings}`);
             return winnings;
        }

        // Calculate the house cut based on the gross winnings
        // Use integer math for precision
        const houseCutMultiplier = Math.round(config.houseEdge * 10000); // e.g., 0.02 -> 200
        const houseCut = (winnings * BigInt(houseCutMultiplier)) / 10000n;

        const netPayout = winnings - houseCut;

        // console.log(`DEBUG_PAYOUT_CALC: Game=${gameType}, Gross=${winnings}, HE=${config.houseEdge}, Cut=${houseCut}, Net=${netPayout}`); // Debug log

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
         let name = user.first_name || `User_${String(user_id).slice(-4)}`;
         if(user.username) name = `@${user.username}`; // Overwrite with username if available

         return escapeMarkdownV2(name); // Use central escape function
     } catch (e) {
         // Handle user not found or other errors gracefully
          if (e.response && e.response.statusCode === 400 && e.message.includes('user not found')) {
              // User might have left the chat
              // console.warn(`Couldn't get username/name for user ${user_id} in chat ${chat_id}: User not found.`);
          } else if (e.response && e.response.statusCode === 403) {
               // Bot might be blocked or lack permissions
               // console.warn(`Couldn't get username/name for user ${user_id} in chat ${chat_id}: Bot blocked or no permission.`);
          } else {
               // console.warn(`Couldn't get username/name for user ${user_id} in chat ${chat_id}:`, e.message); // Other errors
          }
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

    // Determine result considering house edge
    const houseWinRoll = Math.random();
    let result;
    let isHouseWin = houseWinRoll < config.houseEdge;

    // Force house win if roll hits edge
    if (isHouseWin) {
        result = (choice === 'heads') ? 'tails' : 'heads'; // House makes player lose
        // console.log(`${logPrefix}: House edge triggered.`);
    } else {
        // Fair roll otherwise
        result = (Math.random() < 0.5) ? 'heads' : 'tails';
    }

    const win = (result === choice);

    let payoutLamports = 0n;
    if (win) {
        // GrossWin = Bet * 2
        const grossWin = BigInt(expected_lamports) * 2n;
        // Apply house edge using the net payout function
        payoutLamports = calculateNetPayout(grossWin, 'coinflip');
    }

    const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
    const displayName = await getUserDisplayName(chat_id, user_id);

    // Process win
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
            return; // Exit early, no payout job needed yet
        }
        // Wallet linked, proceed to payout
        try {
             const statusUpdated = await updateBetStatus(betId, 'processing_payout');
             if (!statusUpdated) {
                 console.error(`${logPrefix}: CRITICAL! Failed to update status from 'processing_game' to 'processing_payout' before queueing! Aborting payout queue.`);
                 await safeSendMessage(chatId, `⚠️ Internal error preparing your payout for bet \`${escapeMarkdownV2(memo_id)}\`\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
                 await updateBetStatus(betId, 'error_payout_status_update'); // Mark with specific error
                 return;
             }
             // Notify user payout is processing
            await safeSendMessage(chat_id,
                `🎉 ${displayName}, you won ${escapeMarkdownV2(payoutSOL.toFixed(6))} SOL\\!\n` +
                `Result: *${escapeMarkdownV2(result)}*\n\n` +
                `💸 Processing payout to your linked wallet\\.\\.\\.`,
                { parse_mode: 'MarkdownV2' }
            );
             // Queue the payout job
            await paymentProcessor.addPaymentJob({
                type: 'payout',
                betId,
                recipient: winnerAddress,
                amount: payoutLamports.toString(), // Pass calculated net payout
                gameType: 'coinflip', // Use 'coinflip' type to select BOT_PRIVATE_KEY
                priority: 2, // High priority for payouts
                chatId: chat_id,
                displayName: displayName, // Already escaped
                memoId: memo_id,
            });
        } catch (e) {
            console.error(`${logPrefix}: Error preparing/queueing payout info:`, e);
             // Try to set error status if preparation failed
             await updateBetStatus(betId, 'error_payout_preparation');
            await safeSendMessage(chat_id, `⚠️ Error occurred while processing your coinflip win for bet \`${escapeMarkdownV2(memo_id)}\`\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
        }
    } else { // Process loss
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
        { name: 'Silver', emoji: '💎', odds: 15.0, baseProb: 0.01 } // Probabilities should sum close to 1
       ];
     const totalProbSum = horses.reduce((sum, h) => sum + h.baseProb, 0);
     // if (Math.abs(totalProbSum - 1.0) > 0.001) console.warn(`Race base probabilities sum to ${totalProbSum}, not 1.0.`); // Optional warning

    // --- Determine Winning Horse (Weighted selection + House Edge) ---
    let winningHorse = null;
    const houseEdgeTarget = 1.0 - config.houseEdge; // e.g., 1.0 - 0.02 = 0.98
    const randomRoll = Math.random(); // 0.0 to < 1.0
    const isHouseWin = randomRoll >= houseEdgeTarget; // House wins if roll is in the top % defined by house edge

    // Pick a visual winner using weighted probabilities
     let cumulativeProb = 0;
     const visualWinnerRoll = Math.random();
     if (totalProbSum <= 0) { // Safeguard against bad config
          winningHorse = horses[0];
          console.error(`${logPrefix}: Total horse probability is zero or negative! Defaulting winner.`);
     } else {
         for (const horse of horses) {
              cumulativeProb += (horse.baseProb / totalProbSum); // Normalize probability
              if (visualWinnerRoll <= cumulativeProb) {
                  winningHorse = horse;
                  break;
              }
         }
         if (!winningHorse) winningHorse = horses[horses.length - 1]; // Fallback if something went wrong
     }

     // If house edge hits, the player effectively loses, even if their chosen horse visually wins.
      if (isHouseWin) {
           // console.log(`${logPrefix}: House edge triggered. Visual Winner: ${winningHorse.name}`);
      }
    // --- End Winning Horse Determination ---

    // Player wins ONLY if house edge didn't hit AND their horse visually won
    const playerWins = !isHouseWin && (chosenHorseName.toLowerCase() === winningHorse.name.toLowerCase());

    let payoutLamports = 0n;
    if (playerWins) {
        // Calculate gross winnings: Bet Amount * Winning Horse Odds
        const grossWinningsLamports = (BigInt(expected_lamports) * BigInt(Math.round(winningHorse.odds * 100))) / 100n;
        payoutLamports = calculateNetPayout(grossWinningsLamports, 'race'); // Apply house edge
    }

    const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
    const displayName = await getUserDisplayName(chat_id, user_id);

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
        // console.log(`${logPrefix}: ${displayName} WON ${payoutSOL.toFixed(6)} SOL! Horse: ${chosenHorseName}, Winner: ${winningHorse.name}`);
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
                  console.error(`${logPrefix}: CRITICAL! Failed to update status from 'processing_game' to 'processing_payout' before queueing! Aborting payout queue.`);
                  await safeSendMessage(chatId, `⚠️ Internal error preparing your payout for bet \`${escapeMarkdownV2(memo_id)}\`\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
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
    } else { // Loss (either wrong horse OR house edge hit)
        // console.log(`${logPrefix}: ${displayName} LOST. Choice: ${chosenHorseName}, Visual Winner: ${winningHorse.name}, HouseWin: ${isHouseWin}`);
        await updateBetStatus(betId, 'completed_loss');
         // Adjust loss message slightly if house edge might have been the reason
         const lossReason = isHouseWin && (chosenHorseName.toLowerCase() === winningHorse.name.toLowerCase())
             ? `Your horse *${escapeMarkdownV2(chosenHorseName)}* finished first, but the house took the win this time\\!`
             : `Your horse *${escapeMarkdownV2(chosenHorseName)}* lost the race\\! Winner: ${winningHorse.emoji} *${escapeMarkdownV2(winningHorse.name)}*\\.`;

        await safeSendMessage(chat_id,
            `❌ ${displayName}, ${lossReason} Better luck next time\\!`,
            { parse_mode: 'MarkdownV2' }
        );
    }
}

// --- Slots Game Logic ---

const SLOTS_SYMBOLS = {
    CHERRY: { emoji: '🍒', weight: 10, payout: { 2: 1, 3: 19 } }, // 2 cherries pays 1:1 (bet*1), 3 pays 19:1 (bet*19)
    ORANGE: { emoji: '🍊', weight: 8, payout: { 3: 49 } },
    BAR:    { emoji: '🍫', weight: 6, payout: { 3: 99 } }, // Using chocolate bar emoji for BAR
    SEVEN:  { emoji: '7️⃣', weight: 4, payout: { /* Special handling for first reel only */ } },
    TRIPLE_SEVEN: { emoji: '🎰', weight: 2, payout: { 3: 499 } }, // Using slot machine for 777 Jackpot
    BLANK:  { emoji: '➖', weight: 15, payout: {} }, // Blank symbol
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

    // Format result string with escaped pipes for MarkdownV2
    let resultEmojis = results.map(key => SLOTS_SYMBOLS[key].emoji).join(' \\| '); // Add escaped pipes

    // console.log(`${logPrefix}: Spin result display: ${resultEmojis}`); // Log escaped string

    // --- Determine Win ---
    let winMultiplier = 0; // Base payout multiplier (Odds ratio N:1 -> Multiplier = N)
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
        winMultiplier = 4; // Pays 4:1
        winDescription = "Seven on First Reel!";
    }
    // Check for 2 Cherries on first two reels (only if no higher win)
    else if (results[0] === 'CHERRY' && results[1] === 'CHERRY' && winMultiplier === 0) {
        winMultiplier = SLOTS_SYMBOLS.CHERRY.payout[2]; // Pays 1:1
        winDescription = "Two Cherries!";
    }

    // --- Calculate Payout ---
    let payoutLamports = 0n;
    if (winMultiplier > 0) {
         // Gross winnings = Bet Amount * (Multiplier + 1) -> Stake back + Winnings
         const grossWinningsLamports = betAmountLamports * BigInt(winMultiplier + 1);
         payoutLamports = calculateNetPayout(grossWinningsLamports, 'slots'); // Apply house edge
    }

    const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
    const displayName = await getUserDisplayName(chat_id, user_id);
    const win = payoutLamports > 0n; // Win only if final payout > 0

    // --- Send Result Message ---
     let resultMessage = `🎰 *Slots Result* for ${displayName} \\!\n\n` +
                         `*Result:* ${resultEmojis}\n\n`;


    if (win) {
        resultMessage += `🎉 *${escapeMarkdownV2(winDescription)}* You won ${escapeMarkdownV2(payoutSOL.toFixed(6))} SOL\\!`;
         // console.log(`${logPrefix}: ${displayName} WON ${payoutSOL.toFixed(6)} SOL! Result: ${resultEmojis}, Desc: ${winDescription}`);
    } else {
        resultMessage += `❌ No win this time\\. Better luck next spin\\!`;
        // console.log(`${logPrefix}: ${displayName} LOST. Result: ${resultEmojis}`);
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
                 console.error(`${logPrefix}: CRITICAL! Failed to update status from 'processing_game' to 'processing_payout' before queueing! Aborting payout queue.`);
                 await safeSendMessage(chatId, `⚠️ Internal error preparing your payout for bet \`${escapeMarkdownV2(memo_id)}\`\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
                 await updateBetStatus(betId, 'error_payout_status_update');
                 return;
             }
             // Don't send another message immediately, result message was sent above
             await paymentProcessor.addPaymentJob({
                 type: 'payout',
                 betId,
                 recipient: winnerAddress,
                 amount: payoutLamports.toString(),
                 gameType: 'coinflip', // Use main payout key (shared wallet)
                 priority: 1, // Normal priority for game payouts
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

// --- Roulette Game Logic ---

const ROULETTE_NUMBERS = {
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
// Payout odds (N:1, so payout multiplier is N)
const ROULETTE_PAYOUT_ODDS = {
     S: 35, // Straight up (pays 35:1 -> 36x stake back)
     R: 1, B: 1, // Red/Black (pays 1:1 -> 2x stake back)
     E: 1, O: 1, // Even/Odd (pays 1:1 -> 2x stake back)
     L: 1, H: 1, // Low (1-18)/High (19-36) (pays 1:1 -> 2x stake back)
     D1: 2, D2: 2, D3: 2, // Dozens (pays 2:1 -> 3x stake back)
     C1: 2, C2: 2, C3: 2, // Columns (pays 2:1 -> 3x stake back)
};

async function handleRouletteGame(bet) {
    const { id: betId, user_id, chat_id, bet_details, expected_lamports, memo_id } = bet;
    const config = GAME_CONFIG.roulette;
    const logPrefix = `Roulette Bet ${betId} (${memo_id.slice(0, 6)}...)`;
    const userBets = bet_details.bets; // e.g., { 'R': '10000000', 'S17': '5000000' }

    // --- Spin the Wheel (European: 0-36) ---
    const winningNumber = Math.floor(Math.random() * 37); // 0 to 36 inclusive
    const winningInfo = ROULETTE_NUMBERS[winningNumber];
    const winningColorEmoji = winningInfo.color === 'red' ? '🔴' : winningInfo.color === 'black' ? '⚫️' : '🟢'; // Use green for 0

    // console.log(`${logPrefix}: Spin result: ${winningNumber} (${winningInfo.color})`); // Reduce noise

    // --- Calculate Winnings ---
    let totalGrossWinningsLamports = 0n;
    let winningBetDescriptions = [];

    for (const betKey in userBets) { // betKey is 'R', 'S17', 'D1' etc.
        const betAmountLamports = BigInt(userBets[betKey]); // Amount is stored as string
        if (betAmountLamports <= 0n) continue; // Skip potentially invalid 0 amount bets

        let betWins = false;
        let payoutOdds = 0;

        // Determine payout odds based on betKey
        const betTypeCode = betKey.charAt(0); // S, R, B, D, C etc.
        // Handle S<number> type first
        if (betTypeCode === 'S') {
            payoutOdds = ROULETTE_PAYOUT_ODDS[betTypeCode] ?? 0; // Odds for Straight up
        } else if (betTypeCode === 'D' || betTypeCode === 'C') {
             payoutOdds = ROULETTE_PAYOUT_ODDS[betKey] ?? 0; // Lookup D1, C2 etc. directly
        } else {
             payoutOdds = ROULETTE_PAYOUT_ODDS[betTypeCode] ?? 0; // Lookup R, B, E, O, L, H
        }

        // Determine if the bet wins based on winningNumber and betKey
        const betValue = betKey.length > 1 ? betKey.substring(1) : undefined;

        switch (betTypeCode) {
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
            case 'D': // Dozen (D1, D2, D3)
                 const dozen = parseInt(betValue, 10);
                 if (winningInfo.dozen === dozen) betWins = true;
                 break;
            case 'C': // Column (C1, C2, C3)
                 const column = parseInt(betValue, 10);
                 if (winningInfo.column === column) betWins = true;
                 break;
            default:
                 console.warn(`${logPrefix}: Unknown bet type key "${betKey}" during payout calc.`);
                 break;
        }

        if (betWins) {
            // Calculate Gross Win for this specific bet: Stake * (Odds + 1)
            const grossWinForBet = betAmountLamports * BigInt(payoutOdds + 1);
            totalGrossWinningsLamports += grossWinForBet;
            // Add description for the user message - *** CORRECTED THIS LINE ***
            const winAmountSOL = (Number(grossWinForBet) / LAMPORTS_PER_SOL).toFixed(Math.min(6, Math.max(2, (Number(grossWinForBet) / LAMPORTS_PER_SOL).toString().split('.')[1]?.length || 2)));
            winningBetDescriptions.push(`\`${betKey}\` \\(\\+${escapeMarkdownV2(winAmountSOL)} SOL\\)`);
        }
    }

    // --- Calculate Net Payout (Apply House Edge to Total Gross Winnings) ---
    const payoutLamports = calculateNetPayout(totalGrossWinningsLamports, 'roulette');
    const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
    const win = payoutLamports > 0n;
    const displayName = await getUserDisplayName(chat_id, user_id);

    // --- Send Result Message ---
    let resultMessage = `⚪️ *Roulette Result* for ${displayName} \\!\n\n` +
                        `*Winning Number:* ${winningColorEmoji} *${escapeMarkdownV2(winningNumber)}* \\(${escapeMarkdownV2(winningInfo.color)}\\)\n\n`;

    if (win) {
        resultMessage += `🎉 *You won\\!* Total Payout: ${escapeMarkdownV2(payoutSOL.toFixed(6))} SOL\n`;
        if (winningBetDescriptions.length > 0) {
            resultMessage += `Winning Bets: ${winningBetDescriptions.join(', ')}\n`;
        }
        // console.log(`${logPrefix}: ${displayName} WON ${payoutSOL.toFixed(6)} SOL! Winning No: ${winningNumber}`);
    } else {
        resultMessage += `❌ No winning bets this time\\. Better luck next spin\\!`;
        // console.log(`${logPrefix}: ${displayName} LOST. Winning No: ${winningNumber}`);
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
                console.error(`${logPrefix}: CRITICAL! Failed to update status from 'processing_game' to 'processing_payout' before queueing! Aborting payout queue.`);
                await safeSendMessage(chatId, `⚠️ Internal error preparing your payout for bet \`${escapeMarkdownV2(memo_id)}\`\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
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
                gameType: 'coinflip', // Use main payout key (shared wallet)
                priority: 1, // Normal priority
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


// --- Casino War Game Logic --- (NEW)
async function handleWarGame(bet) {
    const { id: betId, user_id, chat_id, expected_lamports, memo_id } = bet;
    const config = GAME_CONFIG.war;
    const logPrefix = `War Bet ${betId} (${memo_id.slice(0, 6)}...)`;

    // Simulate dealing cards (Ace = 14)
    const cardValues = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]; // J=11, Q=12, K=13, A=14
    const playerCardRank = cardValues[Math.floor(Math.random() * cardValues.length)];
    const dealerCardRank = cardValues[Math.floor(Math.random() * cardValues.length)];

    function cardRankToString(rank) {
        if (rank <= 10) return rank.toString();
        if (rank === 11) return 'J';
        if (rank === 12) return 'Q';
        if (rank === 13) return 'K';
        if (rank === 14) return 'A';
        return '?';
    }
    const playerCardStr = cardRankToString(playerCardRank);
    const dealerCardStr = cardRankToString(dealerCardRank);

    // Determine outcome & gross winnings
    let outcome = '';
    let grossWinningsLamports = 0n;
    let playerWins = false;
    let isPush = false;

    if (playerCardRank > dealerCardRank) {
        outcome = 'win';
        playerWins = true;
        grossWinningsLamports = BigInt(expected_lamports) * 2n; // Win 1:1 (stake back + win amount)
    } else if (dealerCardRank > playerCardRank) {
        outcome = 'loss';
        grossWinningsLamports = 0n;
    } else {
        outcome = 'push'; // Tie is a Push
        isPush = true;
        grossWinningsLamports = BigInt(expected_lamports); // Get stake back
    }

    // Calculate Net Payout (apply house edge only if win, not on push)
    const payoutLamports = playerWins ? calculateNetPayout(grossWinningsLamports, 'war') : grossWinningsLamports;
    const winOrPushRequiresPayout = (playerWins || isPush) && payoutLamports > 0n;

    const payoutSOL = (Number(payoutLamports) / LAMPORTS_PER_SOL).toFixed(6);
    const displayName = await getUserDisplayName(chat_id, user_id);

    // --- Send Result Message ---
    let resultMessage = `🃏 *Casino War Result* for ${displayName} \\!\n\n` + // Escaped !
                        `Player Card: *${escapeMarkdownV2(playerCardStr)}*\n` +
                        `Dealer Card: *${escapeMarkdownV2(dealerCardStr)}*\n\n`;

    if (playerWins) {
         resultMessage += `🎉 *You Win\\!* Payout: ${escapeMarkdownV2(payoutSOL)} SOL`; // Escaped !
    } else if (isPush) {
         resultMessage += `🤝 *Push \\(Tie\\)!* Bet returned: ${escapeMarkdownV2(payoutSOL)} SOL`; // Escaped ()!
    } else { // Loss
         resultMessage += `❌ *Dealer Wins\\!* Better luck next time\\.`; // Escaped !.
    }
    await safeSendMessage(chat_id, resultMessage, { parse_mode: 'MarkdownV2' });

    // --- Handle Payout or Update Status ---
    if (winOrPushRequiresPayout) {
        const winnerAddress = await getLinkedWallet(user_id);
        if (!winnerAddress) {
            await updateBetStatus(betId, 'completed_win_no_wallet'); // Use generic status
            // Escaped .
            await safeSendMessage(chat_id, `Your ${outcome === 'win' ? 'winnings' : 'returned bet'} of ${escapeMarkdownV2(payoutSOL)} SOL is waiting\\. Place another bet to link your wallet\\.`, { parse_mode: 'MarkdownV2' });
            return; // Exit early, no payout job needed yet
        }
        // Wallet is linked, proceed to payout
        try {
             const statusUpdated = await updateBetStatus(betId, 'processing_payout');
             if (!statusUpdated) {
                console.error(`${logPrefix}: CRITICAL! Failed to update status from 'processing_game' to 'processing_payout' before queueing! Aborting payout queue.`);
                 // Escaped `.`
                await safeSendMessage(chatId, `⚠️ Internal error preparing your payout for bet \`${escapeMarkdownV2(memo_id)}\`\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
                await updateBetStatus(betId, 'error_payout_status_update'); // Mark with specific error
                return;
             }

             // Queue the payout job
             await paymentProcessor.addPaymentJob({
                 type: 'payout',
                 betId: betId,
                 recipient: winnerAddress,
                 amount: payoutLamports.toString(),
                 gameType: 'coinflip', // War uses the same payout key as coinflip/slots/roulette
                 priority: 1,
                 chatId: chat_id,
                 displayName: displayName,
                 memoId: memo_id,
             });
        } catch (e) {
             console.error(`${logPrefix}: Error preparing/queueing war payout info:`, e);
             await updateBetStatus(betId, 'error_payout_preparation');
              // Escaped `.`
             await safeSendMessage(chatId, `⚠️ Error occurred while processing your war win/push for bet \`${escapeMarkdownV2(memo_id)}\`\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
        }
    } else if (outcome === 'loss') {
         await updateBetStatus(betId, 'completed_loss');
    } else {
         // Case where outcome was push or win but payout is 0 (e.g., HE applied)
         console.warn(`${logPrefix}: Outcome was ${outcome} but payoutLamports is ${payoutLamports}. Marking as completed.`);
         // Mark as completed_loss if player didn't get money back, otherwise completed_push
         await updateBetStatus(betId, isPush ? 'completed_push_zero_payout' : 'completed_loss');
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
              // Escaped `().`
             await safeSendMessage(chatId, `⚠️ There was an issue calculating the payout for bet \`${escapeMarkdownV2(memoId)}\` \\(amount was zero\\)\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
             return; // Exit function, job is considered "done" (failed)
         }
    } catch (e) {
        console.error(`${logPrefix}: ❌ Invalid payout amount format received in job: '${amount}'. Error: ${e.message}`);
        await updateBetStatus(betId, 'error_payout_invalid_amount');
         // Escaped `().`
        await safeSendMessage(chatId, `⚠️ Technical error processing payout for bet \`${escapeMarkdownV2(memoId)}\` \\(invalid amount\\)\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
        return; // Exit function
    }

    // console.log(`${logPrefix}: Processing payout of ${Number(payoutAmountLamports) / LAMPORTS_PER_SOL} SOL to ${recipient}`); // Reduce noise

    // Determine which payout key to use ('coinflip' uses main, 'race' uses race key)
    // War uses 'coinflip' key as determined by handleWarGame when queuing the job
    const payoutWalletType = (gameType === 'race') ? 'race' : 'coinflip';

    // Call sendSol to perform the transaction
    try {
        const sendResult = await sendSol(recipient, payoutAmountLamports, payoutWalletType);

        // sendSol now throws error on failure, so we only reach here on success
        if (sendResult.success && sendResult.signature) {
            // console.log(`${logPrefix}: ✅ Payout successful! TX: ${sendResult.signature.slice(0, 10)}...`); // Reduce noise
            // Attempt to record the payout in the database
            const recorded = await recordPayout(betId, 'completed_win_paid', sendResult.signature);
            if (recorded) {
                // Successfully sent and recorded
                const payoutAmountSOLString = (Number(payoutAmountLamports)/LAMPORTS_PER_SOL).toFixed(6);
                const escapedAmount = escapeMarkdownV2(payoutAmountSOLString);

                // Escaped `!.`
                await safeSendMessage(chatId,
                    `✅ Payout successful for bet \`${escapeMarkdownV2(memoId)}\`\\!\n` +
                    `${escapedAmount} SOL sent\\.\n` +
                    `TX: \`https://solscan.io/tx/${sendResult.signature}\``,
                    { parse_mode: 'MarkdownV2', disable_web_page_preview: true }
                );
                 console.log(`[PAYOUT_JOB_SUCCESS] Bet ${job.betId} payout logged successfully.`);
                 return; // Explicitly return to signal success completion of this attempt
            } else {
                // Sent successfully BUT failed to record in DB - this is critical!
                console.error(`${logPrefix}: 🆘 CRITICAL! Payout sent (TX: ${sendResult.signature}) but FAILED to record in DB! Requires manual investigation.`);
                // Update status to a specific error state if possible
                await updateBetStatus(betId, 'error_payout_record_failed');
                // Notify user about the issue
                // Escaped `..`
                await safeSendMessage(chatId,
                    `⚠️ Your payout for bet \`${escapeMarkdownV2(memo_id)}\` was sent successfully, but there was an issue recording it\\. Please contact support and provide this TX ID\\.\n` +
                    `TX: \`https://solscan.io/tx/${sendResult.signature}\``,
                    { parse_mode: 'MarkdownV2', disable_web_page_preview: true }
                );
                return; // Signal completion of attempt, even though recording failed
            }
        } else {
             // This block should technically not be reached if sendSol throws on error
             console.error(`${logPrefix}: ❌ Payout failed via sendSol but did not throw error? Result:`, sendResult);
             throw new Error(sendResult.error || 'sendSol failed without throwing');
        }
     } catch (error) {
        // This block catches errors thrown by sendSol or potentially other issues
         console.error(`${logPrefix}: ❌ Payout failed. Error caught in handlePayoutJob: ${error.message}`);
         // Re-throw the error. The processJob retry logic will catch it
         // and determine if it's retryable based on isRetryableError(error).
         throw error; // Rethrow original or classified error from sendSol
     }
    // The catch block in processJob handles status updates for final failed retries/non-retryable errors.
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
    } else {
         // Log other polling errors but don't necessarily exit
         console.error(`Unhandled Polling Error: Code ${error.code}, Status ${error.response?.statusCode}`);
    }
});

bot.on('webhook_error', (error) => {
    console.error(`❌ Webhook error: ${error.code} - ${error.message}`);
    // Common webhook errors might include network issues, certificate problems, or Telegram issues.
     if (error.message.includes('ETIMEDOUT') || error.message.includes('ECONNRESET')) {
          console.warn("Webhook connection issue detected.");
     } else {
          console.error(`Unhandled Webhook Error: Code ${error.code}`);
     }
});

bot.on('error', (error) => { // General errors from the library
    console.error('❌ General Bot Error:', error);
    performanceMonitor.logRequest(false);
    // You might want to add specific checks here, e.g., for rate limit errors if not caught elsewhere
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
                     // console.log(`User ${userId} ignored due to cooldown.`); // Reduce noise
                    return; // Ignore command during cooldown
                }
            }
            confirmCooldown.set(userId, now); // Apply/reset cooldown only on command attempt
        }

        // Command Routing
        // Allow commands with or without bot username (e.g., /start or /start@MyBot)
        const commandMatch = messageText.trim().match(/^\/(\w+)(?:@\w+)?(?:\s+(.*))?$/s);
        if (!commandMatch) return; // Not a command

        const command = commandMatch[1].toLowerCase();
        const args = commandMatch[2]?.trim() || ''; // Arguments string

        // --- Command Handler Map --- Updated for War & /betcf ---
        const commandHandlers = {
            'start': handleStartCommand,
            'war': handleWarInfoCommand,         // NEW: Info command for War
            'coinflip': handleCoinflipCommand,
            'betcf': (msg, args) => handleBetCommand(msg, args), // RENAMED Coinflip bet (uses old handler function)
            'race': handleRaceCommand,
            'betrace': (msg, args) => handleBetRaceCommand(msg, args),
            'slots': handleSlotsCommand,
            'betslots': (msg, args) => handleBetSlotsCommand(msg, args),
            'roulette': handleRouletteCommand,
            'betroulette': (msg, args) => handleBetRouletteCommand(msg, args),
            'betwar': (msg, args) => handleBetWarCommand(msg, args), // NEW: Explicit command for War bet
            // Generic 'bet' command is removed
            'wallet': handleWalletCommand,
            'help': handleHelpCommand,
            'botstats': handleBotStatsCommand, // Admin
        };


        const handler = commandHandlers[command];

        if (handler) {
            // Admin Check for specific commands
            if (command === 'botstats') {
                const adminIds = (process.env.ADMIN_USER_IDS || '').split(',').map(id=>id.trim()).filter(id => id);
                if (adminIds.includes(userId)) {
                    await handler(msg); // Admin commands might only expect msg
                } else {
                     console.log(`User ${userId} attempted unauthorized /${command} command.`);
                     // Silently ignore or send a generic "Unknown command" message
                     // await safeSendMessage(chatId, "Unknown command.", { parse_mode: 'MarkdownV2' });
                }
            } else {
                 // Regular command execution
                 if (typeof handler === 'function') {
                     // Check handler arity (number of expected arguments) to pass args correctly
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
             // await safeSendMessage(chatId, `Unknown command: \`/${command}\``, { parse_mode: 'MarkdownV2'});
        }

    } catch (error) {
        // Catch errors thrown by command handlers or routing logic
        console.error(`❌ Error processing msg ${messageId} from user ${userId} ("${messageText}"):`, error);
        performanceMonitor.logRequest(false);
        try {
            // Send a generic error message to the user - Escaped .
            await safeSendMessage(chatId, "⚠️ An unexpected error occurred processing your request\\. Please try again later or contact support if the issue persists\\.", { parse_mode: 'MarkdownV2'});
        } catch (tgError) { /* safeSendMessage already logs its own errors */ }
    } finally {
        // Optional: Cooldown map cleanup (runs occasionally)
         if (Math.random() < 0.05) { // Run roughly 5% of the time
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
    // Basic sanitation for HTML
    const sanitizedFirstName = firstName.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    const welcomeText = `👋 Welcome, <b>${sanitizedFirstName}</b>!\n\n` +
                         `🎰 <b>Solana Gambles Bot</b>\n\n` +
                         `Use the commands below to play:\n` +
                         `/coinflip - Simple Heads/Tails\n` +
                         `/race - Bet on Horse Races\n` +
                         `/slots - Play the Slot Machine\n` +
                         `/roulette - Play European Roulette\n` +
                         `/war - Play Casino War (Tie is Push)\n\n` + // Added War
                         `/wallet - View/Link your Solana wallet\n` +
                         `/help - See all commands & rules\n\n` +
                         `<i>Remember to gamble responsibly!</i>`;
    const bannerUrl = 'https://i.ibb.co/9vDo58q/banner.gif'; // Keep existing banner

    try {
        // Try sending animation first
        await bot.sendAnimation(msg.chat.id, bannerUrl, {
            caption: welcomeText,
            parse_mode: 'HTML'
        }).catch(async (animError) => {
             // Fallback to text message if animation fails
             console.warn("⚠️ Failed to send start animation, sending text fallback:", animError.message);
             await safeSendMessage(msg.chat.id, welcomeText, { parse_mode: 'HTML' });
        });
    } catch (fallbackError) { /* safeSendMessage handles its own logging */ }
}


// /coinflip command (HTML) - UPDATED FOR /betcf
async function handleCoinflipCommand(msg) {
    try {
        const config = GAME_CONFIG.coinflip;
        // Calculate payout multiplier considering house edge
        const payoutMultiplier = (2.0 * (1.0 - config.houseEdge)).toFixed(2);
        const messageText = `🪙 <b>Coinflip Game</b> 🪙\n\n` +
             `Bet on Heads or Tails! Simple and quick.\n\n` +
             `<b>How to play:</b>\n` +
             `1. Type <code>/betcf amount heads</code> (e.g., <code>/betcf 0.1 heads</code>)\n` + // UPDATED
             `2. Type <code>/betcf amount tails</code> (e.g., <code>/betcf 0.1 tails</code>)\n\n` + // UPDATED
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
        await safeSendMessage(msg.chat.id, "Sorry, couldn't display Coinflip info right now."); // Generic error message
    }
}

// /race command (MarkdownV2)
async function handleRaceCommand(msg) {
    const horses = [ // Keep consistent definition
         { name: 'Yellow', emoji: '🟡', odds: 1.1 }, { name: 'Orange', emoji: '🟠', odds: 2.0 }, { name: 'Blue', emoji: '🔵', odds: 3.0 }, { name: 'Cyan', emoji: '💧', odds: 4.0 },
         { name: 'White', emoji: '⚪️', odds: 5.0 }, { name: 'Red', emoji: '🔴', odds: 6.0 }, { name: 'Black', emoji: '⚫️', odds: 7.0 }, { name: 'Pink', emoji: '🌸', odds: 8.0 },
         { name: 'Purple', emoji: '🟣', odds: 9.0 }, { name: 'Green', emoji: '🟢', odds: 10.0 }, { name: 'Silver', emoji: '💎', odds: 15.0 }
    ];
    let raceMessage = `🐎 *Horse Race Game* 🐎\n\nBet on the winning horse\\!\n\n*Available Horses \\& Approx Payout Multiplier* \\(After House Edge\\):\n`;
    horses.forEach(horse => {
        // Payout is Stake * Odds * (1 - HouseEdge)
        const effectiveMultiplier = (horse.odds * (1.0 - GAME_CONFIG.race.houseEdge)).toFixed(2);
        raceMessage += `\\- ${horse.emoji} *${escapeMarkdownV2(horse.name)}* \\(\\~${escapeMarkdownV2(effectiveMultiplier)}x Payout\\)\n`; // Escape -()~
    });
    const config = GAME_CONFIG.race;
    raceMessage += `\n*How to play:*\n` +
                     `1\\. Type \`/betrace amount horse_name\`\n` + // Escape .
                     `  \\(e\\.g\\., \`/betrace 0\\.1 Yellow\`\\)\n\n` + // Escape .()
                     `*Rules:*\n` +
                     `\\- Min Bet: ${escapeMarkdownV2(config.minBet)} SOL\n` + // Escape -
                     `\\- Max Bet: ${escapeMarkdownV2(config.maxBet)} SOL\n` + // Escape -
                     `\\- House Edge: ${escapeMarkdownV2((config.houseEdge * 100).toFixed(1))}% \\(applied to gross winnings\\)\n\n` + // Escape -.%()
                     `You will be given a wallet address and a *unique Memo ID*\\. Send the *exact* SOL amount with the memo to place your bet\\.`; // Escape .
    await safeSendMessage(msg.chat.id, raceMessage, { parse_mode: 'MarkdownV2' });
}

// /slots command (MarkdownV2)
async function handleSlotsCommand(msg) {
    const config = GAME_CONFIG.slots;
    const paylines = [
         `🍒 Cherry \\| 🍒 Cherry \\| \\(Any\\) \\= ${escapeMarkdownV2(SLOTS_SYMBOLS.CHERRY.payout[2])}\\:1`,
         `7️⃣ Seven \\| \\(Any\\) \\| \\(Any\\) \\= 4\\:1`,
         `🍒 Cherry \\| 🍒 Cherry \\| 🍒 Cherry \\= ${escapeMarkdownV2(SLOTS_SYMBOLS.CHERRY.payout[3])}\\:1`,
         `🍊 Orange \\| 🍊 Orange \\| 🍊 Orange \\= ${escapeMarkdownV2(SLOTS_SYMBOLS.ORANGE.payout[3])}\\:1`,
         `🍫 BAR \\| 🍫 BAR \\| 🍫 BAR \\= ${escapeMarkdownV2(SLOTS_SYMBOLS.BAR.payout[3])}\\:1`,
         `🎰 777 \\| 🎰 777 \\| 🎰 777 \\= ${escapeMarkdownV2(SLOTS_SYMBOLS.TRIPLE_SEVEN.payout[3])}\\:1`
    ];

    const message = `🎰 *777 Slots Game* 🎰\n\n` +
                    `Spin the 3 reels and match symbols on the center line\\!\n\n`+
                    `*Symbols:*\n`+
                    `🍒 Cherry, 🍊 Orange, 🍫 BAR, 7️⃣ Seven, 🎰 777, ➖ Blank\n\n` +
                    `*Payouts \\(Odds Ratio \\- Win Amount / Bet Amount\\):*\n` + // Escaped hyphen
                    paylines.map(line => `\\- ${line}`).join('\n') + `\n\n` + // List hyphens escaped
                    `*How to Play:*\n` +
                    `\\- Type \`/betslots amount\` \\(e\\.g\\., \`/betslots 0\\.05\`\\)\n\n` +
                    `*Rules:*\n` +
                    `\\- Min Bet: ${escapeMarkdownV2(config.minBet)} SOL\n` +
                    `\\- Max Bet: ${escapeMarkdownV2(config.maxBet)} SOL\n` +
                    `\\- House Edge: ${escapeMarkdownV2((config.houseEdge * 100).toFixed(1))}% \\(applied to gross winnings\\)\n\n`+
                    `You will be given a wallet address and a *unique Memo ID*\\. Send the *exact* SOL amount with the memo to spin\\.`;

    await safeSendMessage(msg.chat.id, message, { parse_mode: 'MarkdownV2' });
}

// /roulette command (MarkdownV2)
async function handleRouletteCommand(msg) {
    const config = GAME_CONFIG.roulette;
    const message = `⚪️ *European Roulette Game* ⚪️

Place bets on the outcome of the wheel spin \\(numbers 0\\-36\\)\\.

Bet Types & Payouts \\(Odds N:1\\) \\- Payout is Stake * \\(N\\+1\\)*:
\\- *Straight* \\(\`S<number>\`, e\\.g\\. \`S17\`\\): 35:1
\\- *Red* \\(\`R\`\\) \\/ *Black* \\(\`B\`\\): 1:1
\\- *Even* \\(\`E\`\\) \\/ *Odd* \\(\`O\`\\): 1:1
\\- *Low* \\(\`L\`, numbers 1\\-18\\) \\/ *High* \\(\`H\`, numbers 19\\-36\\): 1:1
\\- *Dozens* \\(e\\.g\\. \`D1\` for 1\\-12, \`D2\` for 13\\-24, \`D3\` for 25\\-36\\): 2:1
\\- *Columns* \\(e\\.g\\. \`C1\` for 1,4,7\\.\\.\\.34\\): 2:1

*How to Play*:
\\- Type \`/betroulette <amount> <bet\\_spec>\`
   *\\(e\\.g\\., \`/betroulette 0.1 R\`\\)*
   *\\(e\\.g\\., \`/betroulette 0.05 S17\`\\)*
   *\\(e\\.g\\., \`/betroulette 0.2 D1\`\\)*

*Rules*:
\\- Min Bet \\(per placement\\): ${escapeMarkdownV2(config.minBet)} SOL
\\- Max Bet \\(per placement\\): ${escapeMarkdownV2(config.maxBet)} SOL
\\- House Edge: ${escapeMarkdownV2((config.houseEdge * 100).toFixed(1))}% \\(applied to gross winnings\\)

You will be given a wallet address and a *unique Memo ID* for each bet placement\\. Send the *exact* SOL amount with the memo\\.`;

    await safeSendMessage(msg.chat.id, message, { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}

// /war command (MarkdownV2) - NEW
async function handleWarInfoCommand(msg) {
    const config = GAME_CONFIG.war;
    const message = `🃏 *Casino War Game* 🃏

    Place your bet\\. You and the dealer each get one card\\. Highest card wins \\(Ace high\\)!

    *Rules:*
    \\- If your card is higher, you win 1:1 \\(double your bet back\\)\\.
    \\- If the dealer's card is higher, you lose your bet\\.
    \\- If cards *Tie*, it's a *Push* \\- your bet is returned to you\\.

    *How to Play:*
    \\- Type \`/betwar <amount>\` \\(e\\.g\\., \`/betwar 0\\.1\`\\)

    *Limits:*
    \\- Min Bet: ${escapeMarkdownV2(config.minBet)} SOL
    \\- Max Bet: ${escapeMarkdownV2(config.maxBet)} SOL
    \\- House Edge: ${escapeMarkdownV2((config.houseEdge * 100).toFixed(1))}% \\(Applied to wins only\\)

    You will be given a wallet address and a *unique Memo ID* \\(\`WA-...\`\\)\\. Send the *exact* SOL amount with the memo to play\\.`;
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


// /betcf command (Coinflip - MarkdownV2) - Formerly /bet handler
async function handleBetCommand(msg, args) { // Function name kept, but triggered by /betcf
     const match = args.trim().match(/^(\d+\.?\d*)\s+(heads|tails)/i);
     if (!match) {
         await safeSendMessage(msg.chat.id,
              `⚠️ Invalid format\\. Use: \`/betcf <amount> <heads|tails>\`\n` + // Updated command name
              `Example: \`/betcf 0\\.1 heads\``, // Updated command name
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
         const errorMessage = saveResult.error || 'Unknown error saving bet';
         await safeSendMessage(chatId, `⚠️ Error registering bet: ${escapeMarkdownV2(errorMessage)}\\. Please try the command again\\.`, { parse_mode: 'MarkdownV2' });
         return;
     }

     const betAmountString = escapeMarkdownV2(betAmount.toFixed(Math.max(2, (betAmount.toString().split('.')[1] || '').length)));
     const depositAddress = process.env.MAIN_WALLET_ADDRESS; // Coinflip uses main wallet

     if (!depositAddress) {
          console.error("CRITICAL: MAIN_WALLET_ADDRESS environment variable is not set!");
          await safeSendMessage(chatId, `⚠️ Bot configuration error: Deposit address not set\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
          return;
     }

     const message = `✅ Coinflip bet registered\\! \\(ID: \`${memoId}\`\\)\n\n` +
                     `You chose: *${escapeMarkdownV2(userChoice)}*\n` +
                     `Amount: *${betAmountString} SOL*\n\n` +
                     `➡️ Send *exactly ${betAmountString} SOL* to:\n` +
                     `\`${escapeMarkdownV2(depositAddress)}\`\n\n` +
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

     // Calculate potential payout for display
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
          const errorMessage = saveResult.error || 'Unknown error saving bet';
         await safeSendMessage(chatId, `⚠️ Error registering bet: ${escapeMarkdownV2(errorMessage)}\\. Please try the command again\\.`, { parse_mode: 'MarkdownV2' });
         return;
     }

     const depositAddress = process.env.RACE_WALLET_ADDRESS; // Race uses specific wallet

     if (!depositAddress) {
          console.error("CRITICAL: RACE_WALLET_ADDRESS environment variable is not set!");
          await safeSendMessage(chatId, `⚠️ Bot configuration error: Race deposit address not set\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
          return;
     }

     const message = `✅ Race bet registered\\! \\(ID: \`${memoId}\`\\)\n\n` +
                     `You chose: ${chosenHorse.emoji} *${escapeMarkdownV2(chosenHorse.name)}*\n` +
                     `Amount: *${betAmountString} SOL*\n` +
                     `Potential Payout: \\~${potentialPayoutSOL} SOL \\(after house edge\\)\n\n`+
                     `➡️ Send *exactly ${betAmountString} SOL* to:\n` +
                     `\`${escapeMarkdownV2(depositAddress)}\`\n\n` +
                     `📎 *Include MEMO:* \`${memoId}\`\n\n` +
                     `⏱️ This request expires in ${config.expiryMinutes} minutes\\.\n\n` +
                     `*IMPORTANT:* Send from your own wallet \\(not an exchange\\)\\. Ensure you include the memo correctly\\.`;

     await safeSendMessage(chatId, message, { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}

// /betslots command (MarkdownV2)
async function handleBetSlotsCommand(msg, args) {
    const match = args.trim().match(/^(\d+\.?\d*)$/); // Only expects amount
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

    const betDetails = { betAmountSOL: betAmount }; // Store original amount if needed

    const saveResult = await savePendingBet(
        userId, chatId, 'slots', betDetails, expectedLamports, memoId, expiresAt
    );

    if (!saveResult.success) {
        const errorMessage = saveResult.error || 'Unknown error saving bet';
        await safeSendMessage(chatId, `⚠️ Error registering bet: ${escapeMarkdownV2(errorMessage)}\\. Please try the command again\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    const betAmountString = escapeMarkdownV2(betAmount.toFixed(Math.max(2, (betAmount.toString().split('.')[1] || '').length)));
    const depositAddress = process.env.MAIN_WALLET_ADDRESS; // Slots uses main wallet

    if (!depositAddress) {
         console.error("CRITICAL: MAIN_WALLET_ADDRESS environment variable is not set!");
         await safeSendMessage(chatId, `⚠️ Bot configuration error: Deposit address not set\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
         return;
    }

    const message = `✅ Slots bet registered\\! \\(ID: \`${memoId}\`\\)\n\n` +
                    `Spin Amount: *${betAmountString} SOL*\n\n` +
                    `➡️ Send *exactly ${betAmountString} SOL* to:\n` +
                    `\`${escapeMarkdownV2(depositAddress)}\` \\(Shared Deposit Address\\)\n\n` +
                    `📎 *Include MEMO:* \`${memoId}\`\n\n` +
                    `⏱️ This request expires in ${config.expiryMinutes} minutes\\.\n\n` +
                    `*IMPORTANT:* Send from your own wallet \\(not an exchange\\)\\. Ensure you include the memo correctly\\.`;

    await safeSendMessage(chatId, message, { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}


// /betroulette command (MarkdownV2)
async function handleBetRouletteCommand(msg, args) {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const config = GAME_CONFIG.roulette;

    const match = args.trim().match(/^(\d+\.?\d*)\s+(.+)$/i); // Match amount and the rest (bet spec)

    if (!match) {
        await safeSendMessage(chatId,
            `⚠️ Invalid format\\. Use: \`/betroulette <amount> <bet_spec>\`\n` +
            `Bet Spec Codes: \`R\`, \`B\`, \`E\`, \`O\`, \`L\`, \`H\`, \`D1\`/\`D2\`/\`D3\`, \`C1\`/\`C2\`/\`C3\`, \`S<0-36>\`\n`+
            `Examples: \`/betroulette 0.1 R\` \\| \`/betroulette 0.02 S17\` \\| \`/betroulette 0.2 D1\``,
            { parse_mode: 'MarkdownV2', disable_web_page_preview: true }
        );
        return;
    }

    const betAmount = parseFloat(match[1]);
    const betSpec = match[2].toUpperCase(); // e.g., "R", "S35", "D1"

     if (isNaN(betAmount) || betAmount <= 0) {
        await safeSendMessage(chatId, `⚠️ Invalid bet amount specified: "${escapeMarkdownV2(match[1] || args)}"\\. Amount must be positive\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    if (betAmount < config.minBet || betAmount > config.maxBet) {
        await safeSendMessage(chatId,
            `⚠️ Bet amount out of range for a single placement \\(${escapeMarkdownV2(config.minBet)} \\- ${escapeMarkdownV2(config.maxBet)} SOL\\)\\.`,
            { parse_mode: 'MarkdownV2' }
        );
        return;
    }

    let betKey = '';    // The final validated key, e.g., 'R', 'S17', 'D1'
    let betType = '';   // The category, e.g., 'R', 'S', 'D'
    let betValue = undefined;

    // Parse the betSpec string
    if (/^(R|B|E|O|L|H)<span class="math-inline">/\.test\(betSpec\)\) \{
betKey \= betSpec; betType \= betSpec;
\} else if \(/^D\(\[1\-3\]\)</span>/.test(betSpec)) {
        betKey = betSpec; betType = 'D'; betValue = betSpec.substring(1);
    } else if (/^C([1-3])<span class="math-inline">/\.test\(betSpec\)\) \{
betKey \= betSpec; betType \= 'C'; betValue \= betSpec\.substring\(1\);
\} else if \(/^S\(0\|\[1\-9\]\|\[12\]\\d\|3\[0\-6\]\)</span>/.test(betSpec)) { // Regex ensures number is 0-36
        betKey = betSpec; betType = 'S'; betValue = betSpec.substring(1);
    } else {
        await safeSendMessage(chatId,
            `⚠️ Invalid Bet Specification: \`${escapeMarkdownV2(betSpec)}\`\\.\n` +
            `Use codes like \`R\`, \`B\`, \`E\`, \`O\`, \`L\`, \`H\`, \`D1\`/\`D2\`/\`D3\`, \`C1\`/\`C2\`/\`C3\`, \`S<0-36>\`\\. See /roulette\\.`,
            { parse_mode: 'MarkdownV2', disable_web_page_preview: true }
        );
        return;
    }

    const memoId = generateMemoId('RL'); // Roulette memo prefix
    const expectedLamports = BigInt(Math.round(betAmount * LAMPORTS_PER_SOL));
    const expiresAt = new Date(Date.now() + config.expiryMinutes * 60 * 1000);

    const betDetails = {
        betAmountSOL: betAmount,
        bets: { // Store bets with lamports as string: { 'S17': '5000000' }
            [betKey]: expectedLamports.toString()
        }
    };

    const saveResult = await savePendingBet(
        userId, chatId, 'roulette', betDetails, expectedLamports, memoId, expiresAt
    );

    if (!saveResult.success) {
        const errorMessage = saveResult.error || 'Unknown error saving bet';
        await safeSendMessage(chatId, `⚠️ Error registering bet: ${escapeMarkdownV2(errorMessage)}\\. Please try the command again\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    const betAmountString = escapeMarkdownV2(betAmount.toFixed(Math.max(2, (betAmount.toString().split('.')[1] || '').length)));
    const depositAddress = process.env.MAIN_WALLET_ADDRESS; // Roulette uses main wallet

    if (!depositAddress) {
         console.error("CRITICAL: MAIN_WALLET_ADDRESS environment variable is not set!");
         await safeSendMessage(chatId, `⚠️ Bot configuration error: Deposit address not set\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
         return;
    }

    const message = `✅ Roulette bet registered\\! \\(ID: \`${memoId}\`\\)\n\n` +
                    `Bet Placed: *${escapeMarkdownV2(betKey)}*\n` +
                    `Amount: *${betAmountString} SOL*\n\n` +
                    `➡️ Send *exactly ${betAmountString} SOL* to:\n` +
                    `\`${escapeMarkdownV2(depositAddress)}\` \\(Shared Deposit Address\\)\n\n` +
                    `📎 *Include MEMO:* \`${memoId}\`\n\n` +
                    `⏱️ This request expires in ${config.expiryMinutes} minutes\\.\n\n` +
                    `*IMPORTANT:* Send from your own wallet \\(not an exchange\\)\\. Ensure you include the memo correctly\\.`;

    await safeSendMessage(chatId, message, { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}

// /betwar command (MarkdownV2) - NEW
async function handleBetWarCommand(msg, args) {
    const match = args.trim().match(/^(\d+\.?\d*)$/); // Only amount
    if (!match) {
        await safeSendMessage(msg.chat.id,
            `⚠️ Invalid format\\. Use: \`/betwar <amount>\`\n` + // Reference /betwar
            `Example: \`/betwar 0\\.1\``,
            { parse_mode: 'MarkdownV2' }
        );
        return;
    }

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const config = GAME_CONFIG.war; // Use war config

    const betAmount = parseFloat(match[1]);
    if (isNaN(betAmount) || betAmount < config.minBet || betAmount > config.maxBet) {
        await safeSendMessage(chatId,
            `⚠️ Invalid bet amount\\. Please bet between ${escapeMarkdownV2(config.minBet)} and ${escapeMarkdownV2(config.maxBet)} SOL for War\\.`,
            { parse_mode: 'MarkdownV2' }
        );
        return;
    }

    const memoId = generateMemoId('WA'); // Use 'WA' prefix
    const expectedLamports = BigInt(Math.round(betAmount * LAMPORTS_PER_SOL));
    const expiresAt = new Date(Date.now() + config.expiryMinutes * 60 * 1000);

    const saveResult = await savePendingBet(
        userId, chatId,
        'war', // Game Type
        {},    // Bet Details (empty)
        expectedLamports, memoId, expiresAt
    );

    if (!saveResult.success) {
        const errorMessage = saveResult.error || 'Unknown error saving bet';
        await safeSendMessage(chatId, `⚠️ Error registering War bet: ${escapeMarkdownV2(errorMessage)}\\. Please try again\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    const betAmountString = escapeMarkdownV2(betAmount.toFixed(Math.max(2, (betAmount.toString().split('.')[1] || '').length)));
    const depositAddress = process.env.MAIN_WALLET_ADDRESS; // Assuming War uses main wallet

    if (!depositAddress) {
         console.error("CRITICAL: MAIN_WALLET_ADDRESS environment variable is not set!");
         await safeSendMessage(chatId, `⚠️ Bot configuration error: Deposit address not set\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
         return;
    }

    const message = `✅ Casino War bet registered\\! \\(ID: \`${memoId}\`\\)\n\n` +
                    `Bet Amount: *${betAmountString} SOL*\n\n` +
                    `➡️ Send *exactly ${betAmountString} SOL* to:\n` +
                    `\`${escapeMarkdownV2(depositAddress)}\`\n\n` +
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
                     `/slots \\- Slots info \\& how to bet\n` +
                     `/roulette \\- Roulette info \\& how to bet\n` +
                     `/war \\- Casino War info \\& how to bet\n\n` + // Added War
                     `*Betting:*\n` +
                     `/betcf <amount> <heads|tails> \\- Place a Coinflip bet\n` + // Renamed
                     `/betrace <amount> <horse\\_name> \\- Place a Race bet\n` +
                     `/betslots <amount> \\- Place a Slots bet\n` +
                     `/betroulette <amount> <bet_spec> \\- Place a Roulette bet \\(see /roulette\\)\n` +
                     `/betwar <amount> \\- Place a Casino War bet\n\n` + // Added War bet
                     `*Wallet:*\n` +
                     `/wallet \\- View your linked Solana wallet for payouts\n\n` +
                     `*Support:* If you encounter issues, please contact support \\(details not provided here\\)\\.`;

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

    const messageQueueSize = messageQueue?.size || 0;
    const messageQueuePending = messageQueue?.pending || 0;
    const telegramSendQueueSize = telegramSendQueue?.size || 0;
    const telegramSendQueuePending = telegramSendQueue?.pending || 0;
    const paymentHighPriQueueSize = processor?.highPriorityQueue?.size || 0;
    const paymentHighPriQueuePending = processor?.highPriorityQueue?.pending || 0;
    const paymentNormalQueueSize = processor?.normalQueue?.size || 0;
    const paymentNormalQueuePending = processor?.normalQueue?.pending || 0;

    let statsMsg = `*Bot Statistics* \\(v${escapeMarkdownV2('2.4-war')}\\)\n\n`; // Updated version
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
        statsMsg += `  \\- Success Rate: ${escapeMarkdownV2(connectionStats.stats.successRate ? connectionStats.stats.successRate.toFixed(1) + '%' : 'N/A')}\n`; // Safe access
    } else {
        statsMsg += `*Solana Connection:* Stats unavailable\\.\n`;
    }

    // Add DB Pool Stats
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

// --- End of Part 3 ---
// index.js - Part 4 (Corrected - Final Version with War, /betcf, /betwar)

// (Code continues directly from the end of Part 3)

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
            const botVersion = process.env.npm_package_version || '2.4-war'; // Updated version string
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
