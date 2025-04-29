// index.js - Revised Part 1

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
import { toByteArray, fromByteArray } from 'base64-js'; // Keep if used by bs58/other libs indirectly, though Buffer is preferred
import { Buffer } from 'buffer'; // Standard Node.js Buffer
// --- End of Imports ---

// --- CORRECTED PLACEMENT for Deployment Check Log ---
console.log(`--- INDEX.JS - DEPLOYMENT CHECK --- ${new Date().toISOString()} ---`);
// --- END CORRECTED PLACEMENT ---


console.log("⏳ Starting Solana Gambles Bot (Multi-RPC)... Checking environment variables...");

// --- START: Enhanced Environment Variable Checks (MODIFIED for RPC_URLS) ---
const REQUIRED_ENV_VARS = [
    'BOT_TOKEN',
    'DATABASE_URL',
    'BOT_PRIVATE_KEY', // Coinflip payouts
    'RACE_BOT_PRIVATE_KEY', // Race payouts
    'MAIN_WALLET_ADDRESS', // Coinflip deposits
    'RACE_WALLET_ADDRESS', // Race deposits
    'RPC_URLS',          // Comma-separated list of RPC URLs
    'FEE_MARGIN',        // Lamports buffer for payout fees
    'PAYOUT_PRIORITY_FEE_RATE', // Optional: Rate for dynamic priority fee calculation (e.g., 0.0001 for 0.01%)
    'ADMIN_USER_IDS'     // Optional: Comma-separated Telegram user IDs for admin commands
    // Add other necessary env vars here (e.g., DB pool settings, game limits, etc.)
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
    // Skip optional vars if not present
    if (['PAYOUT_PRIORITY_FEE_RATE', 'ADMIN_USER_IDS'].includes(key) && !process.env[key]) return;

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

// Parse priority fee rate, default if missing/invalid
const PRIORITY_FEE_RATE = parseFloat(process.env.PAYOUT_PRIORITY_FEE_RATE || '0.0001');
if (isNaN(PRIORITY_FEE_RATE) || PRIORITY_FEE_RATE < 0) {
    console.warn(`⚠️ Invalid PAYOUT_PRIORITY_FEE_RATE, defaulting to 0.0001 (0.01%).`);
    process.env.PAYOUT_PRIORITY_FEE_RATE = '0.0001'; // Set validated default back
}
console.log(`ℹ️ Using PAYOUT_PRIORITY_FEE_RATE: ${process.env.PAYOUT_PRIORITY_FEE_RATE}`);
// --- END: Enhanced Environment Variable Checks ---


// --- Startup State Tracking ---
let isFullyInitialized = false; // Tracks completion of background initialization
let server; // Declare server variable for health check and graceful shutdown

// --- Initialize Scalable Components ---
const app = express();

// --- IMMEDIATE Health Check Endpoint ---
app.get('/health', (req, res) => {
  res.status(200).json({
    status: server ? 'ready' : 'starting', // Report 'ready' once server object exists, otherwise 'starting'
    uptime: process.uptime()
  });
});

// --- Railway-Specific Health Check Endpoint ---
app.get('/railway-health', (req, res) => {
    res.status(200).json({
        status: isFullyInitialized ? 'ready' : 'starting',
        version: '2.2-revised' // Update version string
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

// Parse RPC URLs
const rpcUrls = process.env.RPC_URLS.split(',')
    .map(url => url.trim())
    .filter(url => url.length > 0 && (url.startsWith('http://') || url.startsWith('https://'))); // Basic URL validation

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
        'User-Agent': `SolanaGamblesBot/2.2-revised`
    },
    clientId: `SolanaGamblesBot/2.2-revised`
});
console.log("✅ Multi-RPC Solana connection initialized");

// --- LIVE QUEUE MONITOR ---
setInterval(() => {
    if (!solanaConnection?.stats) return;

    const { totalRequestsEnqueued, totalRequestsSucceeded, totalRequestsFailed, endpointRotations } = solanaConnection.stats;
    const queueSize = solanaConnection.requestQueue?.length || 0; // Safer access
    const activeRequests = solanaConnection.activeRequests || 0; // Safer access
    const successRate = totalRequestsEnqueued > 0 ? (100 * totalRequestsSucceeded / totalRequestsEnqueued).toFixed(1) : "N/A";

    console.log(`[Queue Monitor] Active: ${activeRequests}, Pending: ${queueSize}, Success Rate: ${successRate}%, Rotations: ${endpointRotations}`);
}, 60000); // Every 60 seconds

// --- EMERGENCY AUTO-ROTATE ---
let lastQueueProcessTime = Date.now();

const originalProcessQueue = solanaConnection._processQueue.bind(solanaConnection);

solanaConnection._processQueue = async function patchedProcessQueue() {
    lastQueueProcessTime = Date.now();
    // Use try-catch within the patched function for safety
    try {
        await originalProcessQueue();
    } catch (queueProcessingError) {
        console.error("❌ Error during Solana connection internal queue processing:", queueProcessingError);
        // Consider additional error handling here if needed
    }
};

setInterval(() => {
    const now = Date.now();
    const stuckDuration = now - lastQueueProcessTime;
    const queueSize = solanaConnection.requestQueue?.length || 0;
    const activeRequests = solanaConnection.activeRequests || 0;

    // Only rotate if the queue is stuck AND there are pending or active requests
    if (stuckDuration > 30000 && (queueSize > 0 || activeRequests > 0)) { // If no progress for 30 seconds WITH items pending/active
        console.warn(`[Emergency Rotate] No queue movement detected for ${stuckDuration}ms with pending/active requests (${activeRequests} active, ${queueSize} pending). Forcing endpoint rotation...`);
        solanaConnection._rotateEndpoint(); // Force rotation
        lastQueueProcessTime = Date.now(); // Reset timer after forced rotation
    }
}, 10000); // Check every 10 seconds

// --- Initial RPC Connection Test ---
setTimeout(async () => {
  try {
    const currentSlot = await solanaConnection.getSlot();
    console.log(`✅ [Startup Test] RPC call successful! Current Solana Slot: ${currentSlot}`);
  } catch (err) {
    // This error is often expected during startup if RPCs are slow initially
    console.warn(`⚠️ [Startup Test] Initial RPC call failed (may resolve shortly):`, err.message);
  }
}, 5000); // Test after 5 seconds

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
    max: parseInt(process.env.DB_POOL_MAX || '15', 10),           // Max connections in pool
    min: parseInt(process.env.DB_POOL_MIN || '5', 10),            // Min connections maintained
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

// 4. Simple Performance Monitor (remains the same)
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
            const connectionStats = (typeof solanaConnection?.getRequestStats === 'function')
                ? solanaConnection.getRequestStats()
                : null;
            // Format connection stats more concisely
            const statsString = connectionStats
                ? `| SOL Conn: Q:${connectionStats.status.queueSize ?? 'N/A'} A:${connectionStats.status.activeRequests ?? 'N/A'} E:${connectionStats.status.currentEndpointIndex ?? 'N/A'} RL:${connectionStats.status.consecutiveRateLimits ?? 'N/A'} Rot:${connectionStats.stats.endpointRotations ?? 'N/A'} Fail:${connectionStats.stats.totalRequestsFailed ?? 'N/A'}`
                : '';

            console.log(`
📊 Perf Mon: Uptime:${uptime.toFixed(0)}s | Req:${this.requests} | Err:${errorRate}% ${statsString}
            `);
        }
    }
};


// --- Database Initialization ---
// Incorporates syntax fixes and new indexes from previous versions
async function initializeDatabase() {
    console.log("⚙️ Initializing Database schema...");
    let client;
    try {
        client = await pool.connect();

        // Bets Table: Tracks individual game bets
        // Added NOT NULL constraints where appropriate and improved column types
        await client.query(`CREATE TABLE IF NOT EXISTS bets (
            id SERIAL PRIMARY KEY,
            user_id TEXT NOT NULL,
            chat_id TEXT NOT NULL,
            game_type TEXT NOT NULL,
            bet_details JSONB,
            expected_lamports BIGINT NOT NULL CHECK (expected_lamports > 0), -- Ensure positive amount
            memo_id TEXT UNIQUE NOT NULL,
            status TEXT NOT NULL, -- Consider ENUM type in future for specific states
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            expires_at TIMESTAMPTZ NOT NULL,
            paid_tx_signature TEXT UNIQUE,     -- Signature of payment *from* user
            payout_tx_signature TEXT UNIQUE,   -- Signature of payout *to* user
            processed_at TIMESTAMPTZ,          -- Timestamp of final state (win/loss/error)
            fees_paid BIGINT,                  -- Estimated fees for payout (can be set during bet creation)
            priority INT NOT NULL DEFAULT 0
        );`);

        // Wallets Table: Links Telegram User ID to their Solana wallet address
        await client.query(`CREATE TABLE IF NOT EXISTS wallets (
            user_id TEXT PRIMARY KEY,
            wallet_address TEXT NOT NULL, -- Consider adding UNIQUE constraint if one wallet per user needed elsewhere
            linked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_used_at TIMESTAMPTZ -- Updated when wallet is linked/used for payout
        );`);

        // Add columns using ALTER TABLE IF NOT EXISTS for backward compatibility
        // Use transaction for potentially multiple ALTER statements
        await client.query('BEGIN');
        // Bets table alterations (already included in CREATE TABLE above, but good for existing setups)
        await client.query(`ALTER TABLE bets ADD COLUMN IF NOT EXISTS priority INT NOT NULL DEFAULT 0;`); // Ensure NOT NULL
        await client.query(`ALTER TABLE bets ADD COLUMN IF NOT EXISTS fees_paid BIGINT;`);
        await client.query(`ALTER TABLE bets ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ;`);
         // Wallets table alterations
        await client.query(`ALTER TABLE wallets ADD COLUMN IF NOT EXISTS last_used_at TIMESTAMPTZ;`);
        await client.query('COMMIT');

        console.log("  - Columns verified/added.");

        // Add indexes for performance on frequently queried columns
        // Use CONCURRENTLY in production if possible, but IF NOT EXISTS is safer for init script
        console.log("  - Creating indexes (if they don't exist)...");
        // Bets table indexes
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_status_created ON bets(status, created_at DESC);`); // Compound for pending/error checks
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_user_id ON bets(user_id);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_expires_at ON bets(expires_at);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_priority ON bets(priority DESC);`); // Index priority descending
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_paid_tx_sig ON bets(paid_tx_signature) WHERE paid_tx_signature IS NOT NULL;`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_payout_tx_sig ON bets(payout_tx_signature) WHERE payout_tx_signature IS NOT NULL;`);
        // Bets memo index (ensure unique constraint exists before creating index)
        await client.query(`DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'bets_memo_id_key' AND conrelid = 'bets'::regclass) THEN
                ALTER TABLE bets ADD CONSTRAINT bets_memo_id_key UNIQUE (memo_id);
            END IF;
        END $$;`);
        // Index specifically optimized for looking up pending bets by memo
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_memo_id_pending ON bets (memo_id) WHERE status = 'awaiting_payment';`);

        // Wallets table indexes
        await client.query(`CREATE INDEX IF NOT EXISTS idx_wallets_address ON wallets(wallet_address);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_wallets_last_used ON wallets(last_used_at DESC NULLS LAST);`);


        console.log("✅ Database schema initialized/verified.");
    } catch (err) {
        // Log the detailed error if initialization fails
        console.error("❌ Database initialization error:", err);
        if (client) { // Attempt rollback if client was acquired
             await client.query('ROLLBACK').catch(rbErr => console.error("Rollback failed:", rbErr));
        }
        if (err.message.includes('already exists') && (err.code === '42P07' || err.code === '23505')) {
             console.warn("  - Note: Some elements (table/index/constraint) already existed, which is usually okay.");
        } else {
             throw err; // Re-throw other errors to prevent startup
        }
    } finally {
        if (client) client.release(); // Release client back to the pool
    }
}

// --- End of Part 1 ---
// index.js - Revised Part 2a

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
        // console.log("Ignoring non-text message or message from bot."); // Optional debug log
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
const telegramSendQueue = new PQueue({ concurrency: 1, interval: 1050, intervalCap: 1 }); // Slightly over 1s interval

/**
 * Sends a message via Telegram safely using a rate-limited queue.
 * @param {string|number} chatId - The chat ID to send the message to.
 * @param {string} message - The message text.
 * @param {TelegramBot.SendMessageOptions} [options={}] - Telegram Bot API message options (e.g., parse_mode).
 * @returns {Promise<TelegramBot.Message|undefined>} A promise resolving with the sent message or undefined on error.
 */
function safeSendMessage(chatId, message, options = {}) {
    // Basic validation
    if (!chatId || typeof message !== 'string') { // Ensure message is a string
        console.error("safeSendMessage: Missing chatId or invalid message type.");
        return Promise.resolve(undefined); // Return a resolved promise for consistency
    }
    if (message.length > 4096) {
        console.warn(`safeSendMessage: Message too long (${message.length} > 4096), truncating.`);
        message = message.substring(0, 4090) + '...'; // Truncate if too long
    }

    return telegramSendQueue.add(() =>
        bot.sendMessage(chatId, message, options).catch(err => {
            console.error(`❌ Telegram send error to chat ${chatId}:`, err.message);
            // Handle specific errors like "Forbidden: bot was blocked by the user"
            if (err.response && err.response.statusCode === 403) { // Check response status code
                 console.warn(`Bot may be blocked or kicked from chat ${chatId}.`);
                 // Optionally, update user/chat status in DB if needed (e.g., mark as inactive)
            } else if (err.response && err.response.statusCode === 400 && err.message.includes("message is too long")) {
                // Already logged length warning, this confirms Telegram rejected it
                console.error(` -> Confirmed message too long rejection for chat ${chatId}.`);
            } else if (err.response && err.response.statusCode === 429) {
                // Rate limited - queue should handle this, but log if it somehow gets through
                 console.warn(`Hit Telegram rate limit sending to ${chatId}. Queue should manage this.`);
            }
            // Return undefined or null to indicate failure without throwing in the main flow
             return undefined;
        })
    );
}

console.log("✅ Telegram Bot initialized");


// --- Express Setup for Webhook and Health Check ---
app.use(express.json({
    limit: '10kb', // Limit payload size
    // verify: (req, res, buf) => { // Keep raw body if needed for signature verification (rarely needed for TG bot)
    //     req.rawBody = buf;
    // }
}));

// Original Health check / Info endpoint (keep for manual checks / detailed status)
app.get('/', (req, res) => {
    performanceMonitor.logRequest(true); // Count this as a request
    const processor = paymentProcessor; // Assign to local var for safety (paymentProcessor defined below)
     // Include Solana connection stats if available
    const connectionStats = typeof solanaConnection?.getRequestStats === 'function'
        ? solanaConnection.getRequestStats()
        : null;

    // Safely access queue sizes
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
        initialized: isFullyInitialized, // Report background initialization status here
        timestamp: new Date().toISOString(),
        version: '2.2-revised', // Updated version
        queueStats: { // Report queue status
            messageQueuePending: messageQueueSize,
            messageQueueActive: messageQueuePending,
            telegramSendPending: telegramSendQueueSize,
            telegramSendActive: telegramSendQueuePending,
            paymentHighPriPending: paymentHighPriQueueSize,
            paymentHighPriActive: paymentHighPriQueuePending,
            paymentNormalPending: paymentNormalQueueSize,
            paymentNormalActive: paymentNormalQueuePending
        },
        solanaConnection: connectionStats // Add connection stats to the main health check
    });
});

// Webhook handler (listens for updates from Telegram)
const webhookPath = `/bot${process.env.BOT_TOKEN}`;
app.post(webhookPath, (req, res) => {
    // Add webhook processing to the message queue to handle sequentially with polled messages
    messageQueue.add(() => {
        try {
            if (!req.body || typeof req.body !== 'object') {
                console.warn("⚠️ Received invalid webhook request body");
                performanceMonitor.logRequest(false);
                // It's important to still send a 2xx response to Telegram even for bad requests
                // otherwise it might keep retrying. 400 is acceptable.
                return res.status(400).send('Invalid request body'); // Send 400 for invalid body
            }

            // Process the update using the bot instance
            // This will eventually trigger the 'message' listener above if it's a message update
            bot.processUpdate(req.body);
            performanceMonitor.logRequest(true);
            res.sendStatus(200); // Acknowledge receipt to Telegram *immediately*

        } catch (error) { // Catch synchronous errors in processUpdate or body handling
            console.error("❌ Webhook processing error:", error);
            performanceMonitor.logRequest(false);
            // Send 200 to Telegram to prevent retries, but log the internal error
            // Avoid sending JSON back here, just a status
            res.sendStatus(500); // Indicate internal server error, though TG might ignore
        }
    }).catch(queueError => {
        // Handle errors adding to the queue itself (less likely)
        console.error("❌ Error adding webhook update to message queue:", queueError);
        performanceMonitor.logRequest(false);
        // Acknowledge Telegram anyway to prevent retries for queue errors
        if (!res.headersSent) {
             res.sendStatus(500);
        }
    });
});


// --- State Management & Constants ---

// User command cooldown (prevents spam)
const confirmCooldown = new Map(); // Map<userId: string, lastCommandTimestamp: number>
const cooldownInterval = parseInt(process.env.USER_COOLDOWN_MS || '3000', 10); // 3 seconds default

// Cache for linked wallets (reduces DB lookups)
const walletCache = new Map(); // Map<userId: string, { wallet: address, timestamp: cacheTimestamp }>
const CACHE_TTL = parseInt(process.env.WALLET_CACHE_TTL_MS || '300000', 10); // Cache wallet links for 5 minutes default

// Cache of processed transaction signatures during this bot session (prevents double processing)
const processedSignaturesThisSession = new Set(); // Set<signature: string>
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
// Validate game config numbers
if (isNaN(GAME_CONFIG.coinflip.minBet) || isNaN(GAME_CONFIG.coinflip.maxBet) || /* ... other checks ... */ false) {
     console.error("❌ Invalid game configuration values found in environment variables. Please check CF_/RACE_ MIN/MAX/EXPIRY/HOUSE_EDGE.");
     process.exit(1);
}
console.log("ℹ️ Game Config Loaded:", GAME_CONFIG);

// Fee buffer (lamports) - should cover base fee + priority fee estimate
const FEE_BUFFER = BigInt(process.env.FEE_MARGIN); // Already validated FEE_MARGIN exists
// Priority fee rate already parsed into PRIORITY_FEE_RATE constant near env checks

// --- Helper Functions ---

// <<< DEBUG HELPER FUNCTION - Kept for potential future use >>>
function debugInstruction(inst, accountKeys) {
    // ... (implementation remains unchanged from original) ...
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
// <<< END DEBUG HELPER FUNCTION >>>

// For decoding instruction data (Base64 or Buffer)
const decodeInstructionData = (data) => {
    // ... (implementation remains unchanged - handles base64, buffer, uint8array, number array) ...
    if (!data) return null;
    try {
        if (typeof data === 'string') {
           if (/^[A-Za-z0-9+/]*={0,2}$/.test(data)) {
               return Buffer.from(data, 'base64').toString('utf8');
           } else if (/[1-9A-HJ-NP-Za-km-z]{32,}/.test(data) && data.length < 60) { // Heuristic for base58
               try { return Buffer.from(bs58.decode(data)).toString('utf8'); } catch { /* ignore */ }
           }
           return data; // Assume plain string if other decodings fail
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
        // console.warn("Failed to decode instruction data:", e.message); // Optional: log decoding failures
        return null;
    }
};


// --- START: Memo Handling System ---
// (Functions remain unchanged internally as per previous review)

// Define Memo Program IDs
const MEMO_V1_PROGRAM_ID = new PublicKey("Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo");
const MEMO_V2_PROGRAM_ID = new PublicKey("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr");
const MEMO_PROGRAM_IDS = [MEMO_V1_PROGRAM_ID.toBase58(), MEMO_V2_PROGRAM_ID.toBase58()];

// 1. Revised Memo Generation with Checksum (Used for *generating* our memos)
function generateMemoId(prefix = 'BET') {
    // ... (implementation remains unchanged) ...
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

// 2. Memo Normalization (crucial for matching incoming memos)
function normalizeMemo(rawMemo) {
    // ... (implementation remains unchanged - handles control chars, case, prefixes, V1 format/checksum correction) ...
    if (typeof rawMemo !== 'string') {
        return null;
    }
    // Explicitly remove potential SINGLE trailing newline FIRST
    let memo = rawMemo.replace(/\n$/, '');
    // Remove ALL control characters and trim remaining whitespace
    memo = memo.replace(/[\x00-\x1F\x7F]/g, '').trim();
    // Standardize format aggressively
    memo = memo.toUpperCase()
        .replace(/^MEMO[:=\s]*/i, '')
        .replace(/^TEXT[:=\s]*/i, '')
        .replace(/[\u200B-\u200D\uFEFF]/g, '') // Remove zero-width spaces
        .replace(/\s+/g, '-') // Replace spaces with dashes
        .replace(/[^A-Z0-9\-]/g, ''); // Remove remaining non-alphanumeric/dash characters

   // Handle potential trailing 'N' after checksum-like part (Phantom bug workaround)
   const trailingNPattern = /^(BET|CF|RA)-([A-F0-9]{16})-([A-F0-9]{2})N$/;
   const trailingNMatch = memo.match(trailingNPattern);
   if (trailingNMatch) {
       console.warn(`[NORMALIZE_MEMO] Detected V1-like memo with trailing 'N': "${memo}". Correcting.`);
       memo = memo.slice(0, -1); // Remove the last character ('N')
   }

    // V1 Pattern Match and Checksum logic...
    const v1Pattern = /^(BET|CF|RA)-([A-F0-9]{16})-([A-F0-9]{2})$/;
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
             return null; // Exit if hashing fails
        }
        if (checksum !== expectedChecksum) {
             console.warn(`[NORMALIZE_MEMO] Checksum MISMATCH! Input: "${checksum}", Expected: "${expectedChecksum}". Correcting.`);
        }
        // Return the format with the CORRECT calculated checksum
        return `${prefix}-${hex}-${expectedChecksum}`;
    }

    // Fallback if V1 Pattern Did NOT Match - basic sanity check
    if (memo && memo.length >= 3 && memo.length <= 64) { // Example length constraints
         return memo;
    }
    return null; // Return null if memo becomes empty or doesn't meet basic criteria
}


// 3. Strict Memo Validation (Used only for validating OUR generated V1 format before saving)
function validateOriginalMemoFormat(memo) {
    // ... (implementation remains unchanged - checks structure and checksum) ...
     if (typeof memo !== 'string') return false;
     const parts = memo.split('-');
     if (parts.length !== 3) return false;
     const [prefix, hex, checksum] = parts;
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

// 4. Find Memo in Transaction (Uses normalizeMemo)
async function findMemoInTx(tx, signature) {
    // ... (implementation remains unchanged - uses logs, instruction parsing, pattern matching) ...
     const startTime = Date.now(); // For MEMO STATS
     const usedMethods = [];       // For MEMO STATS
     let scanDepth = 0;            // For MEMO STATS

     const transactionResponse = tx; // Type assertion/clarity

     if (!transactionResponse?.transaction?.message) {
         console.log(`[MEMO DEBUG] Sig ${signature?.slice(0,6)}: Invalid tx structure (missing message)`);
         return null;
     }

     // 1. Try log messages first
     scanDepth = 1;
     if (transactionResponse.meta?.logMessages) {
         const memoLogRegex = /Program log: Memo(?: \(len \d+\))?:\s*"?([^"\n]+)"?/; // Explicitly exclude newlines
         for (const log of transactionResponse.meta.logMessages) {
              const match = log.match(memoLogRegex);
              if (match?.[1]) {
                  const rawMemo = match[1].trim();
                  const memo = normalizeMemo(rawMemo);
                  if (memo) {
                      usedMethods.push('LogScanRegex');
                      console.log(`[MEMO DEBUG] Sig ${signature?.slice(0,6)}: Found memo via LogScanRegex: "${memo}" (Raw: "${rawMemo}")`);
                      console.log(`[MEMO STATS] TX:${signature?.slice(0,8)}|M:${usedMethods.join(',')}|D:${scanDepth}|T:${Date.now() - startTime}ms`);
                      return memo;
                  }
              }
         }
     }

     // 2. Fallback to instruction parsing
     scanDepth = 2;
     const message = transactionResponse.transaction.message;
     let accountKeys = []; // Ensure initialized

     // Robustly get account keys as strings
     try {
         if (message.getAccountKeys) { // VersionedMessage
             const accounts = message.getAccountKeys();
             // Combine static and potentially resolved dynamic keys if needed/available (though often not needed for memo)
             accountKeys = accounts.staticAccountKeys.map(k => k.toBase58());
             // if (accounts.accountKeysFromLookups) { ... add resolved keys ... } // Add if necessary later
         } else if (message.accountKeys) { // Legacy Message
             accountKeys = message.accountKeys.map(k => k.toBase58 ? k.toBase58() : String(k));
         }
     } catch (e) {
         console.error(`[MEMO DEBUG] Sig ${signature?.slice(0,6)}: Error extracting account keys: ${e.message}`);
     }

     // Combine regular and inner instructions
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

                  // Check for memo programs
                  if (programId && MEMO_PROGRAM_IDS.includes(programId)) {
                      const dataString = decodeInstructionData(inst.data); // Use helper
                       if (dataString) {
                           const memo = normalizeMemo(dataString);
                           if (memo) {
                               const method = MEMO_PROGRAM_IDS.indexOf(programId) === 0 ? 'InstrParseV1' : 'InstrParseV2';
                               usedMethods.push(method); // Log Method
                               console.log(`[MEMO DEBUG] Sig ${signature?.slice(0,6)}: Found ${method} memo via instruction parse: "${memo}"`);
                               console.log(`[MEMO STATS] TX:${signature?.slice(0,8)}|M:${usedMethods.join(',')}|D:${scanDepth}|T:${Date.now() - startTime}ms`);
                               return memo;
                           }
                       }
                  }

                  // Raw pattern matching fallback *within* the instruction loop
                  scanDepth = 3;
                  // Decode data robustly - handles base64 string or buffer/array
                  let dataBuffer = null;
                  if (inst.data) {
                    if (typeof inst.data === 'string') {
                        try { dataBuffer = Buffer.from(toByteArray(inst.data)); } catch { /* ignore base64 decode error */ }
                    } else if (Buffer.isBuffer(inst.data) || inst.data instanceof Uint8Array || Array.isArray(inst.data)) {
                        try { dataBuffer = Buffer.from(inst.data); } catch { /* ignore buffer conversion error */ }
                    }
                  }

                  if (dataBuffer && dataBuffer.length >= 22) { // Check minimum length for V1 format
                      const potentialMemo = dataBuffer.toString('utf8'); // Decode relevant part
                      const memo = normalizeMemo(potentialMemo); // Try to normalize/validate

                      // Check if normalization resulted in a valid V1 format
                      if (memo && validateOriginalMemoFormat(memo)) {
                          usedMethods.push('PatternMatchV1'); // Log Method
                          console.log(`[MEMO DEBUG] Sig ${signature?.slice(0,6)}: Pattern-matched and validated V1 memo: "${memo}" (Raw: "${potentialMemo}")`);
                          console.log(`[MEMO STATS] TX:${signature?.slice(0,8)}|M:${usedMethods.join(',')}|D:${scanDepth}|T:${Date.now() - startTime}ms`);
                          return memo;
                      }
                  }
              } catch (e) {
                  console.error(`[MEMO DEBUG] Sig ${signature?.slice(0,6)}: Error processing instruction ${i}:`, e?.message || e);
              }
          } // End for loop
     } // End else

     // 3. Deep Log Scan as final fallback
     scanDepth = 4;
     if (transactionResponse.meta?.logMessages) {
         const logString = transactionResponse.meta.logMessages.join('\n');
         const logMemoMatch = logString.match(/(?:Memo|Text|Message|Data|Log):?\s*"?([A-Z0-9\-]{10,64})"?/i); // Broad pattern
         if (logMemoMatch?.[1]) {
             const recoveredMemo = normalizeMemo(logMemoMatch[1]);
             if (recoveredMemo) {
                 usedMethods.push('DeepLogScan'); // Log Method
                 console.log(`[MEMO DEBUG] Sig ${signature?.slice(0,6)}: Recovered memo from deep log scan: ${recoveredMemo} (Matched: ${logMemoMatch[1]})`);
                 console.log(`[MEMO STATS] TX:${signature?.slice(0,8)}|M:${usedMethods.join(',')}|D:${scanDepth}|T:${Date.now() - startTime}ms`);
                 return recoveredMemo;
             }
         }
     }

     // Only log if address lookup tables were present and memo not found by other means
     if (message.addressTableLookups?.length > 0) {
        console.log(`[MEMO DEBUG] Sig ${signature?.slice(0,6)}: Transaction uses address lookup tables (memo finding may be incomplete). No memo found via logs or instructions.`);
     }

     console.log(`[MEMO DEBUG] Sig ${signature?.slice(0,6)}: Exhausted all search methods, no valid memo found.`);
     return null;
}
// --- END: Memo Handling System ---


// --- Database Operations ---
// (These functions remain structurally the same, relying on the corrected schema)

// Saves a new bet intention to the database
async function savePendingBet(userId, chatId, gameType, details, lamports, memoId, expiresAt, priority = 0) {
    // ... (implementation remains unchanged - uses INSERT) ...
    if (!validateOriginalMemoFormat(memoId)) {
        console.error(`DB: Attempted to save bet with invalid generated memo format: ${memoId}`);
        return { success: false, error: 'Internal error: Invalid memo ID generated' };
    }
    const query = `
        INSERT INTO bets (
            user_id, chat_id, game_type, bet_details,
            expected_lamports, memo_id, status, expires_at, fees_paid, priority
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (memo_id) DO NOTHING -- Prevent error on duplicate memo, just don't insert
        RETURNING id;
    `; // Changed to ON CONFLICT DO NOTHING
    const values = [
        String(userId), String(chatId), gameType, details, BigInt(lamports),
        memoId, 'awaiting_payment', expiresAt, FEE_BUFFER, priority
    ];
    try {
        const res = await pool.query(query, values);
        if (res.rows.length > 0) {
            console.log(`DB: Saved pending ${gameType} bet ID ${res.rows[0].id} with memo ${memoId}`);
            return { success: true, id: res.rows[0].id };
        } else {
            // This now means the memo_id already existed
            console.warn(`DB: Memo ID collision for ${memoId}. Bet likely already exists or user retried command quickly.`);
            // Try to fetch the existing bet to inform the user better? Optional.
            // For now, treat as a user-retryable error.
            return { success: false, error: 'Bet with this Memo ID already exists. Please wait a moment or try generating a new bet.' };
        }
    } catch (err) {
        console.error('DB Error saving bet:', err.message, err.code); // Log code
        // Specific unique violation check might be redundant now with ON CONFLICT
        return { success: false, error: `Database error (${err.code || 'N/A'})` };
    }
}


// Links a Solana wallet address to a Telegram User ID
async function linkUserWallet(userId, walletAddress) {
    // ... (implementation remains unchanged - uses INSERT ON CONFLICT DO UPDATE, cache logic) ...
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
             console.log(`DB: Linked/Updated wallet for user ${userId} to ${linkedWallet}`);
             walletCache.set(cacheKey, { wallet: linkedWallet, timestamp: Date.now() });
             // Set TTL for cache entry deletion
             setTimeout(() => {
                 const current = walletCache.get(cacheKey);
                 if (current && current.wallet === linkedWallet && Date.now() - current.timestamp >= CACHE_TTL) {
                     walletCache.delete(cacheKey);
                 }
             }, CACHE_TTL + 1000); // Add slight buffer to timeout
             return { success: true, wallet: linkedWallet };
         } else {
             console.error(`DB: Failed to link/update wallet for user ${userId}. Query returned no address.`);
             return { success: false, error: 'Failed to update wallet in database.' };
         }
     } catch (err) {
         if (err instanceof Error && (err.message.includes('Invalid public key') || err.message.includes('Invalid address'))) {
             console.error(`DB Error linking wallet: Invalid address format provided for user ${userId}: ${walletAddress}`);
             return { success: false, error: 'Invalid wallet address format.' };
         }
         console.error(`DB Error linking wallet for user ${userId}:`, err.message);
         return { success: false, error: `Database error (${err.code || 'N/A'})` };
     }
}

// Retrieves the linked wallet address for a user (checks cache first)
async function getLinkedWallet(userId) {
    // ... (implementation remains unchanged - uses cache, SELECT) ...
    const cacheKey = `wallet-${userId}`;
    const cachedEntry = walletCache.get(cacheKey);
    if (cachedEntry && Date.now() - cachedEntry.timestamp < CACHE_TTL) {
        // console.log(`Cache hit for user ${userId} wallet.`); // Optional: Debug log
        return cachedEntry.wallet; // Return cached wallet if valid
    } else if (cachedEntry) {
        walletCache.delete(cacheKey); // Delete expired entry
    }

    // If not in cache or expired, query DB
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
        return wallet; // Return wallet address or undefined
    } catch (err) {
        console.error(`DB Error fetching wallet for user ${userId}:`, err.message);
        return undefined; // Indicate error or not found
    }
}

// Updates the status of a specific bet
async function updateBetStatus(betId, status) {
    // ... (implementation remains unchanged - uses UPDATE) ...
    const query = `UPDATE bets SET status = $1, processed_at = CASE WHEN $1 LIKE 'completed_%' OR $1 LIKE 'error_%' THEN NOW() ELSE processed_at END WHERE id = $2 RETURNING id;`;
    try {
        const res = await pool.query(query, [status, betId]);
        if (res.rowCount > 0) {
            // console.log(`DB: Updated status for bet ${betId} to ${status}`); // Reduce log noise
            return true;
        } else {
            console.warn(`DB: Failed to update status for bet ${betId} (not found or status already set?)`);
            return false;
        }
    } catch (err) {
        console.error(`DB Error updating bet ${betId} status to ${status}:`, err.message);
        return false;
    }
}

// Records the payout transaction signature for a completed winning bet
async function recordPayout(betId, status, signature) {
    // ... (implementation remains unchanged - uses UPDATE) ...
    const query = `
        UPDATE bets
        SET status = $1,
            payout_tx_signature = $2,
            processed_at = NOW() -- Record time payout was completed
        WHERE id = $3 AND status = 'processing_payout' -- Ensure status is correct for update
        RETURNING id;
    `;
    try {
        const res = await pool.query(query, [status, signature, betId]);
        if (res.rowCount > 0) {
            console.log(`DB: Recorded payout TX ${signature.slice(0,10)}... for bet ${betId} with status ${status}`);
            return true;
        } else {
            // Check current status if update failed to provide better context
            const current = await pool.query('SELECT status FROM bets WHERE id = $1', [betId]);
            console.warn(`DB: Failed to record payout for bet ${betId} (not found or status mismatch?). Current status: ${current.rows[0]?.status ?? 'Not Found'}`);
            return false;
        }
    } catch (err) {
         // Handle unique constraint violation on payout_tx_signature specifically
         if (err.code === '23505' && err.constraint?.includes('payout_tx_signature')) {
             console.warn(`DB: Payout TX Signature ${signature.slice(0,10)}... collision for bet ${betId}. Already recorded.`);
             // Check if it was already recorded correctly
             const current = await pool.query('SELECT status, payout_tx_signature FROM bets WHERE id = $1', [betId]);
              if(current.rows[0]?.payout_tx_signature === signature && current.rows[0]?.status.startsWith('completed_win')){
                  console.log(`DB: Bet ${betId} already correctly recorded with this payout signature. Treating as success.`);
                  return true; // Allow process to continue if already recorded correctly
              }
             return false; // Indicate failure if status doesn't match
         }
        console.error(`DB Error recording payout for bet ${betId}:`, err.message);
        return false;
    }
}


// --- Solana Transaction Analysis ---

/**
 * Analyzes a transaction to find SOL transfers to the target bot wallet.
 * CORRECTED: Checks both top-level and inner instructions for transfers.
 * @param {import('@solana/web3.js').ParsedTransactionWithMeta | import('@solana/web3.js').VersionedTransactionResponse | null} tx - The transaction object from getParsedTransaction.
 * @param {'coinflip' | 'race'} walletType - The type of wallet to check against (determines target address).
 * @returns {{ transferAmount: bigint, payerAddress: string | null }} The transfer amount in lamports and the detected payer address.
 */
function analyzeTransactionAmounts(tx, walletType) {
    let transferAmount = 0n;
    let payerAddress = null;
    const targetAddress = walletType === 'coinflip'
        ? process.env.MAIN_WALLET_ADDRESS
        : process.env.RACE_WALLET_ADDRESS;

    // Basic validation: Ensure we have a successful transaction and target address
    if (!tx || tx.meta?.err || !targetAddress) {
        return { transferAmount: 0n, payerAddress: null };
    }

    // Helper to safely get account keys as Base58 strings from message
    const getAccountKeys = (message) => {
        try {
            if (!message) return [];
            if (message.getAccountKeys) { // VersionedMessage v0
                const accounts = message.getAccountKeys();
                // Note: For full analysis involving Address Lookup Tables (ALTs),
                // you would need the 'loadedAddresses' from getTransaction response.
                // For finding simple transfers, static keys are usually sufficient.
                return accounts.staticAccountKeys.map(k => k.toBase58());
            } else if (message.accountKeys) { // Legacy Message or VersionedMessage without ALTs
                return message.accountKeys.map(k => k.toBase58 ? k.toBase58() : String(k));
            }
        } catch (e) {
            console.error("[analyzeAmounts] Error parsing account keys:", e.message);
        }
        return [];
    };

    // --- 1. Try using balance changes (often reliable for direct transfers) ---
    let balanceChange = 0n; // Store balance change separately
    if (tx.meta?.preBalances && tx.meta?.postBalances && tx.transaction?.message) {
        const accountKeys = getAccountKeys(tx.transaction.message);
        const targetIndex = accountKeys.indexOf(targetAddress);

        if (targetIndex !== -1 && tx.meta.preBalances[targetIndex] !== undefined && tx.meta.postBalances[targetIndex] !== undefined) {
            balanceChange = BigInt(tx.meta.postBalances[targetIndex]) - BigInt(tx.meta.preBalances[targetIndex]);
            if (balanceChange > 0n) {
                transferAmount = balanceChange; // Assume this is the transfer amount initially
                // console.log(`[analyzeAmounts] Detected balance change: +${transferAmount} lamports for ${targetAddress}`); // Debug log

                // Try to identify payer using balance changes (heuristic)
                const feePayerIndex = 0; // Fee payer is always the first account
                if (accountKeys[feePayerIndex]) {
                    const preFeePayerBalance = tx.meta.preBalances[feePayerIndex];
                    const postFeePayerBalance = tx.meta.postBalances[feePayerIndex];
                    if (preFeePayerBalance !== undefined && postFeePayerBalance !== undefined) {
                        const feePayerChange = BigInt(postFeePayerBalance) - BigInt(preFeePayerBalance);
                        const fee = BigInt(tx.meta.fee || 5000n); // Use actual fee or default estimate
                        // Check if fee payer lost *at least* the transferred amount (allowing for fee)
                        if (feePayerChange <= -(transferAmount - fee)) {
                            payerAddress = accountKeys[feePayerIndex];
                        }
                    }
                }
                // Fallback: Look for other signers if fee payer wasn't identified (less reliable)
                if (!payerAddress && tx.transaction?.message?.header?.numRequiredSignatures > 1) {
                    for (let i = 1; i < tx.transaction.message.header.numRequiredSignatures; i++) {
                        if (!accountKeys[i]) continue;
                        const preBalance = tx.meta.preBalances[i];
                        const postBalance = tx.meta.postBalances[i];
                        if (preBalance === undefined || postBalance === undefined) continue;
                        const signerBalanceChange = BigInt(postBalance) - BigInt(preBalance);
                        // Simple check: did this signer lose at least the amount transferred?
                        if (signerBalanceChange <= -transferAmount) {
                            payerAddress = accountKeys[i];
                            break; // Assume first signer found losing enough is the payer
                        }
                    }
                }
            }
        }
    } // End balance check

    // --- 2. Check instructions (top-level and inner) for SystemProgram transfers ---
    // This acts as confirmation or primary source if balance diff was zero/ambiguous.
    if (tx.transaction?.message) {
        const accountKeys = getAccountKeys(tx.transaction.message); // Get account keys again if needed
        const SYSTEM_PROGRAM_ID = SystemProgram.programId.toBase58();

        // Combine instructions for unified checking
        const topLevelInstructions = tx.transaction.message.instructions || [];
        const innerInstructionsNested = tx.meta?.innerInstructions || [];

        // Flatten inner instructions, adding index context if needed later
        const allInstructions = [
            ...topLevelInstructions.map(inst => ({ ...inst, type: 'topLevel' })), // Mark type
            ...innerInstructionsNested.flatMap(innerSet =>
                (innerSet.instructions || []).map(inst => ({ ...inst, type: 'inner', parentIndex: innerSet.index })) // Mark type
            )
        ];

        for (const instWrapper of allInstructions) {
            const inst = instWrapper; // The actual instruction object
            let programId = '';
            let parsedInfo = null;
            let isParsedTransfer = false;

            try {
                // Get programId robustly
                if ('programId' in inst) { // Typically ParsedInstruction or PartiallyDecodedInstruction (inner)
                    programId = typeof inst.programId === 'string' ? inst.programId : inst.programId.toBase58();
                } else if ('programIdIndex' in inst && accountKeys.length > inst.programIdIndex) { // TransactionInstruction (top-level)
                    programId = accountKeys[inst.programIdIndex];
                }

                // Check if it's a SystemProgram transfer (check programId first)
                if (programId === SYSTEM_PROGRAM_ID) {
                    // Check if the instruction is already parsed by getParsedTransaction
                    if ('parsed' in inst && inst.parsed?.type === 'transfer') {
                        isParsedTransfer = true;
                        parsedInfo = inst.parsed.info;
                    }
                    // Add manual decoding here ONLY IF necessary (if parsed info is often missing)
                    // else if ('data' in inst && instWrapper.type === 'topLevel') {
                    //     // Manual decode logic for SystemProgram.transfer from inst.data buffer
                    // }
                }

                // If it's a parsed transfer instruction to our target address
                if (isParsedTransfer && parsedInfo?.destination === targetAddress) {
                    const instructionAmount = BigInt(parsedInfo.lamports || parsedInfo.amount || 0);

                    if (instructionAmount > 0n) {
                        // If balance diff was zero, OR if instruction amount matches balance diff,
                        // trust the instruction details.
                        if (transferAmount === 0n || instructionAmount === balanceChange) {
                            transferAmount = instructionAmount; // Use instruction amount
                            payerAddress = parsedInfo.source; // Trust instruction source as payer
                            // console.log(`[analyzeAmounts] Confirmed transfer via INSTRUCTION: ${transferAmount} from ${payerAddress}`);
                            // Found definitive transfer via instruction, can stop searching
                            return { transferAmount, payerAddress };
                        } else if (transferAmount > 0n && instructionAmount !== balanceChange) {
                            // Ambiguous case: Balance change found different amount than instruction.
                            // This is unusual. Log a warning. Typically balance change reflects the NET effect better.
                            console.warn(`[analyzeAmounts] Ambiguous amounts found for target ${targetAddress}. Balance change: ${balanceChange}, Instruction amount: ${instructionAmount}. Using balance change amount.`);
                            // Keep amount from balance change, but update payer if instruction found one and balance check didn't.
                            if (!payerAddress && parsedInfo.source) {
                                payerAddress = parsedInfo.source;
                            }
                            // Return the potentially ambiguous result (amount from balance, payer maybe from instruction)
                            return { transferAmount, payerAddress };
                        }
                    }
                } // end if parsed transfer to target
            } catch(parseError) {
                console.error(`[analyzeAmounts] Error processing instruction: ${parseError.message}`);
                // Continue to next instruction
            }
        } // End instruction loop
    } // End instruction check block

    // Return final results after checking balances and instructions
    // console.log(`Analyzed TX: Final result - Amount: ${transferAmount}, Payer: ${payerAddress || 'Unknown'}`);
    return { transferAmount, payerAddress };
}


// --- End of Part 2a ---
// index.js - Revised Part 2b

// (Code continues directly from the end of Part 2a)

// --- Payment Processing System ---

// Standalone utility function to check for retryable errors (used by processor)
function isRetryableError(error) {
    const msg = error?.message?.toLowerCase() || '';
    const code = error?.code || error?.cause?.code; // Check nested code too
    const status = error?.response?.status || error?.statusCode; // HTTP status

    // Rate limit (HTTP 429 or specific messages)
    if (status === 429 || msg.includes('429') || msg.includes('rate limit') || msg.includes('too many requests')) {
        return true;
    }
    // Common HTTP/Network errors (potentially transient server-side or network issues)
    if ([500, 502, 503, 504].includes(Number(status)) || msg.includes('server error')) {
        return true;
    }
    // Node.js network errors
    if (['timeout', 'timed out', 'econnreset', 'esockettimedout', 'network error', 'fetch', 'socket hang up', 'connection terminated', 'econnrefused', 'failed to fetch', 'getaddrinfo enotfound'].some(m => msg.includes(m)) ||
        ['ETIMEDOUT', 'ECONNRESET', 'ENETUNREACH', 'EAI_AGAIN', 'ECONNABORTED', 'ECONNREFUSED', 'UND_ERR_CONNECT_TIMEOUT', 'UND_ERR_HEADERS_TIMEOUT', 'UND_ERR_BODY_TIMEOUT'].includes(code)) {
         // Added common Undici error codes
        return true;
    }
    // Database potentially transient errors
    if (msg.includes('connection terminated unexpectedly') || code === 'ECONNREFUSED' || code === '57P01' /* admin shutdown */ || code === '57P03' /* cannot connect now */) {
        return true;
    }
    // Solana specific transient errors (e.g., temporary node load)
     if (msg.includes('transaction simulation failed') ||
         msg.includes('failed to simulate transaction') ||
         msg.includes('blockhash not found') || // Can be retryable immediately with a fresh blockhash
         msg.includes('slot leader does not match') ||
         msg.includes('node is behind')) {
          return true;
     }
     // Custom error reason check (if we add reasons to errors)
     if (error?.reason === 'rpc_error' || error?.reason === 'network_error') {
         return true;
     }

    return false; // Assume not retryable otherwise
}


// *** GuaranteedPaymentProcessor Class - Revised ***
// Incorporates internal methods starting with "_" and fixes the call in processJob.
class GuaranteedPaymentProcessor {
    constructor() {
        this.highPriorityQueue = new PQueue({ concurrency: parseInt(process.env.PAYMENT_HP_CONCURRENCY || '3', 10) });
        this.normalQueue = new PQueue({ concurrency: parseInt(process.env.PAYMENT_NP_CONCURRENCY || '2', 10) });
        this.activeProcesses = new Set(); // Set<jobKey: string>
        this.memoCache = new Map(); // Cache for memo lookups <memoId, betObject>
        this.cacheTTL = parseInt(process.env.PAYMENT_MEMO_CACHE_TTL_MS || '30000', 10); // 30s default
        console.log("✅ Initialized GuaranteedPaymentProcessor");
    }

    // Adds a job to the appropriate queue based on priority
    async addPaymentJob(job) {
        const jobIdentifier = job.signature || job.betId || `job-${Date.now()}`;
        const jobKey = `${job.type}:${jobIdentifier}`;
        // Optional: Check if job already exists in a queue (p-queue doesn't expose this easily)
        // If adding the same job type/identifier repeatedly is an issue, add logic here

        const queue = (job.priority && job.priority > 0) ? this.highPriorityQueue : this.normalQueue;
        queue.add(() => this.processJob(job)).catch(queueError => {
            // Handle errors during the queueing process itself or unhandled rejections from processJob
            console.error(`Queue error processing job ${jobKey}:`, queueError.message);
            performanceMonitor.logRequest(false);
        });
    }

    // Wrapper to handle job execution, retries for payout, and error logging
    async processJob(job) {
        const jobIdentifier = job.signature || job.betId;
        // Use a more robust job key including potentially unique elements if identifiers aren't always present
        const jobKey = `${job.type}:${jobIdentifier || crypto.randomUUID()}`;

        if (this.activeProcesses.has(jobKey)) {
            console.warn(`[PROCESS_JOB] Skipped duplicate job (already active): ${jobKey}`);
            return;
        }
        this.activeProcesses.add(jobKey);
        // console.log(`[PROCESS_JOB] Starting: ${jobKey}`); // Optional debug log

        try {
            let result;
            if (job.type === 'monitor_payment') {
                // --- FIX APPLIED: Call internal method with underscore ---
                result = await this._processIncomingPayment(job.signature, job.walletType);
            } else if (job.type === 'process_bet') {
                // Fetch fresh bet details within the job
                const bet = await pool.query('SELECT * FROM bets WHERE id = $1', [job.betId]).then(res => res.rows[0]);
                if (bet) {
                    // processPaidBet handles its own errors/status updates
                    await processPaidBet(bet); // processPaidBet defined later
                    result = { processed: true }; // Mark job as done
                } else {
                    console.error(`[PROCESS_JOB] Cannot process bet: Bet ID ${job.betId} not found.`);
                    result = { processed: false, reason: 'bet_not_found' };
                }
            } else if (job.type === 'payout') {
                // --- Smart retry logic specifically for payout jobs ---
                let retries = parseInt(process.env.PAYOUT_JOB_RETRIES || '3', 10);
                const baseRetryDelay = parseInt(process.env.PAYOUT_JOB_RETRY_DELAY_MS || '5000', 10); // 5s base
                let attempt = 0;

                while (attempt <= retries) {
                    attempt++;
                    try {
                        // handlePayoutJob handles its own errors/status updates
                        await handlePayoutJob(job); // handlePayoutJob defined later
                        console.log(`[PAYOUT_JOB_SUCCESS] Bet ${job.betId} payout completed on attempt ${attempt}.`);
                        result = { processed: true };
                        break; // Exit retry loop on success
                    } catch (err) {
                        const errorMessage = err?.message || 'Unknown payout error';
                         // Check if handlePayoutJob already set a final error status
                         const currentBetStatus = await pool.query('SELECT status FROM bets WHERE id = $1', [job.betId]).then(r => r.rows[0]?.status);
                         if (currentBetStatus?.startsWith('error_payout_') || currentBetStatus === 'completed_win_paid') {
                             console.warn(`[PAYOUT_JOB_RETRY] Bet ${job.betId} already reached final state (${currentBetStatus}). Stopping retries.`);
                             result = { processed: false, reason: `payout_already_final_state: ${currentBetStatus}` };
                             break; // Stop retrying if bet is in a final state
                         }

                        console.warn(`[PAYOUT_JOB_RETRY] Attempt ${attempt}/${retries} failed for bet ${job.betId}. Error: ${errorMessage}`);

                        if (attempt >= retries) {
                            console.error(`[PAYOUT_JOB_FAIL] Final attempt failed for bet ${job.betId}.`);
                            // Mark bet as failed if not already done by handlePayoutJob
                             if (!currentBetStatus?.startsWith('error_')) {
                                await updateBetStatus(job.betId, 'error_payout_job_failed');
                                await safeSendMessage(job.chatId, `⚠️ Payout for bet \`${job.memoId}\` failed after multiple retries. Please contact support.`, { parse_mode: 'MarkdownV2' });
                             }
                            result = { processed: false, reason: `payout_retries_exhausted: ${errorMessage}` };
                            break; // Exit loop after final failure
                        }

                        // Use the standalone isRetryableError check
                        if (!isRetryableError(err)) {
                            console.warn(`[PAYOUT_JOB_RETRY] Error not deemed retryable for bet ${job.betId}. Aborting retries. Error: ${errorMessage}`);
                             if (!currentBetStatus?.startsWith('error_')) {
                                await updateBetStatus(job.betId, 'error_payout_non_retryable');
                                await safeSendMessage(job.chatId, `⚠️ Payout for bet \`${job.memoId}\` failed with a non-retryable error: ${errorMessage}. Please contact support.`, { parse_mode: 'MarkdownV2' });
                             }
                            result = { processed: false, reason: `payout_non_retryable: ${errorMessage}` };
                            break; // Exit loop for non-retryable errors
                        }

                        // Exponential backoff with jitter for retries
                        const delay = baseRetryDelay * Math.pow(2, attempt - 1) + Math.random() * 1000;
                        console.log(`[PAYOUT_JOB_RETRY] Retrying bet ${job.betId} in ${(delay / 1000).toFixed(1)}s...`);
                        await new Promise(resolve => setTimeout(resolve, delay));
                    }
                }
                // --- End payout retry logic ---
            } else {
                console.error(`[PROCESS_JOB] Unknown job type: ${job.type}`);
                result = { processed: false, reason: 'unknown_job_type' };
            }

            performanceMonitor.logRequest(result?.processed !== false); // Log success unless explicitly failed
            return result; // Return result from job processing

        } catch (error) { // Catch errors from the job logic itself (e.g., DB query in process_bet)
            performanceMonitor.logRequest(false);
            console.error(`[PROCESS_JOB] Unhandled error processing job ${jobKey}:`, error.message, error.stack);

            // Attempt to mark bet as failed if applicable and not already handled
            if (job.betId && job.type !== 'monitor_payment') {
                 const currentStatusCheck = await pool.query('SELECT status FROM bets WHERE id = $1', [job.betId]).then(r => r.rows[0]?.status);
                 if (!currentStatusCheck?.startsWith('error_') && !currentStatusCheck?.startsWith('completed_')) {
                     const errorStatus = `error_${job.type}_uncaught`;
                     await updateBetStatus(job.betId, errorStatus);
                     console.log(`[PROCESS_JOB] Set bet ${job.betId} status to ${errorStatus} due to uncaught job error.`);
                 }
            }
             // Rethrow or return error structure? Returning error reason is safer for the queue.
             return { processed: false, reason: `uncaught_job_error: ${error.message}` };
        } finally {
             // console.log(`[PROCESS_JOB] Finished: ${jobKey}`); // Optional debug log
            this.activeProcesses.delete(jobKey); // Ensure job key is removed
        }
    }


    // --- Internal Helper Methods (prefixed with _) ---

    // Main entry point for processing a detected signature payment
    async _processIncomingPayment(signature, walletType) {
        const logPrefix = `Sig ${signature.slice(0, 6)}...`; // Short prefix for logs

        // 1. Session-level duplicate check (quickest)
        if (processedSignaturesThisSession.has(signature)) {
            // console.log(`${logPrefix}: Already processed this session.`); // Reduce noise
            return { processed: false, reason: 'already_processed_session' };
        }

        // 2. Database-level duplicate check (more definitive)
        try {
            const exists = await pool.query(
                'SELECT 1 FROM bets WHERE paid_tx_signature = $1 LIMIT 1',
                [signature]
            );
            if (exists.rowCount > 0) {
                // console.log(`${logPrefix}: Already recorded in DB.`); // Reduce noise
                processedSignaturesThisSession.add(signature); // Cache if found in DB
                 this._cleanSignatureCache(); // Clean cache periodically
                return { processed: false, reason: 'exists_in_db' };
            }
        } catch (dbError) {
             console.error(`${logPrefix}: DB error checking paid_tx_signature: ${dbError.message}`);
             // Don't cache sig on DB error, could be transient
             return { processed: false, reason: 'db_check_error', error: dbError };
        }

        let tx;
        try {
             // 3. Transaction fetching (uses RateLimitedConnection implicitly via _getTransactionWithRetry)
             console.log(`${logPrefix}: Fetching transaction details...`);
             tx = await this._getTransactionWithRetry(signature);

             // Handle cases where transaction wasn't found or failed on-chain
             if (!tx) {
                 console.log(`${logPrefix}: Failed to fetch valid transaction after retries (not found or RPC error).`);
                 // Do NOT cache signature here, it might confirm later. Monitor will retry.
                 return { processed: false, reason: 'fetch_failed_or_not_confirmed' };
             }
             if (tx.meta?.err){
                 console.log(`${logPrefix}: Transaction failed on-chain, skipping. Error: ${JSON.stringify(tx.meta.err)}`);
                 processedSignaturesThisSession.add(signature); // Cache txs that failed on-chain
                  this._cleanSignatureCache();
                 return { processed: false, reason: 'onchain_failure' };
             }
             console.log(`${logPrefix}: Transaction fetched successfully.`);


             // 4. Memo extraction
             // console.log(`${logPrefix}: Extracting memo...`); // Reduce noise
             const memo = await this._extractMemoGuaranteed(tx, signature); // Uses findMemoInTx -> normalizeMemo
             if (!memo) {
                 console.log(`${logPrefix}: No valid bet memo found.`);
                 processedSignaturesThisSession.add(signature); // Cache if no memo found for this valid tx
                  this._cleanSignatureCache();
                 return { processed: false, reason: 'no_valid_memo' };
             }
             console.log(`${logPrefix}: Found memo "${memo}".`);

             // 5. Bet lookup (includes retries & cache)
             // console.log(`${logPrefix}: Looking up bet for memo "${memo}"...`); // Reduce noise
             const bet = await this._findBetGuaranteed(memo);
             if (!bet) {
                 console.log(`${logPrefix}: No 'awaiting_payment' bet found for memo "${memo}".`);
                 // Check if bet exists but in another state
                 const existingBetStatus = await pool.query('SELECT status FROM bets WHERE memo_id = $1 LIMIT 1', [memo]).then(r=>r.rows[0]?.status);
                 if (existingBetStatus && existingBetStatus !== 'awaiting_payment') {
                     console.log(`${logPrefix}: Bet for memo "${memo}" exists but status is '${existingBetStatus}'. Caching signature.`);
                     processedSignaturesThisSession.add(signature); // Cache signature if bet is already processed/errored
                      this._cleanSignatureCache();
                     return { processed: false, reason: 'bet_already_processed_or_errored' };
                 }
                 // If no bet found at all, don't cache sig, could be unrelated tx or bet created later
                 return { processed: false, reason: 'no_matching_bet_found' };
             }
             console.log(`${logPrefix}: Found bet ID ${bet.id} for memo "${memo}".`);

             // 6. Process payment within DB transaction (validation, status update, wallet link)
             console.log(`${logPrefix}: Processing payment validation for bet ID ${bet.id}...`);
             const processResult = await this._processPaymentGuaranteed(bet, signature, walletType, tx); // Pass tx object

              // 7. Queue for game logic only if payment processing was fully successful
              if (processResult.processed) {
                  console.log(`${logPrefix}: Payment verified for bet ${bet.id}. Queuing game logic.`);
                  // Fetch full bet details again to pass to the queue job
                   const finalBetDetails = await pool.query('SELECT * FROM bets WHERE id = $1', [bet.id]).then(res => res.rows[0]);
                   if (finalBetDetails && finalBetDetails.status === 'payment_verified') {
                       await this._queueBetProcessing(finalBetDetails); // Pass the full details
                   } else {
                       console.error(`${logPrefix}: CRITICAL! Failed to retrieve full bet details or status mismatch for Bet ID ${bet.id} after commit. Cannot queue game processing. Status: ${finalBetDetails?.status ?? 'Not Found'}`);
                       // Status should be payment_verified, potential inconsistency
                       await updateBetStatus(bet.id, 'error_post_verify_fetch');
                   }
              } else {
                   console.log(`${logPrefix}: Payment validation/processing failed for bet ${bet.id}. Reason: ${processResult.reason}`);
                   // Status should have been updated within _processPaymentGuaranteed on failure
              }

              return processResult; // Return the result from _processPaymentGuaranteed

        } catch (error) {
             // Catch errors from fetching, memo extraction, bet finding, or processing steps
              console.error(`${logPrefix}: Critical error during incoming payment processing pipeline: ${error.message}`, error.stack);
              // Decide whether to cache based on the error type and retryability
              const isDefinitiveFailure = !isRetryableError(error) || error.reason === 'onchain_failure' || error.reason === 'no_valid_memo' || error.reason?.includes('_final');
              if (isDefinitiveFailure) {
                  processedSignaturesThisSession.add(signature);
                   this._cleanSignatureCache();
                  console.log(`${logPrefix}: Caching signature due to non-retryable/definitive error.`);
              }
              // Update bet status if we have a bet ID context from the error and it failed during processing
              if (error.betId && typeof error.betId === 'number') { // Assuming error might carry betId context
                  await updateBetStatus(error.betId, 'error_processing_exception');
              }
              return { processed: false, reason: `processing_pipeline_error: ${error.message}` };
        }
    }

    // Helper to get transaction with retries (uses RateLimitedConnection)
    async _getTransactionWithRetry(signature) {
        try {
            // Use the rate-limited connection directly. Retries/rotation handled internally.
            const tx = await solanaConnection.getParsedTransaction(signature, {
                maxSupportedTransactionVersion: 0, // Fetch as Versioned Tx if possible
                commitment: 'confirmed' // Use 'confirmed' to ensure reasonable finality before processing
            });
            // Returns null if not found after connection's internal retries
             return tx;
        } catch (error) {
             // Log the final error if the RateLimitedConnection gives up after its retries
             console.error(`Sig ${signature?.slice(0,6)}: Final error from solanaConnection.getParsedTransaction: ${error.message}`);
             // Treat as fetch failure
             return null;
        }
    }

    // Helper to extract memo using multiple methods (uses findMemoInTx)
    async _extractMemoGuaranteed(tx, signature) {
        if (!tx) return null;
        // Directly use the existing robust function which includes normalization
        return findMemoInTx(tx, signature);
    }

    // Helper to find bet with retries and caching, using SKIP LOCKED
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
                // Use FOR UPDATE SKIP LOCKED to handle potential concurrency without waiting indefinitely
                const bet = await client.query(`
                    SELECT id, user_id, chat_id, game_type, bet_details, expected_lamports, status, expires_at, fees_paid, priority, memo_id
                    FROM bets
                    WHERE memo_id = $1 AND status = 'awaiting_payment'
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                `, [memo]).then(r => r.rows[0]);

                if (bet) {
                    // console.log(`Memo ${memo}: DB lookup successful on attempt ${i + 1}.`); // Reduce noise
                    this.memoCache.set(cacheKey, bet); // Add to cache
                    setTimeout(() => { // Set TTL for cache entry deletion
                         const currentCached = this.memoCache.get(cacheKey);
                         if (currentCached && currentCached.id === bet.id && Date.now() - currentCached.timestamp >= this.cacheTTL) {
                             this.memoCache.delete(cacheKey);
                         }
                    }, this.cacheTTL + 1000); // Check slightly after TTL
                    return bet; // Return the found bet
                }
                // If bet is null here, it means row was skipped (locked by another process) or doesn't exist in 'awaiting_payment' state.
                // console.log(`Memo ${memo}: DB lookup attempt ${i + 1} found no unlocked awaiting_payment bet.`); // Reduce noise

            } catch (error) {
                console.error(`Memo ${memo}: DB lookup attempt ${i + 1} failed: ${error.message}`);
                if (i === attempts - 1 || !isRetryableError(error)) {
                    // Don't throw, just return null after final/non-retryable attempt fails
                    console.error(`Memo ${memo}: Final DB lookup attempt failed.`);
                    return null; // Indicate failure to find
                }
                // Wait before retrying DB connection/query
                await new Promise(r => setTimeout(r, 300 * (i + 1)));
            } finally {
                 if (client) client.release(); // Release client for this attempt
            }
             // Small delay even if query succeeded but returned no rows (e.g., row was locked or not found)
             if (i < attempts - 1) {
                 await new Promise(r => setTimeout(r, 200 * (i + 1)));
             }
        }
        // console.log(`Memo ${memo}: Bet not found after ${attempts} attempts.`); // Reduce noise
        return null; // Return null if not found after all attempts
    }


    // Helper to process payment within a DB transaction (validation, status update, wallet link)
    async _processPaymentGuaranteed(bet, signature, walletType, tx) {
        const client = await pool.connect();
        try {
            await client.query('BEGIN');

            // 1. Verify bet status again within transaction for atomicity (lock row)
            const currentBet = await client.query(`
                SELECT status
                FROM bets WHERE id = $1
                FOR UPDATE
            `, [bet.id]).then(r => r.rows[0]);

            // Handle cases where bet disappeared or status changed concurrently
            if (!currentBet) {
                console.warn(`Sig ${signature?.slice(0,6)}: Bet ID ${bet.id} disappeared during final lock.`);
                await client.query('ROLLBACK');
                // Don't cache signature - the bet might reappear or have been processed by another node briefly
                return { processed: false, reason: 'bet_disappeared_final' };
            }
            if (currentBet.status !== 'awaiting_payment') {
                 console.warn(`Sig ${signature?.slice(0,6)}: Bet ID ${bet.id} status was '${currentBet.status}', not 'awaiting_payment', during final lock.`);
                 await client.query('ROLLBACK');
                 processedSignaturesThisSession.add(signature); // Cache sig - bet definitely processed/errored
                  this._cleanSignatureCache();
                 return { processed: false, reason: 'bet_already_processed_final' };
            }

            // --- Additional Validation Checks within Transaction ---
            // 2a. Analyze transaction amounts
            const { transferAmount, payerAddress: detectedPayer } = analyzeTransactionAmounts(tx, walletType);
            if (transferAmount <= 0n) {
                console.warn(`Sig ${signature?.slice(0,6)}: Bet ID ${bet.id}. No SOL transfer found in final check.`);
                // Update status to error and COMMIT this failure state
                await client.query(`UPDATE bets SET status = 'error_no_transfer_found', processed_at = NOW() WHERE id = $1`, [bet.id]);
                await client.query('COMMIT'); // COMMIT the error status
                processedSignaturesThisSession.add(signature); // Cache on definitive validation failure
                 this._cleanSignatureCache();
                return { processed: false, reason: 'no_transfer_found_final' };
            }

            // 2b. Validate amount sent vs expected (with tolerance)
            const expectedAmount = BigInt(bet.expected_lamports);
            const tolerance = BigInt(process.env.PAYMENT_TOLERANCE_LAMPORTS || '5000'); // 0.000005 SOL tolerance default
            if (transferAmount < (expectedAmount - tolerance) || transferAmount > (expectedAmount + tolerance)) {
                console.warn(`Sig ${signature?.slice(0,6)}: Bet ID ${bet.id}. Amount mismatch. Expected ~${expectedAmount}, got ${transferAmount}.`);
                await client.query(`UPDATE bets SET status = 'error_payment_mismatch', processed_at = NOW() WHERE id = $1`, [bet.id]);
                await client.query('COMMIT'); // Commit the error status
                processedSignaturesThisSession.add(signature); // Cache on definitive validation failure
                 this._cleanSignatureCache();
                // Send Telegram message *after* releasing DB connection
                safeSendMessage(bet.chat_id, `⚠️ Payment amount mismatch for bet \`${bet.memo_id}\`. Expected ${Number(expectedAmount)/LAMPORTS_PER_SOL} SOL, received ${Number(transferAmount)/LAMPORTS_PER_SOL} SOL. Bet cancelled.`, { parse_mode: 'MarkdownV2' }).catch(e => console.error("TG Send Error:", e.message));
                return { processed: false, reason: 'amount_mismatch_final' };
            }

            // 2c. Validate transaction time vs bet expiry
            const txTime = tx.blockTime ? new Date(tx.blockTime * 1000) : null;
            const expiryTime = new Date(bet.expires_at);
             // Add a grace period to expiry (e.g., 60 seconds) to account for block confirmation time variance
             const expiryGracePeriodMs = parseInt(process.env.PAYMENT_EXPIRY_GRACE_MS || '60000', 10); // 60s default
             const effectiveExpiryTime = new Date(expiryTime.getTime() + expiryGracePeriodMs);

            if (!txTime) {
                console.warn(`Sig ${signature?.slice(0,6)}: Bet ID ${bet.id}. Could not determine blockTime for final expiry check. Allowing processing.`);
                // Allow processing but log warning. Could be made stricter if needed.
            } else if (txTime > effectiveExpiryTime) {
                console.warn(`Sig ${signature?.slice(0,6)}: Bet ID ${bet.id}. Payment received at ${txTime.toISOString()} after effective expiry ${effectiveExpiryTime.toISOString()}.`);
                await client.query(`UPDATE bets SET status = 'error_payment_expired', processed_at = NOW() WHERE id = $1`, [bet.id]);
                await client.query('COMMIT'); // Commit error status
                processedSignaturesThisSession.add(signature); // Cache on definitive validation failure
                 this._cleanSignatureCache();
                // Send Telegram message *after* releasing DB connection
                safeSendMessage(bet.chat_id, `⚠️ Payment for bet \`${bet.memo_id}\` received after expiry time. Bet cancelled.`, { parse_mode: 'MarkdownV2' }).catch(e => console.error("TG Send Error:", e.message));
                return { processed: false, reason: 'expired_final' };
            }
            // --- End Additional Validation Checks ---


            // 3. Mark bet as 'payment_verified' (if all checks passed)
            await client.query(`
                UPDATE bets
                SET status = 'payment_verified',
                    paid_tx_signature = $1,
                    processed_at = NOW() -- Record verification time
                WHERE id = $2 AND status = 'awaiting_payment' -- Final check
            `, [signature, bet.id]);

            // 4. Link wallet if possible and not already linked recently
             const payer = detectedPayer || getPayerFromTransaction(tx)?.toBase58();
             if (payer) {
                 const currentWallet = await getLinkedWallet(bet.user_id); // Check cache/DB
                 // Link only if different from current or not linked yet
                  if (!currentWallet || currentWallet !== payer) {
                     try {
                         await linkUserWallet(bet.user_id, payer); // Use the existing helper within the transaction
                     } catch (walletError) {
                          // linkUserWallet already logs errors, just note it here
                          console.warn(`Sig ${signature?.slice(0,6)}: Bet ID ${bet.id}. Non-critical issue linking wallet for payer "${payer}" during verification: ${walletError.message}`);
                      }
                  } else {
                      // Wallet already linked and matches, optionally update last_used_at?
                       // await client.query('UPDATE wallets SET last_used_at = NOW() WHERE user_id = $1', [bet.user_id]); // Optional
                  }
             } else {
                  console.warn(`Sig ${signature?.slice(0,6)}: Bet ID ${bet.id}. Could not identify valid payer address to link wallet.`);
             }

            // Commit the transaction (marks paid, potentially links wallet)
            await client.query('COMMIT');
            console.log(`Sig ${signature?.slice(0,6)}: Bet ID ${bet.id} successfully processed payment and marked as 'payment_verified'.`);

            // Add signature to session cache ONLY AFTER successful commit
            processedSignaturesThisSession.add(signature);
             this._cleanSignatureCache(); // Clean cache periodically

            return { processed: true }; // Indicate success to the caller (_processIncomingPayment)

        } catch (error) { // Catch errors during the DB transaction
            console.error(`Sig ${signature?.slice(0,6)}: Error during final payment processing DB TXN for Bet ID ${bet.id}: ${error.message}`, error.stack);
            await client.query('ROLLBACK').catch((rbError) => console.error(`Rollback failed: ${rbError.message}`));
            // Add context for outer catch in _processIncomingPayment
            error.betId = bet.id; // Add context if possible
             // Don't cache signature on transaction failure, might be recoverable DB issue
            throw error; // Re-throw error to be caught by _processIncomingPayment
        } finally {
            client.release();
        }
    }

    // Helper method to queue bet processing *after* payment verification
    async _queueBetProcessing(bet) {
        // console.log(`Payment verified for bet ${bet.id} (${bet.memo_id}). Queuing for game processing.`); // Reduce noise
        await this.addPaymentJob({
            type: 'process_bet',
            betId: bet.id,
            priority: 1, // High priority for game logic after payment is confirmed
            // Pass necessary details if processPaidBet needs them directly, though fetching by ID is safer
            // betDetails: bet // Example, but fetching fresh data is better
        });
    }

    // Helper to periodically clean the signature cache
     _cleanSignatureCache() {
         // Simple check: clean if size exceeds max by a margin
         const cleanupThreshold = MAX_PROCESSED_SIGNATURES + (MAX_PROCESSED_SIGNATURES * 0.1); // e.g., 11000 if max is 10000
         if (processedSignaturesThisSession.size > cleanupThreshold) {
              console.log(`[CACHE] Clearing processed signatures cache (reached ${processedSignaturesThisSession.size}/${MAX_PROCESSED_SIGNATURES})`);
              // More sophisticated cleanup could remove oldest entries instead of clear all
              processedSignaturesThisSession.clear();
         }
     }

    // --- REMOVED Placeholder processIncomingPayment method ---
    // The call in processJob now correctly uses _processIncomingPayment

} // *** END: GuaranteedPaymentProcessor class ***

// Instantiate the payment processor
const paymentProcessor = new GuaranteedPaymentProcessor();


// --- Payment Monitoring Loop ---
let isMonitorRunning = false; // Flag to prevent concurrent monitor runs
const botStartupTime = Math.floor(Date.now() / 1000); // Timestamp in seconds
let monitorIntervalId = null; // Holds the setInterval ID

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
        // Optional: Throttle based on Payment Processor Queue Load
         const paymentQueueLoad = (paymentProcessor.highPriorityQueue.size + paymentProcessor.normalQueue.size +
                                   paymentProcessor.highPriorityQueue.pending + paymentProcessor.normalQueue.pending);
         const monitorThrottleMs = parseInt(process.env.MONITOR_THROTTLE_MS_PER_ITEM || '50', 10);
         const maxMonitorThrottle = parseInt(process.env.MONITOR_MAX_THROTTLE_MS || '5000', 10);
         const throttleDelay = Math.min(maxMonitorThrottle, paymentQueueLoad * monitorThrottleMs);
         if (throttleDelay > 100) {
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
            const walletAddress = wallet.address;
            if (!walletAddress) {
                console.warn(`[Monitor] Skipping wallet type ${wallet.type} - address not configured.`);
                continue;
            }

             // Add small random delay/jitter before each wallet check
             const jitter = Math.random() * (parseInt(process.env.MONITOR_WALLET_JITTER_MS || '1500', 10)); // 0-1.5s jitter default
             await new Promise(resolve => setTimeout(resolve, jitter));

            let signaturesForWallet = [];
            try {
                const fetchLimit = parseInt(process.env.MONITOR_FETCH_LIMIT || '25', 10);
                const options = { limit: fetchLimit, commitment: 'confirmed' }; // Fetch confirmed signatures
                // console.log(`[Monitor] Checking ${wallet.type} wallet (${walletAddress.slice(0,6)}...) for latest ${options.limit} signatures.`); // Reduce noise

                // Use the RateLimitedConnection directly
                signaturesForWallet = await solanaConnection.getSignaturesForAddress(
                    new PublicKey(walletAddress),
                    options
                );

                if (!signaturesForWallet || signaturesForWallet.length === 0) {
                    // console.log(`[Monitor] No recent confirmed signatures found for ${walletAddress.slice(0,6)}...`); // Reduce noise
                    continue; // Skip to next wallet
                }

                signaturesFoundThisCycle += signaturesForWallet.length;

                // Filter out transactions known to have failed on-chain or occurred before bot reasonably started
                const startupBufferSeconds = 600; // Check 10 mins prior to startup for safety
                const recentSignatures = signaturesForWallet.filter(sigInfo => {
                    // Filter based on on-chain error (if present)
                    if (sigInfo.err) {
                        // console.log(`[Monitor] Skipping failed TX ${sigInfo.signature.slice(0,6)}...`); // Reduce noise
                        if (!processedSignaturesThisSession.has(sigInfo.signature)) {
                             processedSignaturesThisSession.add(sigInfo.signature); // Cache known failures
                             paymentProcessor._cleanSignatureCache(); // Clean cache periodically
                        }
                        return false;
                    }
                    // Filter based on blockTime (if available) vs bot startup time
                     // Only skip if blockTime is definitively *before* the relevant window
                    if (sigInfo.blockTime && sigInfo.blockTime < (botStartupTime - startupBufferSeconds)) {
                        return false;
                    }
                    return true;
                });


                if (recentSignatures.length === 0) continue;

                // Process oldest first within the fetched batch
                recentSignatures.reverse();

                for (const sigInfo of recentSignatures) {
                    // Check session cache first (quickest check)
                    if (processedSignaturesThisSession.has(sigInfo.signature)) continue;
                    // Check if already being processed actively
                    const jobKey = `monitor_payment:${sigInfo.signature}`;
                     if (paymentProcessor.activeProcesses.has(jobKey)) continue;

                    // Queue the signature for full processing (_processIncomingPayment)
                    // console.log(`[Monitor] Queuing signature ${sigInfo.signature.slice(0,10)}... for ${wallet.type} wallet.`); // Reduce noise
                    signaturesQueuedThisCycle++;
                    await paymentProcessor.addPaymentJob({ // Use await to allow throttling effect
                        type: 'monitor_payment',
                        signature: sigInfo.signature,
                        walletType: wallet.type,
                        priority: wallet.priority, // Normal priority for initial check
                    });
                } // End loop through signatures

            } catch (error) {
                // Catch errors during signature fetching for a specific wallet
                // RateLimitedConnection handles RPC errors, this might catch PublicKey errors etc.
                console.error(`[Monitor] Error fetching/processing signatures for wallet ${walletAddress}:`, error.message);
                performanceMonitor.logRequest(false);
            }
        } // End loop through wallets

    } catch (err) {
        // General error handling for the monitor loop itself
        console.error('❌ MonitorPayments Error in main try block:', err);
        performanceMonitor.logRequest(false);
    } finally {
        isMonitorRunning = false; // Release the lock
        const duration = Date.now() - startTime;
        // Only log detailed completion if something was found/queued or took significant time
        if (signaturesFoundThisCycle > 0 || duration > 1500) {
             console.log(`[Monitor] Cycle completed in ${duration}ms. Found:${signaturesFoundThisCycle}. Queued:${signaturesQueuedThisCycle}.`);
        }
    }
}


// --- SOL Sending Function ---
/**
 * Sends SOL to a recipient, handling priority fees and confirmation.
 * Relies on RateLimitedConnection for underlying RPC calls.
 * @param {string | PublicKey} recipientPublicKey - The recipient's address.
 * @param {bigint} amountLamports - The amount to send in lamports.
 * @param {'coinflip' | 'race'} gameType - Determines which private key to use for sending.
 * @returns {Promise<{success: boolean, signature?: string, error?: string}>} Result object.
 */
async function sendSol(recipientPublicKey, amountLamports, gameType) {
    // ... (implementation remains largely unchanged - uses Keypair, builds TX, adds priority fee, uses sendAndConfirmTransaction) ...
    // Key changes: Ensure robust error handling and logging.
    const operationId = `sendSol-${gameType}-${Date.now().toString().slice(-6)}`;
    // console.log(`[${operationId}] Initiating. Recipient: ${recipientPublicKey}, Amount: ${amountLamports}`); // Reduce noise

    const privateKeyEnvVar = gameType === 'coinflip' ? 'BOT_PRIVATE_KEY' : 'RACE_BOT_PRIVATE_KEY';
    const privateKey = process.env[privateKeyEnvVar];
    if (!privateKey) {
        console.error(`[${operationId}] ❌ ERROR: Missing private key env var ${privateKeyEnvVar}.`);
        return { success: false, error: `Missing private key for ${gameType}` };
    }

    let recipientPubKey;
    try {
        recipientPubKey = (typeof recipientPublicKey === 'string')
            ? new PublicKey(recipientPublicKey)
            : recipientPublicKey;
        if (!(recipientPubKey instanceof PublicKey)) throw new Error("Invalid recipient public key type");
    } catch (e) {
        console.error(`[${operationId}] ❌ ERROR: Invalid recipient address format: "${recipientPublicKey}". Error: ${e.message}`);
        return { success: false, error: `Invalid recipient address: ${e.message}` };
    }

    const amountToSend = BigInt(amountLamports);
    if (amountToSend <= 0n) {
        console.error(`[${operationId}] ❌ ERROR: Payout amount ${amountLamports} is zero or negative.`);
        return { success: false, error: 'Payout amount is zero or negative' };
    }
    const amountSOL = Number(amountToSend) / LAMPORTS_PER_SOL;
    // console.log(`[${operationId}] Amount: ${amountToSend} lamports (${amountSOL.toFixed(6)} SOL)`); // Reduce noise

    // Calculate dynamic priority fee
    const basePriorityFee = parseInt(process.env.PAYOUT_BASE_PRIORITY_FEE_MICROLAMPORTS || '1000', 10);
    const maxPriorityFee = parseInt(process.env.PAYOUT_MAX_PRIORITY_FEE_MICROLAMPORTS || '1000000', 10);
    const priorityFeeRate = parseFloat(process.env.PAYOUT_PRIORITY_FEE_RATE); // Already parsed/validated
    const calculatedFee = Math.floor(Number(amountToSend) * priorityFeeRate);
    const priorityFeeMicroLamports = Math.max(basePriorityFee, Math.min(calculatedFee, maxPriorityFee));
    // console.log(`[${operationId}] Calculated priority fee: ${priorityFeeMicroLamports} microLamports`); // Reduce noise

    try {
        const payerWallet = Keypair.fromSecretKey(bs58.decode(privateKey));
        // console.log(`[${operationId}] Payer wallet loaded: ${payerWallet.publicKey.toBase58()}`); // Reduce noise

        // Get latest blockhash (uses RateLimitedConnection internally)
        // console.log(`[${operationId}] Fetching latest blockhash ('confirmed')...`); // Reduce noise
        const latestBlockhash = await solanaConnection.getLatestBlockhash('confirmed');
        if (!latestBlockhash || !latestBlockhash.blockhash || !latestBlockhash.lastValidBlockHeight) {
            throw new Error('Failed to get valid latest blockhash object from RPC.');
        }
        // console.log(`[${operationId}] Got blockhash: ${latestBlockhash.blockhash}, Last Valid Block Height: ${latestBlockhash.lastValidBlockHeight}`); // Reduce noise

        // Build transaction
        const transaction = new Transaction({
            recentBlockhash: latestBlockhash.blockhash,
            feePayer: payerWallet.publicKey
        });
        transaction.add(
            ComputeBudgetProgram.setComputeUnitPrice({ microLamports: priorityFeeMicroLamports })
        );
        // Optional: Add compute limit if needed, though usually not for simple transfers
        // const computeUnits = parseInt(process.env.PAYOUT_COMPUTE_UNITS || '200000', 10); // Default 200k for safety
        // transaction.add(ComputeBudgetProgram.setComputeUnitLimit({ units: computeUnits }));

        transaction.add(
            SystemProgram.transfer({
                fromPubkey: payerWallet.publicKey,
                toPubkey: recipientPubKey,
                lamports: amountToSend
            })
        );
        // console.log(`[${operationId}] Transaction built.`); // Reduce noise

        // Send and confirm transaction with timeout
        const confirmationTimeoutMs = parseInt(process.env.PAYOUT_CONFIRM_TIMEOUT_MS || '60000', 10); // 60s timeout default
        // console.log(`[${operationId}] Calling sendAndConfirmTransaction (Timeout: ${confirmationTimeoutMs / 1000}s)...`); // Reduce noise

        const signature = await sendAndConfirmTransaction(
            solanaConnection, // Use the rate-limited connection
            transaction,
            [payerWallet],
            {
                commitment: 'confirmed', // Wait for 'confirmed'
                skipPreflight: false,    // Perform preflight checks
                // maxRetries for sendAndConfirm relates to resending/status checks, not initial RPC call retries (handled by connection)
                maxRetries: parseInt(process.env.SCT_MAX_RETRIES || '3', 10),
                preflightCommitment: 'confirmed', // Match commitment
                lastValidBlockHeight: latestBlockhash.lastValidBlockHeight, // Provide block height
                confirmTransactionInitialTimeout: confirmationTimeoutMs // Set timeout directly if library supports it (check version)
            }
        );
        // Note: If confirmTransactionInitialTimeout isn't supported, use Promise.race as before.
        // Assuming it is supported for simplicity here.

        // If it succeeds:
        console.log(`[${operationId}] SUCCESS! ✅ Sent ${amountSOL.toFixed(6)} SOL to ${recipientPubKey.toBase58()}. TX: ${signature.slice(0,10)}...`);
        return { success: true, signature };

    } catch (error) {
        console.error(`[${operationId}] ❌ SEND FAILED. Error:`, error.message);
        // Log simulation logs if available
         if (error.logs) {
             console.error(`[${operationId}] Simulation Logs:`);
             error.logs.slice(-10).forEach(log => console.error(`   -> ${log}`)); // Log last 10 lines
         } else if (error.toString().includes("Transaction simulation failed")) {
             // Try to fetch transaction to get logs if simulation failed but error object lacks logs
             // (This is complex and may not always work)
         }

        // Determine more specific error cause if possible
        const errorMsg = error.message.toLowerCase();
        let returnError = error.message; // Default to original message

        if (errorMsg.includes('insufficient funds')) {
            returnError = 'Insufficient funds in payout wallet.';
            // TODO: Add admin notification for low balance
        } else if (errorMsg.includes('blockhash not found') || errorMsg.includes('blockhash expired')) {
            returnError = 'Transaction expired (blockhash not found). Retryable.';
        } else if (errorMsg.includes('transaction was not confirmed') || errorMsg.includes('timed out')) {
            returnError = `Transaction confirmation timeout (${confirmationTimeoutMs / 1000}s). May succeed later.`;
            // Don't assume permanent failure here, might need manual check or reconciliation later
        } else if (errorMsg.includes('custom program error') || errorMsg.includes('invalid account data')) {
            returnError = `Permanent chain error: ${error.message}`;
        } else if (isRetryableError(error)) { // Check against general retryable network/RPC errors
             returnError = `Temporary network/RPC error: ${error.message}`;
        } else {
            returnError = `Send/Confirm error: ${error.message}`; // Generic fallback
        }

        return { success: false, error: returnError };
    }
}


// --- Game Processing Logic ---

// Routes a paid bet to the correct game handler after payment verification
// Ensures bet status is updated atomically before calling game logic
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
            return; // Exit if already processed or state is wrong
        }

        // Update status to 'processing_game' within the transaction
        await client.query(
            'UPDATE bets SET status = $1 WHERE id = $2',
            ['processing_game', bet.id]
        );
        await client.query('COMMIT'); // Commit status change and release lock
        // console.log(`Bet ${bet.id} (${bet.memo_id}) status set to processing_game.`); // Reduce noise

        // Call the appropriate game handler *after* releasing the lock
        if (bet.game_type === 'coinflip') {
            await handleCoinflipGame(bet); // Defined in Part 3
        } else if (bet.game_type === 'race') {
            await handleRaceGame(bet); // Defined in Part 3
        } else {
            console.error(`❌ Unknown game type '${bet.game_type}' for bet ${bet.id}`);
            await updateBetStatus(bet.id, 'error_unknown_game'); // Update status outside transaction
        }
    } catch (error) {
        console.error(`❌ Error during game processing setup for bet ${bet.id} (${bet.memo_id}):`, error.message);
        if (client) { // Rollback if transaction was started
            try { await client.query('ROLLBACK'); } catch (rbError) { console.error("Rollback failed:", rbError); }
        }
        // Mark bet with error status - do this outside the failed transaction
        await updateBetStatus(bet.id, 'error_processing_setup');
    } finally {
        if (client) client.release(); // Ensure client is always released
    }
}

// --- Utility Functions ---

// Calculates payout amount, applying house edge. Returns BigInt lamports.
function calculatePayout(betLamports, gameType, gameDetails = {}) {
    // Ensure input is BigInt - Note: If called internally, might already be BigInt
    // Adding defensive BigInt conversion anyway.
    try {
         betLamports = BigInt(betLamports);
    } catch (e) {
         // Log error if input cannot be converted (e.g., NULL, non-numeric string)
         console.error(`DEBUG_PAYOUT: Error converting betLamports input '${betLamports}' to BigInt: ${e.message}`);
         return 0n; // Cannot proceed if input is invalid
    }

    let payoutLamports = 0n;
    let multiplier = NaN; // Initialize multiplier defensively

    // --- Log Inputs ---
    console.log(`DEBUG_PAYOUT: calculatePayout started. betLamports=${betLamports} (Type: ${typeof betLamports}), gameType=${gameType}`);
    // --- End Log Inputs ---

    if (gameType === 'coinflip') {
        // Calculate multiplier based on config
        multiplier = 2.0 - GAME_CONFIG.coinflip.houseEdge;

        // --- Log Multiplier Info ---
        // Log the calculated multiplier and the house edge value it used
        console.log(`DEBUG_PAYOUT: Coinflip multiplier calculated as ${multiplier} (using houseEdge=${GAME_CONFIG.coinflip.houseEdge}, Type: ${typeof GAME_CONFIG.coinflip.houseEdge})`);
        // --- End Log Multiplier Info ---

        // Use try-catch specifically around calculations that might produce NaN
        try {
            // Ensure multiplier is a valid number before proceeding
            if (isNaN(multiplier)) {
                console.error(`DEBUG_PAYOUT: Coinflip multiplier is NaN! Check GAME_CONFIG.coinflip.houseEdge.`);
                throw new Error("Multiplier is NaN"); // Throw to prevent BigInt(NaN)
            }

            // Perform calculation steps
            const betLamportsAsNumber = Number(betLamports); // Convert BigInt to Number for multiplication
            const intermediate = betLamportsAsNumber * multiplier;
            const floored = Math.floor(intermediate);

            // --- Log Intermediate Steps ---
            console.log(`DEBUG_PAYOUT: Coinflip Calculation Steps: Number(betLamports)=${betLamportsAsNumber}, intermediate=${intermediate}, floored=${floored}`);
            // --- End Log Intermediate Steps ---

            // Explicitly check if the result is NaN before BigInt conversion
            if (isNaN(floored)) {
                console.error(`DEBUG_PAYOUT: Calculated 'floored' value is NaN! Inputs: betLamportsAsNumber=${betLamportsAsNumber}, multiplier=${multiplier}`);
                 payoutLamports = 0n; // Assign 0n if NaN occurs, preventing BigInt(NaN)
            } else {
                payoutLamports = BigInt(floored); // Convert final number to BigInt
            }
        } catch (calcError) {
            console.error(`DEBUG_PAYOUT: Error during coinflip calculation: ${calcError.message}`);
            payoutLamports = 0n; // Assign 0n on any calculation error
        }

    } else if (gameType === 'race' && gameDetails.odds) {
        // Calculate multiplier for race
         multiplier = gameDetails.odds * (1.0 - GAME_CONFIG.race.houseEdge);
        // --- Log Multiplier Info ---
         console.log(`DEBUG_PAYOUT: Race multiplier calculated as ${multiplier} (using odds=${gameDetails.odds}, houseEdge=${GAME_CONFIG.race.houseEdge}, Type: ${typeof GAME_CONFIG.race.houseEdge})`);
        // --- End Log Multiplier Info ---
         try {
              if (isNaN(multiplier)) {
                  console.error(`DEBUG_PAYOUT: Race multiplier is NaN! Check GAME_CONFIG.race.houseEdge or gameDetails.odds.`);
                  throw new Error("Race Multiplier is NaN");
              }
              const betLamportsAsNumber = Number(betLamports);
              const intermediate = betLamportsAsNumber * multiplier;
              const floored = Math.floor(intermediate);
              // --- Log Intermediate Steps ---
              console.log(`DEBUG_PAYOUT: Race Calculation Steps: Number(betLamports)=${betLamportsAsNumber}, intermediate=${intermediate}, floored=${floored}`);
              // --- End Log Intermediate Steps ---
              if (isNaN(floored)) {
                   console.error(`DEBUG_PAYOUT: Calculated 'floored' value for race is NaN!`);
                   payoutLamports = 0n;
              } else {
                   payoutLamports = BigInt(floored);
              }
         } catch (calcError) {
              console.error(`DEBUG_PAYOUT: Error during race calculation: ${calcError.message}`);
              payoutLamports = 0n;
         }

    } else {
        console.error(`Cannot calculate payout: Unknown game type "${gameType}" or missing race odds.`);
        // payoutLamports remains 0n (initial value)
        return 0n; // Return 0 explicitly if type is unknown
    }

    // Ensure final payout is not negative
    const finalResult = payoutLamports > 0n ? payoutLamports : 0n;

    // --- Log Final Result ---
    console.log(`DEBUG_PAYOUT: calculatePayout returning: ${finalResult} (Type: ${typeof finalResult})`);
    // --- End Log Final Result ---
    return finalResult;
}

// Fetches user's display name (username or first name) from Telegram
// Includes basic MarkdownV2 escaping.
async function getUserDisplayName(chat_id, user_id) {
    // ... (implementation remains unchanged - uses getChatMember, cache optional, includes escape) ...
     try {
         // TODO: Implement caching for chat members if needed to reduce API calls
         // const memberCacheKey = `member-${chat_id}-${user_id}`;
         // let cachedMember = memberCache.get(memberCacheKey);
         // if (!cachedMember) { ... fetch and cache ... }

         const chatMember = await bot.getChatMember(chat_id, user_id);
         const user = chatMember.user;
         const name = user.username ? `@${user.username}` : (user.first_name || `User ${String(user_id).substring(0, 6)}...`);

         // Basic sanitization for Markdown V2
         const escapeMarkdown = (text) => {
             if (!text) return '';
             return text.replace(/([_*\[\]()~`>#+\-=|{}.!])/g, '\\$1');
         }
          return escapeMarkdown(name); // Escape the chosen name

     } catch (e) {
         // Handle specific errors like user not found or permissions
         if (e.response && (e.response.statusCode === 400 || e.response.statusCode === 403)) {
             if (e.message.toLowerCase().includes('user not found')) {
                  console.warn(`User ${user_id} not found in chat ${chat_id}.`);
             } else {
                  console.warn(`Permission error getting user ${user_id} in chat ${chat_id}: ${e.message}`);
             }
         } else {
              console.warn(`Couldn't get username/name for user ${user_id} in chat ${chat_id}:`, e.message);
         }
         // Fallback, ensure it's also escaped
          const fallbackName = `User ${String(user_id).substring(0, 6)}...`;
          return fallbackName.replace(/([_*\[\]()~`>#+\-=|{}.!])/g, '\\$1');
     }
}

// --- End of Part 2b ---
// index.js - Revised Part 3

// (Code continues directly from the end of Part 2b)


// --- Game Logic Implementation ---
// Implementations for the stubs defined in Part 2b

/**
 * Handles the Coinflip game logic after payment is verified.
 * Determines win/loss, calculates payout, queues payout job or updates status.
 * @param {object} bet - The bet object from the database.
 */
async function handleCoinflipGame(bet) {
    const { id: betId, user_id, chat_id, bet_details, expected_lamports, memo_id } = bet;
    const choice = bet_details.choice; // 'heads' or 'tails'
    const config = GAME_CONFIG.coinflip;
    const logPrefix = `CF Bet ${betId} (${memo_id.slice(0, 6)}...)`; // Short prefix

    // --- Determine Outcome (Simplified & Corrected House Edge Application) ---
    const houseWinRoll = Math.random();
    let result;
    let isHouseWin = houseWinRoll < config.houseEdge;

    if (isHouseWin) {
        result = (choice === 'heads') ? 'tails' : 'heads'; // House forces opposite result
        console.log(`${logPrefix}: House edge triggered.`);
    } else {
        result = (Math.random() < 0.5) ? 'heads' : 'tails'; // Fair 50/50 flip
    }
    // --- End Outcome Determination ---

    const win = (result === choice) && !isHouseWin; // Player wins only if result matches choice AND house edge didn't trigger

    const payoutLamports = win ? calculatePayout(BigInt(expected_lamports), 'coinflip') : 0n;
    const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;

    let displayName = await getUserDisplayName(chat_id, user_id); // Already escaped

    if (win) {
        console.log(`${logPrefix}: ${displayName} WON ${payoutSOL.toFixed(6)} SOL! Choice: ${choice}, Result: ${result}`);
        const winnerAddress = await getLinkedWallet(user_id);

        if (!winnerAddress) {
            console.warn(`${logPrefix}: Winner ${displayName} has no linked wallet.`);
            await updateBetStatus(betId, 'completed_win_no_wallet'); // Final status
            await safeSendMessage(chat_id,
                `🎉 ${displayName}, you won the coinflip \\(Result: *${result}*\\) but have no wallet linked\\!\n` + // Escape () and *
                `Your payout of ${payoutSOL.toFixed(6)} SOL is waiting\\. Place another bet \\(any amount\\) to link your wallet and receive pending payouts\\.`, // Escape . and ()
                { parse_mode: 'MarkdownV2' }
            );
            return;
        }

        // Send "processing payout" message and queue the actual payout job
        try {
            // Update bet status BEFORE queueing payout job
            const statusUpdated = await updateBetStatus(betId, 'processing_payout');
            if (!statusUpdated) {
                 // This should ideally not happen if processPaidBet locked correctly, but handle defensively
                console.error(`${logPrefix}: CRITICAL! Failed to update status to 'processing_payout' before queueing! Aborting payout queue.`);
                 await safeSendMessage(chat_id, `⚠️ Internal error preparing your payout for bet \`${memo_id}\`\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
                 // Consider setting an error status here if update failed
                 await updateBetStatus(betId, 'error_payout_status_update');
                return;
            }

            // Send message *after* status update, *before* queueing
            await safeSendMessage(chat_id,
                `🎉 ${displayName}, you won ${payoutSOL.toFixed(6)} SOL\\!\n` + // Escape !
                `Result: *${result}*\n\n` + // Escape *
                `💸 Processing payout to your linked wallet\\.\\.\\.`, // Escape .
                { parse_mode: 'MarkdownV2' }
            );

            // Inside handleCoinflipGame -> if (win) block -> try block:

         // Inside handleCoinflipGame -> if (win) block -> try block:

         // Queue the payout job
         console.log(`${logPrefix}: Queueing payout job.`);
         // --- ADD LOGGING HERE ---
         const amountString = payoutLamports.toString();
         console.log(`DEBUG_PAYOUT: Queuing payout job. betId=<span class="math-inline">\{betId\}, amountString\='</span>{amountString}', originalPayoutLamports=${payoutLamports}`);
         // --- END LOGGING ---
         await paymentProcessor.addPaymentJob({
             type: 'payout',
             betId,
             recipient: winnerAddress,
             //amount: payoutLamports.toString(), // Use the logged string
             amount: amountString, // Use the logged string
             gameType: 'coinflip',
             priority: 2,
             chatId: chat_id,
             displayName: displayName,
             memoId: memo_id,
             result: result
         });
         console.log(`${logPrefix}: Payout job queued successfully.`);

        } catch (e) { // Catch errors during the pre-queue process (DB update, TG message)
            console.error(`${logPrefix}: Error preparing/queueing payout info:`, e);
             // Attempt to revert status if possible, though might be difficult
             await updateBetStatus(betId, 'error_payout_preparation');
            await safeSendMessage(chat_id,
                `⚠️ Error occurred while processing your coinflip win for bet \`${memo_id}\`\\. Please contact support\\.`,
                { parse_mode: 'MarkdownV2' }
            );
        }

    } else { // Loss (including house edge wins)
        console.log(`${logPrefix}: ${displayName} LOST. Choice: ${choice}, Result: ${result}`);
        await updateBetStatus(betId, 'completed_loss'); // Final status
        await safeSendMessage(chat_id,
            `❌ ${displayName}, you lost the coinflip\\!\n` + // Escape !
            `You guessed *${choice}* but the result was *${result}*\\. Better luck next time\\!`, // Escape . and !
            { parse_mode: 'MarkdownV2' }
        );
    }
}


/**
 * Handles the Race game logic after payment is verified.
 * Determines winner, calculates payout, queues payout job or updates status.
 * @param {object} bet - The bet object from the database.
 */
async function handleRaceGame(bet) {
    const { id: betId, user_id, chat_id, bet_details, expected_lamports, memo_id } = bet;
    const chosenHorseName = bet_details.horse;
    const config = GAME_CONFIG.race;
    const logPrefix = `Race Bet ${betId} (${memo_id.slice(0, 6)}...)`;

    const horses = [ // Keep horse definitions consistent
        { name: 'Yellow', emoji: '🟡', odds: 1.1, baseProb: 0.25 }, { name: 'Orange', emoji: '🟠', odds: 2.0, baseProb: 0.20 },
        { name: 'Blue',   emoji: '🔵', odds: 3.0, baseProb: 0.15 }, { name: 'Cyan',   emoji: '💧', odds: 4.0, baseProb: 0.12 },
        { name: 'White',  emoji: '⚪️', odds: 5.0, baseProb: 0.09 }, { name: 'Red',    emoji: '🔴', odds: 6.0, baseProb: 0.07 },
        { name: 'Black',  emoji: '⚫️', odds: 7.0, baseProb: 0.05 }, { name: 'Pink',   emoji: '🌸', odds: 8.0, baseProb: 0.03 },
        { name: 'Purple', emoji: '🟣', odds: 9.0, baseProb: 0.02 }, { name: 'Green',  emoji: '🟢', odds: 10.0, baseProb: 0.01 },
        { name: 'Silver', emoji: '💎', odds: 15.0, baseProb: 0.01 } // Ensure probabilities sum close to 1
    ];
    // Optional: Check probability sum during startup or here
    // const totalBaseProb = horses.reduce((sum, h) => sum + h.baseProb, 0);
    // if (Math.abs(totalBaseProb - 1.0) > 0.001) console.warn(`Race base probabilities sum to ${totalBaseProb}, not 1.0.`);

    // --- Determine Winning Horse (Weighted selection + House Edge) ---
    let winningHorse = null;
    const houseEdgeTarget = 1.0 - config.houseEdge; // e.g., 0.98
    const randomRoll = Math.random(); // 0.0 to < 1.0

    if (randomRoll < houseEdgeTarget) {
        // Player win branch - scale roll and probabilities
        const playerWinRoll = randomRoll / houseEdgeTarget; // Normalize roll to 0.0 to < 1.0 within player win space
        let cumulativeProb = 0;
        // Normalize base probabilities to sum to 1 for selection
         const totalProbSum = horses.reduce((sum, h) => sum + h.baseProb, 0); // Recalculate just in case
         if (totalProbSum <= 0) { // Avoid division by zero
             console.error(`${logPrefix}: Total horse probability is zero or negative! Defaulting winner.`);
             winningHorse = horses[0]; // Default to first horse on error
         } else {
             for (const horse of horses) {
                 cumulativeProb += (horse.baseProb / totalProbSum); // Use normalized probability
                 if (playerWinRoll <= cumulativeProb) {
                     winningHorse = horse;
                     break;
                 }
             }
             // Fallback if float precision issue occurs
              if (!winningHorse) winningHorse = horses[horses.length - 1];
         }

    } else {
        // House win branch - still pick a "winning" horse for display, but player loses
        console.log(`${logPrefix}: House edge triggered.`);
         let cumulativeProb = 0;
         const houseRoll = Math.random(); // Separate roll to pick the visual winner
         const totalProbSum = horses.reduce((sum, h) => sum + h.baseProb, 0);
         if (totalProbSum <= 0) { winningHorse = horses[0]; } else { // Avoid division by zero
             for (const horse of horses) {
                 cumulativeProb += (horse.baseProb / totalProbSum);
                 if (houseRoll <= cumulativeProb) {
                     winningHorse = horse;
                     break;
                 }
             }
              if (!winningHorse) winningHorse = horses[horses.length - 1]; // Fallback
         }
    }
    // --- End Winning Horse Determination ---

    const isHouseWin = randomRoll >= houseEdgeTarget; // Determine if house edge actually won
    const playerWins = !isHouseWin && (chosenHorseName.toLowerCase() === winningHorse.name.toLowerCase());

    const payoutLamports = playerWins ? calculatePayout(BigInt(expected_lamports), 'race', winningHorse) : 0n;
    const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;

    let displayName = await getUserDisplayName(chat_id, user_id); // Already escaped

    // Send race commentary messages
    try {
        await safeSendMessage(chat_id, `🐎 Race starting for bet \`${memo_id}\`\\! ${displayName} bet on *${chosenHorseName}*\\!`, { parse_mode: 'MarkdownV2' }); // Escape !, *
        await new Promise(resolve => setTimeout(resolve, 2000)); // Pause
        await safeSendMessage(chat_id, "🚦 And they're off\\!"); // Escape !
        await new Promise(resolve => setTimeout(resolve, 3000)); // Pause
        await safeSendMessage(chat_id,
            `🏆 The winner is\\.\\.\\. ${winningHorse.emoji} *${winningHorse.name}*\\! 🏆`, // Escape ., !
            { parse_mode: 'MarkdownV2' }
        );
        await new Promise(resolve => setTimeout(resolve, 1000)); // Short pause before result
    } catch (e) {
        console.error(`${logPrefix}: Error sending race commentary:`, e);
    }

    // Process win/loss
    if (playerWins) {
        console.log(`${logPrefix}: ${displayName} WON ${payoutSOL.toFixed(6)} SOL! Horse: ${chosenHorseName}, Winner: ${winningHorse.name}`);
        const winnerAddress = await getLinkedWallet(user_id);

        if (!winnerAddress) {
            console.warn(`${logPrefix}: Winner ${displayName} has no linked wallet.`);
            await updateBetStatus(betId, 'completed_win_no_wallet'); // Final status
            await safeSendMessage(chat_id,
                `🎉 ${displayName}, your horse *${chosenHorseName}* won the race\\!\n`+ // Escape !
                `Your payout of ${payoutSOL.toFixed(6)} SOL is waiting\\. Place another bet \\(any amount\\) to link your wallet and receive pending payouts\\.`, // Escape ., ()
                { parse_mode: 'MarkdownV2' }
            );
            return;
        }

        // Queue payout job
        try {
             // Update bet status BEFORE queueing payout job
             const statusUpdated = await updateBetStatus(betId, 'processing_payout');
             if (!statusUpdated) {
                 console.error(`${logPrefix}: CRITICAL! Failed to update status to 'processing_payout' before queueing! Aborting payout queue.`);
                  await safeSendMessage(chat_id, `⚠️ Internal error preparing your payout for bet \`${memo_id}\`\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
                  await updateBetStatus(betId, 'error_payout_status_update');
                 return;
             }

             // Send message after status update, before queueing
             await safeSendMessage(chat_id,
                 `🎉 ${displayName}, your horse *${chosenHorseName}* won\\!\n` + // Escape !
                 `Payout: ${payoutSOL.toFixed(6)} SOL\n\n` +
                 `💸 Processing payout to your linked wallet\\.\\.\\.`, // Escape .
                 { parse_mode: 'MarkdownV2' }
             );

             // Queue the job
             console.log(`${logPrefix}: Queueing payout job.`);
             await paymentProcessor.addPaymentJob({
                 type: 'payout',
                 betId,
                 recipient: winnerAddress,
                 amount: payoutLamports.toString(),
                 gameType: 'race',
                 priority: 2,
                 // Context for handlePayoutJob:
                 chatId: chat_id,
                 displayName: displayName, // Pass escaped name
                 memoId: memo_id,
                 horseName: chosenHorseName,
                 winningHorseName: winningHorse.name,
                 winningHorseEmoji: winningHorse.emoji
             });
              console.log(`${logPrefix}: Payout job queued successfully.`);

        } catch (e) { // Catch errors during pre-queue process
            console.error(`${logPrefix}: Error queuing payout info:`, e);
             await updateBetStatus(betId, 'error_payout_preparation'); // Update status to reflect error
            await safeSendMessage(chat_id,
                `⚠️ Error occurred while processing your race win for bet \`${memo_id}\`\\. Please contact support\\.`,
                { parse_mode: 'MarkdownV2' }
            );
        }

    } else { // Loss
        console.log(`${logPrefix}: ${displayName} LOST. Choice: ${chosenHorseName}, Winner: ${winningHorse.name}`);
        await updateBetStatus(betId, 'completed_loss'); // Final status
        await safeSendMessage(chat_id,
            `❌ ${displayName}, your horse *${chosenHorseName}* lost the race\\!\n` + // Escape !
            `Winner: ${winningHorse.emoji} *${winningHorse.name}*\\. Better luck next time\\!`, // Escape ., !
            { parse_mode: 'MarkdownV2' }
        );
    }
}


/**
 * Handles the actual payout transaction after a win is confirmed.
 * Called by the paymentProcessor queue. Calls sendSol and handles results.
 * MUST re-throw errors if sendSol fails retryably, so processJob can retry.
 * @param {object} job - The payout job details from the queue.
 */
async function handlePayoutJob(job) {
    const { betId, recipient, amount, gameType, chatId, displayName, memoId } = job; // Destructure needed info
    const logPrefix = `Payout Job Bet ${betId} (${memoId?.slice(0, 6)}...)`;

    console.log(`DEBUG_PAYOUT: handlePayoutJob received job. amount='<span class="math-inline">\{amount\}', type\=</span>{typeof amount}`);
    const payoutAmountLamports = BigInt(amount);
    if (payoutAmountLamports <= 0n) {
        console.error(`${logPrefix}: ❌ Payout amount is zero or negative (${payoutAmountLamports}). Skipping.`);
        await updateBetStatus(betId, 'error_payout_zero_amount');
        await safeSendMessage(chatId, `⚠️ There was an issue calculating the payout for bet \`${memoId}\` \\(amount was zero\\)\\. Please contact support\\.`, { parse_mode: 'MarkdownV2' });
        // Do not throw here, this is a final state error
        return;
    }

    console.log(`${logPrefix}: Processing payout of ${Number(payoutAmountLamports) / LAMPORTS_PER_SOL} SOL to ${recipient}`);

    try {
        const sendResult = await sendSol(recipient, payoutAmountLamports, gameType);
        // console.log(`${logPrefix}: sendSol completed. Success: ${sendResult.success}`); // Reduce noise

        if (sendResult.success && sendResult.signature) {
            console.log(`${logPrefix}: ✅ Payout successful! TX: ${sendResult.signature.slice(0, 10)}...`);
            // Record payout in DB *first*
            const recorded = await recordPayout(betId, 'completed_win_paid', sendResult.signature);

            if (recorded) {
                await safeSendMessage(chatId,
                    `✅ Payout successful for bet \`${memoId}\`\\!\n` + // Escape !
                    `${(Number(payoutAmountLamports)/LAMPORTS_PER_SOL).toFixed(6)} SOL sent\\.\n` + // Escape .
                    `TX: \`https://solscan.io/tx/${sendResult.signature}\``, // Signature doesn't need escaping in code block
                    { parse_mode: 'MarkdownV2', disable_web_page_preview: true }
                );
            } else {
                 // This is a critical state - payment sent but DB update failed
                 console.error(`${logPrefix}: 🆘 CRITICAL! Payout sent (TX: ${sendResult.signature}) but FAILED to record in DB! Requires manual investigation.`);
                 await updateBetStatus(betId, 'error_payout_record_failed'); // Set specific error status
                 await safeSendMessage(chatId,
                     `⚠️ Your payout for bet \`${memoId}\` was sent successfully, but there was an issue recording it\\. Please contact support\\.\n` + // Escape .
                     `TX: \`https://solscan.io/tx/${sendResult.signature}\``,
                     { parse_mode: 'MarkdownV2', disable_web_page_preview: true }
                 );
                 // Do not throw here, this is a final error state needing investigation
            }
            // Success, job complete, do not throw.
            return;

        } else {
            // Payout failed via sendSol
            console.error(`${logPrefix}: ❌ Payout failed via sendSol. Error: ${sendResult.error}`);
            // Check if the error is potentially retryable from sendSol's perspective
             const retryableSendError = sendResult.error?.toLowerCase().includes('retryable') ||
                                        sendResult.error?.toLowerCase().includes('temporary') ||
                                        sendResult.error?.toLowerCase().includes('timeout'); // Basic check

             if (retryableSendError) {
                 console.log(`${logPrefix}: sendSol indicated a potentially retryable error. Re-throwing for processJob retry.`);
                 // Re-throw the error so the retry logic in processJob can catch it
                 throw new Error(sendResult.error || 'sendSol failed retryably');
             } else {
                 // Error seems permanent according to sendSol, update status and notify user
                  console.error(`${logPrefix}: sendSol indicated a permanent error. Marking bet as failed.`);
                  await updateBetStatus(betId, 'error_payout_send_failed'); // Final error state
                  await safeSendMessage(chatId,
                      `⚠️ Payout failed for bet \`${memoId}\`\\: ${sendResult.error?.replace(/([_*\[\]()~`>#+\-=|{}.!])/g, '\\$1')}\n` + // Escape error message
                      `The team has been notified\\. Please contact support if needed\\.`, // Escape .
                      { parse_mode: 'MarkdownV2' }
                  );
                  // Do not throw here, this is a final error state for this job.
                  return;
             }
        }
    } catch (error) { // Catch unexpected errors within handlePayoutJob itself
        console.error(`${logPrefix}: ❌ Unexpected error during payout job execution:`, error);
        // Attempt to update status if not already an error
         const currentStatus = await pool.query('SELECT status FROM bets WHERE id = $1', [betId]).then(r => r.rows[0]?.status);
         if (!currentStatus?.startsWith('error_')) {
             await updateBetStatus(betId, 'error_payout_job_exception');
         }
        await safeSendMessage(chatId,
            `⚠️ A technical error occurred during the payout process for bet \`${memoId}\`\\. Please contact support\\.`, // Escape .
            { parse_mode: 'MarkdownV2' }
        );
        // Re-throw the error to signal failure to processJob retry logic if applicable
        // (though exceptions here might be less likely to be retryable)
         throw error;
    }
}


// --- Telegram Bot Command Handlers ---

// Handles polling errors
bot.on('polling_error', (error) => {
    console.error(`❌ Polling error: ${error.code} - ${error.message}`);
    if (error.code === 'ETELEGRAM' && error.message.includes('409 Conflict')) {
        console.error("❌❌❌ FATAL: Conflict detected! Another bot instance might be running with the same token. Exiting.");
        // Attempt graceful shutdown before exiting on conflict
         shutdown('POLLING_CONFLICT', false).catch(() => process.exit(1));
         setTimeout(() => { console.error("Shutdown timed out after polling conflict. Forcing exit."); process.exit(1); }, 5000).unref();
    } else if (error.code === 'ECONNRESET') {
         console.warn("⚠️ Polling connection reset. Attempting to continue...");
    } else if (error.response && error.response.statusCode === 401) {
         console.error("❌❌❌ FATAL: Unauthorized (401). Check BOT_TOKEN. Exiting.");
         shutdown('BOT_TOKEN_INVALID', false).catch(() => process.exit(1));
         setTimeout(() => { console.error("Shutdown timed out after auth error. Forcing exit."); process.exit(1); }, 5000).unref();
    }
    // Add more specific handling if needed
});

// Handles webhook errors (e.g., connection issues to Telegram endpoint)
bot.on('webhook_error', (error) => {
     console.error(`❌ Webhook error: ${error.code} - ${error.message}`);
     // Consider attempting to switch to polling if webhook fails persistently? Complex.
});

// Handles general bot errors not caught elsewhere
bot.on('error', (error) => {
    console.error('❌ General Bot Error:', error);
    performanceMonitor.logRequest(false); // Log as failed request
});


/**
 * Central handler for all incoming messages.
 * Routes messages to specific command handlers via messageQueue.
 * @param {TelegramBot.Message} msg - The incoming message object.
 */
async function handleMessage(msg) {
    // Basic filtering already done in the 'on message' listener

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const messageText = msg.text; // Already checked that it exists
    const messageId = msg.message_id;

    try {
        // User Cooldown Check
        const now = Date.now();
        if (confirmCooldown.has(userId)) {
            const lastTime = confirmCooldown.get(userId);
            if (now - lastTime < cooldownInterval) {
                // console.log(`User ${userId} command ignored due to cooldown.`); // Optional debug log
                return; // Ignore command if user is on cooldown
            }
        }
        // Apply cooldown immediately if it looks like a command, before processing
        if (messageText.startsWith('/')) {
            confirmCooldown.set(userId, now); // Update last command time
        }

        // --- Command Routing ---
        // Use regex to handle commands with or without bot username, and capture arguments
        // Allows for multi-line arguments with the 's' flag
        const commandMatch = messageText.trim().match(/^\/(\w+)(?:@\w+)?(?:\s+(.*))?$/s);

        if (!commandMatch) return; // Not a command format

        const command = commandMatch[1].toLowerCase();
        const args = commandMatch[2]?.trim() || ''; // Arguments string, trimmed

        // Log received command for debugging (optional)
        // console.log(`Cmd: /${command} | User:${userId} | Chat:${chatId} | Args:"${args}"`);

        // Route to specific handler
        // Use object lookup for cleaner routing
        const commandHandlers = {
            'start': handleStartCommand,
            'coinflip': handleCoinflipCommand,
            'wallet': handleWalletCommand,
            'bet': (msg, args) => handleBetCommand(msg, args), // Pass args
            'race': handleRaceCommand,
            'betrace': (msg, args) => handleBetRaceCommand(msg, args), // Pass args
            'help': handleHelpCommand,
            'botstats': handleBotStatsCommand, // Admin command
            // Add other commands here
        };

        const handler = commandHandlers[command];

        if (handler) {
            // Special handling for admin commands
            if (command === 'botstats') {
                const adminIds = (process.env.ADMIN_USER_IDS || '').split(',');
                if (adminIds.includes(userId)) {
                    await handler(msg); // Pass only msg if args not needed by handler signature
                } else {
                     console.log(`User ${userId} attempted unauthorized /${command} command.`);
                     // Optionally send a silent failure or a generic "Unknown command" message
                }
            } else {
                 // Call regular command handler, passing args if its signature expects it
                 // Need to adjust this if handlers have different signatures
                 if (handler.length === 2) { // Check if handler function expects 2 arguments (msg, args)
                     await handler(msg, args);
                 } else {
                     await handler(msg); // Assume handler only needs msg
                 }
            }
            performanceMonitor.logRequest(true); // Log successful command handling *attempt*
        } else {
            // console.log(`Ignoring unknown command: /${command}`); // Optional log
            // Optionally send "Unknown command" message
            // await safeSendMessage(chatId, `Unknown command: \`/${command}\``, { parse_mode: 'MarkdownV2'});
        }


    } catch (error) { // Catch errors within command handlers or routing
        console.error(`❌ Error processing msg ${messageId} from user ${userId} in chat ${chatId}: "${messageText}"`, error);
        performanceMonitor.logRequest(false); // Log error
        try {
            // Use safeSendMessage for error reporting
             // Avoid showing detailed internal errors to the user
            await safeSendMessage(chatId, "⚠️ An unexpected error occurred while processing your command\\. Please try again later or contact support\\.", { parse_mode: 'MarkdownV2'});
        } catch (tgError) {
            // safeSendMessage already logs errors
        }
    } finally {
        // Optional: Periodic cleanup of cooldown map (simple approach)
         if (Math.random() < 0.05) { // Run cleanup occasionally (e.g., 5% chance per message)
             const cutoff = Date.now() - 60000 * 5; // Remove entries older than 5 minutes
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


// --- Specific Command Handler Implementations ---

// Handles the /start command (Uses HTML)
async function handleStartCommand(msg) {
    const firstName = msg.from.first_name || 'there';
    // Basic HTML escaping for name
    const sanitizedFirstName = firstName.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    const welcomeText = `👋 Welcome, <b>${sanitizedFirstName}</b>!\n\n` +
                        `🎰 <b>Solana Gambles Bot</b>\n\n` +
                        `Use /coinflip or /race to see game options.\n` +
                        `Use /wallet to view your linked Solana wallet.\n` +
                        `Use /help to see all commands.\n\n` +
                        `<i>Remember to gamble responsibly!</i>`;
    const bannerUrl = 'https://i.ibb.co/9vDo58q/banner.gif';

    try {
        await bot.sendAnimation(msg.chat.id, bannerUrl, {
            caption: welcomeText,
            parse_mode: 'HTML'
        }).catch(async (animError) => {
             // If animation fails (e.g., URL invalid, Telegram issue), send text fallback
             console.warn("⚠️ Failed to send start animation, sending text fallback:", animError.message);
             await safeSendMessage(msg.chat.id, welcomeText, { parse_mode: 'HTML' });
        });
    } catch (fallbackError) {
         // Should be caught by safeSendMessage now
         // console.error("TG Send Error (start fallback):", fallbackError.message);
    }
}

// Handles the /coinflip command (shows instructions - Uses HTML)
async function handleCoinflipCommand(msg) {
    // ... (Implementation uses HTML, remains unchanged) ...
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
        console.error("Error INSIDE handleCoinflipCommand:", error);
        await safeSendMessage(msg.chat.id, "Sorry, couldn't display Coinflip info right now.");
    }
}

// Handles the /race command (shows instructions - Uses MarkdownV2)
async function handleRaceCommand(msg) {
    // ... (Implementation uses MarkdownV2, applies escaping fixes) ...
     const horses = [
         { name: 'Yellow', emoji: '🟡', odds: 1.1 }, { name: 'Orange', emoji: '🟠', odds: 2.0 },
         { name: 'Blue',   emoji: '🔵', odds: 3.0 }, { name: 'Cyan',   emoji: '💧', odds: 4.0 },
         { name: 'White',  emoji: '⚪️', odds: 5.0 }, { name: 'Red',    emoji: '🔴', odds: 6.0 },
         { name: 'Black',  emoji: '⚫️', odds: 7.0 }, { name: 'Pink',   emoji: '🌸', odds: 8.0 },
         { name: 'Purple', emoji: '🟣', odds: 9.0 }, { name: 'Green',  emoji: '🟢', odds: 10.0 },
         { name: 'Silver', emoji: '💎', odds: 15.0 }
     ];
     const escapeMarkdown = (text) => text.replace(/([_*\[\]()~`>#+\-=|{}.!])/g, '\\$1');

     let raceMessage = `🐎 *Horse Race Game* 🐎\n\nBet on the winning horse\\!\n\n*Available Horses \\& Approx Payout* \\(Bet x Odds After House Edge\\):\n`; // Escape !()
     horses.forEach(horse => {
         const effectiveMultiplier = (horse.odds * (1.0 - GAME_CONFIG.race.houseEdge)).toFixed(2);
         raceMessage += `\\- ${horse.emoji} *${escapeMarkdown(horse.name)}* \\(\\~${escapeMarkdown(effectiveMultiplier)}x Payout\\)\n`; // Escape -()~
     });

     const config = GAME_CONFIG.race;
     raceMessage += `\n*How to play:*\n` +
                       `1\\. Type \`/betrace amount horse_name\`\n` + // Escape .
                       `   \\(e\\.g\\., \`/betrace 0\\.1 Yellow\`\\)\n\n` + // Escape .()
                       `*Rules:*\n` +
                       `\\- Min Bet: ${escapeMarkdown(config.minBet.toString())} SOL\n` + // Escape -
                       `\\- Max Bet: ${escapeMarkdown(config.maxBet.toString())} SOL\n` + // Escape -
                       `\\- House Edge: ${escapeMarkdown((config.houseEdge * 100).toFixed(1))}% \\(applied to winnings\\)\n\n` + // Escape -()
                       `You will be given a wallet address and a *unique Memo ID*\\. Send the *exact* SOL amount with the memo to place your bet\\.`; // Escape .

     await safeSendMessage(msg.chat.id, raceMessage, { parse_mode: 'MarkdownV2' });
}


// Handles the /wallet command (shows linked wallet - Uses MarkdownV2)
async function handleWalletCommand(msg) {
    // ... (Implementation uses MarkdownV2, applies escaping fixes) ...
    const userId = String(msg.from.id);
    const walletAddress = await getLinkedWallet(userId); // Uses cache
    const escapeMarkdown = (text) => text ? text.replace(/([_*\[\]()~`>#+\-=|{}.!])/g, '\\$1') : ''; // Add check for null/undefined

    if (walletAddress) {
        await safeSendMessage(msg.chat.id,
            `🔗 Your linked Solana wallet:\n\`${escapeMarkdown(walletAddress)}\`\n\n`+ // Wallet address unlikely to need escape, but safe
            `Payouts will be sent here\\. It's linked automatically when you make your first paid bet\\.`, // Escape .
            { parse_mode: 'MarkdownV2' }
        );
    } else {
        await safeSendMessage(msg.chat.id,
            `🔗 No wallet linked yet\\.\n` + // Escape .
            `Place a bet and send the required SOL\\. Your sending wallet will be automatically linked for future payouts\\.`, // Escape .
             { parse_mode: 'MarkdownV2' }
        );
    }
}

// Handles /bet command (for Coinflip - Uses MarkdownV2)
async function handleBetCommand(msg, args) {
    // ... (Implementation uses MarkdownV2, applies escaping fixes) ...
      const match = args.trim().match(/^(\d+\.?\d*)\s+(heads|tails)/i);
      if (!match) {
          await safeSendMessage(msg.chat.id,
               `⚠️ Invalid format\\. Use: \`/bet <amount> <heads|tails>\`\n` + // Escape .
               `Example: \`/bet 0\\.1 heads\``, // Escape .
               { parse_mode: 'MarkdownV2' }
          );
          return;
      }

      const userId = String(msg.from.id);
      const chatId = String(msg.chat.id);
      const config = GAME_CONFIG.coinflip;
      const escapeMarkdown = (text) => text ? text.replace(/([_*\[\]()~`>#+\-=|{}.!])/g, '\\$1') : '';

      const betAmount = parseFloat(match[1]);
      if (isNaN(betAmount) || betAmount < config.minBet || betAmount > config.maxBet) {
          await safeSendMessage(chatId,
               `⚠️ Invalid bet amount\\. Please bet between ${escapeMarkdown(config.minBet.toString())} and ${escapeMarkdown(config.maxBet.toString())} SOL\\.`, // Escape .
               { parse_mode: 'MarkdownV2' }
          );
          return;
      }

      const userChoice = match[2].toLowerCase();
      const memoId = generateMemoId('CF'); // Generates V1 format
      const expectedLamports = BigInt(Math.round(betAmount * LAMPORTS_PER_SOL));
      const expiresAt = new Date(Date.now() + config.expiryMinutes * 60 * 1000);

      const saveResult = await savePendingBet(
          userId, chatId, 'coinflip', { choice: userChoice }, expectedLamports, memoId, expiresAt
      );

      if (!saveResult.success) {
          await safeSendMessage(chatId, `⚠️ Error registering bet: ${escapeMarkdown(saveResult.error || 'Unknown error')}\\. Please try the command again\\.`, { parse_mode: 'MarkdownV2' });
          return;
      }

      const betAmountString = betAmount.toFixed(Math.max(2, (betAmount.toString().split('.')[1] || '').length));
      // Construct message with correct MarkdownV2 escaping
      const message = `✅ Coinflip bet registered\\! \\(ID: \`${memoId}\`\\)\n\n` + // Escape !()
                      `You chose: *${escapeMarkdown(userChoice)}*\n` +
                      `Amount: *${escapeMarkdown(betAmountString)} SOL*\n\n` +
                      `➡️ Send *exactly ${escapeMarkdown(betAmountString)} SOL* to:\n` +
                      `\`${escapeMarkdown(process.env.MAIN_WALLET_ADDRESS)}\`\n\n` + // Address unlikely to need escape, but safe
                      `📎 *Include MEMO:* \`${memoId}\`\n\n` + // Memo ID is safe format
                      `⏱️ This request expires in ${config.expiryMinutes} minutes\\.\n\n` + // Escape .
                      `*IMPORTANT:* Send from your own wallet \\(not an exchange\\)\\. Ensure you include the memo correctly\\.`; // Escape .()

      await safeSendMessage(chatId, message, { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}


// Handles /betrace command (Uses MarkdownV2)
async function handleBetRaceCommand(msg, args) {
    // ** Restored original functionality around where line 1454 used to be **
    // ** Applied MarkdownV2 escaping fixes **
    const match = args.trim().match(/^(\d+\.?\d*)\s+([\w\s]+)/i); // Allow spaces in horse name temporarily
    if (!match) {
        await safeSendMessage(msg.chat.id,
            `⚠️ Invalid format\\. Use: \`/betrace <amount> <horse_name>\`\n`+ // Escape .
            `Example: \`/betrace 0\\.1 Yellow\``, // Escape .
            { parse_mode: 'MarkdownV2' }
        );
        return;
    }

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const config = GAME_CONFIG.race;
    const escapeMarkdown = (text) => text ? text.replace(/([_*\[\]()~`>#+\-=|{}.!])/g, '\\$1') : '';

    const betAmount = parseFloat(match[1]);
    if (isNaN(betAmount) || betAmount < config.minBet || betAmount > config.maxBet) {
        await safeSendMessage(chatId,
            `⚠️ Invalid bet amount\\. Please bet between ${escapeMarkdown(config.minBet.toString())} and ${escapeMarkdown(config.maxBet.toString())} SOL\\.`, // Escape .
            { parse_mode: 'MarkdownV2' }
        );
        return;
    }

    // Validate horse selection (case-insensitive, trim whitespace)
     const chosenHorseNameInput = match[2].trim();
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
            `⚠️ Invalid horse name: "${escapeMarkdown(chosenHorseNameInput)}"\\. Please choose from the list in /race\\.`, { parse_mode: 'MarkdownV2' } // Escape .
        );
        return;
    }

    const memoId = generateMemoId('RA'); // Generates V1 format
    const expectedLamports = BigInt(Math.round(betAmount * LAMPORTS_PER_SOL));
    const expiresAt = new Date(Date.now() + config.expiryMinutes * 60 * 1000);

    // --- This is where the misplaced method definition used to be ---
    // --- The correct code continues below ---

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
    // Correct escaping for MarkdownV2
    const potentialPayoutSOL = escapeMarkdown((Number(potentialPayoutLamports) / LAMPORTS_PER_SOL).toFixed(6));
    const betAmountString = escapeMarkdown(betAmount.toFixed(Math.max(2, (betAmount.toString().split('.')[1] || '').length)));

    // Construct message with correct MarkdownV2 escaping
    const message = `✅ Race bet registered\\! \\(ID: \`${memoId}\`\\)\n\n` + // Escape !()
                    `You chose: ${chosenHorse.emoji} *${escapeMarkdown(chosenHorse.name)}*\n` +
                    `Amount: *${betAmountString} SOL*\n` +
                    `Potential Payout: \\~${potentialPayoutSOL} SOL\n\n`+ // Escape ~
                    `➡️ Send *exactly ${betAmountString} SOL* to:\n` +
                    `\`${escapeMarkdown(process.env.RACE_WALLET_ADDRESS)}\`\n\n` + // Address unlikely to need escape, but safe
                    `📎 *Include MEMO:* \`${memoId}\`\n\n` + // Memo ID is safe format
                    `⏱️ This request expires in ${config.expiryMinutes} minutes\\.\n\n` + // Escape .
                    `*IMPORTANT:* Send from your own wallet \\(not an exchange\\)\\. Ensure you include the memo correctly\\.`; // Escape .()

    await safeSendMessage(chatId, message, { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}


// Handles /help command (Uses MarkdownV2)
async function handleHelpCommand(msg) {
    // ... (Implementation uses MarkdownV2, applies escaping fixes) ...
     const helpText = `*Solana Gambles Bot Commands* 🎰\n\n` +
                         `/start \\- Show welcome message\n` + // Escape -
                         `/help \\- Show this help message\n\n` + // Escape -
                         `*Games:*\n` +
                         `/coinflip \\- Show Coinflip game info \\& how to bet\n` + // Escape -&
                         `/race \\- Show Horse Race game info \\& how to bet\n\n` + // Escape -
                         `*Betting:*\n` +
                         `/bet <amount> <heads|tails> \\- Place a Coinflip bet\n` + // Escape -
                         `/betrace <amount> <horse\\_name> \\- Place a Race bet\n\n` + // Escape -_
                         `*Wallet:*\n` +
                         `/wallet \\- View your linked Solana wallet for payouts\n\n` + // Escape -
                         `*Support:* If you encounter issues, please contact support\\.`; // Escape .

     await safeSendMessage(msg.chat.id, helpText, { parse_mode: 'MarkdownV2' });
}

// Example Admin Command: /botstats (Uses MarkdownV2)
async function handleBotStatsCommand(msg) {
    // ... (Implementation uses MarkdownV2, applies escaping fixes) ...
     // Note: Assumes ADMIN_USER_IDS check happened in handleMessage
     const processor = paymentProcessor;
     const connectionStats = typeof solanaConnection?.getRequestStats === 'function' ? solanaConnection.getRequestStats() : null;
     const escapeMarkdown = (text) => String(text).replace(/([_*\[\]()~`>#+\-=|{}.!])/g, '\\$1');

     // Safely access queue sizes
     const messageQueueSize = messageQueue?.size || 0;
     const messageQueuePending = messageQueue?.pending || 0;
     const telegramSendQueueSize = telegramSendQueue?.size || 0;
     const telegramSendQueuePending = telegramSendQueue?.pending || 0;
     const paymentHighPriQueueSize = processor?.highPriorityQueue?.size || 0;
     const paymentHighPriQueuePending = processor?.highPriorityQueue?.pending || 0;
     const paymentNormalQueueSize = processor?.normalQueue?.size || 0;
     const paymentNormalQueuePending = processor?.normalQueue?.pending || 0;

     let statsMsg = `*Bot Statistics* \\(v2\\.2\\-revised\\)\n\n`; // Escape .()
     statsMsg += `*Uptime:* ${escapeMarkdown(Math.floor(process.uptime() / 60))} minutes\n`;
     statsMsg += `*Performance:* Req:${performanceMonitor.requests}, Err:${performanceMonitor.errors}\n`;
     statsMsg += `*Queues:*\n`;
     statsMsg += `  \\- Msg: P:${messageQueueSize} A:${messageQueuePending}\n`; // Escape -
     statsMsg += `  \\- TG Send: P:${telegramSendQueueSize} A:${telegramSendQueuePending}\n`; // Escape -
     statsMsg += `  \\- Pay High: P:${paymentHighPriQueueSize} A:${paymentHighPriQueuePending}\n`; // Escape -
     statsMsg += `  \\- Pay Norm: P:${paymentNormalQueueSize} A:${paymentNormalQueuePending}\n`; // Escape -
     statsMsg += `*Caches:*\n`;
     statsMsg += `  \\- Wallets: ${walletCache.size}\n`; // Escape -
     statsMsg += `  \\- Processed Sigs: ${processedSignaturesThisSession.size} / ${MAX_PROCESSED_SIGNATURES}\n`; // Escape -

     if (connectionStats) {
         statsMsg += `*Solana Connection:*\n`;
         statsMsg += `  \\- Endpoint: ${escapeMarkdown(connectionStats.status.currentEndpoint || 'N/A')} \\(Idx ${connectionStats.status.currentEndpointIndex ?? 'N/A'}\\)\n`; // Escape -()
         statsMsg += `  \\- Q:${connectionStats.status.queueSize ?? 'N/A'}, A:${connectionStats.status.activeRequests ?? 'N/A'}\n`; // Escape -
         statsMsg += `  \\- Consecutive RL: ${connectionStats.status.consecutiveRateLimits ?? 'N/A'}\n`; // Escape -
         statsMsg += `  \\- Last RL: ${escapeMarkdown(connectionStats.status.lastRateLimit || 'None')}\n`; // Escape -
         statsMsg += `  \\- Total Req: S:${connectionStats.stats.totalRequestsSucceeded ?? 'N/A'}, F:${connectionStats.stats.totalRequestsFailed ?? 'N/A'}\n`; // Escape -
         statsMsg += `  \\- RL Events: ${connectionStats.stats.rateLimitEvents ?? 'N/A'}\n`; // Escape -
         statsMsg += `  \\- Rotations: ${connectionStats.stats.endpointRotations ?? 'N/A'}\n`; // Escape -
         statsMsg += `  \\- Success Rate: ${escapeMarkdown(connectionStats.stats.successRate || 'N/A')}\n`; // Escape -
     } else {
         statsMsg += `*Solana Connection:* Stats unavailable\\.\n`; // Escape .
     }

     await safeSendMessage(msg.chat.id, statsMsg, { parse_mode: 'MarkdownV2' });
}


// --- Server Startup & Shutdown Logic ---

// Encapsulated Webhook Setup Logic
async function setupTelegramWebhook() {
    // ... (Implementation remains unchanged - uses env vars, sets webhook) ...
     if (process.env.RAILWAY_ENVIRONMENT && process.env.RAILWAY_PUBLIC_DOMAIN) {
         const webhookUrl = `https://${process.env.RAILWAY_PUBLIC_DOMAIN}${webhookPath}`;
         console.log(`Attempting to set webhook to: ${webhookUrl}`);
         let attempts = 0;
         const maxAttempts = 3;
         while (attempts < maxAttempts) {
             try {
                 // Ensure any existing webhook is cleared first
                  await bot.deleteWebHook({ drop_pending_updates: true }).catch(e => console.warn(`Ignoring error during pre-webhook delete: ${e.message}`)); // Ignore delete errors
                 await bot.setWebHook(webhookUrl, {
                     max_connections: parseInt(process.env.WEBHOOK_MAX_CONN || '10', 10), // Sensible default
                     allowed_updates: ["message", "callback_query"], // Only process needed updates
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
    // ... (Implementation remains unchanged - checks webhook, starts/stops polling) ...
     try {
         const info = await bot.getWebHookInfo();
         if (!info || !info.url) { // Start polling only if webhook is not set
              if (bot.isPolling()) {
                  console.log("ℹ️ Bot is already polling.");
                  return;
              }
             console.log("ℹ️ Webhook not set, starting bot polling...");
             await bot.deleteWebHook({ drop_pending_updates: true }).catch(e => console.warn(`Ignoring error during pre-polling delete: ${e.message}`)); // Ensure no residual webhook
             // Start polling - ensure options are passed correctly if needed
              await bot.startPolling({ polling: { interval: parseInt(process.env.POLLING_INTERVAL_MS || '300', 10), /* other options */ } });
             console.log("✅ Bot polling started successfully");
         } else {
             console.log(`ℹ️ Webhook is set (${info.url}), polling will not be started.`);
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
         // Handle other potential errors (like invalid token during getWebhookInfo)
          if (err.response && err.response.statusCode === 401) {
             console.error("❌❌❌ FATAL: Unauthorized (401) checking webhook/polling. Check BOT_TOKEN. Exiting.");
              shutdown('BOT_TOKEN_INVALID_STARTUP', false).catch(() => process.exit(1));
              setTimeout(() => { console.error("Shutdown timed out after auth error. Forcing exit."); process.exit(1); }, 5000).unref();
         }
     }
}

// Encapsulated Payment Monitor Start Logic
function startPaymentMonitor() {
    // ... (Implementation remains unchanged - uses setInterval, calls monitorPayments) ...
     if (monitorIntervalId) {
          console.log("ℹ️ Payment monitor already running.");
          return; // Prevent multiple intervals
     }
     const monitorIntervalSeconds = parseInt(process.env.MONITOR_INTERVAL_SECONDS || '35', 10); // Slightly faster default? Adjust as needed.
     if (monitorIntervalSeconds < 10) {
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
     const initialDelay = parseInt(process.env.MONITOR_INITIAL_DELAY_MS || '5000', 10);
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
const shutdown = async (signal, isRailwayRotation = false) => {
    console.log(`\n🛑 ${signal} received, ${isRailwayRotation ? 'initiating container rotation procedure' : 'shutting down gracefully'}...`);

    if (isRailwayRotation) {
        // For Railway rotations: stop accepting new connections quickly, allow existing to finish briefly.
        console.log("Railway container rotation: Closing server, stopping monitor, allowing brief queue drain.");
         if (monitorIntervalId) { clearInterval(monitorIntervalId); monitorIntervalId = null; }
         if (server) { server.close(() => console.log("- Server closed for rotation.")); }
        // Allow a very short time for critical in-flight requests before Railway likely terminates
         await Promise.race([
             Promise.all([ // Only wait for payment queues briefly
                  paymentProcessor.highPriorityQueue.onIdle(),
                  paymentProcessor.normalQueue.onIdle()
             ]),
             new Promise(resolve => setTimeout(resolve, parseInt(process.env.RAILWAY_ROTATION_DRAIN_MS || '5000', 10))) // 5s max wait
         ]).catch(e => console.warn("Ignoring error/timeout during rotation drain:", e.message));
        console.log("Rotation procedure complete from app perspective.");
        // Don't explicitly exit process here for rotations.
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
        monitorIntervalId = null; // Clear ID
        console.log("- Stopped payment monitor interval.");
    }
    try {
        if (server) {
            console.log("- Closing Express server...");
            await new Promise((resolve, reject) => {
                server.close((err) => {
                    if (err) {
                         console.error("⚠️ Error closing Express server:", err.message);
                         return reject(err); // Reject promise on error
                    }
                    console.log("- Express server closed.");
                    resolve(undefined);
                });
                // Add a timeout for server closing
                 setTimeout(() => reject(new Error('Server close timeout')), 5000).unref();
            });
        }

        // Stop Telegram listeners gracefully
         console.log("- Stopping Telegram listeners...");
         if (bot.isPolling()) {
             await bot.stopPolling({ cancel: false }).catch(e => console.warn("Ignoring error during polling stop:", e.message)); // Let queued updates process
             console.log("- Stopped Telegram polling.");
         }
         try {
             const webhookInfo = await bot.getWebHookInfo().catch(() => null); // Ignore errors getting info
             if (webhookInfo && webhookInfo.url) {
                 await bot.deleteWebHook({ drop_pending_updates: false }).catch(e => console.warn("Ignoring error during webhook delete:", e.message)); // Don't drop updates
                 console.log("- Removed Telegram webhook.");
             }
         } catch (whErr) { console.warn("⚠️ Error checking/removing webhook:", whErr.message); }

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
    // SIGTERM is often used by process managers (like Railway, Docker, systemd)
    const isRailway = !!process.env.RAILWAY_ENVIRONMENT; // Check if in Railway
    console.log(`SIGTERM received. Railway Environment: ${isRailway}`);
    // Trigger shutdown, potentially the rotation variant if Railway detected
    shutdown('SIGTERM', isRailway).catch((err) => {
        console.error("Error during SIGTERM shutdown:", err);
        process.exit(1); // Exit with error code if shutdown fails
    });
});

process.on('SIGINT', () => { // Ctrl+C
    console.log(`SIGINT received.`);
    // Trigger full shutdown for Ctrl+C
    shutdown('SIGINT', false).catch((err) => {
        console.error("Error during SIGINT shutdown:", err);
        process.exit(1);
    });
});

// Handle uncaught exceptions - last resort
process.on('uncaughtException', (err, origin) => {
    console.error(`🔥🔥🔥 Uncaught Exception at: ${origin}`, err);
    // Attempt a very quick shutdown, then force exit
    shutdown('UNCAUGHT_EXCEPTION', false).catch(() => {}); // Ignore shutdown errors here
    setTimeout(() => {
        console.error("Shutdown timed out after uncaught exception. Forcing exit.");
        process.exit(1);
    }, parseInt(process.env.SHUTDOWN_FAIL_TIMEOUT_MS || '8000', 10)).unref(); // Shorter timeout on fatal error
});

// Handle unhandled promise rejections
process.on('unhandledRejection', (reason, promise) => {
    console.error('🔥🔥🔥 Unhandled Rejection at:', promise, 'reason:', reason);
    // Depending on the severity, you might trigger a shutdown or just log
    // For now, just logging. If these become frequent, consider investigating or shutting down.
     // If reason indicates a critical failure, consider calling shutdown:
     // if (reason instanceof Error && reason.message.includes("CRITICAL")) {
     //     shutdown('UNHANDLED_REJECTION', false).catch(() => process.exit(1));
     // }
});

// --- Start the Application ---
const PORT = process.env.PORT || 3000;

// Start server immediately, listen on 0.0.0.0 for container compatibility
server = app.listen(PORT, "0.0.0.0", () => {
    console.log(`🚀 Server listening on port ${PORT}...`);

    // Start heavy initialization *after* server starts listening (quick health checks pass sooner)
    const initDelay = parseInt(process.env.INIT_DELAY_MS || '1000', 10);
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
                 console.log(`✅ Connected to Telegram as bot: ${me.username}`);
             } catch (tgError) {
                 console.error(`❌ Failed to verify Telegram connection after setup: ${tgError.message}`);
                 throw tgError; // Propagate error to main init catch block
             }

            console.log("  - Starting Payment Monitor...");
            startPaymentMonitor(); // Starts the interval and initial run

            // Optional: Adjust Solana Connection Concurrency after initial startup load
            const concurrencyAdjustDelay = parseInt(process.env.CONCURRENCY_ADJUST_DELAY_MS || '20000', 10); // 20s default
            const targetConcurrency = parseInt(process.env.RPC_TARGET_CONCURRENT || '8', 10);
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
            console.log("🚀🚀🚀 Solana Gambles Bot (v2.2-revised) is fully operational! 🚀🚀🚀");

        } catch (initError) { // Catch errors from DB init, Telegram setup, etc.
            console.error("🔥🔥🔥 Delayed Background Initialization Failed:", initError);
            console.error("❌ Exiting due to critical initialization failure.");
            // Attempt graceful shutdown even on init error
             await shutdown('INITIALIZATION_FAILURE', false).catch(() => {}); // Ignore shutdown errors
             setTimeout(() => {
                  console.error("Shutdown timed out after initialization failure. Forcing exit.");
                  process.exit(1); // Force exit if shutdown hangs
             }, parseInt(process.env.SHUTDOWN_FAIL_TIMEOUT_MS || '10000', 10)).unref();
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

// --- End of Part 3 / End of File ---
