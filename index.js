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
    // Note: TransactionMessage, VersionedTransactionResponse might be needed depending on exact web3.js usage
} from '@solana/web3.js';
import bs58 from 'bs58';
// Ensure crypto is imported if not already globally available in your Node version
import * as crypto from 'crypto'; // Import crypto module
import PQueue from 'p-queue';
// Assuming RateLimitedConnection is correctly implemented in this path
import RateLimitedConnection from './lib/solana-connection.js';
// --- START: Added Imports ---
import { toByteArray, fromByteArray } from 'base64-js';
import { Buffer } from 'buffer';
// --- END: Added Imports ---


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
        version: '2.0.9' // Version (Note: Update if version changes) // Consider updating version if appropriate
    });
});

// --- PreStop hook for Railway graceful shutdown ---
app.get('/prestop', (req, res) => {
    console.log('🚪 Received pre-stop signal from Railway, preparing to shutdown gracefully...');
    res.status(200).send('Shutting down');
});

// --- END IMMEDIATE Health Check Endpoint ---


// 1. Enhanced Solana Connection with Rate Limiting
console.log("⚙️ Initializing scalable Solana connection...");
// TODO: Consider implementing request prioritization (e.g., payouts > monitoring) within RateLimitedConnection.
// TODO: Consider implementing exponential backoff within RateLimitedConnection for RPC errors.
const solanaConnection = new RateLimitedConnection(process.env.RPC_URL, {
    maxConcurrent: 3,      // Initial max parallel requests set to 3
    retryBaseDelay: 600,   // Initial delay for retries (ms)
    commitment: 'confirmed',   // Default commitment level
    httpHeaders: {
        'Content-Type': 'application/json',
        'solana-client': `SolanaGamblesBot/2.1.0 (${process.env.RAILWAY_ENVIRONMENT ? 'railway' : 'local'})` // Updated version potentially
    },
    rateLimitCooloff: 10000,     // Pause duration after hitting rate limits (ms)
    disableRetryOnRateLimit: false // Rely on RateLimitedConnection's internal handling
});
console.log("✅ Scalable Solana connection initialized");


// 2. Message Processing Queue (for handling Telegram messages)
const messageQueue = new PQueue({
    concurrency: 5,   // Max concurrent messages processed
    timeout: 10000      // Max time per message task (ms)
});
console.log("✅ Message processing queue initialized");

// 3. Enhanced PostgreSQL Pool
console.log("⚙️ Setting up optimized PostgreSQL Pool...");
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    max: 15,                     // Max connections in pool
    min: 5,                      // Min connections maintained
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
        await client.query(`ALTER TABLE bets ADD COLUMN IF NOT EXISTS priority INT DEFAULT 0;`);
        await client.query(`ALTER TABLE bets ADD COLUMN IF NOT EXISTS fees_paid BIGINT;`);

        // Add indexes for performance on frequently queried columns
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_status ON bets(status);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_user_id ON bets(user_id);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_expires_at ON bets(expires_at);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_priority ON bets(priority);`);
        // Bets memo index (improved from original)
        await client.query(`CREATE UNIQUE INDEX IF NOT EXISTS idx_bets_memo_id ON bets (memo_id) INCLUDE (status, expected_lamports, expires_at);`);

        // *** START: New Indexes from Ultimate Fix ***
        // Using simple IF NOT EXISTS for initialization robustness instead of CONCURRENTLY
        await client.query(`CREATE INDEX IF NOT EXISTS bets_memo_status_idx ON bets (memo_id, status);`);
        await client.query(`CREATE INDEX IF NOT EXISTS bets_created_at_idx ON bets (created_at DESC);`);
        // *** END: New Indexes from Ultimate Fix ***

        // Wallets indexes (Implicit PRIMARY KEY index on user_id is usually sufficient)

        console.log("✅ Database schema initialized/verified."); // Updated log message
    } catch (err) {
        // Log the detailed error if initialization fails
        console.error("❌ Database initialization error:", err);
        throw err; // Re-throw to prevent startup if DB fails
    } finally {
        if (client) client.release(); // Release client back to the pool
    }
}

// (Part 1/5 Ends Here)
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

// Original Health check / Info endpoint (keep for manual checks / detailed status)
// Modified to include initialization status (Fix #3 - Progress Tracking Info)
app.get('/', (req, res) => {
    performanceMonitor.logRequest(true);
    // Updated to reference the new paymentProcessor variable name if necessary
    const processor = paymentProcessor; // Assign to local var for safety inside JSON response
    res.status(200).json({
        status: 'ok',
        initialized: isFullyInitialized, // Report background initialization status here
        timestamp: new Date().toISOString(),
        version: '2.1.0', // Updated version potentially
        queueStats: { // Report queue status
            pending: messageQueue.size + (processor?.highPriorityQueue?.size || 0) + (processor?.normalQueue?.size || 0), // Combined pending
            active: messageQueue.pending + (processor?.highPriorityQueue?.pending || 0) + (processor?.normalQueue?.pending || 0) // Combined active
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
    }); // <<< Closing parenthesis for messageQueue.add
}); // <<< Closing parenthesis for app.post


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
        return {
                    error: e.message,
                    programIdIndex: inst.programIdIndex,
                    accountIndices: inst.accounts
                  }; // Return error info
    }
}
// <<< END DEBUG HELPER FUNCTION >>>

// --- START: Added Helper Function ---
// For instruction data (Base64)
const decodeInstructionData = (data) => {
  if (!data) return null;

  try {
    return typeof data === 'string'
      ? Buffer.from(toByteArray(data)).toString('utf8') // Decode base64 string
      : Buffer.from(data).toString('utf8'); // Assume already Buffer or similar byte array
  } catch (e) {
    // console.warn("Failed to decode instruction data:", e.message); // Optional: log decoding failures
    return null;
  }
};
// --- END: Added Helper Function ---


// --- START: Updated Memo Handling System ---

// Define Memo Program IDs
const MEMO_V1_PROGRAM_ID = new PublicKey("Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo");
const MEMO_V2_PROGRAM_ID = new PublicKey("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr");
// --- START: Added Memo Program IDs Array ---
const MEMO_PROGRAM_IDS = [MEMO_V1_PROGRAM_ID.toBase58(), MEMO_V2_PROGRAM_ID.toBase58()];
// --- END: Added Memo Program IDs Array ---


// 1. Revised Memo Generation with Checksum (Used for *generating* our memos)
// (This function remains structurally the same as the original)
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

function normalizeMemo(rawMemo) {
    // Add a check for the rawMemo type and log if it's not a string
    if (typeof rawMemo !== 'string') {
        console.warn(`[NORMALIZE_MEMO] Received non-string input: ${typeof rawMemo}`, rawMemo);
        return null;
    }

    // 1. Remove ALL control characters (including newlines, tabs, etc.)
    // Also remove leading/trailing whitespace immediately
    let memo = rawMemo.replace(/[\x00-\x1F\x7F]/g, '').trim();

    // Log the raw and initially trimmed/control-char-cleaned memo
    console.log(`[NORMALIZE_MEMO] Input - Raw: "${rawMemo}", Trimmed/Cleaned Control Chars: "${memo}"`);

    // 2. Standardize format aggressively
    memo = memo.toUpperCase()
        .replace(/^MEMO[:=\s]*/i, '') // Remove potential "MEMO:" prefix variations
        .replace(/^TEXT[:=\s]*/i, '') // Remove potential "TEXT:" prefix variations
        .replace(/[\u200B-\u200D\uFEFF]/g, '') // Remove zero-width spaces
        .replace(/\s+/g, '-') // Replace spaces with dashes
        .replace(/[^A-Z0-9\-]/g, ''); // Remove remaining non-alphanumeric/dash characters

    // --- ADDED: Explicit newline handling ---
    // Remove any remaining newlines that might have slipped through
    memo = memo.replace(/\n/g, '').replace(/\r/g, '');

    // --- START: MEMO STATS Logging ---
    // Log the state after full standardization (before V1 check)
    console.log(`[NORMALIZE_MEMO] Fully Standardized Candidate: "${memo}"`);
    // --- END ADDED LOGGING ---

    // Rest of the function remains the same...
    // 3. Special handling for V1 format (BET/CF/RA)-HEX(16)-CHECKSUM(2)
    const v1Pattern = /^(BET|CF|RA)-([A-F0-9]{16})-([A-F0-9]{2})$/;
    const v1Match = memo.match(v1Pattern);

    if (v1Match) {
        const [/* full match */, prefix, hex, checksum] = v1Match; // Destructure correctly

        // --- ADDED LOGGING ---
        console.log(`[NORMALIZE_MEMO] V1 Pattern Matched. Extracted - Prefix: ${prefix}, Hex: ${hex}, Checksum (from input): ${checksum}`);
        // --- END ADDED LOGGING ---

        let expectedChecksum;
        try {
            expectedChecksum = crypto.createHash('sha256')
                .update(hex) // Ensure 'hex' is correctly captured and is a string
                .digest('hex')
                .slice(-2)
                .toUpperCase();
        } catch (hashError) {
             console.error(`[NORMALIZE_MEMO] CRITICAL ERROR calculating checksum for hex "${hex}":`, hashError);
             // Decide how to handle - return null? Return uncorrected? For now, log and return null.
             return null;
        }

        // --- ADDED LOGGING ---
        console.log(`[NORMALIZE_MEMO] Calculated Expected Checksum: ${expectedChecksum}`);
        if (checksum !== expectedChecksum) {
             console.warn(`[NORMALIZE_MEMO] Checksum MISMATCH! Input Checksum: "${checksum}", Expected Checksum: "${expectedChecksum}". Correcting.`);
        } else {
             console.log(`[NORMALIZE_MEMO] Checksum MATCH! Input: "${checksum}", Expected: "${expectedChecksum}".`);
        }
        // --- END ADDED LOGGING ---

        // Return the format with the CORRECT calculated checksum, regardless of input checksum
        const finalMemo = `${prefix}-${hex}-${expectedChecksum}`;
         // --- ADDED LOGGING ---
        console.log(`[NORMALIZE_MEMO] Returning Corrected V1 Memo: "${finalMemo}"`);
        // --- END ADDED LOGGING ---
        return finalMemo;
    }

    // If it's not our V1 format, return the aggressively cleaned string
    // (could be V2, could be junk, but control chars are gone)
    // --- ADDED LOGGING ---
    console.log(`[NORMALIZE_MEMO] V1 Pattern Did NOT Match "${memo}". Returning cleaned string or null.`);
    // --- END ADDED LOGGING ---
    return memo || null; // Return null if memo becomes empty after cleaning
}

// 2. Strict Memo Validation with Checksum Verification (Used only for validating OUR generated V1 format)
// (This function remains structurally the same as the original)
function validateOriginalMemoFormat(memo) {
    if (typeof memo !== 'string') return false;
    const parts = memo.split('-');
    if (parts.length !== 3) return false;
    const [prefix, hex, checksum] = parts;
    // Uses the *helper* which calculates the expected checksum
    return (
        ['BET', 'CF', 'RA'].includes(prefix) &&
        hex.length === 16 &&
        /^[A-F0-9]{16}$/.test(hex) &&
        checksum.length === 2 &&
        /^[A-F0-9]{2}$/.test(checksum) &&
        validateMemoChecksum(hex, checksum) // Validate against calculated checksum
    );
}

// Helper function to validate the checksum part
// (This function remains structurally the same as the original)
function validateMemoChecksum(hex, checksum) {
     const expectedChecksum = crypto.createHash('sha256')
        .update(hex)
        .digest('hex')
        .slice(-2)
        .toUpperCase();
    return expectedChecksum === checksum;
}


// (Part 2/5 Ends Here)
// --- START: Memo Finding Logic (Kept from original structure, uses new normalizeMemo) ---

// <<< START: findMemoInTx FUNCTION (Adapted - uses latest normalizeMemo, retains structure) >>>
// NOTE: This function was part of the original code structure and is kept,
// as the "ultimate fix" only replaced normalizeMemo and the PaymentProcessor class.
// It will internally use the *new* normalizeMemo defined in Part 2.
async function findMemoInTx(tx, signature) { // Added signature for logging
    const startTime = Date.now(); // For MEMO STATS
    const usedMethods = [];       // For MEMO STATS
    let scanDepth = 0;            // For MEMO STATS

    // Type assertion for tx object expected structure
    const transactionResponse = tx; // as VersionedTransactionResponse;

    if (!transactionResponse?.transaction?.message) {
        console.log("[MEMO DEBUG] Invalid transaction structure (missing message)");
        return null;
    }

    // 1. First try the direct approach using log messages (often includes decoded memo)
    scanDepth = 1;
    if (transactionResponse.meta?.logMessages) {
        // Use Regex to handle variations like "Memo:" or "Memo (len ...):"
        // Updated regex to better capture memo content and avoid trailing junk if present
        // In findMemoInTx function, update the log message regex:
const memoLogRegex = /Program log: Memo(?: \(len \d+\))?:\s*"?([^"\n]+)"?/; // Explicitly exclude newlines

// And where we extract the memo from logs:
if (match?.[1]) {
    const memo = normalizeMemo(match[1].trim().replace(/\n$/, '')); // Explicitly remove trailing newline
    if (memo) {
        usedMethods.push('LogScanRegex');
        console.log(`[MEMO DEBUG] Found memo via LogScanRegex: "${memo}" (Raw: "${rawMemo}")`);
        return memo;
    }
}
                    // --- START: MEMO STATS Logging ---
                    console.log(`[MEMO STATS] TX:${signature?.slice(0,8)} | ` +
                                `Methods:${usedMethods.join(',')} | ` +
                                `Depth:${scanDepth} | ` +
                                `Time:${Date.now() - startTime}ms`);
                    // --- END: MEMO STATS Logging ---
                    return memo; // Return the successfully found and normalized memo
                }
            }
    }

    // 2. Fallback to instruction parsing if not found in logs
    scanDepth = 2;
    const message = transactionResponse.transaction.message;
    const accountKeyObjects = message.accountKeys || []; // Array of PublicKey or LoadedAddresses

    // Convert account keys to base58 strings for consistent lookup
    const accountKeys = accountKeyObjects.map(k => {
        if (k instanceof PublicKey) {
             return k.toBase58();
        }
        // Handle potential VersionedMessage structure (account key as string)
        // Be careful with PublicKey conversion here if keys aren't actually pubkeys
        if (typeof k === 'string') {
            // Basic check if it looks like a Base58 pubkey before trying to convert
             if (k.length >= 32 && k.length <= 44 && /^[1-9A-HJ-NP-Za-km-z]+$/.test(k)) {
                try { return new PublicKey(k).toBase58(); } catch { /* ignore conversion error */ }
             }
             return k; // Return original string if not pubkey-like or invalid
        }
        // Handle potential VersionedMessage structure ({ pubkey: PublicKey })
        if (k?.pubkey instanceof PublicKey) {
             return k.pubkey.toBase58();
        }
        // Handle potential VersionedMessage structure ({ pubkey: string })
         if (typeof k?.pubkey === 'string') {
             // Basic check before conversion
             if (k.pubkey.length >= 32 && k.pubkey.length <= 44 && /^[1-9A-HJ-NP-Za-km-z]+$/.test(k.pubkey)) {
                try { return new PublicKey(k.pubkey).toBase58(); } catch { /* ignore conversion error */ }
             }
             return k.pubkey; // Return original string if not pubkey-like or invalid
        }
        return null; // Indicate unknown format
    }).filter(Boolean); // Remove nulls


    // Combine regular and inner instructions for parsing
    const allInstructions = [
        ...(message.instructions || []),
        ...(transactionResponse.meta?.innerInstructions || []).flatMap(i => i.instructions || [])
    ];

    if (allInstructions.length === 0) {
         console.log("[MEMO DEBUG] No instructions found in message or inner instructions.");
    }

    for (const [i, inst] of allInstructions.entries()) {
        try {
            // Resolve program ID string using the mapped accountKeys array
            const programId = inst.programIdIndex !== undefined && accountKeys[inst.programIdIndex]
                                ? accountKeys[inst.programIdIndex]
                                : null;

            // Check for memo programs using resolved programId string
            if (programId && MEMO_PROGRAM_IDS.includes(programId)) {
                const dataString = decodeInstructionData(inst.data); // Use helper from Part 2
                 if (dataString) {
                     const memo = normalizeMemo(dataString); // Use *new* normalizeMemo
                     if (memo) {
                         const method = MEMO_PROGRAM_IDS.indexOf(programId) === 0 ? 'InstrParseV1' : 'InstrParseV2';
                         usedMethods.push(method); // Log Method
                         console.log(`[MEMO DEBUG] Found ${method === 'InstrParseV1' ? 'V1' : 'V2'} memo via instruction parse: "${memo}"`);
                           // --- START: MEMO STATS Logging ---
                         console.log(`[MEMO STATS] TX:${signature?.slice(0,8)} | ` +
                                     `Methods:${usedMethods.join(',')} | ` +
                                     `Depth:${scanDepth} | ` +
                                     `Time:${Date.now() - startTime}ms`);
                         // --- END: MEMO STATS Logging ---
                         return memo;
                     }
                 }
            }

             // Raw pattern matching fallback *within* the instruction loop (kept as a safety net)
            scanDepth = 3; // Conceptually deeper
            const dataBuffer = inst.data ? (typeof inst.data === 'string' ? Buffer.from(toByteArray(inst.data)) : Buffer.from(inst.data)) : null; // Keep buffer for pattern match

            if (dataBuffer && dataBuffer.length >= 22) { // Check minimum length for V1 format like "CF-" + hex + "-" + checksum
                // Look for prefixes using Buffer comparison for safety
                const prefixCF = Buffer.from('CF-');
                const prefixRA = Buffer.from('RA-');
                const prefixBET = Buffer.from('BET-');

                if (dataBuffer.compare(prefixCF, 0, prefixCF.length, 0, prefixCF.length) === 0 ||
                    dataBuffer.compare(prefixRA, 0, prefixRA.length, 0, prefixRA.length) === 0 ||
                    dataBuffer.compare(prefixBET, 0, prefixBET.length, 0, prefixBET.length) === 0)
                 {
                    const potentialMemo = dataBuffer.toString('utf8'); // Decode relevant part
                    const memo = normalizeMemo(potentialMemo); // Try to normalize/validate using *new* function

                    // The new normalizeMemo handles V1 correction, so just check if it returns a valid V1 format
                    if (memo && validateOriginalMemoFormat(memo)) {
                         usedMethods.push('PatternMatchV1'); // Log Method
                         console.log(`[MEMO DEBUG] Pattern-matched and validated V1 memo: "${memo}" (Raw: "${potentialMemo}")`);
                         // --- START: MEMO STATS Logging ---
                         console.log(`[MEMO STATS] TX:${signature?.slice(0,8)} | ` +
                                     `Methods:${usedMethods.join(',')} | ` +
                                     `Depth:${scanDepth} | ` +
                                     `Time:${Date.now() - startTime}ms`);
                         // --- END: MEMO STATS Logging ---
                         return memo;
                    }
                 }
            }
        } catch (e) {
            console.error(`[MEMO DEBUG] Error processing instruction ${i}:`, e?.message || e);
        }
    }

    // Final check for address lookup tables (as per previous logic, might indicate complex tx)
    if (message.addressTableLookups?.length > 0) {
       console.log("[MEMO DEBUG] Transaction uses address lookup tables (memo finding may be incomplete).");
    }

    // --- START: Enhanced Fallback Parsing (Log Scan - kept as safety net) ---
    scanDepth = 4;
    if (transactionResponse.meta?.logMessages) {
        // Deep scan logs for memo patterns (more generic than the initial check)
        const logString = transactionResponse.meta.logMessages.join('\n');
        // Regex: Look for common keywords followed by potential memo-like strings (alphanumeric, dash, at least 10 chars)
        const logMemoMatch = logString.match(
            /(?:Memo|Text|Message|Data|Log):?\s*"?([A-Z0-9\-]{10,})"?/i // Kept simple pattern
        );
        if (logMemoMatch?.[1]) {
            const recoveredMemo = normalizeMemo(logMemoMatch[1]); // Use *new* normalizeMemo
            if (recoveredMemo) {
                usedMethods.push('DeepLogScan'); // Log Method
                console.log(`[MEMO DEBUG] Recovered memo from deep log scan: ${recoveredMemo} (Matched: ${logMemoMatch[1]})`);
                 // --- START: MEMO STATS Logging ---
                console.log(`[MEMO STATS] TX:${signature?.slice(0,8)} | ` +
                           `Methods:${usedMethods.join(',')} | ` +
                           `Depth:${scanDepth} | ` +
                           `Time:${Date.now() - startTime}ms`);
                // --- END: MEMO STATS Logging ---
                return recoveredMemo;
            }
        }
    }
    // --- END: Enhanced Fallback Parsing (Log Scan) ---


    console.log(`[MEMO DEBUG] TX ${signature?.slice(0,8)}: Exhausted all search methods, no memo found.`);
    return null;
}
// <<< END: findMemoInTx FUNCTION >>>


// --- START: deepScanTransaction Function (Kept from original structure) ---
// NOTE: This function was part of the original code structure and is kept,
// as the "ultimate fix" only replaced normalizeMemo and the PaymentProcessor class.
// It will internally use the *new* normalizeMemo defined in Part 2.
async function deepScanTransaction(tx, accountKeys, signature) { // Added accountKeys and signature
    const startTime = Date.now(); // For MEMO STATS
    const usedMethods = ['DeepScanInit']; // Log Method
    let scanDepth = 5; // Start depth for deep scan

    try {
        if (!tx || !tx.meta || !tx.meta.innerInstructions || !accountKeys) {
             console.log("[MEMO DEEP SCAN] Insufficient data for deep scan.");
             return null;
        }

        // 1. Check for memo in inner instructions more thoroughly
        const innerInstructions = tx.meta.innerInstructions.flatMap(i => i.instructions || []);

        for (const inst of innerInstructions) {
             const programId = inst.programIdIndex !== undefined && accountKeys[inst.programIdIndex]
                                 ? accountKeys[inst.programIdIndex]
                                 : null;
             // Check if programId is one of the known memo programs
             if (programId && MEMO_PROGRAM_IDS.includes(programId)) {
                 const dataString = decodeInstructionData(inst.data); // Use helper
                 if (dataString) {
                     const memo = normalizeMemo(dataString); // Use *new* normalizeMemo
                     if (memo) {
                         usedMethods.push('DeepInnerInstr'); // Log Method
                         console.log(`[MEMO DEEP SCAN] Found memo in inner instruction: ${memo}`);
                          // --- START: MEMO STATS Logging ---
                         console.log(`[MEMO STATS] TX:${signature?.slice(0,8)} | ` +
                                     `Methods:${usedMethods.join(',')} | ` +
                                     `Depth:${scanDepth} | ` +
                                     `Time:${Date.now() - startTime}ms`);
                         // --- END: MEMO STATS Logging ---
                         return memo;
                     }
                 }
             }
        }

        // 2. Raw data pattern matching in inner instructions (even non-memo programs)
        scanDepth = 6;
        for (const inst of innerInstructions) {
            // Use new decodeInstructionData for potential text
            const dataString = decodeInstructionData(inst.data);
            // Use the V1 pattern regex from the new normalizeMemo's logic for consistency
             const v1Pattern = /(BET|CF|RA)-([A-F0-9]{16})-([A-F0-9]{2})/;
             if (dataString && v1Pattern.test(dataString)) { // Test if the pattern exists
                 const memo = normalizeMemo(dataString); // Normalize using new function
                 // The new normalizeMemo corrects checksums, so validate the result format
                 if (memo && validateOriginalMemoFormat(memo)) { // Validate if it *is* V1 after normalization
                     usedMethods.push('DeepPatternV1'); // Log Method
                     console.log(`[MEMO DEEP SCAN] Found V1 pattern in inner data: ${memo}`);
                      // --- START: MEMO STATS Logging ---
                     console.log(`[MEMO STATS] TX:${signature?.slice(0,8)} | ` +
                                 `Methods:${usedMethods.join(',')} | ` +
                                 `Depth:${scanDepth} | ` +
                                 `Time:${Date.now() - startTime}ms`);
                     // --- END: MEMO STATS Logging ---
                     return memo;
                 }
             }
        }

        // 3. Final fallback - hex dump scan (Commented out as it's costly and prone to false positives)
        // scanDepth = 7;
        // ... (Original hex dump logic commented out) ...

    } catch (e) {
        console.error(`[MEMO DEEP SCAN] Deep scan failed for TX ${signature?.slice(0,8)}:`, e.message);
    }
    console.log(`[MEMO DEEP SCAN] No memo found after deep scan for TX ${signature?.slice(0,8)}.`);
    return null;
}
// --- END: deepScanTransaction Function ---

// --- END: Updated Memo Handling System ---


// --- Database Operations ---
// (These functions remain structurally the same, relying on the corrected schema from initializeDatabase)

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
// NOTE: This function is effectively overridden/replaced by _findBetGuaranteed
// within the GuaranteedPaymentProcessor. It might still be called elsewhere,
// so it's kept here, but the core payment loop uses the new method.
async function findBetByMemo(memoId) {
    // Memo ID lookup now handles both V1 (normalized/validated) and V2 (raw) formats
    // Relies primarily on the database index for lookup speed.
    if (!memoId || typeof memoId !== 'string') {
        return undefined;
    }

    // Select the highest priority bet first if multiple match (unlikely with unique memo)
    // Use FOR UPDATE SKIP LOCKED to prevent race conditions if multiple monitors pick up the same TX
    // The new GuaranteedPaymentProcessor._findBetGuaranteed handles retries internally.
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
// NOTE: This specific logic is now handled within _processPaymentGuaranteed
// in the new processor class, using a transaction. Kept for potential other uses.
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
// NOTE: This specific logic is now handled within _processPaymentGuaranteed
// in the new processor class, using a transaction. Kept for potential other uses.
async function linkUserWallet(userId, walletAddress) {
    const cacheKey = `wallet-${userId}`;
    const query = `
        INSERT INTO wallets (user_id, wallet_address, last_used_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (user_id) -- If user already exists
        DO UPDATE SET      -- Update their wallet address and last used time
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
// (Remains unchanged and useful)
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
// (Remains unchanged and useful)
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
// (Remains unchanged and useful, called by handlePayoutJob)
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

// (Part 3/5 Ends Here)
// --- Solana Transaction Analysis ---
// (These functions remain structurally the same)

// Analyzes a transaction to find SOL transfers to the target bot wallet
function analyzeTransactionAmounts(tx, walletType) {
    let transferAmount = 0n; // Use BigInt for lamports
    let payerAddress = null;
    const targetAddress = walletType === 'coinflip'
        ? process.env.MAIN_WALLET_ADDRESS
        : process.env.RACE_WALLET_ADDRESS;

    if (!tx || tx.meta?.err) { // Check tx exists before accessing meta
        // console.warn(`Skipping amount analysis for failed or missing TX`);
        return { transferAmount: 0n, payerAddress: null }; // Ignore failed/missing transactions
    }

    // Check both pre and post balances for direct transfers to the target address
    if (tx.meta?.preBalances && tx.meta?.postBalances && tx.transaction?.message?.accountKeys) {
        const accountKeys = tx.transaction.message.accountKeys.map(keyInfo => {
            // Robust key extraction
            if (keyInfo instanceof PublicKey) return keyInfo.toBase58();
            if (keyInfo?.pubkey instanceof PublicKey) return keyInfo.pubkey.toBase58();
            if (typeof keyInfo?.pubkey === 'string') {
                 try { return new PublicKey(keyInfo.pubkey).toBase58(); } catch { return null; }
            }
            if (typeof keyInfo === 'string') {
                 try { return new PublicKey(keyInfo).toBase58(); } catch { return null; }
            }
            return null;
           }).filter(Boolean); // Remove nulls


        const targetIndex = accountKeys.indexOf(targetAddress);

        if (targetIndex !== -1 && tx.meta.preBalances[targetIndex] !== undefined && tx.meta.postBalances[targetIndex] !== undefined) {
            const balanceChange = BigInt(tx.meta.postBalances[targetIndex]) - BigInt(tx.meta.preBalances[targetIndex]);
            if (balanceChange > 0n) {
                transferAmount = balanceChange;
                // Try to identify payer based on who lost balance
                for (let i = 0; i < accountKeys.length; i++) {
                    if (i === targetIndex) continue;
                    // Ensure balance entries exist before calculating change
                    const preBalance = tx.meta.preBalances[i];
                    const postBalance = tx.meta.postBalances[i];
                    if (preBalance === undefined || postBalance === undefined) continue; // Skip if balance missing

                    const payerBalanceChange = BigInt(postBalance) - BigInt(preBalance);

                    // Consider the fee payer (account 0) as the primary suspect if their balance decreased
                    if (i === 0 && payerBalanceChange < 0n) {
                        payerAddress = accountKeys[i];
                        break; // Assume fee payer is the sender
                    }
                    // Otherwise, look for a signer whose balance decreased appropriately
                    // Handle different ways signer info might be present
                    const keyInfo = tx.transaction.message.accountKeys[i]; // Original keyInfo object
                    const isSigner = keyInfo?.signer || // VersionedMessage format
                                       (tx.transaction.message.header?.numRequiredSignatures > 0 && i < tx.transaction.message.header.numRequiredSignatures); // Legacy format

                    // Check if signer and their balance decreased by at least the transfer amount (allowing for fees)
                    if (isSigner && payerBalanceChange <= -transferAmount) {
                        payerAddress = accountKeys[i];
                        break;
                    }
                }
                // If payer still not found, fallback to fee payer if their balance decreased
                 if (!payerAddress && accountKeys[0] && tx.meta.preBalances[0] !== undefined && tx.meta.postBalances[0] !== undefined) {
                     if ((BigInt(tx.meta.postBalances[0]) - BigInt(tx.meta.preBalances[0])) < 0n) {
                         payerAddress = accountKeys[0];
                     }
                 }
            }
        }
    }

    // Fallback or supplement with instruction parsing (less reliable for exact amount sometimes)
    if (transferAmount === 0n && tx.transaction?.message?.instructions) {
        const instructions = [
            ...(tx.transaction.message.instructions || []),
            ...(tx.meta?.innerInstructions || []).flatMap(i => i.instructions || [])
        ];
        const SYSTEM_PROGRAM_ID = SystemProgram.programId.toBase58();

        // Re-map account keys for instruction parsing fallback
        const accountKeysForInstr = tx.transaction.message.accountKeys.map(keyInfo => {
            if (keyInfo instanceof PublicKey) return keyInfo.toBase58();
            if (keyInfo?.pubkey instanceof PublicKey) return keyInfo.pubkey.toBase58();
            if (typeof keyInfo?.pubkey === 'string') {
                 try { return new PublicKey(keyInfo.pubkey).toBase58(); } catch { return null; }
            }
            if (typeof keyInfo === 'string') {
                 try { return new PublicKey(keyInfo).toBase58(); } catch { return null; }
            }
            return null;
           });


        for (const inst of instructions) {
            let programId = '';
            // Robustly get program ID using re-mapped keys
            try {
                if (inst.programIdIndex !== undefined && accountKeysForInstr && accountKeysForInstr[inst.programIdIndex]) {
                     programId = accountKeysForInstr[inst.programIdIndex];
                 } else if (inst.programId) { // Fallback if programId is directly on instruction
                     programId = inst.programId.toBase58 ? inst.programId.toBase58() : String(inst.programId);
                 }
            } catch { /* Ignore errors getting programId */ }


            // Check for SystemProgram SOL transfers using parsed info
             if (programId === SYSTEM_PROGRAM_ID && inst.parsed?.type === 'transfer' && inst.parsed?.info) { // Added checks
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
// (Remains structurally the same)
function getPayerFromTransaction(tx) {
    if (!tx || !tx.meta || !tx.transaction?.message?.accountKeys) return null;

    const message = tx.transaction.message;
    // Fee payer is always the first account listed
    if (message.accountKeys.length > 0) {
        const feePayerKeyInfo = message.accountKeys[0];
        let feePayerAddress = null;
         // Handle different structures of accountKeys robustly
         try {
             if (feePayerKeyInfo instanceof PublicKey) {
                 feePayerAddress = feePayerKeyInfo.toBase58();
             } else if (feePayerKeyInfo?.pubkey instanceof PublicKey) { // VersionedMessage format
                 feePayerAddress = feePayerKeyInfo.pubkey.toBase58();
             } else if (typeof feePayerKeyInfo?.pubkey === 'string') { // VersionedMessage pubkey as string
                 feePayerAddress = new PublicKey(feePayerKeyInfo.pubkey).toBase58();
             } else if (typeof feePayerKeyInfo === 'string') { // Legacy string format
                 feePayerAddress = new PublicKey(feePayerKeyInfo).toBase58();
             }
         } catch (e) {
             console.warn("Could not parse fee payer address:", e.message);
         }


        // console.log(`Identified fee payer as ${feePayerAddress}`); // Optional log
        // Return PublicKey object if valid, otherwise null
        try {
            return feePayerAddress ? new PublicKey(feePayerAddress) : null;
        } catch {
            return null; // Return null if address is invalid
        }
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
        const keyInfo = message.accountKeys[i];
         const isSigner = keyInfo?.signer || // VersionedMessage format
                         (message.header?.numRequiredSignatures > 0 && i < message.header.numRequiredSignatures); // Legacy format

        if (isSigner) {
            let key;
            try {
                // Extract PublicKey robustly
                 if (keyInfo instanceof PublicKey) {
                     key = keyInfo;
                 } else if (keyInfo?.pubkey instanceof PublicKey) {
                     key = keyInfo.pubkey;
                 } else if (typeof keyInfo?.pubkey === 'string') {
                     key = new PublicKey(keyInfo.pubkey);
                 } else if (typeof keyInfo === 'string') {
                     key = new PublicKey(keyInfo);
                 } else {
                     continue; // Cannot determine key
                 }
            } catch (e) { continue; } // Skip if key is invalid

            // Check if balance decreased (paid fees or sent funds)
            // Ensure balances exist before comparing
            if (preBalances[i] !== undefined && postBalances[i] !== undefined) {
                const balanceDiff = BigInt(preBalances[i]) - BigInt(postBalances[i]);
                if (balanceDiff > 0) {
                    // console.log(`Identified potential payer by balance change: ${key.toBase58()}`); // Optional log
                    return key; // Return the PublicKey object
                }
            }
        }
    }

    // console.warn("Could not definitively identify payer.");
    return null; // Could not determine payer
}


// --- Payment Processing System ---

// Checks if an error is likely retryable (e.g., network/rate limit issues)
// (Remains structurally the same)
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

    // Database potentially transient errors
    if (msg.includes('connection terminated') || code === 'econnrefused') { // Basic examples
        return true;
    }

    // Add specific Solana errors that might be retryable
    // (Example: Blockhash not found might be retryable if caught early)
    // if (msg.includes('blockhash not found')) return true;

    return false;
}

// *** START: New GuaranteedPaymentProcessor class from "Ultimate Fix" ***
class GuaranteedPaymentProcessor {
    constructor() {
        this.highPriorityQueue = new PQueue({ concurrency: 3 });
        this.normalQueue = new PQueue({ concurrency: 2 });
        this.activeProcesses = new Set();
        this.memoCache = new Map(); // Cache for memo lookups
        this.cacheTTL = 30000; // Cache memo results for 30 seconds
        console.log("✅ Initialized GuaranteedPaymentProcessor"); // Log initialization
    }

    // Adds a job to the appropriate queue based on priority (Similar to original)
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

    // Wrapper to handle job execution, retries, and error logging (Similar to original)
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
                // Call the new main processing method
                result = await this._processIncomingPayment(job.signature, job.walletType);
            } else if (job.type === 'process_bet') {
                // Game processing logic remains separate for now
                const bet = await pool.query('SELECT * FROM bets WHERE id = $1', [job.betId]).then(res => res.rows[0]);
                if (bet) {
                    await processPaidBet(bet); // Trigger game logic processing
                    result = { processed: true }; // Assume success unless exception
                } else {
                    console.error(`Cannot process bet: Bet ID ${job.betId} not found.`);
                    result = { processed: false, reason: 'bet_not_found' };
                }
            } else if (job.type === 'payout') {
                // Payout logic remains separate for now
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
            // Log error with more context
            console.error(`Error processing job type ${job.type} for identifier ${jobIdentifier || 'N/A'} in processJob:`, error.message, error.stack);

            // No retry logic here; retries are handled within the _methods called by _processIncomingPayment
            console.error(`Job failed permanently for identifier: ${jobIdentifier}`, error);
             // Potentially update bet status to an error state if applicable and if betId exists
             if(job.betId && job.type !== 'monitor_payment') { // Only update status for non-monitor failures with betId
                 await updateBetStatus(job.betId, `error_${job.type}_failed`);
             }

        } finally {
            // Always remove identifier from active set when processing finishes (success or final fail)
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
            // console.log(`Sig ${signature}: Already processed this session.`);
            return { processed: false, reason: 'already_processed_session' };
        }

        // 2. Database-level duplicate check (checks bets.paid_tx_signature)
        try {
            const exists = await pool.query(
                'SELECT 1 FROM bets WHERE paid_tx_signature = $1 LIMIT 1',
                [signature]
            );
            if (exists.rowCount > 0) {
                // console.log(`Sig ${signature}: Already recorded in DB.`);
                processedSignaturesThisSession.add(signature); // Cache if found in DB
                return { processed: false, reason: 'exists_in_db' };
            }
        } catch (dbError) {
             console.error(`Sig ${signature}: DB error checking paid_tx_signature: ${dbError.message}`);
             // Don't cache, allow potential retry by monitor if DB issue was temporary
             return { processed: false, reason: 'db_check_error' };
        }


        let tx;
        try {
             // 3. Transaction fetching with robust error handling
             console.log(`Sig ${signature}: Fetching transaction...`);
             tx = await this._getTransactionWithRetry(signature); // Use new retry method

             // 4. Memo extraction with guaranteed normalization
             console.log(`Sig ${signature}: Extracting memo...`);
             const memo = await this._extractMemoGuaranteed(tx, signature); // Use new memo method
             if (!memo) {
                 console.log(`Sig ${signature}: No valid memo found after extraction.`);
                 processedSignaturesThisSession.add(signature); // Cache if no memo found
                 return { processed: false, reason: 'no_valid_memo' };
             }
             console.log(`Sig ${signature}: Found memo "${memo}".`);

             // 5. Bet lookup with guaranteed finding (includes retries & cache)
             console.log(`Sig ${signature}: Looking up bet for memo "${memo}"...`);
             const bet = await this._findBetGuaranteed(memo); // Use new bet finding method
             if (!bet) {
                 // If _findBetGuaranteed returns null after retries, it means no matching *awaiting_payment* bet exists.
                 // It might exist but already processed, or be unrelated. Don't cache signature here,
                 // as the monitor might pick it up again if status changes (unlikely but possible).
                 // Let the monitor naturally stop seeing it if it's unrelated.
                 console.log(`Sig ${signature}: No awaiting_payment bet found for memo "${memo}" after retries/cache check.`);
                 return { processed: false, reason: 'no_matching_bet_final' };
             }
             console.log(`Sig ${signature}: Found bet ID ${bet.id} for memo "${memo}".`);

             // 6. Process payment with guaranteed completion (includes DB transaction)
             console.log(`Sig ${signature}: Processing payment for bet ID ${bet.id}...`);
             // *** CORRECTED: Pass tx object to _processPaymentGuaranteed ***
             return await this._processPaymentGuaranteed(bet, signature, walletType, tx);

        } catch (error) {
            // Catch errors from fetching, memo extraction, bet finding, or processing
             console.error(`Sig ${signature}: Critical error during processing: ${error.message}`, error.stack);
             // Decide whether to cache based on the error type
             if (!isRetryableError(error)) {
                 // If it's a non-retryable error during fetch/processing, cache the sig
                 processedSignaturesThisSession.add(signature);
                 console.log(`Sig ${signature}: Caching signature due to non-retryable error.`);
             }
             // Update bet status if we have a bet ID and it failed during processing payment
             if (error.betId) { // Assuming error might carry betId context from _processPaymentGuaranteed
                 await updateBetStatus(error.betId, 'error_processing_exception');
             }
             return { processed: false, reason: `processing_error: ${error.message}` };
        }
    }

    // Helper to get transaction with retries
    async _getTransactionWithRetry(signature, attempts = 3) {
        for (let i = 0; i < attempts; i++) {
            try {
                const tx = await solanaConnection.getParsedTransaction(signature, {
                    maxSupportedTransactionVersion: 0,
                    commitment: 'confirmed'
                });

                if (!tx) {
                    // Handle null response from RPC (transaction might not be found/confirmed yet)
                    throw new Error(`Transaction ${signature} not found (null response)`);
                 }

                 if (tx.meta?.err) {
                     // Transaction found but failed on-chain
                     console.log(`Sig ${signature}: Transaction failed on-chain: ${JSON.stringify(tx.meta.err)}`);
                     // Don't retry on-chain failures, return null to indicate fetch failure for processing logic
                     return null;
                 }

                 // Transaction fetched successfully and did not fail on-chain
                 return tx;

            } catch (error) {
                console.warn(`Sig ${signature}: Fetch attempt ${i + 1}/${attempts} failed: ${error.message}`);
                if (i === attempts - 1 || !isRetryableError(error)) {
                     // If last attempt or not a retryable error, return null
                     console.error(`Sig ${signature}: Final fetch attempt failed or non-retryable error.`);
                     return null; // Indicate final failure to fetch valid TX
                 }
                 // Wait before retrying
                await new Promise(r => setTimeout(r, 1000 * (i + 1)));
            }
        }
        return null; // Should not be reached if loop logic is correct, but acts as fallback
    }

    // Helper to extract memo using multiple methods (uses new normalizeMemo)
    async _extractMemoGuaranteed(tx, signature) {
        if (!tx) return null; // No transaction, no memo

        // 1. Check logs first (most reliable) - Slightly refined regex from user suggestion
        if (tx.meta?.logMessages) {
            const memoLogRegex = /Program log: Memo(?: \(len \d+\))?:\s*"?([^"]+)"?/; // Prefer original robust regex
            for (const log of tx.meta.logMessages) {
                const match = log.match(memoLogRegex);
                if (match?.[1]) {
                    const memo = normalizeMemo(match[1].trim().replace(/\n$/, '')); // Use new normalize + trim/remove trailing \n
                    if (memo) {
                         // console.log(`Sig ${signature}: Memo found via LogScanRegex`);
                         return memo;
                    }
                }
            }
        }

        // 2. Check instructions
        try { // Wrap instruction parsing in try/catch
             const accountKeys = (tx.transaction?.message?.accountKeys || []).map(k =>
                 k instanceof PublicKey ? k.toBase58() :
                 typeof k === 'string' ? k : // Keep original string if not PublicKey
                 k?.pubkey?.toBase58?.() || null // Handle {pubkey: PublicKey} or {pubkey: string} safely
             ).filter(Boolean);

             const allInstructions = [
                 ...(tx.transaction?.message?.instructions || []),
                 ...(tx.meta?.innerInstructions || []).flatMap(i => i.instructions || [])
             ];

             for (const inst of allInstructions) {
                 const programId = inst.programIdIndex !== undefined && accountKeys[inst.programIdIndex]
                                     ? accountKeys[inst.programIdIndex]
                                     : null;

                 if (programId && MEMO_PROGRAM_IDS.includes(programId)) {
                     const dataString = decodeInstructionData(inst.data); // Use helper
                     const memo = normalizeMemo(dataString); // Use new normalizeMemo
                     if (memo) {
                         // console.log(`Sig ${signature}: Memo found via Instruction Parse`);
                         return memo;
                     }
                 }
             }
        } catch(instrError) {
             console.error(`Sig ${signature}: Error during instruction parsing for memo: ${instrError.message}`);
             // Continue to next method even if instruction parsing fails
        }


        // 3. Final fallback - raw data scan (less reliable, kept from user fix)
        try {
             const txString = JSON.stringify(tx); // This could be large
             // Regex looking for V1 format (BET/CF/RA)-HEX(16)-CHECKSUM(2)
             const memoMatch = txString.match(/(?:BET|CF|RA)-[A-F0-9]{16}-[A-F0-9]{2}/);
             if (memoMatch?.[0]) {
                const memo = normalizeMemo(memoMatch[0]); // Normalize just in case
                if (memo && validateOriginalMemoFormat(memo)) { // Validate it's correct V1
                     // console.log(`Sig ${signature}: Memo found via Raw Scan`);
                     return memo;
                }
             }
        } catch (stringifyError) {
            console.error(`Sig ${signature}: Error stringifying TX for raw memo scan: ${stringifyError.message}`);
        }

        return null; // No memo found by any method
    }

// (Part 4/5 Ends Here)
// Helper to find bet with retries and caching (uses new normalizeMemo implicitly via caller)
    async _findBetGuaranteed(memo, attempts = 3) {
        // Check cache first
        const cachedBet = this.memoCache.get(memo);
        if (cachedBet) {
             // Validate cache entry hasn't expired conceptually (TTL handled by setTimeout on set)
             // and that its status is still relevant (awaiting_payment)
             if (cachedBet.status === 'awaiting_payment') {
                 // console.log(`Memo ${memo}: Cache hit.`);
                 return cachedBet;
             } else {
                 // If status changed, remove from cache
                 this.memoCache.delete(memo);
             }
        }

        // Database lookup with retries for temporary locks
        for (let i = 0; i < attempts; i++) {
            let client; // Use separate client for this potentially retried lookup
            try {
                client = await pool.connect(); // Get connection for this attempt
                const bet = await client.query(`
                    SELECT id, user_id, chat_id, game_type, bet_details, expected_lamports, status, expires_at, fees_paid, priority
                    FROM bets
                    WHERE memo_id = $1 AND status = 'awaiting_payment'
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                `, [memo]).then(r => r.rows[0]);

                if (bet) {
                    // console.log(`Memo ${memo}: DB lookup successful on attempt ${i + 1}.`);
                    // Add to cache with TTL
                    this.memoCache.set(memo, bet);
                    setTimeout(() => {
                         // Verify item still exists before deleting, might have been refreshed
                         const currentCached = this.memoCache.get(memo);
                         if (currentCached && currentCached.id === bet.id) { // Simple check
                            this.memoCache.delete(memo);
                         }
                    }, this.cacheTTL);
                    return bet; // Return the found bet
                }
                // If bet is null, it means row was skipped (locked) or doesn't exist in awaiting_payment state
                // console.log(`Memo ${memo}: DB lookup attempt ${i + 1} found no unlocked awaiting_payment bet.`);
                // Continue loop only if bet is null (row skipped/not found), otherwise exit loop

            } catch (error) {
                console.error(`Memo ${memo}: DB lookup attempt ${i + 1} failed: ${error.message}`);
                if (i === attempts - 1 || !isRetryableError(error)) {
                     // Rethrow final or non-retryable error to be caught by _processIncomingPayment
                     throw error;
                 }
                 // Wait before retrying DB connection/query
                await new Promise(r => setTimeout(r, 300 * (i + 1)));
            } finally {
                 if (client) client.release(); // Release client for this attempt
            }
            // Small delay even if query succeeded but returned no rows (i.e., skipped locked row)
             if (i < attempts - 1) {
                 await new Promise(r => setTimeout(r, 200 * (i + 1)));
             }
        }
        // If loop finishes without finding a bet after all attempts
        // console.log(`Memo ${memo}: Bet not found after ${attempts} attempts.`);
        return null;
    }

    // Helper to process payment within a DB transaction
    // *** CORRECTED: Added 'tx' parameter ***
    async _processPaymentGuaranteed(bet, signature, walletType, tx) {
        const client = await pool.connect();
        try {
            await client.query('BEGIN');

            // 1. Verify bet status again within transaction for atomicity
            const currentBet = await client.query(`
                SELECT status
                FROM bets WHERE id = $1
                FOR UPDATE
            `, [bet.id]).then(r => r.rows[0]); // Lock the row we intend to update

            if (!currentBet) {
                // This should ideally not happen if _findBetGuaranteed found it, but safety check
                console.warn(`Sig ${signature}: Bet ID ${bet.id} not found during final lock.`);
                await client.query('ROLLBACK');
                return { processed: false, reason: 'bet_disappeared' };
            }
            if (currentBet.status !== 'awaiting_payment') {
                 console.warn(`Sig ${signature}: Bet ID ${bet.id} status was ${currentBet.status}, not awaiting_payment, during final lock.`);
                 await client.query('ROLLBACK');
                 // Cache the signature here as it's definitively processed or in an unexpected state
                 processedSignaturesThisSession.add(signature);
                 return { processed: false, reason: 'bet_already_processed_final' };
            }

             // --- Additional Validation Checks within Transaction ---
             // Check amount and expiry again for maximum safety, using the passed 'tx' object

             // 8a. Analyze transaction amounts (using original helper function)
             const { transferAmount, payerAddress: detectedPayer } = analyzeTransactionAmounts(tx, walletType);
             if (transferAmount <= 0n) {
                 console.warn(`Sig ${signature}: Bet ID ${bet.id}. No SOL transfer found in final check.`);
                 await client.query('ROLLBACK');
                 processedSignaturesThisSession.add(signature); // Cache on definitive validation failure
                 return { processed: false, reason: 'no_transfer_found_final' };
             }

             // 8b. Validate amount sent vs expected (with tolerance)
             const expectedAmount = BigInt(bet.expected_lamports);
             const tolerance = BigInt(5000); // 0.000005 SOL tolerance
             if (transferAmount < (expectedAmount - tolerance) || transferAmount > (expectedAmount + tolerance)) {
                 console.warn(`Sig ${signature}: Bet ID ${bet.id}. Amount mismatch in final check. Expected ~${expectedAmount}, got ${transferAmount}.`);
                 // Update status to error within this transaction before rollback
                 await client.query(`UPDATE bets SET status = 'error_payment_mismatch', processed_at = NOW() WHERE id = $1`, [bet.id]);
                 await client.query('ROLLBACK'); // Rollback after marking error
                 processedSignaturesThisSession.add(signature); // Cache on definitive validation failure
                 // Send Telegram message *after* releasing DB connection
                 safeSendMessage(bet.chat_id, `⚠️ Payment amount mismatch for bet \`${bet.memo_id}\`. Expected ${Number(expectedAmount)/LAMPORTS_PER_SOL} SOL, received ${Number(transferAmount)/LAMPORTS_PER_SOL} SOL. Bet cancelled.`, { parse_mode: 'Markdown' }).catch(e => console.error("TG Send Error:", e.message));
                 return { processed: false, reason: 'amount_mismatch_final' };
             }

             // 8c. Validate transaction time vs bet expiry
             const txTime = tx.blockTime ? new Date(tx.blockTime * 1000) : null;
             if (!txTime) {
                 console.warn(`Sig ${signature}: Bet ID ${bet.id}. Could not determine blockTime for final expiry check.`);
                 // Decide if this is critical - potentially allow processing? For now, treat as error.
                 await client.query(`UPDATE bets SET status = 'error_missing_blocktime', processed_at = NOW() WHERE id = $1`, [bet.id]);
                 await client.query('ROLLBACK');
                 processedSignaturesThisSession.add(signature); // Cache as validation failed
                 return { processed: false, reason: 'missing_blocktime_final' };
             } else if (txTime > new Date(bet.expires_at)) {
                 console.warn(`Sig ${signature}: Bet ID ${bet.id}. Payment received after expiry in final check.`);
                 await client.query(`UPDATE bets SET status = 'error_payment_expired', processed_at = NOW() WHERE id = $1`, [bet.id]);
                 await client.query('ROLLBACK');
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
                    processed_at = NOW()
                WHERE id = $2
            `, [signature, bet.id]);

            // 3. Link wallet if possible
            // Use payer address detected earlier, but call the original getPayerFromTransaction as fallback if needed
            const payer = detectedPayer || getPayerFromTransaction(tx)?.toBase58(); // Use helper
            if (payer) {
                 try {
                     // Validate payer is a valid PublicKey string before inserting
                     new PublicKey(payer);
                     await client.query(`
                         INSERT INTO wallets (user_id, wallet_address, last_used_at)
                         VALUES ($1, $2, NOW())
                         ON CONFLICT (user_id) DO UPDATE
                         SET wallet_address = EXCLUDED.wallet_address,
                             last_used_at = NOW()
                     `, [bet.user_id, payer]);
                     // Update cache after successful DB link/update
                      walletCache.set(`wallet-${bet.user_id}`, { wallet: payer, timestamp: Date.now() });
                 } catch (walletError) {
                      console.warn(`Sig ${signature}: Bet ID ${bet.id}. Failed to link invalid payer address "${payer}": ${walletError.message}`);
                      // Non-critical error, continue processing the bet
                 }
            } else {
                 console.warn(`Sig ${signature}: Bet ID ${bet.id}. Could not identify valid payer address to link wallet.`);
            }

            // Commit the transaction (marks paid, links wallet)
            await client.query('COMMIT');
            console.log(`Sig ${signature}: Bet ID ${bet.id} successfully marked as 'payment_verified' and wallet linked (if possible).`);

            // Add signature to session cache ONLY AFTER successful commit
            processedSignaturesThisSession.add(signature);
             if (processedSignaturesThisSession.size > MAX_PROCESSED_SIGNATURES) {
                  console.log('Clearing processed signatures cache (reached max size)');
                  processedSignaturesThisSession.clear();
             }


            // 4. Queue for game processing (after successful commit)
            // Need to ensure the bet object passed has all necessary details
             const fullBetDetails = await pool.query('SELECT * FROM bets WHERE id = $1', [bet.id]).then(res => res.rows[0]);
             if (fullBetDetails) {
                 await this._queueBetProcessing(fullBetDetails); // Pass the full details
             } else {
                 console.error(`Sig ${signature}: Failed to retrieve full bet details for Bet ID ${bet.id} after commit. Cannot queue game processing.`);
                 // Status is already payment_verified, needs manual check potentially
             }
            return { processed: true };

        } catch (error) {
            console.error(`Sig ${signature}: Error during final payment processing for Bet ID ${bet.id}: ${error.message}`, error.stack);
            await client.query('ROLLBACK').catch((rbError) => console.error(`Rollback failed: ${rbError.message}`));
            // Add betId to error context for outer catch block
            error.betId = bet.id;
            throw error; // Re-throw error to be handled by _processIncomingPayment's main catch
        } finally {
            client.release();
        }
    }

     // Helper method to queue bet processing (adapted from original logic)
     async _queueBetProcessing(bet) {
         console.log(`Payment verified for bet ${bet.id}. Queuing for game processing.`);
         await this.addPaymentJob({ // Use the processor's own queuing method
             type: 'process_bet',
             betId: bet.id,
             priority: 1, // High priority for game logic
             // signature // Can pass signature along for logging context if needed
         });
     }

} // *** END: New GuaranteedPaymentProcessor class ***

// Instantiate the new payment processor
const paymentProcessor = new GuaranteedPaymentProcessor();


// --- Payment Monitoring Loop ---
let isMonitorRunning = false; // Flag to prevent concurrent monitor runs
const botStartupTime = Math.floor(Date.now() / 1000); // Timestamp in seconds
let monitorIntervalSeconds = 45; // Initial interval
let monitorInterval = null; // Holds the setInterval ID

// *** MONITOR PATCH APPLIED: Fetch latest N signatures, no 'before' ***
// (Structurally unchanged, but now uses the GuaranteedPaymentProcessor instance)
async function monitorPayments() {
    // console.log(`[${new Date().toISOString()}] ---- monitorPayments function START ----`); // Reduce log noise

    if (isMonitorRunning) {
        // console.log('[Monitor] Cycle already running, skipping.'); // Reduce noise
        return;
    }
    if (!isFullyInitialized) {
        console.log('[Monitor] Skipping cycle, application not fully initialized yet.');
        return;
    }

    isMonitorRunning = true;
    const startTime = Date.now();
    let signaturesFoundThisCycle = 0; // Count total signatures found across wallets
    let signaturesQueuedThisCycle = 0; // Count signatures actually queued

    try {
        // --- START: Adaptive Rate Limiting Logic (Kept for stability) ---
        const currentLoad = (paymentProcessor.highPriorityQueue.pending || 0) +
                            (paymentProcessor.normalQueue.pending || 0) +
                            (paymentProcessor.highPriorityQueue.size || 0) + // Include active items too
                            (paymentProcessor.normalQueue.size || 0);
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

        // --- NEW: Add Jitter to Requests ---
        await new Promise(resolve => setTimeout(resolve, Math.random() * 2000)); // 0-2s jitter
        // --- END: Jitter ---

        // --- Core Logic (Adapted with Patch Fix) ---
        console.log("⚙️ Performing payment monitor run...");

        const monitoredWallets = [
             { address: process.env.MAIN_WALLET_ADDRESS, type: 'coinflip', priority: 0 },
             { address: process.env.RACE_WALLET_ADDRESS, type: 'race', priority: 0 },
        ];

        for (const wallet of monitoredWallets) {
            const walletAddress = wallet.address;
            let signaturesForWallet = [];

            try {
                // *** PATCH FIX APPLIED HERE ***
                // Fetch the latest N signatures, DO NOT use 'before'
                const options = { limit: 20 }; // Fetch latest 20
                console.log(`[Monitor] Checking ${wallet.type} wallet (${walletAddress}) for latest ${options.limit} signatures.`);

                signaturesForWallet = await solanaConnection.getSignaturesForAddress(
                    new PublicKey(walletAddress),
                    options // Pass options without 'before'
                );

                if (!signaturesForWallet || signaturesForWallet.length === 0) {
                    // console.log(`ℹ️ No recent signatures found for ${walletAddress}.`);
                    continue; // Skip to next wallet
                }

                signaturesFoundThisCycle += signaturesForWallet.length;

                // Filter out old transactions (Optional but good practice)
                const recentSignatures = signaturesForWallet.filter(sigInfo => {
                    if (sigInfo.blockTime && sigInfo.blockTime < botStartupTime - 300) { // Check ~5 mins back
                        // console.log(`🕒 Skipping potentially old TX for ${walletAddress}: ${sigInfo.signature}`);
                        return false;
                    }
                    return true;
                });

                if (recentSignatures.length === 0) {
                     // console.log(`ℹ️ No RECENT signatures for ${walletAddress} after filtering.`); // Reduce noise
                     continue;
                }

                // Process oldest first within the fetched batch by reversing the array
                recentSignatures.reverse();

                for (const sigInfo of recentSignatures) {
                    // Check session cache first (quickest check)
                    if (processedSignaturesThisSession.has(sigInfo.signature)) {
                        // console.log(`Sig ${sigInfo.signature} already processed this session, skipping queue.`);
                        continue;
                    }

                    // Check if already being processed by another job
                    if (paymentProcessor.activeProcesses.has(sigInfo.signature)) {
                        // console.log(`Sig ${sigInfo.signature} is already actively being processed, skipping queue.`);
                        continue;
                    }

                    // Queue the signature for full processing if not in cache or active
                    // Use the GuaranteedPaymentProcessor instance
                    console.log(`[Monitor] Queuing signature ${sigInfo.signature} for ${wallet.type} wallet.`);
                    signaturesQueuedThisCycle++;
                    await paymentProcessor.addPaymentJob({ // Use the correct processor instance
                        type: 'monitor_payment',
                        signature: sigInfo.signature,
                        walletType: wallet.type,
                        priority: wallet.priority, // Normal priority for initial check
                        // Retries handled internally by GuaranteedPaymentProcessor methods now
                    });
                }

            } catch (error) {
                // Catch errors during signature fetching for a specific wallet
                if (error?.message?.includes('429') || error?.code === 429 || error?.statusCode === 429) {
                    // --- Aggressive Backoff Logic (Keep) ---
                    console.warn(`⚠️ Solana RPC 429 - backing off more aggressively...`);
                    monitorIntervalSeconds = Math.min(monitorIntervalSeconds * 2, 300); // Double interval, max 5 minutes (300s)
                    console.log(`ℹ️ New monitor interval after backoff: ${monitorIntervalSeconds}s`);
                    // --- END: Aggressive Backoff Logic ---

                    if (monitorInterval) clearInterval(monitorInterval);
                    monitorInterval = setInterval(() => {
                        monitorPayments().catch(err => console.error('❌ [FATAL MONITOR ERROR in setInterval catch]:', err));
                    }, monitorIntervalSeconds * 1000);
                    await new Promise(resolve => setTimeout(resolve, 15000)); // Wait after resetting interval
                    isMonitorRunning = false;
                    return; // Exit monitor function for this cycle
                } else {
                    console.error(`[Monitor] Error fetching/processing signatures for wallet ${walletAddress}:`, error.message);
                    performanceMonitor.logRequest(false);
                    // Continue to the next wallet
                }
            }
        } // End loop through wallets

        // console.log(`[Monitor] Cycle check finished.`); // Reduce noise

        if (monitorIntervalSeconds >= 60) { // Adjusted warning threshold slightly
            console.warn(`⚠️ Warning: Monitor interval high (${monitorIntervalSeconds}s). RPC may be struggling or backoff active.`);
        }
       // --- End Core Logic ---

    } catch (err) {
        // General error handling
        console.error('❌ MonitorPayments Error in main block:', err);
        performanceMonitor.logRequest(false);

         if (err?.message?.includes('429') || err?.code === 429 || err?.statusCode === 429) {
             // --- Aggressive Backoff Logic (also applied to main block errors) ---
             console.warn('⚠️ Solana RPC 429 detected in main block. Backing off more aggressively...');
             monitorIntervalSeconds = Math.min(monitorIntervalSeconds * 2, 300); // Double interval, max 5 minutes (300s)
             console.log(`ℹ️ New monitor interval after backoff: ${monitorIntervalSeconds}s`);
             // --- END: Aggressive Backoff Logic ---

             if (monitorInterval) clearInterval(monitorInterval);
             monitorInterval = setInterval(() => {
                  monitorPayments().catch(err => console.error('❌ [FATAL MONITOR ERROR in setInterval catch]:', err));
             }, monitorIntervalSeconds * 1000);
             await new Promise(resolve => setTimeout(resolve, 15000)); // Wait after resetting interval
         }
    } finally {
        isMonitorRunning = false; // Release the lock
        const duration = Date.now() - startTime;
        console.log(`[Monitor] Cycle completed in ${duration}ms. Found ${signaturesFoundThisCycle} total signatures. Queued ${signaturesQueuedThisCycle} new signatures for processing.`);
    }
}
// *** END UPDATED FUNCTION ***


// --- SOL Sending Function ---
// (Remains structurally the same)
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
                        maxRetries: 2,  // Retries within sendAndConfirm (lower internal retries)
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
// (These functions remain structurally the same, relying on the updated payment flow indirectly)

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
        // Ensure we are processing 'payment_verified' status set by GuaranteedPaymentProcessor
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

            // Add payout job to the high priority queue using the paymentProcessor instance
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

    // Define horses within the function scope
    const horses = [
        { name: 'Yellow', emoji: '🟡', odds: 1.1, baseProb: 0.25 }, { name: 'Orange', emoji: '🟠', odds: 2.0, baseProb: 0.20 },
        { name: 'Blue',   emoji: '🔵', odds: 3.0, baseProb: 0.15 }, { name: 'Cyan',   emoji: '💧', odds: 4.0, baseProb: 0.12 },
        { name: 'White',  emoji: '⚪️', odds: 5.0, baseProb: 0.09 }, { name: 'Red',    emoji: '🔴', odds: 6.0, baseProb: 0.07 },
        { name: 'Black',  emoji: '⚫️', odds: 7.0, baseProb: 0.05 }, { name: 'Pink',   emoji: '🌸', odds: 8.0, baseProb: 0.03 },
        { name: 'Purple', emoji: '🟣', odds: 9.0, baseProb: 0.02 }, { name: 'Green',  emoji: '🟢', odds: 10.0, baseProb: 0.01 },
        { name: 'Silver', emoji: '💎', odds: 15.0, baseProb: 0.01 } // Sum of baseProb should be adjusted if needed
    ];

    // Ensure totalBaseProb is calculated correctly (should be close to 1.0)
    const totalBaseProb = horses.reduce((sum, h) => sum + h.baseProb, 0);
    if (Math.abs(totalBaseProb - 1.0) > 0.001) {
        console.warn(`Race probabilities do not sum to 1.0 (Sum: ${totalBaseProb}). Adjusting normalization.`);
    }
    const targetTotalProb = 1.0 - config.houseEdge; // e.g., 0.98

    // Determine the winning horse based on adjusted weighted probabilities
    let winningHorse = null;
    const randomNumber = Math.random(); // Random number between 0 and 1
    let cumulativeProbability = 0;

    if (randomNumber < targetTotalProb) { // Player wins branch
        const playerWinRoll = randomNumber / targetTotalProb; // Normalize roll to 0-1 range for player win space
        let cumulativeBaseProb = 0; // Use separate cumulative for base probabilities
        for (const horse of horses) {
            // Use BASE probability normalized against the sum for distribution WITHIN the player win chance
            cumulativeBaseProb += (horse.baseProb / totalBaseProb);
            if (playerWinRoll <= cumulativeBaseProb) {
                winningHorse = horse;
                break;
            }
        }
         // Fallback within player win branch if precision errors occur
         if (!winningHorse) {
             console.warn(`Race Bet ${betId}: Fallback triggered during winner selection (Player Win Branch).`);
             winningHorse = horses[horses.length - 1]; // Assign last horse as fallback
         }
    } else {
        // House wins branch - Pick a random horse as the "winner" but player loses
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
    // Compare chosen name case-insensitively against the winning horse's name
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

            // Add payout job to the high priority queue using the paymentProcessor instance
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
                `✅ Payout successful for bet ID \`${betId}\`!\n` + // Use betId in message
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
                `⚠️ Payout failed for bet ID \`${betId}\`: ${sendResult.error}\n` + // Use betId
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
            `⚠️ A technical error occurred during payout for bet ID \`${betId}\`.\n` + // Use betId
            `Please contact support.`,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
    }
}

// --- Utility Functions ---

// Calculates payout amount, applying house edge. Returns BigInt lamports.
// (Remains structurally the same)
function calculatePayout(betLamports, gameType, gameDetails = {}) {
    betLamports = BigInt(betLamports); // Ensure input is BigInt
    let payoutLamports = 0n;

    if (gameType === 'coinflip') {
        // Correct calculation: Payout = Bet + (Bet * (1 - HouseEdge)) = Bet * (2 - HouseEdge)
        const multiplier = 2.0 - GAME_CONFIG.coinflip.houseEdge;
        payoutLamports = BigInt(Math.floor(Number(betLamports) * multiplier));
    } else if (gameType === 'race' && gameDetails.odds) {
        // Payout is Bet * Odds * (1 - House Edge) => Applied to the whole payout including stake return
        const multiplier = gameDetails.odds * (1.0 - GAME_CONFIG.race.houseEdge);
        payoutLamports = BigInt(Math.floor(Number(betLamports) * multiplier));
    } else {
        console.error(`Cannot calculate payout: Unknown game type ${gameType} or missing race odds.`);
        return 0n; // Return 0 if calculation fails
    }

    // Return the calculated payout amount.
    return payoutLamports > 0n ? payoutLamports : 0n; // Ensure payout is not negative
}

// Fetches user's display name (username or first name) from Telegram
// (Remains structurally the same)
async function getUserDisplayName(chat_id, user_id) {
    try {
        const chatMember = await bot.getChatMember(chat_id, user_id);
        const username = chatMember.user.username;
        if (username && /^[a-zA-Z0-9_]{5,32}$/.test(username)) { // Basic validation
            return `@${username}`;
        }
        const firstName = chatMember.user.first_name;
        if (firstName) {
            // Basic sanitization for HTML/Markdown special chars
            return firstName.replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/[*_`\[]/g, '\\$&');
        }
        return `User ${String(user_id).substring(0, 6)}...`; // Fallback
    } catch (e) {
        // Handle cases where user might not be in the chat anymore
        if (e.response && e.response.statusCode === 400 && e.response.body?.description?.includes('user not found')) {
             console.warn(`User ${user_id} not found in chat ${chat_id}.`);
        } else {
             console.warn(`Couldn't get username/name for user ${user_id} in chat ${chat_id}:`, e.message);
        }
        return `User ${String(user_id).substring(0, 6)}...`; // Return partial ID as fallback
    }
}


// --- Telegram Bot Command Handlers ---
// (These remain structurally the same, calling the updated underlying logic where appropriate)

// Handles polling errors
bot.on('polling_error', (error) => {
    console.error(`❌ Polling error: ${error.code} - ${error.message}`);
    if (error.code === 'ETELEGRAM' && error.message.includes('409 Conflict')) {
        console.error("❌❌❌ FATAL: Conflict detected! Another bot instance might be running with the same token. Exiting.");
        console.error("❌ Exiting due to conflict."); process.exit(1); // Exit immediately to prevent issues
    }
    // Consider adding more specific error handling (e.g., network issues vs auth issues)
});

// Handles general bot errors
bot.on('error', (error) => {
    console.error('❌ General Bot Error:', error);
    performanceMonitor.logRequest(false); // Log as failed request
});


// Central handler for all incoming messages (routed by command)
async function handleMessage(msg) {
    if (!msg || !msg.from || msg.from.is_bot || !msg.chat || !msg.text) {
        return; // Ignore non-text messages, messages from bots, or incomplete messages
    }

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const messageText = msg.text;

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
        const commandMatch = text.match(/^\/(\w+)(?:@\w+)?(?:\s+(.*))?$/); // Extracts command and arguments

        if (!commandMatch) return; // Not a command format

        const command = commandMatch[1].toLowerCase();
        const args = commandMatch[2] || ''; // Arguments string

        // Log received command for debugging
        // console.log(`Received command: /${command} from user ${userId} in chat ${chatId}. Args: "${args}"`);

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
            default:
                // console.log(`Ignoring unknown command: /${command}`); // Optional debug log
                 break; // Ignore unknown commands silently
        }

        performanceMonitor.logRequest(true); // Log successful command handling attempt

    } catch (error) {
        console.error(`❌ Error processing message from user ${userId} in chat ${chatId}: "${messageText}"`, error);
        performanceMonitor.logRequest(false); // Log error
        try {
            await safeSendMessage(chatId, "⚠️ An unexpected error occurred. Please try again later or contact support.");
        } catch (tgError) {
            console.error("❌ Failed to send error message to chat:", tgError.message);
        }
    } finally {
        // Optional: Periodic cleanup of cooldown map (simple approach)
        const cutoff = Date.now() - 60000; // Remove entries older than 1 minute
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
    // Basic sanitization for HTML/Markdown special chars
    const sanitizedFirstName = firstName.replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/[*_`\[]/g, '\\$&');
    const welcomeText = `👋 Welcome, ${sanitizedFirstName}!\n\n` +
                         `🎰 *Solana Gambles Bot*\n\n` +
                         `Use /coinflip or /race to see game options.\n` +
                         `Use /wallet to view your linked Solana wallet.\n` +
                         `Use /help to see all commands.`;
    const bannerUrl = 'https://i.ibb.co/9vDo58q/banner.gif'; // Keep banner URL

    try {
        // Try sending animation first
        await bot.sendAnimation(msg.chat.id, bannerUrl, {
            caption: welcomeText,
            parse_mode: 'Markdown'
        }).catch(async (animError) => {
             // If animation fails (e.g., URL invalid, bot blocked by user, file too large), send text fallback
             console.warn("⚠️ Failed to send start animation, sending text fallback:", animError.message);
             await safeSendMessage(msg.chat.id, welcomeText, { parse_mode: 'Markdown' });
        });
    } catch (fallbackError) {
         // Catch errors from the fallback send itself
         console.error("TG Send Error (start fallback):", fallbackError.message);
    }
}


// Handles the /coinflip command (shows instructions) - Uses HTML formatting
async function handleCoinflipCommand(msg) {
    try {
        const config = GAME_CONFIG.coinflip;

        // Using HTML tags (<b> for bold, <code> for inline code)
        const messageText = `🪙 <b>Coinflip Game</b> 🪙\n\n` +
            `Bet on Heads or Tails!\n\n` +
            `<b>How to play:</b>\n` +
            `1. Type <code>/bet amount heads</code> (e.g., <code>/bet 0.1 heads</code>)\n` +
            `2. Type <code>/bet amount tails</code> (e.g., <code>/bet 0.1 tails</code>)\n\n` +
            `<b>Rules:</b>\n` +
            `- Min Bet: ${config.minBet} SOL\n` +
            `- Max Bet: ${config.maxBet} SOL\n` +
            `- House Edge: ${(config.houseEdge * 100).toFixed(1)}%\n` +
            `- Payout: ~${(2.0 * (1.0 - config.houseEdge)).toFixed(2)}x (Win Amount = Bet * ${(2.0 * (1.0 - config.houseEdge)).toFixed(2)}x)\n\n` +
            `You will be given a wallet address and a <b>unique Memo ID</b>. Send the <b>exact</b> SOL amount with the memo to place your bet.`;

        await safeSendMessage(msg.chat.id, messageText, { parse_mode: 'HTML' })
            .catch(e => {
                console.error("TG Send Error (HTML - within handleCoinflipCommand catch):", e.message);
            });
    } catch (error) {
        console.error("Error INSIDE handleCoinflipCommand:", error);
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

    let raceMessage = `🐎 *Horse Race Game* 🐎\n\nBet on the winning horse!\n\n*Available Horses & Approx Payout (Bet x Odds After House Edge):*\n`;
    horses.forEach(horse => {
        const effectiveMultiplier = (horse.odds * (1.0 - GAME_CONFIG.race.houseEdge)).toFixed(2);
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
    const memoId = generateMemoId('CF'); // Uses new enhanced generator (generates V1 format)
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
        return;
    }

    // Send payment instructions
    await safeSendMessage(chatId,
        `✅ Coinflip bet registered! (ID: \`${memoId}\`)\n\n` + // Use code formatting for Memo ID
        `You chose: *${userChoice}*\n` +
        `Amount: *${betAmount.toFixed(Math.max(2, (betAmount.toString().split('.')[1] || '').length))} SOL*\n\n` + // Display appropriate decimals
        `➡️ Send *exactly ${betAmount.toFixed(Math.max(2, (betAmount.toString().split('.')[1] || '').length))} SOL* to:\n` +
        `\`${process.env.MAIN_WALLET_ADDRESS}\`\n\n` +
        `📎 *Include MEMO:* \`${memoId}\`\n\n` + // Use code formatting for Memo ID
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
            `⚠️ Invalid horse name: "${chosenHorseNameInput}". Please choose from the list in /race.`
        ).catch(e => console.error("TG Send Error:", e.message));
        return;
    }

    const memoId = generateMemoId('RA'); // Uses new enhanced generator (generates V1 format)
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
        return;
    }

    // Calculate potential payout for display
    const potentialPayoutLamports = calculatePayout(expectedLamports, 'race', chosenHorse);
    const potentialPayoutSOL = (Number(potentialPayoutLamports) / LAMPORTS_PER_SOL).toFixed(6);

    // Send payment instructions
    await safeSendMessage(chatId,
        `✅ Race bet registered! (ID: \`${memoId}\`)\n\n` + // Use code formatting for Memo ID
        `You chose: ${chosenHorse.emoji} *${chosenHorse.name}*\n` +
        `Amount: *${betAmount.toFixed(Math.max(2, (betAmount.toString().split('.')[1] || '').length))} SOL*\n` + // Display appropriate decimals
        `Potential Payout: ~${potentialPayoutSOL} SOL\n\n`+
        `➡️ Send *exactly ${betAmount.toFixed(Math.max(2, (betAmount.toString().split('.')[1] || '').length))} SOL* to:\n` +
        `\`${process.env.RACE_WALLET_ADDRESS}\`\n\n` +
        `📎 *Include MEMO:* \`${memoId}\`\n\n` + // Use code formatting for Memo ID
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
                     `*Support:* If you encounter issues, please contact support.`; // Replace placeholder

    await safeSendMessage(msg.chat.id, helpText, { parse_mode: 'Markdown' }).catch(e => console.error("TG Send Error:", e.message));
}


// --- Server Startup & Shutdown Logic ---
// (Remains structurally the same)

// Encapsulated Webhook Setup Logic
async function setupTelegramWebhook() {
    if (process.env.RAILWAY_ENVIRONMENT && process.env.RAILWAY_PUBLIC_DOMAIN) {
        const webhookUrl = `https://${process.env.RAILWAY_PUBLIC_DOMAIN}${webhookPath}`;
        console.log(`Attempting to set webhook to: ${webhookUrl}`);
        let attempts = 0;
        while (attempts < 3) {
            try {
                await bot.deleteWebHook({ drop_pending_updates: true });
                await bot.setWebHook(webhookUrl, { max_connections: 3 }); // Max connections set to 3
                console.log(`✅ Webhook successfully set to: ${webhookUrl}`);
                return true; // Indicate webhook was set
            } catch (webhookError) {
                attempts++;
                console.error(`❌ Webhook setup attempt ${attempts} failed:`, webhookError.message);
                if (attempts >= 3) {
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
     return false; // Default return if conditions not met
}

// Encapsulated Polling Setup Logic
async function startPollingIfNeeded() {
    try {
        const info = await bot.getWebHookInfo();
        if (!info || !info.url) { // Start polling only if webhook is not set
             // Added check to prevent polling start if already polling
             if (bot.isPolling()) {
                 console.log("ℹ️ Bot is already polling.");
                 return;
             }
            console.log("ℹ️ Webhook not set, starting bot polling...");
            await bot.deleteWebHook({ drop_pending_updates: true }); // Ensure no residual webhook
            await bot.startPolling({ polling: { interval: 300 } }); // Adjust interval if needed
            console.log("✅ Bot polling started successfully");
        } else {
            console.log(`ℹ️ Webhook is set (${info.url}), polling will not be started.`);
             // Ensure polling is stopped if webhook is somehow set later
             if (bot.isPolling()) {
                 console.log("ℹ️ Stopping polling because webhook is set.");
                 await bot.stopPolling({ cancel: true });
             }
        }
    } catch (err) {
        console.error("❌ Error managing polling state:", err.message);
        if (err.message.includes('409 Conflict')) {
            console.error("❌❌❌ Conflict detected! Another instance might be polling.");
            console.error("❌ Exiting due to conflict."); process.exit(1);
        }
         // Handle other potential errors (e.g., network issues communicating with Telegram API)
    }
}

// Encapsulated Payment Monitor Start Logic
function startPaymentMonitor() {
    if (monitorInterval) {
         console.log("ℹ️ Payment monitor already running.");
         return; // Prevent multiple intervals
    }
    console.log(`⚙️ Starting payment monitor (Initial Interval: ${monitorIntervalSeconds}s)`);
    monitorInterval = setInterval(() => {
         try {
             monitorPayments().catch(err => {
                 console.error('[FATAL MONITOR ERROR in setInterval catch]:', err);
                 performanceMonitor.logRequest(false);
             });
         } catch (syncErr) {
             console.error('[FATAL MONITOR SYNC ERROR in setInterval try/catch]:', syncErr);
             performanceMonitor.logRequest(false);
         }
    }, monitorIntervalSeconds * 1000);

    // Run monitor once shortly after initialization completes
    setTimeout(() => {
        console.log("⚙️ Performing initial payment monitor run...");
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
}


// --- NEW Graceful shutdown handler (Modified for Railway Rotations) ---
const shutdown = async (signal, isRailwayRotation = false) => { // Added isRailwayRotation param
    console.log(`\n🛑 ${signal} received, ${isRailwayRotation ? 'container rotation' : 'shutting down gracefully'}...`);

    if (isRailwayRotation) {
        // For Railway rotations, just stop accepting new connections quickly
        console.log("Railway container rotation detected - minimizing disruption.");
        if (server) {
            // Don't wait for full close, just initiate it. Railway manages the rest.
            server.close(() => console.log("- Stopped accepting new server connections for rotation."));
        }
        return; // Exit the shutdown function early for rotation
    }

    // --- START: Original Full Shutdown Logic (for non-rotation signals like SIGINT or manual stop) ---
    console.log("Performing full graceful shutdown sequence...");
    isMonitorRunning = true; // Prevent monitor from starting new cycle during shutdown

    // 1. Stop receiving new events/requests
    console.log("Stopping incoming connections and tasks...");
    if (monitorInterval) {
        clearInterval(monitorInterval);
        console.log("- Stopped payment monitor interval.");
    }
    try {
        // Stop server from accepting new connections (wait for close)
        if (server) {
             await new Promise((resolve, reject) => {
                 server.close((err) => {
                     if (err) {
                         console.error("⚠️ Error closing Express server:", err.message);
                         return reject(err);
                     }
                     console.log("- Express server closed.");
                     resolve(undefined); // Resolve explicitly with undefined for void promise
                 });
                 // Add a timeout for server close
                 setTimeout(() => reject(new Error('Server close timeout')), 5000).unref();
             });
        }

        // Try deleting webhook regardless of env var
        try {
            const webhookInfo = await bot.getWebHookInfo();
            if (webhookInfo && webhookInfo.url) {
                await bot.deleteWebHook({ drop_pending_updates: true });
                console.log("- Removed Telegram webhook.");
            }
        } catch (whErr) {
            console.error("⚠️ Error removing webhook during shutdown:", whErr.message);
        }

        // Stop polling if it was active
        if (bot.isPolling()) {
            await bot.stopPolling({ cancel: true }); // Cancel polling
            console.log("- Stopped Telegram polling.");
        }
    } catch (e) {
        console.error("⚠️ Error stopping listeners/server:", e.message);
    }

    // 2. Wait for ongoing queue processing to finish (with timeout)
    console.log("Waiting for active jobs to finish (max 15s)...");
    try {
        await Promise.race([
            Promise.all([
                messageQueue.onIdle(),
                paymentProcessor.highPriorityQueue.onIdle(), // Use correct processor instance
                paymentProcessor.normalQueue.onIdle(),     // Use correct processor instance
                telegramSendQueue.onIdle() // Wait for telegram send queue too
            ]),
            new Promise((_, reject) => setTimeout(() => reject(new Error('Queue drain timeout (15s)')), 15000)) // Increased timeout
        ]);
        console.log("- All processing queues are idle.");
    } catch (queueError) {
        console.warn("⚠️ Timed out waiting for queues or queue error during shutdown:", queueError.message);
    }

    // 3. Close database pool
    console.log("Closing database pool...");
    try {
        await pool.end(); // Close all connections in the pool
        console.log("✅ Database pool closed.");
    } catch (dbErr) {
        console.error("❌ Error closing database pool:", dbErr);
    } finally {
        console.log("🛑 Full Shutdown complete.");
        process.exit(0); // Exit cleanly after full shutdown
    }
    // --- END: Original Full Shutdown Logic ---
};


// --- UPDATED Signal handlers for graceful shutdown ---
// Detect Railway container rotations (SIGTERM with RAILWAY_ENVIRONMENT)
process.on('SIGTERM', () => {
    const isRailwayRotation = !!process.env.RAILWAY_ENVIRONMENT;
    console.log(`SIGTERM received. Railway Environment: ${isRailwayRotation}`);
    shutdown('SIGTERM', isRailwayRotation).catch((err) => {
        console.error("Error during SIGTERM shutdown:", err);
        process.exit(1); // Force exit if shutdown logic errors
    });
});

process.on('SIGINT', () => { // Ctrl+C remains full shutdown
    console.log(`SIGINT received.`);
    shutdown('SIGINT', false).catch((err) => { // Explicitly pass false
        console.error("Error during SIGINT shutdown:", err);
        process.exit(1);
    });
});

// Handle uncaught exceptions
process.on('uncaughtException', (err, origin) => {
    console.error(`🔥🔥🔥 Uncaught Exception at: ${origin}`, err);
    // Attempt full graceful shutdown, then force exit if needed
    shutdown('UNCAUGHT_EXCEPTION', false).catch(() => process.exit(1)); // Pass false
    setTimeout(() => {
        console.error("Shutdown timed out after uncaught exception. Forcing exit.");
        process.exit(1);
    }, 12000).unref(); // Give shutdown 12 seconds
});

// Handle unhandled promise rejections
process.on('unhandledRejection', (reason, promise) => {
    console.error('🔥🔥🔥 Unhandled Rejection at:', promise, 'reason:', reason);
    // Optional: Decide if shutdown is needed. For now, just logging.
    // Consider shutting down if the rejection indicates a critical state.
    // shutdown('UNHANDLED_REJECTION', false).catch(() => process.exit(1));
    // setTimeout(() => process.exit(1), 12000).unref();
});

// --- Start the Application (MODIFIED Startup Sequence - Fix #2 & #5) ---
const PORT = process.env.PORT || 3000;

// Start server immediately, listen on 0.0.0.0
server = app.listen(PORT, "0.0.0.0", () => { // Assign to the globally declared 'server'
    console.log(`🚀 Server running on port ${PORT}`);

    // Start heavy initialization AFTER server is listening, with a short delay
    // This ensures the /health endpoint is responsive immediately.
    setTimeout(async () => {
        console.log("⚙️ Starting delayed background initialization...");
        try {
            // Initialization Steps (Progress Tracking - Fix #3 via logs)
            console.log("  - Initializing Database...");
            await initializeDatabase(); // Initialize DB first (with fixes)
            console.log("  - Setting up Telegram...");
            const webhookSet = await setupTelegramWebhook(); // Setup webhook (if applicable)
            if (!webhookSet) { // If webhook wasn't set (or failed)
                await startPollingIfNeeded(); // Attempt to start polling
            }
            console.log("  - Starting Payment Monitor...");
            startPaymentMonitor(); // Start the monitor loop regardless of webhook/polling

             // --- BONUS: Solana Connection Boost after 20s (MODIFIED) ---
             setTimeout(() => {
                  if (solanaConnection && solanaConnection.options) {
                       console.log("⚡ Adjusting Solana connection concurrency...");
                       solanaConnection.options.maxConcurrent = 3; // Set max parallel requests to 3 (Adjust if needed)
                       console.log("✅ Solana maxConcurrent adjusted to 3");
                  }
             }, 20000); // Adjust after 20 seconds of being fully ready

            isFullyInitialized = true; // Mark as fully initialized *after* setup completes
            console.log("✅ Delayed Background Initialization Complete. Bot is fully ready.");
            console.log("🚀🚀🚀 Solana Gambles Bot is up and running! 🚀🚀🚀");

        } catch (initError) {
            console.error("🔥🔥🔥 Delayed Background Initialization Error:", initError);
            // If initialization fails, the bot might be unusable.
            // Attempt a graceful shutdown before exiting.
            console.error("❌ Exiting due to critical initialization failure.");
            await shutdown('INITIALIZATION_FAILURE', false).catch(() => process.exit(1)); // Attempt full shutdown
            // Ensure exit if shutdown hangs
             setTimeout(() => {
                  console.error("Shutdown timed out after initialization failure. Forcing exit.");
                  process.exit(1);
             }, 10000).unref();
        }
    }, 1000); // 1-second delay before starting heavy init
});

// Handle server startup errors (like EADDRINUSE) which happen *before* the listen callback
server.on('error', (err) => {
    console.error('❌ Server startup error:', err);
    if (err.code === 'EADDRINUSE') {
        console.error(`❌❌❌ Port ${PORT} is already in use. Exiting.`);
    }
    process.exit(1); // Exit on any server startup error
});

// (Part 5/5 Ends Here - End of File)
