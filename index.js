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
    maxConcurrent: 3,      // Max parallel requests
    retryBaseDelay: 600,     // Initial delay for retries (ms)
    commitment: 'confirmed',  // Default commitment level
    httpHeaders: {
        'Content-Type': 'application/json',
        'solana-client': `SolanaGamblesBot/2.1 (${process.env.RAILWAY_ENVIRONMENT ? 'railway' : 'local'})` // Client version bump
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

// --- Database Initialization (Updated) ---
async function initializeDatabase() {
    console.log("⚙️ Initializing Database schema...");
    let client;
    try {
        client = await pool.connect();

        // Bets Table: Tracks individual game bets
        await client.query(`
            CREATE TABLE IF NOT EXISTS bets (
                id SERIAL PRIMARY KEY,                      -- Unique bet identifier
                user_id TEXT NOT NULL,                      -- Telegram User ID
                chat_id TEXT NOT NULL,                      -- Telegram Chat ID
                game_type TEXT NOT NULL,                    -- 'coinflip' or 'race'
                bet_details JSONB,                          -- Game-specific details (choice, horse, odds)
                expected_lamports BIGINT NOT NULL,          -- Amount user should send (in lamports)
                memo_id TEXT UNIQUE NOT NULL,               -- Unique memo for payment tracking
                status TEXT NOT NULL,                       -- Bet status (e.g., 'awaiting_payment', 'completed_win_paid', 'error_...')
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- When the bet was initiated
                expires_at TIMESTAMPTZ NOT NULL,            -- When the payment window closes
                paid_tx_signature TEXT UNIQUE,              -- Signature of the user's payment transaction
                payout_tx_signature TEXT UNIQUE,            -- Signature of the bot's payout transaction (if win)
                processed_at TIMESTAMPTZ,                   -- When the bet was fully resolved
                fees_paid BIGINT,                           -- Estimated fees buffer associated with this bet
                priority INT DEFAULT 0                      -- Priority for processing (higher first)
            );
        `);

        // Wallets Table: Links Telegram User ID to their Solana wallet address
        await client.query(`
            CREATE TABLE IF NOT EXISTS wallets (
                user_id TEXT PRIMARY KEY,                   -- Telegram User ID
                wallet_address TEXT NOT NULL,               -- User's Solana wallet address
                linked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- When the wallet was first linked
                last_used_at TIMESTAMPTZ                    -- When the wallet was last used for a bet/payout
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
        version: '2.1.1', // Bot version (incremented for memo fix)
        queueStats: { // Report queue status
            pending: messageQueue.size + (paymentProcessor?.highPriorityQueue?.size || 0) + (paymentProcessor?.normalQueue?.size || 0), // Combined pending
            active: messageQueue.pending + (paymentProcessor?.highPriorityQueue?.pending || 0) + (paymentProcessor?.normalQueue?.pending || 0) // Combined active
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

// --- REVISED: normalizeMemo function (simplified, less risky) ---
// Attempts basic cleaning and validation. Avoids risky transformations.
function normalizeMemo(rawMemo) {
    if (!rawMemo || typeof rawMemo !== 'string') {
        // console.log('[NormalizeMemo] Input is not a non-empty string:', rawMemo);
        return null;
    }

    // 1. Trim whitespace
    let memo = rawMemo.trim();

    // 2. Remove common prefixes ONLY if they are clearly separated (e.g., by space or colon)
    // This is safer than blind replacement.
    memo = memo.replace(/^(memo|text):\s*/i, ''); // Remove "memo: " or "text: "

    // 3. Check if it NOW matches the strict format directly
    if (validateOriginalMemoFormat(memo)) {
        // console.log(`[NormalizeMemo] Raw memo "${rawMemo}" normalized to strictly valid "${memo}"`);
        return memo;
    }

    // 4. If not strictly valid, attempt basic split/check (less strict)
    // Use hyphen as the primary separator
    const parts = memo.split('-');
    if (parts.length === 2) {
        const prefix = parts[0].toUpperCase();
        const idPart = parts[1].toUpperCase(); // Standardize case

        // Check prefix and if ID part *looks* like hex (but allow length variations)
        if (['BET', 'CF', 'RA'].includes(prefix) && /^[A-F0-9]+$/.test(idPart)) {
            const normalized = `${prefix}-${idPart}`;
            // Only return if it didn't match strictly before but looks okay now
             console.warn(`[NormalizeMemo] Memo "${normalized}" from raw "${rawMemo}" does not match strict format (PREFIX-HEX12) but has valid prefix and hex ID. Accepting leniently.`);
             return normalized; // Return the leniently matched format
        }
    }

    // console.log(`[NormalizeMemo] Memo "${rawMemo}" could not be normalized to a valid/acceptable format.`);
    return null; // Could not normalize to a usable format
}
// --- END REVISED ---


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

// --- REVISED: findMemoInTx with Hybrid Approach Support ---
// Returns:
// { found: true, dataAvailable: true, memo: string } - Success with parsed data
// { found: true, dataAvailable: false, index: number } - Memo instruction found, data missing in parsed response
// { found: false } - No memo instruction found
function findMemoInTx(tx) {
    const logPrefix = `[FindMemo:${tx?.transaction?.signatures[0]?.substring(0, 8) || 'SIG N/A'}]`;

    if (!tx?.transaction?.message?.instructions) {
        console.log(`${logPrefix} No instructions found in transaction message.`);
        return { found: false }; // Return specific object
    }

    const MEMO_PROGRAM_ID_V1 = 'Memo1UhkJRfHyvLMcVuc6beZNRYqUP2VZwW'; // v1
    const MEMO_PROGRAM_ID_V2 = 'MemoSq4gqABAXKb96qnH8TysNcVtrp5GktfD'; // v2
    const accountKeys = tx.transaction.message.accountKeys;

    const getProgramId = (index) => {
        // ... (getProgramId helper function remains the same) ...
        if (index === undefined || !accountKeys || index >= accountKeys.length) {
            return null;
        }
        const keyInfo = accountKeys[index];
        if (keyInfo?.pubkey) return keyInfo.pubkey.toBase58();
        if (typeof keyInfo === 'string') return new PublicKey(keyInfo).toBase58();
        if (keyInfo instanceof PublicKey) return keyInfo.toBase58();
        return null;
    };

    console.log(`${logPrefix} Searching ${tx.transaction.message.instructions.length} top-level instructions...`);

    for (let i = 0; i < tx.transaction.message.instructions.length; i++) {
        const instruction = tx.transaction.message.instructions[i];
        let programId = null;

        if (instruction.programIdIndex !== undefined) {
            programId = getProgramId(instruction.programIdIndex);
        } else if (instruction.programId) {
             programId = instruction.programId.toBase58 ?
                           instruction.programId.toBase58() :
                           String(instruction.programId);
        }

        // console.log(`${logPrefix} Inst #${i}: Prog ID Index=${instruction.programIdIndex}, Resolved Prog ID="${programId}", Data=${instruction.data ? '"'+instruction.data+'"' : 'N/A'}`); // Keep log concise

        if (programId === MEMO_PROGRAM_ID_V1 || programId === MEMO_PROGRAM_ID_V2) {
            console.log(`${logPrefix} Found potential Memo instruction (#${i}) with Program ID ${programId}.`);
            // *** MODIFICATION START: Check data availability ***
            // Check if data exists and is a non-empty string (required for bs58 decoding)
            if (instruction.data && typeof instruction.data === 'string') {
                 console.log(`${logPrefix} Instruction data field is present. Attempting decoding...`);
                 let decodedMemo = null;
                 try {
                     const buffer = bs58.decode(instruction.data);
                     decodedMemo = buffer.toString('utf-8');
                     console.log(`${logPrefix} Decoded bs58 data to UTF-8: "${decodedMemo}"`);
                 } catch (decodeError) {
                     console.error(`${logPrefix} Error decoding bs58 data for Memo instruction:`, decodeError.message);
                     console.error(`${logPrefix} Raw base58 data was: ${instruction.data}`);
                     continue; // Try next instruction if decoding fails
                 }

                 console.log(`${logPrefix} Normalizing decoded memo: "${decodedMemo}"`);
                 const normalizedMemo = normalizeMemo(decodedMemo);
                 console.log(`${logPrefix} Normalization result: "${normalizedMemo}"`);

                 if (normalizedMemo) {
                      console.log(`${logPrefix} Successfully found and normalized memo: "${normalizedMemo}"`);
                      // Return success object
                      return { found: true, dataAvailable: true, memo: normalizedMemo };
                 } else {
                      console.warn(`${logPrefix} Decoded memo "${decodedMemo}" could not be normalized. Continuing search...`);
                      // Continue loop to check other instructions
                 }
            } else {
                 // Data field is missing or not a string (e.g., the 'Data=N/A' case)
                 console.warn(`${logPrefix} Memo instruction (#${i}) found, but 'data' field is missing or invalid in parsed response. Raw data fetch might be needed.`);
                 // Return object indicating data is missing
                 return { found: true, dataAvailable: false, index: i };
            }
            // *** MODIFICATION END ***
        }
    } // End loop through instructions

    console.log(`${logPrefix} No valid memo instruction found after checking all instructions.`);
    return { found: false }; // Return specific object if loop finishes
}
// --- END REVISED ---

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

    // *** This function RELIES on getParsedTransaction output. ***
    // *** It will NOT work correctly if the input `tx` comes from `getTransaction`. ***
    if (tx?.meta && tx?.transaction?.message?.instructions) {
        const allInstructions = [
            ...(tx.transaction.message.instructions || []).map((inst, index) => ({ ...inst, parentIndex: index, isInner: false })), // Add parent index
            ...(tx.meta.innerInstructions || []).flatMap(inner =>
                 inner.instructions.map(inst => ({ ...inst, parentIndex: inner.index, isInner: true })) // Add parent index
            )
        ];

        const SYSTEM_PROGRAM_ID = SystemProgram.programId.toBase58();
        const accountKeys = tx.transaction.message.accountKeys; // Cache for faster access

        const getProgramId = (instruction) => {
             if (instruction.programIdIndex !== undefined && accountKeys) {
                 const keyInfo = accountKeys[instruction.programIdIndex];
                 if (keyInfo?.pubkey) return keyInfo.pubkey.toBase58();
                 if (typeof keyInfo === 'string') return new PublicKey(keyInfo).toBase58();
                 if (keyInfo instanceof PublicKey) return keyInfo.toBase58();
             } else if (instruction.programId) {
                 return instruction.programId.toBase58 ? instruction.programId.toBase58() : String(instruction.programId);
             }
             return null;
        };

        for (const inst of allInstructions) {
             const programId = getProgramId(inst);

             // Check for SystemProgram SOL transfers (works for parsed instructions)
             // >>> THIS IS THE PART THAT REQUIRES PARSED DATA <<<
             if (programId === SYSTEM_PROGRAM_ID && inst.parsed?.type === 'transfer') {
                 const transferInfo = inst.parsed.info;
                 if (transferInfo.destination === targetAddress) {
                     transferAmount += BigInt(transferInfo.lamports || transferInfo.amount || 0);
                     if (!payerAddress && transferInfo.source) {
                         payerAddress = transferInfo.source;
                     }
                 }
             }
             // >>> END OF PARSED DATA DEPENDENCY <<<
        }

        if (!payerAddress) {
             const feePayer = getPayerFromTransaction(tx);
             if (feePayer) {
                 payerAddress = feePayer.toBase58();
                 // console.log(`[AnalyzeTx] Using fee payer ${payerAddress} as payer address.`);
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
     if (message.accountKeys.length > 0) {
         const feePayerKeyInfo = message.accountKeys[0];
         let feePayerAddressStr = null;
         if (feePayerKeyInfo?.pubkey) {
             feePayerAddressStr = feePayerKeyInfo.pubkey.toBase58 ? feePayerKeyInfo.pubkey.toBase58() : String(feePayerKeyInfo.pubkey);
         } else if (typeof feePayerKeyInfo === 'string') {
             feePayerAddressStr = feePayerKeyInfo;
         } else if (feePayerKeyInfo instanceof PublicKey) {
             feePayerAddressStr = feePayerKeyInfo.toBase58();
         }

         // console.log(`Identified fee payer as ${feePayerAddressStr}`); // Optional log
         try {
             return feePayerAddressStr ? new PublicKey(feePayerAddressStr) : null; // Return PublicKey or null
         } catch (e) {
             console.error(`[GetPayer] Error creating PublicKey from fee payer address: ${feePayerAddressStr}`, e.message);
             return null;
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
        const queue = job.priority > 0 ? this.highPriorityQueue : this.normalQueue;
        // Don't await here, let queue manage execution
        queue.add(() => this.processJob(job)).catch(err => {
             console.error(`Error adding job type ${job.type} to queue:`, err);
             // Handle error adding job itself if necessary
        });
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
            const jobIdentifier = job.signature || `BetID ${job.betId}` || 'Unknown Job';
            console.error(`Error processing job type ${job.type} for ${jobIdentifier}:`, error.message);
            // Log stack trace for unexpected errors
            if (!isRetryableError(error)) {
                 console.error(error.stack);
            }

            // Retry logic for retryable errors (only for initial payment check for now)
            if (job.type === 'monitor_payment' && (job.retries || 0) < 3 && isRetryableError(error)) {
                 job.retries = (job.retries || 0) + 1;
                 console.log(`Retrying job for signature ${job.signature} (Attempt ${job.retries})...`);
                 await new Promise(resolve => setTimeout(resolve, 1000 * job.retries)); // Exponential backoff
                 // Release active lock before requeueing for retry
                 if (job.signature) this.activeProcesses.delete(job.signature);
                 // DO NOT await here, let the requeue happen in the background
                  this.addPaymentJob(job).catch(requeueError => { // Catch errors during requeueing itself
                      console.error(`Failed to requeue job for signature ${job.signature} after error:`, requeueError);
                  });
                 return; // Prevent falling through to finally block immediately after requeueing
            } else {
                // Log final failure or non-retryable error
                 console.error(`Job failed permanently or exceeded retries: ${jobIdentifier}`, error);
                 // Potentially update bet status to an error state if applicable
                 if(job.betId && job.type !== 'monitor_payment') {
                     // Check if status is still one that makes sense to mark as error
                     try {
                         const currentStatusRes = await pool.query('SELECT status FROM bets WHERE id = $1', [job.betId]);
                         const currentStatus = currentStatusRes.rows[0]?.status;
                         // Only mark as error if it's in a state where this makes sense (not already completed/errored differently)
                         const errorStatus = `error_${job.type}_failed`.substring(0, 50); // Ensure status isn't too long
                         if (currentStatus && !currentStatus.startsWith('completed_') && !currentStatus.startsWith('error_')) {
                             await updateBetStatus(job.betId, errorStatus);
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


    // **** START: _processIncomingPayment with Hybrid Approach Logic ****
    async _processIncomingPayment(signature, walletType, attempt = 0) {
        const logPrefix = `[PaymentProc:${signature?.substring(0, 8) || 'SIG N/A'}]`;
        console.log(`${logPrefix} Starting processing (Attempt ${attempt + 1}) for wallet type ${walletType}.`);

        try {
            // 1. Check session cache
            if (processedSignaturesThisSession.has(signature)) {
                console.log(`${logPrefix} Skipped: Already processed in this session cache.`);
                return { processed: false, reason: 'already_processed_session' };
            }
            // console.log(`${logPrefix} Passed session cache check.`); // Less verbose

            // 2. Check database if already recorded
            const checkQuery = `SELECT id FROM bets WHERE paid_tx_signature = $1 LIMIT 1;`;
            try {
                const processed = await pool.query(checkQuery, [signature]);
                if (processed.rowCount > 0) {
                    console.log(`${logPrefix} Skipped: Signature already exists in DB (Bet ID: ${processed.rows[0].id}).`);
                    processedSignaturesThisSession.add(signature);
                    return { processed: false, reason: 'exists_in_db' };
                }
                // console.log(`${logPrefix} Passed DB signature check.`); // Less verbose
            } catch (dbError) {
                console.error(`${logPrefix} DB Error checking signature:`, dbError.message);
                 if (isRetryableError(dbError)) throw dbError;
                 return { processed: false, reason: 'db_check_error' };
            }

            // 3. Fetch PARSED transaction details (Primary attempt)
            console.log(`${logPrefix} Fetching *parsed* transaction details from Solana...`);
            let tx; // Parsed transaction
            try {
                tx = await solanaConnection.getParsedTransaction(
                    signature,
                    { maxSupportedTransactionVersion: 0 }
                );
            } catch (fetchError) {
                console.error(`${logPrefix} Error fetching parsed transaction details:`, fetchError.message);
                if (isRetryableError(fetchError)) throw fetchError;
                 throw new Error(`Non-retryable error fetching parsed TX ${signature}: ${fetchError.message}`);
            }

            // 4. Validate parsed transaction
            if (!tx) {
                console.warn(`${logPrefix} Parsed transaction object is null after fetch attempt.`);
                throw new Error(`Parsed transaction ${signature} null after fetch attempt`);
            }
            // console.log(`${logPrefix} Parsed transaction details fetched successfully.`); // Less verbose

            if (tx.meta?.err) {
                console.log(`${logPrefix} Skipped: Transaction failed on-chain: ${JSON.stringify(tx.meta.err)}`);
                processedSignaturesThisSession.add(signature);
                return { processed: false, reason: 'tx_onchain_error' };
            }
            // console.log(`${logPrefix} Passed on-chain success check.`); // Less verbose

            // *** MODIFICATION START: Handle different findMemoInTx results ***
            // 5. Find memo using the revised findMemoInTx
            const memoResult = findMemoInTx(tx); // Returns { found: bool, dataAvailable: bool, memo?: string, index?: number }
            let finalMemo = null; // This will hold the validated memo string

            if (memoResult.found && memoResult.dataAvailable) {
                // Case 1: Memo found directly in parsed data
                console.log(`${logPrefix} Memo found directly via parsed data: "${memoResult.memo}"`);
                finalMemo = memoResult.memo; // Already normalized and validated format

            } else if (memoResult.found && !memoResult.dataAvailable) {
                // Case 2: Memo instruction found, but data missing in parsed response -> Fallback needed
                console.warn(`${logPrefix} Parsed data missing for Memo instruction index ${memoResult.index}. Attempting fallback fetch for raw data...`);
                let rawTx;
                try {
                    rawTx = await solanaConnection.getTransaction(signature, {
                        encoding: 'base64', // Request base64 encoding for instruction data
                        commitment: 'confirmed',
                        maxSupportedTransactionVersion: 0 // Required for fetching v0 txns
                    });

                    if (!rawTx || !rawTx.transaction?.message?.instructions) {
                        throw new Error('Raw transaction data or instructions missing in fallback fetch.');
                    }

                    const rawInstructions = rawTx.transaction.message.instructions;
                    if (memoResult.index >= rawInstructions.length) {
                         throw new Error(`Memo index ${memoResult.index} out of bounds for raw instructions (length ${rawInstructions.length}).`);
                    }
                    const memoInstructionRaw = rawInstructions[memoResult.index];

                    // Extract and decode raw data (assuming base64)
                    const rawData = memoInstructionRaw.data;
                    if (rawData && typeof rawData === 'string') {
                        const buffer = Buffer.from(rawData, 'base64');
                        const decodedMemo = buffer.toString('utf-8');
                        console.log(`${logPrefix} Fallback successful: Decoded raw memo data: "${decodedMemo}"`);

                        // Normalize the memo obtained from raw data
                        const normalizedRawMemo = normalizeMemo(decodedMemo);
                        if (normalizedRawMemo) {
                            console.log(`${logPrefix} Normalized raw memo: "${normalizedRawMemo}"`);
                            finalMemo = normalizedRawMemo;
                        } else {
                            console.warn(`${logPrefix} Failed to normalize memo extracted from raw data: "${decodedMemo}"`);
                        }
                    } else {
                        console.warn(`${logPrefix} Raw data field missing or not a string in fallback memo instruction.`);
                    }

                } catch (fallbackError) {
                    console.error(`${logPrefix} Error during fallback fetch/decode for memo:`, fallbackError.message);
                    // Don't retry here, proceed without memo if fallback fails
                }
            } else {
                // Case 3: No memo instruction found at all
                console.log(`${logPrefix} Skipped: No Memo program instruction found in transaction.`);
                // Don't add to cache, could be unrelated transaction
                return { processed: false, reason: 'no_memo_instruction' };
            }
            // *** MODIFICATION END ***

            // 6. Proceed only if we have a final, validated memo
            if (!finalMemo) {
                console.log(`${logPrefix} Skipped: Could not obtain a valid & normalized memo ID.`);
                // Don't add to cache, memo wasn't valid or extractable
                return { processed: false, reason: 'no_valid_memo_extracted' };
            }

            // Ensure the finalMemo actually matches the strict format needed for DB lookup
            if (!validateOriginalMemoFormat(finalMemo)) {
                 console.warn(`${logPrefix} Skipped: Extracted memo "${finalMemo}" does not match strict DB format after normalization/fallback.`);
                 return { processed: false, reason: 'memo_format_mismatch_strict' };
            }

            // 7. Find the *exact* bet using the strictly validated memo
            console.log(`${logPrefix} Searching DB for bet with strictly validated memo: "${finalMemo}"`);
            const bet = await findBetByMemo(finalMemo); // Uses strict check internally
            if (!bet) {
                console.warn(`${logPrefix} Skipped: No matching *pending* bet found for memo "${finalMemo}".`);
                // Don't add to cache, memo might be for old/processed bet
                return { processed: false, reason: 'no_matching_bet_strict' };
            }
            console.log(`${logPrefix} Found matching Bet ID: ${bet.id} for memo ${finalMemo}.`);

            // 8. Check bet status (redundant due to findBetByMemo query, but safe)
            if (bet.status !== 'awaiting_payment') {
                console.warn(`${logPrefix} Skipped: Bet ${bet.id} status is ${bet.status}, not 'awaiting_payment'.`);
                processedSignaturesThisSession.add(signature); // Add sig, bet already processed somehow
                return { processed: false, reason: 'bet_already_processed' };
            }
            // console.log(`${logPrefix} Passed bet status check ('awaiting_payment').`); // Less verbose

            // 9. Analyze transaction amounts (using the original parsed 'tx' object)
            // IMPORTANT: This relies on the initially fetched PARSED transaction 'tx'
            const { transferAmount, payerAddress } = analyzeTransactionAmounts(tx, walletType);
            console.log(`${logPrefix} Amount analysis: Found ${transferAmount} lamports transfer to target wallet from ${payerAddress || 'Unknown'}.`);
            if (transferAmount <= 0n) {
                console.warn(`${logPrefix} Skipped: No SOL transfer found TO THE CORRECT target wallet (${walletType} address).`);
                return { processed: false, reason: 'no_transfer_found_to_target' };
            }

            // 10. Validate amount sent vs expected
            const expectedAmount = BigInt(bet.expected_lamports);
            const tolerance = BigInt(5000);
            const lowerBound = expectedAmount - tolerance;
            const upperBound = expectedAmount + tolerance;
            console.log(`${logPrefix} Amount Check: Expected=${expectedAmount}, Received=${transferAmount}, Tolerance=${tolerance} (Range: [${lowerBound}, ${upperBound}])`);

            if (transferAmount < lowerBound || transferAmount > upperBound) {
                console.warn(`${logPrefix} Failed: Amount mismatch.`);
                await updateBetStatus(bet.id, 'error_payment_mismatch');
                processedSignaturesThisSession.add(signature);
                await bot.sendMessage(bet.chat_id, `⚠️ Payment amount mismatch for bet ${finalMemo}. Expected ${Number(expectedAmount)/LAMPORTS_PER_SOL} SOL, received ${Number(transferAmount)/LAMPORTS_PER_SOL} SOL. Bet cancelled.`).catch(e => console.error("TG Send Error:", e.message));
                return { processed: false, reason: 'amount_mismatch' };
            }
            // console.log(`${logPrefix} Passed amount check.`); // Less verbose

            // 11. Validate transaction time vs bet expiry
            const txTime = tx.blockTime ? new Date(tx.blockTime * 1000) : new Date(0);
            const betExpiresAt = new Date(bet.expires_at);
            console.log(`${logPrefix} Expiry Check: TxTime=${txTime.toISOString() || 'Unknown'}, ExpiresAt=${betExpiresAt.toISOString()}`);
            if (txTime === 0) {
                 console.warn(`${logPrefix} Could not determine blockTime for TX. Skipping expiry check.`);
            } else if (txTime > betExpiresAt) {
                console.warn(`${logPrefix} Failed: Payment received after expiry.`);
                await updateBetStatus(bet.id, 'error_payment_expired');
                processedSignaturesThisSession.add(signature);
                await bot.sendMessage(bet.chat_id, `⚠️ Payment for bet ${finalMemo} received after expiry time. Bet cancelled.`).catch(e => console.error("TG Send Error:", e.message));
                return { processed: false, reason: 'expired' };
            }
            // console.log(`${logPrefix} Passed expiry check.`); // Less verbose

            // 12. Mark bet as paid in DB
            console.log(`${logPrefix} Attempting to mark bet ${bet.id} as paid in DB...`);
            const markResult = await markBetPaid(bet.id, signature);
            if (!markResult.success) {
                console.error(`${logPrefix} Failed DB update: ${markResult.error}`);
                 processedSignaturesThisSession.add(signature);
                 if (isRetryableError(new Error(markResult.error || 'DB mark paid failed'))) {
                     throw new Error(markResult.error || 'Retryable DB mark paid failed');
                 }
                 return { processed: false, reason: 'db_mark_paid_failed' };
            }
            console.log(`${logPrefix} Successfully marked bet ${bet.id} as 'payment_verified'.`);

            // 13. Link wallet address to user ID
            let actualPayer = payerAddress || getPayerFromTransaction(tx)?.toBase58();
             console.log(`${logPrefix} Attempting to link wallet ${actualPayer || 'Unknown'} to user ${bet.user_id}...`);
            if (actualPayer) {
                try {
                    await linkUserWallet(bet.user_id, actualPayer);
                    // console.log(`${logPrefix} Wallet link attempt finished.`); // Less verbose
                } catch (linkErr) {
                     console.error(`${logPrefix} Non-critical error linking wallet:`, linkErr.message);
                }
            } else {
                 console.warn(`${logPrefix} Could not identify payer address to link wallet.`);
            }

            // 14. Add signature to session cache
            processedSignaturesThisSession.add(signature);
            // console.log(`${logPrefix} Added signature to session cache.`); // Less verbose
            if (processedSignaturesThisSession.size > MAX_PROCESSED_SIGNATURES) {
                 console.log(`${logPrefix} Clearing processed signatures cache (reached max size)`);
                 processedSignaturesThisSession.clear();
            }

            // 15. Queue the bet for actual game processing
            console.log(`${logPrefix} Queuing bet ${bet.id} for game processing.`);
            // Use addPaymentJob which doesn't await
            this.addPaymentJob({
                type: 'process_bet',
                betId: bet.id,
                priority: 1, // Higher priority than monitoring
                signature // Pass signature for context/logging if needed
            });

            console.log(`${logPrefix} Processing finished successfully.`);
            return { processed: true };

        } catch (error) { // Catch any unexpected errors during the process
            console.error(`${logPrefix} UNEXPECTED ERROR during processing:`, error);
            // Re-throw the error so the job processor knows it failed and can handle retries/final status
            throw error;
        }
    }
    // **** END: _processIncomingPayment with Hybrid Approach Logic ****
}

const paymentProcessor = new PaymentProcessor();


// --- Payment Monitoring Loop ---
let isMonitorRunning = false;
let monitorIntervalSeconds = 30;
let monitorInterval = null;

async function monitorPayments() {
    // console.log(`[${new Date().toISOString()}] ---- monitorPayments function START ----`);
    if (isMonitorRunning) {
        // console.log('[Monitor] Cycle already running, skipping.');
        return;
    }
    isMonitorRunning = true;
    const startTime = Date.now();
    let signaturesFound = 0;

    try {
        // --- START: Adaptive Rate Limiting Logic ---
        const currentLoad = paymentProcessor.highPriorityQueue.pending + paymentProcessor.normalQueue.pending;
        const throttleDelay = Math.min(5000, currentLoad * 50); // Adjusted delay
        if (throttleDelay > 0) {
            console.log(`[Monitor] Queues have ${currentLoad} pending items. Throttling monitor check for ${throttleDelay}ms.`);
            await new Promise(resolve => setTimeout(resolve, throttleDelay));
        }
        // --- END: Adaptive Rate Limiting Logic ---

        const walletsToMonitor = [
            { address: process.env.MAIN_WALLET_ADDRESS, type: 'coinflip', priority: 0 },
            { address: process.env.RACE_WALLET_ADDRESS, type: 'race', priority: 0 },
        ];

        for (const wallet of walletsToMonitor) {
            if (!wallet.address) {
                console.warn(`[Monitor] Skipping monitoring for ${wallet.type}: Address not defined.`);
                continue;
            }
            try {
                const beforeSig = lastProcessedSignature[wallet.address] || null;
                // Fetch latest signatures using rate-limited connection
                const signatures = await solanaConnection.getSignaturesForAddress(
                    new PublicKey(wallet.address),
                    { limit: 15, before: beforeSig }
                );

                if (!signatures || signatures.length === 0) {
                    continue;
                }
                signaturesFound += signatures.length;

                const oldestSignatureInBatch = signatures[signatures.length - 1].signature;
                lastProcessedSignature[wallet.address] = oldestSignatureInBatch;

                for (const sigInfo of signatures) {
                    if (processedSignaturesThisSession.has(sigInfo.signature)) {
                        continue;
                    }
                    // Queue the job - Use addPaymentJob which doesn't await
                    paymentProcessor.addPaymentJob({
                        type: 'monitor_payment',
                        signature: sigInfo.signature,
                        walletType: wallet.type,
                        priority: wallet.priority,
                        retries: 0
                    });
                }
            } catch (error) {
                console.error(`[Monitor] Error fetching/processing signatures for wallet ${wallet.address}:`, error.message);
                 if (!isRetryableError(error)) console.error(error.stack); // Log stack for non-retryable
                performanceMonitor.logRequest(false);
            }
        } // End loop through wallets

    } catch (error) {
        console.error("[Monitor] Unexpected error in main monitor loop:", error);
        console.error(error.stack);
        performanceMonitor.logRequest(false);
    } finally {
        isMonitorRunning = false;
        const duration = Date.now() - startTime;
        // if (signaturesFound > 0 || duration > 1000) { // Log if something found or took > 1s
        //     console.log(`[Monitor] Cycle finished in ${duration}ms. Found ${signaturesFound} new signatures.`);
        // }
        adjustMonitorInterval(signaturesFound);
        // console.log(`[${new Date().toISOString()}] ---- monitorPayments function END ----`);
    }
}

// Adjusts the monitor interval based on how many new signatures were found
function adjustMonitorInterval(processedCount) {
    let newInterval = monitorIntervalSeconds;
    if (processedCount > 10) { // High activity
        newInterval = Math.max(10, monitorIntervalSeconds - 5); // Min 10s
    } else if (processedCount === 0) { // No activity
        newInterval = Math.min(120, monitorIntervalSeconds + 10); // Max 120s
    }

    if (newInterval !== monitorIntervalSeconds && monitorInterval) {
        monitorIntervalSeconds = newInterval;
        clearInterval(monitorInterval);
        monitorInterval = setInterval(() => {
             // console.log(`[${new Date().toISOString()}] ==== Monitor Interval Fired (${monitorIntervalSeconds}s) ====`);
             try {
                 monitorPayments().catch(err => {
                     console.error('[FATAL MONITOR ERROR in setInterval catch]:', err);
                      if (err && err.stack) console.error(err.stack);
                     performanceMonitor.logRequest(false);
                 });
             } catch (syncErr) {
                 console.error('[FATAL MONITOR SYNC ERROR in setInterval try/catch]:', syncErr);
                  if (syncErr && syncErr.stack) console.error(syncErr.stack);
                 performanceMonitor.logRequest(false);
             }
             // console.log(`[${new Date().toISOString()}] ==== Monitor Interval Callback End ====`);
         }, monitorIntervalSeconds * 1000);
        console.log(`ℹ️ Adjusted monitor interval to ${monitorIntervalSeconds}s`);
    } else if (!monitorInterval) {
         // Initialize interval if it wasn't set (e.g., during startup)
         monitorInterval = setInterval(() => {
             // console.log(`[${new Date().toISOString()}] ==== Monitor Interval Fired (${monitorIntervalSeconds}s) ====`);
              try {
                  monitorPayments().catch(err => {
                      console.error('[FATAL MONITOR ERROR in setInterval catch]:', err);
                       if (err && err.stack) console.error(err.stack);
                      performanceMonitor.logRequest(false);
                  });
              } catch (syncErr) {
                  console.error('[FATAL MONITOR SYNC ERROR in setInterval try/catch]:', syncErr);
                   if (syncErr && syncErr.stack) console.error(syncErr.stack);
                  performanceMonitor.logRequest(false);
              }
              // console.log(`[${new Date().toISOString()}] ==== Monitor Interval Callback End ====`);
          }, monitorIntervalSeconds * 1000);
         console.log(`ℹ️ Initialized monitor interval to ${monitorIntervalSeconds}s`);
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
         if (!(recipientPubKey instanceof PublicKey)) {
             throw new Error('Invalid recipient public key type');
         }
    } catch (e) {
         console.error(`❌ Cannot send SOL: Invalid recipient address format "${recipientPublicKey}": ${e.message}`);
         return { success: false, error: `Invalid recipient address format` };
    }

    const amountSOL = Number(amountLamports) / LAMPORTS_PER_SOL;
    console.log(`💸 Attempting to send ${amountSOL.toFixed(6)} SOL to ${recipientPubKey.toBase58()} for ${gameType}`);

    // Calculate dynamic priority fee
    const priorityFeeMicroLamports = Math.min(
        1000000, // Max priority fee (0.001 SOL)
        Math.max(1000, Math.floor(Number(amountLamports) * PRIORITY_FEE_RATE)) // Min 1000 microLamports
    );
    const amountToSend = BigInt(amountLamports);

    if (amountToSend <= 0n) {
         console.error(`❌ Calculated payout amount ${amountLamports} is zero or negative. Cannot send.`);
         return { success: false, error: 'Calculated payout amount is zero or negative' };
    }

    for (let attempt = 1; attempt <= 3; attempt++) {
        try {
            const payerWallet = Keypair.fromSecretKey(bs58.decode(privateKey));
            const latestBlockhash = await solanaConnection.getLatestBlockhash({ commitment: 'confirmed' });

            if (!latestBlockhash || !latestBlockhash.blockhash) {
                throw new Error('Failed to get latest blockhash');
            }

            const transaction = new Transaction({
                recentBlockhash: latestBlockhash.blockhash,
                feePayer: payerWallet.publicKey
            });

            transaction.add(
                ComputeBudgetProgram.setComputeUnitPrice({ microLamports: priorityFeeMicroLamports })
            );
            transaction.add(
                ComputeBudgetProgram.setComputeUnitLimit({ units: 800 }) // Simple transfer CU limit
            );
            transaction.add(
                SystemProgram.transfer({
                    fromPubkey: payerWallet.publicKey,
                    toPubkey: recipientPubKey,
                    lamports: amountToSend
                })
            );

            console.log(`Sending TX (Attempt ${attempt})... Amount: ${amountToSend}, Priority Fee: ${priorityFeeMicroLamports} microLamports`);

            const signature = await Promise.race([
                sendAndConfirmTransaction(
                    solanaConnection,
                    transaction,
                    [payerWallet],
                    { commitment: 'confirmed', skipPreflight: false, maxRetries: 3 }
                ),
                new Promise((_, reject) => {
                    setTimeout(() => {
                        reject(new Error(`Transaction confirmation timeout after 45s (Attempt ${attempt})`));
                    }, 45000); // 45 second timeout
                })
            ]);

            console.log(`✅ Payout successful! Sent ${Number(amountToSend)/LAMPORTS_PER_SOL} SOL to ${recipientPubKey.toBase58()}. TX: ${signature}`);
            return { success: true, signature };

        } catch (error) {
            console.error(`❌ Payout TX failed (Attempt ${attempt}/3):`, error.message);
             if (!isRetryableError(error)) console.error(error.stack); // Log stack for non-retryable

            if (error.message.includes('Invalid param') ||
                error.message.includes('Insufficient funds') ||
                error.message.includes('blockhash not found') ||
                error.message.includes('custom program error')) {
                 console.error("❌ Non-retryable error encountered during send. Aborting payout.");
                 return { success: false, error: `Non-retryable send error: ${error.message}` };
            }

            if (attempt < 3) {
                await new Promise(resolve => setTimeout(resolve, 2000 * attempt));
            }
        }
    } // End retry loop

    console.error(`❌ Payout failed after 3 attempts for ${recipientPubKey.toBase58()}`);
    return { success: false, error: `Payout failed after 3 attempts` };
}


// --- Game Processing Logic ---

async function processPaidBet(bet) {
    console.log(`⚙️ Processing game for bet ${bet.id} (${bet.game_type})`);
    let client;
    try {
         client = await pool.connect();
         await client.query('BEGIN');
         const statusCheck = await client.query('SELECT status FROM bets WHERE id = $1 FOR UPDATE', [bet.id]);

         if (statusCheck.rows[0]?.status !== 'payment_verified') {
             console.warn(`Bet ${bet.id} status is ${statusCheck.rows[0]?.status}, not 'payment_verified'. Aborting game processing.`);
             await client.query('ROLLBACK');
             return;
         }

         await client.query('UPDATE bets SET status = $1 WHERE id = $2', ['processing_game', bet.id]);
         await client.query('COMMIT');

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
         console.error(error.stack);
         if (client) {
             try { await client.query('ROLLBACK'); } catch (rbErr) { console.error('Rollback failed:', rbErr); }
         }
         await updateBetStatus(bet.id, 'error_processing_setup');
    } finally {
         if (client) client.release();
    }
}

async function handleCoinflipGame(bet) {
    const { id: betId, user_id, chat_id, bet_details, expected_lamports } = bet;
    const choice = bet_details.choice;
    const config = GAME_CONFIG.coinflip;

    const playerWinProbability = 0.5 - (config.houseEdge / 2.0);
    const resultRandom = Math.random();
    let result = (choice === 'heads')
        ? (resultRandom < playerWinProbability ? 'heads' : 'tails')
        : (resultRandom < playerWinProbability ? 'tails' : 'heads');
    const win = (result === choice);

    const payoutLamports = win ? calculatePayout(BigInt(expected_lamports), 'coinflip') : 0n;
    let displayName = await getUserDisplayName(chat_id, user_id);

    if (win) {
        const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
        console.log(`🪙 Coinflip Bet ${betId}: ${displayName} WON ${payoutSOL.toFixed(6)} SOL! Choice: ${choice}, Result: ${result}`);
        const winnerAddress = await getLinkedWallet(user_id);

        if (!winnerAddress) {
            console.warn(`Coinflip Bet ${betId}: Winner ${displayName} has no linked wallet.`);
            await bot.sendMessage(chat_id,
                `🎉 ${displayName}, you won the coinflip (Result: *${result}*) but have no wallet linked!\n` +
                `Your payout of ${payoutSOL.toFixed(6)} SOL is waiting. Place another bet (any amount) to link your wallet and receive pending payouts.`,
                { parse_mode: 'Markdown' }
            ).catch(e => console.error("TG Send Error:", e.message));
            await updateBetStatus(betId, 'completed_win_no_wallet');
            return;
        }

        try {
            await bot.sendMessage(chat_id,
                `🎉 ${displayName}, you won ${payoutSOL.toFixed(6)} SOL!\n` +
                `Result: *${result}*\n\n` +
                `💸 Processing payout to your linked wallet...`,
                { parse_mode: 'Markdown' }
            ).catch(e => console.error("TG Send Error:", e.message));

             await updateBetStatus(betId, 'processing_payout');

            // Use addPaymentJob - doesn't await
            paymentProcessor.addPaymentJob({
                type: 'payout',
                betId,
                recipient: winnerAddress,
                amount: payoutLamports,
                gameType: 'coinflip',
                priority: 2,
                chatId: chat_id,
                displayName,
                result
            });
        } catch (e) {
            console.error(`❌ Error queuing payout for coinflip bet ${betId}:`, e);
            console.error(e.stack);
            await bot.sendMessage(chat_id,
                `⚠️ Error occurred while processing your coinflip win for bet ID ${betId}.\nPlease contact support.`,
                { parse_mode: 'Markdown' }
            ).catch(e => console.error("TG Send Error:", e.message));
            await updateBetStatus(betId, 'error_payout_queueing');
        }
    } else { // Loss
        console.log(`🪙 Coinflip Bet ${betId}: ${displayName} LOST. Choice: ${choice}, Result: ${result}`);
        await bot.sendMessage(chat_id,
            `❌ ${displayName}, you lost the coinflip!\nYou guessed *${choice}* but the result was *${result}*. Better luck next time!`,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
        await updateBetStatus(betId, 'completed_loss');
    }
}

async function handleRaceGame(bet) {
    const { id: betId, user_id, chat_id, bet_details, expected_lamports } = bet;
    const chosenHorseName = bet_details.horse;
    const config = GAME_CONFIG.race;

    const horses = [
         { name: 'Yellow', emoji: '🟡', odds: 1.1, winProbability: 0.25 }, { name: 'Orange', emoji: '🟠', odds: 2.0, winProbability: 0.20 },
         { name: 'Blue',   emoji: '🔵', odds: 3.0, winProbability: 0.15 }, { name: 'Cyan',   emoji: '💧', odds: 4.0, winProbability: 0.12 },
         { name: 'White',  emoji: '⚪️', odds: 5.0, winProbability: 0.09 }, { name: 'Red',    emoji: '🔴', odds: 6.0, winProbability: 0.07 },
         { name: 'Black',  emoji: '⚫️', odds: 7.0, winProbability: 0.05 }, { name: 'Pink',   emoji: '🌸', odds: 8.0, winProbability: 0.03 },
         { name: 'Purple', emoji: '🟣', odds: 9.0, winProbability: 0.02 }, { name: 'Green',  emoji: '🟢', odds: 10.0, winProbability: 0.01 },
         { name: 'Silver', emoji: '💎', odds: 15.0, winProbability: 0.01 } // Total: ~0.99
    ];
    // Normalize probabilities (optional but good practice)
    const totalProb = horses.reduce((sum, h) => sum + h.winProbability, 0);
    if (totalProb > 0 && Math.abs(totalProb - 1.0) > 0.001) { // Check if normalization needed
        const adjustmentFactor = 1.0 / totalProb;
        horses.forEach(h => h.winProbability *= adjustmentFactor);
        // console.log(`[Race:${betId}] Adjusted horse probabilities to sum to 1.`);
    }

    // Determine winning horse
    let winningHorse = null;
    const randomNumber = Math.random();
    let cumulativeProbability = 0;
    for (const horse of horses) {
        cumulativeProbability += horse.winProbability;
        if (randomNumber <= cumulativeProbability) {
            winningHorse = horse;
            break;
        }
    }
    winningHorse = winningHorse || horses[horses.length - 1]; // Fallback

    let displayName = await getUserDisplayName(chat_id, user_id);
    try {
        await bot.sendMessage(chat_id, `🐎 Race ${betId} starting! ${displayName} bet on ${chosenHorseName}!`).catch(e => console.error("TG Send Error:", e.message));
        await new Promise(resolve => setTimeout(resolve, 2000));
        await bot.sendMessage(chat_id, "🚦 And they're off!").catch(e => console.error("TG Send Error:", e.message));
        await new Promise(resolve => setTimeout(resolve, 3000));
        await bot.sendMessage(chat_id,
            `🏆 The winner is... ${winningHorse.emoji} *${winningHorse.name}*! 🏆`,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
    } catch (e) {
        console.error(`Error sending race commentary for bet ${betId}:`, e);
    }

    const win = (chosenHorseName.toLowerCase() === winningHorse.name.toLowerCase());
    const payoutLamports = win ? calculatePayout(BigInt(expected_lamports), 'race', winningHorse) : 0n;

    if (win) {
        const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
        console.log(`🐎 Race Bet ${betId}: ${displayName} WON ${payoutSOL.toFixed(6)} SOL! Horse: ${chosenHorseName}, Winner: ${winningHorse.name}`);
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

        try {
            await bot.sendMessage(chat_id,
                `🎉 ${displayName}, your horse *${chosenHorseName}* won!\n` +
                `Payout: ${payoutSOL.toFixed(6)} SOL\n\n` +
                `💸 Processing payout to your linked wallet...`,
                { parse_mode: 'Markdown' }
            ).catch(e => console.error("TG Send Error:", e.message));

             await updateBetStatus(betId, 'processing_payout');

            // Use addPaymentJob - doesn't await
            paymentProcessor.addPaymentJob({
                type: 'payout',
                betId,
                recipient: winnerAddress,
                amount: payoutLamports,
                gameType: 'race',
                priority: 2,
                chatId: chat_id,
                displayName,
                horseName: chosenHorseName,
                winningHorse
            });
        } catch (e) {
            console.error(`❌ Error queuing payout for race bet ${betId}:`, e);
             console.error(e.stack);
             await bot.sendMessage(chat_id,
                 `⚠️ Error occurred while processing your race win for bet ID ${betId}.\nPlease contact support.`,
                 { parse_mode: 'Markdown' }
             ).catch(e => console.error("TG Send Error:", e.message));
            await updateBetStatus(betId, 'error_payout_queueing');
        }
    } else { // Loss
        console.log(`🐎 Race Bet ${betId}: ${displayName} LOST. Horse: ${chosenHorseName}, Winner: ${winningHorse.name}`);
        await bot.sendMessage(chat_id,
            `❌ ${displayName}, your horse *${chosenHorseName}* lost the race!\nWinner: ${winningHorse.emoji} *${winningHorse.name}*. Better luck next time!`,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
        await updateBetStatus(betId, 'completed_loss');
    }
}

async function handlePayoutJob(job) {
    const { betId, recipient, amount, gameType, chatId, displayName, result, horseName, winningHorse } = job;
    const payoutAmountLamports = BigInt(amount);

    if (payoutAmountLamports <= 0n) {
        console.error(`❌ Payout job for bet ${betId} has zero or negative amount (${payoutAmountLamports}). Skipping send.`);
        await updateBetStatus(betId, 'error_payout_zero_amount');
        await bot.sendMessage(chatId,
            `⚠️ There was an issue calculating the payout for bet ID ${betId} (amount was zero). Please contact support.`,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
        return;
    }

    console.log(`💸 Processing payout job for Bet ID: ${betId}, Amount: ${payoutAmountLamports} lamports`);

    try {
        const sendResult = await sendSol(recipient, payoutAmountLamports, gameType);

        if (sendResult.success) {
            console.log(`💸 Payout successful for bet ${betId}, TX: ${sendResult.signature}`);
            await bot.sendMessage(chatId,
                `✅ Payout successful for bet ${betId}!\n` +
                `${(Number(payoutAmountLamports)/LAMPORTS_PER_SOL).toFixed(6)} SOL sent.\n` +
                `TX: \`https://solscan.io/tx/${sendResult.signature}\``,
                { parse_mode: 'Markdown', disable_web_page_preview: true }
            ).catch(e => console.error("TG Send Error:", e.message));
            await recordPayout(betId, 'completed_win_paid', sendResult.signature);
        } else {
            console.error(`❌ Payout failed permanently for bet ${betId}: ${sendResult.error}`);
            await bot.sendMessage(chatId,
                `⚠️ Payout failed for bet ID ${betId}: ${sendResult.error}\nThe team has been notified. Please contact support if needed.`,
                { parse_mode: 'Markdown' }
            ).catch(e => console.error("TG Send Error:", e.message));
            await updateBetStatus(betId, 'error_payout_failed');
            // TODO: Implement admin notification
            // notifyAdmin(`Payout failed for Bet ${betId}: ${sendResult.error}`);
        }
    } catch (error) {
        console.error(`❌ Unexpected error processing payout job for bet ${betId}:`, error);
        console.error(error.stack);
        await updateBetStatus(betId, 'error_payout_exception');
         await bot.sendMessage(chatId,
             `⚠️ A technical error occurred during payout for bet ID ${betId}.\nPlease contact support.`,
             { parse_mode: 'Markdown' }
         ).catch(e => console.error("TG Send Error:", e.message));
         // notifyAdmin(`Unexpected error during payout for Bet ${betId}: ${error.message}`);
    }
}

// --- Utility Functions ---

function calculatePayout(betLamports, gameType, gameDetails = {}) {
    betLamports = BigInt(betLamports);
    let payoutLamports = 0n;

    if (gameType === 'coinflip') {
        const multiplier = 2.0 - GAME_CONFIG.coinflip.houseEdge;
        payoutLamports = BigInt(Math.floor(Number(betLamports) * multiplier));
    } else if (gameType === 'race' && gameDetails.odds) {
        const multiplier = gameDetails.odds * (1.0 - GAME_CONFIG.race.houseEdge);
        payoutLamports = BigInt(Math.floor(Number(betLamports) * multiplier));
    } else {
         console.error(`Cannot calculate payout: Unknown game type ${gameType} or missing race odds.`);
         return 0n;
    }
    // console.log(`Payout Calc: Bet ${betLamports}, Payout ${payoutLamports}`);
    return payoutLamports > 0n ? payoutLamports : 0n;
}

async function getUserDisplayName(chat_id, user_id) {
     try {
         const chatMember = await bot.getChatMember(chat_id, user_id);
         return chatMember.user.username ? `@${chatMember.user.username}` : chatMember.user.first_name;
     } catch (e) {
         if (e.message?.includes('user not found') || e.message?.includes('chat not found') || e.message?.includes('member list is inaccessible')) {
             // Expected errors in some cases
         } else {
             console.warn(`Couldn't get username for user ${user_id} in chat ${chat_id}:`, e.message);
         }
         return `User ${String(user_id).substring(0, 6)}...`;
     }
}


// --- Telegram Bot Command Handlers ---

bot.on('polling_error', (error) => {
    console.error(`❌ Polling error: ${error.code} - ${error.message}`);
    console.error(error.stack);
    if (error.code === 'ETELEGRAM' && error.message.includes('409 Conflict')) {
        console.error("❌❌❌ FATAL: Conflict detected! Another bot instance might be running with the same token. Exiting.");
        process.exit(1);
    }
});

bot.on('error', (error) => {
    console.error('❌ General Bot Error:', error);
    console.error(error.stack);
    performanceMonitor.logRequest(false);
});

async function handleMessage(msg) {
    if (!msg.text || msg.from.is_bot) return;

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const messageText = msg.text || '';

    try {
        const now = Date.now();
        if (confirmCooldown.has(userId)) {
            const lastTime = confirmCooldown.get(userId);
            if (now - lastTime < cooldownInterval) {
                return; // Ignore cooldown
            }
        }
        if (messageText.startsWith('/')) {
             confirmCooldown.set(userId, now);
        }

        const text = messageText.trim();
        let handled = false; // Flag to check if any command matched

        if (text.match(/^\/start$/i)) {
            await handleStartCommand(msg); handled = true;
        }
        else if (text.match(/^\/coinflip$/i)) {
            await handleCoinflipCommand(msg); handled = true;
        }
        else if (text.match(/^\/wallet$/i)) {
            await handleWalletCommand(msg); handled = true;
        }
        else if (text.match(/^\/bet (\d+\.?\d*)\s+(heads|tails)/i)) {
            await handleBetCommand(msg); handled = true;
        }
        else if (text.match(/^\/race$/i)) {
            await handleRaceCommand(msg); handled = true;
        }
        else if (text.match(/^\/betrace (\d+\.?\d*)\s+(\w+)/i)) {
            await handleBetRaceCommand(msg); handled = true;
        }
        else if (text.match(/^\/help$/i)) {
             await handleHelpCommand(msg); handled = true;
        }

        if (handled) {
            performanceMonitor.logRequest(true);
        }
        // Else: ignore non-command messages silently

    } catch (error) {
        console.error(`❌ Error processing message from user ${userId} in chat ${chatId}: "${messageText}"`, error);
        console.error(error.stack);
        performanceMonitor.logRequest(false);
        try {
             await bot.sendMessage(chatId, "⚠️ An unexpected error occurred while processing your request. Please try again later or contact support if the issue persists.");
        } catch (tgError) {
             console.error("❌ Failed to send error message to chat:", tgError.message);
        }
    } finally {
         // Clean up cooldown map periodically
         const cutoff = Date.now() - 60000;
         for (const [key, timestamp] of confirmCooldown.entries()) {
             if (timestamp < cutoff) {
                 confirmCooldown.delete(key);
             }
         }
    }
}

async function handleStartCommand(msg) {
    const firstName = msg.from.first_name || 'there';
    const caption = `👋 Welcome, ${firstName}!\n\n` +
                    `🎰 *Solana Gambles Bot*\n\n` +
                    `Use /coinflip or /race to see game options.\n` +
                    `Use /wallet to view your linked Solana wallet.\n` +
                    `Use /help to see all commands.`;
    const animationUrl = 'https://i.ibb.co/9vDo58q/banner.gif'; // Consider making this an env var

    try {
        await bot.sendAnimation(msg.chat.id, animationUrl, { caption, parse_mode: 'Markdown' });
    } catch (err) {
         console.warn("⚠️ Failed to send start animation, sending text fallback:", err.message);
         try {
              await bot.sendMessage(msg.chat.id, caption, { parse_mode: 'Markdown' });
         } catch (fallbackErr) {
              console.error("Error sending start command text fallback:", fallbackErr);
         }
    }
}

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
        `- Payout: ~${(2.0 - config.houseEdge).toFixed(2)}x (after house edge)\n\n` +
        `You will be given a wallet address and a *unique Memo ID*. Send the *exact* SOL amount with the memo to place your bet.`,
        { parse_mode: 'Markdown' }
    ).catch(e => console.error("TG Send Error:", e.message));
}

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

async function handleWalletCommand(msg) {
    const userId = String(msg.from.id);
    const walletAddress = await getLinkedWallet(userId);

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

async function handleBetCommand(msg) {
    const match = msg.text.match(/^\/bet (\d+\.?\d*)\s+(heads|tails)/i);
    if (!match) return;

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const config = GAME_CONFIG.coinflip;

    const betAmount = parseFloat(match[1]);
    if (isNaN(betAmount) || betAmount < config.minBet || betAmount > config.maxBet) {
        await bot.sendMessage(chatId,
            `⚠️ Invalid bet amount. Please bet between ${config.minBet} and ${config.maxBet} SOL.\n` +
            `Example: \`/bet 0.1 heads\``,
             { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
        return;
    }

    if (!process.env.MAIN_WALLET_ADDRESS) {
        console.error("❌ Coinflip bet failed: MAIN_WALLET_ADDRESS is not configured.");
        await bot.sendMessage(chatId, `⚠️ Betting is temporarily unavailable. Please try again later. (Error: CFG_MW_MISSING)`).catch(e => console.error("TG Send Error:", e.message));
        return;
    }

    const userChoice = match[2].toLowerCase();
    const memoId = generateMemoId('CF');
    const expectedLamports = BigInt(Math.round(betAmount * LAMPORTS_PER_SOL));
    const expiresAt = new Date(Date.now() + config.expiryMinutes * 60 * 1000);

    const saveResult = await savePendingBet(
        userId, chatId, 'coinflip',
        { choice: userChoice },
        expectedLamports, memoId, expiresAt
    );

    if (!saveResult.success) {
         await bot.sendMessage(chatId, `⚠️ Error registering bet: ${saveResult.error}. Please try the command again.`).catch(e => console.error("TG Send Error:", e.message));
         return;
    }

    await bot.sendMessage(chatId,
        `✅ Coinflip bet registered! (ID: ${memoId})\n\n` +
        `You chose: *${userChoice}*\n` +
        `Amount: *${betAmount.toFixed(6)} SOL*\n\n` +
        `➡️ Send *exactly ${betAmount.toFixed(6)} SOL* to:\n` +
        `\`${process.env.MAIN_WALLET_ADDRESS}\`\n\n` +
        `📎 *Include MEMO:* \`${memoId}\`\n\n` +
        `⏱️ This request expires in ${config.expiryMinutes} minutes.\n\n` +
        `*IMPORTANT:* Send from your own wallet. Do not send from an exchange. Ensure you include the memo correctly.`,
        { parse_mode: 'Markdown', disable_web_page_preview: true }
    ).catch(e => console.error("TG Send Error:", e.message));
}

async function handleBetRaceCommand(msg) {
    const match = msg.text.match(/^\/betrace (\d+\.?\d*)\s+(\w+)/i);
     if (!match) return;

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const config = GAME_CONFIG.race;

    const betAmount = parseFloat(match[1]);
    if (isNaN(betAmount) || betAmount < config.minBet || betAmount > config.maxBet) {
        await bot.sendMessage(chatId,
            `⚠️ Invalid bet amount. Please bet between ${config.minBet} and ${config.maxBet} SOL.\n`+
            `Example: \`/betrace 0.1 Yellow\``,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
        return;
    }

    if (!process.env.RACE_WALLET_ADDRESS) {
        console.error("❌ Race bet failed: RACE_WALLET_ADDRESS is not configured.");
        await bot.sendMessage(chatId, `⚠️ Betting is temporarily unavailable. Please try again later. (Error: CFG_RW_MISSING)`).catch(e => console.error("TG Send Error:", e.message));
        return;
    }

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

    const saveResult = await savePendingBet(
        userId, chatId, 'race',
        { horse: chosenHorse.name, odds: chosenHorse.odds },
        expectedLamports, memoId, expiresAt
    );

      if (!saveResult.success) {
         await bot.sendMessage(chatId, `⚠️ Error registering bet: ${saveResult.error}. Please try the command again.`).catch(e => console.error("TG Send Error:", e.message));
         return;
      }

      const potentialPayoutLamports = calculatePayout(expectedLamports, 'race', chosenHorse);
      const potentialPayoutSOL = (Number(potentialPayoutLamports) / LAMPORTS_PER_SOL).toFixed(6);

    await bot.sendMessage(chatId,
        `✅ Race bet registered! (ID: ${memoId})\n\n` +
        `You chose: ${chosenHorse.emoji} *${chosenHorse.name}*\n` +
        `Amount: *${betAmount.toFixed(6)} SOL*\n` +
        `Potential Payout: ~${potentialPayoutSOL} SOL\n\n`+
        `➡️ Send *exactly ${betAmount.toFixed(6)} SOL* to:\n` +
        `\`${process.env.RACE_WALLET_ADDRESS}\`\n\n` +
        `📎 *Include MEMO:* \`${memoId}\`\n\n` +
        `⏱️ This request expires in ${config.expiryMinutes} minutes.\n\n` +
        `*IMPORTANT:* Send from your own wallet. Do not send from an exchange. Ensure you include the memo correctly.`,
        { parse_mode: 'Markdown', disable_web_page_preview: true }
    ).catch(e => console.error("TG Send Error:", e.message));
}

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
                       `*Support:* If you encounter issues, please contact support.`; // TODO: Add actual support link/contact

     await bot.sendMessage(msg.chat.id, helpText, { parse_mode: 'Markdown' }).catch(e => console.error("TG Send Error:", e.message));
}


// --- Server Startup & Shutdown Logic ---

async function startServer() {
    try {
        await initializeDatabase();
        const PORT = process.env.PORT || 3000;

        // Set up Telegram Webhook if in production
        if (process.env.RAILWAY_ENVIRONMENT && process.env.RAILWAY_PUBLIC_DOMAIN) {
            const webhookUrl = `https://${process.env.RAILWAY_PUBLIC_DOMAIN}${webhookPath}`;
            console.log(`Attempting to set webhook to: ${webhookUrl}`);
            let attempts = 0;
            while (attempts < 3) {
                try {
                    await bot.setWebHook(webhookUrl);
                    console.log(`✅ Webhook successfully set to: ${webhookUrl}`);
                    break;
                } catch (webhookError) {
                    attempts++;
                    console.error(`❌ Webhook setup attempt ${attempts} failed:`, webhookError.message);
                    if (attempts >= 3) throw webhookError;
                    await new Promise(resolve => setTimeout(resolve, 3000 * attempts));
                }
            }
        } else {
            console.log("ℹ️ Not in Railway environment or domain not set, webhook not configured.");
        }

        // Start the Express server
        const server = app.listen(PORT, "0.0.0.0", () => {
            console.log(`🚀 Server running on port ${PORT}`);

            // Start the payment monitor loop
            console.log(`⚙️ Starting payment monitor (Initial Interval: ${monitorIntervalSeconds}s)`);
             adjustMonitorInterval(0); // Initialize the interval timer

            // Run monitor once shortly after startup
            setTimeout(() => {
                 console.log("⚙️ Performing initial payment monitor run...");
                 try {
                      monitorPayments().catch(err => {
                          console.error('[FATAL MONITOR ERROR in initial run catch]:', err);
                           if (err && err.stack) console.error(err.stack);
                          performanceMonitor.logRequest(false);
                      });
                 } catch (syncErr) {
                      console.error('[FATAL MONITOR SYNC ERROR in initial run try/catch]:', syncErr);
                       if (syncErr && syncErr.stack) console.error(syncErr.stack);
                      performanceMonitor.logRequest(false);
                 }
             }, 5000); // Delay initial run

            // Start polling if webhook is NOT used
            if (!process.env.RAILWAY_ENVIRONMENT || !process.env.RAILWAY_PUBLIC_DOMAIN) { // Simplified check
                 console.log("ℹ️ Starting bot polling for local development...");
                 bot.deleteWebHook({ drop_pending_updates: true })
                     .then(() => bot.startPolling({ interval: 300 }))
                     .then(() => console.log("✅ Bot polling started successfully"))
                     .catch(err => {
                         console.error("❌ Error starting polling:", err.message);
                          if (err && err.stack) console.error(err.stack);
                          if (err.message?.includes('409 Conflict')) {
                              console.error("❌❌❌ Conflict detected! Another instance might be polling.");
                              process.exit(1);
                          }
                     });
            }
        });

        server.on('error', (err) => {
             console.error('❌ Server error:', err);
             console.error(err.stack);
             if (err.code === 'EADDRINUSE') {
                 console.error(`❌❌❌ Port ${PORT} is already in use. Exiting.`);
                 process.exit(1);
             }
        });

    } catch (error) {
        console.error("🔥🔥🔥 Failed to start application:", error);
        console.error(error.stack);
        process.exit(1);
    }
}

// Graceful shutdown handler
const shutdown = async (signal) => {
    console.log(`\n🛑 ${signal} received, shutting down gracefully...`);
    isMonitorRunning = true; // Prevent new monitor cycle

    console.log("Stopping incoming connections...");
    if (monitorInterval) clearInterval(monitorInterval);

    try {
        let listenersStopped = false;
        if (process.env.RAILWAY_ENVIRONMENT && process.env.RAILWAY_PUBLIC_DOMAIN) {
             try {
                 await bot.deleteWebHook({ drop_pending_updates: true });
                 console.log("- Removed Telegram webhook.");
                 listenersStopped = true;
             } catch (whErr) { console.error("⚠️ Error removing webhook:", whErr.message); }
        }
        if (!listenersStopped && bot.isPolling()) {
            try {
                await bot.stopPolling({ cancel: true });
                console.log("- Stopped Telegram polling.");
            } catch (pollErr) { console.error("⚠️ Error stopping polling:", pollErr.message); }
        }
    } catch (e) { console.error("⚠️ Error stopping bot listeners:", e.message); }

    console.log("Waiting for active jobs to finish...");
    try {
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
         messageQueue.clear();
         paymentProcessor.highPriorityQueue.clear();
         paymentProcessor.normalQueue.clear();
    }

    console.log("Closing database pool...");
    try {
        await pool.end();
        console.log("✅ Database pool closed.");
    } catch (dbErr) {
        console.error("❌ Error closing database pool:", dbErr);
    } finally {
        console.log("🛑 Shutdown complete.");
        process.exit(0);
    }
};

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

process.on('uncaughtException', (err, origin) => {
    console.error(`🔥🔥🔥 Uncaught Exception at: ${origin}`, err);
    if (err && err.stack) console.error(err.stack);
    shutdown('UNCAUGHT_EXCEPTION').catch(() => process.exit(1));
    setTimeout(() => process.exit(1), 10000).unref();
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('🔥🔥🔥 Unhandled Rejection at:', promise, 'reason:', reason);
     if (reason instanceof Error && reason.stack) {
         console.error(reason.stack);
     }
    // Optional: shutdown on unhandled rejection
    // shutdown('UNHANDLED_REJECTION').catch(() => process.exit(1));
    // setTimeout(() => process.exit(1), 10000).unref();
});

// --- Start the Application ---
startServer().then(() => {
    console.log("🚀🚀🚀 Solana Gambles Bot is up and running! 🚀🚀🚀");
}).catch(err => {
    console.error("🔥🔥🔥 Application failed to initialize:", err);
     if (err && err.stack) console.error(err.stack);
    process.exit(1);
});
