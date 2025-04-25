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
    maxConcurrent: 3,         // Max parallel requests
    retryBaseDelay: 600,      // Initial delay for retries (ms)
    commitment: 'confirmed',  // Default commitment level
    httpHeaders: {
        'Content-Type': 'application/json',
        'solana-client': `SolanaGamblesBot/2.0 (${process.env.RAILWAY_ENVIRONMENT ? 'railway' : 'local'})` // Client info
    },
    rateLimitCooloff: 10000,  // Pause duration after hitting rate limits (ms)
    disableRetryOnRateLimit: false // Enable automatic retry on rate limit errors
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
    max: 15,                   // Max connections in pool
    min: 5,                    // Min connections maintained
    idleTimeoutMillis: 30000,  // Close idle connections after 30s
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

        // Add priority column if it doesn't exist (for backward compatibility)
        await client.query(`
            ALTER TABLE bets
            ADD COLUMN IF NOT EXISTS priority INT DEFAULT 0;
        `);

        // Add indexes for performance on frequently queried columns
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_status ON bets(status);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_user_id ON bets(user_id);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_expires_at ON bets(expires_at);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_memo_id ON bets(memo_id);`); // Added index on memo_id
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
        version: '2.0.0', // Bot version
        queueStats: { // Report queue status
            pending: messageQueue.size,
            active: messageQueue.pending
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

// Validates the format of a potential memo ID found in a transaction
function validateMemoFormat(memo) {
    if (!memo || typeof memo !== 'string') return false;
    const parts = memo.split('-');
    return parts.length === 2 &&
           ['BET', 'CF', 'RA'].includes(parts[0]) && // Check prefix
           /^[A-F0-9]{12}$/.test(parts[1]);          // Check hex part length/chars
}

// --- Database Operations ---

// Saves a new bet intention to the database
async function savePendingBet(userId, chatId, gameType, details, lamports, memoId, expiresAt, priority = 0) {
    if (!validateMemoFormat(memoId)) {
        console.error(`DB: Attempted to save bet with invalid memo format: ${memoId}`);
        return { success: false, error: 'Invalid memo ID format provided' };
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
    if (!validateMemoFormat(memoId)) return undefined;

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
        DO UPDATE SET         -- Update their wallet address and last used time
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

// Finds the SPL Memo instruction data in a transaction
function findMemoInTx(tx) {
    // Check if transaction and instructions exist
    if (!tx?.transaction?.message?.instructions) {
        // console.warn("Memo search: Transaction or instructions missing.");
        return null;
    }

    try {
        const MEMO_PROGRAM_ID = 'Memo1UhkJRfHyvLMcVuc6beZNRYqUP2VZwW'; // v1 Memo Program ID
        const MEMO_PROGRAM_ID_V2 = 'MemoSq4gqABAXKb96qnH8TysNcVtrp5GktfD'; // v2 Memo Program ID (less common)

        for (const instruction of tx.transaction.message.instructions) {
            let programId = '';

            // Resolve program ID from account keys using index
            if (instruction.programIdIndex !== undefined && tx.transaction.message.accountKeys) {
                const keyInfo = tx.transaction.message.accountKeys[instruction.programIdIndex];
                programId = keyInfo?.pubkey ? keyInfo.pubkey.toBase58() :
                            (typeof keyInfo === 'string' ? new PublicKey(keyInfo).toBase58() : '');
            } else if (instruction.programId) { // Fallback if programId is directly present
                programId = instruction.programId.toBase58 ?
                            instruction.programId.toBase58() :
                            instruction.programId.toString();
            }

            // Check if it's a memo instruction and has data
            if ((programId === MEMO_PROGRAM_ID || programId === MEMO_PROGRAM_ID_V2) && instruction.data) {
                // Decode base58 data to UTF-8 string
                const memo = bs58.decode(instruction.data).toString('utf-8');
                // Validate format before returning
                return validateMemoFormat(memo) ? memo : null;
            }
        }
    } catch (e) {
        console.error("Error parsing memo instruction:", e.message);
    }
    // console.warn("Memo search: No valid memo instruction found.");
    return null; // No valid memo found
}

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
    const msg = error.message.toLowerCase();
    return msg.includes('429') || // HTTP 429 Too Many Requests
           msg.includes('timeout') ||
           msg.includes('rate limit') ||
           msg.includes('econnreset') || // Connection reset
           msg.includes('esockettimedout');
}


class PaymentProcessor {
    constructor() {
        // Queue for high-priority jobs (e.g., game processing, payouts)
        this.highPriorityQueue = new PQueue({
            concurrency: 3,
            // priority: (job) => job.priority // p-queue doesn't support priority directly, use separate queues
        });
        // Queue for normal priority jobs (e.g., initial payment monitoring checks)
        this.normalQueue = new PQueue({
            concurrency: 2,
            // autoStart: false // Start immediately
        });
        this.activeProcesses = new Set(); // Track signatures currently being processed to prevent duplicates
    }

    // Adds a job to the appropriate queue based on priority
    async addPaymentJob(job) {
        // Simulate priority by choosing the queue
        if (job.priority > 0) {
             // Add higher priority tasks (like processing/payouts) to the highPriorityQueue
            await this.highPriorityQueue.add(() => this.processJob(job));
        } else {
            // Add lower priority tasks (like initial signature checks) to the normalQueue
            await this.normalQueue.add(() => this.processJob(job));
        }
        // this.balanceQueues(); // Balancing might be complex, keep simple for now
    }

    /* Dynamic balancing logic - potentially complex, keeping simpler approach first
    balanceQueues() {
        // Example: Adjust normal queue concurrency based on high priority load
        const highPriorityLoad = this.highPriorityQueue.size + this.highPriorityQueue.pending;
        this.normalQueue.concurrency = Math.max(1, 5 - Math.floor(highPriorityLoad / 2));
    }
    */

    // Wrapper to handle job execution, retries, and error logging
    async processJob(job) {
        // Prevent processing the same signature concurrently if added multiple times quickly
        if (this.activeProcesses.has(job.signature)) {
             console.warn(`Job for signature ${job.signature} already active, skipping duplicate.`);
             return;
        }
        this.activeProcesses.add(job.signature); // Mark as active

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
                await this.addPaymentJob(job); // Re-add the job for retry
            } else {
                // Log final failure or non-retryable error
                 console.error(`Job failed permanently or exceeded retries: ${job.signature || job.betId}`, error);
                 // Potentially update bet status to an error state if applicable
                 if(job.betId && job.type !== 'monitor_payment') {
                     await updateBetStatus(job.betId, `error_${job.type}_failed`);
                 }
            }
        } finally {
             // Always remove signature from active set when processing finishes (success or fail)
            if (job.signature) {
                this.activeProcesses.delete(job.signature);
            }
        }
    }

    // Core logic to process an incoming payment transaction signature
    async _processIncomingPayment(signature, walletType, attempt = 0) {
        // 1. Check session cache first (quickest check)
        if (processedSignaturesThisSession.has(signature)) {
            // console.log(`Signature ${signature} already processed this session.`);
            return { processed: false, reason: 'already_processed_session' };
        }

        // 2. Check database if already recorded as paid (prevent double processing across restarts)
        const checkQuery = `SELECT id FROM bets WHERE paid_tx_signature = $1 LIMIT 1;`;
        try {
            const processed = await pool.query(checkQuery, [signature]);
            if (processed.rowCount > 0) {
                // console.log(`Signature ${signature} already recorded in DB for bet ${processed.rows[0].id}.`);
                processedSignaturesThisSession.add(signature); // Add to session cache too
                return { processed: false, reason: 'exists_in_db' };
            }
        } catch (dbError) {
             console.error(`DB Error checking signature ${signature}:`, dbError.message);
             // Decide if this error is retryable or should fail
             if (isRetryableError(dbError)) throw dbError; // Re-throw to trigger retry logic
             return { processed: false, reason: 'db_check_error' };
        }


        // 3. Fetch the transaction details from Solana
        console.log(`Workspaceing transaction details for signature: ${signature} (Attempt ${attempt + 1})`);
        const tx = await solanaConnection.executeWithRetry(
            'getParsedTransaction',
            [signature, { maxSupportedTransactionVersion: 0 }], // Request parsed format
            attempt // Pass attempt number for retry logic in connection class
        );

        // 4. Validate transaction
        if (!tx) {
             console.warn(`Transaction ${signature} not found or failed to fetch after retries.`);
             // Don't add to processed set yet, might appear later
             return { processed: false, reason: 'tx_fetch_failed' };
        }
         if (tx.meta?.err) {
             console.log(`Transaction ${signature} failed on-chain: ${JSON.stringify(tx.meta.err)}`);
             processedSignaturesThisSession.add(signature); // Add failed TXs to prevent re-checking
             return { processed: false, reason: 'tx_onchain_error' };
         }

        // 5. Find the memo
        const memo = findMemoInTx(tx);
        if (!memo) {
            // console.log(`Transaction ${signature} does not contain a valid game memo.`);
            processedSignaturesThisSession.add(signature); // Add TXs without memos
            return { processed: false, reason: 'no_valid_memo' };
        }
        console.log(`Processing payment TX ${signature} with memo: ${memo}`);

        // 6. Find the corresponding pending bet in DB using memo
        const bet = await findBetByMemo(memo); // Uses FOR UPDATE SKIP LOCKED
        if (!bet) {
             console.warn(`No matching pending bet found for memo ${memo} from TX ${signature}.`);
             processedSignaturesThisSession.add(signature); // Add processed TXs even if no bet found
             // Could potentially refund here if needed, but simpler to ignore for now
             return { processed: false, reason: 'no_matching_bet' };
        }
         if (bet.status !== 'awaiting_payment') {
             console.warn(`Bet ${bet.id} found for memo ${memo} but status is ${bet.status}, not 'awaiting_payment'.`);
             processedSignaturesThisSession.add(signature);
             return { processed: false, reason: 'bet_already_processed' };
         }


        // 7. Analyze transaction amounts
        const { transferAmount, payerAddress } = analyzeTransactionAmounts(tx, walletType);
        if (transferAmount <= 0n) {
            console.warn(`No SOL transfer to target wallet found in TX ${signature} for memo ${memo}.`);
            processedSignaturesThisSession.add(signature);
            // Don't update bet status here, might be unrelated TX with same memo
            return { processed: false, reason: 'no_transfer_found' };
        }

        // 8. Validate amount sent vs expected (with tolerance)
        const expectedAmount = BigInt(bet.expected_lamports);
        const tolerance = BigInt(5000); // Allow +/- 5000 lamports (adjust as needed)
        if (transferAmount < (expectedAmount - tolerance) ||
            transferAmount > (expectedAmount + tolerance)) {
            console.warn(`Amount mismatch for bet ${bet.id} (memo ${memo}). Expected ~${expectedAmount}, got ${transferAmount}.`);
            await updateBetStatus(bet.id, 'error_payment_mismatch');
            processedSignaturesThisSession.add(signature);
            // Send message to user? Optional.
            await bot.sendMessage(bet.chat_id, `⚠️ Payment amount mismatch for bet ${memo}. Expected ${Number(expectedAmount)/LAMPORTS_PER_SOL} SOL, received ${Number(transferAmount)/LAMPORTS_PER_SOL} SOL. Bet cancelled.`).catch(e => console.error("TG Send Error:", e.message));
            return { processed: false, reason: 'amount_mismatch' };
        }

        // 9. Validate transaction time vs bet expiry
        const txTime = tx.blockTime ? new Date(tx.blockTime * 1000) : new Date(0); // Use blockTime if available
        if (txTime === 0) {
            console.warn(`Could not determine blockTime for TX ${signature}. Skipping expiry check.`);
        } else if (txTime > new Date(bet.expires_at)) {
            console.warn(`Payment for bet ${bet.id} (memo ${memo}) received after expiry.`);
            await updateBetStatus(bet.id, 'error_payment_expired');
            processedSignaturesThisSession.add(signature);
            // Refund? Or just notify user.
            await bot.sendMessage(bet.chat_id, `⚠️ Payment for bet ${memo} received after expiry time. Bet cancelled.`).catch(e => console.error("TG Send Error:", e.message));
            return { processed: false, reason: 'expired' };
        }

        // 10. Mark bet as paid in DB
        const markResult = await markBetPaid(bet.id, signature);
        if (!markResult.success) {
            // Handle cases where marking failed (e.g., status changed concurrently)
            console.error(`Failed to mark bet ${bet.id} as paid: ${markResult.error}`);
             processedSignaturesThisSession.add(signature); // Add to processed if DB update fails to prevent retries
            return { processed: false, reason: 'db_mark_paid_failed' };
        }

        // 11. Link wallet address to user ID
        let actualPayer = payerAddress || getPayerFromTransaction(tx)?.toBase58();
        if (actualPayer) {
            await linkUserWallet(bet.user_id, actualPayer);
        } else {
             console.warn(`Could not identify payer address for bet ${bet.id} to link wallet.`);
        }

        // 12. Add signature to session cache AFTER successful DB update
        processedSignaturesThisSession.add(signature);
        // Clean up cache if too large
        if (processedSignaturesThisSession.size > MAX_PROCESSED_SIGNATURES) {
             console.log('Clearing processed signatures cache (reached max size)');
             // Keep only the most recent N signatures? Or clear completely? Clear for simplicity.
             processedSignaturesThisSession.clear();
        }

        // 13. Queue the bet for actual game processing (higher priority)
        console.log(`Payment verified for bet ${bet.id}. Queuing for game processing.`);
        await this.addPaymentJob({
            type: 'process_bet',
            betId: bet.id,
            priority: 1, // Higher priority than monitoring
            signature // Include signature for context if needed
        });

        return { processed: true };
    }
}

const paymentProcessor = new PaymentProcessor();


// --- Payment Monitoring Loop ---
let isMonitorRunning = false; // Flag to prevent concurrent monitor runs
let monitorIntervalSeconds = 30; // Initial interval
let monitorInterval = null; // Holds the setInterval ID

async function monitorPayments() {
    if (isMonitorRunning) {
        // console.log('[Monitor] Cycle already running, skipping.'); // Reduce log noise
        return;
    }

    isMonitorRunning = true;
    const startTime = Date.now();
    let signaturesFound = 0;

    try {
        const walletsToMonitor = [
            { address: process.env.MAIN_WALLET_ADDRESS, type: 'coinflip', priority: 0 },
            { address: process.env.RACE_WALLET_ADDRESS, type: 'race', priority: 0 },
        ];

        // Process each wallet
        for (const wallet of walletsToMonitor) {
            try {
                // *** PAGINATION FIX: Get the last signature processed for THIS wallet ***
                const beforeSig = lastProcessedSignature[wallet.address] || null;
                // console.log(`[Monitor] Checking ${wallet.type} wallet (${wallet.address}) for signatures before: ${beforeSig || 'Start'}`); // Optional: More detailed logging

                // Fetch latest signatures for the wallet using rate-limited connection
                const signatures = await solanaConnection.getSignaturesForAddress(
                    new PublicKey(wallet.address),
                    {
                        limit: 15,         // Fetch a reasonable batch size
                        before: beforeSig  // Use the specific 'before' signature for pagination
                    }
                );

                if (!signatures || signatures.length === 0) {
                   // No new signatures found for this wallet in this cycle
                   continue;
                }

                signaturesFound += signatures.length; // Count total found signatures

                // *** PAGINATION FIX: Update the last processed signature state for the *next* query cycle ***
                // Use the signature of the OLDEST transaction found in this batch (last in the array)
                const oldestSignatureInBatch = signatures[signatures.length - 1].signature;
                lastProcessedSignature[wallet.address] = oldestSignatureInBatch;
                // console.log(`[Monitor] Updated last signature for ${wallet.address} to: ${oldestSignatureInBatch}`); // Optional: Logging
                // *** END PAGINATION FIX ***

                // Queue each found signature for processing by the PaymentProcessor
                for (const sigInfo of signatures) {
                    // Avoid queuing if already seen in this session (quick check)
                    if (processedSignaturesThisSession.has(sigInfo.signature)) {
                       continue;
                    }
                    // Add job to the appropriate queue (normal priority for initial check)
                    await paymentProcessor.addPaymentJob({
                        type: 'monitor_payment',
                        signature: sigInfo.signature,
                        walletType: wallet.type,
                        priority: wallet.priority, // 0 for monitoring jobs
                        retries: 0
                    });
                }
            } catch (error) {
                // Log error for specific wallet but continue monitoring others
                console.error(`[Monitor] Error fetching/processing signatures for wallet ${wallet.address}:`, error.message);
                performanceMonitor.logRequest(false);
                // Optionally reset pagination for this wallet on error?
                // delete lastProcessedSignature[wallet.address];
            }
        } // End loop through wallets

    } catch (error) {
        // Catch errors in the overall monitor structure (less likely)
        console.error("[Monitor] Unexpected error in main monitor loop:", error);
        performanceMonitor.logRequest(false);
    } finally {
        isMonitorRunning = false; // Release the lock
        const duration = Date.now() - startTime;
        // console.log(`[Monitor] Cycle finished in ${duration}ms. Found ${signaturesFound} new signatures.`); // Reduce log noise unless signatures found > 0

        // Adjust monitoring interval based on activity
        adjustMonitorInterval(signaturesFound);
    }
}


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
        monitorInterval = setInterval(monitorPayments, monitorIntervalSeconds * 1000); // Set new interval
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
    // Ensure it's within reasonable bounds (e.g., min 1000, max 1,000,000 microLamports)
    const priorityFeeMicroLamports = Math.min(
        1000000, // Max priority fee (in microLamports)
        Math.max(1000, Math.floor(Number(amountLamports) * PRIORITY_FEE_RATE)) // Min 1000 microLamports
    );

    // Check if amount is sufficient after accounting for FEE_BUFFER
    const netAmountToSend = BigInt(amountLamports) - FEE_BUFFER;
    if (netAmountToSend <= 0n) {
        console.error(`❌ Payout amount ${amountLamports} is less than or equal to fee buffer ${FEE_BUFFER}. Cannot send.`);
        return { success: false, error: 'Payout amount too small after fee buffer' };
    }


    // Retry logic for sending transaction
    for (let attempt = 1; attempt <= 3; attempt++) {
        try {
            const payerWallet = Keypair.fromSecretKey(bs58.decode(privateKey));

            // Get latest blockhash before building transaction
            const latestBlockhash = await solanaConnection.executeWithRetry(
                'getLatestBlockhash',
                [{ commitment: 'confirmed' }] // Use confirmed blockhash
            );

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
            // Optional: Add compute unit limit if needed, but usually not for simple transfers
            // transaction.add(ComputeBudgetProgram.setComputeUnitLimit({ units: 200000 }));

            // Add the SOL transfer instruction
            transaction.add(
                SystemProgram.transfer({
                    fromPubkey: payerWallet.publicKey,
                    toPubkey: recipientPubKey,
                    lamports: netAmountToSend // Send the amount *after* buffer deduction
                })
            );

            console.log(`Sending TX (Attempt ${attempt})... Net Amount: ${netAmountToSend}, Priority Fee: ${priorityFeeMicroLamports} microLamports`);

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
                        reject(new Error(`Transaction confirmation timeout after 30s (Attempt ${attempt})`));
                    }, 30000); // 30 second timeout
                })
            ]);

            console.log(`✅ Payout successful! Sent ${Number(netAmountToSend)/LAMPORTS_PER_SOL} SOL to ${recipientPubKey.toBase58()}. TX: ${signature}`);
            return { success: true, signature };

        } catch (error) {
            console.error(`❌ Payout TX failed (Attempt ${attempt}/3):`, error.message);

            // Check for specific errors that shouldn't be retried
            if (error.message.includes('Invalid param') ||
                error.message.includes('Insufficient funds') ||
                error.message.includes('blockhash not found')) {
                 console.error("❌ Non-retryable error encountered. Aborting payout.");
                 return { success: false, error: `Non-retryable error: ${error.message}` }; // Exit retry loop
            }

            // If retryable error and not last attempt, wait before retrying
            if (attempt < 3) {
                await new Promise(resolve => setTimeout(resolve, 1500 * attempt)); // Wait longer each attempt
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
        if (client) await client.query('ROLLBACK'); // Ensure rollback on error
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
    // Random number < (0.5 - houseEdge/2) means 'heads' wins (adjusting win probability slightly)
    const result = Math.random() < (0.5 - config.houseEdge / 2) ? 'heads' : 'tails';
    const win = (result === choice);

    // Calculate payout amount if win (uses helper function)
    const payoutLamports = win ? calculatePayoutWithFees(expected_lamports, 'coinflip') : 0n;

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
                amount: payoutLamports,
                gameType: 'coinflip',
                priority: 2, // Higher priority than monitoring/game processing
                // Pass details needed for final payout message
                chatId: chat_id,
                displayName,
                result // Pass the result ('heads' or 'tails')
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

    // Define horses with names, emojis, odds (for payout), and win probabilities (for outcome)
    // Ensure winProbabilities sum close to 1.0 (adjust as needed for house edge balance)
    const horses = [
        { name: 'Yellow', emoji: '🟡', odds: 1.1, winProbability: 0.25 },
        { name: 'Orange', emoji: '🟠', odds: 2.0, winProbability: 0.20 },
        { name: 'Blue',   emoji: '🔵', odds: 3.0, winProbability: 0.15 },
        { name: 'Cyan',   emoji: '💧', odds: 4.0, winProbability: 0.12 }, // Using drop for cyan
        { name: 'White',  emoji: '⚪️', odds: 5.0, winProbability: 0.09 },
        { name: 'Red',    emoji: '🔴', odds: 6.0, winProbability: 0.07 },
        { name: 'Black',  emoji: '⚫️', odds: 7.0, winProbability: 0.05 },
        { name: 'Pink',   emoji: '🌸', odds: 8.0, winProbability: 0.03 },
        { name: 'Purple', emoji: '🟣', odds: 9.0, winProbability: 0.02 },
        { name: 'Green',  emoji: '🟢', odds: 10.0, winProbability: 0.01 },
        { name: 'Silver', emoji: '💎', odds: 15.0, winProbability: 0.01 } // Using diamond for silver
    ];
    // Verify probabilities sum close to 1 (for sanity check)
    // const totalProb = horses.reduce((sum, h) => sum + h.winProbability, 0);
    // console.log(`Total Race Probability: ${totalProb}`); // Should be ~1.0

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
    // Fallback in case of floating point issues (shouldn't happen if probs sum to 1)
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
        // Log error but continue processing the result
        console.error(`Error sending race commentary for bet ${betId}:`, e);
    }

    // Determine win/loss and payout
    const win = (chosenHorseName.toLowerCase() === winningHorse.name.toLowerCase());
    const payoutLamports = win
        ? calculatePayoutWithFees(expected_lamports, 'race', winningHorse) // Pass winning horse for odds
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
                amount: payoutLamports,
                gameType: 'race',
                priority: 2, // Higher priority
                // Pass details needed for final payout message
                chatId: chat_id,
                displayName,
                horseName: chosenHorseName, // User's chosen horse
                winningHorse // The actual winning horse object
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
    console.log(`💸 Processing payout job for Bet ID: ${betId}, Amount: ${amount} lamports`);

    try {
        // Attempt to send the SOL payout
        const sendResult = await sendSol(recipient, amount, gameType);

        if (sendResult.success) {
            console.log(`💸 Payout successful for bet ${betId}, TX: ${sendResult.signature}`);
            // Send success message to user
            await bot.sendMessage(chatId,
                `✅ Payout successful for bet ${betId}!\n` +
                `${(Number(amount)/LAMPORTS_PER_SOL).toFixed(6)} SOL sent.\n` +
                `TX: \`${sendResult.signature}\``,
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
            // TODO: Implement admin notification for failed payouts
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
         // Re-throw? Or let the processor handle it based on retry logic? Let processor handle.
         // throw error; // Re-throwing might trigger processor retry logic if not already exhausted
    }
}

// --- Utility Functions ---

// Calculates payout amount, applying house edge and subtracting fee buffer
function calculatePayoutWithFees(betLamports, gameType, gameDetails = {}) {
    betLamports = BigInt(betLamports); // Ensure input is BigInt
    let grossPayout = 0n;

    if (gameType === 'coinflip') {
        // Payout is 2x minus house edge (e.g., 2 - 0.02 = 1.98 multiplier)
        grossPayout = BigInt(Math.floor(Number(betLamports) * (2.0 - GAME_CONFIG.coinflip.houseEdge)));
    } else if (gameType === 'race' && gameDetails.odds) {
        // Payout is Bet * Odds * (1 - House Edge)
        grossPayout = BigInt(Math.floor(Number(betLamports) * gameDetails.odds * (1.0 - GAME_CONFIG.race.houseEdge)));
    } else {
         console.error(`Cannot calculate payout: Unknown game type ${gameType} or missing race odds.`);
         return 0n; // Return 0 if calculation fails
    }

    // Subtract the fee buffer AFTER calculating gross winnings
    const netPayout = grossPayout > FEE_BUFFER ? grossPayout - FEE_BUFFER : 0n;

    // console.log(`Payout Calc: Bet ${betLamports}, Gross ${grossPayout}, Net ${netPayout}`); // Debug log
    return netPayout;
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
         console.warn(`Couldn't get username for user ${user_id} in chat ${chat_id}:`, e.message);
         return `User ${user_id.substring(0, 6)}...`; // Return partial ID as fallback
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
        confirmCooldown.set(userId, now); // Update last command time

        // --- Command Routing ---
        const text = msg.text.trim(); // Trim whitespace

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
        // Add /help command?
        else if (text.match(/^\/help$/i)) {
             await handleHelpCommand(msg);
        }
        // Ignore other messages or unrecognized commands silently, or add a default handler

        performanceMonitor.logRequest(true); // Log successful command handling

    } catch (error) {
        console.error(`❌ Error processing message from user ${userId} in chat ${chatId}: "${msg.text}"`, error);
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
             console.warn("Failed to send start animation, sending text fallback:", err.message);
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
        `- Payout: ~1.98x (after house edge)\n\n` +
        `You will be given a wallet address and a *unique Memo ID*. Send the *exact* SOL amount with the memo to place your bet.`,
        { parse_mode: 'Markdown' }
    ).catch(e => console.error("TG Send Error:", e.message));
}

// Handles the /race command (shows instructions)
async function handleRaceCommand(msg) {
    // Use the same horse data as in handleRaceGame for consistency
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
        raceMessage += `- ${horse.emoji} *${horse.name}* (${horse.odds.toFixed(1)}x Payout)\n`;
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
    if (!match) return; // Should not happen if routing is correct, but safety check

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const config = GAME_CONFIG.coinflip;

    // Validate bet amount from regex match
    const betAmount = parseFloat(match[1]);
    if (isNaN(betAmount) || betAmount < config.minBet || betAmount > config.maxBet) {
        await bot.sendMessage(chatId,
            `⚠️ Invalid bet amount. Please bet between ${config.minBet} and ${config.maxBet} SOL.\n` +
            `Example: \`/bet 0.1 heads\``,
             { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
        return;
    }

    const userChoice = match[2].toLowerCase(); // 'heads' or 'tails'
    const memoId = generateMemoId('CF'); // Generate Coinflip memo
    const expectedLamports = BigInt(Math.round(betAmount * LAMPORTS_PER_SOL));
    const expiresAt = new Date(Date.now() + config.expiryMinutes * 60 * 1000); // Expiry time

    // Save the pending bet to the database
    const saveResult = await savePendingBet(
        userId, chatId, 'coinflip',
        { choice: userChoice }, // Store the user's choice
        expectedLamports, memoId, expiresAt
    );

    if (!saveResult.success) {
         // Inform user if saving failed (e.g., memo collision)
         await bot.sendMessage(chatId, `⚠️ Error registering bet: ${saveResult.error}. Please try the command again.`).catch(e => console.error("TG Send Error:", e.message));
         // Throw error to be caught by main handler for logging
         throw new Error(saveResult.error || "Failed to save coinflip bet");
    }

    // Send payment instructions to the user
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


// Handles /betrace command
async function handleBetRaceCommand(msg) {
    const match = msg.text.match(/^\/betrace (\d+\.?\d*)\s+(\w+)/i);
     if (!match) return;

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
    const horses = [ // Use same list as game logic
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

    const memoId = generateMemoId('RA'); // Generate Race memo
    const expectedLamports = BigInt(Math.round(betAmount * LAMPORTS_PER_SOL));
    const expiresAt = new Date(Date.now() + config.expiryMinutes * 60 * 1000);

    // Save the pending bet to the database
    const saveResult = await savePendingBet(
        userId, chatId, 'race',
        { horse: chosenHorse.name, odds: chosenHorse.odds }, // Store chosen horse and odds
        expectedLamports, memoId, expiresAt
    );

     if (!saveResult.success) {
         await bot.sendMessage(chatId, `⚠️ Error registering bet: ${saveResult.error}. Please try the command again.`).catch(e => console.error("TG Send Error:", e.message));
         throw new Error(saveResult.error || "Failed to save race bet");
     }

    // Send payment instructions
    await bot.sendMessage(chatId,
        `✅ Race bet registered! (ID: ${memoId})\n\n` +
        `You chose: ${chosenHorse.emoji} *${chosenHorse.name}*\n` +
        `Amount: *${betAmount.toFixed(6)} SOL*\n` +
        `Potential Payout: ${(betAmount * chosenHorse.odds * (1 - config.houseEdge)).toFixed(6)} SOL\n\n`+
        `➡️ Send *exactly ${betAmount.toFixed(6)} SOL* to:\n` +
        `\`${process.env.RACE_WALLET_ADDRESS}\`\n\n` +
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
            monitorInterval = setInterval(monitorPayments, monitorIntervalSeconds * 1000);

            // Run monitor once shortly after startup
            setTimeout(() => {
                 console.log("⚙️ Performing initial payment monitor run...");
                 monitorPayments().catch(console.error);
            }, 5000); // Delay initial run slightly

            // Start polling if webhook is NOT used (e.g., local development)
            if (!process.env.RAILWAY_ENVIRONMENT) {
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
         if (process.env.RAILWAY_ENVIRONMENT) {
             await bot.deleteWebHook({ drop_pending_updates: true });
             console.log("- Removed Telegram webhook.");
         } else if (bot.isPolling()) {
              await bot.stopPolling({ cancel: true }); // Cancel polling
              console.log("- Stopped Telegram polling.");
         }
         // Close Express server? (Optional, depends on needs)
         // server.close(() => console.log("- Express server closed."));
    } catch (e) {
        console.error("⚠️ Error stopping bot listeners:", e.message);
    }

    // 2. Wait for ongoing queue processing to finish (with timeout)
    console.log("Waiting for active jobs to finish...");
    try {
         await Promise.all([
             messageQueue.onIdle(),
             paymentProcessor.highPriorityQueue.onIdle(),
             paymentProcessor.normalQueue.onIdle()
         ]);
         console.log("- All processing queues are idle.");
    } catch (queueError) {
         console.warn("⚠️ Timed out waiting for queues or queue error:", queueError.message);
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
    // Optionally trigger shutdown here too? Or just log? Logging is safer.
});

// --- Start the Application ---
startServer().then(() => {
    console.log("🚀🚀🚀 Solana Gambles Bot is up and running! 🚀🚀🚀");
}).catch(err => {
    // This catch is for errors thrown *during* the async startServer call itself
    console.error("🔥🔥🔥 Application failed to initialize:", err);
    process.exit(1);
});
