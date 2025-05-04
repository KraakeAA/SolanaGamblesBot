// index.js - Custodial Model with Buttons & Referrals (Refactored)
// --- VERSION: 3.1.0 ---

// --- All Imports MUST come first ---
import 'dotenv/config';
import { Pool } from 'pg';
import express from 'express';
import TelegramBot from 'node-telegram-bot-api';
import {
    Connection, // Use Connection directly or RateLimitedConnection if preferred
    PublicKey,
    LAMPORTS_PER_SOL,
    Keypair,
    Transaction,
    SystemProgram,
    sendAndConfirmTransaction,
    ComputeBudgetProgram,
    SendTransactionError
} from '@solana/web3.js';
import bs58 from 'bs58';
import * as crypto from 'crypto';
import PQueue from 'p-queue';
import { Buffer } from 'buffer'; // Standard Node.js Buffer
import bip39 from 'bip39'; // For mnemonic seed phrase generation/validation
import { derivePath } from 'ed25519-hd-key'; // For BIP32 derivation
import nacl from 'tweetnacl'; // For keypair generation from derived seed

// Assuming RateLimitedConnection exists and works as intended
// If not using it, replace `solanaConnection` initialization with `new Connection(...)`
import RateLimitedConnection from './lib/solana-connection.js';

// --- Deployment Check Log ---
console.log(`--- INDEX.JS - DEPLOYMENT CHECK --- ${new Date().toISOString()} --- v3.1.0 ---`);
// --- END DEPLOYMENT CHECK ---


console.log("⏳ Starting Solana Gambles Bot (Custodial, Buttons, v3.1.0)... Checking environment variables...");

// --- Environment Variable Configuration ---
const REQUIRED_ENV_VARS = [
    'BOT_TOKEN',
    'DATABASE_URL',
    'DEPOSIT_MASTER_SEED_PHRASE', // **CHANGED**: Expect BIP39 Mnemonic Seed Phrase (SECRET)
    'MAIN_BOT_PRIVATE_KEY',       // Used for general payouts (Withdrawals) (SECRET)
    // Optional separate key for referral payouts (falls back to MAIN_BOT_PRIVATE_KEY)
    'REFERRAL_PAYOUT_PRIVATE_KEY',// (SECRET)
    'RPC_URLS',                     // Comma-separated list of RPC URLs
];

// Check for Railway-specific variables if deployed there
if (process.env.RAILWAY_ENVIRONMENT) {
    REQUIRED_ENV_VARS.push('RAILWAY_PUBLIC_DOMAIN');
}

// Validate required environment variables
let missingVars = false;
REQUIRED_ENV_VARS.forEach((key) => {
    // Allow REFERRAL_PAYOUT_PRIVATE_KEY to be optional (it falls back)
    if (key === 'REFERRAL_PAYOUT_PRIVATE_KEY') return;
    // Check Railway var only if in Railway env
    if (key === 'RAILWAY_PUBLIC_DOMAIN' && !process.env.RAILWAY_ENVIRONMENT) return;

    if (!process.env[key]) {
        console.error(`❌ Environment variable ${key} is missing.`);
        missingVars = true;
    }
});

// Specific check for valid Seed Phrase
if (process.env.DEPOSIT_MASTER_SEED_PHRASE && !bip39.validateMnemonic(process.env.DEPOSIT_MASTER_SEED_PHRASE)) {
    console.error(`❌ Environment variable DEPOSIT_MASTER_SEED_PHRASE is set but is not a valid BIP39 mnemonic.`);
    missingVars = true;
}

// Specific check for RPC_URLS content
const parsedRpcUrls = (process.env.RPC_URLS || '').split(',').map(u => u.trim()).filter(u => u && (u.startsWith('http://') || u.startsWith('https://')));
if (parsedRpcUrls.length === 0) {
    console.error(`❌ Environment variable RPC_URLS is missing or contains no valid URLs.`);
    missingVars = true;
} else if (parsedRpcUrls.length === 1) {
    console.warn(`⚠️ Environment variable RPC_URLS only contains one URL. Multi-RPC features may not be fully utilized.`);
}

// Validate private keys format (basic check for base58)
const validateBase58Key = (keyName) => {
    const key = process.env[keyName];
    if (key) {
        try {
            const decoded = bs58.decode(key);
            if (decoded.length !== 64) { // ed25519 private keys are 64 bytes (32 byte seed + 32 byte public key)
                console.error(`❌ Environment variable ${keyName} has incorrect length after base58 decoding (Expected 64 bytes, Got ${decoded.length}).`);
                return false;
            }
        } catch (e) {
            console.error(`❌ Failed to decode environment variable ${keyName} as base58: ${e.message}`);
            return false;
        }
    } else if (REQUIRED_ENV_VARS.includes(keyName) && keyName !== 'REFERRAL_PAYOUT_PRIVATE_KEY') {
        // Already caught by missingVars check, no need to log again unless optional and present but invalid
        return false; // Return false if required and missing
    }
    return true; // Valid or optional and not set
};

if (!validateBase58Key('MAIN_BOT_PRIVATE_KEY')) missingVars = true;
if (!validateBase58Key('REFERRAL_PAYOUT_PRIVATE_KEY')) missingVars = true; // Check format if present

if (missingVars) {
    console.error("⚠️ Please set all required environment variables correctly. Exiting.");
    process.exit(1);
}

// Optional vars with defaults (using BigInt where appropriate)
const OPTIONAL_ENV_DEFAULTS = {
    // --- Operational ---
    'ADMIN_USER_IDS': '',
    'DEPOSIT_ADDRESS_EXPIRY_MINUTES': '60', // How long a unique deposit address is valid
    'DEPOSIT_CONFIRMATIONS': 'confirmed',   // Solana commitment level ('confirmed' or 'finalized')
    'WITHDRAWAL_FEE_LAMPORTS': '5000',      // Flat withdrawal fee (can be 0)
    'MIN_WITHDRAWAL_LAMPORTS': '10000000',  // Minimum withdrawal amount (0.01 SOL)
    // --- Payouts (Withdrawals, Referral Rewards) ---
    'PAYOUT_BASE_PRIORITY_FEE_MICROLAMPORTS': '1000',   // Min priority fee for payouts
    'PAYOUT_MAX_PRIORITY_FEE_MICROLAMPORTS': '1000000', // Max priority fee for payouts
    'PAYOUT_COMPUTE_UNIT_LIMIT': '200000',             // Default compute unit limit for transfers
    'PAYOUT_JOB_RETRIES': '3',                          // Retries for Withdrawals/Referral payouts
    'PAYOUT_JOB_RETRY_DELAY_MS': '5000',                // Base delay for payout retries
    // --- Game Limits ---
    'CF_MIN_BET_LAMPORTS': '10000000',   // 0.01 SOL
    'CF_MAX_BET_LAMPORTS': '1000000000',  // 1 SOL
    'RACE_MIN_BET_LAMPORTS': '10000000',
    'RACE_MAX_BET_LAMPORTS': '1000000000',
    'SLOTS_MIN_BET_LAMPORTS': '10000000',
    'SLOTS_MAX_BET_LAMPORTS': '500000000', // 0.5 SOL
    'ROULETTE_MIN_BET_LAMPORTS': '10000000',
    'ROULETTE_MAX_BET_LAMPORTS': '1000000000',
    'WAR_MIN_BET_LAMPORTS': '10000000',
    'WAR_MAX_BET_LAMPORTS': '1000000000',
    // --- Game Logic Parameters ---
    'CF_HOUSE_EDGE': '0.03',            // 3% edge
    'RACE_HOUSE_EDGE': '0.05',          // 5% edge
    'SLOTS_HOUSE_EDGE': '0.10',         // Effective edge for slots payouts
    'ROULETTE_HOUSE_EDGE': '0.05',      // Standard Roulette edge (0, 00) ~5.26%, simplified
    'WAR_HOUSE_EDGE': '0.02',           // Approx edge in War (mainly from ties)
    // --- Referral System ---
    'REFERRAL_INITIAL_BET_MIN_LAMPORTS': '10000000', // Min first bet size to trigger initial reward (0.01 SOL)
    'REFERRAL_MILESTONE_REWARD_PERCENT': '0.005',   // 0.5% of wagered amount for milestones
    // --- Technical / Performance ---
    'RPC_MAX_CONCURRENT': '8',          // Concurrent requests for RateLimitedConnection
    'RPC_RETRY_BASE_DELAY': '600',
    'RPC_MAX_RETRIES': '3',
    'RPC_RATE_LIMIT_COOLOFF': '1500',
    'RPC_RETRY_MAX_DELAY': '15000',
    'RPC_RETRY_JITTER': '0.2',
    'RPC_COMMITMENT': 'confirmed',       // Default commitment for reads
    'MSG_QUEUE_CONCURRENCY': '5',        // Incoming Telegram message processing
    'MSG_QUEUE_TIMEOUT_MS': '10000',
    'CALLBACK_QUEUE_CONCURRENCY': '8',   // Button callback processing
    'CALLBACK_QUEUE_TIMEOUT_MS': '15000',
    'PAYOUT_QUEUE_CONCURRENCY': '3',     // Withdrawal/Referral payout processing
    'PAYOUT_QUEUE_TIMEOUT_MS': '60000',  // Longer timeout for payouts
    'DEPOSIT_PROCESS_QUEUE_CONCURRENCY': '4', // Processing confirmed deposit transactions
    'DEPOSIT_PROCESS_QUEUE_TIMEOUT_MS': '30000',
    'TELEGRAM_SEND_QUEUE_CONCURRENCY': '1', // MUST BE 1 for rate limits
    'TELEGRAM_SEND_QUEUE_INTERVAL_MS': '1050', // ~1 message per second allowance
    'TELEGRAM_SEND_QUEUE_INTERVAL_CAP': '1',
    'DEPOSIT_MONITOR_INTERVAL_MS': '20000', // How often to check for deposits (polling method)
    'DEPOSIT_MONITOR_ADDRESS_BATCH_SIZE': '50', // How many pending addresses to check per cycle
    'DEPOSIT_MONITOR_SIGNATURE_FETCH_LIMIT': '5', // How many recent signatures to check per address
    'DB_POOL_MAX': '25',                 // Max DB connections
    'DB_POOL_MIN': '5',
    'DB_IDLE_TIMEOUT': '30000',
    'DB_CONN_TIMEOUT': '5000',
    'DB_SSL': 'true',
    'DB_REJECT_UNAUTHORIZED': 'true',
    'USER_STATE_CACHE_TTL_MS': '300000', // 5 minutes for conversational state
    'WALLET_CACHE_TTL_MS': '600000',     // 10 minutes for user wallet/ref code cache
    'DEPOSIT_ADDR_CACHE_TTL_MS': '3660000', // ~1 hour for active deposit addresses cache
    'MAX_PROCESSED_TX_CACHE': '5000',    // Max deposit TX sigs to keep in memory this session
    'USER_COMMAND_COOLDOWN_MS': '1000',  // 1 second cooldown
    'INIT_DELAY_MS': '1000',             // Delay before starting background tasks
    'SHUTDOWN_QUEUE_TIMEOUT_MS': '20000', // Max time to wait for queues during shutdown
    'SHUTDOWN_FAIL_TIMEOUT_MS': '8000',  // Timeout before force exit on critical failure
    'WEBHOOK_MAX_CONN': '10',            // Max connections for Telegram webhook
};

// Assign defaults for optional vars if they are missing
Object.entries(OPTIONAL_ENV_DEFAULTS).forEach(([key, defaultValue]) => {
    if (!process.env[key]) {
        process.env[key] = defaultValue;
    }
});

console.log("✅ Environment variables checked/defaults applied.");
// --- END: Environment Variable Configuration ---


// --- Global Constants & State ---
const SOL_DECIMALS = 9;
const DEPOSIT_ADDRESS_EXPIRY_MS = parseInt(process.env.DEPOSIT_ADDRESS_EXPIRY_MINUTES, 10) * 60 * 1000;
const DEPOSIT_CONFIRMATION_LEVEL = process.env.DEPOSIT_CONFIRMATIONS === 'finalized' ? 'finalized' : 'confirmed';
const WITHDRAWAL_FEE_LAMPORTS = BigInt(process.env.WITHDRAWAL_FEE_LAMPORTS);
const MIN_WITHDRAWAL_LAMPORTS = BigInt(process.env.MIN_WITHDRAWAL_LAMPORTS);
const MAX_PROCESSED_TX_CACHE_SIZE = parseInt(process.env.MAX_PROCESSED_TX_CACHE, 10);
const USER_COMMAND_COOLDOWN_MS = parseInt(process.env.USER_COMMAND_COOLDOWN_MS, 10);
const USER_STATE_TTL_MS = parseInt(process.env.USER_STATE_CACHE_TTL_MS, 10);
const WALLET_CACHE_TTL_MS = parseInt(process.env.WALLET_CACHE_TTL_MS, 10);
const DEPOSIT_ADDR_CACHE_TTL_MS = parseInt(process.env.DEPOSIT_ADDR_CACHE_TTL_MS, 10);
const REFERRAL_INITIAL_BET_MIN_LAMPORTS = BigInt(process.env.REFERRAL_INITIAL_BET_MIN_LAMPORTS);
const REFERRAL_MILESTONE_REWARD_PERCENT = parseFloat(process.env.REFERRAL_MILESTONE_REWARD_PERCENT);
// Define Referral Tiers (Example) - Based on referrer's *current* count BEFORE this referral's first bet
const REFERRAL_INITIAL_BONUS_TIERS = [
    { maxCount: 10, percent: 0.05 },  // 0-10 referrals -> 5%
    { maxCount: 25, percent: 0.10 },  // 11-25 referrals -> 10%
    { maxCount: 50, percent: 0.15 },  // 26-50 referrals -> 15%
    { maxCount: 100, percent: 0.20 }, // 51-100 referrals -> 20%
    { maxCount: Infinity, percent: 0.25 } // 101+ referrals -> 25%
];
// Define Milestone Thresholds
const REFERRAL_MILESTONE_THRESHOLDS_LAMPORTS = [
    BigInt(1 * LAMPORTS_PER_SOL), BigInt(5 * LAMPORTS_PER_SOL), BigInt(10 * LAMPORTS_PER_SOL),
    BigInt(25 * LAMPORTS_PER_SOL), BigInt(50 * LAMPORTS_PER_SOL), BigInt(100 * LAMPORTS_PER_SOL),
    BigInt(250 * LAMPORTS_PER_SOL), BigInt(500 * LAMPORTS_PER_SOL), BigInt(1000 * LAMPORTS_PER_SOL)
];

// In-memory Caches & State
const userStateCache = new Map(); // Map<userId, { state: string, chatId: string, messageId?: number, data?: any, timestamp: number }>
const walletCache = new Map(); // Map<userId, { withdrawalAddress?: string, referralCode?: string, timestamp: number }>
const activeDepositAddresses = new Map(); // Map<deposit_address, { userId: string, expiresAt: number }>
const processedDepositTxSignatures = new Set(); // Store processed deposit TX sigs this session
const commandCooldown = new Map(); // Map<userId, timestamp>
const pendingReferrals = new Map(); // Map<userId, { referrerUserId: string, timestamp: number }>
const PENDING_REFERRAL_TTL_MS = 24 * 60 * 60 * 1000; // 24 hours

let isFullyInitialized = false;
let server; // Express server instance
let depositMonitorIntervalId = null; // Interval ID for the deposit monitor
// --- End Global Constants & State ---


// --- Core Initializations ---

// Express App
const app = express();
app.use(express.json({ limit: '10kb' })); // Keep request size limit reasonable

// Solana Connection (Using RateLimitedConnection wrapper)
console.log("⚙️ Initializing Multi-RPC Solana connection...");
console.log(`ℹ️ Using RPC Endpoints: ${parsedRpcUrls.join(', ')}`);
const solanaConnection = new RateLimitedConnection(parsedRpcUrls, {
    maxConcurrent: parseInt(process.env.RPC_MAX_CONCURRENT, 10),
    retryBaseDelay: parseInt(process.env.RPC_RETRY_BASE_DELAY, 10),
    maxRetries: parseInt(process.env.RPC_MAX_RETRIES, 10),
    rateLimitCooloff: parseInt(process.env.RPC_RATE_LIMIT_COOLOFF, 10),
    retryMaxDelay: parseInt(process.env.RPC_RETRY_MAX_DELAY, 10),
    retryJitter: parseFloat(process.env.RPC_RETRY_JITTER),
    commitment: process.env.RPC_COMMITMENT, // Default 'confirmed'
    httpHeaders: { 'User-Agent': `SolanaGamblesBot/3.1.0` },
    // disableRetryOnRateLimit: false, // Allow retries on 429 by default? Depends on wrapper library
});
console.log("✅ Multi-RPC Solana connection instance created.");

// Queues
console.log("⚙️ Initializing Processing Queues...");
const messageQueue = new PQueue({
    concurrency: parseInt(process.env.MSG_QUEUE_CONCURRENCY, 10),
    timeout: parseInt(process.env.MSG_QUEUE_TIMEOUT_MS, 10),
    throwOnTimeout: true
});
const callbackQueue = new PQueue({
    concurrency: parseInt(process.env.CALLBACK_QUEUE_CONCURRENCY, 10),
    timeout: parseInt(process.env.CALLBACK_QUEUE_TIMEOUT_MS, 10),
    throwOnTimeout: true
});
const payoutProcessorQueue = new PQueue({ // Renamed from paymentProcessorQueue for clarity
    concurrency: parseInt(process.env.PAYOUT_QUEUE_CONCURRENCY, 10),
    timeout: parseInt(process.env.PAYOUT_QUEUE_TIMEOUT_MS, 10),
    // Payouts should ideally not timeout easily, but throwOnTimeout helps detect stalls
    throwOnTimeout: true
});
const depositProcessorQueue = new PQueue({ // Queue for processing confirmed deposits found by monitor
    concurrency: parseInt(process.env.DEPOSIT_PROCESS_QUEUE_CONCURRENCY, 10),
    timeout: parseInt(process.env.DEPOSIT_PROCESS_QUEUE_TIMEOUT_MS, 10),
    throwOnTimeout: true
});
const telegramSendQueue = new PQueue({ // Separate queue for sending messages respecting Telegram limits
    concurrency: parseInt(process.env.TELEGRAM_SEND_QUEUE_CONCURRENCY, 10), // Should be 1
    interval: parseInt(process.env.TELEGRAM_SEND_QUEUE_INTERVAL_MS, 10),
    intervalCap: parseInt(process.env.TELEGRAM_SEND_QUEUE_INTERVAL_CAP, 10) // Should be 1
});
console.log("✅ Processing Queues initialized.");

// PostgreSQL Pool
console.log("⚙️ Setting up PostgreSQL Pool...");
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    max: parseInt(process.env.DB_POOL_MAX, 10),
    min: parseInt(process.env.DB_POOL_MIN, 10),
    idleTimeoutMillis: parseInt(process.env.DB_IDLE_TIMEOUT, 10),
    connectionTimeoutMillis: parseInt(process.env.DB_CONN_TIMEOUT, 10),
    ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: process.env.DB_REJECT_UNAUTHORIZED === 'true' } : false,
});
pool.on('error', (err, client) => {
    console.error('❌ Unexpected error on idle PostgreSQL client', err);
    // Optional: attempt to remove the client from the pool? pool.remove(client)? Needs testing.
});
console.log("✅ PostgreSQL Pool created.");

// Telegram Bot Instance
console.log("⚙️ Initializing Telegram Bot...");
const bot = new TelegramBot(process.env.BOT_TOKEN, {
    polling: false, // Set dynamically later based on webhook success
    request: {      // Adjust request options for stability
        timeout: 15000, // Increased request timeout
        agentOptions: {
            keepAlive: true,
            keepAliveMsecs: 60000, // Standard keep-alive
            maxSockets: 100, // Allow more sockets for potentially higher throughput (if needed)
            maxFreeSockets: 10,
            scheduling: 'fifo',
        }
    }
});
console.log("✅ Telegram Bot instance created.");

// Game Configuration Loading
console.log("⚙️ Loading Game Configuration...");
const GAME_CONFIG = {
    coinflip: {
        minBetLamports: BigInt(process.env.CF_MIN_BET_LAMPORTS),
        maxBetLamports: BigInt(process.env.CF_MAX_BET_LAMPORTS),
        houseEdge: parseFloat(process.env.CF_HOUSE_EDGE)
    },
    race: {
        minBetLamports: BigInt(process.env.RACE_MIN_BET_LAMPORTS),
        maxBetLamports: BigInt(process.env.RACE_MAX_BET_LAMPORTS),
        houseEdge: parseFloat(process.env.RACE_HOUSE_EDGE)
        // TODO: Define odds/horses within game logic handler
    },
    slots: {
        minBetLamports: BigInt(process.env.SLOTS_MIN_BET_LAMPORTS),
        maxBetLamports: BigInt(process.env.SLOTS_MAX_BET_LAMPORTS),
        houseEdge: parseFloat(process.env.SLOTS_HOUSE_EDGE) // Use this to tune payouts
        // TODO: Define symbols/payouts within game logic handler
    },
    roulette: {
        minBetLamports: BigInt(process.env.ROULETTE_MIN_BET_LAMPORTS),
        maxBetLamports: BigInt(process.env.ROULETTE_MAX_BET_LAMPORTS),
        houseEdge: parseFloat(process.env.ROULETTE_HOUSE_EDGE) // Theoretical edge
        // TODO: Define payouts within game logic handler
    },
    war: {
        minBetLamports: BigInt(process.env.WAR_MIN_BET_LAMPORTS),
        maxBetLamports: BigInt(process.env.WAR_MAX_BET_LAMPORTS),
        houseEdge: parseFloat(process.env.WAR_HOUSE_EDGE) // Mostly from ties
    }
};
// Basic validation function (can be expanded)
function validateGameConfig(config) {
    const errors = [];
    for (const game in config) {
        const gc = config[game];
        if (!gc) { errors.push(`Config for game "${game}" missing.`); continue; }
        if (!gc.minBetLamports || gc.minBetLamports <= 0n) errors.push(`${game}.minBetLamports invalid (${gc.minBetLamports})`);
        if (!gc.maxBetLamports || gc.maxBetLamports <= 0n) errors.push(`${game}.maxBetLamports invalid (${gc.maxBetLamports})`);
        if (gc.minBetLamports > gc.maxBetLamports) errors.push(`${game} minBet > maxBet`);
        if (isNaN(gc.houseEdge) || gc.houseEdge < 0 || gc.houseEdge >= 1) errors.push(`${game}.houseEdge invalid (${gc.houseEdge})`);
    }
    return errors;
}
const configErrors = validateGameConfig(GAME_CONFIG);
if (configErrors.length > 0) {
    console.error(`❌ Invalid game configuration values: ${configErrors.join(', ')}.`);
    process.exit(1);
}
console.log("✅ Game Config Loaded and Validated.");
// --- End Core Initializations ---

// --- End of Part 1 ---
// index.js - Part 2: Database Operations
// --- VERSION: 3.1.0 ---

// --- Helper Function for DB Operations ---
/**
 * Executes a database query using either a provided client or the pool.
 * @param {string} sql The SQL query string with placeholders ($1, $2, ...).
 * @param {Array<any>} [params=[]] The parameters for the SQL query.
 * @param {pg.PoolClient | pg.Pool} [dbClient=pool] The DB client or pool to use.
 * @returns {Promise<pg.QueryResult<any>>} The query result.
 */
async function queryDatabase(sql, params = [], dbClient = pool) {
    try {
        return await dbClient.query(sql, params);
    } catch (error) {
        // Log enhanced error information
        console.error(`❌ DB Query Error:`);
        console.error(`  SQL: ${sql.substring(0, 500)}${sql.length > 500 ? '...' : ''}`); // Log truncated SQL
        console.error(`  Params: ${JSON.stringify(params)}`);
        console.error(`  Error Code: ${error.code}`);
        console.error(`  Error Message: ${error.message}`);
        // console.error(`  Stack: ${error.stack}`); // Optional: full stack trace
        throw error; // Re-throw the original error for upstream handling
    }
}
// --- End Helper ---


// --- Wallet/User Operations ---

/**
 * Ensures a user exists in the 'wallets' table, creating a basic record if not.
 * Also ensures a corresponding 'user_balances' record exists.
 * Should be called within a transaction if used alongside other user modifications.
 * @param {string} userId The user's Telegram ID.
 * @param {pg.PoolClient} client The active database client connection.
 * @returns {Promise<{userId: string, isNewUser: boolean}>}
 */
async function ensureUserExists(userId, client) {
    userId = String(userId);
    let isNewUser = false;

    // 1. Check/Insert into wallets
    const walletCheck = await queryDatabase('SELECT user_id FROM wallets WHERE user_id = $1 FOR UPDATE', [userId], client);
    if (walletCheck.rowCount === 0) {
        // Insert basic wallet record (referral code generated later if needed)
        await queryDatabase(
            `INSERT INTO wallets (user_id, created_at, last_used_at) VALUES ($1, NOW(), NOW())`,
            [userId],
            client
        );
        isNewUser = true;
        console.log(`[DB ensureUserExists] Created base wallet record for new user ${userId}`);
    } else {
        // Update last_used_at for existing user
        await queryDatabase(
            `UPDATE wallets SET last_used_at = NOW() WHERE user_id = $1`,
            [userId],
            client
        );
    }

    // 2. Check/Insert into user_balances
    const balanceCheck = await queryDatabase('SELECT user_id FROM user_balances WHERE user_id = $1 FOR UPDATE', [userId], client);
    if (balanceCheck.rowCount === 0) {
        await queryDatabase(
            `INSERT INTO user_balances (user_id, balance_lamports, updated_at) VALUES ($1, 0, NOW())`,
            [userId],
            client
        );
        if (!isNewUser) { // Log if balance was missing for an existing wallet user
             console.warn(`[DB ensureUserExists] Created missing balance record for existing user ${userId}`);
        }
    }
    // If we just created the user, their balance is guaranteed to be 0. If they existed, we don't modify balance here.

    return { userId, isNewUser };
}


/**
 * Updates the external withdrawal address for a user. Creates user record if non-existent.
 * Generates referral code if missing.
 * Handles its own transaction.
 * @param {string} userId
 * @param {string} externalAddress Valid Solana address string.
 * @returns {Promise<{success: boolean, wallet?: string, referralCode?: string, error?: string}>}
 */
async function linkUserWallet(userId, externalAddress) {
    userId = String(userId);
    const logPrefix = `[LinkWallet User ${userId}]`;
    let client;
    try {
        // Validate address format before hitting DB
        new PublicKey(externalAddress);

        client = await pool.connect();
        await client.query('BEGIN');

        // Ensure user exists (locks rows)
        await ensureUserExists(userId, client);

        // Fetch current details, generate referral code if needed
        const detailsRes = await queryDatabase('SELECT referral_code FROM wallets WHERE user_id = $1 FOR UPDATE', [userId], client);
        let currentReferralCode = detailsRes.rows[0]?.referral_code;
        let needsCodeUpdate = false;

        if (!currentReferralCode) {
            console.log(`${logPrefix} User missing referral code, generating one.`);
            currentReferralCode = generateReferralCode(); // generateReferralCode defined later in utils
            needsCodeUpdate = true;
        }

        // Update external address and linked_at time. Also update referral code if it was just generated.
        const updateQuery = `
            UPDATE wallets
            SET external_withdrawal_address = $1,
                linked_at = COALESCE(linked_at, NOW()), -- Only set linked_at once
                last_used_at = NOW()
                ${needsCodeUpdate ? ', referral_code = $3' : ''}
            WHERE user_id = $2
        `;
        const updateParams = needsCodeUpdate ? [externalAddress, userId, currentReferralCode] : [externalAddress, userId];
        await queryDatabase(updateQuery, updateParams, client);

        await client.query('COMMIT');
        console.log(`${logPrefix} External withdrawal address set/updated to ${externalAddress}. Referral Code: ${currentReferralCode}`);

        // Update cache
        updateWalletCache(userId, { withdrawalAddress: externalAddress, referralCode: currentReferralCode });

        return { success: true, wallet: externalAddress, referralCode: currentReferralCode };

    } catch (err) {
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { console.error(`${logPrefix} Rollback failed:`, rbErr); } }

        if (err instanceof Error && (err.message.includes('Invalid public key') || err.message.includes('Invalid address'))) {
            console.error(`${logPrefix} Invalid address format provided: ${externalAddress}`);
            return { success: false, error: 'Invalid Solana wallet address format.' };
        }
        // Handle potential unique constraint violation on referral_code (unlikely but possible if generation collides)
        if (err.code === '23505' && err.constraint === 'wallets_referral_code_key') {
            console.error(`${logPrefix} CRITICAL - Referral code generation conflict for user ${userId}. Retrying may help.`);
            return { success: false, error: 'Referral code generation conflict. Please try again.' };
        }
        console.error(`${logPrefix} DB Error linking wallet:`, err.message, err.code);
        return { success: false, error: `Database error linking wallet (${err.code || 'N/A'})` };
    } finally {
        if (client) client.release();
    }
}

/**
 * Fetches comprehensive user details from the 'wallets' table.
 * @param {string} userId
 * @returns {Promise<object | null>} Object with wallet details or null if not found.
 */
async function getUserWalletDetails(userId) {
    userId = String(userId);
    const query = `
        SELECT
            external_withdrawal_address,
            linked_at,
            last_used_at,
            referral_code,
            referred_by_user_id,
            referral_count,
            total_wagered,
            last_milestone_paid_lamports,
            created_at
        FROM wallets
        WHERE user_id = $1
    `;
    try {
        const res = await queryDatabase(query, [userId]);
        if (res.rows.length > 0) {
            const details = res.rows[0];
            // Ensure numeric types are correct
            details.total_wagered = BigInt(details.total_wagered || '0');
            details.last_milestone_paid_lamports = BigInt(details.last_milestone_paid_lamports || '0');
            details.referral_count = parseInt(details.referral_count || '0', 10);
            return details;
        }
        return null; // User not found
    } catch (err) {
        console.error(`[DB getUserWalletDetails] Error fetching details for user ${userId}:`, err.message);
        return null;
    }
}

/**
 * Gets the linked external withdrawal address for a user, checking cache first.
 * @param {string} userId
 * @returns {Promise<string | undefined>} The address string or undefined if not set/found.
 */
async function getLinkedWallet(userId) {
    userId = String(userId);
    const cached = getWalletCache(userId);
    if (cached?.withdrawalAddress) {
        return cached.withdrawalAddress;
    }

    try {
        const res = await queryDatabase('SELECT external_withdrawal_address, referral_code FROM wallets WHERE user_id = $1', [userId]);
        const details = res.rows[0];
        if (details) {
            // Update cache with potentially fresh data (address + ref code)
            updateWalletCache(userId, { withdrawalAddress: details.external_withdrawal_address, referralCode: details.referral_code });
            return details.external_withdrawal_address;
        }
        return undefined; // User found but address not set
    } catch (err) {
        console.error(`[DB getLinkedWallet] Error fetching withdrawal wallet for user ${userId}:`, err.message);
        return undefined; // Error occurred
    }
}

/**
 * Finds a user by their referral code.
 * @param {string} refCode The referral code (e.g., 'ref_abcdef12').
 * @returns {Promise<{user_id: string} | null>} User ID object or null if not found/invalid.
 */
async function getUserByReferralCode(refCode) {
    // Basic validation of format
    if (!refCode || typeof refCode !== 'string' || !refCode.startsWith('ref_') || refCode.length < 5) {
        return null;
    }
    try {
        const result = await queryDatabase('SELECT user_id FROM wallets WHERE referral_code = $1', [refCode]);
        return result.rows[0] || null;
    } catch (err) {
        console.error(`[DB getUserByReferralCode] Error finding user for code ${refCode}:`, err);
        return null;
    }
}

/**
 * Links a referee to a referrer if the referee doesn't already have a referrer.
 * Should be called within a transaction, typically during first deposit processing.
 * @param {string} refereeUserId The ID of the user being referred.
 * @param {string} referrerUserId The ID of the user who referred them.
 * @param {pg.PoolClient} client The active database client.
 * @returns {Promise<boolean>} True if the link was successfully made or already existed correctly, false on error or if already referred by someone else.
 */
async function linkReferral(refereeUserId, referrerUserId, client) {
    refereeUserId = String(refereeUserId);
    referrerUserId = String(referrerUserId);

    if (refereeUserId === referrerUserId) {
        console.warn(`[DB linkReferral] User ${refereeUserId} attempted to refer themselves.`);
        return false; // Cannot refer self
    }

    try {
        // Check current status
        const checkRes = await queryDatabase('SELECT referred_by_user_id FROM wallets WHERE user_id = $1 FOR UPDATE', [refereeUserId], client);
        if (checkRes.rowCount === 0) {
             console.error(`[DB linkReferral] Referee user ${refereeUserId} not found.`);
             return false; // Referee must exist
        }
        const currentReferrer = checkRes.rows[0].referred_by_user_id;

        if (currentReferrer === referrerUserId) {
            return true; // Already linked correctly
        } else if (currentReferrer) {
            console.warn(`[DB linkReferral] User ${refereeUserId} already referred by ${currentReferrer}. Cannot link to ${referrerUserId}.`);
            return false; // Already referred by someone else
        }

        // Link the referral
        const updateRes = await queryDatabase(
            'UPDATE wallets SET referred_by_user_id = $1 WHERE user_id = $2 AND referred_by_user_id IS NULL',
            [referrerUserId, refereeUserId],
            client
        );

        if (updateRes.rowCount > 0) {
             console.log(`[DB linkReferral] Successfully linked referee ${refereeUserId} to referrer ${referrerUserId}.`);
             // Now increment the referrer's count
             await queryDatabase('UPDATE wallets SET referral_count = referral_count + 1 WHERE user_id = $1', [referrerUserId], client);
             return true;
        } else {
             // This could happen if the user was referred between the SELECT FOR UPDATE and the UPDATE query (race condition, though unlikely with row lock)
             console.warn(`[DB linkReferral] Failed to link referral for ${refereeUserId} - likely already referred.`);
             return false;
        }
    } catch (err) {
        console.error(`[DB linkReferral] Error linking referee ${refereeUserId} to ${referrerUserId}:`, err);
        return false; // Let transaction rollback handle cleanup
    }
}


/**
 * Updates a user's total wagered amount and potentially the last milestone paid FOR their referrer.
 * Should be called within a transaction after a bet is completed.
 * @param {string} userId The ID of the user who wagered.
 * @param {bigint} wageredAmountLamports The amount wagered in this bet.
 * @param {pg.PoolClient} client The active database client.
 * @param {bigint | null} [newLastMilestonePaidLamports=null] If a milestone was just paid for this user's referrer, update the threshold.
 * @returns {Promise<boolean>} True on success, false on failure.
 */
async function updateUserWagerStats(userId, wageredAmountLamports, client, newLastMilestonePaidLamports = null) {
    userId = String(userId);
    const wageredAmount = BigInt(wageredAmountLamports);

    if (wageredAmount <= 0n) return true; // No change needed

    let query;
    let values;

    if (newLastMilestonePaidLamports !== null && BigInt(newLastMilestonePaidLamports) >= 0n) {
        const lastMilestonePaid = BigInt(newLastMilestonePaidLamports);
        query = `
            UPDATE wallets
            SET total_wagered = total_wagered + $2,
                last_milestone_paid_lamports = $3,
                last_used_at = NOW()
            WHERE user_id = $1;
        `;
        values = [userId, wageredAmount, lastMilestonePaid];
    } else {
        query = `
            UPDATE wallets
            SET total_wagered = total_wagered + $2,
                last_used_at = NOW()
            WHERE user_id = $1;
        `;
        values = [userId, wageredAmount];
    }

    try {
        const res = await queryDatabase(query, values, client);
        if (res.rowCount === 0) {
            console.warn(`[DB updateUserWagerStats] Attempted to update wager stats for non-existent user: ${userId}`);
            return false;
        }
        // console.log(`[DB updateUserWagerStats] Updated wager stats for user ${userId}. Added: ${wageredAmount}. New Milestone: ${newLastMilestonePaidLamports}`);
        return true;
    } catch (err) {
        console.error(`[DB updateUserWagerStats] Error updating wager stats for user ${userId}:`, err.message);
        return false; // Let transaction rollback handle cleanup
    }
}

// --- Balance Operations ---

/**
 * Gets the current internal balance for a user. Creates balance record if needed.
 * Does NOT require an external transaction, but ensures user/balance records exist.
 * @param {string} userId
 * @returns {Promise<bigint>} Current balance in lamports, or 0n if user/balance record creation fails.
 */
async function getUserBalance(userId) {
    userId = String(userId);
    let client;
    try {
        client = await pool.connect();
        await client.query('BEGIN');

        // Ensure user exists (locks rows for safety, though primarily for balance check)
        await ensureUserExists(userId, client);

        // Fetch balance
        const balanceRes = await queryDatabase('SELECT balance_lamports FROM user_balances WHERE user_id = $1', [userId], client);

        await client.query('COMMIT'); // Commit ensuring user/balance exists

        // If somehow still no balance row after ensureUserExists (shouldn't happen), return 0
        return BigInt(balanceRes.rows[0]?.balance_lamports || '0');

    } catch (err) {
        console.error(`[DB GetBalance] Error fetching/ensuring balance for user ${userId}:`, err.message);
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { /* Ignore */ } }
        return 0n; // Return 0 on error
    } finally {
        if (client) client.release();
    }
}

/**
 * Atomically updates a user's balance and records the change in the ledger.
 * MUST be called within a DB transaction.
 * @param {pg.PoolClient} client - The active database client connection.
 * @param {string} userId
 * @param {bigint} changeAmountLamports - Positive for credit, negative for debit.
 * @param {string} transactionType - Type string for the ledger (e.g., 'deposit', 'bet_placed').
 * @param {object} [relatedIds={}] Optional related IDs { betId, depositId, withdrawalId, refPayoutId }.
 * @param {string|null} [notes=null] Optional notes for the ledger entry.
 * @returns {Promise<{success: boolean, newBalance?: bigint, error?: string}>}
 */
async function updateUserBalanceAndLedger(client, userId, changeAmountLamports, transactionType, relatedIds = {}, notes = null) {
    userId = String(userId);
    const changeAmount = BigInt(changeAmountLamports);
    const logPrefix = `[UpdateBalance User ${userId}]`;

    // Ensure related IDs are null if not provided or invalid
    const relatedBetId = Number.isInteger(relatedIds?.betId) ? relatedIds.betId : null;
    const relatedDepositId = Number.isInteger(relatedIds?.depositId) ? relatedIds.depositId : null;
    const relatedWithdrawalId = Number.isInteger(relatedIds?.withdrawalId) ? relatedIds.withdrawalId : null;
    const relatedRefPayoutId = Number.isInteger(relatedIds?.refPayoutId) ? relatedIds.refPayoutId : null;


    try {
        // 1. Get current balance and lock row
        const balanceRes = await queryDatabase(
            'SELECT balance_lamports FROM user_balances WHERE user_id = $1 FOR UPDATE',
            [userId],
            client
        );

        if (balanceRes.rowCount === 0) {
            // This *shouldn't* happen if ensureUserExists was called appropriately beforehand.
            console.error(`${logPrefix} User balance record not found during update. Cannot proceed.`);
            return { success: false, error: 'User balance record unexpectedly missing.' };
        }

        const balanceBefore = BigInt(balanceRes.rows[0].balance_lamports);
        const balanceAfter = balanceBefore + changeAmount;

        // 2. Check for insufficient funds only if debiting
        if (changeAmount < 0n && balanceAfter < 0n) {
            const needed = -changeAmount;
            console.warn(`${logPrefix} Insufficient balance. Current: ${balanceBefore}, Needed: ${needed}, Type: ${transactionType}`);
            return { success: false, error: 'Insufficient balance' };
        }

        // 3. Update balance
        const updateRes = await queryDatabase(
            'UPDATE user_balances SET balance_lamports = $1, updated_at = NOW() WHERE user_id = $2',
            [balanceAfter, userId],
            client
        );
        if (updateRes.rowCount === 0) {
            // Should not happen if FOR UPDATE lock succeeded
            console.error(`${logPrefix} Failed to update balance row after lock? User ${userId}`);
            return { success: false, error: 'Failed to update balance row' };
        }

        // 4. Insert into ledger
        const ledgerQuery = `
            INSERT INTO ledger (
                user_id, transaction_type, amount_lamports, balance_before, balance_after,
                related_bet_id, related_deposit_id, related_withdrawal_id, related_ref_payout_id, notes, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
        `;
        await queryDatabase(ledgerQuery, [
            userId, transactionType, changeAmount, balanceBefore, balanceAfter,
            relatedBetId, relatedDepositId, relatedWithdrawalId, relatedRefPayoutId, notes
        ], client);

        // console.log(`${logPrefix} Balance updated: ${balanceBefore} -> ${balanceAfter}. Type: ${transactionType}`); // Debug log
        return { success: true, newBalance: balanceAfter };

    } catch (err) {
        console.error(`${logPrefix} Error updating balance/ledger (Type: ${transactionType}):`, err.message, err.code);
        // Don't rollback here, let the calling function handle transaction rollback
        return { success: false, error: `Database error during balance update (${err.code})` };
    }
}


// --- Deposit Operations ---

/**
 * Creates a new unique deposit address record for a user.
 * Does NOT require an external transaction.
 * @param {string} userId
 * @param {string} depositAddress - The generated unique address.
 * @param {string} derivationPath - The path used to derive the address.
 * @param {Date} expiresAt - Expiry timestamp for the address.
 * @returns {Promise<{success: boolean, id?: number, error?: string}>}
 */
async function createDepositAddressRecord(userId, depositAddress, derivationPath, expiresAt) {
    const query = `
        INSERT INTO deposit_addresses (user_id, deposit_address, derivation_path, expires_at, status, created_at)
        VALUES ($1, $2, $3, $4, 'pending', NOW())
        RETURNING id;
    `;
    try {
        const res = await queryDatabase(query, [String(userId), depositAddress, derivationPath, expiresAt]);
        if (res.rowCount > 0) {
            const depositAddressId = res.rows[0].id;
            // Add to cache immediately
            addActiveDepositAddressCache(depositAddress, userId, expiresAt.getTime());
            return { success: true, id: depositAddressId };
        } else {
            return { success: false, error: 'Failed to insert deposit address record (no ID returned).' };
        }
    } catch (err) {
        console.error(`[DB CreateDepositAddr] Error for user ${userId} Addr: ${depositAddress.slice(0,6)}..:`, err.message, err.code);
        if (err.code === '23505' && err.constraint === 'deposit_addresses_deposit_address_key') {
            console.error(`[DB CreateDepositAddr] CRITICAL - Deposit address collision: ${depositAddress}`);
            return { success: false, error: 'Deposit address collision detected.' };
        }
        return { success: false, error: `Database error creating deposit address (${err.code || 'N/A'})` };
    }
}

/**
 * Finds user ID and status associated with a deposit address. Checks cache first.
 * @param {string} depositAddress
 * @returns {Promise<{userId: string, status: string, id: number, expiresAt: Date} | null>}
 */
async function findDepositAddressInfo(depositAddress) {
    const cached = getActiveDepositAddressCache(depositAddress);
    const now = Date.now();

    // If cached and not expired, we still need the latest *status* from DB
    if (cached && now < cached.expiresAt) {
        try {
            const statusRes = await queryDatabase(
                'SELECT status, id, expires_at FROM deposit_addresses WHERE deposit_address = $1',
                [depositAddress]
            );
            if (statusRes.rowCount > 0) {
                const dbInfo = statusRes.rows[0];
                // Ensure cache expiry matches DB expiry if needed
                const dbExpiresAt = new Date(dbInfo.expires_at).getTime();
                if (cached.expiresAt !== dbExpiresAt) {
                    console.warn(`[DB FindDepositAddr] Cache expiry mismatch for ${depositAddress}. Updating cache.`);
                    addActiveDepositAddressCache(depositAddress, cached.userId, dbExpiresAt);
                }
                return { userId: cached.userId, status: dbInfo.status, id: dbInfo.id, expiresAt: new Date(dbInfo.expires_at) };
            } else {
                // Not found in DB, remove from cache
                removeActiveDepositAddressCache(depositAddress);
                console.warn(`[DB FindDepositAddr] Address ${depositAddress} was cached but not found in DB.`);
                return null;
            }
        } catch (err) {
            console.error(`[DB FindDepositAddr] Error fetching status for cached addr ${depositAddress}:`, err.message);
            return null; // Error fetching status
        }
    } else if (cached) {
        removeActiveDepositAddressCache(depositAddress); // Expired cache entry
    }

    // Not in valid cache, query DB
    try {
        const res = await queryDatabase(
            'SELECT user_id, status, id, expires_at FROM deposit_addresses WHERE deposit_address = $1',
            [depositAddress]
        );
        if (res.rowCount > 0) {
            const data = res.rows[0];
            const expiresAtDate = new Date(data.expires_at);
            // Add to cache only if status is pending and not expired
            if (data.status === 'pending' && now < expiresAtDate.getTime()) {
                addActiveDepositAddressCache(depositAddress, data.user_id, expiresAtDate.getTime());
            }
            return { userId: data.user_id, status: data.status, id: data.id, expiresAt: expiresAtDate };
        }
        return null; // Not found in DB
    } catch (err) {
        console.error(`[DB FindDepositAddr] Error finding info for addr ${depositAddress}:`, err.message);
        return null;
    }
}

/**
 * Marks a deposit address as used. MUST be called within a DB transaction.
 * @param {pg.PoolClient} client The active database client.
 * @param {number} depositAddressId The ID of the deposit address record.
 * @returns {Promise<boolean>} True if the status was updated from 'pending' to 'used'.
 */
async function markDepositAddressUsed(client, depositAddressId) {
    try {
        const res = await queryDatabase(
            `UPDATE deposit_addresses SET status = 'used' WHERE id = $1 AND status = 'pending'`,
            [depositAddressId],
            client
        );
        if (res.rowCount > 0) {
            // Invalidate cache entry for this address
             const addressRes = await queryDatabase('SELECT deposit_address FROM deposit_addresses WHERE id = $1', [depositAddressId], client);
             if (addressRes.rowCount > 0) {
                removeActiveDepositAddressCache(addressRes.rows[0].deposit_address);
             }
            return true;
        } else {
            console.warn(`[DB MarkDepositUsed] Failed to mark address ID ${depositAddressId} as used (status might not be 'pending').`);
            return false;
        }
    } catch (err) {
        console.error(`[DB MarkDepositUsed] Error updating deposit address ID ${depositAddressId}:`, err.message);
        return false; // Let calling transaction handle rollback
    }
}

/**
 * Records a confirmed deposit transaction. MUST be called within a DB transaction.
 * @param {pg.PoolClient} client The active database client.
 * @param {string} userId
 * @param {number} depositAddressId
 * @param {string} txSignature The Solana transaction signature.
 * @param {bigint} amountLamports The amount deposited.
 * @returns {Promise<{success: boolean, depositId?: number, error?: string}>}
 */
async function recordConfirmedDeposit(client, userId, depositAddressId, txSignature, amountLamports) {
    const query = `
        INSERT INTO deposits (user_id, deposit_address_id, tx_signature, amount_lamports, status, created_at, processed_at)
        VALUES ($1, $2, $3, $4, 'confirmed', NOW(), NOW())
        RETURNING id;
    `;
    try {
        const res = await queryDatabase(query, [String(userId), depositAddressId, txSignature, BigInt(amountLamports)], client);
        if (res.rowCount > 0) {
            return { success: true, depositId: res.rows[0].id };
        } else {
            console.error(`[DB RecordDeposit] Failed to insert deposit record for user ${userId}, TX ${txSignature} (no ID returned).`);
            return { success: false, error: 'Failed to insert deposit record' };
        }
    } catch (err) {
        console.error(`[DB RecordDeposit] Error inserting deposit record for user ${userId}, TX ${txSignature}:`, err.message, err.code);
        if (err.code === '23505' && err.constraint === 'deposits_tx_signature_key') {
            console.error(`[DB RecordDeposit] Deposit TX ${txSignature} already processed (unique constraint violation).`);
            // Still treat as failure to prevent double crediting upstream
            return { success: false, error: 'Deposit transaction already processed.' };
        }
        // Let calling transaction handle rollback
        return { success: false, error: `Database error recording deposit (${err.code || 'N/A'})` };
    }
}


// --- Withdrawal Operations ---

/**
 * Creates a pending withdrawal request. Does NOT require an external transaction.
 * @param {string} userId
 * @param {bigint} requestedAmountLamports Amount user asked for (before fee).
 * @param {bigint} feeLamports Fee applied to this withdrawal.
 * @param {string} recipientAddress The Solana address to send to.
 * @returns {Promise<{success: boolean, withdrawalId?: number, error?: string}>}
 */
async function createWithdrawalRequest(userId, requestedAmountLamports, feeLamports, recipientAddress) {
    const requestedAmount = BigInt(requestedAmountLamports);
    const fee = BigInt(feeLamports);
    // The amount actually sent is the requested amount. The fee is deducted from the user's balance separately.
    const finalSendAmount = requestedAmount;

    const query = `
        INSERT INTO withdrawals (user_id, requested_amount_lamports, fee_lamports, final_send_amount_lamports, recipient_address, status, created_at)
        VALUES ($1, $2, $3, $4, $5, 'pending', NOW())
        RETURNING id;
    `;
    try {
        const res = await queryDatabase(query, [String(userId), requestedAmount, fee, finalSendAmount, recipientAddress]);
        if (res.rowCount > 0) {
            return { success: true, withdrawalId: res.rows[0].id };
        } else {
            return { success: false, error: 'Failed to insert withdrawal request (no ID returned).' };
        }
    } catch (err) {
        console.error(`[DB CreateWithdrawal] Error for user ${userId}:`, err.message, err.code);
        return { success: false, error: `Database error creating withdrawal (${err.code || 'N/A'})` };
    }
}

/**
 * Updates withdrawal status, signature, error message, timestamps.
 * Can be called within a transaction by passing a client, or standalone using the pool.
 * @param {number} withdrawalId The ID of the withdrawal record.
 * @param {'processing' | 'completed' | 'failed'} status The new status.
 * @param {pg.PoolClient | null} client Optional DB client if within a transaction.
 * @param {string | null} [signature=null] The payout transaction signature (required for 'completed').
 * @param {string | null} [errorMessage=null] The error message (required for 'failed').
 * @returns {Promise<boolean>} True on successful update, false otherwise.
 */
async function updateWithdrawalStatus(withdrawalId, status, client = null, signature = null, errorMessage = null) {
    const db = client || pool; // Use client if provided, else pool directly
    const logPrefix = `[UpdateWithdrawal ID:${withdrawalId}]`;

    let query = `UPDATE withdrawals SET status = $1`;
    const values = [status];
    let valueCounter = 2;

    switch (status) {
        case 'processing':
            query += `, processed_at = NOW(), completed_at = NULL, error_message = NULL`;
            break;
        case 'completed':
            if (!signature) {
                console.error(`${logPrefix} Signature required for 'completed' status.`);
                return false;
            }
            query += `, payout_tx_signature = $${valueCounter++}, completed_at = NOW(), error_message = NULL, processed_at = COALESCE(processed_at, NOW())`;
            values.push(signature);
            break;
        case 'failed':
             // Ensure errorMessage is not overly long for DB column
            const safeErrorMessage = (errorMessage || 'Unknown error').substring(0, 500);
            query += `, error_message = $${valueCounter++}, completed_at = NULL, payout_tx_signature = NULL, processed_at = COALESCE(processed_at, NOW())`;
            values.push(safeErrorMessage);
            break;
        default:
            console.error(`${logPrefix} Invalid status "${status}" provided.`);
            return false;
    }

    query += ` WHERE id = $${valueCounter++} RETURNING id;`;
    values.push(withdrawalId);

    try {
        const res = await queryDatabase(query, values, db);
        if (res.rowCount === 0) {
             console.warn(`${logPrefix} Failed to update status to ${status}. Record not found or already in target state?`);
             return false; // Indicate no row was updated
        }
        return true; // Success
    } catch (err) {
        console.error(`${logPrefix} Error updating status to ${status}:`, err.message, err.code);
        if (err.code === '23505' && err.constraint === 'withdrawals_payout_tx_signature_key') {
            console.error(`${logPrefix} CRITICAL - Withdrawal TX Signature ${signature?.slice(0, 10)}... unique constraint violation.`);
        }
        return false; // Error occurred
    }
}

/** Fetches withdrawal details for processing. */
async function getWithdrawalDetails(withdrawalId) {
    try {
        const res = await queryDatabase('SELECT * FROM withdrawals WHERE id = $1', [withdrawalId]);
        if (res.rowCount === 0) return null;
        const details = res.rows[0];
        // Ensure numeric types are correct
        details.requested_amount_lamports = BigInt(details.requested_amount_lamports || '0');
        details.fee_lamports = BigInt(details.fee_lamports || '0');
        details.final_send_amount_lamports = BigInt(details.final_send_amount_lamports || '0');
        return details;
    } catch (err) {
        console.error(`[DB GetWithdrawal] Error fetching withdrawal ${withdrawalId}:`, err.message);
        return null;
    }
}


// --- Bet Operations ---

/**
 * Creates a record for a bet placed using internal balance.
 * MUST be called within a DB transaction that also deducts the balance.
 * @param {pg.PoolClient} client The active database client.
 * @param {string} userId
 * @param {string} chatId
 * @param {string} gameType
 * @param {object} betDetails - Game specific choices/bets (JSONB).
 * @param {bigint} wagerAmountLamports - Amount deducted from internal balance.
 * @param {number} [priority=0] Bet priority.
 * @returns {Promise<{success: boolean, betId?: number, error?: string}>}
 */
async function createBetRecord(client, userId, chatId, gameType, betDetails, wagerAmountLamports, priority = 0) {
    // Basic validation
    if (BigInt(wagerAmountLamports) <= 0n) {
        return { success: false, error: 'Wager amount must be positive.' };
    }

    const query = `
        INSERT INTO bets (user_id, chat_id, game_type, bet_details, wager_amount_lamports, status, created_at, priority)
        VALUES ($1, $2, $3, $4, $5, 'active', NOW(), $6)
        RETURNING id;
    `;
    try {
        const res = await queryDatabase(query, [String(userId), String(chatId), gameType, betDetails, BigInt(wagerAmountLamports), priority], client);
        if (res.rowCount > 0) {
            return { success: true, betId: res.rows[0].id };
        } else {
            return { success: false, error: 'Failed to insert bet record (no ID returned).' };
        }
    } catch (err) {
        console.error(`[DB CreateBet] Error for user ${userId}, game ${gameType}:`, err.message, err.code);
        // Check constraint violation?
        if (err.constraint === 'wager_positive') {
             return { success: false, error: 'Wager amount must be positive (DB check).' };
        }
        return { success: false, error: `Database error creating bet (${err.code || 'N/A'})` };
    }
}

/**
 * Updates a bet record upon completion of game logic.
 * Sets the bet status and the payout amount (amount credited back to internal balance).
 * MUST be called within a DB transaction that potentially credits the balance.
 * @param {pg.PoolClient} client The active database client.
 * @param {number} betId The ID of the bet to update.
 * @param {'processing_game' | 'completed_win' | 'completed_loss' | 'completed_push' | 'error_game_logic'} status The new status.
 * @param {bigint | null} [payoutAmountLamports=null] Amount credited back (Wager + Win, or Wager for push, 0 for loss/error). Required for completed statuses.
 * @returns {Promise<boolean>} True on successful update, false otherwise.
 */
async function updateBetStatus(client, betId, status, payoutAmountLamports = null) {
    const logPrefix = `[UpdateBetStatus ID:${betId}]`;

    // Validate input based on status
    if (['completed_win', 'completed_push'].includes(status) && (payoutAmountLamports === null || BigInt(payoutAmountLamports) <= 0n)) {
        console.error(`${logPrefix} Invalid payout amount ${payoutAmountLamports} for status ${status}.`);
        return false;
    }
    if (['completed_loss', 'error_game_logic'].includes(status)) {
        payoutAmountLamports = 0n; // Ensure payout is zero for loss/error
    }
    if (status === 'processing_game') {
         payoutAmountLamports = null; // Payout not determined yet
    }

    // Allow transitions from 'active' or 'processing_game' (or potentially 'error')
    const query = `
        UPDATE bets
        SET status = $1,
            payout_amount_lamports = $2,
            processed_at = CASE WHEN $1 LIKE 'completed_%' OR $1 = 'error_game_logic' THEN NOW() ELSE processed_at END
        WHERE id = $3 AND status IN ('active', 'processing_game', 'error_game_logic')
        RETURNING id, status; -- Return old status for logging if needed
    `;
    try {
        const res = await queryDatabase(query, [status, payoutAmountLamports !== null ? BigInt(payoutAmountLamports) : null, betId], client);
        if (res.rowCount === 0) {
            // Fetch current status to understand why update failed
            const currentStatusRes = await queryDatabase('SELECT status FROM bets WHERE id = $1', [betId], client);
            const currentStatus = currentStatusRes.rows[0]?.status || 'NOT_FOUND';
            console.warn(`${logPrefix} Failed to update status to ${status}. Current status: ${currentStatus}.`);
            return false;
        }
        // console.log(`${logPrefix} Status updated to ${status}.`); // Debug log
        return true;
    } catch (err) {
        console.error(`${logPrefix} Error updating status to ${status}:`, err.message);
        return false;
    }
}

/**
 * Checks if a specific completed bet ID is the user's first *ever* completed bet.
 * Does NOT require an external transaction.
 * @param {string} userId
 * @param {number} betId The ID of the bet to check.
 * @returns {Promise<boolean>} True if it's the first completed bet, false otherwise.
 */
async function isFirstCompletedBet(userId, betId) {
    userId = String(userId);
    // Find the earliest created bet_id among all completed bets for the user
    const query = `
        SELECT id
        FROM bets
        WHERE user_id = $1 AND status LIKE 'completed_%'
        ORDER BY created_at ASC, id ASC
        LIMIT 1;
    `;
    try {
        const res = await queryDatabase(query, [userId]);
        // Return true only if a first bet exists AND its ID matches the provided betId
        return res.rows.length > 0 && res.rows[0].id === betId;
    } catch (err) {
        console.error(`[DB isFirstCompletedBet] Error checking for user ${userId}, bet ${betId}:`, err.message);
        return false; // Assume not first on error
    }
}

/** Fetches bet details by ID. */
async function getBetDetails(betId) {
    try {
        const res = await queryDatabase('SELECT * FROM bets WHERE id = $1', [betId]);
         if (res.rowCount === 0) return null;
         const details = res.rows[0];
         // Ensure numeric types
         details.wager_amount_lamports = BigInt(details.wager_amount_lamports || '0');
         details.payout_amount_lamports = details.payout_amount_lamports !== null ? BigInt(details.payout_amount_lamports) : null;
         details.priority = parseInt(details.priority || '0', 10);
         return details;
    } catch (err) {
        console.error(`[DB GetBetDetails] Error fetching bet ${betId}:`, err.message);
        return null;
    }
}


// --- Referral Payout Operations ---

/**
 * Records a pending referral payout. Does NOT require an external transaction.
 * @param {string} referrerUserId
 * @param {string} refereeUserId
 * @param {'initial_bet' | 'milestone'} payoutType
 * @param {bigint} payoutAmountLamports
 * @param {number | null} [triggeringBetId=null] Bet ID for 'initial_bet' type.
 * @param {bigint | null} [milestoneReachedLamports=null] Wager threshold for 'milestone' type.
 * @returns {Promise<{success: boolean, payoutId?: number, error?: string}>}
 */
async function recordPendingReferralPayout(referrerUserId, refereeUserId, payoutType, payoutAmountLamports, triggeringBetId = null, milestoneReachedLamports = null) {
    const query = `
        INSERT INTO referral_payouts (
            referrer_user_id, referee_user_id, payout_type, payout_amount_lamports,
            triggering_bet_id, milestone_reached_lamports, status, created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, 'pending', NOW())
        RETURNING id;
    `;
    const values = [
        String(referrerUserId),
        String(refereeUserId),
        payoutType,
        BigInt(payoutAmountLamports),
        Number.isInteger(triggeringBetId) ? triggeringBetId : null,
        milestoneReachedLamports !== null ? BigInt(milestoneReachedLamports) : null
    ];
    try {
        const res = await queryDatabase(query, values);
        if (res.rows.length > 0) {
            const payoutId = res.rows[0].id;
            console.log(`[DB Referral] Recorded pending ${payoutType} payout ID ${payoutId} for referrer ${referrerUserId} (triggered by ${refereeUserId}).`);
            return { success: true, payoutId: payoutId };
        } else {
            console.error("[DB Referral] Failed to insert pending referral payout, no ID returned.");
            return { success: false, error: "Failed to insert pending referral payout." };
        }
    } catch (err) {
        // Check for duplicate milestone payouts
         if (err.code === '23505' && err.constraint === 'idx_refpayout_milestone_check') { // Assuming index name, adjust if needed
             console.warn(`[DB Referral] Duplicate milestone payout attempt for referrer ${referrerUserId}, referee ${refereeUserId}, milestone ${milestoneReachedLamports}. Ignoring.`);
            return { success: false, error: 'Duplicate milestone payout attempt.' };
         }
        console.error(`[DB Referral] Error recording pending ${payoutType} payout for referrer ${referrerUserId} (triggered by ${refereeUserId}):`, err.message);
        return { success: false, error: `Database error recording referral payout (${err.code || 'N/A'})` };
    }
}

/**
 * Updates referral payout status, signature, error message, timestamps.
 * Can be called within a transaction by passing a client, or standalone using the pool.
 * @param {number} payoutId The ID of the referral_payouts record.
 * @param {'processing' | 'paid' | 'failed'} status The new status.
 * @param {pg.PoolClient | null} client Optional DB client if within a transaction.
 * @param {string | null} [signature=null] The payout transaction signature (required for 'paid').
 * @param {string | null} [errorMessage=null] The error message (required for 'failed').
 * @returns {Promise<boolean>} True on successful update, false otherwise.
 */
async function updateReferralPayoutStatus(payoutId, status, client = null, signature = null, errorMessage = null) {
    const db = client || pool; // Use client if provided, else pool directly
    const logPrefix = `[UpdateRefPayout ID:${payoutId}]`;

    let query = `UPDATE referral_payouts SET status = $1`;
    const values = [status];
    let valueCounter = 2;

    switch (status) {
        case 'processing':
            query += `, processed_at = NOW(), paid_at = NULL, error_message = NULL`;
            break;
        case 'paid':
            if (!signature) {
                console.error(`${logPrefix} Signature required for 'paid' status.`);
                return false;
            }
            query += `, payout_tx_signature = $${valueCounter++}, paid_at = NOW(), error_message = NULL, processed_at = COALESCE(processed_at, NOW())`;
            values.push(signature);
            break;
        case 'failed':
             const safeErrorMessage = (errorMessage || 'Unknown error').substring(0, 500);
            query += `, error_message = $${valueCounter++}, paid_at = NULL, payout_tx_signature = NULL, processed_at = COALESCE(processed_at, NOW())`;
            values.push(safeErrorMessage);
            break;
        default:
            console.error(`${logPrefix} Invalid status "${status}" provided.`);
            return false;
    }

    query += ` WHERE id = $${valueCounter++} RETURNING id;`;
    values.push(payoutId);

    try {
        const res = await queryDatabase(query, values, db);
        if (res.rowCount === 0) {
            console.warn(`${logPrefix} Failed to update status to ${status}. Record not found or already in target state?`);
            return false; // Indicate no row was updated
        }
        return true; // Success
    } catch (err) {
        console.error(`${logPrefix} Error updating status to ${status}:`, err.message, err.code);
        if (err.code === '23505' && err.constraint === 'referral_payouts_payout_tx_signature_key') {
             console.error(`${logPrefix} CRITICAL - Referral Payout TX Signature ${signature?.slice(0, 10)}... unique constraint violation.`);
        }
        return false; // Error occurred
    }
}

/** Fetches referral payout details for processing. */
async function getReferralPayoutDetails(payoutId) {
     try {
        const res = await queryDatabase('SELECT * FROM referral_payouts WHERE id = $1', [payoutId]);
        if (res.rowCount === 0) return null;
        const details = res.rows[0];
        // Ensure numeric types
        details.payout_amount_lamports = BigInt(details.payout_amount_lamports || '0');
        details.milestone_reached_lamports = details.milestone_reached_lamports !== null ? BigInt(details.milestone_reached_lamports) : null;
        details.triggering_bet_id = details.triggering_bet_id !== null ? parseInt(details.triggering_bet_id, 10) : null;
        return details;
    } catch (err) {
        console.error(`[DB GetRefPayout] Error fetching payout ${payoutId}:`, err.message);
        return null;
    }
}

/**
 * Calculates the total earnings paid out to a specific referrer.
 * @param {string} userId The referrer's user ID.
 * @returns {Promise<bigint>} Total lamports earned and paid.
 */
async function getTotalReferralEarnings(userId) {
    userId = String(userId);
    try {
        const result = await queryDatabase(
            `SELECT COALESCE(SUM(payout_amount_lamports), 0) AS total_earnings
             FROM referral_payouts
             WHERE referrer_user_id = $1 AND status = 'paid'`,
            [userId]
        );
        return BigInt(result.rows[0]?.total_earnings || '0');
    } catch (err) {
        console.error(`[DB getTotalReferralEarnings] Error calculating earnings for user ${userId}:`, err);
        return 0n; // Return 0 on error
    }
}

// --- End of Part 2 ---
// index.js - Part 3: Solana Utilities & Telegram Helpers
// --- VERSION: 3.1.0 ---

// --- Solana Utilities ---

/**
 * Derives a unique Solana keypair for deposits based on user ID and an index.
 * Uses BIP39 seed phrase and BIP44 path (m/44'/501'/account'/change').
 * Returns the public key and the derived seed necessary to reconstruct the private key for sweeping.
 * @param {string} userId The user's unique ID.
 * @param {number} addressIndex A unique index for this user's addresses (e.g., 0, 1, 2...).
 * @returns {Promise<{publicKey: PublicKey, derivedSeed: Uint8Array, derivationPath: string} | null>} Derived public key, seed, and path, or null on error.
 */
async function generateUniqueDepositAddress(userId, addressIndex) {
    const logPrefix = `[Address Gen User ${userId}]`;
    try {
        if (typeof userId !== 'string' || userId.length === 0 || typeof addressIndex !== 'number' || addressIndex < 0 || !Number.isInteger(addressIndex)) {
            console.error(`${logPrefix} Invalid userId or addressIndex`, { userId, addressIndex });
            return null;
        }

        // 1. Get the master seed from the mnemonic phrase
        const seedPhrase = process.env.DEPOSIT_MASTER_SEED_PHRASE;
        if (!seedPhrase) {
            console.error(`${logPrefix} DEPOSIT_MASTER_SEED_PHRASE is not set.`);
            return null;
        }
        const masterSeed = await bip39.mnemonicToSeed(seedPhrase); // Generates a 64-byte seed

        // 2. Derive the user-specific account path component
        // Use SHA256 hash of user ID, take first 4 bytes as uint32 for account'
        // This provides a deterministic but unique-per-user hardened path component.
        const hash = crypto.createHash('sha256').update(userId).digest();
        const accountIndex = hash.readUInt32BE(0);
        // Ensure it's treated as hardened (BIP32 uses indices >= 2^31 for hardened)
        const hardenedAccountIndex = accountIndex | 0x80000000; // Add hardening bit

        // 3. Construct the full BIP44 derivation path for Solana
        // m / purpose' / coin_type' / account' / change' / address_index'
        // We use account' for user, change'=0 (external), address_index' for the specific address index
        // Using hardened paths for account and index for better security separation.
        const derivationPath = `m/44'/501'/${hardenedAccountIndex >>> 0}'/0'/${addressIndex}'`; // Use >>> 0 to treat as unsigned 32-bit for path segment

        // 4. Derive the private key seed using the path
        const derivedSeedBytes = derivePath(derivationPath, masterSeed.toString('hex')).key; // derivePath expects hex seed

        // 5. Generate the Solana Keypair from the derived seed
        // nacl.sign.keyPair.fromSeed requires a 32-byte seed. The derived key is 64 bytes (priv+pub), use first 32 bytes as seed.
        const keyPair = nacl.sign.keyPair.fromSeed(derivedSeedBytes.slice(0, 32));
        const publicKey = new PublicKey(keyPair.publicKey);

        // 6. Return public key, path, and the *derived seed* (first 32 bytes) needed to reconstruct the private key later for sweeping
        return {
            publicKey: publicKey,
            derivedSeed: derivedSeedBytes.slice(0, 32), // Important for reconstructing Keypair to sign later
            derivationPath: derivationPath
        };

    } catch (error) {
        console.error(`${logPrefix} Error deriving address for index ${addressIndex}: ${error.message}`);
        console.error(error.stack); // Log stack for debugging derivation errors
        return null;
    }
}

/**
 * Checks if a Solana error is likely retryable (network, rate limit, temporary node issues).
 * @param {any} error The error object.
 * @returns {boolean} True if the error is likely retryable.
 */
function isRetryableSolanaError(error) {
    if (!error) return false;

    const message = String(error.message || '').toLowerCase();
    const code = error?.code || error?.cause?.code; // Check nested code property
    const errorCode = error?.errorCode; // Sometimes used by libraries
    const status = error?.response?.status || error?.statusCode || error?.status; // HTTP status

    // Common retryable keywords/phrases
    const retryableMessages = [
        'timeout', 'timed out', 'econnreset', 'esockettimedout', 'network error',
        'fetch', 'socket hang up', 'connection terminated', 'econnrefused', 'failed to fetch',
        'getaddrinfo enotfound', 'connection refused', 'connection reset by peer', 'etimedout',
        'transaction simulation failed', 'failed to simulate transaction',
        'blockhash not found', 'slot leader does not match', 'node is behind',
        'transaction was not confirmed', 'block not available', 'block cleanout',
        'sending transaction', 'connection closed', 'load balancer error',
        'backend unhealthy', 'overloaded', 'proxy internal error'
    ];

    if (retryableMessages.some(m => message.includes(m))) return true;

    // HTTP Status Codes
    if ([429, 500, 502, 503, 504].includes(Number(status))) return true;

    // Error Codes (can be library-specific or RPC-specific)
    const retryableCodes = [
        'ETIMEDOUT', 'ECONNRESET', 'ENETUNREACH', 'EAI_AGAIN', 'ECONNABORTED',
        'ECONNREFUSED', 'UND_ERR_CONNECT_TIMEOUT', 'UND_ERR_HEADERS_TIMEOUT',
        'UND_ERR_BODY_TIMEOUT', 'FETCH_TIMEOUT', 'FETCH_ERROR', '503', '429', '-32003', '-32005' // Solana RPC temporary errors
    ];
    if (retryableCodes.includes(code) || retryableCodes.includes(errorCode)) return true;

    // Specific error types or reasons
    if (error.reason === 'rpc_error' || error.reason === 'network_error') return true;

    // Handle SendTransactionError specifically (check if underlying cause is retryable)
    if (error instanceof SendTransactionError && error.cause) {
        return isRetryableSolanaError(error.cause);
    }

    return false;
}

/**
 * Sends SOL from a designated bot wallet to a recipient.
 * Handles priority fees and uses sendAndConfirmTransaction.
 * @param {PublicKey | string} recipientPublicKey The recipient's address.
 * @param {bigint} amountLamports The amount to send.
 * @param {'withdrawal' | 'referral'} payoutSource Determines which private key to use.
 * @returns {Promise<{success: boolean, signature?: string, error?: string, isRetryable?: boolean}>} Result object.
 */
async function sendSol(recipientPublicKey, amountLamports, payoutSource) {
    const operationId = `sendSol-${payoutSource}-${Date.now().toString().slice(-6)}`;
    let recipientPubKey;
    try {
        recipientPubKey = (typeof recipientPublicKey === 'string') ? new PublicKey(recipientPublicKey) : recipientPublicKey;
        if (!(recipientPubKey instanceof PublicKey)) throw new Error("Invalid recipient public key type");
    } catch (e) {
        const errorMsg = `Invalid recipient address format: "${recipientPublicKey}". Error: ${e.message}`;
        console.error(`[${operationId}] ❌ ERROR: ${errorMsg}`);
        return { success: false, error: errorMsg, isRetryable: false };
    }

    let amountToSend;
    try {
        amountToSend = BigInt(amountLamports);
        if (amountToSend <= 0n) throw new Error('Amount must be positive');
    } catch (e) {
        const errorMsg = `Invalid amountLamports value: '${amountLamports}'. Error: ${e.message}`;
        console.error(`[${operationId}] ❌ ERROR: ${errorMsg}`);
        return { success: false, error: errorMsg, isRetryable: false };
    }

    // Determine which private key to use
    let privateKeyEnvVar;
    let keyTypeForLog;
    if (payoutSource === 'referral' && process.env.REFERRAL_PAYOUT_PRIVATE_KEY) {
        privateKeyEnvVar = 'REFERRAL_PAYOUT_PRIVATE_KEY';
        keyTypeForLog = 'REFERRAL';
    } else {
        // Default to MAIN key for withdrawals or if referral key isn't set
        privateKeyEnvVar = 'MAIN_BOT_PRIVATE_KEY';
        keyTypeForLog = (payoutSource === 'referral') ? 'MAIN (Default for Referral)' : 'MAIN';
    }

    const privateKey = process.env[privateKeyEnvVar];
    if (!privateKey) {
        const errorMsg = `Missing private key env var ${privateKeyEnvVar} for payout source ${payoutSource} (Key Type: ${keyTypeForLog}).`;
        console.error(`[${operationId}] ❌ ERROR: ${errorMsg}`);
        // This is a configuration error, not typically retryable in the short term
        return { success: false, error: errorMsg, isRetryable: false };
    }

    let payerWallet;
    try {
        payerWallet = Keypair.fromSecretKey(bs58.decode(privateKey));
    } catch (e) {
         const errorMsg = `Failed to decode private key ${keyTypeForLog} (${privateKeyEnvVar}): ${e.message}`;
         console.error(`[${operationId}] ❌ ERROR: ${errorMsg}`);
         return { success: false, error: errorMsg, isRetryable: false };
    }

    const amountSOL = Number(amountToSend) / LAMPORTS_PER_SOL;
    console.log(`[${operationId}] Attempting to send ${amountSOL.toFixed(SOL_DECIMALS)} SOL to ${recipientPubKey.toBase58()} using ${keyTypeForLog} key...`);

    try {
        // 1. Get latest blockhash
        const { blockhash, lastValidBlockHeight } = await solanaConnection.getLatestBlockhash(DEPOSIT_CONFIRMATION_LEVEL);
        if (!blockhash || !lastValidBlockHeight) {
            throw new Error('Failed to get valid latest blockhash object.');
        }

        // 2. Calculate Priority Fee (Simplified: using fixed base/max for now)
        // A more dynamic approach would query recent fees: await solanaConnection.getRecentPrioritizationFees()
        const basePriorityFee = parseInt(process.env.PAYOUT_BASE_PRIORITY_FEE_MICROLAMPORTS, 10);
        const maxPriorityFee = parseInt(process.env.PAYOUT_MAX_PRIORITY_FEE_MICROLAMPORTS, 10);
        let priorityFeeMicroLamports = Math.max(1, Math.min(basePriorityFee, maxPriorityFee)); // Ensure at least 1 microLamport
        const computeUnitLimit = parseInt(process.env.PAYOUT_COMPUTE_UNIT_LIMIT, 10);

        // 3. Create Transaction
        const transaction = new Transaction({
            recentBlockhash: blockhash,
            feePayer: payerWallet.publicKey,
        });

        // Add compute budget instructions
        transaction.add(
            ComputeBudgetProgram.setComputeUnitLimit({ units: computeUnitLimit })
        );
        transaction.add(
            ComputeBudgetProgram.setComputeUnitPrice({ microLamports: priorityFeeMicroLamports })
        );

        // Add the transfer instruction
        transaction.add(
            SystemProgram.transfer({
                fromPubkey: payerWallet.publicKey,
                toPubkey: recipientPubKey,
                lamports: amountToSend,
            })
        );

        // 4. Sign and Send Transaction
        const signature = await sendAndConfirmTransaction(
            solanaConnection, // Use the potentially rate-limited connection
            transaction,
            [payerWallet], // Signer
            {
                commitment: DEPOSIT_CONFIRMATION_LEVEL, // Use consistent confirmation level
                skipPreflight: false, // Keep preflight checks enabled
                preflightCommitment: DEPOSIT_CONFIRMATION_LEVEL,
                lastValidBlockHeight: lastValidBlockHeight, // Include for better confirmation
                // maxRetries: 0 // Let the RateLimitedConnection handle retries if configured internally
            }
        );

        console.log(`[${operationId}] SUCCESS! ✅ Sent ${amountSOL.toFixed(SOL_DECIMALS)} SOL to ${recipientPubKey.toBase58()} using ${keyTypeForLog} key. TX: ${signature}`);
        return { success: true, signature: signature };

    } catch (error) {
        const isRetryable = isRetryableSolanaError(error);
        let userFriendlyError = `Send/Confirm error: ${error.message}`;

        // Improve user-friendly messages for common errors
        const errorMsgLower = String(error.message || '').toLowerCase();
        if (errorMsgLower.includes('insufficient lamports') || errorMsgLower.includes('insufficient funds')) {
            userFriendlyError = `Insufficient funds in the ${keyTypeForLog} payout wallet.`;
        } else if (errorMsgLower.includes('blockhash not found') || errorMsgLower.includes('block height exceeded')) {
            userFriendlyError = 'Transaction expired (blockhash invalid).';
        } else if (errorMsgLower.includes('transaction was not confirmed') || errorMsgLower.includes('timed out waiting')) {
            userFriendlyError = `Transaction confirmation timeout. Status unclear, may succeed later.`;
        } else if (isRetryable) {
             userFriendlyError = `Temporary network/RPC error: ${error.message}`;
        }

        console.error(`[${operationId}] ❌ SEND FAILED using ${keyTypeForLog} key. Retryable: ${isRetryable}. Error: ${error.message}`);
        if (error instanceof SendTransactionError && error.logs) {
            console.error(`[${operationId}] Simulation Logs (last 10):`);
            error.logs.slice(-10).forEach(log => console.error(`  -> ${log}`));
        } else if (error.stack) {
            // console.error(`[${operationId}] Stack: ${error.stack}`); // Optionally log full stack
        }

        return { success: false, error: userFriendlyError, isRetryable: isRetryable };
    }
}


/**
 * Analyzes a fetched Solana transaction to find the SOL amount transferred *to* a target address.
 * Handles basic SystemProgram transfers.
 * NOTE: Might need adjustments for complex transactions or different program interactions.
 * @param {ConfirmedTransaction | TransactionResponse | null} tx The fetched transaction object.
 * @param {string} targetAddress The address receiving the funds.
 * @returns {{transferAmount: bigint, payerAddress: string | null}} Amount in lamports and the likely payer.
 */
function analyzeTransactionAmounts(tx, targetAddress) {
    let transferAmount = 0n;
    let payerAddress = null;

    if (!tx || !targetAddress) {
        return { transferAmount: 0n, payerAddress: null };
    }

    try {
        new PublicKey(targetAddress); // Validate target address format
    } catch (e) {
        console.warn(`[analyzeAmounts] Invalid targetAddress format: ${targetAddress}`);
        return { transferAmount: 0n, payerAddress: null };
    }

    // Check for transaction error
    if (tx.meta?.err) {
        // console.log(`[analyzeAmounts] Transaction meta contains error for target ${targetAddress}.`);
        return { transferAmount: 0n, payerAddress: null };
    }

    // --- Method 1: Balance Changes (Most Reliable) ---
    if (tx.meta?.preBalances && tx.meta?.postBalances && tx.transaction?.message) {
        try {
            const accountKeys = tx.transaction.message.accountKeys.map(k => k.toBase58());
            const targetIndex = accountKeys.indexOf(targetAddress);

            if (targetIndex !== -1 &&
                tx.meta.preBalances.length > targetIndex && tx.meta.postBalances.length > targetIndex) {
                const preBalance = BigInt(tx.meta.preBalances[targetIndex]);
                const postBalance = BigInt(tx.meta.postBalances[targetIndex]);
                const balanceChange = postBalance - preBalance;

                if (balanceChange > 0n) {
                    transferAmount = balanceChange;
                    // Try to identify the payer (usually the first signer who had a negative balance change)
                     const signers = tx.transaction.message.accountKeys.slice(0, tx.transaction.message.header.numRequiredSignatures);
                     for (const signer of signers) {
                         const signerIndex = accountKeys.indexOf(signer.toBase58());
                         if (signerIndex !== -1 && signerIndex !== targetIndex &&
                             tx.meta.preBalances.length > signerIndex && tx.meta.postBalances.length > signerIndex) {
                            const preSigner = BigInt(tx.meta.preBalances[signerIndex]);
                            const postSigner = BigInt(tx.meta.postBalances[signerIndex]);
                            if (postSigner < preSigner) {
                                payerAddress = signer.toBase58();
                                break; // Assume first signer with decreased balance is payer
                            }
                         }
                     }
                     // Fallback to first signer if no obvious payer found
                     if (!payerAddress && signers.length > 0) {
                         payerAddress = signers[0].toBase58();
                     }
                }
            }
        } catch (e) {
             console.warn(`[analyzeAmounts] Error analyzing balance changes: ${e.message}`);
             // Fallback to instruction parsing if balance analysis fails
             transferAmount = 0n;
             payerAddress = null;
        }
    }

    // --- Method 2: Instruction Parsing (Fallback / Cross-check) ---
    // Only use if balance change method didn't find a positive transfer
    if (transferAmount === 0n && tx.meta?.logMessages) {
        // Example: Look for SystemProgram transfer logs
        // Logs like "Program log: Instruction: Transfer", "Program log: Source: <payer>", "Program log: Destination: <target>", "Program log: Amount: <lamports>"
        // This requires fragile string parsing and is less reliable than balance changes.
        const transferRegex = /Program log: Instruction: Transfer/i;
        const sysTransferToRegex = /Transfer: src=([1-9A-HJ-NP-Za-km-z]+) dst=([1-9A-HJ-NP-Za-km-z]+) lamports=(\d+)/; // For newer log formats

        let potentialPayer = null;
        let potentialAmount = 0n;

        for (const log of tx.meta.logMessages) {
            // Check newer format first
            const match = log.match(sysTransferToRegex);
            if (match && match[2] === targetAddress) {
                potentialPayer = match[1];
                potentialAmount = BigInt(match[3]);
                 // If found via this regex, prioritize it
                 transferAmount = potentialAmount;
                 payerAddress = potentialPayer;
                 break;
            }

            // Add more sophisticated parsing here if needed for other transfer types or older log formats
        }
    }

    if (transferAmount > 0n) {
       // console.log(`[analyzeAmounts] Analyzed TX for ${targetAddress}: Found ${transferAmount} lamports from ${payerAddress || 'Unknown'}`);
    } else {
       // console.log(`[analyzeAmounts] Analyzed TX for ${targetAddress}: No positive SOL transfer found.`);
    }


    return { transferAmount, payerAddress };
}


// --- Telegram Utilities ---

/**
 * Sends a message via Telegram with rate limiting and error handling.
 * @param {string | number} chatId The chat ID to send to.
 * @param {string} text The message text.
 * @param {TelegramBot.SendMessageOptions} [options={}] Optional parameters for sendMessage.
 * @returns {Promise<TelegramBot.Message | undefined>} The sent message object or undefined on failure.
 */
async function safeSendMessage(chatId, text, options = {}) {
    // Input validation
    if (!chatId || typeof text !== 'string' || text.trim() === '') {
        console.error("[safeSendMessage] Invalid input:", { chatId, text });
        return undefined;
    }

    // Message length check (Telegram limit is 4096 chars)
    const MAX_LENGTH = 4096;
    if (text.length > MAX_LENGTH) {
        console.warn(`[safeSendMessage] Message for chat ${chatId} too long (${text.length} > ${MAX_LENGTH}), truncating.`);
        text = text.substring(0, MAX_LENGTH - 3) + "...";
    }

    // Add default parse_mode if MarkdownV2 is intended but not set
    if (!options.parse_mode && (text.includes('\\') || text.includes('*') || text.includes('_') || text.includes('`'))) {
       // console.warn(`[safeSendMessage] Auto-setting parse_mode to MarkdownV2 for chat ${chatId} due to special characters.`);
       // options.parse_mode = 'MarkdownV2'; // Be careful with auto-setting, ensure escaping is always done
    }

    return telegramSendQueue.add(async () => {
        try {
            // Add a small artificial delay *before* sending, sometimes helps with rapid sends?
            // await new Promise(r => setTimeout(r, 50)); // Optional: test if this helps stability
            const sentMessage = await bot.sendMessage(chatId, text, options);
            return sentMessage;
        } catch (error) {
            console.error(`❌ Telegram send error to chat ${chatId}: Code: ${error.code} | ${error.message}`);
            if (error.response?.body) {
                 console.error(`  -> Response Body: ${JSON.stringify(error.response.body)}`);
            }

            // Handle specific, common errors
            if (error.code === 'ETELEGRAM') {
                if (error.message.includes('400 Bad Request: message is too long')) {
                     console.error(`  -> Message confirmed too long, even after potential truncation.`);
                } else if (error.message.includes('400 Bad Request: can\'t parse entities')) {
                     console.error(`  -> Telegram Parse Error: Ensure proper MarkdownV2 escaping! Text snippet: ${text.substring(0, 100)}...`);
                     // Try sending without parse mode as a fallback? Only if parse mode was likely the issue.
                     // if (options.parse_mode) {
                     //    try {
                     //       console.warn(`  -> Retrying send to ${chatId} without parse_mode.`);
                     //       return await bot.sendMessage(chatId, text, { ...options, parse_mode: undefined });
                     //    } catch (retryError) {
                     //         console.error(`  -> Retry without parse_mode also failed: ${retryError.message}`);
                     //    }
                     // }
                } else if (error.message.includes('403 Forbidden: bot was blocked by the user') || error.message.includes('403 Forbidden: user is deactivated')) {
                    console.warn(`  -> Bot blocked or user deactivated for chat/user ${chatId}. Consider marking user inactive.`);
                    // Implement logic here to potentially deactivate the user in DB
                } else if (error.message.includes('403 Forbidden: bot was kicked')) {
                    console.warn(`  -> Bot kicked from group chat ${chatId}. Consider cleaning up group-related data.`);
                     // Implement cleanup logic
                } else if (error.message.includes('429 Too Many Requests')) {
                    console.warn(`  -> Hit Telegram rate limits sending to ${chatId}. Queue should handle, but may indicate high load.`);
                    // The queue automatically handles this, but persistent warnings might mean interval needs adjustment
                }
            }
            // Consider other errors like 404 chat not found, etc.
            return undefined; // Indicate failure
        }
    });
}


/**
 * Escapes characters for Telegram MarkdownV2 parse mode.
 * @param {string | number | bigint} text Input text or number to escape.
 * @returns {string} Escaped string.
 */
const escapeMarkdownV2 = (text) => {
    const textString = String(text); // Convert numbers/bigints to string
    // Escape all reserved characters: _ * [ ] ( ) ~ ` > # + - = | { } . ! \
    return textString.replace(/([_*\[\]()~`>#+\-=|{}.!\\])/g, '\\$1');
};

/**
 * Escapes characters for HTML contexts.
 * @param {string | number | bigint} text Input text or number to escape.
 * @returns {string} Escaped string.
 */
function escapeHtml(text) {
    const textString = String(text);
    return textString.replace(/&/g, "&amp;")
                     .replace(/</g, "&lt;")
                     .replace(/>/g, "&gt;")
                     .replace(/"/g, "&quot;")
                     .replace(/'/g, "&#039;");
}

/**
 * Gets a display name for a user in a specific chat. Prefers @username, falls back to first name, then User_ID.
 * Escapes the result for MarkdownV2.
 * @param {string | number} chatId The chat ID where the user is.
 * @param {string | number} userId The user ID.
 * @returns {Promise<string>} MarkdownV2 escaped display name.
 */
async function getUserDisplayName(chatId, userId) {
    try {
        // Caching chat member info could be beneficial here if called frequently for the same users
        const member = await bot.getChatMember(chatId, userId);
        const user = member.user;
        let name = '';
        if (user.username) {
            name = `@${user.username}`;
        } else if (user.first_name) {
            name = user.first_name;
            if (user.last_name) {
                name += ` ${user.last_name}`;
            }
        } else {
            name = `User_${String(userId).slice(-4)}`; // Fallback
        }
        return escapeMarkdownV2(name);
    } catch (error) {
        // Handle cases where user is not in chat or other API errors
        // console.warn(`[getUserDisplayName] Error fetching chat member for user ${userId} in chat ${chatId}: ${error.message}`);
        // Fallback to generic name based on ID
        return escapeMarkdownV2(`User_${String(userId).slice(-4)}`);
    }
}


// --- Cache Utilities ---

/** Updates the wallet cache for a user with optional TTL. */
function updateWalletCache(userId, data) {
    userId = String(userId);
    const existing = walletCache.get(userId) || {};
    walletCache.set(userId, { ...existing, ...data, timestamp: Date.now() });
    // Start TTL timer
    setTimeout(() => {
        const current = walletCache.get(userId);
        // Only delete if the timestamp matches the one we set, preventing accidental deletion if updated again quickly
        if (current && current.timestamp === walletCache.get(userId)?.timestamp && Date.now() - current.timestamp >= WALLET_CACHE_TTL_MS) {
            walletCache.delete(userId);
            // console.log(`[Cache] Wallet cache expired for user ${userId}`);
        }
    }, WALLET_CACHE_TTL_MS + 1000); // Add buffer to timeout
}

/** Gets wallet cache data if not expired. */
function getWalletCache(userId) {
    userId = String(userId);
    const entry = walletCache.get(userId);
    if (entry && Date.now() - entry.timestamp < WALLET_CACHE_TTL_MS) {
        return entry;
    }
    if (entry) { // Entry exists but is expired
        walletCache.delete(userId); // Clean up expired entry
    }
    return undefined;
}

/** Adds or updates an active deposit address in the cache. */
function addActiveDepositAddressCache(address, userId, expiresAtTimestamp) {
    activeDepositAddresses.set(address, { userId: String(userId), expiresAt: expiresAtTimestamp });
    // Basic TTL cleanup based on DEPOSIT_ADDR_CACHE_TTL_MS (could also use expiresAtTimestamp)
    setTimeout(() => {
         const current = activeDepositAddresses.get(address);
         if (current && Date.now() >= current.expiresAt) {
             activeDepositAddresses.delete(address);
             // console.log(`[Cache] Deposit address cache expired/removed for ${address}`);
         }
    }, DEPOSIT_ADDR_CACHE_TTL_MS + 1000); // Check slightly after cache TTL
}

/** Gets active deposit address info from cache if not expired. */
function getActiveDepositAddressCache(address) {
    const entry = activeDepositAddresses.get(address);
    if (entry && Date.now() < entry.expiresAt) {
        return entry;
    }
     if (entry) { // Entry exists but is expired according to its own expiry time
        activeDepositAddresses.delete(address); // Clean up expired entry
    }
    return undefined;
}

/** Removes an address from the deposit cache. */
function removeActiveDepositAddressCache(address) {
    activeDepositAddresses.delete(address);
}

/** Adds a signature to the processed set and handles cache size limit. */
function addProcessedDepositTx(signature) {
    if (processedDepositTxSignatures.size >= MAX_PROCESSED_TX_CACHE_SIZE) {
        // Simple FIFO: remove the oldest entry (Set iteration order is insertion order)
        const oldestSig = processedDepositTxSignatures.values().next().value;
        if (oldestSig) {
            processedDepositTxSignatures.delete(oldestSig);
            // console.log(`[Cache] Pruned oldest TX sig from cache: ${oldestSig.slice(0,6)}...`);
        }
    }
    processedDepositTxSignatures.add(signature);
}

/** Checks if a signature has been processed this session. */
function hasProcessedDepositTx(signature) {
    return processedDepositTxSignatures.has(signature);
}


// --- Other Utilities ---

/**
 * Generates a unique referral code.
 * @param {number} [length=8] Desired length of the hex part.
 * @returns {string} Referral code (e.g., "ref_a1b2c3d4").
 */
function generateReferralCode(length = 8) {
    const byteLength = Math.ceil(length / 2);
    const randomBytes = crypto.randomBytes(byteLength);
    const hexString = randomBytes.toString('hex').slice(0, length);
    return `ref_${hexString}`;
}


// --- End of Part 3 ---
// index.js - Part 4: Game Logic Implementation
// --- VERSION: 3.1.0 ---

// --- Game Logic Helpers ---

/**
 * Selects a random winner from weighted options.
 * @param {Array<{name: string, weight: number}>} options Array of options with weights.
 * @returns {string} The name of the winning option.
 */
function weightedRandom(options) {
    let totalWeight = 0;
    for (const option of options) {
        totalWeight += option.weight;
    }
    let random = Math.random() * totalWeight;
    for (const option of options) {
        if (random < option.weight) {
            return option.name;
        }
        random -= option.weight;
    }
    // Fallback in case of floating point issues
    return options[options.length - 1].name;
}

// --- Coinflip Game Logic ---

/**
 * Handles Coinflip game logic, DB updates, and messaging.
 * @param {string} userId
 * @param {string} chatId
 * @param {bigint} wagerAmountLamports
 * @param {string} userChoice 'heads' or 'tails'
 * @param {number} betRecordId The ID from the 'bets' table.
 * @returns {Promise<boolean>} True if game processing and DB updates were successful.
 */
async function handleCoinflipGame(userId, chatId, wagerAmountLamports, userChoice, betRecordId) {
    const logPrefix = `[CF Bet ${betRecordId} User ${userId}]`;
    const cfEdge = GAME_CONFIG.coinflip.houseEdge;
    const displayName = await getUserDisplayName(chatId, userId); // Get name early for messages

    // 1. Determine Result (with house edge)
    const houseAutoWins = Math.random() < cfEdge;
    let actualResult = (Math.random() < 0.5) ? 'heads' : 'tails';
    let win;

    if (houseAutoWins) {
        console.log(`${logPrefix}: House edge (${(cfEdge * 100).toFixed(1)}%) triggered auto-loss.`);
        win = false;
        // Ensure the 'actualResult' shown to the user is the losing one
        actualResult = (userChoice === 'heads') ? 'tails' : 'heads';
    } else {
        win = (actualResult === userChoice);
    }

    // 2. Prepare Results for DB/Messaging
    let finalBetStatus = win ? 'completed_win' : 'completed_loss';
    let payoutLamports = win ? wagerAmountLamports * 2n : 0n; // Total credited back (Wager + Win or 0)
    let balanceChange = win ? wagerAmountLamports : 0n; // Net change for ledger (Win amount or 0)
    let ledgerType = win ? 'bet_won' : null; // Ledger entry only needed for win/push balance changes

    const wagerSOL = Number(wagerAmountLamports) / LAMPORTS_PER_SOL;
    const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;

    // 3. Send Result Message
    let resultMessage;
    if (win) {
        resultMessage = `🎉 ${displayName}, you won the coinflip\\! \\(Result: *${escapeMarkdownV2(actualResult)}*\\)\n` +
                        `Payout: ${escapeMarkdownV2(payoutSOL.toFixed(SOL_DECIMALS))} SOL added to your balance\\.`;
    } else {
        resultMessage = `❌ ${displayName}, you lost the coinflip\\! \\(Result: *${escapeMarkdownV2(actualResult)}*\\)\\. Better luck next time\\.`;
    }
    await safeSendMessage(chatId, resultMessage, { parse_mode: 'MarkdownV2' });

    // 4. Update Database
    let client = null; // Define client outside try if needed in finally
    try {
        if (win) { // Only need DB transaction for wins to credit balance and update ledger
            client = await pool.connect();
            await client.query('BEGIN');

            // Update balance and ledger
            const balanceUpdateResult = await updateUserBalanceAndLedger(
                client, userId, balanceChange, ledgerType, { betId: betRecordId }
            );
            if (!balanceUpdateResult.success) {
                throw new Error(`Failed to credit winnings: ${balanceUpdateResult.error}`);
            }

            // Mark bet as completed
            const betUpdateResult = await updateBetStatus(client, betRecordId, finalBetStatus, payoutLamports);
            if (!betUpdateResult) {
                throw new Error(`Failed to update bet status to ${finalBetStatus}.`);
            }

            await client.query('COMMIT');
            console.log(`${logPrefix} Completed as WIN. Balance credited.`);
        } else { // Loss - just update bet status (no balance change needed)
            const betUpdateResult = await updateBetStatus(pool, betRecordId, finalBetStatus, payoutLamports); // Use pool directly
            if (!betUpdateResult) {
                 // Log error but don't necessarily fail the whole flow if only status update failed after loss
                console.error(`${logPrefix} Failed to update bet status to ${finalBetStatus} after loss.`);
                // Consider if this should still return true or false? Let's return true as game logic finished.
            } else {
                console.log(`${logPrefix} Completed as LOSS.`);
            }
        }
        return true; // Indicate successful processing

    } catch (error) {
        console.error(`${logPrefix} Error processing game result/updating DB:`, error);
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { console.error("Rollback failed:", rbErr); } }
        // Attempt to mark bet as error
        await updateBetStatus(pool, betRecordId, 'error_game_logic');
        await safeSendMessage(chatId, `⚠️ Internal error processing coinflip result\\. Please contact support if your balance seems incorrect\\. Error ref: ${betRecordId}`, { parse_mode: 'MarkdownV2'});
        return false; // Indicate failure
    } finally {
        if (client) client.release();
    }
}

// --- Race Game Logic ---
// Simple weighted random outcome based on fixed odds, adjusted for house edge

// Define Horses and Base Odds (Example)
const RACE_HORSES = [
    { name: "Solar Sprint", weight: 10, payoutMultiplier: 3.0 }, // Lower payout, higher chance
    { name: "Comet Tail", weight: 7, payoutMultiplier: 4.5 },
    { name: "Nova Speedster", weight: 5, payoutMultiplier: 6.0 },
    { name: "Galaxy Gallop", weight: 3, payoutMultiplier: 9.0 },
    { name: "Nebula Bolt", weight: 2, payoutMultiplier: 12.0 }, // Higher payout, lower chance
];

/**
 * Handles Race game logic, DB updates, and messaging.
 * @param {string} userId
 * @param {string} chatId
 * @param {bigint} wagerAmountLamports
 * @param {string} chosenHorseName The name of the horse the user bet on.
 * @param {number} betRecordId The ID from the 'bets' table.
 * @returns {Promise<boolean>} True if game processing and DB updates were successful.
 */
async function handleRaceGame(userId, chatId, wagerAmountLamports, chosenHorseName, betRecordId) {
    const logPrefix = `[Race Bet ${betRecordId} User ${userId}]`;
    const raceEdge = GAME_CONFIG.race.houseEdge;
    const displayName = await getUserDisplayName(chatId, userId);

    // 1. Determine Winner (with house edge consideration)
    // Apply house edge by slightly reducing win probability across the board
    // Or, similar to coinflip, have a small chance the house just wins regardless. Let's use the latter for simplicity here.
    const houseAutoWins = Math.random() < raceEdge;
    let winningHorseName;
    let win = false;
    let payoutMultiplier = 0;

    if (houseAutoWins) {
        console.log(`${logPrefix}: House edge (${(raceEdge * 100).toFixed(1)}%) triggered auto-loss.`);
        // Pick a winner that the user *didn't* choose
        const possibleWinners = RACE_HORSES.filter(h => h.name !== chosenHorseName);
        if (possibleWinners.length === 0) { // Should only happen if only 1 horse exists
            winningHorseName = RACE_HORSES[0].name; // Fallback
        } else {
            winningHorseName = weightedRandom(possibleWinners);
        }
        win = false;
    } else {
        // Normal weighted random selection
        winningHorseName = weightedRandom(RACE_HORSES);
        win = (winningHorseName === chosenHorseName);
        if (win) {
            const winningHorse = RACE_HORSES.find(h => h.name === winningHorseName);
            payoutMultiplier = winningHorse?.payoutMultiplier || 0; // Get multiplier if won
        }
    }

    // 2. Prepare Results
    let finalBetStatus = win ? 'completed_win' : 'completed_loss';
    // Payout = Wager * Multiplier (if win), 0 otherwise
    let payoutLamports = win ? wagerAmountLamports * BigInt(Math.round(payoutMultiplier * 100)) / 100n : 0n; // Use BigInt math for multiplier
    let balanceChange = win ? payoutLamports - wagerAmountLamports : 0n; // Net change = Winnings
    let ledgerType = win ? 'bet_won' : null;

    const wagerSOL = Number(wagerAmountLamports) / LAMPORTS_PER_SOL;
    const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
    const winAmountSOL = Number(balanceChange) / LAMPORTS_PER_SOL;

    // 3. Send Result Message (Consider adding race visualization/text)
    let resultMessage = `🐎 The race is over\\! Winner: *${escapeMarkdownV2(winningHorseName)}*\\!\n\n`;
    if (win) {
        resultMessage += `🎉 ${displayName}, your horse *${escapeMarkdownV2(chosenHorseName)}* won\\!\n` +
                         `Payout: ${escapeMarkdownV2(payoutSOL.toFixed(SOL_DECIMALS))} SOL \\(+${escapeMarkdownV2(winAmountSOL.toFixed(SOL_DECIMALS))} SOL profit\\) added to balance\\.`;
    } else {
        resultMessage += `❌ ${displayName}, your horse *${escapeMarkdownV2(chosenHorseName)}* didn't win this time\\. Better luck next race\\!`;
    }
    await safeSendMessage(chatId, resultMessage, { parse_mode: 'MarkdownV2' });

    // 4. Update Database (Similar pattern to Coinflip)
    let client = null;
    try {
        if (win) { // Transaction only needed for wins
            client = await pool.connect();
            await client.query('BEGIN');
            const balanceUpdateResult = await updateUserBalanceAndLedger(client, userId, balanceChange, ledgerType, { betId: betRecordId });
            if (!balanceUpdateResult.success) throw new Error(`Failed to credit winnings: ${balanceUpdateResult.error}`);
            const betUpdateResult = await updateBetStatus(client, betRecordId, finalBetStatus, payoutLamports);
            if (!betUpdateResult) throw new Error(`Failed to update bet status to ${finalBetStatus}.`);
            await client.query('COMMIT');
            console.log(`${logPrefix} Completed as WIN. Balance credited.`);
        } else { // Loss
            const betUpdateResult = await updateBetStatus(pool, betRecordId, finalBetStatus, payoutLamports);
            if (!betUpdateResult) console.error(`${logPrefix} Failed to update bet status to ${finalBetStatus} after loss.`);
            else console.log(`${logPrefix} Completed as LOSS.`);
        }
        return true;
    } catch (error) {
        console.error(`${logPrefix} Error processing race result/DB:`, error);
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { console.error("Rollback failed:", rbErr); } }
        await updateBetStatus(pool, betRecordId, 'error_game_logic');
        await safeSendMessage(chatId, `⚠️ Internal error processing race result\\. Please contact support if your balance seems incorrect\\. Error ref: ${betRecordId}`, { parse_mode: 'MarkdownV2'});
        return false;
    } finally {
        if (client) client.release();
    }
}


// --- Slots Game Logic ---

// Define Symbols and Payouts (Example 3-Reel)
// Weights determine frequency, Payouts determine multiplier for wager
const SLOTS_SYMBOLS = [
    { symbol: '🍒', weight: 25, payout: { 3: 5, 2: 2 } },   // Payout for 3 or 2 matching on payline
    { symbol: '🍋', weight: 20, payout: { 3: 10, 2: 3 } },
    { symbol: '🍊', weight: 15, payout: { 3: 15, 2: 4 } },
    { symbol: '🍉', weight: 10, payout: { 3: 25, 2: 5 } },
    { symbol: '🔔', weight: 8, payout: { 3: 50 } },
    { symbol: '⭐', weight: 5, payout: { 3: 100 } },
    { symbol: '💎', weight: 2, payout: { 3: 250 } },       // Jackpot symbol
    { symbol: '❌', weight: 15, payout: {} },              // Blank/Miss symbol
];
const SLOTS_REELS = 3;
const SLOTS_PAYLINE = [1, 1, 1]; // Index of row for payline (0=top, 1=middle, 2=bottom) - Middle row

/**
 * Simulates a single reel spin returning one symbol based on weights.
 */
function spinReel(symbols) {
    return weightedRandom(symbols.map(s => ({ name: s.symbol, weight: s.weight })));
}

/**
 * Handles Slots game logic, DB updates, and messaging.
 * @param {string} userId
 * @param {string} chatId
 * @param {bigint} wagerAmountLamports
 * @param {number} betRecordId The ID from the 'bets' table.
 * @returns {Promise<boolean>} True if game processing and DB updates were successful.
 */
async function handleSlotsGame(userId, chatId, wagerAmountLamports, betRecordId) {
    const logPrefix = `[Slots Bet ${betRecordId} User ${userId}]`;
    // Note: Slots house edge is controlled implicitly by the payout table vs probabilities.
    // The env var SLOTS_HOUSE_EDGE isn't directly used in spin logic but serves as a target for tuning payouts.
    const displayName = await getUserDisplayName(chatId, userId);

    // 1. Spin Reels
    const reelResults = [];
    for (let i = 0; i < SLOTS_REELS; i++) {
        reelResults.push(spinReel(SLOTS_SYMBOLS));
    }
    const paylineResult = reelResults; // In this simple case, the result *is* the middle payline

    // 2. Determine Payout
    let winAmountMultiplier = 0;
    let winningSymbol = null;
    let matchCount = 0;

    // Check for 3 of a kind on the payline
    if (paylineResult[0] === paylineResult[1] && paylineResult[1] === paylineResult[2]) {
        winningSymbol = paylineResult[0];
        matchCount = 3;
    } else if (paylineResult[0] === paylineResult[1]) { // Check for 2 of a kind starting from left
        winningSymbol = paylineResult[0];
        matchCount = 2;
    } // Add more complex payline checks if needed (e.g., right-to-left, other symbol combos)

    if (winningSymbol) {
        const symbolConfig = SLOTS_SYMBOLS.find(s => s.symbol === winningSymbol);
        if (symbolConfig && symbolConfig.payout[matchCount]) {
            winAmountMultiplier = symbolConfig.payout[matchCount];
        }
    }

    // 3. Prepare Results
    const win = winAmountMultiplier > 0;
    let finalBetStatus = win ? 'completed_win' : 'completed_loss';
    let payoutLamports = win ? wagerAmountLamports * BigInt(Math.round(winAmountMultiplier * 100)) / 100n : 0n; // Total credited back
    let balanceChange = win ? payoutLamports - wagerAmountLamports : 0n; // Net change = Winnings
    let ledgerType = win ? 'bet_won' : null;

    const wagerSOL = Number(wagerAmountLamports) / LAMPORTS_PER_SOL;
    const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
    const winAmountSOL = Number(balanceChange) / LAMPORTS_PER_SOL;

    // 4. Send Result Message
    let resultMessage = `🎰 Spin Result: | ${paylineResult.join(' | ')} |\n\n`;
    if (win) {
        resultMessage += `🎉 ${displayName}, you won (${matchCount}x ${winningSymbol})\\! Multiplier: ${winAmountMultiplier}x\n` +
                         `Payout: ${escapeMarkdownV2(payoutSOL.toFixed(SOL_DECIMALS))} SOL \\(+${escapeMarkdownV2(winAmountSOL.toFixed(SOL_DECIMALS))} SOL profit\\) added to balance\\.`;
    } else {
        resultMessage += `❌ ${displayName}, no win this time\\. Spin again\\!`;
    }
    await safeSendMessage(chatId, resultMessage, { parse_mode: 'MarkdownV2' });

    // 5. Update Database (Similar pattern)
    let client = null;
    try {
        if (win) { // Transaction only needed for wins
            client = await pool.connect();
            await client.query('BEGIN');
            const balanceUpdateResult = await updateUserBalanceAndLedger(client, userId, balanceChange, ledgerType, { betId: betRecordId });
            if (!balanceUpdateResult.success) throw new Error(`Failed to credit winnings: ${balanceUpdateResult.error}`);
            const betUpdateResult = await updateBetStatus(client, betRecordId, finalBetStatus, payoutLamports);
            if (!betUpdateResult) throw new Error(`Failed to update bet status to ${finalBetStatus}.`);
            await client.query('COMMIT');
            console.log(`${logPrefix} Completed as WIN. Balance credited.`);
        } else { // Loss
            const betUpdateResult = await updateBetStatus(pool, betRecordId, finalBetStatus, payoutLamports);
            if (!betUpdateResult) console.error(`${logPrefix} Failed to update bet status to ${finalBetStatus} after loss.`);
            else console.log(`${logPrefix} Completed as LOSS.`);
        }
        return true;
    } catch (error) {
        console.error(`${logPrefix} Error processing slots result/DB:`, error);
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { console.error("Rollback failed:", rbErr); } }
        await updateBetStatus(pool, betRecordId, 'error_game_logic');
        await safeSendMessage(chatId, `⚠️ Internal error processing slots result\\. Please contact support if your balance seems incorrect\\. Error ref: ${betRecordId}`, { parse_mode: 'MarkdownV2'});
        return false;
    } finally {
        if (client) client.release();
    }
}

// --- Roulette Game Logic ---
// Simplified: Assumes betDetails is an object like { "red": amount, "17": amount, "odd": amount }
// Uses American Roulette wheel (0, 00, 1-36)

const ROULETTE_NUMBERS = Array.from({ length: 38 }, (_, i) => {
    if (i === 37) return '00'; // Index 37 represents '00'
    return String(i); // Indices 0-36 represent '0' through '36'
});
const ROULETTE_RED = [1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36].map(String);
const ROULETTE_BLACK = [2, 4, 6, 8, 10, 11, 13, 15, 17, 20, 22, 24, 26, 28, 29, 31, 33, 35].map(String);

/**
 * Calculates Roulette winnings for a given spin result and user bets.
 * @param {string} winningNumber The winning number ('0', '00', '1'-'36').
 * @param {object} userBets Object where keys are bet types ('red', 'black', 'odd', 'even', '1-18', '19-36', '0', '17', etc.) and values are wager amounts (as bigints).
 * @returns {bigint} Total payout lamports for this spin (includes original wager on winning bets).
 */
function calculateRoulettePayout(winningNumber, userBets) {
    let totalPayout = 0n;
    const numInt = parseInt(winningNumber); // NaN for '0', '00'
    const isZero = winningNumber === '0' || winningNumber === '00';
    const isRed = ROULETTE_RED.includes(winningNumber);
    const isBlack = ROULETTE_BLACK.includes(winningNumber);
    const isOdd = !isZero && numInt % 2 !== 0;
    const isEven = !isZero && numInt % 2 === 0;
    const isLow = !isZero && numInt >= 1 && numInt <= 18;
    const isHigh = !isZero && numInt >= 19 && numInt <= 36;

    for (const betType in userBets) {
        const wager = userBets[betType]; // Already BigInt
        let payoutMultiplier = 0;

        // Check bet types
        if (betType === winningNumber) payoutMultiplier = 36; // Straight up (pays 35:1 + wager)
        else if (betType === 'red' && isRed) payoutMultiplier = 2; // Red (pays 1:1 + wager)
        else if (betType === 'black' && isBlack) payoutMultiplier = 2;
        else if (betType === 'odd' && isOdd) payoutMultiplier = 2;
        else if (betType === 'even' && isEven) payoutMultiplier = 2;
        else if (betType === '1-18' && isLow) payoutMultiplier = 2;
        else if (betType === '19-36' && isHigh) payoutMultiplier = 2;
        // Add checks for splits, streets, corners, dozens, columns if supporting more complex bets

        if (payoutMultiplier > 0) {
            totalPayout += wager * BigInt(payoutMultiplier);
        }
    }
    return totalPayout;
}


/**
 * Handles Roulette game logic, DB updates, and messaging.
 * @param {string} userId
 * @param {string} chatId
 * @param {bigint} totalWagerAmountLamports Total wager across all placements.
 * @param {object} userBets Object mapping bet types to wager amounts (BigInts).
 * @param {number} betRecordId The ID from the 'bets' table.
 * @returns {Promise<boolean>} True if game processing and DB updates were successful.
 */
async function handleRouletteGame(userId, chatId, totalWagerAmountLamports, userBets, betRecordId) {
    const logPrefix = `[Roulette Bet ${betRecordId} User ${userId}]`;
    const displayName = await getUserDisplayName(chatId, userId);

    // 1. Spin the Wheel
    const winningIndex = Math.floor(Math.random() * ROULETTE_NUMBERS.length);
    const winningNumber = ROULETTE_NUMBERS[winningIndex];

    // 2. Calculate Payout
    const payoutLamports = calculateRoulettePayout(winningNumber, userBets); // Total credited back

    // 3. Prepare Results
    const win = payoutLamports > 0n;
    let finalBetStatus = win ? 'completed_win' : 'completed_loss'; // Roulette usually doesn't "push" overall
    // If payout > wager, it's a net win. If payout < wager, it's a net loss but still 'completed_win' technically if any bet hit.
    // Let's simplify: if payout > 0, status is win, otherwise loss.
    finalBetStatus = payoutLamports > 0n ? 'completed_win' : 'completed_loss';
    // Net balance change = Payout - Total Wager
    let balanceChange = payoutLamports - totalWagerAmountLamports;
    let ledgerType = null;
    if (balanceChange > 0n) ledgerType = 'bet_won';
    // If balanceChange < 0, the loss is already accounted for by the initial wager deduction.
    // If balanceChange == 0, could be considered a push if needed, but less common in Roulette total.

    const wagerSOL = Number(totalWagerAmountLamports) / LAMPORTS_PER_SOL;
    const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
    const netChangeSOL = Number(balanceChange) / LAMPORTS_PER_SOL;

    // Determine winning color for message
    let winningColor = 'Green';
    if (ROULETTE_RED.includes(winningNumber)) winningColor = 'Red';
    else if (ROULETTE_BLACK.includes(winningNumber)) winningColor = 'Black';

    // 4. Send Result Message
    let resultMessage = `⚪️ Roulette spin\\! The ball landed on: *${winningColor} ${escapeMarkdownV2(winningNumber)}*\\!\n\n`;
    if (payoutLamports > 0n) {
         resultMessage += `🎉 ${displayName}, you won\\!\n` +
                          `Total Payout: ${escapeMarkdownV2(payoutSOL.toFixed(SOL_DECIMALS))} SOL ` +
                          `\\(${netChangeSOL >= 0 ? '+' : ''}${escapeMarkdownV2(netChangeSOL.toFixed(SOL_DECIMALS))} SOL net\\) added to balance\\.`;
    } else {
        resultMessage += `❌ ${displayName}, no winning bets this spin\\. Better luck next time\\!`;
    }
    await safeSendMessage(chatId, resultMessage, { parse_mode: 'MarkdownV2' });

    // 5. Update Database
    let client = null;
    try {
        // Only need DB transaction if there's a net positive change or exact push (payout = wager)
        if (balanceChange >= 0n && payoutLamports > 0n) { // Need to credit back wager even on push
            client = await pool.connect();
            await client.query('BEGIN');

            // If balanceChange is exactly 0 (payout == wager), record as push return. Otherwise record win amount.
            const ledgerAmount = balanceChange > 0n ? balanceChange : totalWagerAmountLamports; // Credit net win OR original wager back
            const effectiveLedgerType = balanceChange > 0n ? 'bet_won' : 'bet_push_return';

            const balanceUpdateResult = await updateUserBalanceAndLedger(client, userId, ledgerAmount, effectiveLedgerType, { betId: betRecordId });
            if (!balanceUpdateResult.success) throw new Error(`Failed to credit balance: ${balanceUpdateResult.error}`);

            const betUpdateResult = await updateBetStatus(client, betRecordId, finalBetStatus, payoutLamports);
            if (!betUpdateResult) throw new Error(`Failed to update bet status to ${finalBetStatus}.`);

            await client.query('COMMIT');
            console.log(`${logPrefix} Completed as ${finalBetStatus}. Balance updated.`);
        } else { // Net Loss
            const betUpdateResult = await updateBetStatus(pool, betRecordId, 'completed_loss', 0n); // Payout is 0
            if (!betUpdateResult) console.error(`${logPrefix} Failed to update bet status to completed_loss after loss.`);
            else console.log(`${logPrefix} Completed as LOSS.`);
        }
        return true;
    } catch (error) {
        console.error(`${logPrefix} Error processing roulette result/DB:`, error);
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { console.error("Rollback failed:", rbErr); } }
        await updateBetStatus(pool, betRecordId, 'error_game_logic');
        await safeSendMessage(chatId, `⚠️ Internal error processing roulette result\\. Please contact support if your balance seems incorrect\\. Error ref: ${betRecordId}`, { parse_mode: 'MarkdownV2'});
        return false;
    } finally {
        if (client) client.release();
    }
}


// --- Casino War Game Logic ---
// Simplified: Single deck, Ace high. Tie results in player loss (simplest edge).

const WAR_SUITS = ['♠', '♥', '♦', '♣'];
const WAR_RANKS = ['2', '3', '4', '5', '6', '7', '8', '9', '10', 'J', 'Q', 'K', 'A'];
const WAR_CARD_VALUES = { '2': 2, '3': 3, '4': 4, '5': 5, '6': 6, '7': 7, '8': 8, '9': 9, '10': 10, 'J': 11, 'Q': 12, 'K': 13, 'A': 14 };

/** Creates and shuffles a standard 52-card deck. */
function getShuffledWarDeck() {
    const deck = [];
    for (const suit of WAR_SUITS) {
        for (const rank of WAR_RANKS) {
            deck.push({ rank, suit, value: WAR_CARD_VALUES[rank] });
        }
    }
    // Simple shuffle (Fisher-Yates)
    for (let i = deck.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [deck[i], deck[j]] = [deck[j], deck[i]];
    }
    return deck;
}

/**
 * Handles Casino War game logic, DB updates, and messaging.
 * @param {string} userId
 * @param {string} chatId
 * @param {bigint} wagerAmountLamports
 * @param {number} betRecordId The ID from the 'bets' table.
 * @returns {Promise<boolean>} True if game processing and DB updates were successful.
 */
async function handleWarGame(userId, chatId, wagerAmountLamports, betRecordId) {
    const logPrefix = `[War Bet ${betRecordId} User ${userId}]`;
    const displayName = await getUserDisplayName(chatId, userId);

    // 1. Deal Cards
    const deck = getShuffledWarDeck();
    if (deck.length < 2) { // Should never happen with standard deck
        console.error(`${logPrefix} Deck shuffling failed.`);
        await updateBetStatus(pool, betRecordId, 'error_game_logic');
        await safeSendMessage(chatId, `⚠️ Internal error dealing cards\\. Please contact support\\. Error ref: ${betRecordId}`, { parse_mode: 'MarkdownV2'});
        return false;
    }
    const playerCard = deck.pop();
    const dealerCard = deck.pop();

    // 2. Determine Outcome
    let resultText;
    let win = false;
    let push = false; // In this simple version, tie = loss

    if (playerCard.value > dealerCard.value) {
        win = true;
        resultText = "You Win!";
    } else if (playerCard.value < dealerCard.value) {
        win = false;
        resultText = "Dealer Wins!";
    } else { // Tie
        win = false; // Player loses on tie in this simplified version (provides house edge)
        push = false;
        resultText = "Tie! (Dealer Wins)";
        // Alternative: Implement actual "Go to War" logic if desired (more complex)
    }

    // 3. Prepare Results
    let finalBetStatus = win ? 'completed_win' : (push ? 'completed_push' : 'completed_loss');
    let payoutLamports = win ? wagerAmountLamports * 2n : (push ? wagerAmountLamports : 0n); // Total credited back
    let balanceChange = win ? wagerAmountLamports : (push ? wagerAmountLamports : 0n); // Net change OR wager return
    let ledgerType = win ? 'bet_won' : (push ? 'bet_push_return' : null);

    const wagerSOL = Number(wagerAmountLamports) / LAMPORTS_PER_SOL;
    const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
    const winAmountSOL = win ? Number(balanceChange) / LAMPORTS_PER_SOL : 0;

    // 4. Send Result Message
    let resultMessage = `🃏 Casino War Results 🃏\n\n` +
                        `Your Card: ${playerCard.rank}${playerCard.suit} \\(Value: ${playerCard.value}\\)\n` +
                        `Dealer Card: ${dealerCard.rank}${dealerCard.suit} \\(Value: ${dealerCard.value}\\)\n\n` +
                        `*${escapeMarkdownV2(resultText)}*\n\n`;

    if (win) {
        resultMessage += `🎉 ${displayName}, you won\\!\n` +
                         `Payout: ${escapeMarkdownV2(payoutSOL.toFixed(SOL_DECIMALS))} SOL \\(+${escapeMarkdownV2(winAmountSOL.toFixed(SOL_DECIMALS))} SOL profit\\) added to balance\\.`;
    } else if (push) { // If push logic were implemented
         resultMessage += ` PUSH\\! Your wager of ${escapeMarkdownV2(wagerSOL.toFixed(SOL_DECIMALS))} SOL has been returned\\.`;
    } else { // Loss
        resultMessage += `❌ ${displayName}, you lost this hand\\. Try again\\!`;
    }
    await safeSendMessage(chatId, resultMessage, { parse_mode: 'MarkdownV2' });

    // 5. Update Database
    let client = null;
    try {
        // Transaction needed for wins or pushes
        if (win || push) {
            client = await pool.connect();
            await client.query('BEGIN');

            const balanceUpdateResult = await updateUserBalanceAndLedger(client, userId, balanceChange, ledgerType, { betId: betRecordId });
            if (!balanceUpdateResult.success) throw new Error(`Failed to credit balance: ${balanceUpdateResult.error}`);

            const betUpdateResult = await updateBetStatus(client, betRecordId, finalBetStatus, payoutLamports);
            if (!betUpdateResult) throw new Error(`Failed to update bet status to ${finalBetStatus}.`);

            await client.query('COMMIT');
            console.log(`${logPrefix} Completed as ${finalBetStatus}. Balance updated.`);
        } else { // Loss
            const betUpdateResult = await updateBetStatus(pool, betRecordId, 'completed_loss', 0n);
            if (!betUpdateResult) console.error(`${logPrefix} Failed to update bet status to completed_loss after loss.`);
            else console.log(`${logPrefix} Completed as LOSS.`);
        }
        return true;
    } catch (error) {
        console.error(`${logPrefix} Error processing War result/DB:`, error);
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { console.error("Rollback failed:", rbErr); } }
        await updateBetStatus(pool, betRecordId, 'error_game_logic');
        await safeSendMessage(chatId, `⚠️ Internal error processing War result\\. Please contact support if your balance seems incorrect\\. Error ref: ${betRecordId}`, { parse_mode: 'MarkdownV2'});
        return false;
    } finally {
        if (client) client.release();
    }
}


// --- End of Part 4 ---
// index.js - Part 5: Telegram Command & Callback Handlers
// --- VERSION: 3.1.0 ---

// --- Main Message Handler ---

/**
 * Processes incoming Telegram messages (commands or text input based on state).
 * @param {TelegramBot.Message} msg The incoming message object.
 */
async function handleMessage(msg) {
    // Basic filtering at the entry point
    if (!msg || !msg.from || msg.from.is_bot || !msg.chat || (!msg.text && !msg.location && !msg.contact)) {
        return; // Ignore non-text messages from non-bots, allow location/contact for potential future features
    }

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const messageText = msg.text || '';
    const messageId = msg.message_id; // Useful for replies/edits
    const logPrefix = `[Msg ${messageId} User ${userId} Chat ${chatId}]`;

    // Cooldown Check (Apply to commands AND potential state-based text input)
    const now = Date.now();
    if (commandCooldown.has(userId)) {
        const lastTime = commandCooldown.get(userId);
        if (now - lastTime < USER_COMMAND_COOLDOWN_MS) {
            // console.log(`${logPrefix} Cooldown active, ignoring message.`);
            return; // Silently ignore if cooling down
        }
    }
    // Set cooldown immediately, regardless of message type if it passes initial filter
    commandCooldown.set(userId, now);
    setTimeout(() => { if (commandCooldown.get(userId) === now) commandCooldown.delete(userId); }, USER_COMMAND_COOLDOWN_MS + 500); // Clear slightly after interval

    try {
        // --- Check for Conversational State (User sent text while bot expects input) ---
        const currentState = userStateCache.get(userId);
        if (currentState?.state && currentState.state !== 'idle' && !messageText.startsWith('/')) {
            if (now - currentState.timestamp > USER_STATE_TTL_MS) {
                console.log(`${logPrefix} State cache expired for state ${currentState.state}. Clearing.`);
                userStateCache.delete(userId);
                await safeSendMessage(chatId, "Your previous action timed out\\. Please start again\\.", { parse_mode: 'MarkdownV2' });
                return;
            }

            // Route text input based on the expected state
            switch (currentState.state) {
                case 'awaiting_withdrawal_amount':
                case 'awaiting_cf_amount':
                case 'awaiting_race_amount': // Assuming Race also asks for amount first
                case 'awaiting_slots_amount':
                case 'awaiting_war_amount':
                    // TODO: Add awaiting_roulette_amount if needed
                    await handleAmountInput(msg, currentState);
                    return; // Stop further processing, amount handler takes over

                case 'awaiting_withdrawal_address':
                    await handleWithdrawalAddressInput(msg, currentState);
                    return;

                case 'awaiting_roulette_bets': // More complex state
                    await handleRouletteBetInput(msg, currentState);
                    return;

                default:
                    console.log(`${logPrefix} User in unhandled input state '${currentState.state}' received text: ${messageText.slice(0,50)}`);
                    // Clear state and prompt user
                    userStateCache.delete(userId);
                    await safeSendMessage(chatId, "Hmm, I wasn't expecting that input right now\\. Please use a command like /help or select an option\\.", { parse_mode: 'MarkdownV2' });
                    return;
            }
        }
        // --- End State Check ---

        // --- Command Processing ---
        const commandMatch = messageText.trim().match(/^\/(\w+)(?:@\w+)?(?:\s+(.*))?$/s);
        if (!commandMatch) {
            // If not in a state and not a command, ignore in groups, maybe reply in private?
            if (msg.chat.type === 'private') {
                // Avoid spamming "Unknown command" if user just types text without context
                // Check if user has an 'idle' state, if so, maybe show help?
                 if (!currentState || currentState.state === 'idle') {
                     await safeSendMessage(chatId, "Please use a command or select an option from the menu\\. Try /help or /start", { parse_mode: 'MarkdownV2' });
                 }
            }
            return;
        }

        const command = commandMatch[1].toLowerCase();
        const args = commandMatch[2]?.trim() || '';

        // Clear any pending conversational state if user issues a new command
        if (currentState && currentState.state !== 'idle') {
            console.log(`${logPrefix} New command '/${command}' received, clearing previous state '${currentState.state}'.`);
            userStateCache.delete(userId);
        }

        // --- /start Referral Code Parsing ---
        if (command === 'start' && args) {
            const refCodeMatch = args.match(/^(ref_[a-f0-9]{8})$/i);
            if (refCodeMatch && refCodeMatch[1]) {
                const refCode = refCodeMatch[1];
                console.log(`${logPrefix}: User started with referral code ${refCode}`);
                // Check if user exists and *has not* been referred yet
                const refereeDetails = await getUserWalletDetails(userId); // Use detailed check
                const referrer = await getUserByReferralCode(refCode);

                if (referrer && referrer.user_id === userId) {
                     await safeSendMessage(chatId, `Welcome\\! You can't use your own referral code\\.`, { parse_mode: 'MarkdownV2' });
                } else if (refereeDetails && refereeDetails.referred_by_user_id) {
                     await safeSendMessage(chatId, `Welcome back\\! You have already been referred\\.`, { parse_mode: 'MarkdownV2' });
                } else if (referrer) {
                     // User is potentially new OR exists but hasn't been referred yet
                     pendingReferrals.set(userId, { referrerUserId: referrer.user_id, timestamp: Date.now() });
                     // Ensure TTL cleanup for pending referrals map (simple approach)
                     setTimeout(() => {
                         const pending = pendingReferrals.get(userId);
                         if (pending && pending.referrerUserId === referrer.user_id && Date.now() - pending.timestamp >= PENDING_REFERRAL_TTL_MS) {
                             pendingReferrals.delete(userId);
                         }
                     }, PENDING_REFERRAL_TTL_MS + 5000);

                     await safeSendMessage(chatId, `Welcome\\! Referral code \`${escapeMarkdownV2(refCode)}\` accepted\\. Your first deposit will complete the referral link\\.`, { parse_mode: 'MarkdownV2' });
                     console.log(`${logPrefix}: Pending referral stored for user ${userId} referred by ${referrer.user_id}`);
                } else {
                     await safeSendMessage(chatId, `Welcome\\! The referral code \`${escapeMarkdownV2(refCode)}\` seems invalid or expired\\.`, { parse_mode: 'MarkdownV2' });
                }
                 // Fall through to execute normal /start handler anyway
            }
        }
        // --- End /start Referral Parsing ---

        // --- Command Handler Map ---
        const commandHandlers = {
            'start': handleStartCommand,
            'help': handleHelpCommand,
            'wallet': handleWalletCommand,
            'referral': handleReferralCommand,
            'deposit': handleDepositCommand,
            'withdraw': handleWithdrawCommand,
            'coinflip': handleCoinflipCommand,
            'race': handleRaceCommand,
            'slots': handleSlotsCommand,
            'roulette': handleRouletteCommand,
            'war': handleWarCommand,
            'admin': handleAdminCommand,
            // Add aliases if desired e.g., 'cf': handleCoinflipCommand
        };

        const handler = commandHandlers[command];

        if (handler) {
            // Admin Check
            if (command === 'admin') {
                const adminIds = (process.env.ADMIN_USER_IDS || '').split(',').map(id=>id.trim()).filter(id => id);
                if (adminIds.includes(userId)) {
                    await handler(msg, args); // Admin commands might use args
                } else {
                    console.warn(`${logPrefix} Unauthorized attempt to use /admin`);
                    // Optionally send a message, or just ignore
                }
            } else {
                await handler(msg, args); // Pass args for commands that might use them (wallet, withdraw amount shortcut?)
            }
        } else {
            if (msg.chat.type === 'private') {
                await safeSendMessage(chatId, `❓ Unknown command: \`/${escapeMarkdownV2(command)}\`\\. Try /help or /start`, { parse_mode: 'MarkdownV2' });
            }
        }
    } catch (error) {
        console.error(`${logPrefix} Error processing message/command:`, error);
        // Clear state on error just in case
        userStateCache.delete(userId);
        try {
            // Send a generic error message
            await safeSendMessage(chatId, "⚠️ An unexpected error occurred processing your request\\. Please try again or contact support\\.", { parse_mode: 'MarkdownV2'});
        } catch (tgError) {
            console.error(`${logPrefix} Failed to send error message to user:`, tgError);
        }
    }
}


// --- Callback Query Handler ---

/**
 * Processes incoming Telegram callback queries (button presses).
 * @param {TelegramBot.CallbackQuery} callbackQuery The incoming callback query object.
 */
async function handleCallbackQuery(callbackQuery) {
    const userId = String(callbackQuery.from.id);
    const chatId = callbackQuery.message?.chat.id;
    const messageId = callbackQuery.message?.message_id;
    const callbackData = callbackQuery.data;
    const callbackQueryId = callbackQuery.id;
    const logPrefix = `[Callback ${callbackQueryId} User ${userId} Chat ${chatId || 'N/A'}]`;

    // Check for essential data
    if (!chatId || !messageId || !callbackData) {
        console.error(`${logPrefix} Received callback query with missing message data.`);
        // Try to answer anyway to stop the loading spinner
        await bot.answerCallbackQuery(callbackQueryId).catch(e => console.warn(`${logPrefix} Failed to answer callback query with missing data: ${e.message}`));
        return;
    }

    // Always answer the callback query quickly to remove loading state
    await bot.answerCallbackQuery(callbackQueryId).catch(e => console.warn(`${logPrefix} Failed to answer callback query: ${e.message}`));

    console.log(`${logPrefix} Received callback data: ${callbackData}`);

    try {
        const currentState = userStateCache.get(userId);
        const now = Date.now();

        // Check for expired state if applicable to the action
        if (currentState && currentState.state !== 'idle' && now - currentState.timestamp > USER_STATE_TTL_MS) {
            console.log(`${logPrefix} State cache expired processing callback. State: ${currentState.state}`);
            userStateCache.delete(userId);
            // Try removing buttons or showing timeout message
            await bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: messageId }).catch(() => {});
            await safeSendMessage(chatId, "Your previous action timed out\\. Please start again using the commands\\.", { parse_mode: 'MarkdownV2'});
            return;
        }

        // --- Route callback data ---
        const [action, ...params] = callbackData.split(':'); // params is an array of remaining parts

        switch (action) {
            // --- Menu Actions ---
            case 'menu':
                const menuItem = params[0];
                // Simulate calling the command handler for menu items
                const menuCommandHandlers = {
                    'coinflip': handleCoinflipCommand,
                    'race': handleRaceCommand,
                    'slots': handleSlotsCommand,
                    'roulette': handleRouletteCommand,
                    'war': handleWarCommand,
                    'deposit': handleDepositCommand,
                    'withdraw': handleWithdrawCommand,
                    'wallet': handleWalletCommand,
                    'referral': handleReferralCommand,
                    'help': handleHelpCommand,
                };
                const menuHandler = menuCommandHandlers[menuItem];
                if (menuHandler) {
                    // Create a mock msg object suitable for the command handler
                    const mockMsg = { ...callbackQuery.message, from: callbackQuery.from, text: `/${menuItem}` };
                    await menuHandler(mockMsg, ''); // Pass empty args
                } else {
                     console.warn(`${logPrefix} Unknown menu action: ${menuItem}`);
                }
                break;

            // --- Game Actions (Example: Coinflip Choice) ---
            case 'cf_choice':
                if (!currentState || currentState.state !== 'awaiting_cf_choice' || !currentState.data?.amountLamports) {
                    throw new Error("Invalid state for coinflip choice or missing wager amount.");
                }
                const choice = params[0]; // 'heads' or 'tails'
                if (choice !== 'heads' && choice !== 'tails') {
                    throw new Error(`Invalid coinflip choice received: ${choice}`);
                }
                const cfWager = BigInt(currentState.data.amountLamports);

                // Edit message to show processing, remove buttons
                await bot.editMessageText(`🪙 Coinflip: You chose *${escapeMarkdownV2(choice)}* for ${escapeMarkdownV2((Number(cfWager)/LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL\\. Flipping...`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: undefined }).catch(()=>{/*ignore if already gone*/});
                userStateCache.delete(userId); // Clear state before async game logic

                // Place bet (Deduct balance, create record)
                const { success: betPlaced, betId, error: betError } = await placeBet(userId, chatId, 'coinflip', { choice }, cfWager);

                if (!betPlaced || !betId) {
                     // placeBet function should have sent error message to user already
                     console.error(`${logPrefix} Failed to place coinflip bet: ${betError}`);
                     // No need to send another error message here
                     break;
                }

                // Run game logic
                const gameSuccess = await handleCoinflipGame(userId, String(chatId), cfWager, choice, betId);

                // Trigger referral checks if game processed successfully
                if (gameSuccess) {
                     await _handleReferralChecks(userId, betId, cfWager, logPrefix);
                }
                // Result message is sent by handleCoinflipGame
                break;

            // --- Add cases for Race choice, Slots spin, Roulette confirm, War confirm etc. ---
            // Example: Slots Spin Confirmation
            case 'slots_spin_confirm':
                 if (!currentState || currentState.state !== 'awaiting_slots_confirm' || !currentState.data?.amountLamports) {
                     throw new Error("Invalid state for slots confirmation or missing wager amount.");
                 }
                 const slotsWager = BigInt(currentState.data.amountLamports);

                 await bot.editMessageText(`🎰 Slots: Spinning the reels for ${escapeMarkdownV2((Number(slotsWager)/LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL...`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: undefined }).catch(()=>{});
                 userStateCache.delete(userId);

                 const { success: slotsBetPlaced, betId: slotsBetId, error: slotsBetError } = await placeBet(userId, chatId, 'slots', {}, slotsWager); // Empty betDetails for simple slots

                 if (!slotsBetPlaced || !slotsBetId) {
                      console.error(`${logPrefix} Failed to place slots bet: ${slotsBetError}`);
                      break;
                 }
                 const slotsGameSuccess = await handleSlotsGame(userId, String(chatId), slotsWager, slotsBetId);
                 if (slotsGameSuccess) {
                      await _handleReferralChecks(userId, slotsBetId, slotsWager, logPrefix);
                 }
                 break;

            // Example: War Confirmation
            case 'war_confirm':
                 if (!currentState || currentState.state !== 'awaiting_war_confirm' || !currentState.data?.amountLamports) {
                     throw new Error("Invalid state for War confirmation or missing wager amount.");
                 }
                 const warWager = BigInt(currentState.data.amountLamports);

                 await bot.editMessageText(`🃏 Casino War: Dealing cards for ${escapeMarkdownV2((Number(warWager)/LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL...`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: undefined }).catch(()=>{});
                 userStateCache.delete(userId);

                 const { success: warBetPlaced, betId: warBetId, error: warBetError } = await placeBet(userId, chatId, 'war', {}, warWager); // Empty betDetails

                 if (!warBetPlaced || !warBetId) {
                      console.error(`${logPrefix} Failed to place War bet: ${warBetError}`);
                      break;
                 }
                 const warGameSuccess = await handleWarGame(userId, String(chatId), warWager, warBetId);
                 if (warGameSuccess) {
                      await _handleReferralChecks(userId, warBetId, warWager, logPrefix);
                 }
                 break;


             // --- Withdrawal Confirmation ---
            case 'withdraw_confirm':
                if (!currentState || currentState.state !== 'awaiting_withdrawal_confirm' || !currentState.data?.amountLamports || !currentState.data?.recipientAddress) {
                    throw new Error("Invalid state for withdrawal confirmation.");
                }
                const { amountLamports: withdrawAmountStr, recipientAddress: withdrawAddress } = currentState.data;
                const withdrawAmount = BigInt(withdrawAmountStr);
                const fee = WITHDRAWAL_FEE_LAMPORTS;
                const finalSendAmount = withdrawAmount; // Fee is deducted internally
                const totalDeduction = withdrawAmount + fee;

                console.log(`${logPrefix} User confirmed withdrawal of ${Number(finalSendAmount) / LAMPORTS_PER_SOL} SOL to ${withdrawAddress}`);
                userStateCache.delete(userId); // Clear state

                // Edit message
                await bot.editMessageText(`💸 Processing withdrawal request... Please wait.`, { chat_id: chatId, message_id: messageId, reply_markup: undefined }).catch(()=>{});

                // --- Perform checks and queue ---
                // Check balance again just before creating request
                const currentBalance = await getUserBalance(userId);
                if (currentBalance < totalDeduction) {
                    await safeSendMessage(chatId, `⚠️ Withdrawal failed\\. Your current balance \\(${escapeMarkdownV2((Number(currentBalance) / LAMPORTS_PER_SOL).toFixed(4))} SOL\\) is insufficient for the requested amount plus fee \\(${escapeMarkdownV2((Number(totalDeduction) / LAMPORTS_PER_SOL).toFixed(4))} SOL\\)\\.`, { parse_mode: 'MarkdownV2' });
                    break;
                }

                // Create withdrawal record (status 'pending')
                const wdRequest = await createWithdrawalRequest(userId, withdrawAmount, fee, withdrawAddress);
                if (!wdRequest.success || !wdRequest.withdrawalId) {
                    await safeSendMessage(chatId, `⚠️ Failed to initiate withdrawal request in database\\. Please try again or contact support\\. Error: ${escapeMarkdownV2(wdRequest.error || 'DB Error')}`, { parse_mode: 'MarkdownV2' });
                    break;
                }

                // Queue the payout job
                await addPayoutJob(
                    { type: 'payout_withdrawal', withdrawalId: wdRequest.withdrawalId, userId: userId },
                    parseInt(process.env.PAYOUT_JOB_RETRIES, 10),
                    parseInt(process.env.PAYOUT_JOB_RETRY_DELAY_MS, 10)
                );

                await safeSendMessage(chatId, `✅ Withdrawal request submitted for ${escapeMarkdownV2((Number(finalSendAmount) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL\\. You will be notified upon completion\\.`, { parse_mode: 'MarkdownV2' });
                break;

            // --- Generic Cancel ---
            case 'cancel_action':
                console.log(`${logPrefix} User cancelled action.`);
                const cancelledState = currentState?.state || 'unknown';
                userStateCache.delete(userId);
                await bot.editMessageText(`Action cancelled (${escapeMarkdownV2(cancelledState)}).`, { chat_id: chatId, message_id: messageId, reply_markup: undefined }).catch(()=>{});
                break;

            default:
                console.warn(`${logPrefix} Received unknown callback action: ${action}`);
                // Optionally send a message? "Unknown button pressed"
                await bot.answerCallbackQuery(callbackQueryId, { text: "Unknown button action." }).catch(() => {}); // Show alert
                break;
        }

    } catch (error) {
        console.error(`${logPrefix} Error processing callback query data '${callbackData}':`, error);
        userStateCache.delete(userId); // Clear state on error
        try {
            // Try to edit the original message to show an error
            await bot.editMessageText(`⚠️ Error: ${escapeMarkdownV2(error.message)}`, { chat_id: chatId, message_id: messageId, reply_markup: undefined, parse_mode: 'MarkdownV2' }).catch(()=>{});
        } catch (editError) {
            // If editing fails, send a new message
            await safeSendMessage(chatId, `⚠️ An error occurred processing your action\\. Please try again\\. Error: ${escapeMarkdownV2(error.message)}`, { parse_mode: 'MarkdownV2' });
        }
    }
}

// --- Helper to Place Bet (Deduct Balance, Create Record) ---
/**
 * Handles the atomic process of deducting balance and creating a bet record.
 * @returns {Promise<{success: boolean, betId?: number, error?: string}>}
 */
async function placeBet(userId, chatId, gameType, betDetails, wagerAmount) {
    let client = null;
    try {
        client = await pool.connect();
        await client.query('BEGIN');

        // 1. Deduct balance and add 'bet_placed' ledger entry
        const balanceUpdateResult = await updateUserBalanceAndLedger(
            client, userId, -wagerAmount, 'bet_placed', {}, `Bet on ${gameType}`
        );
        if (!balanceUpdateResult.success) {
            // If deduction failed (e.g., insufficient balance), rollback and inform user
            await client.query('ROLLBACK');
             const errorMsg = balanceUpdateResult.error || 'Failed to place bet.';
             console.error(`[PlaceBet ${userId}] Failed: ${errorMsg}`);
             // Try to inform the user directly
             const currentBalance = await getUserBalance(userId); // Fetch current balance to show user
             await safeSendMessage(chatId, `⚠️ Bet failed\\! ${escapeMarkdownV2(errorMsg)}\\. Your balance: ${escapeMarkdownV2((Number(currentBalance)/LAMPORTS_PER_SOL).toFixed(4))} SOL\\.`, { parse_mode: 'MarkdownV2'});
             return { success: false, error: errorMsg };
        }

        // 2. Create bet record
        const betRecordResult = await createBetRecord(client, userId, String(chatId), gameType, betDetails, wagerAmount);
        if (!betRecordResult.success || !betRecordResult.betId) {
            // If bet record creation fails, rollback balance deduction
            await client.query('ROLLBACK');
             const errorMsg = betRecordResult.error || 'Failed to record bet in database.';
             console.error(`[PlaceBet ${userId}] Failed: ${errorMsg}`);
             await safeSendMessage(chatId, `⚠️ Bet failed due to internal error recording the bet\\. Your balance has not been charged\\. Please try again\\. Error: ${escapeMarkdownV2(errorMsg)}`, { parse_mode: 'MarkdownV2'});
             return { success: false, error: errorMsg };
        }

        // 3. Commit transaction
        await client.query('COMMIT');
        console.log(`[PlaceBet ${userId}] Successfully placed bet ${betRecordResult.betId} for ${gameType}. Wager: ${wagerAmount}`);
        return { success: true, betId: betRecordResult.betId };

    } catch (error) {
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { /* Ignore */ } }
        console.error(`[PlaceBet ${userId}] Unexpected error placing bet for ${gameType}:`, error);
        await safeSendMessage(chatId, `⚠️ An unexpected error occurred while placing your bet\\. Please try again or contact support\\.`, { parse_mode: 'MarkdownV2'});
        return { success: false, error: 'Unexpected error placing bet.' };
    } finally {
        if (client) client.release();
    }
}


// --- Input Handlers (Called by handleMessage based on state) ---

/**
 * Handles numeric amount input for games or withdrawals.
 * @param {TelegramBot.Message} msg The user's message containing the amount.
 * @param {object} currentState The user's current state object from userStateCache.
 */
async function handleAmountInput(msg, currentState) {
    const userId = String(msg.from.id);
    const chatId = msg.chat.id;
    const text = msg.text.trim();
    const stateType = currentState.state; // e.g., 'awaiting_cf_amount', 'awaiting_withdrawal_amount'
    const logPrefix = `[Input State:${stateType} User ${userId}]`;

    let amountSOL;
    try {
        // Allow 'max' input for withdrawals? Or specific game amounts? TBD.
        amountSOL = parseFloat(text);
        if (isNaN(amountSOL) || amountSOL <= 0) {
            await safeSendMessage(chatId, "⚠️ Invalid amount\\. Please enter a positive number \\(e\\.g\\., 0\\.1\\)\\.", { parse_mode: 'MarkdownV2' });
            return; // Keep user in same state
        }
    } catch (e) {
        await safeSendMessage(chatId, "⚠️ Could not read amount\\. Please enter a number \\(e\\.g\\., 0\\.1\\)\\.", { parse_mode: 'MarkdownV2' });
        return;
    }

    const amountLamports = BigInt(Math.round(amountSOL * LAMPORTS_PER_SOL));

    // --- Withdrawal Amount Handling ---
    if (stateType === 'awaiting_withdrawal_amount') {
        if (amountLamports < MIN_WITHDRAWAL_LAMPORTS) {
            await safeSendMessage(chatId, `⚠️ Minimum withdrawal amount is ${escapeMarkdownV2((Number(MIN_WITHDRAWAL_LAMPORTS) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL\\. Please enter a valid amount\\.`, { parse_mode: 'MarkdownV2' });
            return;
        }
        const fee = WITHDRAWAL_FEE_LAMPORTS;
        const totalNeeded = amountLamports + fee;
        const currentBalance = await getUserBalance(userId);
        if (currentBalance < totalNeeded) {
            await safeSendMessage(chatId, `⚠️ Insufficient balance \\(${escapeMarkdownV2((Number(currentBalance) / LAMPORTS_PER_SOL).toFixed(4))} SOL\\) for withdrawal amount \\(${escapeMarkdownV2(amountSOL.toFixed(4))} SOL\\) \\+ fee \\(${escapeMarkdownV2((Number(fee)/LAMPORTS_PER_SOL).toFixed(4))} SOL\\)\\. Total needed: ${escapeMarkdownV2((Number(totalNeeded)/LAMPORTS_PER_SOL).toFixed(4))} SOL`, { parse_mode: 'MarkdownV2' });
            userStateCache.delete(userId); // Clear state, user needs to start over
            return;
        }

        // Get linked address
        const linkedAddress = await getLinkedWallet(userId);
        if (!linkedAddress) {
             // This check should ideally happen in /withdraw command, but double-check here
             await safeSendMessage(chatId, `⚠️ Cannot proceed\\. Please set your withdrawal address first using \`/wallet <YourSolanaAddress>\`\\.`, { parse_mode: 'MarkdownV2' });
             userStateCache.delete(userId);
             return;
        }

        // Update state, ask for confirmation WITH the address
        userStateCache.set(userId, {
            state: 'awaiting_withdrawal_confirm',
            chatId: chatId,
            messageId: currentState.messageId, // Keep original message ID if needed for edit context
            data: { amountLamports: amountLamports.toString(), recipientAddress: linkedAddress },
            timestamp: Date.now()
        });

        const options = {
            reply_markup: { inline_keyboard: [[{ text: '✅ Confirm Withdrawal', callback_data: 'withdraw_confirm' }], [{ text: '❌ Cancel', callback_data: 'cancel_action' }]] },
            parse_mode: 'MarkdownV2'
        };
        await safeSendMessage(chatId,
            `*Confirm Withdrawal*\n\n` +
            `Amount: ${escapeMarkdownV2(amountSOL.toFixed(SOL_DECIMALS))} SOL\n` +
            `Fee: ${escapeMarkdownV2((Number(fee) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL\n` +
            `Recipient: \`${escapeMarkdownV2(linkedAddress)}\`\n\n` +
            `Total to be deducted: ${escapeMarkdownV2((Number(totalDeduction) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL\\. Press confirm to proceed\\.`,
            options
        );
        return; // Flow continues via callback
    }

    // --- Game Amount Handling ---
    let gameKey = '';
    let nextState = '';
    let gameName = '';
    let confirmationCallback = ''; // Callback for simple confirm buttons

    if (stateType === 'awaiting_cf_amount') { gameKey = 'coinflip'; nextState = 'awaiting_cf_choice'; gameName = 'Coinflip'; }
    else if (stateType === 'awaiting_race_amount') { gameKey = 'race'; nextState = 'awaiting_race_choice'; gameName = 'Race'; } // Requires horse choice next
    else if (stateType === 'awaiting_slots_amount') { gameKey = 'slots'; nextState = 'awaiting_slots_confirm'; gameName = 'Slots'; confirmationCallback = 'slots_spin_confirm'; }
    else if (stateType === 'awaiting_war_amount') { gameKey = 'war'; nextState = 'awaiting_war_confirm'; gameName = 'Casino War'; confirmationCallback = 'war_confirm'; }
    // Add Roulette later if needed

    if (gameKey) {
        const gameCfg = GAME_CONFIG[gameKey];
        if (!gameCfg) { // Should not happen if state is set
             console.error(`${logPrefix} Invalid game key derived from state: ${gameKey}`);
             userStateCache.delete(userId);
             await safeSendMessage(chatId, `⚠️ Internal error starting game\\. Please try again\\.`, { parse_mode: 'MarkdownV2'});
             return;
        }

        // Validate bet limits
        if (amountLamports < gameCfg.minBetLamports || amountLamports > gameCfg.maxBetLamports) {
            const min = (Number(gameCfg.minBetLamports) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
            const max = (Number(gameCfg.maxBetLamports) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
            await safeSendMessage(chatId, `⚠️ ${escapeMarkdownV2(gameName)} bet amount must be between ${escapeMarkdownV2(min)} and ${escapeMarkdownV2(max)} SOL\\. Please enter a valid amount\\.`, { parse_mode: 'MarkdownV2' });
            return; // Keep user in same state
        }

        // Check balance
        const currentBalance = await getUserBalance(userId);
        if (currentBalance < amountLamports) {
            await safeSendMessage(chatId, `⚠️ Insufficient balance \\(${escapeMarkdownV2((Number(currentBalance) / LAMPORTS_PER_SOL).toFixed(4))} SOL\\) to place that ${escapeMarkdownV2(gameName)} bet\\. Use /deposit to add funds\\.`, { parse_mode: 'MarkdownV2' });
            userStateCache.delete(userId); // Clear state
            return;
        }

        // Update state to next step
        userStateCache.set(userId, {
            state: nextState,
            chatId: chatId,
            messageId: currentState.messageId,
            data: { amountLamports: amountLamports.toString() }, // Store amount for next step
            timestamp: Date.now()
        });

        // Send appropriate next message (buttons for choice/confirmation)
        if (nextState === 'awaiting_cf_choice') {
             const options = {
                 reply_markup: { inline_keyboard: [[{ text: 'Heads', callback_data: 'cf_choice:heads' }, { text: 'Tails', callback_data: 'cf_choice:tails' }], [{ text: 'Cancel', callback_data: 'cancel_action' }]] },
                 parse_mode: 'MarkdownV2'
             };
             await safeSendMessage(chatId, `🪙 Betting ${escapeMarkdownV2(amountSOL.toFixed(SOL_DECIMALS))} SOL on Coinflip\\. Choose Heads or Tails:`, options);
        } else if (nextState === 'awaiting_race_choice') {
             // TODO: Send message asking user to choose a horse with buttons
             const horseButtons = RACE_HORSES.map(h => ({ text: `${h.name} (${h.payoutMultiplier}x)`, callback_data: `race_choice:${h.name}` }));
             // Simple layout - potentially needs multiple rows if many horses
             const keyboard = [];
             for (let i = 0; i < horseButtons.length; i += 2) {
                 keyboard.push(horseButtons.slice(i, i + 2));
             }
             keyboard.push([{ text: 'Cancel', callback_data: 'cancel_action' }]);
             const options = { reply_markup: { inline_keyboard: keyboard }, parse_mode: 'MarkdownV2' };
             await safeSendMessage(chatId, `🐎 Betting ${escapeMarkdownV2(amountSOL.toFixed(SOL_DECIMALS))} SOL on the Race\\. Choose your horse:`, options);
        } else if (confirmationCallback) { // Simple confirmation needed
            const options = {
                reply_markup: { inline_keyboard: [[{ text: `✅ Confirm ${gameName} Bet`, callback_data: confirmationCallback }], [{ text: '❌ Cancel', callback_data: 'cancel_action' }]] },
                parse_mode: 'MarkdownV2'
            };
             await safeSendMessage(chatId, `Confirm ${escapeMarkdownV2(gameName)} bet of ${escapeMarkdownV2(amountSOL.toFixed(SOL_DECIMALS))} SOL?`, options);
        } else {
             console.error(`${logPrefix} Reached end of amount handler with unhandled next state: ${nextState}`);
             userStateCache.delete(userId);
        }
    } else {
        console.warn(`${logPrefix} Amount received in unexpected state: ${stateType}`);
        userStateCache.delete(userId);
    }
}

/**
 * Handles withdrawal address input.
 * @param {TelegramBot.Message} msg The user's message containing the address.
 * @param {object} currentState The user's current state object from userStateCache.
 */
async function handleWithdrawalAddressInput(msg, currentState) {
    const userId = String(msg.from.id);
    const chatId = msg.chat.id;
    const address = msg.text.trim();
    const logPrefix = `[Input State:awaiting_withdrawal_address User ${userId}]`;

    if (!currentState || currentState.state !== 'awaiting_withdrawal_address' || !currentState.data?.amountLamports) {
        console.error(`${logPrefix} Invalid state reached when expecting withdrawal address.`);
        userStateCache.delete(userId);
        await safeSendMessage(chatId, `⚠️ An error occurred\\. Please start the withdrawal again with /withdraw\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }

    // Validate address format
    let recipientPubKey;
    try {
        recipientPubKey = new PublicKey(address);
    } catch (e) {
        await safeSendMessage(chatId, `⚠️ Invalid Solana address format: \`${escapeMarkdownV2(address)}\`\\. Please enter a valid address\\.`, { parse_mode: 'MarkdownV2' });
        return; // Keep user in same state
    }

    // --- Check against known bot operational addresses ---
    const botKeys = [process.env.MAIN_BOT_PRIVATE_KEY, process.env.REFERRAL_PAYOUT_PRIVATE_KEY]
                        .filter(Boolean) // Remove undefined keys
                        .map(pkStr => { try { return Keypair.fromSecretKey(bs58.decode(pkStr)).publicKey.toBase58(); } catch { return null; }})
                        .filter(Boolean); // Filter out decoding errors

    if (botKeys.includes(address)) {
        await safeSendMessage(chatId, `⚠️ You cannot withdraw funds to the bot's own operational addresses\\. Please enter your personal wallet address\\.`, { parse_mode: 'MarkdownV2' });
        return; // Keep user in same state
    }
    // Optional: Check against known *deposit* addresses if feasible? More complex.

    // --- Address seems valid, proceed to confirmation ---
    const amountLamports = BigInt(currentState.data.amountLamports);
    const feeLamports = WITHDRAWAL_FEE_LAMPORTS;
    const totalDeduction = amountLamports + feeLamports;
    const amountSOL = Number(amountLamports) / LAMPORTS_PER_SOL;
    const feeSOL = Number(feeLamports) / LAMPORTS_PER_SOL;

    // Update state, ask for confirmation
    userStateCache.set(userId, {
        state: 'awaiting_withdrawal_confirm',
        chatId: chatId,
        messageId: currentState.messageId,
        data: { amountLamports: amountLamports.toString(), recipientAddress: address },
        timestamp: Date.now()
    });

    const options = {
        reply_markup: { inline_keyboard: [[{ text: '✅ Confirm Withdrawal', callback_data: 'withdraw_confirm' }], [{ text: '❌ Cancel', callback_data: 'cancel_action' }]] },
        parse_mode: 'MarkdownV2'
    };
    await safeSendMessage(chatId,
        `*Confirm Withdrawal*\n\n` +
        `Amount: ${escapeMarkdownV2(amountSOL.toFixed(SOL_DECIMALS))} SOL\n` +
        `Fee: ${escapeMarkdownV2(feeSOL.toFixed(SOL_DECIMALS))} SOL\n` +
        `Recipient: \`${escapeMarkdownV2(address)}\`\n\n` +
        `Total to be deducted: ${escapeMarkdownV2((Number(totalDeduction) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL\\. Press confirm to proceed\\.`,
        options
    );
}

/**
 * Handles Roulette bet input (placeholder for complex betting).
 */
async function handleRouletteBetInput(msg, currentState) {
     const chatId = msg.chat.id;
     // TODO: Implement parsing of roulette bets from text or via a Mini App / web interface
     await safeSendMessage(chatId, `Roulette bet input via text is complex\\. Coming soon via a better interface\\! Please /cancel and try another game\\.`, { parse_mode: 'MarkdownV2'});
     // For now, just cancel the state
     userStateCache.delete(String(msg.from.id));
}


// --- Command Handlers ---

/** Handles /start command */
async function handleStartCommand(msg, args) {
    const chatId = msg.chat.id;
    const userId = String(msg.from.id);
    const firstName = msg.from.first_name || 'there';

    // Ensure user exists in DB (creates if not) before showing menu
    await getUserBalance(userId);

    // Set user state to idle (clears any pending operation)
    userStateCache.set(userId, { state: 'idle', chatId: String(chatId), timestamp: Date.now() });

    const options = {
        reply_markup: {
            inline_keyboard: [
                // Row 1: Popular Games
                [ { text: '🪙 Coinflip', callback_data: 'menu:coinflip' }, { text: '🎰 Slots', callback_data: 'menu:slots' }, { text: '🃏 War', callback_data: 'menu:war' } ],
                // Row 2: Other Games
                [ { text: '🐎 Race', callback_data: 'menu:race' }, { text: '⚪️ Roulette', callback_data: 'menu:roulette' } ],
                 // Row 3: Funds Management
                [ { text: '💰 Deposit SOL', callback_data: 'menu:deposit' }, { text: '💸 Withdraw SOL', callback_data: 'menu:withdraw' } ],
                 // Row 4: Account & Help
                [ { text: '👤 Wallet / Balance', callback_data: 'menu:wallet' }, { text: '🤝 Referral Info', callback_data: 'menu:referral' }, { text: '❓ Help', callback_data: 'menu:help' } ]
            ]
        },
        parse_mode: 'MarkdownV2'
    };

    const welcomeMsg = `👋 Welcome, ${escapeMarkdownV2(firstName)}\\!\n\n` +
                       `I am a Solana-based gaming bot\\. Use the buttons below to play, manage funds, or get help\\.\n\n` +
                       `*Remember to gamble responsibly\\!*`;
    await safeSendMessage(chatId, welcomeMsg, options);
}

/** Handles /help command */
async function handleHelpCommand(msg, args) {
    const chatId = msg.chat.id;
    // Updated help text reflecting custodial model and commands
    const feeSOL = (Number(WITHDRAWAL_FEE_LAMPORTS) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
    const minWithdrawSOL = (Number(MIN_WITHDRAWAL_LAMPORTS) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
    const refRewardPercent = (REFERRAL_MILESTONE_REWARD_PERCENT * 100).toFixed(2);

    const helpMsg = `*Solana Gambles Bot Help* 🤖 (v3.1.0)\n\n` +
                    `This bot uses a *custodial* model\\. Deposit SOL to play instantly using your internal balance\\. Winnings are added to your balance, and you can withdraw anytime\\. \n\n`+
                    `*How to Play:*\n` +
                    `1\\. Use \`/deposit\` to get a unique address & send SOL\\. Wait for confirmation\\. \n` +
                    `2\\. Use \`/start\` or the game commands \\(\`/coinflip\`, \`/slots\`, etc\\.\\) or menu buttons\\. \n` +
                    `3\\. Enter your bet amount when prompted\\. \n` +
                    `4\\. Confirm your bet & play\\!\n\n` +
                    `*Core Commands:*\n` +
                    `/start - Show main menu \n` +
                    `/help - Show this message \n` +
                    `/wallet - View balance & withdrawal address \n` +
                    `/wallet <YourSolAddress> - Set/update withdrawal address \n` +
                    `/deposit - Get deposit address \n` +
                    `/withdraw - Start withdrawal process \n` +
                    `/referral - View your referral stats & link \n\n` +
                    `*Game Commands:* Initiate a game \\(e\\.g\\., \`/coinflip\`\\)\n\n` +
                    `*Referral Program:* 🤝\n` +
                    `Use \`/referral\` to get your code/link\\. New users start with \`/start <YourCode>\`\\. You earn SOL based on their first bet and their total wager volume \\(${escapeMarkdownV2(refRewardPercent)}% milestone bonus\\)\\. Rewards are paid to your linked wallet\\.\n\n` +
                    `*Important Notes:*\n` +
                    `\\- Only deposit to addresses given by \`/deposit\`\\. Each address is temporary and for one deposit only\\. \n` +
                    `\\- You *must* set a withdrawal address using \`/wallet <address>\` before withdrawing\\. \n` +
                    `\\- Withdrawals have a fee \\(${escapeMarkdownV2(feeSOL)} SOL\\) and minimum \\(${escapeMarkdownV2(minWithdrawSOL)} SOL\\)\\.\n` +
                    `\\- Please gamble responsibly\\. Contact support if you encounter issues\\.`;

    await safeSendMessage(chatId, helpMsg, { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}

/** Handles /wallet command (viewing or setting address) */
async function handleWalletCommand(msg, args) {
    const chatId = msg.chat.id;
    const userId = String(msg.from.id);
    const potentialNewAddress = args?.trim();

    if (potentialNewAddress) {
        // --- Set/Update Withdrawal Address ---
        const result = await linkUserWallet(userId, potentialNewAddress); // Handles its own messages
        if (result.success) {
             await safeSendMessage(chatId, `✅ Withdrawal address successfully set to: \`${escapeMarkdownV2(result.wallet)}\``, { parse_mode: 'MarkdownV2' });
             // Maybe show full wallet info again?
             // await handleWalletCommand(msg, ''); // Call self to display info
        } else {
            await safeSendMessage(chatId, `❌ Failed to set withdrawal address\\. Error: ${escapeMarkdownV2(result.error)}`, { parse_mode: 'MarkdownV2' });
        }
    } else {
        // --- View Wallet Info ---
        const userDetails = await getUserWalletDetails(userId); // Fetches from wallets table
        const userBalance = await getUserBalance(userId); // Fetches/creates balance record

        let walletMsg = `*Your Wallet & Referral Info* 💼\n\n` +
                        `*Balance:* ${escapeMarkdownV2((Number(userBalance) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL\n`;

        if (userDetails?.external_withdrawal_address) {
            walletMsg += `*Withdrawal Address:* \`${escapeMarkdownV2(userDetails.external_withdrawal_address)}\`\n`;
        } else {
            walletMsg += `*Withdrawal Address:* Not Set \\(Use \`/wallet <YourSolAddress>\`\\)\n`;
        }
        if (userDetails?.referral_code) {
            walletMsg += `*Referral Code:* \`${escapeMarkdownV2(userDetails.referral_code)}\`\n`;
        } else {
            // A code should ideally be generated upon first interaction or linking wallet
            const generatedCode = generateReferralCode();
            walletMsg += `*Referral Code:* \`N/A\` \\(Generating now?: \`${escapeMarkdownV2(generatedCode)}\` - Link wallet or deposit to confirm\\)\n`;
             // Try to assign the generated code if user exists but has no code
             if (userDetails) {
                 let client = null;
                 try {
                     client = await pool.connect();
                     await client.query('UPDATE wallets SET referral_code = $1 WHERE user_id = $2 AND referral_code IS NULL', [generatedCode, userId]);
                     // Update cache too
                     updateWalletCache(userId, { referralCode: generatedCode });
                 } catch(e){ console.error(`Error assigning generated ref code to user ${userId}: ${e.message}`); }
                 finally { if(client) client.release(); }
             }
        }
        walletMsg += `*Successful Referrals:* ${escapeMarkdownV2(userDetails?.referral_count || 0)}\n\n`;
        walletMsg += `Use \`/deposit\` to add funds, \`/withdraw\` to cash out\\.`;

        await safeSendMessage(chatId, walletMsg, { parse_mode: 'MarkdownV2' });
    }
}

/** Handles /referral command */
async function handleReferralCommand(msg, args) {
    const chatId = msg.chat.id;
    const userId = String(msg.from.id);
    const userDetails = await getUserWalletDetails(userId);

    // Require linked wallet for referral features
    if (!userDetails?.external_withdrawal_address) {
        await safeSendMessage(chatId, `❌ You need to link your wallet first using \`/wallet <YourSolAddress>\` before using the referral system\\. This ensures rewards can be paid out\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    if (!userDetails.referral_code) {
        // Attempt to generate/assign one if missing (should be rare after wallet linking)
         let client = null;
         let generatedCode = generateReferralCode();
         try {
              client = await pool.connect();
              const updateResult = await client.query('UPDATE wallets SET referral_code = $1 WHERE user_id = $2 AND referral_code IS NULL RETURNING referral_code', [generatedCode, userId]);
              if (updateResult.rowCount > 0) {
                 userDetails.referral_code = updateResult.rows[0].referral_code;
                 updateWalletCache(userId, { referralCode: userDetails.referral_code });
                 console.log(`Assigned missing ref code ${userDetails.referral_code} to user ${userId}`);
              } else {
                   // Fetch again in case of race condition
                   const freshDetails = await client.query('SELECT referral_code from wallets where user_id=$1', [userId]);
                   if (freshDetails.rows[0]?.referral_code) {
                       userDetails.referral_code = freshDetails.rows[0].referral_code;
                   } else {
                       throw new Error("Failed to retrieve/assign referral code.");
                   }
              }
         } catch(e){
            console.error(`Error assigning/retrieving ref code for user ${userId}: ${e.message}`);
            await safeSendMessage(chatId, `❌ Could not retrieve or assign your referral code\\. Please try linking your wallet again with \`/wallet <address>\`\\.`, { parse_mode: 'MarkdownV2' });
            return;
         } finally {
            if(client) client.release();
         }
    }

    const totalEarningsLamports = await getTotalReferralEarnings(userId);
    const totalEarningsSOL = (Number(totalEarningsLamports) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
    let botUsername = 'YourBotUsername'; // Fallback
    try {
        const me = await bot.getMe();
        if (me.username) {
            botUsername = me.username;
        }
    } catch (err) { console.error("Error fetching bot info for referral link:", err.message); }

    const referralLink = `https://t.me/${botUsername}?start=${userDetails.referral_code}`;
    const minBetSOL = (Number(REFERRAL_INITIAL_BET_MIN_LAMPORTS)/LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
    const milestonePercent = (REFERRAL_MILESTONE_REWARD_PERCENT * 100).toFixed(2);

    // Build initial bonus tiers text
    let tiersText = REFERRAL_INITIAL_BONUS_TIERS.map(tier => {
        const range = tier.maxCount === Infinity ? `${tier.maxCount === 101 ? 101 : (REFERRAL_INITIAL_BONUS_TIERS[REFERRAL_INITIAL_BONUS_TIERS.length - 2].maxCount + 1)}+` : `${tier.maxCount - (tier.maxCount === 10 ? 9 : REFERRAL_INITIAL_BONUS_TIERS[REFERRAL_INITIAL_BONUS_TIERS.indexOf(tier)-1].maxCount)}-${tier.maxCount}`;
         const countLabel = REFERRAL_INITIAL_BONUS_TIERS.indexOf(tier) === 0 ? `1-${tier.maxCount}` : range;
        return `  \\- ${escapeMarkdownV2(countLabel)} refs: ${escapeMarkdownV2((tier.percent * 100).toFixed(0))}%`;
    }).join('\n');

    let referralMsg = `*Your Referral Stats* 📊\n\n` +
                      `*Your Code:* \`${escapeMarkdownV2(userDetails.referral_code)}\`\n` +
                      `*Your Referral Link \\(Share this\\!\\):*\n` +
                      `\`${escapeMarkdownV2(referralLink)}\`\n\n` +
                      `*Successful Referrals:* ${escapeMarkdownV2(userDetails.referral_count)}\n` +
                      `*Total Referral Earnings Paid:* ${escapeMarkdownV2(totalEarningsSOL)} SOL\n\n` +
                      `*How Rewards Work:*\n` +
                      `1\\. *Initial Bonus:* Earn a tiered percentage of your referral's *first* completed bet \\(min ${escapeMarkdownV2(minBetSOL)} SOL wager\\)\\.\n` +
                      `2\\. *Milestone Bonus:* Earn ${escapeMarkdownV2(milestonePercent)}% of the total amount wagered by your referrals as they hit wagering milestones \\(e\\.g\\., 1, 5, 10 SOL etc\\.\\)\\.\n\n` +
                      `*Your Initial Bonus Tier \\(based on your ref count *before* their first bet\\):*\n` +
                      `${tiersText}\n\n` +
                      `Payouts are sent automatically to your linked wallet: \`${escapeMarkdownV2(userDetails.external_withdrawal_address)}\``;

    await safeSendMessage(chatId, referralMsg, { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}


/** Handles /deposit command */
async function handleDepositCommand(msg, args) {
    const chatId = msg.chat.id;
    const userId = String(msg.from.id);
    const logPrefix = `[DepositCmd User ${userId}]`;

    try {
        // Ensure user exists in wallets table (creates if not)
        await getUserBalance(userId); // Also ensures balance record

        // Get next available deposit address index for this user
        const addressIndex = await getNextDepositAddressIndex(userId); // Defined later or assumed DB function

        // Generate the unique address using the proper derivation
        const derivedInfo = await generateUniqueDepositAddress(userId, addressIndex);
        if (!derivedInfo) {
            throw new Error("Failed to generate a unique deposit address. Master seed phrase might be invalid or derivation failed.");
        }

        const depositAddress = derivedInfo.publicKey.toBase58();
        const derivationPath = derivedInfo.path;
        const expiresAt = new Date(Date.now() + DEPOSIT_ADDRESS_EXPIRY_MS);

        // Store address record in DB
        const recordResult = await createDepositAddressRecord(userId, depositAddress, derivationPath, expiresAt);
        if (!recordResult.success) {
            throw new Error(recordResult.error || "Failed to save deposit address record to database.");
        }

        // Display address to user
        const expiryMinutes = Math.round(DEPOSIT_ADDRESS_EXPIRY_MS / (60 * 1000));
        const message = `💰 *Deposit SOL*\n\n` +
                        `To add funds to your bot balance, send SOL to this unique address:\n\n` +
                        `\`${escapeMarkdownV2(depositAddress)}\`\n\n` +
                        `⚠️ *Important:*\n` +
                        `1\\. This address is temporary and only valid for *one* deposit\\. It expires in *${expiryMinutes} minutes*\\. \n` +
                        `2\\. Do *not* send multiple times to this address\\. Use \`/deposit\` again for new deposits\\. \n` +
                        `3\\. Deposits typically require *${escapeMarkdownV2(DEPOSIT_CONFIRMATION_LEVEL)}* network confirmations to reflect in your /wallet balance\\.`;

        // Send message with address (Consider adding QR code generation here if desired)
        await safeSendMessage(chatId, message, { parse_mode: 'MarkdownV2' });

    } catch (error) {
        console.error(`${logPrefix} Error: ${error.message}`);
        await safeSendMessage(chatId, `❌ Error generating deposit address: ${escapeMarkdownV2(error.message)}\\. Please try again later or contact support\\.`, { parse_mode: 'MarkdownV2' });
    }
}

// Helper function to get next deposit index (defined here for use in handler)
async function getNextDepositAddressIndex(userId) {
    try {
        const res = await queryDatabase(
            'SELECT COUNT(*) as count FROM deposit_addresses WHERE user_id = $1',
            [String(userId)]
        );
        // Use count as the next zero-based index
        return parseInt(res.rows[0]?.count || '0', 10);
    } catch (err) {
        console.error(`[DB GetNextDepositIndex] Error fetching count for user ${userId}:`, err);
        // Fallback to 0 on error? Risky if DB fails consistently. Throwing might be safer.
        throw new Error(`Failed to determine next deposit address index: ${err.message}`);
    }
}

/** Handles /withdraw command */
async function handleWithdrawCommand(msg, args) {
    const chatId = msg.chat.id;
    const userId = String(msg.from.id);
    const logPrefix = `[WithdrawCmd User ${userId}]`;

    try {
        // 1. Check if user has linked an external withdrawal address
        const linkedAddress = await getLinkedWallet(userId); // Uses cache/DB
        if (!linkedAddress) {
            await safeSendMessage(chatId, `⚠️ You must set your external withdrawal address first using \`/wallet <YourSolanaAddress>\`\\. Rewards and withdrawals are sent there\\.`, { parse_mode: 'MarkdownV2' });
            return;
        }

        // 2. Check current balance (ensure they have *something* withdrawable)
        const currentBalance = await getUserBalance(userId);
        if (currentBalance < MIN_WITHDRAWAL_LAMPORTS + WITHDRAWAL_FEE_LAMPORTS) {
             const minNeeded = (Number(MIN_WITHDRAWAL_LAMPORTS + WITHDRAWAL_FEE_LAMPORTS) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
             await safeSendMessage(chatId, `⚠️ Your balance \\(${escapeMarkdownV2((Number(currentBalance)/LAMPORTS_PER_SOL).toFixed(4))} SOL\\) is below the minimum required to cover the withdrawal amount \\+ fee \\(min ${escapeMarkdownV2(minNeeded)} SOL total\\)\\.`, { parse_mode: 'MarkdownV2'});
             return;
        }

        // 3. Start the withdrawal flow by asking for amount
        userStateCache.set(userId, {
            state: 'awaiting_withdrawal_amount',
            chatId: String(chatId),
            messageId: msg.message_id, // Store message ID for context if needed
            timestamp: Date.now(),
            data: {} // No data needed yet
        });

        await safeSendMessage(chatId, `💸 Your withdrawal address: \`${escapeMarkdownV2(linkedAddress)}\`\n` +
                                     `Minimum withdrawal: ${escapeMarkdownV2((Number(MIN_WITHDRAWAL_LAMPORTS)/LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL\\. Fee: ${escapeMarkdownV2((Number(WITHDRAWAL_FEE_LAMPORTS)/LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL\\. \n\n` +
                                     `Please enter the amount of SOL you wish to withdraw:`,
                                     { parse_mode: 'MarkdownV2' });

    } catch (error) {
        console.error(`${logPrefix} Error starting withdrawal: ${error.message}`);
        await safeSendMessage(chatId, `❌ Error starting withdrawal process\\. Please try again later\\.`, { parse_mode: 'MarkdownV2' });
    }
}


// --- Game Command Handlers (Initiate Input Flows) ---

async function handleCoinflipCommand(msg, args) {
    const chatId = msg.chat.id;
    const userId = String(msg.from.id);
    userStateCache.set(userId, { state: 'awaiting_cf_amount', chatId: String(chatId), messageId: msg.message_id, timestamp: Date.now(), data: {} });
    await safeSendMessage(chatId, `🪙 Enter Coinflip bet amount \\(SOL\\):`, { parse_mode: 'MarkdownV2' });
}

async function handleRaceCommand(msg, args) {
     const chatId = msg.chat.id;
     const userId = String(msg.from.id);
     userStateCache.set(userId, { state: 'awaiting_race_amount', chatId: String(chatId), messageId: msg.message_id, timestamp: Date.now(), data: {} });
     await safeSendMessage(chatId, `🐎 Enter Race bet amount \\(SOL\\):`, { parse_mode: 'MarkdownV2' });
}
async function handleSlotsCommand(msg, args) {
    const chatId = msg.chat.id;
    const userId = String(msg.from.id);
    userStateCache.set(userId, { state: 'awaiting_slots_amount', chatId: String(chatId), messageId: msg.message_id, timestamp: Date.now(), data: {} });
    await safeSendMessage(chatId, `🎰 Enter Slots bet amount \\(SOL\\):`, { parse_mode: 'MarkdownV2' });
}
async function handleRouletteCommand(msg, args) {
    const chatId = msg.chat.id;
    const userId = String(msg.from.id);
     // Roulette input is complex, maybe just prompt amount first then explain bet placement?
     // Or link to a Mini App / separate betting interface?
     // For now, just ask for *total* bet amount and simplify betting later.
    // userStateCache.set(userId, { state: 'awaiting_roulette_amount', chatId: String(chatId), messageId: msg.message_id, timestamp: Date.now(), data: {} });
    // await safeSendMessage(chatId, `⚪️ Enter *total* Roulette bet amount \\(SOL\\) you wish to place across different spots:`, { parse_mode: 'MarkdownV2'});
     await safeSendMessage(chatId, `⚪️ Roulette betting via text is limited\\. A better interface is planned\\. Use /help for now\\.`, { parse_mode: 'MarkdownV2'});

}
async function handleWarCommand(msg, args) {
    const chatId = msg.chat.id;
    const userId = String(msg.from.id);
    userStateCache.set(userId, { state: 'awaiting_war_amount', chatId: String(chatId), messageId: msg.message_id, timestamp: Date.now(), data: {} });
    await safeSendMessage(chatId, `🃏 Enter Casino War bet amount \\(SOL\\):`, { parse_mode: 'MarkdownV2' });
}


// --- Admin Command --- (Basic structure, needs detailed implementation based on requirements)
async function handleAdminCommand(msg, args) {
    const chatId = msg.chat.id;
    const userId = String(msg.from.id); // Already verified by caller
    const logPrefix = `[AdminCmd User ${userId}]`;

    const subCommand = args?.split(' ')[0]?.toLowerCase();
    const subArgs = args?.split(' ').slice(1) || [];

    if (!subCommand) {
        await safeSendMessage(chatId, "Admin Usage: `/admin <command> [args]`\nCommands: `status`, `getbalance`, `getconfig`, `setloglevel <level>`, `finduser <id_or_ref>`", { parse_mode: 'MarkdownV2' });
        return;
    }

    console.log(`${logPrefix} Executing: ${subCommand} ${subArgs.join(' ')}`);

    try {
        switch (subCommand) {
            case 'status': {
                 // TODO: Implement detailed status: queue sizes, cache sizes, DB connections, RPC status, monitor status
                 const msgQueueSize = messageQueue.size;
                 const cbQueueSize = callbackQueue.size;
                 const payoutQueueSize = payoutProcessorQueue.size;
                 const depositQSize = depositProcessorQueue.size;
                 const tgSendQSize = telegramSendQueue.size;
                 const walletCacheSize = walletCache.size;
                 const depAddrCacheSize = activeDepositAddresses.size;
                 const processedTxCacheSize = processedDepositTxSignatures.size;
                 const dbPoolTotal = pool.totalCount;
                 const dbPoolIdle = pool.idleCount;
                 const dbPoolWaiting = pool.waitingCount;

                 let statusMsg = `*Bot Status* (v3\\.1\\.0) - ${new Date().toISOString()}\n\n` +
                                 `*Queues*:\n` +
                                 `  Msg: ${msgQueueSize}, Callback: ${cbQueueSize}, Payout: ${payoutQueueSize}, Deposit: ${depositQSize}, TG Send: ${tgSendQSize}\n`+
                                 `*Caches*:\n` +
                                 `  User States: ${userStateCache.size}, Wallets: ${walletCacheSize}, Dep Addr: ${depAddrCacheSize}, Proc TXs: ${processedTxCacheSize}/${MAX_PROCESSED_TX_CACHE_SIZE}\n`+
                                 `*DB Pool*:\n` +
                                 `  Total: ${dbPoolTotal}, Idle: ${dbPoolIdle}, Waiting: ${dbPoolWaiting}\n` +
                                 `*State*: Initialized: ${isFullyInitialized}\n`
                                 // Add RPC connection status if available from RateLimitedConnection
                                 ;
                 await safeSendMessage(chatId, statusMsg, { parse_mode: 'MarkdownV2' });
                 break;
            }
            case 'getbalance': {
                // TODO: Implement fetching balance of MAIN and REFERRAL payout wallets
                 const keyTypes = ['main', 'referral'];
                 let balanceMsg = '*Payout Wallet Balances:*\n';
                 for (const type of keyTypes) {
                      let keyEnvVar = '';
                      let keyDesc = '';
                      if (type === 'main') { keyEnvVar = 'MAIN_BOT_PRIVATE_KEY'; keyDesc = 'Main';}
                      else if (type === 'referral') { keyEnvVar = process.env.REFERRAL_PAYOUT_PRIVATE_KEY ? 'REFERRAL_PAYOUT_PRIVATE_KEY' : 'MAIN_BOT_PRIVATE_KEY'; keyDesc = `Referral (${process.env.REFERRAL_PAYOUT_PRIVATE_KEY ? 'Dedicated' : 'Using Main'})`;}

                      const pk = process.env[keyEnvVar];
                      if (pk) {
                          try {
                               const keypair = Keypair.fromSecretKey(bs58.decode(pk));
                               const balance = await solanaConnection.getBalance(keypair.publicKey);
                               balanceMsg += `  \\- ${keyDesc} (${keypair.publicKey.toBase58().slice(0,6)}\\.{}\\.\\): ${escapeMarkdownV2((Number(balance)/LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL\n`;
                          } catch (e) {
                               balanceMsg += `  \\- ${keyDesc}: Error fetching balance - ${escapeMarkdownV2(e.message)}\n`;
                          }
                      } else {
                           balanceMsg += `  \\- ${keyDesc}: Private key not set\n`;
                      }
                 }
                await safeSendMessage(chatId, balanceMsg, { parse_mode: 'MarkdownV2' });
                break;
            }
            case 'getconfig': {
                // TODO: Show non-secret env vars + potentially game config
                 let configText = "*Current Config (Non\\-Secret Env Vars):*\n\n";
                 const sensitiveKeywords = ['TOKEN', 'DATABASE_URL', 'KEY', 'SECRET', 'PASSWORD', 'SEED'];
                 const allVars = {...OPTIONAL_ENV_DEFAULTS, ...process.env}; // Combine defaults and actual env
                 for (const key in allVars) {
                     if (REQUIRED_ENV_VARS.includes(key) || OPTIONAL_ENV_DEFAULTS.hasOwnProperty(key)) {
                          const isSensitive = sensitiveKeywords.some(k => key.toUpperCase().includes(k));
                          const value = process.env[key] !== undefined ? process.env[key] : `(Default: ${OPTIONAL_ENV_DEFAULTS[key]})`;
                          configText += `\\- ${escapeMarkdownV2(key)}: ${isSensitive && key !== 'RPC_URLS' ? '[Set, Redacted]' : escapeMarkdownV2(value)}\n`;
                     }
                 }
                 configText += "\n*Game Config Limits (Lamports):*\n" + escapeMarkdownV2(JSON.stringify(GAME_CONFIG, (k, v) => typeof v === 'bigint' ? v.toString() : v, 2));
                await safeSendMessage(chatId, configText.substring(0, 4090), { parse_mode: 'MarkdownV2' }); // Substring to avoid length limits
                break;
            }
            case 'setloglevel': // Example
            case 'finduser': // Example
                 await safeSendMessage(chatId, `Admin command \`${escapeMarkdownV2(subCommand)}\` not fully implemented yet\\.`, { parse_mode: 'MarkdownV2'});
                 break;

            default:
                await safeSendMessage(chatId, `Unknown admin command: \`${escapeMarkdownV2(subCommand)}\``, { parse_mode: 'MarkdownV2' });
        }
    } catch (adminError) {
        console.error(`${logPrefix} Admin command error (${subCommand}):`, adminError);
        await safeSendMessage(chatId, `Error executing admin command \`${escapeMarkdownV2(subCommand)}\`: \`${escapeMarkdownV2(adminError.message)}\``, { parse_mode: 'MarkdownV2' });
    }
}


// --- Referral Check & Payout Triggering Logic ---

/**
 * Handles checking for and processing referral bonuses after a bet completes.
 * @param {string} refereeUserId The user who completed the bet.
 * @param {number} completedBetId The ID of the completed bet.
 * @param {bigint} wagerAmountLamports The amount wagered in the completed bet.
 * @param {string} logPrefix Optional logging prefix.
 */
async function _handleReferralChecks(refereeUserId, completedBetId, wagerAmountLamports, logPrefix = '') {
     refereeUserId = String(refereeUserId);
     logPrefix = logPrefix || `[ReferralCheck Bet ${completedBetId} User ${refereeUserId}]`;
     console.log(`${logPrefix} Starting referral checks.`);

     let client = null;
     try {
          // 1. Get Referee details (needs referrer_user_id, total_wagered, last_milestone)
          const refereeDetails = await getUserWalletDetails(refereeUserId);
          if (!refereeDetails?.referred_by_user_id) {
              // console.log(`${logPrefix} User not referred. No referral payouts needed.`);
              // Still need to update their wager stats
              client = await pool.connect();
              await client.query('BEGIN');
              await updateUserWagerStats(refereeUserId, wagerAmountLamports, client);
              await client.query('COMMIT');
              return;
          }

          const referrerUserId = refereeDetails.referred_by_user_id;

          // 2. Get Referrer details (needed for their wallet and referral count for initial bonus)
          const referrerDetails = await getUserWalletDetails(referrerUserId);
          if (!referrerDetails?.external_withdrawal_address) {
               console.warn(`${logPrefix} Referrer ${referrerUserId} has no linked wallet. Cannot process potential payouts.`);
               // Still update referee wager stats
               client = await pool.connect();
               await client.query('BEGIN');
               await updateUserWagerStats(refereeUserId, wagerAmountLamports, client);
               await client.query('COMMIT');
               return;
          }

          // --- Transaction for all referral updates ---
          client = await pool.connect();
          await client.query('BEGIN');

          let newLastMilestonePaid = refereeDetails.last_milestone_paid_lamports; // Start with current value

          // 3. Check for Initial Bet Bonus
          const isFirst = await isFirstCompletedBet(refereeUserId, completedBetId); // Check using pool (doesn't need TX lock)
          if (isFirst && wagerAmountLamports >= REFERRAL_INITIAL_BET_MIN_LAMPORTS) {
              console.log(`${logPrefix} First qualifying bet detected.`);
              // Determine bonus tier based on referrer's count *before* this referral completed signup (approximate with current count)
              const referrerCount = referrerDetails.referral_count || 0;
              let bonusTier = REFERRAL_INITIAL_BONUS_TIERS[REFERRAL_INITIAL_BONUS_TIERS.length - 1]; // Default to highest
              for (const tier of REFERRAL_INITIAL_BONUS_TIERS) {
                  if (referrerCount <= tier.maxCount) {
                      bonusTier = tier;
                      break;
                  }
              }
              const initialBonusAmount = wagerAmountLamports * BigInt(Math.round(bonusTier.percent * 100)) / 100n; // Use BigInt math

              if (initialBonusAmount > 0n) {
                   console.log(`${logPrefix} Calculating initial bonus: ${referrerCount} refs -> ${bonusTier.percent*100}% tier. Bonus: ${initialBonusAmount} lamports.`);
                   // Record pending payout
                   const payoutRecord = await recordPendingReferralPayout(
                       referrerUserId, refereeUserId, 'initial_bet', initialBonusAmount, completedBetId
                   ); // Uses pool, fine outside TX for initial record
                   if (payoutRecord.success && payoutRecord.payoutId) {
                        // Queue the actual payout job
                        await addPayoutJob(
                            { type: 'payout_referral', payoutId: payoutRecord.payoutId },
                            parseInt(process.env.PAYOUT_JOB_RETRIES, 10),
                            parseInt(process.env.PAYOUT_JOB_RETRY_DELAY_MS, 10)
                        );
                        console.log(`${logPrefix} Initial bonus payout job queued (ID: ${payoutRecord.payoutId}).`);
                   } else if (payoutRecord.error !== 'Duplicate milestone payout attempt.') { // Avoid error log for expected duplicates
                        console.error(`${logPrefix} Failed to record initial bonus payout: ${payoutRecord.error}`);
                   }
              }
          }

          // 4. Update Referee's Total Wagered (do this before milestone check)
          const newTotalWagered = (refereeDetails.total_wagered || 0n) + wagerAmountLamports;

          // 5. Check for Milestone Bonus
          let milestoneCrossed = null;
          for (const threshold of REFERRAL_MILESTONE_THRESHOLDS_LAMPORTS) {
               // Check if new total wager crossed a threshold AND that threshold is higher than the last one paid for
               if (newTotalWagered >= threshold && threshold > refereeDetails.last_milestone_paid_lamports) {
                    milestoneCrossed = threshold; // Mark the highest threshold crossed this time
               } else if (newTotalWagered < threshold) {
                    break; // Thresholds are sorted, no need to check higher ones
               }
          }

          if (milestoneCrossed) {
              console.log(`${logPrefix} Milestone ${milestoneCrossed} lamports wagered reached.`);
              const milestoneBonusAmount = milestoneCrossed * BigInt(Math.round(REFERRAL_MILESTONE_REWARD_PERCENT * 10000)) / 10000n; // Use BigInt math

              if (milestoneBonusAmount > 0n) {
                  console.log(`${logPrefix} Calculating milestone bonus (${REFERRAL_MILESTONE_REWARD_PERCENT*100}% of ${milestoneCrossed}): ${milestoneBonusAmount} lamports.`);
                  // Record pending payout
                  const payoutRecord = await recordPendingReferralPayout(
                      referrerUserId, refereeUserId, 'milestone', milestoneBonusAmount, null, milestoneCrossed
                  );
                  if (payoutRecord.success && payoutRecord.payoutId) {
                      // Queue the actual payout job
                      await addPayoutJob(
                          { type: 'payout_referral', payoutId: payoutRecord.payoutId },
                          parseInt(process.env.PAYOUT_JOB_RETRIES, 10),
                          parseInt(process.env.PAYOUT_JOB_RETRY_DELAY_MS, 10)
                      );
                      console.log(`${logPrefix} Milestone bonus payout job queued (ID: ${payoutRecord.payoutId}).`);
                      // Update the last milestone paid marker
                      newLastMilestonePaid = milestoneCrossed;
                  } else if (payoutRecord.error !== 'Duplicate milestone payout attempt.') {
                        console.error(`${logPrefix} Failed to record milestone bonus payout: ${payoutRecord.error}`);
                   }
              }
          }

          // 6. Update the referee's wager stats in the DB transaction
          // Pass the new milestone threshold if it was updated
          const statsUpdated = await updateUserWagerStats(refereeUserId, wagerAmountLamports, client, newLastMilestonePaid);
          if (!statsUpdated) {
               // If this fails, we should probably rollback to avoid inconsistent state?
               console.error(`${logPrefix} CRITICAL: Failed to update referee wager stats after processing bonuses. Rolling back.`);
               await client.query('ROLLBACK');
               // Maybe notify admin?
          } else {
               // All good, commit the transaction (which includes the wager stat update)
               await client.query('COMMIT');
               console.log(`${logPrefix} Referral checks completed. New total wagered: ${newTotalWagered}. Last milestone paid: ${newLastMilestonePaid}.`);
          }

     } catch (error) {
          console.error(`${logPrefix} Error during referral checks: ${error.message}`);
          if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { /* ignore */ } }
          // Don't block game completion, but log the error
     } finally {
          if (client) client.release();
     }
}


// --- End of Part 5 ---
// index.js - Part 6: Background Tasks, Payouts, Startup & Shutdown
// --- VERSION: 3.1.0 --- (Revised DB Init)

// --- Payout Job Handling ---

/**
 * Adds a job to the payout queue with retry logic.
 * @param {object} jobDetails Contains type ('payout_withdrawal' or 'payout_referral') and data (withdrawalId/payoutId, userId).
 * @param {number} maxRetries Number of times to retry on failure.
 * @param {number} baseRetryDelayMs Initial delay before first retry.
 */
async function addPayoutJob(jobDetails, maxRetries, baseRetryDelayMs) {
    const jobId = jobDetails.withdrawalId || jobDetails.payoutId || 'N/A';
    const jobType = jobDetails.type;
    const logPrefix = `[PayoutJob ${jobType} ID:${jobId}]`;

    payoutProcessorQueue.add(async () => {
        let attempt = 0;
        while (attempt <= maxRetries) {
            try {
                console.log(`${logPrefix} Attempt ${attempt + 1}/${maxRetries + 1} starting...`);
                if (jobType === 'payout_withdrawal') {
                    await handleWithdrawalPayoutJob(jobDetails);
                } else if (jobType === 'payout_referral') {
                    await handleReferralPayoutJob(jobDetails);
                } else {
                    console.error(`${logPrefix} Unknown job type: ${jobType}`);
                    // Don't retry unknown types
                    return;
                }
                console.log(`${logPrefix} Attempt ${attempt + 1} successful.`);
                return; // Success, exit loop

            } catch (error) {
                attempt++;
                const isRetryable = isRetryableSolanaError(error); // Use our Solana-specific checker
                console.warn(`${logPrefix} Attempt ${attempt}/${maxRetries + 1} failed. Retryable: ${isRetryable}. Error: ${error.message}`);

                if (!isRetryable || attempt > maxRetries) {
                    console.error(`${logPrefix} Final attempt failed or error not retryable. Marking job as failed.`);
                    try {
                        // Mark as failed in DB - Use the standalone update functions
                        if (jobType === 'payout_withdrawal' && jobDetails.withdrawalId) {
                            await updateWithdrawalStatus(jobDetails.withdrawalId, 'failed', null, null, `Max retries exceeded or non-retryable error: ${error.message}`);
                        } else if (jobType === 'payout_referral' && jobDetails.payoutId) {
                            await updateReferralPayoutStatus(jobDetails.payoutId, 'failed', null, null, `Max retries exceeded or non-retryable error: ${error.message}`);
                        }
                    } catch (dbError) {
                        console.error(`${logPrefix} CRITICAL: Failed to mark job as failed in DB after final error: ${dbError.message}`);
                        // Notify admin urgently here
                    }
                    // Notify user about final failure
                    if (jobDetails.userId) {
                        const errorType = jobType === 'payout_withdrawal' ? 'Withdrawal' : 'Referral Payout';
                        await safeSendMessage(jobDetails.userId, `⚠️ Your ${errorType} (ID: ${jobId}) could not be completed after multiple attempts\\. Please contact support\\. Error: ${escapeMarkdownV2(error.message)}`, { parse_mode: 'MarkdownV2' });
                    }
                    break; // Exit retry loop
                }

                // Calculate delay with exponential backoff and jitter
                const jitter = baseRetryDelayMs * 0.1 * (Math.random() - 0.5); // +/- 5% jitter
                const delay = (baseRetryDelayMs * Math.pow(2, attempt - 1)) + jitter;
                console.log(`${logPrefix} Retrying in ${Math.round(delay / 1000)}s...`);
                await new Promise(resolve => setTimeout(resolve, delay));
            }
        }
    }).catch(queueError => {
        // Catch errors from adding the job itself (e.g., queue timed out immediately)
        console.error(`${logPrefix} Error adding/processing job in payout queue:`, queueError.message);
        // Attempt to mark as failed as a last resort
         try {
            if (jobType === 'payout_withdrawal' && jobDetails.withdrawalId) updateWithdrawalStatus(jobDetails.withdrawalId, 'failed', null, null, `Queue processing error: ${queueError.message}`);
            else if (jobType === 'payout_referral' && jobDetails.payoutId) updateReferralPayoutStatus(jobDetails.payoutId, 'failed', null, null, `Queue processing error: ${queueError.message}`);
         } catch (dbError) { console.error(`${logPrefix} Failed to mark job as failed after queue error: ${dbError.message}`); }
    });
}

/** Handles withdrawal payout jobs. Throws error on failure for retry logic. */
async function handleWithdrawalPayoutJob(job) {
    const { withdrawalId, userId } = job;
    const logPrefix = `[WithdrawalJob ID:${withdrawalId}]`;
    let client = null; // DB client for transaction

    console.log(`${logPrefix} Starting payout processing...`);
    const details = await getWithdrawalDetails(withdrawalId); // Fetch details

    if (!details) {
        console.error(`${logPrefix} Withdrawal record not found.`);
        throw new Error("Withdrawal record not found. Not retryable."); // Throw non-retryable error
    }
    // Allow processing if 'pending' or 'processing' (in case retry starts after marking processing)
    if (details.status !== 'pending' && details.status !== 'processing') {
        console.warn(`${logPrefix} Job started but status is already '${details.status}'. Skipping.`);
        return; // Don't process if already completed/failed
    }

    // Mark as processing (standalone update)
    await updateWithdrawalStatus(withdrawalId, 'processing');

    try {
        const recipient = details.recipient_address;
        const amountToSend = details.final_send_amount_lamports; // Amount to actually send
        const requestedAmount = details.requested_amount_lamports;
        const feeAmount = details.fee_lamports;
        const totalDeduction = requestedAmount + feeAmount; // Amount deducted from internal balance

        // --- Send the SOL ---
        const sendResult = await sendSol(recipient, amountToSend, 'withdrawal');

        if (sendResult.success && sendResult.signature) {
            const signature = sendResult.signature;
            console.log(`${logPrefix} On-chain send successful: ${signature}. Updating database...`);

            // --- Transaction: Deduct Balance & Mark Completed ---
            client = await pool.connect();
            await client.query('BEGIN');

            // 1. Update balance and add ledger entry for withdrawal AND fee
            const balanceUpdateResult = await updateUserBalanceAndLedger(
                client, details.user_id, -totalDeduction, 'withdrawal', { withdrawalId: withdrawalId },
                `To: ${recipient.slice(0, 6)}... Fee: ${Number(feeAmount) / LAMPORTS_PER_SOL} SOL`
            );

            if (!balanceUpdateResult.success) {
                 // CRITICAL: Sent SOL but couldn't deduct internally.
                 await client.query('ROLLBACK'); // Rollback any potential partial changes
                 client.release(); client = null; // Release client
                 console.error(`${logPrefix} CRITICAL ERROR: Sent ${amountToSend} lamports (TX: ${signature}) but failed to deduct internal balance for user ${details.user_id}. Reason: ${balanceUpdateResult.error}`);
                 // Mark withdrawal as failed - this state requires manual intervention
                 await updateWithdrawalStatus(withdrawalId, 'failed', null, null, `CRITICAL: Internal balance deduction failed after successful send (TX: ${signature})`);
                 // Notify Admin!
                 // Throw a specific error to prevent retry of this state
                 throw new Error("Internal balance deduction failed post-send. Requires manual resolution. Not retryable.");
            }

            // 2. Update withdrawal record to completed status with signature
            const statusUpdated = await updateWithdrawalStatus(withdrawalId, 'completed', client, signature);
            if (!statusUpdated) {
                 // Also CRITICAL: Sent and deducted, but couldn't mark withdrawal complete.
                 console.error(`${logPrefix} CRITICAL ERROR: Sent ${amountToSend} lamports (TX: ${signature}), deducted balance, but failed to mark withdrawal as completed.`);
                 // Rollback balance deduction to keep state consistent (forces retry or manual check)
                 await client.query('ROLLBACK');
                 client.release(); client = null;
                 // Throw error, likely retryable (e.g., temporary DB issue preventing status update)
                 throw new Error(`Failed to mark withdrawal completed after send/deduction (TX: ${signature}). Retryable.`);
            }

            // --- Commit Transaction ---
            await client.query('COMMIT');
            console.log(`${logPrefix} Successfully processed, balance deducted, record updated.`);

            // Notify user AFTER successful commit
            await safeSendMessage(details.user_id, `✅ Withdrawal complete\\! ${escapeMarkdownV2((Number(amountToSend) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL sent to \`${escapeMarkdownV2(recipient)}\`\\.\nTX: [View](https://solscan.io/tx/${escapeMarkdownV2(signature)})`, { parse_mode: 'MarkdownV2', disable_web_page_preview: true });

        } else {
            // sendSol failed
            console.error(`${logPrefix} sendSol failed. Error: ${sendResult.error}`);
            // Throw the error from sendSol - retry logic in addPayoutJob will handle it
            const error = new Error(sendResult.error || "sendSol failed without specific error message");
            // Manually add retryable status if sendSol provided it
            if (sendResult.isRetryable !== undefined) { error.isRetryable = sendResult.isRetryable; }
            throw error;
        }
    } catch (error) {
        // Catch errors from sendSol OR the DB transaction block
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { /* ignore */ } }
        console.error(`${logPrefix} Processing failed during execution. Error: ${error.message}`);
        // Re-throw the error to be caught by the retry logic in addPayoutJob
        throw error;
    } finally {
        if (client) client.release();
    }
}

/** Handles referral payout jobs. Throws error on failure for retry logic. */
async function handleReferralPayoutJob(job) {
    const { payoutId } = job; // Get needed data from job (might contain more than just ID)
    const logPrefix = `[RefPayoutJob ID:${payoutId}]`;

    console.log(`${logPrefix} Starting payout processing...`);
    const details = await getReferralPayoutDetails(payoutId); // Fetch details

    if (!details) {
        console.error(`${logPrefix} Referral payout record not found.`);
        throw new Error("Referral payout record not found. Not retryable.");
    }
    if (details.status !== 'pending' && details.status !== 'processing') {
        console.warn(`${logPrefix} Job started but status is already '${details.status}'. Skipping.`);
        return;
    }

    const referrerUserId = details.referrer_user_id;
    const payoutAmount = details.payout_amount_lamports;
    const payoutSOL = (Number(payoutAmount) / LAMPORTS_PER_SOL);

    // Mark as processing
    await updateReferralPayoutStatus(payoutId, 'processing');

    try {
        // 1. Get referrer's linked wallet
        const recipientAddress = await getLinkedWallet(referrerUserId);
        if (!recipientAddress) {
            console.error(`${logPrefix} Cannot process payout. Referrer user ${referrerUserId} has no linked wallet.`);
            // Mark as failed, not retryable as user needs to link wallet
            await updateReferralPayoutStatus(payoutId, 'failed', null, null, 'Referrer has no linked wallet set.');
            // Don't throw, just exit processing for this job
            return;
        }

        console.log(`${logPrefix} Attempting send ${payoutSOL.toFixed(SOL_DECIMALS)} SOL to referrer ${referrerUserId} (${recipientAddress.slice(0,6)}...)`);

        // 2. Send the SOL
        const sendResult = await sendSol(recipientAddress, payoutAmount, 'referral');

        if (sendResult.success && sendResult.signature) {
            const signature = sendResult.signature;
            // 3. Mark payout record as 'paid' with signature
            const recorded = await updateReferralPayoutStatus(payoutId, 'paid', null, signature); // Use pool directly for final status update
            if (!recorded) {
                 // CRITICAL: Payment sent, but failed to record 'paid' status. Requires manual check.
                 console.error(`${logPrefix} CRITICAL ERROR - Referral payment sent (TX: ${signature}) but FAILED to record 'paid' status! Manual check needed.`);
                 // Don't throw error here to prevent retry loop, but log severity
                 // Notify Admin!
                 // Consider alternative recovery or logging mechanism
            } else {
                 console.log(`${logPrefix} SUCCESS! ✅ Referral payout complete. TX: ${signature}`);
                 // 4. Notify Referrer (Best effort)
                 try {
                      await safeSendMessage(referrerUserId, `💰 You received a referral reward of ${escapeMarkdownV2(payoutSOL.toFixed(SOL_DECIMALS))} SOL\\! Check your linked wallet: \`${escapeMarkdownV2(recipientAddress)}\``, { parse_mode: 'MarkdownV2' });
                 } catch (notifyError) {
                      console.warn(`${logPrefix} Failed to send referral payout notification to user ${referrerUserId}: ${notifyError.message}`);
                 }
            }
        } else {
            // sendSol failed
             console.error(`${logPrefix} sendSol failed. Error: ${sendResult.error}`);
             const error = new Error(sendResult.error || "sendSol failed without specific error message");
             if (sendResult.isRetryable !== undefined) { error.isRetryable = sendResult.isRetryable; }
             throw error; // Throw for retry logic
        }
    } catch (error) {
         console.error(`${logPrefix} Processing failed during execution. Error: ${error.message}`);
         // Re-throw error for retry logic
         throw error;
    }
    // No client release needed as DB updates use pool directly or handle their own client lifecycle if modified
}


// --- Deposit Monitoring Task (Polling Method - NOT SCALABLE FOR PRODUCTION) ---

/**
 * Periodically checks pending deposit addresses for incoming transactions (Polling Method).
 * This method is NOT recommended for production due to RPC limitations. Use indexers/webhooks instead.
 */
async function monitorDepositsPolling() {
    if (!isFullyInitialized) {
        // console.log("[DepositMonitor] Bot not fully initialized, skipping check cycle.");
        return;
    }
     // Check if a cycle is already potentially running (basic guard, not foolproof)
     // A more robust lock mechanism might be needed if cycles can overlap significantly
    if (monitorDepositsPolling.isRunning) {
         console.log("[DepositMonitor] Previous check cycle might still be running, skipping.");
         return;
    }
    monitorDepositsPolling.isRunning = true;

    const logPrefix = "[DepositMonitor Polling]";
    // console.log(`${logPrefix} Starting deposit check cycle...`); // Less verbose logging
    const startTime = Date.now();
    let checkedAddresses = 0;
    let foundSignatures = 0;

    try {
        // 1. Fetch a batch of active, non-expired addresses from DB
        const batchSize = parseInt(process.env.DEPOSIT_MONITOR_ADDRESS_BATCH_SIZE, 10);
        const pendingAddresses = await queryDatabase(
            `SELECT id, user_id, deposit_address, expires_at FROM deposit_addresses WHERE status = 'pending' AND expires_at > NOW() ORDER BY created_at ASC LIMIT $1`,
            [batchSize]
        ).then(res => res.rows);

        if (pendingAddresses.length === 0) {
            // console.log(`${logPrefix} No pending addresses to check in this cycle.`);
            monitorDepositsPolling.isRunning = false;
            return;
        }
        // console.log(`${logPrefix} Checking ${pendingAddresses.length} pending addresses...`); // Less verbose

        // 2. For each address, check for recent signatures
        const sigFetchLimit = parseInt(process.env.DEPOSIT_MONITOR_SIGNATURE_FETCH_LIMIT, 10);
        for (const addrInfo of pendingAddresses) {
            checkedAddresses++;
            try {
                const pubKey = new PublicKey(addrInfo.deposit_address);
                // Fetch recent signatures confirmed on the network
                const signaturesInfo = await solanaConnection.getSignaturesForAddress(
                    pubKey,
                    { limit: sigFetchLimit }, // Removed commitment here, rely on connection default
                );

                for (const sigInfo of signaturesInfo) {
                    // Check if successful (no error) and not already processed this session
                    if (!sigInfo.err && !hasProcessedDepositTx(sigInfo.signature)) {
                        foundSignatures++;
                        console.log(`${logPrefix} Potential deposit found for addr ${addrInfo.deposit_address.slice(0,6)}... TX: ${sigInfo.signature.slice(0,10)}... Queuing for processing.`);
                        // Add job to the dedicated deposit processing queue
                        depositProcessorQueue.add(() => processDepositTransaction(sigInfo.signature, addrInfo.deposit_address))
                            .catch(e => console.error(`${logPrefix} Error queueing deposit processing for ${sigInfo.signature}: ${e.message}`));
                        // Add to cache immediately to prevent re-queueing in the same cycle or near future
                        addProcessedDepositTx(sigInfo.signature);
                    } else if (hasProcessedDepositTx(sigInfo.signature)){
                         // console.log(`${logPrefix} Signature ${sigInfo.signature.slice(0,6)} already processed/cached.`); // Verbose
                    }
                }
                // Optional: Small delay between addresses if hitting rate limits hard
                // await new Promise(r => setTimeout(r, 50));

            } catch (error) {
                // Don't stop the whole loop, just log error for this address
                console.error(`${logPrefix} Error checking address ${addrInfo.deposit_address}: ${error.message}`);
                // Check if error is rate limit (429) - maybe pause briefly?
                 if (isRetryableSolanaError(error) && String(error.message).includes('429')) {
                      console.warn(`${logPrefix} Hit rate limit checking address. Pausing briefly...`);
                      await new Promise(r => setTimeout(r, 1500)); // Pause 1.5s on rate limit
                 }
            }
        } // End loop through addresses

    } catch (error) {
        console.error(`${logPrefix} Error during deposit monitoring cycle:`, error);
    } finally {
        const duration = Date.now() - startTime;
        if (checkedAddresses > 0) { // Only log if work was done
            console.log(`${logPrefix} Cycle finished in ${duration}ms. Checked: ${checkedAddresses}. Found/Queued: ${foundSignatures}.`);
        }
        monitorDepositsPolling.isRunning = false; // Release lock
    }
}
monitorDepositsPolling.isRunning = false; // Initialize lock state


/**
 * Processes a deposit transaction found by the monitor. Triggered by depositProcessorQueue.
 * @param {string} signature The transaction signature.
 * @param {string} depositAddress The unique deposit address the transaction was sent to.
 */
async function processDepositTransaction(signature, depositAddress) {
    const logPrefix = `[ProcessDeposit TX:${signature.slice(0, 6)} Addr:${depositAddress.slice(0,6)}]`;
    console.log(`${logPrefix} Processing potential deposit...`);
    let client = null; // DB client for transaction

    try {
        // 0. Double-check if processed (queue might have duplicates if added quickly)
        if (hasProcessedDepositTx(signature)) {
            console.log(`${logPrefix} Signature already processed/cached. Skipping.`);
            return;
        }

        // 1. Fetch transaction details
        const tx = await solanaConnection.getTransaction(signature, {
            maxSupportedTransactionVersion: 0, // Specify version for reliable parsing
            commitment: DEPOSIT_CONFIRMATION_LEVEL
        });

        if (!tx) { throw new Error("Transaction not found via RPC."); }
        if (tx.meta?.err) {
            console.log(`${logPrefix} Transaction failed on-chain. Ignoring. Error: ${JSON.stringify(tx.meta.err)}`);
            addProcessedDepositTx(signature); // Cache failed TX sig to prevent reprocessing
            return;
        }

        // 2. Find associated user and validate deposit address
        const addrInfo = await findDepositAddressInfo(depositAddress); // Uses cache/DB
        if (!addrInfo) { throw new Error(`Deposit address record not found for ${depositAddress}. Ignoring TX.`); }

        const { userId, status: addrStatus, id: depositAddressId, expiresAt } = addrInfo;

        if (addrStatus !== 'pending') { throw new Error(`Deposit address status is '${addrStatus}', not 'pending'. Ignoring TX.`); }
        if (new Date() > expiresAt) { throw new Error(`Deposit address has expired. Ignoring TX.`); }

        // 3. Analyze amount transferred *to the unique address*
        const { transferAmount } = analyzeTransactionAmounts(tx, depositAddress);
        if (transferAmount <= 0n) { throw new Error("No positive SOL transfer amount found to the deposit address in transaction analysis."); }

        console.log(`${logPrefix} Confirmed ${Number(transferAmount) / LAMPORTS_PER_SOL} SOL deposit for user ${userId}. Processing DB updates...`);

        // --- 4. Process Deposit within DB Transaction ---
        client = await pool.connect();
        await client.query('BEGIN');

        // 4a. Ensure user/balance records exist (should be redundant but safe)
        await ensureUserExists(userId, client);

        // 4b. Credit user's internal balance & Add ledger entry
        const balanceUpdateResult = await updateUserBalanceAndLedger(
            client, userId, transferAmount, 'deposit',
            { depositId: null }, // Placeholder, will update below
            `Deposit TX: ${signature.slice(0, 10)}...`
        );
        if (!balanceUpdateResult.success) { throw new Error(`Failed to update balance: ${balanceUpdateResult.error}`); }

        // 4c. Record the confirmed deposit
        const depositRecordResult = await recordConfirmedDeposit(
            client, userId, depositAddressId, signature, transferAmount
        );
        if (!depositRecordResult.success || !depositRecordResult.depositId) { throw new Error(depositRecordResult.error || "Failed to record confirmed deposit in DB."); }
        const newDepositId = depositRecordResult.depositId;

        // 4d. Update the ledger entry with the new deposit ID
        // Find the specific ledger entry just created (match user, type, amount, null related_deposit_id)
        const ledgerUpdateRes = await queryDatabase(
             `UPDATE ledger SET related_deposit_id = $1
              WHERE user_id = $2 AND transaction_type = 'deposit' AND amount_lamports = $3 AND related_deposit_id IS NULL
              ORDER BY created_at DESC LIMIT 1`, // Target the most recent matching entry
             [newDepositId, userId, transferAmount],
             client
         );
         if (ledgerUpdateRes.rowCount === 0) {
              console.warn(`${logPrefix} Could not find matching ledger entry to update related_deposit_id for deposit ${newDepositId}.`);
              // This might indicate a logic issue or race condition, but proceed for now.
         }


        // 4e. Mark deposit address as used
        const markedUsed = await markDepositAddressUsed(client, depositAddressId);
        if (!markedUsed) { throw new Error(`Failed to mark deposit address ID ${depositAddressId} as used (was status pending?).`); }

        // 4f. Check for Referral Linking on First Deposit
        // Count *committed* deposits for this user before this one
        const depositCountRes = await queryDatabase('SELECT COUNT(*) as count FROM deposits WHERE user_id = $1', [userId], client);
        const isFirstDeposit = parseInt(depositCountRes.rows[0].count, 10) === 1; // Count includes the one we just inserted

        if (isFirstDeposit) {
            console.log(`${logPrefix} First deposit detected for user ${userId}. Checking for pending referral...`);
            const pending = pendingReferrals.get(userId);
            const now = Date.now();
            if (pending && (now - pending.timestamp < PENDING_REFERRAL_TTL_MS)) {
                const referrerUserId = pending.referrerUserId;
                const linked = await linkReferral(userId, referrerUserId, client); // Attempt to link in DB TX
                if (linked) {
                    console.log(`${logPrefix} Linked user ${userId} to referrer ${referrerUserId}.`);
                    pendingReferrals.delete(userId); // Clean up map entry on successful link
                     // Optionally notify referrer?
                     await safeSendMessage(referrerUserId, `🎉 Someone you referred \\(${await getUserDisplayName(referrerUserId, userId)}\\) just made their first deposit\\! You may receive bonuses based on their activity\\.`, { parse_mode: 'MarkdownV2'}).catch(()=>{});
                } else {
                     console.log(`${logPrefix} Failed to link pending referral (user might already be referred or self-referral).`);
                     pendingReferrals.delete(userId); // Still remove pending referral
                }
            } else if (pending) {
                console.log(`${logPrefix} Pending referral expired for user ${userId}.`);
                pendingReferrals.delete(userId);
            }
             // Ensure the new user gets a referral code generated if they weren't referred
             await queryDatabase(`UPDATE wallets SET referral_code = COALESCE(referral_code, $2) WHERE user_id = $1`, [userId, generateReferralCode()], client);
        }

        // --- 5. Commit DB Transaction ---
        await client.query('COMMIT');
        console.log(`${logPrefix} Successfully processed deposit and updated balance for user ${userId}.`);

        // 6. Add signature to processed cache AFTER successful commit
        addProcessedDepositTx(signature);

        // 7. Notify User AFTER successful commit
        await safeSendMessage(userId, `✅ Deposit successful\\! ${escapeMarkdownV2((Number(transferAmount) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL has been added to your balance\\. Use /wallet to check\\.`, { parse_mode: 'MarkdownV2' });

    } catch (error) {
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { console.error(`${logPrefix} Rollback failed:`, rbErr); } }
        console.error(`${logPrefix} Error processing deposit: ${error.message}`);

        // If we identified the user, notify them of the failure
        let userIdForNotify = null;
        try {
             const addrInfoOnError = await findDepositAddressInfo(depositAddress); // Fetch again outside TX
             if (addrInfoOnError) userIdForNotify = addrInfoOnError.userId;
        } catch { /* ignore lookup error */ }

        if (userIdForNotify) {
            await safeSendMessage(userIdForNotify, `⚠️ There was an issue processing your deposit \\(TX: \`${escapeMarkdownV2(signature)}\`\\)\\. If funds left your wallet but your balance wasn't updated, please contact support\\. Error: ${escapeMarkdownV2(error.message)}`, { parse_mode: 'MarkdownV2'});
        }

        // Add to processed cache only on definitive, non-retryable errors to prevent loops
        const errMsgLower = error.message.toLowerCase();
        if (errMsgLower.includes('already used') || errMsgLower.includes('expired') || errMsgLower.includes('not found') || errMsgLower.includes('not positive sol transfer') || errMsgLower.includes('status is') || errMsgLower.includes('transaction failed on-chain')) {
            addProcessedDepositTx(signature);
        }
    } finally {
        if (client) client.release();
    }
}


// --- Telegram Bot Event Listeners ---
function setupTelegramListeners() {
    console.log("⚙️ Setting up Telegram event listeners...");

    // Listener for regular text messages/commands
    bot.on('message', (msg) => {
        // Add to message queue for processing
        messageQueue.add(() => handleMessage(msg)).catch(err => {
            console.error(`⚠️ Message processing queue error for msg ID ${msg?.message_id}:`, err);
             // Optionally notify user if possible/safe
             if(err.message?.includes('timeout')) {
                 safeSendMessage(msg.chat.id, "Processing your request timed out, please try again shortly.").catch(()=>{});
             }
        });
    });

    // Listener for inline button callbacks
    bot.on('callback_query', (callbackQuery) => {
        // Add to callback queue for processing
        callbackQueue.add(() => handleCallbackQuery(callbackQuery)).catch(err => {
            console.error(`⚠️ Callback query processing queue error for query ID ${callbackQuery?.id}:`, err);
            // Attempt to answer callback query to clear loading state even on error
            bot.answerCallbackQuery(callbackQuery.id, { text: "Error processing action." }).catch(e => console.warn("Failed to answer callback query on error:", e.message));
        });
    });

    // Basic Error Listeners
    bot.on('polling_error', (error) => {
        console.error(`❌ Telegram Polling Error: Code ${error.code} | ${error.message}`);
        // Handle specific errors like ETELEGRAM: 401 Unauthorized, 409 Conflict, etc.
        if (error.code === 'EFATAL' || error.message.includes('401')) {
             console.error("❌❌ CRITICAL POLLING ERROR (e.g., Bad Token?). Shutting down.");
             // Force exit immediately on critical token/auth errors
             process.exit(1);
        } else if (error.message.includes('409 Conflict')) {
             console.error("❌❌❌ Conflict! Another instance seems to be running with this token. Exiting.");
             process.exit(1);
        }
        // Log other polling errors but don't necessarily exit
    });
    bot.on('webhook_error', (error) => {
        console.error(`❌ Telegram Webhook Error: Code ${error.code} | ${error.message}`);
        // Potentially try to switch back to polling if webhook fails persistently? More complex logic needed.
    });
    bot.on('error', (error) => { // General non-request related errors
        console.error('❌ General Bot Error:', error);
    });

    console.log("✅ Telegram listeners registered.");
}


// --- Express Server Route Setup ---
// Define webhookPath globally for use in setup and Railway check
const webhookPath = `/bot${process.env.BOT_TOKEN}`;

function setupExpressRoutes() {
    console.log("⚙️ Setting up Express routes...");
    // Root Info Endpoint
    app.get('/', (req, res) => {
        res.status(200).json({
            status: 'ok',
            initialized: isFullyInitialized,
            version: '3.1.0',
            timestamp: new Date().toISOString(),
            // Add more stats later if needed (e.g., queue sizes, cache sizes from global vars)
        });
    });

    // Health Check Endpoints
    app.get('/health', (req, res) => {
        // Basic check: server is running and DB pool exists
        const dbOk = pool && pool.totalCount !== undefined; // Basic check pool object exists
        const status = server && dbOk ? (isFullyInitialized ? 'ready' : 'starting') : 'error';
        res.status(status === 'error' ? 503 : 200).json({
             status: status,
             db_pool_total: pool?.totalCount ?? 'N/A',
             uptime: process.uptime()
        });
    });
    app.get('/railway-health', (req, res) => { // For Railway health checks
         res.status(isFullyInitialized ? 200 : 503).json({
             status: isFullyInitialized ? 'ready' : 'initializing',
             version: '3.1.0'
         });
    });
    app.get('/prestop', (req, res) => { // For Railway graceful shutdown signal
         console.log('🚪 Received pre-stop signal from Railway...');
         // No shutdown logic here, Railway sends SIGTERM after this response
         res.status(200).send('Acknowledged pre-stop signal');
    });

    // Telegram Webhook Handler
    app.post(webhookPath, (req, res) => {
        // Validate request body somewhat
        if (req.body && typeof req.body === 'object' && req.body.update_id) {
            // Route message updates to messageQueue, callback_query to callbackQueue
            if (req.body.message) {
                 messageQueue.add(() => bot.processUpdate(req.body)).catch(queueError => {
                      console.error("❌ Error adding webhook message update to queue:", queueError);
                 });
            } else if (req.body.callback_query) {
                 callbackQueue.add(() => bot.processUpdate(req.body)).catch(queueError => {
                      console.error("❌ Error adding webhook callback update to queue:", queueError);
                 });
            } else {
                 // Ignore other update types for now
                 // console.warn("⚠️ Received unhandled update type via webhook:", Object.keys(req.body));
            }
        } else {
             console.warn("⚠️ Received invalid/empty request on webhook path");
        }
        // Always acknowledge Telegram quickly
        res.sendStatus(200);
    });
    console.log(`✅ Express routes configured. Webhook path: ${webhookPath}`);
}

// --- Database Initialization Function ---
async function initializeDatabase() {
    console.log("⚙️ Initializing Database schema (v3.1.0)...");
    let client = null;
    try {
        client = await pool.connect();
        await client.query('BEGIN');

        // Use queryDatabase helper for consistency and logging
        console.log("  Ensuring core extensions...");
        // await queryDatabase(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp";`, [], client); // Example if needed later

        console.log("  Creating/Verifying tables...");
        // Wallets (User Info)
        await queryDatabase(`
            CREATE TABLE IF NOT EXISTS wallets (
                user_id TEXT PRIMARY KEY,                   -- Telegram User ID
                external_withdrawal_address TEXT,           -- User's linked external address
                referral_code TEXT UNIQUE,                  -- This user's unique referral code
                referred_by_user_id TEXT,                   -- user_id of the referrer
                referral_count INT NOT NULL DEFAULT 0,      -- How many users this user has referred
                total_wagered BIGINT NOT NULL DEFAULT 0,    -- Total lamports wagered internally by this user
                last_milestone_paid_lamports BIGINT NOT NULL DEFAULT 0, -- Tracks last milestone paid FOR this user's referrer
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                linked_at TIMESTAMPTZ,                      -- When external_withdrawal_address was first set/updated
                last_used_at TIMESTAMPTZ DEFAULT NOW()      -- Last interaction time
            );`, [], client);
        // Add columns idempotently (handle potential existing columns)
        await queryDatabase(`ALTER TABLE wallets ADD COLUMN IF NOT EXISTS external_withdrawal_address TEXT;`, [], client);
        await queryDatabase(`ALTER TABLE wallets ADD COLUMN IF NOT EXISTS referral_code TEXT UNIQUE;`, [], client);
        await queryDatabase(`ALTER TABLE wallets ADD COLUMN IF NOT EXISTS referred_by_user_id TEXT;`, [], client);
        await queryDatabase(`ALTER TABLE wallets ADD COLUMN IF NOT EXISTS referral_count INT NOT NULL DEFAULT 0;`, [], client);
        await queryDatabase(`ALTER TABLE wallets ADD COLUMN IF NOT EXISTS total_wagered BIGINT NOT NULL DEFAULT 0;`, [], client);
        await queryDatabase(`ALTER TABLE wallets ADD COLUMN IF NOT EXISTS last_milestone_paid_lamports BIGINT NOT NULL DEFAULT 0;`, [], client);
        await queryDatabase(`ALTER TABLE wallets ADD COLUMN IF NOT EXISTS linked_at TIMESTAMPTZ;`, [], client);
        await queryDatabase(`ALTER TABLE wallets ADD COLUMN IF NOT EXISTS last_used_at TIMESTAMPTZ DEFAULT NOW();`, [], client);

        // User Balances
        await queryDatabase(`
            CREATE TABLE IF NOT EXISTS user_balances (
                user_id TEXT PRIMARY KEY REFERENCES wallets(user_id) ON DELETE CASCADE,
                balance_lamports BIGINT NOT NULL DEFAULT 0 CHECK (balance_lamports >= 0), -- Ensure non-negative
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );`, [], client);
        await queryDatabase(`ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();`, [], client);


        // Deposit Addresses
        await queryDatabase(`
            CREATE TABLE IF NOT EXISTS deposit_addresses (
                id SERIAL PRIMARY KEY,
                user_id TEXT NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                deposit_address TEXT UNIQUE NOT NULL,
                derivation_path TEXT NOT NULL, -- Store path for auditing/recovery reference
                status TEXT NOT NULL DEFAULT 'pending', -- 'pending', 'used', 'expired'
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                expires_at TIMESTAMPTZ NOT NULL
            );`, [], client);

        // Deposits Log
        await queryDatabase(`
            CREATE TABLE IF NOT EXISTS deposits (
                id SERIAL PRIMARY KEY,
                user_id TEXT NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                deposit_address_id INT REFERENCES deposit_addresses(id) ON DELETE SET NULL, -- Link to specific address used
                tx_signature TEXT UNIQUE NOT NULL,
                amount_lamports BIGINT NOT NULL,
                status TEXT NOT NULL DEFAULT 'confirmed', -- 'confirmed', 'error' (e.g., couldn't credit balance post-factum)
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW() -- When balance was credited
            );`, [], client);

        // Withdrawals Log
        await queryDatabase(`
            CREATE TABLE IF NOT EXISTS withdrawals (
                id SERIAL PRIMARY KEY,
                user_id TEXT NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                requested_amount_lamports BIGINT NOT NULL, -- Amount user asked for
                fee_lamports BIGINT NOT NULL DEFAULT 0,
                final_send_amount_lamports BIGINT NOT NULL, -- Amount actually sent (usually = requested)
                recipient_address TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending', -- 'pending', 'processing', 'completed', 'failed'
                payout_tx_signature TEXT UNIQUE,
                error_message TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                processed_at TIMESTAMPTZ, -- When processing started
                completed_at TIMESTAMPTZ  -- When completed or failed definitively
            );`, [], client);

        // Referral Payouts
        await queryDatabase(`
             CREATE TABLE IF NOT EXISTS referral_payouts (
                 id SERIAL PRIMARY KEY,
                 referrer_user_id TEXT NOT NULL, -- No FK for flexibility if user leaves/cleared
                 referee_user_id TEXT NOT NULL,  -- No FK for flexibility
                 payout_type TEXT NOT NULL,      -- 'initial_bet' or 'milestone'
                 triggering_bet_id INT,          -- bets.id for 'initial_bet' type (No FK)
                 milestone_reached_lamports BIGINT,-- Wagered amount milestone for 'milestone' type
                 payout_amount_lamports BIGINT NOT NULL, -- Amount paid to referrer
                 status TEXT NOT NULL DEFAULT 'pending', -- 'pending', 'processing', 'paid', 'failed'
                 payout_tx_signature TEXT UNIQUE, -- Signature of the successful payout transaction
                 error_message TEXT,             -- Store reason for failure
                 created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                 processed_at TIMESTAMPTZ,       -- When processing started
                 paid_at TIMESTAMPTZ             -- When payout was confirmed successful
             );`, [], client);
        // Add unique constraint for milestone payouts to prevent duplicates
         await queryDatabase(`
             ALTER TABLE referral_payouts
             ADD CONSTRAINT unique_milestone_payout UNIQUE (referrer_user_id, referee_user_id, payout_type, milestone_reached_lamports)
             WHERE payout_type = 'milestone';
         `, [], client).catch(e => { if (e.code !== '42P07' && e.code !== '42710') throw e; }); // Ignore if constraint/relation already exists


        // Ledger (Audit Trail) - ensure FK to referral_payouts exists
        await queryDatabase(`
            CREATE TABLE IF NOT EXISTS ledger (
                id BIGSERIAL PRIMARY KEY,
                user_id TEXT NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                transaction_type TEXT NOT NULL, -- 'deposit', 'bet_placed', 'bet_won', 'bet_push_return', 'withdrawal', 'referral_bonus_paid', 'manual_adjustment' etc.
                amount_lamports BIGINT NOT NULL, -- Positive for credit, Negative for debit relative to balance
                balance_before BIGINT NOT NULL,
                balance_after BIGINT NOT NULL,
                related_deposit_id INT REFERENCES deposits(id) ON DELETE SET NULL,
                related_withdrawal_id INT REFERENCES withdrawals(id) ON DELETE SET NULL,
                related_bet_id INT, -- No FK to bets by default, can be added if needed
                related_ref_payout_id INT REFERENCES referral_payouts(id) ON DELETE SET NULL, -- FK added below if needed
                notes TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );`, [], client);
        // Add related_ref_payout_id column/constraint idempotently
        await queryDatabase(`ALTER TABLE ledger ADD COLUMN IF NOT EXISTS related_ref_payout_id INT REFERENCES referral_payouts(id) ON DELETE SET NULL;`, [], client);


        // Bets Table (Revised for Custodial)
        await queryDatabase(`
            CREATE TABLE IF NOT EXISTS bets (
                id SERIAL PRIMARY KEY,
                user_id TEXT NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                chat_id TEXT NOT NULL,
                game_type TEXT NOT NULL,
                bet_details JSONB, -- Store game-specific choices (e.g., {'choice': 'heads'}, {'horse': 'Solar Sprint'})
                wager_amount_lamports BIGINT NOT NULL CHECK (wager_amount_lamports > 0),
                status TEXT NOT NULL DEFAULT 'active', -- 'active', 'processing_game', 'completed_win', 'completed_loss', 'completed_push', 'error_game_logic'
                payout_amount_lamports BIGINT, -- Amount credited back to internal balance (wager + win, or wager for push, 0 for loss)
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                processed_at TIMESTAMPTZ, -- When game logic finished
                priority INT NOT NULL DEFAULT 0
            );`, [], client);
        // Add/ensure columns exist
        await queryDatabase(`ALTER TABLE bets ADD COLUMN IF NOT EXISTS bet_details JSONB;`, [], client);
        // Ensure wager column exists AND has the check constraint - combine adding column and constraint definition
        await queryDatabase(`ALTER TABLE bets ADD COLUMN IF NOT EXISTS wager_amount_lamports BIGINT NOT NULL DEFAULT 1 CHECK (wager_amount_lamports > 0);`, [], client); // Default 1 temporarily to allow adding check
        await queryDatabase(`ALTER TABLE bets ALTER COLUMN wager_amount_lamports DROP DEFAULT;`, [], client); // Remove temporary default
        await queryDatabase(`ALTER TABLE bets ADD COLUMN IF NOT EXISTS payout_amount_lamports BIGINT;`, [], client);
        await queryDatabase(`ALTER TABLE bets ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ;`, [], client);
        await queryDatabase(`ALTER TABLE bets ADD COLUMN IF NOT EXISTS priority INT NOT NULL DEFAULT 0;`, [], client);
        // **REMOVED** the separate 'ADD CONSTRAINT wager_positive' line as it's handled by the inline CHECK above


        // --- INDEXES ---
        console.log("  Creating/Verifying indexes...");
        // Wallets
        await queryDatabase(`CREATE INDEX IF NOT EXISTS idx_wallets_ref_code ON wallets(referral_code) WHERE referral_code IS NOT NULL;`, [], client);
        await queryDatabase(`CREATE INDEX IF NOT EXISTS idx_wallets_referred_by ON wallets(referred_by_user_id) WHERE referred_by_user_id IS NOT NULL;`, [], client);
        // User Balances - PK index exists
        // Deposit Addresses
        await queryDatabase(`CREATE INDEX IF NOT EXISTS idx_depaddr_user ON deposit_addresses(user_id);`, [], client);
        await queryDatabase(`CREATE INDEX IF NOT EXISTS idx_depaddr_addr_pending ON deposit_addresses(deposit_address) WHERE status = 'pending';`, [], client);
        await queryDatabase(`CREATE INDEX IF NOT EXISTS idx_depaddr_expires ON deposit_addresses(expires_at);`, [], client);
        // Deposits
        await queryDatabase(`CREATE INDEX IF NOT EXISTS idx_deposits_user ON deposits(user_id);`, [], client);
        // Withdrawals
        await queryDatabase(`CREATE INDEX IF NOT EXISTS idx_withdrawals_user_status ON withdrawals(user_id, status);`, [], client);
        await queryDatabase(`CREATE INDEX IF NOT EXISTS idx_withdrawals_status ON withdrawals(status);`, [], client);
        await queryDatabase(`CREATE INDEX IF NOT EXISTS idx_withdrawals_payout_sig ON withdrawals(payout_tx_signature) WHERE payout_tx_signature IS NOT NULL;`, [], client);
        // Ledger
        await queryDatabase(`CREATE INDEX IF NOT EXISTS idx_ledger_user_time ON ledger(user_id, created_at DESC);`, [], client);
        await queryDatabase(`CREATE INDEX IF NOT EXISTS idx_ledger_type ON ledger(transaction_type);`, [], client);
        await queryDatabase(`CREATE INDEX IF NOT EXISTS idx_ledger_ref_payout_id ON ledger(related_ref_payout_id) WHERE related_ref_payout_id IS NOT NULL;`, [], client);
        // Referral Payouts
        await queryDatabase(`CREATE INDEX IF NOT EXISTS idx_refpayout_referrer ON referral_payouts(referrer_user_id);`, [], client);
        await queryDatabase(`CREATE INDEX IF NOT EXISTS idx_refpayout_referee ON referral_payouts(referee_user_id);`, [], client);
        await queryDatabase(`CREATE INDEX IF NOT EXISTS idx_refpayout_status ON referral_payouts(status);`, [], client);
        await queryDatabase(`CREATE INDEX IF NOT EXISTS idx_refpayout_paid_tx_sig ON referral_payouts(payout_tx_signature) WHERE payout_tx_signature IS NOT NULL;`, [], client);
        // Bets
        await queryDatabase(`CREATE INDEX IF NOT EXISTS idx_bets_user_status_time ON bets(user_id, status, created_at DESC);`, [], client);
        await queryDatabase(`CREATE INDEX IF NOT EXISTS idx_bets_status_priority ON bets(status, priority DESC, created_at ASC);`, [], client); // For processing active bets

        await client.query('COMMIT');
        console.log("✅ Database schema initialized/verified successfully.");

    } catch (err) {
        console.error("❌ Database initialization error:", err);
        if (client) { try { await client.query('ROLLBACK'); console.log("  - Transaction rolled back."); } catch (rbErr) { console.error("  - Rollback failed:", rbErr); } }
        // Re-throw unexpected errors to halt startup
        throw err;
    } finally {
        if (client) client.release();
    }
}

// --- Startup/Shutdown Helpers ---

async function setupTelegramWebhook() {
    // Use global webhookPath variable
    if (process.env.RAILWAY_ENVIRONMENT && process.env.RAILWAY_PUBLIC_DOMAIN) {
        const webhookUrl = `https://${process.env.RAILWAY_PUBLIC_DOMAIN}${webhookPath}`;
        console.log(`Attempting to set webhook to: ${webhookUrl}`);
        let attempts = 0; const maxAttempts = 3;
        while (attempts < maxAttempts) {
            try {
                // Drop pending updates when setting webhook to avoid processing old events
                await bot.deleteWebHook({ drop_pending_updates: true }).catch(e => console.warn(`Ignoring webhook delete error: ${e.message}`));
                await bot.setWebHook(webhookUrl, {
                    max_connections: parseInt(process.env.WEBHOOK_MAX_CONN || '20', 10), // Increased default
                    allowed_updates: ["message", "callback_query"], // Listen for both
                    // drop_pending_updates: true // Alternative way? Check node-telegram-bot-api docs
                });
                console.log(`✅ Webhook successfully set to: ${webhookUrl}`);
                return true;
            } catch (webhookError) {
                attempts++;
                console.error(`❌ Webhook setup attempt ${attempts}/${maxAttempts} failed:`, webhookError.message);
                if (webhookError.code === 'ETELEGRAM' && (webhookError.message.includes('URL host is empty') || webhookError.message.includes('Unauthorized'))) {
                     return false; // Non-retryable config error
                }
                if (attempts >= maxAttempts) {
                     console.error("❌ Max webhook setup attempts reached.");
                     return false;
                }
                await new Promise(resolve => setTimeout(resolve, 3000 * attempts)); // Exponential backoff
            }
        }
    }
    return false; // Not in Railway env or setup failed
}

async function startPollingIfNeeded() {
    try {
        const webhookInfo = await bot.getWebHookInfo().catch(e => { console.error("Error fetching webhook info:", e.message); return null; });
        if (!webhookInfo || !webhookInfo.url) {
            if (bot.isPolling()) {
                 console.log("ℹ️ Bot is already polling.");
                 return; // Already polling
            }
            console.log("ℹ️ Webhook not set or fetch failed, starting bot polling...");
            // Ensure any previous webhook is cleared, drop pending updates
            await bot.deleteWebHook({ drop_pending_updates: true }).catch(e => console.warn(`Ignoring webhook delete error before polling: ${e.message}`));
            await bot.startPolling({ allowed_updates: ["message", "callback_query"] }); // Ensure polling gets both
            console.log("✅ Bot polling started successfully");
        } else {
            // Webhook IS set, ensure polling is OFF
            if (bot.isPolling()) {
                console.log("ℹ️ Stopping existing polling because webhook is set.");
                await bot.stopPolling({ cancel: true }).catch(e => console.warn(`Ignoring polling stop error: ${e.message}`));
            }
             console.log(`ℹ️ Bot is using webhook: ${webhookInfo.url}`);
        }
    } catch (err) {
        console.error("❌ Error managing polling state:", err.message);
         if (err.code === 'ETELEGRAM' && err.message.includes('409 Conflict')) {
             console.error("❌❌❌ Conflict! Another instance seems to be running with this token. Exiting.");
             // Force exit immediately on conflict
             process.exit(1);
         } else if (err.response?.statusCode === 401 || err.message.includes('401 Unauthorized')) {
             console.error("❌❌❌ Unauthorized! Check BOT_TOKEN. Exiting.");
             // Force exit immediately on auth error
             process.exit(1);
         }
         // Otherwise, log the error but try to continue (might be temporary)
    }
}

function startDepositMonitor() {
    if (depositMonitorIntervalId) {
         console.warn("[DepositMonitor] Attempted to start monitor but it's already running.");
         return;
    }

    const intervalMs = parseInt(process.env.DEPOSIT_MONITOR_INTERVAL_MS, 10);
    if (isNaN(intervalMs) || intervalMs < 5000) { // Minimum interval 5s
        console.warn(`⚠️ Invalid or low DEPOSIT_MONITOR_INTERVAL_MS (${process.env.DEPOSIT_MONITOR_INTERVAL_MS}), defaulting to 15000ms.`);
        process.env.DEPOSIT_MONITOR_INTERVAL_MS = '15000';
    }
    const finalIntervalMs = parseInt(process.env.DEPOSIT_MONITOR_INTERVAL_MS, 10);

    console.log(`⚙️ Starting Deposit Monitor (Polling - Interval: ${finalIntervalMs / 1000}s). WARNING: Polling is not scalable for production.`);
    // Initial run shortly after start
    const initialDelay = 5000; // 5 second delay
    console.log(`  - Initial deposit check scheduled in ${initialDelay / 1000}s...`);
    setTimeout(() => {
        console.log("⚙️ Performing initial deposit monitor run...");
        monitorDepositsPolling().catch(err => {
            console.error('❌ [DEPOSIT MONITOR ASYNC ERROR - Initial Run]:', err);
        });
    }, initialDelay);

    // Set interval
    depositMonitorIntervalId = setInterval(() => {
        monitorDepositsPolling().catch(err => {
            // Catch errors from the async function itself to prevent interval from stopping
            console.error('❌ [DEPOSIT MONITOR ASYNC ERROR - Interval]:', err);
        });
    }, finalIntervalMs);
}


// --- Graceful Shutdown Handler ---
let isShuttingDown = false;
const shutdown = async (signal, isRailwayRotation = false) => {
    if (isShuttingDown) {
        console.log("--> Shutdown already in progress...");
        return;
    }
    isShuttingDown = true;
    console.log(`\n🛑 ${signal} received, ${isRailwayRotation ? 'initiating container rotation' : 'shutting down gracefully'}...`);

    // --- Railway Specific Rotation Handling ---
    if (isRailwayRotation) {
        console.log("--> Railway: Closing server, stopping monitor, brief queue drain.");
        // 1. Stop accepting new work
        if (depositMonitorIntervalId) { clearInterval(depositMonitorIntervalId); depositMonitorIntervalId = null; console.log("  - Stopped deposit monitor."); }
        if (server) { server.close(() => console.log("  - Express server closed for rotation.")); }
        // 2. Allow brief drain (optional, short timeout)
        const drainTimeout = 1000; // 1 second max wait during rotation
        console.log(`  - Allowing ${drainTimeout}ms for minimal queue drain...`);
        try {
            // Only wait very briefly, mainly for any in-flight payout network calls
            await Promise.race([
                payoutProcessorQueue.onIdle(),
                new Promise(resolve => setTimeout(resolve, drainTimeout))
            ]);
        } catch (e) { /* Ignore timeout */ }
        console.log("--> Rotation preparation complete. Handing off.");
        isShuttingDown = false; // Allow potential full shutdown later if needed
        return; // Let Railway handle the actual process termination
    }

    // --- Full Graceful Shutdown Logic ---
    console.log("--> Performing full graceful shutdown sequence...");
    isFullyInitialized = false; // Mark as not ready

    // 1. Stop Deposit Monitor Interval
    if (depositMonitorIntervalId) { clearInterval(depositMonitorIntervalId); depositMonitorIntervalId = null; console.log("  - Stopped deposit monitor interval."); }

    // 2. Stop Receiving New Events/Requests
    console.log("  - Stopping incoming connections and listeners...");
    try {
        // Close Express server first
        if (server) {
            await new Promise((resolve, reject) => {
                const timeout = setTimeout(() => reject(new Error('Server close timeout (5s)')), 5000);
                if(timeout.unref) timeout.unref(); // Allow node to exit if only this timer remains
                server.close((err) => {
                    clearTimeout(timeout);
                    if (err) reject(err); else resolve();
                });
            });
            console.log("  - Express server closed.");
        }
        // Stop Telegram listeners
        if (bot.isPolling()) {
            await bot.stopPolling({ cancel: true }).catch(e => console.warn("  - Polling stop error:", e.message));
            console.log("  - Stopped Telegram polling.");
        } else {
            // Try removing webhook if it was set
             await bot.deleteWebHook({ drop_pending_updates: false }).catch(e => console.warn("  - Webhook delete error:", e.message));
             console.log("  - Removed Telegram webhook (if set).");
        }
    } catch (e) {
        console.error("⚠️ Error stopping listeners/server:", e.message);
    }

    // 3. Wait for Ongoing Queue Processing
    const queueDrainTimeoutMs = parseInt(process.env.SHUTDOWN_QUEUE_TIMEOUT_MS, 10);
    console.log(`  - Waiting for active jobs (max ${queueDrainTimeoutMs / 1000}s)...`);
    try {
        const queuesToWait = [
            messageQueue,
            callbackQueue,
            payoutProcessorQueue,
            depositProcessorQueue,
            telegramSendQueue
        ];
        const idlePromises = queuesToWait.map(q => q.onIdle());
        const timeoutPromise = new Promise((_, reject) => {
             const t = setTimeout(() => reject(new Error(`Queue drain timeout (${queueDrainTimeoutMs}ms)`)), queueDrainTimeoutMs);
             if (t.unref) t.unref();
        });

        await Promise.race([
            Promise.all(idlePromises),
            timeoutPromise
        ]);
        console.log("  - All processing queues are idle.");
    } catch (queueError) {
        console.warn(`⚠️ ${queueError.message}. Proceeding with shutdown.`);
        // Log queue sizes to understand what might be stuck
        console.warn(`    Queue sizes: Msg=${messageQueue.size}, Callback=${callbackQueue.size}, Payout=${payoutProcessorQueue.size}, Deposit=${depositProcessorQueue.size}, TGSend=${telegramSendQueue.size}`);
    }

    // 4. Close Database Pool
    console.log("  - Closing database pool...");
    try {
        await pool.end();
        console.log("  - Database pool closed.");
    } catch (dbErr) {
        console.error("❌ Error closing database pool:", dbErr);
    }

    // 5. Exit Process
    console.log("🛑 Full Shutdown sequence complete.");
    // Exit with code 0 for expected signals, 1 otherwise
    process.exit(signal === 'SIGINT' || signal === 'SIGTERM' ? 0 : 1);
};


// --- Process Signal Handlers ---
process.on('SIGTERM', () => {
    const isRailway = !!process.env.RAILWAY_ENVIRONMENT;
    console.info(`Received SIGTERM. Railway Environment: ${isRailway}`);
    shutdown('SIGTERM', isRailway).catch((err) => {
        console.error("SIGTERM shutdown handler error:", err);
        process.exit(1); // Force exit if shutdown handler fails critically
    });
});
process.on('SIGINT', () => { // CTRL+C
    console.info(`Received SIGINT.`);
    shutdown('SIGINT', false).catch((err) => {
        console.error("SIGINT shutdown handler error:", err);
        process.exit(1);
    });
});


// --- Global Exception Handlers ---
process.on('uncaughtException', (err, origin) => {
    console.error(`🔥🔥🔥 Uncaught Exception at: ${origin}`);
    console.error(err); // Log the full error object
    if (!isShuttingDown) {
        console.log("--> Attempting emergency shutdown due to uncaught exception...");
        // Use a simplified, faster shutdown path? Or the full one? Let's try full first.
        shutdown('UNCAUGHT_EXCEPTION', false).catch(() => {}); // Ignore errors during emergency shutdown
    }
    // Ensure the process exits after a short delay, even if shutdown hangs
    const failTimeout = parseInt(process.env.SHUTDOWN_FAIL_TIMEOUT_MS, 10);
    const t = setTimeout(() => {
        console.error(`--> Forcing exit (${failTimeout}ms) after uncaught exception.`);
        process.exit(1);
    }, failTimeout);
    if (t.unref) t.unref(); // Allow Node to exit if this is the only timer left
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('🔥🔥🔥 Unhandled Rejection at:', promise, '\nReason:', reason);
    // Depending on the reason, might consider this fatal or just log it.
    // If reason suggests a critical failure, could trigger shutdown similar to uncaughtException.
    // For now, just log.
});


// --- Application Start ---
async function startApp() {
    console.log(`\n🚀 Starting Solana Gambles Bot v3.1.0...`);
    const PORT = process.env.PORT || 3000;

    // Setup routes and listeners before starting server
    setupExpressRoutes();
    setupTelegramListeners(); // Setup bot listeners

    // Start the Express server
    server = app.listen(PORT, "0.0.0.0", () => {
        console.log(`✅ Server listening on port ${PORT}.`);
        const initDelay = parseInt(process.env.INIT_DELAY_MS, 10);
        console.log(`⚙️ Scheduling background initialization in ${initDelay / 1000}s...`);

        // Start background tasks after a short delay
        setTimeout(async () => {
            console.log("\n⚙️ Starting Delayed Background Initialization...");
            try {
                // 1. Initialize Database Schema
                await initializeDatabase();

                // 2. Setup Telegram Connection (Webhook/Polling) & Verify
                const webhookSet = await setupTelegramWebhook();
                if (!webhookSet) { await startPollingIfNeeded(); } // Fallback to polling
                const me = await bot.getMe();
                console.log(`✅ Connected to Telegram as bot: @${me.username} (ID: ${me.id})`);

                // 3. Start the Deposit Monitor
                startDepositMonitor(); // Starts the interval timer

                // Optional: Add any other background tasks or checks here

                isFullyInitialized = true; // Mark as ready *after* all core init steps
                console.log("\n✅ Background Initialization Complete.");
                console.log(`🚀🚀🚀 Solana Gambles Bot (v3.1.0 - Custodial) is fully operational! 🚀🚀🚀`);

            } catch (initError) {
                console.error("\n🔥🔥🔥 Delayed Background Initialization Failed:", initError);
                console.error("❌ Exiting due to critical initialization failure.");
                // Attempt graceful shutdown, then force exit
                if (!isShuttingDown) await shutdown('INITIALIZATION_FAILURE', false).catch(() => {});
                const failTimeout = parseInt(process.env.SHUTDOWN_FAIL_TIMEOUT_MS, 10);
                const t = setTimeout(() => { console.error(`--> Forcing exit (${failTimeout}ms) after init failure.`); process.exit(1); }, failTimeout);
                if (t.unref) t.unref();
            }
        }, initDelay);
    });

    server.on('error', (err) => {
        console.error('❌ Server startup error:', err);
        if (err.code === 'EADDRINUSE') {
            console.error(`❌ Port ${PORT} is already in use. Is another instance running? Exiting.`);
        } else {
             console.error(`❌ Unhandled server error. Exiting.`);
        }
        process.exit(1); // Exit on critical server start errors
    });
}

// --- Run the application ---
startApp().catch(err => {
     console.error("❌ Unhandled error during application startup sequence:", err);
     process.exit(1);
});

// --- End of Part 6 / End of File ---
