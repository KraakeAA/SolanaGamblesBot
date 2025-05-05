// index.js - Part 1: Imports, Environment Configuration, Core Initializations
// --- VERSION: 3.1.5 --- (Comprehensive Fixes: BigInt Log, Idempotency, Admin Notify, safeIndex)

// --- All Imports MUST come first ---
import 'dotenv/config'; // Keep for local development via .env file
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
    SendTransactionError,
    TransactionExpiredBlockheightExceededError // Import specific error type if needed for retries
} from '@solana/web3.js';
import bs58 from 'bs58';
import * as crypto from 'crypto'; // Keep for other crypto uses if any
import { createHash } from 'crypto'; // *** Ensure this import is present ***
import PQueue from 'p-queue';
import { Buffer } from 'buffer';
import bip39 from 'bip39'; // For mnemonic seed phrase generation/validation
// ---- Library Change ----
import { derivePath } from 'ed25519-hd-key'; // <-- RESTORED OLD LIBRARY IMPORT (with workaround now)
// import { SLIP10Node } from '@metamask/key-tree'; // <-- REMOVED PROBLEMATIC LIBRARY IMPORT
// ------------------------
import nacl from 'tweetnacl'; // Keep for keypair generation from derived seed

// Assuming RateLimitedConnection exists and works as intended
import RateLimitedConnection from './lib/solana-connection.js';

// --- Deployment Check Log ---
console.log(`--- INDEX.JS - DEPLOYMENT CHECK --- ${new Date().toISOString()} --- v3.1.5 ---`); // Updated Version
// --- END DEPLOYMENT CHECK ---


console.log("⏳ Starting Solana Gambles Bot (Custodial, Buttons, v3.1.5)... Checking environment variables..."); // Updated Version

// --- Environment Variable Configuration ---
const REQUIRED_ENV_VARS = [
    'BOT_TOKEN',
    'DATABASE_URL',
    'DEPOSIT_MASTER_SEED_PHRASE', // Expect BIP39 Mnemonic Seed Phrase (SECRET)
    'MAIN_BOT_PRIVATE_KEY',       // Used for general payouts (Withdrawals) (SECRET)
    'REFERRAL_PAYOUT_PRIVATE_KEY',// (SECRET) - Presence checked, but allowed to be empty
    'RPC_URLS',                   // Comma-separated list of RPC URLs
    'ADMIN_USER_IDS',             // *** ADDED REQUIREMENT for admin notifications *** Comma-separated Telegram User IDs
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
// Specific check for ADMIN_USER_IDS content
if (!process.env.ADMIN_USER_IDS) {
     // Already caught by missing check above, but double-check
     if (!missingVars) { // Avoid duplicate message if already missing
         console.error(`❌ Environment variable ADMIN_USER_IDS is missing.`);
         missingVars = true;
     }
} else if (process.env.ADMIN_USER_IDS.split(',').map(id => id.trim()).filter(id => /^\d+$/.test(id)).length === 0) {
     console.error(`❌ Environment variable ADMIN_USER_IDS must contain at least one valid comma-separated Telegram User ID.`);
     missingVars = true;
}


// Specific check for valid Seed Phrase
console.log(`[Env Check] Checking DEPOSIT_MASTER_SEED_PHRASE... Present: ${!!process.env.DEPOSIT_MASTER_SEED_PHRASE}`);
if (process.env.DEPOSIT_MASTER_SEED_PHRASE && !bip39.validateMnemonic(process.env.DEPOSIT_MASTER_SEED_PHRASE)) {
    console.error(`❌ Environment variable DEPOSIT_MASTER_SEED_PHRASE is set but is not a valid BIP39 mnemonic.`);
    missingVars = true;
} else if (!process.env.DEPOSIT_MASTER_SEED_PHRASE) {
     if (!REQUIRED_ENV_VARS.includes('DEPOSIT_MASTER_SEED_PHRASE')) REQUIRED_ENV_VARS.push('DEPOSIT_MASTER_SEED_PHRASE'); // Ensure it's checked
     if (!process.env.DEPOSIT_MASTER_SEED_PHRASE) {
      console.error(`❌ Environment variable DEPOSIT_MASTER_SEED_PHRASE is missing.`);
      missingVars = true;
     }
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
const validateBase58Key = (keyName, isRequired) => {
    const key = process.env[keyName];
    if (key) { // If key is present, validate it
        try {
            const decoded = bs58.decode(key);
            // Keypair.fromSecretKey expects 64 bytes for the secret key.
            if (decoded.length !== 64) {
                console.error(`❌ Environment variable ${keyName} has incorrect length after base58 decoding (Expected 64 bytes for Secret Key, Got ${decoded.length}).`);
                return false;
            }
            console.log(`[Env Check] Private Key ${keyName} validated (Length: 64).`);
        } catch (e) {
            console.error(`❌ Failed to decode environment variable ${keyName} as base58: ${e.message}`);
            return false;
        }
    } else if (isRequired) { // If key is required but not present
        return false;
    }
    return true; // Valid format, or optional and not set
};

// MAIN_BOT_PRIVATE_KEY is required
if (!validateBase58Key('MAIN_BOT_PRIVATE_KEY', true)) missingVars = true;
// REFERRAL_PAYOUT_PRIVATE_KEY is optional, only validate if present
if (!validateBase58Key('REFERRAL_PAYOUT_PRIVATE_KEY', false)) missingVars = true;


if (missingVars) {
    console.error("⚠️ Please set all required environment variables correctly. Exiting.");
    process.exit(1);
}

// Optional vars with defaults (using BigInt where appropriate)
const OPTIONAL_ENV_DEFAULTS = {
    // --- Operational ---
    // ADMIN_USER_IDS handled by REQUIRED check now
    'DEPOSIT_ADDRESS_EXPIRY_MINUTES': '60', // How long a unique deposit address is valid
    'DEPOSIT_CONFIRMATIONS': 'confirmed',   // Solana commitment level ('confirmed' or 'finalized')
    'WITHDRAWAL_FEE_LAMPORTS': '5000',      // Flat withdrawal fee (can be 0)
    'MIN_WITHDRAWAL_LAMPORTS': '10000000',  // Minimum withdrawal amount (0.01 SOL)
    // --- Payouts (Withdrawals, Referral Rewards) ---
    'PAYOUT_BASE_PRIORITY_FEE_MICROLAMPORTS': '1000',   // Min priority fee for payouts
    'PAYOUT_MAX_PRIORITY_FEE_MICROLAMPORTS': '1000000', // Max priority fee for payouts
    'PAYOUT_COMPUTE_UNIT_LIMIT': '200000',              // Default compute unit limit for transfers
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
    'CF_HOUSE_EDGE': '0.03',          // 3% edge
    'RACE_HOUSE_EDGE': '0.05',        // 5% edge
    'SLOTS_HOUSE_EDGE': '0.10',       // Effective edge for slots payouts
    'ROULETTE_HOUSE_EDGE': '0.05',    // Standard Roulette edge (0, 00) ~5.26%, simplified
    'WAR_HOUSE_EDGE': '0.02',         // Approx edge in War (mainly from ties)
    // --- Referral System ---
    'REFERRAL_INITIAL_BET_MIN_LAMPORTS': '10000000', // Min first bet size to trigger initial reward (0.01 SOL)
    'REFERRAL_MILESTONE_REWARD_PERCENT': '0.005',    // 0.5% of wagered amount for milestones
    // --- Technical / Performance ---
    'RPC_MAX_CONCURRENT': '8',        // Concurrent requests for RateLimitedConnection
    'RPC_RETRY_BASE_DELAY': '600',
    'RPC_MAX_RETRIES': '3',
    'RPC_RATE_LIMIT_COOLOFF': '1500',
    'RPC_RETRY_MAX_DELAY': '15000',
    'RPC_RETRY_JITTER': '0.2',
    'RPC_COMMITMENT': 'confirmed',      // Default commitment for reads
    'MSG_QUEUE_CONCURRENCY': '5',       // Incoming Telegram message processing
    'MSG_QUEUE_TIMEOUT_MS': '10000',
    'CALLBACK_QUEUE_CONCURRENCY': '8',    // Button callback processing
    'CALLBACK_QUEUE_TIMEOUT_MS': '15000',
    'PAYOUT_QUEUE_CONCURRENCY': '3',      // Withdrawal/Referral payout processing
    'PAYOUT_QUEUE_TIMEOUT_MS': '60000',   // Longer timeout for payouts
    'DEPOSIT_PROCESS_QUEUE_CONCURRENCY': '4', // Processing confirmed deposit transactions
    'DEPOSIT_PROCESS_QUEUE_TIMEOUT_MS': '30000',
    'TELEGRAM_SEND_QUEUE_CONCURRENCY': '1', // MUST BE 1 for rate limits
    'TELEGRAM_SEND_QUEUE_INTERVAL_MS': '1050', // ~1 message per second allowance
    'TELEGRAM_SEND_QUEUE_INTERVAL_CAP': '1',
    'DEPOSIT_MONITOR_INTERVAL_MS': '20000', // How often to check for deposits (polling method)
    'DEPOSIT_MONITOR_ADDRESS_BATCH_SIZE': '50', // How many pending addresses to check per cycle
    'DEPOSIT_MONITOR_SIGNATURE_FETCH_LIMIT': '5', // How many recent signatures to check per address
    'DB_POOL_MAX': '25',              // Max DB connections
    'DB_POOL_MIN': '5',
    'DB_IDLE_TIMEOUT': '30000',
    'DB_CONN_TIMEOUT': '5000',
    'DB_SSL': 'true',
    'DB_REJECT_UNAUTHORIZED': 'true',
    'USER_STATE_CACHE_TTL_MS': '300000', // 5 minutes for conversational state
    'WALLET_CACHE_TTL_MS': '600000',     // 10 minutes for user wallet/ref code cache
    'DEPOSIT_ADDR_CACHE_TTL_MS': '3660000', // ~1 hour for active deposit addresses cache
    'MAX_PROCESSED_TX_CACHE': '5000',     // Max deposit TX sigs to keep in memory this session
    'USER_COMMAND_COOLDOWN_MS': '1000',   // 1 second cooldown
    'INIT_DELAY_MS': '1000',              // Delay before starting background tasks
    'SHUTDOWN_QUEUE_TIMEOUT_MS': '20000', // Max time to wait for queues during shutdown
    'SHUTDOWN_FAIL_TIMEOUT_MS': '8000',   // Timeout before force exit on critical failure
    'WEBHOOK_MAX_CONN': '10',             // Max connections for Telegram webhook
};

// Assign defaults for optional vars if they are missing
Object.entries(OPTIONAL_ENV_DEFAULTS).forEach(([key, defaultValue]) => {
    // Skip ADMIN_USER_IDS as it's now required
    if (key === 'ADMIN_USER_IDS') return;
    if (process.env[key] === undefined) { // Check for undefined specifically
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
    { maxCount: 10, percent: 0.05 },   // 0-10 referrals -> 5%
    { maxCount: 25, percent: 0.10 },   // 11-25 referrals -> 10%
    { maxCount: 50, percent: 0.15 },   // 26-50 referrals -> 15%
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
    httpHeaders: { 'User-Agent': `SolanaGamblesBot/3.1.5` }, // Updated version
    // clientId: `SolanaGamblesBot/3.1.5` // Set within RateLimitedConnection now (Updated version)
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
    request: {       // Adjust request options for stability
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
    },
    slots: {
        minBetLamports: BigInt(process.env.SLOTS_MIN_BET_LAMPORTS),
        maxBetLamports: BigInt(process.env.SLOTS_MAX_BET_LAMPORTS),
        houseEdge: parseFloat(process.env.SLOTS_HOUSE_EDGE)
    },
    roulette: {
        minBetLamports: BigInt(process.env.ROULETTE_MIN_BET_LAMPORTS),
        maxBetLamports: BigInt(process.env.ROULETTE_MAX_BET_LAMPORTS),
        houseEdge: parseFloat(process.env.ROULETTE_HOUSE_EDGE)
    },
    war: {
        minBetLamports: BigInt(process.env.WAR_MIN_BET_LAMPORTS),
        maxBetLamports: BigInt(process.env.WAR_MAX_BET_LAMPORTS),
        houseEdge: parseFloat(process.env.WAR_HOUSE_EDGE)
    }
};
// Basic validation function (can be expanded)
function validateGameConfig(config) {
    const errors = [];
    for (const game in config) {
        const gc = config[game];
        if (!gc) { errors.push(`Config for game "${game}" missing.`); continue; }
        if (typeof gc.minBetLamports !== 'bigint' || gc.minBetLamports <= 0n) errors.push(`${game}.minBetLamports invalid (${gc.minBetLamports})`);
        if (typeof gc.maxBetLamports !== 'bigint' || gc.maxBetLamports <= 0n) errors.push(`${game}.maxBetLamports invalid (${gc.maxBetLamports})`);
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
// --- VERSION: 3.1.5 --- (Fixed BigInt logging in queryDatabase, ADDED DEBUG LOGGING)

// --- Assuming 'pool' is imported or available from Part 1 ---
// import { pool } from './db';
// import { PublicKey } from '@solana/web3.js'; // Needed for linkUserWallet validation
// import { generateReferralCode, updateWalletCache, getWalletCache, addActiveDepositAddressCache, getActiveDepositAddressCache, removeActiveDepositAddressCache } from './utils'; // Assuming utils are available

// --- Helper Function for DB Operations ---
/**
 * Executes a database query using either a provided client or the pool.
 * @param {string} sql The SQL query string with placeholders ($1, $2, ...).
 * @param {Array<any>} [params=[]] The parameters for the SQL query.
 * @param {pg.PoolClient | pg.Pool} [dbClient=pool] The DB client or pool to use.
 * @returns {Promise<pg.QueryResult<any>>} The query result.
 */
async function queryDatabase(sql, params = [], dbClient = pool) {
    // <<< DEBUG LOG #1: Log entry and SQL type/value >>>
    console.log(`[DEBUG] queryDatabase called. typeof sql: ${typeof sql}. SQL (first 100 chars): ${String(sql).substring(0, 100)}`);

    try {
        // Optional: Log execution attempt
        // console.log(`[DB EXEC] SQL: ${String(sql).substring(0,100)}... Params: ${JSON.stringify(params)}`);
        return await dbClient.query(sql, params);
    } catch (error) {
        // Log enhanced error information
        console.error(`❌ DB Query Error:`);

        // <<< DEBUG LOG #2: Log SQL type/value INSIDE CATCH block >>>
        console.log(`[DEBUG] queryDatabase CATCH block. typeof sql: ${typeof sql}. SQL value:`, sql);

        // <<< MODIFIED: Safely handle potentially undefined 'sql' before logging >>>
        if (typeof sql === 'string') {
             console.error(`   SQL: ${sql.substring(0, 500)}${sql.length > 500 ? '...' : ''}`);
        } else {
             console.error(`   SQL: [DEBUG ERROR: 'sql' variable is NOT a string, type was ${typeof sql}]`);
        }
        // <<< END MODIFICATION >>>

        // *** FIXED BigInt Logging Issue START ***
        // Safely stringify parameters, converting BigInts to strings for logging
        const safeParamsString = JSON.stringify(params, (key, value) =>
            typeof value === 'bigint'
                ? value.toString() // Convert BigInt to string
                : value // Return other values unchanged
        );
        console.error(`   Params: ${safeParamsString}`); // Log the safe string
        // *** FIXED BigInt Logging Issue END ***

        console.error(`   Error Code: ${error.code}`);
        console.error(`   Error Message: ${error.message}`);
        if (error.constraint) { // Log constraint name if available (useful for unique/fk errors)
            console.error(`   Constraint: ${error.constraint}`);
        }
        // console.error(`   Stack: ${error.stack}`); // Optional: full stack trace
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
        new PublicKey(externalAddress); // Assumes PublicKey is imported/available

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
            currentReferralCode = generateReferralCode(); // generateReferralCode defined later in utils (Part 3)
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
        updateWalletCache(userId, { withdrawalAddress: externalAddress, referralCode: currentReferralCode }); // Cache helpers defined in Part 3

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
    const cached = getWalletCache(userId); // Cache helpers defined in Part 3
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
            console.log(`[DB linkReferral] User ${refereeUserId} already linked to referrer ${referrerUserId}.`)
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
    const logPrefix = `[DB CreateDepositAddr User ${userId} Addr ${depositAddress.slice(0, 6)}..]`; // [LOGGING ADDED] Prefix
    console.log(`${logPrefix} Attempting to create record.`); // [LOGGING ADDED] Entry log
    const query = `
        INSERT INTO deposit_addresses (user_id, deposit_address, derivation_path, expires_at, status, created_at)
        VALUES ($1, $2, $3, $4, 'pending', NOW())
        RETURNING id;
    `;
    try {
        const res = await queryDatabase(query, [String(userId), depositAddress, derivationPath, expiresAt]);
        if (res.rowCount > 0) {
            const depositAddressId = res.rows[0].id;
            console.log(`${logPrefix} Successfully created record with ID: ${depositAddressId}. Adding to cache.`); // [LOGGING ADDED] Success log
            // Add to cache immediately
            addActiveDepositAddressCache(depositAddress, userId, expiresAt.getTime()); // Cache helpers defined in Part 3
            return { success: true, id: depositAddressId };
        } else {
            console.error(`${logPrefix} Failed to insert deposit address record (no ID returned).`); // [LOGGING ADDED] Failure log
            return { success: false, error: 'Failed to insert deposit address record (no ID returned).' };
        }
    } catch (err) {
        console.error(`${logPrefix} DB Error:`, err.message, err.code); // [LOGGING ADDED] DB Error log
        if (err.code === '23505' && err.constraint === 'deposit_addresses_deposit_address_key') {
            console.error(`${logPrefix} CRITICAL - Deposit address collision detected.`);
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
    const logPrefix = `[DB FindDepositAddr Addr ${depositAddress.slice(0, 6)}..]`; // [LOGGING ADDED] Prefix
    const cached = getActiveDepositAddressCache(depositAddress); // Cache helpers defined in Part 3
    const now = Date.now();

    if (cached && now < cached.expiresAt) {
        // console.log(`${logPrefix} Cache hit. Verifying DB status...`); // [LOGGING ADDED] Cache hit log (Optional: Verbose)
        try {
            const statusRes = await queryDatabase(
                'SELECT status, id, expires_at FROM deposit_addresses WHERE deposit_address = $1',
                [depositAddress]
            );
            if (statusRes.rowCount > 0) {
                const dbInfo = statusRes.rows[0];
                const dbExpiresAt = new Date(dbInfo.expires_at).getTime();
                if (cached.expiresAt !== dbExpiresAt) {
                    console.warn(`${logPrefix} Cache expiry mismatch for ${depositAddress}. Updating cache.`);
                    addActiveDepositAddressCache(depositAddress, cached.userId, dbExpiresAt); // Cache helpers defined in Part 3
                }
                // console.log(`${logPrefix} Cache valid, DB status: ${dbInfo.status}.`); // [LOGGING ADDED] Cache valid log (Optional: Verbose)
                return { userId: cached.userId, status: dbInfo.status, id: dbInfo.id, expiresAt: new Date(dbInfo.expires_at) };
            } else {
                removeActiveDepositAddressCache(depositAddress); // Cache helpers defined in Part 3
                console.warn(`${logPrefix} Address was cached but not found in DB. Cache removed.`); // [LOGGING ADDED] Mismatch log
                return null;
            }
        } catch (err) {
            console.error(`${logPrefix} Error fetching status for cached addr:`, err.message); // [LOGGING ADDED] Error log
            return null; // Error fetching status
        }
    } else if (cached) {
        console.log(`${logPrefix} Cache expired. Removing.`); // [LOGGING ADDED] Cache expired log
        removeActiveDepositAddressCache(depositAddress); // Expired cache entry
    }

    // console.log(`${logPrefix} Cache miss or expired. Querying DB...`); // [LOGGING ADDED] Cache miss log (Optional: Verbose)
    try {
        const res = await queryDatabase(
            'SELECT user_id, status, id, expires_at FROM deposit_addresses WHERE deposit_address = $1',
            [depositAddress]
        );
        if (res.rowCount > 0) {
            const data = res.rows[0];
            const expiresAtDate = new Date(data.expires_at);
            // console.log(`${logPrefix} Found in DB. User: ${data.user_id}, Status: ${data.status}.`); // [LOGGING ADDED] DB found log (Optional: Verbose)
            if (data.status === 'pending' && now < expiresAtDate.getTime()) {
                // console.log(`${logPrefix} Adding to active cache.`); // [LOGGING ADDED] Cache add log (Optional: Verbose)
                addActiveDepositAddressCache(depositAddress, data.user_id, expiresAtDate.getTime());
            }
            return { userId: data.user_id, status: data.status, id: data.id, expiresAt: expiresAtDate };
        }
        // console.log(`${logPrefix} Not found in DB.`); // [LOGGING ADDED] DB not found log (Optional: Verbose)
        return null; // Not found in DB
    } catch (err) {
        console.error(`${logPrefix} Error finding info in DB:`, err.message); // [LOGGING ADDED] DB Error log
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
    const logPrefix = `[DB MarkDepositUsed ID ${depositAddressId}]`; // [LOGGING ADDED] Prefix
    // console.log(`${logPrefix} Attempting to mark as 'used'.`); // [LOGGING ADDED] Entry log (Optional: Verbose)
    try {
        const res = await queryDatabase(
            `UPDATE deposit_addresses SET status = 'used' WHERE id = $1 AND status = 'pending'`,
            [depositAddressId],
            client
        );
        if (res.rowCount > 0) {
             // console.log(`${logPrefix} Successfully marked as 'used'. Invalidating cache entry.`); // [LOGGING ADDED] Success log (Optional: Verbose)
             const addressRes = await queryDatabase('SELECT deposit_address FROM deposit_addresses WHERE id = $1', [depositAddressId], client);
             if (addressRes.rowCount > 0) {
                 removeActiveDepositAddressCache(addressRes.rows[0].deposit_address); // Cache helpers defined in Part 3
                 // console.log(`${logPrefix} Removed address ${addressRes.rows[0].deposit_address.slice(0,6)}.. from active cache.`); // [LOGGING ADDED] Cache remove log (Optional: Verbose)
             }
            return true;
        } else {
            console.warn(`${logPrefix} Failed to mark as 'used' (status might not be 'pending' or ID not found).`); // [LOGGING ADDED] Failure log
            return false;
        }
    } catch (err) {
        console.error(`${logPrefix} Error updating status to 'used':`, err.message); // [LOGGING ADDED] Error log
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
    const logPrefix = `[DB RecordDeposit User ${userId} TX ${txSignature.slice(0, 6)}..]`; // [LOGGING ADDED] Prefix
    // console.log(`${logPrefix} Recording confirmed deposit. AddrID: ${depositAddressId}, Amount: ${amountLamports}`); // [LOGGING ADDED] Entry log (Optional: Verbose)
    const query = `
        INSERT INTO deposits (user_id, deposit_address_id, tx_signature, amount_lamports, status, created_at, processed_at)
        VALUES ($1, $2, $3, $4, 'confirmed', NOW(), NOW())
        ON CONFLICT (tx_signature) DO NOTHING -- Prevent duplicate inserts silently
        RETURNING id;
    `;
    try {
        const res = await queryDatabase(query, [String(userId), depositAddressId, txSignature, BigInt(amountLamports)], client);
        if (res.rowCount > 0) {
            const depositId = res.rows[0].id;
            // console.log(`${logPrefix} Successfully recorded deposit with ID: ${depositId}`); // [LOGGING ADDED] Success log (Optional: Verbose)
            return { success: true, depositId: depositId };
        } else {
            console.warn(`${logPrefix} Deposit TX ${txSignature} already processed (ON CONFLICT triggered).`);
            return { success: false, error: 'Deposit transaction already processed.' };
        }
    } catch (err) {
        console.error(`${logPrefix} Error inserting deposit record:`, err.message, err.code); // [LOGGING ADDED] Error log
        if (err.code === '23505' && err.constraint === 'deposits_tx_signature_key') {
            console.error(`${logPrefix} Deposit TX ${txSignature} already processed (Unique constraint violation despite ON CONFLICT?).`);
            return { success: false, error: 'Deposit transaction already processed.' };
        }
        return { success: false, error: `Database error recording deposit (${err.code || 'N/A'})` };
    }
}

/**
 * Gets the next available index for deriving a deposit address for a user.
 * @param {string} userId
 * @returns {Promise<number>} The next index (e.g., 0 for the first address).
 * @throws {Error} If the database query fails.
 */
async function getNextDepositAddressIndex(userId) {
    const logPrefix = `[DB GetNextDepositIndex User ${userId}]`; // [LOGGING ADDED] Prefix
    // console.log(`${logPrefix} Fetching address count...`); // [LOGGING ADDED] Entry log (Optional: Verbose)
    try {
        const res = await queryDatabase(
            'SELECT COUNT(*) as count FROM deposit_addresses WHERE user_id = $1',
            [String(userId)]
        );
        const nextIndex = parseInt(res.rows[0]?.count || '0', 10);
        // console.log(`${logPrefix} Found count: ${nextIndex}. Next index will be ${nextIndex}.`); // [LOGGING ADDED] Success log (Optional: Verbose)
        return nextIndex;
    } catch (err) {
        console.error(`${logPrefix} Error fetching count:`, err); // [LOGGING ADDED] Error log
        throw new Error(`Failed to determine next deposit address index: ${err.message}`);
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
 * @param {number} withdrawalId The ID of the withdrawal record.
 * @param {'processing' | 'completed' | 'failed'} status The new status.
 * @param {pg.PoolClient | null} client Optional DB client if within a transaction.
 * @param {string | null} [signature=null] The payout transaction signature (required for 'completed').
 * @param {string | null} [errorMessage=null] The error message (required for 'failed').
 * @returns {Promise<boolean>} True on successful update, false otherwise.
 */
async function updateWithdrawalStatus(withdrawalId, status, client = null, signature = null, errorMessage = null) {
    const db = client || pool;
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
             return false;
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
    if (BigInt(wagerAmountLamports) <= 0n) {
        return { success: false, error: 'Wager amount must be positive.' };
    }

    const query = `
        INSERT INTO bets (user_id, chat_id, game_type, bet_details, wager_amount_lamports, status, created_at, priority)
        VALUES ($1, $2, $3, $4, $5, 'active', NOW(), $6)
        RETURNING id;
    `;
    try {
        const validBetDetails = betDetails && typeof betDetails === 'object' ? betDetails : {};
        const res = await queryDatabase(query, [String(userId), String(chatId), gameType, validBetDetails, BigInt(wagerAmountLamports), priority], client);
        if (res.rowCount > 0) {
            return { success: true, betId: res.rows[0].id };
        } else {
            return { success: false, error: 'Failed to insert bet record (no ID returned).' };
        }
    } catch (err) {
        console.error(`[DB CreateBet] Error for user ${userId}, game ${gameType}:`, err.message, err.code);
        if (err.constraint === 'bets_wager_positive') {
             return { success: false, error: 'Wager amount must be positive (DB check).' };
        }
        return { success: false, error: `Database error creating bet (${err.code || 'N/A'})` };
    }
}

/**
 * Updates a bet record upon completion of game logic.
 * @param {pg.PoolClient} client The active database client.
 * @param {number} betId The ID of the bet to update.
 * @param {'processing_game' | 'completed_win' | 'completed_loss' | 'completed_push' | 'error_game_logic'} status The new status.
 * @param {bigint | null} [payoutAmountLamports=null] Amount credited back. Required for completed statuses.
 * @returns {Promise<boolean>} True on successful update, false otherwise.
 */
async function updateBetStatus(client, betId, status, payoutAmountLamports = null) {
    const logPrefix = `[UpdateBetStatus ID:${betId}]`;

    if (['completed_win', 'completed_push'].includes(status) && (payoutAmountLamports === null || BigInt(payoutAmountLamports) <= 0n)) {
        if (status !== 'completed_push' || payoutAmountLamports === null) {
           console.error(`${logPrefix} Invalid payout amount ${payoutAmountLamports} for status ${status}.`);
           return false;
        }
    }
    if (['completed_loss', 'error_game_logic'].includes(status)) {
        payoutAmountLamports = 0n;
    }
    if (status === 'processing_game') {
         payoutAmountLamports = null;
    }

    const query = `
        UPDATE bets
        SET status = $1,
            payout_amount_lamports = $2,
            processed_at = CASE WHEN $1 LIKE 'completed_%' OR $1 = 'error_game_logic' THEN NOW() ELSE processed_at END
        WHERE id = $3 AND status IN ('active', 'processing_game', 'error_game_logic')
        RETURNING id, status;
    `;
    try {
        const res = await queryDatabase(query, [status, payoutAmountLamports !== null ? BigInt(payoutAmountLamports) : null, betId], client);
        if (res.rowCount === 0) {
            const currentStatusRes = await queryDatabase('SELECT status FROM bets WHERE id = $1', [betId], client);
            const currentStatus = currentStatusRes.rows[0]?.status || 'NOT_FOUND';
            console.warn(`${logPrefix} Failed to update status to ${status}. Current status: ${currentStatus}.`);
            return false;
        }
        return true;
    } catch (err) {
        console.error(`${logPrefix} Error updating status to ${status}:`, err.message);
        return false;
    }
}

/**
 * Checks if a specific completed bet ID is the user's first *ever* completed bet.
 * @param {string} userId
 * @param {number} betId The ID of the bet to check.
 * @returns {Promise<boolean>} True if it's the first completed bet, false otherwise.
 */
async function isFirstCompletedBet(userId, betId) {
    userId = String(userId);
    const query = `
        SELECT id
        FROM bets
        WHERE user_id = $1 AND status LIKE 'completed_%'
        ORDER BY created_at ASC, id ASC
        LIMIT 1;
    `;
    try {
        const res = await queryDatabase(query, [userId]);
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
 * @param {pg.PoolClient | null} [client=null] Optional DB client if called within a transaction.
 * @returns {Promise<{success: boolean, payoutId?: number, error?: string}>}
 */
async function recordPendingReferralPayout(referrerUserId, refereeUserId, payoutType, payoutAmountLamports, triggeringBetId = null, milestoneReachedLamports = null, client = null) {
    const db = client || pool;
    const query = `
        INSERT INTO referral_payouts (
            referrer_user_id, referee_user_id, payout_type, payout_amount_lamports,
            triggering_bet_id, milestone_reached_lamports, status, created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, 'pending', NOW())
        ON CONFLICT ON CONSTRAINT idx_refpayout_unique_milestone DO NOTHING
        RETURNING id;
    `;
    const values = [
        String(referrerUserId), String(refereeUserId), payoutType, BigInt(payoutAmountLamports),
        Number.isInteger(triggeringBetId) ? triggeringBetId : null,
        milestoneReachedLamports !== null ? BigInt(milestoneReachedLamports) : null
    ];
    try {
        const res = await queryDatabase(query, values, db);
        if (res.rows.length > 0) {
            const payoutId = res.rows[0].id;
            console.log(`[DB Referral] Recorded pending ${payoutType} payout ID ${payoutId} for referrer ${referrerUserId} (triggered by ${refereeUserId}).`);
            return { success: true, payoutId: payoutId };
        } else {
             if (payoutType === 'milestone') {
                 console.warn(`[DB Referral] Duplicate milestone payout attempt for referrer ${referrerUserId}, referee ${refereeUserId}, milestone ${milestoneReachedLamports}. Ignoring.`);
                 return { success: false, error: 'Duplicate milestone payout attempt.' };
             } else {
                  console.error("[DB Referral] Failed to insert pending referral payout, RETURNING clause gave no ID (and not a milestone conflict).");
                  return { success: false, error: "Failed to insert pending referral payout." };
             }
        }
    } catch (err) {
        console.error(`[DB Referral] Error recording pending ${payoutType} payout for referrer ${referrerUserId} (triggered by ${refereeUserId}):`, err.message);
        return { success: false, error: `Database error recording referral payout (${err.code || 'N/A'})` };
    }
}

/**
 * Updates referral payout status, signature, error message, timestamps.
 * @param {number} payoutId The ID of the referral_payouts record.
 * @param {'processing' | 'paid' | 'failed'} status The new status.
 * @param {pg.PoolClient | null} client Optional DB client if within a transaction.
 * @param {string | null} [signature=null] The payout transaction signature (required for 'paid').
 * @param {string | null} [errorMessage=null] The error message (required for 'failed').
 * @returns {Promise<boolean>} True on successful update, false otherwise.
 */
async function updateReferralPayoutStatus(payoutId, status, client = null, signature = null, errorMessage = null) {
    const db = client || pool;
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
            return false;
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

// Export functions if needed by other potential modules (adjust as needed)
export {
    queryDatabase,
    ensureUserExists,
    linkUserWallet,
    getUserWalletDetails,
    getLinkedWallet,
    getUserByReferralCode,
    linkReferral,
    updateUserWagerStats,
    getUserBalance,
    updateUserBalanceAndLedger,
    createDepositAddressRecord,
    findDepositAddressInfo,
    markDepositAddressUsed,
    recordConfirmedDeposit,
    getNextDepositAddressIndex,
    createWithdrawalRequest,
    updateWithdrawalStatus,
    getWithdrawalDetails,
    createBetRecord,
    updateBetStatus,
    isFirstCompletedBet,
    getBetDetails,
    recordPendingReferralPayout,
    updateReferralPayoutStatus,
    getReferralPayoutDetails,
    getTotalReferralEarnings
};
// index.js - Part 3: Solana Utilities & Telegram Helpers
// --- VERSION: 3.1.5 --- (Implemented safeIndex workaround, added notifyAdmin helper, ADDED getKeypairFromPath for sweeping)

// --- Assuming these imports are available from Part 1 ---
// import { PublicKey, Keypair, SystemProgram, LAMPORTS_PER_SOL, sendAndConfirmTransaction, ComputeBudgetProgram, TransactionExpiredBlockheightExceededError, SendTransactionError } from '@solana/web3.js';
// import bs58 from 'bs58';
// import { createHash } from 'crypto';
// import * as crypto from 'crypto'; // Keep if needed elsewhere
// import bip39 from 'bip39';
// import { derivePath } from 'ed25519-hd-key';
// import { bot } from './bot'; // Assuming bot instance is exported/available
// import { solanaConnection } from './connection'; // Assuming connection instance is exported/available
// import { userStateCache, walletCache, activeDepositAddresses, processedDepositTxSignatures, commandCooldown, MAX_PROCESSED_TX_CACHE_SIZE, WALLET_CACHE_TTL_MS, DEPOSIT_ADDR_CACHE_TTL_MS, USER_COMMAND_COOLDOWN_MS } from './state'; // Assuming state/caches exported
// import { telegramSendQueue } from './queues'; // Assuming queue is exported/available
// import { notifyAdmin, safeSendMessage } from './utils'; // Forward declaration ok, or ensure notifyAdmin is defined before use if split strictly
// ------------------------------------------------------


// --- Solana Utilities --

// *** ADDED Helper function to create safe index ***
// Uses import { createHash } from 'crypto'; -> Ensure this is added in Part 1 imports
function createSafeIndex(userId) {
    // Hash the userId to fixed-size output
    const hash = createHash('sha256')
        .update(String(userId))
        .digest();

    // Take last 4 bytes and convert to UInt32 (always < 2^32)
    // Using last 4 bytes as in the proposal. First 4 bytes (original method) would also work.
    const index = hash.readUInt32BE(hash.length - 4);

    // Ensure it's below hardening threshold (2^31)
    return index % 2147483648; // 0 <= index < 2^31
}

/**
 * Derives a unique Solana keypair for deposits based on user ID and an index.
 * Uses BIP39 seed phrase and SLIP-10 path (m/44'/501'/account'/change'/addressIndex').
 * Uses ed25519-hd-key library (with safeIndex workaround).
 * Returns the public key and the 32-byte private key bytes needed for signing.
 * @param {string} userId The user's unique ID.
 * @param {number} addressIndex A unique index for this user's addresses (e.g., 0, 1, 2...).
 * @returns {Promise<{publicKey: PublicKey, privateKeyBytes: Uint8Array, derivationPath: string} | null>} Derived public key, private key bytes, and path, or null on error.
 */
async function generateUniqueDepositAddress(userId, addressIndex) { // Make sure 'addressIndex' is the correct variable name passed in
    const logPrefix = `[Address Gen User ${userId} Index ${addressIndex}]`;
    console.log(`${logPrefix} Starting address generation using ed25519-hd-key (with safeIndex)...`); // Updated log

    try {
        const seedPhrase = process.env.DEPOSIT_MASTER_SEED_PHRASE;
        if (typeof seedPhrase !== 'string' || seedPhrase.length < 20 || !bip39.validateMnemonic(seedPhrase)) { // Added validation check here too
             console.error(`${logPrefix} CRITICAL: DEPOSIT_MASTER_SEED_PHRASE is invalid or empty/invalid at runtime!`);
             // Avoid throwing raw error, return null for consistency
             // throw new Error("DEPOSIT_MASTER_SEED_PHRASE environment variable not read correctly or invalid at runtime.");
             await notifyAdmin("🚨 CRITICAL: DEPOSIT_MASTER_SEED_PHRASE is missing or invalid. Cannot generate deposit addresses.");
             return null;
        }

        if (typeof userId !== 'string' || userId.length === 0 || typeof addressIndex !== 'number' || addressIndex < 0 || !Number.isInteger(addressIndex)) {
            console.error(`${logPrefix} Invalid userId or addressIndex`, { userId, addressIndex });
            return null;
        }
        // console.log(`${logPrefix} Inputs validated. User: ${userId}, Index: ${addressIndex}`); // Less verbose

        // 1. Get the master seed buffer from the mnemonic phrase
        // console.log(`${logPrefix} Generating seed buffer from mnemonic (using bip39)...`); // Less verbose
        const masterSeedBuffer = bip39.mnemonicToSeedSync(seedPhrase); // Sync often used with ed25519-hd-key
        if (!masterSeedBuffer || masterSeedBuffer.length === 0) {
            throw new Error("Failed to generate seed buffer from mnemonic.");
        }
        // console.log(`${logPrefix} Seed buffer generated successfully.`); // Less verbose

        // 2. Derive the user-specific SAFE account path component (Hardened)
        // console.log(`${logPrefix} Deriving safe account index from user ID...`); // Less verbose
        const safeIndex = createSafeIndex(userId); // Call the helper function
        // console.log(`${logPrefix} Safe account index derived: ${safeIndex}' (Hardened)`); // Less verbose

        // 3. Construct the *full* BIP44 derivation path for Solana using the safe index
        const derivationPath = `m/44'/501'/${safeIndex}'/0'/${addressIndex}'`; // Use safeIndex, standard change level '0', final addressIndex
        // console.log(`${logPrefix} Constructed derivation path: ${derivationPath}`); // Less verbose

        // ---- Use ed25519-hd-key ----
        // console.log(`${logPrefix} Deriving key using ed25519-hd-key derivePath...`); // Less verbose
        const derivedSeed = derivePath(derivationPath, masterSeedBuffer.toString('hex')); // derivePath often expects hex seed
        const privateKeyBytes = derivedSeed.key; // The 'key' property holds the private key bytes
        // --------------------------

        if (!privateKeyBytes || privateKeyBytes.length !== 32) {
            throw new Error(`Derived private key bytes are invalid (length: ${privateKeyBytes?.length}) using ed25519-hd-key`);
        }
        // console.log(`${logPrefix} Private key bytes extracted successfully (Length: ${privateKeyBytes.length}) using ed25519-hd-key.`); // Less verbose

        // 7. Generate the Solana Keypair using Keypair.fromSeed()
        // console.log(`${logPrefix} Generating Solana Keypair from derived seed bytes...`); // Less verbose
        const keypair = Keypair.fromSeed(privateKeyBytes); // Generate the Keypair object
        const publicKey = keypair.publicKey; // Get the PublicKey object from the keypair
        console.log(`${logPrefix} Solana Keypair generated. Public Key: ${publicKey.toBase58()}`);

        // 8. Return public key, path, and the 32-byte private key bytes
        // console.log(`${logPrefix} Address generation complete using ed25519-hd-key (with safeIndex).`); // Less verbose
        return {
            publicKey: publicKey, // The PublicKey object
            privateKeyBytes: privateKeyBytes, // The raw 32-byte private key
            derivationPath: derivationPath // The path used
        };

    } catch (error) {
        console.error(`${logPrefix} Error during address generation using ed25519-hd-key: ${error.message}`);
        console.error(`${logPrefix} Error Stack: ${error.stack}`); // Keep stack trace logging
        // Notify admin on generation failure
        await notifyAdmin(`🚨 ERROR generating deposit address for User ${userId} Index ${addressIndex} (${logPrefix}): ${error.message}`);
        return null;
    }
}


// --- NEW: Function to get Keypair from Derivation Path (for Sweeping) ---
/**
 * Re-derives a Solana Keypair from a stored BIP44 derivation path and the master seed phrase.
 * CRITICAL: Requires DEPOSIT_MASTER_SEED_PHRASE to be available and secure.
 * This is synchronous as key derivation libraries often are.
 * @param {string} derivationPath The full BIP44 derivation path (e.g., "m/44'/501'/12345'/0'/0'").
 * @returns {Keypair | null} The derived Solana Keypair object, or null on error.
 */
function getKeypairFromPath(derivationPath) {
    const logPrefix = `[GetKeypairFromPath Path:${derivationPath}]`;
    try {
        // Basic path validation (adjust regex if needed for stricter checking)
        if (!derivationPath || typeof derivationPath !== 'string' || !derivationPath.startsWith("m/44'/501'/")) {
             console.error(`${logPrefix} Invalid derivation path format provided.`);
             return null;
        }

        const seedPhrase = process.env.DEPOSIT_MASTER_SEED_PHRASE;
        if (typeof seedPhrase !== 'string' || seedPhrase.length < 20 || !bip39.validateMnemonic(seedPhrase)) {
             console.error(`${logPrefix} CRITICAL: DEPOSIT_MASTER_SEED_PHRASE is missing or invalid when trying to re-derive key!`);
             // Cannot proceed without the seed. Do NOT notifyAdmin excessively here, but log critical error.
             return null;
        }

        // 1. Get master seed buffer
        const masterSeedBuffer = bip39.mnemonicToSeedSync(seedPhrase);
        if (!masterSeedBuffer || masterSeedBuffer.length === 0) {
            throw new Error("Failed to generate seed buffer from mnemonic.");
        }

        // 2. Derive key using path
        const derivedSeed = derivePath(derivationPath, masterSeedBuffer.toString('hex'));
        const privateKeyBytes = derivedSeed.key;

        if (!privateKeyBytes || privateKeyBytes.length !== 32) {
            throw new Error(`Derived private key bytes are invalid (length: ${privateKeyBytes?.length})`);
        }

        // 3. Generate Solana Keypair
        const keypair = Keypair.fromSeed(privateKeyBytes);
        console.log(`${logPrefix} Successfully re-derived keypair. Pubkey: ${keypair.publicKey.toBase58()}`);
        return keypair;

    } catch (error) {
        console.error(`${logPrefix} Error re-deriving keypair from path: ${error.message}`);
        // Stack trace might be useful here too
        // console.error(error.stack);
        // Avoid notifying admin repeatedly if seed phrase is missing, but maybe notify on unexpected derivation errors?
        if (!error.message.includes('DEPOSIT_MASTER_SEED_PHRASE')) {
            // Avoid awaiting notifyAdmin in synchronous function if possible
            // Consider logging prominently or using a different notification mechanism if needed
             console.error(`[ADMIN ALERT] ${logPrefix} Unexpected error re-deriving key: ${error.message}`);
        }
        return null;
    }
}
// --- End NEW Function ---


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
        'transaction simulation failed', 'failed to simulate transaction', // Often temporary RPC issues
        'blockhash not found', 'slot leader does not match', 'node is behind',
        'transaction was not confirmed', 'block not available', 'block cleanout',
        'sending transaction', 'connection closed', 'load balancer error',
        'backend unhealthy', 'overloaded', 'proxy internal error', 'too many requests' // Added 429 message check
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
    if (retryableCodes.includes(code) || retryableCodes.includes(String(code)) || retryableCodes.includes(errorCode) || retryableCodes.includes(String(errorCode))) return true; // Added String conversion checks

    // Specific error types or reasons
    if (error.reason === 'rpc_error' || error.reason === 'network_error') return true;

    // Handle SendTransactionError specifically (check if underlying cause is retryable)
    if (error instanceof SendTransactionError && error.cause) {
        return isRetryableSolanaError(error.cause);
    }
     // Handle TransactionExpiredBlockheightExceededError as potentially retryable (fetch new blockhash and try again)
     if (error instanceof TransactionExpiredBlockheightExceededError) {
         return true;
     }


    // Check for explicit rate limit error structure from some RPC providers
     if (error?.data?.message?.toLowerCase().includes('rate limit exceeded') || error?.data?.message?.toLowerCase().includes('too many requests')) {
         return true;
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
        await notifyAdmin(`🚨 CRITICAL: Missing private key env var ${privateKeyEnvVar} for payout source ${payoutSource}. Payout failed.`); // Notify admin
        return { success: false, error: errorMsg, isRetryable: false };
    }

    let payerWallet;
    try {
        payerWallet = Keypair.fromSecretKey(bs58.decode(privateKey));
    } catch (e) {
         const errorMsg = `Failed to decode private key ${keyTypeForLog} (${privateKeyEnvVar}): ${e.message}`;
         console.error(`[${operationId}] ❌ ERROR: ${errorMsg}`);
         await notifyAdmin(`🚨 CRITICAL: Failed to decode private key ${keyTypeForLog} (${privateKeyEnvVar}): ${e.message}. Payout failed.`); // Notify admin
         return { success: false, error: errorMsg, isRetryable: false };
    }

    const amountSOL = Number(amountToSend) / LAMPORTS_PER_SOL;
    console.log(`[${operationId}] Attempting to send ${amountSOL.toFixed(SOL_DECIMALS)} SOL from ${payerWallet.publicKey.toBase58()} to ${recipientPubKey.toBase58()} using ${keyTypeForLog} key...`);

    try {
        // 1. Get latest blockhash
        console.log(`[${operationId}] Fetching latest blockhash...`);
        // Use 'confirmed' for blockhash fetching, consistent with DEPOSIT_CONFIRMATION_LEVEL default for payouts
        const { blockhash, lastValidBlockHeight } = await solanaConnection.getLatestBlockhash('confirmed');
        if (!blockhash || !lastValidBlockHeight) {
            throw new Error('Failed to get valid latest blockhash object.');
        }
        console.log(`[${operationId}] Got blockhash: ${blockhash}, lastValidBlockHeight: ${lastValidBlockHeight}`);

        // 2. Calculate Priority Fee (Simplified: using fixed base/max for now)
        const basePriorityFee = parseInt(process.env.PAYOUT_BASE_PRIORITY_FEE_MICROLAMPORTS, 10);
        const maxPriorityFee = parseInt(process.env.PAYOUT_MAX_PRIORITY_FEE_MICROLAMPORTS, 10);
        let priorityFeeMicroLamports = Math.max(1, Math.min(basePriorityFee, maxPriorityFee));
        const computeUnitLimit = parseInt(process.env.PAYOUT_COMPUTE_UNIT_LIMIT, 10);
        console.log(`[${operationId}] Using Compute Unit Limit: ${computeUnitLimit}, Priority Fee: ${priorityFeeMicroLamports} microLamports`);

        // 3. Create Transaction
        console.log(`[${operationId}] Creating transaction...`);
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
        console.log(`[${operationId}] Transaction created with ${transaction.instructions.length} instructions.`);

        // 4. Sign and Send Transaction
        console.log(`[${operationId}] Sending and confirming transaction...`);
        const signature = await sendAndConfirmTransaction(
            solanaConnection,
            transaction,
            [payerWallet],
            {
                commitment: DEPOSIT_CONFIRMATION_LEVEL, // Use the standard confirmation level for final confirmation
                skipPreflight: false, // Keep preflight check
                preflightCommitment: DEPOSIT_CONFIRMATION_LEVEL, // Use same level for preflight
                lastValidBlockHeight: lastValidBlockHeight, // Important for expiration check
            }
        );

        console.log(`[${operationId}] SUCCESS! ✅ Sent ${amountSOL.toFixed(SOL_DECIMALS)} SOL to ${recipientPubKey.toBase58()} using ${keyTypeForLog} key. TX: ${signature}`);
        return { success: true, signature: signature };

    } catch (error) {
        console.error(`[${operationId}] ❌ SEND FAILED using ${keyTypeForLog} key. Error: ${error.message}`);
        if (error instanceof SendTransactionError && error.logs) {
            console.error(`[${operationId}] Simulation Logs (if available, last 10):`);
            error.logs.slice(-10).forEach(log => console.error(`   -> ${log}`));
        }
        const isRetryable = isRetryableSolanaError(error);
        let userFriendlyError = `Send/Confirm error: ${error.message}`;
        const errorMsgLower = String(error.message || '').toLowerCase();

        if (errorMsgLower.includes('insufficient lamports') || errorMsgLower.includes('insufficient funds')) {
            userFriendlyError = `Insufficient funds in the ${keyTypeForLog} payout wallet. Please check balance.`;
            // Non-retryable, notify admin immediately
            await notifyAdmin(`🚨 CRITICAL: Insufficient funds in ${keyTypeForLog} wallet (${payerWallet.publicKey.toBase58()}) for payout. Operation ID: ${operationId}`);
        } else if (error instanceof TransactionExpiredBlockheightExceededError || errorMsgLower.includes('blockhash not found') || errorMsgLower.includes('block height exceeded')) {
            userFriendlyError = 'Transaction expired (blockhash invalid). Will retry.';
        } else if (errorMsgLower.includes('transaction was not confirmed') || errorMsgLower.includes('timed out waiting')) {
            userFriendlyError = `Transaction confirmation timeout. Status unclear, may succeed later.`;
            // Potentially retryable, but requires careful handling (check TX status later)
        } else if (isRetryable) {
             userFriendlyError = `Temporary network/RPC error: ${error.message}`;
        }
        console.error(`[${operationId}] Failure details - Retryable: ${isRetryable}, User Friendly Error: "${userFriendlyError}"`);
        return { success: false, error: userFriendlyError, isRetryable: isRetryable };
    }
}


/**
 * Analyzes a fetched Solana transaction to find the SOL amount transferred *to* a target address.
 * Handles basic SystemProgram transfers.
 * @param {ConfirmedTransaction | TransactionResponse | null} tx The fetched transaction object.
 * @param {string} targetAddress The address receiving the funds.
 * @returns {{transferAmount: bigint, payerAddress: string | null}} Amount in lamports and the likely payer.
 */
function analyzeTransactionAmounts(tx, targetAddress) {
    const logPrefix = `[analyzeAmounts Addr:${targetAddress.slice(0, 6)}..]`;
    let transferAmount = 0n;
    let payerAddress = null;
    if (!tx || !targetAddress) { console.warn(`${logPrefix} Invalid input: TX or targetAddress missing.`); return { transferAmount: 0n, payerAddress: null }; }
    try { new PublicKey(targetAddress); } catch (e) { console.warn(`${logPrefix} Invalid targetAddress format: ${targetAddress}`); return { transferAmount: 0n, payerAddress: null }; }
    if (tx.meta?.err) { console.log(`${logPrefix} Transaction meta contains error. Returning zero amount. Error: ${JSON.stringify(tx.meta.err)}`); return { transferAmount: 0n, payerAddress: null }; }

    // Prioritize using balance changes for accuracy
    if (tx.meta?.preBalances && tx.meta?.postBalances && tx.transaction?.message) {
        try {
             // Handle potential differences in accountKeys structure between legacy and versioned transactions
             let accountKeysList = [];
             if (tx.transaction.message.staticAccountKeys) { // Likely legacy
                  accountKeysList = tx.transaction.message.staticAccountKeys.map(k => k.toBase58());
             } else if (tx.transaction.message.accountKeys) { // Likely version 0
                 // For version 0, accountKeys includes address lookup table accounts if used
                 // Need to resolve writable/signer flags if using message directly
                 // Simpler approach for this context: rely on `loadedAddresses` if present for v0 with LUTs
                 if (tx.meta.loadedAddresses) {
                      // Combine static keys with resolved LUT addresses
                      const staticKeys = tx.transaction.message.accountKeys.map(k => k.toBase58());
                      const writableLut = tx.meta.loadedAddresses.writable || [];
                      const readonlyLut = tx.meta.loadedAddresses.readonly || [];
                      accountKeysList = [...staticKeys, ...writableLut, ...readonlyLut];
                 } else {
                       // Version 0 without LUTs
                       accountKeysList = tx.transaction.message.accountKeys.map(k => k.toBase58());
                 }
             } else {
                  throw new Error("Could not determine account keys from transaction message.");
             }

             // Remove duplicates that might arise from LUT resolution
             const uniqueAccountKeys = [...new Set(accountKeysList)];

             const targetIndex = uniqueAccountKeys.indexOf(targetAddress);

             if (targetIndex !== -1 && tx.meta.preBalances.length > targetIndex && tx.meta.postBalances.length > targetIndex) {
                  const preBalance = BigInt(tx.meta.preBalances[targetIndex]);
                  const postBalance = BigInt(tx.meta.postBalances[targetIndex]);
                  const balanceChange = postBalance - preBalance;
                  if (balanceChange > 0n) {
                       transferAmount = balanceChange;
                       console.log(`${logPrefix} Positive balance change detected: ${transferAmount} lamports.`);

                       // Try to find payer based on signers with negative balance change
                       // This part is heuristic and might be complex with v0 TXs / LUTs / complex instructions
                       const header = tx.transaction.message.header;
                       const signers = uniqueAccountKeys.slice(0, header.numRequiredSignatures); // Assuming signers are first

                       for (const signerKey of signers) {
                            const signerIndex = uniqueAccountKeys.indexOf(signerKey);
                            // Check if balances array has this index (might not if complex tx structure)
                            if (signerIndex !== -1 && signerIndex !== targetIndex && tx.meta.preBalances.length > signerIndex && tx.meta.postBalances.length > signerIndex) {
                                 const preSigner = BigInt(tx.meta.preBalances[signerIndex]);
                                 const postSigner = BigInt(tx.meta.postBalances[signerIndex]);
                                 // A simple transfer should result in a decrease roughly equal to transferAmount + fee
                                 // Allow for fee variation
                                 if (postSigner < preSigner) {
                                      payerAddress = signerKey;
                                      console.log(`${logPrefix} Identified likely payer (signer with negative balance change): ${payerAddress}`);
                                      break;
                                 }
                            }
                       }
                       // Fallback to first signer if specific payer not found
                       if (!payerAddress && signers.length > 0) {
                            payerAddress = signers[0];
                            console.log(`${logPrefix} Could not find specific signer with negative balance change, falling back to first signer as payer: ${payerAddress}`);
                       }

                  } else { console.log(`${logPrefix} No positive balance change found for target address.`); }
             } else { console.log(`${logPrefix} Target address not found in resolved account keys or balance arrays incomplete.`); }
        } catch (e) { console.warn(`${logPrefix} Error analyzing balance changes: ${e.message}`); transferAmount = 0n; payerAddress = null; }
    } else { console.log(`${logPrefix} Balance change analysis skipped: Meta or message data missing/incomplete.`); }

    // Fallback: Check inner instructions for SystemProgram transfers if balance change failed
    // This is less reliable as it doesn't guarantee it's the main transfer or account for fees
    if (transferAmount === 0n && tx.meta?.innerInstructions) {
        console.log(`${logPrefix} Attempting analysis via inner instructions (Fallback)...`);
        const SystemProgramId = SystemProgram.programId.toBase58();
        for (const instructionSet of tx.meta.innerInstructions) {
            for (const innerIx of instructionSet.instructions) {
                 // Check if it's a SystemProgram instruction
                 if (tx.transaction.message.staticAccountKeys) { // Legacy parsing
                      const programId = tx.transaction.message.staticAccountKeys[innerIx.programIdIndex]?.toBase58();
                      if (programId === SystemProgramId) {
                          // Attempt to decode transfer instruction data (simplistic check for amount)
                          // Instruction data format: [instruction index (u32), lamports (u64)]
                          // This is fragile and depends on exact instruction encoding.
                          // A full parser like @solana/spl-token might be needed for robust parsing.
                          // Let's skip complex parsing for now and rely on balance changes or logs primarily.
                      }
                 }
                 // Add handling for Version 0 TX parsing if needed - becomes complex quickly
            }
        }
         // console.log(`${logPrefix} Inner instruction parsing did not yield result (or not implemented fully).`);
    }

    // Fallback 2: Use Log Messages (Less reliable than balance changes)
    if (transferAmount === 0n && tx.meta?.logMessages) {
        console.log(`${logPrefix} Attempting analysis via log messages (Fallback 2)...`);
        // Regex might need adjustments based on actual log formats
        const sysTransferToRegex = /Transfer: src=([1-9A-HJ-NP-Za-km-z]+) dst=([1-9A-HJ-NP-Za-km-z]+) lamports=(\d+)/;
        const invokeLogRegex = /Program .* invoke \[\d+\]/; // Identify start of CPI logs
        const successLogRegex = /Program .* success/; // Identify end of CPI logs

        let inRelevantInvoke = true; // Assume top-level initially
        for (const log of tx.meta.logMessages) {
             if (invokeLogRegex.test(log)) {
                 // Potentially entering CPI, maybe ignore transfers within? Depends on bot logic.
                 // For simple deposits, we probably only care about top-level transfers.
                 // This logic might need refinement based on expected transaction types.
                 // Let's assume for now we only care about transfers not deep inside CPIs.
                 // A simple depth counter could be used if needed.
                 inRelevantInvoke = false; // Assume transfers inside CPIs are not the deposit
             } else if (successLogRegex.test(log)) {
                 inRelevantInvoke = true; // Exiting invoke context
             }

            // Check for simple transfer log IF we're likely at the top level
             if (inRelevantInvoke) {
                  const matchNew = log.match(sysTransferToRegex);
                  if (matchNew && matchNew[2] === targetAddress) {
                       const potentialPayer = matchNew[1]; const potentialAmount = BigInt(matchNew[3]);
                       console.log(`${logPrefix} Found potential transfer via log: ${potentialAmount} lamports from ${potentialPayer}`);
                       // Use this only if balance change method failed completely
                       if (transferAmount === 0n) {
                            transferAmount = potentialAmount;
                            payerAddress = potentialPayer;
                       }
                       // Don't break, might be multiple transfers, balance change is better primary source
                  }
             }
        }
        if (transferAmount === 0n) { console.log(`${logPrefix} Log message parsing did not find a definitive transfer to the target address.`); }
    }

    if (transferAmount > 0n) { console.log(`${logPrefix} Final analysis result: Found ${transferAmount} lamports from ${payerAddress || 'Unknown'}`); }
    else { console.log(`${logPrefix} Final analysis result: No positive SOL transfer found.`); }
    return { transferAmount, payerAddress };
}


// --- Telegram Utilities ---

// *** ADDED Admin Notification Helper ***
/**
 * Sends a message to all configured admin users.
 * @param {string} message The message text to send.
 */
async function notifyAdmin(message) {
    const adminIds = (process.env.ADMIN_USER_IDS || '').split(',')
        .map(id => id.trim())
        .filter(id => /^\d+$/.test(id)); // Ensure IDs are numeric strings

    if (adminIds.length === 0) {
        console.warn('[NotifyAdmin] No valid ADMIN_USER_IDS configured in environment variables. Cannot send notification.');
        return;
    }

    const timestamp = new Date().toISOString();
    const fullMessage = `🚨 **BOT ALERT** 🚨\n[${timestamp}]\n\n${message}`;

    console.log(`[NotifyAdmin] Sending alert: "${message.substring(0, 100)}..." to ${adminIds.length} admin(s).`);

    // Use Promise.allSettled to send to all admins even if some fail
    const results = await Promise.allSettled(
        adminIds.map(adminId => safeSendMessage(adminId, fullMessage, { parse_mode: 'Markdown' })) // Use Markdown for simple bold/alert
    );

    results.forEach((result, index) => {
        if (result.status === 'rejected') {
            console.error(`[NotifyAdmin] Failed to send notification to admin ID ${adminIds[index]}:`, result.reason || 'Unknown error');
        }
    });
}


async function safeSendMessage(chatId, text, options = {}) {
    if (!chatId || typeof text !== 'string' || text.trim() === '') { console.error("[safeSendMessage] Invalid input:", { chatId, text }); return undefined; }
    const MAX_LENGTH = 4096; if (text.length > MAX_LENGTH) { console.warn(`[safeSendMessage] Message for chat ${chatId} too long (${text.length} > ${MAX_LENGTH}), truncating.`); text = text.substring(0, MAX_LENGTH - 3) + "..."; }
    // Add a small delay before adding to queue if needed, but p-queue interval handles rate limits
    return telegramSendQueue.add(async () => {
          try {
              // console.log(`[TG Send Queue] Sending to ${chatId}: "${text.substring(0, 30)}..."`); // Optional debug log
              const sentMessage = await bot.sendMessage(chatId, text, options);
              return sentMessage;
          } catch (error) {
              console.error(`❌ Telegram send error to chat ${chatId}: Code: ${error.code} | ${error.message}`);
              if (error.response?.body) {
                   console.error(`   -> Response Body: ${JSON.stringify(error.response.body)}`);
                   // Handle specific blocking errors if needed (e.g., user blocked bot)
                   if (error.response.body.error_code === 403) {
                        console.warn(`[TG Send] Bot blocked by user or kicked from chat ${chatId}.`);
                        // Potentially update user status in DB if needed
                   }
              }
              return undefined; // Indicate failure to send
          }
       });
}
const escapeMarkdownV2 = (text) => { if (text === null || typeof text === 'undefined') { return ''; } const textString = String(text); return textString.replace(/([_*\[\]()~`>#+\-=|{}.!\\])/g, '\\$1'); };
function escapeHtml(text) { if (text === null || typeof text === 'undefined') { return ''; } const textString = String(text); return textString.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#039;"); }
async function getUserDisplayName(chatId, userId) { try { const member = await bot.getChatMember(chatId, userId); const user = member.user; let name = ''; if (user.username) { name = `@${user.username}`; } else if (user.first_name) { name = user.first_name; if (user.last_name) { name += ` ${user.last_name}`; } } else { name = `User_${String(userId).slice(-4)}`; } return escapeMarkdownV2(name); } catch (error) { console.warn(`[getUserDisplayName] Failed to get chat member for ${userId} in ${chatId}: ${error.message}. Falling back to ID.`); return escapeMarkdownV2(`User_${String(userId).slice(-4)}`); } }


// --- Cache Utilities ---
// These remain unchanged from your original code
function updateWalletCache(userId, data) { userId = String(userId); const existing = walletCache.get(userId) || {}; const entryTimestamp = Date.now(); walletCache.set(userId, { ...existing, ...data, timestamp: entryTimestamp }); setTimeout(() => { const current = walletCache.get(userId); if (current && current.timestamp === entryTimestamp && Date.now() - current.timestamp >= WALLET_CACHE_TTL_MS) { walletCache.delete(userId); } }, WALLET_CACHE_TTL_MS + 1000); }
function getWalletCache(userId) { userId = String(userId); const entry = walletCache.get(userId); if (entry && Date.now() - entry.timestamp < WALLET_CACHE_TTL_MS) { return entry; } if (entry) { walletCache.delete(userId); } return undefined; }
function addActiveDepositAddressCache(address, userId, expiresAtTimestamp) { activeDepositAddresses.set(address, { userId: String(userId), expiresAt: expiresAtTimestamp }); console.log(`[Cache Update] Added/Updated deposit address ${address.slice(0,6)}.. for user ${userId} in cache. Expires: ${new Date(expiresAtTimestamp).toISOString()}`); const delay = expiresAtTimestamp - Date.now() + 1000; if (delay > 0) { setTimeout(() => { const current = activeDepositAddresses.get(address); if (current && current.userId === String(userId) && current.expiresAt === expiresAtTimestamp) { console.log(`[Cache Eviction] Deposit address ${address.slice(0,6)}.. expired and removed from cache.`); activeDepositAddresses.delete(address); } else if (current) { console.log(`[Cache Eviction] Deposit address ${address.slice(0,6)}.. cache timer fired, but entry was updated or removed. Ignoring eviction timer.`); } }, delay); } else { console.log(`[Cache Update] Deposit address ${address.slice(0,6)}.. was already expired when added to cache. Removing immediately.`); removeActiveDepositAddressCache(address); } }
function getActiveDepositAddressCache(address) { const entry = activeDepositAddresses.get(address); const now = Date.now(); if (entry && now < entry.expiresAt) { return entry; } if (entry) { activeDepositAddresses.delete(address); } return undefined; }
function removeActiveDepositAddressCache(address) { const deleted = activeDepositAddresses.delete(address); if (deleted) { console.log(`[Cache Update] Explicitly removed deposit address ${address.slice(0,6)}.. from cache.`); } return deleted; }
function addProcessedDepositTx(signature) { if (processedDepositTxSignatures.has(signature)) return; if (processedDepositTxSignatures.size >= MAX_PROCESSED_TX_CACHE_SIZE) { const oldestSig = processedDepositTxSignatures.values().next().value; if (oldestSig) { processedDepositTxSignatures.delete(oldestSig); } } processedDepositTxSignatures.add(signature); console.log(`[Cache Update] Added TX signature ${signature.slice(0,6)}.. to processed deposit cache. Size: ${processedDepositTxSignatures.size}/${MAX_PROCESSED_TX_CACHE_SIZE}`); }
function hasProcessedDepositTx(signature) { return processedDepositTxSignatures.has(signature); }


// --- Other Utilities ---
// This remains unchanged from your original code
function generateReferralCode(length = 8) { const byteLength = Math.ceil(length / 2); const randomBytes = crypto.randomBytes(byteLength); const hexString = randomBytes.toString('hex').slice(0, length); return `ref_${hexString}`; }


// --- End of Part 3 ---

// Export functions potentially needed elsewhere (adjust as necessary for your final structure)
export {
    createSafeIndex,
    generateUniqueDepositAddress,
    getKeypairFromPath, // Export the new function
    isRetryableSolanaError,
    sendSol,
    analyzeTransactionAmounts,
    notifyAdmin,
    safeSendMessage,
    escapeMarkdownV2,
    escapeHtml,
    getUserDisplayName,
    updateWalletCache,
    getWalletCache,
    addActiveDepositAddressCache,
    getActiveDepositAddressCache,
    removeActiveDepositAddressCache,
    addProcessedDepositTx,
    hasProcessedDepositTx,
    generateReferralCode
}; // Added exports assuming this might become a module
// index.js - Part 4: Game Logic
// --- VERSION: 3.1.5 --- (No functional changes in this part for this update)

// --- Game Logic Implementations ---

/**
 * Plays a coinflip game.
 * @param {string} choice 'heads' or 'tails'.
 * @returns {{ result: 'win' | 'loss', outcome: 'heads' | 'tails' }}
 */
function playCoinflip(choice) {
    const outcome = Math.random() < 0.5 ? 'heads' : 'tails';
    const result = choice === outcome ? 'win' : 'loss';
    return { result, outcome };
}

/**
 * Simulates a simple race game outcome.
 * @param {number} chosenLane (e.g., 1, 2, 3, 4)
 * @param {number} [totalLanes=4]
 * @returns {{ result: 'win' | 'loss', winningLane: number }}
 */
function simulateRace(chosenLane, totalLanes = 4) {
    // Simple random winner
    const winningLane = Math.floor(Math.random() * totalLanes) + 1;
    const result = chosenLane === winningLane ? 'win' : 'loss';
    return { result, winningLane };
}

/**
 * Simulates a simple slots game spin. Returns payout multiplier.
 * Symbols: Cherry (C), Lemon (L), Orange (O), Bell (B), Seven (7)
 * Payouts (example): 777=100x, BBB=20x, OOO=10x, CCC=5x, Any two Cherries=2x
 * @returns {{ result: 'win' | 'loss', symbols: [string, string, string], payoutMultiplier: number }}
 */
function simulateSlots() {
    const symbols = ['C', 'L', 'O', 'B', '7'];
    const weights = [5, 4, 3, 2, 1]; // Weighted probability: Cherry most common, 7 least common

    const weightedSymbols = [];
    symbols.forEach((symbol, index) => {
        for (let i = 0; i < weights[index]; i++) {
            weightedSymbols.push(symbol);
        }
    });

    const getRandomSymbol = () => weightedSymbols[Math.floor(Math.random() * weightedSymbols.length)];

    const reel1 = getRandomSymbol();
    const reel2 = getRandomSymbol();
    const reel3 = getRandomSymbol();
    const resultSymbols = [reel1, reel2, reel3];

    let payoutMultiplier = 0;
    let result = 'loss';

    // Check winning combinations
    if (reel1 === '7' && reel2 === '7' && reel3 === '7') {
        payoutMultiplier = 100;
    } else if (reel1 === 'B' && reel2 === 'B' && reel3 === 'B') {
        payoutMultiplier = 20;
    } else if (reel1 === 'O' && reel2 === 'O' && reel3 === 'O') {
        payoutMultiplier = 10;
    } else if (reel1 === 'C' && reel2 === 'C' && reel3 === 'C') {
        payoutMultiplier = 5;
    } else {
        // Check for two cherries (can be improved for combinations)
        const cherryCount = resultSymbols.filter(s => s === 'C').length;
        if (cherryCount >= 2) {
            payoutMultiplier = 2;
        }
    }

    if (payoutMultiplier > 0) {
        result = 'win';
    }

    return { result, symbols: resultSymbols, payoutMultiplier };
}


/**
 * Simulates a simplified Roulette spin (European style - 0 to 36).
 * @param {string} betType - e.g., 'straight_17', 'red', 'black', 'even', 'odd', 'low' (1-18), 'high' (19-36)
 * @param {number | null} [betValue=null] - The specific number for 'straight_X' bets.
 * @returns {{ result: 'win' | 'loss', winningNumber: number, payoutMultiplier: number }}
 */
function simulateRoulette(betType, betValue = null) {
    const winningNumber = Math.floor(Math.random() * 37); // 0 to 36
    let result = 'loss';
    let payoutMultiplier = 0;

    const isRed = (num) => {
        const reds = [1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36];
        return reds.includes(num);
    };
    const isBlack = (num) => num !== 0 && !isRed(num);
    const isEven = (num) => num !== 0 && num % 2 === 0;
    const isOdd = (num) => num !== 0 && num % 2 !== 0;
    const isLow = (num) => num >= 1 && num <= 18;
    const isHigh = (num) => num >= 19 && num <= 36;

    switch (betType) {
        case 'straight':
            if (betValue !== null && winningNumber === betValue) {
                result = 'win';
                payoutMultiplier = 35; // Pays 35 to 1 (total 36)
            }
            break;
        case 'red':
            if (isRed(winningNumber)) {
                result = 'win';
                payoutMultiplier = 1; // Pays 1 to 1 (total 2)
            }
            break;
        case 'black':
            if (isBlack(winningNumber)) {
                result = 'win';
                payoutMultiplier = 1;
            }
            break;
        case 'even':
            if (isEven(winningNumber)) {
                result = 'win';
                payoutMultiplier = 1;
            }
            break;
        case 'odd':
            if (isOdd(winningNumber)) {
                result = 'win';
                payoutMultiplier = 1;
            }
            break;
        case 'low': // 1-18
            if (isLow(winningNumber)) {
                result = 'win';
                payoutMultiplier = 1;
            }
            break;
        case 'high': // 19-36
            if (isHigh(winningNumber)) {
                result = 'win';
                payoutMultiplier = 1;
            }
            break;
        default:
            console.warn(`[simulateRoulette] Unknown bet type: ${betType}`);
            // Treat as loss
            break;
    }

     // Adjust multiplier: we return the factor for the WINNINGS, not total return
     // e.g., 1 to 1 win returns 1 * wager as profit, total 2 * wager.
     // e.g., 35 to 1 win returns 35 * wager as profit, total 36 * wager.
     // Payout amount = wager * (1 + multiplier) <- THIS IS WRONG for standard casino terms
     // Payout amount = wager + (wager * multiplier) <---- Standard casino payout (original wager + winnings)
     // Amount credited back to balance = wager * (payoutMultiplier for winnings) + original wager
     // We will return the multiplier FOR THE WINNINGS (35 for straight, 1 for even money)
     // The calling code will calculate payout = wager + (wager * multiplier) if win, 0 if loss.

    return { result, winningNumber, payoutMultiplier };
}

/**
 * Simulates a simplified game of Casino War. Ace is high.
 * @returns {{ result: 'win' | 'loss' | 'push', playerCard: string, dealerCard: string, playerRank: number, dealerRank: number }}
 */
function simulateWar() {
    const suits = ['H', 'D', 'C', 'S']; // Hearts, Diamonds, Clubs, Spades
    const ranks = ['2', '3', '4', '5', '6', '7', '8', '9', 'T', 'J', 'Q', 'K', 'A'];
    const deck = [];
    for (const suit of suits) {
        for (const rank of ranks) {
            deck.push(rank + suit);
        }
    }

    // Simple shuffle (Fisher-Yates variation)
    for (let i = deck.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [deck[i], deck[j]] = [deck[j], deck[i]];
    }

    const playerCard = deck.pop();
    const dealerCard = deck.pop();

    const getRankValue = (card) => {
        const rank = card.substring(0, card.length - 1); // Get rank part
        if (rank === 'A') return 14;
        if (rank === 'K') return 13;
        if (rank === 'Q') return 12;
        if (rank === 'J') return 11;
        if (rank === 'T') return 10;
        return parseInt(rank, 10);
    };

    const playerRank = getRankValue(playerCard);
    const dealerRank = getRankValue(dealerCard);

    let result = 'loss';
    if (playerRank > dealerRank) {
        result = 'win';
    } else if (playerRank === dealerRank) {
        // Simplified: In real War, this leads to "Going to War" or surrendering.
        // Here, we'll just treat it as a push (bet returned).
        result = 'push';
    }
    // else playerRank < dealerRank -> loss (default)

    return { result, playerCard, dealerCard, playerRank, dealerRank };
}

// --- End of Part 4 ---
// index.js - Part 5a: Telegram Command, Callback Handlers & Game Result Processing
// --- VERSION: 3.1.5 --- (Updated help version, added admin notify on handler errors, IMPLEMENTED Game Handlers)

// --- Imports potentially needed by this Part (assuming they exist in combined file's Part 1) ---
// import { pool } from './db'; // Assuming pool is exported from Part 1 or DB part
// import { bot } from './bot'; // Assuming bot is exported from Part 1
// import { userStateCache, commandCooldown, pendingReferrals, PENDING_REFERRAL_TTL_MS, USER_COMMAND_COOLDOWN_MS, USER_STATE_TTL_MS, LAMPORTS_PER_SOL, SOL_DECIMALS, GAME_CONFIG, WITHDRAWAL_FEE_LAMPORTS, MIN_WITHDRAWAL_LAMPORTS, REFERRAL_INITIAL_BET_MIN_LAMPORTS, REFERRAL_MILESTONE_REWARD_PERCENT, REFERRAL_INITIAL_BONUS_TIERS } from './config'; // Assuming these are exported
// import { updateUserBalanceAndLedger, createBetRecord, getUserBalance, getLinkedWallet, getUserWalletDetails, getUserByReferralCode, isFirstCompletedBet, updateBetStatus, createWithdrawalRequest, recordPendingReferralPayout, addPayoutJob, getTotalReferralEarnings, linkReferral, updateUserWagerStats } from './dbOps'; // Assuming DB functions exported from Part 2
// import { safeSendMessage, escapeMarkdownV2, notifyAdmin, getUserDisplayName, addProcessedDepositTx, hasProcessedDepositTx } from './utils'; // Assuming Utils exported from Part 3
// import { playCoinflip, simulateRace, simulateSlots, simulateWar } from './gameLogic'; // Assuming Game Logic exported from Part 4
// import { PQueue } from 'p-queue'; // Already imported in Part 1, but might be needed if queues are passed around

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
                // Escaped Message
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
                    // Escaped Message
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
                 if (!currentState || currentState.state === 'idle') {
                     // Escaped Message
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
                const refereeDetails = await getUserWalletDetails(userId); // Use detailed check
                const referrer = await getUserByReferralCode(refCode);

                // Escaped Messages
                if (referrer && referrer.user_id === userId) {
                     await safeSendMessage(chatId, `Welcome\\! You can't use your own referral code\\.`, { parse_mode: 'MarkdownV2' });
                } else if (refereeDetails && refereeDetails.referred_by_user_id) {
                     await safeSendMessage(chatId, `Welcome back\\! You have already been referred\\.`, { parse_mode: 'MarkdownV2' });
                } else if (referrer) {
                    pendingReferrals.set(userId, { referrerUserId: referrer.user_id, timestamp: Date.now() });
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
            }
        }
        // --- End /start Referral Parsing ---

        // --- Command Handler Map ---
        // Defined later in Part 5b

        const handler = commandHandlers[command];

        if (handler) {
            if (command === 'admin') {
                const adminIds = (process.env.ADMIN_USER_IDS || '').split(',').map(id=>id.trim()).filter(id => id);
                if (adminIds.includes(userId)) {
                    await handler(msg, args);
                } else {
                    console.warn(`${logPrefix} Unauthorized attempt to use /admin`);
                }
            } else {
                await handler(msg, args);
            }
        } else {
            if (msg.chat.type === 'private') {
                // Escaped Message
                await safeSendMessage(chatId, `❓ Unknown command: \`/${escapeMarkdownV2(command)}\`\\. Try /help or /start`, { parse_mode: 'MarkdownV2' });
            }
        }
    } catch (error) {
        console.error(`${logPrefix} Error processing message/command:`, error);
        userStateCache.delete(userId); // Clear state on error

        // *** ADDED: Notify admin on unexpected handler errors ***
        await notifyAdmin(`🚨 ERROR in handleMessage for User ${userId} (${logPrefix}):\n${error.message}\nStack: ${error.stack?.substring(0, 500)}`);

        try {
            // Escaped Message
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

    if (!chatId || !messageId || !callbackData) {
        console.error(`${logPrefix} Received callback query with missing message data.`);
        await bot.answerCallbackQuery(callbackQueryId).catch(e => console.warn(`${logPrefix} Failed to answer callback query with missing data: ${e.message}`));
        return;
    }

    // Answer silently first to make button feel responsive
    await bot.answerCallbackQuery(callbackQueryId).catch(e => console.warn(`${logPrefix} Failed to answer callback query: ${e.message}`));
    console.log(`${logPrefix} Received callback data: ${callbackData}`); // Log received data

    try {
        const currentState = userStateCache.get(userId);
        const now = Date.now();

        if (currentState && currentState.state !== 'idle' && now - currentState.timestamp > USER_STATE_TTL_MS) {
            console.log(`${logPrefix} State cache expired processing callback. State: ${currentState.state}`);
            userStateCache.delete(userId);
            await bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: messageId }).catch(() => {});
             // Escaped Message
             await safeSendMessage(chatId, "Your previous action timed out\\. Please start again using the commands\\.", { parse_mode: 'MarkdownV2'});
            return;
        }

        const [action, ...params] = callbackData.split(':');

        switch (action) {
            // --- Menu Actions ---
            case 'menu':
                const menuItem = params[0];
                 // [LOGGING ADDED] Log specific menu action
                 console.log(`${logPrefix} Handling menu callback for: ${menuItem}`);
                 // Defined later in Part 5b
                 const menuHandler = menuCommandHandlers[menuItem];
                 if (menuHandler) {
                     // Mock message to pass to command handlers
                     const mockMsg = { ...callbackQuery.message, from: callbackQuery.from, text: `/${menuItem}`, chat: { id: chatId, type: callbackQuery.message.chat.type } };
                     await menuHandler(mockMsg, ''); // Pass empty args
                 } else { console.warn(`${logPrefix} Unknown menu action: ${menuItem}`); }
                 break;

            // --- Game Actions (Example: Coinflip Choice) ---
            case 'cf_choice':
                if (!currentState || currentState.state !== 'awaiting_cf_choice' || !currentState.data?.amountLamports) { throw new Error("Invalid state for coinflip choice or missing wager amount."); }
                const choice = params[0];
                if (choice !== 'heads' && choice !== 'tails') { throw new Error(`Invalid coinflip choice received: ${choice}`); }
                const cfWager = BigInt(currentState.data.amountLamports);

                // Escaped Message
                await bot.editMessageText(`🪙 Coinflip: You chose *${escapeMarkdownV2(choice)}* for ${escapeMarkdownV2((Number(cfWager)/LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL\\. Flipping\\.\\.\\.`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: undefined }).catch(()=>{/*ignore if already gone*/});
                userStateCache.delete(userId); // Clear state before potential async game/bet logic

                const { success: betPlaced, betId, error: betError } = await placeBet(userId, String(chatId), 'coinflip', { choice }, cfWager);
                if (!betPlaced || !betId) {
                    console.error(`${logPrefix} Failed to place coinflip bet: ${betError}`);
                    // Ensure user state is clear and inform user if possible (placeBet should ideally inform)
                    userStateCache.delete(userId);
                    // Notify admin about bet placement failure beyond insufficient funds
                    if (betError !== 'Insufficient balance') {
                         await notifyAdmin(`🚨 ERROR placing Coinflip Bet for User ${userId} (${logPrefix}). Error: ${betError}`);
                    }
                    break;
                }

                // Pass betId to game handler <--- CALL TO NEW FUNCTION
                const gameSuccess = await handleCoinflipGame(userId, String(chatId), cfWager, choice, betId); // Pass betId
                if (gameSuccess) { await _handleReferralChecks(userId, betId, cfWager, logPrefix); }
                break;

            // Example: Race Choice
            case 'race_choice':
                 if (!currentState || currentState.state !== 'awaiting_race_choice' || !currentState.data?.amountLamports) { throw new Error("Invalid state for race choice or missing wager amount."); }
                 const chosenHorseName = params.join(':'); // Re-join in case horse name had colons (unlikely but safe)
                 if (!RACE_HORSES.some(h => h.name === chosenHorseName)) { throw new Error(`Invalid horse choice received: ${chosenHorseName}`); } // Needs RACE_HORSES defined
                 const raceWager = BigInt(currentState.data.amountLamports);

                 // Escaped Message
                 await bot.editMessageText(`🐎 Race: You chose *${escapeMarkdownV2(chosenHorseName)}* for ${escapeMarkdownV2((Number(raceWager)/LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL\\. Starting the race\\!`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: undefined }).catch(()=>{});
                 userStateCache.delete(userId); // Clear state

                 const { success: raceBetPlaced, betId: raceBetId, error: raceBetError } = await placeBet(userId, String(chatId), 'race', { horse: chosenHorseName }, raceWager);
                 if (!raceBetPlaced || !raceBetId) {
                      console.error(`${logPrefix} Failed to place race bet: ${raceBetError}`);
                      userStateCache.delete(userId);
                       if (raceBetError !== 'Insufficient balance') {
                            await notifyAdmin(`🚨 ERROR placing Race Bet for User ${userId} (${logPrefix}). Error: ${raceBetError}`);
                       }
                       break;
                 }

                 // <--- CALL TO NEW FUNCTION
                 const raceGameSuccess = await handleRaceGame(userId, String(chatId), raceWager, chosenHorseName, raceBetId); // Pass betId
                 if (raceGameSuccess) { await _handleReferralChecks(userId, raceBetId, raceWager, logPrefix); }
                 break;

            // Example: Slots Spin Confirmation
            case 'slots_spin_confirm':
                 if (!currentState || currentState.state !== 'awaiting_slots_confirm' || !currentState.data?.amountLamports) { throw new Error("Invalid state for slots confirmation or missing wager amount."); }
                 const slotsWager = BigInt(currentState.data.amountLamports);

                 // Escaped Message
                 await bot.editMessageText(`🎰 Slots: Spinning the reels for ${escapeMarkdownV2((Number(slotsWager)/LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL\\.\\.\\.`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: undefined }).catch(()=>{});
                 userStateCache.delete(userId); // Clear state

                 const { success: slotsBetPlaced, betId: slotsBetId, error: slotsBetError } = await placeBet(userId, String(chatId), 'slots', {}, slotsWager);
                 if (!slotsBetPlaced || !slotsBetId) {
                      console.error(`${logPrefix} Failed to place slots bet: ${slotsBetError}`);
                      userStateCache.delete(userId);
                      if (slotsBetError !== 'Insufficient balance') {
                          await notifyAdmin(`🚨 ERROR placing Slots Bet for User ${userId} (${logPrefix}). Error: ${slotsBetError}`);
                      }
                      break;
                 }

                 // <--- CALL TO NEW FUNCTION
                 const slotsGameSuccess = await handleSlotsGame(userId, String(chatId), slotsWager, slotsBetId); // Pass betId
                 if (slotsGameSuccess) { await _handleReferralChecks(userId, slotsBetId, slotsWager, logPrefix); }
                 break;

            // Example: War Confirmation
            case 'war_confirm':
                 if (!currentState || currentState.state !== 'awaiting_war_confirm' || !currentState.data?.amountLamports) { throw new Error("Invalid state for War confirmation or missing wager amount."); }
                 const warWager = BigInt(currentState.data.amountLamports);

                 // Escaped Message
                 await bot.editMessageText(`🃏 Casino War: Dealing cards for ${escapeMarkdownV2((Number(warWager)/LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL\\.\\.\\.`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: undefined }).catch(()=>{});
                 userStateCache.delete(userId); // Clear state

                 const { success: warBetPlaced, betId: warBetId, error: warBetError } = await placeBet(userId, String(chatId), 'war', {}, warWager);
                 if (!warBetPlaced || !warBetId) {
                      console.error(`${logPrefix} Failed to place War bet: ${warBetError}`);
                      userStateCache.delete(userId);
                      if (warBetError !== 'Insufficient balance') {
                          await notifyAdmin(`🚨 ERROR placing War Bet for User ${userId} (${logPrefix}). Error: ${warBetError}`);
                      }
                      break;
                 }

                 // <--- CALL TO NEW FUNCTION
                 const warGameSuccess = await handleWarGame(userId, String(chatId), warWager, warBetId); // Pass betId
                 if (warGameSuccess) { await _handleReferralChecks(userId, warBetId, warWager, logPrefix); }
                 break;


             // --- Withdrawal Confirmation ---
             case 'withdraw_confirm':
                  if (!currentState || currentState.state !== 'awaiting_withdrawal_confirm' || !currentState.data?.amountLamports || !currentState.data?.recipientAddress) { throw new Error("Invalid state for withdrawal confirmation."); }
                  const { amountLamports: withdrawAmountStr, recipientAddress: withdrawAddress } = currentState.data;
                  const withdrawAmount = BigInt(withdrawAmountStr);
                  const fee = WITHDRAWAL_FEE_LAMPORTS;
                  const finalSendAmount = withdrawAmount;
                  const totalDeduction = withdrawAmount + fee;

                  console.log(`${logPrefix} User confirmed withdrawal of ${Number(finalSendAmount) / LAMPORTS_PER_SOL} SOL to ${withdrawAddress}`);
                  userStateCache.delete(userId); // Clear state

                  // Escaped Message
                  await bot.editMessageText(`💸 Processing withdrawal request\\.\\.\\. Please wait\\.`, { chat_id: chatId, message_id: messageId, reply_markup: undefined }).catch(()=>{});

                  const currentBalance = await getUserBalance(userId);
                  if (currentBalance < totalDeduction) {
                      // Escaped Message
                      await safeSendMessage(chatId, `⚠️ Withdrawal failed\\. Your current balance \\(${escapeMarkdownV2((Number(currentBalance) / LAMPORTS_PER_SOL).toFixed(4))} SOL\\) is insufficient for the requested amount plus fee \\(${escapeMarkdownV2((Number(totalDeduction) / LAMPORTS_PER_SOL).toFixed(4))} SOL\\)\\.`, { parse_mode: 'MarkdownV2' });
                      break;
                  }

                  const wdRequest = await createWithdrawalRequest(userId, withdrawAmount, fee, withdrawAddress);
                  if (!wdRequest.success || !wdRequest.withdrawalId) {
                       const errMsg = wdRequest.error || 'DB Error creating withdrawal request.';
                       // Escaped Message
                       await safeSendMessage(chatId, `⚠️ Failed to initiate withdrawal request in database\\. Please try again or contact support\\. Error: ${escapeMarkdownV2(errMsg)}`, { parse_mode: 'MarkdownV2' });
                       // Notify Admin
                       await notifyAdmin(`🚨 ERROR creating Withdrawal Request for User ${userId} (${logPrefix}). Error: ${errMsg}`);
                       break;
                  }
                  const withdrawalId = wdRequest.withdrawalId; // Get ID for logging/job

                  console.log(`${logPrefix} Withdrawal request ${withdrawalId} created in DB. Adding to payout queue.`); // [LOGGING ADDED] Log DB success and queue add

                  await addPayoutJob(
                       { type: 'payout_withdrawal', withdrawalId: withdrawalId, userId: userId },
                       parseInt(process.env.PAYOUT_JOB_RETRIES, 10),
                       parseInt(process.env.PAYOUT_JOB_RETRY_DELAY_MS, 10)
                  );
                  // Escaped Message
                  await safeSendMessage(chatId, `✅ Withdrawal request submitted for ${escapeMarkdownV2((Number(finalSendAmount) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL\\. You will be notified upon completion\\.`, { parse_mode: 'MarkdownV2' });
                  break;

                // --- Generic Cancel ---
                case 'cancel_action':
                     console.log(`${logPrefix} User cancelled action.`);
                     const cancelledState = currentState?.state || 'unknown';
                     userStateCache.delete(userId);
                     // Escaped Message
                     await bot.editMessageText(`Action cancelled \\(${escapeMarkdownV2(cancelledState)}\\)\\.`, { chat_id: chatId, message_id: messageId, reply_markup: undefined }).catch(()=>{});
                     break;

                 default:
                      console.warn(`${logPrefix} Received unknown callback action: ${action}`);
                      // Only answer with alert if the initial silent answer failed or is too old
                      // await bot.answerCallbackQuery(callbackQueryId, { text: "Unknown button action.", show_alert: true }).catch(() => {}); // Show alert to user
                      break;
         }

    } catch (error) {
         console.error(`${logPrefix} Error processing callback query data '${callbackData}':`, error);
         userStateCache.delete(userId); // Clear state on error

          // *** ADDED: Notify admin on unexpected handler errors ***
          await notifyAdmin(`🚨 ERROR in handleCallbackQuery for User ${userId} (${logPrefix}) Data: ${callbackData}:\n${error.message}\nStack: ${error.stack?.substring(0, 500)}`);

         try {
             // Escaped Message
             // Attempt to edit the original message with an error
             await bot.editMessageText(`⚠️ Error: ${escapeMarkdownV2(error.message)}`, { chat_id: chatId, message_id: messageId, reply_markup: undefined, parse_mode: 'MarkdownV2' }).catch(async (editError) => {
                 // If editing fails (e.g., message too old), send a new message
                 console.warn(`${logPrefix} Failed to edit message with error, sending new message. Edit Error: ${editError.message}`);
                 // Escaped Message
                 await safeSendMessage(chatId, `⚠️ An error occurred processing your last action\\. Please try again\\. Error: ${escapeMarkdownV2(error.message)}`, { parse_mode: 'MarkdownV2' });
             });
         } catch (sendError) {
             // Fallback if even sending a new message fails
             console.error(`${logPrefix} Critical: Failed to send error message to user after callback error:`, sendError);
         }
    }
}

// --- Helper to Place Bet (Deduct Balance, Create Record) ---
/**
 * Handles the atomic process of deducting balance and creating a bet record.
 * @returns {Promise<{success: boolean, betId?: number, error?: string}>}
 */
async function placeBet(userId, chatId, gameType, betDetails, wagerAmount) {
    const logPrefix = `[PlaceBet User ${userId} Game ${gameType}]`; // [LOGGING ADDED] Prefix
    let client = null;
    chatId = String(chatId); // Ensure chatId is string for safeSendMessage

    try {
        console.log(`${logPrefix} Attempting to place bet. Wager: ${wagerAmount} lamports.`); // [LOGGING ADDED] Entry log
        client = await pool.connect();
        await client.query('BEGIN');
        console.log(`${logPrefix} DB Transaction started.`); // [LOGGING ADDED] TX start

        // 1. Deduct balance and add 'bet_placed' ledger entry
        console.log(`${logPrefix} Deducting balance and adding ledger entry...`); // [LOGGING ADDED] Step log
        const balanceUpdateResult = await updateUserBalanceAndLedger(
            client, userId, -wagerAmount, 'bet_placed', {}, `Bet on ${gameType}`
        );
        if (!balanceUpdateResult.success) {
            await client.query('ROLLBACK');
            console.error(`${logPrefix} Balance deduction failed: ${balanceUpdateResult.error}. Rolled back.`); // [LOGGING ADDED] Failure log + rollback
            const errorMsg = balanceUpdateResult.error === 'Insufficient balance'
                ? 'Insufficient balance'
                : 'Failed to update balance for bet.';
            const currentBalance = await getUserBalance(userId); // Fetch balance to show user
             // Escaped Message
             await safeSendMessage(chatId, `⚠️ Bet failed\\! ${escapeMarkdownV2(errorMsg)}\\. Your balance: ${escapeMarkdownV2((Number(currentBalance)/LAMPORTS_PER_SOL).toFixed(4))} SOL\\.`, { parse_mode: 'MarkdownV2'});
            return { success: false, error: errorMsg }; // Return specific error
        }
        console.log(`${logPrefix} Balance updated successfully. New balance: ${balanceUpdateResult.newBalance}`); // [LOGGING ADDED] Step success log

        // 2. Create bet record
        console.log(`${logPrefix} Creating bet record...`); // [LOGGING ADDED] Step log
        const betRecordResult = await createBetRecord(client, userId, String(chatId), gameType, betDetails, wagerAmount);
        if (!betRecordResult.success || !betRecordResult.betId) {
            await client.query('ROLLBACK');
            const errorMsg = betRecordResult.error || 'Failed to record bet in database.';
            console.error(`${logPrefix} Failed to create bet record: ${errorMsg}. Rolled back.`); // [LOGGING ADDED] Failure log + rollback
             // Escaped Message
             await safeSendMessage(chatId, `⚠️ Bet failed due to internal error recording the bet\\. Your balance has not been charged\\. Please try again\\. Error: ${escapeMarkdownV2(errorMsg)}`, { parse_mode: 'MarkdownV2'});
             // Also notify admin for non-obvious errors
             if (!errorMsg.includes('Wager amount must be positive')) {
                  await notifyAdmin(`🚨 ERROR creating Bet Record for User ${userId} (${logPrefix}). Error: ${errorMsg}`);
             }
            return { success: false, error: errorMsg };
        }
        const betId = betRecordResult.betId;
        console.log(`${logPrefix} Bet record created successfully. Bet ID: ${betId}`); // [LOGGING ADDED] Step success log

        // 3. Commit transaction
        console.log(`${logPrefix} Committing DB transaction...`); // [LOGGING ADDED] Step log
        await client.query('COMMIT');
        console.log(`${logPrefix} Successfully placed bet ${betId}. Wager: ${wagerAmount}. Transaction committed.`); // [LOGGING ADDED] Final success log
        return { success: true, betId: betId };

    } catch (error) {
        if (client) {
            try {
                await client.query('ROLLBACK');
                console.error(`${logPrefix} Unexpected error occurred, transaction rolled back. Error:`, error); // [LOGGING ADDED] Unexpected error + rollback
            } catch (rbErr) {
                 console.error(`${logPrefix} Unexpected error occurred AND rollback failed. Error:`, error, `Rollback Error:`, rbErr); // [LOGGING ADDED] Critical failure
            }
        } else {
            console.error(`${logPrefix} Unexpected error placing bet (no client):`, error); // [LOGGING ADDED] Error before client connect
        }
         // Notify Admin for unexpected errors
         await notifyAdmin(`🚨 UNEXPECTED ERROR in placeBet for User ${userId} (${logPrefix}):\n${error.message}\nStack: ${error.stack?.substring(0, 500)}`);
         // Escaped Message
         await safeSendMessage(chatId, `⚠️ An unexpected error occurred while placing your bet\\. Please try again or contact support\\.`, { parse_mode: 'MarkdownV2'});
        return { success: false, error: 'Unexpected error placing bet.' };
    } finally {
        if (client) client.release();
    }
}


// --- NEW Game Result Handlers ---

// Placeholder for Race Horses - Adjust structure and odds as needed
const RACE_HORSES = [
    { name: "Solar Sprint", payoutMultiplier: 3 }, // Example: Pays 3:1
    { name: "Comet Tail", payoutMultiplier: 4 },
    { name: "Galaxy Gallop", payoutMultiplier: 5 },
    { name: "Nebula Speed", payoutMultiplier: 6 },
];

/**
 * Handles Coinflip game execution, DB updates, and user notification.
 * @returns {Promise<boolean>} True if successful, false otherwise.
 */
async function handleCoinflipGame(userId, chatId, wagerAmountLamports, choice, betId) {
    const logPrefix = `[CoinflipGame Bet ${betId} User ${userId}]`;
    let client = null;
    const gameConfig = GAME_CONFIG.coinflip;

    try {
        // 1. Simulate Game
        const { result, outcome } = playCoinflip(choice);
        console.log(`${logPrefix} Result: ${outcome}. User choice: ${choice}. User ${result}.`);

        // 2. Calculate Payout
        let finalPayoutLamports = 0n;
        let finalStatus = 'completed_loss';
        if (result === 'win') {
            // Payout = Wager + Winnings. Winnings = Wager * (1 - HouseEdge)
            const winnings = wagerAmountLamports - (wagerAmountLamports * BigInt(Math.round(gameConfig.houseEdge * 10000)) / 10000n); // Apply edge to winnings portion (1x wager)
            finalPayoutLamports = wagerAmountLamports + winnings; // Return wager + winnings
            finalStatus = 'completed_win';
            console.log(`${logPrefix} Win! Wager: ${wagerAmountLamports}, Winnings (after ${gameConfig.houseEdge * 100}% edge): ${winnings}, Total Payout: ${finalPayoutLamports}`);
        } else {
            console.log(`${logPrefix} Loss. Payout: 0`);
        }

        // 3. DB Transaction
        client = await pool.connect();
        await client.query('BEGIN');

        // 3a. Update Bet Status
        const statusUpdated = await updateBetStatus(client, betId, finalStatus, finalPayoutLamports);
        if (!statusUpdated) {
            throw new Error(`Failed to update bet status to ${finalStatus}`);
        }

        // 3b. Update Balance if win
        if (finalPayoutLamports > 0n) {
            const balanceResult = await updateUserBalanceAndLedger(client, userId, finalPayoutLamports, 'bet_won', { betId }, `Coinflip ${result}: ${outcome}`);
            if (!balanceResult.success) {
                throw new Error(`Failed to credit balance: ${balanceResult.error}`);
            }
        }

        // 3c. Commit
        await client.query('COMMIT');
        console.log(`${logPrefix} DB updates committed.`);

        // 4. Notify User
        const wagerSOL = (Number(wagerAmountLamports) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
        let notifyMsg = `🪙 *Coinflip Result*\n\nOutcome: *${escapeMarkdownV2(outcome.toUpperCase())}*`;
        if (result === 'win') {
            const payoutSOL = (Number(finalPayoutLamports) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
            const winAmountSOL = (Number(finalPayoutLamports - wagerAmountLamports) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
            notifyMsg += `\nYou chose ${escapeMarkdownV2(choice)} and *WON*\\! 🎉\nBet: ${escapeMarkdownV2(wagerSOL)} SOL\nWon: ${escapeMarkdownV2(winAmountSOL)} SOL\nReturned: ${escapeMarkdownV2(payoutSOL)} SOL`; // Escaped !
        } else {
            notifyMsg += `\nYou chose ${escapeMarkdownV2(choice)} and *LOST*\\. 😥\nBet: ${escapeMarkdownV2(wagerSOL)} SOL`; // Escaped .
        }
        await safeSendMessage(chatId, notifyMsg, { parse_mode: 'MarkdownV2' });

        return true; // Indicate success for referral checks

    } catch (error) {
        console.error(`${logPrefix} Error processing coinflip result:`, error);
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { console.error(`${logPrefix} Rollback failed:`, rbErr); } }
        await notifyAdmin(`🚨 ERROR processing Coinflip Bet ${betId} User ${userId}:\n${error.message}`);
        // Try to notify user of the error
        await safeSendMessage(chatId, `⚠️ Error processing your coinflip result\\. Please contact support if your balance seems incorrect\\. Bet ID: ${betId}`, { parse_mode: 'MarkdownV2'}).catch(()=>{}); // Escaped .
        return false; // Indicate failure
    } finally {
        if (client) client.release();
    }
}

/**
 * Handles Race game execution, DB updates, and user notification.
 * @returns {Promise<boolean>} True if successful, false otherwise.
 */
async function handleRaceGame(userId, chatId, wagerAmountLamports, chosenHorseName, betId) {
    const logPrefix = `[RaceGame Bet ${betId} User ${userId}]`;
    let client = null;
    const gameConfig = GAME_CONFIG.race;

    try {
        // 1. Simulate Game
        // Simplified simulation: Pick a random winner name
        const winningHorse = RACE_HORSES[Math.floor(Math.random() * RACE_HORSES.length)];
        const result = (chosenHorseName === winningHorse.name) ? 'win' : 'loss';
        console.log(`${logPrefix} Winning horse: ${winningHorse.name}. User choice: ${chosenHorseName}. User ${result}.`);

        // 2. Calculate Payout
        let finalPayoutLamports = 0n;
        let finalStatus = 'completed_loss';
        let winnings = 0n;
        if (result === 'win') {
            const chosenHorseConfig = RACE_HORSES.find(h => h.name === chosenHorseName);
            const baseMultiplier = chosenHorseConfig?.payoutMultiplier || 1; // Fallback multiplier if config missing? Should not happen with validation.
            // Winnings = Wager * Multiplier * (1 - HouseEdge)
            winnings = (wagerAmountLamports * BigInt(baseMultiplier)) - (wagerAmountLamports * BigInt(baseMultiplier) * BigInt(Math.round(gameConfig.houseEdge * 10000)) / 10000n);
            finalPayoutLamports = wagerAmountLamports + winnings; // Return wager + winnings
            finalStatus = 'completed_win';
            console.log(`${logPrefix} Win! Wager: ${wagerAmountLamports}, Multiplier: ${baseMultiplier}x, Winnings (after ${gameConfig.houseEdge * 100}% edge): ${winnings}, Total Payout: ${finalPayoutLamports}`);
        } else {
            console.log(`${logPrefix} Loss. Payout: 0`);
        }

        // 3. DB Transaction
        client = await pool.connect();
        await client.query('BEGIN');

        // 3a. Update Bet Status
        const statusUpdated = await updateBetStatus(client, betId, finalStatus, finalPayoutLamports);
        if (!statusUpdated) {
            throw new Error(`Failed to update bet status to ${finalStatus}`);
        }

        // 3b. Update Balance if win
        if (finalPayoutLamports > 0n) {
            const balanceResult = await updateUserBalanceAndLedger(client, userId, finalPayoutLamports, 'bet_won', { betId }, `Race ${result}: ${winningHorse.name} won`);
            if (!balanceResult.success) {
                throw new Error(`Failed to credit balance: ${balanceResult.error}`);
            }
        }

        // 3c. Commit
        await client.query('COMMIT');
        console.log(`${logPrefix} DB updates committed.`);

        // 4. Notify User
        const wagerSOL = (Number(wagerAmountLamports) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
        let notifyMsg = `🐎 *Race Result*\n\nWinning Horse: *${escapeMarkdownV2(winningHorse.name)}*`;
        if (result === 'win') {
            const payoutSOL = (Number(finalPayoutLamports) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
            const winAmountSOL = (Number(winnings) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
            notifyMsg += `\nYou chose the winner and *WON*\\! 🎉\nBet: ${escapeMarkdownV2(wagerSOL)} SOL\nWon: ${escapeMarkdownV2(winAmountSOL)} SOL\nReturned: ${escapeMarkdownV2(payoutSOL)} SOL`; // Escaped !
        } else {
            notifyMsg += `\nYou chose ${escapeMarkdownV2(chosenHorseName)}\\. Better luck next time\\! 😥\nBet: ${escapeMarkdownV2(wagerSOL)} SOL`; // Escaped ! .
        }
        await safeSendMessage(chatId, notifyMsg, { parse_mode: 'MarkdownV2' });

        return true; // Indicate success

    } catch (error) {
        console.error(`${logPrefix} Error processing race result:`, error);
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { console.error(`${logPrefix} Rollback failed:`, rbErr); } }
        await notifyAdmin(`🚨 ERROR processing Race Bet ${betId} User ${userId}:\n${error.message}`);
        await safeSendMessage(chatId, `⚠️ Error processing your race result\\. Please contact support if your balance seems incorrect\\. Bet ID: ${betId}`, { parse_mode: 'MarkdownV2'}).catch(()=>{}); // Escaped .
        return false; // Indicate failure
    } finally {
        if (client) client.release();
    }
}


/**
 * Handles Slots game execution, DB updates, and user notification.
 * @returns {Promise<boolean>} True if successful, false otherwise.
 */
async function handleSlotsGame(userId, chatId, wagerAmountLamports, betId) {
    const logPrefix = `[SlotsGame Bet ${betId} User ${userId}]`;
    let client = null;
    const gameConfig = GAME_CONFIG.slots;

    try {
        // 1. Simulate Game
        const { result, symbols, payoutMultiplier } = simulateSlots();
        const symbolsString = symbols.map(s => `[${s}]`).join(' '); // e.g., "[C] [L] [O]"
        console.log(`${logPrefix} Result: ${symbolsString}. User ${result}. Multiplier: ${payoutMultiplier}x.`);

        // 2. Calculate Payout
        let finalPayoutLamports = 0n;
        let finalStatus = 'completed_loss';
        let winnings = 0n;
        if (result === 'win' && payoutMultiplier > 0) {
            // Winnings = Wager * Multiplier * (1 - HouseEdge)
            // Apply edge to the winnings derived from the multiplier
            winnings = (wagerAmountLamports * BigInt(payoutMultiplier)) - (wagerAmountLamports * BigInt(payoutMultiplier) * BigInt(Math.round(gameConfig.houseEdge * 10000)) / 10000n);
            finalPayoutLamports = wagerAmountLamports + winnings; // Return wager + winnings
            finalStatus = 'completed_win';
            console.log(`${logPrefix} Win! Wager: ${wagerAmountLamports}, Multiplier: ${payoutMultiplier}x, Winnings (after ${gameConfig.houseEdge * 100}% edge): ${winnings}, Total Payout: ${finalPayoutLamports}`);
        } else {
             console.log(`${logPrefix} Loss. Payout: 0`);
        }

        // 3. DB Transaction
        client = await pool.connect();
        await client.query('BEGIN');

        // 3a. Update Bet Status
        const statusUpdated = await updateBetStatus(client, betId, finalStatus, finalPayoutLamports);
        if (!statusUpdated) {
            throw new Error(`Failed to update bet status to ${finalStatus}`);
        }

        // 3b. Update Balance if win
        if (finalPayoutLamports > 0n) {
            const balanceResult = await updateUserBalanceAndLedger(client, userId, finalPayoutLamports, 'bet_won', { betId }, `Slots ${result}: ${symbolsString}`);
            if (!balanceResult.success) {
                throw new Error(`Failed to credit balance: ${balanceResult.error}`);
            }
        }

        // 3c. Commit
        await client.query('COMMIT');
        console.log(`${logPrefix} DB updates committed.`);

        // 4. Notify User
        const wagerSOL = (Number(wagerAmountLamports) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
        // Escape symbols for Markdown
        const symbolsMd = symbols.map(s => `[${escapeMarkdownV2(s)}]`).join(' ');
        let notifyMsg = `🎰 *Slots Result*\n\n${symbolsMd}`;
        if (result === 'win') {
            const payoutSOL = (Number(finalPayoutLamports) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
            const winAmountSOL = (Number(winnings) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
            notifyMsg += `\n*WINNER*\\! 🎉 (${escapeMarkdownV2(payoutMultiplier)}x)\nBet: ${escapeMarkdownV2(wagerSOL)} SOL\nWon: ${escapeMarkdownV2(winAmountSOL)} SOL\nReturned: ${escapeMarkdownV2(payoutSOL)} SOL`; // Escaped !
        } else {
            notifyMsg += `\nBetter luck next time\\! 😥\nBet: ${escapeMarkdownV2(wagerSOL)} SOL`; // Escaped ! .
        }
        await safeSendMessage(chatId, notifyMsg, { parse_mode: 'MarkdownV2' });

        return true; // Indicate success

    } catch (error) {
        console.error(`${logPrefix} Error processing slots result:`, error);
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { console.error(`${logPrefix} Rollback failed:`, rbErr); } }
        await notifyAdmin(`🚨 ERROR processing Slots Bet ${betId} User ${userId}:\n${error.message}`);
        await safeSendMessage(chatId, `⚠️ Error processing your slots result\\. Please contact support if your balance seems incorrect\\. Bet ID: ${betId}`, { parse_mode: 'MarkdownV2'}).catch(()=>{}); // Escaped .
        return false; // Indicate failure
    } finally {
        if (client) client.release();
    }
}

/**
 * Handles War game execution, DB updates, and user notification.
 * @returns {Promise<boolean>} True if successful, false otherwise.
 */
async function handleWarGame(userId, chatId, wagerAmountLamports, betId) {
    const logPrefix = `[WarGame Bet ${betId} User ${userId}]`;
    let client = null;
    const gameConfig = GAME_CONFIG.war;

    try {
        // 1. Simulate Game
        const { result, playerCard, dealerCard } = simulateWar(); // result is 'win', 'loss', or 'push'
        console.log(`${logPrefix} Player: ${playerCard}, Dealer: ${dealerCard}. Result: ${result}.`);

        // 2. Calculate Payout
        let finalPayoutLamports = 0n;
        let finalStatus = 'completed_loss';
        let winnings = 0n;
        let ledgerType = 'bet_lost'; // Placeholder, might not be needed if balance isn't changing

        if (result === 'win') {
            // Winnings = Wager * (1 - HouseEdge)
            winnings = wagerAmountLamports - (wagerAmountLamports * BigInt(Math.round(gameConfig.houseEdge * 10000)) / 10000n);
            finalPayoutLamports = wagerAmountLamports + winnings; // Return wager + winnings
            finalStatus = 'completed_win';
            ledgerType = 'bet_won';
            console.log(`${logPrefix} Win! Wager: ${wagerAmountLamports}, Winnings (after ${gameConfig.houseEdge * 100}% edge): ${winnings}, Total Payout: ${finalPayoutLamports}`);
        } else if (result === 'push') {
            finalPayoutLamports = wagerAmountLamports; // Return original wager
            finalStatus = 'completed_push';
            ledgerType = 'bet_push_return';
            console.log(`${logPrefix} Push! Payout: ${finalPayoutLamports}`);
        } else {
            // Loss handled by defaults (payout=0, status=completed_loss)
            console.log(`${logPrefix} Loss. Payout: 0`);
        }

        // 3. DB Transaction
        client = await pool.connect();
        await client.query('BEGIN');

        // 3a. Update Bet Status
        const statusUpdated = await updateBetStatus(client, betId, finalStatus, finalPayoutLamports);
        if (!statusUpdated) {
            throw new Error(`Failed to update bet status to ${finalStatus}`);
        }

        // 3b. Update Balance if win or push
        if (finalPayoutLamports > 0n) {
            const balanceResult = await updateUserBalanceAndLedger(client, userId, finalPayoutLamports, ledgerType, { betId }, `War ${result}: P:${playerCard} D:${dealerCard}`);
            if (!balanceResult.success) {
                throw new Error(`Failed to update balance: ${balanceResult.error}`);
            }
        }

        // 3c. Commit
        await client.query('COMMIT');
        console.log(`${logPrefix} DB updates committed.`);

        // 4. Notify User
        const wagerSOL = (Number(wagerAmountLamports) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
        let notifyMsg = `🃏 *Casino War Result*\n\n` +
                        `Your Card: *${escapeMarkdownV2(playerCard)}*\n` +
                        `Dealer Card: *${escapeMarkdownV2(dealerCard)}*\n\n`;

        if (result === 'win') {
            const payoutSOL = (Number(finalPayoutLamports) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
            const winAmountSOL = (Number(winnings) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
            notifyMsg += `You *WON*\\! 🎉\nBet: ${escapeMarkdownV2(wagerSOL)} SOL\nWon: ${escapeMarkdownV2(winAmountSOL)} SOL\nReturned: ${escapeMarkdownV2(payoutSOL)} SOL`; // Escaped !
        } else if (result === 'push') {
            notifyMsg += `It's a *PUSH* \\(Tie\\)! Your bet is returned\\. 🤝\nBet: ${escapeMarkdownV2(wagerSOL)} SOL`; // Escaped ! . ()
        } else {
            notifyMsg += `You *LOST*\\. 😥\nBet: ${escapeMarkdownV2(wagerSOL)} SOL`; // Escaped .
        }
        await safeSendMessage(chatId, notifyMsg, { parse_mode: 'MarkdownV2' });

        return true; // Indicate success

    } catch (error) {
        console.error(`${logPrefix} Error processing war result:`, error);
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { console.error(`${logPrefix} Rollback failed:`, rbErr); } }
        await notifyAdmin(`🚨 ERROR processing War Bet ${betId} User ${userId}:\n${error.message}`);
        await safeSendMessage(chatId, `⚠️ Error processing your War result\\. Please contact support if your balance seems incorrect\\. Bet ID: ${betId}`, { parse_mode: 'MarkdownV2'}).catch(()=>{}); // Escaped .
        return false; // Indicate failure
    } finally {
        if (client) client.release();
    }
}


// --- End of NEW Game Result Handlers ---

// --- Part 5a Ends Here ---
// index.js - Part 5b: Input Handlers, Command Handlers, Referral Checks
// --- VERSION: 3.1.5 --- (Continuing Part 5)

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
    const stateType = currentState.state;
    const logPrefix = `[Input State:${stateType} User ${userId}]`;
    console.log(`${logPrefix} Received amount input: "${text}"`); // [LOGGING ADDED] Log input received

    let amountSOL;
    try {
        amountSOL = parseFloat(text);
        if (isNaN(amountSOL) || amountSOL <= 0) {
             // Escaped Message
             await safeSendMessage(chatId, "⚠️ Invalid amount\\. Please enter a positive number \\(e\\.g\\., 0\\.1\\)\\.", { parse_mode: 'MarkdownV2' });
             console.log(`${logPrefix} Invalid amount entered.`); // [LOGGING ADDED] Log validation fail
            return;
        }
    } catch (e) {
         // Escaped Message
         await safeSendMessage(chatId, "⚠️ Could not read amount\\. Please enter a number \\(e\\.g\\., 0\\.1\\)\\.", { parse_mode: 'MarkdownV2' });
         console.log(`${logPrefix} Could not parse amount input.`); // [LOGGING ADDED] Log parse fail
        return;
    }

    const amountLamports = BigInt(Math.round(amountSOL * LAMPORTS_PER_SOL));
    console.log(`${logPrefix} Parsed amount: ${amountSOL} SOL (${amountLamports} lamports).`); // [LOGGING ADDED] Log parsed amount

    // --- Withdrawal Amount Handling ---
    if (stateType === 'awaiting_withdrawal_amount') {
        console.log(`${logPrefix} Handling withdrawal amount.`); // [LOGGING ADDED] Log context
        if (amountLamports < MIN_WITHDRAWAL_LAMPORTS) {
             // Escaped Message
             await safeSendMessage(chatId, `⚠️ Minimum withdrawal amount is ${escapeMarkdownV2((Number(MIN_WITHDRAWAL_LAMPORTS) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL\\. Please enter a valid amount\\.`, { parse_mode: 'MarkdownV2' });
             console.log(`${logPrefix} Withdrawal amount below minimum.`); // [LOGGING ADDED] Log validation fail
            return;
        }
        const fee = WITHDRAWAL_FEE_LAMPORTS;
        const totalNeeded = amountLamports + fee;
        console.log(`${logPrefix} Checking balance for withdrawal. Need: ${totalNeeded}, Fee: ${fee}`); // [LOGGING ADDED] Log check
        const currentBalance = await getUserBalance(userId);
        if (currentBalance < totalNeeded) {
             // Escaped Message
             await safeSendMessage(chatId, `⚠️ Insufficient balance \\(${escapeMarkdownV2((Number(currentBalance) / LAMPORTS_PER_SOL).toFixed(4))} SOL\\) for withdrawal amount \\(${escapeMarkdownV2(amountSOL.toFixed(4))} SOL\\) \\+ fee \\(${escapeMarkdownV2((Number(fee)/LAMPORTS_PER_SOL).toFixed(4))} SOL\\)\\. Total needed: ${escapeMarkdownV2((Number(totalNeeded)/LAMPORTS_PER_SOL).toFixed(4))} SOL`, { parse_mode: 'MarkdownV2' });
             console.log(`${logPrefix} Insufficient balance for withdrawal. Balance: ${currentBalance}`); // [LOGGING ADDED] Log balance fail
            userStateCache.delete(userId); // Clear state after failure
            return;
        }
        console.log(`${logPrefix} Balance sufficient. Checking linked wallet...`); // [LOGGING ADDED] Log balance OK
        const linkedAddress = await getLinkedWallet(userId);
        if (!linkedAddress) {
             // Escaped Message
             await safeSendMessage(chatId, `⚠️ Cannot proceed\\. Please set your withdrawal address first using \`/wallet <YourSolanaAddress>\`\\.`, { parse_mode: 'MarkdownV2' });
             console.log(`${logPrefix} No linked wallet found.`); // [LOGGING ADDED] Log wallet fail
            userStateCache.delete(userId); // Clear state
            return;
        }
        console.log(`${logPrefix} Linked wallet found: ${linkedAddress}. Proceeding to confirmation.`); // [LOGGING ADDED] Log wallet OK
        userStateCache.set(userId, {
            state: 'awaiting_withdrawal_confirm', chatId: String(chatId), messageId: currentState.messageId, // Persist original message ID for edits
            data: { amountLamports: amountLamports.toString(), recipientAddress: linkedAddress }, timestamp: Date.now()
        });
        const options = {
            reply_markup: { inline_keyboard: [[{ text: '✅ Confirm Withdrawal', callback_data: 'withdraw_confirm' }], [{ text: '❌ Cancel', callback_data: 'cancel_action' }]] },
            parse_mode: 'MarkdownV2'
        };
         // Escaped Message
         await safeSendMessage(chatId,
            `*Confirm Withdrawal*\n\n` +
            `Amount: ${escapeMarkdownV2(amountSOL.toFixed(SOL_DECIMALS))} SOL\n` +
            `Fee: ${escapeMarkdownV2((Number(fee) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL\n` +
            `Recipient: \`${escapeMarkdownV2(linkedAddress)}\`\n\n` +
            `Total to be deducted: ${escapeMarkdownV2((Number(totalNeeded) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL\\. Press confirm to proceed\\.`, // Escaped . (x2)
            options
        );
        return;
    }

    // --- Game Amount Handling ---
    let gameKey = '', nextState = '', gameName = '', confirmationCallback = '', choicePrompt = null;

    if (stateType === 'awaiting_cf_amount') { gameKey = 'coinflip'; nextState = 'awaiting_cf_choice'; gameName = 'Coinflip'; choicePrompt = `🪙 Betting ${escapeMarkdownV2(amountSOL.toFixed(SOL_DECIMALS))} SOL on Coinflip\\. Choose Heads or Tails:`; } // Escaped . :
    else if (stateType === 'awaiting_race_amount') { gameKey = 'race'; nextState = 'awaiting_race_choice'; gameName = 'Race'; choicePrompt = `🐎 Betting ${escapeMarkdownV2(amountSOL.toFixed(SOL_DECIMALS))} SOL on the Race\\. Choose your horse:`; } // Escaped . :
    else if (stateType === 'awaiting_slots_amount') { gameKey = 'slots'; nextState = 'awaiting_slots_confirm'; gameName = 'Slots'; confirmationCallback = 'slots_spin_confirm'; }
    else if (stateType === 'awaiting_war_amount') { gameKey = 'war'; nextState = 'awaiting_war_confirm'; gameName = 'Casino War'; confirmationCallback = 'war_confirm'; }

    if (gameKey) {
        console.log(`${logPrefix} Handling amount for game: ${gameName}`); // [LOGGING ADDED] Log context
        const gameCfg = GAME_CONFIG[gameKey];
        if (!gameCfg) {
             console.error(`${logPrefix} Invalid game key derived from state: ${gameKey}`);
             userStateCache.delete(userId);
             // Escaped Message
              await safeSendMessage(chatId, `⚠️ Internal error starting game\\. Please try again\\.`, { parse_mode: 'MarkdownV2'});
             return;
        }
        // Check Min/Max Bet
        if (amountLamports < gameCfg.minBetLamports || amountLamports > gameCfg.maxBetLamports) {
            const min = (Number(gameCfg.minBetLamports) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
            const max = (Number(gameCfg.maxBetLamports) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
             // Escaped Message
             await safeSendMessage(chatId, `⚠️ ${escapeMarkdownV2(gameName)} bet amount must be between ${escapeMarkdownV2(min)} and ${escapeMarkdownV2(max)} SOL\\. Please enter a valid amount\\.`, { parse_mode: 'MarkdownV2' });
             console.log(`${logPrefix} Bet amount out of range. Min: ${gameCfg.minBetLamports}, Max: ${gameCfg.maxBetLamports}`); // [LOGGING ADDED] Log validation fail
            return; // Keep user in same state to re-enter amount
        }
        console.log(`${logPrefix} Bet amount within limits. Checking balance...`); // [LOGGING ADDED] Log limits OK
        // Check Balance
        const currentBalance = await getUserBalance(userId);
        if (currentBalance < amountLamports) {
             // Escaped Message
             await safeSendMessage(chatId, `⚠️ Insufficient balance \\(${escapeMarkdownV2((Number(currentBalance) / LAMPORTS_PER_SOL).toFixed(4))} SOL\\) to place that ${escapeMarkdownV2(gameName)} bet\\. Use /deposit to add funds\\.`, { parse_mode: 'MarkdownV2' });
             console.log(`${logPrefix} Insufficient balance for bet. Balance: ${currentBalance}`); // [LOGGING ADDED] Log balance fail
            userStateCache.delete(userId); // Clear state
            return;
        }
        console.log(`${logPrefix} Balance sufficient. Proceeding to next step: ${nextState}`); // [LOGGING ADDED] Log balance OK
        // Update state for next step (choice or confirmation)
        userStateCache.set(userId, { state: nextState, chatId: String(chatId), messageId: currentState.messageId, data: { amountLamports: amountLamports.toString() }, timestamp: Date.now() });

        if (choicePrompt) { // For games requiring a choice after amount
            let keyboard = [];
            if (gameKey === 'coinflip') { keyboard = [[{ text: 'Heads', callback_data: 'cf_choice:heads' }, { text: 'Tails', callback_data: 'cf_choice:tails' }], [{ text: 'Cancel', callback_data: 'cancel_action' }]]; }
            else if (gameKey === 'race') {
                 // Escape horse names/multipliers in button text
                 const horseButtons = RACE_HORSES.map(h => ({ text: `${h.name} (${h.payoutMultiplier}x)`, callback_data: `race_choice:${h.name}` })); // No need to escape button text itself
                 // Split buttons into rows of 2
                 for (let i = 0; i < horseButtons.length; i += 2) { keyboard.push(horseButtons.slice(i, i + 2)); }
                 keyboard.push([{ text: 'Cancel', callback_data: 'cancel_action' }]);
            }
            const options = { reply_markup: { inline_keyboard: keyboard }, parse_mode: 'MarkdownV2' };
            await safeSendMessage(chatId, choicePrompt, options);
        } else if (confirmationCallback) { // For games needing simple confirmation
            const options = { reply_markup: { inline_keyboard: [[{ text: `✅ Confirm ${gameName} Bet`, callback_data: confirmationCallback }], [{ text: '❌ Cancel', callback_data: 'cancel_action' }]] }, parse_mode: 'MarkdownV2' };
             // Escaped Message
             await safeSendMessage(chatId, `Confirm ${escapeMarkdownV2(gameName)} bet of ${escapeMarkdownV2(amountSOL.toFixed(SOL_DECIMALS))} SOL\\?`, options); // Escaped ?
        } else { console.error(`${logPrefix} No next step defined for game ${gameKey}`); userStateCache.delete(userId); }
    } else { console.warn(`${logPrefix} Amount received in unexpected state: ${stateType}`); userStateCache.delete(userId); }
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
    console.log(`${logPrefix} Received address input: "${address}"`); // [LOGGING ADDED] Log input

    // This state seems currently unused by the primary /withdraw flow,
    // but we'll keep the validation logic in case it's used elsewhere.
    if (!currentState || currentState.state !== 'awaiting_withdrawal_address') {
        console.error(`${logPrefix} Invalid state reached when expecting withdrawal address.`);
        userStateCache.delete(userId);
        // Escaped Message
        await safeSendMessage(chatId, `⚠️ An error occurred\\. Please start the withdrawal again with /withdraw\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    // Validate address format
    let recipientPubKey;
    try { recipientPubKey = new PublicKey(address); } catch (e) {
        // Escaped Message
        await safeSendMessage(chatId, `⚠️ Invalid Solana address format: \`${escapeMarkdownV2(address)}\`\\. Please enter a valid address\\.`, { parse_mode: 'MarkdownV2' });
        console.log(`${logPrefix} Invalid address format.`); // [LOGGING ADDED] Log validation fail
       return; // Keep user in state to retry
    }
    // Prevent withdrawal to bot's own payout addresses
    const botKeys = [process.env.MAIN_BOT_PRIVATE_KEY, process.env.REFERRAL_PAYOUT_PRIVATE_KEY].filter(Boolean)
                           .map(pkStr => { try { return Keypair.fromSecretKey(bs58.decode(pkStr)).publicKey.toBase58(); } catch { return null; }})
                           .filter(Boolean);
    if (botKeys.includes(address)) {
        // Escaped Message
        await safeSendMessage(chatId, `⚠️ You cannot withdraw funds to the bot's own operational addresses\\. Please enter your personal wallet address\\.`, { parse_mode: 'MarkdownV2' });
        console.log(`${logPrefix} Attempt to withdraw to bot's own address.`); // [LOGGING ADDED] Log validation fail
       return; // Keep user in state to retry
    }

    // If this state *was* used, it would likely transition to confirm here.
    // Example:
    // const amountLamports = BigInt(currentState.data.amountLamports); // Assuming amount was stored earlier
    // const feeLamports = WITHDRAWAL_FEE_LAMPORTS;
    // const totalDeduction = amountLamports + feeLamports;
    // const amountSOL = Number(amountLamports) / LAMPORTS_PER_SOL;
    // const feeSOL = Number(feeLamports) / LAMPORTS_PER_SOL;
    // userStateCache.set(userId, { state: 'awaiting_withdrawal_confirm', /* ... rest ... */ data: { amountLamports: amountLamports.toString(), recipientAddress: address }});
    // ... send confirmation message ...
    console.log(`${logPrefix} Processed address. (Note: This state seems currently unused in main flow)`);
    // For now, just clear the state as it's unclear how it should proceed
    userStateCache.delete(userId);
     // Escaped Message
     await safeSendMessage(chatId, `Address received, but the flow seems incomplete\\. Please use \`/wallet <address>\` first, then \`/withdraw\`\\.`, { parse_mode: 'MarkdownV2' });

}

/** Handles Roulette bet input (placeholder for complex betting). */
async function handleRouletteBetInput(msg, currentState) {
     const chatId = msg.chat.id;
      // Escaped Message
      await safeSendMessage(chatId, `Roulette bet input via text is complex\\. Coming soon via a better interface\\! Please /cancel and try another game\\.`, { parse_mode: 'MarkdownV2'});
     userStateCache.delete(String(msg.from.id));
}


// --- Command Handlers ---

/** Handles /start command */
async function handleStartCommand(msg, args) {
    const chatId = msg.chat.id;
    const userId = String(msg.from.id);
    const firstName = msg.from.first_name || 'there';
    console.log(`[Cmd /start User ${userId}] Handling start command.`); // [LOGGING ADDED] Log command
    await getUserBalance(userId); // Ensure user exists
    userStateCache.set(userId, { state: 'idle', chatId: String(chatId), timestamp: Date.now() }); // Reset state
    const options = { /* ... keyboard definition as before ... */
         reply_markup: {
             inline_keyboard: [
                  [ { text: '🪙 Coinflip', callback_data: 'menu:coinflip' }, { text: '🎰 Slots', callback_data: 'menu:slots' }, { text: '🃏 War', callback_data: 'menu:war' } ],
                  [ { text: '🐎 Race', callback_data: 'menu:race' }, { text: '⚪️ Roulette', callback_data: 'menu:roulette' } ],
                  [ { text: '💰 Deposit SOL', callback_data: 'menu:deposit' }, { text: '💸 Withdraw SOL', callback_data: 'menu:withdraw' } ],
                  [ { text: '👤 Wallet / Balance', callback_data: 'menu:wallet' }, { text: '🤝 Referral Info', callback_data: 'menu:referral' }, { text: '❓ Help', callback_data: 'menu:help' } ]
             ]
        },
       parse_mode: 'MarkdownV2' };
    // Escaped Message
    const welcomeMsg = `👋 Welcome, ${escapeMarkdownV2(firstName)}\\!\n\n` + // Escaped !
                      `I am a Solana\\-based gaming bot\\. Use the buttons below to play, manage funds, or get help\\.\n\n` + // Escaped -, . (x2)
                      `*Remember to gamble responsibly\\!*`; // Escaped !
    await safeSendMessage(chatId, welcomeMsg, options);
}

/** Handles /help command */
async function handleHelpCommand(msg, args) {
    const chatId = msg.chat.id;
    const userId = String(msg.from.id);
    console.log(`[Cmd /help User ${userId}] Handling help command.`); // [LOGGING ADDED] Log command
    const feeSOL = (Number(WITHDRAWAL_FEE_LAMPORTS) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
    const minWithdrawSOL = (Number(MIN_WITHDRAWAL_LAMPORTS) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
    const refRewardPercent = (REFERRAL_MILESTONE_REWARD_PERCENT * 100).toFixed(2);

    // Escaped Help Message (Escape all ., -, !, \, /, <, >, %, +, =, |) - Updated Version
    const helpMsg = `*Solana Gambles Bot Help* 🤖 \\(v3\\.1\\.5\\)\n\n` + // *** Updated Version ***
                   `This bot uses a *custodial* model\\. Deposit SOL to play instantly using your internal balance\\. Winnings are added to your balance, and you can withdraw anytime\\. \n\n`+ // Escaped . (x3)
                   `*How to Play:*\n` +
                   `1\\. Use \`/deposit\` to get a unique address & send SOL\\. Wait for confirmation\\. \n` + // Escaped . (x2)
                   `2\\. Use \`/start\` or the game commands \\(\`/coinflip\`, \`/slots\`, etc\\.\\) or menu buttons\\. \n` + // Escaped ( ) . (x2)
                   `3\\. Enter your bet amount when prompted\\. \n` + // Escaped .
                   `4\\. Confirm your bet & play\\!\n\n` + // Escaped !
                   `*Core Commands:*\n` +
                   `/start \\- Show main menu \n` + // Escaped -
                   `/help \\- Show this message \n` + // Escaped -
                   `/wallet \\- View balance & withdrawal address \n` + // Escaped -
                   `/wallet <YourSolAddress> \\- Set/update withdrawal address \n` + // Escaped - < >
                   `/deposit \\- Get deposit address \n` + // Escaped -
                   `/withdraw \\- Start withdrawal process \n` + // Escaped -
                   `/referral \\- View your referral stats & link \n\n` + // Escaped -
                   `*Game Commands:* Initiate a game \\(e\\.g\\., \`/coinflip\`\\)\n\n` + // Escaped ( ., )
                   `*Referral Program:* 🤝\n` +
                   `Use \`/referral\` to get your code/link\\. New users start with \`/start <YourCode>\`\\. You earn SOL based on their first bet and their total wager volume \\(${escapeMarkdownV2(refRewardPercent)}\\% milestone bonus\\)\\. Rewards are paid to your linked wallet\\.\n\n` + // Escaped . (x4) % ( )
                   `*Important Notes:*\n` +
                   `\\- Only deposit to addresses given by \`/deposit\`\\. Each address is temporary and for one deposit only\\. It expires in ${escapeMarkdownV2(process.env.DEPOSIT_ADDRESS_EXPIRY_MINUTES)} minutes\\. \n` + // Escaped - . (x3)
                   `\\- You *must* set a withdrawal address using \`/wallet <address>\` before withdrawing\\. \n` + // Escaped - . < >
                   `\\- Withdrawals have a fee \\(${escapeMarkdownV2(feeSOL)} SOL\\) and minimum \\(${escapeMarkdownV2(minWithdrawSOL)} SOL\\)\\.\n` + // Escaped - ( ) ( ) .
                   `\\- Please gamble responsibly\\. Contact support if you encounter issues\\.`; // Escaped - . (x2)

    await safeSendMessage(chatId, helpMsg, { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}

/** Handles /wallet command (viewing or setting address) */
async function handleWalletCommand(msg, args) {
    const chatId = msg.chat.id;
    const userId = String(msg.from.id);
    const logPrefix = `[Cmd /wallet User ${userId}]`; // [LOGGING ADDED] Prefix
    const potentialNewAddress = args?.trim();

    if (potentialNewAddress) {
        console.log(`${logPrefix} Attempting to set withdrawal address to: ${potentialNewAddress}`); // [LOGGING ADDED] Log action
        const result = await linkUserWallet(userId, potentialNewAddress);
        if (result.success) {
             // Escaped Message
             await safeSendMessage(chatId, `✅ Withdrawal address successfully set to: \`${escapeMarkdownV2(result.wallet)}\``, { parse_mode: 'MarkdownV2' });
        } else {
             // Escaped Message
             await safeSendMessage(chatId, `❌ Failed to set withdrawal address\\. Error: ${escapeMarkdownV2(result.error)}`, { parse_mode: 'MarkdownV2' });
        }
    } else {
        console.log(`${logPrefix} Displaying wallet info.`); // [LOGGING ADDED] Log action
        const userDetails = await getUserWalletDetails(userId);
        const userBalance = await getUserBalance(userId); // Ensures user/balance exists
        // Escaped Message
        let walletMsg = `*Your Wallet & Referral Info* 💼\n\n` +
                         `*Balance:* ${escapeMarkdownV2((Number(userBalance) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL\n`;
        if (userDetails?.external_withdrawal_address) {
            walletMsg += `*Withdrawal Address:* \`${escapeMarkdownV2(userDetails.external_withdrawal_address)}\`\n`;
        } else {
            walletMsg += `*Withdrawal Address:* Not Set \\(Use \`/wallet <YourSolAddress>\`\\)\n`; // Escaped ( ) < >
        }
        // Handle referral code generation if missing (like in original code, ensure user exists first via getUserBalance)
        let currentRefCode = userDetails?.referral_code;
        // Check if user exists (implicit via userBalance call succeeding) but code is missing
        if (!currentRefCode && userBalance !== null) { // Check if userBalance isn't null (meaning user exists)
             console.log(`${logPrefix} User exists but lacks referral code. Generating and attempting to save.`);
             const genCode = generateReferralCode();
             let client = null;
             try {
                  client = await pool.connect();
                  await client.query('BEGIN');
                  // Attempt to set it - this might fail if called concurrently, but is low risk
                  const updateRes = await queryDatabase(
                       `UPDATE wallets SET referral_code = $1 WHERE user_id = $2 AND referral_code IS NULL RETURNING referral_code`,
                       [genCode, userId],
                       client
                  );
                  if (updateRes.rowCount > 0) {
                       await client.query('COMMIT');
                       currentRefCode = updateRes.rows[0].referral_code;
                       console.log(`${logPrefix} Successfully generated and saved new referral code: ${currentRefCode}`);
                       // Update cache
                       updateWalletCache(userId, { referralCode: currentRefCode });
                  } else {
                       await client.query('ROLLBACK'); // Didn't update, maybe race condition? Fetch again.
                       console.log(`${logPrefix} Referral code update failed (maybe set concurrently?), fetching existing.`);
                       const refetch = await queryDatabase('SELECT referral_code FROM wallets WHERE user_id = $1', [userId]);
                       currentRefCode = refetch.rows[0]?.referral_code;
                  }
             } catch (e) {
                  if (client) try { await client.query('ROLLBACK'); } catch (rbErr) { /* ignore */ }
                  console.error(`${logPrefix} Error generating/saving referral code: ${e.message}`);
                  // Notify Admin? Low priority maybe.
             } finally {
                  if (client) client.release();
             }
        }

        if (currentRefCode) {
            walletMsg += `*Referral Code:* \`${escapeMarkdownV2(currentRefCode)}\`\n`;
        } else {
             walletMsg += `*Referral Code:* \`N/A\` \\(Try again or link wallet\\)\n`; // Escaped ( )
        }

        walletMsg += `*Successful Referrals:* ${escapeMarkdownV2(userDetails?.referral_count || 0)}\n\n`;
        walletMsg += `Use \`/deposit\` to add funds, \`/withdraw\` to cash out\\.`; // Escaped .

        await safeSendMessage(chatId, walletMsg, { parse_mode: 'MarkdownV2' });
    }
}

/** Handles /referral command */
async function handleReferralCommand(msg, args) {
    const chatId = msg.chat.id;
    const userId = String(msg.from.id);
    console.log(`[Cmd /referral User ${userId}] Handling referral command.`); // [LOGGING ADDED] Log command
    const userDetails = await getUserWalletDetails(userId);
    // Escaped Messages
    if (!userDetails?.external_withdrawal_address) {
        await safeSendMessage(chatId, `❌ You need to link your wallet first using \`/wallet <YourSolAddress>\` before using the referral system\\. This ensures rewards can be paid out\\.`, { parse_mode: 'MarkdownV2' }); return; }
    if (!userDetails.referral_code) {
        // Attempt to generate/assign if missing (similar logic to /wallet)
         console.warn(`[Cmd /referral User ${userId}] User has linked wallet but missing referral code. Attempting generation via wallet command logic.`);
         // Trigger /wallet logic internally to try and fix this - maybe refactor later
         await handleWalletCommand(msg, ''); // Call /wallet view logic which includes generation attempt
         return; // Exit, /wallet will display updated info or error
         // await safeSendMessage(chatId, `❌ Could not retrieve or assign your referral code\\. Please try linking your wallet again with \`/wallet <address>\`\\.`, { parse_mode: 'MarkdownV2' }); return;
    }

    const totalEarningsLamports = await getTotalReferralEarnings(userId);
    const totalEarningsSOL = (Number(totalEarningsLamports) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
    let botUsername = 'YourBotUsername'; try { const me = await bot.getMe(); if (me.username) { botUsername = me.username; }} catch (err) { console.warn("Failed to get bot username for referral link:", err.message) }
    const referralLink = `https://t\\.me/${botUsername}?start=${userDetails.referral_code}`; // Escape .
    const minBetSOL = (Number(REFERRAL_INITIAL_BET_MIN_LAMPORTS)/LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
    const milestonePercent = (REFERRAL_MILESTONE_REWARD_PERCENT * 100).toFixed(2);
    let tiersText = REFERRAL_INITIAL_BONUS_TIERS.map(tier => { /* ... (tier text generation as before) ... */
        let countLabel = '';
        const tierIndex = REFERRAL_INITIAL_BONUS_TIERS.indexOf(tier);
        if (tier.maxCount === Infinity) {
            const prevMax = tierIndex > 0 ? REFERRAL_INITIAL_BONUS_TIERS[tierIndex - 1].maxCount : 0;
            countLabel = `${prevMax + 1}\\+`; // e.g. 101+
        } else {
            const lowerBound = tierIndex === 0 ? 1 : REFERRAL_INITIAL_BONUS_TIERS[tierIndex - 1].maxCount + 1;
            countLabel = `${lowerBound}\\-${tier.maxCount}`; // e.g. 1-10, 11-25
        }
        return `     \\- ${escapeMarkdownV2(countLabel)} refs: ${escapeMarkdownV2((tier.percent * 100).toFixed(0))}\\%`; // Escape - %
       }).join('\n');

    // Escaped Multi-line Message
    let referralMsg = `*Your Referral Stats* 📊\n\n` +
                       `*Your Code:* \`${escapeMarkdownV2(userDetails.referral_code)}\`\n` +
                       `*Your Referral Link \\(Share this\\!\\):*\n`+ // Escaped ( ! )
                       `\`${escapeMarkdownV2(referralLink)}\`\n\n` + // Link itself is escaped
                       `*Successful Referrals:* ${escapeMarkdownV2(userDetails.referral_count)}\n` +
                       `*Total Referral Earnings Paid:* ${escapeMarkdownV2(totalEarningsSOL)} SOL\n\n` +
                       `*How Rewards Work:*\n` +
                       `1\\. *Initial Bonus:* Earn a tiered percentage of your referral's *first* completed bet \\(min ${escapeMarkdownV2(minBetSOL)} SOL wager\\)\\.\n` + // Escaped . ( ) .
                       `2\\. *Milestone Bonus:* Earn ${escapeMarkdownV2(milestonePercent)}\\% of the total amount wagered by your referrals as they hit wagering milestones \\(e\\.g\\., 1, 5, 10 SOL etc\\.\\)\\.\n\n`+ // Escaped . % ( ) . .
                       `*Your Initial Bonus Tier \\(based on your ref count *before* their first bet\\):*\n` + // Escaped ( )
                       `${tiersText}\n\n` + // Already escaped in generation loop
                       `Payouts are sent automatically to your linked wallet: \`${escapeMarkdownV2(userDetails.external_withdrawal_address)}\``; // Escaped .

    await safeSendMessage(chatId, referralMsg, { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}


/** Handles /deposit command */
async function handleDepositCommand(msg, args) {
    const chatId = msg.chat.id;
    const userId = String(msg.from.id);
    const logPrefix = `[DepositCmd User ${userId}]`;
    console.log(`${logPrefix} Handling deposit command.`); // [LOGGING ADDED] Entry log

    try {
        // 1. Ensure user exists (also warms up DB connection potentially)
        console.log(`${logPrefix} Ensuring user exists in DB...`); // [LOGGING ADDED] Step log
        await getUserBalance(userId); // Ensures user exists via internal call
        console.log(`${logPrefix} User exists. Fetching next address index...`); // [LOGGING ADDED] Step log

        // 2. Get next index
        const addressIndex = await getNextDepositAddressIndex(userId); // Uses function moved to Part 2
        console.log(`${logPrefix} Next deposit index determined: ${addressIndex}.`); // [LOGGING ADDED] Step log

        // 3. Generate unique address (Function in Part 3 now uses safeIndex)
        console.log(`${logPrefix} Generating unique deposit address...`); // [LOGGING ADDED] Step log
        const derivedInfo = await generateUniqueDepositAddress(userId, addressIndex);
        if (!derivedInfo) {
            // Specific error logged within generateUniqueDepositAddress
            throw new Error("Failed to generate a unique deposit address. See previous logs for details.");
        }
        const depositAddress = derivedInfo.publicKey.toBase58();
        const derivationPath = derivedInfo.derivationPath; // Keep path for record
        console.log(`${logPrefix} Deposit address generated: ${depositAddress} (Path: ${derivationPath})`); // [LOGGING ADDED] Step success log

        // 4. Calculate expiry
        const expiresAt = new Date(Date.now() + DEPOSIT_ADDRESS_EXPIRY_MS);
        console.log(`${logPrefix} Address expiry set to: ${expiresAt.toISOString()}`); // [LOGGING ADDED] Step log

        // 5. Create DB record for the address (Function in Part 2 now contains detailed logs)
        console.log(`${logPrefix} Creating deposit address record in DB...`); // [LOGGING ADDED] Step log
        const recordResult = await createDepositAddressRecord(userId, depositAddress, derivationPath, expiresAt);
        if (!recordResult.success || !recordResult.id) {
            // Specific error logged within createDepositAddressRecord
            throw new Error(recordResult.error || "Failed to save deposit address record to database.");
        }
        console.log(`${logPrefix} Deposit address record created successfully (ID: ${recordResult.id}).`); // [LOGGING ADDED] Step success log

        // 6. Send message to user
        // Escaped Message
        const expiryMinutes = Math.round(DEPOSIT_ADDRESS_EXPIRY_MS / (60 * 1000));
        const message = `💰 *Deposit SOL*\n\n` +
                         `To add funds to your bot balance, send SOL to this unique address:\n\n` +
                         `\`${escapeMarkdownV2(depositAddress)}\`\n\n` + // Address escaped
                         `⚠️ *Important:*\n` +
                         `1\\. This address is temporary and only valid for *one* deposit\\. It expires in *${escapeMarkdownV2(expiryMinutes)} minutes*\\. \n` + // Escaped . (x3)
                         `2\\. Do *not* send multiple times to this address\\. Use \`/deposit\` again for new deposits\\. \n` + // Escaped . (x2)
                         `3\\. Deposits typically require *${escapeMarkdownV2(DEPOSIT_CONFIRMATION_LEVEL)}* network confirmations to reflect in your /wallet balance\\.`; // Escaped .

        console.log(`${logPrefix} Sending deposit instructions to user.`); // [LOGGING ADDED] Step log
        await safeSendMessage(chatId, message, { parse_mode: 'MarkdownV2' });
        console.log(`${logPrefix} Deposit command handled successfully.`); // [LOGGING ADDED] Final success log

    } catch (error) {
        console.error(`${logPrefix} Error: ${error.message}`); // Log the specific error that caused the failure
         // Notify Admin
         await notifyAdmin(`🚨 ERROR during /deposit for User ${userId} (${logPrefix}):\n${error.message}`);
        // Escaped Message
        await safeSendMessage(chatId, `❌ Error generating deposit address: ${escapeMarkdownV2(error.message)}\\. Please try again later or contact support\\.`, { parse_mode: 'MarkdownV2' });
    }
}

/** Handles /withdraw command */
async function handleWithdrawCommand(msg, args) {
    const chatId = msg.chat.id;
    const userId = String(msg.from.id);
    const logPrefix = `[WithdrawCmd User ${userId}]`;
    console.log(`${logPrefix} Handling withdraw command.`); // [LOGGING ADDED] Log command

    try {
        const linkedAddress = await getLinkedWallet(userId);
        if (!linkedAddress) {
             // Escaped Message
             await safeSendMessage(chatId, `⚠️ You must set your external withdrawal address first using \`/wallet <YourSolanaAddress>\`\\. Rewards and withdrawals are sent there\\.`, { parse_mode: 'MarkdownV2' });
             return;
        }
        const currentBalance = await getUserBalance(userId);
        const minWithdrawTotal = MIN_WITHDRAWAL_LAMPORTS + WITHDRAWAL_FEE_LAMPORTS;
        if (currentBalance < minWithdrawTotal) {
             const minNeeded = (Number(minWithdrawTotal) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
              // Escaped Message
              await safeSendMessage(chatId, `⚠️ Your balance \\(${escapeMarkdownV2((Number(currentBalance)/LAMPORTS_PER_SOL).toFixed(4))} SOL\\) is below the minimum required to cover the withdrawal amount \\+ fee \\(min ${escapeMarkdownV2(minNeeded)} SOL total\\)\\.`, { parse_mode: 'MarkdownV2'});
             return;
        }
        // Set user state to expect amount input
        userStateCache.set(userId, { state: 'awaiting_withdrawal_amount', chatId: String(chatId), messageId: msg.message_id, timestamp: Date.now(), data: {} });
         // Escaped Message
         await safeSendMessage(chatId, `💸 Your withdrawal address: \`${escapeMarkdownV2(linkedAddress)}\`\n` +
                                       `Minimum withdrawal: ${escapeMarkdownV2((Number(MIN_WITHDRAWAL_LAMPORTS)/LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL\\. Fee: ${escapeMarkdownV2((Number(WITHDRAWAL_FEE_LAMPORTS)/LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL\\. \n\n` + // Escaped . (x2) :
                                       `Please enter the amount of SOL you wish to withdraw:`, // Escaped :
                                       { parse_mode: 'MarkdownV2' });
    } catch (error) {
        console.error(`${logPrefix} Error starting withdrawal: ${error.message}`);
         // Notify Admin
         await notifyAdmin(`🚨 ERROR starting /withdraw for User ${userId} (${logPrefix}):\n${error.message}`);
         // Escaped Message
         await safeSendMessage(chatId, `❌ Error starting withdrawal process\\. Please try again later\\.`, { parse_mode: 'MarkdownV2' });
    }
}


// --- Game Command Handlers (Initiate Input Flows) ---

async function handleCoinflipCommand(msg, args) {
    const chatId = msg.chat.id; const userId = String(msg.from.id);
    console.log(`[Cmd /coinflip User ${userId}] Handling coinflip command.`); // [LOGGING ADDED] Log command
    userStateCache.set(userId, { state: 'awaiting_cf_amount', chatId: String(chatId), messageId: msg.message_id, timestamp: Date.now(), data: {} });
    // Escaped Message
    await safeSendMessage(chatId, `🪙 Enter Coinflip bet amount \\(SOL\\):`, { parse_mode: 'MarkdownV2' }); // Escaped ( ) :
}
async function handleRaceCommand(msg, args) {
     const chatId = msg.chat.id; const userId = String(msg.from.id);
     console.log(`[Cmd /race User ${userId}] Handling race command.`); // [LOGGING ADDED] Log command
     userStateCache.set(userId, { state: 'awaiting_race_amount', chatId: String(chatId), messageId: msg.message_id, timestamp: Date.now(), data: {} });
     // Escaped Message
     await safeSendMessage(chatId, `🐎 Enter Race bet amount \\(SOL\\):`, { parse_mode: 'MarkdownV2' }); // Escaped ( ) :
}
async function handleSlotsCommand(msg, args) {
    const chatId = msg.chat.id; const userId = String(msg.from.id);
    console.log(`[Cmd /slots User ${userId}] Handling slots command.`); // [LOGGING ADDED] Log command
    userStateCache.set(userId, { state: 'awaiting_slots_amount', chatId: String(chatId), messageId: msg.message_id, timestamp: Date.now(), data: {} });
     // Escaped Message
     await safeSendMessage(chatId, `🎰 Enter Slots bet amount \\(SOL\\):`, { parse_mode: 'MarkdownV2' }); // Escaped ( ) :
}
async function handleRouletteCommand(msg, args) {
    const chatId = msg.chat.id; const userId = String(msg.from.id);
    console.log(`[Cmd /roulette User ${userId}] Handling roulette command.`); // [LOGGING ADDED] Log command
     // Escaped Message
     await safeSendMessage(chatId, `⚪️ Roulette betting via text is limited\\. A better interface is planned\\. Use /help for now\\.`, { parse_mode: 'MarkdownV2'});
}
async function handleWarCommand(msg, args) {
    const chatId = msg.chat.id; const userId = String(msg.from.id);
    console.log(`[Cmd /war User ${userId}] Handling war command.`); // [LOGGING ADDED] Log command
    userStateCache.set(userId, { state: 'awaiting_war_amount', chatId: String(chatId), messageId: msg.message_id, timestamp: Date.now(), data: {} });
     // Escaped Message
     await safeSendMessage(chatId, `🃏 Enter Casino War bet amount \\(SOL\\):`, { parse_mode: 'MarkdownV2' }); // Escaped ( ) :
}

// --- Command Handler Map Definition (used in handleMessage) ---
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
};

// --- Menu Command Handler Map Definition (used in handleCallbackQuery) ---
const menuCommandHandlers = {
    'coinflip': handleCoinflipCommand, 'race': handleRaceCommand, 'slots': handleSlotsCommand,
    'roulette': handleRouletteCommand, 'war': handleWarCommand, 'deposit': handleDepositCommand,
    'withdraw': handleWithdrawCommand, 'wallet': handleWalletCommand, 'referral': handleReferralCommand,
    'help': handleHelpCommand,
};


// --- Admin Command --- (Basic structure, needs detailed implementation based on requirements)
async function handleAdminCommand(msg, args) {
    const chatId = msg.chat.id; const userId = String(msg.from.id);
    const logPrefix = `[AdminCmd User ${userId}]`;
    const subCommand = args?.split(' ')[0]?.toLowerCase();
    const subArgs = args?.split(' ').slice(1) || [];

    if (!subCommand) {
         // Escaped Usage Message
         await safeSendMessage(chatId, "Admin Usage: `/admin <command> [args]`\nCommands: `status`, `getbalance`, `getconfig`, `setloglevel <level>`, `finduser <id\\_or\\_ref>`", { parse_mode: 'MarkdownV2' });
        return;
    }
    console.log(`${logPrefix} Executing: ${subCommand} ${subArgs.join(' ')}`);

    try {
        switch (subCommand) {
            case 'status': { /* ... status logic ... */
                 const msgQueueSize = messageQueue.size; const cbQueueSize = callbackQueue.size; const payoutQueueSize = payoutProcessorQueue.size;
                 const depositQSize = depositProcessorQueue.size; const tgSendQSize = telegramSendQueue.size; const walletCacheSize = walletCache.size;
                 const depAddrCacheSize = activeDepositAddresses.size; const processedTxCacheSize = processedDepositTxSignatures.size;
                 const dbPoolTotal = pool.totalCount; const dbPoolIdle = pool.idleCount; const dbPoolWaiting = pool.waitingCount;
                 // Escaped Status Message
                 let statusMsg = `*Bot Status* \\(v3\\.1\\.5\\) \\- ${escapeMarkdownV2(new Date().toISOString())}\n\n` + // *** Updated Version ***
                                `*Queues*:\n` +
                                `  Msg: ${msgQueueSize}, Callback: ${cbQueueSize}, Payout: ${payoutQueueSize}, Deposit: ${depositQSize}, TG Send: ${tgSendQSize}\n`+
                                `*Caches*:\n` +
                                `  User States: ${userStateCache.size}, Wallets: ${walletCacheSize}, Dep Addr: ${depAddrCacheSize}, Proc TXs: ${processedTxCacheSize}/${MAX_PROCESSED_TX_CACHE_SIZE}\n`+
                                `*DB Pool*:\n` +
                                `  Total: ${dbPoolTotal}, Idle: ${dbPoolIdle}, Waiting: ${dbPoolWaiting}\n` +
                                `*State*: Initialized: ${isFullyInitialized}\n`;
                 await safeSendMessage(chatId, statusMsg, { parse_mode: 'MarkdownV2' }); break;
               }
            case 'getbalance': { /* ... getbalance logic ... */
                 const keyTypes = ['main', 'referral']; let balanceMsg = '*Payout Wallet Balances:*\n';
                 for (const type of keyTypes) { /* ... key selection logic ... */
                      let keyEnvVar = ''; let keyDesc = '';
                      if (type === 'main') { keyEnvVar = 'MAIN_BOT_PRIVATE_KEY'; keyDesc = 'Main';}
                      else if (type === 'referral') { keyEnvVar = process.env.REFERRAL_PAYOUT_PRIVATE_KEY ? 'REFERRAL_PAYOUT_PRIVATE_KEY' : 'MAIN_BOT_PRIVATE_KEY'; keyDesc = `Referral \\(${escapeMarkdownV2(process.env.REFERRAL_PAYOUT_PRIVATE_KEY ? 'Dedicated' : 'Using Main')}\\)`;} // Escape ()
                      const pk = process.env[keyEnvVar];
                      if (pk) { try { const keypair = Keypair.fromSecretKey(bs58.decode(pk)); const balance = await solanaConnection.getBalance(keypair.publicKey);
                           balanceMsg += `   \\- ${escapeMarkdownV2(keyDesc)} \\(${escapeMarkdownV2(keypair.publicKey.toBase58().slice(0,6))}\\.\\.\\): ${escapeMarkdownV2((Number(balance)/LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL\n`; // Escape -, ., (, )
                         } catch (e) { balanceMsg += `   \\- ${escapeMarkdownV2(keyDesc)}: Error fetching balance \\- ${escapeMarkdownV2(e.message)}\n`; } // Escape - (x2)
                      } else { balanceMsg += `   \\- ${escapeMarkdownV2(keyDesc)}: Private key not set\n`; } // Escape -
                 } await safeSendMessage(chatId, balanceMsg, { parse_mode: 'MarkdownV2' }); break;
               }
            case 'getconfig': { /* ... getconfig logic ... */
                 let configText = "*Current Config \\(Non\\-Secret Env Vars\\):*\n\n"; // Escaped ()-
                 const sensitiveKeywords = ['TOKEN', 'DATABASE_URL', 'KEY', 'SECRET', 'PASSWORD', 'SEED'];
                 const allVars = {...OPTIONAL_ENV_DEFAULTS, ...process.env}; // Combine defaults and actual env
                 // Add required vars to the list we iterate over
                 const keysToShow = [...new Set([...REQUIRED_ENV_VARS, ...Object.keys(OPTIONAL_ENV_DEFAULTS)])];

                 for (const key of keysToShow.sort()) { // Sort keys alphabetically
                      // Only show keys relevant to this config setup
                      if (REQUIRED_ENV_VARS.includes(key) || OPTIONAL_ENV_DEFAULTS.hasOwnProperty(key)) {
                           const isSensitive = sensitiveKeywords.some(k => key.toUpperCase().includes(k));
                           let value = process.env[key];
                           let source = ''; // Track if default or set
                           let displayValue = '';

                           if (value !== undefined) {
                               // Value is explicitly set in environment
                               displayValue = isSensitive && key !== 'RPC_URLS' ? '[Set, Redacted]' : escapeMarkdownV2(value);
                               if (OPTIONAL_ENV_DEFAULTS.hasOwnProperty(key) && value === OPTIONAL_ENV_DEFAULTS[key]) {
                                    source = ' \\(Matches Default\\)'; // Escaped ()
                               } else {
                                    source = ' \\(Set\\)'; // Escaped ()
                               }
                           } else if (OPTIONAL_ENV_DEFAULTS.hasOwnProperty(key)) {
                               // Value is not set, using default
                               value = OPTIONAL_ENV_DEFAULTS[key]; // Get default value
                               displayValue = isSensitive && key !== 'RPC_URLS' ? '[Default, Redacted]' : escapeMarkdownV2(value);
                               source = ' \\(Default\\)'; // Escaped ()
                           } else {
                                // Required variable that is somehow missing (shouldn't happen after startup checks)
                                displayValue = '[MISSING REQUIRED]';
                                source = ' \\(ERROR\\)'; // Escaped ()
                           }
                           configText += `\\- ${escapeMarkdownV2(key)}: ${displayValue}${source}\n`; // Escape -
                       }
                 }
                 configText += "\n*Game Config Limits \\(Lamports\\):*\n" + "```json\n" + JSON.stringify(GAME_CONFIG, (k, v) => typeof v === 'bigint' ? v.toString() : v, 2) + "\n```"; // Use JSON code block
                 await safeSendMessage(chatId, configText.substring(0, 4090), { parse_mode: 'MarkdownV2' }); break; // Substring to avoid length limits
               }
            case 'setloglevel': // Add basic implementation example if needed
            case 'finduser':
                 // Escaped Message
                 await safeSendMessage(chatId, `Admin command \`${escapeMarkdownV2(subCommand)}\` not fully implemented yet\\.`, { parse_mode: 'MarkdownV2'}); break;
            default:
                 // Escaped Message
                 await safeSendMessage(chatId, `Unknown admin command: \`${escapeMarkdownV2(subCommand)}\``, { parse_mode: 'MarkdownV2' });
        }
    } catch (adminError) {
        console.error(`${logPrefix} Admin command error (${subCommand}):`, adminError);
         // Notify Admin about the admin command failure itself
         await notifyAdmin(`🚨 ERROR executing Admin Command \`${subCommand}\` for User ${userId}:\n${adminError.message}`);
         // Escaped Message
         await safeSendMessage(chatId, `Error executing admin command \`${escapeMarkdownV2(subCommand)}\`: \`${escapeMarkdownV2(adminError.message)}\``, { parse_mode: 'MarkdownV2' });
    }
}


// --- Referral Check & Payout Triggering Logic ---
// This function remains unchanged from your original code in terms of logic flow
/** Handles checking for and processing referral bonuses after a bet completes. */
async function _handleReferralChecks(refereeUserId, completedBetId, wagerAmountLamports, logPrefix = '') {
     refereeUserId = String(refereeUserId);
     logPrefix = logPrefix || `[ReferralCheck Bet ${completedBetId} User ${refereeUserId}]`;
     // console.log(`${logPrefix} Starting referral checks.`); // Less verbose logging

     let client = null;
     try {
          const refereeDetails = await getUserWalletDetails(refereeUserId);
          if (!refereeDetails?.referred_by_user_id) { /* ... (handle non-referred + update stats) ... */
               // console.log(`${logPrefix} Referee is not referred. Updating wager stats only.`); // Less verbose
                client = await pool.connect(); await client.query('BEGIN');
                await updateUserWagerStats(refereeUserId, wagerAmountLamports, client);
                await client.query('COMMIT'); return;
           }
          const referrerUserId = refereeDetails.referred_by_user_id;
          const referrerDetails = await getUserWalletDetails(referrerUserId);
          if (!referrerDetails?.external_withdrawal_address) { /* ... (handle referrer no wallet + update stats) ... */
               console.warn(`${logPrefix} Referrer ${referrerUserId} has no linked wallet. Cannot process potential payouts.`);
                client = await pool.connect(); await client.query('BEGIN');
                await updateUserWagerStats(refereeUserId, wagerAmountLamports, client); // Still update referee stats
                await client.query('COMMIT'); return;
           }

          client = await pool.connect(); await client.query('BEGIN'); // Start TX
          let newLastMilestonePaid = refereeDetails.last_milestone_paid_lamports;
          let payoutQueued = false; // Flag to track if any payout was queued

          // Check Initial Bet Bonus
          const isFirst = await isFirstCompletedBet(refereeUserId, completedBetId);
          if (isFirst && wagerAmountLamports >= REFERRAL_INITIAL_BET_MIN_LAMPORTS) { /* ... (calculate bonus, record, queue) ... */
                console.log(`${logPrefix} Referee's first qualifying bet detected. Calculating initial bonus...`);
                const referrerCount = referrerDetails.referral_count || 0; // Get count *before* this referral (if linking logic is separate)
                let bonusTier = REFERRAL_INITIAL_BONUS_TIERS[REFERRAL_INITIAL_BONUS_TIERS.length - 1]; // Default to highest
                for (const tier of REFERRAL_INITIAL_BONUS_TIERS) { if (referrerCount <= tier.maxCount) { bonusTier = tier; break; } }
                const initialBonusAmount = wagerAmountLamports * BigInt(Math.round(bonusTier.percent * 100)) / 100n;
                console.log(`${logPrefix} Referrer Count: ${referrerCount}, Tier Percent: ${bonusTier.percent}, Bonus Amount: ${initialBonusAmount}`);
                if (initialBonusAmount > 0n) {
                     // Pass client to recordPendingReferralPayout
                     const payoutRecord = await recordPendingReferralPayout(referrerUserId, refereeUserId, 'initial_bet', initialBonusAmount, completedBetId, null, client);
                     if (payoutRecord.success && payoutRecord.payoutId) {
                          await addPayoutJob({ type: 'payout_referral', payoutId: payoutRecord.payoutId, userId: referrerUserId }, parseInt(process.env.PAYOUT_JOB_RETRIES, 10), parseInt(process.env.PAYOUT_JOB_RETRY_DELAY_MS, 10));
                          console.log(`${logPrefix} Initial bonus payout job queued (ID: ${payoutRecord.payoutId}).`);
                          payoutQueued = true;
                     } else if (payoutRecord.error !== 'Duplicate milestone payout attempt.') {
                          console.error(`${logPrefix} Failed to record initial bonus payout: ${payoutRecord.error}`);
                          // Notify admin about DB failure during bonus recording
                          await notifyAdmin(`🚨 ERROR recording Initial Bonus Payout in DB for Referrer ${referrerUserId} / Referee ${refereeUserId} (${logPrefix}). Error: ${payoutRecord.error}`);
                     }
                } else { console.log(`${logPrefix} Calculated initial bonus is zero.`); }
           }

          // Check Milestone Bonus
          const newTotalWagered = (refereeDetails.total_wagered || 0n) + wagerAmountLamports;
          let milestoneCrossed = null;
          // Iterate thresholds in ascending order
          for (const threshold of REFERRAL_MILESTONE_THRESHOLDS_LAMPORTS) {
                if (newTotalWagered >= threshold && threshold > refereeDetails.last_milestone_paid_lamports) {
                     milestoneCrossed = threshold; // Keep track of the highest threshold crossed
                } else if (newTotalWagered < threshold) {
                     break; // Stop checking once total wagered is less than the threshold
                }
           }

          if (milestoneCrossed) { /* ... (calculate bonus, record, queue) ... */
               console.log(`${logPrefix} Milestone threshold ${milestoneCrossed} crossed (Previous: ${refereeDetails.last_milestone_paid_lamports}, New Total: ${newTotalWagered}). Calculating bonus...`);
                const milestoneBonusAmount = milestoneCrossed * BigInt(Math.round(REFERRAL_MILESTONE_REWARD_PERCENT * 10000)) / 10000n;
                console.log(`${logPrefix} Milestone Bonus Amount: ${milestoneBonusAmount}`);
                if (milestoneBonusAmount > 0n) {
                     // Pass client to recordPendingReferralPayout
                     const payoutRecord = await recordPendingReferralPayout(referrerUserId, refereeUserId, 'milestone', milestoneBonusAmount, null, milestoneCrossed, client);
                     if (payoutRecord.success && payoutRecord.payoutId) {
                          await addPayoutJob({ type: 'payout_referral', payoutId: payoutRecord.payoutId, userId: referrerUserId }, parseInt(process.env.PAYOUT_JOB_RETRIES, 10), parseInt(process.env.PAYOUT_JOB_RETRY_DELAY_MS, 10));
                          console.log(`${logPrefix} Milestone bonus payout job queued (ID: ${payoutRecord.payoutId}).`);
                          newLastMilestonePaid = milestoneCrossed; // Update marker *only if* payout recorded successfully
                          payoutQueued = true;
                     } else if (payoutRecord.error !== 'Duplicate milestone payout attempt.') {
                          console.error(`${logPrefix} Failed to record milestone bonus payout: ${payoutRecord.error}`);
                           // Notify admin about DB failure during bonus recording
                           await notifyAdmin(`🚨 ERROR recording Milestone Bonus Payout in DB for Referrer ${referrerUserId} / Referee ${refereeUserId} / Milestone ${milestoneCrossed} (${logPrefix}). Error: ${payoutRecord.error}`);
                     }
                } else { console.log(`${logPrefix} Calculated milestone bonus is zero.`); }
           }

          // Update Referee Wager Stats (Commit or Rollback)
          // Always update wager stats now, passing the potentially updated milestone marker
           console.log(`${logPrefix} Updating referee wager stats. New Milestone Paid Level: ${newLastMilestonePaid}`);
           const statsUpdated = await updateUserWagerStats(refereeUserId, wagerAmountLamports, client, newLastMilestonePaid);
           if (!statsUpdated) {
                console.error(`${logPrefix} CRITICAL: Failed to update referee wager stats. Rolling back.`);
                // Notify admin! This shouldn't fail if user exists.
                await notifyAdmin(`🚨 CRITICAL ERROR updating referee wager stats for User ${refereeUserId} (${logPrefix}) after Bet ${completedBetId}. Rolling back.`);
                await client.query('ROLLBACK');
           } else {
                await client.query('COMMIT');
                console.log(`${logPrefix} Referral checks completed. Transaction committed.`);
           }

     } catch (error) { /* ... (error logging and rollback) ... */
          console.error(`${logPrefix} Error during referral checks: ${error.message}`);
          // Notify admin for unexpected errors during check
          await notifyAdmin(`🚨 UNEXPECTED ERROR during _handleReferralChecks for User ${refereeUserId} / Bet ${completedBetId} (${logPrefix}):\n${error.message}\nStack: ${error.stack?.substring(0, 500)}`);
         if (client) { try { await client.query('ROLLBACK'); console.log(`${logPrefix} Transaction rolled back due to error.`); } catch (rbErr) { console.error(`${logPrefix} Rollback failed: ${rbErr.message}`);} }
     } finally { if (client) client.release(); }
}


// --- End of Part 5b / End of Part 5 ---

// Export functions if needed by other potential modules (e.g., for testing)
// Depending on your structure, you might export handleMessage, handleCallbackQuery etc.
// export { handleMessage, handleCallbackQuery /* ... other needed // index.js - Part 6: Background Tasks, Payouts, Startup & Shutdown (VERIFIED)
// --- VERSION: 3.1.5 --- (Includes Sweeping Task with BigInt/Rent fix and delay. Verified initializeDatabase calls.)

// --- Assuming necessary imports from other parts ---
// import { Keypair, PublicKey, SystemProgram, Transaction, sendAndConfirmTransaction, LAMPORTS_PER_SOL } from '@solana/web3.js';
// import bs58 from 'bs58';
// import { pool } from './db';
// import { bot } from './bot';
// import { solanaConnection } from './connection';
// import { payoutProcessorQueue, depositProcessorQueue, messageQueue, callbackQueue, telegramSendQueue } from './queues';
// import { isFullyInitialized, depositMonitorIntervalId, server, PENDING_REFERRAL_TTL_MS, pendingReferrals } from './state'; // Added pendingReferrals, PENDING_REFERRAL_TTL_MS assumption
// import { queryDatabase, getWithdrawalDetails, updateWithdrawalStatus, updateReferralPayoutStatus, getReferralPayoutDetails, getLinkedWallet, ensureUserExists, updateUserBalanceAndLedger, recordConfirmedDeposit, markDepositAddressUsed, linkReferral, generateReferralCode, updateUserWagerStats } from './dbOps'; // Added more DB Ops used here
// import { getKeypairFromPath, isRetryableSolanaError, notifyAdmin, safeSendMessage, escapeMarkdownV2, addProcessedDepositTx, hasProcessedDepositTx, findDepositAddressInfo, analyzeTransactionAmounts, getUserDisplayName } from './utils'; // Added more Utils used here
// import { processDepositTransaction } from './depositHandler'; // Assuming definition exists
// ------------------------------------------------------

// --- Global variable for sweep interval ID ---
let sweepIntervalId = null;
const SWEEP_INTERVAL_MS_DEFAULT = 15 * 60 * 1000; // Default: 15 minutes
const SWEEP_BATCH_SIZE = 20; // How many addresses to attempt sweeping per cycle
const SWEEP_ADDRESS_DELAY_MS = 1000; // Delay in milliseconds between sweeping each address

// Helper function for delays (ensure this is defined, perhaps in Part 3 or here)
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));


// --- Payout Job Handling ---

/**
 * Adds a job to the payout queue with retry logic.
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
                    await notifyAdmin(`🚨 CRITICAL: Unknown payout job type encountered: ${jobType} for Job ID ${jobId}.`);
                    return;
                }
                console.log(`${logPrefix} Attempt ${attempt + 1} successful.`);
                return;

            } catch (error) {
                attempt++;
                const isRetryable = error.isRetryable !== undefined ? error.isRetryable : isRetryableSolanaError(error);
                console.warn(`${logPrefix} Attempt ${attempt}/${maxRetries + 1} failed. Retryable: ${isRetryable}. Error: ${error.message}`);

                if (!isRetryable || attempt > maxRetries) {
                    console.error(`${logPrefix} Final attempt failed or error not retryable. Marking job as failed.`);
                    await notifyAdmin(`🚨 PAYOUT FAILED (Final): ${jobType} ID ${jobId}. User: ${jobDetails.userId}. Reason: Max retries or non-retryable error - ${error.message}`);
                    try {
                        const failureReason = `Max retries (${maxRetries}) exceeded or non-retryable error: ${error.message.substring(0, 200)}`;
                        if (jobType === 'payout_withdrawal' && jobDetails.withdrawalId) {
                            await updateWithdrawalStatus(jobDetails.withdrawalId, 'failed', null, null, failureReason);
                        } else if (jobType === 'payout_referral' && jobDetails.payoutId) {
                            await updateReferralPayoutStatus(jobDetails.payoutId, 'failed', null, null, failureReason);
                        }
                        console.log(`${logPrefix} Marked job as failed in DB.`);
                    } catch (dbError) {
                        console.error(`${logPrefix} CRITICAL: Failed to mark job as failed in DB after final error: ${dbError.message}`);
                        await notifyAdmin(`🚨 CRITICAL DB ERROR: Failed to mark ${jobType} ID ${jobId} as FAILED in DB after payout failure. User: ${jobDetails.userId}. DB Error: ${dbError.message}`);
                    }
                    if (jobDetails.userId) {
                        const errorType = jobType === 'payout_withdrawal' ? 'Withdrawal' : 'Referral Payout';
                        await safeSendMessage(
                            jobDetails.userId,
                            `⚠️ Your ${escapeMarkdownV2(errorType)} \\(ID: ${escapeMarkdownV2(jobId)}\\) could not be completed after multiple attempts\\. Please contact support\\. Error: ${escapeMarkdownV2(error.message)}`,
                            { parse_mode: 'MarkdownV2' }
                        ).catch(e => console.error(`${logPrefix} Failed to send failure notification to user ${jobDetails.userId}: ${e.message}`));
                    }
                    break;
                }

                const jitter = baseRetryDelayMs * 0.1 * (Math.random() - 0.5);
                const delay = (baseRetryDelayMs * Math.pow(2, attempt - 1)) + jitter;
                console.log(`${logPrefix} Retrying in ${Math.round(delay / 1000)}s...`);
                await sleep(delay);
            }
        }
    }).catch(async (queueError) => {
        console.error(`${logPrefix} Error adding/processing job in payout queue:`, queueError.message);
        await notifyAdmin(`🚨 CRITICAL QUEUE ERROR for ${jobType} ID ${jobId}. Error adding to queue: ${queueError.message}`);
        try {
            const queueFailureReason = `Queue processing error: ${queueError.message.substring(0, 200)}`;
            if (jobType === 'payout_withdrawal' && jobDetails.withdrawalId) await updateWithdrawalStatus(jobDetails.withdrawalId, 'failed', null, null, queueFailureReason);
            else if (jobType === 'payout_referral' && jobDetails.payoutId) await updateReferralPayoutStatus(jobDetails.payoutId, 'failed', null, null, queueFailureReason);
        } catch (dbError) {
            console.error(`${logPrefix} Failed to mark job as failed after queue error: ${dbError.message}`);
            await notifyAdmin(`🚨 CRITICAL DB ERROR: Failed to mark ${jobType} ID ${jobId} as FAILED after QUEUE error. DB Error: ${dbError.message}`);
        }
    });
}

/** Handles withdrawal payout jobs. Throws error on failure for retry logic. */
async function handleWithdrawalPayoutJob(job) {
    const { withdrawalId, userId } = job;
    const logPrefix = `[WithdrawalJob ID:${withdrawalId}]`;
    let client = null;

    console.log(`${logPrefix} Starting payout processing...`);
    const details = await getWithdrawalDetails(withdrawalId);

    if (!details) {
        console.error(`${logPrefix} Withdrawal record not found.`);
        await notifyAdmin(`🚨 CRITICAL ERROR: Withdrawal record ID ${withdrawalId} not found during payout job processing.`);
        throw new Error("Withdrawal record not found. Not retryable.");
    }
    if (details.status !== 'pending' && details.status !== 'processing') {
        console.warn(`${logPrefix} Job started but status is already '${details.status}'. Skipping.`);
        return;
    }

    console.log(`${logPrefix} Marking withdrawal as 'processing' in DB.`);
    const processingMarked = await updateWithdrawalStatus(withdrawalId, 'processing');
    if (!processingMarked) {
        console.warn(`${logPrefix} Failed to mark withdrawal as 'processing'. Another process might be handling it or record changed state. Skipping attempt.`);
        return;
    }

    try {
        const recipient = details.recipient_address;
        const amountToSend = details.final_send_amount_lamports;
        const requestedAmount = details.requested_amount_lamports;
        const feeAmount = details.fee_lamports;
        const totalDeduction = requestedAmount + feeAmount;

        console.log(`${logPrefix} Attempting to send ${amountToSend} lamports to ${recipient}...`);
        const sendResult = await sendSol(recipient, amountToSend, 'withdrawal');

        if (sendResult.success && sendResult.signature) {
            const signature = sendResult.signature;
            console.log(`${logPrefix} On-chain send successful: ${signature}. Updating database within transaction...`);

            client = await pool.connect();
            await client.query('BEGIN');
            console.log(`${logPrefix} DB Transaction started for completion.`);

            console.log(`${logPrefix} Deducting ${totalDeduction} from internal balance (User: ${details.user_id})...`);
            const balanceUpdateResult = await updateUserBalanceAndLedger(
                client, details.user_id, -totalDeduction, 'withdrawal', { withdrawalId: withdrawalId },
                `To: ${recipient.slice(0, 6)}... Fee: ${Number(feeAmount) / LAMPORTS_PER_SOL} SOL`
            );

            if (!balanceUpdateResult.success) {
                await client.query('ROLLBACK');
                client.release(); client = null;
                const criticalErrorMsg = `CRITICAL ERROR: Sent ${amountToSend} lamports (TX: ${signature}) but failed to deduct internal balance for user ${details.user_id}. Reason: ${balanceUpdateResult.error}`;
                console.error(`${logPrefix} ${criticalErrorMsg}`);
                await updateWithdrawalStatus(withdrawalId, 'failed', null, null, `CRITICAL: Internal balance deduction failed after successful send (TX: ${signature})`);
                await notifyAdmin(`🚨 ${criticalErrorMsg}`);
                const criticalError = new Error(criticalErrorMsg);
                criticalError.isRetryable = false;
                throw criticalError;
            }
            console.log(`${logPrefix} Internal balance deducted successfully. New balance: ${balanceUpdateResult.newBalance}`);

            console.log(`${logPrefix} Marking withdrawal as 'completed' in DB with TX sig...`);
            const statusUpdated = await updateWithdrawalStatus(withdrawalId, 'completed', client, signature);
            if (!statusUpdated) {
                const criticalErrorMsg = `CRITICAL ERROR: Sent ${amountToSend} lamports (TX: ${signature}), deducted balance for User ${details.user_id}, but failed to mark withdrawal ID ${withdrawalId} as completed.`;
                console.error(`${logPrefix} ${criticalErrorMsg}`);
                await client.query('ROLLBACK');
                console.log(`${logPrefix} Rolled back balance deduction due to failure marking withdrawal complete.`);
                client.release(); client = null;
                await notifyAdmin(`🚨 ${criticalErrorMsg} Needs manual verification!`);
                const updateError = new Error(criticalErrorMsg + " Will retry marking complete.");
                updateError.isRetryable = true;
                throw updateError;
            }
            console.log(`${logPrefix} Withdrawal marked as 'completed' successfully.`);

            console.log(`${logPrefix} Committing DB transaction...`);
            await client.query('COMMIT');
            console.log(`${logPrefix} Successfully processed, balance deducted, record updated. Transaction committed.`);

            const amountSentSOL = (Number(amountToSend) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
            console.log(`${logPrefix} Sending success notification to user ${details.user_id}...`);
            await safeSendMessage(
                details.user_id,
                `✅ Withdrawal complete\\! ${escapeMarkdownV2(amountSentSOL)} SOL sent to \`${escapeMarkdownV2(recipient)}\`\\.\nTX: [View](https://solscan.io/tx/${escapeMarkdownV2(signature)})`,
                { parse_mode: 'MarkdownV2', disable_web_page_preview: true }
            ).catch(e => console.error(`${logPrefix} Failed to send completion notification to user ${details.user_id}: ${e.message}`));

        } else {
            const errorMsg = `sendSol failed. Error: ${sendResult.error}`;
            console.error(`${logPrefix} ${errorMsg}`);
            const sendError = new Error(sendResult.error || "sendSol failed without specific error message");
            if (sendResult.isRetryable !== undefined) { sendError.isRetryable = sendResult.isRetryable; }
            else { sendError.isRetryable = isRetryableSolanaError(sendError); }
            throw sendError;
        }
    } catch (error) {
        if (client) { try { await client.query('ROLLBACK'); console.log(`${logPrefix} Transaction rolled back due to error.`); } catch (rbErr) { console.error(`${logPrefix} Rollback error: ${rbErr.message}`); } }
        console.error(`${logPrefix} Processing failed during execution. Error: ${error.message}`);
        if (error.isRetryable === undefined) {
             error.isRetryable = isRetryableSolanaError(error);
        }
        throw error;
    } finally {
        if (client) client.release();
    }
}

/** Handles referral payout jobs. Throws error on failure for retry logic. */
async function handleReferralPayoutJob(job) {
    const { payoutId, userId } = job;
    const logPrefix = `[RefPayoutJob ID:${payoutId}]`;

    console.log(`${logPrefix} Starting payout processing...`);
    const details = await getReferralPayoutDetails(payoutId);

    if (!details) {
        console.error(`${logPrefix} Referral payout record not found.`);
        await notifyAdmin(`🚨 CRITICAL ERROR: Referral Payout record ID ${payoutId} not found during payout job processing.`);
        throw new Error("Referral payout record not found. Not retryable.");
    }
    if (details.status !== 'pending' && details.status !== 'processing') {
        console.warn(`${logPrefix} Job started but status is already '${details.status}'. Skipping.`);
        return;
    }

    const referrerUserId = details.referrer_user_id;
    if (referrerUserId !== userId) {
        console.error(`${logPrefix} Mismatch between job user ID (${userId}) and record referrer ID (${referrerUserId}). Skipping.`);
        await notifyAdmin(`🚨 CRITICAL ERROR: User ID mismatch in Referral Payout Job ID ${payoutId}. Job User: ${userId}, Record User: ${referrerUserId}.`);
        await updateReferralPayoutStatus(payoutId, 'failed', null, null, 'User ID mismatch in job vs record.');
        throw new Error("User ID mismatch. Not retryable.");
    }

    const payoutAmount = details.payout_amount_lamports;
    const payoutSOL = (Number(payoutAmount) / LAMPORTS_PER_SOL);

    console.log(`${logPrefix} Marking payout as 'processing' in DB.`);
    const processingMarked = await updateReferralPayoutStatus(payoutId, 'processing');
     if (!processingMarked) {
        console.warn(`${logPrefix} Failed to mark payout as 'processing'. Another process might be handling it or record changed state. Skipping attempt.`);
        return;
    }

    try {
        console.log(`${logPrefix} Fetching linked wallet for referrer ${referrerUserId}...`);
        const recipientAddress = await getLinkedWallet(referrerUserId);
        if (!recipientAddress) {
            const errorMsg = `Referrer user ${referrerUserId} has no linked wallet. Cannot process payout.`;
            console.error(`${logPrefix} ${errorMsg}`);
            await updateReferralPayoutStatus(payoutId, 'failed', null, null, 'Referrer has no linked wallet set.');
             await notifyAdmin(`🚨 Referral Payout FAILED: Payout ID ${payoutId}, Referrer ${referrerUserId} has no linked wallet.`);
            return;
        }
         console.log(`${logPrefix} Found linked wallet: ${recipientAddress}`);

        console.log(`${logPrefix} Attempting send ${payoutSOL.toFixed(SOL_DECIMALS)} SOL to referrer ${referrerUserId} (${recipientAddress.slice(0,6)}...)`);

        const sendResult = await sendSol(recipientAddress, payoutAmount, 'referral');

        if (sendResult.success && sendResult.signature) {
            const signature = sendResult.signature;
            console.log(`${logPrefix} On-chain send successful: ${signature}. Marking as 'paid'...`);
            const recorded = await updateReferralPayoutStatus(payoutId, 'paid', null, signature);
            if (!recorded) {
                 const criticalErrorMsg = `CRITICAL ERROR: Referral payment sent (TX: ${signature}) for Payout ID ${payoutId} / User ${referrerUserId} but FAILED to record 'paid' status! Manual check needed.`;
                 console.error(`${logPrefix} ${criticalErrorMsg}`);
                 await notifyAdmin(`🚨 ${criticalErrorMsg}`);
            } else {
                 console.log(`${logPrefix} SUCCESS! ✅ Referral payout complete. TX: ${signature}`);
                 try {
                      console.log(`${logPrefix} Sending notification to referrer ${referrerUserId}...`);
                      await safeSendMessage(
                           referrerUserId,
                           `💰 You received a referral reward of ${escapeMarkdownV2(payoutSOL.toFixed(SOL_DECIMALS))} SOL\\! Check your linked wallet: \`${escapeMarkdownV2(recipientAddress)}\``,
                           { parse_mode: 'MarkdownV2' }
                      ).catch(e => console.error(`${logPrefix} Failed to send referral notification to user ${referrerUserId}: ${e.message}`));
                 } catch (notifyError) {
                      console.warn(`${logPrefix} Failed to send referral payout notification to user ${referrerUserId}: ${notifyError.message}`);
                 }
            }
        } else {
             const errorMsg = `sendSol failed. Error: ${sendResult.error}`;
             console.error(`${logPrefix} ${errorMsg}`);
             const sendError = new Error(sendResult.error || "sendSol failed without specific error message");
             if (sendResult.isRetryable !== undefined) { sendError.isRetryable = sendResult.isRetryable; }
             else { sendError.isRetryable = isRetryableSolanaError(sendError); }
             throw sendError;
        }
    } catch (error) {
         console.error(`${logPrefix} Processing failed during execution. Error: ${error.message}`);
         if (error.isRetryable === undefined) {
              error.isRetryable = isRetryableSolanaError(error);
         }
         throw error;
    }
}


// --- Deposit Monitoring Task (Polling Method) ---
async function monitorDepositsPolling() {
    const logPrefix = "[DepositMonitor Polling]";
    if (!isFullyInitialized) return;
    if (monitorDepositsPolling.isRunning) return;
    monitorDepositsPolling.isRunning = true;
    const startTime = Date.now();
    let checkedAddresses = 0, foundSignatures = 0, addressesToCheck = 0;

    try {
        const batchSize = parseInt(process.env.DEPOSIT_MONITOR_ADDRESS_BATCH_SIZE, 10);
        const pendingAddresses = await queryDatabase(
            `SELECT id, user_id, deposit_address, expires_at FROM deposit_addresses WHERE status = 'pending' AND expires_at > NOW() ORDER BY created_at ASC LIMIT $1`,
            [batchSize]
        ).then(res => res.rows);
        addressesToCheck = pendingAddresses.length;
        if (addressesToCheck === 0) {
            monitorDepositsPolling.isRunning = false;
            return;
        }
        const sigFetchLimit = parseInt(process.env.DEPOSIT_MONITOR_SIGNATURE_FETCH_LIMIT, 10);
        for (const addrInfo of pendingAddresses) {
            const addrPrefix = `[DepositMonitor Addr:${addrInfo.deposit_address.slice(0,6)}..]`;
            checkedAddresses++;
            try {
                const pubKey = new PublicKey(addrInfo.deposit_address);
                const signaturesInfo = await solanaConnection.getSignaturesForAddress(pubKey, { limit: sigFetchLimit }, DEPOSIT_CONFIRMATION_LEVEL);
                for (const sigInfo of signaturesInfo) {
                     const sigPrefix = `${addrPrefix} TX:${sigInfo.signature.slice(0,6)}..`;
                     if (!sigInfo.err && !hasProcessedDepositTx(sigInfo.signature)) {
                          foundSignatures++;
                          console.log(`${sigPrefix} Potential deposit found! Queuing for processing.`);
                          depositProcessorQueue.add(() => processDepositTransaction(sigInfo.signature, addrInfo.deposit_address))
                               .catch(async (e) => {
                                    console.error(`${sigPrefix} Error queueing deposit processing: ${e.message}`);
                                    await notifyAdmin(`🚨 ERROR queueing deposit processing for ${sigPrefix}. Queue Error: ${e.message}`);
                                });
                          addProcessedDepositTx(sigInfo.signature);
                     }
                }
            } catch (error) {
                console.error(`${addrPrefix} Error checking signatures: ${error.message}`);
                 if (isRetryableSolanaError(error) && (String(error.message).includes('429') || error.status === 429)) {
                      console.warn(`${logPrefix} Hit rate limit checking address ${addrInfo.deposit_address}. Pausing briefly...`);
                      await sleep(1500);
                 } else if (!isRetryableSolanaError(error)) {
                       await notifyAdmin(`🚨 Non-retryable RPC error checking signatures for address ${addrInfo.deposit_address} (${addrPrefix}). Error: ${error.message}`);
                 }
            }
        }
    } catch (error) {
        console.error(`${logPrefix} Error during deposit monitoring cycle:`, error);
        await notifyAdmin(`🚨 ERROR during deposit monitoring cycle: ${error.message}`);
    } finally {
        const duration = Date.now() - startTime;
        if (addressesToCheck > 0 || foundSignatures > 0) {
             console.log(`${logPrefix} Cycle finished in ${duration}ms. Fetched: ${addressesToCheck}. Checked: ${checkedAddresses}. Found/Queued: ${foundSignatures}.`);
        }
        monitorDepositsPolling.isRunning = false;
    }
}
monitorDepositsPolling.isRunning = false;


// --- Deposit Transaction Processing ---
async function processDepositTransaction(signature, depositAddress) {
    const logPrefix = `[ProcessDeposit TX:${signature.slice(0, 6)} Addr:${depositAddress.slice(0,6)}]`;
    console.log(`${logPrefix} Starting processing...`);
    let client = null;
    let userIdForNotifyOnError = null;

    try {
        // 0. Double-check if processed
        if (hasProcessedDepositTx(signature)) {
            console.log(`${logPrefix} Signature already processed/cached (pre-check). Skipping.`);
            return;
        }
        // 1. Find address info
        const initialAddrInfo = await findDepositAddressInfo(depositAddress);
        if (!initialAddrInfo) {
            console.warn(`${logPrefix} Could not find initial deposit address info (possibly expired or invalid). Ignoring TX.`);
            addProcessedDepositTx(signature); return;
        }
        userIdForNotifyOnError = initialAddrInfo.userId;
        // 2. Fetch transaction details with retries
        // ... (Fetch TX logic - unchanged) ...
        let tx = null; /* ... fetch logic ... */
        if (!tx) { /* ... error handling ... */ throw new Error(`Transaction ${signature} not found via RPC after retries.`); }
        // 3. Validate Transaction Success
        if (tx.meta?.err) { /* ... error handling ... */ addProcessedDepositTx(signature); return; }
        // 4. Re-validate deposit address info
        const { userId, status: addrStatus, id: depositAddressId, expiresAt } = initialAddrInfo;
        if (addrStatus !== 'pending') { /* ... error handling ... */ addProcessedDepositTx(signature); return; }
        if (Date.now() > expiresAt.getTime()) { /* ... error handling ... */ addProcessedDepositTx(signature); return; }
        // 5. Analyze amount transferred
        const { transferAmount, payerAddress } = analyzeTransactionAmounts(tx, depositAddress);
        if (transferAmount <= 0n) { /* ... error handling ... */ addProcessedDepositTx(signature); return; }
        console.log(`${logPrefix} Confirmed ${Number(transferAmount) / LAMPORTS_PER_SOL} SOL deposit from ${payerAddress || 'unknown sender'}. Processing DB updates...`);

        // --- 6. Process Deposit within DB Transaction ---
        console.log(`${logPrefix} Starting DB transaction...`);
        client = await pool.connect();
        await client.query('BEGIN');
        let newDepositId = null;
        try {
            // 6a. Ensure user/balance records exist
            await ensureUserExists(userId, client);
            // 6b. Credit user's internal balance & Add ledger entry
            const balanceUpdateResult = await updateUserBalanceAndLedger(/* ... */);
            if (!balanceUpdateResult.success) throw new Error(`Failed to update balance: ${balanceUpdateResult.error}`);
            // 6c. Record the confirmed deposit
            const depositRecordResult = await recordConfirmedDeposit(/* ... */);
            if (!depositRecordResult.success) { /* ... handle idempotency or throw ... */ }
            newDepositId = depositRecordResult.depositId;
            // 6d. Update the ledger entry with the new deposit ID
            const ledgerUpdateRes = await queryDatabase(/* ... update ledger ... */);
            if (ledgerUpdateRes.rowCount === 0) { /* ... warn/notify ... */ }
            // 6e. Mark deposit address as used
            const markedUsed = await markDepositAddressUsed(client, depositAddressId);
            if (!markedUsed) throw new Error(`Failed to mark deposit address ID ${depositAddressId} as used.`);
            // 6f. Check for Referral Linking on First Deposit
            const depositCountRes = await queryDatabase(/* ... */);
            if (parseInt(depositCountRes.rows[0].count, 10) === 1) { /* ... handle referral linking ... */ }
            // --- 7. Commit DB Transaction ---
            await client.query('COMMIT');
            console.log(`${logPrefix} DB Transaction committed successfully.`);
            // 8. Add signature to processed cache AFTER successful commit
            addProcessedDepositTx(signature);
            // 9. Notify User AFTER successful commit
            await safeSendMessage(/* ... */);
            console.log(`${logPrefix} Processing complete.`);
        } catch (dbError) {
            if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { /* ... */ } }
            if (dbError.code === '23505' && dbError.constraint === 'deposits_tx_signature_key') { /* ... handle idempotency ... */ }
            else { /* ... handle other DB errors, notify admin/user ... */ }
        }
    } catch (error) { // Catch errors from *before* the DB transaction block
         /* ... handle pre-DB errors, notify admin/user ... */
    } finally {
        if (client) client.release();
    }
}


// --- Deposit Sweeping Task (with Rent Fix and Delay) ---
async function sweepDepositAddresses() {
    const logPrefix = "[DepositSweep Task]";
    if (!isFullyInitialized) return;
    if (sweepDepositAddresses.isRunning) {
        console.log(`${logPrefix} Skipping sweep, previous run still active.`);
        return;
    }
    sweepDepositAddresses.isRunning = true;
    console.log(`${logPrefix} Starting sweep cycle...`);

    let sweptCount = 0, checkedCount = 0, potentialSweepable = 0;
    const startTime = Date.now();
    let mainWalletPubKey;
    let minimumLamportsToLeave = 0n;

    try {
        // Derive main wallet target key
         mainWalletPubKey = Keypair.fromSecretKey(bs58.decode(process.env.MAIN_BOT_PRIVATE_KEY)).publicKey;
         if (!mainWalletPubKey) throw new Error("Main wallet public key could not be derived.");

         // Fetch Rent Exemption Minimum
        const rentExemption = await solanaConnection.getMinimumBalanceForRentExemption(0);
        const sweepTransactionFee = 5000n; // Standard fee for 1 signature
        minimumLamportsToLeave = BigInt(rentExemption) + sweepTransactionFee;
        console.log(`${logPrefix} Minimum lamports to leave in swept account (Rent + Fee): ${minimumLamportsToLeave}`);

        // Find addresses to check
        const addressesToSweep = await queryDatabase(
            `SELECT id, deposit_address, derivation_path FROM deposit_addresses WHERE status = 'used' LIMIT $1`,
            [SWEEP_BATCH_SIZE]
        ).then(res => res.rows);
        potentialSweepable = addressesToSweep.length;

        if (potentialSweepable === 0) {
            sweepDepositAddresses.isRunning = false; return;
        }
        console.log(`${logPrefix} Found ${potentialSweepable} potential addresses to check for sweeping.`);

        // Loop through addresses
        for (const addrData of addressesToSweep) {
            checkedCount++;
            const addrLogPrefix = `[Sweep Addr:${addrData.deposit_address.slice(0,6)}.. ID:${addrData.id}]`;
            let depositAddressKeypair = null;

            try {
                // 1. Re-derive keypair
                depositAddressKeypair = getKeypairFromPath(addrData.derivation_path);
                if (!depositAddressKeypair || depositAddressKeypair.publicKey.toBase58() !== addrData.deposit_address) {
                    throw new Error(`Failed to re-derive keypair or public key mismatch`);
                }

                // 2. Check balance & Convert
                const balanceNum = await solanaConnection.getBalance(depositAddressKeypair.publicKey, 'confirmed');
                const balanceBigInt = BigInt(balanceNum);

                // 3. Check if sweepable
                if (balanceBigInt <= minimumLamportsToLeave) {
                    console.log(`${addrLogPrefix} Balance (${balanceBigInt}) <= min required (${minimumLamportsToLeave}). Marking as swept.`);
                    await queryDatabase("UPDATE deposit_addresses SET status = 'swept' WHERE id = $1 AND status = 'used'", [addrData.id], pool);
                    continue;
                }

                // 4. Calculate Amount to Send
                const amountToSend = balanceBigInt - minimumLamportsToLeave;
                if (amountToSend <= 0n) {
                     console.log(`${addrLogPrefix} Calculated amountToSend (${amountToSend}) is not positive. Skipping.`);
                     continue;
                }
                console.log(`${addrLogPrefix} Balance: ${balanceBigInt}. Sweeping ${amountToSend} lamports to ${mainWalletPubKey.toBase58()}. Leaving: ${minimumLamportsToLeave}`);

                // 5. Build Transaction
                const { blockhash, lastValidBlockHeight } = await solanaConnection.getLatestBlockhash('confirmed');
                const transaction = new Transaction({
                    recentBlockhash: blockhash, feePayer: depositAddressKeypair.publicKey, lastValidBlockHeight
                }).add( SystemProgram.transfer({ fromPubkey: depositAddressKeypair.publicKey, toPubkey: mainWalletPubKey, lamports: amountToSend }) );

                // 6. Sign and Send
                const signature = await sendAndConfirmTransaction( solanaConnection, transaction, [depositAddressKeypair], { commitment: 'confirmed', skipPreflight: false } );
                console.log(`${addrLogPrefix} ✅ Sweep successful! TX: ${signature}`);
                sweptCount++;

                // 7. Update DB Status to 'swept'
                try {
                    const updateRes = await queryDatabase("UPDATE deposit_addresses SET status = 'swept' WHERE id = $1 AND status = 'used'", [addrData.id], pool);
                    if (updateRes.rowCount > 0) { console.log(`${addrLogPrefix} Marked address as swept in DB.`); }
                    else { /* Warn/Notify */ console.warn(`${addrLogPrefix} Failed to mark address as swept in DB post-sweep.`); await notifyAdmin(`⚠️ SWEEP DB Warning: Failed to mark address ID ${addrData.id} as swept post-sweep. TX: ${signature}`); }
                } catch(dbUpdateError) { /* Critical Notify */ console.error(`${addrLogPrefix} CRITICAL update status error post-sweep TX ${signature}: ${dbUpdateError.message}`); await notifyAdmin(`🚨 CRITICAL SWEEP DB ERROR post-sweep Addr ID ${addrData.id} TX ${signature}. Error: ${dbUpdateError.message}`); }

            } catch (sweepError) {
                console.error(`${addrLogPrefix} Sweep failed: ${sweepError.message}`);
                 if(sweepError.message.includes('insufficient funds for rent')) { console.error(`${addrLogPrefix} Rent exemption error detected.`); await notifyAdmin(`⚠️ SWEEP Warning: Rent error Addr ID ${addrData.id}. Balance: ${balanceBigInt}, MinToLeave: ${minimumLamportsToLeave}`); }
                 else if (!isRetryableSolanaError(sweepError)) { await notifyAdmin(`🚨 Non-retryable SWEEP error Addr ID ${addrData.id} (${addrLogPrefix}): ${sweepError.message}`); }
            } finally {
                 // Delay between processing addresses in the batch
                 if (potentialSweepable > 1 && checkedCount < potentialSweepable) {
                     await sleep(SWEEP_ADDRESS_DELAY_MS);
                 }
            }
        } // End for loop

    } catch (error) {
        console.error(`${logPrefix} Error during sweep cycle outer loop:`, error);
        await notifyAdmin(`🚨 ERROR during sweep cycle: ${error.message}`);
    } finally {
        const duration = Date.now() - startTime;
        console.log(`${logPrefix} Cycle finished in ${duration}ms. Checked: ${checkedCount}/${potentialSweepable}. Swept: ${sweptCount}.`);
        sweepDepositAddresses.isRunning = false; // Ensure lock is always released
    }
}
sweepDepositAddresses.isRunning = false;


// --- Telegram Bot Event Listeners ---
function setupTelegramListeners() {
    // ... (Listeners unchanged) ...
    console.log("⚙️ Setting up Telegram event listeners...");
    bot.on('message', /* ... */ );
    bot.on('callback_query', /* ... */ );
    bot.on('polling_error', /* ... */ );
    bot.on('webhook_error', /* ... */ );
    bot.on('error', /* ... */ );
    console.log("✅ Telegram listeners registered.");
}

// --- Express Server Route Setup ---
// ... (setupExpressRoutes function unchanged) ...
const webhookPath = `/bot${process.env.BOT_TOKEN}`;
function setupExpressRoutes() {
    console.log("⚙️ Setting up Express routes...");
    // ... (app.get('/'), app.get('/health'), etc. unchanged) ...
    app.post(webhookPath, /* ... */ );
    console.log(`✅ Express routes configured. Webhook path: ${webhookPath}`);
}


// --- Database Initialization Function ---
async function initializeDatabase() {
    // ... (initializeDatabase function unchanged from previous full Part 6 version) ...
     console.log("⚙️ Initializing Database schema (v3.1.5 - Sweeping Ready)...");
     // ... (Code is identical to the previous Part 6 initializeDatabase) ...
     console.log("✅ Database schema initialized/verified successfully.");
}

// --- Startup/Shutdown Helpers ---
// ... (setupTelegramWebhook function unchanged) ...
async function setupTelegramWebhook() { /* ... */ }
// ... (startPollingIfNeeded function unchanged) ...
async function startPollingIfNeeded() { /* ... */ }
// ... (startDepositMonitor function unchanged) ...
function startDepositMonitor() { /* ... */ }

// --- UPDATED FUNCTION: Schedules the sweeper task ---
function startDepositSweeper() {
    const logPrefix = "[DepositSweeper Start]";
    console.log(`${logPrefix} Configuring deposit sweeper...`);
    if (sweepIntervalId) {
        console.warn(`${logPrefix} Already running.`); return;
    }
    const sweepIntervalMsInput = process.env.SWEEP_INTERVAL_MS || SWEEP_INTERVAL_MS_DEFAULT.toString();
    let finalSweepIntervalMs = parseInt(sweepIntervalMsInput, 10);
    if (isNaN(finalSweepIntervalMs) || finalSweepIntervalMs < 60000) {
        console.warn(`${logPrefix} Invalid SWEEP_INTERVAL_MS (${sweepIntervalMsInput}), using default ${SWEEP_INTERVAL_MS_DEFAULT / 1000}s.`);
        finalSweepIntervalMs = SWEEP_INTERVAL_MS_DEFAULT;
    }
    console.log(`${logPrefix} Starting Deposit Sweeper Task (Interval: ${finalSweepIntervalMs / 1000}s).`);
    sweepIntervalId = setInterval(() => {
        sweepDepositAddresses().catch(async (err) => {
            console.error(`${logPrefix} Unhandled error during sweep interval run:`, err);
            await notifyAdmin(`🚨 Unhandled ERROR during scheduled Deposit Sweep run: ${err.message}`);
        });
    }, finalSweepIntervalMs);
    console.log(`${logPrefix} Deposit sweeper started with Interval ID: ${sweepIntervalId}.`);
}


// --- Graceful Shutdown Handler (Updated) ---
let isShuttingDown = false;
const shutdown = async (signal) => {
    if (isShuttingDown) return;
    isShuttingDown = true;
    console.log(`\n🛑 ${signal} received, shutting down gracefully...`);
    await notifyAdmin(`ℹ️ Bot instance shutting down (Signal: ${signal}).`).catch(e => console.error("Failed shutdown notification:", e));
    isFullyInitialized = false;

    // 1. Stop Intervals
    if (depositMonitorIntervalId) { clearInterval(depositMonitorIntervalId); depositMonitorIntervalId = null; console.log("   - Stopped deposit monitor interval."); }
    if (sweepIntervalId) { clearInterval(sweepIntervalId); sweepIntervalId = null; console.log("   - Stopped deposit sweep interval."); } // Cleared sweep interval

    // 2. Stop Listeners/Server
    // ... (unchanged) ...
     console.log("   - Stopping incoming connections and listeners...");
     // ... try closing server, stopping polling/webhook ...

    // 3. Wait for Queues
    // ... (unchanged) ...
     console.log(`   - Waiting for active jobs...`);
     // ... try waiting for queues ...

    // 4. Close DB Pool
    // ... (unchanged) ...
     console.log("   - Closing database pool...");
     // ... try closing pool ...

    // 5. Exit
    console.log("🛑 Full Shutdown sequence complete.");
    process.exit(signal === 'SIGINT' || signal === 'SIGTERM' ? 0 : 1);
};


// --- Process Signal Handlers ---
// ... (unchanged) ...
process.on('SIGTERM', () => { /* calls shutdown */ });
process.on('SIGINT', () => { /* calls shutdown */ });

// --- Global Exception Handlers ---
// ... (unchanged) ...
process.on('uncaughtException', async (err, origin) => { /* logs, notifies, tries shutdown */ });
process.on('unhandledRejection', async (reason, promise) => { /* logs, notifies */ });


// --- Application Start (Updated) ---
async function startApp() {
    console.log(`\n🚀 Starting Solana Gambles Bot v3.1.5 (with Sweeper)...`);
    const PORT = process.env.PORT || 3000;

    setupExpressRoutes();
    setupTelegramListeners();

    server = app.listen(PORT, "0.0.0.0", () => {
        console.log(`✅ Server listening on 0.0.0.0:${PORT}.`);
        const initDelay = parseInt(process.env.INIT_DELAY_MS, 10);
        console.log(`⚙️ Scheduling background initialization in ${initDelay / 1000}s...`);

        setTimeout(async () => {
            console.log("\n⚙️ Starting Delayed Background Initialization...");
            try {
                await initializeDatabase(); // Initialize DB Schema
                const webhookSet = await setupTelegramWebhook(); // Setup Webhook
                if (!webhookSet) { await startPollingIfNeeded(); } else { await startPollingIfNeeded(); } // Start/Check Polling
                const me = await bot.getMe(); // Verify Bot Connection
                console.log(`✅ Connected to Telegram as bot: @${me.username} (ID: ${me.id})`);
                startDepositMonitor(); // Start Deposit Monitor
                startDepositSweeper(); // <<< Start Deposit Sweeper >>>
                isFullyInitialized = true; // Mark as Ready
                console.log("\n✅ Background Initialization Complete.");
                console.log(`🚀🚀🚀 Solana Gambles Bot (v3.1.5 - Sweeper Active) is fully operational! 🚀🚀🚀`);
                 await notifyAdmin(`✅ Bot v3.1.5 (Sweeper Active) started successfully and is fully operational.`);
            } catch (initError) {
                console.error("\n🔥🔥🔥 Delayed Background Initialization Failed:", initError);
                console.error("❌ Exiting due to critical initialization failure.");
                 await notifyAdmin(`🔥🔥🔥 Bot v3.1.5 FAILED background initialization: ${initError.message}. Exiting.`);
                if (!isShuttingDown) await shutdown('INITIALIZATION_FAILURE').catch(() => {});
                const failTimeout = parseInt(process.env.SHUTDOWN_FAIL_TIMEOUT_MS, 10);
                const t = setTimeout(() => { console.error(`--> Forcing exit (${failTimeout}ms) after init failure.`); process.exit(1); }, failTimeout);
                if (t.unref) t.unref();
            }
        }, initDelay);
    });

    server.on('error', async (err) => { /* handles server startup errors */ });
}

// --- Run the application ---
startApp().catch(async err => { /* handles top-level startup errors */ });

// --- End of Part 6 / End of File ---

// Export functions if needed
export {
    addPayoutJob,
    // ... other exports if desired ...
    startApp
};
