// index.js - Part 1: Imports, Environment Configuration, Core Initializations
// --- VERSION: 3.1.6 --- (DB Init FK Fix, Sweep Fee Buffer, Sweep Retry Logic Prep, Bet Amount Buttons Prep)

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
    // REMOVED: TransactionSignature (TypeScript type alias, cannot be imported as value)
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
import RateLimitedConnection from './lib/solana-connection.js'; // Ensure this path is correct

// --- Deployment Check Log ---
console.log(`--- INDEX.JS - DEPLOYMENT CHECK --- ${new Date().toISOString()} --- v3.1.6 ---`); // Updated Version
// --- END DEPLOYMENT CHECK ---


console.log("⏳ Starting Solana Gambles Bot (Custodial, Buttons, v3.1.6)... Checking environment variables..."); // Updated Version

// --- Environment Variable Configuration ---
const REQUIRED_ENV_VARS = [
    'BOT_TOKEN',
    'DATABASE_URL',
    'DEPOSIT_MASTER_SEED_PHRASE', // Expect BIP39 Mnemonic Seed Phrase (SECRET)
    'MAIN_BOT_PRIVATE_KEY',       // Used for general payouts (Withdrawals) AND sweep target (SECRET)
    'REFERRAL_PAYOUT_PRIVATE_KEY',// (SECRET) - Presence checked, but allowed to be empty (falls back to MAIN)
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
        console.error(`❌ Required environment variable ${keyName} is missing (key is not present).`); // Added specific log
        return false;
    } else {
         console.log(`[Env Check] Optional Private Key ${keyName} not set.`); // Log if optional key is missing
    }
    return true; // Valid format, or optional and not set
};

// MAIN_BOT_PRIVATE_KEY is required
if (!validateBase58Key('MAIN_BOT_PRIVATE_KEY', true)) missingVars = true;
// REFERRAL_PAYOUT_PRIVATE_KEY is optional, only validate if present
if (process.env.REFERRAL_PAYOUT_PRIVATE_KEY && !validateBase58Key('REFERRAL_PAYOUT_PRIVATE_KEY', false)) {
    // This case means the key IS PRESENT but INVALID format
    missingVars = true;
}


if (missingVars) {
    console.error("⚠️ Please set all required environment variables correctly. Exiting.");
    process.exit(1);
}

// Optional vars with defaults (using BigInt where appropriate)
const OPTIONAL_ENV_DEFAULTS = {
    // --- Operational ---
    'DEPOSIT_ADDRESS_EXPIRY_MINUTES': '60',
    'DEPOSIT_CONFIRMATIONS': 'confirmed',
    'WITHDRAWAL_FEE_LAMPORTS': '5000',
    'MIN_WITHDRAWAL_LAMPORTS': '10000000',
    // --- Payouts (Withdrawals, Referral Rewards) ---
    'PAYOUT_BASE_PRIORITY_FEE_MICROLAMPORTS': '1000',
    'PAYOUT_MAX_PRIORITY_FEE_MICROLAMPORTS': '1000000',
    'PAYOUT_COMPUTE_UNIT_LIMIT': '200000',
    'PAYOUT_JOB_RETRIES': '3',
    'PAYOUT_JOB_RETRY_DELAY_MS': '5000',
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
    'CF_HOUSE_EDGE': '0.03',
    'RACE_HOUSE_EDGE': '0.05',
    'SLOTS_HOUSE_EDGE': '0.10',
    'ROULETTE_HOUSE_EDGE': '0.05',
    'WAR_HOUSE_EDGE': '0.02',
    // --- Referral System ---
    'REFERRAL_INITIAL_BET_MIN_LAMPORTS': '10000000',
    'REFERRAL_MILESTONE_REWARD_PERCENT': '0.005',
    // --- Deposit Sweeping ---
    'SWEEP_INTERVAL_MS': '900000', // Default 15 minutes
    'SWEEP_BATCH_SIZE': '20',
    'SWEEP_FEE_BUFFER_LAMPORTS': '15000', // Buffer LEFT BEHIND (covers base 5k + 10k priority allowance)
    'SWEEP_ADDRESS_DELAY_MS': '750',
    'SWEEP_RETRY_ATTEMPTS': '1', // Max *additional* attempts (1 means try twice total)
    'SWEEP_RETRY_DELAY_MS': '3000',
    // --- Technical / Performance ---
    'RPC_MAX_CONCURRENT': '8',
    'RPC_RETRY_BASE_DELAY': '600',
    'RPC_MAX_RETRIES': '3',
    'RPC_RATE_LIMIT_COOLOFF': '1500',
    'RPC_RETRY_MAX_DELAY': '15000',
    'RPC_RETRY_JITTER': '0.2',
    'RPC_COMMITMENT': 'confirmed',
    'MSG_QUEUE_CONCURRENCY': '5',
    'MSG_QUEUE_TIMEOUT_MS': '10000',
    'CALLBACK_QUEUE_CONCURRENCY': '8',
    'CALLBACK_QUEUE_TIMEOUT_MS': '15000',
    'PAYOUT_QUEUE_CONCURRENCY': '3',
    'PAYOUT_QUEUE_TIMEOUT_MS': '60000',
    'DEPOSIT_PROCESS_QUEUE_CONCURRENCY': '4',
    'DEPOSIT_PROCESS_QUEUE_TIMEOUT_MS': '30000',
    'TELEGRAM_SEND_QUEUE_CONCURRENCY': '1',
    'TELEGRAM_SEND_QUEUE_INTERVAL_MS': '1050',
    'TELEGRAM_SEND_QUEUE_INTERVAL_CAP': '1',
    'DEPOSIT_MONITOR_INTERVAL_MS': '20000',
    'DEPOSIT_MONITOR_ADDRESS_BATCH_SIZE': '50',
    'DEPOSIT_MONITOR_SIGNATURE_FETCH_LIMIT': '5',
    'DB_POOL_MAX': '25',
    'DB_POOL_MIN': '5',
    'DB_IDLE_TIMEOUT': '30000',
    'DB_CONN_TIMEOUT': '5000',
    'DB_SSL': 'true',
    'DB_REJECT_UNAUTHORIZED': 'true',
    'USER_STATE_CACHE_TTL_MS': '300000', // 5 minutes
    'WALLET_CACHE_TTL_MS': '600000',     // 10 minutes
    'DEPOSIT_ADDR_CACHE_TTL_MS': '3660000', // ~1 hour + buffer
    'MAX_PROCESSED_TX_CACHE': '5000',
    'USER_COMMAND_COOLDOWN_MS': '1000', // 1 second
    'INIT_DELAY_MS': '1000',
    'SHUTDOWN_QUEUE_TIMEOUT_MS': '20000',
    'SHUTDOWN_FAIL_TIMEOUT_MS': '8000',
    'WEBHOOK_MAX_CONN': '10',
};

// Assign defaults for optional vars if they are missing
Object.entries(OPTIONAL_ENV_DEFAULTS).forEach(([key, defaultValue]) => {
    if (key === 'ADMIN_USER_IDS') return; // Skip handled required var
    if (process.env[key] === undefined) {
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
// Referral Tiers
const REFERRAL_INITIAL_BONUS_TIERS = [
    { maxCount: 10, percent: 0.05 },   // 0-10 referrals -> 5%
    { maxCount: 25, percent: 0.10 },   // 11-25 referrals -> 10%
    { maxCount: 50, percent: 0.15 },   // 26-50 referrals -> 15%
    { maxCount: 100, percent: 0.20 }, // 51-100 referrals -> 20%
    { maxCount: Infinity, percent: 0.25 } // 101+ referrals -> 25%
];
// Milestone Thresholds
const REFERRAL_MILESTONE_THRESHOLDS_LAMPORTS = [
    BigInt(1 * LAMPORTS_PER_SOL), BigInt(5 * LAMPORTS_PER_SOL), BigInt(10 * LAMPORTS_PER_SOL),
    BigInt(25 * LAMPORTS_PER_SOL), BigInt(50 * LAMPORTS_PER_SOL), BigInt(100 * LAMPORTS_PER_SOL),
    BigInt(250 * LAMPORTS_PER_SOL), BigInt(500 * LAMPORTS_PER_SOL), BigInt(1000 * LAMPORTS_PER_SOL)
];
// Sweeping Constants
const SWEEP_FEE_BUFFER_LAMPORTS = BigInt(process.env.SWEEP_FEE_BUFFER_LAMPORTS); // Buffer LEFT BEHIND in address
const SWEEP_BATCH_SIZE = parseInt(process.env.SWEEP_BATCH_SIZE, 10);
const SWEEP_ADDRESS_DELAY_MS = parseInt(process.env.SWEEP_ADDRESS_DELAY_MS, 10);
const SWEEP_RETRY_ATTEMPTS = parseInt(process.env.SWEEP_RETRY_ATTEMPTS, 10); // Max *additional* attempts
const SWEEP_RETRY_DELAY_MS = parseInt(process.env.SWEEP_RETRY_DELAY_MS, 10);

// Standard Bet Amounts
const STANDARD_BET_AMOUNTS_SOL = [0.01, 0.05, 0.10, 0.25, 0.5, 1];
const STANDARD_BET_AMOUNTS_LAMPORTS = STANDARD_BET_AMOUNTS_SOL.map(sol => BigInt(Math.round(sol * LAMPORTS_PER_SOL)));
console.log(`[Config] Standard Bet Amounts (Lamports): ${STANDARD_BET_AMOUNTS_LAMPORTS.join(', ')}`);

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
let sweepIntervalId = null; // Interval ID for the deposit sweeper
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
    httpHeaders: { 'User-Agent': `SolanaGamblesBot/3.1.6` }, // Updated version
    clientId: `SolanaGamblesBot/3.1.6` // Updated version
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
const payoutProcessorQueue = new PQueue({
    concurrency: parseInt(process.env.PAYOUT_QUEUE_CONCURRENCY, 10),
    timeout: parseInt(process.env.PAYOUT_QUEUE_TIMEOUT_MS, 10),
    throwOnTimeout: true
});
const depositProcessorQueue = new PQueue({
    concurrency: parseInt(process.env.DEPOSIT_PROCESS_QUEUE_CONCURRENCY, 10),
    timeout: parseInt(process.env.DEPOSIT_PROCESS_QUEUE_TIMEOUT_MS, 10),
    throwOnTimeout: true
});
const telegramSendQueue = new PQueue({
    concurrency: parseInt(process.env.TELEGRAM_SEND_QUEUE_CONCURRENCY, 10), // MUST BE 1
    interval: parseInt(process.env.TELEGRAM_SEND_QUEUE_INTERVAL_MS, 10),
    intervalCap: parseInt(process.env.TELEGRAM_SEND_QUEUE_INTERVAL_CAP, 10) // MUST BE 1
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
    // Consider removing the client? pool.remove(client)? Requires careful testing.
    notifyAdmin(`🚨 DB Pool Error (Idle Client): ${err.message}`).catch(()=>{}); // Fire and forget notification
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
            maxSockets: 100, // Allow more sockets
            maxFreeSockets: 10,
            scheduling: 'fifo', // Default scheduling
        }
    }
});
console.log("✅ Telegram Bot instance created.");

// Game Configuration Loading
console.log("⚙️ Loading Game Configuration...");
const GAME_CONFIG = {
    coinflip: {
        key: "coinflip", // Add key for reference
        name: "Coinflip", // Add display name
        minBetLamports: BigInt(process.env.CF_MIN_BET_LAMPORTS),
        maxBetLamports: BigInt(process.env.CF_MAX_BET_LAMPORTS),
        houseEdge: parseFloat(process.env.CF_HOUSE_EDGE)
    },
    race: {
        key: "race",
        name: "Race",
        minBetLamports: BigInt(process.env.RACE_MIN_BET_LAMPORTS),
        maxBetLamports: BigInt(process.env.RACE_MAX_BET_LAMPORTS),
        houseEdge: parseFloat(process.env.RACE_HOUSE_EDGE)
    },
    slots: {
        key: "slots",
        name: "Slots",
        minBetLamports: BigInt(process.env.SLOTS_MIN_BET_LAMPORTS),
        maxBetLamports: BigInt(process.env.SLOTS_MAX_BET_LAMPORTS),
        houseEdge: parseFloat(process.env.SLOTS_HOUSE_EDGE)
    },
    roulette: {
        key: "roulette",
        name: "Roulette",
        minBetLamports: BigInt(process.env.ROULETTE_MIN_BET_LAMPORTS),
        maxBetLamports: BigInt(process.env.ROULETTE_MAX_BET_LAMPORTS),
        houseEdge: parseFloat(process.env.ROULETTE_HOUSE_EDGE)
    },
    war: {
        key: "war",
        name: "Casino War",
        minBetLamports: BigInt(process.env.WAR_MIN_BET_LAMPORTS),
        maxBetLamports: BigInt(process.env.WAR_MAX_BET_LAMPORTS),
        houseEdge: parseFloat(process.env.WAR_HOUSE_EDGE)
    }
};
// Basic validation function (can be expanded)
function validateGameConfig(config) {
    const errors = [];
    for (const gameKey in config) {
        const gc = config[gameKey];
        if (!gc) { errors.push(`Config for game "${gameKey}" missing.`); continue; }
        if (!gc.key || gc.key !== gameKey) errors.push(`${gameKey}.key missing or mismatch`);
        if (!gc.name) errors.push(`${gameKey}.name missing`);
        if (typeof gc.minBetLamports !== 'bigint' || gc.minBetLamports <= 0n) errors.push(`${gameKey}.minBetLamports invalid (${gc.minBetLamports})`);
        if (typeof gc.maxBetLamports !== 'bigint' || gc.maxBetLamports <= 0n) errors.push(`${gameKey}.maxBetLamports invalid (${gc.maxBetLamports})`);
        if (gc.minBetLamports > gc.maxBetLamports) errors.push(`${gameKey} minBet > maxBet`);
        if (isNaN(gc.houseEdge) || gc.houseEdge < 0 || gc.houseEdge >= 1) errors.push(`${gameKey}.houseEdge invalid (${gc.houseEdge})`);
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
// --- VERSION: 3.1.6 --- (DB Init FK Fix, Sweep Fee Buffer, Sweep Retry Logic Prep, Bet Amount Buttons Prep)

// --- Assuming 'pool', 'PublicKey', utils like 'generateReferralCode', 'updateWalletCache' etc. are available from other parts ---

// --- Helper Function for DB Operations ---
/**
 * Executes a database query using either a provided client or the pool.
 * Handles basic error logging including parameters (converting BigInts).
 * @param {string} sql The SQL query string with placeholders ($1, $2, ...).
 * @param {Array<any>} [params=[]] The parameters for the SQL query.
 * @param {pg.PoolClient | pg.Pool} [dbClient=pool] The DB client or pool to use. Defaults to the global 'pool'.
 * @returns {Promise<pg.QueryResult<any>>} The query result.
 * @throws {Error} Throws the original database error if the query fails.
 */
async function queryDatabase(sql, params = [], dbClient = pool) {
     // Ensure dbClient is valid, default to pool if necessary
     // Check if pool itself exists before defaulting
     if (!dbClient) {
         if (!pool) {
            const poolError = new Error("Database pool not available for queryDatabase");
            console.error("❌ CRITICAL: queryDatabase called but default pool is not initialized!", poolError.stack);
            throw poolError;
         }
         dbClient = pool; // Fallback to global pool
     }
     // Check if sql is valid before attempting query
     if (typeof sql !== 'string') {
        const sqlError = new TypeError(`Client was passed a non-string query (type: ${typeof sql}, value: ${sql})`);
        console.error(`❌ DB Query Error:`, sqlError.message);
        console.error(`   Params: ${JSON.stringify(params, (k, v) => typeof v === 'bigint' ? v.toString() + 'n' : v)}`);
        console.error(`   Stack:`, sqlError.stack); // Log stack trace here
        throw sqlError;
     }

    try {
        return await dbClient.query(sql, params);
    } catch (error) {
        console.error(`❌ DB Query Error:`);
        console.error(`   SQL: ${sql.substring(0, 500)}${sql.length > 500 ? '...' : ''}`);
        // Safely stringify parameters, converting BigInts to strings for logging
        const safeParamsString = JSON.stringify(params, (key, value) =>
            typeof value === 'bigint'
                ? value.toString() + 'n' // Convert BigInt to string representation
                : value // Return other values unchanged
        );
        console.error(`   Params: ${safeParamsString}`); // Log the safe string
        console.error(`   Error Code: ${error.code}`);
        console.error(`   Error Message: ${error.message}`);
        if (error.constraint) { // Log constraint name if available
            console.error(`   Constraint: ${error.constraint}`);
        }
        // console.error(`   Stack: ${error.stack}`); // Optional: full stack trace
        throw error; // Re-throw the original error
    }
}
// --- End Helper ---


// --- Wallet/User Operations ---

/**
 * Ensures a user exists in the 'wallets' table, creating a basic record if not.
 * Also ensures a corresponding 'user_balances' record exists.
 * Locks relevant rows using FOR UPDATE.
 * Should be called within a transaction if used alongside other user modifications.
 * @param {string} userId The user's Telegram ID.
 * @param {pg.PoolClient} client The active database client connection.
 * @returns {Promise<{userId: string, isNewUser: boolean}>}
 */
async function ensureUserExists(userId, client) {
    userId = String(userId);
    let isNewUser = false;
    const logPrefix = `[DB ensureUserExists User ${userId}]`;

    // 1. Check/Insert into wallets, locking the row (or potential row)
    const walletCheck = await queryDatabase('SELECT user_id FROM wallets WHERE user_id = $1 FOR UPDATE', [userId], client);
    if (walletCheck.rowCount === 0) {
        await queryDatabase(
            `INSERT INTO wallets (user_id, created_at, last_used_at) VALUES ($1, NOW(), NOW())`,
            [userId],
            client
        );
        isNewUser = true;
        console.log(`${logPrefix} Created base wallet record for new user.`);
    } else {
        // Update last_used_at for existing user
        await queryDatabase(
            `UPDATE wallets SET last_used_at = NOW() WHERE user_id = $1`,
            [userId],
            client
        );
    }

    // 2. Check/Insert into user_balances, locking the row
    const balanceCheck = await queryDatabase('SELECT user_id FROM user_balances WHERE user_id = $1 FOR UPDATE', [userId], client);
    if (balanceCheck.rowCount === 0) {
        await queryDatabase(
            `INSERT INTO user_balances (user_id, balance_lamports, updated_at) VALUES ($1, 0, NOW())`,
            [userId],
            client
        );
        if (!isNewUser) {
             console.warn(`${logPrefix} Created missing balance record for existing user.`);
        }
    }

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
    let client = null;
    try {
        // Validate address format before hitting DB
        try {
             new PublicKey(externalAddress);
        } catch (validationError) {
            console.error(`${logPrefix} Invalid address format provided: ${externalAddress}`);
            return { success: false, error: 'Invalid Solana wallet address format.' };
        }

        client = await pool.connect();
        await client.query('BEGIN');

        await ensureUserExists(userId, client); // Locks rows

        const detailsRes = await queryDatabase('SELECT referral_code FROM wallets WHERE user_id = $1 FOR UPDATE', [userId], client);
        if (detailsRes.rowCount === 0) {
             throw new Error(`Wallet row for user ${userId} not found after ensureUserExists.`);
        }

        let currentReferralCode = detailsRes.rows[0]?.referral_code;
        let needsCodeUpdate = false;

        if (!currentReferralCode) {
            console.log(`${logPrefix} User missing referral code, generating one.`);
            currentReferralCode = generateReferralCode(); // Assumes Part 3 function
            needsCodeUpdate = true;
        }

        const updateQuery = `
            UPDATE wallets
            SET external_withdrawal_address = $1,
                linked_at = COALESCE(linked_at, NOW()),
                last_used_at = NOW()
                ${needsCodeUpdate ? ', referral_code = $3' : ''}
            WHERE user_id = $2
        `;
        const updateParams = needsCodeUpdate ? [externalAddress, userId, currentReferralCode] : [externalAddress, userId];
        await queryDatabase(updateQuery, updateParams, client);

        await client.query('COMMIT');
        console.log(`${logPrefix} External withdrawal address set/updated to ${externalAddress}. Referral Code: ${currentReferralCode}`);

        updateWalletCache(userId, { withdrawalAddress: externalAddress, referralCode: currentReferralCode }); // Assumes Part 3 function

        return { success: true, wallet: externalAddress, referralCode: currentReferralCode };

    } catch (err) {
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { console.error(`${logPrefix} Rollback failed:`, rbErr); } }

        if (err.code === '23505' && err.constraint === 'wallets_referral_code_key') {
            console.error(`${logPrefix} CRITICAL - Referral code generation conflict for user ${userId}.`);
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
 * @returns {Promise<string | undefined>} The address string or undefined if not set/found or error.
 */
async function getLinkedWallet(userId) {
    userId = String(userId);
    const cached = getWalletCache(userId); // Assumes Part 3 function
    if (cached?.withdrawalAddress !== undefined) { // Check for explicit undefined from cache miss too
        return cached.withdrawalAddress;
    }

    try {
        const res = await queryDatabase('SELECT external_withdrawal_address, referral_code FROM wallets WHERE user_id = $1', [userId]);
        const details = res.rows[0];
        const withdrawalAddress = details?.external_withdrawal_address || undefined;
        const referralCode = details?.referral_code;
        // Update cache with potentially fresh data (address + ref code), including undefined if not set
        updateWalletCache(userId, { withdrawalAddress: withdrawalAddress, referralCode: referralCode }); // Assumes Part 3 function
        return withdrawalAddress;
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
 * Locks rows using FOR UPDATE.
 * @param {string} refereeUserId The ID of the user being referred.
 * @param {string} referrerUserId The ID of the user who referred them.
 * @param {pg.PoolClient} client The active database client.
 * @returns {Promise<boolean>} True if the link was successfully made or already existed correctly, false on error or if already referred by someone else.
 */
async function linkReferral(refereeUserId, referrerUserId, client) {
    refereeUserId = String(refereeUserId);
    referrerUserId = String(referrerUserId);
    const logPrefix = `[DB linkReferral Ref:${referrerUserId} -> New:${refereeUserId}]`;

    if (refereeUserId === referrerUserId) {
        console.warn(`${logPrefix} User attempted to refer themselves.`);
        return false;
    }

    try {
        // Check current status of referee, locking the row
        const checkRes = await queryDatabase('SELECT referred_by_user_id FROM wallets WHERE user_id = $1 FOR UPDATE', [refereeUserId], client);
        if (checkRes.rowCount === 0) {
             console.error(`${logPrefix} Referee user not found.`);
             return false;
        }
        const currentReferrer = checkRes.rows[0].referred_by_user_id;

        if (currentReferrer === referrerUserId) {
            console.log(`${logPrefix} Already linked to this referrer.`)
            return true;
        } else if (currentReferrer) {
            console.warn(`${logPrefix} Already referred by ${currentReferrer}. Cannot link to ${referrerUserId}.`);
            return false;
        }

        // Link the referral (only if not currently referred)
        const updateRes = await queryDatabase(
            'UPDATE wallets SET referred_by_user_id = $1 WHERE user_id = $2 AND referred_by_user_id IS NULL',
            [referrerUserId, refereeUserId],
            client
        );

        if (updateRes.rowCount > 0) {
             console.log(`${logPrefix} Successfully linked referee to referrer.`);
             // Increment the referrer's count (Lock referrer row first)
             await queryDatabase('SELECT user_id FROM wallets WHERE user_id = $1 FOR UPDATE', [referrerUserId], client);
             await queryDatabase('UPDATE wallets SET referral_count = referral_count + 1 WHERE user_id = $1', [referrerUserId], client);
             console.log(`${logPrefix} Incremented referral count for referrer.`);
             return true;
        } else {
             console.warn(`${logPrefix} Failed to link referral - likely already referred just before update.`);
             return false;
        }
    } catch (err) {
        console.error(`${logPrefix} Error linking referral:`, err);
        return false;
    }
}


/**
 * Updates a user's total wagered amount and potentially the last milestone paid threshold.
 * Should be called within a transaction after a bet is completed. Locks the wallet row.
 * @param {string} userId The ID of the user who wagered.
 * @param {bigint} wageredAmountLamports The amount wagered in this bet.
 * @param {pg.PoolClient} client The active database client.
 * @param {bigint | null} [newLastMilestonePaidLamports=null] If a milestone was just paid for this user's referrer, update the threshold marker.
 * @returns {Promise<boolean>} True on success, false on failure.
 */
async function updateUserWagerStats(userId, wageredAmountLamports, client, newLastMilestonePaidLamports = null) {
    userId = String(userId);
    const wageredAmount = BigInt(wageredAmountLamports);
    const logPrefix = `[DB updateUserWagerStats User ${userId}]`;

    if (wageredAmount <= 0n) return true; // No change needed if wager is zero or negative

    try {
         // Lock the user's wallet row before updating
         await queryDatabase('SELECT user_id FROM wallets WHERE user_id = $1 FOR UPDATE', [userId], client);

         let query;
         let values;
         const lastMilestonePaid = (newLastMilestonePaidLamports !== null && BigInt(newLastMilestonePaidLamports) >= 0n)
                                  ? BigInt(newLastMilestonePaidLamports)
                                  : null;

         // Build query based on whether milestone marker needs updating
         if (lastMilestonePaid !== null) {
             query = `
                 UPDATE wallets
                 SET total_wagered = total_wagered + $2,
                     last_milestone_paid_lamports = $3, -- Update milestone marker
                     last_used_at = NOW()
                 WHERE user_id = $1;
             `;
             values = [userId, wageredAmount, lastMilestonePaid];
         } else {
             query = `
                 UPDATE wallets
                 SET total_wagered = total_wagered + $2, -- Only update total wagered
                     last_used_at = NOW()
                 WHERE user_id = $1;
             `;
             values = [userId, wageredAmount];
         }

         const res = await queryDatabase(query, values, client);
         if (res.rowCount === 0) {
             console.warn(`${logPrefix} Attempted to update wager stats for non-existent user (or lock failed?).`);
             return false;
         }
         return true;
    } catch (err) {
         console.error(`${logPrefix} Error updating wager stats:`, err.message);
         return false;
    }
}

// --- Balance Operations ---

/**
 * Gets the current internal balance for a user. Creates user/balance record if needed.
 * Handles its own transaction to ensure user/balance records exist safely.
 * @param {string} userId
 * @returns {Promise<bigint>} Current balance in lamports, or 0n if user/balance record creation fails.
 */
async function getUserBalance(userId) {
    userId = String(userId);
    let client = null;
    try {
        client = await pool.connect();
        await client.query('BEGIN');

        // Ensure user exists (locks rows for safety via FOR UPDATE inside)
        await ensureUserExists(userId, client);

        // Fetch balance (row is implicitly locked by ensureUserExists FOR UPDATE)
        const balanceRes = await queryDatabase('SELECT balance_lamports FROM user_balances WHERE user_id = $1', [userId], client);

        await client.query('COMMIT'); // Commit ensuring user/balance exists

        return BigInt(balanceRes.rows[0]?.balance_lamports || '0');

    } catch (err) {
        console.error(`[DB GetBalance] Error fetching/ensuring balance for user ${userId}:`, err.message);
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { console.error(`[DB GetBalance] Rollback failed: ${rbErr.message}`); } }
        return 0n; // Return 0 on error
    } finally {
        if (client) client.release();
    }
}

/**
 * Atomically updates a user's balance and records the change in the ledger.
 * MUST be called within a DB transaction. Locks the user_balances row.
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
    const logPrefix = `[UpdateBalance User ${userId} Type ${transactionType}]`;

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
            console.error(`${logPrefix} User balance record not found during update.`);
            return { success: false, error: 'User balance record unexpectedly missing.' };
        }

        const balanceBefore = BigInt(balanceRes.rows[0].balance_lamports);
        const balanceAfter = balanceBefore + changeAmount;

        // 2. Check for insufficient funds only if debiting
        if (changeAmount < 0n && balanceAfter < 0n) {
            const needed = -changeAmount;
            console.warn(`${logPrefix} Insufficient balance. Current: ${balanceBefore}, Needed: ${needed}`);
            return { success: false, error: 'Insufficient balance' };
        }

        // 3. Update balance
        const updateRes = await queryDatabase(
            'UPDATE user_balances SET balance_lamports = $1, updated_at = NOW() WHERE user_id = $2',
            [balanceAfter, userId],
            client
        );
        if (updateRes.rowCount === 0) {
            console.error(`${logPrefix} Failed to update balance row after lock.`);
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

        return { success: true, newBalance: balanceAfter };

    } catch (err) {
        console.error(`${logPrefix} Error updating balance/ledger:`, err.message, err.code);
        let errMsg = `Database error during balance update (${err.code || 'N/A'})`;
        if (err.constraint === 'user_balances_balance_lamports_check') {
            errMsg = 'Insufficient balance (check constraint violation).';
        }
        return { success: false, error: errMsg };
    }
}


// --- Deposit Operations ---

/**
 * Creates a new unique deposit address record for a user.
 * Does NOT require an external transaction.
 * Adds address to cache upon successful creation.
 * @param {string} userId
 * @param {string} depositAddress - The generated unique address.
 * @param {string} derivationPath - The path used to derive the address.
 * @param {Date} expiresAt - Expiry timestamp for the address.
 * @returns {Promise<{success: boolean, id?: number, error?: string}>}
 */
async function createDepositAddressRecord(userId, depositAddress, derivationPath, expiresAt) {
    userId = String(userId);
    const logPrefix = `[DB CreateDepositAddr User ${userId} Addr ${depositAddress.slice(0, 6)}..]`;
    console.log(`${logPrefix} Attempting to create record.`);
    const query = `
        INSERT INTO deposit_addresses (user_id, deposit_address, derivation_path, expires_at, status, created_at)
        VALUES ($1, $2, $3, $4, 'pending', NOW())
        RETURNING id;
    `;
    try {
        const res = await queryDatabase(query, [userId, depositAddress, derivationPath, expiresAt], pool);
        if (res.rowCount > 0 && res.rows[0].id) {
            const depositAddressId = res.rows[0].id;
            console.log(`${logPrefix} Successfully created record with ID: ${depositAddressId}. Adding to cache.`);
            addActiveDepositAddressCache(depositAddress, userId, expiresAt.getTime()); // Assumes Part 3 function
            return { success: true, id: depositAddressId };
        } else {
            console.error(`${logPrefix} Failed to insert deposit address record (no ID returned).`);
            return { success: false, error: 'Failed to insert deposit address record (no ID returned).' };
        }
    } catch (err) {
        console.error(`${logPrefix} DB Error:`, err.message, err.code);
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
 * @returns {Promise<{userId: string, status: string, id: number, expiresAt: Date} | null>} Returns null if not found, expired, or on DB error.
 */
async function findDepositAddressInfo(depositAddress) {
    const logPrefix = `[DB FindDepositAddr Addr ${depositAddress.slice(0, 6)}..]`;
    const cached = getActiveDepositAddressCache(depositAddress); // Assumes Part 3 function
    const now = Date.now();

    if (cached && now < cached.expiresAt) {
        try {
            const statusRes = await queryDatabase(
                'SELECT status, id, expires_at FROM deposit_addresses WHERE deposit_address = $1',
                [depositAddress]
            );
            if (statusRes.rowCount > 0) {
                const dbInfo = statusRes.rows[0];
                const dbExpiresAt = new Date(dbInfo.expires_at).getTime();
                if (cached.expiresAt !== dbExpiresAt) {
                    console.warn(`${logPrefix} Cache expiry mismatch for ${depositAddress}. Updating cache expiry.`);
                    addActiveDepositAddressCache(depositAddress, cached.userId, dbExpiresAt); // Assumes Part 3 function
                }
                return { userId: cached.userId, status: dbInfo.status, id: dbInfo.id, expiresAt: new Date(dbInfo.expires_at) };
            } else {
                removeActiveDepositAddressCache(depositAddress); // Assumes Part 3 function
                console.warn(`${logPrefix} Address was cached but not found in DB. Cache removed.`);
                return null;
            }
        } catch (err) {
            console.error(`${logPrefix} Error fetching status for cached addr:`, err.message);
            return null;
        }
    } else if (cached) {
        console.log(`${logPrefix} Cache expired. Removing.`);
        removeActiveDepositAddressCache(depositAddress); // Assumes Part 3 function
    }

    try {
        const res = await queryDatabase(
            'SELECT user_id, status, id, expires_at FROM deposit_addresses WHERE deposit_address = $1',
            [depositAddress]
        );
        if (res.rowCount > 0) {
            const data = res.rows[0];
            const expiresAtDate = new Date(data.expires_at);
            if (data.status === 'pending' && now < expiresAtDate.getTime()) {
                addActiveDepositAddressCache(depositAddress, data.user_id, expiresAtDate.getTime()); // Assumes Part 3 function
            } else if (data.status === 'pending' && now >= expiresAtDate.getTime()) {
                 console.warn(`${logPrefix} Address found as 'pending' in DB but is expired. Attempting background update.`);
                 queryDatabase("UPDATE deposit_addresses SET status = 'expired' WHERE id = $1 AND status = 'pending'", [data.id], pool)
                    .catch(err => console.error(`${logPrefix} Background attempt to mark expired failed: ${err.message}`));
            }
            return { userId: data.user_id, status: data.status, id: data.id, expiresAt: expiresAtDate };
        }
        return null; // Not found in DB
    } catch (err) {
        console.error(`${logPrefix} Error finding info in DB:`, err.message);
        return null;
    }
}

/**
 * Marks a deposit address as used. MUST be called within a DB transaction.
 * Removes the address from the active cache upon successful update.
 * @param {pg.PoolClient} client The active database client.
 * @param {number} depositAddressId The ID of the deposit address record.
 * @returns {Promise<boolean>} True if the status was updated from 'pending' to 'used'.
 */
async function markDepositAddressUsed(client, depositAddressId) {
    const logPrefix = `[DB MarkDepositUsed ID ${depositAddressId}]`;
    try {
        // Select the address first to remove from cache later, locking the row
        const addrRes = await queryDatabase('SELECT deposit_address FROM deposit_addresses WHERE id = $1 FOR UPDATE', [depositAddressId], client);
        if (addrRes.rowCount === 0) {
            console.warn(`${logPrefix} Cannot mark as used: Address ID not found.`);
            return false;
        }
        const depositAddress = addrRes.rows[0].deposit_address;

        // Update status only if currently pending
        const res = await queryDatabase(
            `UPDATE deposit_addresses SET status = 'used' WHERE id = $1 AND status = 'pending'`,
            [depositAddressId],
            client
        );
        if (res.rowCount > 0) {
             removeActiveDepositAddressCache(depositAddress); // Assumes Part 3 function
             return true;
        } else {
             console.warn(`${logPrefix} Failed to mark as 'used' (status might not be 'pending').`);
             removeActiveDepositAddressCache(depositAddress); // Still remove from cache
             return false;
        }
    } catch (err) {
        console.error(`${logPrefix} Error updating status to 'used':`, err.message);
        return false;
    }
}

/**
 * Records a confirmed deposit transaction. MUST be called within a DB transaction.
 * Uses ON CONFLICT DO NOTHING to handle potential duplicate calls gracefully.
 * @param {pg.PoolClient} client The active database client.
 * @param {string} userId
 * @param {number} depositAddressId
 * @param {string} txSignature The Solana transaction signature.
 * @param {bigint} amountLamports The amount deposited.
 * @returns {Promise<{success: boolean, depositId?: number, error?: string, alreadyProcessed?: boolean}>} `alreadyProcessed:true` if conflict occurred.
 */
async function recordConfirmedDeposit(client, userId, depositAddressId, txSignature, amountLamports) {
    userId = String(userId);
    const logPrefix = `[DB RecordDeposit User ${userId} TX ${txSignature.slice(0, 6)}..]`;
    const query = `
        INSERT INTO deposits (user_id, deposit_address_id, tx_signature, amount_lamports, status, created_at, processed_at)
        VALUES ($1, $2, $3, $4, 'confirmed', NOW(), NOW())
        ON CONFLICT (tx_signature) DO NOTHING
        RETURNING id;
    `;
    try {
        const res = await queryDatabase(query, [userId, depositAddressId, txSignature, BigInt(amountLamports)], client);
        if (res.rowCount > 0 && res.rows[0].id) {
            const depositId = res.rows[0].id;
            return { success: true, depositId: depositId };
        } else {
            // ON CONFLICT triggered or insert failed strangely
            console.warn(`${logPrefix} Deposit TX ${txSignature} potentially already processed (ON CONFLICT or no rows returned). Verifying...`);
            const existing = await queryDatabase('SELECT id FROM deposits WHERE tx_signature = $1', [txSignature], client);
            if (existing.rowCount > 0) {
                 console.log(`${logPrefix} Verified TX already exists in DB (ID: ${existing.rows[0].id}).`);
                 return { success: false, error: 'Deposit transaction already processed.', alreadyProcessed: true, depositId: existing.rows[0].id };
            } else {
                 console.error(`${logPrefix} Insert failed AND ON CONFLICT didn't find existing row. DB state inconsistent?`);
                 return { success: false, error: 'Failed to record deposit and conflict resolution failed.' };
            }
        }
    } catch (err) {
        console.error(`${logPrefix} Error inserting deposit record:`, err.message, err.code);
        if (err.code === '23505' && err.constraint === 'deposits_tx_signature_key') {
            console.error(`${logPrefix} Deposit TX ${txSignature} already processed (Direct unique constraint violation). Verifying...`);
             const existing = await queryDatabase('SELECT id FROM deposits WHERE tx_signature = $1', [txSignature], client);
             const existingId = existing.rows[0]?.id;
            return { success: false, error: 'Deposit transaction already processed.', alreadyProcessed: true, depositId: existingId };
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
    userId = String(userId);
    const logPrefix = `[DB GetNextDepositIndex User ${userId}]`;
    try {
        const res = await queryDatabase(
            'SELECT COUNT(*) as count FROM deposit_addresses WHERE user_id = $1',
            [userId] // Use default pool
        );
        const nextIndex = parseInt(res.rows[0]?.count || '0', 10);
        return nextIndex;
    } catch (err) {
        console.error(`${logPrefix} Error fetching count:`, err);
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
    userId = String(userId);
    const requestedAmount = BigInt(requestedAmountLamports);
    const fee = BigInt(feeLamports);
    const finalSendAmount = requestedAmount; // We send what user requested, fee is deducted separately from balance

    const query = `
        INSERT INTO withdrawals (user_id, requested_amount_lamports, fee_lamports, final_send_amount_lamports, recipient_address, status, created_at)
        VALUES ($1, $2, $3, $4, $5, 'pending', NOW())
        RETURNING id;
    `;
    try {
        const res = await queryDatabase(query, [userId, requestedAmount, fee, finalSendAmount, recipientAddress], pool);
        if (res.rowCount > 0 && res.rows[0].id) {
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
 * Can be called with or without an active transaction client. Handles idempotency.
 * @param {number} withdrawalId The ID of the withdrawal record.
 * @param {'processing' | 'completed' | 'failed'} status The new status.
 * @param {pg.PoolClient | null} [client=null] Optional DB client if within a transaction. Defaults to pool.
 * @param {string | null} [signature=null] The payout transaction signature (required for 'completed').
 * @param {string | null} [errorMessage=null] The error message (required for 'failed').
 * @returns {Promise<boolean>} True on successful update or if already in target state, false otherwise.
 */
async function updateWithdrawalStatus(withdrawalId, status, client = null, signature = null, errorMessage = null) {
    const db = client || pool;
    const logPrefix = `[UpdateWithdrawal ID:${withdrawalId} Status:${status}]`;

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

    query += ` WHERE id = $${valueCounter++} AND status NOT IN ('completed', 'failed') RETURNING id;`; // Prevent updates on terminal states
    values.push(withdrawalId);

    try {
        const res = await queryDatabase(query, values, db);
        if (res.rowCount > 0) {
            return true; // Update successful
        } else {
             // Check if it failed because it was *already* in the desired terminal state
             const currentStatusRes = await queryDatabase('SELECT status FROM withdrawals WHERE id = $1', [withdrawalId], db);
             const currentStatus = currentStatusRes.rows[0]?.status;
             if (currentStatus === status && (status === 'completed' || status === 'failed')) {
                  console.log(`${logPrefix} Record already in terminal state '${currentStatus}', treating update as successful no-op.`);
                  return true; // Idempotency: Already in the target state
             }
             console.warn(`${logPrefix} Failed to update status. Record not found or already in terminal state ('${currentStatus || 'NOT_FOUND'}')?`);
             return false; // Failed for other reasons
        }
    } catch (err) {
        console.error(`${logPrefix} Error updating status:`, err.message, err.code);
        if (err.code === '23505' && err.constraint === 'withdrawals_payout_tx_signature_key') {
             console.error(`${logPrefix} CRITICAL - Withdrawal TX Signature ${signature?.slice(0, 10)}... unique constraint violation. Already processed?`);
             return true; // Treat signature conflict as "already done"
        }
        return false; // Error occurred
    }
}

/** Fetches withdrawal details for processing. */
async function getWithdrawalDetails(withdrawalId) {
    try {
        const res = await queryDatabase('SELECT * FROM withdrawals WHERE id = $1', [withdrawalId]); // Use default pool
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
 * MUST be called within a transaction.
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
    userId = String(userId);
    chatId = String(chatId);
    const wagerAmount = BigInt(wagerAmountLamports);

    if (wagerAmount <= 0n) {
        return { success: false, error: 'Wager amount must be positive.' };
    }

    const query = `
        INSERT INTO bets (user_id, chat_id, game_type, bet_details, wager_amount_lamports, status, created_at, priority)
        VALUES ($1, $2, $3, $4, $5, 'active', NOW(), $6)
        RETURNING id;
    `;
    try {
        const validBetDetails = (betDetails && typeof betDetails === 'object' && Object.keys(betDetails).length > 0) ? betDetails : null; // Store null if empty
        const res = await queryDatabase(query, [userId, chatId, gameType, validBetDetails, wagerAmount, priority], client);
        if (res.rowCount > 0 && res.rows[0].id) {
            return { success: true, betId: res.rows[0].id };
        } else {
            return { success: false, error: 'Failed to insert bet record (no ID returned).' };
        }
    } catch (err) {
        console.error(`[DB CreateBet] Error for user ${userId}, game ${gameType}:`, err.message, err.code);
        if (err.constraint === 'bets_wager_positive') {
             console.error(`[DB CreateBet] Positive wager constraint violation.`);
             return { success: false, error: 'Wager amount must be positive (DB check failed).' };
        }
        return { success: false, error: `Database error creating bet (${err.code || 'N/A'})` };
    }
}

/**
 * Updates a bet record upon completion of game logic.
 * MUST be called within a transaction. Handles idempotency.
 * @param {pg.PoolClient} client The active database client.
 * @param {number} betId The ID of the bet to update.
 * @param {'processing_game' | 'completed_win' | 'completed_loss' | 'completed_push' | 'error_game_logic'} status The new status.
 * @param {bigint | null} [payoutAmountLamports=null] Amount credited back (includes original wager for win/push). Required for completed statuses.
 * @returns {Promise<boolean>} True on successful update or if already in target state, false otherwise.
 */
async function updateBetStatus(client, betId, status, payoutAmountLamports = null) {
    const logPrefix = `[UpdateBetStatus ID:${betId} Status:${status}]`;
    let finalPayout = null;

    // Validate payout amount based on status
    if (status === 'completed_win' || status === 'completed_push') {
        if (payoutAmountLamports === null || BigInt(payoutAmountLamports) < 0n) {
             console.error(`${logPrefix} Invalid or missing payout amount (${payoutAmountLamports}) for status ${status}.`);
             return false;
        }
        finalPayout = BigInt(payoutAmountLamports);
    } else if (status === 'completed_loss' || status === 'error_game_logic') {
        finalPayout = 0n;
    } else if (status === 'processing_game') {
         finalPayout = null;
    } else {
         console.error(`${logPrefix} Invalid status provided: ${status}`);
         return false;
    }

    // Update only if current status allows transition
    const query = `
        UPDATE bets
        SET status = $1,
            payout_amount_lamports = $2,
            processed_at = CASE WHEN $1 LIKE 'completed_%' OR $1 = 'error_game_logic' THEN NOW() ELSE processed_at END
        WHERE id = $3 AND status IN ('active', 'processing_game')
        RETURNING id, status;
    `;
    try {
        const res = await queryDatabase(query, [status, finalPayout, betId], client);
        if (res.rowCount > 0) {
            return true; // Update successful
        } else {
            // Check if already in the desired state (idempotency)
            const currentStatusRes = await queryDatabase('SELECT status FROM bets WHERE id = $1', [betId], client);
            const currentStatus = currentStatusRes.rows[0]?.status;
            if (currentStatus === status && (status.startsWith('completed_') || status === 'error_game_logic')) {
                 console.log(`${logPrefix} Bet already in target state '${currentStatus}'. Treating as successful no-op.`);
                 return true;
            }
            console.warn(`${logPrefix} Failed to update status. Bet not found or already processed? Current status: ${currentStatus || 'NOT_FOUND'}.`);
            return false; // Failed for other reasons
        }
    } catch (err) {
        console.error(`${logPrefix} Error updating status:`, err.message);
        return false;
    }
}

/**
 * Checks if a specific completed bet ID is the user's first *ever* completed bet.
 * Reads from the database directly.
 * @param {string} userId
 * @param {number} betId The ID of the bet to check.
 * @returns {Promise<boolean>} True if it's the first completed bet, false otherwise or on error.
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
        const res = await queryDatabase(query, [userId]); // Use default pool
        return res.rows.length > 0 && res.rows[0].id === betId;
    } catch (err) {
        console.error(`[DB isFirstCompletedBet] Error checking for user ${userId}, bet ${betId}:`, err.message);
        return false;
    }
}

/** Fetches bet details by ID. */
async function getBetDetails(betId) {
    try {
        const res = await queryDatabase('SELECT * FROM bets WHERE id = $1', [betId]); // Use default pool
        if (res.rowCount === 0) return null;
        const details = res.rows[0];
        details.wager_amount_lamports = BigInt(details.wager_amount_lamports || '0');
        details.payout_amount_lamports = details.payout_amount_lamports !== null ? BigInt(details.payout_amount_lamports) : null;
        details.priority = parseInt(details.priority || '0', 10);
        // Ensure bet_details is parsed or null
        details.bet_details = details.bet_details || null;
        return details;
    } catch (err) {
        console.error(`[DB GetBetDetails] Error fetching bet ${betId}:`, err.message);
        return null;
    }
}


// --- Referral Payout Operations ---

/**
 * Records a pending referral payout. Can be called within a transaction or stand-alone.
 * Handles unique constraint for milestone payouts gracefully.
 * @param {string} referrerUserId
 * @param {string} refereeUserId
 * @param {'initial_bet' | 'milestone'} payoutType
 * @param {bigint} payoutAmountLamports
 * @param {number | null} [triggeringBetId=null] Bet ID for 'initial_bet' type.
 * @param {bigint | null} [milestoneReachedLamports=null] Wager threshold for 'milestone' type.
 * @param {pg.PoolClient | null} [client=null] Optional DB client if called within a transaction. Defaults to pool.
 * @returns {Promise<{success: boolean, payoutId?: number, error?: string, duplicate?: boolean}>} `duplicate:true` if it was a milestone conflict.
 */
async function recordPendingReferralPayout(referrerUserId, refereeUserId, payoutType, payoutAmountLamports, triggeringBetId = null, milestoneReachedLamports = null, client = null) {
    const db = client || pool;
    referrerUserId = String(referrerUserId);
    refereeUserId = String(refereeUserId);
    const payoutAmount = BigInt(payoutAmountLamports);
    const milestoneLamports = milestoneReachedLamports !== null ? BigInt(milestoneReachedLamports) : null;
    const betId = Number.isInteger(triggeringBetId) ? triggeringBetId : null;
    const logPrefix = `[DB RecordRefPayout Ref:${referrerUserId} Type:${payoutType}]`;

    const query = `
        INSERT INTO referral_payouts (
            referrer_user_id, referee_user_id, payout_type, payout_amount_lamports,
            triggering_bet_id, milestone_reached_lamports, status, created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, 'pending', NOW())
        ON CONFLICT ON CONSTRAINT idx_refpayout_unique_milestone DO NOTHING
        RETURNING id;
    `;
    const values = [
        referrerUserId, refereeUserId, payoutType, payoutAmount,
        betId, milestoneLamports
    ];
    try {
        const res = await queryDatabase(query, values, db);
        if (res.rows.length > 0 && res.rows[0].id) {
            const payoutId = res.rows[0].id;
            console.log(`${logPrefix} Recorded pending payout ID ${payoutId} (triggered by ${refereeUserId}).`);
            return { success: true, payoutId: payoutId };
        } else {
             if (payoutType === 'milestone') {
                 const checkExisting = await queryDatabase(`
                     SELECT id FROM referral_payouts
                     WHERE referrer_user_id = $1 AND referee_user_id = $2 AND payout_type = 'milestone' AND milestone_reached_lamports = $3
                 `, [referrerUserId, refereeUserId, milestoneLamports], db);
                 if (checkExisting.rowCount > 0) {
                     console.warn(`${logPrefix} Duplicate milestone payout attempt for milestone ${milestoneReachedLamports}. Ignored (ON CONFLICT).`);
                     return { success: false, error: 'Duplicate milestone payout attempt.', duplicate: true };
                 }
             }
            console.error(`${logPrefix} Failed to insert pending referral payout, RETURNING clause gave no ID (and not a handled milestone conflict).`);
            return { success: false, error: "Failed to insert pending referral payout." };
        }
    } catch (err) {
        console.error(`${logPrefix} Error recording pending payout (triggered by ${refereeUserId}):`, err.message, err.code);
        return { success: false, error: `Database error recording referral payout (${err.code || 'N/A'})` };
    }
}

/**
 * Updates referral payout status, signature, error message, timestamps.
 * Can be called with or without an active transaction client. Handles idempotency.
 * @param {number} payoutId The ID of the referral_payouts record.
 * @param {'processing' | 'paid' | 'failed'} status The new status.
 * @param {pg.PoolClient | null} [client=null] Optional DB client if within a transaction. Defaults to pool.
 * @param {string | null} [signature=null] The payout transaction signature (required for 'paid').
 * @param {string | null} [errorMessage=null] The error message (required for 'failed').
 * @returns {Promise<boolean>} True on successful update or if already in target state, false otherwise.
 */
async function updateReferralPayoutStatus(payoutId, status, client = null, signature = null, errorMessage = null) {
    const db = client || pool;
    const logPrefix = `[UpdateRefPayout ID:${payoutId} Status:${status}]`;

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

    query += ` WHERE id = $${valueCounter++} AND status NOT IN ('paid', 'failed') RETURNING id;`; // Prevent updates on terminal states
    values.push(payoutId);

    try {
        const res = await queryDatabase(query, values, db);
        if (res.rowCount > 0) {
            return true; // Update successful
        } else {
            // Check if already in the desired terminal state
            const currentStatusRes = await queryDatabase('SELECT status FROM referral_payouts WHERE id = $1', [payoutId], db);
            const currentStatus = currentStatusRes.rows[0]?.status;
             if (currentStatus === status && (status === 'paid' || status === 'failed')) {
                  console.log(`${logPrefix} Record already in terminal state '${currentStatus}', treating update as successful no-op.`);
                  return true; // Idempotency
             }
            console.warn(`${logPrefix} Failed to update status. Record not found or already in terminal state ('${currentStatus || 'NOT_FOUND'}')?`);
            return false; // Failed for other reasons
        }
    } catch (err) {
        console.error(`${logPrefix} Error updating status:`, err.message, err.code);
        if (err.code === '23505' && err.constraint === 'referral_payouts_payout_tx_signature_key') {
             console.error(`${logPrefix} CRITICAL - Referral Payout TX Signature ${signature?.slice(0, 10)}... unique constraint violation. Already processed?`);
             return true; // Treat signature conflict as "already done"
        }
        return false; // Error occurred
    }
}

/** Fetches referral payout details for processing. */
async function getReferralPayoutDetails(payoutId) {
   try {
        const res = await queryDatabase('SELECT * FROM referral_payouts WHERE id = $1', [payoutId]); // Use default pool
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
 * Reads directly from the database.
 * @param {string} userId The referrer's user ID.
 * @returns {Promise<bigint>} Total lamports earned and paid. Returns 0n on error.
 */
async function getTotalReferralEarnings(userId) {
    userId = String(userId);
    try {
        const result = await queryDatabase( // Use default pool
            `SELECT COALESCE(SUM(payout_amount_lamports), 0) AS total_earnings
             FROM referral_payouts
             WHERE referrer_user_id = $1 AND status = 'paid'`,
            [userId]
        );
        return BigInt(result.rows[0]?.total_earnings || '0');
    } catch (err) {
        console.error(`[DB getTotalReferralEarnings] Error calculating earnings for user ${userId}:`, err);
        return 0n;
    }
}

// --- End of Part 2 ---
// index.js - Part 3: Solana Utilities & Telegram Helpers
// --- VERSION: 3.1.6 --- (DB Init FK Fix, Sweep Fee Buffer, Sweep Retry Logic Prep, Bet Amount Buttons Prep)

// --- Assuming imports from Part 1 are available ---
// import { PublicKey, Keypair, SystemProgram, LAMPORTS_PER_SOL, sendAndConfirmTransaction, ComputeBudgetProgram, TransactionExpiredBlockheightExceededError, SendTransactionError, Connection } from '@solana/web3.js';
// import bs58 from 'bs58';
// import { createHash } from 'crypto';
// import * as crypto from 'crypto';
// import bip39 from 'bip39';
// import { derivePath } from 'ed25519-hd-key';
// import { bot } from './part1'; // Example if split
// import { solanaConnection } from './part1';
// import { userStateCache, walletCache, activeDepositAddresses, processedDepositTxSignatures, commandCooldown, MAX_PROCESSED_TX_CACHE_SIZE, WALLET_CACHE_TTL_MS, DEPOSIT_ADDR_CACHE_TTL_MS, USER_COMMAND_COOLDOWN_MS, PENDING_REFERRAL_TTL_MS } from './part1';
// import { telegramSendQueue } from './part1';

// --- General Utilities ---

/**
 * Creates a promise that resolves after a specified number of milliseconds.
 * @param {number} ms Milliseconds to sleep.
 * @returns {Promise<void>}
 */
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// --- Solana Utilities --

// Helper function to create safe index for BIP-44 hardened derivation
// Uses import { createHash } from 'crypto';
function createSafeIndex(userId) {
    // Hash the userId to fixed-size output
    const hash = createHash('sha256')
        .update(String(userId))
        .digest();

    // Take last 4 bytes and convert to UInt32 (always < 2^32)
    const index = hash.readUInt32BE(hash.length - 4);

    // Ensure it's below hardening threshold (2^31)
    return index % 2147483648; // 0 <= index < 2^31
}

/**
 * Derives a unique Solana keypair for deposits based on user ID and an index.
 * Uses BIP39 seed phrase and SLIP-10 path (m/44'/501'/safeIndex'/0'/addressIndex').
 * Uses ed25519-hd-key library (with safeIndex workaround).
 * Returns the public key and the 32-byte private key bytes needed for signing.
 * @param {string} userId The user's unique ID.
 * @param {number} addressIndex A unique index for this user's addresses (e.g., 0, 1, 2...).
 * @returns {Promise<{publicKey: PublicKey, privateKeyBytes: Uint8Array, derivationPath: string} | null>} Derived public key, private key bytes, and path, or null on error.
 */
async function generateUniqueDepositAddress(userId, addressIndex) {
    userId = String(userId); // Ensure string
    const logPrefix = `[Address Gen User ${userId} Index ${addressIndex}]`;
    // console.log(`${logPrefix} Starting address generation using ed25519-hd-key (with safeIndex)...`); // Less verbose

    try {
        const seedPhrase = process.env.DEPOSIT_MASTER_SEED_PHRASE;
        if (typeof seedPhrase !== 'string' || seedPhrase.length < 20 || !bip39.validateMnemonic(seedPhrase)) {
            console.error(`${logPrefix} CRITICAL: DEPOSIT_MASTER_SEED_PHRASE is invalid or empty/invalid at runtime!`);
            // Notify admin only once potentially rather than every time this fails if seed is missing
            // Consider adding a flag to prevent repeated notifications for the same root cause.
            await notifyAdmin(`🚨 CRITICAL: DEPOSIT\\_MASTER\\_SEED\\_PHRASE is missing or invalid\\. Cannot generate deposit addresses\\. (User: ${escapeMarkdownV2(userId)}, Index: ${addressIndex})`);
            return null;
        }

        if (typeof userId !== 'string' || userId.length === 0 || typeof addressIndex !== 'number' || addressIndex < 0 || !Number.isInteger(addressIndex)) {
            console.error(`${logPrefix} Invalid userId or addressIndex`, { userId, addressIndex });
            return null;
        }

        // 1. Get the master seed buffer from the mnemonic phrase
        const masterSeedBuffer = bip39.mnemonicToSeedSync(seedPhrase);
        if (!masterSeedBuffer || masterSeedBuffer.length === 0) {
            throw new Error("Failed to generate seed buffer from mnemonic.");
        }

        // 2. Derive the user-specific SAFE account path component (Hardened)
        const safeIndex = createSafeIndex(userId); // Uses the helper function

        // 3. Construct the *full* BIP44 derivation path for Solana
        const derivationPath = `m/44'/501'/${safeIndex}'/0'/${addressIndex}'`; // Use safeIndex, standard change level '0', final addressIndex

        // 4. Derive the seed using ed25519-hd-key
        const derivedSeed = derivePath(derivationPath, masterSeedBuffer.toString('hex')); // derivePath often expects hex seed
        const privateKeyBytes = derivedSeed.key; // The 'key' property holds the private key bytes

        if (!privateKeyBytes || privateKeyBytes.length !== 32) {
            throw new Error(`Derived private key bytes are invalid (length: ${privateKeyBytes?.length}) using ed25519-hd-key for path ${derivationPath}`);
        }

        // 5. Generate the Solana Keypair using Keypair.fromSeed()
        const keypair = Keypair.fromSeed(privateKeyBytes); // Generate the Keypair object
        const publicKey = keypair.publicKey; // Get the PublicKey object from the keypair
        // console.log(`${logPrefix} Solana Keypair generated. Public Key: ${publicKey.toBase58()}`); // Less verbose

        // 6. Return public key, path, and the 32-byte private key bytes
        return {
            publicKey: publicKey,
            privateKeyBytes: privateKeyBytes, // Raw 32-byte private key
            derivationPath: derivationPath
        };

    } catch (error) {
        console.error(`${logPrefix} Error during address generation using ed25519-hd-key: ${error.message}`);
        console.error(`${logPrefix} Error Stack: ${error.stack}`); // Keep stack trace logging
        // Notify admin on generation failure
        await notifyAdmin(`🚨 ERROR generating deposit address for User ${escapeMarkdownV2(userId)} Index ${addressIndex} (${escapeMarkdownV2(logPrefix)}): ${escapeMarkdownV2(error.message)}`);
        return null;
    }
}


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
        // Basic path validation
        if (!derivationPath || typeof derivationPath !== 'string' || !derivationPath.startsWith("m/44'/501'/")) {
             console.error(`${logPrefix} Invalid derivation path format provided.`);
             return null;
        }

        const seedPhrase = process.env.DEPOSIT_MASTER_SEED_PHRASE;
        if (typeof seedPhrase !== 'string' || seedPhrase.length < 20 || !bip39.validateMnemonic(seedPhrase)) {
             console.error(`${logPrefix} CRITICAL: DEPOSIT_MASTER_SEED_PHRASE is missing or invalid when trying to re-derive key!`);
             // Do NOT notifyAdmin excessively here if seed is missing, but log critical error.
             return null;
        }

        // 1. Get master seed buffer
        const masterSeedBuffer = bip39.mnemonicToSeedSync(seedPhrase);
        if (!masterSeedBuffer || masterSeedBuffer.length === 0) {
            throw new Error("Failed to generate seed buffer from mnemonic.");
        }

        // 2. Derive key using path and ed25519-hd-key
        const derivedSeed = derivePath(derivationPath, masterSeedBuffer.toString('hex'));
        const privateKeyBytes = derivedSeed.key;

        if (!privateKeyBytes || privateKeyBytes.length !== 32) {
            throw new Error(`Derived private key bytes are invalid (length: ${privateKeyBytes?.length})`);
        }

        // 3. Generate Solana Keypair
        const keypair = Keypair.fromSeed(privateKeyBytes);
        // console.log(`${logPrefix} Successfully re-derived keypair. Pubkey: ${keypair.publicKey.toBase58()}`); // Less verbose
        return keypair;

    } catch (error) {
        console.error(`${logPrefix} Error re-deriving keypair from path: ${error.message}`);
        // console.error(error.stack); // Optional stack
        // Notify admin only for unexpected derivation errors, not missing seed phrase
        if (!error.message.includes('DEPOSIT_MASTER_SEED_PHRASE')) {
             // Use console.error for admin alert here as notifyAdmin is async and this function is sync
             console.error(`[ADMIN ALERT] ${logPrefix} Unexpected error re-deriving key: ${error.message}`);
        }
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

    // Handle specific error instances first
    if (error instanceof TransactionExpiredBlockheightExceededError) {
        return true; // Blockhash expired, definitely retry with a new one
    }
    if (error instanceof SendTransactionError && error.cause) {
         // Check the underlying cause if it's a SendTransactionError
         return isRetryableSolanaError(error.cause);
    }

    // Check error properties
    const message = String(error.message || '').toLowerCase();
    const code = error?.code || error?.cause?.code; // Check nested code property
    const errorCode = error?.errorCode; // Sometimes used by libraries
    const status = error?.response?.status || error?.statusCode || error?.status; // HTTP status

    // Check status codes known to be retryable
    if ([429, 500, 502, 503, 504].includes(Number(status))) return true;

    // Check common retryable keywords/phrases in the message
    const retryableMessages = [
        'timeout', 'timed out', 'econnreset', 'esockettimedout', 'network error',
        'fetch', 'socket hang up', 'connection terminated', 'econnrefused', 'failed to fetch',
        'getaddrinfo enotfound', 'connection refused', 'connection reset by peer', 'etimedout',
        'transaction simulation failed', 'failed to simulate transaction', // Often temporary RPC issues
        'blockhash not found', 'slot leader does not match', 'node is behind',
        'transaction was not confirmed', 'block not available', 'block cleanout',
        'sending transaction', 'connection closed', 'load balancer error',
        'backend unhealthy', 'overloaded', 'proxy internal error', 'too many requests', // Covers 429 message check too
        'rate limit exceeded', // Explicit check
        'unknown block', // Sometimes seen when blockhash is recent but node is slightly behind
        'leader not ready',
        'heavily throttled', // From some RPC providers
    ];
    if (retryableMessages.some(m => message.includes(m))) return true;

    // Check common retryable error codes
    const retryableCodes = [
        'ETIMEDOUT', 'ECONNRESET', 'ENETUNREACH', 'EAI_AGAIN', 'ECONNABORTED',
        'ECONNREFUSED', 'UND_ERR_CONNECT_TIMEOUT', 'UND_ERR_HEADERS_TIMEOUT',
        'UND_ERR_BODY_TIMEOUT', 'FETCH_TIMEOUT', 'FETCH_ERROR',
        // Solana specific (some might overlap with HTTP status checks)
        '-32000', '-32001', '-32002', '-32003', '-32004', '-32005', // Generic temporary server errors
        // Note: Some RPCs might use string codes like '429', '503'
    ];
     // Check code property (case-insensitive for string codes)
     if (typeof code === 'string' && retryableCodes.includes(code.toUpperCase())) return true;
     if (typeof code === 'number' && retryableCodes.includes(code.toString())) return true;
     if (retryableCodes.includes(code)) return true; // Check original type
     // Check errorCode property similarly
     if (typeof errorCode === 'string' && retryableCodes.includes(errorCode.toUpperCase())) return true;
     if (typeof errorCode === 'number' && retryableCodes.includes(errorCode.toString())) return true;
     if (retryableCodes.includes(errorCode)) return true;

    // Check for specific error reasons if available
    if (error.reason === 'rpc_error' || error.reason === 'network_error') return true;

    // Check for explicit rate limit error structure from some RPC providers in nested data
     if (error?.data?.message?.toLowerCase().includes('rate limit exceeded') || error?.data?.message?.toLowerCase().includes('too many requests')) {
         return true;
     }

    // Default to false if none of the above conditions match
    return false;
}

/**
 * Sends SOL from a designated bot wallet to a recipient.
 * Handles priority fees and uses sendAndConfirmTransaction.
 * @param {PublicKey | string} recipientPublicKey The recipient's address.
 * @param {bigint} amountLamports The amount to send.
 * @param {'withdrawal' | 'referral'} payoutSource Determines which private key to use.
 * @returns {Promise<{success: boolean, signature?: TransactionSignature, error?: string, isRetryable?: boolean}>} Result object.
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
        await notifyAdmin(`🚨 CRITICAL: Missing private key env var ${escapeMarkdownV2(privateKeyEnvVar)} for payout source ${payoutSource}\\. Payout failed\\. Operation ID: ${escapeMarkdownV2(operationId)}`);
        return { success: false, error: errorMsg, isRetryable: false };
    }

    let payerWallet;
    try {
        payerWallet = Keypair.fromSecretKey(bs58.decode(privateKey));
    } catch (e) {
         const errorMsg = `Failed to decode private key ${keyTypeForLog} (${privateKeyEnvVar}): ${e.message}`;
         console.error(`[${operationId}] ❌ ERROR: ${errorMsg}`);
         await notifyAdmin(`🚨 CRITICAL: Failed to decode private key ${escapeMarkdownV2(keyTypeForLog)} (${escapeMarkdownV2(privateKeyEnvVar)}): ${escapeMarkdownV2(e.message)}\\. Payout failed\\. Operation ID: ${escapeMarkdownV2(operationId)}`);
         return { success: false, error: errorMsg, isRetryable: false };
    }

    const amountSOL = Number(amountToSend) / LAMPORTS_PER_SOL;
    console.log(`[${operationId}] Attempting to send ${amountSOL.toFixed(SOL_DECIMALS)} SOL from ${payerWallet.publicKey.toBase58()} to ${recipientPubKey.toBase58()} using ${keyTypeForLog} key...`);

    try {
        // 1. Get latest blockhash
        // console.log(`[${operationId}] Fetching latest blockhash...`); // Less verbose
        const { blockhash, lastValidBlockHeight } = await solanaConnection.getLatestBlockhash(DEPOSIT_CONFIRMATION_LEVEL); // Use standard confirmation level
        if (!blockhash || !lastValidBlockHeight) {
            throw new Error('Failed to get valid latest blockhash object.');
        }
        // console.log(`[${operationId}] Got blockhash: ${blockhash}, lastValidBlockHeight: ${lastValidBlockHeight}`); // Less verbose

        // 2. Calculate Priority Fee (Simplified: using fixed base/max for now)
        // TODO: Consider adding dynamic fee fetching here if needed via a utility
        const basePriorityFee = parseInt(process.env.PAYOUT_BASE_PRIORITY_FEE_MICROLAMPORTS, 10);
        const maxPriorityFee = parseInt(process.env.PAYOUT_MAX_PRIORITY_FEE_MICROLAMPORTS, 10);
        // Ensure fee is at least 1 and clamped by max
        let priorityFeeMicroLamports = Math.max(1, Math.min(basePriorityFee, maxPriorityFee));
        const computeUnitLimit = parseInt(process.env.PAYOUT_COMPUTE_UNIT_LIMIT, 10);
        // console.log(`[${operationId}] Using Compute Unit Limit: ${computeUnitLimit}, Priority Fee: ${priorityFeeMicroLamports} microLamports`); // Less verbose

        // 3. Create Transaction
        // console.log(`[${operationId}] Creating transaction...`); // Less verbose
        const transaction = new Transaction({
            recentBlockhash: blockhash,
            feePayer: payerWallet.publicKey,
            // lastValidBlockHeight added via sendAndConfirm options
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
        // console.log(`[${operationId}] Transaction created with ${transaction.instructions.length} instructions.`); // Less verbose

        // 4. Sign and Send Transaction
        console.log(`[${operationId}] Sending and confirming transaction (Commitment: ${DEPOSIT_CONFIRMATION_LEVEL})...`);
        const signature = await sendAndConfirmTransaction(
            solanaConnection, // Use the RateLimitedConnection instance
            transaction,
            [payerWallet], // Signer
            {
                commitment: DEPOSIT_CONFIRMATION_LEVEL, // Final confirmation level
                skipPreflight: false, // Keep preflight check
                preflightCommitment: DEPOSIT_CONFIRMATION_LEVEL, // Use same level for preflight
                lastValidBlockHeight: lastValidBlockHeight, // Important for expiration check
                // maxRetries: 0 // Let the underlying connection handle retries if configured? Or set explicitly? Let wrapper handle.
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
        let userFriendlyError = `Send/Confirm error: ${error.message}`; // Default
        const errorMsgLower = String(error.message || '').toLowerCase();

        // Improve user-facing error messages
        if (errorMsgLower.includes('insufficient lamports') || errorMsgLower.includes('insufficient funds')) {
            userFriendlyError = `Insufficient funds in the ${keyTypeForLog} payout wallet. Please check balance.`;
            // Notify admin immediately as this is critical and non-retryable by the bot
             await notifyAdmin(`🚨 CRITICAL: Insufficient funds in ${escapeMarkdownV2(keyTypeForLog)} wallet (${escapeMarkdownV2(payerWallet.publicKey.toBase58())}) for payout. Operation ID: ${escapeMarkdownV2(operationId)}`);
        } else if (error instanceof TransactionExpiredBlockheightExceededError || errorMsgLower.includes('blockhash not found') || errorMsgLower.includes('block height exceeded')) {
            userFriendlyError = 'Transaction expired (blockhash invalid). Retrying required.';
        } else if (errorMsgLower.includes('transaction was not confirmed') || errorMsgLower.includes('timed out waiting')) {
            userFriendlyError = `Transaction confirmation timeout. Status unclear, may succeed later. Manual check advised.`;
            // Potentially retryable, but requires careful handling (check TX status later if retrying)
        } else if (isRetryable) {
            userFriendlyError = `Temporary network/RPC error during send/confirm: ${error.message}`;
        }

        console.error(`[${operationId}] Failure details - Retryable: ${isRetryable}, User Friendly Error: "${userFriendlyError}"`);
        return { success: false, error: userFriendlyError, isRetryable: isRetryable };
    }
}


/**
 * Analyzes a fetched Solana transaction to find the SOL amount transferred *to* a target address.
 * Handles basic SystemProgram transfers by primarily analyzing balance changes.
 * NOTE: This function's accuracy might degrade with very complex transactions or future transaction versions.
 * @param {ConfirmedTransaction | TransactionResponse | null} tx The fetched transaction object.
 * @param {string} targetAddress The address receiving the funds.
 * @returns {{transferAmount: bigint, payerAddress: string | null}} Amount in lamports and the likely payer. Returns 0n if no positive transfer found or on error.
 */
function analyzeTransactionAmounts(tx, targetAddress) {
    const logPrefix = `[analyzeAmounts Addr:${targetAddress.slice(0, 6)}..]`;
    let transferAmount = 0n;
    let payerAddress = null;

    if (!tx || !targetAddress) {
        console.warn(`${logPrefix} Invalid input: TX or targetAddress missing.`);
        return { transferAmount: 0n, payerAddress: null };
    }
    try {
        new PublicKey(targetAddress);
    } catch (e) {
        console.warn(`${logPrefix} Invalid targetAddress format: ${targetAddress}`);
        return { transferAmount: 0n, payerAddress: null };
    }
    if (tx.meta?.err) {
        console.log(`${logPrefix} Transaction meta contains error. Returning zero amount. Error: ${JSON.stringify(tx.meta.err)}`);
        return { transferAmount: 0n, payerAddress: null };
    }

    // Prioritize using balance changes for accuracy
    if (tx.meta?.preBalances && tx.meta?.postBalances && tx.transaction?.message) {
        try {
             // Resolve account keys based on transaction version (v0 or legacy)
             let accountKeysList = [];
             const message = tx.transaction.message;
             if (message.staticAccountKeys) { // Likely legacy
                 accountKeysList = message.staticAccountKeys.map(k => k.toBase58());
             } else if (message.accountKeys) { // Likely version 0
                 if (tx.meta.loadedAddresses) { // v0 with LUTs
                     const staticKeys = message.accountKeys.map(k => k.toBase58());
                     const writableLut = tx.meta.loadedAddresses.writable || [];
                     const readonlyLut = tx.meta.loadedAddresses.readonly || [];
                     accountKeysList = [...staticKeys, ...writableLut, ...readonlyLut];
                 } else { // v0 without LUTs
                     accountKeysList = message.accountKeys.map(k => k.toBase58());
                 }
             } else {
                 throw new Error("Could not determine account keys from transaction message.");
             }

             // Ensure unique keys
             const uniqueAccountKeys = [...new Set(accountKeysList)];
             const targetIndex = uniqueAccountKeys.indexOf(targetAddress);

             if (targetIndex !== -1 && tx.meta.preBalances.length > targetIndex && tx.meta.postBalances.length > targetIndex) {
                 const preBalance = BigInt(tx.meta.preBalances[targetIndex]);
                 const postBalance = BigInt(tx.meta.postBalances[targetIndex]);
                 const balanceChange = postBalance - preBalance;

                 if (balanceChange > 0n) {
                     transferAmount = balanceChange;
                     // console.log(`${logPrefix} Positive balance change detected: ${transferAmount} lamports.`); // Less verbose

                     // Try to find payer heuristically (first signer is often the payer in simple transfers)
                     const header = message.header;
                     if (header && header.numRequiredSignatures > 0 && uniqueAccountKeys.length > 0) {
                         payerAddress = uniqueAccountKeys[0]; // Assume first signer is payer
                         // console.log(`${logPrefix} Identified likely payer (first signer): ${payerAddress}`); // Less verbose
                     } else {
                         // console.log(`${logPrefix} Could not determine likely payer (no signers?).`); // Less verbose
                     }
                 } else {
                     // console.log(`${logPrefix} No positive balance change found for target address.`); // Less verbose
                 }
             } else {
                 // console.log(`${logPrefix} Target address not found in resolved account keys or balance arrays incomplete.`); // Less verbose
             }
        } catch (e) {
            console.warn(`${logPrefix} Error analyzing balance changes: ${e.message}`);
            // Reset findings if analysis failed
            transferAmount = 0n;
            payerAddress = null;
        }
    } else {
        console.log(`${logPrefix} Balance change analysis skipped: Meta or message data missing/incomplete.`);
    }

    // Fallback: Check Log Messages (Less reliable)
    // Only use if balance change method yielded zero.
    if (transferAmount === 0n && tx.meta?.logMessages) {
        // console.log(`${logPrefix} Attempting analysis via log messages (Fallback)...`); // Less verbose
        const sysTransferRegex = /Program(?:.*)invoke \[\d+\]\s+Program log: Transfer: src=([1-9A-HJ-NP-Za-km-z]+) dst=([1-9A-HJ-NP-Za-km-z]+) lamports=(\d+)/;
        const simpleTransferRegex = /Program 11111111111111111111111111111111 success/; // Look for basic system program success

        // Prioritize finding the simplest system transfer log first
        let foundSimpleTransfer = false;
        for (const log of tx.meta.logMessages) {
            if(simpleTransferRegex.test(log)) {
                foundSimpleTransfer = true;
                break;
            }
        }

        if(foundSimpleTransfer) {
            for (const log of tx.meta.logMessages) {
                 const match = log.match(sysTransferRegex);
                 if (match && match[2] === targetAddress) {
                     const potentialAmount = BigInt(match[3]);
                     if (potentialAmount > 0n) {
                         console.log(`${logPrefix} Found potential transfer via log message: ${potentialAmount} lamports from ${match[1]}`);
                         transferAmount = potentialAmount;
                         payerAddress = match[1];
                         break; // Assume first match is the relevant one for simple transfers
                     }
                 }
             }
        }
        // if (transferAmount === 0n) { console.log(`${logPrefix} Log message parsing did not find a definitive transfer.`); } // Less verbose
    }


    if (transferAmount > 0n) {
        console.log(`${logPrefix} Final analysis result: Found ${transferAmount} lamports transfer (likely from ${payerAddress || 'Unknown'}).`);
    } else {
        console.log(`${logPrefix} Final analysis result: No positive SOL transfer found.`);
    }
    return { transferAmount, payerAddress };
}


// --- Telegram Utilities ---

/**
 * Sends a message to all configured admin users via Telegram.
 * Ensures the input message is escaped for MarkdownV2.
 * Uses the telegramSendQueue for rate limiting.
 * @param {string} message The message text to send.
 */
async function notifyAdmin(message) {
    const adminIds = (process.env.ADMIN_USER_IDS || '').split(',')
        .map(id => id.trim())
        .filter(id => /^\d+$/.test(id)); // Ensure IDs are numeric strings

    if (adminIds.length === 0) {
        console.warn('[NotifyAdmin] No valid ADMIN_USER_IDS configured. Cannot send notification.');
        return;
    }

    const timestamp = new Date().toISOString();
    // Escape the user-provided message content FOR MarkdownV2
    const escapedMessage = escapeMarkdownV2(message);
    // Construct final message using MarkdownV2 syntax
    const fullMessage = `🚨 *BOT ALERT* 🚨\n\\[${escapeMarkdownV2(timestamp)}\\]\n\n${escapedMessage}`;

    console.log(`[NotifyAdmin] Sending alert: "${message.substring(0, 100)}..." to ${adminIds.length} admin(s).`);

    // Use Promise.allSettled to send to all admins even if some fail
    const results = await Promise.allSettled(
        adminIds.map(adminId =>
            safeSendMessage(adminId, fullMessage, { parse_mode: 'MarkdownV2' }) // Send with MarkdownV2
        )
    );

    results.forEach((result, index) => {
        if (result.status === 'rejected') {
            console.error(`[NotifyAdmin] Failed to send notification to admin ID ${adminIds[index]}:`, result.reason?.message || result.reason || 'Unknown error');
        }
    });
}


/**
 * Safely sends a Telegram message using the dedicated queue for rate limiting.
 * Handles potential errors and message length limits.
 * @param {string | number} chatId Target chat ID.
 * @param {string} text Message text.
 * @param {TelegramBot.SendMessageOptions} [options={}] Additional send options (e.g., parse_mode, reply_markup).
 * @returns {Promise<TelegramBot.Message | undefined>} The sent message object or undefined on failure.
 */
async function safeSendMessage(chatId, text, options = {}) {
    if (!chatId || typeof text !== 'string' || text.trim() === '') {
        console.error("[safeSendMessage] Invalid input:", { chatId, text: text?.substring(0, 50) + '...' });
        return undefined;
    }

    const MAX_LENGTH = 4096; // Telegram message length limit
    let messageToSend = text;
    if (messageToSend.length > MAX_LENGTH) {
        console.warn(`[safeSendMessage] Message for chat ${chatId} too long (${messageToSend.length} > ${MAX_LENGTH}), truncating.`);
        // Truncate carefully, try not to break Markdown/HTML entities if possible (basic truncation here)
        messageToSend = messageToSend.substring(0, MAX_LENGTH - 3) + "...";
    }

    // Add job to the dedicated Telegram send queue
    return telegramSendQueue.add(async () => {
          try {
              // console.log(`[TG Send Queue] Sending to ${chatId}: "${messageToSend.substring(0, 30)}..."`); // Optional debug log
              const sentMessage = await bot.sendMessage(chatId, messageToSend, options);
              return sentMessage;
          } catch (error) {
              console.error(`❌ Telegram send error to chat ${chatId}: Code: ${error.code} | ${error.message}`);
              if (error.response?.body) {
                  console.error(`   -> Response Body: ${JSON.stringify(error.response.body)}`);
                  // Handle specific blocking errors
                  const errorCode = error.response.body.error_code;
                  const description = error.response.body.description?.toLowerCase() || '';
                   if (errorCode === 403 || description.includes('blocked') || description.includes('kicked') || description.includes('deactivated')) {
                       console.warn(`[TG Send] Bot blocked/kicked/deactivated in chat ${chatId}.`);
                       // Consider adding logic here to mark user/chat as inactive in DB if needed
                   } else if (errorCode === 400 && description.includes('parse error')) {
                        console.error(`❌❌❌ Telegram Parse Error! Message text (first 100 chars): ${messageToSend.substring(0, 100)}`);
                        // Automatically notify admin about parse errors
                        await notifyAdmin(`🚨 Telegram Parse Error in \`safeSendMessage\`\nChatID: ${chatId}\nOptions: ${JSON.stringify(options)}\nError: ${escapeMarkdownV2(error.response.body.description)}\nOriginal Text \\(Truncated\\):\n\`\`\`\n${escapeMarkdownV2(text.substring(0, 500))}\n\`\`\``)
                            .catch(e => console.error("Failed to send admin notification about parse error:", e));
                   }
              }
              return undefined; // Indicate failure to send
          }
      }).catch(queueError => {
           // Handle errors adding the job to the queue itself (e.g., queue timeout)
           console.error(`❌ Error adding job to telegramSendQueue for chat ${chatId}:`, queueError);
           notifyAdmin(`🚨 ERROR adding job to telegramSendQueue for Chat ${chatId}: ${escapeMarkdownV2(queueError.message)}`).catch(()=>{});
           return undefined;
      });
}

/**
 * Escapes characters for Telegram MarkdownV2 parse mode.
 * @param {string | number | bigint | null | undefined} text Input text to escape.
 * @returns {string} Escaped text.
 */
const escapeMarkdownV2 = (text) => {
    if (text === null || typeof text === 'undefined') {
        return '';
    }
    const textString = String(text);
     // Characters to escape for MarkdownV2: _ * [ ] ( ) ~ ` > # + - = | { } . ! \
    return textString.replace(/([_*\[\]()~`>#+\-=|{}.!\\])/g, '\\$1');
};

/**
 * Escapes characters for Telegram HTML parse mode.
 * @param {string | number | bigint | null | undefined} text Input text to escape.
 * @returns {string} Escaped text.
 */
function escapeHtml(text) {
    if (text === null || typeof text === 'undefined') {
        return '';
    }
    const textString = String(text);
    return textString.replace(/&/g, "&amp;")
                     .replace(/</g, "&lt;")
                     .replace(/>/g, "&gt;")
                     .replace(/"/g, "&quot;")
                     .replace(/'/g, "&#039;");
}

/**
 * Gets a display name for a user in a chat, escaping it for MarkdownV2.
 * Prioritizes username, then first/last name, then fallback ID.
 * @param {string | number} chatId
 * @param {string | number} userId
 * @returns {Promise<string>} MarkdownV2 escaped display name.
 */
async function getUserDisplayName(chatId, userId) {
    try {
        // Use getChatMember which is generally more reliable than getUserProfilePhotos
        const member = await bot.getChatMember(chatId, userId);
        const user = member.user;
        let name = '';
        if (user.username) {
            name = `@${user.username}`; // Don't escape the '@' for usernames
        } else if (user.first_name) {
            name = user.first_name;
            if (user.last_name) {
                name += ` ${user.last_name}`;
            }
        } else {
            name = `User_${String(userId).slice(-4)}`; // Fallback
        }
        return escapeMarkdownV2(name); // Escape the final chosen name
    } catch (error) {
        console.warn(`[getUserDisplayName] Failed to get chat member for ${userId} in ${chatId}: ${error.message}. Falling back to ID.`);
        // Don't notify admin for this, just use fallback
        return escapeMarkdownV2(`User_${String(userId).slice(-4)}`);
    }
}


// --- Cache Utilities ---

/** Updates the wallet cache for a user with new data, resets TTL. */
function updateWalletCache(userId, data) {
    userId = String(userId);
    const existing = walletCache.get(userId) || {};
    // Ensure timestamp is updated even if data is the same
    const entryTimestamp = Date.now();
    const newData = { ...existing, ...data, timestamp: entryTimestamp };
    walletCache.set(userId, newData);

    // Set a timer to clear the entry after TTL
    // Clear any existing timer for this user first
    if (existing.timeoutId) {
        clearTimeout(existing.timeoutId);
    }
    const timeoutId = setTimeout(() => {
        const current = walletCache.get(userId);
        // Only delete if it's the exact same entry (based on timestamp)
        if (current && current.timestamp === entryTimestamp) {
            walletCache.delete(userId);
            // console.log(`[Cache Evict] Wallet cache for user ${userId} expired.`); // Optional log
        }
    }, WALLET_CACHE_TTL_MS);
     // Store timeoutId with the entry to allow clearing it if updated again
     newData.timeoutId = timeoutId;
     // Ensure the timeout doesn't keep the process alive if it's the only thing left
     if (timeoutId.unref) {
          timeoutId.unref();
     }
}

/** Gets data from the wallet cache if entry exists and is not expired. */
function getWalletCache(userId) {
    userId = String(userId);
    const entry = walletCache.get(userId);
    if (entry && Date.now() - entry.timestamp < WALLET_CACHE_TTL_MS) {
        return entry; // Return the whole entry (excluding timeoutId potentially)
    }
    // If entry exists but is expired, delete it
    if (entry) {
        if(entry.timeoutId) clearTimeout(entry.timeoutId); // Clear associated timer
        walletCache.delete(userId);
    }
    return undefined; // Not found or expired
}

/** Adds/Updates an active deposit address in the cache, sets expiry timer. */
function addActiveDepositAddressCache(address, userId, expiresAtTimestamp) {
    const existing = activeDepositAddresses.get(address);
    if (existing?.timeoutId) {
        clearTimeout(existing.timeoutId); // Clear old timer if updating
    }

    activeDepositAddresses.set(address, { userId: String(userId), expiresAt: expiresAtTimestamp });
    // console.log(`[Cache Update] Added/Updated deposit address ${address.slice(0,6)}.. for user ${userId} in cache. Expires: ${new Date(expiresAtTimestamp).toISOString()}`); // Less verbose

    const delay = expiresAtTimestamp - Date.now();
    if (delay > 0) {
        const timeoutId = setTimeout(() => {
            const current = activeDepositAddresses.get(address);
            // Only delete if it's the exact same entry we set the timer for
            if (current && current.userId === String(userId) && current.expiresAt === expiresAtTimestamp) {
                // console.log(`[Cache Eviction] Deposit address ${address.slice(0,6)}.. expired and removed from cache.`); // Less verbose
                activeDepositAddresses.delete(address);
            }
        }, delay);
        // Store timeoutId with the entry
        activeDepositAddresses.get(address).timeoutId = timeoutId;
         // Ensure timer doesn't keep process alive
         if (timeoutId.unref) {
             timeoutId.unref();
         }
    } else {
        // If already expired when added, remove immediately
        // console.log(`[Cache Update] Deposit address ${address.slice(0,6)}.. was already expired when added/updated. Removing.`); // Less verbose
        removeActiveDepositAddressCache(address); // Use the remove function to ensure timer cleared if somehow existed
    }
}

/** Gets an active deposit address entry from cache if it exists and hasn't expired. */
function getActiveDepositAddressCache(address) {
    const entry = activeDepositAddresses.get(address);
    const now = Date.now();
    if (entry && now < entry.expiresAt) {
        return entry; // Return the entry { userId, expiresAt, timeoutId }
    }
    // If entry exists but is expired, delete it
    if (entry) {
        if(entry.timeoutId) clearTimeout(entry.timeoutId); // Clear timer
        activeDepositAddresses.delete(address);
    }
    return undefined; // Not found or expired
}

/** Explicitly removes an address from the deposit cache and clears its timer. */
function removeActiveDepositAddressCache(address) {
    const entry = activeDepositAddresses.get(address);
    if (entry?.timeoutId) {
        clearTimeout(entry.timeoutId); // Clear timer before deleting
    }
    const deleted = activeDepositAddresses.delete(address);
    // if (deleted) { console.log(`[Cache Update] Explicitly removed deposit address ${address.slice(0,6)}.. from cache.`); } // Less verbose
    return deleted;
}

/** Adds a transaction signature to the processed deposit cache (LRU). */
function addProcessedDepositTx(signature) {
    if (processedDepositTxSignatures.has(signature)) return; // Already exists

    // Simple LRU: If cache is full, delete the oldest entry (first one added)
    if (processedDepositTxSignatures.size >= MAX_PROCESSED_TX_CACHE_SIZE) {
        const oldestSig = processedDepositTxSignatures.values().next().value;
        if (oldestSig) {
            processedDepositTxSignatures.delete(oldestSig);
        }
    }
    processedDepositTxSignatures.add(signature);
    // console.log(`[Cache Update] Added TX signature ${signature.slice(0,6)}.. to processed deposit cache. Size: ${processedDepositTxSignatures.size}/${MAX_PROCESSED_TX_CACHE_SIZE}`); // Less verbose
}

/** Checks if a transaction signature exists in the processed deposit cache. */
function hasProcessedDepositTx(signature) {
    return processedDepositTxSignatures.has(signature);
}


// --- Other Utilities ---

/** Generates a random referral code prefixed with 'ref_'. */
function generateReferralCode(length = 8) {
    const byteLength = Math.ceil(length / 2);
    const randomBytes = crypto.randomBytes(byteLength);
    const hexString = randomBytes.toString('hex').slice(0, length);
    return `ref_${hexString}`;
}


// --- End of Part 3 ---
// index.js - Part 4: Game Logic
// --- VERSION: 3.1.6 --- (DB Init FK Fix, Sweep Fee Buffer, Sweep Retry Logic Prep, Bet Amount Buttons Prep)

// --- Game Logic Implementations ---
// These functions simulate the core outcome of each game.
// They do NOT handle betting, balance updates, house edge application, or user notifications/animations.
// That logic resides in the game result handlers (Part 5a).

/**
 * Plays a coinflip game.
 * @param {string} choice 'heads' or 'tails'. Not used in simulation but good for context.
 * @returns {{ outcome: 'heads' | 'tails' }} The random result of the flip.
 */
function playCoinflip(choice) {
    // Simple 50/50 random outcome
    const outcome = Math.random() < 0.5 ? 'heads' : 'tails';
    // The handler in Part 5a will compare this outcome to the user's choice.
    return { outcome };
}

/**
 * Simulates a simple race game outcome by picking a winning lane number.
 * @param {number} [totalLanes=4] The number of lanes/competitors in the race.
 * @returns {{ winningLane: number }} The randomly determined winning lane number.
 */
function simulateRace(totalLanes = 4) {
    // Simple random winner lane selection
    if (totalLanes <= 0) totalLanes = 1; // Prevent errors with invalid input
    const winningLane = Math.floor(Math.random() * totalLanes) + 1;
    // The handler in Part 5a will map this lane to a horse/competitor name and compare with user's choice.
    return { winningLane };
}

/**
 * Simulates a simple slots game spin. Returns symbols and payout multiplier *before* house edge.
 * Symbols: Cherry (C), Lemon (L), Orange (O), Bell (B), Seven (7)
 * Payouts (example before edge): 777=100x, BBB=20x, OOO=10x, CCC=5x, Any two Cherries=2x
 * @returns {{ symbols: [string, string, string], payoutMultiplier: number }} The resulting symbols and the base payout multiplier.
 */
function simulateSlots() {
    // Define symbols and their relative weights for randomness
    const symbols = ['C', 'L', 'O', 'B', '7'];
    const weights = [5, 4, 3, 2, 1]; // Weighted probability: Cherry most common, 7 least common

    // Create a weighted array for easier random selection
    const weightedSymbols = [];
    symbols.forEach((symbol, index) => {
        for (let i = 0; i < weights[index]; i++) {
            weightedSymbols.push(symbol);
        }
    });

    const getRandomSymbol = () => weightedSymbols[Math.floor(Math.random() * weightedSymbols.length)];

    // Spin the reels
    const reel1 = getRandomSymbol();
    const reel2 = getRandomSymbol();
    const reel3 = getRandomSymbol();
    const resultSymbols = [reel1, reel2, reel3];

    // Determine base payout multiplier based on combinations
    let payoutMultiplier = 0;
    if (reel1 === '7' && reel2 === '7' && reel3 === '7') {
        payoutMultiplier = 100;
    } else if (reel1 === 'B' && reel2 === 'B' && reel3 === 'B') {
        payoutMultiplier = 20;
    } else if (reel1 === 'O' && reel2 === 'O' && reel3 === 'O') {
        payoutMultiplier = 10;
    } else if (reel1 === 'C' && reel2 === 'C' && reel3 === 'C') {
        payoutMultiplier = 5;
    } else {
        // Check for two cherries (can be improved for combinations, e.g., C C L vs L C C)
        const cherryCount = resultSymbols.filter(s => s === 'C').length;
        if (cherryCount >= 2) {
            payoutMultiplier = 2;
        }
    }

    // The handler in Part 5a will determine win/loss based on multiplier and apply house edge.
    return { symbols: resultSymbols, payoutMultiplier };
}


/**
 * Simulates a simplified Roulette spin (European style - 0 to 36).
 * Determines the winning number.
 * @returns {{ winningNumber: number }} The winning number (0-36).
 */
function simulateRouletteSpin() {
    // Generate a random number between 0 and 36 inclusive
    const winningNumber = Math.floor(Math.random() * 37);
    // The handler in Part 5a will take this number and check it against the user's specific bet(s).
    return { winningNumber };
}

// Helper function (could be moved to utils/Part 3 if used elsewhere)
// Determines the base payout multiplier for a given Roulette bet type and winning number.
// Note: This returns the multiplier for WINNINGS (e.g., 35 for straight, 1 for even money).
function getRoulettePayoutMultiplier(betType, betValue, winningNumber) {
    let payoutMultiplier = 0;

    const isRed = (num) => [1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36].includes(num);
    const isBlack = (num) => num !== 0 && !isRed(num);
    const isEven = (num) => num !== 0 && num % 2 === 0;
    const isOdd = (num) => num !== 0 && num % 2 !== 0;
    const isLow = (num) => num >= 1 && num <= 18;
    const isHigh = (num) => num >= 19 && num <= 36;

    switch (betType) {
        case 'straight': // Bet on a specific number
            if (betValue !== null && winningNumber === parseInt(betValue, 10)) {
                payoutMultiplier = 35; // Pays 35 to 1
            }
            break;
        case 'red':
            if (isRed(winningNumber)) {
                payoutMultiplier = 1; // Pays 1 to 1
            }
            break;
        case 'black':
            if (isBlack(winningNumber)) {
                payoutMultiplier = 1;
            }
            break;
        case 'even':
            if (isEven(winningNumber)) {
                payoutMultiplier = 1;
            }
            break;
        case 'odd':
            if (isOdd(winningNumber)) {
                payoutMultiplier = 1;
            }
            break;
        case 'low': // 1-18
            if (isLow(winningNumber)) {
                payoutMultiplier = 1;
            }
            break;
        case 'high': // 19-36
            if (isHigh(winningNumber)) {
                payoutMultiplier = 1;
            }
            break;
        // Add other bet types here (splits, streets, corners, dozens, columns) if implementing full Roulette
        default:
            // Unknown bet type, should be handled by caller
            console.warn(`[getRoulettePayoutMultiplier] Unknown bet type: ${betType}`);
            break;
    }
    return payoutMultiplier;
}


/**
 * Simulates a simplified game of Casino War. Ace is high. Returns raw comparison result.
 * @returns {{ result: 'win' | 'loss' | 'push', playerCard: string, dealerCard: string, playerRank: number, dealerRank: number }}
 */
function simulateWar() {
    const suits = ['H', 'D', 'C', 'S']; // Hearts, Diamonds, Clubs, Spades
    const ranks = ['2', '3', '4', '5', '6', '7', '8', '9', 'T', 'J', 'Q', 'K', 'A']; // T=10
    const deck = [];
    for (const suit of suits) {
        for (const rank of ranks) {
            deck.push(rank + suit);
        }
    }

    // Simple shuffle (Fisher-Yates variation)
    for (let i = deck.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [deck[i], deck[j]] = [deck[j], deck[i]]; // Swap elements
    }

    // Draw cards (ensure deck isn't empty, though highly unlikely with full deck)
    const playerCard = deck.pop() || '??';
    const dealerCard = deck.pop() || '??';

    const getRankValue = (card) => {
        if(card === '??') return 0;
        const rank = card.substring(0, card.length - 1); // Get rank part (e.g., 'K', 'A', '7')
        if (rank === 'A') return 14;
        if (rank === 'K') return 13;
        if (rank === 'Q') return 12;
        if (rank === 'J') return 11;
        if (rank === 'T') return 10;
        return parseInt(rank, 10); // Converts '2' through '9'
    };

    const playerRank = getRankValue(playerCard);
    const dealerRank = getRankValue(dealerCard);

    let result;
    if (playerRank > dealerRank) {
        result = 'win';
    } else if (playerRank < dealerRank) {
        result = 'loss';
    } else {
        // Simplified: In real War, this leads to "Going to War" or surrendering.
        // Here, we just treat it as a push (bet returned).
        result = 'push';
    }

    // The handler in Part 5a will use this result to calculate payout/update balance.
    return { result, playerCard, dealerCard, playerRank, dealerRank };
}

// --- End of Part 4 ---
// index.js - Part 5a: Telegram Message/Callback Handlers & Game Result Processing
// --- VERSION: 3.1.6 --- (DB Init FK Fix, Sweep Fee Buffer, Sweep Retry Logic Prep, Bet Amount Buttons, Game Animations)

// --- Assuming imports from Part 1, DB functions from Part 2, Utils from Part 3, Game Sim from Part 4 are available ---
// import { pool } from './part1';
// import { bot } from './part1';
// import { userStateCache, commandCooldown, pendingReferrals, PENDING_REFERRAL_TTL_MS, USER_COMMAND_COOLDOWN_MS, USER_STATE_TTL_MS, LAMPORTS_PER_SOL, SOL_DECIMALS, GAME_CONFIG, WITHDRAWAL_FEE_LAMPORTS, MIN_WITHDRAWAL_LAMPORTS, REFERRAL_INITIAL_BET_MIN_LAMPORTS, REFERRAL_MILESTONE_REWARD_PERCENT, REFERRAL_INITIAL_BONUS_TIERS } from './part1';
// import { updateUserBalanceAndLedger, createBetRecord, getUserBalance, getLinkedWallet, getUserWalletDetails, getUserByReferralCode, isFirstCompletedBet, updateBetStatus, createWithdrawalRequest, recordPendingReferralPayout, addPayoutJob, getTotalReferralEarnings, linkReferral, updateUserWagerStats, getBetDetails, getWithdrawalDetails, updateWithdrawalStatus as updateWithdrawalDbStatus, updateReferralPayoutStatus } from './part2'; // Renamed imported withdrawal update to avoid conflict
// import { safeSendMessage, escapeMarkdownV2, notifyAdmin, getUserDisplayName, addProcessedDepositTx, hasProcessedDepositTx, sleep } from './part3'; // Added sleep
// import { playCoinflip, simulateRace, simulateSlots, simulateWar, simulateRouletteSpin, getRoulettePayoutMultiplier } from './part4';
// import { PQueue } from 'p-queue'; // Already imported in Part 1
// Define commandHandlers, menuCommandHandlers, handleAdminCommand, _handleReferralChecks later in Part 5b

// --- Main Message Handler ---

/**
 * Processes incoming Telegram messages (commands or text input based on state).
 * Routes to appropriate handlers or state processors.
 * @param {TelegramBot.Message} msg The incoming message object.
 */
async function handleMessage(msg) {
    // Basic filtering
    if (!msg || !msg.from || msg.from.is_bot || !msg.chat || (!msg.text && !msg.location && !msg.contact)) {
        // console.log("[handleMessage] Ignoring irrelevant message type or bot message.");
        return;
    }

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const messageText = msg.text || '';
    const messageId = msg.message_id;
    const logPrefix = `[Msg ${messageId} User ${userId} Chat ${chatId}]`;
    // console.log(`${logPrefix} Received message: "${messageText.slice(0, 50)}..."`); // Optional: Log incoming

    // Cooldown Check
    const now = Date.now();
    if (commandCooldown.has(userId)) {
        const lastTime = commandCooldown.get(userId);
        if (now - lastTime < USER_COMMAND_COOLDOWN_MS) {
            // console.log(`${logPrefix} Cooldown active, ignoring message.`);
            return; // Silently ignore
        }
    }
    commandCooldown.set(userId, now);
    const cooldownTimeout = setTimeout(() => {
         if (commandCooldown.get(userId) === now) commandCooldown.delete(userId);
    }, USER_COMMAND_COOLDOWN_MS + 500);
    if(cooldownTimeout.unref) cooldownTimeout.unref(); // Allow process to exit if only timers remain

    try {
        // --- Check for Conversational State (User sent text when input expected) ---
        const currentState = userStateCache.get(userId);
        if (currentState?.state && currentState.state !== 'idle' && !messageText.startsWith('/')) {
            if (now - currentState.timestamp > USER_STATE_TTL_MS) {
                console.log(`${logPrefix} State cache expired for state ${currentState.state}. Clearing.`);
                userStateCache.delete(userId);
                await safeSendMessage(chatId, "Your previous action timed out\\. Please start again\\.", { parse_mode: 'MarkdownV2' });
                return;
            }

            // Route text input based on the expected state (defined in Part 5b)
            await routeStatefulInput(msg, currentState); // Refactored routing logic
            return; // Stop further processing if state handled input
        }
        // --- End State Check ---

        // --- Command Processing ---
        const commandMatch = messageText.trim().match(/^\/(\w+)(?:@\w+)?(?:\s+(.*))?$/s);
        if (!commandMatch) {
            // Ignore non-command, non-state messages in groups. Reply in private if idle.
            if (msg.chat.type === 'private' && (!currentState || currentState.state === 'idle')) {
                // console.log(`${logPrefix} Ignoring non-command text in private chat from idle user.`);
                // Maybe send a gentle prompt? Or stay silent. Let's stay silent for now.
                // await safeSendMessage(chatId, "Please use a command or select an option from the menu\\. Try /help or /start", { parse_mode: 'MarkdownV2' });
            }
            return;
        }

        const command = commandMatch[1].toLowerCase();
        const args = commandMatch[2]?.trim() || '';

        // Clear any pending conversational state if user issues a new command
        if (currentState && currentState.state !== 'idle') {
            console.log(`${logPrefix} New command '/${command}' received, clearing previous state '${currentState.state}'.`);
            userStateCache.delete(userId);
            // Maybe edit the previous message to remove buttons if applicable? Complicates things. Let timeout handle it.
        }

        // --- /start Referral Code Parsing ---
        if (command === 'start' && args) {
            const refCodeMatch = args.match(/^(ref_[a-f0-9]{8})$/i);
            if (refCodeMatch && refCodeMatch[1]) {
                const refCode = refCodeMatch[1];
                console.log(`${logPrefix} User started with referral code ${refCode}`);
                const refereeDetails = await getUserWalletDetails(userId);
                const referrer = await getUserByReferralCode(refCode);

                if (referrer && referrer.user_id === userId) {
                     await safeSendMessage(chatId, `👋 Welcome\\! You can't use your own referral code\\.`, { parse_mode: 'MarkdownV2' });
                } else if (refereeDetails?.referred_by_user_id) { // Check if already referred using DB details
                     await safeSendMessage(chatId, `👋 Welcome back\\! You have already been referred\\.`, { parse_mode: 'MarkdownV2' });
                } else if (referrer) {
                    pendingReferrals.set(userId, { referrerUserId: referrer.user_id, timestamp: Date.now() });
                    const referralTimeout = setTimeout(() => {
                        const pending = pendingReferrals.get(userId);
                        if (pending && pending.referrerUserId === referrer.user_id && Date.now() - pending.timestamp >= PENDING_REFERRAL_TTL_MS) {
                            console.log(`[Referral] Pending referral expired for User ${userId} (Referrer ${referrer.user_id})`);
                            pendingReferrals.delete(userId);
                        }
                    }, PENDING_REFERRAL_TTL_MS + 5000); // Check slightly after expiry
                    if(referralTimeout.unref) referralTimeout.unref(); // Allow node process to exit

                    await safeSendMessage(chatId, `👋 Welcome\\! Referral code \`${escapeMarkdownV2(refCode)}\` accepted\\. Your first deposit will complete the referral link\\.`, { parse_mode: 'MarkdownV2' });
                    console.log(`${logPrefix} Pending referral stored for user ${userId} referred by ${referrer.user_id}`);
                } else {
                     await safeSendMessage(chatId, `👋 Welcome\\! The referral code \`${escapeMarkdownV2(refCode)}\` seems invalid or expired\\.`, { parse_mode: 'MarkdownV2' });
                }
                // Proceed to show normal start menu regardless of referral outcome
                await handleStartCommand(msg, ''); // Call start handler without args now
                return; // Stop processing here as start handled referral *and* menu
            }
        }
        // --- End /start Referral Parsing ---

        // --- Command Handler Map ---
        // commandHandlers map will be defined later in Part 5b
        const handler = commandHandlers[command];

        if (handler) {
            if (command === 'admin') { // Special handling for admin command
                const adminIds = (process.env.ADMIN_USER_IDS || '').split(',').map(id=>id.trim()).filter(id => id);
                if (adminIds.includes(userId)) {
                    await handler(msg, args); // Execute admin command
                } else {
                    console.warn(`${logPrefix} Unauthorized attempt to use /admin`);
                    // Maybe send a silent reply or ignore
                }
            } else {
                 // Execute regular command handler
                 await handler(msg, args);
            }
        } else {
            // Handle unknown commands
            if (msg.chat.type === 'private') {
                await safeSendMessage(chatId, `❓ Unknown command: \`/${escapeMarkdownV2(command)}\`\\. Try /help or /start`, { parse_mode: 'MarkdownV2' });
            } else {
                 // Ignore unknown commands in groups silently
                 // console.log(`${logPrefix} Ignoring unknown command '/${command}' in group chat.`);
            }
        }
    } catch (error) {
        console.error(`${logPrefix} Error processing message/command:`, error);
        userStateCache.delete(userId); // Clear state on error

        // Notify admin on unexpected handler errors
        await notifyAdmin(`🚨 ERROR in handleMessage for User ${userId} (${escapeMarkdownV2(logPrefix)}):\n${escapeMarkdownV2(error.message)}\nStack: ${escapeMarkdownV2(error.stack?.substring(0, 500))}`);

        try {
            await safeSendMessage(chatId, "⚠️ An unexpected error occurred processing your request\\. Please try again or contact support if the issue persists\\.", { parse_mode: 'MarkdownV2'});
        } catch (tgError) {
            console.error(`${logPrefix} Failed to send error message to user:`, tgError);
        }
    }
}


// --- Callback Query Handler ---

/**
 * Processes incoming Telegram callback queries (button presses).
 * Routes to appropriate handlers based on callback data.
 * @param {TelegramBot.CallbackQuery} callbackQuery The incoming callback query object.
 */
async function handleCallbackQuery(callbackQuery) {
    const userId = String(callbackQuery.from.id);
    const chatId = callbackQuery.message?.chat?.id; // Optional chaining
    const messageId = callbackQuery.message?.message_id; // Optional chaining
    const callbackData = callbackQuery.data;
    const callbackQueryId = callbackQuery.id;
    const logPrefix = `[Callback ${callbackQueryId} User ${userId} Chat ${chatId || 'N/A'}]`;

    if (!chatId || !messageId || !callbackData) {
        console.error(`${logPrefix} Received callback query with missing message/chat/data.`);
        // Try to answer anyway to dismiss the loading spinner on the button
        await bot.answerCallbackQuery(callbackQueryId, { text: "Error: Missing data" }).catch(e => console.warn(`${logPrefix} Failed to answer callback query with missing data: ${e.message}`));
        return;
    }

    // Answer silently first to make button feel responsive
    await bot.answerCallbackQuery(callbackQueryId).catch(e => console.warn(`${logPrefix} Failed to answer callback query silently: ${e.message}`));
    console.log(`${logPrefix} Received callback data: ${callbackData}`);

    try {
        const currentState = userStateCache.get(userId);
        const now = Date.now();

        // Check for expired state relevant to this interaction
        if (currentState && currentState.state !== 'idle' && now - currentState.timestamp > USER_STATE_TTL_MS) {
            console.log(`${logPrefix} State cache expired processing callback. State: ${currentState.state}`);
            userStateCache.delete(userId);
            // Try to remove buttons from the expired message
            bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: messageId }).catch(() => {/* Ignore if already gone */});
            await safeSendMessage(chatId, "Your previous action timed out\\. Please start again using the commands\\.", { parse_mode: 'MarkdownV2'});
            return;
        }

        const [action, ...params] = callbackData.split(':');

        // --- Route Callback Actions ---
        switch (action) {
            // --- Menu Actions ---
            case 'menu': {
                const menuItem = params[0];
                console.log(`${logPrefix} Handling menu callback for: ${menuItem}`);
                // menuCommandHandlers map defined later in Part 5b
                const menuHandler = menuCommandHandlers[menuItem];
                if (menuHandler) {
                    // Clear state before executing menu action
                    userStateCache.delete(userId);
                    // Mock a message object to pass to command handlers
                    const mockMsg = { ...callbackQuery.message, from: callbackQuery.from, text: `/${menuItem}`, chat: { id: chatId, type: callbackQuery.message.chat.type } };
                    await menuHandler(mockMsg, ''); // Pass empty args
                } else {
                    console.warn(`${logPrefix} Unknown menu action: ${menuItem}`);
                    await bot.answerCallbackQuery(callbackQueryId, { text: `Unknown action: ${menuItem}`, show_alert: true }).catch(() => {});
                }
                break;
            }

            // --- Bet Amount Selection ---
            case 'game_bet_amount': {
                const gameKey = params[0];
                const amountParam = params[1];
                const gameConfig = GAME_CONFIG[gameKey];

                if (!gameConfig) {
                     throw new Error(`Invalid game key '${gameKey}' in callback.`);
                }

                if (amountParam === 'custom') {
                    // Set state to await custom amount input
                    const newState = `awaiting_${gameKey}_amount`;
                    userStateCache.set(userId, { state: newState, chatId: String(chatId), messageId: messageId, timestamp: Date.now(), data: { gameKey } });
                    // Edit message to remove buttons and ask for input
                    await bot.editMessageText(
                        `🔢 Please type your custom bet amount for ${escapeMarkdownV2(gameConfig.name)} \\(in SOL\\):`,
                        { chat_id: chatId, message_id: messageId, reply_markup: undefined, parse_mode: 'MarkdownV2' }
                    ).catch(()=>{/*ignore edit error*/});
                    console.log(`${logPrefix} User selected custom amount for ${gameKey}. Awaiting input.`);
                } else {
                    // Predefined amount selected
                    const amountLamports = BigInt(amountParam); // Amount passed in lamports
                    const amountSOL = Number(amountLamports) / LAMPORTS_PER_SOL;
                    console.log(`${logPrefix} User selected predefined amount: ${amountSOL} SOL (${amountLamports} lamports) for ${gameKey}.`);

                    // Validate Min/Max Bet
                    if (amountLamports < gameConfig.minBetLamports || amountLamports > gameConfig.maxBetLamports) {
                        const min = (Number(gameConfig.minBetLamports) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
                        const max = (Number(gameConfig.maxBetLamports) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
                        await safeSendMessage(chatId, `⚠️ Bet amount ${escapeMarkdownV2(amountSOL.toFixed(SOL_DECIMALS))} SOL is outside the allowed range \\(${escapeMarkdownV2(min)} \\- ${escapeMarkdownV2(max)} SOL\\) for ${escapeMarkdownV2(gameConfig.name)}\\. Please choose another amount\\.`, { parse_mode: 'MarkdownV2' });
                        // Re-send the initial bet amount prompt (or handle differently?) Let's just inform user. Keep state cleared.
                        userStateCache.delete(userId);
                        // Try removing buttons from original message
                        await bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: messageId }).catch(()=>{});
                        return; // Stop processing
                    }

                    // Check Balance
                    const currentBalance = await getUserBalance(userId);
                    if (currentBalance < amountLamports) {
                        await safeSendMessage(chatId, `⚠️ Insufficient balance \\(${escapeMarkdownV2((Number(currentBalance) / LAMPORTS_PER_SOL).toFixed(4))} SOL\\) to place a ${escapeMarkdownV2(amountSOL.toFixed(SOL_DECIMALS))} SOL bet on ${escapeMarkdownV2(gameConfig.name)}\\. Use /deposit to add funds\\.`, { parse_mode: 'MarkdownV2' });
                        userStateCache.delete(userId);
                        await bot.editMessageReplyMarkup({ inline_keyboard: [] }, { chat_id: chatId, message_id: messageId }).catch(()=>{});
                        return; // Stop processing
                    }

                    // Amount and balance are valid, proceed to next step (choice or confirmation)
                    await proceedToGameStep(userId, String(chatId), messageId, gameKey, amountLamports); // Helper for next step
                }
                break;
            } // End case 'game_bet_amount'

            // --- Game Specific Choices / Confirmations ---
            // These are called AFTER amount is selected and validated via proceedToGameStep

            case 'cf_choice': { // Coinflip: Heads/Tails choice
                if (!currentState || currentState.state !== 'awaiting_cf_choice' || !currentState.data?.amountLamports || !currentState.data?.gameKey) { throw new Error("Invalid state for coinflip choice or missing wager amount/gameKey."); }
                const choice = params[0];
                if (choice !== 'heads' && choice !== 'tails') { throw new Error(`Invalid coinflip choice received: ${choice}`); }
                const cfWager = BigInt(currentState.data.amountLamports);
                const cfGameKey = currentState.data.gameKey;

                await bot.editMessageText(`🪙 ${escapeMarkdownV2(GAME_CONFIG[cfGameKey]?.name || 'Coinflip')}: You chose *${escapeMarkdownV2(choice)}* for ${escapeMarkdownV2((Number(cfWager)/LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL\\. Placing bet and flipping\\.\\.\\.`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: undefined }).catch(()=>{/*ignore if already gone*/});
                userStateCache.delete(userId); // Clear state before async game logic

                // Place the bet atomically first
                const { success: betPlaced, betId, error: betError } = await placeBet(userId, String(chatId), cfGameKey, { choice }, cfWager);
                if (!betPlaced || !betId) {
                    console.error(`${logPrefix} Failed to place coinflip bet: ${betError}`);
                    userStateCache.delete(userId); // Ensure state clear
                    // placeBet function already handles user notification on failure
                    if (betError !== 'Insufficient balance') {
                        await notifyAdmin(`🚨 ERROR placing Coinflip Bet for User ${userId} (${escapeMarkdownV2(logPrefix)}). Error: ${escapeMarkdownV2(betError)}`);
                    }
                    break; // Stop processing
                }

                // If bet placed, run the game handler (which includes DB updates and notifications)
                const gameSuccess = await handleCoinflipGame(userId, String(chatId), messageId, cfWager, choice, betId);
                if (gameSuccess) { await _handleReferralChecks(userId, betId, cfWager, logPrefix); } // Defined in 5b
                break;
            } // End case 'cf_choice'

            case 'race_choice': { // Race: Horse choice
                 // Assumes RACE_HORSES defined near handleRaceGame handler below
                 if (!currentState || currentState.state !== 'awaiting_race_choice' || !currentState.data?.amountLamports || !currentState.data?.gameKey) { throw new Error("Invalid state for race choice or missing wager amount/gameKey."); }
                 const chosenHorseName = params.join(':'); // Re-join if name had ':'
                 if (!RACE_HORSES.some(h => h.name === chosenHorseName)) { throw new Error(`Invalid horse choice received: ${chosenHorseName}`); }
                 const raceWager = BigInt(currentState.data.amountLamports);
                 const raceGameKey = currentState.data.gameKey;

                 await bot.editMessageText(`🐎 ${escapeMarkdownV2(GAME_CONFIG[raceGameKey]?.name || 'Race')}: You chose *${escapeMarkdownV2(chosenHorseName)}* for ${escapeMarkdownV2((Number(raceWager)/LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL\\. Placing bet and starting race\\!`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: undefined }).catch(()=>{});
                 userStateCache.delete(userId); // Clear state

                 const { success: betPlaced, betId, error: betError } = await placeBet(userId, String(chatId), raceGameKey, { horse: chosenHorseName }, raceWager);
                 if (!betPlaced || !betId) {
                     console.error(`${logPrefix} Failed to place race bet: ${betError}`);
                     userStateCache.delete(userId);
                     if (betError !== 'Insufficient balance') {
                         await notifyAdmin(`🚨 ERROR placing Race Bet for User ${userId} (${escapeMarkdownV2(logPrefix)}). Error: ${escapeMarkdownV2(betError)}`);
                     }
                     break;
                 }

                 // Run the animated game handler
                 const gameSuccess = await handleRaceGame(userId, String(chatId), messageId, raceWager, chosenHorseName, betId);
                 if (gameSuccess) { await _handleReferralChecks(userId, betId, raceWager, logPrefix); } // Defined in 5b
                 break;
            } // End case 'race_choice'

            case 'slots_confirm': { // Slots: Confirmation to spin
                 if (!currentState || currentState.state !== 'awaiting_slots_confirm' || !currentState.data?.amountLamports || !currentState.data?.gameKey) { throw new Error("Invalid state for slots confirmation or missing wager amount/gameKey."); }
                 const slotsWager = BigInt(currentState.data.amountLamports);
                 const slotsGameKey = currentState.data.gameKey;

                 await bot.editMessageText(`🎰 ${escapeMarkdownV2(GAME_CONFIG[slotsGameKey]?.name || 'Slots')}: Placing bet for ${escapeMarkdownV2((Number(slotsWager)/LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL and spinning\\!`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: undefined }).catch(()=>{});
                 userStateCache.delete(userId); // Clear state

                 const { success: betPlaced, betId, error: betError } = await placeBet(userId, String(chatId), slotsGameKey, {}, slotsWager);
                 if (!betPlaced || !betId) {
                     console.error(`${logPrefix} Failed to place slots bet: ${betError}`);
                     userStateCache.delete(userId);
                     if (betError !== 'Insufficient balance') {
                         await notifyAdmin(`🚨 ERROR placing Slots Bet for User ${userId} (${escapeMarkdownV2(logPrefix)}). Error: ${escapeMarkdownV2(betError)}`);
                     }
                     break;
                 }

                 const gameSuccess = await handleSlotsGame(userId, String(chatId), messageId, slotsWager, betId);
                 if (gameSuccess) { await _handleReferralChecks(userId, betId, slotsWager, logPrefix); } // Defined in 5b
                 break;
            } // End case 'slots_confirm'

            case 'war_confirm': { // War: Confirmation to deal
                 if (!currentState || currentState.state !== 'awaiting_war_confirm' || !currentState.data?.amountLamports || !currentState.data?.gameKey) { throw new Error("Invalid state for War confirmation or missing wager amount/gameKey."); }
                 const warWager = BigInt(currentState.data.amountLamports);
                 const warGameKey = currentState.data.gameKey;

                 await bot.editMessageText(`🃏 ${escapeMarkdownV2(GAME_CONFIG[warGameKey]?.name || 'Casino War')}: Placing bet for ${escapeMarkdownV2((Number(warWager)/LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL and dealing\\!`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: undefined }).catch(()=>{});
                 userStateCache.delete(userId); // Clear state

                 const { success: betPlaced, betId, error: betError } = await placeBet(userId, String(chatId), warGameKey, {}, warWager);
                 if (!betPlaced || !betId) {
                     console.error(`${logPrefix} Failed to place War bet: ${betError}`);
                     userStateCache.delete(userId);
                     if (betError !== 'Insufficient balance') {
                         await notifyAdmin(`🚨 ERROR placing War Bet for User ${userId} (${escapeMarkdownV2(logPrefix)}). Error: ${escapeMarkdownV2(betError)}`);
                     }
                     break;
                 }

                 const gameSuccess = await handleWarGame(userId, String(chatId), messageId, warWager, betId);
                 if (gameSuccess) { await _handleReferralChecks(userId, betId, warWager, logPrefix); } // Defined in 5b
                 break;
            } // End case 'war_confirm'

            // --- Withdrawal Confirmation ---
            case 'withdraw_confirm': {
                 if (!currentState || currentState.state !== 'awaiting_withdrawal_confirm' || !currentState.data?.amountLamports || !currentState.data?.recipientAddress) {
                     console.warn(`${logPrefix} Invalid state or missing data for withdrawal confirmation. State:`, currentState);
                     throw new Error("Invalid state for withdrawal confirmation.");
                 }
                 const { amountLamports: withdrawAmountStr, recipientAddress: withdrawAddress } = currentState.data;
                 const withdrawAmount = BigInt(withdrawAmountStr);
                 const fee = WITHDRAWAL_FEE_LAMPORTS; // Global constant
                 const finalSendAmount = withdrawAmount; // Amount requested by user
                 const totalDeduction = withdrawAmount + fee; // Total deducted from internal balance

                 console.log(`${logPrefix} User confirmed withdrawal of ${Number(finalSendAmount) / LAMPORTS_PER_SOL} SOL to ${withdrawAddress}`);
                 userStateCache.delete(userId); // Clear state

                 await bot.editMessageText(`💸 Processing withdrawal request\\.\\.\\. Please wait\\. This may take a minute\\.`, { chat_id: chatId, message_id: messageId, reply_markup: undefined, parse_mode: 'MarkdownV2' }).catch(()=>{});

                 // Final balance check before creating request and queuing job
                 const currentBalance = await getUserBalance(userId);
                 if (currentBalance < totalDeduction) {
                     await safeSendMessage(chatId, `⚠️ Withdrawal failed\\. Your current balance \\(${escapeMarkdownV2((Number(currentBalance) / LAMPORTS_PER_SOL).toFixed(4))} SOL\\) is insufficient for the requested amount plus fee \\(${escapeMarkdownV2((Number(totalDeduction) / LAMPORTS_PER_SOL).toFixed(4))} SOL\\)\\.`, { parse_mode: 'MarkdownV2' });
                     break;
                 }

                 // Create the withdrawal request record
                 const wdRequest = await createWithdrawalRequest(userId, withdrawAmount, fee, withdrawAddress);
                 if (!wdRequest.success || !wdRequest.withdrawalId) {
                      const errMsg = wdRequest.error || 'DB Error creating withdrawal request.';
                      await safeSendMessage(chatId, `⚠️ Failed to initiate withdrawal request in database\\. Please try again or contact support\\. Error: ${escapeMarkdownV2(errMsg)}`, { parse_mode: 'MarkdownV2' });
                      await notifyAdmin(`🚨 ERROR creating Withdrawal Request for User ${userId} (${escapeMarkdownV2(logPrefix)}). Error: ${escapeMarkdownV2(errMsg)}`);
                      break;
                 }
                 const withdrawalId = wdRequest.withdrawalId;

                 console.log(`${logPrefix} Withdrawal request ${withdrawalId} created in DB. Adding to payout queue.`);

                 // Add job to the payout queue (addPayoutJob defined in Part 6)
                 await addPayoutJob(
                     { type: 'payout_withdrawal', withdrawalId: withdrawalId, userId: userId },
                     parseInt(process.env.PAYOUT_JOB_RETRIES, 10),
                     parseInt(process.env.PAYOUT_JOB_RETRY_DELAY_MS, 10)
                 );
                 await safeSendMessage(chatId, `✅ Withdrawal request submitted for ${escapeMarkdownV2((Number(finalSendAmount) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL\\. You will be notified upon completion \\(check your wallet: \`${escapeMarkdownV2(withdrawAddress)}\`\\)\\.`, { parse_mode: 'MarkdownV2' });
                 break;
            } // End case 'withdraw_confirm'

            // --- Generic Cancel Action ---
            case 'cancel_action': {
                console.log(`${logPrefix} User cancelled action.`);
                const cancelledState = currentState?.state || 'unknown';
                userStateCache.delete(userId);
                await bot.editMessageText(`Action cancelled \\(${escapeMarkdownV2(cancelledState)}\\)\\.`, { chat_id: chatId, message_id: messageId, reply_markup: undefined, parse_mode: 'MarkdownV2' }).catch(()=>{});
                break;
            }

            default: {
                console.warn(`${logPrefix} Received unknown callback action: ${action}`);
                await bot.answerCallbackQuery(callbackQueryId, { text: "Unknown button action.", show_alert: false }).catch(() => {}); // Answer silently
                break;
            }
        } // End switch (action)

    } catch (error) {
        console.error(`${logPrefix} Error processing callback query data '${callbackData}':`, error);
        userStateCache.delete(userId); // Clear state on error

        // Notify admin on unexpected callback handler errors
        await notifyAdmin(`🚨 ERROR in handleCallbackQuery for User ${userId} (${escapeMarkdownV2(logPrefix)}) Data: ${escapeMarkdownV2(callbackData)}:\n${escapeMarkdownV2(error.message)}\nStack: ${escapeMarkdownV2(error.stack?.substring(0, 500))}`);

        try {
            // Attempt to edit the original message with a generic error
            await bot.editMessageText(`⚠️ An error occurred processing that action\\. Please try again\\. \n\`Error: ${escapeMarkdownV2(error.message)}\``, { chat_id: chatId, message_id: messageId, reply_markup: undefined, parse_mode: 'MarkdownV2' }).catch(async (editError) => {
                // If editing fails (e.g., message too old), send a new message
                console.warn(`${logPrefix} Failed to edit message with error, sending new message. Edit Error: ${editError.message}`);
                await safeSendMessage(chatId, `⚠️ An error occurred processing your last action\\. Please try again\\. Error: ${escapeMarkdownV2(error.message)}`, { parse_mode: 'MarkdownV2' });
            });
        } catch (sendError) {
            // Fallback if even sending a new message fails
            console.error(`${logPrefix} Critical: Failed to send error message to user after callback error:`, sendError);
        }
    }
}

// --- Helper to proceed to next game step (choice or confirmation) ---
/**
 * After a bet amount is chosen and validated, this transitions the state
 * and sends the appropriate next message (choice buttons or confirmation).
 */
async function proceedToGameStep(userId, chatId, messageId, gameKey, amountLamports) {
    const gameConfig = GAME_CONFIG[gameKey];
    const amountSOL = Number(amountLamports) / LAMPORTS_PER_SOL;
    const logPrefix = `[GameStep User ${userId} Game ${gameKey}]`;

    let nextState = '';
    let promptText = '';
    let keyboard = [];

    // Determine next step based on game
    if (gameKey === 'coinflip') {
        nextState = 'awaiting_cf_choice';
        promptText = `🪙 ${escapeMarkdownV2(gameConfig.name)}: Betting ${escapeMarkdownV2(amountSOL.toFixed(SOL_DECIMALS))} SOL\\. Choose Heads or Tails:`;
        keyboard = [
            [{ text: 'Heads', callback_data: `cf_choice:heads` }, { text: 'Tails', callback_data: `cf_choice:tails` }],
            [{ text: 'Cancel', callback_data: 'cancel_action' }]
        ];
    } else if (gameKey === 'race') {
        // Assumes RACE_HORSES is defined below near its handler
        nextState = 'awaiting_race_choice';
        promptText = `🐎 ${escapeMarkdownV2(gameConfig.name)}: Betting ${escapeMarkdownV2(amountSOL.toFixed(SOL_DECIMALS))} SOL\\. Choose your horse:`;
        const horseButtons = RACE_HORSES.map(h => ({ text: `${h.name} (${h.payoutMultiplier || '?' }x)`, callback_data: `race_choice:${h.name}` }));
        // Split buttons into rows of 2 for better layout
        for (let i = 0; i < horseButtons.length; i += 2) { keyboard.push(horseButtons.slice(i, i + 2)); }
        keyboard.push([{ text: 'Cancel', callback_data: 'cancel_action' }]);
    } else if (gameKey === 'slots') {
        nextState = 'awaiting_slots_confirm';
        promptText = `🎰 ${escapeMarkdownV2(gameConfig.name)}: Confirm bet of ${escapeMarkdownV2(amountSOL.toFixed(SOL_DECIMALS))} SOL\\?`;
        keyboard = [
            [{ text: '✅ Spin!', callback_data: 'slots_confirm' }],
            [{ text: '❌ Cancel', callback_data: 'cancel_action' }]
        ];
    } else if (gameKey === 'war') {
        nextState = 'awaiting_war_confirm';
        promptText = `🃏 ${escapeMarkdownV2(gameConfig.name)}: Confirm bet of ${escapeMarkdownV2(amountSOL.toFixed(SOL_DECIMALS))} SOL\\?`;
        keyboard = [
            [{ text: '✅ Deal!', callback_data: 'war_confirm' }],
            [{ text: '❌ Cancel', callback_data: 'cancel_action' }]
        ];
    }
    // Add Roulette case here if implemented
    // else if (gameKey === 'roulette') { ... }

    else {
        console.error(`${logPrefix} Unknown game key '${gameKey}' in proceedToGameStep.`);
        await safeSendMessage(chatId, `⚠️ Internal error starting game \`${escapeMarkdownV2(gameKey)}\`\\.`, { parse_mode: 'MarkdownV2'});
        userStateCache.delete(userId);
        return;
    }

    // Update user state
    userStateCache.set(userId, {
        state: nextState,
        chatId: chatId,
        messageId: messageId, // Store original message ID to edit
        data: { gameKey, amountLamports: amountLamports.toString() }, // Store game and amount
        timestamp: Date.now()
    });

    // Edit the previous message (which showed amount buttons) to show the next step
    await bot.editMessageText(promptText, {
        chat_id: chatId,
        message_id: messageId,
        reply_markup: { inline_keyboard: keyboard },
        parse_mode: 'MarkdownV2'
    }).catch(async (err) => {
         console.error(`${logPrefix} Failed to edit message for next step: ${err.message}`);
         // If edit fails, try sending a new message, although state might be confusing
         userStateCache.delete(userId); // Clear state if we can't reliably continue flow
         await safeSendMessage(chatId, `⚠️ Error displaying game options\\. Please start the game again\\.`, { parse_mode: 'MarkdownV2'});
    });
}


// --- Helper to Place Bet (Deduct Balance, Create Record) ---
/**
 * Handles the atomic process of deducting balance and creating a bet record.
 * Returns the bet ID if successful. Notifies user on balance failure.
 * @param {string} userId
 * @param {string} chatId
 * @param {string} gameType Internal game key (e.g., 'coinflip')
 * @param {object} betDetails Game-specific details (e.g., { choice: 'heads' })
 * @param {bigint} wagerAmount Lamports wagered
 * @returns {Promise<{success: boolean, betId?: number, error?: string}>}
 */
async function placeBet(userId, chatId, gameType, betDetails, wagerAmount) {
    const logPrefix = `[PlaceBet User ${userId} Game ${gameType}]`;
    let client = null;
    chatId = String(chatId); // Ensure string

    if (wagerAmount <= 0n) {
        console.error(`${logPrefix} Invalid wager amount: ${wagerAmount}`);
        return { success: false, error: 'Wager amount must be positive.' };
    }

    try {
        // console.log(`${logPrefix} Attempting to place bet. Wager: ${wagerAmount} lamports.`); // Less verbose
        client = await pool.connect();
        await client.query('BEGIN');
        // console.log(`${logPrefix} DB Transaction started.`); // Less verbose

        // 1. Deduct balance and add 'bet_placed' ledger entry
        const balanceUpdateResult = await updateUserBalanceAndLedger(
            client, userId, -wagerAmount, 'bet_placed', {}, `Bet on ${GAME_CONFIG[gameType]?.name || gameType}`
        );
        if (!balanceUpdateResult.success) {
            await client.query('ROLLBACK');
            console.error(`${logPrefix} Balance deduction failed: ${balanceUpdateResult.error}. Rolled back.`);
            const errorMsg = balanceUpdateResult.error === 'Insufficient balance'
                ? 'Insufficient balance'
                : 'Failed to update balance for bet.';
            const currentBalance = await getUserBalance(userId); // Fetch balance AFTER rollback attempt
            await safeSendMessage(chatId, `⚠️ Bet failed\\! ${escapeMarkdownV2(errorMsg)}\\. Your balance: ${escapeMarkdownV2((Number(currentBalance)/LAMPORTS_PER_SOL).toFixed(4))} SOL\\.`, { parse_mode: 'MarkdownV2'});
            return { success: false, error: errorMsg }; // Return specific error
        }
        // console.log(`${logPrefix} Balance updated successfully. New balance: ${balanceUpdateResult.newBalance}`); // Less verbose

        // 2. Create bet record
        const betRecordResult = await createBetRecord(client, userId, String(chatId), gameType, betDetails, wagerAmount);
        if (!betRecordResult.success || !betRecordResult.betId) {
            await client.query('ROLLBACK');
            const errorMsg = betRecordResult.error || 'Failed to record bet in database.';
            console.error(`${logPrefix} Failed to create bet record: ${errorMsg}. Rolled back.`);
            // Notify user about internal error, balance was deducted but rolled back
            await safeSendMessage(chatId, `⚠️ Bet failed due to internal error recording the bet\\. Your balance has not been charged\\. Please try again\\. Error: ${escapeMarkdownV2(errorMsg)}`, { parse_mode: 'MarkdownV2'});
            // Notify admin for non-obvious DB errors during bet creation
            if (!errorMsg.includes('Wager amount must be positive')) {
                 await notifyAdmin(`🚨 ERROR creating Bet Record for User ${userId} (${escapeMarkdownV2(logPrefix)}). Error: ${escapeMarkdownV2(errorMsg)}`);
            }
            return { success: false, error: errorMsg };
        }
        const betId = betRecordResult.betId;
        // console.log(`${logPrefix} Bet record created successfully. Bet ID: ${betId}`); // Less verbose

        // 3. Commit transaction
        await client.query('COMMIT');
        console.log(`${logPrefix} Successfully placed bet ${betId}. Wager: ${wagerAmount}. Transaction committed.`);
        return { success: true, betId: betId };

    } catch (error) {
        if (client) { try { await client.query('ROLLBACK'); console.error(`${logPrefix} Unexpected error occurred, transaction rolled back. Error:`, error); } catch (rbErr) { console.error(`${logPrefix} Unexpected error occurred AND rollback failed. Error:`, error, `Rollback Error:`, rbErr); }}
        else { console.error(`${logPrefix} Unexpected error placing bet (no client):`, error); }

        await notifyAdmin(`🚨 UNEXPECTED ERROR in placeBet for User ${userId} (${escapeMarkdownV2(logPrefix)}):\n${escapeMarkdownV2(error.message)}\nStack: ${escapeMarkdownV2(error.stack?.substring(0, 500))}`);
        await safeSendMessage(chatId, `⚠️ An unexpected error occurred while placing your bet\\. Please try again or contact support\\.`, { parse_mode: 'MarkdownV2'});
        return { success: false, error: 'Unexpected error placing bet.' };
    } finally {
        if (client) client.release();
    }
}


// --- Animated Game Result Handlers ---

// Define Race Horses constant (used by handleRaceGame and proceedToGameStep)
// Payout multipliers here are indicative for display, actual calculation uses house edge from GAME_CONFIG
const RACE_HORSES = [
    { name: "Solar Sprint", payoutMultiplier: 3 },
    { name: "Comet Tail", payoutMultiplier: 4 },
    { name: "Galaxy Gallop", payoutMultiplier: 5 },
    { name: "Nebula Speed", payoutMultiplier: 6 },
];

/**
 * Handles Coinflip game execution, DB updates, and animated user notification.
 * @param {string} userId
 * @param {string} chatId
 * @param {number} messageId - The ID of the message to edit for animation.
 * @param {bigint} wagerAmountLamports
 * @param {'heads' | 'tails'} choice - User's choice.
 * @param {number} betId - The ID of the bet record created by placeBet.
 * @returns {Promise<boolean>} True if game processed and DB committed successfully, false otherwise.
 */
async function handleCoinflipGame(userId, chatId, messageId, wagerAmountLamports, choice, betId) {
    const logPrefix = `[CoinflipGame Bet ${betId} User ${userId}]`;
    let client = null;
    const gameConfig = GAME_CONFIG.coinflip;
    const gameName = gameConfig.name;
    const wagerSOL = (Number(wagerAmountLamports) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);

    try {
        // --- Animation Step 1: Flipping ---
        await bot.editMessageText(`🪙 ${escapeMarkdownV2(gameName)} \\(${escapeMarkdownV2(wagerSOL)} SOL\\): Flipping the coin\\.\\.\\. 💨`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2' }).catch(()=>{});
        await sleep(1500); // Suspense delay

        // --- Simulate Game ---
        const { outcome } = playCoinflip(choice); // From Part 4
        const userResult = choice === outcome ? 'win' : 'loss';
        console.log(`${logPrefix} Result: ${outcome}. User choice: ${choice}. User ${userResult}.`);

        // --- Animation Step 2: Reveal Outcome ---
        const outcomeEmoji = outcome === 'heads' ? '👑' : '💲';
        await bot.editMessageText(`🪙 ${escapeMarkdownV2(gameName)} \\(${escapeMarkdownV2(wagerSOL)} SOL\\): ${outcomeEmoji} It's *${escapeMarkdownV2(outcome.toUpperCase())}*\\! ${outcomeEmoji}`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2' }).catch(()=>{});
        await sleep(1500); // Pause on result

        // --- Calculate Payout ---
        let finalPayoutLamports = 0n;
        let finalStatus = 'completed_loss';
        let winnings = 0n; // Store winnings separately for display

        if (userResult === 'win') {
            // Payout = Wager + Winnings. Winnings = Wager * (1 - HouseEdge)
            winnings = wagerAmountLamports - BigInt(Math.round(Number(wagerAmountLamports) * gameConfig.houseEdge)); // Apply edge to winnings portion (1x wager)
            if (winnings < 0n) winnings = 0n; // Ensure winnings aren't negative due to rounding/edge
            finalPayoutLamports = wagerAmountLamports + winnings; // Return wager + winnings
            finalStatus = 'completed_win';
            console.log(`${logPrefix} Win! Wager: ${wagerAmountLamports}, Winnings (after edge): ${winnings}, Total Payout: ${finalPayoutLamports}`);
        } else {
            finalPayoutLamports = 0n;
            finalStatus = 'completed_loss';
            console.log(`${logPrefix} Loss. Payout: 0`);
        }

        // --- DB Transaction ---
        client = await pool.connect();
        await client.query('BEGIN');

        const statusUpdated = await updateBetStatus(client, betId, finalStatus, finalPayoutLamports);
        if (!statusUpdated) throw new Error(`Failed to update bet status to ${finalStatus}`);

        if (finalPayoutLamports > 0n) {
            const ledgerNote = `${gameName} ${userResult}: ${outcome}`;
            const balanceResult = await updateUserBalanceAndLedger(client, userId, finalPayoutLamports, finalStatus === 'completed_win' ? 'bet_won' : 'bet_push_return', { betId }, ledgerNote);
            if (!balanceResult.success) throw new Error(`Failed to credit balance: ${balanceResult.error}`);
        }

        await client.query('COMMIT');
        console.log(`${logPrefix} DB updates committed.`);
        client.release(); client = null; // Release client after commit

        // --- Final Notification ---
        let notifyMsg = `🪙 *${escapeMarkdownV2(gameName)} Result* 🪙\n`
                      + `Outcome: ${outcomeEmoji} *${escapeMarkdownV2(outcome.toUpperCase())}* ${outcomeEmoji}\n\n`;
        if (userResult === 'win') {
            const payoutSOL = (Number(finalPayoutLamports) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
            const winAmountSOL = (Number(winnings) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
            notifyMsg += `You chose ${escapeMarkdownV2(choice)} and *WON*\\! 🎉\n`
                       + `Bet: ${escapeMarkdownV2(wagerSOL)} SOL\n`
                       + `Won: ${escapeMarkdownV2(winAmountSOL)} SOL\n`
                       + `Returned: *${escapeMarkdownV2(payoutSOL)} SOL*`;
        } else {
            notifyMsg += `You chose ${escapeMarkdownV2(choice)} and *LOST*\\. 😥\n`
                       + `Bet: ${escapeMarkdownV2(wagerSOL)} SOL`;
        }
        // Use editMessageText one last time for the final result
        await bot.editMessageText(notifyMsg, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2' }).catch(async (e) => {
             console.warn(`${logPrefix} Final edit failed (maybe message deleted?): ${e.message}. Sending as new message.`);
             await safeSendMessage(chatId, notifyMsg, { parse_mode: 'MarkdownV2'}); // Fallback to new message
        });

        return true; // Indicate success for referral checks

    } catch (error) {
        console.error(`${logPrefix} Error processing coinflip result:`, error);
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { console.error(`${logPrefix} Rollback failed:`, rbErr); } finally { client.release(); } }
        await notifyAdmin(`🚨 ERROR processing Coinflip Bet ${betId} User ${userId}:\n${escapeMarkdownV2(error.message)}`);
        // Try to edit the message to show an error
        await bot.editMessageText(`⚠️ Error processing your coinflip result\\. Please contact support if your balance seems incorrect\\. Bet ID: ${betId}\\. \nError: ${escapeMarkdownV2(error.message)}`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2'}).catch(async (e) => {
             console.warn(`${logPrefix} Failed to edit message with error: ${e.message}. Sending as new message.`);
             await safeSendMessage(chatId, `⚠️ Error processing your coinflip result \\(Bet ID: ${betId}\\)\\. Please contact support\\.`, { parse_mode: 'MarkdownV2'});
        });
        return false; // Indicate failure
    }
}

/**
 * Handles Race game execution, DB updates, and animated user notification.
 * @param {string} userId
 * @param {string} chatId
 * @param {number} messageId - The ID of the message to edit for animation.
 * @param {bigint} wagerAmountLamports
 * @param {string} chosenHorseName - User's chosen horse.
 * @param {number} betId - The ID of the bet record.
 * @returns {Promise<boolean>} True if game processed and DB committed successfully, false otherwise.
 */
async function handleRaceGame(userId, chatId, messageId, wagerAmountLamports, chosenHorseName, betId) {
    const logPrefix = `[RaceGame Bet ${betId} User ${userId}]`;
    let client = null;
    const gameConfig = GAME_CONFIG.race;
    const gameName = gameConfig.name;
    const wagerSOL = (Number(wagerAmountLamports) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
    const totalHorses = RACE_HORSES.length; // Use the defined horses

    try {
        // --- Animation Step 1: Starting ---
        await bot.editMessageText(`🐎 ${escapeMarkdownV2(gameName)} \\(${escapeMarkdownV2(wagerSOL)} SOL\\): And they're off\\! 🏁`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2' }).catch(()=>{});
        await sleep(1500);

        // --- Animation Step 2: Mid-race suspense ---
        const midRaceHorse = RACE_HORSES[Math.floor(Math.random() * totalHorses)].name;
        await bot.editMessageText(`🐎 ${escapeMarkdownV2(gameName)} \\(${escapeMarkdownV2(wagerSOL)} SOL\\): *${escapeMarkdownV2(midRaceHorse)}* makes a move on the outside\\!`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2' }).catch(()=>{});
        await sleep(2000);

        // --- Simulate Game ---
        const { winningLane } = simulateRace(totalHorses); // Get winning lane number (1-based)
        const winningHorse = RACE_HORSES[winningLane - 1]; // Get corresponding horse object
        if (!winningHorse) { throw new Error(`Simulation returned invalid winning lane ${winningLane}`); }
        const winningHorseName = winningHorse.name;
        const userResult = (chosenHorseName === winningHorseName) ? 'win' : 'loss';
        console.log(`${logPrefix} Winning horse: ${winningHorseName} (Lane ${winningLane}). User choice: ${chosenHorseName}. User ${userResult}.`);

        // --- Animation Step 3: Finish line ---
        await bot.editMessageText(`🐎 ${escapeMarkdownV2(gameName)} \\(${escapeMarkdownV2(wagerSOL)} SOL\\): Down the stretch\\! It's *${escapeMarkdownV2(winningHorseName)}* by a nose\\! 🏆`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2' }).catch(()=>{});
        await sleep(1500); // Pause on result

        // --- Calculate Payout ---
        let finalPayoutLamports = 0n;
        let finalStatus = 'completed_loss';
        let winnings = 0n;

        if (userResult === 'win') {
            const chosenHorseConfig = RACE_HORSES.find(h => h.name === chosenHorseName);
            const baseMultiplier = chosenHorseConfig?.payoutMultiplier || 1; // Fallback if somehow missing
            // Winnings = Wager * Multiplier * (1 - HouseEdge)
            winnings = (wagerAmountLamports * BigInt(baseMultiplier)) - BigInt(Math.round(Number(wagerAmountLamports * BigInt(baseMultiplier)) * gameConfig.houseEdge));
            if (winnings < 0n) winnings = 0n;
            finalPayoutLamports = wagerAmountLamports + winnings;
            finalStatus = 'completed_win';
            console.log(`${logPrefix} Win! Wager: ${wagerAmountLamports}, Multiplier: ${baseMultiplier}x, Winnings (after edge): ${winnings}, Total Payout: ${finalPayoutLamports}`);
        } else {
            finalPayoutLamports = 0n;
            finalStatus = 'completed_loss';
            console.log(`${logPrefix} Loss. Payout: 0`);
        }

        // --- DB Transaction ---
        client = await pool.connect();
        await client.query('BEGIN');

        const statusUpdated = await updateBetStatus(client, betId, finalStatus, finalPayoutLamports);
        if (!statusUpdated) throw new Error(`Failed to update bet status to ${finalStatus}`);

        if (finalPayoutLamports > 0n) {
             const ledgerNote = `${gameName} ${userResult}: ${winningHorseName} won`;
             const balanceResult = await updateUserBalanceAndLedger(client, userId, finalPayoutLamports, 'bet_won', { betId }, ledgerNote);
             if (!balanceResult.success) throw new Error(`Failed to credit balance: ${balanceResult.error}`);
        }

        await client.query('COMMIT');
        console.log(`${logPrefix} DB updates committed.`);
        client.release(); client = null;

        // --- Final Notification ---
        let notifyMsg = `🐎 *${escapeMarkdownV2(gameName)} Result* 🐎\n`
                      + `Winning Horse: 🏆 *${escapeMarkdownV2(winningHorseName)}*\n\n`;
        if (userResult === 'win') {
            const payoutSOL = (Number(finalPayoutLamports) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
            const winAmountSOL = (Number(winnings) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
             notifyMsg += `You chose the winner and *WON*\\! 🎉\n`
                       + `Bet: ${escapeMarkdownV2(wagerSOL)} SOL\n`
                       + `Won: ${escapeMarkdownV2(winAmountSOL)} SOL\n`
                       + `Returned: *${escapeMarkdownV2(payoutSOL)} SOL*`;
        } else {
            notifyMsg += `You chose ${escapeMarkdownV2(chosenHorseName)}\\. Better luck next time\\! 😥\n`
                       + `Bet: ${escapeMarkdownV2(wagerSOL)} SOL`;
        }
        await bot.editMessageText(notifyMsg, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2' }).catch(async (e) => {
             console.warn(`${logPrefix} Final edit failed: ${e.message}. Sending as new message.`);
             await safeSendMessage(chatId, notifyMsg, { parse_mode: 'MarkdownV2'});
        });

        return true; // Indicate success

    } catch (error) {
        console.error(`${logPrefix} Error processing race result:`, error);
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { console.error(`${logPrefix} Rollback failed:`, rbErr); } finally { client.release(); } }
        await notifyAdmin(`🚨 ERROR processing Race Bet ${betId} User ${userId}:\n${escapeMarkdownV2(error.message)}`);
        await bot.editMessageText(`⚠️ Error processing your race result\\. Please contact support if your balance seems incorrect\\. Bet ID: ${betId}\\. \nError: ${escapeMarkdownV2(error.message)}`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2'}).catch(async (e) => {
             console.warn(`${logPrefix} Failed to edit message with error: ${e.message}. Sending as new message.`);
             await safeSendMessage(chatId, `⚠️ Error processing your race result \\(Bet ID: ${betId}\\)\\. Please contact support\\.`, { parse_mode: 'MarkdownV2'});
        });
        return false; // Indicate failure
    }
}

/**
 * Handles Slots game execution, DB updates, and animated user notification.
 * @param {string} userId
 * @param {string} chatId
 * @param {number} messageId - The ID of the message to edit for animation.
 * @param {bigint} wagerAmountLamports
 * @param {number} betId - The ID of the bet record.
 * @returns {Promise<boolean>} True if game processed and DB committed successfully, false otherwise.
 */
async function handleSlotsGame(userId, chatId, messageId, wagerAmountLamports, betId) {
    const logPrefix = `[SlotsGame Bet ${betId} User ${userId}]`;
    let client = null;
    const gameConfig = GAME_CONFIG.slots;
    const gameName = gameConfig.name;
    const wagerSOL = (Number(wagerAmountLamports) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
    const slotEmojis = {'C':'🍒','L':'🍋','O':'🍊','B':'🔔','7':'7️⃣'}; // Emojis for display

    try {
        // --- Animation Step 1: Initial Spin ---
        await bot.editMessageText(`🎰 ${escapeMarkdownV2(gameName)} \\(${escapeMarkdownV2(wagerSOL)} SOL\\): Spinning\\! \\(❓\\|❓\\|❓\\)`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2' }).catch(()=>{});
        await sleep(1000);

        // --- Simulate Game ---
        const { symbols, payoutMultiplier } = simulateSlots(); // From Part 4
        const userResult = payoutMultiplier > 0 ? 'win' : 'loss';
        console.log(`${logPrefix} Result: ${symbols.join('')}. Multiplier: ${payoutMultiplier}x. User ${userResult}.`);

        // --- Animation Step 2: Reveal Reel 1 ---
        const sym1 = symbols[0];
        const e1 = slotEmojis[sym1] || sym1;
        await bot.editMessageText(`🎰 ${escapeMarkdownV2(gameName)} \\(${escapeMarkdownV2(wagerSOL)} SOL\\): Spinning\\! \\(${e1}\\|❓\\|❓\\)`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2' }).catch(()=>{});
        await sleep(1000);

        // --- Animation Step 3: Reveal Reel 2 ---
        const sym2 = symbols[1];
        const e2 = slotEmojis[sym2] || sym2;
        await bot.editMessageText(`🎰 ${escapeMarkdownV2(gameName)} \\(${escapeMarkdownV2(wagerSOL)} SOL\\): Spinning\\! \\(${e1}\\|${e2}\\|❓\\)`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2' }).catch(()=>{});
        await sleep(1000);

        // --- Animation Step 4: Reveal Reel 3 ---
        const sym3 = symbols[2];
        const e3 = slotEmojis[sym3] || sym3;
        const finalReels = `${e1}|${e2}|${e3}`;
        await bot.editMessageText(`🎰 ${escapeMarkdownV2(gameName)} \\(${escapeMarkdownV2(wagerSOL)} SOL\\): Result: \\(${finalReels}\\)`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2' }).catch(()=>{});
        await sleep(1500); // Pause on result

        // --- Calculate Payout ---
        let finalPayoutLamports = 0n;
        let finalStatus = 'completed_loss';
        let winnings = 0n;

        if (userResult === 'win') {
            // Winnings = Wager * Multiplier * (1 - HouseEdge)
            winnings = (wagerAmountLamports * BigInt(payoutMultiplier)) - BigInt(Math.round(Number(wagerAmountLamports * BigInt(payoutMultiplier)) * gameConfig.houseEdge));
            if (winnings < 0n) winnings = 0n;
            finalPayoutLamports = wagerAmountLamports + winnings;
            finalStatus = 'completed_win';
            console.log(`${logPrefix} Win! Wager: ${wagerAmountLamports}, Multiplier: ${payoutMultiplier}x, Winnings (after edge): ${winnings}, Total Payout: ${finalPayoutLamports}`);
        } else {
            finalPayoutLamports = 0n;
            finalStatus = 'completed_loss';
             console.log(`${logPrefix} Loss. Payout: 0`);
        }

        // --- DB Transaction ---
        client = await pool.connect();
        await client.query('BEGIN');

        const statusUpdated = await updateBetStatus(client, betId, finalStatus, finalPayoutLamports);
        if (!statusUpdated) throw new Error(`Failed to update bet status to ${finalStatus}`);

        if (finalPayoutLamports > 0n) {
            const ledgerNote = `${gameName} ${userResult}: ${symbols.join('')} (x${payoutMultiplier})`;
            const balanceResult = await updateUserBalanceAndLedger(client, userId, finalPayoutLamports, 'bet_won', { betId }, ledgerNote);
            if (!balanceResult.success) throw new Error(`Failed to credit balance: ${balanceResult.error}`);
        }

        await client.query('COMMIT');
        console.log(`${logPrefix} DB updates committed.`);
        client.release(); client = null;

        // --- Final Notification ---
        let notifyMsg = `🎰 *${escapeMarkdownV2(gameName)} Result* 🎰\n`
                      + `\\( ${finalReels} \\)\n\n`; // Display final reels again
        if (userResult === 'win') {
            const payoutSOL = (Number(finalPayoutLamports) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
            const winAmountSOL = (Number(winnings) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
            notifyMsg += `*WINNER*\\! 🎉 (${escapeMarkdownV2(payoutMultiplier)}x)\n`
                       + `Bet: ${escapeMarkdownV2(wagerSOL)} SOL\n`
                       + `Won: ${escapeMarkdownV2(winAmountSOL)} SOL\n`
                       + `Returned: *${escapeMarkdownV2(payoutSOL)} SOL*`;
        } else {
            notifyMsg += `Better luck next time\\! 😥\n`
                       + `Bet: ${escapeMarkdownV2(wagerSOL)} SOL`;
        }
        await bot.editMessageText(notifyMsg, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2' }).catch(async (e) => {
             console.warn(`${logPrefix} Final edit failed: ${e.message}. Sending as new message.`);
             await safeSendMessage(chatId, notifyMsg, { parse_mode: 'MarkdownV2'});
        });

        return true; // Indicate success

    } catch (error) {
        console.error(`${logPrefix} Error processing slots result:`, error);
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { console.error(`${logPrefix} Rollback failed:`, rbErr); } finally { client.release(); } }
        await notifyAdmin(`🚨 ERROR processing Slots Bet ${betId} User ${userId}:\n${escapeMarkdownV2(error.message)}`);
        await bot.editMessageText(`⚠️ Error processing your slots result\\. Please contact support if your balance seems incorrect\\. Bet ID: ${betId}\\. \nError: ${escapeMarkdownV2(error.message)}`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2'}).catch(async (e) => {
             console.warn(`${logPrefix} Failed to edit message with error: ${e.message}. Sending as new message.`);
             await safeSendMessage(chatId, `⚠️ Error processing your slots result \\(Bet ID: ${betId}\\)\\. Please contact support\\.`, { parse_mode: 'MarkdownV2'});
        });
        return false; // Indicate failure
    }
}

/**
 * Handles War game execution, DB updates, and animated user notification.
 * @param {string} userId
 * @param {string} chatId
 * @param {number} messageId - The ID of the message to edit for animation.
 * @param {bigint} wagerAmountLamports
 * @param {number} betId - The ID of the bet record.
 * @returns {Promise<boolean>} True if game processed and DB committed successfully, false otherwise.
 */
async function handleWarGame(userId, chatId, messageId, wagerAmountLamports, betId) {
    const logPrefix = `[WarGame Bet ${betId} User ${userId}]`;
    let client = null;
    const gameConfig = GAME_CONFIG.war;
    const gameName = gameConfig.name;
    const wagerSOL = (Number(wagerAmountLamports) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);

    try {
        // --- Animation Step 1: Dealing ---
        await bot.editMessageText(`🃏 ${escapeMarkdownV2(gameName)} \\(${escapeMarkdownV2(wagerSOL)} SOL\\): Dealing cards\\.\\.\\.`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2' }).catch(()=>{});
        await sleep(1000);

        // --- Simulate Game ---
        const { result, playerCard, dealerCard } = simulateWar(); // result is 'win', 'loss', or 'push'
        console.log(`${logPrefix} Player: ${playerCard}, Dealer: ${dealerCard}. Result: ${result}.`);

        // --- Animation Step 2: Reveal Player Card ---
        await bot.editMessageText(`🃏 ${escapeMarkdownV2(gameName)} \\(${escapeMarkdownV2(wagerSOL)} SOL\\):\nPlayer: *${escapeMarkdownV2(playerCard)}*`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2' }).catch(()=>{});
        await sleep(1500);

        // --- Animation Step 3: Reveal Dealer Card ---
        await bot.editMessageText(`🃏 ${escapeMarkdownV2(gameName)} \\(${escapeMarkdownV2(wagerSOL)} SOL\\):\nPlayer: *${escapeMarkdownV2(playerCard)}*\nDealer: *${escapeMarkdownV2(dealerCard)}*`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2' }).catch(()=>{});
        await sleep(1500); // Pause on result

        // --- Calculate Payout ---
        let finalPayoutLamports = 0n;
        let finalStatus;
        let winnings = 0n;
        let ledgerType;

        if (result === 'win') {
            winnings = wagerAmountLamports - BigInt(Math.round(Number(wagerAmountLamports) * gameConfig.houseEdge));
            if (winnings < 0n) winnings = 0n;
            finalPayoutLamports = wagerAmountLamports + winnings;
            finalStatus = 'completed_win';
            ledgerType = 'bet_won';
            console.log(`${logPrefix} Win! Wager: ${wagerAmountLamports}, Winnings (after edge): ${winnings}, Total Payout: ${finalPayoutLamports}`);
        } else if (result === 'push') {
            finalPayoutLamports = wagerAmountLamports; // Return original wager
            finalStatus = 'completed_push';
            ledgerType = 'bet_push_return';
             console.log(`${logPrefix} Push! Payout: ${finalPayoutLamports}`);
        } else { // Loss
            finalPayoutLamports = 0n;
            finalStatus = 'completed_loss';
            ledgerType = 'bet_lost'; // For consistency, although no balance change happens
            console.log(`${logPrefix} Loss. Payout: 0`);
        }

        // --- DB Transaction ---
        client = await pool.connect();
        await client.query('BEGIN');

        const statusUpdated = await updateBetStatus(client, betId, finalStatus, finalPayoutLamports);
        if (!statusUpdated) throw new Error(`Failed to update bet status to ${finalStatus}`);

        if (finalPayoutLamports > 0n) { // Only update balance if win or push
             const ledgerNote = `${gameName} ${result}: P:${playerCard} D:${dealerCard}`;
             const balanceResult = await updateUserBalanceAndLedger(client, userId, finalPayoutLamports, ledgerType, { betId }, ledgerNote);
             if (!balanceResult.success) throw new Error(`Failed to update balance: ${balanceResult.error}`);
        }

        await client.query('COMMIT');
        console.log(`${logPrefix} DB updates committed.`);
        client.release(); client = null;

        // --- Final Notification ---
        let notifyMsg = `🃏 *${escapeMarkdownV2(gameName)} Result* 🃏\n`
                      + `Player: *${escapeMarkdownV2(playerCard)}*\n`
                      + `Dealer: *${escapeMarkdownV2(dealerCard)}*\n\n`;

        if (result === 'win') {
            const payoutSOL = (Number(finalPayoutLamports) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
            const winAmountSOL = (Number(winnings) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
            notifyMsg += `You *WON*\\! 🎉\n`
                       + `Bet: ${escapeMarkdownV2(wagerSOL)} SOL\n`
                       + `Won: ${escapeMarkdownV2(winAmountSOL)} SOL\n`
                       + `Returned: *${escapeMarkdownV2(payoutSOL)} SOL*`;
        } else if (result === 'push') {
            notifyMsg += `It's a *PUSH* \\(Tie\\)\\! Your bet is returned\\. 🤝\n`
                       + `Bet: ${escapeMarkdownV2(wagerSOL)} SOL`;
        } else {
            notifyMsg += `You *LOST*\\. 😥\n`
                       + `Bet: ${escapeMarkdownV2(wagerSOL)} SOL`;
        }
        await bot.editMessageText(notifyMsg, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2' }).catch(async (e) => {
             console.warn(`${logPrefix} Final edit failed: ${e.message}. Sending as new message.`);
             await safeSendMessage(chatId, notifyMsg, { parse_mode: 'MarkdownV2'});
        });

        return true; // Indicate success

    } catch (error) {
        console.error(`${logPrefix} Error processing war result:`, error);
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { console.error(`${logPrefix} Rollback failed:`, rbErr); } finally { client.release(); } }
        await notifyAdmin(`🚨 ERROR processing War Bet ${betId} User ${userId}:\n${escapeMarkdownV2(error.message)}`);
        await bot.editMessageText(`⚠️ Error processing your War result\\. Please contact support if your balance seems incorrect\\. Bet ID: ${betId}\\. \nError: ${escapeMarkdownV2(error.message)}`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2'}).catch(async (e) => {
             console.warn(`${logPrefix} Failed to edit message with error: ${e.message}. Sending as new message.`);
             await safeSendMessage(chatId, `⚠️ Error processing your War result \\(Bet ID: ${betId}\\)\\. Please contact support\\.`, { parse_mode: 'MarkdownV2'});
        });
        return false; // Indicate failure
    }
}

// --- End of Game Result Handlers ---


// --- End of Part 5a ---
// index.js - Part 5b: Input Handlers, Command Handlers, Referral Checks
// --- VERSION: 3.1.6 --- (DB Init FK Fix, Sweep Fee Buffer, Sweep Retry Logic Prep, Bet Amount Buttons, Game Animations)

// --- Assuming imports, constants, functions from previous parts are available ---
// Includes GAME_CONFIG, STANDARD_BET_AMOUNTS_SOL, STANDARD_BET_AMOUNTS_LAMPORTS, etc.
// Includes safeSendMessage, escapeMarkdownV2, getUserBalance, placeBet, notifyAdmin, etc.

// --- Input Handling Router ---

/**
 * Routes incoming text messages based on the user's current conversational state.
 * Called by handleMessage when a non-command text message is received from a user in an active state.
 * @param {TelegramBot.Message} msg The user's message.
 * @param {object} currentState The user's current state object from userStateCache.
 */
async function routeStatefulInput(msg, currentState) {
    const userId = String(msg.from.id);
    const chatId = msg.chat.id;
    const logPrefix = `[StatefulInput State:${currentState.state} User ${userId}]`;
    console.log(`${logPrefix} Routing stateful input...`);

    switch (currentState.state) {
        // --- Custom Amount Input States ---
        // These states are set when user clicks the "Custom" bet amount button
        case 'awaiting_cf_amount':
        case 'awaiting_race_amount':
        case 'awaiting_slots_amount':
        case 'awaiting_war_amount':
        // Add case 'awaiting_roulette_amount': if implemented
            await handleCustomAmountInput(msg, currentState);
            break;

        // --- Other Input States ---
        case 'awaiting_withdrawal_address': // Currently seems unused in primary flow but kept for potential use
            await handleWithdrawalAddressInput(msg, currentState);
            break;

        case 'awaiting_roulette_bets': // Placeholder for complex roulette input
            await handleRouletteBetInput(msg, currentState);
            break;

        default:
            console.log(`${logPrefix} User in unhandled input state '${currentState.state}' received text: ${msg.text?.slice(0,50)}`);
            userStateCache.delete(userId); // Clear unexpected state
            await safeSendMessage(chatId, "Hmm, I wasn't expecting that input right now\\. Please use a command like /help or select an option\\.", { parse_mode: 'MarkdownV2' });
            break;
    }
}

// --- Input Handlers (Called by routeStatefulInput) ---

/**
 * Handles numeric amount input AFTER user clicks "Custom" amount button.
 * Validates the typed amount against game limits and balance, then proceeds.
 * @param {TelegramBot.Message} msg The user's message containing the amount.
 * @param {object} currentState The user's current state object from userStateCache.
 */
async function handleCustomAmountInput(msg, currentState) {
    const userId = String(msg.from.id);
    const chatId = msg.chat.id;
    const text = msg.text?.trim(); // Use optional chaining
    const stateType = currentState.state; // e.g., 'awaiting_cf_amount'
    const gameKey = currentState.data?.gameKey; // Get gameKey stored in state
    const originalMessageId = currentState.messageId; // Get original message ID to potentially edit later
    const logPrefix = `[Input State:${stateType} User ${userId}]`;

    if (!text) {
        await safeSendMessage(chatId, `⚠️ Please enter a valid bet amount\\.`, { parse_mode: 'MarkdownV2' });
        return; // Remain in state
    }
    console.log(`${logPrefix} Received custom amount input: "${text}" for game ${gameKey}`);

    if (!gameKey || !GAME_CONFIG[gameKey]) {
        console.error(`${logPrefix} Invalid or missing gameKey in state data.`);
        userStateCache.delete(userId);
        await safeSendMessage(chatId, `⚠️ Internal error: Could not determine the game\\. Please start again\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    const gameConfig = GAME_CONFIG[gameKey];
    const gameName = gameConfig.name;

    let amountSOL;
    try {
        amountSOL = parseFloat(text);
        if (isNaN(amountSOL) || amountSOL <= 0) {
            await safeSendMessage(chatId, `⚠️ Invalid amount\\. Please enter a positive number \\(e\\.g\\., 0\\.1\\) for your ${escapeMarkdownV2(gameName)} bet\\.`, { parse_mode: 'MarkdownV2' });
            console.log(`${logPrefix} Invalid amount entered.`);
            return; // Remain in state
        }
    } catch (e) {
        await safeSendMessage(chatId, `⚠️ Could not read amount\\. Please enter a number \\(e\\.g\\., 0\\.1\\) for your ${escapeMarkdownV2(gameName)} bet\\.`, { parse_mode: 'MarkdownV2' });
        console.log(`${logPrefix} Could not parse amount input.`);
        return; // Remain in state
    }

    const amountLamports = BigInt(Math.round(amountSOL * LAMPORTS_PER_SOL));
    console.log(`${logPrefix} Parsed amount: ${amountSOL} SOL (${amountLamports} lamports). Validating...`);

    // Validate Min/Max Bet
    if (amountLamports < gameConfig.minBetLamports || amountLamports > gameConfig.maxBetLamports) {
        const min = (Number(gameConfig.minBetLamports) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
        const max = (Number(gameConfig.maxBetLamports) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
        await safeSendMessage(chatId, `⚠️ ${escapeMarkdownV2(gameName)} bet amount must be between ${escapeMarkdownV2(min)} and ${escapeMarkdownV2(max)} SOL\\. Your entry: ${escapeMarkdownV2(amountSOL.toFixed(SOL_DECIMALS))} SOL\\. Please enter a valid amount\\.`, { parse_mode: 'MarkdownV2' });
        console.log(`${logPrefix} Bet amount out of range.`);
        return; // Remain in state
    }

    // Check Balance
    const currentBalance = await getUserBalance(userId);
    if (currentBalance < amountLamports) {
        await safeSendMessage(chatId, `⚠️ Insufficient balance \\(${escapeMarkdownV2((Number(currentBalance) / LAMPORTS_PER_SOL).toFixed(4))} SOL\\) to place a ${escapeMarkdownV2(amountSOL.toFixed(SOL_DECIMALS))} SOL bet on ${escapeMarkdownV2(gameName)}\\. Use /deposit to add funds\\.`, { parse_mode: 'MarkdownV2' });
        console.log(`${logPrefix} Insufficient balance for bet.`);
        userStateCache.delete(userId); // Clear state on failure
        return;
    }

    console.log(`${logPrefix} Custom amount ${amountSOL} SOL validated. Proceeding to game step.`);
    // Amount and balance are valid, proceed to the next game step (choice or confirmation)
    // We use the *original message ID* stored in the state when the user clicked "Custom"
    // to potentially edit that message (or send a new one if edit fails).
    await proceedToGameStep(userId, String(chatId), originalMessageId, gameKey, amountLamports); // Uses helper from Part 5a
}

/**
 * Handles withdrawal address input.
 * (Currently seems unused by primary /withdraw flow which uses /wallet command, but kept for completeness).
 * @param {TelegramBot.Message} msg The user's message containing the address.
 * @param {object} currentState The user's current state object from userStateCache.
 */
async function handleWithdrawalAddressInput(msg, currentState) {
    const userId = String(msg.from.id);
    const chatId = msg.chat.id;
    const address = msg.text?.trim();
    const logPrefix = `[Input State:awaiting_withdrawal_address User ${userId}]`;

    if (!currentState || currentState.state !== 'awaiting_withdrawal_address') {
        console.error(`${logPrefix} Invalid state reached when expecting withdrawal address.`);
        userStateCache.delete(userId);
        await safeSendMessage(chatId, `⚠️ An error occurred\\. Please start the withdrawal again with /withdraw\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }
     if (!address) {
        await safeSendMessage(chatId, `⚠️ Please enter a valid Solana wallet address\\.`, { parse_mode: 'MarkdownV2' });
        return; // Remain in state
    }
    console.log(`${logPrefix} Received address input: "${address}"`);

    // Validate address format
    try { new PublicKey(address); } catch (e) {
        await safeSendMessage(chatId, `⚠️ Invalid Solana address format: \`${escapeMarkdownV2(address)}\`\\. Please enter a valid address\\.`, { parse_mode: 'MarkdownV2' });
        console.log(`${logPrefix} Invalid address format.`);
        return; // Keep user in state to retry
    }

    // Prevent withdrawal to bot's own payout addresses
    const botKeys = [process.env.MAIN_BOT_PRIVATE_KEY, process.env.REFERRAL_PAYOUT_PRIVATE_KEY]
        .filter(Boolean)
        .map(pkStr => { try { return Keypair.fromSecretKey(bs58.decode(pkStr)).publicKey.toBase58(); } catch { return null; }})
        .filter(Boolean);
    if (botKeys.includes(address)) {
        await safeSendMessage(chatId, `⚠️ You cannot withdraw funds to the bot's own operational addresses\\. Please enter your personal wallet address\\.`, { parse_mode: 'MarkdownV2' });
        console.log(`${logPrefix} Attempt to withdraw to bot's own address.`);
        return; // Keep user in state to retry
    }

    // If this state *was* used, it would likely transition to confirm here.
    // Since the main flow uses /wallet, we just clear state and inform user.
    console.log(`${logPrefix} Processed address input, but this state is not used in the main /withdraw flow.`);
    userStateCache.delete(userId);
    await safeSendMessage(chatId, `Address received\\. However, please use \`/wallet <YourSolanaAddress>\` to set your address first, then use \`/withdraw\` to start the process\\.`, { parse_mode: 'MarkdownV2' });
}

/** Handles Roulette bet input (Placeholder - Requires dedicated interface). */
async function handleRouletteBetInput(msg, currentState) {
     const chatId = msg.chat.id;
     const userId = String(msg.from.id);
     console.log(`[Input State:awaiting_roulette_bets User ${userId}] Received input, but feature not fully implemented.`);
     await safeSendMessage(chatId, `Roulette betting via text is complex and not fully supported yet\\. A better interface is planned\\! Please /cancel and try another game\\.`, { parse_mode: 'MarkdownV2'});
     userStateCache.delete(userId);
}


// --- Command Handlers ---

/** Handles /start command - Shows main menu */
async function handleStartCommand(msg, args) {
    const chatId = msg.chat.id;
    const userId = String(msg.from.id);
    const firstName = msg.from.first_name || 'there';
    const logPrefix = `[Cmd /start User ${userId}]`;
    // console.log(`${logPrefix} Handling start command.`); // Less verbose

    await getUserBalance(userId); // Ensure user exists in DB
    userStateCache.set(userId, { state: 'idle', chatId: String(chatId), timestamp: Date.now() }); // Reset state

    const options = {
        reply_markup: {
            inline_keyboard: [
                 // Row 1: Common Games
                [ { text: '🪙 Coinflip', callback_data: 'menu:coinflip' }, { text: '🎰 Slots', callback_data: 'menu:slots' }, { text: '🃏 War', callback_data: 'menu:war' } ],
                // Row 2: Other Games
                [ { text: '🐎 Race', callback_data: 'menu:race' }, /*{ text: '⚪️ Roulette', callback_data: 'menu:roulette' } */ ], // Roulette disabled temporarily
                // Row 3: Funds
                [ { text: '💰 Deposit SOL', callback_data: 'menu:deposit' }, { text: '💸 Withdraw SOL', callback_data: 'menu:withdraw' } ],
                 // Row 4: Account & Help
                [ { text: '👤 Wallet / Balance', callback_data: 'menu:wallet' }, { text: '🤝 Referral Info', callback_data: 'menu:referral' }, { text: '❓ Help', callback_data: 'menu:help' } ]
            ]
        },
        parse_mode: 'MarkdownV2'
    };
    const welcomeMsg = `👋 Welcome, ${escapeMarkdownV2(firstName)}\\!\n\n`
                     + `I am a Solana\\-based gaming bot \\(v${escapeMarkdownV2(process.env.npm_package_version || '3.1.6')}\\)\\. Use the buttons below to play, manage funds, or get help\\.\n\n`
                     + `*Remember to gamble responsibly\\!*`;
    await safeSendMessage(chatId, welcomeMsg, options);
}

/** Handles /help command */
async function handleHelpCommand(msg, args) {
    const chatId = msg.chat.id;
    const userId = String(msg.from.id);
    // console.log(`[Cmd /help User ${userId}] Handling help command.`); // Less verbose
    const feeSOL = (Number(WITHDRAWAL_FEE_LAMPORTS) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
    const minWithdrawSOL = (Number(MIN_WITHDRAWAL_LAMPORTS) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
    const refRewardPercent = (REFERRAL_MILESTONE_REWARD_PERCENT * 100).toFixed(2); // Use constant from Part 1
    const botVersion = process.env.npm_package_version || '3.1.6'; // Get version if available

    // Meticulously escaped help message
    const helpMsg = `*Solana Gambles Bot Help* 🤖 \\(v${escapeMarkdownV2(botVersion)}\\)\n\n`
                  + `This bot uses a *custodial* model\\. Deposit SOL to play instantly using your internal balance\\. Winnings are added to your balance, and you can withdraw anytime\\. \n\n` // Escaped ., ., .
                  + `*How to Play:*\n`
                  + `1\\. Use \`/deposit\` to get a unique address & send SOL\\. Wait for confirmation \\(${escapeMarkdownV2(DEPOSIT_CONFIRMATION_LEVEL)}\\)\\. \n` // Escaped ., ., ()
                  + `2\\. Use \`/start\` or the game commands \\(\`/coinflip\`, \`/slots\`, etc\\.\\) or menu buttons\\. \n` // Escaped (), ., .
                  + `3\\. Choose your bet amount using the buttons, or select 'Custom' and type the amount\\. \n` // Escaped ., '
                  + `4\\. Confirm your bet & play\\!\n\n` // Escaped !
                  + `*Core Commands:*\n`
                  + `\\*/start* \\- Show main menu \n` // Escaped -, *
                  + `\\*/help* \\- Show this message \n` // Escaped -, *
                  + `\\*/wallet* \\- View balance & withdrawal address \n` // Escaped -, *
                  + `\\*/wallet <YourSolAddress>* \\- Set/update withdrawal address \n` // Escaped -, <>, *
                  + `\\*/deposit* \\- Get deposit address \n` // Escaped -, *
                  + `\\*/withdraw* \\- Start withdrawal process \n` // Escaped -, *
                  + `\\*/referral* \\- View your referral stats & link \n\n` // Escaped -, *
                  + `*Game Commands:* Initiate a game \\(e\\.g\\., \`/coinflip\`\\)\n\n` // Escaped (), .
                  + `*Referral Program:* 🤝\n`
                  + `Use \`/referral\` to get your code/link\\. New users start with \`/start <YourCode>\`\\. You earn SOL based on their first bet and their total wager volume \\(${escapeMarkdownV2(refRewardPercent)}\\% milestone bonus\\)\\. Rewards are paid to your linked wallet\\.\n\n` // Escaped ., <>, ., (), %, ., .
                  + `*Important Notes:*\n`
                  + `\\- Only deposit to addresses given by \`/deposit\`\\. Each address is temporary and for one deposit only\\. It expires in ${escapeMarkdownV2(process.env.DEPOSIT_ADDRESS_EXPIRY_MINUTES)} minutes\\. \n` // Escaped -, ., ., ., .
                  + `\\- You *must* set a withdrawal address using \`/wallet <address>\` before withdrawing\\. \n` // Escaped -, <>, .
                  + `\\- Withdrawals have a fee \\(${escapeMarkdownV2(feeSOL)} SOL\\) and minimum \\(${escapeMarkdownV2(minWithdrawSOL)} SOL\\)\\.\n` // Escaped -, (), (), .
                  + `\\- Please gamble responsibly\\. Contact support if you encounter issues\\.`; // Escaped -, ., .

    await safeSendMessage(chatId, helpMsg, { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}

/** Handles /wallet command (viewing or setting address) */
async function handleWalletCommand(msg, args) {
    const chatId = msg.chat.id;
    const userId = String(msg.from.id);
    const logPrefix = `[Cmd /wallet User ${userId}]`;
    const potentialNewAddress = args?.trim();

    if (potentialNewAddress) {
        // Attempt to set address
        console.log(`${logPrefix} Attempting to set withdrawal address to: ${potentialNewAddress}`);
        // linkUserWallet handles validation, DB updates, transaction, caching
        const result = await linkUserWallet(userId, potentialNewAddress);
        if (result.success) {
            await safeSendMessage(chatId, `✅ Withdrawal address successfully set/updated to: \`${escapeMarkdownV2(result.wallet)}\``, { parse_mode: 'MarkdownV2' });
        } else {
            await safeSendMessage(chatId, `❌ Failed to set withdrawal address\\. Error: ${escapeMarkdownV2(result.error)}`, { parse_mode: 'MarkdownV2' });
        }
    } else {
        // View wallet info
        // console.log(`${logPrefix} Displaying wallet info.`); // Less verbose
        // Ensure user exists & get balance first
        const userBalance = await getUserBalance(userId); // This handles ensuring user/balance records exist
        const userDetails = await getUserWalletDetails(userId); // Fetch other details

        let currentRefCode = userDetails?.referral_code;
        // If user exists but code is missing, try to generate/save it now
        if (userBalance !== null && userDetails && !currentRefCode) {
             console.log(`${logPrefix} User exists but lacks referral code. Generating and attempting to save.`);
             const genCode = generateReferralCode(); // Assumes Part 3 function
             let client = null;
             try {
                 client = await pool.connect(); await client.query('BEGIN');
                 // Attempt to set it - lock the row first
                 await queryDatabase('SELECT user_id FROM wallets WHERE user_id = $1 FOR UPDATE', [userId], client);
                 const updateRes = await queryDatabase(`UPDATE wallets SET referral_code = $1 WHERE user_id = $2 AND referral_code IS NULL RETURNING referral_code`, [genCode, userId], client);
                 if (updateRes.rowCount > 0) {
                     await client.query('COMMIT');
                     currentRefCode = updateRes.rows[0].referral_code;
                     console.log(`${logPrefix} Successfully generated and saved new referral code: ${currentRefCode}`);
                     updateWalletCache(userId, { referralCode: currentRefCode }); // Assumes Part 3 function
                 } else {
                     await client.query('ROLLBACK'); // Didn't update, maybe race condition? Fetch again.
                     console.log(`${logPrefix} Referral code update failed (maybe set concurrently?), fetching existing.`);
                     const refetch = await queryDatabase('SELECT referral_code FROM wallets WHERE user_id = $1', [userId]);
                     currentRefCode = refetch.rows[0]?.referral_code;
                 }
             } catch (e) {
                 if (client) try { await client.query('ROLLBACK'); } catch (rbErr) { /* ignore */ }
                 console.error(`${logPrefix} Error generating/saving referral code: ${e.message}`);
             } finally {
                 if (client) client.release();
             }
        }

        // Construct message
        let walletMsg = `*Your Wallet & Referral Info* 💼\n\n`
                      + `*Balance:* ${escapeMarkdownV2((Number(userBalance) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL\n`;
        if (userDetails?.external_withdrawal_address) {
            walletMsg += `*Withdrawal Address:* \`${escapeMarkdownV2(userDetails.external_withdrawal_address)}\`\n`;
        } else {
            walletMsg += `*Withdrawal Address:* Not Set \\(Use \`/wallet <YourSolAddress>\`\\)\n`; // Escaped (), <>
        }
        if (currentRefCode) {
            walletMsg += `*Referral Code:* \`${escapeMarkdownV2(currentRefCode)}\` \\(Share with friends\\!\n`; // Escaped ()!
        } else {
             walletMsg += `*Referral Code:* \`N/A\` \\(Try again or link wallet\\)\n`; // Escaped ()
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
    // console.log(`[Cmd /referral User ${userId}] Handling referral command.`); // Less verbose
    const userDetails = await getUserWalletDetails(userId);

    // Ensure user exists and has linked wallet before showing referral info
    if (!userDetails?.external_withdrawal_address) {
        await safeSendMessage(chatId, `❌ You need to link your wallet first using \`/wallet <YourSolAddress>\` before using the referral system\\. This ensures rewards can be paid out\\.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    // Ensure referral code exists (generate if missing via wallet logic)
    let refCode = userDetails.referral_code;
    if (!refCode) {
         console.warn(`[Cmd /referral User ${userId}] User has linked wallet but missing referral code. Attempting generation via wallet command logic.`);
         await handleWalletCommand(msg, ''); // Call /wallet view logic which includes generation attempt
         // After calling handleWalletCommand, the necessary info should be displayed or an error shown.
         // We return here because handleWalletCommand will send the relevant message.
         return;
    }

    const totalEarningsLamports = await getTotalReferralEarnings(userId);
    const totalEarningsSOL = (Number(totalEarningsLamports) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
    let botUsername = 'YourBotUsername'; // Fallback
    try {
        const me = await bot.getMe();
        if (me.username) { botUsername = me.username; }
    } catch (err) { console.warn("Failed to get bot username for referral link:", err.message) }
    const referralLink = `https://t\\.me/${botUsername}?start=${refCode}`; // Needs escaping later

    const minBetSOL = (Number(REFERRAL_INITIAL_BET_MIN_LAMPORTS)/LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
    const milestonePercent = (REFERRAL_MILESTONE_REWARD_PERCENT * 100).toFixed(2);

    // Generate tiered bonus text
    let tiersText = REFERRAL_INITIAL_BONUS_TIERS.map((tier, index) => {
        let countLabel = '';
        if (tier.maxCount === Infinity) {
            const prevMax = index > 0 ? REFERRAL_INITIAL_BONUS_TIERS[index - 1].maxCount : 0;
            countLabel = `${prevMax + 1}\\+`; // Escaped +
        } else {
            const lowerBound = index === 0 ? 1 : REFERRAL_INITIAL_BONUS_TIERS[index - 1].maxCount + 1;
            countLabel = `${lowerBound}\\-${tier.maxCount}`; // Escaped -
        }
        return `      \\- ${escapeMarkdownV2(countLabel)} refs: ${escapeMarkdownV2((tier.percent * 100).toFixed(0))}\\%`; // Escaped -, %
        }).join('\n');

    // Construct the message, escaping all dynamic parts
    let referralMsg = `*Your Referral Stats* 📊\n\n`
                    + `*Your Code:* \`${escapeMarkdownV2(refCode)}\`\n`
                    + `*Your Referral Link \\(Share this\\!\\):*\n` // Escaped ()!
                    + `\`${escapeMarkdownV2(referralLink)}\`\n\n` // Escape the constructed link
                    + `*Successful Referrals:* ${escapeMarkdownV2(userDetails.referral_count)}\n`
                    + `*Total Referral Earnings Paid:* ${escapeMarkdownV2(totalEarningsSOL)} SOL\n\n`
                    + `*How Rewards Work:*\n`
                    + `1\\. *Initial Bonus:* Earn a tiered percentage of your referral's *first* completed bet \\(min ${escapeMarkdownV2(minBetSOL)} SOL wager\\)\\.\n` // Escaped .(), .
                    + `2\\. *Milestone Bonus:* Earn ${escapeMarkdownV2(milestonePercent)}\\% of the total amount wagered by your referrals as they hit wagering milestones \\(e\\.g\\., 1, 5, 10 SOL etc\\.\\)\\.\n\n` // Escaped %, .(), ., .
                    + `*Your Initial Bonus Tier \\(based on your ref count *before* their first bet\\):*\n` // Escaped ()
                    + `${tiersText}\n\n` // tiersText is already escaped
                    + `Payouts are sent automatically to your linked wallet: \`${escapeMarkdownV2(userDetails.external_withdrawal_address)}\``; // Escaped .

    await safeSendMessage(chatId, referralMsg, { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}


/** Handles /deposit command */
async function handleDepositCommand(msg, args) {
    const chatId = msg.chat.id;
    const userId = String(msg.from.id);
    const logPrefix = `[DepositCmd User ${userId}]`;
    console.log(`${logPrefix} Handling deposit command.`);

    try {
        // 1. Ensure user exists (also warms up DB connection potentially)
        await getUserBalance(userId); // Ensures user exists via internal call

        // 2. Get next index
        const addressIndex = await getNextDepositAddressIndex(userId);

        // 3. Generate unique address
        const derivedInfo = await generateUniqueDepositAddress(userId, addressIndex); // Uses Part 3 function
        if (!derivedInfo) {
            // Error logged and admin notified within generateUniqueDepositAddress
            throw new Error("Failed to generate a unique deposit address."); // Throw to trigger catch block below
        }
        const depositAddress = derivedInfo.publicKey.toBase58();
        const derivationPath = derivedInfo.derivationPath;
        console.log(`${logPrefix} Deposit address generated: ${depositAddress}`);

        // 4. Calculate expiry
        const expiresAt = new Date(Date.now() + DEPOSIT_ADDRESS_EXPIRY_MS); // Uses constant from Part 1

        // 5. Create DB record for the address
        const recordResult = await createDepositAddressRecord(userId, depositAddress, derivationPath, expiresAt); // Uses Part 2 function
        if (!recordResult.success || !recordResult.id) {
            // Error logged within createDepositAddressRecord
            throw new Error(recordResult.error || "Failed to save deposit address record to database.");
        }
        console.log(`${logPrefix} Deposit address record created (ID: ${recordResult.id}).`);

        // 6. Send message to user - Meticulously escaped
        const expiryMinutes = Math.round(DEPOSIT_ADDRESS_EXPIRY_MS / (60 * 1000));
        const message = `💰 *Deposit SOL*\n\n`
                      + `To add funds to your bot balance, send SOL to this unique address:\n\n`
                      + `\`${escapeMarkdownV2(depositAddress)}\`\n\n` // Escaped address
                      + `⚠️ *Important:*\n`
                      + `1\\. This address is temporary and only valid for *one* deposit\\. It expires in *${escapeMarkdownV2(expiryMinutes)} minutes*\\. \n` // Escaped ., ., .
                      + `2\\. Do *not* send multiple times to this address\\. Use \`/deposit\` again for new deposits\\. \n` // Escaped ., .
                      + `3\\. Deposits typically require *${escapeMarkdownV2(DEPOSIT_CONFIRMATION_LEVEL)}* network confirmations to reflect in your /wallet balance\\.`; // Escaped .

        console.log(`${logPrefix} Sending deposit instructions to user.`);
        await safeSendMessage(chatId, message, { parse_mode: 'MarkdownV2' });
        console.log(`${logPrefix} Deposit command handled successfully.`);

    } catch (error) {
        console.error(`${logPrefix} Error: ${error.message}`);
        // Admin already notified by sub-functions if specific error occurred there
        await safeSendMessage(chatId, `❌ Error generating deposit address: ${escapeMarkdownV2(error.message)}\\. Please try again later or contact support\\.`, { parse_mode: 'MarkdownV2' });
    }
}

/** Handles /withdraw command - Starts the withdrawal flow */
async function handleWithdrawCommand(msg, args) {
    const chatId = msg.chat.id;
    const userId = String(msg.from.id);
    const logPrefix = `[WithdrawCmd User ${userId}]`;
    // console.log(`${logPrefix} Handling withdraw command.`); // Less verbose

    try {
        // 1. Check if wallet is linked
        const linkedAddress = await getLinkedWallet(userId); // Uses Part 2/3 functions
        if (!linkedAddress) {
            await safeSendMessage(chatId, `⚠️ You must set your external withdrawal address first using \`/wallet <YourSolanaAddress>\`\\. Rewards and withdrawals are sent there\\.`, { parse_mode: 'MarkdownV2' });
            return;
        }

        // 2. Check if balance is sufficient for minimum withdrawal + fee
        const currentBalance = await getUserBalance(userId); // Assumes Part 2 function
        const minWithdrawTotal = MIN_WITHDRAWAL_LAMPORTS + WITHDRAWAL_FEE_LAMPORTS; // Constants from Part 1
        if (currentBalance < minWithdrawTotal) {
            const minNeededSOL = (Number(minWithdrawTotal) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
            const balanceSOL = (Number(currentBalance)/LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
            await safeSendMessage(chatId, `⚠️ Your balance \\(${escapeMarkdownV2(balanceSOL)} SOL\\) is below the minimum required to cover the withdrawal amount \\+ fee \\(min ${escapeMarkdownV2(minNeededSOL)} SOL total\\)\\.`, { parse_mode: 'MarkdownV2'});
            return;
        }

        // 3. Set state to expect amount input
        // Note: We transition state WITHOUT asking for text input now.
        // Instead, we send the bet amount buttons directly.
        // For withdrawals, we still need the amount typed.
        userStateCache.set(userId, { state: 'awaiting_withdrawal_amount', chatId: String(chatId), messageId: msg.message_id, timestamp: Date.now(), data: {} });

        // 4. Send message asking for amount
        const minWithdrawSOLText = (Number(MIN_WITHDRAWAL_LAMPORTS)/LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
        const feeSOLText = (Number(WITHDRAWAL_FEE_LAMPORTS)/LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
        await safeSendMessage(chatId, `💸 Your withdrawal address: \`${escapeMarkdownV2(linkedAddress)}\`\n`
                                   + `Minimum withdrawal: ${escapeMarkdownV2(minWithdrawSOLText)} SOL\\. Fee: ${escapeMarkdownV2(feeSOLText)} SOL\\. \n\n` // Escaped ., ., .
                                   + `Please enter the amount of SOL you wish to withdraw \\(e\\.g\\., 0\\.5\\):`, // Escaped (), . :
                                   { parse_mode: 'MarkdownV2' });

    } catch (error) {
        console.error(`${logPrefix} Error starting withdrawal: ${error.message}`);
        await notifyAdmin(`🚨 ERROR starting /withdraw for User ${userId} (${escapeMarkdownV2(logPrefix)}):\n${escapeMarkdownV2(error.message)}`);
        await safeSendMessage(chatId, `❌ Error starting withdrawal process\\. Please try again later\\.`, { parse_mode: 'MarkdownV2' });
    }
}


// --- Game Command Handlers (Rewritten to show bet amount buttons) ---

/** Generic function to show bet amount buttons for a game */
async function showBetAmountButtons(msg, gameKey) {
    const chatId = msg.chat.id;
    const userId = String(msg.from.id);
    const gameConfig = GAME_CONFIG[gameKey];
    const logPrefix = `[Cmd /${gameKey} User ${userId}]`;

    if (!gameConfig) {
         console.error(`${logPrefix} Invalid game key: ${gameKey}`);
         await safeSendMessage(chatId, `⚠️ Internal error: Unknown game \`${escapeMarkdownV2(gameKey)}\`\\.`, { parse_mode: 'MarkdownV2'});
         return;
    }
     // Optional: Quick balance check before showing buttons
     const balance = await getUserBalance(userId);
     if (balance < gameConfig.minBetLamports) {
         await safeSendMessage(chatId, `⚠️ Your balance \\(${escapeMarkdownV2((Number(balance)/LAMPORTS_PER_SOL).toFixed(4))} SOL\\) is too low to play ${escapeMarkdownV2(gameConfig.name)} \\(min bet: ${escapeMarkdownV2((Number(gameConfig.minBetLamports)/LAMPORTS_PER_SOL).toFixed(4))} SOL\\)\\. Use /deposit to add funds\\.`, { parse_mode: 'MarkdownV2'});
         return;
     }

    console.log(`${logPrefix} Showing bet amount buttons.`);
    userStateCache.set(userId, { state: 'idle', chatId: String(chatId), timestamp: Date.now() }); // Reset state before showing options

    const buttons = [];
    const amountsRow1 = [];
    const amountsRow2 = [];

    STANDARD_BET_AMOUNTS_SOL.forEach((solAmount, index) => {
        const lamports = STANDARD_BET_AMOUNTS_LAMPORTS[index];
        // Only show button if amount is within game limits AND user has enough balance
        if (lamports >= gameConfig.minBetLamports && lamports <= gameConfig.maxBetLamports /*&& balance >= lamports*/) { // Balance check done above minBet, maybe don't check here? Let callback handle specific amount.
             const button = { text: `${solAmount} SOL`, callback_data: `game_bet_amount:${gameKey}:${lamports.toString()}` };
             // Simple split into two rows
             if (amountsRow1.length < 3) {
                 amountsRow1.push(button);
             } else {
                 amountsRow2.push(button);
             }
        }
    });

    if(amountsRow1.length > 0) buttons.push(amountsRow1);
    if(amountsRow2.length > 0) buttons.push(amountsRow2);

    // Add Custom amount button and Cancel button
    buttons.push([
        { text: "✏️ Custom Amount", callback_data: `game_bet_amount:${gameKey}:custom` },
        { text: "❌ Cancel", callback_data: 'cancel_action' }
    ]);

    const prompt = `Select bet amount for *${escapeMarkdownV2(gameConfig.name)}*:`;
    await safeSendMessage(chatId, prompt, {
        reply_markup: { inline_keyboard: buttons },
        parse_mode: 'MarkdownV2'
    });
}

// Specific game command handlers now just call the button function
async function handleCoinflipCommand(msg, args) { await showBetAmountButtons(msg, 'coinflip'); }
async function handleRaceCommand(msg, args) { await showBetAmountButtons(msg, 'race'); }
async function handleSlotsCommand(msg, args) { await showBetAmountButtons(msg, 'slots'); }
async function handleWarCommand(msg, args) { await showBetAmountButtons(msg, 'war'); }
async function handleRouletteCommand(msg, args) {
    // Roulette betting is complex, keep placeholder or implement button-based betting later
     await safeSendMessage(msg.chat.id, `⚪️ Roulette betting interface is under development\\. Please select another game for now\\.`, { parse_mode: 'MarkdownV2'});
     // await showBetAmountButtons(msg, 'roulette'); // Enable if button flow is designed
}


// --- Command Handler Map Definition ---
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
    'admin': handleAdminCommand, // Defined below
    // Add aliases if needed: e.g., 'cf': handleCoinflipCommand,
};

// --- Menu Command Handler Map Definition (used in handleCallbackQuery for menu buttons) ---
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


// --- Admin Command ---
// TODO: Implement more detailed admin commands as needed (e.g., user lookup, manual adjustments requires extreme care)
async function handleAdminCommand(msg, args) {
    const chatId = msg.chat.id; const userId = String(msg.from.id);
    const logPrefix = `[AdminCmd User ${userId}]`;
    const subCommand = args?.split(' ')[0]?.toLowerCase();
    const subArgs = args?.split(' ').slice(1) || [];

    if (!subCommand) {
         // Escaped Usage Message
         await safeSendMessage(chatId, "Admin Usage: `/admin <command> [args]`\nCommands: `status`, `getbalance`, `getconfig`, `finduser <id\\_or\\_ref>`", { parse_mode: 'MarkdownV2' });
         return;
    }
    console.log(`${logPrefix} Executing: ${subCommand} ${subArgs.join(' ')}`);

    try {
        switch (subCommand) {
            case 'status': {
                 const msgQueueSize = messageQueue.size; const cbQueueSize = callbackQueue.size; const payoutQueueSize = payoutProcessorQueue.size;
                 const depositQSize = depositProcessorQueue.size; const tgSendQSize = telegramSendQueue.size; const walletCacheSize = walletCache.size;
                 const depAddrCacheSize = activeDepositAddresses.size; const processedTxCacheSize = processedDepositTxSignatures.size;
                 const dbPoolTotal = pool.totalCount; const dbPoolIdle = pool.idleCount; const dbPoolWaiting = pool.waitingCount;
                 const uptime = process.uptime(); const uptimeHours = Math.floor(uptime / 3600); const uptimeMins = Math.floor((uptime % 3600) / 60); const uptimeSecs = Math.floor(uptime % 60);

                 let statusMsg = `*Bot Status* \\(v${escapeMarkdownV2(process.env.npm_package_version || '3.1.6')}\\) \\- ${escapeMarkdownV2(new Date().toISOString())}\n`
                               + `Uptime: ${uptimeHours}h ${uptimeMins}m ${uptimeSecs}s\n\n`
                               + `*Queues \\(Active/Pending\\):*\n`
                               + `  Msg: ${messageQueue.pending}/${messageQueue.size}, CB: ${callbackQueue.pending}/${callbackQueue.size}, Payout: ${payoutProcessorQueue.pending}/${payoutQueueSize}, Deposit: ${depositProcessorQueue.pending}/${depositQSize}, TG Send: ${telegramSendQueue.pending}/${tgSendQSize}\n`
                               + `*Caches:*\n`
                               + `  User States: ${userStateCache.size}, Wallets: ${walletCacheSize}, Dep Addr: ${depAddrCacheSize}, Proc TXs: ${processedTxCacheSize}/${MAX_PROCESSED_TX_CACHE_SIZE}\n`
                               + `*DB Pool:*\n`
                               + `  Total: ${dbPoolTotal}, Idle: ${dbPoolIdle}, Waiting: ${dbPoolWaiting}\n`
                               + `*State*: Initialized: ${isFullyInitialized}\n`;
                 await safeSendMessage(chatId, statusMsg, { parse_mode: 'MarkdownV2' }); break;
            }
            case 'getbalance': {
                 const keyTypes = ['main', 'referral']; let balanceMsg = '*Payout Wallet Balances:*\n';
                 for (const type of keyTypes) {
                     let keyEnvVar = ''; let keyDesc = '';
                     if (type === 'main') { keyEnvVar = 'MAIN_BOT_PRIVATE_KEY'; keyDesc = 'Main';}
                     else if (type === 'referral') { keyEnvVar = process.env.REFERRAL_PAYOUT_PRIVATE_KEY ? 'REFERRAL_PAYOUT_PRIVATE_KEY' : 'MAIN_BOT_PRIVATE_KEY'; keyDesc = `Referral \\(${escapeMarkdownV2(process.env.REFERRAL_PAYOUT_PRIVATE_KEY ? 'Dedicated' : 'Using Main')}\\)`;} // Escaped ()
                     const pk = process.env[keyEnvVar];
                     if (pk) { try { const keypair = Keypair.fromSecretKey(bs58.decode(pk)); const balance = await solanaConnection.getBalance(keypair.publicKey);
                           balanceMsg += `  \\- ${escapeMarkdownV2(keyDesc)} \\(\`${escapeMarkdownV2(keypair.publicKey.toBase58())}\`\\): ${escapeMarkdownV2((Number(balance)/LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS))} SOL\n`; // Escaped -, (), ``, .
                         } catch (e) { balanceMsg += `  \\- ${escapeMarkdownV2(keyDesc)}: Error fetching balance \\- ${escapeMarkdownV2(e.message)}\n`; } // Escaped - (x2)
                     } else { balanceMsg += `  \\- ${escapeMarkdownV2(keyDesc)}: Private key not set\n`; } // Escaped -
                 } await safeSendMessage(chatId, balanceMsg, { parse_mode: 'MarkdownV2' }); break;
            }
             case 'getconfig': {
                 let configText = "*Current Config \\(Non\\-Secret Env Vars & Defaults\\):*\n\n"; // Escaped ()-
                 const sensitiveKeywords = ['TOKEN', 'DATABASE_URL', 'KEY', 'SECRET', 'PASSWORD', 'SEED', 'MONGO', 'REDIS']; // More keywords
                 const combinedConfig = {};
                 // Prioritize actual env vars over defaults for display
                 Object.keys(OPTIONAL_ENV_DEFAULTS).forEach(key => combinedConfig[key] = OPTIONAL_ENV_DEFAULTS[key]);
                 Object.keys(process.env).forEach(key => {
                     if (OPTIONAL_ENV_DEFAULTS.hasOwnProperty(key) || REQUIRED_ENV_VARS.includes(key)) {
                         combinedConfig[key] = process.env[key];
                     }
                 });

                 for (const key of Object.keys(combinedConfig).sort()) {
                     if (REQUIRED_ENV_VARS.includes(key) || OPTIONAL_ENV_DEFAULTS.hasOwnProperty(key)) {
                          const isSensitive = sensitiveKeywords.some(k => key.toUpperCase().includes(k));
                          const value = combinedConfig[key];
                          let displayValue = '';
                          let source = '';

                          if (process.env[key] !== undefined) { // Value is set in env
                              displayValue = isSensitive && key !== 'RPC_URLS' && key !== 'ADMIN_USER_IDS' ? '[Set, Redacted]' : escapeMarkdownV2(value);
                              source = (OPTIONAL_ENV_DEFAULTS.hasOwnProperty(key) && value === OPTIONAL_ENV_DEFAULTS[key]) ? ' \\(Matches Default\\)' : ' \\(Set\\)';
                          } else if (OPTIONAL_ENV_DEFAULTS.hasOwnProperty(key)) { // Value is using default
                              displayValue = isSensitive && key !== 'RPC_URLS' && key !== 'ADMIN_USER_IDS' ? '[Default, Redacted]' : escapeMarkdownV2(value);
                              source = ' \\(Default\\)';
                          } else { // Required var missing (should not happen)
                              displayValue = '[MISSING REQUIRED]';
                              source = ' \\(ERROR\\)';
                          }
                          configText += `\\- \`${escapeMarkdownV2(key)}\`: ${displayValue}${escapeMarkdownV2(source)}\n`; // Escape -, `
                      }
                 }
                 configText += "\n*Game Config Limits & Edge:*\n" + "```json\n" + JSON.stringify(GAME_CONFIG, (k, v) => typeof v === 'bigint' ? v.toString() + 'n' : v, 2) + "\n```"; // Use JSON code block
                 await safeSendMessage(chatId, configText.substring(0, 4090), { parse_mode: 'MarkdownV2' }); break;
             }
            // Add 'finduser' or other commands here later
            default:
                await safeSendMessage(chatId, `Unknown admin command: \`${escapeMarkdownV2(subCommand)}\``, { parse_mode: 'MarkdownV2' });
        }
    } catch (adminError) {
        console.error(`${logPrefix} Admin command error (${subCommand}):`, adminError);
        await notifyAdmin(`🚨 ERROR executing Admin Command \`${escapeMarkdownV2(subCommand)}\` for User ${userId}:\n${escapeMarkdownV2(adminError.message)}`);
        await safeSendMessage(chatId, `Error executing admin command \`${escapeMarkdownV2(subCommand)}\`: \`${escapeMarkdownV2(adminError.message)}\``, { parse_mode: 'MarkdownV2' });
    }
}


// --- Referral Check & Payout Triggering Logic ---
/**
 * Checks for and processes referral bonuses after a bet completes successfully.
 * Must be called AFTER the main game logic/DB commit is successful.
 * Handles DB operations within its own transaction where necessary.
 * @param {string} refereeUserId - The user who placed the bet.
 * @param {number} completedBetId - The ID of the completed bet.
 * @param {bigint} wagerAmountLamports - The amount wagered in the bet.
 * @param {string} [logPrefix=''] - Optional prefix for logs.
 */
async function _handleReferralChecks(refereeUserId, completedBetId, wagerAmountLamports, logPrefix = '') {
      refereeUserId = String(refereeUserId);
      logPrefix = logPrefix || `[ReferralCheck Bet ${completedBetId} User ${refereeUserId}]`;
      // console.log(`${logPrefix} Starting referral checks.`); // Less verbose

      let client = null;
      try {
          const refereeDetails = await getUserWalletDetails(refereeUserId); // Needs latest wager stats
          if (!refereeDetails) {
               console.error(`${logPrefix} Could not fetch referee details. Aborting referral check.`);
               // Should not happen if user placed a bet, but handle defensively.
               return;
          }
          if (!refereeDetails.referred_by_user_id) {
              // console.log(`${logPrefix} Referee is not referred. No referral checks needed.`); // Less verbose
              // Ensure wager stats were updated in the main game handler transaction. No further action needed here.
              return;
          }
          const referrerUserId = refereeDetails.referred_by_user_id;

          // Check if referrer has linked wallet (required for payouts)
          const referrerDetails = await getUserWalletDetails(referrerUserId);
          if (!referrerDetails?.external_withdrawal_address) {
              console.warn(`${logPrefix} Referrer ${referrerUserId} has no linked wallet. Cannot process potential payouts.`);
              // Referee wager stats already updated, nothing more to do here.
              return;
          }

          // Proceed with checks within a new transaction
          client = await pool.connect();
          await client.query('BEGIN');
          let newLastMilestonePaid = refereeDetails.last_milestone_paid_lamports; // Use potentially updated value
          let payoutQueued = false; // Flag to track if any payout was queued

          // --- Check Initial Bet Bonus ---
          // isFirstCompletedBet checks based on the DB state, which should be accurate *after* the game handler commit.
          const isFirst = await isFirstCompletedBet(refereeUserId, completedBetId);
          if (isFirst && wagerAmountLamports >= REFERRAL_INITIAL_BET_MIN_LAMPORTS) {
              console.log(`${logPrefix} Referee's first qualifying bet detected. Calculating initial bonus...`);
              // Use referrer's count *before* this referral was fully linked (assuming linkReferral increments)
              // If linkReferral runs in same tx as first deposit, this count might be off by 1.
              // Best practice: Get count just before potential bonus calculation.
              const referrerWallet = await queryDatabase('SELECT referral_count FROM wallets WHERE user_id = $1 FOR UPDATE', [referrerUserId], client);
              const referrerCount = parseInt(referrerWallet.rows[0]?.referral_count || '0', 10);

              let bonusTier = REFERRAL_INITIAL_BONUS_TIERS[REFERRAL_INITIAL_BONUS_TIERS.length - 1]; // Default highest
              for (const tier of REFERRAL_INITIAL_BONUS_TIERS) { if (referrerCount <= tier.maxCount) { bonusTier = tier; break; } }

              const initialBonusAmount = wagerAmountLamports * BigInt(Math.round(bonusTier.percent * 100)) / 100n;
              console.log(`${logPrefix} Referrer Count: ${referrerCount}, Tier Percent: ${bonusTier.percent}, Bonus Amount: ${initialBonusAmount}`);

              if (initialBonusAmount > 0n) {
                  const payoutRecord = await recordPendingReferralPayout(referrerUserId, refereeUserId, 'initial_bet', initialBonusAmount, completedBetId, null, client);
                  if (payoutRecord.success && payoutRecord.payoutId) {
                      await addPayoutJob({ type: 'payout_referral', payoutId: payoutRecord.payoutId, userId: referrerUserId }, parseInt(process.env.PAYOUT_JOB_RETRIES, 10), parseInt(process.env.PAYOUT_JOB_RETRY_DELAY_MS, 10)); // Part 6 func
                      console.log(`${logPrefix} Initial bonus payout job queued (ID: ${payoutRecord.payoutId}).`);
                      payoutQueued = true;
                  } else if (!payoutRecord.duplicate) { // Only log/notify non-duplicate errors
                      console.error(`${logPrefix} Failed to record initial bonus payout: ${payoutRecord.error}`);
                      await notifyAdmin(`🚨 ERROR recording Initial Bonus Payout DB for Referrer ${referrerUserId} / Referee ${refereeUserId} (${escapeMarkdownV2(logPrefix)}). Error: ${escapeMarkdownV2(payoutRecord.error)}`);
                  }
              }
          }

          // --- Check Milestone Bonus ---
          // Use the total wagered amount from the details fetched at the start of this function
          const currentTotalWagered = refereeDetails.total_wagered; // This reflects the state *after* the last successful wager update
          let milestoneCrossed = null;
          for (const threshold of REFERRAL_MILESTONE_THRESHOLDS_LAMPORTS) {
              // Check if the current total wagered has crossed a threshold that hasn't been paid yet
              if (currentTotalWagered >= threshold && threshold > refereeDetails.last_milestone_paid_lamports) {
                  milestoneCrossed = threshold; // Track the highest threshold crossed *this time*
              } else if (currentTotalWagered < threshold) {
                  break; // Stop checking once below threshold
              }
          }

          if (milestoneCrossed) {
              console.log(`${logPrefix} Milestone threshold ${milestoneCrossed} crossed (Previous Paid: ${refereeDetails.last_milestone_paid_lamports}, New Total: ${currentTotalWagered}). Calculating bonus...`);
              const milestoneBonusAmount = milestoneCrossed * BigInt(Math.round(REFERRAL_MILESTONE_REWARD_PERCENT * 10000)) / 10000n;
              console.log(`${logPrefix} Milestone Bonus Amount: ${milestoneBonusAmount}`);

              if (milestoneBonusAmount > 0n) {
                  const payoutRecord = await recordPendingReferralPayout(referrerUserId, refereeUserId, 'milestone', milestoneBonusAmount, null, milestoneCrossed, client);
                  if (payoutRecord.success && payoutRecord.payoutId) {
                      await addPayoutJob({ type: 'payout_referral', payoutId: payoutRecord.payoutId, userId: referrerUserId }, parseInt(process.env.PAYOUT_JOB_RETRIES, 10), parseInt(process.env.PAYOUT_JOB_RETRY_DELAY_MS, 10)); // Part 6 func
                      console.log(`${logPrefix} Milestone bonus payout job queued (ID: ${payoutRecord.payoutId}).`);
                      // IMPORTANT: Update the milestone marker *within this transaction*
                      newLastMilestonePaid = milestoneCrossed;
                      const milestoneUpdate = await queryDatabase(
                          'UPDATE wallets SET last_milestone_paid_lamports = $1 WHERE user_id = $2 AND last_milestone_paid_lamports < $1', // Only update if less
                          [newLastMilestonePaid, refereeUserId],
                          client
                      );
                       if(milestoneUpdate.rowCount === 0) {
                           // This could happen in a race condition if another bet triggered the milestone update concurrently
                           console.warn(`${logPrefix} Milestone marker for ${refereeUserId} potentially updated concurrently. Expected to set to ${newLastMilestonePaid}.`);
                       } else {
                           console.log(`${logPrefix} Updated milestone marker for referee ${refereeUserId} to ${newLastMilestonePaid}.`);
                       }
                      payoutQueued = true;
                  } else if (!payoutRecord.duplicate) { // Only log/notify non-duplicate errors
                      console.error(`${logPrefix} Failed to record milestone bonus payout: ${payoutRecord.error}`);
                       await notifyAdmin(`🚨 ERROR recording Milestone Bonus Payout DB for Referrer ${referrerUserId} / Referee ${refereeUserId} / Milestone ${milestoneCrossed} (${escapeMarkdownV2(logPrefix)}). Error: ${escapeMarkdownV2(payoutRecord.error)}`);
                  }
              }
          }

          // Commit transaction for any recorded payouts / milestone marker update
          await client.query('COMMIT');
          console.log(`${logPrefix} Referral checks transaction committed.`);

      } catch (error) {
           console.error(`${logPrefix} Error during referral checks: ${error.message}`, error.stack);
           await notifyAdmin(`🚨 UNEXPECTED ERROR during _handleReferralChecks for User ${refereeUserId} / Bet ${completedBetId} (${escapeMarkdownV2(logPrefix)}):\n${escapeMarkdownV2(error.message)}`);
          if (client) { try { await client.query('ROLLBACK'); console.log(`${logPrefix} Transaction rolled back due to error.`); } catch (rbErr) { console.error(`${logPrefix} Rollback failed: ${rbErr.message}`);} }
      } finally {
          if (client) client.release();
      }
}


// --- End of Part 5b ---
// index.js - Part 6: Background Tasks, Payouts, Startup & Shutdown
// --- VERSION: 3.1.7 --- (Fix BigInt/Number TypeError in Sweeper)

// --- Assuming imports, constants, functions from previous parts are available ---

// --- Database Initialization ---

/**
 * Initializes the database by creating tables if they don't exist
 * and applying necessary constraints/indexes idempotently.
 */
async function initializeDatabase() {
    console.log('⚙️ Initializing Database Schema...');
    let client = null;
    try {
        client = await pool.connect();
        await client.query('BEGIN');

        // Wallets Table (Users, Withdrawal Address, Referral Info)
        await client.query(`
            CREATE TABLE IF NOT EXISTS wallets (
                user_id VARCHAR(255) PRIMARY KEY,
                external_withdrawal_address VARCHAR(44),
                linked_at TIMESTAMPTZ,
                last_used_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                referral_code VARCHAR(12) UNIQUE,
                referred_by_user_id VARCHAR(255) REFERENCES wallets(user_id) ON DELETE SET NULL,
                referral_count INTEGER NOT NULL DEFAULT 0,
                total_wagered BIGINT NOT NULL DEFAULT 0,
                last_milestone_paid_lamports BIGINT NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);
        await client.query(`ALTER TABLE wallets ADD COLUMN IF NOT EXISTS last_used_at TIMESTAMPTZ NOT NULL DEFAULT NOW();`);
        await client.query('CREATE INDEX IF NOT EXISTS idx_wallets_referral_code ON wallets (referral_code);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_wallets_referred_by ON wallets (referred_by_user_id);');


        // User Balances Table (Internal Balance)
        await client.query(`
            CREATE TABLE IF NOT EXISTS user_balances (
                user_id VARCHAR(255) PRIMARY KEY REFERENCES wallets(user_id) ON DELETE CASCADE,
                balance_lamports BIGINT NOT NULL DEFAULT 0 CHECK (balance_lamports >= 0),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);
         await client.query(`ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();`);


        // Deposit Addresses Table (Generated addresses for users)
        await client.query(`
            CREATE TABLE IF NOT EXISTS deposit_addresses (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                deposit_address VARCHAR(44) NOT NULL UNIQUE,
                derivation_path VARCHAR(255) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                expires_at TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_checked_at TIMESTAMPTZ
            );
        `);
         await client.query(`ALTER TABLE deposit_addresses ADD COLUMN IF NOT EXISTS derivation_path VARCHAR(255) NOT NULL DEFAULT 'UNKNOWN';`); // Add default temporarily if needed
         await client.query(`ALTER TABLE deposit_addresses ALTER COLUMN derivation_path DROP DEFAULT;`); // Remove default after ensuring population
         await client.query(`ALTER TABLE deposit_addresses ADD COLUMN IF NOT EXISTS last_checked_at TIMESTAMPTZ;`);

        // Indexes for deposit addresses
        await client.query('CREATE INDEX IF NOT EXISTS idx_deposit_addresses_user_id ON deposit_addresses (user_id);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_deposit_addresses_status_expires ON deposit_addresses (status, expires_at);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_deposit_addresses_status_last_checked ON deposit_addresses (status, last_checked_at ASC NULLS FIRST);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_deposit_addresses_status_created ON deposit_addresses (status, created_at ASC);');


        // Deposits Table (Confirmed incoming transactions)
        await client.query(`
            CREATE TABLE IF NOT EXISTS deposits (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                deposit_address_id INTEGER, -- FK added below
                tx_signature VARCHAR(88) NOT NULL UNIQUE,
                amount_lamports BIGINT NOT NULL CHECK (amount_lamports > 0),
                status VARCHAR(20) NOT NULL DEFAULT 'confirmed',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);
        const fkDepositAddrCheck = await queryDatabase(`SELECT 1 FROM pg_constraint WHERE conname = 'deposits_deposit_address_id_fkey'`, [], client);
        if (fkDepositAddrCheck.rowCount === 0) {
             console.log("Adding deposits_deposit_address_id_fkey constraint...");
             await client.query(`ALTER TABLE deposits ADD CONSTRAINT deposits_deposit_address_id_fkey FOREIGN KEY (deposit_address_id) REFERENCES deposit_addresses(id) ON DELETE SET NULL;`);
        } else { /*Constraint exists*/ }
        await client.query('CREATE INDEX IF NOT EXISTS idx_deposits_user_id ON deposits (user_id);');


        // Bets Table
        await client.query(`
            CREATE TABLE IF NOT EXISTS bets (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                chat_id VARCHAR(255) NOT NULL,
                game_type VARCHAR(50) NOT NULL,
                bet_details JSONB,
                wager_amount_lamports BIGINT NOT NULL, -- CHECK constraint added below
                payout_amount_lamports BIGINT,
                status VARCHAR(30) NOT NULL DEFAULT 'active',
                priority INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                processed_at TIMESTAMPTZ
            );
        `);
        await client.query(`ALTER TABLE bets DROP CONSTRAINT IF EXISTS bets_wager_positive;`);
        await client.query(`ALTER TABLE bets DROP CONSTRAINT IF EXISTS bets_wager_amount_lamports_check;`);
        await client.query(`ALTER TABLE bets ADD CONSTRAINT bets_wager_amount_lamports_check CHECK (wager_amount_lamports > 0);`);
        await client.query(`ALTER TABLE bets ADD COLUMN IF NOT EXISTS priority INTEGER NOT NULL DEFAULT 0;`);
        await client.query('CREATE INDEX IF NOT EXISTS idx_bets_user_id_status ON bets (user_id, status);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_bets_created_at ON bets (created_at);');


        // Withdrawals Table
        await client.query(`
            CREATE TABLE IF NOT EXISTS withdrawals (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                requested_amount_lamports BIGINT NOT NULL,
                fee_lamports BIGINT NOT NULL DEFAULT 0,
                final_send_amount_lamports BIGINT NOT NULL,
                recipient_address VARCHAR(44) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                payout_tx_signature VARCHAR(88) UNIQUE,
                error_message TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                processed_at TIMESTAMPTZ,
                completed_at TIMESTAMPTZ
            );
        `);
        await client.query('CREATE INDEX IF NOT EXISTS idx_withdrawals_user_id_status ON withdrawals (user_id, status);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_withdrawals_status ON withdrawals (status);');


        // Referral Payouts Table
        await client.query(`
            CREATE TABLE IF NOT EXISTS referral_payouts (
                id SERIAL PRIMARY KEY,
                referrer_user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                referee_user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                payout_type VARCHAR(20) NOT NULL,
                payout_amount_lamports BIGINT NOT NULL,
                triggering_bet_id INTEGER REFERENCES bets(id) ON DELETE SET NULL,
                milestone_reached_lamports BIGINT,
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                payout_tx_signature VARCHAR(88) UNIQUE,
                error_message TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                processed_at TIMESTAMPTZ,
                paid_at TIMESTAMPTZ
            );
        `);
        await client.query('CREATE INDEX IF NOT EXISTS idx_refpayout_referrer ON referral_payouts (referrer_user_id, status);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_refpayout_referee ON referral_payouts (referee_user_id);');
        await client.query(`
            CREATE UNIQUE INDEX IF NOT EXISTS idx_refpayout_unique_milestone ON referral_payouts
            (referrer_user_id, referee_user_id, payout_type, milestone_reached_lamports)
            WHERE payout_type = 'milestone';
        `);


        // Ledger Table (Transaction history for user balances)
        await client.query(`
            CREATE TABLE IF NOT EXISTS ledger (
                id BIGSERIAL PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                transaction_type VARCHAR(50) NOT NULL,
                amount_lamports BIGINT NOT NULL,
                balance_before BIGINT NOT NULL,
                balance_after BIGINT NOT NULL,
                related_bet_id INTEGER REFERENCES bets(id) ON DELETE SET NULL,
                related_deposit_id INTEGER REFERENCES deposits(id) ON DELETE SET NULL,
                related_withdrawal_id INTEGER REFERENCES withdrawals(id) ON DELETE SET NULL,
                related_ref_payout_id INTEGER, -- FK added below
                notes TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);
        await client.query(`ALTER TABLE ledger ADD COLUMN IF NOT EXISTS related_ref_payout_id INTEGER;`);
        const fkLedgerRefCheck = await queryDatabase(`SELECT 1 FROM pg_constraint WHERE conname = 'ledger_related_ref_payout_id_fkey'`, [], client);
        if (fkLedgerRefCheck.rowCount === 0) {
             console.log("Adding ledger_related_ref_payout_id_fkey constraint...");
             await client.query(`ALTER TABLE ledger ADD CONSTRAINT ledger_related_ref_payout_id_fkey FOREIGN KEY (related_ref_payout_id) REFERENCES referral_payouts(id) ON DELETE SET NULL;`);
        } else { /*Constraint exists*/ }
        await client.query('CREATE INDEX IF NOT EXISTS idx_ledger_user_id_created ON ledger (user_id, created_at DESC);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_ledger_transaction_type ON ledger (transaction_type);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_ledger_related_bet_id ON ledger (related_bet_id);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_ledger_related_withdrawal_id ON ledger (related_withdrawal_id);');


        await client.query('COMMIT');
        console.log('✅ Database schema initialized/verified successfully.');
    } catch (err) {
        console.error('❌ CRITICAL DATABASE INITIALIZATION ERROR:', err);
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { console.error('Rollback failed:', rbErr); } }
        await notifyAdmin(`🚨 CRITICAL DB INIT FAILED: ${escapeMarkdownV2(err.message)}\\. Bot cannot start\\. Check logs immediately\\.`).catch(()=>{});
        process.exit(2);
    } finally {
        if (client) client.release();
    }
}


// --- Load Active Deposits Cache ---
/** Loads pending deposit addresses from DB into the active cache on startup. */
async function loadActiveDepositsCache() {
    const logPrefix = '[LoadActiveDeposits]';
    console.log(`${logPrefix} Loading active deposit addresses from DB into cache...`);
    let count = 0; let expiredCount = 0;
    try {
        const res = await queryDatabase(`SELECT deposit_address, user_id, expires_at FROM deposit_addresses WHERE status = 'pending'`);
        const now = Date.now();
        for (const row of res.rows) {
            const expiresAtTime = new Date(row.expires_at).getTime();
            if (now < expiresAtTime) { addActiveDepositAddressCache(row.deposit_address, row.user_id, expiresAtTime); count++; }
            else { expiredCount++; }
        }
        console.log(`${logPrefix} ✅ Loaded ${count} active deposit addresses into cache. ${expiredCount} expired.`);
    } catch (error) {
        console.error(`${logPrefix} ❌ Error loading active deposits: ${error.message}`);
        await notifyAdmin(`🚨 ERROR Loading active deposit cache on startup: ${escapeMarkdownV2(error.message)}`);
    }
}

// --- Deposit Monitor ---
/** Starts the deposit monitoring polling loop. */
function startDepositMonitor() {
    let intervalMs = parseInt(process.env.DEPOSIT_MONITOR_INTERVAL_MS, 10);
    if (isNaN(intervalMs) || intervalMs < 5000) { intervalMs = 20000; console.warn(`Invalid DEPOSIT_MONITOR_INTERVAL_MS, using ${intervalMs}ms.`); }
    if (depositMonitorIntervalId) { clearInterval(depositMonitorIntervalId); console.log('🔄 Restarting deposit monitor...'); }
    else { console.log(`⚙️ Starting Deposit Monitor (Polling Interval: ${intervalMs / 1000}s)...`); }
    setTimeout(() => {
         monitorDepositsPolling().catch(err => console.error("[Initial Deposit Monitor Run] Error:", err));
         depositMonitorIntervalId = setInterval(monitorDepositsPolling, intervalMs);
         if (depositMonitorIntervalId.unref) depositMonitorIntervalId.unref();
    }, 3000);
}
/** Polling function to check pending deposit addresses for transactions. */
async function monitorDepositsPolling() {
    const logPrefix = '[DepositMonitor]';
    if (monitorDepositsPolling.isRunning) return;
    monitorDepositsPolling.isRunning = true;
    let client = null;
    try {
        const batchSize = parseInt(process.env.DEPOSIT_MONITOR_ADDRESS_BATCH_SIZE, 10);
        const sigFetchLimit = parseInt(process.env.DEPOSIT_MONITOR_SIGNATURE_FETCH_LIMIT, 10);
        const pendingAddressesRes = await queryDatabase(`SELECT id, deposit_address, expires_at FROM deposit_addresses WHERE status = 'pending' AND expires_at > NOW() ORDER BY last_checked_at ASC NULLS FIRST LIMIT $1`, [batchSize]);
        if (pendingAddressesRes.rowCount === 0) { monitorDepositsPolling.isRunning = false; return; }
        client = await pool.connect(); await client.query('BEGIN');
        for (const row of pendingAddressesRes.rows) {
            const address = row.deposit_address; const addressId = row.id; const addrLogPrefix = `[DepositMonitor Addr:${address.slice(0,6)}.. ID:${addressId}]`;
            try {
                 await client.query('UPDATE deposit_addresses SET last_checked_at = NOW() WHERE id = $1', [addressId]);
                 const pubKey = new PublicKey(address);
                 const signatures = await solanaConnection.getSignaturesForAddress(pubKey, { limit: sigFetchLimit }, DEPOSIT_CONFIRMATION_LEVEL);
                 if (signatures && signatures.length > 0) {
                     for (const sigInfo of signatures) {
                         if (sigInfo?.signature && !hasProcessedDepositTx(sigInfo.signature)) {
                             if (!sigInfo.err && (sigInfo.confirmationStatus === DEPOSIT_CONFIRMATION_LEVEL || sigInfo.confirmationStatus === 'finalized')) {
                                  console.log(`${addrLogPrefix} Found new confirmed TX: ${sigInfo.signature}. Queueing.`);
                                  depositProcessorQueue.add(() => processDepositTransaction(sigInfo.signature, address, addressId));
                                  addProcessedDepositTx(sigInfo.signature);
                             }
                         }
                     }
                 }
            } catch (error) {
                 console.error(`${addrLogPrefix} Error checking signatures: ${error.message}`);
                 if (isRetryableSolanaError(error) && (error?.status === 429 || String(error?.message).includes('429'))) { console.warn(`${addrLogPrefix} Rate limit, pausing...`); await sleep(1500); }
            }
        }
        await client.query('COMMIT');
    } catch (error) {
        console.error(`${logPrefix} Error in polling loop: ${error.message}`);
        if (client) await client.query('ROLLBACK').catch(() => {});
        await notifyAdmin(`🚨 ERROR in Deposit Monitor loop: ${escapeMarkdownV2(error.message)}`);
    } finally { if (client) client.release(); monitorDepositsPolling.isRunning = false; }
}
monitorDepositsPolling.isRunning = false;

/** Processes a single confirmed deposit transaction. */
async function processDepositTransaction(signature, depositAddress, depositAddressId) {
    const logPrefix = `[ProcessDeposit TX:${signature.slice(0,6)}.. AddrID:${depositAddressId}]`;
    console.log(`${logPrefix} Processing transaction...`);
    let client = null; let userIdForNotifyOnError = null;
    try {
        const addressInfo = await findDepositAddressInfo(depositAddress);
        if (!addressInfo) { console.error(`${logPrefix} CRITICAL: No user info for deposit address ${depositAddress}`); await notifyAdmin(`🚨 CRITICAL: No user found for deposit \`${escapeMarkdownV2(signature)}\`.`); addProcessedDepositTx(signature); return; }
        userIdForNotifyOnError = addressInfo.userId; const userId = addressInfo.userId;
        const tx = await solanaConnection.getTransaction(signature, { maxSupportedTransactionVersion: 0, commitment: DEPOSIT_CONFIRMATION_LEVEL });
        if (!tx) { console.warn(`${logPrefix} TX details not found.`); return; }
        if (tx.meta?.err) { console.log(`${logPrefix} TX failed on-chain.`); addProcessedDepositTx(signature); return; }
        console.log(`${logPrefix} Found User ${userId}. Fetched TX.`);
        const { transferAmount, payerAddress } = analyzeTransactionAmounts(tx, depositAddress);
        if (transferAmount <= 0n) { console.log(`${logPrefix} No positive transfer found.`); addProcessedDepositTx(signature); return; }
        const depositAmountSOL = (Number(transferAmount) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
        console.log(`${logPrefix} Deposit detected: ${depositAmountSOL} SOL.`);
        client = await pool.connect(); await client.query('BEGIN'); console.log(`${logPrefix} DB TX started.`);
        const depositRecordResult = await recordConfirmedDeposit(client, userId, depositAddressId, signature, transferAmount);
        if (!depositRecordResult.success && !depositRecordResult.alreadyProcessed) throw new Error(`DB record error: ${depositRecordResult.error}`);
        else if (depositRecordResult.alreadyProcessed) { console.warn(`${logPrefix} Already processed.`); await client.query('ROLLBACK'); client.release(); client = null; addProcessedDepositTx(signature); return; }
        const depositId = depositRecordResult.depositId; console.log(`${logPrefix} Deposit recorded (ID: ${depositId}).`);
        await markDepositAddressUsed(client, depositAddressId); console.log(`${logPrefix} Marked address used.`);
        const ledgerNote = `Deposit from ${payerAddress ? payerAddress.slice(0,6)+'..' : 'Unknown'} TX:${signature.slice(0,6)}..`;
        const balanceUpdateResult = await updateUserBalanceAndLedger(client, userId, transferAmount, 'deposit', { depositId }, ledgerNote);
        if (!balanceUpdateResult.success) throw new Error(`Balance update error: ${balanceUpdateResult.error}`);
        const newBalanceSOL = (Number(balanceUpdateResult.newBalance) / LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
        console.log(`${logPrefix} Balance updated. New: ${newBalanceSOL} SOL.`);
        const pendingRef = pendingReferrals.get(userId);
        if (pendingRef) { /* Handle referral linking */
            const refereeWalletDetails = await queryDatabase('SELECT referred_by_user_id FROM wallets WHERE user_id = $1', [userId], client);
            if (!refereeWalletDetails.rows[0]?.referred_by_user_id) {
                if (await linkReferral(userId, pendingRef.referrerUserId, client)) {
                     try { const refereeName = await getUserDisplayName(pendingRef.referrerUserId, userId); await safeSendMessage(pendingRef.referrerUserId, `🤝 Referral ${refereeName} deposited\\!`, {parse_mode: 'MarkdownV2'}).catch(()=>{}); } catch (e) {}
                     pendingReferrals.delete(userId); console.log(`${logPrefix} Referral linked.`);
                } else { pendingReferrals.delete(userId); console.warn(`${logPrefix} Referral link failed.`); }
            } else { pendingReferrals.delete(userId); console.log(`${logPrefix} Already referred.`); }
        }
        await client.query('COMMIT'); console.log(`${logPrefix} DB TX committed.`);
        await safeSendMessage(userId, `✅ Deposit Confirmed\\! \nAmount: *${escapeMarkdownV2(depositAmountSOL)} SOL*\nNew Balance: *${escapeMarkdownV2(newBalanceSOL)} SOL*\nTX: \`${escapeMarkdownV2(signature)}\`\n\nUse /start\\!`, { parse_mode: 'MarkdownV2' });
        addProcessedDepositTx(signature); console.log(`${logPrefix} Processing complete.`);
    } catch (error) {
        console.error(`${logPrefix} CRITICAL ERROR processing deposit: ${error.message}`, error.stack);
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { console.error(`${logPrefix} Rollback failed:`, rbErr); } }
        await notifyAdmin(`🚨 CRITICAL Error Processing Deposit TX \`${escapeMarkdownV2(signature)}\` Addr \`${escapeMarkdownV2(depositAddress)}\` User ${userIdForNotifyOnError}:\n${escapeMarkdownV2(error.message)}`);
        addProcessedDepositTx(signature); // Cache error signature
    } finally { if (client) client.release(); }
}


// --- Deposit Sweeper ---

/** Starts the deposit address sweeping background task. */
function startDepositSweeper() {
    let intervalMs = parseInt(process.env.SWEEP_INTERVAL_MS, 10);
     if (isNaN(intervalMs) || intervalMs <= 0) { console.warn("⚠️ Deposit sweeping is disabled."); return; }
     if (intervalMs < 60000) { intervalMs = 60000; console.warn(`SWEEP_INTERVAL_MS too low, using ${intervalMs}ms.`); }
    if (sweepIntervalId) { clearInterval(sweepIntervalId); console.log('🔄 Restarting deposit sweeper...'); }
    else { console.log(`⚙️ Starting Deposit Sweeper (Interval: ${intervalMs / 1000}s)...`); }
    setTimeout(() => { // Delay first run
         sweepDepositAddresses().catch(err => console.error("[Initial Sweep Run] Error:", err));
         sweepIntervalId = setInterval(sweepDepositAddresses, intervalMs);
         if (sweepIntervalId.unref) sweepIntervalId.unref();
    }, 15000); // e.g., 15s delay
}

/**
 * Finds 'used' deposit addresses and sweeps funds (minus rent/buffer) to the main bot wallet.
 * Handles retries for transient network errors during sweeps.
 */
async function sweepDepositAddresses() {
    const logPrefix = '[DepositSweeper]';
    if (sweepDepositAddresses.isRunning) { return; }
    sweepDepositAddresses.isRunning = true;
    console.log(`${logPrefix} Starting sweep cycle...`);
    let addressesProcessed = 0, addressesSwept = 0, sweepErrors = 0;

    try {
        const batchSize = SWEEP_BATCH_SIZE;
        const sweepTargetAddress = Keypair.fromSecretKey(bs58.decode(process.env.MAIN_BOT_PRIVATE_KEY)).publicKey;
        const delayBetweenAddresses = SWEEP_ADDRESS_DELAY_MS;
        const maxRetryAttempts = SWEEP_RETRY_ATTEMPTS + 1;
        const retryDelay = SWEEP_RETRY_DELAY_MS;

        // --- FIX: Convert rentLamports to BigInt ---
        const rentLamports = BigInt(await solanaConnection.getMinimumBalanceForRentExemption(0));
        const minimumLamportsToLeave = rentLamports + SWEEP_FEE_BUFFER_LAMPORTS;
        console.log(`${logPrefix} Min balance to leave: ${minimumLamportsToLeave} (Rent: ${rentLamports}, Buffer: ${SWEEP_FEE_BUFFER_LAMPORTS})`);

        const addressesRes = await queryDatabase(`
            SELECT id, deposit_address, derivation_path
            FROM deposit_addresses WHERE status = 'used' ORDER BY created_at ASC LIMIT $1
        `, [batchSize]);

        if (addressesRes.rowCount === 0) { sweepDepositAddresses.isRunning = false; return; }
        console.log(`${logPrefix} Found ${addressesRes.rowCount} 'used' addresses.`);

        for (const row of addressesRes.rows) {
            addressesProcessed++;
            const addressId = row.id; const addressString = row.deposit_address; const derivationPath = row.derivation_path;
            const addrLogPrefix = `[Sweep Addr:${addressString.slice(0,6)}.. ID:${addressId}]`;

            try {
                 const depositKeypair = getKeypairFromPath(derivationPath);
                 if (!depositKeypair) throw new Error("Failed to derive keypair.");

                 // --- FIX: Convert balanceLamports to BigInt ---
                 const balanceLamportsBigInt = BigInt(await solanaConnection.getBalance(depositKeypair.publicKey, DEPOSIT_CONFIRMATION_LEVEL));
                 // console.log(`${addrLogPrefix} Balance: ${balanceLamportsBigInt} lamports.`); // Verbose

                 // --- FIX: Use BigInt for comparison ---
                 if (balanceLamportsBigInt <= minimumLamportsToLeave) {
                      console.log(`${addrLogPrefix} Balance (${balanceLamportsBigInt}) too low. Marking swept.`);
                      await queryDatabase("UPDATE deposit_addresses SET status = 'swept' WHERE id = $1 AND status = 'used'", [addressId]);
                      continue;
                 }

                 // --- FIX: Use BigInt for calculation ---
                 const amountToSweep = balanceLamportsBigInt - minimumLamportsToLeave;
                 console.log(`${addrLogPrefix} Sweeping ${amountToSweep} lamports...`);

                 let sweepSuccess = false, sweepSignature = null, lastError = null;
                 for (let attempt = 1; attempt <= maxRetryAttempts; attempt++) {
                    try {
                         const { blockhash, lastValidBlockHeight } = await solanaConnection.getLatestBlockhash(DEPOSIT_CONFIRMATION_LEVEL);
                         const transaction = new Transaction({ recentBlockhash: blockhash, feePayer: depositKeypair.publicKey })
                             .add( SystemProgram.transfer({ fromPubkey: depositKeypair.publicKey, toPubkey: sweepTargetAddress, lamports: amountToSweep }) );

                         // console.log(`${addrLogPrefix} Sending sweep TX (Attempt ${attempt}/${maxRetryAttempts})...`); // Verbose
                         sweepSignature = await sendAndConfirmTransaction(
                             solanaConnection, transaction, [depositKeypair],
                             { commitment: DEPOSIT_CONFIRMATION_LEVEL, skipPreflight: false, preflightCommitment: DEPOSIT_CONFIRMATION_LEVEL, lastValidBlockHeight }
                         );
                         sweepSuccess = true; console.log(`${addrLogPrefix} ✅ Sweep successful. TX: ${sweepSignature}`); break;
                    } catch (error) {
                         lastError = error; console.warn(`${addrLogPrefix} Sweep Attempt ${attempt}/${maxRetryAttempts} FAILED: ${error.message}`);
                         if (isRetryableSolanaError(error) && attempt < maxRetryAttempts) await sleep(retryDelay);
                         else break;
                    }
                 } // End Retry Loop

                 if (sweepSuccess && sweepSignature) {
                     await queryDatabase("UPDATE deposit_addresses SET status = 'swept' WHERE id = $1 AND status = 'used'", [addressId]);
                     // console.log(`${addrLogPrefix} Marked swept.`); // Verbose
                      addressesSwept++;
                 } else {
                     sweepErrors++; const finalErrorMsg = lastError?.message || "Unknown failure";
                     console.error(`${addrLogPrefix} Sweep failed: ${finalErrorMsg}`);
                     await notifyAdmin(`🚨 SWEEP FAILED (TX) Addr \`${escapeMarkdownV2(addressString)}\` (ID: ${addressId}): ${escapeMarkdownV2(finalErrorMsg)}. Manual check needed.`);
                 }
            } catch (processError) {
                 sweepErrors++; console.error(`${addrLogPrefix} Error processing address: ${processError.message}`, processError.stack);
                 await notifyAdmin(`🚨 SWEEP FAILED (Proc Err) Addr \`${escapeMarkdownV2(addressString)}\` (ID: ${addressId}): ${escapeMarkdownV2(processError.message)}. Manual check needed.`);
            }
            if (delayBetweenAddresses > 0 && addressesProcessed < addressesRes.rowCount) await sleep(delayBetweenAddresses);
        } // End for loop

        console.log(`${logPrefix} Sweep cycle finished. Processed: ${addressesProcessed}, Swept: ${addressesSwept}, Errors: ${sweepErrors}.`);
    } catch (error) {
        console.error(`${logPrefix} CRITICAL ERROR in sweep cycle: ${error.message}`, error.stack);
        await notifyAdmin(`🚨 CRITICAL Error in Deposit Sweeper loop: ${escapeMarkdownV2(error.message)}.`);
    } finally {
        sweepDepositAddresses.isRunning = false;
    }
}
sweepDepositAddresses.isRunning = false;


// --- Payout Processing ---
/** Adds a job to the payout queue with retry logic handled by the job itself */
async function addPayoutJob(jobData, _retries = 0, _retryDelay = 0) {
    const logPrefix = `[AddPayoutJob Type:${jobData?.type} ID:${jobData?.withdrawalId || jobData?.payoutId}]`;
    return payoutProcessorQueue.add(async () => {
        let attempts = 0; const maxAttempts = parseInt(process.env.PAYOUT_JOB_RETRIES, 10) + 1; const baseDelay = parseInt(process.env.PAYOUT_JOB_RETRY_DELAY_MS, 10);
        while(attempts < maxAttempts) {
            attempts++;
            try {
                 if (jobData.type === 'payout_withdrawal') await handleWithdrawalPayoutJob(jobData.withdrawalId);
                 else if (jobData.type === 'payout_referral') await handleReferralPayoutJob(jobData.payoutId);
                 else throw new Error(`Unknown payout job type: ${jobData.type}`);
                 return; // Success
             } catch(error) {
                 console.warn(`${logPrefix} Attempt ${attempts}/${maxAttempts} failed: ${error.message}`);
                 const isRetryable = error.isRetryable ?? false;
                 if (!isRetryable || attempts >= maxAttempts) { console.error(`${logPrefix} Job failed permanently.`); throw error; }
                 const delay = baseDelay * Math.pow(2, attempts - 1) * (1 + (Math.random() - 0.5) * 0.2);
                 console.log(`${logPrefix} Retrying in ${Math.round(delay / 1000)}s...`); await sleep(delay);
            }
        }
    }).catch(queueError => { console.error(`${logPrefix} Error adding/processing job: ${queueError.message}`); notifyAdmin(`🚨 ERROR in Payout Queue. Type: ${jobData?.type}, ID: ${jobData?.withdrawalId || jobData?.payoutId}. Error: ${escapeMarkdownV2(queueError.message)}`).catch(()=>{}); });
}
/** Handles withdrawal payout jobs. Throws error with isRetryable flag for retry logic. */
async function handleWithdrawalPayoutJob(withdrawalId) {
    const logPrefix = `[WithdrawJob ID:${withdrawalId}]`;
    const details = await getWithdrawalDetails(withdrawalId);
    if (!details) throw new Error(`Withdrawal details not found.`);
    if (details.status === 'completed') { console.log(`${logPrefix} Already completed.`); return; }
    const userId = details.user_id, recipient = details.recipient_address, amount = details.final_send_amount_lamports, amountSOL = (Number(amount)/LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
    try {
         await updateWithdrawalDbStatus(withdrawalId, 'processing');
         const sendResult = await sendSol(recipient, amount, 'withdrawal');
         if (sendResult.success && sendResult.signature) {
              await updateWithdrawalDbStatus(withdrawalId, 'completed', null, sendResult.signature);
              console.log(`${logPrefix} Marked completed. TX: ${sendResult.signature}`);
              await safeSendMessage(userId, `✅ Withdrawal Completed\\! \nAmount: *${escapeMarkdownV2(amountSOL)} SOL* sent to \`${escapeMarkdownV2(recipient)}\`\\.\nTX: \`${escapeMarkdownV2(sendResult.signature)}\``, { parse_mode: 'MarkdownV2' });
         } else {
              const finalErrorMsg = sendResult.error || 'Unknown sendSol failure';
              await updateWithdrawalDbStatus(withdrawalId, 'failed', null, null, finalErrorMsg);
              console.log(`${logPrefix} Marked failed.`);
              await safeSendMessage(userId, `❌ Withdrawal Failed\\! \nReason: ${escapeMarkdownV2(finalErrorMsg)}\nPlease contact support\\.`, { parse_mode: 'MarkdownV2'});
              await notifyAdmin(`🚨 WITHDRAWAL FAILED (User ${userId}, ID ${withdrawalId}): ${escapeMarkdownV2(finalErrorMsg)}`);
              const error = new Error(finalErrorMsg); error.isRetryable = sendResult.isRetryable ?? false; throw error;
         }
    } catch (jobError) {
         console.error(`${logPrefix} Error handling job: ${jobError.message}`);
         const jobProcessingError = `Job error: ${jobError.message}`;
         await updateWithdrawalDbStatus(withdrawalId, 'failed', null, null, jobProcessingError).catch(()=>{});
         if (jobError.isRetryable === undefined) jobError.isRetryable = false; throw jobError;
    }
}
/** Handles referral payout jobs. Throws error with isRetryable flag for retry logic. */
async function handleReferralPayoutJob(payoutId) {
    const logPrefix = `[ReferralJob ID:${payoutId}]`;
    const details = await getReferralPayoutDetails(payoutId);
    if (!details) throw new Error(`Referral payout details not found.`);
    if (details.status === 'paid') { console.log(`${logPrefix} Already paid.`); return; }
    const referrerUserId = details.referrer_user_id, amount = details.payout_amount_lamports, amountSOL = (Number(amount)/LAMPORTS_PER_SOL).toFixed(SOL_DECIMALS);
    let recipientAddress = null;
    try {
        const referrerDetails = await getUserWalletDetails(referrerUserId);
        if (!referrerDetails?.external_withdrawal_address) throw new Error(`Referrer ${referrerUserId} has no linked wallet.`);
        recipientAddress = referrerDetails.external_withdrawal_address;
        await updateReferralPayoutStatus(payoutId, 'processing');
        const sendResult = await sendSol(recipientAddress, amount, 'referral');
        if (sendResult.success && sendResult.signature) {
            await updateReferralPayoutStatus(payoutId, 'paid', null, sendResult.signature);
            console.log(`${logPrefix} Marked paid. TX: ${sendResult.signature}`);
            await safeSendMessage(referrerUserId, `💰 Referral Reward Paid\\! \nAmount: *${escapeMarkdownV2(amountSOL)} SOL* sent to \`${escapeMarkdownV2(recipientAddress)}\`\\.\nTX: \`${escapeMarkdownV2(sendResult.signature)}\``, { parse_mode: 'MarkdownV2' });
        } else {
             const finalErrorMsg = sendResult.error || 'Unknown sendSol failure';
             await updateReferralPayoutStatus(payoutId, 'failed', null, null, finalErrorMsg);
             console.log(`${logPrefix} Marked failed.`);
             await safeSendMessage(referrerUserId, `❌ Referral Reward Failed\\! \nPayout failed\\. Reason: ${escapeMarkdownV2(finalErrorMsg)}\nContact support\\.`, { parse_mode: 'MarkdownV2'});
             await notifyAdmin(`🚨 REFERRAL PAYOUT FAILED (Referrer ${referrerUserId}, ID ${payoutId}): ${escapeMarkdownV2(finalErrorMsg)}`);
             const error = new Error(finalErrorMsg); error.isRetryable = sendResult.isRetryable ?? false; throw error;
        }
    } catch (jobError) {
         console.error(`${logPrefix} Error handling job: ${jobError.message}`);
         const jobProcessingError = `Job error: ${jobError.message}`;
         await updateReferralPayoutStatus(payoutId, 'failed', null, null, jobProcessingError).catch(()=>{});
         if (jobError.isRetryable === undefined) jobError.isRetryable = false;
         if (jobError.message.includes("no linked wallet")) jobError.isRetryable = false;
         throw jobError;
    }
}

// --- Startup and Shutdown ---
/** Setup Telegram: Set Webhook or start Polling */
async function setupTelegramConnection() {
     console.log('⚙️ Configuring Telegram connection (Webhook/Polling)...');
     let webhookUrl = process.env.RAILWAY_PUBLIC_DOMAIN ? `https://${process.env.RAILWAY_PUBLIC_DOMAIN}` : process.env.WEBHOOK_URL;
     if (webhookUrl) {
          const webhookPath = `/telegram/${process.env.BOT_TOKEN}`; webhookUrl = `${webhookUrl}${webhookPath}`; console.log(`Attempting to set webhook: ${webhookUrl}`);
          try {
              await bot.deleteWebHook({ drop_pending_updates: true }).catch(() => {});
              await bot.setWebHook(webhookUrl, { max_connections: parseInt(process.env.WEBHOOK_MAX_CONN, 10) || 10, allowed_updates: ['message', 'callback_query'] });
              console.log(`✅ Telegram Webhook set successfully.`); bot.options.polling = false; return true;
          } catch (error) { console.error(`❌ Failed Webhook: ${error.message}. Falling back.`); await notifyAdmin(`⚠️ WARNING: Webhook Fail (${escapeMarkdownV2(webhookUrl)}): ${escapeMarkdownV2(error.message)}\\. Falling back to polling\\.`).catch(()=>{}); return false; }
     } else { console.log('Webhook URL not configured, using polling.'); return false; }
}
/** Start polling if webhook wasn't used or failed */
async function startPollingFallback() {
      console.log('⚙️ Starting Polling for Telegram updates...');
      try {
          await bot.deleteWebHook({ drop_pending_updates: true }).catch(() => {}); bot.options.polling = true;
          await bot.startPolling({ polling: { interval: 300, params: { timeout: 10 } } }); console.log('✅ Telegram Polling started.');
      } catch (err) { console.error(`❌ CRITICAL Polling Fail: ${err.message}`); await notifyAdmin(`🚨 CRITICAL POLLING FAILED: ${escapeMarkdownV2(err.message)}. Exiting.`).catch(()=>{}); process.exit(3); }
}
/** Configure Express server routes and start listening */
function setupExpressServer() {
     const port = process.env.PORT || 3000;
     app.get('/', (req, res) => res.send(`Solana Gambles Bot v${process.env.npm_package_version || '3.1.7'} Alive! ${new Date().toISOString()}`));
     app.get('/health', (req, res) => res.status(isFullyInitialized ? 200 : 503).json({ status: isFullyInitialized ? 'OK' : 'INITIALIZING' }));
     if (!bot.options.polling && process.env.BOT_TOKEN) {
         const webhookPath = `/telegram/${process.env.BOT_TOKEN}`; app.post(webhookPath, (req, res) => { bot.processUpdate(req.body); res.sendStatus(200); }); console.log(`Webhook endpoint configured at ${webhookPath}`);
     }
     server = app.listen(port, '0.0.0.0', () => { console.log(`✅ Express server listening on 0.0.0.0:${port}`); });
     server.on('error', async (error) => { console.error(`❌ Express Server Error: ${error.message}`); await notifyAdmin(`🚨 CRITICAL Express Server Error: ${escapeMarkdownV2(error.message)}. Restart required.`).catch(()=>{}); process.exit(4); });
}
/** Graceful shutdown handler */
let isShuttingDown = false;
async function shutdown(signal) {
     if (isShuttingDown) { console.warn("Shutdown already in progress..."); return; } isShuttingDown = true; console.warn(`\n🚦 Received ${signal}. Initiating graceful shutdown...`); await notifyAdmin(`ℹ️ Bot instance shutting down (Signal: ${escapeMarkdownV2(signal)})...`).catch(()=>{}); isFullyInitialized = false;
     if (bot?.isPolling?.()) await bot.stopPolling({ cancel: true }).catch(e => console.error("Polling stop error:", e)); else if(bot) await bot.deleteWebHook({ drop_pending_updates: false }).catch(e => console.error("Webhook delete error:", e));
     if (server) { await new Promise(resolve => server.close(err => { if(err) console.error("HTTP close error:", err); resolve(); })); console.log("HTTP server closed."); }
     console.log("Stopping background intervals..."); if (depositMonitorIntervalId) clearInterval(depositMonitorIntervalId); depositMonitorIntervalId = null; if (sweepIntervalId) clearInterval(sweepIntervalId); sweepIntervalId = null;
     console.log("Waiting for queues to idle..."); const queueTimeout = parseInt(process.env.SHUTDOWN_QUEUE_TIMEOUT_MS, 10); const allQueues = [messageQueue, callbackQueue, payoutProcessorQueue, depositProcessorQueue, telegramSendQueue];
     try { await Promise.race([ Promise.all(allQueues.map(q => q.onIdle())), sleep(queueTimeout).then(() => Promise.reject(new Error(`Queue idle timeout (${queueTimeout}ms)`))) ]); console.log("Queues idle."); } catch (queueError) { console.warn(`Queue idle timeout/error: ${queueError.message}`); }
     console.log("Closing DB pool..."); await pool.end().then(() => console.log("DB pool closed.")).catch(e => console.error("DB pool close error:", e.message));
     console.log(`🏁 Shutdown complete (Signal: ${signal}). Exiting.`); process.exit(signal === 'SIGINT' || signal === 'SIGTERM' ? 0 : 1);
}
// Setup Signal Handlers & Global Error Handlers
process.on('SIGINT', () => shutdown('SIGINT')); process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('uncaughtException', async (error, origin) => { console.error(`\n🚨🚨🚨 UNCAUGHT EXCEPTION [${origin}] 🚨🚨🚨`, error); isFullyInitialized = false; if (!isShuttingDown) { await notifyAdmin(`🚨🚨🚨 UNCAUGHT EXCEPTION (${escapeMarkdownV2(origin)}) 🚨🚨🚨\n${escapeMarkdownV2(error.message)}\nShutting down...`).catch(()=>{}); await shutdown('uncaughtException').catch(() => {}); } setTimeout(() => process.exit(1), parseInt(process.env.SHUTDOWN_FAIL_TIMEOUT_MS, 10)).unref(); });
process.on('unhandledRejection', async (reason, promise) => { console.error('\n🔥🔥🔥 UNHANDLED REJECTION 🔥🔥🔥', reason); isFullyInitialized = false; await notifyAdmin(`🔥🔥🔥 UNHANDLED REJECTION 🔥🔥🔥\nReason: ${escapeMarkdownV2(String(reason))}`).catch(()=>{}); });

// --- Main Application Start ---
(async () => {
    const botVersion = process.env.npm_package_version || '3.1.7'; // Update version marker
    console.log(`\n🚀 Initializing Solana Gambles Bot v${botVersion}...`);
    try {
        setupTelegramListeners(); // Setup listeners early
        await initializeDatabase();
        await loadActiveDepositsCache();
        const useWebhook = await setupTelegramConnection();
        setupExpressServer(); // Start listening
        if (!useWebhook) await startPollingFallback(); // Start polling if needed
        // Start background tasks *after* main setup, with delay
        const initDelay = parseInt(process.env.INIT_DELAY_MS, 10);
        console.log(`⚙️ Scheduling background tasks (Monitor, Sweeper) in ${initDelay / 1000}s...`);
        setTimeout(() => {
            console.log("🚀 Starting Background Tasks...");
            startDepositMonitor();
            startDepositSweeper();
            isFullyInitialized = true;
            console.log("✅ Background Tasks Started.");
            notifyAdmin(`✅ Bot v${escapeMarkdownV2(botVersion)} Started \\(Mode: ${useWebhook ? 'Webhook' : 'Polling'}\\)`).catch(()=>{});
            console.log(`\n🎉🎉🎉 Bot is fully operational! (${new Date().toISOString()}) 🎉🎉🎉`);
        }, initDelay);
    } catch (error) {
        console.error("❌❌❌ FATAL ERROR DURING STARTUP SEQUENCE ❌❌❌:", error);
        await pool.end().catch(() => {});
        await notifyAdmin(`🚨 BOT STARTUP FAILED: ${escapeMarkdownV2(error.message)}. Exiting.`).catch(()=>{});
        process.exit(1);
    }
})();

/** Sets up global Telegram bot event listeners */
function setupTelegramListeners() { // Define function used in startup
    console.log("⚙️ Setting up Telegram event listeners (for polling mode)...");
     bot.removeAllListeners('message'); bot.removeAllListeners('callback_query'); bot.removeAllListeners('polling_error'); bot.removeAllListeners('webhook_error'); bot.removeAllListeners('error'); // Clear existing first
    bot.on('message', (msg) => { if (bot.options.polling) messageQueue.add(() => handleMessage(msg)).catch(e => console.error(`[MsgQueueErr]: ${e.message}`)); });
    bot.on('callback_query', (cb) => { if (bot.options.polling) callbackQueue.add(() => handleCallbackQuery(cb)).catch(e => { console.error(`[CBQueueErr]: ${e.message}`); bot.answerCallbackQuery(cb.id).catch(()=>{}); }); });
    bot.on('polling_error', (error) => { console.error(`❌ TG Polling Error: ${error.code} ${error.message}`); if (String(error.message).includes('conflict')) { notifyAdmin(`🚨 POLLING CONFLICT (409)`).catch(()=>{}); shutdown('POLLING_CONFLICT').catch(() => process.exit(1)); } });
    bot.on('webhook_error', (error) => console.error(`❌ TG Webhook Error: ${error.code} ${error.message}`));
    bot.on('error', (error) => { console.error('❌ General Bot Error:', error); notifyAdmin(`🚨 General TG Bot Error:\n${escapeMarkdownV2(error.message)}`).catch(()=>{}); });
    console.log("✅ Telegram polling listeners ready (if polling is active).");
}

// --- End of Part 6 / End of File index.js ---
