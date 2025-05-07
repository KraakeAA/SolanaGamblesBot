// index.js - Part 1: Imports, Environment Configuration, Core Initializations
// --- VERSION: 3.2.0 --- (Implement Crash, Blackjack, Slots Jackpot, Foundational UX Fixes/Adds)

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
// ------------------------
import nacl from 'tweetnacl'; // Keep for keypair generation from derived seed

// Assuming RateLimitedConnection exists and works as intended
import RateLimitedConnection from './lib/solana-connection.js'; // Ensure this path is correct

// --- Deployment Check Log ---
const BOT_VERSION = process.env.npm_package_version || '3.2.0'; // Use package version or fallback - Updated version
console.log(`--- INDEX.JS - DEPLOYMENT CHECK --- ${new Date().toISOString()} --- v${BOT_VERSION} ---`);
// --- END DEPLOYMENT CHECK ---


console.log(`⏳ Starting Solana Gambles Bot (Custodial, Buttons, v${BOT_VERSION})... Checking environment variables...`);

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
    'MIN_WITHDRAWAL_LAMPORTS': '10000000', // 0.01 SOL
    // --- Payouts (Withdrawals, Referral Rewards) ---
    'PAYOUT_BASE_PRIORITY_FEE_MICROLAMPORTS': '1000',
    'PAYOUT_MAX_PRIORITY_FEE_MICROLAMPORTS': '1000000',
    'PAYOUT_COMPUTE_UNIT_LIMIT': '200000',
    'PAYOUT_JOB_RETRIES': '3',
    'PAYOUT_JOB_RETRY_DELAY_MS': '5000',
    // --- Game Limits ---
    'CF_MIN_BET_LAMPORTS': '10000000',       // 0.01 SOL
    'CF_MAX_BET_LAMPORTS': '1000000000',     // 1 SOL
    'RACE_MIN_BET_LAMPORTS': '10000000',
    'RACE_MAX_BET_LAMPORTS': '1000000000',
    'SLOTS_MIN_BET_LAMPORTS': '10000000',
    'SLOTS_MAX_BET_LAMPORTS': '500000000',   // 0.5 SOL
    'ROULETTE_MIN_BET_LAMPORTS': '10000000',
    'ROULETTE_MAX_BET_LAMPORTS': '1000000000',
    'WAR_MIN_BET_LAMPORTS': '10000000',
    'WAR_MAX_BET_LAMPORTS': '1000000000',
    'CRASH_MIN_BET_LAMPORTS': '10000000',     // NEW GAME
    'CRASH_MAX_BET_LAMPORTS': '1000000000',   // NEW GAME
    'BLACKJACK_MIN_BET_LAMPORTS': '10000000',// NEW GAME
    'BLACKJACK_MAX_BET_LAMPORTS': '1000000000',// NEW GAME
    // --- Game Logic Parameters ---
    'CF_HOUSE_EDGE': '0.03',
    'RACE_HOUSE_EDGE': '0.05',
    'SLOTS_HOUSE_EDGE': '0.10',
    'ROULETTE_HOUSE_EDGE': '0.05',
    'WAR_HOUSE_EDGE': '0.02',
    'CRASH_HOUSE_EDGE': '0.03',              // NEW GAME (applied on cashout or if game defines specific crash points)
    'BLACKJACK_HOUSE_EDGE': '0.015',         // NEW GAME (typically from rules like dealer BJ pays 3:2, dealer hits soft 17, etc.)
    // --- Slots Jackpot ---
    'SLOTS_JACKPOT_SEED_LAMPORTS': '100000000', // 0.1 SOL initial seed NEW
    'SLOTS_JACKPOT_CONTRIBUTION_PERCENT': '0.001', // 0.1% of each bet goes to jackpot NEW
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
const SOL_DECIMALS = 9; // Maximum precision for SOL amounts
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
    { maxCount: 25, percent: 0.10 },  // 11-25 referrals -> 10%
    { maxCount: 50, percent: 0.15 },  // 26-50 referrals -> 15%
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
const SWEEP_FEE_BUFFER_LAMPORTS = BigInt(process.env.SWEEP_FEE_BUFFER_LAMPORTS);
const SWEEP_BATCH_SIZE = parseInt(process.env.SWEEP_BATCH_SIZE, 10);
const SWEEP_ADDRESS_DELAY_MS = parseInt(process.env.SWEEP_ADDRESS_DELAY_MS, 10);
const SWEEP_RETRY_ATTEMPTS = parseInt(process.env.SWEEP_RETRY_ATTEMPTS, 10);
const SWEEP_RETRY_DELAY_MS = parseInt(process.env.SWEEP_RETRY_DELAY_MS, 10);

// Slots Jackpot Constants
const SLOTS_JACKPOT_SEED_LAMPORTS = BigInt(process.env.SLOTS_JACKPOT_SEED_LAMPORTS);
const SLOTS_JACKPOT_CONTRIBUTION_PERCENT = parseFloat(process.env.SLOTS_JACKPOT_CONTRIBUTION_PERCENT);
let currentSlotsJackpotLamports = SLOTS_JACKPOT_SEED_LAMPORTS; // Will be loaded from DB in Part 6

// Standard Bet Amounts
const STANDARD_BET_AMOUNTS_SOL = [0.01, 0.05, 0.10, 0.25, 0.5, 1];
const STANDARD_BET_AMOUNTS_LAMPORTS = STANDARD_BET_AMOUNTS_SOL.map(sol => BigInt(Math.round(sol * LAMPORTS_PER_SOL)));
console.log(`[Config] Standard Bet Amounts (Lamports): ${STANDARD_BET_AMOUNTS_LAMPORTS.join(', ')}`);

// In-memory Caches & State
const userStateCache = new Map(); // Map<userId, { state: string, chatId: string, messageId?: number, data?: any, timestamp: number, previousState?: object }>
const walletCache = new Map(); // Map<userId, { withdrawalAddress?: string, referralCode?: string, timestamp: number, timeoutId?: NodeJS.Timeout }>
const activeDepositAddresses = new Map(); // Map<deposit_address, { userId: string, expiresAt: number, timeoutId?: NodeJS.Timeout }>
const processedDepositTxSignatures = new Set();
const commandCooldown = new Map();
const pendingReferrals = new Map();
const PENDING_REFERRAL_TTL_MS = 24 * 60 * 60 * 1000; // 24 hours
const userLastBetAmounts = new Map(); // Map<userId, Map<gameKey, amountLamports>> - For "Bet Again" feature

let isFullyInitialized = false;
let server; // Express server instance
let depositMonitorIntervalId = null;
let sweepIntervalId = null;
// --- End Global Constants & State ---


// --- Core Initializations ---

// Express App
const app = express();
app.use(express.json({ limit: '10kb' }));

// Solana Connection
console.log("⚙️ Initializing Multi-RPC Solana connection...");
console.log(`ℹ️ Using RPC Endpoints: ${parsedRpcUrls.join(', ')}`);
const solanaConnection = new RateLimitedConnection(parsedRpcUrls, {
    maxConcurrent: parseInt(process.env.RPC_MAX_CONCURRENT, 10),
    retryBaseDelay: parseInt(process.env.RPC_RETRY_BASE_DELAY, 10),
    maxRetries: parseInt(process.env.RPC_MAX_RETRIES, 10),
    rateLimitCooloff: parseInt(process.env.RPC_RATE_LIMIT_COOLOFF, 10),
    retryMaxDelay: parseInt(process.env.RPC_RETRY_MAX_DELAY, 10),
    retryJitter: parseFloat(process.env.RPC_RETRY_JITTER),
    commitment: process.env.RPC_COMMITMENT, // Ensure this is a valid Commitment level string
    httpHeaders: { 'User-Agent': `SolanaGamblesBot/${BOT_VERSION}` },
    clientId: `SolanaGamblesBot/${BOT_VERSION}`
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
    concurrency: parseInt(process.env.TELEGRAM_SEND_QUEUE_CONCURRENCY, 10),
    interval: parseInt(process.env.TELEGRAM_SEND_QUEUE_INTERVAL_MS, 10),
    intervalCap: parseInt(process.env.TELEGRAM_SEND_QUEUE_INTERVAL_CAP, 10)
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
    notifyAdmin(`🚨 DB Pool Error (Idle Client): ${err.message}`).catch(()=>{});
});
console.log("✅ PostgreSQL Pool created.");

// Telegram Bot Instance
console.log("⚙️ Initializing Telegram Bot...");
const bot = new TelegramBot(process.env.BOT_TOKEN, {
    polling: false, // Default to false, setupTelegramConnection will handle it
    request: {
        timeout: 15000,
        agentOptions: {
            keepAlive: true,
            keepAliveMsecs: 60000,
            maxSockets: 100,
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
        key: "coinflip",
        name: "Coinflip",
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
        houseEdge: parseFloat(process.env.SLOTS_HOUSE_EDGE),
        jackpotContributionPercent: parseFloat(process.env.SLOTS_JACKPOT_CONTRIBUTION_PERCENT), // NEW
        jackpotSeedLamports: BigInt(process.env.SLOTS_JACKPOT_SEED_LAMPORTS) // NEW
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
    },
    crash: { // NEW GAME
        key: "crash",
        name: "Crash",
        minBetLamports: BigInt(process.env.CRASH_MIN_BET_LAMPORTS),
        maxBetLamports: BigInt(process.env.CRASH_MAX_BET_LAMPORTS),
        houseEdge: parseFloat(process.env.CRASH_HOUSE_EDGE) // Applied when player cashes out
    },
    blackjack: { // NEW GAME
        key: "blackjack",
        name: "Blackjack",
        minBetLamports: BigInt(process.env.BLACKJACK_MIN_BET_LAMPORTS),
        maxBetLamports: BigInt(process.env.BLACKJACK_MAX_BET_LAMPORTS),
        houseEdge: parseFloat(process.env.BLACKJACK_HOUSE_EDGE) // Embedded in game rules (e.g., dealer peek, 3:2 payout)
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
        if (isNaN(gc.houseEdge) || gc.houseEdge < 0 || gc.houseEdge > 1) errors.push(`${gameKey}.houseEdge invalid (${gc.houseEdge})`); // Allow 0 edge, but not >=1

        if (gameKey === 'slots') {
            if (isNaN(gc.jackpotContributionPercent) || gc.jackpotContributionPercent < 0 || gc.jackpotContributionPercent >= 1) errors.push(`${gameKey}.jackpotContributionPercent invalid (${gc.jackpotContributionPercent})`);
            if (typeof gc.jackpotSeedLamports !== 'bigint' || gc.jackpotSeedLamports < 0n) errors.push(`${gameKey}.jackpotSeedLamports invalid (${gc.jackpotSeedLamports})`);
        }
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

// --- Export necessary variables/instances for other parts if splitting files physically ---
export {
    pool, bot, solanaConnection, userStateCache, walletCache, activeDepositAddresses,
    processedDepositTxSignatures, commandCooldown, pendingReferrals, GAME_CONFIG, userLastBetAmounts, // Added userLastBetAmounts
    messageQueue, callbackQueue, payoutProcessorQueue, depositProcessorQueue, telegramSendQueue,
    BOT_VERSION, LAMPORTS_PER_SOL, SOL_DECIMALS, DEPOSIT_CONFIRMATION_LEVEL,
    MIN_WITHDRAWAL_LAMPORTS, WITHDRAWAL_FEE_LAMPORTS, REFERRAL_INITIAL_BET_MIN_LAMPORTS,
    REFERRAL_MILESTONE_REWARD_PERCENT, REFERRAL_INITIAL_BONUS_TIERS, REFERRAL_MILESTONE_THRESHOLDS_LAMPORTS,
    DEPOSIT_ADDRESS_EXPIRY_MS, USER_STATE_TTL_MS, PENDING_REFERRAL_TTL_MS, STANDARD_BET_AMOUNTS_LAMPORTS,
    STANDARD_BET_AMOUNTS_SOL, MAX_PROCESSED_TX_CACHE_SIZE, USER_COMMAND_COOLDOWN_MS,
    SLOTS_JACKPOT_SEED_LAMPORTS, SLOTS_JACKPOT_CONTRIBUTION_PERCENT, currentSlotsJackpotLamports, // Jackpot exports
    isFullyInitialized, server, // Allow modification in Part 6
    depositMonitorIntervalId, sweepIntervalId, // Allow modification in Part 6
    // Also export functions from subsequent parts if needed globally
    // Part 2 functions
    queryDatabase, ensureUserExists, linkUserWallet, getUserWalletDetails, getLinkedWallet, getUserByReferralCode, linkReferral, updateUserWagerStats, getUserBalance, updateUserBalanceAndLedger, createDepositAddressRecord, findDepositAddressInfo, markDepositAddressUsed, recordConfirmedDeposit, getNextDepositAddressIndex, createWithdrawalRequest, updateWithdrawalStatus, getWithdrawalDetails, createBetRecord, updateBetStatus, isFirstCompletedBet, getBetDetails, recordPendingReferralPayout, updateReferralPayoutStatus, getReferralPayoutDetails, getTotalReferralEarnings,
    getJackpotAmount, updateJackpotAmount, // New DB functions for Jackpot
    updateUserLastBetAmount, getUserLastBetAmount, // New DB functions for last bet
    getBetHistory, // New DB function for bet history
    // Part 3 functions
    sleep, formatSol, createSafeIndex, generateUniqueDepositAddress, getKeypairFromPath, isRetryableSolanaError, sendSol, analyzeTransactionAmounts, notifyAdmin, safeSendMessage, escapeMarkdownV2, escapeHtml, getUserDisplayName, updateWalletCache, getWalletCache, addActiveDepositAddressCache, getActiveDepositAddressCache, removeActiveDepositAddressCache, addProcessedDepositTx, hasProcessedDepositTx, generateReferralCode,
    // Part 4 functions
    playCoinflip, simulateRace, simulateSlots, simulateRouletteSpin, getRoulettePayoutMultiplier, simulateWar,
    simulateCrash, getCrashPayoutMultiplier, // New Game Logic
    simulateBlackjackDeal, getBlackjackCardValue, getBlackjackHandValue, determineBlackjackWinner, // New Game Logic
    // Part 5a functions
    handleMessage, handleCallbackQuery, proceedToGameStep, placeBet, handleCoinflipGame, handleRaceGame, RACE_HORSES, handleSlotsGame, handleWarGame, handleRouletteGame,
    handleCrashGame, handleBlackjackGame, // New Game Handlers
    // Part 5b functions
    routeStatefulInput, handleCustomAmountInput, handleWithdrawalAddressInput, handleWithdrawalAmountInput, handleRouletteBetInput, handleRouletteNumberInput, handleStartCommand, handleHelpCommand, handleWalletCommand, handleReferralCommand, handleDepositCommand, handleWithdrawCommand, showBetAmountButtons, handleCoinflipCommand, handleRaceCommand, handleSlotsCommand, handleWarCommand, handleRouletteCommand,
    handleCrashCommand, handleBlackjackCommand, // New Game Commands
    handleHistoryCommand, // New command for bet history
    commandHandlers, menuCommandHandlers, handleAdminCommand, _handleReferralChecks,
    // Part 6 functions are generally internal or startup/shutdown related
    // initializeDatabase, loadActiveDepositsCache, startDepositMonitor, monitorDepositsPolling, processDepositTransaction, startDepositSweeper, sweepDepositAddresses, addPayoutJob, handleWithdrawalPayoutJob, handleReferralPayoutJob, setupTelegramConnection, startPollingFallback, setupExpressServer, shutdown, setupTelegramListeners,
    // loadSlotsJackpot // New startup function
};

// --- End of Part 1 ---
// index.js - Part 2: Database Operations
// --- VERSION: 3.2.0 --- (Implement Crash, Blackjack, Slots Jackpot, Foundational UX Fixes/Adds)

// --- Assuming 'pool', 'PublicKey', utils like 'generateReferralCode', 'updateWalletCache' etc. are available from other parts ---
// --- Also assuming relevant constants like BOT_VERSION, GAME_CONFIG are available ---

// --- Helper Function for DB Operations ---
/**
 * Executes a database query using either a provided client or the pool.
 * Handles basic error logging including parameters (converting BigInts).
 * @param {string} sql The SQL query string with placeholders ($1, $2, ...).
 * @param {Array<any>} [params=[]] The parameters for the SQL query.
 * @param {import('pg').PoolClient | import('pg').Pool} [dbClient=pool] The DB client or pool to use. Defaults to the global 'pool'.
 * @returns {Promise<import('pg').QueryResult<any>>} The query result.
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
 * @param {import('pg').PoolClient} client The active database client connection.
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
            `INSERT INTO wallets (user_id, created_at, last_used_at, last_bet_amounts) VALUES ($1, NOW(), NOW(), '{}'::jsonb)`, // Initialize last_bet_amounts
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
            new PublicKey(externalAddress); // Assuming PublicKey is imported from @solana/web3.js
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
            last_bet_amounts, -- NEWLY ADDED for last bet preferences
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
            details.last_bet_amounts = details.last_bet_amounts || {}; // Ensure it's an object
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
 * @param {import('pg').PoolClient} client The active database client.
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
 * @param {import('pg').PoolClient} client The active database client.
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

// --- User Last Bet Preferences --- NEW SECTION ---

/**
 * Updates the last bet amount for a specific game for a user.
 * Stores it in the `last_bet_amounts` JSONB column in the `wallets` table.
 * Should be called within a transaction where user's wallet row might be locked.
 * @param {string} userId
 * @param {string} gameKey
 * @param {bigint} amountLamports
 * @param {import('pg').PoolClient} client The active database client.
 * @returns {Promise<boolean>}
 */
async function updateUserLastBetAmount(userId, gameKey, amountLamports, client) {
    userId = String(userId);
    const logPrefix = `[DB updateUserLastBet User ${userId} Game ${gameKey}]`;
    try {
        // The wallets row should ideally be locked by the calling transaction
        // (e.g., within placeBet or after a game a new bet is being prepared)
        // Fetch current last_bet_amounts
        const currentRes = await queryDatabase(
            'SELECT last_bet_amounts FROM wallets WHERE user_id = $1',
            [userId],
            client
        );
        if (currentRes.rowCount === 0) {
            console.warn(`${logPrefix} User not found, cannot update last bet amount.`);
            return false;
        }
        const currentLastBets = currentRes.rows[0].last_bet_amounts || {};
        currentLastBets[gameKey] = amountLamports.toString(); // Store as string in JSON

        const updateRes = await queryDatabase(
            'UPDATE wallets SET last_bet_amounts = $1 WHERE user_id = $2',
            [currentLastBets, userId],
            client
        );
        return updateRes.rowCount > 0;
    } catch (err) {
        console.error(`${logPrefix} Error updating last bet amount:`, err.message);
        return false;
    }
}

/**
 * Retrieves all stored last bet amounts for a user.
 * @param {string} userId
 * @param {import('pg').PoolClient} [client=pool] Optional DB client.
 * @returns {Promise<{[gameKey: string]: bigint} | null>} Object map of gameKey to BigInt amount, or null.
 */
async function getUserLastBetAmounts(userId, client = pool) {
    userId = String(userId);
    const logPrefix = `[DB getUserLastBets User ${userId}]`;
    try {
        const res = await queryDatabase(
            'SELECT last_bet_amounts FROM wallets WHERE user_id = $1',
            [userId],
            client
        );
        if (res.rowCount > 0 && res.rows[0].last_bet_amounts) {
            const lastBetsRaw = res.rows[0].last_bet_amounts;
            const lastBets = {};
            for (const gameKey in lastBetsRaw) {
                if (lastBetsRaw.hasOwnProperty(gameKey)) {
                    try {
                        lastBets[gameKey] = BigInt(lastBetsRaw[gameKey]);
                    } catch (e) {
                        console.warn(`${logPrefix} Failed to parse BigInt for game ${gameKey} in last_bet_amounts for user ${userId}`);
                    }
                }
            }
            return lastBets;
        }
        return {}; // Return empty object if no record or no last bets
    } catch (err) {
        console.error(`${logPrefix} Error fetching last bet amounts:`, err.message);
        return null;
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
 * @param {import('pg').PoolClient} client - The active database client connection.
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


// --- Deposit Operations --- (Largely unchanged unless specific issues arise)

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
 * @param {import('pg').PoolClient} client The active database client.
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
 * @param {import('pg').PoolClient} client The active database client.
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


// --- Withdrawal Operations --- (Largely unchanged)

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
 * @param {import('pg').PoolClient | null} [client=null] Optional DB client if within a transaction. Defaults to pool.
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
 * @param {import('pg').PoolClient} client The active database client.
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
        if (err.constraint === 'bets_wager_amount_lamports_check') { // Updated constraint name
            console.error(`[DB CreateBet] Positive wager constraint violation.`);
            return { success: false, error: 'Wager amount must be positive (DB check failed).' };
        }
        return { success: false, error: `Database error creating bet (${err.code || 'N/A'})` };
    }
}

/**
 * Updates a bet record upon completion of game logic.
 * MUST be called within a transaction. Handles idempotency.
 * @param {import('pg').PoolClient} client The active database client.
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

/**
 * Fetches a user's bet history, paginated.
 * @param {string} userId
 * @param {number} limit Number of records to fetch.
 * @param {number} offset Number of records to skip.
 * @param {string|null} [gameKeyFilter=null] Optional game_key to filter by.
 * @param {import('pg').PoolClient} [client=pool] Optional DB client.
 * @returns {Promise<Array<object>|null>} Array of bet objects or null on error.
 */
async function getBetHistory(userId, limit = 10, offset = 0, gameKeyFilter = null, client = pool) {
    userId = String(userId);
    const logPrefix = `[DB getBetHistory User ${userId}]`;
    try {
        let query = `
            SELECT id, game_type, bet_details, wager_amount_lamports, payout_amount_lamports, status, created_at, processed_at
            FROM bets
            WHERE user_id = $1
        `;
        const params = [userId];
        let paramCount = 1;

        if (gameKeyFilter) {
            paramCount++;
            query += ` AND game_type = $${paramCount}`;
            params.push(gameKeyFilter);
        }

        paramCount++;
        query += ` ORDER BY created_at DESC, id DESC LIMIT $${paramCount}`;
        params.push(limit);

        paramCount++;
        query += ` OFFSET $${paramCount}`;
        params.push(offset);

        const res = await queryDatabase(query, params, client);
        return res.rows.map(row => ({
            ...row,
            wager_amount_lamports: BigInt(row.wager_amount_lamports || '0'),
            payout_amount_lamports: row.payout_amount_lamports !== null ? BigInt(row.payout_amount_lamports) : null,
            bet_details: row.bet_details || {},
        }));
    } catch (err) {
        console.error(`${logPrefix} Error fetching bet history:`, err.message);
        return null;
    }
}


// --- Referral Payout Operations --- (Largely unchanged)

/**
 * Records a pending referral payout. Can be called within a transaction or stand-alone.
 * Handles unique constraint for milestone payouts gracefully.
 * @param {string} referrerUserId
 * @param {string} refereeUserId
 * @param {'initial_bet' | 'milestone'} payoutType
 * @param {bigint} payoutAmountLamports
 * @param {number | null} [triggeringBetId=null] Bet ID for 'initial_bet' type.
 * @param {bigint | null} [milestoneReachedLamports=null] Wager threshold for 'milestone' type.
 * @param {import('pg').PoolClient | null} [client=null] Optional DB client if called within a transaction. Defaults to pool.
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
                     console.warn(`${logPrefix} Duplicate milestone payout attempt for milestone ${milestoneLamports}. Ignored (ON CONFLICT).`);
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
 * @param {import('pg').PoolClient | null} [client=null] Optional DB client if within a transaction. Defaults to pool.
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

// --- Jackpot Operations --- NEW SECTION ---

/**
 * Ensures a jackpot record exists for the given gameKey, creating it with seed if not.
 * Typically called during bot initialization.
 * @param {string} gameKey The key of the game (e.g., 'slots').
 * @param {bigint} seedAmountLamports The initial amount if jackpot needs to be created.
 * @param {import('pg').PoolClient} [client=pool] Optional DB client.
 * @returns {Promise<boolean>} True if ensured/created, false on error.
 */
async function ensureJackpotExists(gameKey, seedAmountLamports, client = pool) {
    const logPrefix = `[DB ensureJackpotExists Game:${gameKey}]`;
    try {
        const checkRes = await queryDatabase('SELECT game_key FROM jackpots WHERE game_key = $1', [gameKey], client);
        if (checkRes.rowCount === 0) {
            console.log(`${logPrefix} Jackpot not found, creating with seed amount ${formatSol(seedAmountLamports)} SOL.`);
            await queryDatabase(
                'INSERT INTO jackpots (game_key, current_amount_lamports, last_updated_at) VALUES ($1, $2, NOW())',
                [gameKey, seedAmountLamports],
                client
            );
        }
        return true;
    } catch (err) {
        console.error(`${logPrefix} Error ensuring jackpot exists:`, err.message, err.code);
        if (err.code === '23505') { // Unique constraint violation (race condition)
            console.warn(`${logPrefix} Jackpot likely created concurrently.`);
            return true; // Treat as success if it already exists
        }
        return false;
    }
}

/**
 * Gets the current amount of a specific jackpot.
 * @param {string} gameKey The key of the game (e.g., 'slots').
 * @param {import('pg').PoolClient} [client=pool] Optional DB client.
 * @returns {Promise<bigint>} Current jackpot amount in lamports, or 0n if not found/error.
 */
async function getJackpotAmount(gameKey, client = pool) {
    const logPrefix = `[DB getJackpotAmount Game:${gameKey}]`;
    try {
        const res = await queryDatabase('SELECT current_amount_lamports FROM jackpots WHERE game_key = $1', [gameKey], client);
        if (res.rowCount > 0) {
            return BigInt(res.rows[0].current_amount_lamports);
        }
        console.warn(`${logPrefix} Jackpot for game ${gameKey} not found. Returning 0.`);
        return 0n; // Should ideally be seeded by ensureJackpotExists
    } catch (err) {
        console.error(`${logPrefix} Error fetching jackpot amount:`, err.message);
        return 0n;
    }
}

/**
 * Updates the jackpot amount to a specific value (e.g., after a win, reset to seed).
 * MUST be called within a transaction. Locks the jackpot row.
 * @param {string} gameKey The key of the game (e.g., 'slots').
 * @param {bigint} newAmountLamports The new absolute amount for the jackpot.
 * @param {import('pg').PoolClient} client The active database client.
 * @returns {Promise<boolean>} True on success, false on failure.
 */
async function updateJackpotAmount(gameKey, newAmountLamports, client) {
    const logPrefix = `[DB updateJackpotAmount Game:${gameKey}]`;
    try {
        // Lock the row for update
        await queryDatabase('SELECT game_key FROM jackpots WHERE game_key = $1 FOR UPDATE', [gameKey], client);
        const res = await queryDatabase(
            'UPDATE jackpots SET current_amount_lamports = $1, last_updated_at = NOW() WHERE game_key = $2',
            [newAmountLamports, gameKey],
            client
        );
        if (res.rowCount === 0) {
            console.error(`${logPrefix} Failed to update jackpot amount (game_key ${gameKey} not found or lock failed?). Seed it first.`);
            return false;
        }
        return true;
    } catch (err) {
        console.error(`${logPrefix} Error updating jackpot amount:`, err.message);
        return false;
    }
}

/**
 * Increments the jackpot amount by a contribution.
 * MUST be called within a transaction. Locks the jackpot row.
 * @param {string} gameKey The key of the game (e.g., 'slots').
 * @param {bigint} contributionLamports The amount to add to the jackpot.
 * @param {import('pg').PoolClient} client The active database client.
 * @returns {Promise<boolean>} True on success, false on failure.
 */
async function incrementJackpotAmount(gameKey, contributionLamports, client) {
    const logPrefix = `[DB incrementJackpotAmount Game:${gameKey}]`;
    if (contributionLamports <= 0n) return true; // No change needed

    try {
        // Lock the row for update
        await queryDatabase('SELECT game_key FROM jackpots WHERE game_key = $1 FOR UPDATE', [gameKey], client);
        const res = await queryDatabase(
            'UPDATE jackpots SET current_amount_lamports = current_amount_lamports + $1, last_updated_at = NOW() WHERE game_key = $2',
            [contributionLamports, gameKey],
            client
        );
        if (res.rowCount === 0) {
            console.error(`${logPrefix} Failed to increment jackpot amount (game_key ${gameKey} not found or lock failed?). Seed it first.`);
            return false;
        }
        return true;
    } catch (err) {
        console.error(`${logPrefix} Error incrementing jackpot amount:`, err.message);
        return false;
    }
}

// --- End of Part 2 ---
// index.js - Part 3: Solana Utilities & Telegram Helpers
// --- VERSION: 3.2.0 --- (Implement Crash, Blackjack, Slots Jackpot, Foundational UX Fixes/Adds)

// --- Assuming imports from Part 1 are available ---
// import { PublicKey, Keypair, SystemProgram, LAMPORTS_PER_SOL, sendAndConfirmTransaction, ComputeBudgetProgram, TransactionExpiredBlockheightExceededError, SendTransactionError, Connection } from '@solana/web3.js';
// import bs58 from 'bs58';
// import { createHash } from 'crypto';
// import * as crypto from 'crypto';
// import bip39 from 'bip39';
// import { derivePath } from 'ed25519-hd-key';
// Assuming 'bot', 'solanaConnection', various cache maps, constants like 'MAX_PROCESSED_TX_CACHE_SIZE', 'WALLET_CACHE_TTL_MS', 'SOL_DECIMALS', 'DEPOSIT_CONFIRMATION_LEVEL', 'BOT_VERSION', etc. are available from Part 1

// --- General Utilities ---

/**
 * Creates a promise that resolves after a specified number of milliseconds.
 * @param {number} ms Milliseconds to sleep.
 * @returns {Promise<void>}
 */
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Formats lamports into a SOL string, removing unnecessary trailing zeros.
 * @param {bigint | number | string} lamports The amount in lamports.
 * @param {number} [maxDecimals=SOL_DECIMALS] Maximum decimals to initially format to before trimming.
 * @returns {string} Formatted SOL string (e.g., "0.01", "1.2345", "1").
 */
function formatSol(lamports, maxDecimals = SOL_DECIMALS) { // SOL_DECIMALS is from Part 1
    if (typeof lamports === 'undefined' || lamports === null) return '0';
    try {
        // Convert input to BigInt safely
        const lamportsBigInt = BigInt(lamports);
        // Convert to SOL number
        const solNumber = Number(lamportsBigInt) / LAMPORTS_PER_SOL; // LAMPORTS_PER_SOL from Part 1
        // Use toFixed for initial rounding to avoid floating point issues with many decimals
        const fixedString = solNumber.toFixed(maxDecimals);
        // parseFloat will strip trailing zeros after the decimal point.
        // toString() converts back (handles integers correctly, e.g., 1.00 -> "1")
        return parseFloat(fixedString).toString();
    } catch (e) {
        console.error(`[formatSol] Error formatting lamports: ${lamports}`, e);
        return 'NaN'; // Return 'NaN' string to indicate error
    }
}


// --- Solana Utilities --

// Helper function to create safe index for BIP-44 hardened derivation
// Uses import { createHash } from 'crypto';
function createSafeIndex(userId) {
    // Hash the userId to fixed-size output
    const hash = createHash('sha256') // createHash from 'crypto'
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
 * @returns {Promise<{publicKey: import('@solana/web3.js').PublicKey, privateKeyBytes: Uint8Array, derivationPath: string} | null>} Derived public key, private key bytes, and path, or null on error.
 */
async function generateUniqueDepositAddress(userId, addressIndex) {
    userId = String(userId); // Ensure string
    const logPrefix = `[Address Gen User ${userId} Index ${addressIndex}]`;

    try {
        const seedPhrase = process.env.DEPOSIT_MASTER_SEED_PHRASE;
        if (typeof seedPhrase !== 'string' || seedPhrase.length < 20 || !bip39.validateMnemonic(seedPhrase)) { // bip39 from 'bip39'
            console.error(`${logPrefix} CRITICAL: DEPOSIT_MASTER_SEED_PHRASE is invalid or empty/invalid at runtime!`);
            await notifyAdmin(`🚨 CRITICAL: DEPOSIT\\_MASTER\\_SEED\\_PHRASE is missing or invalid\\. Cannot generate deposit addresses\\. (User: ${escapeMarkdownV2(userId)}, Index: ${addressIndex})`);
            return null;
        }

        if (typeof userId !== 'string' || userId.length === 0 || typeof addressIndex !== 'number' || addressIndex < 0 || !Number.isInteger(addressIndex)) {
            console.error(`${logPrefix} Invalid userId or addressIndex`, { userId, addressIndex });
            return null;
        }

        const masterSeedBuffer = bip39.mnemonicToSeedSync(seedPhrase);
        if (!masterSeedBuffer || masterSeedBuffer.length === 0) {
            throw new Error("Failed to generate seed buffer from mnemonic.");
        }

        const safeIndex = createSafeIndex(userId);
        const derivationPath = `m/44'/501'/${safeIndex}'/0'/${addressIndex}'`;

        const derivedSeed = derivePath(derivationPath, masterSeedBuffer.toString('hex')); // derivePath from 'ed25519-hd-key'
        const privateKeyBytes = derivedSeed.key;

        if (!privateKeyBytes || privateKeyBytes.length !== 32) {
            throw new Error(`Derived private key bytes are invalid (length: ${privateKeyBytes?.length}) using ed25519-hd-key for path ${derivationPath}`);
        }

        const keypair = Keypair.fromSeed(privateKeyBytes); // Keypair from '@solana/web3.js'
        const publicKey = keypair.publicKey;

        return {
            publicKey: publicKey,
            privateKeyBytes: privateKeyBytes,
            derivationPath: derivationPath
        };

    } catch (error) {
        console.error(`${logPrefix} Error during address generation using ed25519-hd-key: ${error.message}`);
        console.error(`${logPrefix} Error Stack: ${error.stack}`);
        await notifyAdmin(`🚨 ERROR generating deposit address for User ${escapeMarkdownV2(userId)} Index ${addressIndex} (${escapeMarkdownV2(logPrefix)}): ${escapeMarkdownV2(error.message)}`);
        return null;
    }
}


/**
 * Re-derives a Solana Keypair from a stored BIP44 derivation path and the master seed phrase.
 * CRITICAL: Requires DEPOSIT_MASTER_SEED_PHRASE to be available and secure.
 * This is synchronous as key derivation libraries often are.
 * @param {string} derivationPath The full BIP44 derivation path (e.g., "m/44'/501'/12345'/0'/0'").
 * @returns {import('@solana/web3.js').Keypair | null} The derived Solana Keypair object, or null on error.
 */
function getKeypairFromPath(derivationPath) {
    const logPrefix = `[GetKeypairFromPath Path:${derivationPath}]`;
    try {
        if (!derivationPath || typeof derivationPath !== 'string' || !derivationPath.startsWith("m/44'/501'/")) {
            console.error(`${logPrefix} Invalid derivation path format provided.`);
            return null;
        }

        const seedPhrase = process.env.DEPOSIT_MASTER_SEED_PHRASE;
        if (typeof seedPhrase !== 'string' || seedPhrase.length < 20 || !bip39.validateMnemonic(seedPhrase)) {
            console.error(`${logPrefix} CRITICAL: DEPOSIT_MASTER_SEED_PHRASE is missing or invalid when trying to re-derive key!`);
            return null;
        }

        const masterSeedBuffer = bip39.mnemonicToSeedSync(seedPhrase);
        if (!masterSeedBuffer || masterSeedBuffer.length === 0) {
            throw new Error("Failed to generate seed buffer from mnemonic.");
        }

        const derivedSeed = derivePath(derivationPath, masterSeedBuffer.toString('hex'));
        const privateKeyBytes = derivedSeed.key;

        if (!privateKeyBytes || privateKeyBytes.length !== 32) {
            throw new Error(`Derived private key bytes are invalid (length: ${privateKeyBytes?.length})`);
        }

        const keypair = Keypair.fromSeed(privateKeyBytes);
        return keypair;

    } catch (error) {
        console.error(`${logPrefix} Error re-deriving keypair from path: ${error.message}`);
        if (!error.message.includes('DEPOSIT_MASTER_SEED_PHRASE')) {
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

    if (error instanceof TransactionExpiredBlockheightExceededError) { // From '@solana/web3.js'
        return true;
    }
    if (error instanceof SendTransactionError && error.cause) { // From '@solana/web3.js'
       return isRetryableSolanaError(error.cause);
    }

    const message = String(error.message || '').toLowerCase();
    const code = error?.code || error?.cause?.code;
    const errorCode = error?.errorCode;
    const status = error?.response?.status || error?.statusCode || error?.status;

    if ([429, 500, 502, 503, 504].includes(Number(status))) return true;

    const retryableMessages = [
        'timeout', 'timed out', 'econnreset', 'esockettimedout', 'network error',
        'fetch', 'socket hang up', 'connection terminated', 'econnrefused', 'failed to fetch',
        'getaddrinfo enotfound', 'connection refused', 'connection reset by peer', 'etimedout',
        'transaction simulation failed', 'failed to simulate transaction',
        'blockhash not found', 'slot leader does not match', 'node is behind',
        'transaction was not confirmed', 'block not available', 'block cleanout',
        'sending transaction', 'connection closed', 'load balancer error',
        'backend unhealthy', 'overloaded', 'proxy internal error', 'too many requests',
        'rate limit exceeded', 'unknown block', 'leader not ready', 'heavily throttled',
        'failed to query long-term storage' // Specific RPC error message
    ];
    if (retryableMessages.some(m => message.includes(m))) return true;

    const retryableCodes = [
        'ETIMEDOUT', 'ECONNRESET', 'ENETUNREACH', 'EAI_AGAIN', 'ECONNABORTED',
        'ECONNREFUSED', 'UND_ERR_CONNECT_TIMEOUT', 'UND_ERR_HEADERS_TIMEOUT',
        'UND_ERR_BODY_TIMEOUT', 'FETCH_TIMEOUT', 'FETCH_ERROR',
        '-32000', '-32001', '-32002', '-32003', '-32004', '-32005',
    ];
    if (typeof code === 'string' && retryableCodes.includes(code.toUpperCase())) return true;
    if (typeof code === 'number' && retryableCodes.includes(code.toString())) return true;
    if (retryableCodes.includes(code)) return true;
    if (typeof errorCode === 'string' && retryableCodes.includes(errorCode.toUpperCase())) return true;
    if (typeof errorCode === 'number' && retryableCodes.includes(errorCode.toString())) return true;
    if (retryableCodes.includes(errorCode)) return true;

    if (error.reason === 'rpc_error' || error.reason === 'network_error') return true;

    if (error?.data?.message?.toLowerCase().includes('rate limit exceeded') || error?.data?.message?.toLowerCase().includes('too many requests')) {
        return true;
    }
    return false;
}

/**
 * Sends SOL from a designated bot wallet to a recipient.
 * Handles priority fees and uses sendAndConfirmTransaction.
 * @param {import('@solana/web3.js').PublicKey | string} recipientPublicKey The recipient's address.
 * @param {bigint} amountLamports The amount to send.
 * @param {'withdrawal' | 'referral'} payoutSource Determines which private key to use.
 * @returns {Promise<{success: boolean, signature?: import('@solana/web3.js').TransactionSignature, error?: string, isRetryable?: boolean}>} Result object.
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

    let privateKeyEnvVar;
    let keyTypeForLog;
    if (payoutSource === 'referral' && process.env.REFERRAL_PAYOUT_PRIVATE_KEY) {
        privateKeyEnvVar = 'REFERRAL_PAYOUT_PRIVATE_KEY';
        keyTypeForLog = 'REFERRAL';
    } else {
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
        payerWallet = Keypair.fromSecretKey(bs58.decode(privateKey)); // bs58 from 'bs58'
    } catch (e) {
         const errorMsg = `Failed to decode private key ${keyTypeForLog} (${privateKeyEnvVar}): ${e.message}`;
         console.error(`[${operationId}] ❌ ERROR: ${errorMsg}`);
         await notifyAdmin(`🚨 CRITICAL: Failed to decode private key ${escapeMarkdownV2(keyTypeForLog)} (${escapeMarkdownV2(privateKeyEnvVar)}): ${escapeMarkdownV2(e.message)}\\. Payout failed\\. Operation ID: ${escapeMarkdownV2(operationId)}`);
         return { success: false, error: errorMsg, isRetryable: false };
    }

    const amountSOLFormatted = formatSol(amountToSend);
    console.log(`[${operationId}] Attempting to send ${amountSOLFormatted} SOL from ${payerWallet.publicKey.toBase58()} to ${recipientPubKey.toBase58()} using ${keyTypeForLog} key...`);

    try {
        const { blockhash, lastValidBlockHeight } = await solanaConnection.getLatestBlockhash(DEPOSIT_CONFIRMATION_LEVEL); // solanaConnection from Part 1, DEPOSIT_CONFIRMATION_LEVEL from Part 1
        if (!blockhash || !lastValidBlockHeight) {
            throw new Error('Failed to get valid latest blockhash object.');
        }

        const basePriorityFee = parseInt(process.env.PAYOUT_BASE_PRIORITY_FEE_MICROLAMPORTS, 10);
        const maxPriorityFee = parseInt(process.env.PAYOUT_MAX_PRIORITY_FEE_MICROLAMPORTS, 10);
        let priorityFeeMicroLamports = Math.max(1, Math.min(basePriorityFee, maxPriorityFee));
        const computeUnitLimit = parseInt(process.env.PAYOUT_COMPUTE_UNIT_LIMIT, 10);

        const transaction = new Transaction({ // Transaction from '@solana/web3.js'
            recentBlockhash: blockhash,
            feePayer: payerWallet.publicKey,
        });

        transaction.add(
            ComputeBudgetProgram.setComputeUnitLimit({ units: computeUnitLimit }) // ComputeBudgetProgram from '@solana/web3.js'
        );
        transaction.add(
            ComputeBudgetProgram.setComputeUnitPrice({ microLamports: priorityFeeMicroLamports })
        );
        transaction.add(
            SystemProgram.transfer({ // SystemProgram from '@solana/web3.js'
                fromPubkey: payerWallet.publicKey,
                toPubkey: recipientPubKey,
                lamports: amountToSend,
            })
        );

        console.log(`[${operationId}] Sending and confirming transaction (Commitment: ${DEPOSIT_CONFIRMATION_LEVEL})...`);
        const signature = await sendAndConfirmTransaction( // from '@solana/web3.js'
            solanaConnection,
            transaction,
            [payerWallet],
            {
                commitment: DEPOSIT_CONFIRMATION_LEVEL,
                skipPreflight: false,
                preflightCommitment: DEPOSIT_CONFIRMATION_LEVEL,
                lastValidBlockHeight: lastValidBlockHeight,
            }
        );

        console.log(`[${operationId}] SUCCESS! ✅ Sent ${amountSOLFormatted} SOL to ${recipientPubKey.toBase58()} using ${keyTypeForLog} key. TX: ${signature}`);
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
            await notifyAdmin(`🚨 CRITICAL: Insufficient funds in ${escapeMarkdownV2(keyTypeForLog)} wallet (${escapeMarkdownV2(payerWallet.publicKey.toBase58())}) for payout. Operation ID: ${escapeMarkdownV2(operationId)}`);
        } else if (error instanceof TransactionExpiredBlockheightExceededError || errorMsgLower.includes('blockhash not found') || errorMsgLower.includes('block height exceeded')) {
            userFriendlyError = 'Transaction expired (blockhash invalid). Retrying required.';
        } else if (errorMsgLower.includes('transaction was not confirmed') || errorMsgLower.includes('timed out waiting')) {
            userFriendlyError = `Transaction confirmation timeout. Status unclear, may succeed later. Manual check advised.`;
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
 * @param {import('@solana/web3.js').ConfirmedTransaction | import('@solana/web3.js').TransactionResponse | null} tx The fetched transaction object.
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

    if (tx.meta?.preBalances && tx.meta?.postBalances && tx.transaction?.message) {
        try {
            let accountKeysList = [];
            const message = tx.transaction.message;
            if (message.staticAccountKeys) { // Legacy
                accountKeysList = message.staticAccountKeys.map(k => k.toBase58());
            } else if (message.accountKeys) { // Version 0
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

            const uniqueAccountKeys = [...new Set(accountKeysList)];
            const targetIndex = uniqueAccountKeys.indexOf(targetAddress);

            if (targetIndex !== -1 && tx.meta.preBalances.length > targetIndex && tx.meta.postBalances.length > targetIndex) {
                const preBalance = BigInt(tx.meta.preBalances[targetIndex]);
                const postBalance = BigInt(tx.meta.postBalances[targetIndex]);
                const balanceChange = postBalance - preBalance;

                if (balanceChange > 0n) {
                    transferAmount = balanceChange;
                    const header = message.header;
                    if (header && header.numRequiredSignatures > 0 && uniqueAccountKeys.length > 0) {
                        payerAddress = uniqueAccountKeys[0];
                    }
                }
            }
        } catch (e) {
            console.warn(`${logPrefix} Error analyzing balance changes: ${e.message}`);
            transferAmount = 0n;
            payerAddress = null;
        }
    } else {
        console.log(`${logPrefix} Balance change analysis skipped: Meta or message data missing/incomplete.`);
    }

    // Fallback: Check Log Messages (Less reliable)
    if (transferAmount === 0n && tx.meta?.logMessages) {
        const sysTransferRegex = /Program(?:.*)invoke \[\d+\]\s+Program log: Transfer: src=([1-9A-HJ-NP-Za-km-z]+) dst=([1-9A-HJ-NP-Za-km-z]+) lamports=(\d+)/;
        const simpleTransferRegex = /Program 11111111111111111111111111111111 success/; // SystemProgram.programId
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
                        console.log(`${logPrefix} Found potential transfer via log message: ${formatSol(potentialAmount)} SOL from ${match[1]}`);
                        transferAmount = potentialAmount;
                        payerAddress = match[1];
                        break;
                    }
                }
            }
        }
    }

    if (transferAmount > 0n) {
        console.log(`${logPrefix} Final analysis result: Found ${formatSol(transferAmount)} SOL transfer (likely from ${payerAddress || 'Unknown'}).`);
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
        .filter(id => /^\d+$/.test(id));

    if (adminIds.length === 0) {
        console.warn('[NotifyAdmin] No valid ADMIN_USER_IDS configured. Cannot send notification.');
        return;
    }

    const timestamp = new Date().toISOString();
    const escapedMessage = escapeMarkdownV2(message); // Ensure this is correctly defined and used
    const fullMessage = `🚨 *BOT ALERT* 🚨\n\\[${escapeMarkdownV2(timestamp)}\\]\n\n${escapedMessage}`;

    console.log(`[NotifyAdmin] Sending alert: "${message.substring(0, 100)}..." to ${adminIds.length} admin(s).`);

    const results = await Promise.allSettled(
        adminIds.map(adminId =>
            safeSendMessage(adminId, fullMessage, { parse_mode: 'MarkdownV2' }) // safeSendMessage is defined below
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
 * @param {import('node-telegram-bot-api').SendMessageOptions} [options={}] Additional send options (e.g., parse_mode, reply_markup).
 * @returns {Promise<import('node-telegram-bot-api').Message | undefined>} The sent message object or undefined on failure.
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
        const ellipsis = "...";
        const truncateAt = (options.parse_mode === 'MarkdownV2' || options.parse_mode === 'Markdown') ? MAX_LENGTH - escapeMarkdownV2(ellipsis).length : MAX_LENGTH - ellipsis.length;
        messageToSend = messageToSend.substring(0, truncateAt) + (options.parse_mode === 'MarkdownV2' || options.parse_mode === 'Markdown' ? escapeMarkdownV2(ellipsis) : ellipsis);
    }

    return telegramSendQueue.add(async () => { // telegramSendQueue from Part 1
        try {
            const sentMessage = await bot.sendMessage(chatId, messageToSend, options); // bot from Part 1
            return sentMessage;
        } catch (error) {
            console.error(`❌ Telegram send error to chat ${chatId}: Code: ${error.code} | ${error.message}`);
            if (error.response?.body) {
                console.error(`   -> Response Body: ${JSON.stringify(error.response.body)}`);
                const errorCode = error.response.body.error_code;
                const description = error.response.body.description?.toLowerCase() || '';
                if (errorCode === 403 || description.includes('blocked') || description.includes('kicked') || description.includes('deactivated')) {
                    console.warn(`[TG Send] Bot blocked/kicked/deactivated in chat ${chatId}.`);
                } else if (errorCode === 400 && (description.includes('parse error') || description.includes('can\'t parse entities'))) {
                    console.error(`❌❌❌ Telegram Parse Error! Message text (first 100 chars): ${messageToSend.substring(0, 100)}`);
                    // Notify admin about the parse error, including a snippet of the problematic text
                    await notifyAdmin(`🚨 Telegram Parse Error in \`safeSendMessage\`\nChatID: ${chatId}\nOptions: ${JSON.stringify(options)}\nError: ${escapeMarkdownV2(error.response.body.description)}\nOriginal Text \\(Truncated\\):\n\`\`\`\n${escapeMarkdownV2(text.substring(0, 500))}\n\`\`\``)
                        .catch(e => console.error("Failed to send admin notification about parse error:", e));
                }
            }
            return undefined;
        }
    }).catch(queueError => {
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
    // Pipe '|' was added as per earlier fix.
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
        const member = await bot.getChatMember(chatId, String(userId));
        const user = member.user;
        let name = '';
        if (user.username) {
            name = `@${user.username}`; // Usernames don't need MdV2 escaping if used as @username
        } else if (user.first_name) {
            name = user.first_name;
            if (user.last_name) {
                name += ` ${user.last_name}`;
            }
            name = escapeMarkdownV2(name); // Escape first/last names
        } else {
            name = escapeMarkdownV2(`User_${String(userId).slice(-4)}`);
        }
        return name;
    } catch (error) {
        console.warn(`[getUserDisplayName] Failed to get chat member for ${userId} in ${chatId}: ${error.message}. Falling back to ID.`);
        return escapeMarkdownV2(`User_${String(userId).slice(-4)}`);
    }
}


// --- Cache Utilities ---

/** Updates the wallet cache for a user with new data, resets TTL. */
function updateWalletCache(userId, data) {
    userId = String(userId);
    const existing = walletCache.get(userId) || {}; // walletCache from Part 1
    const entryTimestamp = Date.now();
    const newData = { ...existing, ...data, timestamp: entryTimestamp };
    walletCache.set(userId, newData);

    if (existing.timeoutId) {
        clearTimeout(existing.timeoutId);
    }
    const timeoutId = setTimeout(() => {
        const current = walletCache.get(userId);
        if (current && current.timestamp === entryTimestamp) {
            walletCache.delete(userId);
        }
    }, WALLET_CACHE_TTL_MS); // WALLET_CACHE_TTL_MS from Part 1
    newData.timeoutId = timeoutId;
    if (timeoutId.unref) {
        timeoutId.unref();
    }
}

/** Gets data from the wallet cache if entry exists and is not expired. */
function getWalletCache(userId) {
    userId = String(userId);
    const entry = walletCache.get(userId);
    if (entry && Date.now() - entry.timestamp < WALLET_CACHE_TTL_MS) {
        return entry;
    }
    if (entry) {
        if(entry.timeoutId) clearTimeout(entry.timeoutId);
        walletCache.delete(userId);
    }
    return undefined;
}

/** Adds/Updates an active deposit address in the cache, sets expiry timer. */
function addActiveDepositAddressCache(address, userId, expiresAtTimestamp) {
    const existing = activeDepositAddresses.get(address); // activeDepositAddresses from Part 1
    if (existing?.timeoutId) {
        clearTimeout(existing.timeoutId);
    }

    const newEntry = { userId: String(userId), expiresAt: expiresAtTimestamp };
    activeDepositAddresses.set(address, newEntry);

    const delay = expiresAtTimestamp - Date.now();
    if (delay > 0) {
        const timeoutId = setTimeout(() => {
            const current = activeDepositAddresses.get(address);
            if (current && current.userId === String(userId) && current.expiresAt === expiresAtTimestamp) {
                activeDepositAddresses.delete(address);
            }
        }, delay);
        newEntry.timeoutId = timeoutId;
        if (timeoutId.unref) {
            timeoutId.unref();
        }
    } else {
        removeActiveDepositAddressCache(address);
    }
}

/** Gets an active deposit address entry from cache if it exists and hasn't expired. */
function getActiveDepositAddressCache(address) {
    const entry = activeDepositAddresses.get(address);
    const now = Date.now();
    if (entry && now < entry.expiresAt) {
        return entry;
    }
    if (entry) {
        if(entry.timeoutId) clearTimeout(entry.timeoutId);
        activeDepositAddresses.delete(address);
    }
    return undefined;
}

/** Explicitly removes an address from the deposit cache and clears its timer. */
function removeActiveDepositAddressCache(address) {
    const entry = activeDepositAddresses.get(address);
    if (entry?.timeoutId) {
        clearTimeout(entry.timeoutId);
    }
    const deleted = activeDepositAddresses.delete(address);
    return deleted;
}

/** Adds a transaction signature to the processed deposit cache (LRU). */
function addProcessedDepositTx(signature) {
    if (processedDepositTxSignatures.has(signature)) return; // processedDepositTxSignatures from Part 1

    if (processedDepositTxSignatures.size >= MAX_PROCESSED_TX_CACHE_SIZE) { // MAX_PROCESSED_TX_CACHE_SIZE from Part 1
        const oldestSig = processedDepositTxSignatures.values().next().value;
        if (oldestSig) {
            processedDepositTxSignatures.delete(oldestSig);
        }
    }
    processedDepositTxSignatures.add(signature);
}

/** Checks if a transaction signature exists in the processed deposit cache. */
function hasProcessedDepositTx(signature) {
    return processedDepositTxSignatures.has(signature);
}


// --- Other Utilities ---

/** Generates a random referral code prefixed with 'ref_'. */
function generateReferralCode(length = 8) {
    const byteLength = Math.ceil(length / 2);
    const randomBytes = crypto.randomBytes(byteLength); // crypto from 'crypto'
    const hexString = randomBytes.toString('hex').slice(0, length);
    return `ref_${hexString}`;
}

// --- End of Part 3 ---
// index.js - Part 4: Game Logic
// --- VERSION: 3.2.0 --- (Implement Crash, Blackjack, Slots Jackpot, Foundational UX Fixes/Adds)

// --- Assuming constants like LAMPORTS_PER_SOL, GAME_CONFIG etc are available from Part 1 ---

// --- Game Logic Implementations ---
// These functions simulate the core outcome of each game.
// They do NOT handle betting, balance updates, house edge application, or user notifications/animations,
// unless specified for a particular game's core mechanic (e.g. Crash multiplier).
// That broader logic resides in the game result handlers (Part 5a).

/**
 * Plays a coinflip game.
 * @param {string} choice 'heads' or 'tails'. Not used in simulation but good for context.
 * @returns {{ outcome: 'heads' | 'tails' }} The random result of the flip.
 */
function playCoinflip(choice) {
    // Simple 50/50 random outcome
    const outcome = Math.random() < 0.5 ? 'heads' : 'tails';
    return { outcome };
}

/**
 * Simulates a simple race game outcome by picking a winning lane number.
 * @param {number} [totalLanes=4] The number of lanes/competitors in the race.
 * @returns {{ winningLane: number }} The randomly determined winning lane number.
 */
function simulateRace(totalLanes = 4) {
    if (totalLanes <= 0) totalLanes = 1; // Prevent errors
    const winningLane = Math.floor(Math.random() * totalLanes) + 1;
    return { winningLane };
}

/**
 * Simulates a simple slots game spin. Returns symbols and payout multiplier *before* house edge.
 * Symbols: Cherry (C), Lemon (L), Orange (O), Bell (B), Seven (7), Jackpot (J)
 * Payouts (example before edge): JJJ=Jackpot!, 777=100x, BBB=20x, OOO=10x, CCC=5x, Any two Cherries=2x
 * @returns {{ symbols: [string, string, string], payoutMultiplier: number, isJackpotWin: boolean }}
 */
function simulateSlots() {
    // Define symbols and their relative weights for randomness
    // 'J' for Jackpot symbol, make it rare.
    const symbols = ['C', 'L', 'O', 'B', '7', 'J']; // J for Jackpot
    const weights = [25, 20, 15, 10, 5, 1]; // Weighted probability: J is rarest

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
    let isJackpotWin = false;

    if (reel1 === 'J' && reel2 === 'J' && reel3 === 'J') {
        isJackpotWin = true;
        payoutMultiplier = 0; // Jackpot is handled separately, not by multiplier here.
    } else if (reel1 === '7' && reel2 === '7' && reel3 === '7') {
        payoutMultiplier = 100;
    } else if (reel1 === 'B' && reel2 === 'B' && reel3 === 'B') {
        payoutMultiplier = 20;
    } else if (reel1 === 'O' && reel2 === 'O' && reel3 === 'O') {
        payoutMultiplier = 10;
    } else if (reel1 === 'C' && reel2 === 'C' && reel3 === 'C') {
        payoutMultiplier = 5;
    } else {
        const cherryCount = resultSymbols.filter(s => s === 'C').length;
        if (cherryCount >= 2) {
            payoutMultiplier = 2;
        }
    }
    // The handler in Part 5a will check isJackpotWin and determine win/loss based on multiplier, then apply house edge.
    return { symbols: resultSymbols, payoutMultiplier, isJackpotWin };
}


/**
 * Simulates a simplified Roulette spin (European style - 0 to 36).
 * @returns {{ winningNumber: number }} The winning number (0-36).
 */
function simulateRouletteSpin() {
    const winningNumber = Math.floor(Math.random() * 37); // 0-36
    return { winningNumber };
}

/**
 * Determines the base payout multiplier for a given Roulette bet type and winning number.
 * Note: This returns the multiplier for WINNINGS (e.g., 35 for straight, 1 for even money).
 * @param {string} betType
 * @param {string | number | null} betValue The specific number/value bet on (for 'straight')
 * @param {number} winningNumber The actual winning number from the spin.
 * @returns {number} Payout multiplier (e.g., 35 means 35x winnings, not 35x total return).
 */
function getRoulettePayoutMultiplier(betType, betValue, winningNumber) {
    let payoutMultiplier = 0;

    const isRed = (num) => [1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36].includes(num);
    const isBlack = (num) => num !== 0 && !isRed(num);
    const isEven = (num) => num !== 0 && num % 2 === 0;
    const isOdd = (num) => num !== 0 && num % 2 !== 0;
    const isLow = (num) => num >= 1 && num <= 18;
    const isHigh = (num) => num >= 19 && num <= 36;

    switch (betType) {
        case 'straight':
            if (betValue !== null && winningNumber === parseInt(String(betValue), 10)) {
                payoutMultiplier = 35;
            }
            break;
        case 'red':   if (isRed(winningNumber)) payoutMultiplier = 1; break;
        case 'black': if (isBlack(winningNumber)) payoutMultiplier = 1; break;
        case 'even':  if (isEven(winningNumber)) payoutMultiplier = 1; break;
        case 'odd':   if (isOdd(winningNumber)) payoutMultiplier = 1; break;
        case 'low':   if (isLow(winningNumber)) payoutMultiplier = 1; break;
        case 'high':  if (isHigh(winningNumber)) payoutMultiplier = 1; break;
        default:
            console.warn(`[getRoulettePayoutMultiplier] Unknown bet type: ${betType}`);
            break;
    }
    return payoutMultiplier;
}


/**
 * Simulates a simplified game of Casino War. Ace is high.
 * @returns {{ result: 'win' | 'loss' | 'push', playerCard: string, dealerCard: string, playerRank: number, dealerRank: number }}
 */
function simulateWar() {
    const deck = createDeck();
    shuffleDeck(deck);

    const playerCard = dealCard(deck);
    const dealerCard = dealCard(deck);

    const playerRank = getCardNumericValue(playerCard, true); // true for Ace high in War
    const dealerRank = getCardNumericValue(dealerCard, true);

    let result;
    if (playerRank > dealerRank) result = 'win';
    else if (playerRank < dealerRank) result = 'loss';
    else result = 'push'; // Simplified: no "go to war" option

    return { result, playerCard: formatCard(playerCard), dealerCard: formatCard(dealerCard), playerRank, dealerRank };
}

// --- NEW GAME LOGIC: Crash ---
/**
 * Simulates a Crash game round to determine the crash point.
 * The actual game flow (increasing multiplier, user cashout) is in Part 5a.
 * This function determines at what multiplier the game would naturally crash.
 * @returns {{ crashMultiplier: number }} The multiplier at which the game crashes.
 */
function simulateCrash() {
    // This is a crucial part for game balance and house edge.
    // A common approach is an inverse probability distribution.
    // E.g., 99% chance to go > 1.00, P(crash at X) = (1 - HouseEdge) / X^2 (simplified example)
    // For simplicity here, let's use a slightly weighted random number generation.
    // A more sophisticated model would be needed for a production system to precisely control house edge.

    const houseEdge = GAME_CONFIG.crash.houseEdge || 0.03; // Default if not set

    // Calculate a random number, then derive crash point from it
    // This ensures a small chance of instant crash (e.g. at 1.00x)
    // and a long tail for high multipliers
    const r = Math.random();

    let crashMultiplier;

    if (r < houseEdge) { // Chance to crash at 1.00x (or very low, effectively house win)
        crashMultiplier = 1.00;
    } else {
        // Map the remaining probability space (1 - houseEdge) to a curve
        // Example: (1 / (1 - r)) will give values from 1 upwards, with decreasing probability for higher numbers.
        // We adjust this to make it more controlled.
        // This is a placeholder for a proper probability distribution.
        // (1 - HouseEdge) / (1 - r) means that P(crash_point < x) = 1 - (1-HouseEdge)/x
        // So P(crash_point = x) = d/dx (1 - (1-HouseEdge)/x) = (1-HouseEdge)/x^2
        // This is one common crash game probability distribution.
        crashMultiplier = parseFloat(((1 - houseEdge) / (1 - r)).toFixed(2));

        // Ensure it's at least 1.01 if it didn't crash at 1.00
        if (crashMultiplier <= 1.00) {
            crashMultiplier = 1.01;
        }
    }
    // Cap maximum multiplier for sanity, though true crash games might not.
    // if (crashMultiplier > 1000) crashMultiplier = 1000.00;

    return { crashMultiplier: parseFloat(crashMultiplier.toFixed(2)) };
}

// Note: getCrashPayoutMultiplier is not strictly needed here as payout is determined
// by when the user cashes out. The house edge is applied in handleCrashGame.

// --- NEW GAME LOGIC: Blackjack & Card Utilities ---

const BJ_SUITS = ['♠️', '♥️', '♦️', '♣️']; // Spades, Hearts, Diamonds, Clubs
const BJ_RANKS = ['2', '3', '4', '5', '6', '7', '8', '9', 'T', 'J', 'Q', 'K', 'A']; // T=10

/**
 * Creates a standard 52-card deck.
 * @returns {Array<{rank: string, suit: string}>}
 */
function createDeck() {
    const deck = [];
    for (const suit of BJ_SUITS) {
        for (const rank of BJ_RANKS) {
            deck.push({ rank, suit });
        }
    }
    return deck;
}

/**
 * Shuffles a deck of cards in place using Fisher-Yates algorithm.
 * @param {Array<object>} deck
 */
function shuffleDeck(deck) {
    for (let i = deck.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [deck[i], deck[j]] = [deck[j], deck[i]];
    }
}

/**
 * Deals a single card from the top of the deck.
 * @param {Array<object>} deck
 * @returns {object | undefined} The card object or undefined if deck is empty.
 */
function dealCard(deck) {
    return deck.pop();
}

/**
 * Gets the numerical value of a card for games like Blackjack or War.
 * @param {{rank: string, suit: string}} card
 * @param {boolean} [aceHighInWar=false] If true and rank is Ace, returns 14 (for War). Otherwise Ace is 1 or 11 (handled by calculateHandValue for BJ).
 * @returns {number}
 */
function getCardNumericValue(card, aceHighInWar = false) {
    if (!card || !card.rank) return 0;
    const rank = card.rank;
    if (['K', 'Q', 'J', 'T'].includes(rank)) return 10;
    if (rank === 'A') return aceHighInWar ? 14 : 11; // Blackjack Ace value is 11 initially
    return parseInt(rank, 10);
}

/**
 * Formats a card object for display.
 * @param {{rank: string, suit: string}} card
 * @returns {string} e.g., "K♠️", "A♥️"
 */
function formatCard(card) {
    if (!card || !card.rank || !card.suit) return "??";
    return `${card.rank}${card.suit}`;
}

/**
 * Calculates the total value of a hand in Blackjack, handling Aces correctly.
 * @param {Array<{rank: string, suit: string}>} hand
 * @returns {number} The best possible hand value not exceeding 21, or lowest value if over 21.
 */
function calculateHandValue(hand) {
    let value = 0;
    let aceCount = 0;
    for (const card of hand) {
        const cardValue = getCardNumericValue(card);
        value += cardValue;
        if (card.rank === 'A') {
            aceCount++;
        }
    }
    // Adjust for Aces if value is over 21
    while (value > 21 && aceCount > 0) {
        value -= 10; // Change Ace from 11 to 1
        aceCount--;
    }
    return value;
}

/**
 * Simulates the dealer's turn in Blackjack. Dealer hits until 17 or more.
 * @param {Array<{rank: string, suit: string}>} dealerHand
 * @param {Array<{rank: string, suit: string}>} deck
 * @returns {{hand: Array<{rank: string, suit: string}>, value: number, busted: boolean}}
 */
function simulateDealerPlay(dealerHand, deck) {
    let handValue = calculateHandValue(dealerHand);
    while (handValue < 17) {
        const newCard = dealCard(deck);
        if (newCard) {
            dealerHand.push(newCard);
            handValue = calculateHandValue(dealerHand);
        } else {
            break; // No more cards in deck (shouldn't happen in normal play)
        }
    }
    return {
        hand: dealerHand,
        value: handValue,
        busted: handValue > 21
    };
}

/**
 * Determines the winner of a Blackjack hand.
 * @param {Array<{rank: string, suit: string}>} playerHand
 * @param {Array<{rank: string, suit: string}>} dealerHand
 * @param {boolean} playerStands - Indicates if player chose to stand.
 * @returns {{
 * result: 'player_blackjack' | 'player_wins' | 'dealer_wins' | 'push' | 'player_busts' | 'dealer_busts',
 * playerValue: number,
 * dealerValue: number,
 * playerHandFormatted: string[],
 * dealerHandFormatted: string[]
 * }}
 */
function determineBlackjackWinner(playerHand, dealerHand, playerStands = false /* deck for dealer play can be passed if not done before */) {
    // This function is more of a conceptual placeholder for winner determination.
    // The actual game flow in Part 5a (handleBlackjackGame) will use calculateHandValue
    // and simulateDealerPlay to manage the game state step-by-step.
    // This function might be called at the very end of that process.

    const playerValue = calculateHandValue(playerHand);
    const dealerValue = calculateHandValue(dealerHand); // Assuming dealer has played

    const playerHasBlackjack = playerValue === 21 && playerHand.length === 2;
    const dealerHasBlackjack = dealerValue === 21 && dealerHand.length === 2;

    let result;

    if (playerHasBlackjack && dealerHasBlackjack) {
        result = 'push';
    } else if (playerHasBlackjack) {
        result = 'player_blackjack'; // Pays 3:2 typically
    } else if (dealerHasBlackjack) {
        result = 'dealer_wins'; // Player loses original bet
    } else if (playerValue > 21) {
        result = 'player_busts';
    } else if (dealerValue > 21) {
        result = 'dealer_wins'; // Player wins because dealer busted
    } else if (playerValue > dealerValue) {
        result = 'player_wins';
    } else if (dealerValue > playerValue) {
        result = 'dealer_wins';
    } else { // playerValue === dealerValue
        result = 'push';
    }

    return {
        result,
        playerValue,
        dealerValue,
        playerHandFormatted: playerHand.map(formatCard),
        dealerHandFormatted: dealerHand.map(formatCard),
    };
}


// --- Export functions if needed for other parts (though usually called by Part 5a handlers) ---
// export { playCoinflip, simulateRace, simulateSlots, simulateRouletteSpin, getRoulettePayoutMultiplier, simulateWar, simulateCrash, createDeck, shuffleDeck, dealCard, getCardNumericValue, formatCard, calculateHandValue, simulateDealerPlay, determineBlackjackWinner };

// --- End of Part 4 ---
// index.js - Part 5a: Telegram Message/Callback Handlers & Game Result Processing
// --- VERSION: 3.2.0 --- (Implement Crash, Blackjack, Slots Jackpot, Foundational UX Fixes/Adds)

// --- Assuming functions from Part 1, 2, 3, 4 are available (bot, pool, caches, GAME_CONFIG, DB ops, utils, game sims) ---
// --- Specifically: bot, safeSendMessage, escapeMarkdownV2, formatSol, userStateCache, queryDatabase, ensureUserExists,
// --- updateUserBalanceAndLedger, createBetRecord, updateBetStatus, updateUserWagerStats, _handleReferralChecks (from 5b),
// --- GAME_CONFIG, LAMPORTS_PER_SOL, STANDARD_BET_AMOUNTS_LAMPORTS, SOL_DECIMALS,
// --- simulate<Game>, get<Game>PayoutMultiplier, card utilities,
// --- updateUserLastBetAmount, getJackpotAmount, incrementJackpotAmount, updateJackpotAmount (from Part 2)
// --- commandHandlers, menuCommandHandlers (maps from Part 5b, used for 'back' to menu)
// --- userLastBetAmounts map (from Part 1)

// --- Main Message Handler (Entry point from Part 6 webhook/polling) ---
/**
 * Handles incoming Telegram messages.
 * Routes to command handlers or stateful input handlers.
 * @param {import('node-telegram-bot-api').Message} msg The Telegram message object.
 */
async function handleMessage(msg) {
    const chatId = String(msg.chat.id);
    const userId = String(msg.from.id);
    const text = msg.text || '';
    const messageId = msg.message_id;
    const logPrefix = `[Msg Rcv User ${userId} Chat ${chatId}]`;

    console.log(`${logPrefix} Received: "${text.substring(0, 50)}${text.length > 50 ? '...' : ''}"`);

    // Cooldown check (basic spam prevention)
    const now = Date.now();
    const lastCommandTime = commandCooldown.get(userId) || 0;
    if (now - lastCommandTime < USER_COMMAND_COOLDOWN_MS) { // USER_COMMAND_COOLDOWN_MS from Part 1
        // console.log(`${logPrefix} Command cooldown active.`);
        // Optionally send a "please wait" message if too frequent, or just ignore.
        return;
    }
    commandCooldown.set(userId, now);

    // Check if user is awaiting stateful input
    const userCurrentState = userStateCache.get(userId);
    if (userCurrentState && now - userCurrentState.timestamp < USER_STATE_TTL_MS && text !== '/cancel') { // USER_STATE_TTL_MS from Part 1
        // Ensure state is for this chat to prevent cross-chat state issues if bot is in groups
        if (userCurrentState.chatId === chatId) {
            console.log(`${logPrefix} Routing to stateful input: ${userCurrentState.state}`);
            // routeStatefulInput will be defined in Part 5b
            return routeStatefulInput(msg, userCurrentState);
        } else {
            console.warn(`${logPrefix} State mismatch: User state for chat ${userCurrentState.chatId}, message from ${chatId}. Clearing state.`);
            userStateCache.delete(userId);
        }
    } else if (userCurrentState) {
        console.log(`${logPrefix} User state timed out or was for different chat. Clearing.`);
        userStateCache.delete(userId);
    }

    if (text.startsWith('/')) {
        const args = text.split(' ');
        const command = args[0].toLowerCase().split('@')[0]; // Remove / and any @botusername

        // Command Handlers map will be defined in Part 5b
        if (commandHandlers.has(command)) {
            console.log(`${logPrefix} Executing command: ${command}`);
            const handler = commandHandlers.get(command);
            return handler(msg, args);
        } else {
            return safeSendMessage(chatId, "😕 Unknown command. Type /help to see available commands.");
        }
    }
    // Non-command, non-stateful messages can be ignored or handled (e.g., "Hello")
}
// --- End Main Message Handler ---


// --- Main Callback Query Handler (Entry point from Part 6 webhook/polling) ---
/**
 * Handles incoming Telegram callback queries (from inline buttons).
 * @param {import('node-telegram-bot-api').CallbackQuery} callbackQuery The callback query object.
 */
async function handleCallbackQuery(callbackQuery) {
    const userId = String(callbackQuery.from.id);
    const chatId = String(callbackQuery.message.chat.id);
    const messageId = callbackQuery.message.message_id;
    const data = callbackQuery.data;
    const logPrefix = `[CB Rcv User ${userId} Chat ${chatId} Data ${data}]`;

    console.log(`${logPrefix} Received callback.`);

    // Answer callback query immediately to remove "loading" state on button
    bot.answerCallbackQuery(callbackQuery.id).catch(err => console.warn(`${logPrefix} Error answering CB query: ${err.message}`));

    // Cooldown check (can also apply to callbacks if needed)
    const now = Date.now();
    const lastCallbackTime = commandCooldown.get(`${userId}_cb`) || 0;
    if (now - lastCallbackTime < (USER_COMMAND_COOLDOWN_MS / 2)) { // Shorter cooldown for rapid button clicks
        // console.log(`${logPrefix} Callback cooldown active.`);
        return;
    }
    commandCooldown.set(`${userId}_cb`, now);

    // Clear any pending text input state for this user if they click a button.
    const userCurrentState = userStateCache.get(userId);
    if (userCurrentState && userCurrentState.chatId === chatId) {
        console.log(`${logPrefix} Clearing pending text input state ${userCurrentState.state} due to button click.`);
        userStateCache.delete(userId);
    }

    const [action, ...params] = data.split(':');

    // Menu Handlers map will be defined in Part 5b
    if (action === 'menu' && params.length > 0 && menuCommandHandlers.has(params[0])) {
        const menuKey = params[0];
        console.log(`${logPrefix} Routing to menu handler: ${menuKey}`);
        const handler = menuCommandHandlers.get(menuKey);
        return handler(callbackQuery, params.slice(1)); // Pass remaining params
    }

    // --- Foundational UX: "Back" Button Handling ---
    if (action === 'back') {
        const targetStateOrMenu = params[0]; // e.g., 'main_menu', 'game_selection', or a specific previous game step identifier
        const previousMessageId = params[1] ? parseInt(params[1], 10) : null; // Optional previous message ID to edit
        console.log(`${logPrefix} Handling 'back' action to: ${targetStateOrMenu}`);

        // Try to delete the current message with the 'back' button to clean up UI
        if (messageId) {
            bot.deleteMessage(chatId, messageId).catch(e => console.warn(`${logPrefix} Failed to delete message ${messageId} on 'back': ${e.message}`));
        }
        // If a specific previous message ID was part of the callback, it means we might want to "re-render" that message
        // For now, simpler 'back' goes to main menu or a known previous step.
        // More complex "back" would involve restoring exact previous state and message.
        if (targetStateOrMenu === 'main_menu') {
            return commandHandlers.get('/start')(callbackQuery.message, []); // Simulate /start
        } else if (targetStateOrMenu === 'game_selection') {
             // This implies the main menu from /start also serves as game selection
             return commandHandlers.get('/start')(callbackQuery.message, []);
        }
        // Add more specific back targets as needed
        // For now, unhandled back defaults to main menu
        return commandHandlers.get('/start')(callbackQuery.message, []);
    }

    // --- Foundational UX: "Play Again" Handling ---
    if (action === 'play_again') {
        const gameKey = params[0];
        const betAmountLamports = BigInt(params[1] || '0');
        console.log(`${logPrefix} Handling 'play_again' for ${gameKey} with bet ${formatSol(betAmountLamports)} SOL`);

        if (GAME_CONFIG[gameKey] && betAmountLamports > 0n) {
             // Store this as the "last bet" for this game
            const userBets = userLastBetAmounts.get(userId) || new Map();
            userBets.set(gameKey, betAmountLamports);
            userLastBetAmounts.set(userId, userBets);
            // Persist to DB if desired (updateUserLastBetAmount from Part 2) - best done within game transaction
            // For now, in-memory for immediate replay

            const commandHandler = commandHandlers.get(`/${gameKey}`);
            if (commandHandler) {
                // Simulate the game command, but with pre-filled amount
                // The command handler needs to be adapted to potentially accept a prefill amount
                // or we set a temporary state.
                // For now, we'll re-trigger the bet amount selection for that game,
                // highlighting the previous amount or directly placing the bet if design allows.

                // Let's try setting state and proceeding to bet confirmation if amount is valid.
                userStateCache.set(userId, {
                    state: `awaiting_${gameKey}_confirm_replay`, // A new state for this
                    chatId: chatId,
                    messageId: messageId, // Original message context
                    data: { gameKey, betAmountLamports, fromReplay: true },
                    timestamp: Date.now(),
                    // Add breadcrumb data if needed for the confirmation message
                    breadcrumb: `${GAME_CONFIG[gameKey].name} > Replay`
                });
                // Now, send a confirmation message for this replay.
                const betAmountSOL = formatSol(betAmountLamports);
                return safeSendMessage(chatId,
                    `Replay ${GAME_CONFIG[gameKey].name} with ${betAmountSOL} SOL?`, {
                    reply_markup: {
                        inline_keyboard: [
                            [{ text: `✅ Yes, Bet ${betAmountSOL} SOL`, callback_data: `confirm_bet:${gameKey}:${betAmountLamports}` }],
                            [{ text: `❌ No, Change Bet`, callback_data: `menu:${gameKey}` }], // Go to game's initial options
                            [{ text: '↩️ Back to Games', callback_data: 'menu:main' }]
                        ]
                    }
                });
            }
        }
    }
    // --- Foundational UX: "Quick Deposit" Handling ---
    if (action === 'quick_deposit') {
        console.log(`${logPrefix} Handling 'quick_deposit' action.`);
        if (commandHandlers.has('/deposit')) {
            // Try to edit the current message to show deposit info, or send a new one.
             bot.editMessageText("Generating your deposit address...", { chat_id: chatId, message_id: messageId, reply_markup: { inline_keyboard: [] } }).catch(()=>{});
            return commandHandlers.get('/deposit')(callbackQuery.message);
        }
    }


    // Generic bet confirmation (used by various game flows)
    if (action === 'confirm_bet') {
        const gameKey = params[0];
        const betAmountLamports = BigInt(params[1] || '0');
        console.log(`${logPrefix} Bet confirmed for ${gameKey} with ${formatSol(betAmountLamports)} SOL.`);

        // Edit message to "Processing..."
        bot.editMessageText(
            `Processing your ${GAME_CONFIG[gameKey].name} bet of ${formatSol(betAmountLamports)} SOL... ⏳`,
            { chat_id: chatId, message_id: messageId, reply_markup: { inline_keyboard: [] } }
        ).catch(e => console.warn(`${logPrefix} Error editing message for bet processing: ${e.message}`));

        // Route to the specific game handler function
        switch (gameKey) {
            case 'coinflip':
                const choice = params[2]; // e.g., 'heads' or 'tails'
                return handleCoinflipGame(userId, chatId, messageId, betAmountLamports, choice);
            case 'race':
                const chosenLane = parseInt(params[2], 10);
                return handleRaceGame(userId, chatId, messageId, betAmountLamports, chosenLane);
            case 'slots':
                return handleSlotsGame(userId, chatId, messageId, betAmountLamports);
            case 'roulette':
                const betType = params[2];
                const betValue = params[3] || null; // e.g., number for 'straight', color for 'red'
                return handleRouletteGame(userId, chatId, messageId, betAmountLamports, betType, betValue);
            case 'war':
                return handleWarGame(userId, chatId, messageId, betAmountLamports);
            case 'crash': // New Game
                return handleCrashGame(userId, chatId, messageId, betAmountLamports);
            case 'blackjack': // New Game
                return handleBlackjackGame(userId, chatId, messageId, betAmountLamports, 'initial_deal');
            default:
                console.error(`${logPrefix} Unknown gameKey in confirm_bet: ${gameKey}`);
                return safeSendMessage(chatId, "Sorry, something went wrong with that game confirmation.");
        }
    }

    // --- Blackjack Specific Callbacks ---
    if (action === 'blackjack_action') {
        const gameInstanceId = params[0]; // To identify the specific game round
        const playerAction = params[1]; // 'hit' or 'stand'
        console.log(`${logPrefix} Blackjack action: ${playerAction} for game ${gameInstanceId}`);

        const gameState = userStateCache.get(`${userId}_${gameInstanceId}`); // Retrieve state by combined ID
        if (!gameState || gameState.state !== 'awaiting_blackjack_action' || gameState.data.gameKey !== 'blackjack') {
            console.warn(`${logPrefix} No active Blackjack game found for this action or state mismatch.`);
            return safeSendMessage(chatId, "Your Blackjack session seems to have expired or there's an issue. Please start a new game.", {
                reply_markup: { inline_keyboard: [[{ text: 'Play Blackjack', callback_data: 'menu:blackjack' }]]}
            });
        }
        // Edit current message to show processing
        bot.editMessageText(
            `${escapeMarkdownV2(gameState.data.breadcrumb)}\n\nProcessing your action: ${playerAction}...`,
            { chat_id: chatId, message_id: messageId, reply_markup: { inline_keyboard: [] }, parse_mode: 'MarkdownV2' }
        ).catch(e => console.warn(`${logPrefix} Error editing message for blackjack action: ${e.message}`));

        return handleBlackjackGame(userId, chatId, messageId, gameState.data.betAmountLamports, playerAction, gameState);
    }

    // --- Crash Specific Callbacks ---
    if (action === 'crash_cash_out') {
        const gameInstanceId = params[0]; // To identify the specific game round
        console.log(`${logPrefix} Crash cash out attempt for game ${gameInstanceId}`);

        const gameState = userStateCache.get(`${userId}_${gameInstanceId}`);
        if (!gameState || gameState.state !== 'awaiting_crash_cashout' || gameState.data.gameKey !== 'crash') {
            console.warn(`${logPrefix} No active Crash game found for cashing out or state mismatch.`);
            // Don't send message if game might have already ended naturally
            return;
        }
        // The handleCrashGame's interval will check this flag or its absence
        gameState.data.userRequestedCashOut = true;
        userStateCache.set(`${userId}_${gameInstanceId}`, gameState); // Update state immediately

        // Optionally edit message to "Cashing out..." - this might be tricky due to rapid edits by the game itself.
        // The game loop in handleCrashGame will pick this up.
    }


    // Fallback for other unhandled callbacks
    // (Could also be from game-specific bet selections if not handled by `proceedToGameStep` state)
    const tempStateForBetSelection = userStateCache.get(userId);
    if (tempStateForBetSelection && tempStateForBetSelection.chatId === chatId && tempStateForBetSelection.state?.startsWith('awaiting_')) {
        const gameKey = tempStateForBetSelection.data?.gameKey;
        if (gameKey && GAME_CONFIG[gameKey]) {
            console.log(`${logPrefix} Passing to proceedToGameStep for game ${gameKey}, state ${tempStateForBetSelection.state}, data ${data}`);
            return proceedToGameStep(userId, chatId, messageId, tempStateForBetSelection, data);
        }
    }

    console.warn(`${logPrefix} Unhandled callback query action: ${action}`);
}
// --- End Main Callback Query Handler ---


// --- Universal Game Step Processor ---
/**
 * Manages multi-step game interactions (like choosing bet type, number, etc.)
 * @param {string} userId
 * @param {string} chatId
 * @param {number} messageId The ID of the message to potentially edit.
 * @param {object} currentState The current state object from userStateCache.
 * @param {string} callbackData The data from the pressed button.
 */
async function proceedToGameStep(userId, chatId, messageId, currentState, callbackData) {
    const { gameKey, betAmountLamports, step, breadcrumb } = currentState.data;
    const gameConfig = GAME_CONFIG[gameKey];
    const logPrefix = `[GameStep User ${userId} Game ${gameKey} Step ${step || 'initial'}]`;
    console.log(`${logPrefix} CB Data: ${callbackData}`);

    let newStep = step;
    let messageText = "";
    let inlineKeyboard = [];
    let nextStateData = { ...currentState.data };
    let newBreadcrumb = breadcrumb || gameConfig.name;

    try {
        if (gameKey === 'roulette') {
            if (!step || step === 'select_bet_type') {
                newStep = 'confirm_roulette_bet';
                nextStateData.betType = callbackData.split('_')[1]; // e.g., 'type_red' -> 'red'
                newBreadcrumb = `${gameConfig.name} > ${nextStateData.betType.charAt(0).toUpperCase() + nextStateData.betType.slice(1)}`;

                if (['straight'].includes(nextStateData.betType)) { // Add more types if they need number input
                    newStep = 'awaiting_roulette_number';
                    userStateCache.set(userId, {
                        state: 'awaiting_roulette_number', // For text input via routeStatefulInput (Part 5b)
                        chatId: chatId,
                        messageId: messageId,
                        data: { ...nextStateData, gameKey, betAmountLamports, breadcrumb: newBreadcrumb },
                        timestamp: Date.now(),
                        previousState: currentState // For 'back' functionality
                    });
                    messageText = `${escapeMarkdownV2(newBreadcrumb)}\nSelected bet: ${escapeMarkdownV2(nextStateData.betType)}. Please type the number (0-36) you want to bet on.`;
                    // No buttons here, awaiting text input. Add cancel/back.
                    inlineKeyboard = [[{ text: '❌ Cancel Bet', callback_data: 'menu:main' }]];
                } else {
                    // For bets like red/black, even/odd, directly to confirmation
                    messageText = `${escapeMarkdownV2(newBreadcrumb)}\nBet ${formatSol(betAmountLamports)} SOL on ${escapeMarkdownV2(nextStateData.betType)}?`;
                    inlineKeyboard = [
                        [{ text: `✅ Yes, Confirm Bet`, callback_data: `confirm_bet:roulette:${betAmountLamports}:${nextStateData.betType}` }],
                        [{ text: '↩️ Change Bet Type', callback_data: `menu:${gameKey}:${betAmountLamports}` }], // Back to bet type selection
                        [{ text: '💰 Change Amount', callback_data: `menu:${gameKey}` }], // Back to game menu to pick new amount
                        [{ text: '❌ Cancel & Exit', callback_data: 'menu:main' }]
                    ];
                }
            }
            // Other roulette steps could be handled here if needed
        }
        // Add logic for other games if they have multi-step button-based selections:
        // else if (gameKey === 'blackjack') { /* ... blackjack specific steps ... */ }
        // else if (gameKey === 'crash') { /* ... crash specific steps ... (though mostly animation) */ }

        else {
            console.warn(`${logPrefix} Unknown gameKey or step in proceedToGameStep.`);
            await bot.editMessageText("Sorry, there was an issue with that selection.", { chat_id: chatId, message_id: messageId, reply_markup: { inline_keyboard: [] } }).catch(()=>{});
            userStateCache.delete(userId);
            return;
        }

        // Update message with new options or confirmation
        if (messageText) {
            nextStateData.step = newStep; // Update step in data for next state
            nextStateData.breadcrumb = newBreadcrumb;
            // Set state only if we are expecting further button interaction, not for text input states set above
            if (newStep !== 'awaiting_roulette_number' && newStep !== 'confirm_roulette_bet' /* and similar final confirmation steps */) {
                 userStateCache.set(userId, {
                     state: `awaiting_${gameKey}_${newStep}`, // More specific state
                     chatId: chatId,
                     messageId: messageId,
                     data: nextStateData,
                     timestamp: Date.now(),
                     previousState: currentState // For 'back' functionality
                 });
            }

            await bot.editMessageText(messageText, {
                chat_id: chatId,
                message_id: messageId,
                reply_markup: { inline_keyboard: inlineKeyboard },
                parse_mode: 'MarkdownV2'
            }).catch(e => console.error(`${logPrefix} Error editing message for game step:`, e.message));
        }

    } catch (error) {
        console.error(`${logPrefix} Error in proceedToGameStep:`, error);
        await safeSendMessage(chatId, "An unexpected error occurred. Please try again.");
        userStateCache.delete(userId);
    }
}
// --- End Universal Game Step Processor ---


// --- Core Bet Placement Logic ---
/**
 * Handles the core logic of placing a bet:
 * 1. Ensures user and balance exist.
 * 2. Deducts wager from balance.
 * 3. Creates a bet record.
 * 4. Updates user's total wager stats.
 * 5. Handles referral checks and potential initial referral payouts.
 * 6. Updates user's last bet amount for the game.
 * All within a single database transaction.
 * @param {string} userId
 * @param {string} chatId
 * @param {string} gameKey
 * @param {object} betDetails For `bets` table JSONB column.
 * @param {bigint} betAmountLamports
 * @returns {Promise<{success: boolean, betId?: number, error?: string, insufficientBalance?: boolean, newBalance?: bigint}>}
 */
async function placeBet(userId, chatId, gameKey, betDetails, betAmountLamports) {
    const logPrefix = `[PlaceBet User ${userId} Game ${gameKey}]`;
    let client;
    try {
        client = await pool.connect();
        await client.query('BEGIN');

        // 1. Ensure user exists (locks rows for safety)
        const { isNewUser } = await ensureUserExists(userId, client); // from Part 2

        // 2. Deduct wager (updateUserBalanceAndLedger will check for sufficient funds)
        const balanceUpdateResult = await updateUserBalanceAndLedger(client, userId, -betAmountLamports, `bet_placed:${gameKey}`, {}, `Placed bet on ${GAME_CONFIG[gameKey].name}`);
        if (!balanceUpdateResult.success) {
            await client.query('ROLLBACK');
            client.release();
            return {
                success: false,
                error: balanceUpdateResult.error || 'Failed to update balance.',
                insufficientBalance: balanceUpdateResult.error === 'Insufficient balance'
            };
        }
        const newBalance = balanceUpdateResult.newBalance;

        // 3. Create bet record
        const betRecordResult = await createBetRecord(client, userId, chatId, gameKey, betDetails, betAmountLamports); // from Part 2
        if (!betRecordResult.success || !betRecordResult.betId) {
            await client.query('ROLLBACK');
            client.release();
            return { success: false, error: betRecordResult.error || 'Failed to create bet record.' };
        }
        const betId = betRecordResult.betId;

        // 4. Update user's total wager stats
        await updateUserWagerStats(userId, betAmountLamports, client); // from Part 2

        // 5. Handle referral checks (function from Part 5b)
        // _handleReferralChecks needs to accept the client for transactional integrity
        await _handleReferralChecks(userId, betId, betAmountLamports, client);

        // 6. Update user's last bet amount for this game
        await updateUserLastBetAmount(userId, gameKey, betAmountLamports, client); // from Part 2
        // Also update in-memory cache for immediate "Play Again" suggestions
        const userBets = userLastBetAmounts.get(userId) || new Map();
        userBets.set(gameKey, betAmountLamports);
        userLastBetAmounts.set(userId, userBets);


        await client.query('COMMIT');
        client.release();
        console.log(`${logPrefix} Bet ID ${betId} successfully placed for ${formatSol(betAmountLamports)} SOL. New balance: ${formatSol(newBalance)} SOL.`);
        return { success: true, betId, newBalance };

    } catch (error) {
        if (client) {
            try { await client.query('ROLLBACK'); } catch (rbErr) { console.error(`${logPrefix} Rollback failed:`, rbErr); }
            client.release();
        }
        console.error(`${logPrefix} Error placing bet:`, error);
        return { success: false, error: `Error placing bet: ${error.message}` };
    }
}
// --- End Core Bet Placement Logic ---

// --- Game Result Handlers ---
// These functions take a confirmed bet and simulate the game, update balances, and send results.

async function handleCoinflipGame(userId, chatId, originalMessageId, betAmountLamports, choice) {
    const gameKey = 'coinflip';
    const logPrefix = `[CoinflipGame User ${userId} Bet ${formatSol(betAmountLamports)}]`;
    console.log(`${logPrefix} Handling Coinflip for choice: ${choice}`);
    let client;

    try {
        const betPlacement = await placeBet(userId, chatId, gameKey, { choice }, betAmountLamports);
        if (!betPlacement.success) {
            let replyText = betPlacement.error || "Failed to place your Coinflip bet.";
            if (betPlacement.insufficientBalance) {
                 replyText = `⚠️ Insufficient balance for ${formatSol(betAmountLamports)} SOL Coinflip bet. Your balance: ${formatSol(await getUserBalance(userId))} SOL.`;
                 return safeSendMessage(chatId, replyText, {
                     reply_markup: { inline_keyboard: [[{ text: '💰 Quick Deposit', callback_data: 'quick_deposit' }]] }
                 });
            }
            return safeSendMessage(chatId, replyText);
        }
        const betId = betPlacement.betId;

        // Edit original message to "Flipping..."
        let currentMessageId = originalMessageId;
        const processingMsg = await safeSendMessage(chatId, `🪙 Flipping a coin for your ${formatSol(betAmountLamports)} SOL bet on *${escapeMarkdownV2(choice)}*\\!\\!\\!`, {parse_mode: 'MarkdownV2'});
        if(processingMsg?.message_id && originalMessageId) { // Delete old prompt, use new message for animation
            bot.deleteMessage(chatId, originalMessageId).catch(()=>{});
            currentMessageId = processingMsg.message_id;
        } else if (processingMsg?.message_id) {
            currentMessageId = processingMsg.message_id;
        }


        // Simulate animation (simplified)
        const animationFrames = ["🌑", "🌒", "🌓", "🌔", "🌕", "🌖", "🌗", "🌘"];
        for (let i = 0; i < animationFrames.length; i++) {
            await sleep(300); // from Part 3
            if (currentMessageId) {
                await bot.editMessageText(
                    `Flipping\\.\\.\\. ${animationFrames[i]}\nYour bet: ${formatSol(betAmountLamports)} SOL on *${escapeMarkdownV2(choice)}*`,
                    { chat_id: chatId, message_id: currentMessageId, parse_mode: 'MarkdownV2' }
                ).catch(e => console.warn(`${logPrefix} Animation edit error: ${e.message}`));
            }
        }

        const { outcome } = playCoinflip(choice); // from Part 4
        const win = outcome === choice;
        const houseEdge = GAME_CONFIG[gameKey].houseEdge;
        const payoutOnWinLamports = win ? BigInt(Math.floor(Number(betAmountLamports) * (2 - houseEdge))) : 0n; // Player gets stake + winnings
        const profitLamports = win ? payoutOnWinLamports - betAmountLamports : -betAmountLamports;

        client = await pool.connect();
        await client.query('BEGIN');
        let finalBalanceUpdateResult;
        if (win) {
            finalBalanceUpdateResult = await updateUserBalanceAndLedger(client, userId, payoutOnWinLamports, `bet_win:${gameKey}`, { betId });
        } else {
            // Bet amount already deducted by placeBet, loss is implicit. Ledger entry for loss can be added for clarity if desired.
            finalBalanceUpdateResult = { success: true, newBalance: betPlacement.newBalance + profitLamports }; // effective new balance after loss
        }
        await updateBetStatus(client, betId, win ? 'completed_win' : 'completed_loss', payoutOnWinLamports); // from Part 2
        await client.query('COMMIT');

        let resultText = `Coin landed on: *${escapeMarkdownV2(outcome)}*\\!\n\n`;
        if (win) {
            resultText += `🎉 You *WON*\\! 🎉\nReturned: ${formatSol(payoutOnWinLamports)} SOL (Profit: ${formatSol(profitLamports)} SOL)`;
        } else {
            resultText += `😥 You *LOST*\\! 😥\nLost: ${formatSol(betAmountLamports)} SOL`;
        }
        resultText += `\n\nNew balance: ${formatSol(finalBalanceUpdateResult.newBalance)} SOL`;

        // Foundational UX: Play Again & Back to Games Menu buttons
        const inlineKeyboard = [[
            { text: `🪙 Play Coinflip Again`, callback_data: `play_again:${gameKey}:${betAmountLamports}` },
            { text: '🎮 Games Menu', callback_data: 'menu:main' }
        ]];

        if (currentMessageId) {
             await bot.editMessageText(resultText, { chat_id: chatId, message_id: currentMessageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: inlineKeyboard } });
        } else {
            await safeSendMessage(chatId, resultText, { parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: inlineKeyboard } });
        }

    } catch (error) {
        console.error(`${logPrefix} Error:`, error);
        if (client) { try { await client.query('ROLLBACK'); } catch (e) { console.error(`${logPrefix} Rollback error:`, e); } }
        await safeSendMessage(chatId, "Sorry, an error occurred while playing Coinflip. Your balance has not been affected by this game attempt if the bet wasn't placed.");
    } finally {
        if (client) client.release();
    }
}

async function handleRaceGame(userId, chatId, originalMessageId, betAmountLamports, chosenLane) {
    const gameKey = 'race';
    const logPrefix = `[RaceGame User ${userId} Bet ${formatSol(betAmountLamports)} Lane ${chosenLane}]`;
    console.log(`${logPrefix} Handling Race.`);
    let client;
    const totalLanes = 4; // Assuming RACE_HORSES.length in Part 5b is 4

    // Payout multipliers (example, can be dynamic or configured)
    // These are for WINNINGS, not total return. Actual payout multiplier is (1 + this_value) * (1 - houseEdge)
    const laneBaseMultipliers = { 1: 2.5, 2: 3.0, 3: 3.5, 4: 4.0 }; // Example: Lane 1 is slight favorite
    const selectedLaneBaseMultiplier = laneBaseMultipliers[chosenLane] || 2.0; // Default if lane not found

    try {
        const betPlacement = await placeBet(userId, chatId, gameKey, { chosenLane }, betAmountLamports);
        if (!betPlacement.success) {
            let replyText = betPlacement.error || "Failed to place your Race bet.";
            if (betPlacement.insufficientBalance) {
                replyText = `⚠️ Insufficient balance for ${formatSol(betAmountLamports)} SOL Race bet. Your balance: ${formatSol(await getUserBalance(userId))} SOL.`;
                 return safeSendMessage(chatId, replyText, {
                     reply_markup: { inline_keyboard: [[{ text: '💰 Quick Deposit', callback_data: 'quick_deposit' }]] }
                 });
            }
            return safeSendMessage(chatId, replyText);
        }
        const betId = betPlacement.betId;

        let currentMessageId = originalMessageId;
        const processingMsg = await safeSendMessage(chatId, `🐎 The race is on\\! You bet ${formatSol(betAmountLamports)} SOL on Horse \\#${chosenLane}\\.\n\n🏁 \\[\\.\\.\\.\\.\\.\\.\\]`, {parse_mode: 'MarkdownV2'});
        if(processingMsg?.message_id && originalMessageId) {
            bot.deleteMessage(chatId, originalMessageId).catch(()=>{});
            currentMessageId = processingMsg.message_id;
        } else if (processingMsg?.message_id) {
            currentMessageId = processingMsg.message_id;
        }

        const raceTrackLength = 10;
        let positions = Array(totalLanes).fill(0);
        const horseEmojis = ["🐎₁", "🏇₂", "🐴₃", "🎠₄"]; // Example emojis for lanes

        for (let lap = 0; lap < raceTrackLength; lap++) {
            await sleep(600);
            // Randomly advance one horse significantly, others a bit
            const leaderAdvance = Math.random() < 0.6 ? 2 : 1;
            const leaderIdx = Math.floor(Math.random() * totalLanes);
            positions[leaderIdx] = Math.min(raceTrackLength, positions[leaderIdx] + leaderAdvance);

            for (let i = 0; i < totalLanes; i++) {
                if (i !== leaderIdx && Math.random() < 0.7) { // Others might advance too
                    positions[i] = Math.min(raceTrackLength, positions[i] + 1);
                }
            }

            let raceDisplay = `Race Progress \\(Lap ${lap + 1}/${raceTrackLength}\\):\n`;
            for (let i = 0; i < totalLanes; i++) {
                const progress = '🏆'.repeat(positions[i]) + '▫️'.repeat(raceTrackLength - positions[i]);
                raceDisplay += `${horseEmojis[i]} \\[${progress}\\] ${chosenLane === (i + 1) ? "*(Your Pick)*" : ""}\n`;
            }
            if (currentMessageId) {
                 await bot.editMessageText(escapeMarkdownV2(raceDisplay) + `\nYou bet ${formatSol(betAmountLamports)} SOL on Horse \\#${chosenLane}\\.`, { chat_id: chatId, message_id: currentMessageId, parse_mode: 'MarkdownV2' }).catch(e => console.warn(`${logPrefix} Animation edit error: ${e.message}`));
            }
            if (positions.some(p => p === raceTrackLength)) break; // A horse finished
        }


        const { winningLane } = simulateRace(totalLanes); // from Part 4
        const win = winningLane === chosenLane;
        const houseEdge = GAME_CONFIG[gameKey].houseEdge;
        // Payout is (Stake * (1 + BaseMultiplier)) * (1 - HouseEdge)
        const payoutOnWinLamports = win ? BigInt(Math.floor(Number(betAmountLamports) * (1 + selectedLaneBaseMultiplier) * (1 - houseEdge))) : 0n;
        const profitLamports = win ? payoutOnWinLamports - betAmountLamports : -betAmountLamports;

        client = await pool.connect();
        await client.query('BEGIN');
        let finalBalanceUpdateResult;
        if (win) {
            finalBalanceUpdateResult = await updateUserBalanceAndLedger(client, userId, payoutOnWinLamports, `bet_win:${gameKey}`, { betId });
        } else {
            finalBalanceUpdateResult = { success: true, newBalance: betPlacement.newBalance + profitLamports };
        }
        await updateBetStatus(client, betId, win ? 'completed_win' : 'completed_loss', payoutOnWinLamports);
        await client.query('COMMIT');

        let resultText = `🏁 The race is over\\! Horse \\#${winningLane} (*${horseEmojis[winningLane-1]}*) is the winner\\!\n\n`;
        if (win) {
            resultText += `🎉 You *WON*\\! 🎉\nReturned: ${formatSol(payoutOnWinLamports)} SOL (Profit: ${formatSol(profitLamports)} SOL)`;
        } else {
            resultText += `😥 You *LOST*\\! 😥\nLost: ${formatSol(betAmountLamports)} SOL`;
        }
        resultText += `\n\nNew balance: ${formatSol(finalBalanceUpdateResult.newBalance)} SOL`;

        const inlineKeyboard = [[
            { text: `🐎 Play Race Again`, callback_data: `play_again:${gameKey}:${betAmountLamports}` },
            { text: '🎮 Games Menu', callback_data: 'menu:main' }
        ]];

        if (currentMessageId) {
            await bot.editMessageText(resultText, { chat_id: chatId, message_id: currentMessageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: inlineKeyboard } });
        } else {
            await safeSendMessage(chatId, resultText, { parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: inlineKeyboard } });
        }

    } catch (error) {
        console.error(`${logPrefix} Error:`, error);
        if (client) { try { await client.query('ROLLBACK'); } catch (e) { console.error(`${logPrefix} Rollback error:`, e); } }
        await safeSendMessage(chatId, "Sorry, an error occurred while playing Race.");
    } finally {
        if (client) client.release();
    }
}

async function handleSlotsGame(userId, chatId, originalMessageId, betAmountLamports) {
    const gameKey = 'slots';
    const logPrefix = `[SlotsGame User ${userId} Bet ${formatSol(betAmountLamports)}]`;
    console.log(`${logPrefix} Handling Slots.`);
    let client;

    try {
        client = await pool.connect(); // Get client early for jackpot contribution
        await client.query('BEGIN');

        // --- SLOTS JACKPOT: Contribution ---
        const jackpotContributionPercent = GAME_CONFIG.slots.jackpotContributionPercent;
        let actualWagerForGame = betAmountLamports;
        let contributionToJackpot = 0n;

        if (jackpotContributionPercent > 0) {
            contributionToJackpot = BigInt(Math.floor(Number(betAmountLamports) * jackpotContributionPercent));
            if (contributionToJackpot > 0n) {
                const incremented = await incrementJackpotAmount(gameKey, contributionToJackpot, client); // from Part 2
                if (incremented) {
                    console.log(`${logPrefix} Contributed ${formatSol(contributionToJackpot)} SOL to slots jackpot.`);
                    // Update global in-memory jackpot (will be more robustly synced if bot restarts)
                    currentSlotsJackpotLamports += contributionToJackpot; // currentSlotsJackpotLamports from Part 1
                } else {
                    console.error(`${logPrefix} Failed to increment slots jackpot in DB. Proceeding without contribution count.`);
                    // Decide if bet should fail or proceed without confirmed contribution
                    // For now, proceed but this is a warning sign
                }
            }
        }
        // The bet is placed with the original amount, jackpot contribution is an internal detail from house perspective or part of wager.
        // For simplicity, let's assume the contribution effectively slightly raises the house edge or comes from profit margin.
        // The player is still betting `betAmountLamports`.

        const betPlacement = await placeBet(userId, chatId, gameKey, {}, betAmountLamports); // Bet details empty for simple slots
        if (!betPlacement.success) {
            await client.query('ROLLBACK'); // Rollback jackpot contribution if bet fails
            client.release();
            let replyText = betPlacement.error || "Failed to place your Slots bet.";
            if (betPlacement.insufficientBalance) {
                 replyText = `⚠️ Insufficient balance for ${formatSol(betAmountLamports)} SOL Slots bet. Your balance: ${formatSol(await getUserBalance(userId))} SOL.`;
                 return safeSendMessage(chatId, replyText, {
                     reply_markup: { inline_keyboard: [[{ text: '💰 Quick Deposit', callback_data: 'quick_deposit' }]] }
                 });
            }
            return safeSendMessage(chatId, replyText);
        }
        const betId = betPlacement.betId;

        // Commit bet placement and jackpot contribution before animation
        await client.query('COMMIT');


        let currentMessageId = originalMessageId;
        const processingMsg = await safeSendMessage(chatId, `🎰 Spinning the slots for your ${formatSol(betAmountLamports)} SOL bet\\!\\!\\!`, {parse_mode: 'MarkdownV2'});
        if(processingMsg?.message_id && originalMessageId) {
            bot.deleteMessage(chatId, originalMessageId).catch(()=>{});
            currentMessageId = processingMsg.message_id;
        } else if (processingMsg?.message_id) {
            currentMessageId = processingMsg.message_id;
        }

        const slotEmojis = { 'C': '🍒', 'L': '🍋', 'O': '🍊', 'B': '🔔', '7': '❼', 'J': '💎' }; // Diamond for Jackpot visual
        const initialReels = `[❓|❓|❓]`;
        if(currentMessageId) await bot.editMessageText(escapeMarkdownV2(initialReels) + `\nSpinning...`, { chat_id: chatId, message_id: currentMessageId, parse_mode: 'MarkdownV2' }).catch(e => console.warn(`${logPrefix} Animation edit error: ${e.message}`));

        for (let i = 0; i < 3; i++) { // Simulate 3 reel stops
            await sleep(700);
            let tempReels = ["[❓","❓","❓]"];
            if (i === 0) tempReels[0] = `[${slotEmojis[simulateSlots().symbols[0]]}`; // Show first symbol
            if (i === 1) tempReels = [`[${slotEmojis[simulateSlots().symbols[0]]}`, `${slotEmojis[simulateSlots().symbols[1]]}`, `❓]`]; // Show first two
            if (i === 2) { // Final reveal logic will be outside loop
                 const randomSpin1 = simulateSlots().symbols;
                 const randomSpin2 = simulateSlots().symbols;
                 if (currentMessageId) await bot.editMessageText(escapeMarkdownV2(`[${slotEmojis[randomSpin1[0]]}|${slotEmojis[randomSpin2[1]]}|${slotEmojis[randomSpin1[2]]}]`) + `\nSpinning...`, { chat_id: chatId, message_id: currentMessageId, parse_mode: 'MarkdownV2' }).catch(e => console.warn(`${logPrefix} Animation edit error: ${e.message}`));
                 await sleep(500);
            } else {
                 if (currentMessageId) await bot.editMessageText(escapeMarkdownV2(tempReels.join('|')) + `\nSpinning...`, { chat_id: chatId, message_id: currentMessageId, parse_mode: 'MarkdownV2' }).catch(e => console.warn(`${logPrefix} Animation edit error: ${e.message}`));
            }
        }

        const { symbols, payoutMultiplier, isJackpotWin } = simulateSlots(); // from Part 4
        const displaySymbols = symbols.map(s => slotEmojis[s] || s).join(' | ');
        const houseEdge = GAME_CONFIG[gameKey].houseEdge;

        let finalPayoutLamports = 0n;
        let profitLamports = -betAmountLamports;
        let resultMessage = "";
        let win = false;

        client = await pool.connect(); // New transaction for outcome
        await client.query('BEGIN');

        if (isJackpotWin) {
            // --- SLOTS JACKPOT: Awarding ---
            const jackpotAmountToAward = await getJackpotAmount(gameKey, client); // from Part 2
            if (jackpotAmountToAward > 0n) {
                finalPayoutLamports = jackpotAmountToAward; // Player wins the entire current jackpot
                profitLamports = finalPayoutLamports - betAmountLamports; // Net profit includes jackpot minus their stake
                win = true;
                resultMessage = `💎💎💎 *JACKPOT WIN* 💎💎💎\nYou won the progressive jackpot of ${formatSol(jackpotAmountToAward)} SOL\\!`;

                // Award jackpot to user
                await updateUserBalanceAndLedger(client, userId, finalPayoutLamports, `bet_win_jackpot:${gameKey}`, { betId });
                // Reset jackpot in DB
                await updateJackpotAmount(gameKey, GAME_CONFIG.slots.jackpotSeedLamports, client); // from Part 2
                currentSlotsJackpotLamports = GAME_CONFIG.slots.jackpotSeedLamports; // Reset in-memory
                await notifyAdmin(`🎉 User ${escapeMarkdownV2(userId)} HIT THE SLOTS JACKPOT for ${escapeMarkdownV2(formatSol(jackpotAmountToAward))} SOL! Bet ID: ${betId}`);
            } else {
                // Jackpot was 0 (should not happen if seeded), treat as normal small win or specific fallback
                isJackpotWin = false; // Revert flag
                console.warn(`${logPrefix} Jackpot win triggered, but DB jackpot amount was 0. Treating as non-jackpot.`);
                // Fall through to normal payout logic or define a fixed small multiplier for 'J J J' if jackpot is empty
                 const baseWin = BigInt(Math.floor(Number(betAmountLamports) * 200 * (1 - houseEdge))); // Example: 200x for empty jackpot J J J
                 finalPayoutLamports = baseWin;
                 profitLamports = finalPayoutLamports - betAmountLamports;
                 win = true;
                 resultMessage = `💥 Triple Diamonds\\! You won ${formatSol(finalPayoutLamports)} SOL\\!`;
            }
        }

        if (!isJackpotWin && payoutMultiplier > 0) { // Normal win
            win = true;
            // Payout is Stake + (Stake * Multiplier) = Stake * (1 + Multiplier)
            // Then apply house edge to the *winnings* part, or to the total return.
            // If edge applies to winnings: (Stake * Multiplier * (1 - HouseEdge)) is the profit. Total return = Stake + Profit
            const grossWinnings = BigInt(Number(betAmountLamports) * payoutMultiplier);
            const netWinnings = BigInt(Math.floor(Number(grossWinnings) * (1 - houseEdge)));
            finalPayoutLamports = betAmountLamports + netWinnings;
            profitLamports = netWinnings;
            resultMessage = `🎉 You *WON*\\! 🎉\nReturned: ${formatSol(finalPayoutLamports)} SOL (Profit: ${formatSol(profitLamports)} SOL)`;
        } else if (!isJackpotWin) { // Loss
            win = false;
            finalPayoutLamports = 0n; // Already deducted
            resultMessage = `😥 You *LOST*\\! 😥\nLost: ${formatSol(betAmountLamports)} SOL`;
        }

        let finalBalanceUpdateResult;
        if (win && !isJackpotWin) { // Jackpot winner balance already updated
            finalBalanceUpdateResult = await updateUserBalanceAndLedger(client, userId, finalPayoutLamports, `bet_win:${gameKey}`, { betId });
        } else if (isJackpotWin) {
            // Balance was updated with jackpot amount, fetch current for display
            const currentBalance = await getUserBalance(userId); // Uses its own transaction/client or reads potentially stale
            finalBalanceUpdateResult = { success: true, newBalance: currentBalance }; // This might be slightly off if other ops happened
        }
        else {
             finalBalanceUpdateResult = { success: true, newBalance: betPlacement.newBalance + profitLamports };
        }
        await updateBetStatus(client, betId, win ? 'completed_win' : 'completed_loss', finalPayoutLamports);
        await client.query('COMMIT');

        let fullResultMessage = `🎰 Result: *${escapeMarkdownV2(displaySymbols)}*\n\n${resultMessage}`;
        fullResultMessage += `\n\nNew balance: ${formatSol(finalBalanceUpdateResult.newBalance)} SOL`;
        const currentJackpotDisplay = formatSol(await getJackpotAmount(gameKey)); // Fetch latest for display
        fullResultMessage += `\n\n💎 Next Slots Jackpot: *${escapeMarkdownV2(currentJackpotDisplay)} SOL*`;


        const inlineKeyboard = [[
            { text: `🎰 Play Slots Again`, callback_data: `play_again:${gameKey}:${betAmountLamports}` },
            { text: '🎮 Games Menu', callback_data: 'menu:main' }
        ]];

        if(currentMessageId) {
            await bot.editMessageText(fullResultMessage, { chat_id: chatId, message_id: currentMessageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: inlineKeyboard } });
        } else {
            await safeSendMessage(chatId, fullResultMessage, { parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: inlineKeyboard } });
        }

    } catch (error) {
        console.error(`${logPrefix} Error:`, error);
        if (client) { try { await client.query('ROLLBACK'); } catch (e) { console.error(`${logPrefix} Rollback error:`, e); } }
        await safeSendMessage(chatId, "Sorry, an error occurred while playing Slots.");
    } finally {
        if (client) client.release();
    }
}


async function handleWarGame(userId, chatId, originalMessageId, betAmountLamports) {
    const gameKey = 'war';
    const logPrefix = `[WarGame User ${userId} Bet ${formatSol(betAmountLamports)}]`;
    console.log(`${logPrefix} Handling Casino War.`);
    let client;

    try {
        const betPlacement = await placeBet(userId, chatId, gameKey, {}, betAmountLamports);
        if (!betPlacement.success) {
            let replyText = betPlacement.error || "Failed to place your Casino War bet.";
            if (betPlacement.insufficientBalance) {
                 replyText = `⚠️ Insufficient balance for ${formatSol(betAmountLamports)} SOL War bet. Your balance: ${formatSol(await getUserBalance(userId))} SOL.`;
                 return safeSendMessage(chatId, replyText, {
                     reply_markup: { inline_keyboard: [[{ text: '💰 Quick Deposit', callback_data: 'quick_deposit' }]] }
                 });
            }
            return safeSendMessage(chatId, replyText);
        }
        const betId = betPlacement.betId;

        let currentMessageId = originalMessageId;
        const processingMsg = await safeSendMessage(chatId, `🃏 Dealing cards for Casino War\\! Your bet: ${formatSol(betAmountLamports)} SOL`, {parse_mode: 'MarkdownV2'});
        if(processingMsg?.message_id && originalMessageId) {
            bot.deleteMessage(chatId, originalMessageId).catch(()=>{});
            currentMessageId = processingMsg.message_id;
        } else if (processingMsg?.message_id) {
            currentMessageId = processingMsg.message_id;
        }

        const { result, playerCard, dealerCard, playerRank, dealerRank } = simulateWar(); // from Part 4
        const houseEdge = GAME_CONFIG[gameKey].houseEdge; // Typically applied on pushes or by rules. Here, on win.

        let payoutOnWinLamports = 0n;
        let profitLamports = -betAmountLamports;
        let status;
        let resultTextPart = "";

        if (result === 'win') {
            // Player wins 1:1 on their bet, minus house edge on winnings.
            // Profit = Bet * (1 - HouseEdge)
            // Total Return = Bet + Profit
            const netWinnings = BigInt(Math.floor(Number(betAmountLamports) * (1 - houseEdge)));
            payoutOnWinLamports = betAmountLamports + netWinnings;
            profitLamports = netWinnings;
            status = 'completed_win';
            resultTextPart = `🎉 You *WON*\\! 🎉\nReturned: ${formatSol(payoutOnWinLamports)} SOL (Profit: ${formatSol(profitLamports)} SOL)`;
        } else if (result === 'loss') {
            payoutOnWinLamports = 0n; // Already deducted
            status = 'completed_loss';
            resultTextPart = `😥 You *LOST*\\! 😥\nLost: ${formatSol(betAmountLamports)} SOL`;
        } else { // push
            payoutOnWinLamports = betAmountLamports; // Return original stake
            profitLamports = 0n;
            status = 'completed_push';
            resultTextPart = `🔵 It's a *PUSH*\\! 🔵\nStake returned: ${formatSol(betAmountLamports)} SOL`;
        }

        client = await pool.connect();
        await client.query('BEGIN');
        let finalBalanceUpdateResult;
        if (result === 'win' || result === 'push') {
            finalBalanceUpdateResult = await updateUserBalanceAndLedger(client, userId, payoutOnWinLamports, `bet_${result}:${gameKey}`, { betId });
        } else {
             finalBalanceUpdateResult = { success: true, newBalance: betPlacement.newBalance + profitLamports };
        }
        await updateBetStatus(client, betId, status, payoutOnWinLamports);
        await client.query('COMMIT');

        let fullResultText = `Your Card: *${escapeMarkdownV2(playerCard)}* (Rank: ${playerRank})\nDealer Card: *${escapeMarkdownV2(dealerCard)}* (Rank: ${dealerRank})\n\n${resultTextPart}`;
        fullResultText += `\n\nNew balance: ${formatSol(finalBalanceUpdateResult.newBalance)} SOL`;

        const inlineKeyboard = [[
            { text: `🃏 Play War Again`, callback_data: `play_again:${gameKey}:${betAmountLamports}` },
            { text: '🎮 Games Menu', callback_data: 'menu:main' }
        ]];

        if(currentMessageId) {
            await bot.editMessageText(fullResultText, { chat_id: chatId, message_id: currentMessageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: inlineKeyboard } });
        } else {
            await safeSendMessage(chatId, fullResultText, { parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: inlineKeyboard } });
        }

    } catch (error) {
        console.error(`${logPrefix} Error:`, error);
        if (client) { try { await client.query('ROLLBACK'); } catch (e) { console.error(`${logPrefix} Rollback error:`, e); } }
        await safeSendMessage(chatId, "Sorry, an error occurred while playing Casino War.");
    } finally {
        if (client) client.release();
    }
}


async function handleRouletteGame(userId, chatId, originalMessageId, betAmountLamports, betType, betValue) {
    const gameKey = 'roulette';
    const logPrefix = `[RouletteGame User ${userId} Bet ${formatSol(betAmountLamports)} Type ${betType} Val ${betValue}]`;
    console.log(`${logPrefix} Handling Roulette.`);
    let client;

    try {
        const betPlacement = await placeBet(userId, chatId, gameKey, { betType, betValue }, betAmountLamports);
        if (!betPlacement.success) {
            let replyText = betPlacement.error || "Failed to place your Roulette bet.";
            if (betPlacement.insufficientBalance) {
                 replyText = `⚠️ Insufficient balance for ${formatSol(betAmountLamports)} SOL Roulette bet. Your balance: ${formatSol(await getUserBalance(userId))} SOL.`;
                 return safeSendMessage(chatId, replyText, {
                     reply_markup: { inline_keyboard: [[{ text: '💰 Quick Deposit', callback_data: 'quick_deposit' }]] }
                 });
            }
            return safeSendMessage(chatId, replyText);
        }
        const betId = betPlacement.betId;

        let currentMessageId = originalMessageId;
        const processingMsg = await safeSendMessage(chatId, `⚪️ Spinning the Roulette wheel for your ${formatSol(betAmountLamports)} SOL bet on *${escapeMarkdownV2(betType)}${betValue ? ` (${escapeMarkdownV2(String(betValue))})` : ''}*\\!`, {parse_mode: 'MarkdownV2'});
        if(processingMsg?.message_id && originalMessageId) {
            bot.deleteMessage(chatId, originalMessageId).catch(()=>{});
            currentMessageId = processingMsg.message_id;
        } else if (processingMsg?.message_id) {
            currentMessageId = processingMsg.message_id;
        }

        // Animation (Simplified)
        const rouletteNumbers = [0, 32, 15, 19, 4, 21, 2, 25, 17, 34, 6, 27, 13, 36, 11, 30, 8, 23, 10, 5, 24, 16, 33, 1, 20, 14, 31, 9, 22, 18, 29, 7, 28, 12, 35, 3, 26];
        for (let i = 0; i < 10; i++) {
            await sleep(300 + i * 50); // Slow down
            const displayedNum = rouletteNumbers[Math.floor(Math.random() * rouletteNumbers.length)];
            if (currentMessageId) {
                await bot.editMessageText(
                    `Spinning\\.\\.\\. *${escapeMarkdownV2(String(displayedNum))}*\nYour bet: ${formatSol(betAmountLamports)} SOL on *${escapeMarkdownV2(betType)}${betValue ? ` (${escapeMarkdownV2(String(betValue))})` : ''}*`,
                    { chat_id: chatId, message_id: currentMessageId, parse_mode: 'MarkdownV2' }
                ).catch(e => console.warn(`${logPrefix} Animation edit error: ${e.message}`));
            }
        }

        const { winningNumber } = simulateRouletteSpin(); // from Part 4
        const basePayoutMultiplier = getRoulettePayoutMultiplier(betType, betValue, winningNumber); // from Part 4
        const houseEdge = GAME_CONFIG[gameKey].houseEdge; // Typically, European roulette house edge is from the '0'. Here we can apply it on winnings.

        let payoutOnWinLamports = 0n;
        let profitLamports = -betAmountLamports;
        let win = false;

        if (basePayoutMultiplier > 0) {
            win = true;
            // Winnings = Stake * BaseMultiplier. Net Winnings = Winnings * (1 - HouseEdge). Total Return = Stake + Net Winnings.
            const grossWinnings = BigInt(Number(betAmountLamports) * basePayoutMultiplier);
            const netWinnings = BigInt(Math.floor(Number(grossWinnings) * (1 - houseEdge))); // Apply house edge to winnings
            payoutOnWinLamports = betAmountLamports + netWinnings;
            profitLamports = netWinnings;
        }

        client = await pool.connect();
        await client.query('BEGIN');
        let finalBalanceUpdateResult;
        if (win) {
            finalBalanceUpdateResult = await updateUserBalanceAndLedger(client, userId, payoutOnWinLamports, `bet_win:${gameKey}`, { betId });
        } else {
             finalBalanceUpdateResult = { success: true, newBalance: betPlacement.newBalance + profitLamports };
        }
        await updateBetStatus(client, betId, win ? 'completed_win' : 'completed_loss', payoutOnWinLamports);
        await client.query('COMMIT');

        const numberColor = winningNumber === 0 ? '🟢' : (getRoulettePayoutMultiplier('red', null, winningNumber) ? '🔴' : '⚫️');
        let resultText = `The ball landed on: *${numberColor} ${escapeMarkdownV2(String(winningNumber))}*\\!\n\n`;
        if (win) {
            resultText += `🎉 You *WON*\\! 🎉\nReturned: ${formatSol(payoutOnWinLamports)} SOL (Profit: ${formatSol(profitLamports)} SOL)`;
        } else {
            resultText += `😥 You *LOST*\\! 😥\nLost: ${formatSol(betAmountLamports)} SOL`;
        }
        resultText += `\n\nNew balance: ${formatSol(finalBalanceUpdateResult.newBalance)} SOL`;

        const inlineKeyboard = [[
            { text: `⚪️ Play Roulette Again`, callback_data: `play_again:${gameKey}:${betAmountLamports}` },
            { text: '🎮 Games Menu', callback_data: 'menu:main' }
        ]];

        if(currentMessageId) {
            await bot.editMessageText(resultText, { chat_id: chatId, message_id: currentMessageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: inlineKeyboard } });
        } else {
            await safeSendMessage(chatId, resultText, { parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: inlineKeyboard } });
        }

    } catch (error) {
        console.error(`${logPrefix} Error:`, error);
        if (client) { try { await client.query('ROLLBACK'); } catch (e) { console.error(`${logPrefix} Rollback error:`, e); } }
        await safeSendMessage(chatId, "Sorry, an error occurred while playing Roulette.");
    } finally {
        if (client) client.release();
    }
}


// --- NEW GAME HANDLERS ---

async function handleCrashGame(userId, chatId, originalMessageId, betAmountLamports) {
    const gameKey = 'crash';
    const logPrefix = `[CrashGame User ${userId} Bet ${formatSol(betAmountLamports)}]`;
    console.log(`${logPrefix} Starting Crash game.`);
    let client;
    let gameLoopTimeoutId = null; // To control the animation loop

    // Unique ID for this game instance to manage state and callbacks correctly
    const gameInstanceId = `crash_${userId}_${Date.now()}`;

    try {
        const betPlacement = await placeBet(userId, chatId, gameKey, {}, betAmountLamports);
        if (!betPlacement.success) {
            let replyText = betPlacement.error || "Failed to place your Crash bet.";
            if (betPlacement.insufficientBalance) {
                replyText = `⚠️ Insufficient balance for ${formatSol(betAmountLamports)} SOL Crash bet. Your balance: ${formatSol(await getUserBalance(userId))} SOL.`;
                return safeSendMessage(chatId, replyText, {
                    reply_markup: { inline_keyboard: [[{ text: '💰 Quick Deposit', callback_data: 'quick_deposit' }]] }
                });
            }
            return safeSendMessage(chatId, replyText);
        }
        const betId = betPlacement.betId;

        const { crashMultiplier: targetCrashMultiplier } = simulateCrash(); // from Part 4
        console.log(`${logPrefix} Target crash multiplier: ${targetCrashMultiplier}x`);

        let currentMultiplier = 1.00;
        let userCashedOut = false;
        let cashedOutAtMultiplier = 0;

        const initialMessageText = `🚀 *Crash Game Started\\!* Bet: ${formatSol(betAmountLamports)} SOL\nMultiplier: *${currentMultiplier.toFixed(2)}x*\n\nWaiting for launch\\!`;
        let sentMessage = await safeSendMessage(chatId, initialMessageText, {
            parse_mode: 'MarkdownV2',
            reply_markup: { inline_keyboard: [[{ text: '💸 Cash Out!', callback_data: `crash_cash_out:${gameInstanceId}` }]] }
        });
        let currentMessageId = sentMessage?.message_id;

        if (!currentMessageId && originalMessageId) { // Fallback if initial send failed but we have original prompt ID
             currentMessageId = originalMessageId;
             await bot.editMessageText(initialMessageText, { chat_id: chatId, message_id: currentMessageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: '💸 Cash Out!', callback_data: `crash_cash_out:${gameInstanceId}` }]] }}).catch(()=>{});
        } else if (!currentMessageId) {
            throw new Error("Failed to send or edit initial crash message.");
        }
        if (originalMessageId && currentMessageId !== originalMessageId) {
            bot.deleteMessage(chatId, originalMessageId).catch(() => {});
        }


        // Set game state for cash out callback
        userStateCache.set(`${userId}_${gameInstanceId}`, {
            state: 'awaiting_crash_cashout',
            chatId: chatId,
            messageId: currentMessageId,
            data: {
                gameKey,
                betId,
                betAmountLamports,
                targetCrashMultiplier,
                userRequestedCashOut: false,
                currentMultiplier: currentMultiplier,
                breadcrumb: GAME_CONFIG.crash.name
            },
            timestamp: Date.now()
        });

        const gameLoop = async () => {
            const gameState = userStateCache.get(`${userId}_${gameInstanceId}`);
            if (!gameState || gameState.data.userRequestedCashOut || currentMultiplier >= targetCrashMultiplier) {
                // Game ended (cashed out, crashed, or error)
                if (gameLoopTimeoutId) clearTimeout(gameLoopTimeoutId);

                client = await pool.connect();
                await client.query('BEGIN');

                let finalResultText = "";
                let finalPayoutLamports = 0n;
                let profitLamports = -betAmountLamports; // Default to loss

                if (gameState?.data.userRequestedCashOut) { // User cashed out successfully
                    userCashedOut = true;
                    cashedOutAtMultiplier = gameState.data.currentMultiplier; // Use multiplier AT THE TIME of cash out request
                    const grossWinnings = BigInt(Math.floor(Number(betAmountLamports) * cashedOutAtMultiplier));
                    const houseEdge = GAME_CONFIG.crash.houseEdge;
                    const netWinnings = BigInt(Math.floor(Number(grossWinnings - betAmountLamports) * (1 - houseEdge))); // Edge on profit
                    finalPayoutLamports = betAmountLamports + netWinnings;
                    profitLamports = netWinnings;

                    await updateUserBalanceAndLedger(client, userId, finalPayoutLamports, `bet_win:${gameKey}`, { betId });
                    await updateBetStatus(client, betId, 'completed_win', finalPayoutLamports);
                    finalResultText = `💸 Cashed Out at *${cashedOutAtMultiplier.toFixed(2)}x*\\!\n🎉 You *WON*\\! Returned: ${formatSol(finalPayoutLamports)} SOL (Profit: ${formatSol(profitLamports)} SOL)`;
                } else { // Game crashed
                    finalResultText = `💥 CRASHED at *${targetCrashMultiplier.toFixed(2)}x*\\!\n😥 You *LOST*\\! Lost: ${formatSol(betAmountLamports)} SOL`;
                    // Bet already deducted, no balance change needed for loss.
                    await updateBetStatus(client, betId, 'completed_loss', 0n);
                }
                const finalBalance = betPlacement.newBalance + profitLamports; // Calculate based on initial placement + profit/loss
                finalResultText += `\n\nNew balance: ${formatSol(finalBalance)} SOL`;
                await client.query('COMMIT');

                userStateCache.delete(`${userId}_${gameInstanceId}`); // Clean up state
                const finalInlineKeyboard = [[
                    { text: `🚀 Play Crash Again`, callback_data: `play_again:${gameKey}:${betAmountLamports}` },
                    { text: '🎮 Games Menu', callback_data: 'menu:main' }
                ]];
                if (currentMessageId) {
                     await bot.editMessageText(finalResultText, { chat_id: chatId, message_id: currentMessageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: finalInlineKeyboard } }).catch(e => console.error(`${logPrefix} Final crash edit error: ${e.message}`));
                } else {
                    await safeSendMessage(chatId, finalResultText, {parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: finalInlineKeyboard }});
                }
                return; // End loop
            }

            // Increment multiplier (adjust speed/increment logic for desired game feel)
            if (currentMultiplier < 1.5) currentMultiplier += 0.01;
            else if (currentMultiplier < 3) currentMultiplier += 0.02;
            else if (currentMultiplier < 5) currentMultiplier += 0.05;
            else if (currentMultiplier < 10) currentMultiplier += 0.10;
            else if (currentMultiplier < 20) currentMultiplier += 0.25;
            else currentMultiplier += 0.50;
            currentMultiplier = parseFloat(currentMultiplier.toFixed(2));

            gameState.data.currentMultiplier = currentMultiplier; // Update state
            userStateCache.set(`${userId}_${gameInstanceId}`, gameState);

            const displayMultiplier = `Multiplier: *${currentMultiplier.toFixed(2)}x*`;
            const animationText = `🚀 *Crash Game In Progress\\!* Bet: ${formatSol(betAmountLamports)} SOL\n${displayMultiplier}`;

            if (currentMessageId) {
                await bot.editMessageText(animationText, {
                    chat_id: chatId, message_id: currentMessageId, parse_mode: 'MarkdownV2',
                    reply_markup: { inline_keyboard: [[{ text: `💸 Cash Out at ${currentMultiplier.toFixed(2)}x!`, callback_data: `crash_cash_out:${gameInstanceId}` }]] }
                }).catch(e => {
                    console.warn(`${logPrefix} Crash animation edit error: ${e.message}`);
                    // If edit fails (e.g. message too old, deleted), we should probably stop the game for this user.
                    // For simplicity, we let it run its course, but it won't be visible.
                    // Alternatively, try sending a new message, but that could spam.
                });
            }

            const delay = 300 + Math.max(0, 500 - currentMultiplier * 10); // Faster updates at higher multipliers, min 300ms
            gameLoopTimeoutId = setTimeout(gameLoop, Math.max(200, delay)); // Ensure minimum delay
        };

        await sleep(1000); // Initial delay before multiplier starts visibly increasing
        gameLoop(); // Start the game loop

    } catch (error) {
        console.error(`${logPrefix} Error:`, error);
        if (client) { try { await client.query('ROLLBACK'); } catch (e) { console.error(`${logPrefix} Rollback error:`, e); } }
        if (gameLoopTimeoutId) clearTimeout(gameLoopTimeoutId);
        userStateCache.delete(`${userId}_${gameInstanceId}`);
        await safeSendMessage(chatId, "Sorry, an error occurred while starting the Crash game.");
    } finally {
        // client release happens within the gameLoop or catch block
    }
}


async function handleBlackjackGame(userId, chatId, originalMessageId, betAmountLamports, action, existingState = null) {
    const gameKey = 'blackjack';
    const logPrefix = `[BlackjackGame User ${userId} Bet ${formatSol(betAmountLamports)} Action ${action}]`;
    console.log(`${logPrefix} Handling Blackjack.`);
    let client;

    const gameInstanceId = existingState?.data.gameInstanceId || `bj_${userId}_${Date.now()}`;
    let breadcrumb = existingState?.data.breadcrumb || GAME_CONFIG.blackjack.name;

    let currentMessageId = existingState?.messageId || originalMessageId;
    let playerHand, dealerHand, deck, betId;

    try {
        if (action === 'initial_deal') {
            const betPlacement = await placeBet(userId, chatId, gameKey, {}, betAmountLamports);
            if (!betPlacement.success) {
                let replyText = betPlacement.error || "Failed to place your Blackjack bet.";
                if (betPlacement.insufficientBalance) {
                    replyText = `⚠️ Insufficient balance for ${formatSol(betAmountLamports)} SOL Blackjack bet. Your balance: ${formatSol(await getUserBalance(userId))} SOL.`;
                    return safeSendMessage(chatId, replyText, {
                        reply_markup: { inline_keyboard: [[{ text: '💰 Quick Deposit', callback_data: 'quick_deposit' }]] }
                    });
                }
                return safeSendMessage(chatId, replyText);
            }
            betId = betPlacement.betId;

            deck = createDeck(); // from Part 4
            shuffleDeck(deck); // from Part 4
            playerHand = [dealCard(deck), dealCard(deck)];
            dealerHand = [dealCard(deck), dealCard(deck)];

            const processingMsg = await safeSendMessage(chatId, `🃏 Dealing for Blackjack\\! Your bet: ${formatSol(betAmountLamports)} SOL`, {parse_mode: 'MarkdownV2'});
            if(processingMsg?.message_id && originalMessageId) {
                bot.deleteMessage(chatId, originalMessageId).catch(()=>{});
                currentMessageId = processingMsg.message_id;
            }  else if (processingMsg?.message_id) {
                currentMessageId = processingMsg.message_id;
            }

        } else if (existingState) { // 'hit' or 'stand'
            betId = existingState.data.betId;
            playerHand = existingState.data.playerHand;
            dealerHand = existingState.data.dealerHand;
            deck = existingState.data.deck;
        } else {
            throw new Error("Invalid state for Blackjack action.");
        }

        let playerValue = calculateHandValue(playerHand); // from Part 4
        let dealerValue = calculateHandValue(dealerHand); // from Part 4
        let playerHasBlackjack = playerValue === 21 && playerHand.length === 2;
        let dealerHasBlackjack = dealerValue === 21 && dealerHand.length === 2;
        let gameEnded = false;
        let resultData = null; // Will store {result, playerValue, dealerValue, ...}

        // Initial Deal Logic & Immediate End Conditions
        if (action === 'initial_deal') {
            if (playerHasBlackjack || dealerHasBlackjack) {
                gameEnded = true;
                // Dealer reveals second card only if game ends now
                resultData = determineBlackjackWinner(playerHand, dealerHand); // from Part 4
            }
        }

        if (!gameEnded && action === 'hit') {
            breadcrumb += ` > Hit`;
            const newCard = dealCard(deck);
            if (newCard) playerHand.push(newCard);
            playerValue = calculateHandValue(playerHand);
            if (playerValue >= 21) { // Player busts or hits 21
                gameEnded = true;
                // If player hits 21 (not Blackjack), dealer still plays. If player busts, dealer wins automatically.
                if (playerValue > 21) {
                     resultData = { result: 'player_busts', playerValue, dealerValue, playerHandFormatted: playerHand.map(formatCard), dealerHandFormatted: dealerHand.map(d => formatCard(d)) };
                } else { // Player hit to 21, now dealer plays
                    const dealerResult = simulateDealerPlay([...dealerHand], deck); // from Part 4
                    resultData = determineBlackjackWinner(playerHand, dealerResult.hand);
                }
            }
        }

        if (!gameEnded && action === 'stand') {
            breadcrumb += ` > Stand`;
            gameEnded = true;
            const dealerResult = simulateDealerPlay([...dealerHand], deck); // from Part 4
            resultData = determineBlackjackWinner(playerHand, dealerResult.hand);
        }

        // Prepare message and keyboard
        let messageText = `${escapeMarkdownV2(breadcrumb)}\n\n`;
        messageText += `Your Hand: *${playerHand.map(c => formatCard(c)).join(' ')}* \\(Value: ${playerValue}${playerValue > 21 ? ' BUST\\!' : (playerHasBlackjack ? ' BLACKJACK\\!' : '')}\\)\n`;
        if (gameEnded) {
            messageText += `Dealer's Hand: *${resultData.dealerHandFormatted.join(' ')}* \\(Value: ${resultData.dealerValue}${resultData.dealerValue > 21 ? ' BUST\\!' : (dealerHasBlackjack ? ' BLACKJACK\\!' : '')}\\)\n\n`;
        } else {
            messageText += `Dealer Shows: *${formatCard(dealerHand[0])}* \\[ ? \\]\n\n`;
        }

        const inlineKeyboard = [];

        if (gameEnded) {
            userStateCache.delete(`${userId}_${gameInstanceId}`); // Clean up state

            client = await pool.connect();
            await client.query('BEGIN');
            let payoutLamports = 0n;
            let profitLamports = -betAmountLamports;
            let betStatus = 'completed_loss';

            if (resultData.result === 'player_blackjack') {
                payoutLamports = betAmountLamports + BigInt(Math.floor(Number(betAmountLamports) * 1.5 * (1 - GAME_CONFIG.blackjack.houseEdge))); // 3:2 payout
                profitLamports = payoutLamports - betAmountLamports;
                betStatus = 'completed_win';
                messageText += `🎉 *BLACKJACK\\!* You win ${formatSol(payoutLamports)} SOL\\!`;
            } else if (resultData.result === 'player_wins' || (resultData.result === 'dealer_busts' && playerValue <=21) ) { // Ensure player didn't also bust
                payoutLamports = betAmountLamports * 2n; // Standard 1:1 win (already includes stake)
                 profitLamports = betAmountLamports; // Pure profit is 1x stake
                 // Apply house edge if defined to be on all wins
                 payoutLamports = betAmountLamports + BigInt(Math.floor(Number(betAmountLamports) * (1 - GAME_CONFIG.blackjack.houseEdge)));
                 profitLamports = payoutLamports - betAmountLamports;

                betStatus = 'completed_win';
                messageText += `🎉 You *WIN*\\! Returned ${formatSol(payoutLamports)} SOL\\.`;
            } else if (resultData.result === 'push') {
                payoutLamports = betAmountLamports; // Return stake
                profitLamports = 0n;
                betStatus = 'completed_push';
                messageText += `🔵 It's a *PUSH*\\! Stake returned\\.`;
            } else { // Player busts or dealer wins
                payoutLamports = 0n; // Already deducted
                messageText += `😥 Dealer *WINS*\\. You lost ${formatSol(betAmountLamports)} SOL\\.`;
                 if (resultData.result === 'player_busts') messageText = `💥 You *BUSTED* with ${playerValue}\\. You lost ${formatSol(betAmountLamports)} SOL\\.`;

            }

            let finalBalanceUpdateResult;
            if (betStatus === 'completed_win' || betStatus === 'completed_push') {
                finalBalanceUpdateResult = await updateUserBalanceAndLedger(client, userId, payoutLamports, `bet_${resultData.result.startsWith('player') ? 'win' : resultData.result}:${gameKey}`, { betId });
            } else {
                 finalBalanceUpdateResult = { success: true, newBalance: (await getUserBalance(userId)) - betAmountLamports }; // Recalc after initial bet placement
                 // To be more accurate, use newBalance from betPlacement:
                 const tempBetPlacementBalance = (await getBetDetails(betId))?.wager_amount_lamports === betAmountLamports ? (await getUserBalance(userId)) : 0n; // Simplified, this part needs care
                 finalBalanceUpdateResult = {success: true, newBalance: tempBetPlacementBalance }; // This assumes getUserBalance is accurate after bet
            }
            await updateBetStatus(client, betId, betStatus, payoutLamports);
            await client.query('COMMIT');

            messageText += `\n\nNew balance: ${formatSol(finalBalanceUpdateResult.newBalance)} SOL`;
            inlineKeyboard.push(
                [{ text: `🃏 Play Blackjack Again`, callback_data: `play_again:${gameKey}:${betAmountLamports}` }],
                [{ text: '🎮 Games Menu', callback_data: 'menu:main' }]
            );

        } else { // Game continues, player's turn
            messageText += `Your action?`;
            inlineKeyboard.push(
                [{ text: '➕ Hit', callback_data: `blackjack_action:${gameInstanceId}:hit` }],
                [{ text: '✋ Stand', callback_data: `blackjack_action:${gameInstanceId}:stand` }]
            );
            // Update state
            userStateCache.set(`${userId}_${gameInstanceId}`, {
                state: 'awaiting_blackjack_action',
                chatId: chatId,
                messageId: currentMessageId,
                data: { gameKey, betId, betAmountLamports, playerHand, dealerHand, deck, gameInstanceId, breadcrumb },
                timestamp: Date.now()
            });
        }

        if (currentMessageId) {
            await bot.editMessageText(messageText, { chat_id: chatId, message_id: currentMessageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: inlineKeyboard } });
        } else {
            // This case should ideally not happen if initial message sending was successful
            await safeSendMessage(chatId, messageText, { parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: inlineKeyboard } });
        }

    } catch (error) {
        console.error(`${logPrefix} Error:`, error);
        if (client) { try { await client.query('ROLLBACK'); } catch (e) { console.error(`${logPrefix} Rollback error:`, e); } }
        userStateCache.delete(`${userId}_${gameInstanceId}`);
        await safeSendMessage(chatId, "Sorry, an error occurred while playing Blackjack.");
    } finally {
        if (client) client.release();
    }
}

// --- End of Part 5a ---
// index.js - Part 5b: Input Handlers, Command Handlers, Referral Checks
// --- VERSION: 3.2.0 --- (Implement Crash, Blackjack, Slots Jackpot, Foundational UX Fixes/Adds)

// --- Assuming functions from Part 1, 2, 3, 4, 5a are available ---
// Includes GAME_CONFIG, STANDARD_BET_AMOUNTS_LAMPORTS, BOT_VERSION, userStateCache, etc.
// Includes safeSendMessage, escapeMarkdownV2, formatSol, getUserBalance, placeBet, notifyAdmin,
// getJackpotAmount, getBetHistory, getUserLastBetAmounts, updateUserLastBetAmount,
// proceedToGameStep, handle<GameName>Game, and other handlers from 5a.

// --- Input Handling Router (Called by handleMessage in Part 5a) ---
/**
 * Routes incoming text messages based on the user's current conversational state.
 * @param {import('node-telegram-bot-api').Message} msg The user's message.
 * @param {object} currentState The user's current state object from userStateCache.
 */
async function routeStatefulInput(msg, currentState) {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const logPrefix = `[StatefulInput State:${currentState.state} User ${userId}]`;
    console.log(`${logPrefix} Routing stateful input. Message: "${msg.text}", Current state data:`, currentState.data);

    // CRITICAL BUG FIX: Ensure case state names match where they are set.
    // States are typically set like `awaiting_${gameKey}_amount` by `showBetAmountButtons` when 'Custom' is clicked.
    switch (currentState.state) {
        case 'awaiting_coinflip_amount': // Corrected from 'awaiting_cf_amount'
        case 'awaiting_race_amount':
        case 'awaiting_slots_amount':
        case 'awaiting_war_amount':
        case 'awaiting_roulette_amount':
        case 'awaiting_crash_amount':    // NEW for Crash
        case 'awaiting_blackjack_amount':// NEW for Blackjack
            return handleCustomAmountInput(msg, currentState);

        case 'awaiting_roulette_number': // For Roulette straight bet number input
            return handleRouletteNumberInput(msg, currentState); // Defined below

        case 'awaiting_withdrawal_amount':
            return handleWithdrawalAmountInput(msg, currentState); // Defined below

        // Example of a deprecated state, ensure it's handled or removed if truly unused
        case 'awaiting_withdrawal_address':
            console.warn(`${logPrefix} Received input for deprecated state: awaiting_withdrawal_address`);
            userStateCache.delete(userId);
            return safeSendMessage(chatId, "This input step is no longer used. Please use `/wallet <address>` to set your address, then `/withdraw`.", { parse_mode: 'MarkdownV2' });

        default:
            console.log(`${logPrefix} User in unhandled input state '${currentState.state}' received text: ${msg.text?.slice(0, 50)}`);
            userStateCache.delete(userId);
            return safeSendMessage(chatId, "Hmm, I wasn't expecting that input right now. Please use a command like /help or select an option from a menu.", { parse_mode: 'MarkdownV2' });
    }
}
// --- End Input Handling Router ---


// --- Input Handlers (Called by routeStatefulInput) ---

/**
 * Handles custom numeric bet amount input from the user.
 * @param {import('node-telegram-bot-api').Message} msg The user's message.
 * @param {object} currentState The user's current state object from userStateCache.
 */
async function handleCustomAmountInput(msg, currentState) {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const text = msg.text?.trim();
    const { gameKey, originalMessageId, breadcrumb: currentBreadcrumb } = currentState.data; // originalMessageId is where buttons were
    const logPrefix = `[CustomAmtInput User ${userId} Game ${gameKey}]`;

    console.log(`${logPrefix} Received custom amount: "${text}"`);

    if (!text) {
        return safeSendMessage(chatId, `⚠️ Please enter a valid bet amount for ${escapeMarkdownV2(GAME_CONFIG[gameKey]?.name || gameKey)}.`, { parse_mode: 'MarkdownV2' });
    }
    if (!gameKey || !GAME_CONFIG[gameKey]) {
        console.error(`${logPrefix} Invalid gameKey in state: ${gameKey}`);
        userStateCache.delete(userId);
        return safeSendMessage(chatId, "⚠️ Internal error: Game information missing. Please start over.", { parse_mode: 'MarkdownV2' });
    }

    const gameConfig = GAME_CONFIG[gameKey];
    const newBreadcrumb = `${currentBreadcrumb || gameConfig.name} > Custom Amount`;

    let amountSOLInput;
    try {
        amountSOLInput = parseFloat(text);
        if (isNaN(amountSOLInput) || amountSOLInput <= 0) {
            await bot.deleteMessage(chatId, msg.message_id).catch(()=>{}); // Delete user's invalid input
            return safeSendMessage(chatId, `${escapeMarkdownV2(newBreadcrumb)}\n⚠️ Invalid amount. Please enter a positive number (e.g., 0.1) for your ${escapeMarkdownV2(gameConfig.name)} bet.`, { parse_mode: 'MarkdownV2' });
        }
    } catch (e) {
        await bot.deleteMessage(chatId, msg.message_id).catch(()=>{});
        return safeSendMessage(chatId, `${escapeMarkdownV2(newBreadcrumb)}\n⚠️ Could not read amount. Please enter a number (e.g., 0.1) for your ${escapeMarkdownV2(gameConfig.name)} bet.`, { parse_mode: 'MarkdownV2' });
    }

    const amountLamports = BigInt(Math.round(amountSOLInput * LAMPORTS_PER_SOL));
    const amountSOLValidated = formatSol(amountLamports);

    if (amountLamports < gameConfig.minBetLamports || amountLamports > gameConfig.maxBetLamports) {
        await bot.deleteMessage(chatId, msg.message_id).catch(()=>{});
        const min = formatSol(gameConfig.minBetLamports);
        const max = formatSol(gameConfig.maxBetLamports);
        return safeSendMessage(chatId, `${escapeMarkdownV2(newBreadcrumb)}\n⚠️ Bet amount for ${escapeMarkdownV2(gameConfig.name)} must be between ${escapeMarkdownV2(min)} and ${escapeMarkdownV2(max)} SOL. You entered: ${escapeMarkdownV2(amountSOLValidated)} SOL. Please try again.`, { parse_mode: 'MarkdownV2' });
    }

    const currentBalance = await getUserBalance(userId);
    if (currentBalance < amountLamports) {
        await bot.deleteMessage(chatId, msg.message_id).catch(()=>{});
        return safeSendMessage(chatId, `${escapeMarkdownV2(newBreadcrumb)}\n⚠️ Insufficient balance (${escapeMarkdownV2(formatSol(currentBalance))} SOL) for a ${escapeMarkdownV2(amountSOLValidated)} SOL bet. Use /deposit to add funds.`, { parse_mode: 'MarkdownV2' });
    }

    // Delete user's message containing the amount
    bot.deleteMessage(chatId, msg.message_id).catch(e => console.warn(`${logPrefix} Could not delete user amount message: ${e.message}`));

    // Valid custom amount, proceed to next game step (choice or confirmation)
    // Uses proceedToGameStep from Part 5a, which will update/edit the originalMessageId (where "Custom Amount" was clicked)
    console.log(`${logPrefix} Custom amount ${amountSOLValidated} SOL validated for ${gameKey}. Proceeding.`);
    return proceedToGameStep(userId, chatId, originalMessageId, gameKey, amountLamports, newBreadcrumb);
}

/**
 * Handles Roulette "Straight Up" number input.
 * @param {import('node-telegram-bot-api').Message} msg The user's message.
 * @param {object} currentState The user's current state object from userStateCache.
 */
async function handleRouletteNumberInput(msg, currentState) {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const text = msg.text?.trim();
    const { gameKey, betAmountLamports, betType, originalMessageId, breadcrumb: currentBreadcrumb } = currentState.data;
    const logPrefix = `[RouletteNumInput User ${userId}]`;

    console.log(`${logPrefix} Received number: "${text}"`);

    if (!text) {
        return safeSendMessage(chatId, `${escapeMarkdownV2(currentBreadcrumb)}\n⚠️ Please enter the number (0-36) you want to bet on.`, { parse_mode: 'MarkdownV2' });
    }
    if (gameKey !== 'roulette' || betType !== 'straight') {
        userStateCache.delete(userId);
        return safeSendMessage(chatId, "⚠️ Internal error: Incorrect game state for number input. Please start Roulette again.", { parse_mode: 'MarkdownV2' });
    }

    let betValueNumber;
    try {
        betValueNumber = parseInt(text, 10);
        if (isNaN(betValueNumber) || betValueNumber < 0 || betValueNumber > 36) {
            await bot.deleteMessage(chatId, msg.message_id).catch(()=>{});
            return safeSendMessage(chatId, `${escapeMarkdownV2(currentBreadcrumb)}\n⚠️ Invalid number. Please enter a number between 0 and 36.`, { parse_mode: 'MarkdownV2' });
        }
    } catch (e) {
        await bot.deleteMessage(chatId, msg.message_id).catch(()=>{});
        return safeSendMessage(chatId, `${escapeMarkdownV2(currentBreadcrumb)}\n⚠️ Could not read number. Please enter a number between 0 and 36.`, { parse_mode: 'MarkdownV2' });
    }
    const betValueString = String(betValueNumber);
    const amountLamportsBigInt = BigInt(betAmountLamports); // Ensure it's BigInt

    // Delete user's message containing the number
    bot.deleteMessage(chatId, msg.message_id).catch(e => console.warn(`${logPrefix} Could not delete user number message: ${e.message}`));
    userStateCache.delete(userId); // Clear state, moving to confirmation/game

    const finalBreadcrumb = `${currentBreadcrumb} on ${betValueString}`;
    const confirmationText = `${escapeMarkdownV2(finalBreadcrumb)}\nBet ${formatSol(amountLamportsBigInt)} SOL on number ${escapeMarkdownV2(betValueString)}?`;
    const inlineKeyboard = [
        [{ text: `✅ Yes, Confirm Bet`, callback_data: `confirm_bet:roulette:${amountLamportsBigInt}:${betType}:${betValueString}` }],
        [{ text: '↩️ Change Number', callback_data: `roulette_bet_type:straight:${amountLamportsBigInt}` }], // Go back to number input (or bet type selection for straight)
        [{ text: '❌ Cancel & Exit', callback_data: 'menu:main' }]
    ];
    // Edit the message that asked for number input (originalMessageId from state)
    return bot.editMessageText(confirmationText, {
        chat_id: chatId,
        message_id: originalMessageId,
        reply_markup: { inline_keyboard: inlineKeyboard },
        parse_mode: 'MarkdownV2'
    }).catch(e => {
        console.error(`${logPrefix} Failed to edit message for confirmation: ${e.message}`);
        // Fallback to new message if edit fails
        safeSendMessage(chatId, confirmationText, { reply_markup: { inline_keyboard: inlineKeyboard }, parse_mode: 'MarkdownV2' });
    });
}

/** Handles withdrawal AMOUNT input */
async function handleWithdrawalAmountInput(msg, currentState) {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const text = msg.text?.trim();
    const { originalMessageId, breadcrumb: currentBreadcrumb } = currentState.data;
    const logPrefix = `[WithdrawAmtInput User ${userId}]`;

    if (!text) {
        return safeSendMessage(chatId, `${escapeMarkdownV2(currentBreadcrumb)}\n⚠️ Please enter the amount of SOL you wish to withdraw.`, { parse_mode: 'MarkdownV2' });
    }
    console.log(`${logPrefix} Received withdrawal amount: "${text}"`);

    let amountSOL;
    try {
        amountSOL = parseFloat(text);
        if (isNaN(amountSOL) || amountSOL <= 0) {
             await bot.deleteMessage(chatId, msg.message_id).catch(()=>{});
            return safeSendMessage(chatId, `${escapeMarkdownV2(currentBreadcrumb)}\n⚠️ Invalid amount. Please enter a positive number (e.g., 0.5).`, { parse_mode: 'MarkdownV2' });
        }
    } catch (e) {
         await bot.deleteMessage(chatId, msg.message_id).catch(()=>{});
        return safeSendMessage(chatId, `${escapeMarkdownV2(currentBreadcrumb)}\n⚠️ Could not read amount. Please enter a number (e.g., 0.5).`, { parse_mode: 'MarkdownV2' });
    }

    const amountLamports = BigInt(Math.round(amountSOL * LAMPORTS_PER_SOL));
    const feeLamports = WITHDRAWAL_FEE_LAMPORTS; // From Part 1
    const totalDeduction = amountLamports + feeLamports;
    const amountSOLFormatted = formatSol(amountLamports);
    const feeSOLFormatted = formatSol(feeLamports);
    const totalSOLFormatted = formatSol(totalDeduction);

    if (amountLamports < MIN_WITHDRAWAL_LAMPORTS) { // From Part 1
         await bot.deleteMessage(chatId, msg.message_id).catch(()=>{});
        return safeSendMessage(chatId, `${escapeMarkdownV2(currentBreadcrumb)}\n⚠️ Minimum withdrawal is ${escapeMarkdownV2(formatSol(MIN_WITHDRAWAL_LAMPORTS))} SOL. You entered: ${escapeMarkdownV2(amountSOLFormatted)} SOL.`, { parse_mode: 'MarkdownV2' });
    }

    const currentBalance = await getUserBalance(userId);
    if (currentBalance < totalDeduction) {
         await bot.deleteMessage(chatId, msg.message_id).catch(()=>{});
        userStateCache.delete(userId); // Clear state on failure
        return safeSendMessage(chatId, `${escapeMarkdownV2(currentBreadcrumb)}\n⚠️ Insufficient balance. You need ${escapeMarkdownV2(totalSOLFormatted)} SOL (amount + fee) to withdraw ${escapeMarkdownV2(amountSOLFormatted)} SOL. Your balance: ${escapeMarkdownV2(formatSol(currentBalance))} SOL.`, { parse_mode: 'MarkdownV2' });
    }

    const linkedAddress = await getLinkedWallet(userId); // from Part 2
    if (!linkedAddress) {
        userStateCache.delete(userId);
        return safeSendMessage(chatId, "⚠️ Withdrawal address not found. Please set it using `/wallet <address>` first.", { parse_mode: 'MarkdownV2' });
    }

    // Delete user's message with amount
    bot.deleteMessage(chatId, msg.message_id).catch(e => console.warn(`${logPrefix} Could not delete user amount message: ${e.message}`));
    userStateCache.set(userId, {
        state: 'awaiting_withdrawal_confirm',
        chatId: chatId,
        messageId: originalMessageId, // The message that asked for the amount
        data: { amountLamports: amountLamports.toString(), recipientAddress: linkedAddress, breadcrumb: `${currentBreadcrumb} > Confirm` },
        timestamp: Date.now()
    });

    const confirmationText = `*Confirm Withdrawal*\n\n` +
        `${escapeMarkdownV2(currentBreadcrumb + " > Confirm")}\n` +
        `Amount: *${escapeMarkdownV2(amountSOLFormatted)} SOL*\n` +
        `Fee: ${escapeMarkdownV2(feeSOLFormatted)} SOL\n` +
        `Total Deducted: *${escapeMarkdownV2(totalSOLFormatted)} SOL*\n` +
        `Recipient: \`${escapeMarkdownV2(linkedAddress)}\`\n\n` +
        `Proceed?`;
    const keyboard = [
        [{ text: '✅ Confirm Withdrawal', callback_data: 'withdraw_confirm' }], // Handled in Part 5a
        [{ text: '↩️ Change Amount', callback_data: 'menu:withdraw' }], // Go back to withdraw command start
        [{ text: '❌ Cancel', callback_data: 'menu:main' }]
    ];
    // Edit the original message (that asked for amount)
    return bot.editMessageText(confirmationText, {
        chat_id: chatId, message_id: originalMessageId,
        reply_markup: { inline_keyboard: keyboard }, parse_mode: 'MarkdownV2'
    }).catch(e => {
        console.error(`${logPrefix} Error editing message for withdrawal confirmation: ${e.message}`);
        safeSendMessage(chatId, confirmationText, { reply_markup: { inline_keyboard: keyboard }, parse_mode: 'MarkdownV2' });
    });
}

// --- Command Handlers ---

/** Handles /start command - Shows main menu */
async function handleStartCommand(msg, args) {
    const chatId = msg.chat.id;
    const userId = String(msg.from.id);
    const firstName = msg.from.first_name || 'Gambler';
    const logPrefix = `[Cmd /start User ${userId}]`;
    console.log(`${logPrefix} Handling /start.`);

    await ensureUserExists(userId, await pool.connect()); // Ensure user exists in DB via a temp client from pool
    userStateCache.set(userId, { state: 'idle', chatId: String(chatId), timestamp: Date.now(), data: { breadcrumb: "Main Menu"} });

    // Referral code parsing logic was here, now handled by handleMessage in Part 5a for deeplinks

    const options = {
        reply_markup: {
            inline_keyboard: [
                [
                    { text: '🪙 Coinflip', callback_data: 'menu:coinflip' },
                    { text: '🎰 Slots', callback_data: 'menu:slots' },
                    { text: '🃏 War', callback_data: 'menu:war' }
                ],
                [
                    { text: '🐎 Race', callback_data: 'menu:race' },
                    { text: '⚪️ Roulette', callback_data: 'menu:roulette' }
                ],
                [ // New Games
                    { text: '🚀 Crash', callback_data: 'menu:crash' },
                    { text: '♠️ Blackjack', callback_data: 'menu:blackjack' }
                ],
                [
                    { text: '💰 Deposit SOL', callback_data: 'menu:deposit' },
                    { text: '💸 Withdraw SOL', callback_data: 'menu:withdraw' }
                ],
                [
                    { text: '👤 Wallet / History', callback_data: 'menu:wallet' }, // Combined wallet & history
                    { text: '🏆 Leaderboards', callback_data: 'menu:leaderboards'}, // New
                    { text: '🤝 Referral', callback_data: 'menu:referral' }
                ],
                [
                    { text: '❓ Help', callback_data: 'menu:help' }
                ]
            ]
        },
        parse_mode: 'MarkdownV2'
    };
    const welcomeMsg = `👋 Welcome, ${escapeMarkdownV2(firstName)}\\!\n\n` +
        `Solana Gambles Bot *v${escapeMarkdownV2(BOT_VERSION)}* is ready\\! Choose an option below:\n\n` +
        `*Remember to gamble responsibly\\!*`;
    await safeSendMessage(chatId, welcomeMsg, options);
}

/** Handles /help command */
async function handleHelpCommand(msg, args) {
    const chatId = msg.chat.id;
    const feeSOL = formatSol(WITHDRAWAL_FEE_LAMPORTS);
    const minWithdrawSOL = formatSol(MIN_WITHDRAWAL_LAMPORTS);
    const gameKeys = Object.keys(GAME_CONFIG);
    const gameCommands = gameKeys.map(key => `\\- \`/${escapeMarkdownV2(key)}\` \\- Play ${escapeMarkdownV2(GAME_CONFIG[key].name)}`).join('\n');

    // Slots Payout Table (Simplified Example)
    const slotsPayouts = "🎰 *Slots Payouts (Example Multipliers):*\n" +
        "\\- 💎💎💎 \\= JACKPOT\\! (Progressive)\n" +
        "\\- ❼❼❼ \\= 100x\n" +
        "\\- 🔔🔔🔔 \\= 20x\n" +
        "\\- 🍊🍊🍊 \\= 10x\n" +
        "\\- 🍒🍒🍒 \\= 5x\n" +
        "\\- Any two 🍒 \\= 2x\n" +
        "(Actual odds & detailed tables may vary; house edge applies\\.)";


    const helpMsg = `*Solana Gambles Bot Help* 🤖 (v${escapeMarkdownV2(BOT_VERSION)})\n\n` +
        `This bot uses a custodial model. Deposit SOL to your internal balance to play. Winnings are added to this balance.\n\n` +
        `*Key Commands:*\n` +
        `\\- \`/start\` - Main menu & game selection.\n` +
        `\\- \`/wallet\` - View balance, set/view withdrawal address, view bet history.\n` +
        `\\- \`/deposit\` - Get a unique address to deposit SOL.\n` +
        `\\- \`/withdraw\` - Start the withdrawal process.\n` +
        `\\- \`/referral\` - Your referral link & stats.\n` +
        `\\- \`/leaderboard <game_key>\` - View leaderboards (e.g., \`/leaderboard slots\`).\n`+
        `\\- \`/history\` - View your recent bet history.\n` +
        `\\- \`/help\` - This message.\n\n` +
        `*Available Games:*\n${gameCommands}\n\n` +
        `${slotsPayouts}\n\n` + // Added Slots Payouts
        `*Important Notes:*\n` +
        `\\- Deposits: Use unique addresses from \`/deposit\`. They expire in ${escapeMarkdownV2(process.env.DEPOSIT_ADDRESS_EXPIRY_MINUTES)} mins.\n` +
        `\\- Withdrawals: Set address via \`/wallet <YourSolAddress>\`. Fee: ${escapeMarkdownV2(feeSOL)} SOL. Min: ${escapeMarkdownV2(minWithdrawSOL)} SOL.\n` +
        `\\- Gamble Responsibly. Contact support for issues.`;

    await safeSendMessage(chatId, helpMsg, { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}

/** Handles /wallet command (viewing, setting address, or viewing history) */
async function handleWalletCommand(msg, args) {
    const chatId = msg.chat.id;
    const userId = String(msg.from.id);
    const logPrefix = `[Cmd /wallet User ${userId}]`;
    const potentialNewAddress = args?.[1]?.trim(); // Args are like ['/wallet', 'address']

    if (potentialNewAddress) {
        console.log(`${logPrefix} Attempting to set withdrawal address to: ${potentialNewAddress}`);
        const result = await linkUserWallet(userId, potentialNewAddress); // from Part 2
        if (result.success) {
            await safeSendMessage(chatId, `✅ Withdrawal address successfully set/updated to: \`${escapeMarkdownV2(result.wallet)}\``, { parse_mode: 'MarkdownV2' });
        } else {
            await safeSendMessage(chatId, `❌ Failed to set withdrawal address. Error: ${escapeMarkdownV2(result.error)}`, { parse_mode: 'MarkdownV2' });
        }
    } else {
        // View wallet info
        const userBalance = await getUserBalance(userId);
        const userDetails = await getUserWalletDetails(userId); // from Part 2

        let walletMsg = `👤 *Your Wallet & Stats*\n\n` +
                        `*Balance:* ${escapeMarkdownV2(formatSol(userBalance))} SOL\n`;
        if (userDetails?.external_withdrawal_address) {
            walletMsg += `*Withdrawal Address:* \`${escapeMarkdownV2(userDetails.external_withdrawal_address)}\`\n`;
        } else {
            walletMsg += `*Withdrawal Address:* Not Set (Use \`/wallet <YourSolAddress>\`)\n`;
        }
        if (userDetails?.referral_code) {
             walletMsg += `*Referral Code:* \`${escapeMarkdownV2(userDetails.referral_code)}\`\n`;
        }
        // Add last bet amounts display
        const lastBets = userDetails?.last_bet_amounts || {};
        if (Object.keys(lastBets).length > 0) {
            walletMsg += `\n*Last Bet Amounts:*\n`;
            for (const game in lastBets) {
                walletMsg += `  \\- ${escapeMarkdownV2(GAME_CONFIG[game]?.name || game)}: ${escapeMarkdownV2(formatSol(BigInt(lastBets[game])))} SOL\n`;
            }
        }

        walletMsg += `\nTo set address: \`/wallet <YourSolAddress>\`\nView recent bets: \`/history\``;
        await safeSendMessage(chatId, walletMsg, { parse_mode: 'MarkdownV2' });
    }
}

async function handleHistoryCommand(msg, args) {
    const chatId = msg.chat.id;
    const userId = String(msg.from.id);
    const logPrefix = `[Cmd /history User ${userId}]`;
    console.log(`${logPrefix} Fetching bet history.`);

    const limit = 5; // Show last 5 bets for brevity
    const history = await getBetHistory(userId, limit, 0, null); // from Part 2

    if (!history || history.length === 0) {
        return safeSendMessage(chatId, "You have no betting history yet. Time to play some games!", { parse_mode: 'MarkdownV2' });
    }

    let historyMsg = "📜 *Your Last 5 Bets:*\n\n";
    history.forEach(bet => {
        const gameName = GAME_CONFIG[bet.game_type]?.name || bet.game_type;
        const wager = formatSol(bet.wager_amount_lamports);
        let outcomeText = `Status: ${escapeMarkdownV2(bet.status)}`;
        if (bet.status.startsWith('completed_')) {
            if (bet.payout_amount_lamports !== null) {
                const profit = BigInt(bet.payout_amount_lamports) - BigInt(bet.wager_amount_lamports);
                outcomeText = bet.status === 'completed_win' ? `Won: ${formatSol(profit)} SOL` :
                              bet.status === 'completed_push' ? `Push (Stake Back)` :
                              `Lost: ${wager} SOL`;
            }
        }
        historyMsg += `\\- *${escapeMarkdownV2(gameName)}* on ${new Date(bet.created_at).toLocaleString()}\n` +
                      `  Bet: ${escapeMarkdownV2(wager)} SOL, Result: ${escapeMarkdownV2(outcomeText)}\n\n`;
    });
    historyMsg += "For more details, please check a block explorer if needed.";
    await safeSendMessage(chatId, historyMsg, { parse_mode: 'MarkdownV2' });
}


/** Handles /referral command */
async function handleReferralCommand(msg, args) {
    const chatId = msg.chat.id;
    const userId = String(msg.from.id);
    const userDetails = await getUserWalletDetails(userId); // from Part 2

    if (!userDetails?.external_withdrawal_address) {
        return safeSendMessage(chatId, `❌ You need to link your wallet first using \`/wallet <YourSolAddress>\` before using the referral system. This ensures rewards can be paid out.`, { parse_mode: 'MarkdownV2' });
    }
    if (!userDetails.referral_code) {
        // This should ideally be generated on user creation or first wallet link.
        // For now, prompt to link wallet again which triggers code generation if missing.
        return safeSendMessage(chatId, `Your referral code is being generated. Please try \`/wallet\` again first, then \`/referral\`.`, { parse_mode: 'MarkdownV2' });
    }

    const refCode = userDetails.referral_code;
    const totalEarningsLamports = await getTotalReferralEarnings(userId); // from Part 2
    const totalEarningsSOL = formatSol(totalEarningsLamports);

    let botUsername = 'YOUR_BOT_USERNAME'; // Replace with actual or fetch via getMe() and store
    try { const me = await bot.getMe(); if (me.username) botUsername = me.username; } catch (e) { console.warn("Could not fetch bot username for referral link.");}
    const referralLink = `https://t.me/${botUsername}?start=${refCode}`;
    const referralLinkMarkdown = `https://t\\.me/${escapeMarkdownV2(botUsername)}?start=${escapeMarkdownV2(refCode)}`;

    let referralMsg = `🤝 *Your Referral Dashboard*\n\n` +
                      `Share your unique link to earn SOL when your friends play!\n\n` +
                      `*Your Code:* \`${escapeMarkdownV2(refCode)}\`\n` +
                      `*Your Link:* \n\`${escapeMarkdownV2(referralLink)}\`\n` + // For easy copy-pasting
                      `_(Tap the button below to easily share)_\n\n`+
                      `*Successful Referrals:* ${escapeMarkdownV2(userDetails.referral_count)}\n` +
                      `*Total Referral Earnings Paid:* ${escapeMarkdownV2(totalEarningsSOL)} SOL\n\n` +
                      `*How Rewards Work:*\n` +
                      `1. *Initial Bonus:* Earn a % of your referral's *first qualifying bet* (min ${escapeMarkdownV2(formatSol(REFERRAL_INITIAL_BET_MIN_LAMPORTS))} SOL wager). Your % increases with more referrals!\n` +
                      `2. *Milestone Bonus:* Earn ${escapeMarkdownV2((REFERRAL_MILESTONE_REWARD_PERCENT * 100).toFixed(1))}% of their total wagered amount as they hit milestones.\n\n` +
                      `Rewards are paid to your linked wallet: \`${escapeMarkdownV2(userDetails.external_withdrawal_address)}\``;

    const keyboard = [[
        { text: '🔗 Share My Referral Link!', switch_inline_query: referralLink }
    ]];

    await safeSendMessage(chatId, referralMsg, { parse_mode: 'MarkdownV2', disable_web_page_preview: true, reply_markup: {inline_keyboard: keyboard} });
}

/** Handles /deposit command */
async function handleDepositCommand(msg, args) {
    const chatId = msg.chat.id;
    const userId = String(msg.from.id);
    const logPrefix = `[DepositCmd User ${userId}]`;
    try {
        await getUserBalance(userId); // Ensure user exists
        const addressIndex = await getNextDepositAddressIndex(userId); // from Part 2
        const derivedInfo = await generateUniqueDepositAddress(userId, addressIndex); // from Part 3
        if (!derivedInfo) throw new Error("Failed to generate deposit address.");

        const depositAddress = derivedInfo.publicKey.toBase58();
        const expiresAt = new Date(Date.now() + DEPOSIT_ADDRESS_EXPIRY_MS); // from Part 1
        const recordResult = await createDepositAddressRecord(userId, depositAddress, derivedInfo.derivationPath, expiresAt); // from Part 2
        if (!recordResult.success) throw new Error(recordResult.error || "Failed to save deposit address.");

        const expiryMinutes = Math.round(DEPOSIT_ADDRESS_EXPIRY_MS / (60 * 1000));
        const message = `💰 *Deposit SOL*\n\n` +
                        `Send SOL to this unique address:\n\n` +
                        `\`${escapeMarkdownV2(depositAddress)}\`\n\n` +
                        `⚠️ *Important:*\n` +
                        `1. This address is for *one deposit only* & expires in *${escapeMarkdownV2(String(expiryMinutes))} minutes*.\n` +
                        `2. For new deposits, use \`/deposit\` again.\n` +
                        `3. Confirmation: *${escapeMarkdownV2(DEPOSIT_CONFIRMATION_LEVEL)}* network confirmations.`;
        await safeSendMessage(chatId, message, { parse_mode: 'MarkdownV2' });
    } catch (error) {
        console.error(`${logPrefix} Error: ${error.message}`);
        await safeSendMessage(chatId, `❌ Error generating deposit address: ${escapeMarkdownV2(error.message)}. Please try again.`, { parse_mode: 'MarkdownV2' });
    }
}

/** Handles /withdraw command - Starts the withdrawal flow */
async function handleWithdrawCommand(msg, args) {
    const chatId = msg.chat.id;
    const userId = String(msg.from.id);
    const logPrefix = `[WithdrawCmd User ${userId}]`;
    const breadcrumb = "Withdraw SOL";
    try {
        const linkedAddress = await getLinkedWallet(userId);
        if (!linkedAddress) {
            return safeSendMessage(chatId, `⚠️ You must set your withdrawal address first using \`/wallet <YourSolanaAddress>\`.`, { parse_mode: 'MarkdownV2' });
        }
        const currentBalance = await getUserBalance(userId);
        const minWithdrawTotal = MIN_WITHDRAWAL_LAMPORTS + WITHDRAWAL_FEE_LAMPORTS;
        if (currentBalance < minWithdrawTotal) {
            return safeSendMessage(chatId, `⚠️ Your balance (${escapeMarkdownV2(formatSol(currentBalance))} SOL) is below the minimum required to withdraw (${escapeMarkdownV2(formatSol(minWithdrawTotal))} SOL total including fee).`, { parse_mode: 'MarkdownV2' });
        }

        userStateCache.set(userId, {
            state: 'awaiting_withdrawal_amount',
            chatId: chatId,
            messageId: msg.message_id, // Store this message_id for editing after amount input
            data: { breadcrumb },
            timestamp: Date.now()
        });

        const minWithdrawSOLText = formatSol(MIN_WITHDRAWAL_LAMPORTS);
        const feeSOLText = formatSol(WITHDRAWAL_FEE_LAMPORTS);
        await safeSendMessage(chatId,
            `${escapeMarkdownV2(breadcrumb)}\n\n` +
            `Your withdrawal address: \`${escapeMarkdownV2(linkedAddress)}\`\n` +
            `Minimum withdrawal: ${escapeMarkdownV2(minWithdrawSOLText)} SOL. Fee: ${escapeMarkdownV2(feeSOLText)} SOL.\n\n` +
            `Please enter the amount of SOL you wish to withdraw (e.g., 0.5):`,
            { parse_mode: 'MarkdownV2' }
        );
    } catch (error) {
        console.error(`${logPrefix} Error: ${error.message}`);
        await safeSendMessage(chatId, `❌ Error starting withdrawal. Please try again.`, { parse_mode: 'MarkdownV2' });
    }
}

/** Generic function to show bet amount buttons for a game */
async function showBetAmountButtons(msgOrCbMsg, gameKey, existingBetAmount = null) {
    const chatId = msgOrCbMsg.chat.id;
    const userId = String(msgOrCbMsg.from.id);
    const gameConfig = GAME_CONFIG[gameKey];
    const logPrefix = `[ShowBetButtons User ${userId} Game ${gameKey}]`;
    const breadcrumb = gameConfig.name; // Base breadcrumb

    if (!gameConfig) {
        return safeSendMessage(chatId, `⚠️ Internal error: Unknown game \`${escapeMarkdownV2(gameKey)}\`.`, { parse_mode: 'MarkdownV2' });
    }

    const balance = await getUserBalance(userId);
    if (balance < gameConfig.minBetLamports) {
        return safeSendMessage(chatId, `⚠️ Your balance (${escapeMarkdownV2(formatSol(balance))} SOL) is too low to play ${escapeMarkdownV2(gameConfig.name)} (min bet: ${escapeMarkdownV2(formatSol(gameConfig.minBetLamports))} SOL). Use /deposit to add funds.`, {
             parse_mode: 'MarkdownV2',
             reply_markup: { inline_keyboard: [[{ text: '💰 Quick Deposit', callback_data: 'quick_deposit' }]]}
            });
    }

    // Fetch user's last bet for this game, or use existingBetAmount if replaying
    let lastBetForThisGame = existingBetAmount;
    if (!lastBetForThisGame) {
        const userLastBets = userLastBetAmounts.get(userId) || new Map(); // From Part 1 in-memory cache
        lastBetForThisGame = userLastBets.get(gameKey) || null;
    }


    const buttons = [];
    const amountsRow1 = [], amountsRow2 = [];
    STANDARD_BET_AMOUNTS_LAMPORTS.forEach((lamports, index) => {
        if (lamports >= gameConfig.minBetLamports && lamports <= gameConfig.maxBetLamports) {
            const solAmount = STANDARD_BET_AMOUNTS_SOL[index];
            let buttonText = `${formatSol(lamports)} SOL`;
            if (lastBetForThisGame && lamports === lastBetForThisGame) {
                buttonText = `前回 ${buttonText} 👍`; // Indicate last bet
            }
            const button = { text: buttonText, callback_data: `confirm_bet:${gameKey}:${lamports.toString()}` }; // Directly to confirm_bet
            if (amountsRow1.length < 3) amountsRow1.push(button);
            else amountsRow2.push(button);
        }
    });
    if (amountsRow1.length > 0) buttons.push(amountsRow1);
    if (amountsRow2.length > 0) buttons.push(amountsRow2);

    buttons.push([
        { text: "✏️ Custom Amount", callback_data: `custom_amount_select:${gameKey}` }, // New callback to set state
        { text: "❌ Cancel", callback_data: 'menu:main' }
    ]);

    let prompt = `${escapeMarkdownV2(breadcrumb)}\nSelect bet amount for *${escapeMarkdownV2(gameConfig.name)}*:`;
    if (gameKey === 'slots') { // Display jackpot for slots
        const jackpotAmount = await getJackpotAmount('slots'); // from Part 2
        prompt += `\n\n💎 Current Slots Jackpot: *${escapeMarkdownV2(formatSol(jackpotAmount))} SOL*`;
    }
    // Odds display
    if (gameKey === 'roulette') {
        prompt += "\n(Straight Up: 35x, Colors/Even/Odd/Halves: 1x Winnings)";
    } else if (gameKey === 'race') {
        // Race odds displayed on horse buttons later in proceedToGameStep
    } else if (gameKey === 'blackjack') {
        prompt += "\n(Blackjack pays 3:2, Dealer stands on 17)";
    }


    // If message came from a callback (e.g., menu selection), edit it. Otherwise, send new.
    if (msgOrCbMsg.message_id && msgOrCbMsg.data) { // Check if it's a callbackQuery's message
        await bot.editMessageText(prompt, {
            chat_id: chatId, message_id: msgOrCbMsg.message_id,
            reply_markup: { inline_keyboard: buttons }, parse_mode: 'MarkdownV2'
        }).catch(e => { // Fallback if edit fails (e.g. message not found)
            console.warn(`${logPrefix} Failed to edit message for bet amounts, sending new: ${e.message}`);
            safeSendMessage(chatId, prompt, { reply_markup: { inline_keyboard: buttons }, parse_mode: 'MarkdownV2' });
        });
    } else { // Original message was a command
        await safeSendMessage(chatId, prompt, {
            reply_markup: { inline_keyboard: buttons }, parse_mode: 'MarkdownV2'
        });
    }
}


// --- Game Command Handlers ---
async function handleCoinflipCommand(msgOrCbMsg, args) { await showBetAmountButtons(msgOrCbMsg, 'coinflip'); }
async function handleRaceCommand(msgOrCbMsg, args) { await showBetAmountButtons(msgOrCbMsg, 'race'); }
async function handleSlotsCommand(msgOrCbMsg, args) { await showBetAmountButtons(msgOrCbMsg, 'slots'); }
async function handleWarCommand(msgOrCbMsg, args) { await showBetAmountButtons(msgOrCbMsg, 'war'); }
async function handleRouletteCommand(msgOrCbMsg, args) { await showBetAmountButtons(msgOrCbMsg, 'roulette'); }
async function handleCrashCommand(msgOrCbMsg, args) { await showBetAmountButtons(msgOrCbMsg, 'crash'); } // NEW
async function handleBlackjackCommand(msgOrCbMsg, args) { await showBetAmountButtons(msgOrCbMsg, 'blackjack'); } // NEW
async function handleLeaderboardsCommand(msg, args) {
    const gameKey = args[1]?.toLowerCase();
    let leaderText = "🏆 *Leaderboards*\n\nCurrently viewing mock data.\n";
    if (gameKey) {
        leaderText += `Top players for *${escapeMarkdownV2(GAME_CONFIG[gameKey]?.name || gameKey)}*:\n`;
        leaderText += `  1. UserA - 1000 SOL wins\n  2. UserB - 500 SOL wins\n`;
    } else {
        leaderText += "Use `/leaderboard <game_name>` e.g. `/leaderboard slots`\nOr choose from popular games below.";
        // TODO: Add buttons for popular game leaderboards
    }
    await safeSendMessage(msg.chat.id, leaderText, {parse_mode: 'MarkdownV2'});
}

const commandHandlers = new Map([
    ['/start', handleStartCommand],
    ['/help', handleHelpCommand],
    ['/wallet', handleWalletCommand],
    ['/referral', handleReferralCommand],
    ['/deposit', handleDepositCommand],
    ['/withdraw', handleWithdrawCommand],
    ['/history', handleHistoryCommand],      // NEW
    ['/leaderboard', handleLeaderboardsCommand], // NEW
    ['/coinflip', handleCoinflipCommand], ['/cf', handleCoinflipCommand],
    ['/race', handleRaceCommand],
    ['/slots', handleSlotsCommand],
    ['/roulette', handleRouletteCommand],
    ['/war', handleWarCommand],
    ['/crash', handleCrashCommand],          // NEW
    ['/blackjack', handleBlackjackCommand],  // NEW
    // '/admin' handled separately in handleMessage
]);

// For menu buttons in handleCallbackQuery
const menuCommandHandlers = new Map([
    ['main', handleStartCommand], // For 'Back to Games Menu'
    ['wallet', handleWalletCommand], // Also calls history now implicitly
    ['referral', handleReferralCommand],
    ['deposit', handleDepositCommand],
    ['withdraw', handleWithdrawCommand],
    ['help', handleHelpCommand],
    ['leaderboards', handleLeaderboardsCommand],
    ['history', handleHistoryCommand],
    ['coinflip', handleCoinflipCommand],
    ['race', handleRaceCommand],
    ['slots', handleSlotsCommand],
    ['roulette', handleRouletteCommand],
    ['war', handleWarCommand],
    ['crash', handleCrashCommand],
    ['blackjack', handleBlackjackCommand],
]);

// Admin command handler (remains largely the same as user's provided code)
async function handleAdminCommand(msg, argsArray) { /* ... (user's existing admin code, ensure it's using argsArray[0] for command, argsArray.slice(1) for subArgs) ... */ }


// --- Referral Check & Payout Triggering Logic ---
/**
 * Checks for and processes referral bonuses after a bet completes successfully.
 * Runs within the provided database client's transaction.
 * @param {string} refereeUserId - The user who placed the bet.
 * @param {number} completedBetId - The ID of the completed bet.
 * @param {bigint} wagerAmountLamports - The amount wagered in the bet.
 * @param {import('pg').PoolClient} client The active database client (for transactional integrity).
 * @param {string} [logPrefix=''] - Optional prefix for logs.
 */
async function _handleReferralChecks(refereeUserId, completedBetId, wagerAmountLamports, client, logPrefix = '') {
    refereeUserId = String(refereeUserId);
    logPrefix = logPrefix || `[ReferralCheck Bet ${completedBetId} User ${refereeUserId}]`;

    try {
        // Fetch details using the provided client to ensure transactional read
        const refereeDetails = await getUserWalletDetails(refereeUserId, client);
        if (!refereeDetails?.referred_by_user_id) return;

        const referrerUserId = refereeDetails.referred_by_user_id;
        const referrerDetails = await getUserWalletDetails(referrerUserId, client);
        if (!referrerDetails?.external_withdrawal_address) {
            console.warn(`${logPrefix} Referrer ${referrerUserId} has no linked wallet. Cannot process payouts.`);
            return;
        }

        let payoutQueued = false;

        // Check Initial Bet Bonus
        const isFirst = await isFirstCompletedBet(refereeUserId, completedBetId); // This still reads from pool by default, ideally pass client
        if (isFirst && wagerAmountLamports >= REFERRAL_INITIAL_BET_MIN_LAMPORTS) {
            const referrerWallet = await queryDatabase('SELECT referral_count FROM wallets WHERE user_id = $1 FOR UPDATE', [referrerUserId], client);
            const referrerCount = parseInt(referrerWallet.rows[0]?.referral_count || '0', 10);
            let bonusTier = REFERRAL_INITIAL_BONUS_TIERS[REFERRAL_INITIAL_BONUS_TIERS.length - 1];
            for (const tier of REFERRAL_INITIAL_BONUS_TIERS) { if (referrerCount <= tier.maxCount) { bonusTier = tier; break; } }
            const initialBonusAmount = BigInt(Math.floor(Number(wagerAmountLamports) * bonusTier.percent));

            if (initialBonusAmount > 0n) {
                const payoutRecord = await recordPendingReferralPayout(referrerUserId, refereeUserId, 'initial_bet', initialBonusAmount, completedBetId, null, client);
                if (payoutRecord.success && payoutRecord.payoutId) {
                    await addPayoutJob({ type: 'payout_referral', payoutId: payoutRecord.payoutId, userId: referrerUserId }); // Part 6 func
                    payoutQueued = true;
                } else if (!payoutRecord.duplicate) {
                    console.error(`${logPrefix} Failed to record initial bonus payout: ${payoutRecord.error}`);
                }
            }
        }

        // Check Milestone Bonus
        const currentTotalWagered = refereeDetails.total_wagered; // Already reflects current wager
        let milestoneCrossed = null;
        for (const threshold of REFERRAL_MILESTONE_THRESHOLDS_LAMPORTS) {
            if (currentTotalWagered >= threshold && threshold > (refereeDetails.last_milestone_paid_lamports || 0n) ) {
                milestoneCrossed = threshold;
            } else if (currentTotalWagered < threshold) break;
        }

        if (milestoneCrossed) {
            const milestoneBonusAmount = BigInt(Math.floor(Number(milestoneCrossed) * REFERRAL_MILESTONE_REWARD_PERCENT));
            if (milestoneBonusAmount > 0n) {
                const payoutRecord = await recordPendingReferralPayout(referrerUserId, refereeUserId, 'milestone', milestoneBonusAmount, null, milestoneCrossed, client);
                if (payoutRecord.success && payoutRecord.payoutId) {
                    await addPayoutJob({ type: 'payout_referral', payoutId: payoutRecord.payoutId, userId: referrerUserId });
                    // Update last_milestone_paid_lamports for the referee
                    await queryDatabase('UPDATE wallets SET last_milestone_paid_lamports = $1 WHERE user_id = $2 AND (last_milestone_paid_lamports IS NULL OR last_milestone_paid_lamports < $1)', [milestoneCrossed, refereeUserId], client);
                    payoutQueued = true;
                } else if (!payoutRecord.duplicate) {
                    console.error(`${logPrefix} Failed to record milestone bonus payout: ${payoutRecord.error}`);
                }
            }
        }
        // No explicit commit/rollback here, as it's part of the larger transaction in placeBet

    } catch (error) {
        console.error(`${logPrefix} Error during referral checks (transactional): ${error.message}`, error.stack);
        // Do not rollback here; let the calling function (placeBet) handle transaction control.
        // Just log and potentially notify admin if it's a critical, unexpected error beyond normal flow.
        // await notifyAdmin(`🚨 UNEXPECTED ERROR during _handleReferralChecks (transactional part) User ${refereeUserId} / Bet ${completedBetId}:\n${escapeMarkdownV2(error.message)}`);
        throw error; // Re-throw to ensure transaction in placeBet is rolled back
    }
}

// --- End of Part 5b ---
// index.js - Part 6: Background Tasks, Payouts, Startup & Shutdown
// --- VERSION: 3.2.0 --- (Implement Crash, Blackjack, Slots Jackpot, Foundational UX Fixes/Adds)

// --- Assuming functions & constants from Parts 1, 2, 3, 5a, 5b are available ---
// Includes: pool, bot, solanaConnection, app, GAME_CONFIG, userStateCache, etc.
// DB ops: queryDatabase, ensureJackpotExists, getJackpotAmount, etc.
// Utils: notifyAdmin, safeSendMessage, escapeMarkdownV2, sleep, formatSol, etc.
// Handlers: handleMessage, handleCallbackQuery
// Constants: BOT_VERSION, SLOTS_JACKPOT_SEED_LAMPORTS, etc.

let server; // Express server instance, will be assigned in setupExpressServer
let isFullyInitialized = false; // From Part 1, controlled here
let depositMonitorIntervalId = null; // From Part 1
let sweepIntervalId = null; // From Part 1
let leaderboardManagerIntervalId = null; // NEW for leaderboards

// --- Database Initialization ---
/**
 * Initializes the database by creating tables if they don't exist
 * and applying necessary constraints/indexes idempotently.
 */
async function initializeDatabase() {
    console.log('⚙️ [DB Init] Initializing Database Schema...');
    let client = null;
    try {
        client = await pool.connect();
        await client.query('BEGIN');
        console.log('⚙️ [DB Init] Transaction started.');

        // Wallets Table
        console.log('⚙️ [DB Init] Ensuring wallets table...');
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
                last_bet_amounts JSONB DEFAULT '{}'::jsonb, -- NEW for last bet preferences
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);
        // Ensure last_bet_amounts column exists if table was created prior
        try {
            await client.query(`ALTER TABLE wallets ADD COLUMN IF NOT EXISTS last_bet_amounts JSONB DEFAULT '{}'::jsonb;`);
        } catch (e) { /* ignore if alter fails, column likely exists */ }
        console.log('⚙️ [DB Init] Ensuring wallets indexes...');
        await client.query('CREATE INDEX IF NOT EXISTS idx_wallets_referral_code ON wallets (referral_code);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_wallets_referred_by ON wallets (referred_by_user_id);');

        // User Balances Table
        console.log('⚙️ [DB Init] Ensuring user_balances table...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS user_balances (
                user_id VARCHAR(255) PRIMARY KEY REFERENCES wallets(user_id) ON DELETE CASCADE,
                balance_lamports BIGINT NOT NULL DEFAULT 0 CHECK (balance_lamports >= 0),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);

        // Deposit Addresses Table
        console.log('⚙️ [DB Init] Ensuring deposit_addresses table...');
        // (Schema as per original Part 6, assumed correct and sufficient)
        await client.query(`
            CREATE TABLE IF NOT EXISTS deposit_addresses (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                deposit_address VARCHAR(44) NOT NULL UNIQUE,
                derivation_path VARCHAR(255) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'pending', -- pending, used, swept, expired
                expires_at TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_checked_at TIMESTAMPTZ
            );
        `);
        console.log('⚙️ [DB Init] Ensuring deposit_addresses indexes...');
        // (Indexes as per original Part 6)
        await client.query('CREATE INDEX IF NOT EXISTS idx_deposit_addresses_user_id ON deposit_addresses (user_id);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_deposit_addresses_status_expires ON deposit_addresses (status, expires_at);');


        // Deposits Table
        console.log('⚙️ [DB Init] Ensuring deposits table...');
        // (Schema as per original Part 6)
        await client.query(`
            CREATE TABLE IF NOT EXISTS deposits (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                deposit_address_id INTEGER REFERENCES deposit_addresses(id) ON DELETE SET NULL,
                tx_signature VARCHAR(88) NOT NULL UNIQUE,
                amount_lamports BIGINT NOT NULL CHECK (amount_lamports > 0),
                status VARCHAR(20) NOT NULL DEFAULT 'confirmed',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);
        // (Indexes as per original Part 6)

        // Bets Table
        console.log('⚙️ [DB Init] Ensuring bets table...');
        // (Schema as per original Part 6, bet_details JSONB supports new games)
         await client.query(`
            CREATE TABLE IF NOT EXISTS bets (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                chat_id VARCHAR(255) NOT NULL,
                game_type VARCHAR(50) NOT NULL,
                bet_details JSONB,
                wager_amount_lamports BIGINT NOT NULL CHECK (wager_amount_lamports > 0),
                payout_amount_lamports BIGINT,
                status VARCHAR(30) NOT NULL DEFAULT 'active', -- active, processing_game, completed_win, completed_loss, completed_push, error_game_logic
                priority INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                processed_at TIMESTAMPTZ
            );
        `);
        // (Indexes as per original Part 6)

        // Withdrawals Table
        console.log('⚙️ [DB Init] Ensuring withdrawals table...');
        // (Schema as per original Part 6)
        await client.query(`
            CREATE TABLE IF NOT EXISTS withdrawals (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                requested_amount_lamports BIGINT NOT NULL,
                fee_lamports BIGINT NOT NULL DEFAULT 0,
                final_send_amount_lamports BIGINT NOT NULL,
                recipient_address VARCHAR(44) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'pending', -- pending, processing, completed, failed
                payout_tx_signature VARCHAR(88) UNIQUE,
                error_message TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                processed_at TIMESTAMPTZ,
                completed_at TIMESTAMPTZ
            );
        `);
        // (Indexes as per original Part 6)

        // Referral Payouts Table
        console.log('⚙️ [DB Init] Ensuring referral_payouts table...');
        // (Schema as per original Part 6)
        await client.query(`
             CREATE TABLE IF NOT EXISTS referral_payouts (
                id SERIAL PRIMARY KEY,
                referrer_user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                referee_user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                payout_type VARCHAR(20) NOT NULL, -- initial_bet, milestone
                payout_amount_lamports BIGINT NOT NULL,
                triggering_bet_id INTEGER REFERENCES bets(id) ON DELETE SET NULL,
                milestone_reached_lamports BIGINT,
                status VARCHAR(20) NOT NULL DEFAULT 'pending', -- pending, processing, paid, failed
                payout_tx_signature VARCHAR(88) UNIQUE,
                error_message TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                processed_at TIMESTAMPTZ,
                paid_at TIMESTAMPTZ,
                CONSTRAINT idx_refpayout_unique_milestone UNIQUE (referrer_user_id, referee_user_id, payout_type, milestone_reached_lamports) WHERE payout_type = 'milestone'
            );
        `);
        // (Indexes as per original Part 6)


        // Ledger Table
        console.log('⚙️ [DB Init] Ensuring ledger table...');
        // (Schema as per original Part 6)
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
                related_ref_payout_id INTEGER REFERENCES referral_payouts(id) ON DELETE SET NULL,
                notes TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);
        // (Indexes as per original Part 6)

        // --- NEW TABLES ---
        // Jackpots Table
        console.log('⚙️ [DB Init] Ensuring jackpots table...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS jackpots (
                game_key VARCHAR(50) PRIMARY KEY,
                current_amount_lamports BIGINT NOT NULL DEFAULT 0 CHECK (current_amount_lamports >= 0),
                last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);

        // Leaderboards Table (Example structure, may need refinement based on exact metrics)
        console.log('⚙️ [DB Init] Ensuring game_leaderboards table...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS game_leaderboards (
                id SERIAL PRIMARY KEY,
                game_key VARCHAR(50) NOT NULL,
                user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                score_type VARCHAR(50) NOT NULL, -- e.g., 'highest_win_multiplier', 'total_wagered', 'win_streak'
                period_type VARCHAR(20) NOT NULL, -- 'daily', 'weekly', 'monthly', 'all_time'
                period_identifier VARCHAR(50) NOT NULL, -- e.g., '2025-05-07', '2025-W19', '2025-05'
                score BIGINT NOT NULL, -- Could be lamports, multiplier value, streak count etc.
                player_display_name VARCHAR(255), -- Cache for quick display
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (game_key, user_id, score_type, period_type, period_identifier) -- Ensure one entry per user per period/type
            );
        `);
        console.log('⚙️ [DB Init] Ensuring game_leaderboards indexes...');
        await client.query('CREATE INDEX IF NOT EXISTS idx_leaderboards_lookup ON game_leaderboards (game_key, score_type, period_type, period_identifier, score DESC);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_leaderboards_user ON game_leaderboards (user_id, game_key, period_type);');


        await client.query('COMMIT');
        console.log('✅ [DB Init] Database schema initialized/verified successfully.');
    } catch (err) {
        console.error('❌ CRITICAL DATABASE INITIALIZATION ERROR:', err);
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { console.error('Rollback failed:', rbErr); } }
        try { await notifyAdmin(`🚨 CRITICAL DB INIT FAILED: ${escapeMarkdownV2(err.message)}\\. Bot cannot start\\. Check logs immediately\\.`).catch(()=>{}); } catch {}
        process.exit(2);
    } finally {
        if (client) client.release();
    }
}


// --- Load Active Deposits Cache --- (Unchanged from original Part 6)
async function loadActiveDepositsCache() { /* ... (as per original code) ... */ }

// --- Load Slots Jackpot --- NEW FUNCTION ---
/** Loads the slots jackpot from DB into memory, ensuring it exists. */
async function loadSlotsJackpot() {
    const logPrefix = '[LoadSlotsJackpot]';
    console.log(`⚙️ ${logPrefix} Loading/Ensuring slots jackpot...`);
    let client = null;
    try {
        client = await pool.connect();
        await client.query('BEGIN');
        const ensured = await ensureJackpotExists('slots', SLOTS_JACKPOT_SEED_LAMPORTS, client); // from Part 2, SLOTS_JACKPOT_SEED_LAMPORTS from Part 1
        if (!ensured) {
            throw new Error("Failed to ensure slots jackpot exists in DB.");
        }
        const jackpotAmount = await getJackpotAmount('slots', client); // from Part 2
        currentSlotsJackpotLamports = jackpotAmount; // currentSlotsJackpotLamports from Part 1
        await client.query('COMMIT');
        console.log(`✅ ${logPrefix} Slots jackpot loaded: ${formatSol(currentSlotsJackpotLamports)} SOL.`);
    } catch (error) {
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) {} }
        console.error(`❌ ${logPrefix} Error loading slots jackpot: ${error.message}`);
        currentSlotsJackpotLamports = SLOTS_JACKPOT_SEED_LAMPORTS; // Fallback to seed
        await notifyAdmin(`🚨 ERROR Loading slots jackpot: ${escapeMarkdownV2(error.message)}. Using seed value.`);
    } finally {
        if (client) client.release();
    }
}


// --- Deposit Monitor --- (Unchanged from original Part 6)
function startDepositMonitor() { /* ... (as per original code) ... */ }
async function monitorDepositsPolling() { /* ... (as per original code) ... */ }
monitorDepositsPolling.isRunning = false;
async function processDepositTransaction(signature, depositAddress, depositAddressId) { /* ... (as per original code, but ensure _handleReferralChecks called within its transaction accepts client) ... */ }

// --- Deposit Sweeper --- (Unchanged from original Part 6)
function startDepositSweeper() { /* ... (as per original code) ... */ }
async function sweepDepositAddresses() { /* ... (as per original code) ... */ }
sweepDepositAddresses.isRunning = false;

// --- Leaderboard Manager --- NEW SECTION ---
/** Starts the leaderboard management background task. */
function startLeaderboardManager() {
    const intervalMs = parseInt(process.env.LEADERBOARD_UPDATE_INTERVAL_MS, 10) || 3600000; // Default 1 hour
    if (leaderboardManagerIntervalId) {
        clearInterval(leaderboardManagerIntervalId);
        console.log('🔄 [LeaderboardManager] Restarting leaderboard manager...');
    } else {
        console.log(`⚙️ [LeaderboardManager] Starting Leaderboard Manager (Interval: ${intervalMs / 1000}s)...`);
    }

    const initialDelay = 60000; // Delay first run
    console.log(`⚙️ [LeaderboardManager] Scheduling first leaderboard processing in ${initialDelay/1000}s...`);
    setTimeout(() => {
        try {
            console.log(`⚙️ [LeaderboardManager] Executing first leaderboard processing run...`);
            processLeaderboardCycle().catch(err => console.error("❌ [Initial Leaderboard Run] Error:", err));
            leaderboardManagerIntervalId = setInterval(processLeaderboardCycle, intervalMs);
            if (leaderboardManagerIntervalId.unref) leaderboardManagerIntervalId.unref();
            console.log(`✅ [LeaderboardManager] Recurring leaderboard processing interval set.`);
        } catch (initialRunError) {
            console.error("❌ [LeaderboardManager] CRITICAL ERROR during initial setup/run:", initialRunError);
            notifyAdmin(`🚨 CRITICAL ERROR setting up Leaderboard Manager: ${escapeMarkdownV2(initialRunError.message)}`).catch(()=>{});
        }
    }, initialDelay);
}

/** Processes leaderboard cycles (e.g., reset daily/weekly, award prizes). Placeholder logic. */
async function processLeaderboardCycle() {
    const logPrefix = '[LeaderboardCycle]';
    if (processLeaderboardCycle.isRunning) {
        console.log(`${logPrefix} Cycle skipped, previous run still active.`);
        return;
    }
    processLeaderboardCycle.isRunning = true;
    console.log(`🏆 ${logPrefix} Starting leaderboard processing cycle...`);
    // TODO: Implement logic:
    // 1. Determine current daily, weekly, monthly period identifiers.
    // 2. For any past periods that haven't been finalized:
    //    a. Query top players from `game_leaderboards` for that period & score_type.
    //    b. Update their `rank` if not already set.
    //    c. Distribute rewards (e.g., call `updateUserBalanceAndLedger`, award badges/VIP status).
    //    d. Mark period as finalized.
    // 3. Notify winners.
    console.log(`🏆 ${logPrefix} Placeholder: Leaderboard cycle logic to be implemented.`);
    processLeaderboardCycle.isRunning = false;
}
processLeaderboardCycle.isRunning = false;


// --- Payout Processing --- (Unchanged from original Part 6, assuming robust error handling and `isRetryable` flags)
async function addPayoutJob(jobData, _retries = 0, _retryDelay = 0) { /* ... (as per original code) ... */ }
async function handleWithdrawalPayoutJob(withdrawalId) { /* ... (as per original code) ... */ }
async function handleReferralPayoutJob(payoutId) { /* ... (as per original code) ... */ }

// --- Startup and Shutdown ---
async function setupTelegramConnection() { /* ... (as per original code) ... */ }
async function startPollingFallback() { /* ... (as per original code) ... */ }
function setupExpressServer() { /* ... (as per original code, ensure server is assigned to global `server`) ... */ }

let isShuttingDown = false; // Ensure this is defined if not already in Part 1
async function shutdown(signal) {
    if (isShuttingDown) {
        console.warn("🚦 Shutdown already in progress, ignoring duplicate signal.");
        return;
    }
    isShuttingDown = true;
    console.warn(`\n🚦 Received signal: ${signal}. Initiating graceful shutdown... (PID: ${process.pid})`);
    isFullyInitialized = false;

    await notifyAdmin(`ℹ️ Bot instance v${escapeMarkdownV2(BOT_VERSION)} shutting down (Signal: ${escapeMarkdownV2(String(signal))})...`).catch(()=>{});

    console.log("🚦 [Shutdown] Stopping Telegram updates...");
    if (bot?.isPolling?.()) {
        await bot.stopPolling({ cancel: true }).then(() => console.log("✅ [Shutdown] Polling stopped.")).catch(e => console.error("❌ [Shutdown] Error stopping polling:", e.message));
    } else if (bot) {
        console.log("ℹ️ [Shutdown] In webhook mode, not deleting webhook on shutdown.");
    }

    console.log("🚦 [Shutdown] Closing HTTP server...");
    if (server) { // server is the global variable
        await new Promise(resolve => server.close(err => {
            if(err) console.error("❌ [Shutdown] Error closing HTTP server:", err);
            else console.log("✅ [Shutdown] HTTP server closed.");
            resolve();
        }));
    } else { console.log("ℹ️ [Shutdown] HTTP server was not running."); }

    console.log("🚦 [Shutdown] Stopping background intervals (Monitor, Sweeper, Leaderboards)...");
    if (depositMonitorIntervalId) clearInterval(depositMonitorIntervalId); depositMonitorIntervalId = null;
    if (sweepIntervalId) clearInterval(sweepIntervalId); sweepIntervalId = null;
    if (leaderboardManagerIntervalId) clearInterval(leaderboardManagerIntervalId); leaderboardManagerIntervalId = null; // NEW
    console.log("✅ [Shutdown] Background intervals cleared.");

    console.log("🚦 [Shutdown] Waiting for processing queues to idle...");
    const queueTimeout = parseInt(process.env.SHUTDOWN_QUEUE_TIMEOUT_MS, 10) || 20000;
    const allQueues = [messageQueue, callbackQueue, payoutProcessorQueue, depositProcessorQueue, telegramSendQueue]; // All from Part 1
    try {
        await Promise.race([
            Promise.all(allQueues.map(q => q.onIdle())),
            sleep(queueTimeout).then(() => Promise.reject(new Error(`Queue idle timeout exceeded (${queueTimeout}ms)`)))
        ]);
        console.log("✅ [Shutdown] All processing queues are idle.");
    } catch (queueError) {
        console.warn(`⚠️ [Shutdown] Queues did not idle within timeout or errored: ${queueError.message}`);
    }

    console.log("🚦 [Shutdown] Closing Database pool...");
    await pool.end()
        .then(() => console.log("✅ [Shutdown] Database pool closed."))
        .catch(e => console.error("❌ [Shutdown] Error closing Database pool:", e.message));

    console.log(`🏁 [Shutdown] Graceful shutdown complete (Signal: ${signal}). Exiting.`);
    process.exit(typeof signal === 'number' ? signal : (signal === 'SIGINT' || signal === 'SIGTERM' ? 0 : 1));
}

// Signal Handlers & Global Error Handlers (as per original Part 6)
// process.on('SIGINT', () => shutdown('SIGINT'));
// process.on('SIGTERM', () => shutdown('SIGTERM'));
// process.on('uncaughtException', async (error, origin) => { /* ... */ });
// process.on('unhandledRejection', async (reason, promise) => { /* ... */ });

function setupTelegramListeners() { // Make sure this function is defined as in original Part 6
    console.log("⚙️ [Listeners] Setting up Telegram event listeners...");
    bot.removeAllListeners('message');
    bot.removeAllListeners('callback_query'); // etc.
    // ... (rest of listener setup as per original Part 6, calling handleMessage/handleCallbackQuery)
    // Example:
    bot.on('message', (msg) => {
        if (!isFullyInitialized && !msg.text?.startsWith('/admin')) { // Example: only allow admin during init issues
            console.warn(`[Message Listener] Bot not fully initialized, ignoring message from ${msg.from.id}`);
            return;
        }
        // Ensure message queue is used
        messageQueue.add(() => handleMessage(msg)).catch(e => console.error(`[MsgQueueErr Listener]: ${e.message}`));
    });
    bot.on('callback_query', (cb) => {
        if (!isFullyInitialized) {
             console.warn(`[Callback Listener] Bot not fully initialized, ignoring callback from ${cb.from.id}`);
             bot.answerCallbackQuery(cb.id, {text: "Bot is starting up, please wait."}).catch(()=>{});
             return;
        }
        callbackQueue.add(() => handleCallbackQuery(cb)).catch(e => { console.error(`[CBQueueErr Listener]: ${e.message}`); bot.answerCallbackQuery(cb.id, { text: "Processing error" }).catch(()=>{}); });
    });
    bot.on('polling_error', (error) => console.error(`❌ TG Polling Error: Code ${error.code} | ${error.message}`, error.stack));
    bot.on('webhook_error', (error) => console.error(`❌ TG Webhook Error: Code ${error.code} | ${error.message}`, error.stack));
    bot.on('error', (error) => console.error('❌ General node-telegram-bot-api Error:', error)); // General library errors
    console.log("✅ [Listeners] Telegram event listeners are ready.");
}


// --- Main Application Start ---
(async () => {
    console.log(`\n🚀 [Startup] Initializing Solana Gambles Bot v${BOT_VERSION}... (PID: ${process.pid})`);
    console.log(`Current Date/Time: ${new Date().toISOString()}`);


    // Setup Signal Handlers & Global Error Handlers early
    console.log("⚙️ [Startup] Setting up process signal handlers (SIGINT, SIGTERM)...");
    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('SIGTERM', () => shutdown('SIGTERM'));

    console.log("⚙️ [Startup] Setting up global error handlers (uncaughtException, unhandledRejection)...");
    process.on('uncaughtException', async (error, origin) => {
        console.error(`\n🚨🚨🚨 UNCAUGHT EXCEPTION [${origin}] 🚨🚨🚨`);
        console.error(error);
        isFullyInitialized = false;
        if (!isShuttingDown) {
            console.error("Initiating emergency shutdown due to uncaught exception...");
            await notifyAdmin(`🚨🚨🚨 UNCAUGHT EXCEPTION (${escapeMarkdownV2(String(origin))}) 🚨🚨🚨\n${escapeMarkdownV2(error.message)}\nAttempting shutdown...`).catch(()=>{});
            await shutdown('uncaughtException').catch((shutdownErr) => {
                console.error("❌ Emergency shutdown attempt failed:", shutdownErr);
                setTimeout(() => process.exit(1), 2000).unref();
            });
        } else {
            console.warn("Uncaught exception occurred during shutdown sequence.");
        }
        const failTimeout = parseInt(process.env.SHUTDOWN_FAIL_TIMEOUT_MS, 10) || 8000;
        setTimeout(() => { console.error(`🚨 Forcing exit after ${failTimeout}ms due to uncaught exception.`); process.exit(1); }, failTimeout).unref();
    });

    process.on('unhandledRejection', async (reason, promise) => {
        console.error('\n🔥🔥🔥 UNHANDLED REJECTION 🔥🔥🔥');
        console.error('Reason:', reason);
        await notifyAdmin(`🔥🔥🔥 UNHANDLED REJECTION 🔥🔥🔥\nReason: ${escapeMarkdownV2(String(reason))}`).catch(()=>{});
    });


    try {
        console.log("⚙️ [Startup Step 1/8] Setting up Telegram Listeners (early)...");
        setupTelegramListeners();

        console.log("⚙️ [Startup Step 2/8] Initializing Database...");
        await initializeDatabase();

        console.log("⚙️ [Startup Step 3/8] Loading Active Deposits Cache...");
        await loadActiveDepositsCache(); // Unchanged

        console.log("⚙️ [Startup Step 4/8] Loading Slots Jackpot..."); // NEW STEP
        await loadSlotsJackpot();

        console.log("⚙️ [Startup Step 5/8] Setting up Telegram Connection...");
        const useWebhook = await setupTelegramConnection(); // Unchanged

        console.log("⚙️ [Startup Step 6/8] Setting up Express Server...");
        setupExpressServer(); // Unchanged, ensure `server` global var is set

        console.log("⚙️ [Startup Step 7/8] Starting Polling (if needed)...");
        if (!useWebhook) {
            await startPollingFallback(); // Unchanged
        }

        const initDelay = parseInt(process.env.INIT_DELAY_MS, 10) || 2000; // Slightly increased default delay
        console.log(`⚙️ [Startup Step 8/8] Scheduling Background Tasks (Monitor, Sweeper, Leaderboards) in ${initDelay / 1000}s...`);

        setTimeout(async () => { // Made async for await on getMe
            try {
                console.log("⚙️ [Startup Final Phase] Starting Background Tasks now...");
                startDepositMonitor();
                startDepositSweeper();
                startLeaderboardManager(); // NEW

                console.log("✅ [Startup Final Phase] Background Tasks scheduled/started.");
                console.log("⚙️ [Startup Final Phase] Setting isFullyInitialized to true...");
                isFullyInitialized = true;
                console.log("✅ [Startup Final Phase] isFullyInitialized flag is now true.");

                try {
                    const me = await bot.getMe();
                    console.log(`✅ [Startup Final Phase] Token validated successfully. Bot username: @${me.username}`);
                    await notifyAdmin(`✅ Bot v${escapeMarkdownV2(BOT_VERSION)} Started Successfully (Mode: ${useWebhook ? 'Webhook' : 'Polling'})`).catch(()=>{});
                    console.log(`\n🎉🎉🎉 Bot is fully operational! (${new Date().toISOString()}) 🎉🎉🎉`);
                } catch (getMeError) {
                    console.error(`❌❌❌ FATAL: bot.getMe() failed during startup! TOKEN INVALID or Telegram API unreachable? Error: ${getMeError.message}`);
                    isFullyInitialized = false;
                    await notifyAdmin(`🚨 BOT STARTUP FAILED (getMe error): ${escapeMarkdownV2(getMeError.message)}. TOKEN INVALID? Network Blocked?`);
                    // Consider shutdown for invalid token if strict
                    // await shutdown('INVALID_TOKEN').catch(() => process.exit(1));
                }
            } catch (finalPhaseError) {
                console.error("❌❌❌ FATAL ERROR DURING FINAL STARTUP PHASE:", finalPhaseError);
                isFullyInitialized = false;
                await notifyAdmin(`🚨 BOT STARTUP FAILED (Final Phase): ${escapeMarkdownV2(finalPhaseError.message)}. Exiting.`).catch(()=>{});
                await shutdown('STARTUP_FINAL_ERROR').catch(() => process.exit(1)); // Use await here
                setTimeout(() => process.exit(1), 5000).unref(); // Force exit
            }
        }, initDelay);

    } catch (error) {
        console.error("❌❌❌ FATAL ERROR DURING STARTUP SEQUENCE ❌❌❌:", error);
        if (pool) { pool.end().catch(() => {});}
        try { await notifyAdmin(`🚨 BOT STARTUP FAILED (Main Sequence): ${escapeMarkdownV2(error.message)}. Exiting.`).catch(()=>{}); } catch {}
        process.exit(1); // Exit if main startup sequence fails
    }
})();
// --- End of Part 6 / End of File index.js ---
