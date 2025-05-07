// index.js - Part 1: Imports, Environment Configuration, Core Initializations, Utility Definitions
// --- VERSION: 3.2.1 --- (Adjusted based on discussions for correctness)

// --- All Imports MUST come first ---
import 'dotenv/config'; // Keep for local development via .env file
import { Pool } from 'pg';
import express from 'express';
import TelegramBot from 'node-telegram-bot-api';
import {
    Connection,
    PublicKey,
    LAMPORTS_PER_SOL,
    Keypair,
    Transaction,
    SystemProgram,
    sendAndConfirmTransaction,
    ComputeBudgetProgram,
    SendTransactionError, // Corrected import name
    TransactionExpiredBlockheightExceededError
} from '@solana/web3.js';
import bs58 from 'bs58';
import * as crypto from 'crypto'; // Keep for crypto.randomBytes
import { createHash } from 'crypto'; // Keep for createHash specifically
import PQueue from 'p-queue';
import { Buffer } from 'buffer';
import bip39 from 'bip39';
import { derivePath } from 'ed25519-hd-key';
import nacl from 'tweetnacl';

import RateLimitedConnection from './lib/solana-connection.js';

// --- Utility function definitions moved to Part 1 before export block ---
/**
 * Creates a promise that resolves after a specified number of milliseconds.
 * @param {number} ms Milliseconds to sleep.
 * @returns {Promise<void>}
 */
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

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

const BOT_VERSION = process.env.npm_package_version || '3.2.1'; // Updated version
console.log(`--- INDEX.JS - DEPLOYMENT CHECK --- ${new Date().toISOString()} --- v${BOT_VERSION} ---`);

console.log(`⏳ Starting Solana Gambles Bot (Custodial, Buttons, v${BOT_VERSION})... Checking environment variables...`);

const REQUIRED_ENV_VARS = [
    'BOT_TOKEN',
    'DATABASE_URL',
    'DEPOSIT_MASTER_SEED_PHRASE',
    'MAIN_BOT_PRIVATE_KEY',
    'REFERRAL_PAYOUT_PRIVATE_KEY',
    'RPC_URLS',
    'ADMIN_USER_IDS',
];

if (process.env.RAILWAY_ENVIRONMENT) {
    REQUIRED_ENV_VARS.push('RAILWAY_PUBLIC_DOMAIN');
}

let missingVars = false;
REQUIRED_ENV_VARS.forEach((key) => {
    if (key === 'REFERRAL_PAYOUT_PRIVATE_KEY') return; // Optional, will default to MAIN_BOT_PRIVATE_KEY if not present, checked in sendSol
    if (key === 'RAILWAY_PUBLIC_DOMAIN' && !process.env.RAILWAY_ENVIRONMENT) return;
    if (!process.env[key]) {
        console.error(`❌ Environment variable ${key} is missing.`);
        missingVars = true;
    }
});

if (!process.env.ADMIN_USER_IDS) {
     if (!missingVars) { // Avoid double logging if already caught by the loop
        console.error(`❌ Environment variable ADMIN_USER_IDS is missing.`);
        missingVars = true;
     }
} else if (process.env.ADMIN_USER_IDS.split(',').map(id => id.trim()).filter(id => /^\d+$/.test(id)).length === 0) {
    console.error(`❌ Environment variable ADMIN_USER_IDS must contain at least one valid comma-separated Telegram User ID.`);
    missingVars = true;
}


console.log(`[Env Check] Checking DEPOSIT_MASTER_SEED_PHRASE... Present: ${!!process.env.DEPOSIT_MASTER_SEED_PHRASE}`);
if (process.env.DEPOSIT_MASTER_SEED_PHRASE && !bip39.validateMnemonic(process.env.DEPOSIT_MASTER_SEED_PHRASE)) {
    console.error(`❌ Environment variable DEPOSIT_MASTER_SEED_PHRASE is set but is not a valid BIP39 mnemonic.`);
    missingVars = true;
} else if (!process.env.DEPOSIT_MASTER_SEED_PHRASE) {
    // Ensure it gets added to the list if it was somehow missed, though the loop above should catch it.
    if (!REQUIRED_ENV_VARS.includes('DEPOSIT_MASTER_SEED_PHRASE')) REQUIRED_ENV_VARS.push('DEPOSIT_MASTER_SEED_PHRASE');
    // This specific re-check ensures it's flagged even if logic above changes
    if (!process.env.DEPOSIT_MASTER_SEED_PHRASE && !missingVars) {
     console.error(`❌ Environment variable DEPOSIT_MASTER_SEED_PHRASE is missing.`);
     missingVars = true;
    }
}

const parsedRpcUrls = (process.env.RPC_URLS || '').split(',').map(u => u.trim()).filter(u => u && (u.startsWith('http://') || u.startsWith('https://')));
if (parsedRpcUrls.length === 0) {
    console.error(`❌ Environment variable RPC_URLS is missing or contains no valid URLs.`);
    missingVars = true;
} else if (parsedRpcUrls.length === 1) {
    console.warn(`⚠️ Environment variable RPC_URLS only contains one URL.`);
}

const validateBase58Key = (keyName, isRequired) => {
    const key = process.env[keyName];
    if (key) {
        try {
            const decoded = bs58.decode(key);
            if (decoded.length !== 64) { // Solana secret keys are 64 bytes
                console.error(`❌ Env var ${keyName} has incorrect length (Expected 64 bytes, Got ${decoded.length}). It should be the full secret key, not just the public key.`);
                return false;
            }
            console.log(`[Env Check] Private Key ${keyName} format validated.`);
        } catch (e) {
            console.error(`❌ Failed to decode env var ${keyName} as base58: ${e.message}`);
            return false;
        }
    } else if (isRequired) {
        console.error(`❌ Required env var ${keyName} is missing.`);
        return false;
    }
    return true;
};

if (!validateBase58Key('MAIN_BOT_PRIVATE_KEY', true)) missingVars = true;
// REFERRAL_PAYOUT_PRIVATE_KEY is optional; if present, validate it. sendSol has fallback.
if (process.env.REFERRAL_PAYOUT_PRIVATE_KEY && !validateBase58Key('REFERRAL_PAYOUT_PRIVATE_KEY', false)) {
    missingVars = true;
}


if (missingVars) {
    console.error("⚠️ Please set all required environment variables correctly. Exiting.");
    process.exit(1);
}

const OPTIONAL_ENV_DEFAULTS = {
    'DEPOSIT_ADDRESS_EXPIRY_MINUTES': '60',
    'DEPOSIT_CONFIRMATIONS': 'confirmed', // 'confirmed' or 'finalized'
    'WITHDRAWAL_FEE_LAMPORTS': '5000',
    'MIN_WITHDRAWAL_LAMPORTS': '10000000', // 0.01 SOL
    'PAYOUT_BASE_PRIORITY_FEE_MICROLAMPORTS': '1000', // For withdrawals/payouts
    'PAYOUT_MAX_PRIORITY_FEE_MICROLAMPORTS': '1000000',
    'PAYOUT_COMPUTE_UNIT_LIMIT': '200000', // Standard transfer is ~200 CUs, but set higher for safety.
    'PAYOUT_JOB_RETRIES': '3',
    'PAYOUT_JOB_RETRY_DELAY_MS': '5000',
    'CF_MIN_BET_LAMPORTS': '10000000', // 0.01 SOL
    'CF_MAX_BET_LAMPORTS': '1000000000', // 1 SOL
    'RACE_MIN_BET_LAMPORTS': '10000000',
    'RACE_MAX_BET_LAMPORTS': '1000000000',
    'SLOTS_MIN_BET_LAMPORTS': '10000000',
    'SLOTS_MAX_BET_LAMPORTS': '500000000', // 0.5 SOL
    'ROULETTE_MIN_BET_LAMPORTS': '10000000',
    'ROULETTE_MAX_BET_LAMPORTS': '1000000000',
    'WAR_MIN_BET_LAMPORTS': '10000000',
    'WAR_MAX_BET_LAMPORTS': '1000000000',
    'CRASH_MIN_BET_LAMPORTS': '10000000',
    'CRASH_MAX_BET_LAMPORTS': '1000000000',
    'BLACKJACK_MIN_BET_LAMPORTS': '10000000',
    'BLACKJACK_MAX_BET_LAMPORTS': '1000000000',
    'CF_HOUSE_EDGE': '0.03',    // 3%
    'RACE_HOUSE_EDGE': '0.05',  // 5%
    'SLOTS_HOUSE_EDGE': '0.10', // 10% (typical for slots, includes jackpot contribution)
    'ROULETTE_HOUSE_EDGE': '0.05', // ~5.26% for double zero, ~2.7% for single. Let's use 5% as a general configurable.
    'WAR_HOUSE_EDGE': '0.02',   // ~2.88% if player always goes to war, higher if not. Simplified.
    'CRASH_HOUSE_EDGE': '0.03', // 3%
    'BLACKJACK_HOUSE_EDGE': '0.015', // ~1.5% with good strategy, can be lower.
    'SLOTS_JACKPOT_SEED_LAMPORTS': '100000000', // 0.1 SOL
    'SLOTS_JACKPOT_CONTRIBUTION_PERCENT': '0.001', // 0.1% of each bet goes to jackpot
    'REFERRAL_INITIAL_BET_MIN_LAMPORTS': '10000000', // Min first bet for referral bonus to trigger
    'REFERRAL_MILESTONE_REWARD_PERCENT': '0.005', // 0.5% of milestone wager amount
    'SWEEP_INTERVAL_MS': '900000', // 15 minutes
    'SWEEP_BATCH_SIZE': '20',
    'SWEEP_FEE_BUFFER_LAMPORTS': '15000', // Buffer for rent + transaction fee when sweeping
    'SWEEP_ADDRESS_DELAY_MS': '750',
    'SWEEP_RETRY_ATTEMPTS': '1',
    'SWEEP_RETRY_DELAY_MS': '3000',
    'RPC_MAX_CONCURRENT': '8',
    'RPC_RETRY_BASE_DELAY': '600',
    'RPC_MAX_RETRIES': '3',
    'RPC_RATE_LIMIT_COOLOFF': '1500',
    'RPC_RETRY_MAX_DELAY': '15000',
    'RPC_RETRY_JITTER': '0.2',
    'RPC_COMMITMENT': 'confirmed', // Default Solana commitment for RPC calls
    'MSG_QUEUE_CONCURRENCY': '5',
    'MSG_QUEUE_TIMEOUT_MS': '10000',
    'CALLBACK_QUEUE_CONCURRENCY': '8',
    'CALLBACK_QUEUE_TIMEOUT_MS': '15000',
    'PAYOUT_QUEUE_CONCURRENCY': '3',
    'PAYOUT_QUEUE_TIMEOUT_MS': '60000',
    'DEPOSIT_PROCESS_QUEUE_CONCURRENCY': '4',
    'DEPOSIT_PROCESS_QUEUE_TIMEOUT_MS': '30000',
    'TELEGRAM_SEND_QUEUE_CONCURRENCY': '1', // Telegram has strict rate limits per chat, globally ~30/sec
    'TELEGRAM_SEND_QUEUE_INTERVAL_MS': '1050', // For 1 message per ~1 second for global limit (adjust based on chat limits too)
    'TELEGRAM_SEND_QUEUE_INTERVAL_CAP': '1', // Process 1 message per interval
    'DEPOSIT_MONITOR_INTERVAL_MS': '20000', // 20 seconds
    'DEPOSIT_MONITOR_ADDRESS_BATCH_SIZE': '50',
    'DEPOSIT_MONITOR_SIGNATURE_FETCH_LIMIT': '5', // Limit for getSignaturesForAddress
    'DB_POOL_MAX': '25',
    'DB_POOL_MIN': '5',
    'DB_IDLE_TIMEOUT': '30000',
    'DB_CONN_TIMEOUT': '5000',
    'DB_SSL': 'true', // Set to 'false' for local DB without SSL
    'DB_REJECT_UNAUTHORIZED': 'true', // Set to 'false' if using self-signed certs (not recommended for prod)
    'USER_STATE_CACHE_TTL_MS': (5 * 60 * 1000).toString(), // 5 minutes
    'WALLET_CACHE_TTL_MS': (10 * 60 * 1000).toString(), // 10 minutes
    'DEPOSIT_ADDR_CACHE_TTL_MS': (61 * 60 * 1000).toString(), // 61 minutes (slightly more than expiry)
    'MAX_PROCESSED_TX_CACHE': '5000',
    'USER_COMMAND_COOLDOWN_MS': '1000', // 1 second
    'INIT_DELAY_MS': '3000', // Increased default for services to settle
    'SHUTDOWN_QUEUE_TIMEOUT_MS': '20000', // How long to wait for queues to idle on shutdown
    'SHUTDOWN_FAIL_TIMEOUT_MS': '8000', // Force exit if graceful shutdown hangs
    'WEBHOOK_MAX_CONN': '40', // Telegram default
    'LEADERBOARD_UPDATE_INTERVAL_MS': '3600000' // 1 hour
};

Object.entries(OPTIONAL_ENV_DEFAULTS).forEach(([key, defaultValue]) => {
    // ADMIN_USER_IDS handled separately above
    if (process.env[key] === undefined) {
        process.env[key] = defaultValue;
    }
});

console.log("✅ Environment variables checked/defaults applied.");

// --- Global Constants & State ---
const SOL_DECIMALS = 9;
const DEPOSIT_ADDRESS_EXPIRY_MS = parseInt(process.env.DEPOSIT_ADDRESS_EXPIRY_MINUTES, 10) * 60 * 1000;
const DEPOSIT_CONFIRMATION_LEVEL = process.env.DEPOSIT_CONFIRMATIONS === 'finalized' ? 'finalized' : (process.env.DEPOSIT_CONFIRMATIONS === 'confirmed' ? 'confirmed' : 'processed'); // 'processed' is also a valid option, but 'confirmed' is common
const WITHDRAWAL_FEE_LAMPORTS = BigInt(process.env.WITHDRAWAL_FEE_LAMPORTS);
const MIN_WITHDRAWAL_LAMPORTS = BigInt(process.env.MIN_WITHDRAWAL_LAMPORTS);
const MAX_PROCESSED_TX_CACHE_SIZE = parseInt(process.env.MAX_PROCESSED_TX_CACHE, 10);
const USER_COMMAND_COOLDOWN_MS = parseInt(process.env.USER_COMMAND_COOLDOWN_MS, 10);
const USER_STATE_TTL_MS = parseInt(process.env.USER_STATE_CACHE_TTL_MS, 10);
const WALLET_CACHE_TTL_MS = parseInt(process.env.WALLET_CACHE_TTL_MS, 10);
const DEPOSIT_ADDR_CACHE_TTL_MS = parseInt(process.env.DEPOSIT_ADDR_CACHE_TTL_MS, 10);
const REFERRAL_INITIAL_BET_MIN_LAMPORTS = BigInt(process.env.REFERRAL_INITIAL_BET_MIN_LAMPORTS);
const REFERRAL_MILESTONE_REWARD_PERCENT = parseFloat(process.env.REFERRAL_MILESTONE_REWARD_PERCENT);
const REFERRAL_INITIAL_BONUS_TIERS = [
    { maxCount: 10, percent: 0.05 },  // 5% for first 10 referrals
    { maxCount: 25, percent: 0.10 },  // 10% for 11-25
    { maxCount: 50, percent: 0.15 },  // 15% for 26-50
    { maxCount: 100, percent: 0.20 }, // 20% for 51-100
    { maxCount: Infinity, percent: 0.25 } // 25% for >100
];
const REFERRAL_MILESTONE_THRESHOLDS_LAMPORTS = [ // Total wagered by referee
    BigInt(1 * LAMPORTS_PER_SOL), BigInt(5 * LAMPORTS_PER_SOL), BigInt(10 * LAMPORTS_PER_SOL),
    BigInt(25 * LAMPORTS_PER_SOL), BigInt(50 * LAMPORTS_PER_SOL), BigInt(100 * LAMPORTS_PER_SOL),
    BigInt(250 * LAMPORTS_PER_SOL), BigInt(500 * LAMPORTS_PER_SOL), BigInt(1000 * LAMPORTS_PER_SOL)
];
const SWEEP_FEE_BUFFER_LAMPORTS = BigInt(process.env.SWEEP_FEE_BUFFER_LAMPORTS);
const SWEEP_BATCH_SIZE = parseInt(process.env.SWEEP_BATCH_SIZE, 10);
const SWEEP_ADDRESS_DELAY_MS = parseInt(process.env.SWEEP_ADDRESS_DELAY_MS, 10);
const SWEEP_RETRY_ATTEMPTS = parseInt(process.env.SWEEP_RETRY_ATTEMPTS, 10);
const SWEEP_RETRY_DELAY_MS = parseInt(process.env.SWEEP_RETRY_DELAY_MS, 10);

const SLOTS_JACKPOT_SEED_LAMPORTS = BigInt(process.env.SLOTS_JACKPOT_SEED_LAMPORTS);
const SLOTS_JACKPOT_CONTRIBUTION_PERCENT = parseFloat(process.env.SLOTS_JACKPOT_CONTRIBUTION_PERCENT);
let currentSlotsJackpotLamports = SLOTS_JACKPOT_SEED_LAMPORTS; // Loaded from DB in Part 6

const STANDARD_BET_AMOUNTS_SOL = [0.01, 0.05, 0.10, 0.25, 0.5, 1.0];
const STANDARD_BET_AMOUNTS_LAMPORTS = STANDARD_BET_AMOUNTS_SOL.map(sol => BigInt(Math.round(sol * LAMPORTS_PER_SOL)));
console.log(`[Config] Standard Bet Amounts (Lamports): ${STANDARD_BET_AMOUNTS_LAMPORTS.join(', ')}`);

const RACE_HORSES = [
    { name: "Solar Sprint", emoji: "🐎₁", payoutMultiplier: 3.0 },
    { name: "Comet Tail", emoji: "🏇₂", payoutMultiplier: 4.0 },
    { name: "Galaxy Gallop", emoji: "🐴₃", payoutMultiplier: 5.0 },
    { name: "Nebula Speed", emoji: "🎠₄", payoutMultiplier: 6.0 },
];

// In-memory Caches & State
const userStateCache = new Map();
const walletCache = new Map();
const activeDepositAddresses = new Map(); // Key: deposit_address_string, Value: { userId, expiresAtTimestamp, timeoutId }
const processedDepositTxSignatures = new Set(); // LRU behavior handled manually if needed, or use a library for larger scale
const commandCooldown = new Map(); // Key: userId, Value: lastCommandTimestamp
const pendingReferrals = new Map(); // Key: refereeUserId, Value: { referrerUserId, timestamp }
const PENDING_REFERRAL_TTL_MS = 24 * 60 * 60 * 1000; // 24 hours
const userLastBetAmounts = new Map(); // Key: userId, Value: Map(gameKey -> lastBetAmountLamports)

let isFullyInitialized = false;
let server; // For Express server instance
let depositMonitorIntervalId = null;
let sweepIntervalId = null;
let leaderboardManagerIntervalId = null; // Defined here, used in Part 6

// --- Core Initializations ---
const app = express();
app.use(express.json({ limit: '10kb' })); // Limit payload size for security

console.log("⚙️ Initializing Multi-RPC Solana connection...");
console.log(`ℹ️ Using RPC Endpoints: ${parsedRpcUrls.join(', ')}`);
const solanaConnection = new RateLimitedConnection(parsedRpcUrls, {
    maxConcurrent: parseInt(process.env.RPC_MAX_CONCURRENT, 10),
    retryBaseDelay: parseInt(process.env.RPC_RETRY_BASE_DELAY, 10),
    maxRetries: parseInt(process.env.RPC_MAX_RETRIES, 10),
    rateLimitCooloff: parseInt(process.env.RPC_RATE_LIMIT_COOLOFF, 10),
    retryMaxDelay: parseInt(process.env.RPC_RETRY_MAX_DELAY, 10),
    retryJitter: parseFloat(process.env.RPC_RETRY_JITTER),
    commitment: process.env.RPC_COMMITMENT, // e.g. 'confirmed'
    httpHeaders: { 'User-Agent': `SolanaGamblesBot/${BOT_VERSION}` },
    clientId: `SolanaGamblesBot/${BOT_VERSION}` // For potential internal tracking in RateLimitedConnection
});
console.log("✅ Multi-RPC Solana connection instance created.");


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
    // notifyAdmin is defined later, so direct call here might error if it's not hoisted or Part 3 not parsed.
    // For now, just console log. We can improve this if notifyAdmin is also moved to Part 1 or structured differently.
    // For now, using a self-contained temporary notifier or queuing this notification.
    console.error(`[DB Pool Error (Idle Client)] To Admin: ${err.message}`);
});
console.log("✅ PostgreSQL Pool created.");

console.log("⚙️ Initializing Telegram Bot...");
const bot = new TelegramBot(process.env.BOT_TOKEN, {
    polling: false, // Explicitly false, will be enabled in Part 6 if no webhook
    request: { // Options for underlying 'request' library or http(s) agent
        timeout: 15000, // Request timeout
        agentOptions: { // Passed to http(s).Agent constructor
            keepAlive: true,
            keepAliveMsecs: 60000,
            maxSockets: 100, // Max sockets for all requests
            maxFreeSockets: 10, // Max sockets to keep open in a free state
            scheduling: 'fifo', // Scheduling strategy for requests ('fifo' or 'lifo')
        }
    }
});
console.log("✅ Telegram Bot instance created.");

console.log("⚙️ Loading Game Configuration...");
const GAME_CONFIG = {
    coinflip: {
        key: "coinflip", name: "Coinflip",
        minBetLamports: BigInt(process.env.CF_MIN_BET_LAMPORTS), maxBetLamports: BigInt(process.env.CF_MAX_BET_LAMPORTS),
        houseEdge: parseFloat(process.env.CF_HOUSE_EDGE)
    },
    race: {
        key: "race", name: "Race",
        minBetLamports: BigInt(process.env.RACE_MIN_BET_LAMPORTS), maxBetLamports: BigInt(process.env.RACE_MAX_BET_LAMPORTS),
        houseEdge: parseFloat(process.env.RACE_HOUSE_EDGE)
    },
    slots: {
        key: "slots", name: "Slots",
        minBetLamports: BigInt(process.env.SLOTS_MIN_BET_LAMPORTS), maxBetLamports: BigInt(process.env.SLOTS_MAX_BET_LAMPORTS),
        houseEdge: parseFloat(process.env.SLOTS_HOUSE_EDGE),
        jackpotContributionPercent: parseFloat(process.env.SLOTS_JACKPOT_CONTRIBUTION_PERCENT),
        jackpotSeedLamports: BigInt(process.env.SLOTS_JACKPOT_SEED_LAMPORTS) // Redundant with SLOTS_JACKPOT_SEED_LAMPORTS const but fine for config
    },
    roulette: {
        key: "roulette", name: "Roulette",
        minBetLamports: BigInt(process.env.ROULETTE_MIN_BET_LAMPORTS), maxBetLamports: BigInt(process.env.ROULETTE_MAX_BET_LAMPORTS),
        houseEdge: parseFloat(process.env.ROULETTE_HOUSE_EDGE)
    },
    war: {
        key: "war", name: "Casino War",
        minBetLamports: BigInt(process.env.WAR_MIN_BET_LAMPORTS), maxBetLamports: BigInt(process.env.WAR_MAX_BET_LAMPORTS),
        houseEdge: parseFloat(process.env.WAR_HOUSE_EDGE)
    },
    crash: {
        key: "crash", name: "Crash",
        minBetLamports: BigInt(process.env.CRASH_MIN_BET_LAMPORTS), maxBetLamports: BigInt(process.env.CRASH_MAX_BET_LAMPORTS),
        houseEdge: parseFloat(process.env.CRASH_HOUSE_EDGE)
    },
    blackjack: {
        key: "blackjack", name: "Blackjack",
        minBetLamports: BigInt(process.env.BLACKJACK_MIN_BET_LAMPORTS), maxBetLamports: BigInt(process.env.BLACKJACK_MAX_BET_LAMPORTS),
        houseEdge: parseFloat(process.env.BLACKJACK_HOUSE_EDGE)
    }
};
function validateGameConfig(config) {
    const errors = [];
    for (const gameKey in config) {
        const gc = config[gameKey];
        if (!gc) { errors.push(`Config for game "${gameKey}" missing.`); continue; }
        if (gc.key !== gameKey) errors.push(`${gameKey}.key mismatch`);
        if (!gc.name) errors.push(`${gameKey}.name missing`);
        if (typeof gc.minBetLamports !== 'bigint' || gc.minBetLamports <= 0n) errors.push(`${gameKey}.minBetLamports invalid`);
        if (typeof gc.maxBetLamports !== 'bigint' || gc.maxBetLamports <= 0n) errors.push(`${gameKey}.maxBetLamports invalid`);
        if (gc.minBetLamports > gc.maxBetLamports) errors.push(`${gameKey} minBet > maxBet`);
        if (isNaN(gc.houseEdge) || gc.houseEdge < 0 || gc.houseEdge >= 1) errors.push(`${gameKey}.houseEdge invalid`);
        if (gameKey === 'slots') {
            if (isNaN(gc.jackpotContributionPercent) || gc.jackpotContributionPercent < 0 || gc.jackpotContributionPercent >= 1) errors.push(`${gameKey}.jackpotContributionPercent invalid`);
            if (typeof gc.jackpotSeedLamports !== 'bigint' || gc.jackpotSeedLamports < 0n) errors.push(`${gameKey}.jackpotSeedLamports invalid`);
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

// Declare commandHandlers and menuCommandHandlers with `let` so they can be exported
// and then populated later in Part 5b. This avoids moving all handler function
// definitions to Part 1.
let commandHandlers;
let menuCommandHandlers;

// --- Export necessary variables/instances ---
// Note: Functions defined in later parts are exported by name.
// Due to function hoisting (for `function name(){}`) this is generally fine in a single ES module file.
// `const` or `let` assigned functions/variables like `sleep` or `escapeMarkdownV2` have been moved above.
export {
    // Core Instances
    pool, bot, solanaConnection, app,
    // Caches & State
    userStateCache, walletCache, activeDepositAddresses,
    processedDepositTxSignatures, commandCooldown, pendingReferrals, userLastBetAmounts,
    // Config & Constants
    GAME_CONFIG, RACE_HORSES,
    BOT_VERSION, LAMPORTS_PER_SOL, SOL_DECIMALS, DEPOSIT_CONFIRMATION_LEVEL,
    MIN_WITHDRAWAL_LAMPORTS, WITHDRAWAL_FEE_LAMPORTS, REFERRAL_INITIAL_BET_MIN_LAMPORTS,
    REFERRAL_MILESTONE_REWARD_PERCENT, REFERRAL_INITIAL_BONUS_TIERS, REFERRAL_MILESTONE_THRESHOLDS_LAMPORTS,
    DEPOSIT_ADDRESS_EXPIRY_MS, USER_STATE_TTL_MS, PENDING_REFERRAL_TTL_MS, STANDARD_BET_AMOUNTS_LAMPORTS,
    STANDARD_BET_AMOUNTS_SOL, MAX_PROCESSED_TX_CACHE_SIZE, USER_COMMAND_COOLDOWN_MS,
    SLOTS_JACKPOT_SEED_LAMPORTS, SLOTS_JACKPOT_CONTRIBUTION_PERCENT, currentSlotsJackpotLamports,
    // Queues
    messageQueue, callbackQueue, payoutProcessorQueue, depositProcessorQueue, telegramSendQueue,
    // Mutable Global State (for startup/shutdown)
    isFullyInitialized, server,
    depositMonitorIntervalId, sweepIntervalId, leaderboardManagerIntervalId,
    // Utility functions defined in this Part (Part 1)
    sleep, escapeMarkdownV2,
    // Placeholders for functions to be defined in subsequent Parts
    // Part 2 (Database Operations)
    queryDatabase, ensureUserExists, linkUserWallet, getUserWalletDetails, getLinkedWallet,
    getUserByReferralCode, linkReferral, updateUserWagerStats, getUserBalance,
    updateUserBalanceAndLedger, createDepositAddressRecord, findDepositAddressInfo,
    markDepositAddressUsed, recordConfirmedDeposit, getNextDepositAddressIndex,
    createWithdrawalRequest, updateWithdrawalStatus, getWithdrawalDetails, // Corrected: updateWithdrawalStatus was updateWithdrawalDbStatus
    createBetRecord, updateBetStatus, isFirstCompletedBet, getBetDetails,
    recordPendingReferralPayout, updateReferralPayoutStatus, getReferralPayoutDetails,
    getTotalReferralEarnings, updateUserLastBetAmount, getUserLastBetAmounts,
    ensureJackpotExists, getJackpotAmount, updateJackpotAmount, incrementJackpotAmount,
    getBetHistory,
    // Part 3 (Solana Utilities & Telegram Helpers)
    formatSol, createSafeIndex, generateUniqueDepositAddress, getKeypairFromPath,
    isRetryableSolanaError, sendSol, analyzeTransactionAmounts, notifyAdmin,
    safeSendMessage, escapeHtml, getUserDisplayName,
    updateWalletCache, getWalletCache, addActiveDepositAddressCache,
    getActiveDepositAddressCache, removeActiveDepositAddressCache, addProcessedDepositTx,
    hasProcessedDepositTx, generateReferralCode,
    // Part 4 (Game Logic)
    playCoinflip, simulateRace, simulateSlots, simulateRouletteSpin,
    getRoulettePayoutMultiplier, simulateWar,
    simulateCrash,
    createDeck, shuffleDeck, dealCard, getCardNumericValue, formatCard, calculateHandValue,
    simulateDealerPlay, determineBlackjackWinner,
    // Part 5a (Telegram Message/Callback Handlers & Game Result Processing)
    handleMessage, handleCallbackQuery, proceedToGameStep, placeBet,
    handleCoinflipGame, handleRaceGame, handleSlotsGame, handleWarGame, handleRouletteGame,
    handleCrashGame, handleBlackjackGame,
    // Part 5b (Input Handlers, Command Handlers, Referral Checks)
    routeStatefulInput, handleCustomAmountInput, handleRouletteNumberInput,
    handleWithdrawalAmountInput,
    handleStartCommand, handleHelpCommand, handleWalletCommand, handleReferralCommand,
    handleDepositCommand, handleWithdrawCommand, showBetAmountButtons,
    handleCoinflipCommand, handleRaceCommand, handleSlotsCommand, handleWarCommand,
    handleRouletteCommand, handleCrashCommand, handleBlackjackCommand,
    handleHistoryCommand, handleLeaderboardsCommand,
    commandHandlers, menuCommandHandlers, // Exporting the let-declared variables
    handleAdminCommand, _handleReferralChecks,
    // Part 6 (Background Tasks, Payouts, Startup & Shutdown)
    initializeDatabase, loadActiveDepositsCache, loadSlotsJackpot,
    startDepositMonitor, startDepositSweeper, startLeaderboardManager, // startLeaderboardManager will be defined in Part 6
    addPayoutJob,
    setupTelegramConnection, startPollingFallback, setupExpressServer, shutdown, setupTelegramListeners
};

// --- End of Part 1 ---
// index.js - Part 2: Database Operations
// --- VERSION: 3.2.1 --- (Adjusted based on discussions for correctness)

// --- Assuming 'pool', 'PublicKey', utils like 'generateReferralCode', 'updateWalletCache' etc. are available from other parts ---
// --- Also assuming relevant constants like BOT_VERSION, GAME_CONFIG, LAMPORTS_PER_SOL, SOL_DECIMALS are available ---
// --- 'addActiveDepositAddressCache', 'getActiveDepositAddressCache', 'removeActiveDepositAddressCache' are from Part 3 but used here.
// --- 'formatSol' utility (used in a log in ensureJackpotExists) is from Part 3.
// --- 'addPayoutJob' (used in _handleReferralChecks in Part 5b which relies on DB ops) is from Part 6.

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
         if (!pool) { // pool is from Part 1
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
        // console.error(`   Stack: ${error.stack}`); // Optional: full stack trace for more debugging
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
            `INSERT INTO wallets (user_id, created_at, last_used_at, last_bet_amounts) VALUES ($1, NOW(), NOW(), '{}'::jsonb)`,
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
        if (!isNewUser) { // If wallet existed but balance didn't
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
            new PublicKey(externalAddress); // PublicKey from @solana/web3.js (Part 1)
        } catch (validationError) {
            console.error(`${logPrefix} Invalid address format provided: ${externalAddress}`);
            return { success: false, error: 'Invalid Solana wallet address format.' };
        }

        client = await pool.connect(); // pool from Part 1
        await client.query('BEGIN');

        await ensureUserExists(userId, client); // Locks rows

        // ensureUserExists should create the row if it doesn't exist.
        // Fetch details again to get referral code, ensuring row lock from ensureUserExists is respected.
        const detailsRes = await queryDatabase('SELECT referral_code FROM wallets WHERE user_id = $1 FOR UPDATE', [userId], client);
        if (detailsRes.rowCount === 0) {
            // This should ideally not happen if ensureUserExists works correctly.
            throw new Error(`Wallet row for user ${userId} not found after ensureUserExists call.`);
        }

        let currentReferralCode = detailsRes.rows[0]?.referral_code;
        let needsCodeUpdate = false;

        if (!currentReferralCode) {
            console.log(`${logPrefix} User missing referral code, generating one.`);
            currentReferralCode = generateReferralCode(); // generateReferralCode from Part 3
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

        updateWalletCache(userId, { withdrawalAddress: externalAddress, referralCode: currentReferralCode }); // updateWalletCache from Part 3

        return { success: true, wallet: externalAddress, referralCode: currentReferralCode };

    } catch (err) {
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { console.error(`${logPrefix} Rollback failed:`, rbErr); } }

        if (err.code === '23505' && err.constraint === 'wallets_referral_code_key') {
            console.error(`${logPrefix} CRITICAL - Referral code generation conflict for user ${userId}. This should be rare.`);
            // It's possible to retry generating a new code here, but for simplicity, ask user to try again.
            return { success: false, error: 'Referral code generation conflict. Please try again.' };
        }
        console.error(`${logPrefix} DB Error linking wallet:`, err.message, err.code);
        return { success: false, error: `Database error linking wallet (Code: ${err.code || 'N/A'})` };
    } finally {
        if (client) client.release();
    }
}

/**
 * Fetches comprehensive user details from the 'wallets' table.
 * @param {string} userId
 * @param {import('pg').PoolClient} [client=pool] Optional DB client for transactions.
 * @returns {Promise<object | null>} Object with wallet details or null if not found.
 */
async function getUserWalletDetails(userId, client = pool) { // Allow passing client
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
            last_bet_amounts,
            created_at
        FROM wallets
        WHERE user_id = $1
    `;
    try {
        const res = await queryDatabase(query, [userId], client); // Use provided client or default pool
        if (res.rows.length > 0) {
            const details = res.rows[0];
            // Ensure correct types, especially for BigInt and numbers
            details.total_wagered = BigInt(details.total_wagered || '0');
            details.last_milestone_paid_lamports = BigInt(details.last_milestone_paid_lamports || '0');
            details.referral_count = parseInt(details.referral_count || '0', 10);
            details.last_bet_amounts = details.last_bet_amounts || {}; // Ensure it's an object, not null
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
    const cached = getWalletCache(userId); // getWalletCache from Part 3
    if (cached?.withdrawalAddress !== undefined) {
        return cached.withdrawalAddress;
    }

    try {
        // Fetching less data than getUserWalletDetails for performance if only address is needed
        const res = await queryDatabase('SELECT external_withdrawal_address, referral_code FROM wallets WHERE user_id = $1', [userId]);
        const details = res.rows[0];
        const withdrawalAddress = details?.external_withdrawal_address || undefined; // Ensure undefined if null/missing
        const referralCode = details?.referral_code;

        // Update cache with potentially fresh data (address + ref code), including undefined if not set
        updateWalletCache(userId, { withdrawalAddress: withdrawalAddress, referralCode: referralCode }); // updateWalletCache from Part 3
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
        return null; // Basic validation
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
        // Ensure referee exists by this point (should be handled by calling logic, e.g. ensureUserExists in deposit)
        const checkRes = await queryDatabase('SELECT referred_by_user_id FROM wallets WHERE user_id = $1 FOR UPDATE', [refereeUserId], client);
        if (checkRes.rowCount === 0) {
            console.error(`${logPrefix} Referee user ${refereeUserId} not found. Cannot link.`);
            return false; // Referee must exist
        }
        const currentReferrer = checkRes.rows[0].referred_by_user_id;

        if (currentReferrer === referrerUserId) {
            console.log(`${logPrefix} Already linked to this referrer. No action needed.`);
            return true; // Idempotent: already linked correctly
        } else if (currentReferrer) {
            console.warn(`${logPrefix} Already referred by ${currentReferrer}. Cannot link to ${referrerUserId}.`);
            return false; // Cannot change referrer
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
            await queryDatabase('SELECT user_id FROM wallets WHERE user_id = $1 FOR UPDATE', [referrerUserId], client); // Lock referrer
            await queryDatabase('UPDATE wallets SET referral_count = referral_count + 1 WHERE user_id = $1', [referrerUserId], client);
            console.log(`${logPrefix} Incremented referral count for referrer ${referrerUserId}.`);
            return true;
        } else {
            // This case means the referee was linked by someone else between the SELECT FOR UPDATE and this UPDATE.
            console.warn(`${logPrefix} Failed to link referral - likely already referred just before update (race condition handled).`);
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

    if (wageredAmount <= 0n) {
        console.log(`${logPrefix} Wager amount is ${wageredAmount}, no update to wager stats needed.`);
        return true; // No change needed if wager is zero or negative
    }

    try {
         // Lock the user's wallet row before updating
         // Ensure user exists (this also locks)
        await queryDatabase('SELECT user_id FROM wallets WHERE user_id = $1 FOR UPDATE', [userId], client);

        let query;
        let values;
        // Validate newLastMilestonePaidLamports if provided
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
            console.warn(`${logPrefix} Attempted to update wager stats for non-existent user ${userId} (or lock failed?). This should not happen if ensureUserExists was called.`);
            return false;
        }
        return true;
    } catch (err) {
        console.error(`${logPrefix} Error updating wager stats:`, err.message);
        return false;
    }
}

// --- User Last Bet Preferences ---

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
        // Fetch current last_bet_amounts. It's crucial this read is consistent with the update.
        const currentRes = await queryDatabase(
            'SELECT last_bet_amounts FROM wallets WHERE user_id = $1 FOR UPDATE', // Added FOR UPDATE here
            [userId],
            client
        );
        if (currentRes.rowCount === 0) {
            console.warn(`${logPrefix} User not found, cannot update last bet amount.`);
            return false; // Or throw error if user should always exist here
        }
        const currentLastBets = currentRes.rows[0].last_bet_amounts || {};
        currentLastBets[gameKey] = amountLamports.toString(); // Store as string in JSON

        const updateRes = await queryDatabase(
            'UPDATE wallets SET last_bet_amounts = $1, last_used_at = NOW() WHERE user_id = $2',
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
 * @returns {Promise<{[gameKey: string]: bigint} | null>} Object map of gameKey to BigInt amount, or null on error, or {} if none.
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
                if (Object.prototype.hasOwnProperty.call(lastBetsRaw, gameKey)) { // Safer hasOwnProperty check
                    try {
                        lastBets[gameKey] = BigInt(lastBetsRaw[gameKey]);
                    } catch (e) {
                        console.warn(`${logPrefix} Failed to parse BigInt for game ${gameKey} ('${lastBetsRaw[gameKey]}') in last_bet_amounts for user ${userId}. Skipping this entry.`);
                    }
                }
            }
            return lastBets;
        }
        return {}; // Return empty object if no record or no last bets
    } catch (err) {
        console.error(`${logPrefix} Error fetching last bet amounts:`, err.message);
        return null; // Indicate error
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

        // Fetch balance (row is implicitly locked by ensureUserExists FOR UPDATE on user_balances table)
        const balanceRes = await queryDatabase('SELECT balance_lamports FROM user_balances WHERE user_id = $1', [userId], client);

        await client.query('COMMIT'); // Commit ensuring user/balance exists

        return BigInt(balanceRes.rows[0]?.balance_lamports || '0');

    } catch (err) {
        console.error(`[DB GetBalance] Error fetching/ensuring balance for user ${userId}:`, err.message);
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { console.error(`[DB GetBalance] Rollback failed: ${rbErr.message}`); } }
        return 0n; // Return 0 on error to prevent further issues, though this hides the problem.
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

    // Validate and nullify related IDs if not proper integers
    const relatedBetId = Number.isInteger(relatedIds?.betId) && relatedIds.betId > 0 ? relatedIds.betId : null;
    const relatedDepositId = Number.isInteger(relatedIds?.depositId) && relatedIds.depositId > 0 ? relatedIds.depositId : null;
    const relatedWithdrawalId = Number.isInteger(relatedIds?.withdrawalId) && relatedIds.withdrawalId > 0 ? relatedIds.withdrawalId : null;
    const relatedRefPayoutId = Number.isInteger(relatedIds?.refPayoutId) && relatedIds.refPayoutId > 0 ? relatedIds.refPayoutId : null;

    try {
        // 1. Get current balance and lock row
        const balanceRes = await queryDatabase(
            'SELECT balance_lamports FROM user_balances WHERE user_id = $1 FOR UPDATE',
            [userId],
            client
        );

        if (balanceRes.rowCount === 0) {
            // This should not happen if ensureUserExists was called prior to this transaction block.
            console.error(`${logPrefix} User balance record not found during update for user ${userId}. Ensure user exists first.`);
            return { success: false, error: 'User balance record unexpectedly missing.' };
        }

        const balanceBefore = BigInt(balanceRes.rows[0].balance_lamports);
        const balanceAfter = balanceBefore + changeAmount;

        // 2. Check for insufficient funds only if debiting
        if (changeAmount < 0n && balanceAfter < 0n) {
            const needed = -changeAmount; // Amount attempted to debit
            console.warn(`${logPrefix} Insufficient balance for user ${userId}. Current: ${balanceBefore}, Needed: ${needed}, Would be: ${balanceAfter}`);
            return { success: false, error: 'Insufficient balance' };
        }

        // 3. Update balance
        const updateRes = await queryDatabase(
            'UPDATE user_balances SET balance_lamports = $1, updated_at = NOW() WHERE user_id = $2',
            [balanceAfter, userId],
            client
        );
        if (updateRes.rowCount === 0) {
            // Should not happen if row was locked
            console.error(`${logPrefix} Failed to update balance row for user ${userId} after lock. This is unexpected.`);
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
        console.error(`${logPrefix} Error updating balance/ledger for user ${userId}:`, err.message, err.code);
        let errMsg = `Database error during balance update (Code: ${err.code || 'N/A'})`;
        if (err.constraint === 'user_balances_balance_lamports_check') { // Matches DB schema
            errMsg = 'Insufficient balance (check constraint violation).';
        }
        return { success: false, error: errMsg };
    }
}


// --- Deposit Operations ---

/**
 * Creates a new unique deposit address record for a user.
 * Does NOT require an external transaction (manages its own with the pool).
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
    console.log(`${logPrefix} Attempting to create record. Path: ${derivationPath}`); // Log path
    const query = `
        INSERT INTO deposit_addresses (user_id, deposit_address, derivation_path, expires_at, status, created_at)
        VALUES ($1, $2, $3, $4, 'pending', NOW())
        RETURNING id;
    `;
    try {
        const res = await queryDatabase(query, [userId, depositAddress, derivationPath, expiresAt], pool); // Using default pool
        if (res.rowCount > 0 && res.rows[0].id) {
            const depositAddressId = res.rows[0].id;
            console.log(`${logPrefix} Successfully created record with ID: ${depositAddressId}. Adding to cache.`);
            addActiveDepositAddressCache(depositAddress, userId, expiresAt.getTime()); // addActiveDepositAddressCache from Part 3
            return { success: true, id: depositAddressId };
        } else {
            console.error(`${logPrefix} Failed to insert deposit address record (no ID returned). This is unexpected.`);
            return { success: false, error: 'Failed to insert deposit address record (no ID returned).' };
        }
    } catch (err) {
        console.error(`${logPrefix} DB Error:`, err.message, err.code);
        if (err.code === '23505' && err.constraint === 'deposit_addresses_deposit_address_key') {
            console.error(`${logPrefix} CRITICAL - Deposit address collision detected for address ${depositAddress}. This should be extremely rare.`);
            // Consider notifying admin immediately if this happens.
            return { success: false, error: 'Deposit address collision detected.' };
        }
        return { success: false, error: `Database error creating deposit address (Code: ${err.code || 'N/A'})` };
    }
}

/**
 * Finds user ID and status associated with a deposit address. Checks cache first.
 * @param {string} depositAddress
 * @returns {Promise<{userId: string, status: string, id: number, expiresAt: Date, derivationPath: string | null} | null>} Returns null if not found, expired, or on DB error.
 */
async function findDepositAddressInfo(depositAddress) {
    const logPrefix = `[DB FindDepositAddr Addr ${depositAddress.slice(0, 6)}..]`;
    const cached = getActiveDepositAddressCache(depositAddress); // getActiveDepositAddressCache from Part 3
    const now = Date.now();

    if (cached && now < cached.expiresAt) {
        // Cache hit, but still need to fetch full details like status and ID from DB if not in cache
        // Or assume cache is only for userId and expiry check, and always fetch full from DB.
        // For consistency, let's fetch from DB to get current status and other fields.
        // The cache helps quickly identify if an address *might* be active for a user.
        try {
            // Fetch status, id, expires_at, derivation_path for the cached entry
            const statusRes = await queryDatabase(
                'SELECT status, id, expires_at, derivation_path FROM deposit_addresses WHERE deposit_address = $1',
                [depositAddress]
            );
            if (statusRes.rowCount > 0) {
                const dbInfo = statusRes.rows[0];
                const dbExpiresAt = new Date(dbInfo.expires_at).getTime();

                // Optional: Validate or update cache expiry if different (though unlikely if managed well)
                if (cached.expiresAt !== dbExpiresAt) {
                    console.warn(`${logPrefix} Cache expiry mismatch for ${depositAddress}. DB: ${dbExpiresAt}, Cache: ${cached.expiresAt}. Updating cache.`);
                    addActiveDepositAddressCache(depositAddress, cached.userId, dbExpiresAt); // from Part 3
                }
                return { userId: cached.userId, status: dbInfo.status, id: dbInfo.id, expiresAt: new Date(dbInfo.expires_at), derivationPath: dbInfo.derivation_path };
            } else {
                // Address was cached but not found in DB - inconsistent state. Remove from cache.
                removeActiveDepositAddressCache(depositAddress); // from Part 3
                console.warn(`${logPrefix} Address ${depositAddress} was cached but not found in DB. Cache removed.`);
                return null;
            }
        } catch (err) {
            console.error(`${logPrefix} Error fetching details for cached address ${depositAddress}:`, err.message);
            return null; // DB error
        }
    } else if (cached) { // Cached but expired
        console.log(`${logPrefix} Cache entry for ${depositAddress} expired. Removing.`);
        removeActiveDepositAddressCache(depositAddress); // from Part 3
        // Proceed to fetch from DB anyway, it might still be 'pending' but expired, or already 'used'/'swept'.
    }

    // Not in active cache or cache expired, fetch from DB
    try {
        const res = await queryDatabase(
            'SELECT user_id, status, id, expires_at, derivation_path FROM deposit_addresses WHERE deposit_address = $1',
            [depositAddress]
        );
        if (res.rowCount > 0) {
            const data = res.rows[0];
            const expiresAtDate = new Date(data.expires_at);
            // If it's pending and not expired, add/update it in cache
            if (data.status === 'pending' && now < expiresAtDate.getTime()) {
                addActiveDepositAddressCache(depositAddress, data.user_id, expiresAtDate.getTime()); // from Part 3
            } else if (data.status === 'pending' && now >= expiresAtDate.getTime()) {
                // Found in DB as 'pending' but is now expired. Mark it 'expired' in DB.
                console.warn(`${logPrefix} Address ${depositAddress} found as 'pending' in DB but is expired. Attempting background update to 'expired'.`);
                // Fire-and-forget update for status, or queue it. For simplicity, direct query.
                queryDatabase("UPDATE deposit_addresses SET status = 'expired' WHERE id = $1 AND status = 'pending'", [data.id], pool)
                 .catch(err => console.error(`${logPrefix} Background attempt to mark address ID ${data.id} as expired failed: ${err.message}`));
            }
            return { userId: data.user_id, status: data.status, id: data.id, expiresAt: expiresAtDate, derivationPath: data.derivation_path };
        }
        return null; // Not found in DB
    } catch (err) {
        console.error(`${logPrefix} Error finding address info in DB for ${depositAddress}:`, err.message);
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
        const addrRes = await queryDatabase('SELECT deposit_address, status FROM deposit_addresses WHERE id = $1 FOR UPDATE', [depositAddressId], client);
        if (addrRes.rowCount === 0) {
            console.warn(`${logPrefix} Cannot mark as used: Address ID ${depositAddressId} not found.`);
            return false;
        }
        const { deposit_address: depositAddress, status: currentStatus } = addrRes.rows[0];

        if (currentStatus !== 'pending') {
            console.warn(`${logPrefix} Address ID ${depositAddressId} is already in status '${currentStatus}', not 'pending'. Cannot mark as 'used'.`);
            // Still remove from cache if it was there, as it's no longer 'pending' for new deposits.
            removeActiveDepositAddressCache(depositAddress); // from Part 3
            return false; // Indicate it wasn't updated *by this call* from pending to used
        }

        // Update status only if currently pending
        const res = await queryDatabase(
            `UPDATE deposit_addresses SET status = 'used' WHERE id = $1 AND status = 'pending'`,
            [depositAddressId],
            client
        );
        if (res.rowCount > 0) {
            removeActiveDepositAddressCache(depositAddress); // from Part 3
            return true;
        } else {
            // Should not happen if the SELECT FOR UPDATE and status check above worked.
            console.warn(`${logPrefix} Failed to mark as 'used' (status might not have been 'pending' despite initial check, or row lock issue).`);
            removeActiveDepositAddressCache(depositAddress); // Still attempt to remove from cache
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
 * @param {number} depositAddressId The ID of the 'deposit_addresses' record used.
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
            // ON CONFLICT triggered or insert failed strangely (no ID returned).
            // Verify if it exists due to ON CONFLICT.
            console.warn(`${logPrefix} Deposit TX ${txSignature} potentially already processed (ON CONFLICT or no rows returned from INSERT). Verifying...`);
            const existing = await queryDatabase('SELECT id FROM deposits WHERE tx_signature = $1', [txSignature], client);
            if (existing.rowCount > 0) {
                 console.log(`${logPrefix} Verified TX ${txSignature} already exists in DB (ID: ${existing.rows[0].id}).`);
                 return { success: false, error: 'Deposit transaction already processed.', alreadyProcessed: true, depositId: existing.rows[0].id };
            } else {
                 // This is a more problematic state: INSERT didn't return ID, and ON CONFLICT didn't find it either.
                 console.error(`${logPrefix} Insert failed AND ON CONFLICT didn't find existing row for TX ${txSignature}. DB state might be inconsistent or another issue occurred.`);
                 return { success: false, error: 'Failed to record deposit and conflict resolution also failed to find it.' };
            }
        }
    } catch (err) {
        console.error(`${logPrefix} Error inserting deposit record for TX ${txSignature}:`, err.message, err.code);
        // Explicitly check for unique constraint violation if ON CONFLICT wasn't robust enough or if there's another unique key issue.
        if (err.code === '23505' && err.constraint === 'deposits_tx_signature_key') {
            console.error(`${logPrefix} Deposit TX ${txSignature} already processed (Direct unique constraint violation). Verifying existing ID...`);
            const existing = await queryDatabase('SELECT id FROM deposits WHERE tx_signature = $1', [txSignature], client);
            const existingId = existing.rows[0]?.id;
            return { success: false, error: 'Deposit transaction already processed (unique constraint).', alreadyProcessed: true, depositId: existingId };
        }
        return { success: false, error: `Database error recording deposit (Code: ${err.code || 'N/A'})` };
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
            [userId] // Use default pool for this non-transactional query
        );
        const nextIndex = parseInt(res.rows[0]?.count || '0', 10);
        return nextIndex;
    } catch (err) {
        console.error(`${logPrefix} Error fetching count:`, err);
        // This error is critical for deposit address generation.
        throw new Error(`Failed to determine next deposit address index for user ${userId}: ${err.message}`);
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
    // The final_send_amount_lamports is the amount the user will RECEIVE.
    // The fee is deducted from their balance IN ADDITION to requestedAmountLamports when the withdrawal is initiated.
    // So, if user requests 1 SOL, and fee is 0.005 SOL, their balance is debited 1.005 SOL, and 1 SOL is sent.
    const finalSendAmount = requestedAmount; // This is what sendSol will attempt to send.

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
            console.error(`[DB CreateWithdrawal] Failed to insert withdrawal request for user ${userId} (no ID returned).`);
            return { success: false, error: 'Failed to insert withdrawal request (no ID returned).' };
        }
    } catch (err) {
        console.error(`[DB CreateWithdrawal] Error for user ${userId}:`, err.message, err.code);
        return { success: false, error: `Database error creating withdrawal (Code: ${err.code || 'N/A'})` };
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
    let valueCounter = 2; // Start parameter index from $2

    switch (status) {
        case 'processing':
            query += `, processed_at = NOW(), completed_at = NULL, error_message = NULL, payout_tx_signature = NULL`; // Clear previous error/sig
            break;
        case 'completed':
            if (!signature) {
                console.error(`${logPrefix} Signature required for 'completed' status.`);
                return false; // Critical: signature is needed for completed state
            }
            query += `, payout_tx_signature = $${valueCounter++}, completed_at = NOW(), error_message = NULL, processed_at = COALESCE(processed_at, NOW())`;
            values.push(signature);
            break;
        case 'failed':
            const safeErrorMessage = (errorMessage || 'Unknown error').substring(0, 500); // Truncate error message
            query += `, error_message = $${valueCounter++}, completed_at = NULL, payout_tx_signature = NULL, processed_at = COALESCE(processed_at, NOW())`;
            values.push(safeErrorMessage);
            break;
        default:
            console.error(`${logPrefix} Invalid status "${status}" provided.`);
            return false;
    }

    // Prevent updates on already terminal states unless transitioning *to* a different terminal state (which isn't typical here)
    // The main protection is trying to update *from* non-terminal states.
    query += ` WHERE id = $${valueCounter++} AND status NOT IN ('completed', 'failed') RETURNING id;`;
    values.push(withdrawalId);

    try {
        const res = await queryDatabase(query, values, db);
        if (res.rowCount > 0) {
            return true; // Update successful
        } else {
             // Check if it failed because it was *already* in the desired terminal state or a different terminal state
             const currentStatusRes = await queryDatabase('SELECT status FROM withdrawals WHERE id = $1', [withdrawalId], db);
             const currentStatus = currentStatusRes.rows[0]?.status;
             if (currentStatusRes.rowCount === 0) {
                console.warn(`${logPrefix} Record ID ${withdrawalId} not found for status update.`);
                return false;
             }

             if (currentStatus === status && (status === 'completed' || status === 'failed')) {
                 console.log(`${logPrefix} Record already in target terminal state '${currentStatus}'. Treating update as successful no-op.`);
                 return true; // Idempotency: Already in the target state
             } else if (currentStatus === 'completed' || currentStatus === 'failed') {
                 console.warn(`${logPrefix} Failed to update. Record ID ${withdrawalId} is already in a different terminal state ('${currentStatus}') and cannot be changed to '${status}'.`);
                 return false; // Cannot change from one terminal state to another via this logic.
             } else {
                console.warn(`${logPrefix} Failed to update status. Record not found or status was not updatable from '${currentStatus || 'NOT_FOUND'}' to '${status}'.`);
                return false; // Failed for other reasons (e.g. status was 'processing' and tried to set to 'processing' again without effect)
             }
        }
    } catch (err) {
        console.error(`${logPrefix} Error updating status:`, err.message, err.code);
        if (err.code === '23505' && err.constraint === 'withdrawals_payout_tx_signature_key') {
             console.error(`${logPrefix} CRITICAL - Withdrawal TX Signature ${signature?.slice(0,10)}... unique constraint violation. This implies the signature was already used for another withdrawal. This should not happen if signatures are unique. Treating as if already completed to avoid retry loops.`);
             // This is a tricky situation. If it's a true duplicate signature for *this* withdrawalId, it's an internal error.
             // If it's a duplicate signature from *another* withdrawal, that's a major issue.
             // For now, assume it means this withdrawal was somehow processed with this sig.
             return true; // Treat as "already done" to break potential loops. Manual investigation needed.
        }
        return false; // Other error occurred
    }
}

/** Fetches withdrawal details for processing. */
async function getWithdrawalDetails(withdrawalId) {
    try {
        const res = await queryDatabase('SELECT * FROM withdrawals WHERE id = $1', [withdrawalId]); // Use default pool
        if (res.rowCount === 0) {
            console.warn(`[DB GetWithdrawal] Withdrawal ID ${withdrawalId} not found.`);
            return null;
        }
        const details = res.rows[0];
        // Convert numeric fields from string (if they are) to BigInt
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
 * @param {object} betDetails - Game specific choices/bets (JSONB). Can be null.
 * @param {bigint} wagerAmountLamports - Amount deducted from internal balance.
 * @param {number} [priority=0] Bet priority.
 * @returns {Promise<{success: boolean, betId?: number, error?: string}>}
 */
async function createBetRecord(client, userId, chatId, gameType, betDetails, wagerAmountLamports, priority = 0) {
    userId = String(userId);
    chatId = String(chatId);
    const wagerAmount = BigInt(wagerAmountLamports);

    if (wagerAmount <= 0n) { // This check should align with the DB constraint
        return { success: false, error: 'Wager amount must be positive.' };
    }

    const query = `
        INSERT INTO bets (user_id, chat_id, game_type, bet_details, wager_amount_lamports, status, created_at, priority)
        VALUES ($1, $2, $3, $4, $5, 'active', NOW(), $6)
        RETURNING id;
    `;
    try {
        // Ensure betDetails is either a valid JSON object or null, not undefined or empty object that becomes invalid JSONB.
        const validBetDetails = (betDetails && typeof betDetails === 'object' && Object.keys(betDetails).length > 0) ? betDetails : null;
        const res = await queryDatabase(query, [userId, chatId, gameType, validBetDetails, wagerAmount, priority], client);
        if (res.rowCount > 0 && res.rows[0].id) {
            return { success: true, betId: res.rows[0].id };
        } else {
            console.error(`[DB CreateBet] Failed to insert bet record for user ${userId}, game ${gameType} (no ID returned).`);
            return { success: false, error: 'Failed to insert bet record (no ID returned).' };
        }
    } catch (err) {
        console.error(`[DB CreateBet] Error for user ${userId}, game ${gameType}:`, err.message, err.code);
        if (err.constraint === 'bets_wager_amount_lamports_check') { // Matches schema
            console.error(`[DB CreateBet] Positive wager constraint (bets_wager_amount_lamports_check) violation.`);
            return { success: false, error: 'Wager amount must be positive (DB check failed).' };
        }
        return { success: false, error: `Database error creating bet (Code: ${err.code || 'N/A'})` };
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
        if (payoutAmountLamports === null || BigInt(payoutAmountLamports) < 0n) { // Payout can be 0 for push if wager was 0 (though wager should be >0)
            console.error(`${logPrefix} Invalid or missing payout amount (${payoutAmountLamports}) for status ${status}.`);
            return false;
        }
        finalPayout = BigInt(payoutAmountLamports);
    } else if (status === 'completed_loss' || status === 'error_game_logic') {
        finalPayout = 0n; // For loss or error, payout is 0 (original wager is lost)
    } else if (status === 'processing_game') {
        finalPayout = null; // No payout amount yet
    } else {
        console.error(`${logPrefix} Invalid status provided: ${status}`);
        return false;
    }

    // Update only if current status allows transition (e.g., from 'active' or 'processing_game')
    // Prevent re-processing a bet that's already completed or errored out.
    const query = `
        UPDATE bets
        SET status = $1,
            payout_amount_lamports = $2,
            processed_at = CASE
                               WHEN $1 LIKE 'completed_%' OR $1 = 'error_game_logic' THEN NOW()
                               ELSE processed_at -- Keep existing processed_at if just moving to 'processing_game'
                           END
        WHERE id = $3 AND status IN ('active', 'processing_game')
        RETURNING id, status;
    `;
    try {
        const res = await queryDatabase(query, [status, finalPayout, betId], client);
        if (res.rowCount > 0) {
            return true; // Update successful
        } else {
            // Check if already in the desired state (idempotency for terminal states)
            const currentStatusRes = await queryDatabase('SELECT status FROM bets WHERE id = $1', [betId], client);
            const currentStatus = currentStatusRes.rows[0]?.status;

            if (currentStatusRes.rowCount === 0) {
                console.warn(`${logPrefix} Bet ID ${betId} not found during status update.`);
                return false;
            }

            if (currentStatus === status && (status.startsWith('completed_') || status === 'error_game_logic')) {
                console.log(`${logPrefix} Bet already in target terminal state '${currentStatus}'. Treating as successful no-op.`);
                return true;
            }
            console.warn(`${logPrefix} Failed to update status. Bet ID ${betId} not found or already in a non-updatable state (current: '${currentStatus || 'NOT_FOUND'}'). Target: '${status}'.`);
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
 * @param {import('pg').PoolClient} [client=pool] Optional DB client.
 * @returns {Promise<boolean>} True if it's the first completed bet, false otherwise or on error.
 */
async function isFirstCompletedBet(userId, betId, client = pool) { // Added client parameter
    userId = String(userId);
    const query = `
        SELECT id
        FROM bets
        WHERE user_id = $1 AND status LIKE 'completed_%' -- Matches 'completed_win', 'completed_loss', 'completed_push'
        ORDER BY created_at ASC, id ASC -- Ensure consistent ordering for the very first bet
        LIMIT 1;
    `;
    try {
        const res = await queryDatabase(query, [userId], client); // Use provided client or default pool
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
        if (res.rowCount === 0) {
            console.warn(`[DB GetBetDetails] Bet ID ${betId} not found.`);
            return null;
        }
        const details = res.rows[0];
        details.wager_amount_lamports = BigInt(details.wager_amount_lamports || '0');
        details.payout_amount_lamports = details.payout_amount_lamports !== null ? BigInt(details.payout_amount_lamports) : null;
        details.priority = parseInt(details.priority || '0', 10);
        details.bet_details = details.bet_details || null; // Ensure it's null if DB returns null, or parsed if JSON
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
        let paramCount = 1; // Start with $1 for userId

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
            bet_details: row.bet_details || {}, // Ensure bet_details is an object, even if empty
        }));
    } catch (err) {
        console.error(`${logPrefix} Error fetching bet history:`, err.message);
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
 * @param {import('pg').PoolClient | null} [client=null] Optional DB client if called within a transaction. Defaults to pool.
 * @returns {Promise<{success: boolean, payoutId?: number, error?: string, duplicate?: boolean}>} `duplicate:true` if it was a milestone conflict.
 */
async function recordPendingReferralPayout(referrerUserId, refereeUserId, payoutType, payoutAmountLamports, triggeringBetId = null, milestoneReachedLamports = null, client = null) {
    const db = client || pool;
    referrerUserId = String(referrerUserId);
    refereeUserId = String(refereeUserId);
    const payoutAmount = BigInt(payoutAmountLamports);
    // Ensure null if not provided or invalid for DB
    const milestoneLamports = (milestoneReachedLamports !== null && BigInt(milestoneReachedLamports) >= 0n) ? BigInt(milestoneReachedLamports) : null;
    const betId = (Number.isInteger(triggeringBetId) && triggeringBetId > 0) ? triggeringBetId : null;
    const logPrefix = `[DB RecordRefPayout Ref:${referrerUserId} Type:${payoutType}]`;

    const query = `
        INSERT INTO referral_payouts (
            referrer_user_id, referee_user_id, payout_type, payout_amount_lamports,
            triggering_bet_id, milestone_reached_lamports, status, created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, 'pending', NOW())
        ON CONFLICT ON CONSTRAINT idx_refpayout_unique_milestone DO NOTHING
        RETURNING id;
    `;
    // Note: idx_refpayout_unique_milestone is defined in Part 6 on (referrer_user_id, referee_user_id, payout_type, milestone_reached_lamports) WHERE payout_type = 'milestone'
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
             // ON CONFLICT triggered or insert failed for other reasons.
             // If it was a milestone conflict, verify it.
             if (payoutType === 'milestone') {
                 const checkExisting = await queryDatabase(`
                     SELECT id FROM referral_payouts
                     WHERE referrer_user_id = $1 AND referee_user_id = $2 AND payout_type = 'milestone' AND milestone_reached_lamports = $3
                 `, [referrerUserId, refereeUserId, milestoneLamports], db);
                 if (checkExisting.rowCount > 0) {
                     console.warn(`${logPrefix} Duplicate milestone payout attempt for milestone ${milestoneLamports}. Ignored (ON CONFLICT or already exists). Payout ID: ${checkExisting.rows[0].id}`);
                     return { success: false, error: 'Duplicate milestone payout attempt.', duplicate: true, payoutId: checkExisting.rows[0].id };
                 }
             }
            console.error(`${logPrefix} Failed to insert pending referral payout, RETURNING clause gave no ID (and not a handled milestone conflict). This might indicate a different issue or constraint failure.`);
            return { success: false, error: "Failed to insert pending referral payout and not a recognized duplicate." };
        }
    } catch (err) {
        console.error(`${logPrefix} Error recording pending payout (triggered by ${refereeUserId}):`, err.message, err.code);
        return { success: false, error: `Database error recording referral payout (Code: ${err.code || 'N/A'})` };
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
            query += `, processed_at = NOW(), paid_at = NULL, error_message = NULL, payout_tx_signature = NULL`;
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

    query += ` WHERE id = $${valueCounter++} AND status NOT IN ('paid', 'failed') RETURNING id;`;
    values.push(payoutId);

    try {
        const res = await queryDatabase(query, values, db);
        if (res.rowCount > 0) {
            return true; // Update successful
        } else {
            const currentStatusRes = await queryDatabase('SELECT status FROM referral_payouts WHERE id = $1', [payoutId], db);
            const currentStatus = currentStatusRes.rows[0]?.status;

            if (currentStatusRes.rowCount === 0) {
                console.warn(`${logPrefix} Record ID ${payoutId} not found for status update.`);
                return false;
            }

            if (currentStatus === status && (status === 'paid' || status === 'failed')) {
                console.log(`${logPrefix} Record already in terminal state '${currentStatus}'. Treating as successful no-op.`);
                return true; // Idempotency
            } else if (currentStatus === 'paid' || currentStatus === 'failed') {
                console.warn(`${logPrefix} Failed to update. Record ID ${payoutId} is already in a different terminal state ('${currentStatus}') and cannot be changed to '${status}'.`);
                return false;
            }
            console.warn(`${logPrefix} Failed to update status. Record not found or status was not updatable from '${currentStatus || 'NOT_FOUND'}' to '${status}'.`);
            return false;
        }
    } catch (err) {
        console.error(`${logPrefix} Error updating status:`, err.message, err.code);
        if (err.code === '23505' && err.constraint === 'referral_payouts_payout_tx_signature_key') {
            console.error(`${logPrefix} CRITICAL - Referral Payout TX Signature ${signature?.slice(0,10)}... unique constraint violation.`);
            return true; // Treat as "already done" to avoid loops. Needs investigation.
        }
        return false;
    }
}

/** Fetches referral payout details for processing. */
async function getReferralPayoutDetails(payoutId) {
   try {
        const res = await queryDatabase('SELECT * FROM referral_payouts WHERE id = $1', [payoutId]); // Use default pool
        if (res.rowCount === 0) {
            console.warn(`[DB GetRefPayout] Payout ID ${payoutId} not found.`);
            return null;
        }
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
        return 0n; // Return 0 on error
    }
}

// --- Jackpot Operations ---

/**
 * Ensures a jackpot record exists for the given gameKey, creating it with seed if not.
 * Typically called during bot initialization or before first jackpot interaction.
 * @param {string} gameKey The key of the game (e.g., 'slots').
 * @param {bigint} seedAmountLamports The initial amount if jackpot needs to be created.
 * @param {import('pg').PoolClient} [client=pool] Optional DB client.
 * @returns {Promise<boolean>} True if ensured/created, false on error.
 */
async function ensureJackpotExists(gameKey, seedAmountLamports, client = pool) {
    const logPrefix = `[DB ensureJackpotExists Game:${gameKey}]`;
    try {
        // Try to insert, and if it conflicts (already exists), do nothing.
        // This is safer than SELECT then INSERT in concurrent environments.
        const insertQuery = `
            INSERT INTO jackpots (game_key, current_amount_lamports, last_updated_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (game_key) DO NOTHING;
        `;
        await queryDatabase(insertQuery, [gameKey, seedAmountLamports], client);
        // If the row already existed, ON CONFLICT DO NOTHING ensures no error.
        // If it didn't exist, it's created.
        console.log(`${logPrefix} Ensured jackpot exists (created if new, or did nothing if existing).`);
        return true;
    } catch (err) {
        // This catch block is for unexpected errors during the INSERT query itself,
        // not for the ON CONFLICT case which is handled gracefully by the DB.
        console.error(`${logPrefix} Error ensuring jackpot exists for game ${gameKey}:`, err.message, err.code);
        return false;
    }
}

/**
 * Gets the current amount of a specific jackpot.
 * @param {string} gameKey The key of the game (e.g., 'slots').
 * @param {import('pg').PoolClient} [client=pool] Optional DB client.
 * @returns {Promise<bigint>} Current jackpot amount in lamports, or 0n if not found/error (after trying to ensure it).
 */
async function getJackpotAmount(gameKey, client = pool) {
    const logPrefix = `[DB getJackpotAmount Game:${gameKey}]`;
    try {
        const res = await queryDatabase('SELECT current_amount_lamports FROM jackpots WHERE game_key = $1', [gameKey], client);
        if (res.rowCount > 0) {
            return BigInt(res.rows[0].current_amount_lamports);
        } else {
            // Jackpot not found, implies ensureJackpotExists might not have run or failed.
            // Try to ensure it exists now with a default seed (e.g., from GAME_CONFIG or a global const)
            console.warn(`${logPrefix} Jackpot for game ${gameKey} not found. Attempting to ensure it with seed.`);
            const gameConfigForJackpot = GAME_CONFIG[gameKey];
            const seedForMissingJackpot = gameConfigForJackpot?.jackpotSeedLamports || SLOTS_JACKPOT_SEED_LAMPORTS || 0n; // Fallback chain

            const ensured = await ensureJackpotExists(gameKey, seedForMissingJackpot, client);
            if (ensured) {
                // Try fetching again
                const resAgain = await queryDatabase('SELECT current_amount_lamports FROM jackpots WHERE game_key = $1', [gameKey], client);
                if (resAgain.rowCount > 0) return BigInt(resAgain.rows[0].current_amount_lamports);
            }
            console.error(`${logPrefix} Jackpot still not found for game ${gameKey} after trying to ensure. Returning 0.`);
            return 0n;
        }
    } catch (err) {
        console.error(`${logPrefix} Error fetching jackpot amount for game ${gameKey}:`, err.message);
        return 0n; // Return 0 on error
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
        // Lock the row for update first to prevent race conditions
        await queryDatabase('SELECT game_key FROM jackpots WHERE game_key = $1 FOR UPDATE', [gameKey], client);
        const res = await queryDatabase(
            'UPDATE jackpots SET current_amount_lamports = $1, last_updated_at = NOW() WHERE game_key = $2',
            [newAmountLamports, gameKey],
            client
        );
        if (res.rowCount === 0) {
            console.error(`${logPrefix} Failed to update jackpot amount (game_key ${gameKey} not found or lock failed?). Ensure jackpot is seeded first.`);
            return false;
        }
        return true;
    } catch (err) {
        console.error(`${logPrefix} Error updating jackpot amount for game ${gameKey}:`, err.message);
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
    if (contributionLamports <= 0n) {
        // console.log(`${logPrefix} Contribution is ${contributionLamports}, no increment needed.`);
        return true; // No change needed
    }

    try {
        // Lock the row for update
        await queryDatabase('SELECT game_key FROM jackpots WHERE game_key = $1 FOR UPDATE', [gameKey], client);
        const res = await queryDatabase(
            'UPDATE jackpots SET current_amount_lamports = current_amount_lamports + $1, last_updated_at = NOW() WHERE game_key = $2',
            [contributionLamports, gameKey],
            client
        );
        if (res.rowCount === 0) {
            console.error(`${logPrefix} Failed to increment jackpot amount (game_key ${gameKey} not found or lock failed?). Ensure jackpot is seeded first.`);
            return false;
        }
        return true;
    } catch (err) {
        console.error(`${logPrefix} Error incrementing jackpot amount for game ${gameKey}:`, err.message);
        return false;
    }
}

// --- End of Part 2 ---
// index.js - Part 3: Solana Utilities & Telegram Helpers
// --- VERSION: 3.2.1 --- (Adjusted based on discussions for correctness)

// --- Assuming imports from Part 1 are available ---
// (PublicKey, Keypair, SystemProgram, LAMPORTS_PER_SOL, sendAndConfirmTransaction, ComputeBudgetProgram, TransactionExpiredBlockheightExceededError, SendTransactionError, Connection)
// (bs58, createHash, crypto, bip39, derivePath)
// --- Assuming 'bot', 'solanaConnection', various cache maps, constants like 'MAX_PROCESSED_TX_CACHE_SIZE', 'WALLET_CACHE_TTL_MS', 'SOL_DECIMALS', 'DEPOSIT_CONFIRMATION_LEVEL', 'BOT_VERSION', etc. are available from Part 1 ---
// --- 'sleep' and 'escapeMarkdownV2' are defined in Part 1 and used here. ---

// --- General Utilities ---

/**
 * Formats lamports into a SOL string, removing unnecessary trailing zeros.
 * @param {bigint | number | string} lamports The amount in lamports.
 * @param {number} [maxDecimals=SOL_DECIMALS] Maximum decimals to initially format to before trimming. SOL_DECIMALS is from Part 1.
 * @returns {string} Formatted SOL string (e.g., "0.01", "1.2345", "1").
 */
function formatSol(lamports, maxDecimals = SOL_DECIMALS) {
    if (typeof lamports === 'undefined' || lamports === null) return '0';
    try {
        // Convert input to BigInt safely
        const lamportsBigInt = BigInt(lamports);
        // Convert to SOL number
        const solNumber = Number(lamportsBigInt) / Number(LAMPORTS_PER_SOL); // LAMPORTS_PER_SOL from Part 1

        // Use toFixed for initial rounding to avoid floating point issues with many decimals
        // Ensure maxDecimals is reasonable
        const effectiveMaxDecimals = Math.min(Math.max(0, maxDecimals), SOL_DECIMALS + 2); // Cap reasonably
        const fixedString = solNumber.toFixed(effectiveMaxDecimals);

        // parseFloat will strip trailing zeros after the decimal point.
        // toString() converts back (handles integers correctly, e.g., 1.00 -> "1")
        // If the number is very small (e.g. 0.000000001), toFixed might give "0.00000000", parseFloat makes it "0".
        // We want to preserve precision for small numbers if they are non-zero.
        const parsedNum = parseFloat(fixedString);
        if (parsedNum === 0 && solNumber !== 0) { // Avoid converting small non-zero to "0"
            // For very small numbers that parse to 0, try to find the first significant digit.
            // This is a simple attempt; a more robust solution might use a library for arbitrary-precision arithmetic display.
            if (fixedString.match(/^0\.0*[1-9]/)) {
                return fixedString.replace(/0+$/, ''); // Remove only trailing zeros after decimal part
            }
        }
        return parsedNum.toString();

    } catch (e) {
        console.error(`[formatSol] Error formatting lamports: ${lamports}`, e);
        return 'NaN'; // Return 'NaN' string to indicate error
    }
}


// --- Solana Utilities --

// Helper function to create safe index for BIP-44 hardened derivation
// Uses import { createHash } from 'crypto' (available from Part 1).
function createSafeIndex(userId) {
    // Hash the userId to fixed-size output
    const hash = createHash('sha256') // createHash from 'crypto' (Part 1)
        .update(String(userId))
        .digest();

    // Take last 4 bytes and convert to UInt32BE (always < 2^32)
    // Ensure the hash length is at least 4 bytes. SHA256 produces 32 bytes, so this is safe.
    const index = hash.readUInt32BE(hash.length - 4);

    // Ensure it's below hardening threshold (2^31 - 1 for hardened, or just ensure it's a valid non-hardened index part if we add 2^31 later)
    // For SLIP-10, hardened indices are >= 0x80000000 (2^31).
    // The path `m/44'/501'/${safeIndex}'/0'/${addressIndex}'` uses hardened `safeIndex`.
    // So, `safeIndex` itself should be a value that, when added to 0x80000000, doesn't overflow.
    // More simply, `safeIndex` should be < 2^31. The apostrophe denotes hardening.
    return index % 2147483648; // 0 <= index < 2^31 (0x80000000)
}

/**
 * Derives a unique Solana keypair for deposits based on user ID and an index.
 * Uses BIP39 seed phrase and SLIP-10 path (m/44'/501'/safeIndex'/0'/addressIndex').
 * Uses ed25519-hd-key library.
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
        if (typeof seedPhrase !== 'string' || seedPhrase.length < 20 || !bip39.validateMnemonic(seedPhrase)) { // bip39 from Part 1
            console.error(`${logPrefix} CRITICAL: DEPOSIT_MASTER_SEED_PHRASE is invalid or empty/invalid at runtime!`);
            // notifyAdmin is defined later in this Part.
            // We should use a try-catch or ensure it's available if called here.
            // For now, just log, as this function itself might be called before notifyAdmin is fully usable during very early startup errors.
            // If called later, notifyAdmin should work.
            if (typeof notifyAdmin === "function") {
                 await notifyAdmin(`🚨 CRITICAL: DEPOSIT\\_MASTER\\_SEED\\_PHRASE is missing or invalid\\. Cannot generate deposit addresses\\. (User: ${escapeMarkdownV2(userId)}, Index: ${addressIndex})`);
            } else {
                console.error(`[ADMIN ALERT during generateUniqueDepositAddress] DEPOSIT_MASTER_SEED_PHRASE missing/invalid.`);
            }
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

        const safeUserIndex = createSafeIndex(userId); // This is the account index in BIP44
        // Path: m / purpose' / coin_type' / account' / change / address_index'
        // Solana SLIP-0010 uses m/44'/501'/account'/0'/address_index' (note: address_index is also hardened here)
        // Or sometimes m/44'/501'/account'/0'/address_index (non-hardened address_index)
        // The original code used: `m/44'/501'/${safeIndex}'/0'/${addressIndex}'` where safeIndex is based on userId.
        // Let's stick to the user's path convention. safeIndex is the "account" derived from userId.
        const derivationPath = `m/44'/501'/${safeUserIndex}'/0'/${addressIndex}'`;


        const derivedSeedNode = derivePath(derivationPath, masterSeedBuffer.toString('hex')); // derivePath from ed25519-hd-key (Part 1)
        const privateKeyBytes = derivedSeedNode.key; // This should be the 32-byte seed for the Keypair

        if (!privateKeyBytes || privateKeyBytes.length !== 32) {
            throw new Error(`Derived private key (seed bytes) are invalid (length: ${privateKeyBytes?.length}) using ed25519-hd-key for path ${derivationPath}`);
        }

        // For Solana, the derived key from ed25519-hd-key is the *seed* for Keypair.fromSeed().
        const keypair = Keypair.fromSeed(privateKeyBytes); // Keypair from @solana/web3.js (Part 1)
        const publicKey = keypair.publicKey;

        return {
            publicKey: publicKey,
            privateKeyBytes: privateKeyBytes, // This is the seed, not the expanded secret key. Keypair.secretKey would be the 64-byte version.
                                            // For consistency, if other parts expect the 64-byte secret, adjust here or there.
                                            // However, Keypair.fromSeed() is the standard.
            derivationPath: derivationPath
        };

    } catch (error) {
        console.error(`${logPrefix} Error during address generation: ${error.message}`);
        console.error(`${logPrefix} Error Stack: ${error.stack}`);
        if (typeof notifyAdmin === "function") {
            await notifyAdmin(`🚨 ERROR generating deposit address for User ${escapeMarkdownV2(userId)} Index ${addressIndex} (${escapeMarkdownV2(logPrefix)}): ${escapeMarkdownV2(error.message)}`);
        } else {
            console.error(`[ADMIN ALERT during generateUniqueDepositAddress] Error: ${error.message}`);
        }
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
            console.error(`${logPrefix} Invalid derivation path format provided: ${derivationPath}`);
            return null;
        }

        const seedPhrase = process.env.DEPOSIT_MASTER_SEED_PHRASE;
        if (typeof seedPhrase !== 'string' || seedPhrase.length < 20 || !bip39.validateMnemonic(seedPhrase)) { // bip39 from Part 1
            console.error(`${logPrefix} CRITICAL: DEPOSIT_MASTER_SEED_PHRASE is missing or invalid when trying to re-derive key!`);
            // No async notifyAdmin here as this is synchronous.
            return null;
        }

        const masterSeedBuffer = bip39.mnemonicToSeedSync(seedPhrase);
        if (!masterSeedBuffer || masterSeedBuffer.length === 0) {
            throw new Error("Failed to generate seed buffer from mnemonic for path derivation.");
        }

        const derivedSeedNode = derivePath(derivationPath, masterSeedBuffer.toString('hex')); // derivePath from ed25519-hd-key (Part 1)
        const privateKeySeedBytes = derivedSeedNode.key; // This is the 32-byte seed

        if (!privateKeySeedBytes || privateKeySeedBytes.length !== 32) {
            throw new Error(`Derived private key seed bytes are invalid (length: ${privateKeySeedBytes?.length}) for path ${derivationPath}`);
        }

        const keypair = Keypair.fromSeed(privateKeySeedBytes); // Keypair from @solana/web3.js (Part 1)
        return keypair;

    } catch (error) {
        console.error(`${logPrefix} Error re-deriving keypair from path ${derivationPath}: ${error.message}`);
        // Avoid logging seed phrase related errors to admin unless absolutely necessary and secured.
        if (!error.message.toLowerCase().includes('deposit_master_seed_phrase')) {
            console.error(`[ADMIN ALERT POTENTIAL] ${logPrefix} Unexpected error re-deriving key: ${error.message}. Path: ${derivationPath}`);
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

    // Handle specific error instances from @solana/web3.js
    if (error instanceof TransactionExpiredBlockheightExceededError) { // from Part 1
        return true;
    }
    // If SendTransactionError has a cause, analyze the cause.
    if (error instanceof SendTransactionError && error.cause) { // from Part 1
       return isRetryableSolanaError(error.cause);
    }

    // Check error messages (case-insensitive)
    const message = String(error.message || '').toLowerCase();
    const retryableMessages = [
        'timeout', 'timed out', 'econnreset', 'esockettimedout', 'network error',
        'fetch', 'socket hang up', 'connection terminated', 'econnrefused', 'failed to fetch',
        'getaddrinfo enotfound', 'connection refused', 'connection reset by peer', 'etimedout',
        'transaction simulation failed', // Often retryable if due to temp node issues
        'failed to simulate transaction', // Same as above
        'blockhash not found', 'slot leader does not match', 'node is behind',
        'transaction was not confirmed', // Can be retryable, or may need checking
        'block not available', 'block cleanout',
        'sending transaction', 'connection closed', 'load balancer error',
        'backend unhealthy', 'overloaded', 'proxy internal error', 'too many requests',
        'rate limit exceeded', 'unknown block', 'leader not ready', 'heavily throttled',
        'failed to query long-term storage', 'rpc node error', 'temporarily unavailable',
        'service unavailable'
    ];
    if (retryableMessages.some(m => message.includes(m))) return true;

    // Check HTTP status codes if present (often in error.response.status or error.status or error.statusCode)
    const status = error?.response?.status || error?.statusCode || error?.status;
    if (status && [408, 429, 500, 502, 503, 504].includes(Number(status))) return true;

    // Check error codes (often in error.code or error.cause.code or error.errorCode)
    const code = error?.code || error?.cause?.code;
    const errorCode = error?.errorCode; // Another place some libraries put it

    const retryableErrorCodesStrings = [ // String codes
        'ETIMEDOUT', 'ECONNRESET', 'ENETUNREACH', 'EAI_AGAIN', 'ECONNABORTED',
        'ECONNREFUSED', 'UND_ERR_CONNECT_TIMEOUT', 'UND_ERR_HEADERS_TIMEOUT',
        'UND_ERR_BODY_TIMEOUT', 'FETCH_TIMEOUT', 'FETCH_ERROR',
        'SLOT_LEADER_NOT_READY', // Some RPCs might return this as a string code
    ];
    const retryableErrorCodesNumbers = [ // Numeric codes (often RPC specific, e.g. JSON RPC errors)
        -32000, -32001, -32002, -32003, -32004, -32005, // Common JSON-RPC server error range
    ];

    if (typeof code === 'string' && retryableErrorCodesStrings.includes(code.toUpperCase())) return true;
    if (typeof code === 'number' && retryableErrorCodesNumbers.includes(code)) return true;
    if (typeof errorCode === 'string' && retryableErrorCodesStrings.includes(errorCode.toUpperCase())) return true;
    if (typeof errorCode === 'number' && retryableErrorCodesNumbers.includes(errorCode)) return true;


    // Check for specific error reasons some libraries might use
    if (error?.reason === 'rpc_error' || error?.reason === 'network_error') return true;

    // Check nested error data messages (e.g., from some RPC wrappers)
    if (error?.data?.message?.toLowerCase().includes('rate limit exceeded') ||
        error?.data?.message?.toLowerCase().includes('too many requests')) {
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
        recipientPubKey = (typeof recipientPublicKey === 'string') ? new PublicKey(recipientPublicKey) : recipientPublicKey; // PublicKey from Part 1
        if (!(recipientPubKey instanceof PublicKey)) throw new Error("Invalid recipient public key type");
    } catch (e) {
        const errorMsg = `Invalid recipient address format: "${recipientPublicKey}". Error: ${e.message}`;
        console.error(`[${operationId}] ❌ ERROR: ${errorMsg}`);
        return { success: false, error: errorMsg, isRetryable: false };
    }

    let amountToSend;
    try {
        amountToSend = BigInt(amountLamports);
        if (amountToSend <= 0n) throw new Error('Amount to send must be positive.');
    } catch (e) {
        const errorMsg = `Invalid amountLamports value: '${amountLamports}'. Error: ${e.message}`;
        console.error(`[${operationId}] ❌ ERROR: ${errorMsg}`);
        return { success: false, error: errorMsg, isRetryable: false };
    }

    let privateKeyEnvVar;
    let keyTypeForLog;
    // Use referral payout key if specified and available, otherwise default to main bot key
    if (payoutSource === 'referral' && process.env.REFERRAL_PAYOUT_PRIVATE_KEY) {
        privateKeyEnvVar = 'REFERRAL_PAYOUT_PRIVATE_KEY';
        keyTypeForLog = 'REFERRAL';
    } else {
        privateKeyEnvVar = 'MAIN_BOT_PRIVATE_KEY';
        keyTypeForLog = (payoutSource === 'referral' && !process.env.REFERRAL_PAYOUT_PRIVATE_KEY)
            ? 'MAIN (Defaulted for Referral)'
            : 'MAIN';
    }

    const privateKeyBase58 = process.env[privateKeyEnvVar];
    if (!privateKeyBase58) {
        const errorMsg = `Missing or empty private key environment variable ${privateKeyEnvVar} for payout source ${payoutSource} (Key Type: ${keyTypeForLog}).`;
        console.error(`[${operationId}] ❌ ERROR: ${errorMsg}`);
        if (typeof notifyAdmin === "function") {
             await notifyAdmin(`🚨 CRITICAL: Missing private key env var ${escapeMarkdownV2(privateKeyEnvVar)} for payout source ${payoutSource}\\. Payout failed\\. Operation ID: ${escapeMarkdownV2(operationId)}`);
        }
        return { success: false, error: errorMsg, isRetryable: false };
    }

    let payerWallet;
    try {
        payerWallet = Keypair.fromSecretKey(bs58.decode(privateKeyBase58)); // Keypair, bs58 from Part 1
    } catch (e) {
        const errorMsg = `Failed to decode private key ${keyTypeForLog} (from env var ${privateKeyEnvVar}): ${e.message}`;
        console.error(`[${operationId}] ❌ ERROR: ${errorMsg}`);
        if (typeof notifyAdmin === "function") {
            await notifyAdmin(`🚨 CRITICAL: Failed to decode private key ${escapeMarkdownV2(keyTypeForLog)} (${escapeMarkdownV2(privateKeyEnvVar)}): ${escapeMarkdownV2(e.message)}\\. Payout failed\\. Operation ID: ${escapeMarkdownV2(operationId)}`);
        }
        return { success: false, error: errorMsg, isRetryable: false };
    }

    const amountSOLFormatted = formatSol(amountToSend); // formatSol from this Part
    console.log(`[${operationId}] Attempting to send ${amountSOLFormatted} SOL from ${payerWallet.publicKey.toBase58()} to ${recipientPubKey.toBase58()} using ${keyTypeForLog} key...`);

    try {
        const { blockhash, lastValidBlockHeight } = await solanaConnection.getLatestBlockhash(DEPOSIT_CONFIRMATION_LEVEL); // solanaConnection, DEPOSIT_CONFIRMATION_LEVEL from Part 1
        if (!blockhash || !lastValidBlockHeight) {
            throw new Error('Failed to get valid latest blockhash object from RPC.');
        }

        const basePriorityFee = parseInt(process.env.PAYOUT_BASE_PRIORITY_FEE_MICROLAMPORTS, 10);
        const maxPriorityFee = parseInt(process.env.PAYOUT_MAX_PRIORITY_FEE_MICROLAMPORTS, 10);
        let priorityFeeMicroLamports = Math.max(1, Math.min(basePriorityFee, maxPriorityFee)); // Ensure it's within bounds and at least 1
        const computeUnitLimit = parseInt(process.env.PAYOUT_COMPUTE_UNIT_LIMIT, 10);

        const transaction = new Transaction({ // Transaction from Part 1
            recentBlockhash: blockhash,
            feePayer: payerWallet.publicKey,
        });

        // Add compute budget instructions first
        transaction.add(
            ComputeBudgetProgram.setComputeUnitLimit({ units: computeUnitLimit }) // ComputeBudgetProgram from Part 1
        );
        transaction.add(
            ComputeBudgetProgram.setComputeUnitPrice({ microLamports: priorityFeeMicroLamports })
        );
        // Then add the transfer instruction
        transaction.add(
            SystemProgram.transfer({ // SystemProgram from Part 1
                fromPubkey: payerWallet.publicKey,
                toPubkey: recipientPubKey,
                lamports: amountToSend,
            })
        );

        console.log(`[${operationId}] Sending and confirming transaction (Commitment: ${DEPOSIT_CONFIRMATION_LEVEL}, Priority Fee: ${priorityFeeMicroLamports} microLamports, CU Limit: ${computeUnitLimit})...`);
        const signature = await sendAndConfirmTransaction( // from Part 1
            solanaConnection,
            transaction,
            [payerWallet],
            {
                commitment: DEPOSIT_CONFIRMATION_LEVEL,
                skipPreflight: false, // Recommended to keep preflight for most cases
                preflightCommitment: DEPOSIT_CONFIRMATION_LEVEL,
                lastValidBlockHeight: lastValidBlockHeight,
            }
        );

        console.log(`[${operationId}] SUCCESS! ✅ Sent ${amountSOLFormatted} SOL to ${recipientPubKey.toBase58()} using ${keyTypeForLog} key. TX: ${signature}`);
        return { success: true, signature: signature };

    } catch (error) {
        console.error(`[${operationId}] ❌ SEND FAILED using ${keyTypeForLog} key. Error: ${error.message}`);
        if (error instanceof SendTransactionError && error.logs) { // SendTransactionError from Part 1
            console.error(`[${operationId}] Simulation Logs (if available, last 10):`);
            error.logs.slice(-10).forEach(log => console.error(`   -> ${log}`));
        }

        const isRetryable = isRetryableSolanaError(error); // Defined in this Part
        let userFriendlyError = `Send/Confirm error: ${error.message}`; // Default error
        const errorMsgLower = String(error.message || '').toLowerCase();

        if (errorMsgLower.includes('insufficient lamports') || errorMsgLower.includes('insufficient funds')) {
            userFriendlyError = `Insufficient funds in the ${keyTypeForLog} payout wallet (${payerWallet.publicKey.toBase58()}). Please check its balance.`;
            if (typeof notifyAdmin === "function") {
                await notifyAdmin(`🚨 CRITICAL: Insufficient funds in ${escapeMarkdownV2(keyTypeForLog)} wallet (${escapeMarkdownV2(payerWallet.publicKey.toBase58())}) for payout. Operation ID: ${escapeMarkdownV2(operationId)}`);
            }
        } else if (error instanceof TransactionExpiredBlockheightExceededError || errorMsgLower.includes('blockhash not found') || errorMsgLower.includes('block height exceeded')) {
            userFriendlyError = 'Transaction expired (blockhash invalid). Retrying may be required.';
        } else if (errorMsgLower.includes('transaction was not confirmed') || errorMsgLower.includes('timed out waiting')) {
            userFriendlyError = `Transaction confirmation timeout. Status unclear, may succeed later. Manual check advised for TX to ${recipientPubKey.toBase58()}.`;
        } else if (isRetryable) {
            userFriendlyError = `Temporary network/RPC error during send/confirm: ${error.message}`;
        }
        // If no specific user-friendly message was set, the default one with error.message stands.

        console.error(`[${operationId}] Failure details - Retryable: ${isRetryable}, User Friendly Error: "${userFriendlyError}"`);
        return { success: false, error: userFriendlyError, isRetryable: isRetryable };
    }
}


/**
 * Analyzes a fetched Solana transaction to find the SOL amount transferred *to* a target address.
 * Handles basic SystemProgram transfers by primarily analyzing balance changes.
 * NOTE: This function's accuracy might degrade with very complex transactions or future transaction versions.
 * It primarily relies on `meta.preBalances` and `meta.postBalances`.
 * @param {import('@solana/web3.js').ConfirmedTransaction | import('@solana/web3.js').TransactionResponse | null} tx The fetched transaction object.
 * @param {string} targetAddress The address receiving the funds.
 * @returns {{transferAmount: bigint, payerAddress: string | null}} Amount in lamports and the likely payer. Returns 0n if no positive transfer found or on error.
 */
function analyzeTransactionAmounts(tx, targetAddress) {
    const logPrefix = `[analyzeAmounts Addr:${targetAddress.slice(0, 6)}..]`;
    let transferAmount = 0n;
    let payerAddress = null; // Likely payer (first signer)

    if (!tx || !targetAddress) {
        console.warn(`${logPrefix} Invalid input: Transaction object or targetAddress missing.`);
        return { transferAmount: 0n, payerAddress: null };
    }
    try {
        new PublicKey(targetAddress); // Validate targetAddress format
    } catch (e) {
        console.warn(`${logPrefix} Invalid targetAddress format: ${targetAddress}. Error: ${e.message}`);
        return { transferAmount: 0n, payerAddress: null };
    }

    if (tx.meta?.err) {
        console.log(`${logPrefix} Transaction meta contains an error. Assuming zero amount transferred. Error: ${JSON.stringify(tx.meta.err)}`);
        return { transferAmount: 0n, payerAddress: null };
    }

    if (tx.meta?.preBalances && tx.meta?.postBalances && tx.transaction?.message) {
        try {
            let accountKeysList = [];
            const message = tx.transaction.message;

            // Construct the full list of account keys involved in the transaction
            // Versioned transactions (v0) store static keys in `message.accountKeys` and LUT addresses in `meta.loadedAddresses`
            // Legacy transactions store all keys in `message.accountKeys` (older versions) or `message.staticAccountKeys` (newer legacy).
            if (message.staticAccountKeys && message.staticAccountKeys.length > 0) { // Newer legacy or specific v0 cases without LUTs
                accountKeysList = message.staticAccountKeys.map(k => k.toBase58());
            } else if (message.accountKeys && message.accountKeys.length > 0) { // v0 transactions or older legacy
                accountKeysList = message.accountKeys.map(k => k.toBase58());
                 // For v0 transactions with LUTs, combine static keys with keys from loaded LUTs
                if (tx.meta.loadedAddresses) {
                    const writableLut = tx.meta.loadedAddresses.writable || [];
                    const readonlyLut = tx.meta.loadedAddresses.readonly || [];
                    accountKeysList = [...accountKeysList, ...writableLut.map(k => k.toBase58()), ...readonlyLut.map(k => k.toBase58())];
                }
            } else {
                throw new Error("Could not determine account keys from transaction message structure.");
            }


            // The preBalances and postBalances arrays correspond to the order of accountKeys
            // (static first, then loaded writable, then loaded readonly for v0 with LUTs).
            // For simplicity and broader compatibility, we'll use the accountKeysList derived above
            // and find the targetAddress index within it. This assumes pre/postBalances align with this constructed list.
            // Note: The @solana/web3.js library's `TransactionMessage`'s `accountKeys` (if versioned)
            // or `staticAccountKeys` (if legacy) should provide the correct mapping for pre/postBalances.
            // The most robust way is to use `tx.transaction.message.getAccountKeys()` if available and it resolves LUTs.
            // However, `getAccountKeys()` might need `meta.loadedAddresses` passed to it for v0.
            // For now, using the constructed uniqueAccountKeys.

            const uniqueAccountKeys = [...new Set(accountKeysList)]; // Ensure uniqueness, though order matters for balance arrays.
                                                                  // Sticking to tx.transaction.message.accountKeys for indexing if possible for direct alignment.
            let keysForBalanceLookup = message.accountKeys.map(k => k.toBase58());
            if (tx.meta.loadedAddresses) { // If LUTs are involved, the balance arrays are longer
                 keysForBalanceLookup = [
                    ...message.accountKeys.map(k => k.toBase58()),
                    ...(tx.meta.loadedAddresses.writable || []).map(k => k.toBase58()),
                    ...(tx.meta.loadedAddresses.readonly || []).map(k => k.toBase58())
                ];
            }


            const targetIndex = keysForBalanceLookup.indexOf(targetAddress);

            if (targetIndex !== -1 &&
                tx.meta.preBalances.length > targetIndex &&
                tx.meta.postBalances.length > targetIndex) {
                const preBalance = BigInt(tx.meta.preBalances[targetIndex]);
                const postBalance = BigInt(tx.meta.postBalances[targetIndex]);
                const balanceChange = postBalance - preBalance;

                if (balanceChange > 0n) {
                    transferAmount = balanceChange;
                    // Determine payer: typically the first signer if not explicitly known otherwise
                    if (message.header && message.header.numRequiredSignatures > 0 && message.accountKeys.length > 0) {
                        payerAddress = message.accountKeys[0].toBase58(); // First account is usually the fee payer
                    }
                }
            } else {
                console.warn(`${logPrefix} Target address ${targetAddress} not found in transaction account keys, or balance arrays too short. Keys: ${keysForBalanceLookup.join(', ')}`);
            }
        } catch (e) {
            console.warn(`${logPrefix} Error analyzing balance changes for TX: ${e.message}. TX structure might be unexpected.`);
            transferAmount = 0n; // Reset on error
            payerAddress = null;
        }
    } else {
        console.log(`${logPrefix} Balance change analysis skipped: Meta or message data missing/incomplete for TX.`);
    }

    // Fallback: Check Log Messages (Less reliable, but can sometimes catch simple transfers if balance analysis fails)
    // This is a very basic check and might misinterpret complex interactions.
    if (transferAmount === 0n && tx.meta?.logMessages) {
        const sysTransferRegex = /Program(?:.*System Program|.*11111111111111111111111111111111) invoke \[\d+\]\s+Program log: Transfer: src=([1-9A-HJ-NP-Za-km-z]{32,44}) dst=([1-9A-HJ-NP-Za-km-z]{32,44}) lamports=(\d+)/;
        // A more generic check for any instruction logging a transfer to the targetAddress.
        const generalTransferToTargetRegex = new RegExp(`Transfer(?:.*)to ${targetAddress}(?:.*)lamports=(\\d+)`, 'i');


        for (const log of tx.meta.logMessages) {
            const match = log.match(sysTransferRegex);
            if (match && match[2] === targetAddress) {
                const potentialAmount = BigInt(match[3]);
                if (potentialAmount > 0n) {
                    console.log(`${logPrefix} Found potential transfer via SystemProgram log message: ${formatSol(potentialAmount)} SOL from ${match[1]}`);
                    transferAmount = potentialAmount;
                    payerAddress = match[1]; // Source from the log
                    break; // Take the first such explicit transfer found
                }
            } else {
                 const generalMatch = log.match(generalTransferToTargetRegex);
                 if (generalMatch) {
                    const potentialAmount = BigInt(generalMatch[1]);
                    if (potentialAmount > 0n && transferAmount === 0n) { // Only if not already found by specific regex
                         console.log(`${logPrefix} Found potential transfer via general log message: ${formatSol(potentialAmount)} SOL.`);
                         transferAmount = potentialAmount;
                         // Payer might be harder to determine from this generic log.
                         if (tx.transaction.message.header && tx.transaction.message.accountKeys.length > 0) {
                            payerAddress = tx.transaction.message.accountKeys[0].toBase58();
                         }
                         // Don't break here, sysTransferRegex is more precise.
                    }
                 }
            }
        }
    }

    if (transferAmount > 0n) {
        console.log(`${logPrefix} Final analysis result for TX: Found ${formatSol(transferAmount)} SOL transfer (likely from ${payerAddress || 'Unknown'}) to ${targetAddress}.`);
    } else {
        console.log(`${logPrefix} Final analysis result for TX: No positive SOL transfer found to ${targetAddress}.`);
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
    const adminIdsRaw = process.env.ADMIN_USER_IDS || '';
    const adminIds = adminIdsRaw.split(',')
        .map(id => id.trim())
        .filter(id => /^\d+$/.test(id)); // Filter for valid numeric Telegram User IDs

    if (adminIds.length === 0) {
        console.warn('[NotifyAdmin] No valid ADMIN_USER_IDS configured. Cannot send notification.');
        return;
    }

    const timestamp = new Date().toISOString();
    // escapeMarkdownV2 is defined in Part 1
    const escapedMessage = escapeMarkdownV2(message);
    const fullMessage = `🚨 *BOT ALERT* 🚨\n\\[${escapeMarkdownV2(timestamp)}\\]\n\n${escapedMessage}`;

    console.log(`[NotifyAdmin] Sending alert: "${message.substring(0, 100)}..." to ${adminIds.length} admin(s).`);

    // Use Promise.allSettled to attempt sending to all admins and log individual failures
    const results = await Promise.allSettled(
        adminIds.map(adminId =>
            safeSendMessage(adminId, fullMessage, { parse_mode: 'MarkdownV2' }) // safeSendMessage defined below
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
        console.error("[safeSendMessage] Invalid input provided:", { chatId, textPreview: text?.substring(0, 50) + '...' });
        return undefined;
    }

    const MAX_LENGTH = 4096; // Telegram API message length limit
    let messageToSend = text;

    if (messageToSend.length > MAX_LENGTH) {
        console.warn(`[safeSendMessage] Message for chat ${chatId} too long (${messageToSend.length} chars), truncating to ${MAX_LENGTH}.`);
        const ellipsis = "... (truncated)";
        // Adjust truncation length based on parse mode to ensure ellipsis itself doesn't cause parse error
        const ellipsisLength = (options.parse_mode === 'MarkdownV2' || options.parse_mode === 'Markdown')
                             ? escapeMarkdownV2(ellipsis).length
                             : ellipsis.length;
        const truncateAt = MAX_LENGTH - ellipsisLength;
        messageToSend = messageToSend.substring(0, truncateAt) +
                        ((options.parse_mode === 'MarkdownV2' || options.parse_mode === 'Markdown') ? escapeMarkdownV2(ellipsis) : ellipsis);
    }

    // telegramSendQueue is from Part 1
    return telegramSendQueue.add(async () => {
        try {
            const sentMessage = await bot.sendMessage(chatId, messageToSend, options); // bot from Part 1
            return sentMessage;
        } catch (error) {
            console.error(`❌ Telegram send error to chat ${chatId}: Code: ${error.code || 'N/A'} | Message: ${error.message}`);
            if (error.response?.body) {
                console.error(`   -> Telegram API Response Body: ${JSON.stringify(error.response.body)}`);
                const errorCode = error.response.body.error_code;
                const description = error.response.body.description?.toLowerCase() || '';

                if (errorCode === 403 || description.includes('blocked by the user') || description.includes('user is deactivated') || description.includes('bot was kicked') || description.includes('chat not found')) {
                    console.warn(`[TG Send] Bot interaction issue with chat ${chatId}: ${description}. Further messages might fail.`);
                    // Consider marking user/chat as inactive in DB to prevent further send attempts.
                } else if (errorCode === 400 && (description.includes('parse error') || description.includes('can\'t parse entities') || description.includes('wrong file identifier'))) {
                    console.error(`❌❌❌ Telegram Parse Error or Bad Request! Message for chat ${chatId}. Description: ${description}`);
                    console.error(`   -> Original Text (first 200 chars): ${text.substring(0,200)}`);
                    console.error(`   -> Sent Text (first 200 chars): ${messageToSend.substring(0,200)}`);
                    console.error(`   -> Options: ${JSON.stringify(options)}`);
                    // Notify admin about persistent parse errors
                    if (typeof notifyAdmin === "function") {
                         await notifyAdmin(`🚨 Telegram Parse Error in \`safeSendMessage\`\nChatID: ${chatId}\nOptions: ${JSON.stringify(options)}\nError: ${escapeMarkdownV2(description)}\nOriginal Text (Truncated):\n\`\`\`\n${escapeMarkdownV2(text.substring(0, 300))}\n\`\`\``)
                             .catch(e => console.error("Failed to send admin notification about parse error:", e));
                    }
                } else if (errorCode === 429) { // Too Many Requests - Should be handled by queue, but if API bursts, log it.
                     console.warn(`[TG Send] Received 429 (Too Many Requests) for chat ${chatId} despite queue. Queue interval might need adjustment or global limit hit. Error: ${description}`);
                }
            }
            return undefined; // Indicate failure
        }
    }).catch(queueError => {
        // This catch is for errors adding to the PQueue itself (e.g., queue timeout if throwOnTimeout was true, though typically it's for the promise from the task)
        console.error(`❌ Error adding job to telegramSendQueue for chat ${chatId}:`, queueError.message);
        if (typeof notifyAdmin === "function") {
            notifyAdmin(`🚨 ERROR adding job to telegramSendQueue for Chat ${chatId}: ${escapeMarkdownV2(queueError.message)}`).catch(()=>{});
        }
        return undefined;
    });
}


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
                     .replace(/'/g, "&#039;"); // Or use &apos; if target supports it. &#039; is safer.
}

/**
 * Gets a display name for a user in a chat, escaping it for MarkdownV2 if not a username.
 * Prioritizes username, then first/last name, then fallback ID.
 * @param {string | number} chatId Chat ID.
 * @param {string | number} userId User ID.
 * @returns {Promise<string>} MarkdownV2 escaped display name (or @username).
 */
async function getUserDisplayName(chatId, userId) {
    const strUserId = String(userId);
    const strChatId = String(chatId);
    try {
        const member = await bot.getChatMember(strChatId, strUserId); // bot from Part 1
        const user = member.user;
        let name = '';
        if (user.username) {
            name = `@${user.username}`; // Usernames don't need MdV2 escaping when used as @username
        } else {
            if (user.first_name) {
                name = user.first_name;
                if (user.last_name) {
                    name += ` ${user.last_name}`;
                }
            } else {
                name = `User ${strUserId.slice(-4)}`; // Fallback if no name
            }
            name = escapeMarkdownV2(name); // Escape first/last names or fallback
        }
        return name;
    } catch (error) {
        console.warn(`[getUserDisplayName] Failed to get chat member for user ${strUserId} in chat ${strChatId}: ${error.message}. Falling back to ID.`);
        return escapeMarkdownV2(`User ${strUserId.slice(-4)}`); // Fallback, escaped
    }
}


// --- Cache Utilities ---

/** Updates the wallet cache for a user with new data, resets TTL. */
function updateWalletCache(userId, data) {
    userId = String(userId);
    const existingEntry = walletCache.get(userId); // walletCache from Part 1
    if (existingEntry?.timeoutId) {
        clearTimeout(existingEntry.timeoutId);
    }

    const entryTimestamp = Date.now();
    const newData = { ...(existingEntry || {}), ...data, timestamp: entryTimestamp }; // Merge with existing, update timestamp

    const timeoutId = setTimeout(() => {
        const currentEntry = walletCache.get(userId);
        // Only delete if it's the same entry we set the timeout for (check timestamp)
        if (currentEntry && currentEntry.timestamp === entryTimestamp) {
            walletCache.delete(userId);
            // console.log(`[Cache] Wallet cache expired and removed for user ${userId}`);
        }
    }, WALLET_CACHE_TTL_MS); // WALLET_CACHE_TTL_MS from Part 1

    newData.timeoutId = timeoutId;
    if (timeoutId.unref) { // Allows Node.js to exit if this is the only thing in event loop
        timeoutId.unref();
    }
    walletCache.set(userId, newData);
}

/** Gets data from the wallet cache if entry exists and is not expired. */
function getWalletCache(userId) {
    userId = String(userId);
    const entry = walletCache.get(userId);
    if (entry) { // Check if entry exists
        if (Date.now() - entry.timestamp < WALLET_CACHE_TTL_MS) {
            return entry; // Valid and not expired
        } else {
            // Expired, remove it
            if(entry.timeoutId) clearTimeout(entry.timeoutId);
            walletCache.delete(userId);
            // console.log(`[Cache] Wallet cache for user ${userId} was expired upon access.`);
            return undefined; // Expired
        }
    }
    return undefined; // Not found
}

/** Adds/Updates an active deposit address in the cache, sets expiry timer. */
function addActiveDepositAddressCache(address, userId, expiresAtTimestamp) {
    const existingEntry = activeDepositAddresses.get(address); // activeDepositAddresses from Part 1
    if (existingEntry?.timeoutId) {
        clearTimeout(existingEntry.timeoutId);
    }

    const newEntry = { userId: String(userId), expiresAt: expiresAtTimestamp };

    const delay = expiresAtTimestamp - Date.now();
    if (delay > 0) {
        const timeoutId = setTimeout(() => {
            const currentEntry = activeDepositAddresses.get(address);
            // Only delete if it's the same entry (check all relevant fields, or use a unique ID per entry if needed)
            if (currentEntry && currentEntry.userId === String(userId) && currentEntry.expiresAt === expiresAtTimestamp) {
                activeDepositAddresses.delete(address);
                // console.log(`[Cache] Active deposit address ${address} expired and removed from cache.`);
            }
        }, delay);
        newEntry.timeoutId = timeoutId;
        if (timeoutId.unref) {
            timeoutId.unref();
        }
        activeDepositAddresses.set(address, newEntry);
    } else {
        // Already expired, don't add or ensure it's removed if somehow present
        activeDepositAddresses.delete(address); // Ensure it's not in cache if already expired
        // console.log(`[Cache] Attempted to add already expired deposit address ${address} to cache. Not added/Ensured removed.`);
    }
}

/** Gets an active deposit address entry from cache if it exists and hasn't expired. */
function getActiveDepositAddressCache(address) {
    const entry = activeDepositAddresses.get(address);
    if (entry) {
        if (Date.now() < entry.expiresAt) {
            return entry; // Valid and not expired
        } else {
            // Expired, clear it from cache
            if(entry.timeoutId) clearTimeout(entry.timeoutId);
            activeDepositAddresses.delete(address);
            // console.log(`[Cache] Active deposit address ${address} was expired upon access from cache.`);
            return undefined; // Expired
        }
    }
    return undefined; // Not found
}

/** Explicitly removes an address from the deposit cache and clears its timer. */
function removeActiveDepositAddressCache(address) {
    const entry = activeDepositAddresses.get(address);
    if (entry?.timeoutId) {
        clearTimeout(entry.timeoutId);
    }
    const deleted = activeDepositAddresses.delete(address);
    // if(deleted) console.log(`[Cache] Explicitly removed deposit address ${address} from cache.`);
    return deleted;
}

/** Adds a transaction signature to the processed deposit cache (Set acts like LRU by re-adding). */
function addProcessedDepositTx(signature) {
    // Set inherently handles uniqueness.
    // For LRU behavior with a fixed size Set, we'd need to manage it manually:
    // if size > MAX, remove oldest. But Set doesn't preserve insertion order directly for easy "oldest" removal.
    // A simple Set will prevent re-processing recently seen TXs.
    // For a strict LRU with size limit, an array + Set or a dedicated LRU cache library would be better.
    // Given MAX_PROCESSED_TX_CACHE_SIZE, a more robust LRU might be needed if memory becomes an issue.
    // For now, simple Set:
    if (processedDepositTxSignatures.has(signature)) { // processedDepositTxSignatures from Part 1
        // If it's already there, and we want to make it "newest" for some LRU strategies,
        // we might delete and re-add. But for a simple "have I seen this?" Set, no action needed.
        return;
    }

    // Simple size management: if full, delete a random one (not true LRU) or just let it grow.
    // The original code's LRU was: delete oldest if full, then add.
    // With a Set, "oldest" is not trivial. We can convert to array, shift, then re-create set, or use a proper LRU.
    // For simplicity here, if size exceeds, we'll just warn or implement a basic FIFO-like removal if critical.
    // The current set just grows. If MAX_PROCESSED_TX_CACHE_SIZE is important, an actual LRU cache is better.
    // Original had:
    // if (processedDepositTxSignatures.size >= MAX_PROCESSED_TX_CACHE_SIZE) {
    //     const oldestSig = processedDepositTxSignatures.values().next().value; // This gets an arbitrary value, not necessarily oldest for a Set
    //     if (oldestSig) processedDepositTxSignatures.delete(oldestSig);
    // }
    // This "oldestSig" from a Set is not guaranteed. Using an array as a queue alongside the set is better for LRU.
    // For now, just add to the set. The cache's primary goal is to prevent immediate re-processing.
    processedDepositTxSignatures.add(signature);

    // If strict size limiting is critical:
    // static processedTxArrayForLru = [];
    // if (!processedDepositTxSignatures.has(signature)) {
    //    if (processedTxArrayForLru.length >= MAX_PROCESSED_TX_CACHE_SIZE) {
    //        const removedSig = processedTxArrayForLru.shift();
    //        processedDepositTxSignatures.delete(removedSig);
    //    }
    //    processedTxArrayForLru.push(signature);
    //    processedDepositTxSignatures.add(signature);
    // }
}

/** Checks if a transaction signature exists in the processed deposit cache. */
function hasProcessedDepositTx(signature) {
    return processedDepositTxSignatures.has(signature);
}


// --- Other Utilities ---

/** Generates a random referral code prefixed with 'ref_'. Uses crypto.randomBytes from Part 1. */
function generateReferralCode(length = 8) { // Default length for the hex part
    const byteLength = Math.ceil(length / 2); // Each byte becomes 2 hex chars
    const randomBytes = crypto.randomBytes(byteLength); // crypto from Part 1
    const hexString = randomBytes.toString('hex').slice(0, length); // Ensure exact length if byteLength was rounded up
    return `ref_${hexString}`;
}

// --- End of Part 3 ---
// index.js - Part 4: Game Logic
// --- VERSION: 3.2.1 --- (Adjusted based on discussions for correctness)

// --- Assuming constants like LAMPORTS_PER_SOL, GAME_CONFIG etc are available from Part 1 ---
// --- Card utilities like createDeck, shuffleDeck etc. are defined within this Part. ---

// --- Game Logic Implementations ---
// These functions simulate the core outcome of each game.
// They do NOT handle betting, balance updates, house edge application, or user notifications/animations,
// unless specified for a particular game's core mechanic (e.g. Crash multiplier).
// That broader logic resides in the game result handlers (Part 5a).

/**
 * Plays a coinflip game.
 * @param {string} choice 'heads' or 'tails'. Not strictly used in this simple simulation but good for context if logic expands.
 * @returns {{ outcome: 'heads' | 'tails' }} The random result of the flip.
 */
function playCoinflip(choice) { // choice parameter kept for potential future use
    // Simple 50/50 random outcome
    const outcome = Math.random() < 0.5 ? 'heads' : 'tails';
    return { outcome };
}

/**
 * Simulates a simple race game outcome by picking a winning lane number.
 * @param {number} [totalLanes=4] The number of lanes/competitors in the race. This should ideally match RACE_HORSES.length.
 * @returns {{ winningLane: number }} The randomly determined winning lane number (1-indexed).
 */
function simulateRace(totalLanes = 4) { // RACE_HORSES.length could be passed from calling context
    if (totalLanes <= 0) totalLanes = 1; // Prevent errors with invalid input
    const winningLane = Math.floor(Math.random() * totalLanes) + 1; // 1 to totalLanes
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
    const weights = [25, 20, 15, 10, 5, 1]; // Weighted probability: J is rarest. Sum = 76

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
        payoutMultiplier = 0; // Jackpot win is handled separately, not by a standard multiplier here.
                              // The handler in Part 5a will award currentSlotsJackpotLamports.
    } else if (reel1 === '7' && reel2 === '7' && reel3 === '7') {
        payoutMultiplier = 100;
    } else if (reel1 === 'B' && reel2 === 'B' && reel3 === 'B') {
        payoutMultiplier = 20;
    } else if (reel1 === 'O' && reel2 === 'O' && reel3 === 'O') {
        payoutMultiplier = 10;
    } else if (reel1 === 'C' && reel2 === 'C' && reel3 === 'C') {
        payoutMultiplier = 5;
    } else {
        // Check for any two cherries (a common slot payout)
        const cherryCount = resultSymbols.filter(s => s === 'C').length;
        if (cherryCount >= 2) { // Exactly two or three (if three wasn't caught above, which it is)
            payoutMultiplier = 2;
        }
    }
    // The game handler in Part 5a will check isJackpotWin, then use payoutMultiplier, then apply house edge.
    return { symbols: resultSymbols, payoutMultiplier, isJackpotWin };
}


/**
 * Simulates a simplified Roulette spin (European style - 0 to 36).
 * @returns {{ winningNumber: number }} The winning number (0-36).
 */
function simulateRouletteSpin() {
    const winningNumber = Math.floor(Math.random() * 37); // Generates a number from 0 to 36
    return { winningNumber };
}

/**
 * Determines the base payout multiplier for a given Roulette bet type and winning number.
 * Note: This returns the multiplier for WINNINGS (e.g., 35 for straight means 35x winnings, player gets 36x stake back).
 * The actual payout calculation (stake + winnings * (1-edge)) is in the game handler.
 * @param {string} betType The type of bet (e.g., 'straight', 'red', 'even').
 * @param {string | number | null} betValue The specific number/value bet on (e.g., '5' for straight, not used for 'red').
 * @param {number} winningNumber The actual winning number from the spin (0-36).
 * @returns {number} Payout multiplier for the winnings (e.g., 35 for straight, 1 for even money bets). Returns 0 if the bet loses.
 */
function getRoulettePayoutMultiplier(betType, betValue, winningNumber) {
    let payoutMultiplier = 0; // Default to no win

    const isRed = (num) => [1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36].includes(num);
    const isBlack = (num) => num !== 0 && ![1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36].includes(num); // Any non-zero, non-red number
    const isEven = (num) => num !== 0 && num % 2 === 0;
    const isOdd = (num) => num !== 0 && num % 2 !== 0; // Number 0 is neither even nor odd for payouts
    const isLow = (num) => num >= 1 && num <= 18;  // 1-18
    const isHigh = (num) => num >= 19 && num <= 36; // 19-36

    switch (betType) {
        case 'straight': // Bet on a single number
            // betValue for straight should be the number itself (0-36)
            if (betValue !== null && winningNumber === parseInt(String(betValue), 10)) {
                payoutMultiplier = 35; // Pays 35 to 1
            }
            break;
        case 'red':   if (isRed(winningNumber)) payoutMultiplier = 1; break; // Pays 1 to 1
        case 'black': if (isBlack(winningNumber)) payoutMultiplier = 1; break; // Pays 1 to 1
        case 'even':  if (isEven(winningNumber)) payoutMultiplier = 1; break; // Pays 1 to 1
        case 'odd':   if (isOdd(winningNumber)) payoutMultiplier = 1; break;  // Pays 1 to 1
        case 'low':   if (isLow(winningNumber)) payoutMultiplier = 1; break;   // Pays 1 to 1 (1-18)
        case 'high':  if (isHigh(winningNumber)) payoutMultiplier = 1; break;  // Pays 1 to 1 (19-36)
        // Other bet types like dozens, columns, splits, streets, corners can be added here.
        default:
            console.warn(`[getRoulettePayoutMultiplier] Unknown or unhandled bet type: ${betType}`);
            break;
    }
    return payoutMultiplier;
}


/**
 * Simulates a simplified game of Casino War. Ace is high. No "going to war" on a tie for this version.
 * @returns {{ result: 'win' | 'loss' | 'push', playerCard: string, dealerCard: string, playerRank: number, dealerRank: number }}
 */
function simulateWar() {
    const deck = createDeck(); // Defined later in this Part
    shuffleDeck(deck); // Defined later in this Part

    const playerCardObject = dealCard(deck); // Card object
    const dealerCardObject = dealCard(deck); // Card object

    if (!playerCardObject || !dealerCardObject) { // Should not happen with a fresh deck
        console.error("[simulateWar] Error dealing cards, deck empty prematurely.");
        // Fallback or throw error. For now, let's assume a loss to prevent undefined behavior.
        return { result: 'loss', playerCard: 'Error', dealerCard: 'Error', playerRank: 0, dealerRank: 0 };
    }

    const playerCard = formatCard(playerCardObject); // Formatted string
    const dealerCard = formatCard(dealerCardObject); // Formatted string

    const playerRank = getCardNumericValue(playerCardObject, true); // true for Ace high in War (Ace=14)
    const dealerRank = getCardNumericValue(dealerCardObject, true); // true for Ace high in War

    let result;
    if (playerRank > dealerRank) result = 'win';
    else if (playerRank < dealerRank) result = 'loss';
    else result = 'push'; // Simplified: no "go to war" option, push on tie

    return { result, playerCard, dealerCard, playerRank, dealerRank };
}

// --- NEW GAME LOGIC: Crash ---
/**
 * Simulates a Crash game round to determine the crash point.
 * The actual game flow (increasing multiplier, user cashout) is in Part 5a.
 * This function determines at what multiplier the game would naturally crash.
 * @returns {{ crashMultiplier: number }} The multiplier at which the game crashes, rounded to 2 decimal places.
 */
function simulateCrash() {
    // Using the formula: E = (1-H) / ( (1-H) * ln( (1-H)/(1-MAX_X) ) + (1-MAX_X) -1 )
    // simpler: P(crash at X) = (1 - HouseEdge) / X^2 (for X >= 1)
    // To simulate this, we can use inverse transform sampling if `r` is uniform random in [0,1):
    // r = P(crash_point < x) = integral from 1 to x of ( (1-H) / t^2 ) dt
    // r = (1-H) * [-1/t] from 1 to x
    // r = (1-H) * (1 - 1/x)
    // r / (1-H) = 1 - 1/x
    // 1/x = 1 - r / (1-H)
    // x = 1 / (1 - r / (1-H))
    // Let u = Math.random() be a uniform random number in [0, 1).
    // To prevent crashMultiplier from being 1 exactly (unless houseEdge makes it so),
    // and to ensure a minimum increase if it doesn't crash at 1.00x.
    // A common setup is to have a small chance (e.g., houseEdge) of an instant bust (crash at 1.00x).
    // If it doesn't bust instantly, then the multiplier is determined.

    const houseEdge = GAME_CONFIG.crash.houseEdge; // From Part 1

    const r = Math.random(); // Uniform random number [0, 1)

    let crashMultiplier;

    // Small probability (e.g., 1% or houseEdge directly) for an instant crash at 1.00x
    // This ensures the house edge is met over time.
    // Let's use houseEdge as the probability of an instant 1.00x crash.
    if (r < houseEdge) {
        crashMultiplier = 1.00;
    } else {
        // For the remaining (1 - houseEdge) probability space, calculate crash point.
        // We need to use a new random number `r_prime` that is uniform in [0, 1)
        // and map it to the desired distribution.
        // The formula `x = 1 / (1 - r_prime)` gives a distribution where P(x > X) = 1/X.
        // To incorporate house edge properly into the curve itself if not doing an instant bust:
        const r_prime = Math.random(); // Another random draw
        crashMultiplier = (1 - houseEdge) / (1 - r_prime); // Derived from P(crash < X) = 1 - (1-H)/X

        // Ensure it's at least 1.00 (or 1.01 if instant bust didn't occur)
        if (crashMultiplier < 1.00) {
             crashMultiplier = 1.00; // Or set a minimum like 1.01 to ensure some animation
        }
    }

    // Round to two decimal places as is common in Crash games
    return { crashMultiplier: parseFloat(crashMultiplier.toFixed(2)) };
}


// --- NEW GAME LOGIC: Blackjack & Card Utilities ---

const BJ_SUITS = ['♠️', '♥️', '♦️', '♣️']; // Spades, Hearts, Diamonds, Clubs
const BJ_RANKS = ['2', '3', '4', '5', '6', '7', '8', '9', 'T', 'J', 'Q', 'K', 'A']; // T for Ten

/**
 * Creates a standard 52-card deck.
 * @returns {Array<{rank: string, suit: string}>} An array of card objects.
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
 * @param {Array<{rank: string, suit: string}>} deck The deck to be shuffled.
 */
function shuffleDeck(deck) {
    for (let i = deck.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [deck[i], deck[j]] = [deck[j], deck[i]]; // Swap elements
    }
}

/**
 * Deals a single card from the top of the deck (end of the array).
 * @param {Array<{rank: string, suit: string}>} deck The deck to deal from.
 * @returns {{rank: string, suit: string} | undefined} The card object, or undefined if deck is empty.
 */
function dealCard(deck) {
    return deck.pop();
}

/**
 * Gets the numerical value of a card for games like Blackjack or War.
 * For Blackjack, Ace is initially 11, adjusted by calculateHandValue.
 * For War, Ace is typically high (14).
 * @param {{rank: string, suit: string}} card The card object.
 * @param {boolean} [aceHighInWar=false] If true and rank is Ace, returns 14 (for War).
 * For Blackjack, Ace is 11 (calculateHandValue handles 1 vs 11 logic).
 * @returns {number} The numerical value of the card.
 */
function getCardNumericValue(card, aceHighInWar = false) {
    if (!card || !card.rank) return 0;
    const rank = card.rank;
    if (['K', 'Q', 'J', 'T'].includes(rank)) return 10;
    if (rank === 'A') {
        return aceHighInWar ? 14 : 11; // For Blackjack, Ace is 11 (value adjusted later if bust)
    }
    return parseInt(rank, 10); // For ranks '2' through '9'
}

/**
 * Formats a card object for display (e.g., "K♠️", "A♥️").
 * @param {{rank: string, suit: string}} card The card object.
 * @returns {string} A string representation of the card.
 */
function formatCard(card) {
    if (!card || !card.rank || !card.suit) return "??"; // Should not happen with valid card objects
    return `${card.rank}${card.suit}`;
}

/**
 * Calculates the total value of a hand in Blackjack, handling Aces correctly (as 1 or 11).
 * @param {Array<{rank: string, suit: string}>} hand An array of card objects.
 * @returns {number} The best possible hand value not exceeding 21, or the lowest value if over 21.
 */
function calculateHandValue(hand) {
    let value = 0;
    let aceCount = 0;
    for (const card of hand) {
        const cardValue = getCardNumericValue(card, false); // false for aceHighInWar, so Ace is 11
        value += cardValue;
        if (card.rank === 'A') {
            aceCount++;
        }
    }
    // Adjust for Aces if value is over 21 and Aces are present
    while (value > 21 && aceCount > 0) {
        value -= 10; // Change an Ace from 11 to 1
        aceCount--;
    }
    return value;
}

/**
 * Simulates the dealer's turn in Blackjack. Dealer hits until 17 or more, or busts.
 * Dealer typically hits on soft 17 (Ace + 6) in many rules, but for simplicity here, stands on any 17.
 * @param {Array<{rank: string, suit: string}>} dealerHand The dealer's current hand.
 * @param {Array<{rank: string, suit: string}>} deck The current game deck.
 * @returns {{hand: Array<{rank: string, suit: string}>, value: number, busted: boolean}} The dealer's final hand, value, and bust status.
 */
function simulateDealerPlay(dealerHand, deck) {
    let handValue = calculateHandValue(dealerHand);
    // Standard rule: Dealer hits until hand value is 17 or more.
    // Some casinos hit on soft 17 (Ace + 6). For simplicity, let's assume dealer stands on all 17s.
    while (handValue < 17) {
        const newCard = dealCard(deck);
        if (newCard) {
            dealerHand.push(newCard);
            handValue = calculateHandValue(dealerHand);
        } else {
            // Should not happen in a normal game if deck is sufficient.
            console.error("[simulateDealerPlay] Deck empty during dealer's turn.");
            break;
        }
    }
    return {
        hand: dealerHand,
        value: handValue,
        busted: handValue > 21
    };
}

/**
 * Determines the winner of a Blackjack hand after all actions are complete.
 * This function is called by the game handler in Part 5a once player stands or busts, or game ends.
 * @param {Array<{rank: string, suit: string}>} playerHand Player's final hand.
 * @param {Array<{rank: string, suit: string}>} dealerHand Dealer's final hand (after dealer has played).
 * @returns {{
 * result: 'player_blackjack' | 'player_wins' | 'dealer_wins' | 'push' | 'player_busts' | 'dealer_busts_player_wins',
 * playerValue: number,
 * dealerValue: number,
 * playerHandFormatted: string[],
 * dealerHandFormatted: string[]
 * }} Detailed result of the game.
 */
function determineBlackjackWinner(playerHand, dealerHand) {
    const playerValue = calculateHandValue(playerHand);
    const dealerValue = calculateHandValue(dealerHand); // Assuming dealer has completed their play

    const playerHasBlackjack = playerValue === 21 && playerHand.length === 2;
    const dealerHasBlackjack = dealerValue === 21 && dealerHand.length === 2;

    let result;

    if (playerValue > 21) {
        result = 'player_busts'; // Player loses
    } else if (dealerValue > 21) {
        result = 'dealer_busts_player_wins'; // Player wins (as long as player didn't bust)
    } else if (playerHasBlackjack && !dealerHasBlackjack) {
        result = 'player_blackjack'; // Player wins with Blackjack (typically 3:2 payout)
    } else if (dealerHasBlackjack && !playerHasBlackjack) {
        result = 'dealer_wins'; // Dealer wins with Blackjack
    } else if (playerHasBlackjack && dealerHasBlackjack) {
        result = 'push'; // Both have Blackjack
    } else if (playerValue > dealerValue) {
        result = 'player_wins';
    } else if (dealerValue > playerValue) {
        result = 'dealer_wins';
    } else { // playerValue === dealerValue (and neither busted, neither has un-pushed blackjack)
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

// --- End of Part 4 ---
// index.js - Part 5a: Telegram Message/Callback Handlers & Game Result Processing
// --- VERSION: 3.2.1 --- (Adjusted based on discussions for correctness)

// --- Assuming functions from Part 1, 2, 3, 4 are available ---
// (bot, pool, caches, GAME_CONFIG, DB ops, utils like safeSendMessage, escapeMarkdownV2, formatSol,
// game simulation functions, card utilities, getUserBalance, etc.)
// --- 'commandHandlers' and 'menuCommandHandlers' (Maps from Part 5b, declared in Part 1) will be used here.
// --- '_handleReferralChecks' (from Part 5b) is used by 'placeBet'.
// --- 'routeStatefulInput' (from Part 5b) is used by 'handleMessage'.
// --- USER_COMMAND_COOLDOWN_MS, USER_STATE_TTL_MS, userStateCache, commandCooldown, userLastBetAmounts from Part 1.
// --- LAMPORTS_PER_SOL, SOL_DECIMALS from Part 1.
// --- getJackpotAmount, incrementJackpotAmount, updateJackpotAmount (from Part 2) used by handleSlotsGame.
// --- Constants like RACE_HORSES are from Part 1.

// --- Main Message Handler (Entry point from Part 6 webhook/polling) ---
/**
 * Handles incoming Telegram messages.
 * Routes to command handlers or stateful input handlers.
 * @param {import('node-telegram-bot-api').Message} msg The Telegram message object.
 */
async function handleMessage(msg) {
    const chatId = String(msg.chat.id);
    const userId = String(msg.from.id); // User ID of the sender
    const text = msg.text || '';
    const messageId = msg.message_id; // ID of the incoming message
    const logPrefix = `[Msg Rcv User ${userId} Chat ${chatId}]`;

    console.log(`${logPrefix} Received: "${text.substring(0, 50)}${text.length > 50 ? '...' : ''}"`);

    // Cooldown check (basic spam prevention)
    const now = Date.now();
    const lastCommandTime = commandCooldown.get(userId) || 0; // commandCooldown from Part 1
    if (now - lastCommandTime < USER_COMMAND_COOLDOWN_MS) { // USER_COMMAND_COOLDOWN_MS from Part 1
        // console.log(`${logPrefix} Command cooldown active for user ${userId}. Ignoring.`);
        return; // Silently ignore if too fast
    }
    commandCooldown.set(userId, now);

    // Check if user is awaiting stateful input
    const userCurrentState = userStateCache.get(userId); // userStateCache from Part 1
    // Handle /cancel command to clear state at any point during stateful input
    if (text.toLowerCase() === '/cancel' && userCurrentState) {
        const stateClearedMessage = userCurrentState.data?.breadcrumb
            ? `Action '${escapeMarkdownV2(userCurrentState.data.breadcrumb)}' cancelled.`
            : "Current action cancelled.";
        userStateCache.delete(userId);
        console.log(`${logPrefix} User cancelled stateful input. State cleared.`);
        // Try to edit the message the user was replying to, if available and makes sense
        if (userCurrentState.messageId && userCurrentState.chatId === chatId) {
            bot.editMessageText(stateClearedMessage, { // bot from Part 1
                chat_id: chatId,
                message_id: userCurrentState.messageId,
                reply_markup: { inline_keyboard: [] }, // Clear buttons
                parse_mode: 'MarkdownV2'
            }).catch(e => {
                console.warn(`${logPrefix} Failed to edit message on /cancel: ${e.message}. Sending new message.`);
                safeSendMessage(chatId, stateClearedMessage, { parse_mode: 'MarkdownV2' }); // safeSendMessage from Part 3
            });
        } else {
            safeSendMessage(chatId, stateClearedMessage, { parse_mode: 'MarkdownV2' });
        }
        return;
    }


    if (userCurrentState && now - userCurrentState.timestamp < USER_STATE_TTL_MS) { // USER_STATE_TTL_MS from Part 1
        // Ensure state is for this chat to prevent cross-chat state issues if bot is in groups
        if (userCurrentState.chatId === chatId) {
            console.log(`${logPrefix} Routing to stateful input handler for state: ${userCurrentState.state}`);
            // routeStatefulInput will be defined in Part 5b
            return routeStatefulInput(msg, userCurrentState);
        } else {
            console.warn(`${logPrefix} State mismatch: User state for chat ${userCurrentState.chatId}, but message from ${chatId}. Clearing state for safety.`);
            userStateCache.delete(userId);
            // Potentially inform the user that their previous context in another chat was cleared.
        }
    } else if (userCurrentState) { // State exists but timed out
        console.log(`${logPrefix} User state '${userCurrentState.state}' timed out for user ${userId}. Clearing state.`);
        userStateCache.delete(userId);
        // Optionally inform the user their previous action timed out.
    }

    // Handle commands if not in a stateful input mode (or if state timed out)
    if (text.startsWith('/')) {
        const args = text.split(' ');
        const command = args[0].toLowerCase().split('@')[0]; // Remove / and any @botusername

        // commandHandlers is a Map defined in Part 5b, declared in Part 1
        if (commandHandlers && commandHandlers.has(command)) {
            console.log(`${logPrefix} Executing command: ${command}`);
            const handler = commandHandlers.get(command);
            // Ensure user exists before executing most commands (except /start which handles it)
            if (command !== '/start') {
                let tempClient = null;
                try {
                    tempClient = await pool.connect(); // pool from Part 1
                    await ensureUserExists(userId, tempClient); // ensureUserExists from Part 2
                } catch(dbError) {
                    console.error(`${logPrefix} DB error ensuring user exists for command ${command}: ${dbError.message}`);
                    safeSendMessage(chatId, "A database error occurred. Please try again later.");
                    if (tempClient) tempClient.release();
                    return;
                } finally {
                    if (tempClient) tempClient.release();
                }
            }
            return handler(msg, args);
        } else if (command === '/admin' && process.env.ADMIN_USER_IDS.split(',').map(id => id.trim()).includes(userId)) {
            // handleAdminCommand will be defined in Part 5b
            return handleAdminCommand(msg, args);
        } else {
            return safeSendMessage(chatId, "😕 Unknown command. Type /help to see available commands.");
        }
    }
    // Non-command, non-stateful messages can be ignored
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
    const messageId = callbackQuery.message.message_id; // ID of the message with the buttons
    const data = callbackQuery.data; // Data string from the button
    const logPrefix = `[CB Rcv User ${userId} Chat ${chatId} Data ${data}]`;

    console.log(`${logPrefix} Received callback.`);

    bot.answerCallbackQuery(callbackQuery.id).catch(err => console.warn(`${logPrefix} Non-critical error answering CB query: ${err.message}`));

    const now = Date.now();
    const lastCallbackTimeKey = `${userId}_cb`;
    const lastCallbackTime = commandCooldown.get(lastCallbackTimeKey) || 0;
    if (now - lastCallbackTime < (USER_COMMAND_COOLDOWN_MS / 2)) {
        return;
    }
    commandCooldown.set(lastCallbackTimeKey, now);

    const userCurrentState = userStateCache.get(userId);
    if (userCurrentState && userCurrentState.chatId === chatId && userCurrentState.state?.startsWith('awaiting_')) {
        console.log(`${logPrefix} Clearing pending text input state '${userCurrentState.state}' for user ${userId} due to button click.`);
        userStateCache.delete(userId);
    }

    const [action, ...params] = data.split(':');

    if (action !== 'menu' || (action === 'menu' && params[0] !== 'main')) {
        let tempClient = null;
        try {
            tempClient = await pool.connect();
            await ensureUserExists(userId, tempClient);
        } catch (dbError) {
            console.error(`${logPrefix} DB error ensuring user exists for callback ${data}: ${dbError.message}`);
            safeSendMessage(chatId, "A database error occurred. Please try again later.");
            if (tempClient) tempClient.release();
            return;
        } finally {
            if (tempClient) tempClient.release();
        }
    }

    if (action === 'menu' && params.length > 0 && menuCommandHandlers && menuCommandHandlers.has(params[0])) {
        const menuKey = params[0];
        console.log(`${logPrefix} Routing to menu handler: ${menuKey}`);
        const handler = menuCommandHandlers.get(menuKey);
        return handler(callbackQuery.message, params.slice(1));
    }

    if (action === 'back') {
        const targetStateOrMenu = params[0];
        console.log(`${logPrefix} Handling 'back' action to: ${targetStateOrMenu}`);
        bot.deleteMessage(chatId, messageId).catch(e => console.warn(`${logPrefix} Failed to delete message ${messageId} on 'back': ${e.message}`));

        if (targetStateOrMenu === 'main_menu' || targetStateOrMenu === 'game_selection') {
            if (commandHandlers && commandHandlers.has('/start')) {
                return commandHandlers.get('/start')(callbackQuery.message, []);
            }
        }
        console.warn(`${logPrefix} Unhandled 'back' target: ${targetStateOrMenu}. Defaulting to main menu.`);
        if (commandHandlers && commandHandlers.has('/start')) {
            return commandHandlers.get('/start')(callbackQuery.message, []);
        }
        return;
    }

    if (action === 'play_again') {
        const gameKey = params[0];
        const betAmountLamportsStr = params[1];
        const betAmountLamports = BigInt(betAmountLamportsStr || '0');
        console.log(`${logPrefix} Handling 'play_again' for ${gameKey} with bet ${formatSol(betAmountLamports)} SOL`);

        const gameConfig = GAME_CONFIG[gameKey];
        if (gameConfig && betAmountLamports > 0n) {
            const userBets = userLastBetAmounts.get(userId) || new Map();
            userBets.set(gameKey, betAmountLamports);
            userLastBetAmounts.set(userId, userBets);

            const betAmountSOL = formatSol(betAmountLamports);
            const confirmationMessage = `Replay ${escapeMarkdownV2(gameConfig.name)} with ${escapeMarkdownV2(betAmountSOL)} SOL?`;
            const inlineKeyboard = [
                [{ text: `✅ Yes, Bet ${escapeMarkdownV2(betAmountSOL)} SOL`, callback_data: `confirm_bet:${gameKey}:${betAmountLamportsStr}` }],
                [{ text: `✏️ Change Bet`, callback_data: `menu:${gameKey}` }],
                [{ text: '↩️ Games Menu', callback_data: 'menu:main' }]
            ];
            return bot.editMessageText(confirmationMessage, {
                chat_id: chatId,
                message_id: messageId,
                reply_markup: { inline_keyboard: inlineKeyboard },
                parse_mode: 'MarkdownV2'
            }).catch(e => console.error(`${logPrefix} Error editing message for play_again confirmation: ${e.message}`));
        } else {
            console.warn(`${logPrefix} Invalid gameKey or betAmount for play_again. Game: ${gameKey}, Amount: ${betAmountLamportsStr}`);
            return safeSendMessage(chatId, "Sorry, I couldn't replay that game. Please select from the menu.", {parse_mode: 'MarkdownV2'});
        }
    }

    if (action === 'quick_deposit') {
        console.log(`${logPrefix} Handling 'quick_deposit' action.`);
        if (commandHandlers && commandHandlers.has('/deposit')) {
            bot.editMessageText("Generating your deposit address...", { chat_id: chatId, message_id: messageId, reply_markup: { inline_keyboard: [] } }).catch(()=>{});
            return commandHandlers.get('/deposit')(callbackQuery.message, []);
        }
    }

    if (action === 'confirm_bet') {
        const gameKey = params[0];
        const betAmountLamportsStr = params[1];
        const betAmountLamports = BigInt(betAmountLamportsStr || '0');
        const gameConfig = GAME_CONFIG[gameKey];

        if (!gameConfig) {
            console.error(`${logPrefix} Invalid gameKey in confirm_bet: ${gameKey}`);
            return safeSendMessage(chatId, "Error: Unknown game. Please try again.");
        }
        console.log(`${logPrefix} Bet confirmed for ${gameKey} with ${formatSol(betAmountLamports)} SOL.`);

        bot.editMessageText(
            `Processing your ${escapeMarkdownV2(gameConfig.name)} bet of ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL... ⏳`,
            { chat_id: chatId, message_id: messageId, reply_markup: { inline_keyboard: [] }, parse_mode: 'MarkdownV2' }
        ).catch(e => console.warn(`${logPrefix} Error editing message for bet processing (confirm_bet): ${e.message}`));

        switch (gameKey) {
            case 'coinflip':
                const choice = params[2];
                return handleCoinflipGame(userId, chatId, messageId, betAmountLamports, choice);
            case 'race':
                const chosenLane = parseInt(params[2], 10);
                return handleRaceGame(userId, chatId, messageId, betAmountLamports, chosenLane);
            case 'slots':
                return handleSlotsGame(userId, chatId, messageId, betAmountLamports);
            case 'roulette':
                const betType = params[2];
                const betValue = params[3] || null;
                return handleRouletteGame(userId, chatId, messageId, betAmountLamports, betType, betValue);
            case 'war':
                return handleWarGame(userId, chatId, messageId, betAmountLamports);
            case 'crash':
                return handleCrashGame(userId, chatId, messageId, betAmountLamports);
            case 'blackjack':
                return handleBlackjackGame(userId, chatId, messageId, betAmountLamports, 'initial_deal');
            default:
                console.error(`${logPrefix} Unknown gameKey in confirm_bet: ${gameKey}`);
                return safeSendMessage(chatId, "Sorry, something went wrong with that game confirmation.");
        }
    }

    if (action === 'blackjack_action') {
        const gameInstanceId = params[0];
        const playerAction = params[1];
        console.log(`${logPrefix} Blackjack action: ${playerAction} for game instance ${gameInstanceId}`);

        const gameState = userStateCache.get(`${userId}_${gameInstanceId}`);
        if (!gameState || gameState.state !== 'awaiting_blackjack_action' || gameState.data.gameKey !== 'blackjack') {
            console.warn(`${logPrefix} No active Blackjack game found for instance ${gameInstanceId} or state mismatch. Current state:`, gameState);
            return safeSendMessage(chatId, "Your Blackjack session seems to have expired or there's an issue. Please start a new game.", {
                reply_markup: { inline_keyboard: [[{ text: 'Play Blackjack', callback_data: 'menu:blackjack' }]] }
            });
        }
        bot.editMessageText(
            `${escapeMarkdownV2(gameState.data.breadcrumb || GAME_CONFIG.blackjack.name)}\n\nProcessing your action: ${escapeMarkdownV2(playerAction)}...`,
            { chat_id: chatId, message_id: messageId, reply_markup: { inline_keyboard: [] }, parse_mode: 'MarkdownV2' }
        ).catch(e => console.warn(`${logPrefix} Error editing message for blackjack action: ${e.message}`));

        return handleBlackjackGame(userId, chatId, messageId, gameState.data.betAmountLamports, playerAction, gameState);
    }

    if (action === 'crash_cash_out') {
        const gameInstanceId = params[0];
        console.log(`${logPrefix} Crash cash out attempt for game instance ${gameInstanceId}`);

        const gameState = userStateCache.get(`${userId}_${gameInstanceId}`);
        if (!gameState || gameState.state !== 'awaiting_crash_cashout' || gameState.data.gameKey !== 'crash') {
            console.warn(`${logPrefix} No active Crash game found for instance ${gameInstanceId} to cash out, or state mismatch.`);
            return;
        }
        gameState.data.userRequestedCashOut = true;
        userStateCache.set(`${userId}_${gameInstanceId}`, gameState);
        return;
    }

    if (action === 'custom_amount_select') {
        const gameKey = params[0];
        const gameConfig = GAME_CONFIG[gameKey];
        if (!gameConfig) {
            console.error(`${logPrefix} Invalid gameKey for custom_amount_select: ${gameKey}`);
            return safeSendMessage(chatId, "Error: Unknown game for custom amount. Please try again.");
        }
        const breadcrumb = `${gameConfig.name} > Custom Amount`;
        userStateCache.set(userId, {
            state: `awaiting_${gameKey}_amount`,
            chatId: chatId,
            messageId: messageId,
            data: { gameKey, breadcrumb },
            timestamp: Date.now()
        });
        return bot.editMessageText(
            `${escapeMarkdownV2(breadcrumb)}\nPlease type the amount of SOL you want to bet for ${escapeMarkdownV2(gameConfig.name)} (e.g., 0.1 or 2.5).\nMin: ${escapeMarkdownV2(formatSol(gameConfig.minBetLamports))} SOL, Max: ${escapeMarkdownV2(formatSol(gameConfig.maxBetLamports))} SOL.`,
            {
                chat_id: chatId, message_id: messageId,
                reply_markup: { inline_keyboard: [[{ text: '❌ Cancel', callback_data: `menu:${gameKey}` }]] },
                parse_mode: 'MarkdownV2'
            }
        ).catch(e => console.error(`${logPrefix} Error editing message for custom amount prompt: ${e.message}`));
    }
    
    if (action === 'roulette_bet_type') {
        const betType = params[0];
        const betAmountLamportsStr = params[1];
        const currentBetAmountLamports = BigInt(betAmountLamportsStr);
        const gameConfig = GAME_CONFIG.roulette;
        const breadcrumb = `${gameConfig.name} > ${betType.charAt(0).toUpperCase() + betType.slice(1)}`;

        userStateCache.set(userId, {
            state: 'awaiting_roulette_confirm',
            chatId: chatId,
            messageId: messageId,
            data: { gameKey: 'roulette', betAmountLamports: currentBetAmountLamports, betType: betType, step: 'select_bet_type', breadcrumb: `${GAME_CONFIG.roulette.name}` },
            timestamp: Date.now()
        });

        const confirmationText = `${escapeMarkdownV2(breadcrumb)}\nBet ${escapeMarkdownV2(formatSol(currentBetAmountLamports))} SOL on ${escapeMarkdownV2(betType)}?`;
        const inlineKeyboard = [
            [{ text: `✅ Yes, Confirm Bet`, callback_data: `confirm_bet:roulette:${currentBetAmountLamportsStr}:${betType}` }],
            [{ text: '↩️ Change Bet Type', callback_data: `menu:roulette:${currentBetAmountLamportsStr}` }],
            [{ text: '💰 Change Amount', callback_data: `menu:roulette` }],
            [{ text: '❌ Cancel & Exit', callback_data: 'menu:main' }]
        ];
        return bot.editMessageText(confirmationText, {
            chat_id: chatId, message_id: messageId,
            reply_markup: { inline_keyboard: inlineKeyboard }, parse_mode: 'MarkdownV2'
        }).catch(e => console.error(`${logPrefix} Error editing roulette confirmation: ${e.message}`));
    }

    const tempStateForBetSelection = userStateCache.get(userId);
    if (tempStateForBetSelection && tempStateForBetSelection.chatId === chatId && tempStateForBetSelection.state?.startsWith('awaiting_')) {
        const gameKeyFromState = tempStateForBetSelection.data?.gameKey;
        if (gameKeyFromState && GAME_CONFIG[gameKeyFromState]) {
            console.log(`${logPrefix} Passing to proceedToGameStep for game ${gameKeyFromState}, state ${tempStateForBetSelection.state}, callback data '${data}'`);
            return proceedToGameStep(userId, chatId, messageId, tempStateForBetSelection, data);
        }
    }

    if (!['custom_amount_select', 'roulette_bet_type', 'menu', 'back', 'play_again', 'quick_deposit', 'confirm_bet', 'blackjack_action', 'crash_cash_out'].includes(action)) {
      console.warn(`${logPrefix} Unhandled callback query action: '${action}' with params: ${params.join(',')}. Data: '${data}'`);
    }
}
// --- End Main Callback Query Handler ---


// --- Universal Game Step Processor ---
/**
 * Manages multi-step game interactions (like choosing bet type, number, etc., primarily for Roulette and Race).
 * Called when a user clicks a button that represents a choice in a game flow.
 * @param {string} userId
 * @param {string} chatId
 * @param {number} messageId The ID of the message to potentially edit.
 * @param {object} currentState The current state object from userStateCache for this user.
 * @param {string} callbackData The `data` string from the callback_query (e.g., "type_red", "type_straight").
 */
async function proceedToGameStep(userId, chatId, messageId, currentState, callbackData) {
    const { gameKey, betAmountLamports, step: currentStep, breadcrumb: currentBreadcrumb } = currentState.data;
    const gameConfig = GAME_CONFIG[gameKey];
    const logPrefix = `[GameStep User ${userId} Game ${gameKey} CurStep ${currentStep || 'initial'} CB ${callbackData}]`;
    console.log(`${logPrefix} Proceeding to game step. Current state data:`, currentState.data);

    let nextStepName = currentStep;
    let messageText = "";
    let inlineKeyboard = [];
    let nextStateData = { ...currentState.data };
    let newBreadcrumb = currentBreadcrumb || gameConfig.name;

    try {
        if (gameKey === 'roulette') {
            const [cbActionType, ...cbParams] = callbackData.split('_');

            if (currentStep === 'select_bet_type' || !currentStep || (currentState.state === `awaiting_${gameKey}_amount_selection`)) { // Initial choice of bet type
                if (cbActionType === 'type') {
                    const chosenBetType = cbParams[0];
                    nextStateData.betType = chosenBetType;
                    newBreadcrumb = `${gameConfig.name} > ${chosenBetType.charAt(0).toUpperCase() + chosenBetType.slice(1)}`;
                    nextStateData.breadcrumb = newBreadcrumb;

                    if (chosenBetType === 'straight') {
                        nextStepName = 'awaiting_roulette_number';
                        userStateCache.set(userId, {
                            state: nextStepName,
                            chatId: chatId,
                            messageId: messageId,
                            data: { ...nextStateData, gameKey, betAmountLamports: String(betAmountLamports), breadcrumb: newBreadcrumb, originalMessageId: messageId }, // Store amount as string
                            timestamp: Date.now(),
                        });
                        messageText = `${escapeMarkdownV2(newBreadcrumb)}\nSelected bet: Straight Up.\nPlease type the number (0-36) you want to bet on.`;
                        inlineKeyboard = [[{ text: '↩️ Back to Bet Types', callback_data: `menu:roulette:${betAmountLamports}` }],[{ text: '❌ Cancel Bet', callback_data: 'menu:main' }]];
                    } else {
                        nextStepName = 'confirm_final_bet'; // Leads to confirm_bet action
                        messageText = `${escapeMarkdownV2(newBreadcrumb)}\nBet ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL on ${escapeMarkdownV2(chosenBetType)}?`;
                        inlineKeyboard = [
                            [{ text: `✅ Yes, Confirm Bet`, callback_data: `confirm_bet:roulette:${betAmountLamports}:${chosenBetType}` }],
                            [{ text: '↩️ Change Bet Type', callback_data: `menu:roulette:${betAmountLamports}` }],
                            [{ text: '💰 Change Amount', callback_data: `menu:roulette` }],
                            [{ text: '❌ Cancel & Exit', callback_data: 'menu:main' }]
                        ];
                        userStateCache.delete(userId); // No further state needed
                    }
                } else {
                     console.warn(`${logPrefix} Unexpected callback data '${callbackData}' for Roulette step '${currentStep}'.`);
                     safeSendMessage(chatId, "An unexpected error occurred with your selection. Please try again.");
                     userStateCache.delete(userId); return;
                }
            }
        } else if (gameKey === 'race') {
            // After amount is selected via showBetAmountButtons, that function directly sets up confirm_bet callback if standard amount is chosen.
            // If custom amount was chosen, handleCustomAmountInput calls this proceedToGameStep.
            // State `awaiting_race_amount` is handled by `handleCustomAmountInput`.
            // This proceedToGameStep is thus for selecting a horse *after* the amount is known (either standard or custom).
            // The calling context (e.g., game command handler after amount selection) needs to set state for 'select_horse'.

            if (currentStep === 'select_horse' || !currentStep || (currentState.state === `awaiting_${gameKey}_amount_selection`)) {
                 newBreadcrumb = `${gameConfig.name} > Select Horse`;
                 nextStateData.breadcrumb = newBreadcrumb;
                 nextStepName = 'confirm_final_bet'; // Next click will be a confirm_bet
                 messageText = `${escapeMarkdownV2(newBreadcrumb)}\nYour bet: ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL.\nChoose your horse:`;
                 const raceHorsesList = RACE_HORSES; // From Part 1
                 const horseButtons = raceHorsesList.map((horse, index) => ({
                    text: `${horse.emoji} ${escapeMarkdownV2(horse.name)} (${escapeMarkdownV2(String(horse.payoutMultiplier))}x)`,
                    callback_data: `confirm_bet:race:${betAmountLamports}:${index + 1}` // horse lane is 1-indexed
                 }));
                 for(let i = 0; i < horseButtons.length; i += 2) { // Group buttons
                    inlineKeyboard.push(horseButtons.slice(i, i + 2));
                 }
                 inlineKeyboard.push([{ text: '↩️ Change Amount', callback_data: `menu:race` }, { text: '❌ Cancel', callback_data: 'menu:main' }]);
                 userStateCache.delete(userId); // No further state, leads to confirm_bet
            }
        } else {
            console.warn(`${logPrefix} Game '${gameKey}' does not have multi-step button logic handled by proceedToGameStep, or current step '${currentStep}' is not recognized. Callback data: ${callbackData}.`);
            userStateCache.delete(userId);
            safeSendMessage(chatId, "There was an issue with that game selection. Please try starting the game again.");
            return;
        }

        if (messageText && messageId) {
            if (nextStepName && !nextStepName.startsWith('awaiting_') && nextStepName !== 'confirm_final_bet') {
                 nextStateData.step = nextStepName;
                 userStateCache.set(userId, {
                     state: `awaiting_${gameKey}_${nextStepName}`,
                     chatId: chatId, messageId: messageId, data: nextStateData, timestamp: Date.now(),
                 });
            }
            await bot.editMessageText(messageText, {
                chat_id: chatId, message_id: messageId,
                reply_markup: { inline_keyboard: inlineKeyboard }, parse_mode: 'MarkdownV2'
            }).catch(e => {
                console.error(`${logPrefix} Error editing message for game step '${nextStepName}':`, e.message);
                safeSendMessage(chatId, "An error occurred updating the game options. Please try again.");
            });
        } else if (!messageId) {
            console.warn(`${logPrefix} No messageId provided to proceedToGameStep. Cannot edit message.`);
        }
    } catch (error) {
        console.error(`${logPrefix} Error in proceedToGameStep for game ${gameKey}:`, error);
        await safeSendMessage(chatId, "An unexpected error occurred while processing your game choice. Please try again.");
        userStateCache.delete(userId);
    }
}
// --- End Universal Game Step Processor ---


// --- Core Bet Placement Logic ---
/**
 * Handles the core logic of placing a bet.
 * @param {string} userId
 * @param {string} chatId
 * @param {string} gameKey
 * @param {object} betDetails For `bets` table JSONB column.
 * @param {bigint} betAmountLamports
 * @returns {Promise<{success: boolean, betId?: number, error?: string, insufficientBalance?: boolean, newBalance?: bigint}>}
 */
async function placeBet(userId, chatId, gameKey, betDetails, betAmountLamports) {
    const logPrefix = `[PlaceBet User ${userId} Game ${gameKey}]`;
    let client = null;
    try {
        client = await pool.connect();
        await client.query('BEGIN');

        const { isNewUser } = await ensureUserExists(userId, client);

        const balanceUpdateResult = await updateUserBalanceAndLedger(client, userId, -betAmountLamports, `bet_placed:${gameKey}`, {}, `Placed bet on ${GAME_CONFIG[gameKey].name}`);
        if (!balanceUpdateResult.success) {
            await client.query('ROLLBACK');
            return {
                success: false,
                error: balanceUpdateResult.error || 'Failed to update balance.',
                insufficientBalance: balanceUpdateResult.error === 'Insufficient balance'
            };
        }
        const newBalanceAfterBet = balanceUpdateResult.newBalance;

        const betRecordResult = await createBetRecord(client, userId, chatId, gameKey, betDetails, betAmountLamports);
        if (!betRecordResult.success || !betRecordResult.betId) {
            await client.query('ROLLBACK');
            return { success: false, error: betRecordResult.error || 'Failed to create bet record.' };
        }
        const betId = betRecordResult.betId;

        await updateUserWagerStats(userId, betAmountLamports, client);
        await _handleReferralChecks(userId, betId, betAmountLamports, client);
        await updateUserLastBetAmount(userId, gameKey, betAmountLamports, client);

        const userBetsMemoryCache = userLastBetAmounts.get(userId) || new Map();
        userBetsMemoryCache.set(gameKey, betAmountLamports);
        userLastBetAmounts.set(userId, userBetsMemoryCache);

        await client.query('COMMIT');
        console.log(`${logPrefix} Bet ID ${betId} successfully placed for ${formatSol(betAmountLamports)} SOL. New balance: ${formatSol(newBalanceAfterBet)} SOL.`);
        return { success: true, betId, newBalance: newBalanceAfterBet };

    } catch (error) {
        if (client) {
            try { await client.query('ROLLBACK'); } catch (rbErr) { console.error(`${logPrefix} Rollback failed:`, rbErr); }
        }
        console.error(`${logPrefix} Error placing bet:`, error);
        let returnError = `Error placing bet: ${error.message}`;
        if (error.message.toLowerCase().includes('insufficient balance')) {
            returnError = 'Insufficient balance.';
            return { success: false, error: returnError, insufficientBalance: true};
        }
        return { success: false, error: returnError };
    } finally {
        if (client) client.release();
    }
}
// --- End Core Bet Placement Logic ---

// --- Game Result Handlers (Continued) ---

async function handleCoinflipGame(userId, chatId, originalMessageId, betAmountLamports, choice) {
    const gameKey = 'coinflip';
    const logPrefix = `[CoinflipGame User ${userId} Bet ${formatSol(betAmountLamports)}]`;
    console.log(`${logPrefix} Handling Coinflip for choice: ${choice}`);
    let client = null;
    let currentMessageId = originalMessageId;

    try {
        const betPlacement = await placeBet(userId, chatId, gameKey, { choice }, betAmountLamports);
        if (!betPlacement.success) {
            let replyText = betPlacement.error || "Failed to place your Coinflip bet.";
            if (betPlacement.insufficientBalance) {
                 const currentBal = await getUserBalance(userId);
                 replyText = `⚠️ Insufficient balance for ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL Coinflip bet. Your balance: ${escapeMarkdownV2(formatSol(currentBal))} SOL.`;
                 return bot.editMessageText(replyText, {
                    chat_id: chatId, message_id: originalMessageId, parse_mode: 'MarkdownV2',
                    reply_markup: { inline_keyboard: [[{ text: '💰 Quick Deposit', callback_data: 'quick_deposit' }],[{ text: '↩️ Back to Coinflip', callback_data: 'menu:coinflip' }]] }
                 }).catch(e => safeSendMessage(chatId, replyText, {parse_mode: 'MarkdownV2'}));
            }
             return bot.editMessageText(replyText, { chat_id: chatId, message_id: originalMessageId, parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: [[{text: '↩️ Back to Coinflip', callback_data: 'menu:coinflip'}]]} }).catch(e => safeSendMessage(chatId, replyText, {parse_mode: 'MarkdownV2'}));
        }
        const betId = betPlacement.betId;

        const animationStartText = `🪙 Flipping a coin for your ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL bet on *${escapeMarkdownV2(choice)}*\\!\\!\\!`;
        if (currentMessageId) {
            await bot.editMessageText(animationStartText, { chat_id: chatId, message_id: currentMessageId, parse_mode: 'MarkdownV2' }).catch(e => {
                console.warn(`${logPrefix} Failed to edit message for animation start, sending new. Error: ${e.message}`);
                currentMessageId = null;
            });
        }
        if (!currentMessageId) {
            const newMsg = await safeSendMessage(chatId, animationStartText, {parse_mode: 'MarkdownV2'});
            currentMessageId = newMsg?.message_id;
        }

        const animationFrames = ["🌑", "🌒", "🌓", "🌔", "🌕", "🌖", "🌗", "🌘", "🪙"];
        for (let i = 0; i < animationFrames.length; i++) {
            await sleep(300);
            if (currentMessageId) {
                await bot.editMessageText(
                    `Flipping\\.\\.\\. ${animationFrames[i]}\nYour bet: ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL on *${escapeMarkdownV2(choice)}*`,
                    { chat_id: chatId, message_id: currentMessageId, parse_mode: 'MarkdownV2' }
                ).catch(e => { console.warn(`${logPrefix} Coinflip animation edit error: ${e.message}.`); });
            }
        }

        const { outcome } = playCoinflip(choice);
        const win = outcome === choice;
        const houseEdge = GAME_CONFIG[gameKey].houseEdge;
        const payoutOnWinLamports = win ? BigInt(Math.floor(Number(betAmountLamports * 2n) * (1 - houseEdge))) : 0n;
        const profitLamports = win ? payoutOnWinLamports - betAmountLamports : -betAmountLamports;

        client = await pool.connect();
        await client.query('BEGIN');
        let finalBalanceUpdateResult;
        if (win) {
            finalBalanceUpdateResult = await updateUserBalanceAndLedger(client, userId, payoutOnWinLamports, `bet_win:${gameKey}`, { betId });
        } else {
            finalBalanceUpdateResult = { success: true, newBalance: betPlacement.newBalance };
        }
        await updateBetStatus(client, betId, win ? 'completed_win' : 'completed_loss', payoutOnWinLamports);
        await client.query('COMMIT');

        let resultText = `Coin landed on: *${escapeMarkdownV2(outcome)}*\\!\n\n`;
        if (win) {
            resultText += `🎉 You *WON*\\! 🎉\nReturned: ${escapeMarkdownV2(formatSol(payoutOnWinLamports))} SOL (Profit: ${escapeMarkdownV2(formatSol(profitLamports))} SOL)`;
        } else {
            resultText += `😥 You *LOST*\\! 😥\nLost: ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL`;
        }
        resultText += `\n\nNew balance: ${escapeMarkdownV2(formatSol(finalBalanceUpdateResult.newBalance))} SOL`;

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
        const userFacingError = "Sorry, an error occurred while playing Coinflip. If SOL was deducted, it will be handled. Please check your balance or contact support if issues persist.";
        if (currentMessageId) {
            bot.editMessageText(userFacingError, {chat_id: chatId, message_id: currentMessageId, reply_markup: {inline_keyboard: [[{text: '↩️ Back to Games', callback_data: 'menu:main'}]]}, parse_mode: 'MarkdownV2'})
               .catch(e => safeSendMessage(chatId, userFacingError, {parse_mode: 'MarkdownV2'}));
        } else {
            safeSendMessage(chatId, userFacingError, {parse_mode: 'MarkdownV2'});
        }
    } finally {
        if (client) client.release();
    }
}

async function handleRaceGame(userId, chatId, originalMessageId, betAmountLamports, chosenLane) {
    const gameKey = 'race';
    const logPrefix = `[RaceGame User ${userId} Bet ${formatSol(betAmountLamports)} Lane ${chosenLane}]`;
    console.log(`${logPrefix} Handling Race.`);
    let client = null;
    let currentMessageId = originalMessageId;
    const raceHorses = RACE_HORSES;
    const totalLanes = raceHorses.length;

    if (chosenLane < 1 || chosenLane > totalLanes) {
        console.error(`${logPrefix} Invalid chosenLane: ${chosenLane}`);
        safeSendMessage(chatId, "Invalid horse selection. Please try again.", {parse_mode: 'MarkdownV2'});
        return;
    }
    const selectedHorse = raceHorses[chosenLane - 1];

    try {
        const betPlacement = await placeBet(userId, chatId, gameKey, { chosenLane, chosenHorseName: selectedHorse.name }, betAmountLamports);
        if (!betPlacement.success) {
            let replyText = betPlacement.error || "Failed to place your Race bet.";
            if (betPlacement.insufficientBalance) {
                const currentBal = await getUserBalance(userId);
                replyText = `⚠️ Insufficient balance for ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL Race bet. Your balance: ${escapeMarkdownV2(formatSol(currentBal))} SOL.`;
                return bot.editMessageText(replyText, {
                    chat_id: chatId, message_id: originalMessageId, parse_mode: 'MarkdownV2',
                    reply_markup: { inline_keyboard: [[{ text: '💰 Quick Deposit', callback_data: 'quick_deposit' }],[{ text: '↩️ Back to Race', callback_data: 'menu:race' }]] }
                 }).catch(e => safeSendMessage(chatId, replyText, {parse_mode: 'MarkdownV2'}));
            }
            return bot.editMessageText(replyText, { chat_id: chatId, message_id: originalMessageId, parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: [[{text: '↩️ Back to Race', callback_data: 'menu:race'}]]} }).catch(e => safeSendMessage(chatId, replyText, {parse_mode: 'MarkdownV2'}));
        }
        const betId = betPlacement.betId;

        const animationStartText = `🐎 The race is on\\! You bet ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL on ${escapeMarkdownV2(selectedHorse.emoji)} *${escapeMarkdownV2(selectedHorse.name)}* \\(Lane ${chosenLane}\\)\\.\n\n🏁 \\[\\.\\.\\.\\.\\.\\.\\]`;
        if (currentMessageId) {
            await bot.editMessageText(animationStartText, { chat_id: chatId, message_id: currentMessageId, parse_mode: 'MarkdownV2' }).catch(e => {
                console.warn(`${logPrefix} Failed to edit message for race start, sending new. Error: ${e.message}`);
                currentMessageId = null;
            });
        }
        if (!currentMessageId) {
            const newMsg = await safeSendMessage(chatId, animationStartText, {parse_mode: 'MarkdownV2'});
            currentMessageId = newMsg?.message_id;
        }

        const raceTrackLength = 10;
        let positions = Array(totalLanes).fill(0);
        const progressEmoji = '🏇';
        const trackEmoji = '➖';

        for (let lap = 0; lap < raceTrackLength * 2 && !positions.some(p => p >= raceTrackLength) ; lap++) {
            await sleep(500 + Math.random() * 300);
            for (let i = 0; i < totalLanes; i++) {
                if (Math.random() < 0.35) {
                    positions[i] = Math.min(raceTrackLength, positions[i] + 1);
                }
            }

            let raceDisplay = `Race Progress \\(Lap ${Math.floor(lap/totalLanes)+1}\\):\n`;
            for (let i = 0; i < totalLanes; i++) {
                const progress = progressEmoji.repeat(positions[i]) + trackEmoji.repeat(raceTrackLength - positions[i]);
                raceDisplay += `${escapeMarkdownV2(raceHorses[i].emoji)} \\[${escapeMarkdownV2(progress)}\\] ${chosenLane === (i + 1) ? "*(Your Pick)*" : ""}\n`;
            }
            raceDisplay += `\nYou bet ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL on ${escapeMarkdownV2(selectedHorse.emoji)} *${escapeMarkdownV2(selectedHorse.name)}*\\.`;

            if (currentMessageId) {
                 await bot.editMessageText(raceDisplay, { chat_id: chatId, message_id: currentMessageId, parse_mode: 'MarkdownV2' }).catch(e => console.warn(`${logPrefix} Race animation edit error: ${e.message}`));
            }
            if (positions.some(p => p >= raceTrackLength)) break;
        }

        const { winningLane } = simulateRace(totalLanes);
        const winningHorse = raceHorses[winningLane - 1];
        const win = winningLane === chosenLane;
        const houseEdge = GAME_CONFIG[gameKey].houseEdge;
        const basePayoutMultiplierForHorse = selectedHorse.payoutMultiplier;
        const winnings = win ? BigInt(Math.floor(Number(betAmountLamports) * basePayoutMultiplierForHorse)) : 0n;
        const netWinnings = win ? BigInt(Math.floor(Number(winnings) * (1 - houseEdge))) : 0n;
        const totalReturnOnWin = win ? betAmountLamports + netWinnings : 0n;
        const profitLamports = win ? netWinnings : -betAmountLamports;

        client = await pool.connect();
        await client.query('BEGIN');
        let finalBalanceUpdateResult;
        if (win) {
            finalBalanceUpdateResult = await updateUserBalanceAndLedger(client, userId, totalReturnOnWin, `bet_win:${gameKey}`, { betId });
        } else {
            finalBalanceUpdateResult = { success: true, newBalance: betPlacement.newBalance };
        }
        await updateBetStatus(client, betId, win ? 'completed_win' : 'completed_loss', totalReturnOnWin);
        await client.query('COMMIT');

        let resultText = `🏁 The race is over\\! ${escapeMarkdownV2(winningHorse.emoji)} *${escapeMarkdownV2(winningHorse.name)}* \\(Lane ${winningLane}\\) is the winner\\!\n\n`;
        if (win) {
            resultText += `🎉 You *WON*\\! 🎉\nReturned: ${escapeMarkdownV2(formatSol(totalReturnOnWin))} SOL (Profit: ${escapeMarkdownV2(formatSol(profitLamports))} SOL)`;
        } else {
            resultText += `😥 You *LOST*\\! 😥\nLost: ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL`;
        }
        resultText += `\n\nNew balance: ${escapeMarkdownV2(formatSol(finalBalanceUpdateResult.newBalance))} SOL`;

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
        const userFacingError = "Sorry, an error occurred while playing Race. If SOL was deducted, it will be handled.";
        if (currentMessageId) {
             bot.editMessageText(userFacingError, {chat_id: chatId, message_id: currentMessageId, reply_markup: {inline_keyboard: [[{text: '↩️ Back to Games', callback_data: 'menu:main'}]]}, parse_mode: 'MarkdownV2'})
                .catch(e => safeSendMessage(chatId, userFacingError, {parse_mode: 'MarkdownV2'}));
        } else {
             safeSendMessage(chatId, userFacingError, {parse_mode: 'MarkdownV2'});
        }
    } finally {
        if (client) client.release();
    }
}

async function handleSlotsGame(userId, chatId, originalMessageId, betAmountLamports) {
    const gameKey = 'slots';
    const logPrefix = `[SlotsGame User ${userId} Bet ${formatSol(betAmountLamports)}]`;
    console.log(`${logPrefix} Handling Slots.`);
    let client = null;
    let currentMessageId = originalMessageId;

    try {
        client = await pool.connect();
        await client.query('BEGIN');

        const jackpotContributionPercent = GAME_CONFIG.slots.jackpotContributionPercent;
        let contributionToJackpot = 0n;
        if (jackpotContributionPercent > 0) {
            contributionToJackpot = BigInt(Math.floor(Number(betAmountLamports) * jackpotContributionPercent));
            if (contributionToJackpot > 0n) {
                const incremented = await incrementJackpotAmount(gameKey, contributionToJackpot, client);
                if (incremented) {
                    console.log(`${logPrefix} Contributed ${formatSol(contributionToJackpot)} SOL to slots jackpot.`);
                    currentSlotsJackpotLamports += contributionToJackpot;
                } else {
                    console.error(`${logPrefix} CRITICAL: Failed to increment slots jackpot in DB.`);
                }
            }
        }

        const betPlacement = await placeBet(userId, chatId, gameKey, {}, betAmountLamports);
        if (!betPlacement.success) {
            await client.query('ROLLBACK');
            client.release(); client = null;
            let replyText = betPlacement.error || "Failed to place your Slots bet.";
            if (betPlacement.insufficientBalance) {
                const currentBal = await getUserBalance(userId);
                replyText = `⚠️ Insufficient balance for ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL Slots bet. Your balance: ${escapeMarkdownV2(formatSol(currentBal))} SOL.`;
                return bot.editMessageText(replyText, {
                    chat_id: chatId, message_id: originalMessageId, parse_mode: 'MarkdownV2',
                    reply_markup: { inline_keyboard: [[{ text: '💰 Quick Deposit', callback_data: 'quick_deposit' }],[{ text: '↩️ Back to Slots', callback_data: 'menu:slots' }]] }
                 }).catch(e => safeSendMessage(chatId, replyText, {parse_mode: 'MarkdownV2'}));
            }
            return bot.editMessageText(replyText, { chat_id: chatId, message_id: originalMessageId, parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: [[{text: '↩️ Back to Slots', callback_data: 'menu:slots'}]]} }).catch(e => safeSendMessage(chatId, replyText, {parse_mode: 'MarkdownV2'}));
        }
        const betId = betPlacement.betId;
        await client.query('COMMIT'); // Commit bet placement and jackpot contribution
        client.release(); client = null;

        const animationStartText = `🎰 Spinning the slots for your ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL bet\\!\\!\\!`;
        if (currentMessageId) {
            await bot.editMessageText(animationStartText, { chat_id: chatId, message_id: currentMessageId, parse_mode: 'MarkdownV2' }).catch(e => { currentMessageId = null; });
        }
        if (!currentMessageId) {
            const newMsg = await safeSendMessage(chatId, animationStartText, {parse_mode: 'MarkdownV2'});
            currentMessageId = newMsg?.message_id;
        }

        const slotEmojis = { 'C': '🍒', 'L': '🍋', 'O': '🍊', 'B': '🔔', '7': '❼', 'J': '💎' };
        const initialReels = `[❓|❓|❓]`;
        if(currentMessageId) await bot.editMessageText(escapeMarkdownV2(initialReels) + `\nSpinning...`, { chat_id: chatId, message_id: currentMessageId, parse_mode: 'MarkdownV2' }).catch(e => {});

        let tempReelDisplay = ["[❓","❓","❓]"];
        for (let i = 0; i < 3; i++) {
            await sleep(700 + i * 100);
            const randomSymbolForAnimation = Object.keys(slotEmojis)[Math.floor(Math.random() * Object.keys(slotEmojis).length)];
            tempReelDisplay[i] = slotEmojis[randomSymbolForAnimation];
            if (currentMessageId) {
                await bot.editMessageText(escapeMarkdownV2(`[${tempReelDisplay.slice(0, i + 1).join('|')}${i < 2 ? ('|❓'.repeat(2 - i)) : ''}]`) + `\nSpinning...`, { chat_id: chatId, message_id: currentMessageId, parse_mode: 'MarkdownV2' }).catch(e => {});
            }
        }
        await sleep(500);

        const { symbols, payoutMultiplier, isJackpotWin } = simulateSlots();
        const displaySymbols = symbols.map(s => slotEmojis[s] || s).join(' | ');
        const houseEdge = GAME_CONFIG.slots.houseEdge;

        let finalPayoutLamports = 0n;
        let profitLamports = -betAmountLamports;
        let resultMessagePart = "";
        let win = false;

        client = await pool.connect();
        await client.query('BEGIN');

        if (isJackpotWin) {
            win = true;
            const jackpotAmountToAward = await getJackpotAmount(gameKey, client);
            if (jackpotAmountToAward > 0n) {
                finalPayoutLamports = jackpotAmountToAward;
                profitLamports = finalPayoutLamports - betAmountLamports;
                resultMessagePart = `💎💎💎 *JACKPOT WIN* 💎💎💎\nYou won the progressive jackpot of ${escapeMarkdownV2(formatSol(jackpotAmountToAward))} SOL\\!`;
                await updateUserBalanceAndLedger(client, userId, finalPayoutLamports, `bet_win_jackpot:${gameKey}`, { betId });
                await updateJackpotAmount(gameKey, GAME_CONFIG.slots.jackpotSeedLamports, client);
                currentSlotsJackpotLamports = GAME_CONFIG.slots.jackpotSeedLamports;
                if(typeof notifyAdmin === "function") await notifyAdmin(`🎉 User ${escapeMarkdownV2(userId)} HIT THE SLOTS JACKPOT for ${escapeMarkdownV2(formatSol(jackpotAmountToAward))} SOL! Bet ID: ${betId}`);
            } else {
                isJackpotWin = false; // Fallback if jackpot is somehow zero
                const fixedMultiplierForEmptyJackpot = 200;
                const grossWinnings = BigInt(Number(betAmountLamports) * fixedMultiplierForEmptyJackpot);
                const netWinnings = BigInt(Math.floor(Number(grossWinnings) * (1 - houseEdge)));
                finalPayoutLamports = betAmountLamports + netWinnings;
                profitLamports = netWinnings;
                resultMessagePart = `💥 Triple Diamonds\\! You won ${escapeMarkdownV2(formatSol(finalPayoutLamports))} SOL\\! (Jackpot was empty)`;
                 await updateUserBalanceAndLedger(client, userId, finalPayoutLamports, `bet_win:${gameKey}`, { betId });
            }
        }

        if (!isJackpotWin && payoutMultiplier > 0) {
            win = true;
            const grossWinnings = BigInt(Math.floor(Number(betAmountLamports) * payoutMultiplier));
            const netWinningsAfterEdge = BigInt(Math.floor(Number(grossWinnings) * (1 - houseEdge)));
            finalPayoutLamports = betAmountLamports + netWinningsAfterEdge;
            profitLamports = netWinningsAfterEdge;
            resultMessagePart = `🎉 You *WON*\\! 🎉\nReturned: ${escapeMarkdownV2(formatSol(finalPayoutLamports))} SOL (Profit: ${escapeMarkdownV2(formatSol(profitLamports))} SOL)`;
            await updateUserBalanceAndLedger(client, userId, finalPayoutLamports, `bet_win:${gameKey}`, { betId });
        } else if (!isJackpotWin && !win) {
            resultMessagePart = `😥 You *LOST*\\! 😥\nLost: ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL`;
        }

        await updateBetStatus(client, betId, win ? 'completed_win' : 'completed_loss', finalPayoutLamports);
        const finalBalanceAfterOutcome = betPlacement.newBalance + profitLamports;
        await client.query('COMMIT');
        client.release(); client = null;

        let fullResultMessage = `🎰 Result: *${escapeMarkdownV2(displaySymbols)}*\n\n${resultMessagePart}`;
        fullResultMessage += `\n\nNew balance: ${escapeMarkdownV2(formatSol(finalBalanceAfterOutcome))} SOL`;
        const currentJackpotDisplay = formatSol(await getJackpotAmount(gameKey));
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
        const userFacingError = "Sorry, an error occurred while playing Slots.";
        if (currentMessageId) {
            bot.editMessageText(userFacingError, {chat_id: chatId, message_id: currentMessageId, reply_markup: {inline_keyboard: [[{text: '↩️ Back to Games', callback_data: 'menu:main'}]]}, parse_mode: 'MarkdownV2'})
                .catch(e => safeSendMessage(chatId, userFacingError, {parse_mode: 'MarkdownV2'}));
        } else {
            safeSendMessage(chatId, userFacingError, {parse_mode: 'MarkdownV2'});
        }
    } finally {
        if (client) client.release();
    }
}

async function handleRouletteGame(userId, chatId, originalMessageId, betAmountLamports, betType, betValue) {
    const gameKey = 'roulette';
    const logPrefix = `[RouletteGame User ${userId} Bet ${formatSol(betAmountLamports)} Type ${betType} Val ${betValue}]`;
    console.log(`${logPrefix} Handling Roulette.`);
    let client = null;
    let currentMessageId = originalMessageId;

    try {
        const betPlacement = await placeBet(userId, chatId, gameKey, { betType, betValue }, betAmountLamports);
        if (!betPlacement.success) {
            let replyText = betPlacement.error || "Failed to place your Roulette bet.";
            if (betPlacement.insufficientBalance) {
                const currentBal = await getUserBalance(userId);
                replyText = `⚠️ Insufficient balance for ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL Roulette bet. Your balance: ${escapeMarkdownV2(formatSol(currentBal))} SOL.`;
                return bot.editMessageText(replyText, {
                    chat_id: chatId, message_id: originalMessageId, parse_mode: 'MarkdownV2',
                    reply_markup: { inline_keyboard: [[{ text: '💰 Quick Deposit', callback_data: 'quick_deposit' }],[{ text: '↩️ Back to Roulette', callback_data: 'menu:roulette' }]] }
                 }).catch(e => safeSendMessage(chatId, replyText, {parse_mode: 'MarkdownV2'}));
            }
            return bot.editMessageText(replyText, { chat_id: chatId, message_id: originalMessageId, parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: [[{text: '↩️ Back to Roulette', callback_data: 'menu:roulette'}]]} }).catch(e => safeSendMessage(chatId, replyText, {parse_mode: 'MarkdownV2'}));
        }
        const betId = betPlacement.betId;

        const betValueDisplay = betValue ? ` (${escapeMarkdownV2(String(betValue))})` : '';
        const animationStartText = `⚪️ Spinning the Roulette wheel for your ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL bet on *${escapeMarkdownV2(betType)}${betValueDisplay}*\\!`;
        if (currentMessageId) {
            await bot.editMessageText(animationStartText, { chat_id: chatId, message_id: currentMessageId, parse_mode: 'MarkdownV2' }).catch(e => { currentMessageId = null; });
        }
        if (!currentMessageId) {
            const newMsg = await safeSendMessage(chatId, animationStartText, {parse_mode: 'MarkdownV2'});
            currentMessageId = newMsg?.message_id;
        }

        const rouletteNumbersAnim = [0, 32, 15, 19, 4, 21, 2, 25, 17, 34, 6, 27, 13, 36, 11, 30, 8, 23, 10, 5, 24, 16, 33, 1, 20, 14, 31, 9, 22, 18, 29, 7, 28, 12, 35, 3, 26];
        for (let i = 0; i < 10 + Math.floor(Math.random()*5); i++) { // Variable animation length
            await sleep(250 + i * 30); // Slow down effect
            const displayedNum = rouletteNumbersAnim[Math.floor(Math.random() * rouletteNumbersAnim.length)];
            const numColorAnim = displayedNum === 0 ? '🟢' : (getRoulettePayoutMultiplier('red', null, displayedNum) ? '🔴' : '⚫️');
            if (currentMessageId) {
                await bot.editMessageText(
                    `Spinning\\.\\.\\. *${escapeMarkdownV2(numColorAnim + String(displayedNum))}*\nYour bet: ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL on *${escapeMarkdownV2(betType)}${betValueDisplay}*`,
                    { chat_id: chatId, message_id: currentMessageId, parse_mode: 'MarkdownV2' }
                ).catch(e => { console.warn(`${logPrefix} Roulette animation edit error: ${e.message}`); });
            }
        }

        const { winningNumber } = simulateRouletteSpin(); // from Part 4
        const basePayoutMultiplier = getRoulettePayoutMultiplier(betType, betValue, winningNumber); // from Part 4
        const houseEdge = GAME_CONFIG[gameKey].houseEdge;
        let win = basePayoutMultiplier > 0;

        let payoutOnWinLamports = 0n;
        let profitLamports = -betAmountLamports;

        if (win) {
            const grossWinnings = BigInt(Math.floor(Number(betAmountLamports) * basePayoutMultiplier));
            const netWinningsAfterEdge = BigInt(Math.floor(Number(grossWinnings) * (1 - houseEdge)));
            payoutOnWinLamports = betAmountLamports + netWinningsAfterEdge; // Stake + Net Winnings
            profitLamports = netWinningsAfterEdge;
        }

        client = await pool.connect();
        await client.query('BEGIN');
        let finalBalanceUpdateResult;
        if (win) {
            finalBalanceUpdateResult = await updateUserBalanceAndLedger(client, userId, payoutOnWinLamports, `bet_win:${gameKey}`, { betId });
        } else {
            finalBalanceUpdateResult = { success: true, newBalance: betPlacement.newBalance };
        }
        await updateBetStatus(client, betId, win ? 'completed_win' : 'completed_loss', payoutOnWinLamports);
        await client.query('COMMIT');

        const numberColor = winningNumber === 0 ? '🟢' : (getRoulettePayoutMultiplier('red', null, winningNumber) ? '🔴' : '⚫️');
        let resultText = `The ball landed on: *${escapeMarkdownV2(numberColor + String(winningNumber))}*\\!\n\n`;
        if (win) {
            resultText += `🎉 You *WON*\\! 🎉\nReturned: ${escapeMarkdownV2(formatSol(payoutOnWinLamports))} SOL (Profit: ${escapeMarkdownV2(formatSol(profitLamports))} SOL)`;
        } else {
            resultText += `😥 You *LOST*\\! 😥\nLost: ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL`;
        }
        resultText += `\n\nNew balance: ${escapeMarkdownV2(formatSol(finalBalanceUpdateResult.newBalance))} SOL`;

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
        const userFacingError = "Sorry, an error occurred while playing Roulette.";
        if (currentMessageId) {
            bot.editMessageText(userFacingError, {chat_id: chatId, message_id: currentMessageId, reply_markup: {inline_keyboard: [[{text: '↩️ Back to Games', callback_data: 'menu:main'}]]}, parse_mode: 'MarkdownV2'})
               .catch(e => safeSendMessage(chatId, userFacingError, {parse_mode: 'MarkdownV2'}));
        } else {
            safeSendMessage(chatId, userFacingError, {parse_mode: 'MarkdownV2'});
        }
    } finally {
        if (client) client.release();
    }
}

async function handleWarGame(userId, chatId, originalMessageId, betAmountLamports) {
    const gameKey = 'war';
    const logPrefix = `[WarGame User ${userId} Bet ${formatSol(betAmountLamports)}]`;
    console.log(`${logPrefix} Handling Casino War.`);
    let client = null;
    let currentMessageId = originalMessageId;

    try {
        const betPlacement = await placeBet(userId, chatId, gameKey, {}, betAmountLamports);
        if (!betPlacement.success) {
            let replyText = betPlacement.error || "Failed to place your Casino War bet.";
            if (betPlacement.insufficientBalance) {
                const currentBal = await getUserBalance(userId);
                replyText = `⚠️ Insufficient balance for ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL War bet. Your balance: ${escapeMarkdownV2(formatSol(currentBal))} SOL.`;
                return bot.editMessageText(replyText, {
                    chat_id: chatId, message_id: originalMessageId, parse_mode: 'MarkdownV2',
                    reply_markup: { inline_keyboard: [[{ text: '💰 Quick Deposit', callback_data: 'quick_deposit' }],[{ text: '↩️ Back to War', callback_data: 'menu:war' }]] }
                 }).catch(e => safeSendMessage(chatId, replyText, {parse_mode: 'MarkdownV2'}));
            }
            return bot.editMessageText(replyText, { chat_id: chatId, message_id: originalMessageId, parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: [[{text: '↩️ Back to War', callback_data: 'menu:war'}]]} }).catch(e => safeSendMessage(chatId, replyText, {parse_mode: 'MarkdownV2'}));
        }
        const betId = betPlacement.betId;

        const animationStartText = `🃏 Dealing cards for Casino War\\! Your bet: ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL`;
        if (currentMessageId) {
            await bot.editMessageText(animationStartText, { chat_id: chatId, message_id: currentMessageId, parse_mode: 'MarkdownV2' }).catch(e => { currentMessageId = null; });
        }
        if (!currentMessageId) {
            const newMsg = await safeSendMessage(chatId, animationStartText, {parse_mode: 'MarkdownV2'});
            currentMessageId = newMsg?.message_id;
        }
        await sleep(1000); // Pause for dealing effect

        const { result, playerCard, dealerCard, playerRank, dealerRank } = simulateWar(); // from Part 4
        const houseEdge = GAME_CONFIG[gameKey].houseEdge;

        let payoutAmountToCredit = 0n; // Amount to credit back to balance (includes stake for win/push)
        let profitLamports = -betAmountLamports;
        let betStatusDB = 'completed_loss';
        let resultTextPart = "";

        if (result === 'win') {
            const netWinnings = BigInt(Math.floor(Number(betAmountLamports) * (1 - houseEdge))); // Win 1x stake, edge on winnings
            payoutAmountToCredit = betAmountLamports + netWinnings; // Return stake + net winnings
            profitLamports = netWinnings;
            betStatusDB = 'completed_win';
            resultTextPart = `🎉 You *WON*\\! 🎉\nReturned: ${escapeMarkdownV2(formatSol(payoutAmountToCredit))} SOL (Profit: ${escapeMarkdownV2(formatSol(profitLamports))} SOL)`;
        } else if (result === 'push') {
            payoutAmountToCredit = betAmountLamports; // Return original stake
            profitLamports = 0n;
            betStatusDB = 'completed_push';
            resultTextPart = `🔵 It's a *PUSH*\\! 🔵\nStake returned: ${escapeMarkdownV2(formatSol(payoutAmountToCredit))} SOL`;
        } else { // loss
            // payoutAmountToCredit remains 0n (already deducted)
            betStatusDB = 'completed_loss';
            resultTextPart = `😥 You *LOST*\\! 😥\nLost: ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL`;
        }

        client = await pool.connect();
        await client.query('BEGIN');
        let finalBalanceUpdateResult;
        if (result === 'win' || result === 'push') {
            finalBalanceUpdateResult = await updateUserBalanceAndLedger(client, userId, payoutAmountToCredit, `bet_${result}:${gameKey}`, { betId });
        } else { // loss
            finalBalanceUpdateResult = { success: true, newBalance: betPlacement.newBalance };
        }
        await updateBetStatus(client, betId, betStatusDB, payoutAmountToCredit);
        await client.query('COMMIT');

        let fullResultText = `Your Card: *${escapeMarkdownV2(playerCard)}* (Rank: ${playerRank})\nDealer Card: *${escapeMarkdownV2(dealerCard)}* (Rank: ${dealerRank})\n\n${resultTextPart}`;
        fullResultText += `\n\nNew balance: ${escapeMarkdownV2(formatSol(finalBalanceUpdateResult.newBalance))} SOL`;

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
        const userFacingError = "Sorry, an error occurred while playing Casino War.";
        if (currentMessageId) {
            bot.editMessageText(userFacingError, {chat_id: chatId, message_id: currentMessageId, reply_markup: {inline_keyboard: [[{text: '↩️ Back to Games', callback_data: 'menu:main'}]]}, parse_mode: 'MarkdownV2'})
               .catch(e => safeSendMessage(chatId, userFacingError, {parse_mode: 'MarkdownV2'}));
        } else {
            safeSendMessage(chatId, userFacingError, {parse_mode: 'MarkdownV2'});
        }
    } finally {
        if (client) client.release();
    }
}

async function handleCrashGame(userId, chatId, originalMessageId, betAmountLamports) {
    const gameKey = 'crash';
    const logPrefix = `[CrashGame User ${userId} Bet ${formatSol(betAmountLamports)}]`;
    console.log(`${logPrefix} Starting Crash game.`);
    let client = null;
    let gameLoopTimeoutId = null;
    const gameInstanceId = `crash_${userId}_${Date.now()}`; // Unique ID for this game instance
    let currentMessageId = originalMessageId;

    try {
        const betPlacement = await placeBet(userId, chatId, gameKey, {}, betAmountLamports);
        if (!betPlacement.success) {
            let replyText = betPlacement.error || "Failed to place your Crash bet.";
            if (betPlacement.insufficientBalance) {
                const currentBal = await getUserBalance(userId);
                replyText = `⚠️ Insufficient balance for ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL Crash bet. Your balance: ${escapeMarkdownV2(formatSol(currentBal))} SOL.`;
                return bot.editMessageText(replyText, {
                    chat_id: chatId, message_id: originalMessageId, parse_mode: 'MarkdownV2',
                    reply_markup: { inline_keyboard: [[{ text: '💰 Quick Deposit', callback_data: 'quick_deposit' }],[{ text: '↩️ Back to Crash', callback_data: 'menu:crash' }]] }
                 }).catch(e => safeSendMessage(chatId, replyText, {parse_mode: 'MarkdownV2'}));
            }
            return bot.editMessageText(replyText, { chat_id: chatId, message_id: originalMessageId, parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: [[{text: '↩️ Back to Crash', callback_data: 'menu:crash'}]]} }).catch(e => safeSendMessage(chatId, replyText, {parse_mode: 'MarkdownV2'}));
        }
        const betId = betPlacement.betId;

        const { crashMultiplier: targetCrashMultiplier } = simulateCrash(); // from Part 4
        console.log(`${logPrefix} Target crash multiplier: ${targetCrashMultiplier.toFixed(2)}x`);

        let currentMultiplier = 1.00;
        const initialMessageText = `🚀 *Crash Game Started\\!* Bet: ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL\nMultiplier: *${currentMultiplier.toFixed(2)}x*\n\nGet ready to Cash Out\\!`;
        const initialKeyboard = { inline_keyboard: [[{ text: `💸 Cash Out at ${currentMultiplier.toFixed(2)}x!`, callback_data: `crash_cash_out:${gameInstanceId}` }]] };

        if (currentMessageId) {
            await bot.editMessageText(initialMessageText, { chat_id: chatId, message_id: currentMessageId, parse_mode: 'MarkdownV2', reply_markup: initialKeyboard }).catch(e => { currentMessageId = null; });
        }
        if (!currentMessageId) {
            const newMsg = await safeSendMessage(chatId, initialMessageText, { parse_mode: 'MarkdownV2', reply_markup: initialKeyboard });
            currentMessageId = newMsg?.message_id;
        }
        if (!currentMessageId) throw new Error("Failed to send or edit initial Crash message.");

        userStateCache.set(`${userId}_${gameInstanceId}`, {
            state: 'awaiting_crash_cashout', chatId: chatId, messageId: currentMessageId,
            data: {
                gameKey, betId, betAmountLamports, targetCrashMultiplier,
                userRequestedCashOut: false, currentMultiplier: currentMultiplier,
                breadcrumb: GAME_CONFIG.crash.name
            },
            timestamp: Date.now()
        });

        const gameLoop = async () => {
            const gameState = userStateCache.get(`${userId}_${gameInstanceId}`);
            if (!gameState) { // State might have been cleared due to error or completion elsewhere
                console.warn(`${logPrefix} Game state for ${gameInstanceId} not found. Ending loop.`);
                if (gameLoopTimeoutId) clearTimeout(gameLoopTimeoutId);
                return;
            }

            let gameEnded = false;
            let cashedOutAtMultiplier = 0;

            if (gameState.data.userRequestedCashOut) {
                gameEnded = true;
                cashedOutAtMultiplier = gameState.data.currentMultiplier; // Multiplier when cashout was processed
                console.log(`${logPrefix} User cashed out at ${cashedOutAtMultiplier.toFixed(2)}x`);
            } else if (gameState.data.currentMultiplier >= gameState.data.targetCrashMultiplier) {
                gameEnded = true;
                gameState.data.currentMultiplier = gameState.data.targetCrashMultiplier; // Cap display at actual crash
                console.log(`${logPrefix} Game crashed at ${gameState.data.targetCrashMultiplier.toFixed(2)}x`);
            }

            if (gameEnded) {
                if (gameLoopTimeoutId) clearTimeout(gameLoopTimeoutId);
                userStateCache.delete(`${userId}_${gameInstanceId}`);
                client = await pool.connect();
                await client.query('BEGIN');

                let finalResultText = "";
                let payoutAmountToCredit = 0n;
                let profitLamportsOutcome = -betAmountLamports; // Default to loss
                let dbBetStatus = 'completed_loss';

                if (gameState.data.userRequestedCashOut) {
                    const grossWinnings = BigInt(Math.floor(Number(betAmountLamports) * cashedOutAtMultiplier));
                    const profitBeforeEdge = grossWinnings - betAmountLamports;
                    const netProfit = BigInt(Math.floor(Number(profitBeforeEdge) * (1 - GAME_CONFIG.crash.houseEdge)));
                    payoutAmountToCredit = betAmountLamports + netProfit;
                    profitLamportsOutcome = netProfit;
                    dbBetStatus = 'completed_win';
                    finalResultText = `💸 Cashed Out at *${cashedOutAtMultiplier.toFixed(2)}x*\\!\n🎉 You *WON*\\! Returned: ${escapeMarkdownV2(formatSol(payoutAmountToCredit))} SOL (Profit: ${escapeMarkdownV2(formatSol(profitLamportsOutcome))} SOL)`;
                    await updateUserBalanceAndLedger(client, userId, payoutAmountToCredit, `bet_win:${gameKey}`, { betId });
                } else { // Game crashed
                    // payoutAmountToCredit remains 0n
                    finalResultText = `💥 CRASHED at *${gameState.data.currentMultiplier.toFixed(2)}x*\\!\n😥 You *LOST*\\! Lost: ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL`;
                }
                await updateBetStatus(client, betId, dbBetStatus, payoutAmountToCredit);
                const finalBalance = betPlacement.newBalance + profitLamportsOutcome;
                finalResultText += `\n\nNew balance: ${escapeMarkdownV2(formatSol(finalBalance))} SOL`;
                await client.query('COMMIT');
                client.release(); client = null;

                const finalKeyboard = { inline_keyboard: [[
                    { text: `🚀 Play Crash Again`, callback_data: `play_again:${gameKey}:${betAmountLamports}` },
                    { text: '🎮 Games Menu', callback_data: 'menu:main' }
                ]]};
                if (currentMessageId) {
                    await bot.editMessageText(finalResultText, { chat_id: chatId, message_id: currentMessageId, parse_mode: 'MarkdownV2', reply_markup: finalKeyboard }).catch(e => console.error(`${logPrefix} Final crash edit error: ${e.message}`));
                }
                return; // End loop
            }

            // Increment multiplier
            const oldMultiplier = gameState.data.currentMultiplier;
            let increment = 0.01;
            if (oldMultiplier >= 20) increment = 0.50;
            else if (oldMultiplier >= 10) increment = 0.25;
            else if (oldMultiplier >= 5) increment = 0.10;
            else if (oldMultiplier >= 3) increment = 0.05;
            else if (oldMultiplier >= 1.5) increment = 0.02;
            
            gameState.data.currentMultiplier = parseFloat((oldMultiplier + increment).toFixed(2));
            userStateCache.set(`${userId}_${gameInstanceId}`, gameState); // Persist updated multiplier

            const displayMultiplier = gameState.data.currentMultiplier.toFixed(2);
            const animationText = `🚀 *Crash Game In Progress\\!* Bet: ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL\nMultiplier: *${displayMultiplier}x*`;
            const loopKeyboard = { inline_keyboard: [[{ text: `💸 Cash Out at ${displayMultiplier}x!`, callback_data: `crash_cash_out:${gameInstanceId}` }]] };

            if (currentMessageId) { // Check if currentMessageId is still valid
                await bot.editMessageText(animationText, {
                    chat_id: chatId, message_id: currentMessageId, parse_mode: 'MarkdownV2', reply_markup: loopKeyboard
                }).catch(e => {
                    console.warn(`${logPrefix} Crash animation edit error: ${e.message}. Loop may continue without UI update.`);
                    if(e.message.includes("message to edit not found") || e.message.includes("message is not modified")){
                        currentMessageId = null; // Stop trying to edit this message
                        if (gameLoopTimeoutId) clearTimeout(gameLoopTimeoutId); // Stop the game if UI is broken
                        userStateCache.delete(`${userId}_${gameInstanceId}`); // Clean state
                        // Optionally inform user via new message if game had to be aborted due to UI edit issue
                        safeSendMessage(chatId, "A display error occurred with the Crash game. Please start a new game.", {parse_mode: 'MarkdownV2'});
                    }
                });
            }
            if (!currentMessageId && gameLoopTimeoutId) { // If message became invalid, stop loop
                clearTimeout(gameLoopTimeoutId);
                userStateCache.delete(`${userId}_${gameInstanceId}`);
                return;
            }

            const delay = Math.max(150, 700 - Math.floor(oldMultiplier * 15)); // Faster updates at higher multipliers
            gameLoopTimeoutId = setTimeout(gameLoop, delay);
        };

        await sleep(1000); // Initial delay before multiplier starts visibly increasing
        if (currentMessageId) gameLoop(); // Start the game loop only if the message was successfully sent/edited

    } catch (error) {
        console.error(`${logPrefix} Error:`, error);
        if (client) { try { await client.query('ROLLBACK'); } catch (e) { console.error(`${logPrefix} Rollback error:`, e); } }
        if (gameLoopTimeoutId) clearTimeout(gameLoopTimeoutId);
        userStateCache.delete(`${userId}_${gameInstanceId}`);
        const userFacingError = "Sorry, an error occurred while starting the Crash game.";
         if (currentMessageId) {
            bot.editMessageText(userFacingError, {chat_id: chatId, message_id: currentMessageId, reply_markup: {inline_keyboard: [[{text: '↩️ Back to Games', callback_data: 'menu:main'}]]}, parse_mode: 'MarkdownV2'})
               .catch(e => safeSendMessage(chatId, userFacingError, {parse_mode: 'MarkdownV2'}));
        } else {
            safeSendMessage(chatId, userFacingError, {parse_mode: 'MarkdownV2'});
        }
    } finally {
        if (client) client.release();
    }
}

async function handleBlackjackGame(userId, chatId, originalMessageId, betAmountLamports, action, existingState = null) {
    const gameKey = 'blackjack';
    const logPrefix = `[BlackjackGame User ${userId} Action ${action}]`;
    console.log(`${logPrefix} Bet ${formatSol(betAmountLamports)}`);
    let client = null;
    const gameInstanceId = existingState?.data.gameInstanceId || `bj_${userId}_${Date.now()}`;
    let currentMessageId = existingState?.messageId || originalMessageId;
    let playerHand, dealerHand, deck, betId, currentBetAmount;

    try {
        if (action === 'initial_deal') {
            currentBetAmount = betAmountLamports;
            const betPlacement = await placeBet(userId, chatId, gameKey, {}, currentBetAmount);
            if (!betPlacement.success) {
                let replyText = betPlacement.error || "Failed to place your Blackjack bet.";
                if (betPlacement.insufficientBalance) {
                    const currentBal = await getUserBalance(userId);
                    replyText = `⚠️ Insufficient balance for ${escapeMarkdownV2(formatSol(currentBetAmount))} SOL Blackjack bet. Your balance: ${escapeMarkdownV2(formatSol(currentBal))} SOL.`;
                     return bot.editMessageText(replyText, {
                        chat_id: chatId, message_id: originalMessageId, parse_mode: 'MarkdownV2',
                        reply_markup: { inline_keyboard: [[{ text: '💰 Quick Deposit', callback_data: 'quick_deposit' }],[{ text: '↩️ Back to Blackjack', callback_data: 'menu:blackjack' }]] }
                     }).catch(e => safeSendMessage(chatId, replyText, {parse_mode: 'MarkdownV2'}));
                }
                 return bot.editMessageText(replyText, { chat_id: chatId, message_id: originalMessageId, parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: [[{text: '↩️ Back to Blackjack', callback_data: 'menu:blackjack'}]]} }).catch(e => safeSendMessage(chatId, replyText, {parse_mode: 'MarkdownV2'}));
            }
            betId = betPlacement.betId;
            deck = createDeck(); shuffleDeck(deck); // from Part 4
            playerHand = [dealCard(deck), dealCard(deck)];
            dealerHand = [dealCard(deck), dealCard(deck)];

            const initialDealText = `🃏 Dealing Blackjack for your ${escapeMarkdownV2(formatSol(currentBetAmount))} SOL bet...`;
            if(currentMessageId) {
                await bot.editMessageText(initialDealText, {chat_id: chatId, message_id: currentMessageId, parse_mode: 'MarkdownV2'}).catch(e => currentMessageId = null);
            }
            if(!currentMessageId) {
                const newMsg = await safeSendMessage(chatId, initialDealText, {parse_mode: 'MarkdownV2'});
                currentMessageId = newMsg?.message_id;
            }
             if (!currentMessageId) throw new Error("Failed to send or edit initial Blackjack message.");


        } else if (existingState && (action === 'hit' || action === 'stand')) {
            betId = existingState.data.betId;
            playerHand = existingState.data.playerHand;
            dealerHand = existingState.data.dealerHand;
            deck = existingState.data.deck;
            currentBetAmount = existingState.data.betAmountLamports; // Should be BigInt from state
        } else {
            throw new Error(`Invalid Blackjack action '${action}' or missing state.`);
        }

        let playerValue = calculateHandValue(playerHand); // from Part 4
        let dealerValue = calculateHandValue(dealerHand); // Initial dealer value, second card hidden
        let playerHasBlackjack = playerValue === 21 && playerHand.length === 2;
        let dealerHasBlackjack = dealerValue === 21 && dealerHand.length === 2; // Check full dealer hand for their blackjack
        let gameEnded = false;
        let resultData = null;
        let breadcrumb = existingState?.data.breadcrumb || GAME_CONFIG.blackjack.name;

        if (action === 'initial_deal') {
            if (playerHasBlackjack || dealerHasBlackjack) { // If anyone has Blackjack on deal
                gameEnded = true; // Dealer reveals if game ends now
                resultData = determineBlackjackWinner(playerHand, dealerHand); // from Part 4
            }
        } else if (action === 'hit') {
            breadcrumb += ` > Hit`;
            const newCard = dealCard(deck);
            if (newCard) playerHand.push(newCard); else throw new Error("Deck empty during player hit - should not happen.");
            playerValue = calculateHandValue(playerHand);
            if (playerValue >= 21) { // Player busts or hits 21
                gameEnded = true;
                const dealerFinalState = simulateDealerPlay([...dealerHand], deck); // Dealer plays out their hand
                resultData = determineBlackjackWinner(playerHand, dealerFinalState.hand);
            }
        } else if (action === 'stand') {
            breadcrumb += ` > Stand`;
            gameEnded = true;
            const dealerFinalState = simulateDealerPlay([...dealerHand], deck);
            resultData = determineBlackjackWinner(playerHand, dealerFinalState.hand);
        }

        let messageText = `${escapeMarkdownV2(breadcrumb)}\n\n`;
        messageText += `Your Hand: *${playerHand.map(c => escapeMarkdownV2(formatCard(c))).join(' ')}* \\(Value: ${playerValue}${playerValue > 21 ? ' BUST\\!' : (playerHasBlackjack && playerHand.length === 2 ? ' BLACKJACK\\!' : (playerValue === 21 ? ' \\- 21\\!' : ''))}\\)\n`;
        const dealerUpCardFormatted = escapeMarkdownV2(formatCard(dealerHand[0]));
        if (gameEnded && resultData) {
            messageText += `Dealer's Hand: *${resultData.dealerHandFormatted.map(c => escapeMarkdownV2(c)).join(' ')}* \\(Value: ${resultData.dealerValue}${resultData.dealerValue > 21 ? ' BUST\\!' : (dealerHasBlackjack && dealerHand.length === 2 ? ' BLACKJACK\\!' : (resultData.dealerValue === 21 ? ' \\- 21\\!' : ''))}\\)\n\n`;
        } else {
            messageText += `Dealer Shows: *${dealerUpCardFormatted}* \\[ ? \\]\n\n`;
        }

        const inlineKeyboard = [];
        if (gameEnded && resultData) {
            userStateCache.delete(`${userId}_${gameInstanceId}`);
            client = await pool.connect();
            await client.query('BEGIN');

            let payoutAmountToCredit = 0n;
            let profitLamportsOutcome = -currentBetAmount;
            let dbBetStatus = 'completed_loss';
            const houseEdgeBJ = GAME_CONFIG.blackjack.houseEdge;

            switch (resultData.result) {
                case 'player_blackjack': // Player Blackjack, dealer does not have Blackjack
                    payoutAmountToCredit = currentBetAmount + BigInt(Math.floor(Number(currentBetAmount) * 1.5 * (1 - houseEdgeBJ))); // 3:2 payout on winnings
                    profitLamportsOutcome = payoutAmountToCredit - currentBetAmount;
                    dbBetStatus = 'completed_win';
                    messageText += `🎉 *BLACKJACK\\!* You win\\! Returned ${escapeMarkdownV2(formatSol(payoutAmountToCredit))} SOL\\.`;
                    break;
                case 'player_wins':
                case 'dealer_busts_player_wins':
                    payoutAmountToCredit = currentBetAmount + BigInt(Math.floor(Number(currentBetAmount) * (1 - houseEdgeBJ))); // 1:1 payout on winnings
                    profitLamportsOutcome = payoutAmountToCredit - currentBetAmount;
                    dbBetStatus = 'completed_win';
                    messageText += `🎉 You *WIN*\\! Returned ${escapeMarkdownV2(formatSol(payoutAmountToCredit))} SOL\\.`;
                    break;
                case 'push':
                    payoutAmountToCredit = currentBetAmount; // Return stake
                    profitLamportsOutcome = 0n;
                    dbBetStatus = 'completed_push';
                    messageText += `🔵 It's a *PUSH*\\! Stake returned\\.`;
                    break;
                case 'dealer_wins': // Includes dealer blackjack if player didn't have one
                    messageText += `😥 Dealer *WINS*\\. You lost ${escapeMarkdownV2(formatSol(currentBetAmount))} SOL\\.`;
                    break;
                case 'player_busts':
                    messageText += `💥 You *BUSTED* with ${playerValue}\\. You lost ${escapeMarkdownV2(formatSol(currentBetAmount))} SOL\\.`;
                    break;
            }

            let finalBalanceUpdateResult;
            if (dbBetStatus === 'completed_win' || dbBetStatus === 'completed_push') {
                finalBalanceUpdateResult = await updateUserBalanceAndLedger(client, userId, payoutAmountToCredit, `bet_${resultData.result.replace(/_/g,'')}:${gameKey}`, { betId });
            } else { // Loss
                 const betPlacementInfo = await getBetDetails(betId); // get original state after deduction
                 finalBalanceUpdateResult = { success: true, newBalance: (await getUserBalance(userId)) }; // Use fresh balance as placeBet already deducted.
                                                                                                         // Or, more accurately, use betPlacement.newBalance from initial call.
                                                                                                         // This part is tricky if multiple bets happened.
                                                                                                         // Let's assume getUserBalance() reflects the state *after* the initial deduction.
            }
            await updateBetStatus(client, betId, dbBetStatus, payoutAmountToCredit);
            await client.query('COMMIT');
            client.release(); client = null;

            messageText += `\n\nNew balance: ${escapeMarkdownV2(formatSol(finalBalanceUpdateResult.newBalance))} SOL`;
            inlineKeyboard.push(
                [{ text: `🃏 Play Blackjack Again`, callback_data: `play_again:${gameKey}:${currentBetAmount}` }],
                [{ text: '🎮 Games Menu', callback_data: 'menu:main' }]
            );
        } else { // Game continues
            messageText += `Your action?`;
            inlineKeyboard.push(
                [{ text: '➕ Hit', callback_data: `blackjack_action:${gameInstanceId}:hit` }],
                [{ text: '✋ Stand', callback_data: `blackjack_action:${gameInstanceId}:stand` }]
            );
            userStateCache.set(`${userId}_${gameInstanceId}`, {
                state: 'awaiting_blackjack_action', chatId: chatId, messageId: currentMessageId,
                data: { gameKey, betId, betAmountLamports: currentBetAmount, playerHand, dealerHand, deck, gameInstanceId, breadcrumb },
                timestamp: Date.now()
            });
        }

        if (currentMessageId) {
            await bot.editMessageText(messageText, { chat_id: chatId, message_id: currentMessageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: inlineKeyboard } });
        }

    } catch (error) {
        console.error(`${logPrefix} Error:`, error);
        if (client) { try { await client.query('ROLLBACK'); } catch (e) { console.error(`${logPrefix} Rollback error:`, e); } }
        userStateCache.delete(`${userId}_${gameInstanceId}`);
        const userFacingError = "Sorry, an error occurred while playing Blackjack.";
        if (currentMessageId) {
            bot.editMessageText(userFacingError, {chat_id: chatId, message_id: currentMessageId, reply_markup: {inline_keyboard: [[{text: '↩️ Back to Games', callback_data: 'menu:main'}]]}, parse_mode: 'MarkdownV2'})
               .catch(e => safeSendMessage(chatId, userFacingError, {parse_mode: 'MarkdownV2'}));
        } else {
            safeSendMessage(chatId, userFacingError, {parse_mode: 'MarkdownV2'});
        }
    } finally {
        if (client) client.release();
    }
}

// --- End of Part 5a ---
// index.js - Part 5b: Input Handlers, Command Handlers, Referral Checks
// --- VERSION: 3.2.1 --- (Adjusted based on discussions for correctness)

// --- Assuming functions from Part 1, 2, 3, 4, 5a are available ---
// (GAME_CONFIG, STANDARD_BET_AMOUNTS_LAMPORTS, BOT_VERSION, userStateCache, etc.)
// (safeSendMessage, escapeMarkdownV2, formatSol, getUserBalance, placeBet, notifyAdmin, getLinkedWallet, linkUserWallet, etc.)
// (Constants like LAMPORTS_PER_SOL, MIN_WITHDRAWAL_LAMPORTS, WITHDRAWAL_FEE_LAMPORTS from Part 1)
// (DB functions like getUserWalletDetails, getTotalReferralEarnings, getNextDepositAddressIndex, createDepositAddressRecord, getBetHistory from Part 2)
// (Utilities like generateUniqueDepositAddress, addPayoutJob (from Part 6 but used by _handleReferralChecks))
// (proceedToGameStep from Part 5a)

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

    // Ensure state names match where they are set.
    // States for custom amount are typically `awaiting_${gameKey}_amount`
    switch (currentState.state) {
        case 'awaiting_coinflip_amount':
        case 'awaiting_race_amount':
        case 'awaiting_slots_amount':
        case 'awaiting_war_amount':
        case 'awaiting_roulette_amount':
        case 'awaiting_crash_amount':
        case 'awaiting_blackjack_amount':
            return handleCustomAmountInput(msg, currentState);

        case 'awaiting_roulette_number': // For Roulette straight bet number input
            return handleRouletteNumberInput(msg, currentState);

        case 'awaiting_withdrawal_amount':
            return handleWithdrawalAmountInput(msg, currentState);

        case 'awaiting_withdrawal_address': // This state was marked as deprecated in original notes
            console.warn(`${logPrefix} Received input for deprecated state: awaiting_withdrawal_address. Clearing state.`);
            userStateCache.delete(userId); // userStateCache from Part 1
            return safeSendMessage(chatId, "This input step is no longer used. Please use `/wallet <address>` to set your address, then `/withdraw`.", { parse_mode: 'MarkdownV2' }); // safeSendMessage from Part 3, escapeMarkdownV2 from Part 1

        default:
            console.log(`${logPrefix} User ${userId} in unhandled input state '${currentState.state}'. Received text: ${msg.text?.slice(0, 50)}`);
            userStateCache.delete(userId); // Clear confusing state
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
    // originalMessageId is the ID of the message containing the "Custom Amount" button, to be edited.
    const { gameKey, originalMessageId: msgIdToEdit, breadcrumb: currentBreadcrumb } = currentState.data;
    const logPrefix = `[CustomAmtInput User ${userId} Game ${gameKey}]`;

    console.log(`${logPrefix} Received custom amount: "${text}"`);

    if (!text) {
        // Re-prompt without deleting their invalid input, or delete and re-prompt.
        // Deleting user's message can be cleaner.
        bot.deleteMessage(chatId, msg.message_id).catch(()=>{}); // bot from Part 1
        return safeSendMessage(chatId, `⚠️ Please enter a valid bet amount for ${escapeMarkdownV2(GAME_CONFIG[gameKey]?.name || gameKey)}. Or /cancel.`, { parse_mode: 'MarkdownV2' });
    }
    if (!gameKey || !GAME_CONFIG[gameKey]) { // GAME_CONFIG from Part 1
        console.error(`${logPrefix} Invalid gameKey in state: ${gameKey}`);
        userStateCache.delete(userId);
        bot.deleteMessage(chatId, msg.message_id).catch(()=>{});
        return safeSendMessage(chatId, "⚠️ Internal error: Game information missing. Please start over or /cancel.", { parse_mode: 'MarkdownV2' });
    }

    const gameConfig = GAME_CONFIG[gameKey];
    const newBreadcrumb = `${currentBreadcrumb || gameConfig.name} > Bet ${text} SOL`; // Tentative breadcrumb

    let amountSOLInput;
    try {
        amountSOLInput = parseFloat(text);
        if (isNaN(amountSOLInput) || amountSOLInput <= 0) {
            bot.deleteMessage(chatId, msg.message_id).catch(()=>{});
            return safeSendMessage(chatId, `${escapeMarkdownV2(currentBreadcrumb || gameConfig.name)}\n⚠️ Invalid amount. Please enter a positive number (e.g., 0.1) for your ${escapeMarkdownV2(gameConfig.name)} bet, or /cancel.`, { parse_mode: 'MarkdownV2' });
        }
    } catch (e) {
        bot.deleteMessage(chatId, msg.message_id).catch(()=>{});
        return safeSendMessage(chatId, `${escapeMarkdownV2(currentBreadcrumb || gameConfig.name)}\n⚠️ Could not read amount. Please enter a number (e.g., 0.1) for your ${escapeMarkdownV2(gameConfig.name)} bet, or /cancel.`, { parse_mode: 'MarkdownV2' });
    }

    const amountLamports = BigInt(Math.round(amountSOLInput * LAMPORTS_PER_SOL)); // LAMPORTS_PER_SOL from Part 1
    const amountSOLValidated = formatSol(amountLamports); // formatSol from Part 3

    if (amountLamports < gameConfig.minBetLamports || amountLamports > gameConfig.maxBetLamports) {
        bot.deleteMessage(chatId, msg.message_id).catch(()=>{});
        const min = formatSol(gameConfig.minBetLamports);
        const max = formatSol(gameConfig.maxBetLamports);
        return safeSendMessage(chatId, `${escapeMarkdownV2(currentBreadcrumb || gameConfig.name)}\n⚠️ Bet amount for ${escapeMarkdownV2(gameConfig.name)} must be between ${escapeMarkdownV2(min)} and ${escapeMarkdownV2(max)} SOL. You entered: ${escapeMarkdownV2(amountSOLValidated)} SOL. Please try again, or /cancel.`, { parse_mode: 'MarkdownV2' });
    }

    const currentBalance = await getUserBalance(userId); // getUserBalance from Part 2
    if (currentBalance < amountLamports) {
        bot.deleteMessage(chatId, msg.message_id).catch(()=>{});
        userStateCache.delete(userId); // Clear state as they need to deposit
        return safeSendMessage(chatId, `${escapeMarkdownV2(currentBreadcrumb || gameConfig.name)}\n⚠️ Insufficient balance (${escapeMarkdownV2(formatSol(currentBalance))} SOL) for a ${escapeMarkdownV2(amountSOLValidated)} SOL bet. Use /deposit to add funds, or /cancel.`, { parse_mode: 'MarkdownV2' });
    }

    bot.deleteMessage(chatId, msg.message_id).catch(e => console.warn(`${logPrefix} Could not delete user amount message: ${e.message}`));
    userStateCache.delete(userId); // Clear the 'awaiting_X_amount' state

    // Valid custom amount, proceed to next game step (choice or confirmation)
    // This now directly calls the game-specific confirmation or next step logic.
    // For most games, this means showing confirmation buttons.
    // For Roulette (straight bet) or Race (horse selection), it's different.
    console.log(`${logPrefix} Custom amount ${amountSOLValidated} SOL validated for ${gameKey}. Proceeding to confirmation/next step.`);

    const finalBreadcrumb = `${currentBreadcrumb || gameConfig.name} > Bet ${amountSOLValidated} SOL`;

    if (gameKey === 'roulette') {
        // For Roulette, after custom amount, show bet type selection
        const stateForProceed = { state: 'awaiting_roulette_type_selection', chatId, messageId: msgIdToEdit, data: { gameKey, betAmountLamports, breadcrumb: finalBreadcrumb, step: 'select_bet_type', originalMessageId: msgIdToEdit }, timestamp: Date.now()};
        userStateCache.set(userId, stateForProceed);
        // Show bet type selection buttons for Roulette
        const rouletteBetTypes = [
            { text: "🔴 Red / ⚫️ Black", callback_data: `roulette_bet_type_category:color:${amountLamports}` }, // Will lead to red/black choice
            { text: "🔢 Even / Odd", callback_data: `roulette_bet_type_category:parity:${amountLamports}` },
            { text: "📉 Low (1-18) / 📈 High (19-36)", callback_data: `roulette_bet_type_category:range:${amountLamports}` },
            { text: "🎯 Straight Up (Number)", callback_data: `roulette_bet_type:straight:${amountLamports}` } // Leads to number input
        ];
        const keyboard = rouletteBetTypes.map(btn => [btn]);
        keyboard.push([{ text: '↩️ Change Amount', callback_data: `menu:${gameKey}` }, { text: '❌ Cancel', callback_data: 'menu:main' }]);

        return bot.editMessageText(`${escapeMarkdownV2(finalBreadcrumb)}\nSelected amount: ${escapeMarkdownV2(amountSOLValidated)} SOL. Now choose your bet type for Roulette:`, {
            chat_id: chatId, message_id: msgIdToEdit, reply_markup: {inline_keyboard: keyboard}, parse_mode: 'MarkdownV2'
        }).catch(e => console.error(`${logPrefix} Error editing for Roulette type selection: ${e.message}`));

    } else if (gameKey === 'race') {
        // For Race, after custom amount, select horse
        const stateForProceed = { state: `awaiting_${gameKey}_horse_selection`, chatId, messageId: msgIdToEdit, data: { gameKey, betAmountLamports, breadcrumb: finalBreadcrumb, step: 'select_horse', originalMessageId: msgIdToEdit }, timestamp: Date.now()};
        userStateCache.set(userId, stateForProceed);
        return proceedToGameStep(userId, chatId, msgIdToEdit, stateForProceed, ''); // Call proceedToGameStep to show horse buttons

    } else {
        // For other games (Coinflip, Slots, War, Crash, Blackjack), custom amount leads to direct confirmation
        const confirmationText = `${escapeMarkdownV2(finalBreadcrumb)}\nConfirm bet of ${escapeMarkdownV2(amountSOLValidated)} SOL on ${escapeMarkdownV2(gameConfig.name)}?`;
        let confirmCallbackData = `confirm_bet:${gameKey}:${amountLamports}`;
        if (gameKey === 'coinflip') { // Coinflip needs a choice (heads/tails)
            // After custom amount, ask for heads or tails for coinflip
            const cfBreadcrumb = `${finalBreadcrumb} > Choose Side`;
            const cfKeyboard = [
                [{ text: '👤 Heads', callback_data: `confirm_bet:coinflip:${amountLamports}:heads` }, { text: '🪙 Tails', callback_data: `confirm_bet:coinflip:${amountLamports}:tails` }],
                [{ text: '↩️ Change Amount', callback_data: `menu:${gameKey}` }, { text: '❌ Cancel', callback_data: 'menu:main' }]
            ];
            return bot.editMessageText(`${escapeMarkdownV2(cfBreadcrumb)}\nSelected amount: ${escapeMarkdownV2(amountSOLValidated)} SOL. Choose Heads or Tails:`, {
                chat_id: chatId, message_id: msgIdToEdit, reply_markup: {inline_keyboard: cfKeyboard}, parse_mode: 'MarkdownV2'
            }).catch(e => console.error(`${logPrefix} Error editing for Coinflip choice: ${e.message}`));
        }
        // Default confirmation for other games
        const confirmKeyboard = [
            [{ text: `✅ Yes, Bet ${escapeMarkdownV2(amountSOLValidated)} SOL`, callback_data: confirmCallbackData }],
            [{ text: '↩️ Change Amount', callback_data: `menu:${gameKey}` }, { text: '❌ Cancel', callback_data: 'menu:main' }]
        ];
        return bot.editMessageText(confirmationText, {
            chat_id: chatId, message_id: msgIdToEdit, reply_markup: {inline_keyboard: confirmKeyboard}, parse_mode: 'MarkdownV2'
        }).catch(e => console.error(`${logPrefix} Error editing for generic confirmation: ${e.message}`));
    }
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
    const { gameKey, betAmountLamports: betAmountLamportsStr, betType, originalMessageId, breadcrumb: currentBreadcrumb } = currentState.data;
    const betAmountLamports = BigInt(betAmountLamportsStr); // Convert string from state back to BigInt
    const logPrefix = `[RouletteNumInput User ${userId}]`;

    console.log(`${logPrefix} Received number: "${text}" for straight bet.`);

    if (!text) {
        bot.deleteMessage(chatId, msg.message_id).catch(()=>{});
        return safeSendMessage(chatId, `${escapeMarkdownV2(currentBreadcrumb)}\n⚠️ Please enter the number (0-36) you want to bet on, or /cancel.`, { parse_mode: 'MarkdownV2' });
    }
    if (gameKey !== 'roulette' || betType !== 'straight') {
        userStateCache.delete(userId);
        bot.deleteMessage(chatId, msg.message_id).catch(()=>{});
        return safeSendMessage(chatId, "⚠️ Internal error: Incorrect game state for number input. Please start Roulette again, or /cancel.", { parse_mode: 'MarkdownV2' });
    }

    let betValueNumber;
    try {
        betValueNumber = parseInt(text, 10);
        if (isNaN(betValueNumber) || betValueNumber < 0 || betValueNumber > 36) {
            bot.deleteMessage(chatId, msg.message_id).catch(()=>{});
            return safeSendMessage(chatId, `${escapeMarkdownV2(currentBreadcrumb)}\n⚠️ Invalid number. Please enter a number between 0 and 36, or /cancel.`, { parse_mode: 'MarkdownV2' });
        }
    } catch (e) {
        bot.deleteMessage(chatId, msg.message_id).catch(()=>{});
        return safeSendMessage(chatId, `${escapeMarkdownV2(currentBreadcrumb)}\n⚠️ Could not read number. Please enter a number between 0 and 36, or /cancel.`, { parse_mode: 'MarkdownV2' });
    }
    const betValueString = String(betValueNumber);

    bot.deleteMessage(chatId, msg.message_id).catch(e => console.warn(`${logPrefix} Could not delete user number message: ${e.message}`));
    userStateCache.delete(userId);

    const finalBreadcrumb = `${currentBreadcrumb} on Number ${betValueString}`;
    const confirmationText = `${escapeMarkdownV2(finalBreadcrumb)}\nBet ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL on number ${escapeMarkdownV2(betValueString)}?`;
    const inlineKeyboard = [
        [{ text: `✅ Yes, Confirm Bet`, callback_data: `confirm_bet:roulette:${betAmountLamportsStr}:${betType}:${betValueString}` }], // Pass amount as string
        [{ text: '↩️ Change Number', callback_data: `roulette_bet_type:straight:${betAmountLamportsStr}` }], // Re-trigger straight number input
        [{ text: '❌ Cancel & Exit', callback_data: 'menu:main' }]
    ];
    return bot.editMessageText(confirmationText, {
        chat_id: chatId, message_id: originalMessageId, // Edit the message that asked for number
        reply_markup: { inline_keyboard: inlineKeyboard }, parse_mode: 'MarkdownV2'
    }).catch(e => {
        console.error(`${logPrefix} Failed to edit message for Roulette straight confirmation: ${e.message}`);
        safeSendMessage(chatId, confirmationText, { reply_markup: { inline_keyboard: inlineKeyboard }, parse_mode: 'MarkdownV2' });
    });
}

/** Handles withdrawal AMOUNT input */
async function handleWithdrawalAmountInput(msg, currentState) {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const text = msg.text?.trim();
    const { originalMessageId: msgIdToEdit, breadcrumb: currentBreadcrumb } = currentState.data;
    const logPrefix = `[WithdrawAmtInput User ${userId}]`;

    if (!text) {
        bot.deleteMessage(chatId, msg.message_id).catch(()=>{});
        return safeSendMessage(chatId, `${escapeMarkdownV2(currentBreadcrumb)}\n⚠️ Please enter the amount of SOL you wish to withdraw, or /cancel.`, { parse_mode: 'MarkdownV2' });
    }
    console.log(`${logPrefix} Received withdrawal amount: "${text}"`);

    let amountSOL;
    try {
        amountSOL = parseFloat(text);
        if (isNaN(amountSOL) || amountSOL <= 0) {
            bot.deleteMessage(chatId, msg.message_id).catch(()=>{});
            return safeSendMessage(chatId, `${escapeMarkdownV2(currentBreadcrumb)}\n⚠️ Invalid amount. Please enter a positive number (e.g., 0.5), or /cancel.`, { parse_mode: 'MarkdownV2' });
        }
    } catch (e) {
        bot.deleteMessage(chatId, msg.message_id).catch(()=>{});
        return safeSendMessage(chatId, `${escapeMarkdownV2(currentBreadcrumb)}\n⚠️ Could not read amount. Please enter a number (e.g., 0.5), or /cancel.`, { parse_mode: 'MarkdownV2' });
    }

    const amountLamports = BigInt(Math.round(amountSOL * LAMPORTS_PER_SOL));
    const feeLamports = WITHDRAWAL_FEE_LAMPORTS; // From Part 1
    const totalDeduction = amountLamports + feeLamports;
    const amountSOLFormatted = formatSol(amountLamports);
    const feeSOLFormatted = formatSol(feeLamports);
    const totalSOLFormatted = formatSol(totalDeduction);

    if (amountLamports < MIN_WITHDRAWAL_LAMPORTS) { // MIN_WITHDRAWAL_LAMPORTS from Part 1
        bot.deleteMessage(chatId, msg.message_id).catch(()=>{});
        return safeSendMessage(chatId, `${escapeMarkdownV2(currentBreadcrumb)}\n⚠️ Minimum withdrawal is ${escapeMarkdownV2(formatSol(MIN_WITHDRAWAL_LAMPORTS))} SOL. You entered: ${escapeMarkdownV2(amountSOLFormatted)} SOL. Please try again, or /cancel.`, { parse_mode: 'MarkdownV2' });
    }

    const currentBalance = await getUserBalance(userId);
    if (currentBalance < totalDeduction) {
        bot.deleteMessage(chatId, msg.message_id).catch(()=>{});
        userStateCache.delete(userId);
        return safeSendMessage(chatId, `${escapeMarkdownV2(currentBreadcrumb)}\n⚠️ Insufficient balance. You need ${escapeMarkdownV2(totalSOLFormatted)} SOL (amount + fee) to withdraw ${escapeMarkdownV2(amountSOLFormatted)} SOL. Your balance: ${escapeMarkdownV2(formatSol(currentBalance))} SOL. Or /cancel.`, { parse_mode: 'MarkdownV2' });
    }

    const linkedAddress = await getLinkedWallet(userId); // getLinkedWallet from Part 2
    if (!linkedAddress) {
        bot.deleteMessage(chatId, msg.message_id).catch(()=>{});
        userStateCache.delete(userId);
        return safeSendMessage(chatId, `⚠️ Withdrawal address not found. Please set it using \`/wallet <address>\` first, then \`/withdraw\`. Or /cancel.`, { parse_mode: 'MarkdownV2' });
    }

    bot.deleteMessage(chatId, msg.message_id).catch(e => console.warn(`${logPrefix} Could not delete user amount message: ${e.message}`));
    
    const newBreadcrumb = `${currentBreadcrumb || 'Withdraw SOL'} > Confirm`;
    userStateCache.set(userId, {
        state: 'awaiting_withdrawal_confirm', // This state will be checked by a callback handler for 'withdraw_confirm'
        chatId: chatId,
        messageId: msgIdToEdit, // The message that asked for the amount
        data: { amountLamports: amountLamports.toString(), feeLamports: feeLamports.toString(), recipientAddress: linkedAddress, breadcrumb: newBreadcrumb },
        timestamp: Date.now()
    });

    const confirmationText = `*Confirm Withdrawal*\n\n` +
        `${escapeMarkdownV2(newBreadcrumb)}\n` +
        `Amount: *${escapeMarkdownV2(amountSOLFormatted)} SOL*\n` +
        `Fee: ${escapeMarkdownV2(feeSOLFormatted)} SOL\n` +
        `Total Deducted: *${escapeMarkdownV2(totalSOLFormatted)} SOL*\n` +
        `Recipient: \`${escapeMarkdownV2(linkedAddress)}\`\n\n` +
        `Proceed?`;
    const keyboard = [
        // The callback_data for 'withdraw_confirm' should ideally carry the necessary info or use the state.
        // Here, using state is fine since we set it above.
        [{ text: '✅ Confirm Withdrawal', callback_data: 'withdraw_confirm' }],
        [{ text: '↩️ Change Amount', callback_data: 'menu:withdraw' }], // Go back to withdraw command start
        [{ text: '❌ Cancel', callback_data: 'menu:main' }]
    ];

    return bot.editMessageText(confirmationText, {
        chat_id: chatId, message_id: msgIdToEdit,
        reply_markup: { inline_keyboard: keyboard }, parse_mode: 'MarkdownV2'
    }).catch(e => {
        console.error(`${logPrefix} Error editing message for withdrawal confirmation: ${e.message}`);
        safeSendMessage(chatId, confirmationText, { reply_markup: { inline_keyboard: keyboard }, parse_mode: 'MarkdownV2' });
    });
}

// --- Command Handlers ---

/** Handles /start command - Shows main menu */
async function handleStartCommand(msgOrCbMsg, args) { // Can be triggered by msg or callback's message
    const chatId = msgOrCbMsg.chat.id;
    const userId = String(msgOrCbMsg.from.id);
    const firstName = msgOrCbMsg.from.first_name || 'Gambler';
    const logPrefix = `[Cmd /start User ${userId}]`;
    console.log(`${logPrefix} Handling /start.`);

    let tempClient = null;
    try {
        tempClient = await pool.connect();
        await ensureUserExists(userId, tempClient); // Ensures user record exists
    } catch (dbError) {
        console.error(`${logPrefix} DB error in /start: ${dbError.message}`);
        safeSendMessage(chatId, "A database error occurred while starting. Please try again in a moment.");
        if (tempClient) tempClient.release();
        return;
    } finally {
        if (tempClient) tempClient.release();
    }
    
    userStateCache.set(userId, { state: 'idle', chatId: String(chatId), timestamp: Date.now(), data: { breadcrumb: "Main Menu"} });

    // Referral code handling from /start <ref_code> payload
    // args[0] is the command itself, like '/start'. args[1] would be the payload if present.
    const potentialRefCode = args?.[1];
    if (potentialRefCode && potentialRefCode.startsWith('ref_')) {
        console.log(`${logPrefix} Potential referral code found in /start: ${potentialRefCode}`);
        const referrer = await getUserByReferralCode(potentialRefCode); // from Part 2
        if (referrer && referrer.user_id !== userId) {
            const refereeDetails = await getUserWalletDetails(userId); // from Part 2
            if (!refereeDetails?.referred_by_user_id) {
                // Set pending referral. Actual linking happens on first deposit.
                pendingReferrals.set(userId, { referrerUserId: referrer.user_id, timestamp: Date.now() }); // pendingReferrals from Part 1
                const referrerDisplayName = await getUserDisplayName(chatId, referrer.user_id); // from Part 3
                await safeSendMessage(chatId, `👋 Welcome via ${escapeMarkdownV2(referrerDisplayName || 'a friend')}'s referral link! Your accounts will be linked upon your first deposit.`);
                console.log(`${logPrefix} User ${userId} tentatively referred by ${referrer.user_id} via /start link.`);
            } else if (refereeDetails.referred_by_user_id === referrer.user_id) {
                await safeSendMessage(chatId, `Welcome back! You were already referred by this user.`);
            } else {
                await safeSendMessage(chatId, `Welcome! You are already part of another referral.`);
            }
        } else if (referrer && referrer.user_id === userId) {
            await safeSendMessage(chatId, "You can't refer yourself! 😉");
        }
    }


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
                [
                    { text: '🚀 Crash', callback_data: 'menu:crash' },
                    { text: '♠️ Blackjack', callback_data: 'menu:blackjack' }
                ],
                [
                    { text: '💰 Deposit SOL', callback_data: 'menu:deposit' },
                    { text: '💸 Withdraw SOL', callback_data: 'menu:withdraw' }
                ],
                [
                    { text: '👤 Wallet / History', callback_data: 'menu:wallet' },
                    { text: '🏆 Leaderboards', callback_data: 'menu:leaderboards'},
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
        `Solana Gambles Bot *v${escapeMarkdownV2(BOT_VERSION)}* is ready\\! Choose an option below:\n\n` + // BOT_VERSION from Part 1
        `*Remember to gamble responsibly\\!*`;

    // If triggered by a button click (msgOrCbMsg is a Message object from callbackQuery.message)
    if (msgOrCbMsg.message_id && msgOrCbMsg.chat) {
         bot.editMessageText(welcomeMsg, {
            chat_id: chatId,
            message_id: msgOrCbMsg.message_id, // Use message_id from the callback's message
            ...options // Spread options including reply_markup and parse_mode
        }).catch(e => { // Fallback if edit fails
            console.warn(`${logPrefix} Failed to edit message for /start, sending new: ${e.message}`);
            safeSendMessage(chatId, welcomeMsg, options);
        });
    } else { // Triggered by a direct /start command (msgOrCbMsg is the incoming Message)
        safeSendMessage(chatId, welcomeMsg, options);
    }
}

/** Handles /help command */
async function handleHelpCommand(msg, args) {
    const chatId = msg.chat.id;
    const feeSOL = formatSol(WITHDRAWAL_FEE_LAMPORTS);
    const minWithdrawSOL = formatSol(MIN_WITHDRAWAL_LAMPORTS);
    const gameKeys = Object.keys(GAME_CONFIG);
    const gameCommands = gameKeys.map(key => `\\- \`/${escapeMarkdownV2(key)}\` \\- Play ${escapeMarkdownV2(GAME_CONFIG[key].name)}`).join('\n');

    const slotsPayouts = "🎰 *Slots Payouts (Example Multipliers):*\n" +
        "\\- 💎💎💎 \\= JACKPOT\\! (Progressive)\n" +
        "\\- ❼❼❼ \\= 100x\n" +
        "\\- 🔔🔔🔔 \\= 20x\n" +
        "\\- 🍊🍊🍊 \\= 10x\n" +
        "\\- 🍒🍒🍒 \\= 5x\n" +
        "\\- Any two 🍒 \\= 2x\n" +
        "_(Actual odds & detailed tables may vary; house edge applies\\.)_";

    const helpMsg = `*Solana Gambles Bot Help* 🤖 (v${escapeMarkdownV2(BOT_VERSION)})\n\n` +
        `This bot uses a custodial model\\. Deposit SOL to your internal balance to play\\. Winnings are added to this balance\\.\n\n` +
        `*Key Commands:*\n` +
        `\\- \`/start\` \\- Main menu & game selection\\.\n` +
        `\\- \`/wallet\` \\- View balance, set/view withdrawal address\\.\n` +
        `\\- \`/history\` \\- View your recent bet history\\.\n` +
        `\\- \`/deposit\` \\- Get a unique address to deposit SOL\\.\n` +
        `\\- \`/withdraw\` \\- Start the withdrawal process\\.\n` +
        `\\- \`/referral\` \\- Your referral link & stats\\.\n` +
        `\\- \`/leaderboards\` \\- View game leaderboards \\(e\\.g\\., \`/leaderboards slots\`\\)\\.\n`+
        `\\- \`/help\` \\- This message\\.\n\n` +
        `*Available Games:*\n${gameCommands}\n\n` +
        `${slotsPayouts}\n\n` +
        `*Important Notes:*\n` +
        `\\- Deposits: Use unique addresses from \`/deposit\`\\. They expire in ${escapeMarkdownV2(String(process.env.DEPOSIT_ADDRESS_EXPIRY_MINUTES))} mins\\. Confirmation: *${escapeMarkdownV2(DEPOSIT_CONFIRMATION_LEVEL)}*\\.\n` +
        `\\- Withdrawals: Set address via \`/wallet <YourSolAddress>\`\\. Fee: ${escapeMarkdownV2(feeSOL)} SOL\\. Min: ${escapeMarkdownV2(minWithdrawSOL)} SOL\\.\n` +
        `\\- House Edge: Each game has a built\\-in house edge\\. Details may be available per game or in \`/help\`\\.\n`+
        `\\- Gamble Responsibly\\. All bets are final\\. Contact support for issues\\.`;

    // If triggered by a button click (msg is a Message object from callbackQuery.message)
    if (msg.message_id && msg.chat) {
        bot.editMessageText(helpMsg, {
            chat_id: chatId, message_id: msg.message_id, ...{parse_mode: 'MarkdownV2', disable_web_page_preview: true, reply_markup: {inline_keyboard: [[{text: "↩️ Back to Menu", callback_data: "menu:main"}]]} }
        }).catch(e => {
            console.warn(`[Cmd /help] Failed to edit message for /help, sending new: ${e.message}`);
            safeSendMessage(chatId, helpMsg, { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
        });
    } else { // Triggered by a direct /help command
        safeSendMessage(chatId, helpMsg, { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
    }
}

/** Handles /wallet command (viewing, setting address) */
async function handleWalletCommand(msgOrCbMsg, args) {
    const chatId = msgOrCbMsg.chat.id;
    const userId = String(msgOrCbMsg.from.id);
    const logPrefix = `[Cmd /wallet User ${userId}]`;
    // args[0] is /wallet. args[1] is potential address if not from callback.
    // If from callback, args might be empty or carry other params.
    const potentialNewAddress = (msgOrCbMsg.text && args.length > 1) ? args[1].trim() : null;

    let messageToEditId = msgOrCbMsg.message_id; // If from callback, this is the button message. If from command, it's the command message.

    if (potentialNewAddress) {
        console.log(`${logPrefix} Attempting to set withdrawal address to: ${potentialNewAddress}`);
        const result = await linkUserWallet(userId, potentialNewAddress); // from Part 2
        const replyMsg = result.success
            ? `✅ Withdrawal address successfully set/updated to: \`${escapeMarkdownV2(result.wallet)}\``
            : `❌ Failed to set withdrawal address. Error: ${escapeMarkdownV2(result.error || 'Unknown error')}`;
        
        if(msgOrCbMsg.text) { // From a /wallet <addr> command
            safeSendMessage(chatId, replyMsg, { parse_mode: 'MarkdownV2' });
        } else { // From a callback (e.g. if wallet menu had a "Set Address" button leading to text input then this)
             bot.editMessageText(replyMsg, {chat_id: chatId, message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: [[{text: "↩️ Back to Wallet", callback_data: "menu:wallet"}]]}}).catch(e => safeSendMessage(chatId, replyMsg, {parse_mode: 'MarkdownV2'}));
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
            walletMsg += `*Withdrawal Address:* Not Set \\(Use \`/wallet <YourSolAddress>\`\\)\n`;
        }
        if (userDetails?.referral_code) {
            walletMsg += `*Referral Code:* \`${escapeMarkdownV2(userDetails.referral_code)}\`\n`;
        }
        walletMsg += `*Referrals Made:* ${escapeMarkdownV2(String(userDetails?.referral_count || 0))}\n`;
        walletMsg += `*Total Wagered:* ${escapeMarkdownV2(formatSol(userDetails?.total_wagered || 0n))} SOL\n`;

        const lastBets = userDetails?.last_bet_amounts || {}; // From DB via getUserWalletDetails
        const lastBetsInMemory = userLastBetAmounts.get(userId) || new Map(); // userLastBetAmounts from Part 1
        const combinedLastBets = {...lastBets}; // DB first
        lastBetsInMemory.forEach((value, key) => { // Override with in-memory if more recent (though DB should be source of truth for display here)
            if (!combinedLastBets[key]) combinedLastBets[key] = value.toString(); // Add if missing, keep as string for consistency
        });


        if (Object.keys(combinedLastBets).length > 0) {
            walletMsg += `\n*Last Bet Amounts (Approx\\.):*\n`;
            for (const game in combinedLastBets) {
                if (GAME_CONFIG[game]) { // Only show for known games
                     walletMsg += `  \\- ${escapeMarkdownV2(GAME_CONFIG[game]?.name || game)}: ${escapeMarkdownV2(formatSol(BigInt(combinedLastBets[game])))} SOL\n`;
                }
            }
        }

        walletMsg += `\nTo set/update address: \`/wallet <YourSolAddress>\`\nView recent bets: \`/history\``;
        const walletKeyboard = [
            [{ text: '📜 Bet History', callback_data: 'menu:history' }],
            [{ text: '💰 Deposit SOL', callback_data: 'menu:deposit' }, { text: '💸 Withdraw SOL', callback_data: 'menu:withdraw' }],
            [{ text: '↩️ Back to Main Menu', callback_data: 'menu:main' }]
        ];

        if (msgOrCbMsg.message_id && msgOrCbMsg.data) { // If it's from a callback like 'menu:wallet'
            bot.editMessageText(walletMsg, {chat_id: chatId, message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: walletKeyboard}}).catch(e => safeSendMessage(chatId, walletMsg, {parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: walletKeyboard}}));
        } else { // From a direct /wallet command
            safeSendMessage(chatId, walletMsg, { parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: walletKeyboard} });
        }
    }
}

async function handleHistoryCommand(msgOrCbMsg, args) {
    const chatId = msgOrCbMsg.chat.id;
    const userId = String(msgOrCbMsg.from.id);
    const logPrefix = `[Cmd /history User ${userId}]`;
    console.log(`${logPrefix} Fetching bet history.`);
    let messageToEditId = msgOrCbMsg.message_id;

    const limit = 5; // Show last 5 bets
    const history = await getBetHistory(userId, limit, 0, null); // getBetHistory from Part 2

    if (!history || history.length === 0) {
        const noHistoryMsg = "You have no betting history yet. Time to play some games!";
        if (msgOrCbMsg.message_id && msgOrCbMsg.data) { // From callback
             return bot.editMessageText(noHistoryMsg, {chat_id: chatId, message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: [[{text: "🎮 Games Menu", callback_data: "menu:main"}]]}});
        }
        return safeSendMessage(chatId, noHistoryMsg, { parse_mode: 'MarkdownV2' });
    }

    let historyMsg = "📜 *Your Last 5 Bets:*\n\n";
    history.forEach(bet => {
        const gameName = GAME_CONFIG[bet.game_type]?.name || bet.game_type;
        const wager = formatSol(bet.wager_amount_lamports);
        let outcomeText = `Status: ${escapeMarkdownV2(bet.status)}`;
        if (bet.status.startsWith('completed_')) {
            const payout = bet.payout_amount_lamports !== null ? BigInt(bet.payout_amount_lamports) : 0n;
            const profit = payout - BigInt(bet.wager_amount_lamports);
            if (bet.status === 'completed_win') outcomeText = `Won ${escapeMarkdownV2(formatSol(profit))} SOL (Returned ${escapeMarkdownV2(formatSol(payout))})`;
            else if (bet.status === 'completed_push') outcomeText = `Push (Returned ${escapeMarkdownV2(formatSol(payout))} SOL)`;
            else if (bet.status === 'completed_loss') outcomeText = `Lost ${escapeMarkdownV2(wager)} SOL`;
        }
        const betDate = new Date(bet.created_at).toLocaleString('en-GB', { day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit', hour12: false });
        historyMsg += `\\- *${escapeMarkdownV2(gameName)}* on ${escapeMarkdownV2(betDate)}\n` +
                      `   Bet: ${escapeMarkdownV2(wager)} SOL, Result: ${outcomeText}\n\n`;
    });
    historyMsg += "_For full history, please use an external service if available or contact support for older records\\._";

    const historyKeyboard = [[{ text: '↩️ Back to Wallet', callback_data: 'menu:wallet' }, { text: '🎮 Games Menu', callback_data: 'menu:main' }]];

    if (msgOrCbMsg.message_id && msgOrCbMsg.data) { // From callback 'menu:history'
        bot.editMessageText(historyMsg, {chat_id: chatId, message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: historyKeyboard}}).catch(e => safeSendMessage(chatId, historyMsg, {parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: historyKeyboard}}));
    } else { // From /history command
        safeSendMessage(chatId, historyMsg, { parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: historyKeyboard} });
    }
}


/** Handles /referral command */
async function handleReferralCommand(msgOrCbMsg, args) {
    const chatId = msgOrCbMsg.chat.id;
    const userId = String(msgOrCbMsg.from.id);
    let messageToEditId = msgOrCbMsg.message_id;

    const userDetails = await getUserWalletDetails(userId); // from Part 2

    if (!userDetails?.external_withdrawal_address) {
        const noWalletMsg = `❌ You need to link your wallet first using \`/wallet <YourSolAddress>\` before using the referral system. This ensures rewards can be paid out.`;
         if (msgOrCbMsg.message_id && msgOrCbMsg.data) {
            return bot.editMessageText(noWalletMsg, {chat_id: chatId, message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: [[{text: "↩️ Back to Menu", callback_data: "menu:main"}]]}});
        }
        return safeSendMessage(chatId, noWalletMsg, { parse_mode: 'MarkdownV2' });
    }
    if (!userDetails.referral_code) {
        // This should be rare if linkUserWallet generates it.
        const genCodeMsg = `Your referral code is being generated. Please try \`/wallet\` again first (this may create your code), then \`/referral\`.`;
        if (msgOrCbMsg.message_id && msgOrCbMsg.data) {
            return bot.editMessageText(genCodeMsg, {chat_id: chatId, message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: [[{text: "↩️ Back to Menu", callback_data: "menu:main"}]]}});
        }
        return safeSendMessage(chatId, genCodeMsg, { parse_mode: 'MarkdownV2' });
    }

    const refCode = userDetails.referral_code;
    const totalEarningsLamports = await getTotalReferralEarnings(userId); // from Part 2
    const totalEarningsSOL = formatSol(totalEarningsLamports);

    let botUsername = 'YOUR_BOT_USERNAME';
    try { const me = await bot.getMe(); if (me.username) botUsername = me.username; } catch (e) { console.warn("Could not fetch bot username for referral link.");}
    
    const referralLink = `https://t.me/${botUsername}?start=${refCode}`;
    // For display in MarkdownV2, escape the username and code if they contain special chars (though unlikely for username)
    const referralLinkMarkdown = `https://t\\.me/${escapeMarkdownV2(botUsername)}?start=${escapeMarkdownV2(refCode)}`;

    let referralMsg = `🤝 *Your Referral Dashboard*\n\n` +
        `Share your unique link to earn SOL when your friends play\\!\n\n` +
        `*Your Code:* \`${escapeMarkdownV2(refCode)}\`\n` +
        `*Your Link:* \n\`${escapeMarkdownV2(referralLink)}\`\n` + // Plain for copy-paste
        `_(Tap the button below to easily share)_\n\n`+
        `*Successful Referrals:* ${escapeMarkdownV2(String(userDetails.referral_count || 0))}\n` +
        `*Total Referral Earnings Paid:* ${escapeMarkdownV2(totalEarningsSOL)} SOL\n\n` +
        `*How Rewards Work:*\n` +
        `1\\. *Initial Bonus:* Earn a % of your referral's *first qualifying bet* \\(min ${escapeMarkdownV2(formatSol(REFERRAL_INITIAL_BET_MIN_LAMPORTS))} SOL wager\\)\\. Your % increases with more referrals\\!\n` + // REFERRAL_INITIAL_BET_MIN_LAMPORTS from Part 1
        `    tiers: ${REFERRAL_INITIAL_BONUS_TIERS.map(t => `${t.maxCount === Infinity ? '100+' : '<=' + t.maxCount} refs = ${t.percent*100}%`).join(', ')}\n` + // REFERRAL_INITIAL_BONUS_TIERS from Part 1
        `2\\. *Milestone Bonus:* Earn ${escapeMarkdownV2(String(REFERRAL_MILESTONE_REWARD_PERCENT * 100))}% of their total wagered amount as they hit milestones \\(e\\.g\\., 1 SOL, 5 SOL wagered, etc\\.\\)\\.\n\n` + // REFERRAL_MILESTONE_REWARD_PERCENT from Part 1
        `Rewards are paid to your linked wallet: \`${escapeMarkdownV2(userDetails.external_withdrawal_address)}\``;

    const keyboard = [
        [{ text: '🔗 Share My Referral Link!', switch_inline_query: referralLink }], // switch_inline_query requires bot to have inline mode enabled
        [{ text: '↩️ Back to Main Menu', callback_data: 'menu:main' }]
    ];
    
    if (msgOrCbMsg.message_id && msgOrCbMsg.data) { // From callback
        bot.editMessageText(referralMsg, {chat_id: chatId, message_id: messageToEditId, parse_mode: 'MarkdownV2', disable_web_page_preview: true, reply_markup: {inline_keyboard: keyboard}}).catch(e => safeSendMessage(chatId, referralMsg, {parse_mode: 'MarkdownV2', disable_web_page_preview: true, reply_markup: {inline_keyboard: keyboard}}));
    } else { // From command
        safeSendMessage(chatId, referralMsg, { parse_mode: 'MarkdownV2', disable_web_page_preview: true, reply_markup: {inline_keyboard: keyboard} });
    }
}

/** Handles /deposit command */
async function handleDepositCommand(msgOrCbMsg, args) {
    const chatId = msgOrCbMsg.chat.id;
    const userId = String(msgOrCbMsg.from.id);
    const logPrefix = `[DepositCmd User ${userId}]`;
    let messageToEditId = msgOrCbMsg.message_id;

    try {
        // Ensure user exists, as getNextDepositAddressIndex relies on it
        let tempClient = null;
        try {
            tempClient = await pool.connect();
            await ensureUserExists(userId, tempClient);
        } finally {
            if (tempClient) tempClient.release();
        }

        const addressIndex = await getNextDepositAddressIndex(userId); // from Part 2
        const derivedInfo = await generateUniqueDepositAddress(userId, addressIndex); // from Part 3
        if (!derivedInfo) {
            throw new Error("Failed to generate deposit address. Master seed phrase might be an issue.");
        }

        const depositAddress = derivedInfo.publicKey.toBase58();
        const expiresAt = new Date(Date.now() + DEPOSIT_ADDRESS_EXPIRY_MS); // DEPOSIT_ADDRESS_EXPIRY_MS from Part 1
        const recordResult = await createDepositAddressRecord(userId, depositAddress, derivedInfo.derivationPath, expiresAt); // from Part 2
        if (!recordResult.success) {
            throw new Error(recordResult.error || "Failed to save deposit address record in DB.");
        }

        const expiryMinutes = Math.round(DEPOSIT_ADDRESS_EXPIRY_MS / (60 * 1000));
        const message = `💰 *Deposit SOL*\n\n` +
                        `Send SOL to this unique address:\n\n` +
                        `\`${escapeMarkdownV2(depositAddress)}\`  _(Tap to copy)_\n\n` +
                        `⚠️ *Important:*\n` +
                        `1\\. This address is for *one deposit only* & expires in *${escapeMarkdownV2(String(expiryMinutes))} minutes*\\.\n` +
                        `2\\. For new deposits, use \`/deposit\` again or the menu option\\.\n` +
                        `3\\. Confirmation: *${escapeMarkdownV2(DEPOSIT_CONFIRMATION_LEVEL)}* network confirmations required\\.`; // DEPOSIT_CONFIRMATION_LEVEL from Part 1
        
        const depositKeyboard = [[{ text: '↩️ Back to Main Menu', callback_data: 'menu:main' }]];

        if (msgOrCbMsg.message_id && msgOrCbMsg.data) { // From callback
            bot.editMessageText(message, {chat_id: chatId, message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: depositKeyboard}}).catch(e => safeSendMessage(chatId, message, {parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: depositKeyboard}}));
        } else { // From command
            safeSendMessage(chatId, message, { parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: depositKeyboard} });
        }

    } catch (error) {
        console.error(`${logPrefix} Error: ${error.message}`);
        const errorMsg = `❌ Error generating deposit address: ${escapeMarkdownV2(error.message)}. Please try again. If the issue persists, contact support.`;
        if (msgOrCbMsg.message_id && msgOrCbMsg.data) {
            bot.editMessageText(errorMsg, {chat_id: chatId, message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: [[{text: "↩️ Back to Menu", callback_data: "menu:main"}]]}}).catch(e => safeSendMessage(chatId, errorMsg, {parse_mode: 'MarkdownV2'}));
        } else {
            safeSendMessage(chatId, errorMsg, { parse_mode: 'MarkdownV2' });
        }
    }
}

/** Handles /withdraw command - Starts the withdrawal flow */
async function handleWithdrawCommand(msgOrCbMsg, args) {
    const chatId = msgOrCbMsg.chat.id;
    const userId = String(msgOrCbMsg.from.id);
    const logPrefix = `[WithdrawCmd User ${userId}]`;
    const breadcrumb = "Withdraw SOL";
    let messageToEditId = msgOrCbMsg.message_id;

    try {
        const linkedAddress = await getLinkedWallet(userId);
        if (!linkedAddress) {
            const noWalletMsg = `⚠️ You must set your withdrawal address first using \`/wallet <YourSolanaAddress>\`\\.`;
            if (msgOrCbMsg.message_id && msgOrCbMsg.data) {
                 return bot.editMessageText(noWalletMsg, {chat_id: chatId, message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: [[{text: "↩️ Back to Menu", callback_data: "menu:main"}]]}});
            }
            return safeSendMessage(chatId, noWalletMsg, { parse_mode: 'MarkdownV2' });
        }
        const currentBalance = await getUserBalance(userId);
        const minWithdrawTotal = MIN_WITHDRAWAL_LAMPORTS + WITHDRAWAL_FEE_LAMPORTS;
        if (currentBalance < minWithdrawTotal) {
             const lowBalMsg = `⚠️ Your balance (${escapeMarkdownV2(formatSol(currentBalance))} SOL) is below the minimum required to withdraw (${escapeMarkdownV2(formatSol(minWithdrawTotal))} SOL total including fee)\\.`;
             if (msgOrCbMsg.message_id && msgOrCbMsg.data) {
                return bot.editMessageText(lowBalMsg, {chat_id: chatId, message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: [[{text: "↩️ Back to Menu", callback_data: "menu:main"}]]}});
            }
            return safeSendMessage(chatId, lowBalMsg, { parse_mode: 'MarkdownV2' });
        }

        // Store the message ID that prompted for amount, so it can be edited later with confirmation.
        // If this handler was called by a command, msgOrCbMsg.message_id is the command message.
        // If called by a menu button, msgOrCbMsg.message_id is the menu message.
        // We want to edit *that* message after user types amount.
        let msgForAmountPrompt = msgOrCbMsg;
        if (msgOrCbMsg.message_id && msgOrCbMsg.data) { // From callback, use the message associated with callback
            msgForAmountPrompt = msgOrCbMsg;
        } else { // From command, the msg itself is what we might edit or reply to.
            // For text input, we usually send a new prompt message.
            // Let's send a new message that will then be edited.
            const sentPromptMsg = await safeSendMessage(chatId, "Preparing withdrawal...", {parse_mode: 'MarkdownV2'});
            if (!sentPromptMsg) {
                throw new Error("Failed to send withdrawal amount prompt message.");
            }
            messageToEditId = sentPromptMsg.message_id; // This is the ID we will edit later.
        }


        userStateCache.set(userId, {
            state: 'awaiting_withdrawal_amount',
            chatId: chatId,
            messageId: messageToEditId, // The message that will be edited to show confirmation
            data: { breadcrumb, originalMessageId: messageToEditId }, // originalMessageId for handleWithdrawalAmountInput
            timestamp: Date.now()
        });

        const minWithdrawSOLText = formatSol(MIN_WITHDRAWAL_LAMPORTS);
        const feeSOLText = formatSol(WITHDRAWAL_FEE_LAMPORTS);
        const promptText = `${escapeMarkdownV2(breadcrumb)}\n\n` +
            `Your withdrawal address: \`${escapeMarkdownV2(linkedAddress)}\`\n` +
            `Minimum withdrawal: ${escapeMarkdownV2(minWithdrawSOLText)} SOL\\. Fee: ${escapeMarkdownV2(feeSOLText)} SOL\\.\n\n` +
            `Please enter the amount of SOL you wish to withdraw (e\\.g\\., 0\\.5), or /cancel:`;
        
        // Edit the (potentially new) message to ask for amount
        await bot.editMessageText(promptText, {
            chat_id: chatId, message_id: messageToEditId, parse_mode: 'MarkdownV2',
            reply_markup: { inline_keyboard: [[{ text: '❌ Cancel Withdrawal', callback_data: 'menu:main' }]]}
        }).catch(async (e) => { // If edit fails, try sending as new message
             console.warn(`[WithdrawCmd] Failed to edit message ${messageToEditId} for amount prompt, sending new. Error: ${e.message}`);
             const newSentPromptMsg = await safeSendMessage(chatId, promptText, { parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: '❌ Cancel Withdrawal', callback_data: 'menu:main' }]]}});
             if (newSentPromptMsg) { // If new message sent successfully, update state's messageId
                userStateCache.set(userId, {
                    state: 'awaiting_withdrawal_amount', chatId: chatId, messageId: newSentPromptMsg.message_id,
                    data: { breadcrumb, originalMessageId: newSentPromptMsg.message_id }, timestamp: Date.now()
                });
             }
        });

    } catch (error) {
        console.error(`${logPrefix} Error: ${error.message}`);
        const errorMsg = `❌ Error starting withdrawal. Please try again. Or /cancel.`;
        if (msgOrCbMsg.message_id && msgOrCbMsg.data && messageToEditId) {
            bot.editMessageText(errorMsg, {chat_id: chatId, message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: [[{text: "↩️ Back to Menu", callback_data: "menu:main"}]]}}).catch(e => safeSendMessage(chatId, errorMsg, {parse_mode: 'MarkdownV2'}));
        } else {
             safeSendMessage(chatId, errorMsg, { parse_mode: 'MarkdownV2' });
        }
    }
}

/** Generic function to show bet amount buttons for a game */
async function showBetAmountButtons(msgOrCbMsg, gameKey, existingBetAmountStr = null) {
    const chatId = msgOrCbMsg.chat.id;
    const userId = String(msgOrCbMsg.from.id);
    const gameConfig = GAME_CONFIG[gameKey];
    const logPrefix = `[ShowBetButtons User ${userId} Game ${gameKey}]`;
    let messageToEditId = msgOrCbMsg.message_id; // ID of the message to edit (either command or menu button message)

    if (!gameConfig) {
        const errorMsg = `⚠️ Internal error: Unknown game \`${escapeMarkdownV2(gameKey)}\`.`;
        if (messageToEditId && msgOrCbMsg.data) { // From callback
             return bot.editMessageText(errorMsg, {chat_id: chatId, message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: [[{text:"↩️ Back", callback_data:"menu:main"}]]}});
        }
        return safeSendMessage(chatId, errorMsg, { parse_mode: 'MarkdownV2' });
    }

    const balance = await getUserBalance(userId);
    if (balance < gameConfig.minBetLamports) {
        const lowBalMsg = `⚠️ Your balance (${escapeMarkdownV2(formatSol(balance))} SOL) is too low to play ${escapeMarkdownV2(gameConfig.name)} (min bet: ${escapeMarkdownV2(formatSol(gameConfig.minBetLamports))} SOL). Use /deposit to add funds.`;
        const lowBalKeyboard = { inline_keyboard: [[{ text: '💰 Quick Deposit', callback_data: 'quick_deposit' }],[{ text: '↩️ Back to Games', callback_data: 'menu:main' }]]};
        if (messageToEditId && msgOrCbMsg.data) {
            return bot.editMessageText(lowBalMsg, { chat_id: chatId, message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: lowBalKeyboard });
        }
        return safeSendMessage(chatId, lowBalMsg, { parse_mode: 'MarkdownV2', reply_markup: lowBalKeyboard });
    }

    let lastBetForThisGame = existingBetAmountStr ? BigInt(existingBetAmountStr) : null;
    if (!lastBetForThisGame) {
        const userLastBets = userLastBetAmounts.get(userId) || new Map();
        lastBetForThisGame = userLastBets.get(gameKey) || null;
    }

    const buttons = [];
    const amountsRow1 = [], amountsRow2 = [];
    STANDARD_BET_AMOUNTS_LAMPORTS.forEach((lamports, index) => { // STANDARD_BET_AMOUNTS_LAMPORTS from Part 1
        if (lamports >= gameConfig.minBetLamports && lamports <= gameConfig.maxBetLamports) {
            const solAmountStr = STANDARD_BET_AMOUNTS_SOL[index].toFixed(2).replace(/\.00$/, ''); // e.g. "0.01", "0.5", "1"
            let buttonText = `${solAmountStr} SOL`;
            if (lastBetForThisGame && lamports === lastBetForThisGame) {
                buttonText = `前回 ${buttonText} 👍`;
            }
            // Determine next action based on gameKey
            let callbackActionData = `confirm_bet:${gameKey}:${lamports.toString()}`;
            if (gameKey === 'coinflip') { // Coinflip needs side choice after amount
                 callbackActionData = `coinflip_select_side:${lamports.toString()}`; // New action for Part 5a to handle
            } else if (gameKey === 'race') { // Race needs horse choice
                 callbackActionData = `race_select_horse:${lamports.toString()}`;
            } else if (gameKey === 'roulette') { // Roulette needs bet type choice
                 callbackActionData = `roulette_select_bet_type:${lamports.toString()}`;
            }

            const button = { text: buttonText, callback_data: callbackActionData };
            if (amountsRow1.length < 3) amountsRow1.push(button);
            else amountsRow2.push(button);
        }
    });
    if (amountsRow1.length > 0) buttons.push(amountsRow1);
    if (amountsRow2.length > 0) buttons.push(amountsRow2);

    buttons.push([
        { text: "✏️ Custom Amount", callback_data: `custom_amount_select:${gameKey}` },
        { text: "❌ Cancel", callback_data: 'menu:main' }
    ]);

    const breadcrumb = gameConfig.name;
    let prompt = `${escapeMarkdownV2(breadcrumb)}\nSelect bet amount for *${escapeMarkdownV2(gameConfig.name)}*:`;
    if (gameKey === 'slots') {
        const jackpotAmount = await getJackpotAmount('slots'); // from Part 2
        prompt += `\n\n💎 Current Slots Jackpot: *${escapeMarkdownV2(formatSol(jackpotAmount))} SOL*`;
    }
    if (gameKey === 'roulette') prompt += "\n_(Common payouts: Straight Up 35x, Even Money Bets 1x winnings)_";
    else if (gameKey === 'blackjack') prompt += "\n_(Blackjack pays 3:2, Dealer typically stands on 17)_";


    if (msgOrCbMsg.message_id && msgOrCbMsg.chat) { // msgOrCbMsg could be a Message from command or callbackQuery.message
        bot.editMessageText(prompt, {
            chat_id: chatId, message_id: messageToEditId,
            reply_markup: { inline_keyboard: buttons }, parse_mode: 'MarkdownV2'
        }).catch(e => {
            console.warn(`${logPrefix} Failed to edit message ${messageToEditId} for bet amounts, sending new: ${e.message}`);
            safeSendMessage(chatId, prompt, { reply_markup: { inline_keyboard: buttons }, parse_mode: 'MarkdownV2' });
        });
    } else { // Fallback if no message_id (should not happen for commands/callbacks)
        safeSendMessage(chatId, prompt, { reply_markup: { inline_keyboard: buttons }, parse_mode: 'MarkdownV2' });
    }
}


// --- Game Command Handlers ---
async function handleCoinflipCommand(msgOrCbMsg, args) {
    const betAmountLamportsStr = args[1]; // Optional direct bet amount from command like /cf 0.1
    if (betAmountLamportsStr) {
        try {
            const amountSOL = parseFloat(betAmountLamportsStr);
            if (isNaN(amountSOL) || amountSOL <=0) throw new Error("Invalid amount");
            const amountLamports = BigInt(Math.round(amountSOL * LAMPORTS_PER_SOL));
            const gameConfig = GAME_CONFIG.coinflip;
            if (amountLamports < gameConfig.minBetLamports || amountLamports > gameConfig.maxBetLamports) {
                 safeSendMessage(msgOrCbMsg.chat.id, `⚠️ Bet amount for Coinflip must be between ${escapeMarkdownV2(formatSol(gameConfig.minBetLamports))} and ${escapeMarkdownV2(formatSol(gameConfig.maxBetLamports))} SOL.`, {parse_mode:'MarkdownV2'});
                 return showBetAmountButtons(msgOrCbMsg, 'coinflip');
            }
            // If amount is valid, proceed to ask for heads/tails
            const breadcrumb = `${gameConfig.name} > Bet ${formatSol(amountLamports)} SOL > Choose Side`;
            const cfKeyboard = [
                [{ text: '👤 Heads', callback_data: `confirm_bet:coinflip:${amountLamports}:heads` }, { text: '🪙 Tails', callback_data: `confirm_bet:coinflip:${amountLamports}:tails` }],
                [{ text: '↩️ Change Amount', callback_data: `menu:coinflip` }, { text: '❌ Cancel', callback_data: 'menu:main' }]
            ];
            const promptMsg = `${escapeMarkdownV2(breadcrumb)}\nSelected amount: ${escapeMarkdownV2(formatSol(amountLamports))} SOL. Choose Heads or Tails:`;
            if (msgOrCbMsg.message_id && msgOrCbMsg.data) { // From menu callback
                 return bot.editMessageText(promptMsg, {chat_id: msgOrCbMsg.chat.id, message_id: msgOrCbMsg.message_id, reply_markup: {inline_keyboard: cfKeyboard}, parse_mode: 'MarkdownV2'});
            } else { // From direct command
                return safeSendMessage(msgOrCbMsg.chat.id, promptMsg, {reply_markup: {inline_keyboard: cfKeyboard}, parse_mode: 'MarkdownV2'});
            }
        } catch (e) {
            safeSendMessage(msgOrCbMsg.chat.id, "⚠️ Invalid bet amount format. Please use a number (e.g., /cf 0.1).", {parse_mode:'MarkdownV2'});
            return showBetAmountButtons(msgOrCbMsg, 'coinflip');
        }
    } else {
        await showBetAmountButtons(msgOrCbMsg, 'coinflip');
    }
}
async function handleRaceCommand(msgOrCbMsg, args) {
    // Race requires selecting a horse after amount, so showBetAmountButtons will lead to race_select_horse
    const stateForProceed = { state: `awaiting_race_amount_selection`, chatId: msgOrCbMsg.chat.id, messageId: msgOrCbMsg.message_id, data: { gameKey: 'race', breadcrumb: GAME_CONFIG.race.name, step: 'select_horse', originalMessageId: msgOrCbMsg.message_id }, timestamp: Date.now()};
    userStateCache.set(String(msgOrCbMsg.from.id), stateForProceed);
    // The callback from showBetAmountButtons for race (race_select_horse:AMOUNT) will be handled by proceedToGameStep
    await showBetAmountButtons(msgOrCbMsg, 'race', args[1] ? String(BigInt(parseFloat(args[1])*LAMPORTS_PER_SOL)) : null);
}
async function handleSlotsCommand(msgOrCbMsg, args) { await showBetAmountButtons(msgOrCbMsg, 'slots', args[1] ? String(BigInt(parseFloat(args[1])*LAMPORTS_PER_SOL)) : null); }
async function handleWarCommand(msgOrCbMsg, args) { await showBetAmountButtons(msgOrCbMsg, 'war', args[1] ? String(BigInt(parseFloat(args[1])*LAMPORTS_PER_SOL)) : null); }
async function handleRouletteCommand(msgOrCbMsg, args) {
    const stateForProceed = { state: `awaiting_roulette_amount_selection`, chatId: msgOrCbMsg.chat.id, messageId: msgOrCbMsg.message_id, data: { gameKey: 'roulette', breadcrumb: GAME_CONFIG.roulette.name, step: 'select_bet_type', originalMessageId: msgOrCbMsg.message_id }, timestamp: Date.now()};
    userStateCache.set(String(msgOrCbMsg.from.id), stateForProceed);
    await showBetAmountButtons(msgOrCbMsg, 'roulette', args[1] ? String(BigInt(parseFloat(args[1])*LAMPORTS_PER_SOL)) : null);
}
async function handleCrashCommand(msgOrCbMsg, args) { await showBetAmountButtons(msgOrCbMsg, 'crash', args[1] ? String(BigInt(parseFloat(args[1])*LAMPORTS_PER_SOL)) : null); }
async function handleBlackjackCommand(msgOrCbMsg, args) { await showBetAmountButtons(msgOrCbMsg, 'blackjack', args[1] ? String(BigInt(parseFloat(args[1])*LAMPORTS_PER_SOL)) : null); }

async function handleLeaderboardsCommand(msgOrCbMsg, args) {
    const chatId = msgOrCbMsg.chat.id;
    const gameKeyArg = args[1]?.toLowerCase(); // Game key from command like /leaderboard slots
    let messageToEditId = msgOrCbMsg.message_id;

    // This is a placeholder for actual leaderboard fetching logic from 'game_leaderboards' table
    let leaderText = "🏆 *Leaderboards*\n\n";
    const leaderboardKeyboard = [[{ text: '↩️ Back to Main Menu', callback_data: 'menu:main' }]];

    if (gameKeyArg && GAME_CONFIG[gameKeyArg]) {
        leaderText += `Top players for *${escapeMarkdownV2(GAME_CONFIG[gameKeyArg].name || gameKeyArg)}* \\(Example Data\\):\n`;
        leaderText += `  1\\. UserAlpha \\- 1000 SOL wins\n`;
        leaderText += `  2\\. UserBeta \\- 500 SOL wins\n`;
        leaderText += `  3\\. UserGamma \\- 250 SOL wins\n`;
        // TODO: Add buttons to switch leaderboard periods (daily, weekly, all-time) or score types (total wagered, profit)
    } else {
        leaderText += "Use \`/leaderboard <game_key>\` e\\.g\\. \`/leaderboard slots\` to see specific game stats\\.\n" +
                      "Or choose a game below \\(Not yet implemented\\):";
        // Example buttons for specific leaderboards - functionality to be added
        // const gameButtons = Object.keys(GAME_CONFIG).map(gk => ({text: GAME_CONFIG[gk].name, callback_data: `leaderboard_game:${gk}`}));
        // leaderboardKeyboard.unshift(...gameButtons.map(b => [b])); // Add game buttons at the top
    }
    leaderText += "\n\n_Leaderboard feature is under development for full data\\._";
    
    if (msgOrCbMsg.message_id && msgOrCbMsg.data) { // From callback menu:leaderboards
        bot.editMessageText(leaderText, {chat_id: chatId, message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: leaderboardKeyboard}}).catch(e => safeSendMessage(chatId, leaderText, {parse_mode:'MarkdownV2', reply_markup: {inline_keyboard: leaderboardKeyboard}}));
    } else { // From /leaderboard command
        safeSendMessage(msgOrCbMsg.chat.id, leaderText, {parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: leaderboardKeyboard}});
    }
}

// Assign to the 'let' declared variables from Part 1
commandHandlers = new Map([
    ['/start', handleStartCommand],
    ['/help', handleHelpCommand],
    ['/wallet', handleWalletCommand],
    ['/referral', handleReferralCommand],
    ['/deposit', handleDepositCommand],
    ['/withdraw', handleWithdrawCommand],
    ['/history', handleHistoryCommand],
    ['/leaderboards', handleLeaderboardsCommand], // Corrected from /leaderboard
    ['/coinflip', handleCoinflipCommand], ['/cf', handleCoinflipCommand],
    ['/race', handleRaceCommand],
    ['/slots', handleSlotsCommand],
    ['/roulette', handleRouletteCommand],
    ['/war', handleWarCommand],
    ['/crash', handleCrashCommand],
    ['/blackjack', handleBlackjackCommand],
    // Admin command is handled separately in handleMessage to check user ID first
]);

menuCommandHandlers = new Map([
    ['main', handleStartCommand],
    ['wallet', handleWalletCommand],
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
    // Special callbacks handled in handleCallbackQuery directly:
    // 'back', 'play_again', 'quick_deposit', 'confirm_bet', 'blackjack_action', 'crash_cash_out', 'custom_amount_select', 'roulette_bet_type'
]);

async function handleAdminCommand(msg, argsArray) {
    const userId = String(msg.from.id);
    const chatId = msg.chat.id;
    const command = argsArray[1]?.toLowerCase(); // Sub-command, e.g., /admin broadcast message
    const subArgs = argsArray.slice(2);
    const logPrefix = `[AdminCmd User ${userId}]`;

    if (!process.env.ADMIN_USER_IDS.split(',').map(id=>id.trim()).includes(userId)) {
        return safeSendMessage(chatId, "You are not authorized to use admin commands.");
    }

    console.log(`${logPrefix} Received admin command: ${command}, args: ${subArgs.join(' ')}`);
    let reply = "Admin command executed.";

    switch (command) {
        case 'broadcast':
            if (subArgs.length === 0) {
                reply = "Usage: /admin broadcast <message_to_send>";
            } else {
                const broadcastMessage = subArgs.join(' ');
                console.log(`${logPrefix} Broadcasting: ${broadcastMessage}`);
                // TODO: Implement actual broadcast logic (fetch all user_ids from DB and send message)
                // This is a placeholder for a complex feature.
                reply = `Broadcast initiated with message: "${broadcastMessage.substring(0,50)}..." (Actual broadcast not yet implemented).`;
                await notifyAdmin(`Admin ${userId} initiated broadcast: ${escapeMarkdownV2(broadcastMessage)}`);
            }
            break;
        case 'checkbalance': // Check bot's main wallet balance
            const mainKey = process.env.MAIN_BOT_PRIVATE_KEY;
            if (!mainKey) { reply = "Main bot private key not set."; break; }
            try {
                const keypair = Keypair.fromSecretKey(bs58.decode(mainKey));
                const balance = await solanaConnection.getBalance(keypair.publicKey);
                reply = `Main bot wallet (${keypair.publicKey.toBase58()}) balance: ${escapeMarkdownV2(formatSol(balance))} SOL.`;
            } catch (e) {
                reply = `Error checking main balance: ${e.message}`;
            }
            break;
        case 'userinfo':
            const targetUserId = subArgs[0];
            if (!targetUserId) { reply = "Usage: /admin userinfo <user_id>"; break; }
            const userInfo = await getUserWalletDetails(targetUserId); // from Part 2
            const userBal = await getUserBalance(targetUserId);
            if (userInfo) {
                reply = `User Info for ${targetUserId}:\nBalance: ${formatSol(userBal)} SOL\nWallet: ${userInfo.external_withdrawal_address || 'N/A'}\nReferral Code: ${userInfo.referral_code || 'N/A'}\nReferred By: ${userInfo.referred_by_user_id || 'N/A'}\nTotal Wagered: ${formatSol(userInfo.total_wagered)} SOL`;
            } else {
                reply = `User ${targetUserId} not found.`;
            }
            break;
        // Add more admin commands here: refund, manual payout, check system status, etc.
        default:
            reply = `Unknown admin command: ${command}.\nAvailable: broadcast, checkbalance, userinfo`;
            break;
    }
    await safeSendMessage(chatId, escapeMarkdownV2(reply), {parse_mode: 'MarkdownV2'});
}


// --- Referral Check & Payout Triggering Logic ---
/**
 * Checks for and processes referral bonuses after a bet completes successfully.
 * Runs within the provided database client's transaction.
 * @param {string} refereeUserId - The user who placed the bet.
 * @param {number} completedBetId - The ID of the completed bet.
 * @param {bigint} wagerAmountLamports - The amount wagered in the bet.
 * @param {import('pg').PoolClient} client The active database client (for transactional integrity).
 */
async function _handleReferralChecks(refereeUserId, completedBetId, wagerAmountLamports, client) {
    refereeUserId = String(refereeUserId);
    const logPrefix = `[ReferralCheck Bet ${completedBetId} User ${refereeUserId}]`;

    try {
        // Fetch referee details using the provided client to ensure transactional read
        const refereeDetails = await getUserWalletDetails(refereeUserId, client); // Pass client
        if (!refereeDetails?.referred_by_user_id) {
            // console.log(`${logPrefix} Referee ${refereeUserId} was not referred. No referral checks needed.`);
            return;
        }

        const referrerUserId = refereeDetails.referred_by_user_id;
        // Fetch referrer details also within the same transaction client
        const referrerDetails = await getUserWalletDetails(referrerUserId, client); // Pass client
        if (!referrerDetails?.external_withdrawal_address) {
            console.warn(`${logPrefix} Referrer ${referrerUserId} has no linked wallet. Cannot process referral payouts for them.`);
            return;
        }

        let payoutQueuedForReferrer = false; // To track if we initiated any payout for this referrer from this bet

        // 1. Check Initial Bet Bonus for the Referee
        // isFirstCompletedBet should also use the client for transactional consistency
        const isRefereeFirstCompletedBet = await isFirstCompletedBet(refereeUserId, completedBetId, client); // Pass client

        if (isRefereeFirstCompletedBet && wagerAmountLamports >= REFERRAL_INITIAL_BET_MIN_LAMPORTS) { // REFERRAL_INITIAL_BET_MIN_LAMPORTS from Part 1
            // Lock referrer's wallet row to get accurate referral_count and prevent race conditions if multiple referees trigger this.
            const referrerWalletForCount = await queryDatabase('SELECT referral_count FROM wallets WHERE user_id = $1 FOR UPDATE', [referrerUserId], client);
            const currentReferrerCount = parseInt(referrerWalletForCount.rows[0]?.referral_count || '0', 10);

            let bonusTier = REFERRAL_INITIAL_BONUS_TIERS[REFERRAL_INITIAL_BONUS_TIERS.length - 1]; // Default to highest tier
            for (const tier of REFERRAL_INITIAL_BONUS_TIERS) { // REFERRAL_INITIAL_BONUS_TIERS from Part 1
                if (currentReferrerCount <= tier.maxCount) {
                    bonusTier = tier;
                    break;
                }
            }
            const initialBonusAmount = BigInt(Math.floor(Number(wagerAmountLamports) * bonusTier.percent));

            if (initialBonusAmount > 0n) {
                const payoutRecord = await recordPendingReferralPayout(referrerUserId, refereeUserId, 'initial_bet', initialBonusAmount, completedBetId, null, client); // recordPendingReferralPayout from Part 2
                if (payoutRecord.success && payoutRecord.payoutId) {
                    await addPayoutJob({ type: 'payout_referral', payoutId: payoutRecord.payoutId, userId: referrerUserId }); // addPayoutJob from Part 6
                    payoutQueuedForReferrer = true;
                    console.log(`${logPrefix} Queued initial bet referral payout ID ${payoutRecord.payoutId} for referrer ${referrerUserId} (${formatSol(initialBonusAmount)} SOL).`);
                } else if (!payoutRecord.duplicate) { // Don't log error if it was a duplicate handled by DB constraint
                    console.error(`${logPrefix} Failed to record initial bonus payout for referrer ${referrerUserId}: ${payoutRecord.error}`);
                }
            }
        }

        // 2. Check Milestone Bonus for the Referee's Wagers benefitting the Referrer
        // refereeDetails.total_wagered already includes the current wagerAmountLamports due to updateUserWagerStats in placeBet
        const currentTotalWageredByReferee = refereeDetails.total_wagered;
        const lastMilestonePaidForReferee = refereeDetails.last_milestone_paid_lamports; // This is the threshold *they* last triggered a payment for *their* referrer.
                                                                                        // This needs to be the last milestone *this specific referrer* was paid for *this referee*.
                                                                                        // The current DB schema (wallets.last_milestone_paid_lamports on referee) is not ideal for this specific per-referrer tracking.
                                                                                        // For now, we'll assume it means the highest milestone the referee has crossed that *anyone* could have been paid for.
                                                                                        // A more robust system would track (referrer, referee, milestone_level_paid) in referral_payouts.

        let milestoneCrossedForThisReferrer = null;
        for (const threshold of REFERRAL_MILESTONE_THRESHOLDS_LAMPORTS) { // From Part 1
            if (currentTotalWageredByReferee >= threshold && threshold > lastMilestonePaidForReferee ) { // Check against referee's own last paid milestone
                milestoneCrossedForThisReferrer = threshold; // This is the highest new milestone crossed
                                                            // Could pay for multiple if a large wager crosses several. Simpler to pay for highest new.
            } else if (currentTotalWageredByReferee < threshold) {
                break; // No need to check higher thresholds
            }
        }

        if (milestoneCrossedForThisReferrer) {
            const milestoneBonusAmount = BigInt(Math.floor(Number(milestoneCrossedForThisReferrer) * REFERRAL_MILESTONE_REWARD_PERCENT)); // REFERRAL_MILESTONE_REWARD_PERCENT from Part 1
            if (milestoneBonusAmount > 0n) {
                // Attempt to record this specific milestone payout. The unique constraint in DB for milestone payouts
                // (referrer_user_id, referee_user_id, payout_type='milestone', milestone_reached_lamports)
                // should prevent duplicate payments for the exact same milestone for this pair.
                const payoutRecord = await recordPendingReferralPayout(
                    referrerUserId,
                    refereeUserId,
                    'milestone',
                    milestoneBonusAmount,
                    null, /* triggeringBetId not directly relevant for milestone */
                    milestoneCrossedForThisReferrer,
                    client
                );

                if (payoutRecord.success && payoutRecord.payoutId) {
                    await addPayoutJob({ type: 'payout_referral', payoutId: payoutRecord.payoutId, userId: referrerUserId });
                    payoutQueuedForReferrer = true;
                    console.log(`${logPrefix} Queued milestone referral payout ID ${payoutRecord.payoutId} for referrer ${referrerUserId} (${formatSol(milestoneBonusAmount)} SOL for ${formatSol(milestoneCrossedForThisReferrer)} milestone).`);
                    // Update the referee's `last_milestone_paid_lamports` to this new highest threshold
                    // This should ideally be updated only if the payout is for *this* referrer and *this* specific milestone.
                    // The current logic updates the referee's general marker.
                    await queryDatabase(
                        'UPDATE wallets SET last_milestone_paid_lamports = GREATEST(last_milestone_paid_lamports, $1) WHERE user_id = $2',
                        [milestoneCrossedForThisReferrer, refereeUserId], client
                    );

                } else if (payoutRecord.duplicate) {
                    console.log(`${logPrefix} Milestone payout for ${formatSol(milestoneCrossedForThisReferrer)} for referrer ${referrerUserId} from referee ${refereeUserId} was a duplicate (already paid or pending).`);
                } else {
                    console.error(`${logPrefix} Failed to record milestone bonus payout for referrer ${referrerUserId}: ${payoutRecord.error}`);
                }
            }
        }
        // No explicit commit/rollback here, as this function is called within the larger `placeBet` transaction.
        if (payoutQueuedForReferrer) {
            // Optionally notify referrer if a new payout has been queued (they'll get another when it's actually sent)
            // safeSendMessage(referrerUserId, `💰 You have new referral earnings pending from ${refereeUserId}! They will be processed shortly.`).catch(()=>{});
        }

    } catch (error) {
        console.error(`${logPrefix} Error during referral checks (within transaction): ${error.message}`, error.stack);
        // Do not rollback here; let the calling function (placeBet) handle transaction control.
        // Re-throw to ensure the main transaction in placeBet is rolled back.
        throw error;
    }
}

// --- End of Part 5b ---
// index.js - Part 6: Background Tasks, Payouts, Startup & Shutdown
// --- VERSION: 3.2.1 --- (Incorporating all discussed DB Init Fixes)

// --- Assuming functions & constants from Parts 1, 2, 3, 5a, 5b are available ---
// Global variables like 'server', 'isFullyInitialized', 'depositMonitorIntervalId', 'sweepIntervalId',
// 'leaderboardManagerIntervalId', 'currentSlotsJackpotLamports', various caches, queues,
// 'pool', 'bot', 'solanaConnection', 'app', 'GAME_CONFIG', 'ADMIN_USER_IDS', etc., are from Part 1.
// DB ops: queryDatabase, ensureJackpotExists, getJackpotAmount, etc. (from Part 2)
// Utils: notifyAdmin, safeSendMessage, escapeMarkdownV2, sleep, formatSol, etc. (from Part 1/3)
// Handlers: handleMessage, handleCallbackQuery (from Part 5a)
// Solana specific imports like Keypair, bs58 from Part 1 imports.

// --- Database Initialization ---
async function initializeDatabase() {
    console.log('⚙️ [DB Init] Initializing Database Schema...');
    let client = null;
    try {
        client = await pool.connect(); // pool from Part 1
        await client.query('BEGIN');
        console.log('⚙️ [DB Init] Transaction started.');

        // Wallets Table
        console.log('⚙️ [DB Init] Ensuring wallets table...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS wallets (
                user_id VARCHAR(255) PRIMARY KEY,
                external_withdrawal_address VARCHAR(44), -- Solana address length
                linked_at TIMESTAMPTZ,
                last_used_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                referral_code VARCHAR(12) UNIQUE, -- e.g., ref_XXXXXXXX
                referred_by_user_id VARCHAR(255) REFERENCES wallets(user_id) ON DELETE SET NULL,
                referral_count INTEGER NOT NULL DEFAULT 0,
                total_wagered BIGINT NOT NULL DEFAULT 0,
                last_milestone_paid_lamports BIGINT NOT NULL DEFAULT 0,
                last_bet_amounts JSONB DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);

        // Ensure 'last_bet_amounts' column exists if table was created before this column was added to DDL
        console.log('⚙️ [DB Init] Ensuring wallets.last_bet_amounts column exists...');
        try {
            await client.query(`ALTER TABLE wallets ADD COLUMN last_bet_amounts JSONB DEFAULT '{}'::jsonb;`);
            console.log('✅ [DB Init] Column wallets.last_bet_amounts added (or an error was expected if it already existed and caught).');
        } catch (e) {
            if (e.code === '42701') { // 42701 is 'duplicate_column' error in PostgreSQL
                console.log('ℹ️ [DB Init] Column wallets.last_bet_amounts already exists.');
            } else {
                console.error(`❌ [DB Init] Error trying to add wallets.last_bet_amounts: ${e.message} (Code: ${e.code})`);
                throw e; // Re-throw other errors to fail initialization
            }
        }

        console.log('⚙️ [DB Init] Ensuring wallets indexes...');
        await client.query('CREATE INDEX IF NOT EXISTS idx_wallets_referral_code ON wallets (referral_code);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_wallets_referred_by ON wallets (referred_by_user_id);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_wallets_total_wagered ON wallets (total_wagered DESC);');


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
        await client.query(`
            CREATE TABLE IF NOT EXISTS deposit_addresses (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                deposit_address VARCHAR(44) NOT NULL UNIQUE,
                derivation_path VARCHAR(255) NOT NULL,
                status VARCHAR(30) NOT NULL DEFAULT 'pending', -- pending, used, expired, swept, sweep_error, sweep_error_key_mismatch, sweep_error_processing, swept_low_balance
                expires_at TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_checked_at TIMESTAMPTZ
            );
        `);
        console.log('⚙️ [DB Init] Ensuring deposit_addresses indexes...');
        await client.query('CREATE INDEX IF NOT EXISTS idx_deposit_addresses_user_id ON deposit_addresses (user_id);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_deposit_addresses_status_expires ON deposit_addresses (status, expires_at);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_deposit_addresses_status_last_checked ON deposit_addresses (status, last_checked_at ASC NULLS FIRST);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_deposit_addresses_status_created ON deposit_addresses (status, created_at ASC);');


        // Deposits Table
        console.log('⚙️ [DB Init] Ensuring deposits table...');
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
        console.log('⚙️ [DB Init] Ensuring deposits indexes...');
        await client.query('CREATE INDEX IF NOT EXISTS idx_deposits_user_id ON deposits (user_id);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_deposits_deposit_address_id ON deposits (deposit_address_id);');


        // Bets Table
        console.log('⚙️ [DB Init] Ensuring bets table...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS bets (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                chat_id VARCHAR(255) NOT NULL,
                game_type VARCHAR(50) NOT NULL,
                bet_details JSONB,
                wager_amount_lamports BIGINT NOT NULL CHECK (wager_amount_lamports > 0),
                payout_amount_lamports BIGINT,
                status VARCHAR(30) NOT NULL DEFAULT 'active',
                priority INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                processed_at TIMESTAMPTZ
            );
        `);
        console.log('⚙️ [DB Init] Ensuring bets indexes...');
        await client.query('CREATE INDEX IF NOT EXISTS idx_bets_user_id_status ON bets (user_id, status);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_bets_game_type_status ON bets (game_type, status);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_bets_created_at_desc ON bets (created_at DESC);');


        // Inside initializeDatabase function in Part 6:

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
                last_bet_amounts JSONB DEFAULT '{}'::jsonb, // Column definition included here
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);
        // REMOVED the ALTER TABLE ... ADD COLUMN last_bet_amounts block entirely.

        console.log('⚙️ [DB Init] Ensuring wallets indexes...'); // This should now run without transaction aborted error
        await client.query('CREATE INDEX IF NOT EXISTS idx_wallets_referral_code ON wallets (referral_code);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_wallets_referred_by ON wallets (referred_by_user_id);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_wallets_total_wagered ON wallets (total_wagered DESC);');

        // User Balances Table (No changes needed here)
        console.log('⚙️ [DB Init] Ensuring user_balances table...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS user_balances (
                user_id VARCHAR(255) PRIMARY KEY REFERENCES wallets(user_id) ON DELETE CASCADE,
                balance_lamports BIGINT NOT NULL DEFAULT 0 CHECK (balance_lamports >= 0),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);

        // ... (rest of table/index creations, ensuring referral_payouts and ledger DDL are also corrected as previously discussed) ...

        // Referral Payouts Table
        console.log('⚙️ [DB Init] Ensuring referral_payouts table...');
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
        // Correctly define the partial unique index separately:
        console.log('⚙️ [DB Init] Ensuring referral_payouts unique partial index for milestones...');
        await client.query(`
            CREATE UNIQUE INDEX IF NOT EXISTS idx_refpayout_unique_milestone ON referral_payouts
            (referrer_user_id, referee_user_id, payout_type, milestone_reached_lamports)
            WHERE (payout_type = 'milestone');
        `);
        console.log('⚙️ [DB Init] Ensuring other referral_payouts indexes...');
        await client.query('CREATE INDEX IF NOT EXISTS idx_refpayout_referrer_status ON referral_payouts (referrer_user_id, status);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_refpayout_referee ON referral_payouts (referee_user_id);');


        // Ledger Table
        console.log('⚙️ [DB Init] Ensuring ledger table...');
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
        console.log('⚙️ [DB Init] Ensuring ledger indexes...');
        await client.query('CREATE INDEX IF NOT EXISTS idx_ledger_user_id_created_desc ON ledger (user_id, created_at DESC);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_ledger_transaction_type ON ledger (transaction_type);');
        // Corrected conditional indexes: WHERE clause outside parentheses
        await client.query('CREATE INDEX IF NOT EXISTS idx_ledger_related_bet_id ON ledger (related_bet_id) WHERE related_bet_id IS NOT NULL;');
        await client.query('CREATE INDEX IF NOT EXISTS idx_ledger_related_withdrawal_id ON ledger (related_withdrawal_id) WHERE related_withdrawal_id IS NOT NULL;');


        // Jackpots Table
        console.log('⚙️ [DB Init] Ensuring jackpots table...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS jackpots (
                game_key VARCHAR(50) PRIMARY KEY,
                current_amount_lamports BIGINT NOT NULL DEFAULT 0 CHECK (current_amount_lamports >= 0),
                last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);

        // Leaderboards Table
        console.log('⚙️ [DB Init] Ensuring game_leaderboards table...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS game_leaderboards (
                id SERIAL PRIMARY KEY,
                game_key VARCHAR(50) NOT NULL,
                user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                score_type VARCHAR(50) NOT NULL,
                period_type VARCHAR(20) NOT NULL,
                period_identifier VARCHAR(50) NOT NULL,
                score BIGINT NOT NULL,
                player_display_name VARCHAR(255),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (game_key, user_id, score_type, period_type, period_identifier)
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
        if (typeof notifyAdmin === "function") {
            try { await notifyAdmin(`🚨 CRITICAL DB INIT FAILED: ${escapeMarkdownV2(String(err.message || err))}\\. Bot cannot start\\. Check logs immediately\\.`).catch(()=>{}); } catch {}
        } else {
            console.error(`[ADMIN ALERT during DB Init Error] DB Init failed: ${String(err.message || err)}`)
        }
        process.exit(2);
    } finally {
        if (client) client.release();
    }
}

async function loadActiveDepositsCache() {
    const logPrefix = '[LoadActiveDeposits]';
    console.log(`⚙️ ${logPrefix} Loading active deposit addresses from DB into cache...`);
    let count = 0; let expiredCount = 0;
    try {
        const res = await queryDatabase(`SELECT deposit_address, user_id, expires_at FROM deposit_addresses WHERE status = 'pending'`);
        const now = Date.now();
        for (const row of res.rows) {
            const expiresAtTime = new Date(row.expires_at).getTime();
            if (now < expiresAtTime) {
                addActiveDepositAddressCache(row.deposit_address, row.user_id, expiresAtTime);
                count++;
            } else {
                expiredCount++;
                queryDatabase("UPDATE deposit_addresses SET status = 'expired' WHERE deposit_address = $1 AND status = 'pending'", [row.deposit_address])
                    .catch(e => console.error(`${logPrefix} Failed to mark old address ${row.deposit_address} as expired during cache load: ${e.message}`));
            }
        }
        console.log(`✅ ${logPrefix} Loaded ${count} active deposit addresses into cache. Found and processed ${expiredCount} already expired.`);
    } catch (error) {
        console.error(`❌ ${logPrefix} Error loading active deposits: ${error.message}`);
        if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 ERROR Loading active deposit cache on startup: ${escapeMarkdownV2(String(error.message || error))}`);
    }
}

async function loadSlotsJackpot() {
    const logPrefix = '[LoadSlotsJackpot]';
    console.log(`⚙️ ${logPrefix} Loading/Ensuring slots jackpot...`);
    let client = null;
    let loadedJackpotAmount = SLOTS_JACKPOT_SEED_LAMPORTS;
    try {
        client = await pool.connect();
        await client.query('BEGIN');
        const ensured = await ensureJackpotExists('slots', SLOTS_JACKPOT_SEED_LAMPORTS, client);
        if (!ensured) {
            console.warn(`${logPrefix} Failed to ensure slots jackpot exists in DB. Will use seed value ${formatSol(SLOTS_JACKPOT_SEED_LAMPORTS)} SOL.`);
        } else {
            loadedJackpotAmount = await getJackpotAmount('slots', client);
        }
        await client.query('COMMIT');
        console.log(`✅ ${logPrefix} Slots jackpot in DB is ${formatSol(loadedJackpotAmount)} SOL.`);
        return loadedJackpotAmount;
    } catch (error) {
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { console.error(`${logPrefix} Rollback error on jackpot load: ${rbErr.message}`);} }
        console.error(`❌ ${logPrefix} Error loading slots jackpot: ${error.message}. Using seed value ${formatSol(SLOTS_JACKPOT_SEED_LAMPORTS)} SOL.`);
        if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 ERROR Loading slots jackpot: ${escapeMarkdownV2(String(error.message || error))}. Using seed value.`);
        return SLOTS_JACKPOT_SEED_LAMPORTS;
    } finally {
        if (client) client.release();
    }
}

function startDepositMonitor() {
    let intervalMs = parseInt(process.env.DEPOSIT_MONITOR_INTERVAL_MS, 10);
    if (isNaN(intervalMs) || intervalMs < 5000) {
        intervalMs = 20000;
        console.warn(`[DepositMonitor] Invalid DEPOSIT_MONITOR_INTERVAL_MS, using default ${intervalMs}ms.`);
    }
    if (depositMonitorIntervalId) {
        clearInterval(depositMonitorIntervalId);
        console.log('🔄 [DepositMonitor] Restarting deposit monitor...');
    } else {
        console.log(`⚙️ [DepositMonitor] Starting Deposit Monitor (Polling Interval: ${intervalMs / 1000}s)...`);
    }
    const initialDelay = parseInt(process.env.INIT_DELAY_MS, 10) || 3000;
    console.log(`[DepositMonitor] Scheduling first monitor run in ${initialDelay/1000}s...`);

    setTimeout(() => {
        try {
            console.log(`[DepositMonitor] Executing first monitor run...`);
            monitorDepositsPolling().catch(err => console.error("❌ [Initial Deposit Monitor Run] Error:", err.message, err.stack));
            depositMonitorIntervalId = setInterval(monitorDepositsPolling, intervalMs);
            if (depositMonitorIntervalId.unref) depositMonitorIntervalId.unref();
            console.log(`✅ [DepositMonitor] Recurring monitor interval set.`);
        } catch (initialRunError) {
            console.error("❌ [DepositMonitor] CRITICAL ERROR during initial monitor setup/run:", initialRunError);
            if (typeof notifyAdmin === "function") notifyAdmin(`🚨 CRITICAL ERROR setting up Deposit Monitor interval: ${escapeMarkdownV2(String(initialRunError.message || initialRunError))}`).catch(()=>{});
        }
    }, initialDelay);
}

monitorDepositsPolling.isRunning = false;
async function monitorDepositsPolling() {
    const logPrefix = '[DepositMonitor Polling]';
    if (monitorDepositsPolling.isRunning) {
        return;
    }
    monitorDepositsPolling.isRunning = true;
    let batchUpdateClient = null;

    try {
        const batchSize = parseInt(process.env.DEPOSIT_MONITOR_ADDRESS_BATCH_SIZE, 10) || 50;
        const sigFetchLimit = parseInt(process.env.DEPOSIT_MONITOR_SIGNATURE_FETCH_LIMIT, 10) || 5;

        const pendingAddressesRes = await queryDatabase(
            `SELECT id, deposit_address, user_id, derivation_path, expires_at
             FROM deposit_addresses
             WHERE status = 'pending' AND expires_at > NOW()
             ORDER BY last_checked_at ASC NULLS FIRST, created_at ASC
             LIMIT $1`,
            [batchSize]
        );

        if (pendingAddressesRes.rowCount === 0) {
            monitorDepositsPolling.isRunning = false;
            return;
        }

        batchUpdateClient = await pool.connect();
        await batchUpdateClient.query('BEGIN');
        const addressIdsToCheck = pendingAddressesRes.rows.map(r => r.id);
        await batchUpdateClient.query('UPDATE deposit_addresses SET last_checked_at = NOW() WHERE id = ANY($1::int[])', [addressIdsToCheck]);
        await batchUpdateClient.query('COMMIT');
        batchUpdateClient.release(); batchUpdateClient = null;

        for (const row of pendingAddressesRes.rows) {
            const address = row.deposit_address;
            const addressId = row.id;
            const userId = row.user_id;
            const addrLogPrefix = `[Monitor Addr:${address.slice(0,6)}.. ID:${addressId} User:${userId}]`;

            try {
                const pubKey = new PublicKey(address);
                const signatures = await solanaConnection.getSignaturesForAddress(
                    pubKey, { limit: sigFetchLimit }, DEPOSIT_CONFIRMATION_LEVEL
                );

                if (signatures && signatures.length > 0) {
                    for (const sigInfo of signatures.reverse()) {
                        if (sigInfo?.signature && !hasProcessedDepositTx(sigInfo.signature)) {
                            const isConfirmed = sigInfo.confirmationStatus === DEPOSIT_CONFIRMATION_LEVEL || sigInfo.confirmationStatus === 'finalized' || sigInfo.confirmationStatus === 'processed';
                            if (!sigInfo.err && isConfirmed) {
                                console.log(`${addrLogPrefix} Found new confirmed/processed TX: ${sigInfo.signature}. Queuing for processing.`);
                                depositProcessorQueue.add(() => processDepositTransaction(sigInfo.signature, address, addressId, userId))
                                    .catch(queueError => console.error(`❌ ${addrLogPrefix} Error adding TX ${sigInfo.signature} to deposit queue: ${queueError.message}`));
                                addProcessedDepositTx(sigInfo.signature);
                            } else if (sigInfo.err) {
                                addProcessedDepositTx(sigInfo.signature);
                            }
                        }
                    }
                }
            } catch (error) {
                console.error(`❌ ${addrLogPrefix} Error checking signatures: ${error.message}`);
                if (isRetryableSolanaError(error) && (error?.status === 429 || String(error?.message).toLowerCase().includes('rate limit'))) {
                    console.warn(`🚦 ${addrLogPrefix} Rate limit detected while fetching signatures. Pausing briefly...`);
                    await sleep(2000 + Math.random() * 1000);
                }
            }
        }
    } catch (error) {
        console.error(`❌ ${logPrefix} Error in main polling loop: ${error.message}`, error.stack);
        if (batchUpdateClient) {
            try { await batchUpdateClient.query('ROLLBACK'); } catch (rbErr) { console.error(`${logPrefix} Rollback failed for last_checked_at update: ${rbErr.message}`); }
            batchUpdateClient.release();
        }
        if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 ERROR in Deposit Monitor loop: ${escapeMarkdownV2(String(error.message || error))}`);
    } finally {
        if (batchUpdateClient) batchUpdateClient.release();
        monitorDepositsPolling.isRunning = false;
    }
}

async function processDepositTransaction(signature, depositAddress, depositAddressId, userId) {
    const logPrefix = `[ProcessDeposit TX:${signature.slice(0,6)}.. AddrID:${depositAddressId} User:${userId}]`;
    console.log(`${logPrefix} Processing transaction...`);
    let client = null;

    try {
        const tx = await solanaConnection.getTransaction(signature, {
            maxSupportedTransactionVersion: 0, commitment: DEPOSIT_CONFIRMATION_LEVEL
        });

        if (!tx) {
            console.warn(`⚠️ ${logPrefix} Transaction details not found via getTransaction for ${signature}. RPC lag or invalid/unconfirmed signature? Will retry on next monitor cycle.`);
            return;
        }

        if (tx.meta?.err) {
            console.log(`ℹ️ ${logPrefix} Transaction ${signature} has failed on-chain (meta.err: ${JSON.stringify(tx.meta.err)}). Ignoring.`);
            addProcessedDepositTx(signature);
            return;
        }

        const { transferAmount, payerAddress } = analyzeTransactionAmounts(tx, depositAddress);

        if (transferAmount <= 0n) {
            addProcessedDepositTx(signature);
            return;
        }
        const depositAmountSOL = formatSol(transferAmount);
        console.log(`✅ ${logPrefix} Valid deposit: ${depositAmountSOL} SOL from ${payerAddress || 'unknown'} for TX ${signature}.`);

        client = await pool.connect();
        await client.query('BEGIN');
        console.log(`${logPrefix} DB transaction started for ${signature}.`);

        const depositRecordResult = await recordConfirmedDeposit(client, userId, depositAddressId, signature, transferAmount);
        if (depositRecordResult.alreadyProcessed) {
            console.warn(`⚠️ ${logPrefix} TX ${signature} already processed by DB (ID: ${depositRecordResult.depositId}). Rolling back this attempt.`);
            await client.query('ROLLBACK');
            addProcessedDepositTx(signature);
            return;
        }
        if (!depositRecordResult.success || !depositRecordResult.depositId) {
            throw new Error(`Failed to record deposit in DB for ${signature}: ${depositRecordResult.error}`);
        }
        const depositId = depositRecordResult.depositId;
        console.log(`${logPrefix} Deposit ID ${depositId} recorded for TX ${signature}.`);

        const markedUsed = await markDepositAddressUsed(client, depositAddressId);
        if (!markedUsed) {
            console.warn(`⚠️ ${logPrefix} Could not mark deposit address ID ${depositAddressId} as 'used' (status might not have been 'pending'). Proceeding with balance update.`);
        } else {
            console.log(`${logPrefix} Marked deposit address ID ${depositAddressId} as 'used'.`);
        }

        const ledgerNote = `Deposit from ${payerAddress ? payerAddress.slice(0,6)+'..' : 'Unknown'} TX:${signature.slice(0,6)}..`;
        const balanceUpdateResult = await updateUserBalanceAndLedger(client, userId, transferAmount, 'deposit', { depositId }, ledgerNote);
        if (!balanceUpdateResult.success || balanceUpdateResult.newBalance === undefined) {
            throw new Error(`Failed to update user ${userId} balance/ledger for TX ${signature}: ${balanceUpdateResult.error}`);
        }
        const newBalanceSOL = formatSol(balanceUpdateResult.newBalance);
        console.log(`${logPrefix} User ${userId} balance updated. New balance: ${newBalanceSOL} SOL after TX ${signature}.`);

        const pendingRef = pendingReferrals.get(userId);
        if (pendingRef && pendingRef.referrerUserId && (Date.now() - pendingRef.timestamp < PENDING_REFERRAL_TTL_MS)) {
            console.log(`${logPrefix} Found pending referral for User ${userId} from ${pendingRef.referrerUserId}. Attempting link for TX ${signature}.`);
            const refereeWalletCheck = await queryDatabase('SELECT referred_by_user_id FROM wallets WHERE user_id = $1 FOR UPDATE', [userId], client);
            if (refereeWalletCheck.rowCount > 0 && !refereeWalletCheck.rows[0].referred_by_user_id) {
                if (await linkReferral(userId, pendingRef.referrerUserId, client)) {
                    console.log(`✅ ${logPrefix} Referral link successful: ${pendingRef.referrerUserId} -> ${userId} (TX ${signature}).`);
                    pendingReferrals.delete(userId);
                    try {
                        const refereeDisplayName = await getUserDisplayName(pendingRef.referrerUserId, userId);
                        safeSendMessage(pendingRef.referrerUserId, `🤝 Your referral ${escapeMarkdownV2(refereeDisplayName || `User ${userId.slice(-4)}`)} just made their first deposit and is now linked\\! Thanks\\!`, { parse_mode: 'MarkdownV2' });
                    } catch (notifyError) { console.warn(`${logPrefix} Failed to send referral link notification to ${pendingRef.referrerUserId}: ${notifyError.message}`); }
                } else {
                    console.warn(`⚠️ ${logPrefix} linkReferral function failed (maybe already linked concurrently by another process for TX ${signature}?). This attempt won't link.`);
                    pendingReferrals.delete(userId);
                }
            } else {
                console.log(`${logPrefix} User ${userId} was already referred by ${refereeWalletCheck.rows[0]?.referred_by_user_id || 'N/A'} or wallet not found during referral link attempt. Removing pending referral.`);
                pendingReferrals.delete(userId);
            }
        } else if (pendingRef) {
            pendingReferrals.delete(userId);
            console.log(`${logPrefix} Expired pending referral removed for user ${userId}.`);
        }

        await client.query('COMMIT');
        console.log(`✅ ${logPrefix} DB transaction committed successfully for TX ${signature}.`);

        await safeSendMessage(userId,
            `✅ *Deposit Confirmed\\!* ✅\n\n` +
            `Amount: *${escapeMarkdownV2(depositAmountSOL)} SOL*\n` +
            `New Balance: *${escapeMarkdownV2(newBalanceSOL)} SOL*\n` +
            `TX: \`${escapeMarkdownV2(signature)}\`\n\n` +
            `You can now use /start to play\\!`,
            { parse_mode: 'MarkdownV2' }
        );
        addProcessedDepositTx(signature);

    } catch (error) {
        console.error(`❌ ${logPrefix} CRITICAL ERROR processing deposit ${signature}: ${error.message}`, error.stack);
        if (client) {
            try { await client.query('ROLLBACK'); console.log(`ℹ️ ${logPrefix} Transaction rolled back due to error for TX ${signature}.`); }
            catch (rbErr) { console.error(`❌ ${logPrefix} Rollback failed for TX ${signature}:`, rbErr); }
        }
        if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 CRITICAL Error Processing Deposit TX \`${escapeMarkdownV2(signature)}\` Addr \`${escapeMarkdownV2(depositAddress)}\` User \`${escapeMarkdownV2(userId)}\`:\n${escapeMarkdownV2(String(error.message || error))}`);
        addProcessedDepositTx(signature);
    } finally {
        if (client) client.release();
    }
}

function startDepositSweeper() {
    let intervalMs = parseInt(process.env.SWEEP_INTERVAL_MS, 10);
    if (isNaN(intervalMs) || intervalMs <= 0) {
        console.warn("⚠️ [DepositSweeper] Sweeping is disabled (SWEEP_INTERVAL_MS not set or zero).");
        return;
    }
    if (intervalMs < 60000) {
        intervalMs = 60000;
        console.warn(`⚠️ [DepositSweeper] SWEEP_INTERVAL_MS too low, enforcing minimum ${intervalMs}ms.`);
    }

    if (sweepIntervalId) {
        clearInterval(sweepIntervalId);
        console.log('🔄 [DepositSweeper] Restarting deposit sweeper...');
    } else {
        console.log(`⚙️ [DepositSweeper] Starting Deposit Sweeper (Interval: ${intervalMs / 1000}s)...`);
    }

    const initialDelay = parseInt(process.env.INIT_DELAY_MS, 10) || 15000;
    console.log(`⚙️ [DepositSweeper] Scheduling first sweep run in ${initialDelay/1000}s...`);
    setTimeout(() => {
        try {
            console.log(`⚙️ [DepositSweeper] Executing first sweep run...`);
            sweepDepositAddresses().catch(err => console.error("❌ [Initial Sweep Run] Error:", err.message, err.stack));
            sweepIntervalId = setInterval(sweepDepositAddresses, intervalMs);
            if (sweepIntervalId.unref) sweepIntervalId.unref();
            console.log(`✅ [DepositSweeper] Recurring sweep interval set.`);
        } catch (initialRunError) {
            console.error("❌ [DepositSweeper] CRITICAL ERROR during initial sweep setup/run:", initialRunError);
            if (typeof notifyAdmin === "function") notifyAdmin(`🚨 CRITICAL ERROR setting up Deposit Sweeper interval: ${escapeMarkdownV2(String(initialRunError.message || initialRunError))}`).catch(()=>{});
        }
    }, initialDelay);
}

sweepDepositAddresses.isRunning = false;
async function sweepDepositAddresses() {
    const logPrefix = '[DepositSweeper]';
    if (sweepDepositAddresses.isRunning) {
        return;
    }
    sweepDepositAddresses.isRunning = true;
    console.log(`🧹 ${logPrefix} Starting sweep cycle...`);
    let addressesProcessed = 0, addressesSwept = 0, sweepErrors = 0, lowBalanceSkipped = 0;

    try {
        const batchSize = parseInt(process.env.SWEEP_BATCH_SIZE, 10) || 20;
        const mainBotPrivateKeyBase58 = process.env.MAIN_BOT_PRIVATE_KEY;
        if (!mainBotPrivateKeyBase58) {
            console.error(`❌ ${logPrefix} MAIN_BOT_PRIVATE_KEY not set. Cannot determine sweep target address.`);
            sweepDepositAddresses.isRunning = false;
            if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 [DepositSweeper] MAIN_BOT_PRIVATE_KEY not set. Sweeping disabled.`);
            return;
        }
        const sweepTargetKeypair = Keypair.fromSecretKey(bs58.decode(mainBotPrivateKeyBase58));
        const sweepTargetAddress = sweepTargetKeypair.publicKey;

        const delayBetweenAddressesMs = parseInt(process.env.SWEEP_ADDRESS_DELAY_MS, 10) || 750;
        const maxRetryAttemptsPerAddress = (parseInt(process.env.SWEEP_RETRY_ATTEMPTS, 10) || 1) + 1;
        const retryDelayBaseMs = parseInt(process.env.SWEEP_RETRY_DELAY_MS, 10) || 3000;
        const rentLamports = BigInt(await solanaConnection.getMinimumBalanceForRentExemption(0));
        const feeBufferLamports = SWEEP_FEE_BUFFER_LAMPORTS;
        const minimumLamportsToLeaveAfterSweep = rentLamports + feeBufferLamports;

        const addressesRes = await queryDatabase(
            `SELECT id, deposit_address, derivation_path
             FROM deposit_addresses WHERE status = 'used' ORDER BY created_at ASC LIMIT $1`,
            [batchSize]
        );

        if (addressesRes.rowCount === 0) {
            sweepDepositAddresses.isRunning = false;
            return;
        }
        console.log(`${logPrefix} Found ${addressesRes.rowCount} 'used' addresses potentially needing sweep.`);

        for (const row of addressesRes.rows) {
            addressesProcessed++;
            const addressId = row.id;
            const addressString = row.deposit_address;
            const derivationPath = row.derivation_path;
            const addrLogPrefix = `[Sweep Addr:${addressString.slice(0,6)}.. ID:${addressId}]`;

            if (!derivationPath || derivationPath === 'UNKNOWN_MIGRATED') {
                console.warn(`⚠️ ${addrLogPrefix} Missing or unknown derivation path. Cannot derive key to sweep. Marking as 'sweep_error'.`);
                await queryDatabase("UPDATE deposit_addresses SET status = 'sweep_error' WHERE id = $1 AND status = 'used'", [addressId]);
                if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 SWEEP FAILED (No Path) Addr \`${escapeMarkdownV2(addressString)}\` (ID: ${addressId}). Manual check needed.`);
                sweepErrors++;
                continue;
            }

            try {
                const depositKeypair = getKeypairFromPath(derivationPath);
                if (!depositKeypair) {
                    throw new Error("Failed to derive keypair from path.");
                }
                if (depositKeypair.publicKey.toBase58() !== addressString) {
                    console.error(`❌ CRITICAL ${addrLogPrefix} Derived public key mismatch! DB: ${addressString}, Derived: ${depositKeypair.publicKey.toBase58()}. Skipping sweep & marking error.`);
                    await queryDatabase("UPDATE deposit_addresses SET status = 'sweep_error_key_mismatch' WHERE id = $1 AND status = 'used'", [addressId]);
                    sweepErrors++;
                    if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 CRITICAL SWEEP KEY MISMATCH Addr \`${escapeMarkdownV2(addressString)}\` (ID: ${addressId}). Path: ${escapeMarkdownV2(derivationPath)}. Manual check needed.`);
                    continue;
                }

                const balanceLamports = BigInt(await solanaConnection.getBalance(depositKeypair.publicKey, DEPOSIT_CONFIRMATION_LEVEL));

                if (balanceLamports <= minimumLamportsToLeaveAfterSweep) {
                    lowBalanceSkipped++;
                    await queryDatabase("UPDATE deposit_addresses SET status = 'swept_low_balance' WHERE id = $1 AND status = 'used'", [addressId]);
                    continue;
                }

                const amountToSweep = balanceLamports - minimumLamportsToLeaveAfterSweep;
                console.log(`${addrLogPrefix} Balance: ${formatSol(balanceLamports)} SOL. Attempting to sweep ${formatSol(amountToSweep)} SOL to ${sweepTargetAddress.toBase58()}...`);

                let sweepSuccessThisAddress = false;
                let sweepTxSignature = null;
                let lastSweepError = null;

                for (let attempt = 1; attempt <= maxRetryAttemptsPerAddress; attempt++) {
                    try {
                        const { blockhash, lastValidBlockHeight } = await solanaConnection.getLatestBlockhash(DEPOSIT_CONFIRMATION_LEVEL);
                        const transaction = new Transaction({ recentBlockhash: blockhash, feePayer: depositKeypair.publicKey })
                            .add( SystemProgram.transfer({
                                fromPubkey: depositKeypair.publicKey,
                                toPubkey: sweepTargetAddress,
                                lamports: amountToSweep
                            }));
                        
                        sweepTxSignature = await sendAndConfirmTransaction(
                            solanaConnection, transaction, [depositKeypair],
                            { commitment: DEPOSIT_CONFIRMATION_LEVEL, skipPreflight: false, preflightCommitment: DEPOSIT_CONFIRMATION_LEVEL, lastValidBlockHeight }
                        );
                        sweepSuccessThisAddress = true;
                        console.log(`✅ ${addrLogPrefix} Sweep successful on attempt ${attempt}. TX: ${sweepTxSignature}`);
                        break; 
                    } catch (error) {
                        lastSweepError = error;
                        console.warn(`⚠️ ${addrLogPrefix} Sweep Attempt ${attempt}/${maxRetryAttemptsPerAddress} FAILED: ${error.message}`);
                        if (isRetryableSolanaError(error) && attempt < maxRetryAttemptsPerAddress) {
                            const jitterDelay = retryDelayBaseMs * Math.pow(1.5, attempt - 1) * (0.8 + Math.random() * 0.4);
                            console.log(`⏳ ${addrLogPrefix} Retrying sweep in ~${Math.round(jitterDelay / 1000)}s...`);
                            await sleep(jitterDelay);
                        } else {
                            console.error(`❌ ${addrLogPrefix} Sweep failed permanently for this address after ${attempt} attempts.`);
                            break; 
                        }
                    }
                }

                if (sweepSuccessThisAddress && sweepTxSignature) {
                    await queryDatabase("UPDATE deposit_addresses SET status = 'swept' WHERE id = $1 AND status = 'used'", [addressId]);
                    addressesSwept++;
                } else {
                    sweepErrors++;
                    const finalErrorMsg = lastSweepError?.message || "Unknown permanent sweep failure";
                    console.error(`❌ ${addrLogPrefix} Final Sweep outcome: FAILED for address ${addressString}. Last Error: ${finalErrorMsg}`);
                    await queryDatabase("UPDATE deposit_addresses SET status = 'sweep_error' WHERE id = $1 AND status = 'used'", [addressId]);
                    if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 SWEEP FAILED (Permanent) Addr \`${escapeMarkdownV2(addressString)}\` (ID: ${addressId}): ${escapeMarkdownV2(finalErrorMsg)}. Manual check needed.`);
                }
            } catch (processAddrError) {
                sweepErrors++;
                console.error(`❌ ${addrLogPrefix} Error processing address ${addressString} for sweep: ${processAddrError.message}`, processAddrError.stack);
                await queryDatabase("UPDATE deposit_addresses SET status = 'sweep_error_processing' WHERE id = $1 AND status = 'used'", [addressId]);
                if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 SWEEP FAILED (Processing Error) Addr \`${escapeMarkdownV2(addressString)}\` (ID: ${addressId}): ${escapeMarkdownV2(String(processAddrError.message || processAddrError))}. Manual check needed.`);
            }
            if (delayBetweenAddressesMs > 0 && addressesProcessed < addressesRes.rowCount) {
                await sleep(delayBetweenAddressesMs);
            }
        }
        console.log(`🧹 ${logPrefix} Sweep cycle finished. Processed: ${addressesProcessed}, Swept OK: ${addressesSwept}, Low Balance Skipped: ${lowBalanceSkipped}, Errors: ${sweepErrors}.`);
    } catch (error) {
        console.error(`❌ ${logPrefix} CRITICAL ERROR in main sweep cycle: ${error.message}`, error.stack);
        if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 CRITICAL Error in Deposit Sweeper main loop: ${escapeMarkdownV2(String(error.message || error))}.`);
    } finally {
        sweepDepositAddresses.isRunning = false;
    }
}

// --- Background Task: Leaderboard Manager (Full Definition) ---
async function updateLeaderboardsCycle() {
    const logPrefix = '[LeaderboardManager Cycle]';
    console.log(`🏆 ${logPrefix} Starting leaderboard update cycle...`);
    
    const periods = ['daily', 'weekly', 'all_time'];
    const scoreTypes = ['total_wagered', 'total_profit']; // Define which scores to track
    const gamesToTrack = Object.keys(GAME_CONFIG); // Track for all configured games

    const now = new Date();
    const todayISO = now.toISOString().split('T')[0]; // YYYY-MM-DD for daily

    const getWeekNumber = (d) => {
        d = new Date(Date.UTC(d.getFullYear(), d.getMonth(), d.getDate()));
        d.setUTCDate(d.getUTCDate() + 4 - (d.getUTCDay()||7));
        const yearStart = new Date(Date.UTC(d.getUTCFullYear(),0,1));
        const weekNo = Math.ceil((((d - yearStart) / 86400000) + 1)/7);
        return `${d.getUTCFullYear()}-W${String(weekNo).padStart(2, '0')}`;
    }
    const currentWeekIdentifier = getWeekNumber(now);

    let client = null;
    try {
        client = await pool.connect();

        for (const gameKey of gamesToTrack) {
            for (const period of periods) {
                let startDate;
                let periodIdentifier;

                switch (period) {
                    case 'daily':
                        startDate = new Date(todayISO); // Start of today
                        periodIdentifier = todayISO;
                        break;
                    case 'weekly':
                        const dayOfWeek = now.getDay(); // Sunday - 0, Monday - 1
                        const diff = now.getDate() - dayOfWeek + (dayOfWeek === 0 ? -6 : 1); // Adjust to Monday
                        startDate = new Date(now.setDate(diff));
                        startDate.setHours(0,0,0,0); // Start of Monday
                        periodIdentifier = currentWeekIdentifier;
                        break;
                    case 'all_time':
                        startDate = new Date(0); // Unix epoch start
                        periodIdentifier = 'all';
                        break;
                    default:
                        console.warn(`${logPrefix} Unknown period type: ${period}. Skipping.`);
                        continue;
                }

                for (const scoreType of scoreTypes) {
                    console.log(`${logPrefix} Processing: Game=${gameKey}, Period=${period} (${periodIdentifier}), ScoreType=${scoreType}...`);
                    let scoreDataQuery = '';
                    let queryParams = [gameKey, startDate.toISOString()];
                    const scoreColumnAlias = 'calculated_score_value';

                    if (scoreType === 'total_wagered') {
                        scoreDataQuery = `
                            SELECT user_id, SUM(wager_amount_lamports) as ${scoreColumnAlias}
                            FROM bets
                            WHERE game_type = $1 AND status LIKE 'completed_%' AND created_at >= $2
                            GROUP BY user_id
                            HAVING SUM(wager_amount_lamports) > 0;
                        `;
                    } else if (scoreType === 'total_profit') {
                        scoreDataQuery = `
                            SELECT user_id, SUM(COALESCE(payout_amount_lamports, 0) - wager_amount_lamports) as ${scoreColumnAlias}
                            FROM bets
                            WHERE game_type = $1 AND status LIKE 'completed_%' AND created_at >= $2
                            GROUP BY user_id; 
                            -- Not filtering by HAVING SUM > 0, as even net loss can be a "score" for some leaderboards
                        `;
                    } else {
                        console.warn(`${logPrefix} Unknown score type: ${scoreType}. Skipping.`);
                        continue;
                    }

                    await client.query('BEGIN'); // Transaction for each specific leaderboard update
                    try {
                        const scoreDataRes = await client.query(scoreDataQuery, queryParams);
                        if (scoreDataRes.rowCount > 0) {
                            for (const row of scoreDataRes.rows) {
                                // Fetch display name (can be cached or simplified)
                                const userDetails = await getUserWalletDetails(row.user_id, client); // Ensures transactional read
                                const displayName = userDetails?.referral_code || `User...${row.user_id.slice(-4)}`; // Example display name

                                await client.query(`
                                    INSERT INTO game_leaderboards (game_key, user_id, score_type, period_type, period_identifier, score, player_display_name, updated_at)
                                    VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
                                    ON CONFLICT (game_key, user_id, score_type, period_type, period_identifier)
                                    DO UPDATE SET score = EXCLUDED.score, player_display_name = EXCLUDED.player_display_name, updated_at = NOW()
                                    WHERE game_leaderboards.score <> EXCLUDED.score OR game_leaderboards.player_display_name <> EXCLUDED.player_display_name;
                                `, [gameKey, row.user_id, scoreType, period, periodIdentifier, BigInt(row[scoreColumnAlias]), displayName]);
                            }
                            console.log(`${logPrefix} Updated/checked ${scoreDataRes.rowCount} entries for ${gameKey}/${period}/${scoreType}.`);
                        } else {
                            console.log(`${logPrefix} No new data to update for ${gameKey}/${period}/${scoreType}.`);
                        }
                        await client.query('COMMIT');
                    } catch (innerError) {
                        console.error(`❌ ${logPrefix} Error updating specific leaderboard ${gameKey}/${period}/${scoreType}: ${innerError.message}`);
                        await client.query('ROLLBACK');
                    }
                } // end scoreType loop
            } // end period loop
        } // end gameKey loop
    } catch (error) {
        console.error(`❌ ${logPrefix} Major error in leaderboard update cycle: ${error.message}`, error.stack);
        if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 Error during leaderboard update cycle: ${escapeMarkdownV2(String(error.message || error))}`);
    } finally {
        if (client) client.release();
        console.log(`🏆 ${logPrefix} Leaderboard update cycle finished.`);
    }
}

function startLeaderboardManager() {
    const logPrefix = '[LeaderboardManager Start]';
    console.log(`⚙️ ${logPrefix} Initializing Leaderboard Manager...`);
    const intervalMs = parseInt(process.env.LEADERBOARD_UPDATE_INTERVAL_MS, 10);

    if (isNaN(intervalMs) || intervalMs <= 0) {
        console.warn(`⚠️ ${logPrefix} Leaderboard updates are disabled (LEADERBOARD_UPDATE_INTERVAL_MS not set or invalid).`);
        return;
    }

    if (leaderboardManagerIntervalId) {
        clearInterval(leaderboardManagerIntervalId);
        console.log(`🔄 ${logPrefix} Restarting leaderboard manager...`);
    }

    const initialDelayMs = (parseInt(process.env.INIT_DELAY_MS, 10) || 5000) + 7000; // Delay a bit more
    console.log(`⚙️ ${logPrefix} Scheduling first leaderboard update run in ${initialDelayMs / 1000}s...`);

    setTimeout(() => {
        updateLeaderboardsCycle().catch(err => console.error("❌ [Initial Leaderboard Update Run] Error:", err.message, err.stack));
        leaderboardManagerIntervalId = setInterval(updateLeaderboardsCycle, intervalMs);
        if (leaderboardManagerIntervalId.unref) leaderboardManagerIntervalId.unref();
        console.log(`✅ ${logPrefix} Leaderboard updates scheduled every ${intervalMs / (1000 * 60)} minutes.`);
    }, initialDelayMs);
}


// --- Payout Processing --- (Identical to previous correct version)
async function addPayoutJob(jobData) {
    const jobType = jobData?.type || 'unknown_job';
    const jobId = jobData?.withdrawalId || jobData?.payoutId || 'N/A_ID';
    const logPrefix = `[AddPayoutJob Type:${jobType} ID:${jobId}]`;
    console.log(`⚙️ ${logPrefix} Adding job to payout queue for user ${jobData.userId || 'N/A'}.`);

    return payoutProcessorQueue.add(async () => {
        let attempts = 0;
        const maxAttempts = (parseInt(process.env.PAYOUT_JOB_RETRIES, 10) || 3) + 1;
        const baseDelayMs = parseInt(process.env.PAYOUT_JOB_RETRY_DELAY_MS, 10) || 5000;

        while(attempts < maxAttempts) {
            attempts++;
            const attemptLogPrefix = `[PayoutJob Attempt:${attempts}/${maxAttempts} Type:${jobType} ID:${jobId}]`;
            try {
                console.log(`${attemptLogPrefix} Starting processing...`);
                if (jobData.type === 'payout_withdrawal') {
                    await handleWithdrawalPayoutJob(jobData.withdrawalId);
                } else if (jobData.type === 'payout_referral') {
                    await handleReferralPayoutJob(jobData.payoutId);
                } else {
                    throw new Error(`Unknown payout job type: ${jobData.type}`);
                }
                console.log(`✅ ${attemptLogPrefix} Job completed successfully.`);
                return;
            } catch(error) {
                console.warn(`⚠️ ${attemptLogPrefix} Attempt failed: ${error.message}`);
                const isRetryableFlag = error.isRetryable === true;

                if (!isRetryableFlag || attempts >= maxAttempts) {
                    console.error(`❌ ${attemptLogPrefix} Job failed permanently after ${attempts} attempts. Error: ${error.message}`);
                    return;
                }

                const delayWithJitter = baseDelayMs * Math.pow(2, attempts - 1) * (0.8 + Math.random() * 0.4);
                console.log(`⏳ ${attemptLogPrefix} Retrying in ~${Math.round(delayWithJitter / 1000)}s...`);
                await sleep(delayWithJitter);
            }
        }
    }).catch(queueError => {
        console.error(`❌ ${logPrefix} CRITICAL Error processing job in Payout Queue: ${queueError.message}`);
        if (typeof notifyAdmin === "function") notifyAdmin(`🚨 CRITICAL Payout Queue Error. Type: ${jobType}, ID: ${jobId}. Error: ${escapeMarkdownV2(String(queueError.message || queueError))}`).catch(()=>{});
    });
}

async function handleWithdrawalPayoutJob(withdrawalId) {
    const logPrefix = `[WithdrawJob ID:${withdrawalId}]`;
    console.log(`⚙️ ${logPrefix} Processing withdrawal job...`);
    let clientForRefund = null;
    let sendSolResult = { success: false, error: "Send SOL not initiated", isRetryable: false };

    const details = await getWithdrawalDetails(withdrawalId);
    if (!details) {
        console.error(`❌ ${logPrefix} Withdrawal details not found for ID ${withdrawalId}. Cannot process.`);
        const error = new Error(`Withdrawal details not found for ID ${withdrawalId}. Job cannot proceed.`);
        error.isRetryable = false; throw error;
    }

    if (details.status === 'completed' || details.status === 'failed') {
        console.log(`ℹ️ ${logPrefix} Job skipped, withdrawal ID ${withdrawalId} already in terminal state '${details.status}'.`);
        return;
    }

    const userId = details.user_id;
    const recipient = details.recipient_address;
    const amountToActuallySend = details.final_send_amount_lamports;
    const feeApplied = details.fee_lamports;
    const totalAmountDebitedFromBalance = amountToActuallySend + feeApplied;

    try {
        const statusUpdatedToProcessing = await updateWithdrawalStatus(withdrawalId, 'processing');
        if (!statusUpdatedToProcessing) {
            const currentDetailsAfterAttempt = await getWithdrawalDetails(withdrawalId);
            if (currentDetailsAfterAttempt && (currentDetailsAfterAttempt.status === 'completed' || currentDetailsAfterAttempt.status === 'failed')) {
                console.warn(`⚠️ ${logPrefix} Failed to update status to 'processing' for ID ${withdrawalId}, but it's already '${currentDetailsAfterAttempt.status}'. Exiting job as no-op.`);
                return;
            }
            const error = new Error(`Failed to update withdrawal ${withdrawalId} status to 'processing' and it's not yet terminal. Current status: ${currentDetailsAfterAttempt?.status}`);
            error.isRetryable = true; throw error;
        }
        console.log(`${logPrefix} Status updated to 'processing'. Attempting to send ${formatSol(amountToActuallySend)} SOL.`);

        sendSolResult = await sendSol(recipient, amountToActuallySend, 'withdrawal');

        if (sendSolResult.success && sendSolResult.signature) {
            console.log(`✅ ${logPrefix} sendSol successful for withdrawal ID ${withdrawalId}. TX: ${sendSolResult.signature}. Marking 'completed'.`);
            await updateWithdrawalStatus(withdrawalId, 'completed', null, sendSolResult.signature);
            await safeSendMessage(userId,
                `✅ *Withdrawal Completed\\!* ✅\n\n` +
                `Amount: *${escapeMarkdownV2(formatSol(amountToActuallySend))} SOL* sent to \`${escapeMarkdownV2(recipient)}\`\\.\n` +
                `TX: \`${escapeMarkdownV2(sendSolResult.signature)}\``,
                { parse_mode: 'MarkdownV2' }
            );
            return;
        } else {
            const sendErrorMsg = sendSolResult.error || 'Unknown sendSol failure.';
            console.error(`❌ ${logPrefix} sendSol FAILED for withdrawal ID ${withdrawalId}. Reason: ${sendErrorMsg}. Attempting to mark 'failed' and refund user.`);
            await updateWithdrawalStatus(withdrawalId, 'failed', null, null, sendErrorMsg.substring(0, 500));

            clientForRefund = await pool.connect();
            await clientForRefund.query('BEGIN');
            const refundUpdateResult = await updateUserBalanceAndLedger(
                clientForRefund, userId, totalAmountDebitedFromBalance,
                'withdrawal_refund', { withdrawalId }, `Refund for failed withdrawal ID ${withdrawalId}`
            );
            if (refundUpdateResult.success) {
                await clientForRefund.query('COMMIT');
                console.log(`✅ ${logPrefix} Successfully refunded ${formatSol(totalAmountDebitedFromBalance)} SOL to user ${userId} for failed withdrawal ${withdrawalId}.`);
                await safeSendMessage(userId, `⚠️ Your withdrawal of ${escapeMarkdownV2(formatSol(amountToActuallySend))} SOL failed \\(Reason: ${escapeMarkdownV2(sendErrorMsg)}\\)\\. The amount ${escapeMarkdownV2(formatSol(totalAmountDebitedFromBalance))} SOL \\(including fee\\) has been refunded to your internal balance\\.`, {parse_mode: 'MarkdownV2'});
            } else {
                await clientForRefund.query('ROLLBACK');
                console.error(`❌ CRITICAL ${logPrefix} FAILED TO REFUND USER ${userId} for withdrawal ${withdrawalId}. Amount: ${formatSol(totalAmountDebitedFromBalance)}. Refund DB Error: ${refundUpdateResult.error}`);
                if (typeof notifyAdmin === "function") await notifyAdmin(`🚨🚨 CRITICAL: FAILED REFUND User ${userId}/WD ${withdrawalId}/Amt ${formatSol(totalAmountDebitedFromBalance)}. SendErr: ${escapeMarkdownV2(sendErrorMsg)} RefundErr: ${escapeMarkdownV2(refundUpdateResult.error || 'Unknown DB error')}`);
            }
            clientForRefund.release(); clientForRefund = null;

            const errorToThrowForRetry = new Error(sendErrorMsg);
            errorToThrowForRetry.isRetryable = sendSolResult.isRetryable === true;
            throw errorToThrowForRetry;
        }
    } catch (jobError) {
        console.error(`❌ ${logPrefix} Error during withdrawal job ID ${withdrawalId}: ${jobError.message}`, jobError.stack);
        if (jobError.isRetryable === undefined) {
            jobError.isRetryable = isRetryableSolanaError(jobError) || sendSolResult.isRetryable === true;
        }
        const currentDetailsAfterJobError = await getWithdrawalDetails(withdrawalId);
        if (currentDetailsAfterJobError && currentDetailsAfterJobError.status !== 'completed' && currentDetailsAfterJobError.status !== 'failed') {
            await updateWithdrawalStatus(withdrawalId, 'failed', null, null, `Job error: ${jobError.message}`.substring(0,500));
        }
        throw jobError;
    } finally {
        if (clientForRefund) clientForRefund.release();
    }
}

async function handleReferralPayoutJob(payoutId) {
    const logPrefix = `[ReferralJob ID:${payoutId}]`;
    console.log(`⚙️ ${logPrefix} Processing referral payout job...`);
    let sendSolResult = { success: false, error: "Send SOL not initiated for referral", isRetryable: false };

    const details = await getReferralPayoutDetails(payoutId);
    if (!details) {
        console.error(`❌ ${logPrefix} Referral payout details not found for ID ${payoutId}.`);
        const error = new Error(`Referral payout details not found for ID ${payoutId}. Cannot proceed.`);
        error.isRetryable = false; throw error;
    }
    if (details.status === 'paid' || details.status === 'failed') {
        console.log(`ℹ️ ${logPrefix} Job skipped, referral payout ID ${payoutId} already in terminal state '${details.status}'.`); return;
    }

    const referrerUserId = details.referrer_user_id;
    const amountToPay = details.payout_amount_lamports;
    const amountToPaySOL = formatSol(amountToPay);

    try {
        const referrerWalletDetails = await getUserWalletDetails(referrerUserId);
        if (!referrerWalletDetails?.external_withdrawal_address) {
            const noWalletMsg = `Referrer ${referrerUserId} has no linked external withdrawal address for referral payout ID ${payoutId}.`;
            console.error(`❌ ${logPrefix} ${noWalletMsg}`);
            await updateReferralPayoutStatus(payoutId, 'failed', null, null, noWalletMsg.substring(0,500));
            const error = new Error(noWalletMsg); error.isRetryable = false; throw error;
        }
        const recipientAddress = referrerWalletDetails.external_withdrawal_address;

        await updateReferralPayoutStatus(payoutId, 'processing');
        console.log(`${logPrefix} Status updated to 'processing'. Attempting to send ${amountToPaySOL} SOL to ${recipientAddress}.`);

        sendSolResult = await sendSol(recipientAddress, amountToPay, 'referral');

        if (sendSolResult.success && sendSolResult.signature) {
            console.log(`✅ ${logPrefix} sendSol successful for referral payout ID ${payoutId}. TX: ${sendSolResult.signature}. Marking 'paid'.`);
            await updateReferralPayoutStatus(payoutId, 'paid', null, sendSolResult.signature);
            const rewardTypeMsg = details.payout_type === 'initial_bet'
                ? `Initial Referral Bonus (from User ${details.referee_user_id})`
                : `Milestone Bonus (from User ${details.referee_user_id} reaching ${formatSol(details.milestone_reached_lamports || 0)} SOL wagered)`;
            await safeSendMessage(referrerUserId,
                `💰 *${escapeMarkdownV2(rewardTypeMsg)} Paid\\!* 💰\n\n` +
                `Amount: *${escapeMarkdownV2(amountToPaySOL)} SOL* sent to your linked wallet \`${escapeMarkdownV2(recipientAddress)}\`\\.\n` +
                `TX: \`${escapeMarkdownV2(sendSolResult.signature)}\``,
                { parse_mode: 'MarkdownV2' }
            );
            return;
        } else {
            const sendErrorMsg = sendSolResult.error || 'Unknown sendSol failure for referral payout.';
            console.error(`❌ ${logPrefix} sendSol FAILED for referral payout ID ${payoutId}. Reason: ${sendErrorMsg}.`);
            await updateReferralPayoutStatus(payoutId, 'failed', null, null, sendErrorMsg.substring(0, 500));
            await safeSendMessage(referrerUserId, `❌ Your Referral Reward of ${escapeMarkdownV2(amountToPaySOL)} SOL failed to send \\(Reason: ${escapeMarkdownV2(sendErrorMsg)}\\)\\. Please contact support if this issue persists\\.`, {parse_mode: 'MarkdownV2'});
            if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 REFERRAL PAYOUT FAILED (Referrer ${referrerUserId}, Payout ID ${payoutId}, Amount ${amountToPaySOL} SOL): ${escapeMarkdownV2(sendErrorMsg)}`);
            
            const errorToThrowForRetry = new Error(sendErrorMsg);
            errorToThrowForRetry.isRetryable = sendSolResult.isRetryable === true;
            throw errorToThrowForRetry;
        }
    } catch (jobError) {
        console.error(`❌ ${logPrefix} Error during referral payout job ID ${payoutId}: ${jobError.message}`, jobError.stack);
        if (jobError.isRetryable === undefined) {
            jobError.isRetryable = isRetryableSolanaError(jobError) || sendSolResult.isRetryable === true;
        }
        const currentDetailsAfterJobError = await getReferralPayoutDetails(payoutId);
        if (currentDetailsAfterJobError && currentDetailsAfterJobError.status !== 'paid' && currentDetailsAfterJobError.status !== 'failed') {
            await updateReferralPayoutStatus(payoutId, 'failed', null, null, `Job error: ${jobError.message}`.substring(0,500));
        }
        throw jobError;
    }
}

async function setupTelegramConnection() {
    console.log('⚙️ [Startup] Configuring Telegram connection (Webhook/Polling)...');
    let webhookUrlBase = process.env.RAILWAY_PUBLIC_DOMAIN ? `https://${process.env.RAILWAY_PUBLIC_DOMAIN}` : process.env.WEBHOOK_URL;

    if (webhookUrlBase && process.env.BOT_TOKEN) {
        const webhookPath = `/telegram/${process.env.BOT_TOKEN}`;
        const fullWebhookUrl = `${webhookUrlBase.replace(/\/$/, '')}${webhookPath}`;
        console.log(`ℹ️ [Startup] Attempting to set webhook: ${fullWebhookUrl}`);

        try {
            if (bot.isPolling()) {
                console.log("ℹ️ [Startup] Stopping existing polling before setting webhook...");
                await bot.stopPolling({ cancel: true }).catch(e => console.warn("⚠️ [Startup] Error stopping polling:", e.message));
            }
            await bot.deleteWebHook({ drop_pending_updates: true }).catch((e) => console.warn(`⚠️ [Startup] Non-critical error deleting old webhook: ${e.message}`));

            const webhookMaxConn = parseInt(process.env.WEBHOOK_MAX_CONN, 10) || 40;
            const setResult = await bot.setWebHook(fullWebhookUrl, {
                max_connections: webhookMaxConn,
                allowed_updates: ['message', 'callback_query']
            });

            if (!setResult) {
                throw new Error("setWebHook API call returned false or timed out, indicating failure.");
            }

            console.log(`✅ [Startup] Telegram Webhook set successfully to ${fullWebhookUrl} (Max Connections: ${webhookMaxConn})`);
            bot.options.polling = false;
            return true;
        } catch (error) {
            console.error(`❌ [Startup] Failed to set Telegram Webhook: ${error.message}. Falling back to polling.`, error.response?.body ? JSON.stringify(error.response.body) : '');
            if (typeof notifyAdmin === "function") await notifyAdmin(`⚠️ WARNING: Webhook setup failed for ${escapeMarkdownV2(fullWebhookUrl)}\nError: ${escapeMarkdownV2(String(error.message || error))}\nFalling back to polling.`).catch(()=>{});
            bot.options.polling = true;
            return false;
        }
    } else {
        console.log('ℹ️ [Startup] Webhook URL not configured (RAILWAY_PUBLIC_DOMAIN or WEBHOOK_URL) or BOT_TOKEN missing. Using Polling mode.');
        bot.options.polling = true;
        return false;
    }
}

async function startPollingFallback() {
    if (!bot.options.polling) {
        return;
    }
    console.log('⚙️ [Startup] Starting Polling for Telegram updates...');
    try {
        await bot.deleteWebHook({ drop_pending_updates: true }).catch(() => {});
        await bot.startPolling({
            timeout: 10, // Long polling timeout in seconds
            allowed_updates: JSON.stringify(['message', 'callback_query']) // Must be JSON string array
        });
        console.log('✅ [Startup] Telegram Polling started successfully.');
    } catch (err) {
        console.error(`❌ CRITICAL: Telegram Polling failed to start: ${err.message}`, err.stack);
        if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 CRITICAL POLLING FAILED TO START: ${escapeMarkdownV2(String(err.message || err))}. Bot cannot receive updates. Exiting.`).catch(()=>{});
        process.exit(3);
    }
}

function setupExpressServer() {
    console.log('⚙️ [Startup] Setting up Express server...');
    const port = process.env.PORT || 3000;

    app.get('/', (req, res) => {
        res.status(200).send(`Solana Gambles Bot v${BOT_VERSION} is running.`);
    });

    app.get('/health', (req, res) => {
        const status = isFullyInitialized ? 'OK' : 'INITIALIZING';
        const httpStatus = isFullyInitialized ? 200 : 503;
        res.status(httpStatus).json({ status: status, version: BOT_VERSION, timestamp: new Date().toISOString() });
    });

    if (!bot.options.polling && process.env.BOT_TOKEN) {
        const webhookPath = `/telegram/${process.env.BOT_TOKEN}`;
        console.log(`⚙️ [Startup] Configuring Express webhook endpoint at ${webhookPath}`);
        app.post(webhookPath, (req, res) => {
            try {
                if (req.body.message) {
                    messageQueue.add(() => handleMessage(req.body.message)).catch(e => console.error(`[MsgQueueErr Webhook]: ${e.message}`));
                } else if (req.body.callback_query) {
                    callbackQueue.add(() => handleCallbackQuery(req.body.callback_query)).catch(e => {
                        console.error(`[CBQueueErr Webhook]: ${e.message}`);
                        bot.answerCallbackQuery(req.body.callback_query.id, {text: "⚠️ Error processing request."}).catch(()=>{});
                    });
                } else {
                    console.warn("[Webhook POST] Received unknown update type:", req.body);
                }
            } catch (processError) {
                console.error(`❌ [Webhook Handler] Error processing update: ${processError.message}`, processError.stack);
            }
            res.sendStatus(200);
        });
    } else {
         console.log("ℹ️ [Startup] Skipping Express webhook endpoint setup (Polling mode or no BOT_TOKEN).");
    }

    server = app.listen(port, '0.0.0.0', () => {
        console.log(`✅ [Startup] Express server listening on 0.0.0.0:${port}`);
    });

    server.on('error', async (error) => {
        console.error(`❌ Express Server Error: ${error.message}`, error.stack);
        if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 CRITICAL Express Server Error: ${escapeMarkdownV2(String(error.message || error))}. Bot may not function. Restart advised.`).catch(()=>{});
        if (error.code === 'EADDRINUSE' && !isShuttingDown) {
            console.error("Address in use, shutting down...");
            await shutdown('EADDRINUSE_SERVER_ERROR').catch(() => process.exit(4));
        }
    });
}

let isShuttingDown = false;
async function shutdown(signal) {
    if (isShuttingDown) {
        console.warn("🚦 Shutdown already in progress, ignoring duplicate signal:", signal);
        return;
    }
    isShuttingDown = true;
    console.warn(`\n🚦 Received signal: ${signal}. Initiating graceful shutdown... (PID: ${process.pid})`);
    isFullyInitialized = false;

    if (typeof notifyAdmin === "function") {
        await notifyAdmin(`ℹ️ Bot instance v${BOT_VERSION} shutting down (Signal: ${escapeMarkdownV2(String(signal))})...`).catch(()=>{});
    }

    console.log("🚦 [Shutdown] Stopping Telegram updates...");
    if (bot?.isPolling?.()) {
        await bot.stopPolling({ cancel: true }).then(() => console.log("✅ [Shutdown] Polling stopped.")).catch(e => console.error("❌ [Shutdown] Error stopping polling:", e.message));
    } else if (bot && !bot.options.polling) {
        console.log("ℹ️ [Shutdown] In webhook mode or polling was not active.");
    } else {
        console.log("ℹ️ [Shutdown] Telegram bot instance not available or polling already off.");
    }

    console.log("🚦 [Shutdown] Closing HTTP server...");
    if (server) {
        await new Promise(resolve => server.close(err => {
            if(err) console.error("❌ [Shutdown] Error closing HTTP server:", err);
            else console.log("✅ [Shutdown] HTTP server closed.");
            resolve();
        }));
    } else { console.log("ℹ️ [Shutdown] HTTP server was not running or already closed."); }

    console.log("🚦 [Shutdown] Stopping background intervals (Monitor, Sweeper, Leaderboards)...");
    if (depositMonitorIntervalId) clearInterval(depositMonitorIntervalId); depositMonitorIntervalId = null;
    if (sweepIntervalId) clearInterval(sweepIntervalId); sweepIntervalId = null;
    if (leaderboardManagerIntervalId) clearInterval(leaderboardManagerIntervalId); leaderboardManagerIntervalId = null;
    console.log("✅ [Shutdown] Background intervals cleared.");

    console.log("🚦 [Shutdown] Waiting for processing queues to idle...");
    const queueShutdownTimeout = parseInt(process.env.SHUTDOWN_QUEUE_TIMEOUT_MS, 10) || 20000;
    const allQueues = [messageQueue, callbackQueue, payoutProcessorQueue, depositProcessorQueue, telegramSendQueue];
    try {
        await Promise.race([
            Promise.all(allQueues.map(q => q.onIdle())),
            sleep(queueShutdownTimeout).then(() => Promise.reject(new Error(`Queue idle timeout (${queueShutdownTimeout}ms) exceeded during shutdown.`)))
        ]);
        console.log("✅ [Shutdown] All processing queues are idle.");
    } catch (queueError) {
        console.warn(`⚠️ [Shutdown] Queues did not idle within timeout or errored: ${queueError.message}`);
        allQueues.forEach(q => q.pause());
    }

    console.log("🚦 [Shutdown] Closing Database pool...");
    if (pool) {
        await pool.end()
            .then(() => console.log("✅ [Shutdown] Database pool closed."))
            .catch(e => console.error("❌ [Shutdown] Error closing Database pool:", e.message));
    } else {
        console.log("ℹ️ [Shutdown] Database pool not available or already closed.");
    }

    console.log(`🏁 [Shutdown] Graceful shutdown complete (Signal: ${signal}). Exiting.`);
    const exitCode = (typeof signal === 'number' && signal !==0) ? signal : (signal === 'SIGINT' || signal === 'SIGTERM' ? 0 : 1);
    process.exit(exitCode);
}

function setupTelegramListeners() {
    console.log("⚙️ [Listeners] Setting up Telegram event listeners...");
    if (bot.listeners('message').length > 0 || bot.listeners('callback_query').length > 0) {
        console.warn("⚠️ [Listeners] Listeners already attached. Clearing them first.");
        bot.removeAllListeners();
    }

    bot.on('message', (msg) => {
        const adminUserIdsArray = (process.env.ADMIN_USER_IDS || '').split(',').map(id => id.trim());
        if (!isFullyInitialized && !(msg.from && adminUserIdsArray.includes(String(msg.from.id)) && msg.text?.startsWith('/admin'))) {
            return;
        }
        messageQueue.add(() => handleMessage(msg)).catch(e => console.error(`[MsgQueueErr Listener Processing]: ${e.message}`));
    });

    bot.on('callback_query', (cb) => {
        if (!isFullyInitialized) {
            bot.answerCallbackQuery(cb.id, {text: "🛠️ Bot is still starting up. Please wait a moment and try again.", show_alert: true}).catch(()=>{});
            return;
        }
        callbackQueue.add(() => handleCallbackQuery(cb)).catch(e => {
            console.error(`[CBQueueErr Listener Processing]: ${e.message}`);
            bot.answerCallbackQuery(cb.id, { text: "⚠️ Error processing your request." }).catch(()=>{});
        });
    });

    bot.on('polling_error', (error) => {
        console.error(`❌ TG Polling Error: Code ${error.code || 'N/A'} | ${error.message}`, error.stack);
        if (String(error.message).includes('409') || String(error.code).includes('EFATAL') || String(error.message).toLowerCase().includes('conflict')) {
            if (typeof notifyAdmin === "function") notifyAdmin(`🚨 POLLING CONFLICT (409) or EFATAL detected. Another instance running? Shutting down. Error: ${escapeMarkdownV2(String(error.message || error))}`).catch(()=>{});
            if (!isShuttingDown) shutdown('POLLING_FATAL_ERROR').catch(() => process.exit(1));
        }
    });
    bot.on('webhook_error', (error) => {
        console.error(`❌ TG Webhook Error: Code ${error.code || 'N/A'} | ${error.message}`, error.stack);
        if (typeof notifyAdmin === "function") notifyAdmin(`🚨 Telegram Webhook Error: ${error.code || 'N/A'} - ${escapeMarkdownV2(String(error.message || error))}`).catch(()=>{});
    });
    bot.on('error', (error) => {
        console.error('❌ General node-telegram-bot-api Library Error:', error);
        if (typeof notifyAdmin === "function") notifyAdmin(`🚨 General TG Bot Library Error:\n${escapeMarkdownV2(String(error.message || error))}`).catch(()=>{});
    });
    console.log("✅ [Listeners] Telegram event listeners are ready.");
}

// --- Main Application Start ---
(async () => {
    const startTime = Date.now();
    console.log(`\n🚀 [Startup] Initializing Solana Gambles Bot v${BOT_VERSION}... (PID: ${process.pid})`);
    console.log(`Current Date/Time: ${new Date().toISOString()}`);

    console.log("⚙️ [Startup] Setting up process signal & error handlers...");
    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('uncaughtException', async (error, origin) => {
        console.error(`\n🚨🚨🚨 UNCAUGHT EXCEPTION [Origin: ${origin}] 🚨🚨🚨\n`, error);
        isFullyInitialized = false;
        if (!isShuttingDown) {
            console.error("Initiating emergency shutdown due to uncaught exception...");
            try {
                if (typeof notifyAdmin === "function") await notifyAdmin(`🚨🚨🚨 UNCAUGHT EXCEPTION (${escapeMarkdownV2(String(origin))})\n${escapeMarkdownV2(String(error.message || error))}\nAttempting emergency shutdown...`).catch(e => console.error("Admin notify fail (uncaught):", e));
                await shutdown('uncaughtException');
            } catch (shutdownErr) {
                console.error("❌ Emergency shutdown attempt itself FAILED:", shutdownErr);
                if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 Emergency shutdown itself FAILED after uncaught exception. Forcing exit.`).catch(e => console.error("Admin notify fail (uncaught shutdown fail):", e));
                setTimeout(() => process.exit(1), 1000).unref();
            }
        } else {
            console.warn("Uncaught exception occurred during an ongoing shutdown sequence. Forcing exit.");
            setTimeout(() => process.exit(1), 1000).unref();
        }
        const failTimeout = parseInt(process.env.SHUTDOWN_FAIL_TIMEOUT_MS, 10) || 8000;
        setTimeout(() => { console.error(`🚨 Forcing exit after ${failTimeout}ms due to uncaught exception (watchdog).`); process.exit(1); }, failTimeout).unref();
    });
    process.on('unhandledRejection', async (reason, promise) => {
        console.error('\n🔥🔥🔥 UNHANDLED PROMISE REJECTION 🔥🔥🔥');
        console.error('Promise:', promise);
        console.error('Reason:', reason);
        if (typeof notifyAdmin === "function") {
             const reasonMsg = reason instanceof Error ? reason.message : String(reason);
             await notifyAdmin(`🔥🔥🔥 UNHANDLED REJECTION\nReason: ${escapeMarkdownV2(reasonMsg)}`).catch(()=>{});
        }
    });

    try {
        console.log("⚙️ [Startup Step 1/8] Initializing Database (this will also set up listeners internally if needed by notifyAdmin)...");
        await initializeDatabase(); // Contains the DDL fixes now

        console.log("⚙️ [Startup Step 2/8] Loading Active Deposits Cache...");
        await loadActiveDepositsCache();

        console.log("⚙️ [Startup Step 3/8] Loading Slots Jackpot...");
        const loadedJackpot = await loadSlotsJackpot();
        currentSlotsJackpotLamports = loadedJackpot;
        console.log(`⚙️ [Startup] Global slots jackpot variable set to: ${formatSol(currentSlotsJackpotLamports)} SOL`);

        console.log("⚙️ [Startup Step 4/8] Setting up Telegram Connection (Webhook/Polling)...");
        const useWebhook = await setupTelegramConnection();

        console.log("⚙️ [Startup Step 5/8] Setting up Express Server...");
        setupExpressServer();

        console.log("⚙️ [Startup Step 6/8] Attaching Telegram Listeners & Starting Polling (if applicable)...");
        setupTelegramListeners();
        if (!useWebhook && bot.options.polling) {
            await startPollingFallback();
        } else if (!useWebhook && !bot.options.polling && (process.env.WEBHOOK_URL || process.env.RAILWAY_PUBLIC_DOMAIN)) {
            console.warn("⚠️ [Startup] Webhook setup failed, and polling was also not enabled. Bot might not receive updates.");
        } else if (useWebhook) {
            console.log("ℹ️ [Startup] Webhook mode active. Polling not started.");
        } else {
             console.log("ℹ️ [Startup] Polling mode is primary or webhook setup failed. Polling should be active if configured.")
        }

        const initDelay = parseInt(process.env.INIT_DELAY_MS, 10) || 3000;
        console.log(`⚙️ [Startup Step 7/8] Finalizing startup & scheduling Background Tasks in ${initDelay / 1000}s...`);

        setTimeout(async () => {
            try {
                console.log("⚙️ [Startup Final Phase Step 8/8] Starting Background Tasks now...");
                startDepositMonitor();
                startDepositSweeper();
                startLeaderboardManager();

                console.log("✅ [Startup Final Phase] Background Tasks scheduled/started.");
                
                await pool.query('SELECT NOW()'); // Final check of DB pool
                console.log("✅ [Startup Final Phase] Database pool confirmed operational post-init.");

                console.log("⚙️ [Startup Final Phase] Setting isFullyInitialized to true...");
                isFullyInitialized = true;
                console.log("✅ [Startup Final Phase] isFullyInitialized flag is now true. Bot is ready for full operation.");

                const me = await bot.getMe();
                const startupDuration = (Date.now() - startTime) / 1000;
                const startupMessage = `✅ Bot v${escapeMarkdownV2(BOT_VERSION)} Started Successfully\\!\nMode: ${useWebhook ? 'Webhook' : 'Polling'}\nJackpot: ${formatSol(currentSlotsJackpotLamports)} SOL\nStartup Time: ${startupDuration.toFixed(2)}s\\.`;
                console.log(`✅ [Startup Final Phase] Token validated. Bot Username: @${me.username}. Startup took ${startupDuration.toFixed(2)}s.`);
                if (typeof notifyAdmin === "function") await notifyAdmin(startupMessage).catch(()=>{});
                console.log(`\n🎉🎉🎉 Solana Gambles Bot is fully operational! (${new Date().toISOString()}) 🎉🎉🎉`);
            } catch (finalPhaseError) {
                console.error("❌ FATAL ERROR DURING FINAL STARTUP PHASE (after initial delay):", finalPhaseError);
                isFullyInitialized = false;
                if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 BOT STARTUP FAILED (Final Phase): ${escapeMarkdownV2(String(finalPhaseError.message || finalPhaseError))}. Bot may not be operational.`).catch(()=>{});
                if (!isShuttingDown) await shutdown('STARTUP_FINAL_PHASE_ERROR').catch(() => process.exit(5));
            }
        }, initDelay);

    } catch (error) {
        console.error("❌❌❌ FATAL ERROR DURING MAIN STARTUP SEQUENCE:", error);
        isFullyInitialized = false;
        if (pool) { pool.end().catch(() => {});}
        try {
            if (process.env.BOT_TOKEN && process.env.ADMIN_USER_IDS) {
                 const tempBotForError = new TelegramBot(process.env.BOT_TOKEN, {polling: false});
                 const adminIdsForError = process.env.ADMIN_USER_IDS.split(',');
                 for (const adminId of adminIdsForError) {
                     if(adminId.trim()) {
                         tempBotForError.sendMessage(adminId.trim(), `🚨 BOT STARTUP FAILED (Main Sequence): ${String(error.message || error)}. Check logs & DB. Exiting.`).catch(e => console.error("Fallback admin notify failed:", e));
                     }
                 }
            }
        } catch (finalNotifyError) {console.error("Could not send final admin error notification:", finalNotifyError);}
        process.exit(1);
    }
})();
// --- End of Part 6 / End of File index.js ---
