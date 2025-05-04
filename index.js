// index.js - Custodial Model with Buttons & Referrals
// --- VERSION: 3.0.0 ---

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
// import { toByteArray, fromByteArray } from 'base64-js'; // Base64 used internally by web3.js/bs58 if needed
import { Buffer } from 'buffer'; // Standard Node.js Buffer
// --- End of Imports ---

// --- Deployment Check Log ---
console.log(`--- INDEX.JS - DEPLOYMENT CHECK --- ${new Date().toISOString()} --- v3.0.0 ---`);
// --- END DEPLOYMENT CHECK ---


console.log("⏳ Starting Solana Gambles Bot (Custodial, Buttons, v3.0.0)... Checking environment variables...");

// --- Environment Variable Configuration (Revised for Custodial) ---
const REQUIRED_ENV_VARS = [
    'BOT_TOKEN',
    'DATABASE_URL',
    // Payout/Operational Private Keys (SECRET)
    'MAIN_BOT_PRIVATE_KEY',   // Used for general payouts (Withdrawals, potentially non-race game payouts if needed later)
    'RACE_BOT_PRIVATE_KEY',   // Used for Race game payouts (If race game logic still requires direct payout - unlikely in pure custodial) - **REVISIT IF RACE GAME NEEDS SPECIFIC HANDLING**
    // Deposit Master Key (SECRET - For deriving unique deposit addresses)
    // Recommend storing the SEED PHRASE securely, not the raw private key if possible
    'DEPOSIT_MASTER_SEED',    // BIP39 Seed Phrase for generating deposit addresses
    // OR 'DEPOSIT_MASTER_PRIVATE_KEY' // Alternative if using a raw key
    // ** AT LEAST ONE of the above deposit master keys MUST be set **

    // Public addresses are less critical now as primary interaction is internal, but good for reference/external checks
    // 'MAIN_OPERATING_WALLET_ADDRESS', // Public address of MAIN_BOT_PRIVATE_KEY (Optional for display/logging)
    // 'RACE_OPERATING_WALLET_ADDRESS', // Public address of RACE_BOT_PRIVATE_KEY (Optional)

    // Other Required
    'RPC_URLS',               // Comma-separated list of RPC URLs
];

// --- Deposit Key Check ---
if (!process.env.DEPOSIT_MASTER_SEED && !process.env.DEPOSIT_MASTER_PRIVATE_KEY) {
    console.error("❌ Critical Error: Either DEPOSIT_MASTER_SEED or DEPOSIT_MASTER_PRIVATE_KEY environment variable must be set for custodial deposit address generation.");
    process.exit(1);
}
// ---

// Check for Railway-specific variables if deployed there
if (process.env.RAILWAY_ENVIRONMENT) {
    REQUIRED_ENV_VARS.push('RAILWAY_PUBLIC_DOMAIN');
}

// Optional vars with defaults - Adjusted for Custodial Model
const OPTIONAL_ENV_DEFAULTS = {
    // --- Operational ---
    'ADMIN_USER_IDS': '',
    'DEPOSIT_ADDRESS_EXPIRY_MINUTES': '60', // How long a unique deposit address is valid
    'DEPOSIT_CONFIRMATIONS': '12',          // How many Solana confirmations needed for a deposit
    'WITHDRAWAL_FEE_LAMPORTS': '5000',     // Flat withdrawal fee (can be 0)
    'MIN_WITHDRAWAL_LAMPORTS': '10000000', // Minimum withdrawal amount (e.g., 0.01 SOL)
    // --- Payouts (Withdrawals, Referral Rewards) ---
    'PAYOUT_PRIORITY_FEE_RATE': '0.0001',
    'PAYOUT_BASE_PRIORITY_FEE_MICROLAMPORTS': '1000',
    'PAYOUT_MAX_PRIORITY_FEE_MICROLAMPORTS': '1000000',
    'PAYOUT_JOB_RETRIES': '3', // Retries for Withdrawals/Referral payouts
    'PAYOUT_JOB_RETRY_DELAY_MS': '5000',
    'REFERRAL_PAYOUT_PRIVATE_KEY': '', // Optional separate key for referral payouts
    // --- Game Limits (Now apply to internal balance bets) ---
    'CF_MIN_BET_LAMPORTS': '10000000',  // 0.01 SOL
    'CF_MAX_BET_LAMPORTS': '1000000000', // 1 SOL
    'RACE_MIN_BET_LAMPORTS': '10000000',
    'RACE_MAX_BET_LAMPORTS': '1000000000',
    'SLOTS_MIN_BET_LAMPORTS': '10000000',
    'SLOTS_MAX_BET_LAMPORTS': '500000000', // 0.5 SOL
    'ROULETTE_MIN_BET_LAMPORTS': '10000000', // Per placement
    'ROULETTE_MAX_BET_LAMPORTS': '1000000000', // Per placement
    'WAR_MIN_BET_LAMPORTS': '10000000',
    'WAR_MAX_BET_LAMPORTS': '1000000000',
    // --- Game Logic Parameters ---
    'CF_HOUSE_EDGE': '0.03', // Example: 3% edge
    'RACE_HOUSE_EDGE': '0.05', // Example: 5% edge
    'SLOTS_HIDDEN_EDGE': '0.10',
    'ROULETTE_GUARANTEED_EDGE_PROBABILITY': '0.10', // Example 10% chance
    // --- Technical ---
    'RPC_MAX_CONCURRENT': '8',
    'RPC_RETRY_BASE_DELAY': '600',
    'RPC_MAX_RETRIES': '3',
    'RPC_RATE_LIMIT_COOLOFF': '1500',
    'RPC_RETRY_MAX_DELAY': '15000',
    'RPC_RETRY_JITTER': '0.2',
    'RPC_COMMITMENT': 'confirmed', // Use 'confirmed' for balance checks, deposit confirmations
    'MSG_QUEUE_CONCURRENCY': '5',
    'MSG_QUEUE_TIMEOUT_MS': '10000',
    'CALLBACK_QUEUE_CONCURRENCY': '8', // Queue for handling button callbacks
    'CALLBACK_QUEUE_TIMEOUT_MS': '15000',
    'PAYMENT_PROCESSOR_CONCURRENCY': '3', // Single queue for Withdrawals/Referral payouts now
    'DEPOSIT_MONITOR_INTERVAL_MS': '15000', // How often to check for deposits (adjust based on method)
    'DEPOSIT_MONITOR_FETCH_LIMIT': '100', // Limit for fetching deposit TXs (depends on method)
    'DB_POOL_MAX': '20', // May need more connections for ledger/balance updates
    'DB_POOL_MIN': '5',
    'DB_IDLE_TIMEOUT': '30000',
    'DB_CONN_TIMEOUT': '5000',
    'DB_SSL': 'true',
    'DB_REJECT_UNAUTHORIZED': 'true',
    'USER_STATE_CACHE_TTL_MS': '300000', // Cache for conversational state (e.g., waiting for amount)
    'WALLET_CACHE_TTL_MS': '300000', // Cache for user withdrawal addresses / referral codes
    'DEPOSIT_ADDR_CACHE_TTL_MS': '3660000', // Cache for active deposit addresses (slightly > 1 hour)
    'MAX_PROCESSED_TX_CACHE': '10000', // Cache for processed deposit/withdrawal TX sigs
    'USER_COMMAND_COOLDOWN_MS': '1000', // Cooldown for general commands
    'INIT_DELAY_MS': '1000',
    'CONCURRENCY_ADJUST_DELAY_MS': '20000',
    'RPC_TARGET_CONCURRENT': '8',
    'RAILWAY_ROTATION_DRAIN_MS': '5000',
    'SHUTDOWN_QUEUE_TIMEOUT_MS': '20000',
    'SHUTDOWN_FAIL_TIMEOUT_MS': '8000',
    'WEBHOOK_MAX_CONN': '10'
};

// Validate required environment variables
let missingVars = false;
REQUIRED_ENV_VARS.forEach((key) => {
    if (key === 'RAILWAY_PUBLIC_DOMAIN' && !process.env.RAILWAY_ENVIRONMENT) return;
    if (!process.env[key]) {
        // Check for deposit key presence which was checked earlier
        if(key === 'DEPOSIT_MASTER_SEED' && !process.env.DEPOSIT_MASTER_PRIVATE_KEY) {
             console.error(`❌ Environment variable ${key} OR DEPOSIT_MASTER_PRIVATE_KEY is missing.`);
             missingVars = true;
        } else if (key !== 'DEPOSIT_MASTER_SEED') { // Avoid double logging if both missing
            console.error(`❌ Environment variable ${key} is missing.`);
            missingVars = true;
        }
    }
});

// Assign defaults for optional vars if they are missing
Object.entries(OPTIONAL_ENV_DEFAULTS).forEach(([key, defaultValue]) => {
    if (!process.env[key]) {
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

console.log("✅ Environment variables checked/defaults applied.");
// --- END: Environment Variable Configuration ---


// --- Startup State Tracking ---
let isFullyInitialized = false;
let server;
// Derive master keypair for deposits ONCE (using seed first if available)
// This is sensitive, ensure env vars are handled securely
let depositMasterKeypair;
try {
    if (process.env.DEPOSIT_MASTER_SEED) {
        // Note: Standard BIP39 seed phrase to Keypair requires specific derivation paths.
        // For simplicity here, we might assume DEPOSIT_MASTER_SEED IS the base58 secret key
        // OR use a library like @solana/wallet-adapter-base `derivePath` if using real BIP39/44
        // **Assuming DEPOSIT_MASTER_SEED is treated as base58 secret for this example**
        // **IN PRODUCTION: USE PROPER BIP39/44 DERIVATION for seed phrases**
        console.warn("⚠️ Treating DEPOSIT_MASTER_SEED as base58 secret key for derivation. Use BIP39 derivation libraries for production seed phrases.");
        depositMasterKeypair = Keypair.fromSecretKey(bs58.decode(process.env.DEPOSIT_MASTER_SEED));
    } else if (process.env.DEPOSIT_MASTER_PRIVATE_KEY) {
        depositMasterKeypair = Keypair.fromSecretKey(bs58.decode(process.env.DEPOSIT_MASTER_PRIVATE_KEY));
    }
    console.log(`ℹ️ Deposit master public key: ${depositMasterKeypair.publicKey.toBase58()}`);
} catch (e) {
     console.error(`❌ Critical Error: Failed to decode deposit master key/seed. Check DEPOSIT_MASTER_SEED or DEPOSIT_MASTER_PRIVATE_KEY format. Error: ${e.message}`);
     process.exit(1);
}
// --- End Startup State & Key Derivation ---


// --- Initialize Scalable Components ---
const app = express();

// --- Health Check Endpoints (Unchanged) ---
app.get('/health', (req, res) => { res.status(200).json({ status: server ? 'ready' : 'starting', uptime: process.uptime() }); });
app.get('/railway-health', (req, res) => { res.status(200).json({ status: isFullyInitialized ? 'ready' : 'starting', version: '3.0.0' }); });
app.get('/prestop', (req, res) => { console.log('🚪 Received pre-stop signal from Railway...'); res.status(200).send('Shutting down'); });
// --- END Health Check Endpoints ---


// --- Initialize Multi-RPC Solana connection (Unchanged Setup) ---
console.log("⚙️ Initializing Multi-RPC Solana connection...");
const rpcUrls = process.env.RPC_URLS.split(',').map(url => url.trim()).filter(url => url.length > 0 && (url.startsWith('http://') || url.startsWith('https://')));
if (rpcUrls.length === 0) { console.error("❌ No valid RPC URLs found. Exiting."); process.exit(1); }
console.log(`ℹ️ Using RPC Endpoints: ${rpcUrls.join(', ')}`);
const solanaConnection = new RateLimitedConnection(rpcUrls, {
    maxConcurrent: parseInt(process.env.RPC_MAX_CONCURRENT, 10),
    retryBaseDelay: parseInt(process.env.RPC_RETRY_BASE_DELAY, 10),
    maxRetries: parseInt(process.env.RPC_MAX_RETRIES, 10),
    rateLimitCooloff: parseInt(process.env.RPC_RATE_LIMIT_COOLOFF, 10),
    retryMaxDelay: parseInt(process.env.RPC_RETRY_MAX_DELAY, 10),
    retryJitter: parseFloat(process.env.RPC_RETRY_JITTER),
    commitment: process.env.RPC_COMMITMENT,
    httpHeaders: { 'User-Agent': `SolanaGamblesBot/3.0.0` },
    clientId: `SolanaGamblesBot/3.0.0`
});
console.log("✅ Multi-RPC Solana connection instance created.");
// --- End Solana Connection ---


// --- Live Queue Monitor & Emergency Rotate (Structurally Unchanged) ---
// (Logic remains the same, monitors the Solana connection queue)
setInterval(() => { /* ... Queue Monitor logging ... */ }, 60000);
let lastQueueProcessTime = Date.now();
if (typeof solanaConnection?._processQueue === 'function') { /* ... Patch _processQueue ... */ setInterval(() => { /* ... Emergency Rotate Check ... */ }, 10000); }
else { console.warn("[Emergency Rotate] Cannot patch _processQueue: Function not found."); }
// --- End Queue Monitor & Rotate ---


// --- Initial RPC Connection Test (Unchanged) ---
setTimeout(async () => { /* ... getSlot test ... */ }, parseInt(process.env.INIT_DELAY_MS, 10));


// --- Queues (Revised) ---
// Message queue for incoming Telegram messages/commands
const messageQueue = new PQueue({
    concurrency: parseInt(process.env.MSG_QUEUE_CONCURRENCY, 10),
    timeout: parseInt(process.env.MSG_QUEUE_TIMEOUT_MS, 10)
});
console.log(`✅ Message processing queue initialized (Concurrency: ${messageQueue.concurrency})`);

// Separate queue for handling button callback queries
const callbackQueue = new PQueue({
    concurrency: parseInt(process.env.CALLBACK_QUEUE_CONCURRENCY, 10),
    timeout: parseInt(process.env.CALLBACK_QUEUE_TIMEOUT_MS, 10)
});
console.log(`✅ Callback processing queue initialized (Concurrency: ${callbackQueue.concurrency})`);

// Queue for sending Telegram messages (Rate Limited)
const telegramSendQueue = new PQueue({ concurrency: 1, interval: 1050, intervalCap: 1 });
console.log(`✅ Telegram send queue initialized (Rate Limited)`);

// Single queue for processing outgoing payments (Withdrawals, Referral Payouts)
const payoutProcessorQueue = new PQueue({
     concurrency: parseInt(process.env.PAYMENT_PROCESSOR_CONCURRENCY, 10)
});
console.log(`✅ Payout processing queue initialized (Concurrency: ${payoutProcessorQueue.concurrency})`);
// --- End Queues ---


// --- PostgreSQL Pool (Unchanged Setup) ---
console.log("⚙️ Setting up optimized PostgreSQL Pool...");
const pool = new Pool({ /* ... pool config ... */ });
pool.on('error', (err, client) => { console.error('❌ Unexpected error on idle PostgreSQL client', err); });
console.log("✅ PostgreSQL Pool created.");
// --- End Pool ---


// --- Performance Monitor (Unchanged) ---
const performanceMonitor = { /* ... monitor logic ... */ };


// --- Database Initialization (Custodial Schema v3.0.0) ---
async function initializeDatabase() {
    console.log("⚙️ Initializing Database schema (v3.0.0 - Custodial)...");
    let client;
    try {
        client = await pool.connect();
        await client.query('BEGIN');

        // Core User/Wallet Info + Referral Tracking
        await client.query(`CREATE TABLE IF NOT EXISTS wallets (
            user_id TEXT PRIMARY KEY,                     -- Telegram User ID
            external_withdrawal_address TEXT,           -- User's linked external address (for withdrawals)
            referral_code TEXT UNIQUE,                   -- This user's unique referral code
            referred_by_user_id TEXT,                    -- user_id of the referrer
            referral_count INT NOT NULL DEFAULT 0,       -- How many users this user has referred
            total_wagered BIGINT NOT NULL DEFAULT 0,     -- Total lamports wagered internally by this user
            last_milestone_paid_lamports BIGINT NOT NULL DEFAULT 0, -- Tracks last milestone paid FOR this user's referrer
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            linked_at TIMESTAMPTZ,                      -- When external_withdrawal_address was first set/updated
            last_used_at TIMESTAMPTZ                     -- Last interaction time
        );`);
        console.log("   - Wallets table verified/created.");
        // Add columns idempotently
        await client.query(`ALTER TABLE wallets ADD COLUMN IF NOT EXISTS external_withdrawal_address TEXT;`);
        await client.query(`ALTER TABLE wallets ADD COLUMN IF NOT EXISTS referral_code TEXT UNIQUE;`);
        await client.query(`ALTER TABLE wallets ADD COLUMN IF NOT EXISTS referred_by_user_id TEXT;`);
        await client.query(`ALTER TABLE wallets ADD COLUMN IF NOT EXISTS referral_count INT NOT NULL DEFAULT 0;`);
        await client.query(`ALTER TABLE wallets ADD COLUMN IF NOT EXISTS total_wagered BIGINT NOT NULL DEFAULT 0;`);
        await client.query(`ALTER TABLE wallets ADD COLUMN IF NOT EXISTS last_milestone_paid_lamports BIGINT NOT NULL DEFAULT 0;`);
        await client.query(`ALTER TABLE wallets ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();`);
        await client.query(`ALTER TABLE wallets ADD COLUMN IF NOT EXISTS linked_at TIMESTAMPTZ;`);
        await client.query(`ALTER TABLE wallets ADD COLUMN IF NOT EXISTS last_used_at TIMESTAMPTZ;`);

        // User Internal Balances
        await client.query(`CREATE TABLE IF NOT EXISTS user_balances (
            user_id TEXT PRIMARY KEY REFERENCES wallets(user_id) ON DELETE CASCADE,
            balance_lamports BIGINT NOT NULL DEFAULT 0 CHECK (balance_lamports >= 0), -- Ensure non-negative
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );`);
        console.log("   - User_Balances table verified/created.");
         // Add columns idempotently
        await client.query(`ALTER TABLE user_balances ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();`);


        // Deposit Address Tracking
        await client.query(`CREATE TABLE IF NOT EXISTS deposit_addresses (
            id SERIAL PRIMARY KEY,
            user_id TEXT NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
            deposit_address TEXT UNIQUE NOT NULL,
            derivation_path TEXT NOT NULL, -- Store path for potential key recovery/auditing
            status TEXT NOT NULL DEFAULT 'pending', -- 'pending', 'used', 'expired'
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            expires_at TIMESTAMPTZ NOT NULL
        );`);
        console.log("   - Deposit_Addresses table verified/created.");

        // Deposit Transaction Log
        await client.query(`CREATE TABLE IF NOT EXISTS deposits (
            id SERIAL PRIMARY KEY,
            user_id TEXT NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
            deposit_address_id INT REFERENCES deposit_addresses(id) ON DELETE SET NULL, -- Link to the specific address used
            tx_signature TEXT UNIQUE NOT NULL,
            amount_lamports BIGINT NOT NULL,
            status TEXT NOT NULL DEFAULT 'confirmed', -- 'confirmed', 'error' (e.g., couldn't credit balance)
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW() -- When balance was credited
        );`);
        console.log("   - Deposits table verified/created.");

        // Withdrawal Request Log
        await client.query(`CREATE TABLE IF NOT EXISTS withdrawals (
            id SERIAL PRIMARY KEY,
            user_id TEXT NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
            requested_amount_lamports BIGINT NOT NULL,
            fee_lamports BIGINT NOT NULL DEFAULT 0,
            final_send_amount_lamports BIGINT NOT NULL,
            recipient_address TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending', -- 'pending', 'processing', 'completed', 'failed'
            payout_tx_signature TEXT UNIQUE,
            error_message TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            processed_at TIMESTAMPTZ,
            completed_at TIMESTAMPTZ
        );`);
        console.log("   - Withdrawals table verified/created.");

        // Transaction Ledger (Audit Trail)
        await client.query(`CREATE TABLE IF NOT EXISTS ledger (
            id BIGSERIAL PRIMARY KEY,
            user_id TEXT NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
            transaction_type TEXT NOT NULL, -- 'deposit', 'bet_placed', 'bet_won', 'bet_push_return', 'withdrawal', 'withdrawal_fee', 'referral_bonus', 'manual_adjustment'
            amount_lamports BIGINT NOT NULL, -- Positive for credit, Negative for debit
            balance_before BIGINT NOT NULL,
            balance_after BIGINT NOT NULL,
            related_deposit_id INT REFERENCES deposits(id) ON DELETE SET NULL,
            related_withdrawal_id INT REFERENCES withdrawals(id) ON DELETE SET NULL,
            related_bet_id INT, -- No FK to bets, as bets might be cleaned up sooner? Or add FK? Let's skip FK for now.
            related_ref_payout_id INT, -- REFERENCES referral_payouts(id) ON DELETE SET NULL (Add this table next)
            notes TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );`);
        console.log("   - Ledger table verified/created.");

         // Referral Payouts Table (from previous version)
         await client.query(`CREATE TABLE IF NOT EXISTS referral_payouts (
             id SERIAL PRIMARY KEY,
             referrer_user_id TEXT NOT NULL,-- REFERENCES wallets(user_id) ON DELETE CASCADE, -- Removed FK for flexibility if user leaves
             referee_user_id TEXT NOT NULL, -- REFERENCES wallets(user_id) ON DELETE CASCADE,
             payout_type TEXT NOT NULL,              -- 'initial_bet' or 'milestone'
             triggering_bet_id INT,                  -- bets.id of the referee's first bet (for initial_bet type) - No FK for flexibility
             milestone_reached_lamports BIGINT,      -- Wagered amount milestone reached (for milestone type)
             payout_amount_lamports BIGINT NOT NULL, -- Amount paid to referrer
             status TEXT NOT NULL,                   -- 'pending', 'processing', 'paid', 'failed'
             payout_tx_signature TEXT UNIQUE,        -- Signature of the successful payout transaction
             error_message TEXT,                     -- Store reason for failure
             created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
             processed_at TIMESTAMPTZ,               -- When processing started
             paid_at TIMESTAMPTZ                     -- When payout was confirmed successful
         );`);
         console.log("   - Referral_Payouts table verified/created.");
         // Add FK constraint from ledger to referral_payouts if needed
         await client.query(`ALTER TABLE ledger ADD COLUMN IF NOT EXISTS related_ref_payout_id INT REFERENCES referral_payouts(id) ON DELETE SET NULL;`);


        // Bets Table (Still needed for tracking game state/results)
        // Consider if bet expiry is needed in custodial? Less critical perhaps.
        // Memo ID becomes less important for matching payments.
        // paid_tx_signature not needed. payout_tx_signature not needed (handled by withdrawal/ref_payout tables)
        await client.query(`CREATE TABLE IF NOT EXISTS bets (
            id SERIAL PRIMARY KEY,
            user_id TEXT NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
            chat_id TEXT NOT NULL,
            game_type TEXT NOT NULL,
            bet_details JSONB,
            wager_amount_lamports BIGINT NOT NULL CHECK (wager_amount_lamports > 0), -- Renamed from expected_lamports
            -- memo_id TEXT UNIQUE NOT NULL, -- No longer needed for matching payment
            status TEXT NOT NULL, -- 'active', 'processing_game', 'completed_win', 'completed_loss', 'completed_push', 'error_game_logic'
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            -- expires_at TIMESTAMPTZ NOT NULL, -- Less relevant? Keep for cleanup maybe?
            -- paid_tx_signature TEXT UNIQUE, -- Removed
            -- payout_tx_signature TEXT UNIQUE, -- Removed
            processed_at TIMESTAMPTZ, -- When game logic finished
            payout_amount_lamports BIGINT, -- Amount credited back to internal balance (wager + win, or wager for push)
            priority INT NOT NULL DEFAULT 0
        );`);
        console.log("   - Bets table verified/created (revised for custodial).");
        // Add/remove columns idempotently
        await client.query(`ALTER TABLE bets RENAME COLUMN IF EXISTS expected_lamports TO wager_amount_lamports;`);
        await client.query(`ALTER TABLE bets DROP COLUMN IF EXISTS memo_id;`);
        await client.query(`ALTER TABLE bets DROP COLUMN IF EXISTS expires_at;`);
        await client.query(`ALTER TABLE bets DROP COLUMN IF EXISTS paid_tx_signature;`);
        await client.query(`ALTER TABLE bets DROP COLUMN IF EXISTS payout_tx_signature;`);
        await client.query(`ALTER TABLE bets DROP COLUMN IF EXISTS fees_paid;`);
        await client.query(`ALTER TABLE bets ADD COLUMN IF NOT EXISTS wager_amount_lamports BIGINT NOT NULL DEFAULT 0;`);
        await client.query(`ALTER TABLE bets ADD COLUMN IF NOT EXISTS payout_amount_lamports BIGINT;`);
        await client.query(`ALTER TABLE bets ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ;`);
        await client.query(`ALTER TABLE bets ADD COLUMN IF NOT EXISTS priority INT NOT NULL DEFAULT 0;`);
        await client.query(`ALTER TABLE bets ADD CONSTRAINT wager_positive CHECK (wager_amount_lamports > 0);`);


        // --- INDEXES ---
        console.log("   - Creating/verifying indexes...");
        // Wallets
        await client.query(`CREATE INDEX IF NOT EXISTS idx_wallets_ref_code ON wallets(referral_code) WHERE referral_code IS NOT NULL;`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_wallets_referred_by ON wallets(referred_by_user_id) WHERE referred_by_user_id IS NOT NULL;`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_wallets_external_addr ON wallets(external_withdrawal_address) WHERE external_withdrawal_address IS NOT NULL;`);
        // User Balances
        // PRIMARY KEY index already exists on user_id
        // Deposit Addresses
        await client.query(`CREATE INDEX IF NOT EXISTS idx_depaddr_user ON deposit_addresses(user_id);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_depaddr_addr_pending ON deposit_addresses(deposit_address) WHERE status = 'pending';`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_depaddr_expires ON deposit_addresses(expires_at);`);
        // Deposits
        await client.query(`CREATE INDEX IF NOT EXISTS idx_deposits_user ON deposits(user_id);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_deposits_tx_sig ON deposits(tx_signature);`);
        // Withdrawals
        await client.query(`CREATE INDEX IF NOT EXISTS idx_withdrawals_user ON withdrawals(user_id);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_withdrawals_status ON withdrawals(status);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_withdrawals_payout_sig ON withdrawals(payout_tx_signature) WHERE payout_tx_signature IS NOT NULL;`);
        // Ledger
        await client.query(`CREATE INDEX IF NOT EXISTS idx_ledger_user_time ON ledger(user_id, created_at DESC);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_ledger_type ON ledger(transaction_type);`);
        // Referral Payouts (Existing + FK index)
        await client.query(`CREATE INDEX IF NOT EXISTS idx_refpayout_referrer ON referral_payouts(referrer_user_id);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_refpayout_referee ON referral_payouts(referee_user_id);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_refpayout_status ON referral_payouts(status);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_refpayout_paid_tx_sig ON referral_payouts(payout_tx_signature) WHERE payout_tx_signature IS NOT NULL;`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_refpayout_milestone_check ON referral_payouts(referrer_user_id, referee_user_id, payout_type, milestone_reached_lamports) WHERE payout_type = 'milestone';`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_ledger_ref_payout_id ON ledger(related_ref_payout_id) WHERE related_ref_payout_id IS NOT NULL;`);
        // Bets (Revised)
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_user_status_time ON bets(user_id, status, created_at DESC);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_status ON bets(status);`); // Bets needing processing etc
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_game_type ON bets(game_type);`);


        await client.query('COMMIT'); // Commit transaction
        console.log("✅ Database schema initialized/verified successfully (v3.0.0).");

    } catch (err) {
        console.error("❌ Database initialization error:", err);
        if (client) { try { await client.query('ROLLBACK'); console.log("   - Transaction rolled back."); } catch (rbErr) { console.error("   - Rollback failed:", rbErr); } }
        // Check for common non-fatal errors during setup
        if (['42P07', '23505', '42701'].includes(err.code) || err.message.includes('already exists') || err.message.includes('multiple primary keys') || err.message.includes('is not unique') || err.message.includes('check constraint')) {
            console.warn("   - Note: Some elements might already exist or constraints failed benignly during idempotent setup.");
        } else {
            throw err; // Throw unexpected errors
        }
    } finally {
        if (client) client.release(); // Always release the client
    }
}

// --- End of Part 1 ---
// index.js - Part 2a (Custodial Model with Buttons & Referrals)
// --- VERSION: 3.0.0 ---

// (Code continues directly from the end of Part 1)

// --- Telegram Bot Initialization with Queues ---
console.log("⚙️ Initializing Telegram Bot...");
const bot = new TelegramBot(process.env.BOT_TOKEN, {
    polling: false, // Use webhooks in production (set later in startup)
    request: {       // Adjust request options for stability
        timeout: 10000, // Request timeout: 10s
        agentOptions: {
            keepAlive: true,   // Reuse connections
            timeout: 60000     // Keep-alive timeout: 60s
        }
    }
});

// Listener for regular text messages/commands
bot.on('message', (msg) => {
    // Basic filtering at the entry point
    if (!msg || !msg.from || msg.from.is_bot || !msg.chat || (!msg.text && !msg.location && !msg.contact)) { // Allow location/contact for potential future features
        return;
    }
    // Add to message queue for processing
    messageQueue.add(() => handleMessage(msg)) // handleMessage defined later
        .catch(err => {
            console.error(`⚠️ Message processing queue error for msg ID ${msg?.message_id}:`, err);
            performanceMonitor.logRequest(false); // Log error
        });
});

// *** NEW: Listener for inline button callbacks ***
bot.on('callback_query', (callbackQuery) => {
    // Add to callback queue for processing
    callbackQueue.add(() => handleCallbackQuery(callbackQuery)) // handleCallbackQuery defined later
        .catch(err => {
            console.error(`⚠️ Callback query processing queue error for query ID ${callbackQuery?.id}:`, err);
             // Attempt to answer callback query to clear loading state even on error
             bot.answerCallbackQuery(callbackQuery.id).catch(e => console.warn("Failed to answer callback query on error:", e.message));
        });
});


// --- Rate-Limited Telegram Sending Function ---
// (Unchanged Functionality)
const telegramSendQueue = new PQueue({ concurrency: 1, interval: 1050, intervalCap: 1 });
async function safeSendMessage(chatId, message, options = {}) {
    // Input validation
    if (!chatId || typeof message !== 'string') {
        console.error("[safeSendMessage] Invalid input:", { chatId, message });
        return Promise.resolve(undefined);
    }
     // Message length check
     const MAX_LENGTH = 4096;
     if (message.length > MAX_LENGTH) {
         console.warn(`[safeSendMessage] Message for chat ${chatId} too long (${message.length} > ${MAX_LENGTH}), truncating.`);
         message = message.substring(0, MAX_LENGTH - 3) + "...";
     }

    // Add to queue
    return telegramSendQueue.add(async () => {
        try {
            const sentMessage = await bot.sendMessage(chatId, message, options);
            return sentMessage;
        } catch (error) {
            console.error(`❌ Telegram send error to chat ${chatId}:`, error.message);
            // Handle specific errors
            if (error.response && error.response.statusCode === 403) { // Forbidden
                 console.warn(` -> Bot blocked or kicked from chat ${chatId}?`);
            } else if (error.response && error.response.statusCode === 400) { // Bad Request
                 if (error.message.includes("can't parse entities")) {
                     console.error(` -> Telegram Parse Error: Check MarkdownV2 escaping!`);
                     console.error(` -> Snippet: ${message.substring(0, 100)}...`);
                 } else if (error.message.includes("message is too long")) {
                     console.error(` -> Message confirmed too long after truncation attempt? Check calculation.`);
                 } else {
                      console.error(` -> Bad Request: ${error.message}`);
                 }
            } else if (error.response && error.response.statusCode === 429) { // Too Many Requests
                 console.warn(` -> Hit Telegram rate limit sending to ${chatId}. Queue should handle, but check interval if persistent.`);
            } else {
                // Log other errors generically
                console.error(` -> Status Code: ${error.response?.statusCode || 'N/A'}`);
            }
            return undefined; // Return undefined on error
        }
    });
}
console.log("✅ Telegram Bot initialized with message/callback listeners.");
// --- End Telegram Bot Init ---


// --- Express Setup for Webhook and Health Check ---
app.use(express.json({ limit: '10kb' })); // Keep request size limit reasonable

// Info endpoint
app.get('/', (req, res) => {
    performanceMonitor.logRequest(true);
    // Simplified info for now
    res.status(200).json({
        status: 'ok',
        initialized: isFullyInitialized,
        timestamp: new Date().toISOString(),
        version: '3.0.0', // Updated version
        // Add more stats later if needed (e.g., queue sizes, cache sizes)
    });
});

// Webhook handler - Only processes 'message' updates now
const webhookPath = `/bot${process.env.BOT_TOKEN}`;
app.post(webhookPath, (req, res) => {
    // Check if it's a message update before queueing
    if (req.body && req.body.message) {
        messageQueue.add(() => {
            try {
                bot.processUpdate(req.body); // Hand off valid message update to bot library
                performanceMonitor.logRequest(true);
            } catch (error) {
                console.error("❌ Webhook processing error:", error);
                performanceMonitor.logRequest(false);
            }
        }).catch(queueError => {
            console.error("❌ Error adding webhook update to message queue:", queueError);
            performanceMonitor.logRequest(false);
        });
    } else if (req.body && req.body.callback_query) {
         // Let the main `bot.on('callback_query', ...)` handle this via polling or direct webhook push
         // Do nothing here for callbacks to avoid double processing if polling is also active
    } else {
         console.warn("⚠️ Received non-message/non-callback_query update via webhook:", req.body);
    }
    // Always acknowledge Telegram quickly for any valid JSON received
    res.sendStatus(200);
});
console.log("✅ Express App initialized.");
// --- End Express Setup ---


// --- State Management & Constants (Custodial Model) ---

// User command cooldown (Unchanged)
const confirmCooldown = new Map(); // Map<userId, timestamp>
const cooldownInterval = parseInt(process.env.USER_COMMAND_COOLDOWN_MS, 10);

// *** NEW: User conversational state cache ***
// Used for multi-step interactions (e.g., asking for amount, then choice)
// Stores: Map<userId, { state: string, chatId: string, data?: any, timestamp: number }>
const userStateCache = new Map();
const USER_STATE_TTL = parseInt(process.env.USER_STATE_CACHE_TTL_MS, 10);

// Cache for linked EXTERNAL withdrawal wallets & referral codes (fetched from DB)
const walletCache = new Map(); // Map<userId, { withdrawalAddress?: string, referralCode?: string, timestamp: number }>
const WALLET_CACHE_TTL = parseInt(process.env.WALLET_CACHE_TTL_MS, 10);

// Cache of processed deposit/withdrawal transaction signatures
const processedSignaturesThisSession = new Set();
const MAX_PROCESSED_TX_CACHE = parseInt(process.env.MAX_PROCESSED_TX_CACHE, 10);

// Cache for active deposit addresses (to quickly map incoming deposits)
// Stores: Map<deposit_address, { userId: string, expiresAt: number }>
const activeDepositAddresses = new Map();
const DEPOSIT_ADDR_CACHE_TTL = parseInt(process.env.DEPOSIT_ADDR_CACHE_TTL_MS, 10);
// Note: Requires a mechanism to populate/update this cache from the DB

// Temporary storage for users who used a referral code at /start (Unchanged)
const pendingReferrals = new Map(); // Map<userId, { referrerUserId: string, timestamp: number }>
const PENDING_REFERRAL_TTL_MS = 24 * 60 * 60 * 1000; // 24 hours

// Referral System Constants (Unchanged)
const REFERRAL_MILESTONE_THRESHOLDS_LAMPORTS = [ /* Same values as before */ BigInt(1 * LAMPORTS_PER_SOL), BigInt(5 * LAMPORTS_PER_SOL), BigInt(10 * LAMPORTS_PER_SOL), BigInt(50 * LAMPORTS_PER_SOL), BigInt(100 * LAMPORTS_PER_SOL), BigInt(500 * LAMPORTS_PER_SOL), BigInt(1000 * LAMPORTS_PER_SOL) ];
const REFERRAL_MILESTONE_REWARD_PERCENT = 0.005; // 0.5%

// Custodial Operation Constants
const DEPOSIT_ADDRESS_EXPIRY_MS = parseInt(process.env.DEPOSIT_ADDRESS_EXPIRY_MINUTES, 10) * 60 * 1000;
const DEPOSIT_CONFIRMATIONS_NEEDED = parseInt(process.env.DEPOSIT_CONFIRMATIONS, 10);
const WITHDRAWAL_FEE_LAMPORTS = BigInt(process.env.WITHDRAWAL_FEE_LAMPORTS);
const MIN_WITHDRAWAL_LAMPORTS = BigInt(process.env.MIN_WITHDRAWAL_LAMPORTS);

console.log("✅ State caches and constants initialized.");
// --- End State Management & Constants ---


// --- Game Configuration (Custodial Bets) ---
function validateGameConfigCustodial(config) {
     const errors = [];
     for (const game in config) {
         const gc = config[game];
         if (!gc) { errors.push(`Config for game "${game}" missing.`); continue; }
         // Validate bet limits (now lamports)
         if (!gc.minBetLamports || BigInt(gc.minBetLamports) <= 0n) errors.push(`${game}.minBetLamports invalid (${gc.minBetLamports})`);
         if (!gc.maxBetLamports || BigInt(gc.maxBetLamports) <= 0n) errors.push(`${game}.maxBetLamports invalid (${gc.maxBetLamports})`);
         if (BigInt(gc.minBetLamports) > BigInt(gc.maxBetLamports)) errors.push(`${game} minBet > maxBet`);
         // Validate edge parameters
         if (gc.houseEdge !== undefined && (isNaN(gc.houseEdge) || gc.houseEdge < 0 || gc.houseEdge >= 1)) errors.push(`${game}.houseEdge invalid (${gc.houseEdge})`);
         if (gc.hiddenEdge !== undefined && (isNaN(gc.hiddenEdge) || gc.hiddenEdge < 0 || gc.hiddenEdge >= 1)) errors.push(`${game}.hiddenEdge invalid (${gc.hiddenEdge})`);
         if (gc.guaranteedEdgeProbability !== undefined && (isNaN(gc.guaranteedEdgeProbability) || gc.guaranteedEdgeProbability < 0 || gc.guaranteedEdgeProbability >= 1)) errors.push(`${game}.guaranteedEdgeProbability invalid (${gc.guaranteedEdgeProbability})`);
     }
     return errors;
}

// Game config now uses lamports for limits and has no expiry minutes
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
        // Define odds within the handler or here if static
    },
    slots: {
        minBetLamports: BigInt(process.env.SLOTS_MIN_BET_LAMPORTS),
        maxBetLamports: BigInt(process.env.SLOTS_MAX_BET_LAMPORTS),
        hiddenEdge: parseFloat(process.env.SLOTS_HIDDEN_EDGE)
        // Define symbols/payouts within handler or here
    },
    roulette: {
        minBetLamports: BigInt(process.env.ROULETTE_MIN_BET_LAMPORTS), // Per placement
        maxBetLamports: BigInt(process.env.ROULETTE_MAX_BET_LAMPORTS), // Per placement
        guaranteedEdgeProbability: parseFloat(process.env.ROULETTE_GUARANTEED_EDGE_PROBABILITY)
        // Define numbers/payouts within handler or here
    },
    war: {
        minBetLamports: BigInt(process.env.WAR_MIN_BET_LAMPORTS),
        maxBetLamports: BigInt(process.env.WAR_MAX_BET_LAMPORTS),
        // Skew handled internally
    }
};
// Validate game config structure
const configErrorsCustodial = validateGameConfigCustodial(GAME_CONFIG);
if (configErrorsCustodial.length > 0) {
    console.error(`❌ Invalid game configuration values found: ${configErrorsCustodial.join(', ')}. Check corresponding LAMPORTS env variables.`);
    process.exit(1);
}
console.log("✅ Game Config Loaded (Lamports Limits, Edges - Custodial):", JSON.stringify(GAME_CONFIG, (key, value) => typeof value === 'bigint' ? value.toString() : value)); // Log bigints as strings
// --- End Game Configuration ---


// --- Helper Functions ---

// Markdown Escaping (Unchanged)
const escapeMarkdownV2 = (text) => { /* ... same code ... */ if (typeof text !== 'string') text = String(text); text = text.replace(/\\/g, '\\\\'); return text.replace(/([_*\[\]()~`>#+\-=|{}.!])/g, '\\$1'); };

// Referral Code Generation (Unchanged)
function generateReferralCode(length = 8) { /* ... same code ... */ const randomBytes = crypto.randomBytes(Math.ceil(length / 2)); const hexString = randomBytes.toString('hex').slice(0, length); return `ref_${hexString}`; }

// *** NEW: Unique Deposit Address Generation ***
// NOTE: This is a simplified example. Production needs robust index/nonce management.
// It assumes user_id can be converted to a number for the path.
/**
 * Generates a unique deposit address for a user.
 * WARNING: Uses a simplified derivation path for demonstration. Production requires secure index management.
 * @param {string} userId - The user's unique ID (e.g., Telegram ID).
 * @param {number} addressIndex - A unique index for this user's addresses (e.g., fetch max index from DB + 1).
 * @returns {Promise<{publicKey: PublicKey, privateKey: Uint8Array, path: string} | null>} Derived keypair and path, or null on error.
 */
async function generateUniqueDepositAddress(userId, addressIndex) {
    try {
        // Attempt to create a somewhat unique numeric part from userId
        // THIS IS NOT SECURE FOR ALL USER IDs - NEEDS ROBUST HASHING/MAPPING
        const userIdBigInt = BigInt(userId);
        const userIdPathPart = Number(userIdBigInt % BigInt(2**31 - 1)); // Keep within BIP32 hardened range limit approximation

        if (isNaN(userIdPathPart) || typeof addressIndex !== 'number' || addressIndex < 0) {
             console.error(`[Address Gen] Invalid userId path part or address index`, {userId, addressIndex});
             return null;
        }

        // Example Derivation Path (BIP44 for Solana: m/44'/501'/account'/change')
        // We'll use account' for user ID part and change' for address index
        // Using hardened paths (') requires parent private key.
        // Let's use non-hardened for simplicity IF depositMasterKeypair is derived directly.
        // **PRODUCTION MUST USE CORRECT HARDENED DERIVATION**
        const derivationPath = `m/44'/501'/${userIdPathPart}'/${addressIndex}'`; // Example path

        // Derivation logic depends heavily on how depositMasterKeypair was generated (seed vs key)
        // Using a library like `hdkey` or `@solana/wallet-adapter-base` `derivePath` is recommended for BIP39 seeds.
        // **Simplified placeholder - replace with proper derivation library:**
        if (depositMasterKeypair) {
            // THIS IS NOT REAL DERIVATION - JUST A PLACEHOLDER CONCEPT
            // You need a library function like: derivedKeypair = derivePath(derivationPath, masterSeed);
            console.warn(`[Address Gen] Using placeholder derivation for ${derivationPath}. Implement actual BIP32/44 derivation.`);
            // Simulate derivation by creating a new keypair and associating the path - **DO NOT USE IN PROD**
            const derivedKeypair = Keypair.generate(); // FAKE!

            return {
                publicKey: derivedKeypair.publicKey,
                privateKey: derivedKeypair.secretKey, // Bot needs this to sweep funds
                path: derivationPath
            };
        } else {
            console.error("[Address Gen] Deposit master keypair not available.");
            return null;
        }
    } catch (error) {
        console.error(`[Address Gen] Error deriving address for user ${userId}, index ${addressIndex}: ${error.message}`);
        return null;
    }
}


// --- Keep Debug/Decode Helpers? ---
// May not be needed if memo parsing is gone, but low harm.
function debugInstruction(inst, accountKeys) { /* ... same code ... */ try { const programIdKeyInfo = accountKeys[inst.programIdIndex]; const programId = programIdKeyInfo?.pubkey ? new PublicKey(programIdKeyInfo.pubkey) : (typeof programIdKeyInfo === 'string' ? new PublicKey(programIdKeyInfo) : (programIdKeyInfo instanceof PublicKey ? programIdKeyInfo : null)); const accountPubkeys = inst.accounts?.map(idx => { const keyInfo = accountKeys[idx]; return keyInfo?.pubkey ? new PublicKey(keyInfo.pubkey) : (typeof keyInfo === 'string' ? new PublicKey(keyInfo) : (keyInfo instanceof PublicKey ? keyInfo : null)); }).filter(pk => pk !== null) .map(pk => pk.toBase58()); return { programId: programId ? programId.toBase58() : `Invalid Index ${inst.programIdIndex}`, data: inst.data ? Buffer.from(inst.data, 'base64').toString('hex') : null, accounts: accountPubkeys }; } catch (e) { console.error("[DEBUG INSTR HELPER] Error:", e); return { error: e.message, programIdIndex: inst.programIdIndex, accountIndices: inst.accounts }; } }
const decodeInstructionData = (data) => { /* ... same code ... */ if (!data) return null; try { if (typeof data === 'string') { if (/^[A-Za-z0-9+/]*={0,2}$/.test(data) && data.length % 4 === 0) { try { const decoded = Buffer.from(data, 'base64').toString('utf8'); if (/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/.test(decoded)) return null; return decoded; } catch { /* ignore */ } } if (/[1-9A-HJ-NP-Za-km-z]{20,}/.test(data) && data.length < 70) { try { const decoded = Buffer.from(bs58.decode(data)).toString('utf8'); if (/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/.test(decoded)) return null; return decoded; } catch { /* ignore */ } } if (/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/.test(data)) return null; return data; } else if (Buffer.isBuffer(data) || data instanceof Uint8Array) { const decoded = Buffer.from(data).toString('utf8'); if (/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/.test(decoded)) return null; return decoded; } else if (Array.isArray(data) && data.every(n => typeof n === 'number')) { const decoded = Buffer.from(data).toString('utf8'); if (/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/.test(decoded)) return null; return decoded; } return null; } catch (e) { return null; } };

// --- End of Part 2a ---
// index.js - Part 2b (Custodial Model with Buttons & Referrals)
// --- VERSION: 3.0.0 ---

// (Code continues directly from the end of Part 2a)

// --- Memo Handling System ---
// **REMOVED** - Memos are no longer the primary mechanism for linking bets/payments in the custodial model.
// Deposit identification uses unique addresses. Memo-related functions like
// findMemoInTx, normalizeMemo, validateOriginalMemoFormat, generateMemoId (for bets)
// and associated constants (MEMO_PROGRAM_IDS, VALID_MEMO_PREFIXES) are removed.
// --- END Memo Handling System ---


// --- Database Operations (Custodial Model) ---

// --- Wallet/User Ops ---

/**
 * Updates the external withdrawal address for a user.
 * Generates referral code if missing (should ideally be done on first record creation).
 * @param {string} userId
 * @param {string} externalAddress
 * @returns {Promise<{success: boolean, wallet?: string, error?: string}>}
 */
async function linkUserWallet(userId, externalAddress) {
    userId = String(userId);
    const logPrefix = `[LinkWallet User ${userId}]`;
    let client;
    try {
        new PublicKey(externalAddress); // Validate address format
        client = await pool.connect();
        await client.query('BEGIN');

        // Check if user exists, create if not (basic record)
        // Referral linking happens on first deposit now, not here primarily.
        const checkUser = await client.query('SELECT user_id, referral_code FROM wallets WHERE user_id = $1 FOR UPDATE', [userId]);
        let currentReferralCode = null;

        if (checkUser.rowCount === 0) {
            console.log(`${logPrefix} User not found, creating basic wallet record.`);
            currentReferralCode = generateReferralCode();
            // Insert basic wallet record, balance created separately if needed
            await client.query(
                `INSERT INTO wallets (user_id, external_withdrawal_address, referral_code, linked_at, last_used_at)
                 VALUES ($1, $2, $3, NOW(), NOW())`,
                [userId, externalAddress, currentReferralCode]
            );
             // Also ensure a balance record exists
             await client.query(
                 `INSERT INTO user_balances (user_id, balance_lamports) VALUES ($1, 0) ON CONFLICT (user_id) DO NOTHING`,
                 [userId]
             );
        } else {
            // User exists, update address and timestamps
            currentReferralCode = checkUser.rows[0].referral_code;
            // Generate referral code if somehow missing for existing user
            if (!currentReferralCode) {
                 console.warn(`${logPrefix} Existing user missing referral code, generating one now.`);
                 currentReferralCode = generateReferralCode();
            }
            await client.query(
                `UPDATE wallets SET external_withdrawal_address = $1, referral_code = $3, linked_at = NOW(), last_used_at = NOW() WHERE user_id = $2`,
                [externalAddress, userId, currentReferralCode]
            );
        }

        await client.query('COMMIT');
        console.log(`${logPrefix} External withdrawal address set/updated to ${externalAddress}`);

        // Update cache
        const cacheKey = `wallet-${userId}`;
        walletCache.set(cacheKey, { withdrawalAddress: externalAddress, referralCode: currentReferralCode, timestamp: Date.now() });
         setTimeout(() => {
             const current = walletCache.get(cacheKey);
             if (current && current.withdrawalAddress === externalAddress && Date.now() - current.timestamp >= WALLET_CACHE_TTL) {
                 walletCache.delete(cacheKey);
             }
         }, WALLET_CACHE_TTL + 1000);

        return { success: true, wallet: externalAddress };

    } catch (err) {
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { console.error(`${logPrefix} Rollback failed:`, rbErr); } }
        if (err instanceof Error && (err.message.includes('Invalid public key') || err.message.includes('Invalid address'))) {
            console.error(`${logPrefix} Invalid address format: ${externalAddress}`);
            return { success: false, error: 'Invalid wallet address format.' };
        }
         // Handle potential unique constraint violation on referral_code (very unlikely on update)
        if (err.code === '23505' && err.constraint === 'wallets_referral_code_key') {
             console.error(`${logPrefix} CRITICAL - Referral code generation conflict during update for user ${userId}.`);
             return { success: true, wallet: externalAddress }; // Allow update to succeed without code change, but log error
        }
        console.error(`${logPrefix} DB Error linking wallet:`, err.message, err.code);
        return { success: false, error: `Database error (${err.code || 'N/A'})` };
    } finally {
        if (client) client.release();
    }
}

// getUserWalletDetails: Fetches all details needed for /wallet, /referral (Unchanged from v2.7.0)
async function getUserWalletDetails(userId) { /* ... same as Part 2b (v2.7.0) ... */ userId = String(userId); const query = ` SELECT wallet_address as external_withdrawal_address, linked_at, last_used_at, referral_code, referred_by_user_id, referral_count, total_wagered, last_milestone_paid_lamports FROM wallets WHERE user_id = $1`; try { const res = await pool.query(query, [userId]); if (res.rows.length > 0) { const details = res.rows[0]; details.total_wagered = BigInt(details.total_wagered || '0'); details.last_milestone_paid_lamports = BigInt(details.last_milestone_paid_lamports || '0'); details.referral_count = parseInt(details.referral_count || '0', 10); return details; } return null; } catch (err) { console.error(`DB Error fetching wallet details for user ${userId}:`, err.message); return null; } }

// getLinkedWallet: Simplified - Gets only the external withdrawal address
async function getLinkedWallet(userId) {
    const cacheKey = `wallet-${userId}`;
    const cachedEntry = walletCache.get(cacheKey);
    // Use cached withdrawal address if available and not expired
    if (cachedEntry?.withdrawalAddress && Date.now() - cachedEntry.timestamp < WALLET_CACHE_TTL) {
        return cachedEntry.withdrawalAddress;
    } else if (cachedEntry) {
        // Don't delete entire entry if only address expired, referral code might still be valid
        // Let cache manage TTL
    }

    const query = `SELECT external_withdrawal_address, referral_code FROM wallets WHERE user_id = $1`;
    try {
        const res = await pool.query(query, [String(userId)]);
        const details = res.rows[0];
        if (details) {
            // Update cache with potentially fresh data
            walletCache.set(cacheKey, {
                 withdrawalAddress: details.external_withdrawal_address,
                 referralCode: details.referral_code, // Cache ref code too
                 timestamp: Date.now()
             });
             setTimeout(() => { /* Basic TTL Check */ const current = walletCache.get(cacheKey); if (current && current.timestamp && (Date.now() - current.timestamp >= WALLET_CACHE_TTL)) { walletCache.delete(cacheKey); } }, WALLET_CACHE_TTL + 1000);

            return details.external_withdrawal_address; // Return only the address
        }
        return undefined; // User or address not found/set
    } catch (err) {
        console.error(`DB Error fetching withdrawal wallet for user ${userId}:`, err.message);
        return undefined;
    }
}

// getUserByReferralCode: (Unchanged from v2.7.0)
async function getUserByReferralCode(refCode) { /* ... same as Part 3b (v2.7.0) ... */ if (!refCode || !/^ref_[a-f0-9]{8}$/i.test(refCode)) { return null; } try { const result = await pool.query('SELECT user_id FROM wallets WHERE referral_code = $1', [refCode]); return result.rows[0] || null; } catch (err) { console.error(`[DB getUserByReferralCode] Error finding user for code ${refCode}:`, err); return null; } }

// incrementReferralCount: (Unchanged from v2.7.0)
async function incrementReferralCount(referrerUserId) { /* ... same as Part 2b (v2.7.0) ... */ const query = ` UPDATE wallets SET referral_count = referral_count + 1 WHERE user_id = $1; `; try { const res = await pool.query(query, [String(referrerUserId)]); if (res.rowCount === 0) { console.warn(`[DB Referral] Attempted to increment referral count for non-existent user: ${referrerUserId}`); return false; } return true; } catch (err) { console.error(`[DB Referral] Error incrementing referral count for user ${referrerUserId}:`, err.message); return false; } }

// updateUserWagerStats: (Unchanged from v2.7.0 - Updates wallets table)
async function updateUserWagerStats(userId, wageredAmountLamports, newLastMilestonePaidLamports = null) { /* ... same as Part 2b (v2.7.0) ... */ let query; let values; const wageredAmount = BigInt(wageredAmountLamports); const lastMilestonePaid = newLastMilestonePaidLamports !== null ? BigInt(newLastMilestonePaidLamports) : null; if (lastMilestonePaid !== null) { query = ` UPDATE wallets SET total_wagered = total_wagered + $2, last_milestone_paid_lamports = $3 WHERE user_id = $1; `; values = [String(userId), wageredAmount, lastMilestonePaid]; } else { query = ` UPDATE wallets SET total_wagered = total_wagered + $2 WHERE user_id = $1; `; values = [String(userId), wageredAmount]; } try { const res = await pool.query(query, values); if (res.rowCount === 0) { console.warn(`[DB Referral] Attempted to update wager stats for non-existent user: ${userId}`); return false; } return true; } catch (err) { console.error(`[DB Referral] Error updating wager stats for user ${userId}:`, err.message); return false; } }

// --- Balance Ops ---

/**
 * Gets the current internal balance for a user. Creates balance record if needed.
 * @param {string} userId
 * @returns {Promise<bigint>} Current balance in lamports, or 0n if user/balance record not found.
 */
async function getUserBalance(userId) {
    userId = String(userId);
    const client = await pool.connect();
    try {
        await client.query('BEGIN');
        // Ensure wallet record exists first (or create it minimally)
        await client.query(
             `INSERT INTO wallets (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING`,
             [userId]
         );
         // Ensure balance record exists, return balance
        const balanceRes = await client.query(
            `INSERT INTO user_balances (user_id, balance_lamports) VALUES ($1, 0)
             ON CONFLICT (user_id) DO UPDATE SET user_id = EXCLUDED.user_id -- Dummy update to get RETURNING
             RETURNING balance_lamports`,
             [userId]
        );
        await client.query('COMMIT');
        return BigInt(balanceRes.rows[0]?.balance_lamports || '0');
    } catch (err) {
        console.error(`[DB GetBalance] Error fetching/creating balance for user ${userId}:`, err.message);
        await client.query('ROLLBACK');
        return 0n; // Return 0 on error
    } finally {
        client.release();
    }
}

/**
 * Atomically updates a user's balance and records the change in the ledger.
 * MUST be called within a DB transaction if used alongside other operations.
 * @param {pg.PoolClient} client - The active database client connection (from pool.connect()).
 * @param {string} userId
 * @param {bigint} changeAmountLamports - Positive for credit, negative for debit.
 * @param {string} transactionType - Type string for the ledger (e.g., 'deposit', 'bet_placed').
 * @param {number|null} [relatedBetId=null]
 * @param {number|null} [relatedDepositId=null]
 * @param {number|null} [relatedWithdrawalId=null]
 * @param {number|null} [relatedRefPayoutId=null]
 * @param {string|null} [notes=null]
 * @returns {Promise<{success: boolean, newBalance?: bigint, error?: string}>}
 */
async function updateUserBalance(client, userId, changeAmountLamports, transactionType, relatedBetId = null, relatedDepositId = null, relatedWithdrawalId = null, relatedRefPayoutId = null, notes = null) {
    userId = String(userId);
    const changeAmount = BigInt(changeAmountLamports);
    const logPrefix = `[UpdateBalance User ${userId}]`;

    try {
        // 1. Get current balance and lock row
        const balanceRes = await client.query(
            'SELECT balance_lamports FROM user_balances WHERE user_id = $1 FOR UPDATE',
            [userId]
        );

        if (balanceRes.rowCount === 0) {
             console.error(`${logPrefix} User balance record not found. Cannot update.`);
             // Attempt to create balance record minimally? Or fail? Let's fail cleanly.
             // await client.query(`INSERT INTO user_balances (user_id, balance_lamports) VALUES ($1, 0)`, [userId]);
             // return { success: false, error: 'User balance record not found, created zero balance.' };
             return { success: false, error: 'User balance record not found.' };
        }

        const balanceBefore = BigInt(balanceRes.rows[0].balance_lamports);
        const balanceAfter = balanceBefore + changeAmount;

        // 2. Check for negative balance only if debiting
        if (changeAmount < 0n && balanceAfter < 0n) {
            console.warn(`${logPrefix} Insufficient balance. Needed: ${-changeAmount}, Has: ${balanceBefore}`);
            return { success: false, error: 'Insufficient balance' };
        }

        // 3. Update balance
        const updateRes = await client.query(
            'UPDATE user_balances SET balance_lamports = $1, updated_at = NOW() WHERE user_id = $2',
            [balanceAfter, userId]
        );
        if (updateRes.rowCount === 0) {
            // Should not happen if FOR UPDATE lock succeeded, but check anyway
            console.error(`${logPrefix} Failed to update balance row after lock?`);
            return { success: false, error: 'Failed to update balance row' };
        }

        // 4. Insert into ledger
        const ledgerQuery = `
            INSERT INTO ledger (
                user_id, transaction_type, amount_lamports, balance_before, balance_after,
                related_bet_id, related_deposit_id, related_withdrawal_id, related_ref_payout_id, notes, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
        `;
        await client.query(ledgerQuery, [
            userId, transactionType, changeAmount, balanceBefore, balanceAfter,
            relatedBetId, relatedDepositId, relatedWithdrawalId, relatedRefPayoutId, notes
        ]);

        // console.log(`${logPrefix} Balance updated: ${balanceBefore} -> ${balanceAfter}. Type: ${transactionType}`); // Debug log
        return { success: true, newBalance: balanceAfter };

    } catch (err) {
        console.error(`${logPrefix} Error updating balance/ledger:`, err.message, err.code);
        // Don't rollback here, let the calling function handle transaction rollback
        return { success: false, error: `Database error during balance update (${err.code})` };
    }
}


// --- Deposit Ops ---

/**
 * Creates a new unique deposit address record for a user.
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
        const res = await pool.query(query, [String(userId), depositAddress, derivationPath, expiresAt]);
        if (res.rowCount > 0) {
            return { success: true, id: res.rows[0].id };
        } else {
             return { success: false, error: 'Failed to insert deposit address record.' };
        }
    } catch (err) {
         console.error(`[DB CreateDepositAddr] Error for user ${userId}:`, err.message, err.code);
         // Handle potential unique constraint violation on address (should be rare with good derivation)
         if (err.code === '23505' && err.constraint === 'deposit_addresses_deposit_address_key') {
             console.error(`[DB CreateDepositAddr] CRITICAL - Deposit address collision: ${depositAddress}`);
             return { success: false, error: 'Deposit address collision.' };
         }
         return { success: false, error: `Database error (${err.code || 'N/A'})` };
    }
}

/**
 * Finds user ID and status associated with a deposit address. Checks cache first.
 * @param {string} depositAddress
 * @returns {Promise<{userId: string, status: string, id: number, expiresAt: Date} | null>}
 */
async function findDepositAddressInfo(depositAddress) {
    const cacheKey = `depaddr-${depositAddress}`;
    const cached = activeDepositAddresses.get(cacheKey);
    const now = Date.now();

    if (cached && now < cached.expiresAt) {
        // Fetch fresh status from DB even if cache hit, as status might change
        try {
             const statusRes = await pool.query('SELECT status, id, expires_at FROM deposit_addresses WHERE deposit_address = $1', [depositAddress]);
             if (statusRes.rowCount > 0) {
                 return { userId: cached.userId, status: statusRes.rows[0].status, id: statusRes.rows[0].id, expiresAt: new Date(statusRes.rows[0].expires_at) };
             } else {
                 activeDepositAddresses.delete(cacheKey); // Remove if not found in DB
                 return null;
             }
        } catch (err) {
             console.error(`[DB FindDepositAddr] Error fetching status for cached addr ${depositAddress}:`, err.message);
             return null; // Error fetching status
        }
    } else if (cached) {
        activeDepositAddresses.delete(cacheKey); // Expired cache entry
    }

    // Not in cache or expired, query DB
    try {
        const res = await pool.query(
            'SELECT user_id, status, id, expires_at FROM deposit_addresses WHERE deposit_address = $1',
            [depositAddress]
        );
        if (res.rowCount > 0) {
            const data = res.rows[0];
            const expiresAtDate = new Date(data.expires_at);
            // Add to cache only if not expired and pending
            if (now < expiresAtDate.getTime() && data.status === 'pending') {
                 activeDepositAddresses.set(cacheKey, { userId: data.user_id, expiresAt: expiresAtDate.getTime() });
                  setTimeout(() => { activeDepositAddresses.delete(cacheKey); }, DEPOSIT_ADDR_CACHE_TTL); // Basic TTL cleanup
            }
            return { userId: data.user_id, status: data.status, id: data.id, expiresAt: expiresAtDate };
        }
        return null; // Not found
    } catch (err) {
        console.error(`[DB FindDepositAddr] Error finding info for addr ${depositAddress}:`, err.message);
        return null;
    }
}

/**
 * Marks a deposit address as used. To be called within a DB transaction.
 * @param {pg.PoolClient} client
 * @param {number} depositAddressId
 * @returns {Promise<boolean>}
 */
async function markDepositAddressUsed(client, depositAddressId) {
    try {
        const res = await client.query(
            `UPDATE deposit_addresses SET status = 'used' WHERE id = $1 AND status = 'pending'`, // Only update if pending
            [depositAddressId]
        );
        return res.rowCount > 0;
    } catch (err) {
         console.error(`[DB MarkDepositUsed] Error updating deposit address ID ${depositAddressId}:`, err.message);
         // Let calling transaction handle rollback
         return false;
    }
}

/**
 * Records a confirmed deposit transaction. To be called within a DB transaction.
 * @param {pg.PoolClient} client
 * @param {string} userId
 * @param {number} depositAddressId
 * @param {string} txSignature
 * @param {bigint} amountLamports
 * @returns {Promise<{success: boolean, depositId?: number}>}
 */
async function recordConfirmedDeposit(client, userId, depositAddressId, txSignature, amountLamports) {
    const query = `
        INSERT INTO deposits (user_id, deposit_address_id, tx_signature, amount_lamports, status, created_at, processed_at)
        VALUES ($1, $2, $3, $4, 'confirmed', NOW(), NOW())
        RETURNING id;
    `;
    try {
        const res = await client.query(query, [String(userId), depositAddressId, txSignature, BigInt(amountLamports)]);
        if (res.rowCount > 0) {
             return { success: true, depositId: res.rows[0].id };
        } else {
             console.error(`[DB RecordDeposit] Failed to insert deposit record for user ${userId}, TX ${txSignature}`);
             return { success: false };
        }
    } catch (err) {
         console.error(`[DB RecordDeposit] Error inserting deposit record for user ${userId}, TX ${txSignature}:`, err.message, err.code);
         // Handle unique constraint violation on signature
         if (err.code === '23505' && err.constraint === 'deposits_tx_signature_key') {
              console.error(`[DB RecordDeposit] Deposit TX ${txSignature} already processed.`);
         }
         // Let calling transaction handle rollback
         return { success: false };
    }
}


// --- Withdrawal Ops ---

/**
 * Creates a pending withdrawal request.
 * @param {string} userId
 * @param {bigint} requestedAmountLamports
 * @param {bigint} feeLamports
 * @param {string} recipientAddress
 * @returns {Promise<{success: boolean, withdrawalId?: number, error?: string}>}
 */
async function createWithdrawalRequest(userId, requestedAmountLamports, feeLamports, recipientAddress) {
    const requestedAmount = BigInt(requestedAmountLamports);
    const fee = BigInt(feeLamports);
    const finalSendAmount = requestedAmount; // Fee is deducted from balance, not send amount

    const query = `
        INSERT INTO withdrawals (user_id, requested_amount_lamports, fee_lamports, final_send_amount_lamports, recipient_address, status, created_at)
        VALUES ($1, $2, $3, $4, $5, 'pending', NOW())
        RETURNING id;
    `;
    try {
        const res = await pool.query(query, [String(userId), requestedAmount, fee, finalSendAmount, recipientAddress]);
        if (res.rowCount > 0) {
            return { success: true, withdrawalId: res.rows[0].id };
        } else {
             return { success: false, error: 'Failed to insert withdrawal request.' };
        }
    } catch (err) {
         console.error(`[DB CreateWithdrawal] Error for user ${userId}:`, err.message, err.code);
         return { success: false, error: `Database error (${err.code || 'N/A'})` };
    }
}

/**
 * Updates withdrawal status, signature, error message, timestamps. Usually called within a DB transaction.
 * @param {pg.PoolClient} client - Pass null if not in transaction (less safe)
 * @param {number} withdrawalId
 * @param {'processing' | 'completed' | 'failed'} status
 * @param {string | null} [signature=null]
 * @param {string | null} [errorMessage=null]
 * @returns {Promise<boolean>}
 */
async function updateWithdrawalStatus(client, withdrawalId, status, signature = null, errorMessage = null) {
    const db = client || pool; // Use client if provided, else pool directly
    let query = `UPDATE withdrawals SET status = $1`;
    const values = [status];
    let valueCounter = 2;

    if (status === 'processing') {
        query += `, processed_at = NOW(), completed_at = NULL, error_message = NULL`;
    } else if (status === 'completed') {
        if (!signature) { console.error(`[DB UpdateWithdrawal] Signature required for 'completed' status on withdrawal ID ${withdrawalId}`); return false; }
        query += `, payout_tx_signature = $${valueCounter++}, completed_at = NOW(), error_message = NULL, processed_at = COALESCE(processed_at, NOW())`;
        values.push(signature);
    } else if (status === 'failed') {
        query += `, error_message = $${valueCounter++}, completed_at = NULL, payout_tx_signature = NULL, processed_at = COALESCE(processed_at, NOW())`;
        values.push(errorMessage || 'Unknown error');
    } else {
        console.error(`[DB UpdateWithdrawal] Invalid status "${status}" for withdrawal ID ${withdrawalId}`);
        return false;
    }

    query += ` WHERE id = $${valueCounter++} RETURNING id;`;
    values.push(withdrawalId);

    try {
        const res = await db.query(query, values);
        return res.rowCount > 0;
    } catch (err) {
        console.error(`[DB UpdateWithdrawal] Error updating withdrawal ID ${withdrawalId} to status ${status}:`, err.message, err.code);
        if (err.code === '23505' && err.constraint === 'withdrawals_payout_tx_signature_key') {
             console.error(`[DB UpdateWithdrawal] CRITICAL - Withdrawal TX Signature ${signature?.slice(0,10)}... unique constraint violation for withdrawal ${withdrawalId}.`);
        }
        return false;
    }
}

/** Fetches withdrawal details for processing */
async function getWithdrawalDetails(withdrawalId) {
     try {
         const res = await pool.query('SELECT * FROM withdrawals WHERE id = $1', [withdrawalId]);
         return res.rows[0] || null;
     } catch (err) {
          console.error(`[DB GetWithdrawal] Error fetching withdrawal ${withdrawalId}:`, err.message);
          return null;
     }
}


// --- Bet Ops (Custodial) ---

/**
 * Creates a record for a bet placed using internal balance.
 * @param {string} userId
 * @param {string} chatId
 * @param {string} gameType
 * @param {object} betDetails - Game specific choices/bets
 * @param {bigint} wagerAmountLamports - Amount deducted from internal balance
 * @param {number} [priority=0]
 * @returns {Promise<{success: boolean, betId?: number, error?: string}>}
 */
async function createBetRecord(userId, chatId, gameType, betDetails, wagerAmountLamports, priority = 0) {
    const query = `
        INSERT INTO bets (user_id, chat_id, game_type, bet_details, wager_amount_lamports, status, created_at, priority)
        VALUES ($1, $2, $3, $4, $5, 'active', NOW(), $6)
        RETURNING id;
    `;
    try {
        const res = await pool.query(query, [String(userId), String(chatId), gameType, betDetails, BigInt(wagerAmountLamports), priority]);
        if (res.rowCount > 0) {
            return { success: true, betId: res.rows[0].id };
        } else {
            return { success: false, error: 'Failed to insert bet record.' };
        }
    } catch (err) {
        console.error(`[DB CreateBet] Error for user ${userId}, game ${gameType}:`, err.message);
        return { success: false, error: `Database error (${err.code || 'N/A'})` };
    }
}

/**
 * Updates a bet record upon completion of game logic. To be called within a DB transaction.
 * @param {pg.PoolClient} client
 * @param {number} betId
 * @param {'completed_win' | 'completed_loss' | 'completed_push'} status
 * @param {bigint | null} [payoutAmountLamports=null] - Amount credited back to internal balance (Wager + Win, or Wager for push)
 * @returns {Promise<boolean>}
 */
async function updateBetCompletion(client, betId, status, payoutAmountLamports = null) {
    // Validate status
    if (!['completed_win', 'completed_loss', 'completed_push'].includes(status)) {
         console.error(`[DB UpdateBetComplete] Invalid final status "${status}" for bet ${betId}`);
         return false;
    }
    // Ensure payout amount is provided for win/push
    if ((status === 'completed_win' || status === 'completed_push') && (payoutAmountLamports === null || BigInt(payoutAmountLamports) <= 0n)) {
         console.error(`[DB UpdateBetComplete] Invalid payout amount ${payoutAmountLamports} for status ${status} on bet ${betId}`);
         return false;
    }
     if (status === 'completed_loss') {
         payoutAmountLamports = 0n; // Ensure payout is zero for loss
     }

    const query = `
        UPDATE bets
        SET status = $1, payout_amount_lamports = $2, processed_at = NOW()
        WHERE id = $3 AND status = 'processing_game' -- Ensure it was being processed
        RETURNING id;
    `;
    try {
        const res = await client.query(query, [status, BigInt(payoutAmountLamports), betId]);
        if (res.rowCount === 0) {
            console.warn(`[DB UpdateBetComplete] Failed to update bet ${betId} to ${status}. Status might not have been 'processing_game'.`);
            // Check current status maybe?
            // const current = await client.query('SELECT status from bets where id=$1', [betId]);
            // console.warn(`Current status is ${current.rows[0]?.status}`);
            return false;
        }
        return true;
    } catch (err) {
         console.error(`[DB UpdateBetComplete] Error updating bet ${betId} to ${status}:`, err.message);
         return false;
    }
}

// isFirstCompletedBet: (Unchanged from v2.7.0)
async function isFirstCompletedBet(userId, betId) { /* ... same as Part 2b (v2.7.0) ... */ const query = ` SELECT id FROM bets WHERE user_id = $1 AND status LIKE 'completed_%' ORDER BY created_at ASC, id ASC LIMIT 1; `; try { const res = await pool.query(query, [String(userId)]); return res.rows.length > 0 && res.rows[0].id === betId; } catch (err) { console.error(`[DB Referral] Error checking first completed bet for user ${userId}, bet ${betId}:`, err.message); return false; } }


// --- Referral Payout Ops ---
// (Unchanged from v2.7.0)
async function recordPendingReferralPayout(referrerUserId, refereeUserId, payoutType, payoutAmountLamports, triggeringBetId = null, milestoneReachedLamports = null) { /* ... same as Part 2b (v2.7.0) ... */ const query = ` INSERT INTO referral_payouts ( referrer_user_id, referee_user_id, payout_type, payout_amount_lamports, triggering_bet_id, milestone_reached_lamports, status, created_at ) VALUES ($1, $2, $3, $4, $5, $6, 'pending', NOW()) RETURNING id; `; const values = [ String(referrerUserId), String(refereeUserId), payoutType, BigInt(payoutAmountLamports), triggeringBetId, milestoneReachedLamports !== null ? BigInt(milestoneReachedLamports) : null ]; try { const res = await pool.query(query, values); if (res.rows.length > 0) { console.log(`[DB Referral] Recorded pending ${payoutType} payout ID ${res.rows[0].id} for referrer ${referrerUserId} (triggered by ${refereeUserId}).`); return { success: true, payoutId: res.rows[0].id }; } else { console.error("[DB Referral] Failed to insert pending referral payout, RETURNING clause gave no ID."); return { success: false, error: "Failed to insert pending referral payout." }; } } catch (err) { console.error(`[DB Referral] Error recording pending ${payoutType} payout for referrer ${referrerUserId} (triggered by ${refereeUserId}):`, err.message); return { success: false, error: `Database error (${err.code || 'N/A'})` }; } }
async function updateReferralPayoutRecord(payoutId, status, signature = null, errorMessage = null) { /* ... same as Part 2b (v2.7.0) ... */ let query = `UPDATE referral_payouts SET status = $1`; const values = [status]; let valueCounter = 2; if (status === 'processing') { query += `, processed_at = NOW()`; } else if (status === 'paid') { if (!signature) { console.error(`[DB Referral Update] Signature required for 'paid' status on payout ID ${payoutId}`); return false; } query += `, payout_tx_signature = $${valueCounter++}, paid_at = NOW(), error_message = NULL`; values.push(signature); } else if (status === 'failed') { query += `, error_message = $${valueCounter++}, paid_at = NULL`; values.push(errorMessage || 'Unknown error'); } else { console.error(`[DB Referral Update] Invalid status "${status}" for payout ID ${payoutId}`); return false; } query += ` WHERE id = $${valueCounter++} RETURNING id;`; values.push(payoutId); try { const res = await pool.query(query, values); if (res.rowCount > 0) { return true; } else { console.warn(`[DB Referral Update] Failed to update referral payout ID ${payoutId} (maybe already updated or ID invalid?).`); return false; } } catch (err) { console.error(`[DB Referral Update] Error updating referral payout ID ${payoutId} to status ${status}:`, err.message, err.code); if (err.code === '23505' && err.constraint?.includes('payout_tx_signature')) { console.error(`[DB Referral Update] CRITICAL - Payout TX Signature ${signature?.slice(0,10)}... unique constraint violation for referral payout ${payoutId}.`); } return false; } }
async function getTotalReferralEarnings(userId) { /* ... same as Part 3b (v2.7.0) ... */ try { const result = await pool.query( `SELECT COALESCE(SUM(payout_amount_lamports), 0) AS total_earnings FROM referral_payouts WHERE referrer_user_id = $1 AND status = 'paid'`, [String(userId)] ); return BigInt(result.rows[0].total_earnings || '0'); } catch (err) { console.error(`[DB getTotalReferralEarnings] Error calculating earnings for user ${userId}:`, err); return 0n; } }


// --- Solana Transaction Analysis ---

// analyzeTransactionAmounts: Still needed for verifying deposit amounts (Unchanged)
function analyzeTransactionAmounts(tx, targetAddress) { /* ... same as Part 2b (v2.7.0) ... */ let transferAmount = 0n; let payerAddress = null; if (!tx || !targetAddress) { return { transferAmount: 0n, payerAddress: null }; } try { new PublicKey(targetAddress); } catch (e) { console.error(`[analyzeAmounts] Invalid targetAddress format: ${targetAddress}`); return { transferAmount: 0n, payerAddress: null }; } if (tx.meta?.err) { return { transferAmount: 0n, payerAddress: null }; } const getAccountKeysFromTx = (tx) => { try { if (!tx?.transaction?.message) return []; const message = tx.transaction.message; if (message.staticAccountKeys) { return message.staticAccountKeys.map(k => k.toBase58()); } else if (message.accountKeys) { return message.accountKeys.map(k => k instanceof PublicKey ? k.toBase58() : String(k)); } } catch (e) { console.error("[analyzeAmounts] Error parsing account keys:", e.message); } return []; }; let balanceChange = 0n; if (tx.meta?.preBalances && tx.meta?.postBalances && tx.transaction?.message) { const accountKeys = getAccountKeysFromTx(tx); const targetIndex = accountKeys.indexOf(targetAddress); if (targetIndex !== -1 && tx.meta.preBalances.length > targetIndex && tx.meta.postBalances.length > targetIndex && tx.meta.preBalances[targetIndex] !== null && tx.meta.postBalances[targetIndex] !== null) { balanceChange = BigInt(tx.meta.postBalances[targetIndex]) - BigInt(tx.meta.preBalances[targetIndex]); if (balanceChange > 0n) { transferAmount = balanceChange; const signers = tx.transaction.signatures?.length > 0 ? (tx.transaction.message.accountKeys || []).slice(0, tx.transaction.message.header?.numRequiredSignatures || 1) : []; const signerKeys = signers.map(s => s.toBase58 ? s.toBase58() : String(s)); for (const signerKey of signerKeys) { const signerIndex = accountKeys.indexOf(signerKey); if (signerIndex !== -1 && signerIndex !== targetIndex && tx.meta.preBalances.length > signerIndex && tx.meta.postBalances.length > signerIndex && tx.meta.preBalances[signerIndex] !== null && tx.meta.postBalances[signerIndex] !== null) { const preSignerBalance = BigInt(tx.meta.preBalances[signerIndex]); const postSignerBalance = BigInt(tx.meta.postBalances[signerIndex]); const signerBalanceChange = postSignerBalance - preSignerBalance; const fee = BigInt(tx.meta.fee || 0); const expectedDecrease = -(transferAmount + (signerIndex === 0 ? fee : 0n)); const tolerance = BigInt(100); if (signerBalanceChange <= expectedDecrease && signerBalanceChange >= expectedDecrease - tolerance) { payerAddress = signerKey; break; } else if (signerBalanceChange <= -transferAmount) { if (!payerAddress) payerAddress = signerKey; } } } if (!payerAddress && signerKeys.length > 0) { payerAddress = signerKeys[0]; } } } } let instructionTransferAmount = 0n; let instructionPayer = null; if (tx.transaction?.message) { const accountKeys = getAccountKeysFromTx(tx); const SYSTEM_PROGRAM_ID = SystemProgram.programId.toBase58(); const topLevelInstructions = tx.transaction.message.instructions || []; const innerInstructionsNested = tx.meta?.innerInstructions || []; const allInstructions = [ ...topLevelInstructions.map((inst, index) => ({ ...inst, type: 'topLevel', index })), ...innerInstructionsNested.flatMap(innerSet => (innerSet.instructions || []).map((inst, subIndex) => ({ ...inst, type: 'inner', parentIndex: innerSet.index, subIndex })) ) ]; for (const instWrapper of allInstructions) { const inst = instWrapper; let programId = ''; let parsedInfo = null; let isParsedTransfer = false; try { if ('programIdIndex' in inst && accountKeys.length > inst.programIdIndex) { programId = accountKeys[inst.programIdIndex]; } else if ('programId' in inst) { programId = typeof inst.programId === 'string' ? inst.programId : inst.programId?.toBase58(); } if (programId === SYSTEM_PROGRAM_ID) { if (inst.parsed?.type === 'transfer' || inst.parsed?.type === 'transferChecked') { isParsedTransfer = true; parsedInfo = inst.parsed.info; } } if (isParsedTransfer && parsedInfo?.destination === targetAddress) { const currentInstructionAmount = BigInt(parsedInfo.lamports || parsedInfo.amount || parsedInfo.tokenAmount?.amount || 0); if (currentInstructionAmount > 0n) { instructionTransferAmount += currentInstructionAmount; if (!instructionPayer && parsedInfo.source) { instructionPayer = parsedInfo.source; } } } } catch(parseError) { console.error(`[analyzeAmounts] Error processing instruction (${instWrapper.type} ${instWrapper.index ?? ''}/${instWrapper.subIndex ?? ''}): ${parseError.message}`); } } } if (transferAmount > 0n) { return { transferAmount, payerAddress: payerAddress ?? instructionPayer ?? null }; } else if (instructionTransferAmount > 0n) { console.log(`[analyzeAmounts] Using instruction analysis. Amount: ${instructionTransferAmount}, Payer: ${instructionPayer}`); return { transferAmount: instructionTransferAmount, payerAddress: instructionPayer }; } else { return { transferAmount: 0n, payerAddress: null }; } }


// --- Payment Processing System ---

// isRetryableError: Still needed for payout retries (Unchanged)
function isRetryableError(error) { /* ... same as Part 2b (v2.7.0) ... */ const msg = error?.message?.toLowerCase() || ''; const code = error?.code || error?.cause?.code; const status = error?.response?.status || error?.statusCode; if (status === 429 || msg.includes('429') || msg.includes('rate limit') || msg.includes('too many requests')) return true; if ([500, 502, 503, 504].includes(Number(status)) || msg.includes('server error') || msg.includes('internal server error')) return true; if (['timeout', 'timed out', 'econnreset', 'esockettimedout', 'network error', 'fetch', 'socket hang up', 'connection terminated', 'econnrefused', 'failed to fetch', 'getaddrinfo enotfound', 'connection refused'].some(m => msg.includes(m)) || ['ETIMEDOUT', 'ECONNRESET', 'ENETUNREACH', 'EAI_AGAIN', 'ECONNABORTED', 'ECONNREFUSED', 'UND_ERR_CONNECT_TIMEOUT', 'UND_ERR_HEADERS_TIMEOUT', 'UND_ERR_BODY_TIMEOUT', 'FETCH_TIMEOUT', 'FETCH_ERROR'].includes(code)) return true; if (msg.includes('connection terminated unexpectedly') || code === 'ECONNREFUSED' || code === '57P01' || code === '57P03') return true; if (msg.includes('transaction simulation failed') || msg.includes('failed to simulate transaction') || msg.includes('blockhash not found') || msg.includes('slot leader does not match') || msg.includes('node is behind') || msg.includes("processing transaction") || msg.includes("block not available") || msg.includes("sending transaction")) return true; if (error?.reason === 'rpc_error' || error?.reason === 'network_error') return true; return false; }


// **REMOVED**: GuaranteedPaymentProcessor class is removed.
// Payout jobs (Withdrawal, Referral) will be added directly to payoutProcessorQueue.
// Retry logic will be implemented around the call to the handler function.
// Referral trigger logic moved to happen after game completion / balance update.

// --- End of Part 2b ---
// index.js - Part 3a (Custodial Model with Buttons & Referrals)
// --- VERSION: 3.0.0 ---

// (Code continues directly from the end of Part 2b)

// --- Deposit Monitoring & Processing (Custodial) ---

// Placeholder for deposit transaction detection mechanism state
let depositMonitorRunning = false;
let depositMonitorIntervalId = null;
const activeDepositMonitors = new Map(); // Could store subscription IDs or similar

// ** NEW: Deposit Monitoring Loop (Conceptual) **
// WARNING: Efficiently monitoring thousands of addresses is complex.
// This is a simplified placeholder. Production needs indexers or advanced RPC methods.
async function monitorDeposits() {
    if (depositMonitorRunning) {
        // console.log("[DepositMonitor] Already running.");
        return;
    }
    if (!isFullyInitialized) return;

    depositMonitorRunning = true;
    // console.log("[DepositMonitor] Starting deposit check cycle...");
    const startTime = Date.now();
    let processedCount = 0;

    try {
        // --- Strategy Idea 1: Poll Pending Addresses (RPC Intensive) ---
        // 1. Fetch active, non-expired addresses from DB
        const pendingAddresses = await pool.query(
            `SELECT id, user_id, deposit_address, expires_at FROM deposit_addresses WHERE status = 'pending' AND expires_at > NOW()`
        ).then(res => res.rows);

        if (pendingAddresses.length === 0) {
            // console.log("[DepositMonitor] No pending addresses to check.");
            depositMonitorRunning = false;
            return;
        }
         console.log(`[DepositMonitor] Checking ${pendingAddresses.length} pending addresses...`);

        // 2. For each address, check for recent transactions (Example using getSignaturesForAddress - inefficient at scale)
        const fetchLimit = 5; // Check only a few recent TXs per address per cycle
        for (const addrInfo of pendingAddresses) {
             try {
                 const pubKey = new PublicKey(addrInfo.deposit_address);
                 const signatures = await solanaConnection.getSignaturesForAddress(pubKey, { limit: fetchLimit, commitment: 'confirmed' });

                 for (const sigInfo of signatures) {
                     if (!sigInfo.err && !processedSignaturesThisSession.has(sigInfo.signature)) {
                         // Found a potential new, successful deposit TX
                         console.log(`[DepositMonitor] Potential deposit found for address ${addrInfo.deposit_address.slice(0,6)}... TX: ${sigInfo.signature.slice(0,6)}...`);
                         // Add job to process this specific deposit
                         // Use messageQueue or a dedicated depositQueue
                         messageQueue.add(() => processDepositTransaction(sigInfo.signature, addrInfo.deposit_address))
                             .catch(e => console.error(`Error queueing deposit processing for ${sigInfo.signature}: ${e.message}`));
                         processedSignaturesThisSession.add(sigInfo.signature); // Add to cache to avoid reprocessing quickly
                         processedCount++;
                     }
                 }
                 // Small delay between checking addresses if needed
                 // await new Promise(r => setTimeout(r, 50));
             } catch (error) {
                  console.error(`[DepositMonitor] Error checking address ${addrInfo.deposit_address}: ${error.message}`);
                   performanceMonitor.logRequest(false);
             }
        }
        // --- End Strategy Idea 1 ---

        // --- Strategy Idea 2: Use WebSocket Subscriptions ---
        // Would involve `connection.onAccountChange` or `connection.onLogs` for each address
        // Requires managing subscriptions, state, and potential reconnection logic. More complex setup.

        // --- Strategy Idea 3: Use Indexer Service ---
        // Query an external indexer API for transactions sent to known pending addresses. Most scalable.

    } catch (error) {
        console.error("❌ Error during deposit monitoring cycle:", error);
        performanceMonitor.logRequest(false);
    } finally {
        depositMonitorRunning = false;
        const duration = Date.now() - startTime;
        if (processedCount > 0 || duration > 5000) {
             console.log(`[DepositMonitor] Cycle finished in ${duration}ms. Queued ${processedCount} potential deposits for processing.`);
        }
        // Schedule next run? (Handled by setInterval in start function)
    }
}

// ** NEW: Function to process a detected deposit transaction **
async function processDepositTransaction(signature, depositAddress) {
    const logPrefix = `[ProcessDeposit TX ${signature.slice(0, 6)}]`;
    console.log(`${logPrefix} Processing deposit to address ${depositAddress.slice(0,6)}...`);

    let client; // DB client for transaction
    let depositAddressId = null;
    let userId = null;

    try {
        // 1. Fetch and validate transaction
        const tx = await solanaConnection.getTransaction(signature, {
             maxSupportedTransactionVersion: 0,
             commitment: 'confirmed' // Use confirmed for balance checks later potentially
        });

        if (!tx) { throw new Error("Transaction not found or failed to fetch."); }
        if (tx.meta?.err) {
             console.log(`${logPrefix} Transaction failed on-chain. Ignoring.`);
             processedSignaturesThisSession.add(signature); // Ensure we don't retry failed TXs
             return; // Do not proceed
        }

        // Optional: Check confirmations (getTransaction doesn't easily provide this, may need getSignatureStatus)
        // const status = await solanaConnection.getSignatureStatus(signature, { searchTransactionHistory: true });
        // if (!status?.value?.confirmationStatus === 'finalized' && !status?.value?.confirmationStatus === 'confirmed') {
        //      throw new Error(`Transaction not yet confirmed/finalized. Status: ${status?.value?.confirmationStatus}`);
        // }

        // 2. Find associated user and validate deposit address status/expiry
        const addrInfo = await findDepositAddressInfo(depositAddress); // Uses cache/DB
        if (!addrInfo) { throw new Error(`No pending deposit address record found for ${depositAddress}.`); }

        depositAddressId = addrInfo.id; // Store for later use
        userId = addrInfo.userId;

        if (addrInfo.status !== 'pending') { throw new Error(`Deposit address ${depositAddress} already used or expired (status: ${addrInfo.status}). Ignoring TX.`); }
        if (new Date() > addrInfo.expiresAt) { throw new Error(`Deposit address ${depositAddress} has expired. Ignoring TX.`); }

        // 3. Analyze amount transferred to the unique address
        const { transferAmount } = analyzeTransactionAmounts(tx, depositAddress);
        if (transferAmount <= 0n) { throw new Error("No valid SOL transfer amount found to the deposit address."); }

        console.log(`${logPrefix} Confirmed ${Number(transferAmount) / LAMPORTS_PER_SOL} SOL deposit for user ${userId} to address ${depositAddress.slice(0,6)}...`);

        // 4. Process deposit within a DB transaction
        client = await pool.connect();
        await client.query('BEGIN');

        // 4a. Credit user's internal balance & Add ledger entry
        const balanceUpdateResult = await updateUserBalance(
            client,
            userId,
            transferAmount,
            'deposit', // transaction_type
            null, // relatedBetId
            null, // relatedDepositId will be set after insert below
            null, // relatedWithdrawalId
            null, // relatedRefPayoutId
            `Deposit TX: ${signature.slice(0, 10)}...` // notes
        );

        if (!balanceUpdateResult.success) {
            throw new Error(`Failed to update balance: ${balanceUpdateResult.error}`);
        }

        // 4b. Record the confirmed deposit
        const depositRecordResult = await recordConfirmedDeposit(
            client,
            userId,
            depositAddressId,
            signature,
            transferAmount
        );
        if (!depositRecordResult.success || !depositRecordResult.depositId) {
             throw new Error("Failed to record confirmed deposit in DB.");
        }

        // 4c. Update the ledger entry with the deposit ID
        await client.query(
             `UPDATE ledger SET related_deposit_id = $1 WHERE user_id = $2 AND transaction_type = 'deposit' AND amount_lamports = $3 ORDER BY created_at DESC LIMIT 1`,
             [depositRecordResult.depositId, userId, transferAmount]
         ); // Note: This assumes only one deposit is processed at a time per user, might need refinement

         // 4d. Mark deposit address as used
         const markedUsed = await markDepositAddressUsed(client, depositAddressId);
         if (!markedUsed) {
             // Should not happen if status was pending, indicates potential race condition or issue
             throw new Error(`Failed to mark deposit address ID ${depositAddressId} as used.`);
         }

         // --- Referral Linking on First Deposit ---
         const depositCountRes = await client.query('SELECT COUNT(*) as count FROM deposits WHERE user_id = $1', [userId]);
         const isFirstDeposit = parseInt(depositCountRes.rows[0].count, 10) === 1;

         if (isFirstDeposit) {
             console.log(`${logPrefix} First deposit detected for user ${userId}. Checking for pending referral...`);
             const pending = pendingReferrals.get(userId);
             const now = Date.now();
             if (pending && (now - pending.timestamp < PENDING_REFERRAL_TTL_MS)) {
                  const referrerUserId = pending.referrerUserId;
                  // Ensure referral code is generated if missing (should be done by getUserBalance already)
                  await client.query(`UPDATE wallets SET referral_code = COALESCE(referral_code, $2) WHERE user_id = $1`, [userId, generateReferralCode()]);
                  // Set the referred_by_user_id
                  await client.query('UPDATE wallets SET referred_by_user_id = $1 WHERE user_id = $2 AND referred_by_user_id IS NULL', [referrerUserId, userId]);
                  console.log(`${logPrefix} Linked user ${userId} to referrer ${referrerUserId}.`);
                  pendingReferrals.delete(userId); // Clean up map entry
                  // Optionally notify referrer?
             } else if (pending) {
                 console.log(`${logPrefix} Pending referral expired for user ${userId}.`);
                 pendingReferrals.delete(userId);
             }
              // Generate referral code if it wasn't generated via pending check
             await client.query(`UPDATE wallets SET referral_code = COALESCE(referral_code, $2) WHERE user_id = $1`, [userId, generateReferralCode()]);
         }
         // --- End Referral Linking ---

        await client.query('COMMIT');
        console.log(`${logPrefix} Successfully processed deposit and updated balance for user ${userId}.`);

        // 5. Notify User
        await safeSendMessage(userId, `✅ Deposit successful\\! ${escapeMarkdownV2((Number(transferAmount) / LAMPORTS_PER_SOL).toFixed(4))} SOL has been added to your balance\\.`, { parse_mode: 'MarkdownV2' });

        // Add signature to processed cache *after* successful commit
        processedSignaturesThisSession.add(signature);
        this._cleanSignatureCache(); // Assuming _cleanSignatureCache is defined elsewhere or added later

    } catch (error) {
        console.error(`${logPrefix} Error processing deposit: ${error.message}`);
        if (client) {
            try { await client.query('ROLLBACK'); } catch (rbErr) { console.error(`${logPrefix} Rollback failed:`, rbErr); }
        }
        // If we identified the user, maybe notify them of the failure?
        if (userId) {
            await safeSendMessage(userId, `⚠️ There was an issue processing your deposit \\(TX: \`${escapeMarkdownV2(signature)}\`\\)\\. Please contact support if your balance was not updated\\. Error: ${escapeMarkdownV2(error.message)}`, { parse_mode: 'MarkdownV2'});
        }
         // Add to processed cache even on failure if it's definitive (e.g., address used/expired/TX failed)
         if (error.message.includes('already used') || error.message.includes('expired') || error.message.includes('Transaction failed on-chain')) {
             processedSignaturesThisSession.add(signature);
             this._cleanSignatureCache();
         }
        performanceMonitor.logRequest(false);
    } finally {
        if (client) client.release();
    }
}

// --- SOL Sending Function ---
// (Unchanged from v2.7.0 - Handles 'referral' source)
async function sendSol(recipientPublicKey, amountLamports, payoutSource) { /* ... same as Part 3a (v2.7.0) ... */ const operationId = `sendSol-${payoutSource}-${Date.now().toString().slice(-6)}`; let recipientPubKey; try { recipientPubKey = (typeof recipientPublicKey === 'string') ? new PublicKey(recipientPublicKey) : recipientPublicKey; if (!(recipientPubKey instanceof PublicKey)) throw new Error("Invalid recipient public key type"); } catch (e) { console.error(`[${operationId}] ❌ ERROR: Invalid recipient address format: "${recipientPublicKey}". Error: ${e.message}`); throw new Error(`Invalid recipient address: ${e.message}`); } let amountToSend; try { amountToSend = BigInt(amountLamports); if (amountToSend <= 0n) { console.error(`[${operationId}] ❌ ERROR: Payout amount ${amountToSend} is zero or negative.`); throw new Error('Payout amount is zero or negative'); } } catch (e) { console.error(`[${operationId}] ❌ ERROR: Failed to convert input amountLamports '${amountLamports}' to BigInt. Error: ${e.message}`); throw new Error(`Invalid payout amount format: ${e.message}`); } let privateKeyEnvVar; let keyTypeForLog; if (payoutSource === 'race') { privateKeyEnvVar = 'RACE_BOT_PRIVATE_KEY'; keyTypeForLog = 'RACE'; } else if (payoutSource === 'referral') { privateKeyEnvVar = process.env.REFERRAL_PAYOUT_PRIVATE_KEY ? 'REFERRAL_PAYOUT_PRIVATE_KEY' : 'MAIN_BOT_PRIVATE_KEY'; keyTypeForLog = process.env.REFERRAL_PAYOUT_PRIVATE_KEY ? 'REFERRAL' : 'MAIN (Default for Referral)'; } else { privateKeyEnvVar = 'MAIN_BOT_PRIVATE_KEY'; keyTypeForLog = 'MAIN'; } const privateKey = process.env[privateKeyEnvVar]; if (!privateKey) { console.error(`[${operationId}] ❌ ERROR: Missing private key env var ${privateKeyEnvVar} for payout source ${payoutSource} (Key Type: ${keyTypeForLog}).`); throw new Error(`Missing private key for ${keyTypeForLog} payouts`); } const amountSOL = Number(amountToSend) / LAMPORTS_PER_SOL; const basePriorityFee = parseInt(process.env.PAYOUT_BASE_PRIORITY_FEE_MICROLAMPORTS, 10); const maxPriorityFee = parseInt(process.env.PAYOUT_MAX_PRIORITY_FEE_MICROLAMPORTS, 10); const currentPriorityFeeRate = parseFloat(process.env.PAYOUT_PRIORITY_FEE_RATE); if (isNaN(currentPriorityFeeRate) || currentPriorityFeeRate < 0) { console.warn(`[${operationId}] Invalid PAYOUT_PRIORITY_FEE_RATE in sendSol, using 0.0001.`); process.env.PAYOUT_PRIORITY_FEE_RATE = '0.0001'; } const calculatedFee = Math.floor(Number(amountToSend) * parseFloat(process.env.PAYOUT_PRIORITY_FEE_RATE)); let priorityFeeMicroLamports = Math.max(basePriorityFee, Math.min(calculatedFee, maxPriorityFee)); if (isNaN(priorityFeeMicroLamports)) { console.error(`[${operationId}] ❌ ERROR: NaN priority fee calc! base=${basePriorityFee}, max=${maxPriorityFee}, rate=${process.env.PAYOUT_PRIORITY_FEE_RATE}, amount=${amountToSend}, calc=${calculatedFee}, final=${priorityFeeMicroLamports}`); priorityFeeMicroLamports = basePriorityFee; } try { const payerWallet = Keypair.fromSecretKey(bs58.decode(privateKey)); const latestBlockhash = await solanaConnection.getLatestBlockhash('confirmed'); if (!latestBlockhash || !latestBlockhash.blockhash || !latestBlockhash.lastValidBlockHeight) { throw new Error('Failed to get valid latest blockhash object.'); } const transaction = new Transaction({ recentBlockhash: latestBlockhash.blockhash, feePayer: payerWallet.publicKey }); transaction.add( ComputeBudgetProgram.setComputeUnitPrice({ microLamports: priorityFeeMicroLamports }) ); transaction.add( SystemProgram.transfer({ fromPubkey: payerWallet.publicKey, toPubkey: recipientPubKey, lamports: amountToSend }) ); const signature = await sendAndConfirmTransaction( solanaConnection, transaction, [payerWallet], { commitment: 'confirmed', skipPreflight: false, preflightCommitment: 'confirmed', } ); console.log(`[${operationId}] SUCCESS! ✅ Sent ${amountSOL.toFixed(3)} SOL to ${recipientPubKey.toBase58()} using ${keyTypeForLog} key (${privateKeyEnvVar}). TX: ${signature.slice(0,10)}...`); return { success: true, signature }; } catch (error) { console.error(`[${operationId}] ❌ SEND FAILED using ${keyTypeForLog} key (${privateKeyEnvVar}). Error message:`, error.message); if (error.logs) { console.error(`[${operationId}] Simulation Logs:`); error.logs.slice(-10).forEach(log => console.error(`   -> ${log}`)); } const errorMsg = error.message.toLowerCase(); let returnError = error.message; if (errorMsg.includes('insufficient lamports') || errorMsg.includes('insufficient funds')) { returnError = `Insufficient funds in ${keyTypeForLog} payout wallet.`; } else if (errorMsg.includes('blockhash not found') || errorMsg.includes('block height exceeded')) { returnError = 'Transaction expired (blockhash invalid). Retryable.'; } else if (errorMsg.includes('transaction was not confirmed') || errorMsg.includes('timed out waiting')) { returnError = `Transaction confirmation timeout. May succeed later. Retryable.`;} else if (isRetryableError(error)) { returnError = `Temporary network/RPC error: ${error.message}. Retryable.`; } else { returnError = `Send/Confirm error: ${error.message}`; } error.retryable = isRetryableError(error); error.message = returnError; throw error; } }


// --- Game Processing Logic ---

// **REMOVED** processPaidBet function. Game logic is now invoked directly by command/callback handlers.

// --- Utility Functions ---

// getUserDisplayName (Unchanged)
async function getUserDisplayName(chat_id, user_id) { /* ... same as v2.7.0 ... */ try { const chatMember = await bot.getChatMember(chat_id, user_id); const user = chatMember.user; let name = user.first_name || `User_${String(user_id).slice(-4)}`; if(user.username) name = `@${user.username}`; return escapeMarkdownV2(name); } catch (e) { const fallbackName = `User_${String(user_id).slice(-4)}`; return escapeMarkdownV2(fallbackName); } }


// --- Game Logic Implementation (Rewritten for Custodial) ---

/**
 * Handles Coinflip game logic and internal balance updates.
 * @param {string} userId
 * @param {string} chatId
 * @param {bigint} wagerAmountLamports
 * @param {string} userChoice 'heads' or 'tails'
 * @param {number} betRecordId - The ID from the 'bets' table.
 * @returns {Promise<boolean>} True if game processing and DB updates were successful.
 */
async function handleCoinflipGame(userId, chatId, wagerAmountLamports, userChoice, betRecordId) {
    const logPrefix = `CF Bet ${betRecordId} (User ${userId})`;
    const cfEdge = parseFloat(process.env.CF_HOUSE_EDGE);

    // Determine result (with house edge)
    const houseAutoWins = Math.random() < cfEdge;
    let result;
    let win;
    if (houseAutoWins) {
        console.log(`${logPrefix}: House auto-win triggered (Edge: ${cfEdge*100}%).`);
        win = false;
        result = (userChoice === 'heads') ? 'tails' : 'heads'; // Force loss
    } else {
        result = (Math.random() < 0.5) ? 'heads' : 'tails';
        win = (result === userChoice);
    }

    let payoutLamports = 0n; // Amount to CREDIT back (includes original wager if win/push)
    let finalBetStatus = 'completed_loss';
    let ledgerType = 'bet_lost'; // Assuming loss initially
    let balanceChange = 0n; // Net change from this outcome (win = +wager, loss = 0, push = 0 relative to post-deduction)

    if (win) {
        payoutLamports = wagerAmountLamports * 2n; // Stake back + Winnings
        finalBetStatus = 'completed_win';
        ledgerType = 'bet_won';
        balanceChange = wagerAmountLamports; // Net gain is the wager amount
    }
    // No explicit push in Coinflip

    const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
    const displayName = await getUserDisplayName(chatId, userId);

    // Send result message first
    let resultMessage;
    if (win) {
         // ** MD ESCAPE & DECIMAL APPLIED ** - Escaped `\` `\` `!` `.` twice using toFixed(3)
         resultMessage = `🎉 ${displayName}, you won the coinflip\\! \\(Result: *${escapeMarkdownV2(result)}*\\)\n` +
                         `Payout: ${escapeMarkdownV2(payoutSOL.toFixed(3))} SOL added to your balance\\.`;
    } else {
         // ** MD ESCAPE APPLIED ** - Escaped `\` `\` `.` `!`
         resultMessage = `❌ ${displayName}, you lost the coinflip\\! \\(Result: *${escapeMarkdownV2(result)}*\\)\\. Better luck next time\\.`;
    }
    await safeSendMessage(chatId, resultMessage, { parse_mode: 'MarkdownV2' });


    // Update balance and bet status in DB transaction if win
    let client;
    try {
        if (balanceChange > 0n) { // Only need DB interaction for wins to credit balance
            client = await pool.connect();
            await client.query('BEGIN');

            // Credit balance
            const balanceUpdateResult = await updateUserBalance(client, userId, balanceChange, ledgerType, betRecordId);
            if (!balanceUpdateResult.success) {
                throw new Error(`Failed to credit winnings: ${balanceUpdateResult.error}`);
            }

            // Mark bet as completed
            const betUpdateResult = await updateBetCompletion(client, betRecordId, finalBetStatus, payoutLamports);
             if (!betUpdateResult) {
                throw new Error(`Failed to update bet status to ${finalBetStatus}.`);
            }

            await client.query('COMMIT');
            console.log(`${logPrefix} Completed as WIN. Balance credited.`);
        } else {
            // Loss - just update bet status (no balance change needed here)
            const betUpdateResult = await updateBetCompletion(null, betRecordId, finalBetStatus, 0n); // Use pool directly
             if (!betUpdateResult) {
                 // Log error but don't necessarily fail the whole flow if only status update failed
                 console.error(`${logPrefix} Failed to update bet status to ${finalBetStatus} after loss.`);
             } else {
                  console.log(`${logPrefix} Completed as LOSS.`);
             }
        }
        return true; // Indicate successful processing

    } catch (error) {
        console.error(`${logPrefix} Error processing game result/updating DB:`, error);
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { console.error("Rollback failed:", rbErr); } }
        // Attempt to mark bet as error if balance update failed
        await updateBetCompletion(null, betRecordId, 'error_game_logic'); // Use pool directly for final error status
        await safeSendMessage(chatId, `⚠️ Internal error processing coinflip result\\. Please contact support if your balance seems incorrect\\.`, { parse_mode: 'MarkdownV2'});
        return false; // Indicate failure
    } finally {
        if (client) client.release();
    }
}

// handleRaceGame, handleSlotsGame, handleRouletteGame, handleWarGame
// need similar rewrites:
// - Accept userId, chatId, wagerAmountLamports, details, betRecordId
// - Perform game logic
// - Calculate finalBetStatus, balanceChange, payoutLamports (amount credited back)
// - Send result message
// - If balanceChange > 0: Start DB TX, call updateUserBalance, call updateBetCompletion, Commit TX.
// - If balanceChange <= 0: Call updateBetCompletion outside TX (or start/commit simple TX).
// - Return true on success, false on failure (triggering referral checks happens based on this return)

// ** Placeholder Signatures - Implementation needs rewrite **
async function handleRaceGame(userId, chatId, wagerAmountLamports, chosenHorseName, betRecordId) {
     console.warn("handleRaceGame needs full rewrite for custodial model");
     // TODO: Implement race logic, calculate win/loss, update balance, update bet status
     await updateBetCompletion(null, betRecordId, 'error_game_logic'); // Mark as error for now
     return false;
 }
async function handleSlotsGame(userId, chatId, wagerAmountLamports, betRecordId) {
    console.warn("handleSlotsGame needs full rewrite for custodial model");
     // TODO: Implement slots logic, calculate win/loss, update balance, update bet status
     await updateBetCompletion(null, betRecordId, 'error_game_logic');
     return false;
 }
async function handleRouletteGame(userId, chatId, wagerAmountLamports, userBets, betRecordId) {
     console.warn("handleRouletteGame needs full rewrite for custodial model");
     // TODO: Implement roulette logic, calculate win/loss, update balance, update bet status
     await updateBetCompletion(null, betRecordId, 'error_game_logic');
     return false;
 }
async function handleWarGame(userId, chatId, wagerAmountLamports, betRecordId) {
     console.warn("handleWarGame needs full rewrite for custodial model");
     // TODO: Implement war logic, calculate win/loss/push, update balance, update bet status
     await updateBetCompletion(null, betRecordId, 'error_game_logic');
     return false;
 }


// --- End of Part 3a ---
// index.js - Part 3b (Custodial Model with Buttons & Referrals)
// --- VERSION: 3.0.0 ---

// (Code continues directly from the end of Part 3a)

// --- HTML Escape Function --- (Kept)
function escapeHtml(text) { /* ... same as v2.7.0 ... */ if (typeof text !== 'string') text = String(text); return text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#039;"); }

// --- Database Helper ---
/**
 * Gets the next available index for deriving a deposit address for a user.
 * @param {string} userId
 * @returns {Promise<number>} The next index (e.g., 0 for the first address).
 */
async function getNextDepositAddressIndex(userId) {
    // In a real implementation, this might involve tracking used indices more robustly.
    // For simplicity, we can query the count of existing addresses for the user.
    try {
        const res = await pool.query(
            'SELECT COUNT(*) as count FROM deposit_addresses WHERE user_id = $1',
            [String(userId)]
        );
        return parseInt(res.rows[0]?.count || '0', 10);
    } catch (err) {
        console.error(`[DB GetNextDepositIndex] Error fetching count for user ${userId}:`, err);
        // Fallback to 0 on error, potentially causing collisions if not handled carefully
        return 0;
    }
}

// --- Payout Job Handling (Replaces Payment Processor for Outgoing) ---

/**
 * Adds a job to the payout queue with retry logic.
 * @param {object} jobDetails - Contains type ('payout_withdrawal' or 'payout_referral') and other necessary data.
 * @param {number} maxRetries - Number of times to retry on failure.
 * @param {number} baseRetryDelayMs - Initial delay before first retry.
 */
async function addPayoutJob(jobDetails, maxRetries, baseRetryDelayMs) {
    payoutProcessorQueue.add(async () => {
        let attempt = 0;
        while (attempt <= maxRetries) {
            try {
                if (jobDetails.type === 'payout_withdrawal') {
                    await handleWithdrawalPayoutJob(jobDetails);
                } else if (jobDetails.type === 'payout_referral') {
                    await handleReferralPayoutJob(jobDetails);
                } else {
                    console.error(`[AddPayoutJob] Unknown job type: ${jobDetails.type}`);
                }
                return; // Success, exit loop
            } catch (error) {
                attempt++;
                const jobId = jobDetails.withdrawalId || jobDetails.payoutId || 'N/A';
                const isRetryable = isRetryableError(error);
                console.warn(`[PayoutJob ${jobDetails.type} ID:${jobId}] Attempt ${attempt}/${maxRetries + 1} failed. Error: ${error.message}. Retryable: ${isRetryable}`);

                if (!isRetryable || attempt > maxRetries) {
                    console.error(`[PayoutJob ${jobDetails.type} ID:${jobId}] Final attempt failed or error not retryable. Marking as failed.`);
                    // Final failure handling (update status in DB) should happen within the specific handlers ideally,
                    // but we can try a generic update here if the handler failed to do so before throwing.
                    try {
                         if (jobDetails.type === 'payout_withdrawal' && jobDetails.withdrawalId) {
                              await updateWithdrawalStatus(null, jobDetails.withdrawalId, 'failed', null, `Max retries exceeded or non-retryable error: ${error.message}`);
                         } else if (jobDetails.type === 'payout_referral' && jobDetails.payoutId) {
                              await updateReferralPayoutRecord(jobDetails.payoutId, 'failed', null, `Max retries exceeded or non-retryable error: ${error.message}`);
                         }
                    } catch (dbError) {
                        console.error(`[PayoutJob ${jobDetails.type} ID:${jobId}] Failed to mark job as failed in DB after final error: ${dbError.message}`);
                    }
                    // Optional: Notify admin/user about final failure
                    if (jobDetails.userId) {
                         const errorType = jobDetails.type === 'payout_withdrawal' ? 'Withdrawal' : 'Referral Payout';
                         await safeSendMessage(jobDetails.userId, `⚠️ Your ${errorType} could not be completed after multiple attempts\\. Please contact support\\. Error: ${escapeMarkdownV2(error.message)}`, { parse_mode: 'MarkdownV2' });
                    }
                    break; // Exit retry loop
                }

                // Calculate delay with jitter before retrying
                const delay = baseRetryDelayMs * Math.pow(2, attempt - 1) + Math.random() * 1000;
                console.log(`[PayoutJob ${jobDetails.type} ID:${jobId}] Retrying in ${Math.round(delay / 1000)}s...`);
                await new Promise(resolve => setTimeout(resolve, delay));
            }
        }
    }).catch(queueError => {
        const jobId = jobDetails.withdrawalId || jobDetails.payoutId || 'N/A';
        console.error(`[PayoutQueue] Error processing job ${jobDetails.type} ID:${jobId}:`, queueError.message);
        // Consider marking the job as failed in DB here too as a fallback
    });
}


/**
 * Handles withdrawal payout jobs.
 * @param {object} job Contains withdrawalId, userId.
 */
async function handleWithdrawalPayoutJob(job) {
    const { withdrawalId, userId } = job; // userId passed for notifications
    const logPrefix = `[WithdrawalJob ID:${withdrawalId}]`;
    let client;

    console.log(`${logPrefix} Starting processing...`);
    const details = await getWithdrawalDetails(withdrawalId);

    if (!details) {
        console.error(`${logPrefix} Withdrawal record not found.`);
        throw new Error("Withdrawal record not found."); // Throw to prevent retry
    }
    if (details.status !== 'pending' && details.status !== 'processing') {
        console.warn(`${logPrefix} Job started but status is already '${details.status}'. Skipping.`);
        return; // Don't process if already completed/failed
    }

    // Mark as processing
    await updateWithdrawalStatus(null, withdrawalId, 'processing');

    try {
        const recipient = details.recipient_address;
        const amountToSend = BigInt(details.final_send_amount_lamports);
        const requestedAmount = BigInt(details.requested_amount_lamports);
        const feeAmount = BigInt(details.fee_lamports);
        const totalDeduction = requestedAmount + feeAmount; // Amount to deduct from internal balance

        // Send the SOL
        const { success, signature } = await sendSol(recipient, amountToSend, 'withdrawal'); // Use specific source? Or 'main'? Let's use 'main' for now.

        if (success && signature) {
            console.log(`${logPrefix} On-chain send successful: ${signature}. Deducting internal balance...`);
            // ** CRUCIAL: Deduct internal balance AFTER successful send **
            client = await pool.connect();
            await client.query('BEGIN');

            // Update balance and add ledger entry
            const balanceUpdateResult = await updateUserBalance(
                client,
                details.user_id,
                -totalDeduction, // Negative amount for deduction
                'withdrawal',
                null, null, withdrawalId, null, // Link withdrawal ID
                `To: ${recipient.slice(0, 6)}... Fee: ${Number(feeAmount) / LAMPORTS_PER_SOL} SOL`
            );

            if (!balanceUpdateResult.success) {
                // This is bad: Sent SOL but couldn't deduct internally. Requires manual intervention/logging.
                console.error(`${logPrefix} CRITICAL ERROR: Sent ${amountToSend} lamports (TX: ${signature}) but failed to deduct internal balance for user ${details.user_id}. Reason: ${balanceUpdateResult.error}`);
                // Rollback DB changes for this attempt
                await client.query('ROLLBACK');
                // Mark withdrawal as failed with specific error? Or leave as processing for manual check? Let's mark failed.
                await updateWithdrawalStatus(null, withdrawalId, 'failed', null, `Internal balance deduction failed after successful send (TX: ${signature})`);
                // Notify Admin!
                // Do NOT throw error here, as the outer retry loop shouldn't retry this specific failure.
                return; // Exit processing for this job
            }

            // Update withdrawal record to completed
            const statusUpdated = await updateWithdrawalStatus(client, withdrawalId, 'completed', signature);
             if (!statusUpdated) {
                 // Also bad, sent and deducted, but couldn't mark withdrawal complete.
                  console.error(`${logPrefix} ERROR: Sent ${amountToSend} lamports (TX: ${signature}), deducted balance, but failed to mark withdrawal as completed.`);
                   // Commit balance change anyway? Or rollback? Rollback seems safer to force check.
                   await client.query('ROLLBACK');
                    await updateWithdrawalStatus(null, withdrawalId, 'failed', null, `Failed to mark completed after send/deduction (TX: ${signature})`);
                     // Notify Admin!
                     return; // Exit processing
             }

            await client.query('COMMIT');
            console.log(`${logPrefix} Successfully processed, balance deducted, record updated.`);

            // Notify user
             // ** MD ESCAPE & DECIMAL APPLIED **
             await safeSendMessage(details.user_id, `✅ Withdrawal complete\\! ${escapeMarkdownV2((Number(amountToSend) / LAMPORTS_PER_SOL).toFixed(4))} SOL sent to \`${escapeMarkdownV2(recipient)}\`\\.\nTX: [View](https://solscan.io/tx/${escapeMarkdownV2(signature)})`, { parse_mode: 'MarkdownV2', disable_web_page_preview: true });

        } else {
            // Should only happen if sendSol fails without throwing (unlikely)
            throw new Error("sendSol reported failure without throwing");
        }
    } catch (error) {
        // Error during sendSol or DB updates (if client wasn't acquired yet)
        console.error(`${logPrefix} Processing failed. Error: ${error.message}`);
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { /* ignore */ } }
        // Mark as failed - this re-throw will be caught by addPayoutJob retry logic
        // updateWithdrawalStatus will be called there on final failure.
        throw error; // Re-throw for retry logic in addPayoutJob
    } finally {
         if (client) client.release();
    }
}

// handleReferralPayoutJob: (Unchanged from v2.7.0 Part 3b - Already uses sendSol correctly)
async function handleReferralPayoutJob(job) { /* ... same as Part 3b (v2.7.0) ... */ const { payoutId, referrerUserId, refereeUserId, amount } = job; const logPrefix = `Ref Payout ID ${payoutId}`; const payoutAmount = BigInt(amount); const payoutSOL = (Number(payoutAmount) / LAMPORTS_PER_SOL); try { const recipientAddress = await getLinkedWallet(referrerUserId); if (!recipientAddress) { console.error(`${logPrefix}: Cannot process payout. Referrer user ${referrerUserId} has no linked wallet.`); await updateReferralPayoutRecord(payoutId, 'failed', null, 'Referrer has no linked wallet'); return; } const statusUpdated = await updateReferralPayoutRecord(payoutId, 'processing'); if (!statusUpdated) { const current = await pool.query('SELECT status FROM referral_payouts WHERE id = $1', [payoutId]).then(r=>r.rows[0]); if (current && current.status !== 'pending' && current.status !== 'processing') { console.warn(`${logPrefix}: Failed update to 'processing', but current status is '${current.status}'. Aborting.`); return; } console.error(`${logPrefix}: CRITICAL! Failed update status to 'processing'. Aborting.`); throw new Error("Failed update status to processing"); } console.log(`${logPrefix}: Attempting send ${payoutSOL.toFixed(5)} SOL to referrer ${referrerUserId} (${recipientAddress.slice(0,6)}...)`); const { success, signature } = await sendSol(recipientAddress, payoutAmount, 'referral'); if (success && signature) { const recorded = await updateReferralPayoutRecord(payoutId, 'paid', signature); if (!recorded) { console.error(`${logPrefix}: CRITICAL ERROR - Referral payment sent (TX: ${signature}) but FAILED to record 'paid' status! Manual check needed for payout ID ${payoutId}.`); } else { console.log(`${logPrefix}: SUCCESS! ✅ Referral payout complete. TX: ${signature}`); try { const referrerDisplayName = await getUserDisplayName(referrerUserId, referrerUserId); await safeSendMessage(referrerUserId, `💰 You received a referral reward of ${escapeMarkdownV2(payoutSOL.toFixed(3))} SOL\\! Check your linked wallet\\.`, { parse_mode: 'MarkdownV2' }); } catch (notifyError) { console.warn(`${logPrefix}: Failed to send referral payout notification to user ${referrerUserId}: ${notifyError.message}`); } } } else { console.error(`${logPrefix}: sendSol returned false without throwing. Marking failed.`); await updateReferralPayoutRecord(payoutId, 'failed', null, 'sendSol returned false'); throw new Error("sendSol returned false without throwing"); } } catch (error) { console.error(`${logPrefix}: Referral payout failed. Error: ${error.message}`); throw error; } }


// --- Telegram Bot Error Handlers --- (Unchanged)
bot.on('polling_error', (error) => { /* ... same error handling ... */ });
bot.on('webhook_error', (error) => { /* ... same error handling ... */ });
bot.on('error', (error) => { console.error('❌ General Bot Error:', error); performanceMonitor.logRequest(false); });


// --- Main Message Handler (Custodial + Buttons) ---
async function handleMessage(msg) {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const messageText = msg.text || ''; // Default to empty string if no text
    const messageId = msg.message_id;
    const logPrefix = `Msg ${messageId} | User ${userId} | Chat ${chatId}`;

    // Ignore messages from bots
    if (msg.from.is_bot) return;

    // Cooldown Check (Apply to commands AND potential state-based text input)
    const now = Date.now();
    if (confirmCooldown.has(userId)) {
        const lastTime = confirmCooldown.get(userId);
        if (now - lastTime < cooldownInterval) return; // Silently ignore if cooling down
    }
    confirmCooldown.set(userId, now);
    setTimeout(() => { if (confirmCooldown.get(userId) === now) confirmCooldown.delete(userId); }, cooldownInterval);

    try {
        // --- Check for Conversational State ---
        const currentState = userStateCache.get(userId);
        if (currentState && messageText && !messageText.startsWith('/')) { // User sent text while in a state
             if (now - currentState.timestamp > USER_STATE_TTL) {
                 console.log(`${logPrefix} State cache expired for state ${currentState.state}. Clearing.`);
                 userStateCache.delete(userId);
                 await safeSendMessage(chatId, "Your previous action timed out\\. Please start again\\.", { parse_mode: 'MarkdownV2' });
                 return;
             }

             // Handle text input based on state
             switch (currentState.state) {
                 case 'awaiting_deposit_amount': // Example state if deposit asks for amount
                 case 'awaiting_withdrawal_amount':
                 case 'awaiting_cf_amount':
                 case 'awaiting_war_amount':
                 case 'awaiting_slots_amount':
                 case 'awaiting_race_amount':
                 case 'awaiting_roulette_bets': // Roulette might need more complex state
                     // Process the amount/bet text provided
                     await handleAmountOrBetInput(msg, currentState);
                     return; // Stop further processing, amount handler takes over
                 case 'awaiting_withdrawal_address':
                      await handleWithdrawalAddressInput(msg, currentState);
                      return;
                 // Add other states as needed
                 default:
                      console.log(`${logPrefix} User in unhandled state '${currentState.state}' received text: ${messageText}`);
                      // Maybe clear state or ignore? Let's clear and tell user to use commands.
                      userStateCache.delete(userId);
                      await safeSendMessage(chatId, "Hmm, I wasn't expecting that\\. Please use a command like /help or select an option\\.", { parse_mode: 'MarkdownV2' });
                      return;
             }
        }
        // --- End State Check ---

        // --- Command Processing ---
        const commandMatch = messageText.trim().match(/^\/(\w+)(?:@\w+)?(?:\s+(.*))?$/s);
        if (!commandMatch) {
             // If not in a state and not a command, ignore in groups, maybe reply in private?
             if (msg.chat.type === 'private') {
                  await safeSendMessage(chatId, "Please use a command or select an option\\. Try /help", { parse_mode: 'MarkdownV2' });
             }
             return;
        }

        const command = commandMatch[1].toLowerCase();
        const args = commandMatch[2]?.trim() || '';

        // Clear state if user issues a new command
        if (userStateCache.has(userId)) {
             console.log(`${logPrefix} New command '/${command}' received, clearing previous state '${userStateCache.get(userId)?.state}'.`);
             userStateCache.delete(userId);
        }


        // --- /start Referral Code Parsing ---
        if (command === 'start') {
            const refCodeMatch = args.match(/^(ref_[a-f0-9]{8})$/i);
            if (refCodeMatch && refCodeMatch[1]) {
                const refCode = refCodeMatch[1];
                // console.log(`${logPrefix}: User started with referral code ${refCode}`);
                const existingWallet = await getUserWalletDetails(userId); // Check if user exists using detailed fetch
                if (!existingWallet) { // User is potentially new
                    const referrer = await getUserByReferralCode(refCode);
                    if (referrer && referrer.user_id !== userId) {
                        pendingReferrals.set(userId, { referrerUserId: referrer.user_id, timestamp: Date.now() });
                        await safeSendMessage(chatId, `Welcome\\! Referral code \`${escapeMarkdownV2(refCode)}\` accepted\\. Your first deposit will link the referral\\.`, { parse_mode: 'MarkdownV2' });
                        console.log(`${logPrefix}: Pending referral stored for user ${userId} referred by ${referrer.user_id}`);
                    } else if (referrer && referrer.user_id === userId) {
                         await safeSendMessage(chatId, `Welcome\\! You can't use your own referral code\\!`, { parse_mode: 'MarkdownV2' });
                    } else {
                         await safeSendMessage(chatId, `Welcome\\! The referral code \`${escapeMarkdownV2(refCode)}\` seems invalid\\.`, { parse_mode: 'MarkdownV2' });
                    }
                } else {
                    await safeSendMessage(chatId, `Welcome back\\! Referral codes only apply to new users before their first deposit\\.`, { parse_mode: 'MarkdownV2' });
                }
                 // Fall through to execute normal /start handler anyway
            }
        }
        // --- End /start Referral Parsing ---


        // --- Command Handler Map (v3.0.0) ---
        const commandHandlers = {
            'start': handleStartCommand,        // Displays welcome/main menu buttons
            'help': handleHelpCommand,          // Displays help text
            'wallet': handleWalletCommand,      // Displays balance/withdrawal address/ref info, or links withdrawal address
            'referral': handleReferralCommand,  // Displays referral stats/link
            'deposit': handleDepositCommand,    // Initiates deposit flow
            'withdraw': handleWithdrawCommand,  // Initiates withdrawal flow

            // Game Commands (Initiate the game flow, usually asking for amount)
            'coinflip': handleCoinflipCommand,
            'race': handleRaceCommand,
            'slots': handleSlotsCommand,
            'roulette': handleRouletteCommand,
            'war': handleWarCommand,

            'admin': handleAdminCommand,
            // Old /bet... commands are removed
        };

        const handler = commandHandlers[command];

        if (handler) {
            // Admin Check
            if (command === 'admin') {
                const adminIds = (process.env.ADMIN_USER_IDS || '').split(',').map(id=>id.trim()).filter(id => id);
                if (adminIds.includes(userId)) { await handler(msg, args); }
            } else {
                 await handler(msg, args); // Pass args for commands that might use them (wallet, admin, withdraw)
                 performanceMonitor.logRequest(true);
            }
        } else {
             if (msg.chat.type === 'private') {
                  await safeSendMessage(chatId, `❓ Unknown command: \`/${escapeMarkdownV2(command)}\`\\. Try /help`, { parse_mode: 'MarkdownV2' });
             }
        }
    } catch (error) {
        console.error(`${logPrefix}: Error processing message/command "/${command || 'UNKNOWN'}" ("${messageText}"):`, error);
        performanceMonitor.logRequest(false);
        userStateCache.delete(userId); // Clear state on error
        try { await safeSendMessage(chatId, "⚠️ An unexpected error occurred\\. Please try again or contact support\\.", { parse_mode: 'MarkdownV2'}); }
        catch (tgError) { /* ignore */ }
    }
}


// --- Callback Query Handler (Handles Button Presses) ---
async function handleCallbackQuery(callbackQuery) {
    const userId = String(callbackQuery.from.id);
    const chatId = callbackQuery.message.chat.id;
    const messageId = callbackQuery.message.message_id;
    const callbackData = callbackQuery.data;
    const logPrefix = `Callback ${callbackQuery.id} | User ${userId} | Chat ${chatId}`;

    // Always answer the callback query quickly to remove loading state
    // Can add text/alert later if needed within specific handlers
    await bot.answerCallbackQuery(callbackQuery.id).catch(e => console.warn(`${logPrefix} Failed to answer callback query: ${e.message}`));

    console.log(`${logPrefix} Received callback data: ${callbackData}`); // Debug log

    try {
        const currentState = userStateCache.get(userId);
        const now = Date.now();

        // Check for expired state if applicable
         if (currentState && now - currentState.timestamp > USER_STATE_TTL) {
             console.log(`${logPrefix} State cache expired processing callback. State: ${currentState.state}`);
             userStateCache.delete(userId);
             await bot.editMessageText("Action timed out. Please start again.", { chat_id: chatId, message_id: messageId, reply_markup: undefined }).catch(()=>{}); // Try removing buttons
             return;
         }

        // --- Route callback data ---
        // Example: Data format could be "action:value" or just "action"
        const [action, value] = callbackData.split(':');

        switch (action) {
            // --- Coinflip Example Flow ---
            case 'cf_choice': // User chose Heads or Tails
                if (!currentState || currentState.state !== 'awaiting_cf_choice' || !currentState.data?.amountLamports) {
                     throw new Error("Invalid state for coinflip choice.");
                }
                const choice = value; // 'heads' or 'tails'
                const cfWager = BigInt(currentState.data.amountLamports);

                // Edit message to show processing
                await bot.editMessageText(`🪙 Coinflip: You chose *${escapeMarkdownV2(choice)}*\\. Flipping...`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: undefined });
                userStateCache.delete(userId); // Clear state before async game logic

                // Start DB transaction for bet placement (deduct balance)
                let client;
                let betRecordId = null;
                let betPlacedSuccess = false;
                try {
                    client = await pool.connect();
                    await client.query('BEGIN');
                    const balanceUpdateResult = await updateUserBalance(client, userId, -cfWager, 'bet_placed', null, null, null, null, `Coinflip ${choice}`);
                    if (!balanceUpdateResult.success) { throw new Error(balanceUpdateResult.error || 'Insufficient balance or DB error placing bet.'); }
                    // Create bet record
                    const betRecordResult = await createBetRecord(userId, String(chatId), 'coinflip', { choice: choice }, cfWager);
                    if (!betRecordResult.success) { throw new Error(betRecordResult.error || 'Failed to create bet record.'); }
                    betRecordId = betRecordResult.betId;
                    await client.query('COMMIT');
                    betPlacedSuccess = true;
                } catch (betError) {
                    if (client) await client.query('ROLLBACK');
                    throw betError; // Re-throw to be caught below
                } finally {
                    if (client) client.release();
                }

                // If bet placement succeeded, run game logic
                if (betPlacedSuccess && betRecordId) {
                    const gameSuccess = await handleCoinflipGame(userId, String(chatId), cfWager, choice, betRecordId);
                    // --- Trigger Referral Checks after game completion ---
                    if (gameSuccess) {
                        // Need to fetch the completed bet details to check status and amount
                         const finalBet = await pool.query('SELECT status, wager_amount_lamports FROM bets WHERE id=$1', [betRecordId]).then(r => r.rows[0]);
                         if (finalBet?.status?.startsWith('completed_')) {
                             // Update wager stats & check milestones
                             await paymentProcessor._handleWagerUpdateAndMilestones(userId, BigInt(finalBet.wager_amount_lamports)); // Use the payment processor's helper
                             // Check for first bet bonus
                             const isFirst = await isFirstCompletedBet(userId, betRecordId);
                             if (isFirst) {
                                 await paymentProcessor._handleFirstBetReferral(userId, betRecordId, BigInt(finalBet.wager_amount_lamports)); // Use helper
                             }
                         } else {
                              console.warn(`${logPrefix} Game handler reported success, but final bet status is not 'completed_'. Status: ${finalBet?.status}`);
                         }
                    }
                     // Result message is sent by handleCoinflipGame now, message edit happens there too ideally
                     // We already edited to "Flipping...", maybe handleCoinflipGame edits again? Or we edit here?
                     // Let's assume handleCoinflipGame sends the final result message.
                }
                break;

            // --- Withdrawal Example Flow ---
            case 'withdraw_confirm':
                if (!currentState || currentState.state !== 'awaiting_withdrawal_confirm' || !currentState.data?.amountLamports || !currentState.data?.recipientAddress) {
                    throw new Error("Invalid state for withdrawal confirmation.");
                }
                const { amountLamports: withdrawAmount, recipientAddress: withdrawAddress } = currentState.data;
                const fee = WITHDRAWAL_FEE_LAMPORTS;
                const finalSendAmount = BigInt(withdrawAmount); // Fee is deducted internally
                const totalDeduction = finalSendAmount + fee;

                console.log(`${logPrefix} User confirmed withdrawal of ${Number(finalSendAmount) / LAMPORTS_PER_SOL} SOL to ${withdrawAddress}`);
                userStateCache.delete(userId); // Clear state

                // Edit message
                 await bot.editMessageText(`Processing withdrawal request...`, { chat_id: chatId, message_id: messageId, reply_markup: undefined }).catch(()=>{});

                 // Check balance again just before queueing (though deduction happens after send)
                 const currentBalance = await getUserBalance(userId);
                 if (currentBalance < totalDeduction) {
                     await safeSendMessage(chatId, `⚠️ Withdrawal failed\\. Insufficient balance \\(${escapeMarkdownV2((Number(currentBalance) / LAMPORTS_PER_SOL).toFixed(4))} SOL\\) for amount \\+ fee\\.`, { parse_mode: 'MarkdownV2' });
                     break;
                 }

                // Create withdrawal record
                const wdRequest = await createWithdrawalRequest(userId, finalSendAmount, fee, withdrawAddress);
                if (!wdRequest.success || !wdRequest.withdrawalId) {
                     await safeSendMessage(chatId, `⚠️ Failed to initiate withdrawal request\\. Please try again or contact support\\. Error: ${escapeMarkdownV2(wdRequest.error || 'DB Error')}`, { parse_mode: 'MarkdownV2' });
                     break;
                }

                // Queue the payout job using the new helper
                await addPayoutJob(
                    { type: 'payout_withdrawal', withdrawalId: wdRequest.withdrawalId, userId: userId },
                    parseInt(process.env.PAYOUT_JOB_RETRIES, 10),
                    parseInt(process.env.PAYOUT_JOB_RETRY_DELAY_MS, 10)
                );

                await safeSendMessage(chatId, `✅ Withdrawal request submitted\\. You will be notified upon completion\\.`, { parse_mode: 'MarkdownV2' });
                break;

            case 'cancel_action': // Generic cancel
                 console.log(`${logPrefix} User cancelled action.`);
                 userStateCache.delete(userId);
                 await bot.editMessageText("Action cancelled.", { chat_id: chatId, message_id: messageId, reply_markup: undefined }).catch(()=>{});
                 break;

            // Add cases for Race choice, Slots confirm, Roulette confirm/bets, War confirm etc.

            default:
                console.warn(`${logPrefix} Received unknown callback action: ${action}`);
                // Optionally send a message? "Unknown button pressed"
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

// --- Text Input Handlers (Called by handleMessage based on state) ---

async function handleAmountOrBetInput(msg, currentState) {
    const userId = String(msg.from.id);
    const chatId = msg.chat.id;
    const text = msg.text.trim();
    const logPrefix = `[Input State:${currentState.state}] User ${userId}`;

    // Example: Coinflip Amount Handling
    if (currentState.state === 'awaiting_cf_amount') {
        const amountSOL = parseFloat(text);
        const gameCfg = GAME_CONFIG.coinflip;
        if (isNaN(amountSOL) || amountSOL <= 0) {
            await safeSendMessage(chatId, "⚠️ Invalid amount\\. Please enter a positive number\\.", { parse_mode: 'MarkdownV2' });
            return; // Keep user in same state
        }
        const amountLamports = BigInt(Math.round(amountSOL * LAMPORTS_PER_SOL));

        if (amountLamports < gameCfg.minBetLamports || amountLamports > gameCfg.maxBetLamports) {
            const min = (Number(gameCfg.minBetLamports) / LAMPORTS_PER_SOL).toFixed(3);
            const max = (Number(gameCfg.maxBetLamports) / LAMPORTS_PER_SOL).toFixed(3);
             await safeSendMessage(chatId, `⚠️ Bet amount must be between ${escapeMarkdownV2(min)} and ${escapeMarkdownV2(max)} SOL\\. Please enter a valid amount\\.`, { parse_mode: 'MarkdownV2' });
            return; // Keep user in same state
        }

        // Check balance
        const currentBalance = await getUserBalance(userId);
        if (currentBalance < amountLamports) {
            await safeSendMessage(chatId, `⚠️ Insufficient balance \\(${escapeMarkdownV2((Number(currentBalance) / LAMPORTS_PER_SOL).toFixed(4))} SOL\\) to place that bet\\. Use /deposit to add funds\\.`, { parse_mode: 'MarkdownV2' });
            userStateCache.delete(userId); // Clear state
            return;
        }

        // Update state to await choice, store amount
        userStateCache.set(userId, {
            state: 'awaiting_cf_choice',
            chatId: chatId,
            data: { amountLamports: amountLamports.toString() }, // Store as string for safety
            timestamp: Date.now()
        });

        // Send message with buttons
        const options = {
            reply_markup: {
                inline_keyboard: [
                    [
                        { text: 'Heads', callback_data: 'cf_choice:heads' },
                        { text: 'Tails', callback_data: 'cf_choice:tails' }
                    ],
                    [ { text: 'Cancel', callback_data: 'cancel_action' } ]
                ]
            },
            parse_mode: 'MarkdownV2'
        };
        await safeSendMessage(chatId, `🪙 Betting ${escapeMarkdownV2(amountSOL.toFixed(3))} SOL\\. Choose Heads or Tails:`, options);
    }
    // Add handlers for War amount, Slots amount, Race amount, Roulette bets input...
     else if (currentState.state === 'awaiting_withdrawal_amount') {
         const amountSOL = parseFloat(text);
         if (isNaN(amountSOL) || amountSOL <= 0) { await safeSendMessage(chatId, "⚠️ Invalid amount\\. Please enter a positive number\\.", { parse_mode: 'MarkdownV2' }); return; }
         const amountLamports = BigInt(Math.round(amountSOL * LAMPORTS_PER_SOL));
         if (amountLamports < MIN_WITHDRAWAL_LAMPORTS) { await safeSendMessage(chatId, `⚠️ Minimum withdrawal amount is ${escapeMarkdownV2((Number(MIN_WITHDRAWAL_LAMPORTS) / LAMPORTS_PER_SOL).toFixed(4))} SOL\\.`, { parse_mode: 'MarkdownV2' }); return; }
         const fee = WITHDRAWAL_FEE_LAMPORTS;
         const totalNeeded = amountLamports + fee;
         const currentBalance = await getUserBalance(userId);
         if (currentBalance < totalNeeded) { await safeSendMessage(chatId, `⚠️ Insufficient balance \\(${escapeMarkdownV2((Number(currentBalance) / LAMPORTS_PER_SOL).toFixed(4))} SOL\\) for withdrawal amount \\+ fee \\(${escapeMarkdownV2((Number(totalNeeded) / LAMPORTS_PER_SOL).toFixed(4))} SOL\\)\\.`, { parse_mode: 'MarkdownV2' }); userStateCache.delete(userId); return; }

         // Update state, ask for address
         userStateCache.set(userId, { state: 'awaiting_withdrawal_address', chatId: chatId, data: { amountLamports: amountLamports.toString() }, timestamp: Date.now() });
         await safeSendMessage(chatId, `Please enter the *Solana wallet address* you want to withdraw to:`, { parse_mode: 'MarkdownV2' });
     }
     // ... other state handlers ...
     else {
         console.warn(`${logPrefix} Text received in unhandled input state: ${currentState.state}`);
         userStateCache.delete(userId); // Clear unexpected state
     }
}

async function handleWithdrawalAddressInput(msg, currentState) {
     const userId = String(msg.from.id);
     const chatId = msg.chat.id;
     const address = msg.text.trim();
     const logPrefix = `[Input State:awaiting_withdrawal_address] User ${userId}`;

     if (!currentState || currentState.state !== 'awaiting_withdrawal_address' || !currentState.data?.amountLamports) {
          console.error(`${logPrefix} Invalid state reached.`);
          userStateCache.delete(userId);
          await safeSendMessage(chatId, `⚠️ An error occurred\\. Please start the withdrawal again with /withdraw\\.`, { parse_mode: 'MarkdownV2' });
          return;
     }

     // Validate address
     let recipientPubKey;
     try {
         recipientPubKey = new PublicKey(address);
     } catch (e) {
         await safeSendMessage(chatId, `⚠️ Invalid Solana address format\\. Please enter a valid address\\.`, { parse_mode: 'MarkdownV2' });
         return; // Keep user in same state
     }

     // --- Check against bot addresses ---
     const botAddresses = [
         process.env.MAIN_WALLET_ADDRESS,
         process.env.RACE_WALLET_ADDRESS,
         // Add public keys derived from private keys if known/needed
         // Keypair.fromSecretKey(bs58.decode(process.env.MAIN_BOT_PRIVATE_KEY)).publicKey.toBase58(), ...etc
     ].filter(Boolean); // Remove empty entries

     if (botAddresses.includes(address)) {
          await safeSendMessage(chatId, `⚠️ You cannot withdraw funds to the bot's own operational addresses\\. Please enter your personal wallet address\\.`, { parse_mode: 'MarkdownV2' });
          return; // Keep user in same state
     }
     // --- End Check ---


     const amountLamports = BigInt(currentState.data.amountLamports);
     const feeLamports = WITHDRAWAL_FEE_LAMPORTS;
     const finalSendAmount = amountLamports; // Fee deducted internally
     const totalDeduction = amountLamports + feeLamports;
     const amountSOL = Number(amountLamports) / LAMPORTS_PER_SOL;
     const feeSOL = Number(feeLamports) / LAMPORTS_PER_SOL;

     // Update state, ask for confirmation
     userStateCache.set(userId, {
          state: 'awaiting_withdrawal_confirm',
          chatId: chatId,
          data: { amountLamports: amountLamports.toString(), recipientAddress: address },
          timestamp: Date.now()
     });

     const options = {
          reply_markup: {
              inline_keyboard: [
                  [ { text: '✅ Confirm Withdrawal', callback_data: 'withdraw_confirm' } ],
                  [ { text: '❌ Cancel', callback_data: 'cancel_action' } ]
              ]
          },
          parse_mode: 'MarkdownV2'
     };
     // ** MD ESCAPE & DECIMAL APPLIED **
     await safeSendMessage(chatId,
         `*Confirm Withdrawal*\n\n` +
         `Amount: ${escapeMarkdownV2(amountSOL.toFixed(4))} SOL\n` +
         `Fee: ${escapeMarkdownV2(feeSOL.toFixed(4))} SOL\n` +
         `Recipient: \`${escapeMarkdownV2(address)}\`\n\n` +
         `Total to be deducted: ${escapeMarkdownV2((Number(totalDeduction) / LAMPORTS_PER_SOL).toFixed(4))} SOL\\. Press confirm to proceed\\.`,
         options
     );
}


// --- Command Handlers (Custodial) ---

// /start - Displays welcome message
async function handleStartCommand(msg, args) {
    const chatId = msg.chat.id;
    const userId = String(msg.from.id);
    const firstName = msg.from.first_name || 'there';

    // Update state to show main menu or clear any pending state
    userStateCache.set(userId, { state: 'idle', chatId: chatId, timestamp: Date.now() });

    const options = {
        reply_markup: {
            inline_keyboard: [
                // Add buttons to initiate games, deposit, withdraw, view referral etc.
                [
                    { text: '🪙 Coinflip', callback_data: 'menu:coinflip' }, // Example callback data
                    { text: '🐎 Race', callback_data: 'menu:race' },
                    { text: '🎰 Slots', callback_data: 'menu:slots' }
                ],
                 [
                     { text: '⚪️ Roulette', callback_data: 'menu:roulette' },
                     { text: '🃏 War', callback_data: 'menu:war' }
                 ],
                 [
                     { text: '💰 Deposit SOL', callback_data: 'menu:deposit' },
                     { text: '💸 Withdraw SOL', callback_data: 'menu:withdraw' }
                 ],
                 [
                      { text: '👤 Wallet / Balance', callback_data: 'menu:wallet' },
                      { text: '🤝 Referral Info', callback_data: 'menu:referral' }
                 ],
                  [ { text: '❓ Help', callback_data: 'menu:help' } ]
            ]
        },
        parse_mode: 'MarkdownV2'
    };

    const welcomeMsg = `👋 Welcome, ${escapeMarkdownV2(firstName)}\\!\n\n` +
                       `Use the buttons below to play, manage funds, or get info\\.`;
    await safeSendMessage(chatId, welcomeMsg, options);
}


// /help - Displays help text
async function handleHelpCommand(msg, args) {
     const chatId = msg.chat.id;
    const helpMsg = `*Solana Gambles Bot Help* 🤖\n\n` +
                    `This bot operates on a *custodial* model\\. You deposit SOL to your internal balance first, then use that balance to play games instantly via buttons\\.\n\n`+
                    `*Commands & Buttons:*\n` +
                    `/start \\- Show main menu\n` +
                    `/help \\- Show this help message\n` +
                    `/wallet \\- View balance & withdrawal address\n` +
                    `/wallet <address> \\- Set/Update withdrawal address\n` +
                    `/referral \\- View referral stats & link\n` +
                    `/deposit \\- Get a unique address to deposit SOL\n` +
                    `/withdraw \\- Start the withdrawal process\n\n` +

                    `*Game Buttons (from main menu):*\n` +
                    `Select a game, enter amount when prompted, confirm bet\\!\n\n` +

                    `*Referral Program:* 🤝\n` +
                    `Use \`/referral\` for your code/link\\. New users use \`/start <YourCode>\`\\. Earn on their first bet \\(tiered %\\) & when they hit wager milestones \\(${escapeMarkdownV2((REFERRAL_MILESTONE_REWARD_PERCENT * 100).toFixed(2))}%\\)\\.\n\n` +

                    `*Important:*\n` +
                    `\\- Only deposit to addresses given by \`/deposit\`\\.\n` +
                    `\\- Link a withdrawal address using \`/wallet <address>\` before withdrawing\\.\n` +
                    `\\- Withdrawals have a small fee \\(${escapeMarkdownV2((Number(WITHDRAWAL_FEE_LAMPORTS)/LAMPORTS_PER_SOL).toFixed(4))} SOL\\) and minimum \\(${escapeMarkdownV2((Number(MIN_WITHDRAWAL_LAMPORTS)/LAMPORTS_PER_SOL).toFixed(4))} SOL\\)\\.\n` +
                    `\\- Gamble responsibly\\.`;

    await safeSendMessage(chatId, helpMsg, { parse_mode: 'MarkdownV2', disable_web_page_preview: true });
}


// /wallet - Displays balance, withdrawal address, ref info OR links address
async function handleWalletCommand(msg, args) {
    const chatId = msg.chat.id;
    const userId = String(msg.from.id);
    const newAddress = args?.trim(); // Check if address was provided

    if (newAddress) {
        // Link/Update withdrawal address
        await linkUserWallet(userId, newAddress); // Reuse the modified function
        // linkUserWallet sends its own success/error message
    } else {
        // View current balance, address, ref info
        const userDetails = await getUserWalletDetails(userId); // Fetches from wallets table
        const userBalance = await getUserBalance(userId); // Fetches from user_balances table

        let walletMsg = `*Your Wallet & Referral Info* 💼\n\n` +
                        `*Balance:* ${escapeMarkdownV2((Number(userBalance) / LAMPORTS_PER_SOL).toFixed(4))} SOL\n`;

        if (userDetails && userDetails.external_withdrawal_address) {
             walletMsg += `*Withdrawal Address:* \`${escapeMarkdownV2(userDetails.external_withdrawal_address)}\`\n`;
        } else {
             walletMsg += `*Withdrawal Address:* Not Set \\(Use \`/wallet <address>\`\\)\n`;
        }
         if (userDetails && userDetails.referral_code) {
              walletMsg += `*Referral Code:* \`${escapeMarkdownV2(userDetails.referral_code)}\`\n`;
         } else {
              walletMsg += `*Referral Code:* \`N/A\` \\(Link wallet or deposit first\\)\n`;
         }
          walletMsg += `*Successful Referrals:* ${escapeMarkdownV2(userDetails?.referral_count || 0)}\n\n`;
          walletMsg += `Use \`/deposit\` to add funds, \`/withdraw\` to cash out\\.`;

        await safeSendMessage(chatId, walletMsg, { parse_mode: 'MarkdownV2' });
    }
}

// /referral - Displays referral stats
async function handleReferralCommand(msg, args) { /* ... same as corrected version in previous response ... */ const chatId = msg.chat.id; const userId = String(msg.from.id); const userDetails = await getUserWalletDetails(userId); if (!userDetails || !userDetails.external_withdrawal_address) { await safeSendMessage(chatId, `❌ You need to link your wallet first using \`/wallet <address>\` before viewing referral stats\\.`, { parse_mode: 'MarkdownV2' }); return; } if (!userDetails.referral_code) { await safeSendMessage(chatId, `❌ Could not retrieve your referral code\\. Please try linking your wallet again with \`/wallet <address>\`\\.`, { parse_mode: 'MarkdownV2' }); return; } const totalEarningsLamports = await getTotalReferralEarnings(userId); const totalEarningsSOL = (Number(totalEarningsLamports) / LAMPORTS_PER_SOL).toFixed(4); let botUsername = 'YourBotUsername'; try { const me = await bot.getMe(); if (me.username) { botUsername = me.username; } } catch (err) { console.error("Error fetching bot info:", err.message); } const referralLink = `https://t.me/${botUsername}?start=${userDetails.referral_code}`; let referralMsg = `*Your Referral Stats* 📊\n\n` + `*Your Code:* \`${escapeMarkdownV2(userDetails.referral_code)}\`\n` + `*Your Referral Link \\(Share this\\!\\):*\n`+ `\`${escapeMarkdownV2(referralLink)}\`\n\n` + `*Successful Referrals:* ${escapeMarkdownV2(userDetails.referral_count)}\n` + `*Total Referral Earnings:* ${escapeMarkdownV2(totalEarningsSOL)} SOL\n\n` + `*How it Works:*\n` + `1\\. *Initial Bonus:* Earn a % of your referral's first qualifying bet \\(tiers below\\)\\.\n` + `2\\. *Milestone Bonus:* Earn ${escapeMarkdownV2((REFERRAL_MILESTONE_REWARD_PERCENT * 100).toFixed(2))}% of the total amount wagered by your referrals as they hit wagering milestones \\(e\\.g\\., 1, 5, 10 SOL etc\\.\\)\\.\n\n`+ `*Initial Bonus Tiers \\(Your total ref count before their first bet\\):*\n` + `  \\- 1\\-10: 5% | 11\\-25: 10% | 26\\-50: 15%\n` + `  \\- 51\\-100: 20% | 101+: 25%\n\n` + `Keep referring to climb the tiers and maximize earnings\\!`; await safeSendMessage(chatId, referralMsg, { parse_mode: 'MarkdownV2', disable_web_page_preview: true }); }


// --- NEW: /deposit Command Handler ---
async function handleDepositCommand(msg, args) {
    const chatId = msg.chat.id;
    const userId = String(msg.from.id);
    const logPrefix = `[DepositCmd User ${userId}]`;

    try {
        // Ensure user has a wallet record (getUserBalance creates if missing)
        await getUserBalance(userId);

        // Get next address index for this user
        const addressIndex = await getNextDepositAddressIndex(userId);

        // Generate unique address
        const derivedInfo = await generateUniqueDepositAddress(userId, addressIndex);
        if (!derivedInfo) {
             throw new Error("Failed to generate a unique deposit address.");
        }

        const depositAddress = derivedInfo.publicKey.toBase58();
        const derivationPath = derivedInfo.path;
        const expiresAt = new Date(Date.now() + DEPOSIT_ADDRESS_EXPIRY_MS);

        // Store address record in DB
        const recordResult = await createDepositAddressRecord(userId, depositAddress, derivationPath, expiresAt);
        if (!recordResult.success) {
             throw new Error(recordResult.error || "Failed to save deposit address record.");
        }

        // Display address to user
         const expiryMinutes = Math.round(DEPOSIT_ADDRESS_EXPIRY_MS / (60 * 1000));
         const message = `💰 *Deposit SOL*\n\n` +
                         `To top up your balance, send SOL to this unique address:\n\n` +
                         `\`${escapeMarkdownV2(depositAddress)}\`\n\n` +
                         `⚠️ *Important:*\n` +
                         `1\\. This address is temporary and expires in *${expiryMinutes} minutes*\\.\n` +
                         `2\\. Send *only one* transaction to this address\\.\n` +
                         `3\\. Minimum deposit depends on network fees, but generally > 0\\.001 SOL is safe\\.\n` +
                         `4\\. Deposited funds will reflect in your /wallet balance after network confirmations \\(${escapeMarkdownV2(DEPOSIT_CONFIRMATIONS_NEEDED)}\\)\\.`;

          // Add QR Code? (Requires generating QR image and sending photo)

          await safeSendMessage(chatId, message, { parse_mode: 'MarkdownV2' });

    } catch (error) {
         console.error(`${logPrefix} Error: ${error.message}`);
         await safeSendMessage(chatId, `❌ Error generating deposit address: ${escapeMarkdownV2(error.message)}\\. Please try again later\\.`, { parse_mode: 'MarkdownV2' });
    }
}

// --- NEW: /withdraw Command Handler ---
async function handleWithdrawCommand(msg, args) {
     const chatId = msg.chat.id;
     const userId = String(msg.from.id);
     const logPrefix = `[WithdrawCmd User ${userId}]`;

     try {
         // Check if user has linked an external withdrawal address
         const userDetails = await getUserWalletDetails(userId);
         if (!userDetails?.external_withdrawal_address) {
              await safeSendMessage(chatId, `⚠️ You must set your external withdrawal address first using \`/wallet <YourSolanaAddress>\`\\.`, { parse_mode: 'MarkdownV2' });
              return;
         }

         // Start the withdrawal flow by asking for amount
         userStateCache.set(userId, {
             state: 'awaiting_withdrawal_amount',
             chatId: chatId,
             timestamp: Date.now(),
             data: {}
         });

         await safeSendMessage(chatId, `💸 Please enter the amount of SOL you wish to withdraw:`, { parse_mode: 'MarkdownV2' });

     } catch (error) {
          console.error(`${logPrefix} Error starting withdrawal: ${error.message}`);
          await safeSendMessage(chatId, `❌ Error starting withdrawal process\\. Please try again later\\.`, { parse_mode: 'MarkdownV2' });
     }
}


// --- Game Command Handlers (Initiate Button Flows) ---

async function handleCoinflipCommand(msg, args) {
     const chatId = msg.chat.id;
     const userId = String(msg.from.id);
     // Start coinflip flow: Ask for amount
     userStateCache.set(userId, { state: 'awaiting_cf_amount', chatId: chatId, timestamp: Date.now(), data: {} });
     await safeSendMessage(chatId, `🪙 Enter Coinflip bet amount \\(SOL\\):`, { parse_mode: 'MarkdownV2' });
}

async function handleRaceCommand(msg, args) {
    const chatId = msg.chat.id;
    // TODO: Implement Race betting flow with buttons
    await safeSendMessage(chatId, `🐎 Race betting coming soon via buttons\\!`, { parse_mode: 'MarkdownV2' });
}
async function handleSlotsCommand(msg, args) {
    const chatId = msg.chat.id;
    const userId = String(msg.from.id);
     // Start slots flow: Ask for amount
     userStateCache.set(userId, { state: 'awaiting_slots_amount', chatId: chatId, timestamp: Date.now(), data: {} });
     await safeSendMessage(chatId, `🎰 Enter Slots bet amount \\(SOL\\):`, { parse_mode: 'MarkdownV2' });
}
async function handleRouletteCommand(msg, args) {
     const chatId = msg.chat.id;
      // TODO: Implement Roulette betting flow (likely needs Mini App or complex button convo)
      await safeSendMessage(chatId, `⚪️ Roulette betting coming soon via buttons/interface\\!`, { parse_mode: 'MarkdownV2' });
}
async function handleWarCommand(msg, args) {
     const chatId = msg.chat.id;
     const userId = String(msg.from.id);
     // Start war flow: Ask for amount
     userStateCache.set(userId, { state: 'awaiting_war_amount', chatId: chatId, timestamp: Date.now(), data: {} });
     await safeSendMessage(chatId, `🃏 Enter Casino War bet amount \\(SOL\\):`, { parse_mode: 'MarkdownV2' });
}


// --- Admin Command --- (Unchanged from v2.7.0)
async function handleAdminCommand(msg, args) { /* ... same admin command logic ... */ const chatId = msg.chat.id; const userId = String(msg.from.id); const adminUserIds = (process.env.ADMIN_USER_IDS || '').split(',').map(id => id.trim()).filter(id => id); if (!adminUserIds.includes(String(userId))) { await safeSendMessage(chatId, "🚫 Unauthorized.", { parse_mode: 'MarkdownV2' }); return; } const subCommand = args?.split(' ')[0]?.toLowerCase(); const subArgs = args?.split(' ').slice(1) || []; if (!subCommand) { await safeSendMessage(chatId, "Admin cmds: `status`, `setconcurrency <type> <num>`, `forcerotate`, `getconfig`, `getbalance <key_type>`", { parse_mode: 'MarkdownV2' }); return; } try { switch(subCommand) { case 'status': { /* Status logic (needs update for new queues/cache) */ let statsMsg = `*Bot Status* (v3.0.0)\n`; /* ... */ await safeSendMessage(chatId, statsMsg, { parse_mode: 'MarkdownV2' }); break; } case 'forcerotate': { if (typeof solanaConnection?._rotateEndpoint === 'function') { solanaConnection._rotateEndpoint(); await safeSendMessage(chatId, "RPC endpoint rotation forced.", { parse_mode: 'MarkdownV2' }); } else { await safeSendMessage(chatId, "Error: Rotate function not available.", { parse_mode: 'MarkdownV2' }); } break; } case 'getconfig': { let configText = "*Current Config (Non-Secret Env Vars):*\n\n"; const safeVars = Object.keys(OPTIONAL_ENV_DEFAULTS).concat(['RPC_URLS' /* Add others if needed */]); safeVars.forEach(key => { if (key.includes('TOKEN') || key.includes('DATABASE_URL') || key.includes('KEY') || key.includes('SECRET') || key.includes('PASSWORD') || key.includes('SEED')) { if(key !== 'RPC_URLS' && key !== 'DATABASE_URL') return; /* Redact sensitive */ configText += `${key}: [Set, Redacted]\n`; return; } configText += `${key}: ${process.env[key] || '(Not Set / Using Default)'}\n`; }); await safeSendMessage(chatId, escapeMarkdownV2(configText), { parse_mode: 'MarkdownV2' }); break; } /* Add setconcurrency for different queues? Add getbalance for payout keys? */ default: await safeSendMessage(chatId, `Unknown admin command: \`${escapeMarkdownV2(subCommand)}\``, { parse_mode: 'MarkdownV2' }); } } catch (adminError) { console.error(`Admin command error (${subCommand}):`, adminError); await safeSendMessage(chatId, `Error executing admin command: \`${escapeMarkdownV2(adminError.message)}\``, { parse_mode: 'MarkdownV2' }); } }


// --- End of Part 3b ---
// index.js - Part 4 (Custodial Model with Buttons & Referrals)
// --- VERSION: 3.0.0 ---

// (Code continues directly from the end of Part 3b)

// --- Server Startup & Shutdown Logic ---

// Encapsulated Webhook Setup Logic (Unchanged Functionality)
async function setupTelegramWebhook() {
    if (process.env.RAILWAY_ENVIRONMENT && process.env.RAILWAY_PUBLIC_DOMAIN) {
        const webhookUrl = `https://${process.env.RAILWAY_PUBLIC_DOMAIN}${webhookPath}`;
        console.log(`Attempting to set webhook to: ${webhookUrl}`);
        let attempts = 0; const maxAttempts = 3;
        while (attempts < maxAttempts) {
            try {
                await bot.deleteWebHook({ drop_pending_updates: true }).catch(e => console.warn(`Ignoring webhook delete error: ${e.message}`));
                await bot.setWebHook(webhookUrl, {
                    max_connections: parseInt(process.env.WEBHOOK_MAX_CONN || '10', 10),
                    allowed_updates: ["message", "callback_query"], // Now need callback_query too if webhook should handle them
                });
                console.log(`✅ Webhook successfully set to: ${webhookUrl}`);
                return true;
            } catch (webhookError) {
                attempts++; console.error(`❌ Webhook setup attempt ${attempts}/${maxAttempts} failed:`, webhookError.message);
                if (webhookError.code === 'ETELEGRAM' && (webhookError.message.includes('URL host is empty') || webhookError.message.includes('Unauthorized'))) { return false; } // Non-retryable
                if (attempts >= maxAttempts) { console.error("❌ Max webhook setup attempts reached."); return false; }
                await new Promise(resolve => setTimeout(resolve, 3000 * attempts));
            }
        }
    }
    return false;
}

// Encapsulated Polling Setup Logic (Unchanged Functionality)
async function startPollingIfNeeded() {
    try {
        const info = await bot.getWebHookInfo().catch(e => { console.error("Error fetching webhook info:", e.message); return null; });
        if (!info || !info.url) {
            if (bot.isPolling()) return;
            console.log("ℹ️ Webhook not set, starting bot polling...");
            await bot.deleteWebHook({ drop_pending_updates: true }).catch(e => console.warn(`Ignoring webhook delete error: ${e.message}`));
            await bot.startPolling({ allowed_updates: ["message", "callback_query"] }); // Ensure polling gets callbacks too
            console.log("✅ Bot polling started successfully");
        } else {
            if (bot.isPolling()) {
                console.log("ℹ️ Stopping existing polling because webhook is set.");
                await bot.stopPolling({ cancel: true }).catch(e => console.warn(`Ignoring polling stop error: ${e.message}`));
            }
        }
    } catch (err) { /* ... same critical error handling for 409/401 ... */ console.error("❌ Error managing polling state:", err.message); if (err.code === 'ETELEGRAM' && err.message.includes('409 Conflict')) { console.error("❌❌❌ Conflict! Exiting."); shutdown('POLLING_CONFLICT_STARTUP', false).catch(()=>{}); setTimeout(() => process.exit(1), 5000).unref(); } else if (err.response && err.response.statusCode === 401) { console.error("❌❌❌ Unauthorized! Check Token. Exiting."); shutdown('BOT_TOKEN_INVALID_STARTUP', false).catch(()=>{}); setTimeout(() => process.exit(1), 5000).unref(); } }
}

// *** NEW: Encapsulated Deposit Monitor Start Logic ***
// Uses `monitorDeposits` function defined in Part 3a
function startDepositMonitor() {
    if (depositMonitorIntervalId) return; // Already running

    const intervalMs = parseInt(process.env.DEPOSIT_MONITOR_INTERVAL_MS, 10);
    if (isNaN(intervalMs) || intervalMs < 5000) { // Minimum interval 5s
        console.warn(`⚠️ Invalid or low DEPOSIT_MONITOR_INTERVAL_MS (${process.env.DEPOSIT_MONITOR_INTERVAL_MS}), defaulting to 15000ms.`);
        process.env.DEPOSIT_MONITOR_INTERVAL_MS = '15000';
    }
    const finalIntervalMs = parseInt(process.env.DEPOSIT_MONITOR_INTERVAL_MS, 10);

    console.log(`⚙️ Starting deposit monitor (Interval: ${finalIntervalMs / 1000}s)`);
    // Initial run shortly after start
    const initialDelay = 5000; // 5 second delay for initial run
    setTimeout(() => {
         console.log("⚙️ Performing initial deposit monitor run...");
         monitorDeposits().catch(err => {
             console.error('❌ [DEPOSIT MONITOR ASYNC ERROR - Initial Run]:', err);
             performanceMonitor.logRequest(false);
         });
    }, initialDelay);

    // Set interval
    depositMonitorIntervalId = setInterval(() => {
        monitorDeposits().catch(err => {
            console.error('❌ [DEPOSIT MONITOR ASYNC ERROR - Interval]:', err);
            performanceMonitor.logRequest(false);
        });
    }, finalIntervalMs);
}


// --- Graceful shutdown handler ---
// *** MODIFIED: Stop deposit monitor, wait for correct queues ***
let isShuttingDown = false;
const shutdown = async (signal, isRailwayRotation = false) => {
    if (isShuttingDown) { console.log("Shutdown already in progress..."); return; }
    isShuttingDown = true;
    console.log(`\n🛑 ${signal} received, ${isRailwayRotation ? 'initiating container rotation' : 'shutting down gracefully'}...`);

    // Special handling for Railway rotation
    if (isRailwayRotation) {
        console.log("Railway: Closing server, stopping monitor, brief queue drain.");
        // Stop Deposit Monitor
        if (depositMonitorIntervalId) { clearInterval(depositMonitorIntervalId); depositMonitorIntervalId = null; }
        if (server) { server.close(() => console.log("- Server closed for rotation.")); }
        const drainTimeout = parseInt(process.env.RAILWAY_ROTATION_DRAIN_MS, 10);
        console.log(`- Allowing ${drainTimeout}ms for queue drain...`);
        try {
            // Wait only for payout queue briefly during rotation
            await Promise.race([
                payoutProcessorQueue.onIdle(),
                new Promise(resolve => setTimeout(resolve, drainTimeout))
            ]);
            console.log("- Queue drain period ended.");
        } catch(e) { console.warn(`Ignoring error during rotation drain:`, e.message); }
        console.log("Rotation procedure complete.");
        return;
    }

    // Full Shutdown Logic
    console.log("Performing full graceful shutdown sequence...");
    isFullyInitialized = false;
    depositMonitorRunning = true; // Prevent monitor starting new cycle

    // 1. Stop receiving new events/requests
    console.log("Stopping incoming connections and tasks...");
    // Stop Deposit Monitor Interval
    if (depositMonitorIntervalId) { clearInterval(depositMonitorIntervalId); depositMonitorIntervalId = null; console.log("- Stopped deposit monitor interval."); }
    try {
        // Close Express server
        if (server) {
            console.log("- Closing Express server...");
            await new Promise((resolve, reject) => { /* ... server close with timeout ... */ const timeout = setTimeout(()=>reject(new Error('Server close timeout')), 5000); if(timeout.unref) timeout.unref(); server.close(e => { clearTimeout(timeout); if(e) reject(e); else resolve(); }); });
            console.log("- Express server closed.");
        }
        // Stop Telegram listeners
        console.log("- Stopping Telegram listeners...");
        if (bot.isPolling()) { await bot.stopPolling({ cancel: true }).catch(e => console.warn("Polling stop error:", e.message)); console.log("- Stopped polling."); }
        const webhookInfo = await bot.getWebHookInfo().catch(() => null);
        if (webhookInfo && webhookInfo.url) { await bot.deleteWebHook({ drop_pending_updates: false }).catch(e => console.warn("Webhook delete error:", e.message)); console.log("- Removed webhook."); }
    } catch (e) { console.error("⚠️ Error stopping listeners/server:", e.message); }

    // 2. Wait for ongoing queue processing (Updated Queues)
    const queueDrainTimeoutMs = parseInt(process.env.SHUTDOWN_QUEUE_TIMEOUT_MS, 10);
    console.log(`Waiting for active jobs (max ${queueDrainTimeoutMs / 1000}s)...`);
    try {
        const timeoutPromise = new Promise((_, reject) => { const t = setTimeout(() => reject(new Error(`Queue drain timeout`)), queueDrainTimeoutMs); if(t.unref) t.unref(); });
        await Promise.race([
            Promise.all([
                messageQueue.onIdle(),         // Wait for message processing
                callbackQueue.onIdle(),        // Wait for callback processing
                payoutProcessorQueue.onIdle(), // Wait for withdrawals/referral payouts
                telegramSendQueue.onIdle()     // Wait for outgoing TG messages
            ]),
            timeoutPromise
        ]);
        console.log("- All processing queues are idle.");
    } catch (queueError) {
        console.warn(`⚠️ ${queueError.message}. Proceeding.`);
        console.warn(`  Queue sizes: Msg=${messageQueue.size}, Callback=${callbackQueue.size}, Payout=${payoutProcessorQueue.size}, TGSend=${telegramSendQueue.size}`);
    }

    // 3. Close database pool
    console.log("Closing database pool...");
    try {
        await pool.end();
        console.log("✅ Database pool closed.");
    } catch (dbErr) { console.error("❌ Error closing database pool:", dbErr); }
    finally {
        console.log("🛑 Full Shutdown sequence complete.");
        process.exit(signal === 'SIGINT' || signal === 'SIGTERM' ? 0 : 1);
    }
    // --- END: Full Shutdown Logic ---
};


// --- Signal handlers for graceful shutdown --- (Unchanged Functionality)
process.on('SIGTERM', () => { const isRailway = !!process.env.RAILWAY_ENVIRONMENT; console.log(`SIGTERM received. Railway: ${isRailway}`); shutdown('SIGTERM', isRailway).catch((err) => { console.error("SIGTERM shutdown error:", err); process.exit(1); }); });
process.on('SIGINT', () => { console.log(`SIGINT received.`); shutdown('SIGINT', false).catch((err) => { console.error("SIGINT shutdown error:", err); process.exit(1); }); });

// --- Exception Handlers (Unchanged Functionality) ---
process.on('uncaughtException', (err, origin) => { console.error(`🔥🔥🔥 Uncaught Exception at: ${origin}`, err); if (!isShuttingDown) { console.log("Attempting emergency shutdown..."); shutdown('UNCAUGHT_EXCEPTION', false).catch(() => {}); } const t = setTimeout(() => { console.error(`Forcing exit after uncaught exception timeout.`); process.exit(1); }, parseInt(process.env.SHUTDOWN_FAIL_TIMEOUT_MS, 10)); if(t.unref) t.unref(); });
process.on('unhandledRejection', (reason, promise) => { console.error('🔥🔥🔥 Unhandled Rejection at:', promise, 'reason:', reason); /* Log only for now */ });


// --- Start the Application ---
const PORT = process.env.PORT || 3000;

server = app.listen(PORT, "0.0.0.0", () => {
    console.log(`🚀 Server listening on port ${PORT}...`);
    const initDelay = parseInt(process.env.INIT_DELAY_MS, 10);
    console.log(`⚙️ Scheduling background initialization in ${initDelay / 1000}s...`);

    setTimeout(async () => {
        console.log("⚙️ Starting delayed background initialization...");
        try {
            console.log("  - Initializing Database (v3.0.0 Schema)...");
            await initializeDatabase(); // Includes custodial & referral schema

            console.log("  - Setting up Telegram connection (Webhook/Polling)...");
            const webhookSet = await setupTelegramWebhook();
            if (!webhookSet) { await startPollingIfNeeded(); }
             try { const me = await bot.getMe(); console.log(`✅ Connected to Telegram as bot: @${me.username}`); }
             catch (tgError) { console.error(`❌ TG connection verify failed: ${tgError.message}`); if (tgError.response?.statusCode === 401) { throw new Error("Invalid BOT_TOKEN."); } throw tgError; }

            // ** Start the NEW Deposit Monitor **
            console.log("  - Starting Deposit Monitor...");
            startDepositMonitor(); // Replaces startPaymentMonitor

            // Optional: Adjust Solana Concurrency (Unchanged logic)
            const concurrencyAdjustDelay = parseInt(process.env.CONCURRENCY_ADJUST_DELAY_MS, 10);
             const targetConcurrency = parseInt(process.env.RPC_TARGET_CONCURRENT, 10);
             // ... (rest of concurrency adjustment logic) ...
             if (solanaConnection?.options && targetConcurrency !== parseInt(process.env.RPC_MAX_CONCURRENT, 10) && !isNaN(targetConcurrency) && targetConcurrency > 0 && concurrencyAdjustDelay > 0) {
                 console.log(`⚙️ Scheduling Solana concurrency adjustment to ${targetConcurrency} in ${concurrencyAdjustDelay / 1000}s...`);
                 setTimeout(() => { if (solanaConnection?.options) { console.log(`⚡ Adjusting Solana concurrency to ${targetConcurrency}...`); solanaConnection.setMaxConcurrent(targetConcurrency); /* Use setMaxConcurrent method */ console.log(`✅ Solana maxConcurrent adjusted.`); } }, concurrencyAdjustDelay);
             }


            isFullyInitialized = true;
            console.log("✅ Background Initialization Complete.");
            // ** VERSION UPDATED **
            const botVersion = process.env.npm_package_version || '3.0.0';
            console.log(`🚀🚀🚀 Solana Gambles Bot (v${botVersion} - Custodial) is fully operational! 🚀🚀🚀`);

        } catch (initError) { // Catch errors from DB init, Telegram setup, etc.
            console.error("🔥🔥🔥 Delayed Background Initialization Failed:", initError);
            console.error("❌ Exiting due to critical initialization failure.");
            if (!isShuttingDown) await shutdown('INITIALIZATION_FAILURE', false).catch(() => {});
            const failTimeout = parseInt(process.env.SHUTDOWN_FAIL_TIMEOUT_MS, 10);
             const t = setTimeout(() => { console.error(`Forcing exit after init failure timeout.`); process.exit(1); }, failTimeout); if (t.unref) t.unref();
        }
    }, initDelay);
});

// Handle server startup errors (Unchanged)
server.on('error', (err) => { /* ... same error handling ... */ console.error('❌ Server startup error:', err); if (err.code === 'EADDRINUSE') { console.error(`❌ Port ${PORT} busy. Exiting.`); } process.exit(1); });

// --- End of Part 4 / End of File ---
