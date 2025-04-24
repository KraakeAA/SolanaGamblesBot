require('dotenv').config();
const REQUIRED_ENV_VARS = ['BOT_TOKEN', 'BOT_PRIVATE_KEY', 'RACE_BOT_PRIVATE_KEY'];
// Check only BOT_TOKEN for now, as others aren't used in this simplified version
if (!process.env.BOT_TOKEN) {
     console.error(`Environment variable BOT_TOKEN is missing.`);
     process.exit(1); // Exit if required vars are missing
}
// REQUIRED_ENV_VARS.forEach((key) => {
//     if (!process.env[key]) {
//         console.error(`Environment variable ${key} is missing.`);
//         process.exit(1); // Exit if required vars are missing
//     }
// });

// Only require necessary packages for this test
const express = require('express');
const { Pool } = require('pg'); // Import the pg library

const app = express();

// --- PostgreSQL Setup ---
// Create a Pool. It will automatically use the DATABASE_URL environment variable
// provided by Railway when deployed.
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  // Depending on Railway's Postgres setup, you might need SSL configuration.
  // Start without it, but if you get SSL errors later, uncomment/adjust this:
  // ssl: {
  //   rejectUnauthorized: false
  // }
});

// Function to make sure the 'bets' table exists
async function initializeDatabase() {
  let client; // Declare client outside try block
  try {
    client = await pool.connect(); // Get a client connection from the pool
    await client.query(`
      CREATE TABLE IF NOT EXISTS bets (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        chat_id TEXT NOT NULL,
        game_type TEXT NOT NULL, -- 'coinflip', 'race'
        bet_details JSONB,      -- e.g., { "choice": "heads" } or { "horse": "Blue" }
        expected_lamports BIGINT NOT NULL, -- Use BIGINT for Lamports
        memo_id TEXT UNIQUE,     -- Make memo unique
        status TEXT NOT NULL,    -- 'awaiting_memo_payment', 'payment_verified', 'completed_win', 'completed_loss', 'expired', 'error'
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        expires_at TIMESTAMPTZ NOT NULL,
        used_tx_id TEXT UNIQUE -- Keeping just in case, but primarily using memo_id
      );
    `);
    console.log("✅ Database table 'bets' checked/created successfully.");
  } catch (err) {
    console.error("❌ Error initializing database table:", err);
    // Exiting if DB connection fails might be desired in production
    process.exit(1); // Let's exit if DB fails in this test
  } finally {
    if (client) {
      client.release(); // IMPORTANT: Always release the client connection back to the pool
      console.log("ℹ️ Database client released after initialization check.");
    }
  }
}

// Call the function to initialize the database when the app starts
initializeDatabase().catch(err => {
    // Catch potential errors during async init before server start
    console.error("Unhandled error during async DB initialization:", err);
    process.exit(1);
});
// --- End of PostgreSQL section ---


// --- Express Healthcheck ---
// Basic health check endpoint
app.get('/', (req, res) => res.send('OK'));

// Start listening - ensure PORT matches Railway health check config
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
    console.log(`✅ Healthcheck server listening on port ${PORT}`);
});


/* ===============================================
   START OF COMMENTED OUT BOT & SOLANA RELATED CODE
   =============================================== */

/* // Temporarily commented out for debugging startup/shutdown issue

const TelegramBot = require('node-telegram-bot-api');
const {
    Connection,
    // clusterApiUrl, // Not used directly, can be removed if desired
    PublicKey,
    LAMPORTS_PER_SOL,
    Keypair,
    Transaction,
    SystemProgram,
    sendAndConfirmTransaction,
    ComputeBudgetProgram
} = require('@solana/web3.js');
const bs58 = require('bs58');
const fs = require('fs');

// --- Bot and Solana Setup ---
const bot = new TelegramBot(process.env.BOT_TOKEN, { polling: true });
const connection = new Connection(process.env.RPC_URL || 'https://api.mainnet-beta.solana.com', 'confirmed');

// --- Constants and In-Memory State ---
const MAIN_WALLET_ADDRESS = process.env.MAIN_WALLET_ADDRESS || 'YOUR_MAIN_WALLET_ADDRESS_HERE';
const RACE_WALLET_ADDRESS = process.env.RACE_WALLET_ADDRESS || 'YOUR_RACE_WALLET_ADDRESS_HERE';
const MIN_BET = 0.01;
const MAX_BET = 1.0;
const LOG_DIR = './data';
const LOG_PATH = './data/bets.json';
const raceSessions = {};
const userRaceBets = {};
let nextRaceId = 1;
const RACE_MIN_BET = 0.01;
const RACE_MAX_BET = 1.0;
const userBets = {};
const coinFlipSessions = {};
const linkedWallets = {};
const userPayments = {};
const confirmCooldown = {};
const cooldownInterval = 3000;

// --- Functions (checkPayment, sendSol, etc.) ---
async function checkPayment(expectedSol, userId, gameType, targetWalletAddress) {
    console.warn("checkPayment function is disabled in simplified mode.");
    return { success: false, message: 'CheckPayment disabled.' };
}
async function sendSol(connection, payerPrivateKey, recipientPublicKey, amount) {
    console.warn("sendSol function is disabled in simplified mode.");
     return { success: false, error: 'SendSol disabled.' };
}
const getHouseEdge = (amount) => {
     console.warn("getHouseEdge function is disabled in simplified mode.");
     return 0.95; // Return a default edge maybe
};
// Filesystem checks
// if (!fs.existsSync(LOG_DIR)) {
//     try { fs.mkdirSync(LOG_DIR); } catch (e) { console.warn("Could not create LOG_DIR", e.message)}
// }
// if (!fs.existsSync(LOG_PATH)) {
//     try { fs.writeFileSync(LOG_PATH, '[]'); } catch (e) { console.warn("Could not write initial LOG_PATH", e.message)}
// }
function getPayerFromTransaction(tx, expectedAmount) {
    console.warn("getPayerFromTransaction function is disabled in simplified mode.");
    return null;
}

// --- Bot Command Handlers ---
bot.onText(/\/start$/, async (msg) => {
    console.log("Received /start, but bot logic is disabled.");
    // Optionally send a simple message back
    // bot.sendMessage(msg.chat.id, "Bot is currently in maintenance mode.");
});
// ... (All other bot.onText handlers are effectively disabled) ...

// --- Payment Monitoring Logic ---
async function monitorPayments() {
    console.log("Payment monitor disabled in simplified mode.");
}
// const monitorInterval = setInterval(monitorPayments, 30000);

// --- Database Interaction Functions ---
async function savePendingBet(userId, chatId, gameType, details, lamports, memoId, expiryDate) {
     console.warn("DB function savePendingBet disabled in simplified mode.");
     return { success: false, error: 'DB disabled' };
}
async function findBetByMemo(memoId) {
     console.warn("DB function findBetByMemo disabled in simplified mode.");
   return undefined;
}
async function updateBetStatus(betId, newStatus) {
    console.warn("DB function updateBetStatus disabled in simplified mode.");
   return { success: false, error: 'DB disabled' };
}

// --- Game Processing Logic ---
async function processPaidBet(bet) {
     console.warn("Game function processPaidBet disabled in simplified mode.");
}
async function handleCoinflipGame(bet) {
     console.warn("Game function handleCoinflipGame disabled in simplified mode.");
}
async function handleRaceGame(bet) {
     console.warn("Game function handleRaceGame disabled in simplified mode.");
}

console.log("Bot logic initialization skipped (commented out).");

*/ // <--- END OF COMMENTED OUT CODE


// --- Graceful shutdown ---
// Keep these active to see how the process exits
process.on('SIGINT', () => {
  console.log("Received SIGINT. Shutting down server...");
  // No bot or monitor interval to clear in this version
  process.exit(0);
});
process.on('SIGTERM', () => {
  console.log("Received SIGTERM. Shutting down server...");
  // No bot or monitor interval to clear in this version
  process.exit(0);
});

console.log("Simplified application started. Waiting for requests or signals...");
