// Minimal app + Bot/Solana Init (NO POLLING)
require('dotenv').config();
const express = require('express');
const { Pool } = require('pg'); // Keep pg
const TelegramBot = require('node-telegram-bot-api'); // Add back TelegramBot require
const { // Add back Solana requires
    Connection,
    // clusterApiUrl,
    PublicKey,
    LAMPORTS_PER_SOL,
    Keypair,
    Transaction,
    SystemProgram,
    sendAndConfirmTransaction,
    ComputeBudgetProgram
} = require('@solana/web3.js');
const bs58 = require('bs58'); // Add back bs58 require

// Check only BOT_TOKEN for now
if (!process.env.BOT_TOKEN) {
     console.error(`Environment variable BOT_TOKEN is missing.`);
     process.exit(1);
}

const app = express();

// --- PostgreSQL Setup ---
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  // ssl: { rejectUnauthorized: false } // Keep commented unless needed
});
async function initializeDatabase() {
  let client;
  try {
    client = await pool.connect();
    await client.query(`
      CREATE TABLE IF NOT EXISTS bets (
        id SERIAL PRIMARY KEY, user_id TEXT NOT NULL, chat_id TEXT NOT NULL,
        game_type TEXT NOT NULL, bet_details JSONB, expected_lamports BIGINT NOT NULL,
        memo_id TEXT UNIQUE, status TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        expires_at TIMESTAMPTZ NOT NULL, used_tx_id TEXT UNIQUE
      );
    `);
    console.log("✅ Database table 'bets' checked/created successfully.");
  } catch (err) {
    console.error("❌ Error initializing database table:", err);
    process.exit(1);
  } finally {
    if (client) {
      client.release();
      console.log("ℹ️ Database client released after initialization check.");
    }
  }
}
initializeDatabase().catch(err => {
    console.error("Unhandled error during async DB initialization:", err);
    process.exit(1);
});
// --- End of PostgreSQL section ---

// --- Bot and Solana Initialization ---
console.log("Initializing Telegram Bot...");
// IMPORTANT: Initialize bot WITHOUT polling for this test
const bot = new TelegramBot(process.env.BOT_TOKEN);
console.log("Telegram Bot initialized.");

console.log("Initializing Solana Connection...");
// Ensure RPC_URL is set in Railway variables or default to a public one
const connection = new Connection(process.env.RPC_URL || 'https://api.mainnet-beta.solana.com', 'confirmed');
console.log("Solana Connection initialized.");
// --- End Initialization ---


// --- Express Healthcheck ---
app.get('/', (req, res) => {
  console.log("Health check endpoint '/' hit!");
  res.status(200).send('OK');
});
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
    console.log(`✅ Healthcheck server listening on port ${PORT}`);
    console.log("Waiting for requests or signals...");
});


/* ===============================================
   BOT LOGIC REMAINS COMMENTED OUT FOR NOW
   =============================================== */

/*

// --- Constants and In-Memory State ---
// ... (all constants and in-memory state variables remain commented) ...

// --- Functions (checkPayment, sendSol, etc.) ---
// ... (all these functions remain commented) ...

// --- Bot Command Handlers ---
// ... (all bot.onText handlers remain commented) ...

// --- Payment Monitoring Logic ---
// ... (monitorPayments function remains commented) ...

// --- Database Interaction Functions ---
// ... (findBetByMemo, updateBetStatus remain commented for now) ...

// --- Game Processing Logic ---
// ... (processPaidBet, handleCoinflipGame, handleRaceGame remain commented) ...

console.log("Bot logic initialization skipped (commented out).");

*/ // <--- END OF COMMENTED OUT CODE


// --- Graceful shutdown ---
process.on('SIGINT', () => {
  console.log("Received SIGINT. Shutting down server...");
  process.exit(0);
});
process.on('SIGTERM', () => {
  console.log("Received SIGTERM. Shutting down server...");
  process.exit(0);
});

// Catch unhandled promise rejections
process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
});

// Catch uncaught exceptions
process.on('uncaughtException', (error) => {
  console.error('Uncaught Exception:', error);
  process.exit(1); // Exit on uncaught exception
});

console.log("Application started with Bot/Solana initializers (NO POLLING).");
