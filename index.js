// Test: Ensure DB init completes before app listens
require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
const TelegramBot = require('node-telegram-bot-api');
const { Connection } = require('@solana/web3.js'); // Simplified Solana require for init only

// Check only BOT_TOKEN
if (!process.env.BOT_TOKEN) {
     console.error(`Environment variable BOT_TOKEN is missing.`);
     process.exit(1);
}

const app = express();

// --- PostgreSQL Setup ---
console.log("Setting up PostgreSQL Pool...");
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  // ssl: { rejectUnauthorized: false }
});
console.log("PostgreSQL Pool created.");

// Function to make sure the 'bets' table exists
async function initializeDatabase() {
  console.log("Initializing Database...");
  let client;
  try {
    client = await pool.connect();
    console.log("DB client connected.");
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
    throw err; // Re-throw error to be caught by calling function
  } finally {
    if (client) {
      client.release();
      console.log("ℹ️ Database client released after initialization check.");
    }
  }
}
// --- End of PostgreSQL section ---

// --- Bot and Solana Initialization ---
console.log("Initializing Telegram Bot (NO POLLING)...");
// IMPORTANT: Initialize bot WITHOUT polling
const bot = new TelegramBot(process.env.BOT_TOKEN);
console.log("Telegram Bot initialized.");

console.log("Initializing Solana Connection...");
const connection = new Connection(process.env.RPC_URL || 'https://api.mainnet-beta.solana.com', 'confirmed');
console.log("Solana Connection initialized.");
// --- End Initialization ---

// --- Express Healthcheck Route ---
app.get('/', (req, res) => {
  console.log("Health check endpoint '/' hit!");
  res.status(200).send('OK');
});

// --- Main Server Start Function ---
async function startServer() {
    try {
        // Ensure DB is ready first
        await initializeDatabase();

        // Then start listening for HTTP requests
        const PORT = process.env.PORT || 8080;
        app.listen(PORT, () => {
            console.log(`✅ Healthcheck server listening on port ${PORT}`);
            console.log("Application fully started. Waiting for requests or signals...");
        });

    } catch (error) {
        console.error("💥 Failed to start server:", error);
        process.exit(1); // Exit if essential startup fails
    }
}

// --- Start the server ---
startServer();


/* ===============================================
   BOT LOGIC REMAINS COMMENTED OUT
   =============================================== */

/*
// ... (All commented out code remains here) ...
*/


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
  process.exit(1);
});
