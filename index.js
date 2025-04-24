require('dotenv').config();
const REQUIRED_ENV_VARS = ['BOT_TOKEN', 'BOT_PRIVATE_KEY', 'RACE_BOT_PRIVATE_KEY'];
REQUIRED_ENV_VARS.forEach((key) => {
    if (!process.env[key]) {
        console.error(`Environment variable ${key} is missing.`);
        process.exit(1); // Exit if required vars are missing
    }
});
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
const fs = require('fs'); // Keep for now, might be needed for other things or can be removed if LOG_DIR/LOG_PATH logic changes
const express = require('express');
const app = express();

// --- Add this section for PostgreSQL ---
const { Pool } = require('pg'); // Import the pg library

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
    // Consider adding: process.exit(1);
  } finally {
    if (client) {
      client.release(); // IMPORTANT: Always release the client connection back to the pool
      console.log("ℹ️ Database client released after initialization check.");
    }
  }
}

// Call the function to initialize the database when the bot starts
// We need to make sure this completes before maybe doing other DB operations on startup
// For now, just calling it is okay. More complex logic might await it if needed.
initializeDatabase();

// --- End of PostgreSQL section ---


// --- Existing Express Healthcheck ---
app.get('/', (req, res) => res.send('OK'));
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Healthcheck listening on ${PORT}`));

// --- Existing Bot and Solana Setup ---
const bot = new TelegramBot(process.env.BOT_TOKEN, { polling: true });
// Consider switching polling: true to webhooks later for production efficiency
const connection = new Connection(process.env.RPC_URL || 'https://api.mainnet-beta.solana.com', 'confirmed'); // Use RPC_URL from env if available

// --- Existing Constants and In-Memory State (!!! We will replace these later !!!) ---
const MAIN_WALLET_ADDRESS = process.env.MAIN_WALLET_ADDRESS || 'YOUR_MAIN_WALLET_ADDRESS_HERE'; // Use env var
const RACE_WALLET_ADDRESS = process.env.RACE_WALLET_ADDRESS || 'YOUR_RACE_WALLET_ADDRESS_HERE'; // Use env var
const MIN_BET = 0.01;
const MAX_BET = 1.0;
const LOG_DIR = './data'; // Note: Filesystem might be ephemeral on Railway unless using a volume
const LOG_PATH = './data/bets.json'; // This file logging will likely be replaced by DB logging

// These in-memory objects will be replaced or refactored to use the database:
const raceSessions = {};
const userRaceBets = {};
let nextRaceId = 1; // This might need to be managed differently or based on DB
const RACE_MIN_BET = 0.01;
const RACE_MAX_BET = 1.0;
const userBets = {};
const coinFlipSessions = {};
const linkedWallets = {}; // This could potentially be a table in the DB
const userPayments = {}; // Tracking used TXs will be done via DB status/used_tx_id/memo_id
const confirmCooldown = {}; // This can likely remain in memory
const cooldownInterval = 3000;


// --- Existing Functions (checkPayment, sendSol, etc.) ---
// !!! We will need to modify or replace checkPayment significantly later !!!
async function checkPayment(expectedSol, userId, gameType, targetWalletAddress) {
    // ... (Keep existing unreliable checkPayment for now, will replace later) ...
    // This function needs to be replaced with logic that:
    // 1. Monitors transactions to MAIN_WALLET_ADDRESS / RACE_WALLET_ADDRESS
    // 2. Extracts the MEMO from relevant transactions
    // 3. Calls a DB function like `findBetByMemo`
    // 4. Updates the bet status in the DB via `updateBetStatus`
    // We are NOT fixing this function in this step.
    console.warn("WARNING: Current checkPayment function is placeholder and unreliable. Needs replacement with Memo checking logic.");
    const pubKey = new PublicKey(targetWalletAddress);
    const signatures = await connection.getSignaturesForAddress(pubKey, { limit: 5 });

    for (let sig of signatures) {
        const tx = await connection.getParsedTransaction(sig.signature);
        if (!tx || !tx.meta) continue;

        const txTime = new Date(sig.blockTime * 1000);
        const timeDiff = (new Date() - txTime) / 1000 / 60;
        if (timeDiff > 15) continue; // Old tx

        // !! TEMPORARY amount check !!
        const amountLamportsReceived = (tx.meta.postBalances[0] - tx.meta.preBalances[0]);
        const expectedLamports = Math.round(expectedSol * LAMPORTS_PER_SOL);

         // Basic amount check (still flawed, just placeholder)
        if (Math.abs(amountLamportsReceived - expectedLamports) < 5000) { // Compare lamports, allow small diff for fees?
             // Still missing memo check!
            console.log(`Placeholder checkPayment found potential match: ${sig.signature}`);
            // We need to get payer and check if TX already used IN DATABASE now
            // Returning success prematurely here for temporary compatibility:
            return { success: true, tx: sig.signature };
        }
    }
    return { success: false, message: 'Payment not found (placeholder logic).' };
}

// sendSol seems okay, but ensure BOT_PRIVATE_KEY and RACE_BOT_PRIVATE_KEY are securely in Railway env vars
async function sendSol(connection, payerPrivateKey, recipientPublicKey, amount) {
    // ... (keep existing sendSol function) ...
    const maxRetries = 3;
    let lastError = null;

    for (let retryCount = 0; retryCount < maxRetries; retryCount++) {
        try {
            const payerWallet = Keypair.fromSecretKey(bs58.decode(payerPrivateKey));
            const payerPublicKey = payerWallet.publicKey;
            // Ensure amount is converted to Lamports correctly if passed as SOL
            const payoutAmountLamports = Math.round(amount * LAMPORTS_PER_SOL);

            const latestBlockhash = await connection.getLatestBlockhash();

            const transaction = new Transaction({
                recentBlockhash: latestBlockhash.blockhash,
                feePayer: payerPublicKey,
            }).add(
                SystemProgram.transfer({
                    fromPubkey: payerPublicKey,
                    toPubkey: new PublicKey(recipientPublicKey), // Ensure recipientPublicKey is PublicKey object
                    lamports: payoutAmountLamports,
                })
            );

            // Add compute budget instructions (optional but good practice)
            const modifyComputeUnits = ComputeBudgetProgram.setComputeUnitLimit({
                units: 100000, // Adjust as needed
            });
            const addPriorityFee = ComputeBudgetProgram.setComputeUnitPrice({
                microLamports: 1000, // Adjust as needed
            });
            transaction.add(modifyComputeUnits, addPriorityFee);

            // Sign the transaction
            // transaction.sign(payerWallet); // sign outside sendAndConfirmTransaction

            const signature = await sendAndConfirmTransaction(
                connection,
                transaction,
                [payerWallet], // Pass signer keypair here
                { commitment: 'confirmed', skipPreflight: false } // skipPreflight default is false
            );

            console.log(`Sent ${amount} SOL. Signature: ${signature}`);
            return { success: true, signature };

        } catch (error) {
            console.error(`Error sending SOL (attempt ${retryCount + 1}/${maxRetries}):`, error);
            lastError = error;
            // Simple retry delay for expired blockhash
            if (error.message.includes('blockhash') || error.message.includes('Blockhash not found')) {
                // Add a small delay before retrying
                 await new Promise(resolve => setTimeout(resolve, 1500 * (retryCount + 1)));
            } else {
                 // Don't retry for other errors
                break;
            }
        }
    }
    console.error(`Failed to send SOL after ${maxRetries} retries.`);
    return { success: false, error: lastError ? lastError.message : 'Max retries exceeded without success' };
}

// getHouseEdge seems okay
const getHouseEdge = (amount) => {
    // ... (keep existing getHouseEdge function) ...
    if (amount <= 0.01) return 0.70;
    if (amount <= 0.049) return 0.75;
    if (amount <= 0.0999999) return 0.80;
    return 0.99; // This seems high, maybe 0.95 or similar? Review house edge logic.
};

// Filesystem checks might not work as expected on Railway unless using a volume
if (!fs.existsSync(LOG_DIR)) {
    try { fs.mkdirSync(LOG_DIR); } catch (e) { console.warn("Could not create LOG_DIR", e.message)}
}
if (!fs.existsSync(LOG_PATH)) {
    try { fs.writeFileSync(LOG_PATH, '[]'); } catch (e) { console.warn("Could not write initial LOG_PATH", e.message)}
}


// getPayerFromTransaction seems okay, but relies on transaction structure
function getPayerFromTransaction(tx, expectedAmount) {
    // ... (keep existing getPayerFromTransaction function) ...
    // Note: This heuristic might not always be reliable.
    if (!tx || !tx.meta || !tx.transaction || !tx.transaction.message || !tx.transaction.message.accountKeys) return null;

    const message = tx.transaction.message;
    const keys = message.accountKeys.map(keyObj => keyObj.pubkey ? keyObj.pubkey : new PublicKey(keyObj)); // Ensure PublicKeys
    const preBalances = tx.meta.preBalances;
    const postBalances = tx.meta.postBalances;

    for (let i = 0; i < keys.length; i++) {
        // Ensure the index is valid for balance arrays
        if (i >= preBalances.length || i >= postBalances.length) continue;

        const key = keys[i];
        const balanceDiff = (preBalances[i] - postBalances[i]); // Lamports difference

        // Check if this account is a signer and if their balance decreased by roughly the expected amount
        // Allow for transaction fees by checking >= expectedAmountLamports
        const expectedAmountLamports = Math.round(expectedAmount * LAMPORTS_PER_SOL);
        if (message.accountKeys[i]?.signer && balanceDiff >= expectedAmountLamports - 5000 /* small fee tolerance */ ) {
            // Return the PublicKey object
            return key;
        }
    }
    console.warn("getPayerFromTransaction: Could not reliably determine payer.");
    return null; // Return null if no suitable payer found
}


// --- Existing Bot Command Handlers ---
// These will need significant changes later to use DB functions

bot.onText(/\/start$/, async (msg) => {
    const chatId = msg.chat.id;
    // Using a static image URL might be more reliable than Giphy if it goes down
    const imageUrl = 'https://i.ibb.co/9vDo58q/banner.gif'; // Example static URL

    try {
        await bot.sendAnimation(chatId, imageUrl, {
            caption: `🎰 Welcome to *Solana Gambles*!\n\nAvailable games:\n- /coinflip\n- /race\n\nType /refresh to see this menu again.`,
            parse_mode: 'Markdown'
        });
    } catch (error) {
        console.error("Error sending start animation:", error);
        // Fallback to text message if animation fails
        await bot.sendMessage(chatId, `🎰 Welcome to *Solana Gambles*!\n\nAvailable games:\n- /coinflip\n- /race\n\nType /refresh to see this menu again.`, { parse_mode: 'Markdown' });
    }
});

// Reset might need adjustment depending on how state is fully managed in DB
bot.onText(/\/reset$/, async (msg) => {
    // ... (keep existing /reset command for now) ...
    // Note: This only clears in-memory state, doesn't affect DB yet.
    const chatId = msg.chat.id;

    if (msg.chat.type === 'group' || msg.chat.type === 'supergroup') {
        await bot.sendMessage(chatId, `⚠️ The /reset command is disabled in group chats.`);
    } else {
        // We need a DB-aware reset function later
        resetBotState(chatId); // This only clears memory
        await bot.sendMessage(chatId, `🔄 In-memory bot state has been reset. Database state is unaffected.`);
    }
});

// This function only clears memory state - needs updating later
function resetBotState(chatId) {
    delete coinFlipSessions[chatId];
    delete userBets[chatId];
    delete userRaceBets[chatId];
    delete userPayments[chatId];
    // Clearing raceSessions might not be right if multiple users use races
    // for (const raceId in raceSessions) {
    //     if (raceSessions[raceId].status === 'open') {
    //         delete raceSessions[raceId];
    //     }
    // }
    console.log(`In-memory state cleared for chat ${chatId}`);
    // Optionally send messages back
    // bot.sendMessage(chatId, `🎰 *Welcome to Solana Gambles!*\n\n...`, { parse_mode: "Markdown" });
}


bot.onText(/\/coinflip$/, (msg) => {
    // ... (keep existing /coinflip command - starts the process) ...
    const userId = msg.from.id;
    // This session flag will be replaced by DB status checks
    // coinFlipSessions[userId] = true;
    bot.sendMessage(
        msg.chat.id,
        `🪙 You've started a coin flip game! Please choose an amount and heads/tails:\n\n` +
        `/bet 0.01 heads\n/bet 0.05 tails\n\nMin: ${MIN_BET} SOL | Max: ${MAX_BET} SOL`,
        { parse_mode: 'Markdown' }
    );
});

// /wallet should eventually query the DB for the linked wallet
bot.onText(/\/wallet$/, async (msg) => {
    // ... (keep existing /wallet command - uses in-memory linkedWallets) ...
    const userId = msg.from.id;
    const wallet = linkedWallets[userId]; // Needs DB lookup later
    if (wallet) {
        await bot.sendMessage(msg.chat.id, `💰 Your linked wallet address is:\n\`${wallet}\``, { parse_mode: 'Markdown' });
    } else {
        await bot.sendMessage(msg.chat.id, `⚠️ No wallet is linked to your account yet. Place a verified bet to link one.`);
    }
});

bot.onText(/\/refresh$/, async (msg) => {
    // ... (keep existing /refresh command) ...
     const chatId = msg.chat.id;
    const imageUrl = 'https://i.ibb.co/9vDo58q/banner.gif'; // Example static URL
     try {
         await bot.sendAnimation(chatId, imageUrl, {
             caption: `🎰 Welcome to Solana Gambles!\n\nAvailable games:\n- /coinflip\n- /race`,
             parse_mode: 'Markdown'
         });
     } catch (error) {
         console.error("Error sending refresh animation:", error);
         await bot.sendMessage(chatId, `🎰 Welcome to Solana Gambles!\n\nAvailable games:\n- /coinflip\n- /race`, { parse_mode: 'Markdown' });
     }
});


// --- /bet command needs significant changes ---
bot.onText(/\/bet (\d+\.?\d*) (heads|tails)/i, async (msg, match) => { // Allow integer amounts too
    // !!! THIS FUNCTION NEEDS TO BE REWRITTEN TO USE THE DATABASE !!!
    const userId = msg.from.id;
    const chatId = msg.chat.id;

    // Check if user already has a pending bet in the DB? (Optional enhancement)

    const betAmount = parseFloat(match[1]);
    const userChoice = match[2].toLowerCase();

    if (isNaN(betAmount) || betAmount < MIN_BET || betAmount > MAX_BET) {
        return bot.sendMessage(chatId, `⚠️ Bet must be between ${MIN_BET} - ${MAX_BET} SOL`);
    }

    // 1. Generate Unique Memo ID
    const memoId = `CF${userId}${Date.now()}`; // Example: CF[UserID][Timestamp] - Ensure uniqueness

    // 2. Calculate Lamports & Expiry
    const expectedLamports = Math.round(betAmount * LAMPORTS_PER_SOL);
    const expiryMinutes = 15;
    const expiresAt = new Date(Date.now() + expiryMinutes * 60 * 1000);

    // 3. Save Bet to Database
    const betDetails = { choice: userChoice };
    const saveResult = await savePendingBet(userId, chatId, 'coinflip', betDetails, expectedLamports, memoId, expiresAt);

    if (!saveResult.success) {
        console.error(`Failed to save bet to DB for user ${userId}:`, saveResult.error);
        return bot.sendMessage(chatId, `⚠️ There was an error registering your bet. Please try again.`);
    }

    // 4. Instruct User
    await bot.sendMessage(chatId,
        `✅ Bet registered (ID: ${saveResult.id})!\n\n`+
        `💸 Send *exactly ${betAmount.toFixed(6)} SOL* to:\n` + // Show more precision
        `\`${MAIN_WALLET_ADDRESS}\`\n\n` +
        `*IMPORTANT:* Include this Memo ID in your transaction:\n` +
        `\`${memoId}\`\n\n` +
        `Payment must be received within ${expiryMinutes} minutes.`,
        { parse_mode: 'Markdown' }
    );

    // Remove old logic:
    // if (!coinFlipSessions[userId]) {
    //     return bot.sendMessage(chatId, `⚠️ Please start a coin flip game first using /coinflip`);
    // }
    // userBets[userId] = { amount: betAmount, choice: userChoice };
    // await bot.sendMessage(chatId, `... type /confirm ...`); // No longer need /confirm
});


// --- /confirm command needs to be REMOVED or DISABLED ---
// We won't use /confirm with the Memo method. Payment verification will be automatic.
bot.onText(/^\/confirm$/, async (msg) => {
    const chatId = msg.chat.id;
    await bot.sendMessage(chatId, `⚠️ The /confirm command is no longer needed. Payment verification is automatic when you send SOL with the correct Memo ID.`);
});

// --- /race command - similar changes needed as /coinflip & /bet ---
bot.onText(/\/race$/, async (msg) => {
    // ... (keep existing /race setup message for now) ...
    // Needs refactoring later if race state is stored in DB
    const chatId = msg.chat.id;
    // const raceId = nextRaceId++; // Managing race IDs needs thought with DB
    const horses = [ /* ... horse data ... */
        { name: 'Yellow', emoji: '🟡', odds: 1.1, winProbability: 0.25 },
        { name: 'Orange', emoji: '🟠', odds: 2.0, winProbability: 0.20 },
        { name: 'Blue', emoji: '🔵', odds: 3.0, winProbability: 0.15 },
        { name: 'Cyan', emoji: '🔷', odds: 4.0, winProbability: 0.12 },
        { name: 'White', emoji: '⚪', odds: 5.0, winProbability: 0.09 },
        { name: 'Red', emoji: '🔴', odds: 6.0, winProbability: 0.07 },
        { name: 'Black', emoji: '⚫', odds: 7.0, winProbability: 0.05 },
        { name: 'Pink', emoji: '🌸', odds: 8.0, winProbability: 0.03 },
        { name: 'Purple', emoji: '🟣', odds: 9.0, winProbability: 0.02 },
        { name: 'Green', emoji: '🟢', odds: 10.0, winProbability: 0.01 },
        { name: 'Silver', emoji: '💎', odds: 15.0, winProbability: 0.01 },
    ];

    // Storing race sessions in memory is problematic for persistence
    // raceSessions[raceId] = { horses, status: 'open' };

    let raceMessage = `🏇 New Race! Place your bets!\n\n`;
    horses.forEach(horse => {
        raceMessage += `${horse.emoji} *${horse.name}* (Odds: ${horse.odds.toFixed(1)}x)\n`;
    });
    raceMessage += `\nTo place your bet, use:\n\`/betrace [amount] [horse_name]\`\n` +
        `Example: \`/betrace 0.1 Blue\``;

    await bot.sendMessage(chatId, raceMessage, { parse_mode: 'Markdown' });
});


// --- /betrace command needs significant changes ---
bot.onText(/\/betrace (\d+\.?\d*) (\w+)/i, async (msg, match) => { // Allow integer amounts
    // !!! THIS FUNCTION NEEDS TO BE REWRITTEN TO USE THE DATABASE !!!
    const chatId = msg.chat.id;
    const userId = msg.from.id;
    const betAmount = parseFloat(match[1]);
    const chosenHorseName = match[2]; // Keep case for potential display? Match lower later.

    // Find horse info (currently hardcoded, could be in DB later)
     const horses = [ /* ... horse data ... */
        { name: 'Yellow', emoji: '🟡', odds: 1.1, winProbability: 0.25 }, { name: 'Orange', emoji: '🟠', odds: 2.0, winProbability: 0.20 }, { name: 'Blue', emoji: '🔵', odds: 3.0, winProbability: 0.15 }, { name: 'Cyan', emoji: '🔷', odds: 4.0, winProbability: 0.12 }, { name: 'White', emoji: '⚪', odds: 5.0, winProbability: 0.09 }, { name: 'Red', emoji: '🔴', odds: 6.0, winProbability: 0.07 }, { name: 'Black', emoji: '⚫', odds: 7.0, winProbability: 0.05 }, { name: 'Pink', emoji: '🌸', odds: 8.0, winProbability: 0.03 }, { name: 'Purple', emoji: '🟣', odds: 9.0, winProbability: 0.02 }, { name: 'Green', emoji: '🟢', odds: 10.0, winProbability: 0.01 }, { name: 'Silver', emoji: '💎', odds: 15.0, winProbability: 0.01 },
    ];
    const horse = horses.find(h => h.name.toLowerCase() === chosenHorseName.toLowerCase());

    if (!horse) {
        return bot.sendMessage(chatId, `⚠️ Invalid horse name. Options:\n` +
            horses.map(h => `${h.emoji} ${h.name}`).join('\n'));
    }

    if (isNaN(betAmount) || betAmount < RACE_MIN_BET || betAmount > RACE_MAX_BET) {
        return bot.sendMessage(chatId, `⚠️ Bet must be between ${RACE_MIN_BET} - ${RACE_MAX_BET} SOL`);
    }

     // 1. Generate Unique Memo ID
    const memoId = `RA${userId}${Date.now()}`; // Example: RA[UserID][Timestamp] - Ensure uniqueness

    // 2. Calculate Lamports & Expiry
    const expectedLamports = Math.round(betAmount * LAMPORTS_PER_SOL);
    const expiryMinutes = 15;
    const expiresAt = new Date(Date.now() + expiryMinutes * 60 * 1000);

    // 3. Save Bet to Database
    const betDetails = { horse: horse.name, odds: horse.odds }; // Store odds too maybe
    const saveResult = await savePendingBet(userId, chatId, 'race', betDetails, expectedLamports, memoId, expiresAt);

    if (!saveResult.success) {
        console.error(`Failed to save race bet to DB for user ${userId}:`, saveResult.error);
        return bot.sendMessage(chatId, `⚠️ There was an error registering your race bet. Please try again.`);
    }

     // 4. Instruct User
    await bot.sendMessage(chatId,
        `✅ Race bet registered (ID: ${saveResult.id}) on ${horse.emoji} *${horse.name}* (Odds: ${horse.odds.toFixed(1)}x)!\n\n`+
        `💸 Send *exactly ${betAmount.toFixed(6)} SOL* to:\n` + // Show more precision
        `\`${RACE_WALLET_ADDRESS}\`\n\n` + // Use the specific race wallet
        `*IMPORTANT:* Include this Memo ID in your transaction:\n` +
        `\`${memoId}\`\n\n` +
        `Payment must be received within ${expiryMinutes} minutes.`,
        { parse_mode: 'Markdown' }
    );

    // Remove old logic:
    // const raceId = Object.keys(raceSessions).reverse().find(id => raceSessions[id].status === 'open');
    // if (!raceId) { return bot.sendMessage(chatId, `⚠️ No race is currently accepting bets.`); }
    // userRaceBets[userId] = { raceId, amount: betAmount, horse: horse.name };
    // await bot.sendMessage(chatId, `... type /confirmrace ...`); // No longer need /confirmrace
});


// --- /confirmrace command needs to be REMOVED or DISABLED ---
bot.onText(/^\/confirmrace$/, async (msg) => {
    const chatId = msg.chat.id;
    await bot.sendMessage(chatId, `⚠️ The /confirmrace command is no longer needed. Payment verification is automatic when you send SOL with the correct Memo ID.`);
});


// --- Add Payment Monitoring Logic ---
// This is a crucial piece that needs to be added.
// It should run periodically to check for transactions with memos.

// Placeholder - needs proper implementation
async function monitorPayments() {
    console.log("Running payment monitor cycle (placeholder)...");
    // 1. Get pending bets from DB (status 'awaiting_memo_payment' and not expired)
    // const pendingBets = await findPendingBetsFromDB(); // Need to write DB query function

    // 2. For each relevant wallet (MAIN_WALLET_ADDRESS, RACE_WALLET_ADDRESS):
    //    - Get recent transaction signatures using connection.getSignaturesForAddress()
    //    - Keep track of signatures already processed (maybe store last checked signature in DB/memory)

    // 3. For each new signature:
    //    - Fetch the full transaction: connection.getParsedTransaction(signature)
    //    - Check if it contains a Memo instruction (using spl-memo library or manual parsing)
    //    - Extract the memo text

    // 4. If a memo is found:
    //    - Look up the memo in the DB: const bet = await findBetByMemo(extractedMemo);
    //    - If bet found AND status is 'awaiting_memo_payment':
    //       - Verify amount matches bet.expected_lamports (approx)
    //       - Verify recipient address in TX matches MAIN_WALLET_ADDRESS or RACE_WALLET_ADDRESS for the game type
    //       - If all checks pass:
    //          - Mark bet as 'payment_verified' in DB: await updateBetStatus(bet.id, 'payment_verified');
    //          - Trigger game processing logic (pass the bet record) -> processPaidBet(bet);
    //       - Else (amount mismatch, etc.):
    //          - Mark bet as 'error_payment_mismatch' or similar in DB.

    // This function should handle errors gracefully (RPC limits, etc.)
}

// Run monitor periodically (e.g., every 15-30 seconds)
// Make sure interval is not too short to avoid hitting RPC rate limits
// const monitorInterval = setInterval(monitorPayments, 30000); // 30 seconds

console.log("Bot started successfully.");

// Graceful shutdown (optional but good practice)
process.on('SIGINT', () => {
  console.log("Shutting down bot...");
  // clearInterval(monitorInterval); // Stop monitor
  // Add any other cleanup logic here
  process.exit(0);
});
process.on('SIGTERM', () => {
  console.log("Shutting down bot...");
  // clearInterval(monitorInterval); // Stop monitor
  process.exit(0);
});


// --- Database Interaction Functions (Placeholders - Need Implementation) ---

// We defined savePendingBet within the /bet and /betrace handlers for now.
// Need to define findBetByMemo and updateBetStatus (similar to examples shown before)

async function findBetByMemo(memoId) {
   console.log(`DB: Searching for memo ${memoId}`);
   // IMPORTANT: Replace with actual DB query using 'pool'
   const query = 'SELECT * FROM bets WHERE memo_id = $1 AND status = $2';
   try {
       const res = await pool.query(query, [memoId, 'awaiting_memo_payment']);
       console.log(`DB: Found ${res.rowCount} bet(s) for memo ${memoId}`);
       return res.rows[0]; // Returns the first matching row (or undefined)
   } catch (err) {
       console.error("DB Error: Error finding bet by memo:", err);
       return undefined;
   }
}

async function updateBetStatus(betId, newStatus) {
   console.log(`DB: Updating bet ${betId} status to ${newStatus}`);
   // IMPORTANT: Replace with actual DB query using 'pool'
   const query = 'UPDATE bets SET status = $1 WHERE id = $2';
   try {
       const res = await pool.query(query, [newStatus, betId]);
       console.log(`DB: Update status result rows affected: ${res.rowCount}`);
       return { success: res.rowCount > 0 };
   } catch (err) {
       console.error(`DB Error: Error updating bet ${betId} status:`, err);
       return { success: false, error: err.message };
   }
}


// --- Game Processing Logic (Needs Implementation) ---

async function processPaidBet(bet) {
    // This function gets called by the monitor when a payment is verified
    console.log(`Processing paid bet ID: ${bet.id}, Type: ${bet.game_type}`);

    if (bet.game_type === 'coinflip') {
        await handleCoinflipGame(bet);
    } else if (bet.game_type === 'race') {
        await handleRaceGame(bet);
    } else {
        console.error(`Unknown game type for bet ${bet.id}: ${bet.game_type}`);
        await updateBetStatus(bet.id, 'error_unknown_game');
    }
}

async function handleCoinflipGame(bet) {
    console.log(`Handling coinflip game for bet ${bet.id}`);
    const { user_id, chat_id, bet_details, expected_lamports } = bet;
    const amount = expected_lamports / LAMPORTS_PER_SOL;
    const choice = bet_details.choice;

    // --- Coinflip Logic ---
    const houseEdge = getHouseEdge(amount); // Using existing function
    // Determine winner based on house edge
    // Using Math.random() is okay for simple games, but consider Provably Fair methods for real casinos
    const randomRoll = Math.random();
    let result;
    if (randomRoll < 0.5 * (1 - houseEdge)) { // Win condition (adjust probability based on edge)
         result = choice;
    } else {
         result = (choice === 'heads' ? 'tails' : 'heads');
    }
    // Old logic: const result = Math.random() > houseEdge ? choice : (choice === 'heads' ? 'tails' : 'heads'); - review this logic if houseEdge is < 0.5


    const win = (result === choice);
    const payout = win ? amount * 2 : 0; // Standard coinflip payout is 2x bet amount

    // Get display name (optional)
    let displayName = String(user_id);
    try {
        const chatMember = await bot.getChatMember(chat_id, user_id);
        displayName = chatMember.user.username ? `@${chatMember.user.username}` : chatMember.user.first_name;
    } catch (e) { console.warn("Couldn't get username for payout message", e.message); }

    // --- Payout or Lose Message ---
    if (win) {
        console.log(`Bet ${bet.id}: User ${user_id} WON ${payout} SOL!`);
        const payerPrivateKey = process.env.BOT_PRIVATE_KEY;
        if (!payerPrivateKey) {
            console.error('FATAL: BOT_PRIVATE_KEY environment variable not set!');
            await bot.sendMessage(chat_id, `🎉 Congratulations, ${displayName}! You won ${payout.toFixed(4)} SOL!\nResult: ${result}\n⚠️ Payout failed: Bot misconfiguration.`);
            await updateBetStatus(bet.id, 'error_payout_config');
            return;
        }

        // Fetch payer address for payout
        // We need the transaction hash from the payment verification step.
        // This needs adjustment - the monitor should pass the verified TX details or payer address.
        // For now, attempt to get linked wallet or fail.
        // --> This part REQUIRES redesigning the monitor <---
        let winnerAddress = linkedWallets[user_id]; // !!! TEMPORARY - NEED PAYER FROM VERIFIED TX !!!
        if (!winnerAddress) {
             console.error(`Bet ${bet.id}: Cannot find winner address for payout!`);
              await bot.sendMessage(chat_id, `🎉 Congratulations, ${displayName}! You won ${payout.toFixed(4)} SOL!\nResult: ${result}\n⚠️ Payout failed: Could not determine your wallet address.`);
              await updateBetStatus(bet.id, 'error_payout_no_wallet');
              return;
        }

        try {
             const winnerPublicKey = new PublicKey(winnerAddress);
             await bot.sendMessage(chat_id, `🎉 Congratulations, ${displayName}! You won ${payout.toFixed(4)} SOL!\nResult: ${result}\n\n💸 Sending payout...`);
             const sendResult = await sendSol(connection, payerPrivateKey, winnerPublicKey, payout);

             if (sendResult.success) {
                 await bot.sendMessage(chat_id, `💰 Payout successful! TX: \`${sendResult.signature}\``, { parse_mode: 'Markdown'});
                 await updateBetStatus(bet.id, 'completed_win_paid');
             } else {
                 await bot.sendMessage(chat_id, `⚠️ Payout failed: ${sendResult.error}. Please contact support.`);
                 await updateBetStatus(bet.id, 'error_payout_failed');
             }
         } catch(e) {
             console.error(`Bet ${bet.id}: Error during payout process:`, e);
             await bot.sendMessage(chat_id, `⚠️ Payout failed due to an internal error. Please contact support.`);
             await updateBetStatus(bet.id, 'error_payout_exception');
         }

    } else {
        console.log(`Bet ${bet.id}: User ${user_id} LOST.`);
        await bot.sendMessage(chat_id, `❌ *YOU LOSE!*\n\nYou guessed *${choice}*, but the coin landed *${result}*.`, { parse_mode: "Markdown" });
        await updateBetStatus(bet.id, 'completed_loss');
    }
}


async function handleRaceGame(bet) {
    console.log(`Handling race game for bet ${bet.id}`);
     const { user_id, chat_id, bet_details, expected_lamports } = bet;
     const amount = expected_lamports / LAMPORTS_PER_SOL;
     const horseName = bet_details.horse;
     const odds = bet_details.odds; // We stored odds in bet_details

     // --- Race Logic ---
     const horses = [ /* ... horse data ... */
        { name: 'Yellow', emoji: '🟡', odds: 1.1, winProbability: 0.25 }, { name: 'Orange', emoji: '🟠', odds: 2.0, winProbability: 0.20 }, { name: 'Blue', emoji: '🔵', odds: 3.0, winProbability: 0.15 }, { name: 'Cyan', emoji: '🔷', odds: 4.0, winProbability: 0.12 }, { name: 'White', emoji: '⚪', odds: 5.0, winProbability: 0.09 }, { name: 'Red', emoji: '🔴', odds: 6.0, winProbability: 0.07 }, { name: 'Black', emoji: '⚫', odds: 7.0, winProbability: 0.05 }, { name: 'Pink', emoji: '🌸', odds: 8.0, winProbability: 0.03 }, { name: 'Purple', emoji: '🟣', odds: 9.0, winProbability: 0.02 }, { name: 'Green', emoji: '🟢', odds: 10.0, winProbability: 0.01 }, { name: 'Silver', emoji: '💎', odds: 15.0, winProbability: 0.01 },
    ]; // Use the same horse data as before

    let winningHorse;
    const randomNumber = Math.random();
    let cumulativeProbability = 0;
    for (const contender of horses) {
        cumulativeProbability += contender.winProbability;
        if (randomNumber <= cumulativeProbability) { // Use <= to ensure one always wins
            winningHorse = contender;
            break;
        }
    }
     if (!winningHorse) winningHorse = horses[horses.length - 1]; // Fallback just in case


    // Send race commentary...
    await bot.sendMessage(chatId, `🏇 Race for Bet ID ${bet.id} is on!`);
    await new Promise(resolve => setTimeout(resolve, 1000));
    // ... (add more commentary as before if desired) ...
    await bot.sendMessage(chatId, `🏁 **And the winner is... ${winningHorse.emoji} *${winningHorse.name}*!** 🏁`, { parse_mode: 'Markdown' });

    // --- Payout or Lose Message ---
    const win = (horseName === winningHorse.name);
    const payout = win ? amount * odds : 0;

    let displayName = String(user_id);
     try { /* ... get display name ... */ } catch (e) { /* ... */ }

    if (win) {
        console.log(`Bet ${bet.id}: User ${user_id} WON Race ${payout} SOL!`);
        const payerPrivateKey = process.env.RACE_BOT_PRIVATE_KEY; // Use the RACE key
        if (!payerPrivateKey) {
            console.error('FATAL: RACE_BOT_PRIVATE_KEY environment variable not set!');
             await bot.sendMessage(chat_id, `🎉 Congratulations, ${displayName}! Your horse *${horseName}* won!\nPayout: ${payout.toFixed(4)} SOL\n⚠️ Payout failed: Bot misconfiguration.`);
            await updateBetStatus(bet.id, 'error_payout_config');
            return;
        }

        // !!! TEMPORARY - NEED PAYER FROM VERIFIED TX !!!
        let winnerAddress = linkedWallets[user_id];
        if (!winnerAddress) {
             console.error(`Bet ${bet.id}: Cannot find winner address for race payout!`);
              await bot.sendMessage(chat_id, `🎉 Congratulations, ${displayName}! Your horse *${horseName}* won!\nPayout: ${payout.toFixed(4)} SOL\n⚠️ Payout failed: Could not determine your wallet address.`);
              await updateBetStatus(bet.id, 'error_payout_no_wallet');
              return;
        }

         try {
             const winnerPublicKey = new PublicKey(winnerAddress);
             await bot.sendMessage(chat_id, `🎉 Congratulations, ${displayName}! Your horse *${horseName}* won!\nPayout: ${payout.toFixed(4)} SOL\n\n💸 Sending payout...`);
             const sendResult = await sendSol(connection, payerPrivateKey, winnerPublicKey, payout);

             if (sendResult.success) {
                 await bot.sendMessage(chat_id, `💰 Race payout successful! TX: \`${sendResult.signature}\``, { parse_mode: 'Markdown'});
                 await updateBetStatus(bet.id, 'completed_win_paid');
             } else {
                 await bot.sendMessage(chat_id, `⚠️ Race payout failed: ${sendResult.error}. Please contact support.`);
                 await updateBetStatus(bet.id, 'error_payout_failed');
             }
         } catch(e) {
             console.error(`Bet ${bet.id}: Error during race payout process:`, e);
             await bot.sendMessage(chat_id, `⚠️ Race payout failed due to an internal error. Please contact support.`);
             await updateBetStatus(bet.id, 'error_payout_exception');
         }

    } else {
         console.log(`Bet ${bet.id}: User ${user_id} LOST Race.`);
        await bot.sendMessage(chat_id, `❌ *Your horse lost!*\n\nYou picked *${horseName}*, but *${winningHorse.name}* won the race.`, { parse_mode: "Markdown" });
        await updateBetStatus(bet.id, 'completed_loss');
    }
}
