require('dotenv').config(); // For local development with .env file

// --- Environment Variable Checks ---
const REQUIRED_ENV_VARS = [
    'BOT_TOKEN',          // For Telegram API
    'DATABASE_URL',       // For PostgreSQL connection (provided by Railway)
    'BOT_PRIVATE_KEY',    // For Coinflip payouts
    'RACE_BOT_PRIVATE_KEY', // For Race payouts
    'MAIN_WALLET_ADDRESS',// Wallet for Coinflip bets
    'RACE_WALLET_ADDRESS',// Wallet for Race bets
    'RPC_URL'             // Solana RPC endpoint
];
let missingVars = false;
REQUIRED_ENV_VARS.forEach((key) => {
    if (!process.env[key]) {
        console.error(`❌ Environment variable ${key} is missing.`);
        missingVars = true;
    }
});
if (missingVars) {
    console.error("Please set all required environment variables. Exiting.");
    process.exit(1);
}

// --- Requires ---
const express = require('express');
const { Pool } = require('pg');
const TelegramBot = require('node-telegram-bot-api');
const {
    Connection,
    PublicKey,
    LAMPORTS_PER_SOL,
    Keypair,
    Transaction,
    SystemProgram,
    sendAndConfirmTransaction,
    ComputeBudgetProgram,
    // MemoProgram, // Not strictly needed if just parsing memo text
} = require('@solana/web3.js');
const bs58 = require('bs58');
const { randomBytes } = require('crypto'); // For generating unique memos (better than timestamp)

const app = express();

// --- PostgreSQL Setup ---
console.log("Setting up PostgreSQL Pool...");
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  // Consider adding SSL config if needed for Railway, but test without first
  // ssl: { rejectUnauthorized: false }
});
console.log("PostgreSQL Pool created.");

// --- Database Initialization ---
async function initializeDatabase() {
  console.log("Initializing Database...");
  let client;
  try {
    client = await pool.connect();
    console.log("DB client connected.");
    // Bets table stores pending/completed bets & links via memo_id
    await client.query(`
      CREATE TABLE IF NOT EXISTS bets (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        chat_id TEXT NOT NULL,
        game_type TEXT NOT NULL, -- 'coinflip', 'race'
        bet_details JSONB,      -- { "choice": "heads" } or { "horse": "Blue", "odds": 3.0 }
        expected_lamports BIGINT NOT NULL,
        memo_id TEXT UNIQUE NOT NULL, -- Ensure memos are unique
        status TEXT NOT NULL,    -- 'awaiting_payment', 'payment_verified', 'processing_game', 'completed_win_paid', 'completed_win_payout_failed', 'completed_loss', 'expired', 'error_payment_mismatch', 'error_unknown'
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        expires_at TIMESTAMPTZ NOT NULL,
        paid_tx_signature TEXT UNIQUE, -- Store the signature that paid this bet
        payout_tx_signature TEXT UNIQUE -- Store the payout signature if successful
      );
    `);
    console.log("✅ Database table 'bets' checked/created successfully.");

    // Wallets table to link user_id to their Solana wallet address
    await client.query(`
        CREATE TABLE IF NOT EXISTS wallets (
            user_id TEXT PRIMARY KEY,
            wallet_address TEXT NOT NULL,
            linked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    `);
     console.log("✅ Database table 'wallets' checked/created successfully.");

  } catch (err) {
    console.error("❌ Error initializing database tables:", err);
    throw err;
  } finally {
    if (client) {
      client.release();
      console.log("ℹ️ Database client released after initialization check.");
    }
  }
}
// --- End PostgreSQL Setup ---


// --- Bot and Solana Initialization ---
console.log("Initializing Telegram Bot (NO POLLING)...");
// Production bots should use Webhooks instead of polling
const bot = new TelegramBot(process.env.BOT_TOKEN /* { polling: false } */); // Polling explicitly off
console.log("Telegram Bot initialized.");

console.log("Initializing Solana Connection...");
const connection = new Connection(process.env.RPC_URL, 'confirmed');
console.log("Solana Connection initialized.");
// --- End Initialization ---

// --- Express Healthcheck ---
app.get('/', (req, res) => {
  // console.log("Health check endpoint '/' hit!"); // Optional: reduce log noise
  res.status(200).send('OK');
});

// --- In-Memory State (Minimal - only for non-persistent things) ---
const confirmCooldown = {}; // Still useful to prevent command spam
const cooldownInterval = 3000; // 3 seconds

// --- Constants ---
const MIN_BET = 0.01;
const MAX_BET = 1.0; // Coinflip Max
const RACE_MIN_BET = 0.01;
const RACE_MAX_BET = 1.0; // Race Max
const PAYMENT_EXPIRY_MINUTES = 15; // How long user has to pay

// --- Helper Functions ---

// Generate a unique memo ID (more robust than timestamp)
function generateMemoId(prefix = 'BET') {
    return prefix + randomBytes(6).toString('hex').toUpperCase(); // e.g., BETABC123DEF
}

// Parse Memo from Transaction Instructions
function findMemoInTx(tx) {
    if (!tx?.transaction?.message?.instructions) {
        return null;
    }
    try {
        for (const instruction of tx.transaction.message.instructions) {
            // Check if instruction is from Memo Program (Memo1UhkJRfHyvLMcVuc6beZNRYघड)
             // Comparing keys directly can be error-prone if PublicKey objects aren't stringified identically.
             // Better to check if programId matches the known Memo program ID string.
            const MEMO_PROGRAM_ID = 'Memo1UhkJRfHyvLMcVuc6beZNRYqUP2VZwW';
            let programId = '';
            if (instruction.programId) {
                 // Find the program ID key from the accountKeys array using the index
                 const programIdIndex = instruction.programIdIndex;
                 if (tx.transaction.message.accountKeys && programIdIndex < tx.transaction.message.accountKeys.length) {
                      const keyInfo = tx.transaction.message.accountKeys[programIdIndex];
                      programId = keyInfo.pubkey ? keyInfo.pubkey.toBase58() : keyInfo.toString(); // Handle potential string key
                 }
            } else if (instruction.programIdIndex !== undefined) {
                 // Alternative structure sometimes seen
                  const programIdIndex = instruction.programIdIndex;
                  if (tx.transaction.message.accountKeys && programIdIndex < tx.transaction.message.accountKeys.length) {
                       const keyInfo = tx.transaction.message.accountKeys[programIdIndex];
                       programId = keyInfo.pubkey ? keyInfo.pubkey.toBase58() : keyInfo.toString();
                  }
            }


            if (programId === MEMO_PROGRAM_ID && instruction.data) {
                // Memo data is usually base58 encoded message
                return bs58.decode(instruction.data).toString('utf-8');
            }
        }
    } catch (e) {
        console.error("Error parsing memo:", e);
    }
    return null;
}


// Get Payer (ensure it returns PublicKey object or null)
function getPayerFromTransaction(tx) {
    if (!tx || !tx.meta || !tx.transaction?.message?.accountKeys) {
        console.warn("getPayerFromTransaction: Invalid transaction data provided.");
        return null;
    }
    const message = tx.transaction.message;
    const preBalances = tx.meta.preBalances;
    const postBalances = tx.meta.postBalances;

    // The first signer is usually the fee payer and primary actor
    if (message.accountKeys.length > 0 && message.accountKeys[0]?.signer) {
        const firstSignerKey = message.accountKeys[0].pubkey
                             ? message.accountKeys[0].pubkey // Handle ParsedMessageAccount
                             : new PublicKey(message.accountKeys[0]); // Handle string address

        // Optional: Check balance change for confirmation, though less reliable
        const balanceDiff = (preBalances[0] ?? 0) - (postBalances[0] ?? 0);
        if (balanceDiff > 0) { // Ensure they actually spent SOL (incl. fees)
             console.log(`getPayerFromTransaction: Identified payer as first signer: ${firstSignerKey.toBase58()}`);
            return firstSignerKey;
        } else {
             console.warn(`getPayerFromTransaction: First signer ${firstSignerKey.toBase58()} had non-positive balance change (${balanceDiff}). Might not be payer.`);
             // Fallback or return null if strict check needed
        }
    }

    // Fallback logic (less reliable) - find first signer with balance decrease
     for (let i = 0; i < message.accountKeys.length; i++) {
        if (i >= preBalances.length || i >= postBalances.length) continue;
        if (message.accountKeys[i]?.signer) {
            const key = message.accountKeys[i].pubkey
                        ? message.accountKeys[i].pubkey
                        : new PublicKey(message.accountKeys[i]);
            const balanceDiff = (preBalances[i] ?? 0) - (postBalances[i] ?? 0);
            if (balanceDiff > 0) {
                 console.log(`getPayerFromTransaction: Identified payer via fallback (signer with balance decrease): ${key.toBase58()}`);
                return key;
            }
        }
    }

    console.error("getPayerFromTransaction: Could not determine transaction payer.");
    return null;
}

// Send SOL (ensure recipient is PublicKey)
async function sendSol(connection, payerPrivateKey, recipientPublicKey, amountLamports) {
    const maxRetries = 3;
    let lastError = null;
    const recipientPubKey = (typeof recipientPublicKey === 'string') ? new PublicKey(recipientPublicKey) : recipientPublicKey;
    const amountSOL = amountLamports / LAMPORTS_PER_SOL; // For logging

    for (let retryCount = 0; retryCount < maxRetries; retryCount++) {
        try {
            const payerWallet = Keypair.fromSecretKey(bs58.decode(payerPrivateKey));
            const payerPublicKey = payerWallet.publicKey;

            const latestBlockhash = await connection.getLatestBlockhash();

            const transaction = new Transaction({
                recentBlockhash: latestBlockhash.blockhash,
                feePayer: payerPublicKey,
            }).add(
                SystemProgram.transfer({
                    fromPubkey: payerPublicKey,
                    toPubkey: recipientPubKey, // Use the PublicKey object
                    lamports: amountLamports,
                })
            );

            // Add compute budget instructions
            const modifyComputeUnits = ComputeBudgetProgram.setComputeUnitLimit({ units: 100000 });
            const addPriorityFee = ComputeBudgetProgram.setComputeUnitPrice({ microLamports: 1000 });
            transaction.add(modifyComputeUnits, addPriorityFee);

            const signature = await sendAndConfirmTransaction(
                connection,
                transaction,
                [payerWallet],
                { commitment: 'confirmed', skipPreflight: false }
            );

            console.log(`✅ Sent ${amountSOL.toFixed(6)} SOL to ${recipientPubKey.toBase58()}. Signature: ${signature}`);
            return { success: true, signature };

        } catch (error) {
            console.error(`❌ Error sending ${amountSOL.toFixed(6)} SOL (attempt ${retryCount + 1}/${maxRetries}):`, error);
            lastError = error;
            if (error.message.includes('blockhash') || error.message.includes('Blockhash not found')) {
                await new Promise(resolve => setTimeout(resolve, 1500 * (retryCount + 1)));
            } else {
                break; // Don't retry other errors
            }
        }
    }
    console.error(`❌ Failed to send ${amountSOL.toFixed(6)} SOL after ${maxRetries} retries.`);
    return { success: false, error: lastError ? lastError.message : 'Max retries exceeded without success' };
}

// --- Database Interaction Functions ---

async function savePendingBet(userId, chatId, gameType, details, lamports, memoId, expiresAt) {
  const query = `
    INSERT INTO bets (user_id, chat_id, game_type, bet_details, expected_lamports, memo_id, status, expires_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    RETURNING id;
  `;
  const values = [ String(userId), String(chatId), gameType, details, lamports, memoId, 'awaiting_payment', expiresAt ];
  try {
    const res = await pool.query(query, values);
    console.log(`DB: Saved pending ${gameType} bet ID ${res.rows[0].id} for user ${userId} with memo ${memoId}`);
    return { success: true, id: res.rows[0].id };
  } catch (err) {
     console.error(`DB Error: Failed to save bet for user ${userId}:`, err);
     if (err.code === '23505' && err.constraint === 'bets_memo_id_key') { // Check if unique constraint failed
         return { success: false, error: 'Memo ID collision occurred.' };
     }
     return { success: false, error: err.message };
  }
}

async function findBetByMemo(memoId) {
   const query = 'SELECT * FROM bets WHERE memo_id = $1 AND status = $2';
   try {
       const res = await pool.query(query, [memoId, 'awaiting_payment']);
       if (res.rowCount > 0) {
            console.log(`DB: Found pending bet ID ${res.rows[0].id} for memo ${memoId}`);
       }
       return res.rows[0]; // Returns the first matching row (or undefined)
   } catch (err) {
       console.error(`DB Error: Error finding bet by memo ${memoId}:`, err);
       return undefined;
   }
}

async function findPendingBets() {
    // Find bets awaiting payment that haven't expired yet
    const query = `SELECT * FROM bets WHERE status = 'awaiting_payment' AND expires_at > NOW()`;
    try {
        const res = await pool.query(query);
        return res.rows;
    } catch(err) {
        console.error(`DB Error: Error finding pending bets:`, err);
        return []; // Return empty array on error
    }
}

async function updateBetStatus(betId, newStatus) {
   const query = 'UPDATE bets SET status = $1 WHERE id = $2';
   try {
       const res = await pool.query(query, [newStatus, betId]);
       console.log(`DB: Updated bet ${betId} status to ${newStatus}. Rows affected: ${res.rowCount}`);
       return { success: res.rowCount > 0 };
   } catch (err) {
       console.error(`DB Error: Error updating bet ${betId} status to ${newStatus}:`, err);
       return { success: false, error: err.message };
   }
}

// Function to record the transaction that paid the bet
async function markBetPaid(betId, txSignature) {
    const query = 'UPDATE bets SET status = $1, paid_tx_signature = $2 WHERE id = $3 AND status = $4';
    try {
        const res = await pool.query(query, ['payment_verified', txSignature, betId, 'awaiting_payment']);
        console.log(`DB: Marked bet ${betId} as paid by tx ${txSignature}. Rows affected: ${res.rowCount}`);
        // Check rowCount to ensure the status was actually 'awaiting_payment' before update
        return { success: res.rowCount > 0 };
    } catch (err) {
        console.error(`DB Error: Error marking bet ${betId} as paid:`, err);
        // Handle potential unique constraint violation on paid_tx_signature
        if (err.code === '23505' && err.constraint === 'bets_paid_tx_signature_key') {
            console.warn(`DB Warning: Transaction ${txSignature} may already be associated with another bet.`);
            return { success: false, error: 'Transaction signature already used.' };
        }
        return { success: false, error: err.message };
    }
}

// Function to link wallet or update link time
async function linkUserWallet(userId, walletAddress) {
    const query = `
        INSERT INTO wallets (user_id, wallet_address, linked_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (user_id) DO UPDATE SET
            wallet_address = EXCLUDED.wallet_address,
            linked_at = NOW();
    `;
     try {
        await pool.query(query, [String(userId), walletAddress]);
        console.log(`DB: Linked/Updated wallet for user ${userId} to ${walletAddress}`);
        return { success: true };
    } catch (err) {
        console.error(`DB Error: Error linking wallet for user ${userId}:`, err);
        return { success: false, error: err.message };
    }
}

// Function to get linked wallet
async function getLinkedWallet(userId) {
     const query = 'SELECT wallet_address FROM wallets WHERE user_id = $1';
     try {
        const res = await pool.query(query, [String(userId)]);
        return res.rows[0]?.wallet_address; // Return address or undefined
    } catch (err) {
        console.error(`DB Error: Error fetching wallet for user ${userId}:`, err);
        return undefined;
    }
}

// Function to record payout tx
async function recordPayout(betId, newStatus, payoutSignature) {
    const query = 'UPDATE bets SET status = $1, payout_tx_signature = $2 WHERE id = $3';
     try {
        const res = await pool.query(query, [newStatus, payoutSignature, betId]);
        console.log(`DB: Recorded payout tx ${payoutSignature} for bet ${betId} with status ${newStatus}. Rows affected: ${res.rowCount}`);
        return { success: res.rowCount > 0 };
    } catch (err) {
        console.error(`DB Error: Error recording payout for bet ${betId}:`, err);
        return { success: false, error: err.message };
    }
}


// --- Payment Monitoring Logic ---
let lastProcessedSignature = { // Store last signature processed for each wallet to avoid re-checking history
    main: null,
    race: null
};
let isMonitorRunning = false; // Prevent overlapping monitor runs

async function monitorPayments() {
    if (isMonitorRunning) {
        // console.log("Monitor already running, skipping cycle.");
        return;
    }
    isMonitorRunning = true;
    // console.log("Running payment monitor cycle..."); // Can be noisy

    try {
        const walletsToMonitor = [
            { address: process.env.MAIN_WALLET_ADDRESS, type: 'main' },
            { address: process.env.RACE_WALLET_ADDRESS, type: 'race' },
        ];

        for (const wallet of walletsToMonitor) {
            const targetAddress = wallet.address;
            const targetPubKey = new PublicKey(targetAddress);
            let signatures;
            let fetchOptions = { limit: 25 }; // Fetch more signatures
            if (lastProcessedSignature[wallet.type]) {
                fetchOptions.until = lastProcessedSignature[wallet.type]; // Fetch only transactions *before* the last processed one
            }

            try {
                 console.log(`Monitor: Fetching signatures for ${wallet.type} wallet (${targetAddress}) ${lastProcessedSignature[wallet.type] ? 'until ' + lastProcessedSignature[wallet.type] : 'limit ' + fetchOptions.limit}...`);
                signatures = await connection.getSignaturesForAddress(targetPubKey, fetchOptions);
                 console.log(`Monitor: Found ${signatures.length} new potential signatures for ${wallet.type} wallet.`);
            } catch (rpcError) {
                 console.error(`Monitor Error: Failed to fetch signatures for ${targetAddress}:`, rpcError.message);
                 continue; // Skip to next wallet if RPC fails
            }


            if (signatures && signatures.length > 0) {
                // Store the newest signature found in this batch to use as 'until' next time
                const newestSignature = signatures[0].signature;

                // Process oldest first to maintain order somewhat
                for (let i = signatures.length - 1; i >= 0; i--) {
                    const sigInfo = signatures[i];
                    const signature = sigInfo.signature;

                    // Fetch transaction details
                    let tx;
                    try {
                        tx = await connection.getParsedTransaction(signature, { maxSupportedTransactionVersion: 0 });
                        if (!tx) {
                            console.warn(`Monitor: Skipping signature ${signature} - failed to fetch transaction details.`);
                            continue;
                        }
                         if (tx.meta?.err) {
                             // console.log(`Monitor: Skipping signature ${signature} - transaction failed.`);
                             continue; // Skip failed transactions
                         }
                    } catch (fetchErr) {
                         console.error(`Monitor Error: Failed to fetch details for tx ${signature}:`, fetchErr.message);
                         continue; // Skip if fetching fails
                    }


                    // Find Memo
                    const memo = findMemoInTx(tx);
                    if (!memo) {
                        // console.log(`Monitor: Skipping signature ${signature} - no memo found.`);
                        continue; // No memo, not a bet payment we can track this way
                    }
                    console.log(`Monitor: Found memo "${memo}" in transaction ${signature}`);

                    // Check if memo corresponds to a pending bet
                    const bet = await findBetByMemo(memo);
                    if (!bet) {
                         console.log(`Monitor: No pending bet found for memo "${memo}".`);
                        continue; // Memo doesn't match an active bet we're waiting for
                    }

                    // Check if game type matches wallet
                    const expectedWallet = bet.game_type === 'race' ? process.env.RACE_WALLET_ADDRESS : process.env.MAIN_WALLET_ADDRESS;
                    if (targetAddress !== expectedWallet) {
                        console.warn(`Monitor: Memo ${memo} found in wrong wallet type (${wallet.type}). Expected wallet ${expectedWallet}. Skipping.`);
                        continue;
                    }

                    // Check amount (ensure transfer to OUR wallet matches expected)
                    let transferAmount = 0;
                    let payerAddress = null;
                    if (tx.meta && tx.transaction?.message?.instructions) {
                         // Find the transfer instruction to our wallet address
                         for (const inst of tx.transaction.message.instructions) {
                             // Check for SystemProgram.transfer
                             const SYSTEM_PROGRAM_ID = SystemProgram.programId.toBase58();
                              let programId = '';
                              if (inst.programIdIndex !== undefined && tx.transaction.message.accountKeys) {
                                   const keyInfo = tx.transaction.message.accountKeys[inst.programIdIndex];
                                   programId = keyInfo.pubkey ? keyInfo.pubkey.toBase58() : keyInfo.toString();
                              }

                             if (programId === SYSTEM_PROGRAM_ID && inst.parsed?.type === 'transfer') {
                                  const transferInfo = inst.parsed.info;
                                  if (transferInfo.destination === targetAddress) {
                                       transferAmount = parseInt(transferInfo.lamports, 10);
                                       payerAddress = transferInfo.source; // Get payer from instruction
                                       console.log(`Monitor: Found transfer of ${transferAmount} lamports from ${payerAddress} to ${targetAddress} in tx ${signature}`);
                                       break; // Found the relevant transfer
                                  }
                             }
                         }
                    }

                    // Use a tolerance for amount check (e.g., +/- 1% or small lamport diff)
                    const lowerBound = bet.expected_lamports * 0.99;
                    const upperBound = bet.expected_lamports * 1.01;
                    if (transferAmount < lowerBound || transferAmount > upperBound) {
                        console.warn(`Monitor: Amount mismatch for memo ${memo}. Expected ~${bet.expected_lamports}, Got ${transferAmount}.`);
                        // Update DB status to reflect mismatch?
                        await updateBetStatus(bet.id, 'error_payment_mismatch');
                        continue;
                    }

                    // Verify timestamp (optional, but good sanity check)
                    const txTime = tx.blockTime ? new Date(tx.blockTime * 1000) : new Date();
                    if (txTime > new Date(bet.expires_at)) {
                        console.warn(`Monitor: Payment for memo ${memo} arrived after expiry.`);
                        await updateBetStatus(bet.id, 'error_payment_expired');
                        continue;
                    }

                    // --- Payment Verified ---
                    console.log(`✅ Monitor: Payment VERIFIED for bet ID ${bet.id} (memo: ${memo}, tx: ${signature})`);

                    // Try to mark bet as paid, checking if TX was already used
                    const markResult = await markBetPaid(bet.id, signature);
                    if (!markResult.success) {
                        console.warn(`Monitor: Failed to mark bet ${bet.id} as paid (race condition or duplicate tx processing?). Error: ${markResult.error}`);
                        continue; // Skip if we couldn't mark it (e.g., tx already used)
                    }

                    // Link wallet address if we found one
                    if (payerAddress) {
                        await linkUserWallet(bet.user_id, payerAddress);
                    } else {
                         // Try getting payer using the older heuristic if needed
                         const payerPubKey = getPayerFromTransaction(tx);
                         if (payerPubKey) {
                             await linkUserWallet(bet.user_id, payerPubKey.toBase58());
                         } else {
                             console.warn(`Monitor: Could not determine payer for bet ${bet.id} to link wallet.`);
                         }
                    }

                    // Trigger game processing (don't await, let it run in background)
                    processPaidBet(bet).catch(e => {
                         console.error(`Error during background game processing for bet ${bet.id}:`, e);
                         updateBetStatus(bet.id, 'error_processing'); // Mark as error if processing fails
                    });

                } // End loop through signatures

                // Update last processed signature only if we successfully processed this batch
                lastProcessedSignature[wallet.type] = newestSignature;

            } // End if signatures found
        } // End loop through wallets

    } catch (error) {
        console.error("❌ Monitor Error: Unexpected error in monitor cycle:", error);
    } finally {
        isMonitorRunning = false; // Allow next run
    }
}

// Run monitor periodically
const monitorIntervalSeconds = 30; // Check every 30 seconds (adjust as needed based on RPC limits/latency)
console.log(`ℹ️ Starting payment monitor. Interval: ${monitorIntervalSeconds} seconds.`);
const monitorInterval = setInterval(monitorPayments, monitorIntervalSeconds * 1000);


// --- Bot Command Handlers ---

bot.onText(/\/start$/, async (msg) => {
    const chatId = msg.chat.id;
    const imageUrl = 'https://i.ibb.co/9vDo58q/banner.gif';
    try {
        await bot.sendAnimation(chatId, imageUrl, {
            caption: `🎰 Welcome to *Solana Gambles*!\n\nUse /coinflip or /race to start.\nUse /wallet to see linked wallet.`,
            parse_mode: 'Markdown'
        });
    } catch (error) { console.error("Error sending start animation:", error); }
});

// Reset only clears memory cooldowns now, doesn't affect DB state
bot.onText(/\/reset$/, async (msg) => {
    const chatId = msg.chat.id;
    if (msg.chat.type === 'group' || msg.chat.type === 'supergroup') {
        await bot.sendMessage(chatId, `⚠️ The /reset command is disabled in group chats.`);
    } else {
        delete confirmCooldown[chatId]; // Clear cooldown for this user
        await bot.sendMessage(chatId, `🔄 Cooldowns reset.`);
    }
});

bot.onText(/\/coinflip$/, (msg) => {
    bot.sendMessage(
        msg.chat.id,
        `🪙 Coinflip! Choose amount and side:\n\n`/bet amount heads`\n`/bet amount tails`\n\nMin: ${MIN_BET} SOL | Max: ${MAX_BET} SOL`,
        { parse_mode: 'Markdown' }
    );
});

bot.onText(/\/wallet$/, async (msg) => {
    const userId = msg.from.id;
    const walletAddress = await getLinkedWallet(userId); // Fetch from DB
    if (walletAddress) {
        await bot.sendMessage(msg.chat.id, `💰 Your linked wallet address is:\n\`${walletAddress}\``, { parse_mode: 'Markdown' });
    } else {
        await bot.sendMessage(msg.chat.id, `⚠️ No wallet linked yet. Place a bet, pay with the memo, and your wallet will be linked automatically.`);
    }
});

bot.onText(/\/refresh$/, async (msg) => {
     const chatId = msg.chat.id;
     const imageUrl = 'https://i.ibb.co/9vDo58q/banner.gif';
     try { await bot.sendAnimation(chatId, imageUrl, { caption: `🎰 Solana Gambles\n\n/coinflip | /race | /wallet`, parse_mode: 'Markdown'}); }
     catch (error) { console.error("Error sending refresh animation:", error); }
});

// --- /bet command (Coinflip) ---
bot.onText(/\/bet (\d+\.?\d*) (heads|tails)/i, async (msg, match) => {
    const userId = msg.from.id;
    const chatId = msg.chat.id;
    const betAmount = parseFloat(match[1]);
    const userChoice = match[2].toLowerCase();

    if (isNaN(betAmount) || betAmount < MIN_BET || betAmount > MAX_BET) {
        return bot.sendMessage(chatId, `⚠️ Bet must be between ${MIN_BET} - ${MAX_BET} SOL`);
    }

    // Check cooldown (optional)
    if (confirmCooldown[userId] && (Date.now() - confirmCooldown[userId]) < cooldownInterval) {
        return bot.sendMessage(chatId, `⚠️ Please wait a few seconds before placing another bet.`);
    }
    confirmCooldown[userId] = Date.now(); // Set cooldown

    // Generate Unique Memo ID
    const memoId = generateMemoId('CF'); // Coinflip prefix

    // Calculate Lamports & Expiry
    const expectedLamports = Math.round(betAmount * LAMPORTS_PER_SOL);
    const expiresAt = new Date(Date.now() + PAYMENT_EXPIRY_MINUTES * 60 * 1000);

    // Save Bet to Database
    const betDetails = { choice: userChoice };
    const saveResult = await savePendingBet(userId, chatId, 'coinflip', betDetails, expectedLamports, memoId, expiresAt);

    if (!saveResult.success) {
        console.error(`Failed to save bet to DB for user ${userId}:`, saveResult.error);
        return bot.sendMessage(chatId, `⚠️ Error registering bet. ${saveResult.error === 'Memo ID collision occurred.' ? 'Please try again in a moment.' : 'Please try again.'}`);
    }

    // Instruct User
    await bot.sendMessage(chatId,
        `✅ Coinflip Bet registered!\n\n`+
        `💸 Send *exactly ${betAmount.toFixed(6)} SOL* to:\n` +
        `\`${process.env.MAIN_WALLET_ADDRESS}\`\n\n` + // Use env var
        `*IMPORTANT:* Include this Memo ID in your transaction:\n` +
        `\`${memoId}\`\n\n` +
        `Payment must be received within ${PAYMENT_EXPIRY_MINUTES} minutes.\nVerification is automatic.`,
        { parse_mode: 'Markdown' }
    );
});

// --- /race command ---
bot.onText(/\/race$/, async (msg) => {
    const chatId = msg.chat.id;
    const horses = [ /* ... horse data ... */
        { name: 'Yellow', emoji: '🟡', odds: 1.1, winProbability: 0.25 }, { name: 'Orange', emoji: '🟠', odds: 2.0, winProbability: 0.20 }, { name: 'Blue', emoji: '🔵', odds: 3.0, winProbability: 0.15 }, { name: 'Cyan', emoji: '🔷', odds: 4.0, winProbability: 0.12 }, { name: 'White', emoji: '⚪', odds: 5.0, winProbability: 0.09 }, { name: 'Red', emoji: '🔴', odds: 6.0, winProbability: 0.07 }, { name: 'Black', emoji: '⚫', odds: 7.0, winProbability: 0.05 }, { name: 'Pink', emoji: '🌸', odds: 8.0, winProbability: 0.03 }, { name: 'Purple', emoji: '🟣', odds: 9.0, winProbability: 0.02 }, { name: 'Green', emoji: '🟢', odds: 10.0, winProbability: 0.01 }, { name: 'Silver', emoji: '💎', odds: 15.0, winProbability: 0.01 },
    ];
    let raceMessage = `🏇 Race! Place your bets!\n\n`;
    horses.forEach(horse => {
        raceMessage += `${horse.emoji} *${horse.name}* (Odds: ${horse.odds.toFixed(1)}x)\n`;
    });
    raceMessage += `\nUse:\n\`/betrace [amount] [horse_name]\`\n` +
        `Min: ${RACE_MIN_BET} SOL | Max: ${RACE_MAX_BET} SOL`;
    await bot.sendMessage(chatId, raceMessage, { parse_mode: 'Markdown' });
});

// --- /betrace command ---
bot.onText(/\/betrace (\d+\.?\d*) (\w+)/i, async (msg, match) => {
    const chatId = msg.chat.id;
    const userId = msg.from.id;
    const betAmount = parseFloat(match[1]);
    const chosenHorseName = match[2];

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

     // Check cooldown
    if (confirmCooldown[userId] && (Date.now() - confirmCooldown[userId]) < cooldownInterval) {
        return bot.sendMessage(chatId, `⚠️ Please wait a few seconds before placing another bet.`);
    }
    confirmCooldown[userId] = Date.now();

    // Generate Unique Memo ID
    const memoId = generateMemoId('RA'); // Race prefix

    // Calculate Lamports & Expiry
    const expectedLamports = Math.round(betAmount * LAMPORTS_PER_SOL);
    const expiresAt = new Date(Date.now() + PAYMENT_EXPIRY_MINUTES * 60 * 1000);

    // Save Bet to Database
    const betDetails = { horse: horse.name, odds: horse.odds }; // Store chosen horse and its odds
    const saveResult = await savePendingBet(userId, chatId, 'race', betDetails, expectedLamports, memoId, expiresAt);

    if (!saveResult.success) {
        console.error(`Failed to save race bet to DB for user ${userId}:`, saveResult.error);
        return bot.sendMessage(chatId, `⚠️ Error registering bet. ${saveResult.error === 'Memo ID collision occurred.' ? 'Please try again in a moment.' : 'Please try again.'}`);
    }

    // Instruct User
    await bot.sendMessage(chatId,
        `✅ Race bet registered on ${horse.emoji} *${horse.name}*!\n\n` +
        `💸 Send *exactly ${betAmount.toFixed(6)} SOL* to:\n` +
        `\`${process.env.RACE_WALLET_ADDRESS}\`\n\n` + // Use env var for race wallet
        `*IMPORTANT:* Include this Memo ID in your transaction:\n` +
        `\`${memoId}\`\n\n` +
        `Payment must be received within ${PAYMENT_EXPIRY_MINUTES} minutes.\nVerification is automatic.`,
        { parse_mode: 'Markdown' }
    );
});

// --- Disabled /confirm commands ---
bot.onText(/^\/confirm$/, async (msg) => {
    await bot.sendMessage(msg.chat.id, `⚠️ The /confirm command is no longer needed. Payment verification is automatic.`);
});
bot.onText(/^\/confirmrace$/, async (msg) => {
     await bot.sendMessage(msg.chat.id, `⚠️ The /confirmrace command is no longer needed. Payment verification is automatic.`);
});


// --- Game Processing Logic ---

async function processPaidBet(bet) {
    // This function is called by monitorPayments when a payment is verified
    console.log(`Processing paid bet ID: ${bet.id}, Type: ${bet.game_type}`);

    // Update status to prevent reprocessing
    await updateBetStatus(bet.id, 'processing_game');

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
    const { id: betId, user_id, chat_id, bet_details, expected_lamports } = bet;
    const amount = expected_lamports / LAMPORTS_PER_SOL;
    const choice = bet_details.choice;

    // Coinflip Logic
    // Simple 50/50 for now (can add house edge later if needed)
    const result = Math.random() < 0.5 ? 'heads' : 'tails';
    const win = (result === choice);
    const payoutLamports = win ? expected_lamports * 2 : 0; // Payout 2x

    // Get display name (optional)
    let displayName = `User ${user_id}`;
    try { const chatMember = await bot.getChatMember(chat_id, user_id); displayName = chatMember.user.username ? `@${chatMember.user.username}` : chatMember.user.first_name; }
    catch (e) { console.warn("Couldn't get username for payout message", e.message); }

    if (win) {
        console.log(`Bet ${betId}: User ${user_id} WON Coinflip! Payout: ${payoutLamports / LAMPORTS_PER_SOL} SOL`);
        const payerPrivateKey = process.env.BOT_PRIVATE_KEY;

        // Get winner's wallet address from linked wallets DB
        const winnerAddress = await getLinkedWallet(user_id);
        if (!winnerAddress) {
             console.error(`Bet ${betId}: Cannot find winner address for payout! User ${user_id} has no linked wallet.`);
              await bot.sendMessage(chat_id, `🎉 Congratulations, ${displayName}! You won!\nResult: ${result}\n⚠️ Payout failed: Could not determine your linked wallet address. Please ensure you've completed a bet before.`);
              await updateBetStatus(betId, 'error_payout_no_wallet');
              return;
        }

         try {
             const winnerPublicKey = new PublicKey(winnerAddress);
             await bot.sendMessage(chat_id, `🎉 Congratulations, ${displayName}! You won ${(payoutLamports / LAMPORTS_PER_SOL).toFixed(6)} SOL!\nResult: *${result}*\n\n💸 Sending payout...`, { parse_mode: 'Markdown'});
             // Use Lamports for sending
             const sendResult = await sendSol(connection, payerPrivateKey, winnerPublicKey, payoutLamports);

             if (sendResult.success) {
                 await bot.sendMessage(chat_id, `💰 Payout successful! TX: \`${sendResult.signature}\``, { parse_mode: 'Markdown'});
                 await recordPayout(betId, 'completed_win_paid', sendResult.signature);
             } else {
                 await bot.sendMessage(chat_id, `⚠️ Payout failed: ${sendResult.error}. Please contact support.`);
                 await updateBetStatus(betId, 'completed_win_payout_failed'); // Specific status for failed payout
             }
         } catch(e) {
             console.error(`Bet ${betId}: Error during payout process:`, e);
             await bot.sendMessage(chat_id, `⚠️ Payout failed due to an internal error. Please contact support.`);
             await updateBetStatus(betId, 'error_payout_exception');
         }

    } else {
        console.log(`Bet ${betId}: User ${user_id} LOST Coinflip.`);
        await bot.sendMessage(chat_id, `❌ *YOU LOSE!*\n\nYou guessed *${choice}*, but the coin landed *${result}*. Better luck next time!`, { parse_mode: "Markdown" });
        await updateBetStatus(betId, 'completed_loss');
    }
}


async function handleRaceGame(bet) {
    console.log(`Handling race game for bet ${bet.id}`);
    const { id: betId, user_id, chat_id, bet_details, expected_lamports } = bet;
    const amount = expected_lamports / LAMPORTS_PER_SOL;
    const horseName = bet_details.horse;
    const odds = bet_details.odds;

    // Race Logic
     const horses = [ /* ... horse data ... */
        { name: 'Yellow', emoji: '🟡', odds: 1.1, winProbability: 0.25 }, { name: 'Orange', emoji: '🟠', odds: 2.0, winProbability: 0.20 }, { name: 'Blue', emoji: '🔵', odds: 3.0, winProbability: 0.15 }, { name: 'Cyan', emoji: '🔷', odds: 4.0, winProbability: 0.12 }, { name: 'White', emoji: '⚪', odds: 5.0, winProbability: 0.09 }, { name: 'Red', emoji: '🔴', odds: 6.0, winProbability: 0.07 }, { name: 'Black', emoji: '⚫', odds: 7.0, winProbability: 0.05 }, { name: 'Pink', emoji: '🌸', odds: 8.0, winProbability: 0.03 }, { name: 'Purple', emoji: '🟣', odds: 9.0, winProbability: 0.02 }, { name: 'Green', emoji: '🟢', odds: 10.0, winProbability: 0.01 }, { name: 'Silver', emoji: '💎', odds: 15.0, winProbability: 0.01 },
    ];
    let winningHorse;
    const randomNumber = Math.random();
    let cumulativeProbability = 0;
    for (const contender of horses) {
        cumulativeProbability += contender.winProbability;
        if (randomNumber <= cumulativeProbability) { winningHorse = contender; break; }
    }
    if (!winningHorse) winningHorse = horses[horses.length - 1]; // Fallback

    // Send race commentary...
    await bot.sendMessage(chatId, `🏇 Race for Bet ID ${betId} is starting! You picked *${horseName}*!`, {parse_mode: 'Markdown'});
    await new Promise(resolve => setTimeout(resolve, 2000)); // Pause for effect
    await bot.sendMessage(chatId, `...and they're off!`);
    await new Promise(resolve => setTimeout(resolve, 3000));
    await bot.sendMessage(chatId, `🏁 **The winner is... ${winningHorse.emoji} *${winningHorse.name}*!** 🏁`, { parse_mode: 'Markdown' });

    // Payout or Lose Message
    const win = (horseName.toLowerCase() === winningHorse.name.toLowerCase()); // Case-insensitive compare
    const payoutLamports = win ? Math.round(expected_lamports * odds) : 0; // Payout based on odds, ensure integer

    let displayName = `User ${user_id}`;
     try { const chatMember = await bot.getChatMember(chat_id, user_id); displayName = chatMember.user.username ? `@${chatMember.user.username}` : chatMember.user.first_name; }
     catch (e) { console.warn("Couldn't get username for payout message", e.message); }

    if (win) {
        console.log(`Bet ${betId}: User ${user_id} WON Race! Payout: ${payoutLamports / LAMPORTS_PER_SOL} SOL`);
        const payerPrivateKey = process.env.RACE_BOT_PRIVATE_KEY; // Use RACE key

        const winnerAddress = await getLinkedWallet(user_id); // Get from DB
        if (!winnerAddress) {
             console.error(`Bet ${betId}: Cannot find winner address for race payout!`);
              await bot.sendMessage(chat_id, `🎉 Congratulations, ${displayName}! Your horse *${horseName}* won!\nPayout: ${(payoutLamports / LAMPORTS_PER_SOL).toFixed(6)} SOL\n⚠️ Payout failed: Could not determine your linked wallet address.`);
              await updateBetStatus(betId, 'error_payout_no_wallet');
              return;
        }

         try {
             const winnerPublicKey = new PublicKey(winnerAddress);
             await bot.sendMessage(chat_id, `🎉 Congratulations, ${displayName}! Your horse *${horseName}* won!\nPayout: ${(payoutLamports / LAMPORTS_PER_SOL).toFixed(6)} SOL\n\n💸 Sending payout...`, {parse_mode: 'Markdown'});
             const sendResult = await sendSol(connection, payerPrivateKey, winnerPublicKey, payoutLamports); // Send lamports

             if (sendResult.success) {
                 await bot.sendMessage(chat_id, `💰 Race payout successful! TX: \`${sendResult.signature}\``, { parse_mode: 'Markdown'});
                  await recordPayout(betId, 'completed_win_paid', sendResult.signature);
             } else {
                 await bot.sendMessage(chat_id, `⚠️ Race payout failed: ${sendResult.error}. Please contact support.`);
                 await updateBetStatus(betId, 'completed_win_payout_failed');
             }
         } catch(e) {
             console.error(`Bet ${betId}: Error during race payout process:`, e);
             await bot.sendMessage(chat_id, `⚠️ Race payout failed due to an internal error. Please contact support.`);
             await updateBetStatus(betId, 'error_payout_exception');
         }
    } else {
        console.log(`Bet ${betId}: User ${user_id} LOST Race.`);
        await bot.sendMessage(chat_id, `❌ *Your horse lost!*\n\nYou picked *${horseName}*, but *${winningHorse.name}* won the race. Better luck next time!`, { parse_mode: "Markdown" });
        await updateBetStatus(betId, 'completed_loss');
    }
}

// --- Main Server Start Function ---
async function startServer() {
    try {
        // Ensure DB is ready first
        await initializeDatabase();

        // Then start listening for HTTP requests
        const PORT = process.env.PORT || 3000; // Use 3000 based on last successful deploy
        app.listen(PORT, () => {
            console.log(`✅ Healthcheck server listening on port ${PORT}`);
            console.log("Application fully started. Waiting for signals / payments...");
            console.log("--- BOT IS NOT POLLING ---");
            console.log("Implement Webhooks or manually send updates for interaction.");
        });

    } catch (error) {
        console.error("💥 Failed to start server:", error);
        process.exit(1); // Exit if essential startup fails
    }
}

// --- Start the server ---
startServer();

// --- Graceful shutdown ---
process.on('SIGINT', () => {
  console.log("Received SIGINT. Shutting down bot...");
  clearInterval(monitorInterval); // Stop monitor
  pool.end(() => { // Close DB pool
      console.log('Database pool closed.');
      process.exit(0);
  });
});
process.on('SIGTERM', () => {
  console.log("Received SIGTERM. Shutting down bot...");
   clearInterval(monitorInterval); // Stop monitor
   pool.end(() => { // Close DB pool
      console.log('Database pool closed.');
      process.exit(0);
   });
});

// --- Error Handlers ---
process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
});
process.on('uncaughtException', (error) => {
  console.error('Uncaught Exception:', error);
  // Attempt graceful shutdown? Or just exit? Exiting might be safer.
   clearInterval(monitorInterval);
   pool.end(() => { process.exit(1); });
   setTimeout(() => process.exit(1), 2000); // Force exit if pool doesn't close quickly
});
