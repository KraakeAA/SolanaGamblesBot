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
const { randomBytes } = require('crypto'); // For generating unique memos

const app = express();

// --- PostgreSQL Setup ---
console.log("Setting up PostgreSQL Pool...");
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  // Consider adding SSL config if needed for Railway, but test without first
  // ssl: { rejectUnauthorized: false }
});
console.log("PostgreSQL Pool created.");

// --- Database Initialization (Corrected Version) ---
async function initializeDatabase() {
  console.log("Initializing Database...");
  let client;
  try {
    client = await pool.connect();
    console.log("DB client connected.");

    // Step 1: Ensure the 'bets' table exists with base columns
    await client.query(`
      CREATE TABLE IF NOT EXISTS bets (
        id SERIAL PRIMARY KEY,
        user_id TEXT NOT NULL,
        chat_id TEXT NOT NULL,
        game_type TEXT NOT NULL,
        bet_details JSONB,
        expected_lamports BIGINT NOT NULL,
        memo_id TEXT UNIQUE NOT NULL,
        status TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        expires_at TIMESTAMPTZ NOT NULL
        -- Missing columns will be added below if they don't exist
      );
    `);
    console.log("✅ Base 'bets' table structure checked/created.");

    // Step 2: Check for and add the specific columns if they are missing
    const columnsToAdd = [
        { name: 'paid_tx_signature', type: 'TEXT UNIQUE' },
        { name: 'payout_tx_signature', type: 'TEXT UNIQUE' }
    ];

    for (const col of columnsToAdd) {
         // Query to check if the column exists in the 'bets' table
         const checkColumnQuery = `
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'bets' AND column_name = $1;
         `;
         const colExistsResult = await client.query(checkColumnQuery, [col.name]);

         if (colExistsResult.rowCount === 0) {
             // Column is missing, so add it
             const addColumnQuery = `ALTER TABLE public.bets ADD COLUMN ${col.name} ${col.type};`; // Use public schema qualifier just in case
             await client.query(addColumnQuery);
             console.log(`✅ Added missing column '${col.name}' to 'bets' table.`);
         } else {
             // Optional: Log that column already exists if needed for debugging
             // console.log(`ℹ️ Column '${col.name}' already exists in 'bets' table.`);
         }
    }
    console.log("✅ All required columns in 'bets' table verified/added.");

    // Step 3: Ensure the 'wallets' table exists
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
    throw err; // Re-throw error to be caught by startServer
  } finally {
    if (client) {
      client.release();
      console.log("ℹ️ Database client released after initialization check.");
    }
  }
}
// --- End PostgreSQL Setup ---


// --- Bot and Solana Initialization ---
console.log("Initializing Telegram Bot (POLLING ENABLED)...");
// Initialize bot WITH polling
const bot = new TelegramBot(process.env.BOT_TOKEN, { polling: true }); // <<< POLLING IS TRUE HERE
console.log("Telegram Bot initialized with polling.");

console.log("Initializing Solana Connection...");
const connection = new Connection(process.env.RPC_URL, 'confirmed');
console.log("Solana Connection initialized.");
// --- End Initialization ---

// --- Express Healthcheck ---
app.get('/', (req, res) => {
  // console.log("Health check endpoint '/' hit!"); // Optional: reduce log noise
  res.status(200).send('OK');
});

// --- In-Memory State (Minimal) ---
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
        const MEMO_PROGRAM_ID = 'Memo1UhkJRfHyvLMcVuc6beZNRYqUP2VZwW'; // Program ID for SPL Memo
        for (const instruction of tx.transaction.message.instructions) {
            let programId = '';
             // Find the program ID key from the accountKeys array using the index provided in the instruction
             if (instruction.programIdIndex !== undefined && tx.transaction.message.accountKeys) {
                 const keyInfo = tx.transaction.message.accountKeys[instruction.programIdIndex];
                 // Handle different possible structures for account keys
                 if (keyInfo && typeof keyInfo === 'object' && keyInfo.pubkey) {
                     programId = keyInfo.pubkey.toBase58();
                 } else if (keyInfo && typeof keyInfo === 'string') {
                    // Sometimes it might just be the string representation
                    programId = new PublicKey(keyInfo).toBase58();
                 } else {
                     // console.warn("Could not determine program ID from instruction:", instruction);
                 }
             } else if (instruction.programId) { // Handle cases where programId is directly available
                  programId = instruction.programId.toBase58 ? instruction.programId.toBase58() : instruction.programId.toString();
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
        let firstSignerKey;
         // Handle different possible structures for account keys
         if (message.accountKeys[0].pubkey) {
             firstSignerKey = message.accountKeys[0].pubkey;
         } else if (typeof message.accountKeys[0] === 'string') {
             firstSignerKey = new PublicKey(message.accountKeys[0]);
         } else {
             console.warn("Could not derive PublicKey from first signer account key.");
             // Continue to fallback...
         }

         if (firstSignerKey){
             const balanceDiff = (preBalances[0] ?? 0) - (postBalances[0] ?? 0);
             if (balanceDiff > 0) { // Ensure they actually spent SOL (incl. fees)
                  console.log(`getPayerFromTransaction: Identified payer as first signer: ${firstSignerKey.toBase58()}`);
                 return firstSignerKey;
             } else {
                  console.warn(`getPayerFromTransaction: First signer ${firstSignerKey.toBase58()} had non-positive balance change (${balanceDiff}). Might not be payer.`);
             }
         }
    }

    // Fallback logic (less reliable) - find first signer with balance decrease
     for (let i = 0; i < message.accountKeys.length; i++) {
        if (i >= preBalances.length || i >= postBalances.length) continue;
        if (message.accountKeys[i]?.signer) {
            let key;
             if (message.accountKeys[i].pubkey) {
                 key = message.accountKeys[i].pubkey;
             } else if (typeof message.accountKeys[i] === 'string') {
                 key = new PublicKey(message.accountKeys[i]);
             } else {
                 continue; // Skip if key cannot be determined
             }

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
    // Ensure recipientPublicKey is a PublicKey object
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
            // Simple retry delay for specific, retryable errors
            if (error.message.includes('blockhash') || error.message.includes('Blockhash not found') || error.message.includes('timeout')) {
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
   // Finds a bet that is awaiting payment and matches the memo
   const query = `SELECT * FROM bets WHERE memo_id = $1 AND status = 'awaiting_payment'`;
   try {
       const res = await pool.query(query, [memoId]);
       // Do not log here to reduce noise, log in monitor if found
       return res.rows[0];
   } catch (err) {
       console.error(`DB Error: Error finding bet by memo ${memoId}:`, err);
       return undefined;
   }
}

async function findPendingBets() {
    // This function isn't strictly needed by the current monitor logic but could be useful
    const query = `SELECT * FROM bets WHERE status = 'awaiting_payment' AND expires_at > NOW()`;
    try {
        const res = await pool.query(query);
        return res.rows;
    } catch(err) {
        console.error(`DB Error: Error finding pending bets:`, err);
        return [];
    }
}

async function updateBetStatus(betId, newStatus) {
   const query = 'UPDATE bets SET status = $1 WHERE id = $2';
   try {
       const res = await pool.query(query, [newStatus, betId]);
       if (res.rowCount > 0) {
            console.log(`DB: Updated bet ${betId} status to ${newStatus}.`);
       } else {
            console.warn(`DB Warning: Attempted to update status for bet ${betId} to ${newStatus}, but no row was affected (check ID and current status?).`);
       }
       return { success: res.rowCount > 0 };
   } catch (err) {
       console.error(`DB Error: Error updating bet ${betId} status to ${newStatus}:`, err);
       return { success: false, error: err.message };
   }
}

// Function to record the transaction that paid the bet
async function markBetPaid(betId, txSignature) {
    const query = `
        UPDATE bets
        SET status = $1, paid_tx_signature = $2
        WHERE id = $3 AND status = $4`; // Only update if status is awaiting_payment
    try {
        const res = await pool.query(query, ['payment_verified', txSignature, betId, 'awaiting_payment']);
        console.log(`DB: Marked bet ${betId} as paid by tx ${txSignature}. Rows affected: ${res.rowCount}`);
        return { success: res.rowCount > 0 };
    } catch (err) {
        console.error(`DB Error: Error marking bet ${betId} as paid:`, err);
        if (err.code === '23505') { // Catch unique constraint violations generally
            console.warn(`DB Warning: Constraint violation when marking bet ${betId} paid by ${txSignature}. Signature might already exist.`);
            return { success: false, error: 'Transaction signature already used or another constraint failed.' };
        }
        return { success: false, error: err.message };
    }
}

// Function to link wallet or update link time
async function linkUserWallet(userId, walletAddress) {
    try {
        new PublicKey(walletAddress); // Validate address format
    } catch(e) {
        console.error(`DB Error: Invalid wallet address format for user ${userId}: ${walletAddress}`);
        return { success: false, error: 'Invalid wallet address format.' };
    }
    const query = `
        INSERT INTO wallets (user_id, wallet_address, linked_at) VALUES ($1, $2, NOW())
        ON CONFLICT (user_id) DO UPDATE SET wallet_address = EXCLUDED.wallet_address, linked_at = NOW();`;
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
        return res.rows[0]?.wallet_address;
    } catch (err) {
        console.error(`DB Error: Error fetching wallet for user ${userId}:`, err);
        return undefined;
    }
}

// Function to record payout tx
async function recordPayout(betId, newStatus, payoutSignature) {
    const query = `
        UPDATE bets SET status = $1, payout_tx_signature = $2 WHERE id = $3`;
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
let lastProcessedSignatures = {}; // Store last processed signatures IN MEMORY (clears on restart)
let isMonitorRunning = false;

async function monitorPayments() {
    if (isMonitorRunning) return;
    isMonitorRunning = true;
    // console.log("Running payment monitor cycle...");

    try {
        const walletsToMonitor = [
            { address: process.env.MAIN_WALLET_ADDRESS, type: 'main', game: 'coinflip' },
            { address: process.env.RACE_WALLET_ADDRESS, type: 'race', game: 'race' },
        ];

        for (const wallet of walletsToMonitor) {
            const targetAddress = wallet.address;
            const targetPubKey = new PublicKey(targetAddress);
            let signatures = [];
            // Fetch recent transactions
            let fetchOptions = { limit: 25 }; // Fetch a smaller batch more frequently maybe?

            try {
                signatures = await connection.getSignaturesForAddress(targetPubKey, fetchOptions);
            } catch (rpcError) {
                if (rpcError.message.includes('429')) {
                    console.warn(`Monitor Warn: RPC rate limit hit for ${targetAddress}. Skipping cycle.`);
                } else {
                    console.error(`Monitor Error: RPC error fetching signatures for ${targetAddress}:`, rpcError.message);
                }
                continue;
            }

            if (!signatures || signatures.length === 0) continue;

            // Process oldest fetched first towards newest
            for (let i = signatures.length - 1; i >= 0; i--) {
                const signature = signatures[i].signature;

                // Simple in-memory check to avoid reprocessing in *this current run*
                if (lastProcessedSignatures[signature]) continue;

                // Check DB if this TX has already paid a bet
                const checkProcessedQuery = 'SELECT id FROM bets WHERE paid_tx_signature = $1';
                const processedResult = await pool.query(checkProcessedQuery, [signature]);
                if (processedResult.rowCount > 0) {
                    lastProcessedSignatures[signature] = true; // Mark as seen in this run
                    continue;
                }

                let tx;
                try {
                    tx = await connection.getParsedTransaction(signature, { maxSupportedTransactionVersion: 0 });
                    if (!tx || tx.meta?.err) {
                        lastProcessedSignatures[signature] = true; // Mark failed/unparseable tx as seen
                        continue;
                    }
                } catch (fetchErr) {
                    console.error(`Monitor Error: Failed fetch for tx ${signature}:`, fetchErr.message);
                    continue; // Skip this signature on error
                }

                const memo = findMemoInTx(tx);
                if (!memo) {
                    lastProcessedSignatures[signature] = true; // Mark no-memo tx as seen
                    continue;
                }
                // Log only if memo might be relevant based on prefix?
                if (memo.startsWith('CF') || memo.startsWith('RA') || memo.startsWith('BET')) {
                    console.log(`Monitor: Found memo "${memo}" in transaction ${signature}`);
                }

                const bet = await findBetByMemo(memo); // Checks for memo AND status='awaiting_payment'
                if (!bet) {
                    lastProcessedSignatures[signature] = true; // Mark as seen, no matching bet found
                    continue;
                }

                // Check game type vs wallet
                if (bet.game_type !== wallet.game) {
                    console.warn(`Monitor: Memo ${memo} found in ${wallet.type} wallet, bet is ${bet.game_type}. Skipping.`);
                    lastProcessedSignatures[signature] = true; // Mark as seen
                    continue;
                }

                // Check amount
                let transferAmount = 0;
                let payerAddress = null;
                if (tx.meta && tx.transaction?.message?.instructions) {
                     const instructions = (tx.transaction.message.instructions || []).concat(...(tx.meta.innerInstructions || []).map(i => i.instructions));
                     const SYSTEM_PROGRAM_ID = SystemProgram.programId.toBase58();
                     for (const inst of instructions) {
                         let programId = '';
                         if (inst.programIdIndex !== undefined && tx.transaction.message.accountKeys) {
                              const keyInfo = tx.transaction.message.accountKeys[inst.programIdIndex];
                              if(keyInfo?.pubkey) programId = keyInfo.pubkey.toBase58();
                              else if (typeof keyInfo === 'string') programId = new PublicKey(keyInfo).toBase58();
                         } else if (inst.programId) { programId = inst.programId.toBase58 ? inst.programId.toBase58() : inst.programId.toString(); }

                         if (programId === SYSTEM_PROGRAM_ID && inst.parsed?.type === 'transfer') {
                              const transferInfo = inst.parsed.info;
                              if (transferInfo.destination === targetAddress) {
                                   transferAmount += parseInt(transferInfo.lamports || transferInfo.amount || 0, 10); // Sum up transfers to target
                                   if (!payerAddress) payerAddress = transferInfo.source; // Capture first payer
                              }
                         }
                     }
                     if(payerAddress) console.log(`Monitor: Found total transfer of ${transferAmount} lamports to ${targetAddress} in tx ${signature}. Payer: ${payerAddress}`);
                }
                const lamportTolerance = 5000;
                if (Math.abs(transferAmount - bet.expected_lamports) > lamportTolerance) {
                    console.warn(`Monitor: Amount mismatch memo ${memo}. Expected ${bet.expected_lamports}, Got ${transferAmount}.`);
                    await updateBetStatus(bet.id, 'error_payment_mismatch');
                    lastProcessedSignatures[signature] = true; // Mark as seen/processed
                    continue;
                }

                // Timestamp check
                const txTime = tx.blockTime ? new Date(tx.blockTime * 1000) : new Date(0);
                if (txTime > new Date(bet.expires_at)) {
                    console.warn(`Monitor: Payment memo ${memo} (Tx: ${signature}) expired.`);
                    await updateBetStatus(bet.id, 'error_payment_expired');
                    lastProcessedSignatures[signature] = true; // Mark as seen/processed
                    continue;
                }

                // --- Payment Verified ---
                console.log(`✅ Monitor: Payment VERIFIED for bet ID ${bet.id} (memo: ${memo}, tx: ${signature})`);

                const markResult = await markBetPaid(bet.id, signature);
                if (!markResult.success) {
                    console.warn(`Monitor: Failed to mark bet ${bet.id} paid by ${signature}. Error: ${markResult.error}. Already processed?`);
                    lastProcessedSignatures[signature] = true; // Mark as seen/processed
                    continue;
                }

                // Link wallet
                if (payerAddress) { await linkUserWallet(bet.user_id, payerAddress); }
                else { const pKey = getPayerFromTransaction(tx); if (pKey) await linkUserWallet(bet.user_id, pKey.toBase58()); else console.warn(`Monitor: Could not determine payer for bet ${bet.id} to link wallet.`);}

                lastProcessedSignatures[signature] = true; // Mark as processed successfully

                // Trigger game processing (async)
                processPaidBet(bet).catch(e => {
                     console.error(`Error processing bet ${bet.id}:`, e);
                     updateBetStatus(bet.id, 'error_processing_exception');
                });
            } // End signature loop
        } // End wallet loop
    } catch (error) {
        console.error("❌ Monitor Error: Unexpected error in monitor cycle:", error);
    } finally {
        isMonitorRunning = false;
    }
}
const monitorIntervalSeconds = 30;
console.log(`ℹ️ Starting payment monitor. Interval: ${monitorIntervalSeconds} seconds.`);
const monitorInterval = setInterval(monitorPayments, monitorIntervalSeconds * 1000);


// --- Bot Command Handlers ---
// Wrapped in try/catch for basic error handling

bot.onText(/\/start$/, async (msg) => {
    try {
        const chatId = msg.chat.id;
        const imageUrl = 'https://i.ibb.co/9vDo58q/banner.gif';
        await bot.sendAnimation(chatId, imageUrl, { caption: `🎰 Welcome to *Solana Gambles*!\n\nUse /coinflip or /race to start.\nUse /wallet to see linked wallet.`, parse_mode: 'Markdown' });
    } catch (error) { console.error("Error in /start handler:", error); }
});

bot.onText(/\/reset$/, async (msg) => {
    try {
        const chatId = msg.chat.id;
        if (msg.chat.type === 'group' || msg.chat.type === 'supergroup') {
            await bot.sendMessage(chatId, `⚠️ The /reset command is disabled in group chats.`);
        } else {
            delete confirmCooldown[msg.from.id];
            await bot.sendMessage(chatId, `🔄 Cooldowns reset.`);
        }
    } catch (error) { console.error("Error in /reset handler:", error); }
});

bot.onText(/\/coinflip$/, async (msg) => {
    try {
        const helpMessage = `🪙 Coinflip! Choose amount and side:\n\n` + `\`/bet amount heads\`\n` + `\`/bet amount tails\`\n\n` + `Min: ${MIN_BET} SOL | Max: ${MAX_BET} SOL`;
        await bot.sendMessage( msg.chat.id, helpMessage, { parse_mode: 'Markdown' } );
    } catch (error) { console.error("Error in /coinflip handler:", error); }
});

bot.onText(/\/wallet$/, async (msg) => {
    try {
        const userId = msg.from.id;
        const walletAddress = await getLinkedWallet(userId);
        if (walletAddress) {
            await bot.sendMessage(msg.chat.id, `💰 Your linked wallet address is:\n\`${walletAddress}\``, { parse_mode: 'Markdown' });
        } else {
            await bot.sendMessage(msg.chat.id, `⚠️ No wallet linked yet. Place a bet, pay with the memo, and your wallet will be linked automatically.`);
        }
    } catch (error) { console.error("Error in /wallet handler:", error); }
});

bot.onText(/\/refresh$/, async (msg) => {
     try {
         const chatId = msg.chat.id;
         const imageUrl = 'https://i.ibb.co/9vDo58q/banner.gif';
         await bot.sendAnimation(chatId, imageUrl, { caption: `🎰 Solana Gambles\n\n/coinflip | /race | /wallet`, parse_mode: 'Markdown'});
     } catch (error) { console.error("Error sending refresh animation:", error); }
});

// /bet command (Coinflip)
bot.onText(/\/bet (\d+\.?\d*) (heads|tails)/i, async (msg, match) => {
    const userId = msg.from.id; // Define userId here
    const chatId = msg.chat.id; // Define chatId here
    try {
        const betAmount = parseFloat(match[1]);
        const userChoice = match[2].toLowerCase();

        if (isNaN(betAmount) || betAmount < MIN_BET || betAmount > MAX_BET) {
            return bot.sendMessage(chatId, `⚠️ Bet must be between ${MIN_BET} - ${MAX_BET} SOL`);
        }
        if (confirmCooldown[userId] && (Date.now() - confirmCooldown[userId]) < cooldownInterval) {
            return bot.sendMessage(chatId, `⚠️ Please wait a few seconds before placing another bet.`);
        }
        confirmCooldown[userId] = Date.now();

        const memoId = generateMemoId('CF');
        const expectedLamports = Math.round(betAmount * LAMPORTS_PER_SOL);
        const expiresAt = new Date(Date.now() + PAYMENT_EXPIRY_MINUTES * 60 * 1000);
        const betDetails = { choice: userChoice };

        const saveResult = await savePendingBet(userId, chatId, 'coinflip', betDetails, expectedLamports, memoId, expiresAt);
        if (!saveResult.success) { throw new Error(saveResult.error); } // Throw error to be caught below

        await bot.sendMessage(chatId,
            `✅ Coinflip Bet registered!\n\n`+
            `💸 Send *exactly ${betAmount.toFixed(6)} SOL* to:\n` +
            `\`${process.env.MAIN_WALLET_ADDRESS}\`\n\n` +
            `*IMPORTANT:* Include this Memo ID in your transaction:\n` +
            `\`${memoId}\`\n\n` +
            `Payment must be received within ${PAYMENT_EXPIRY_MINUTES} minutes.\nVerification is automatic.`,
            { parse_mode: 'Markdown' }
        );
    } catch (error) {
        console.error(`Error in /bet handler for user ${userId}:`, error); // Use userId here
        await bot.sendMessage(chatId, `⚠️ An error occurred registering your bet. ${error.message === 'Memo ID collision occurred.' ? 'Please try again in a moment.' : 'Please try again later.'}`);
    }
});

// /race command
bot.onText(/\/race$/, async (msg) => {
    try {
        const chatId = msg.chat.id;
        const horses = [ { name: 'Yellow', emoji: '🟡', odds: 1.1, winProbability: 0.25 }, { name: 'Orange', emoji: '🟠', odds: 2.0, winProbability: 0.20 }, { name: 'Blue', emoji: '🔵', odds: 3.0, winProbability: 0.15 }, { name: 'Cyan', emoji: '🔷', odds: 4.0, winProbability: 0.12 }, { name: 'White', emoji: '⚪', odds: 5.0, winProbability: 0.09 }, { name: 'Red', emoji: '🔴', odds: 6.0, winProbability: 0.07 }, { name: 'Black', emoji: '⚫', odds: 7.0, winProbability: 0.05 }, { name: 'Pink', emoji: '🌸', odds: 8.0, winProbability: 0.03 }, { name: 'Purple', emoji: '🟣', odds: 9.0, winProbability: 0.02 }, { name: 'Green', emoji: '🟢', odds: 10.0, winProbability: 0.01 }, { name: 'Silver', emoji: '💎', odds: 15.0, winProbability: 0.01 }, ];
        let raceMessage = `🏇 Race! Place your bets!\n\n`;
        horses.forEach(horse => { raceMessage += `${horse.emoji} *${horse.name}* (Odds: ${horse.odds.toFixed(1)}x)\n`; });
        raceMessage += `\nUse:\n\`/betrace [amount] [horse_name]\`\n` + `Min: ${RACE_MIN_BET} SOL | Max: ${RACE_MAX_BET} SOL`;
        await bot.sendMessage(chatId, raceMessage, { parse_mode: 'Markdown' });
    } catch(error) { console.error("Error in /race handler:", error); }
});

// /betrace command
bot.onText(/\/betrace (\d+\.?\d*) (\w+)/i, async (msg, match) => {
     const userId = msg.from.id; // Define userId here
     const chatId = msg.chat.id; // Define chatId here
     try {
        const betAmount = parseFloat(match[1]);
        const chosenHorseName = match[2];
        const horses = [ { name: 'Yellow', emoji: '🟡', odds: 1.1, winProbability: 0.25 }, { name: 'Orange', emoji: '🟠', odds: 2.0, winProbability: 0.20 }, { name: 'Blue', emoji: '🔵', odds: 3.0, winProbability: 0.15 }, { name: 'Cyan', emoji: '🔷', odds: 4.0, winProbability: 0.12 }, { name: 'White', emoji: '⚪', odds: 5.0, winProbability: 0.09 }, { name: 'Red', emoji: '🔴', odds: 6.0, winProbability: 0.07 }, { name: 'Black', emoji: '⚫', odds: 7.0, winProbability: 0.05 }, { name: 'Pink', emoji: '🌸', odds: 8.0, winProbability: 0.03 }, { name: 'Purple', emoji: '🟣', odds: 9.0, winProbability: 0.02 }, { name: 'Green', emoji: '🟢', odds: 10.0, winProbability: 0.01 }, { name: 'Silver', emoji: '💎', odds: 15.0, winProbability: 0.01 }, ];
        const horse = horses.find(h => h.name.toLowerCase() === chosenHorseName.toLowerCase());

        if (!horse) { return bot.sendMessage(chatId, `⚠️ Invalid horse name. Options:\n` + horses.map(h => `${h.emoji} ${h.name}`).join('\n')); }
        if (isNaN(betAmount) || betAmount < RACE_MIN_BET || betAmount > RACE_MAX_BET) { return bot.sendMessage(chatId, `⚠️ Bet must be between ${RACE_MIN_BET} - ${RACE_MAX_BET} SOL`); }
        if (confirmCooldown[userId] && (Date.now() - confirmCooldown[userId]) < cooldownInterval) { return bot.sendMessage(chatId, `⚠️ Please wait a few seconds before placing another bet.`); }
        confirmCooldown[userId] = Date.now();

        const memoId = generateMemoId('RA');
        const expectedLamports = Math.round(betAmount * LAMPORTS_PER_SOL);
        const expiresAt = new Date(Date.now() + PAYMENT_EXPIRY_MINUTES * 60 * 1000);
        const betDetails = { horse: horse.name, odds: horse.odds };

        const saveResult = await savePendingBet(userId, chatId, 'race', betDetails, expectedLamports, memoId, expiresAt);
        if (!saveResult.success) { throw new Error(saveResult.error); }

        await bot.sendMessage(chatId,
            `✅ Race bet registered on ${horse.emoji} *${horse.name}*!\n\n` +
            `💸 Send *exactly ${betAmount.toFixed(6)} SOL* to:\n` +
            `\`${process.env.RACE_WALLET_ADDRESS}\`\n\n` +
            `*IMPORTANT:* Include this Memo ID in your transaction:\n` +
            `\`${memoId}\`\n\n` +
            `Payment must be received within ${PAYMENT_EXPIRY_MINUTES} minutes.\nVerification is automatic.`,
            { parse_mode: 'Markdown' }
        );
    } catch (error) {
        console.error(`Error in /betrace handler for user ${userId}:`, error); // Use userId here
        await bot.sendMessage(chatId, `⚠️ An error occurred registering your bet. ${error.message === 'Memo ID collision occurred.' ? 'Please try again in a moment.' : 'Please try again later.'}`);
    }
});

// Disabled /confirm commands
bot.onText(/^\/confirm$/, async (msg) => { await bot.sendMessage(msg.chat.id, `⚠️ The /confirm command is no longer needed.`); });
bot.onText(/^\/confirmrace$/, async (msg) => { await bot.sendMessage(msg.chat.id, `⚠️ The /confirmrace command is no longer needed.`); });


// --- Game Processing Logic ---
// These functions handle the actual game mechanics after payment verification

async function processPaidBet(bet) {
    console.log(`Processing paid bet ID: ${bet.id}, Type: ${bet.game_type}`);
    // Ensure status is correct before processing
    if (bet.status !== 'payment_verified') {
        console.warn(`Attempted to process bet ${bet.id} but status was ${bet.status}. Aborting.`);
        return;
    }
    const updateResult = await updateBetStatus(bet.id, 'processing_game');
    if (!updateResult.success) {
        console.error(`Failed to update status to processing_game for bet ${bet.id}. Aborting processing.`);
        return; // Don't proceed if we couldn't lock the status
    }

    try {
        if (bet.game_type === 'coinflip') {
            await handleCoinflipGame(bet);
        } else if (bet.game_type === 'race') {
            await handleRaceGame(bet);
        } else {
            console.error(`Unknown game type for bet ${bet.id}: ${bet.game_type}`);
            await updateBetStatus(bet.id, 'error_unknown_game');
        }
    } catch (gameError) {
         console.error(`Error during game logic execution for bet ${bet.id}:`, gameError);
         // Try to mark the bet as having an error during processing
         await updateBetStatus(bet.id, 'error_processing_exception');
    }
}

async function handleCoinflipGame(bet) {
    console.log(`Handling coinflip game for bet ${bet.id}`);
    const { id: betId, user_id, chat_id, bet_details, expected_lamports } = bet;
    const choice = bet_details.choice;

    // Coinflip Logic (Simple 50/50) - Add house edge back if desired
    // const amount = expected_lamports / LAMPORTS_PER_SOL;
    // const houseEdge = getHouseEdge(amount); // Define getHouseEdge function if using
    const result = Math.random() < 0.5 ? 'heads' : 'tails';
    const win = (result === choice);
    const payoutLamports = win ? expected_lamports * 2 : 0; // 2x payout

    let displayName = `User ${user_id}`;
    try { const chatMember = await bot.getChatMember(chat_id, user_id); displayName = chatMember.user.username ? `@${chatMember.user.username}` : chatMember.user.first_name; }
    catch (e) { console.warn(`Couldn't get username for payout message (Bet ${betId}):`, e.message); }

    if (win) {
        console.log(`Bet ${betId}: User ${user_id} WON Coinflip! Payout: ${payoutLamports / LAMPORTS_PER_SOL} SOL`);
        const payerPrivateKey = process.env.BOT_PRIVATE_KEY;
        const winnerAddress = await getLinkedWallet(user_id);

        if (!winnerAddress) {
             console.error(`Bet ${betId}: Cannot find winner address for payout!`);
              await bot.sendMessage(chat_id, `🎉 Congratulations, ${displayName}! You won!\nResult: *${result}*\n⚠️ Payout failed: Could not determine your linked wallet address.`, { parse_mode: 'Markdown'});
              await updateBetStatus(betId, 'error_payout_no_wallet');
              return;
        }

         try {
             const winnerPublicKey = new PublicKey(winnerAddress);
             await bot.sendMessage(chat_id, `🎉 Congratulations, ${displayName}! You won ${(payoutLamports / LAMPORTS_PER_SOL).toFixed(6)} SOL!\nResult: *${result}*\n\n💸 Attempting payout...`, { parse_mode: 'Markdown'});
             const sendResult = await sendSol(connection, payerPrivateKey, winnerPublicKey, payoutLamports);

             if (sendResult.success) {
                 await bot.sendMessage(chat_id, `💰 Payout successful! TX: \`${sendResult.signature}\``, { parse_mode: 'Markdown'});
                 await recordPayout(betId, 'completed_win_paid', sendResult.signature);
             } else {
                 await bot.sendMessage(chat_id, `⚠️ Payout failed: ${sendResult.error}. Please contact support with Bet ID ${betId}.`);
                 await updateBetStatus(betId, 'completed_win_payout_failed');
             }
         } catch(e) {
             console.error(`Bet ${betId}: Error during payout process:`, e);
             await bot.sendMessage(chat_id, `⚠️ Payout failed due to an internal error. Please contact support with Bet ID ${betId}.`);
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
    const horseName = bet_details.horse;
    const odds = bet_details.odds;

    // Race Logic
     const horses = [
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

    await bot.sendMessage(chatId, `🏇 Race for Bet ID ${betId} is starting! You picked *${horseName}*!`, {parse_mode: 'Markdown'});
    await new Promise(resolve => setTimeout(resolve, 2000));
    await bot.sendMessage(chatId, `...and they're off!`);
    await new Promise(resolve => setTimeout(resolve, 3000));
    await bot.sendMessage(chatId, `🏁 **The winner is... ${winningHorse.emoji} *${winningHorse.name}*!** 🏁`, { parse_mode: 'Markdown' });

    const win = (horseName.toLowerCase() === winningHorse.name.toLowerCase());
    const payoutLamports = win ? Math.round(expected_lamports * odds) : 0;

    let displayName = `User ${user_id}`;
     try { const chatMember = await bot.getChatMember(chat_id, user_id); displayName = chatMember.user.username ? `@${chatMember.user.username}` : chatMember.user.first_name; }
     catch (e) { console.warn(`Couldn't get username for payout message (Bet ${betId}):`, e.message); }

    if (win) {
        console.log(`Bet ${betId}: User ${user_id} WON Race! Payout: ${payoutLamports / LAMPORTS_PER_SOL} SOL`);
        const payerPrivateKey = process.env.RACE_BOT_PRIVATE_KEY;
        const winnerAddress = await getLinkedWallet(user_id);

        if (!winnerAddress) {
             console.error(`Bet ${betId}: Cannot find winner address for race payout!`);
              await bot.sendMessage(chatId, `🎉 Congratulations, ${displayName}! Your horse *${horseName}* won!\nPayout: ${(payoutLamports / LAMPORTS_PER_SOL).toFixed(6)} SOL\n⚠️ Payout failed: Could not determine your linked wallet address.`, { parse_mode: 'Markdown'});
              await updateBetStatus(betId, 'error_payout_no_wallet');
              return;
        }

         try {
             const winnerPublicKey = new PublicKey(winnerAddress);
             await bot.sendMessage(chatId, `🎉 Congratulations, ${displayName}! Your horse *${horseName}* won!\nPayout: ${(payoutLamports / LAMPORTS_PER_SOL).toFixed(6)} SOL\n\n💸 Attempting payout...`, {parse_mode: 'Markdown'});
             const sendResult = await sendSol(connection, payerPrivateKey, winnerPublicKey, payoutLamports);

             if (sendResult.success) {
                 await bot.sendMessage(chat_id, `💰 Race payout successful! TX: \`${sendResult.signature}\``, { parse_mode: 'Markdown'});
                  await recordPayout(betId, 'completed_win_paid', sendResult.signature);
             } else {
                 await bot.sendMessage(chat_id, `⚠️ Race payout failed: ${sendResult.error}. Please contact support with Bet ID ${betId}.`);
                 await updateBetStatus(betId, 'completed_win_payout_failed');
             }
         } catch(e) {
             console.error(`Bet ${betId}: Error during race payout process:`, e);
             await bot.sendMessage(chat_id, `⚠️ Race payout failed due to an internal error. Please contact support with Bet ID ${betId}.`);
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
        await initializeDatabase(); // Ensure DB is ready first
        const PORT = process.env.PORT || 3000; // Use 3000 based on successful deploy config
        app.listen(PORT, () => {
            console.log(`✅ Healthcheck server listening on port ${PORT}`);
            console.log("Application fully started.");
            // Log polling status
            if (bot.isPolling()) {
                 console.log("--- Bot is ACTIVE and POLLING for messages ---");
            } else {
                 console.log("--- Bot is initialized but NOT POLLING (Webhooks needed for interaction) ---");
            }
             console.log("--- Monitoring for payments... ---");
        });
    } catch (error) {
        console.error("💥 Failed to start server:", error);
        process.exit(1);
    }
}

// --- Start the server ---
startServer();

// --- Graceful shutdown ---
const gracefulShutdown = (signal) => {
    console.log(`Received ${signal}. Shutting down gracefully...`);
    clearInterval(monitorInterval); // Stop monitor first
    console.log("Payment monitor stopped.");
    // Optional: Inform users bot is restarting if possible/needed
    // bot.sendMessage(...).catch(...); // Might fail if bot already stopping

    // Attempt to close DB pool
    pool.end().then(() => {
        console.log('Database pool closed.');
        process.exit(0); // Exit after pool is closed
    }).catch(err => {
         console.error("Error closing database pool:", err);
         process.exit(1); // Exit with error if pool fails to close
    });

    // Force exit if pool doesn't close quickly
    setTimeout(() => {
        console.warn("Database pool did not close in time. Forcing exit.");
        process.exit(1);
    }, 5000); // 5 second timeout
};

process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

// --- Error Handlers ---
process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
  // Consider whether to crash or attempt recovery
  // gracefulShutdown('Unhandled Rejection'); // Optional: attempt graceful shutdown
});
process.on('uncaughtException', (error) => {
  console.error('Uncaught Exception:', error);
  // Definitely exit after uncaught exception, but try to shutdown gracefully first
  gracefulShutdown('Uncaught Exception');
  setTimeout(() => process.exit(1), 7000); // Force exit if graceful shutdown hangs
});
