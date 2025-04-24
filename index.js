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
    // MemoProgram,
} = require('@solana/web3.js');
const bs58 = require('bs58');
const { randomBytes } = require('crypto');

const app = express();

// --- PostgreSQL Setup ---
console.log("Setting up PostgreSQL Pool...");
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  // ssl: { rejectUnauthorized: false } // Optional SSL config
});
console.log("PostgreSQL Pool created.");

// --- Database Initialization (Corrected Version) ---
async function initializeDatabase() {
  console.log("Initializing Database...");
  let client;
  try {
    client = await pool.connect();
    console.log("DB client connected.");
    // Bets table
    await client.query(`
      CREATE TABLE IF NOT EXISTS bets (
        id SERIAL PRIMARY KEY, user_id TEXT NOT NULL, chat_id TEXT NOT NULL,
        game_type TEXT NOT NULL, bet_details JSONB, expected_lamports BIGINT NOT NULL,
        memo_id TEXT UNIQUE NOT NULL, status TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        expires_at TIMESTAMPTZ NOT NULL, paid_tx_signature TEXT UNIQUE, payout_tx_signature TEXT UNIQUE
      );
    `);
    console.log("✅ Base 'bets' table structure checked/created.");
    // Check/Add columns
    const columnsToAdd = [ { name: 'paid_tx_signature', type: 'TEXT UNIQUE' }, { name: 'payout_tx_signature', type: 'TEXT UNIQUE' } ];
    for (const col of columnsToAdd) {
         const checkColumnQuery = `SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'bets' AND column_name = $1;`;
         const colExistsResult = await client.query(checkColumnQuery, [col.name]);
         if (colExistsResult.rowCount === 0) {
             const addColumnQuery = `ALTER TABLE public.bets ADD COLUMN ${col.name} ${col.type};`;
             await client.query(addColumnQuery);
             console.log(`✅ Added missing column '${col.name}' to 'bets' table.`);
         }
    }
    console.log("✅ All required columns in 'bets' table verified/added.");
    // Wallets table
    await client.query(`CREATE TABLE IF NOT EXISTS wallets ( user_id TEXT PRIMARY KEY, wallet_address TEXT NOT NULL, linked_at TIMESTAMPTZ NOT NULL DEFAULT NOW() );`);
    console.log("✅ Database table 'wallets' checked/created successfully.");
  } catch (err) {
    console.error("❌ Error initializing database tables:", err); throw err;
  } finally {
    if (client) { client.release(); console.log("ℹ️ Database client released after initialization check."); }
  }
}
// --- End PostgreSQL Setup ---


// --- Bot and Solana Initialization ---
console.log("Initializing Telegram Bot (NO POLLING)...");
// Initialize bot WITHOUT polling for stability on Railway
const bot = new TelegramBot(process.env.BOT_TOKEN); // <<< POLLING IS REMOVED/FALSE
console.log("Telegram Bot initialized.");

console.log("Initializing Solana Connection...");
const connection = new Connection(process.env.RPC_URL, 'confirmed');
console.log("Solana Connection initialized.");
// --- End Initialization ---

// --- Express Setup (Needed for Health Check and Webhooks later) ---
// Middleware to parse JSON bodies (needed for Telegram webhook later)
app.use(express.json());
app.get('/', (req, res) => {
  // console.log("Health check endpoint '/' hit!");
  res.status(200).send('OK');
});
// --- NOTE: We will add the app.post('/webhook/...') route later ---

// --- In-Memory State (Minimal) ---
const confirmCooldown = {};
const cooldownInterval = 3000;

// --- Constants ---
const MIN_BET = 0.01; const MAX_BET = 1.0; const RACE_MIN_BET = 0.01; const RACE_MAX_BET = 1.0; const PAYMENT_EXPIRY_MINUTES = 15;

// --- Helper Functions ---
function generateMemoId(prefix = 'BET') { return prefix + randomBytes(6).toString('hex').toUpperCase(); }

function findMemoInTx(tx) { if (!tx?.transaction?.message?.instructions) return null; try { const MEMO_PROGRAM_ID = 'Memo1UhkJRfHyvLMcVuc6beZNRYqUP2VZwW'; for (const instruction of tx.transaction.message.instructions) { let programId = ''; if (instruction.programIdIndex !== undefined && tx.transaction.message.accountKeys) { const keyInfo = tx.transaction.message.accountKeys[instruction.programIdIndex]; if (keyInfo?.pubkey) programId = keyInfo.pubkey.toBase58(); else if (typeof keyInfo === 'string') programId = new PublicKey(keyInfo).toBase58(); } else if (instruction.programId) { programId = instruction.programId.toBase58 ? instruction.programId.toBase58() : instruction.programId.toString(); } if (programId === MEMO_PROGRAM_ID && instruction.data) return bs58.decode(instruction.data).toString('utf-8'); } } catch (e) { console.error("Error parsing memo:", e); } return null; }

function getPayerFromTransaction(tx) { if (!tx || !tx.meta || !tx.transaction?.message?.accountKeys) return null; const message = tx.transaction.message; const preBalances = tx.meta.preBalances; const postBalances = tx.meta.postBalances; if (message.accountKeys.length > 0 && message.accountKeys[0]?.signer) { let firstSignerKey; if (message.accountKeys[0].pubkey) firstSignerKey = message.accountKeys[0].pubkey; else if (typeof message.accountKeys[0] === 'string') firstSignerKey = new PublicKey(message.accountKeys[0]); else console.warn("Could not derive PublicKey from first signer account key."); if (firstSignerKey) { const balanceDiff = (preBalances[0] ?? 0) - (postBalances[0] ?? 0); if (balanceDiff > 0) { console.log(`getPayer: Identified payer as first signer: ${firstSignerKey.toBase58()}`); return firstSignerKey; } else console.warn(`getPayer: First signer ${firstSignerKey.toBase58()} had non-positive balance change.`); } } for (let i = 0; i < message.accountKeys.length; i++) { if (i >= preBalances.length || i >= postBalances.length) continue; if (message.accountKeys[i]?.signer) { let key; if (message.accountKeys[i].pubkey) key = message.accountKeys[i].pubkey; else if (typeof message.accountKeys[i] === 'string') key = new PublicKey(message.accountKeys[i]); else continue; const balanceDiff = (preBalances[i] ?? 0) - (postBalances[i] ?? 0); if (balanceDiff > 0) { console.log(`getPayer: Identified payer via fallback: ${key.toBase58()}`); return key; } } } console.error("getPayerFromTransaction: Could not determine transaction payer."); return null; }

async function sendSol(connection, payerPrivateKey, recipientPublicKey, amountLamports) { const maxRetries = 3; let lastError = null; const recipientPubKey = (typeof recipientPublicKey === 'string') ? new PublicKey(recipientPublicKey) : recipientPublicKey; const amountSOL = Number(amountLamports) / LAMPORTS_PER_SOL; for (let retryCount = 0; retryCount < maxRetries; retryCount++) { try { const payerWallet = Keypair.fromSecretKey(bs58.decode(payerPrivateKey)); const latestBlockhash = await connection.getLatestBlockhash(); const transaction = new Transaction({ recentBlockhash: latestBlockhash.blockhash, feePayer: payerWallet.publicKey }).add( SystemProgram.transfer({ fromPubkey: payerWallet.publicKey, toPubkey: recipientPubKey, lamports: BigInt(amountLamports) }) ).add( ComputeBudgetProgram.setComputeUnitLimit({ units: 100000 }) ).add( ComputeBudgetProgram.setComputeUnitPrice({ microLamports: 1000 }) ); const signature = await sendAndConfirmTransaction( connection, transaction, [payerWallet], { commitment: 'confirmed', skipPreflight: false } ); console.log(`✅ Sent ${amountSOL.toFixed(6)} SOL to ${recipientPubKey.toBase58()}. Signature: ${signature}`); return { success: true, signature }; } catch (error) { console.error(`❌ Error sending ${amountSOL.toFixed(6)} SOL (attempt ${retryCount + 1}/${maxRetries}):`, error); lastError = error; if (error.message.includes('blockhash') || error.message.includes('Blockhash not found') || error.message.includes('timeout')) { await new Promise(resolve => setTimeout(resolve, 1500 * (retryCount + 1))); } else { break; } } } console.error(`❌ Failed to send ${amountSOL.toFixed(6)} SOL after ${maxRetries} retries.`); return { success: false, error: lastError ? lastError.message : 'Max retries exceeded without success' }; }

// --- Database Interaction Functions ---
async function savePendingBet(userId, chatId, gameType, details, lamports, memoId, expiresAt) { const query = ` INSERT INTO bets (user_id, chat_id, game_type, bet_details, expected_lamports, memo_id, status, expires_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id; `; const values = [ String(userId), String(chatId), gameType, details, BigInt(lamports), memoId, 'awaiting_payment', expiresAt ]; try { const res = await pool.query(query, values); console.log(`DB: Saved pending ${gameType} bet ID ${res.rows[0].id} for user ${userId} with memo ${memoId}`); return { success: true, id: res.rows[0].id }; } catch (err) { console.error(`DB Error: Failed to save bet for user ${userId}:`, err); if (err.code === '23505' && err.constraint === 'bets_memo_id_key') return { success: false, error: 'Memo ID collision occurred.' }; return { success: false, error: err.message }; } }
async function findBetByMemo(memoId) { const query = `SELECT * FROM bets WHERE memo_id = $1 AND status = 'awaiting_payment'`; try { const res = await pool.query(query, [memoId]); return res.rows[0]; } catch (err) { console.error(`DB Error: Error finding bet by memo ${memoId}:`, err); return undefined; } }
async function findPendingBets() { const query = `SELECT * FROM bets WHERE status = 'awaiting_payment' AND expires_at > NOW()`; try { const res = await pool.query(query); return res.rows; } catch(err) { console.error(`DB Error: Error finding pending bets:`, err); return []; } }
async function updateBetStatus(betId, newStatus) { const query = 'UPDATE bets SET status = $1 WHERE id = $2'; try { const res = await pool.query(query, [newStatus, betId]); if (res.rowCount > 0) console.log(`DB: Updated bet ${betId} status to ${newStatus}.`); else console.warn(`DB Warn: Update status bet ${betId} to ${newStatus} no row affected.`); return { success: res.rowCount > 0 }; } catch (err) { console.error(`DB Error: Updating bet ${betId} status to ${newStatus}:`, err); return { success: false, error: err.message }; } }
async function markBetPaid(betId, txSignature) { const query = ` UPDATE bets SET status = $1, paid_tx_signature = $2 WHERE id = $3 AND status = $4`; try { const res = await pool.query(query, ['payment_verified', txSignature, betId, 'awaiting_payment']); console.log(`DB: Marked bet ${betId} paid by ${txSignature}. Rows affected: ${res.rowCount}`); return { success: res.rowCount > 0 }; } catch (err) { console.error(`DB Error: Marking bet ${betId} paid:`, err); if (err.code === '23505') { console.warn(`DB Warn: Constraint violation mark bet ${betId} paid ${txSignature}.`); return { success: false, error: 'Tx sig already used or constraint failed.' }; } return { success: false, error: err.message }; } }
async function linkUserWallet(userId, walletAddress) { try { new PublicKey(walletAddress); } catch(e) { console.error(`DB Error: Invalid wallet addr ${userId}: ${walletAddress}`); return { success: false, error: 'Invalid wallet address format.' }; } const query = ` INSERT INTO wallets (user_id, wallet_address, linked_at) VALUES ($1, $2, NOW()) ON CONFLICT (user_id) DO UPDATE SET wallet_address = EXCLUDED.wallet_address, linked_at = NOW(); `; try { await pool.query(query, [String(userId), walletAddress]); console.log(`DB: Linked/Updated wallet user ${userId} to ${walletAddress}`); return { success: true }; } catch (err) { console.error(`DB Error: Linking wallet user ${userId}:`, err); return { success: false, error: err.message }; } }
async function getLinkedWallet(userId) { const query = 'SELECT wallet_address FROM wallets WHERE user_id = $1'; try { const res = await pool.query(query, [String(userId)]); return res.rows[0]?.wallet_address; } catch (err) { console.error(`DB Error: Fetching wallet user ${userId}:`, err); return undefined; } }
async function recordPayout(betId, newStatus, payoutSignature) { const query = ` UPDATE bets SET status = $1, payout_tx_signature = $2 WHERE id = $3`; try { const res = await pool.query(query, [newStatus, payoutSignature, betId]); console.log(`DB: Recorded payout tx ${payoutSignature} bet ${betId} status ${newStatus}. Rows: ${res.rowCount}`); return { success: res.rowCount > 0 }; } catch (err) { console.error(`DB Error: Recording payout bet ${betId}:`, err); return { success: false, error: err.message }; } }

// --- END OF CHUNK 1 of 5 ---
// --- Payment Monitoring Logic ---
// Store last processed signature for each wallet IN MEMORY (clears on restart)
// A more robust solution checks the DB to see if a signature was already processed.
let processedSignaturesThisSession = new Set(); // Use a Set for efficient checks within this session
let isMonitorRunning = false; // Prevent overlapping monitor runs

async function monitorPayments() {
    if (isMonitorRunning) return;
    isMonitorRunning = true;
    // console.log("Running payment monitor cycle..."); // Can enable for debugging

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
            let fetchOptions = { limit: 25 }; // Fetch a reasonable number

            try {
                signatures = await connection.getSignaturesForAddress(targetPubKey, fetchOptions);
            } catch (rpcError) {
                if (rpcError.message.includes('429')) {
                    console.warn(`Monitor Warn: RPC rate limit hit for ${targetAddress}. Skipping cycle.`);
                } else {
                    console.error(`Monitor Error: RPC error fetching signatures for ${targetAddress}:`, rpcError.message);
                }
                continue; // Skip this wallet on RPC error
            }

            if (!signatures || signatures.length === 0) continue; // No recent signatures for this wallet

            // Process oldest fetched first towards newest
            for (let i = signatures.length - 1; i >= 0; i--) {
                const signature = signatures[i].signature;

                // Skip if already processed in this session (basic in-memory check)
                if (processedSignaturesThisSession.has(signature)) continue;

                // Check DB if this TX has already paid a bet (more robust check)
                // This prevents processing the same tx again after a restart
                const checkProcessedQuery = 'SELECT id FROM bets WHERE paid_tx_signature = $1';
                const processedResult = await pool.query(checkProcessedQuery, [signature]);
                if (processedResult.rowCount > 0) {
                    processedSignaturesThisSession.add(signature); // Add to session memory too
                    // console.log(`Monitor: Skipping ${signature}, already processed in DB.`);
                    continue;
                }

                // Fetch transaction details only if not already processed
                let tx;
                try {
                    tx = await connection.getParsedTransaction(signature, { maxSupportedTransactionVersion: 0 });
                    if (!tx || tx.meta?.err) {
                        processedSignaturesThisSession.add(signature); // Mark irrelevant tx as seen this session
                        continue; // Skip failed or unparseable tx
                    }
                } catch (fetchErr) {
                    // Don't mark as processed if fetch fails, could be temporary RPC issue
                    console.error(`Monitor Error: Failed fetch for tx ${signature}:`, fetchErr.message);
                    continue; // Skip this signature on error
                }

                // Find Memo
                const memo = findMemoInTx(tx);
                if (!memo) {
                    processedSignaturesThisSession.add(signature); // Mark no-memo tx as seen this session
                    continue;
                }

                // Only log memos that might be ours to reduce noise
                if (memo.startsWith('CF') || memo.startsWith('RA') || memo.startsWith('BET')) {
                    console.log(`Monitor: Found memo "${memo}" in transaction ${signature}`);
                }

                // Check if memo corresponds to a pending bet
                const bet = await findBetByMemo(memo); // Checks for memo AND status='awaiting_payment'
                if (!bet) {
                    processedSignaturesThisSession.add(signature); // No matching pending bet
                    continue;
                }

                // Double-check: Ensure bet status is still 'awaiting_payment' right before processing
                // (Handles rare race condition if status changed between findBetByMemo and now)
                 if (bet.status !== 'awaiting_payment') {
                    console.warn(`Monitor: Bet ${bet.id} for memo ${memo} status changed to ${bet.status} before processing. Skipping.`);
                    processedSignaturesThisSession.add(signature); // Mark as seen
                    continue;
                 }

                // Check if game type matches wallet
                if (bet.game_type !== wallet.game) {
                    console.warn(`Monitor: Memo ${memo} found in ${wallet.type} wallet, but bet is for ${bet.game_type}. Skipping.`);
                    processedSignaturesThisSession.add(signature);
                    continue;
                }

                // Check amount transferred TO the target wallet
                let transferAmount = 0n; // Use BigInt for lamports
                let payerAddress = null; // Solana address (string) of the sender
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
                                   // Add amounts as BigInts
                                   transferAmount += BigInt(transferInfo.lamports || transferInfo.amount || 0);
                                   if (!payerAddress) payerAddress = transferInfo.source; // Capture first source paying target
                              }
                         }
                     }
                     if(payerAddress) console.log(`Monitor: Found total transfer of ${transferAmount} lamports from ${payerAddress} to ${targetAddress} in tx ${signature}`);
                     else console.log(`Monitor: No direct transfer to ${targetAddress} found in tx ${signature}`);
                }

                // Amount check with tolerance (ensure expected_lamports is BigInt)
                const expectedLamportsBigInt = BigInt(bet.expected_lamports);
                const lamportTolerance = 5000n; // Use BigInt for tolerance
                if (transferAmount < (expectedLamportsBigInt - lamportTolerance) || transferAmount > (expectedLamportsBigInt + lamportTolerance)) {
                    console.warn(`Monitor: Amount mismatch memo ${memo}. Expected ${expectedLamportsBigInt}, Got ${transferAmount}.`);
                    await updateBetStatus(bet.id, 'error_payment_mismatch');
                    processedSignaturesThisSession.add(signature); // Mark as processed (mismatched)
                    continue;
                }

                // Timestamp check
                const txTime = tx.blockTime ? new Date(tx.blockTime * 1000) : new Date(0);
                if (txTime.getTime() === 0) console.warn(`Monitor: Tx ${signature} has no blockTime!`);
                if (txTime > new Date(bet.expires_at)) {
                    console.warn(`Monitor: Payment memo ${memo} (Tx: ${signature}) expired.`);
                    await updateBetStatus(bet.id, 'error_payment_expired');
                    processedSignaturesThisSession.add(signature); // Mark as processed (expired)
                    continue;
                }

                // --- Payment Verified ---
                console.log(`✅ Monitor: Payment VERIFIED for bet ID ${bet.id} (memo: ${memo}, tx: ${signature})`);

                // Mark bet as paid IN DATABASE (this prevents reprocessing the same bet/tx)
                // Pass the current signature as the one that paid
                const markResult = await markBetPaid(bet.id, signature);
                if (!markResult.success) {
                    // If marking failed (e.g., signature constraint violation because another monitor cycle processed it first, or status wasn't awaiting_payment)
                    console.warn(`Monitor: Failed to mark bet ${bet.id} paid by ${signature}. Error: ${markResult.error}. Already processed by another cycle?`);
                    processedSignaturesThisSession.add(signature); // Mark as seen anyway
                    continue; // IMPORTANT: Skip if we couldn't mark it paid, prevents double processing
                }

                // Link wallet address (only if we successfully marked the bet paid)
                if (payerAddress) { await linkUserWallet(bet.user_id, payerAddress); }
                else {
                    const pKey = getPayerFromTransaction(tx); // Fallback to heuristic
                    if (pKey) { await linkUserWallet(bet.user_id, pKey.toBase58()); }
                    else { console.warn(`Monitor: Could not determine payer for bet ${bet.id} to link wallet.`); }
                }

                processedSignaturesThisSession.add(signature); // Mark as successfully processed in this session

                // Trigger game processing (run async, don't await here)
                processPaidBet(bet).catch(e => {
                     console.error(`Error during background game processing for bet ${bet.id}:`, e);
                     updateBetStatus(bet.id, 'error_processing_exception'); // Mark bet as error
                });

            } // End signature loop
        } // End wallet loop
    } catch (error) {
        console.error("❌ Monitor Error: Unexpected error in monitor cycle:", error);
    } finally {
        isMonitorRunning = false; // Allow next run
    }
}
const monitorIntervalSeconds = 30;
console.log(`ℹ️ Starting payment monitor. Interval: ${monitorIntervalSeconds} seconds.`);
let monitorInterval = null; // Define variable to hold interval ID
// --- End Payment Monitor ---
// --- Bot Command Handlers ---
// Wrapped in try/catch for basic error handling

bot.on('polling_error', (error) => {
    console.error(`Polling error: ${error.code} - ${error.message}`);
    // Optional: Add logic here if specific polling errors need handling,
    // but often indicates instability or the 409 conflict if another instance runs.
});

bot.on('webhook_error', (error) => {
    console.error(`Webhook error: ${error.code} - ${error.message}`);
    // If/when using webhooks, handle errors here (e.g., certificate issues)
});

bot.on('error', (error) => {
    console.error('General Bot Error:', error);
    // Generic error handler
});


bot.onText(/\/start$/, async (msg) => {
    try {
        const chatId = msg.chat.id;
        const imageUrl = 'https://i.ibb.co/9vDo58q/banner.gif'; // Example static URL
        await bot.sendAnimation(chatId, imageUrl, { caption: `🎰 Welcome to *Solana Gambles*!\n\nUse /coinflip or /race to start.\nUse /wallet to see linked wallet.`, parse_mode: 'Markdown' });
    } catch (error) {
        console.error("Error in /start handler:", error);
        // Avoid crashing, maybe send a text message if animation fails
        if (msg && msg.chat && msg.chat.id) {
             try { await bot.sendMessage(msg.chat.id, "Welcome! Use /coinflip or /race."); } catch (e) {}
        }
    }
});

bot.onText(/\/reset$/, async (msg) => {
    try {
        const chatId = msg.chat.id;
        if (msg.chat.type === 'group' || msg.chat.type === 'supergroup') {
            await bot.sendMessage(chatId, `⚠️ The /reset command is disabled in group chats.`);
        } else {
            delete confirmCooldown[msg.from.id]; // Clear memory cooldown
            // Consider if you want /reset to clear PENDING bets for this user in DB
            // await pool.query("UPDATE bets SET status = 'expired' WHERE user_id = $1 AND status = 'awaiting_payment'", [String(msg.from.id)]);
            await bot.sendMessage(chatId, `🔄 Cooldowns reset.`);
        }
    } catch (error) {
        console.error("Error in /reset handler:", error);
        if (msg && msg.chat && msg.chat.id) {
             try { await bot.sendMessage(msg.chat.id, "Error processing reset."); } catch (e) {}
        }
    }
});

bot.onText(/\/coinflip$/, async (msg) => {
    try {
        // Use Markdown backticks for code formatting within the main template literal
        const helpMessage = `🪙 Coinflip! Choose amount and side:\n\n` +
                            `\`/bet amount heads\`\n` + // Use Markdown code format
                            `\`/bet amount tails\`\n\n` + // Use Markdown code format
                            `Min: ${MIN_BET} SOL | Max: ${MAX_BET} SOL`;

        await bot.sendMessage( msg.chat.id, helpMessage, { parse_mode: 'Markdown' } );
    } catch (error) {
        console.error("Error in /coinflip handler:", error);
         if (msg && msg.chat && msg.chat.id) {
             try { await bot.sendMessage(msg.chat.id, "Error showing coinflip info."); } catch (e) {}
        }
    }
});

bot.onText(/\/wallet$/, async (msg) => {
    try {
        const userId = String(msg.from.id); // Ensure user ID is string for DB consistency
        const walletAddress = await getLinkedWallet(userId); // Fetch from DB
        if (walletAddress) {
            await bot.sendMessage(msg.chat.id, `💰 Your linked wallet address is:\n\`${walletAddress}\``, { parse_mode: 'Markdown' });
        } else {
            await bot.sendMessage(msg.chat.id, `⚠️ No wallet linked yet. Place a bet, pay with the memo, and your wallet will be linked automatically.`);
        }
    } catch (error) {
        console.error("Error in /wallet handler:", error);
         if (msg && msg.chat && msg.chat.id) {
             try { await bot.sendMessage(msg.chat.id, "Error fetching wallet info."); } catch (e) {}
        }
    }
});

bot.onText(/\/refresh$/, async (msg) => {
     try {
         const chatId = msg.chat.id;
         const imageUrl = 'https://i.ibb.co/9vDo58q/banner.gif';
         await bot.sendAnimation(chatId, imageUrl, { caption: `🎰 Solana Gambles\n\n/coinflip | /race | /wallet`, parse_mode: 'Markdown'});
     } catch (error) {
        console.error("Error sending refresh animation:", error);
         if (msg && msg.chat && msg.chat.id) {
             try { await bot.sendMessage(msg.chat.id, `Commands:\n/coinflip | /race | /wallet`); } catch (e) {}
        }
    }
});

// /bet command (Coinflip)
bot.onText(/\/bet (\d+\.?\d*) (heads|tails)/i, async (msg, match) => {
    const userId = String(msg.from.id); // Ensure string
    const chatId = String(msg.chat.id); // Ensure string
    try {
        const betAmount = parseFloat(match[1]);
        const userChoice = match[2].toLowerCase();

        if (isNaN(betAmount) || betAmount < MIN_BET || betAmount > MAX_BET) {
            // Don't need return if we throw/catch, just send message
             await bot.sendMessage(chatId, `⚠️ Bet must be between ${MIN_BET} - ${MAX_BET} SOL`);
             return; // Exit handler after sending message
        }
        if (confirmCooldown[userId] && (Date.now() - confirmCooldown[userId]) < cooldownInterval) {
             await bot.sendMessage(chatId, `⚠️ Please wait a few seconds...`);
             return;
        }
        confirmCooldown[userId] = Date.now(); // Set cooldown

        // TODO: Check if user has another 'awaiting_payment' bet? Prevent multiple pending bets?

        const memoId = generateMemoId('CF');
        const expectedLamports = BigInt(Math.round(betAmount * LAMPORTS_PER_SOL)); // Use BigInt
        const expiresAt = new Date(Date.now() + PAYMENT_EXPIRY_MINUTES * 60 * 1000);
        const betDetails = { choice: userChoice };

        const saveResult = await savePendingBet(userId, chatId, 'coinflip', betDetails, expectedLamports, memoId, expiresAt);
        // If save failed, throw error to be caught
        if (!saveResult.success) { throw new Error(saveResult.error || 'Failed to save bet to database.'); }

        // Instruct User
        await bot.sendMessage(chatId,
            `✅ Coinflip Bet registered!\n\n`+
            `💸 Send *exactly ${betAmount.toFixed(6)} SOL* to:\n` +
            `\`${process.env.MAIN_WALLET_ADDRESS}\`\n\n` +
            `*MEMO:* \`${memoId}\`\n\n` + // Use Markdown for memo
            `Expires in ${PAYMENT_EXPIRY_MINUTES} mins. Auto verification.`,
            { parse_mode: 'Markdown' }
        );
    } catch (error) {
        console.error(`Error in /bet handler for user ${userId}:`, error);
        // Inform user about the error
        await bot.sendMessage(chatId, `⚠️ An error occurred registering your bet. ${error.message.includes('collision') ? 'Please try again.' : 'Please try again later.'}`);
    }
});

// /race command
bot.onText(/\/race$/, async (msg) => {
    try {
        const chatId = msg.chat.id;
        const horses = [ /* Horse data */ { name: 'Yellow', emoji: '🟡', odds: 1.1, winProbability: 0.25 }, { name: 'Orange', emoji: '🟠', odds: 2.0, winProbability: 0.20 }, { name: 'Blue', emoji: '🔵', odds: 3.0, winProbability: 0.15 }, { name: 'Cyan', emoji: '🔷', odds: 4.0, winProbability: 0.12 }, { name: 'White', emoji: '⚪', odds: 5.0, winProbability: 0.09 }, { name: 'Red', emoji: '🔴', odds: 6.0, winProbability: 0.07 }, { name: 'Black', emoji: '⚫', odds: 7.0, winProbability: 0.05 }, { name: 'Pink', emoji: '🌸', odds: 8.0, winProbability: 0.03 }, { name: 'Purple', emoji: '🟣', odds: 9.0, winProbability: 0.02 }, { name: 'Green', emoji: '🟢', odds: 10.0, winProbability: 0.01 }, { name: 'Silver', emoji: '💎', odds: 15.0, winProbability: 0.01 }, ];
        let raceMessage = `🏇 Race! Place your bets!\n\n`;
        horses.forEach(horse => { raceMessage += `${horse.emoji} *${horse.name}* (Odds: ${horse.odds.toFixed(1)}x)\n`; });
        // Use Markdown backticks for the example command
        raceMessage += `\nUse:\n\`${String.fromCharCode(96)}/betrace [amount] [horse_name]\`\nMin: ${RACE_MIN_BET} SOL | Max: ${RACE_MAX_BET} SOL`;
        await bot.sendMessage(chatId, raceMessage, { parse_mode: 'Markdown' });
    } catch(error) {
        console.error("Error in /race handler:", error);
         if (msg && msg.chat && msg.chat.id) {
             try { await bot.sendMessage(msg.chat.id, "Error showing race info."); } catch (e) {}
        }
    }
});

// /betrace command
bot.onText(/\/betrace (\d+\.?\d*) (\w+)/i, async (msg, match) => {
     const userId = String(msg.from.id); // Ensure string
     const chatId = String(msg.chat.id); // Ensure string
     try {
        const betAmount = parseFloat(match[1]);
        const chosenHorseName = match[2];
        const horses = [ /* Horse data */ { name: 'Yellow', emoji: '🟡', odds: 1.1, winProbability: 0.25 }, { name: 'Orange', emoji: '🟠', odds: 2.0, winProbability: 0.20 }, { name: 'Blue', emoji: '🔵', odds: 3.0, winProbability: 0.15 }, { name: 'Cyan', emoji: '🔷', odds: 4.0, winProbability: 0.12 }, { name: 'White', emoji: '⚪', odds: 5.0, winProbability: 0.09 }, { name: 'Red', emoji: '🔴', odds: 6.0, winProbability: 0.07 }, { name: 'Black', emoji: '⚫', odds: 7.0, winProbability: 0.05 }, { name: 'Pink', emoji: '🌸', odds: 8.0, winProbability: 0.03 }, { name: 'Purple', emoji: '🟣', odds: 9.0, winProbability: 0.02 }, { name: 'Green', emoji: '🟢', odds: 10.0, winProbability: 0.01 }, { name: 'Silver', emoji: '💎', odds: 15.0, winProbability: 0.01 }, ];
        const horse = horses.find(h => h.name.toLowerCase() === chosenHorseName.toLowerCase());

        if (!horse) { return await bot.sendMessage(chatId, `⚠️ Invalid horse name.`); } // Use await
        if (isNaN(betAmount) || betAmount < RACE_MIN_BET || betAmount > RACE_MAX_BET) { return await bot.sendMessage(chatId, `⚠️ Bet must be ${RACE_MIN_BET} - ${RACE_MAX_BET} SOL`); }
        if (confirmCooldown[userId] && (Date.now() - confirmCooldown[userId]) < cooldownInterval) { return await bot.sendMessage(chatId, `⚠️ Please wait...`); }
        confirmCooldown[userId] = Date.now();

        const memoId = generateMemoId('RA');
        const expectedLamports = BigInt(Math.round(betAmount * LAMPORTS_PER_SOL)); // Use BigInt
        const expiresAt = new Date(Date.now() + PAYMENT_EXPIRY_MINUTES * 60 * 1000);
        const betDetails = { horse: horse.name, odds: horse.odds }; // Store horse name and its odds

        const saveResult = await savePendingBet(userId, chatId, 'race', betDetails, expectedLamports, memoId, expiresAt);
        if (!saveResult.success) { throw new Error(saveResult.error); } // Throw error

        await bot.sendMessage(chatId,
            `✅ Race bet registered on ${horse.emoji} *${horse.name}*!\n\n` +
            `💸 Send *exactly ${betAmount.toFixed(6)} SOL* to:\n` +
            `\`${process.env.RACE_WALLET_ADDRESS}\`\n\n` +
            `*MEMO:* \`${memoId}\`\n\n` + // Use Markdown for memo
            `Expires in ${PAYMENT_EXPIRY_MINUTES} mins. Auto verification.`,
            { parse_mode: 'Markdown' }
        );
    } catch (error) {
        console.error(`Error in /betrace handler for user ${userId}:`, error);
        // Inform user about the error
        await bot.sendMessage(chatId, `⚠️ An error occurred registering your bet. ${error.message.includes('collision') ? 'Please try again.' : 'Please try again later.'}`);
    }
});

// Disabled /confirm commands - Send message informing user
bot.onText(/^\/confirm$/, async (msg) => { try { await bot.sendMessage(msg.chat.id, `⚠️ The /confirm command is no longer needed.`); } catch(e){} });
bot.onText(/^\/confirmrace$/, async (msg) => { try { await bot.sendMessage(msg.chat.id, `⚠️ The /confirmrace command is no longer needed.`); } catch(e){} });

// --- End Command Handlers ---
// --- Game Processing Logic ---
// These functions handle the actual game mechanics after payment verification

async function processPaidBet(bet) {
    console.log(`Processing paid bet ID: ${bet.id}, Type: ${bet.game_type}`);
    // Double-check status before processing, even though monitor should have marked it
    if (bet.status !== 'payment_verified') {
        console.warn(`Attempted to process bet ${bet.id} but status was ${bet.status} instead of payment_verified. Aborting.`);
        // If status indicates already processed or error, just return
        if (bet.status !== 'awaiting_payment') return; // Avoid resetting error statuses etc.
        // If somehow still awaiting_payment, mark error? Or rely on monitor retries? For now, just abort.
        return;
    }
    // Lock the bet status to prevent concurrent processing
    const updateResult = await updateBetStatus(bet.id, 'processing_game');
    if (!updateResult.success) {
        // This might happen if another process already started processing this bet ID (unlikely in single instance)
        console.error(`Failed to update status to processing_game for bet ${bet.id}. Aborting processing.`);
        return;
    }

    // Execute the actual game logic
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

    // Simple 50/50 logic (Add house edge back later if needed)
    const result = Math.random() < 0.5 ? 'heads' : 'tails';
    const win = (result === choice);
    const payoutLamports = win ? BigInt(expected_lamports) * 2n : 0n; // Use BigInt; 2x payout

    let displayName = `User ${user_id}`;
    try {
        // Fetch user info to make messages nicer
        const chatMember = await bot.getChatMember(chat_id, user_id);
        displayName = chatMember.user.username ? `@${chatMember.user.username}` : chatMember.user.first_name;
    } catch (e) {
        console.warn(`Couldn't get username for payout message (Bet ${betId}):`, e.message);
    }

    if (win) {
        const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
        console.log(`Bet ${betId}: User ${user_id} WON Coinflip! Payout: ${payoutSOL} SOL`);
        const payerPrivateKey = process.env.BOT_PRIVATE_KEY;
        const winnerAddress = await getLinkedWallet(user_id); // Get wallet from DB

        if (!winnerAddress) {
             console.error(`Bet ${betId}: Cannot find winner address for payout! User ${user_id} has no linked wallet.`);
              await bot.sendMessage(chat_id, `🎉 Congratulations, ${displayName}! You won!\nResult: *${result}*\n⚠️ Payout failed: Could not determine your linked wallet address. Make sure you have successfully completed a bet before to link it.`, { parse_mode: 'Markdown'});
              await updateBetStatus(betId, 'error_payout_no_wallet'); // Final status
              return;
        }

         try {
             const winnerPublicKey = new PublicKey(winnerAddress);
             // Send win message FIRST, then attempt payout
             await bot.sendMessage(chat_id, `🎉 Congratulations, ${displayName}! You won ${payoutSOL.toFixed(6)} SOL!\nResult: *${result}*\n\n💸 Attempting payout...`, { parse_mode: 'Markdown'});

             // Use BigInt payoutLamports for sending
             const sendResult = await sendSol(connection, payerPrivateKey, winnerPublicKey, payoutLamports);

             if (sendResult.success) {
                 await bot.sendMessage(chat_id, `💰 Payout successful! TX: \`${sendResult.signature}\``, { parse_mode: 'Markdown'});
                 // Record payout signature and final status
                 await recordPayout(betId, 'completed_win_paid', sendResult.signature);
             } else {
                 await bot.sendMessage(chat_id, `⚠️ Payout failed: ${sendResult.error}. Please contact support with Bet ID ${betId}.`);
                 // Update status to reflect payout failure after win
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
        await updateBetStatus(betId, 'completed_loss'); // Final status
    }
}


async function handleRaceGame(bet) {
    console.log(`Handling race game for bet ${bet.id}`);
    const { id: betId, user_id, chat_id, bet_details, expected_lamports } = bet;
    const horseName = bet_details.horse;
    const odds = bet_details.odds; // Retrieve odds stored during bet placement

    // Race Logic
     const horses = [
        { name: 'Yellow', emoji: '🟡', odds: 1.1, winProbability: 0.25 }, { name: 'Orange', emoji: '🟠', odds: 2.0, winProbability: 0.20 }, { name: 'Blue', emoji: '🔵', odds: 3.0, winProbability: 0.15 }, { name: 'Cyan', emoji: '🔷', odds: 4.0, winProbability: 0.12 }, { name: 'White', emoji: '⚪', odds: 5.0, winProbability: 0.09 }, { name: 'Red', emoji: '🔴', odds: 6.0, winProbability: 0.07 }, { name: 'Black', emoji: '⚫', odds: 7.0, winProbability: 0.05 }, { name: 'Pink', emoji: '🌸', odds: 8.0, winProbability: 0.03 }, { name: 'Purple', emoji: '🟣', odds: 9.0, winProbability: 0.02 }, { name: 'Green', emoji: '🟢', odds: 10.0, winProbability: 0.01 }, { name: 'Silver', emoji: '💎', odds: 15.0, winProbability: 0.01 },
    ]; // Use the same horse data as before
    let winningHorse;
    const randomNumber = Math.random();
    let cumulativeProbability = 0;
    for (const contender of horses) {
        cumulativeProbability += contender.winProbability;
        if (randomNumber <= cumulativeProbability) { winningHorse = contender; break; }
    }
    if (!winningHorse) winningHorse = horses[horses.length - 1]; // Fallback just in case

    // Send race commentary...
    await bot.sendMessage(chatId, `🏇 Race for Bet ID ${betId} is starting! You picked *${horseName}*!`, {parse_mode: 'Markdown'});
    await new Promise(resolve => setTimeout(resolve, 2000)); // Pause for effect
    await bot.sendMessage(chatId, `...and they're off!`);
    // Add more commentary here if desired...
    await new Promise(resolve => setTimeout(resolve, 3000));
    await bot.sendMessage(chatId, `🏁 **The winner is... ${winningHorse.emoji} *${winningHorse.name}*!** 🏁`, { parse_mode: 'Markdown' });

    // Payout or Lose Message
    const win = (horseName.toLowerCase() === winningHorse.name.toLowerCase()); // Case-insensitive compare
    // Use BigInt for potentially large lamport calculations with odds
    const payoutLamports = win ? BigInt(Math.round(Number(expected_lamports) * odds)) : 0n;

    let displayName = `User ${user_id}`;
     try { const chatMember = await bot.getChatMember(chat_id, user_id); displayName = chatMember.user.username ? `@${chatMember.user.username}` : chatMember.user.first_name; }
     catch (e) { console.warn(`Couldn't get username for payout message (Bet ${betId}):`, e.message); }

    if (win) {
        const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
        console.log(`Bet ${betId}: User ${user_id} WON Race! Payout: ${payoutSOL} SOL`);
        const payerPrivateKey = process.env.RACE_BOT_PRIVATE_KEY; // Use RACE key
        const winnerAddress = await getLinkedWallet(user_id); // Get wallet from DB

        if (!winnerAddress) {
             console.error(`Bet ${betId}: Cannot find winner address for race payout!`);
              await bot.sendMessage(chatId, `🎉 Congratulations, ${displayName}! Your horse *${horseName}* won!\nPayout: ${payoutSOL.toFixed(6)} SOL\n⚠️ Payout failed: Could not determine your linked wallet address.`, { parse_mode: 'Markdown'});
              await updateBetStatus(betId, 'error_payout_no_wallet'); // Final status
              return;
        }

         try {
             const winnerPublicKey = new PublicKey(winnerAddress);
             // Send win message FIRST
              await bot.sendMessage(chatId, `🎉 Congratulations, ${displayName}! Your horse *${horseName}* won!\nPayout: ${payoutSOL.toFixed(6)} SOL\n\n💸 Attempting payout...`, {parse_mode: 'Markdown'});
             // Pass BigInt payoutLamports
             const sendResult = await sendSol(connection, payerPrivateKey, winnerPublicKey, payoutLamports);

             if (sendResult.success) {
                 await bot.sendMessage(chat_id, `💰 Race payout successful! TX: \`${sendResult.signature}\``, { parse_mode: 'Markdown'});
                 // Record payout signature and final status
                  await recordPayout(betId, 'completed_win_paid', sendResult.signature);
             } else {
                 await bot.sendMessage(chat_id, `⚠️ Race payout failed: ${sendResult.error}. Please contact support with Bet ID ${betId}.`);
                 // Update status to reflect payout failure after win
                 await updateBetStatus(betId, 'completed_win_payout_failed');
             }
         } catch(e) {
             console.error(`Bet ${betId}: Error during race payout process:`, e);
             await bot.sendMessage(chat_id, `⚠️ Race payout failed due to an internal error. Please contact support with Bet ID ${betId}.`);
             await updateBetStatus(betId, 'error_payout_exception');
         }
    } else {
        console.log(`Bet ${betId}: User ${user_id} LOST Race.`);
        await bot.sendMessage(chat_id, `❌ *Horse Lost!*\n\nYou picked *${horseName}*, but *${winningHorse.name}* won the race. Better luck next time!`, { parse_mode: "Markdown" });
        await updateBetStatus(betId, 'completed_loss'); // Final status
    }
}
// --- End Game Processing ---
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
                 // This state should ideally not be reached if polling: true was intended
                 // Or means polling failed to start properly.
                 console.log("--- Bot is initialized but WARNING: NOT POLLING (Check for polling errors) ---");
            }
             console.log("--- Monitoring for payments... ---");
             // Start monitor slightly after full startup
             if (!monitorInterval) { // Prevent multiple intervals if restart happens
                 monitorInterval = setInterval(monitorPayments, monitorIntervalSeconds * 1000);
                 monitorPayments(); // Run immediately once
             }
        });
    } catch (error) {
        console.error("💥 Failed to start server:", error);
        process.exit(1); // Exit if essential startup fails
    }
}

// --- Start the server ---
startServer();

// --- Graceful shutdown ---
const gracefulShutdown = (signal) => {
    console.log(`Received ${signal}. Shutting down gracefully...`);
    if (monitorInterval) {
        clearInterval(monitorInterval); // Stop monitor first
        console.log("Payment monitor stopped.");
    }
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
  // Optional: Decide if you want to crash or attempt recovery
  // gracefulShutdown('Unhandled Rejection'); // Optional: attempt graceful shutdown
});
process.on('uncaughtException', (error) => {
  console.error('Uncaught Exception:', error);
  // Definitely exit after uncaught exception, but try to shutdown gracefully first
  gracefulShutdown('Uncaught Exception');
  // Ensure process exits even if graceful shutdown hangs
  setTimeout(() => process.exit(1), 7000);
});

console.log("--- Bot script finished initialization setup ---"); // Final log
