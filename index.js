require('dotenv').config(); // For local development with .env file

// --- Enhanced Environment Variable Checks ---
const REQUIRED_ENV_VARS = [
    'BOT_TOKEN',          // For Telegram API
    'DATABASE_URL',       // For PostgreSQL connection
    'BOT_PRIVATE_KEY',    // For Coinflip payouts
    'RACE_BOT_PRIVATE_KEY', // For Race payouts
    'MAIN_WALLET_ADDRESS',// Wallet for Coinflip bets
    'RACE_WALLET_ADDRESS',// Wallet for Race bets
    'RPC_URL'             // Solana RPC endpoint
];

// Check for Railway-specific variables
if (process.env.RAILWAY_ENVIRONMENT) {
    REQUIRED_ENV_VARS.push('RAILWAY_PUBLIC_DOMAIN');
}

// Validate environment variables
let missingVars = false;
REQUIRED_ENV_VARS.forEach((key) => {
    if (!process.env[key]) {
        if (key === 'RAILWAY_PUBLIC_DOMAIN' && !process.env.RAILWAY_ENVIRONMENT) {
            return; // Skip if not on Railway
        }
        console.error(`❌ Environment variable ${key} is missing.`);
        missingVars = true;
    }
});

if (missingVars) {
    console.error("Please set all required environment variables. Exiting.");
    process.exit(1);
}

// --- Optimized Requires ---
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
} = require('@solana/web3.js');
const bs58 = require('bs58');
const { randomBytes } = require('crypto');

const app = express();

// --- Enhanced PostgreSQL Setup ---
console.log("Setting up optimized PostgreSQL Pool...");
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    max: 20,
    min: 4,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
    ssl: process.env.NODE_ENV === 'production' ? { 
        rejectUnauthorized: false 
    } : false
});
console.log("✅ PostgreSQL Pool created with optimized settings.");

// --- Database Initialization ---
async function initializeDatabase() {
    console.log("Initializing Database with optimized schema...");
    let client;
    try {
        client = await pool.connect();
        console.log("DB client connected.");
        
        // Create tables with optimized schema
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
                expires_at TIMESTAMPTZ NOT NULL,
                paid_tx_signature TEXT UNIQUE,
                payout_tx_signature TEXT UNIQUE,
                processed_at TIMESTAMPTZ
            );
        `);
        
        console.log("✅ Optimized 'bets' table structure verified.");
        
        await client.query(`
            CREATE TABLE IF NOT EXISTS wallets (
                user_id TEXT PRIMARY KEY,
                wallet_address TEXT NOT NULL,
                linked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_used_at TIMESTAMPTZ
            );
        `);
        
        console.log("✅ Optimized 'wallets' table structure verified.");
        
        // Add indexes for performance
        await client.query(`
            CREATE INDEX IF NOT EXISTS idx_bets_status ON bets(status);
            CREATE INDEX IF NOT EXISTS idx_bets_user_id ON bets(user_id);
            CREATE INDEX IF NOT EXISTS idx_bets_expires_at ON bets(expires_at);
        `);
        
        console.log("✅ Database indexes verified.");
    } catch (err) {
        console.error("❌ Error initializing database:", err);
        throw err;
    } finally {
        if (client) {
            client.release();
            console.log("ℹ️ Database client released.");
        }
    }
}
// --- Optimized Solana Connection ---
console.log("Initializing enhanced Solana Connection...");
const connection = new Connection(process.env.RPC_URL, {
    commitment: 'confirmed',
    wsEndpoint: process.env.RPC_WS_URL || undefined,
    httpHeaders: {
        'Content-Type': 'application/json',
        'solana-client': 'SolanaGamblesBot/1.0'
    },
    disableRetryOnRateLimit: false,
    confirmTransactionInitialTimeout: 60000
});
console.log("✅ Solana Connection initialized with optimized settings.");

// --- Telegram Bot Initialization ---
console.log("Initializing Telegram Bot with optimized settings...");
const bot = new TelegramBot(process.env.BOT_TOKEN, {
    polling: false, // We'll handle polling/webhook conditionally later
    request: {
        timeout: 10000,
        agentOptions: {
            keepAlive: true,
            timeout: 60000
        }
    }
});
console.log("✅ Telegram Bot initialized.");

// --- Enhanced Express Setup ---
app.use(express.json({
    limit: '10kb', // Limit JSON payload size
    verify: (req, res, buf) => {
        req.rawBody = buf; // Store raw body for potential verification
    }
}));

// Health check endpoint with monitoring
app.get('/', (req, res) => {
    res.status(200).json({
        status: 'ok',
        timestamp: new Date().toISOString(),
        version: '1.0.0'
    });
});

// Enhanced webhook endpoint
const webhookPath = `/bot${process.env.BOT_TOKEN}`;
app.post(webhookPath, (req, res) => {
    try {
        // Basic validation
        if (!req.body || typeof req.body !== 'object') {
            return res.status(400).send('Invalid request');
        }
        
        bot.processUpdate(req.body);
        res.sendStatus(200);
    } catch (error) {
        console.error("❌ Webhook processing error:", error);
        res.status(500).json({
            error: 'Internal server error',
            details: error.message
        });
    }
});

// --- State Management ---
const confirmCooldown = new Map(); // Using Map for better performance
const cooldownInterval = 3000;

// Wallet address cache with TTL
const walletCache = new Map();
const CACHE_TTL = 300000; // 5 minutes

// Processed signatures tracking with size limit
const processedSignaturesThisSession = new Set();
const MAX_PROCESSED_SIGNATURES = 1000;

// --- Constants ---
const MIN_BET = 0.01;
const MAX_BET = 1.0;
const RACE_MIN_BET = 0.01;
const RACE_MAX_BET = 1.0;
const PAYMENT_EXPIRY_MINUTES = 15;
const HOUSE_EDGE = 0.02; // 2% house edge
// --- Optimized Helper Functions ---
function generateMemoId(prefix = 'BET') {
    return prefix + randomBytes(6).toString('hex').toUpperCase();
}

function findMemoInTx(tx) {
    if (!tx?.transaction?.message?.instructions) return null;
    
    try {
        const MEMO_PROGRAM_ID = 'Memo1UhkJRfHyvLMcVuc6beZNRYqUP2VZwW';
        for (const instruction of tx.transaction.message.instructions) {
            let programId = '';
            
            if (instruction.programIdIndex !== undefined && tx.transaction.message.accountKeys) {
                const keyInfo = tx.transaction.message.accountKeys[instruction.programIdIndex];
                programId = keyInfo?.pubkey ? keyInfo.pubkey.toBase58() : 
                          (typeof keyInfo === 'string' ? new PublicKey(keyInfo).toBase58() : '');
            } else if (instruction.programId) {
                programId = instruction.programId.toBase58 ? 
                           instruction.programId.toBase58() : 
                           instruction.programId.toString();
            }
            
            if (programId === MEMO_PROGRAM_ID && instruction.data) {
                return bs58.decode(instruction.data).toString('utf-8');
            }
        }
    } catch (e) {
        console.error("Error parsing memo:", e);
    }
    return null;
}

function getPayerFromTransaction(tx) {
    if (!tx || !tx.meta || !tx.transaction?.message?.accountKeys) return null;
    
    const message = tx.transaction.message;
    const preBalances = tx.meta.preBalances;
    const postBalances = tx.meta.postBalances;
    
    // Check first signer (most common case)
    if (message.accountKeys.length > 0 && message.accountKeys[0]?.signer) {
        let firstSignerKey;
        if (message.accountKeys[0].pubkey) {
            firstSignerKey = message.accountKeys[0].pubkey;
        } else if (typeof message.accountKeys[0] === 'string') {
            firstSignerKey = new PublicKey(message.accountKeys[0]);
        }
        
        if (firstSignerKey) {
            const balanceDiff = (preBalances[0] ?? 0) - (postBalances[0] ?? 0);
            if (balanceDiff > 0) {
                return firstSignerKey;
            }
        }
    }
    
    // Fallback to checking all signers
    for (let i = 0; i < message.accountKeys.length; i++) {
        if (i >= preBalances.length || i >= postBalances.length) continue;
        
        if (message.accountKeys[i]?.signer) {
            let key;
            if (message.accountKeys[i].pubkey) {
                key = message.accountKeys[i].pubkey;
            } else if (typeof message.accountKeys[i] === 'string') {
                key = new PublicKey(message.accountKeys[i]);
            } else {
                continue;
            }
            
            const balanceDiff = (preBalances[i] ?? 0) - (postBalances[i] ?? 0);
            if (balanceDiff > 0) {
                return key;
            }
        }
    }
    
    return null;
}

// --- Enhanced Database Operations ---
async function savePendingBet(userId, chatId, gameType, details, lamports, memoId, expiresAt) {
    const query = `
        INSERT INTO bets (
            user_id, chat_id, game_type, bet_details, 
            expected_lamports, memo_id, status, expires_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING id;
    `;
    
    const values = [
        String(userId), 
        String(chatId), 
        gameType, 
        details, 
        BigInt(lamports), 
        memoId, 
        'awaiting_payment', 
        expiresAt
    ];
    
    try {
        const res = await pool.query(query, values);
        console.log(`DB: Saved bet ${res.rows[0].id} for user ${userId}`);
        return { success: true, id: res.rows[0].id };
    } catch (err) {
        console.error(`DB Error saving bet ${userId}:`, err);
        if (err.code === '23505') {
            return { success: false, error: 'Memo ID collision.' };
        }
        return { success: false, error: err.message };
    }
}

async function findBetByMemo(memoId) {
    const query = `
        SELECT * FROM bets 
        WHERE memo_id = $1 AND status = 'awaiting_payment'
        FOR UPDATE SKIP LOCKED;
    `;
    
    try {
        const res = await pool.query(query, [memoId]);
        return res.rows[0];
    } catch (err) {
        console.error(`DB Error finding bet ${memoId}:`, err);
        return undefined;
    }
}

async function getLinkedWallet(userId) {
    const cacheKey = `wallet-${userId}`;
    
    // Check cache first
    if (walletCache.has(cacheKey)) {
        const { wallet, timestamp } = walletCache.get(cacheKey);
        if (Date.now() - timestamp < CACHE_TTL) {
            return wallet;
        }
    }
    
    // Query database if not in cache
    const query = `
        SELECT wallet_address FROM wallets 
        WHERE user_id = $1;
    `;
    
    try {
        const res = await pool.query(query, [String(userId)]);
        const wallet = res.rows[0]?.wallet_address;
        
        // Update cache
        if (wallet) {
            walletCache.set(cacheKey, {
                wallet,
                timestamp: Date.now()
            });
        }
        
        return wallet;
    } catch (err) {
        console.error(`DB Error fetching wallet ${userId}:`, err);
        return undefined;
    }
}
// --- Optimized Payment Monitoring ---
let isMonitorRunning = false;
let monitorIntervalSeconds = 30;
let lastProcessedCount = 0;
let monitorInterval = null;

async function processSignature(signature, wallet) {
    if (processedSignaturesThisSession.has(signature)) {
        return 0; // Skip if already processed
    }

    // Check if already processed in DB
    const checkProcessedQuery = `
        SELECT id FROM bets 
        WHERE paid_tx_signature = $1 
        LIMIT 1;
    `;
    
    try {
        const processedResult = await pool.query(checkProcessedQuery, [signature]);
        if (processedResult.rowCount > 0) {
            processedSignaturesThisSession.add(signature);
            return 0;
        }
    } catch (dbError) {
        console.error(`DB Error checking signature ${signature}:`, dbError);
        return 0;
    }

    // Fetch transaction details
    let tx;
    try {
        tx = await connection.getParsedTransaction(signature, { 
            maxSupportedTransactionVersion: 0 
        });
        
        if (!tx || tx.meta?.err) {
            processedSignaturesThisSession.add(signature);
            return 0;
        }
    } catch (fetchErr) {
        console.error(`Error fetching tx ${signature}:`, fetchErr.message);
        return 0;
    }

    // Process memo and bet
    const memo = findMemoInTx(tx);
    if (!memo) {
        processedSignaturesThisSession.add(signature);
        return 0;
    }

    if (memo.startsWith('CF') || memo.startsWith('RA') || memo.startsWith('BET')) {
        console.log(`Monitor: Found memo "${memo}" in tx ${signature}`);
    }

    const bet = await findBetByMemo(memo);
    if (!bet || bet.status !== 'awaiting_payment') {
        processedSignaturesThisSession.add(signature);
        return 0;
    }

    if (bet.game_type !== wallet.game) {
        console.warn(`Memo ${memo} found in wrong wallet type`);
        processedSignaturesThisSession.add(signature);
        return 0;
    }

    // Process transaction amounts
    let transferAmount = 0n;
    let payerAddress = null;
    
    if (tx.meta && tx.transaction?.message?.instructions) {
        const instructions = [
            ...(tx.transaction.message.instructions || []),
            ...(tx.meta.innerInstructions || []).flatMap(i => i.instructions)
        ];
        
        const SYSTEM_PROGRAM_ID = SystemProgram.programId.toBase58();
        
        for (const inst of instructions) {
            let programId = '';
            if (inst.programIdIndex !== undefined && tx.transaction.message.accountKeys) {
                const keyInfo = tx.transaction.message.accountKeys[inst.programIdIndex];
                programId = keyInfo?.pubkey ? keyInfo.pubkey.toBase58() : 
                          (typeof keyInfo === 'string' ? new PublicKey(keyInfo).toBase58() : '');
            } else if (inst.programId) {
                programId = inst.programId.toBase58 ? 
                           inst.programId.toBase58() : 
                           inst.programId.toString();
            }

            if (programId === SYSTEM_PROGRAM_ID && inst.parsed?.type === 'transfer') {
                const transferInfo = inst.parsed.info;
                if (transferInfo.destination === wallet.address) {
                    transferAmount += BigInt(transferInfo.lamports || transferInfo.amount || 0);
                    if (!payerAddress) payerAddress = transferInfo.source;
                }
            }
        }
    }

    // Validate amount
    const expectedLamportsBigInt = BigInt(bet.expected_lamports);
    const lamportTolerance = 5000n;
    
    if (transferAmount < (expectedLamportsBigInt - lamportTolerance) || 
        transferAmount > (expectedLamportsBigInt + lamportTolerance)) {
        console.warn(`Amount mismatch for memo ${memo}`);
        await updateBetStatus(bet.id, 'error_payment_mismatch');
        processedSignaturesThisSession.add(signature);
        return 0;
    }

    // Validate timestamp
    const txTime = tx.blockTime ? new Date(tx.blockTime * 1000) : new Date(0);
    if (txTime > new Date(bet.expires_at)) {
        console.warn(`Payment expired for memo ${memo}`);
        await updateBetStatus(bet.id, 'error_payment_expired');
        processedSignaturesThisSession.add(signature);
        return 0;
    }

    // Mark as paid
    const markResult = await markBetPaid(bet.id, signature);
    if (!markResult.success) {
        console.warn(`Failed to mark bet ${bet.id} as paid`);
        processedSignaturesThisSession.add(signature);
        return 0;
    }

    // Link wallet if possible
    if (payerAddress) {
        await linkUserWallet(bet.user_id, payerAddress);
    } else {
        const pKey = getPayerFromTransaction(tx);
        if (pKey) await linkUserWallet(bet.user_id, pKey.toBase58());
    }

    processedSignaturesThisSession.add(signature);
    processPaidBet(bet).catch(console.error);
    return 1;
}

async function monitorPayments() {
    if (isMonitorRunning) {
        console.log('Monitor already running, skipping cycle');
        return;
    }

    isMonitorRunning = true;
    const startTime = Date.now();
    let processedCount = 0;

    try {
        const walletsToMonitor = [
            { address: process.env.MAIN_WALLET_ADDRESS, type: 'main', game: 'coinflip' },
            { address: process.env.RACE_WALLET_ADDRESS, type: 'race', game: 'race' },
        ];

        // Process each wallet in parallel
        await Promise.all(walletsToMonitor.map(async (wallet) => {
            try {
                const targetPubKey = new PublicKey(wallet.address);
                const signatures = await connection.getSignaturesForAddress(targetPubKey, {
                    limit: 25
                });

                if (!signatures || signatures.length === 0) return;

                // Process signatures in batches
                const BATCH_SIZE = 5;
                for (let i = 0; i < signatures.length; i += BATCH_SIZE) {
                    const batch = signatures.slice(i, i + BATCH_SIZE);
                    const batchResults = await Promise.all(
                        batch.map(sig => processSignature(sig.signature, wallet))
                    );
                    processedCount += batchResults.reduce((sum, count) => sum + count, 0);
                }
            } catch (error) {
                console.error(`Error monitoring wallet ${wallet.address}:`, error);
            }
        }));

    } catch (error) {
        console.error("Monitor Error:", error);
    } finally {
        isMonitorRunning = false;
        lastProcessedCount = processedCount;
        
        console.log(`Monitor processed ${processedCount} txs in ${Date.now() - startTime}ms`);
        
        // Clean up old signatures if cache is too large
        if (processedSignaturesThisSession.size > MAX_PROCESSED_SIGNATURES) {
            console.log('Clearing old processed signatures');
            processedSignaturesThisSession.clear();
        }
        
        // Adjust monitoring interval based on activity
        adjustMonitorInterval(processedCount);
    }
}

function adjustMonitorInterval(currentProcessed) {
    const newInterval = currentProcessed > 10 ? Math.max(10, monitorIntervalSeconds - 5) :
                      currentProcessed === 0 ? Math.min(120, monitorIntervalSeconds + 10) :
                      monitorIntervalSeconds;
    
    if (newInterval !== monitorIntervalSeconds && monitorInterval) {
        monitorIntervalSeconds = newInterval;
        clearInterval(monitorInterval);
        monitorInterval = setInterval(monitorPayments, monitorIntervalSeconds * 1000);
        console.log(`Adjusted monitor interval to ${monitorIntervalSeconds}s`);
    }
}
// --- Optimized Transaction Handling ---
async function sendSol(connection, payerPrivateKey, recipientPublicKey, amountLamports) {
    const maxRetries = 3;
    const baseDelay = 1000;
    const recipientPubKey = (typeof recipientPublicKey === 'string') ? 
        new PublicKey(recipientPublicKey) : recipientPublicKey;
    const amountSOL = Number(amountLamports) / LAMPORTS_PER_SOL;
    
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            const payerWallet = Keypair.fromSecretKey(bs58.decode(payerPrivateKey));
            
            // Get fresh blockhash for each attempt
            const latestBlockhash = await connection.getLatestBlockhash({
                commitment: 'confirmed'
            });
            
            // Build optimized transaction
            const transaction = new Transaction({
                recentBlockhash: latestBlockhash.blockhash,
                feePayer: payerWallet.publicKey
            });
            
            // Add priority fee
            transaction.add(
                ComputeBudgetProgram.setComputeUnitPrice({ microLamports: 1000 })
            );
            
            // Add transfer instruction
            transaction.add(
                SystemProgram.transfer({
                    fromPubkey: payerWallet.publicKey,
                    toPubkey: recipientPubKey,
                    lamports: BigInt(amountLamports)
                })
            );
            
            // Send with timeout
            const signature = await Promise.race([
                sendAndConfirmTransaction(
                    connection,
                    transaction,
                    [payerWallet],
                    { commitment: 'confirmed', skipPreflight: true }
                ),
                new Promise((_, reject) => 
                    setTimeout(() => reject(new Error('Transaction timeout')), 15000)
                )
            ]);
            
            console.log(`✅ Sent ${amountSOL.toFixed(6)} SOL to ${recipientPubKey.toBase58()}`);
            return { success: true, signature };
            
        } catch (error) {
            console.error(`Attempt ${attempt}/${maxRetries} failed:`, error.message);
            
            // Don't retry for invalid params or insufficient funds
            if (error.message.includes('Invalid param') || 
                error.message.includes('Insufficient funds')) {
                break;
            }
            
            // Exponential backoff
            const delay = baseDelay * Math.pow(2, attempt - 1);
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }
    
    return { 
        success: false, 
        error: `Failed after ${maxRetries} attempts` 
    };
}

// --- Enhanced Game Logic ---
async function processPaidBet(bet) {
    console.log(`Processing bet ${bet.id} (${bet.game_type})`);
    
    // Double-check status with database-level lock
    const statusCheck = await pool.query(
        'SELECT status FROM bets WHERE id = $1 FOR UPDATE',
        [bet.id]
    );
    
    if (statusCheck.rows[0]?.status !== 'payment_verified') {
        console.warn(`Bet ${bet.id} status mismatch, aborting`);
        return;
    }

    // Lock the bet status
    await pool.query(
        'UPDATE bets SET status = $1 WHERE id = $2',
        ['processing_game', bet.id]
    );

    try {
        if (bet.game_type === 'coinflip') {
            await handleCoinflipGame(bet);
        } else if (bet.game_type === 'race') {
            await handleRaceGame(bet);
        } else {
            console.error(`Unknown game type for bet ${bet.id}`);
            await updateBetStatus(bet.id, 'error_unknown_game');
        }
    } catch (error) {
        console.error(`Error processing bet ${bet.id}:`, error);
        await updateBetStatus(bet.id, 'error_processing_exception');
    }
}

async function handleCoinflipGame(bet) {
    const { id: betId, user_id, chat_id, bet_details, expected_lamports } = bet;
    const choice = bet_details.choice;

    // Apply house edge (2%)
    const result = Math.random() < (0.5 - HOUSE_EDGE/2) ? 'heads' : 'tails';
    const win = (result === choice);
    const payoutLamports = win ? BigInt(Math.floor(Number(expected_lamports) * (2 - HOUSE_EDGE))) : 0n;

    // Get user info for messaging
    let displayName = `User ${user_id}`;
    try {
        const chatMember = await bot.getChatMember(chat_id, user_id);
        displayName = chatMember.user.username ? 
                     `@${chatMember.user.username}` : 
                     chatMember.user.first_name;
    } catch (e) {
        console.warn(`Couldn't get username for user ${user_id}:`, e.message);
    }

    if (win) {
        const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
        console.log(`Bet ${betId}: ${displayName} WON ${payoutSOL} SOL`);

        const winnerAddress = await getLinkedWallet(user_id);
        if (!winnerAddress) {
            await bot.sendMessage(chat_id,
                `🎉 ${displayName}, you won but no wallet linked!\n` +
                `Result: *${result}*\n` +
                `Please place another bet to link your wallet.`,
                { parse_mode: 'Markdown' }
            );
            await updateBetStatus(betId, 'error_payout_no_wallet');
            return;
        }

        try {
            // Notify user first
            await bot.sendMessage(chat_id,
                `🎉 ${displayName} won ${payoutSOL.toFixed(6)} SOL!\n` +
                `Result: *${result}*\n\n` +
                `💸 Processing payout...`,
                { parse_mode: 'Markdown' }
            );

            // Send payout
            const sendResult = await sendSol(
                connection,
                process.env.BOT_PRIVATE_KEY,
                winnerAddress,
                payoutLamports
            );

            if (sendResult.success) {
                await bot.sendMessage(chat_id,
                    `💰 Payout successful!\n` +
                    `TX: \`${sendResult.signature}\``,
                    { parse_mode: 'Markdown' }
                );
                await recordPayout(betId, 'completed_win_paid', sendResult.signature);
            } else {
                await bot.sendMessage(chat_id,
                    `⚠️ Payout failed: ${sendResult.error}\n` +
                    `Please contact support with Bet ID: ${betId}`,
                    { parse_mode: 'Markdown' }
                );
                await updateBetStatus(betId, 'completed_win_payout_failed');
            }
        } catch (e) {
            console.error(`Payout error for bet ${betId}:`, e);
            await bot.sendMessage(chat_id,
                `⚠️ Payout failed due to technical error\n` +
                `Please contact support with Bet ID: ${betId}`,
                { parse_mode: 'Markdown' }
            );
            await updateBetStatus(betId, 'error_payout_exception');
        }
    } else {
        await bot.sendMessage(chat_id,
            `❌ ${displayName}, you lost!\n` +
            `You guessed *${choice}* but got *${result}*`,
            { parse_mode: 'Markdown' }
        );
        await updateBetStatus(betId, 'completed_loss');
    }
}

async function handleRaceGame(bet) {
    const { id: betId, user_id, chat_id, bet_details, expected_lamports } = bet;
    const horseName = bet_details.horse;
    const odds = bet_details.odds;

    // Race horses data
    const horses = [
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
        { name: 'Silver', emoji: '💎', odds: 15.0, winProbability: 0.01 }
    ];

    // Determine winner
    let winningHorse;
    const randomNumber = Math.random();
    let cumulativeProbability = 0;
    
    for (const horse of horses) {
        cumulativeProbability += horse.winProbability;
        if (randomNumber <= cumulativeProbability) {
            winningHorse = horse;
            break;
        }
    }
    winningHorse = winningHorse || horses[horses.length - 1];

    // Race commentary
    try {
        await bot.sendMessage(chat_id, `🏇 Race ${betId} starting! You bet on ${horseName}!`, 
            { parse_mode: 'Markdown' });
        await new Promise(resolve => setTimeout(resolve, 2000));
        await bot.sendMessage(chat_id, "And they're off!");
        await new Promise(resolve => setTimeout(resolve, 3000));
        await bot.sendMessage(chat_id,
            `🏁 Winner: ${winningHorse.emoji} *${winningHorse.name}*! 🏁`,
            { parse_mode: 'Markdown' }
        );
    } catch (e) {
        console.error(`Error sending race updates for bet ${betId}:`, e);
    }

    // Determine result
    const win = (horseName.toLowerCase() === winningHorse.name.toLowerCase());
    const payoutLamports = win ? 
        BigInt(Math.floor(Number(expected_lamports) * odds * (1 - HOUSE_EDGE))) : 
        0n;

    // Get user info
    let displayName = `User ${user_id}`;
    try {
        const chatMember = await bot.getChatMember(chat_id, user_id);
        displayName = chatMember.user.username ? 
                     `@${chatMember.user.username}` : 
                     chatMember.user.first_name;
    } catch (e) {
        console.warn(`Couldn't get username for user ${user_id}:`, e.message);
    }

    if (win) {
        const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
        console.log(`Bet ${betId}: ${displayName} WON ${payoutSOL} SOL`);

        const winnerAddress = await getLinkedWallet(user_id);
        if (!winnerAddress) {
            await bot.sendMessage(chat_id,
                `🎉 ${displayName}, your horse won but no wallet linked!\n` +
                `Payout: ${payoutSOL.toFixed(6)} SOL\n` +
                `Please place another bet to link your wallet.`,
                { parse_mode: 'Markdown' }
            );
            await updateBetStatus(betId, 'error_payout_no_wallet');
            return;
        }

        try {
            // Notify user first
            await bot.sendMessage(chat_id,
                `🎉 ${displayName}, your horse *${horseName}* won!\n` +
                `Payout: ${payoutSOL.toFixed(6)} SOL\n\n` +
                `💸 Processing payout...`,
                { parse_mode: 'Markdown' }
            );

            // Send payout
            const sendResult = await sendSol(
                connection,
                process.env.RACE_BOT_PRIVATE_KEY,
                winnerAddress,
                payoutLamports
            );

            if (sendResult.success) {
                await bot.sendMessage(chat_id,
                    `💰 Payout successful!\n` +
                    `TX: \`${sendResult.signature}\``,
                    { parse_mode: 'Markdown' }
                );
                await recordPayout(betId, 'completed_win_paid', sendResult.signature);
            } else {
                await bot.sendMessage(chat_id,
                    `⚠️ Payout failed: ${sendResult.error}\n` +
                    `Please contact support with Bet ID: ${betId}`,
                    { parse_mode: 'Markdown' }
                );
                await updateBetStatus(betId, 'completed_win_payout_failed');
            }
        } catch (e) {
            console.error(`Payout error for bet ${betId}:`, e);
            await bot.sendMessage(chat_id,
                `⚠️ Payout failed due to technical error\n` +
                `Please contact support with Bet ID: ${betId}`,
                { parse_mode: 'Markdown' }
            );
            await updateBetStatus(betId, 'error_payout_exception');
        }
    } else {
        await bot.sendMessage(chat_id,
            `❌ ${displayName}, your horse *${horseName}* lost!\n` +
            `Winner: ${winningHorse.emoji} *${winningHorse.name}*`,
            { parse_mode: 'Markdown' }
        );
        await updateBetStatus(betId, 'completed_loss');
    }
}
// --- Bot Command Handlers ---
bot.on('polling_error', (error) => {
    console.error(`Polling error: ${error.code} - ${error.message}`);
    if (error.code === 'ETELEGRAM' && error.message.includes('409 Conflict')) {
        console.error("FATAL: Another bot instance is running!");
        process.exit(1);
    }
});

bot.on('error', (error) => {
    console.error('Bot Error:', error);
});

bot.onText(/\/start$/, async (msg) => {
    try {
        const chatId = msg.chat.id;
        await bot.sendAnimation(chatId, 'https://i.ibb.co/9vDo58q/banner.gif', {
            caption: `🎰 *Solana Gambles*\n\n` +
                     `Use /coinflip or /race to start\n` +
                     `/wallet - View linked wallet\n` +
                     `/help - Show help`,
            parse_mode: 'Markdown'
        });
    } catch (error) {
        console.error("Start command error:", error);
        if (msg?.chat?.id) {
            await bot.sendMessage(msg.chat.id, "Welcome! Use /coinflip or /race to start.");
        }
    }
});

bot.onText(/\/coinflip$/, async (msg) => {
    try {
        await bot.sendMessage(msg.chat.id,
            `🪙 *Coinflip Game*\n\n` +
            `\`/bet amount heads\` - Bet on heads\n` +
            `\`/bet amount tails\` - Bet on tails\n\n` +
            `Min: ${MIN_BET} SOL | Max: ${MAX_BET} SOL\n` +
            `House edge: ${(HOUSE_EDGE * 100).toFixed(1)}%`,
            { parse_mode: 'Markdown' }
        );
    } catch (error) {
        console.error("Coinflip command error:", error);
        if (msg?.chat?.id) {
            await bot.sendMessage(msg.chat.id, "Error showing coinflip info.");
        }
    }
});

bot.onText(/\/wallet$/, async (msg) => {
    try {
        const userId = String(msg.from.id);
        const walletAddress = await getLinkedWallet(userId);
        
        if (walletAddress) {
            await bot.sendMessage(msg.chat.id,
                `💰 Your linked wallet:\n\`${walletAddress}\``,
                { parse_mode: 'Markdown' }
            );
        } else {
            await bot.sendMessage(msg.chat.id,
                `⚠️ No wallet linked yet.\n` +
                `Place a bet to automatically link your wallet.`
            );
        }
    } catch (error) {
        console.error("Wallet command error:", error);
        if (msg?.chat?.id) {
            await bot.sendMessage(msg.chat.id, "Error fetching wallet info.");
        }
    }
});

bot.onText(/\/bet (\d+\.?\d*) (heads|tails)/i, async (msg, match) => {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    
    try {
        // Validate cooldown
        if (confirmCooldown.has(userId)) {
            const lastTime = confirmCooldown.get(userId);
            if (Date.now() - lastTime < cooldownInterval) {
                await bot.sendMessage(chatId, "⚠️ Please wait a few seconds...");
                return;
            }
        }
        confirmCooldown.set(userId, Date.now());

        // Validate bet amount
        const betAmount = parseFloat(match[1]);
        if (isNaN(betAmount) || betAmount < MIN_BET || betAmount > MAX_BET) {
            await bot.sendMessage(chatId,
                `⚠️ Bet must be between ${MIN_BET}-${MAX_BET} SOL`
            );
            return;
        }

        const userChoice = match[2].toLowerCase();
        const memoId = generateMemoId('CF');
        const expectedLamports = BigInt(Math.round(betAmount * LAMPORTS_PER_SOL));
        const expiresAt = new Date(Date.now() + PAYMENT_EXPIRY_MINUTES * 60 * 1000);

        // Save to database
        const saveResult = await savePendingBet(
            userId, chatId, 'coinflip', 
            { choice: userChoice }, 
            expectedLamports, memoId, expiresAt
        );

        if (!saveResult.success) {
            throw new Error(saveResult.error || "Failed to save bet");
        }

        // Send payment instructions
        await bot.sendMessage(chatId,
            `✅ Coinflip bet registered!\n\n` +
            `💸 Send *exactly ${betAmount.toFixed(6)} SOL* to:\n` +
            `\`${process.env.MAIN_WALLET_ADDRESS}\`\n\n` +
            `*MEMO:* \`${memoId}\`\n\n` +
            `Expires in ${PAYMENT_EXPIRY_MINUTES} minutes.`,
            { parse_mode: 'Markdown' }
        );

    } catch (error) {
        console.error(`Bet error for user ${userId}:`, error);
        if (msg?.chat?.id) {
            await bot.sendMessage(chatId,
                `⚠️ Error: ${error.message.includes('collision') ? 
                 'Try again' : 'Please try again later'}`
            );
        }
    }
});

bot.onText(/\/race$/, async (msg) => {
    try {
        const horses = [
            { name: 'Yellow', emoji: '🟡', odds: 1.1 },
            { name: 'Orange', emoji: '🟠', odds: 2.0 },
            { name: 'Blue', emoji: '🔵', odds: 3.0 },
            { name: 'Cyan', emoji: '🔷', odds: 4.0 },
            { name: 'White', emoji: '⚪', odds: 5.0 },
            { name: 'Red', emoji: '🔴', odds: 6.0 },
            { name: 'Black', emoji: '⚫', odds: 7.0 },
            { name: 'Pink', emoji: '🌸', odds: 8.0 },
            { name: 'Purple', emoji: '🟣', odds: 9.0 },
            { name: 'Green', emoji: '🟢', odds: 10.0 },
            { name: 'Silver', emoji: '💎', odds: 15.0 }
        ];

        let raceMessage = `🏇 *Race Game* 🏇\n\n`;
        horses.forEach(horse => {
            raceMessage += `${horse.emoji} *${horse.name}* (${horse.odds.toFixed(1)}x)\n`;
        });

        raceMessage += `\n\`/betrace amount horse_name\`\n` +
                      `Min: ${RACE_MIN_BET} SOL | Max: ${RACE_MAX_BET} SOL\n` +
                      `House edge: ${(HOUSE_EDGE * 100).toFixed(1)}%`;

        await bot.sendMessage(msg.chat.id, raceMessage, { parse_mode: 'Markdown' });

    } catch (error) {
        console.error("Race command error:", error);
        if (msg?.chat?.id) {
            await bot.sendMessage(msg.chat.id, "Error showing race info.");
        }
    }
});

bot.onText(/\/betrace (\d+\.?\d*) (\w+)/i, async (msg, match) => {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    
    try {
        // Validate cooldown
        if (confirmCooldown.has(userId)) {
            const lastTime = confirmCooldown.get(userId);
            if (Date.now() - lastTime < cooldownInterval) {
                await bot.sendMessage(chatId, "⚠️ Please wait...");
                return;
            }
        }
        confirmCooldown.set(userId, Date.now());

        // Validate bet amount
        const betAmount = parseFloat(match[1]);
        if (isNaN(betAmount) || betAmount < RACE_MIN_BET || betAmount > RACE_MAX_BET) {
            await bot.sendMessage(chatId,
                `⚠️ Bet must be ${RACE_MIN_BET}-${RACE_MAX_BET} SOL`
            );
            return;
        }

        // Validate horse selection
        const chosenHorse = match[2].toLowerCase();
        const horse = [
            { name: 'Yellow', emoji: '🟡', odds: 1.1 },
            { name: 'Orange', emoji: '🟠', odds: 2.0 },
            { name: 'Blue', emoji: '🔵', odds: 3.0 },
            { name: 'Cyan', emoji: '🔷', odds: 4.0 },
            { name: 'White', emoji: '⚪', odds: 5.0 },
            { name: 'Red', emoji: '🔴', odds: 6.0 },
            { name: 'Black', emoji: '⚫', odds: 7.0 },
            { name: 'Pink', emoji: '🌸', odds: 8.0 },
            { name: 'Purple', emoji: '🟣', odds: 9.0 },
            { name: 'Green', emoji: '🟢', odds: 10.0 },
            { name: 'Silver', emoji: '💎', odds: 15.0 }
        ].find(h => h.name.toLowerCase() === chosenHorse);

        if (!horse) {
            await bot.sendMessage(chatId, "⚠️ Invalid horse name");
            return;
        }

        const memoId = generateMemoId('RA');
        const expectedLamports = BigInt(Math.round(betAmount * LAMPORTS_PER_SOL));
        const expiresAt = new Date(Date.now() + PAYMENT_EXPIRY_MINUTES * 60 * 1000);

        // Save to database
        const saveResult = await savePendingBet(
            userId, chatId, 'race',
            { horse: horse.name, odds: horse.odds },
            expectedLamports, memoId, expiresAt
        );

        if (!saveResult.success) {
            throw new Error(saveResult.error || "Failed to save bet");
        }

        // Send payment instructions
        await bot.sendMessage(chatId,
            `✅ Bet on ${horse.emoji} *${horse.name}* registered!\n\n` +
            `💸 Send *exactly ${betAmount.toFixed(6)} SOL* to:\n` +
            `\`${process.env.RACE_WALLET_ADDRESS}\`\n\n` +
            `*MEMO:* \`${memoId}\`\n\n` +
            `Expires in ${PAYMENT_EXPIRY_MINUTES} minutes.`,
            { parse_mode: 'Markdown' }
        );

    } catch (error) {
        console.error(`Betrace error for user ${userId}:`, error);
        if (msg?.chat?.id) {
            await bot.sendMessage(chatId,
                `⚠️ Error: ${error.message.includes('collision') ? 
                 'Try again' : 'Please try again later'}`
            );
        }
    }
});

// --- Server Startup & Shutdown ---
async function startServer() {
    try {
        await initializeDatabase();
        const PORT = process.env.PORT || 3000;

        // Webhook setup for Railway
        if (process.env.RAILWAY_ENVIRONMENT && process.env.RAILWAY_PUBLIC_DOMAIN) {
            const webhookUrl = `https://${process.env.RAILWAY_PUBLIC_DOMAIN}${webhookPath}`;
            
            try {
                await bot.setWebHook(webhookUrl);
                console.log(`✅ Webhook set to: ${webhookUrl}`);
                
                const webhookInfo = await bot.getWebHookInfo();
                if (webhookInfo.url !== webhookUrl) {
                    console.error("❌ Webhook URL mismatch");
                }
            } catch (webhookError) {
                console.error("❌ Webhook setup failed:", webhookError.message);
            }
        }

        // Start server
        app.listen(PORT, "0.0.0.0", () => {
            console.log(`✅ Server running on port ${PORT}`);
            
            // Start payment monitor
            monitorInterval = setInterval(monitorPayments, monitorIntervalSeconds * 1000);
            setTimeout(monitorPayments, 5000); // Initial run
            
            // Start polling if not in production
            if (!process.env.RAILWAY_ENVIRONMENT) {
                bot.startPolling().then(() => {
                    console.log("🔵 Bot polling started");
                }).catch(pollError => {
                    console.error("Polling error:", pollError);
                });
            }
        });

    } catch (error) {
        console.error("💥 Failed to start:", error);
        process.exit(1);
    }
}

// Graceful shutdown handler
const shutdown = (signal) => {
    console.log(`\n${signal} received, shutting down...`);
    
    clearInterval(monitorInterval);
    
    pool.end().then(() => {
        console.log("✅ Database pool closed");
        process.exit(0);
    }).catch(err => {
        console.error("❌ Pool close error:", err);
        process.exit(1);
    });
    
    setTimeout(() => {
        console.warn("⚠️ Forcing shutdown");
        process.exit(1);
    }, 5000);
};

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

// Start the server
startServer();

console.log("🚀 Bot initialization complete");
