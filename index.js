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
import { randomBytes } from 'crypto';
import PQueue from 'p-queue';
import RateLimitedConnection from './lib/solana-connection.js';

// --- Enhanced Environment Variable Checks ---
const REQUIRED_ENV_VARS = [
    'BOT_TOKEN',
    'DATABASE_URL',
    'BOT_PRIVATE_KEY',
    'RACE_BOT_PRIVATE_KEY',
    'MAIN_WALLET_ADDRESS',
    'RACE_WALLET_ADDRESS',
    'RPC_URL',
    'FEE_MARGIN'
];

// Check for Railway-specific variables
if (process.env.RAILWAY_ENVIRONMENT) {
    REQUIRED_ENV_VARS.push('RAILWAY_PUBLIC_DOMAIN');
}

// Validate environment variables
let missingVars = false;
REQUIRED_ENV_VARS.forEach((key) => {
    if (!process.env[key]) {
        if (key === 'RAILWAY_PUBLIC_DOMAIN' && !process.env.RAILWAY_ENVIRONMENT) return;
        console.error(`❌ Environment variable ${key} is missing.`);
        missingVars = true;
    }
});

if (missingVars) {
    console.error("Please set all required environment variables. Exiting.");
    process.exit(1);
}

// Set default fee margin if not specified
if (!process.env.FEE_MARGIN) {
    process.env.FEE_MARGIN = '5000';
}

// --- Initialize Scalable Components ---
const app = express();

// 1. Enhanced Solana Connection with Rate Limiting
console.log("Initializing scalable Solana connection...");
const solanaConnection = new RateLimitedConnection(process.env.RPC_URL, {
    maxConcurrent: 3,
    retryBaseDelay: 300,
    commitment: 'confirmed',
    httpHeaders: {
        'Content-Type': 'application/json',
        'solana-client': 'SolanaGamblesBot/1.0'
    }
});
console.log("✅ Scalable Solana connection initialized");

// 2. Message Processing Queue
const messageQueue = new PQueue({
    concurrency: 5,
    timeout: 10000
});

// 3. Enhanced PostgreSQL Pool
console.log("Setting up optimized PostgreSQL Pool...");
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    max: 15,
    min: 5,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
    ssl: process.env.NODE_ENV === 'production' ? { 
        rejectUnauthorized: false 
    } : false
});
console.log("✅ PostgreSQL Pool created with optimized settings");

// 4. Performance Monitor
const performanceMonitor = {
    requests: 0,
    errors: 0,
    startTime: Date.now(),
    logRequest(success) {
        this.requests++;
        if (!success) this.errors++;
        
        if (this.requests % 50 === 0) {
            const uptime = (Date.now() - this.startTime) / 1000;
            console.log(`
                📊 Performance Metrics:
                - Uptime: ${uptime.toFixed(0)}s
                - Total Requests: ${this.requests}
                - Error Rate: ${(this.errors/this.requests*100).toFixed(1)}%
            `);
        }
    }
};

// --- Database Initialization (Updated) ---
async function initializeDatabase() {
    console.log("Initializing Database with scalable schema...");
    let client;
    try {
        client = await pool.connect();
        
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
                processed_at TIMESTAMPTZ,
                fees_paid BIGINT,
                priority INT DEFAULT 0
            );
        `);
        
        // Add this to ensure the column exists if table already existed
        await client.query(`
            ALTER TABLE bets 
            ADD COLUMN IF NOT EXISTS priority INT DEFAULT 0;
        `);
        
        await client.query(`
            CREATE TABLE IF NOT EXISTS wallets (
                user_id TEXT PRIMARY KEY,
                wallet_address TEXT NOT NULL,
                linked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_used_at TIMESTAMPTZ
            );
        `);
        
        // Add indexes for performance
        await client.query(`
            CREATE INDEX IF NOT EXISTS idx_bets_status ON bets(status);
            CREATE INDEX IF NOT EXISTS idx_bets_user_id ON bets(user_id);
            CREATE INDEX IF NOT EXISTS idx_bets_expires_at ON bets(expires_at);
            CREATE INDEX IF NOT EXISTS idx_bets_priority ON bets(priority);
        `);
        
        console.log("✅ Database initialized with scalable schema");
    } catch (err) {
        console.error("❌ Database initialization error:", err);
        throw err;
    } finally {
        if (client) client.release();
    }
}

// --- Telegram Bot Initialization with Queue ---
console.log("Initializing Telegram Bot with queue system...");
const bot = new TelegramBot(process.env.BOT_TOKEN, {
    polling: false,
    request: {
        timeout: 10000,
        agentOptions: {
            keepAlive: true,
            timeout: 60000
        }
    }
});

// Wrap bot handlers in queue
bot.on('message', (msg) => {
    messageQueue.add(() => handleMessage(msg))
        .catch(err => {
            console.error("Message processing error:", err);
            performanceMonitor.logRequest(false);
        });
});

console.log("✅ Telegram Bot initialized with queue system");

// --- Express Setup with Enhanced Monitoring ---
app.use(express.json({
    limit: '10kb',
    verify: (req, res, buf) => {
        req.rawBody = buf;
    }
}));

// Health check with monitoring
app.get('/', (req, res) => {
    performanceMonitor.logRequest(true);
    res.status(200).json({
        status: 'ok',
        timestamp: new Date().toISOString(),
        version: '1.0.0',
        queueStats: {
            pending: messageQueue.size,
            active: messageQueue.pending
        }
    });
});

// Webhook handler with queue
const webhookPath = `/bot${process.env.BOT_TOKEN}`;
app.post(webhookPath, (req, res) => {
    messageQueue.add(() => {
        try {
            if (!req.body || typeof req.body !== 'object') {
                performanceMonitor.logRequest(false);
                return res.status(400).send('Invalid request');
            }
            
            bot.processUpdate(req.body);
            performanceMonitor.logRequest(true);
            res.sendStatus(200);
        } catch (error) {
            console.error("Webhook processing error:", error);
            performanceMonitor.logRequest(false);
            res.status(500).json({
                error: 'Internal server error',
                details: error.message
            });
        }
    });
});

// --- State Management with Enhanced Caching ---
const confirmCooldown = new Map();
const cooldownInterval = 3000;

const walletCache = new Map();
const CACHE_TTL = 300000;

const processedSignaturesThisSession = new Set();
const MAX_PROCESSED_SIGNATURES = 1000;

// --- Constants with Scalability in Mind ---
const GAME_CONFIG = {
    coinflip: {
        minBet: 0.01,
        maxBet: 1.0,
        expiryMinutes: 15,
        houseEdge: 0.02
    },
    race: {
        minBet: 0.01,
        maxBet: 1.0,
        expiryMinutes: 15,
        houseEdge: 0.02
    }
};

const FEE_BUFFER = BigInt(process.env.FEE_MARGIN);
const PRIORITY_FEE_RATE = 0.0001;

// --- Helper Functions with Rate Limiting ---
function generateMemoId(prefix = 'BET') {
    const validPrefixes = ['BET', 'CF', 'RA'];
    if (!validPrefixes.includes(prefix)) {
        throw new Error('Invalid memo prefix');
    }
    return `${prefix}-${randomBytes(6).toString('hex').toUpperCase()}`;
}

function validateMemoFormat(memo) {
    if (!memo) return false;
    const parts = memo.split('-');
    return parts.length === 2 && 
           ['BET', 'CF', 'RA'].includes(parts[0]) && 
           /^[A-F0-9]{12}$/.test(parts[1]);
}

// --- Enhanced Database Operations ---
async function savePendingBet(userId, chatId, gameType, details, lamports, memoId, expiresAt, priority = 0) {
    if (!validateMemoFormat(memoId)) {
        throw new Error('Invalid memo ID format');
    }

    const query = `
        INSERT INTO bets (
            user_id, chat_id, game_type, bet_details, 
            expected_lamports, memo_id, status, expires_at, fees_paid, priority
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
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
        expiresAt,
        FEE_BUFFER,
        priority
    ];
    
    try {
        const res = await pool.query(query, values);
        return { success: true, id: res.rows[0].id };
    } catch (err) {
        console.error('DB Error saving bet:', err);
        return { 
            success: false, 
            error: err.code === '23505' ? 'Memo ID collision' : err.message 
        };
    }
}
// --- Enhanced Payment Monitoring System ---
let isMonitorRunning = false;
let monitorIntervalSeconds = 30;
let monitorInterval = null;

class PaymentProcessor {
    constructor() {
        this.highPriorityQueue = new PQueue({
            concurrency: 3,
            priority: (job) => job.priority
        });
        this.normalQueue = new PQueue({
            concurrency: 2,
            autoStart: false
        });
        this.activeProcesses = new Set();
    }

    async addPaymentJob(job) {
        if (job.priority > 0) {
            await this.highPriorityQueue.add(job, { priority: job.priority });
        } else {
            await this.normalQueue.add(() => this.processJob(job));
        }
        this.balanceQueues();
    }

    balanceQueues() {
        // Dynamically adjust concurrency based on load
        const highPriorityLoad = this.highPriorityQueue.size;
        this.normalQueue.concurrency = Math.max(1, 5 - Math.floor(highPriorityLoad / 2));
    }

    async processJob(job) {
        if (this.activeProcesses.has(job.signature)) return;
        this.activeProcesses.add(job.signature);

        try {
            const result = await this._processPayment(
                job.signature, 
                job.walletType,
                job.retries || 0
            );
            performanceMonitor.logRequest(true);
            return result;
        } catch (error) {
            performanceMonitor.logRequest(false);
            
            if (job.retries < 3 && isRetryableError(error)) {
                job.retries = (job.retries || 0) + 1;
                await this.addPaymentJob(job);
            } else {
                console.error(`Payment failed after retries: ${job.signature}`, error);
            }
        } finally {
            this.activeProcesses.delete(job.signature);
        }
    }

    async _processPayment(signature, walletType, attempt = 0) {
        if (processedSignaturesThisSession.has(signature)) {
            return { processed: false, reason: 'already_processed' };
        }

        // Check if already processed in DB
        const checkQuery = `
            SELECT id FROM bets 
            WHERE paid_tx_signature = $1 
            LIMIT 1;
        `;
        
        const processed = await pool.query(checkQuery, [signature]);
        if (processed.rowCount > 0) {
            processedSignaturesThisSession.add(signature);
            return { processed: false, reason: 'exists_in_db' };
        }

        // Fetch transaction with retry logic
        const tx = await solanaConnection.executeWithRetry(
            'getParsedTransaction',
            [signature, { maxSupportedTransactionVersion: 0 }],
            attempt
        );

        if (!tx || tx.meta?.err) {
            processedSignaturesThisSession.add(signature);
            return { processed: false, reason: 'invalid_tx' };
        }

        // Process memo and bet
        const memo = findMemoInTx(tx);
        if (!memo) {
            processedSignaturesThisSession.add(signature);
            return { processed: false, reason: 'no_valid_memo' };
        }

        console.log(`Processing payment with memo: ${memo}`);

        const bet = await findBetByMemo(memo);
        if (!bet || bet.status !== 'awaiting_payment') {
            processedSignaturesThisSession.add(signature);
            return { processed: false, reason: 'no_matching_bet' };
        }

        // Process transaction amounts
        const { transferAmount, payerAddress } = analyzeTransactionAmounts(tx, walletType);

        // Validate amount with tolerance
        const expectedAmount = BigInt(bet.expected_lamports);
        const tolerance = BigInt(5000);
        
        if (transferAmount < (expectedAmount - tolerance) || 
            transferAmount > (expectedAmount + tolerance)) {
            await updateBetStatus(bet.id, 'error_payment_mismatch');
            return { processed: false, reason: 'amount_mismatch' };
        }

        // Validate timestamp
        const txTime = tx.blockTime ? new Date(tx.blockTime * 1000) : new Date(0);
        if (txTime > new Date(bet.expires_at)) {
            await updateBetStatus(bet.id, 'error_payment_expired');
            return { processed: false, reason: 'expired' };
        }

        // Mark as paid in DB
        await markBetPaid(bet.id, signature);

        // Link wallet if possible
        if (payerAddress) {
            await linkUserWallet(bet.user_id, payerAddress);
        } else {
            const payerKey = getPayerFromTransaction(tx);
            if (payerKey) await linkUserWallet(bet.user_id, payerKey.toBase58());
        }

        // Add to processed set
        processedSignaturesThisSession.add(signature);

        // Queue for game processing with priority
        await this.addPaymentJob({
            type: 'process_bet',
            betId: bet.id,
            priority: 1, // Higher than normal monitoring
            signature
        });

        return { processed: true };
    }
}

const paymentProcessor = new PaymentProcessor();

// --- Optimized Transaction Analysis ---
function analyzeTransactionAmounts(tx, walletType) {
    let transferAmount = 0n;
    let payerAddress = null;
    const targetAddress = walletType === 'coinflip' 
        ? process.env.MAIN_WALLET_ADDRESS
        : process.env.RACE_WALLET_ADDRESS;

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
                if (transferInfo.destination === targetAddress) {
                    transferAmount += BigInt(transferInfo.lamports || transferInfo.amount || 0);
                    if (!payerAddress) payerAddress = transferInfo.source;
                }
            }
        }
    }

    return { transferAmount, payerAddress };
}

// --- Enhanced Monitoring Loop ---
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
            { 
                address: process.env.MAIN_WALLET_ADDRESS, 
                type: 'coinflip',
                priority: 0 // Normal priority
            },
            { 
                address: process.env.RACE_WALLET_ADDRESS, 
                type: 'race',
                priority: 0
            },
        ];

        // Process each wallet with priority awareness
        for (const wallet of walletsToMonitor) {
            try {
                // New version (using built-in rate limiting)
const signatures = await solanaConnection.getSignaturesForAddress(
    new PublicKey(wallet.address),
    { limit: 10 }
);
                if (!signatures || signatures.length === 0) continue;

                // Process signatures with priority queuing
                for (const sig of signatures) {
                    await paymentProcessor.addPaymentJob({
                        type: 'monitor_payment',
                        signature: sig.signature,
                        walletType: wallet.type,
                        priority: wallet.priority,
                        retries: 0
                    });
                    processedCount++;
                }
            } catch (error) {
                console.error(`Error monitoring wallet ${wallet.address}:`, error);
                performanceMonitor.logRequest(false);
            }
        }
    } catch (error) {
        console.error("Monitor Error:", error);
        performanceMonitor.logRequest(false);
    } finally {
        isMonitorRunning = false;
        console.log(`Monitor processed ${processedCount} txs in ${Date.now() - startTime}ms`);
        
        // Clean up if cache is too large
        if (processedSignaturesThisSession.size > MAX_PROCESSED_SIGNATURES) {
            console.log('Clearing processed signatures cache');
            processedSignaturesThisSession.clear();
        }
        
        // Adjust monitoring interval
        adjustMonitorInterval(processedCount);
    }
}

function adjustMonitorInterval(processedCount) {
    const newInterval = processedCount > 10 ? Math.max(10, monitorIntervalSeconds - 5) :
                    processedCount === 0 ? Math.min(120, monitorIntervalSeconds + 10) :
                    monitorIntervalSeconds;
    
    if (newInterval !== monitorIntervalSeconds && monitorInterval) {
        monitorIntervalSeconds = newInterval;
        clearInterval(monitorInterval);
        monitorInterval = setInterval(monitorPayments, monitorIntervalSeconds * 1000);
        console.log(`Adjusted monitor interval to ${monitorIntervalSeconds}s`);
    }
}

// --- Database Operations with Priority Support ---
async function findBetByMemo(memoId) {
    if (!validateMemoFormat(memoId)) return undefined;

    const query = `
        SELECT * FROM bets 
        WHERE memo_id = $1 AND status = 'awaiting_payment'
        ORDER BY priority DESC, created_at ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1;
    `;
    
    try {
        const res = await pool.query(query, [memoId]);
        return res.rows[0];
    } catch (err) {
        console.error(`DB Error finding bet:`, err);
        return undefined;
    }
}

async function markBetPaid(betId, signature) {
    const query = `
        UPDATE bets
        SET status = 'payment_verified',
            paid_tx_signature = $1,
            processed_at = NOW()
        WHERE id = $2
        RETURNING *;
    `;
    
    try {
        const res = await pool.query(query, [signature, betId]);
        return { success: true, bet: res.rows[0] };
    } catch (err) {
        console.error(`DB Error marking bet paid:`, err);
        return { success: false, error: err.message };
    }
}

async function linkUserWallet(userId, walletAddress) {
    const cacheKey = `wallet-${userId}`;
    const query = `
        INSERT INTO wallets (user_id, wallet_address)
        VALUES ($1, $2)
        ON CONFLICT (user_id) 
        DO UPDATE SET 
            wallet_address = EXCLUDED.wallet_address,
            last_used_at = NOW()
        RETURNING *;
    `;
    
    try {
        const res = await pool.query(query, [String(userId), walletAddress]);
        
        // Update cache
        if (res.rows[0]) {
            walletCache.set(cacheKey, {
                wallet: walletAddress,
                timestamp: Date.now()
            });
        }
        
        return { success: true };
    } catch (err) {
        console.error(`DB Error linking wallet:`, err);
        return { success: false, error: err.message };
    }
}
// --- Enhanced Transaction Processing ---
async function sendSol(recipientPublicKey, amountLamports, gameType) {
    const privateKey = gameType === 'coinflip' 
        ? process.env.BOT_PRIVATE_KEY 
        : process.env.RACE_BOT_PRIVATE_KEY;
    
    const recipientPubKey = (typeof recipientPublicKey === 'string') 
        ? new PublicKey(recipientPublicKey) 
        : recipientPublicKey;
    
    const amountSOL = Number(amountLamports) / LAMPORTS_PER_SOL;
    const priorityFee = Math.min(
        1000000,
        Math.max(1000, Math.floor(Number(amountLamports) * PRIORITY_FEE_RATE))
    );

    for (let attempt = 1; attempt <= 3; attempt++) {
        try {
            const payerWallet = Keypair.fromSecretKey(bs58.decode(privateKey));
            const latestBlockhash = await solanaConnection.executeWithRetry(
                'getLatestBlockhash',
                [{ commitment: 'confirmed' }]
            );

            const transaction = new Transaction({
                recentBlockhash: latestBlockhash.blockhash,
                feePayer: payerWallet.publicKey
            });

            transaction.add(
                ComputeBudgetProgram.setComputeUnitPrice({ 
                    microLamports: priorityFee 
                })
            );

            const transferAmount = amountLamports > FEE_BUFFER 
                ? amountLamports - FEE_BUFFER 
                : 0n;
                
            if (transferAmount <= 0n) {
                throw new Error('Insufficient amount after fee deduction');
            }
            
            transaction.add(
                SystemProgram.transfer({
                    fromPubkey: payerWallet.publicKey,
                    toPubkey: recipientPubKey,
                    lamports: transferAmount
                })
            );

            const signature = await Promise.race([
                sendAndConfirmTransaction(
                    solanaConnection,
                    transaction,
                    [payerWallet],
                    { 
                        commitment: 'confirmed',
                        skipPreflight: false,
                        maxRetries: 3
                    }
                ),
                new Promise((_, reject) => {
                    setTimeout(() => {
                        reject(new Error('Transaction timeout after 30s'));
                    }, 30000);
                })
            ]);

            console.log(`✅ Sent ${(Number(transferAmount)/LAMPORTS_PER_SOL).toFixed(6)} SOL`);
            return { success: true, signature };
            
        } catch (error) {
            console.error(`Attempt ${attempt}/3 failed:`, error.message);
            
            if (error.message.includes('Invalid param') || 
                error.message.includes('Insufficient funds')) {
                break;
            }
            
            await new Promise(resolve => setTimeout(resolve, 1000 * attempt));
        }
    }
    
    return { 
        success: false, 
        error: `Failed after 3 attempts` 
    };
}

// --- Enhanced Game Processors ---
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
    const config = GAME_CONFIG.coinflip;

    // Determine outcome with house edge
    const result = Math.random() < (0.5 - config.houseEdge/2) ? 'heads' : 'tails';
    const win = (result === choice);
    
    // Calculate payout with fees
    const payoutLamports = win ? calculatePayoutWithFees(expected_lamports, 'coinflip') : 0n;

    // Get user info for messaging
    let displayName = `User ${user_id}`;
    try {
        const chatMember = await bot.getChatMember(chat_id, user_id);
        displayName = chatMember.user.username 
            ? `@${chatMember.user.username}` 
            : chatMember.user.first_name;
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

            // Process payout through queue
            await paymentProcessor.addPaymentJob({
                type: 'payout',
                betId,
                recipient: winnerAddress,
                amount: payoutLamports,
                gameType: 'coinflip',
                priority: 2, // Higher than monitoring
                chatId: chat_id,
                displayName,
                result
            });

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

async function handlePayoutJob(job) {
    const { betId, recipient, amount, gameType, chatId, displayName, result } = job;
    
    try {
        const sendResult = await sendSol(recipient, amount, gameType);

        if (sendResult.success) {
            await bot.sendMessage(chatId,
                `💰 Payout successful!\n` +
                `TX: \`${sendResult.signature}\``,
                { parse_mode: 'Markdown' }
            );
            await recordPayout(betId, 'completed_win_paid', sendResult.signature);
        } else {
            await bot.sendMessage(chatId,
                `⚠️ Payout failed: ${sendResult.error}\n` +
                `Please contact support with Bet ID: ${betId}`,
                { parse_mode: 'Markdown' }
            );
            await updateBetStatus(betId, 'completed_win_payout_failed');
        }
    } catch (error) {
        console.error(`Payout processing failed for bet ${betId}:`, error);
        throw error; // Will trigger retry logic
    }
}

async function handleRaceGame(bet) {
    const { id: betId, user_id, chat_id, bet_details, expected_lamports } = bet;
    const horseName = bet_details.horse;
    const config = GAME_CONFIG.race;

    // Race horses data with probabilities
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
    const payoutLamports = win 
        ? calculatePayoutWithFees(expected_lamports, 'race', bet_details) 
        : 0n;

    // Get user info
    let displayName = `User ${user_id}`;
    try {
        const chatMember = await bot.getChatMember(chat_id, user_id);
        displayName = chatMember.user.username 
            ? `@${chatMember.user.username}` 
            : chatMember.user.first_name;
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

            // Process payout through queue
            await paymentProcessor.addPaymentJob({
                type: 'payout',
                betId,
                recipient: winnerAddress,
                amount: payoutLamports,
                gameType: 'race',
                priority: 2,
                chatId: chat_id,
                displayName,
                horseName,
                winningHorse
            });

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

// --- Enhanced Bot Command Handlers ---
bot.on('polling_error', (error) => {
    console.error(`Polling error: ${error.code} - ${error.message}`);
    if (error.code === 'ETELEGRAM' && error.message.includes('409 Conflict')) {
        console.error("FATAL: Another bot instance is running!");
        process.exit(1);
    }
});

bot.on('error', (error) => {
    console.error('Bot Error:', error);
    performanceMonitor.logRequest(false);
});

// Queue-based command processing
async function handleMessage(msg) {
    if (!msg.text) return;
    
    try {
        // Cooldown check
        if (confirmCooldown.has(msg.from.id)) {
            const lastTime = confirmCooldown.get(msg.from.id);
            if (Date.now() - lastTime < cooldownInterval) {
                await bot.sendMessage(msg.chat.id, "⚠️ Please wait a few seconds...");
                return;
            }
        }
        confirmCooldown.set(msg.from.id, Date.now());

        // Route commands
        if (msg.text.match(/^\/start$/i)) {
            await handleStartCommand(msg);
        } 
        else if (msg.text.match(/^\/coinflip$/i)) {
            await handleCoinflipCommand(msg);
        }
        else if (msg.text.match(/^\/wallet$/i)) {
            await handleWalletCommand(msg);
        }
        else if (msg.text.match(/^\/bet (\d+\.?\d*) (heads|tails)/i)) {
            await handleBetCommand(msg);
        }
        else if (msg.text.match(/^\/race$/i)) {
            await handleRaceCommand(msg);
        }
        else if (msg.text.match(/^\/betrace (\d+\.?\d*) (\w+)/i)) {
            await handleBetRaceCommand(msg);
        }

        performanceMonitor.logRequest(true);
    } catch (error) {
        console.error(`Error processing message: ${msg.text}`, error);
        performanceMonitor.logRequest(false);
        
        if (msg?.chat?.id) {
            await bot.sendMessage(msg.chat.id, 
                "⚠️ An error occurred. Please try again later.");
        }
    }
}

async function handleStartCommand(msg) {
    try {
        await bot.sendAnimation(msg.chat.id, 'https://i.ibb.co/9vDo58q/banner.gif', {
            caption: `🎰 *Solana Gambles*\n\n` +
                     `Use /coinflip or /race to start\n` +
                     `/wallet - View linked wallet\n` +
                     `/help - Show help`,
            parse_mode: 'Markdown'
        });
    } catch (error) {
        console.error("Start command error:", error);
        await bot.sendMessage(msg.chat.id, "Welcome! Use /coinflip or /race to start.");
    }
}

async function handleCoinflipCommand(msg) {
    const config = GAME_CONFIG.coinflip;
    await bot.sendMessage(msg.chat.id,
        `🪙 *Coinflip Game*\n\n` +
        `\`/bet amount heads\` - Bet on heads\n` +
        `\`/bet amount tails\` - Bet on tails\n\n` +
        `Min: ${config.minBet} SOL | Max: ${config.maxBet} SOL\n` +
        `House edge: ${(config.houseEdge * 100).toFixed(1)}%`,
        { parse_mode: 'Markdown' }
    );
}

async function handleWalletCommand(msg) {
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
}

async function handleBetCommand(msg) {
    const match = msg.text.match(/^\/bet (\d+\.?\d*) (heads|tails)/i);
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const config = GAME_CONFIG.coinflip;

    // Validate bet amount
    const betAmount = parseFloat(match[1]);
    if (isNaN(betAmount) || betAmount < config.minBet || betAmount > config.maxBet) {
        await bot.sendMessage(chatId,
            `⚠️ Bet must be between ${config.minBet}-${config.maxBet} SOL`
        );
        return;
    }

    const userChoice = match[2].toLowerCase();
    const memoId = generateMemoId('CF');
    const expectedLamports = BigInt(Math.round(betAmount * LAMPORTS_PER_SOL));
    const expiresAt = new Date(Date.now() + config.expiryMinutes * 60 * 1000);

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
        `Expires in ${config.expiryMinutes} minutes.`,
        { parse_mode: 'Markdown' }
    );
}

async function handleRaceCommand(msg) {
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

    const config = GAME_CONFIG.race;
    raceMessage += `\n\`/betrace amount horse_name\`\n` +
                  `Min: ${config.minBet} SOL | Max: ${config.maxBet} SOL\n` +
                  `House edge: ${(config.houseEdge * 100).toFixed(1)}%`;

    await bot.sendMessage(msg.chat.id, raceMessage, { parse_mode: 'Markdown' });
}

async function handleBetRaceCommand(msg) {
    const match = msg.text.match(/^\/betrace (\d+\.?\d*) (\w+)/i);
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const config = GAME_CONFIG.race;

    // Validate bet amount
    const betAmount = parseFloat(match[1]);
    if (isNaN(betAmount) || betAmount < config.minBet || betAmount > config.maxBet) {
        await bot.sendMessage(chatId,
            `⚠️ Bet must be ${config.minBet}-${config.maxBet} SOL`
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
    const expiresAt = new Date(Date.now() + config.expiryMinutes * 60 * 1000);

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
        `Expires in ${config.expiryMinutes} minutes.`,
        { parse_mode: 'Markdown' }
    );
}
// --- Server Startup & Shutdown ---
async function startServer() {
    try {
        await initializeDatabase();
        const PORT = process.env.PORT || 3000;

        // Webhook setup for Railway
        if (process.env.RAILWAY_ENVIRONMENT && process.env.RAILWAY_PUBLIC_DOMAIN) {
            const webhookUrl = `https://${process.env.RAILWAY_PUBLIC_DOMAIN}${webhookPath}`;
            
            let attempts = 0;
            while (attempts < 3) {
                try {
                    await bot.setWebHook(webhookUrl);
                    console.log(`✅ Webhook set to: ${webhookUrl}`);
                    break;
                } catch (webhookError) {
                    attempts++;
                    console.error(`Webhook setup attempt ${attempts} failed:`, webhookError.message);
                    if (attempts >= 3) throw webhookError;
                    await new Promise(resolve => setTimeout(resolve, 2000 * attempts));
                }
            }
        }

        // Start server
        const server = app.listen(PORT, "0.0.0.0", () => {
            console.log(`✅ Server running on port ${PORT}`);
            
            // Start payment monitor
            monitorInterval = setInterval(() => {
                monitorPayments().catch(err => {
                    console.error('Monitor error:', err);
                    performanceMonitor.logRequest(false);
                });
            }, monitorIntervalSeconds * 1000);

            // Initial run with delay
            setTimeout(() => {
                monitorPayments().catch(console.error);
            }, 3000);

            // Start polling if not in production
            if (!process.env.RAILWAY_ENVIRONMENT) {
                bot.startPolling().then(() => {
                    console.log("🔵 Bot polling started");
                }).catch(console.error);
            }
        });

        server.on('error', (err) => {
            console.error('Server error:', err);
            if (err.code === 'EADDRINUSE') {
                console.error(`Port ${PORT} already in use`);
                process.exit(1);
            }
        });

    } catch (error) {
        console.error("💥 Failed to start:", error);
        process.exit(1);
    }
}

// Enhanced graceful shutdown
const shutdown = (signal) => {
    console.log(`\n${signal} received, shutting down gracefully...`);
    
    // 1. Stop monitoring first
    if (monitorInterval) {
        clearInterval(monitorInterval);
        console.log("🛑 Stopped payment monitor");
    }
    
    // 2. Close Telegram bot
    try {
        if (bot.isPolling()) {
            bot.stopPolling();
            console.log("🛑 Stopped bot polling");
        }
        if (process.env.RAILWAY_ENVIRONMENT) {
            bot.deleteWebHook();
            console.log("🛑 Removed webhook");
        }
    } catch (e) {
        console.error("Error stopping bot:", e);
    }
    
    // 3. Close queues
    messageQueue.pause();
    paymentProcessor.highPriorityQueue.pause();
    paymentProcessor.normalQueue.pause();
    console.log("🛑 Paused all processing queues");

    // 4. Close database pool with timeout
    const dbTimeout = setTimeout(() => {
        console.warn("⚠️ Forcing database pool closure");
        process.exit(1);
    }, 5000);
    
    pool.end().then(() => {
        clearTimeout(dbTimeout);
        console.log("✅ Database pool closed");
        process.exit(0);
    }).catch(err => {
        console.error("❌ Pool close error:", err);
        process.exit(1);
    });
};

// Handle signals
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

// Handle uncaught exceptions
process.on('uncaughtException', (err) => {
    console.error('Uncaught Exception:', err);
    shutdown('UNCAUGHT_EXCEPTION');
});

process.on('unhandledRejection', (reason) => {
    console.error('Unhandled Rejection:', reason);
});

// Start the server
startServer().then(() => {
    console.log("🚀 Bot initialization complete");
}).catch(err => {
    console.error("🔥 Failed to initialize:", err);
});

// --- Helper Functions ---
async function updateBetStatus(betId, status) {
    const query = `
        UPDATE bets SET status = $1 
        WHERE id = $2
        RETURNING *;
    `;
    
    try {
        await pool.query(query, [status, betId]);
        return true;
    } catch (err) {
        console.error(`DB Error updating bet status:`, err);
        return false;
    }
}

async function recordPayout(betId, status, signature) {
    const query = `
        UPDATE bets 
        SET status = $1,
            payout_tx_signature = $2,
            processed_at = NOW()
        WHERE id = $3
        RETURNING *;
    `;
    
    try {
        await pool.query(query, [status, signature, betId]);
        return true;
    } catch (err) {
        console.error(`DB Error recording payout:`, err);
        return false;
    }
}

function calculatePayoutWithFees(lamports, gameType, betDetails = {}) {
    const basePayout = gameType === 'coinflip'
        ? BigInt(Math.floor(Number(lamports) * (2 - GAME_CONFIG.coinflip.houseEdge)))
        : BigInt(Math.floor(Number(lamports) * (betDetails.odds || 1) * (1 - GAME_CONFIG.race.houseEdge)));
    
    return basePayout > FEE_BUFFER 
        ? basePayout - FEE_BUFFER 
        : 0n;
}

function getPayerFromTransaction(tx) {
    if (!tx || !tx.meta || !tx.transaction?.message?.accountKeys) return null;
    
    const message = tx.transaction.message;
    const preBalances = tx.meta.preBalances;
    const postBalances = tx.meta.postBalances;
    
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
                const memo = bs58.decode(instruction.data).toString('utf-8');
                return validateMemoFormat(memo) ? memo : null;
            }
        }
    } catch (e) {
        console.error("Error parsing memo:", e);
    }
    return null;
}

async function getLinkedWallet(userId) {
    const cacheKey = `wallet-${userId}`;
    
    if (walletCache.has(cacheKey)) {
        const { wallet, timestamp } = walletCache.get(cacheKey);
        if (Date.now() - timestamp < CACHE_TTL) {
            return wallet;
        }
    }
    
    const query = `SELECT wallet_address FROM wallets WHERE user_id = $1`;
    try {
        const res = await pool.query(query, [String(userId)]);
        const wallet = res.rows[0]?.wallet_address;
        
        if (wallet) {
            walletCache.set(cacheKey, {
                wallet,
                timestamp: Date.now()
            });
        }
        
        return wallet;
    } catch (err) {
        console.error(`DB Error fetching wallet:`, err);
        return undefined;
    }
}

function isRetryableError(error) {
    return error.message.includes('429') || 
           error.message.includes('timeout') ||
           error.message.includes('rate limit');
}
