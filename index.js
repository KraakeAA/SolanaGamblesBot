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

// --- Environment Validation ---
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

if (process.env.RAILWAY_ENVIRONMENT) {
    REQUIRED_ENV_VARS.push('RAILWAY_PUBLIC_DOMAIN');
}

let missingVars = false;
REQUIRED_ENV_VARS.forEach((key) => {
    if (!process.env[key]) {
        console.error(`❌ Missing env var: ${key}`);
        missingVars = true;
    }
});

if (missingVars) {
    console.error("Missing required environment variables");
    process.exit(1);
}

// --- Constants ---
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

const FEE_BUFFER = BigInt(process.env.FEE_MARGIN || '5000');
const PRIORITY_FEE_RATE = 0.0001;
const CACHE_TTL = 300000;
const MAX_PROCESSED_SIGNATURES = 1000;

// --- Initialize Core Components ---
const app = express();

// 1. Solana Connection
const solanaConnection = new RateLimitedConnection(process.env.RPC_URL, {
    maxConcurrent: 3,
    retryBaseDelay: 1000,
    commitment: 'confirmed',
    httpHeaders: {
        'Content-Type': 'application/json',
        'solana-client': `SolanaGamblesBot/2.0 (${process.env.RAILWAY_ENVIRONMENT ? 'railway' : 'local'})`
    },
    rateLimitCooloff: 15000
});

// 2. Database Pool
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

// 3. Telegram Bot
const bot = new TelegramBot(process.env.BOT_TOKEN, {
    polling: !process.env.RAILWAY_ENVIRONMENT,
    request: {
        timeout: 10000,
        agentOptions: {
            keepAlive: true,
            timeout: 60000
        }
    }
});

// 4. Processing Queues
const messageQueue = new PQueue({
    concurrency: 5,
    timeout: 30000,
    throwOnTimeout: true
});

// --- State Management ---
const walletTrackers = new Map([
    [process.env.MAIN_WALLET_ADDRESS, { lastSignature: null, lastChecked: 0 }],
    [process.env.RACE_WALLET_ADDRESS, { lastSignature: null, lastChecked: 0 }]
]);

const confirmCooldown = new Map();
const walletCache = new Map();
const processedSignaturesThisSession = new Set();
let isShuttingDown = false;

// --- Performance Tracking ---
const performanceMonitor = {
    requests: 0,
    errors: 0,
    startTime: Date.now(),
    logRequest(success) {
        this.requests++;
        if (!success) this.errors++;
        if (this.requests % 50 === 0) {
            console.log(`📊 Requests: ${this.requests} | Errors: ${this.errors}`);
        }
    }
};

// --- Database Initialization ---
async function initializeDatabase() {
    console.log("Initializing database...");
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
        
        await client.query(`
            CREATE INDEX IF NOT EXISTS idx_bets_status ON bets(status);
            CREATE INDEX IF NOT EXISTS idx_bets_user_id ON bets(user_id);
            CREATE INDEX IF NOT EXISTS idx_bets_expires_at ON bets(expires_at);
            CREATE INDEX IF NOT EXISTS idx_bets_priority ON bets(priority);
        `);
        
        console.log("✅ Database initialized");
    } catch (err) {
        console.error("❌ Database init error:", err);
        throw err;
    } finally {
        if (client) client.release();
    }
}
// --- Payment Processor Implementation ---
class PaymentProcessor {
    constructor() {
        this.highPriorityQueue = new PQueue({
            concurrency: 3,
            priority: (job) => job.priority || 0
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
        const highPriorityLoad = this.highPriorityQueue.size;
        this.normalQueue.concurrency = Math.max(1, 5 - Math.floor(highPriorityLoad / 2));
    }

    async processJob(job) {
        if (this.activeProcesses.has(job.signature)) return;
        this.activeProcesses.add(job.signature);

        try {
            if (job.type === 'payout') {
                return await this._processPayout(job);
            } else {
                return await this._processPayment(
                    job.signature, 
                    job.walletType,
                    job.retries || 0
                );
            }
        } finally {
            this.activeProcesses.delete(job.signature);
        }
    }

    async _processPayout(job) {
        const { betId, recipient, amount, gameType, chatId } = job;
        try {
            const sendResult = await sendSol(recipient, amount, gameType);
            if (sendResult.success) {
                await recordPayout(betId, 'completed_win_paid', sendResult.signature);
                await bot.sendMessage(
                    chatId,
                    `💰 Payout successful!\nTX: \`${sendResult.signature}\``,
                    { parse_mode: 'Markdown' }
                );
                return { success: true };
            } else if (job.retries < 3) {
                job.retries++;
                job.priority = Math.min(job.priority + 1, 3);
                await this.addPaymentJob(job);
            }
            return { success: false };
        } catch (error) {
            console.error(`Payout failed for bet ${betId}:`, error);
            throw error;
        }
    }

    async _processPayment(signature, walletType, attempt = 0) {
        if (processedSignaturesThisSession.has(signature)) {
            return { processed: false, reason: 'already_processed' };
        }

        const checkQuery = `SELECT id FROM bets WHERE paid_tx_signature = $1 LIMIT 1`;
        const processed = await pool.query(checkQuery, [signature]);
        if (processed.rowCount > 0) {
            processedSignaturesThisSession.add(signature);
            return { processed: false, reason: 'exists_in_db' };
        }

        const tx = await solanaConnection.getParsedTransaction(
            signature, 
            { maxSupportedTransactionVersion: 0 }
        );

        if (!tx || tx.meta?.err) {
            processedSignaturesThisSession.add(signature);
            return { processed: false, reason: 'invalid_tx' };
        }

        const memo = findMemoInTx(tx);
        if (!memo) {
            processedSignaturesThisSession.add(signature);
            return { processed: false, reason: 'no_valid_memo' };
        }

        const bet = await findBetByMemo(memo);
        if (!bet || bet.status !== 'awaiting_payment') {
            processedSignaturesThisSession.add(signature);
            return { processed: false, reason: 'no_matching_bet' };
        }

        const { transferAmount, payerAddress } = analyzeTransactionAmounts(tx, walletType);
        const expectedAmount = BigInt(bet.expected_lamports);
        const tolerance = BigInt(5000);
        
        if (transferAmount < (expectedAmount - tolerance) || 
            transferAmount > (expectedAmount + tolerance)) {
            await updateBetStatus(bet.id, 'error_payment_mismatch');
            return { processed: false, reason: 'amount_mismatch' };
        }

        const txTime = tx.blockTime ? new Date(tx.blockTime * 1000) : new Date(0);
        if (txTime > new Date(bet.expires_at)) {
            await updateBetStatus(bet.id, 'error_payment_expired');
            return { processed: false, reason: 'expired' };
        }

        await markBetPaid(bet.id, signature);

        if (payerAddress) {
            await linkUserWallet(bet.user_id, payerAddress);
        } else {
            const payerKey = getPayerFromTransaction(tx);
            if (payerKey) await linkUserWallet(bet.user_id, payerKey.toBase58());
        }

        processedSignaturesThisSession.add(signature);
        return { processed: true };
    }
}

const paymentProcessor = new PaymentProcessor();

// --- Transaction Helpers ---
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

async function sendSol(recipientPublicKey, amountLamports, gameType) {
    const privateKey = gameType === 'coinflip' 
        ? process.env.BOT_PRIVATE_KEY 
        : process.env.RACE_BOT_PRIVATE_KEY;
    
    const recipientPubKey = new PublicKey(recipientPublicKey);
    const amountSOL = Number(amountLamports) / LAMPORTS_PER_SOL;
    const priorityFee = Math.min(
        1000000,
        Math.max(1000, Math.floor(Number(amountLamports) * PRIORITY_FEE_RATE))
    );

    for (let attempt = 1; attempt <= 3; attempt++) {
        try {
            const payerWallet = Keypair.fromSecretKey(bs58.decode(privateKey));
            const latestBlockhash = await solanaConnection.getLatestBlockhash();

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

            const signature = await sendAndConfirmTransaction(
                solanaConnection,
                transaction,
                [payerWallet],
                { 
                    commitment: 'confirmed',
                    skipPreflight: false
                }
            );

            console.log(`✅ Sent ${(Number(transferAmount)/LAMPORTS_PER_SOL).toFixed(6)} SOL`);
            return { success: true, signature };
            
        } catch (error) {
            console.error(`Attempt ${attempt}/3 failed:`, error.message);
            if (attempt === 3) {
                return { 
                    success: false, 
                    error: `Failed after 3 attempts: ${error.message}` 
                };
            }
            await new Promise(resolve => setTimeout(resolve, 1000 * attempt));
        }
    }
}
// --- Monitoring System ---
let isMonitorRunning = false;
let monitorIntervalSeconds = 30;
let monitorInterval = null;

async function monitorPayments() {
    if (isMonitorRunning || isShuttingDown) {
        console.log('Monitor already running/shutting down, skipping cycle');
        return;
    }

    isMonitorRunning = true;
    const startTime = Date.now();
    let processedCount = 0;

    try {
        for (const [address, tracker] of walletTrackers) {
            try {
                // Skip if checked recently
                if (Date.now() - tracker.lastChecked < monitorIntervalSeconds * 500) {
                    continue;
                }

                const signatures = await solanaConnection.getSignaturesForAddress(
                    new PublicKey(address),
                    { 
                        limit: 15,
                        before: tracker.lastSignature
                    }
                );

                if (!signatures?.length) {
                    tracker.lastChecked = Date.now();
                    continue;
                }

                // Process oldest first
                for (const sig of signatures.reverse()) {
                    await paymentProcessor.addPaymentJob({
                        type: 'monitor_payment',
                        signature: sig.signature,
                        walletAddress: address,
                        walletType: address === process.env.MAIN_WALLET_ADDRESS ? 'coinflip' : 'race',
                        timestamp: Date.now(),
                        priority: 1,
                        retries: 0
                    });
                    processedCount++;
                }

                // Update tracker
                tracker.lastSignature = signatures[0].signature;
                tracker.lastChecked = Date.now();

            } catch (error) {
                console.error(`Wallet ${address} monitoring error:`, error.message);
                performanceMonitor.logRequest(false);
            }
        }
    } finally {
        isMonitorRunning = false;
        console.log(`♻️ Processed ${processedCount} txs in ${Date.now() - startTime}ms`);
        adjustMonitorInterval(processedCount);
    }
}

function adjustMonitorInterval(processedCount) {
    const newInterval = processedCount > 10 ? Math.max(15, monitorIntervalSeconds - 5) :
                      processedCount === 0 ? Math.min(120, monitorIntervalSeconds + 15) :
                      monitorIntervalSeconds;

    if (newInterval !== monitorIntervalSeconds) {
        monitorIntervalSeconds = newInterval;
        clearInterval(monitorInterval);
        monitorInterval = setInterval(monitorPayments, monitorIntervalSeconds * 1000);
        console.log(`⏱ Monitoring interval adjusted to ${monitorIntervalSeconds}s`);
    }
}

// --- Game Processing ---
async function processPaidBet(bet) {
    const statusCheck = await pool.query(
        'SELECT status FROM bets WHERE id = $1 FOR UPDATE',
        [bet.id]
    );
    
    if (statusCheck.rows[0]?.status !== 'payment_verified') {
        return;
    }

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
    
    // Calculate payout
    const payoutLamports = win ? calculatePayoutWithFees(expected_lamports, 'coinflip') : 0n;

    // Get user info
    let displayName = `User ${user_id}`;
    try {
        const chatMember = await bot.getChatMember(chat_id, user_id);
        displayName = chatMember.user.username 
            ? `@${chatMember.user.username}` 
            : chatMember.user.first_name;
    } catch (e) {
        console.warn(`Couldn't get username for ${user_id}:`, e.message);
    }

    if (win) {
        const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
        console.log(`🎰 Bet ${betId}: ${displayName} won ${payoutSOL} SOL`);

        const winnerAddress = await getLinkedWallet(user_id);
        if (!winnerAddress) {
            await bot.sendMessage(
                chat_id,
                `🎉 ${displayName}, you won but no wallet linked!\n` +
                `Result: *${result}*\n` +
                `Place another bet to link your wallet.`,
                { parse_mode: 'Markdown' }
            );
            await updateBetStatus(betId, 'error_payout_no_wallet');
            return;
        }

        try {
            await bot.sendMessage(
                chat_id,
                `🎉 ${displayName} won ${payoutSOL.toFixed(6)} SOL!\n` +
                `Result: *${result}*\n\n` +
                `💸 Processing payout...`,
                { parse_mode: 'Markdown' }
            );

            await paymentProcessor.addPaymentJob({
                type: 'payout',
                betId,
                recipient: winnerAddress,
                amount: payoutLamports,
                gameType: 'coinflip',
                priority: 2,
                chatId: chat_id,
                displayName,
                result,
                retries: 0
            });

        } catch (e) {
            console.error(`Payout error for ${betId}:`, e);
            await bot.sendMessage(
                chat_id,
                `⚠️ Payout failed - contact support with Bet ID: ${betId}`,
                { parse_mode: 'Markdown' }
            );
            await updateBetStatus(betId, 'error_payout_exception');
        }
    } else {
        await bot.sendMessage(
            chat_id,
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
    const config = GAME_CONFIG.race;

    // Race horses with probabilities
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
        await bot.sendMessage(
            chat_id,
            `🏁 Winner: ${winningHorse.emoji} *${winningHorse.name}*! 🏁`,
            { parse_mode: 'Markdown' }
        );
    } catch (e) {
        console.error(`Race updates failed for ${betId}:`, e);
    }
        // Determine race result
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
        console.warn(`Couldn't get username for ${user_id}:`, e.message);
    }

    if (win) {
        const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
        console.log(`🏇 Bet ${betId}: ${displayName} won ${payoutSOL} SOL`);

        const winnerAddress = await getLinkedWallet(user_id);
        if (!winnerAddress) {
            await bot.sendMessage(
                chat_id,
                `🎉 ${displayName}, your horse won but no wallet linked!\n` +
                `Payout: ${payoutSOL.toFixed(6)} SOL\n` +
                `Place another bet to link your wallet.`,
                { parse_mode: 'Markdown' }
            );
            await updateBetStatus(betId, 'error_payout_no_wallet');
            return;
        }

        try {
            await bot.sendMessage(
                chat_id,
                `🎉 ${displayName}, your horse *${horseName}* won!\n` +
                `Payout: ${payoutSOL.toFixed(6)} SOL\n\n` +
                `💸 Processing payout...`,
                { parse_mode: 'Markdown' }
            );

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
                winningHorse,
                retries: 0
            });

        } catch (e) {
            console.error(`Payout error for ${betId}:`, e);
            await bot.sendMessage(
                chat_id,
                `⚠️ Payout failed - contact support with Bet ID: ${betId}`,
                { parse_mode: 'Markdown' }
            );
            await updateBetStatus(betId, 'error_payout_exception');
        }
    } else {
        await bot.sendMessage(
            chat_id,
            `❌ ${displayName}, your horse *${horseName}* lost!\n` +
            `Winner: ${winningHorse.emoji} *${winningHorse.name}*`,
            { parse_mode: 'Markdown' }
        );
        await updateBetStatus(betId, 'completed_loss');
    }
}

// --- Database Helpers ---
async function findBetByMemo(memoId) {
    const query = `
        SELECT * FROM bets 
        WHERE memo_id = $1 AND status = 'awaiting_payment'
        ORDER BY priority DESC, created_at ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1;
    `;
    const res = await pool.query(query, [memoId]);
    return res.rows[0];
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
    await pool.query(query, [signature, betId]);
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
    const res = await pool.query(query, [String(userId), walletAddress]);
    
    if (res.rows[0]) {
        walletCache.set(cacheKey, {
            wallet: walletAddress,
            timestamp: Date.now()
        });
    }
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
    const res = await pool.query(query, [String(userId)]);
    const wallet = res.rows[0]?.wallet_address;
    
    if (wallet) {
        walletCache.set(cacheKey, {
            wallet,
            timestamp: Date.now()
        });
    }
    
    return wallet;
}

// --- Bot Command Handlers ---
async function handleMessage(msg) {
    if (!msg.text) return;
    
    try {
        // Cooldown check
        if (confirmCooldown.has(msg.from.id)) {
            const lastTime = confirmCooldown.get(msg.from.id);
            if (Date.now() - lastTime < 3000) {
                await bot.sendMessage(msg.chat.id, "⚠️ Please wait a few seconds...");
                return;
            }
        }
        confirmCooldown.set(msg.from.id, Date.now());

        if (/^\/start$/i.test(msg.text)) {
            await handleStartCommand(msg);
        } 
        else if (/^\/coinflip$/i.test(msg.text)) {
            await handleCoinflipCommand(msg);
        }
        else if (/^\/wallet$/i.test(msg.text)) {
            await handleWalletCommand(msg);
        }
        else if (/^\/bet (\d+\.?\d*) (heads|tails)/i.test(msg.text)) {
            await handleBetCommand(msg);
        }
        else if (/^\/race$/i.test(msg.text)) {
            await handleRaceCommand(msg);
        }
        else if (/^\/betrace (\d+\.?\d*) (\w+)/i.test(msg.text)) {
            await handleBetRaceCommand(msg);
        }

        performanceMonitor.logRequest(true);
    } catch (error) {
        console.error(`Message error: ${msg.text}`, error);
        performanceMonitor.logRequest(false);
        await bot.sendMessage(msg.chat.id, "⚠️ An error occurred. Please try again.");
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

    // Validate amount
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

    // Validate amount
    const betAmount = parseFloat(match[1]);
    if (isNaN(betAmount) || betAmount < config.minBet || betAmount > config.maxBet) {
        await bot.sendMessage(chatId,
            `⚠️ Bet must be ${config.minBet}-${config.maxBet} SOL`
        );
        return;
    }

    // Validate horse
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

// --- Server Setup ---
const webhookPath = `/bot${process.env.BOT_TOKEN}`;
app.use(express.json({ limit: '10kb' }));

// Health endpoint
app.get('/', (req, res) => {
    performanceMonitor.logRequest(true);
    res.json({
        status: 'online',
        version: '2.0',
        uptime: Math.floor(process.uptime()),
        solana: solanaConnection.getRequestStats(),
        queues: {
            messages: messageQueue.size,
            payments: paymentProcessor.highPriorityQueue.size + paymentProcessor.normalQueue.size
        }
    });
});

// Webhook handler
app.post(webhookPath, (req, res) => {
    messageQueue.add(() => {
        try {
            bot.processUpdate(req.body);
            res.sendStatus(200);
        } catch (error) {
            console.error("Webhook error:", error);
            res.status(500).json({ error: error.message });
        }
    });
});

// --- Startup & Shutdown ---
async function startServer() {
    try {
        await initializeDatabase();
        const PORT = process.env.PORT || 3000;

        // Webhook setup for production
        if (process.env.RAILWAY_ENVIRONMENT) {
            const webhookUrl = `https://${process.env.RAILWAY_PUBLIC_DOMAIN}${webhookPath}`;
            await bot.setWebHook(webhookUrl);
            console.log(`✅ Webhook set to: ${webhookUrl}`);
        }

        // Start server
        const server = app.listen(PORT, "0.0.0.0", () => {
            console.log(`✅ Server running on port ${PORT}`);
            
            // Start monitoring
            monitorInterval = setInterval(() => {
                monitorPayments().catch(err => {
                    console.error('Monitor error:', err.message);
                });
            }, monitorIntervalSeconds * 1000);

            // Initial run
            setTimeout(monitorPayments, 3000);
        });

        server.on('error', (err) => {
            console.error('Server error:', err);
            process.exit(1);
        });

    } catch (error) {
        console.error("💥 Failed to start:", error);
        process.exit(1);
    }
}

const shutdown = async (signal) => {
    if (isShuttingDown) return;
    isShuttingDown = true;

    console.log(`\n${signal} received, shutting down...`);
    
    // 1. Stop monitoring
    clearInterval(monitorInterval);
    console.log("🛑 Stopped payment monitor");

    // 2. Stop bot
    try {
        if (bot.isPolling()) bot.stopPolling();
        if (process.env.RAILWAY_ENVIRONMENT) await bot.deleteWebHook();
        console.log("🛑 Stopped Telegram bot");
    } catch (e) {
        console.error("Bot shutdown error:", e);
    }

    // 3. Pause queues
    messageQueue.pause();
    paymentProcessor.highPriorityQueue.pause();
    paymentProcessor.normalQueue.pause();
    console.log("🛑 Paused all queues");

    // 4. Close database
    try {
        await pool.end();
        console.log("✅ Database pool closed");
        process.exit(0);
    } catch (err) {
        console.error("❌ Database shutdown error:", err);
        process.exit(1);
    }
};

// Signal handlers
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

// Start the server
startServer().then(() => {
    console.log("🚀 Bot initialization complete");
}).catch(err => {
    console.error("🔥 Failed to initialize:", err);
});

// --- Helper Functions ---
function generateMemoId(prefix = 'BET') {
    return `${prefix}-${randomBytes(6).toString('hex').toUpperCase()}`;
}

async function savePendingBet(userId, chatId, gameType, details, lamports, memoId, expiresAt) {
    const query = `
        INSERT INTO bets (
            user_id, chat_id, game_type, bet_details, 
            expected_lamports, memo_id, status, expires_at, fees_paid, priority
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        RETURNING id;
    `;
    const values = [
        String(userId), String(chatId), gameType, details, 
        lamports, memoId, 'awaiting_payment', expiresAt, FEE_BUFFER, 0
    ];
    const res = await pool.query(query, values);
    return { success: true, id: res.rows[0].id };
}

function calculatePayoutWithFees(lamports, gameType, betDetails = {}) {
    const basePayout = gameType === 'coinflip'
        ? BigInt(Math.floor(Number(lamports) * (2 - GAME_CONFIG.coinflip.houseEdge)))
        : BigInt(Math.floor(Number(lamports) * (betDetails.odds || 1) * (1 - GAME_CONFIG.race.houseEdge)));
    
    return basePayout > FEE_BUFFER ? basePayout - FEE_BUFFER : 0n;
}

async function updateBetStatus(betId, status) {
    await pool.query(
        'UPDATE bets SET status = $1 WHERE id = $2',
        [status, betId]
    );
}

async function recordPayout(betId, status, signature) {
    await pool.query(
        'UPDATE bets SET status = $1, payout_tx_signature = $2 WHERE id = $3',
        [status, signature, betId]
    );
}
// ==================== [9] Express Server Setup ====================
app.use(express.json({ limit: '10kb' }));

// Health endpoint with system diagnostics
app.get('/', (req, res) => {
    performanceMonitor.logRequest(true);
    res.json({
        status: 'operational',
        version: '2.0',
        uptime: Math.floor(process.uptime()),
        requests: performanceMonitor.requests,
        solana: {
            ...solanaConnection.getRequestStats(),
            rpcUrl: process.env.RPC_URL.replace(/https?:\/\/([^/]+).*/, '$1') // Mask sensitive URL
        },
        memory: process.memoryUsage(),
        queues: {
            messages: messageQueue.size,
            highPriority: paymentProcessor.highPriorityQueue.size,
            normal: paymentProcessor.normalQueue.size
        }
    });
});

// Webhook handler with queue integration
app.post(webhookPath, (req, res) => {
    messageQueue.add(() => {
        try {
            if (!req.body || typeof req.body !== 'object') {
                return res.status(400).json({ error: 'Invalid request format' });
            }
            bot.processUpdate(req.body);
            res.sendStatus(200);
        } catch (error) {
            console.error("❌ Webhook processing failed:", error);
            performanceMonitor.logRequest(false);
            res.status(500).json({ 
                error: 'Internal server error',
                details: error.message 
            });
        }
    });
});


// ==================== [12] Process Handlers ====================
process.on('SIGTERM', () => shutdown('SIGTERM')); // Kubernetes/Docker
process.on('SIGINT', () => shutdown('SIGINT'));   // Ctrl+C
process.on('uncaughtException', (err) => {
    console.error('💥 Uncaught Exception:', err);
    shutdown('UNCAUGHT_EXCEPTION');
});
process.on('unhandledRejection', (reason, promise) => {
    console.error('⚠️ Unhandled Rejection at:', promise, 'reason:', reason);
});

// ==================== [13] Application Launch ====================
async function startServer() {
    try {
        await initializeDatabase();
        const PORT = process.env.PORT || 3000;

        // Webhook setup for production
        if (process.env.RAILWAY_ENVIRONMENT) {
            const webhookUrl = `https://${process.env.RAILWAY_PUBLIC_DOMAIN}${webhookPath}`;
            await bot.setWebHook(webhookUrl, {
                max_connections: 50,
                allowed_updates: ['message', 'callback_query']
            });
            console.log(`🌍 Webhook configured: ${webhookUrl}`);
        }

        // Start HTTP server
        const server = app.listen(PORT, "0.0.0.0", () => {
            console.log(`✅ Server running on port ${PORT}`);
            
            // Start monitoring
            monitorInterval = setInterval(() => {
                monitorPayments().catch(err => {
                    console.error('Monitor error:', err);
                    performanceMonitor.logRequest(false);
                });
            }, monitorIntervalSeconds * 1000);

            // Initial delayed execution
            setTimeout(() => {
                monitorPayments().catch(console.error);
            }, 3000);
        });

        server.on('error', (err) => {
            console.error('💢 Server error:', err);
            process.exit(1);
        });

    } catch (error) {
        console.error("🔥 Catastrophic startup failure:", error);
        process.exit(1);
    }
}

// ==================== [14] FINAL LINE ====================
startServer().then(() => console.log("🚀 Application startup complete"));
