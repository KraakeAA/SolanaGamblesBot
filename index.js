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
import { RateLimitedConnection } from './lib/solana-connection.js';

// ==================== ENVIRONMENT ====================
const REQUIRED_ENV_VARS = [
    'BOT_TOKEN',
    'DATABASE_URL',
    'BOT_PRIVATE_KEY',
    'RACE_BOT_PRIVATE_KEY',
    'MAIN_WALLET_ADDRESS',
    'RACE_WALLET_ADDRESS',
    'RPC_URL',
    'FEE_MARGIN',
    ...(process.env.RAILWAY_ENVIRONMENT ? [
        'RAILWAY_PUBLIC_DOMAIN',
        'WEBHOOK_SECRET'
    ] : [])
];

const missingVars = REQUIRED_ENV_VARS.filter(key => !process.env[key]);
if (missingVars.length) {
    console.error(`❌ Missing env vars: ${missingVars.join(', ')}`);
    process.exit(1);
}

// ==================== CONSTANTS ====================
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
        houseEdge: 0.02,
        horses: [
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
        ].map(h => ({
            ...h,
            normalizedProbability: h.winProbability / 
                horses.reduce((sum, x) => sum + x.winProbability, 0)
        }))
    }
};

const FEE_BUFFER = BigInt(process.env.FEE_MARGIN || 5000);
const PRIORITY_FEE_RATE = 0.0001;
const CACHE_TTL = 300000;
const MAX_PROCESSED_SIGNATURES = 1000;

// ==================== INITIALIZATION ====================
const app = express();
const solanaConnection = new RateLimitedConnection(process.env.RPC_URL, {
    maxConcurrent: 3,
    retryBaseDelay: 1000,
    commitment: 'confirmed',
    rateLimitCooloff: 15000,
    httpHeaders: {
        'Content-Type': 'application/json',
        'solana-client': `Bot/2.0 (${process.env.NODE_ENV})`
    }
});

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

const messageQueue = new PQueue({
    concurrency: 5,
    timeout: 30000,
    throwOnTimeout: true
});

// ==================== STATE MANAGEMENT ====================
const walletTrackers = new Map([
    [process.env.MAIN_WALLET_ADDRESS, { lastSignature: null, lastChecked: 0 }],
    [process.env.RACE_WALLET_ADDRESS, { lastSignature: null, lastChecked: 0 }]
]);

const confirmCooldown = new Map();
const walletCache = new Map();
const processedSignaturesThisSession = new Set();
let isShuttingDown = false;

// ==================== PERFORMANCE MONITOR ====================
const performanceMonitor = {
    requests: 0,
    errors: 0,
    startTime: Date.now(),
    logRequest(success) {
        this.requests++;
        if (!success) this.errors++;
        if (this.requests % 50 === 0) {
            console.log(`📊 Requests: ${this.requests} | Error Rate: ${(this.errors/this.requests*100).toFixed(1)}%`);
        }
    }
};

// ==================== DATABASE ====================
async function initializeDatabase() {
    const client = await pool.connect();
    try {
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
            CREATE TABLE IF NOT EXISTS wallets (
                user_id TEXT PRIMARY KEY,
                wallet_address TEXT NOT NULL,
                linked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_used_at TIMESTAMPTZ
            );
            CREATE INDEX IF NOT EXISTS idx_bets_status ON bets(status);
            CREATE INDEX IF NOT EXISTS idx_bets_user_id ON bets(user_id);
            CREATE INDEX IF NOT EXISTS idx_bets_expires_at ON bets(expires_at);
            CREATE INDEX IF NOT EXISTS idx_bets_priority ON bets(priority);
        `);
    } finally {
        client.release();
    }
}

// ==================== PAYMENT PROCESSOR ====================
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
            return job.type === 'payout' 
                ? await this._processPayout(job)
                : await this._processPayment(job.signature, job.walletType, job.retries || 0);
        } finally {
            this.activeProcesses.delete(job.signature);
        }
    }

    async _processPayout(job) {
        const { betId, recipient, amount, gameType, chatId } = job;
        try {
            const sendResult = await sendSol(recipient, amount, gameType);
            if (!sendResult.success) throw new Error(sendResult.error);

            await recordPayout(betId, 'completed_win_paid', sendResult.signature);
            await bot.sendMessage(
                chatId,
                `💰 Payout successful!\nTX: \`${sendResult.signature}\``,
                { parse_mode: 'Markdown' }
            );
            return { success: true };
        } catch (error) {
            console.error(`Payout failed for bet ${betId}:`, error);
            if (job.retries < 3) {
                job.retries++;
                job.priority = Math.min(job.priority + 1, 3);
                await this.addPaymentJob(job);
            }
            throw error;
        }
    }

    async _processPayment(signature, walletType, attempt = 0) {
        if (processedSignaturesThisSession.has(signature)) {
            return { processed: false, reason: 'already_processed' };
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
        const bet = memo ? await findBetByMemo(memo) : null;

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

        await markBetPaid(bet.id, signature);
        if (payerAddress) await linkUserWallet(bet.user_id, payerAddress);
        
        processedSignaturesThisSession.add(signature);
        return { processed: true };
    }
}

const paymentProcessor = new PaymentProcessor();

// ==================== TRANSACTION HELPERS ====================
function analyzeTransactionAmounts(tx, walletType) {
    let transferAmount = 0n;
    let payerAddress = null;
    const targetAddress = walletType === 'coinflip' 
        ? process.env.MAIN_WALLET_ADDRESS
        : process.env.RACE_WALLET_ADDRESS;

    const instructions = [
        ...(tx.transaction.message.instructions || []),
        ...(tx.meta.innerInstructions?.flatMap(i => i.instructions) || [])
    ];

    for (const inst of instructions) {
        const programId = inst.programIdIndex !== undefined 
            ? tx.transaction.message.accountKeys[inst.programIdIndex]?.pubkey?.toBase58()
            : inst.programId?.toBase58();

        if (programId === SystemProgram.programId.toBase58() && inst.parsed?.type === 'transfer') {
            const { destination, source, lamports } = inst.parsed.info;
            if (destination === targetAddress) {
                transferAmount += BigInt(lamports || 0);
                if (!payerAddress) payerAddress = source;
            }
        }
    }

    return { transferAmount, payerAddress };
}

async function sendSol(recipientPublicKey, amountLamports, gameType) {
    const privateKey = gameType === 'coinflip' 
        ? process.env.BOT_PRIVATE_KEY 
        : process.env.RACE_BOT_PRIVATE_KEY;
    
    const payerWallet = Keypair.fromSecretKey(bs58.decode(privateKey));
    const recipientPubKey = new PublicKey(recipientPublicKey);
    const priorityFee = Math.min(
        1000000,
        Math.max(1000, Math.floor(Number(amountLamports) * PRIORITY_FEE_RATE))
    );

    for (let attempt = 1; attempt <= 3; attempt++) {
        try {
            const latestBlockhash = await solanaConnection.getLatestBlockhash();
            const transaction = new Transaction({
                recentBlockhash: latestBlockhash.blockhash,
                feePayer: payerWallet.publicKey
            });

            transaction.add(
                ComputeBudgetProgram.setComputeUnitPrice({ microLamports: priorityFee }),
                SystemProgram.transfer({
                    fromPubkey: payerWallet.publicKey,
                    toPubkey: recipientPubKey,
                    lamports: amountLamports > FEE_BUFFER ? amountLamports - FEE_BUFFER : 0n
                })
            );

            const signature = await sendAndConfirmTransaction(
                solanaConnection,
                transaction,
                [payerWallet],
                { commitment: 'confirmed', skipPreflight: false }
            );

            console.log(`✅ Sent ${(Number(amountLamports)/LAMPORTS_PER_SOL).toFixed(6)} SOL`);
            return { success: true, signature };
        } catch (error) {
            console.error(`Attempt ${attempt}/3 failed:`, error.message);
            if (attempt === 3) return { success: false, error: error.message };
            await new Promise(resolve => setTimeout(resolve, 1000 * attempt));
        }
    }
}

// ==================== GAME LOGIC ====================
async function processPaidBet(bet) {
    const statusCheck = await pool.query(
        'SELECT status FROM bets WHERE id = $1 FOR UPDATE',
        [bet.id]
    );
    
    if (statusCheck.rows[0]?.status !== 'payment_verified') return;

    await pool.query(
        'UPDATE bets SET status = $1 WHERE id = $2',
        ['processing_game', bet.id]
    );

    try {
        await (bet.game_type === 'coinflip' 
            ? handleCoinflipGame(bet) 
            : handleRaceGame(bet));
    } catch (error) {
        console.error(`Error processing bet ${bet.id}:`, error);
        await updateBetStatus(bet.id, 'error_processing_exception');
    }
}

async function handleCoinflipGame(bet) {
    const { id: betId, user_id, chat_id, bet_details, expected_lamports } = bet;
    const result = Math.random() < (0.5 - GAME_CONFIG.coinflip.houseEdge/2) ? 'heads' : 'tails';
    const win = (result === bet_details.choice);
    const payoutLamports = win ? calculatePayoutWithFees(expected_lamports, 'coinflip') : 0n;

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
        const winnerAddress = await getLinkedWallet(user_id);
        if (!winnerAddress) {
            await bot.sendMessage(
                chat_id,
                `🎉 ${displayName}, you won but no wallet linked!\nResult: *${result}*`,
                { parse_mode: 'Markdown' }
            );
            return await updateBetStatus(betId, 'error_payout_no_wallet');
        }

        try {
            await bot.sendMessage(
                chat_id,
                `🎉 ${displayName} won ${(Number(payoutLamports)/LAMPORTS_PER_SOL).toFixed(6)} SOL!\nResult: *${result}*`,
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
            await updateBetStatus(betId, 'error_payout_exception');
        }
    } else {
        await bot.sendMessage(
            chat_id,
            `❌ ${displayName}, you lost!\nYou guessed *${bet_details.choice}* but got *${result}*`,
            { parse_mode: 'Markdown' }
        );
        await updateBetStatus(betId, 'completed_loss');
    }
}

async function handleRaceGame(bet) {
    const { id: betId, user_id, chat_id, bet_details } = bet;
    const { horse: chosenHorse } = bet_details;
    
    // Normalized probability calculation
    let cumulativeProb = 0;
    const rand = Math.random();
    let winningHorse = GAME_CONFIG.race.horses[0];

    for (const horse of GAME_CONFIG.race.horses) {
        cumulativeProb += horse.normalizedProbability;
        if (rand <= cumulativeProb) {
            winningHorse = horse;
            break;
        }
    }

    try {
        await bot.sendMessage(chat_id, `🏇 Race ${betId} starting! You bet on ${chosenHorse}!`, 
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

    const win = (chosenHorse.toLowerCase() === winningHorse.name.toLowerCase());
    if (win) {
        const winnerAddress = await getLinkedWallet(user_id);
        if (!winnerAddress) {
            await bot.sendMessage(
                chat_id,
                `🎉 You won but no wallet linked! Payout: ${calculatePayoutWithFees(bet.expected_lamports, 'race').toFixed(6)} SOL`,
                { parse_mode: 'Markdown' }
            );
            return await updateBetStatus(betId, 'error_payout_no_wallet');
        }

        await paymentProcessor.addPaymentJob({
            type: 'payout',
            betId,
            recipient: winnerAddress,
            amount: calculatePayoutWithFees(bet.expected_lamports, 'race'),
            gameType: 'race',
            priority: 2,
            chatId: chat_id,
            horseName: chosenHorse,
            winningHorse,
            retries: 0
        });
    } else {
        await updateBetStatus(betId, 'completed_loss');
    }
}

// ==================== BOT COMMANDS ====================
async function handleMessage(msg) {
    if (!msg.text) return;
    
    try {
        // Cooldown check
        if (confirmCooldown.has(msg.from.id) && 
            Date.now() - confirmCooldown.get(msg.from.id) < 3000) {
            return await bot.sendMessage(msg.chat.id, "⚠️ Please wait a few seconds...");
        }
        confirmCooldown.set(msg.from.id, Date.now());

        const text = msg.text.replace(/[<>$]/g, ''); // Sanitization
        if (/^\/start$/i.test(text)) {
            await bot.sendAnimation(msg.chat.id, 'https://i.ibb.co/9vDo58q/banner.gif', {
                caption: `🎰 *Solana Gambles*\n\nUse /coinflip or /race to start`,
                parse_mode: 'Markdown'
            });
        }
        else if (/^\/coinflip$/i.test(text)) {
            await handleCoinflipCommand(msg);
        }
        else if (/^\/wallet$/i.test(text)) {
            await handleWalletCommand(msg);
        }
        else if (match = text.match(/^\/bet (\d+\.?\d*) (heads|tails)/i)) {
            await handleBetCommand(msg, match);
        }
        else if (match = text.match(/^\/betrace (\d+\.?\d*) (\w+)/i)) {
            await handleBetRaceCommand(msg, match);
        }

        performanceMonitor.logRequest(true);
    } catch (error) {
        console.error(`Message error: ${msg.text}`, error);
        performanceMonitor.logRequest(false);
        await bot.sendMessage(msg.chat.id, "⚠️ An error occurred. Please try again.");
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
            `⚠️ No wallet linked yet.\nPlace a bet to automatically link your wallet.`
        );
    }
}

async function handleBetCommand(msg, match) {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const config = GAME_CONFIG.coinflip;
    const betAmount = parseFloat(match[1]);

    if (isNaN(betAmount) || betAmount < config.minBet || betAmount > config.maxBet) {
        return await bot.sendMessage(chatId,
            `⚠️ Bet must be between ${config.minBet}-${config.maxBet} SOL`
        );
    }

    const memoId = generateMemoId('CF');
    const expiresAt = new Date(Date.now() + config.expiryMinutes * 60 * 1000);

    await savePendingBet(
        userId, chatId, 'coinflip', 
        { choice: match[2].toLowerCase() }, 
        BigInt(Math.round(betAmount * LAMPORTS_PER_SOL)), 
        memoId, 
        expiresAt
    );

    await bot.sendMessage(chatId,
        `✅ Coinflip bet registered!\n\n` +
        `💸 Send *exactly ${betAmount.toFixed(6)} SOL* to:\n` +
        `\`${process.env.MAIN_WALLET_ADDRESS}\`\n\n` +
        `*MEMO:* \`${memoId}\`\n\n` +
        `Expires in ${config.expiryMinutes} minutes.`,
        { parse_mode: 'Markdown' }
    );
}

async function handleBetRaceCommand(msg, match) {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const config = GAME_CONFIG.race;
    const betAmount = parseFloat(match[1]);

    if (isNaN(betAmount) || betAmount < config.minBet || betAmount > config.maxBet) {
        return await bot.sendMessage(chatId,
            `⚠️ Bet must be ${config.minBet}-${config.maxBet} SOL`
        );
    }

    const chosenHorse = match[2].toLowerCase();
    const horse = GAME_CONFIG.race.horses.find(h => 
        h.name.toLowerCase() === chosenHorse
    );

    if (!horse) {
        return await bot.sendMessage(chatId, "⚠️ Invalid horse name");
    }

    const memoId = generateMemoId('RA');
    const expiresAt = new Date(Date.now() + config.expiryMinutes * 60 * 1000);

    await savePendingBet(
        userId, chatId, 'race',
        { horse: horse.name, odds: horse.odds },
        BigInt(Math.round(betAmount * LAMPORTS_PER_SOL)),
        memoId,
        expiresAt
    );

    await bot.sendMessage(chatId,
        `✅ Bet on ${horse.emoji} *${horse.name}* registered!\n\n` +
        `💸 Send *exactly ${betAmount.toFixed(6)} SOL* to:\n` +
        `\`${process.env.RACE_WALLET_ADDRESS}\`\n\n` +
        `*MEMO:* \`${memoId}\`\n\n` +
        `Expires in ${config.expiryMinutes} minutes.`,
        { parse_mode: 'Markdown' }
    );
}

// ==================== SERVER SETUP ====================
const webhookPath = `/bot${process.env.BOT_TOKEN}`;
app.use(express.json({ limit: '10kb' }));

app.get('/', (req, res) => {
    performanceMonitor.logRequest(true);
    res.json({
        status: 'online',
        version: '2.0',
        uptime: Math.floor(process.uptime()),
        queues: {
            messages: messageQueue.size,
            payments: paymentProcessor.highPriorityQueue.size + paymentProcessor.normalQueue.size
        }
    });
});

app.post(webhookPath, (req, res) => {
    if (req.headers['x-telegram-bot-api-secret-token'] !== process.env.WEBHOOK_SECRET) {
        return res.sendStatus(403);
    }
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

// ==================== MONITORING ====================
let isMonitorRunning = false;
let monitorIntervalSeconds = 30;
let monitorInterval = null;

async function monitorPayments() {
    if (isMonitorRunning || isShuttingDown) return;
    isMonitorRunning = true;

    try {
        for (const [address, tracker] of walletTrackers) {
            if (Date.now() - tracker.lastChecked < monitorIntervalSeconds * 500) continue;

            const signatures = await solanaConnection.getSignaturesForAddress(
                new PublicKey(address),
                { limit: 15, before: tracker.lastSignature }
            );

            if (signatures?.length) {
                for (const sig of signatures.reverse()) {
                    await paymentProcessor.addPaymentJob({
                        type: 'monitor_payment',
                        signature: sig.signature,
                        walletAddress: address,
                        walletType: address === process.env.MAIN_WALLET_ADDRESS ? 'coinflip' : 'race',
                        priority: 1,
                        retries: 0
                    });
                }
                tracker.lastSignature = signatures[0].signature;
            }
            tracker.lastChecked = Date.now();
        }
    } finally {
        isMonitorRunning = false;
        if (processedSignaturesThisSession.size > MAX_PROCESSED_SIGNATURES) {
            processedSignaturesThisSession.clear();
        }
        adjustMonitorInterval();
    }
}

function adjustMonitorInterval() {
    const newInterval = messageQueue.size > 20 ? Math.max(15, monitorIntervalSeconds - 5) :
                      messageQueue.size === 0 ? Math.min(120, monitorIntervalSeconds + 15) :
                      monitorIntervalSeconds;

    if (newInterval !== monitorIntervalSeconds) {
        monitorIntervalSeconds = newInterval;
        clearInterval(monitorInterval);
        monitorInterval = setInterval(() => {
            monitorPayments().catch(err => {
                console.error('Monitor error:', err);
                performanceMonitor.logRequest(false);
            });
        }, monitorIntervalSeconds * 1000);
    }
}

// ==================== LIFECYCLE MANAGEMENT ====================
async function startServer() {
    await initializeDatabase();
    const PORT = process.env.PORT || 3000;

    if (process.env.RAILWAY_ENVIRONMENT) {
        await bot.setWebHook(
            `https://${process.env.RAILWAY_PUBLIC_DOMAIN}${webhookPath}`,
            { secret_token: process.env.WEBHOOK_SECRET }
        );
    }

    const server = app.listen(PORT, "0.0.0.0", () => {
        console.log(`✅ Server running on port ${PORT}`);
        monitorInterval = setInterval(monitorPayments, monitorIntervalSeconds * 1000);
        setTimeout(monitorPayments, 3000);
    });

    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('uncaughtException', (err) => {
        console.error('💥 Uncaught Exception:', err);
        shutdown('UNCAUGHT_EXCEPTION');
    });
}

async function shutdown(signal) {
    if (isShuttingDown) return;
    isShuttingDown = true;

    console.log(`\n${signal} received, shutting down gracefully...`);
    clearInterval(monitorInterval);
    
    try {
        if (bot.isPolling()) bot.stopPolling();
        if (process.env.RAILWAY_ENVIRONMENT) await bot.deleteWebHook();
    } catch (e) {
        console.error("Bot shutdown error:", e);
    }

    messageQueue.pause();
    paymentProcessor.highPriorityQueue.pause();
    paymentProcessor.normalQueue.pause();

    try {
        await pool.end();
        console.log("✅ Shutdown complete");
        process.exit(0);
    } catch (err) {
        console.error("❌ Database shutdown error:", err);
        process.exit(1);
    }
}

// ==================== HELPERS ====================
function generateMemoId(prefix = 'BET') {
    return `${prefix}-${randomBytes(6).toString('hex').toUpperCase()}`;
}

async function savePendingBet(userId, chatId, gameType, details, lamports, memoId, expiresAt) {
    const res = await pool.query(`
        INSERT INTO bets (
            user_id, chat_id, game_type, bet_details, 
            expected_lamports, memo_id, status, expires_at, fees_paid
        ) VALUES ($1, $2, $3, $4, $5, $6, 'awaiting_payment', $7, $8)
        RETURNING id
    `, [
        String(userId), String(chatId), gameType, details, 
        lamports, memoId, expiresAt, FEE_BUFFER
    ]);
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

async function findBetByMemo(memoId) {
    const res = await pool.query(`
        SELECT * FROM bets 
        WHERE memo_id = $1 AND status = 'awaiting_payment'
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    `, [memoId]);
    return res.rows[0];
}

async function markBetPaid(betId, signature) {
    await pool.query(`
        UPDATE bets
        SET status = 'payment_verified',
            paid_tx_signature = $1,
            processed_at = NOW()
        WHERE id = $2
    `, [signature, betId]);
}

async function linkUserWallet(userId, walletAddress) {
    const cacheKey = `wallet-${userId}`;
    const res = await pool.query(`
        INSERT INTO wallets (user_id, wallet_address)
        VALUES ($1, $2)
        ON CONFLICT (user_id) DO UPDATE SET
            wallet_address = EXCLUDED.wallet_address,
            last_used_at = NOW()
        RETURNING *
    `, [String(userId), walletAddress]);
    
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
        if (Date.now() - timestamp < CACHE_TTL) return wallet;
    }
    
    const res = await pool.query(
        'SELECT wallet_address FROM wallets WHERE user_id = $1',
        [String(userId)]
    );
    const wallet = res.rows[0]?.wallet_address;
    
    if (wallet) {
        walletCache.set(cacheKey, {
            wallet,
            timestamp: Date.now()
        });
    }
    
    return wallet;
}

function findMemoInTx(tx) {
    const MEMO_PROGRAM_ID = 'Memo1UhkJRfHyvLMcVuc6beZNRYqUP2VZwW';
    for (const inst of tx.transaction.message.instructions) {
        const programId = inst.programIdIndex !== undefined 
            ? tx.transaction.message.accountKeys[inst.programIdIndex]?.pubkey?.toBase58()
            : inst.programId?.toBase58();
        
        if (programId === MEMO_PROGRAM_ID && inst.data) {
            const memo = bs58.decode(inst.data).toString();
            if (/^(BET|CF|RA)-[A-F0-9]{12}$/.test(memo)) return memo;
        }
    }
    return null;
}

// ==================== STARTUP ====================
startServer().catch(err => {
    console.error("🔥 Startup failed:", err);
    process.exit(1);
});
