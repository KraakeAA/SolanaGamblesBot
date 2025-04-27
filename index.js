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
    ComputeBudgetProgram,
    AddressLookupTableAccount
} from '@solana/web3.js';
import bs58 from 'bs58';
import * as crypto from 'crypto';
import PQueue from 'p-queue';
import RateLimitedConnection from './lib/solana-connection.js';


const MEMO_PROGRAM_IDS = [
    "Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo",
    "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr"
];


console.log("⏳ Starting Solana Gambles Bot... Checking environment variables...");

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

    if (key === 'RAILWAY_PUBLIC_DOMAIN' && !process.env.RAILWAY_ENVIRONMENT) return;

    if (!process.env[key]) {
        console.error(`❌ Environment variable ${key} is missing.`);
        missingVars = true;
    }
});

if (missingVars) {
    console.error("⚠️ Please set all required environment variables. Exiting.");
    console.error("❌ Exiting due to missing environment variables."); process.exit(1);
}


if (!process.env.FEE_MARGIN) {
    process.env.FEE_MARGIN = '5000';
}
console.log(`ℹ️ Using FEE_MARGIN: ${process.env.FEE_MARGIN} lamports`);


let isFullyInitialized = false;
let server;


const app = express();


app.get('/health', (req, res) => {
  res.status(200).json({
    status: server ? 'ready' : 'starting',
    uptime: process.uptime()
  });
});


app.get('/railway-health', (req, res) => {
    res.status(200).json({
        status: isFullyInitialized ? 'ready' : 'starting',
        version: '2.0.9'
    });
});


app.get('/prestop', (req, res) => {
    console.log('🚪 Received pre-stop signal from Railway, preparing to shutdown gracefully...');
    res.status(200).send('Shutting down');
});




console.log("⚙️ Initializing scalable Solana connection...");


const solanaConnection = new RateLimitedConnection(process.env.RPC_URL, {
    maxConcurrent: 3,
    retryBaseDelay: 600,
    commitment: 'confirmed',
    httpHeaders: {
        'Content-Type': 'application/json',
        'solana-client': `SolanaGamblesBot/2.0.9 (${process.env.RAILWAY_ENVIRONMENT ? 'railway' : 'local'})`
    },
    rateLimitCooloff: 10000,
    disableRetryOnRateLimit: false
});
console.log("✅ Scalable Solana connection initialized");



const messageQueue = new PQueue({
    concurrency: 5,
    timeout: 10000
});
console.log("✅ Message processing queue initialized");


console.log("⚙️ Setting up optimized PostgreSQL Pool...");
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


const performanceMonitor = {
    requests: 0,
    errors: 0,
    startTime: Date.now(),
    logRequest(success) {
        this.requests++;
        if (!success) this.errors++;


        if (this.requests % 50 === 0) {
            const uptime = (Date.now() - this.startTime) / 1000;
            const errorRate = this.requests > 0 ? (this.errors / this.requests * 100).toFixed(1) : 0;
            console.log(`
📊 Performance Metrics:
    - Uptime: ${uptime.toFixed(0)}s
    - Total Requests Handled: ${this.requests}
    - Error Rate: ${errorRate}%
            `);
        }
    }
};


async function initializeDatabase() {
    console.log("⚙️ Initializing Database schema...");
    let client;
    try {
        client = await pool.connect();


        await client.query(`
            CREATE TABLE IF NOT EXISTS bets (
                id SERIAL PRIMARY KEY,                       -- Unique bet identifier
                user_id TEXT NOT NULL,                       -- Telegram User ID
                chat_id TEXT NOT NULL,                       -- Telegram Chat ID
                game_type TEXT NOT NULL,                     -- 'coinflip' or 'race'
                bet_details JSONB,                           -- Game-specific details (choice, horse, odds)
                expected_lamports BIGINT NOT NULL,           -- Amount user should send (in lamports)
                memo_id TEXT UNIQUE NOT NULL,                -- Unique memo for payment tracking
                status TEXT NOT NULL,                        -- Bet status (e.g., 'awaiting_payment', 'completed_win_paid', 'error_...')
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- When the bet was initiated
                expires_at TIMESTAMPTZ NOT NULL,             -- When the payment window closes
                paid_tx_signature TEXT UNIQUE,               -- Signature of the user's payment transaction
                payout_tx_signature TEXT UNIQUE,             -- Signature of the bot's payout transaction (if win)
                processed_at TIMESTAMPTZ,                    -- When the bet was fully resolved
                fees_paid BIGINT,                            -- Estimated fees buffer associated with this bet
                priority INT DEFAULT 0                       -- Priority for processing (higher first)
            );
        `);


        await client.query(`
            CREATE TABLE IF NOT EXISTS wallets (
                user_id TEXT PRIMARY KEY,                         -- Telegram User ID
                wallet_address TEXT NOT NULL,                     -- User's Solana wallet address
                linked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),     -- When the wallet was first linked
                last_used_at TIMESTAMPTZ                          -- When the wallet was last used for a bet/payout
            );
        `);




        await client.query(`ALTER TABLE bets ADD COLUMN IF NOT EXISTS priority INT DEFAULT 0;`);
        await client.query(`ALTER TABLE bets ADD COLUMN IF NOT EXISTS fees_paid BIGINT;`);



        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_status ON bets(status);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_user_id ON bets(user_id);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_expires_at ON bets(expires_at);`);
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_priority ON bets(priority);`);

        await client.query(`
            CREATE UNIQUE INDEX IF NOT EXISTS idx_bets_memo_id
            ON bets (memo_id)
            INCLUDE (status, expected_lamports, expires_at);
        `);




        console.log("✅ Database schema initialized/verified.");
    } catch (err) {
        console.error("❌ Database initialization error:", err);
        throw err;
    } finally {
        if (client) client.release();
    }
}



console.log("⚙️ Initializing Telegram Bot...");
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


bot.on('message', (msg) => {
    messageQueue.add(() => handleMessage(msg))
        .catch(err => {
            console.error("⚠️ Message processing queue error:", err);
            performanceMonitor.logRequest(false);
        });
});



const telegramSendQueue = new PQueue({ concurrency: 1, interval: 1000, intervalCap: 1 });

function safeSendMessage(chatId, message, options = {}) {
    return telegramSendQueue.add(() =>
        bot.sendMessage(chatId, message, options).catch(err => {
            console.error("❌ Telegram send error:", err.message);

        })
    );
}

console.log("✅ Telegram Bot initialized");



app.use(express.json({
    limit: '10kb',
    verify: (req, res, buf) => {
        req.rawBody = buf;
    }
}));


app.get('/', (req, res) => {
    performanceMonitor.logRequest(true);
    res.status(200).json({
        status: 'ok',
        initialized: isFullyInitialized,
        timestamp: new Date().toISOString(),
        version: '2.0.9',
        queueStats: {
            pending: messageQueue.size + paymentProcessor.highPriorityQueue.size + paymentProcessor.normalQueue.size,
            active: messageQueue.pending + paymentProcessor.highPriorityQueue.pending + paymentProcessor.normalQueue.pending
        }
    });
});


const webhookPath = `/bot${process.env.BOT_TOKEN}`;
app.post(webhookPath, (req, res) => {

    messageQueue.add(() => {
        try {
            if (!req.body || typeof req.body !== 'object') {
                console.warn("⚠️ Received invalid webhook request body");
                performanceMonitor.logRequest(false);
                return res.status(400).send('Invalid request body');
            }


            bot.processUpdate(req.body);
            performanceMonitor.logRequest(true);
            res.sendStatus(200);
        } catch (error) {
            console.error("❌ Webhook processing error:", error);
            performanceMonitor.logRequest(false);
            res.status(500).json({
                error: 'Internal server error processing webhook',
                details: error.message
            });

        }
    });
});



const confirmCooldown = new Map();
const cooldownInterval = 3000;


const walletCache = new Map();
const CACHE_TTL = 300000;


const processedSignaturesThisSession = new Set();
const MAX_PROCESSED_SIGNATURES = 10000;


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




function debugInstruction(inst, accountKeys) {
    try {
        const programIdKeyInfo = accountKeys[inst.programIdIndex];

        const programId = programIdKeyInfo?.pubkey ? new PublicKey(programIdKeyInfo.pubkey) :
                          (typeof programIdKeyInfo === 'string' ? new PublicKey(programIdKeyInfo) :
                          (programIdKeyInfo instanceof PublicKey ? programIdKeyInfo : null));

        const accountPubkeys = inst.accounts?.map(idx => {
             const keyInfo = accountKeys[idx];
             return keyInfo?.pubkey ? new PublicKey(keyInfo.pubkey) :
                    (typeof keyInfo === 'string' ? new PublicKey(keyInfo) :
                    (keyInfo instanceof PublicKey ? keyInfo : null));
        }).filter(pk => pk !== null)
          .map(pk => pk.toBase58());

        return {
            programId: programId ? programId.toBase58() : `Invalid Index ${inst.programIdIndex}`,
            data: inst.data ? Buffer.from(inst.data, 'base64').toString('hex') : null,
            accounts: accountPubkeys
        };
    } catch (e) {
        console.error("[DEBUG INSTR HELPER] Error:", e);
        return {
             error: e.message,
             programIdIndex: inst.programIdIndex,
             accountIndices: inst.accounts
        };
    }
}



function generateMemoId(prefix = 'BET') {

    const validPrefixes = ['BET', 'CF', 'RA'];
    if (!validPrefixes.includes(prefix)) {
        prefix = 'BET';
    }
    const randomBytes = crypto.randomBytes(8);
    const hexString = randomBytes.toString('hex').toUpperCase();
    const checksum = crypto.createHash('sha256')
        .update(hexString)
        .digest('hex')
        .slice(-2)
        .toUpperCase();
    return `${prefix}-${hexString}-${checksum}`;
}


function validateOriginalMemoFormat(memo) {

    if (typeof memo !== 'string') return false;
    const parts = memo.split('-');
    if (parts.length !== 3) return false;
    const [prefix, hex, checksum] = parts;
    return (
        ['BET', 'CF', 'RA'].includes(prefix) &&
        hex.length === 16 &&
        /^[A-F0-9]{16}$/.test(hex) &&
        checksum.length === 2 &&
        /^[A-F0-9]{2}$/.test(checksum) &&
        validateMemoChecksum(hex, checksum)
    );
}


function validateMemoChecksum(hex, checksum) {

     const expectedChecksum = crypto.createHash('sha256')
        .update(hex)
        .digest('hex')
        .slice(-2)
        .toUpperCase();
    return expectedChecksum === checksum;
}



function normalizeMemo(rawMemo) {
    if (typeof rawMemo !== 'string') return null;


    let memo = rawMemo
        .trim()
        .replace(/^memo[:=\s]*/i, '')
        .replace(/^text[:=\s]*/i, '')
        .replace(/[\u200B-\u200D\uFEFF]/g, '')
        .replace(/\s+/g, '-')
        .toUpperCase();


    const v1Match = memo.match(/^([A-Z]{2,3})-([A-F0-9]{16})-([A-F0-9]{2})$/);
    if (v1Match) {
        const [_, prefix, hex, checksum] = v1Match;
        if (validateMemoChecksum(hex, checksum)) {
            return `${prefix}-${hex}-${checksum}`;
        }

        const correctedChecksum = crypto.createHash('sha256')
            .update(hex)
            .digest('hex')
            .slice(-2)
            .toUpperCase();
        console.warn(`⚠️ Memo checksum mismatch for V1 format "${memo}". Corrected: ${prefix}-${hex}-${correctedChecksum}`);
        return `${prefix}-${hex}-${correctedChecksum}`;
    }


    if (memo.length >= 6) {

        return memo.replace(/[^A-Z0-9\-]/g, '').slice(0, 64);
    }

    return null;
}



async function findMemoInTx(tx) {
    let foundMemo = null;
    let method = 'none';

    if (!tx?.transaction?.message) {
        console.log("[MEMO DEBUG] Invalid transaction structure");
        return null;
    }


    if (tx.meta?.logMessages) {
        const memoLogPrefix = "Program log: Memo:";
        for (const log of tx.meta.logMessages) {
            if (log.includes(memoLogPrefix)) {

                const rawMemo = log.substring(log.indexOf(memoLogPrefix) + memoLogPrefix.length).trim().replace(/^"+|"+$/g, '');
                const memo = normalizeMemo(rawMemo);
                if (memo) {
                    console.log(`[MEMO DEBUG] Found memo in logs: "${memo}"`);
                    foundMemo = memo;
                    method = 'logs';
                    break;
                }
            }
        }
    }


    if (!foundMemo) {
        const message = tx.transaction.message;
        let accountKeys = [];

        try {

            const mainKeys = (message.accountKeys || []).map(keyInfo => {
                try {
                    if (keyInfo instanceof PublicKey) return keyInfo;
                    if (typeof keyInfo === 'string') return new PublicKey(keyInfo);
                    if (keyInfo?.pubkey) return new PublicKey(keyInfo.pubkey);
                    console.warn("[MEMO DEBUG] Unknown account key format in mainKeys:", keyInfo);
                    return null;
                } catch (e) {

                    return null;
                }
            }).filter(Boolean);

            accountKeys.push(...mainKeys);


            if (message.addressTableLookups && message.addressTableLookups.length > 0) {

                for (const lookup of message.addressTableLookups) {
                     try {
                         const lookupAccountKey = new PublicKey(lookup.accountKey);

                         const lookupTableAccount = await solanaConnection.getAddressLookupTable(lookupAccountKey);

                         if (lookupTableAccount?.value) {

                              const addresses = lookupTableAccount.value.state.addresses || [];
                              accountKeys.push(...addresses);

                         } else {
                             console.warn(`[MEMO DEBUG] Failed to fetch or parse ALT: ${lookup.accountKey}`);
                         }
                     } catch (altError) {
                         console.error(`[MEMO DEBUG] Error fetching/processing ALT ${lookup.accountKey}:`, altError.message);

                     }
                }
            }


        } catch (e) {
            console.error("[MEMO DEBUG] Error resolving account keys section:", e.message);

            accountKeys = [];
        }



        const instructions = [
            ...(message.instructions || []),
            ...(tx.meta?.innerInstructions || []).flatMap(i => i.instructions || [])
        ];

        for (const [i, inst] of instructions.entries()) {
            try {
                if (inst.programIdIndex === undefined || !accountKeys[inst.programIdIndex]) {

                    continue;
                }

                const programKey = accountKeys[inst.programIdIndex];
                const programId = programKey.toBase58();


                if ([
                    SystemProgram.programId.toBase58(),
                    ComputeBudgetProgram.programId.toBase58(),
                    'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA',
                    'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb',
                    '11111111111111111111111111111111'
                ].includes(programId)) {
                    continue;
                }


                let rawData = inst.data;
                if (!rawData) continue;


                let dataStr = '';
                try {

                     if (typeof rawData === 'string') {

                         try {
                             dataStr = Buffer.from(bs58.decode(rawData)).toString('utf8');
                         } catch (bs58Error) {

                             try {
                                 dataStr = Buffer.from(rawData, 'base64').toString('utf8');
                             } catch (b64Error) {

                                 dataStr = rawData;
                             }
                         }
                     } else if (rawData instanceof Uint8Array || Buffer.isBuffer(rawData)) {
                         dataStr = Buffer.from(rawData).toString('utf8');
                     } else {

                         continue;
                     }
                } catch (decodeError) {

                     continue;
                }



                if (MEMO_PROGRAM_IDS.includes(programId)) {
                    const memo = normalizeMemo(dataStr);
                    if (memo) {
                        console.log(`[MEMO DEBUG] Found memo in instruction ${i} (Program: ${programId}): "${memo}"`);
                        foundMemo = memo;
                        method = 'instructions';
                        break;
                    }
                }


                if (!foundMemo && dataStr.length >= 10) {
                    const prefixes = ['CF-', 'RA-', 'BET-'];
                    if (prefixes.some(p => dataStr.startsWith(p))) {
                        const memo = normalizeMemo(dataStr);
                        if (memo) {
                           console.log(`[MEMO DEBUG] Found memo via pattern match in instruction ${i} data: "${memo}"`);
                           foundMemo = memo;
                           method = 'pattern';
                           break;
                        }
                    }
                }
            } catch (e) {
                console.error(`[MEMO DEBUG] Error processing instruction ${i}:`, e?.message || e);
            }
            if (foundMemo) break;
        }
    }


    if (foundMemo) {

        console.log(`[MEMO STATS] ${new Date().toISOString()} | Success: 1 | Method: ${method} | Version: ${tx.transaction.message.version ?? 'legacy'}`);
        return foundMemo;
    } else {

        console.log("[MEMO DEBUG] Exhausted all search methods, no memo found.");
        return null;
    }
}




async function savePendingBet(userId, chatId, gameType, details, lamports, memoId, expiresAt, priority = 0) {


    if (!validateOriginalMemoFormat(memoId)) {
        console.error(`DB: Attempted to save bet with invalid generated memo format: ${memoId}`);
        return { success: false, error: 'Internal error: Invalid memo ID generated' };
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
        console.log(`DB: Saved pending ${gameType} bet ID ${res.rows[0].id} with memo ${memoId}`);
        return { success: true, id: res.rows[0].id };
    } catch (err) {
        console.error('DB Error saving bet:', err.message);

        if (err.code === '23505' && err.constraint === 'bets_memo_id_key') {
            console.warn(`DB: Memo ID collision for ${memoId}. User might be retrying.`);
            return { success: false, error: 'Memo ID already exists. Please try generating the bet again.' };
        }
        return { success: false, error: err.message };
    }
}


async function findBetByMemo(memoId) {


    if (!memoId || typeof memoId !== 'string') {
        return undefined;
    }


    const query = `
        SELECT id, user_id, chat_id, game_type, bet_details, expected_lamports, status, expires_at, fees_paid, priority
        FROM bets
        WHERE memo_id = $1 AND status = 'awaiting_payment'
        ORDER BY priority DESC, created_at ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED;
    `;
    try {
        const res = await pool.query(query, [memoId]);
        return res.rows[0];
    } catch (err) {
        console.error(`DB Error finding bet by memo ${memoId}:`, err.message);
        return undefined;
    }
}


async function markBetPaid(betId, signature) {
    const query = `
        UPDATE bets
        SET status = 'payment_verified',
            paid_tx_signature = $1,
            processed_at = NOW()
        WHERE id = $2 AND status = 'awaiting_payment'
        RETURNING *;
    `;
    try {
        const res = await pool.query(query, [signature, betId]);
        if (res.rowCount === 0) {
            console.warn(`DB: Attempted to mark bet ${betId} as paid, but status was not 'awaiting_payment' or bet not found.`);
            return { success: false, error: 'Bet not found or already processed' };
        }
        console.log(`DB: Marked bet ${betId} as paid with TX ${signature}`);
        return { success: true, bet: res.rows[0] };
    } catch (err) {

        if (err.code === '23505' && err.constraint === 'bets_paid_tx_signature_key') {
            console.warn(`DB: Paid TX Signature ${signature} collision for bet ${betId}. Already processed.`);
            return { success: false, error: 'Transaction signature already recorded' };
        }
        console.error(`DB Error marking bet ${betId} paid:`, err.message);
        return { success: false, error: err.message };
    }
}


async function linkUserWallet(userId, walletAddress) {
    const cacheKey = `wallet-${userId}`;
    const query = `
        INSERT INTO wallets (user_id, wallet_address, last_used_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (user_id)
        DO UPDATE SET
            wallet_address = EXCLUDED.wallet_address,
            last_used_at = NOW()
        RETURNING wallet_address;
    `;
    try {
        const res = await pool.query(query, [String(userId), walletAddress]);
        const linkedWallet = res.rows[0]?.wallet_address;

        if (linkedWallet) {
            console.log(`DB: Linked/Updated wallet for user ${userId} to ${linkedWallet}`);

            walletCache.set(cacheKey, {
                wallet: linkedWallet,
                timestamp: Date.now()
            });
            return { success: true, wallet: linkedWallet };
        } else {
            console.error(`DB: Failed to link/update wallet for user ${userId}. Query returned no address.`);
            return { success: false, error: 'Failed to update wallet in database.' };
        }
    } catch (err) {
        console.error(`DB Error linking wallet for user ${userId}:`, err.message);
        return { success: false, error: err.message };
    }
}


async function getLinkedWallet(userId) {
    const cacheKey = `wallet-${userId}`;


    if (walletCache.has(cacheKey)) {
        const { wallet, timestamp } = walletCache.get(cacheKey);
        if (Date.now() - timestamp < CACHE_TTL) {

            return wallet;
        } else {
            walletCache.delete(cacheKey);
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
        console.error(`DB Error fetching wallet for user ${userId}:`, err.message);
        return undefined;
    }
}


async function updateBetStatus(betId, status) {
    const query = `UPDATE bets SET status = $1, processed_at = CASE WHEN $1 LIKE 'completed_%' OR $1 LIKE 'error_%' THEN NOW() ELSE processed_at END WHERE id = $2 RETURNING id;`;
    try {
        const res = await pool.query(query, [status, betId]);
        if (res.rowCount > 0) {
            console.log(`DB: Updated status for bet ${betId} to ${status}`);
            return true;
        } else {
            console.warn(`DB: Failed to update status for bet ${betId} (not found?)`);
            return false;
        }
    } catch (err) {
        console.error(`DB Error updating bet ${betId} status to ${status}:`, err.message);
        return false;
    }
}


async function recordPayout(betId, status, signature) {
    const query = `
        UPDATE bets
        SET status = $1,
            payout_tx_signature = $2,
            processed_at = NOW()
        WHERE id = $3 AND status = 'processing_payout'
        RETURNING id;
    `;
    try {
        const res = await pool.query(query, [status, signature, betId]);
        if (res.rowCount > 0) {
            console.log(`DB: Recorded payout TX ${signature} for bet ${betId} with status ${status}`);
            return true;
        } else {
            console.warn(`DB: Failed to record payout for bet ${betId} (not found or status mismatch?)`);
            return false;
        }
    } catch (err) {

        if (err.code === '23505' && err.constraint === 'bets_payout_tx_signature_key') {
            console.warn(`DB: Payout TX Signature ${signature} collision for bet ${betId}. Already recorded.`);
            return false;
        }
        console.error(`DB Error recording payout for bet ${betId}:`, err.message);
        return false;
    }
}



function analyzeTransactionAmounts(tx, walletType) {
    let transferAmount = 0n;
    let payerAddress = null;
    const targetAddress = walletType === 'coinflip'
        ? process.env.MAIN_WALLET_ADDRESS
        : process.env.RACE_WALLET_ADDRESS;

    if (tx?.meta?.err) {

        return { transferAmount: 0n, payerAddress: null };
    }


    if (tx?.meta?.preBalances && tx?.meta?.postBalances && tx?.transaction?.message?.accountKeys) {


        const accountKeys = tx.transaction.message.accountKeys.map(k => {
            try {

                return k.pubkey ? new PublicKey(k.pubkey) : new PublicKey(k);
            } catch {
                return null;
            }
        }).filter(Boolean);

        const accountKeyStrings = accountKeys.map(pk => pk.toBase58());



        const targetIndex = accountKeyStrings.indexOf(targetAddress);

        if (targetIndex !== -1 && tx.meta.preBalances[targetIndex] !== undefined && tx.meta.postBalances[targetIndex] !== undefined) {
            const balanceChange = BigInt(tx.meta.postBalances[targetIndex]) - BigInt(tx.meta.preBalances[targetIndex]);
            if (balanceChange > 0n) {
                transferAmount = balanceChange;

                for (let i = 0; i < accountKeyStrings.length; i++) {
                    if (i === targetIndex) continue;

                    if (tx.meta.preBalances[i] === undefined || tx.meta.postBalances[i] === undefined) continue;

                    const payerBalanceChange = BigInt(tx.meta.postBalances[i]) - BigInt(tx.meta.preBalances[i]);

                    if (i === 0 && payerBalanceChange < 0n) {
                        payerAddress = accountKeyStrings[i];
                        break;
                    }


                    const keyInfo = tx.transaction.message.accountKeys[i];

                    const isSigner = keyInfo?.signer === true ||
                                       (tx.transaction.message.header?.numRequiredSignatures > 0 && i < tx.transaction.message.header.numRequiredSignatures);


                    if (isSigner && payerBalanceChange <= -transferAmount) {
                        payerAddress = accountKeyStrings[i];
                        break;
                    }
                }

                if (!payerAddress && accountKeyStrings[0] && tx.meta.preBalances[0] !== undefined && tx.meta.postBalances[0] !== undefined && (BigInt(tx.meta.postBalances[0]) - BigInt(tx.meta.preBalances[0])) < 0n) {
                    payerAddress = accountKeyStrings[0];
                }
            }
        }
    }


    if (transferAmount === 0n && tx?.transaction?.message?.instructions) {
        const instructions = [
            ...(tx.transaction.message.instructions || []),
            ...(tx.meta?.innerInstructions || []).flatMap(i => i.instructions || [])
        ];
        const SYSTEM_PROGRAM_ID = SystemProgram.programId.toBase58();


        const accountKeysForInstr = tx.transaction.message.accountKeys.map(k => {
             try {
                 return k.pubkey ? new PublicKey(k.pubkey) : new PublicKey(k);
             } catch { return null; }
          }).filter(Boolean);
        const accountKeyStringsForInstr = accountKeysForInstr.map(pk => pk.toBase58());


        for (const inst of instructions) {
            let programId = '';

            try {
                if (inst.programIdIndex !== undefined && accountKeyStringsForInstr) {
                     programId = accountKeyStringsForInstr[inst.programIdIndex] || '';
                } else if (inst.programId) {
                     programId = inst.programId.toBase58 ? inst.programId.toBase58() : String(inst.programId);
                }
            } catch { /* Ignore errors getting programId */ }



             if (programId === SYSTEM_PROGRAM_ID && inst.parsed?.type === 'transfer') {
                 const transferInfo = inst.parsed.info;

                 if (transferInfo.destination === targetAddress) {
                     const instructionAmount = BigInt(transferInfo.lamports || transferInfo.amount || 0);
                     if (instructionAmount > 0n) {
                         transferAmount = instructionAmount;
                         payerAddress = transferInfo.source;
                         break;
                     }
                 }
             }
        }
    }


    return { transferAmount, payerAddress };
}


function getPayerFromTransaction(tx) {
    if (!tx || !tx.meta || !tx.transaction?.message?.accountKeys) return null;

    const message = tx.transaction.message;

    if (message.accountKeys.length > 0) {
        const feePayerKeyInfo = message.accountKeys[0];
        let feePayerAddress = null;

         try {
             if (feePayerKeyInfo instanceof PublicKey) {
                 feePayerAddress = feePayerKeyInfo.toBase58();
             } else if (feePayerKeyInfo?.pubkey) {
                 feePayerAddress = new PublicKey(feePayerKeyInfo.pubkey).toBase58();
             } else if (typeof feePayerKeyInfo === 'string') {
                 feePayerAddress = new PublicKey(feePayerKeyInfo).toBase58();
             }
         } catch (e) {
             console.warn("Could not parse fee payer address:", e.message);
         }



        return feePayerAddress ? new PublicKey(feePayerAddress) : null;
    }


    const preBalances = tx.meta.preBalances;
    const postBalances = tx.meta.postBalances;
    if (!preBalances || !postBalances || preBalances.length !== postBalances.length || preBalances.length !== message.accountKeys.length) {
        console.warn("Inconsistent balance/account key data, cannot reliably determine payer by balance change.");
        return null;
    }

    for (let i = 0; i < message.accountKeys.length; i++) {

        const keyInfo = message.accountKeys[i];
         const isSigner = keyInfo?.signer === true ||
                         (message.header?.numRequiredSignatures > 0 && i < message.header.numRequiredSignatures);

        if (isSigner) {
            let key;
            try {

                 if (keyInfo instanceof PublicKey) {
                     key = keyInfo;
                 } else if (keyInfo?.pubkey) {
                     key = new PublicKey(keyInfo.pubkey);
                 } else if (typeof keyInfo === 'string') {
                     key = new PublicKey(keyInfo);
                 } else {
                     continue;
                 }
            } catch (e) { continue; }



               if (preBalances[i] === undefined || postBalances[i] === undefined) continue;
               const balanceDiff = BigInt(preBalances[i]) - BigInt(postBalances[i]);
               if (balanceDiff > 0) {

                   return key;
               }
        }
    }


    return null;
}




function isRetryableError(error) {
    const msg = error?.message?.toLowerCase() || '';
    const code = error?.code?.toLowerCase() || '';


    if (msg.includes('429') ||
        msg.includes('timeout') ||
        msg.includes('timed out') ||
        msg.includes('rate limit') ||
        msg.includes('econnreset') ||
        msg.includes('esockettimedout') ||
        msg.includes('network error') ||
        msg.includes('fetch') ||
        code === 'etimedout')
    {
        return true;
    }


    if (msg.includes('connection terminated') || code === 'econnrefused') {
        return true;
    }

    return false;
}


function extractMemoFromDescription(tx) {
    const desc = tx.meta?.logMessages?.join('\n') || '';

    const memoMatch = desc.match(/instruction: Memo.*?text: "?([^"]+)"?/i);
    if (memoMatch && memoMatch[1]) {
        console.log(`[MEMO DEBUG] Extracted raw memo from description: "${memoMatch[1]}"`);
        return normalizeMemo(memoMatch[1]);
    }

    const simpleMemoMatch = desc.match(/instruction: Memo ([^\n]+)/i);
    if (simpleMemoMatch && simpleMemoMatch[1]){
        console.log(`[MEMO DEBUG] Extracted simple raw memo from description: "${simpleMemoMatch[1]}"`);
        return normalizeMemo(simpleMemoMatch[1]);
    }
    return null;
}



class PaymentProcessor {
    constructor() {

        this.highPriorityQueue = new PQueue({
            concurrency: 3,
        });

        this.normalQueue = new PQueue({
            concurrency: 2,
        });
        this.activeProcesses = new Set();
    }


    async addPaymentJob(job) {

        const queue = (job.priority && job.priority > 0) ? this.highPriorityQueue : this.normalQueue;

        queue.add(() => this.processJob(job)).catch(queueError => {
            console.error(`Queue error processing job ${job.type} (${job.signature || job.betId || 'N/A'}):`, queueError.message);

            performanceMonitor.logRequest(false);
        });
    }


    async processJob(job) {

        const jobIdentifier = job.signature || job.betId;
        if (jobIdentifier && this.activeProcesses.has(jobIdentifier)) {

            return;
        }
        if (jobIdentifier) this.activeProcesses.add(jobIdentifier);

        try {
            let result;

            if (job.type === 'monitor_payment') {
                result = await this._processIncomingPayment(job.signature, job.walletType, job.retries || 0);
            } else if (job.type === 'process_bet') {
                const bet = await pool.query('SELECT * FROM bets WHERE id = $1', [job.betId]).then(res => res.rows[0]);
                if (bet) {
                    await processPaidBet(bet);
                    result = { processed: true };
                } else {
                    console.error(`Cannot process bet: Bet ID ${job.betId} not found.`);
                    result = { processed: false, reason: 'bet_not_found' };
                }
            } else if (job.type === 'payout') {
                await handlePayoutJob(job);
                result = { processed: true };
            } else {
                console.error(`Unknown job type: ${job.type}`);
                result = { processed: false, reason: 'unknown_job_type'};
            }

            performanceMonitor.logRequest(true);
            return result;

        } catch (error) {
            performanceMonitor.logRequest(false);
            console.error(`Error processing job type ${job.type} for identifier ${jobIdentifier || 'N/A'}:`, error.message);


            if (job.type === 'monitor_payment' && (job.retries || 0) < 3 && isRetryableError(error)) {
                job.retries = (job.retries || 0) + 1;
                console.log(`Retrying job for signature ${job.signature} (Attempt ${job.retries})...`);
                await new Promise(resolve => setTimeout(resolve, 1000 * job.retries));

                if (jobIdentifier) this.activeProcesses.delete(jobIdentifier);

                await this.addPaymentJob(job);
                return;
            } else {

                console.error(`Job failed permanently or exceeded retries for identifier: ${jobIdentifier}`, error);

                if(job.betId && job.type !== 'monitor_payment') {
                    await updateBetStatus(job.betId, `error_${job.type}_failed`);
                }
            }
        } finally {

            if (jobIdentifier) {
                this.activeProcesses.delete(jobIdentifier);
            }
        }
    }


    async _processIncomingPayment(signature, walletType, attempt = 0) {

        if (processedSignaturesThisSession.has(signature)) {

            return { processed: false, reason: 'already_processed_session' };
        }


        const checkQuery = `SELECT id FROM bets WHERE paid_tx_signature = $1 LIMIT 1;`;
        try {
            const processed = await pool.query(checkQuery, [signature]);
            if (processed.rowCount > 0) {

                processedSignaturesThisSession.add(signature);
                return { processed: false, reason: 'exists_in_db' };
            }
        } catch (dbError) {
            console.error(`DB Error checking signature ${signature}:`, dbError.message);
            if (isRetryableError(dbError) && attempt < 3) throw dbError;
            return { processed: false, reason: 'db_check_error' };
        }


        console.log(`Processing transaction details for signature: ${signature} (Attempt ${attempt + 1})`);
        let tx;
        const targetAddress = walletType === 'coinflip' ? process.env.MAIN_WALLET_ADDRESS : process.env.RACE_WALLET_ADDRESS;
        try {

            tx = await solanaConnection.getParsedTransaction(
                signature,
                {
                    maxSupportedTransactionVersion: 2,
                    commitment: 'confirmed'
                }
            );

             if (!tx) {
                  console.warn(`[PAYMENT DEBUG] Transaction ${signature} returned null from RPC.`);
                  throw new Error(`Transaction ${signature} not found (null response)`);
             }
            console.log(`[PAYMENT DEBUG] Fetched transaction for signature: ${signature}`);




        } catch (fetchError) {
            console.error(`Failed to fetch TX ${signature}: ${fetchError.message}`);
            if (isRetryableError(fetchError) && attempt < 3) throw fetchError;

            processedSignaturesThisSession.add(signature);
            return { processed: false, reason: 'tx_fetch_failed' };
        }


        if (tx.meta?.err) {
            console.log(`Transaction ${signature} failed on-chain: ${JSON.stringify(tx.meta.err)}`);
            processedSignaturesThisSession.add(signature);
            return { processed: false, reason: 'tx_onchain_error' };
        }



        let memo = await findMemoInTx(tx);
        console.log(`[PAYMENT DEBUG] Memo found by findMemoInTx for TX ${signature}: "${memo || 'null'}"`);


        if (!memo) {


            if (tx.meta?.logMessages?.some(log => log.includes("Program log: Memo") || log.includes("instruction: Memo"))) {
                console.log("[MEMO DEBUG] Primary memo find failed, attempting fallback parsing from logs...");
                const altMemo = extractMemoFromDescription(tx);
                if (altMemo) {
                    console.log(`[MEMO DEBUG] Found fallback memo via description: "${altMemo}"`);
                    memo = altMemo;
                } else {
                    console.log("[MEMO DEBUG] Fallback memo parsing from logs failed.");
                }
            }

            if (!memo) {
                console.log(`Transaction ${signature} did not contain a usable game memo after primary and fallback checks.`);
                processedSignaturesThisSession.add(signature);
                return { processed: false, reason: 'no_valid_memo' };
            }
        }



        const bet = await findBetByMemo(memo);
        console.log(`[PAYMENT DEBUG] Found pending bet ID: ${bet?.id || 'None'} for memo: ${memo}`);
        if (!bet) {
            console.warn(`No matching pending bet found for memo "${memo}" from TX ${signature}. Could be unrelated or already processed.`);
            processedSignaturesThisSession.add(signature);
            return { processed: false, reason: 'no_matching_bet' };
        }
        console.log(`Processing payment TX ${signature} with memo: ${memo} for Bet ID: ${bet.id}`);



        if (bet.status !== 'awaiting_payment') {
            console.warn(`Bet ${bet.id} found for memo ${memo} but status is ${bet.status}, not 'awaiting_payment'.`);
            processedSignaturesThisSession.add(signature);
            return { processed: false, reason: 'bet_already_processed' };
        }


        const { transferAmount, payerAddress } = analyzeTransactionAmounts(tx, walletType);
        if (transferAmount <= 0n) {
            console.warn(`No SOL transfer to target wallet found in TX ${signature} for memo ${memo}.`);
            processedSignaturesThisSession.add(signature);
            return { processed: false, reason: 'no_transfer_found' };
        }


        const expectedAmount = BigInt(bet.expected_lamports);
        const tolerance = BigInt(5000);
        if (transferAmount < (expectedAmount - tolerance) || transferAmount > (expectedAmount + tolerance)) {
            console.warn(`Amount mismatch for bet ${bet.id} (memo ${memo}). Expected ~${expectedAmount}, got ${transferAmount}.`);
            await updateBetStatus(bet.id, 'error_payment_mismatch');
            processedSignaturesThisSession.add(signature);
            await safeSendMessage(bet.chat_id, `⚠️ Payment amount mismatch for bet \`${memo}\`. Expected ${Number(expectedAmount)/LAMPORTS_PER_SOL} SOL, received ${Number(transferAmount)/LAMPORTS_PER_SOL} SOL. Bet cancelled.`, { parse_mode: 'Markdown' }).catch(e => console.error("TG Send Error:", e.message));
            return { processed: false, reason: 'amount_mismatch' };
        }


        const txTime = tx.blockTime ? new Date(tx.blockTime * 1000) : null;
        if (!txTime) {
            console.warn(`Could not determine blockTime for TX ${signature}. Skipping expiry check.`);
        } else if (txTime > new Date(bet.expires_at)) {
            console.warn(`Payment for bet ${bet.id} (memo ${memo}) received after expiry.`);
            await updateBetStatus(bet.id, 'error_payment_expired');
            processedSignaturesThisSession.add(signature);
            await safeSendMessage(bet.chat_id, `⚠️ Payment for bet \`${memo}\` received after expiry time. Bet cancelled.`, { parse_mode: 'Markdown' }).catch(e => console.error("TG Send Error:", e.message));
            return { processed: false, reason: 'expired' };
        }


        const markResult = await markBetPaid(bet.id, signature);
        if (markResult.success) {
            console.log(`[PAYMENT DEBUG] Successfully marked bet ${bet.id} as paid with TX: ${signature}`);
        } else {
            console.error(`Failed to mark bet ${bet.id} as paid: ${markResult.error}`);
             if (markResult.error === 'Transaction signature already recorded' || markResult.error === 'Bet not found or already processed') {
                 processedSignaturesThisSession.add(signature);
                 return { processed: false, reason: 'db_mark_paid_collision' };
             }

             return { processed: false, reason: 'db_mark_paid_failed' };
        }

        processedSignaturesThisSession.add(signature);
         if (processedSignaturesThisSession.size > MAX_PROCESSED_SIGNATURES) {
             console.log('Clearing processed signatures cache (reached max size)');
             processedSignaturesThisSession.clear();
         }


        let actualPayer = payerAddress || getPayerFromTransaction(tx)?.toBase58();
        if (actualPayer) {
            try {
                new PublicKey(actualPayer);
                await linkUserWallet(bet.user_id, actualPayer);
            } catch(e) {
                console.warn(`Identified payer address "${actualPayer}" for bet ${bet.id} is invalid. Cannot link wallet.`);
            }
        } else {
            console.warn(`Could not identify valid payer address for bet ${bet.id} to link wallet.`);
        }


        console.log(`Payment verified for bet ${bet.id}. Queuing for game processing.`);
        await this.addPaymentJob({
            type: 'process_bet',
            betId: bet.id,
            priority: 1,
            signature
        });

        return { processed: true };
    }
}

const paymentProcessor = new PaymentProcessor();



let isMonitorRunning = false;


const botStartupTime = Math.floor(Date.now() / 1000);

let monitorIntervalSeconds = 45;
let monitorInterval = null;


async function monitorPayments() {


    if (isMonitorRunning) {

        return;
    }
    if (!isFullyInitialized) {
        console.log('[Monitor] Skipping cycle, application not fully initialized yet.');
        return;
    }

    isMonitorRunning = true;
    const startTime = Date.now();
    let signaturesFoundThisCycle = 0;
    let signaturesQueuedThisCycle = 0;

    try {

        const currentLoad = paymentProcessor.highPriorityQueue.pending +
                              paymentProcessor.normalQueue.pending +
                              paymentProcessor.highPriorityQueue.size +
                              paymentProcessor.normalQueue.size;
        const baseDelay = 500;
        const delayPerItem = 100;
        const maxThrottleDelay = 10000;
        const throttleDelay = Math.min(maxThrottleDelay, baseDelay + currentLoad * delayPerItem);
        if (throttleDelay > baseDelay) {
            console.log(`[Monitor] Queues have ${currentLoad} pending/active items. Throttling monitor check for ${throttleDelay}ms.`);
            await new Promise(resolve => setTimeout(resolve, throttleDelay));
        } else {
            await new Promise(resolve => setTimeout(resolve, baseDelay));
        }


        await new Promise(resolve => setTimeout(resolve, Math.random() * 2000));


        console.log("⚙️ Performing payment monitor run...");

        const monitoredWallets = [
             { address: process.env.MAIN_WALLET_ADDRESS, type: 'coinflip', priority: 0 },
             { address: process.env.RACE_WALLET_ADDRESS, type: 'race', priority: 0 },
        ];

        for (const wallet of monitoredWallets) {
            const walletAddress = wallet.address;
            let signaturesForWallet = [];

            try {


                const options = { limit: 20 };
                console.log(`[Monitor] Checking ${wallet.type} wallet (${walletAddress}) for latest ${options.limit} signatures.`);

                signaturesForWallet = await solanaConnection.getSignaturesForAddress(
                    new PublicKey(walletAddress),
                    options
                );

                if (!signaturesForWallet || signaturesForWallet.length === 0) {

                    continue;
                }

                signaturesFoundThisCycle += signaturesForWallet.length;


                const recentSignatures = signaturesForWallet.filter(sigInfo => {
                    if (sigInfo.blockTime && sigInfo.blockTime < botStartupTime - 300) {

                        return false;
                    }
                    return true;
                });

                if (recentSignatures.length === 0) {

                     continue;
                }


                recentSignatures.reverse();

                for (const sigInfo of recentSignatures) {

                    if (processedSignaturesThisSession.has(sigInfo.signature)) {

                        continue;
                    }


                    if (paymentProcessor.activeProcesses.has(sigInfo.signature)) {

                        continue;
                    }


                    console.log(`[Monitor] Queuing signature ${sigInfo.signature} for ${wallet.type} wallet.`);
                    signaturesQueuedThisCycle++;
                    await paymentProcessor.addPaymentJob({
                        type: 'monitor_payment',
                        signature: sigInfo.signature,
                        walletType: wallet.type,
                        priority: wallet.priority,
                        retries: 0
                    });
                }

            } catch (error) {

                if (error?.message?.includes('429') || error?.code === 429 || error?.statusCode === 429) {

                    console.warn(`⚠️ Solana RPC 429 - backing off more aggressively...`);
                    monitorIntervalSeconds = Math.min(monitorIntervalSeconds * 2, 300);
                    console.log(`ℹ️ New monitor interval after backoff: ${monitorIntervalSeconds}s`);


                    if (monitorInterval) clearInterval(monitorInterval);
                    monitorInterval = setInterval(() => {
                        monitorPayments().catch(err => console.error('❌ [FATAL MONITOR ERROR in setInterval catch]:', err));
                    }, monitorIntervalSeconds * 1000);
                    await new Promise(resolve => setTimeout(resolve, 15000));
                    isMonitorRunning = false;
                    return;
                } else {
                    console.error(`[Monitor] Error fetching/processing signatures for wallet ${walletAddress}:`, error.message);
                    performanceMonitor.logRequest(false);

                }
            }
        }


        if (monitorIntervalSeconds >= 60) {
            console.warn(`⚠️ Warning: Monitor interval high (${monitorIntervalSeconds}s). RPC may be struggling or backoff active.`);
        }


    } catch (err) {

        console.error('❌ MonitorPayments Error in main block:', err);
        performanceMonitor.logRequest(false);

         if (err?.message?.includes('429') || err?.code === 429 || err?.statusCode === 429) {

             console.warn('⚠️ Solana RPC 429 detected in main block. Backing off more aggressively...');
             monitorIntervalSeconds = Math.min(monitorIntervalSeconds * 2, 300);
             console.log(`ℹ️ New monitor interval after backoff: ${monitorIntervalSeconds}s`);


             if (monitorInterval) clearInterval(monitorInterval);
             monitorInterval = setInterval(() => {
                  monitorPayments().catch(err => console.error('❌ [FATAL MONITOR ERROR in setInterval catch]:', err));
             }, monitorIntervalSeconds * 1000);
             await new Promise(resolve => setTimeout(resolve, 15000));
         }
    } finally {
        isMonitorRunning = false;
        const duration = Date.now() - startTime;
        console.log(`[Monitor] Cycle completed in ${duration}ms. Found ${signaturesFoundThisCycle} total signatures. Queued ${signaturesQueuedThisCycle} new signatures for processing.`);
    }
}



async function sendSol(recipientPublicKey, amountLamports, gameType) {

    const privateKey = gameType === 'coinflip'
        ? process.env.BOT_PRIVATE_KEY
        : process.env.RACE_BOT_PRIVATE_KEY;

    if (!privateKey) {
        console.error(`❌ Cannot send SOL: Missing private key for game type ${gameType}`);
        return { success: false, error: `Missing private key for ${gameType}` };
    }

    let recipientPubKey;
    try {
        recipientPubKey = (typeof recipientPublicKey === 'string')
            ? new PublicKey(recipientPublicKey)
            : recipientPublicKey;
        if (!(recipientPubKey instanceof PublicKey)) throw new Error("Invalid recipient public key type");
    } catch (e) {
        console.error(`❌ Invalid recipient address format: ${recipientPublicKey}`);
        return { success: false, error: `Invalid recipient address: ${e.message}` };
    }


    const amountSOL = Number(amountLamports) / LAMPORTS_PER_SOL;
    console.log(`💸 Attempting to send ${amountSOL.toFixed(6)} SOL to ${recipientPubKey.toBase58()} for ${gameType}`);


    const basePriorityFee = 1000;
    const maxPriorityFee = 1000000;
    const calculatedFee = Math.floor(Number(amountLamports) * PRIORITY_FEE_RATE);
    const priorityFeeMicroLamports = Math.max(basePriorityFee, Math.min(calculatedFee, maxPriorityFee));



    const amountToSend = BigInt(amountLamports);
    if (amountToSend <= 0n) {
        console.error(`❌ Calculated payout amount ${amountLamports} is zero or negative. Cannot send.`);
        return { success: false, error: 'Calculated payout amount is zero or negative' };
    }



    for (let attempt = 1; attempt <= 3; attempt++) {
        try {
            const payerWallet = Keypair.fromSecretKey(bs58.decode(privateKey));


            const latestBlockhash = await solanaConnection.getLatestBlockhash(
                { commitment: 'confirmed' }
            );

            if (!latestBlockhash || !latestBlockhash.blockhash) {
                throw new Error('Failed to get latest blockhash');
            }

            const transaction = new Transaction({
                recentBlockhash: latestBlockhash.blockhash,
                feePayer: payerWallet.publicKey
            });


            transaction.add(
                ComputeBudgetProgram.setComputeUnitPrice({
                    microLamports: priorityFeeMicroLamports
                })
            );


            transaction.add(
                SystemProgram.transfer({
                    fromPubkey: payerWallet.publicKey,
                    toPubkey: recipientPubKey,
                    lamports: amountToSend
                })
            );

            console.log(`Sending TX (Attempt ${attempt})... Amount: ${amountToSend}, Priority Fee: ${priorityFeeMicroLamports} microLamports`);


            const confirmationTimeoutMs = 45000;
            const signature = await Promise.race([
                sendAndConfirmTransaction(
                    solanaConnection,
                    transaction,
                    [payerWallet],
                    {
                        commitment: 'confirmed',
                        skipPreflight: false,
                        maxRetries: 2,
                        preflightCommitment: 'confirmed'
                    }
                ),

                new Promise((_, reject) => {
                    setTimeout(() => {
                        reject(new Error(`Transaction confirmation timeout after ${confirmationTimeoutMs/1000}s (Attempt ${attempt})`));
                    }, confirmationTimeoutMs);
                })
            ]);

            console.log(`✅ Payout successful! Sent ${Number(amountToSend)/LAMPORTS_PER_SOL} SOL to ${recipientPubKey.toBase58()}. TX: ${signature}`);
            return { success: true, signature };

        } catch (error) {
            console.error(`❌ Payout TX failed (Attempt ${attempt}/3):`, error.message);



            const errorMsg = error.message.toLowerCase();
            if (errorMsg.includes('invalid param') ||
                errorMsg.includes('insufficient funds') ||
                errorMsg.includes('blockhash not found') ||
                errorMsg.includes('custom program error') ||
                errorMsg.includes('account not found') ||
                errorMsg.includes('invalid recipient')) {
                console.error("❌ Non-retryable error encountered. Aborting payout.");
                return { success: false, error: `Non-retryable error: ${error.message}` };
            }


            if (attempt < 3) {
                await new Promise(resolve => setTimeout(resolve, 2000 * attempt));
            }
        }
    }


    console.error(`❌ Payout failed after 3 attempts for ${recipientPubKey.toBase58()}`);
    return { success: false, error: `Payout failed after 3 attempts` };
}




async function processPaidBet(bet) {
    console.log(`⚙️ Processing game for bet ${bet.id} (${bet.game_type})`);
    let client;

    try {

        client = await pool.connect();
        await client.query('BEGIN');
        const statusCheck = await client.query(
            'SELECT status FROM bets WHERE id = $1 FOR UPDATE',
            [bet.id]
        );


        if (!statusCheck.rows[0] || statusCheck.rows[0].status !== 'payment_verified') {
            console.warn(`Bet ${bet.id} status is ${statusCheck.rows[0]?.status ?? 'not found'}, not 'payment_verified'. Aborting game processing.`);
            await client.query('ROLLBACK');
            return;
        }


        await client.query(
            'UPDATE bets SET status = $1 WHERE id = $2',
            ['processing_game', bet.id]
        );
        await client.query('COMMIT');


        if (bet.game_type === 'coinflip') {
            await handleCoinflipGame(bet);
        } else if (bet.game_type === 'race') {
            await handleRaceGame(bet);
        } else {
            console.error(`❌ Unknown game type '${bet.game_type}' for bet ${bet.id}`);
            await updateBetStatus(bet.id, 'error_unknown_game');
        }
    } catch (error) {
        console.error(`❌ Error during game processing setup for bet ${bet.id}:`, error.message);
        if (client) {
            try { await client.query('ROLLBACK'); } catch (rbError) { console.error("Rollback failed:", rbError); }
        }
        await updateBetStatus(bet.id, 'error_processing_setup');
    } finally {
        if (client) client.release();
    }
}



async function handleCoinflipGame(bet) {
    const { id: betId, user_id, chat_id, bet_details, expected_lamports } = bet;
    const choice = bet_details.choice;
    const config = GAME_CONFIG.coinflip;


    const houseWinRoll = Math.random();
    let result;
    let isHouseWin = houseWinRoll < config.houseEdge;

    if (isHouseWin) {

        result = (choice === 'heads') ? 'tails' : 'heads';
        console.log(`🪙 Coinflip Bet ${betId}: House edge triggered.`);
    } else {

        result = (Math.random() < 0.5) ? 'heads' : 'tails';
    }


    const win = (result === choice);


    const payoutLamports = win ? calculatePayout(BigInt(expected_lamports), 'coinflip') : 0n;


    let displayName = await getUserDisplayName(chat_id, user_id);

    if (win) {
        const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
        console.log(`🪙 Coinflip Bet ${betId}: ${displayName} WON ${payoutSOL.toFixed(6)} SOL! Choice: ${choice}, Result: ${result}`);


        const winnerAddress = await getLinkedWallet(user_id);
        if (!winnerAddress) {
            console.warn(`Coinflip Bet ${betId}: Winner ${displayName} has no linked wallet.`);
            await safeSendMessage(chat_id,
                `🎉 ${displayName}, you won the coinflip (Result: *${result}*) but have no wallet linked!\n` +
                `Your payout of ${payoutSOL.toFixed(6)} SOL is waiting. Place another bet (any amount) to link your wallet and receive pending payouts.`,
                { parse_mode: 'Markdown' }
            ).catch(e => console.error("TG Send Error:", e.message));
            await updateBetStatus(betId, 'completed_win_no_wallet');
            return;
        }


        try {
            await safeSendMessage(chat_id,
                `🎉 ${displayName}, you won ${payoutSOL.toFixed(6)} SOL!\n` +
                `Result: *${result}*\n\n` +
                `💸 Processing payout to your linked wallet...`,
                { parse_mode: 'Markdown' }
            ).catch(e => console.error("TG Send Error:", e.message));


            await updateBetStatus(betId, 'processing_payout');


            await paymentProcessor.addPaymentJob({
                type: 'payout',
                betId,
                recipient: winnerAddress,
                amount: payoutLamports.toString(),
                gameType: 'coinflip',
                priority: 2,
                chatId: chat_id,
                displayName,
                result
            });

        } catch (e) {
            console.error(`❌ Error queuing payout for coinflip bet ${betId}:`, e);
            await safeSendMessage(chat_id,
                `⚠️ Error occurred while processing your coinflip win for bet ID ${betId}.\n` +
                `Please contact support.`,
                { parse_mode: 'Markdown' }
            ).catch(e => console.error("TG Send Error:", e.message));
            await updateBetStatus(betId, 'error_payout_queueing');
        }

    } else {
        console.log(`🪙 Coinflip Bet ${betId}: ${displayName} LOST. Choice: ${choice}, Result: ${result}`);
        await safeSendMessage(chat_id,
            `❌ ${displayName}, you lost the coinflip!\n` +
            `You guessed *${choice}* but the result was *${result}*. Better luck next time!`,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
        await updateBetStatus(betId, 'completed_loss');
    }
}


async function handleRaceGame(bet) {
    const { id: betId, user_id, chat_id, bet_details, expected_lamports } = bet;
    const chosenHorseName = bet_details.horse;
    const config = GAME_CONFIG.race;


    const horses = [
        { name: 'Yellow', emoji: '🟡', odds: 1.1, baseProb: 0.25 }, { name: 'Orange', emoji: '🟠', odds: 2.0, baseProb: 0.20 },
        { name: 'Blue',   emoji: '🔵', odds: 3.0, baseProb: 0.15 }, { name: 'Cyan',   emoji: '💧', odds: 4.0, baseProb: 0.12 },
        { name: 'White',  emoji: '⚪️', odds: 5.0, baseProb: 0.09 }, { name: 'Red',    emoji: '🔴', odds: 6.0, baseProb: 0.07 },
        { name: 'Black',  emoji: '⚫️', odds: 7.0, baseProb: 0.05 }, { name: 'Pink',   emoji: '🌸', odds: 8.0, baseProb: 0.03 },
        { name: 'Purple', emoji: '🟣', odds: 9.0, baseProb: 0.02 }, { name: 'Green',  emoji: '🟢', odds: 10.0, baseProb: 0.01 },
        { name: 'Silver', emoji: '💎', odds: 15.0, baseProb: 0.01 }
    ];

    const totalBaseProb = horses.reduce((sum, h) => sum + h.baseProb, 0);
    const targetTotalProb = 1.0 - config.houseEdge;


    let winningHorse = null;
    const randomNumber = Math.random();
    let cumulativeProbability = 0;

    if (randomNumber < targetTotalProb) {
        const playerWinRoll = randomNumber / targetTotalProb;
        for (const horse of horses) {

            cumulativeProbability += (horse.baseProb / totalBaseProb);
            if (playerWinRoll <= cumulativeProbability) {
                winningHorse = horse;
                break;
            }
        }

        winningHorse = winningHorse || horses[horses.length - 1];
    } else {

        const houseWinnerIndex = Math.floor(Math.random() * horses.length);
        winningHorse = horses[houseWinnerIndex];
        console.log(`🐎 Race Bet ${betId}: House edge triggered.`);
    }



    let displayName = await getUserDisplayName(chat_id, user_id);
    try {
        await safeSendMessage(chat_id, `🐎 Race ${betId} starting! ${displayName} bet on ${chosenHorseName}!`).catch(e => console.error("TG Send Error:", e.message));
        await new Promise(resolve => setTimeout(resolve, 2000));
        await safeSendMessage(chat_id, "🚦 And they're off!").catch(e => console.error("TG Send Error:", e.message));
        await new Promise(resolve => setTimeout(resolve, 3000));
        await safeSendMessage(chat_id,
            `🏆 The winner is... ${winningHorse.emoji} *${winningHorse.name}*! 🏆`,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
    } catch (e) {
        console.error(`Error sending race commentary for bet ${betId}:`, e);
    }


    const win = (randomNumber < targetTotalProb) && (chosenHorseName.toLowerCase() === winningHorse.name.toLowerCase());
    const payoutLamports = win
        ? calculatePayout(BigInt(expected_lamports), 'race', winningHorse)
        : 0n;

    if (win) {
        const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
        console.log(`🐎 Race Bet ${betId}: ${displayName} WON ${payoutSOL.toFixed(6)} SOL! Horse: ${chosenHorseName}, Winner: ${winningHorse.name}`);


        const winnerAddress = await getLinkedWallet(user_id);
        if (!winnerAddress) {
            console.warn(`Race Bet ${betId}: Winner ${displayName} has no linked wallet.`);
            await safeSendMessage(chat_id,
                `🎉 ${displayName}, your horse *${chosenHorseName}* won the race!\n` +
                `Your payout of ${payoutSOL.toFixed(6)} SOL is waiting. Place another bet (any amount) to link your wallet and receive pending payouts.`,
                { parse_mode: 'Markdown' }
            ).catch(e => console.error("TG Send Error:", e.message));
            await updateBetStatus(betId, 'completed_win_no_wallet');
            return;
        }


        try {
            await safeSendMessage(chat_id,
                `🎉 ${displayName}, your horse *${chosenHorseName}* won!\n` +
                `Payout: ${payoutSOL.toFixed(6)} SOL\n\n` +
                `💸 Processing payout to your linked wallet...`,
                { parse_mode: 'Markdown' }
            ).catch(e => console.error("TG Send Error:", e.message));


            await updateBetStatus(betId, 'processing_payout');


            await paymentProcessor.addPaymentJob({
                type: 'payout',
                betId,
                recipient: winnerAddress,
                amount: payoutLamports.toString(),
                gameType: 'race',
                priority: 2,
                chatId: chat_id,
                displayName,
                horseName: chosenHorseName,
                winningHorse
            });
        } catch (e) {
            console.error(`❌ Error queuing payout for race bet ${betId}:`, e);
            await safeSendMessage(chat_id,
                `⚠️ Error occurred while processing your race win for bet ID ${betId}.\n` +
                `Please contact support.`,
                { parse_mode: 'Markdown' }
            ).catch(e => console.error("TG Send Error:", e.message));
            await updateBetStatus(betId, 'error_payout_queueing');
        }

    } else {
        console.log(`🐎 Race Bet ${betId}: ${displayName} LOST. Horse: ${chosenHorseName}, Winner: ${winningHorse.name}`);
        await safeSendMessage(chat_id,
            `❌ ${displayName}, your horse *${chosenHorseName}* lost the race!\n` +
            `Winner: ${winningHorse.emoji} *${winningHorse.name}*. Better luck next time!`,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
        await updateBetStatus(betId, 'completed_loss');
    }
}



async function handlePayoutJob(job) {
    const { betId, recipient, amount, gameType, chatId, displayName, result, horseName, winningHorse } = job;
    const payoutAmountLamports = BigInt(amount);


    if (payoutAmountLamports <= 0n) {
        console.error(`❌ Payout job for bet ${betId} has zero or negative amount (${payoutAmountLamports}). Skipping send.`);
        await updateBetStatus(betId, 'error_payout_zero_amount');
        await safeSendMessage(chatId,
            `⚠️ There was an issue calculating the payout for bet ID ${betId} (amount was zero). Please contact support.`,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
        return;
    }

    console.log(`💸 Processing payout job for Bet ID: ${betId}, Amount: ${payoutAmountLamports} lamports to ${recipient}`);

    try {

        const sendResult = await sendSol(recipient, payoutAmountLamports, gameType);

        if (sendResult.success) {
            console.log(`💸 Payout successful for bet ${betId}, TX: ${sendResult.signature}`);

            await safeSendMessage(chatId,
                `✅ Payout successful for bet ID \`${betId}\`!\n` +
                `${(Number(payoutAmountLamports)/LAMPORTS_PER_SOL).toFixed(6)} SOL sent.\n` +
                `TX: \`https://solscan.io/tx/${sendResult.signature}\``,
                { parse_mode: 'Markdown', disable_web_page_preview: true }
            ).catch(e => console.error("TG Send Error:", e.message));

            await recordPayout(betId, 'completed_win_paid', sendResult.signature);

        } else {

            console.error(`❌ Payout failed permanently for bet ${betId}: ${sendResult.error}`);
            await safeSendMessage(chatId,
                `⚠️ Payout failed for bet ID \`${betId}\`: ${sendResult.error}\n` +
                `The team has been notified. Please contact support if needed.`,
                { parse_mode: 'Markdown' }
            ).catch(e => console.error("TG Send Error:", e.message));

            await updateBetStatus(betId, 'error_payout_failed');

        }
    } catch (error) {

        console.error(`❌ Unexpected error processing payout job for bet ${betId}:`, error);

        await updateBetStatus(betId, 'error_payout_exception');

        await safeSendMessage(chatId,
            `⚠️ A technical error occurred during payout for bet ID \`${betId}\`.\n` +
            `Please contact support.`,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
    }
}



function calculatePayout(betLamports, gameType, gameDetails = {}) {
    betLamports = BigInt(betLamports);
    let payoutLamports = 0n;

    if (gameType === 'coinflip') {

        const multiplier = 2.0 - GAME_CONFIG.coinflip.houseEdge;
        payoutLamports = BigInt(Math.floor(Number(betLamports) * multiplier));
    } else if (gameType === 'race' && gameDetails.odds) {

        const multiplier = gameDetails.odds * (1.0 - GAME_CONFIG.race.houseEdge);
        payoutLamports = BigInt(Math.floor(Number(betLamports) * multiplier));
    } else {
        console.error(`Cannot calculate payout: Unknown game type ${gameType} or missing race odds.`);
        return 0n;
    }


    return payoutLamports > 0n ? payoutLamports : 0n;
}


async function getUserDisplayName(chat_id, user_id) {
    try {
        const chatMember = await bot.getChatMember(chat_id, user_id);
        const username = chatMember.user.username;
        if (username && /^[a-zA-Z0-9_]{5,32}$/.test(username)) {
            return `@${username}`;
        }
        const firstName = chatMember.user.first_name;
        if (firstName) {
            return firstName.replace(/</g, '&lt;').replace(/>/g, '&gt;');
        }
        return `User ${String(user_id).substring(0, 6)}...`;
    } catch (e) {

        if (e.response && e.response.statusCode === 400 && e.response.body?.description?.includes('user not found')) {
             console.warn(`User ${user_id} not found in chat ${chat_id}.`);
        } else {
             console.warn(`Couldn't get username/name for user ${user_id} in chat ${chat_id}:`, e.message);
        }
        return `User ${String(user_id).substring(0, 6)}...`;
    }
}




bot.on('polling_error', (error) => {
    console.error(`❌ Polling error: ${error.code} - ${error.message}`);
    if (error.code === 'ETELEGRAM' && error.message.includes('409 Conflict')) {
        console.error("❌❌❌ FATAL: Conflict detected! Another bot instance might be running with the same token. Exiting.");
        console.error("❌ Exiting due to conflict."); process.exit(1);
    }

});


bot.on('error', (error) => {
    console.error('❌ General Bot Error:', error);
    performanceMonitor.logRequest(false);
});



async function handleMessage(msg) {
    if (!msg || !msg.from || msg.from.is_bot || !msg.chat || !msg.text) {
        return;
    }

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const messageText = msg.text;

    try {

        const now = Date.now();
        if (confirmCooldown.has(userId)) {
            const lastTime = confirmCooldown.get(userId);
            if (now - lastTime < cooldownInterval) {

                return;
            }
        }

        if (messageText.startsWith('/')) {
            confirmCooldown.set(userId, now);
        }


        const text = messageText.trim();
        const commandMatch = text.match(/^\/(\w+)(?:@\w+)?(?:\s+(.*))?$/);

        if (!commandMatch) return;

        const command = commandMatch[1].toLowerCase();
        const args = commandMatch[2] || '';



        switch (command) {
            case 'start':
                await handleStartCommand(msg);
                break;
            case 'coinflip':
                await handleCoinflipCommand(msg);
                break;
            case 'wallet':
                await handleWalletCommand(msg);
                break;
            case 'bet':
                await handleBetCommand(msg, args);
                break;
            case 'race':
                await handleRaceCommand(msg);
                break;
            case 'betrace':
                await handleBetRaceCommand(msg, args);
                break;
            case 'help':
                await handleHelpCommand(msg);
                break;
            default:

                 break;
        }

        performanceMonitor.logRequest(true);

    } catch (error) {
        console.error(`❌ Error processing message from user ${userId} in chat ${chatId}: "${messageText}"`, error);
        performanceMonitor.logRequest(false);
        try {
            await safeSendMessage(chatId, "⚠️ An unexpected error occurred. Please try again later or contact support.");
        } catch (tgError) {
            console.error("❌ Failed to send error message to chat:", tgError.message);
        }
    } finally {

        const cutoff = Date.now() - 60000;
        for (const [key, timestamp] of confirmCooldown.entries()) {
            if (timestamp < cutoff) {
                confirmCooldown.delete(key);
            }
        }
    }
}


async function handleStartCommand(msg) {
    const firstName = msg.from.first_name || 'there';
    const sanitizedFirstName = firstName.replace(/</g, '&lt;').replace(/>/g, '&gt;');
    const welcomeText = `👋 Welcome, ${sanitizedFirstName}!\n\n` +
                          `🎰 *Solana Gambles Bot*\n\n` +
                          `Use /coinflip or /race to see game options.\n` +
                          `Use /wallet to view your linked Solana wallet.\n` +
                          `Use /help to see all commands.`;
    const bannerUrl = 'https://i.ibb.co/9vDo58q/banner.gif';

    try {

        await bot.sendAnimation(msg.chat.id, bannerUrl, {
            caption: welcomeText,
            parse_mode: 'Markdown'
        }).catch(async (animError) => {

             console.warn("⚠️ Failed to send start animation, sending text fallback:", animError.message);
             await safeSendMessage(msg.chat.id, welcomeText, { parse_mode: 'Markdown' });
        });
    } catch (fallbackError) {

         console.error("TG Send Error (start fallback):", fallbackError.message);
    }
}



async function handleCoinflipCommand(msg) {

    try {
        const config = GAME_CONFIG.coinflip;


        const messageText = `🪙 <b>Coinflip Game</b> 🪙\n\n` +
              `Bet on Heads or Tails!\n\n` +
              `<b>How to play:</b>\n` +
              `1. Type <code>/bet amount heads</code> (e.g., <code>/bet 0.1 heads</code>)\n` +
              `2. Type <code>/bet amount tails</code> (e.g., <code>/bet 0.1 tails</code>)\n\n` +
              `<b>Rules:</b>\n` +
              `- Min Bet: ${config.minBet} SOL\n` +
              `- Max Bet: ${config.maxBet} SOL\n` +
              `- House Edge: ${(config.houseEdge * 100).toFixed(1)}%\n` +
              `- Payout: ~${(2.0 * (1.0 - config.houseEdge)).toFixed(2)}x (Win Amount = Bet * ${(2.0 * (1.0 - config.houseEdge)).toFixed(2)}x)\n\n` +
              `You will be given a wallet address and a <b>unique Memo ID</b>. Send the <b>exact</b> SOL amount with the memo to place your bet.`;


        await safeSendMessage(msg.chat.id, messageText, { parse_mode: 'HTML' })
            .catch(e => {
                console.error("TG Send Error (HTML - within handleCoinflipCommand catch):", e.message);
            });

    } catch (error) {
        console.error("Error INSIDE handleCoinflipCommand:", error);
    }
}

async function handleRaceCommand(msg) {
    const horses = [
        { name: 'Yellow', emoji: '🟡', odds: 1.1 }, { name: 'Orange', emoji: '🟠', odds: 2.0 },
        { name: 'Blue',   emoji: '🔵', odds: 3.0 }, { name: 'Cyan',   emoji: '💧', odds: 4.0 },
        { name: 'White',  emoji: '⚪️', odds: 5.0 }, { name: 'Red',    emoji: '🔴', odds: 6.0 },
        { name: 'Black',  emoji: '⚫️', odds: 7.0 }, { name: 'Pink',   emoji: '🌸', odds: 8.0 },
        { name: 'Purple', emoji: '🟣', odds: 9.0 }, { name: 'Green',  emoji: '🟢', odds: 10.0 },
        { name: 'Silver', emoji: '💎', odds: 15.0 }
    ];

    let raceMessage = `🐎 *Horse Race Game* 🐎\n\nBet on the winning horse!\n\n*Available Horses & Approx Payout (Bet x Odds After House Edge):*\n`;
    horses.forEach(horse => {
        const effectiveMultiplier = (horse.odds * (1.0 - GAME_CONFIG.race.houseEdge)).toFixed(2);
        raceMessage += `- ${horse.emoji} *${horse.name}* (~${effectiveMultiplier}x Payout)\n`;
    });

    const config = GAME_CONFIG.race;
    raceMessage += `\n*How to play:*\n` +
                   `1. Type \`/betrace amount horse_name\`\n` +
                   `   (e.g., \`/betrace 0.1 Yellow\`)\n\n` +
                   `*Rules:*\n` +
                   `- Min Bet: ${config.minBet} SOL\n` +
                   `- Max Bet: ${config.maxBet} SOL\n` +
                   `- House Edge: ${(config.houseEdge * 100).toFixed(1)}% (applied to winnings)\n\n` +
                   `You will be given a wallet address and a *unique Memo ID*. Send the *exact* SOL amount with the memo to place your bet.`;

    await safeSendMessage(msg.chat.id, raceMessage, { parse_mode: 'Markdown' }).catch(e => console.error("TG Send Error:", e.message));
}


async function handleWalletCommand(msg) {
    const userId = String(msg.from.id);
    const walletAddress = await getLinkedWallet(userId);

    if (walletAddress) {
        await safeSendMessage(msg.chat.id,
            `🔗 Your linked Solana wallet:\n\`${walletAddress}\`\n\n`+
            `Payouts will be sent here. It's linked automatically when you make your first paid bet.`,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
    } else {
        await safeSendMessage(msg.chat.id,
            `🔗 No wallet linked yet.\n` +
            `Place a bet and send the required SOL. Your sending wallet will be automatically linked for future payouts.`
        ).catch(e => console.error("TG Send Error:", e.message));
    }
}


async function handleBetCommand(msg, args) {
    const match = args.trim().match(/^(\d+\.?\d*)\s+(heads|tails)/i);
    if (!match) {
        await safeSendMessage(msg.chat.id,
            `⚠️ Invalid format. Use: \`/bet <amount> <heads|tails>\`\n` +
            `Example: \`/bet 0.1 heads\``,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
        return;
    }

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const config = GAME_CONFIG.coinflip;


    const betAmount = parseFloat(match[1]);
    if (isNaN(betAmount) || betAmount < config.minBet || betAmount > config.maxBet) {
        await safeSendMessage(chatId,
            `⚠️ Invalid bet amount. Please bet between ${config.minBet} and ${config.maxBet} SOL.\n` +
            `Example: \`/bet 0.1 heads\``,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
        return;
    }

    const userChoice = match[2].toLowerCase();
    const memoId = generateMemoId('CF');
    const expectedLamports = BigInt(Math.round(betAmount * LAMPORTS_PER_SOL));
    const expiresAt = new Date(Date.now() + config.expiryMinutes * 60 * 1000);


    const saveResult = await savePendingBet(
        userId, chatId, 'coinflip',
        { choice: userChoice },
        expectedLamports, memoId, expiresAt
    );

    if (!saveResult.success) {
        await safeSendMessage(chatId, `⚠️ Error registering bet: ${saveResult.error}. Please try the command again.`).catch(e => console.error("TG Send Error:", e.message));
        return;
    }


    await safeSendMessage(chatId,
        `✅ Coinflip bet registered! (ID: \`${memoId}\`)\n\n` +
        `You chose: *${userChoice}*\n` +
        `Amount: *${betAmount.toFixed(Math.max(2, (betAmount.toString().split('.')[1] || '').length))} SOL*\n\n` +
        `➡️ Send *exactly ${betAmount.toFixed(Math.max(2, (betAmount.toString().split('.')[1] || '').length))} SOL* to:\n` +
        `\`${process.env.MAIN_WALLET_ADDRESS}\`\n\n` +
        `📎 *Include MEMO:* \`${memoId}\`\n\n` +
        `⏱️ This request expires in ${config.expiryMinutes} minutes.\n\n` +
        `*IMPORTANT:* Send from your own wallet. Do not send from an exchange. Ensure you include the memo correctly.`,
        { parse_mode: 'Markdown', disable_web_page_preview: true }
    ).catch(e => console.error("TG Send Error:", e.message));
}



async function handleBetRaceCommand(msg, args) {
    const match = args.trim().match(/^(\d+\.?\d*)\s+(\w+)/i);
    if (!match) {
        await safeSendMessage(msg.chat.id,
            `⚠️ Invalid format. Use: \`/betrace <amount> <horse_name>\`\n`+
            `Example: \`/betrace 0.1 Yellow\``,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
        return;
    }

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const config = GAME_CONFIG.race;


    const betAmount = parseFloat(match[1]);
    if (isNaN(betAmount) || betAmount < config.minBet || betAmount > config.maxBet) {
        await safeSendMessage(chatId,
            `⚠️ Invalid bet amount. Please bet between ${config.minBet} and ${config.maxBet} SOL.\n`+
            `Example: \`/betrace 0.1 Yellow\``,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
        return;
    }


    const chosenHorseNameInput = match[2];
    const horses = [
        { name: 'Yellow', emoji: '🟡', odds: 1.1 }, { name: 'Orange', emoji: '🟠', odds: 2.0 },
        { name: 'Blue',   emoji: '🔵', odds: 3.0 }, { name: 'Cyan',   emoji: '💧', odds: 4.0 },
        { name: 'White',  emoji: '⚪️', odds: 5.0 }, { name: 'Red',    emoji: '🔴', odds: 6.0 },
        { name: 'Black',  emoji: '⚫️', odds: 7.0 }, { name: 'Pink',   emoji: '🌸', odds: 8.0 },
        { name: 'Purple', emoji: '🟣', odds: 9.0 }, { name: 'Green',  emoji: '🟢', odds: 10.0 },
        { name: 'Silver', emoji: '💎', odds: 15.0 }
    ];
    const chosenHorse = horses.find(h => h.name.toLowerCase() === chosenHorseNameInput.toLowerCase());

    if (!chosenHorse) {
        await safeSendMessage(chatId,
            `⚠️ Invalid horse name: "${chosenHorseNameInput}". Please choose from the list in /race.`
        ).catch(e => console.error("TG Send Error:", e.message));
        return;
    }

    const memoId = generateMemoId('RA');
    const expectedLamports = BigInt(Math.round(betAmount * LAMPORTS_PER_SOL));
    const expiresAt = new Date(Date.now() + config.expiryMinutes * 60 * 1000);


    const saveResult = await savePendingBet(
        userId, chatId, 'race',
        { horse: chosenHorse.name, odds: chosenHorse.odds },
        expectedLamports, memoId, expiresAt
    );

    if (!saveResult.success) {
        await safeSendMessage(chatId, `⚠️ Error registering bet: ${saveResult.error}. Please try the command again.`).catch(e => console.error("TG Send Error:", e.message));
        return;
    }


    const potentialPayoutLamports = calculatePayout(expectedLamports, 'race', chosenHorse);
    const potentialPayoutSOL = (Number(potentialPayoutLamports) / LAMPORTS_PER_SOL).toFixed(6);


    await safeSendMessage(chatId,
        `✅ Race bet registered! (ID: \`${memoId}\`)\n\n` +
        `You chose: ${chosenHorse.emoji} *${chosenHorse.name}*\n` +
        `Amount: *${betAmount.toFixed(Math.max(2, (betAmount.toString().split('.')[1] || '').length))} SOL*\n` +
        `Potential Payout: ~${potentialPayoutSOL} SOL\n\n`+
        `➡️ Send *exactly ${betAmount.toFixed(Math.max(2, (betAmount.toString().split('.')[1] || '').length))} SOL* to:\n` +
        `\`${process.env.RACE_WALLET_ADDRESS}\`\n\n` +
        `📎 *Include MEMO:* \`${memoId}\`\n\n` +
        `⏱️ This request expires in ${config.expiryMinutes} minutes.\n\n` +
        `*IMPORTANT:* Send from your own wallet. Do not send from an exchange. Ensure you include the memo correctly.`,
        { parse_mode: 'Markdown', disable_web_page_preview: true }
    ).catch(e => console.error("TG Send Error:", e.message));
}


async function handleHelpCommand(msg) {
    const helpText = `*Solana Gambles Bot Commands* 🎰\n\n` +
                       `/start - Show welcome message\n` +
                       `/help - Show this help message\n\n` +
                       `*Games:*\n` +
                       `/coinflip - Show Coinflip game info & how to bet\n` +
                       `/race - Show Horse Race game info & how to bet\n\n` +
                       `*Betting:*\n` +
                       `/bet <amount> <heads|tails> - Place a Coinflip bet\n` +
                       `/betrace <amount> <horse_name> - Place a Race bet\n\n` +
                       `*Wallet:*\n` +
                       `/wallet - View your linked Solana wallet for payouts\n\n` +
                       `*Support:* If you encounter issues, please contact support.`;

    await safeSendMessage(msg.chat.id, helpText, { parse_mode: 'Markdown' }).catch(e => console.error("TG Send Error:", e.message));
}




async function setupTelegramWebhook() {
    if (process.env.RAILWAY_ENVIRONMENT && process.env.RAILWAY_PUBLIC_DOMAIN) {
        const webhookUrl = `https://${process.env.RAILWAY_PUBLIC_DOMAIN}${webhookPath}`;
        console.log(`Attempting to set webhook to: ${webhookUrl}`);
        let attempts = 0;
        while (attempts < 3) {
            try {
                await bot.deleteWebHook({ drop_pending_updates: true });
                await bot.setWebHook(webhookUrl, { max_connections: 3 });
                console.log(`✅ Webhook successfully set to: ${webhookUrl}`);
                return true;
            } catch (webhookError) {
                attempts++;
                console.error(`❌ Webhook setup attempt ${attempts} failed:`, webhookError.message);
                if (attempts >= 3) {
                    console.error("❌ Max webhook setup attempts reached. Continuing without webhook.");
                    return false;
                }
                await new Promise(resolve => setTimeout(resolve, 3000 * attempts));
            }
        }
    } else {
        console.log("ℹ️ Not in Railway environment or domain not set, webhook not configured.");
        return false;
    }
     return false;
}


async function startPollingIfNeeded() {
    try {
        const info = await bot.getWebHookInfo();
        if (!info || !info.url) {

             if (bot.isPolling()) {
                 console.log("ℹ️ Bot is already polling.");
                 return;
             }
            console.log("ℹ️ Webhook not set, starting bot polling...");
            await bot.deleteWebHook({ drop_pending_updates: true });
            await bot.startPolling({ polling: { interval: 300 } });
            console.log("✅ Bot polling started successfully");
        } else {
            console.log(`ℹ️ Webhook is set (${info.url}), polling will not be started.`);

             if (bot.isPolling()) {
                 console.log("ℹ️ Stopping polling because webhook is set.");
                 await bot.stopPolling({ cancel: true });
             }
        }
    } catch (err) {
        console.error("❌ Error managing polling state:", err.message);
        if (err.message.includes('409 Conflict')) {
            console.error("❌❌❌ Conflict detected! Another instance might be polling.");
            console.error("❌ Exiting due to conflict."); process.exit(1);
        }

    }
}


function startPaymentMonitor() {
    if (monitorInterval) {
         console.log("ℹ️ Payment monitor already running.");
         return;
    }
    console.log(`⚙️ Starting payment monitor (Initial Interval: ${monitorIntervalSeconds}s)`);
    monitorInterval = setInterval(() => {
         try {
             monitorPayments().catch(err => {
                 console.error('[FATAL MONITOR ERROR in setInterval catch]:', err);
                 performanceMonitor.logRequest(false);
             });
         } catch (syncErr) {
             console.error('[FATAL MONITOR SYNC ERROR in setInterval try/catch]:', syncErr);
             performanceMonitor.logRequest(false);
         }
    }, monitorIntervalSeconds * 1000);


    setTimeout(() => {
        console.log("⚙️ Performing initial payment monitor run...");
        try {
             monitorPayments().catch(err => {
                 console.error('[FATAL MONITOR ERROR in initial run catch]:', err);
                 performanceMonitor.logRequest(false);
             });
        } catch (syncErr) {
             console.error('[FATAL MONITOR SYNC ERROR in initial run try/catch]:', syncErr);
             performanceMonitor.logRequest(false);
        }
    }, 5000);
}



const shutdown = async (signal, isRailwayRotation = false) => {
    console.log(`\n🛑 ${signal} received, ${isRailwayRotation ? 'container rotation' : 'shutting down gracefully'}...`);

    if (isRailwayRotation) {

        console.log("Railway container rotation detected - minimizing disruption.");
        if (server) {

            server.close(() => console.log("- Stopped accepting new server connections for rotation."));
        }
        return;
    }


    console.log("Performing full graceful shutdown sequence...");
    isMonitorRunning = true;


    console.log("Stopping incoming connections and tasks...");
    if (monitorInterval) {
        clearInterval(monitorInterval);
        console.log("- Stopped payment monitor interval.");
    }
    try {

        if (server) {
             await new Promise((resolve, reject) => {
                 server.close((err) => {
                     if (err) {
                         console.error("⚠️ Error closing Express server:", err.message);
                         return reject(err);
                     }
                     console.log("- Express server closed.");
                     resolve(undefined);
                 });

                 setTimeout(() => reject(new Error('Server close timeout')), 5000).unref();
             });
        }


        try {
            const webhookInfo = await bot.getWebHookInfo();
            if (webhookInfo && webhookInfo.url) {
                await bot.deleteWebHook({ drop_pending_updates: true });
                console.log("- Removed Telegram webhook.");
            }
        } catch (whErr) {
            console.error("⚠️ Error removing webhook during shutdown:", whErr.message);
        }


        if (bot.isPolling()) {
            await bot.stopPolling({ cancel: true });
            console.log("- Stopped Telegram polling.");
        }
    } catch (e) {
        console.error("⚠️ Error stopping listeners/server:", e.message);
    }


    console.log("Waiting for active jobs to finish (max 15s)...");
    try {
        await Promise.race([
            Promise.all([
                messageQueue.onIdle(),
                paymentProcessor.highPriorityQueue.onIdle(),
                paymentProcessor.normalQueue.onIdle(),
                telegramSendQueue.onIdle()
            ]),
            new Promise((_, reject) => setTimeout(() => reject(new Error('Queue drain timeout (15s)')), 15000))
        ]);
        console.log("- All processing queues are idle.");
    } catch (queueError) {
        console.warn("⚠️ Timed out waiting for queues or queue error during shutdown:", queueError.message);
    }


    console.log("Closing database pool...");
    try {
        await pool.end();
        console.log("✅ Database pool closed.");
    } catch (dbErr) {
        console.error("❌ Error closing database pool:", dbErr);
    } finally {
        console.log("🛑 Full Shutdown complete.");
        process.exit(0);
    }

};



process.on('SIGTERM', () => {
    const isRailwayRotation = !!process.env.RAILWAY_ENVIRONMENT;
    console.log(`SIGTERM received. Railway Environment: ${isRailwayRotation}`);
    shutdown('SIGTERM', isRailwayRotation).catch((err) => {
        console.error("Error during SIGTERM shutdown:", err);
        process.exit(1);
    });
});

process.on('SIGINT', () => {
    console.log(`SIGINT received.`);
    shutdown('SIGINT', false).catch((err) => {
        console.error("Error during SIGINT shutdown:", err);
        process.exit(1);
    });
});


process.on('uncaughtException', (err, origin) => {
    console.error(`🔥🔥🔥 Uncaught Exception at: ${origin}`, err);

    shutdown('UNCAUGHT_EXCEPTION', false).catch(() => process.exit(1));
    setTimeout(() => {
        console.error("Shutdown timed out after uncaught exception. Forcing exit.");
        process.exit(1);
    }, 12000).unref();
});


process.on('unhandledRejection', (reason, promise) => {
    console.error('🔥🔥🔥 Unhandled Rejection at:', promise, 'reason:', reason);


});


const PORT = process.env.PORT || 3000;


server = app.listen(PORT, "0.0.0.0", () => {
    console.log(`🚀 Server running on port ${PORT}`);



    setTimeout(async () => {
        console.log("⚙️ Starting delayed background initialization...");
        try {

            console.log("  - Initializing Database...");
            await initializeDatabase();
            console.log("  - Setting up Telegram...");
            const webhookSet = await setupTelegramWebhook();
            if (!webhookSet) {
                await startPollingIfNeeded();
            }
            console.log("  - Starting Payment Monitor...");
            startPaymentMonitor();


             setTimeout(() => {
                  if (solanaConnection && solanaConnection.options) {
                       console.log("⚡ Adjusting Solana connection concurrency...");
                       solanaConnection.options.maxConcurrent = 3;
                       console.log("✅ Solana maxConcurrent adjusted to 3");
                  }
             }, 20000);

            isFullyInitialized = true;
            console.log("✅ Delayed Background Initialization Complete. Bot is fully ready.");
            console.log("🚀🚀🚀 Solana Gambles Bot is up and running! 🚀🚀🚀");

        } catch (initError) {
            console.error("🔥🔥🔥 Delayed Background Initialization Error:", initError);


            console.error("❌ Exiting due to critical initialization failure.");
            await shutdown('INITIALIZATION_FAILURE', false).catch(() => process.exit(1));

             setTimeout(() => {
                  console.error("Shutdown timed out after initialization failure. Forcing exit.");
                  process.exit(1);
             }, 10000).unref();
        }
    }, 1000);
});


server.on('error', (err) => {
    console.error('❌ Server startup error:', err);
    if (err.code === 'EADDRINUSE') {
        console.error(`❌❌❌ Port ${PORT} is already in use. Exiting.`);
    }
    process.exit(1);
});
