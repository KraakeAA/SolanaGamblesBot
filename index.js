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
import { toByteArray, fromByteArray } from 'base64-js'; // Added for Base64 decoding
import { Buffer } from 'buffer'; // Added for Buffer operations

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
        version: '2.0.9' // Consider updating this if appropriate
    });
});


app.get('/prestop', (req, res) => {
    console.log('🚪 Received pre-stop signal from Railway, preparing to shutdown gracefully...');
    res.status(200).send('Shutting down');
});




console.log("⚙️ Initializing scalable Solana connection...");


const solanaConnection = new RateLimitedConnection(process.env.RPC_URL, {
    maxConcurrent: 2,
    retryBaseDelay: 600,
    commitment: 'confirmed',
    httpHeaders: {
        'Content-Type': 'application/json',
        'solana-client': `SolanaGamblesBot/2.0.9 (${process.env.RAILWAY_ENVIRONMENT ? 'railway' : 'local'})` // Consider updating version
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
                id SERIAL PRIMARY KEY,                      -- Unique bet identifier
                user_id TEXT NOT NULL,                      -- Telegram User ID
                chat_id TEXT NOT NULL,                      -- Telegram Chat ID
                game_type TEXT NOT NULL,                    -- 'coinflip' or 'race'
                bet_details JSONB,                          -- Game-specific details (choice, horse, odds)
                expected_lamports BIGINT NOT NULL,          -- Amount user should send (in lamports)
                memo_id TEXT UNIQUE NOT NULL,               -- Unique memo for payment tracking
                status TEXT NOT NULL,                       -- Bet status (e.g., 'awaiting_payment', 'completed_win_paid', 'error_...')
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- When the bet was initiated
                expires_at TIMESTAMPTZ NOT NULL,            -- When the payment window closes
                paid_tx_signature TEXT UNIQUE,              -- Signature of the user's payment transaction
                payout_tx_signature TEXT UNIQUE,            -- Signature of the bot's payout transaction (if win)
                processed_at TIMESTAMPTZ,                   -- When the bet was fully resolved
                fees_paid BIGINT,                           -- Estimated fees buffer associated with this bet
                priority INT DEFAULT 0                      -- Priority for processing (higher first)
            );
        `);


        await client.query(`
            CREATE TABLE IF NOT EXISTS wallets (
                user_id TEXT PRIMARY KEY,                      -- Telegram User ID
                wallet_address TEXT NOT NULL,                  -- User's Solana wallet address
                linked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),  -- When the wallet was first linked
                last_used_at TIMESTAMPTZ                       -- When the wallet was last used for a bet/payout
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
             // Optional: Add more handling here if needed
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
        version: '2.0.9', // Consider updating this if appropriate
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
             // Consider adding more specific error handling
        }
    });
});



const confirmCooldown = new Map();
const cooldownInterval = 3000;


const walletCache = new Map();
const CACHE_TTL = 300000; // 5 minutes


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

const PRIORITY_FEE_RATE = 0.0001; // Example: 0.01% of amount


// Helper function to decode instruction data (Base64 or raw buffer)
const decodeInstructionData = (data) => {
    if (!data) return null;
    try {
        // Check if it's a base64 string
        if (typeof data === 'string') {
             // Base64 standard requires padding, but some RPCs might omit it
             let paddedData = data;
             while (paddedData.length % 4 !== 0) {
                 paddedData += '=';
             }
            return Buffer.from(toByteArray(paddedData)).toString('utf8');
        }
        // Assume it's already a buffer-like object (Uint8Array, Buffer)
        return Buffer.from(data).toString('utf8');
    } catch (e) {
        // console.warn("Failed to decode instruction data:", e.message, "Input:", data);
        return null; // Return null if decoding fails
    }
};


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

        // Use the new decodeInstructionData helper
        const decodedData = decodeInstructionData(inst.data);

        return {
            programId: programId ? programId.toBase58() : `Invalid Index ${inst.programIdIndex}`,
            // data: inst.data ? Buffer.from(inst.data, 'base64').toString('hex') : null, // Original hex data
            decodedData: decodedData, // Add decoded data if available
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
     // Ensure prefix is valid or default
    const validPrefixes = ['BET', 'CF', 'RA'];
    if (!validPrefixes.includes(prefix)) {
        prefix = 'BET';
    }
    const randomBytes = crypto.randomBytes(8);
    const hexString = randomBytes.toString('hex').toUpperCase();
    // Create a simple checksum from the hex string
    const checksum = crypto.createHash('sha256')
        .update(hexString)
        .digest('hex')
        .slice(-2) // Take last 2 hex chars
        .toUpperCase();
    return `${prefix}-${hexString}-${checksum}`;
}


function validateOriginalMemoFormat(memo) {
    // Check if the input memo conforms to the V1 standard: PREFIX-HEX(16)-CHECKSUM(2)
    if (typeof memo !== 'string') return false;
    const parts = memo.split('-');
    if (parts.length !== 3) return false;
    const [prefix, hex, checksum] = parts;
    return (
        ['BET', 'CF', 'RA'].includes(prefix) &&
        hex.length === 16 &&
        /^[A-F0-9]{16}$/.test(hex) && // Validate hex characters and length
        checksum.length === 2 &&
        /^[A-F0-9]{2}$/.test(checksum) && // Validate checksum characters and length
        validateMemoChecksum(hex, checksum) // Validate checksum calculation
    );
}


function validateMemoChecksum(hex, checksum) {
    // Re-calculate the checksum from the hex part and compare
     const expectedChecksum = crypto.createHash('sha256')
        .update(hex)
        .digest('hex')
        .slice(-2)
        .toUpperCase();
    return expectedChecksum === checksum;
}



function normalizeMemo(rawMemo) {
    if (typeof rawMemo !== 'string') return null;

    // Clean up the raw memo string: trim, remove prefixes, zero-width chars, normalize whitespace
    let memo = rawMemo
        .trim()
        .replace(/^memo[:=\s]*/i, '') // Remove common prefixes like "memo:", "text="
        .replace(/^text[:=\s]*/i, '')
        .replace(/[\u200B-\u200D\uFEFF]/g, '') // Remove zero-width spaces and BOM
        .replace(/\s+/g, '-') // Replace whitespace sequences with a single dash
        .toUpperCase(); // Convert to uppercase

    // Attempt to match the V1 format (PREFIX-HEX16-CHECKSUM2)
    const v1Match = memo.match(/^([A-Z]{2,3})-([A-F0-9]{16})-([A-F0-9]{2})$/);
    if (v1Match) {
        const [_, prefix, hex, checksum] = v1Match;
        // Validate the checksum
        if (validateMemoChecksum(hex, checksum)) {
            return `${prefix}-${hex}-${checksum}`; // Valid V1 memo
        }
         // Checksum failed - maybe a typo? Try to correct it.
         const correctedChecksum = crypto.createHash('sha256')
            .update(hex)
            .digest('hex')
            .slice(-2)
            .toUpperCase();
         console.warn(`⚠️ Memo checksum mismatch for V1 format "${memo}". Corrected: ${prefix}-${hex}-${correctedChecksum}`);
         return `${prefix}-${hex}-${correctedChecksum}`; // Return potentially corrected V1 memo
    }

    // Fallback: If it doesn't match V1, clean it up and ensure minimum length.
    // This handles potentially simpler memo formats or user input.
    // Remove non-alphanumeric characters (except hyphens), limit length.
    const cleanedFallback = memo.replace(/[^A-Z0-9\-]/g, '').slice(0, 64);
    if (cleanedFallback.length >= 6) { // Require a minimum length for fallback memos
         // console.log(`[MEMO DEBUG] Normalized fallback memo: "${cleanedFallback}" from raw: "${rawMemo}"`);
        return cleanedFallback;
    }

    return null; // Return null if no valid/usable memo could be extracted
}


async function findMemoInTx(tx) {
    const startTime = Date.now();
    let foundMemo = null;
    let method = 'none';
    let usedMethods = []; // Track methods used
    let scanDepth = 0; // Track depth/complexity

    if (!tx?.transaction?.message) {
        console.log("[MEMO DEBUG] Invalid transaction structure provided to findMemoInTx");
        return null;
    }

    const signature = tx.transaction.signatures?.[0] || 'N/A'; // Get signature for logging

    // 1. Check Logs First (Often the most direct)
    if (tx.meta?.logMessages) {
        scanDepth++;
        const memoLogPrefix = "Program log: Memo:";
        const altMemoLogPrefix = "Program log: Instruction: Memo"; // Handle variations
        for (const log of tx.meta.logMessages) {
            let rawMemo = null;
            if (log.includes(memoLogPrefix)) {
                rawMemo = log.substring(log.indexOf(memoLogPrefix) + memoLogPrefix.length).trim().replace(/^"+|"+$/g, '');
            } else if (log.includes(altMemoLogPrefix)) {
                 // Extract from "Instruction: Memo ..." pattern if needed
                 const match = log.match(/Instruction: Memo.*?text:?\s*"?([^"]+)"?/i);
                 if (match?.[1]) {
                     rawMemo = match[1].trim();
                 } else {
                     // Simpler "Instruction: Memo DATA" pattern
                     const simpleMatch = log.match(/Instruction: Memo\s+(.*)/i);
                     if (simpleMatch?.[1]) {
                        rawMemo = simpleMatch[1].trim();
                     }
                 }
            }

            if (rawMemo) {
                const memo = normalizeMemo(rawMemo);
                if (memo) {
                    console.log(`[MEMO DEBUG] Found memo in logs: "${memo}"`);
                    foundMemo = memo;
                    method = 'logs';
                    usedMethods.push(method);
                    break;
                }
            }
        }
    }

    // 2. Decode Instructions if no log memo found
    let accountKeys = []; // Define accountKeys here for broader scope
    if (!foundMemo) {
        scanDepth++;
        const message = tx.transaction.message;

        // --- Resolve Account Keys (including ALT) ---
        try {
             // Get main account keys
            const mainKeys = (message.accountKeys || []).map(keyInfo => {
                try {
                    if (keyInfo instanceof PublicKey) return keyInfo;
                    if (typeof keyInfo === 'string') return new PublicKey(keyInfo);
                    if (keyInfo?.pubkey) return new PublicKey(keyInfo.pubkey);
                    // console.warn("[MEMO DEBUG] Unknown account key format in mainKeys:", keyInfo);
                    return null;
                } catch (e) {
                     // console.warn(`[MEMO DEBUG] Error parsing main key: ${e.message}`, keyInfo);
                    return null;
                }
            }).filter(Boolean);
            accountKeys.push(...mainKeys);

            // Get keys from Address Lookup Tables (ALT) if present
            if (message.addressTableLookups && message.addressTableLookups.length > 0) {
                 // console.log(`[MEMO DEBUG] Found ${message.addressTableLookups.length} ALTs. Fetching...`);
                for (const lookup of message.addressTableLookups) {
                     try {
                         const lookupAccountKey = new PublicKey(lookup.accountKey);
                         const lookupTableAccount = await solanaConnection.getAddressLookupTable(lookupAccountKey);
                         if (lookupTableAccount?.value?.state?.addresses) {
                             const addresses = lookupTableAccount.value.state.addresses;
                             // console.log(`[MEMO DEBUG] Fetched ${addresses.length} addresses from ALT ${lookup.accountKey}`);
                             // Add only unique addresses to avoid duplicates
                             addresses.forEach(addr => {
                                 if (!accountKeys.some(existingKey => existingKey.equals(addr))) {
                                     accountKeys.push(addr);
                                 }
                             });
                         } else {
                             console.warn(`[MEMO DEBUG] Failed to fetch or parse ALT: ${lookup.accountKey}`);
                         }
                     } catch (altError) {
                         console.error(`[MEMO DEBUG] Error fetching/processing ALT ${lookup.accountKey}:`, altError.message);
                         // Continue processing other ALTs even if one fails
                     }
                 }
            }
            // console.log(`[MEMO DEBUG] Total resolved account keys (incl. ALTs): ${accountKeys.length}`);
        } catch (e) {
            console.error("[MEMO DEBUG] Error resolving account keys section:", e.message);
             // Continue without ALT keys if resolution fails
        }
        // --- End Resolve Account Keys ---


        // --- Process Instructions ---
        const instructions = [
            ...(message.instructions || []),
            ...(tx.meta?.innerInstructions || []).flatMap(i => i.instructions || []) // Include inner instructions
        ];
        // console.log(`[MEMO DEBUG] Processing ${instructions.length} total instructions.`);

        for (const [i, inst] of instructions.entries()) {
            try {
                if (inst.programIdIndex === undefined || !accountKeys[inst.programIdIndex]) {
                    // console.log(`[MEMO DEBUG] Skipping instruction ${i}: Invalid programIdIndex or missing key.`);
                    continue;
                }

                const programKey = accountKeys[inst.programIdIndex];
                const programId = programKey.toBase58();

                // Skip common system/token program instructions early
                if ([
                    SystemProgram.programId.toBase58(),
                    ComputeBudgetProgram.programId.toBase58(),
                    'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA', // SPL Token Program ID
                    'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb', // Token-2022 Program ID
                    '11111111111111111111111111111111' // System Program often stringified
                ].includes(programId)) {
                    continue;
                }

                // Use the robust decodeInstructionData helper
                const dataStr = decodeInstructionData(inst.data);
                if (!dataStr) {
                    // console.log(`[MEMO DEBUG] Instruction ${i} (Program: ${programId}): No decodable data.`);
                    continue; // Skip if data is null or undecodable
                }

                // Check if it's a known Memo program instruction
                if (MEMO_PROGRAM_IDS.includes(programId)) {
                    scanDepth++;
                    const memo = normalizeMemo(dataStr);
                    if (memo) {
                        console.log(`[MEMO DEBUG] Found memo in instruction ${i} (Program: ${programId}): "${memo}"`);
                        foundMemo = memo;
                        method = 'instructions';
                        usedMethods.push(method);
                        break; // Exit loop once memo is found
                    }
                }

                // Fallback: Pattern match within *any* instruction data (less reliable)
                if (!foundMemo && dataStr.length >= 10) { // Check length before regex
                    scanDepth++;
                    const prefixes = ['CF-', 'RA-', 'BET-'];
                    if (prefixes.some(p => dataStr.toUpperCase().startsWith(p))) { // Match prefixes case-insensitively
                        const memo = normalizeMemo(dataStr);
                        if (memo) {
                            console.log(`[MEMO DEBUG] Found memo via pattern match in instruction ${i} data (Program: ${programId}): "${memo}"`);
                            foundMemo = memo;
                            method = 'pattern';
                            usedMethods.push(method);
                            break; // Exit loop once memo is found
                        }
                    }
                }
            } catch (e) {
                console.error(`[MEMO DEBUG] Error processing instruction ${i}:`, e?.message || e);
                // Continue to next instruction even if one fails
            }
            if (foundMemo) break; // Exit outer loop if memo found in inner try-catch
        }
        // --- End Process Instructions ---
    }


    // 3. Deep Log Scan (Final Fallback if nothing else worked)
    if (!foundMemo && tx.meta?.logMessages) {
        scanDepth++;
        // Join all logs and try a broader regex match for memo-like patterns
        const logString = tx.meta.logMessages.join('\n');
        // Match common labels followed by potential memo IDs (adjust regex as needed)
        // Looks for things like "Memo: ID", "Text: ID", "Message ID" allowing some flexibility
        const logMatch = logString.match(/(?:Memo|Text|Message)[:=\s]*"?([A-Z0-9\-]{6,64})"?/i);

        if (logMatch?.[1]) {
            const potentialMemoFromLog = logMatch[1];
             // console.log(`[MEMO DEBUG] Potential memo found via deep log scan regex: "${potentialMemoFromLog}"`);
            const recoveredMemo = normalizeMemo(potentialMemoFromLog);
            if (recoveredMemo) {
                console.log(`[MEMO DEBUG] Recovered memo from deep log scan: ${recoveredMemo}`);
                foundMemo = recoveredMemo;
                method = 'deep_logs';
                usedMethods.push(method);
            }
        }
    }


    if (foundMemo) {
        // ** ADDED MEMO STATS LOG **
        console.log(`[MEMO STATS] TX:${signature.slice(0,8)} | ` +
            `Methods:${usedMethods.join(',')} | ` +
            `Depth:${scanDepth} | ` +
            `Time:${Date.now() - startTime}ms | ` +
             `Version:${tx.version ?? 'legacy'}`
        );
        return foundMemo;
    } else {
         // console.log(`[MEMO DEBUG] Exhausted all search methods for TX ${signature.slice(0,8)}, no memo found. Time: ${Date.now() - startTime}ms`);
        return null;
    }
}


async function savePendingBet(userId, chatId, gameType, details, lamports, memoId, expiresAt, priority = 0) {
    // Validate the generated Memo ID *before* attempting DB insert
    if (!validateOriginalMemoFormat(memoId)) {
        console.error(`DB: Attempted to save bet with invalid generated memo format: ${memoId}`);
        // It's crucial *not* to save if the memo is invalid, as it can't be matched later.
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
        FEE_BUFFER, // Store the fee buffer used for this bet
        priority
    ];

    try {
        const res = await pool.query(query, values);
        console.log(`DB: Saved pending ${gameType} bet ID ${res.rows[0].id} with memo ${memoId}`);
        return { success: true, id: res.rows[0].id };
    } catch (err) {
        console.error('DB Error saving bet:', err.message);
        // Handle specific DB errors, like unique constraint violations
        if (err.code === '23505' && err.constraint === 'bets_memo_id_key') {
            console.warn(`DB: Memo ID collision for ${memoId}. User might be retrying or system generated duplicate.`);
            // Inform user the memo already exists, suggesting they try again might generate a new one
            return { success: false, error: 'Memo ID already exists. Please try generating the bet again.' };
        }
        // Generic error for other DB issues
        return { success: false, error: `Database error: ${err.message}` };
    }
}


async function findBetByMemo(memoId) {
    // Basic validation of input memoId
    if (!memoId || typeof memoId !== 'string') {
         console.warn(`DB: Attempted findBetByMemo with invalid memoId: ${memoId}`);
        return undefined;
    }

    // Find a bet matching the memo that is still awaiting payment.
    // Prioritize higher priority bets, then older ones.
    // Use FOR UPDATE SKIP LOCKED to prevent race conditions if multiple workers process payments.
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
        return res.rows[0]; // Returns the bet object or undefined if not found/locked
    } catch (err) {
        console.error(`DB Error finding bet by memo ${memoId}:`, err.message);
        return undefined; // Return undefined on error
    }
}


async function markBetPaid(betId, signature) {
    const query = `
        UPDATE bets
        SET status = 'payment_verified',
            paid_tx_signature = $1,
            processed_at = NOW() -- Mark processed time when payment is verified
        WHERE id = $2 AND status = 'awaiting_payment' -- Ensure we only update bets awaiting payment
        RETURNING *; -- Return the updated bet row
    `;
    try {
        const res = await pool.query(query, [signature, betId]);
        if (res.rowCount === 0) {
            // This can happen if the bet was already processed by another worker due to SKIP LOCKED,
            // or if the status changed for other reasons (e.g., expired)
            console.warn(`DB: Attempted to mark bet ${betId} as paid, but status was not 'awaiting_payment' or bet not found/locked.`);
            return { success: false, error: 'Bet not found or already processed' };
        }
        console.log(`DB: Marked bet ${betId} as paid with TX ${signature}`);
        return { success: true, bet: res.rows[0] };
    } catch (err) {
         // Handle potential unique constraint violation if the signature was somehow already recorded
        if (err.code === '23505' && err.constraint === 'bets_paid_tx_signature_key') {
            console.warn(`DB: Paid TX Signature ${signature} collision for bet ${betId}. Already processed.`);
            // This indicates the transaction was likely processed already
            return { success: false, error: 'Transaction signature already recorded' };
        }
        console.error(`DB Error marking bet ${betId} paid:`, err.message);
        return { success: false, error: `Database error: ${err.message}` };
    }
}

// --- END OF PART 1 ---
// Part 2 of 2

async function linkUserWallet(userId, walletAddress) {
    const cacheKey = `wallet-${userId}`;
    // Upsert: Insert or update the wallet address for the user.
    // Update last_used_at timestamp on every link/update.
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
             // Update cache
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
        return { success: false, error: `Database error: ${err.message}` };
    }
}


async function getLinkedWallet(userId) {
    const cacheKey = `wallet-${userId}`;

    // Check cache first
    if (walletCache.has(cacheKey)) {
        const { wallet, timestamp } = walletCache.get(cacheKey);
        if (Date.now() - timestamp < CACHE_TTL) {
             // console.log(`CACHE HIT for wallet user ${userId}`);
            return wallet; // Return cached wallet if still valid
        } else {
            walletCache.delete(cacheKey); // Remove expired cache entry
             // console.log(`CACHE EXPIRED for wallet user ${userId}`);
        }
    }
     // console.log(`CACHE MISS for wallet user ${userId}`);

    // If not in cache or expired, query the database
    const query = `SELECT wallet_address FROM wallets WHERE user_id = $1`;
    try {
        const res = await pool.query(query, [String(userId)]);
        const wallet = res.rows[0]?.wallet_address;

        if (wallet) {
            // Store fetched wallet in cache
            walletCache.set(cacheKey, {
                wallet,
                timestamp: Date.now()
            });
        }
        return wallet; // Return wallet address or undefined if not found
    } catch (err) {
        console.error(`DB Error fetching wallet for user ${userId}:`, err.message);
        return undefined; // Return undefined on database error
    }
}


async function updateBetStatus(betId, status) {
    // Update bet status. If status indicates completion or error, also update processed_at.
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
    // Record payout transaction signature and final status.
    // Ensure we only update bets that were in the 'processing_payout' state.
    const query = `
        UPDATE bets
        SET status = $1,
            payout_tx_signature = $2,
            processed_at = NOW() -- Final processed time
        WHERE id = $3 AND status = 'processing_payout'
        RETURNING id;
    `;
    try {
        const res = await pool.query(query, [status, signature, betId]);
        if (res.rowCount > 0) {
            console.log(`DB: Recorded payout TX ${signature} for bet ${betId} with status ${status}`);
            return true;
        } else {
            // This could happen if the status wasn't 'processing_payout' or bet not found
            console.warn(`DB: Failed to record payout for bet ${betId} (not found or status mismatch?)`);
            return false;
        }
    } catch (err) {
        // Handle potential unique constraint violation on payout signature
        if (err.code === '23505' && err.constraint === 'bets_payout_tx_signature_key') {
            console.warn(`DB: Payout TX Signature ${signature} collision for bet ${betId}. Already recorded.`);
            return false; // Indicate failure because it's already done
        }
        console.error(`DB Error recording payout for bet ${betId}:`, err.message);
        return false;
    }
}


function analyzeTransactionAmounts(tx, walletType) {
    let transferAmount = 0n; // Use BigInt for lamports
    let payerAddress = null;
    const targetAddress = walletType === 'coinflip'
        ? process.env.MAIN_WALLET_ADDRESS
        : process.env.RACE_WALLET_ADDRESS;

    // If the transaction failed on-chain, there's no balance change to analyze
    if (tx?.meta?.err) {
         // console.log(`[AnalyzeAmount] Transaction ${tx.transaction?.signatures?.[0]?.slice(0,8)} failed, returning zero amount.`);
        return { transferAmount: 0n, payerAddress: null };
    }

    // Method 1: Analyze balance changes (most reliable for simple transfers)
    if (tx?.meta?.preBalances && tx?.meta?.postBalances && tx?.transaction?.message?.accountKeys) {
         try {
             const accountKeys = tx.transaction.message.accountKeys.map(k => {
                 try {
                     // Handle different possible key formats in the message
                     return k.pubkey ? new PublicKey(k.pubkey) : new PublicKey(k);
                 } catch { return null; }
             }).filter(Boolean);
             const accountKeyStrings = accountKeys.map(pk => pk.toBase58());

             const targetIndex = accountKeyStrings.indexOf(targetAddress);

             // Check if target wallet is in the transaction and has balance info
             if (targetIndex !== -1 && tx.meta.preBalances[targetIndex] !== undefined && tx.meta.postBalances[targetIndex] !== undefined) {
                 const balanceChange = BigInt(tx.meta.postBalances[targetIndex]) - BigInt(tx.meta.preBalances[targetIndex]);

                 // If the target wallet's balance increased, this is likely the transfer amount
                 if (balanceChange > 0n) {
                     transferAmount = balanceChange;
                     // console.log(`[AnalyzeAmount] Detected ${transferAmount} lamport increase for target ${targetAddress}`);

                     // Try to identify the payer by finding a signer whose balance decreased appropriately
                     for (let i = 0; i < accountKeyStrings.length; i++) {
                         if (i === targetIndex) continue; // Skip the target wallet itself
                         if (tx.meta.preBalances[i] === undefined || tx.meta.postBalances[i] === undefined) continue; // Skip if no balance info

                         const keyInfo = tx.transaction.message.accountKeys[i];
                         // Determine if the account is a signer (fee payer is always signer 0)
                         const isSigner = keyInfo?.signer === true ||
                                          (tx.transaction.message.header?.numRequiredSignatures > 0 && i < tx.transaction.message.header.numRequiredSignatures);

                         if (isSigner) {
                             const payerBalanceChange = BigInt(tx.meta.postBalances[i]) - BigInt(tx.meta.preBalances[i]);
                             // If a signer's balance decreased by at least the transfer amount (+fees), assume they are the payer
                             if (payerBalanceChange <= -transferAmount) {
                                 payerAddress = accountKeyStrings[i];
                                 // console.log(`[AnalyzeAmount] Identified signer ${payerAddress} as likely payer (balance change: ${payerBalanceChange}).`);
                                 break;
                             }
                         }
                     }

                     // Fallback: If no specific signer matched, assume the fee payer (index 0) is the sender if their balance decreased
                     if (!payerAddress && accountKeyStrings[0] && tx.meta.preBalances[0] !== undefined && tx.meta.postBalances[0] !== undefined) {
                         const feePayerBalanceChange = BigInt(tx.meta.postBalances[0]) - BigInt(tx.meta.preBalances[0]);
                         if (feePayerBalanceChange < 0n) {
                             payerAddress = accountKeyStrings[0];
                             // console.log(`[AnalyzeAmount] Assuming fee payer ${payerAddress} is the sender (balance change: ${feePayerBalanceChange}).`);
                         }
                     }
                 }
             }
         } catch (e) {
             console.error(`[AnalyzeAmount] Error during balance change analysis: ${e.message}`);
         }
    }

    // Method 2: Look for explicit SystemProgram.transfer instructions (fallback if balance change is unclear)
    // This is less reliable if transfers happen via complex contracts (inner instructions might be needed)
    if (transferAmount === 0n && tx?.transaction?.message?.instructions) {
         try {
             const instructions = [
                 ...(tx.transaction.message.instructions || []),
                 ...(tx.meta?.innerInstructions || []).flatMap(i => i.instructions || []) // Check inner instructions too
             ];
             const SYSTEM_PROGRAM_ID = SystemProgram.programId.toBase58();

             // Need account keys resolved to interpret instructions correctly
             const accountKeysForInstr = tx.transaction.message.accountKeys.map(k => {
                 try { return k.pubkey ? new PublicKey(k.pubkey) : new PublicKey(k); } catch { return null; }
             }).filter(Boolean);
             const accountKeyStringsForInstr = accountKeysForInstr.map(pk => pk.toBase58());

             for (const inst of instructions) {
                 let programId = '';
                 try { // Safely get program ID string
                     if (inst.programIdIndex !== undefined && accountKeyStringsForInstr) {
                         programId = accountKeyStringsForInstr[inst.programIdIndex] || '';
                     } else if (inst.programId) {
                         programId = inst.programId.toBase58 ? inst.programId.toBase58() : String(inst.programId);
                     }
                 } catch { /* Ignore errors getting programId */ }

                 // Check if it's a system transfer instruction *and* parsed info is available
                 if (programId === SYSTEM_PROGRAM_ID && inst.parsed?.type === 'transfer') {
                     const transferInfo = inst.parsed.info;
                     // If the destination is our target wallet
                     if (transferInfo.destination === targetAddress) {
                         const instructionAmount = BigInt(transferInfo.lamports || transferInfo.amount || 0);
                         if (instructionAmount > 0n) {
                             transferAmount = instructionAmount;
                             payerAddress = transferInfo.source; // The source of the transfer instruction
                             // console.log(`[AnalyzeAmount] Found transfer instruction: ${transferAmount} lamports from ${payerAddress}`);
                             break; // Found the relevant transfer
                         }
                     }
                 }
             }
         } catch(e) {
             console.error(`[AnalyzeAmount] Error during instruction analysis: ${e.message}`);
         }
    }

    // console.log(`[AnalyzeAmount] Final Result - Amount: ${transferAmount}, Payer: ${payerAddress}`);
    return { transferAmount, payerAddress };
}


function getPayerFromTransaction(tx) {
    if (!tx || !tx.meta || !tx.transaction?.message?.accountKeys) return null;

    const message = tx.transaction.message;

    // The fee payer is always the first account in the message's accountKeys array
    if (message.accountKeys.length > 0) {
        const feePayerKeyInfo = message.accountKeys[0];
        let feePayerAddress = null;
         try { // Safely extract the public key string
            if (feePayerKeyInfo instanceof PublicKey) {
                 feePayerAddress = feePayerKeyInfo.toBase58();
            } else if (feePayerKeyInfo?.pubkey) { // Handle object format { pubkey: '...', signer: ..., writable: ... }
                 feePayerAddress = new PublicKey(feePayerKeyInfo.pubkey).toBase58();
            } else if (typeof feePayerKeyInfo === 'string') { // Handle string format
                 feePayerAddress = new PublicKey(feePayerKeyInfo).toBase58();
            }
         } catch (e) {
             console.warn("Could not parse fee payer address:", e.message, "KeyInfo:", feePayerKeyInfo);
         }

         // Return the PublicKey object if successfully parsed
        return feePayerAddress ? new PublicKey(feePayerAddress) : null;
    }

    // Fallback (less common): If accountKeys is empty (shouldn't happen for valid tx), return null
    // The balance change method is less reliable for identifying the *fee* payer specifically.
    console.warn("getPayerFromTransaction: No account keys found in message, cannot determine fee payer.");
    return null;
}


// Helper to identify errors that might warrant a retry (network issues, timeouts, rate limits)
function isRetryableError(error) {
    const msg = error?.message?.toLowerCase() || '';
    const code = error?.code?.toLowerCase() || ''; // Node.js error codes (e.g., ECONNRESET)
    const statusCode = error?.statusCode; // HTTP status codes (e.g., 429, 503)

    // Check for common network/timeout/rate limit errors by message content
    if (msg.includes('timeout') ||
        msg.includes('timed out') ||
        msg.includes('rate limit') ||
        msg.includes('econnreset') || // Connection reset
        msg.includes('esockettimedout') || // Socket timeout
        msg.includes('network error') || // Generic network issues
        msg.includes('fetch') || // Errors from underlying fetch implementation
        msg.includes('connection terminated') ||
        msg.includes('gateway timeout') || // Common proxy/gateway error
        msg.includes('service unavailable') // 503 errors
       )
    {
        return true;
    }

    // Check by error code or HTTP status code
    if (code === 'etimedout' ||
        code === 'econnrefused' || // Connection refused (maybe temporary infra issue)
        statusCode === 429 || // Too Many Requests
        statusCode === 503 || // Service Unavailable
        statusCode === 504    // Gateway Timeout
       )
    {
        return true;
    }

    // Solana specific: BlockhashNotFound might be retryable if caught quickly
    if (msg.includes('blockhash not found')) {
         // console.log("Retryable: Blockhash not found detected.");
         return true;
    }


    return false; // Assume not retryable otherwise
}


// Fallback function to extract memo from log messages if findMemoInTx fails initially.
// Kept for potential edge cases, though findMemoInTx is now more robust.
function extractMemoFromDescription(tx) {
     // Note: This is largely redundant now with the improved findMemoInTx
    if (!tx?.meta?.logMessages) return null;

    const desc = tx.meta.logMessages.join('\n');

    // Try matching common log patterns for memos
    const memoMatch = desc.match(/Instruction: Memo.*?text:?\s*"?([^"]+)"?/i);
    if (memoMatch?.[1]) {
        // console.log(`[MEMO DEBUG - Fallback] Extracted raw memo from description: "${memoMatch[1]}"`);
        return normalizeMemo(memoMatch[1]);
    }

    // Simpler pattern
    const simpleMemoMatch = desc.match(/Instruction: Memo\s+([^\n]+)/i);
    if (simpleMemoMatch?.[1]){
        // console.log(`[MEMO DEBUG - Fallback] Extracted simple raw memo from description: "${simpleMemoMatch[1]}"`);
        return normalizeMemo(simpleMemoMatch[1]);
    }
    return null;
}

// New function for deeper scanning of complex transactions
// Needs accountKeys passed or resolved within its scope
async function deepScanTransaction(tx, accountKeys) {
    const signature = tx.transaction.signatures?.[0]?.slice(0, 8) || 'N/A';
    console.log(`[Deep Scan TX: ${signature}] Starting deep scan...`);
    try {
        // 1. Check for memo in inner instructions specifically
        if (tx.meta?.innerInstructions) {
            for (const inner of tx.meta.innerInstructions) {
                for (const inst of inner.instructions) {
                    try {
                         if (inst.programIdIndex !== undefined && accountKeys[inst.programIdIndex]) {
                             const programId = accountKeys[inst.programIdIndex].toBase58();
                             if (MEMO_PROGRAM_IDS.includes(programId)) {
                                 const data = decodeInstructionData(inst.data);
                                 if (data) {
                                     const memo = normalizeMemo(data);
                                     if (memo) {
                                         console.log(`[Deep Scan TX: ${signature}] Found memo in inner instruction: ${memo}`);
                                         return memo;
                                     }
                                 }
                             }
                         }
                    } catch (e) { /* ignore errors in single inner instruction */ }
                }
            }
        }

        // 2. Raw data pattern matching across *all* instructions (less targeted)
        const allInstructions = [
            ...(tx.transaction.message.instructions || []),
            ...(tx.meta?.innerInstructions || []).flatMap(i => i.instructions || [])
        ];
        for (const inst of allInstructions) {
             try {
                 const data = decodeInstructionData(inst.data);
                 // Look for patterns like PREFIX-ID or just ID if it looks like our format
                 if (data?.match(/[A-Z]{2,3}-[A-F0-9]{16}-[A-F0-9]{2}/) || data?.match(/^BET-[A-F0-9]{16}-[A-F0-9]{2}$/) || data?.match(/^CF-[A-F0-9]{16}-[A-F0-9]{2}$/) || data?.match(/^RA-[A-F0-9]{16}-[A-F0-9]{2}$/)) {
                     const memo = normalizeMemo(data);
                     if (memo) {
                         console.log(`[Deep Scan TX: ${signature}] Found memo via raw data pattern: ${memo}`);
                         return memo;
                     }
                 }
             } catch(e) { /* ignore data decoding errors */ }
        }

        // 3. Final fallback - Hex dump scan (very speculative)
        // Convert the entire transaction object to string and look for hex patterns
        // This is a last resort and might produce false positives
        const fullTxString = JSON.stringify(tx);
        // Look for long hex strings that *might* contain UTF8 encoded memos
        const hexMatches = fullTxString.match(/[A-Fa-f0-9]{32,}/g); // Find hex strings of 16+ bytes
        if (hexMatches) {
            for (const hex of hexMatches) {
                 try {
                     const potentialUtf8 = Buffer.from(hex, 'hex').toString('utf8');
                     // Check if the decoded string contains a memo-like pattern
                     if (potentialUtf8.match(/[A-Z]{2,3}-[A-F0-9]{16}-[A-F0-9]{2}/)) {
                         const memo = normalizeMemo(potentialUtf8);
                         if (memo) {
                             console.log(`[Deep Scan TX: ${signature}] Found potential memo via hex dump scan: ${memo}`);
                             return memo;
                         }
                     }
                 } catch (e) { /* Ignore errors decoding hex or normalizing */ }
            }
        }

    } catch (e) {
        console.error(`[Deep Scan TX: ${signature}] Deep scan failed:`, e);
    }
    console.log(`[Deep Scan TX: ${signature}] Deep scan completed, no memo found.`);
    return null; // Return null if deep scan fails
}


class PaymentProcessor {
    constructor() {
         // Separate queues for potentially prioritizing paid bets over new signature checks
        this.highPriorityQueue = new PQueue({
            concurrency: 3, // Process verified payments/payouts faster
        });
        this.normalQueue = new PQueue({
            concurrency: 2, // Check incoming signatures at a lower concurrency
        });
        this.activeProcesses = new Set(); // Track jobs currently being processed (by signature or betId)
    }

    // Add a job (payment check, bet processing, payout) to the appropriate queue
    async addPaymentJob(job) {
        // Prioritize jobs with higher priority value
        const queue = (job.priority && job.priority > 0) ? this.highPriorityQueue : this.normalQueue;
        // console.log(`[Queue] Adding job type ${job.type} (ID: ${job.signature || job.betId || 'N/A'}, Prio: ${job.priority || 0}) to ${queue === this.highPriorityQueue ? 'High' : 'Normal'} Prio Queue.`);

        queue.add(() => this.processJob(job)).catch(queueError => {
            console.error(`Queue error processing job ${job.type} (${job.signature || job.betId || 'N/A'}):`, queueError.message);
            performanceMonitor.logRequest(false); // Log error in performance stats
            // Consider additional error handling for queue failures
        });
    }

    // Worker function to process a single job from the queue
    async processJob(job) {
        // Use signature or betId as a unique identifier for the job
        const jobIdentifier = job.signature || job.betId;
        if (jobIdentifier && this.activeProcesses.has(jobIdentifier)) {
            // console.log(`[Queue] Job ${job.type} for ${jobIdentifier} is already active. Skipping duplicate.`);
            return; // Avoid processing the same signature/bet concurrently
        }
        if (jobIdentifier) this.activeProcesses.add(jobIdentifier); // Mark as active

        try {
            let result;
            // console.log(`[Queue] Processing job type ${job.type} for identifier ${jobIdentifier || 'N/A'}`);

            // Route job based on its type
            if (job.type === 'monitor_payment') {
                result = await this._processIncomingPayment(job.signature, job.walletType, job.retries || 0);
            } else if (job.type === 'process_bet') {
                 // Fetch the latest bet details before processing
                const bet = await pool.query('SELECT * FROM bets WHERE id = $1', [job.betId]).then(res => res.rows[0]);
                if (bet) {
                    await processPaidBet(bet); // Handle game logic
                    result = { processed: true };
                } else {
                    console.error(`Cannot process bet: Bet ID ${job.betId} not found.`);
                    result = { processed: false, reason: 'bet_not_found' };
                }
            } else if (job.type === 'payout') {
                await handlePayoutJob(job); // Handle sending SOL winnings
                result = { processed: true };
            } else {
                console.error(`Unknown job type: ${job.type}`);
                result = { processed: false, reason: 'unknown_job_type'};
            }

            performanceMonitor.logRequest(true); // Log successful processing
            return result;

        } catch (error) {
            performanceMonitor.logRequest(false); // Log error
            console.error(`Error processing job type ${job.type} for identifier ${jobIdentifier || 'N/A'}:`, error.message);

            // Retry logic specifically for 'monitor_payment' if the error is retryable
            if (job.type === 'monitor_payment' && (job.retries || 0) < 3 && isRetryableError(error)) {
                job.retries = (job.retries || 0) + 1;
                const retryDelay = 1000 * job.retries; // Simple linear backoff for retries
                console.log(`Retrying job for signature ${job.signature} (Attempt ${job.retries}) in ${retryDelay}ms...`);
                await new Promise(resolve => setTimeout(resolve, retryDelay));

                if (jobIdentifier) this.activeProcesses.delete(jobIdentifier); // Allow retry by removing from active set
                await this.addPaymentJob(job); // Re-queue the job
                return; // Exit current failed attempt
            } else {
                // If not retryable, exceeded retries, or different job type, log final failure
                console.error(`Job failed permanently or exceeded retries for identifier: ${jobIdentifier || 'N/A'}`, error);
                 // If it was a bet processing or payout job, mark the bet with an error status
                if(job.betId && job.type !== 'monitor_payment') {
                    await updateBetStatus(job.betId, `error_${job.type}_failed`);
                }
            }
        } finally {
             // Always remove the job identifier from the active set when processing finishes (success or fail)
            if (jobIdentifier) {
                this.activeProcesses.delete(jobIdentifier);
            }
            // console.log(`[Queue] Finished job type ${job.type} for identifier ${jobIdentifier || 'N/A'}`);
        }
    }


    // Internal method to handle the logic of processing an incoming payment transaction
    async _processIncomingPayment(signature, walletType, attempt = 0) {
        const logPrefix = `[PaymentProc TX:${signature.slice(0,8)}]`;

        // 1. Check if already processed in this session or in DB
        if (processedSignaturesThisSession.has(signature)) {
            // console.log(`${logPrefix} Skipped: Already processed in this session.`);
            return { processed: false, reason: 'already_processed_session' };
        }
        const checkQuery = `SELECT id FROM bets WHERE paid_tx_signature = $1 LIMIT 1;`;
        try {
            const processed = await pool.query(checkQuery, [signature]);
            if (processed.rowCount > 0) {
                // console.log(`${logPrefix} Skipped: Signature already exists in DB (Bet ID: ${processed.rows[0].id}).`);
                processedSignaturesThisSession.add(signature); // Add to session cache to avoid re-checking DB
                return { processed: false, reason: 'exists_in_db' };
            }
        } catch (dbError) {
            console.error(`${logPrefix} DB Error checking signature:`, dbError.message);
            if (isRetryableError(dbError) && attempt < 3) throw dbError; // Rethrow for queue retry logic
            return { processed: false, reason: 'db_check_error' };
        }

        // 2. Fetch Transaction Details
        // console.log(`${logPrefix} Processing transaction details (Attempt ${attempt + 1})`);
        let tx;
        const targetAddress = walletType === 'coinflip' ? process.env.MAIN_WALLET_ADDRESS : process.env.RACE_WALLET_ADDRESS;
        let accountKeys = []; // Define accountKeys for use in memo finding and deep scan

        try {
            tx = await solanaConnection.getParsedTransaction(
                signature,
                {
                    maxSupportedTransactionVersion: 0, // Request version 0 for broader compatibility if needed, or keep higher if ALTs are essential
                    commitment: 'confirmed' // Use confirmed commitment for reliability
                }
            );

            if (!tx) {
                // This might happen if the transaction hasn't propagated fully or was dropped
                console.warn(`${logPrefix} Transaction data returned null from RPC.`);
                // Consider retrying vs failing permanently based on policy
                if (attempt < 2) { // Allow maybe one retry for propagation delay
                   throw new Error(`Transaction ${signature} not found (null response)`);
                } else {
                   processedSignaturesThisSession.add(signature); // Mark as processed to prevent infinite loops if TX never appears
                   return { processed: false, reason: 'tx_fetch_null' };
                }
            }
            // console.log(`${logPrefix} Fetched transaction.`);

            // Resolve account keys *after* fetching TX, needed for memo finding and deep scan
             if (tx.transaction?.message) {
                 const message = tx.transaction.message;
                 const mainKeys = (message.accountKeys || []).map(keyInfo => {
                     try {
                         if (keyInfo instanceof PublicKey) return keyInfo;
                         if (typeof keyInfo === 'string') return new PublicKey(keyInfo);
                         if (keyInfo?.pubkey) return new PublicKey(keyInfo.pubkey);
                         return null;
                     } catch { return null; }
                 }).filter(Boolean);
                 accountKeys.push(...mainKeys);

                 if (message.addressTableLookups && message.addressTableLookups.length > 0) {
                     for (const lookup of message.addressTableLookups) {
                         try {
                             const lookupAccountKey = new PublicKey(lookup.accountKey);
                             const lookupTableAccount = await solanaConnection.getAddressLookupTable(lookupAccountKey);
                             if (lookupTableAccount?.value?.state?.addresses) {
                                 const addresses = lookupTableAccount.value.state.addresses;
                                 addresses.forEach(addr => {
                                     if (!accountKeys.some(existingKey => existingKey.equals(addr))) {
                                         accountKeys.push(addr);
                                     }
                                 });
                             }
                         } catch (altError) {
                             console.error(`${logPrefix} Error fetching/processing ALT ${lookup.accountKey}:`, altError.message);
                         }
                     }
                 }
             }

        } catch (fetchError) {
            console.error(`${logPrefix} Failed to fetch/parse TX: ${fetchError.message}`);
            if (isRetryableError(fetchError) && attempt < 3) throw fetchError; // Rethrow for queue retry
             // Mark as processed if fetch fails permanently
            processedSignaturesThisSession.add(signature);
            return { processed: false, reason: 'tx_fetch_failed' };
        }

        // 3. Check for On-Chain Errors
        if (tx.meta?.err) {
            console.log(`${logPrefix} Transaction failed on-chain: ${JSON.stringify(tx.meta.err)}`);
            processedSignaturesThisSession.add(signature); // Mark as processed (failed tx)
            return { processed: false, reason: 'tx_onchain_error' };
        }

        // ** ADDED: Transaction Pre-Filtering for Complexity **
        // Check if the transaction looks complex (e.g., many inner instructions)
        // Threshold (e.g., 5) might need tuning based on typical bet transactions vs complex DeFi interactions.
        if (tx.meta?.innerInstructions && tx.meta.innerInstructions.length > 5) {
            console.log(`${logPrefix} Complex transaction detected (${tx.meta.innerInstructions.length} inner instructions). Initiating deep scan.`);
            // Call the deep scan function, passing the transaction and resolved account keys
            const deepMemo = await deepScanTransaction(tx, accountKeys);
            if (deepMemo) {
                 console.log(`${logPrefix} Memo found via deep scan: ${deepMemo}`);
                 // If deep scan finds it, continue processing with this memo
                 // (This replaces calling findMemoInTx below for this specific complex case)
                 // Jump to step 5 logic, reusing the 'memo' variable name
                 await this._continueProcessingWithMemo(tx, signature, deepMemo, walletType, targetAddress, logPrefix);
                 return { processed: true }; // Indicate processing was handled
            } else {
                 console.log(`${logPrefix} Deep scan did not find a memo. Aborting processing for this complex TX.`);
                 processedSignaturesThisSession.add(signature);
                 return { processed: false, reason: 'complex_tx_no_memo' };
            }
        }
        // --- End Pre-Filtering ---

        // 4. Find Memo (Using the improved findMemoInTx)
        let memo = await findMemoInTx(tx); // Pass the full tx object
        // console.log(`${logPrefix} Memo found by findMemoInTx: "${memo || 'null'}"`);

        // If findMemoInTx returns null, try the older fallback (less likely needed now)
        if (!memo) {
             memo = extractMemoFromDescription(tx); // Use fallback as a last resort
             if (memo) {
                 console.log(`${logPrefix} Found memo using fallback description parser: "${memo}"`);
             }
        }

        if (!memo) {
            console.log(`${logPrefix} No usable game memo found.`);
            processedSignaturesThisSession.add(signature); // Mark as processed (no memo)
            return { processed: false, reason: 'no_valid_memo' };
        }

        // --- Continue processing from step 5 using the found memo ---
        await this._continueProcessingWithMemo(tx, signature, memo, walletType, targetAddress, logPrefix);
        return { processed: true }; // Indicate processing was handled

    } // End _processIncomingPayment

    // Helper to avoid duplicating logic after memo is found (either normally or via deep scan)
    async _continueProcessingWithMemo(tx, signature, memo, walletType, targetAddress, logPrefix) {
        // 5. Find Matching Bet in DB
        const bet = await findBetByMemo(memo);
        // console.log(`${logPrefix} Found pending bet ID: ${bet?.id || 'None'} for memo: ${memo}`);
        if (!bet) {
            console.warn(`${logPrefix} No matching *pending* bet found for memo "${memo}". Could be unrelated, expired, or already processed.`);
            processedSignaturesThisSession.add(signature);
            // Don't return error here, just mark processed if no *pending* bet exists
            return; // { processed: false, reason: 'no_matching_bet' };
        }
        console.log(`${logPrefix} Processing payment for Bet ID: ${bet.id} (Memo: ${memo})`);

        // 6. Validate Bet Status
        if (bet.status !== 'awaiting_payment') {
            console.warn(`${logPrefix} Bet ${bet.id} status is ${bet.status}, not 'awaiting_payment'. Already processed?`);
            processedSignaturesThisSession.add(signature);
            return; // { processed: false, reason: 'bet_already_processed' };
        }

        // 7. Analyze Transfer Amount
        const { transferAmount, payerAddress } = analyzeTransactionAmounts(tx, walletType);
        if (transferAmount <= 0n) {
            console.warn(`${logPrefix} No SOL transfer to target wallet (${targetAddress}) found in TX for memo ${memo}.`);
            processedSignaturesThisSession.add(signature);
            // Could be a different type of transaction with the same memo - mark processed, don't error bet
            return; // { processed: false, reason: 'no_transfer_found' };
        }

        // 8. Verify Amount Tolerance
        const expectedAmount = BigInt(bet.expected_lamports);
        const tolerance = BigInt(5000); // Allow +/- 5000 lamports tolerance
        if (transferAmount < (expectedAmount - tolerance) || transferAmount > (expectedAmount + tolerance)) {
            const expectedSOL = Number(expectedAmount) / LAMPORTS_PER_SOL;
            const receivedSOL = Number(transferAmount) / LAMPORTS_PER_SOL;
            console.warn(`${logPrefix} Amount mismatch for bet ${bet.id}. Expected ~${expectedAmount} (${expectedSOL} SOL), got ${transferAmount} (${receivedSOL} SOL).`);
            await updateBetStatus(bet.id, 'error_payment_mismatch');
            processedSignaturesThisSession.add(signature);
            await safeSendMessage(bet.chat_id, `⚠️ Payment amount mismatch for bet \`${memo}\`. Expected ${expectedSOL.toFixed(6)} SOL, received ${receivedSOL.toFixed(6)} SOL. Bet cancelled.`, { parse_mode: 'Markdown' }).catch(e => console.error("TG Send Error:", e.message));
            return; // { processed: false, reason: 'amount_mismatch' };
        }

        // 9. Check Expiry
        const txTime = tx.blockTime ? new Date(tx.blockTime * 1000) : null;
        if (!txTime) {
            console.warn(`${logPrefix} Could not determine blockTime for TX. Skipping expiry check.`);
        } else if (txTime > new Date(bet.expires_at)) {
            console.warn(`${logPrefix} Payment for bet ${bet.id} received at ${txTime.toISOString()} after expiry (${new Date(bet.expires_at).toISOString()}).`);
            await updateBetStatus(bet.id, 'error_payment_expired');
            processedSignaturesThisSession.add(signature);
            await safeSendMessage(bet.chat_id, `⚠️ Payment for bet \`${memo}\` received after expiry time. Bet cancelled.`, { parse_mode: 'Markdown' }).catch(e => console.error("TG Send Error:", e.message));
            return; // { processed: false, reason: 'expired' };
        }

        // 10. Mark Bet as Paid in DB
        const markResult = await markBetPaid(bet.id, signature);
        if (!markResult.success) {
            console.error(`${logPrefix} Failed to mark bet ${bet.id} as paid: ${markResult.error}`);
             // If the error indicates it was already processed (e.g., signature collision), add to session cache
            if (markResult.error === 'Transaction signature already recorded' || markResult.error === 'Bet not found or already processed') {
                 processedSignaturesThisSession.add(signature);
                 return; // { processed: false, reason: 'db_mark_paid_collision' };
            }
             // For other DB errors, maybe don't add to session cache yet? Or handle differently.
            // For now, we just log the error and stop processing this TX for this bet.
            return; // { processed: false, reason: 'db_mark_paid_failed' };
        }
        // console.log(`${logPrefix} Successfully marked bet ${bet.id} as paid.`);


        // 11. Add to Processed Signatures Cache & Link Wallet
        processedSignaturesThisSession.add(signature);
         if (processedSignaturesThisSession.size > MAX_PROCESSED_SIGNATURES) {
              console.log('Clearing processed signatures cache (reached max size)');
              // Simple clear strategy: remove the oldest half (requires storing timestamps or using an LRU cache)
              // For simplicity now, just clear the whole set periodically or based on size.
              processedSignaturesThisSession.clear();
         }

        // Try to link the wallet using the determined payer address or fallback to fee payer
        let actualPayer = payerAddress || getPayerFromTransaction(tx)?.toBase58();
        if (actualPayer) {
            try {
                new PublicKey(actualPayer); // Validate address format before linking
                await linkUserWallet(bet.user_id, actualPayer);
            } catch(e) {
                console.warn(`${logPrefix} Identified payer address "${actualPayer}" for bet ${bet.id} is invalid or linking failed. Wallet not linked/updated. Error: ${e.message}`);
            }
        } else {
            console.warn(`${logPrefix} Could not reliably identify payer address for bet ${bet.id}. Wallet not linked.`);
        }

        // 12. Queue Bet for Game Processing (High Priority)
        console.log(`${logPrefix} Payment verified for bet ${bet.id}. Queuing for game processing.`);
        await this.addPaymentJob({
            type: 'process_bet',
            betId: bet.id,
            priority: 1, // Higher priority than scanning new payments
            signature // Pass signature for potential logging/tracing in processing step
        });

    } // End _continueProcessingWithMemo

} // End PaymentProcessor Class

const paymentProcessor = new PaymentProcessor();


let isMonitorRunning = false; // Flag to prevent overlapping monitor runs


const botStartupTime = Math.floor(Date.now() / 1000); // Store bot start time

let monitorIntervalSeconds = 90; // Initial check interval
let monitorInterval = null; // Holds the setInterval timer


async function monitorPayments() {
    if (isMonitorRunning) {
        // console.log('[Monitor] Skipping cycle: Previous run still active.');
        return;
    }
    if (!isFullyInitialized) {
        // console.log('[Monitor] Skipping cycle, application not fully initialized yet.');
        return;
    }

    isMonitorRunning = true;
    const startTime = Date.now();
    let signaturesFoundThisCycle = 0;
    let signaturesQueuedThisCycle = 0;

    try {
        // --- Optional: Dynamic Throttling based on Queue Load ---
        // const currentLoad = paymentProcessor.highPriorityQueue.pending +
        //                       paymentProcessor.normalQueue.pending +
        //                       paymentProcessor.highPriorityQueue.size +
        //                       paymentProcessor.normalQueue.size;
        // const baseDelay = 500; // Minimum delay between wallet checks
        // const delayPerItem = 100; // Add delay per item in queue
        // const maxThrottleDelay = 10000; // Max delay
        // const throttleDelay = Math.min(maxThrottleDelay, baseDelay + currentLoad * delayPerItem);
        // if (throttleDelay > baseDelay) {
        //     console.log(`[Monitor] Queues have ${currentLoad} pending/active items. Throttling monitor check for ${throttleDelay}ms.`);
        //     await new Promise(resolve => setTimeout(resolve, throttleDelay));
        // } else {
        //     await new Promise(resolve => setTimeout(resolve, baseDelay));
        // }
        // --- End Throttling ---

        // Add slight random delay to desynchronize multiple instances if scaled horizontally
        // await new Promise(resolve => setTimeout(resolve, Math.random() * 2000));


        // console.log("⚙️ [Monitor] Performing payment monitor run...");

        const monitoredWallets = [
             { address: process.env.MAIN_WALLET_ADDRESS, type: 'coinflip', priority: 0 }, // Normal priority for checks
             { address: process.env.RACE_WALLET_ADDRESS, type: 'race', priority: 0 },
        ];

        for (const wallet of monitoredWallets) {
            const walletAddress = wallet.address;
            let signaturesForWallet = [];

            try {
                const options = { limit: 25 }; // Fetch slightly more signatures
                // console.log(`[Monitor] Checking ${wallet.type} wallet (${walletAddress}) for latest ${options.limit} signatures.`);

                signaturesForWallet = await solanaConnection.getSignaturesForAddress(
                    new PublicKey(walletAddress),
                    options
                );

                if (!signaturesForWallet || signaturesForWallet.length === 0) {
                    // console.log(`[Monitor] No signatures found for wallet ${walletAddress}.`);
                    continue; // Skip to next wallet
                }

                signaturesFoundThisCycle += signaturesForWallet.length;

                 // Filter out very old signatures (e.g., older than bot startup + buffer)
                 // Keep a buffer (e.g., 300s = 5min) before startup time just in case.
                const recentSignatures = signaturesForWallet.filter(sigInfo => {
                    if (sigInfo.blockTime && sigInfo.blockTime < (botStartupTime - 300)) {
                         // console.log(`[Monitor] Filtering out old signature ${sigInfo.signature.slice(0,8)} (BlockTime: ${sigInfo.blockTime})`);
                        return false;
                    }
                    return true; // Keep recent or signatures without blockTime
                });

                if (recentSignatures.length === 0) {
                     // console.log(`[Monitor] No *recent* signatures found for wallet ${walletAddress}.`);
                    continue;
                }

                 // Process oldest recent signatures first by reversing
                recentSignatures.reverse();

                for (const sigInfo of recentSignatures) {
                     // Skip if already processed in this session or currently being processed
                    if (processedSignaturesThisSession.has(sigInfo.signature)) {
                        continue;
                    }
                    if (paymentProcessor.activeProcesses.has(sigInfo.signature)) {
                        continue;
                    }

                    // console.log(`[Monitor] Queuing signature ${sigInfo.signature.slice(0,8)}... for ${wallet.type} wallet.`);
                    signaturesQueuedThisCycle++;
                    await paymentProcessor.addPaymentJob({
                        type: 'monitor_payment',
                        signature: sigInfo.signature,
                        walletType: wallet.type,
                        priority: wallet.priority, // Use defined priority (e.g., 0 for normal checks)
                        retries: 0 // Initial retry count
                    });
                }

            } catch (error) {
                // ** UPDATED 429 Handling for getSignaturesForAddress **
                // This handles rate limits specifically during the signature fetching step.
                if (error?.message?.includes('429') || error?.statusCode === 429) {
                    const backoff = Math.min(60000, 5000 * Math.pow(2, error.attempt || 0)); // Exponential backoff, max 60s
                    console.warn(`[Monitor] RPC Rate Limited fetching signatures for ${walletAddress}. Backing off monitor for ${backoff}ms.`);
                    await new Promise(r => setTimeout(r, backoff)); // Wait before checking next wallet or finishing cycle

                     // Increase the *overall* monitor interval significantly on rate limit
                    monitorIntervalSeconds = Math.min(300, monitorIntervalSeconds * 1.5); // Increase interval, cap at 5min
                    console.log(`[Monitor] Increased monitor interval to ${monitorIntervalSeconds}s due to rate limit.`);

                     // Reset the interval timer with the new longer duration
                    if (monitorInterval) clearInterval(monitorInterval);
                    monitorInterval = setInterval(() => {
                         monitorPayments().catch(err => console.error('❌ [FATAL MONITOR ERROR in setInterval catch]:', err));
                    }, monitorIntervalSeconds * 1000);

                    // Skip the rest of the current monitor cycle after a rate limit hit
                    // continue; // Use 'return' to exit the entire monitorPayments function for this cycle
                    isMonitorRunning = false; // Ensure flag is reset before returning
                    return;
                } else {
                     // Handle other errors during signature fetching
                    console.error(`[Monitor] Error fetching/processing signatures for wallet ${walletAddress}:`, error.message);
                    performanceMonitor.logRequest(false);
                     // Decide if other errors should also trigger backoff or just be logged
                }
            } // End catch block for single wallet processing
        } // End loop through monitoredWallets

        // --- Optional: Reduce monitor interval if runs are consistently fast and no rate limits hit ---
        // const cycleDuration = Date.now() - startTime;
        // if (cycleDuration < 10000 && monitorIntervalSeconds > 45 && signaturesQueuedThisCycle === 0) { // Example condition
        //     monitorIntervalSeconds = Math.max(45, Math.floor(monitorIntervalSeconds / 1.2));
        //     console.log(`[Monitor] Cycle fast and quiet, reducing interval to ${monitorIntervalSeconds}s.`);
        //     // Reset interval timer with new duration
        //     if (monitorInterval) clearInterval(monitorInterval);
        //     monitorInterval = setInterval(() => { /* ... */ }, monitorIntervalSeconds * 1000);
        // }
        // --- End Interval Reduction ---


    } catch (err) {
         // Catch errors in the main monitor logic (outside the wallet loop)
        console.error('❌ MonitorPayments Error in main block:', err);
        performanceMonitor.logRequest(false);
         // Optional: Add global backoff here too if needed
    } finally {
        isMonitorRunning = false; // Release the lock
        const duration = Date.now() - startTime;
        // console.log(`[Monitor] Cycle completed in ${duration}ms. Found ${signaturesFoundThisCycle} total sigs. Queued ${signaturesQueuedThisCycle} new sigs for processing.`);
    }
}



async function sendSol(recipientPublicKey, amountLamports, gameType) {
    // Determine which bot wallet (and private key) to use based on game type
    const privateKey = gameType === 'coinflip'
        ? process.env.BOT_PRIVATE_KEY
        : process.env.RACE_BOT_PRIVATE_KEY;

    if (!privateKey) {
        console.error(`❌ Cannot send SOL: Missing private key for game type ${gameType}`);
        return { success: false, error: `Missing private key configuration for ${gameType}` };
    }

    let recipientPubKey;
    try {
         // Ensure recipient address is a valid PublicKey object
        recipientPubKey = (typeof recipientPublicKey === 'string')
            ? new PublicKey(recipientPublicKey)
            : recipientPublicKey; // Assume it's already a PublicKey if not string
        if (!(recipientPubKey instanceof PublicKey)) throw new Error("Invalid recipient public key type");
    } catch (e) {
        console.error(`❌ Invalid recipient address format: ${recipientPublicKey}`);
        return { success: false, error: `Invalid recipient address: ${e.message}` };
    }

    const amountSOL = Number(amountLamports) / LAMPORTS_PER_SOL;
    console.log(`💸 Attempting to send ${amountSOL.toFixed(6)} SOL to ${recipientPubKey.toBase58()} for ${gameType}`);

    // Calculate priority fee based on payout amount (adjust rate as needed)
    const basePriorityFee = 1000; // Minimum priority fee in microLamports
    const maxPriorityFee = 1000000; // Maximum priority fee (1 SOL = 1M microLamports)
    const calculatedFee = Math.floor(Number(amountLamports) * PRIORITY_FEE_RATE); // fee = amount * rate
    const priorityFeeMicroLamports = Math.max(basePriorityFee, Math.min(calculatedFee, maxPriorityFee));
    // console.log(`[SendSol] Using priority fee: ${priorityFeeMicroLamports} microLamports`);

    const amountToSend = BigInt(amountLamports);
    if (amountToSend <= 0n) {
        console.error(`❌ Calculated payout amount ${amountLamports} is zero or negative. Cannot send.`);
        return { success: false, error: 'Calculated payout amount is zero or negative' };
    }


    // Retry logic for sending transaction
    for (let attempt = 1; attempt <= 3; attempt++) {
        try {
            const payerWallet = Keypair.fromSecretKey(bs58.decode(privateKey));

             // Get a recent blockhash before each attempt
            const latestBlockhash = await solanaConnection.getLatestBlockhash(
                { commitment: 'confirmed' } // Use confirmed for blockhash fetching too
            );
            if (!latestBlockhash || !latestBlockhash.blockhash) {
                throw new Error('Failed to get latest blockhash');
            }

            const transaction = new Transaction({
                recentBlockhash: latestBlockhash.blockhash,
                feePayer: payerWallet.publicKey
            });

            // Add compute unit price instruction for priority
            transaction.add(
                ComputeBudgetProgram.setComputeUnitPrice({
                    microLamports: priorityFeeMicroLamports
                })
            );
             // Optional: Add compute unit limit if needed, though often not required for simple transfers
             // transaction.add(ComputeBudgetProgram.setComputeUnitLimit({ units: 200000 }));

            // Add the SOL transfer instruction
            transaction.add(
                SystemProgram.transfer({
                    fromPubkey: payerWallet.publicKey,
                    toPubkey: recipientPubKey,
                    lamports: amountToSend
                })
            );

            console.log(`[SendSol Attempt ${attempt}] Sending TX... Amount: ${amountToSend}, Prio Fee: ${priorityFeeMicroLamports} microLamports`);

            // Send and confirm the transaction with a timeout
            const confirmationTimeoutMs = 45000; // 45 seconds timeout for confirmation
            const signature = await Promise.race([
                sendAndConfirmTransaction(
                    solanaConnection,
                    transaction,
                    [payerWallet], // Signer
                    {
                        commitment: 'confirmed', // Wait for confirmation
                        skipPreflight: false, // Enable preflight checks
                        maxRetries: 2, // Retries within sendAndConfirm
                        preflightCommitment: 'confirmed'
                    }
                ),
                 // Timeout promise
                new Promise((_, reject) => {
                    setTimeout(() => {
                        reject(new Error(`Transaction confirmation timeout after ${confirmationTimeoutMs/1000}s (Attempt ${attempt})`));
                    }, confirmationTimeoutMs);
                })
            ]);

            console.log(`✅ Payout successful! Sent ${Number(amountToSend)/LAMPORTS_PER_SOL} SOL to ${recipientPubKey.toBase58()}. TX: ${signature}`);
            return { success: true, signature }; // Success!

        } catch (error) {
            console.error(`❌ Payout TX failed (Attempt ${attempt}/3):`, error.message);

            // Check if the error is non-retryable (e.g., insufficient funds, invalid account)
            const errorMsg = String(error.message || '').toLowerCase();
            if (errorMsg.includes('insufficient funds') ||
                errorMsg.includes('custom program error') || // Often indicates on-chain program failure
                errorMsg.includes('account not found') || // Recipient doesn't exist?
                errorMsg.includes('invalid recipient') ||
                errorMsg.includes('invalid account data') ||
                errorMsg.includes('invalid instruction data') // Non-retryable setup issues
               ) {
                console.error("❌ Non-retryable error encountered. Aborting payout.");
                return { success: false, error: `Non-retryable error: ${error.message}` };
            }

            // If retryable error and attempts remain, wait before next attempt
            if (attempt < 3) {
                const delay = 2000 * attempt; // Increasing delay
                console.log(`Retrying payout in ${delay}ms...`);
                await new Promise(resolve => setTimeout(resolve, delay));
            }
        } // End catch block
    } // End retry loop


    console.error(`❌ Payout failed after 3 attempts for ${recipientPubKey.toBase58()}`);
    return { success: false, error: `Payout failed after 3 attempts` };
}




async function processPaidBet(bet) {
    console.log(`⚙️ Processing game for bet ${bet.id} (${bet.game_type})`);
    let client;

    try {
        // Start a transaction to lock the bet row and update status atomically
        client = await pool.connect();
        await client.query('BEGIN');

        // Select the bet row FOR UPDATE to lock it
        const statusCheck = await client.query(
            'SELECT status FROM bets WHERE id = $1 FOR UPDATE',
            [bet.id]
        );

        // Double-check status after acquiring lock
        if (!statusCheck.rows[0] || statusCheck.rows[0].status !== 'payment_verified') {
            console.warn(`Bet ${bet.id} status is ${statusCheck.rows[0]?.status ?? 'not found'} after lock, not 'payment_verified'. Aborting game processing.`);
            await client.query('ROLLBACK'); // Rollback the transaction
            return; // Exit if status is wrong
        }

        // Update status to 'processing_game' within the transaction
        await client.query(
            'UPDATE bets SET status = $1 WHERE id = $2',
            ['processing_game', bet.id]
        );
        await client.query('COMMIT'); // Commit the status update

        // Now that status is 'processing_game', proceed with game logic outside the DB transaction
        if (bet.game_type === 'coinflip') {
            await handleCoinflipGame(bet);
        } else if (bet.game_type === 'race') {
            await handleRaceGame(bet);
        } else {
            console.error(`❌ Unknown game type '${bet.game_type}' for bet ${bet.id}`);
            await updateBetStatus(bet.id, 'error_unknown_game'); // Mark bet with error status
        }
    } catch (error) {
        console.error(`❌ Error during game processing setup for bet ${bet.id}:`, error.message);
        if (client) {
             // If an error occurred, try to rollback any pending transaction
            try { await client.query('ROLLBACK'); } catch (rbError) { console.error("Rollback failed:", rbError); }
        }
        // Mark bet with an error status if setup failed
        await updateBetStatus(bet.id, 'error_processing_setup');
    } finally {
        if (client) client.release(); // Always release the client back to the pool
    }
}



async function handleCoinflipGame(bet) {
    const { id: betId, user_id, chat_id, bet_details, expected_lamports } = bet;
    const choice = bet_details.choice; // 'heads' or 'tails'
    const config = GAME_CONFIG.coinflip;

    // Determine result: Apply house edge first, then random flip
    const houseWinRoll = Math.random();
    let result;
    let isHouseWin = houseWinRoll < config.houseEdge;

    if (isHouseWin) {
         // If house edge hits, make the result the opposite of user's choice
        result = (choice === 'heads') ? 'tails' : 'heads';
        console.log(`🪙 Coinflip Bet ${betId}: House edge triggered (Roll: ${houseWinRoll.toFixed(3)} < ${config.houseEdge}). Forced result: ${result}`);
    } else {
         // Normal random flip
        result = (Math.random() < 0.5) ? 'heads' : 'tails';
    }

    const win = (result === choice); // Did the user win?
    const payoutLamports = win ? calculatePayout(BigInt(expected_lamports), 'coinflip') : 0n;

    // Get user's display name for messages
    let displayName = await getUserDisplayName(chat_id, user_id);

    if (win) {
        const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
        console.log(`🪙 Coinflip Bet ${betId}: ${displayName} WON ${payoutSOL.toFixed(6)} SOL! Choice: ${choice}, Result: ${result}`);

        // Check if user has a linked wallet
        const winnerAddress = await getLinkedWallet(user_id);
        if (!winnerAddress) {
            console.warn(`Coinflip Bet ${betId}: Winner ${displayName} has no linked wallet.`);
            await safeSendMessage(chat_id,
                `🎉 ${displayName}, you won the coinflip (Result: *${result}*) but have no wallet linked!\n` +
                `Your payout of ${payoutSOL.toFixed(6)} SOL is waiting. Place another bet (any amount) to link your wallet and receive pending payouts.`,
                { parse_mode: 'Markdown' }
            ).catch(e => console.error("TG Send Error:", e.message));
            await updateBetStatus(betId, 'completed_win_no_wallet'); // Mark as won but needs wallet
            return;
        }

        // Announce win and queue payout
        try {
            await safeSendMessage(chat_id,
                `🎉 ${displayName}, you won ${payoutSOL.toFixed(6)} SOL!\n` +
                `Result: *${result}*\n\n` +
                `💸 Processing payout to your linked wallet...`,
                { parse_mode: 'Markdown' }
            ).catch(e => console.error("TG Send Error:", e.message));

            await updateBetStatus(betId, 'processing_payout'); // Update status before queueing

            // Add payout job to high priority queue
            await paymentProcessor.addPaymentJob({
                type: 'payout',
                betId,
                recipient: winnerAddress,
                amount: payoutLamports.toString(),
                gameType: 'coinflip',
                priority: 2, // High priority for payouts
                chatId: chat_id,
                displayName,
                result // Pass result for potential message customization
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

    } else { // User lost
        console.log(`🪙 Coinflip Bet ${betId}: ${displayName} LOST. Choice: ${choice}, Result: ${result}`);
        await safeSendMessage(chat_id,
            `❌ ${displayName}, you lost the coinflip!\n` +
            `You guessed *${choice}* but the result was *${result}*. Better luck next time!`,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
        await updateBetStatus(betId, 'completed_loss'); // Mark as completed loss
    }
}


async function handleRaceGame(bet) {
    const { id: betId, user_id, chat_id, bet_details, expected_lamports } = bet;
    const chosenHorseName = bet_details.horse;
    const config = GAME_CONFIG.race;

    // Define horses, odds, and base probabilities (adjust probabilities as needed for balance)
    const horses = [
         { name: 'Yellow', emoji: '🟡', odds: 1.1, baseProb: 0.25 }, { name: 'Orange', emoji: '🟠', odds: 2.0, baseProb: 0.20 },
         { name: 'Blue',   emoji: '🔵', odds: 3.0, baseProb: 0.15 }, { name: 'Cyan',   emoji: '💧', odds: 4.0, baseProb: 0.12 },
         { name: 'White',  emoji: '⚪️', odds: 5.0, baseProb: 0.09 }, { name: 'Red',    emoji: '🔴', odds: 6.0, baseProb: 0.07 },
         { name: 'Black',  emoji: '⚫️', odds: 7.0, baseProb: 0.05 }, { name: 'Pink',   emoji: '🌸', odds: 8.0, baseProb: 0.03 },
         { name: 'Purple', emoji: '🟣', odds: 9.0, baseProb: 0.02 }, { name: 'Green',  emoji: '🟢', odds: 10.0, baseProb: 0.01 },
         { name: 'Silver', emoji: '💎', odds: 15.0, baseProb: 0.01 }
    ];

    // Calculate total base probability and target probability after house edge
    const totalBaseProb = horses.reduce((sum, h) => sum + h.baseProb, 0);
    const targetTotalProb = 1.0 - config.houseEdge; // e.g., 0.98 if houseEdge is 0.02

    let winningHorse = null;
    const randomNumber = Math.random(); // Single random number determines outcome

    // Determine if the house edge is triggered
    if (randomNumber < targetTotalProb) {
         // Player win possible: Scale probabilities to fit the target range
        const playerWinRoll = randomNumber / targetTotalProb; // Normalize roll to [0, 1) within player win range
        let cumulativeProbability = 0;
        for (const horse of horses) {
            // Use normalized base probability
            cumulativeProbability += (horse.baseProb / totalBaseProb);
            if (playerWinRoll <= cumulativeProbability) {
                winningHorse = horse;
                break;
            }
        }
         // Fallback in case of floating point issues
        winningHorse = winningHorse || horses[horses.length - 1];
    } else {
         // House edge triggered: Select a winner randomly (could be any horse, doesn't matter for house win)
         // This ensures the overall probability distribution respects the house edge.
        const houseWinnerIndex = Math.floor(Math.random() * horses.length);
        winningHorse = horses[houseWinnerIndex];
        console.log(`🐎 Race Bet ${betId}: House edge triggered (Roll: ${randomNumber.toFixed(3)} >= ${targetTotalProb}). Random winner: ${winningHorse.name}`);
    }

    // Simulate race announcement
    let displayName = await getUserDisplayName(chat_id, user_id);
    try {
        await safeSendMessage(chat_id, `🐎 Race ${betId} starting! ${displayName} bet on ${chosenHorseName}!`).catch(e => console.error("TG Send Error:", e.message));
        await new Promise(resolve => setTimeout(resolve, 2000)); // Pause for effect
        await safeSendMessage(chat_id, "🚦 And they're off!").catch(e => console.error("TG Send Error:", e.message));
        await new Promise(resolve => setTimeout(resolve, 3000)); // Pause
        await safeSendMessage(chat_id,
            `🏆 The winner is... ${winningHorse.emoji} *${winningHorse.name}*! 🏆`,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
    } catch (e) {
        console.error(`Error sending race commentary for bet ${betId}:`, e);
    }

    // Determine if the user won (correct horse AND house edge didn't force a loss)
    const win = (randomNumber < targetTotalProb) && (chosenHorseName.toLowerCase() === winningHorse.name.toLowerCase());
    const payoutLamports = win
        ? calculatePayout(BigInt(expected_lamports), 'race', winningHorse) // Calculate payout based on winning horse odds
        : 0n;

    if (win) {
        const payoutSOL = Number(payoutLamports) / LAMPORTS_PER_SOL;
        console.log(`🐎 Race Bet ${betId}: ${displayName} WON ${payoutSOL.toFixed(6)} SOL! Horse: ${chosenHorseName}, Winner: ${winningHorse.name}`);

        // Check linked wallet
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

        // Announce win and queue payout
        try {
            await safeSendMessage(chat_id,
                `🎉 ${displayName}, your horse *${chosenHorseName}* won!\n` +
                `Payout: ${payoutSOL.toFixed(6)} SOL\n\n` +
                `💸 Processing payout to your linked wallet...`,
                { parse_mode: 'Markdown' }
            ).catch(e => console.error("TG Send Error:", e.message));

            await updateBetStatus(betId, 'processing_payout');

            // Add payout job
            await paymentProcessor.addPaymentJob({
                type: 'payout',
                betId,
                recipient: winnerAddress,
                amount: payoutLamports.toString(),
                gameType: 'race',
                priority: 2, // High priority
                chatId: chat_id,
                displayName,
                horseName: chosenHorseName, // Pass horse details for messages
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

    } else { // User lost
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

    // Safety check for zero/negative payout amount
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
        // Attempt to send the SOL
        const sendResult = await sendSol(recipient, payoutAmountLamports, gameType);

        if (sendResult.success) {
            const payoutSOL = Number(payoutAmountLamports) / LAMPORTS_PER_SOL;
            console.log(`💸 Payout successful for bet ${betId}, TX: ${sendResult.signature}`);
            // Send success message to user
            await safeSendMessage(chatId,
                `✅ Payout successful for bet ID \`${betId}\`!\n` +
                `${payoutSOL.toFixed(6)} SOL sent.\n` +
                `TX: \`https://solscan.io/tx/${sendResult.signature}\``, // Use Solscan link
                { parse_mode: 'Markdown', disable_web_page_preview: true }
            ).catch(e => console.error("TG Send Error:", e.message));
             // Record the successful payout in the database
            await recordPayout(betId, 'completed_win_paid', sendResult.signature);

        } else {
             // Payout failed after retries
            console.error(`❌ Payout failed permanently for bet ${betId}: ${sendResult.error}`);
            await safeSendMessage(chatId,
                `⚠️ Payout failed for bet ID \`${betId}\`: ${sendResult.error}\n` +
                `The team has been notified. Please contact support if needed.`,
                { parse_mode: 'Markdown' }
            ).catch(e => console.error("TG Send Error:", e.message));
             // Mark the bet as failed payout in the DB
            await updateBetStatus(betId, 'error_payout_failed');
        }
    } catch (error) {
         // Catch unexpected errors during the payout job execution
        console.error(`❌ Unexpected error processing payout job for bet ${betId}:`, error);
        await updateBetStatus(betId, 'error_payout_exception'); // Mark with generic payout error
        await safeSendMessage(chatId,
            `⚠️ A technical error occurred during payout for bet ID \`${betId}\`.\n` +
            `Please contact support.`,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
    }
}


function calculatePayout(betLamports, gameType, gameDetails = {}) {
    betLamports = BigInt(betLamports); // Ensure input is BigInt
    let payoutLamports = 0n;

    if (gameType === 'coinflip') {
        // Payout = Bet * (2 - HouseEdge)
        const multiplier = 2.0 - GAME_CONFIG.coinflip.houseEdge; // e.g., 2.0 - 0.02 = 1.98
        payoutLamports = BigInt(Math.floor(Number(betLamports) * multiplier));
    } else if (gameType === 'race' && gameDetails.odds) {
        // Payout = Bet * Odds * (1 - HouseEdge)
        const multiplier = gameDetails.odds * (1.0 - GAME_CONFIG.race.houseEdge); // e.g., 5.0 * (1.0 - 0.02) = 4.9
        payoutLamports = BigInt(Math.floor(Number(betLamports) * multiplier));
    } else {
        console.error(`Cannot calculate payout: Unknown game type ${gameType} or missing race odds.`);
        return 0n; // Return zero if calculation is not possible
    }

    // Ensure payout is not negative (shouldn't happen with positive inputs/multipliers)
    return payoutLamports > 0n ? payoutLamports : 0n;
}


async function getUserDisplayName(chat_id, user_id) {
    try {
        // Fetch chat member info to get username or first name
        const chatMember = await bot.getChatMember(chat_id, user_id);
        const user = chatMember.user;
        const username = user.username;
        // Prefer username if available and valid format
        if (username && /^[a-zA-Z0-9_]{5,32}$/.test(username)) {
            return `@${username}`;
        }
        // Fallback to first name (escape potential HTML tags)
        const firstName = user.first_name;
        if (firstName) {
            return firstName.replace(/</g, '&lt;').replace(/>/g, '&gt;');
        }
        // Absolute fallback if no name/username
        return `User ${String(user_id).substring(0, 6)}...`;
    } catch (e) {
         // Handle potential errors (e.g., user left the chat)
        if (e.response && e.response.statusCode === 400 && e.response.body?.description?.includes('user not found')) {
            console.warn(`User ${user_id} not found in chat ${chat_id}. Cannot get display name.`);
        } else if (e.response && e.response.statusCode === 403) {
             console.warn(`Bot might be blocked by user ${user_id} or kicked from chat ${chat_id}. Cannot get display name.`);
        }
        else {
            console.warn(`Couldn't get display name for user ${user_id} in chat ${chat_id}:`, e.message);
        }
        // Fallback display name on error
        return `User ${String(user_id).substring(0, 6)}...`;
    }
}


// --- Telegram Bot Event Handlers ---

bot.on('polling_error', (error) => {
    console.error(`❌ Polling error: ${error.code} - ${error.message}`);
    // Specific handling for 409 Conflict (likely another instance running)
    if (error.code === 'ETELEGRAM' && String(error.message).includes('409 Conflict')) {
        console.error("❌❌❌ FATAL: Conflict detected! Another bot instance might be running with the same token. Exiting to prevent issues.");
        console.error("❌ Exiting due to conflict.");
        process.exit(1); // Exit immediately
    }
    // Add handling for other specific polling errors if needed
});


bot.on('webhook_error', (error) => {
    console.error(`❌ Webhook error: ${error.code} - ${error.message}`);
    // Add handling for webhook specific errors if needed
});

bot.on('error', (error) => {
    console.error('❌ General Bot Error:', error);
    performanceMonitor.logRequest(false); // Log general errors too
});


// --- Main Message Handler ---

async function handleMessage(msg) {
    // Basic filtering for valid messages
    if (!msg || !msg.from || msg.from.is_bot || !msg.chat || !msg.text) {
        return; // Ignore irrelevant messages/events
    }

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const messageText = msg.text;

    try {
        // --- Command Cooldown Logic ---
        const now = Date.now();
        if (confirmCooldown.has(userId)) {
            const lastTime = confirmCooldown.get(userId);
            if (now - lastTime < cooldownInterval) {
                 // console.log(`User ${userId} throttled, command ignored.`);
                return; // Ignore command if user is on cooldown
            }
        }
        // Apply cooldown only if it's a command message
        if (messageText.startsWith('/')) {
            confirmCooldown.set(userId, now);
        }
        // --- End Cooldown Logic ---

        // Parse command and arguments
        const text = messageText.trim();
        // Regex to capture command (e.g., /bet) and the rest as arguments
        // Allows for optional bot username (@YourBotName) after command
        const commandMatch = text.match(/^\/(\w+)(?:@\w+)?(?:\s+(.*))?$/s); // Use 's' flag for multiline args

        if (!commandMatch) return; // Ignore non-command messages

        const command = commandMatch[1].toLowerCase(); // Command name (lowercase)
        const args = commandMatch[2] || ''; // Arguments string (or empty string if none)

        // Route command to appropriate handler
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
            case 'bet': // Handles /bet <amount> <heads|tails>
                await handleBetCommand(msg, args);
                break;
            case 'race':
                await handleRaceCommand(msg);
                break;
            case 'betrace': // Handles /betrace <amount> <horse_name>
                await handleBetRaceCommand(msg, args);
                break;
            case 'help':
                await handleHelpCommand(msg);
                break;
            // Add more commands here if needed
            default:
                 // Optional: Send message for unknown commands
                 // await safeSendMessage(chatId, "❓ Unknown command. Use /help to see available commands.");
                break;
        }

        performanceMonitor.logRequest(true); // Log successful command handling

    } catch (error) {
        console.error(`❌ Error processing message from user ${userId} in chat ${chatId}: "${messageText}"`, error);
        performanceMonitor.logRequest(false); // Log error
        try {
             // Send a generic error message to the user
            await safeSendMessage(chatId, "⚠️ An unexpected error occurred while processing your request. Please try again later or contact support if the issue persists.");
        } catch (tgError) {
            console.error("❌ Failed to send error message to chat:", tgError.message);
        }
    } finally {
        // --- Cooldown Cleanup ---
        // Periodically clean up old entries from the cooldown map (optional)
        const cutoff = Date.now() - 60000; // Remove entries older than 1 minute
        for (const [key, timestamp] of confirmCooldown.entries()) {
            if (timestamp < cutoff) {
                confirmCooldown.delete(key);
            }
        }
        // --- End Cooldown Cleanup ---
    }
}

// --- Command Handler Functions ---

async function handleStartCommand(msg) {
    const firstName = msg.from.first_name || 'there';
    // Sanitize name for MarkdownV2 or HTML
    const sanitizedFirstName = firstName.replace(/</g, '&lt;').replace(/>/g, '&gt;'); // Basic HTML escape
    const welcomeText = `👋 Welcome, ${sanitizedFirstName}!\n\n` +
                          `🎰 <b>Solana Gambles Bot</b> 🎰\n\n` +
                          `Use /coinflip or /race to see game options.\n` +
                          `Use /wallet to view or link your Solana wallet.\n` +
                          `Use /help to see all commands.\n\n` +
                          `Gamble responsibly!`;
    // Optional: Use an intro banner image/GIF
    // const bannerUrl = 'https://example.com/your-banner.gif';

    try {
        // Example using sendPhoto instead of sendAnimation
        // await bot.sendPhoto(msg.chat.id, bannerUrl || 'path/to/local/fallback.jpg', {
        await safeSendMessage(msg.chat.id, welcomeText, { // Send text directly if no banner
            // caption: welcomeText,
            parse_mode: 'HTML' // Use HTML for simple formatting
        });
        // }).catch(async (sendError) => {
        //     console.warn("⚠️ Failed to send start image/animation, sending text fallback:", sendError.message);
        //     await safeSendMessage(msg.chat.id, welcomeText, { parse_mode: 'HTML' });
        // });
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
                            `- Payout: ~${(2.0 * (1.0 - config.houseEdge)).toFixed(2)}x (Win Amount = Bet * ${(2.0 * (1.0 - config.houseEdge)).toFixed(2)})\n\n` + // Corrected payout display
                            `You will be given a wallet address and a <b>unique Memo ID</b>. Send the <b>exact</b> SOL amount with the memo to place your bet. Your sending wallet will be linked for payouts.`;

        await safeSendMessage(msg.chat.id, messageText, { parse_mode: 'HTML' });
    } catch (error) {
        console.error("Error in handleCoinflipCommand:", error);
        // Optionally send an error message to the user
        await safeSendMessage(msg.chat.id, "Sorry, there was an error displaying the Coinflip info.");
    }
}


async function handleRaceCommand(msg) {
    const horses = [
         { name: 'Yellow', emoji: '🟡', odds: 1.1 }, { name: 'Orange', emoji: '🟠', odds: 2.0 },
         { name: 'Blue',   emoji: '🔵', odds: 3.0 }, { name: 'Cyan',   emoji: '💧', odds: 4.0 },
         { name: 'White',  emoji: '⚪️', odds: 5.0 }, { name: 'Red',    emoji: '🔴', odds: 6.0 },
         { name: 'Black',  emoji: '⚫️', odds: 7.0 }, { name: 'Pink',   emoji: '🌸', odds: 8.0 },
         { name: 'Purple', emoji: '🟣', odds: 9.0 }, { name: 'Green',  emoji: '🟢', odds: 10.0 },
         { name: 'Silver', emoji: '💎', odds: 15.0 }
    ];

    let raceMessage = `🐎 *Horse Race Game* 🐎\n\nBet on the winning horse!\n\n*Available Horses & Approx Payout Multiplier* (Bet x Odds After House Edge):\n`;
    horses.forEach(horse => {
        // Calculate effective multiplier after house edge
        const effectiveMultiplier = (horse.odds * (1.0 - GAME_CONFIG.race.houseEdge)).toFixed(2);
        raceMessage += `- ${horse.emoji} *${horse.name}* (~${effectiveMultiplier}x Payout)\n`;
    });

    const config = GAME_CONFIG.race;
    raceMessage += `\n*How to play:*\n` +
                    `1. Type \`/betrace amount horse\\_name\`\n` + // Escape underscore for Markdown
                    `   (e.g., \`/betrace 0.1 Yellow\`)\n\n` +
                    `*Rules:*\n` +
                    `- Min Bet: ${config.minBet} SOL\n` +
                    `- Max Bet: ${config.maxBet} SOL\n` +
                    `- House Edge: ${(config.houseEdge * 100).toFixed(1)}% (applied to winnings)\n\n` +
                    `You will be given a wallet address and a *unique Memo ID*. Send the *exact* SOL amount with the memo to place your bet. Your sending wallet will be linked for payouts.`;

    await safeSendMessage(msg.chat.id, raceMessage, { parse_mode: 'MarkdownV2' }).catch(async mdv2Error => {
         // Fallback to Markdown if V2 fails (e.g., complex user input)
         console.warn("MarkdownV2 failed for /race, falling back to Markdown:", mdv2Error.message);
         // Need to reformat for basic Markdown (less escaping needed)
         let raceMessageMD = `🐎 **Horse Race Game** 🐎\n\nBet on the winning horse!\n\n**Available Horses & Approx Payout Multiplier**:\n`;
         horses.forEach(horse => {
             const effectiveMultiplier = (horse.odds * (1.0 - GAME_CONFIG.race.houseEdge)).toFixed(2);
             raceMessageMD += `- ${horse.emoji} **${horse.name}** (~${effectiveMultiplier}x Payout)\n`;
         });
         raceMessageMD += `\n**How to play:**\n` +
                         `1. Type \`/betrace amount horse_name\`\n` +
                         `   (e.g., \`/betrace 0.1 Yellow\`)\n\n` +
                         `**Rules:**\n` +
                         `- Min Bet: ${config.minBet} SOL\n` +
                         `- Max Bet: ${config.maxBet} SOL\n` +
                         `- House Edge: ${(config.houseEdge * 100).toFixed(1)}%\n\n` +
                         `Send the **exact** SOL amount with the **unique Memo ID** provided.`;
         await safeSendMessage(msg.chat.id, raceMessageMD, { parse_mode: 'Markdown' });
    });
}


async function handleWalletCommand(msg) {
    const userId = String(msg.from.id);
    const walletAddress = await getLinkedWallet(userId);

    if (walletAddress) {
        await safeSendMessage(msg.chat.id,
            `🔗 Your linked Solana wallet:\n\`${walletAddress}\`\n\n`+
            `Payouts will be sent here. This wallet was linked automatically when you made a paid bet. To change it, simply make a bet sending SOL from your new desired wallet.`,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
    } else {
        await safeSendMessage(msg.chat.id,
            `🔗 No wallet linked yet.\n\n` +
            `Place a bet and send the required SOL from your personal Solana wallet (like Phantom, Solflare, etc.). Your sending wallet will be automatically linked for future payouts.\n\n`+
            `*Do not send directly from an exchange.*`
        ).catch(e => console.error("TG Send Error:", e.message));
    }
}


async function handleBetCommand(msg, args) {
    // Regex to parse "<amount> <choice>"
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

    // Validate bet amount
    const betAmount = parseFloat(match[1]);
    if (isNaN(betAmount) || betAmount < config.minBet || betAmount > config.maxBet) {
        await safeSendMessage(chatId,
            `⚠️ Invalid bet amount. Please bet between ${config.minBet} and ${config.maxBet} SOL.\n` +
            `Example: \`/bet 0.1 heads\``,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
        return;
    }

    const userChoice = match[2].toLowerCase(); // 'heads' or 'tails'
    const memoId = generateMemoId('CF'); // Generate coinflip-specific memo
    const expectedLamports = BigInt(Math.round(betAmount * LAMPORTS_PER_SOL));
    const expiresAt = new Date(Date.now() + config.expiryMinutes * 60 * 1000); // Calculate expiry time

    // Save the pending bet details to the database
    const saveResult = await savePendingBet(
        userId, chatId, 'coinflip',
        { choice: userChoice }, // Store game-specific details
        expectedLamports, memoId, expiresAt
    );

    if (!saveResult.success) {
        await safeSendMessage(chatId, `⚠️ Error registering bet: ${saveResult.error}. Please try the command again.`).catch(e => console.error("TG Send Error:", e.message));
        return;
    }

    // Send instructions to the user
    // Use MarkdownV2 for better formatting control, fallback to Markdown
    const betAmountString = betAmount.toFixed(Math.max(2, (betAmount.toString().split('.')[1] || '').length)); // Format SOL amount accurately
    const messageText = `✅ Coinflip bet registered\\! \\(ID: \`${memoId}\`\\)\n\n` +
                        `You chose: *${userChoice}*\n` +
                        `Amount: *${betAmountString} SOL*\n\n` +
                        `➡️ Send *exactly ${betAmountString} SOL* to:\n` +
                        `\`${process.env.MAIN_WALLET_ADDRESS}\`\n\n` +
                        `📎 *Include MEMO:* \`${memoId}\`\n\n` +
                        `⏱️ This request expires in ${config.expiryMinutes} minutes\\.\n\n` +
                        `*IMPORTANT:* Send from your own wallet \\(Phantom, Solflare, etc\\), *not* an exchange\\. Ensure you include the memo correctly\\.`;

    await safeSendMessage(chatId, messageText, { parse_mode: 'MarkdownV2', disable_web_page_preview: true }).catch(async mdv2Error => {
         console.warn("MarkdownV2 failed for /bet response, falling back to Markdown:", mdv2Error.message);
         const messageTextMD = `✅ Coinflip bet registered! (ID: \`${memoId}\`)\n\n` +
                             `You chose: **${userChoice}**\n` +
                             `Amount: **${betAmountString} SOL**\n\n` +
                             `➡️ Send **exactly ${betAmountString} SOL** to:\n` +
                             `\`${process.env.MAIN_WALLET_ADDRESS}\`\n\n` +
                             `📎 **Include MEMO:** \`${memoId}\`\n\n` +
                             `⏱️ This request expires in ${config.expiryMinutes} minutes.\n\n` +
                             `**IMPORTANT:** Send from your own wallet, **not** an exchange. Include the memo.`;
         await safeSendMessage(chatId, messageTextMD, { parse_mode: 'Markdown', disable_web_page_preview: true });
    });
}


async function handleBetRaceCommand(msg, args) {
    // Regex to parse "<amount> <horse_name>"
    const match = args.trim().match(/^(\d+\.?\d*)\s+([\w\s]+)/i); // Allow spaces in horse name initially
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

    // Validate amount
    const betAmount = parseFloat(match[1]);
    if (isNaN(betAmount) || betAmount < config.minBet || betAmount > config.maxBet) {
        await safeSendMessage(chatId,
            `⚠️ Invalid bet amount. Please bet between ${config.minBet} and ${config.maxBet} SOL.\n`+
            `Example: \`/betrace 0.1 Yellow\``,
            { parse_mode: 'Markdown' }
        ).catch(e => console.error("TG Send Error:", e.message));
        return;
    }

    // Validate horse name
    const chosenHorseNameInput = match[2].trim(); // Trim whitespace from horse name
    const horses = [ // Keep this consistent with handleRaceCommand
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

    // Generate memo, calculate lamports, expiry
    const memoId = generateMemoId('RA'); // Race-specific memo prefix
    const expectedLamports = BigInt(Math.round(betAmount * LAMPORTS_PER_SOL));
    const expiresAt = new Date(Date.now() + config.expiryMinutes * 60 * 1000);

    // Save pending bet
    const saveResult = await savePendingBet(
        userId, chatId, 'race',
        { horse: chosenHorse.name, odds: chosenHorse.odds }, // Store chosen horse and its odds
        expectedLamports, memoId, expiresAt
    );

    if (!saveResult.success) {
        await safeSendMessage(chatId, `⚠️ Error registering bet: ${saveResult.error}. Please try the command again.`).catch(e => console.error("TG Send Error:", e.message));
        return;
    }

    // Calculate potential payout for display
    const potentialPayoutLamports = calculatePayout(expectedLamports, 'race', chosenHorse);
    const potentialPayoutSOL = (Number(potentialPayoutLamports) / LAMPORTS_PER_SOL).toFixed(6);
    const betAmountString = betAmount.toFixed(Math.max(2, (betAmount.toString().split('.')[1] || '').length));

    // Send instructions (using MarkdownV2 with fallback)
    const messageText = `✅ Race bet registered\\! \\(ID: \`${memoId}\`\\)\n\n` +
                        `You chose: ${chosenHorse.emoji} *${chosenHorse.name}*\n` +
                        `Amount: *${betAmountString} SOL*\n` +
                        `Potential Payout: ~*${potentialPayoutSOL} SOL*\n\n`+ // Display potential payout
                        `➡️ Send *exactly ${betAmountString} SOL* to:\n` +
                        `\`${process.env.RACE_WALLET_ADDRESS}\`\n\n` + // Use RACE wallet
                        `📎 *Include MEMO:* \`${memoId}\`\n\n` +
                        `⏱️ This request expires in ${config.expiryMinutes} minutes\\.\n\n` +
                        `*IMPORTANT:* Send from your own wallet \\(Phantom, Solflare, etc\\), *not* an exchange\\. Ensure you include the memo correctly\\.`;


    await safeSendMessage(chatId, messageText, { parse_mode: 'MarkdownV2', disable_web_page_preview: true }).catch(async mdv2Error => {
         console.warn("MarkdownV2 failed for /betrace response, falling back to Markdown:", mdv2Error.message);
         const messageTextMD = `✅ Race bet registered! (ID: \`${memoId}\`)\n\n` +
                             `You chose: ${chosenHorse.emoji} **${chosenHorse.name}**\n` +
                             `Amount: **${betAmountString} SOL**\n` +
                             `Potential Payout: ~**${potentialPayoutSOL} SOL**\n\n`+
                             `➡️ Send **exactly ${betAmountString} SOL** to:\n` +
                             `\`${process.env.RACE_WALLET_ADDRESS}\`\n\n` +
                             `📎 **Include MEMO:** \`${memoId}\`\n\n` +
                             `⏱️ This request expires in ${config.expiryMinutes} minutes.\n\n` +
                             `**IMPORTANT:** Send from your own wallet, **not** an exchange. Include the memo.`;
         await safeSendMessage(chatId, messageTextMD, { parse_mode: 'Markdown', disable_web_page_preview: true });
    });
}


async function handleHelpCommand(msg) {
    // Use MarkdownV2 with escaping, fallback to Markdown
    const helpText = `*Solana Gambles Bot Commands* 🎰\n\n` +
                    `/start \\- Show welcome message\n` +
                    `/help \\- Show this help message\n\n` +
                    `*Games:*\n` +
                    `/coinflip \\- Show Coinflip game info & how to bet\n` +
                    `/race \\- Show Horse Race game info & how to bet\n\n` +
                    `*Betting:*\n` +
                    `/bet <amount> <heads|tails> \\- Place a Coinflip bet\n` +
                    `Example: \`/bet 0\\.1 heads\`\n` + // Escaped dot
                    `/betrace <amount> <horse\\_name> \\- Place a Race bet\n` + // Escaped underscore
                    `Example: \`/betrace 0\\.1 Yellow\`\n\n` +
                    `*Wallet:*\n` +
                    `/wallet \\- View your linked Solana wallet for payouts\n\n` +
                    `*Support:* If you encounter issues, please provide details like the Memo ID or Transaction Signature if possible\\.`;

    await safeSendMessage(msg.chat.id, helpText, { parse_mode: 'MarkdownV2' }).catch(async mdv2Error => {
        console.warn("MarkdownV2 failed for /help, falling back to Markdown:", mdv2Error.message);
        const helpTextMD = `**Solana Gambles Bot Commands** 🎰\n\n` +
                        `/start - Show welcome message\n` +
                        `/help - Show this help message\n\n` +
                        `**Games:**\n` +
                        `/coinflip - Show Coinflip game info & how to bet\n` +
                        `/race - Show Horse Race game info & how to bet\n\n` +
                        `**Betting:**\n` +
                        `/bet <amount> <heads|tails> - Place a Coinflip bet\n` +
                        `Example: \`/bet 0.1 heads\`\n` +
                        `/betrace <amount> <horse_name> - Place a Race bet\n` +
                        `Example: \`/betrace 0.1 Yellow\`\n\n` +
                        `**Wallet:**\n` +
                        `/wallet - View your linked Solana wallet for payouts\n\n` +
                        `**Support:** If you encounter issues, provide details like Memo ID or TX Signature.`;
        await safeSendMessage(msg.chat.id, helpTextMD, { parse_mode: 'Markdown' });
    });
}


// --- Webhook / Polling Setup ---

async function setupTelegramWebhook() {
    // Only setup webhook if running in Railway env and domain is provided
    if (process.env.RAILWAY_ENVIRONMENT && process.env.RAILWAY_PUBLIC_DOMAIN) {
        const webhookUrl = `https://${process.env.RAILWAY_PUBLIC_DOMAIN}${webhookPath}`;
        console.log(`Attempting to set webhook to: ${webhookUrl}`);
        let attempts = 0;
        const maxAttempts = 3;
        while (attempts < maxAttempts) {
            try {
                // Ensure no old webhook/polling is active
                await bot.deleteWebHook({ drop_pending_updates: true }); // Drop pending updates during setup
                // Set the new webhook
                const success = await bot.setWebHook(webhookUrl, {
                    max_connections: 5, // Adjust as needed
                     // allowed_updates: ["message", "callback_query"] // Specify desired update types
                });
                if (success) {
                   console.log(`✅ Webhook successfully set to: ${webhookUrl}`);
                   return true;
                } else {
                   throw new Error("setWebHook returned false");
                }
            } catch (webhookError) {
                attempts++;
                console.error(`❌ Webhook setup attempt ${attempts}/${maxAttempts} failed:`, webhookError.message);
                if (attempts >= maxAttempts) {
                    console.error("❌ Max webhook setup attempts reached. Will likely fallback to polling if enabled.");
                    return false;
                }
                // Wait before retrying
                await new Promise(resolve => setTimeout(resolve, 3000 * attempts));
            }
        }
    } else {
        console.log("ℹ️ Not in Railway environment or RAILWAY_PUBLIC_DOMAIN not set. Webhook setup skipped.");
        return false;
    }
    return false; // Should not be reached if loop completes successfully
}


async function startPollingIfNeeded() {
    try {
        const info = await bot.getWebHookInfo();
        // If no webhook URL is set, start polling
        if (!info || !info.url) {
            // Check if already polling to avoid conflicts
             if (bot.isPolling()) {
                console.log("ℹ️ Bot is already polling.");
                return;
             }
            console.log("ℹ️ Webhook not set, starting bot polling...");
            await bot.deleteWebHook({ drop_pending_updates: true }); // Ensure webhook is off before polling
            // Start polling with options (e.g., interval)
            await bot.startPolling({ polling: { interval: 300 } }); // 300ms interval
            console.log("✅ Bot polling started successfully");
        } else {
            console.log(`ℹ️ Webhook is set (${info.url}), polling will not be started.`);
             // If somehow polling is active while webhook is set, stop polling
             if (bot.isPolling()) {
                console.log("ℹ️ Stopping polling because webhook is set.");
                await bot.stopPolling({ cancel: true }); // Cancel active polling requests
             }
        }
    } catch (err) {
        console.error("❌ Error managing polling state:", err.message);
        if (String(err.message).includes('409 Conflict')) {
            console.error("❌❌❌ Conflict detected! Another instance might be polling or webhook mismatch.");
            console.error("❌ Exiting due to conflict."); process.exit(1);
        }
        // Handle other errors (e.g., network issues connecting to Telegram API)
    }
}


// --- Payment Monitor Control ---

function startPaymentMonitor() {
    if (monitorInterval) {
         console.log("ℹ️ Payment monitor already running.");
         return;
    }
    console.log(`⚙️ Starting payment monitor (Initial Interval: ${monitorIntervalSeconds}s)`);
    // Define the monitoring function to be called repeatedly
    const monitorTask = () => {
         try {
             // Wrap the async monitorPayments call in a self-executing async function
             // to catch potential synchronous errors within the interval callback itself
             (async () => {
                 await monitorPayments();
             })().catch(err => { // Catch errors from the async function execution
                 console.error('[FATAL MONITOR ASYNC ERROR in setInterval catch]:', err);
                 performanceMonitor.logRequest(false);
                 // Consider if monitor should stop or attempt recovery on fatal errors
             });
         } catch (syncErr) { // Catch synchronous errors in the interval callback setup
             console.error('[FATAL MONITOR SYNC ERROR in setInterval try/catch]:', syncErr);
             performanceMonitor.logRequest(false);
             // Probably stop the monitor if this happens
             if (monitorInterval) clearInterval(monitorInterval);
             monitorInterval = null;
         }
    };

    // Start the interval timer
    monitorInterval = setInterval(monitorTask, monitorIntervalSeconds * 1000);

    // Perform an initial run shortly after startup
    console.log("⚙️ Performing initial payment monitor run in 5 seconds...");
    setTimeout(monitorTask, 5000);
}


// --- Graceful Shutdown Logic ---

const shutdown = async (signal, isRailwayRotation = false) => {
    console.log(`\n🛑 ${signal} received, initiating ${isRailwayRotation ? 'container rotation handling' : 'graceful shutdown'}...`);

    // For Railway rotation, we might only stop the server and let Railway handle the rest.
    // For full shutdown (SIGINT, error), we do the complete sequence.
    if (isRailwayRotation) {
        console.log("Railway container rotation detected - stopping new connections only.");
        if (server) {
             // Close the server to stop accepting new HTTP requests
            server.close((err) => {
                if (err) console.error("⚠️ Error closing Express server during rotation:", err.message);
                else console.log("- Stopped accepting new server connections for rotation.");
                 // Railway might force kill after this, so we don't wait for queues necessarily.
            });
        }
        // Don't exit the process here for rotation, let Railway manage it.
        return;
    }

    // --- Full Shutdown Sequence ---
    console.log("Performing full graceful shutdown sequence...");
    isMonitorRunning = true; // Prevent monitor from starting new cycles

    console.log("1. Stopping incoming connections & tasks...");
    // Stop the payment monitor interval
    if (monitorInterval) {
        clearInterval(monitorInterval);
        console.log("- Stopped payment monitor interval.");
        monitorInterval = null;
    }
    // Stop the Express server
    if (server) {
        try {
            await new Promise((resolve, reject) => {
                server.close((err) => {
                    if (err) {
                        console.error("⚠️ Error closing Express server:", err.message);
                        return reject(err); // Reject promise on error
                    }
                    console.log("- Express server closed.");
                    resolve(undefined); // Resolve promise on successful close
                });
                 // Add a timeout for server closing
                setTimeout(() => reject(new Error('Server close timeout (5s)')), 5000).unref();
            });
        } catch (serverErr) {
            console.warn("⚠️ Could not close server gracefully:", serverErr.message);
        }
    }
    // Stop Telegram interactions (webhook removal / polling stop)
    try {
        const webhookInfo = await bot.getWebHookInfo().catch(() => null); // Ignore errors getting info
        if (webhookInfo && webhookInfo.url) {
            await bot.deleteWebHook({ drop_pending_updates: false }); // Keep updates to finish processing? Or true?
            console.log("- Removed Telegram webhook.");
        } else if (bot.isPolling()) {
            await bot.stopPolling({ cancel: true }); // Cancel active poll requests
            console.log("- Stopped Telegram polling.");
        }
    } catch (tgErr) {
        console.error("⚠️ Error stopping Telegram listeners:", tgErr.message);
    }

    console.log("2. Waiting for active jobs to finish (max 15s)...");
    try {
         // Wait for all queues to become idle, with a timeout
        await Promise.race([
            Promise.all([
                messageQueue.onIdle(), // Telegram message processing queue
                paymentProcessor.highPriorityQueue.onIdle(), // High priority payment/payout queue
                paymentProcessor.normalQueue.onIdle(), // Normal priority payment check queue
                telegramSendQueue.onIdle() // Telegram message sending queue
            ]),
            // Timeout promise
            new Promise((_, reject) => setTimeout(() => reject(new Error('Queue drain timeout (15s)')), 15000))
        ]);
        console.log("- All processing queues are idle.");
    } catch (queueError) {
        console.warn(`⚠️ Timed out waiting for queues or queue error during shutdown: ${queueError.message}. Pending tasks may be lost.`);
        // Log queue sizes for debugging
        console.log(`Queue Stats at Timeout: Msg=${messageQueue.size}/${messageQueue.pending}, HP=${paymentProcessor.highPriorityQueue.size}/${paymentProcessor.highPriorityQueue.pending}, NP=${paymentProcessor.normalQueue.size}/${paymentProcessor.normalQueue.pending}, TGSend=${telegramSendQueue.size}/${telegramSendQueue.pending}`);
    }

    console.log("3. Closing database pool...");
    try {
        await pool.end(); // Close all connections in the pool
        console.log("✅ Database pool closed.");
    } catch (dbErr) {
        console.error("❌ Error closing database pool:", dbErr);
    } finally {
        console.log("🛑 Full Shutdown sequence complete.");
        process.exit(0); // Exit cleanly
    }
};


// --- Process Signal Handling ---

process.on('SIGTERM', () => {
    // SIGTERM is often sent by hosting platforms (like Railway, Docker) for shutdown/rotation
    const isRailwayRotation = !!process.env.RAILWAY_ENVIRONMENT; // Check if likely a Railway rotation
    console.log(`SIGTERM received. Railway Environment: ${isRailwayRotation}`);
    shutdown('SIGTERM', isRailwayRotation).catch((err) => {
        console.error("Error during SIGTERM shutdown:", err);
        process.exit(1); // Exit with error code if shutdown fails
    });
});

process.on('SIGINT', () => {
    // SIGINT is typically sent by Ctrl+C in the terminal
    console.log(`SIGINT received (Ctrl+C).`);
    shutdown('SIGINT', false).catch((err) => { // Treat as full shutdown
        console.error("Error during SIGINT shutdown:", err);
        process.exit(1);
    });
});


process.on('uncaughtException', (err, origin) => {
    console.error(`🔥🔥🔥 Uncaught Exception at: ${origin}`, err);
    // Attempt a very quick, minimal shutdown on uncaught exceptions
    // Avoid complex async operations here as the state is unknown
    console.error("Attempting emergency shutdown due to uncaught exception...");
    // Maybe just try closing DB pool synchronously if possible?
    pool.end().catch(dbErr => console.error("Error closing DB pool during emergency shutdown:", dbErr));
    console.error("Exiting immediately after uncaught exception.");
    process.exit(1); // Exit immediately with error code

    // Previous logic with timeout (might be too slow/unreliable after uncaught exception)
    // shutdown('UNCAUGHT_EXCEPTION', false).catch(() => process.exit(1));
    // setTimeout(() => {
    //     console.error("Shutdown timed out after uncaught exception. Forcing exit.");
    //     process.exit(1);
    // }, 12000).unref();
});


process.on('unhandledRejection', (reason, promise) => {
    console.error('🔥🔥🔥 Unhandled Rejection at:', promise, 'reason:', reason);
    // Decide how to handle unhandled promise rejections.
    // Depending on the severity, you might log it, attempt shutdown, or let the process continue.
    // For critical rejections, shutdown might be appropriate:
    // console.error("Attempting shutdown due to unhandled rejection...");
    // shutdown('UNHANDLED_REJECTION', false).catch(() => process.exit(1));
    // For less critical ones, just logging might suffice.
});


// --- Server Startup ---

const PORT = process.env.PORT || 3000;

// Start the Express server
server = app.listen(PORT, "0.0.0.0", () => { // Listen on all interfaces
    console.log(`🚀 Server running on port ${PORT}`);

    // --- Delayed Initialization ---
    // Start background tasks slightly after the server starts listening
    // This ensures the server is responsive quickly for health checks.
    setTimeout(async () => {
        console.log("⚙️ Starting delayed background initialization...");
        try {
             // 1. Initialize Database
            console.log("  - Initializing Database...");
            await initializeDatabase();

             // 2. Setup Telegram Connection (Webhook or Polling)
            console.log("  - Setting up Telegram...");
            const webhookSet = await setupTelegramWebhook();
            if (!webhookSet) { // If webhook setup failed or was skipped
                 await startPollingIfNeeded(); // Start polling as fallback
            }

             // 3. Start Payment Monitor
            console.log("  - Starting Payment Monitor...");
            startPaymentMonitor(); // Start the background monitoring loop

            // 4. Adjust Solana Connection Concurrency (Optional)
            // If needed, adjust concurrency after initial load period
            // setTimeout(() => {
            //      if (solanaConnection && solanaConnection.options) {
            //           console.log("⚡ Adjusting Solana connection concurrency...");
            //           solanaConnection.options.maxConcurrent = 5; // Example adjustment
            //           console.log(`✅ Solana maxConcurrent adjusted to ${solanaConnection.options.maxConcurrent}`);
            //      }
            // }, 20000); // Adjust after 20 seconds

            isFullyInitialized = true; // Mark the bot as ready
            console.log("✅ Delayed Background Initialization Complete. Bot is fully ready.");
            console.log("🚀🚀🚀 Solana Gambles Bot is up and running! 🚀🚀🚀");

        } catch (initError) {
            console.error("🔥🔥🔥 Delayed Background Initialization Error:", initError);
            console.error("❌ Bot failed to initialize critical components. Shutting down...");
            // Attempt graceful shutdown on initialization failure
            await shutdown('INITIALIZATION_FAILURE', false).catch(() => process.exit(1)); // Exit if shutdown fails
             // Force exit if shutdown hangs
             setTimeout(() => {
                 console.error("Shutdown timed out after initialization failure. Forcing exit.");
                 process.exit(1);
             }, 10000).unref();
        }
    }, 1000); // Start initialization 1 second after server listens
});

// Handle server startup errors (e.g., port already in use)
server.on('error', (err) => {
    console.error('❌ Server startup error:', err);
    if (err.code === 'EADDRINUSE') {
        console.error(`❌❌❌ Port ${PORT} is already in use. Is another instance running? Exiting.`);
    }
    process.exit(1); // Exit if server cannot start
});

// --- END OF FILE ---
