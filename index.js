// index.js - Part 1: Imports, Environment Configuration, Core Initializations, Utility Definitions
// --- VERSION: 3.2.1p --- (Applied Fixes: Roulette MdV2 parens verified, War rename verified, Added Emojis to showBetAmountButtons)

// --- All Imports MUST come first ---
import 'dotenv/config'; // Keep for local development via .env file
import { Pool } from 'pg';
import express from 'express';
import TelegramBot from 'node-telegram-bot-api';
import {
    Connection,
    PublicKey,
    LAMPORTS_PER_SOL,
    Keypair,
    Transaction,
    SystemProgram,
    sendAndConfirmTransaction,
    ComputeBudgetProgram,
    SendTransactionError, // Corrected import name
    TransactionExpiredBlockheightExceededError
} from '@solana/web3.js';
import bs58 from 'bs58';
import * as crypto from 'crypto'; // Keep for crypto.randomBytes
import { createHash } from 'crypto'; // Keep for createHash specifically
import PQueue from 'p-queue';
import { Buffer } from 'buffer';
import bip39 from 'bip39';
import { derivePath } from 'ed25519-hd-key';
import nacl from 'tweetnacl';

// Import rate-limited connection wrapper
import RateLimitedConnection from './lib/solana-connection.js'; // Assuming this path is correct

// --- Utility function definitions ---
/**
 * Creates a promise that resolves after a specified number of milliseconds.
 * @param {number} ms Milliseconds to sleep.
 * @returns {Promise<void>}
 */
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Escapes characters for Telegram MarkdownV2 parse mode.
 * Handles null/undefined inputs.
 * Characters to escape: _ * [ ] ( ) ~ ` > # + - = | { } . ! \
 * @param {string | number | bigint | null | undefined} text Input text to escape.
 * @returns {string} Escaped text.
 */
const escapeMarkdownV2 = (text) => {
    if (text === null || typeof text === 'undefined') {
        return '';
    }
    const textString = String(text);
    // Escape reserved characters by prefixing them with a backslash
    // IMPORTANT: Need to escape the backslash itself LAST.
    return textString.replace(/([_*\[\]()~`>#+\-=|{}.!\\])/g, '\\$1');
};

// --- Moved Functions from Part 5b ---

/**
 * Handles admin commands. Checks authorization.
 * @param {import('node-telegram-bot-api').Message} msg The Telegram message object.
 * @param {string[]} argsArray Command arguments array (index 0 is command itself if from msg).
 * @param {string | null} [correctUserIdFromCb=null] Optional: Correct user ID if called from callback. (Note: Admin commands should generally only come from direct messages)
 */
async function handleAdminCommand(msg, argsArray, correctUserIdFromCb = null) {
    // Admin commands must originate from a direct message, use msg.from.id
    const userId = String(msg.from.id);
    const chatId = msg.chat.id;
    // Args array from direct message includes command, so sub-command is index 1
    const command = argsArray[1]?.toLowerCase();
    const subArgs = argsArray.slice(2);
    const logPrefix = `[AdminCmd User ${userId}]`;

    // Double check authorization using the message sender's ID
    if (!process.env.ADMIN_USER_IDS.split(',').map(id=>id.trim()).includes(userId)) {
         // MarkdownV2 Safety: Escape static message
        return safeSendMessage(chatId, "You are not authorized to use admin commands\\.", { parse_mode: 'MarkdownV2' });
    }

    console.log(`${logPrefix} Received admin command: ${command}, args: ${subArgs.join(' ')}`);
    // MarkdownV2 Safety: Default reply needs escaping. Escape all specific replies below too.
    let reply = "Admin command executed\\."; // Escaped .

    switch (command) {
        case 'broadcast':
            if (subArgs.length === 0) {
                // MarkdownV2 Safety: Escape command usage example
                reply = "Usage: /admin broadcast \\<message\\_to\\_send\\>";
            } else {
                const broadcastMessage = subArgs.join(' ');
                console.log(`${logPrefix} Broadcasting: ${broadcastMessage}`);
                // TODO: Implement actual broadcast logic
                // MarkdownV2 Safety: Escape the preview and punctuation
                reply = `Broadcast initiated with message: "${escapeMarkdownV2(broadcastMessage.substring(0,50))}\\.\\.\\." \\(Actual broadcast not yet implemented\\)\\.`; // Escaped .()
                // notifyAdmin function expects already escaped message for main content
                await notifyAdmin(`Admin ${userId} initiated broadcast: ${escapeMarkdownV2(broadcastMessage)}`);
            }
            break;
        case 'checkbalance': // Check bot's main wallet balance
            const mainKey = process.env.MAIN_BOT_PRIVATE_KEY;
            if (!mainKey) { reply = "Main bot private key not set\\."; break; } // Escaped .
            try {
                const keypair = Keypair.fromSecretKey(bs58.decode(mainKey));
                const balance = await solanaConnection.getBalance(keypair.publicKey);
                // MarkdownV2 Safety: Escape wallet address, balance, and punctuation
                reply = `Main bot wallet \\(${escapeMarkdownV2(keypair.publicKey.toBase58())}\\) balance: ${escapeMarkdownV2(formatSol(balance))} SOL\\.`; // Escaped .()
            } catch (e) {
                // MarkdownV2 Safety: Escape error message
                reply = `Error checking main balance: ${escapeMarkdownV2(e.message)}`;
            }
            break;
        case 'userinfo':
            const targetUserId = subArgs[0];
            if (!targetUserId) { reply = "Usage: /admin userinfo \\<user\\_id\\>"; break; } // Escaped <>
            const targetUserIdString = String(targetUserId); // Ensure string
            try {
                 const userInfo = await getUserWalletDetails(targetUserIdString); // from Part 2
                 const userBal = await getUserBalance(targetUserIdString); // from Part 2
                 if (userInfo) {
                      // MarkdownV2 Safety: Escape all dynamic values and static labels/punctuation, use backticks for code blocks
                      reply = `*User Info for ${escapeMarkdownV2(targetUserIdString)}:*\n` +
                              `Balance: \`${escapeMarkdownV2(formatSol(userBal))} SOL\`\n` +
                              `Wallet: \`${escapeMarkdownV2(userInfo.external_withdrawal_address || 'N/A')}\`\n` +
                              `Referral Code: \`${escapeMarkdownV2(userInfo.referral_code || 'N/A')}\`\n` +
                              `Referred By: \`${escapeMarkdownV2(userInfo.referred_by_user_id || 'N/A')}\`\n` +
                              `Referrals: \`${escapeMarkdownV2(String(userInfo.referral_count || 0))}\`\n`+
                              `Total Wagered: \`${escapeMarkdownV2(formatSol(userInfo.total_wagered || 0n))} SOL\``;
                 } else {
                       // MarkdownV2 Safety: Escape target user ID
                      reply = `User \`${escapeMarkdownV2(targetUserIdString)}\` not found\\.`; // Escaped .
                 }
            } catch (dbError) {
                 console.error(`${logPrefix} DB error fetching user info for ${targetUserIdString}: ${dbError.message}`);
                  // MarkdownV2 Safety: Escape target user ID and static text
                 reply = `Database error fetching info for user \`${escapeMarkdownV2(targetUserIdString)}\`\\.`; // Escaped .
            }
            break;
        // Add more admin commands here
        default:
             // MarkdownV2 Safety: Escape command name and punctuation
            reply = `Unknown admin command: \`${escapeMarkdownV2(command || 'N/A')}\`\\.\nAvailable: \`broadcast\`, \`checkbalance\`, \`userinfo\``; // Escaped .
            break;
    }
    // Admin replies are sent with MarkdownV2
    await safeSendMessage(chatId, reply, {parse_mode: 'MarkdownV2'});
}

/**
 * Checks for and processes referral bonuses after a bet completes successfully.
 * Runs within the provided database client's transaction.
 * @param {string} refereeUserId - The user who placed the bet.
 * @param {number} completedBetId - The ID of the completed bet.
 * @param {bigint} wagerAmountLamports - The amount wagered in the bet.
 * @param {import('pg').PoolClient} client The active database client (for transactional integrity).
 */
async function _handleReferralChecks(refereeUserId, completedBetId, wagerAmountLamports, client) {
    const stringRefereeUserId = String(refereeUserId); // Ensure string
    const logPrefix = `[ReferralCheck Bet ${completedBetId} User ${stringRefereeUserId}]`;

    try {
        const refereeDetails = await getUserWalletDetails(stringRefereeUserId, client); // from Part 2
        if (!refereeDetails?.referred_by_user_id) { return; } // Not referred

        const referrerUserId = refereeDetails.referred_by_user_id;
        const referrerDetails = await getUserWalletDetails(referrerUserId, client); // from Part 2
        if (!referrerDetails?.external_withdrawal_address) {
            console.warn(`${logPrefix} Referrer ${referrerUserId} has no linked wallet. Cannot process referral payouts.`);
            return;
        }

        let payoutQueuedForReferrer = false;

        // 1. Check Initial Bet Bonus
        const isRefereeFirstCompletedBet = await isFirstCompletedBet(stringRefereeUserId, completedBetId, client); // from Part 2
        if (isRefereeFirstCompletedBet && wagerAmountLamports >= REFERRAL_INITIAL_BET_MIN_LAMPORTS) {
            const referrerWalletForCount = await queryDatabase('SELECT referral_count FROM wallets WHERE user_id = $1 FOR UPDATE', [referrerUserId], client); // from Part 2
            const currentReferrerCount = parseInt(referrerWalletForCount.rows[0]?.referral_count || '0', 10);

            let bonusTier = REFERRAL_INITIAL_BONUS_TIERS[REFERRAL_INITIAL_BONUS_TIERS.length - 1];
            for (const tier of REFERRAL_INITIAL_BONUS_TIERS) {
                if (currentReferrerCount < tier.maxCount) { bonusTier = tier; break; }
            }
            const initialBonusAmount = BigInt(Math.floor(Number(wagerAmountLamports) * bonusTier.percent));

            if (initialBonusAmount > 0n) {
                const payoutRecord = await recordPendingReferralPayout(referrerUserId, stringRefereeUserId, 'initial_bet', initialBonusAmount, completedBetId, null, client); // from Part 2
                if (payoutRecord.success && payoutRecord.payoutId) {
                    await addPayoutJob({ type: 'payout_referral', payoutId: payoutRecord.payoutId, userId: referrerUserId }); // from Part 6
                    payoutQueuedForReferrer = true;
                    console.log(`${logPrefix} Queued initial bet referral payout ID ${payoutRecord.payoutId} for referrer ${referrerUserId} (${formatSol(initialBonusAmount)} SOL).`); // formatSol from Part 3
                } else if (!payoutRecord.duplicate) {
                    console.error(`${logPrefix} Failed to record initial bonus payout for referrer ${referrerUserId}: ${payoutRecord.error}`);
                }
            }
        }

        // 2. Check Milestone Bonus
        const currentTotalWageredByReferee = refereeDetails.total_wagered;
        const lastMilestonePaidForReferee = refereeDetails.last_milestone_paid_lamports;
        let highestNewMilestoneCrossed = null;
        for (const threshold of REFERRAL_MILESTONE_THRESHOLDS_LAMPORTS) {
            if (currentTotalWageredByReferee >= threshold && threshold > lastMilestonePaidForReferee) {
                highestNewMilestoneCrossed = threshold;
            } else if (currentTotalWageredByReferee < threshold) { break; }
        }

        if (highestNewMilestoneCrossed) {
            const milestoneBonusAmount = BigInt(Math.floor(Number(highestNewMilestoneCrossed) * REFERRAL_MILESTONE_REWARD_PERCENT));
            if (milestoneBonusAmount > 0n) {
                const payoutRecord = await recordPendingReferralPayout( // from Part 2
                    referrerUserId, stringRefereeUserId, 'milestone',
                    milestoneBonusAmount, null, highestNewMilestoneCrossed, client
                );
                if (payoutRecord.success && payoutRecord.payoutId) {
                    await addPayoutJob({ type: 'payout_referral', payoutId: payoutRecord.payoutId, userId: referrerUserId }); // from Part 6
                    payoutQueuedForReferrer = true;
                    console.log(`${logPrefix} Queued milestone referral payout ID ${payoutRecord.payoutId} for referrer ${referrerUserId} (${formatSol(milestoneBonusAmount)} SOL for ${formatSol(highestNewMilestoneCrossed)} milestone).`); // formatSol from Part 3
                    await queryDatabase( // from Part 2
                        'UPDATE wallets SET last_milestone_paid_lamports = $1 WHERE user_id = $2 AND $1 > last_milestone_paid_lamports',
                        [highestNewMilestoneCrossed, stringRefereeUserId], client
                    );
                } else if (payoutRecord.duplicate) { /* Optional logging */ }
                 else { console.error(`${logPrefix} Failed to record milestone bonus payout for referrer ${referrerUserId}: ${payoutRecord.error}`); }
            }
        }
        // Optional: Notify referrer if payoutQueuedForReferrer is true
    } catch (error) {
        console.error(`${logPrefix} Error during referral checks (within transaction): ${error.message}`, error.stack);
        throw error; // Re-throw to ensure the main transaction is rolled back.
    }
}

/** Generic function to show bet amount buttons for a game */
// Moved from Part 5b to Part 1
async function showBetAmountButtons(msgOrCbMsg, gameKey, existingBetAmountStr = null, correctUserIdFromCb = null) {
    const userId = String(correctUserIdFromCb || msgOrCbMsg.from.id);
    const chatId = String(msgOrCbMsg.chat.id);
    const gameConfig = GAME_CONFIG[gameKey]; // From Part 1 GAME_CONFIG population
    const logPrefix = `[ShowBetButtons User ${userId} Game ${gameKey}]`;
    let messageToEditId = msgOrCbMsg.message_id; // ID of the message to edit
    let isFromCallback = !!correctUserIdFromCb;

    if (!messageToEditId) {
         console.warn(`${logPrefix} Cannot show bet buttons - no valid message context (messageId missing).`);
         return;
    }
    if (!gameConfig) {
        // MarkdownV2 Safety: Escape game key
        const errorMsg = `⚠️ Internal error: Unknown game \`${escapeMarkdownV2(gameKey)}\`\\.`; // Escaped .
        const keyboard = {inline_keyboard: [[{text:"↩️ Back", callback_data:"menu:main"}]]};
        return bot.editMessageText(errorMsg, {chat_id: chatId, message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: keyboard});
    }

    const balance = await getUserBalance(userId); // from Part 2
    if (balance < gameConfig.minBetLamports) {
         // MarkdownV2 Safety: Escape balance, game name, amount, punctuation
        const lowBalMsg = `⚠️ Your balance \\(${escapeMarkdownV2(formatSol(balance))} SOL\\) is too low to play ${escapeMarkdownV2(gameConfig.name)} \\(min bet: ${escapeMarkdownV2(formatSol(gameConfig.minBetLamports))} SOL\\)\\. Use /deposit to add funds\\.`; // Escaped . ()
        const lowBalKeyboard = { inline_keyboard: [[{ text: '💰 Quick Deposit', callback_data: 'quick_deposit' }],[{ text: '↩️ Back to Games', callback_data: 'menu:game_selection' }]]}; // Go back to game selection
        return bot.editMessageText(lowBalMsg, { chat_id: chatId, message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: lowBalKeyboard });
    }

    let lastBetForThisGame = null;
    if (existingBetAmountStr) { try { lastBetForThisGame = BigInt(existingBetAmountStr); } catch {} }
    if (!lastBetForThisGame) {
        const userLastBets = userLastBetAmounts.get(userId) || new Map(); // from Part 1 state
        lastBetForThisGame = userLastBets.get(gameKey) || null;
    }

    const buttons = [];
    const amountsRow1 = [], amountsRow2 = [];
    STANDARD_BET_AMOUNTS_LAMPORTS.forEach((lamports, index) => {
        if (lamports >= gameConfig.minBetLamports && lamports <= gameConfig.maxBetLamports) {
            const solAmountStr = formatSol(lamports); // from Part 3
            let buttonText = `${solAmountStr} SOL`;
            if (lastBetForThisGame && lamports === lastBetForThisGame) { buttonText = `👍 ${buttonText}`; } // Add emoji for last bet
            let callbackActionData;
            // *** FIX: Use specific actions for multi-step games ***
            if (gameKey === 'coinflip') callbackActionData = `coinflip_select_side:${lamports.toString()}`;
            else if (gameKey === 'race') callbackActionData = `race_select_horse:${lamports.toString()}`;
            else if (gameKey === 'roulette') callbackActionData = `roulette_select_bet_type:${lamports.toString()}`;
            else callbackActionData = `confirm_bet:${gameKey}:${lamports.toString()}`; // Direct confirm for others
            const button = { text: buttonText, callback_data: callbackActionData };
            if (amountsRow1.length < 3) amountsRow1.push(button); else amountsRow2.push(button);
        }
    });
    if (amountsRow1.length > 0) buttons.push(amountsRow1);
    if (amountsRow2.length > 0) buttons.push(amountsRow2);
    // Add emojis to buttons
    buttons.push([{ text: "✏️ Custom Amount", callback_data: `custom_amount_select:${gameKey}` }, { text: "❌ Cancel", callback_data: 'menu:game_selection' }]); // Cancel goes back to game selection

    // MarkdownV2 Safety: Escape breadcrumb, game name
    const breadcrumb = escapeMarkdownV2(gameConfig.name);
    let prompt = `${breadcrumb}\nSelect bet amount for *${breadcrumb}*:`; // Escaped

    if (gameKey === 'slots') {
        const jackpotAmount = await getJackpotAmount('slots'); // from Part 2
        // MarkdownV2 Safety: Escape jackpot amount
        prompt += `\n\n💎 Current Slots Jackpot: *${escapeMarkdownV2(formatSol(jackpotAmount))} SOL*`; // formatSol from Part 3
    }
    // MarkdownV2 Safety: Escape numbers, punctuation, symbols like :, (), use italics, fix parens escape
    // *** FIX: Ensure parens are escaped for MdV2 *** (VERIFIED - already correctly escaped)
    if (gameKey === 'roulette') prompt += "\n\\_\\(Common payouts: Straight Up 35x, Even Money Bets 1x winnings\\)\\_"; // Escaped ()
    else if (gameKey === 'blackjack') prompt += "\n\\_\\(Blackjack pays 3:2, Dealer typically stands on 17\\)\\_"; // Escaped ()


    // Edit the message that triggered this
    bot.editMessageText(prompt, { // bot from Part 1
        chat_id: chatId, message_id: messageToEditId,
        reply_markup: { inline_keyboard: buttons }, parse_mode: 'MarkdownV2'
    }).catch(e => {
        if (!e.message.includes("message is not modified")) {
            console.warn(`${logPrefix} Failed to edit message ${messageToEditId} for bet amounts, sending new: ${e.message}`);
            safeSendMessage(chatId, prompt, { reply_markup: { inline_keyboard: buttons }, parse_mode: 'MarkdownV2' }); // safeSendMessage from Part 3
        }
    });
}

// --- End of Moved Functions ---


// --- Version and Startup Logging ---
const BOT_VERSION = process.env.npm_package_version || '3.2.1p'; // Updated version
console.log(`--- INDEX.JS - DEPLOYMENT CHECK --- ${new Date().toISOString()} --- v${BOT_VERSION} ---`);
console.log(`⏳ Starting Solana Gambles Bot (Custodial, Buttons, v${BOT_VERSION})... Checking environment variables...`);


// --- Environment Variable Validation ---
// ... (Validation code remains the same as the previous correct version) ...
const REQUIRED_ENV_VARS = [ 'BOT_TOKEN', 'DATABASE_URL', 'DEPOSIT_MASTER_SEED_PHRASE', 'MAIN_BOT_PRIVATE_KEY', 'REFERRAL_PAYOUT_PRIVATE_KEY', 'RPC_URLS', 'ADMIN_USER_IDS'];
if (process.env.RAILWAY_ENVIRONMENT === 'production' || process.env.RAILWAY_ENVIRONMENT === 'staging') { REQUIRED_ENV_VARS.push('RAILWAY_PUBLIC_DOMAIN'); }
let missingVars = false;
REQUIRED_ENV_VARS.forEach((key) => { if (key === 'REFERRAL_PAYOUT_PRIVATE_KEY') return; if (key === 'RAILWAY_PUBLIC_DOMAIN' && !(process.env.RAILWAY_ENVIRONMENT === 'production' || process.env.RAILWAY_ENVIRONMENT === 'staging')) return; if (!process.env[key]) { console.error(`❌ Environment variable ${key} is missing.`); missingVars = true; } });
if (!process.env.ADMIN_USER_IDS) { if (!missingVars && REQUIRED_ENV_VARS.includes('ADMIN_USER_IDS')) { console.error(`❌ Environment variable ADMIN_USER_IDS is missing.`); missingVars = true; } } else if (process.env.ADMIN_USER_IDS.split(',').map(id => id.trim()).filter(id => /^\d+$/.test(id)).length === 0) { console.error(`❌ ADMIN_USER_IDS must contain at least one valid numeric ID.`); missingVars = true; }
console.log(`[Env Check] Checking DEPOSIT_MASTER_SEED_PHRASE... Present: ${!!process.env.DEPOSIT_MASTER_SEED_PHRASE}`); if (process.env.DEPOSIT_MASTER_SEED_PHRASE && !bip39.validateMnemonic(process.env.DEPOSIT_MASTER_SEED_PHRASE)) { console.error(`❌ DEPOSIT_MASTER_SEED_PHRASE is set but invalid.`); missingVars = true; } else if (!process.env.DEPOSIT_MASTER_SEED_PHRASE && REQUIRED_ENV_VARS.includes('DEPOSIT_MASTER_SEED_PHRASE')) { if (!missingVars) { console.error(`❌ Environment variable DEPOSIT_MASTER_SEED_PHRASE is missing.`); missingVars = true; } }
const parsedRpcUrls = (process.env.RPC_URLS || '').split(',').map(u => u.trim()).filter(u => u && (u.startsWith('http://') || u.startsWith('https://'))); if (parsedRpcUrls.length === 0 && REQUIRED_ENV_VARS.includes('RPC_URLS')) { console.error(`❌ RPC_URLS missing or contains no valid URLs.`); missingVars = true; } else if (parsedRpcUrls.length === 1) { console.warn(`⚠️ RPC_URLS only contains one URL. Redundancy recommended.`); }
const validateBase58Key = (keyName, isRequired) => { const key = process.env[keyName]; if (key) { try { const decoded = bs58.decode(key); if (decoded.length !== 64) { console.error(`❌ Env var ${keyName} incorrect length (Expected 64): ${decoded.length}.`); return false; } console.log(`[Env Check] Key ${keyName} OK.`); } catch (e) { console.error(`❌ Failed to decode ${keyName}: ${e.message}`); return false; } } else if (isRequired) { console.error(`❌ Required env var ${keyName} missing.`); return false; } return true; };
if (!validateBase58Key('MAIN_BOT_PRIVATE_KEY', true)) missingVars = true; if (process.env.REFERRAL_PAYOUT_PRIVATE_KEY && !validateBase58Key('REFERRAL_PAYOUT_PRIVATE_KEY', false)) { missingVars = true; }
if (missingVars) { console.error("⚠️ Required environment variables missing/invalid. Exiting."); process.exit(1); }


// --- Optional Environment Variables & Defaults ---
const OPTIONAL_ENV_DEFAULTS = { /* ... all the defaults as listed before ... */
    'DEPOSIT_ADDRESS_EXPIRY_MINUTES': '60', 'DEPOSIT_CONFIRMATIONS': 'confirmed', 'WITHDRAWAL_FEE_LAMPORTS': '5000',
    'MIN_WITHDRAWAL_LAMPORTS': '10000000', 'PAYOUT_BASE_PRIORITY_FEE_MICROLAMPORTS': '1000', 'PAYOUT_MAX_PRIORITY_FEE_MICROLAMPORTS': '1000000',
    'PAYOUT_COMPUTE_UNIT_LIMIT': '200000', 'PAYOUT_JOB_RETRIES': '3', 'PAYOUT_JOB_RETRY_DELAY_MS': '5000',
    'CF_MIN_BET_LAMPORTS': '10000000', 'CF_MAX_BET_LAMPORTS': '1000000000', 'RACE_MIN_BET_LAMPORTS': '10000000', 'RACE_MAX_BET_LAMPORTS': '1000000000',
    'SLOTS_MIN_BET_LAMPORTS': '10000000', 'SLOTS_MAX_BET_LAMPORTS': '500000000', 'ROULETTE_MIN_BET_LAMPORTS': '10000000', 'ROULETTE_MAX_BET_LAMPORTS': '1000000000',
    'WAR_MIN_BET_LAMPORTS': '10000000', 'WAR_MAX_BET_LAMPORTS': '1000000000', 'CRASH_MIN_BET_LAMPORTS': '10000000', 'CRASH_MAX_BET_LAMPORTS': '1000000000',
    'BLACKJACK_MIN_BET_LAMPORTS': '10000000', 'BLACKJACK_MAX_BET_LAMPORTS': '1000000000', 'CF_HOUSE_EDGE': '0.03', 'RACE_HOUSE_EDGE': '0.05',
    'SLOTS_HOUSE_EDGE': '0.10', 'ROULETTE_HOUSE_EDGE': '0.05', 'WAR_HOUSE_EDGE': '0.02', 'CRASH_HOUSE_EDGE': '0.03', 'BLACKJACK_HOUSE_EDGE': '0.015',
    'SLOTS_JACKPOT_SEED_LAMPORTS': '100000000', 'SLOTS_JACKPOT_CONTRIBUTION_PERCENT': '0.001', 'REFERRAL_INITIAL_BET_MIN_LAMPORTS': '10000000',
    'REFERRAL_MILESTONE_REWARD_PERCENT': '0.005', 'SWEEP_INTERVAL_MS': '900000', 'SWEEP_BATCH_SIZE': '20', 'SWEEP_FEE_BUFFER_LAMPORTS': '15000',
    'SWEEP_ADDRESS_DELAY_MS': '750', 'SWEEP_RETRY_ATTEMPTS': '1', 'SWEEP_RETRY_DELAY_MS': '3000', 'RPC_MAX_CONCURRENT': '8', 'RPC_RETRY_BASE_DELAY': '600',
    'RPC_MAX_RETRIES': '3', 'RPC_RATE_LIMIT_COOLOFF': '1500', 'RPC_RETRY_MAX_DELAY': '15000', 'RPC_RETRY_JITTER': '0.2', 'RPC_COMMITMENT': 'confirmed',
    'MSG_QUEUE_CONCURRENCY': '5', 'MSG_QUEUE_TIMEOUT_MS': '10000', 'CALLBACK_QUEUE_CONCURRENCY': '8', 'CALLBACK_QUEUE_TIMEOUT_MS': '30000', // Increased from 15000 based on Crash freeze diagnosis
    'PAYOUT_QUEUE_CONCURRENCY': '3', 'PAYOUT_QUEUE_TIMEOUT_MS': '60000', 'DEPOSIT_PROCESS_QUEUE_CONCURRENCY': '4', 'DEPOSIT_PROCESS_QUEUE_TIMEOUT_MS': '30000',
    'TELEGRAM_SEND_QUEUE_CONCURRENCY': '1', 'TELEGRAM_SEND_QUEUE_INTERVAL_MS': '1050', 'TELEGRAM_SEND_QUEUE_INTERVAL_CAP': '1',
    'DEPOSIT_MONITOR_INTERVAL_MS': '20000', 'DEPOSIT_MONITOR_ADDRESS_BATCH_SIZE': '50', 'DEPOSIT_MONITOR_SIGNATURE_FETCH_LIMIT': '5',
    'DB_POOL_MAX': '25', 'DB_POOL_MIN': '5', 'DB_IDLE_TIMEOUT': '30000', 'DB_CONN_TIMEOUT': '5000', 'DB_SSL': 'true', 'DB_REJECT_UNAUTHORIZED': 'true',
    'USER_STATE_CACHE_TTL_MS': (5 * 60 * 1000).toString(), 'WALLET_CACHE_TTL_MS': (10 * 60 * 1000).toString(), 'DEPOSIT_ADDR_CACHE_TTL_MS': (61 * 60 * 1000).toString(),
    'MAX_PROCESSED_TX_CACHE': '5000', 'USER_COMMAND_COOLDOWN_MS': '1000', 'INIT_DELAY_MS': '3000', 'SHUTDOWN_QUEUE_TIMEOUT_MS': '20000',
    'SHUTDOWN_FAIL_TIMEOUT_MS': '8000', 'WEBHOOK_MAX_CONN': '40', 'LEADERBOARD_UPDATE_INTERVAL_MS': '3600000'
};
Object.entries(OPTIONAL_ENV_DEFAULTS).forEach(([key, defaultValue]) => { if (process.env[key] === undefined) { process.env[key] = defaultValue; } });
console.log("✅ Environment variables checked/defaults applied.");

// --- Global Constants & State (Derived from Env Vars) ---
// ... (All constant definitions remain the same as the previous correct version) ...
const SOL_DECIMALS = 9; const DEPOSIT_ADDRESS_EXPIRY_MS = parseInt(process.env.DEPOSIT_ADDRESS_EXPIRY_MINUTES, 10) * 60 * 1000;
const validConfirmLevels = ['processed', 'confirmed', 'finalized']; const envConfirmLevel = process.env.DEPOSIT_CONFIRMATIONS?.toLowerCase();
const DEPOSIT_CONFIRMATION_LEVEL = validConfirmLevels.includes(envConfirmLevel) ? envConfirmLevel : 'confirmed';
const WITHDRAWAL_FEE_LAMPORTS = BigInt(process.env.WITHDRAWAL_FEE_LAMPORTS); const MIN_WITHDRAWAL_LAMPORTS = BigInt(process.env.MIN_WITHDRAWAL_LAMPORTS);
const MAX_PROCESSED_TX_CACHE_SIZE = parseInt(process.env.MAX_PROCESSED_TX_CACHE, 10); const USER_COMMAND_COOLDOWN_MS = parseInt(process.env.USER_COMMAND_COOLDOWN_MS, 10);
const USER_STATE_TTL_MS = parseInt(process.env.USER_STATE_CACHE_TTL_MS, 10); const WALLET_CACHE_TTL_MS = parseInt(process.env.WALLET_CACHE_TTL_MS, 10);
const DEPOSIT_ADDR_CACHE_TTL_MS = parseInt(process.env.DEPOSIT_ADDR_CACHE_TTL_MS, 10); const REFERRAL_INITIAL_BET_MIN_LAMPORTS = BigInt(process.env.REFERRAL_INITIAL_BET_MIN_LAMPORTS);
const REFERRAL_MILESTONE_REWARD_PERCENT = parseFloat(process.env.REFERRAL_MILESTONE_REWARD_PERCENT);
const REFERRAL_INITIAL_BONUS_TIERS = [ { maxCount: 10, percent: 0.05 }, { maxCount: 25, percent: 0.10 }, { maxCount: 50, percent: 0.15 }, { maxCount: 100, percent: 0.20 }, { maxCount: Infinity, percent: 0.25 } ];
const REFERRAL_MILESTONE_THRESHOLDS_LAMPORTS = [ BigInt(1 * LAMPORTS_PER_SOL), BigInt(5 * LAMPORTS_PER_SOL), BigInt(10 * LAMPORTS_PER_SOL), BigInt(25 * LAMPORTS_PER_SOL), BigInt(50 * LAMPORTS_PER_SOL), BigInt(100 * LAMPORTS_PER_SOL), BigInt(250 * LAMPORTS_PER_SOL), BigInt(500 * LAMPORTS_PER_SOL), BigInt(1000 * LAMPORTS_PER_SOL) ];
const SWEEP_FEE_BUFFER_LAMPORTS = BigInt(process.env.SWEEP_FEE_BUFFER_LAMPORTS); const SWEEP_BATCH_SIZE = parseInt(process.env.SWEEP_BATCH_SIZE, 10);
const SWEEP_ADDRESS_DELAY_MS = parseInt(process.env.SWEEP_ADDRESS_DELAY_MS, 10); const SWEEP_RETRY_ATTEMPTS = parseInt(process.env.SWEEP_RETRY_ATTEMPTS, 10); const SWEEP_RETRY_DELAY_MS = parseInt(process.env.SWEEP_RETRY_DELAY_MS, 10);
const SLOTS_JACKPOT_SEED_LAMPORTS = BigInt(process.env.SLOTS_JACKPOT_SEED_LAMPORTS); const SLOTS_JACKPOT_CONTRIBUTION_PERCENT = parseFloat(process.env.SLOTS_JACKPOT_CONTRIBUTION_PERCENT);
let currentSlotsJackpotLamports = SLOTS_JACKPOT_SEED_LAMPORTS;
const STANDARD_BET_AMOUNTS_SOL = [0.01, 0.05, 0.10, 0.25, 0.5, 1.0]; const STANDARD_BET_AMOUNTS_LAMPORTS = STANDARD_BET_AMOUNTS_SOL.map(sol => BigInt(Math.round(sol * LAMPORTS_PER_SOL)));
console.log(`[Config] Standard Bet Amounts (Lamports): ${STANDARD_BET_AMOUNTS_LAMPORTS.join(', ')}`);
const RACE_HORSES = [ { name: "Solar Sprint", emoji: "🐎₁", payoutMultiplier: 3.0 }, { name: "Comet Tail", emoji: "🏇₂", payoutMultiplier: 4.0 }, { name: "Galaxy Gallop", emoji: "🐴₃", payoutMultiplier: 5.0 }, { name: "Nebula Speed", emoji: "🎠₄", payoutMultiplier: 6.0 } ];

// In-memory Caches & State
const userStateCache = new Map(); const walletCache = new Map(); const activeDepositAddresses = new Map();
const processedDepositTxSignatures = new Set(); const commandCooldown = new Map(); const pendingReferrals = new Map();
const PENDING_REFERRAL_TTL_MS = 24 * 60 * 60 * 1000; const userLastBetAmounts = new Map();

// Global State Variables
let isFullyInitialized = false; let server; let depositMonitorIntervalId = null;
let sweepIntervalId = null; let leaderboardManagerIntervalId = null;

// --- Core Initializations ---
const app = express();
app.use(express.json({ limit: '10kb' }));

// Initialize Solana Connection
console.log("⚙️ Initializing Multi-RPC Solana connection...");
console.log(`ℹ️ Using RPC Endpoints: ${parsedRpcUrls.join(', ')}`);
const solanaConnection = new RateLimitedConnection(parsedRpcUrls, { /* ... options ... */
    maxConcurrent: parseInt(process.env.RPC_MAX_CONCURRENT, 10), retryBaseDelay: parseInt(process.env.RPC_RETRY_BASE_DELAY, 10),
    maxRetries: parseInt(process.env.RPC_MAX_RETRIES, 10), rateLimitCooloff: parseInt(process.env.RPC_RATE_LIMIT_COOLOFF, 10),
    retryMaxDelay: parseInt(process.env.RPC_RETRY_MAX_DELAY, 10), retryJitter: parseFloat(process.env.RPC_RETRY_JITTER),
    commitment: process.env.RPC_COMMITMENT, httpHeaders: { 'User-Agent': `SolanaGamblesBot/${BOT_VERSION}` }, clientId: `SolanaGamblesBot/${BOT_VERSION}`
});
console.log("✅ Multi-RPC Solana connection instance created.");


// Initialize Processing Queues
console.log("⚙️ Initializing Processing Queues...");
const messageQueue = new PQueue({ /* ... */ concurrency: parseInt(process.env.MSG_QUEUE_CONCURRENCY, 10), timeout: parseInt(process.env.MSG_QUEUE_TIMEOUT_MS, 10), throwOnTimeout: true });
// *** FIX: Increased Callback Queue Timeout for Crash Freeze Issue ***
const callbackQueueTimeoutMs = parseInt(process.env.CALLBACK_QUEUE_TIMEOUT_MS, 10);
console.log(`ℹ️ Using CALLBACK_QUEUE_TIMEOUT_MS: ${callbackQueueTimeoutMs}ms`);
const callbackQueue = new PQueue({ /* ... */ concurrency: parseInt(process.env.CALLBACK_QUEUE_CONCURRENCY, 10), timeout: callbackQueueTimeoutMs, throwOnTimeout: true });
const payoutProcessorQueue = new PQueue({ /* ... */ concurrency: parseInt(process.env.PAYOUT_QUEUE_CONCURRENCY, 10), timeout: parseInt(process.env.PAYOUT_QUEUE_TIMEOUT_MS, 10), throwOnTimeout: true });
const depositProcessorQueue = new PQueue({ /* ... */ concurrency: parseInt(process.env.DEPOSIT_PROCESS_QUEUE_CONCURRENCY, 10), timeout: parseInt(process.env.DEPOSIT_PROCESS_QUEUE_TIMEOUT_MS, 10), throwOnTimeout: true });
const telegramSendQueue = new PQueue({ /* ... */ concurrency: parseInt(process.env.TELEGRAM_SEND_QUEUE_CONCURRENCY, 10), interval: parseInt(process.env.TELEGRAM_SEND_QUEUE_INTERVAL_MS, 10), intervalCap: parseInt(process.env.TELEGRAM_SEND_QUEUE_INTERVAL_CAP, 10) });
console.log("✅ Processing Queues initialized.");


// Initialize PostgreSQL Pool
console.log("⚙️ Setting up PostgreSQL Pool...");
const pool = new Pool({ /* ... options ... */
    connectionString: process.env.DATABASE_URL, max: parseInt(process.env.DB_POOL_MAX, 10), min: parseInt(process.env.DB_POOL_MIN, 10),
    idleTimeoutMillis: parseInt(process.env.DB_IDLE_TIMEOUT, 10), connectionTimeoutMillis: parseInt(process.env.DB_CONN_TIMEOUT, 10),
    ssl: process.env.DB_SSL === 'true' ? { rejectUnauthorized: process.env.DB_REJECT_UNAUTHORIZED === 'true' } : false,
});
pool.on('error', (err, client) => { /* ... error handling ... */
    console.error('❌ Unexpected error on idle PostgreSQL client', err);
    if (typeof notifyAdmin === "function") { notifyAdmin(`🚨 DATABASE POOL ERROR (Idle Client): ${escapeMarkdownV2(err.message || String(err))}`).catch(notifyErr => console.error("Failed to notify admin about DB pool error:", notifyErr)); }
    else { console.error(`[ADMIN ALERT during DB Pool Error (Idle Client)] ${err.message}`); }
});
console.log("✅ PostgreSQL Pool created.");

// Initialize Telegram Bot
console.log("⚙️ Initializing Telegram Bot...");
const bot = new TelegramBot(process.env.BOT_TOKEN, { /* ... options ... */
    polling: false, // Will be potentially overridden by setupTelegramConnection
    request: { timeout: 15000, agentOptions: { keepAlive: true, keepAliveMsecs: 60000, maxSockets: 100, maxFreeSockets: 10 } }
});
console.log("✅ Telegram Bot instance created.");


// Load Game Configuration
console.log("⚙️ Loading Game Configuration...");
const GAME_CONFIG = {};
// Robust BigInt Env Getter
const safeGetBigIntEnv = (key, defaultValue) => {
    const envValue = process.env[key];
    const valueToConvert = (envValue !== undefined && envValue !== null && envValue !== '') ? envValue : defaultValue;
    if (valueToConvert === undefined || valueToConvert === null) {
         console.error(`❌ safeGetBigIntEnv: Cannot convert value for key "${key}". Both process.env value and defaultValue are missing or invalid. DefaultValue received was: ${defaultValue}`);
         throw new TypeError(`Configuration error: Missing value for BigInt conversion for key "${key}"`);
    }
    try { return BigInt(valueToConvert); }
    catch (error) {
         console.error(`❌ safeGetBigIntEnv: Error converting value "${valueToConvert}" for key "${key}" to BigInt. Falling back to defaultValue if possible. Error: ${error.message}`);
         if (defaultValue === undefined || defaultValue === null) {
              console.error(`❌ safeGetBigIntEnv: Cannot fall back to defaultValue for key "${key}" as it's also missing or invalid.`);
              throw new TypeError(`Configuration error: Invalid fallback defaultValue for BigInt conversion for key "${key}"`);
         }
         try { return BigInt(defaultValue); }
         catch (fallbackError) {
              console.error(`❌ safeGetBigIntEnv: CRITICAL - Error converting even the defaultValue "${defaultValue}" for key "${key}" to BigInt. Error: ${fallbackError.message}`);
              throw new TypeError(`Configuration error: Cannot convert defaultValue for key "${key}"`);
         }
    }
};
// Float Env Getter
const safeGetFloatEnv = (key, defaultValue) => { try { const val = parseFloat(process.env[key]); return isNaN(val) ? defaultValue : val; } catch { return defaultValue; } };

// ** CORRECTED GAME_CONFIG LOOP **
// Helper to get the correct ENV prefix (Matches OPTIONAL_ENV_DEFAULTS keys)
const getEnvPrefix = (gameKey) => {
    if (gameKey === 'coinflip') return 'CF';
    if (gameKey === 'blackjack') return 'BLACKJACK'; // Ensure this prefix exists in OPTIONAL_ENV_DEFAULTS
    if (gameKey === 'war') return 'WAR';           // Ensure this prefix exists in OPTIONAL_ENV_DEFAULTS
    if (gameKey === 'race') return 'RACE';          // Ensure this prefix exists in OPTIONAL_ENV_DEFAULTS
    if (gameKey === 'slots') return 'SLOTS';        // Ensure this prefix exists in OPTIONAL_ENV_DEFAULTS
    if (gameKey === 'roulette') return 'ROULETTE';  // Ensure this prefix exists in OPTIONAL_ENV_DEFAULTS
    if (gameKey === 'crash') return 'CRASH';        // Ensure this prefix exists in OPTIONAL_ENV_DEFAULTS
    // Add any other specific short prefixes if needed
    return gameKey.toUpperCase(); // Fallback (should not be needed if all covered)
};
const games = ['coinflip', 'race', 'slots', 'roulette', 'war', 'crash', 'blackjack'];
games.forEach(key => {
    const envPrefix = getEnvPrefix(key);
    const defaultMinKey = `${envPrefix}_MIN_BET_LAMPORTS`;
    const defaultMaxKey = `${envPrefix}_MAX_BET_LAMPORTS`;
    const defaultEdgeKey = `${envPrefix}_HOUSE_EDGE`;

    // Check if default keys actually exist before using them, log warning if missing
    if (!(defaultMinKey in OPTIONAL_ENV_DEFAULTS)) console.warn(`[Config Load] Missing default key in OPTIONAL_ENV_DEFAULTS: ${defaultMinKey}`);
    if (!(defaultMaxKey in OPTIONAL_ENV_DEFAULTS)) console.warn(`[Config Load] Missing default key in OPTIONAL_ENV_DEFAULTS: ${defaultMaxKey}`);
    if (!(defaultEdgeKey in OPTIONAL_ENV_DEFAULTS)) console.warn(`[Config Load] Missing default key in OPTIONAL_ENV_DEFAULTS: ${defaultEdgeKey}`);

    GAME_CONFIG[key] = {
        key: key,
        name: key.charAt(0).toUpperCase() + key.slice(1), // Default name, override below
        minBetLamports: safeGetBigIntEnv(defaultMinKey, OPTIONAL_ENV_DEFAULTS[defaultMinKey]),
        maxBetLamports: safeGetBigIntEnv(defaultMaxKey, OPTIONAL_ENV_DEFAULTS[defaultMaxKey]),
        houseEdge: safeGetFloatEnv(defaultEdgeKey, parseFloat(OPTIONAL_ENV_DEFAULTS[defaultEdgeKey] || '0.1')) // Added fallback '0.1'
    };
    // Override default names (ensure these match game keys)
    // *** FIX: Ensure War name is set correctly *** (VERIFIED - already correct)
    if(key === 'slots') GAME_CONFIG[key].name = 'Slots';
    if(key === 'war') GAME_CONFIG[key].name = 'War'; // Use updated name 'War'
    if(key === 'blackjack') GAME_CONFIG[key].name = 'Blackjack';
    if(key === 'coinflip') GAME_CONFIG[key].name = 'Coinflip';
    if(key === 'race') GAME_CONFIG[key].name = 'Race';
    if(key === 'roulette') GAME_CONFIG[key].name = 'Roulette';
    if(key === 'crash') GAME_CONFIG[key].name = 'Crash';

    // Add slots specific config
    if(key === 'slots') {
        const contributionKey = 'SLOTS_JACKPOT_CONTRIBUTION_PERCENT';
        const seedKey = 'SLOTS_JACKPOT_SEED_LAMPORTS';
        if (!(contributionKey in OPTIONAL_ENV_DEFAULTS)) console.warn(`[Config Load] Missing default key in OPTIONAL_ENV_DEFAULTS: ${contributionKey}`);
        if (!(seedKey in OPTIONAL_ENV_DEFAULTS)) console.warn(`[Config Load] Missing default key in OPTIONAL_ENV_DEFAULTS: ${seedKey}`);
        GAME_CONFIG[key].jackpotContributionPercent = safeGetFloatEnv(contributionKey, parseFloat(OPTIONAL_ENV_DEFAULTS[contributionKey] || '0'));
        GAME_CONFIG[key].jackpotSeedLamports = safeGetBigIntEnv(seedKey, OPTIONAL_ENV_DEFAULTS[seedKey]);
    }
});
// Validate Game Config
function validateGameConfig(config) { /* ...validation logic as before... */ const errors = []; for (const gameKey in config) { const gc = config[gameKey]; if (!gc) { errors.push(`Config for game "${gameKey}" missing.`); continue; } if (gc.key !== gameKey) errors.push(`${gameKey}: key mismatch`); if (!gc.name) errors.push(`${gameKey}: name missing`); if (typeof gc.minBetLamports !== 'bigint' || gc.minBetLamports <= 0n) errors.push(`${gameKey}: minBetLamports invalid`); if (typeof gc.maxBetLamports !== 'bigint' || gc.maxBetLamports <= 0n) errors.push(`${gameKey}: maxBetLamports invalid`); if (gc.minBetLamports > gc.maxBetLamports) errors.push(`${gameKey}: minBet > maxBet`); if (isNaN(gc.houseEdge) || gc.houseEdge < 0 || gc.houseEdge >= 1) errors.push(`${gameKey}: houseEdge invalid (must be 0 <= edge < 1)`); if (gameKey === 'slots') { if (isNaN(gc.jackpotContributionPercent) || gc.jackpotContributionPercent < 0 || gc.jackpotContributionPercent >= 1) errors.push(`${gameKey}: jackpotContributionPercent invalid`); if (typeof gc.jackpotSeedLamports !== 'bigint' || gc.jackpotSeedLamports < 0n) errors.push(`${gameKey}: jackpotSeedLamports invalid`); } } return errors; }
const configErrorsAfterFix = validateGameConfig(GAME_CONFIG);
if (configErrorsAfterFix.length > 0) {
    console.error(`❌ Invalid game configuration values AFTER FIX ATTEMPT: ${configErrorsAfterFix.join(', ')}. Exiting.`);
    process.exit(1);
}
console.log("✅ Game Config Loaded and Validated (with corrected key prefixes).");

// Declare commandHandlers and menuCommandHandlers with `let`
let commandHandlers;
let menuCommandHandlers;

// --- Export Block Removed ---

// --- End of Part 1 ---
// index.js - Part 2: Database Operations
// --- VERSION: 3.2.1i --- (Applying Fix: Explicit type casting in updateBetStatus query. Includes previous verification.)

// --- Assuming 'pool', 'PublicKey', utils like 'generateReferralCode', 'updateWalletCache' etc. are available from other parts ---
// --- Also assuming relevant constants like BOT_VERSION, GAME_CONFIG, LAMPORTS_PER_SOL, SOL_DECIMALS are available ---
// --- 'addActiveDepositAddressCache', 'getActiveDepositAddressCache', 'removeActiveDepositAddressCache' are from Part 3 but used here.
// --- 'formatSol' utility (used in a log in ensureJackpotExists) is from Part 3.
// --- 'addPayoutJob' (used in _handleReferralChecks which relies on DB ops) is from Part 6.

// --- Helper Function for DB Operations ---
/**
 * Executes a database query using either a provided client or the pool.
 * Handles basic error logging including parameters (converting BigInts).
 * @param {string} sql The SQL query string with placeholders ($1, $2, ...).
 * @param {Array<any>} [params=[]] The parameters for the SQL query.
 * @param {import('pg').PoolClient | import('pg').Pool} [dbClient=pool] The DB client or pool to use. Defaults to the global 'pool'.
 * @returns {Promise<import('pg').QueryResult<any>>} The query result.
 * @throws {Error} Throws the original database error if the query fails.
 */
async function queryDatabase(sql, params = [], dbClient = pool) {
     // Ensure dbClient is valid, default to pool if necessary
     if (!dbClient) {
         if (!pool) { // pool is from Part 1
             const poolError = new Error("Database pool not available for queryDatabase");
             console.error("❌ CRITICAL: queryDatabase called but default pool is not initialized!", poolError.stack);
             throw poolError;
         }
         dbClient = pool; // Fallback to global pool
     }
     // Basic check for SQL validity before attempting query
     if (typeof sql !== 'string' || sql.trim().length === 0) {
         const sqlError = new TypeError(`queryDatabase received invalid SQL query (type: ${typeof sql}, value: ${sql})`);
         console.error(`❌ DB Query Error:`, sqlError.message);
         console.error(`   Params: ${JSON.stringify(params, (k, v) => typeof v === 'bigint' ? v.toString() + 'n' : v)}`);
         throw sqlError;
     }

    try {
        // Execute the query
        return await dbClient.query(sql, params);
    } catch (error) {
        // Log detailed error information
        console.error(`❌ DB Query Error:`);
        console.error(`   SQL: ${sql.substring(0, 500)}${sql.length > 500 ? '...' : ''}`);
        // Safely stringify parameters, converting BigInts to strings for logging
        const safeParamsString = JSON.stringify(params, (key, value) =>
            typeof value === 'bigint'
                ? value.toString() + 'n' // Indicate BigInt in log
                : value // Return other values unchanged
        );
        console.error(`   Params: ${safeParamsString}`);
        console.error(`   Error Code: ${error.code || 'N/A'}`);
        console.error(`   Error Message: ${error.message}`);
        if (error.constraint) { console.error(`   Constraint: ${error.constraint}`); }
        // Optional: Log full stack for deeper debugging if needed
        // console.error(`   Stack: ${error.stack}`);
        throw error; // Re-throw the original error
    }
}
// --- End Helper ---


// --- Wallet/User Operations ---

/**
 * Ensures a user exists in the 'wallets' table, creating a basic record if not.
 * Also ensures a corresponding 'user_balances' record exists.
 * Locks relevant rows using FOR UPDATE.
 * Should be called within a transaction if used alongside other user modifications.
 * @param {string} userId The user's Telegram ID.
 * @param {import('pg').PoolClient} client The active database client connection.
 * @returns {Promise<{userId: string, isNewUser: boolean}>}
 */
async function ensureUserExists(userId, client) {
    const stringUserId = String(userId); // Ensure string type
    let isNewUser = false;
    const logPrefix = `[DB ensureUserExists User ${stringUserId}]`;

    // 1. Check/Insert into wallets, locking the row (or potential row)
    const walletCheck = await queryDatabase('SELECT user_id FROM wallets WHERE user_id = $1 FOR UPDATE', [stringUserId], client);
    if (walletCheck.rowCount === 0) {
        await queryDatabase(
            `INSERT INTO wallets (user_id, created_at, last_used_at, last_bet_amounts) VALUES ($1, NOW(), NOW(), '{}'::jsonb)`,
            [stringUserId],
            client
        );
        isNewUser = true;
        console.log(`${logPrefix} Created base wallet record for new user.`);
    } else {
        // Update last_used_at for existing user
        await queryDatabase(
            `UPDATE wallets SET last_used_at = NOW() WHERE user_id = $1`,
            [stringUserId],
            client
        );
    }

    // 2. Check/Insert into user_balances, locking the row
    const balanceCheck = await queryDatabase('SELECT user_id FROM user_balances WHERE user_id = $1 FOR UPDATE', [stringUserId], client);
    if (balanceCheck.rowCount === 0) {
        await queryDatabase(
            `INSERT INTO user_balances (user_id, balance_lamports, updated_at) VALUES ($1, 0, NOW())`,
            [stringUserId],
            client
        );
        if (!isNewUser) { // If wallet existed but balance didn't
            console.warn(`${logPrefix} Created missing balance record for existing user.`);
        }
    }

    return { userId: stringUserId, isNewUser };
}


/**
 * Updates the external withdrawal address for a user. Creates user record if non-existent.
 * Generates referral code if missing.
 * Handles its own transaction.
 * @param {string} userId
 * @param {string} externalAddress Valid Solana address string.
 * @returns {Promise<{success: boolean, wallet?: string, referralCode?: string, error?: string}>}
 */
async function linkUserWallet(userId, externalAddress) {
    const stringUserId = String(userId);
    const logPrefix = `[LinkWallet User ${stringUserId}]`;
    let client = null;
    try {
        // Validate address format before hitting DB
        try {
            new PublicKey(externalAddress); // PublicKey from @solana/web3.js (Part 1)
        } catch (validationError) {
            console.error(`${logPrefix} Invalid address format provided: ${externalAddress}`);
            return { success: false, error: 'Invalid Solana wallet address format.' };
        }

        client = await pool.connect(); // pool from Part 1
        await client.query('BEGIN');

        await ensureUserExists(stringUserId, client); // Locks rows

        // Fetch details again to get referral code, ensuring row lock from ensureUserExists is respected.
        const detailsRes = await queryDatabase('SELECT referral_code FROM wallets WHERE user_id = $1 FOR UPDATE', [stringUserId], client);
        if (detailsRes.rowCount === 0) {
            throw new Error(`Wallet row for user ${stringUserId} not found after ensureUserExists call.`);
        }

        let currentReferralCode = detailsRes.rows[0]?.referral_code;
        let needsCodeUpdate = false;

        if (!currentReferralCode) {
            console.log(`${logPrefix} User missing referral code, generating one.`);
            currentReferralCode = generateReferralCode(); // generateReferralCode from Part 3
            needsCodeUpdate = true;
        }

        // Prepare update query based on whether referral code needs update
        const updateQuery = `
            UPDATE wallets
            SET external_withdrawal_address = $1,
                linked_at = COALESCE(linked_at, NOW()),
                last_used_at = NOW()
                ${needsCodeUpdate ? ', referral_code = $3' : ''}
            WHERE user_id = $2
        `;
        const updateParams = needsCodeUpdate ? [externalAddress, stringUserId, currentReferralCode] : [externalAddress, stringUserId];
        await queryDatabase(updateQuery, updateParams, client);

        await client.query('COMMIT');
        console.log(`${logPrefix} External withdrawal address set/updated to ${externalAddress}. Referral Code: ${currentReferralCode}`);

        // Update cache after successful commit
        updateWalletCache(stringUserId, { withdrawalAddress: externalAddress, referralCode: currentReferralCode }); // updateWalletCache from Part 3

        return { success: true, wallet: externalAddress, referralCode: currentReferralCode };

    } catch (err) {
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { console.error(`${logPrefix} Rollback failed:`, rbErr); } }

        if (err.code === '23505' && err.constraint === 'wallets_referral_code_key') {
            console.error(`${logPrefix} CRITICAL - Referral code generation conflict for user ${stringUserId}. Retrying might be needed.`);
            return { success: false, error: 'Referral code generation conflict. Please try again.' };
        }
        console.error(`${logPrefix} DB Error linking wallet:`, err.message, err.code);
        return { success: false, error: `Database error linking wallet (Code: ${err.code || 'N/A'})` };
    } finally {
        if (client) client.release();
    }
}

/**
 * Fetches comprehensive user details from the 'wallets' table.
 * @param {string} userId
 * @param {import('pg').PoolClient} [client=pool] Optional DB client for transactions.
 * @returns {Promise<object | null>} Object with wallet details or null if not found.
 */
async function getUserWalletDetails(userId, client = pool) {
    const stringUserId = String(userId);
    const query = `
        SELECT
            external_withdrawal_address, linked_at, last_used_at, referral_code,
            referred_by_user_id, referral_count, total_wagered,
            last_milestone_paid_lamports, last_bet_amounts, created_at
        FROM wallets
        WHERE user_id = $1
    `;
    try {
        const res = await queryDatabase(query, [stringUserId], client);
        if (res.rows.length > 0) {
            const details = res.rows[0];
            // Ensure correct types
            details.total_wagered = BigInt(details.total_wagered || '0');
            details.last_milestone_paid_lamports = BigInt(details.last_milestone_paid_lamports || '0');
            details.referral_count = parseInt(details.referral_count || '0', 10);
            details.last_bet_amounts = details.last_bet_amounts || {}; // Ensure it's an object
            return details;
        }
        return null; // User not found
    } catch (err) {
        console.error(`[DB getUserWalletDetails] Error fetching details for user ${stringUserId}:`, err.message);
        return null;
    }
}

/**
 * Gets the linked external withdrawal address for a user, checking cache first.
 * @param {string} userId
 * @returns {Promise<string | undefined>} The address string or undefined if not set/found or error.
 */
async function getLinkedWallet(userId) {
    const stringUserId = String(userId);
    const cached = getWalletCache(stringUserId); // getWalletCache from Part 3
    if (cached?.withdrawalAddress !== undefined) {
        return cached.withdrawalAddress;
    }

    try {
        // Fetch only needed data for performance
        const res = await queryDatabase('SELECT external_withdrawal_address, referral_code FROM wallets WHERE user_id = $1', [stringUserId]);
        const details = res.rows[0];
        const withdrawalAddress = details?.external_withdrawal_address || undefined;
        const referralCode = details?.referral_code;

        // Update cache with potentially fresh data
        updateWalletCache(stringUserId, { withdrawalAddress: withdrawalAddress, referralCode: referralCode }); // updateWalletCache from Part 3
        return withdrawalAddress;
    } catch (err) {
        console.error(`[DB getLinkedWallet] Error fetching withdrawal wallet for user ${stringUserId}:`, err.message);
        return undefined; // Error occurred
    }
}

/**
 * Finds a user by their referral code.
 * @param {string} refCode The referral code (e.g., 'ref_abcdef12').
 * @returns {Promise<{user_id: string} | null>} User ID object or null if not found/invalid.
 */
async function getUserByReferralCode(refCode) {
    if (!refCode || typeof refCode !== 'string' || !refCode.startsWith('ref_') || refCode.length < 5) {
        return null; // Basic validation
    }
    try {
        const result = await queryDatabase('SELECT user_id FROM wallets WHERE referral_code = $1', [refCode]);
        return result.rows[0] || null;
    } catch (err) {
        console.error(`[DB getUserByReferralCode] Error finding user for code ${refCode}:`, err);
        return null;
    }
}

/**
 * Links a referee to a referrer if the referee doesn't already have a referrer.
 * Should be called within a transaction. Locks rows using FOR UPDATE.
 * @param {string} refereeUserId The ID of the user being referred.
 * @param {string} referrerUserId The ID of the user who referred them.
 * @param {import('pg').PoolClient} client The active database client.
 * @returns {Promise<boolean>} True if the link was successfully made or already existed correctly, false on error or if already referred by someone else.
 */
async function linkReferral(refereeUserId, referrerUserId, client) {
    const strRefereeUserId = String(refereeUserId);
    const strReferrerUserId = String(referrerUserId);
    const logPrefix = `[DB linkReferral Ref:${strReferrerUserId} -> New:${strRefereeUserId}]`;

    if (strRefereeUserId === strReferrerUserId) {
        console.warn(`${logPrefix} User attempted to refer themselves.`);
        return false;
    }

    try {
        // Check current status of referee, locking the row
        const checkRes = await queryDatabase('SELECT referred_by_user_id FROM wallets WHERE user_id = $1 FOR UPDATE', [strRefereeUserId], client);
        if (checkRes.rowCount === 0) {
            console.error(`${logPrefix} Referee user ${strRefereeUserId} not found. Cannot link.`);
            return false; // Referee must exist
        }
        const currentReferrer = checkRes.rows[0].referred_by_user_id;

        if (currentReferrer === strReferrerUserId) {
            console.log(`${logPrefix} Already linked to this referrer.`);
            return true; // Idempotent: already linked correctly
        } else if (currentReferrer) {
            console.warn(`${logPrefix} Already referred by ${currentReferrer}. Cannot link to ${strReferrerUserId}.`);
            return false; // Cannot change referrer
        }

        // Link the referral (only if not currently referred)
        const updateRes = await queryDatabase(
            'UPDATE wallets SET referred_by_user_id = $1 WHERE user_id = $2 AND referred_by_user_id IS NULL',
            [strReferrerUserId, strRefereeUserId],
            client
        );

        if (updateRes.rowCount > 0) {
            console.log(`${logPrefix} Successfully linked referee to referrer.`);
            // Increment the referrer's count (Lock referrer row first)
            await queryDatabase('SELECT user_id FROM wallets WHERE user_id = $1 FOR UPDATE', [strReferrerUserId], client); // Lock referrer
            await queryDatabase('UPDATE wallets SET referral_count = referral_count + 1 WHERE user_id = $1', [strReferrerUserId], client);
            console.log(`${logPrefix} Incremented referral count for referrer ${strReferrerUserId}.`);
            return true;
        } else {
            // This case means the referee was linked by someone else between the SELECT FOR UPDATE and this UPDATE.
            console.warn(`${logPrefix} Failed to link referral - likely already referred just before update (race condition handled).`);
            return false;
        }
    } catch (err) {
        console.error(`${logPrefix} Error linking referral:`, err);
        return false;
    }
}


/**
 * Updates a user's total wagered amount and potentially the last milestone paid threshold.
 * Should be called within a transaction after a bet is completed. Locks the wallet row.
 * @param {string} userId The ID of the user who wagered.
 * @param {bigint} wageredAmountLamports The amount wagered in this bet.
 * @param {import('pg').PoolClient} client The active database client.
 * @param {bigint | null} [newLastMilestonePaidLamports=null] If a milestone was just paid for this user's referrer, update the threshold marker.
 * @returns {Promise<boolean>} True on success, false on failure.
 */
async function updateUserWagerStats(userId, wageredAmountLamports, client, newLastMilestonePaidLamports = null) {
    const stringUserId = String(userId);
    const wageredAmount = BigInt(wageredAmountLamports);
    const logPrefix = `[DB updateUserWagerStats User ${stringUserId}]`;

    if (wageredAmount <= 0n) {
        console.log(`${logPrefix} Wager amount is ${wageredAmount}, no update to wager stats needed.`);
        return true;
    }

    try {
        // Lock the user's wallet row before updating
        await queryDatabase('SELECT user_id FROM wallets WHERE user_id = $1 FOR UPDATE', [stringUserId], client);

        let query;
        let values;
        const lastMilestonePaid = (newLastMilestonePaidLamports !== null && BigInt(newLastMilestonePaidLamports) >= 0n)
                                     ? BigInt(newLastMilestonePaidLamports) : null;

        if (lastMilestonePaid !== null) {
            query = `UPDATE wallets SET total_wagered = total_wagered + $2, last_milestone_paid_lamports = $3, last_used_at = NOW() WHERE user_id = $1;`;
            // Ensure BigInts are passed correctly
            values = [stringUserId, wageredAmount, lastMilestonePaid];
        } else {
            query = `UPDATE wallets SET total_wagered = total_wagered + $2, last_used_at = NOW() WHERE user_id = $1;`;
            values = [stringUserId, wageredAmount];
        }

        const res = await queryDatabase(query, values, client);
        if (res.rowCount === 0) {
            console.warn(`${logPrefix} Attempted to update wager stats for non-existent user ${stringUserId} (or lock failed?).`);
            return false;
        }
        return true;
    } catch (err) {
        console.error(`${logPrefix} Error updating wager stats:`, err.message);
        return false;
    }
}

// --- User Last Bet Preferences ---

/**
 * Updates the last bet amount for a specific game for a user. Stores it in JSONB.
 * Should be called within a transaction where user's wallet row might be locked.
 * @param {string} userId
 * @param {string} gameKey
 * @param {bigint} amountLamports
 * @param {import('pg').PoolClient} client The active database client.
 * @returns {Promise<boolean>}
 */
async function updateUserLastBetAmount(userId, gameKey, amountLamports, client) {
    const stringUserId = String(userId);
    const logPrefix = `[DB updateUserLastBet User ${stringUserId} Game ${gameKey}]`;
    try {
        // Fetch current last_bet_amounts, ensuring row lock is respected
        const currentRes = await queryDatabase(
            'SELECT last_bet_amounts FROM wallets WHERE user_id = $1 FOR UPDATE', // Added FOR UPDATE here earlier
            [stringUserId], client
        );
        if (currentRes.rowCount === 0) {
            console.warn(`${logPrefix} User not found, cannot update last bet amount.`);
            return false;
        }
        const currentLastBets = currentRes.rows[0].last_bet_amounts || {};
        currentLastBets[gameKey] = amountLamports.toString(); // Store as string in JSON

        const updateRes = await queryDatabase(
            'UPDATE wallets SET last_bet_amounts = $1, last_used_at = NOW() WHERE user_id = $2',
            [currentLastBets, stringUserId], client
        );
        return updateRes.rowCount > 0;
    } catch (err) {
        console.error(`${logPrefix} Error updating last bet amount:`, err.message);
        return false;
    }
}

/**
 * Retrieves all stored last bet amounts for a user.
 * @param {string} userId
 * @param {import('pg').PoolClient} [client=pool] Optional DB client.
 * @returns {Promise<{[gameKey: string]: bigint} | null>} Map of gameKey to BigInt amount, null on error, {} if none.
 */
async function getUserLastBetAmounts(userId, client = pool) {
    const stringUserId = String(userId);
    const logPrefix = `[DB getUserLastBets User ${stringUserId}]`;
    try {
        const res = await queryDatabase('SELECT last_bet_amounts FROM wallets WHERE user_id = $1', [stringUserId], client);
        if (res.rowCount > 0 && res.rows[0].last_bet_amounts) {
            const lastBetsRaw = res.rows[0].last_bet_amounts;
            const lastBets = {};
            for (const gameKey in lastBetsRaw) {
                if (Object.prototype.hasOwnProperty.call(lastBetsRaw, gameKey)) {
                    try { lastBets[gameKey] = BigInt(lastBetsRaw[gameKey]); }
                    catch (e) { console.warn(`${logPrefix} Failed to parse BigInt for ${gameKey} ('${lastBetsRaw[gameKey]}'). Skipping.`); }
                }
            }
            return lastBets;
        }
        return {}; // Return empty object if none found
    } catch (err) {
        console.error(`${logPrefix} Error fetching last bet amounts:`, err.message);
        return null; // Indicate error
    }
}

// --- Balance Operations ---

/**
 * Gets the current internal balance for a user. Creates user/balance record if needed.
 * Handles its own transaction.
 * @param {string} userId
 * @returns {Promise<bigint>} Current balance in lamports, or 0n if user/balance record creation fails.
 */
async function getUserBalance(userId) {
    const stringUserId = String(userId);
    let client = null;
    try {
        client = await pool.connect();
        await client.query('BEGIN');
        await ensureUserExists(stringUserId, client); // Ensure user and balance rows exist and are locked
        const balanceRes = await queryDatabase('SELECT balance_lamports FROM user_balances WHERE user_id = $1', [stringUserId], client);
        await client.query('COMMIT'); // Commit existence checks
        return BigInt(balanceRes.rows[0]?.balance_lamports || '0');
    } catch (err) {
        console.error(`[DB GetBalance] Error fetching/ensuring balance for user ${stringUserId}:`, err.message);
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) {} }
        return 0n; // Return 0 on error
    } finally {
        if (client) client.release();
    }
}

/**
 * Atomically updates a user's balance and records the change in the ledger.
 * MUST be called within a DB transaction. Locks the user_balances row.
 * @param {import('pg').PoolClient} client - The active database client connection.
 * @param {string} userId
 * @param {bigint} changeAmountLamports - Positive for credit, negative for debit.
 * @param {string} transactionType - Type string for the ledger (e.g., 'deposit', 'bet_placed').
 * @param {object} [relatedIds={}] Optional related IDs { betId, depositId, withdrawalId, refPayoutId }.
 * @param {string|null} [notes=null] Optional notes for the ledger entry.
 * @returns {Promise<{success: boolean, newBalance?: bigint, error?: string}>}
 */
async function updateUserBalanceAndLedger(client, userId, changeAmountLamports, transactionType, relatedIds = {}, notes = null) {
    const stringUserId = String(userId);
    const changeAmount = BigInt(changeAmountLamports);
    const logPrefix = `[UpdateBalance User ${stringUserId} Type ${transactionType}]`;

    // Validate and nullify related IDs if not proper integers > 0
    const relatedBetId = Number.isInteger(relatedIds?.betId) && relatedIds.betId > 0 ? relatedIds.betId : null;
    const relatedDepositId = Number.isInteger(relatedIds?.depositId) && relatedIds.depositId > 0 ? relatedIds.depositId : null;
    const relatedWithdrawalId = Number.isInteger(relatedIds?.withdrawalId) && relatedIds.withdrawalId > 0 ? relatedIds.withdrawalId : null;
    const relatedRefPayoutId = Number.isInteger(relatedIds?.refPayoutId) && relatedIds.refPayoutId > 0 ? relatedIds.refPayoutId : null;

    try {
        // 1. Get current balance and lock row
        const balanceRes = await queryDatabase('SELECT balance_lamports FROM user_balances WHERE user_id = $1 FOR UPDATE', [stringUserId], client);
        if (balanceRes.rowCount === 0) {
            console.error(`${logPrefix} User balance record not found during update. Ensure user exists first.`);
            return { success: false, error: 'User balance record unexpectedly missing.' };
        }
        const balanceBefore = BigInt(balanceRes.rows[0].balance_lamports);
        const balanceAfter = balanceBefore + changeAmount;

        // 2. Check for insufficient funds only if debiting
        if (changeAmount < 0n && balanceAfter < 0n) {
            const needed = -changeAmount;
            console.warn(`${logPrefix} Insufficient balance. Current: ${balanceBefore}, Needed: ${needed}, Would be: ${balanceAfter}`);
            return { success: false, error: 'Insufficient balance' };
        }

        // 3. Update balance
        const updateRes = await queryDatabase('UPDATE user_balances SET balance_lamports = $1, updated_at = NOW() WHERE user_id = $2', [balanceAfter, stringUserId], client);
        if (updateRes.rowCount === 0) {
            console.error(`${logPrefix} Failed to update balance row after lock. Unexpected.`);
            return { success: false, error: 'Failed to update balance row' };
        }

        // 4. Insert into ledger
        const ledgerQuery = `
            INSERT INTO ledger (
                user_id, transaction_type, amount_lamports, balance_before, balance_after,
                related_bet_id, related_deposit_id, related_withdrawal_id, related_ref_payout_id, notes, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
        `;
        await queryDatabase(ledgerQuery, [
            stringUserId, transactionType, changeAmount, balanceBefore, balanceAfter,
            relatedBetId, relatedDepositId, relatedWithdrawalId, relatedRefPayoutId, notes
        ], client);

        return { success: true, newBalance: balanceAfter };

    } catch (err) {
        console.error(`${logPrefix} Error updating balance/ledger:`, err.message, err.code);
        let errMsg = `Database error during balance update (Code: ${err.code || 'N/A'})`;
        if (err.constraint === 'user_balances_balance_lamports_check') { // Check constraint from schema
            errMsg = 'Insufficient balance (check constraint violation).';
        }
        return { success: false, error: errMsg };
    }
}


// --- Deposit Operations ---

/**
 * Creates a new unique deposit address record for a user. Manages its own transaction.
 * Adds address to cache upon successful creation.
 * @param {string} userId
 * @param {string} depositAddress - The generated unique address.
 * @param {string} derivationPath - The path used to derive the address.
 * @param {Date} expiresAt - Expiry timestamp for the address.
 * @returns {Promise<{success: boolean, id?: number, error?: string}>}
 */
async function createDepositAddressRecord(userId, depositAddress, derivationPath, expiresAt) {
    const stringUserId = String(userId);
    const logPrefix = `[DB CreateDepositAddr User ${stringUserId} Addr ${depositAddress.slice(0, 6)}..]`;
    console.log(`${logPrefix} Attempting to create record. Path: ${derivationPath}`);
    const query = `
        INSERT INTO deposit_addresses (user_id, deposit_address, derivation_path, expires_at, status, created_at)
        VALUES ($1, $2, $3, $4, 'pending', NOW())
        RETURNING id;
    `;
    try {
        const res = await queryDatabase(query, [stringUserId, depositAddress, derivationPath, expiresAt], pool); // Use default pool for this self-contained operation
        if (res.rowCount > 0 && res.rows[0].id) {
            const depositAddressId = res.rows[0].id;
            console.log(`${logPrefix} Successfully created record ID: ${depositAddressId}. Adding to cache.`);
            addActiveDepositAddressCache(depositAddress, stringUserId, expiresAt.getTime()); // addActiveDepositAddressCache from Part 3
            return { success: true, id: depositAddressId };
        } else {
            console.error(`${logPrefix} Failed to insert deposit address record (no ID returned).`);
            return { success: false, error: 'Failed to insert deposit address record (no ID returned).' };
        }
    } catch (err) {
        console.error(`${logPrefix} DB Error:`, err.message, err.code);
        if (err.code === '23505' && err.constraint === 'deposit_addresses_deposit_address_key') {
            console.error(`${logPrefix} CRITICAL - Deposit address collision: ${depositAddress}.`);
            // Consider notifying admin immediately.
            return { success: false, error: 'Deposit address collision detected.' };
        }
        return { success: false, error: `Database error creating deposit address (Code: ${err.code || 'N/A'})` };
    }
}

/**
 * Finds user ID and status associated with a deposit address. Checks cache first.
 * @param {string} depositAddress
 * @returns {Promise<{userId: string, status: string, id: number, expiresAt: Date, derivationPath: string | null} | null>} Returns null if not found, expired, or on DB error.
 */
async function findDepositAddressInfo(depositAddress) {
    const logPrefix = `[DB FindDepositAddr Addr ${depositAddress.slice(0, 6)}..]`;
    const cached = getActiveDepositAddressCache(depositAddress); // getActiveDepositAddressCache from Part 3
    const now = Date.now();

    if (cached && now < cached.expiresAt) {
        // Cache hit and still valid based on cached expiry. Fetch full details from DB for current status.
        try {
            const statusRes = await queryDatabase(
                'SELECT status, id, expires_at, derivation_path FROM deposit_addresses WHERE deposit_address = $1',
                [depositAddress]
            );
            if (statusRes.rowCount > 0) {
                const dbInfo = statusRes.rows[0];
                const dbExpiresAt = new Date(dbInfo.expires_at).getTime();
                // Optional: Validate or update cache expiry if different
                if (cached.expiresAt !== dbExpiresAt) {
                    console.warn(`${logPrefix} Cache expiry mismatch for ${depositAddress}. Updating cache.`);
                    addActiveDepositAddressCache(depositAddress, cached.userId, dbExpiresAt); // from Part 3
                }
                return { userId: cached.userId, status: dbInfo.status, id: dbInfo.id, expiresAt: new Date(dbInfo.expires_at), derivationPath: dbInfo.derivation_path };
            } else {
                // Address was cached but not found in DB - inconsistent state. Remove from cache.
                removeActiveDepositAddressCache(depositAddress); // from Part 3
                console.warn(`${logPrefix} Address ${depositAddress} cached but not in DB. Cache removed.`);
                return null;
            }
        } catch (err) {
            console.error(`${logPrefix} Error fetching details for cached address ${depositAddress}:`, err.message);
            return null; // DB error
        }
    } else if (cached) { // Cached but expired
        console.log(`${logPrefix} Cache entry for ${depositAddress} expired. Removing.`);
        removeActiveDepositAddressCache(depositAddress); // from Part 3
    }

    // Not in active cache or cache expired, fetch from DB
    try {
        const res = await queryDatabase(
            'SELECT user_id, status, id, expires_at, derivation_path FROM deposit_addresses WHERE deposit_address = $1',
            [depositAddress]
        );
        if (res.rowCount > 0) {
            const data = res.rows[0];
            const expiresAtDate = new Date(data.expires_at);
            // If it's pending and not expired, add/update it in cache
            if (data.status === 'pending' && now < expiresAtDate.getTime()) {
                addActiveDepositAddressCache(depositAddress, data.user_id, expiresAtDate.getTime()); // from Part 3
            } else if (data.status === 'pending' && now >= expiresAtDate.getTime()) {
                // Found in DB as 'pending' but is now expired. Mark it 'expired' in DB (fire-and-forget).
                console.warn(`${logPrefix} Address ${depositAddress} found as 'pending' but expired. Marking as 'expired'.`);
                queryDatabase("UPDATE deposit_addresses SET status = 'expired' WHERE id = $1 AND status = 'pending'", [data.id], pool)
                   .catch(err => console.error(`${logPrefix} Background attempt to mark ID ${data.id} as expired failed: ${err.message}`));
            }
            return { userId: data.user_id, status: data.status, id: data.id, expiresAt: expiresAtDate, derivationPath: data.derivation_path };
        }
        return null; // Not found in DB
    } catch (err) {
        console.error(`${logPrefix} Error finding address info in DB for ${depositAddress}:`, err.message);
        return null;
    }
}


/**
 * Marks a deposit address as used. MUST be called within a DB transaction.
 * Removes the address from the active cache upon successful update.
 * @param {import('pg').PoolClient} client The active database client.
 * @param {number} depositAddressId The ID of the deposit address record.
 * @returns {Promise<boolean>} True if the status was updated from 'pending' to 'used'.
 */
async function markDepositAddressUsed(client, depositAddressId) {
    const logPrefix = `[DB MarkDepositUsed ID ${depositAddressId}]`;
    try {
        // Select the address first to remove from cache later, locking the row
        const addrRes = await queryDatabase('SELECT deposit_address, status FROM deposit_addresses WHERE id = $1 FOR UPDATE', [depositAddressId], client);
        if (addrRes.rowCount === 0) {
            console.warn(`${logPrefix} Cannot mark as used: Address ID ${depositAddressId} not found.`);
            return false;
        }
        const { deposit_address: depositAddress, status: currentStatus } = addrRes.rows[0];

        if (currentStatus !== 'pending') {
            console.warn(`${logPrefix} Address ID ${depositAddressId} is already '${currentStatus}', not 'pending'. Cannot mark 'used'.`);
            // Still remove from cache if it was there (shouldn't be if not pending, but just in case)
            removeActiveDepositAddressCache(depositAddress); // from Part 3
            return false; // Indicate it wasn't updated *by this call*
        }

        // Update status only if currently pending
        const res = await queryDatabase(
            `UPDATE deposit_addresses SET status = 'used' WHERE id = $1 AND status = 'pending'`,
            [depositAddressId], client
        );
        if (res.rowCount > 0) {
            removeActiveDepositAddressCache(depositAddress); // from Part 3
            return true; // Successfully marked as used
        } else {
            // Should not happen if the SELECT FOR UPDATE and status check above worked.
            console.warn(`${logPrefix} Failed to mark as 'used' (status race condition or lock issue?).`);
            removeActiveDepositAddressCache(depositAddress); // Still attempt removal
            return false;
        }
    } catch (err) {
        console.error(`${logPrefix} Error updating status to 'used':`, err.message);
        return false;
    }
}

/**
 * Records a confirmed deposit transaction. MUST be called within a DB transaction.
 * Uses ON CONFLICT DO NOTHING to handle potential duplicate calls gracefully.
 * @param {import('pg').PoolClient} client The active database client.
 * @param {string} userId
 * @param {number} depositAddressId The ID of the 'deposit_addresses' record used.
 * @param {string} txSignature The Solana transaction signature.
 * @param {bigint} amountLamports The amount deposited.
 * @returns {Promise<{success: boolean, depositId?: number, error?: string, alreadyProcessed?: boolean}>} `alreadyProcessed:true` if conflict occurred.
 */
async function recordConfirmedDeposit(client, userId, depositAddressId, txSignature, amountLamports) {
    const stringUserId = String(userId);
    const logPrefix = `[DB RecordDeposit User ${stringUserId} TX ${txSignature.slice(0, 6)}..]`;
    const query = `
        INSERT INTO deposits (user_id, deposit_address_id, tx_signature, amount_lamports, status, created_at, processed_at)
        VALUES ($1, $2, $3, $4, 'confirmed', NOW(), NOW())
        ON CONFLICT (tx_signature) DO NOTHING
        RETURNING id;
    `;
    try {
        const res = await queryDatabase(query, [stringUserId, depositAddressId, txSignature, BigInt(amountLamports)], client);
        if (res.rowCount > 0 && res.rows[0].id) {
            const depositId = res.rows[0].id;
            return { success: true, depositId: depositId };
        } else {
            // ON CONFLICT triggered or insert failed strangely. Verify if it exists due to ON CONFLICT.
            console.warn(`${logPrefix} Deposit TX ${txSignature} potentially already processed (ON CONFLICT or no rows returned). Verifying...`);
            const existing = await queryDatabase('SELECT id FROM deposits WHERE tx_signature = $1', [txSignature], client);
            if (existing.rowCount > 0) {
                 console.log(`${logPrefix} Verified TX ${txSignature} already exists in DB (ID: ${existing.rows[0].id}).`);
                 return { success: false, error: 'Deposit transaction already processed.', alreadyProcessed: true, depositId: existing.rows[0].id };
            } else {
                 console.error(`${logPrefix} Insert failed AND ON CONFLICT didn't find row for TX ${txSignature}. DB state might be inconsistent.`);
                 return { success: false, error: 'Failed to record deposit and conflict resolution failed.' };
            }
        }
    } catch (err) {
        console.error(`${logPrefix} Error inserting deposit record for TX ${txSignature}:`, err.message, err.code);
        // Explicitly check for unique constraint violation if ON CONFLICT wasn't robust enough
        if (err.code === '23505' && err.constraint === 'deposits_tx_signature_key') {
            console.error(`${logPrefix} Deposit TX ${txSignature} already processed (Direct unique constraint violation). Verifying existing ID...`);
            const existing = await queryDatabase('SELECT id FROM deposits WHERE tx_signature = $1', [txSignature], client); // Use client
            const existingId = existing.rows[0]?.id;
            return { success: false, error: 'Deposit transaction already processed (unique constraint).', alreadyProcessed: true, depositId: existingId };
        }
        return { success: false, error: `Database error recording deposit (Code: ${err.code || 'N/A'})` };
    }
}

/**
 * Gets the next available index for deriving a deposit address for a user.
 * @param {string} userId
 * @returns {Promise<number>} The next index (e.g., 0 for the first address).
 * @throws {Error} If the database query fails.
 */
async function getNextDepositAddressIndex(userId) {
    const stringUserId = String(userId);
    const logPrefix = `[DB GetNextDepositIndex User ${stringUserId}]`;
    try {
        // Simple count - assumes indices are 0, 1, 2...
        const res = await queryDatabase(
            'SELECT COUNT(*) as count FROM deposit_addresses WHERE user_id = $1',
            [stringUserId], pool // Use default pool for this read-only operation
        );
        const nextIndex = parseInt(res.rows[0]?.count || '0', 10);
        return nextIndex;
    } catch (err) {
        console.error(`${logPrefix} Error fetching count:`, err);
        throw new Error(`Failed to determine next deposit address index for user ${stringUserId}: ${err.message}`);
    }
}


// --- Withdrawal Operations ---

/**
 * Creates a pending withdrawal request. Does NOT require an external transaction.
 * @param {string} userId
 * @param {bigint} requestedAmountLamports Amount user asked for (before fee).
 * @param {bigint} feeLamports Fee applied to this withdrawal.
 * @param {string} recipientAddress The Solana address to send to.
 * @returns {Promise<{success: boolean, withdrawalId?: number, error?: string}>}
 */
async function createWithdrawalRequest(userId, requestedAmountLamports, feeLamports, recipientAddress) {
    const stringUserId = String(userId);
    const requestedAmount = BigInt(requestedAmountLamports);
    const fee = BigInt(feeLamports);
    // Store the requested amount, fee, and final send amount (which is same as requested for user clarity, fee deducted from balance separately)
    const finalSendAmount = requestedAmount;

    const query = `
        INSERT INTO withdrawals (user_id, requested_amount_lamports, fee_lamports, final_send_amount_lamports, recipient_address, status, created_at)
        VALUES ($1, $2, $3, $4, $5, 'pending', NOW())
        RETURNING id;
    `;
    try {
        const res = await queryDatabase(query, [stringUserId, requestedAmount, fee, finalSendAmount, recipientAddress], pool); // Use default pool
        if (res.rowCount > 0 && res.rows[0].id) {
            return { success: true, withdrawalId: res.rows[0].id };
        } else {
            console.error(`[DB CreateWithdrawal] Failed to insert withdrawal request for user ${stringUserId} (no ID returned).`);
            return { success: false, error: 'Failed to insert withdrawal request (no ID returned).' };
        }
    } catch (err) {
        console.error(`[DB CreateWithdrawal] Error for user ${stringUserId}:`, err.message, err.code);
        return { success: false, error: `Database error creating withdrawal (Code: ${err.code || 'N/A'})` };
    }
}

/**
 * Updates withdrawal status, signature, error message, timestamps. Handles idempotency.
 * @param {number} withdrawalId The ID of the withdrawal record.
 * @param {'processing' | 'completed' | 'failed'} status The new status.
 * @param {import('pg').PoolClient | import('pg').Pool} [db=pool] Optional DB client or pool. Defaults to pool.
 * @param {string | null} [signature=null] Required for 'completed'.
 * @param {string | null} [errorMessage=null] Recommended for 'failed'.
 * @returns {Promise<boolean>} True on successful update or if already in target state, false otherwise.
 */
async function updateWithdrawalStatus(withdrawalId, status, db = pool, signature = null, errorMessage = null) {
    const logPrefix = `[UpdateWithdrawal ID:${withdrawalId} Status:${status}]`;

    let query = `UPDATE withdrawals SET status = $1`;
    const values = [status];
    let valueCounter = 2;

    switch (status) {
        case 'processing':
            query += `, processed_at = NOW(), completed_at = NULL, error_message = NULL, payout_tx_signature = NULL`;
            break;
        case 'completed':
            if (!signature) { console.error(`${logPrefix} Signature required for 'completed' status.`); return false; }
            query += `, payout_tx_signature = $${valueCounter++}, completed_at = NOW(), error_message = NULL, processed_at = COALESCE(processed_at, NOW())`;
            values.push(signature);
            break;
        case 'failed':
            const safeErrorMessage = (errorMessage || 'Unknown error').substring(0, 500); // Truncate
            query += `, error_message = $${valueCounter++}, completed_at = NULL, payout_tx_signature = NULL, processed_at = COALESCE(processed_at, NOW())`;
            values.push(safeErrorMessage);
            break;
        default: console.error(`${logPrefix} Invalid status "${status}".`); return false;
    }

    // Update only if not already in a terminal state (unless transitioning TO processing from pending)
    query += ` WHERE id = $${valueCounter++}`;
    if (status === 'processing') {
         query += ` AND status = 'pending'`; // Only move from pending to processing
    } else {
         query += ` AND status NOT IN ('completed', 'failed')`; // Don't update terminal states
    }
    query += ` RETURNING id;`; // Return ID if update happened
    values.push(withdrawalId);

    try {
        const res = await queryDatabase(query, values, db);
        if (res.rowCount > 0) {
            console.log(`${logPrefix} Status successfully updated.`);
            return true; // Update successful
        } else {
             // Check current status to see if it failed because it was already terminal or pending wasn't met
             const currentStatusRes = await queryDatabase('SELECT status FROM withdrawals WHERE id = $1', [withdrawalId], db);
             const currentStatus = currentStatusRes.rows[0]?.status;
             if (currentStatusRes.rowCount === 0) { console.warn(`${logPrefix} Record ID ${withdrawalId} not found for status update check.`); return false; }

             if (currentStatus === status) {
                 console.log(`${logPrefix} Record already in target state '${currentStatus}'. Update considered successful (idempotent).`);
                 return true; // Idempotency
             } else if (currentStatus === 'completed' || currentStatus === 'failed') {
                 console.warn(`${logPrefix} Failed to update. Record ID ${withdrawalId} already in terminal state ('${currentStatus}') and cannot be changed to '${status}'.`);
                 return false; // Cannot change from terminal state
             } else if (status === 'processing' && currentStatus !== 'pending') {
                 console.warn(`${logPrefix} Failed to update status to 'processing'. Current status is '${currentStatus}', not 'pending'.`);
                 return false;
             } else {
                  console.warn(`${logPrefix} Failed to update status from '${currentStatus}' to '${status}'. Condition not met.`);
                  return false;
             }
        }
    } catch (err) {
        console.error(`${logPrefix} Error updating status:`, err.message, err.code);
        if (err.code === '23505' && err.constraint === 'withdrawals_payout_tx_signature_key') {
             console.error(`${logPrefix} CRITICAL - Withdrawal TX Signature unique constraint violation for sig ${signature?.slice(0,10)}...`);
             // This implies the signature was already used. Treat as completed to avoid loops, but needs investigation.
             return true;
        }
        return false; // Other error occurred
    }
}

/** Fetches withdrawal details for processing. */
async function getWithdrawalDetails(withdrawalId) {
    try {
        const res = await queryDatabase('SELECT * FROM withdrawals WHERE id = $1', [withdrawalId], pool); // Use default pool
        if (res.rowCount === 0) {
            console.warn(`[DB GetWithdrawal] Withdrawal ID ${withdrawalId} not found.`);
            return null;
        }
        const details = res.rows[0];
        // Convert numeric fields to BigInt
        details.requested_amount_lamports = BigInt(details.requested_amount_lamports || '0');
        details.fee_lamports = BigInt(details.fee_lamports || '0');
        details.final_send_amount_lamports = BigInt(details.final_send_amount_lamports || '0');
        return details;
    } catch (err) {
        console.error(`[DB GetWithdrawal] Error fetching withdrawal ${withdrawalId}:`, err.message);
        return null;
    }
}


// --- Bet Operations ---

/**
 * Creates a record for a bet placed using internal balance.
 * MUST be called within a transaction.
 * @param {import('pg').PoolClient} client The active database client.
 * @param {string} userId
 * @param {string} chatId
 * @param {string} gameType
 * @param {object} betDetails - Game specific choices/bets (JSONB). Can be null.
 * @param {bigint} wagerAmountLamports - Amount deducted from internal balance.
 * @param {number} [priority=0] Bet priority.
 * @returns {Promise<{success: boolean, betId?: number, error?: string}>}
 */
async function createBetRecord(client, userId, chatId, gameType, betDetails, wagerAmountLamports, priority = 0) {
    const stringUserId = String(userId);
    const stringChatId = String(chatId);
    const wagerAmount = BigInt(wagerAmountLamports);

    if (wagerAmount <= 0n) {
        return { success: false, error: 'Wager amount must be positive.' };
    }

    const query = `
        INSERT INTO bets (user_id, chat_id, game_type, bet_details, wager_amount_lamports, status, created_at, priority)
        VALUES ($1, $2, $3, $4, $5, 'active', NOW(), $6)
        RETURNING id;
    `;
    try {
        // Ensure betDetails is valid JSONB or null
        const validBetDetails = (betDetails && typeof betDetails === 'object' && Object.keys(betDetails).length > 0) ? betDetails : null;
        // Parameters should match placeholders: $1=userId, $2=chatId, $3=gameType, $4=betDetails, $5=wagerAmount, $6=priority
        const res = await queryDatabase(query, [stringUserId, stringChatId, gameType, validBetDetails, wagerAmount, priority], client);
        if (res.rowCount > 0 && res.rows[0].id) {
            return { success: true, betId: res.rows[0].id };
        } else {
            console.error(`[DB CreateBet] Failed insert bet record for user ${stringUserId}, game ${gameType} (no ID).`);
            return { success: false, error: 'Failed to insert bet record (no ID returned).' };
        }
    } catch (err) {
        console.error(`[DB CreateBet] Error for user ${stringUserId}, game ${gameType}:`, err.message, err.code);
        if (err.constraint === 'bets_wager_amount_lamports_check') {
            return { success: false, error: 'Wager amount must be positive (DB check failed).' };
        }
        return { success: false, error: `Database error creating bet (Code: ${err.code || 'N/A'})` };
    }
}

/**
 * Updates a bet record upon completion. MUST be called within a transaction. Handles idempotency.
 * @param {import('pg').PoolClient} client The active database client.
 * @param {number} betId The ID of the bet to update.
 * @param {'processing_game' | 'completed_win' | 'completed_loss' | 'completed_push' | 'error_game_logic'} status New status.
 * @param {bigint | null} [payoutAmountLamports=null] Amount credited back (includes stake for win/push). Required for completed statuses except loss/error.
 * @returns {Promise<boolean>} True on successful update or if already in target state, false otherwise.
 */
async function updateBetStatus(client, betId, status, payoutAmountLamports = null) {
    // *** APPLYING FIX #4: Explicit Type Casting ***
    const logPrefix = `[UpdateBetStatus ID:${betId} Status:${status}]`;
    let finalPayout = null; // Use null for DB insertion, not string "0n"

    // Validate payout amount based on status
    if (status === 'completed_win' || status === 'completed_push') {
        if (payoutAmountLamports === null || BigInt(payoutAmountLamports) < 0n) {
            console.error(`${logPrefix} Invalid or missing payout amount (${payoutAmountLamports}) for status ${status}.`);
            return false;
        }
        finalPayout = BigInt(payoutAmountLamports);
    } else if (status === 'completed_loss' || status === 'error_game_logic') {
         finalPayout = 0n; // Correctly using BigInt 0n
    } else if (status === 'processing_game') {
         finalPayout = null; // No payout amount yet
    } else {
        console.error(`${logPrefix} Invalid status provided: ${status}`);
        return false;
    }

    // Added explicit type casts (::VARCHAR, ::BIGINT, ::INTEGER) to parameters
    // Also added check for finalPayout being NULL in the CASE statement for safety
    const query = `
        UPDATE bets
        SET status = $1::VARCHAR,
            payout_amount_lamports = $2::BIGINT, -- Use finalPayout here (verified as BigInt or null)
            processed_at = CASE
                                WHEN $1::VARCHAR LIKE 'completed_%' OR $1::VARCHAR = 'error_game_logic' THEN NOW()
                                ELSE processed_at -- Keep existing if moving to 'processing_game'
                             END
        WHERE id = $3::INTEGER AND status IN ('active', 'processing_game')
        RETURNING id; -- Return ID if update occurred
    `;
    try {
        // Ensure correct parameter types are passed (BigInt or null for payout)
        const res = await queryDatabase(query, [status, finalPayout, betId], client);
        if (res.rowCount > 0) {
            console.log(`${logPrefix} Bet status updated successfully.`);
            return true; // Update successful
        } else {
            // Check if already in the desired state (idempotency for terminal states)
            const currentStatusRes = await queryDatabase('SELECT status FROM bets WHERE id = $1::INTEGER', [betId], client);
            const currentStatus = currentStatusRes.rows[0]?.status;

            if (currentStatusRes.rowCount === 0) {
                console.warn(`${logPrefix} Bet ID ${betId} not found during status update check.`);
                return false;
            }
            if (currentStatus === status && (status.startsWith('completed_') || status === 'error_game_logic')) {
                console.log(`${logPrefix} Bet already in target terminal state '${currentStatus}'. Update successful (no-op).`);
                return true; // Idempotent success
            }
            console.warn(`${logPrefix} Failed to update status. Bet ID ${betId} not found or already in a non-updatable state (current: '${currentStatus || 'NOT_FOUND'}'). Target: '${status}'.`);
            return false; // Failed for other reasons
        }
    } catch (err) {
        // Catch potential DB errors, including type mismatches if BigInt handling isn't perfect
        console.error(`${logPrefix} Error updating status:`, err.message, err.code);
        // Log the specific error code if available
        if(err.code === '42P08') {
             console.error(`❌ ${logPrefix} Received DB Error 42P08 (ambiguous parameter type) even after casting. Query: ${query} Params: [${status}, ${finalPayout}, ${betId}]`);
        }
        return false;
    }
}


/**
 * Checks if a specific completed bet ID is the user's first *ever* completed bet.
 * Reads from the database directly.
 * @param {string} userId
 * @param {number} betId The ID of the bet to check.
 * @param {import('pg').PoolClient} [client=pool] Optional DB client.
 * @returns {Promise<boolean>} True if it's the first completed bet, false otherwise or on error.
 */
async function isFirstCompletedBet(userId, betId, client = pool) {
    const stringUserId = String(userId);
    const query = `
        SELECT id
        FROM bets
        WHERE user_id = $1 AND status LIKE 'completed_%' -- Matches win, loss, push
        ORDER BY created_at ASC, id ASC -- Consistent ordering
        LIMIT 1;
    `;
    try {
        const res = await queryDatabase(query, [stringUserId], client);
        return res.rows.length > 0 && res.rows[0].id === betId;
    } catch (err) {
        console.error(`[DB isFirstCompletedBet] Error checking for user ${stringUserId}, bet ${betId}:`, err.message);
        return false;
    }
}

/** Fetches bet details by ID. */
async function getBetDetails(betId) {
    try {
        const res = await queryDatabase('SELECT * FROM bets WHERE id = $1', [betId], pool); // Use default pool
        if (res.rowCount === 0) {
            console.warn(`[DB GetBetDetails] Bet ID ${betId} not found.`);
            return null;
        }
        const details = res.rows[0];
        // Ensure correct types
        details.wager_amount_lamports = BigInt(details.wager_amount_lamports || '0');
        details.payout_amount_lamports = details.payout_amount_lamports !== null ? BigInt(details.payout_amount_lamports) : null;
        details.priority = parseInt(details.priority || '0', 10);
        details.bet_details = details.bet_details || null; // Ensure null or parsed JSON
        return details;
    } catch (err) {
        console.error(`[DB GetBetDetails] Error fetching bet ${betId}:`, err.message);
        return null;
    }
}

/**
 * Fetches a user's bet history, paginated.
 * @param {string} userId
 * @param {number} limit Number of records to fetch.
 * @param {number} offset Number of records to skip.
 * @param {string|null} [gameKeyFilter=null] Optional game_key to filter by.
 * @param {import('pg').PoolClient} [client=pool] Optional DB client.
 * @returns {Promise<Array<object>|null>} Array of bet objects or null on error.
 */
async function getBetHistory(userId, limit = 10, offset = 0, gameKeyFilter = null, client = pool) {
    const stringUserId = String(userId);
    const logPrefix = `[DB getBetHistory User ${stringUserId}]`;
    try {
        let query = `
            SELECT id, game_type, bet_details, wager_amount_lamports, payout_amount_lamports, status, created_at, processed_at
            FROM bets
            WHERE user_id = $1
        `;
        const params = [stringUserId];
        let paramCount = 1;

        if (gameKeyFilter) {
            paramCount++; query += ` AND game_type = $${paramCount}`; params.push(gameKeyFilter);
        }

        paramCount++; query += ` ORDER BY created_at DESC, id DESC LIMIT $${paramCount}`; params.push(limit);
        paramCount++; query += ` OFFSET $${paramCount}`; params.push(offset);

        const res = await queryDatabase(query, params, client);
        return res.rows.map(row => ({
            ...row,
            wager_amount_lamports: BigInt(row.wager_amount_lamports || '0'),
            payout_amount_lamports: row.payout_amount_lamports !== null ? BigInt(row.payout_amount_lamports) : null,
            bet_details: row.bet_details || {}, // Ensure object
        }));
    } catch (err) {
        console.error(`${logPrefix} Error fetching bet history:`, err.message);
        return null;
    }
}


// --- Referral Payout Operations ---

/**
 * Records a pending referral payout. Handles unique constraint for milestone payouts gracefully.
 * @param {string} referrerUserId
 * @param {string} refereeUserId
 * @param {'initial_bet' | 'milestone'} payoutType
 * @param {bigint} payoutAmountLamports
 * @param {number | null} [triggeringBetId=null] Bet ID for 'initial_bet' type.
 * @param {bigint | null} [milestoneReachedLamports=null] Wager threshold for 'milestone' type.
 * @param {import('pg').PoolClient | import('pg').Pool} [db=pool] Optional DB client or pool. Defaults to pool.
 * @returns {Promise<{success: boolean, payoutId?: number, error?: string, duplicate?: boolean}>} `duplicate:true` if it was a milestone conflict.
 */
async function recordPendingReferralPayout(referrerUserId, refereeUserId, payoutType, payoutAmountLamports, triggeringBetId = null, milestoneReachedLamports = null, db = pool) {
    const strReferrerUserId = String(referrerUserId);
    const strRefereeUserId = String(refereeUserId);
    const payoutAmount = BigInt(payoutAmountLamports);
    const milestoneLamports = (milestoneReachedLamports !== null && BigInt(milestoneReachedLamports) >= 0n) ? BigInt(milestoneReachedLamports) : null;
    const betId = (Number.isInteger(triggeringBetId) && triggeringBetId > 0) ? triggeringBetId : null;
    const logPrefix = `[DB RecordRefPayout Ref:${strReferrerUserId} Type:${payoutType}]`;

    const query = `
        INSERT INTO referral_payouts (
            referrer_user_id, referee_user_id, payout_type, payout_amount_lamports,
            triggering_bet_id, milestone_reached_lamports, status, created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, 'pending', NOW())
        ON CONFLICT ON CONSTRAINT idx_refpayout_unique_milestone DO NOTHING
        RETURNING id;
    `;
    const values = [strReferrerUserId, strRefereeUserId, payoutType, payoutAmount, betId, milestoneLamports];
    try {
        const res = await queryDatabase(query, values, db);
        if (res.rows.length > 0 && res.rows[0].id) {
            const payoutId = res.rows[0].id;
            console.log(`${logPrefix} Recorded pending payout ID ${payoutId} (triggered by ${strRefereeUserId}).`);
            return { success: true, payoutId: payoutId };
        } else {
            // ON CONFLICT triggered or insert failed. Check if milestone conflict occurred.
            if (payoutType === 'milestone') {
                const checkExisting = await queryDatabase(`
                    SELECT id FROM referral_payouts
                    WHERE referrer_user_id = $1 AND referee_user_id = $2 AND payout_type = 'milestone' AND milestone_reached_lamports = $3
                `, [strReferrerUserId, strRefereeUserId, milestoneLamports], db);
                if (checkExisting.rowCount > 0) {
                    console.warn(`${logPrefix} Duplicate milestone payout attempt for ${formatSol(milestoneLamports)}. Ignored. Payout ID: ${checkExisting.rows[0].id}`); // formatSol from Part 3
                    return { success: false, error: 'Duplicate milestone payout attempt.', duplicate: true, payoutId: checkExisting.rows[0].id };
                }
            }
            console.error(`${logPrefix} Failed insert pending referral payout, RETURNING gave no ID (not a handled milestone conflict?).`);
            return { success: false, error: "Failed to insert pending referral payout and not a recognized duplicate." };
        }
    } catch (err) {
        console.error(`${logPrefix} Error recording pending payout (triggered by ${strRefereeUserId}):`, err.message, err.code);
        return { success: false, error: `Database error recording referral payout (Code: ${err.code || 'N/A'})` };
    }
}

/**
 * Updates referral payout status, signature, error message, timestamps. Handles idempotency.
 * @param {number} payoutId The ID of the referral_payouts record.
 * @param {'processing' | 'paid' | 'failed'} status The new status.
 * @param {import('pg').PoolClient | import('pg').Pool} [db=pool] Optional DB client or pool. Defaults to pool.
 * @param {string | null} [signature=null] Required for 'paid'.
 * @param {string | null} [errorMessage=null] Recommended for 'failed'.
 * @returns {Promise<boolean>} True on successful update or if already in target state, false otherwise.
 */
async function updateReferralPayoutStatus(payoutId, status, db = pool, signature = null, errorMessage = null) {
    const logPrefix = `[UpdateRefPayout ID:${payoutId} Status:${status}]`;

    let query = `UPDATE referral_payouts SET status = $1`;
    const values = [status];
    let valueCounter = 2;

    switch (status) {
        case 'processing': query += `, processed_at = NOW(), paid_at = NULL, error_message = NULL, payout_tx_signature = NULL`; break;
        case 'paid':
            if (!signature) { console.error(`${logPrefix} Signature required for 'paid' status.`); return false; }
            query += `, payout_tx_signature = $${valueCounter++}, paid_at = NOW(), error_message = NULL, processed_at = COALESCE(processed_at, NOW())`; values.push(signature); break;
        case 'failed':
            const safeErrorMessage = (errorMessage || 'Unknown error').substring(0, 500);
            query += `, error_message = $${valueCounter++}, paid_at = NULL, payout_tx_signature = NULL, processed_at = COALESCE(processed_at, NOW())`; values.push(safeErrorMessage); break;
        default: console.error(`${logPrefix} Invalid status "${status}".`); return false;
    }

    query += ` WHERE id = $${valueCounter++} AND status NOT IN ('paid', 'failed') RETURNING id;`;
    values.push(payoutId);

    try {
        const res = await queryDatabase(query, values, db);
        if (res.rowCount > 0) { console.log(`${logPrefix} Status updated successfully.`); return true; }
        else {
            const currentStatusRes = await queryDatabase('SELECT status FROM referral_payouts WHERE id = $1', [payoutId], db);
            const currentStatus = currentStatusRes.rows[0]?.status;
            if (currentStatusRes.rowCount === 0) { console.warn(`${logPrefix} Record ID ${payoutId} not found.`); return false; }
            if (currentStatus === status) { console.log(`${logPrefix} Record already in state '${currentStatus}'. Update successful (no-op).`); return true; }
            else if (currentStatus === 'paid' || currentStatus === 'failed') { console.warn(`${logPrefix} Failed update. Record already in terminal state ('${currentStatus}')`); return false; }
            else { console.warn(`${logPrefix} Failed update from '${currentStatus}' to '${status}'. Condition not met.`); return false; }
        }
    } catch (err) {
        console.error(`${logPrefix} Error updating status:`, err.message, err.code);
        if (err.code === '23505' && err.constraint === 'referral_payouts_payout_tx_signature_key') { console.error(`${logPrefix} CRITICAL - Ref Payout TX Sig unique constraint violation for sig ${signature?.slice(0,10)}...`); return true; /* Treat as done */ }
        return false;
    }
}

/** Fetches referral payout details for processing. */
async function getReferralPayoutDetails(payoutId) {
   try {
        const res = await queryDatabase('SELECT * FROM referral_payouts WHERE id = $1', [payoutId], pool);
        if (res.rowCount === 0) { console.warn(`[DB GetRefPayout] Payout ID ${payoutId} not found.`); return null; }
        const details = res.rows[0];
        details.payout_amount_lamports = BigInt(details.payout_amount_lamports || '0');
        details.milestone_reached_lamports = details.milestone_reached_lamports !== null ? BigInt(details.milestone_reached_lamports) : null;
        details.triggering_bet_id = details.triggering_bet_id !== null ? parseInt(details.triggering_bet_id, 10) : null;
        return details;
    } catch (err) {
        console.error(`[DB GetRefPayout] Error fetching payout ${payoutId}:`, err.message);
        return null;
    }
}

/**
 * Calculates the total earnings paid out to a specific referrer.
 * @param {string} userId The referrer's user ID.
 * @returns {Promise<bigint>} Total lamports earned and paid. Returns 0n on error.
 */
async function getTotalReferralEarnings(userId) {
    const stringUserId = String(userId);
    try {
        const result = await queryDatabase( // Use default pool
            `SELECT COALESCE(SUM(payout_amount_lamports), 0) AS total_earnings FROM referral_payouts WHERE referrer_user_id = $1 AND status = 'paid'`,
            [stringUserId]
        );
        return BigInt(result.rows[0]?.total_earnings || '0');
    } catch (err) {
        console.error(`[DB getTotalReferralEarnings] Error calculating earnings for user ${stringUserId}:`, err);
        return 0n; // Return 0 on error
    }
}

// --- Jackpot Operations ---

/**
 * Ensures a jackpot record exists for the given gameKey, creating it with seed if not.
 * @param {string} gameKey The key of the game (e.g., 'slots').
 * @param {bigint} seedAmountLamports The initial amount if jackpot needs to be created.
 * @param {import('pg').PoolClient | import('pg').Pool} [db=pool] Optional DB client or pool.
 * @returns {Promise<boolean>} True if ensured/created, false on error.
 */
async function ensureJackpotExists(gameKey, seedAmountLamports, db = pool) {
    const logPrefix = `[DB ensureJackpotExists Game:${gameKey}]`;
    try {
        const insertQuery = `
            INSERT INTO jackpots (game_key, current_amount_lamports, last_updated_at) VALUES ($1, $2, NOW())
            ON CONFLICT (game_key) DO NOTHING;
        `;
        await queryDatabase(insertQuery, [gameKey, BigInt(seedAmountLamports)], db);
        // console.log(`${logPrefix} Ensured jackpot exists.`); // Can be noisy
        return true;
    } catch (err) {
        console.error(`${logPrefix} Error ensuring jackpot exists:`, err.message, err.code);
        return false;
    }
}

/**
 * Gets the current amount of a specific jackpot. Attempts to ensure exists if not found.
 * @param {string} gameKey The key of the game (e.g., 'slots').
 * @param {import('pg').PoolClient | import('pg').Pool} [db=pool] Optional DB client or pool.
 * @returns {Promise<bigint>} Current jackpot amount in lamports, or 0n if not found/error.
 */
async function getJackpotAmount(gameKey, db = pool) {
    const logPrefix = `[DB getJackpotAmount Game:${gameKey}]`;
    try {
        const res = await queryDatabase('SELECT current_amount_lamports FROM jackpots WHERE game_key = $1', [gameKey], db);
        if (res.rowCount > 0) {
            return BigInt(res.rows[0].current_amount_lamports);
        } else {
            // Jackpot not found, try ensuring it exists with default seed
            console.warn(`${logPrefix} Jackpot not found. Ensuring with seed.`);
            const seed = GAME_CONFIG[gameKey]?.jackpotSeedLamports || SLOTS_JACKPOT_SEED_LAMPORTS || 0n; // Constants from Part 1
            const ensured = await ensureJackpotExists(gameKey, seed, db);
            if (ensured) {
                const resAgain = await queryDatabase('SELECT current_amount_lamports FROM jackpots WHERE game_key = $1', [gameKey], db);
                if (resAgain.rowCount > 0) return BigInt(resAgain.rows[0].current_amount_lamports);
            }
            console.error(`${logPrefix} Jackpot still not found after ensuring. Returning 0.`);
            return 0n;
        }
    } catch (err) {
        console.error(`${logPrefix} Error fetching jackpot amount:`, err.message);
        return 0n; // Return 0 on error
    }
}

/**
 * Updates the jackpot amount to a specific value (e.g., after a win, reset to seed).
 * MUST be called within a transaction. Locks the jackpot row.
 * @param {import('pg').PoolClient} client The active database client.
 * @param {string} gameKey The key of the game (e.g., 'slots').
 * @param {bigint} newAmountLamports The new absolute amount for the jackpot.
 * @returns {Promise<boolean>} True on success, false on failure.
 */
async function updateJackpotAmount(client, gameKey, newAmountLamports) {
    const logPrefix = `[DB updateJackpotAmount Game:${gameKey}]`;
    try {
        await queryDatabase('SELECT game_key FROM jackpots WHERE game_key = $1 FOR UPDATE', [gameKey], client); // Lock row
        const res = await queryDatabase(
            'UPDATE jackpots SET current_amount_lamports = $1, last_updated_at = NOW() WHERE game_key = $2',
            [BigInt(newAmountLamports), gameKey], client
        );
        if (res.rowCount === 0) {
            console.error(`${logPrefix} Failed update jackpot (key ${gameKey} not found?). Ensure seeded.`);
            return false;
        }
        return true;
    } catch (err) {
        console.error(`${logPrefix} Error updating jackpot amount:`, err.message);
        return false;
    }
}

/**
 * Increments the jackpot amount by a contribution.
 * MUST be called within a transaction. Locks the jackpot row.
 * @param {import('pg').PoolClient} client The active database client.
 * @param {string} gameKey The key of the game (e.g., 'slots').
 * @param {bigint} contributionLamports The amount to add to the jackpot.
 * @returns {Promise<boolean>} True on success, false on failure.
 */
async function incrementJackpotAmount(client, gameKey, contributionLamports) {
    const logPrefix = `[DB incrementJackpotAmount Game:${gameKey}]`;
    const contribution = BigInt(contributionLamports);
    if (contribution <= 0n) { return true; } // No change needed

    try {
        await queryDatabase('SELECT game_key FROM jackpots WHERE game_key = $1 FOR UPDATE', [gameKey], client); // Lock row
        const res = await queryDatabase(
            'UPDATE jackpots SET current_amount_lamports = current_amount_lamports + $1, last_updated_at = NOW() WHERE game_key = $2',
            [contribution, gameKey], client
        );
        if (res.rowCount === 0) {
            console.error(`${logPrefix} Failed increment jackpot (key ${gameKey} not found?). Ensure seeded.`);
            return false;
        }
        return true;
    } catch (err) {
        console.error(`${logPrefix} Error incrementing jackpot amount:`, err.message);
        return false;
    }
}

// --- End of Part 2 ---
// index.js - Part 3: Solana Utilities & Telegram Helpers
// --- VERSION: 3.2.1a --- (Applied Fixes: Added detailed logging to analyzeTransactionAmounts)

// --- Assuming imports from Part 1 are available ---
// (PublicKey, Keypair, SystemProgram, LAMPORTS_PER_SOL, sendAndConfirmTransaction, ComputeBudgetProgram, TransactionExpiredBlockheightExceededError, SendTransactionError, Connection)
// (bs58, createHash, crypto, bip39, derivePath)
// --- Assuming 'bot', 'solanaConnection', various cache maps, constants like 'MAX_PROCESSED_TX_CACHE_SIZE', 'WALLET_CACHE_TTL_MS', 'SOL_DECIMALS', 'DEPOSIT_CONFIRMATION_LEVEL', 'BOT_VERSION', etc. are available from Part 1 ---
// --- 'sleep' and 'escapeMarkdownV2' are defined in Part 1 and used here. ---

// --- General Utilities ---

/**
 * Formats lamports into a SOL string, removing unnecessary trailing zeros.
 * @param {bigint | number | string} lamports The amount in lamports.
 * @param {number} [maxDecimals=SOL_DECIMALS] Maximum decimals to initially format to before trimming. SOL_DECIMALS is from Part 1.
 * @returns {string} Formatted SOL string (e.g., "0.01", "1.2345", "1").
 */
function formatSol(lamports, maxDecimals = SOL_DECIMALS) { // SOL_DECIMALS is from Part 1
    if (typeof lamports === 'undefined' || lamports === null) return '0';
    try {
        // Convert input to BigInt safely
        const lamportsBigInt = BigInt(lamports);
        // Convert to SOL number using floating point for formatting
        const solNumber = Number(lamportsBigInt) / Number(LAMPORTS_PER_SOL); // LAMPORTS_PER_SOL from Part 1

        // Use toFixed for initial rounding to avoid floating point issues with many decimals
        // Ensure maxDecimals is reasonable
        const effectiveMaxDecimals = Math.min(Math.max(0, maxDecimals), SOL_DECIMALS + 2); // Cap reasonably
        let fixedString = solNumber.toFixed(effectiveMaxDecimals);

        // Improve trailing zero removal for cleaner output, especially for integers
        if (fixedString.includes('.')) {
           fixedString = fixedString.replace(/0+$/, ''); // Remove trailing zeros after decimal
           fixedString = fixedString.replace(/\.$/, ''); // Remove trailing decimal point if it exists
        }

        // Handle very small non-zero numbers that might become "0" after parseFloat/toString
        if (parseFloat(fixedString) === 0 && solNumber !== 0 && fixedString.includes('.')) {
             // If it parsed to 0 but wasn't truly 0, return the toFixed version (trimmed)
             // This preserves small values like 0.000000001
             return fixedString;
        }

        return parseFloat(fixedString).toString(); // Use parseFloat to handle cases like "1.0" -> "1"

    } catch (e) {
        console.error(`[formatSol] Error formatting lamports: ${lamports}`, e);
        return 'NaN'; // Return 'NaN' string to indicate error
    }
}


// --- Solana Utilities --

// Helper function to create safe index for BIP-44 hardened derivation
// Uses import { createHash } from 'crypto' (available from Part 1).
function createSafeIndex(userId) {
    // Hash the userId to fixed-size output
    const hash = createHash('sha256') // createHash from 'crypto' (Part 1)
        .update(String(userId))
        .digest();

    // Take last 4 bytes and convert to UInt32BE (always < 2^32)
    // Ensure the hash length is at least 4 bytes. SHA256 produces 32 bytes, so this is safe.
    const index = hash.readUInt32BE(hash.length - 4);

    // Ensure it's below hardening threshold (2^31 - 1 for hardened, or just ensure it's a valid non-hardened index part if we add 2^31 later)
    // The path `m/44'/501'/${safeIndex}'/0'/${addressIndex}'` uses hardened `safeIndex`.
    // So, `safeIndex` itself should be a value that, when added to 0x80000000, doesn't overflow.
    // More simply, `safeIndex` should be < 2^31. The apostrophe denotes hardening.
    return index % 2147483648; // 0 <= index < 2^31 (0x80000000)
}

/**
 * Derives a unique Solana keypair for deposits based on user ID and an index.
 * Uses BIP39 seed phrase and SLIP-10 path (m/44'/501'/safeIndex'/0'/addressIndex').
 * Uses ed25519-hd-key library.
 * Returns the public key and the 32-byte private key bytes needed for signing.
 * @param {string} userId The user's unique ID.
 * @param {number} addressIndex A unique index for this user's addresses (e.g., 0, 1, 2...).
 * @returns {Promise<{publicKey: import('@solana/web3.js').PublicKey, privateKeyBytes: Uint8Array, derivationPath: string} | null>} Derived public key, private key bytes, and path, or null on error.
 */
async function generateUniqueDepositAddress(userId, addressIndex) {
    userId = String(userId); // Ensure string
    const logPrefix = `[Address Gen User ${userId} Index ${addressIndex}]`;

    try {
        const seedPhrase = process.env.DEPOSIT_MASTER_SEED_PHRASE;
        if (typeof seedPhrase !== 'string' || seedPhrase.length < 20 || !bip39.validateMnemonic(seedPhrase)) { // bip39 from Part 1
            console.error(`${logPrefix} CRITICAL: DEPOSIT_MASTER_SEED_PHRASE is invalid or empty/invalid at runtime!`);
            // notifyAdmin is defined later in this Part.
            // MarkdownV2 Safety: Escape user ID, index, and static text
            if (typeof notifyAdmin === "function") {
                 await notifyAdmin(`🚨 CRITICAL: DEPOSIT\\_MASTER\\_SEED\\_PHRASE is missing or invalid\\. Cannot generate deposit addresses\\. \\(User: ${escapeMarkdownV2(userId)}, Index: ${addressIndex}\\)`); // Escaped ()
            } else {
                 console.error(`[ADMIN ALERT during generateUniqueDepositAddress] DEPOSIT_MASTER_SEED_PHRASE missing/invalid.`);
            }
            return null;
        }

        if (typeof userId !== 'string' || userId.length === 0 || typeof addressIndex !== 'number' || addressIndex < 0 || !Number.isInteger(addressIndex)) {
            console.error(`${logPrefix} Invalid userId or addressIndex`, { userId, addressIndex });
            return null;
        }

        const masterSeedBuffer = bip39.mnemonicToSeedSync(seedPhrase);
        if (!masterSeedBuffer || masterSeedBuffer.length === 0) {
            throw new Error("Failed to generate seed buffer from mnemonic\\."); // Escaped .
        }

        const safeUserIndex = createSafeIndex(userId); // This is the account index in BIP44
        // Path: m / purpose' / coin_type' / account' / change / address_index'
        // Solana SLIP-0010 uses m/44'/501'/account'/0'/address_index' (note: address_index is also hardened here)
        // Or sometimes m/44'/501'/account'/0'/address_index (non-hardened address_index)
        // Sticking to the user's path convention: `m/44'/501'/${safeIndex}'/0'/${addressIndex}'` where safeIndex is based on userId.
        const derivationPath = `m/44'/501'/${safeUserIndex}'/0'/${addressIndex}'`;


        const derivedSeedNode = derivePath(derivationPath, masterSeedBuffer.toString('hex')); // derivePath from ed25519-hd-key (Part 1)
        const privateKeyBytes = derivedSeedNode.key; // This should be the 32-byte seed for the Keypair

        if (!privateKeyBytes || privateKeyBytes.length !== 32) {
            throw new Error(`Derived private key \\(seed bytes\\) are invalid \\(length: ${privateKeyBytes?.length}\\) using ed25519\\-hd\\-key for path ${derivationPath}`); // Escaped () -
        }

        // For Solana, the derived key from ed25519-hd-key is the *seed* for Keypair.fromSeed().
        const keypair = Keypair.fromSeed(privateKeyBytes); // Keypair from @solana/web3.js (Part 1)
        const publicKey = keypair.publicKey;

        return {
            publicKey: publicKey,
            privateKeyBytes: privateKeyBytes, // This is the seed, not the expanded secret key.
            derivationPath: derivationPath
        };

    } catch (error) {
        console.error(`${logPrefix} Error during address generation: ${error.message}`);
        console.error(`${logPrefix} Error Stack: ${error.stack}`);
         // MarkdownV2 Safety: Escape user ID, index, prefix, error message
        if (typeof notifyAdmin === "function") {
           await notifyAdmin(`🚨 ERROR generating deposit address for User ${escapeMarkdownV2(userId)} Index ${addressIndex} \\(${escapeMarkdownV2(logPrefix)}\\): ${escapeMarkdownV2(error.message)}`); // Escaped ()
        } else {
            console.error(`[ADMIN ALERT during generateUniqueDepositAddress] Error: ${error.message}`);
        }
        return null;
    }
}


/**
 * Re-derives a Solana Keypair from a stored BIP44 derivation path and the master seed phrase.
 * CRITICAL: Requires DEPOSIT_MASTER_SEED_PHRASE to be available and secure.
 * This is synchronous as key derivation libraries often are.
 * @param {string} derivationPath The full BIP44 derivation path (e.g., "m/44'/501'/12345'/0'/0'").
 * @returns {import('@solana/web3.js').Keypair | null} The derived Solana Keypair object, or null on error.
 */
function getKeypairFromPath(derivationPath) {
    const logPrefix = `[GetKeypairFromPath Path:${derivationPath}]`;
    try {
        if (!derivationPath || typeof derivationPath !== 'string' || !derivationPath.startsWith("m/44'/501'/")) {
            console.error(`${logPrefix} Invalid derivation path format provided: ${derivationPath}`);
            return null;
        }

        const seedPhrase = process.env.DEPOSIT_MASTER_SEED_PHRASE;
        if (typeof seedPhrase !== 'string' || seedPhrase.length < 20 || !bip39.validateMnemonic(seedPhrase)) { // bip39 from Part 1
            console.error(`${logPrefix} CRITICAL: DEPOSIT_MASTER_SEED_PHRASE is missing or invalid when trying to re-derive key!`);
            // No async notifyAdmin here as this is synchronous. Log critical error.
            return null;
        }

        const masterSeedBuffer = bip39.mnemonicToSeedSync(seedPhrase);
        if (!masterSeedBuffer || masterSeedBuffer.length === 0) {
            throw new Error("Failed to generate seed buffer from mnemonic for path derivation\\."); // Escaped .
        }

        const derivedSeedNode = derivePath(derivationPath, masterSeedBuffer.toString('hex')); // derivePath from ed25519-hd-key (Part 1)
        const privateKeySeedBytes = derivedSeedNode.key; // This is the 32-byte seed

        if (!privateKeySeedBytes || privateKeySeedBytes.length !== 32) {
            throw new Error(`Derived private key seed bytes are invalid \\(length: ${privateKeySeedBytes?.length}\\) for path ${derivationPath}`); // Escaped ()
        }

        const keypair = Keypair.fromSeed(privateKeySeedBytes); // Keypair from @solana/web3.js (Part 1)
        return keypair;

    } catch (error) {
        console.error(`${logPrefix} Error re-deriving keypair from path ${derivationPath}: ${error.message}`);
        // Avoid logging seed phrase related errors to admin unless absolutely necessary and secured.
        if (!error.message.toLowerCase().includes('deposit_master_seed_phrase')) {
             // MarkdownV2 Safety: Escape prefix, error message, path
            console.error(`[ADMIN ALERT POTENTIAL] ${escapeMarkdownV2(logPrefix)} Unexpected error re-deriving key: ${escapeMarkdownV2(error.message)}\\. Path: ${escapeMarkdownV2(derivationPath)}`); // Escaped .
        }
        return null;
    }
}


/**
 * Checks if a Solana error is likely retryable (network, rate limit, temporary node issues).
 * @param {any} error The error object.
 * @returns {boolean} True if the error is likely retryable.
 */
function isRetryableSolanaError(error) {
    if (!error) return false;

    // Handle specific error instances from @solana/web3.js
    if (error instanceof TransactionExpiredBlockheightExceededError) { // from Part 1
        return true;
    }
    // If SendTransactionError has a cause, analyze the cause.
    if (error instanceof SendTransactionError && error.cause) { // from Part 1
       return isRetryableSolanaError(error.cause);
    }

    // Check error messages (case-insensitive)
    const message = String(error.message || '').toLowerCase();
    const retryableMessages = [
        'timeout', 'timed out', 'econnreset', 'esockettimedout', 'network error',
        'fetch', 'socket hang up', 'connection terminated', 'econnrefused', 'failed to fetch',
        'getaddrinfo enotfound', 'connection refused', 'connection reset by peer', 'etimedout',
        'transaction simulation failed', // Often retryable if due to temp node issues
        'failed to simulate transaction', // Same as above
        'blockhash not found', 'slot leader does not match', 'node is behind',
        'transaction was not confirmed', // Can be retryable, or may need checking
        'block not available', 'block cleanout',
        'sending transaction', 'connection closed', 'load balancer error',
        'backend unhealthy', 'overloaded', 'proxy internal error', 'too many requests',
        'rate limit exceeded', 'unknown block', 'leader not ready', 'heavily throttled',
        'failed to query long-term storage', 'rpc node error', 'temporarily unavailable',
        'service unavailable'
        // Add more known retryable error substrings if needed
    ];
    if (retryableMessages.some(m => message.includes(m))) return true;

    // Check HTTP status codes if present (often in error.response.status or error.status or error.statusCode)
    const status = error?.response?.status || error?.statusCode || error?.status;
    if (status && [408, 429, 500, 502, 503, 504].includes(Number(status))) return true;

    // Check error codes (often in error.code or error.cause.code or error.errorCode)
    const code = error?.code || error?.cause?.code;
    const errorCode = error?.errorCode; // Another place some libraries put it

    const retryableErrorCodesStrings = [ // String codes
        'ETIMEDOUT', 'ECONNRESET', 'ENETUNREACH', 'EAI_AGAIN', 'ECONNABORTED',
        'ECONNREFUSED', 'UND_ERR_CONNECT_TIMEOUT', 'UND_ERR_HEADERS_TIMEOUT',
        'UND_ERR_BODY_TIMEOUT', 'FETCH_TIMEOUT', 'FETCH_ERROR',
        'SLOT_LEADER_NOT_READY', // Some RPCs might return this as a string code
        // Add more known retryable string codes
    ];
    const retryableErrorCodesNumbers = [ // Numeric codes (often RPC specific, e.g. JSON RPC errors)
        -32000, -32001, -32002, -32003, -32004, -32005, // Common JSON-RPC server error range
        // Add more known retryable numeric codes
    ];

    if (typeof code === 'string' && retryableErrorCodesStrings.includes(code.toUpperCase())) return true;
    if (typeof code === 'number' && retryableErrorCodesNumbers.includes(code)) return true;
    if (typeof errorCode === 'string' && retryableErrorCodesStrings.includes(errorCode.toUpperCase())) return true;
    if (typeof errorCode === 'number' && retryableErrorCodesNumbers.includes(errorCode)) return true;


    // Check for specific error reasons some libraries might use
    if (error?.reason === 'rpc_error' || error?.reason === 'network_error') return true;

    // Check nested error data messages (e.g., from some RPC wrappers)
    if (error?.data?.message?.toLowerCase().includes('rate limit exceeded') ||
        error?.data?.message?.toLowerCase().includes('too many requests')) {
        return true;
    }

    // Default to not retryable if none of the above conditions match
    return false;
}

/**
 * Sends SOL from a designated bot wallet to a recipient.
 * Handles priority fees and uses sendAndConfirmTransaction.
 * @param {import('@solana/web3.js').PublicKey | string} recipientPublicKey The recipient's address.
 * @param {bigint} amountLamports The amount to send.
 * @param {'withdrawal' | 'referral'} payoutSource Determines which private key to use.
 * @returns {Promise<{success: boolean, signature?: import('@solana/web3.js').TransactionSignature, error?: string, isRetryable?: boolean}>} Result object.
 */
async function sendSol(recipientPublicKey, amountLamports, payoutSource) {
    const operationId = `sendSol-${payoutSource}-${Date.now().toString().slice(-6)}`;
    let recipientPubKey;
    try {
        recipientPubKey = (typeof recipientPublicKey === 'string') ? new PublicKey(recipientPublicKey) : recipientPublicKey; // PublicKey from Part 1
        if (!(recipientPubKey instanceof PublicKey)) throw new Error("Invalid recipient public key type");
    } catch (e) {
        const errorMsg = `Invalid recipient address format: "${recipientPublicKey}"\\. Error: ${e.message}`; // Escaped .
        console.error(`[${operationId}] ❌ ERROR: ${errorMsg}`);
        return { success: false, error: errorMsg, isRetryable: false };
    }

    let amountToSend;
    try {
        amountToSend = BigInt(amountLamports);
        if (amountToSend <= 0n) throw new Error('Amount to send must be positive\\.'); // Escaped .
    } catch (e) {
        const errorMsg = `Invalid amountLamports value: '${amountLamports}'\\. Error: ${e.message}`; // Escaped .
        console.error(`[${operationId}] ❌ ERROR: ${errorMsg}`);
        return { success: false, error: errorMsg, isRetryable: false };
    }

    let privateKeyEnvVar;
    let keyTypeForLog;
    // Use referral payout key if specified and available, otherwise default to main bot key
    if (payoutSource === 'referral' && process.env.REFERRAL_PAYOUT_PRIVATE_KEY) {
        privateKeyEnvVar = 'REFERRAL_PAYOUT_PRIVATE_KEY';
        keyTypeForLog = 'REFERRAL';
    } else {
        privateKeyEnvVar = 'MAIN_BOT_PRIVATE_KEY';
        keyTypeForLog = (payoutSource === 'referral' && !process.env.REFERRAL_PAYOUT_PRIVATE_KEY)
            ? 'MAIN (Defaulted for Referral)'
            : 'MAIN';
    }

    const privateKeyBase58 = process.env[privateKeyEnvVar];
    if (!privateKeyBase58) {
        const errorMsg = `Missing or empty private key environment variable ${privateKeyEnvVar} for payout source ${payoutSource} \\(Key Type: ${keyTypeForLog}\\)\\.`; // Escaped . ()
        console.error(`[${operationId}] ❌ ERROR: ${errorMsg}`);
        // MarkdownV2 Safety: Escape env var, payout source, op ID
        if (typeof notifyAdmin === "function") {
             await notifyAdmin(`🚨 CRITICAL: Missing private key env var ${escapeMarkdownV2(privateKeyEnvVar)} for payout source ${escapeMarkdownV2(payoutSource)}\\. Payout failed\\. Operation ID: ${escapeMarkdownV2(operationId)}`);
        }
        return { success: false, error: errorMsg, isRetryable: false };
    }

    let payerWallet;
    try {
        payerWallet = Keypair.fromSecretKey(bs58.decode(privateKeyBase58)); // Keypair, bs58 from Part 1
    } catch (e) {
        const errorMsg = `Failed to decode private key ${keyTypeForLog} \\(from env var ${privateKeyEnvVar}\\): ${e.message}`; // Escaped ()
        console.error(`[${operationId}] ❌ ERROR: ${errorMsg}`);
        // MarkdownV2 Safety: Escape key type, env var, error message, op ID
        if (typeof notifyAdmin === "function") {
           await notifyAdmin(`🚨 CRITICAL: Failed to decode private key ${escapeMarkdownV2(keyTypeForLog)} \\(${escapeMarkdownV2(privateKeyEnvVar)}\\): ${escapeMarkdownV2(e.message)}\\. Payout failed\\. Operation ID: ${escapeMarkdownV2(operationId)}`); // Escaped ()
        }
        return { success: false, error: errorMsg, isRetryable: false };
    }

    const amountSOLFormatted = formatSol(amountToSend); // formatSol from this Part
    console.log(`[${operationId}] Attempting to send ${amountSOLFormatted} SOL from ${payerWallet.publicKey.toBase58()} to ${recipientPubKey.toBase58()} using ${keyTypeForLog} key...`);

    try {
        const { blockhash, lastValidBlockHeight } = await solanaConnection.getLatestBlockhash(DEPOSIT_CONFIRMATION_LEVEL); // solanaConnection, DEPOSIT_CONFIRMATION_LEVEL from Part 1
        if (!blockhash || !lastValidBlockHeight) {
            throw new Error('Failed to get valid latest blockhash object from RPC\\.'); // Escaped .
        }

        const basePriorityFee = parseInt(process.env.PAYOUT_BASE_PRIORITY_FEE_MICROLAMPORTS, 10);
        const maxPriorityFee = parseInt(process.env.PAYOUT_MAX_PRIORITY_FEE_MICROLAMPORTS, 10);
        let priorityFeeMicroLamports = Math.max(1, Math.min(basePriorityFee, maxPriorityFee)); // Ensure it's within bounds and at least 1
        const computeUnitLimit = parseInt(process.env.PAYOUT_COMPUTE_UNIT_LIMIT, 10);

        const transaction = new Transaction({ // Transaction from Part 1
            recentBlockhash: blockhash,
            feePayer: payerWallet.publicKey,
        });

        // Add compute budget instructions first
        transaction.add(
            ComputeBudgetProgram.setComputeUnitLimit({ units: computeUnitLimit }) // ComputeBudgetProgram from Part 1
        );
        transaction.add(
            ComputeBudgetProgram.setComputeUnitPrice({ microLamports: priorityFeeMicroLamports })
        );
        // Then add the transfer instruction
        transaction.add(
            SystemProgram.transfer({ // SystemProgram from Part 1
                fromPubkey: payerWallet.publicKey,
                toPubkey: recipientPubKey,
                lamports: amountToSend,
            })
        );

        console.log(`[${operationId}] Sending and confirming transaction (Commitment: ${DEPOSIT_CONFIRMATION_LEVEL}, Priority Fee: ${priorityFeeMicroLamports} microLamports, CU Limit: ${computeUnitLimit})...`);
        const signature = await sendAndConfirmTransaction( // from Part 1
            solanaConnection,
            transaction,
            [payerWallet], // Signer
            {
                commitment: DEPOSIT_CONFIRMATION_LEVEL,
                skipPreflight: false, // Recommended to keep preflight for most cases
                preflightCommitment: DEPOSIT_CONFIRMATION_LEVEL,
                lastValidBlockHeight: lastValidBlockHeight,
            }
        );

        console.log(`[${operationId}] SUCCESS! ✅ Sent ${amountSOLFormatted} SOL to ${recipientPubKey.toBase58()} using ${keyTypeForLog} key. TX: ${signature}`);
        return { success: true, signature: signature };

    } catch (error) {
        console.error(`[${operationId}] ❌ SEND FAILED using ${keyTypeForLog} key. Error: ${error.message}`);
        if (error instanceof SendTransactionError && error.logs) { // SendTransactionError from Part 1
            console.error(`[${operationId}] Simulation Logs (if available, last 10):`);
            error.logs.slice(-10).forEach(log => console.error(`   -> ${log}`)); // Note: Logs themselves might contain special chars, careful if displaying to user.
        }

        const isRetryable = isRetryableSolanaError(error); // Defined in this Part
        // Generate user-friendly error message, ensuring it's escaped for potential display later
        let userFriendlyError = `Send/Confirm error: ${escapeMarkdownV2(error.message)}`; // Default error, escaped
        const errorMsgLower = String(error.message || '').toLowerCase();

        if (errorMsgLower.includes('insufficient lamports') || errorMsgLower.includes('insufficient funds')) {
             // MarkdownV2 Safety: Escape key type, address, punctuation
            userFriendlyError = `Insufficient funds in the ${escapeMarkdownV2(keyTypeForLog)} payout wallet \\(${escapeMarkdownV2(payerWallet.publicKey.toBase58())}\\)\\. Please check its balance\\.`; // Escaped .()
            // MarkdownV2 Safety: Escape key type, address, op ID
            if (typeof notifyAdmin === "function") {
                 await notifyAdmin(`🚨 CRITICAL: Insufficient funds in ${escapeMarkdownV2(keyTypeForLog)} wallet \\(${escapeMarkdownV2(payerWallet.publicKey.toBase58())}\\) for payout\\. Operation ID: ${escapeMarkdownV2(operationId)}`); // Escaped .()
            }
        } else if (error instanceof TransactionExpiredBlockheightExceededError || errorMsgLower.includes('blockhash not found') || errorMsgLower.includes('block height exceeded')) {
             // MarkdownV2 Safety: Escape punctuation
            userFriendlyError = 'Transaction expired \\(blockhash invalid\\)\\. Retrying may be required\\.'; // Escaped .()
        } else if (errorMsgLower.includes('transaction was not confirmed') || errorMsgLower.includes('timed out waiting')) {
             // MarkdownV2 Safety: Escape address, punctuation
            userFriendlyError = `Transaction confirmation timeout\\. Status unclear, may succeed later\\. Manual check advised for TX to ${escapeMarkdownV2(recipientPubKey.toBase58())}\\.`; // Escaped .
        } else if (isRetryable) {
             // MarkdownV2 Safety: Escape error message
            userFriendlyError = `Temporary network/RPC error during send/confirm: ${escapeMarkdownV2(error.message)}`;
        }
        // If no specific user-friendly message was set, the default one with escaped error.message stands.

        console.error(`[${operationId}] Failure details - Retryable: ${isRetryable}, User Friendly Error: "${userFriendlyError}"`);
        // Return the *already escaped* user-friendly error message
        return { success: false, error: userFriendlyError, isRetryable: isRetryable };
    }
}


/**
 * Analyzes a fetched Solana transaction to find the SOL amount transferred *to* a target address.
 * Handles basic SystemProgram transfers by primarily analyzing balance changes.
 * NOTE: This function's accuracy might degrade with very complex transactions or future transaction versions.
 * It primarily relies on `meta.preBalances` and `meta.postBalances`.
 * @param {import('@solana/web3.js').ConfirmedTransaction | import('@solana/web3.js').TransactionResponse | null} tx The fetched transaction object.
 * @param {string} targetAddress The address receiving the funds.
 * @returns {{transferAmount: bigint, payerAddress: string | null}} Amount in lamports and the likely payer. Returns 0n if no positive transfer found or on error.
 */
function analyzeTransactionAmounts(tx, targetAddress) {
    const logPrefix = `[analyzeAmounts Addr:${targetAddress.slice(0, 6)}..]`;
    let transferAmount = 0n;
    let payerAddress = null; // Likely payer (first signer)

    if (!tx || !targetAddress) {
        console.warn(`${logPrefix} Invalid input: Transaction object or targetAddress missing.`);
        return { transferAmount: 0n, payerAddress: null };
    }
    try {
        new PublicKey(targetAddress); // Validate targetAddress format
    } catch (e) {
        console.warn(`${logPrefix} Invalid targetAddress format: ${targetAddress}. Error: ${e.message}`);
        return { transferAmount: 0n, payerAddress: null };
    }

    if (tx.meta?.err) {
        console.log(`${logPrefix} Transaction meta contains an error. Assuming zero amount transferred. Error: ${JSON.stringify(tx.meta.err)}`);
        return { transferAmount: 0n, payerAddress: null };
    }

    // Analyze balance changes (primary method)
    if (tx.meta?.preBalances && tx.meta?.postBalances && tx.transaction?.message) {
        try {
            let accountKeysList = [];
            const message = tx.transaction.message;

            // Correctly determine the list of accounts corresponding to pre/postBalances
            // This depends on transaction version (legacy vs v0) and presence of LUTs.
            // The `TransactionMessage` object provides methods to get keys in the correct order.
            let keysForBalanceLookup = [];
            if (message.getAccountKeys) { // Check if method exists (newer @solana/web3.js)
                const accountKeysParams = tx.meta.loadedAddresses ? { accountKeys: message.accountKeys, addressLookupTableAccounts: tx.meta.loadedAddresses } : undefined;
                 // If getAccountKeys needs loadedAddresses, you might need to fetch them if not present in meta for older RPC responses
                keysForBalanceLookup = message.getAccountKeys(accountKeysParams).staticAccountKeys.map(k => k.toBase58());
                // For v0 with LUTs, the full list includes loaded addresses
                if (tx.meta.loadedAddresses) {
                    keysForBalanceLookup = [
                        ...keysForBalanceLookup, // Static keys first
                        ...(tx.meta.loadedAddresses.writable || []).map(k => k.toBase58()),
                        ...(tx.meta.loadedAddresses.readonly || []).map(k => k.toBase58())
                    ];
                }
            } else { // Fallback for older structures or libraries (less robust)
                 keysForBalanceLookup = (message.staticAccountKeys || message.accountKeys || []).map(k => k.toBase58());
                 if (tx.meta.loadedAddresses) { // Combine if LUTs present
                     keysForBalanceLookup = [
                          ...keysForBalanceLookup,
                          ...(tx.meta.loadedAddresses.writable || []).map(k => k.toBase58()),
                          ...(tx.meta.loadedAddresses.readonly || []).map(k => k.toBase58())
                     ];
                 }
            }

            if (keysForBalanceLookup.length !== tx.meta.preBalances.length || keysForBalanceLookup.length !== tx.meta.postBalances.length) {
                 console.warn(`${logPrefix} Account key length mismatch with balance arrays. Keys: ${keysForBalanceLookup.length}, Pre: ${tx.meta.preBalances.length}, Post: ${tx.meta.postBalances.length}. Cannot reliably determine balance change.`);
            } else {
                const targetIndex = keysForBalanceLookup.indexOf(targetAddress);
                // *** FIX: Added detailed logging ***
                console.log(`${logPrefix} DEBUG: Target Address: ${targetAddress}, Found Index: ${targetIndex}`);
                if (targetIndex !== -1) {
                    console.log(`${logPrefix} DEBUG: PreBalance[${targetIndex}]: ${tx.meta.preBalances[targetIndex]}, PostBalance[${targetIndex}]: ${tx.meta.postBalances[targetIndex]}`);
                } else {
                    console.log(`${logPrefix} DEBUG: Target address not found in keysForBalanceLookup:`, keysForBalanceLookup);
                }
                // *** END FIX ***

                if (targetIndex !== -1) {
                    const preBalance = BigInt(tx.meta.preBalances[targetIndex]);
                    const postBalance = BigInt(tx.meta.postBalances[targetIndex]);
                    const balanceChange = postBalance - preBalance;
                    // *** FIX: Added detailed logging ***
                    console.log(`${logPrefix} DEBUG: Calculated balanceChange: ${balanceChange}`);
                    // *** END FIX ***

                    if (balanceChange > 0n) {
                        transferAmount = balanceChange;
                        // Determine payer: typically the first signer (fee payer)
                         // Accessing message.accountKeys directly might be needed for signer info
                        const signerKeys = (message.staticAccountKeys || message.accountKeys || []);
                        if (message.header && message.header.numRequiredSignatures > 0 && signerKeys.length > 0) {
                            payerAddress = signerKeys[0].toBase58(); // First account is usually the fee payer/first signer
                        }
                    }
                } else {
                    console.log(`${logPrefix} Target address ${targetAddress} not found in transaction account keys used for balance lookup.`);
                }
            }
        } catch (e) {
            console.warn(`${logPrefix} Error analyzing balance changes for TX: ${e.message}. TX structure might be unexpected.`);
            transferAmount = 0n; // Reset on error
            payerAddress = null;
        }
    } else {
        console.log(`${logPrefix} Balance change analysis skipped: Meta or message data missing/incomplete for TX.`);
    }

    // Fallback: Check Log Messages (Less reliable, but can sometimes catch simple transfers if balance analysis fails)
    // This is a very basic check and might misinterpret complex interactions.
    if (transferAmount === 0n && tx.meta?.logMessages) {
        // Regex for SystemProgram transfers (more specific)
        const sysTransferRegex = /Program(?:.*System Program|.*11111111111111111111111111111111) invoke \[\d+\]\s+Program log: Transfer: src=([1-9A-HJ-NP-Za-km-z]{32,44}) dst=([1-9A-HJ-NP-Za-km-z]{32,44}) lamports=(\d+)/;
        // A more generic check for any instruction logging a transfer *to* the targetAddress. Be careful with this one.
        // Example: "Program log: Instruction: Transfer", followed by logs indicating source/dest/amount
        // This requires more complex log parsing and is less reliable than balance changes.
        // Sticking to the SystemProgram regex as a simple fallback.

        for (const log of tx.meta.logMessages) {
            const match = log.match(sysTransferRegex);
            if (match && match[2] === targetAddress) { // Check if destination matches target
                const potentialAmount = BigInt(match[3]);
                if (potentialAmount > 0n) {
                    console.log(`${logPrefix} Found potential transfer via SystemProgram log message: ${formatSol(potentialAmount)} SOL from ${match[1]}`);
                    transferAmount = potentialAmount;
                    payerAddress = match[1]; // Source from the log
                    break; // Take the first such explicit SystemProgram transfer found
                }
            }
        }
    }

    if (transferAmount > 0n) {
        console.log(`${logPrefix} Final analysis result for TX: Found ${formatSol(transferAmount)} SOL transfer (likely from ${payerAddress || 'Unknown'}) to ${targetAddress}.`);
    } else {
        console.log(`${logPrefix} Final analysis result for TX: No positive SOL transfer found to ${targetAddress}.`);
    }
    // *** FIX: Added detailed logging ***
    console.log(`${logPrefix} DEBUG: Returning transferAmount: ${transferAmount}, payerAddress: ${payerAddress}`);
    // *** END FIX ***
    return { transferAmount, payerAddress };
}


// --- Telegram Utilities ---

/**
 * Sends a message to all configured admin users via Telegram.
 * Ensures the input message is escaped for MarkdownV2.
 * Uses the telegramSendQueue for rate limiting.
 * @param {string} message The message text to send.
 */
async function notifyAdmin(message) {
    const adminIdsRaw = process.env.ADMIN_USER_IDS || '';
    const adminIds = adminIdsRaw.split(',')
        .map(id => id.trim())
        .filter(id => /^\d+$/.test(id)); // Filter for valid numeric Telegram User IDs

    if (adminIds.length === 0) {
        console.warn('[NotifyAdmin] No valid ADMIN_USER_IDS configured. Cannot send notification.');
        return;
    }

    const timestamp = new Date().toISOString();
    // escapeMarkdownV2 is defined in Part 1 - Escapes the user-provided message content
    const escapedMessage = escapeMarkdownV2(message);
    // MarkdownV2 Safety: Manually escape the wrapper text's special characters
    const fullMessage = `🚨 *BOT ALERT* 🚨\n\\[${escapeMarkdownV2(timestamp)}\\]\n\n${escapedMessage}`; // Escaped []

    console.log(`[NotifyAdmin] Sending alert: "${message.substring(0, 100)}..." to ${adminIds.length} admin(s).`);

    // Use Promise.allSettled to attempt sending to all admins and log individual failures
    const results = await Promise.allSettled(
        adminIds.map(adminId =>
            // safeSendMessage itself handles internal queuing and API errors
            safeSendMessage(adminId, fullMessage, { parse_mode: 'MarkdownV2' }) // Pass the fully constructed and escaped message
        )
    );

    results.forEach((result, index) => {
        if (result.status === 'rejected') {
            console.error(`[NotifyAdmin] Failed to send notification to admin ID ${adminIds[index]}:`, result.reason?.message || result.reason || 'Unknown error');
        }
    });
}


/**
 * Safely sends a Telegram message using the dedicated queue for rate limiting.
 * Handles potential errors and message length limits.
 * @param {string | number} chatId Target chat ID.
 * @param {string} text Message text. **Should already be escaped if using MarkdownV2 parse_mode**.
 * @param {import('node-telegram-bot-api').SendMessageOptions} [options={}] Additional send options (e.g., parse_mode, reply_markup).
 * @returns {Promise<import('node-telegram-bot-api').Message | undefined>} The sent message object or undefined on failure.
 */
async function safeSendMessage(chatId, text, options = {}) {
    if (!chatId || typeof text !== 'string' || text.trim() === '') {
        console.error("[safeSendMessage] Invalid input provided:", { chatId, textPreview: text?.substring(0, 50) + '...' });
        return undefined;
    }

    const MAX_LENGTH = 4096; // Telegram API message length limit
    let messageToSend = text;

    // Truncate if necessary BEFORE adding to queue
    if (messageToSend.length > MAX_LENGTH) {
        console.warn(`[safeSendMessage] Message for chat ${chatId} too long (${messageToSend.length} chars), truncating to ${MAX_LENGTH}.`);
        // Ensure ellipsis is also escaped if needed for the specified parse mode
        const ellipsis = "... (truncated)";
        const escapedEllipsis = (options.parse_mode === 'MarkdownV2' || options.parse_mode === 'Markdown')
                                 ? escapeMarkdownV2(ellipsis) // Escaped ()
                                 : ellipsis;
        const ellipsisLength = escapedEllipsis.length;
        const truncateAt = MAX_LENGTH - ellipsisLength;
        // Ensure truncation happens at a valid point (avoid cutting multi-byte chars or Markdown entities)
        // This simple substring is often okay but can break complex Markdown or unicode.
        // A more robust solution would parse/tokenize or use character counts carefully.
        messageToSend = messageToSend.substring(0, truncateAt) + escapedEllipsis;
    }

    // telegramSendQueue is from Part 1
    return telegramSendQueue.add(async () => {
        try {
            const sentMessage = await bot.sendMessage(chatId, messageToSend, options); // bot from Part 1
            return sentMessage;
        } catch (error) {
            console.error(`❌ Telegram send error to chat ${chatId}: Code: ${error.code || 'N/A'} | Message: ${error.message}`);
            if (error.response?.body) {
                console.error(`   -> Telegram API Response Body: ${JSON.stringify(error.response.body)}`);
                const errorCode = error.response.body.error_code;
                const description = error.response.body.description?.toLowerCase() || '';

                // Handle specific errors like user blocking the bot
                if (errorCode === 403 || description.includes('blocked by the user') || description.includes('user is deactivated') || description.includes('bot was kicked') || description.includes('chat not found') || description.includes("bots can't send messages to bots")) {
                    console.warn(`[TG Send] Bot interaction issue with chat ${chatId}: ${description}. Further messages might fail.`);
                    // Consider marking user/chat as inactive in DB to prevent further send attempts.
                } else if (errorCode === 400 && (description.includes('parse error') || description.includes('can\'t parse entities') || description.includes('wrong file identifier') || description.includes("character") || description.includes("tag"))) {
                    // Log parse errors more verbosely for debugging
                    console.error(`❌❌❌ Telegram Parse Error or Bad Request! Chat: ${chatId}. Description: ${description}`);
                    console.error(`   -> Sent Text (first 200 chars): ${messageToSend.substring(0,200)}`); // Log the exact text that failed
                    console.error(`   -> Options: ${JSON.stringify(options)}`);
                    // Notify admin about persistent parse errors
                    if (typeof notifyAdmin === "function") {
                         // MarkdownV2 Safety: Escape chat ID, options, description, text preview
                         await notifyAdmin(`🚨 Telegram Parse Error in \`safeSendMessage\`\nChatID: ${escapeMarkdownV2(String(chatId))}\nOptions: ${escapeMarkdownV2(JSON.stringify(options))}\nError: ${escapeMarkdownV2(description)}\nText Preview (Truncated):\n\`\`\`\n${escapeMarkdownV2(messageToSend.substring(0, 300))}\n\`\`\``)
                             .catch(e => console.error("Failed to send admin notification about parse error:", e));
                    }
                } else if (errorCode === 429) { // Too Many Requests - Should be handled by queue, but if API bursts, log it.
                     console.warn(`[TG Send] Received 429 (Too Many Requests) for chat ${chatId} despite queue. Queue interval might need adjustment or global limit hit. Error: ${description}`);
                }
            }
            return undefined; // Indicate failure
        }
    }).catch(queueError => {
        // This catch is for errors adding to the PQueue itself or task promise rejections not caught inside
        console.error(`❌ Error adding/executing job in telegramSendQueue for chat ${chatId}:`, queueError.message);
        // MarkdownV2 Safety: Escape chat ID and error message
        if (typeof notifyAdmin === "function") {
           notifyAdmin(`🚨 ERROR adding/executing job in telegramSendQueue for Chat ${escapeMarkdownV2(String(chatId))}: ${escapeMarkdownV2(queueError.message)}`).catch(()=>{});
        }
        return undefined;
    });
}


/**
 * Escapes characters for Telegram HTML parse mode.
 * @param {string | number | bigint | null | undefined} text Input text to escape.
 * @returns {string} Escaped text.
 */
function escapeHtml(text) {
    if (text === null || typeof text === 'undefined') {
        return '';
    }
    const textString = String(text);
    // Basic HTML escaping
    return textString.replace(/&/g, "&amp;")
                     .replace(/</g, "&lt;")
                     .replace(/>/g, "&gt;")
                     .replace(/"/g, "&quot;")
                     .replace(/'/g, "&#039;"); // Use &#039; for broader compatibility than &apos;
}

/**
 * Gets a display name for a user in a chat, escaping it for MarkdownV2 if not a username.
 * Prioritizes username, then first/last name, then fallback ID.
 * @param {string | number} chatId Chat ID.
 * @param {string | number} userId User ID.
 * @returns {Promise<string>} MarkdownV2 escaped display name (or @username).
 */
async function getUserDisplayName(chatId, userId) {
    const strUserId = String(userId);
    const strChatId = String(chatId);
    try {
        // Use getChatMember to fetch user details within the context of a chat
        const member = await bot.getChatMember(strChatId, strUserId); // bot from Part 1
        const user = member.user;
        let name = '';
        if (user.username) {
            // Usernames starting with @ are handled correctly by Telegram MarkdownV2, no escaping needed *if used as a mention*
            // If used purely as text, the '@' might need consideration, but typically it's for mentions.
             // Let's escape the username just in case it's used in a context where '@' isn't automatically a mention.
             // Standard practice is usually NOT to escape @usernames. Let's stick to that.
            name = `@${user.username}`; // Usernames don't need MdV2 escaping when used as @username
        } else {
            // Construct name from first/last, escape for MarkdownV2
            if (user.first_name) {
                name = user.first_name;
                if (user.last_name) {
                    name += ` ${user.last_name}`;
                }
            } else {
                name = `User ${strUserId.slice(-4)}`; // Fallback if no name
            }
            name = escapeMarkdownV2(name); // Escape first/last names or fallback ID
        }
        return name;
    } catch (error) {
        console.warn(`[getUserDisplayName] Failed to get chat member for user ${strUserId} in chat ${strChatId}: ${error.message}. Falling back to ID.`);
        // MarkdownV2 Safety: Escape the fallback ID display
        return escapeMarkdownV2(`User ${strUserId.slice(-4)}`); // Fallback, escaped
    }
}


// --- Cache Utilities ---

/** Updates the wallet cache for a user with new data, resets TTL. */
function updateWalletCache(userId, data) {
    userId = String(userId);
    const existingEntry = walletCache.get(userId); // walletCache from Part 1
    if (existingEntry?.timeoutId) {
        clearTimeout(existingEntry.timeoutId);
    }

    const entryTimestamp = Date.now();
    // Merge new data with existing, ensuring timestamp is updated
    const newData = { ...(existingEntry || {}), ...data, timestamp: entryTimestamp };

    // Set TTL expiry
    const timeoutId = setTimeout(() => {
        const currentEntry = walletCache.get(userId);
        // Verify it's the same entry before deleting (prevent race condition deletion)
        if (currentEntry && currentEntry.timestamp === entryTimestamp) {
            walletCache.delete(userId);
            // console.log(`[Cache] Wallet cache expired and removed for user ${userId}`);
        }
    }, WALLET_CACHE_TTL_MS); // WALLET_CACHE_TTL_MS from Part 1

    newData.timeoutId = timeoutId;
    // Allow Node.js to exit even if only cache timers remain
    if (timeoutId.unref) {
        timeoutId.unref();
    }
    walletCache.set(userId, newData);
}

/** Gets data from the wallet cache if entry exists and is not expired. */
function getWalletCache(userId) {
    userId = String(userId);
    const entry = walletCache.get(userId);
    if (entry) { // Check if entry exists
        // Check if TTL is still valid
        if (Date.now() - entry.timestamp < WALLET_CACHE_TTL_MS) {
            return entry; // Valid and not expired
        } else {
            // Entry found but expired, remove it
            if(entry.timeoutId) clearTimeout(entry.timeoutId); // Clear the expiry timer
            walletCache.delete(userId);
            // console.log(`[Cache] Wallet cache for user ${userId} was expired upon access.`);
            return undefined; // Return undefined for expired
        }
    }
    return undefined; // Not found
}

/** Adds/Updates an active deposit address in the cache, sets expiry timer. */
function addActiveDepositAddressCache(address, userId, expiresAtTimestamp) {
    const existingEntry = activeDepositAddresses.get(address); // activeDepositAddresses from Part 1
    if (existingEntry?.timeoutId) {
        clearTimeout(existingEntry.timeoutId); // Clear previous timer if updating
    }

    const newEntry = { userId: String(userId), expiresAt: expiresAtTimestamp };

    const delay = expiresAtTimestamp - Date.now();
    // Only set timeout and add to cache if it hasn't already expired
    if (delay > 0) {
        const timeoutId = setTimeout(() => {
            const currentEntry = activeDepositAddresses.get(address);
            // Verify it's the exact entry we set the timer for before deleting
            if (currentEntry && currentEntry.userId === String(userId) && currentEntry.expiresAt === expiresAtTimestamp) {
                activeDepositAddresses.delete(address);
                // console.log(`[Cache] Active deposit address ${address} expired and removed from cache.`);
            }
        }, delay);
        newEntry.timeoutId = timeoutId;
        if (timeoutId.unref) { // Allow Node.js to exit if only timers remain
            timeoutId.unref();
        }
        activeDepositAddresses.set(address, newEntry);
    } else {
        // Address provided is already expired, ensure it's not in the cache
        activeDepositAddresses.delete(address);
        // console.log(`[Cache] Attempted to add already expired deposit address ${address} to cache. Not added/Ensured removed.`);
    }
}

/** Gets an active deposit address entry from cache if it exists and hasn't expired. */
function getActiveDepositAddressCache(address) {
    const entry = activeDepositAddresses.get(address);
    if (entry) {
        if (Date.now() < entry.expiresAt) {
            return entry; // Valid and not expired
        } else {
            // Entry found but is expired, remove it from cache
            if(entry.timeoutId) clearTimeout(entry.timeoutId);
            activeDepositAddresses.delete(address);
            // console.log(`[Cache] Active deposit address ${address} was expired upon access from cache.`);
            return undefined; // Expired
        }
    }
    return undefined; // Not found
}

/** Explicitly removes an address from the deposit cache and clears its timer. */
function removeActiveDepositAddressCache(address) {
    const entry = activeDepositAddresses.get(address);
    if (entry?.timeoutId) {
        clearTimeout(entry.timeoutId); // Clear associated timer
    }
    const deleted = activeDepositAddresses.delete(address); // Remove from map
    // if(deleted) console.log(`[Cache] Explicitly removed deposit address ${address} from cache.`);
    return deleted; // Return true if an element was deleted, false otherwise
}

/** Adds a transaction signature to the processed deposit cache (Set acts like LRU by re-adding). */
function addProcessedDepositTx(signature) {
    // processedDepositTxSignatures is a Set from Part 1
    // Sets automatically handle uniqueness.

    // Simple approach: Just add. If memory becomes an issue, implement LRU.
    // A Set doesn't guarantee order for easy LRU. Use an Array + Set or Map for LRU.
    // Current implementation just prevents immediate re-processing.
    // Check MAX_PROCESSED_TX_CACHE_SIZE (from Part 1)
    if (processedDepositTxSignatures.size >= MAX_PROCESSED_TX_CACHE_SIZE && !processedDepositTxSignatures.has(signature)) {
         // Basic FIFO-like eviction if size limit is reached: delete an arbitrary element.
         // This is NOT true LRU. For true LRU, use a dedicated library or Map.
         const firstElementIterator = processedDepositTxSignatures.values();
         const elementToRemove = firstElementIterator.next().value;
         if (elementToRemove) {
             processedDepositTxSignatures.delete(elementToRemove);
             console.warn(`[Cache] Processed deposit TX cache reached limit (${MAX_PROCESSED_TX_CACHE_SIZE}). Removed an element to make space.`);
         }
    }
    processedDepositTxSignatures.add(signature);
}

/** Checks if a transaction signature exists in the processed deposit cache. */
function hasProcessedDepositTx(signature) {
    return processedDepositTxSignatures.has(signature); // Check existence in the Set
}


// --- Other Utilities ---

/** Generates a random referral code prefixed with 'ref_'. Uses crypto.randomBytes from Part 1. */
function generateReferralCode(length = 8) { // Default length for the hex part
    const byteLength = Math.ceil(length / 2); // Each byte becomes 2 hex chars
    const randomBytes = crypto.randomBytes(byteLength); // crypto from Part 1
    const hexString = randomBytes.toString('hex').slice(0, length); // Ensure exact length if byteLength was rounded up
    return `ref_${hexString}`;
}

// --- End of Part 3 ---
// index.js - Part 4: Game Logic
// --- VERSION: 3.2.1a --- (No fixes applied directly to this part)

// --- Assuming constants like LAMPORTS_PER_SOL, GAME_CONFIG etc are available from Part 1 ---
// --- Card utilities like createDeck, shuffleDeck etc. are defined within this Part. ---

// --- Game Logic Implementations ---
// These functions simulate the core outcome of each game.
// They do NOT handle betting, balance updates, house edge application, or user notifications/animations,
// unless specified for a particular game's core mechanic (e.g. Crash multiplier).
// That broader logic resides in the game result handlers (Part 5a).

/**
 * Plays a coinflip game.
 * @param {string} choice 'heads' or 'tails'. Not strictly used in this simple simulation but good for context if logic expands.
 * @returns {{ outcome: 'heads' | 'tails' }} The random result of the flip.
 */
function playCoinflip(choice) { // choice parameter kept for potential future use
    // Simple 50/50 random outcome
    const outcome = Math.random() < 0.5 ? 'heads' : 'tails';
    return { outcome };
}

/**
 * Simulates a simple race game outcome by picking a winning lane number.
 * @param {number} [totalLanes=4] The number of lanes/competitors in the race. This should ideally match RACE_HORSES.length.
 * @returns {{ winningLane: number }} The randomly determined winning lane number (1-indexed).
 */
function simulateRace(totalLanes = 4) { // RACE_HORSES.length could be passed from calling context
    if (totalLanes <= 0) totalLanes = 1; // Prevent errors with invalid input
    const winningLane = Math.floor(Math.random() * totalLanes) + 1; // 1 to totalLanes
    return { winningLane };
}

/**
 * Simulates a simple slots game spin. Returns symbols and payout multiplier *before* house edge.
 * Symbols: Cherry (C), Lemon (L), Orange (O), Bell (B), Seven (7), Jackpot (J)
 * Payouts (example before edge): JJJ=Jackpot!, 777=100x, BBB=20x, OOO=10x, CCC=5x, Any two Cherries=2x
 * @returns {{ symbols: [string, string, string], payoutMultiplier: number, isJackpotWin: boolean }}
 */
function simulateSlots() {
    // Define symbols and their relative weights for randomness
    // 'J' for Jackpot symbol, make it rare.
    const symbols = ['C', 'L', 'O', 'B', '7', 'J']; // J for Jackpot
    const weights = [25, 20, 15, 10, 5, 1]; // Weighted probability: J is rarest. Sum = 76

    const weightedSymbols = [];
    symbols.forEach((symbol, index) => {
        for (let i = 0; i < weights[index]; i++) {
            weightedSymbols.push(symbol);
        }
    });

    const getRandomSymbol = () => weightedSymbols[Math.floor(Math.random() * weightedSymbols.length)];

    const reel1 = getRandomSymbol();
    const reel2 = getRandomSymbol();
    const reel3 = getRandomSymbol();
    const resultSymbols = [reel1, reel2, reel3];

    let payoutMultiplier = 0;
    let isJackpotWin = false;

    if (reel1 === 'J' && reel2 === 'J' && reel3 === 'J') {
        isJackpotWin = true;
        payoutMultiplier = 0; // Jackpot win is handled separately, not by a standard multiplier here.
                               // The handler in Part 5a will award currentSlotsJackpotLamports.
    } else if (reel1 === '7' && reel2 === '7' && reel3 === '7') {
        payoutMultiplier = 100;
    } else if (reel1 === 'B' && reel2 === 'B' && reel3 === 'B') {
        payoutMultiplier = 20;
    } else if (reel1 === 'O' && reel2 === 'O' && reel3 === 'O') {
        payoutMultiplier = 10;
    } else if (reel1 === 'C' && reel2 === 'C' && reel3 === 'C') {
        payoutMultiplier = 5;
    } else {
        // Check for any two cherries (a common slot payout)
        const cherryCount = resultSymbols.filter(s => s === 'C').length;
        if (cherryCount >= 2) { // Exactly two or three (if three wasn't caught above, which it is)
            payoutMultiplier = 2;
        }
    }
    // The game handler in Part 5a will check isJackpotWin, then use payoutMultiplier, then apply house edge.
    return { symbols: resultSymbols, payoutMultiplier, isJackpotWin };
}


/**
 * Simulates a simplified Roulette spin (European style - 0 to 36).
 * @returns {{ winningNumber: number }} The winning number (0-36).
 */
function simulateRouletteSpin() {
    const winningNumber = Math.floor(Math.random() * 37); // Generates a number from 0 to 36
    return { winningNumber };
}

/**
 * Determines the base payout multiplier for a given Roulette bet type and winning number.
 * Note: This returns the multiplier for WINNINGS (e.g., 35 for straight means 35x winnings, player gets 36x stake back).
 * The actual payout calculation (stake + winnings * (1-edge)) is in the game handler.
 * @param {string} betType The type of bet (e.g., 'straight', 'red', 'even').
 * @param {string | number | null} betValue The specific number/value bet on (e.g., '5' for straight, not used for 'red').
 * @param {number} winningNumber The actual winning number from the spin (0-36).
 * @returns {number} Payout multiplier for the winnings (e.g., 35 for straight, 1 for even money bets). Returns 0 if the bet loses.
 */
function getRoulettePayoutMultiplier(betType, betValue, winningNumber) {
    let payoutMultiplier = 0; // Default to no win

    const isRed = (num) => [1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36].includes(num);
    const isBlack = (num) => num !== 0 && ![1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36].includes(num); // Any non-zero, non-red number
    const isEven = (num) => num !== 0 && num % 2 === 0;
    const isOdd = (num) => num !== 0 && num % 2 !== 0; // Number 0 is neither even nor odd for payouts
    const isLow = (num) => num >= 1 && num <= 18;  // 1-18
    const isHigh = (num) => num >= 19 && num <= 36; // 19-36

    switch (betType) {
        case 'straight': // Bet on a single number
            // betValue for straight should be the number itself (0-36)
            if (betValue !== null && winningNumber === parseInt(String(betValue), 10)) {
                payoutMultiplier = 35; // Pays 35 to 1
            }
            break;
        case 'red':   if (isRed(winningNumber)) payoutMultiplier = 1; break; // Pays 1 to 1
        case 'black': if (isBlack(winningNumber)) payoutMultiplier = 1; break; // Pays 1 to 1
        case 'even':  if (isEven(winningNumber)) payoutMultiplier = 1; break; // Pays 1 to 1
        case 'odd':   if (isOdd(winningNumber)) payoutMultiplier = 1; break;  // Pays 1 to 1
        case 'low':   if (isLow(winningNumber)) payoutMultiplier = 1; break;   // Pays 1 to 1 (1-18)
        case 'high':  if (isHigh(winningNumber)) payoutMultiplier = 1; break;  // Pays 1 to 1 (19-36)
        // Other bet types like dozens, columns, splits, streets, corners can be added here.
        default:
            console.warn(`[getRoulettePayoutMultiplier] Unknown or unhandled bet type: ${betType}`);
            break;
    }
    return payoutMultiplier;
}


/**
 * Simulates a simplified game of Casino War. Ace is high. No "going to war" on a tie for this version.
 * @returns {{ result: 'win' | 'loss' | 'push', playerCard: string, dealerCard: string, playerRank: number, dealerRank: number }}
 */
function simulateWar() {
    const deck = createDeck(); // Defined later in this Part
    shuffleDeck(deck); // Defined later in this Part

    const playerCardObject = dealCard(deck); // Card object
    const dealerCardObject = dealCard(deck); // Card object

    if (!playerCardObject || !dealerCardObject) { // Should not happen with a fresh deck
        console.error("[simulateWar] Error dealing cards, deck empty prematurely.");
        // Fallback or throw error. For now, let's assume a loss to prevent undefined behavior.
        return { result: 'loss', playerCard: 'Error', dealerCard: 'Error', playerRank: 0, dealerRank: 0 };
    }

    const playerCard = formatCard(playerCardObject); // Formatted string
    const dealerCard = formatCard(dealerCardObject); // Formatted string

    const playerRank = getCardNumericValue(playerCardObject, true); // true for Ace high in War (Ace=14)
    const dealerRank = getCardNumericValue(dealerCardObject, true); // true for Ace high in War

    let result;
    if (playerRank > dealerRank) result = 'win';
    else if (playerRank < dealerRank) result = 'loss';
    else result = 'push'; // Simplified: no "go to war" option, push on tie

    return { result, playerCard, dealerCard, playerRank, dealerRank };
}

// --- NEW GAME LOGIC: Crash ---
/**
 * Simulates a Crash game round to determine the crash point.
 * The actual game flow (increasing multiplier, user cashout) is in Part 5a.
 * This function determines at what multiplier the game would naturally crash.
 * @returns {{ crashMultiplier: number }} The multiplier at which the game crashes, rounded to 2 decimal places.
 */
function simulateCrash() {
    // Using the formula: E = (1-H) / ( (1-H) * ln( (1-H)/(1-MAX_X) ) + (1-MAX_X) -1 )
    // simpler: P(crash at X) = (1 - HouseEdge) / X^2 (for X >= 1)
    // To simulate this, we can use inverse transform sampling if `r` is uniform random in [0,1):
    // r = P(crash_point < x) = integral from 1 to x of ( (1-H) / t^2 ) dt
    // r = (1-H) * [-1/t] from 1 to x
    // r = (1-H) * (1 - 1/x)
    // r / (1-H) = 1 - 1/x
    // 1/x = 1 - r / (1-H)
    // x = 1 / (1 - r / (1-H))
    // Let u = Math.random() be a uniform random number in [0, 1).
    // To prevent crashMultiplier from being 1 exactly (unless houseEdge makes it so),
    // and to ensure a minimum increase if it doesn't crash at 1.00x.
    // A common setup is to have a small chance (e.g., houseEdge) of an instant bust (crash at 1.00x).
    // If it doesn't bust instantly, then the multiplier is determined.

    const houseEdge = GAME_CONFIG.crash.houseEdge; // From Part 1

    const r = Math.random(); // Uniform random number [0, 1)

    let crashMultiplier;

    // Small probability (e.g., 1% or houseEdge directly) for an instant crash at 1.00x
    // This ensures the house edge is met over time.
    // Let's use houseEdge as the probability of an instant 1.00x crash.
    if (r < houseEdge) {
        crashMultiplier = 1.00;
    } else {
        // For the remaining (1 - houseEdge) probability space, calculate crash point.
        // We need to use a new random number `r_prime` that is uniform in [0, 1)
        // and map it to the desired distribution.
        // The formula `x = 1 / (1 - r_prime)` gives a distribution where P(x > X) = 1/X.
        // To incorporate house edge properly into the curve itself if not doing an instant bust:
        const r_prime = Math.random(); // Another random draw
        crashMultiplier = (1 - houseEdge) / (1 - r_prime); // Derived from P(crash < X) = 1 - (1-H)/X

        // Ensure it's at least 1.00 (or 1.01 if instant bust didn't occur)
        if (crashMultiplier < 1.00) {
             crashMultiplier = 1.00; // Or set a minimum like 1.01 to ensure some animation
        }
    }

    // Round to two decimal places as is common in Crash games
    return { crashMultiplier: parseFloat(crashMultiplier.toFixed(2)) };
}


// --- NEW GAME LOGIC: Blackjack & Card Utilities ---

const BJ_SUITS = ['♠️', '♥️', '♦️', '♣️']; // Spades, Hearts, Diamonds, Clubs
const BJ_RANKS = ['2', '3', '4', '5', '6', '7', '8', '9', 'T', 'J', 'Q', 'K', 'A']; // T for Ten

/**
 * Creates a standard 52-card deck.
 * @returns {Array<{rank: string, suit: string}>} An array of card objects.
 */
function createDeck() {
    const deck = [];
    for (const suit of BJ_SUITS) {
        for (const rank of BJ_RANKS) {
            deck.push({ rank, suit });
        }
    }
    return deck;
}

/**
 * Shuffles a deck of cards in place using Fisher-Yates algorithm.
 * @param {Array<{rank: string, suit: string}>} deck The deck to be shuffled.
 */
function shuffleDeck(deck) {
    for (let i = deck.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [deck[i], deck[j]] = [deck[j], deck[i]]; // Swap elements
    }
}

/**
 * Deals a single card from the top of the deck (end of the array).
 * @param {Array<{rank: string, suit: string}>} deck The deck to deal from.
 * @returns {{rank: string, suit: string} | undefined} The card object, or undefined if deck is empty.
 */
function dealCard(deck) {
    return deck.pop();
}

/**
 * Gets the numerical value of a card for games like Blackjack or War.
 * For Blackjack, Ace is initially 11, adjusted by calculateHandValue.
 * For War, Ace is typically high (14).
 * @param {{rank: string, suit: string}} card The card object.
 * @param {boolean} [aceHighInWar=false] If true and rank is Ace, returns 14 (for War).
 * For Blackjack, Ace is 11 (calculateHandValue handles 1 vs 11 logic).
 * @returns {number} The numerical value of the card.
 */
function getCardNumericValue(card, aceHighInWar = false) {
    if (!card || !card.rank) return 0;
    const rank = card.rank;
    if (['K', 'Q', 'J', 'T'].includes(rank)) return 10;
    if (rank === 'A') {
        return aceHighInWar ? 14 : 11; // For Blackjack, Ace is 11 (value adjusted later if bust)
    }
    return parseInt(rank, 10); // For ranks '2' through '9'
}

/**
 * Formats a card object for display (e.g., "K♠️", "A♥️").
 * @param {{rank: string, suit: string}} card The card object.
 * @returns {string} A string representation of the card.
 */
function formatCard(card) {
    if (!card || !card.rank || !card.suit) return "??"; // Should not happen with valid card objects
    return `${card.rank}${card.suit}`;
}

/**
 * Calculates the total value of a hand in Blackjack, handling Aces correctly (as 1 or 11).
 * @param {Array<{rank: string, suit: string}>} hand An array of card objects.
 * @returns {number} The best possible hand value not exceeding 21, or the lowest value if over 21.
 */
function calculateHandValue(hand) {
    let value = 0;
    let aceCount = 0;
    for (const card of hand) {
        const cardValue = getCardNumericValue(card, false); // false for aceHighInWar, so Ace is 11
        value += cardValue;
        if (card.rank === 'A') {
            aceCount++;
        }
    }
    // Adjust for Aces if value is over 21 and Aces are present
    while (value > 21 && aceCount > 0) {
        value -= 10; // Change an Ace from 11 to 1
        aceCount--;
    }
    return value;
}

/**
 * Simulates the dealer's turn in Blackjack. Dealer hits until 17 or more, or busts.
 * Dealer typically hits on soft 17 (Ace + 6) in many rules, but for simplicity here, stands on any 17.
 * @param {Array<{rank: string, suit: string}>} dealerHand The dealer's current hand.
 * @param {Array<{rank: string, suit: string}>} deck The current game deck.
 * @returns {{hand: Array<{rank: string, suit: string}>, value: number, busted: boolean}} The dealer's final hand, value, and bust status.
 */
function simulateDealerPlay(dealerHand, deck) {
    let handValue = calculateHandValue(dealerHand);
    // Standard rule: Dealer hits until hand value is 17 or more.
    // Some casinos hit on soft 17 (Ace + 6). For simplicity, let's assume dealer stands on all 17s.
    while (handValue < 17) {
        const newCard = dealCard(deck);
        if (newCard) {
            dealerHand.push(newCard);
            handValue = calculateHandValue(dealerHand);
        } else {
            // Should not happen in a normal game if deck is sufficient.
            console.error("[simulateDealerPlay] Deck empty during dealer's turn.");
            break;
        }
    }
    return {
        hand: dealerHand,
        value: handValue,
        busted: handValue > 21
    };
}

/**
 * Determines the winner of a Blackjack hand after all actions are complete.
 * This function is called by the game handler in Part 5a once player stands or busts, or game ends.
 * @param {Array<{rank: string, suit: string}>} playerHand Player's final hand.
 * @param {Array<{rank: string, suit: string}>} dealerHand Dealer's final hand (after dealer has played).
 * @returns {{
 * result: 'player_blackjack' | 'player_wins' | 'dealer_wins' | 'push' | 'player_busts' | 'dealer_busts_player_wins',
 * playerValue: number,
 * dealerValue: number,
 * playerHandFormatted: string[],
 * dealerHandFormatted: string[]
 * }} Detailed result of the game.
 */
function determineBlackjackWinner(playerHand, dealerHand) {
    const playerValue = calculateHandValue(playerHand);
    const dealerValue = calculateHandValue(dealerHand); // Assuming dealer has completed their play

    const playerHasBlackjack = playerValue === 21 && playerHand.length === 2;
    const dealerHasBlackjack = dealerValue === 21 && dealerHand.length === 2;

    let result;

    if (playerValue > 21) {
        result = 'player_busts'; // Player loses
    } else if (dealerValue > 21) {
        result = 'dealer_busts_player_wins'; // Player wins (as long as player didn't bust)
    } else if (playerHasBlackjack && !dealerHasBlackjack) {
        result = 'player_blackjack'; // Player wins with Blackjack (typically 3:2 payout)
    } else if (dealerHasBlackjack && !playerHasBlackjack) {
        result = 'dealer_wins'; // Dealer wins with Blackjack
    } else if (playerHasBlackjack && dealerHasBlackjack) {
        result = 'push'; // Both have Blackjack
    } else if (playerValue > dealerValue) {
        result = 'player_wins';
    } else if (dealerValue > playerValue) {
        result = 'dealer_wins';
    } else { // playerValue === dealerValue (and neither busted, neither has un-pushed blackjack)
        result = 'push';
    }

    return {
        result,
        playerValue,
        dealerValue,
        playerHandFormatted: playerHand.map(formatCard),
        dealerHandFormatted: dealerHand.map(formatCard),
    };
}

// --- End of Part 4 ---
// index.js - Part 5a: Telegram Message/Callback Handlers & Game Result Processing
// --- VERSION: 3.2.1t --- (Applying Fixes: CORRECTED '>' escape in proceedToGameStep category message, CORRECTED ReferenceError in proceedToGameStep, Routing Roulette intermediate buttons, Win Payout Accounting(CF,Race,Slots), Play Again Cooldown)

// --- Assuming functions from Part 1, 2, 3, 4 are available ---
// (bot, pool, caches, GAME_CONFIG, DB ops, utils like safeSendMessage, escapeMarkdownV2, formatSol, sleep, getUserBalance, etc.),
// (game simulation functions, card utilities),
// --- 'commandHandlers' and 'menuCommandHandlers' (Maps defined in Part 5b, declared in Part 1) will be used here.
// --- '_handleReferralChecks' (from Part 1) is used by 'placeBet'.
// --- 'routeStatefulInput' (from Part 5b) is used by 'handleMessage'.
// --- USER_COMMAND_COOLDOWN_MS, USER_STATE_TTL_MS, userStateCache, commandCooldown, userLastBetAmounts from Part 1.
// --- LAMPORTS_PER_SOL, SOL_DECIMALS from Part 1.
// --- getJackpotAmount, incrementJackpotAmount, updateJackpotAmount (from Part 2) used by handleSlotsGame.
// --- Constants like RACE_HORSES are from Part 1.
// --- clearUserState is from Part 6.
// --- showBetAmountButtons is from Part 1.

// --- Main Message Handler (Entry point from Part 6 webhook/polling) ---
/**
 * Handles incoming Telegram messages.
 * Routes to command handlers or stateful input handlers.
 * @param {import('node-telegram-bot-api').Message} msg The Telegram message object.
 */
async function handleMessage(msg) {
    // Check if message is valid and has sender ID
    if (!msg || !msg.from || !msg.from.id || !msg.chat || !msg.chat.id) {
        console.warn('[handleMessage] Received invalid message object:', msg);
        return;
    }
    const chatId = String(msg.chat.id);
    const userId = String(msg.from.id); // User ID of the sender
    const text = msg.text || '';
    const messageId = msg.message_id; // ID of the incoming message
    const logPrefix = `[Msg Rcv User ${userId} Chat ${chatId}]`;

    console.log(`${logPrefix} Received: "${text.substring(0, 50)}${text.length > 50 ? '...' : ''}"`);

    // Cooldown check (basic spam prevention)
    const now = Date.now();
    const lastCommandTime = commandCooldown.get(userId) || 0; // commandCooldown from Part 1
    if (now - lastCommandTime < USER_COMMAND_COOLDOWN_MS) { // USER_COMMAND_COOLDOWN_MS from Part 1
        // console.log(`${logPrefix} Command cooldown active for user ${userId}. Ignoring.`);
        return; // Silently ignore if too fast
    }
    commandCooldown.set(userId, now); // Update last command time immediately

    // Check if user is awaiting stateful input
    const userCurrentState = userStateCache.get(userId); // userStateCache from Part 1

    // Handle /cancel command to clear state at any point during stateful input
    if (text.toLowerCase() === '/cancel' && userCurrentState) {
        // MarkdownV2 Safety: Escape breadcrumb and static text
        const stateClearedMessage = userCurrentState.data?.breadcrumb
            ? `Action '${escapeMarkdownV2(userCurrentState.data.breadcrumb)}' cancelled\\.` // Escaped .
            : "Current action cancelled\\."; // Escaped .
        const originalStateMessageId = userCurrentState.messageId; // Message that prompted the state
        clearUserState(userId); // Clear state from cache (from Part 6)
        console.log(`${logPrefix} User cancelled stateful input. State cleared.`);
        // Try to edit the message that prompted the state, if available and makes sense
        if (originalStateMessageId && userCurrentState.chatId === chatId) {
            bot.editMessageText(stateClearedMessage, { // bot from Part 1
                chat_id: chatId,
                message_id: originalStateMessageId,
                reply_markup: { inline_keyboard: [] }, // Clear buttons
                parse_mode: 'MarkdownV2'
            }).catch(e => {
                console.warn(`${logPrefix} Failed to edit message on /cancel: ${e.message}. Sending new message.`);
                safeSendMessage(chatId, stateClearedMessage, { parse_mode: 'MarkdownV2' }); // safeSendMessage from Part 3
            });
        } else {
            safeSendMessage(chatId, stateClearedMessage, { parse_mode: 'MarkdownV2' });
        }
        // Delete the user's /cancel message
        bot.deleteMessage(chatId, messageId).catch(()=>{});
        return;
    }

    // Route to stateful input handler if applicable
    if (userCurrentState && now - userCurrentState.timestamp < USER_STATE_TTL_MS) { // USER_STATE_TTL_MS from Part 1
        // Ensure state is for this chat to prevent cross-chat state issues if bot is in groups
        if (userCurrentState.chatId === chatId) {
            console.log(`${logPrefix} Routing to stateful input handler for state: ${userCurrentState.state || userCurrentState.action}`);
            // routeStatefulInput defined in Part 5b
            return routeStatefulInput(msg, userCurrentState);
        } else {
            console.warn(`${logPrefix} State mismatch: User state for chat ${userCurrentState.chatId}, but message from ${chatId}. Clearing state for safety.`);
            clearUserState(userId); // from Part 6
            // Potentially inform the user that their previous context in another chat was cleared.
        }
    } else if (userCurrentState) { // State exists but timed out
        console.log(`${logPrefix} User state '${userCurrentState.state || userCurrentState.action}' timed out for user ${userId}. Clearing state.`);
        clearUserState(userId);
        // Optionally inform the user their previous action timed out.
    }

    // Handle commands if not in a stateful input mode (or if state timed out)
    if (text.startsWith('/')) {
        const args = text.split(' ');
        const commandName = args[0].toLowerCase().split('@')[0]; // Command name like '/start', '/wallet'

        // commandHandlers is a Map defined in Part 5b, declared in Part 1
        if (commandHandlers && commandHandlers.has(commandName)) {
            console.log(`[DEBUG handleMessage] Command received:`, commandName); // DEBUG LOG ADDED PREVIOUSLY
            console.log(`[DEBUG handleMessage] commandHandlers object exists?`, !!commandHandlers); // DEBUG LOG ADDED PREVIOUSLY
            if (commandHandlers) { // DEBUG LOG ADDED PREVIOUSLY
                 console.log('[DEBUG handleMessage] commandHandlers size:', commandHandlers.size); // DEBUG LOG ADDED PREVIOUSLY
                 console.log('[DEBUG handleMessage] Has /start?', commandHandlers.has('/start')); // DEBUG LOG ADDED PREVIOUSLY
                 console.log('[DEBUG handleMessage] Has received command?', commandHandlers.has(commandName)); // DEBUG LOG ADDED PREVIOUSLY
            }
            console.log(`${logPrefix} Executing command: ${commandName}`);
            const handler = commandHandlers.get(commandName);
            // Ensure user exists before executing most commands (except /start which handles it)
            if (commandName !== '/start') {
                let tempClient = null;
                try {
                    tempClient = await pool.connect(); // pool from Part 1
                    // Ensure user exists using the sender's ID from the message
                    await ensureUserExists(userId, tempClient); // ensureUserExists from Part 2
                } catch(dbError) {
                    console.error(`${logPrefix} DB error ensuring user exists for command ${commandName}: ${dbError.message}`);
                    // MarkdownV2 Safety: Escape static error message
                    safeSendMessage(chatId, "A database error occurred\\. Please try again later\\.", { parse_mode: 'MarkdownV2' }); // Escaped .
                    if (tempClient) tempClient.release();
                    return;
                } finally {
                    if (tempClient) tempClient.release();
                }
            }
            // Call handler with message and args array (including command name at index 0).
            // correctUserIdFromCb will be null here, handlers use msg.from.id.
            return handler(msg, args, null);
        } else if (commandName === '/admin' && process.env.ADMIN_USER_IDS.split(',').map(id => id.trim()).includes(userId)) { // ADMIN_USER_IDS from Part 1 env
            // handleAdminCommand is now defined in Part 1
            // Pass only msg and args, as admin commands check msg.from.id directly
            return handleAdminCommand(msg, args); // handleAdminCommand from Part 1
        } else {
            // *** ADD DEBUG LOGS BEFORE THIS BLOCK *** (Added previously)
            console.log('[DEBUG handleMessage] Command received:', commandName);
            console.log('[DEBUG handleMessage] commandHandlers object exists?', !!commandHandlers);
            if (commandHandlers) {
                 console.log('[DEBUG handleMessage] commandHandlers size:', commandHandlers.size);
                 console.log('[DEBUG handleMessage] Has /start?', commandHandlers.has('/start'));
                 console.log('[DEBUG handleMessage] Has received command?', commandHandlers.has(commandName));
            }
            // Original "Unknown command" message send:
            return safeSendMessage(chatId, "😕 Unknown command\\. Type /help to see available commands\\.", { parse_mode: 'MarkdownV2' }); // Escaped .
        }
    }
    // Ignore non-command, non-stateful messages
}
// --- End Main Message Handler ---


// --- Main Callback Query Handler ---
/**
 * Handles incoming Telegram callback queries (from inline buttons).
 * @param {import('node-telegram-bot-api').CallbackQuery} callbackQuery The callback query object.
 */
async function handleCallbackQuery(callbackQuery) {
    if (!callbackQuery || !callbackQuery.from || !callbackQuery.from.id || !callbackQuery.message || !callbackQuery.message.chat || !callbackQuery.message.chat.id || !callbackQuery.data) {
        console.warn('[handleCallbackQuery] Received invalid callbackQuery object:', callbackQuery);
        if (callbackQuery && callbackQuery.id) { bot.answerCallbackQuery(callbackQuery.id, { text: "Error processing action.", show_alert: true }).catch(()=>{}); }
        return;
    }
    const userId = String(callbackQuery.from.id); // User who CLICKED the button
    const chatId = String(callbackQuery.message.chat.id);
    const messageId = callbackQuery.message.message_id;
    const originalMessage = callbackQuery.message; // Keep original message object
    const data = callbackQuery.data;
    const logPrefix = `[CB Rcv User ${userId} Chat ${chatId} Data ${data}]`;

    console.log(`${logPrefix} Received callback.`);
    bot.answerCallbackQuery(callbackQuery.id).catch(err => console.warn(`${logPrefix} Non-critical error answering CB query: ${err.message}`)); // Answer immediately

    // *** FIX #1: Removed Callback Cooldown Check ***

    // Parse action and params
    const actionParts = data.split(':');
    const action = actionParts[0];
    const params = actionParts.slice(1); // Define params here for wider scope if needed later

    // Ensure user exists for most actions
    let tempClient = null;
    try {
        tempClient = await pool.connect(); // pool from Part 1
        await ensureUserExists(userId, tempClient); // ensureUserExists from Part 2
    } catch (dbError) {
        console.error(`${logPrefix} DB error ensuring user exists for callback ${data}: ${dbError.message}`);
        safeSendMessage(chatId, "A database error occurred\\. Please try again later\\.", { parse_mode: 'MarkdownV2' }); // Escaped .
        if (tempClient) tempClient.release();
        return;
    } finally {
        if (tempClient) tempClient.release();
    }

    // --- Route callback based on action prefix ---
    try {
        // Route menu actions
        if (action === 'menu') {
            const menuKey = params[0] || 'main';
            // menuCommandHandlers defined in Part 5b
            if (menuCommandHandlers && menuCommandHandlers.has(menuKey)) {
                console.log(`${logPrefix} Routing to menu handler: ${menuKey}`);
                const handler = menuCommandHandlers.get(menuKey);
                const remainingParams = params.slice(1);
                // Handle based on expected handler signature
                // Standard command handlers expect (msgOrCbMsg, args, correctUserIdFromCb)
                // handleMenuAction expects (userId, chatId, messageId, menuKey, params, isFromCallback)
                if (handler === handleMenuAction) {
                    // Specific call signature for handleMenuAction
                    return handler(userId, chatId, messageId, menuKey, remainingParams, true);
                } else if (typeof handler === 'function') {
                    // Standard command handler signature
                    // Pass originalMessage as first arg, remaining params as second, userId as third
                    return handler(originalMessage, remainingParams, userId);
                } else {
                    console.error(`${logPrefix} Invalid handler found in menuCommandHandlers for key '${menuKey}'.`);
                    return safeSendMessage(chatId, `Internal error processing menu option: ${escapeMarkdownV2(menuKey)}`, { parse_mode: 'MarkdownV2'});
                }
            } else {
                console.warn(`${logPrefix} Unknown menu key: ${menuKey}`);
                return safeSendMessage(chatId, `Unknown menu option: \`${escapeMarkdownV2(menuKey)}\``, { parse_mode: 'MarkdownV2'});
            }
        }

        // Route select_game action (shows bet amount buttons)
        else if (action === 'select_game') {
            const gameKey = params[0];
            if (GAME_CONFIG[gameKey]) { // GAME_CONFIG from Part 1
                await showBetAmountButtons(originalMessage, gameKey, null, userId); // showBetAmountButtons from Part 1
            } else {
                console.warn(`${logPrefix} Unknown game key in select_game: ${gameKey}`);
                bot.editMessageText(`⚠️ Unknown game: \`${escapeMarkdownV2(gameKey)}\``, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2' });
            }
        }

        // --- FIX #3: Explicitly handle intermediate Roulette steps ---
        else if (action === 'roulette_select_bet_type') {
            const gameKey = 'roulette';
            console.log(`${logPrefix} Routing intermediate step for Roulette bet type selection.`);
            // Call proceedToGameStep to display category buttons or handle straight bet setup
            await proceedToGameStep(userId, chatId, messageId, gameKey, data); // data contains the full callback string
        }
        else if (action === 'roulette_bet_type_category') {
            const gameKey = 'roulette';
            console.log(`${logPrefix} Routing intermediate step for Roulette category specific bet selection.`);
            // Call proceedToGameStep again to display specific bet buttons (Red/Black, Even/Odd, Low/High)
            await proceedToGameStep(userId, chatId, messageId, gameKey, data); // data contains the full callback string
        }
        // --- End FIX #3 ---

        // Handle intermediate game callbacks explicitly (Coinflip, Race - Roulette handled above)
        else if (action === 'coinflip_select_side' || action === 'race_select_horse') {
            const gameKey = action.split('_')[0];
            if (GAME_CONFIG[gameKey]) {
                 console.log(`${logPrefix} Routing intermediate step for ${gameKey}.`);
                 await proceedToGameStep(userId, chatId, messageId, gameKey, data); // data contains the full callback string
            } else {
                console.error(`${logPrefix} Invalid game key derived from action: ${action}`);
                bot.editMessageText("⚠️ Error processing game step selection\\.", { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2' });
            }
        }

        // Handle 'confirm_bet' action (Core bet execution trigger)
        else if (action === 'confirm_bet') {
            const gameKey = params[0];
            const betAmountLamportsStr = params[1];
            const betChoice1 = params[2]; // e.g., 'heads', horse number, 'red', 'straight'
            const betChoice2 = params[3]; // e.g., straight bet number for Roulette

            console.log(`${logPrefix} Confirming bet for ${gameKey}, amount ${betAmountLamportsStr}, choice1: ${betChoice1}, choice2: ${betChoice2}`);

            if (!GAME_CONFIG[gameKey] || !betAmountLamportsStr) {
                console.error(`${logPrefix} Invalid gameKey or betAmount in confirm_bet: ${data}`);
                bot.editMessageText("⚠️ Error confirming bet due to invalid parameters\\.", { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2' });
                return;
            }

            let betAmountLamports;
            try {
                betAmountLamports = BigInt(betAmountLamportsStr);
                if (betAmountLamports <= 0n) throw new Error("Invalid amount");
            } catch (e) {
                console.error(`${logPrefix} Invalid bet amount in confirm_bet: ${betAmountLamportsStr}`);
                 return bot.editMessageText(`⚠️ Invalid bet amount specified: \`${escapeMarkdownV2(betAmountLamportsStr)}\`\\.`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2' });
            }

            bot.editMessageText(
                `⏳ Processing your ${escapeMarkdownV2(GAME_CONFIG[gameKey].name)} bet of ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL\\.\\.\\.`,
                { chat_id: chatId, message_id: messageId, reply_markup: { inline_keyboard: [] }, parse_mode: 'MarkdownV2' }
            ).catch(e => console.warn(`${logPrefix} Error editing message for bet processing (confirm_bet): ${e.message}`));

            switch (gameKey) {
                case 'coinflip': return handleCoinflipGame(userId, chatId, messageId, betAmountLamports, betChoice1);
                case 'race': return handleRaceGame(userId, chatId, messageId, betAmountLamports, parseInt(betChoice1, 10));
                case 'slots': return handleSlotsGame(userId, chatId, messageId, betAmountLamports);
                case 'roulette': return handleRouletteGame(userId, chatId, messageId, betAmountLamports, betChoice1, betChoice2);
                case 'war': return handleWarGame(userId, chatId, messageId, betAmountLamports);
                case 'crash': return handleCrashGame(userId, chatId, messageId, betAmountLamports);
                case 'blackjack': return handleBlackjackGame(userId, chatId, messageId, betAmountLamports, 'start_game');
                default:
                    console.error(`${logPrefix} Unknown gameKey in confirm_bet: ${gameKey}`);
                    return safeSendMessage(chatId, "Sorry, something went wrong with that game confirmation\\.", {parse_mode: 'MarkdownV2'});
            }
        }

        // Handle 'play_again' action
        else if (action === 'play_again') {
            const gameKey = params[0];
            const lastBetAmountLamportsStr = params[1];
            console.log(`${logPrefix} Play Again for ${gameKey}, last bet: ${lastBetAmountLamportsStr}`);

            if (GAME_CONFIG[gameKey] && lastBetAmountLamportsStr) {
                 // *** FIX: Play Again for BJ/Crash/Multi-step games should show selection again ***
                if (gameKey === 'coinflip' || gameKey === 'race' || gameKey === 'roulette') {
                     // For multi-step games, call proceedToGameStep to show the first choice again
                     let initialStepCallbackPrefix = '';
                    if (gameKey === 'coinflip') initialStepCallbackPrefix = 'coinflip_select_side';
                    else if (gameKey === 'race') initialStepCallbackPrefix = 'race_select_horse';
                    else if (gameKey === 'roulette') initialStepCallbackPrefix = 'roulette_select_bet_type';
                    // Use the existing messageId to edit the result message into the next step prompt
                    await proceedToGameStep(userId, chatId, messageId, gameKey, `${initialStepCallbackPrefix}:${lastBetAmountLamportsStr}`);
                } else if (gameKey === 'blackjack' || gameKey === 'crash') {
                     // These games should show bet selection again, passing the last bet amount
                     console.log(`${logPrefix} Play Again for ${gameKey} routing to showBetAmountButtons.`);
                     // Use the existing message context (originalMessage) to edit into the bet selection screen
                     await showBetAmountButtons(originalMessage, gameKey, lastBetAmountLamportsStr, userId);
                } else {
                    // Games like Slots, War can directly confirm the bet again
                    // Simulate a confirm_bet callback to re-trigger the game handler
                    const fakeCallbackData = `confirm_bet:${gameKey}:${lastBetAmountLamportsStr}`;
                    // Construct a minimal callback object to pass to the handler
                    // Ensure all necessary properties are present for handleCallbackQuery
                    const fakeCallbackQuery = {
                        id: 'fakecb-pa-' + Date.now(), // Unique ID
                        from: callbackQuery.from, // User info
                        message: { // Message context (use original message)
                            chat: callbackQuery.message.chat,
                            message_id: messageId,
                            date: Math.floor(Date.now() / 1000), // Add date field
                            text: callbackQuery.message.text || "" // Include original text if available
                        },
                        data: fakeCallbackData, // The action data
                        chat_instance: callbackQuery.chat_instance || String(chatId) // Add chat_instance
                    };
                    callbackQueue.add(() => handleCallbackQuery(fakeCallbackQuery)); // Add to queue
                }
            } else {
                bot.editMessageText("⚠️ Error processing 'Play Again' request due to invalid parameters\\.", { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2' });
            }
        }

        // Handle 'custom_amount_select' action
        else if (action === 'custom_amount_select') {
            const gameKey = params[0];
            if (GAME_CONFIG[gameKey]) { await handleCustomAmountSelection(userId, chatId, messageId, gameKey); } // Defined in Part 5b
            else { console.warn(`${logPrefix} Unknown game key for custom amount: ${gameKey}`); bot.editMessageText(`⚠️ Unknown game for custom amount: \`${escapeMarkdownV2(gameKey)}\``, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2' }); }
        }

        // Handle 'blackjack_action' (Hit/Stand)
        else if (action === 'blackjack_action') {
            const playerChoice = params[0]; const originalBetIdStr = params[1]; const originalBetId = parseInt(originalBetIdStr, 10);
            if (!isNaN(originalBetId) && (playerChoice === 'hit' || playerChoice === 'stand')) {
                const gameState = userStateCache.get(userId);
                // Verify state matches the action and bet ID
                if (gameState && gameState.action === 'awaiting_blackjack_action' && gameState.betId === originalBetId) {
                    console.log(`${logPrefix} Blackjack action: ${playerChoice} for Bet ID ${originalBetId}`);
                    // Call the main blackjack handler to process the action
                    return handleBlackjackGame(userId, chatId, messageId, BigInt(gameState.betAmountLamports), playerChoice, gameState);
                } else {
                    console.warn(`${logPrefix} Blackjack state mismatch or not found for action ${playerChoice}, Bet ID ${originalBetId}. State:`, gameState);
                    bot.editMessageText("⚠️ Blackjack game session expired or invalid\\. Please start a new game\\.", { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: '↩️ Back to Games', callback_data: 'menu:game_selection' }]] } });
                }
            } else { console.error(`${logPrefix} Invalid Bet ID or action received for blackjack_action: ${data}`); bot.editMessageText("⚠️ Error processing Blackjack action due to invalid data\\.", { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2' }); }
        }

        // Handle 'cash_out_crash' action
        else if (action === 'cash_out_crash') {
            const originalBetIdStr = params[0]; const originalBetId = parseInt(originalBetIdStr, 10); const gameState = userStateCache.get(userId);
            // Verify state matches action and bet ID
            if (!isNaN(originalBetId) && gameState && gameState.action === 'awaiting_crash_cashout' && gameState.betId === originalBetId) {
                clearUserState(userId); // Clear state immediately on successful cashout action
                const cashedOutAtMultiplier = parseFloat(gameState.currentMultiplier.toFixed(2)); const betAmountN = BigInt(gameState.betAmountLamports);
                console.log(`${logPrefix} User initiated cash out for Crash Bet ID ${originalBetId} at ${cashedOutAtMultiplier}x.`);
                let clientCashout = null;
                try {
                    clientCashout = await pool.connect(); await clientCashout.query('BEGIN');
                    // Calculate payout
                    const grossWinnings = BigInt(Math.floor(Number(betAmountN) * cashedOutAtMultiplier));
                    const profitBeforeEdge = grossWinnings - betAmountN;
                    const netProfit = profitBeforeEdge > 0n ? BigInt(Math.floor(Number(profitBeforeEdge) * (1 - GAME_CONFIG.crash.houseEdge))) : 0n;
                    const payoutAmountToCredit = betAmountN + netProfit; // Return stake + net profit
                    // Update balance and ledger
                    const balanceUpdateResult = await updateUserBalanceAndLedger(clientCashout, userId, payoutAmountToCredit, 'crash_cashout', { betId: originalBetId });
                    if (!balanceUpdateResult.success) { throw new Error(`Failed balance update (Cashout): ${escapeMarkdownV2(balanceUpdateResult.error || "DB Error")}`); }
                    const finalUserBalance = balanceUpdateResult.newBalance;
                    // Update bet status
                    await updateBetStatus(clientCashout, originalBetId, 'completed_win', payoutAmountToCredit);
                    await clientCashout.query('COMMIT');
                    // Send success message
                    const cashoutSuccessMsg = `💸 *Cashed Out at ${escapeMarkdownV2(cashedOutAtMultiplier.toFixed(2))}x\\!* 💸\nOriginal Bet: ${escapeMarkdownV2(formatSol(betAmountN))} SOL\nYou won: ${escapeMarkdownV2(formatSol(netProfit))} SOL\n\nNew Balance: ${escapeMarkdownV2(formatSol(finalUserBalance))} SOL`;
                    bot.editMessageText(cashoutSuccessMsg, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: '🔄 Play Again', callback_data: `play_again:crash:${betAmountN}` }, { text: '🎮 Games Menu', callback_data: 'menu:game_selection' }]] } }).catch(e => {});
                } catch (dbError) {
                    if (clientCashout) await clientCashout.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} Cashout DB Rollback failed:`, rbErr)); console.error(`${logPrefix} DB Error processing cashout for Bet ID ${originalBetId}:`, dbError);
                    bot.editMessageText(`⚠️ Database error processing cashout for Bet ID ${originalBetId}: ${escapeMarkdownV2(dbError.message)}\\. Please contact support if balance is incorrect\\.`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: `🔄 Play Crash Again`, callback_data: `play_again:crash:${gameState.betAmountLamports}` }, { text: '🎮 Games Menu', callback_data: 'menu:game_selection' }]] } }).catch(e => {});
                } finally { if (clientCashout) clientCashout.release(); }
            } else {
                // State mismatch or game already ended
                console.warn(`${logPrefix} Crash state mismatch or not found for cash_out_crash, Bet ID ${originalBetId}. Game might have already ended. State:`, gameState);
                // Inform user cashout was too late or game ended
                bot.editMessageText("⚠️ Too late to cash out or game already ended\\.", { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: `🔄 Play Crash Again`, callback_data: `play_again:crash:${gameState?.betAmountLamports || GAME_CONFIG.crash.minBetLamports}` }, { text: '🎮 Games Menu', callback_data: 'menu:game_selection' }]] } });
            }
        }

        // Handle 'quick_deposit' action
        else if (action === 'quick_deposit') { console.log(`${logPrefix} Handling 'quick_deposit' action.`); await handleDepositCommand(originalMessage, [], userId); } // handleDepositCommand defined in Part 5b

        // Handle withdrawal confirmation actions
        else if (action === 'confirm_withdrawal') { const recipientAddress = params[0]; const amountLamportsStr = params[1]; await handleWithdrawalConfirmation(userId, chatId, messageId, recipientAddress, amountLamportsStr, true); } // handleWithdrawalConfirmation defined in Part 5b
        else if (action === 'cancel_withdrawal') { console.log(`${logPrefix} Withdrawal cancelled by user.`); bot.editMessageText("🚫 Withdrawal cancelled\\.", { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: '↩️ Back to Wallet', callback_data: 'menu:wallet' }]] }}); }

        // Handle Help section navigation
        else if (action === 'show_help_section') { const section = params[0]; await handleHelpCommand(originalMessage, [section], userId); } // handleHelpCommand defined in Part 5b

        // Handle Leaderboard navigation
        else if (action === 'leaderboard_nav') { const type = params[0]; const page = parseInt(params[1] || '0', 10); await displayLeaderboard(chatId, messageId, userId, type, page, true); } // displayLeaderboard defined in Part 5b

        // Default case for unhandled actions
        else {
            console.warn(`${logPrefix} Potentially Unhandled callback query action: '${action}' with params: ${params.join(',')}. Data: '${data}'`);
            // Optionally inform the user or just ignore
            // safeSendMessage(chatId, `Action '${escapeMarkdownV2(action)}' not recognized.`, {parse_mode: 'MarkdownV2'});
        }

    } catch (error) {
        // Generic error handler for the entire callback processing
        console.error(`${logPrefix} Error processing callback query: ${error.message}\nStack: ${error.stack}`);
        await safeSendMessage(chatId, `⚠️ Error processing your request: ${escapeMarkdownV2(error.message)}\\. Please try again\\.`, { parse_mode: 'MarkdownV2' });
    }
}
// --- End Main Callback Query Handler ---


// --- Universal Game Step Processor (Intermediate Steps) ---
/**
 * Handles intermediate steps in games like Coinflip (choose side), Race (choose horse), Roulette (choose bet type/category).
 * Called by handleCallbackQuery for specific actions (e.g., coinflip_select_side:AMOUNT).
 * @param {string} userId
 * @param {string} chatId
 * @param {number | null} messageId The ID of the message to potentially edit. Can be null if sending new msg.
 * @param {string} gameKey 'coinflip', 'race', or 'roulette'.
 * @param {string} callbackData The full callback data string triggering this step.
 */
async function proceedToGameStep(userId, chatId, messageId, gameKey, callbackData) {
    // *** CORRECTED ReferenceError fix & '>' escape fix ***
    const gameConfig = GAME_CONFIG[gameKey]; // GAME_CONFIG from Part 1
    const logPrefix = `[ProceedToStep User ${userId} Game ${gameKey} CB ${callbackData}]`;
    console.log(`${logPrefix} Proceeding intermediate step.`);

    if (!gameConfig || !callbackData) { console.error(`${logPrefix} Invalid gameKey or missing callbackData.`); safeSendMessage(chatId, "An internal error occurred with game selection\\. Please start again\\.", { parse_mode: 'MarkdownV2'}); return; }

    const parts = callbackData.split(':');
    const actionPrefix = parts[0]; // e.g., 'coinflip_select_side', 'roulette_select_bet_type', 'roulette_bet_type_category'
    const params = parts.slice(1); // *** Define params based on the split ***

    // Bet amount is usually the last part in the original callbackData structure
    // e.g., roulette_bet_type_category:color:AMOUNT -> params = ['color', 'AMOUNT'] -> last part is amount
    // e.g., roulette_select_bet_type:straight:AMOUNT -> params = ['straight', 'AMOUNT'] -> last part is amount
    // e.g., coinflip_select_side:AMOUNT -> params = ['AMOUNT'] -> last part is amount
    let betAmountLamportsStr = params[params.length - 1];
    let currentBetAmount;
    try { currentBetAmount = BigInt(betAmountLamportsStr); if (currentBetAmount <= 0n) throw new Error("Invalid amount"); }
    catch { console.error(`${logPrefix} Invalid bet amount in callback: ${betAmountLamportsStr}`); safeSendMessage(chatId, "Invalid bet amount specified for game step\\.", {parse_mode: 'MarkdownV2'}); return; }

    const amountSOLFormatted = formatSol(currentBetAmount); // formatSol from Part 3
    let messageText = `*${escapeMarkdownV2(gameConfig.name)}* \\- Bet: ${escapeMarkdownV2(amountSOLFormatted)} SOL\n`; // Base message
    let inlineKeyboard = [];

    try {
        if (gameKey === 'coinflip' && actionPrefix === 'coinflip_select_side') {
            messageText += "\nChoose your side:";
            inlineKeyboard = [
                [{ text: '🪙 Heads', callback_data: `confirm_bet:coinflip:${betAmountLamportsStr}:heads` }, { text: '🪙 Tails', callback_data: `confirm_bet:coinflip:${betAmountLamportsStr}:tails` }],
                [{ text: '✏️ Change Amount', callback_data: `select_game:${gameKey}` }, { text: '❌ Cancel', callback_data: 'menu:game_selection' }]
            ];
        }
        else if (gameKey === 'race' && actionPrefix === 'race_select_horse') {
            messageText += "\nChoose your horse:";
            const horseButtons = [];
            RACE_HORSES.forEach((horse, index) => { // RACE_HORSES from Part 1
                horseButtons.push({ text: `${horse.emoji} ${escapeMarkdownV2(horse.name)} (${escapeMarkdownV2(horse.payoutMultiplier)}x)`, callback_data: `confirm_bet:race:${betAmountLamportsStr}:${index + 1}` });
            });
            for(let i = 0; i < horseButtons.length; i += 2) { inlineKeyboard.push(horseButtons.slice(i, i + 2)); }
            inlineKeyboard.push([{ text: '✏️ Change Amount', callback_data: `select_game:${gameKey}` }, { text: '❌ Cancel', callback_data: 'menu:game_selection' }]);
        }
        // --- FIX #3: Roulette Step Logic (with corrected params access & '>' escape) ---
        else if (gameKey === 'roulette') {
            if (actionPrefix === 'roulette_select_bet_type') {
                // This is the first step after amount selection (or re-selection)
              // params[0] would be 'straight' if passed, or undefined otherwise
                const betTypeParam = params.length > 1 ? params[0] : null; // Amount is last, type is before amount if passed
                if (betTypeParam === 'straight') {
                    // Call the setup function to prompt for number input
                    await handleRouletteStraightBetSetup(userId, chatId, messageId, gameKey, betAmountLamportsStr); // Defined in Part 5b
                    return; // Exit early as handleRouletteStraightBetSetup handles the message edit/send
                } else {
                    // Show the bet type categories
                    messageText += "\nSelect your bet type category or Straight Up:";
                    inlineKeyboard = [
                        [{ text: "🔴⚫️ Color", callback_data: `roulette_bet_type_category:color:${betAmountLamportsStr}` }],
                        [{ text: "🔢 Even / Odd", callback_data: `roulette_bet_type_category:parity:${betAmountLamportsStr}` }],
                        [{ text: "📉📈 Range (1\\-18 / 19\\-36)", callback_data: `roulette_bet_type_category:range:${betAmountLamportsStr}` }],
                        [{ text: "🎯 Straight Up \\(\\#\\)", callback_data: `roulette_select_bet_type:straight:${betAmountLamportsStr}` }]
                    ];
                    inlineKeyboard.push([{ text: '✏️ Change Amount', callback_data: `select_game:${gameKey}` }, { text: '❌ Cancel', callback_data: 'menu:game_selection' }]);
                }
            }
            else if (actionPrefix === 'roulette_bet_type_category') {
                // This is the step after selecting a category (Color, Parity, Range)
              // params[0] is the category (e.g., 'color'), params[1] is the amount string
                const category = params[0]; // Get category from the correctly defined params array
                if (!category) { throw new Error("Invalid callback data: Missing category for roulette_bet_type_category"); }
              // *** CORRECTED '>' escaping in message text ***
                messageText = `*${escapeMarkdownV2(gameConfig.name)}* \\- Bet: ${escapeMarkdownV2(amountSOLFormatted)} SOL \\> ${escapeMarkdownV2(category)}\nSelect your specific bet:`; // Escape '>'
                switch(category) {
                    case 'color': inlineKeyboard.push([{ text: '🔴 Red', callback_data: `confirm_bet:roulette:${betAmountLamportsStr}:red` }, { text: '⚫️ Black', callback_data: `confirm_bet:roulette:${betAmountLamportsStr}:black` }]); break;
                    case 'parity': inlineKeyboard.push([{ text: '🔢 Even', callback_data: `confirm_bet:roulette:${betAmountLamportsStr}:even` }, { text: '🔢 Odd', callback_data: `confirm_bet:roulette:${betAmountLamportsStr}:odd` }]); break;
                    case 'range': inlineKeyboard.push([{ text: '📉 Low (1\\-18)', callback_data: `confirm_bet:roulette:${betAmountLamportsStr}:low` }, { text: '📈 High (19\\-36)', callback_data: `confirm_bet:roulette:${betAmountLamportsStr}:high` }]); break;
                    default: throw new Error(`Invalid Roulette category received: ${category}`);
                }
                // Add navigation buttons
                inlineKeyboard.push([{ text: '↩️ Back to Bet Types', callback_data: `roulette_select_bet_type:${betAmountLamportsStr}` }]);
                inlineKeyboard.push([{ text: '❌ Cancel Bet', callback_data: 'menu:game_selection' }]);
            }
            else {
                // Should not happen if routing in handleCallbackQuery is correct
                console.warn(`${logPrefix} proceedToGameStep called for Roulette with unexpected action prefix: ${actionPrefix}`);
                safeSendMessage(chatId, "An unexpected error occurred in the Roulette game flow\\. Please start again\\.", {parse_mode: 'MarkdownV2'}); return;
            }
        }
        // --- End FIX #3 ---
        else {
            // This case should ideally not be reached if all intermediate steps are handled above
            console.warn(`${logPrefix} proceedToGameStep called for unhandled action prefix: ${actionPrefix} in callback: ${callbackData}`);
            safeSendMessage(chatId, "An unexpected error occurred in game flow\\. Please start again\\.", {parse_mode: 'MarkdownV2'}); return;
        }

        // Send or Edit the message with the generated text and keyboard
        const options = { chat_id: chatId, reply_markup: { inline_keyboard: inlineKeyboard }, parse_mode: 'MarkdownV2' };
        if (messageId) {
            await bot.editMessageText(messageText, { ...options, message_id: messageId })
                .catch(e => {
                    if (!e.message.includes("message is not modified")) {
                        console.error(`${logPrefix} Error editing message in proceedToGameStep (${messageId}):`, e.message);
                        // Log specific parse errors
                        if (e.message.toLowerCase().includes("can't parse entities")) {
                            console.error(`Failed Text (${logPrefix}): ${messageText}`);
                        }
                        // If edit fails, send as a new message
                        safeSendMessage(chatId, messageText, options);
                    }
                });
        } else {
            // Send as a new message if no messageId was provided (e.g., after custom input)
            await safeSendMessage(chatId, messageText, options);
        }

    } catch (error) {
        console.error(`${logPrefix} Error in proceedToGameStep:`, error);
        await safeSendMessage(chatId, "An unexpected error occurred processing your choice\\. Please try again\\.", { parse_mode: 'MarkdownV2'});
        clearUserState(userId); // Clear state on error
    }
}
// --- End Universal Game Step Processor ---


// --- Core Bet Placement Logic ---
/**
 * Handles the core logic of placing a bet. Acquires DB client and manages transaction.
 * IMPORTANT: Must be called within a transaction managed by the game handler.
 * @param {import('pg').PoolClient} client The active database client from the calling handler.
 * @param {string} userId
 * @param {string} chatId
 * @param {string} gameKey
 * @param {object} betDetails For `bets` table JSONB column.
 * @param {bigint} betAmountLamports
 * @returns {Promise<{success: boolean, betId?: number, error?: string, insufficientBalance?: boolean, newBalance?: bigint, currentBalance?: bigint}>} Includes currentBalance if insufficient.
 */
async function placeBet(client, userId, chatId, gameKey, betDetails, betAmountLamports) {
    const stringUserId = String(userId);
    const logPrefix = `[PlaceBet User ${stringUserId} Game ${gameKey}]`;

    try {
        // Lock balance row first
        const balanceCheck = await queryDatabase('SELECT balance_lamports FROM user_balances WHERE user_id = $1 FOR UPDATE', [stringUserId], client); // queryDatabase from Part 2
        if (balanceCheck.rowCount === 0) {
            // This should ideally not happen if ensureUserExists was called, but handle defensively
            throw new Error(`User balance record not found for user ${stringUserId} during bet placement.`);
        }
        const currentBalance = BigInt(balanceCheck.rows[0].balance_lamports);

        // Check balance AFTER getting the lock
        if (currentBalance < betAmountLamports) {
            console.warn(`${logPrefix} Insufficient balance during placeBet. Current: ${currentBalance}, Needed: ${betAmountLamports}`);
            return { success: false, error: 'Insufficient balance', insufficientBalance: true, currentBalance: currentBalance };
        }

        // Deduct balance using updateUserBalanceAndLedger
        const balanceUpdateResult = await updateUserBalanceAndLedger( // from Part 2
            client,
            stringUserId,
            -betAmountLamports, // Negative amount for deduction
            `bet_placed:${gameKey}`,
            {}, // relatedIds (none here)
            `Placed bet on ${GAME_CONFIG[gameKey]?.name || gameKey}` // notes
        );
        if (!balanceUpdateResult.success) {
            // If balance update failed (e.g., constraint violation despite initial check - highly unlikely with FOR UPDATE, but possible)
            return { success: false, error: balanceUpdateResult.error || 'Failed to update balance.', insufficientBalance: balanceUpdateResult.error === 'Insufficient balance', currentBalance: currentBalance };
        }
        const newBalanceAfterBet = balanceUpdateResult.newBalance;

        // Create the bet record
        const betRecordResult = await createBetRecord(client, stringUserId, String(chatId), gameKey, betDetails, betAmountLamports); // from Part 2
        if (!betRecordResult.success || !betRecordResult.betId) {
            // Rollback should be handled by the caller if this throws or returns false
            return { success: false, error: betRecordResult.error || 'Failed to create bet record.' };
        }
        const betId = betRecordResult.betId;

        // Update wager stats and referral checks (use the same client)
        await updateUserWagerStats(stringUserId, betAmountLamports, client); // from Part 2
        await _handleReferralChecks(stringUserId, betId, betAmountLamports, client); // from Part 1
        await updateUserLastBetAmount(stringUserId, gameKey, betAmountLamports, client); // from Part 2

        // Update in-memory cache (optional but good for responsiveness)
        const userBetsMemoryCache = userLastBetAmounts.get(stringUserId) || new Map(); // userLastBetAmounts from Part 1 state
        userBetsMemoryCache.set(gameKey, betAmountLamports);
        userLastBetAmounts.set(stringUserId, userBetsMemoryCache);

        console.log(`${logPrefix} Bet ID ${betId} successfully recorded for ${formatSol(betAmountLamports)} SOL. New balance (within Tx): ${formatSol(newBalanceAfterBet)} SOL.`);
        return { success: true, betId, newBalance: newBalanceAfterBet };

    } catch (error) {
        console.error(`${logPrefix} Error during placeBet logic (caller should rollback):`, error);
        let returnError = `Error placing bet: ${escapeMarkdownV2(error.message)}`;
        let insufficientBal = false;
        // Check for specific error messages or codes indicating insufficient balance
        if (error.message.toLowerCase().includes('insufficient balance') || error.constraint === 'user_balances_balance_lamports_check') {
            returnError = 'Insufficient balance.';
            insufficientBal = true;
        }
        // Return error object, let caller handle rollback
        return { success: false, error: returnError, insufficientBalance: insufficientBal};
        // Or re-throw to ensure rollback: throw error; (Choose based on desired error handling flow in callers)
    }
}
// --- End Core Bet Placement Logic ---

// Section 2a: Coinflip, Race, Slots Game Handlers (Win Payout Accounting Fix Applied)

async function handleCoinflipGame(userId, chatId, messageId, betAmountLamports, chosenSide) {
    const gameKey = 'coinflip'; const gameConfig = GAME_CONFIG[gameKey]; const logPrefix = `[CoinflipGame User ${userId} Bet ${betAmountLamports}]`; console.log(`${logPrefix} Handling for side: ${chosenSide}`);
    let client = null; let finalUserBalance;
    try {
        client = await pool.connect(); await client.query('BEGIN');
        const betPlacementResult = await placeBet(client, userId, chatId, gameKey, { side: chosenSide }, betAmountLamports);
        if (!betPlacementResult.success) { await client.query('ROLLBACK'); const errorMsg = betPlacementResult.error === 'Insufficient balance' ? `⚠️ Insufficient balance for a ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL bet\\. Your balance is ${escapeMarkdownV2(formatSol(betPlacementResult.currentBalance || 0n))} SOL\\.` : `⚠️ Error placing bet: ${escapeMarkdownV2(betPlacementResult.error || 'Unknown error')}\\.`; if(client) client.release(); return bot.editMessageText(errorMsg, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: '↩️ Back to Games', callback_data: 'menu:game_selection' }]] } }); }
        const { betId, newBalance: balanceAfterBet } = betPlacementResult; finalUserBalance = balanceAfterBet;
        const { outcome } = playCoinflip(chosenSide); // from Part 4
      const win = outcome === chosenSide;
        let profitLamportsOutcome = -betAmountLamports; let payoutAmountForDB = 0n;
        if (win) {
            const profitOnWin = BigInt(Math.floor(Number(betAmountLamports) * (1 - gameConfig.houseEdge))); profitLamportsOutcome = profitOnWin; payoutAmountForDB = betAmountLamports + profitOnWin;
            const balanceUpdateResult = await updateUserBalanceAndLedger(client, userId, payoutAmountForDB, 'coinflip_win', { betId }); // FIX #0: Use payoutAmountForDB
            if (!balanceUpdateResult.success) { await client.query('ROLLBACK'); console.error(`${logPrefix} Failed balance update.`); const errorMsg = `⚠️ Critical error processing game result: ${escapeMarkdownV2(balanceUpdateResult.error || "DB Error")}\\. Bet recorded but result uncertain\\. Contact support with Bet ID: ${betId}`; if(client) client.release(); return bot.editMessageText(errorMsg, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: '↩️ Back to Games', callback_data: 'menu:game_selection' }]] } }); }
            finalUserBalance = balanceUpdateResult.newBalance;
        } else { profitLamportsOutcome = -betAmountLamports; payoutAmountForDB = 0n; console.log(`${logPrefix} Coinflip loss for Bet ID ${betId}. Balance remains ${formatSol(finalUserBalance)} SOL.`); }
        await updateBetStatus(client, betId, win ? 'completed_win' : 'completed_loss', payoutAmountForDB); // from Part 2
      await client.query('COMMIT');
        let resultMsg = `🪙 *Coinflip Result*\n\nBet: ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL\nYour Choice: ${escapeMarkdownV2(chosenSide.toUpperCase())}\nOutcome: ${escapeMarkdownV2(outcome.toUpperCase())}\n\n`; resultMsg += win ? `🎉 You won ${escapeMarkdownV2(formatSol(profitLamportsOutcome))} SOL\\!` : `😢 You lost ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL\\.`; resultMsg += `\n\nNew Balance: ${escapeMarkdownV2(formatSol(finalUserBalance))} SOL`;
        const keyboard = { inline_keyboard: [ [{ text: '🔄 Play Again', callback_data: `play_again:${gameKey}:${betAmountLamports}` }, { text: '🎮 Games Menu', callback_data: 'menu:game_selection' }] ] }; bot.editMessageText(resultMsg, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: keyboard });
    } catch (error) { if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} Rollback failed:`, rbErr)); console.error(`${logPrefix} Error in coinflip game:`, error); bot.editMessageText(`⚠️ An unexpected error occurred during Coinflip: ${escapeMarkdownV2(error.message)}\\. Please try again later\\.`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: '↩️ Back to Games', callback_data: 'menu:game_selection' }]] } });
    } finally { if (client) client.release(); }
}


async function handleRaceGame(userId, chatId, messageId, betAmountLamports, chosenHorseNumber) {
    const gameKey = 'race'; const gameConfig = GAME_CONFIG[gameKey]; const logPrefix = `[RaceGame User ${userId} Bet ${betAmountLamports} Horse ${chosenHorseNumber}]`; const chosenHorseConfig = RACE_HORSES[chosenHorseNumber - 1]; // RACE_HORSES from Part 1
    if (!chosenHorseConfig) { console.error(`${logPrefix} Invalid horse number: ${chosenHorseNumber}`); return bot.editMessageText(`⚠️ Invalid horse selected: ${escapeMarkdownV2(chosenHorseNumber)}\\.`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: '↩️ Back to Games', callback_data: 'menu:game_selection' }]] } }); }
    console.log(`${logPrefix} Handling for horse: ${chosenHorseConfig.name}`);
    let client = null; let finalUserBalance;
    try {
        client = await pool.connect(); await client.query('BEGIN');
        const betPlacementResult = await placeBet(client, userId, chatId, gameKey, { horse: chosenHorseConfig.name, horseNum: chosenHorseNumber }, betAmountLamports);
        if (!betPlacementResult.success) { await client.query('ROLLBACK'); const errorMsg = betPlacementResult.error === 'Insufficient balance' ? `⚠️ Insufficient balance for a ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL bet\\. Your balance is ${escapeMarkdownV2(formatSol(betPlacementResult.currentBalance || 0n))} SOL\\.` : `⚠️ Error placing bet: ${escapeMarkdownV2(betPlacementResult.error || 'Unknown error')}\\.`; if(client) client.release(); return bot.editMessageText(errorMsg, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: '↩️ Back to Games', callback_data: 'menu:game_selection' }]] } }); }
        const { betId, newBalance: balanceAfterBet } = betPlacementResult; finalUserBalance = balanceAfterBet;
        const { winningLane } = simulateRace(RACE_HORSES.length); // from Part 4
      const win = winningLane === chosenHorseNumber; const winningHorseConfig = RACE_HORSES[winningLane - 1];
        let profitLamportsOutcome = -betAmountLamports; let payoutAmountForDB = 0n;
        if (win) {
            const basePayoutMultiplierForHorse = chosenHorseConfig.payoutMultiplier; const profitBeforeEdge = BigInt(Math.floor(Number(betAmountLamports) * (basePayoutMultiplierForHorse - 1))); const netProfit = profitBeforeEdge > 0n ? BigInt(Math.floor(Number(profitBeforeEdge) * (1 - gameConfig.houseEdge))) : 0n; profitLamportsOutcome = netProfit; payoutAmountForDB = betAmountLamports + netProfit;
            const balanceUpdateResult = await updateUserBalanceAndLedger(client, userId, payoutAmountForDB, 'race_win', { betId }); // FIX #0: Use payoutAmountForDB
            if (!balanceUpdateResult.success) { await client.query('ROLLBACK'); const errorMsg = `⚠️ Critical error processing game result: ${escapeMarkdownV2(balanceUpdateResult.error || "DB Error")}\\. Bet recorded but result uncertain\\. Contact support with Bet ID: ${betId}`; if(client) client.release(); return bot.editMessageText(errorMsg, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: '↩️ Back to Games', callback_data: 'menu:game_selection' }]] } }); }
            finalUserBalance = balanceUpdateResult.newBalance;
        } else { profitLamportsOutcome = -betAmountLamports; payoutAmountForDB = 0n; console.log(`${logPrefix} Race loss for Bet ID ${betId}. Balance remains ${formatSol(finalUserBalance)} SOL.`); }
        await updateBetStatus(client, betId, win ? 'completed_win' : 'completed_loss', payoutAmountForDB); // from Part 2
      await client.query('COMMIT');
        // Animation... (omitted for brevity but is unchanged in this fix)
        let animationText = `🏁 *Horse Race Starting\\!* 🏇\n\nYour Pick: ${chosenHorseConfig.emoji} ${escapeMarkdownV2(chosenHorseConfig.name)}\nBet: ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL\n\nHorses at the gate:\n`; RACE_HORSES.forEach(h => animationText += `${h.emoji} ${escapeMarkdownV2(h.name)}\n`); await bot.editMessageText(animationText, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2' }).catch(e => {}); await sleep(1500); animationText = `🏁 *And they're off\\!* 💨\n\n`; let progress = RACE_HORSES.map(h => ({ ...h, p: '' })); let lastAnimationText = ""; for (let i = 0; i < 5; i++) { progress.forEach(h => { h.p += '─'; }); progress[winningLane - 1].p += '─'; let frameText = animationText; progress.forEach(h => frameText += `${h.emoji} ${h.p}\n`); if (frameText !== lastAnimationText) { await bot.editMessageText(frameText, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2' }).catch(e => {}); lastAnimationText = frameText; } await sleep(800); } await sleep(1000);
        // Final Result
        let resultMsg = `🏆 *Race Result* 🏆\n\nWinning Horse: ${winningHorseConfig.emoji} *${escapeMarkdownV2(winningHorseConfig.name)}*\\!\nYour Pick: ${chosenHorseConfig.emoji} ${escapeMarkdownV2(chosenHorseConfig.name)}\nBet: ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL\n\n`; resultMsg += win ? `🎉 You won ${escapeMarkdownV2(formatSol(profitLamportsOutcome))} SOL\\! (Multiplier: ${escapeMarkdownV2(chosenHorseConfig.payoutMultiplier)}x base)` : `😢 You lost ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL\\.`; resultMsg += `\n\nNew Balance: ${escapeMarkdownV2(formatSol(finalUserBalance))} SOL`;
        const keyboard = { inline_keyboard: [ [{ text: '🔄 Play Again', callback_data: `play_again:${gameKey}:${betAmountLamports}` }, { text: '🎮 Games Menu', callback_data: 'menu:game_selection' }] ] }; bot.editMessageText(resultMsg, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: keyboard });
    } catch (error) { if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} Rollback failed:`, rbErr)); console.error(`${logPrefix} Error in race game:`, error); bot.editMessageText(`⚠️ An unexpected error occurred during Horse Race: ${escapeMarkdownV2(error.message)}\\. Please try again later\\.`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: '↩️ Back to Games', callback_data: 'menu:game_selection' }]] } });
    } finally { if (client) client.release(); }
}

async function handleSlotsGame(userId, chatId, messageId, betAmountLamports) {
    const gameKey = 'slots'; const gameConfig = GAME_CONFIG[gameKey]; const logPrefix = `[SlotsGame User ${userId} Bet ${betAmountLamports}]`; console.log(`${logPrefix} Handling slots spin.`);
    let client = null; let betId; let balanceAfterBet; let finalUserBalance; let lastMessageText = "";
    try {
        client = await pool.connect(); await client.query('BEGIN');
        const { symbols: finalSymbols, payoutMultiplier: baseMultiplier, isJackpotWin } = simulateSlots(); // from Part 4
        const betPlacementResult = await placeBet(client, userId, chatId, gameKey, {result: finalSymbols.join('')}, betAmountLamports);
        if (!betPlacementResult.success) { await client.query('ROLLBACK'); const errorMsg = betPlacementResult.error === 'Insufficient balance' ? `⚠️ Insufficient balance for a ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL bet\\. Your balance is ${escapeMarkdownV2(formatSol(betPlacementResult.currentBalance || 0n))} SOL\\.` : `⚠️ Error placing bet: ${escapeMarkdownV2(betPlacementResult.error || 'Unknown error')}\\.`; if(client) client.release(); return bot.editMessageText(errorMsg, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: '↩️ Back to Games', callback_data: 'menu:game_selection' }]] } }); }
        betId = betPlacementResult.betId; balanceAfterBet = betPlacementResult.newBalance; finalUserBalance = balanceAfterBet;
        // Ensure jackpot record exists and contribute to it
        await ensureJackpotExists(gameKey, gameConfig.jackpotSeedLamports || 0n, client); // from Part 2
        let jackpotContribution = 0n;
        if (gameConfig.jackpotContributionPercent && gameConfig.jackpotContributionPercent > 0 && betAmountLamports > 0n) {
            jackpotContribution = BigInt(Math.floor(Number(betAmountLamports) * gameConfig.jackpotContributionPercent));
            if (jackpotContribution > 0n) {
                const incremented = await incrementJackpotAmount(client, gameKey, jackpotContribution); // from Part 2
                if (!incremented) console.warn(`${logPrefix} Failed jackpot increment.`);
                else console.log(`${logPrefix} Contributed ${formatSol(jackpotContribution)} SOL to jackpot.`);
            }
        }
        await client.query('COMMIT'); // Commit bet placement and jackpot contribution
        if(client) client.release(); client = null; // Release client after initial commit

        // Animation... (omitted for brevity but is unchanged in this fix)
        const slotEmojis = { 'C': '🍒', 'L': '🍋', 'O': '🍊', 'B': '🔔', '7': '❼', 'J': '💎' }; const animationFrames = 10; let currentFrameText = ""; const initialSpinText = `🎰 *Slots Spinning\\!* 🎰\n\nBet: ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL\n\n\\| 🎰 \\| 🎰 \\| 🎰 \\|\n\nSpinning\\.\\.\\.`; await bot.editMessageText(initialSpinText, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2' }).catch(e => {}); lastMessageText = initialSpinText; await sleep(500); for (let i = 0; i < animationFrames + finalSymbols.length; i++) { let currentDisplay = ['🎰', '🎰', '🎰']; let revealIndex = Math.max(-1, i - animationFrames); for(let k=0; k < finalSymbols.length; k++){ if(k <= revealIndex) { currentDisplay[k] = slotEmojis[finalSymbols[k]] || '❓'; } else { currentDisplay[k] = slotEmojis[Object.keys(slotEmojis)[Math.floor(Math.random() * Object.keys(slotEmojis).length)]]; } } currentFrameText = `🎰 *Slots Spinning\\!* 🎰\n\nBet: ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL\n\n\\| ${currentDisplay[0]} \\| ${currentDisplay[1]} \\| ${currentDisplay[2]} \\|\n\nSpinning\\.\\.\\.`; if (currentFrameText !== lastMessageText) { await bot.editMessageText(currentFrameText, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2' }).catch(e => { if (!e.message.includes("message is not modified")) console.warn(`${logPrefix} Slots animation edit error: ${e.message}.`); }); lastMessageText = currentFrameText; } await sleep(i < animationFrames ? 200 : 600); }

        // Process Result (needs new DB transaction)
        const displaySymbols = finalSymbols.map(s => slotEmojis[s] || '❓'); let profitLamportsOutcome = -betAmountLamports; let finalPayoutForDB = 0n; let winMessage = ''; let dbBetStatus = 'completed_loss';
        client = await pool.connect(); await client.query('BEGIN'); // Start new transaction for result processing
        try {
            if (isJackpotWin) {
                dbBetStatus = 'completed_win';
                const currentJackpot = await getJackpotAmount(gameKey, client); // from Part 2, use client
                let jackpotPayoutAmount = currentJackpot;
                if (currentJackpot <= 0n) {
                    // Fallback payout if jackpot somehow became zero or negative
                    const fixedMultiplier = 200; // Example fallback multiplier
                    const profitBeforeEdge = BigInt(Math.floor(Number(betAmountLamports) * (fixedMultiplier - 1)));
                    const netProfit = BigInt(Math.floor(Number(profitBeforeEdge) * (1 - gameConfig.houseEdge)));
                    profitLamportsOutcome = netProfit; // Payout is net profit
                    finalPayoutForDB = betAmountLamports + netProfit; // Return stake + net profit
                    winMessage = `💥 Triple Diamonds\\! You won ${escapeMarkdownV2(formatSol(profitLamportsOutcome))} SOL\\! \\(Jackpot was empty\\)`; // Escaped () !
                } else {
                    // Award the actual jackpot amount
                    profitLamportsOutcome = jackpotPayoutAmount; // User profit IS the jackpot amount here
                    finalPayoutForDB = betAmountLamports + jackpotPayoutAmount; // Return stake + jackpot amount
                    winMessage = `💎💎💎 JACKPOT\\! 🎉 You won ${escapeMarkdownV2(formatSol(jackpotPayoutAmount))} SOL\\!`; // Escaped !
                    // Reset jackpot to seed amount within the transaction
                    await updateJackpotAmount(client, gameKey, gameConfig.jackpotSeedLamports); // from Part 2
                }
                // Notify admin about the jackpot win
                if (jackpotPayoutAmount > 0n && typeof notifyAdmin === "function") { // notifyAdmin from Part 3
                    await notifyAdmin(`🎉 User ${escapeMarkdownV2(userId)} HIT THE SLOTS JACKPOT for ${escapeMarkdownV2(formatSol(jackpotPayoutAmount))} SOL\\! Bet ID: ${betId}`); // Escaped !
                }
                // Update balance with the total payout (stake + jackpot/fallback win)
                const balanceUpdateResult = await updateUserBalanceAndLedger(client, userId, finalPayoutForDB, 'slots_jackpot', { betId });
                if (!balanceUpdateResult.success) { throw new Error(`Failed balance update (Jackpot): ${balanceUpdateResult.error}`); }
                finalUserBalance = balanceUpdateResult.newBalance;
            } else if (baseMultiplier > 0) {
                // Regular win based on symbol match multiplier
                dbBetStatus = 'completed_win';
                const profitBeforeEdge = BigInt(Math.floor(Number(betAmountLamports) * (baseMultiplier - 1)));
                const netProfit = profitBeforeEdge > 0n ? BigInt(Math.floor(Number(profitBeforeEdge) * (1 - gameConfig.houseEdge))) : 0n;
                profitLamportsOutcome = netProfit; // Profit for message
                finalPayoutForDB = betAmountLamports + netProfit; // Return stake + net profit
                winMessage = `🎉 You matched\\! Won ${escapeMarkdownV2(formatSol(profitLamportsOutcome))} SOL \\(Multiplier: ${escapeMarkdownV2(baseMultiplier)}x base\\)\\!`; // Escaped ! ()
                // Update balance
                const balanceUpdateResult = await updateUserBalanceAndLedger(client, userId, finalPayoutForDB, 'slots_win', { betId });
                if (!balanceUpdateResult.success) { throw new Error(`Failed balance update (Slots Win): ${balanceUpdateResult.error}`); }
                finalUserBalance = balanceUpdateResult.newBalance;
            } else {
                // Loss
                dbBetStatus = 'completed_loss';
                profitLamportsOutcome = -betAmountLamports;
                finalPayoutForDB = 0n; // No payout for loss status
                winMessage = `😢 No win this time\\. Lost ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL\\.`; // Escaped .
                console.log(`${logPrefix} Slots loss for Bet ID ${betId}. Balance remains ${formatSol(finalUserBalance)} SOL.`); // Balance doesn't change further on loss
            }
            await updateBetStatus(client, betId, dbBetStatus, finalPayoutForDB); // Update bet status with correct payout amount (0 for loss)
            await client.query('COMMIT'); // Commit balance update (if any) and bet status update

            // Fetch jackpot amount *after* potential reset
            const jackpotAfterSpin = await getJackpotAmount(gameKey); // Use default pool connection for read after commit
            let resultMsg = `🎰 *Slots Result* 🎰\n\nBet: ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL\nResult: \\| ${displaySymbols[0]} \\| ${displaySymbols[1]} \\| ${displaySymbols[2]} \\|\n\n${winMessage}`;
            resultMsg += `\n\nNew Balance: ${escapeMarkdownV2(formatSol(finalUserBalance))} SOL`;
            resultMsg += `\n💎 Next Jackpot: ${escapeMarkdownV2(formatSol(jackpotAfterSpin))} SOL`;
            const keyboard = { inline_keyboard: [ [{ text: '🔄 Spin Again', callback_data: `play_again:${gameKey}:${betAmountLamports}` }, { text: '🎮 Games Menu', callback_data: 'menu:game_selection' }] ] };
            bot.editMessageText(resultMsg, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: keyboard });

        } catch (dbError) {
            // Rollback the result processing transaction on error
            if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} Result DB Rollback failed:`, rbErr));
            console.error(`${logPrefix} DB Error during game end processing: ${dbError.message}`);
            throw dbError; // Re-throw to outer catch block
        } finally {
            if (client) client.release(); client = null; // Release client used for result processing
        }
    } catch (error) {
        // Catch errors from initial setup or re-thrown from result processing
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} Outer Catch Rollback failed:`, rbErr)); // Ensure rollback if initial TX failed
        console.error(`${logPrefix} Error in slots game:`, error);
        bot.editMessageText(`⚠️ An unexpected error occurred during Slots: ${escapeMarkdownV2(error.message)}\\. Please try again later\\.`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: '↩️ Back to Games', callback_data: 'menu:game_selection' }]] } });
    } finally {
        // Ensure client is released if error occurred before explicit release
        if (client && !client.isReleased) client.release();
    }
}

// --- End of Part 5a Section 1 ---
// index.js - Part 5a: Telegram Message/Callback Handlers & Game Result Processing - Section 2 CORRECTED
// --- VERSION: 3.2.1v --- (Applying Fixes: FINAL ROULETTE FIXES - Escaping '()!.' in handleRouletteGame results message)

// (Continuing from Part 5a, Section 1)
// ... (Assume functions like placeBet, simulateRouletteSpin, getRoulettePayoutMultiplier, updateUserBalanceAndLedger, updateBetStatus etc. are available) ...

// Section 2b: Roulette, War, Crash, Blackjack Game Handlers (Modified)

async function handleRouletteGame(userId, chatId, messageId, betAmountLamports, betType, betValue) {
    const gameKey = 'roulette';
    const gameConfig = GAME_CONFIG[gameKey]; // GAME_CONFIG from Part 1
    const logPrefix = `[RouletteGame User ${userId} Bet ${betAmountLamports} Type ${betType} Val ${betValue}]`;
    console.log(`${logPrefix} Handling roulette spin.`);

    let client = null;
    let finalUserBalance; // To store the correct final balance

    try {
        client = await pool.connect(); // pool from Part 1
        await client.query('BEGIN');

        // Define bet details based on type
        let betDetails = { type: betType };
        if (betType === 'straight') {
            betDetails.value = betValue; // betValue here is the chosen number string
        } // Other types don't need a specific value stored beyond the type itself

        const betPlacementResult = await placeBet(client, userId, chatId, gameKey, betDetails, betAmountLamports); // placeBet from Part 5a-1
        if (!betPlacementResult.success) {
            await client.query('ROLLBACK');
            const errorMsg = betPlacementResult.error === 'Insufficient balance'
                ? `⚠️ Insufficient balance for a ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL bet\\. Your balance is ${escapeMarkdownV2(formatSol(betPlacementResult.currentBalance || 0n))} SOL\\.` // Escaped . formatSol from Part 3
                : `⚠️ Error placing bet: ${escapeMarkdownV2(betPlacementResult.error || 'Unknown error')}\\.`; // Escaped .
                if(client) client.release();
            return bot.editMessageText(errorMsg, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: '↩️ Back to Games', callback_data: 'menu:game_selection' }]] } });
        }
        const { betId, newBalance: balanceAfterBet } = betPlacementResult;
        finalUserBalance = balanceAfterBet; // Initialize final balance with balance after bet

        const { winningNumber } = simulateRouletteSpin(); // from Part 4
        // Use the bet details saved during placement for consistency
        const basePayoutMultiplier = getRoulettePayoutMultiplier(betDetails.type, betDetails.value, winningNumber); // from Part 4
        const win = basePayoutMultiplier > 0;

        // Payout Logic
        let profitLamportsOutcome = -betAmountLamports; // Default loss, used for win calculation/message display
        let payoutAmountForDB = 0n; // For DB bet status AND balance update on win (Stake + Net Profit)

        if (win) {
            const grossWinnings = BigInt(Math.floor(Number(betAmountLamports) * basePayoutMultiplier));
            const netWinningsAfterEdge = BigInt(Math.floor(Number(grossWinnings) * (1 - gameConfig.houseEdge)));
            profitLamportsOutcome = netWinningsAfterEdge; // This is the actual profit for message display
            payoutAmountForDB = betAmountLamports + netWinningsAfterEdge; // Total returned to user for balance update

            // *** FIX #0: Update balance using TOTAL PAYOUT (Stake + Profit) ***
            const balanceUpdateResult = await updateUserBalanceAndLedger(client, userId, payoutAmountForDB, 'roulette_win', { betId }); // from Part 2
            if (!balanceUpdateResult.success) {
                await client.query('ROLLBACK');
                // MarkdownV2 Safety: Escape error message, bet ID
                const errorMsg = `⚠️ Critical error processing game result: ${escapeMarkdownV2(balanceUpdateResult.error || "DB Error")}\\. Bet recorded but result uncertain\\. Contact support with Bet ID: ${betId}`; // Escaped .
                if(client) client.release();
                return bot.editMessageText(errorMsg, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: '↩️ Back to Games', callback_data: 'menu:game_selection' }]] } });
            }
            finalUserBalance = balanceUpdateResult.newBalance; // Update final balance on win (will be B+P)
        } else {
             // *** LOSS: No balance update needed here, only mark bet status ***
            profitLamportsOutcome = -betAmountLamports; // Keep for potential logging/message
            payoutAmountForDB = 0n; // Payout for DB status update
             // `finalUserBalance` remains `balanceAfterBet`
             console.log(`${logPrefix} Roulette loss for Bet ID ${betId}. Balance remains ${formatSol(finalUserBalance)} SOL after initial deduction.`);
        }

        // Update bet status regardless of win/loss, use payoutAmountForDB
        await updateBetStatus(client, betId, win ? 'completed_win' : 'completed_loss', payoutAmountForDB); // from Part 2
        await client.query('COMMIT');

        // Animation: Show "spinning" message
      // Construct bet description string, ensure escaping
        let betDescription = escapeMarkdownV2(betDetails.type);
        if (betDetails.type === 'straight' && betDetails.value !== undefined && betDetails.value !== null) {
            betDescription += ` on ${escapeMarkdownV2(betDetails.value)}`;
        } else if (betDetails.type === 'red') { betDescription = '🔴 Red'; }
        else if (betDetails.type === 'black') { betDescription = '⚫️ Black'; }
        else if (betDetails.type === 'even') { betDescription = '🔢 Even'; }
        else if (betDetails.type === 'odd') { betDescription = '🔢 Odd'; }
        else if (betDetails.type === 'low') { betDescription = '📉 Low \\(1\\-18\\)'; } // Escaped () -
        else if (betDetails.type === 'high') { betDescription = '📈 High \\(19\\-36\\)'; } // Escaped () -

        const spinningText = `⚪️ *Roulette Spinning\\!* ⚪️\n\nBet: ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL on ${betDescription}\n\nSpinning the wheel\\.\\.\\.`; // Escaped ! ... Add Emoji
        await bot.editMessageText(spinningText, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2' }).catch(e => {
           if (!e.message.includes("message is not modified") && e.message.toLowerCase().includes("can't parse")) {
               console.error(`[RouletteGame] PARSE ERROR ON SPINNING TEXT: ${spinningText}`, e.message);
           } else if (!e.message.includes("message is not modified")) {
               console.warn(`[RouletteGame] Failed edit for spinning text: ${e.message}`);
           }
       });
        await sleep(2000); // sleep from Part 1

        // Final Message Construction with careful escaping
        const numberColor = winningNumber === 0 ? '🟢' : (getRoulettePayoutMultiplier('red', null, winningNumber) ? '🔴' : '⚫️');
        let resultMsg = `⚪️ *Roulette Result* ⚪️\n\nWinning Number: *${escapeMarkdownV2(numberColor + String(winningNumber))}*\\!\nBet: ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL on ${betDescription}\n\n`; // Escaped !

      // *** CORRECTED escaping for result lines ***
        if (win) {
            // Escape parentheses and exclamation mark
            resultMsg += `🎉 You won ${escapeMarkdownV2(formatSol(profitLamportsOutcome))} SOL\\! \\(Multiplier: ${escapeMarkdownV2(basePayoutMultiplier)}x base\\)`; // Display profit in message, escaped ()!
        } else {
            // Escape period
            resultMsg += `😢 You lost ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL\\.`; // Escaped .
        }
        resultMsg += `\n\nNew Balance: ${escapeMarkdownV2(formatSol(finalUserBalance))} SOL`; // Display correct final balance (B+P on win)

        const keyboard = { // Add Emojis
            inline_keyboard: [[{ text: '🔄 Play Again', callback_data: `play_again:${gameKey}:${betAmountLamports}` }, { text: '🎮 Games Menu', callback_data: 'menu:game_selection' }]]
        };
        await bot.editMessageText(resultMsg, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: keyboard }).catch(e => {
            if (!e.message.includes("message is not modified") && e.message.toLowerCase().includes("can't parse")) {
               console.error(`[RouletteGame] PARSE ERROR ON FINAL RESULT TEXT: ${resultMsg}`, e.message);
               // Fallback to plain text if parse fails
               const plainResult = `Roulette Result\nWinning Number: ${numberColor + String(winningNumber)}\nBet: ${formatSol(betAmountLamports)} SOL on ${betDetails.type}${betDetails.value ? ' on '+betDetails.value : ''}\nOutcome: ${win ? 'Win '+formatSol(profitLamportsOutcome)+' SOL' : 'Loss '+formatSol(betAmountLamports)+' SOL'}\nNew Balance: ${formatSol(finalUserBalance)} SOL`;
               safeSendMessage(chatId, plainResult, { reply_markup: keyboard });
            } else if (!e.message.includes("message is not modified")) {
               console.warn(`[RouletteGame] Failed edit for final result text: ${e.message}`);
               // Attempt to send as new message if edit fails non-parsing reason
               safeSendMessage(chatId, resultMsg, { parse_mode: 'MarkdownV2', reply_markup: keyboard });
            }
       });

    } catch (error) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} Rollback failed:`, rbErr));
        console.error(`${logPrefix} Error in roulette game:`, error);
        // MarkdownV2 Safety: Escape error message
        bot.editMessageText(`⚠️ An unexpected error occurred during Roulette: ${escapeMarkdownV2(error.message)}\\. Please try again later\\.`, { // Escaped .
            chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2',
            reply_markup: { inline_keyboard: [[{ text: '↩️ Back to Games', callback_data: 'menu:game_selection' }]] }
        });
    } finally {
        if (client) client.release();
    }
}


async function handleWarGame(userId, chatId, messageId, betAmountLamports) {
    // *** Applying Fix #11: Add Emojis to War Result (Verified Already Present) ***
    const gameKey = 'war';
    const gameConfig = GAME_CONFIG[gameKey]; // GAME_CONFIG from Part 1
    const logPrefix = `[WarGame User ${userId} Bet ${betAmountLamports}]`;
    console.log(`${logPrefix} Handling war game.`);

    let client = null;
    let finalUserBalance; // To store correct final balance

    try {
        client = await pool.connect(); // pool from Part 1
        await client.query('BEGIN');

        const betPlacementResult = await placeBet(client, userId, chatId, gameKey, {}, betAmountLamports); // placeBet from Part 5a-1
        if (!betPlacementResult.success) {
            await client.query('ROLLBACK');
            const errorMsg = betPlacementResult.error === 'Insufficient balance'
                ? `⚠️ Insufficient balance for a ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL bet\\. Your balance is ${escapeMarkdownV2(formatSol(betPlacementResult.currentBalance || 0n))} SOL\\.` // Escaped . formatSol from Part 3
                : `⚠️ Error placing bet: ${escapeMarkdownV2(betPlacementResult.error || 'Unknown error')}\\.`; // Escaped .
            if (client) client.release();
            return bot.editMessageText(errorMsg, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: '↩️ Back to Games', callback_data: 'menu:game_selection' }]] } });
        }
        const { betId, newBalance: balanceAfterBet } = betPlacementResult;
        finalUserBalance = balanceAfterBet; // Initialize final balance

        // simulateWar returns { result, playerCard: formatted string, dealerCard: formatted string }
        const { result, playerCard, dealerCard } = simulateWar(); // simulateWar from Part 4

        let profitLamportsOutcome = -betAmountLamports; // Default loss, used for message display
        let payoutAmountForDB = 0n; // For DB: Stake + Net Profit for wins/pushes, 0 for loss
        let dbBetStatus = 'completed_loss';
        let ledgerTransactionType = 'war_loss';

        if (result === 'win') {
            dbBetStatus = 'completed_win';
            ledgerTransactionType = 'war_win';
            const profitBeforeEdge = betAmountLamports; // War pays 1:1 on the bet
            const netProfit = BigInt(Math.floor(Number(profitBeforeEdge) * (1 - gameConfig.houseEdge)));
            profitLamportsOutcome = netProfit; // For message display
            payoutAmountForDB = betAmountLamports + netProfit; // For balance update & DB status

             // *** FIX #0: Update balance using TOTAL PAYOUT (Stake + Profit) ***
            const balanceUpdateResult = await updateUserBalanceAndLedger(client, userId, payoutAmountForDB, ledgerTransactionType, { betId }); // from Part 2
            if (!balanceUpdateResult.success) {
                await client.query('ROLLBACK');
                const errorMsg = `⚠️ Critical error processing game result: ${escapeMarkdownV2(balanceUpdateResult.error || "DB Error")}\\. Bet recorded but result uncertain\\. Contact support with Bet ID: ${betId}`;
                if (client) client.release();
                return bot.editMessageText(errorMsg, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: '↩️ Back to Games', callback_data: 'menu:game_selection' }]] } });
            }
            finalUserBalance = balanceUpdateResult.newBalance; // Update final balance on win (will be B+P)

        } else if (result === 'push') {
            dbBetStatus = 'completed_push';
            ledgerTransactionType = 'war_push';
            profitLamportsOutcome = 0n;
            payoutAmountForDB = betAmountLamports; // Return stake for balance update & DB status

            // *** FIX #0: Update balance using TOTAL PAYOUT (Stake) to reverse deduction ***
            const balanceUpdateResult = await updateUserBalanceAndLedger(client, userId, payoutAmountForDB, ledgerTransactionType, { betId }); // from Part 2
            if (!balanceUpdateResult.success) {
                await client.query('ROLLBACK');
                const errorMsg = `⚠️ Critical error processing game result: ${escapeMarkdownV2(balanceUpdateResult.error || "DB Error")}\\. Bet recorded but result uncertain\\. Contact support with Bet ID: ${betId}`;
                if (client) client.release();
                return bot.editMessageText(errorMsg, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: '↩️ Back to Games', callback_data: 'menu:game_selection' }]] } });
            }
             finalUserBalance = balanceUpdateResult.newBalance; // Update final balance on push (will be same as balanceAfterBet)
        } else {
             // *** LOSS: No balance update needed here, only mark bet status ***
             dbBetStatus = 'completed_loss';
             ledgerTransactionType = 'war_loss';
             profitLamportsOutcome = -betAmountLamports; // Keep for logging/message
             payoutAmountForDB = 0n; // Payout for DB status update
             // `finalUserBalance` remains `balanceAfterBet`
             console.log(`${logPrefix} War loss for Bet ID ${betId}. Balance remains ${formatSol(finalUserBalance)} SOL after initial deduction.`);
        }

        // Update bet status using payoutAmountForDB
        await updateBetStatus(client, betId, dbBetStatus, payoutAmountForDB); // from Part 2
        await client.query('COMMIT');

        const dealingText = `🃏 *Casino War* 🃏\n\nBet: ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL\nDealing cards\\.\\.\\.`; // escapeMarkdownV2 from Part 1, formatSol from Part 3
        await bot.editMessageText(dealingText, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2' }).catch(e => {});
        await sleep(1500); // sleep from Part 1

        // *** FIX #11: Emojis are already present here in version 3.2.1q ***
        let resultMsg = `🃏 *Casino War Result* 🃏\n\nBet: ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL\n\nPlayer's Card: *${escapeMarkdownV2(playerCard)}*\nDealer's Card: *${escapeMarkdownV2(dealerCard)}*\n\n`; // Card strings directly from simulateWar
        if (result === 'win') resultMsg += `🎉 You won ${escapeMarkdownV2(formatSol(profitLamportsOutcome))} SOL\\!`; // Added Emoji
        else if (result === 'push') resultMsg += `🤝 Push\\! Your bet of ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL is returned\\.`; // Added Emoji
        else resultMsg += `😢 You lost ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL\\.`; // Added Emoji
        resultMsg += `\n\nNew Balance: ${escapeMarkdownV2(formatSol(finalUserBalance))} SOL`; // Display correct final balance

        const keyboard = {
            inline_keyboard: [
                [{ text: '🔄 Play Again', callback_data: `play_again:${gameKey}:${betAmountLamports}` }, { text: '🎮 Games Menu', callback_data: 'menu:game_selection' }]
            ]
        };
        await bot.editMessageText(resultMsg, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: keyboard });

    } catch (error) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} Rollback failed:`, rbErr));
        console.error(`${logPrefix} Error in war game:`, error);
        bot.editMessageText(`⚠️ An unexpected error occurred during War: ${escapeMarkdownV2(error.message)}\\. Please try again later\\.`, { // Escaped .
            chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2',
            reply_markup: { inline_keyboard: [[{ text: '↩️ Back to Games', callback_data: 'menu:game_selection' }]] }
        });
    } finally {
        if (client) client.release();
    }
}


async function handleCrashGame(userId, chatId, messageId, betAmountLamports) {
    // *** Applying Fix #7: Increase Crash Game Speed (Verified Already Present) ***
    const gameKey = 'crash';
    const gameConfig = GAME_CONFIG[gameKey]; // GAME_CONFIG from Part 1
    const logPrefix = `[CrashGame User ${userId} Bet ${betAmountLamports}]`;
    console.log(`${logPrefix} Starting Crash game.`);

    let client = null;
    let betId = null;
    let balanceAfterBet = null;

    try {
        client = await pool.connect(); // pool from Part 1
        await client.query('BEGIN');

        const betPlacementResult = await placeBet(client, userId, chatId, gameKey, {}, betAmountLamports); // placeBet from Part 5a-1
        if (!betPlacementResult.success) {
            await client.query('ROLLBACK');
            const errorMsg = betPlacementResult.error === 'Insufficient balance'
                ? `⚠️ Insufficient balance for a ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL bet\\. Your balance is ${escapeMarkdownV2(formatSol(betPlacementResult.currentBalance || 0n))} SOL\\.` // Escaped . formatSol from Part 3
                : `⚠️ Error placing bet: ${escapeMarkdownV2(betPlacementResult.error || 'Unknown error')}\\.`; // Escaped .
                if(client) client.release();
            return bot.editMessageText(errorMsg, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: '↩️ Back to Games', callback_data: 'menu:game_selection' }]] } });
        }
        betId = betPlacementResult.betId; // Store betId
        balanceAfterBet = betPlacementResult.newBalance; // Store balance AFTER deduction

        await updateBetStatus(client, betId, 'processing_game'); // updateBetStatus from Part 2
        await client.query('COMMIT'); // Commit bet placement
        if(client) client.release(); client = null;

        const crashPoint = simulateCrash(); // from Part 4
        if (typeof crashPoint?.crashMultiplier !== 'number') {
            console.error(`${logPrefix} simulateCrash returned invalid data:`, crashPoint);
            throw new Error("Failed to simulate crash point.");
        }
        const actualCrashPoint = crashPoint.crashMultiplier;
        console.log(`${logPrefix} Game will crash at ${actualCrashPoint}x. Bet ID: ${betId}`);

        // Store game state in cache for cashout actions
        userStateCache.set(userId, { // userStateCache from Part 1
            action: 'awaiting_crash_cashout', // Use 'action' key consistently
            gameKey: gameKey,
            betId: betId,
            betAmountLamports: betAmountLamports.toString(),
            chatId: String(chatId),
            messageId: messageId,
            currentMultiplier: 1.00, // Start multiplier
            targetCrashMultiplier: actualCrashPoint, // Store the target
            balanceAfterBet: balanceAfterBet.toString(), // Store balance *after* bet placement
            timestamp: Date.now() // Add timestamp for TTL check
        });

        let currentMultiplier = 1.00;
        let loopActive = true;
        let lastMessageText = "";

        const initialMessageText = `🚀 *Crash Game Started\\!* 🚀\nBet: ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL\nMultiplier: *${escapeMarkdownV2(currentMultiplier.toFixed(2))}x*\n\nWaiting for lift off\\!`; // Escaped ! Add Emoji
        await bot.editMessageText(initialMessageText, {
            chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2',
            reply_markup: { inline_keyboard: [[{ text: `💸 Cash Out`, callback_data: `cash_out_crash:${betId}` }]] }
        }).catch(e => { if(!e.message.includes("message is not modified")) console.warn(`${logPrefix} Error sending initial crash message: ${e.message}`)});
        lastMessageText = initialMessageText;

        await sleep(1500); // Initial pause before lift-off

        // Game Loop
        while (loopActive) {
            const gameState = userStateCache.get(userId);
            // Check if state is still valid for this game instance before proceeding
            if (!gameState || gameState.action !== 'awaiting_crash_cashout' || gameState.betId !== betId) {
                console.log(`${logPrefix} Game state changed or cleared externally (e.g., user cashed out or cancelled). Ending loop for Bet ID ${betId}.`);
                loopActive = false;
                break; // Exit loop if state is gone or doesn't match
            }

            // Check for crash condition
            if (currentMultiplier >= actualCrashPoint) {
                console.log(`${logPrefix} Multiplier ${currentMultiplier} reached crash point ${actualCrashPoint}. Crashing.`);
                loopActive = false;
                break; // Exit loop to process the crash
            }

            // Increment multiplier based on current value
            const oldMultiplier = currentMultiplier;
            let increment = 0.01; // Base increment
            if (oldMultiplier >= 20) increment = 0.50;
            else if (oldMultiplier >= 10) increment = 0.25;
            else if (oldMultiplier >= 5) increment = 0.10;
            else if (oldMultiplier >= 3) increment = 0.05;
            else if (oldMultiplier >= 1.5) increment = 0.02;
            currentMultiplier = parseFloat((oldMultiplier + Math.max(0.01, increment)).toFixed(2));

            // Update multiplier in cache state
            gameState.currentMultiplier = currentMultiplier;
            userStateCache.set(userId, gameState); // Update the state in the cache

            // Update Telegram message
            const displayMultiplier = escapeMarkdownV2(currentMultiplier.toFixed(2));
            let animationText = `🚀 *Crash Game In Progress\\!* 🚀\nBet: ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL\nMultiplier: *${displayMultiplier}x*\n\n`; // Escaped ! Add Emoji
            if (currentMultiplier < 1.5) animationText += `Climbing steadily\\.\\.\\.`;
            else if (currentMultiplier < 3) animationText += `Gaining altitude\\! 📈`;
            else if (currentMultiplier < 7) animationText += `To the moon\\! 🌕`;
            else animationText += `Beyond the stars\\! ✨ This is getting risky\\!`;

            // Only edit message if text content has changed to avoid rate limits/errors
            if (animationText !== lastMessageText) {
                await bot.editMessageText(animationText, {
                    chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2',
                    reply_markup: { inline_keyboard: [[{ text: `💸 Cash Out at ${displayMultiplier}x`, callback_data: `cash_out_crash:${betId}` }]] }
                }).catch(loopError => {
                    // Handle errors, potentially stop loop if message is gone
                    if (!loopError.message.includes("message is not modified")) {
                        console.warn(`Error updating crash animation (Bet ID ${betId}): ${loopError.message}.`);
                        console.error("Crash Animation Loop Error Stack:", loopError.stack);
                        // If message is gone, stop the loop for this user
                        if (loopError.message.includes("message to edit not found") || loopError.message.includes("chat not found")) {
                            loopActive = false;
                            console.error(`${logPrefix} Halting crash loop for Bet ID ${betId} due to message edit error.`);
                            clearUserState(userId); // Clear state if message is gone
                        }
                    }
                });
                lastMessageText = animationText;
            }

            // Break if loop was deactivated by error handling
            if(!loopActive) break;

            // *** FIX #7: Adjusted Crash Speed (Verified Already Present) ***
            // Calculate delay based on multiplier - faster start, slower later
            const delay = Math.max(100, 550 - Math.floor(oldMultiplier * 20)); // Faster start/acceleration
            await sleep(delay); // sleep from Part 1
        } // End while loop

        // --- Process Natural Crash (if loop ended because crash point was reached) ---
        const finalStateCheck = userStateCache.get(userId);
        // Only process crash if the state still exists and matches this betId
        if (finalStateCheck && finalStateCheck.action === 'awaiting_crash_cashout' && finalStateCheck.betId === betId) {
            clearUserState(userId); // Clear state as game ended
            console.log(`${logPrefix} Natural crash occurred for Bet ID ${betId} at ${actualCrashPoint}x.`);
            let clientCrash = null;
            try {
                clientCrash = await pool.connect();
                await clientCrash.query('BEGIN');
                // LOSS: No balance update needed, only update bet status to loss
                const finalUserBalanceCrash = BigInt(finalStateCheck.balanceAfterBet || '0'); // Show balance after bet deduction
                await updateBetStatus(clientCrash, betId, 'completed_loss', 0n); // Loss means 0 payout
                await clientCrash.query('COMMIT');

                // Notify user about the crash
                const crashResultText = `💥 *CRASHED at ${escapeMarkdownV2(actualCrashPoint.toFixed(2))}x\\!* 💥\n\nBet: ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL\nYou didn't cash out in time and lost your bet\\. Tough luck\\!\n\nFinal Balance: ${escapeMarkdownV2(formatSol(finalUserBalanceCrash))} SOL`; // Show balance after initial bet
                await bot.editMessageText(crashResultText, {
                    chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2',
                    reply_markup: { inline_keyboard: [[{ text: '🔄 Play Again', callback_data: `play_again:${gameKey}:${betAmountLamports}` }, { text: '🎮 Games Menu', callback_data: 'menu:game_selection' }]] }
                }).catch(e => {}); // Ignore errors editing final message
            } catch (dbError) {
                if (clientCrash) await clientCrash.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} Crash Loss Rollback failed:`, rbErr));
                console.error(`${logPrefix} DB Error processing natural crash for Bet ID ${betId}:`, dbError);
                // Notify user about the DB error
                 bot.editMessageText(`⚠️ Database error processing crash result for Bet ID ${betId}: ${escapeMarkdownV2(dbError.message)}\\. Please contact support if balance is incorrect\\.`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: `🔄 Play Crash Again`, callback_data: `play_again:${gameKey}:${betAmountLamports}` }, { text: '🎮 Games Menu', callback_data: 'menu:game_selection' }]] } }).catch(e => {});
            } finally { if (clientCrash) clientCrash.release(); }
        } else {
            console.log(`${logPrefix} Crash game loop ended, but user state was already cleared (likely cashed out). Bet ID: ${betId}.`);
        }

    } catch (error) {
        // Catch errors during initial setup phase
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} Initial Setup Rollback failed:`, rbErr));
        console.error(`${logPrefix} Major error starting or during crash game (Bet ID ${betId || 'N/A'}):`, error);
        clearUserState(userId); // Ensure state is cleared on major error
         bot.editMessageText(`⚠️ An unexpected error occurred setting up Crash: ${escapeMarkdownV2(error.message)}\\. Please try again later\\.`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: '↩️ Back to Games', callback_data: 'menu:game_selection' }]] } }).catch(e => {});
    } finally {
        // Ensure client is released if error occurred after initial commit but before further actions
        if (client && !client.isReleased) client.release();
    }
}

// Handles Blackjack state and interactions
async function handleBlackjackGame(userId, chatId, messageId, betAmountLamports, playerAction, existingGameState = null) {
    // *** Applying Fix: Escape Parentheses for player score display (Verified Already Present) ***
    const gameKey = 'blackjack';
    const gameConfig = GAME_CONFIG[gameKey]; // GAME_CONFIG from Part 1
    const houseEdgeBJ = gameConfig.houseEdge;
    const logPrefix = `[BlackjackGame User ${userId} Bet ${betAmountLamports} Action ${playerAction}]`;

    let client = null;
    let currentMessageId = messageId;
    let gameState = existingGameState;
    let betId;

    try {
        let deck, playerHand, dealerHand, playerValue, dealerValue, balanceAfterBet;

        // --- Start New Game ---
        if (playerAction === 'start_game') {
            console.log(`${logPrefix} Starting new Blackjack game.`);
            client = await pool.connect(); // pool from Part 1
            await client.query('BEGIN');

            // Place the bet (deducts balance, creates bet record)
            const betPlacementResult = await placeBet(client, userId, chatId, gameKey, {}, betAmountLamports); // placeBet from Part 5a-1
            if (!betPlacementResult.success) {
                await client.query('ROLLBACK');
                const errorMsg = betPlacementResult.error === 'Insufficient balance' ? `⚠️ Insufficient balance for a ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL bet\\. Your balance is ${escapeMarkdownV2(formatSol(betPlacementResult.currentBalance || 0n))} SOL\\.` : `⚠️ Error placing bet: ${escapeMarkdownV2(betPlacementResult.error || 'Unknown error')}\\.`;
                    if(client) client.release();
                return bot.editMessageText(errorMsg, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: '↩️ Back to Games', callback_data: 'menu:game_selection' }]] } });
            }
            betId = betPlacementResult.betId;
            balanceAfterBet = betPlacementResult.newBalance; // Balance *after* deduction

            // Deal initial hands
            deck = createDeck(); shuffleDeck(deck); // Card utils from Part 4
            playerHand = [dealCard(deck), dealCard(deck)];
            dealerHand = [dealCard(deck), dealCard(deck)];
            playerValue = calculateHandValue(playerHand); // Card utils from Part 4
            dealerValue = calculateHandValue(dealerHand); // Used for immediate BJ check

            // Store game state in cache
            gameState = {
                action: 'awaiting_blackjack_action', // State marker
                gameKey, betId,
                betAmountLamports: betAmountLamports.toString(),
                playerCards: playerHand,
                dealerCards: dealerHand,
                deck, // Store remaining deck
                playerScore: playerValue,
                dealerScore: dealerValue, // Initial dealer score for BJ check
                chatId: String(chatId),
                messageId: messageId,
                timestamp: Date.now(),
                balanceAfterBet: balanceAfterBet.toString() // Store for result processing
            };
            userStateCache.set(userId, gameState); // userStateCache from Part 1

            await updateBetStatus(client, betId, 'processing_game'); // updateBetStatus from Part 2
            await client.query('COMMIT'); // Commit bet placement and status update
            if (client) client.release(); client = null;

            // Show initial dealing message
            const initialDealText = `🃏 Dealing Blackjack for your ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL bet\\.\\.\\.`;
            await bot.editMessageText(initialDealText, {chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2'}).catch(e => {});
            await sleep(1000);

            // Check for immediate Blackjacks
            const playerHasBlackjack = playerValue === 21 && playerHand.length === 2;
            const dealerHasBlackjack = dealerValue === 21 && dealerHand.length === 2;

            if (playerHasBlackjack || dealerHasBlackjack) {
                let immediateOutcome = 'push'; // Assume push if both have BJ
                if (playerHasBlackjack && !dealerHasBlackjack) immediateOutcome = 'player_blackjack';
                else if (dealerHasBlackjack && !playerHasBlackjack) immediateOutcome = 'dealer_wins';
                return processBlackjackResult(userId, chatId, messageId, gameState, immediateOutcome); // Process result immediately
            }

        // --- Continue Existing Game (Hit/Stand) ---
        } else if (playerAction === 'hit' || playerAction === 'stand') {
            if (!gameState || gameState.betId === undefined) {
                console.warn(`${logPrefix} No existing Blackjack game state found for user.`);
                clearUserState(userId); // clearUserState from Part 6
                return bot.editMessageText("⚠️ Your Blackjack session has expired or could not be found\\. Please start a new game\\.", { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: '↩️ Back to Games', callback_data: 'menu:game_selection' }]] } });
            }
            // Load state from cache
            betId = gameState.betId;
            playerHand = gameState.playerCards;
            dealerHand = gameState.dealerCards;
            deck = gameState.deck;
            betAmountLamports = BigInt(gameState.betAmountLamports);
            balanceAfterBet = BigInt(gameState.balanceAfterBet);
            currentMessageId = gameState.messageId; // Use the message ID stored in state

            console.log(`${logPrefix} Continuing game for Bet ID: ${betId} with action: ${playerAction}`);
             if (client) client.release(); client = null; // Ensure no dangling client

        } else {
            throw new Error(`Invalid playerAction received in handleBlackjackGame: ${playerAction}`);
        }

        // --- Handle Player Action ---
        if (playerAction === 'hit') {
            const newCard = dealCard(deck); // Card utils from Part 4
            if (newCard) {
                playerHand.push(newCard);
                console.log(`${logPrefix} Player hits, dealt: ${formatCard(newCard)}`);
            } else {
                // Extremely unlikely with standard deck assumptions, but handle defensively
                console.error(`${logPrefix} Deck empty when player tried to hit for Bet ID ${betId}. Auto-standing.`);
                playerAction = 'stand'; // Force stand if deck is empty
            }
            // Update gameState with new hand and deck
            gameState.playerCards = playerHand;
            gameState.deck = deck;
            gameState.playerScore = calculateHandValue(playerHand); // Recalculate score
            // Check for bust immediately after hit
            if (gameState.playerScore > 21) {
                console.log(`${logPrefix} Player busted with score ${gameState.playerScore}.`);
                return processBlackjackResult(userId, chatId, currentMessageId, gameState, 'player_busts'); // Process bust result
            }
        }

        // If player stands (or was forced to stand due to empty deck)
        if (playerAction === 'stand') {
            console.log(`${logPrefix} Player stands with score ${gameState.playerScore}. Dealer's turn.`);
            // Simulate dealer's play
             const dealerResult = simulateDealerPlay([...dealerHand], deck); // Pass copy of hand, from Part 4
            gameState.dealerCards = dealerResult.hand; // Update state with final dealer hand
            gameState.dealerScore = dealerResult.value; // Update state with final dealer score
            // Determine winner based on final hands
            const gameOutcome = determineBlackjackWinner(playerHand, gameState.dealerCards).result; // from Part 4
            console.log(`${logPrefix} Dealer finishes play. Final Outcome: ${gameOutcome}`);
            return processBlackjackResult(userId, chatId, currentMessageId, gameState, gameOutcome); // Process final result
        }

        // --- Update UI for next player action (Hit/Stand) ---
        // This part is only reached if player hit and did NOT bust
        const formattedPlayerHand = gameState.playerCards.map(c => formatCard(c)).join(' '); // Card utils from Part 4
        const formattedDealerShowCard = gameState.dealerCards.length > 0 ? formatCard(gameState.dealerCards[0]) : '??';

        // *** FIX: Escape Parentheses around player score (Verified Already Present) ***
        let currentHandsText = `♠️ *Blackjack Table* ♥️\n\nBet: ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL\n\n`;
        currentHandsText += `*Your Hand \\(${escapeMarkdownV2(gameState.playerScore)}\\):* ${escapeMarkdownV2(formattedPlayerHand)}\n`; // Escaped () around score
        currentHandsText += `*Dealer Shows:* ${escapeMarkdownV2(formattedDealerShowCard)} \\(?\\)\n\n`; // Escaped () ?
        currentHandsText += `What's your move?`;

        const actionKeyboard = { inline_keyboard: [
            [{ text: '➕ Hit', callback_data: `blackjack_action:hit:${betId}` }, { text: '✋ Stand', callback_data: `blackjack_action:stand:${betId}` }]
        ] };

        // Update the state cache before sending the message
        gameState.timestamp = Date.now(); // Update timestamp
        userStateCache.set(userId, gameState);

        // Edit the existing message with the updated hands and action buttons
        await bot.editMessageText(currentHandsText, {
            chat_id: chatId, message_id: currentMessageId,
            parse_mode: 'MarkdownV2', reply_markup: actionKeyboard
        });

    } catch (error) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} Outer Rollback failed:`, rbErr));
        console.error(`${logPrefix} Error in Blackjack game:`, error);
        clearUserState(userId); // Clear state on error
        bot.editMessageText(`⚠️ An unexpected error occurred during Blackjack: ${escapeMarkdownV2(error.message)}\\. Please try again later\\.`, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: '↩️ Back to Games', callback_data: 'menu:game_selection' }]] } });
    } finally {
        // Ensure client is released if an error occurred after initial commit but before further actions
        if (client && !client.isReleased) client.release();
    }
}

/**
 * Handles the final result processing for a Blackjack game (win, loss, push, bust).
 * Needs to perform DB updates for balance and bet status.
 */
async function processBlackjackResult(userId, chatId, messageId, gameState, outcomeKey) {
    // *** Applying Fix #0: Update balance using TOTAL PAYOUT for wins/pushes ***
    // *** Applying Fix #1: Do not update balance on loss/bust ***
    const gameKey = 'blackjack';
    const gameConfig = GAME_CONFIG[gameKey]; // GAME_CONFIG from Part 1
    const houseEdge = gameConfig.houseEdge;
    const betId = gameState.betId;
    const betAmountLamports = BigInt(gameState.betAmountLamports);
    const playerHand = gameState.playerCards;
    const dealerHand = gameState.dealerCards;
    const playerValue = gameState.playerScore;
    const dealerValue = calculateHandValue(dealerHand); // Calculate final dealer score here
    const balanceAfterBet = BigInt(gameState.balanceAfterBet); // Balance AFTER bet was placed
    const logPrefix = `[ProcessBJResult BetID ${betId} Outcome ${outcomeKey}]`;
    clearUserState(userId); // Game over, clear state

    let client = null;
    let profitLamportsOutcome = -betAmountLamports; // Default to loss, used for message display
    let payoutAmountForDB = 0n; // For DB status AND balance update (Stake + Net Profit)
    let resultDisplayString = "";
    let dbBetStatus = 'completed_loss'; // Default status
    let ledgerTransactionType = `blackjack_${outcomeKey}`;
    let finalUserBalance = balanceAfterBet; // Initialize with balance AFTER bet was placed

    try {
        client = await pool.connect(); // pool from Part 1
        await client.query('BEGIN');

        // Determine outcome and calculate payouts/updates
        switch (outcomeKey) {
            case 'player_blackjack':
                dbBetStatus = 'completed_win'; ledgerTransactionType = 'blackjack_player_blackjack';
                // Blackjack typically pays 3:2 on the wager amount
                const bjWinnings = BigInt(Math.floor(Number(betAmountLamports) * 1.5)); // 3:2 payout
                const netBjWinnings = BigInt(Math.floor(Number(bjWinnings) * (1 - houseEdge)));
                profitLamportsOutcome = netBjWinnings; // For message
                payoutAmountForDB = betAmountLamports + netBjWinnings; // Total returned = stake + net winnings
              // Escape !, ()
                resultDisplayString = `♠️♥️♦️♣️ BLACKJACK\\! You win ${escapeMarkdownV2(formatSol(profitLamportsOutcome))} SOL \\(3:2 payout\\)\\!`; // Escaped ! ()
                // Update balance by adding stake + net winnings
                const balanceUpdateResultBJ = await updateUserBalanceAndLedger(client, userId, payoutAmountForDB, ledgerTransactionType, { betId });
                if (!balanceUpdateResultBJ.success) { throw new Error(`Failed balance update (BJ Win): ${escapeMarkdownV2(balanceUpdateResultBJ.error)}`); }
                finalUserBalance = balanceUpdateResultBJ.newBalance; // Update final balance
                break;

            case 'dealer_busts_player_wins':
            case 'player_wins':
                dbBetStatus = 'completed_win'; ledgerTransactionType = outcomeKey === 'dealer_busts_player_wins' ? 'blackjack_dealer_busts' : 'blackjack_player_wins';
                // Regular win pays 1:1 on the wager amount
                const regWinnings = betAmountLamports; // 1:1 payout
                const netRegularWinnings = BigInt(Math.floor(Number(regWinnings) * (1 - houseEdge)));
                profitLamportsOutcome = netRegularWinnings; // For message
                payoutAmountForDB = betAmountLamports + netRegularWinnings; // Total returned = stake + net winnings
              // Escape ! .
                if (outcomeKey === 'dealer_busts_player_wins') { resultDisplayString = `🎉 Dealer Busted\\! You win ${escapeMarkdownV2(formatSol(profitLamportsOutcome))} SOL\\!`; } // Escaped !
                else { resultDisplayString = `🎉 You Win\\! ${escapeMarkdownV2(formatSol(profitLamportsOutcome))} SOL earned\\.`; } // Escaped ! .
                // Update balance by adding stake + net winnings
                const balanceUpdateResultWin = await updateUserBalanceAndLedger(client, userId, payoutAmountForDB, ledgerTransactionType, { betId });
                if (!balanceUpdateResultWin.success) { throw new Error(`Failed balance update (BJ Win/Bust): ${escapeMarkdownV2(balanceUpdateResultWin.error)}`); }
                finalUserBalance = balanceUpdateResultWin.newBalance; // Update final balance
                break;

            case 'push':
                dbBetStatus = 'completed_push'; ledgerTransactionType = 'blackjack_push';
                profitLamportsOutcome = 0n;
                payoutAmountForDB = betAmountLamports; // Return stake for balance + status
              // Escape ! .
                resultDisplayString = `🤝 Push\\! Your bet is returned\\.`; // Escaped ! .
                // Update balance (adds stake back effectively, reversing the initial deduction)
                const balanceUpdateResultPush = await updateUserBalanceAndLedger(client, userId, payoutAmountForDB, ledgerTransactionType, { betId });
                if (!balanceUpdateResultPush.success) { throw new Error(`Failed balance update (BJ Push): ${escapeMarkdownV2(balanceUpdateResultPush.error)}`); }
                finalUserBalance = balanceUpdateResultPush.newBalance; // Update final balance
                break;

            case 'dealer_wins':
                dbBetStatus = 'completed_loss'; ledgerTransactionType = 'blackjack_dealer_wins';
                profitLamportsOutcome = -betAmountLamports; // For message display
                payoutAmountForDB = 0n; // Payout for DB status update
              // Escape () .
                resultDisplayString = `😢 Dealer Wins \\(${escapeMarkdownV2(dealerValue)}\\ vs ${escapeMarkdownV2(playerValue)}\\)\\. You lost ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL\\.`; // Escaped () .
                // *** LOSS: No balance update needed here. finalUserBalance remains balanceAfterBet ***
                console.log(`${logPrefix} Blackjack loss (Dealer Wins) for Bet ID ${betId}. Balance remains ${formatSol(finalUserBalance)} SOL after initial deduction.`);
                break;

            case 'player_busts':
                dbBetStatus = 'completed_loss'; ledgerTransactionType = 'blackjack_player_busts';
                profitLamportsOutcome = -betAmountLamports; // For message display
                payoutAmountForDB = 0n; // Payout for DB status update
              // Escape ! .
                resultDisplayString = `💥 You Busted at ${escapeMarkdownV2(playerValue)}\\! Lost ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL\\.`; // Escaped ! .
                 // *** LOSS: No balance update needed here. finalUserBalance remains balanceAfterBet ***
                 console.log(`${logPrefix} Blackjack loss (Player Busts) for Bet ID ${betId}. Balance remains ${formatSol(finalUserBalance)} SOL after initial deduction.`);
                break;

            default: // Includes 'error_game_logic' potential state
                console.error(`${logPrefix} Unknown outcomeKey received in processBlackjackResult: ${outcomeKey}`);
              // Escape ()
                resultDisplayString = `Internal error determining winner \\(${escapeMarkdownV2(outcomeKey)}\\)\\.`; // Escaped ()
                profitLamportsOutcome = -betAmountLamports;
                payoutAmountForDB = 0n;
                dbBetStatus = 'error_game_logic'; ledgerTransactionType = 'blackjack_error';
                 // *** ERROR: No balance update needed here. finalUserBalance remains balanceAfterBet ***
                 console.log(`${logPrefix} Blackjack error outcome for Bet ID ${betId}. Balance remains ${formatSol(finalUserBalance)} SOL after initial deduction.`);
                break;
        }

        // Update bet status in DB
        await updateBetStatus(client, betId, dbBetStatus, payoutAmountForDB); // from Part 2
        await client.query('COMMIT'); // Commit transaction (includes balance update if applicable)

        // Construct final message
        let fullResultText = `♠️ *Blackjack Result* ♥️\n\nBet: ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL\n\n`;
      // Escape () in hand displays
        fullResultText += `*Your Hand \\(${escapeMarkdownV2(playerValue)}\\):* ${escapeMarkdownV2(playerHand.map(c => formatCard(c)).join(', '))}\n`; // formatCard from Part 4
        fullResultText += `*Dealer's Hand \\(${escapeMarkdownV2(dealerValue)}\\):* ${escapeMarkdownV2(dealerHand.map(c => formatCard(c)).join(', '))}\n\n`; // formatCard from Part 4
        fullResultText += `${resultDisplayString}\n\nNew Balance: ${escapeMarkdownV2(formatSol(finalUserBalance))} SOL`; // Displays B+P on win/push now

        const keyboard = { // Add Emojis
            inline_keyboard: [
                [{ text: '🔄 Play Again', callback_data: `play_again:${gameKey}:${betAmountLamports}` }, { text: '🎮 Games Menu', callback_data: 'menu:game_selection' }]
            ]
        };
        await bot.editMessageText(fullResultText, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: keyboard });

    } catch (error) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} Result DB Rollback failed:`, rbErr));
        console.error(`${logPrefix} Error in Blackjack result processing:`, error);
        const errorText = `⚠️ Error finalizing Blackjack game: ${escapeMarkdownV2(error.message)}\\. Your bet may be unresolved\\. Please contact support with Bet ID ${betId}\\.`; // Escaped .
        bot.editMessageText(errorText, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2' }).catch(e => {});
    } finally {
        if (client) client.release();
    }
}
// --- End of Game Result Handlers ---


// --- End of Part 5a ---
// index.js - Part 5b: Telegram Command Handlers & Stateful Inputs (Section 1 of 2)
// --- VERSION: 3.2.1r --- (Applying Fixes: Ensuring custom bet routes correctly for all games)

// --- Assuming functions/variables from Part 1, 2, 3, 5a are available ---
// (bot, pool, userStateCache, GAME_CONFIG, ensureUserExists, updateUserWallet,
//  getWalletCache, updateWalletCache, formatSol, escapeMarkdownV2, safeSendMessage,
//  generateReferralCode, createDepositAddressRecord, getNextDepositAddressIndex,
//  generateUniqueDepositAddress, addActiveDepositAddressCache, addPayoutJob,
//  getUserBalance, getUserReferralStats, getUserGameHistory, getLeaderboardData,
//  getJackpotAmount, LAMPORTS_PER_SOL, SOL_DECIMALS, MIN_WITHDRAWAL_LAMPORTS, etc.)
// --- USER_STATE_TTL_MS from Part 1.
// --- proceedToGameStep from Part 5a is used here.
// --- clearUserState is from Part 6 ---

// --- Stateful Input Router ---
/**
 * Routes incoming messages to the appropriate stateful input handler.
 * @param {import('node-telegram-bot-api').Message} msg The Telegram message object.
 * @param {object} currentState The user's current state object from userStateCache.
 */
async function routeStatefulInput(msg, currentState) {
    // Check if message is valid and has sender ID
    if (!msg || !msg.from || !msg.from.id || !msg.chat || !msg.chat.id) {
        console.warn('[routeStatefulInput] Received invalid message object:', msg);
        return;
    }
    const userId = String(msg.from.id); // User ID from the message sender
    const chatId = String(msg.chat.id);
    const text = msg.text || '';
    // Use 'state' primarily, fallback to 'action' if 'state' is missing (older state structure?)
    const stateName = currentState.state || currentState.action;
    const logPrefix = `[StatefulInput User ${userId} State ${stateName}]`;
    console.log(`${logPrefix} Routing stateful input. Message: "${text.substring(0,30)}..."`);

    // Always ensure the user exists before processing stateful input
    let tempClient = null;
    try {
        tempClient = await pool.connect(); // pool from Part 1
        await ensureUserExists(userId, tempClient); // ensureUserExists from Part 2
    } catch (dbError) {
        console.error(`${logPrefix} DB error ensuring user exists for stateful input: ${dbError.message}`);
        // MarkdownV2 Safety: Escape static error message
        safeSendMessage(chatId, "A database error occurred\\. Please try again later\\.", { parse_mode: 'MarkdownV2'}); // Escaped . safeSendMessage from Part 3
        clearUserState(userId); // Clear state on DB error. clearUserState from Part 6
        if (tempClient) tempClient.release();
        return;
    } finally {
        if (tempClient) tempClient.release();
    }

    // Route based on the state name
    switch (stateName) {
        // Consolidated handling for all custom game amount inputs
        case 'awaiting_coinflip_amount':
        case 'awaiting_race_amount':
        case 'awaiting_slots_amount':
        case 'awaiting_roulette_amount':
        case 'awaiting_war_amount':
        case 'awaiting_crash_amount':
        case 'awaiting_blackjack_amount':
            return handleCustomAmountInput(msg, currentState); // Defined below in THIS section

        case 'awaiting_roulette_number':
            return handleRouletteNumberInput(msg, currentState); // Defined below in THIS section

        case 'awaiting_withdrawal_address':
            return handleWalletAddressInput(msg, currentState); // Defined below in THIS section

        case 'awaiting_withdrawal_amount':
            return handleWithdrawalAmountInput(msg, currentState); // Defined below in THIS section

        // Deprecated/Old state names (Optional: Add handling or let default catch it)
        // case 'awaiting_wallet_address': // Example if old states existed
        //     console.warn(`${logPrefix} Handling deprecated state 'awaiting_wallet_address'. Routing to handleWalletAddressInput.`);
        //     return handleWalletAddressInput(msg, currentState);

        default:
            console.warn(`${logPrefix} Unknown or unhandled state: ${stateName}. Clearing state.`);
            clearUserState(userId);
            // MarkdownV2 Safety: Escape static error message
            safeSendMessage(chatId, "Your previous action seems to have expired or was unclear\\. Please try again\\.", { parse_mode: 'MarkdownV2'}); // Escaped .
            return;
    }
}
// --- End Stateful Input Router ---


// --- Stateful Input Handlers (DEFINED ONLY HERE) ---

/** Handles custom bet amount input from the user. */
async function handleCustomAmountInput(msg, currentState) {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const text = msg.text ? msg.text.trim() : '';
    // Ensure currentState.data exists and has expected properties
    if (!currentState || !currentState.data || !currentState.data.gameKey || !GAME_CONFIG[currentState.data.gameKey]) { // GAME_CONFIG from Part 1
        console.error(`[CustomAmountInput User ${userId}] Invalid state data received:`, currentState);
        clearUserState(userId); // clearUserState from Part 6
        safeSendMessage(chatId, "An internal error occurred processing your input\\. Please try again\\.", { parse_mode: 'MarkdownV2'}); // Escaped . safeSendMessage from Part 3
        return;
    }
    const { gameKey, breadcrumb, originalMessageId } = currentState.data;
    const gameConfig = GAME_CONFIG[gameKey];
    const logPrefix = `[CustomAmount User ${userId} Game ${gameKey}]`;

    // Attempt to delete the original message with "Type amount..." prompt and the user's input message
    // Do this regardless of valid input to clean up chat
    if (originalMessageId && originalMessageId !== msg.message_id) { bot.deleteMessage(chatId, originalMessageId).catch(()=>{}); } // bot from Part 1
    bot.deleteMessage(chatId, msg.message_id).catch(()=>{});

    // Clear state immediately after receiving input, before further processing
    clearUserState(userId);

    let betAmountSOL = 0;
    try {
        betAmountSOL = parseFloat(text);
        if (isNaN(betAmountSOL) || betAmountSOL <= 0) throw new Error("Invalid number format or non\\-positive amount\\."); // Escaped - .
    } catch (parseError) {
        // Inform user of parsing error and provide options to go back
        // MarkdownV2 Safety: Escape error message, game name, min/max amounts, example, punctuation
        const errorText = `Invalid amount entered: "${escapeMarkdownV2(text)}"\\. Please enter a valid number \\(e\\.g\\., 0\\.1, 2\\.5\\)\\.\nMin: ${escapeMarkdownV2(formatSol(gameConfig.minBetLamports))} SOL, Max: ${escapeMarkdownV2(formatSol(gameConfig.maxBetLamports))} SOL\\. Or /cancel\\.`; // Escaped . () formatSol from Part 3
        safeSendMessage(chatId, errorText, {
            parse_mode: 'MarkdownV2',
            // Add Emoji
            // Provide button to go back to standard bet selection for the same game
            reply_markup: { inline_keyboard: [[{ text: `↩️ Back to ${escapeMarkdownV2(gameConfig.name)} Bet Selection`, callback_data: `select_game:${gameKey}` }]] }
        });
        return;
    }

    const betAmountLamports = BigInt(Math.floor(betAmountSOL * Number(LAMPORTS_PER_SOL))); // LAMPORTS_PER_SOL from Part 1

    // Validate amount against game limits
    // Needs gameConfig
    if (betAmountLamports < gameConfig.minBetLamports || betAmountLamports > gameConfig.maxBetLamports) {
        const boundsErrorText = `Amount ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL is outside the allowed range for ${escapeMarkdownV2(gameConfig.name)}\\.\nMin: ${escapeMarkdownV2(formatSol(gameConfig.minBetLamports))} SOL, Max: ${escapeMarkdownV2(formatSol(gameConfig.maxBetLamports))} SOL\\. Or /cancel\\.`; // Escaped . ()
        safeSendMessage(chatId, boundsErrorText, {
            parse_mode: 'MarkdownV2',
            // Add Emoji
            reply_markup: { inline_keyboard: [[{ text: `↩️ Back to ${escapeMarkdownV2(gameConfig.name)} Bet Selection`, callback_data: `select_game:${gameKey}` }]] }
        });
        return;
    }

    // Amount is valid. Check balance.
    const balanceLamports = await getUserBalance(userId); // getUserBalance from Part 2
    if (balanceLamports < betAmountLamports) {
        const insufficientBalanceText = `⚠️ Insufficient balance for a ${escapeMarkdownV2(formatSol(betAmountLamports))} SOL bet on ${escapeMarkdownV2(gameConfig.name)}\\. Your balance is ${escapeMarkdownV2(formatSol(balanceLamports))} SOL\\. Or /cancel\\.`; // Escaped .
        safeSendMessage(chatId, insufficientBalanceText, {
            parse_mode: 'MarkdownV2',
            // Add Emojis
            // Offer deposit and back to bet selection
            reply_markup: { inline_keyboard: [[{ text: '💰 Quick Deposit', callback_data: 'quick_deposit' }], [{ text: `↩️ Back to ${escapeMarkdownV2(gameConfig.name)} Bet Selection`, callback_data: `select_game:${gameKey}` }]] }
        });
        return;
    }

    // Balance sufficient, amount valid. Proceed to game-specific confirmation or next step.

    // Update last bet amount preference (in memory and DB)
    // userLastBetAmounts from Part 1 state
    const userBets = userLastBetAmounts.get(userId) || new Map();
    userBets.set(gameKey, betAmountLamports);
    userLastBetAmounts.set(userId, userBets);
    // Update DB async, don't block flow - pool from Part 1
    updateUserLastBetAmount(userId, gameKey, betAmountLamports, pool) // updateUserLastBetAmount from Part 2
        .catch(e => console.warn(`${logPrefix} Non-critical error updating last bet amount in DB: ${e.message}`));


    const currentAmountSOL = formatSol(betAmountLamports);
    const currentBreadcrumb = breadcrumb || gameConfig.name; // Use breadcrumb from state if available

    // --- FIX #4: Ensure routing works for all games ---
    // Route to next step based on game type
    // Needs gameKey
    if (gameKey === 'coinflip' || gameKey === 'race' || gameKey === 'roulette') {
        // These games have intermediate steps (choose side/horse/bet type)
        // Call proceedToGameStep (from Part 5a) to display the next set of choices
        // Determine the correct initial callback data for the next step
        let nextStepCallbackData = '';
        if (gameKey === 'coinflip') nextStepCallbackData = `coinflip_select_side:${betAmountLamports}`;
        else if (gameKey === 'race') nextStepCallbackData = `race_select_horse:${betAmountLamports}`;
        else if (gameKey === 'roulette') nextStepCallbackData = `roulette_select_bet_type:${betAmountLamports}`;

        // Send a *new* message for the next step, as originals were deleted. Pass null for messageId.
        console.log(`${logPrefix} Custom amount valid. Proceeding to intermediate step for ${gameKey}. Callback: ${nextStepCallbackData}`);
        return proceedToGameStep(userId, chatId, null, gameKey, nextStepCallbackData); // Pass null messageId

    } else if (gameKey === 'slots' || gameKey === 'war' || gameKey === 'crash' || gameKey === 'blackjack') {
        // These games can proceed directly to confirmation after amount selection
        // Needs gameConfig, currentAmountSOL, gameKey, betAmountLamports
        console.log(`${logPrefix} Custom amount valid. Proceeding directly to confirmation for ${gameKey}.`);
        const confirmationMessage = `${escapeMarkdownV2(currentBreadcrumb)} > Bet ${escapeMarkdownV2(currentAmountSOL)} SOL\nConfirm bet on ${escapeMarkdownV2(gameConfig.name)}?`;
        const inlineKeyboard = [ // Add Emojis
            [{ text: `✅ Yes, Bet ${escapeMarkdownV2(currentAmountSOL)} SOL`, callback_data: `confirm_bet:${gameKey}:${betAmountLamports}` }],
            // Add back button to allow changing amount or cancelling
            [{ text: '✏️ Change Amount', callback_data: `select_game:${gameKey}` }, { text: '❌ Cancel', callback_data: 'menu:game_selection' }]
        ];
        // Send as a new message
        safeSendMessage(chatId, confirmationMessage, { parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: inlineKeyboard } });
    } else {
        // Fallback for any potentially unhandled game keys
        console.error(`${logPrefix} Unhandled gameKey '${gameKey}' in custom amount routing logic.`);
        safeSendMessage(chatId, `⚠️ Internal error: Unhandled game type '${escapeMarkdownV2(gameKey)}' for custom bet.`, { parse_mode: 'MarkdownV2' });
    }
}

/** Handles Roulette straight up number input from the user. */
async function handleRouletteNumberInput(msg, currentState) {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const text = msg.text ? msg.text.trim() : '';
    // Ensure state consistency
    if (!currentState || !currentState.data || currentState.data.gameKey !== 'roulette' || (currentState.state !== 'awaiting_roulette_number' && currentState.action !== 'awaiting_roulette_number') || !currentState.data.betAmountLamportsStr) {
        console.error(`[RouletteNumberInput User ${userId}] Invalid state data received:`, currentState);
        clearUserState(userId);
        safeSendMessage(chatId, "An internal error occurred processing your number input\\. Please try again\\.", { parse_mode: 'MarkdownV2'}); // Escaped .
        return;
    }
    const betAmountLamportsBigInt = BigInt(currentState.data.betAmountLamportsStr);
    const { gameKey, breadcrumb, originalMessageId } = currentState.data;
    const logPrefix = `[RouletteNumberInput User ${userId}]`;

    // Delete prompt and user input messages
    if (originalMessageId && originalMessageId !== msg.message_id) { bot.deleteMessage(chatId, originalMessageId).catch(()=>{}); }
    bot.deleteMessage(chatId, msg.message_id).catch(()=>{});

    clearUserState(userId); // Clear state now

    // Validate the number input
    const betValueNum = parseInt(text, 10);
    if (isNaN(betValueNum) || betValueNum < 0 || betValueNum > 36) {
        // MarkdownV2 Safety: Escape everything
        const errorText = `${escapeMarkdownV2(breadcrumb || 'Roulette')} > Straight Up Bet\nInvalid number: "${escapeMarkdownV2(text)}"\\. Please enter a number between 0 and 36\\. Or /cancel\\.`; // Escaped .
        // Send new message as originals were deleted
        safeSendMessage(chatId, errorText, {
            parse_mode: 'MarkdownV2',
            // Add Emoji
            // Changed back button to retry straight bet number input directly
            // Use the same callback data that initially prompted for the number
            reply_markup: { inline_keyboard: [[{ text: '✏️ Try Again', callback_data: `roulette_select_bet_type:straight:${betAmountLamportsBigInt}` }]] }
        });
        return;
    }

    // Valid number, proceed to confirmation
    const betValueString = String(betValueNum);
    const amountSOL = formatSol(betAmountLamportsBigInt); // formatSol from Part 3
    // MarkdownV2 Safety: Escape everything
    const confirmationMessage = `${escapeMarkdownV2(breadcrumb || 'Roulette')} \\> ${escapeMarkdownV2(betValueString)}\nBet ${escapeMarkdownV2(amountSOL)} SOL on *Straight Up: ${escapeMarkdownV2(betValueString)}*\\?`; // Escaped > and ?
    const inlineKeyboard = [ // Add Emojis
        // Callback includes game, amount, bet type ('straight'), and bet value (the number)
        [{ text: `✅ Yes, Confirm Bet`, callback_data: `confirm_bet:roulette:${betAmountLamportsBigInt}:straight:${betValueString}` }],
        // Button to go back to the number prompt
        [{ text: '✏️ Change Number', callback_data: `roulette_select_bet_type:straight:${betAmountLamportsBigInt}` }],
        [{ text: '❌ Cancel & Exit', callback_data: 'menu:game_selection' }] // Go back to game selection
    ];
    // Send new message
    safeSendMessage(chatId, confirmationMessage, { parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: inlineKeyboard } });
}

/** Handles user input when bot is expecting a wallet address for linking */
async function handleWalletAddressInput(msg, currentState) {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const potentialNewAddress = msg.text ? msg.text.trim() : '';
    // Ensure state consistency
    if (!currentState || !currentState.data || (currentState.state !== 'awaiting_withdrawal_address' && currentState.action !== 'awaiting_withdrawal_address')) {
        console.error(`[WalletAddrInput User ${userId}] Invalid state data received:`, currentState);
        clearUserState(userId);
        safeSendMessage(chatId, "An internal error occurred processing your address input\\. Please try again\\.", { parse_mode: 'MarkdownV2'}); // Escaped .
        return;
    }
    const { breadcrumb, originalMessageId } = currentState.data; // `breadcrumb` might be useful context
    const logPrefix = `[WalletAddrInput User ${userId}]`;

    // Delete user's message containing the address and the original prompt message
    if (originalMessageId && originalMessageId !== msg.message_id) { bot.deleteMessage(chatId, originalMessageId).catch(()=>{}); }
    bot.deleteMessage(chatId, msg.message_id).catch(()=>{});

    clearUserState(userId); // Clear state now

    // Send a temporary "Linking..." message and get its ID to edit later
    const sentMsg = await safeSendMessage(chatId, `🔗 Linking wallet \`${escapeMarkdownV2(potentialNewAddress)}\`\\.\\.\\.`, { parse_mode: 'MarkdownV2' }); // Escaped . ... Add Emoji
    if (!sentMsg) {
        console.error(`${logPrefix} Failed to send 'Linking...' message.`);
        // Attempt to send a generic error if the linking message failed
        safeSendMessage(chatId, `⚠️ Error initiating wallet link process. Please try again.`, { parse_mode: 'MarkdownV2' });
        return;
    }
    const messageToEditId = sentMsg.message_id; // Edit this new message with the result

    try {
        new PublicKey(potentialNewAddress); // Validate address format before DB (PublicKey from Part 1)
        const linkResult = await linkUserWallet(userId, potentialNewAddress); // from Part 2
        if (!linkResult.success) {
            // Use the error message from linkUserWallet if available
            throw new Error(linkResult.error || "Failed to link wallet in database.");
        }

        // MarkdownV2 Safety: Escape address, referral code
        const successMsg = `✅ Wallet \`${escapeMarkdownV2(linkResult.wallet)}\` successfully linked\\.\nYour referral code: \`${escapeMarkdownV2(linkResult.referralCode || 'Generated!')}\``; // Escaped . !
        bot.editMessageText(successMsg, { chat_id: chatId, message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: '↩️ Back to Wallet Menu', callback_data: 'menu:wallet' }]] }}); // Add Emoji

    } catch (e) {
        console.error(`${logPrefix} Error linking wallet: ${e.message}`);
        // MarkdownV2 Safety: Escape everything
        const errorText = `⚠️ Invalid Solana address or failed to save: "${escapeMarkdownV2(potentialNewAddress)}"\\. Error: ${escapeMarkdownV2(e.message)}\\. Please enter a valid Solana address or use the \`/wallet <address>\` command\\. Or /cancel\\.`; // Escaped . <>
        bot.editMessageText(errorText, {
            chat_id: chatId, message_id: messageToEditId, parse_mode: 'MarkdownV2',
            // Add Emoji
            reply_markup: { inline_keyboard: [[{ text: '↩️ Back to Wallet', callback_data: 'menu:wallet' }]] }
        });
    }
}

/** Handles user input when bot is expecting a withdrawal amount */
async function handleWithdrawalAmountInput(msg, currentState) {
    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const textAmount = msg.text ? msg.text.trim() : '';
    // Ensure state consistency
    if (!currentState || !currentState.data || !currentState.data.linkedWallet || !currentState.data.breadcrumb || !currentState.data.currentBalance || (currentState.state !== 'awaiting_withdrawal_amount' && currentState.action !== 'awaiting_withdrawal_amount')) {
        console.error(`[WithdrawAmountInput User ${userId}] Invalid state data received:`, currentState);
        clearUserState(userId);
        safeSendMessage(chatId, "An internal error occurred processing your amount input \\(context missing\\)\\. Please start withdrawal again\\.", { parse_mode: 'MarkdownV2'}); // Escaped () .
        return;
    }
    const { linkedWallet, breadcrumb, originalMessageId, currentBalance: currentBalanceStr } = currentState.data;
    const currentBalance = BigInt(currentBalanceStr);
    const logPrefix = `[WithdrawAmountInput User ${userId}]`;

    // Delete prompt and user input
    if (originalMessageId && originalMessageId !== msg.message_id) { bot.deleteMessage(chatId, originalMessageId).catch(()=>{}); }
    bot.deleteMessage(chatId, msg.message_id).catch(()=>{});

    clearUserState(userId); // Clear state now

    let amountSOL = 0;
    try {
        amountSOL = parseFloat(textAmount);
        if (isNaN(amountSOL) || amountSOL <= 0) throw new Error("Invalid number format or non\\-positive amount\\."); // Escaped . -

        const amountLamports = BigInt(Math.floor(amountSOL * Number(LAMPORTS_PER_SOL))); // LAMPORTS_PER_SOL from Part 1
        const feeLamports = BigInt(process.env.WITHDRAWAL_FEE_LAMPORTS || '5000'); // From Part 1 constants
        const totalDeductionLamports = amountLamports + feeLamports;

        // Validate against minimum withdrawal amount
        if (amountLamports < MIN_WITHDRAWAL_LAMPORTS) { // From Part 1 constants
            // MarkdownV2 Safety: Escape everything
            const minErrorText = `${escapeMarkdownV2(breadcrumb)}\nAmount ${escapeMarkdownV2(formatSol(amountLamports))} SOL is less than the minimum withdrawal of ${escapeMarkdownV2(formatSol(MIN_WITHDRAWAL_LAMPORTS))} SOL\\. Please try again or /cancel\\.`; // Escaped .
            safeSendMessage(chatId, minErrorText, {
                parse_mode: 'MarkdownV2',
                // Add Emoji
                // Send as new message, provide option to go back to Wallet menu
                reply_markup: { inline_keyboard: [[{ text: '↩️ Back to Wallet', callback_data: 'menu:wallet' }]] }
            });
            return;
        }

        // Validate against user's current balance
        if (currentBalance < totalDeductionLamports) {
            // MarkdownV2 Safety: Escape everything
            const insufficientText = `${escapeMarkdownV2(breadcrumb)}\nInsufficient balance for withdrawal of ${escapeMarkdownV2(formatSol(amountLamports))} SOL \\(Total needed including fee: ${escapeMarkdownV2(formatSol(totalDeductionLamports))} SOL\\)\\. Your balance: ${escapeMarkdownV2(formatSol(currentBalance))} SOL\\. The fee is ${escapeMarkdownV2(formatSol(feeLamports))} SOL\\. Or /cancel\\.`; // Escaped . ()
            safeSendMessage(chatId, insufficientText, {
                parse_mode: 'MarkdownV2',
                // Add Emoji
                // Send as new message, provide option to go back to Wallet menu
                reply_markup: { inline_keyboard: [[{ text: '↩️ Back to Wallet', callback_data: 'menu:wallet' }]] }
            });
            return;
        }

        // Amount valid and balance sufficient, show confirmation button
        const amountSOLFormatted = escapeMarkdownV2(formatSol(amountLamports));
        const feeSOLFormatted = escapeMarkdownV2(formatSol(feeLamports));
        const totalSOLFormatted = escapeMarkdownV2(formatSol(totalDeductionLamports));
        const addressFormatted = escapeMarkdownV2(linkedWallet);
        const finalBreadcrumb = `${escapeMarkdownV2(breadcrumb)} > Confirm ${amountSOLFormatted} SOL`; // Escaped >

        // MarkdownV2 Safety: Escape everything
        const confirmationText = `*Confirm Withdrawal*\n\n` +
                                 `${finalBreadcrumb}\n` +
                                 `Amount: \`${amountSOLFormatted} SOL\`\n` +
                                 `Fee: \`${feeSOLFormatted} SOL\`\n` +
                                 `Total Deducted: \`${totalSOLFormatted} SOL\`\n` +
                                 `Recipient: \`${addressFormatted}\`\n\n` +
                                 `Proceed?`;

        // Pass necessary info to the callback handler
        const callbackData = `confirm_withdrawal:${linkedWallet}:${amountLamports.toString()}`;
        const inlineKeyboard = [ // Add Emojis
            [{ text: '✅ Yes, Confirm Withdrawal', callback_data: callbackData }],
            [{ text: '❌ Cancel', callback_data: 'menu:wallet' }] // Go back to wallet menu
        ];
        // Send confirmation as a new message
        safeSendMessage(chatId, confirmationText, { parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: inlineKeyboard } });

    } catch (e) {
        console.error(`${logPrefix} Error processing withdrawal amount: ${e.message}`);
        // MarkdownV2 Safety: Escape text, error message
        const errorText = `${escapeMarkdownV2(breadcrumb)}\nInvalid amount: "${escapeMarkdownV2(textAmount)}"\\. ${escapeMarkdownV2(e.message)}\\.\nPlease enter a valid withdrawal amount or /cancel\\.`; // Escaped .
        safeSendMessage(chatId, errorText, {
            parse_mode: 'MarkdownV2',
            // Add Emoji
            // Send as new message, provide option to go back to Wallet menu
            reply_markup: { inline_keyboard: [[{ text: '↩️ Back to Wallet', callback_data: 'menu:wallet' }]] }
        });
    }
}
// --- End Stateful Input Handlers ---

// --- End of Part 5b (Section 1) ---
// index.js - Part 5b: General Commands, Game Commands, Menus & Maps (Section 2a of 4) - CORRECTED
// --- VERSION: 3.2.1s --- (Applying Fixes: CORRECTED '>' escape in Roulette setup, Verifying Start Cmd Escaping, Referral/Help Parse Errors, Deposit Hint Format, Link Wallet Escaping, Ref Link Display, Error Recovery, Balance on Start, Start Animation + previous)

// (Continuing directly from Part 5b, Section 1)
// ... (Assume functions like routeStatefulInput, handleCustomAmountInput, etc. are defined above in Section 1) ...
// ... (Assume other dependencies like bot, pool, GAME_CONFIG, utils, DB functions etc., are available) ...
// --- clearUserState is from Part 6 ---
// --- showBetAmountButtons is from Part 1 ---

// --- Specific Menu/Action Helper Functions (Belong with Command/Menu Logic) ---

/**
 * Initiates the process for a user to enter a custom bet amount for a game.
 * Sets user state to 'awaiting_<gameKey>_amount'.
 * @param {string} userId
 * @param {string} chatId
 * @param {number} messageId The ID of the message to edit with the prompt.
 * @param {string} gameKey The key of the game for which the amount is being selected.
 */
async function handleCustomAmountSelection(userId, chatId, messageId, gameKey) {
    const gameConfig = GAME_CONFIG[gameKey]; // GAME_CONFIG from Part 1
    if (!gameConfig) {
        console.error(`[CustomAmountSelect] Invalid gameKey: ${gameKey} for user ${userId}`);
        // MarkdownV2 Safety: Escape key
        bot.editMessageText(`⚠️ Invalid game specified: \`${escapeMarkdownV2(gameKey)}\``, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2'});
        return;
    }

    const breadcrumb = `${escapeMarkdownV2(gameConfig.name)} \\> Custom Amount`; // Escape > as \\> here
    // MarkdownV2 Safety: Escape breadcrumb, game name, min/max, punctuation
    const promptText = `${breadcrumb}\nPlease enter your desired bet amount for ${escapeMarkdownV2(gameConfig.name)} \\(e\\.g\\., 0\\.1, 2\\.5\\)\\.\nMin: ${escapeMarkdownV2(formatSol(gameConfig.minBetLamports))} SOL, Max: ${escapeMarkdownV2(formatSol(gameConfig.maxBetLamports))} SOL\\. Or /cancel\\.`; // Escaped . () formatSol from Part 3
    const keyboard = { // Add Emojis
        // Go back to the standard bet amount buttons for this game
        inline_keyboard: [[{ text: '↩️ Back to Bet Selection', callback_data: `select_game:${gameKey}` }]]
    };

    let messageToSetStateWith = messageId; // Assume we edit the original message initially

    // Edit the message that triggered this state
    await bot.editMessageText(promptText, {
        chat_id: chatId,
        message_id: messageId,
        reply_markup: keyboard,
        parse_mode: 'MarkdownV2'
    }).catch(async (e) => { // Make catch async
        // If edit fails (e.g., message too old, deleted), send as new message
        if (!e.message.includes("message is not modified")) {
            console.warn(`[CustomAmountSelect] Failed to edit message ${messageId}, sending new. Error: ${e.message}`);
            // Send as a new message if edit fails
            const sentMsg = await safeSendMessage(chatId, promptText, { reply_markup: keyboard, parse_mode: 'MarkdownV2' }); // safeSendMessage from Part 3
            messageToSetStateWith = sentMsg?.message_id; // Use the new message ID for state
        }
    });

    // Set user state using the ID of the message that was actually presented (edited or new)
    if (messageToSetStateWith) {
        userStateCache.set(userId, {
            state: `awaiting_${gameKey}_amount`, // Specific state for each game
            chatId: String(chatId),
            messageId: messageToSetStateWith, // Use the correct message ID
            data: { gameKey, breadcrumb, originalMessageId: messageToSetStateWith }, // Pass gameKey and breadcrumb
            timestamp: Date.now()
        });
    } else {
        console.error(`[CustomAmountSelect] Could not determine message ID for state setting for user ${userId}. State not set.`);
        // Optionally inform the user
        safeSendMessage(chatId, "Error setting up custom amount input. Please try again.", {});
    }
}


/**
 * Sets up the state for a user to input a number for a Roulette straight bet.
 * @param {string} userId
 * @param {string} chatId
 * @param {number | null} messageId The ID of the message to edit (can be null if sending new).
 * @param {string} gameKey Should always be 'roulette'.
 * @param {string} betAmountLamportsStr The bet amount chosen previously.
 */
async function handleRouletteStraightBetSetup(userId, chatId, messageId, gameKey, betAmountLamportsStr) {
    const gameConfig = GAME_CONFIG[gameKey]; // GAME_CONFIG from Part 1
    // *** CORRECTED '>' escaping in breadcrumb ***
    const breadcrumb = `${escapeMarkdownV2(gameConfig.name)} \\> Straight Up Bet`; // Escape > as \\> here
    // MarkdownV2 Safety: Escape breadcrumb, amount
    const promptText = `${breadcrumb}\nBetting ${escapeMarkdownV2(formatSol(BigInt(betAmountLamportsStr)))} SOL on a Straight Up number\\.\nPlease type the number you want to bet on \\(0\\-36\\), or /cancel\\:`; // Escaped . () - :
    const keyboard = { // Add Emoji
        // Go back to the bet type selection screen for the same amount
        inline_keyboard: [[{ text: '↩️ Back to Bet Types', callback_data: `roulette_select_bet_type:${betAmountLamportsStr}` }]]
    };

    let effectiveMessageId = messageId;

    // Try to edit, if fails, send new message and update messageId for state
    if (messageId) {
        await bot.editMessageText(promptText, {
            chat_id: chatId,
            message_id: messageId,
            reply_markup: keyboard,
            parse_mode: 'MarkdownV2'
        }).catch(async (e) => { // Make catch async
            if (!e.message.includes("message is not modified")) {
                console.warn(`[RouletteStraightSetup] Failed to edit msg ${messageId}, sending new. Err: ${e.message}`);
                // Log the specific error if it's a parse error
                if (e.message?.toLowerCase().includes("can't parse entities")) {
                    console.error(`[RouletteStraightSetup] PARSE ERROR ON EDIT/SEND: Text: ${promptText}`);
                }
                const sentMsg = await safeSendMessage(chatId, promptText, { reply_markup: keyboard, parse_mode: 'MarkdownV2' });
                effectiveMessageId = sentMsg?.message_id || null; // Update effectiveMessageId if new message sent
            }
        });
    } else {
        // If no original messageId, send a new one
        const sentMsg = await safeSendMessage(chatId, promptText, { reply_markup: keyboard, parse_mode: 'MarkdownV2' });
        effectiveMessageId = sentMsg?.message_id || null;
    }

    // Set user state only if we have a message context (effectiveMessageId is not null)
    if (effectiveMessageId) {
        userStateCache.set(userId, {
            state: 'awaiting_roulette_number', // Use 'state' key
            chatId: String(chatId),
            messageId: effectiveMessageId, // Use the ID of the message actually displayed
            data: {
                gameKey,
                action: 'awaiting_roulette_number', // Keep action in data for reference
                betAmountLamportsStr,
                breadcrumb, // Store the escaped breadcrumb used in the prompt
                originalMessageId: effectiveMessageId // Store the ID of the prompt message
            },
            timestamp: Date.now()
        });
    } else {
        console.error(`[RouletteStraightSetup] Failed to get a message ID for user ${userId}. State not set.`);
        // Optionally send an error message to the user
        safeSendMessage(chatId, "Error setting up number input. Please try again.", {});
    }
}

/**
 * Handles the confirmation (or cancellation) of a withdrawal request.
 * Called from a callback query. Deducts balance *before* queueing payout.
 * @param {string} userId
 * @param {string} chatId
 * @param {number} messageId
 * @param {string} recipientAddress The address to send to.
 * @param {string} amountLamportsStr The amount in lamports as a string.
 * @param {boolean} isConfirmed True if confirmed, false if cancelled.
 */
async function handleWithdrawalConfirmation(userId, chatId, messageId, recipientAddress, amountLamportsStr, isConfirmed) {
    const logPrefix = `[WithdrawConfirm User ${userId} Confirm ${isConfirmed}]`;
    console.log(`${logPrefix} Address: ${recipientAddress}, AmountStr: ${amountLamportsStr}`);

    if (!isConfirmed) {
        // MarkdownV2 Safety: Escape static text
        await bot.editMessageText("🚫 Withdrawal cancelled by user\\.", { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{text: "↩️ Back to Wallet", callback_data: "menu:wallet"}]] } }); // Escaped . Add Emoji
        return;
    }

    try {
        const amountLamports = BigInt(amountLamportsStr);
        const feeLamports = BigInt(process.env.WITHDRAWAL_FEE_LAMPORTS || '5000'); // From Part 1 constants
        const totalDeductionLamports = amountLamports + feeLamports;

        // Final balance check BEFORE creating DB record and adding to queue
        const currentBalance = await getUserBalance(userId); // from Part 2
        if (currentBalance < totalDeductionLamports) {
            // MarkdownV2 Safety: Escape amounts, fee
            const insufficientText = `⚠️ Insufficient balance for withdrawal\\. Needed: ${escapeMarkdownV2(formatSol(totalDeductionLamports))} SOL, Have: ${escapeMarkdownV2(formatSol(currentBalance))} SOL\\. Withdrawal cancelled\\.`; // Escaped .
            return bot.editMessageText(insufficientText, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{text: "↩️ Back to Wallet", callback_data: "menu:wallet"}]] } }); // Add Emoji
        }

        // Edit message to "Processing..."
        await bot.editMessageText("⏳ Processing your withdrawal request\\.\\.\\. This may take a moment\\.", { // Escaped . ...
            chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [] }
        }).catch(e => console.warn(`${logPrefix} Error editing message for processing state: ${e.message}`));

        // Create withdrawal record in DB (status 'pending') BEFORE deducting balance
        const withdrawalRequest = await createWithdrawalRequest(userId, amountLamports, feeLamports, recipientAddress); // from Part 2
        if (!withdrawalRequest.success || !withdrawalRequest.withdrawalId) {
            // MarkdownV2 Safety: Escape error
            throw new Error(`Failed to create withdrawal record: ${escapeMarkdownV2(withdrawalRequest.error || 'DB error')}`);
        }
        const withdrawalId = withdrawalRequest.withdrawalId;

        // Now deduct balance from user's internal account within a transaction
        let deductClient = null;
        try {
            deductClient = await pool.connect();
            await deductClient.query('BEGIN');
            const balanceUpdateResult = await updateUserBalanceAndLedger(
                deductClient, userId, -totalDeductionLamports, // Deduct amount + fee
                'withdrawal_initiated', { withdrawalId }, `Withdrawal ID ${withdrawalId} initiated`
            );
            if (!balanceUpdateResult.success) {
                await deductClient.query('ROLLBACK'); // Rollback deduction
                // Also mark the withdrawal request as failed in DB since balance couldn't be deducted
                await updateWithdrawalStatus(withdrawalId, 'failed', null, null, `Failed balance deduction: ${balanceUpdateResult.error}`); // from Part 2
                throw new Error(`Failed to deduct balance for withdrawal: ${escapeMarkdownV2(balanceUpdateResult.error || 'Balance error')}`);
            }
            await deductClient.query('COMMIT'); // Commit deduction
        } catch (deductError) {
            if (deductClient) await deductClient.query('ROLLBACK').catch(()=>{}); // Ensure rollback on error
            // Mark withdrawal failed if deduction failed
            await updateWithdrawalStatus(withdrawalId, 'failed', null, null, `Failed balance deduction: ${deductError.message}`);
            throw deductError; // Re-throw to outer catch
        } finally {
            if (deductClient) deductClient.release();
        }

        // Add job to payout queue ONLY AFTER successful deduction
        await addPayoutJob({ type: 'payout_withdrawal', withdrawalId: withdrawalId, userId }); // addPayoutJob from Part 6

        console.log(`${logPrefix} Withdrawal ID ${withdrawalId} for ${formatSol(amountLamports)} SOL to ${recipientAddress} queued for processing.`);
        // MarkdownV2 Safety: Escape amount, address
        const queuedMessage = `✅ Your withdrawal request for ${escapeMarkdownV2(formatSol(amountLamports))} SOL to \`${escapeMarkdownV2(recipientAddress)}\` has been queued and will be processed shortly\\.\nYou will receive another message upon completion\\.`; // Escaped .
        // Update the message again (or send new if edit failed)
        bot.editMessageText(queuedMessage, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{text: "↩️ Back to Wallet", callback_data: "menu:wallet"}]] }}) // Add Emoji
            .catch(e => safeSendMessage(chatId, queuedMessage, {parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{text: "↩️ Back to Wallet", callback_data: "menu:wallet"}]] }})); // Add Emoji

    } catch (error) {
        console.error(`${logPrefix} Error during withdrawal confirmation: ${error.message}`);
        // MarkdownV2 Safety: Escape error message
        const errorMsg = `❌ Error processing withdrawal confirmation: ${escapeMarkdownV2(error.message)}\\. Please try again\\.`; // Escaped .
        bot.editMessageText(errorMsg, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{text: "↩️ Back to Wallet", callback_data: "menu:wallet"}]] }}) // Add Emoji
            .catch(e => safeSendMessage(chatId, errorMsg, {parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{text: "↩️ Back to Wallet", callback_data: "menu:wallet"}]] }})); // Add Emoji
    }
}


// --- General Command Handlers (/start, /help, /wallet, etc.) ---
// All command handlers accept msgOrCbMsg, args, and optional correctUserIdFromCb

/**
 * Handles the /start command or 'menu:main' callback. Displays welcome message and main menu.
 * @param {import('node-telegram-bot-api').Message | import('node-telegram-bot-api').CallbackQuery['message']} msgOrCbMsg The message object or callback message object.
 * @param {Array<string>} args Command arguments or callback parameters.
 * @param {string | null} [correctUserIdFromCb=null] User ID if called from callback.
 */
async function handleStartCommand(msgOrCbMsg, args, correctUserIdFromCb = null) {
    // *** Applying Fix #9: Display Balance on Main Menu ***
    // *** Applying Fix #10: Re-implement Start Animation with Caption+Fallback ***
    const userId = String(correctUserIdFromCb || msgOrCbMsg.from.id);
    const chatId = String(msgOrCbMsg.chat.id);
    const logPrefix = `[StartCmd User ${userId}]`;
    let isFromCallback = !!correctUserIdFromCb;
    // Use a known, reliable GIF URL for Solana theme
    const TENOR_GIF_URL = "https://media1.tenor.com/m/vFdyZ-CK6IYAAAAC/solana.gif";
    const MAX_CAPTION_LENGTH = 1024; // Telegram caption limit

    // Delete original message if invoked from callback to prevent clutter
    if (isFromCallback && msgOrCbMsg.message_id) {
        const messageIdToDelete = msgOrCbMsg.message_id;
        try {
            await bot.deleteMessage(chatId, messageIdToDelete); // bot from Part 1
            console.log(`${logPrefix} Deleted original message ${messageIdToDelete} as start is being re-invoked from callback.`);
        } catch (delErr) {
            // Non-critical if deletion fails (message might already be gone)
            console.warn(`${logPrefix} Non-critical error deleting original message ${messageIdToDelete}: ${delErr.message}`);
        }
    }

    console.log(`${logPrefix} Handling /start or menu:main.`);

    // Handle User Existence, Referrals, and Balance Fetch
    let client = null;
    let isNewUser = false;
    let currentBalance = 0n; // Default balance if fetch fails
    try {
        client = await pool.connect(); // pool from Part 1
        // Ensure user exists and lock rows potentially needed for referral linking
        await client.query('BEGIN');
        const userCheckResult = await ensureUserExists(userId, client); // ensureUserExists from Part 2
        isNewUser = userCheckResult.isNewUser;

        // Fetch Balance (Fix #9)
        const balanceCheck = await queryDatabase('SELECT balance_lamports FROM user_balances WHERE user_id = $1 FOR UPDATE', [userId], client); // Lock balance row too
        if (balanceCheck.rowCount > 0) {
            currentBalance = BigInt(balanceCheck.rows[0].balance_lamports || '0');
        } else {
             // This case should be handled by ensureUserExists creating the balance row
             console.warn(`${logPrefix} Balance record not found for user ${userId} after ensureUserExists call during start command.`);
             // Balance remains 0n
        }

        // Process potential referral code from '/start ref_...'
        const commandArgs = isFromCallback ? args : msgOrCbMsg.text?.split(' ').slice(1) || [];
        const refCodeArg = commandArgs.length > 0 && commandArgs[0].startsWith('ref_') ? commandArgs[0] : null;

        if (refCodeArg) {
            console.log(`${logPrefix} Processing referral code ${refCodeArg} for user ${userId}`);
            const refereeDetails = await getUserWalletDetails(userId, client); // Use client for transaction // getUserWalletDetails from Part 2
            // Only attempt link if user doesn't already have a referrer
            if (!refereeDetails?.referred_by_user_id) {
                const referrerInfo = await getUserByReferralCode(refCodeArg); // getUserByReferralCode from Part 2 (doesn't need client)
                if (referrerInfo && referrerInfo.user_id !== userId) {
                    // Use linkReferral which handles DB updates and checks within the transaction
                    const linkSuccess = await linkReferral(userId, referrerInfo.user_id, client); // linkReferral from Part 2
                    if (linkSuccess) {
                        console.log(`${logPrefix} Referral link successful: ${referrerInfo.user_id} -> ${userId} during start.`);
                        const referrerDisplayName = await getUserDisplayName(chatId, referrerInfo.user_id); // getUserDisplayName from Part 3
                        // Send notification outside the transaction
                        safeSendMessage(chatId, `👋 Welcome via ${referrerDisplayName || escapeMarkdownV2('a friend')}'s referral link\\! Your accounts are now linked\\.`, {parse_mode: 'MarkdownV2'});
                    } else {
                        // linkReferral handles cases where user is already referred or self-referral
                        console.log(`${logPrefix} Referral link failed or unnecessary (already referred / self-referral?).`);
                        // Send generic welcome, or specific message if needed
                        if (referrerInfo && referrerInfo.user_id === userId) {
                            safeSendMessage(chatId, "You can't use your own referral code\\!", {parse_mode: 'MarkdownV2'});
                        } else if (!referrerInfo) {
                            safeSendMessage(chatId, "Invalid referral code provided\\.", {parse_mode: 'MarkdownV2'});
                        }
                    }
                } else if (referrerInfo && referrerInfo.user_id === userId) {
                    // Self-referral attempt
                    safeSendMessage(chatId, "You can't use your own referral code\\!", {parse_mode: 'MarkdownV2'});
                } else if (!referrerInfo) {
                    // Invalid code
                    safeSendMessage(chatId, "Invalid referral code provided\\.", {parse_mode: 'MarkdownV2'});
                }
            } else {
                // User already has a referrer, ignore the ref code from /start link
                console.log(`${logPrefix} User ${userId} already referred by ${refereeDetails?.referred_by_user_id}. Ignoring start ref code ${refCodeArg}.`);
            }
        }
        // Commit transaction after potentially linking referral
        await client.query('COMMIT');

    } catch (error) {
        if (client) await client.query('ROLLBACK').catch(rbErr => console.error(`${logPrefix} Rollback failed:`, rbErr));
        console.error(`${logPrefix} DB error during start command (user/ref/balance): ${error.message}`);
        // Allow execution to continue to show welcome message if possible, but log error
    } finally { if (client) client.release(); }

    // Construct Welcome Message (Ensure meticulous escaping)
    const displayName = await getUserDisplayName(chatId, userId); // from Part 3
    const botName = escapeMarkdownV2(process.env.BOT_USERNAME || "SolanaGamblesBot"); // BOT_USERNAME from Part 1 env
    const botVersion = escapeMarkdownV2(BOT_VERSION || "3.2.1r"); // BOT_VERSION from Part 1
    const balanceString = escapeMarkdownV2(formatSol(currentBalance)); // formatSol from Part 3

    let welcomeMsg = `👋 Welcome, ${displayName}\\!\n\nI am ${botName} \\(v${botVersion}\\), your home for exciting on\\-chain games on Solana\\.\n\n`;
    welcomeMsg += `*Current Balance:* ${balanceString} SOL\n\n`; // Added Balance Display (Fix #9)
    if (isNewUser) {
        welcomeMsg += "Looks like you're new here\\!\\! Here's how to get started:\n1\\. Use \`/deposit\` to get your unique address\\.\n2\\. Send SOL to that address\\.\n3\\. Use the menu below to play games\\!\n\n"; // Escaped ! . \
    }
    welcomeMsg += "Use the menu below or type /help for a list of commands\\."; // Escaped .

    // Truncate if exceeds caption limit
    if (welcomeMsg.length > MAX_CAPTION_LENGTH) {
        const ellipsis = "... \\(message truncated\\)"; // Escaped ()
        welcomeMsg = welcomeMsg.substring(0, MAX_CAPTION_LENGTH - ellipsis.length) + ellipsis;
        console.warn(`${logPrefix} Welcome message truncated due to caption length limit.`);
    }

    const mainMenuKeyboard = {
        inline_keyboard: [
            [{ text: '🎮 Play Games', callback_data: 'menu:game_selection' }, { text: '👤 My Wallet', callback_data: 'menu:wallet' }], // Check callback data validity - seems OK
            [{ text: '🏆 Leaderboards', callback_data: 'leaderboard_nav:overall_wagered:0'}, { text: '👥 Referrals', callback_data: 'menu:referral'}], // Check callback data validity - seems OK
            [{ text: 'ℹ️ Help & Info', callback_data: 'menu:help' }]
        ]
    };

    // Attempt to send Animation + Caption + Keyboard (Fix #10 Revised)
    try {
        // Using sendAnimation for a more engaging start
        await bot.sendAnimation(chatId, TENOR_GIF_URL, {
            caption: welcomeMsg,
            parse_mode: 'MarkdownV2',
            reply_markup: mainMenuKeyboard
        });
         console.log(`${logPrefix} Sent start animation with caption and menu.`);
    } catch (sendError) {
        console.error(`${logPrefix} Failed to send start animation with caption: ${sendError.message}. Falling back to text message.`);
        // Fallback to sending text only if animation+caption fails
         try {
            // Use safeSendMessage for queued sending of the fallback
            await safeSendMessage(chatId, welcomeMsg, {
                parse_mode: 'MarkdownV2',
                reply_markup: mainMenuKeyboard
            });
            console.log(`${logPrefix} Sent fallback welcome text message and main menu.`);
        } catch (fallbackError) {
             console.error(`${logPrefix} Failed to send even fallback welcome message: ${fallbackError.message}.`);
             // Final fallback with no special formatting
             safeSendMessage(chatId, "Welcome! Use /help for commands.", {}).catch(()=>{});
        }
    }
}

// --- End of Part 5b (Section 2a) ---
// index.js - Part 5b: General Commands, Game Commands, Menus & Maps (Section 2b of 4)
// --- VERSION: 3.2.1r --- (Applying Fixes: Verifying Help/Wallet Cmd Escaping, Referral/Help Parse Errors, Deposit Hint Format, Link Wallet Escaping, Ref Link Display, Error Recovery, Balance on Start, Start Animation + previous)

// (Continuing directly from Part 5b, Section 2a)
// ... (Assume functions, dependencies etc. are available) ...

/**
 * Handles the 'menu:game_selection' callback or /games command. Displays game selection buttons.
 * @param {import('node-telegram-bot-api').Message | import('node-telegram-bot-api').CallbackQuery['message']} msgOrCbMsg Message or callback message.
 * @param {Array<string>} args Command arguments or callback parameters.
 * @param {string | null} [correctUserIdFromCb=null] User ID if called from callback.
 */
async function handleGameSelectionCommand(msgOrCbMsg, args, correctUserIdFromCb = null) {
    const userId = String(correctUserIdFromCb || msgOrCbMsg.from.id);
    const chatId = String(msgOrCbMsg.chat.id);
    const logPrefix = `[GameSelectCmd User ${userId}]`;
    let messageToEditId = msgOrCbMsg.message_id;
    let isFromCallback = !!correctUserIdFromCb;

    console.log(`${logPrefix} Handling game selection menu.`);
    clearUserState(userId); // Clear any pending state like custom amount input

    const messageText = "🎮 *Choose a Game*";

    // Generate buttons dynamically from GAME_CONFIG
    const gameButtons = Object.values(GAME_CONFIG).map(game => { // GAME_CONFIG from Part 1
        let emoji = '🕹️'; // Default emoji
        // Assign specific emojis based on game key
        if (game.key === 'coinflip') emoji = '🪙';
        else if (game.key === 'slots') emoji = '🎰';
        else if (game.key === 'war') emoji = '🃏';
        else if (game.key === 'race') emoji = '🐎';
        else if (game.key === 'roulette') emoji = '⚪️';
        else if (game.key === 'crash') emoji = '🚀';
        else if (game.key === 'blackjack') emoji = '♠️';
        // Button text usually doesn't need escaping unless game names have special chars
        // Assuming game names are simple like "Coinflip", "Blackjack" etc.
        return { text: `${emoji} ${game.name}`, callback_data: `select_game:${game.key}` };
    });

    // Arrange buttons in rows (e.g., 2 per row)
    const gameKeyboardRows = [];
    for (let i = 0; i < gameButtons.length; i += 2) {
        gameKeyboardRows.push(gameButtons.slice(i, i + 2));
    }
    // Add back button
    gameKeyboardRows.push([{ text: '↩️ Back to Main Menu', callback_data: 'menu:main' }]);

    const options = { reply_markup: { inline_keyboard: gameKeyboardRows }, parse_mode: 'MarkdownV2' };

    // Edit the message if called from a callback, otherwise send a new one
    if (isFromCallback && messageToEditId) {
        bot.editMessageText(messageText, { chat_id: chatId, message_id: messageToEditId, ...options })
            .catch(e => {
                if (!e.message.includes("message is not modified")) {
                    console.warn(`${logPrefix} Failed to edit message ${messageToEditId} for game selection, sending new: ${e.message}`);
                    safeSendMessage(chatId, messageText, options); // safeSendMessage from Part 3
                }
            });
    } else {
        safeSendMessage(chatId, messageText, options);
    }
}


/**
 * Handles the /help command and callbacks for help sections.
 * @param {import('node-telegram-bot-api').Message | import('node-telegram-bot-api').CallbackQuery['message']} msgOrCbMsg Message or callback message.
 * @param {Array<string>} args args[0] is command or menu key, args[1] might be section.
 * @param {string | null} [correctUserIdFromCb=null] User ID if from callback.
 */
async function handleHelpCommand(msgOrCbMsg, args, correctUserIdFromCb = null) {
    // *** Applying Fix #3: Reconstruct lines with variables inside escaped parens (Verified Already Present) ***
    // *** Applying Fix #8: Improve Error Recovery (Verified Already Present) ***
    const userId = String(correctUserIdFromCb || msgOrCbMsg.from.id);
    const chatId = String(msgOrCbMsg.chat.id);
    const messageId = msgOrCbMsg.message_id;
    const isFromCallback = !!correctUserIdFromCb;
    // Determine section from args. If from callback, args[0] is section. If from command /help section, args[1] is section.
    const section = isFromCallback ? (args[0] || null) : (args.length > 1 ? args[1] : null);
    const logPrefix = `[HelpCmd User ${userId} Section ${section || 'main'}]`;
    clearUserState(userId); // Clear any previous state

    const fallbackKeyboard = { inline_keyboard: [[{ text: '↩️ Back to Main Menu', callback_data: 'menu:main' }]] }; // Fallback keyboard

    // If no section specified, show the main help menu
    if (!section) {
        const text = `ℹ️ *Help & Information*\n\nSelect a topic for more details:`;
        const keyboard = {
            inline_keyboard: [
                [{ text: '❓ How to Play', callback_data: 'show_help_section:how_to_play' }],
                [{ text: '💰 Deposits & Withdrawals', callback_data: 'show_help_section:deposits_withdrawals' }],
                [{ text: '🎲 Game Rules', callback_data: 'show_help_section:game_rules' }],
                [{ text: '⚖️ Fairness & House Edge', callback_data: 'show_help_section:fairness' }],
                [{ text: '👥 Referral Program', callback_data: 'menu:referral' }], // Go direct to referral menu
                [{ text: '📞 Support', callback_data: 'show_help_section:support' }],
                [{ text: '↩️ Back to Main Menu', callback_data: 'menu:main' }]
            ]
        };
        const options = { parse_mode: 'MarkdownV2', disable_web_page_preview: true, reply_markup: keyboard };
        if (isFromCallback && messageId) {
             // Edit the existing message
             bot.editMessageText(text, { chat_id: chatId, message_id: messageId, ...options })
                 .catch(e => { if (!e.message.includes("message is not modified")) safeSendMessage(chatId, text, options); });
        } else {
            // Send a new message if called via command
            safeSendMessage(chatId, text, options);
        }
        return;
    }

    // If a specific section is requested
    console.log(`${logPrefix} Displaying help section: ${section}`);
    let text = '';
    let keyboard = { inline_keyboard: [[{ text: '↩️ Back to Help Menu', callback_data: 'menu:help' }]] }; // Default back button

    // Pre-fetch and escape dynamic values to avoid errors during string construction
    const feeSOL = escapeMarkdownV2(formatSol(WITHDRAWAL_FEE_LAMPORTS)); // Constants from Part 1, formatSol from Part 3
    const minWithdrawSOL = escapeMarkdownV2(formatSol(MIN_WITHDRAWAL_LAMPORTS));
    const botVersionHelp = escapeMarkdownV2(BOT_VERSION); // BOT_VERSION from Part 1
    const expiryMinutesHelp = escapeMarkdownV2(String(process.env.DEPOSIT_ADDRESS_EXPIRY_MINUTES || '60')); // Env from Part 1
    const confirmationLevelHelp = escapeMarkdownV2(DEPOSIT_CONFIRMATION_LEVEL); // Constant from Part 1

    try { // Wrap text construction in try block for safety
        switch (section) {
            case 'how_to_play':
                // Ensure all punctuation etc is escaped
                text = "❓ *How to Play*\n\n" +
                        "1\\. *Deposit SOL*: Use the `/deposit` command or 'Deposit SOL' button in your Wallet menu to get a unique SOL address\\. Send SOL to that address\\.\n" +
                        "2\\. *Choose a Game*: Navigate to 'Play Games' from the main menu and select a game\\.\n" +
                        "3\\. *Place Your Bet*: Select a predefined bet amount or enter a custom amount when prompted\\.\n" +
                        "4\\. *Play & Win\\!*: Follow game\\-specific instructions\\. Winnings are instantly credited to your internal balance\\.\n" +
                        "5\\. *Withdraw SOL*: Use the 'Withdraw SOL' button in your Wallet to send funds to your linked external Solana wallet\\.";
                break;
            case 'deposits_withdrawals':
                // *** FIX #3: Reconstruct lines to avoid interpolation inside escaped parens (Verified Already Present) ***
                const confirmationsText = `\\(${confirmationLevelHelp}\\)`; // Pre-construct escaped parenthesis content
                const expiryText = `\\(${expiryMinutesHelp} minutes\\)`; // Pre-construct
                const feeText = `\\(${feeSOL} SOL\\)`; // Pre-construct

                text = "💰 *Deposits & Withdrawals*\n\n" +
                        "*Deposits:*\n" +
                        "  \\- Use `/deposit` or the 'Deposit SOL' button for a unique, temporary SOL address\\.\n" +
                        "  \\- Send SOL to this address from your external wallet\\.\n" +
                        `  \\- Funds are credited after network confirmations ${confirmationsText}\\.\n` + // Use pre-constructed
                        `  \\- Each deposit address expires after a set time ${expiryText}\\. Do NOT reuse expired addresses\\.\n\n` + // Use pre-constructed
                        "*Withdrawals:*\n" +
                        "  \\- Link an external wallet using `/wallet YOUR_ADDRESS` or via the 'My Wallet' menu\\.\n" +
                        "  \\- Choose 'Withdraw SOL' from the Wallet menu and enter the amount\\.\n" +
                        `  \\- A small network fee applies ${feeText}\\.\n` + // Use pre-constructed
                        `  \\- Minimum withdrawal: ${minWithdrawSOL} SOL\\.`; // Ensure final period escaped
                break;
            case 'game_rules':
                 // Ensure all punctuation is escaped
                 text = "🎲 *Game Rules*\n\n" +
                         "*Coinflip*: Choose Heads or Tails\\. 50/50 chance\\. Simple as that\\!\n" +
                         "*Slots*: Spin the reels\\! Match symbols for payouts\\. Hit the Jackpot \\(💎💎💎\\) for a big win\\!\n" +
                         "*War*: Your card vs\\. the Dealer's card\\. Highest card wins \\(Ace is high\\)\\. Push on a tie\\.\n" +
                         "*Race*: Pick a horse to win the race\\. Each horse has different payout odds\\.\n" +
                         "*Roulette*: European style \\(0\\-36\\)\\. Bet on numbers, colors \\(Red/Black\\), Even/Odd, or Low \\(1\\-18\\)/High \\(19\\-36\\)\\.\n" +
                         "*Crash*: Watch the multiplier increase\\! Cash out before it crashes to win your bet multiplied by the cashout value\\. If it crashes before you cash out, you lose\\.\n" +
                         "*Blackjack*: Get closer to 21 than the dealer without going over\\. Aces can be 1 or 11\\. Dealer stands on 17\\. Blackjack \\(Ace \\+ 10\\-value card\\) pays 3:2\\.";
                break;
            case 'fairness':
                 // Ensure % signs are escaped here
                 text = "⚖️ *Fairness & House Edge*\n\n" +
                         "Our games use provably fair algorithms where applicable \\(details may vary per game\\)\\.\n" +
                         "The House Edge is a small percentage ensuring the bot's operational sustainability\\. It's applied to winnings and varies per game:\n" +
                         `  \\- Coinflip: ${escapeMarkdownV2((GAME_CONFIG.coinflip.houseEdge * 100).toFixed(1))}\\%\n` + // Escaped %, format to 1 decimal
                         `  \\- Slots: ${escapeMarkdownV2((GAME_CONFIG.slots.houseEdge * 100).toFixed(1))}\\% \\(plus jackpot contribution\\)\n` + // Escaped %
                         `  \\- War: ${escapeMarkdownV2((GAME_CONFIG.war.houseEdge * 100).toFixed(1))}\\%\n` + // Escaped %
                         `  \\- Race: ${escapeMarkdownV2((GAME_CONFIG.race.houseEdge * 100).toFixed(1))}\\%\n` + // Escaped %
                         `  \\- Roulette: ${escapeMarkdownV2((GAME_CONFIG.roulette.houseEdge * 100).toFixed(1))}\\%\n` + // Escaped %
                         `  \\- Crash: ${escapeMarkdownV2((GAME_CONFIG.crash.houseEdge * 100).toFixed(1))}\\%\n`+ // Escaped %
                         `  \\- Blackjack: ${escapeMarkdownV2((GAME_CONFIG.blackjack.houseEdge * 100).toFixed(1))}\\%`; // Escaped %
                break;
            case 'support':
                // Fetch support contact from env, provide fallback
                const supportContact = process.env.SUPPORT_CONTACT_TELEGRAM || 'support_not_configured'; // Env from Part 1
                const escapedSupportContact = escapeMarkdownV2(supportContact);
                text = `📞 *Support*\n\nIf you need help or encounter any issues, please reach out to our support team`;
                if (supportContact !== 'support_not_configured') {
                    text += `: @${escapedSupportContact}\\.\n\n`; // Only add @ if configured
                } else {
                    text += `\\. \\(Support contact not configured\\)\\.\n\n`; // Inform if not configured
                }
                text += `Please provide your User ID \\(\`${escapeMarkdownV2(userId)}\`\\) and any relevant Bet IDs or Transaction details when contacting support\\.`; // Escaped . () `
                break;
            default:
                // Handle unknown section request
                text = `⚠️ Help section \`${escapeMarkdownV2(section)}\` not found\\.`; // Escaped `
                keyboard.inline_keyboard = [[{ text: '↩️ Back to Main Help', callback_data: 'menu:help' }]]; // Offer back to main help
        }
    } catch (buildError) {
        // Catch errors during text construction (e.g., if GAME_CONFIG is missing an expected key)
        console.error(`${logPrefix} Error building help text for section '${section}': ${buildError.message}`);
        text = `⚠️ An internal error occurred while generating help content for \`${escapeMarkdownV2(section)}\`\\.`;
        keyboard = fallbackKeyboard; // Use fallback keyboard on text build error
    }

    const options = { parse_mode: 'MarkdownV2', disable_web_page_preview: true, reply_markup: keyboard };

    // Try sending/editing the help section message
    try {
        if (isFromCallback && messageId) {
            // Attempt to edit the existing message
            await bot.editMessageText(text, { chat_id: chatId, message_id: messageId, ...options });
        } else {
            // Send as a new message if called via command
            await safeSendMessage(chatId, text, options);
        }
    } catch (error) {
        console.error(`${logPrefix} Error sending/editing help section '${section}': ${error.message}`);
        // *** FIX #8: Improved Error Recovery (Verified Already Present) ***
        // If sending/editing failed (likely parse error), send/edit a fallback message
        if (error.message?.toLowerCase().includes("can't parse entities") || error.message?.toLowerCase().includes("bad request: tags")) {
            const errorText = `⚠️ There was an error displaying the help section for \`${escapeMarkdownV2(section)}\` due to a formatting issue\\. Please report this\\.`; // Escaped . `
            if (isFromCallback && messageId) { // Try editing to the error message + fallback keyboard
                 bot.editMessageText(errorText, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: fallbackKeyboard }).catch(()=>{
                     // If edit fails, send new fallback
                     safeSendMessage(chatId, errorText, { parse_mode: 'MarkdownV2', reply_markup: fallbackKeyboard });
                 });
            } else { // Send new fallback message
                 safeSendMessage(chatId, errorText, { parse_mode: 'MarkdownV2', reply_markup: fallbackKeyboard });
            }
        } else {
            // For other errors, send a generic error message if the primary send/edit failed
            const genericErrorText = `⚠️ An error occurred loading the help section. Please try again later.`;
            if (isFromCallback && messageId) {
                bot.editMessageText(genericErrorText, { chat_id: chatId, message_id: messageId, reply_markup: fallbackKeyboard }).catch(() => {});
            } else {
                safeSendMessage(chatId, genericErrorText, { reply_markup: fallbackKeyboard });
            }
        }
    }
}


/**
 * Handles the /wallet command and wallet menu callbacks.
 * Allows viewing balance/address and initiating wallet linking.
 * Can also directly link wallet via command: /wallet <address>
 * @param {import('node-telegram-bot-api').Message | import('node-telegram-bot-api').CallbackQuery['message']} msgOrCbMsg Message or callback message.
 * @param {Array<string>} args Command arguments or callback parameters. args[0] is '/wallet' or 'wallet'. args[1] could be address.
 * @param {string | null} [correctUserIdFromCb=null] User ID if from callback.
 */
async function handleWalletCommand(msgOrCbMsg, args, correctUserIdFromCb = null) {
    const userId = String(correctUserIdFromCb || msgOrCbMsg.from.id);
    const chatId = String(msgOrCbMsg.chat.id);
    const logPrefix = `[WalletCmd User ${userId}]`;
    let messageToEditId = msgOrCbMsg.message_id;
    let isFromCallback = !!correctUserIdFromCb;
    clearUserState(userId); // Clear any pending state

    // Check if an address was provided directly with the /wallet command
    const potentialNewAddress = (!isFromCallback && args && args.length > 1) ? args[1].trim() : null;

    if (potentialNewAddress) {
        // User provided an address like /wallet <address>
        console.log(`${logPrefix} Attempting to link new address via command: ${potentialNewAddress}`);
        // Delete the user's command message containing the address
        if (!isFromCallback && messageToEditId) { bot.deleteMessage(chatId, messageToEditId).catch(()=>{}); }

        // Send a "Linking..." message first
        const loadingMsg = await safeSendMessage(chatId, `🔗 Linking wallet \`${escapeMarkdownV2(potentialNewAddress)}\`\\.\\.\\.`, { parse_mode: 'MarkdownV2' });
        if (!loadingMsg) return; // Exit if message failed to send
        const resultMessageId = loadingMsg.message_id;

        // Attempt to link the wallet
        // *** Applying Fix: Ensure linkUserWallet handles escaping internally or result is escaped ***
        const linkResult = await linkUserWallet(userId, potentialNewAddress); // from Part 2
        if (linkResult.success) {
            // Success message - Ensure dynamic parts are escaped
            const successMsg = `✅ Wallet \`${escapeMarkdownV2(linkResult.wallet)}\` successfully linked\\.\nYour referral code: \`${escapeMarkdownV2(linkResult.referralCode || 'Generated!')}\``; // Escaped . ! `
            bot.editMessageText(successMsg, { chat_id: chatId, message_id: resultMessageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: '↩️ Back to Wallet Menu', callback_data: 'menu:wallet' }]] }});
        } else {
            // Failure message - Escape error message
            const errorMsg = `⚠️ Failed to link wallet: ${escapeMarkdownV2(linkResult.error || 'Unknown error')}\\. Please ensure it's a valid Solana address\\.`; // Escaped .
            bot.editMessageText(errorMsg, { chat_id: chatId, message_id: resultMessageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [[{ text: '↩️ Back to Wallet Menu', callback_data: 'menu:wallet' }]] } });
        }
    }
    else {
        // No address provided, just show the wallet menu
        console.log(`${logPrefix} Displaying wallet info via menu action.`);
        // Call handleMenuAction to display the standard wallet menu
        await handleMenuAction(userId, chatId, messageToEditId, 'wallet', [], isFromCallback); // handleMenuAction defined later in 5b
    }
}


// --- End of Part 5b (Section 2b) ---
// index.js - Part 5b: General Commands, Game Commands, Menus & Maps (Section 2c of 4) - FULLY CORRECTED WITH LOGS
// --- VERSION: Based on 3.2.1v with targeted fixes for referral command ---

// (Continuing directly from Part 5b, Section 2b)
// ... (Assume functions, dependencies etc. from other parts are available:
//      bot, pool, GAME_CONFIG, userLastBetAmounts, escapeMarkdownV2, formatSol, 
//      getBetHistory, getUserWalletDetails, getTotalReferralEarnings, clearUserState,
//      REFERRAL_INITIAL_BET_MIN_LAMPORTS, REFERRAL_MILESTONE_REWARD_PERCENT, REFERRAL_INITIAL_BONUS_TIERS,
//      generateReferralCode, updateWalletCache, queryDatabase, ensureUserExists, getLinkedWallet,
//      getUserBalance, MIN_WITHDRAWAL_LAMPORTS, DEPOSIT_ADDRESS_EXPIRY_MS, DEPOSIT_CONFIRMATION_LEVEL,
//      safeSendMessage, process.env variables, etc.)

/**
 * Handles the /history command and corresponding menu action. Displays recent bets.
 * @param {import('node-telegram-bot-api').Message | import('node-telegram-bot-api').CallbackQuery['message']} msgOrCbMsg Message or callback message.
 * @param {Array<string>} args Command arguments or callback parameters.
 * @param {string | null} [correctUserIdFromCb=null] User ID if from callback.
 */
async function handleHistoryCommand(msgOrCbMsg, args, correctUserIdFromCb = null) {
    const userId = String(correctUserIdFromCb || msgOrCbMsg.from.id);
    const chatId = String(msgOrCbMsg.chat.id);
    const logPrefix = `[HistoryCmd User ${userId}]`;
    let messageToEditId = msgOrCbMsg.message_id;
    let isFromCallback = !!correctUserIdFromCb;
    clearUserState(userId); // Clear any pending state

    console.log(`${logPrefix} Fetching bet history.`);

    const limit = 5; // Show last 5 bets
    const history = await getBetHistory(userId, limit, 0, null); // getBetHistory from Part 2

    if (!history || history.length === 0) {
        const noHistoryMsg = "You have no betting history yet\\. Time to play some games\\!"; // Escaped . !
        const keyboard = {inline_keyboard: [[{text: "🎮 Games Menu", callback_data: "menu:game_selection"}]]};
        const options = { parse_mode: 'MarkdownV2', reply_markup: keyboard };
        if (isFromCallback && messageToEditId) {
            return bot.editMessageText(noHistoryMsg, {chat_id: chatId, message_id: messageToEditId, ...options})
                    .catch(e => { if (!e.message.includes("message is not modified")) safeSendMessage(chatId, noHistoryMsg, options); });
        }
        return safeSendMessage(chatId, noHistoryMsg, options);
    }

    let historyMsg = "📜 *Your Last 5 Bets:*\n\n";
    history.forEach(bet => {
        const gameName = GAME_CONFIG[bet.game_type]?.name || bet.game_type; // GAME_CONFIG from Part 1
        const wager = formatSol(bet.wager_amount_lamports); // formatSol from Part 3
        let outcomeText = `Status: ${escapeMarkdownV2(bet.status)}`; // escapeMarkdownV2 from Part 1
        if (bet.status.startsWith('completed_')) {
            const payout = bet.payout_amount_lamports !== null ? BigInt(bet.payout_amount_lamports) : 0n;
            const profit = payout - BigInt(bet.wager_amount_lamports || '0');
            if (bet.status === 'completed_win') outcomeText = `Won ${escapeMarkdownV2(formatSol(profit))} SOL \\(Returned ${escapeMarkdownV2(formatSol(payout))}\\)`; // Escaped ()
            else if (bet.status === 'completed_push') outcomeText = `Push \\(Returned ${escapeMarkdownV2(formatSol(payout))}\\)`; // Escaped ()
            else if (bet.status === 'completed_loss') outcomeText = `Lost ${escapeMarkdownV2(wager)} SOL`;
        } else if (bet.status === 'processing_game') {
            outcomeText = `Processing...`; 
        } else if (bet.status === 'active') {
            outcomeText = `Active`;
        }

        const betDate = escapeMarkdownV2(new Date(bet.created_at).toLocaleString('en-GB', { day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit', hour12: false }));
        historyMsg += `\\- *${escapeMarkdownV2(gameName)}* on ${betDate}\n` +
                      `  Bet: ${escapeMarkdownV2(wager)} SOL, Result: ${outcomeText}\n\n`;
    });
    historyMsg += "\\_For full history, please use an external service if available or contact support for older records\\.\\_"; // Escaped . _

    const historyKeyboard = [[{ text: '↩️ Back to Wallet', callback_data: 'menu:wallet' }, { text: '🎮 Games Menu', callback_data: 'menu:game_selection' }]];
    const options = { parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: historyKeyboard} };

    if (isFromCallback && messageToEditId) {
        bot.editMessageText(historyMsg, {chat_id: chatId, message_id: messageToEditId, ...options})
            .catch(e => { if (!e.message.includes("message is not modified")) safeSendMessage(chatId, historyMsg, options); });
    } else {
        safeSendMessage(chatId, historyMsg, options);
    }
}

/**
 * Handles the /referral command and menu option. Displays referral info and link.
 * @param {import('node-telegram-bot-api').Message | import('node-telegram-bot-api').CallbackQuery['message']} msgOrCbMsg Message or callback message.
 * @param {Array<string>} args Command arguments or callback parameters.
 * @param {string | null} [correctUserIdFromCb=null] User ID if from callback.
 */
async function handleReferralCommand(msgOrCbMsg, args, correctUserIdFromCb = null) {
    const userId = String(correctUserIdFromCb || msgOrCbMsg.from.id);
    const chatId = String(msgOrCbMsg.chat.id);
    let messageToEditId = msgOrCbMsg.message_id;
    let isFromCallback = !!correctUserIdFromCb;
    clearUserState(userId); 

    const fallbackKeyboard = { inline_keyboard: [[{ text: '↩️ Back to Main Menu', callback_data: 'menu:main' }]] };
    const logPrefix = `[ReferralCmd User ${userId}]`; // Added for consistency in logging

    try { 
        const userDetails = await getUserWalletDetails(userId); 

        if (!userDetails?.external_withdrawal_address) {
            const noWalletMsg = `❌ You need to link your wallet first using \`/wallet <YourSolAddress>\` before using the referral system\\. This ensures rewards can be paid out\\.`;
            const keyboard = {inline_keyboard: [[{text: "🔗 Link Wallet", callback_data: "menu:link_wallet_prompt"}, {text: "↩️ Back to Menu", callback_data: "menu:main"}]]};
            if (isFromCallback && messageToEditId) {
                return bot.editMessageText(noWalletMsg, {chat_id: chatId, message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: keyboard})
                        .catch(e => { if (!e.message.includes("message is not modified")) safeSendMessage(chatId, noWalletMsg, {parse_mode: 'MarkdownV2', reply_markup: keyboard }); });
            }
            return safeSendMessage(chatId, noWalletMsg, { parse_mode: 'MarkdownV2', reply_markup: keyboard });
        }

        let currentRefCode = userDetails.referral_code; // Use let as it might be re-assigned
        if (!currentRefCode) {
            console.warn(`${logPrefix} Referral code was missing after wallet link check. Attempting to generate.`);
            let clientGen = null;
            try {
                clientGen = await pool.connect(); await clientGen.query('BEGIN');
                const walletCheck = await queryDatabase('SELECT referral_code FROM wallets WHERE user_id=$1 FOR UPDATE', [userId], clientGen);
                if (walletCheck.rowCount > 0 && !walletCheck.rows[0].referral_code) {
                    currentRefCode = generateReferralCode(); 
                    await queryDatabase('UPDATE wallets SET referral_code=$1 WHERE user_id=$2', [currentRefCode, userId], clientGen);
                    await clientGen.query('COMMIT');
                    console.log(`${logPrefix} Generated missing referral code: ${currentRefCode}`);
                    updateWalletCache(userId, { referralCode: currentRefCode }); 
                } else if (walletCheck.rowCount > 0 && walletCheck.rows[0].referral_code) {
                    await clientGen.query('ROLLBACK'); 
                    currentRefCode = walletCheck.rows[0].referral_code; 
                } else { 
                    await clientGen.query('ROLLBACK');
                    throw new Error("User not found during referral code generation sanity check.");
                }
            } catch (genError) {
                if(clientGen) await clientGen.query('ROLLBACK').catch(()=>{});
                console.error(`${logPrefix} Error in on-demand referral code generation: ${genError.message}`);
                throw genError; 
            } finally { if(clientGen) clientGen.release(); }
            
            if (!currentRefCode) { 
                 console.error(`${logPrefix} CRITICAL: Referral code is still missing after generation attempt.`);
                 throw new Error("Could not retrieve or generate referral code.");
            }
        }
        
        const totalEarningsLamports = await getTotalReferralEarnings(userId);
        const totalEarningsSOL = escapeMarkdownV2(formatSol(totalEarningsLamports));
        const referralCount = escapeMarkdownV2(String(userDetails.referral_count || 0));
        const withdrawalAddress = escapeMarkdownV2(userDetails.external_withdrawal_address); // This is the display-escaped withdrawal address
        const escapedRefCode = escapeMarkdownV2(currentRefCode); // Use the potentially newly generated or confirmed refCode

        let botUsername = process.env.BOT_USERNAME || 'YOUR_BOT_USERNAME';
        if (botUsername === 'YOUR_BOT_USERNAME') {
            try {
                const me = await bot.getMe();
                if (me.username) {
                    botUsername = me.username;
                }
            } catch (e) {
                console.warn(`${logPrefix} Could not fetch bot username for referral link, link might be incorrect: ${e.message}`);
            }
        }
        const rawReferralLink = `https://t.me/${botUsername}?start=${currentRefCode}`; // Use currentRefCode for the raw link
        const escapedReferralLinkForCodeBlock = escapeMarkdownV2(rawReferralLink);

        const minBetAmount = escapeMarkdownV2(formatSol(REFERRAL_INITIAL_BET_MIN_LAMPORTS));
        const milestonePercentNumber = (REFERRAL_MILESTONE_REWARD_PERCENT * 100);
        const milestonePercentString = milestonePercentNumber.toFixed(1); 
        const milestonePercent = escapeMarkdownV2(milestonePercentString);

        console.log(`[Debug Tiers User ${userId}] Starting construction of tiersDesc. REFERRAL_INITIAL_BONUS_TIERS:`, JSON.stringify(REFERRAL_INITIAL_BONUS_TIERS));
        const tiersDesc = REFERRAL_INITIAL_BONUS_TIERS.map(t => {
            const count = t.maxCount === Infinity ? '100\\+' : `\\<\\=${escapeMarkdownV2(String(t.maxCount))}`;
            const rawTierPercentValue = (t.percent * 100);
            const rawTierPercentString = rawTierPercentValue.toFixed(1);
            console.log(`[Debug Tier Build User ${userId}] For count: ${count}, rawTierPercentValue: ${rawTierPercentValue}, rawTierPercentString: '${rawTierPercentString}'`);
            const tierPercent = escapeMarkdownV2(rawTierPercentString);
            console.log(`[Debug Tier Build User ${userId}] Escaped tierPercent string for count ${count}: '${tierPercent}'`);
            return `${count} refs \\= ${tierPercent}%`;
        }).join('\\, ');
        console.log(`[Debug Tier Build User ${userId}] Final tiersDesc string generated: '${tiersDesc}'`);

        let referralMsg = `🤝 *Your Referral Dashboard*\n\n` +
            `Share your unique link to earn SOL when your friends play\\!\n\n` +
            `*Your Code:* \`${escapedRefCode}\`\n` +
            `*Your Clickable Link:*\n[Click here to use your link](${rawReferralLink})\n` +
            `\\_\(Tap button below or copy here: \`${escapedReferralLinkForCodeBlock}\`\\)_\n\n` +
            `*Successful Referrals:* ${referralCount}\n` +
            `*Total Referral Earnings Paid:* ${totalEarningsSOL} SOL\n\n` +
            `*How Rewards Work:*\n` +
            `1\\. *Initial Bonus:* Earn a % of your referral's *first qualifying bet* \\(min ${minBetAmount} SOL wager\\)\\. Your % increases with more referrals\\!\n` +
            `   *Tiers:* ${tiersDesc}\n` +
            `2\\. *Milestone Bonus:* Earn ${milestonePercent}% of their total wagered amount as they hit milestones \\(e\\.g\\., 1 SOL, 5 SOL wagered, etc\\.\\)\\.\\.\n\n` +
            `Rewards are paid to your linked wallet: \`${withdrawalAddress}\``; // Use the 'withdrawalAddress' variable defined above

        console.log(`--- START OF referralMsg ATTEMPT (handleReferralCommand User ${userId}) ---`);
        console.log(referralMsg);
        console.log(`--- END OF referralMsg ATTEMPT (User ${userId}) ---`);

        const keyboard = [
            [{ text: '🔗 Share My Referral Link!', switch_inline_query: rawReferralLink }],
            [{ text: '↩️ Back to Main Menu', callback_data: 'menu:main' }]
        ];
        const options = { parse_mode: 'MarkdownV2', disable_web_page_preview: true, reply_markup: {inline_keyboard: keyboard} };

        if (isFromCallback && messageToEditId) {
            await bot.editMessageText(referralMsg, {chat_id: chatId, message_id: messageToEditId, ...options})
                .catch(e => {
                    console.error(`${logPrefix} FAILED to edit referralMsg. Error: ${e.message}`);
                    if (e.response && e.response.body) {
                        console.error(`${logPrefix} Telegram API Error Body on EDIT:`, JSON.stringify(e.response.body));
                    }
                    if (!e.message?.toLowerCase().includes("can't parse entities") && !e.message?.includes("message is not modified")) {
                         safeSendMessage(chatId, referralMsg, options).catch(e_send => {
                            console.error(`${logPrefix} Fallback safeSendMessage ALSO FAILED. Error: ${e_send.message}`);
                            if (e_send.response && e_send.response.body) {
                                console.error(`${logPrefix} Telegram API Error Body on Fallback SEND:`, JSON.stringify(e_send.response.body));
                            }
                         });
                    } else if (e.message?.toLowerCase().includes("can't parse entities")){
                         safeSendMessage(chatId, "⚠️ An error occurred displaying referral information due to a formatting problem\\. Please try again later or contact support\\.", {parse_mode: 'MarkdownV2', reply_markup: fallbackKeyboard});
                    }
                });
        } else {
            await safeSendMessage(chatId, referralMsg, options)
                .catch(e => {
                    console.error(`${logPrefix} FAILED to send new referralMsg. Error: ${e.message}`);
                    if (e.response && e.response.body) {
                        console.error(`${logPrefix} Telegram API Error Body on SEND:`, JSON.stringify(e.response.body));
                    }
                    safeSendMessage(chatId, "⚠️ An error occurred displaying referral information\\. Please try again later or contact support\\.", {parse_mode: 'MarkdownV2', reply_markup: fallbackKeyboard});
                });
        }

    } catch (error) { 
        console.error(`${logPrefix} Error in main referral command handler: ${error.message}`, error.stack);
        const errorText = `⚠️ An error occurred displaying referral info: ${escapeMarkdownV2(error.message)}\\.`; 
        if (isFromCallback && messageToEditId) {
             bot.editMessageText(errorText, { chat_id: chatId, message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: fallbackKeyboard }).catch(()=>{
                 safeSendMessage(chatId, errorText, { parse_mode: 'MarkdownV2', reply_markup: fallbackKeyboard });
             });
        } else {
             safeSendMessage(chatId, errorText, { parse_mode: 'MarkdownV2', reply_markup: fallbackKeyboard });
        }
    }
}


/**
 * Handles the /deposit command and 'quick_deposit' callback. Shows deposit address.
 * @param {import('node-telegram-bot-api').Message | import('node-telegram-bot-api').CallbackQuery['message']} msgOrCbMsg Message or callback message.
 * @param {Array<string>} args Command arguments or callback parameters.
 * @param {string | null} [correctUserIdFromCb=null] User ID if from callback.
 */
async function handleDepositCommand(msgOrCbMsg, args, correctUserIdFromCb = null) {
    const userId = String(correctUserIdFromCb || msgOrCbMsg.from.id);
    const chatId = String(msgOrCbMsg.chat.id);
    const logPrefix = `[DepositCmd User ${userId}]`;
    let messageToEditId = msgOrCbMsg.message_id;
    let isFromCallback = !!correctUserIdFromCb;
    clearUserState(userId); 

    let workingMessageId = messageToEditId; 

    const generatingText = "⏳ Generating your unique deposit address\\.\\.\\."; 
    try {
        if (isFromCallback && messageToEditId) {
            await bot.editMessageText(generatingText, { chat_id: chatId, message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [] } });
        } else {
            const tempMsg = await safeSendMessage(chatId, generatingText, { parse_mode: 'MarkdownV2' });
            workingMessageId = tempMsg?.message_id; 
        }
    } catch (editError) {
        if (!editError.message?.includes("message is not modified")) {
            console.warn(`${logPrefix} Failed to edit message ${messageToEditId} for generating state, sending new. Error: ${editError.message}`);
            const tempMsg = await safeSendMessage(chatId, generatingText, { parse_mode: 'MarkdownV2' });
            workingMessageId = tempMsg?.message_id;
        } else {
            workingMessageId = messageToEditId;
        }
    }

    if (!workingMessageId) {
        console.error(`${logPrefix} Failed to establish message context for deposit address display.`);
        safeSendMessage(chatId, "Failed to initiate deposit process\\. Please try again\\.", { parse_mode: 'MarkdownV2' }); 
        return;
    }

    try {
        let tempClient = null;
        try { tempClient = await pool.connect(); await ensureUserExists(userId, tempClient); } finally { if (tempClient) tempClient.release(); }

        const existingAddresses = await queryDatabase(
            `SELECT deposit_address, expires_at FROM deposit_addresses WHERE user_id = $1 AND status = 'pending' AND expires_at > NOW() ORDER BY created_at DESC LIMIT 1`,
            [userId]
        );

        if (existingAddresses.rowCount > 0) {
            const existing = existingAddresses.rows[0];
            const existingAddress = existing.deposit_address;
            const existingExpiresAt = new Date(existing.expires_at);
            const expiresInMs = existingExpiresAt.getTime() - Date.now();
            const expiresInMinutes = Math.max(1, Math.ceil(expiresInMs / (60 * 1000))); 
            const escapedExistingAddress = escapeMarkdownV2(existingAddress);

            let text = `💰 *Active Deposit Address*\n\nYou already have an active deposit address:\n\`${escapedExistingAddress}\`\n` + 
                       `\\_\(Tap the address above to copy\\)\\_\\n\n` + 
                       `It expires in approximately ${escapeMarkdownV2(String(expiresInMinutes))} minutes\\.`; 
            text += `\n\nOnce you send SOL, it will be credited after confirmations\\. New deposits to this address will be credited until it expires\\.`; 

            const keyboard = [[{ text: '↩️ Back to Wallet', callback_data: 'menu:wallet' }], [{ text: `📲 Show QR Code`, url: `https://api.qrserver.com/v1/create-qr-code/?size=200x200&data=solana:${existingAddress}` }]];
            bot.editMessageText(text, { chat_id: chatId, message_id: workingMessageId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: keyboard } });
            return; 
        }

        console.log(`${logPrefix} No active address found. Generating new one.`);
        const nextIndex = await getNextDepositAddressIndex(userId); 
        const derivedInfo = await generateUniqueDepositAddress(userId, nextIndex); 
        if (!derivedInfo) {
            throw new Error("Failed to generate deposit address\\. Master seed phrase might be an issue\\."); 
        }

        const depositAddress = derivedInfo.publicKey.toBase58();
        const expiresAt = new Date(Date.now() + DEPOSIT_ADDRESS_EXPIRY_MS); 
        const recordResult = await createDepositAddressRecord(userId, depositAddress, derivedInfo.derivationPath, expiresAt); 
        if (!recordResult.success) {
            throw new Error(escapeMarkdownV2(recordResult.error || "Failed to save deposit address record in DB\\.")); 
        }

        const expiryMinutes = escapeMarkdownV2(String(Math.round(DEPOSIT_ADDRESS_EXPIRY_MS / (60 * 1000))));
        const confirmationLevel = escapeMarkdownV2(DEPOSIT_CONFIRMATION_LEVEL); 
        const escapedAddress = escapeMarkdownV2(depositAddress);

        const message = `💰 *Your Unique Deposit Address*\n\n` +
                        `Send SOL to this unique address:\n\n` +
                        `\`${escapedAddress}\`\n` + 
                        `\\_\(Tap the address above to copy\\)\\_\\n\n` + 
                        `⚠️ *Important:*\n` +
                        `1\\. This address is unique to you and for this deposit session\\. It will expire in *${expiryMinutes} minutes*\\.\n` + 
                        `2\\. For new deposits, use \`/deposit\` again or the menu option\\.\n` + 
                        `3\\. Confirmation: *${confirmationLevel}* network confirmations required\\.`; 

        const depositKeyboard = [
            [{ text: `📲 Show QR Code`, url: `https://api.qrserver.com/v1/create-qr-code/?size=200x200&data=solana:${depositAddress}` }],
            [{ text: '✅ Done / Back to Wallet', callback_data: 'menu:wallet' }]
        ];
        const options = { parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: depositKeyboard} };

        await bot.editMessageText(message, {chat_id: chatId, message_id: workingMessageId, ...options}).catch(e => {
             if (e.message && (e.message.includes("can't parse entities") || e.message.includes("bad request"))) {
                 console.error(`❌ [DepositCmd User ${userId}] PARSE ERROR with revised hint! Message attempted: ${message}`);
                 const plainMessage = `Your Deposit Address (Tap to copy):\n${depositAddress}\n\nExpires in ${expiryMinutes} minutes. Confirmation: ${confirmationLevel}. Do not reuse after expiry.`;
                 safeSendMessage(chatId, plainMessage, {reply_markup: {inline_keyboard: depositKeyboard}});
             }
             else if (!e.message.includes("message is not modified")) {
                 console.warn(`${logPrefix} Failed to edit message ${workingMessageId} with deposit address, sending new. Error: ${e.message}`);
                 safeSendMessage(chatId, message, options);
             }
         });

    } catch (error) {
        console.error(`${logPrefix} Error generating deposit address: ${error.message}`);
        const errorMsg = `❌ Error generating deposit address: ${escapeMarkdownV2(error.message)}\\. Please try again\\. If the issue persists, contact support\\.`; 
        const errorKeyboard = [[{text: "↩️ Back to Menu", callback_data: "menu:main"}]];
        const errorOptions = { parse_mode: 'MarkdownV2', reply_markup: {inline_keyboard: errorKeyboard} };
        bot.editMessageText(errorMsg, {chat_id: chatId, message_id: workingMessageId, ...errorOptions}).catch(e => safeSendMessage(chatId, errorMsg, errorOptions));
    }
}

/**
 * Handles the /withdraw command and 'menu:withdraw' callback. Starts withdrawal process.
 * @param {import('node-telegram-bot-api').Message | import('node-telegram-bot-api').CallbackQuery['message']} msgOrCbMsg Message or callback message.
 * @param {Array<string>} args Command arguments or callback parameters.
 * @param {string | null} [correctUserIdFromCb=null] User ID if from callback.
 */
async function handleWithdrawCommand(msgOrCbMsg, args, correctUserIdFromCb = null) {
    const userId = String(correctUserIdFromCb || msgOrCbMsg.from.id);
    const chatId = String(msgOrCbMsg.chat.id);
    const logPrefix = `[WithdrawCmd User ${userId}]`;
    const breadcrumb = "Withdraw SOL"; 
    let messageToEditId = msgOrCbMsg.message_id;
    let isFromCallback = !!correctUserIdFromCb;
    clearUserState(userId); 

    let workingMessageId = messageToEditId; 

    const preparingText = "💸 Preparing withdrawal process\\.\\.\\."; 
    try {
        if (isFromCallback && messageToEditId) {
            await bot.editMessageText(preparingText, { chat_id: chatId, message_id: messageToEditId, parse_mode: 'MarkdownV2', reply_markup: { inline_keyboard: [] } });
        } else {
            const tempMsg = await safeSendMessage(chatId, preparingText, { parse_mode: 'MarkdownV2' });
            workingMessageId = tempMsg?.message_id;
        }
    } catch (editError) {
        if (!editError.message?.includes("message is not modified")) {
            console.warn(`${logPrefix} Failed to edit message ${messageToEditId} for preparing state, sending new. Error: ${editError.message}`);
            const tempMsg = await safeSendMessage(chatId, preparingText, { parse_mode: 'MarkdownV2' });
            workingMessageId = tempMsg?.message_id;
        } else {
            workingMessageId = messageToEditId;
        }
    }
    if (!workingMessageId) {
        console.error(`${logPrefix} Failed message context for withdraw.`);
        safeSendMessage(chatId, "Failed to initiate withdrawal process\\. Please try again\\.", { parse_mode: 'MarkdownV2' }); 
        return;
    }

    try {
        const linkedAddress = await getLinkedWallet(userId); 
        if (!linkedAddress) {
            const noWalletMsg = `⚠️ You must set your withdrawal address first using \`/wallet <YourSolanaAddress>\`\\.`; 
            const keyboard = {inline_keyboard: [[{text: "🔗 Link Wallet", callback_data: "menu:link_wallet_prompt"}, {text: "↩️ Back to Menu", callback_data: "menu:main"}]]};
            return bot.editMessageText(noWalletMsg, {chat_id: chatId, message_id: workingMessageId, parse_mode: 'MarkdownV2', reply_markup: keyboard})
                    .catch(e => safeSendMessage(chatId, noWalletMsg, {parse_mode: 'MarkdownV2', reply_markup: keyboard})); 
        }

        const currentBalance = await getUserBalance(userId); 
        const feeLamports = BigInt(process.env.WITHDRAWAL_FEE_LAMPORTS || '5000'); 
        const minWithdrawTotal = MIN_WITHDRAWAL_LAMPORTS + feeLamports; 

        if (currentBalance < minWithdrawTotal) {
            const lowBalMsg = `⚠️ Your balance \\(${escapeMarkdownV2(formatSol(currentBalance))} SOL\\) is below the minimum required to withdraw \\(${escapeMarkdownV2(formatSol(MIN_WITHDRAWAL_LAMPORTS))} SOL \\+ ${escapeMarkdownV2(formatSol(feeLamports))} SOL fee\\)\\.`; 
            const keyboard = {inline_keyboard: [[{text: "↩️ Back to Menu", callback_data: "menu:main"}]]};
            return bot.editMessageText(lowBalMsg, {chat_id: chatId, message_id: workingMessageId, parse_mode: 'MarkdownV2', reply_markup: keyboard})
                    .catch(e => safeSendMessage(chatId, lowBalMsg, {parse_mode: 'MarkdownV2', reply_markup: keyboard})); 
        }

        userStateCache.set(userId, {
            state: 'awaiting_withdrawal_amount', 
            chatId: String(chatId),
            messageId: workingMessageId, 
            data: {
                linkedWallet: linkedAddress,
                breadcrumb,
                originalMessageId: workingMessageId, 
                currentBalance: currentBalance.toString() 
            },
            timestamp: Date.now()
        });

        const minWithdrawSOLText = escapeMarkdownV2(formatSol(MIN_WITHDRAWAL_LAMPORTS));
        const feeSOLText = escapeMarkdownV2(formatSol(feeLamports));
        const escapedAddress = escapeMarkdownV2(linkedAddress);
        const promptText = `${escapeMarkdownV2(breadcrumb)}\n\n` +
                            `Your withdrawal address: \`${escapedAddress}\`\n` + 
                            `Minimum withdrawal: ${minWithdrawSOLText} SOL\\. Fee: ${feeSOLText} SOL\\.\n\n` + 
                            `Please enter the amount of SOL you wish to withdraw \\(e\\.g\\., 0\\.5\\), or /cancel\\:`; 

        await bot.editMessageText(promptText, {
            chat_id: chatId, message_id: workingMessageId, parse_mode: 'MarkdownV2',
            reply_markup: { inline_keyboard: [[{ text: '❌ Cancel Withdrawal', callback_data: 'menu:wallet' }]]} 
        });

    } catch (error) {
        console.error(`${logPrefix} Error setting up withdrawal: ${error.message}`);
        const errorMsg = `❌ Error starting withdrawal\\. Please try again\\. Or /cancel\\.`; 
        const keyboard = {inline_keyboard: [[{text: "↩️ Back to Menu", callback_data: "menu:main"}]]};
        bot.editMessageText(errorMsg, {chat_id: chatId, message_id: workingMessageId, parse_mode: 'MarkdownV2', reply_markup: keyboard})
            .catch(e => safeSendMessage(chatId, errorMsg, {parse_mode: 'MarkdownV2', reply_markup: keyboard})); 
        clearUserState(userId); 
    }
}

/**
 * Handles the /leaderboards command and related callbacks.
 * @param {import('node-telegram-bot-api').Message | import('node-telegram-bot-api').CallbackQuery['message']} msgOrCbMsg Message or callback message.
 * @param {Array<string>} args Command arguments or callback parameters. args[0]=command/menu, args[1]=type, args[2]=page (if command) OR args[0]=type, args[1]=page (if callback 'leaderboard_nav')
 * @param {string | null} [correctUserIdFromCb=null] User ID if from callback.
 */
async function handleLeaderboardsCommand(msgOrCbMsg, args, correctUserIdFromCb = null) {
    const userId = String(correctUserIdFromCb || msgOrCbMsg.from.id);
    const chatId = String(msgOrCbMsg.chat.id);
    const messageId = msgOrCbMsg.message_id;
    const isFromCallback = !!correctUserIdFromCb;
    clearUserState(userId); 

    let type = 'overall_wagered'; 
    let page = 0; 

    if (isFromCallback) {
        type = args[0] || 'overall_wagered';
        page = parseInt(args[1] || '0', 10);
        if (isNaN(page) || page < 0) page = 0; 
    } else {
        type = args.length > 1 ? args[1] : 'overall_wagered';
        page = args.length > 2 ? parseInt(args[2], 10) - 1 : 0; 
        if (isNaN(page) || page < 0) page = 0; 
    }

    await displayLeaderboard(chatId, messageId, userId, type, page, isFromCallback);
}


/**
 * Displays leaderboards. Fetches data and formats the message.
 * @param {string} chatId
 * @param {number | null} messageId Message ID to edit (if from callback). Can be null.
 * @param {string} userId User requesting (for context).
 * @param {string} type Leaderboard type (e.g., 'overall_wagered', 'overall_profit').
 * @param {number} page Current page number (0-indexed).
 * @param {boolean} [isFromCallback=false] If true, try to edit message.
 */
async function displayLeaderboard(chatId, messageId, userId, type = 'overall_wagered', page = 0, isFromCallback = false) {
    const logPrefix = `[DisplayLeaderboard Type:${type} Page:${page} User:${userId}]`;
    console.log(`${logPrefix}`);
    const itemsPerPage = 10;
    const offset = page * itemsPerPage;

    let querySQL = '';
    let paramsSQL = [itemsPerPage, offset];
    let title = '🏆 Overall Top Wagerers'; 

    switch (type) {
        case 'overall_wagered':
            querySQL = `SELECT user_id, total_wagered as score FROM wallets WHERE total_wagered > 0 ORDER BY total_wagered DESC LIMIT $1 OFFSET $2;`;
            title = '🏆 Overall Top Wagerers (Total SOL)';
            break;
        case 'overall_profit':
            querySQL = `
                SELECT
                    b.user_id,
                    SUM(COALESCE(b.payout_amount_lamports, 0) - b.wager_amount_lamports) as score
                FROM bets b
                WHERE b.status LIKE 'completed_%' OR b.status = 'error_game_logic' 
                GROUP BY b.user_id
                HAVING SUM(COALESCE(b.payout_amount_lamports, 0) - b.wager_amount_lamports) != 0 
                ORDER BY score DESC
                LIMIT $1 OFFSET $2;
            `;
            title = '📈 Overall Top Profit (Total SOL)';
            break;
        default:
            const errorText = `⚠️ Leaderboard type \`${escapeMarkdownV2(type)}\` is not available yet\\.`; 
            const backKeyboard = { inline_keyboard: [[{ text: '🏆 Leaderboards Home', callback_data: 'menu:leaderboards' }, { text: '↩️ Back to Main Menu', callback_data: 'menu:main' }]] };
            if (isFromCallback && messageId) { bot.editMessageText(errorText, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: backKeyboard }); }
            else { safeSendMessage(chatId, errorText, { parse_mode: 'MarkdownV2', reply_markup: backKeyboard }); }
            return;
    }

    try {
        const results = await queryDatabase(querySQL, paramsSQL); 
        let leaderboardText = `👑 *${escapeMarkdownV2(title)}* \\- Page ${escapeMarkdownV2(String(page + 1))}\n\n`; 

        if (results.rows.length === 0) {
            leaderboardText += (page === 0) ? "No data available for this leaderboard yet\\." : "No more entries on this page\\."; 
        } else {
            for (let i = 0; i < results.rows.length; i++) {
                const row = results.rows[i];
                const rank = offset + i + 1;
                const displayName = `User\\.\\.\\.${escapeMarkdownV2(String(row.user_id).slice(-5))}`; 
                let valueDisplay = 'N/A';
                if (row.score !== undefined && row.score !== null) {
                    valueDisplay = `${escapeMarkdownV2(formatSol(row.score))} SOL`; 
                }
                leaderboardText += `${escapeMarkdownV2(String(rank))}\\. ${displayName}: ${valueDisplay}\n`; 
            }
        }

        const keyboardButtons = [];
        const rowNav = [];
        if (page > 0) {
            rowNav.push({ text: '⬅️ Previous', callback_data: `leaderboard_nav:${type}:${page - 1}` });
        }
        if (results.rows.length === itemsPerPage) {
            rowNav.push({ text: 'Next ➡️', callback_data: `leaderboard_nav:${type}:${page + 1}` });
        }
        if (rowNav.length > 0) keyboardButtons.push(rowNav);
        keyboardButtons.push([{ text: '🏆 Leaderboards Home', callback_data: 'menu:leaderboards' }, { text: '↩️ Main Menu', callback_data: 'menu:main' }]);

        const replyMarkup = { inline_keyboard: keyboardButtons };
        const options = { parse_mode: 'MarkdownV2', reply_markup: replyMarkup };

        if (isFromCallback && messageId) {
            bot.editMessageText(leaderboardText, { chat_id: chatId, message_id: messageId, ...options })
                .catch(e => { if (!e.message.includes("message is not modified")) console.warn(`[DisplayLeaderboard] Edit error: ${e.message}`); });
        } else {
            safeSendMessage(chatId, leaderboardText, options); 
        }

    } catch (err) {
        console.error(`${logPrefix} Error fetching leaderboard data: ${err.message}`);
        const errorText = `⚠️ Error loading leaderboard: ${escapeMarkdownV2(err.message)}\\. Please try again later\\.`; 
        const backKeyboard = { inline_keyboard: [[{ text: '↩️ Back to Main Menu', callback_data: 'menu:main' }]] };
        if (isFromCallback && messageId) { bot.editMessageText(errorText, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: backKeyboard }); }
        else { safeSendMessage(chatId, errorText, { parse_mode: 'MarkdownV2', reply_markup: backKeyboard }); }
    }
}

/**
 * Handles generic menu actions that don't have dedicated command handlers. Routes based on menuType.
 * @param {string} userId
 * @param {string} chatId
 * @param {number | null} messageId ID of the message to edit (can be null if not editing).
 * @param {string} menuType Type of menu requested (e.g., 'link_wallet_prompt', 'wallet', 'leaderboards').
 * @param {Array<string>} params Additional parameters from callback data.
 * @param {boolean} [isFromCallback=true] Assume true if called here.
 */
async function handleMenuAction(userId, chatId, messageId, menuType, params = [], isFromCallback = true) {
    const logPrefix = `[MenuAction User ${userId} Menu ${menuType}]`;
    console.log(`${logPrefix} Handling menu action.`);
    let text = `Menu: ${escapeMarkdownV2(menuType)}`; 
    let keyboard = { inline_keyboard: [] }; 
    let setState = null; 
    const fallbackKeyboard = { inline_keyboard: [[{ text: '↩️ Back to Main Menu', callback_data: 'menu:main' }]] };

    if (menuType !== 'link_wallet_prompt') { 
        clearUserState(userId); 
    }

    try {
        switch (menuType) {
            case 'link_wallet_prompt':
                clearUserState(userId); 
                const breadcrumbWallet = "Link Wallet";
                text = `🔗 *Link/Update External Wallet*\n\nPlease send your Solana wallet address in the chat\\.\n\nExample: \`SoLmaNqerT3ZpPT1qS9j2kKx2o5x94s2f8u5aA3bCgD\`\n\nOr type /cancel to go back\\.`; 
                keyboard.inline_keyboard = [ [{ text: '❌ Cancel', callback_data: 'menu:wallet' }] ]; 
                setState = {
                    state: 'awaiting_withdrawal_address', 
                    chatId: String(chatId),
                    messageId: messageId, 
                    data: { breadcrumb: breadcrumbWallet, originalMessageId: messageId },
                    timestamp: Date.now()
                };
                break;

            case 'wallet':
                const userBalance = await getUserBalance(userId); 
                const userDetails = await getUserWalletDetails(userId); 
                text = `👤 *Your Wallet & Stats*\n\n*Balance:* ${escapeMarkdownV2(formatSol(userBalance))} SOL\n`;
                if (userDetails?.external_withdrawal_address) {
                    text += `*Withdrawal Address:* \`${escapeMarkdownV2(userDetails.external_withdrawal_address)}\`\n`; 
                } else {
                    text += `*Withdrawal Address:* Not Set \\(Use \`/wallet <YourSolAddress>\` or button below\\)\n`; 
                }
                if (userDetails?.referral_code) {
                    text += `*Referral Code:* \`${escapeMarkdownV2(userDetails.referral_code)}\`\n`; 
                } else {
                    text += `*Referral Code:* Not Yet Generated \\(Link wallet first\\)\n`; 
                }
                text += `*Referrals Made:* ${escapeMarkdownV2(String(userDetails?.referral_count || 0))}\n`;
                text += `*Total Wagered:* ${escapeMarkdownV2(formatSol(userDetails?.total_wagered || 0n))} SOL\n`;
                const lastBetsDB = userDetails?.last_bet_amounts || {};
                const lastBetsMemory = userLastBetAmounts.get(userId) || new Map(); 
                const combinedLastBets = {...lastBetsDB};
                lastBetsMemory.forEach((value, key) => { combinedLastBets[key] = value.toString(); }); 

                if (Object.keys(combinedLastBets).length > 0) {
                    text += `\n*Last Bet Amounts \\(Approx\\.\\):*\n`; 
                    for (const gameKey in combinedLastBets) {
                        if (GAME_CONFIG[gameKey]) { 
                            let formattedAmount = 'N/A';
                            try { formattedAmount = formatSol(BigInt(combinedLastBets[gameKey])); } 
                            catch { console.warn(`[MenuAction Wallet] Invalid BigInt format for last bet amount, game ${gameKey}: ${combinedLastBets[gameKey]}`); }
                            text += `  \\- ${escapeMarkdownV2(GAME_CONFIG[gameKey]?.name || gameKey)}: ${escapeMarkdownV2(formattedAmount)} SOL\n`; 
                        }
                    }
                }
                keyboard.inline_keyboard = [
                    [{ text: '📜 Bet History', callback_data: 'menu:history' }], 
                    [{ text: '💰 Deposit SOL', callback_data: 'quick_deposit' }, { text: '💸 Withdraw SOL', callback_data: 'menu:withdraw' }], 
                    [{ text: '🔗 Link/Update Address', callback_data: 'menu:link_wallet_prompt' }], 
                    [{ text: '↩️ Back to Main Menu', callback_data: 'menu:main' }] 
                ];
                break;

            case 'my_stats':
                text = "📊 *My Stats* \\- Coming Soon\\!"; 
                keyboard.inline_keyboard = [[{ text: '↩️ Back to Wallet', callback_data: 'menu:wallet' }]];
                break;

            case 'leaderboards':
                text = "🏆 *Leaderboards*\n\nSelect a category:";
                keyboard.inline_keyboard = [
                    [{ text: '💰 Overall Wagered', callback_data: 'leaderboard_nav:overall_wagered:0' }], 
                    [{ text: '📈 Overall Profit', callback_data: 'leaderboard_nav:overall_profit:0' }], 
                    [{ text: '↩️ Back to Main Menu', callback_data: 'menu:main' }]
                ];
                break;

            case 'history':
                await handleHistoryCommand(msgOrCbMsg, ['/history'], userId); return; 
            case 'withdraw':
                await handleWithdrawCommand(msgOrCbMsg, ['/withdraw'], userId); return; 
            case 'referral':
                await handleReferralCommand(msgOrCbMsg, ['/referral'], userId); return; 
            case 'help':
                await handleHelpCommand(msgOrCbMsg, ['/help'], userId); return; 

            default:
                console.warn(`${logPrefix} Unknown menu type in handleMenuAction: ${menuType}`);
                text = `⚠️ Unknown menu option: \`${escapeMarkdownV2(menuType)}\``; 
                keyboard.inline_keyboard = [[{ text: '↩️ Back to Main Menu', callback_data: 'menu:main' }]];
        }

        if (setState) {
            if (!messageId) {
                console.error(`${logPrefix} Cannot set state for menu '${menuType}' because messageId is missing.`);
                await safeSendMessage(chatId, text, { parse_mode: 'MarkdownV2', reply_markup: keyboard });
                return; 
            }
            setState.messageId = messageId; 
            setState.data.originalMessageId = messageId; 
            userStateCache.set(userId, setState); 
            console.log(`${logPrefix} Set user state to: ${setState.state}`);
        }

        const options = { parse_mode: 'MarkdownV2', reply_markup: keyboard };
        if (isFromCallback && messageId) {
            await bot.editMessageText(text, { chat_id: chatId, message_id: messageId, ...options })
                .catch(e => { if (!e.message.includes("message is not modified")) {
                        console.warn(`${logPrefix} Failed edit for menu ${menuType}, sending new. Error: ${e.message}`);
                        safeSendMessage(chatId, text, options);
                    }});
        } else {
            await safeSendMessage(chatId, text, options);
        }

    } catch (error) {
        console.error(`${logPrefix} Error handling menu action '${menuType}': ${error.message}`, error.stack); 
        const errorText = `⚠️ An error occurred loading this menu \\(${escapeMarkdownV2(menuType)}\\)\\. Please try again\\.`; 
         if (isFromCallback && messageId) {
             bot.editMessageText(errorText, { chat_id: chatId, message_id: messageId, parse_mode: 'MarkdownV2', reply_markup: fallbackKeyboard }).catch(()=>{
                 safeSendMessage(chatId, errorText, { parse_mode: 'MarkdownV2', reply_markup: fallbackKeyboard });
             });
         } else {
             safeSendMessage(chatId, errorText, { parse_mode: 'MarkdownV2', reply_markup: fallbackKeyboard });
         }
         if (setState) clearUserState(userId);
    }
}

// --- End of Part 5b (Section 2c) ---
// index.js - Part 5b: General Commands, Game Commands, Menus & Maps (Section 2d of 4)
// --- VERSION: 3.2.1r --- (Applying Fixes: Ensure game commands handle amounts/routing, Verify handler maps + previous)

// (Continuing directly from Part 5b, Section 2c)
// ... (Assume functions, dependencies etc. are available) ...

// --- Game Command Handlers ---
// These typically initiate the game flow, usually by showing bet buttons,
// or handle direct bet amounts if provided via command arguments.

/** Handles /coinflip or /cf command, optionally with a bet amount */
async function handleCoinflipCommand(msgOrCbMsg, args, correctUserIdFromCb = null) {
    const userId = String(correctUserIdFromCb || msgOrCbMsg.from.id);
    const chatId = String(msgOrCbMsg.chat.id);
    const isFromCallback = !!correctUserIdFromCb; // Should generally be false for commands
    // Use original message context for commands
    const messageContext = msgOrCbMsg; // msgOrCbMsg is the message object for commands
    // args[0] is the command itself (e.g., '/cf'), args[1] could be the amount
    const betAmountArg = (!isFromCallback && args?.length > 1) ? args[1] : null;
    const logPrefix = `[CoinflipCmd User ${userId}]`;

    clearUserState(userId); // Clear any previous state

    if (betAmountArg) {
        // User provided amount like /cf 0.5
        console.log(`${logPrefix} Received command with amount: ${betAmountArg}`);
        try {
            const amountSOL = parseFloat(betAmountArg);
            if (isNaN(amountSOL) || amountSOL <= 0) throw new Error("Invalid amount format");

            const amountLamports = BigInt(Math.round(amountSOL * Number(LAMPORTS_PER_SOL))); // LAMPORTS_PER_SOL from Part 1
            const gameConfig = GAME_CONFIG.coinflip; // GAME_CONFIG from Part 1

            // Validate amount against game limits
            if (amountLamports >= gameConfig.minBetLamports && amountLamports <= gameConfig.maxBetLamports) {
                // Amount is valid, proceed to the 'select side' step
                console.log(`${logPrefix} Amount valid. Proceeding to select side step.`);
                // Send a new message for the next step (as we don't edit the user's command message)
                // Call proceedToGameStep with null messageId to force sending a new message
                const nextStepCallbackData = `coinflip_select_side:${amountLamports}`;
                return proceedToGameStep(userId, chatId, null, 'coinflip', nextStepCallbackData); // proceedToGameStep from Part 5a
            } else {
                // Amount out of bounds
                safeSendMessage(chatId, `⚠️ Bet amount must be between ${escapeMarkdownV2(formatSol(gameConfig.minBetLamports))} and ${escapeMarkdownV2(formatSol(gameConfig.maxBetLamports))} SOL\\.`, {parse_mode:'MarkdownV2'}); // safeSendMessage from Part 3
            }
        } catch (e) {
            // Invalid amount format
            safeSendMessage(chatId, `⚠️ Invalid bet amount format\\. Please use a number \\(e\\.g\\., /cf 0\\.1\\)\\.`, {parse_mode:'MarkdownV2'}); // Escaped . () \
        }
    } else {
        // No amount provided, show standard bet selection buttons
        console.log(`${logPrefix} No amount provided. Showing bet amount buttons.`);
        await showBetAmountButtons(messageContext, 'coinflip', null, userId); // showBetAmountButtons from Part 1
    }
}

/** Handles /race command */
async function handleRaceCommand(msgOrCbMsg, args, correctUserIdFromCb = null) {
    const userId = String(correctUserIdFromCb || msgOrCbMsg.from.id);
    clearUserState(userId);
    // Always show bet amount buttons first for Race
    await showBetAmountButtons(msgOrCbMsg, 'race', null, userId); // msgOrCbMsg is Message object here
}

/** Handles /slots command */
async function handleSlotsCommand(msgOrCbMsg, args, correctUserIdFromCb = null) {
    const userId = String(correctUserIdFromCb || msgOrCbMsg.from.id);
    clearUserState(userId);
    // Always show bet amount buttons first for Slots
    await showBetAmountButtons(msgOrCbMsg, 'slots', null, userId); // msgOrCbMsg is Message object here
}

/** Handles /war command */
async function handleWarCommand(msgOrCbMsg, args, correctUserIdFromCb = null) {
    const userId = String(correctUserIdFromCb || msgOrCbMsg.from.id);
    clearUserState(userId);
    // Always show bet amount buttons first for War
    await showBetAmountButtons(msgOrCbMsg, 'war', null, userId); // msgOrCbMsg is Message object here
}

/** Handles /roulette command */
async function handleRouletteCommand(msgOrCbMsg, args, correctUserIdFromCb = null) {
    const userId = String(correctUserIdFromCb || msgOrCbMsg.from.id);
    clearUserState(userId);
    // Always show bet amount buttons first for Roulette
    await showBetAmountButtons(msgOrCbMsg, 'roulette', null, userId); // msgOrCbMsg is Message object here
}

/** Handles /crash command */
async function handleCrashCommand(msgOrCbMsg, args, correctUserIdFromCb = null) {
    const userId = String(correctUserIdFromCb || msgOrCbMsg.from.id);
    clearUserState(userId);
    // Always show bet amount buttons first for Crash
    await showBetAmountButtons(msgOrCbMsg, 'crash', null, userId); // msgOrCbMsg is Message object here
}

/** Handles /blackjack or /bj command */
async function handleBlackjackCommand(msgOrCbMsg, args, correctUserIdFromCb = null) {
    const userId = String(correctUserIdFromCb || msgOrCbMsg.from.id);
    clearUserState(userId);
    // Always show bet amount buttons first for Blackjack
    await showBetAmountButtons(msgOrCbMsg, 'blackjack', null, userId); // msgOrCbMsg is Message object here
}

// --- Handler Map Definitions ---
// Maps command strings (lowercase) to their handler functions.
// Ensure commandHandlers and menuCommandHandlers are declared with 'let' in Part 1.
commandHandlers = new Map([
    ['/start', handleStartCommand],
    ['/help', handleHelpCommand],
    ['/wallet', handleWalletCommand],
    ['/referral', handleReferralCommand],
    ['/deposit', handleDepositCommand],
    ['/withdraw', handleWithdrawCommand],
    ['/history', handleHistoryCommand],
    ['/leaderboards', handleLeaderboardsCommand],
    ['/games', handleGameSelectionCommand],
    ['/coinflip', handleCoinflipCommand], ['/cf', handleCoinflipCommand], // Aliases
    ['/race', handleRaceCommand],
    ['/slots', handleSlotsCommand],
    ['/roulette', handleRouletteCommand],
    ['/war', handleWarCommand],
    ['/crash', handleCrashCommand],
    ['/blackjack', handleBlackjackCommand], ['/bj', handleBlackjackCommand], // Aliases
    // '/admin' is handled separately in handleMessage
]);

// Maps menu callback data prefixes/keys to their handler functions.
// This determines what function is called when a button with matching callback_data prefix is clicked.
menuCommandHandlers = new Map([
    // Main Menu & Navigation
    ['main', handleStartCommand], // 'menu:main'
    ['game_selection', handleGameSelectionCommand], // 'menu:game_selection'
    ['wallet', handleWalletCommand], // 'menu:wallet' -> shows wallet info via handleWalletCommand
    ['referral', handleReferralCommand], // 'menu:referral' -> shows referral info via handleReferralCommand
    ['withdraw', handleWithdrawCommand], // 'menu:withdraw' -> starts withdrawal process via handleWithdrawCommand
    ['help', handleHelpCommand], // 'menu:help' -> shows main help menu via handleHelpCommand
    ['leaderboards', handleLeaderboardsCommand], // 'menu:leaderboards' -> shows leaderboard categories via handleLeaderboardsCommand
    ['history', handleHistoryCommand], // 'menu:history' -> shows bet history via handleHistoryCommand

    // Specific Actions from Menus (Often handled by handleMenuAction)
    ['link_wallet_prompt', handleMenuAction], // 'menu:link_wallet_prompt' -> handled by handleMenuAction to prompt for address
    ['my_stats', handleMenuAction], // 'menu:my_stats' -> handled by handleMenuAction (currently placeholder)

    // Direct Game Selection (Less common, usually goes via select_game first)
    ['coinflip', handleCoinflipCommand], // 'menu:coinflip' (if exists) -> handleCoinflipCommand
    ['race', handleRaceCommand], // 'menu:race' (if exists) -> handleRaceCommand
    ['slots', handleSlotsCommand], // 'menu:slots' (if exists) -> handleSlotsCommand
    ['roulette', handleRouletteCommand], // 'menu:roulette' (if exists) -> handleRouletteCommand
    ['war', handleWarCommand], // 'menu:war' (if exists) -> handleWarCommand
    ['crash', handleCrashCommand], // 'menu:crash' (if exists) -> handleCrashCommand
    ['blackjack', handleBlackjackCommand], // 'menu:blackjack' (if exists) -> handleBlackjackCommand

    // Other specific callbacks handled elsewhere (e.g., in handleCallbackQuery directly)
    // 'select_game', 'confirm_bet', 'play_again', 'custom_amount_select',
    // 'coinflip_select_side', 'race_select_horse', 'roulette_select_bet_type',
    // 'roulette_bet_type_category', 'blackjack_action', 'cash_out_crash',
    // 'quick_deposit', 'confirm_withdrawal', 'cancel_withdrawal',
    // 'show_help_section', 'leaderboard_nav'
]);

console.log("✅ Command and Menu handlers mapped."); // Add confirmation log

// --- End of Part 5b (Section 2d) ---
// --- End of Part 5b ---
// index.js - Part 6: Background Tasks, Payouts, Startup & Shutdown
// --- VERSION: 3.2.1a --- (Added clearUserState utility function)

// --- Assuming functions & constants from Parts 1, 2, 3, 5a, 5b are available ---
// Global variables like 'server', 'isFullyInitialized', 'depositMonitorIntervalId', 'sweepIntervalId',
// 'leaderboardManagerIntervalId', 'currentSlotsJackpotLamports', various caches, queues,
// 'pool', 'bot', 'solanaConnection', 'app', 'GAME_CONFIG', 'ADMIN_USER_IDS', etc., are from Part 1.
// DB ops: queryDatabase, ensureJackpotExists, getJackpotAmount, etc. (from Part 2)
// Utils: notifyAdmin, safeSendMessage, escapeMarkdownV2, sleep, formatSol, etc. (from Part 1/3)
// Handlers: handleMessage, handleCallbackQuery (from Part 5a)
// Solana specific imports like Keypair, bs58, PublicKey from Part 1 imports.

// --- Utility for this Part ---
/** Clears user state cache and associated timers */
function clearUserState(userId) {
    const state = userStateCache.get(String(userId)); // userStateCache from Part 1
    if (state) {
        // Specific logic if needed, e.g., clearing associated timeouts stored in state.data
        // For Crash/Blackjack, ensure game loop timeouts are cleared if necessary,
        // although ideally the game logic itself handles this when ending.
        // if (state.data?.gameLoopTimeoutId) clearTimeout(state.data.gameLoopTimeoutId); // Example if needed
        userStateCache.delete(String(userId));
        console.log(`[StateUtil] Cleared state for user ${userId}. State was: ${state.action || state.state || 'N/A'}`);
    }
}


// --- Database Initialization ---
async function initializeDatabase() {
    console.log('⚙️ [DB Init] Initializing Database Schema...');
    let client = null;
    try {
        client = await pool.connect(); // pool from Part 1
        await client.query('BEGIN');
        console.log('⚙️ [DB Init] Transaction started.');

        // Wallets Table
        console.log('⚙️ [DB Init] Ensuring wallets table...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS wallets (
                user_id VARCHAR(255) PRIMARY KEY,
                external_withdrawal_address VARCHAR(44), -- Solana address length
                linked_at TIMESTAMPTZ,
                last_used_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                referral_code VARCHAR(12) UNIQUE, -- e.g., ref_XXXXXXXX
                referred_by_user_id VARCHAR(255) REFERENCES wallets(user_id) ON DELETE SET NULL,
                referral_count INTEGER NOT NULL DEFAULT 0,
                total_wagered BIGINT NOT NULL DEFAULT 0,
                last_milestone_paid_lamports BIGINT NOT NULL DEFAULT 0,
                last_bet_amounts JSONB DEFAULT '{}'::jsonb, -- Definition included here
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);
        // REMOVED the ALTER TABLE check for last_bet_amounts. CREATE TABLE above must be correct.

        console.log('⚙️ [DB Init] Ensuring wallets indexes...');
        await client.query('CREATE INDEX IF NOT EXISTS idx_wallets_referral_code ON wallets (referral_code);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_wallets_referred_by ON wallets (referred_by_user_id);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_wallets_total_wagered ON wallets (total_wagered DESC);');


        // User Balances Table
        console.log('⚙️ [DB Init] Ensuring user_balances table...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS user_balances (
                user_id VARCHAR(255) PRIMARY KEY REFERENCES wallets(user_id) ON DELETE CASCADE,
                balance_lamports BIGINT NOT NULL DEFAULT 0 CHECK (balance_lamports >= 0),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);

        // Deposit Addresses Table
        console.log('⚙️ [DB Init] Ensuring deposit_addresses table...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS deposit_addresses (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                deposit_address VARCHAR(44) NOT NULL UNIQUE,
                derivation_path VARCHAR(255) NOT NULL,
                status VARCHAR(30) NOT NULL DEFAULT 'pending', -- pending, used, expired, swept, sweep_error, etc.
                expires_at TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_checked_at TIMESTAMPTZ
            );
        `);
        console.log('⚙️ [DB Init] Ensuring deposit_addresses indexes...');
        await client.query('CREATE INDEX IF NOT EXISTS idx_deposit_addresses_user_id ON deposit_addresses (user_id);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_deposit_addresses_status_expires ON deposit_addresses (status, expires_at);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_deposit_addresses_status_last_checked ON deposit_addresses (status, last_checked_at ASC NULLS FIRST);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_deposit_addresses_status_created ON deposit_addresses (status, created_at ASC);');


        // Deposits Table
        console.log('⚙️ [DB Init] Ensuring deposits table...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS deposits (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                deposit_address_id INTEGER REFERENCES deposit_addresses(id) ON DELETE SET NULL,
                tx_signature VARCHAR(88) NOT NULL UNIQUE,
                amount_lamports BIGINT NOT NULL CHECK (amount_lamports > 0),
                status VARCHAR(20) NOT NULL DEFAULT 'confirmed',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);
        console.log('⚙️ [DB Init] Ensuring deposits indexes...');
        await client.query('CREATE INDEX IF NOT EXISTS idx_deposits_user_id ON deposits (user_id);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_deposits_deposit_address_id ON deposits (deposit_address_id);');


        // Bets Table
        console.log('⚙️ [DB Init] Ensuring bets table...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS bets (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                chat_id VARCHAR(255) NOT NULL,
                game_type VARCHAR(50) NOT NULL,
                bet_details JSONB,
                wager_amount_lamports BIGINT NOT NULL CHECK (wager_amount_lamports > 0),
                payout_amount_lamports BIGINT,
                status VARCHAR(30) NOT NULL DEFAULT 'active',
                priority INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                processed_at TIMESTAMPTZ
            );
        `);
        console.log('⚙️ [DB Init] Ensuring bets indexes...');
        await client.query('CREATE INDEX IF NOT EXISTS idx_bets_user_id_status ON bets (user_id, status);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_bets_game_type_status ON bets (game_type, status);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_bets_created_at_desc ON bets (created_at DESC);');


        // Withdrawals Table
        console.log('⚙️ [DB Init] Ensuring withdrawals table...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS withdrawals (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                requested_amount_lamports BIGINT NOT NULL,
                fee_lamports BIGINT NOT NULL DEFAULT 0,
                final_send_amount_lamports BIGINT NOT NULL,
                recipient_address VARCHAR(44) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                payout_tx_signature VARCHAR(88) UNIQUE,
                error_message TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                processed_at TIMESTAMPTZ,
                completed_at TIMESTAMPTZ
            );
        `);
        console.log('⚙️ [DB Init] Ensuring withdrawals indexes...');
        await client.query('CREATE INDEX IF NOT EXISTS idx_withdrawals_user_id_status ON withdrawals (user_id, status);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_withdrawals_status ON withdrawals (status);');


        // Referral Payouts Table
        console.log('⚙️ [DB Init] Ensuring referral_payouts table...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS referral_payouts (
                id SERIAL PRIMARY KEY,
                referrer_user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                referee_user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                payout_type VARCHAR(20) NOT NULL,
                payout_amount_lamports BIGINT NOT NULL,
                triggering_bet_id INTEGER REFERENCES bets(id) ON DELETE SET NULL,
                milestone_reached_lamports BIGINT,
                status VARCHAR(20) NOT NULL DEFAULT 'pending',
                payout_tx_signature VARCHAR(88) UNIQUE,
                error_message TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                processed_at TIMESTAMPTZ,
                paid_at TIMESTAMPTZ
            );
        `);
        // Define the partial unique index separately using CREATE UNIQUE INDEX
        console.log('⚙️ [DB Init] Ensuring referral_payouts unique partial index for milestones...');
        await client.query(`
            CREATE UNIQUE INDEX IF NOT EXISTS idx_refpayout_unique_milestone ON referral_payouts
            (referrer_user_id, referee_user_id, payout_type, milestone_reached_lamports)
            WHERE (payout_type = 'milestone');
        `);
        console.log('⚙️ [DB Init] Ensuring other referral_payouts indexes...');
        await client.query('CREATE INDEX IF NOT EXISTS idx_refpayout_referrer_status ON referral_payouts (referrer_user_id, status);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_refpayout_referee ON referral_payouts (referee_user_id);');


        // Ledger Table
        console.log('⚙️ [DB Init] Ensuring ledger table...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS ledger (
                id BIGSERIAL PRIMARY KEY,
                user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                transaction_type VARCHAR(50) NOT NULL,
                amount_lamports BIGINT NOT NULL,
                balance_before BIGINT NOT NULL,
                balance_after BIGINT NOT NULL,
                related_bet_id INTEGER REFERENCES bets(id) ON DELETE SET NULL,
                related_deposit_id INTEGER REFERENCES deposits(id) ON DELETE SET NULL,
                related_withdrawal_id INTEGER REFERENCES withdrawals(id) ON DELETE SET NULL,
                related_ref_payout_id INTEGER REFERENCES referral_payouts(id) ON DELETE SET NULL,
                notes TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);
        console.log('⚙️ [DB Init] Ensuring ledger indexes...');
        await client.query('CREATE INDEX IF NOT EXISTS idx_ledger_user_id_created_desc ON ledger (user_id, created_at DESC);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_ledger_transaction_type ON ledger (transaction_type);');
        // Corrected conditional indexes: WHERE clause outside parentheses
        await client.query('CREATE INDEX IF NOT EXISTS idx_ledger_related_bet_id ON ledger (related_bet_id) WHERE related_bet_id IS NOT NULL;');
        await client.query('CREATE INDEX IF NOT EXISTS idx_ledger_related_withdrawal_id ON ledger (related_withdrawal_id) WHERE related_withdrawal_id IS NOT NULL;');


        // Jackpots Table
        console.log('⚙️ [DB Init] Ensuring jackpots table...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS jackpots (
                game_key VARCHAR(50) PRIMARY KEY,
                current_amount_lamports BIGINT NOT NULL DEFAULT 0 CHECK (current_amount_lamports >= 0),
                last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        `);

        // Leaderboards Table
        console.log('⚙️ [DB Init] Ensuring game_leaderboards table...');
        await client.query(`
            CREATE TABLE IF NOT EXISTS game_leaderboards (
                id SERIAL PRIMARY KEY,
                game_key VARCHAR(50) NOT NULL,
                user_id VARCHAR(255) NOT NULL REFERENCES wallets(user_id) ON DELETE CASCADE,
                score_type VARCHAR(50) NOT NULL,
                period_type VARCHAR(20) NOT NULL,
                period_identifier VARCHAR(50) NOT NULL,
                score BIGINT NOT NULL,
                player_display_name VARCHAR(255),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (game_key, user_id, score_type, period_type, period_identifier)
            );
        `);
        console.log('⚙️ [DB Init] Ensuring game_leaderboards indexes...');
        await client.query('CREATE INDEX IF NOT EXISTS idx_leaderboards_lookup ON game_leaderboards (game_key, score_type, period_type, period_identifier, score DESC);');
        await client.query('CREATE INDEX IF NOT EXISTS idx_leaderboards_user ON game_leaderboards (user_id, game_key, period_type);');

        await client.query('COMMIT');
        console.log('✅ [DB Init] Database schema initialized/verified successfully.');
    } catch (err) {
        console.error('❌ CRITICAL DATABASE INITIALIZATION ERROR:', err);
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { console.error('Rollback failed:', rbErr); } }
        if (typeof notifyAdmin === "function") { // notifyAdmin from Part 3
            try { await notifyAdmin(`🚨 CRITICAL DB INIT FAILED: ${escapeMarkdownV2(String(err.message || err))}\\. Bot cannot start\\. Check logs immediately\\.`).catch(()=>{}); } catch {} // Escaped .
        } else {
            console.error(`[ADMIN ALERT during DB Init Error] DB Init failed: ${String(err.message || err)}`)
        }
        process.exit(2);
    } finally {
        if (client) client.release();
    }
}

async function loadActiveDepositsCache() {
    const logPrefix = '[LoadActiveDeposits]';
    console.log(`⚙️ ${logPrefix} Loading active deposit addresses from DB into cache...`);
    let count = 0; let expiredCount = 0;
    try {
        const res = await queryDatabase(`SELECT deposit_address, user_id, expires_at FROM deposit_addresses WHERE status = 'pending'`); // queryDatabase from Part 2
        const now = Date.now();
        for (const row of res.rows) {
            const expiresAtTime = new Date(row.expires_at).getTime();
            if (now < expiresAtTime) {
                addActiveDepositAddressCache(row.deposit_address, row.user_id, expiresAtTime); // from Part 3
                count++;
            } else {
                expiredCount++;
                queryDatabase("UPDATE deposit_addresses SET status = 'expired' WHERE deposit_address = $1 AND status = 'pending'", [row.deposit_address]) // from Part 2
                   .catch(e => console.error(`${logPrefix} Failed to mark old address ${row.deposit_address} as expired during cache load: ${e.message}`));
            }
        }
        console.log(`✅ ${logPrefix} Loaded ${count} active deposit addresses into cache. Found and processed ${expiredCount} already expired.`);
    } catch (error) {
        console.error(`❌ ${logPrefix} Error loading active deposits: ${error.message}`);
        if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 ERROR Loading active deposit cache on startup: ${escapeMarkdownV2(String(error.message || error))}`); // Escaped
    }
}

async function loadSlotsJackpot() {
    const logPrefix = '[LoadSlotsJackpot]';
    console.log(`⚙️ ${logPrefix} Loading/Ensuring slots jackpot...`);
    let client = null;
    let loadedJackpotAmount = SLOTS_JACKPOT_SEED_LAMPORTS; // from Part 1 constants
    try {
        client = await pool.connect(); // pool from Part 1
        await client.query('BEGIN');
        const ensured = await ensureJackpotExists('slots', SLOTS_JACKPOT_SEED_LAMPORTS, client); // from Part 2
        if (!ensured) {
            console.warn(`${logPrefix} Failed to ensure slots jackpot exists in DB. Will use seed value ${formatSol(SLOTS_JACKPOT_SEED_LAMPORTS)} SOL.`); // formatSol from Part 3
        } else {
            loadedJackpotAmount = await getJackpotAmount('slots', client); // from Part 2
        }
        await client.query('COMMIT');
        console.log(`✅ ${logPrefix} Slots jackpot in DB is ${formatSol(loadedJackpotAmount)} SOL.`);
        return loadedJackpotAmount;
    } catch (error) {
        if (client) { try { await client.query('ROLLBACK'); } catch (rbErr) { console.error(`${logPrefix} Rollback error on jackpot load: ${rbErr.message}`);} }
        console.error(`❌ ${logPrefix} Error loading slots jackpot: ${error.message}. Using seed value ${formatSol(SLOTS_JACKPOT_SEED_LAMPORTS)} SOL.`); // Escaped .
        if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 ERROR Loading slots jackpot: ${escapeMarkdownV2(String(error.message || error))}. Using seed value.`); // Escaped .
        return SLOTS_JACKPOT_SEED_LAMPORTS;
    } finally {
        if (client) client.release();
    }
}

function startDepositMonitor() {
    let intervalMs = parseInt(process.env.DEPOSIT_MONITOR_INTERVAL_MS, 10);
    if (isNaN(intervalMs) || intervalMs < 5000) {
        intervalMs = 20000;
        console.warn(`[DepositMonitor] Invalid DEPOSIT_MONITOR_INTERVAL_MS, using default ${intervalMs}ms.`);
    }
    if (depositMonitorIntervalId) { // from Part 1 state
        clearInterval(depositMonitorIntervalId);
        console.log('🔄 [DepositMonitor] Restarting deposit monitor...');
    } else {
        console.log(`⚙️ [DepositMonitor] Starting Deposit Monitor (Polling Interval: ${intervalMs / 1000}s)...`);
    }
    const initialDelay = parseInt(process.env.INIT_DELAY_MS, 10) || 3000; // from Part 1 env
    console.log(`[DepositMonitor] Scheduling first monitor run in ${initialDelay/1000}s...`);

    setTimeout(() => {
        try {
            console.log(`[DepositMonitor] Executing first monitor run...`);
            monitorDepositsPolling().catch(err => console.error("❌ [Initial Deposit Monitor Run] Error:", err.message, err.stack));
            depositMonitorIntervalId = setInterval(monitorDepositsPolling, intervalMs);
            if (depositMonitorIntervalId.unref) depositMonitorIntervalId.unref(); // Allow process to exit if only interval remains
            console.log(`✅ [DepositMonitor] Recurring monitor interval set.`);
        } catch (initialRunError) {
            console.error("❌ [DepositMonitor] CRITICAL ERROR during initial monitor setup/run:", initialRunError);
            if (typeof notifyAdmin === "function") notifyAdmin(`🚨 CRITICAL ERROR setting up Deposit Monitor interval: ${escapeMarkdownV2(String(initialRunError.message || initialRunError))}`).catch(()=>{}); // Escaped
        }
    }, initialDelay);
}

monitorDepositsPolling.isRunning = false; // Prevent overlapping runs
async function monitorDepositsPolling() {
    const logPrefix = '[DepositMonitor Polling]';
    if (monitorDepositsPolling.isRunning) {
        console.log(`${logPrefix} Run skipped, previous run still active.`);
        return;
    }
    monitorDepositsPolling.isRunning = true;
    let batchUpdateClient = null; // Client for batch updating last_checked_at

    try {
        const batchSize = parseInt(process.env.DEPOSIT_MONITOR_ADDRESS_BATCH_SIZE, 10) || 50;
        const sigFetchLimit = parseInt(process.env.DEPOSIT_MONITOR_SIGNATURE_FETCH_LIMIT, 10) || 5;

        const pendingAddressesRes = await queryDatabase(
            `SELECT id, deposit_address, user_id, derivation_path, expires_at
             FROM deposit_addresses
             WHERE status = 'pending' AND expires_at > NOW()
             ORDER BY last_checked_at ASC NULLS FIRST, created_at ASC
             LIMIT $1`,
            [batchSize]
        );

        if (pendingAddressesRes.rowCount === 0) {
            // console.log(`${logPrefix} No pending addresses found to check.`);
            monitorDepositsPolling.isRunning = false;
            return;
        }
         console.log(`${logPrefix} Found ${pendingAddressesRes.rowCount} pending addresses to check.`);

        // Batch update last_checked_at for the selected addresses
        batchUpdateClient = await pool.connect();
        await batchUpdateClient.query('BEGIN');
        const addressIdsToCheck = pendingAddressesRes.rows.map(r => r.id);
        await batchUpdateClient.query('UPDATE deposit_addresses SET last_checked_at = NOW() WHERE id = ANY($1::int[])', [addressIdsToCheck]);
        await batchUpdateClient.query('COMMIT');
        batchUpdateClient.release(); batchUpdateClient = null; // Release immediately after commit


        for (const row of pendingAddressesRes.rows) {
            const address = row.deposit_address;
            const addressId = row.id;
            const userId = row.user_id;
            const addrLogPrefix = `[Monitor Addr:${address.slice(0,6)}.. ID:${addressId} User:${userId}]`;

            try {
                const pubKey = new PublicKey(address); // PublicKey from Part 1
                // solanaConnection from Part 1
                // DEPOSIT_CONFIRMATION_LEVEL from Part 1 constants
                const signatures = await solanaConnection.getSignaturesForAddress(
                    pubKey, { limit: sigFetchLimit }, DEPOSIT_CONFIRMATION_LEVEL
                );

                if (signatures && signatures.length > 0) {
                    // Process oldest signatures first to catch the deposit correctly
                    for (const sigInfo of signatures.reverse()) {
                        if (sigInfo?.signature && !hasProcessedDepositTx(sigInfo.signature)) { // hasProcessedDepositTx from Part 3 cache utils
                             // Check confirmation status allows 'confirmed' or 'finalized' (or 'processed' as lower level)
                            const isConfirmed = sigInfo.confirmationStatus === DEPOSIT_CONFIRMATION_LEVEL || sigInfo.confirmationStatus === 'finalized' || sigInfo.confirmationStatus === 'processed';
                            if (!sigInfo.err && isConfirmed) {
                                console.log(`${addrLogPrefix} Found new confirmed/processed TX: ${sigInfo.signature}. Queuing for processing.`);
                                // depositProcessorQueue from Part 1
                                depositProcessorQueue.add(() => processDepositTransaction(sigInfo.signature, address, addressId, userId))
                                   .catch(queueError => console.error(`❌ ${addrLogPrefix} Error adding TX ${sigInfo.signature} to deposit queue: ${queueError.message}`));
                                addProcessedDepositTx(sigInfo.signature); // from Part 3 cache utils - Add immediately to prevent re-queue
                            } else if (sigInfo.err) {
                                // If transaction failed on chain, mark as processed so we don't check it again.
                                console.log(`${addrLogPrefix} Found failed TX ${sigInfo.signature}. Marking as processed locally. Error: ${JSON.stringify(sigInfo.err)}`);
                                addProcessedDepositTx(sigInfo.signature);
                            }
                            // Ignore signatures that haven't reached the desired confirmation level yet.
                        }
                    }
                }
            } catch (error) {
                console.error(`❌ ${addrLogPrefix} Error checking signatures: ${error.message}`);
                 // isRetryableSolanaError from Part 3
                if (isRetryableSolanaError(error) && (error?.status === 429 || String(error?.message).toLowerCase().includes('rate limit'))) {
                    console.warn(`🚦 ${addrLogPrefix} Rate limit detected while fetching signatures. Pausing briefly...`);
                    await sleep(2000 + Math.random() * 1000); // sleep from Part 1
                }
            }
            // Optional short delay between checking each address to further mitigate potential RPC limits
            // await sleep(50);
        }
    } catch (error) {
        console.error(`❌ ${logPrefix} Error in main polling loop: ${error.message}`, error.stack);
        if (batchUpdateClient) {
            try { await batchUpdateClient.query('ROLLBACK'); } catch (rbErr) { console.error(`${logPrefix} Rollback failed for last_checked_at update: ${rbErr.message}`); }
            batchUpdateClient.release(); // Ensure release even on error
        }
        if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 ERROR in Deposit Monitor loop: ${escapeMarkdownV2(String(error.message || error))}`); // Escaped
    } finally {
        if (batchUpdateClient) batchUpdateClient.release(); // Final safety release
        monitorDepositsPolling.isRunning = false;
    }
}

async function processDepositTransaction(signature, depositAddress, depositAddressId, userId) {
    const logPrefix = `[ProcessDeposit TX:${signature.slice(0,6)}.. AddrID:${depositAddressId} User:${userId}]`;
    console.log(`${logPrefix} Processing transaction...`);
    let client = null;

    try {
        const tx = await solanaConnection.getTransaction(signature, { // solanaConnection from Part 1
            maxSupportedTransactionVersion: 0, commitment: DEPOSIT_CONFIRMATION_LEVEL // Constants from Part 1
        });

        if (!tx) {
            // Might happen if RPC node hasn't seen the TX yet even though signature appeared.
            console.warn(`⚠️ ${logPrefix} Transaction details not found via getTransaction for ${signature}. RPC lag or invalid/unconfirmed signature? Will retry on next monitor cycle.`);
            // Remove from processed cache so monitor can pick it up again if valid
            // processedDepositTxSignatures.delete(signature); // From Part 1 state (Set)
            // Reconsider removing from cache - might cause infinite loop if TX never appears. Better to keep it marked and log warning.
            return;
        }

        if (tx.meta?.err) {
            console.log(`ℹ️ ${logPrefix} Transaction ${signature} has failed on-chain (meta.err: ${JSON.stringify(tx.meta.err)}). Ignoring.`);
            addProcessedDepositTx(signature); // from Part 3 cache utils - ensure failed TX aren't re-processed
            return;
        }

        const { transferAmount, payerAddress } = analyzeTransactionAmounts(tx, depositAddress); // from Part 3

        if (transferAmount <= 0n) {
            console.log(`ℹ️ ${logPrefix} No positive SOL transfer found to target address in TX ${signature}. Ignoring.`);
            addProcessedDepositTx(signature);
            return;
        }
        const depositAmountSOL = formatSol(transferAmount); // formatSol from Part 3
        console.log(`✅ ${logPrefix} Valid deposit: ${depositAmountSOL} SOL from ${payerAddress || 'unknown'} for TX ${signature}.`);

        client = await pool.connect(); // pool from Part 1
        await client.query('BEGIN');
        console.log(`${logPrefix} DB transaction started for ${signature}.`);

        // recordConfirmedDeposit (from Part 2) handles the ON CONFLICT for tx_signature
        const depositRecordResult = await recordConfirmedDeposit(client, userId, depositAddressId, signature, transferAmount);
        if (depositRecordResult.alreadyProcessed) {
            console.warn(`⚠️ ${logPrefix} TX ${signature} already processed by DB (ID: ${depositRecordResult.depositId}). Rolling back this attempt.`);
            await client.query('ROLLBACK');
            addProcessedDepositTx(signature); // Ensure it's in cache if DB caught conflict
            return;
        }
        if (!depositRecordResult.success || !depositRecordResult.depositId) {
            throw new Error(`Failed to record deposit in DB for ${signature}: ${depositRecordResult.error}`);
        }
        const depositId = depositRecordResult.depositId;
        console.log(`${logPrefix} Deposit ID ${depositId} recorded for TX ${signature}.`);

        const markedUsed = await markDepositAddressUsed(client, depositAddressId); // from Part 2
        if (!markedUsed) {
            // This is unusual if status was 'pending', but proceed anyway.
            console.warn(`⚠️ ${logPrefix} Could not mark deposit address ID ${depositAddressId} as 'used' (status might not have been 'pending'). Proceeding with balance update.`);
        } else {
            console.log(`${logPrefix} Marked deposit address ID ${depositAddressId} as 'used'.`);
        }

        const ledgerNote = `Deposit from ${payerAddress ? payerAddress.slice(0,6)+'..' : 'Unknown'} TX:${signature.slice(0,6)}..`;
        // updateUserBalanceAndLedger (from Part 2) updates balance and creates ledger entry
        const balanceUpdateResult = await updateUserBalanceAndLedger(client, userId, transferAmount, 'deposit', { depositId }, ledgerNote);
        if (!balanceUpdateResult.success || balanceUpdateResult.newBalance === undefined) {
            throw new Error(`Failed to update user ${userId} balance/ledger for TX ${signature}: ${balanceUpdateResult.error}`);
        }
        const newBalanceSOL = formatSol(balanceUpdateResult.newBalance);
        console.log(`${logPrefix} User ${userId} balance updated. New balance: ${newBalanceSOL} SOL after TX ${signature}.`);

        // --- Referral Linking on First Deposit ---
        // pendingReferrals, PENDING_REFERRAL_TTL_MS from Part 1 state
        const pendingRef = pendingReferrals.get(userId);
        if (pendingRef && pendingRef.referrerUserId && (Date.now() - pendingRef.timestamp < PENDING_REFERRAL_TTL_MS)) {
            console.log(`${logPrefix} Found pending referral for User ${userId} from ${pendingRef.referrerUserId}. Attempting link for TX ${signature}.`);
            // Check if user is already referred before attempting link (within transaction)
            const refereeWalletCheck = await queryDatabase('SELECT referred_by_user_id FROM wallets WHERE user_id = $1 FOR UPDATE', [userId], client); // queryDatabase from Part 2
            if (refereeWalletCheck.rowCount > 0 && !refereeWalletCheck.rows[0].referred_by_user_id) {
                 // linkReferral from Part 2
                if (await linkReferral(userId, pendingRef.referrerUserId, client)) {
                    console.log(`✅ ${logPrefix} Referral link successful: ${pendingRef.referrerUserId} -> ${userId} (TX ${signature}).`);
                    pendingReferrals.delete(userId); // Remove from pending cache
                    try {
                        // Notify referrer
                        const refereeDisplayName = await getUserDisplayName(userId, pendingRef.referrerUserId); // getUserDisplayName from Part 3
                         // MarkdownV2 Safety: Escape name, punctuation
                        safeSendMessage(pendingRef.referrerUserId, `🤝 Your referral ${escapeMarkdownV2(refereeDisplayName || `User ${userId.slice(-4)}`)} just made their first deposit and is now linked\\! Thanks\\!`, { parse_mode: 'MarkdownV2' }); // safeSendMessage from Part 3, Escaped !
                    } catch (notifyError) { console.warn(`${logPrefix} Failed to send referral link notification to ${pendingRef.referrerUserId}: ${notifyError.message}`); }
                } else {
                    console.warn(`⚠️ ${logPrefix} linkReferral function failed (maybe already linked concurrently by another process for TX ${signature}?). This attempt won't link.`);
                    pendingReferrals.delete(userId); // Remove anyway as link failed/unnecessary
                }
            } else {
                console.log(`${logPrefix} User ${userId} was already referred by ${refereeWalletCheck.rows[0]?.referred_by_user_id || 'N/A'} or wallet not found during referral link attempt. Removing pending referral.`);
                pendingReferrals.delete(userId);
            }
        } else if (pendingRef) { // Referral exists but timed out
            pendingReferrals.delete(userId);
            console.log(`${logPrefix} Expired pending referral removed for user ${userId}.`);
        }
        // --- End Referral Linking ---

        await client.query('COMMIT');
        console.log(`✅ ${logPrefix} DB transaction committed successfully for TX ${signature}.`);

        // Notify user AFTER successful commit
         // MarkdownV2 Safety: Escape amounts, signature, commands
        await safeSendMessage(userId,
            `✅ *Deposit Confirmed\\!* ✅\n\n` +
            `Amount: *${escapeMarkdownV2(depositAmountSOL)} SOL*\n` +
            `New Balance: *${escapeMarkdownV2(newBalanceSOL)} SOL*\n` +
            `TX: \`${escapeMarkdownV2(signature)}\`\n\n` +
            `You can now use /start or /games to play\\!`, // Escaped !
            { parse_mode: 'MarkdownV2' }
        );
        addProcessedDepositTx(signature); // Add again to be sure (Set handles duplicates)

    } catch (error) {
        console.error(`❌ ${logPrefix} CRITICAL ERROR processing deposit ${signature}: ${error.message}`, error.stack);
        if (client) {
            try { await client.query('ROLLBACK'); console.log(`ℹ️ ${logPrefix} Transaction rolled back due to error for TX ${signature}.`); }
            catch (rbErr) { console.error(`❌ ${logPrefix} Rollback failed for TX ${signature}:`, rbErr); }
        }
         // MarkdownV2 Safety: Escape signature, address, user ID, error
        if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 CRITICAL Error Processing Deposit TX \`${escapeMarkdownV2(signature)}\` Addr \`${escapeMarkdownV2(depositAddress)}\` User \`${escapeMarkdownV2(userId)}\`:\n${escapeMarkdownV2(String(error.message || error))}`);
        addProcessedDepositTx(signature); // Ensure problematic TX isn't retried infinitely
    } finally {
        if (client) client.release();
    }
}

function startDepositSweeper() {
    let intervalMs = parseInt(process.env.SWEEP_INTERVAL_MS, 10);
    if (isNaN(intervalMs) || intervalMs <= 0) {
        console.warn("⚠️ [DepositSweeper] Sweeping is disabled (SWEEP_INTERVAL_MS not set or zero).");
        return;
    }
    if (intervalMs < 60000) {
        intervalMs = 60000;
        console.warn(`⚠️ [DepositSweeper] SWEEP_INTERVAL_MS too low, enforcing minimum ${intervalMs}ms.`);
    }

    if (sweepIntervalId) { // from Part 1 state
        clearInterval(sweepIntervalId);
        console.log('🔄 [DepositSweeper] Restarting deposit sweeper...');
    } else {
        console.log(`⚙️ [DepositSweeper] Starting Deposit Sweeper (Interval: ${intervalMs / 1000}s)...`);
    }

    const initialDelay = (parseInt(process.env.INIT_DELAY_MS, 10) || 5000) + 10000; // Stagger slightly after deposit monitor starts
    console.log(`⚙️ [DepositSweeper] Scheduling first sweep run in ${initialDelay/1000}s...`);
    setTimeout(() => {
        try {
            console.log(`⚙️ [DepositSweeper] Executing first sweep run...`);
            sweepDepositAddresses().catch(err => console.error("❌ [Initial Sweep Run] Error:", err.message, err.stack));
            sweepIntervalId = setInterval(sweepDepositAddresses, intervalMs);
            if (sweepIntervalId.unref) sweepIntervalId.unref(); // Allow exit
            console.log(`✅ [DepositSweeper] Recurring sweep interval set.`);
        } catch (initialRunError) {
            console.error("❌ [DepositSweeper] CRITICAL ERROR during initial sweep setup/run:", initialRunError);
             // MarkdownV2 Safety: Escape error message
            if (typeof notifyAdmin === "function") notifyAdmin(`🚨 CRITICAL ERROR setting up Deposit Sweeper interval: ${escapeMarkdownV2(String(initialRunError.message || initialRunError))}`).catch(()=>{}); // Escaped
        }
    }, initialDelay);
}

sweepDepositAddresses.isRunning = false; // Prevent overlapping runs
async function sweepDepositAddresses() {
    const logPrefix = '[DepositSweeper]';
    if (sweepDepositAddresses.isRunning) {
        console.log(`${logPrefix} Run skipped, previous sweep still active.`);
        return;
    }
    sweepDepositAddresses.isRunning = true;
    console.log(`🧹 ${logPrefix} Starting sweep cycle...`);
    let addressesProcessed = 0, addressesSwept = 0, sweepErrors = 0, lowBalanceSkipped = 0;

    try {
        const batchSize = parseInt(process.env.SWEEP_BATCH_SIZE, 10) || 20;
        const mainBotPrivateKeyBase58 = process.env.MAIN_BOT_PRIVATE_KEY;
        if (!mainBotPrivateKeyBase58) {
            console.error(`❌ ${logPrefix} MAIN_BOT_PRIVATE_KEY not set. Cannot determine sweep target address.`);
            sweepDepositAddresses.isRunning = false;
            if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 [DepositSweeper] MAIN_BOT_PRIVATE_KEY not set. Sweeping disabled.`);
            return;
        }
        const sweepTargetKeypair = Keypair.fromSecretKey(bs58.decode(mainBotPrivateKeyBase58)); // Keypair, bs58 from Part 1
        const sweepTargetAddress = sweepTargetKeypair.publicKey;

        const delayBetweenAddressesMs = parseInt(process.env.SWEEP_ADDRESS_DELAY_MS, 10) || 750;
        const maxRetryAttemptsPerAddress = (parseInt(process.env.SWEEP_RETRY_ATTEMPTS, 10) || 1) + 1; // +1 because loop starts at 1
        const retryDelayBaseMs = parseInt(process.env.SWEEP_RETRY_DELAY_MS, 10) || 3000;
        const rentLamports = BigInt(await solanaConnection.getMinimumBalanceForRentExemption(0)); // solanaConnection from Part 1
        const feeBufferLamports = SWEEP_FEE_BUFFER_LAMPORTS; // from Part 1 constants
        // Leave enough for rent + buffer for potential transaction fees if needed in future
        const minimumLamportsToLeaveAfterSweep = rentLamports + feeBufferLamports;

        const addressesRes = await queryDatabase(
            `SELECT id, deposit_address, derivation_path
             FROM deposit_addresses WHERE status = 'used' ORDER BY created_at ASC LIMIT $1`,
            [batchSize]
        );

        if (addressesRes.rowCount === 0) {
             console.log(`${logPrefix} No 'used' addresses found needing sweep in this batch.`);
            sweepDepositAddresses.isRunning = false;
            return;
        }
        console.log(`${logPrefix} Found ${addressesRes.rowCount} 'used' addresses potentially needing sweep.`);

        for (const row of addressesRes.rows) {
            addressesProcessed++;
            const addressId = row.id;
            const addressString = row.deposit_address;
            const derivationPath = row.derivation_path;
            const addrLogPrefix = `[Sweep Addr:${addressString.slice(0,6)}.. ID:${addressId}]`;

            if (!derivationPath || derivationPath === 'UNKNOWN_MIGRATED') {
                console.warn(`⚠️ ${addrLogPrefix} Missing or unknown derivation path. Cannot derive key to sweep. Marking as 'sweep_error'.`);
                await queryDatabase("UPDATE deposit_addresses SET status = 'sweep_error' WHERE id = $1 AND status = 'used'", [addressId]); // queryDatabase from Part 2
                 // MarkdownV2 Safety: Escape address, ID
                if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 SWEEP FAILED (No Path) Addr \`${escapeMarkdownV2(addressString)}\` (ID: ${addressId}). Manual check needed.`); // Escaped ()
                sweepErrors++;
                continue;
            }

            try {
                 const depositKeypair = getKeypairFromPath(derivationPath); // from Part 3
                 if (!depositKeypair) {
                     throw new Error("Failed to derive keypair from path.");
                 }
                 if (depositKeypair.publicKey.toBase58() !== addressString) {
                     console.error(`❌ CRITICAL ${addrLogPrefix} Derived public key mismatch! DB: ${addressString}, Derived: ${depositKeypair.publicKey.toBase58()}. Skipping sweep & marking error.`);
                     await queryDatabase("UPDATE deposit_addresses SET status = 'sweep_error_key_mismatch' WHERE id = $1 AND status = 'used'", [addressId]);
                     sweepErrors++;
                      // MarkdownV2 Safety: Escape address, ID, path
                     if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 CRITICAL SWEEP KEY MISMATCH Addr \`${escapeMarkdownV2(addressString)}\` (ID: ${addressId}). Path: ${escapeMarkdownV2(derivationPath)}. Manual check needed.`); // Escaped ()
                     continue;
                 }

                 const balanceLamports = BigInt(await solanaConnection.getBalance(depositKeypair.publicKey, DEPOSIT_CONFIRMATION_LEVEL)); // DEPOSIT_CONFIRMATION_LEVEL from Part 1

                 if (balanceLamports <= minimumLamportsToLeaveAfterSweep) {
                     console.log(`${addrLogPrefix} Balance ${formatSol(balanceLamports)} SOL is too low to sweep (Min required: ${formatSol(minimumLamportsToLeaveAfterSweep)} SOL). Marking 'swept_low_balance'.`); // formatSol from Part 3
                     lowBalanceSkipped++;
                     await queryDatabase("UPDATE deposit_addresses SET status = 'swept_low_balance' WHERE id = $1 AND status = 'used'", [addressId]);
                     continue;
                 }

                 const amountToSweep = balanceLamports - minimumLamportsToLeaveAfterSweep;
                 console.log(`${addrLogPrefix} Balance: ${formatSol(balanceLamports)} SOL. Attempting to sweep ${formatSol(amountToSweep)} SOL to ${sweepTargetAddress.toBase58()}...`);

                 let sweepSuccessThisAddress = false;
                 let sweepTxSignature = null;
                 let lastSweepError = null;

                 for (let attempt = 1; attempt <= maxRetryAttemptsPerAddress; attempt++) {
                     try {
                          const { blockhash, lastValidBlockHeight } = await solanaConnection.getLatestBlockhash(DEPOSIT_CONFIRMATION_LEVEL);
                          const transaction = new Transaction({ recentBlockhash: blockhash, feePayer: depositKeypair.publicKey }) // Transaction, SystemProgram from Part 1
                              .add( SystemProgram.transfer({
                                  fromPubkey: depositKeypair.publicKey,
                                  toPubkey: sweepTargetAddress,
                                  lamports: amountToSweep
                              }));
                          // Send sweep transaction (sendAndConfirmTransaction from Part 1)
                          sweepTxSignature = await sendAndConfirmTransaction(
                              solanaConnection, transaction, [depositKeypair],
                              { commitment: DEPOSIT_CONFIRMATION_LEVEL, skipPreflight: false, preflightCommitment: DEPOSIT_CONFIRMATION_LEVEL, lastValidBlockHeight }
                          );
                          sweepSuccessThisAddress = true;
                          console.log(`✅ ${addrLogPrefix} Sweep successful on attempt ${attempt}. TX: ${sweepTxSignature}`);
                          break; // Exit retry loop on success
                     } catch (error) {
                          lastSweepError = error;
                          console.warn(`⚠️ ${addrLogPrefix} Sweep Attempt ${attempt}/${maxRetryAttemptsPerAddress} FAILED: ${error.message}`);
                          // isRetryableSolanaError from Part 3
                          if (isRetryableSolanaError(error) && attempt < maxRetryAttemptsPerAddress) {
                              const jitterDelay = retryDelayBaseMs * Math.pow(1.5, attempt - 1) * (0.8 + Math.random() * 0.4);
                              console.log(`⏳ ${addrLogPrefix} Retrying sweep in ~${Math.round(jitterDelay / 1000)}s...`);
                              await sleep(jitterDelay); // sleep from Part 1
                          } else {
                              console.error(`❌ ${addrLogPrefix} Sweep failed permanently for this address after ${attempt} attempts.`);
                              break; // Exit retry loop on permanent failure or max attempts
                          }
                     }
                 } // End retry loop

                 if (sweepSuccessThisAddress && sweepTxSignature) {
                     await queryDatabase("UPDATE deposit_addresses SET status = 'swept' WHERE id = $1 AND status = 'used'", [addressId]);
                     addressesSwept++;
                 } else {
                     sweepErrors++;
                     const finalErrorMsg = lastSweepError?.message || "Unknown permanent sweep failure";
                     console.error(`❌ ${addrLogPrefix} Final Sweep outcome: FAILED for address ${addressString}. Last Error: ${finalErrorMsg}`);
                     await queryDatabase("UPDATE deposit_addresses SET status = 'sweep_error' WHERE id = $1 AND status = 'used'", [addressId]);
                      // MarkdownV2 Safety: Escape address, ID, error
                     if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 SWEEP FAILED (Permanent) Addr \`${escapeMarkdownV2(addressString)}\` (ID: ${addressId}): ${escapeMarkdownV2(finalErrorMsg)}. Manual check needed.`); // Escaped ()
                 }
            } catch (processAddrError) {
                 sweepErrors++;
                 console.error(`❌ ${addrLogPrefix} Error processing address ${addressString} for sweep: ${processAddrError.message}`, processAddrError.stack);
                 await queryDatabase("UPDATE deposit_addresses SET status = 'sweep_error_processing' WHERE id = $1 AND status = 'used'", [addressId]);
                  // MarkdownV2 Safety: Escape address, ID, error
                 if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 SWEEP FAILED (Processing Error) Addr \`${escapeMarkdownV2(addressString)}\` (ID: ${addressId}): ${escapeMarkdownV2(String(processAddrError.message || processAddrError))}. Manual check needed.`); // Escaped ()
            }
            // Delay between processing each address in the batch
            if (delayBetweenAddressesMs > 0 && addressesProcessed < addressesRes.rowCount) {
                await sleep(delayBetweenAddressesMs);
            }
        } // End loop through addresses in batch

        console.log(`🧹 ${logPrefix} Sweep cycle finished. Processed: ${addressesProcessed}, Swept OK: ${addressesSwept}, Low Balance Skipped: ${lowBalanceSkipped}, Errors: ${sweepErrors}.`);

    } catch (error) {
        console.error(`❌ ${logPrefix} CRITICAL ERROR in main sweep cycle: ${error.message}`, error.stack);
         // MarkdownV2 Safety: Escape error message
        if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 CRITICAL Error in Deposit Sweeper main loop: ${escapeMarkdownV2(String(error.message || error))}.`); // Escaped .
    } finally {
        sweepDepositAddresses.isRunning = false;
    }
}

async function updateLeaderboardsCycle() {
    const logPrefix = '[LeaderboardManager Cycle]';
    console.log(`🏆 ${logPrefix} Starting leaderboard update cycle...`);
    // Placeholder / Example structure - Needs real implementation
    // Fetch relevant betting data (e.g., total wagered, total profit per user/game/period)
    // Store/Update results in the `game_leaderboards` table

    // Example: Updating 'overall_wagered' for 'all_time'
    try {
        // This query updates based on the `wallets` table's `total_wagered` column
        await queryDatabase(`
            INSERT INTO game_leaderboards (game_key, user_id, score_type, period_type, period_identifier, score, player_display_name, updated_at)
            SELECT 'overall', w.user_id, 'total_wagered', 'all_time', 'all', w.total_wagered, COALESCE(w.referral_code, 'User...' || RIGHT(w.user_id, 4)), NOW()
            FROM wallets w
            WHERE w.total_wagered > 0
            ON CONFLICT (game_key, user_id, score_type, period_type, period_identifier)
            DO UPDATE SET
                score = EXCLUDED.score,
                player_display_name = EXCLUDED.player_display_name,
                updated_at = NOW()
            WHERE game_leaderboards.score <> EXCLUDED.score OR game_leaderboards.player_display_name <> EXCLUDED.player_display_name;
        `);
        console.log(`${logPrefix} Updated 'overall_wagered' / 'all_time' leaderboard.`);
    } catch (error) {
        console.error(`❌ ${logPrefix} Error updating overall_wagered leaderboard: ${error.message}`);
    }

    // TODO: Implement similar logic for daily/weekly periods and profit calculations, likely requiring aggregation from the `bets` table.
    console.log(`🏆 ${logPrefix} Leaderboard update cycle finished (basic implementation).`);
}

function startLeaderboardManager() {
    const logPrefix = '[LeaderboardManager Start]';
    console.log(`⚙️ ${logPrefix} Initializing Leaderboard Manager...`);
    const intervalMs = parseInt(process.env.LEADERBOARD_UPDATE_INTERVAL_MS, 10); // from Part 1 env

    if (isNaN(intervalMs) || intervalMs <= 0) {
        console.warn(`⚠️ ${logPrefix} Leaderboard updates are disabled (LEADERBOARD_UPDATE_INTERVAL_MS not set or invalid).`);
        return;
    }

    if (leaderboardManagerIntervalId) { // from Part 1 state
        clearInterval(leaderboardManagerIntervalId);
        console.log(`🔄 ${logPrefix} Restarting leaderboard manager...`);
    }

    const initialDelayMs = (parseInt(process.env.INIT_DELAY_MS, 10) || 5000) + 7000; // Stagger after other tasks
    console.log(`⚙️ ${logPrefix} Scheduling first leaderboard update run in ${initialDelayMs / 1000}s...`);

    setTimeout(() => {
        updateLeaderboardsCycle().catch(err => console.error("❌ [Initial Leaderboard Update Run] Error:", err.message, err.stack));
        leaderboardManagerIntervalId = setInterval(updateLeaderboardsCycle, intervalMs);
        if (leaderboardManagerIntervalId.unref) leaderboardManagerIntervalId.unref(); // Allow exit
        console.log(`✅ ${logPrefix} Leaderboard updates scheduled every ${intervalMs / (1000 * 60)} minutes.`);
    }, initialDelayMs);
}

async function addPayoutJob(jobData) {
    const jobType = jobData?.type || 'unknown_job';
    const jobId = jobData?.withdrawalId || jobData?.payoutId || 'N/A_ID';
    const logPrefix = `[AddPayoutJob Type:${jobType} ID:${jobId}]`;
    console.log(`⚙️ ${logPrefix} Adding job to payout queue for user ${jobData.userId || 'N/A'}.`);
    // payoutProcessorQueue from Part 1
    return payoutProcessorQueue.add(async () => {
        let attempts = 0;
        const maxAttempts = (parseInt(process.env.PAYOUT_JOB_RETRIES, 10) || 3) + 1; // Env from Part 1
        const baseDelayMs = parseInt(process.env.PAYOUT_JOB_RETRY_DELAY_MS, 10) || 5000; // Env from Part 1

        while(attempts < maxAttempts) {
            attempts++;
            const attemptLogPrefix = `[PayoutJob Attempt:${attempts}/${maxAttempts} Type:${jobType} ID:${jobId}]`;
            try {
                console.log(`${attemptLogPrefix} Starting processing...`);
                if (jobData.type === 'payout_withdrawal') {
                    await handleWithdrawalPayoutJob(jobData.withdrawalId); // Defined below
                } else if (jobData.type === 'payout_referral') {
                    await handleReferralPayoutJob(jobData.payoutId); // Defined below
                } else {
                    throw new Error(`Unknown payout job type: ${jobData.type}`);
                }
                console.log(`✅ ${attemptLogPrefix} Job completed successfully.`);
                return; // Success, exit retry loop
            } catch(error) {
                console.warn(`⚠️ ${attemptLogPrefix} Attempt failed: ${error.message}`);
                const isRetryableFlag = error.isRetryable === true; // Check flag set by job handler

                if (!isRetryableFlag || attempts >= maxAttempts) {
                    console.error(`❌ ${attemptLogPrefix} Job failed permanently after ${attempts} attempts. Error: ${error.message}`);
                    // No re-throw needed, error logged. Might notify admin here if needed.
                    if (typeof notifyAdmin === "function") {
                         // MarkdownV2 Safety: Escape type, ID, error
                        await notifyAdmin(`🚨 PAYOUT JOB FAILED (Permanent): Type: ${escapeMarkdownV2(jobType)}, ID: ${escapeMarkdownV2(jobId)}, Attempts: ${attempts}. Error: ${escapeMarkdownV2(String(error.message || error))}`).catch(()=>{}); // Escaped .
                    }
                    return; // Exit loop
                }

                // Calculate exponential backoff with jitter
                const delayWithJitter = baseDelayMs * Math.pow(2, attempts - 1) * (0.8 + Math.random() * 0.4);
                console.log(`⏳ ${attemptLogPrefix} Retrying in ~${Math.round(delayWithJitter / 1000)}s...`);
                await sleep(delayWithJitter); // sleep from Part 1
            }
        }
    }).catch(queueError => {
        // This catches errors adding the job to the queue or unhandled rejections from the job itself
        console.error(`❌ ${logPrefix} CRITICAL Error processing job in Payout Queue: ${queueError.message}`);
         // MarkdownV2 Safety: Escape type, ID, error
        if (typeof notifyAdmin === "function") notifyAdmin(`🚨 CRITICAL Payout Queue Error. Type: ${escapeMarkdownV2(jobType)}, ID: ${escapeMarkdownV2(jobId)}. Error: ${escapeMarkdownV2(String(queueError.message || queueError))}`).catch(()=>{}); // Escaped .
    });
}

async function handleWithdrawalPayoutJob(withdrawalId) {
    const logPrefix = `[WithdrawJob ID:${withdrawalId}]`;
    console.log(`⚙️ ${logPrefix} Processing withdrawal job...`);
    let clientForRefund = null;
    let sendSolResult = { success: false, error: "Send SOL not initiated", isRetryable: false }; // Default result

    const details = await getWithdrawalDetails(withdrawalId); // from Part 2
    if (!details) {
        console.error(`❌ ${logPrefix} Withdrawal details not found for ID ${withdrawalId}. Cannot process.`);
        const error = new Error(`Withdrawal details not found for ID ${withdrawalId}. Job cannot proceed.`);
        error.isRetryable = false; throw error; // Non-retryable error
    }

    if (details.status === 'completed' || details.status === 'failed') {
        console.log(`ℹ️ ${logPrefix} Job skipped, withdrawal ID ${withdrawalId} already in terminal state '${details.status}'.`);
        return; // Job already done or failed permanently
    }

    const userId = details.user_id;
    const recipient = details.recipient_address;
    const amountToActuallySend = details.final_send_amount_lamports;
    const feeApplied = details.fee_lamports;
    const totalAmountDebitedFromBalance = amountToActuallySend + feeApplied;

    try {
        // updateWithdrawalStatus (from Part 2) handles idempotency
        const statusUpdatedToProcessing = await updateWithdrawalStatus(withdrawalId, 'processing');
        if (!statusUpdatedToProcessing) {
            // Check status again in case of race condition where it became completed/failed just before this update
            const currentDetailsAfterAttempt = await getWithdrawalDetails(withdrawalId);
            if (currentDetailsAfterAttempt && (currentDetailsAfterAttempt.status === 'completed' || currentDetailsAfterAttempt.status === 'failed')) {
                console.warn(`⚠️ ${logPrefix} Failed to update status to 'processing' for ID ${withdrawalId}, but it's already '${currentDetailsAfterAttempt.status}'. Exiting job as no-op.`);
                return; // Exit successfully, already handled
            }
            // If still pending or other state, throw retryable error
            const error = new Error(`Failed to update withdrawal ${withdrawalId} status to 'processing' and it's not yet terminal. Current status: ${currentDetailsAfterAttempt?.status}`);
            error.isRetryable = true; throw error;
        }
        console.log(`${logPrefix} Status updated to 'processing'. Attempting to send ${formatSol(amountToActuallySend)} SOL.`);

        sendSolResult = await sendSol(recipient, amountToActuallySend, 'withdrawal'); // sendSol from Part 3

        if (sendSolResult.success && sendSolResult.signature) {
            console.log(`✅ ${logPrefix} sendSol successful for withdrawal ID ${withdrawalId}. TX: ${sendSolResult.signature}. Marking 'completed'.`);
            await updateWithdrawalStatus(withdrawalId, 'completed', null, sendSolResult.signature); // Mark completed in DB
            // Notify user
             // MarkdownV2 Safety: Escape amount, recipient, signature
            await safeSendMessage(userId,
                `✅ *Withdrawal Completed\\!* ✅\n\n` +
                `Amount: *${escapeMarkdownV2(formatSol(amountToActuallySend))} SOL* sent to \`${escapeMarkdownV2(recipient)}\`\\.\n` + // Escaped .
                `TX: \`${escapeMarkdownV2(sendSolResult.signature)}\``,
                { parse_mode: 'MarkdownV2' }
            );
            return; // Success
        } else {
            // sendSol failed
            const sendErrorMsg = sendSolResult.error || 'Unknown sendSol failure.';
            console.error(`❌ ${logPrefix} sendSol FAILED for withdrawal ID ${withdrawalId}. Reason: ${sendErrorMsg}. Attempting to mark 'failed' and refund user.`);
            await updateWithdrawalStatus(withdrawalId, 'failed', null, null, sendErrorMsg.substring(0, 500)); // Mark failed in DB

            // --- Refund Logic ---
            clientForRefund = await pool.connect();
            await clientForRefund.query('BEGIN');
            // updateUserBalanceAndLedger from Part 2
            const refundUpdateResult = await updateUserBalanceAndLedger(
                clientForRefund, userId, totalAmountDebitedFromBalance, // Refund the full amount including fee
                'withdrawal_refund', { withdrawalId }, `Refund for failed withdrawal ID ${withdrawalId}`
            );
            if (refundUpdateResult.success) {
                await clientForRefund.query('COMMIT');
                console.log(`✅ ${logPrefix} Successfully refunded ${formatSol(totalAmountDebitedFromBalance)} SOL to user ${userId} for failed withdrawal ${withdrawalId}.`);
                 // MarkdownV2 Safety: Escape amounts, error reason
                await safeSendMessage(userId, `⚠️ Your withdrawal of ${escapeMarkdownV2(formatSol(amountToActuallySend))} SOL failed \\(Reason: ${escapeMarkdownV2(sendErrorMsg)}\\)\\. The amount ${escapeMarkdownV2(formatSol(totalAmountDebitedFromBalance))} SOL \\(including fee\\) has been refunded to your internal balance\\.`, {parse_mode: 'MarkdownV2'}); // Escaped . ()
            } else {
                await clientForRefund.query('ROLLBACK');
                console.error(`❌ CRITICAL ${logPrefix} FAILED TO REFUND USER ${userId} for withdrawal ${withdrawalId}. Amount: ${formatSol(totalAmountDebitedFromBalance)}. Refund DB Error: ${refundUpdateResult.error}`);
                 // MarkdownV2 Safety: Escape all details
                if (typeof notifyAdmin === "function") await notifyAdmin(`🚨🚨 CRITICAL: FAILED REFUND User ${escapeMarkdownV2(userId)}/WD ${withdrawalId}/Amt ${escapeMarkdownV2(formatSol(totalAmountDebitedFromBalance))}. SendErr: ${escapeMarkdownV2(sendErrorMsg)} RefundErr: ${escapeMarkdownV2(refundUpdateResult.error || 'Unknown DB error')}`);
                // Even if refund fails, throw the original send error for retry logic
            }
            clientForRefund.release(); clientForRefund = null;
            // --- End Refund Logic ---

            // Throw error to potentially trigger retry based on sendSolResult.isRetryable
            const errorToThrowForRetry = new Error(sendErrorMsg);
            errorToThrowForRetry.isRetryable = sendSolResult.isRetryable === true;
            throw errorToThrowForRetry;
        }
    } catch (jobError) {
        // Catch errors from DB updates or the thrown sendSol error
        console.error(`❌ ${logPrefix} Error during withdrawal job ID ${withdrawalId}: ${jobError.message}`, jobError.stack);
        // Ensure isRetryable flag is set for the queue handler
        if (jobError.isRetryable === undefined) {
            // If error didn't come from sendSol, check if it's a retryable Solana/DB error
             jobError.isRetryable = isRetryableSolanaError(jobError) || sendSolResult.isRetryable === true; // Use isRetryable from sendSol if it was set
        }
        // Ensure withdrawal status is marked 'failed' if not already completed
        const currentDetailsAfterJobError = await getWithdrawalDetails(withdrawalId);
        if (currentDetailsAfterJobError && currentDetailsAfterJobError.status !== 'completed' && currentDetailsAfterJobError.status !== 'failed') {
             // MarkdownV2 Safety: Escape error message
            await updateWithdrawalStatus(withdrawalId, 'failed', null, null, `Job error: ${escapeMarkdownV2(jobError.message)}`.substring(0,500));
        }
        throw jobError; // Re-throw for the queue retry logic
    } finally {
        if (clientForRefund) clientForRefund.release(); // Ensure release if refund attempt failed before release
    }
}

async function handleReferralPayoutJob(payoutId) {
    const logPrefix = `[ReferralJob ID:${payoutId}]`;
    console.log(`⚙️ ${logPrefix} Processing referral payout job...`);
    let sendSolResult = { success: false, error: "Send SOL not initiated for referral", isRetryable: false };

    const details = await getReferralPayoutDetails(payoutId); // from Part 2
    if (!details) {
        console.error(`❌ ${logPrefix} Referral payout details not found for ID ${payoutId}.`);
        const error = new Error(`Referral payout details not found for ID ${payoutId}. Cannot proceed.`);
        error.isRetryable = false; throw error;
    }
    if (details.status === 'paid' || details.status === 'failed') {
        console.log(`ℹ️ ${logPrefix} Job skipped, referral payout ID ${payoutId} already in terminal state '${details.status}'.`); return;
    }

    const referrerUserId = details.referrer_user_id;
    const amountToPay = details.payout_amount_lamports;
    const amountToPaySOL = formatSol(amountToPay);

    try {
        const referrerWalletDetails = await getUserWalletDetails(referrerUserId); // from Part 2
        if (!referrerWalletDetails?.external_withdrawal_address) {
            const noWalletMsg = `Referrer ${referrerUserId} has no linked external withdrawal address for referral payout ID ${payoutId}.`;
            console.error(`❌ ${logPrefix} ${noWalletMsg}`);
            await updateReferralPayoutStatus(payoutId, 'failed', null, null, noWalletMsg.substring(0,500)); // from Part 2
            const error = new Error(noWalletMsg); error.isRetryable = false; throw error;
        }
        const recipientAddress = referrerWalletDetails.external_withdrawal_address;

        await updateReferralPayoutStatus(payoutId, 'processing'); // from Part 2
        console.log(`${logPrefix} Status updated to 'processing'. Attempting to send ${amountToPaySOL} SOL to ${recipientAddress}.`);

        sendSolResult = await sendSol(recipientAddress, amountToPay, 'referral'); // sendSol from Part 3

        if (sendSolResult.success && sendSolResult.signature) {
            console.log(`✅ ${logPrefix} sendSol successful for referral payout ID ${payoutId}. TX: ${sendSolResult.signature}. Marking 'paid'.`);
            await updateReferralPayoutStatus(payoutId, 'paid', null, sendSolResult.signature); // from Part 2
            const rewardTypeMsg = details.payout_type === 'initial_bet'
                ? `Initial Referral Bonus (from User ${details.referee_user_id})` // getUserDisplayName from Part 3 could be used for referee_user_id
                : `Milestone Bonus (from User ${details.referee_user_id} reaching ${formatSol(details.milestone_reached_lamports || 0)} SOL wagered)`;
             // MarkdownV2 Safety: Escape type message, amount, address, signature
            await safeSendMessage(referrerUserId, // safeSendMessage from Part 3
                `💰 *${escapeMarkdownV2(rewardTypeMsg)} Paid\\!* 💰\n\n` + // Escaped !
                `Amount: *${escapeMarkdownV2(amountToPaySOL)} SOL* sent to your linked wallet \`${escapeMarkdownV2(recipientAddress)}\`\\.\n` + // Escaped .
                `TX: \`${escapeMarkdownV2(sendSolResult.signature)}\``,
                { parse_mode: 'MarkdownV2' }
            );
            return; // Success
        } else {
            const sendErrorMsg = sendSolResult.error || 'Unknown sendSol failure for referral payout.';
            console.error(`❌ ${logPrefix} sendSol FAILED for referral payout ID ${payoutId}. Reason: ${sendErrorMsg}.`);
            await updateReferralPayoutStatus(payoutId, 'failed', null, null, sendErrorMsg.substring(0, 500)); // Mark failed
             // MarkdownV2 Safety: Escape amount, error reason
            await safeSendMessage(referrerUserId, `❌ Your Referral Reward of ${escapeMarkdownV2(amountToPaySOL)} SOL failed to send \\(Reason: ${escapeMarkdownV2(sendErrorMsg)}\\)\\. Please contact support if this issue persists\\.`, {parse_mode: 'MarkdownV2'}); // Escaped . ()
             // MarkdownV2 Safety: Escape user ID, payout ID, amount, error
            if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 REFERRAL PAYOUT FAILED (Referrer ${escapeMarkdownV2(referrerUserId)}, Payout ID ${payoutId}, Amount ${escapeMarkdownV2(amountToPaySOL)} SOL): ${escapeMarkdownV2(sendErrorMsg)}`);

            // Throw error for retry logic
            const errorToThrowForRetry = new Error(sendErrorMsg);
            errorToThrowForRetry.isRetryable = sendSolResult.isRetryable === true;
            throw errorToThrowForRetry;
        }
    } catch (jobError) {
        console.error(`❌ ${logPrefix} Error during referral payout job ID ${payoutId}: ${jobError.message}`, jobError.stack);
        if (jobError.isRetryable === undefined) {
            jobError.isRetryable = isRetryableSolanaError(jobError) || sendSolResult.isRetryable === true;
        }
        // Ensure status is failed if not already terminal
        const currentDetailsAfterJobError = await getReferralPayoutDetails(payoutId);
        if (currentDetailsAfterJobError && currentDetailsAfterJobError.status !== 'paid' && currentDetailsAfterJobError.status !== 'failed') {
            await updateReferralPayoutStatus(payoutId, 'failed', null, null, `Job error: ${escapeMarkdownV2(jobError.message)}`.substring(0,500));
        }
        throw jobError; // Re-throw for queue retry logic
    }
}

async function setupTelegramConnection() {
    console.log('⚙️ [Startup] Configuring Telegram connection (Webhook/Polling)...');
    let webhookUrlBase = process.env.RAILWAY_PUBLIC_DOMAIN ? `https://${process.env.RAILWAY_PUBLIC_DOMAIN}` : process.env.WEBHOOK_URL; // Env from Part 1

    if (webhookUrlBase && process.env.BOT_TOKEN) {
        const webhookPath = `/telegram/${process.env.BOT_TOKEN}`;
        const fullWebhookUrl = `${webhookUrlBase.replace(/\/$/, '')}${webhookPath}`;
        console.log(`ℹ️ [Startup] Attempting to set webhook: ${fullWebhookUrl}`);

        try {
            if (bot.isPolling()) { // bot from Part 1
                console.log("ℹ️ [Startup] Stopping existing polling before setting webhook...");
                await bot.stopPolling({ cancel: true }).catch(e => console.warn("⚠️ [Startup] Error stopping polling:", e.message));
            }
            await bot.deleteWebHook({ drop_pending_updates: true }).catch((e) => console.warn(`⚠️ [Startup] Non-critical error deleting old webhook: ${e.message}`));

            const webhookMaxConn = parseInt(process.env.WEBHOOK_MAX_CONN, 10) || 40; // Env from Part 1
            const setResult = await bot.setWebHook(fullWebhookUrl, {
                max_connections: webhookMaxConn,
                allowed_updates: ['message', 'callback_query']
            });

            if (!setResult) {
                throw new Error("setWebHook API call returned false or timed out, indicating failure.");
            }

            console.log(`✅ [Startup] Telegram Webhook set successfully to ${fullWebhookUrl} (Max Connections: ${webhookMaxConn})`);
            bot.options.polling = false; // Ensure bot instance knows it's not polling
            return true; // Using webhook
        } catch (error) {
            console.error(`❌ [Startup] Failed to set Telegram Webhook: ${error.message}. Falling back to polling.`, error.response?.body ? JSON.stringify(error.response.body) : '');
             // MarkdownV2 Safety: Escape URL, error message
            if (typeof notifyAdmin === "function") await notifyAdmin(`⚠️ WARNING: Webhook setup failed for ${escapeMarkdownV2(fullWebhookUrl)}\nError: ${escapeMarkdownV2(String(error.message || error))}\nFalling back to polling.`).catch(()=>{}); // Escaped .
            bot.options.polling = true; // Fallback to polling
            return false; // Not using webhook
        }
    } else {
        console.log('ℹ️ [Startup] Webhook URL not configured (RAILWAY_PUBLIC_DOMAIN or WEBHOOK_URL) or BOT_TOKEN missing. Using Polling mode.');
        bot.options.polling = true;
        return false; // Using polling
    }
}

async function startPollingFallback() {
    if (!bot.options.polling) { // bot from Part 1
        console.log('ℹ️ [Startup] Polling not required (Webhook mode active or polling disabled).');
        return;
    }
    console.log('⚙️ [Startup] Starting Polling for Telegram updates...');
    try {
        // Ensure webhook is deleted before starting polling
        await bot.deleteWebHook({ drop_pending_updates: true }).catch((e) => console.warn(`⚠️ [Startup Polling] Non-critical error deleting webhook before polling: ${e.message}`));
        await bot.startPolling({
            // Polling options can be adjusted here if needed
            // interval: 300, // Example: Poll every 300ms (default is often fine)
        });
        console.log('✅ [Startup] Telegram Polling started successfully.');
    } catch (err) {
        console.error(`❌ CRITICAL: Telegram Polling failed to start: ${err.message}`, err.stack);
         // MarkdownV2 Safety: Escape error message
        if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 CRITICAL POLLING FAILED TO START: ${escapeMarkdownV2(String(err.message || err))}. Bot cannot receive updates. Exiting.`).catch(()=>{}); // Escaped .
        process.exit(3);
    }
}

function setupExpressServer() {
    console.log('⚙️ [Startup] Setting up Express server...');
    const port = process.env.PORT || 3000; // Env from Part 1
    // app from Part 1
    app.get('/', (req, res) => {
        // BOT_VERSION from Part 1
        res.status(200).send(`Solana Gambles Bot v${BOT_VERSION} is running.`);
    });

    app.get('/health', (req, res) => {
        // isFullyInitialized, BOT_VERSION from Part 1 state
        const status = isFullyInitialized ? 'OK' : 'INITIALIZING';
        const httpStatus = isFullyInitialized ? 200 : 503;
        res.status(httpStatus).json({ status: status, version: BOT_VERSION, timestamp: new Date().toISOString() });
    });

    // Only set up the webhook endpoint if polling is off and token exists
    if (!bot.options.polling && process.env.BOT_TOKEN) { // bot from Part 1
        const webhookPath = `/telegram/${process.env.BOT_TOKEN}`;
        console.log(`⚙️ [Startup] Configuring Express webhook endpoint at ${webhookPath}`);
        app.post(webhookPath, (req, res) => {
            try {
                if (req.body.message) {
                    // messageQueue from Part 1, handleMessage from Part 5a
                    messageQueue.add(() => handleMessage(req.body.message)).catch(e => console.error(`[MsgQueueErr Webhook]: ${e.message}`));
                } else if (req.body.callback_query) {
                     // callbackQueue from Part 1, handleCallbackQuery from Part 5a
                    callbackQueue.add(() => handleCallbackQuery(req.body.callback_query)).catch(e => {
                        console.error(`[CBQueueErr Webhook]: ${e.message}`);
                        bot.answerCallbackQuery(req.body.callback_query.id, {text: "⚠️ Error processing request."}).catch(()=>{});
                    });
                } else {
                    console.warn("[Webhook POST] Received unknown update type:", req.body);
                }
            } catch (processError) {
                console.error(`❌ [Webhook Handler] Error processing update: ${processError.message}`, processError.stack);
            }
            res.sendStatus(200); // Acknowledge receipt to Telegram immediately
        });
    } else {
         console.log("ℹ️ [Startup] Skipping Express webhook endpoint setup (Polling mode or no BOT_TOKEN).");
    }

    server = app.listen(port, '0.0.0.0', () => { // server global from Part 1
        console.log(`✅ [Startup] Express server listening on 0.0.0.0:${port}`);
    });

    server.on('error', async (error) => {
        console.error(`❌ Express Server Error: ${error.message}`, error.stack);
         // MarkdownV2 Safety: Escape error message
        if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 CRITICAL Express Server Error: ${escapeMarkdownV2(String(error.message || error))}. Bot may not function. Restart advised.`).catch(()=>{}); // Escaped .
        if (error.code === 'EADDRINUSE' && !isShuttingDown) { // isShuttingDown from Part 1 state
            console.error("Address in use, shutting down...");
            await shutdown('EADDRINUSE_SERVER_ERROR').catch(() => process.exit(4));
        }
    });
}

let isShuttingDown = false; // Defined in Part 1 state
async function shutdown(signal) {
    if (isShuttingDown) {
        console.warn("🚦 Shutdown already in progress, ignoring duplicate signal:", signal);
        return;
    }
    isShuttingDown = true;
    console.warn(`\n🚦 Received signal: ${signal}. Initiating graceful shutdown... (PID: ${process.pid})`);
    isFullyInitialized = false; // Mark as not ready during shutdown

    if (typeof notifyAdmin === "function") { // notifyAdmin from Part 3
         // MarkdownV2 Safety: Escape version, signal
        await notifyAdmin(`ℹ️ Bot instance v${escapeMarkdownV2(BOT_VERSION)} shutting down (Signal: ${escapeMarkdownV2(String(signal))})...`).catch(()=>{}); // Escaped () ...
    }

    console.log("🚦 [Shutdown] Stopping Telegram updates...");
    if (bot?.isPolling?.()) { // bot from Part 1
        await bot.stopPolling({ cancel: true }).then(() => console.log("✅ [Shutdown] Polling stopped.")).catch(e => console.error("❌ [Shutdown] Error stopping polling:", e.message));
    } else if (bot && !bot.options.polling) {
        console.log("ℹ️ [Shutdown] In webhook mode or polling was not active.");
        // Attempt to delete webhook on shutdown if configured
        await bot.deleteWebHook({ drop_pending_updates: false }) // Don't drop updates during shutdown, let queues process
            .then(() => console.log("✅ [Shutdown] Webhook deleted."))
            .catch(e => console.warn(`⚠️ [Shutdown] Non-critical error deleting webhook: ${e.message}`));
    } else {
        console.log("ℹ️ [Shutdown] Telegram bot instance not available or polling already off.");
    }

    console.log("🚦 [Shutdown] Closing HTTP server...");
    if (server) { // server from Part 1 state
        await new Promise(resolve => server.close(err => {
            if(err) console.error("❌ [Shutdown] Error closing HTTP server:", err);
            else console.log("✅ [Shutdown] HTTP server closed.");
            resolve();
        }));
    } else { console.log("ℹ️ [Shutdown] HTTP server was not running or already closed."); }

    console.log("🚦 [Shutdown] Stopping background intervals (Monitor, Sweeper, Leaderboards)...");
    // Interval IDs from Part 1 state
    if (depositMonitorIntervalId) clearInterval(depositMonitorIntervalId); depositMonitorIntervalId = null;
    if (sweepIntervalId) clearInterval(sweepIntervalId); sweepIntervalId = null;
    if (leaderboardManagerIntervalId) clearInterval(leaderboardManagerIntervalId); leaderboardManagerIntervalId = null;
    console.log("✅ [Shutdown] Background intervals cleared.");

    console.log("🚦 [Shutdown] Waiting for processing queues to idle...");
    const queueShutdownTimeout = parseInt(process.env.SHUTDOWN_QUEUE_TIMEOUT_MS, 10) || 20000; // Env from Part 1
     // Queues from Part 1
    const allQueues = [messageQueue, callbackQueue, payoutProcessorQueue, depositProcessorQueue, telegramSendQueue];
    try {
        await Promise.race([
            Promise.all(allQueues.map(q => q.onIdle())),
            sleep(queueShutdownTimeout).then(() => Promise.reject(new Error(`Queue idle timeout (${queueShutdownTimeout}ms) exceeded during shutdown.`))) // sleep from Part 1
        ]);
        console.log("✅ [Shutdown] All processing queues are idle.");
    } catch (queueError) {
        console.warn(`⚠️ [Shutdown] Queues did not idle within timeout or errored: ${queueError.message}`);
        allQueues.forEach(q => q.pause()); // Pause queues forcefully if timeout reached
    }

    console.log("🚦 [Shutdown] Closing Database pool...");
    if (pool) { // pool from Part 1
        await pool.end()
            .then(() => console.log("✅ [Shutdown] Database pool closed."))
            .catch(e => console.error("❌ [Shutdown] Error closing Database pool:", e.message));
    } else {
        console.log("ℹ️ [Shutdown] Database pool not available or already closed.");
    }

    console.log(`🏁 [Shutdown] Graceful shutdown complete (Signal: ${signal}). Exiting.`);
    const exitCode = (typeof signal === 'number' && signal !==0) ? signal : (signal === 'SIGINT' || signal === 'SIGTERM' ? 0 : 1);
    process.exit(exitCode);
}

function setupTelegramListeners() {
    console.log("⚙️ [Listeners] Setting up Telegram event listeners...");
    // bot from Part 1
    if (bot.listeners('message').length > 0 || bot.listeners('callback_query').length > 0) {
        console.warn("⚠️ [Listeners] Listeners already attached. Clearing them first.");
        bot.removeAllListeners();
    }

    bot.on('message', (msg) => {
        const adminUserIdsArray = (process.env.ADMIN_USER_IDS || '').split(',').map(id => id.trim()); // Env from Part 1
         // isFullyInitialized from Part 1 state
        // Allow admin commands even during initialization for diagnostics
        if (!isFullyInitialized && !(msg.from && adminUserIdsArray.includes(String(msg.from.id)) && msg.text?.startsWith('/admin'))) {
            // Optional: Send a "bot is starting" message if desired
            // safeSendMessage(msg.chat.id, "⏳ Bot is starting up, please wait a moment...").catch(()=>{});
            return;
        }
         // messageQueue from Part 1, handleMessage from Part 5a
        messageQueue.add(() => handleMessage(msg)).catch(e => console.error(`[MsgQueueErr Listener Processing]: ${e.message}`));
    });

    bot.on('callback_query', (cb) => {
        if (!isFullyInitialized) { // isFullyInitialized from Part 1 state
            bot.answerCallbackQuery(cb.id, {text: "🛠️ Bot is still starting up. Please wait a moment and try again.", show_alert: true}).catch(()=>{});
            return;
        }
         // callbackQueue from Part 1, handleCallbackQuery from Part 5a
        callbackQueue.add(() => handleCallbackQuery(cb)).catch(e => {
            console.error(`[CBQueueErr Listener Processing]: ${e.message}`);
            bot.answerCallbackQuery(cb.id, { text: "⚠️ Error processing your request." }).catch(()=>{});
        });
    });

    bot.on('polling_error', async (error) => { // Added async for potential notifyAdmin call
        console.error(`❌ TG Polling Error: Code ${error.code || 'N/A'} | ${error.message}`, error.stack);
        if (String(error.message).includes('409') || String(error.code).includes('EFATAL') || String(error.message).toLowerCase().includes('conflict')) {
            // MarkdownV2 Safety: Escape error message
            if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 POLLING CONFLICT (409) or EFATAL detected. Another instance running? Shutting down. Error: ${escapeMarkdownV2(String(error.message || error))}`).catch(()=>{}); // Escaped .
            if (!isShuttingDown) shutdown('POLLING_FATAL_ERROR').catch(() => process.exit(1)); // shutdown from this Part
        }
        // Handle other polling errors (e.g., network issues) - potentially add retry logic or notify admin
    });
    bot.on('webhook_error', async (error) => { // Added async for notifyAdmin
        console.error(`❌ TG Webhook Error: Code ${error.code || 'N/A'} | ${error.message}`, error.stack);
        // MarkdownV2 Safety: Escape error message
        if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 Telegram Webhook Error: ${error.code || 'N/A'} - ${escapeMarkdownV2(String(error.message || error))}`).catch(()=>{});
    });
    bot.on('error', async (error) => { // Added async for notifyAdmin
        console.error('❌ General node-telegram-bot-api Library Error:', error);
        // MarkdownV2 Safety: Escape error message
        if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 General TG Bot Library Error:\n${escapeMarkdownV2(String(error.message || error))}`).catch(()=>{});
    });
    console.log("✅ [Listeners] Telegram event listeners are ready.");
}

// --- Main Application Start ---
(async () => {
    const startTime = Date.now();
    // BOT_VERSION from Part 1
    console.log(`\n🚀 [Startup] Initializing Solana Gambles Bot v${BOT_VERSION}... (PID: ${process.pid})`);
    console.log(`Current Date/Time: ${new Date().toISOString()}`);

    console.log("⚙️ [Startup] Setting up process signal & error handlers...");
    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('uncaughtException', async (error, origin) => {
        console.error(`\n🚨🚨🚨 UNCAUGHT EXCEPTION [Origin: ${origin}] 🚨🚨🚨\n`, error);
        isFullyInitialized = false; // Mark as not ready
        if (!isShuttingDown) {
            console.error("Initiating emergency shutdown due to uncaught exception...");
            try {
                 // MarkdownV2 Safety: Escape origin, error message
                if (typeof notifyAdmin === "function") await notifyAdmin(`🚨🚨🚨 UNCAUGHT EXCEPTION (${escapeMarkdownV2(String(origin))})\n${escapeMarkdownV2(String(error.message || error))}\nAttempting emergency shutdown...`).catch(e => console.error("Admin notify fail (uncaught):", e)); // Escaped () ...
                await shutdown('uncaughtException');
            } catch (shutdownErr) {
                console.error("❌ Emergency shutdown attempt itself FAILED:", shutdownErr);
                if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 Emergency shutdown itself FAILED after uncaught exception. Forcing exit.`).catch(e => console.error("Admin notify fail (uncaught shutdown fail):", e)); // Escaped .
                setTimeout(() => process.exit(1), 1000).unref(); // Force exit after delay
            }
        } else {
            console.warn("Uncaught exception occurred during an ongoing shutdown sequence. Forcing exit.");
            setTimeout(() => process.exit(1), 1000).unref();
        }
        // Watchdog timer to ensure exit even if shutdown hangs
        const failTimeout = parseInt(process.env.SHUTDOWN_FAIL_TIMEOUT_MS, 10) || 8000; // Env from Part 1
        setTimeout(() => { console.error(`🚨 Forcing exit after ${failTimeout}ms due to uncaught exception (watchdog).`); process.exit(1); }, failTimeout).unref();
    });
    process.on('unhandledRejection', async (reason, promise) => {
        console.error('\n🔥🔥🔥 UNHANDLED PROMISE REJECTION 🔥🔥🔥');
        console.error('Promise:', promise); // Log the promise details if possible
        console.error('Reason:', reason); // Log the reason (could be Error object or other value)
        if (typeof notifyAdmin === "function") { // notifyAdmin from Part 3
             const reasonMsg = reason instanceof Error ? reason.message : String(reason);
              // MarkdownV2 Safety: Escape reason
             await notifyAdmin(`🔥🔥🔥 UNHANDLED REJECTION\nReason: ${escapeMarkdownV2(reasonMsg)}`).catch(()=>{});
        }
        // Depending on the severity/frequency, might consider triggering shutdown here too.
    });
    console.log("✅ [Startup] Process handlers set up.");

    try {
        console.log("⚙️ [Startup Step 1/8] Initializing Database...");
        await initializeDatabase(); // Uses simplified approach now

        console.log("⚙️ [Startup Step 2/8] Loading Active Deposits Cache...");
        await loadActiveDepositsCache();

        console.log("⚙️ [Startup Step 3/8] Loading Slots Jackpot...");
        const loadedJackpot = await loadSlotsJackpot();
        currentSlotsJackpotLamports = loadedJackpot; // global from Part 1
        console.log(`⚙️ [Startup] Global slots jackpot variable set to: ${formatSol(currentSlotsJackpotLamports)} SOL`); // formatSol from Part 3

        console.log("⚙️ [Startup Step 4/8] Setting up Telegram Connection (Webhook/Polling)...");
        const useWebhook = await setupTelegramConnection();

        console.log("⚙️ [Startup Step 5/8] Setting up Express Server...");
        setupExpressServer();

        console.log("⚙️ [Startup Step 6/8] Attaching Telegram Listeners...");
        setupTelegramListeners(); // Attaches listeners, doesn't start polling

        console.log("⚙️ [Startup Step 7/8] Starting Polling (if applicable)...");
        if (!useWebhook && bot.options.polling) { // Check if polling is still the desired mode
            await startPollingFallback();
        } else if (!useWebhook && !bot.options.polling && (process.env.WEBHOOK_URL || process.env.RAILWAY_PUBLIC_DOMAIN)) {
            console.warn("⚠️ [Startup] Webhook setup failed, and polling was also not enabled. Bot might not receive updates.");
        } else if (useWebhook) {
            console.log("ℹ️ [Startup] Webhook mode active. Polling not started.");
        } else {
             console.log("ℹ️ [Startup] Polling mode is primary or webhook setup failed. Polling should be active if configured.")
        }

        const initDelay = parseInt(process.env.INIT_DELAY_MS, 10) || 3000; // Env from Part 1
        console.log(`⚙️ [Startup Step 8/8] Finalizing startup & scheduling Background Tasks in ${initDelay / 1000}s...`);

        setTimeout(async () => {
            try {
                console.log("⚙️ [Startup Final Phase] Starting Background Tasks now...");
                startDepositMonitor();
                startDepositSweeper();
                startLeaderboardManager();
                console.log("✅ [Startup Final Phase] Background Tasks scheduled/started.");

                // Final check on DB pool
                await pool.query('SELECT NOW()'); // pool from Part 1
                console.log("✅ [Startup Final Phase] Database pool confirmed operational post-init.");

                console.log("⚙️ [Startup Final Phase] Setting isFullyInitialized to true...");
                isFullyInitialized = true; // global from Part 1
                console.log("✅ [Startup Final Phase] isFullyInitialized flag is now true. Bot is ready for full operation.");

                const me = await bot.getMe(); // bot from Part 1
                const startupDuration = (Date.now() - startTime) / 1000;
                // MarkdownV2 Safety: Escape version, jackpot amount, time
                const startupMessage = `✅ Bot v${escapeMarkdownV2(BOT_VERSION)} Started Successfully\\!\nMode: ${useWebhook ? 'Webhook' : 'Polling'}\nJackpot: ${escapeMarkdownV2(formatSol(currentSlotsJackpotLamports))} SOL\nStartup Time: ${escapeMarkdownV2(startupDuration.toFixed(2))}s\\.`; // Escaped ! .
                console.log(`✅ [Startup Final Phase] Token validated. Bot Username: @${me.username}. Startup took ${startupDuration.toFixed(2)}s.`);
                if (typeof notifyAdmin === "function") await notifyAdmin(startupMessage).catch(()=>{});
                console.log(`\n🎉🎉🎉 Solana Gambles Bot is fully operational! (${new Date().toISOString()}) 🎉🎉🎉`);
            } catch (finalPhaseError) {
                console.error("❌ FATAL ERROR DURING FINAL STARTUP PHASE (after initial delay):", finalPhaseError);
                isFullyInitialized = false;
                 // MarkdownV2 Safety: Escape error message
                if (typeof notifyAdmin === "function") await notifyAdmin(`🚨 BOT STARTUP FAILED (Final Phase): ${escapeMarkdownV2(String(finalPhaseError.message || finalPhaseError))}. Bot may not be operational.`).catch(()=>{}); // Escaped .
                if (!isShuttingDown) await shutdown('STARTUP_FINAL_PHASE_ERROR').catch(() => process.exit(5));
            }
        }, initDelay);

    } catch (error) {
        console.error("❌❌❌ FATAL ERROR DURING MAIN STARTUP SEQUENCE:", error);
        isFullyInitialized = false;
        if (pool) { pool.end().catch(() => {});} // Attempt to close pool if init failed
        try {
            // Fallback notification if notifyAdmin might not be available/working
            if (process.env.BOT_TOKEN && process.env.ADMIN_USER_IDS) {
                 const tempBotForError = new TelegramBot(process.env.BOT_TOKEN, {polling: false});
                 const adminIdsForError = process.env.ADMIN_USER_IDS.split(',');
                 for (const adminId of adminIdsForError) {
                     if(adminId.trim()) {
                         // Use basic text, avoid MarkdownV2 complexity here
                         tempBotForError.sendMessage(adminId.trim(), `🚨 BOT STARTUP FAILED (Main Sequence): ${String(error.message || error)}. Check logs & DB. Exiting.`).catch(e => console.error("Fallback admin notify failed:", e));
                     }
                 }
            }
        } catch (finalNotifyError) {console.error("Could not send final admin error notification:", finalNotifyError);}
        process.exit(1); // Exit on major startup failure
    }
})(); // End of main async IIFE

// --- End of Part 6 / End of File index.js ---
