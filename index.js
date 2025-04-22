require('dotenv').config();
const TelegramBot = require('node-telegram-bot-api');
const { Connection, clusterApiUrl, PublicKey, LAMPORTS_PER_SOL, Keypair, Transaction, SystemProgram, sendAndConfirmTransaction } = require('@solana/web3.js');
const bs58 = require('bs58');
const fs = require('fs');
const express = require('express');
const app = express();
app.get('/', (req, res) => res.send('OK'));
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Healthcheck listening on ${PORT}`));
const bot = new TelegramBot(process.env.BOT_TOKEN, { polling: true });
const connection = new Connection('https://solana-mainnet.g.alchemy.com/v2/RQg--XCO8P6g4VdM845rlCUs5r3CSEbE', 'confirmed');
const WALLET_ADDRESS = '9HL7W4XZJDX6br3ojjU6BLHp7oZVP3nCDKxQ21TNanQf';
const MIN_BET = 0.01;
const MAX_BET = 1.0;
const LOG_DIR = './data';
const LOG_PATH = './data/bets.json';

// Store user bet information (chatId: { amount, choice })
const userBets = {};

async function checkPayment(expectedSol) {
    const pubKey = new PublicKey(WALLET_ADDRESS);
    const signatures = await connection.getSignaturesForAddress(pubKey, { limit: 5 }); // Reduced limit

    console.log(`Checking for payment of ${expectedSol} SOL. Found ${signatures.length} recent signatures.`);

    for (let sig of signatures) {
        const tx = await connection.getParsedTransaction(sig.signature);
        if (!tx || !tx.meta) {
            console.log(`Skipping signature ${sig.signature} due to missing transaction or meta.`);
            continue;
        }

        const txTime = new Date(sig.blockTime * 1000);
        const timeDiff = (new Date() - txTime) / 1000 / 60;

        console.log(`Signature: ${sig.signature}, Time Diff: ${timeDiff.toFixed(2)} mins`);

        if (timeDiff > 15) {
            console.log(`Skipping ${sig.signature} due to time difference.`);
            continue;
        }

        const amount = (tx.meta.postBalances[0] - tx.meta.preBalances[0]) / LAMPORTS_PER_SOL;

        console.log(`Signature: ${sig.signature}, Amount Change: ${amount.toFixed(6)} SOL`);

        if (Math.abs(Math.abs(amount) - expectedSol) < 0.0015) { // Revised tolerance check
            console.log(`Payment found! Signature: ${sig.signature}`);
            return { success: true, tx: sig.signature };
        }
    }
    console.log(`Payment not found for ${expectedSol} SOL within recent transactions.`);
    return { success: false };
}
const getHouseEdge = (amount) => {
  if (amount <= 0.01) return 0.70;
  if (amount <= 0.049) return 0.75;
  if (amount <= 0.0999999) return 0.80;
  return 0.99;
};

if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR);
if (!fs.existsSync(LOG_PATH)) fs.writeFileSync(LOG_PATH, '[]');

bot.onText(/\/start/, (msg) => {
  bot.sendMessage(msg.chat.id, `Welcome to Solana Gambles!
You can place bets by typing:

/bet 0.01 heads

Min bet: 0.01 SOL
Max bet: 1.0 SOL

Send your SOL to:
9HL7W4XZJDX6br3ojjU6BLHp7oZVP3nCDKxQ21TNanQf`);
});

bot.onText(/\/test/, (msg) => {
  bot.sendMessage(msg.chat.id, 'Test successful!');
});

bot.onText(/^\/bet$/i, (msg) => {
  bot.sendMessage(
    msg.chat.id,
    `🎰 Please choose an amount:\n\n` +
    `/bet 0.01 heads\n` +
    `/bet 0.05 tails\n\n` +
    `Min: ${MIN_BET} SOL | Max: ${MAX_BET} SOL`,
    { parse_mode: 'Markdown' }
  );
});

bot.onText(/\/bet (\d+\.\d+) (heads|tails)/i, async (msg, match) => {
    const chatId = msg.chat.id;
    const betAmount = parseFloat(match[1]);
    const userChoice = match[2].toLowerCase();

    if (betAmount < MIN_BET || betAmount > MAX_BET) {
        return bot.sendMessage(chatId, `❌ Bet must be between ${MIN_BET}-${MAX_BET} SOL`);
    }

    // Store the bet information
    userBets[chatId] = { amount: betAmount, choice: userChoice };

    await bot.sendMessage(chatId,
        `💸 *To place your bet:*\n\n` +
        `1. Send *exactly ${betAmount} SOL* to:\n` +
        `\`${WALLET_ADDRESS}\`\n\n` +
        `2. Once sent, type: /confirm\n\n` +
        `⚠️ You have 15 minutes to complete payment`,
        { parse_mode: 'Markdown' }
    );
});

bot.onText(/^\/confirm$/, async (msg) => {
    const chatId = msg.chat.id;
    const betInfo = userBets[chatId];

    if (!betInfo) {
        return bot.sendMessage(chatId, `⚠️ No active bet found. Please use the /bet command first.`);
    }

    const { amount, choice } = betInfo;

    try {
        await bot.sendMessage(chatId, `🔍 Verifying your payment of ${amount} SOL...`);
        await new Promise(resolve => setTimeout(resolve, 5000));
        const paymentCheck = await checkPayment(amount);
        console.log('Payment check result:', paymentCheck);

        if (!paymentCheck.success) {
            return await bot.sendMessage(chatId, `❌ Payment not verified!`);
        }

        await bot.sendMessage(chatId, `✅ Payment verified!`);

        const houseEdge = getHouseEdge(amount);
        const result = Math.random() > houseEdge ? choice : (choice === 'heads' ? 'tails' : 'heads');
        const win = result === choice;
        const payout = win ? amount * (1/houseEdge - 1) : 0;

        // Log the bet
        const log = JSON.parse(fs.readFileSync(LOG_PATH));
        log.push({
            ts: new Date().toISOString(),
            user: msg.from.username || msg.from.id,
            amount,
            choice,
            result,
            payout,
            tx: paymentCheck.tx
        });
        fs.writeFileSync(LOG_PATH, JSON.stringify(log, null, 2));

        await bot.sendMessage(chatId,
            win ? `🎉 Congratulations! You won ${payout.toFixed(4)} SOL!` +
                  `\n\nYour choice: ${choice}\nResult: ${result}\n\n` +
                  `Incoming TX: ${paymentCheck.tx}`
                : `❌ Sorry! You lost.\n\nYour choice: ${choice}\nResult: ${result}`,
            { parse_mode: 'Markdown' }
        );

        if (win && payout > 0) {
            try {
                console.log('Inside payout try block');
                const payerPrivateKey = process.env.BOT_PRIVATE_KEY;
                if (!payerPrivateKey) {
                    console.error('BOT_PRIVATE_KEY environment variable not set!');
                    return await bot.sendMessage(chatId, `⚠️ Payout failed: Bot's private key not configured.`);
                }
                const payerWallet = Keypair.fromSecretKey(bs58.decode(payerPrivateKey));
                const payerPublicKey = payerWallet.publicKey;

                // Simplified winner key retrieval (for testing)
                const winnerPublicKey = new PublicKey(WALLET_ADDRESS); // Sending to the bot's own address for now

                const payoutAmountLamports = Math.round(payout * LAMPORTS_PER_SOL);

                const transaction = new Transaction().add(
                    SystemProgram.transfer({
                        fromPubkey: payerPublicKey,
                        toPubkey: winnerPublicKey,
                        lamports: payoutAmountLamports,
                    })
                );

                const signature = await sendAndConfirmTransaction(
                    connection,
                    transaction,
                    [payerWallet]
                );

                await bot.sendMessage(chatId, `✅ Winnings of ${payout.toFixed(4)} SOL sent! TX: ${signature}`);

            } catch (error) {
                console.error('Payout error:', error);
                await bot.sendMessage(chatId, `⚠️ Payout failed due to an error.`);
            } finally {
                console.log('Payout attempt finished.');
            }
        }

        delete userBets[chatId];
    }
});
