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

// Store user bet information (userId: { amount, choice })
const userBets = {};
const coinFlipSessions = {}; // Track if a user has initiated coin flip

async function checkPayment(expectedSol) {
    const pubKey = new PublicKey(WALLET_ADDRESS);
    const signatures = await connection.getSignaturesForAddress(pubKey, { limit: 5 }); // Reduced limit

    console.log(`Checking for payment of ${expectedSol} SOL. Found ${signatures.length} recent signatures.`);

    for (let sig of signatures) {
        const tx = await connection.getParsedTransaction(sig.signature);
        if (!tx || !tx.meta) {
            console.log(`Skipping signature ${sig.signature} for missing data.`);
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

bot.onText(/\/start$/, async (msg) => {
    const chatId = msg.chat.id;
    const gifUrl = 'https://media4.giphy.com/media/mrJg7yrURBntrDL804/giphy.gif?cid=6c09b952c8nzwcr45gvyqv7bfp80blroxd4wt1bdtrsixwok&ep=v1_internal_gif_by_id&rid=giphy.gif&ct=g'; // Your GIF URL

    await bot.sendAnimation(chatId, gifUrl, {
        caption: `Welcome to Solana Gambles!\n\nAvailable games:\n- Click to start: */start coinflip*\n- /start race (coming soon!)\n\nType /refresh to see this menu again.`,
        parse_mode: 'Markdown', // Enable Markdown parsing
        reply_markup: {
            inline_keyboard: [
                [{ text: '🪙 Start Coin Flip (Button)', callback_data: 'start_coinflip' }],
                [{ text: '🏁 Start Race (Button - Coming Soon!)', callback_data: 'start_race' }]
            ]
        }
    });
});

bot.onText(/\/start coinflip/, (msg) => {
  const userId = msg.from.id;
  coinFlipSessions[userId] = true; // Mark the user as in a coin flip session
  bot.sendMessage(
    msg.chat.id,
    `🪙 You've started a coin flip game! Please choose an amount and heads/tails:\n\n` +
    `/bet 0.01 heads\n` +
    `/bet 0.05 tails\n\n` +
    `Min: ${MIN_BET} SOL | Max: ${MAX_BET} SOL`
    ,
    { parse_mode: 'Markdown' }
  );
});

bot.onText(/\/refresh$/, (msg) => {
  bot.sendMessage(msg.chat.id, `Welcome to Solana Gambles!

        Available games:
        - Click to start: */start coinflip*
        - /start race (coming soon!)

        Type /refresh to see this menu again.`, {
            parse_mode: 'Markdown',
            reply_markup: {
                inline_keyboard: [
                    [{ text: '🪙 Start Coin Flip (Button)', callback_data: 'start_coinflip' }],
                    [{ text: '🏁 Start Race (Button - Coming Soon!)', callback_data: 'start_race' }]
                ]
            }
        });
});

// Modify the /bet handler to check if a coin flip session is active for the user
bot.onText(/\/bet (\d+\.\d+) (heads|tails)/i, async (msg, match) => {
  const userId = msg.from.id;
  const chatId = msg.chat.id;

  if (!coinFlipSessions[userId]) {
    return bot.sendMessage(chatId, `⚠️ Please start a coin flip game first using /start coinflip`);
  }

  const betAmount = parseFloat(match[1]);
  const userChoice = match[2].toLowerCase();

  if (betAmount < MIN_BET || betAmount > MAX_BET) {
    return bot.sendMessage(chatId, `❌ Bet must be between ${MIN_BET}-${MAX_BET} SOL`);
  }

  // Store the bet information using the user's ID
  userBets[userId] = { amount: betAmount, choice: userChoice };

  await bot.sendMessage(chatId,
    `💸 *To place your bet:*\n\n` +
    `Send *exactly ${betAmount} SOL* to:\n` +
    `\`${WALLET_ADDRESS}\`\n\n` +
    `Once sent, type /confirm to finalize your bet.\n` +
    `⚠️ You have 15 minutes to complete payment.`,
    { parse_mode: 'Markdown' }
  );
});

bot.onText(/^\/confirm$/, async (msg) => {
  const userId = msg.from.id;
  const chatId = msg.chat.id;
  const betInfo = userBets[userId];

  if (!betInfo) {
    return await bot.sendMessage(chatId, `⚠️ No active bet found. Please use the /bet command first.`);
  }

  const { amount, choice } = betInfo;

  let paymentCheckResult;
  try {
    await bot.sendMessage(chatId, `🔍 Verifying your payment of ${amount} SOL...`);
    await new Promise(resolve => setTimeout(resolve, 5000));
    paymentCheckResult = await checkPayment(amount);
    console.log('Payment check result:', paymentCheckResult);

    if (!paymentCheckResult.success) {
      return await bot.sendMessage(chatId, `❌ Payment not verified!`);
    }

    await bot.sendMessage(chatId, `✅ Payment verified!`);

    const houseEdge = getHouseEdge(amount);
    const result = Math.random() > houseEdge ? choice : (choice === 'heads' ? 'tails' : 'heads');
    const win = result === choice;
    const payout = win ? amount : 0; // 1:1 payout on win

    console.log(`DEBUG: Before winning message - win=${win}, amount=${amount}, payout=${payout}`); // Added debug log

    // Re-enable logging
    const log = JSON.parse(fs.readFileSync(LOG_PATH));
    log.push({
      ts: new Date().toISOString(),
      user: msg.from.username || msg.from.id, // Keep both for logging
      amount,
      choice,
      result,
      payout,
      tx: paymentCheckResult.tx
    });
    fs.writeFileSync(LOG_PATH, JSON.stringify(log, null, 2));

    const displayName = msg.from.username ? `@${msg.from.username}` : `<@${userId}>`;

    if (win) {
      const winGifUrl = 'https://media.tenor.com/vbsbOyrKFnAAAAAC/confetti-pop.gif'; // Example win GIF URL
      await bot.sendAnimation(chatId, winGifUrl);
      await bot.sendMessage(chatId, `🎉 Congratulations, ${displayName}! You won ${payout.toFixed(4)} SOL!\n\nYour choice: ${choice}\nResult: ${result}`,
        { parse_mode: 'Markdown' }
      );
    } else {
      const loseGifUrl = 'https://media.tenor.com/8F5-gn46H0gAAAAC/sad-face.gif'; // Example lose GIF URL
      await bot.sendAnimation(chatId, loseGifUrl);
      await bot.sendMessage(chatId, `❌ Sorry, ${displayName}! You lost.\n\nYour choice: ${choice}\nResult: ${result}`,
        { parse_mode: 'Markdown' }
      );
    }

    if (win) {
      try {
        console.log('Inside payout try block');
        console.log(`DEBUG Payout: win=${win}, amount=${amount}`); // Added debug

        const payerPrivateKey = process.env.BOT_PRIVATE_KEY;
        if (!payerPrivateKey) {
          console.error('BOT_PRIVATE_KEY environment variable not set!');
          return await bot.sendMessage(chatId, `⚠️ Payout failed: Bot's private key not configured.`);
        }
        const payerWallet = Keypair.fromSecretKey(bs58.decode(payerPrivateKey));
        const payerPublicKey = payerWallet.publicKey;

        // Attempt to get the sender's public key from the payment transaction
        let winnerPublicKey;
        if (paymentCheckResult && paymentCheckResult.tx) {
          try {
            const parsedTransaction = await connection.getParsedTransaction(paymentCheckResult.tx);
            if (parsedTransaction && parsedTransaction.transaction && parsedTransaction.transaction.message && parsedTransaction.transaction.message.accountKeys && parsedTransaction.transaction.message.accountKeys.length > 0) {
              // Assuming the first non-fee payer account is the sender
              winnerPublicKey = parsedTransaction.transaction.message.accountKeys[0].pubkey;
              console.log('Extracted winner public key:', winnerPublicKey.toBase58());
            } else {
              console.warn('Could not parse transaction to determine sender.');
              return await bot.sendMessage(chatId, `⚠️ Payout failed: Could not analyze your payment transaction.`);
            }
          } catch (error) {
            console.error('Error parsing transaction for sender:', error);
            return await bot.sendMessage(chatId, `⚠️ Payout failed: Error analyzing your payment transaction.`);
          }
        } else {
          console.warn('No transaction signature available to determine sender.');
          return await bot.sendMessage(chatId, `⚠️ Payout failed: No payment transaction found.`);
        }

        if (!winnerPublicKey) {
          console.warn('Winner public key is undefined.');
          return await bot.sendMessage(chatId, `⚠️ Payout failed: Could not determine recipient.`);
        }

        const payoutAmountLamports = Math.round(amount * LAMPORTS_PER_SOL); // ENSURE using 'amount'

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

        await bot.sendMessage(chatId, `✅ Winnings of ${amount.toFixed(4)} SOL sent to ${displayName}! TX: ${signature}`); // Use displayName in payout message

      } catch (error) {
        console.error('Payout error:', error);
        await bot.sendMessage(chatId, `⚠️ Payout failed due to an error.`);
      } finally {
        console.log('Payout attempt finished.');
      }
    }

    delete userBets[userId];
    delete coinFlipSessions[userId]; // End the coin flip session after confirmation
  } catch (topLevelError) {
    console.error('Top-level error in /confirm:', topLevelError);
    await bot.sendMessage(chatId, `⚠️ An unexpected error occurred while processing your confirmation.`);
  }
});

// Handle the button clicks
bot.on('callback_query', async (query) => {
    const chatId = query.message.chat.id;
    const userId = query.from.id;

    if (query.data === 'start_coinflip') {
        coinFlipSessions[userId] = true; // Mark the user as in a coin flip session
        await bot.sendMessage(
            chatId,
            `🪙 You've started a coin flip game! Please choose an amount and heads/tails:\n\n` +
            `/bet 0.01 heads\n` +
            `/bet 0.05 tails\n\n` +
            `Min: ${MIN_BET} SOL | Max: ${MAX_BET} SOL`,
            { parse_mode: 'Markdown' }
        );

        // Acknowledge the callback
        bot.answerCallbackQuery(query.id);
    } else if (query.data === 'start_race') {
        await bot.sendMessage(chatId, `🏁 The race game is coming soon! Stay tuned for updates!`);
        // You would add logic here to initiate the race game when it's implemented
        bot.answerCallbackQuery(query.id);
    }
});

// We will add /start race and /betrace logic here later

bot.onText(/\/test/, (msg) => {
  bot.sendMessage(msg.chat.id, 'Test successful!');
});
