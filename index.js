require('dotenv').config();
const TelegramBot = require('node-telegram-bot-api');
const {
    Connection,
    clusterApiUrl,
    PublicKey,
    LAMPORTS_PER_SOL,
    Keypair,
    Transaction,
    SystemProgram,
    sendAndConfirmTransaction
} = require('@solana/web3.js');
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

const raceSessions = {};
const userRaceBets = {};
let nextRaceId = 1;
const RACE_MIN_BET = 0.01;
const RACE_MAX_BET = 1.0;
const availableHorses = [
    { name: 'Yellow', emoji: '💛', odds: 1.1, winProbability: 0.25 },
    { name: 'Orange', emoji: '🧡', odds: 2.0, winProbability: 0.20 },
    { name: 'Blue', emoji: '💙', odds: 3.0, winProbability: 0.15 },
    { name: 'Cyan', emoji: '🇨🇾', odds: 4.0, winProbability: 0.12 },
    { name: 'White', emoji: '🤍', odds: 5.0, winProbability: 0.09 },
    { name: 'Red', emoji: '❤️', odds: 6.0, winProbability: 0.07 },
    { name: 'Black', emoji: '🖤', odds: 7.0, winProbability: 0.05 },
    { name: 'Pink', emoji: '🩷', odds: 8.0, winProbability: 0.03 },
    { name: 'Purple', emoji: '💜', odds: 9.0, winProbability: 0.02 },
    { name: 'Green', emoji: '💚', odds: 10.0, winProbability: 0.01 },
    { name: 'Silver', emoji: '🩶', odds: 15.0, winProbability: 0.01 },
];

const userBets = {};
const coinFlipSessions = {};
const usedTransactions = new Set(); // To store used transaction signatures

async function checkPayment(expectedSol) {
    const pubKey = new PublicKey(WALLET_ADDRESS);
    const signatures = await connection.getSignaturesForAddress(pubKey, { limit: 5 });

    for (let sig of signatures) {
        const tx = await connection.getParsedTransaction(sig.signature);
        if (!tx || !tx.meta) continue;

        const txTime = new Date(sig.blockTime * 1000);
        const timeDiff = (new Date() - txTime) / 1000 / 60;
        if (timeDiff > 15) continue;

        const amount = (tx.meta.postBalances[0] - tx.meta.preBalances[0]) / LAMPORTS_PER_SOL;
        if (Math.abs(Math.abs(amount) - expectedSol) < 0.0015) {
            if (usedTransactions.has(sig.signature)) {
                return { success: false, message: 'Transaction already used' }; // Add message
            }
            return { success: true, tx: sig.signature };
        }
    }
    return { success: false, message: 'Payment not found' }; //Add message
}

async function sendSol(connection, payerPrivateKey, recipientPublicKey, amount) {
    try {
        const payerWallet = Keypair.fromSecretKey(bs58.decode(payerPrivateKey));
        const payerPublicKey = payerWallet.publicKey;
        const payoutAmountLamports = Math.round(amount * LAMPORTS_PER_SOL);

        const transaction = new Transaction().add(
            SystemProgram.transfer({
                fromPubkey: payerPublicKey,
                toPubkey: recipientPublicKey,
                lamports: payoutAmountLamports,
            })
        );

        const signature = await sendAndConfirmTransaction(
            connection,
            transaction,
            [payerWallet]
        );

        return { success: true, signature };
    } catch (error) {
        console.error('Error sending SOL:', error);
        return { success: false, error: error.message };
    }
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
    const gifUrl = 'https://media4.giphy.com/media/mrJg7yrURBntrDL804/giphy.gif';

    await bot.sendAnimation(chatId, gifUrl, {
        caption: `Welcome to Solana Gambles!\n\nAvailable games:\n- Click to start: */coinflip*\n- /race\n\nType /refresh to see this menu again.`,
        parse_mode: 'Markdown',
        reply_markup: {
            inline_keyboard: [
                [{ text: '🪙 Start Coin Flip (Button)', callback_data: 'start_coinflip' }],
                [{ text: '🏁 Start Race (Button)', callback_data: 'start_race' }]
            ]
        }
    });
});

bot.onText(/\/coinflip$/, (msg) => {
    const userId = msg.from.id;
    coinFlipSessions[userId] = true;
    bot.sendMessage(
        msg.chat.id,
        `🪙 You've started a coin flip game! Please choose an amount and heads/tails:\n\n` +
        `/bet 0.01 heads\n/bet 0.05 tails\n\nMin: ${MIN_BET} SOL | Max: ${MAX_BET} SOL`,
        { parse_mode: 'Markdown' }
    );
});

bot.onText(/\/refresh$/, async (msg) => {
    const chatId = msg.chat.id;
    const gifUrl = 'https://media4.giphy.com/media/mrJg7yrURBntrDL804/giphy.gif';
    await bot.sendAnimation(chatId, gifUrl, {
        caption: `Welcome to Solana Gambles!\n\nAvailable games:\n- /coinflip\n- /race`,
        parse_mode: 'Markdown'
    });
});

bot.onText(/\/bet (\d+\.\d+) (heads|tails)/i, async (msg, match) => {
    const userId = msg.from.id;
    const chatId = msg.chat.id;

    if (!coinFlipSessions[userId]) {
        return bot.sendMessage(chatId, `⚠️ Please start a coin flip game first using /coinflip`);
    }

    const betAmount = parseFloat(match[1]);
    const userChoice = match[2].toLowerCase();

    if (betAmount < MIN_BET || betAmount > MAX_BET) {
        return bot.sendMessage(chatId, `❌ Bet must be between ${MIN_BET} - ${MAX_BET} SOL`);
    }

    userBets[userId] = { amount: betAmount, choice: userChoice };

    await bot.sendMessage(chatId,
        `💸 *To place your bet:*\nSend *exactly ${betAmount} SOL* to:\n` +
        `\`${WALLET_ADDRESS}\`\nOnce sent, type /confirm to finalize your bet.`,
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

        if (!paymentCheckResult.success) {
            return await bot.sendMessage(chatId, `❌ Payment not verified! ${paymentCheckResult.message}`); // show message
        }
        usedTransactions.add(paymentCheckResult.tx); //store
        await bot.sendMessage(chatId, `✅ Payment verified!`);

        const houseEdge = getHouseEdge(amount);
        const result = Math.random() > houseEdge ? choice : (choice === 'heads' ? 'tails' : 'heads');
        const win = result === choice;
        const payout = win ? amount : 0;

        const displayName = msg.from.username ? `@${msg.from.username}` : `<@${userId}>`;

        if (win) {
            // --- PAYOUT LOGIC USING sendSol FUNCTION ---
            const payerPrivateKey = process.env.BOT_PRIVATE_KEY;
            if (!payerPrivateKey) {
                console.error('BOT_PRIVATE_KEY environment variable not set!');
                return await bot.sendMessage(chatId, `⚠️ Payout failed: Bot's private key not configured.`);
            }

            let winnerPublicKey;
            if (paymentCheckResult && paymentCheckResult.tx) {
                try {
                    const parsedTransaction = await connection.getParsedTransaction(paymentCheckResult.tx);
                    if (parsedTransaction && parsedTransaction.transaction && parsedTransaction.transaction.message && parsedTransaction.transaction.message.accountKeys && parsedTransaction.transaction.message.accountKeys.length > 0) {
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

            const sendResult = await sendSol(connection, payerPrivateKey, winnerPublicKey, payout);

            if (sendResult.success) {
                await bot.sendMessage(chatId, `🎉 Congratulations, ${displayName}! You won ${payout.toFixed(4)} SOL!\nResult: ${result}\n💸 Winnings sent! TX: ${sendResult.signature}`);
            } else {
                await bot.sendMessage(chatId, `🎉 Congratulations, ${displayName}! You won ${payout.toFixed(4)} SOL!\nResult: ${result}\n⚠️ Payout failed: ${sendResult.error}`);
            }
            // --- END PAYOUT LOGIC ---
        } else {
            await bot.sendMessage(chatId, `❌ Sorry, ${displayName}! You lost.\nResult: ${result}`);
        }

        delete userBets[userId];
        delete coinFlipSessions[userId];
    } catch (error) {
        console.error('Error in /confirm:', error);
        await bot.sendMessage(chatId, `⚠️ An error occurred during confirmation.`);
    }
});

bot.onText(/\/race$/, async (msg) => {
    const chatId = msg.chat.id;
    const raceId = nextRaceId++;
    raceSessions[raceId] = {
        horses: availableHorses,
        bets: {}, // We might not even need this for a solo game
        status: 'open', // Could potentially go straight to 'betting' or similar
        usedTransactions: new Set()
    };

    let raceMessage = `🏁 **New Race! Place your bets!** 🏁\n\n`;
    raceSessions[raceId].horses.forEach(horse => {
        raceMessage += `${horse.emoji} ${horse.name} (Odds: ${horse.odds.toFixed(1)}x)\n`;
    });

    raceMessage += `\nTo place your bet, use:\n\`/betrace [amount] [horse_name]\`\n` +
        `Example: \`/betrace 0.1 Blue\``;

    await bot.sendMessage(chatId, raceMessage, { parse_mode: 'Markdown' });

    // The setTimeout for closing betting is REMOVED
    // We will proceed with the race after the user confirms their bet.
});

bot.onText(/\/betrace (\d+\.\d+) (\w+)/i, async (msg, match) => {
    const chatId = msg.chat.id;
    const userId = msg.from.id;
    const betAmount = parseFloat(match[1]);
    const chosenHorseName = match[2].toLowerCase();

    const raceId = Object.keys(raceSessions).reverse().find(id => raceSessions[id].status === 'open');
    if (!raceId) {
        return bot.sendMessage(chatId, `❌ No race is currently accepting bets.`);
    }

    if (betAmount < RACE_MIN_BET || betAmount > RACE_MAX_BET) {
        return bot.sendMessage(chatId, `❌ Bet must be between ${RACE_MIN_BET} - ${RACE_MAX_BET} SOL`);
    }

    const race = raceSessions[raceId];
    const horse = race.horses.find(h => h.name.toLowerCase() === chosenHorseName);

    if (!horse) {
        return bot.sendMessage(chatId, `❌ Invalid horse name. Options:\n` +
            race.horses.map(h => `${h.emoji} ${h.name}`).join('\n'));
    }

    userRaceBets[userId] = { raceId, amount: betAmount, horse: horse.name };

    await bot.sendMessage(chatId, `✅ Bet placed: ${betAmount} SOL on ${horse.emoji} *${horse.name}* (Odds: ${horse.odds.toFixed(1)}x).\nSend the amount to:\n\`${WALLET_ADDRESS}\`\nThen type /confirmrace to verify payment and start the race!`,
        { parse_mode: 'Markdown' }
    );
});

bot.onText(/^\/confirmrace$/, async (msg) => {
    const chatId = msg.chat.id;
    const userId = msg.from.id;
    const raceBetInfo = userRaceBets[userId];

    if (!raceBetInfo) {
        return bot.sendMessage(chatId, `⚠️ No active race bet found. Please use /betrace first.`);
    }

    const { raceId, amount, horse } = raceBetInfo;
    const race = raceSessions[raceId];

    try {
        await bot.sendMessage(chatId, `🔍 Verifying your payment of ${amount} SOL for Race ${raceId}...`);
        const paymentCheckResult = await checkPayment(amount);

        if (!paymentCheckResult.success) {
            return bot.sendMessage(chatId, `❌ Payment not verified for Race ${raceId}! ${paymentCheckResult.message}`); //send message
        }

        if (race.usedTransactions.has(paymentCheckResult.tx)) {
            return bot.sendMessage(chatId, `❌ Payment for this race has already been used.`);
        }

        race.usedTransactions.add(paymentCheckResult.tx); //store
        await bot.sendMessage(chatId, `✅ Payment verified for Race ${raceId}! The race is on! 🐎💨`);

        const horsesInRace = race.horses;

        let winningHorse;
        const randomNumber = Math.random();
        let cumulativeProbability = 0;

        for (const contender of horsesInRace) {
            cumulativeProbability += contender.winProbability;
            if (randomNumber < cumulativeProbability) {
                winningHorse = contender;
                break;
            }
        }

        await bot.sendMessage(chatId, `And they're off! The horses are neck and neck...`, { parse_mode: 'Markdown' });
        await new Promise(resolve => setTimeout(resolve, 1500));
        await bot.sendMessage(chatId, `${horsesInRace[Math.floor(Math.random() * horsesInRace.length)].emoji} ${horsesInRace[Math.floor(Math.random() * horsesInRace.length)].name} surges forward!`, { parse_mode: 'Markdown' });
        await new Promise(resolve => setTimeout(resolve, 2000));
        await bot.sendMessage(chatId, `It's a tight finish!`, { parse_mode: 'Markdown' });
        await new Promise(resolve => setTimeout(resolve, 1000));
        await bot.sendMessage(chatId, `🏆 **And the winner is... ${winningHorse.emoji} ${winningHorse.name}!** 🏆`, { parse_mode: 'Markdown' });

        if (horse === winningHorse.name) {
            const winningHorseData = race.horses.find(h => h.name === horse);
            const payout = amount * winningHorseData.odds;
            await bot.sendMessage(chatId, `🎉 You backed the winner! You won ${payout.toFixed(4)} SOL.`);

            try {
                const payerPrivateKey = process.env.BOT_PRIVATE_KEY;
                if (!payerPrivateKey) {
                    console.error('BOT_PRIVATE_KEY environment variable not set!');
                    return await bot.sendMessage(chatId, `⚠️ Payout failed: Bot's private key not configured.`);
                }

                let winnerPublicKey;
                if (paymentCheckResult && paymentCheckResult.tx) {
                    try {
                        const parsedTransaction = await connection.getParsedTransaction(paymentCheckResult.tx);
                        if (parsedTransaction && parsedTransaction.transaction && parsedTransaction.transaction.message && parsedTransaction.transaction.message.accountKeys && parsedTransaction.transaction.message.accountKeys.length > 0) {
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

                const sendResult = await sendSol(connection, payerPrivateKey, winnerPublicKey, payout);

                if (sendResult.success) {
                    await bot.sendMessage(chatId, `💸 Winnings of ${payout.toFixed(4)} SOL sent! TX: ${sendResult.signature}`);
                } else {
                    await bot.sendMessage(chatId, `⚠️ Payout failed: ${sendResult.error}`);
                }

            } catch (error) {
                console.error('Error during payout:', error);
                await bot.sendMessage(chatId, `⚠️ Payout failed due to an error.`);
            }

        } else {
            await bot.sendMessage(chatId, `Sorry, your horse ${horse} didn't win this time. Better luck next race!`);
        }

        delete userRaceBets[userId];
        delete raceSessions[raceId];

    } catch (error) {
        console.error('Error in /confirmrace:', error);
        await bot.sendMessage(chatId, `⚠️ An error occurred while processing the race.`);
    }
});

