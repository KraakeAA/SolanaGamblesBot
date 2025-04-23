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
    { name: 'Red', emoji: '🐎', odds: 2.5 },
    { name: 'Blue', emoji: '💙', odds: 3.0 },
    { name: 'Green', emoji: '💚', odds: 5.0 },
    { name: 'Yellow', emoji: '💛', odds: 2.0 },
    { name: 'Purple', emoji: '💜', odds: 4.0 },
];

const userBets = {};
const coinFlipSessions = {};

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

bot.onText(/\/start$/, async (msg) => {
    const chatId = msg.chat.id;
    const gifUrl = 'https://media4.giphy.com/media/mrJg7yrURBntrDL804/giphy.gif';

    await bot.sendAnimation(chatId, gifUrl, {
        caption: `Welcome to Solana Gambles!\n\nAvailable games:\n- /coinflip\n- /race\n\nType /refresh to see this menu again.`,
        parse_mode: 'Markdown',
        reply_markup: {
            inline_keyboard: [
                [{ text: '🪙 Start Coin Flip', callback_data: 'start_coinflip' }],
                [{ text: '🏁 Start Race', callback_data: 'start_race' }]
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
            return await bot.sendMessage(chatId, `❌ Payment not verified!`);
        }

        await bot.sendMessage(chatId, `✅ Payment verified!`);

        const houseEdge = getHouseEdge(amount);
        const result = Math.random() > houseEdge ? choice : (choice === 'heads' ? 'tails' : 'heads');
        const win = result === choice;
        const payout = win ? amount : 0;

        const displayName = msg.from.username ? `@${msg.from.username}` : `<@${userId}>`;

        if (win) {
            await bot.sendMessage(chatId, `🎉 Congratulations, ${displayName}! You won ${payout.toFixed(4)} SOL!\nResult: ${result}`);
            // Implement payout logic here (similar to previous versions)
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
        bets: {},
        status: 'open',
    };

    let raceMessage = `🏁 **New Race! Place your bets!** 🏁\n\n`;
    raceSessions[raceId].horses.forEach(horse => {
        raceMessage += `${horse.emoji} ${horse.name} (Odds: ${horse.odds}x)\n`;
    });

    raceMessage += `\nTo place your bet, use:\n\`/betrace [amount] [horse_name]\`\n` +
        `Example: \`/betrace 0.1 Blue\``;

    await bot.sendMessage(chatId, raceMessage, { parse_mode: 'Markdown' });

    setTimeout(() => {
        if (raceSessions[raceId] && raceSessions[raceId].status === 'open') {
            raceSessions[raceId].status = 'closed';
            bot.sendMessage(chatId, `Betting for Race ${raceId} is now closed! The race will begin shortly...`);
            // Implement runRace(chatId, raceId) here
        }
    }, 60000);
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

    race.bets[userId] = { amount: betAmount, horse: horse.name };
    userRaceBets[userId] = { raceId, amount: betAmount, horse: horse.name };

    await bot.sendMessage(chatId, `✅ Bet placed: ${betAmount} SOL on ${horse.emoji} *${horse.name}*.\nSend the amount to:\n\`${WALLET_ADDRESS}\`\nThen type /confirmrace to verify payment.`,
        { parse_mode: 'Markdown' }
    );
}); // <-- THIS CLOSING BRACE IS NOW PRESENT

bot.onText(/^\/confirmrace$/, async (msg) => {
    const chatId = msg.chat.id;
    const userId = msg.from.id;
    const raceBetInfo = userRaceBets[userId];

    if (!raceBetInfo) {
        return bot.sendMessage(chatId, `⚠️ No active race bet found. Please use /betrace first.`);
    }

    const { raceId, amount, horse } = raceBetInfo;

    try {
        await bot.sendMessage(chatId, `🔍 Verifying your payment of ${amount} SOL for Race ${raceId}...`);
        const paymentCheckResult = await checkPayment(amount);

        if (!paymentCheckResult.success) {
            return bot.sendMessage(chatId, `❌ Payment not verified for Race ${raceId}!`);
        }

        await bot.sendMessage(chatId, `✅ Payment verified for Race ${raceId}!`);

        // --- IMPLEMENT RACE OUTCOME LOGIC HERE ---
        // 1. Determine the winning horse for raceId.
        // 2. Check if the user's chosen horse (from raceBetInfo) is the winner.
        // 3. Calculate the payout based on the odds of the chosen horse.
        // 4. Send the payout to the user if they won.
        // 5. Log the race result and bets.

        const winningHorse = "Blue"; // Placeholder for actual race outcome logic
        if (horse === winningHorse) {
            const winningHorseData = availableHorses.find(h => h.name === horse);
            const payout = amount * winningHorseData.odds;
            await bot.sendMessage(chatId, `🎉 Congratulations! ${horse} won Race ${raceId}! You won ${payout.toFixed(4)} SOL.`);
            // Implement payout transaction here
        } else {
            await bot.sendMessage(chatId, `Sorry! ${horse} did not win Race ${raceId}. The winner was ${winningHorse}.`);
        }

        delete userRaceBets[userId]; // Clear the bet info after processing
        delete raceSessions[raceId]; // Optionally clear the race session after it's finished

    } catch (error) {
        console.error('Error in /confirmrace:', error);
        await bot.sendMessage(chatId, `⚠️ An error occurred while confirming your race bet.`);
    }
});

// You'll need to implement the runRace function if you want the bot to automatically trigger the race.
// async function runRace(chatId, raceId) {
//     // ... logic to simulate the race and determine the winner ...
// }
