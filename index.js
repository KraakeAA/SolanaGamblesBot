require('dotenv').config();
const REQUIRED_ENV_VARS = ['BOT_TOKEN', 'BOT_PRIVATE_KEY', 'RACE_BOT_PRIVATE_KEY'];
REQUIRED_ENV_VARS.forEach((key) => {
    if (!process.env[key]) {
        console.error(`Environment variable ${key} is missing.`);
        process.exit(1);
    }
});
const TelegramBot = require('node-telegram-bot-api');
const {
    Connection,
    clusterApiUrl,
    PublicKey,
    LAMPORTS_PER_SOL,
    Keypair,
    Transaction,
    SystemProgram,
    sendAndConfirmTransaction,
    ComputeBudgetProgram
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
const MAIN_WALLET_ADDRESS = '9HL7W4XZJDX6br3ojjU6BLHp7oZVP3nCDKxQ21TNanQf';
const RACE_WALLET_ADDRESS = 'ESwYNvNieSniHZKAw2MezWi7niYmtZbjtKUR8KyaySiv'; // New race wallet
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
    { name: 'Yellow', emoji: 'ð', odds: 1.1, winProbability: 0.25 },
    { name: 'Orange', emoji: 'ð§¡', odds: 2.0, winProbability: 0.20 },
    { name: 'Blue', emoji: 'ð', odds: 3.0, winProbability: 0.15 },
    { name: 'Cyan', emoji: 'ð§', odds: 4.0, winProbability: 0.12 },
    { name: 'White', emoji: 'ð¤', odds: 5.0, winProbability: 0.09 },
    { name: 'Red', emoji: 'â¤ï¸', odds: 6.0, winProbability: 0.07 },
    { name: 'Black', emoji: 'ð¤', odds: 7.0, winProbability: 0.05 },
    { name: 'Pink', emoji: 'ð©·', odds: 8.0, winProbability: 0.03 },
    { name: 'Purple', emoji: 'ð', odds: 9.0, winProbability: 0.02 },
    { name: 'Green', emoji: 'ð', odds: 10.0, winProbability: 0.01 },
    { name: 'Silver', emoji: 'ð©¶', odds: 15.0, winProbability: 0.01 },
];

const userBets = {};
const coinFlipSessions = {};
const linkedWallets = {}; // Telegram userId -> Wallet address mapping
const userPayments = {}; // Store payment details { userId: { coinflipTx: txId, raceTx: txId } }
const confirmCooldown = {}; // userId: lastConfirmationTimestamp
const cooldownInterval = 3000; // 3 seconds

async function checkPayment(expectedSol, userId, gameType, targetWalletAddress) {
    const pubKey = new PublicKey(targetWalletAddress);
    const signatures = await connection.getSignaturesForAddress(pubKey, { limit: 5 });

    for (let sig of signatures) {
        const tx = await connection.getParsedTransaction(sig.signature);
        if (!tx || !tx.meta) continue;

        const txTime = new Date(sig.blockTime * 1000);
        const timeDiff = (new Date() - txTime) / 1000 / 60;
        if (timeDiff > 15) continue;

        const amount = (tx.meta.postBalances[0] - tx.meta.preBalances[0]) / LAMPORTS_PER_SOL;
        if (Math.abs(Math.abs(amount) - expectedSol) < 0.0015) {
            if (!userPayments[userId]) {
                userPayments[userId] = {};
            }
            if (gameType === 'coinflip' && userPayments[userId].coinflipTx === sig.signature) {
                return { success: false, message: 'Payment already used for coinflip.' };
            }
            if (gameType === 'race' && userPayments[userId].raceTx === sig.signature) {
                return { success: false, message: 'Payment already used for race.' };
            }
            return { success: true, tx: sig.signature };
        }
    }
    return { success: false, message: 'Payment not found.' };
}

async function sendSol(connection, payerPrivateKey, recipientPublicKey, amount) {
    const maxRetries = 3;
    let lastError = null;

    for (let retryCount = 0; retryCount < maxRetries; retryCount++) {
        try {
            const payerWallet = Keypair.fromSecretKey(bs58.decode(payerPrivateKey));
            const payerPublicKey = payerWallet.publicKey;
            const payoutAmountLamports = Math.round(amount * LAMPORTS_PER_SOL);

            // Fetch a fresh recent blockhash
            const latestBlockhash = await connection.getLatestBlockhash();

            const transaction = new Transaction({
                recentBlockhash: latestBlockhash.blockhash,
                feePayer: payerPublicKey,
            }).add(
                SystemProgram.transfer({
                    fromPubkey: payerPublicKey,
                    toPubkey: recipientPublicKey,
                    lamports: payoutAmountLamports,
                })
            );

            // Add compute budget instructions for increased priority
            const modifyComputeUnits = ComputeBudgetProgram.setComputeUnitLimit({
                units: 100000, // Adjust this value as needed
            });
            const addPriorityFee = ComputeBudgetProgram.setComputeUnitPrice({
                microLamports: 1000, // Adjust this value as needed
            });
            transaction.add(modifyComputeUnits, addPriorityFee);

            transaction.sign(payerWallet);

            const signature = await sendAndConfirmTransaction(
                connection,
                transaction,
                [payerWallet],
                { commitment: 'confirmed' } // Explicitly set commitment level
            );

            return { success: true, signature };

        } catch (error) {
            console.error(`Error sending SOL (attempt ${retryCount + 1}/${maxRetries}):`, error);
            lastError = error;
            if (error.message.includes('TransactionExpiredBlockheightExceededError')) {
                // Wait before retrying (exponential backoff)
                await new Promise(resolve => setTimeout(resolve, 1000 * Math.pow(2, retryCount)));
            } else {
                // Don't retry for other errors
                break;
            }
        }
    }
    return { success: false, error: lastError ? lastError.message : 'Max retries exceeded without success' };
}

const getHouseEdge = (amount) => {
    if (amount <= 0.01) return 0.70;
    if (amount <= 0.049) return 0.75;
    if (amount <= 0.0999999) return 0.80;
    return 0.99;
};

if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR);
if (!fs.existsSync(LOG_PATH)) fs.writeFileSync(LOG_PATH, '[]');


function getPayerFromTransaction(tx, expectedAmount) {
    if (!tx || !tx.meta || !tx.transaction) return null;

    const keys = tx.transaction.message.accountKeys;
    const preBalances = tx.meta.preBalances;
    const postBalances = tx.meta.postBalances;

    for (let i = 0; i < keys.length; i++) {
        const balanceDiff = (preBalances[i] - postBalances[i]) / LAMPORTS_PER_SOL;
        if (balanceDiff >= expectedAmount - 0.001) {
            return keys[i].pubkey;
        }
    }

    return null;
}


bot.onText(/\/start$/, async (msg) => {
    const chatId = msg.chat.id;
    const gifUrl = 'https://media4.giphy.com/media/mrJg7yrURBntrDL804/giphy.gif';

    await bot.sendAnimation(chatId, gifUrl, {
        caption: `Welcome to *Solana Gambles*!\n\nAvailable games:\n- /coinflip\n- /race\n\nType /refresh to see this menu again.`,
        parse_mode: 'Markdown'
    });
});


bot.onText(/\/reset$/, async (msg) => {
    const chatId = msg.chat.id;

    // Check if it's a group chat
    if (msg.chat.type === 'group' || msg.chat.type === 'supergroup') {
        // Send a message indicating that the command is disabled in group chats.
        await bot.sendMessage(chatId, `â ï¸ The /reset command is disabled in group chats.`);
    } else {
        // If it's a private chat, proceed with reset
        resetBotState(chatId); // Extract reset logic into a function
    }
});

function resetBotState(chatId) {
    // Clear user-specific data
    delete coinFlipSessions[chatId];
    delete userBets[chatId];
    delete userRaceBets[chatId];
    delete userPayments[chatId]; // Clear payment tracking as well

    // Clear any active race sessions.
    for (const raceId in raceSessions) {
        if (raceSessions[raceId].status === 'open') {
            delete raceSessions[raceId];
        }
    }
    // usedTransactions.clear(); // We are now tracking per user/game
    // Send the /start message to reset the bot's state in the chat
    bot.sendMessage(chatId, `*Welcome to Solana Gambles!*\n\nAvailable games:\n- /coinflip\n- /race\n\nUse /refresh to return to this menu.`, { parse_mode: "Markdown" });
    bot.sendMessage(chatId, `Bot state has been reset.`);
}


bot.onText(/\/coinflip$/, (msg) => {
    const userId = msg.from.id;
    coinFlipSessions[userId] = true;
    bot.sendMessage(
        msg.chat.id,
        `ðª You've started a coin flip game! Please choose an amount and heads/tails:\n\n` +
        `/bet 0.01 heads\n/bet 0.05 tails\n\nMin: ${MIN_BET} SOL | Max: ${MAX_BET} SOL`,
        { parse_mode: 'Markdown' }
    );
});


bot.onText(/\/wallet$/, async (msg) => {
    const userId = msg.from.id;
    const wallet = linkedWallets[userId];
    if (wallet) {
        await bot.sendMessage(msg.chat.id, `ð³ Your linked wallet address is:\n\`${wallet}\``, { parse_mode: 'Markdown' });
    } else {
        await bot.sendMessage(msg.chat.id, `â ï¸ No wallet is linked to your account yet. Place a verified bet to link one.`);
    }
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
        return bot.sendMessage(chatId, `â ï¸ Please start a coin flip game first using /coinflip`);
    }

    const betAmount = parseFloat(match[1]);
    const userChoice = match[2].toLowerCase();

    if (betAmount < MIN_BET || betAmount > MAX_BET) {
        return bot.sendMessage(chatId, `ð« Bet must be between ${MIN_BET} - ${MAX_BET} SOL`);
    }

    userBets[userId] = { amount: betAmount, choice: userChoice };

    await bot.sendMessage(chatId,
        `ð° *To place your bet:*\nSend *exactly ${betAmount} SOL* to:\n` +
        `\`${MAIN_WALLET_ADDRESS}\`\nOnce sent, type /confirm to finalize your bet.`,
        { parse_mode: 'Markdown' }
    );
});

bot.onText(/^\/confirm$/, async (msg) => {
    const userId = msg.from.id;
    const chatId = msg.chat.id;

    if (confirmCooldown[userId] && (Date.now() - confirmCooldown[userId]) < cooldownInterval) {
        console.log(`[/confirm] User ${userId} is on cooldown.`);
        return bot.sendMessage(chatId, `â ï¸ Please wait a few seconds before confirming again.`);
    }

    confirmCooldown[userId] = Date.now();

    const betInfo = userBets[userId];

    if (!betInfo) {
        console.log(`[/confirm] User ${userId} has no active bet.`);
        return await bot.sendMessage(chatId, `â ï¸ No active bet found. Please use the /bet command first.`);
    }

    const { amount, choice } = betInfo;

    let paymentCheckResult;
    try {
        console.log(`[/confirm] User ${userId} confirming payment of ${amount} SOL.`);
        await bot.sendMessage(chatId, `ð Verifying your payment of ${amount} SOL...`);
        await new Promise(resolve => setTimeout(resolve, 5000));
        paymentCheckResult = await checkPayment(amount, userId, 'coinflip', MAIN_WALLET_ADDRESS); // Use MAIN_WALLET_ADDRESS

        if (!paymentCheckResult.success) {
            console.log(`[/confirm] User ${userId} - Payment not verified: ${paymentCheckResult.message}`);
            return await bot.sendMessage(chatId, `â Payment not verified! ${paymentCheckResult.message}`);
        }

        if (!userPayments[userId]) {
            userPayments[userId] = {};
        }
        userPayments[userId].coinflipTx = paymentCheckResult.tx; // Store the transaction ID

        await bot.sendMessage(chatId, `â Payment verified!`);
        console.log(`[/confirm] User ${userId} - Payment verified.`);

        const houseEdge = getHouseEdge(amount);
        const result = Math.random() > houseEdge ? choice : (choice === 'heads' ? 'tails' : 'heads');
        const win = result === choice;
        const payout = win ? amount : 0;

        const displayName = msg.from.username ? `@${msg.from.username}` : `<@${userId}>`;
        console.log(`[/confirm] User ${userId} - Result: ${result}, Win: ${win}, Payout: ${payout}`);

        if (win) {
            console.log(`[/confirm] User ${userId} - Sending winning message.`);
            // --- PAYOUT LOGIC USING sendSol FUNCTION ---
            const payerPrivateKey = process.env.BOT_PRIVATE_KEY;
            if (!payerPrivateKey) {
                console.error('BOT_PRIVATE_KEY environment variable not set!');
                return await bot.sendMessage(chatId, `â ï¸ Payout failed: Bot's private key not configured.`);
            }

            let winnerPublicKey;
            if (paymentCheckResult && paymentCheckResult.tx) {
                try {
                    const parsedTransaction = await connection.getParsedTransaction(paymentCheckResult.tx);
                    if (!parsedTransaction) {
                        console.warn('Could not fetch parsed transaction for payout.');
                        return await bot.sendMessage(chatId, `â ï¸ Payout failed: Could not retrieve transaction details.`);
                    }
                    if (!parsedTransaction.transaction) {
                        console.warn('Parsed transaction does not contain transaction data.');
                        return await bot.sendMessage(chatId, `â ï¸ Payout failed: Incomplete transaction data.`);
                    }
                    if (!parsedTransaction.transaction.message || !parsedTransaction.transaction.message.accountKeys) {
                        console.warn('Parsed transaction missing message or account keys.');
                        return await bot.sendMessage(chatId, `â ï¸ Payout failed: Missing account information.`);
                    }

                    winnerPublicKey = getPayerFromTransaction(parsedTransaction, amount);
                    if (!winnerPublicKey) {
                        console.warn('Could not determine the sender from the transaction.');
                        return await bot.sendMessage(chatId, `â ï¸ Payout failed: Could not determine payment sender.`);
                    }
                    }
                    const winnerAddress = winnerPublicKey.toBase58();
                    if (linkedWallets[userId] && linkedWallets[userId] !== winnerAddress) {
                        return await bot.sendMessage(chatId, `â ï¸ This wallet does not match your linked wallet. Please use your original address.`);
                    }
                    linkedWallets[userId] = winnerAddress;
                    console.log('Extracted winner public key:', winnerPublicKey.toBase58());

                } catch (error) {
                    console.error('Error parsing transaction for sender during payout:', error);
                    return await bot.sendMessage(chatId, `â ï¸ Payout failed: Error analyzing your payment transaction.`);
                }
            } else {
                console.warn('No transaction signature available to determine sender for payout.');
                return await bot.sendMessage(chatId, `â ï¸ Payout failed: No payment transaction found for payout.`);
            }

            if (!winnerPublicKey) {
                console.warn('Winner public key is undefined for payout.');
                return await bot.sendMessage(chatId, `â ï¸ Payout failed: Could not determine recipient for payout.`);
            }

            const sendResult = await sendSol(connection, process.env.BOT_PRIVATE_KEY, winnerPublicKey, payout); // Use BOT_PRIVATE_KEY

            if (sendResult.success) {
                await bot.sendMessage(chatId, `ð Congratulations, ${displayName}! You won ${payout.toFixed(4)} SOL!\nResult: ${result}\nð¸ Winnings sent! TX: ${sendResult.signature}`);
            } else {
                await bot.sendMessage(chatId, `ð Congratulations, ${displayName}! You won ${payout.toFixed(4)} SOL!\nResult: ${result}\nâ ï¸ Payout failed: ${sendResult.error}`);
            }
            // --- END PAYOUT LOGIC ---
        } else {
            console.log(`[/confirm] User ${userId} - Sending losing message.`);
            await bot.sendAnimation(chatId, "https://media.giphy.com/media/l2JHPBFzSF1zG0y92/giphy.gif");
            await bot.sendMessage(chatId, `ð *YOU LOSE!*\n\n${displayName}, you guessed *${choice}* but the coin landed *${result}*.`,
                { parse_mode: "Markdown" });
            await bot.sendMessage(chatId, `ð Sorry, ${displayName}! You lost.\nResult: ${result}`);
        }

        delete userBets[userId];
        delete coinFlipSessions[userId];
        if (userPayments[userId] && userPayments[userId].coinflipTx === paymentCheckResult.tx) {
            delete userPayments[userId].coinflipTx; // Clear the coinflip transaction after confirmation
        }

    } catch (error) {
        console.error('Error in /confirm:', error);
        await bot.sendMessage(chatId, `â ï¸ An error occurred during confirmation.`);
    }
});

bot.onText(/\/race$/, async (msg) => {
    const chatId = msg.chat.id;
    const raceId = nextRaceId++;
    const horses = [
        { name: 'Yellow', emoji: 'ð', odds: 1.1, winProbability: 0.25 },
        { name: 'Orange', emoji: 'ð§¡', odds: 2.0, winProbability: 0.20 },
        { name: 'Blue', emoji: 'ð', odds: 3.0, winProbability: 0.15 },
        { name: 'Cyan', emoji: 'ð§', odds: 4.0, winProbability: 0.12 },
        { name: 'White', emoji: 'ð¤', odds: 5.0, winProbability: 0.09 },
        { name: 'Red', emoji: 'â¤ï¸', odds: 6.0, winProbability: 0.07 },
        { name: 'Black', emoji: 'ð¤', odds: 7.0, winProbability: 0.05 },
        { name: 'Pink', emoji: 'ð©·', odds: 8.0, winProbability: 0.03 },
        { name: 'Purple', emoji: 'ð', odds: 9.0, winProbability: 0.02 },
        { name: 'Green', emoji: 'ð', odds: 10.0, winProbability: 0.01 },
        { name: 'Silver', emoji: 'ð©¶', odds: 15.0, winProbability: 0.01 },
    ];

    raceSessions[raceId] = {
        horses,
        usedTransactions: new Set(), // Keeping this for race-specific transaction tracking within a race
        status: 'open',
    };

    let raceMessage = `ð New Race! Place your bets!\n\n`;
    horses.forEach(horse => {
        raceMessage += `${horse.emoji} *${horse.name}* (Odds: ${horse.odds.toFixed(1)}x)\n`;
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
        return bot.sendMessage(chatId, `â ï¸ No race is currently accepting bets.`);
    }

    if (betAmount < RACE_MIN_BET || betAmount > RACE_MAX_BET) {
        return bot.sendMessage(chatId, `ð« Bet must be between ${RACE_MIN_BET} - ${RACE_MAX_BET} SOL`);
    }

    const race = raceSessions[raceId];
    const horse = race.horses.find(h => h.name.toLowerCase() === chosenHorseName);

    if (!horse) {
        return bot.sendMessage(chatId, `â ï¸ Invalid horse name. Options:\n` +
            race.horses.map(h => `${h.emoji} ${h.name}`).join('\n'));
    }

    userRaceBets[userId] = { raceId, amount: betAmount, horse: horse.name };

    await bot.sendMessage(chatId, `â Bet placed: ${betAmount} SOL on ${horse.emoji} *${horse.name}* (Odds: ${horse.odds.toFixed(1)}x).\nSend the amount to:\n\`${RACE_WALLET_ADDRESS}\`\nThen type /confirmrace to verify payment and start the race!`,
        { parse_mode: 'Markdown' }
    );
});

bot.onText(/^\/confirmrace$/, async (msg) => {
    const chatId = msg.chat.id;
    const userId = msg.from.id;
    const raceBetInfo = userRaceBets[userId];

    if (!raceBetInfo) {
        return bot.sendMessage(chatId, `â ï¸ No active race bet found. Please use /betrace first.`);
    }

    const { raceId, amount, horse } = raceBetInfo;
    const race = raceSessions[raceId];

    try {
        await bot.sendMessage(chatId, `ð Verifying your payment of ${amount} SOL for Race ${raceId}...`);
        const paymentCheckResult = await checkPayment(amount, userId, 'race', RACE_WALLET_ADDRESS); // Use RACE_WALLET_ADDRESS

        if (!paymentCheckResult.success) {
            return bot.sendMessage(chatId, `â Payment not verified for Race ${raceId}! ${paymentCheckResult.message}`);
        }

        if (!userPayments[userId]) {
            userPayments[userId] = {};
        }
        userPayments[userId].raceTx = paymentCheckResult.tx; // Store the transaction ID for the race

        await bot.sendMessage(chatId, `â Payment verified for Race ${raceId}! The race is on! ð`, { parse_mode: 'Markdown' });

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
        const midRaceDrama = [
            "Yellow surges ahead!",
            "Blue stumbles on the turn!",
            "Itâs neck and neck between Red and Black!",
            "Commentator: Unbelievable move from Orange!",
            "The crowd roars as the final stretch approaches!"
        ];
        const randomDrama = midRaceDrama[Math.floor(Math.random() * midRaceDrama.length)];
        await bot.sendMessage(chatId, randomDrama, { parse_mode: "Markdown" });
        await new Promise(resolve => setTimeout(resolve, 1200));
        await bot.sendMessage(chatId, `ð **And the winner is... ${winningHorse.emoji} ${winningHorse.name}!** ð`, { parse_mode: 'Markdown' });

        if (horse === winningHorse.name) {
            await bot.sendAnimation(chatId, "https://media.giphy.com/media/3ohzdIuqJoo8QdKlnW/giphy.gif");
            const finishFlair = {
                "Yellow": "Yellow crosses like lightning!",
                "Blue": "Blue storms the track in style!",
                "Black": "Black takes the win in total silence... mysterious!",
                "Red": "Red burns the turf behind them!",
                "Silver": "Silver shines in the spotlight!",
            };
            const flair = finishFlair[horse] || "";
            await bot.sendMessage(chatId, flair, { parse_mode: "Markdown" });
            await bot.sendMessage(chatId, `ð *You backed the winner!*\n\n${horse} took the crown!\n\nð° Payout: ${(amount * winningHorse.odds).toFixed(4)} SOL`, { parse_mode: "Markdown" });
            const payout = amount * winningHorse.odds;

            try {
                const racePayerPrivateKey = process.env.RACE_BOT_PRIVATE_KEY;
                if (!racePayerPrivateKey) {
                    console.error('RACE_BOT_PRIVATE_KEY environment variable not set!');
                    return await bot.sendMessage(chatId, `â ï¸ Race payout failed: Bot's race private key not configured.`);
                }

                let winnerPublicKey;
                if (paymentCheckResult && paymentCheckResult.tx) {
                    try {
                        const parsedTransaction = await connection.getParsedTransaction(paymentCheckResult.tx);
                        if (parsedTransaction && parsedTransaction.transaction && parsedTransaction.transaction.message && parsedTransaction.transaction.message.accountKeys && parsedTransaction.transaction.message.length > 0) {
                            winnerPublicKey = getPayerFromTransaction(parsedTransaction, amount);
                            if (!winnerPublicKey) {
                                console.warn('Could not determine the sender from the transaction.');
                                return await bot.sendMessage(chatId, `â ï¸ Race payout failed: Could not determine payment sender.`);
                            }
                            const winnerAddress = winnerPublicKey.toBase58();
                            if (linkedWallets[userId] && linkedWallets[userId] !== winnerAddress) {
                                return await bot.sendMessage(chatId, `â ï¸ This wallet does not match your linked wallet. Please use your original address.`);
                            }
                            linkedWallets[userId] = winnerAddress;
                            console.log('Extracted winner public key:', winnerPublicKey.toBase58());
                        } else {
                            console.warn('Could not parse transaction to determine sender.');
                            return await bot.sendMessage(chatId, `â ï¸ Race payout failed: Could not analyze your payment transaction.`);
                        }
                    } catch (error) {
                        console.error('Error parsing transaction for sender:', error);
                        return await bot.sendMessage(chatId, `â ï¸ Race payout failed: Error analyzing your payment transaction.`);
                    }
                } else {
                    console.warn('No transaction signature available to determine sender.');
                    return await bot.sendMessage(chatId, `â ï¸ Race payout failed: No payment transaction found.`);
                }

                if (!winnerPublicKey) {
                    console.warn('Winner public key is undefined.');
                    return await bot.sendMessage(chatId, `â ï¸ Race payout failed: Could not determine recipient.`);
                }

                const sendResult = await sendSol(connection, racePayerPrivateKey, winnerPublicKey, payout); // Use race private key

                if (sendResult.success) {
                    await bot.sendMessage(chatId, `ð¸ Race winnings of ${payout.toFixed(4)} SOL sent! TX: ${sendResult.signature}`);
                } else {
                    await bot.sendMessage(chatId, `â ï¸ Race payout failed: ${sendResult.error}`);
                }

            } catch (error) {
                console.error('Error during race payout:', error);
                await bot.sendMessage(chatId, `â ï¸ Race payout failed due to an error.`);
            }

        } else {
            await bot.sendAnimation(chatId, "https://media.giphy.com/media/26BRBupa6nRXMGBP2/giphy.gif");
            await bot.sendMessage(chatId, `ð *Your horse lost!*\n\n${horse} didnât cross the line first. Better luck next time.`, { parse_mode: "Markdown" });
            await bot.sendMessage(chatId, `Sorry, your horse ${horse} didn't win this time. Better luck next race!`);
        }

        delete userRaceBets[userId];
        delete raceSessions[raceId];
        if (userPayments[userId] && userPayments[userId].raceTx === paymentCheckResult.tx) {
            delete userPayments[userId].raceTx; // Clear the race transaction after confirmation
        }

    } catch (error) {
        console.error('Error in /confirmrace:', error);
        await bot.sendMessage(chatId, `â ï¸ An error occurred while processing the race.`);
    }
});
