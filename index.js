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
    { name: 'Yellow', emoji: 'ð', odds: 1.1, winProbability: 0.25 },
    { name: 'Orange', emoji: 'ð§¡', odds: 2.0, winProbability: 0.20 },
    { name: 'Blue', emoji: 'ð', odds: 3.0, winProbability: 0.15 },
    { name: 'Cyan', emoji: 'ð¨ð¾', odds: 4.0, winProbability: 0.12 },
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
const usedTransactions = new Set(); // To store used transaction signatures

async function checkPayment(expectedSol) {
    const pubKey = new PublicKey(WALLET_ADDRESS);
    const signatures = await connection.getSignaturesForAddress(pubKey, { limit: 5 });

    for (let sig of signatures) {
        const tx = await connection.getParsedTransaction(sig.signature);
        if (!tx || !tx.meta) continue;
}

        const txTime = new Date(sig.blockTime * 1000);
        const timeDiff = (new Date() - txTime) / 1000 / 60;
        if (timeDiff > 15) continue;
}

        const amount = (tx.meta.postBalances[0] - tx.meta.preBalances[0]) / LAMPORTS_PER_SOL;
        if (Math.abs(Math.abs(amount) - expectedSol) < 0.0015) {
}
            if (usedTransactions.has(sig.signature)) {
}
                return { success: false, message: 'Transaction already used' }; // Add message
            }
            return { success: true, tx: sig.signature };
        }
    }
    return { success: false, message: 'Payment not found' }; //Add message
}

async function sendSol(connection, payerPrivateKey, recipientPublicKey, amount) {
    try {
}
        const payerWallet = Keypair.fromSecretKey(bs58.decode(payerPrivateKey));
        const payerPublicKey = payerWallet.publicKey;
        const payoutAmountLamports = Math.round(amount * LAMPORTS_PER_SOL);

        const transaction = new Transaction().add(
            SystemProgram.transfer({
                fromPubkey: payerPublicKey
                toPubkey: recipientPublicKey
                lamports: payoutAmountLamports
            })
        );

        const signature = await sendAndConfirmTransaction(
            connection
            transaction
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
}
    if (amount <= 0.049) return 0.75;
}
    if (amount <= 0.0999999) return 0.80;
}
    return 0.99;
};

if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR);
}
if (!fs.existsSync(LOG_PATH)) fs.writeFileSync(LOG_PATH, '[]');
}


function getPayerFromTransaction(tx, expectedAmount) {
    if (!tx || !tx.meta || !tx.transaction) return null;
}

    const keys = tx.transaction.message.accountKeys;
    const preBalances = tx.meta.preBalances;
    const postBalances = tx.meta.postBalances;

    for (let i = 0; i < keys.length; i++) {
        const balanceDiff = (preBalances[i] - postBalances[i]) / LAMPORTS_PER_SOL;
        if (balanceDiff >= expectedAmount - 0.001) {
}
            return keys[i].pubkey;
        }
    }

    return null;
}


bot.onText(/\/start$/, async (msg) => {
    const chatId = msg.chat.id;
    const gifUrl = 'https://media4.giphy.com/media/mrJg7yrURBntrDL804/giphy.gif';

    await bot.sendAnimation(chatId, gifUrl, {
        caption: `Welcome to *Solana Gambles*!\n\nAvailable games:\n- /coinflip\n- /race\n\nType /refresh to see this menu again.`
        parse_mode: 'Markdown'
    });
});


bot.onText(/\/reset$/, async (msg) => {
    const chatId = msg.chat.id;

    // Check if it's a group chat
    if (msg.chat.type === 'group' || msg.chat.type === 'supergroup') {
}
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

    // Clear any active race sessions.
    for (const raceId in raceSessions) {
        if (raceSessions[raceId].status === 'open') {
}
            delete raceSessions[raceId];
        }
    }
    usedTransactions.clear();
    // Send the /start message to reset the bot's state in the chat
    bot.sendMessage(chatId, `Bot state has been reset.`);
}


bot.onText(/\/coinflip$/, (msg) => {
    const userId = msg.from.id;
    coinFlipSessions[userId] = true;
    bot.sendMessage(
        msg.chat.id
        `ðª You've started a coin flip game! Please choose an amount and heads/tails:\n\n` +
        `/bet 0.01 heads\n/bet 0.05 tails\n\nMin: ${MIN_BET} SOL | Max: ${MAX_BET} SOL`
        { parse_mode: 'Markdown' }
    );
});


bot.onText(/\/wallet$/, async (msg) => {
    const userId = msg.from.id;
    const wallet = linkedWallets[userId];
    if (wallet) {
}
        await bot.sendMessage(msg.chat.id, `ð Your linked wallet address is:
\`${wallet}\``, { parse_mode: 'Markdown' });
    } else {
        await bot.sendMessage(msg.chat.id, `â ï¸ No wallet is linked to your account yet. Place a verified bet to link one.`);
    }
});


bot.onText(/\/refresh$/, async (msg) => {
    const chatId = msg.chat.id;
    const gifUrl = 'https://media4.giphy.com/media/mrJg7yrURBntrDL804/giphy.gif';
    await bot.sendAnimation(chatId, gifUrl, {
        caption: `Welcome to Solana Gambles!\n\nAvailable games:\n- /coinflip\n- /race`
        parse_mode: 'Markdown'
    });
});

bot.onText(/\/bet (\d+\.\d+) (heads|tails)/i, async (msg, match) => {
    const userId = msg.from.id;
    const chatId = msg.chat.id;

    if (!coinFlipSessions[userId]) {
}
        return bot.sendMessage(chatId, `â ï¸ Please start a coin flip game first using /coinflip`);
    }

    const betAmount = parseFloat(match[1]);
    const userChoice = match[2].toLowerCase();

    if (betAmount < MIN_BET || betAmount > MAX_BET) {
}
        return bot.sendMessage(chatId, `â Bet must be between ${MIN_BET} - ${MAX_BET} SOL`);
    }

    userBets[userId] = { amount: betAmount, choice: userChoice };

    await bot.sendMessage(chatId
        `ð¸ *To place your bet:*\nSend *exactly ${betAmount} SOL* to:\n` +
        `\`${WALLET_ADDRESS}\`\nOnce sent, type /confirm to finalize your bet.`
        { parse_mode: 'Markdown' }
    );
});

bot.onText(/^\/confirm$/, async (msg) => {
    const userId = msg.from.id;
    const chatId = msg.chat.id;
    const betInfo = userBets[userId];

    if (!betInfo) {
}
        return await bot.sendMessage(chatId, `â ï¸ No active bet found. Please use the /bet command first.`);
    }

    const { amount, choice } = betInfo;

    let paymentCheckResult;
    try {
}
        await bot.sendMessage(chatId, `ð Verifying your payment of ${amount} SOL...`);
        await new Promise(resolve => setTimeout(resolve, 5000));
        paymentCheckResult = await checkPayment(amount);

        if (!paymentCheckResult.success) {
}
            return await bot.sendMessage(chatId, `â Payment not verified! ${paymentCheckResult.message}`); // show message
        }
        usedTransactions.add(paymentCheckResult.tx); //store
        await bot.sendMessage(chatId, `â Payment verified!`);

        const houseEdge = getHouseEdge(amount);
        await bot.sendMessage(chatId, `ðª Flipping the coin...`);
        await new Promise(resolve => setTimeout(resolve, 2000));
        await bot.sendMessage(chatId, `ð Spinning...`);
        await new Promise(resolve => setTimeout(resolve, 1500));
        const result = Math.random() > houseEdge ? choice : (choice === 'heads' ? 'tails' : 'heads');
        const win = result === choice;
        const payout = win ? amount : 0;

        const displayName = msg.from.username ? `@${msg.from.username}` : `<@${userId}>`;

        if (win) {
}
            // --- PAYOUT LOGIC USING sendSol FUNCTION ---
            const payerPrivateKey = process.env.BOT_PRIVATE_KEY;
            if (!payerPrivateKey) {
}
                console.error('BOT_PRIVATE_KEY environment variable not set!');
                return await bot.sendMessage(chatId, `â ï¸ Payout failed: Bot's private key not configured.`);
            }

            let winnerPublicKey;
            if (paymentCheckResult && paymentCheckResult.tx) {
}
                try {
}
                    const parsedTransaction = await connection.getParsedTransaction(paymentCheckResult.tx);
                    if (parsedTransaction && parsedTransaction.transaction && parsedTransaction.transaction.message && parsedTransaction.transaction.message.accountKeys && parsedTransaction.transaction.message.length > 0) {
}
                        

winnerPublicKey = getPayerFromTransaction(parsedTransaction, amount);
if (!winnerPublicKey) {
}
    console.warn('Could not determine the sender from the transaction.');
    return await bot.sendMessage(chatId, `â ï¸ Payout failed: Could not determine payment sender.`);
}

const winnerAddress = winnerPublicKey.toBase58();
if (linkedWallets[userId] && linkedWallets[userId] !== winnerAddress) {
}
    return await bot.sendMessage(chatId, `â ï¸ This wallet does not match your linked wallet. Please use your original address.`);
}
linkedWallets[userId] = winnerAddress;


                        console.log('Extracted winner public key:', winnerPublicKey.toBase58());
                    } else {
                        console.warn('Could not parse transaction to determine sender.');
                        return await bot.sendMessage(chatId, `â ï¸ Payout failed: Could not analyze your payment transaction.`);
                    }
                } catch (error) {
                    console.error('Error parsing transaction for sender:', error);
                    return await bot.sendMessage(chatId, `â ï¸ Payout failed: Error analyzing your payment transaction.`);
                }
            } else {
                console.warn('No transaction signature available to determine sender.');
                return await bot.sendMessage(chatId, `â ï¸ Payout failed: No payment transaction found.`);
            }

            if (!winnerPublicKey) {
                console.warn('Winner public key is undefined.');
                return await bot.sendMessage(chatId, `â ï¸ Payout failed: Could not determine recipient.`);
            }

            const sendResult = await sendSol(connection, payerPrivateKey, winnerPublicKey, payout);

            if (sendResult.success) {
}
                
await bot.sendAnimation(chatId, "https://media.giphy.com/media/v1.Y2lkPTc5MGI3NjExczB1anY2Y3YzdXY0NxdnUwZ3NtNWhkZ3h2b2puZjZ2dDdpdmliZmV6aSZlcD12MV9naWZzX3NlYXJjaCZjdD1n/3ohjUWVkjvGf4RAjDi/giphy.gif");
await bot.sendMessage(chatId, 
    `ð *YOU WIN!* ð

ð Congratulations, ${displayName}!

*Result:* \`${result}\`
ð¸ Winnings sent!
    { parse_mode: 'Markdown' });
            } else {
                
await bot.sendAnimation(chatId, "https://media.giphy.com/media/v1.Y2lkPTc5MGI3NjExczB1anY2Y3YzdXY0NxdnUwZ3NtNWhkZ3h2b2puZjZ2dDdpdmliZmV6aSZlcD12MV9naWZzX3NlYXJjaCZjdD1n/3ohjUWVkjvGf4RAjDi/giphy.gif");
await bot.sendMessage(chatId, 
    `ð *YOU WIN!* ð

ð Congratulations, ${displayName}!

*Result:* \`${result}\`
ð¸ Winnings sent!
await bot.sendAnimation(chatId, "https://media.giphy.com/media/v1.Y2lkPTc5MGI3NjExczB1anY2Y3YzdXY0NxdnUwZ3NtNWhkZ3h2b2puZjZ2dDdpdmliZmV6aSZlcD12MV9naWZzX3NlYXJjaCZjdD1n/3ohjUWVkjvGf4RAjDi/giphy.gif");
await bot.sendMessage(chatId, `ð *YOU WIN!* ð\n\nð Congratulations, ${displayName}!\n\n*Result:* \`${result}\`\nð¸ Winnings sent!\nTX: \`${sendResult.signature}\`, { parse_mode: 'Markdown' });
                }

            } catch (error) {
                console.error('Error during payout:', error);
                await bot.sendMessage(chatId, `â ï¸ Payout failed due to an error.`);
            }

        } else {
            await bot.sendMessage(chatId, `ð *YOU LOSE THE RACE!* ð

Your horse ${horse} didn't make it.
Better luck next time!`, { parse_mode: 'Markdown' });
        }

        delete userRaceBets[userId];
        delete raceSessions[raceId];

    } catch (error) {
        console.error('Error in /confirmrace:', error);
        await bot.sendMessage(chatId, `â ï¸ An error occurred while processing the race.`);
});




bot.onText(/\/help$/, async (msg) => {
    const chatId = msg.chat.id;
    await bot.sendMessage(chatId, `ð *How to Play*\n\n1. Start a game: /coinflip or /race\n2. Place a bet: /bet [amount] [heads/tails] or /betrace [amount] [horse]\n3. Send SOL to the bot address\n4. Confirm with /confirm or /confirmrace\n\nUse /wallet to view your linked wallet.`, { parse_mode: 'Markdown' });
});
)
