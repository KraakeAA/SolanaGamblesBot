// Create a temporary file, e.g., test-polling.js
// Make sure BOT_TOKEN is set in Railway env vars for this test
import 'dotenv/config'; // If you use dotenv
import TelegramBot from 'node-telegram-bot-api';

const token = process.env.BOT_TOKEN;

if (!token) {
    console.error("MINIMAL TEST: Missing BOT_TOKEN!");
    process.exit(1);
}
console.log("MINIMAL TEST: Starting minimal polling test...");

try {
    // Explicitly enable polling in constructor
    const bot = new TelegramBot(token, { polling: true });
    console.log("MINIMAL TEST: Bot instance created with polling: true");

    bot.on('polling_error', (error) => {
        // Log ALL polling errors
        console.error(`MINIMAL POLLING ERROR: Code ${error.code} | ${error.message}`, error.stack);
    });

    bot.on('error', (error) => {
        console.error('MINIMAL GENERAL ERROR:', error);
    });

    // Listener for ANY message
    bot.on('message', (msg) => {
        console.log("---- MINIMAL TEST: MESSAGE RECEIVED! ----"); // Log clearly
        console.log(JSON.stringify(msg, null, 2)); // Log the full message object
        const chatId = msg.chat.id;
        bot.sendMessage(chatId, 'Minimal test received message!').catch(sendErr => {
            console.error(`MINIMAL TEST: Failed to send reply: ${sendErr.message}`);
        });
    });

    // Verify token again with getMe within this context
    bot.getMe()
        .then(me => console.log(`MINIMAL TEST: Polling started for @${me.username}`))
        .catch(err => console.error(`MINIMAL TEST: getMe failed: ${err.message}. Token likely invalid.`));

    console.log("Minimal polling bot script running... Send a message to the bot.");

} catch (initError) {
    console.error("MINIMAL TEST: Error during bot initialization:", initError);
}

// Keep alive basic handlers
process.on('SIGINT', () => { console.log("Minimal test exiting (SIGINT)"); process.exit(0); });
process.on('SIGTERM', () => { console.log("Minimal test exiting (SIGTERM)"); process.exit(0); });
