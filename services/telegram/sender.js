// services/telegram/sender.js
import PQueue from 'p-queue';
import { bot } from './bot.js'; // Import the bot instance

const telegramSendQueue = new PQueue({ concurrency: 1, interval: 1000, intervalCap: 1 });

export function safeSendMessage(chatId, message, options = {}) {
    return telegramSendQueue.add(() =>
        bot.sendMessage(chatId, message, options).catch(err => {
            console.error(`❌ Telegram send error to chat ${chatId}:`, err.message);
            // Optionally re-throw or handle specific errors (e.g., 403 Forbidden)
            // Consider logging more context like the message content (truncated)
        })
    );
}
