// services/telegram/bot.js
import TelegramBot from 'node-telegram-bot-api';
import { config } from '../../config/index.js';

console.log("⚙️ Initializing Telegram Bot...");

export const bot = new TelegramBot(config.botToken, {
    polling: false, // Will be managed by setup.js
    request: {
        timeout: 10000,
        agentOptions: {
            keepAlive: true,
            timeout: 60000
        }
    }
});

console.log("✅ Telegram Bot instance created");
