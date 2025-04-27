// services/telegram/setup.js
import { bot } from './bot.js';
import { config } from '../../config/index.js';

const webhookPath = `/bot${config.botToken}`; // Use config

export async function setupTelegramWebhook() {
    if (config.isRailway && config.railwayPublicDomain) {
        const webhookUrl = `https://${config.railwayPublicDomain}${webhookPath}`;
        // ... (copy the rest of setupTelegramWebhook function content)
    } else {
         console.log("ℹ️ Not in Railway environment or domain not set, webhook not configured.");
         return false;
    }
     return false; // Should have returned true/false within the loop/logic
}

export async function startPollingIfNeeded() {
    try {
        const info = await bot.getWebHookInfo();
        // ... (copy the rest of startPollingIfNeeded function content)
    } catch (err) {
         // ... (copy error handling)
    }
}
