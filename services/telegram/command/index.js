// services/telegram/commands/index.js
// This file acts as a router for incoming Telegram commands.

import { performanceMonitor } from '../../../utils/performance.js';
import { checkCooldown, setCooldown, cleanupCooldownMap } from '../../../utils/state.js'; // Import cooldown functions
import { safeSendMessage } from '../sender.js'; // Import safe sender

// Import ALL the command handler functions from the other files in this folder
import { handleStartCommand } from './start.js';
import { handleCoinflipCommand, handleBetCommand } from './coinflip.js';
import { handleRaceCommand, handleBetRaceCommand } from './race.js';
import { handleWalletCommand } from './wallet.js';
import { handleHelpCommand } from './help.js';

// This is the main function that decides which command handler to call
// It will be called by the bot.on('message', ...) listener in handlers.js
export async function handleMessage(msg) {
    // Basic filtering should happen before this function is called (e.g., ignore bots, non-text)
    if (!msg || !msg.text) return;

    const userId = String(msg.from.id);
    const chatId = String(msg.chat.id);
    const messageText = msg.text.trim();

    // --- Command Parsing ---
    // Extracts command (e.g., 'start') and arguments (e.g., '0.1 heads')
    const commandMatch = messageText.match(/^\/(\w+)(?:@\w+)?(?:\s+(.*))?$/);
    if (!commandMatch) return; // Not a command format we handle

    const command = commandMatch[1].toLowerCase();
    const args = commandMatch[2] || ''; // The rest of the message after the command

    // --- Cooldown Check ---
    // Apply cooldown only for actual commands to prevent spam
    if (checkCooldown(userId)) {
        // console.log(`User ${userId} command /${command} ignored due to cooldown.`); // Optional debug log
        return; // Ignore command if user is on cooldown
    }
    setCooldown(userId); // Apply cooldown *after* identifying it's a command we might handle

    // Log received command for debugging
    // console.log(`Received command: /${command} from user ${userId} in chat ${chatId}. Args: "${args}"`);

    try {
        // --- Command Routing ---
        let commandProcessed = true; // Assume success unless error or unknown command
        switch (command) {
            case 'start':
                await handleStartCommand(msg);
                break;
            case 'coinflip':
                await handleCoinflipCommand(msg);
                break;
            case 'bet': // Coinflip bet command
                await handleBetCommand(msg, args); // Pass arguments string
                break;
            case 'race':
                await handleRaceCommand(msg);
                break;
            case 'betrace': // Race bet command
                await handleBetRaceCommand(msg, args); // Pass arguments string
                break;
            case 'wallet':
                await handleWalletCommand(msg);
                break;
            case 'help':
                await handleHelpCommand(msg);
                break;
            default:
                commandProcessed = false; // Not a known command we handle
                // console.log(`Ignoring unknown command: /${command}`); // Optional debug log
                break;
        }

        if (commandProcessed) {
            performanceMonitor.logRequest(true); // Log successful handling attempt
        }

    } catch (error) {
        console.error(`❌ Error processing command /${command} from user ${userId} in chat ${chatId}:`, error);
        performanceMonitor.logRequest(false); // Log error
        try {
            // Notify user about the error
            await safeSendMessage(chatId, "⚠️ An unexpected error occurred while processing your command. Please try again later.");
        } catch (tgError) {
            console.error("❌ Failed to send error message to chat:", tgError.message);
        }
    } finally {
        // Optional: Periodic cleanup of cooldown map (maybe better called from a timer in index.js)
        // cleanupCooldownMap();
    }
}
