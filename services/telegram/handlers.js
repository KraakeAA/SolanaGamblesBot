// services/telegram/handlers.js
import { bot } from './bot.js';
import { performanceMonitor } from '../../utils/performance.js';
import { handleMessage } from './commands/index.js'; // Import main message router
import PQueue from 'p-queue'; // Import PQueue if not already used

// --- Message Queue (Specific to handling incoming messages) ---
const messageQueue = new PQueue({
    concurrency: 5,   // Max concurrent messages processed
    timeout: 10000    // Max time per message task (ms)
});
console.log("✅ Message processing queue initialized");


export function setupEventHandlers() {
    console.log("⚙️ Setting up Telegram event handlers...");

    // Incoming message handler (wrapped in queue)
    bot.on('message', (msg) => {
         // Basic filtering before adding to queue
         if (!msg || !msg.from || msg.from.is_bot || !msg.chat || !msg.text) {
             return;
         }

        messageQueue.add(() => handleMessage(msg)) // Route to central command router
            .catch(err => {
                console.error("⚠️ Message processing queue error:", err);
                performanceMonitor.logRequest(false);
            });
    });

    // Polling error handler
    bot.on('polling_error', (error) => {
        console.error(`❌ Polling error: ${error.code} - ${error.message}`);
        if (error.code === 'ETELEGRAM' && error.message.includes('409 Conflict')) {
            console.error("❌❌❌ FATAL: Conflict detected! Another bot instance might be running. Exiting.");
            process.exit(1);
        }
        // Add more specific handling if needed
    });

    // General bot error handler
    bot.on('error', (error) => {
        console.error('❌ General Bot Error:', error);
        performanceMonitor.logRequest(false);
    });

    // Webhook error (optional, useful for debugging)
    bot.on('webhook_error', (error) => {
        console.error('❌ Webhook Error:', error.code); // => 'EPARSE'
         // console.error(error.response.body); // => { ok: false, error_code: 400, description: 'Bad Request: ...' }
    });

    console.log("✅ Telegram event handlers set up.");
}

// --- Webhook Request Handler ---
// This function will be used by app.js for the POST route
export function processWebhookUpdate(req, res) {
    messageQueue.add(() => {
        try {
            if (!req.body || typeof req.body !== 'object') {
                console.warn("⚠️ Received invalid webhook request body");
                performanceMonitor.logRequest(false);
                // Don't return res status here, let the outer promise handle it
                throw new Error('Invalid request body'); // Throw error for catch block
            }
            bot.processUpdate(req.body);
            performanceMonitor.logRequest(true);
             // If successful, resolve the promise implicitly (no return needed for success)
        } catch (error) {
            console.error("❌ Webhook processing error:", error);
            performanceMonitor.logRequest(false);
            // Re-throw the error to be caught by the queue's error handler if needed,
            // or handle the response directly if appropriate for webhook ACK
            throw error; // Let the queue handler log it, response is sent in app.js
        }
    });
     // Acknowledge receipt immediately to Telegram, regardless of queue processing
     res.sendStatus(200);
}

// Export the queue stats function if needed by health checks
export function getMessageQueueStats() {
    return {
        pending: messageQueue.size,
        active: messageQueue.pending,
    };
}
