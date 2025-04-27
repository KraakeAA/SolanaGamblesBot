// utils/index.js
import { bot } from '../services/telegram/bot.js'; // May cause circular dependency if bot needs utils. Refactor if needed.

// Checks if an error is likely retryable
export function isRetryableError(error) {
    // ... (copy the function content here)
     const msg = error?.message?.toLowerCase() || '';
     const code = error?.code?.toLowerCase() || '';
     // ... rest of function
     return false;
}

// Fetches user's display name
export async function getUserDisplayName(chat_id, user_id) {
    // ... (copy the function content here)
    try {
        // Ensure bot is imported correctly. May need dependency injection or passing bot instance.
        const chatMember = await bot.getChatMember(chat_id, user_id);
        // ... rest of function
    } catch (e) {
       // ... rest of function
    }
     return `User ${String(user_id).substring(0, 6)}...`; // Fallback
}

// Other general utilities can go here
