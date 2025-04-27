// services/solana/connection.js
import RateLimitedConnection from '../../lib/solana-connection.js'; // Adjust path if needed
import { config } from '../../config/index.js';

console.log("⚙️ Initializing scalable Solana connection...");

export const solanaConnection = new RateLimitedConnection(config.RPC_URL, {
    maxConcurrent: 3, // Initial value, maybe adjust later based on config/logic
    retryBaseDelay: 600,
    commitment: 'confirmed',
    httpHeaders: {
        'Content-Type': 'application/json',
        'solana-client': `SolanaGamblesBot/${config.version} (${config.isRailway ? 'railway' : 'local'})`
    },
    rateLimitCooloff: 10000,
    disableRetryOnRateLimit: false
});

// Optional: Add a function to update concurrency later
export function updateSolanaConcurrency(newConcurrency) {
     if (solanaConnection && solanaConnection.options) {
         console.log(`⚡ Adjusting Solana connection concurrency to ${newConcurrency}...`);
         solanaConnection.options.maxConcurrent = newConcurrency;
         console.log(`✅ Solana maxConcurrent adjusted to ${newConcurrency}`);
     }
}


console.log("✅ Scalable Solana connection initialized");
