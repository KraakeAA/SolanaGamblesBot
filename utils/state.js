// utils/state.js

// User command cooldown
export const confirmCooldown = new Map(); // Map<userId, lastCommandTimestamp>
export const COOLDOWN_INTERVAL = 3000; // 3 seconds

// Cache for linked wallets
export const walletCache = new Map(); // Map<userId, { wallet: address, timestamp: cacheTimestamp }>
export const CACHE_TTL = 300000; // 5 minutes

// Cache of processed transaction signatures during this bot session
export const processedSignaturesThisSession = new Set(); // Set<signature>
export const MAX_PROCESSED_SIGNATURES = 10000; // Reset if too large

// --- Functions to manage state ---

export function checkCooldown(userId) {
    const now = Date.now();
    if (confirmCooldown.has(userId)) {
        const lastTime = confirmCooldown.get(userId);
        if (now - lastTime < COOLDOWN_INTERVAL) {
            return true; // Still on cooldown
        }
    }
    return false; // Not on cooldown
}

export function setCooldown(userId) {
    confirmCooldown.set(userId, Date.now());
}

// Optional: Function to periodically clean up cooldown map (called from index.js or handler)
export function cleanupCooldownMap() {
     const cutoff = Date.now() - 60000; // Remove entries older than 1 minute
     for (const [key, timestamp] of confirmCooldown.entries()) {
         if (timestamp < cutoff) {
             confirmCooldown.delete(key);
         }
     }
}

export function getWalletFromCache(userId) {
    const cacheKey = `wallet-${userId}`;
    if (walletCache.has(cacheKey)) {
        const { wallet, timestamp } = walletCache.get(cacheKey);
        if (Date.now() - timestamp < CACHE_TTL) {
            return wallet; // Return cached wallet if not expired
        } else {
            walletCache.delete(cacheKey); // Delete expired cache entry
        }
    }
    return undefined;
}

export function setWalletCache(userId, wallet) {
     const cacheKey = `wallet-${userId}`;
     walletCache.set(cacheKey, { wallet, timestamp: Date.now() });
}

export function isSignatureProcessed(signature) {
    return processedSignaturesThisSession.has(signature);
}

export function markSignatureProcessed(signature) {
    processedSignaturesThisSession.add(signature);
    if (processedSignaturesThisSession.size > MAX_PROCESSED_SIGNATURES) {
        console.log('Clearing processed signatures cache (reached max size)');
        processedSignaturesThisSession.clear();
    }
}
