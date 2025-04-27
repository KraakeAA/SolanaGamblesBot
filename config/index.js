// config/index.js
import 'dotenv/config';
import * as solanaConfig from './solana.js';
import * as gameConfig from './game.js';
import { pool as dbPool, testDbConnection } from './database.js'; // Import pool

// --- Central Environment Variable Checks ---
console.log("⏳ Checking environment variables...");
const BASE_REQUIRED_ENV_VARS = [
    'BOT_TOKEN',
    'DATABASE_URL',
    'BOT_PRIVATE_KEY', // Coinflip payouts
    'RACE_BOT_PRIVATE_KEY', // Race payouts
    'MAIN_WALLET_ADDRESS', // Coinflip deposits
    'RACE_WALLET_ADDRESS', // Race deposits
    'RPC_URL',
    // FEE_MARGIN has a default in solana.js
];

let REQUIRED_ENV_VARS = [...BASE_REQUIRED_ENV_VARS];

// Check for Railway-specific variables if deployed there
const IS_RAILWAY = !!process.env.RAILWAY_ENVIRONMENT;
const RAILWAY_PUBLIC_DOMAIN = process.env.RAILWAY_PUBLIC_DOMAIN;
if (IS_RAILWAY) {
    REQUIRED_ENV_VARS.push('RAILWAY_PUBLIC_DOMAIN');
}

let missingVars = false;
REQUIRED_ENV_VARS.forEach((key) => {
    if (!process.env[key]) {
        console.error(`❌ Environment variable ${key} is missing.`);
        missingVars = true;
    }
});

if (missingVars) {
    console.error("⚠️ Please set all required environment variables. Exiting.");
    process.exit(1);
}

console.log(`✅ All required environment variables are set.`);
console.log(`ℹ️ Using FEE_MARGIN: ${solanaConfig.FEE_MARGIN} lamports`);
if (IS_RAILWAY) {
    console.log(`ℹ️ Running in Railway environment. Public Domain: ${RAILWAY_PUBLIC_DOMAIN}`);
}

// --- Export all config ---
export const config = {
    env: process.env.NODE_ENV || 'development',
    isProduction: process.env.NODE_ENV === 'production',
    port: process.env.PORT || 3000,
    botToken: process.env.BOT_TOKEN,
    isRailway: IS_RAILWAY,
    railwayPublicDomain: RAILWAY_PUBLIC_DOMAIN,
    version: '2.0.9', // Keep version here or read from package.json

    ...solanaConfig, // Spread Solana config (RPC_URL, wallets, etc.)
    ...gameConfig,  // Spread game config (GAME_CONFIG)
};

// Export DB pool and test function separately for direct use
export const pool = dbPool;
export { testDbConnection };
