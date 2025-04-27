// config/solana.js
import 'dotenv/config'; // Or load centrally
import { PublicKey } from '@solana/web3.js';

// Basic validation (could be enhanced in config/index.js)
if (!process.env.RPC_URL || !process.env.BOT_PRIVATE_KEY || !process.env.MAIN_WALLET_ADDRESS /* ... etc */) {
    // console.error("Missing critical Solana environment variables!");
    // process.exit(1); // Handled centrally now
}

export const RPC_URL = process.env.RPC_URL;
export const FEE_MARGIN = BigInt(process.env.FEE_MARGIN || '5000'); // Default here or centrally
export const PRIORITY_FEE_RATE = 0.0001; // Example rate

export const wallets = {
    coinflip: {
        privateKey: process.env.BOT_PRIVATE_KEY,
        address: process.env.MAIN_WALLET_ADDRESS,
        publicKey: new PublicKey(process.env.MAIN_WALLET_ADDRESS),
    },
    race: {
        privateKey: process.env.RACE_BOT_PRIVATE_KEY,
        address: process.env.RACE_WALLET_ADDRESS,
        publicKey: new PublicKey(process.env.RACE_WALLET_ADDRESS),
    },
};

export const MEMO_PROGRAM_IDS = [
    "Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo",
    "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr",
];

// You might also export Keypair instances here if needed globally, but careful with secret keys
// import bs58 from 'bs58';
// import { Keypair } from '@solana/web3.js';
// export const coinflipPayer = Keypair.fromSecretKey(bs58.decode(wallets.coinflip.privateKey));
// export const racePayer = Keypair.fromSecretKey(bs58.decode(wallets.race.privateKey));
