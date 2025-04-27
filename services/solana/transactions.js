// services/solana/transactions.js
import { PublicKey, Keypair, Transaction, SystemProgram, ComputeBudgetProgram, sendAndConfirmTransaction, LAMPORTS_PER_SOL } from '@solana/web3.js';
import bs58 from 'bs58';
import { solanaConnection } from './connection.js';
import { config } from '../../config/index.js'; // For wallets and priority fee rate

export async function sendSol(recipientPublicKey, amountLamports, gameType) {
    // Select the correct private key based on the game/wallet being paid from
    const walletConfig = config.wallets[gameType]; // Use config.wallets
    if (!walletConfig || !walletConfig.privateKey) {
        console.error(`❌ Cannot send SOL: Missing config/private key for game type ${gameType}`);
        return { success: false, error: `Missing config/private key for ${gameType}` };
    }
    const privateKey = walletConfig.privateKey;

    // ... (copy the rest of the sendSol function content here)
    // ... ensure it uses solanaConnection for getLatestBlockhash and sendAndConfirmTransaction
    // ... ensure it uses config.PRIORITY_FEE_RATE
}
