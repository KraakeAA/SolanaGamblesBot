// services/solana/utils.js
import { PublicKey, SystemProgram, ComputeBudgetProgram, AddressLookupTableAccount } from '@solana/web3.js';
import bs58 from 'bs58';
import * as crypto from 'crypto';
import { solanaConnection } from './connection.js'; // Import connection
import { config } from '../../config/index.js'; // Import config for wallets

const MEMO_PROGRAM_IDS = config.MEMO_PROGRAM_IDS;

// <<< START: Memo Handling System >>>

// generateMemoId
export function generateMemoId(prefix = 'BET') {
    // ... (copy function content)
}

// validateMemoChecksum
function validateMemoChecksum(hex, checksum) {
    // ... (copy function content)
}

// validateOriginalMemoFormat
export function validateOriginalMemoFormat(memo) {
     // ... (copy function content - uses validateMemoChecksum)
}

// normalizeMemo
export function normalizeMemo(rawMemo) {
    // ... (copy function content - uses validateMemoChecksum)
}

// findMemoInTx (IMPORTANT: Depends on solanaConnection for ALT lookups)
export async function findMemoInTx(tx) {
    // ... (copy function content - carefully check imports and solanaConnection usage)
     if (message.addressTableLookups && message.addressTableLookups.length > 0) {
         // ...
          const lookupTableAccount = await solanaConnection.getAddressLookupTable(lookupAccountKey); // Uses connection
         // ...
     }
     // ...
}

// extractMemoFromDescription
export function extractMemoFromDescription(tx) {
    // ... (copy function content - uses normalizeMemo)
}

// <<< END: Memo Handling System >>>


// --- Transaction Analysis ---

// analyzeTransactionAmounts
export function analyzeTransactionAmounts(tx, walletType) {
     // ... (copy function content - uses config.wallets)
     const targetAddress = walletType === 'coinflip'
         ? config.wallets.coinflip.address
         : config.wallets.race.address;
    // ... rest of function
}

// getPayerFromTransaction
export function getPayerFromTransaction(tx) {
    // ... (copy function content)
}

// debugInstruction (if needed)
export function debugInstruction(inst, accountKeys) {
    // ... (copy function content)
}
