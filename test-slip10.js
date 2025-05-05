import { mnemonicToSeedSync } from 'bip39';
import { SLIP10Node } from '@metamask/key-tree';

const mnemonic = 'abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about';
const seedBytes = new Uint8Array(mnemonicToSeedSync(mnemonic)); // use sync version for simplicity

(async () => {
  const root = await SLIP10Node.fromSeed(seedBytes, { curve: 'ed25519' });
  const child = await root.derive("slip10:m/44'/501'/0'/0'/0'");
  console.log('Public Key Bytes:', child.publicKeyBytes);
})();
