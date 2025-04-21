# Solana Casino Telegram Bot

This is a Telegram-based Solana Casino bot that runs a 100% house edge coin flip game.

### Features
- Accepts SOL on Solana Mainnet
- Supports `/bet` command in Telegram
- Automated game logic (no payout)

---

### Requirements
- Node.js (v18+)
- A Telegram bot token from @BotFather
- Your funded Solana wallet

---

### Setup Instructions

1. Clone or upload this project to [https://railway.app](https://railway.app) (or use a VPS)
2. Rename `.env.example` to `.env`
3. Replace `YOUR_TELEGRAM_BOT_TOKEN_HERE` with your actual token
4. Run:
   ```
   npm install
   npm start
   ```

---

### Usage

In Telegram:
```
/bet 0.01 heads
```

User sends SOL to your wallet, bot checks it, flips a coin, and keeps the money even if they win.

---

**Wallet Address:** `9HL7W4XZJDX6br3ojjU6BLHp7oZVP3nCDKxQ21TNanQf`