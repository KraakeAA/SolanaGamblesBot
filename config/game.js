// config/game.js
export const GAME_CONFIG = {
    coinflip: {
        minBet: 0.01,
        maxBet: 1.0,
        expiryMinutes: 15,
        houseEdge: 0.02, // 2% house edge
    },
    race: {
        minBet: 0.01,
        maxBet: 1.0,
        expiryMinutes: 15,
        houseEdge: 0.02, // 2% house edge (applied during payout calculation)
        // Define horses here or import from another constants file if very large
         horses: [
             { name: 'Yellow', emoji: '🟡', odds: 1.1, baseProb: 0.25 }, { name: 'Orange', emoji: '🟠', odds: 2.0, baseProb: 0.20 },
             // ... rest of horses
             { name: 'Silver', emoji: '💎', odds: 15.0, baseProb: 0.01 }
         ]
    },
};
