// services/database/schema.js
import { pool } from '../../config/index.js'; // Use the centrally exported pool

export async function initializeDatabase() {
    console.log("⚙️ Initializing Database schema...");
    let client;
    try {
        client = await pool.connect();

        // Bets Table
        await client.query(`
            CREATE TABLE IF NOT EXISTS bets (
                // ... (copy CREATE TABLE bets content here)
            );
        `);

        // Wallets Table
        await client.query(`
            CREATE TABLE IF NOT EXISTS wallets (
                 // ... (copy CREATE TABLE wallets content here)
            );
        `);

        // ALTER TABLE commands
        await client.query(`ALTER TABLE bets ADD COLUMN IF NOT EXISTS priority INT DEFAULT 0;`);
        await client.query(`ALTER TABLE bets ADD COLUMN IF NOT EXISTS fees_paid BIGINT;`);

        // Indexes
        await client.query(`CREATE INDEX IF NOT EXISTS idx_bets_status ON bets(status);`);
        // ... (copy all CREATE INDEX commands here)

        console.log("✅ Database schema initialized/verified.");
    } catch (err) {
        console.error("❌ Database initialization error:", err);
        throw err; // Re-throw
    } finally {
        if (client) client.release();
    }
}
