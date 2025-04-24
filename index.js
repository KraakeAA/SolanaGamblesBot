// Absolute minimal Express app for testing Railway deployment/healthcheck
const express = require('express');
const app = express();

// Health check endpoint
app.get('/', (req, res) => {
  console.log("Health check endpoint '/' hit!"); // Log when accessed
  res.status(200).send('OK'); // Explicitly send 200 status
});

// Start listening - Railway provides the PORT environment variable.
// Default to 8080 if not set locally.
const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
    console.log(`✅ Minimal healthcheck server listening on port ${PORT}`);
    console.log("Waiting for requests or signals...");
});

// Graceful shutdown handlers
process.on('SIGINT', () => {
  console.log("Received SIGINT. Shutting down server...");
  process.exit(0);
});
process.on('SIGTERM', () => {
  console.log("Received SIGTERM. Shutting down server...");
  process.exit(0);
});

// Catch unhandled promise rejections (good practice)
process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
  // Application specific logging, throwing an error, or other logic here
});

// Catch uncaught exceptions (good practice)
process.on('uncaughtException', (error) => {
  console.error('Uncaught Exception:', error);
  // Important: Hexit after logging uncaught exception as the application state is unknown
  process.exit(1);
});
