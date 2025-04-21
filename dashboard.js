const express = require('express');
const fs = require('fs');
const path = require('path');
const app = express();

const PORT = process.env.DASHBOARD_PORT || 3000;
const LOG_PATH = path.resolve(__dirname, 'data', 'bets.json');

app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));
app.use('/static', express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => {
  if (!fs.existsSync(LOG_PATH)) {
    return res.send('No bet history found.');
  }

  const bets = JSON.parse(fs.readFileSync(LOG_PATH));
  const totalBets = bets.length;
  const totalIn = bets.reduce((sum, b) => sum + b.amount, 0);
  const totalOut = bets.reduce((sum, b) => sum + b.payout, 0);

  res.render('index', {
    totalBets,
    totalIn: totalIn.toFixed(4),
    totalOut: totalOut.toFixed(4),
    net: (totalIn - totalOut).toFixed(4),
    times: bets.map(b => b.ts),
    amounts: bets.map(b => b.amount)
  });
});

app.listen(PORT, () => {
  console.log(`Dashboard running on http://localhost:${PORT}`);
});
