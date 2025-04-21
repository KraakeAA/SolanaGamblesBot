const express=require('express');const fs=require('fs');const path=require('path');
const app=express();const PORT=process.env.DASHBOARD_PORT||3000;
const LOG_PATH=path.resolve(__dirname,'data','bets.json');
app.set('view engine','ejs');app.use('/static',express.static(path.join(__dirname,'public')));
app.get('/',(req,res)=>{const bets=JSON.parse(fs.readFileSync(LOG_PATH));const ti=bets.length;
const tin=bets.reduce((s,b)=>s+b.amount,0);const tout=bets.reduce((s,b)=>s+b.payout,0);
res.render('index',{ totalBets:ti,totalIn:tin.toFixed(4),totalOut:tout.toFixed(4),
net:(tin-tout).toFixed(4),times:bets.map(b=>b.ts),amounts:bets.map(b=>b.amount)});});
app.listen(PORT,()=>console.log(`Dashboard on http://localhost:${PORT}`));