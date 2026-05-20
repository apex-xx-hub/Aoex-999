// ╔══════════════════════════════════════════╗
// ║       Abdu-xx Bot — index.js             ║
// ║       Heroku-ready entry point           ║
// ╚══════════════════════════════════════════╝
'use strict';

try { require('dotenv').config(); } catch (_) {}

// Raise limit once — listeners per socket per event
require('events').EventEmitter.defaultMaxListeners = 50;

const express     = require('express');
const bodyParser  = require('body-parser');
const compression = require('compression');
const path        = require('path');
const https       = require('https');
const http        = require('http');

const app  = express();
const PORT = process.env.PORT || 8002;

// ── Middleware ────────────────────────────────────────────────
app.use(compression());                                 // gzip all responses
app.use(bodyParser.json({ limit: '1mb' }));
app.use(bodyParser.urlencoded({ extended: true, limit: '1mb' }));
app.use(express.static(path.join(__dirname, 'public'), { maxAge: '1h' }));

// ── Routes ────────────────────────────────────────────────────
const botRouter = require('./bot');
app.use('/code', botRouter);

app.get('/pair',   (_req, res) => res.sendFile(path.join(__dirname, 'pair.html')));
app.get('/',       (_req, res) => res.sendFile(path.join(__dirname, 'main.html')));
app.get('/health', (_req, res) => res.json({
  status : 'ok',
  bot    : 'Abdu-xx Bot',
  uptime : process.uptime().toFixed(0) + 's',
  mem_mb : (process.memoryUsage().rss / 1024 / 1024).toFixed(1),
  time   : new Date().toISOString(),
}));

// ── Start ─────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`╔══════════════════════════════════════╗`);
  console.log(`║  Abdu-xx Bot — ONLINE  Port: ${PORT}  ║`);
  console.log(`╚══════════════════════════════════════╝`);
});

// ── Keep-alive (single interval, no duplicate in bot.js) ──────
if (process.env.APP_URL) {
  const pingUrl = process.env.APP_URL.trim().replace(/\/$/, '') + '/health';
  const mod = pingUrl.startsWith('https') ? https : http;
  setInterval(() => {
    mod.get(pingUrl, r => console.log('[keep-alive]', r.statusCode))
       .on('error', e => console.error('[keep-alive]', e.message));
  }, 14 * 60 * 1000);   // every 14 min — well under Heroku's 30-min sleep threshold
}

module.exports = app;