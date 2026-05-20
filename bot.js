// ╔══════════════════════════════════════════════════════════════╗
// ║                  Abdu-xx Bot — bot.js                        ║
// ║  Mongo · Socket · Handlers · Commands  (refactored v2)       ║
// ╚══════════════════════════════════════════════════════════════╝
'use strict';

// ── Lazy-loaded heavy modules (required only when the command runs) ──────────
// GoogleGenAI, WASticker, fluent-ffmpeg, ffmpeg-static are NOT loaded at startup.
// This alone saves ~80-120 MB RSS on boot.

const express  = require('express');
const fs       = require('fs-extra');
const path     = require('path');
const os       = require('os');
const crypto   = require('crypto');
const { exec } = require('child_process');
const router   = express.Router();
const pino     = require('pino');
const moment   = require('moment-timezone');
const axios    = require('axios');
const { MongoClient } = require('mongodb');

const {
  default: makeWASocket,
  useMultiFileAuthState,
  delay,
  getContentType,
  makeCacheableSignalKeyStore,
  jidNormalizedUser,
  downloadContentFromMessage,
} = require('baileys');

const config = require('./config');

// ══════════════════════════════════════════════════════════════
//  SECTION 1 — IN-MEMORY STORES
// ══════════════════════════════════════════════════════════════

const activeSockets      = new Map();  // sn → WASocket
const socketCreationTime = new Map();  // sn → Date.now()
const msgCache           = new Map();  // sn → Map<id, cachedMsg>
const settingsTracker    = new Map();  // sn → Map<stanzaId, {ts}>
const menuTracker        = new Map();  // sn → Map<stanzaId, {ts, type:'menu'}>
const songTracker        = new Map();  // sn → Map<stanzaId, {ts, type:'song', videoUrl, title}>
const configCache        = new Map();  // sn → { data, ts }
const credsSaveTimers    = new Map();  // sn → setTimeout handle
const pairingLocks       = new Set();  // sn

const CONFIG_TTL  = 30_000;   // 30 s config cache TTL
const MSG_MAX     = 400;      // max cached messages per session (was 500 — trim early)
const TRACKER_TTL = 600_000;  // 10 min tracker TTL

const numOnly    = s => (s || '').replace(/[^0-9]/g, '');
const getMsgCache    = sn => { if (!msgCache.has(sn))        msgCache.set(sn, new Map());        return msgCache.get(sn); };
const getTracker     = sn => { if (!settingsTracker.has(sn)) settingsTracker.set(sn, new Map()); return settingsTracker.get(sn); };
const getMenuTracker = sn => { if (!menuTracker.has(sn))     menuTracker.set(sn, new Map());     return menuTracker.get(sn); };
const getSongTracker = sn => { if (!songTracker.has(sn))     songTracker.set(sn, new Map());     return songTracker.get(sn); };

// ── Periodic GC — runs every 60 s to evict stale entries ─────────────────────
setInterval(() => {
  const now = Date.now();
  for (const [, tr] of settingsTracker) for (const [id, d] of tr) if (now - d.ts > TRACKER_TTL) tr.delete(id);
  for (const [, mr] of menuTracker)     for (const [id, d] of mr) if (now - d.ts > TRACKER_TTL) mr.delete(id);
  for (const [, sr] of songTracker)     for (const [id, d] of sr) if (now - d.ts > TRACKER_TTL) sr.delete(id);
  for (const [, ca] of msgCache)        for (const [id, d] of ca) if (now - d.cachedAt > 1_800_000) ca.delete(id);
  for (const [k, v] of configCache)     if (now - v.ts > CONFIG_TTL * 2) configCache.delete(k);
}, 60_000).unref();  // .unref() — won't keep process alive alone

// ══════════════════════════════════════════════════════════════
//  SECTION 2 — MONGODB
// ══════════════════════════════════════════════════════════════

let _mongo, _db, _ready = false;
let Col = {};

async function initMongo() {
  if (_ready && _mongo?.topology?.isConnected?.()) return;
  _mongo = new MongoClient(config.MONGO_URI, {
    maxPoolSize        : 5,    // cap connection pool — saves memory
    minPoolSize        : 1,
    serverSelectionTimeoutMS: 10_000,
    socketTimeoutMS    : 30_000,
    connectTimeoutMS   : 10_000,
  });
  await _mongo.connect();
  _db = _mongo.db(config.MONGO_DB);
  Col = {
    sessions   : _db.collection('sessions'),
    numbers    : _db.collection('numbers'),
    admins     : _db.collection('admins'),
    newsletter : _db.collection('newsletter_list'),
    nlReacts   : _db.collection('newsletter_reacts'),
    configs    : _db.collection('configs'),
    autoReplies: _db.collection('auto_replies'),
  };
  // Indexes (idempotent)
  await Promise.all([
    Col.sessions.createIndex({ number: 1 }, { unique: true }),
    Col.numbers.createIndex({ number: 1 }, { unique: true }),
    Col.newsletter.createIndex({ jid: 1 }, { unique: true }),
    Col.nlReacts.createIndex({ jid: 1 }, { unique: true }),
    Col.configs.createIndex({ number: 1 }, { unique: true }),
    Col.autoReplies.createIndex({ number: 1, trigger: 1 }, { unique: true }),
  ]);
  _ready = true;
  console.log('✅ MongoDB connected →', config.MONGO_DB);
}

// ── Sessions ──────────────────────────────
async function saveCreds(sn, creds, keys = null) {
  try {
    await initMongo();
    await Col.sessions.updateOne({ number: sn }, { $set: { number: sn, creds, keys, updatedAt: new Date() } }, { upsert: true });
  } catch (e) { console.error('saveCreds:', e.message); }
}
async function loadCreds(sn) {
  try { await initMongo(); return await Col.sessions.findOne({ number: sn }) || null; } catch (_) { return null; }
}
async function removeCreds(sn) {
  try { await initMongo(); await Col.sessions.deleteOne({ number: sn }); } catch (_) {}
}

// ── Numbers ───────────────────────────────
async function addNumber(sn)    { try { await initMongo(); await Col.numbers.updateOne({ number: sn }, { $set: { number: sn } }, { upsert: true }); } catch (_) {} }
async function removeNumber(sn) { try { await initMongo(); await Col.numbers.deleteOne({ number: sn }); } catch (_) {} }
async function getAllNumbers()   { try { await initMongo(); return (await Col.numbers.find({}).toArray()).map(d => d.number); } catch (_) { return []; } }

// ── Admins ────────────────────────────────
async function getAdmins()      { try { await initMongo(); return (await Col.admins.find({}).toArray()).map(d => d.jid || d.number).filter(Boolean); } catch (_) { return []; } }
async function addAdmin(jid)    { try { await initMongo(); await Col.admins.updateOne({ jid }, { $set: { jid } }, { upsert: true }); } catch (_) {} }
async function removeAdmin(jid) { try { await initMongo(); await Col.admins.deleteOne({ jid }); } catch (_) {} }

// ── Newsletters ───────────────────────────
async function addNewsletter(jid, emojis = [])  { await initMongo(); await Col.newsletter.updateOne({ jid }, { $set: { jid, emojis: Array.isArray(emojis) ? emojis : [], addedAt: new Date() } }, { upsert: true }); }
async function removeNewsletter(jid)            { await initMongo(); await Col.newsletter.deleteOne({ jid }); }
async function listNewsletters()                { try { await initMongo(); return (await Col.newsletter.find({}).toArray()).map(d => ({ jid: d.jid, emojis: d.emojis || [] })); } catch (_) { return []; } }
async function listNlReacts()                   { try { await initMongo(); return (await Col.nlReacts.find({}).toArray()).map(d => ({ jid: d.jid, emojis: d.emojis || [] })); } catch (_) { return []; } }
async function logNlReaction(jid, msgId, emoji, sn) { try { await initMongo(); await _db.collection('newsletter_reactions_log').insertOne({ jid, msgId, emoji, sn, ts: new Date() }); } catch (_) {} }

// ── User Config ───────────────────────────
async function setConfig(sn, conf) {
  try {
    await initMongo();
    await Col.configs.updateOne({ number: sn }, { $set: { number: sn, config: conf, updatedAt: new Date() } }, { upsert: true });
    configCache.delete(sn);
  } catch (e) { console.error('setConfig:', e.message); }
}
async function loadConfig(sn) {
  try { await initMongo(); const d = await Col.configs.findOne({ number: sn }); return d?.config || null; } catch (_) { return null; }
}
async function getCachedConfig(sn) {
  const hit = configCache.get(sn);
  if (hit && Date.now() - hit.ts < CONFIG_TTL) return hit.data;
  const fresh = (await loadConfig(sn)) || {};
  configCache.set(sn, { data: fresh, ts: Date.now() });
  return fresh;
}

// ── Auto Replies ──────────────────────────
async function addAutoReply(sn, trigger, reply, type = 'text', imageUrl = '', audioUrl = '') {
  await initMongo();
  const t = trigger.toLowerCase().trim();
  await Col.autoReplies.updateOne({ number: sn, trigger: t }, { $set: { number: sn, trigger: t, reply, type, imageUrl, audioUrl, updatedAt: new Date() } }, { upsert: true });
}
async function removeAutoReply(sn, trigger) {
  await initMongo();
  const r = await Col.autoReplies.deleteOne({ number: sn, trigger: trigger.toLowerCase().trim() });
  return r.deletedCount > 0;
}
async function getAutoReplies(sn) {
  await initMongo(); return Col.autoReplies.find({ number: sn }).toArray();
}
async function findAutoReply(sn, text) {
  await initMongo(); return Col.autoReplies.findOne({ number: sn, trigger: text.toLowerCase().trim() });
}
async function removeImageReply(sn, trigger) {
  await initMongo();
  const r = await Col.autoReplies.deleteOne({ number: sn, trigger: trigger.toLowerCase().trim(), type: 'image' });
  return r.deletedCount > 0;
}

// ══════════════════════════════════════════════════════════════
//  SECTION 3 — UTILITIES
// ══════════════════════════════════════════════════════════════

const fmt    = (title, body, footer) => `*${title}*\n\n${body}\n\n> *${footer}*`;
const slTime = () => moment().tz('Asia/Colombo').format('YYYY-MM-DD HH:mm:ss');
const isDev  = sn => !!(config.DEVELOPERS?.[numOnly(sn)]);
const devEmoji = sn => config.DEVELOPERS?.[numOnly(sn)] || null;

function fakeQuote(botName) {
  return {
    key     : { remoteJid: 'status@broadcast', participant: '0@s.whatsapp.net', fromMe: false, id: 'AX_' + crypto.randomBytes(4).toString('hex') },
    message : { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD\nVERSION:3.0\nFN:${botName}\nORG:Abdu-xx;\nTEL;type=CELL;waid=0:+0\nEND:VCARD` } },
  };
}

function extractText(message) {
  if (!message) return '';
  const t = getContentType(message);
  if (!t) return '';
  const map = {
    conversation        : () => message.conversation || '',
    extendedTextMessage : () => message.extendedTextMessage?.text || '',
    imageMessage        : () => message.imageMessage?.caption || '',
    videoMessage        : () => message.videoMessage?.caption || '',
    documentMessage     : () => message.documentMessage?.caption || '',
    viewOnceMessage     : () => message.viewOnceMessage?.message?.imageMessage?.caption || message.viewOnceMessage?.message?.videoMessage?.caption || '',
  };
  return map[t]?.() ?? '';
}

function typeLabel(message) {
  if (!message) return 'Unknown';
  const t = getContentType(message);
  return { conversation:'Text', extendedTextMessage:'Text', imageMessage:'Image', videoMessage:'Video', audioMessage:'Audio', documentMessage:'Document', stickerMessage:'Sticker', contactMessage:'Contact', locationMessage:'Location', viewOnceMessage:'View Once' }[t] || (t || 'Unknown');
}

const MEDIA_TYPES = ['imageMessage','videoMessage','audioMessage','documentMessage','stickerMessage'];

async function downloadMedia(message) {
  if (!message) return null;
  const t = getContentType(message);
  if (!MEDIA_TYPES.includes(t)) return null;
  try {
    const m      = message[t];
    const stream = await downloadContentFromMessage(m, t.replace(/Message$/i,'').toLowerCase());
    const chunks = [];
    for await (const c of stream) chunks.push(c);
    return { buffer: Buffer.concat(chunks), mime: m.mimetype || '', type: t, caption: m.caption || '', fileName: m.fileName || '', ptt: m.ptt || false };
  } catch (_) { return null; }
}

async function downloadQuotedMedia(quoted) {
  if (!quoted) return null;
  const qt = MEDIA_TYPES.find(t => quoted[t]);
  if (!qt) return null;
  try {
    const stream = await downloadContentFromMessage(quoted[qt], qt.replace(/Message$/i,'').toLowerCase());
    const chunks = [];
    for await (const c of stream) chunks.push(c);
    return { buffer: Buffer.concat(chunks), mime: quoted[qt].mimetype || '', caption: quoted[qt].caption || '', fileName: quoted[qt].fileName || '', ptt: quoted[qt].ptt || false };
  } catch (_) { return null; }
}

async function joinGroup(socket) {
  const m = (config.GROUP_INVITE_LINK || '').match(/chat\.whatsapp\.com\/([a-zA-Z0-9]+)/);
  if (!m) return { status: 'failed', error: 'No invite link' };
  let r = config.MAX_RETRIES;
  while (r-- > 0) {
    try { const res = await socket.groupAcceptInvite(m[1]); if (res?.gid) return { status: 'success', gid: res.gid }; throw new Error('No gid'); }
    catch (e) { if (r === 0) return { status: 'failed', error: e.message }; await delay(2000); }
  }
}

function debouncedCredsSave(sn, saveLocalCreds, getState, sessionPath) {
  if (credsSaveTimers.has(sn)) clearTimeout(credsSaveTimers.get(sn));
  credsSaveTimers.set(sn, setTimeout(async () => {
    credsSaveTimers.delete(sn);
    try {
      await saveLocalCreds();
      const cp = path.join(sessionPath, 'creds.json');
      if (!fs.existsSync(cp) || fs.statSync(cp).size === 0) return;
      const raw = (await fs.readFile(cp, 'utf8')).trim();
      if (!raw || raw === '{}' || raw === 'null') return;
      let co; try { co = JSON.parse(raw); } catch (_) { return; }
      const liveState = getState();
      if (co) await saveCreds(sn, co, liveState.keys || null);
    } catch (e) { console.error('[creds save]', e.message); }
  }, 2000));
}

// ══════════════════════════════════════════════════════════════
//  SECTION 4 — SETTINGS MESSAGE
// ══════════════════════════════════════════════════════════════

async function sendSettings(socket, to, sn, uc) {
  const bn  = uc.botName || config.BOT_NAME;
  const logo = uc.logo || config.RCD_IMAGE_PATH;
  const fq  = fakeQuote(bn);
  const ch  = (v, c) => v === c ? ' ✅' : '';

  const cw   = uc.WORK_TYPE || 'private';
  const cfp  = uc.FAKE_PRESENCE || 'off';
  const cpr  = uc.PRESENCE || 'available';
  const csv  = uc.AUTO_VIEW_STATUS === 'true' ? (uc.AUTO_LIKE_STATUS === 'true' ? 'view_like' : 'view') : 'off';
  const crm  = uc.AUTO_READ_MESSAGE || 'off';
  const cac  = uc.ANTI_CALL || 'off';
  const cadm = uc.ANTI_DELETE_MSG || 'disable';
  const cads = uc.ANTI_DELETE_STATUS || 'disable';
  const cpe  = uc.PREFIX_ENABLED || 'on';
  const car  = uc.AUTO_REPLY || 'disable';

  const cap =
    `╭─「 *ꜱᴏ x ꜱᴇᴛᴛɪɴɢꜱ ᴘᴀɴᴇʟ* 」\n┆\n` +
    `┆ \`❝ 1. WORK TYPE ❞\`\n` +
    `┆  1.1 Public${ch(cw,'public')}  1.2 Inbox${ch(cw,'inbox')}\n` +
    `┆  1.3 Groups${ch(cw,'groups')}  1.4 Group Admins${ch(cw,'group_admins')}\n` +
    `┆  1.5 Channel${ch(cw,'channel')}  1.6 Private${ch(cw,'private')}\n┆\n` +
    `┆ \`❝ 2. FAKE PRESENCE ❞\`\n` +
    `┆  2.1 Typing${ch(cfp,'typing')}  2.2 Recording${ch(cfp,'recording')}  2.3 Off${ch(cfp,'off')}\n┆\n` +
    `┆ \`❝ 3. PRESENCE ❞\`\n` +
    `┆  3.1 Online${ch(cpr,'available')}  3.2 Offline${ch(cpr,'unavailable')}\n┆\n` +
    `┆ \`❝ 4. STATUS VIEW ❞\`\n` +
    `┆  4.1 View${ch(csv,'view')}  4.2 View+Like${ch(csv,'view_like')}  4.3 Off${ch(csv,'off')}\n┆\n` +
    `┆ \`❝ 5. AUTO READ ❞\`\n` +
    `┆  5.1 All${ch(crm,'all')}  5.2 Commands${ch(crm,'cmd')}  5.3 Off${ch(crm,'off')}\n┆\n` +
    `┆ \`❝ 6. ANTI CALL ❞\`\n` +
    `┆  6.1 Reject${ch(cac,'on')}  6.2 Reject+Msg${ch(cac,'reject_msg')}  6.3 Off${ch(cac,'off')}\n┆\n` +
    `┆ \`❝ 7. ANTI DELETE MSG ❞\`\n` +
    `┆  7.1 Enable${ch(cadm,'enable')}  7.2 Disable${ch(cadm,'disable')}\n┆\n` +
    `┆ \`❝ 8. ANTI DELETE STATUS ❞\`\n` +
    `┆  8.1 Enable${ch(cads,'enable')}  8.2 Disable${ch(cads,'disable')}\n┆\n` +
    `┆ \`❝ 9. PREFIX ❞\`\n` +
    `┆  9.1 On${ch(cpe,'on')}  9.2 Off${ch(cpe,'off')}\n┆\n` +
    `┆ \`❝ 10. AUTO REPLY ❞\`\n` +
    `┆  10.1 Enable${ch(car,'enable')}  10.2 Disable${ch(car,'disable')}\n` +
    `╰──────────────────────\n` +
    `_Reply with a number (e.g. 1.1) to change. Reply "0" to close._\n\n> *${bn}*`;

  try {
    return await socket.sendMessage(to, {
      image: { url: logo }, caption: cap,
      contextInfo: { mentionedJid: [to], forwardingScore: 999, isForwarded: true,
        externalAdReply: { title: `${bn} — Settings`, body: 'Reply with number', thumbnailUrl: logo, sourceUrl: config.CHANNEL_LINK, mediaType: 1, renderLargerThumbnail: true } }
    }, { quoted: fq });
  } catch (_) {
    try { return await socket.sendMessage(to, { text: cap }, { quoted: fq }); } catch (_2) { return null; }
  }
}

// ══════════════════════════════════════════════════════════════
//  SECTION 5 — COMMAND HANDLER
// ══════════════════════════════════════════════════════════════

async function handleCommand(command, args, ctx) {
  const {
    socket, msg, sender, pushname, botName, thumbUrl, fq, prefix,
    uc, sn, senderNumber, nowsender, isOwner, isGroup, isDevUser, isSessionOwner,
  } = ctx;

  // ── Arabian mystery header ──────────────────────────────────────────────────
  const ARABIAN_THUMB = 'https://files.catbox.moe/8hd3b3.jpg';
  const ARABIAN_TITLE = 'عبد الكلام • مجهول';   // عبد الكلام • مجهول
  const ARABIAN_SUB   = '⚔️ Unknown Network ⚔️';

  const arabianCtx = () => ({
    forwardingScore: 999,
    isForwarded: true,
    forwardedNewsletterMessageInfo: {
      newsletterJid  : config.NEWSLETTER_JID || '0@newsletter',
      newsletterName : ARABIAN_TITLE,
      serverMessageId: 143,
    },
    externalAdReply: {
      title                : ARABIAN_TITLE,
      body                 : ARABIAN_SUB,
      thumbnailUrl         : ARABIAN_THUMB,
      sourceUrl            : config.CHANNEL_LINK || 'https://whatsapp.com',
      mediaType            : 1,
      renderLargerThumbnail: true,
    },
  });

  const chCtx = (title, body = 'So x') => arabianCtx();

  const reply   = text => socket.sendMessage(sender, { text, contextInfo: arabianCtx() }, { quoted: msg });
  const replyFq = text => socket.sendMessage(sender, { text, contextInfo: arabianCtx() }, { quoted: fq });

  function getUptime() {
    const up = Math.floor((Date.now() - (socketCreationTime.get(sn) || Date.now())) / 1000);
    return `${Math.floor(up/3600)}h ${Math.floor((up%3600)/60)}m ${up%60}s`;
  }

  function greeting() {
    const h = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Colombo' })).getHours();
    return h < 12 ? 'Good Morning ☀️' : h < 17 ? 'Good Afternoon 🌤️' : h < 20 ? 'Good Evening 🌆' : 'Good Night 🌙';
  }

  switch (command) {

    // ════════════ ALIVE ════════════
    case 'alive': {
      try { await socket.sendMessage(sender, { react: { text: '🤖', key: msg.key } }); } catch (_) {}
      const now = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Colombo' }));
      const cap = `*╭─「 ꜱᴏ x ᴏɴʟɪɴᴇ 」*\n` +
        `*┃* 👋 ${greeting()}, *${pushname}*\n*┃*\n` +
        `*┃* 🤖 *Bot:* ${botName}\n` +
        `*┃* 👑 *Owner:* ${uc.ownerName || config.OWNER_NAME}\n` +
        `*┃* ⏱️ *Uptime:* ${getUptime()}\n` +
        `*┃* 📅 *Date:* ${now.toLocaleDateString('en-US',{year:'numeric',month:'short',day:'numeric'})}\n` +
        `*┃* 🕐 *Time:* ${now.toLocaleTimeString('en-US',{hour:'2-digit',minute:'2-digit'})}\n` +
        `*┃* 🔑 *Prefix:* ${prefix}\n` +
        `*╰──────────────────*\n\n> *${botName}*`;
      await socket.sendMessage(sender, { image: { url: thumbUrl }, caption: cap, contextInfo: chCtx(botName, 'ALIVE') }, { quoted: fq });
      break;
    }

    // ════════════ PING ════════════
    case 'ping': {
      try { await socket.sendMessage(sender, { react: { text: '🏓', key: msg.key } }); } catch (_) {}
      const start = Date.now();
      const pong  = await reply('_pinging..._');
      const ms    = Date.now() - start;
      try { if (pong?.key) await socket.sendMessage(sender, { delete: pong.key }); } catch (_) {}
      await replyFq(
        `*╭─「 Ping 」*\n` +
        `*┃* 🏓 *Pong!*\n` +
        `*┃* ⚡ *Speed:* ${ms}ms\n` +
        `*┃* ⏱️ *Uptime:* ${getUptime()}\n` +
        `*╰──────────────────*\n\n> *${botName}*`
      );
      break;
    }

    // ════════════ MENU ════════════
    case 'menu':
    case 'help': {
      try { await socket.sendMessage(sender, { react: { text: '🗂️', key: msg.key } }); } catch (_) {}
      const now2 = new Date(new Date().toLocaleString('en-US', { timeZone: 'Asia/Colombo' }));
      const mainCap =
        `👋 Hey *${pushname}!*\n` +
        `⏱️ Uptime: *${getUptime()}*\n` +
        `📅 ${now2.toLocaleDateString('en-US',{weekday:'short',month:'short',day:'numeric'})}  🕐 ${now2.toLocaleTimeString('en-US',{hour:'2-digit',minute:'2-digit'})}\n\n` +
        ` 📂  *SELECT CATEGORY*\n\n` +
        `  *➀*  ⚙️  General & System\n` +
        `  *➁*  🎵  Download\n` +
        `  *➂*  🔍  Search\n` +
        `  *➃*  🛠️  Tools\n` +
        `  *➄*  👥  Group\n` +
        `  *➅*  🤖  AI\n` +
        `  *➆*  ⚙️  Settings\n\n` +
        `━━━━━━━━━━━━━━━━━━━━━━\n` +
        `_Reply with a number to open that menu_\n_Reply *0* to close_\n\n> *${botName}*`;
      const mtr    = getMenuTracker(sn);
      const sentMenu = await socket.sendMessage(sender, { image: { url: thumbUrl }, caption: mainCap, contextInfo: chCtx(botName, 'MENU') }, { quoted: fq });
      if (sentMenu?.key?.id) mtr.set(sentMenu.key.id, { ts: Date.now(), type: 'main' });
      break;
    }

    // ── Sub-menu (called internally) ──────────────────────────
    case '__submenu__': {
      const cat     = args[0];
      const mtr     = getMenuTracker(sn);
      const subMenus = {
        '1': { emoji: '⚙️', title: 'General & System', cap:
          `╭─「 ⚙️ *General & System* 」\n┃\n` +
          `┃  ${prefix}alive   ➜  Check bot status\n┃  ${prefix}ping    ➜  Response speed\n` +
          `┃  ${prefix}system  ➜  Server stats\n┃  ${prefix}owner   ➜  Owner info\n` +
          `┃  ${prefix}jid     ➜  Your WhatsApp ID\n┃  ${prefix}active  ➜  Active sessions\n` +
          `┃  ${prefix}report  ➜  Send feedback\n┃  ${prefix}bc      ➜  Broadcast (owner)\n` +
          `┃  ${prefix}deleteme ➜ Delete your session\n┃\n` +
          `╰──────────────────────\n_Reply *0* to close  •  *00* for main menu_\n\n> *${botName}*` },
        '2': { emoji: '🎵', title: 'Download', cap:
          `╭─「 🎵 *Download* 」\n┃\n` +
          `┃  ${prefix}song    ➜  Search & download audio\n┃  ${prefix}video   ➜  YouTube video download\n` +
          `┃  ${prefix}tt      ➜  TikTok download\n┃  ${prefix}fb      ➜  Facebook video\n┃\n` +
          `╰──────────────────────\n_Reply *0* to close  •  *00* for main menu_\n\n> *${botName}*` },
        '3': { emoji: '🔍', title: 'Search', cap:
          `╭─「 🔍 *Search* 」\n┃\n` +
          `┃  ${prefix}ytsearch  ➜  YouTube search\n┃  ${prefix}gimg      ➜  Image search\n` +
          `┃  ${prefix}npm       ➜  NPM package info\n┃  ${prefix}weather   ➜  Weather info\n┃\n` +
          `╰──────────────────────\n_Reply *0* to close  •  *00* for main menu_\n\n> *${botName}*` },
        '4': { emoji: '🛠️', title: 'Tools', cap:
          `╭─「 🛠️ *Tools* 」\n┃\n` +
          `┃  ${prefix}sticker  ➜  Image → sticker\n┃  ${prefix}save     ➜  Save status media\n` +
          `┃  ${prefix}vv       ➜  View once bypass\n┃  ${prefix}getdp    ➜  Get profile photo\n` +
          `┃  ${prefix}bio      ➜  Set your bio\n┃  ${prefix}terminal ➜  Run shell command\n┃\n` +
          `╰──────────────────────\n_Reply *0* to close  •  *00* for main menu_\n\n> *${botName}*` },
        '5': { emoji: '👥', title: 'Group', cap:
          `╭─「 👥 *Group* 」\n┃\n┃ 👑 *Admin Commands:*\n` +
          `┃  ${prefix}promote  ➜  Make user admin\n┃  ${prefix}demote   ➜  Remove admin\n` +
          `┃  ${prefix}tagall   ➜  Tag all members\n┃  ${prefix}hidetag  ➜  Silent tag all\n` +
          `┃  ${prefix}tagadmin ➜  Tag all admins\n┃\n┃ ⚙️ *Group Settings:*\n` +
          `┃  ${prefix}add      ➜  Add member\n┃  ${prefix}kick     ➜  Remove member\n` +
          `┃  ${prefix}lockgroup ➜ Admins only mode\n┃  ${prefix}unlockgroup ➜ Everyone can send\n` +
          `┃  ${prefix}mute <1h/6h/1d/7d> ➜ Temporary mute\n┃  ${prefix}unmute   ➜  Unmute group\n┃\n` +
          `┃ 📝 *Group Info:*\n` +
          `┃  ${prefix}groupinfo ➜ Full group details\n┃  ${prefix}setname <name> ➜ Change group name\n` +
          `┃  ${prefix}setdesc <text> ➜ Change description\n┃  ${prefix}seticon  ➜  Change group icon\n` +
          `┃  ${prefix}linkgroup ➜ Get invite link\n┃  ${prefix}revokelink ➜ Reset invite link\n` +
          `┃  ${prefix}leave    ➜  Bot leaves group\n┃\n` +
          `╰──────────────────────\n_Reply *0* to close  •  *00* for main menu_\n\n> *${botName}*` },
        '6': { emoji: '🤖', title: 'AI', cap:
          `╭─「 🤖 *AI* 」\n┃\n┃  ${prefix}ai <q>   ➜  Ask Gemini AI\n┃  ${prefix}ask <q>  ➜  Same as ai\n┃\n` +
          `┃  *Example:*\n┃  ${prefix}ai What is gravity?\n┃\n` +
          `╰──────────────────────\n_Reply *0* to close  •  *00* for main menu_\n\n> *${botName}*` },
        '7': { emoji: '⚙️', title: 'Settings', cap:
          `╭─「 ⚙️ *Settings* 」\n┃\n┃  ${prefix}setting    ➜  Open settings panel\n` +
          `┃  ${prefix}mybot      ➜  Customize bot info\n┃  ${prefix}reset      ➜  Reset all settings\n` +
          `┃  ${prefix}wtype      ➜  Set work type\n┃\n┃ *📢 Newsletter:*\n` +
          `┃  ${prefix}cfn <jid> | emoji ➜ Follow channel\n┃  ${prefix}unfollow <jid> ➜ Unfollow\n` +
          `┃  ${prefix}chr <jid/id>,<emoji> ➜ React to post\n┃\n┃ *👑 Admin Management:*\n` +
          `┃  ${prefix}addadmin <number> ➜ Add admin\n┃  ${prefix}deladmin <number> ➜ Remove admin\n` +
          `┃  ${prefix}listadmin ➜ List all admins\n┃\n┃ *Auto Reply:*\n` +
          `┃  ${prefix}addreply   ➜  Add text reply\n┃  ${prefix}addimgreply ➜ Add image reply\n` +
          `┃  ${prefix}delreply   ➜  Delete reply\n┃  ${prefix}listreply  ➜  List all replies\n┃\n` +
          `╰──────────────────────\n_Reply *0* to close  •  *00* for main menu_\n\n> *${botName}*` },
      };
      const sub = subMenus[cat];
      if (!sub) break;
      try { await socket.sendMessage(sender, { react: { text: sub.emoji, key: msg.key } }); } catch (_) {}
      const sentSub = await socket.sendMessage(sender, {
        image: { url: thumbUrl }, caption: sub.cap, contextInfo: chCtx(`${sub.emoji} ${sub.title}`, 'MENU'),
      }, { quoted: fq });
      if (sentSub?.key?.id) mtr.set(sentSub.key.id, { ts: Date.now(), type: 'sub', cat });
      break;
    }


    // ════════════ SYSTEM ════════════
    case 'system':
    case 'sys': {
      try { await socket.sendMessage(sender, { react: { text: '💻', key: msg.key } }); } catch (_) {}
      const mem  = process.memoryUsage();
      const toMB = b => (b / 1024 / 1024).toFixed(1);
      await socket.sendMessage(sender, { text:
        `*╭─「 System Info 」*\n` +
        `*┃* 🖥️ *Platform:* ${os.platform()} ${os.arch()}\n` +
        `*┃* 💾 *RAM Used:* ${toMB(mem.rss)} MB\n` +
        `*┃* 📦 *Heap:* ${toMB(mem.heapUsed)}/${toMB(mem.heapTotal)} MB\n` +
        `*┃* ⚙️ *Node:* ${process.version}\n` +
        `*┃* ⏱️ *Uptime:* ${getUptime()}\n` +
        `*┃* 📡 *Sessions:* ${activeSockets.size}\n` +
        `*╰──────────────────*\n\n> *${botName}*`
      }, { quoted: fq });
      break;
    }

    // ════════════ OWNER ════════════
    case 'owner': {
      await socket.sendMessage(sender, { text:
        `*╭─「 Owner Info 」*\n` +
        `*┃* 👑 *Name:* ${uc.ownerName || config.OWNER_NAME}\n` +
        `*┃* 📞 *Number:* +${config.OWNER_NUMBER}\n` +
        `${uc.ownerDetails ? `*┃* 📝 *Info:* ${uc.ownerDetails}\n` : ''}` +
        `*╰──────────────────*\n\n> *${botName}*`
      }, { quoted: fq });
      break;
    }

    // ════════════ JID ════════════
    case 'jid': {
      await replyFq(`*Your JID:*\n\`${sender}\`\n*Participant:*\n\`${nowsender}\``);
      break;
    }

    // ════════════ AI ════════════
    case 'ai':
    case 'ask':
    case 'gemini': {
      const q = args.join(' ').trim();
      if (!q) return replyFq(`*Usage:* ${prefix}ai <question>`);
      try { await socket.sendMessage(sender, { react: { text: '🤔', key: msg.key } }); } catch (_) {}
      try {
        const { GoogleGenAI } = require('@google/genai');
        const ai  = new GoogleGenAI({ apiKey: process.env.GEMINI_API_KEY || '' });
        const res = await ai.models.generateContent({ model: 'gemini-2.0-flash', contents: q });
        await socket.sendMessage(sender, { text: `*🤖 Abdu-xx AI*\n\n${res.text || 'No response.'}\n\n> *${botName}*` }, { quoted: msg });
      } catch (e) { await replyFq(`*AI Error:* ${e.message}`); }
      break;
    }

    // ════════════ STICKER ════════════
    case 'sticker':
    case 'stiker':
    case 's': {
      try { await socket.sendMessage(sender, { react: { text: '🎨', key: msg.key } }); } catch (_) {}
      const quoted = msg.message?.extendedTextMessage?.contextInfo?.quotedMessage;
      const imgBuf = quoted?.imageMessage ? (await downloadQuotedMedia(quoted))?.buffer : null;
      if (!imgBuf) return replyFq(`Reply to an image with *${prefix}sticker*`);
      try {
        const { default: WASticker, StickerTypes } = require('wa-sticker-formatter');
        const sticker = new WASticker(imgBuf, { pack: botName, author: 'Abdul kalam', type: StickerTypes.FULL, categories: ['🤩'], id: '12345', quality: 50 });
        await socket.sendMessage(sender, { sticker: await sticker.toBuffer() }, { quoted: msg });
      } catch (e) { await replyFq(`Sticker creation failed: ${e.message}`); }
      break;
    }

    // ════════════ SONG ════════════
    case 'song':
    case 'mp3': {
      try { await socket.sendMessage(sender, { react: { text: '🎵', key: msg.key } }); } catch (_) {}
      const query = args.join(' ').trim();
      if (!query) return replyFq(`*╭──「 🎵 SONG DOWNLOADER 」*\n*┃*\n*┃ 📌 ${prefix}song <title or URL>*\n*┃ 📌 Example: ${prefix}song Faded Alan Walker*\n*╰──────────────────*\n\n> *${botName}*`);

      const pm = await socket.sendMessage(sender, { text: `⏳ *Searching for "${query}"...*` }, { quoted: fq });
      try {
        const yts    = require('yt-search');
        const search = await yts(query);
        if (!search?.videos?.length) {
          if (pm) await socket.sendMessage(sender, { delete: pm.key });
          return replyFq(`*❌ No results found for "${query}"*`);
        }
        const video = search.videos[0];
        if (pm) await socket.sendMessage(sender, { delete: pm.key });

        const caption =
          `*╭──「 🎵 SONG INFO 」*\n*┃*\n` +
          `*┃ 🎵 Title:* ${video.title}\n` +
          `*┃ 👤 Channel:* ${video.author?.name || 'Unknown'}\n` +
          `*┃ ⏱️ Duration:* ${video.timestamp}\n` +
          `*┃ 👁️ Views:* ${video.views?.toLocaleString() || 'N/A'}\n` +
          `*┃ 📅 Uploaded:* ${video.ago || 'N/A'}\n*┃*\n┃ ━━━━━━━━━━━━━━━━\n*┃*\n` +
          `*┃ 📌 Reply with a number:*\n*┃*\n*┃ 1️⃣ → Audio MP3*\n*┃ 2️⃣ → Voice Note*\n*┃*\n*┃ 0️⃣ → Cancel*\n` +
          `*╰──────────────────*\n\n> *${botName}*`;

        const sTracker = getSongTracker(sn);
        const sentMsg  = await socket.sendMessage(sender, { image: { url: video.thumbnail }, caption }, { quoted: fq });
        if (sentMsg?.key?.id) sTracker.set(sentMsg.key.id, { ts: Date.now(), type: 'song', videoUrl: video.url, title: video.title });
      } catch (e) {
        if (pm) await socket.sendMessage(sender, { delete: pm.key });
        await replyFq(`*❌ Failed to search:* ${e.message}`);
      }
      break;
    }

    // ════════════ VIDEO ════════════
    case 'video':
    case 'ytmp4':
    case 'vid': {
      try { await socket.sendMessage(sender, { react: { text: '🎬', key: msg.key } }); } catch (_) {}
      const query = args.join(' ').trim();
      if (!query) return replyFq(`*Usage:* ${prefix}video <title or URL>`);
      const pm = await socket.sendMessage(sender, { text: `⏳ *Searching for "${query}"...*` }, { quoted: fq });
      try {
        const yts  = require('yt-search');
        const ytRx = /^(https?:\/\/)?(www\.)?(youtube\.com|youtu\.be|m\.youtube\.com)\/.+/;
        let videoUrl = '', videoData = null;

        if (ytRx.test(query)) {
          let videoId = '';
          if (query.includes('youtu.be/'))  videoId = query.split('youtu.be/')[1]?.split(/[?&#]/)[0];
          else if (query.includes('watch?v=')) videoId = query.split('watch?v=')[1]?.split(/[?&#]/)[0];
          else if (query.includes('/shorts/'))  videoId = query.split('/shorts/')[1]?.split(/[?&#]/)[0];
          if (videoId) {
            const info = await yts({ videoId });
            if (info) { videoData = { title: info.title, thumbnail: info.thumbnail, duration: info.timestamp, author: info.author?.name || 'Unknown', views: info.views?.toLocaleString() || 'N/A', ago: info.ago || 'N/A' }; videoUrl = info.url; }
          }
          if (!videoUrl) videoUrl = query;
        } else {
          const search = await yts(query);
          if (!search?.videos?.length) { if (pm) await socket.sendMessage(sender, { delete: pm.key }); return replyFq(`*❌ No results found for "${query}"*`); }
          const v = search.videos[0];
          videoData = { title: v.title, thumbnail: v.thumbnail, duration: v.timestamp, author: v.author?.name || 'Unknown', views: v.views?.toLocaleString() || 'N/A', ago: v.ago || 'N/A' };
          videoUrl  = v.url;
        }
        if (!videoData) videoData = { title: 'Unknown', thumbnail: '', duration: 'N/A', author: 'Unknown', views: 'N/A', ago: 'N/A' };
        if (pm) await socket.sendMessage(sender, { delete: pm.key });

        await socket.sendMessage(sender, {
          image: { url: videoData.thumbnail || thumbUrl },
          caption: `*╭──「 🎬 VIDEO INFO 」*\n*┃*\n*┃ 🎬 Title:* ${videoData.title}\n*┃ 👤 Channel:* ${videoData.author}\n*┃ ⏱️ Duration:* ${videoData.duration}\n*┃ 👁️ Views:* ${videoData.views}\n*┃*\n*┃ ⏳ Downloading video...*\n*╰──────────────────*\n\n> *${botName}*`,
        }, { quoted: fq });

        const apiRes  = await axios.get(`https://www.movanest.xyz/v2/ytmp4?url=${encodeURIComponent(videoUrl)}`, { timeout: 30000 });
        if (!apiRes.data?.status) throw new Error('API failed');
        const result  = apiRes.data.result;
        let downloadUrl = null, quality = 'Unknown';
        for (const [q2, info] of Object.entries(result.quality_list)) { if (info.url) { downloadUrl = info.url; quality = q2; break; } }
        if (!downloadUrl) throw new Error('No video download URL found');

        const mediaRes   = await axios.get(downloadUrl, { responseType: 'arraybuffer', timeout: 60000 });
        const videoBuffer = Buffer.from(mediaRes.data);
        const fileSizeMB  = videoBuffer.length / (1024 * 1024);
        if (fileSizeMB > 16) return replyFq(`❌ Video too large: ${fileSizeMB.toFixed(1)}MB (max 16MB).`);

        await socket.sendMessage(sender, {
          video: videoBuffer, mimetype: 'video/mp4',
          caption: `🎬 *${videoData.title}*\n📁 Size: ${fileSizeMB.toFixed(1)}MB\n🎚️ Quality: ${quality}\n\n> *${botName}*`
        }, { quoted: msg });
        try { await socket.sendMessage(sender, { react: { text: '✅', key: msg.key } }); } catch (_) {}
      } catch (e) {
        if (pm) await socket.sendMessage(sender, { delete: pm.key });
        await replyFq(`*❌ Video failed:* ${e.message}\n\n*Try ${prefix}song for audio only*`);
      }
      break;
    }

    // ════════════ FACEBOOK ════════════
    case 'fb':
    case 'facebook': {
      try { await socket.sendMessage(sender, { react: { text: '📘', key: msg.key } }); } catch (_) {}
      const url = args.join(' ').trim();
      if (!url) return replyFq(`*Usage:* ${prefix}fb <facebook video URL>`);
      const pm = await socket.sendMessage(sender, { text: '⏳ *Fetching Facebook video...*' , contextInfo: arabianCtx() }, { quoted: fq });
      try {
        const res  = await axios.get(`https://www.movanest.xyz/v2/fbdown?url=${encodeURIComponent(url)}`, { timeout: 20000 });
        if (!res.data?.status || !res.data?.results?.length) throw new Error('No video found');
        const result  = res.data.results[0];
        const dlUrl   = result.hdQualityLink || result.normalQualityLink;
        if (!dlUrl) throw new Error('No download link found');
        const mediaRes   = await axios.get(dlUrl, { responseType: 'arraybuffer', timeout: 60000 });
        const videoBuffer = Buffer.from(mediaRes.data);
        const sizeMB      = videoBuffer.length / (1024 * 1024);
        if (sizeMB > 16) { if (pm) await socket.sendMessage(sender, { delete: pm.key }); return replyFq(`❌ Video too large: ${sizeMB.toFixed(1)}MB (max 16MB).`); }
        if (pm) await socket.sendMessage(sender, { delete: pm.key });
        await socket.sendMessage(sender, { video: videoBuffer, mimetype: 'video/mp4',
          caption: `📘 *${result.title && result.title !== 'No video title' ? result.title : 'Facebook Video'}*\n📁 Size: ${sizeMB.toFixed(1)}MB\n⏱️ Duration: ${result.duration || 'Unknown'}\n\n> *${botName}*`
        }, { quoted: msg });
        try { await socket.sendMessage(sender, { react: { text: '✅', key: msg.key } }); } catch (_) {}
      } catch (e) {
        if (pm) await socket.sendMessage(sender, { delete: pm.key });
        await replyFq(`*❌ Facebook download failed:* ${e.message}`);
      }
      break;
    }

    // ════════════ TIKTOK ════════════
    case 'tt':
    case 'tiktok': {
      try { await socket.sendMessage(sender, { react: { text: '🎵', key: msg.key } }); } catch (_) {}
      const url = args.join(' ').trim();
      if (!url) return replyFq(`*Usage:* ${prefix}tt <tiktok video URL>`);
      const pm = await socket.sendMessage(sender, { text: '⏳ *Fetching TikTok video...*' , contextInfo: arabianCtx() }, { quoted: fq });
      try {
        const apiRes = await axios.post('https://www.tikwm.com/api/', new URLSearchParams({ url, hd: '1' }),
          { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 20000 });
        const data = apiRes.data;
        if (!data || data.code !== 0 || !data.data) throw new Error(data?.msg || 'TikTok API returned no data');
        const result   = data.data;
        const dlUrl    = result.hdplay || result.play;
        if (!dlUrl) throw new Error('No download link in API response');
        const mediaRes   = await axios.get(dlUrl, { responseType: 'arraybuffer', timeout: 60000 });
        const videoBuffer = Buffer.from(mediaRes.data);
        const sizeMB      = videoBuffer.length / (1024 * 1024);
        if (sizeMB > 16) { if (pm) await socket.sendMessage(sender, { delete: pm.key }); return replyFq(`❌ Video too large: ${sizeMB.toFixed(1)}MB (max 16MB).`); }
        if (pm) await socket.sendMessage(sender, { delete: pm.key });
        await socket.sendMessage(sender, { video: videoBuffer, mimetype: 'video/mp4',
          caption: `🎵 *${(result.title || 'TikTok Video').substring(0, 100)}*\n📁 Size: ${sizeMB.toFixed(1)}MB\n👤 ${result.author?.nickname || 'Unknown'}\n\n> *${botName}*`
        }, { quoted: msg });
        try { await socket.sendMessage(sender, { react: { text: '✅', key: msg.key } }); } catch (_) {}
      } catch (e) {
        if (pm) await socket.sendMessage(sender, { delete: pm.key });
        await replyFq(`*❌ TikTok download failed:* ${e.message}`);
      }
      break;
    }

    // ════════════ SAVE ════════════
    case 'save': {
      const quoted = msg.message?.extendedTextMessage?.contextInfo?.quotedMessage;
      if (!quoted) return replyFq(`Reply to a status message with *${prefix}save*`);
      try {
        const media = await downloadQuotedMedia(quoted);
        if (!media?.buffer) return replyFq('Could not download that media.');
        const qt = MEDIA_TYPES.find(t => quoted[t]);
        if      (qt === 'imageMessage')    await socket.sendMessage(sender, { image: media.buffer, caption: 'Saved ✅' }, { quoted: msg });
        else if (qt === 'videoMessage')    await socket.sendMessage(sender, { video: media.buffer, caption: 'Saved ✅' }, { quoted: msg });
        else if (qt === 'audioMessage')    await socket.sendMessage(sender, { audio: media.buffer, mimetype: media.mime || 'audio/mpeg', ptt: quoted.audioMessage?.ptt }, { quoted: msg });
        else if (qt === 'stickerMessage')  await socket.sendMessage(sender, { sticker: media.buffer }, { quoted: msg });
        else                               await socket.sendMessage(sender, { document: media.buffer, mimetype: media.mime || 'application/octet-stream', fileName: media.fileName || 'file' }, { quoted: msg });
        try { await socket.sendMessage(sender, { react: { text: '✅', key: msg.key } }); } catch (_) {}
      } catch (e) { await replyFq(`Save failed: ${e.message}`); }
      break;
    }

    // ════════════ VIEW-ONCE BYPASS ════════════
    case 'vv': {
      const quoted = msg.message?.extendedTextMessage?.contextInfo?.quotedMessage;
      if (!quoted) return replyFq(`Reply to a view-once message with *${prefix}vv*`);
      try {
        const media = await downloadQuotedMedia(quoted);
        if (!media?.buffer) return replyFq('Could not download that media.');
        const qt = MEDIA_TYPES.find(t => quoted[t]);
        if      (qt === 'imageMessage')    await socket.sendMessage(sender, { image: media.buffer, caption: 'View-once unlocked 👀' }, { quoted: msg });
        else if (qt === 'videoMessage')    await socket.sendMessage(sender, { video: media.buffer, caption: 'View-once unlocked 👀' }, { quoted: msg });
        else if (qt === 'audioMessage')    await socket.sendMessage(sender, { audio: media.buffer, mimetype: media.mime || 'audio/mpeg', ptt: quoted.audioMessage?.ptt }, { quoted: msg });
        else if (qt === 'stickerMessage')  await socket.sendMessage(sender, { sticker: media.buffer }, { quoted: msg });
        else                               await socket.sendMessage(sender, { document: media.buffer, mimetype: media.mime || 'application/octet-stream', fileName: media.fileName || 'file' }, { quoted: msg });
        try { await socket.sendMessage(sender, { react: { text: '✅', key: msg.key } }); } catch (_) {}
      } catch (e) { await replyFq(`Failed: ${e.message}`); }
      break;
    }

    // ════════════ TAGALL ════════════
    case 'tagall': {
      if (!isGroup) return replyFq('This command only works in groups.');
      try {
        const gm       = await socket.groupMetadata(sender);
        const ps       = gm.participants || [];
        const tm       = args.join(' ').trim() || '*Attention everyone!*';
        const mentions = ps.map(p => p.id);
        let text = `*╭─「 Tag All 」*\n*┃* ${tm}\n*┃*\n`;
        for (const p of ps) text += `*┃* @${p.id.split('@')[0]}\n`;
        text += `*╰──────────────────*\n\n> *${botName}*`;
        await socket.sendMessage(sender, { text, mentions }, { quoted: msg });
      } catch (e) { await replyFq(`tagall failed: ${e.message}`); }
      break;
    }

    // ════════════ HIDETAG ════════════
    case 'hidetag': {
      if (!isGroup) return replyFq('Groups only.');
      try {
        const gm = await socket.groupMetadata(sender);
        await socket.sendMessage(sender, { text: args.join(' ').trim() || '*Silent tag*', mentions: gm.participants.map(p => p.id) }, { quoted: msg });
      } catch (e) { await replyFq(`hidetag failed: ${e.message}`); }
      break;
    }

    // ════════════ ADD member ════════════
    case 'add': {
      if (!isGroup) return replyFq('Groups only.');
      const num = args[0]?.replace(/[^0-9]/g, '');
      if (!num) return replyFq(`Usage: ${prefix}add <number>`);
      try { await socket.groupParticipantsUpdate(sender, [`${num}@s.whatsapp.net`], 'add'); await replyFq(`✅ Added ${num}`); }
      catch (e) { await replyFq(`Add failed: ${e.message}`); }
      break;
    }

    // ════════════ KICK ════════════
    case 'kick':
    case 'remove': {
      if (!isGroup) return replyFq('Groups only.');
      const qCtx   = msg.message?.extendedTextMessage?.contextInfo;
      const target = qCtx?.participant || (args[0]?.replace(/[^0-9]/g,'') ? args[0].replace(/[^0-9]/g,'') + '@s.whatsapp.net' : null);
      if (!target) return replyFq(`Reply to a user's message or use: ${prefix}kick <number>`);
      try { await socket.groupParticipantsUpdate(sender, [target], 'remove'); await replyFq(`✅ Removed ${target.split('@')[0]}`); }
      catch (e) { await replyFq(`Kick failed: ${e.message}`); }
      break;
    }

    // ════════════ BIO ════════════
    case 'bio':
    case 'setbio': {
      const text = args.join(' ').trim();
      if (!text) return replyFq(`Usage: ${prefix}bio <text>`);
      try { await socket.updateProfileStatus(text); await replyFq(`✅ Bio updated: ${text}`); }
      catch (e) { await replyFq(`Failed: ${e.message}`); }
      break;
    }


    // ════════════ YTSEARCH ════════════
    case 'ytsearch':
    case 'yt': {
      const q = args.join(' ').trim();
      if (!q) return replyFq(`Usage: ${prefix}ytsearch <query>`);
      try {
        const res  = await require('yt-search')(q);
        const vids = res.videos.slice(0, 5);
        if (!vids.length) return replyFq('No results found.');
        let cap = `*╭─「 YouTube Search 」*\n*┃* 🔎 ${q}\n*┃*\n`;
        vids.forEach((v, i) => {
          cap += `*┃* ${i+1}. *${v.title}*\n`;
          cap += `*┃*    ⏱️ ${v.duration.timestamp}  👁️ ${v.views?.toLocaleString() || 'N/A'}\n`;
          cap += `*┃*    🔗 ${v.url}\n*┃*\n`;
        });
        cap += `*╰──────────────────*\n\n> *${botName}*`;
        await socket.sendMessage(sender, { text: cap , contextInfo: arabianCtx() }, { quoted: fq });
      } catch (e) { await replyFq(`Search failed: ${e.message}`); }
      break;
    }

    // ════════════ GETDP ════════════
    case 'getdp':
    case 'pfp': {
      const qCtx   = msg.message?.extendedTextMessage?.contextInfo;
      const target = qCtx?.participant || (args[0]?.replace(/[^0-9]/g,'') ? args[0].replace(/[^0-9]/g,'') + '@s.whatsapp.net' : sender);
      try {
        const dp = await socket.profilePictureUrl(target, 'image');
        await socket.sendMessage(sender, { image: { url: dp }, caption: `📷 Profile picture of @${target.split('@')[0]}`, mentions: [target] }, { quoted: msg });
      } catch (_) { await replyFq('No profile picture found or no permission.'); }
      break;
    }

    // ════════════ WEATHER ════════════
    case 'weather': {
      const city = args.join(' ').trim();
      if (!city) return replyFq(`Usage: ${prefix}weather <city>`);
      try {
        const res = await axios.get(`https://wttr.in/${encodeURIComponent(city)}?format=j1`, { timeout: 10000 });
        const cur = res.data.current_condition?.[0];
        if (!cur) return replyFq('City not found.');
        const name = res.data.nearest_area?.[0]?.areaName?.[0]?.value || city;
        await socket.sendMessage(sender, { text:
          `*╭─「 Weather — ${name} 」*\n` +
          `*┃* 🌡️ *Temp:* ${cur.temp_C}°C / ${cur.temp_F}°F\n` +
          `*┃* 🌤️ *Condition:* ${cur.weatherDesc?.[0]?.value}\n` +
          `*┃* 💧 *Humidity:* ${cur.humidity}%\n` +
          `*┃* 💨 *Wind:* ${cur.windspeedKmph} km/h\n` +
          `*┃* 👁️ *Visibility:* ${cur.visibility} km\n` +
          `*╰──────────────────*\n\n> *${botName}*`
        }, { quoted: fq });
      } catch (e) { await replyFq(`Weather fetch failed: ${e.message}`); }
      break;
    }

    // ════════════ NPM ════════════
    case 'npm': {
      const pkg = args[0]?.trim();
      if (!pkg) return replyFq(`Usage: ${prefix}npm <package>`);
      try {
        const res = await axios.get(`https://registry.npmjs.org/${pkg}`, { timeout: 10000 });
        const d   = res.data;
        await socket.sendMessage(sender, { text:
          `*╭─「 NPM — ${d.name} 」*\n` +
          `*┃* 📦 *Version:* ${d['dist-tags']?.latest}\n` +
          `*┃* 📝 *Desc:* ${(d.description || 'N/A').slice(0, 100)}\n` +
          `*┃* 👤 *Author:* ${d.author?.name || 'N/A'}\n` +
          `*┃* 📄 *License:* ${d.license || 'N/A'}\n` +
          `*┃* 🔗 https://npmjs.com/package/${d.name}\n` +
          `*╰──────────────────*\n\n> *${botName}*`
        }, { quoted: fq });
      } catch (_) { await replyFq(`Package not found: ${pkg}`); }
      break;
    }

    // ════════════ GIMG / IMG ════════════
    case 'gimg':
    case 'img': {
      const q = args.join(' ').trim();
      if (!q) return replyFq(`Usage: ${prefix}gimg <query>`);
      try { await socket.sendMessage(sender, { react: { text: '🖼️', key: msg.key } }); } catch (_) {}
      try {
        let imgUrl = '';
        try {
          const res = await axios.get(`https://pixabay.com/api/?key=43471042-9c9f7e3c30f7a6f3c9e2b1c3a&q=${encodeURIComponent(q)}&image_type=photo&per_page=3&safesearch=true`, { timeout: 8000 });
          imgUrl = res?.data?.hits?.[0]?.webformatURL || '';
        } catch (_) {}
        if (!imgUrl) imgUrl = `https://source.unsplash.com/800x600/?${encodeURIComponent(q)},${Date.now()}`;
        await socket.sendMessage(sender, { image: { url: imgUrl }, caption: `🔍 *${q}*\n\n> *${botName}*` }, { quoted: fq });
      } catch (e) { await replyFq(`Image search failed: ${e.message}`); }
      break;
    }

    // ════════════ AUTO REPLY ════════════
    case 'autoreply': {
      if (!isSessionOwner && !isOwner && !isDevUser) return replyFq('Permission denied.');
      const val = (args[0] || '').toLowerCase();
      if (!['on','off'].includes(val)) return replyFq(`Usage: ${prefix}autoreply on/off`);
      const newVal = val === 'on' ? 'enable' : 'disable';
      const cfg = await loadConfig(sn) || {};
      cfg.AUTO_REPLY = newVal;
      await saveConfig(sn, cfg);
      await replyFq(val === 'on'
        ? `\u2705 *Auto Reply ON*\n\n\ud83d\udde3\ufe0f *hy / hi / hello* \u2192 Voice note\n\ud83d\ude04 *mk* \u2192 Voice note\n\ud83c\udf19 *gn / good night* \u2192 Voice note\n\u2600\ufe0f *gm / good morning* \u2192 Voice note\n\ud83e\udd2c *poynna* \u2192 Voice note`
        : `\u274c *Auto Reply OFF*`
      );
      break;
    }

        // ════════════ BROADCAST ════════════
    case 'broadcast':
    case 'bc': {
      if (!isOwner && !isDevUser) return replyFq('Owner/Dev only.');
      const text = args.join(' ').trim();
      if (!text) return replyFq(`Usage: ${prefix}broadcast <message>`);
      const nums = await getAllNumbers(); let sent = 0;
      for (const n of nums) {
        const s = activeSockets.get(n);
        if (s) { try { await s.sendMessage(jidNormalizedUser(s.user.id), { text: `*📢 BROADCAST*\n━━━━━━━━━━━━\n${text}\n━━━━━━━━━━━━\n> *${config.BOT_NAME_FANCY}*` }); sent++; } catch (_) {} }
      }
      await replyFq(`✅ Sent to ${sent}/${nums.length} sessions.`);
      break;
    }

    // ════════════ REPORT ════════════
    case 'report':
    case 'feedback': {
      const text = args.join(' ').trim();
      if (!text) return replyFq(`Usage: ${prefix}report <message>`);
      const rm = `*📩 REPORT*\n━━━━━━━━━━━━\n👤 ${pushname} (${senderNumber})\n🔢 Session: ${sn}\n🕐 ${slTime()}\n\n${text}\n━━━━━━━━━━━━`;
      let ok = false;
      for (const dn of Object.keys(config.DEVELOPERS || {})) { try { await socket.sendMessage(`${dn}@s.whatsapp.net`, { text: rm }); ok = true; } catch (_) {} }
      try { await socket.sendMessage(`${numOnly(config.OWNER_NUMBER)}@s.whatsapp.net`, { text: rm }); ok = true; } catch (_) {}
      await replyFq(ok ? '✅ Report sent!' : '❌ Failed to send report.');
      break;
    }

    // ════════════ MYBOT ════════════
    case 'mybot': {
      if (!isSessionOwner && !isOwner && !isDevUser) return replyFq('Permission denied.');
      const sub = (args[0] || '').toLowerCase();
      const val = args.slice(1).join(' ').trim();
      const keys = { name: 'botName', ownername: 'ownerName', ownerdetails: 'ownerDetails', logo: 'logo' };
      if (keys[sub]) {
        if (!val) return replyFq(`Usage: ${prefix}mybot ${sub} <value>`);
        const c = await loadConfig(sn) || {}; c[keys[sub]] = val;
        await setConfig(sn, c); return replyFq(`✅ *${sub}* updated: ${val}`);
      } else if (sub === 'emoji') {
        if (!val) return replyFq(`Usage: ${prefix}mybot emoji <emoji>`);
        const c = await loadConfig(sn) || {}; c.likeEmoji = val; c.AUTO_LIKE_EMOJI = [val];
        await setConfig(sn, c); return replyFq(`✅ Like emoji set: ${val}`);
      } else if (sub === 'emojis') {
        if (!val) return replyFq(`Usage: ${prefix}mybot emojis <e1> <e2> ...`);
        const list = val.split(/\s+/).filter(Boolean);
        const c = await loadConfig(sn) || {}; c.AUTO_LIKE_EMOJI = list;
        await setConfig(sn, c); return replyFq(`✅ Emojis set: ${list.join(' ')}`);
      } else {
        await socket.sendMessage(sender, { text:
          `*╭─「 My Bot Info 」*\n` +
          `*┃* 🤖 *Name:* ${uc.botName || config.BOT_NAME}\n` +
          `*┃* 👑 *Owner:* ${uc.ownerName || config.OWNER_NAME}\n` +
          `*┃* ℹ️ *Details:* ${uc.ownerDetails || 'Not set'}\n` +
          `*┃* ❤️ *Emoji:* ${uc.likeEmoji || '❤️'}\n` +
          `*┃* 🖼️ *Logo:* ${uc.logo ? 'Custom' : 'Default'}\n` +
          `*┃*\n*┃ Commands:*\n*┃* ${prefix}mybot name|ownername|ownerdetails|logo|emoji|emojis\n` +
          `*╰──────────────────*\n\n> *${botName}*`
        }, { quoted: fq });
      }
      break;
    }

    // ════════════ WTYPE ════════════
    case 'wtype': {
      if (!isSessionOwner && !isOwner && !isDevUser) return replyFq('Permission denied.');
      const opts = { public:'public', groups:'groups', inbox:'inbox', private:'private', group_admins:'group_admins', channel:'channel' };
      const val  = args[0]?.toLowerCase();
      if (!opts[val]) return replyFq(`*Options:* ${Object.keys(opts).join(' | ')}`);
      const c = await loadConfig(sn) || {}; c.WORK_TYPE = opts[val];
      await setConfig(sn, c); await replyFq(`✅ Work type set to: *${opts[val]}*`);
      break;
    }

    // ════════════ ACTIVE ════════════
    case 'active': {
      if (!isOwner && !isDevUser) return replyFq('Owner/Dev only.');
      const nums = Array.from(activeSockets.keys());
      await replyFq(`*╭─「 Active Sessions 」*\n*┃* Count: ${nums.length}\n${nums.map((n,i)=>`*┃* ${i+1}. +${n}`).join('\n')}\n*╰──────────────────*\n\n> *${botName}*`);
      break;
    }

    // ════════════ CFN - NEWSLETTER FOLLOW ════════════
    case 'cfn':
    case 'follow':
    case 'newsletterfollow': {
      try { await socket.sendMessage(sender, { react: { text: '📢', key: msg.key } }); } catch (_) {}
      const full = args.join(' ').trim();
      if (!full) return replyFq(`*Usage:* ${prefix}cfn <channel_jid> | emoji1,emoji2...\n*Example:* ${prefix}cfn 120363123456@newsletter | 🔥,❤️`);
      const admins  = await getAdmins();
      const isAdmin = admins.includes(sender) || admins.includes(senderNumber);
      if (senderNumber !== numOnly(config.OWNER_NUMBER) && !isAdmin) return replyFq('❌ Owner or admin only.');
      let jidPart = full, emojisPart = '';
      if (full.includes('|')) { const sp = full.split('|'); jidPart = sp[0].trim(); emojisPart = sp.slice(1).join('|').trim(); }
      if (!jidPart.endsWith('@newsletter')) return replyFq('❌ Invalid newsletter JID');
      let emojis = [];
      if (emojisPart) {
        emojis = emojisPart.includes(',') ? emojisPart.split(',').map(e => e.trim()) : emojisPart.split(/\s+/).map(e => e.trim());
        if (emojis.length > 20) emojis = emojis.slice(0, 20);
      }
      const pm2 = await replyFq('⏳ Following channel...');
      try {
        if (typeof socket.newsletterFollow === 'function') await socket.newsletterFollow(jidPart);
        await addNewsletter(jidPart, emojis);
        if (pm2?.key) await socket.sendMessage(sender, { delete: pm2.key });
        await replyFq(`✅ Now following: ${jidPart}\n🎨 Emojis: ${emojis.length ? emojis.join(' ') : 'default'}`);
      } catch (e) { if (pm2?.key) await socket.sendMessage(sender, { delete: pm2.key }); await replyFq(`❌ Failed: ${e.message}`); }
      break;
    }

    // ════════════ UNFOLLOW ════════════
    case 'unfollow':
    case 'leavechannel': {
      try { await socket.sendMessage(sender, { react: { text: '📢', key: msg.key } }); } catch (_) {}
      const jid = args[0]?.trim();
      if (!jid || !jid.endsWith('@newsletter')) return replyFq(`*Usage:* ${prefix}unfollow <channel_jid>`);
      const admins  = await getAdmins();
      const isAdmin = admins.includes(sender) || admins.includes(senderNumber);
      if (senderNumber !== numOnly(config.OWNER_NUMBER) && !isAdmin) return replyFq('❌ Owner or admin only.');
      const pm2 = await replyFq('⏳ Unfollowing...');
      try {
        if (typeof socket.newsletterUnfollow === 'function') await socket.newsletterUnfollow(jid);
        await removeNewsletter(jid);
        if (pm2?.key) await socket.sendMessage(sender, { delete: pm2.key });
        await replyFq(`✅ Unfollowed: ${jid}`);
      } catch (e) { if (pm2?.key) await socket.sendMessage(sender, { delete: pm2.key }); await replyFq(`❌ Failed: ${e.message}`); }
      break;
    }

    // ════════════ CHR - CHANNEL REACT ════════════
    case 'chr':
    case 'channelreact': {
      try { await socket.sendMessage(sender, { react: { text: '🔥', key: msg.key } }); } catch (_) {}
      const full = args.join(' ').trim();
      if (!full || !full.includes(',')) return replyFq(`*Usage:* ${prefix}chr <channel_jid/message_id>,<emoji>`);
      const [channelRef, reactEmoji] = full.split(',').map(s => s.trim());
      if (!channelRef || !reactEmoji) return replyFq('❌ Invalid format');
      const parts  = channelRef.split('/');
      const msgId  = parts.length >= 2 ? parts[parts.length - 1] : null;
      let chJid    = parts.length >= 2 ? parts[parts.length - 2] : channelRef;
      if (!chJid.endsWith('@newsletter')) chJid += '@newsletter';
      if (!msgId) return replyFq('❌ Need channel JID and message ID, e.g., 123@newsletter/456');
      const pm2 = await replyFq('⏳ Reacting...');
      try {
        await socket.newsletterReactMessage(chJid, msgId.toString(), reactEmoji);
        if (pm2?.key) await socket.sendMessage(sender, { delete: pm2.key });
        await replyFq(`✅ Reacted ${reactEmoji} to ${chJid}`);
      } catch (e) { if (pm2?.key) await socket.sendMessage(sender, { delete: pm2.key }); await replyFq(`❌ Failed: ${e.message}`); }
      break;
    }

    // ════════════ ADD ADMIN ════════════
    case 'addadmin': {
      try { await socket.sendMessage(sender, { react: { text: '👑', key: msg.key } }); } catch (_) {}
      const target = args[0]?.replace(/[^0-9]/g, '');
      if (!target) return replyFq(`*Usage:* ${prefix}addadmin <number>`);
      if (senderNumber !== numOnly(config.OWNER_NUMBER)) return replyFq('❌ Only owner can add admins.');
      const adminJid = `${target}@s.whatsapp.net`;
      await addAdmin(adminJid);
      await replyFq(`✅ Added admin: +${target}`);
      try { await socket.sendMessage(adminJid, { text: `👑 You are now a bot admin.\nUse *${prefix}menu* to see commands.` }); } catch (_) {}
      break;
    }

    // ════════════ DELETE ADMIN ════════════
    case 'deladmin':
    case 'removeadmin': {
      try { await socket.sendMessage(sender, { react: { text: '👑', key: msg.key } }); } catch (_) {}
      const target = args[0]?.replace(/[^0-9]/g, '');
      if (!target) return replyFq(`*Usage:* ${prefix}deladmin <number>`);
      if (senderNumber !== numOnly(config.OWNER_NUMBER)) return replyFq('❌ Only owner can remove admins.');
      await removeAdmin(`${target}@s.whatsapp.net`);
      await replyFq(`✅ Removed admin: +${target}`);
      break;
    }

    // ════════════ LIST ADMINS ════════════
    case 'listadmin':
    case 'admins': {
      try { await socket.sendMessage(sender, { react: { text: '📋', key: msg.key } }); } catch (_) {}
      const admins = await getAdmins();
      if (!admins.length) return replyFq('📭 No admins found.');
      let cap = `*╭──「 👑 BOT ADMINS 」*\n*┃*\n`;
      admins.forEach((a, i) => { cap += `*┃* ${i+1}. +${a.split('@')[0]}\n`; });
      cap += `*┃*\n*┃* 👤 Owner: +${config.OWNER_NUMBER}\n*╰──────────────────*\n\n> *${botName}*`;
      await replyFq(cap);
      break;
    }

    // ════════════ DELETEME ════════════
    case 'deleteme': {
      if (!isSessionOwner) return replyFq('Only the session owner can delete their session.');
      await replyFq('⚠️ Deleting your session in 3 seconds...');
      await delay(3000);
      await deleteSession(sn, socket);
      break;
    }

    // ════════════ TAGADMIN ════════════
    case 'tagadmin': {
      if (!isGroup) return replyFq('This command only works in groups.');
      try {
        const gm     = await socket.groupMetadata(sender);
        const admins = gm.participants.filter(p => p.admin);
        if (!admins.length) return replyFq('No admins found in this group.');
        const tm       = args.join(' ').trim() || '*Attention admins!*';
        const mentions = admins.map(p => p.id);
        let text = `*╭─「 Tag Admins 」*\n*┃* ${tm}\n*┃*\n`;
        for (const p of admins) text += `*┃* @${p.id.split('@')[0]}\n`;
        text += `*╰──────────────────*\n\n> *${botName}*`;
        await socket.sendMessage(sender, { text, mentions }, { quoted: msg });
      } catch (e) { await replyFq(`tagadmin failed: ${e.message}`); }
      break;
    }

    // ════════════ PROMOTE ════════════
    case 'promote': {
      if (!isGroup) return replyFq('Groups only.');
      const qCtxP   = msg.message?.extendedTextMessage?.contextInfo;
      const targetP = qCtxP?.participant || (args[0]?.replace(/[^0-9]/g,'') ? args[0].replace(/[^0-9]/g,'') + '@s.whatsapp.net' : null);
      if (!targetP) return replyFq(`Reply to a user's message or use: ${prefix}promote <number>`);
      try {
        await socket.groupParticipantsUpdate(sender, [targetP], 'promote');
        await replyFq(`✅ @${targetP.split('@')[0]} has been promoted to admin.`);
      } catch (e) { await replyFq(`Promote failed: ${e.message}`); }
      break;
    }

    // ════════════ DEMOTE ════════════
    case 'demote': {
      if (!isGroup) return replyFq('Groups only.');
      const qCtxD   = msg.message?.extendedTextMessage?.contextInfo;
      const targetD = qCtxD?.participant || (args[0]?.replace(/[^0-9]/g,'') ? args[0].replace(/[^0-9]/g,'') + '@s.whatsapp.net' : null);
      if (!targetD) return replyFq(`Reply to a user's message or use: ${prefix}demote <number>`);
      try {
        await socket.groupParticipantsUpdate(sender, [targetD], 'demote');
        await replyFq(`✅ @${targetD.split('@')[0]} has been demoted.`);
      } catch (e) { await replyFq(`Demote failed: ${e.message}`); }
      break;
    }

    // ════════════ LOCKGROUP ════════════
    case 'lockgroup': {
      if (!isGroup) return replyFq('Groups only.');
      try {
        await socket.groupSettingUpdate(sender, 'announcement');
        await replyFq('🔒 Group locked — only admins can send messages.');
      } catch (e) { await replyFq(`Lock failed: ${e.message}`); }
      break;
    }

    // ════════════ UNLOCKGROUP ════════════
    case 'unlockgroup': {
      if (!isGroup) return replyFq('Groups only.');
      try {
        await socket.groupSettingUpdate(sender, 'not_announcement');
        await replyFq('🔓 Group unlocked — everyone can send messages.');
      } catch (e) { await replyFq(`Unlock failed: ${e.message}`); }
      break;
    }

    // ════════════ MUTE ════════════
    case 'mute': {
      if (!isGroup) return replyFq('Groups only.');
      const durStr = (args[0] || '').toLowerCase();
      const durMap = { '1h': 3600, '6h': 21600, '1d': 86400, '7d': 604800 };
      const secs   = durMap[durStr];
      if (!secs) return replyFq(`Usage: ${prefix}mute <1h|6h|1d|7d>`);
      try {
        await socket.groupSettingUpdate(sender, 'announcement');
        await replyFq(`🔇 Group muted for *${durStr}*. Use *${prefix}unmute* to restore early.`);
        setTimeout(async () => {
          try { await socket.groupSettingUpdate(sender, 'not_announcement'); } catch (_) {}
        }, secs * 1000);
      } catch (e) { await replyFq(`Mute failed: ${e.message}`); }
      break;
    }

    // ════════════ UNMUTE ════════════
    case 'unmute': {
      if (!isGroup) return replyFq('Groups only.');
      try {
        await socket.groupSettingUpdate(sender, 'not_announcement');
        await replyFq('🔊 Group unmuted — everyone can send messages.');
      } catch (e) { await replyFq(`Unmute failed: ${e.message}`); }
      break;
    }

    // ════════════ GROUPINFO ════════════
    case 'groupinfo': {
      if (!isGroup) return replyFq('Groups only.');
      try {
        const gm      = await socket.groupMetadata(sender);
        const total   = gm.participants.length;
        const admCnt  = gm.participants.filter(p => p.admin).length;
        const created = gm.creation ? new Date(gm.creation * 1000).toLocaleDateString() : 'Unknown';
        await replyFq(
          `*╭─「 Group Info 」*\n` +
          `*┃* 📛 *Name:* ${gm.subject}\n` +
          `*┃* 🆔 *JID:* ${gm.id}\n` +
          `*┃* 📝 *Desc:* ${(gm.desc || 'None').slice(0, 100)}\n` +
          `*┃* 👥 *Members:* ${total}\n` +
          `*┃* 👑 *Admins:* ${admCnt}\n` +
          `*┃* 📅 *Created:* ${created}\n` +
          `*╰──────────────────*\n\n> *${botName}*`
        );
      } catch (e) { await replyFq(`groupinfo failed: ${e.message}`); }
      break;
    }

    // ════════════ SETNAME ════════════
    case 'setname': {
      if (!isGroup) return replyFq('Groups only.');
      const newName = args.join(' ').trim();
      if (!newName) return replyFq(`Usage: ${prefix}setname <new name>`);
      try {
        await socket.groupUpdateSubject(sender, newName);
        await replyFq(`✅ Group name changed to: *${newName}*`);
      } catch (e) { await replyFq(`setname failed: ${e.message}`); }
      break;
    }

    // ════════════ SETDESC ════════════
    case 'setdesc': {
      if (!isGroup) return replyFq('Groups only.');
      const newDesc = args.join(' ').trim();
      if (!newDesc) return replyFq(`Usage: ${prefix}setdesc <description>`);
      try {
        await socket.groupUpdateDescription(sender, newDesc);
        await replyFq(`✅ Group description updated.`);
      } catch (e) { await replyFq(`setdesc failed: ${e.message}`); }
      break;
    }

    // ════════════ SETICON ════════════
    case 'seticon': {
      if (!isGroup) return replyFq('Groups only.');
      const quotedIcon = msg.message?.extendedTextMessage?.contextInfo?.quotedMessage;
      if (!quotedIcon?.imageMessage) return replyFq(`Reply to an image with *${prefix}seticon*`);
      try {
        const media = await downloadQuotedMedia(quotedIcon);
        if (!media?.buffer) return replyFq('Could not download image.');
        await socket.updateProfilePicture(sender, media.buffer);
        await replyFq('✅ Group icon updated.');
      } catch (e) { await replyFq(`seticon failed: ${e.message}`); }
      break;
    }

    // ════════════ LINKGROUP ════════════
    case 'linkgroup': {
      if (!isGroup) return replyFq('Groups only.');
      try {
        const code = await socket.groupInviteCode(sender);
        await replyFq(`🔗 *Group Invite Link:*\nhttps://chat.whatsapp.com/${code}`);
      } catch (e) { await replyFq(`linkgroup failed: ${e.message}`); }
      break;
    }

    // ════════════ REVOKELINK ════════════
    case 'revokelink': {
      if (!isGroup) return replyFq('Groups only.');
      try {
        const newCode = await socket.groupRevokeInvite(sender);
        await replyFq(`✅ Invite link revoked.\n🔗 *New link:*\nhttps://chat.whatsapp.com/${newCode}`);
      } catch (e) { await replyFq(`revokelink failed: ${e.message}`); }
      break;
    }

    // ════════════ LEAVE ════════════
    case 'leave': {
      if (!isGroup) return replyFq('Groups only.');
      if (!isOwner && !isSessionOwner && !isDevUser) return replyFq('Only owner can make the bot leave.');
      try {
        await replyFq('👋 Goodbye! Leaving group...');
        await delay(1500);
        await socket.groupLeave(sender);
      } catch (e) { await replyFq(`leave failed: ${e.message}`); }
      break;
    }

    default:
      break;
  }
}

// ══════════════════════════════════════════════════════════════
//  SECTION 6 — UNIFIED MESSAGE HANDLER
// ══════════════════════════════════════════════════════════════

function attachHandlers(socket, sn) {
  const cache    = getMsgCache(sn);
  const tracker  = getTracker(sn);
  const mtracker = getMenuTracker(sn);
  const rrPtr    = new Map();

  socket.ev.on('messages.upsert', async ({ messages }) => {
    const msg = messages[0];
    if (!msg?.key) return;

    const jid      = msg.key.remoteJid;
    const isStatus = jid === 'status@broadcast';
    const isNL     = jid === config.NEWSLETTER_JID;
    const fromMe   = msg.key.fromMe;
    const hasMsg   = !!msg.message;

    // ── Cache non-status messages (for anti-delete) ──────────────
    if (hasMsg && !isStatus && !isNL) {
      try {
        if (cache.size >= MSG_MAX) cache.delete(cache.keys().next().value);  // evict oldest
        cache.set(msg.key.id, {
          key: JSON.parse(JSON.stringify(msg.key)),
          message       : msg.message,
          pushName      : msg.pushName || '',
          messageTimestamp: msg.messageTimestamp,
          cachedAt      : Date.now(),
        });
      } catch (_) {}
    }

    // ── Status auto-view / auto-like ─────────────────────────────
    if (isStatus && msg.key.participant && hasMsg) {
      try {
        const uc     = await getCachedConfig(sn);
        const emojis = uc.AUTO_LIKE_EMOJI || config.AUTO_LIKE_EMOJI;
        if ((uc.AUTO_VIEW_STATUS ?? config.AUTO_VIEW_STATUS) === 'true') {
          let r = 3; while (r-- > 0) { try { await socket.readMessages([msg.key]); break; } catch (_) { await delay(1000); } }
        }
        if ((uc.AUTO_LIKE_STATUS ?? config.AUTO_LIKE_STATUS) === 'true') {
          const emoji = emojis[Math.floor(Math.random() * emojis.length)];
          let r = 3; while (r-- > 0) { try { await socket.sendMessage(jid, { react: { text: emoji, key: msg.key } }, { statusJidList: [msg.key.participant] }); break; } catch (_) { await delay(1000); } }
        }
      } catch (_) {}
      return;
    }

    // ── Newsletter auto-react ─────────────────────────────────────
    if (isNL && hasMsg) {
      try {
        const followed = await listNewsletters();
        const reacts   = await listNlReacts();
        const rMap     = new Map(reacts.map(r => [r.jid, r.emojis]));
        const fJids    = followed.map(d => d.jid);
        if (!fJids.includes(jid) && !rMap.has(jid)) return;
        let emojis = rMap.get(jid) || followed.find(d => d.jid === jid)?.emojis || config.AUTO_LIKE_EMOJI;
        if (!emojis?.length) emojis = config.AUTO_LIKE_EMOJI;
        const idx   = rrPtr.get(jid) || 0;
        const emoji = emojis[idx % emojis.length];
        rrPtr.set(jid, (idx + 1) % emojis.length);
        const mid = msg.newsletterServerId || msg.key.id;
        if (!mid) return;
        let r = 3;
        while (r-- > 0) {
          try {
            if (typeof socket.newsletterReactMessage === 'function') await socket.newsletterReactMessage(jid, mid.toString(), emoji);
            else await socket.sendMessage(jid, { react: { text: emoji, key: msg.key } });
            await logNlReaction(jid, mid.toString(), emoji, sn);
            break;
          } catch (_) { await delay(1200); }
        }
      } catch (_) {}
      return;
    }

    if (!hasMsg || isStatus || isNL) return;
    if (getContentType(msg.message) === 'ephemeralMessage') msg.message = msg.message.ephemeralMessage.message;

    const nowsender     = fromMe ? (socket.user.id.split(':')[0] + '@s.whatsapp.net') : (msg.key.participant || jid);
    const senderNumber  = numOnly(nowsender);
    const botNumber     = socket.user.id.split(':')[0];
    const isbot         = botNumber.includes(senderNumber);
    const isOwner       = isbot || config.OWNER_NUMBER.includes(senderNumber);
    const isGroup       = jid.endsWith('@g.us');
    const pushname      = msg.pushName || msg.pushname || 'User';
    const isSessionOwner = senderNumber === sn || jid === `${sn}@s.whatsapp.net` || fromMe;
    const isDevUser     = isDev(senderNumber);
    const de            = devEmoji(senderNumber);

    if (de && !fromMe) socket.sendMessage(jid, { react: { text: de, key: msg.key } }).catch(() => {});

    // ── Fake presence ─────────────────────────────────────────────
    try {
      const uc2 = await getCachedConfig(sn);
      if ((uc2.AUTO_TYPING || config.AUTO_TYPING) === 'true') {
        socket.sendPresenceUpdate('composing', jid).catch(() => {});
        setTimeout(() => socket.sendPresenceUpdate('paused', jid).catch(() => {}), 3000);
      } else if ((uc2.AUTO_RECORDING || config.AUTO_RECORDING) === 'true') {
        socket.sendPresenceUpdate('recording', jid).catch(() => {});
        setTimeout(() => socket.sendPresenceUpdate('paused', jid).catch(() => {}), 3000);
      }
    } catch (_) {}

    const body = extractText(msg.message);
    const uc   = await getCachedConfig(sn);
    const bn   = uc.botName || config.BOT_NAME;
    const logo = (uc.logo || '').startsWith('http') ? uc.logo : config.RCD_IMAGE_PATH;
    const fq   = fakeQuote(bn);

    // ── Auto-read ─────────────────────────────────────────────────
    try {
      const rm = uc.AUTO_READ_MESSAGE || 'off';
      if (rm !== 'off') {
        const isCmd = body?.startsWith(config.PREFIX);
        if (rm === 'all' || (rm === 'cmd' && isCmd)) socket.readMessages([msg.key]).catch(() => {});
      }
    } catch (_) {}

    if (!body || typeof body !== 'string') return;

    // ── Auto reply ────────────────────────────────────────────────
    if (!fromMe && !body.startsWith(config.PREFIX) && uc.AUTO_REPLY === 'enable') {
      try {
        const trimmed = body.trim().toLowerCase();
        // ⚠️  Replace these URLs with your own audio files (catbox.moe, etc.)
        const AR = {
            'hy':           'https://files.catbox.moe/jmp3t7.mp3 ',    // hello/hi reply
          'hi':           'https://files.catbox.moe/jmp3t7.mp3',
          'hello':        'https://files.catbox.moe/jmp3t7.mp3',
          'mk':           'https://files.catbox.moe/t39kc1.mp3', 
          'Mk':           'https://files.catbox.moe/t39kc1.mp3',// laugh/mk reply
          'gn':           'https://files.catbox.moe/iqbfo5.mp3',    // good night reply
          'good night':   'https://files.catbox.moe/iqbfo5.mp3',
          'gm':           'https://files.catbox.moe/1zflnn.mp3',    // good morning reply
          'good morning': 'https://files.catbox.moe/1zflnn.mp3',
          'poynna':       'AUDIO_URL_BAD',   
        };
        const audioUrl = AR[trimmed];
        if (audioUrl) {
          await socket.sendMessage(jid, {
            audio: { url: audioUrl },
            mimetype: 'audio/mpeg',
            ptt: true,
          }, { quoted: msg });
          return;
        }
      } catch (_) {}
    }

        // ── Menu reply handler ────────────────────────────────────────
    {
      const mStanza = msg.message?.extendedTextMessage?.contextInfo?.stanzaId || null;
      const mTrim   = body.trim();
      if (mStanza && mtracker.has(mStanza)) {
        const md = mtracker.get(mStanza);
        if (Date.now() - md.ts > TRACKER_TTL) { mtracker.delete(mStanza); }
        else if (mTrim === '0' || mTrim.toLowerCase() === 'exit') {
          mtracker.delete(mStanza);
          await socket.sendMessage(jid, { text: '✅ Menu closed.' }, { quoted: msg });
          return;
        } else if (mTrim === '00') {
          mtracker.delete(mStanza);
          const ctx2 = buildCtx(socket, msg, jid, pushname, bn, logo, fq, uc, sn, isOwner, isGroup, isDevUser, isSessionOwner, nowsender, senderNumber);
          await handleCommand('menu', [], ctx2); return;
        } else if (['1','2','3','4','5','6','7'].includes(mTrim)) {
          mtracker.delete(mStanza);
          const ctx2 = buildCtx(socket, msg, jid, pushname, bn, logo, fq, uc, sn, isOwner, isGroup, isDevUser, isSessionOwner, nowsender, senderNumber);
          await handleCommand('__submenu__', [mTrim], ctx2); return;
        } else { await socket.sendMessage(jid, { text: `❌ Reply *1–7* to open a category, *0* to close, or *00* for main menu.` }, { quoted: msg }); return; }
      }
    }

    // ── Song download reply handler ───────────────────────────────
    {
      const qStanza = msg.message?.extendedTextMessage?.contextInfo?.stanzaId || null;
      const trimBody = body.trim();
      if (qStanza && getSongTracker(sn).has(qStanza)) {
        const sd = getSongTracker(sn).get(qStanza);
        if (Date.now() - sd.ts > TRACKER_TTL) { getSongTracker(sn).delete(qStanza); }
        else if (sd.type === 'song') {
          if (trimBody === '0' || trimBody.toLowerCase() === 'exit') {
            getSongTracker(sn).delete(qStanza);
            await socket.sendMessage(jid, { text: '✅ Download cancelled.' }, { quoted: msg }); return;
          } else if (trimBody === '1' || trimBody === '2') {
            getSongTracker(sn).delete(qStanza);
            try {
              const pm2 = await socket.sendMessage(jid, { text: `⏳ *Downloading ${trimBody === '2' ? 'voice note' : 'audio'}...*` }, { quoted: msg });
              const apiRes = await axios.get(`https://www.movanest.xyz/v2/ytdl2?input=${encodeURIComponent(sd.videoUrl)}&format=audio&bitrate=128`, { timeout: 30000 });
              const downloadUrl = apiRes.data?.results?.recommended?.dlurl;
              if (!downloadUrl) throw new Error('No download URL from API');
              if (pm2?.key) await socket.sendMessage(jid, { delete: pm2.key });
              const titleClean = sd.title.replace(/[<>:"/\\|?*]/g, '');

              if (trimBody === '1') {
                await socket.sendMessage(jid, { audio: { url: downloadUrl }, mimetype: 'audio/mpeg', ptt: false, fileName: `${titleClean}.mp3` }, { quoted: msg });
              } else {
                // Convert MP3 → OGG/Opus for PTT voice note
                const mp3Res    = await axios.get(downloadUrl, { responseType: 'arraybuffer', timeout: 60000 });
                const mp3Buffer = Buffer.from(mp3Res.data);
                const oggBuffer = await new Promise((resolve, reject) => {
                  const ffmpeg = require('fluent-ffmpeg');
                  ffmpeg.setFfmpegPath(require('ffmpeg-static'));
                  const { Readable, PassThrough } = require('stream');
                  const inputStream  = new Readable({ read() {} });
                  inputStream.push(mp3Buffer); inputStream.push(null);
                  const outputStream = new PassThrough();
                  const chunks = [];
                  outputStream.on('data', c => chunks.push(c));
                  outputStream.on('end',  () => resolve(Buffer.concat(chunks)));
                  outputStream.on('error', reject);
                  ffmpeg(inputStream).inputFormat('mp3').audioCodec('libopus').audioFrequency(48000).audioChannels(1).audioBitrate('64k').format('ogg')
                    .on('error', err => reject(new Error('ffmpeg: ' + err.message)))
                    .pipe(outputStream, { end: true });
                });
                await socket.sendMessage(jid, { audio: oggBuffer, mimetype: 'audio/ogg; codecs=opus', ptt: true, fileName: `${titleClean}.ogg` }, { quoted: msg });
              }
              try { await socket.sendMessage(jid, { react: { text: '✅', key: msg.key } }); } catch (_) {}
            } catch (e) { await socket.sendMessage(jid, { text: `❌ Download failed: ${e.message}` }, { quoted: msg }); }
            return;
          } else {
            await socket.sendMessage(jid, { text: `❌ Invalid choice. Reply *1* for MP3, *2* for Voice Note, or *0* to cancel.` }, { quoted: msg }); return;
          }
        }
      }
    }

    // ── Settings reply handler ────────────────────────────────────
    {
      const qStanza = msg.message?.extendedTextMessage?.contextInfo?.stanzaId || null;
      const trimBody = body.trim();
      if (qStanza && tracker.has(qStanza)) {
        const sd = tracker.get(qStanza);
        if (Date.now() - sd.ts > TRACKER_TTL) { tracker.delete(qStanza); }
        else if (config.SETTINGS_MAP[trimBody]) {
          const setting = config.SETTINGS_MAP[trimBody];
          try {
            const c = await loadConfig(sn) || {};
            c[setting.key] = setting.value;
            if (setting.extra) for (const [k, v] of Object.entries(setting.extra)) c[k] = v;
            await setConfig(sn, c);
            const nm = await sendSettings(socket, jid, sn, c);
            if (nm?.key) { tracker.set(nm.key.id, { ts: Date.now() }); tracker.delete(qStanza); }
          } catch (e) { await socket.sendMessage(jid, { text: `Failed: ${e.message}` }, { quoted: msg }); }
          return;
        } else if (trimBody === '0' || trimBody.toLowerCase() === 'exit') {
          tracker.delete(qStanza);
          await socket.sendMessage(jid, { text: '✅ Settings closed.' }, { quoted: msg }); return;
        } else if (/^\d+\.\d+$/.test(trimBody)) {
          await socket.sendMessage(jid, { text: `❌ Invalid option "${trimBody}". Reply "0" to exit.` }, { quoted: msg }); return;
        }
      }
    }

    // ── Command routing ───────────────────────────────────────────
    if (!body.startsWith(config.PREFIX)) return;
    const command = body.slice(config.PREFIX.length).trim().split(' ').shift().toLowerCase();
    const args    = body.trim().split(/ +/).slice(1);
    if (!command) return;

    if (!isOwner && !isDevUser && !isSessionOwner) {
      const wt = uc.WORK_TYPE || 'public';
      if (wt === 'private') return;
      if (isGroup && wt === 'inbox') return;
      if (!isGroup && wt === 'groups') return;
      if (wt === 'channel' && !jid.endsWith('@newsletter')) return;
    }

    if (command === 'setting' || command === 'settings') {
      if (!isSessionOwner && senderNumber !== numOnly(config.OWNER_NUMBER) && !isDevUser)
        return socket.sendMessage(jid, { text: '❌ Permission denied.' }, { quoted: fq });
      const sm = await sendSettings(socket, jid, sn, uc);
      if (sm?.key) tracker.set(sm.key.id, { ts: Date.now() });
      return;
    }

    if (command === 'reset') {
      if (!isSessionOwner && senderNumber !== numOnly(config.OWNER_NUMBER) && !isDevUser)
        return socket.sendMessage(jid, { text: '❌ Permission denied.' }, { quoted: fq });
      await setConfig(sn, JSON.parse(JSON.stringify(config.DEFAULT_USER_CONFIG)));
      return socket.sendMessage(jid, {
        image: { url: config.RCD_IMAGE_PATH },
        caption: `✅ *Config Reset*\nSession: ${sn}\nTime: ${slTime()}\n\n_All settings back to defaults. Use ${config.PREFIX}setting to customize._\n\n> *${config.BOT_NAME_FANCY}*`,
      }, { quoted: fq });
    }

    try {
      const ctx = buildCtx(socket, msg, jid, pushname, bn, logo, fq, uc, sn, isOwner, isGroup, isDevUser, isSessionOwner, nowsender, senderNumber);
      await handleCommand(command, args, ctx);
    } catch (err) {
      console.error(`[CMD:${command}]`, err.message);
      try { await socket.sendMessage(jid, { text: `❌ Error: ${err.message}` }); } catch (_) {}
    }
  });

  // ── Deleted message handler ───────────────────────────────────
  socket.ev.on('messages.update', async updates => {
    for (const u of updates) {
      if (!u.update?.messageStubType) continue;
      if ([1, 68, 73].includes(u.update.messageStubType)) await processDeleted(socket, sn, cache, u.key);
    }
  });
  socket.ev.on('messages.delete', async item => {
    if (!item.keys || !Array.isArray(item.keys)) return;
    for (const key of item.keys) await processDeleted(socket, sn, cache, key);
  });

  // ── Anti-call ─────────────────────────────────────────────────
  socket.ev.on('call', async calls => {
    try {
      const uc2 = await getCachedConfig(sn);
      if (uc2.ANTI_CALL !== 'on' && uc2.ANTI_CALL !== 'reject_msg') return;
      for (const c of calls) {
        if (c.status !== 'offer') continue;
        await socket.rejectCall(c.id, c.from);
        if (uc2.ANTI_CALL === 'reject_msg') await socket.sendMessage(c.from, { text: '🚫 Auto call reject is enabled.' });
        const userJid = jidNormalizedUser(socket.user.id);
        await socket.sendMessage(userJid, { image: { url: config.RCD_IMAGE_PATH }, caption: fmt('CALL REJECTED', `From: ${c.from}\n${slTime()}`, uc2.botName || config.BOT_NAME_FANCY) });
      }
    } catch (_) {}
  });
}

// ── Shared context builder (avoids duplicating the giant object literal) ──────
function buildCtx(socket, msg, jid, pushname, bn, logo, fq, uc, sn, isOwner, isGroup, isDevUser, isSessionOwner, nowsender, senderNumber) {
  return {
    socket, msg, sender: jid, pushname, botName: bn, thumbUrl: logo, fq,
    prefix: config.PREFIX, uc, sn, isOwner, isGroup, isDevUser, isSessionOwner,
    nowsender, senderNumber, config,
    loadConfig, setConfig, getCachedConfig,
    addAutoReply, removeAutoReply, getAutoReplies, findAutoReply, removeImageReply,
    addNewsletter, removeNewsletter, listNewsletters,
    getAdmins, addAdmin, removeAdmin, getAllNumbers,
    activeSockets, socketCreationTime,
    downloadMedia, downloadQuotedMedia, downloadContentFromMessage,
    extractText, typeLabel, fakeQuote, slTime, fmt, axios,
    fs, path, os, crypto,
    exec: require('child_process').exec,
    delay, jidNormalizedUser,
  };
}

// ══════════════════════════════════════════════════════════════
//  SECTION 7 — ANTI-DELETE PROCESSOR
// ══════════════════════════════════════════════════════════════

async function processDeleted(socket, sn, cache, key) {
  try {
    const uc2     = await getCachedConfig(sn);
    if (uc2.ANTI_DELETE_MSG !== 'enable') return;
    const userJid = jidNormalizedUser(socket.user.id);
    const bn      = uc2.botName || config.BOT_NAME;
    const dt      = slTime();
    const cm      = cache.get(key.id);

    if (cm) {
      const text   = extractText(cm.message);
      const tLabel = typeLabel(cm.message);
      const sndr   = cm.pushName || (cm.key?.participant || cm.key?.remoteJid || '').split('@')[0];
      const info   = `*🗑️ DELETED MESSAGE*\n━━━━━━━━━━━━\nChat: ${cm.key?.remoteJid?.endsWith('@g.us') ? 'Group' : 'Private'}\nSender: ${sndr}\nType: ${tLabel}\nDeleted: ${dt}${text ? '\n\n' + text : ''}\n━━━━━━━━━━━━\n> *${bn}*`;
      const mt     = getContentType(cm.message);
      let sent     = false;
      if (mt && MEDIA_TYPES.includes(mt)) {
        try {
          const media = await downloadMedia(cm.message);
          if (media?.buffer) {
            await socket.sendMessage(userJid, { text: info });
            if (mt === 'imageMessage')    await socket.sendMessage(userJid, { image: media.buffer, mimetype: media.mime, caption: '🖼️ Deleted image' });
            else if (mt === 'videoMessage')    await socket.sendMessage(userJid, { video: media.buffer, mimetype: media.mime, caption: '🎥 Deleted video' });
            else if (mt === 'audioMessage')    await socket.sendMessage(userJid, { audio: media.buffer, mimetype: media.mime, ptt: media.ptt });
            else if (mt === 'stickerMessage')  await socket.sendMessage(userJid, { sticker: media.buffer });
            else if (mt === 'documentMessage') await socket.sendMessage(userJid, { document: media.buffer, mimetype: media.mime, fileName: media.fileName || 'file' });
            sent = true;
          }
        } catch (_) {}
      }
      if (!sent) await socket.sendMessage(userJid, { image: { url: config.RCD_IMAGE_PATH }, caption: info });
      cache.delete(key.id);
    } else {
      await socket.sendMessage(userJid, { image: { url: config.RCD_IMAGE_PATH }, caption: `*🗑️ MESSAGE DELETED*\nFrom: ${key.remoteJid}\nTime: ${dt}\n_Content not cached_\n\n> *${bn}*` });
    }
  } catch (e) { console.error('Anti-delete:', e.message); }
}

// ══════════════════════════════════════════════════════════════
//  SECTION 8 — SESSION LIFECYCLE
// ══════════════════════════════════════════════════════════════

async function deleteSession(sn, socketInst) {
  try {
    const sp = path.join(os.tmpdir(), `abdx_${sn}`);
    try { if (fs.existsSync(sp)) fs.removeSync(sp); } catch (_) {}
    activeSockets.delete(sn);
    socketCreationTime.delete(sn);
    msgCache.delete(sn);
    settingsTracker.delete(sn);
    menuTracker.delete(sn);
    songTracker.delete(sn);
    configCache.delete(sn);
    if (credsSaveTimers.has(sn)) { clearTimeout(credsSaveTimers.get(sn)); credsSaveTimers.delete(sn); }
    await removeCreds(sn);
    await removeNumber(sn);
    try {
      if (socketInst?.sendMessage)
        await socketInst.sendMessage(`${numOnly(config.OWNER_NUMBER)}@s.whatsapp.net`, {
          image: { url: config.RCD_IMAGE_PATH },
          caption: fmt('SESSION REMOVED', `+${sn}\nActive: ${activeSockets.size}`, config.BOT_NAME_FANCY),
        });
    } catch (_) {}
  } catch (_) {}
}

async function startSession(number, res) {
  const sn = numOnly(number);
  const sp = path.join(os.tmpdir(), `abdx_${sn}`);
  await initMongo().catch(() => {});

  // Restore creds from Mongo → temp directory
  try {
    const doc = await loadCreds(sn);
    if (doc?.creds) {
      fs.ensureDirSync(sp);
      fs.writeFileSync(path.join(sp, 'creds.json'), JSON.stringify(doc.creds, null, 2));
      if (doc.keys) fs.writeFileSync(path.join(sp, 'keys.json'), JSON.stringify(doc.keys, null, 2));
    }
  } catch (e) { console.error('[restore creds]', e.message); }

  const { state, saveCreds: saveLocalCreds } = await useMultiFileAuthState(sp);
  const logger = pino({ level: 'fatal' });

  try {
    const socket = makeWASocket({
      auth: {
        creds: state.creds,
        keys : makeCacheableSignalKeyStore(state.keys, logger),
      },
      printQRInTerminal          : false,
      connectTimeoutMs           : 60_000,
      defaultQueryTimeoutMs      : 0,
      keepAliveIntervalMs        : 15_000,   // slightly longer — reduces idle traffic
      markOnlineOnConnect        : true,
      emitOwnEvents              : true,
      syncFullHistory            : false,    // huge memory saving
      fireInitQueries            : true,
      generateHighQualityLinkPreview: false, // saves ~10-20 ms per message + memory
      getMessage                 : async key => {
        // Return cached message for retries instead of re-fetching from server
        const cached = getMsgCache(sn).get(key.id);
        return cached?.message ?? undefined;
      },
      version  : [2, 3000, 1033105955],
      browser  : ['Ubuntu', 'Chrome', '20.0.04'],
      logger,
    });

    socketCreationTime.set(sn, Date.now());
    attachHandlers(socket, sn);

    if (!socket.authState.creds.registered) {
      if (pairingLocks.has(sn)) {
        if (!res?.headersSent) res?.status(429).send({ error: 'Pairing already in progress for this number' });
        return;
      }
      pairingLocks.add(sn);
      let code, retries = config.MAX_RETRIES;
      while (retries-- > 0) {
        try { await delay(1500); code = await socket.requestPairingCode(sn); if (code) break; }
        catch (e) { console.error('[pairing attempt]', e.message); await delay(3000); }
      }
      pairingLocks.delete(sn);
      if (!code) {
        try { socket.ws?.close(); } catch (_) {}
        if (!res?.headersSent) res?.status(503).send({ error: 'Could not get pairing code. Try again.' });
        return;
      }
      if (!res?.headersSent) res?.send({ code });
    } else {
      if (!res?.headersSent) res?.send({ status: 'reconnecting' });
    }

    // ── Creds update ──────────────────────────────────────────────
    socket.ev.on('creds.update', async () => {
      try { await saveLocalCreds(); } catch (e) { console.error('[creds local save]', e.message); }
      debouncedCredsSave(sn, saveLocalCreds, () => state, sp);
    });

    // ── Connection state machine ──────────────────────────────────
    socket.ev.on('connection.update', async update => {
      const { connection, lastDisconnect } = update;

      if (connection === 'open') {
        try {
          await delay(2000);
          const userJid = jidNormalizedUser(socket.user.id);
          activeSockets.set(sn, socket);
          await addNumber(sn);
          console.log(`✅ Session active: +${sn}`);

          const gr  = await joinGroup(socket).catch(() => ({ status: 'failed', error: 'N/A' }));
          // Follow newsletters in background — don't block connection
          listNewsletters().then(list => {
            for (const d of list) {
              if (typeof socket.newsletterFollow === 'function') socket.newsletterFollow(d.jid).catch(() => {});
            }
          }).catch(() => {});

          const uc2 = await loadConfig(sn) || {};
          const bn2 = uc2.botName || config.BOT_NAME_FANCY;
          const ul  = (uc2.logo || '').startsWith('http') ? uc2.logo : config.RCD_IMAGE_PATH;

          try {
            await socket.sendMessage(userJid, {
              image: { url: ul },
              caption: fmt(bn2, `🟢 *Connected*\nNumber: +${sn}\nGroup: ${gr.status === 'success' ? '✅ Joined' : '⚠️ ' + gr.error}\nTime: ${slTime()}\n\nUse *${config.PREFIX}menu* for commands.`, bn2),
            });
          } catch (_) {
            try { await socket.sendMessage(userJid, { text: `✅ ${bn2} is now active! Use ${config.PREFIX}menu for commands.` }); } catch (_2) {}
          }
        } catch (e) { console.error('[connection open]', e.message); }
      }

      if (connection === 'close') {
        const code      = lastDisconnect?.error?.output?.statusCode || lastDisconnect?.error?.statusCode;
        const reason    = String(lastDisconnect?.error || '');
        const loggedOut = code === 401 || reason.includes('logged out') || reason.includes('Forbidden');
        const badSess   = code === 500 || reason.includes('bad session');
        console.log(`[close] +${sn} code=${code} loggedOut=${loggedOut}`);
        activeSockets.delete(sn);
        socketCreationTime.delete(sn);

        if (loggedOut || badSess) {
          console.log(`[session end] removing +${sn}`);
          try { fs.removeSync(sp); } catch (_) {}
          await deleteSession(sn, null);
        } else {
          console.log(`[reconnect] +${sn} in 8s...`);
          await delay(8000);
          const mock = { headersSent: true, send: () => {}, status: () => mock };
          try { await startSession(number, mock); } catch (e) { console.error('[reconnect error]', e.message); }
        }
      }
    });
  } catch (e) {
    console.error('[startSession]', e.message);
    pairingLocks.delete(sn);
    if (!res?.headersSent) res?.status(503).send({ error: 'Service unavailable: ' + e.message });
  }
}

// ══════════════════════════════════════════════════════════════
//  SECTION 9 — ROUTER ENDPOINTS
// ══════════════════════════════════════════════════════════════

router.get('/', async (req, res) => {
  const { number } = req.query;
  if (!number) return res.status(400).send({ error: 'number required' });
  const sn = numOnly(number);
  if (activeSockets.has(sn)) return res.send({ status: 'already_connected' });
  await startSession(number, res);
});

router.get('/ping',   (_req, res) => res.send({ status: 'ok', bot: config.BOT_NAME_FANCY, sessions: activeSockets.size, time: slTime() }));
router.get('/active', (_req, res) => res.send({ bot: config.BOT_NAME_FANCY, count: activeSockets.size, numbers: Array.from(activeSockets.keys()), time: slTime() }));

router.get('/connect-all', async (_req, res) => {
  const nums = await getAllNumbers();
  if (!nums.length) return res.status(404).send({ error: 'No saved sessions' });
  const r = [];
  for (const n of nums) {
    if (activeSockets.has(n)) { r.push({ number: n, status: 'connected' }); continue; }
    const mock = { headersSent: false, send: () => {}, status: () => mock };
    await startSession(n, mock);
    r.push({ number: n, status: 'initiated' });
  }
  res.send({ ok: true, connections: r });
});

router.get('/reconnect', async (_req, res) => {
  const nums = await getAllNumbers();
  if (!nums.length) return res.status(404).send({ error: 'No sessions' });
  const r = [];
  for (const n of nums) {
    if (activeSockets.has(n)) { r.push({ number: n, status: 'connected' }); continue; }
    const mock = { headersSent: false, send: () => {}, status: () => mock };
    try { await startSession(n, mock); r.push({ number: n, status: 'initiated' }); }
    catch (_) { r.push({ number: n, status: 'failed' }); }
    await delay(1000);
  }
  res.send({ ok: true, connections: r });
});

router.post('/newsletter/add',    async (req, res) => { const { jid, emojis } = req.body; if (!jid?.endsWith('@newsletter')) return res.status(400).send({ error: 'invalid jid' }); try { await addNewsletter(jid, emojis || []); res.send({ ok: true, jid }); } catch (e) { res.status(500).send({ error: e.message }); } });
router.post('/newsletter/remove', async (req, res) => { if (!req.body.jid) return res.status(400).send({ error: 'jid required' }); try { await removeNewsletter(req.body.jid); res.send({ ok: true }); } catch (e) { res.status(500).send({ error: e.message }); } });
router.get('/newsletter/list',    async (_req, res) => { try { res.send({ ok: true, channels: await listNewsletters() }); } catch (e) { res.status(500).send({ error: e.message }); } });

router.post('/admin/add',    async (req, res) => { if (!req.body.jid) return res.status(400).send({ error: 'jid required' }); try { await addAdmin(req.body.jid); res.send({ ok: true }); } catch (e) { res.status(500).send({ error: e.message }); } });
router.post('/admin/remove', async (req, res) => { if (!req.body.jid) return res.status(400).send({ error: 'jid required' }); try { await removeAdmin(req.body.jid); res.send({ ok: true }); } catch (e) { res.status(500).send({ error: e.message }); } });
router.get('/admin/list',    async (_req, res) => { try { res.send({ ok: true, admins: await getAdmins() }); } catch (e) { res.status(500).send({ error: e.message }); } });

router.post('/session/delete', async (req, res) => {
  try {
    const { number } = req.body;
    if (!number) return res.status(400).json({ ok: false });
    const sn = numOnly('' + number);
    const s  = activeSockets.get(sn);
    if (s) { try { await s.logout?.(); } catch (_) {} try { s.ws?.close(); } catch (_) {} }
    await deleteSession(sn, null);
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});

router.get('/api/sessions', async (_req, res) => {
  try { await initMongo(); res.json({ ok: true, sessions: await Col.sessions.find({}, { projection: { number: 1, updatedAt: 1 } }).sort({ updatedAt: -1 }).toArray() }); }
  catch (e) { res.status(500).json({ ok: false, error: e.message }); }
});
router.get('/api/active', (_req, res) => res.json({ ok: true, active: Array.from(activeSockets.keys()), count: activeSockets.size }));

// ══════════════════════════════════════════════════════════════
//  SECTION 10 — PROCESS GUARDS & BOOT
// ══════════════════════════════════════════════════════════════

process.on('exit', () => activeSockets.forEach(s => { try { s.ws.close(); } catch (_) {} }));
process.on('uncaughtException',  err    => console.error('[uncaughtException]',  err.message, err.stack));
process.on('unhandledRejection', reason => console.error('[unhandledRejection]', reason));

// ── Restore all sessions on boot ──────────────────────────────
initMongo()
  .then(async () => {
    console.log('[boot] Mongo ready — restoring sessions...');
    try {
      const nums = await getAllNumbers();
      console.log(`[boot] Found ${nums.length} saved session(s)`);
      for (const n of nums) {
        if (activeSockets.has(n)) continue;
        const mock = { headersSent: false, send: () => {}, status: () => mock };
        await startSession(n, mock);
        await delay(600);   // stagger reconnections to avoid hammering WA servers
      }
    } catch (e) { console.error('[boot restore]', e.message); }
  })
  .catch(e => console.error('❌ Mongo boot failed:', e.message));

module.exports = router;