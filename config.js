// ╔══════════════════════════════════════════╗
// ║       Abdu-xx Bot — config.js            ║
// ║  All values read from env vars first     ║
// ╚══════════════════════════════════════════╝
'use strict';

module.exports = {
  // ── Identity ──────────────────────────────
  BOT_NAME        : process.env.BOT_NAME         || 'Abdu-xx',
  BOT_NAME_FANCY  : process.env.BOT_NAME_FANCY   || 'Abdu-xx Bot',
  BOT_VERSION     : '1.0',
  BOT_FOOTER      : '© ᴘᴏᴡᴇʀᴇᴅ ʙʏ ᴀʙᴅᴜ-xx',

  // ── Owner ─────────────────────────────────
  OWNER_NUMBER : process.env.OWNER_NUMBER || '94776803526',
  OWNER_NAME   : process.env.OWNER_NAME   || 'Abdul Kalam',

  // ── Developers ────────────────────────────
  DEVELOPERS: {
    '94743209781': '🎀',
    '94776803526': '🥤',
  },

  // ── Defaults ──────────────────────────────
  PREFIX          : process.env.PREFIX || '.',
  WORK_TYPE       : 'private',
  AUTO_VIEW_STATUS: 'true',
  AUTO_LIKE_STATUS: 'true',
  AUTO_RECORDING  : 'false',
  AUTO_TYPING     : 'false',
  MAX_RETRIES     : 3,
  OTP_EXPIRY      : 300_000,
  AUTO_LIKE_EMOJI : ['🎀','🧃','🪼','🦄','🍒','🍫','🧸','☁️','🌟','👀','💎'],

  // ── Links & Media ─────────────────────────
  GROUP_INVITE_LINK : process.env.GROUP_LINK   || 'https://chat.whatsapp.com/JaHC5S98mjIK6STcWf25rh?mode=gi_c',
  CHANNEL_LINK      : process.env.CHANNEL_LINK || 'https://whatsapp.com/channel/0029Vb6yaNMIt5s3s5iUK51g',
  NEWSLETTER_JID    : process.env.NEWSLETTER_JID || '120363424595683472@newsletter',
  RCD_IMAGE_PATH    : process.env.BOT_IMAGE    || 'https://files.catbox.moe/8hd3b3.jpg',
  IMAGE_PATH        : process.env.BOT_IMAGE    || 'https://files.catbox.moe/8hd3b3.jpg',

  // ── MongoDB ───────────────────────────────
  MONGO_URI : process.env.MONGO_URI || '',
  MONGO_DB  : process.env.MONGO_DB  || 'Abduxx',

  // ── Default per-user config ───────────────
  DEFAULT_USER_CONFIG: {
    WORK_TYPE          : 'private',
    FAKE_PRESENCE      : 'off',
    PRESENCE           : 'available',
    STATUS_VIEW        : 'view',
    AUTO_VIEW_STATUS   : 'true',
    AUTO_LIKE_STATUS   : 'false',
    AUTO_READ_MESSAGE  : 'off',
    ANTI_CALL          : 'off',
    ANTI_DELETE_MSG    : 'disable',
    ANTI_DELETE_STATUS : 'disable',
    PREFIX_ENABLED     : 'on',
    AUTO_TYPING        : 'false',
    AUTO_RECORDING     : 'false',
    AUTO_REPLY         : 'disable',
    AUTO_LIKE_EMOJI    : ['🎀','🧃','🪼','🦄','🍒','🍫','🧸','☁️','🌟','👀','💎'],
    botName            : 'Abdu-xx',
    ownerName          : process.env.OWNER_NAME || 'Abdul Kalam',
    ownerDetails       : '',
    likeEmoji          : '❤️',
    logo               : '',
  },

  // ── Settings map ──────────────────────────
  SETTINGS_MAP: {
    '1.1': { key: 'WORK_TYPE',          value: 'public',       label: 'Public' },
    '1.2': { key: 'WORK_TYPE',          value: 'inbox',        label: 'Inbox Only' },
    '1.3': { key: 'WORK_TYPE',          value: 'groups',       label: 'Groups Only' },
    '1.4': { key: 'WORK_TYPE',          value: 'group_admins', label: 'Group Admins Only' },
    '1.5': { key: 'WORK_TYPE',          value: 'channel',      label: 'Channel Only' },
    '1.6': { key: 'WORK_TYPE',          value: 'private',      label: 'Private (Owner Only)' },
    '2.1': { key: 'FAKE_PRESENCE',      value: 'typing',       label: 'Fake Typing',    extra: { AUTO_TYPING: 'true',  AUTO_RECORDING: 'false' } },
    '2.2': { key: 'FAKE_PRESENCE',      value: 'recording',    label: 'Fake Recording', extra: { AUTO_TYPING: 'false', AUTO_RECORDING: 'true'  } },
    '2.3': { key: 'FAKE_PRESENCE',      value: 'off',          label: 'Off',            extra: { AUTO_TYPING: 'false', AUTO_RECORDING: 'false' } },
    '3.1': { key: 'PRESENCE',           value: 'available',    label: 'Always Online' },
    '3.2': { key: 'PRESENCE',           value: 'unavailable',  label: 'Always Offline' },
    '4.1': { key: 'STATUS_VIEW',        value: 'view',         label: 'View Only',   extra: { AUTO_VIEW_STATUS: 'true',  AUTO_LIKE_STATUS: 'false' } },
    '4.2': { key: 'STATUS_VIEW',        value: 'view_like',    label: 'View + Like', extra: { AUTO_VIEW_STATUS: 'true',  AUTO_LIKE_STATUS: 'true'  } },
    '4.3': { key: 'STATUS_VIEW',        value: 'off',          label: 'Off',         extra: { AUTO_VIEW_STATUS: 'false', AUTO_LIKE_STATUS: 'false' } },
    '5.1': { key: 'AUTO_READ_MESSAGE',  value: 'all',          label: 'Read All' },
    '5.2': { key: 'AUTO_READ_MESSAGE',  value: 'cmd',          label: 'Commands Only' },
    '5.3': { key: 'AUTO_READ_MESSAGE',  value: 'off',          label: 'Off' },
    '6.1': { key: 'ANTI_CALL',          value: 'on',           label: 'Reject Call' },
    '6.2': { key: 'ANTI_CALL',          value: 'reject_msg',   label: 'Reject + Message' },
    '6.3': { key: 'ANTI_CALL',          value: 'off',          label: 'Off' },
    '7.1': { key: 'ANTI_DELETE_MSG',    value: 'enable',       label: 'Enable' },
    '7.2': { key: 'ANTI_DELETE_MSG',    value: 'disable',      label: 'Disable' },
    '8.1': { key: 'ANTI_DELETE_STATUS', value: 'enable',       label: 'Enable' },
    '8.2': { key: 'ANTI_DELETE_STATUS', value: 'disable',      label: 'Disable' },
    '9.1': { key: 'PREFIX_ENABLED',     value: 'on',           label: 'Prefix On' },
    '9.2': { key: 'PREFIX_ENABLED',     value: 'off',          label: 'Prefix Off' },
    '10.1':{ key: 'AUTO_REPLY',         value: 'enable',       label: 'Auto Reply On' },
    '10.2':{ key: 'AUTO_REPLY',         value: 'disable',      label: 'Auto Reply Off' },
  },
};