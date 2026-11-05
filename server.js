'use strict';
/*
 * BiliTools-Lite 后端（桌面版）
 * 零第三方运行时依赖：Node 原生 http / fs / crypto / child_process / node:sqlite
 *
 * 在原有 bili-downloader 基础上补齐三块 BiliTools 式能力：
 *   1) SQLite 持久化任务队列（重启后自动续跑、历史可查）
 *   2) aria2c RPC 多连接下载（失败自动回退单连接 fetch）
 *   3) B 站二维码登录 + Cookie 本地持久化
 *
 * 接口：
 *   GET  /api/health
 *   GET  /api/parse?url=&cookie=&page=
 *   GET  /api/cover?url=
 *   POST /api/download           {videoUrl,audioUrl,filename,cookie,engine}
 *   POST /api/download-all      {url,cookie,qn,engine}
 *   GET  /api/batch?ids=
 *   GET  /api/progress?job=     (SSE)
 *   GET  /api/file?job=
 *   GET  /api/jobs              历史任务
 *   GET  /api/open-folder?path= 在资源管理器中定位成品
 *   GET  /api/engine           下载引擎状态
 *   GET  /api/auth             已保存的登录态
 *   POST /api/auth/logout
 *   GET  /api/qr/generate
 *   GET  /api/qr/poll?key=
 */

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { spawn, execFile } = require('child_process');

let DatabaseSync = null;
try { ({ DatabaseSync } = require('node:sqlite')); } catch {}

const PORT0 = Number(process.env.PORT || 3000);
const HOST = '127.0.0.1';
const ROOT = __dirname;
const PUBLIC_DIR = path.join(ROOT, 'public');
const TMP_DIR = path.join(ROOT, 'tmp');
const DL_DIR = path.join(ROOT, 'downloads');
// 持久化开关：默认关闭（不在磁盘写任何状态文件）。
// 想保留登录态与下载历史时，启动时设置环境变量 BILI_PERSIST=1（或 true）。
const PERSIST = process.env.BILI_PERSIST === '1' || process.env.BILI_PERSIST === 'true';
const DATA_DIR = PERSIST ? path.join(ROOT, 'data') : null;
for (const d of [TMP_DIR, DL_DIR, ...(PERSIST ? [DATA_DIR] : [])]) fs.mkdirSync(d, { recursive: true });

const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36';
const REFERER = 'https://www.bilibili.com';
const sleep = ms => new Promise(r => setTimeout(r, ms));

// ---------- 外部工具定位（优先复用 BiliTools 自带二进制） ----------
function firstExisting(candidates) {
  for (const p of candidates) { try { if (p && fs.existsSync(p)) return p; } catch {} }
  return null;
}
const ARIA2_EXE = firstExisting([
  process.env.ARIA2_EXE,
  path.join(ROOT, 'bin', 'aria2c.exe'),
  'D:/MyBuddy/BiliTools/aria2c.exe',
  'D:/MyBuddy/BiliTools/aria2c'
]);
const FFMPEG_EXE = firstExisting([
  process.env.FFMPEG_EXE,
  path.join(ROOT, 'bin', 'ffmpeg.exe'),
  'D:/MyBuddy/BiliTools/ffmpeg.exe'
]) || 'ffmpeg';

// ---------- SQLite 持久化 ----------
let db = null;
const stmts = {};
if (DatabaseSync) {
  try {
    // 持久化关闭时使用内存数据库：所有状态只存内存、完全不落盘
    db = PERSIST ? new DatabaseSync(path.join(DATA_DIR, 'jobs.db')) : new DatabaseSync(':memory:');
    db.exec(`CREATE TABLE IF NOT EXISTS jobs (
      id TEXT PRIMARY KEY, req TEXT NOT NULL, stage TEXT, percent INTEGER,
      done INTEGER, error TEXT, out_path TEXT, created_at INTEGER, updated_at INTEGER
    )`);
    db.exec('CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT)');
    stmts.upsert = db.prepare('INSERT OR REPLACE INTO jobs (id,req,stage,percent,done,error,out_path,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?)');
    stmts.list = db.prepare('SELECT * FROM jobs ORDER BY created_at DESC LIMIT 100');
    stmts.pending = db.prepare('SELECT * FROM jobs WHERE done=0 ORDER BY created_at ASC');
    stmts.kvGet = db.prepare('SELECT v FROM kv WHERE k=?');
    stmts.kvSet = db.prepare('INSERT OR REPLACE INTO kv (k,v) VALUES (?,?)');
    stmts.kvDel = db.prepare('DELETE FROM kv WHERE k=?');
  } catch (e) { console.warn('[db] SQLite 不可用，退回内存队列:', e.message); db = null; }
}
function kvGet(k) { try { return db ? (stmts.kvGet.get(k) || {}).v || '' : ''; } catch { return ''; } }
function kvSet(k, v) { try { if (db) stmts.kvSet.run(k, v); } catch {} }
function kvDel(k) { try { if (db) stmts.kvDel.run(k); } catch {} }

function persist(job, force = true) {
  if (!db) return;
  const now = Date.now();
  if (!force && job._lastWrite && now - job._lastWrite < 600) return;
  job._lastWrite = now;
  try {
    stmts.upsert.run(job.id, JSON.stringify(job.req), job.stage, job.percent | 0,
      job.done ? 1 : 0, job.error || '', job.outPath || '', job.createdAt || now, now);
  } catch (e) { console.warn('[db] 写入失败:', e.message); }
}
function updateJob(job, patch, force = true) {
  Object.assign(job, patch);
  persist(job, force);
}
function hydrate(row) {
  let req = {};
  try { req = JSON.parse(row.req); } catch {}
  return {
    id: row.id, req, stage: row.stage, percent: row.percent || 0,
    done: !!row.done, error: row.error || '', outPath: row.out_path || '',
    createdAt: row.created_at, _lastWrite: Date.now()
  };
}

// ---------- WBI 签名 ----------
const MIXIN_KEY_ENC_TAB = [
  46,47,18,2,53,8,23,32,15,50,10,31,58,3,45,35,27,43,5,49,33,9,42,19,29,28,14,
  39,12,38,41,13,37,48,7,16,24,55,40,61,26,17,0,1,57,22,25,11,52,34,44,59,20,
  54,6,51,21,4,36,56,60,62,63
];
function getMixinKey(orig) { return MIXIN_KEY_ENC_TAB.map(i => orig[i]).join('').slice(0, 32); }
function md5(s) { return crypto.createHash('md5').update(s).digest('hex'); }

async function getNavMixinKey(cookie) {
  const headers = { 'User-Agent': UA, 'Referer': REFERER };
  if (cookie) headers['Cookie'] = cookie;
  const r = await fetch('https://api.bilibili.com/x/web-interface/nav', { headers });
  const j = await r.json();
  if (!j.data || !j.data.wbi_img) throw new Error('nav 接口失败: ' + (j.message || '无 wbi_img'));
  const img = j.data.wbi_img.img_url.split('/').pop().split('.')[0];
  const sub = j.data.wbi_img.sub_url.split('/').pop().split('.')[0];
  return getMixinKey(img + sub);
}
function wbiSign(params, mixinKey) {
  const sorted = Object.keys(params).sort()
    .map(k => `${encodeURIComponent(k)}=${encodeURIComponent(params[k])}`).join('&');
  return sorted + '&w_rid=' + md5(sorted + mixinKey);
}

// ---------- Bilibili API ----------
const biliHeaders = cookie => {
  const h = { 'User-Agent': UA, 'Referer': REFERER };
  if (cookie) h['Cookie'] = cookie;
  return h;
};
async function getView(bvid, cookie) {
  const r = await fetch(`https://api.bilibili.com/x/web-interface/view?bvid=${bvid}`, { headers: biliHeaders(cookie) });
  const j = await r.json();
  if (j.code !== 0) throw new Error('view 接口失败: ' + j.message);
  return j.data;
}
async function getPlayUrl(bvid, cid, cookie) {
  const mk = await getNavMixinKey(cookie);
  const params = { bvid, cid, qn: 127, fnval: 16, fourk: 1, wts: Math.floor(Date.now() / 1000) };
  const qs = wbiSign(params, mk);
  const r = await fetch(`https://api.bilibili.com/x/player/wbi/playurl?${qs}`, { headers: biliHeaders(cookie) });
  const j = await r.json();
  if (j.code !== 0) throw new Error('playurl 接口失败: ' + j.message);
  return j.data;
}
function parseInput(url) {
  const s = String(url || '');
  let m;
  if ((m = s.match(/ep(\d+)/))) return { type: 'bangumi', epId: m[1] };
  if ((m = s.match(/ss(\d+)/))) return { type: 'bangumi', ssId: m[1] };
  if ((m = s.match(/BV[0-9A-Za-z]+/))) return { type: 'video', bvid: m[0] };
  return { type: 'unknown' };
}
// pgc/view/web/season 覆盖番剧、电影、电视剧、纪录片等所有 PGC 内容，用 type 区分
const PGC_TYPES = { 1: '番剧', 2: '电影', 3: '纪录片', 4: '国创', 5: '电视剧', 7: '综艺' };
function pgcTypeName(t) { return PGC_TYPES[Number(t)] || '影视'; }
function pgcUnit(t) { return Number(t) === 2 ? '版本' : (Number(t) === 7 ? '期' : '集'); }
// pgc/view/web/season 的 duration 是毫秒，x/web-interface/view 是秒。
// 单集时长不可能超过 100000 秒（约 27 小时），据此区分并统一换算成秒。
function toSeconds(d) {
  const n = Number(d) || 0;
  return n > 100000 ? Math.round(n / 1000) : Math.round(n);
}
async function getPgcSeason({ epId, ssId }, cookie) {
  const qs = epId ? `ep_id=${epId}` : `season_id=${ssId}`;
  const r = await fetch(`https://api.bilibili.com/pgc/view/web/season?${qs}`, { headers: biliHeaders(cookie) });
  const j = await r.json();
  if (j.code !== 0) throw new Error('番剧信息获取失败: ' + (j.message || j.code));
  const res = j.result || {};
  const episodes = (res.episodes || []).map(e => ({
    epId: e.id, cid: e.cid, aid: e.aid, bvid: e.bvid, index: e.index,
    title: e.long_title || e.index_title || e.title || (e.index != null ? `第${e.index}集` : `ep${e.id}`),
    duration: toSeconds(e.duration), cover: e.cover || e.pic || res.cover || res.pic || ''
  }));
  if (!episodes.length) throw new Error('该内容没有可播放的分集');
  let cur = epId ? episodes.find(e => String(e.epId) === String(epId)) : episodes[0];
  if (!cur) cur = episodes[0];
  return {
    title: res.title || res.season_title || '影视', cover: res.cover || '', episodes,
    current: cur, session: res.session || '', seasonId: res.season_id || ssId || '',
    mediaId: res.media_id || '', pgcType: Number(res.type) || 0,
    typeName: pgcTypeName(res.type), unit: pgcUnit(res.type)
  };
}
async function getBangumiPlayUrl(aid, cid, epId, session, cookie) {
  const mk = await getNavMixinKey(cookie);
  const params = { avid: aid, cid, ep_id: epId, qn: 127, fnval: 16, fourk: 1, wts: Math.floor(Date.now() / 1000) };
  if (session) params.session = session;
  const qs = wbiSign(params, mk);
  const r = await fetch(`https://api.bilibili.com/pgc/player/web/playurl?${qs}`, { headers: biliHeaders(cookie) });
  const j = await r.json();
  if (j.code !== 0) throw new Error('番剧 playurl 失败: ' + (j.message || j.code));
  return j.result;
}

// ---------- 二维码登录 ----------
let buvidCache = '';
async function getBuvid() {
  if (buvidCache) return buvidCache;
  try {
    const r = await fetch('https://api.bilibili.com/x/frontend/finger/spi', { headers: biliHeaders('') });
    const j = await r.json();
    const d = j.data || {};
    buvidCache = [d.b_3 && 'buvid3=' + d.b_3, d.b_4 && 'buvid4=' + d.b_4].filter(Boolean).join('; ');
  } catch { buvidCache = ''; }
  return buvidCache;
}
async function qrGenerate() {
  const buvid = await getBuvid();
  const r = await fetch('https://passport.bilibili.com/x/passport-login/web/qrcode/generate?source=main-fe-header',
    { headers: biliHeaders(buvid) });
  const j = await r.json();
  if (j.code !== 0 || !j.data) throw new Error('二维码生成失败: ' + j.message);
  return j.data; // { url, qrcode_key }
}
async function qrPoll(key) {
  const buvid = await getBuvid();
  const r = await fetch(`https://passport.bilibili.com/x/passport-login/web/qrcode/poll?qrcode_key=${encodeURIComponent(key)}&source=main-fe-header`,
    { headers: biliHeaders(buvid) });
  const j = await r.json();
  const d = j.data || {};
  const code = d.code;
  if (code === 0) {
    // 新版接口通过 Set-Cookie 下发凭证，旧版把参数拼在 url 上，两种都兼容
    let cookie = '';
    try { cookie = (r.headers.getSetCookie() || []).map(c => c.split(';')[0]).join('; '); } catch {}
    if (!cookie && d.url) {
      try {
        const q = new URL(d.url).searchParams;
        cookie = ['SESSDATA', 'bili_jct', 'DedeUserID', 'DedeUserID__ckMd5', 'sid']
          .filter(k => q.get(k)).map(k => `${k}=${q.get(k)}`).join('; ');
      } catch {}
    }
    const full = [buvid, cookie].filter(Boolean).join('; ');
    if (full) kvSet('cookie', full);
    return { status: 'success', cookie: full };
  }
  const map = { 86038: 'expired', 86090: 'scanned', 86101: 'waiting' };
  return { status: map[code] || 'waiting', message: d.message || '' };
}

// ---------- aria2c RPC 下载引擎 ----------
const ARIA2_PORT = Number(process.env.ARIA2_PORT || 6800);
const ARIA2_SECRET = crypto.randomBytes(12).toString('hex');
let aria2Proc = null;
let aria2Ready = null;

function aria2Rpc(method, params = []) {
  return fetch(`http://127.0.0.1:${ARIA2_PORT}/jsonrpc`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', id: String(Date.now()), method, params: ['token:' + ARIA2_SECRET, ...params] })
  }).then(r => r.json());
}
function ensureAria2() {
  if (!ARIA2_EXE) return Promise.resolve(null);
  if (aria2Ready) return aria2Ready;
  aria2Ready = new Promise(resolve => {
    const args = [
      '--enable-rpc', '--rpc-listen-all=false', `--rpc-listen-port=${ARIA2_PORT}`,
      `--rpc-secret=${ARIA2_SECRET}`, '--disable-ipv6=true', '--no-conf=true',
      '--log-level=warn',
      '--continue=true', '--max-connection-per-server=8', '--split=8', '--min-split-size=1M',
      '--max-concurrent-downloads=4',
      `--dir=${TMP_DIR}`
    ];
    // 仅持久化模式才写 aria2 会话/日志文件；否则完全不落盘
    if (PERSIST) {
      const session = path.join(DATA_DIR, 'aria2.session');
      args.push('--save-session-interval=30', `--save-session=${session}`, `--log=${path.join(DATA_DIR, 'aria2.log')}`);
      if (fs.existsSync(session)) args.push(`--input-file=${session}`);
    }
    let proc;
    try { proc = spawn(ARIA2_EXE, args, { windowsHide: true, stdio: 'ignore' }); }
    catch (e) { console.warn('[aria2] 启动失败:', e.message); aria2Ready = null; return resolve(null); }
    aria2Proc = proc;
    proc.on('exit', () => { if (aria2Proc === proc) { aria2Proc = null; aria2Ready = null; } });
    (async () => {
      for (let i = 0; i < 30; i++) {
        try { const v = await aria2Rpc('aria2.getVersion'); if (v && v.result) return resolve(aria2Rpc); } catch {}
        await sleep(200);
      }
      console.warn('[aria2] RPC 未就绪，使用单连接回退');
      try { proc.kill(); } catch {}
      aria2Ready = null;
      resolve(null);
    })();
  });
  return aria2Ready;
}
async function aria2Download(job, urls, dests, cookie) {
  const rpc = await ensureAria2();
  if (!rpc) return false;
  const header = [
    'Referer: ' + REFERER,
    'Origin: https://www.bilibili.com',
    'User-Agent: ' + UA,
    'Accept: */*'
  ];
  if (cookie) header.push('Cookie: ' + cookie.replace(/[\r\n]+/g, ' '));
  const gids = [];
  try {
    for (let i = 0; i < urls.length; i++) {
      const res = await rpc('aria2.addUri', [[urls[i]], {
        dir: TMP_DIR, out: path.basename(dests[i]),
        header, continue: 'true', split: '8', 'max-connection-per-server': '8',
        'min-split-size': '1M', 'max-tries': '3', 'retry-wait': '2'
      }]);
      if (!res || !res.result) throw new Error('aria2.addUri 失败: ' + JSON.stringify(res && res.error));
      gids.push(res.result);
    }
    while (true) {
      let total = 0, done = 0, speed = 0, finished = 0;
      for (const gid of gids) {
        const res = await rpc('aria2.tellStatus', [gid]);
        const st = res && res.result;
        if (!st) throw new Error('aria2.tellStatus 无返回');
        if (st.status === 'error' || (st.errorCode && st.errorCode !== '0')) {
          throw new Error('aria2 下载失败: ' + (st.errorMessage || st.errorCode));
        }
        total += Number(st.totalLength) || 0;
        done += Number(st.completedLength) || 0;
        speed += Number(st.downloadSpeed) || 0;
        if (st.status === 'complete') finished++;
      }
      updateJob(job, {
        stage: `下载中 (aria2 ${(speed / 1048576).toFixed(1)} MB/s)`,
        percent: total ? Math.min(99, Math.round(done / total * 100)) : 0
      }, false);
      if (finished === gids.length) break;
      if (job._cancelled) throw new Error('任务已取消');
      await sleep(500);
    }
    return true;
  } catch (e) {
    console.warn('[aria2] 回退到单连接:', e.message);
    for (const gid of gids) { try { await rpc('aria2.forceRemove', [gid]); } catch {} }
    for (const d of dests) { try { fs.unlinkSync(d); } catch {} }
    return false;
  }
}

// ---------- 单连接回退下载 ----------
async function fetchStream(url, dest, cookie, job, stageLabel, onChunk) {
  const headers = { 'User-Agent': UA, 'Referer': REFERER, 'Accept': '*/*' };
  if (cookie) headers['Cookie'] = cookie;
  const r = await fetch(url, { headers });
  if (!r.ok) throw new Error(`${stageLabel} 下载失败 HTTP ${r.status}`);
  const total = Number(r.headers.get('content-length')) || 0;
  const file = fs.createWriteStream(dest);
  let got = 0;
  const reader = r.body.getReader();
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    file.write(Buffer.from(value));
    got += value.length;
    onChunk(got, total);
  }
  await new Promise((res, rej) => file.end(err => (err ? rej(err) : res())));
}

// ---------- 下载任务队列 ----------
const jobs = new Map();
const dlQueue = [];
const MAX_CONCURRENT = Number(process.env.MAX_CONCURRENT || 2);
let activeDl = 0;

function enqueueDownload(job) {
  updateJob(job, { stage: '排队中' });
  dlQueue.push(job);
  pumpQueue();
}
function pumpQueue() {
  while (activeDl < MAX_CONCURRENT && dlQueue.length) {
    const job = dlQueue.shift();
    activeDl++;
    runJob(job).finally(() => { activeDl--; pumpQueue(); });
  }
}
function newJobId() { return crypto.randomBytes(8).toString('hex'); }
function sanitize(name) {
  return String(name).replace(/[\\/:*?"<>|\n\r\t]+/g, '_').slice(0, 80) || 'bilibili';
}
const BILI_HOST_SUFFIX = ['bilibili.com', 'bilivideo.com', 'bilivideo.cn', 'akamaized.net', 'mcdn.bilivideo.cn', 'hdslb.com'];
function isBiliUrl(u) {
  try {
    const url = new URL(u);
    if (url.protocol !== 'http:' && url.protocol !== 'https:') return false;
    return BILI_HOST_SUFFIX.some(s => url.hostname === s || url.hostname.endsWith('.' + s));
  } catch { return false; }
}
function makeJob(req, id) {
  const job = {
    id: id || newJobId(), req, stage: '排队中', percent: 0, done: false,
    error: null, outPath: null, createdAt: Date.now(), _lastWrite: 0
  };
  jobs.set(job.id, job);
  persist(job);
  return job;
}

async function runJob(job) {
  const { videoUrl, audioUrl, filename, cookie, engine } = job.req;
  const mode = ['video', 'audio'].includes(job.req.mode) ? job.req.mode : 'both';
  const ext = mode === 'audio' ? '.m4a' : '.mp4';
  const safeName = sanitize(filename);
  const vidPath = path.join(TMP_DIR, job.id + '.video.m4s');
  const audPath = path.join(TMP_DIR, job.id + '.audio.m4s');
  const needVideo = mode !== 'audio', needAudio = mode !== 'video';
  let outPath = path.join(DL_DIR, safeName + ext);
  if (fs.existsSync(outPath)) outPath = path.join(DL_DIR, safeName + '_' + job.id.slice(0, 6) + ext);
  const urls = [], dests = [];
  if (needVideo) { urls.push(videoUrl); dests.push(vidPath); }
  if (needAudio) { urls.push(audioUrl); dests.push(audPath); }
  try {
    let usedAria2 = false;
    if (engine !== 'fetch') {
      updateJob(job, { stage: '连接 aria2…' });
      usedAria2 = await aria2Download(job, urls, dests, cookie);
    }
    if (!usedAria2) {
      const totals = new Array(urls.length).fill(0), got = new Array(urls.length).fill(0);
      const tick = () => {
        const total = totals.reduce((a, b) => a + b, 0), cur = got.reduce((a, b) => a + b, 0);
        updateJob(job, {
          stage: '下载中 (单连接)',
          percent: total ? Math.min(99, Math.round(cur / total * 100)) : 0
        }, false);
      };
      updateJob(job, { stage: '下载中 (单连接)' });
      await Promise.all(urls.map((u, i) => fetchStream(u, dests[i], cookie, job,
        needVideo && i === 0 ? '视频流' : '音频流',
        (g, t) => { got[i] = g; totals[i] = t; tick(); })));
    }
    updateJob(job, { stage: mode === 'both' ? '合并中 (ffmpeg)' : '封装中 (ffmpeg)', percent: 100 });
    const inputs = [];
    if (needVideo) inputs.push('-i', vidPath);
    if (needAudio) inputs.push('-i', audPath);
    await new Promise((res, rej) => {
      const p = spawn(FFMPEG_EXE, ['-y', ...inputs, '-c', 'copy', '-movflags', '+faststart', outPath]);
      let errBuf = '';
      p.stderr.on('data', d => (errBuf += d));
      p.on('error', rej);
      p.on('close', c => (c === 0 ? res() : rej(new Error('ffmpeg 退出码 ' + c + '\n' + errBuf.slice(-500)))));
    });
    for (const d of dests) { try { fs.unlinkSync(d); } catch {} }
    updateJob(job, { outPath, done: true, stage: '完成', percent: 100 });
  } catch (e) {
    updateJob(job, { error: e.message, done: true, stage: '失败', percent: 0 });
    for (const d of dests) { try { fs.unlinkSync(d); } catch {} }
  }
}

// 重启后恢复未完成的任务（BiliTools 的 doing 队列等价物）
function resumePending() {
  if (!db) return;
  try {
    const rows = stmts.pending.all();
    for (const row of rows) {
      const job = hydrate(row);
      jobs.set(job.id, job);
      updateJob(job, { stage: '排队中', percent: 0, done: false, error: '' });
      dlQueue.push(job);
    }
    if (rows.length) { pumpQueue(); console.log(`[queue] 恢复 ${rows.length} 个未完成任务`); }
  } catch (e) { console.warn('[queue] 恢复失败:', e.message); }
}

// ---------- HTTP 工具 ----------
function sendJSON(res, code, obj) {
  res.writeHead(code, {
    'Content-Type': 'application/json; charset=utf-8',
    'Access-Control-Allow-Origin': '*',
    'Cache-Control': 'no-store'
  });
  res.end(Buffer.from(JSON.stringify(obj)));
}
function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', c => (data += c));
    req.on('end', () => { try { resolve(data ? JSON.parse(data) : {}); } catch (e) { reject(e); } });
    req.on('error', reject);
  });
}
function serveStatic(res, filePath) {
  fs.readFile(filePath, (err, buf) => {
    if (err) { res.writeHead(404); res.end('Not found'); return; }
    const ext = path.extname(filePath);
    const mime = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png', '.svg': 'image/svg+xml', '.ico': 'image/x-icon' }[ext] || 'application/octet-stream';
    res.writeHead(200, { 'Content-Type': mime + (mime.startsWith('text/') ? '; charset=utf-8' : '') });
    res.end(buf);
  });
}

// ---------- 路由 ----------
const server = http.createServer(async (req, res) => {
  const u = new URL(req.url, `http://${req.headers.host}`);
  const p = u.pathname;
  try {
    if (p === '/api/health') return sendJSON(res, 200, { ok: true, aria2: !!ARIA2_EXE, ffmpeg: FFMPEG_EXE, sqlite: !!db, persist: PERSIST });

    if (p === '/api/engine') return sendJSON(res, 200, {
      ok: true,
      engines: [
        { id: 'aria2', name: 'aria2 多连接', available: !!ARIA2_EXE },
        { id: 'fetch', name: '单连接直连', available: true }
      ],
      sqlite: !!db, ffmpeg: FFMPEG_EXE, aria2Exe: ARIA2_EXE || ''
    });

    // ----- 登录 -----
    if (p === '/api/auth' && req.method === 'GET') {
      const c = kvGet('cookie');
      return sendJSON(res, 200, { ok: true, loggedIn: !!c, cookie: c });
    }
    if (p === '/api/auth/logout' && req.method === 'POST') {
      kvDel('cookie');
      return sendJSON(res, 200, { ok: true });
    }
    if (p === '/api/qr/generate') {
      const d = await qrGenerate();
      return sendJSON(res, 200, { ok: true, url: d.url, key: d.qrcode_key });
    }
    if (p === '/api/qr/poll') {
      const key = u.searchParams.get('key') || '';
      if (!key) return sendJSON(res, 400, { ok: false, error: '缺少 key' });
      return sendJSON(res, 200, { ok: true, ...(await qrPoll(key)) });
    }

    // ----- 解析 -----
    if (p === '/api/parse' && req.method === 'GET') {
      const url = u.searchParams.get('url') || '';
      const cookie = u.searchParams.get('cookie') || '';
      const pageIdx = u.searchParams.has('page') ? Number(u.searchParams.get('page')) : 0;
      const input = parseInput(url);
      if (input.type === 'unknown') {
        return sendJSON(res, 400, { ok: false, error: '无法识别链接，请使用 bilibili.com/video/BV... 或 bangumi/play/ep.../ss... 链接' });
      }
      let meta, play, idx = 0;
      if (input.type === 'video') {
        const view = await getView(input.bvid, cookie);
        const total = view.pages.length;
        idx = Math.max(0, Math.min(pageIdx | 0, total - 1));
        const page = view.pages[idx];
        play = await getPlayUrl(input.bvid, page.cid, cookie);
        meta = {
          title: total > 1 ? `${view.title} - P${idx + 1} ${page.part || ''}`.trim() : view.title,
          owner: view.owner ? view.owner.name : '', ownerLabel: 'UP',
          typeName: '视频', unit: 'P',
          mediaId: input.bvid, mediaTitle: 'B 站稿件 BV 号',
          episodeLabel: total > 1 ? `P${idx + 1} / 共${total}P` : '单P',
          cover: view.pic || '', duration: toSeconds(view.duration),
          pages: view.pages.map((pg, i) => ({ index: i, cid: pg.cid, part: pg.part || ('P' + (i + 1)), duration: toSeconds(pg.duration) }))
        };
      } else {
        const season = await getPgcSeason({ epId: input.epId, ssId: input.ssId }, cookie);
        const total = season.episodes.length;
        const isMovie = season.pgcType === 2;
        const unit = season.unit;
        idx = Math.max(0, Math.min(pageIdx | 0, total - 1));
        const ep = season.episodes[idx];
        play = await getBangumiPlayUrl(ep.aid, ep.cid, ep.epId, season.session, cookie);
        // 电影的多个「分集」通常是粤语/国语等版本，不叫「第几集」
        const epPart = total > 1
          ? (isMovie ? ` - ${ep.title}` : ` - 第${idx + 1}${unit} ${ep.title}`)
          : (ep.title ? ` - ${ep.title}` : '');
        meta = {
          title: `${season.title}${epPart}`,
          owner: season.typeName, ownerLabel: '类型',
          typeName: season.typeName, unit,
          mediaId: ep.bvid || (ep.epId ? 'ep' + ep.epId : (season.seasonId ? 'ss' + season.seasonId : '影视')),
          mediaTitle: [
            ep.epId ? 'PGC 单集 ep' + ep.epId : '',
            season.seasonId ? '系列 ss' + season.seasonId : '',
            season.mediaId ? '媒体 md' + season.mediaId : ''
          ].filter(Boolean).join(' · ') || 'B 站 PGC 内容编号',
          episodeLabel: total > 1
            ? (isMovie ? `版本 ${idx + 1} / 共 ${total} 个版本` : `第${idx + 1}${unit} / 共${total}${unit}`)
            : (isMovie ? '单版本' : `单${unit}`),
          cover: ep.cover || season.cover || '', duration: ep.duration,
          pages: season.episodes.map((e, i) => ({
            index: i, cid: e.cid, epId: e.epId, bvid: e.bvid || '',
            part: isMovie ? e.title : `${i + 1}. ${e.title}`, duration: e.duration
          }))
        };
      }
      const dash = play.dash || null;
      const vids = dash ? (dash.video || []).slice().sort((a, b) => (b.id || 0) - (a.id || 0)) : [];
      const auds = dash ? (dash.audio || []).slice().sort((a, b) => (b.bandwidth || 0) - (a.bandwidth || 0)) : [];
      const maxQ = vids.reduce((m, v) => Math.max(m, v.id || 0), 0);
      return sendJSON(res, 200, {
        ok: true, title: meta.title,
        bvid: meta.mediaId,
        owner: meta.owner, ownerLabel: meta.ownerLabel, episodeLabel: meta.episodeLabel,
        typeName: meta.typeName, unit: meta.unit, mediaTitle: meta.mediaTitle,
        kind: input.type,
        cover: meta.cover, duration: meta.duration,
        pageCount: meta.pages.length, pages: meta.pages, currentPage: idx,
        streams: dash ? { video: vids, audio: auds } : null,
        acceptDescription: play.accept_description || [], maxQuality: maxQ,
        hdAvailable: maxQ >= 80, needLogin: !dash || vids.length === 0
      });
    }

    if (p === '/api/cover' && req.method === 'GET') {
      let imgUrl = u.searchParams.get('url') || '';
      if (imgUrl.startsWith('//')) imgUrl = 'https:' + imgUrl;
      let ok = false;
      try {
        const iu = new URL(imgUrl);
        ok = (iu.protocol === 'http:' || iu.protocol === 'https:') &&
             ['hdslb.com', 'bilibili.com'].some(s => iu.hostname === s || iu.hostname.endsWith('.' + s));
      } catch {}
      if (!ok) { res.writeHead(400); res.end('invalid image url'); return; }
      try {
        const r = await fetch(imgUrl, { headers: { 'User-Agent': UA, 'Referer': REFERER } });
        if (!r.ok) { res.writeHead(r.status); res.end('upstream error'); return; }
        const buf = Buffer.from(await r.arrayBuffer());
        res.writeHead(200, { 'Content-Type': r.headers.get('content-type') || 'image/jpeg', 'Cache-Control': 'public, max-age=86400' });
        res.end(buf);
      } catch { res.writeHead(502); res.end('proxy error'); }
      return;
    }

    // ----- 下载 -----
    if (p === '/api/download' && req.method === 'POST') {
      const body = await readBody(req);
      body.mode = ['video', 'audio'].includes(body.mode) ? body.mode : 'both';
      const needVideo = body.mode !== 'audio', needAudio = body.mode !== 'video';
      if ((needVideo && !body.videoUrl) || (needAudio && !body.audioUrl)) {
        return sendJSON(res, 400, { ok: false, error: '缺少所需的流地址' });
      }
      if ((needVideo && !isBiliUrl(body.videoUrl)) || (needAudio && !isBiliUrl(body.audioUrl))) {
        return sendJSON(res, 400, { ok: false, error: '非法的下载地址（仅支持 B 站资源）' });
      }
      const job = makeJob(body);
      enqueueDownload(job);
      return sendJSON(res, 200, { ok: true, jobId: job.id });
    }

    if (p === '/api/download-all' && req.method === 'POST') {
      const body = await readBody(req);
      const url = body.url || '', cookie = body.cookie || '';
      const qnPref = body.qn ? Number(body.qn) : null;
      const mode = ['video', 'audio'].includes(body.mode) ? body.mode : 'both';
      // indices 为前端多选的分集序号（0-based 数组位置）；不传或传 null 表示全部
      const indices = Array.isArray(body.indices)
        ? [...new Set(body.indices.map(Number).filter(n => Number.isInteger(n) && n >= 0))]
        : null;
      const input = parseInput(url);
      if (input.type === 'unknown') return sendJSON(res, 400, { ok: false, error: '无法识别链接' });
      let items = [];
      try {
        if (input.type === 'video') {
          const view = await getView(input.bvid, cookie);
          items = view.pages.map((pg, i) => ({ kind: 'video', bvid: input.bvid, cid: pg.cid, title: pg.part || ('P' + (pg.index + 1)), index: pg.index, pos: i }));
        } else {
          const season = await getPgcSeason({ epId: input.epId, ssId: input.ssId }, cookie);
          items = season.episodes.map((e, i) => ({ kind: 'bangumi', aid: e.aid, cid: e.cid, epId: e.epId, title: e.title, index: e.index, pos: i, session: season.session }));
        }
      } catch (e) { return sendJSON(res, 400, { ok: false, error: '获取分集列表失败: ' + e.message }); }
      if (indices) items = items.filter(it => indices.includes(it.pos));
      if (!items.length) return sendJSON(res, 400, { ok: false, error: indices ? '没有选中任何分集' : '没有可下载的分集' });
      const jobIds = [], created = [];
      for (const it of items) {
        const prefix = String((it.pos != null ? it.pos : 0) + 1).padStart(2, '0') + '.';
        try {
          const play = it.kind === 'video'
            ? await getPlayUrl(it.bvid, it.cid, cookie)
            : await getBangumiPlayUrl(it.aid, it.cid, it.epId, it.session, cookie);
          const dash = play.dash || null;
          const vids = dash ? dash.video.slice().sort((a, b) => (b.id || 0) - (a.id || 0)) : [];
          const auds = dash ? dash.audio.slice().sort((a, b) => (b.bandwidth || 0) - (a.bandwidth || 0)) : [];
          if ((mode !== 'audio' && !vids.length) || (mode !== 'video' && !auds.length)) {
            created.push({ title: it.title, index: it.index, pos: it.pos, skip: '无可用流' }); continue;
          }
          let v = vids[0] || null;
          if (qnPref && vids.length) { const m = vids.find(x => x.id === qnPref); if (m) v = m; }
          const a = auds[0] || null;
          const job = makeJob({
            videoUrl: v ? v.baseUrl : '', audioUrl: a ? a.baseUrl : '',
            filename: prefix + it.title, cookie, engine: body.engine, mode
          });
          jobIds.push(job.id);
          enqueueDownload(job);
          created.push({ id: job.id, title: it.title, index: it.index, pos: it.pos });
        } catch (e) { created.push({ title: it.title, index: it.index, pos: it.pos, error: e.message }); }
      }
      return sendJSON(res, 200, { ok: true, count: jobIds.length, jobIds, items: created });
    }

    if (p === '/api/batch' && req.method === 'GET') {
      const ids = (u.searchParams.get('ids') || '').split(',').filter(Boolean);
      const list = ids.map(id => {
        const j = jobs.get(id);
        if (!j) return { id, gone: true };
        return { id, stage: j.stage, percent: j.percent, done: j.done, error: j.error };
      });
      return sendJSON(res, 200, { ok: true, jobs: list });
    }

    if (p === '/api/progress' && req.method === 'GET') {
      const id = u.searchParams.get('job');
      const job = jobs.get(id);
      if (!job) { res.writeHead(404); res.end('job not found'); return; }
      res.writeHead(200, {
        'Content-Type': 'text/event-stream; charset=utf-8', 'Cache-Control': 'no-cache',
        'Connection': 'keep-alive', 'Access-Control-Allow-Origin': '*'
      });
      const push = () => res.write(`data: ${JSON.stringify({ stage: job.stage, percent: job.percent, done: job.done, error: job.error })}\n\n`);
      push();
      const timer = setInterval(push, 500);
      const watch = setInterval(() => {
        if (job.done) {
          push();
          clearInterval(watch); clearInterval(timer);
          setTimeout(() => { try { res.end(); } catch {} }, 300);
        }
      }, 300);
      req.on('close', () => { clearInterval(timer); clearInterval(watch); });
      return;
    }

    if (p === '/api/file' && req.method === 'GET') {
      const job = jobs.get(u.searchParams.get('job'));
      if (!job || !job.outPath || !fs.existsSync(job.outPath)) { res.writeHead(404); res.end('文件不存在'); return; }
      const name = path.basename(job.outPath);
      const contentType = name.toLowerCase().endsWith('.m4a') ? 'audio/mp4' : 'video/mp4';
      res.writeHead(200, {
        'Content-Type': contentType,
        'Content-Disposition': `attachment; filename="${encodeURIComponent(name)}"; filename*=UTF-8''${encodeURIComponent(name)}`,
        'Access-Control-Allow-Origin': '*'
      });
      fs.createReadStream(job.outPath).pipe(res);
      return;
    }

    if (p === '/api/jobs' && req.method === 'GET') {
      let rows = [];
      if (db) {
        try {
          rows = stmts.list.all().map(r => ({
            id: r.id, filename: (() => { try { return JSON.parse(r.req).filename || ''; } catch { return ''; } })(),
            stage: r.stage, percent: r.percent, done: !!r.done, error: r.error || '',
            hasFile: !!(r.out_path && fs.existsSync(r.out_path)),
            createdAt: r.created_at
          }));
        } catch (e) { console.warn('[db] 读取历史失败:', e.message); }
      } else {
        rows = [...jobs.values()].map(j => ({ id: j.id, filename: j.req.filename, stage: j.stage, percent: j.percent, done: j.done, error: j.error, hasFile: !!j.outPath, createdAt: j.createdAt })).reverse();
      }
      return sendJSON(res, 200, { ok: true, jobs: rows });
    }

    if (p === '/api/open-folder' && req.method === 'GET') {
      const job = jobs.get(u.searchParams.get('job'));
      const target = job && job.outPath ? job.outPath : DL_DIR;
      const resolved = path.resolve(target);
      if (!resolved.startsWith(path.resolve(DL_DIR))) { res.writeHead(403); res.end('forbidden'); return; }
      const arg = job && job.outPath ? '/select,' + resolved : resolved;
      execFile('explorer.exe', [arg], { windowsHide: true }, () => {});
      return sendJSON(res, 200, { ok: true });
    }

    // ----- 静态资源 -----
    if (p === '/' || p === '/index.html') return serveStatic(res, path.join(PUBLIC_DIR, 'index.html'));
    if (/^\/[A-Za-z0-9_.-]+\.(js|css|png|svg|ico|html)$/.test(p)) {
      const f = path.resolve(PUBLIC_DIR, '.' + p);
      if (f.startsWith(path.resolve(PUBLIC_DIR))) return serveStatic(res, f);
    }
    res.writeHead(404); res.end('Not found');
  } catch (e) {
    sendJSON(res, 500, { ok: false, error: e.message });
  }
});

// 端口占用时自动顺延，桌面壳多开不冲突
let PORT = PORT0;
server.on('error', e => {
  if (e.code === 'EADDRINUSE' && PORT < PORT0 + 20) {
    console.log(`[server] 端口 ${PORT} 占用，尝试 ${PORT + 1}`);
    PORT++;
    setTimeout(() => server.listen(PORT, HOST), 100);
  } else { console.error('[server] 启动失败:', e.message); process.exit(1); }
});
server.listen(PORT, HOST, () => {
  console.log(`[server] BiliTools-Lite 已启动: http://${HOST}:${PORT}`);
  console.log(`[server] 下载引擎: aria2=${ARIA2_EXE || '不可用(回退单连接)'} | ffmpeg=${FFMPEG_EXE} | sqlite=${!!db}`);
  resumePending();
});

function shutdown() { if (aria2Proc) { try { aria2Proc.kill(); } catch {} } }
process.on('exit', shutdown);
process.on('SIGINT', () => { shutdown(); process.exit(0); });
process.on('SIGTERM', () => { shutdown(); process.exit(0); });
