'use strict';
/*
 * BiliTools-Lite 桌面壳启动器（零依赖、免编译）
 *
 * 职责：
 *   1) 挑一个空闲端口，拉起 server.js（Node 后端）
 *   2) 等 /api/health 就绪
 *   3) 用系统 Edge / Chrome 的 --app 模式打开独立窗口（无地址栏，等同桌面应用）
 *   4) 关闭窗口时一并结束后端与 aria2c 子进程
 *
 * 用法：node desktop.js   或双击「启动桌面版.vbs」
 * 可选环境变量：PORT（起始端口）、BROWSER（指定浏览器路径）
 */
const { spawn } = require('child_process');
const net = require('net');
const fs = require('fs');
const os = require('os');
const path = require('path');

const ROOT = __dirname;
// 持久化关闭时，Edge 用户目录放到系统临时区（不污染项目文件夹），退出时清理；
// 持久化开启时落在项目 data/edge-profile，方便保留。
const PERSIST = process.env.BILI_PERSIST === '1' || process.env.BILI_PERSIST === 'true';
const PROFILE_DIR = PERSIST
  ? path.join(ROOT, 'data', 'edge-profile')
  : path.join(os.tmpdir(), 'bilitools-lite-edge');
const sleep = ms => new Promise(r => setTimeout(r, ms));

function firstExisting(list) {
  for (const p of list) { try { if (p && fs.existsSync(p)) return p; } catch {} }
  return null;
}
function findBrowser() {
  return firstExisting([
    process.env.BROWSER,
    'C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe',
    'C:/Program Files/Microsoft/Edge/Application/msedge.exe',
    'C:/Program Files/Google/Chrome/Application/chrome.exe',
    'C:/Program Files (x86)/Google/Chrome/Application/chrome.exe',
    path.join(process.env.LOCALAPPDATA || '', 'Google/Chrome/Application/chrome.exe'),
    path.join(process.env.LOCALAPPDATA || '', 'Microsoft/Edge/Application/msedge.exe')
  ]);
}
function findFreePort(start, limit = 20) {
  return new Promise(resolve => {
    if (limit <= 0) return resolve(0);
    const s = net.createServer();
    s.once('error', () => resolve(findFreePort(start + 1, limit - 1)));
    s.listen(start, '127.0.0.1', () => {
      const port = s.address().port;
      s.close(() => resolve(port));
    });
  });
}
async function waitHealth(port, timeout = 15000) {
  const t0 = Date.now();
  while (Date.now() - t0 < timeout) {
    try {
      const r = await fetch(`http://127.0.0.1:${port}/api/health`, { signal: AbortSignal.timeout(1000) });
      if (r.ok) return true;
    } catch {}
    await sleep(200);
  }
  return false;
}

(async () => {
  const port = await findFreePort(Number(process.env.PORT || 3000));
  if (!port) { console.error('找不到空闲端口（3000-3020）'); process.exit(1); }

  const server = spawn(process.execPath, [path.join(ROOT, 'server.js')], {
    cwd: ROOT,
    env: { ...process.env, PORT: String(port) },
    windowsHide: true,
    stdio: ['ignore', 'pipe', 'pipe']
  });
  server.stdout.on('data', d => process.stdout.write('[server] ' + d));
  server.stderr.on('data', d => process.stderr.write('[server] ' + d));

  if (!await waitHealth(port)) {
    console.error('后端启动超时，请检查 server.js');
    server.kill();
    process.exit(1);
  }
  const url = `http://127.0.0.1:${port}/`;
  const browser = findBrowser();

  if (!browser) {
    // 没有 Edge/Chrome 时退化为默认浏览器，服务保持运行直到 Ctrl+C
    spawn('cmd.exe', ['/c', 'start', '', url], { windowsHide: true, stdio: 'ignore' });
    console.log('未找到 Edge/Chrome，已用默认浏览器打开：' + url);
    console.log('按 Ctrl+C 结束服务。');
    process.on('SIGINT', () => { server.kill(); process.exit(0); });
    return;
  }

  fs.mkdirSync(PROFILE_DIR, { recursive: true });
  // 预置 Edge 偏好：关闭同步、禁止自动登录微软账号，避免首次打开弹出"正在同步数据"通知
  const prefPath = path.join(PROFILE_DIR, 'Preferences');
  if (!fs.existsSync(prefPath)) {
    try {
      fs.writeFileSync(prefPath, JSON.stringify({
        sync: { disabled: true },
        signin: { allowed: false },
        browser: { has_seen_welcome_page: true }
      }));
    } catch {}
  }
  const win = spawn(browser, [
    `--app=${url}`,
    `--user-data-dir=${PROFILE_DIR}`,
    '--window-size=1180,880',
    '--window-position=120,60',
    '--no-first-run',
    '--no-default-browser-check',
    '--disable-sync',
    '--disable-features=DesktopIdentityConsistency,EdgeSync,msEdgeSidebarV2,msUndersideButton,msWebOOUI,msPdfOOUI'
  ], { windowsHide: false, stdio: 'ignore' });

  let closing = false;
  const stop = () => {
    if (closing) return;
    closing = true;
    try { win.kill(); } catch {}
    try { server.kill(); } catch {}
    // 持久化关闭时，清理临时 Edge 目录（最佳努力，失败不阻塞退出）
    if (!PERSIST) {
      try { fs.rmSync(PROFILE_DIR, { recursive: true, force: true, maxRetries: 3, retryDelay: 100 }); } catch {}
    }
    setTimeout(() => process.exit(0), 300);
  };
  win.on('error', e => { console.error('窗口启动失败：' + e.message); stop(); });
  win.on('exit', stop);
  server.on('exit', () => { if (!closing) { try { win.kill(); } catch {} process.exit(0); } });
  process.on('SIGINT', stop);
  process.on('SIGTERM', stop);
})();
