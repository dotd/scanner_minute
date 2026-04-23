// scanner_minute dashboard server.
//   /              -> realtime breakout dashboard (auth-protected)
//   /industries    -> latest industries report (auth-protected)
//   /login         -> Google OAuth login page
//   /auth/google*  -> OAuth handshake
//   POST /breakouts /scan /candles /news  -> from local Python scanner (localhost-only)
//
// Config (env):
//   PORT                    default 3000
//   HOST                    default 0.0.0.0
//   GOOGLE_CLIENT_ID        required unless DISABLE_AUTH=1
//   GOOGLE_CLIENT_SECRET    required unless DISABLE_AUTH=1
//   OAUTH_CALLBACK_URL      e.g. http://localhost:3000/auth/google/callback
//   SESSION_SECRET          random string; defaults to a dev placeholder
//   ALLOWED_EMAILS_FILE     default /app/api_keys/allowed_emails.txt
//   INDUSTRIES_REPORTS_DIR  default /app/data/industries_reports
//   DISABLE_AUTH            if "1", bypass OAuth (local-only dev)

const fs = require('fs');
const path = require('path');
const express = require('express');
const session = require('express-session');
const passport = require('passport');
const GoogleStrategy = require('passport-google-oauth20').Strategy;

const PORT = parseInt(process.env.PORT || '3000', 10);
const HOST = process.env.HOST || '127.0.0.1';
// Auth is off by default so `python tst_scan_realtime.py` works on the host
// without any OAuth setup (back-compat with pre-Docker usage).
// The Dockerfile / run_local.sh explicitly enable it.
const AUTH_ENABLED = process.env.AUTH_ENABLED === '1';
const DISABLE_AUTH = !AUTH_ENABLED;
const ALLOWED_EMAILS_FILE = process.env.ALLOWED_EMAILS_FILE || '/app/api_keys/allowed_emails.txt';
const INDUSTRIES_REPORTS_DIR = process.env.INDUSTRIES_REPORTS_DIR || '/app/data/industries_reports';
const SESSION_SECRET = process.env.SESSION_SECRET || 'dev-only-please-override';

function loadAllowedEmails() {
    try {
        const raw = fs.readFileSync(ALLOWED_EMAILS_FILE, 'utf8');
        const emails = raw
            .split('\n')
            .map(line => line.trim().toLowerCase())
            .filter(line => line && !line.startsWith('#'));
        return new Set(emails);
    } catch (err) {
        console.warn(`[auth] could not load ${ALLOWED_EMAILS_FILE}: ${err.message}`);
        return new Set();
    }
}

let allowedEmails = loadAllowedEmails();
console.log(`[auth] allowlist contains ${allowedEmails.size} email(s)`);

// --- shared state from Python scanner ---------------------------------------
let latestBreakouts = { breakouts: [], time: '' };
let latestSnapshotTime = '';
const tickerCandles = {};
const tickerNews = {};
const clients = [];

// --- express app ------------------------------------------------------------
const app = express();
app.set('trust proxy', 1);
app.use(express.json({ limit: '2mb' }));

app.use(session({
    secret: SESSION_SECRET,
    resave: false,
    saveUninitialized: false,
    cookie: {
        httpOnly: true,
        sameSite: 'lax',
        secure: process.env.OAUTH_CALLBACK_URL && process.env.OAUTH_CALLBACK_URL.startsWith('https://'),
        maxAge: 7 * 24 * 60 * 60 * 1000,
    },
}));

app.use(passport.initialize());
app.use(passport.session());

passport.serializeUser((user, done) => done(null, user));
passport.deserializeUser((user, done) => done(null, user));

if (DISABLE_AUTH) {
    console.warn('[auth] AUTH_ENABLED != 1 — running with NO authentication. Set AUTH_ENABLED=1 with GOOGLE_CLIENT_ID/SECRET/OAUTH_CALLBACK_URL to enforce login.');
} else {
    const callbackURL = process.env.OAUTH_CALLBACK_URL;
    if (!process.env.GOOGLE_CLIENT_ID || !process.env.GOOGLE_CLIENT_SECRET || !callbackURL) {
        console.error('[auth] AUTH_ENABLED=1 but GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, or OAUTH_CALLBACK_URL is missing.');
        process.exit(1);
    }
    passport.use(new GoogleStrategy({
        clientID: process.env.GOOGLE_CLIENT_ID,
        clientSecret: process.env.GOOGLE_CLIENT_SECRET,
        callbackURL,
    }, (accessToken, refreshToken, profile, done) => {
        const email = (profile.emails && profile.emails[0] && profile.emails[0].value || '').toLowerCase();
        if (!email) return done(null, false, { message: 'no email from Google' });
        if (!allowedEmails.has(email)) {
            console.warn(`[auth] denied ${email} (not in allowlist)`);
            return done(null, false, { message: 'not allowed' });
        }
        return done(null, { email, name: profile.displayName });
    }));
}

function ensureAuth(req, res, next) {
    if (DISABLE_AUTH) return next();
    if (req.isAuthenticated && req.isAuthenticated()) return next();
    if (req.accepts('html')) return res.redirect('/login');
    return res.status(401).json({ error: 'unauthenticated' });
}

function localhostOnly(req, res, next) {
    const ip = (req.ip || '').replace('::ffff:', '');
    if (ip === '127.0.0.1' || ip === '::1') return next();
    return res.status(403).json({ error: 'forbidden' });
}

// --- OAuth routes -----------------------------------------------------------
app.get('/login', (req, res) => {
    const err = req.query.err;
    res.type('html').send(`
        <!DOCTYPE html>
        <html><head><title>Scanner Minute — Sign in</title>
        <style>body{font-family:sans-serif;background:#1a1a2e;color:#e0e0e0;display:flex;align-items:center;justify-content:center;height:100vh;margin:0}
        .box{text-align:center}
        a.btn{background:#4285f4;color:#fff;padding:12px 24px;border-radius:4px;text-decoration:none;display:inline-block;margin-top:20px}
        .err{color:#ff6b6b;margin-top:12px}</style></head>
        <body><div class="box">
          <h1>Scanner Minute</h1>
          <p>Sign in with an allowlisted Google account to continue.</p>
          <a class="btn" href="/auth/google">Sign in with Google</a>
          ${err ? `<div class="err">Access denied (${err}).</div>` : ''}
        </div></body></html>`);
});

app.get('/auth/google',
    passport.authenticate('google', { scope: ['profile', 'email'] }));

app.get('/auth/google/callback',
    passport.authenticate('google', { failureRedirect: '/login?err=denied' }),
    (req, res) => res.redirect('/'));

app.post('/logout', (req, res, next) => {
    req.logout(err => {
        if (err) return next(err);
        req.session.destroy(() => res.redirect('/login'));
    });
});

// --- browser-facing routes (protected) --------------------------------------
app.get('/', ensureAuth, (req, res) => {
    res.type('html').send(dashboardHtml(req.user));
});

app.get('/latest', ensureAuth, (req, res) => {
    const breakoutTickers = (latestBreakouts.breakouts || []).map(b => b.ticker);
    const candles = {};
    breakoutTickers.forEach(t => { if (tickerCandles[t]) candles[t] = tickerCandles[t]; });
    const news = {};
    breakoutTickers.forEach(t => { if (tickerNews[t]) news[t] = tickerNews[t]; });
    res.json({ ...latestBreakouts, candles, news, snapshot_time: latestSnapshotTime });
});

app.get('/candles', ensureAuth, (req, res) => {
    const ticker = req.query.ticker;
    res.json({ ticker, candles: tickerCandles[ticker] || [] });
});

app.get('/events', ensureAuth, (req, res) => {
    res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
    });
    res.write(':\n\n');
    clients.push(res);
    req.on('close', () => {
        const idx = clients.indexOf(res);
        if (idx >= 0) clients.splice(idx, 1);
    });
});

app.get('/industries', ensureAuth, (req, res) => {
    let reports = [];
    try {
        reports = fs.readdirSync(INDUSTRIES_REPORTS_DIR)
            .filter(n => n.startsWith('industries_ranking_') && n.endsWith('.txt'))
            .sort()
            .reverse();
    } catch (err) {
        return res.type('html').send(industriesShell(req.user, `<p>No reports yet (${err.message}).</p>`));
    }
    if (!reports.length) {
        return res.type('html').send(industriesShell(req.user, '<p>No reports generated yet. The hourly scanner will create one shortly.</p>'));
    }
    const selected = req.query.file && reports.includes(req.query.file) ? req.query.file : reports[0];
    let content = '';
    try {
        content = fs.readFileSync(path.join(INDUSTRIES_REPORTS_DIR, selected), 'utf8');
    } catch (err) {
        content = `(error reading ${selected}: ${err.message})`;
    }
    const options = reports.map(n => `<option value="${n}"${n === selected ? ' selected' : ''}>${n}</option>`).join('');
    const body = `
        <form method="get" action="/industries" style="margin-bottom:12px">
          <label>Report: <select name="file" onchange="this.form.submit()">${options}</select></label>
        </form>
        <pre>${escapeHtml(content)}</pre>`;
    res.type('html').send(industriesShell(req.user, body));
});

// --- Python-scanner POST endpoints (localhost only) -------------------------
app.post('/breakouts', localhostOnly, (req, res) => {
    latestBreakouts = req.body;
    const body = JSON.stringify(req.body);
    clients.forEach(c => c.write('event: breakouts\ndata: ' + body + '\n\n'));
    res.json({ ok: true });
});

app.post('/scan', localhostOnly, (req, res) => {
    if (req.body && req.body.snapshot_time) latestSnapshotTime = req.body.snapshot_time;
    const body = JSON.stringify(req.body);
    clients.forEach(c => c.write('event: scan\ndata: ' + body + '\n\n'));
    res.json({ ok: true });
});

app.post('/candles', localhostOnly, (req, res) => {
    const p = req.body;
    if (p && p.ticker && p.candles) tickerCandles[p.ticker] = p.candles;
    res.json({ ok: true });
});

app.post('/news', localhostOnly, (req, res) => {
    const p = req.body;
    if (p && p.ticker && p.articles) tickerNews[p.ticker] = p.articles;
    res.json({ ok: true });
});

// --- HTML helpers -----------------------------------------------------------
function escapeHtml(s) {
    return String(s)
        .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

function navBar(user) {
    const who = user ? `${escapeHtml(user.email)} ` : '';
    return `<div style="display:flex;gap:16px;align-items:center;padding:8px 16px;background:#0f1523;border-bottom:1px solid #1f2a44;font-family:sans-serif">
        <a href="/" style="color:#00d4ff;text-decoration:none;font-weight:bold">Breakouts</a>
        <a href="/industries" style="color:#00d4ff;text-decoration:none">Industries</a>
        <span style="flex:1"></span>
        <span style="color:#888;font-size:13px">${who}</span>
        <form method="post" action="/logout" style="margin:0"><button style="background:none;border:1px solid #2a3a5a;color:#e0e0e0;padding:4px 10px;border-radius:3px;cursor:pointer">Logout</button></form>
      </div>`;
}

function industriesShell(user, body) {
    return `<!DOCTYPE html><html><head><title>Scanner Minute — Industries</title>
      <style>body{font-family:monospace;background:#1a1a2e;color:#e0e0e0;margin:0}
      .wrap{padding:20px}
      h1{color:#00d4ff}
      pre{background:#0f1523;padding:12px;border-radius:4px;overflow-x:auto;font-size:12px;line-height:1.4}
      select{background:#16213e;color:#e0e0e0;border:1px solid #2a3a5a;padding:4px}
      </style></head><body>
      ${navBar(user)}
      <div class="wrap"><h1>Industries Rankings</h1>${body}</div>
      </body></html>`;
}

function dashboardHtml(user) {
    return `<!DOCTYPE html>
<html>
<head>
  <title>Scanner Minute - Breakout Dashboard</title>
  <style>
    body { font-family: monospace; background: #1a1a2e; color: #e0e0e0; margin: 0; }
    .wrap { padding: 20px; }
    h1 { color: #00d4ff; }
    #status { color: #4caf50; margin-bottom: 10px; }
    #breakouts { width: 100%; border-collapse: collapse; }
    #breakouts th { background: #16213e; color: #00d4ff; padding: 8px; text-align: left; border-bottom: 2px solid #0f3460; }
    #breakouts td { padding: 6px 8px; border-bottom: 1px solid #0f3460; }
    .breakout-row { background: #1a1a2e; cursor: pointer; }
    .breakout-row:hover { background: #16213e; }
    .ratio-high { color: #ff6b6b; font-weight: bold; }
    .ratio-med { color: #ffa726; }
    .ratio-low { color: #4caf50; }
    .chart-row td { padding: 0; }
    .chart-row { display: none; }
    .chart-row.open { display: table-row; }
    .chart-container { width: 100%; height: 600px; background: #131722; }
    .tv-link { color: #00d4ff; text-decoration: none; }
    .tv-link:hover { text-decoration: underline; }
    .news-row td { padding: 4px 8px; }
    .news-row { display: none; }
    .news-row.open { display: table-row; }
    .news-list { list-style: none; padding: 0; margin: 4px 0; }
    .news-list li { padding: 3px 0; border-bottom: 1px solid #1e222d; }
    .news-list a { color: #4caf50; text-decoration: none; }
    .news-list a:hover { text-decoration: underline; }
    .sentiment-positive { color: #26a69a; }
    .sentiment-negative { color: #ef5350; }
    .sentiment-neutral { color: #888; }
    .news-source { color: #666; font-size: 0.85em; }
    .news-time { color: #666; font-size: 0.85em; }
  </style>
  <script src="https://unpkg.com/lightweight-charts@4.1.1/dist/lightweight-charts.standalone.production.js"></script>
</head>
<body>
  ${navBar(user)}
  <div class="wrap">
  <h1>Breakout Dashboard</h1>
  <div id="status">Connecting...</div>
  <table id="breakouts">
    <thead>
      <tr>
        <th>Time (UTC)</th><th>Ticker</th><th>Lookback</th><th>Ratio</th>
        <th>Price Change</th><th>Past Time</th><th>Vol %</th><th></th>
      </tr>
    </thead>
    <tbody id="tbody"></tbody>
  </table>
  </div>
  <script>
    const tbody = document.getElementById('tbody');
    const status = document.getElementById('status');
    const charts = {};
    const openCharts = new Set();

    function createChart(container, ticker, candles) {
      const pct = (candles ? candles.length : 1) / 100;
      const chartWidth = Math.max(Math.floor(container.clientWidth / 100), Math.floor(container.clientWidth * pct));
      const chart = LightweightCharts.createChart(container, {
        width: chartWidth, height: 600,
        layout: { background: { color: '#131722' }, textColor: '#d1d4dc' },
        grid: { vertLines: { color: '#1e222d' }, horzLines: { color: '#1e222d' } },
        timeScale: { timeVisible: true, secondsVisible: false },
        crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
      });
      const candleSeries = chart.addCandlestickSeries({
        upColor: '#26a69a', downColor: '#ef5350',
        borderUpColor: '#26a69a', borderDownColor: '#ef5350',
        wickUpColor: '#26a69a', wickDownColor: '#ef5350',
      });
      const volumeSeries = chart.addHistogramSeries({
        color: '#26a69a', priceFormat: { type: 'volume' },
        priceScaleId: '', scaleMargins: { top: 0.8, bottom: 0 },
      });
      if (candles && candles.length) {
        candleSeries.setData(candles.map(c => ({ time: c.time, open: c.open, high: c.high, low: c.low, close: c.close })));
        volumeSeries.setData(candles.map(c => ({ time: c.time, value: c.volume, color: c.close >= c.open ? '#26a69a80' : '#ef535080' })));
      }
      chart.timeScale().fitContent();
      charts[ticker] = { chart, candleSeries, volumeSeries, container };
      new ResizeObserver(() => chart.applyOptions({ width: Math.max(Math.floor(container.clientWidth / 100), Math.floor(container.clientWidth * pct)) })).observe(container);
    }

    function renderBreakouts(data) {
      const currentTickers = new Set();
      if (!data.breakouts || data.breakouts.length === 0) {
        tbody.innerHTML = '<tr><td colspan="8" style="color:#888;text-align:center;padding:20px;">No breakouts yet</td></tr>';
        Object.keys(charts).forEach(t => { charts[t].chart.remove(); delete charts[t]; });
        return;
      }
      const snapshotInfo = data.snapshot_time ? ' | snapshot: ' + data.snapshot_time : '';
      status.textContent = 'Last update: ' + data.time + ' (' + data.breakouts.length + ' breakouts)' + snapshotInfo;
      status.style.color = '#4caf50';
      data.breakouts.forEach(b => currentTickers.add(b.ticker));
      Object.keys(charts).forEach(t => {
        if (!currentTickers.has(t)) { charts[t].chart.remove(); delete charts[t]; openCharts.delete(t); }
      });

      tbody.innerHTML = '';
      const candleData = data.candles || {};
      const newsData = data.news || {};

      data.breakouts.forEach(b => {
        const row = document.createElement('tr');
        row.className = 'breakout-row';
        const ratioClass = b.ratio >= 1.2 ? 'ratio-high' : b.ratio >= 1.1 ? 'ratio-med' : 'ratio-low';
        const tvUrl = 'https://www.tradingview.com/chart/?symbol=' + encodeURIComponent(b.ticker) + '&interval=1';
        row.innerHTML =
          '<td>' + b.current_time + '</td>' +
          '<td><strong>' + b.ticker + '</strong></td>' +
          '<td>' + b.lookback_min + 'm</td>' +
          '<td class="' + ratioClass + '">' + b.ratio.toFixed(4) + '</td>' +
          '<td>$' + b.past_close.toFixed(2) + ' &rarr; $' + b.current_close.toFixed(2) + '</td>' +
          '<td>' + b.past_time + '</td>' +
          '<td>' + (b.volume_pct != null ? b.volume_pct + '%' : '') + '</td>' +
          '<td><a class="tv-link" href="' + tvUrl + '" target="_blank">TradingView</a>' +
          ' | <a class="tv-link" href="https://finviz.com/quote.ashx?t=' + encodeURIComponent(b.ticker) + '" target="_blank">Finviz</a></td>';
        tbody.appendChild(row);

        const chartRow = document.createElement('tr');
        chartRow.className = 'chart-row' + (openCharts.has(b.ticker) ? ' open' : '');
        const chartCell = document.createElement('td');
        chartCell.colSpan = 8;
        const chartDiv = document.createElement('div');
        chartDiv.className = 'chart-container';
        chartDiv.id = 'chart-' + b.ticker;
        chartCell.appendChild(chartDiv);
        chartRow.appendChild(chartCell);
        tbody.appendChild(chartRow);

        const newsRow = document.createElement('tr');
        newsRow.className = 'news-row' + (openCharts.has(b.ticker) ? ' open' : '');
        const newsCell = document.createElement('td');
        newsCell.colSpan = 8;
        const articles = newsData[b.ticker];
        if (articles && articles.length) {
          let html = '<ul class="news-list">';
          articles.forEach(a => {
            const sentClass = a.sentiment === 'positive' ? 'sentiment-positive' : a.sentiment === 'negative' ? 'sentiment-negative' : 'sentiment-neutral';
            const timeStr = a.published ? new Date(a.published).toLocaleTimeString() : '';
            html += '<li><a href="' + a.url + '" target="_blank">' + a.title + '</a> ' +
              (a.sentiment ? '<span class="' + sentClass + '">[' + a.sentiment + ']</span> ' : '') +
              '<span class="news-source">' + a.source + '</span> ' +
              '<span class="news-time">' + timeStr + '</span></li>';
          });
          html += '</ul>';
          newsCell.innerHTML = html;
        } else {
          newsCell.innerHTML = '<div style="color:#666;padding:6px;text-align:center;">No recent news</div>';
        }
        newsRow.appendChild(newsCell);
        tbody.appendChild(newsRow);

        row.addEventListener('click', (e) => {
          if (e.target.closest('a')) return;
          if (chartRow.classList.contains('open')) {
            chartRow.classList.remove('open');
            newsRow.classList.remove('open');
            openCharts.delete(b.ticker);
          } else {
            chartRow.classList.add('open');
            newsRow.classList.add('open');
            openCharts.add(b.ticker);
            const container = document.getElementById('chart-' + b.ticker);
            const candles = candleData[b.ticker];
            if (container && candles && candles.length && !charts[b.ticker]) {
              createChart(container, b.ticker, candles);
            } else if (container && (!candles || !candles.length) && !charts[b.ticker]) {
              container.innerHTML = '<div style="color:#666;padding:10px;text-align:center;">No candle data yet</div>';
            }
          }
        });

        if (openCharts.has(b.ticker)) {
          const container = document.getElementById('chart-' + b.ticker);
          const candles = candleData[b.ticker];
          if (charts[b.ticker]) { charts[b.ticker].chart.remove(); delete charts[b.ticker]; }
          if (container && candles && candles.length) {
            createChart(container, b.ticker, candles);
          }
        }
      });
    }

    async function fetchLatest() {
      try {
        const res = await fetch('/latest');
        if (res.status === 401) { window.location = '/login'; return; }
        const data = await res.json();
        renderBreakouts(data);
      } catch (e) {
        status.textContent = 'Error fetching breakouts';
        status.style.color = '#ff6b6b';
      }
    }

    fetchLatest();
    setInterval(fetchLatest, 10000);
  </script>
</body>
</html>`;
}

// --- start ------------------------------------------------------------------
app.listen(PORT, HOST, () => {
    const mode = AUTH_ENABLED ? 'OAuth enabled' : 'NO AUTH';
    console.log(`[server] listening on http://${HOST}:${PORT}/ (${mode})`);
});
