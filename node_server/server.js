const http = require('http');

const host = '127.0.0.1';
const port = 3000;

// Connected SSE clients
const clients = [];

// Store latest breakouts
let latestBreakouts = { breakouts: [], time: '' };

// Store latest snapshot time
let latestSnapshotTime = '';

// Store candle data per ticker
const tickerCandles = {};

// Store news per ticker
const tickerNews = {};

const dashboardHtml = `
<!DOCTYPE html>
<html>
<head>
  <title>Scanner Minute - Breakout Dashboard</title>
  <style>
    body { font-family: monospace; background: #1a1a2e; color: #e0e0e0; margin: 20px; }
    h1 { color: #00d4ff; }
    #status { color: #4caf50; margin-bottom: 10px; }
    #breakouts { width: 100%; border-collapse: collapse; }
    #breakouts th { background: #16213e; color: #00d4ff; padding: 8px; text-align: left; border-bottom: 2px solid #0f3460; }
    #breakouts td { padding: 6px 8px; border-bottom: 1px solid #0f3460; }
    .scan-header { background: #16213e; color: #4caf50; font-weight: bold; }
    .breakout-row { background: #1a1a2e; }
    .breakout-row { cursor: pointer; }
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
  <h1>Breakout Dashboard</h1>
  <div id="status">Connecting...</div>
  <table id="breakouts">
    <thead>
      <tr>
        <th>Time (UTC)</th>
        <th>Ticker</th>
        <th>Lookback</th>
        <th>Ratio</th>
        <th>Price Change</th>
        <th>Past Time</th>
        <th>Vol %</th>
        <th></th>
      </tr>
    </thead>
    <tbody id="tbody"></tbody>
  </table>
  <script>
    const tbody = document.getElementById('tbody');
    const status = document.getElementById('status');
    const charts = {};  // ticker -> { chart, candleSeries, volumeSeries }

    function createChart(container, ticker, candles) {
      const pct = (candles ? candles.length : 1) / 100;
      const chartWidth = Math.max(Math.floor(container.clientWidth / 100), Math.floor(container.clientWidth * pct));
      const chart = LightweightCharts.createChart(container, {
        width: chartWidth,
        height: 600,
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

    function updateChart(ticker, candles) {
      if (!charts[ticker] || !candles || !candles.length) return;
      const { candleSeries, volumeSeries, chart } = charts[ticker];
      candleSeries.setData(candles.map(c => ({ time: c.time, open: c.open, high: c.high, low: c.low, close: c.close })));
      volumeSeries.setData(candles.map(c => ({ time: c.time, value: c.volume, color: c.close >= c.open ? '#26a69a80' : '#ef535080' })));
      chart.timeScale().fitContent();
    }

    // Track which tickers have their chart open
    const openCharts = new Set();

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

      // Remove charts for tickers no longer in breakouts
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
          '<td><a class="tv-link" href="' + tvUrl + '" target="_blank">TradingView</a></td>';
        tbody.appendChild(row);

        // Chart row (hidden by default, toggled on click)
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

        // News row (shown together with chart)
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
            html += '<li>' +
              '<a href="' + a.url + '" target="_blank">' + a.title + '</a> ' +
              (a.sentiment ? '<span class="' + sentClass + '">[' + a.sentiment + ']</span> ' : '') +
              '<span class="news-source">' + a.source + '</span> ' +
              '<span class="news-time">' + timeStr + '</span>' +
              '</li>';
          });
          html += '</ul>';
          newsCell.innerHTML = html;
        } else {
          newsCell.innerHTML = '<div style="color:#666;padding:6px;text-align:center;">No recent news</div>';
        }
        newsRow.appendChild(newsCell);
        tbody.appendChild(newsRow);

        // Toggle chart + news rows together
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

        // If chart was already open, recreate it in the new DOM
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
</html>
`;

const server = http.createServer((req, res) => {
  // Dashboard
  if (req.method === 'GET' && req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(dashboardHtml);
    return;
  }

  // SSE endpoint
  if (req.method === 'GET' && req.url === '/events') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
    });
    res.write(':\n\n'); // SSE comment to establish connection
    clients.push(res);
    req.on('close', () => {
      const idx = clients.indexOf(res);
      if (idx >= 0) clients.splice(idx, 1);
    });
    return;
  }

  // POST breakouts from Python
  if (req.method === 'POST' && req.url === '/breakouts') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      latestBreakouts = JSON.parse(body);
      // Broadcast to all SSE clients
      clients.forEach(client => {
        client.write('event: breakouts\ndata: ' + body + '\n\n');
      });
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end('{"ok":true}');
    });
    return;
  }

  // POST scan status from Python
  if (req.method === 'POST' && req.url === '/scan') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      const payload = JSON.parse(body);
      if (payload.snapshot_time) latestSnapshotTime = payload.snapshot_time;
      clients.forEach(client => {
        client.write('event: scan\ndata: ' + body + '\n\n');
      });
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end('{"ok":true}');
    });
    return;
  }

  // POST candle data from Python
  if (req.method === 'POST' && req.url === '/candles') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      const payload = JSON.parse(body);
      if (payload.ticker && payload.candles) {
        tickerCandles[payload.ticker] = payload.candles;
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end('{"ok":true}');
    });
    return;
  }

  // POST news from Python
  if (req.method === 'POST' && req.url === '/news') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      const payload = JSON.parse(body);
      if (payload.ticker && payload.articles) {
        tickerNews[payload.ticker] = payload.articles;
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end('{"ok":true}');
    });
    return;
  }

  // GET candles for a specific ticker
  if (req.method === 'GET' && req.url.startsWith('/candles?')) {
    const params = new URL(req.url, 'http://localhost').searchParams;
    const ticker = params.get('ticker');
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ticker, candles: tickerCandles[ticker] || [] }));
    return;
  }

  // GET latest breakouts for polling (includes candle data for breakout tickers)
  if (req.method === 'GET' && req.url === '/latest') {
    const breakoutTickers = (latestBreakouts.breakouts || []).map(b => b.ticker);
    const candles = {};
    breakoutTickers.forEach(t => { if (tickerCandles[t]) candles[t] = tickerCandles[t]; });
    res.writeHead(200, { 'Content-Type': 'application/json' });
    const news = {};
    breakoutTickers.forEach(t => { if (tickerNews[t]) news[t] = tickerNews[t]; });
    res.end(JSON.stringify({ ...latestBreakouts, candles, news, snapshot_time: latestSnapshotTime }));
    return;
  }

  res.writeHead(404);
  res.end('Not found');
});

server.listen(port, host, () => {
  console.log(`Dashboard running at http://${host}:${port}/`);
});
