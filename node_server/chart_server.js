const http = require('http');

const host = '127.0.0.1';
const port = 3001;

// Store candle data per ticker: { AAPL: [{time, open, high, low, close, volume}, ...], ... }
const tickerData = {};
const clients = [];

const chartHtml = `
<!DOCTYPE html>
<html>
<head>
  <title>Scanner Minute - Candlestick Chart</title>
  <script src="https://unpkg.com/lightweight-charts@4.1.1/dist/lightweight-charts.standalone.production.js"></script>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { font-family: monospace; background: #1a1a2e; color: #e0e0e0; }
    #controls { padding: 12px 20px; display: flex; align-items: center; gap: 16px; background: #16213e; }
    #controls h1 { color: #00d4ff; font-size: 18px; }
    #controls select, #controls input {
      background: #1a1a2e; color: #e0e0e0; border: 1px solid #0f3460;
      padding: 6px 10px; font-family: monospace; font-size: 14px;
    }
    #controls button {
      background: #0f3460; color: #00d4ff; border: none; padding: 6px 14px;
      font-family: monospace; font-size: 14px; cursor: pointer;
    }
    #controls button:hover { background: #00d4ff; color: #1a1a2e; }
    #status { color: #4caf50; font-size: 12px; }
    #chart-container { width: 100%; height: calc(100vh - 50px); }
  </style>
</head>
<body>
  <div id="controls">
    <h1>Candlestick Chart</h1>
    <label>Ticker: <select id="ticker-select"><option value="">Loading...</option></select></label>
    <label style="color: #ffa726; font-size: 13px;">Timezone:
      <select id="tz-select" style="background:#1a1a2e;color:#ffa726;border:1px solid #0f3460;padding:4px 8px;font-family:monospace;font-size:13px;">
        <option value="0" selected>UTC</option>
        <option value="2">Israel (UTC+2)</option>
        <option value="3">Israel DST (UTC+3)</option>
        <option value="-5">New York (UTC-5)</option>
        <option value="-4">New York DST (UTC-4)</option>
      </select>
    </label>
    <span id="status">Connecting...</span>
  </div>
  <div id="chart-container"></div>
  <script>
    const container = document.getElementById('chart-container');
    const tickerSelect = document.getElementById('ticker-select');
    const tzSelect = document.getElementById('tz-select');
    const statusEl = document.getElementById('status');

    let tzOffsetHours = 0;

    const chart = LightweightCharts.createChart(container, {
      layout: { background: { color: '#1a1a2e' }, textColor: '#e0e0e0' },
      grid: { vertLines: { color: '#0f3460' }, horzLines: { color: '#0f3460' } },
      crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
      localization: {
        timeFormatter: (t) => {
          const d = new Date(t * 1000);
          return d.getUTCHours().toString().padStart(2, '0') + ':' +
                 d.getUTCMinutes().toString().padStart(2, '0');
        },
      },
      timeScale: { timeVisible: true, secondsVisible: false },
    });

    const candleSeries = chart.addCandlestickSeries({
      upColor: '#4caf50', downColor: '#ff5252',
      borderUpColor: '#4caf50', borderDownColor: '#ff5252',
      wickUpColor: '#4caf50', wickDownColor: '#ff5252',
    });

    const volumeSeries = chart.addHistogramSeries({
      priceFormat: { type: 'volume' },
      priceScaleId: 'volume',
    });
    chart.priceScale('volume').applyOptions({
      scaleMargins: { top: 0.8, bottom: 0 },
    });

    // All data received so far
    const allData = {};

    function renderTicker(ticker) {
      const data = allData[ticker];
      if (!data || data.length === 0) return;
      const offset = tzOffsetHours * 3600;
      candleSeries.setData(data.map(d => ({
        time: d.time + offset, open: d.open, high: d.high, low: d.low, close: d.close
      })));
      volumeSeries.setData(data.map(d => ({
        time: d.time + offset,
        value: d.volume,
        color: d.close >= d.open ? 'rgba(76,175,80,0.4)' : 'rgba(255,82,82,0.4)',
      })));
      chart.timeScale().fitContent();
      const tzLabel = tzSelect.options[tzSelect.selectedIndex].text;
      statusEl.textContent = ticker + ': ' + data.length + ' candles (' + tzLabel + ')';
    }

    tickerSelect.addEventListener('change', () => {
      if (tickerSelect.value) renderTicker(tickerSelect.value);
    });

    tzSelect.addEventListener('change', () => {
      tzOffsetHours = parseInt(tzSelect.value);
      if (tickerSelect.value) renderTicker(tickerSelect.value);
    });

    // SSE connection
    const evtSource = new EventSource('/events');
    evtSource.onopen = () => { statusEl.textContent = 'Connected - waiting for data...'; };
    evtSource.onerror = () => { statusEl.textContent = 'Disconnected'; statusEl.style.color = '#ff6b6b'; };

    evtSource.addEventListener('candles', (e) => {
      const msg = JSON.parse(e.data);
      allData[msg.ticker] = msg.candles;

      // Update dropdown
      const tickers = Object.keys(allData).sort();
      const current = tickerSelect.value;
      tickerSelect.innerHTML = tickers.map(t =>
        '<option value="' + t + '"' + (t === current ? ' selected' : '') + '>' + t + '</option>'
      ).join('');

      // Auto-render first ticker or current selection
      if (!current && tickers.length > 0) {
        tickerSelect.value = tickers[0];
        renderTicker(tickers[0]);
      } else if (msg.ticker === current) {
        renderTicker(current);
      }
    });

    // Handle resize
    window.addEventListener('resize', () => {
      chart.applyOptions({ width: container.clientWidth, height: container.clientHeight });
    });
  </script>
</body>
</html>
`;

const server = http.createServer((req, res) => {
  // Chart page
  if (req.method === 'GET' && req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(chartHtml);
    return;
  }

  // SSE endpoint
  if (req.method === 'GET' && req.url === '/events') {
    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
    });
    res.write(':\n\n');
    clients.push(res);

    // Send existing data to new client
    for (const [ticker, candles] of Object.entries(tickerData)) {
      const payload = JSON.stringify({ ticker, candles });
      res.write('event: candles\ndata: ' + payload + '\n\n');
    }

    req.on('close', () => {
      const idx = clients.indexOf(res);
      if (idx >= 0) clients.splice(idx, 1);
    });
    return;
  }

  // POST candles from Python
  if (req.method === 'POST' && req.url === '/candles') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      try {
        const msg = JSON.parse(body);
        tickerData[msg.ticker] = msg.candles;
        // Broadcast to all SSE clients
        clients.forEach(client => {
          client.write('event: candles\ndata: ' + body + '\n\n');
        });
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end('{"ok":true}');
      } catch (e) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end('{"error":"invalid json"}');
      }
    });
    return;
  }

  res.writeHead(404);
  res.end('Not found');
});

server.listen(port, host, () => {
  console.log('Chart server running at http://' + host + ':' + port + '/');
});
