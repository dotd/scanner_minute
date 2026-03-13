const http = require('http');

const host = '127.0.0.1';
const port = 3000;

// Connected SSE clients
const clients = [];

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
  </style>
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
      </tr>
    </thead>
    <tbody id="tbody"></tbody>
  </table>
  <script>
    const tbody = document.getElementById('tbody');
    const status = document.getElementById('status');
    const evtSource = new EventSource('/events');

    evtSource.onopen = () => { status.textContent = 'Connected - waiting for breakouts...'; };
    evtSource.onerror = () => { status.textContent = 'Disconnected - reconnecting...'; status.style.color = '#ff6b6b'; };

    evtSource.addEventListener('scan', (e) => {
      const data = JSON.parse(e.data);
      status.textContent = 'Last scan: ' + data.time + ' (' + data.ticker_count + ' tickers)';
      status.style.color = '#4caf50';
    });

    evtSource.addEventListener('breakouts', (e) => {
      const data = JSON.parse(e.data);
      if (data.breakouts.length === 0) return;

      // Add header row for this batch
      const headerRow = document.createElement('tr');
      headerRow.className = 'scan-header';
      headerRow.innerHTML = '<td colspan="6">=== ' + data.breakouts.length + ' breakouts at ' + data.time + ' ===</td>';
      tbody.insertBefore(headerRow, tbody.firstChild);

      // Add breakout rows (reversed so strongest appears first)
      data.breakouts.forEach(b => {
        const row = document.createElement('tr');
        row.className = 'breakout-row';
        const ratioClass = b.ratio >= 1.2 ? 'ratio-high' : b.ratio >= 1.1 ? 'ratio-med' : 'ratio-low';
        row.innerHTML =
          '<td>' + b.current_time + '</td>' +
          '<td><strong>' + b.ticker + '</strong></td>' +
          '<td>' + b.lookback_min + 'm</td>' +
          '<td class="' + ratioClass + '">' + b.ratio.toFixed(4) + '</td>' +
          '<td>$' + b.past_close.toFixed(2) + ' → $' + b.current_close.toFixed(2) + '</td>' +
          '<td>' + b.past_time + '</td>';
        row.addEventListener('click', () => {
          window.open('https://www.tradingview.com/chart/?symbol=' + encodeURIComponent(b.ticker), '_blank');
        });
        tbody.insertBefore(row, headerRow.nextSibling);
      });
    });
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
      clients.forEach(client => {
        client.write('event: scan\ndata: ' + body + '\n\n');
      });
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end('{"ok":true}');
    });
    return;
  }

  res.writeHead(404);
  res.end('Not found');
});

server.listen(port, host, () => {
  console.log(`Dashboard running at http://${host}:${port}/`);
});
