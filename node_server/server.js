const http = require('http');

const host = '127.0.0.1';
const port = 3000;

const html = `
<!DOCTYPE html>
<html>
<head><title>Scanner Minute - Node Test</title></head>
<body>
  <h1>Node.js is working!</h1>
  <p>Server running on ${host}:${port}</p>
  <p>Timestamp: ${new Date().toISOString()}</p>
</body>
</html>
`;

const server = http.createServer((req, res) => {
    res.writeHead(200, {'Content-Type': 'text/html'});
    res.end(html);
});

server.listen(port, host, () => {
    console.log(`Server running at http://${host}:${port}/`);
});
