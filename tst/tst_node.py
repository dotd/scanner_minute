import logging
import os
import time
import webbrowser

from ScannerMinute.src import imaging_utils
from ScannerMinute.src import logging_utils

# Simple Node.js HTTP server script
SERVER_JS = """\
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
"""


def main():
    logging_utils.setup_logging(
        log_level="INFO",
        include_time=True,
        log_folder=f"{os.path.dirname(os.path.abspath(__file__))}/logs/",
    )

    # 1. Check / install Node.js
    found, path = imaging_utils.is_node_installed()
    if not found:
        logging.info("Node.js not found, installing...")
        path = imaging_utils.install_node(ask=True)
        if not path:
            logging.error("Node.js installation was declined. Exiting.")
            return

    # 2. Write a simple server.js
    server_js_path = os.path.join(imaging_utils.NODE_DIR, "server.js")
    os.makedirs(imaging_utils.NODE_DIR, exist_ok=True)
    with open(server_js_path, "w") as f:
        f.write(SERVER_JS)
    logging.info(f"Wrote test server to {server_js_path}")

    # 3. Run the server
    process = imaging_utils.run_server(server_js_path)
    logging.info("Server is running at http://127.0.0.1:3000/")
    logging.info("Press Ctrl+C to stop.")

    # 4. Open browser
    webbrowser.open("http://127.0.0.1:3000/")

    try:
        process.wait()
    except KeyboardInterrupt:
        logging.info("Stopping server...")
        process.terminate()
        process.wait()
        logging.info("Server stopped.")


if __name__ == "__main__":
    main()
