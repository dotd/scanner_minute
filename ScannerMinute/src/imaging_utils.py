import logging
import os
import platform
import shutil
import subprocess
import tarfile
import urllib.request

from ScannerMinute.definitions import PROJECT_ROOT_DIR

NODE_DIR = os.path.join(PROJECT_ROOT_DIR, "node_server")
NODE_VERSION = "v20.18.1"


def _get_node_binary_url():
    """Return the download URL for the Node.js binary tarball for this platform."""
    system = platform.system().lower()
    machine = platform.machine()

    if system == "darwin":
        arch = "arm64" if machine == "arm64" else "x64"
        return f"https://nodejs.org/dist/{NODE_VERSION}/node-{NODE_VERSION}-darwin-{arch}.tar.gz"
    elif system == "linux":
        arch = "arm64" if machine == "aarch64" else "x64"
        return f"https://nodejs.org/dist/{NODE_VERSION}/node-{NODE_VERSION}-linux-{arch}.tar.gz"
    else:
        raise RuntimeError(f"Unsupported platform: {system} {machine}")


def _get_node_path():
    """Return path to the local node binary, or None if not installed locally."""
    node_bin = os.path.join(NODE_DIR, "bin", "node")
    return node_bin if os.path.isfile(node_bin) else None


def _get_npm_path():
    """Return path to the local npm binary, or None if not installed locally."""
    npm_bin = os.path.join(NODE_DIR, "bin", "npm")
    return npm_bin if os.path.isfile(npm_bin) else None


def is_node_installed():
    """
    Check whether Node.js is available.
    Returns (True, path) if found locally in node_server/ or on the system PATH.
    Returns (False, None) if not found.
    """
    # Check local installation first
    local_node = _get_node_path()
    if local_node:
        logging.info(f"Node.js found locally: {local_node}")
        return True, local_node

    # Check system PATH
    system_node = shutil.which("node")
    if system_node:
        logging.info(f"Node.js found on system PATH: {system_node}")
        return True, system_node

    logging.info("Node.js not found.")
    return False, None


def install_node(ask=True):
    """
    Install Node.js locally into node_server/ under the project root.

    Parameters:
        ask: bool — if True, prompt the user for confirmation before installing.

    Returns:
        str — path to the installed node binary, or None if installation was declined.
    """
    found, path = is_node_installed()
    if found:
        logging.info(f"Node.js already installed at {path}")
        return path

    if ask:
        answer = input(f"Node.js not found. Install {NODE_VERSION} to {NODE_DIR}? [y/N] ")
        if answer.strip().lower() not in ("y", "yes"):
            logging.info("Installation declined.")
            return None

    url = _get_node_binary_url()
    tarball_path = os.path.join(PROJECT_ROOT_DIR, "node.tar.gz")

    logging.info(f"Downloading Node.js {NODE_VERSION} from {url}")
    urllib.request.urlretrieve(url, tarball_path)

    logging.info(f"Extracting to {NODE_DIR}")
    os.makedirs(NODE_DIR, exist_ok=True)
    with tarfile.open(tarball_path, "r:gz") as tar:
        tar.extractall(path=NODE_DIR)

    os.remove(tarball_path)

    # The tarball extracts into a subdirectory like node-v20.18.1-darwin-arm64/
    # Move contents up to NODE_DIR
    extracted_dirs = [
        d for d in os.listdir(NODE_DIR)
        if os.path.isdir(os.path.join(NODE_DIR, d)) and d.startswith("node-")
    ]
    if extracted_dirs:
        extracted_path = os.path.join(NODE_DIR, extracted_dirs[0])
        for item in os.listdir(extracted_path):
            shutil.move(os.path.join(extracted_path, item), os.path.join(NODE_DIR, item))
        os.rmdir(extracted_path)

    node_path = _get_node_path()
    if node_path:
        logging.info(f"Node.js {NODE_VERSION} installed successfully at {node_path}")
    else:
        logging.error("Installation failed — node binary not found after extraction.")

    return node_path


def uninstall_node():
    """Remove the local Node.js installation from node_server/."""
    if os.path.isdir(NODE_DIR):
        shutil.rmtree(NODE_DIR)
        logging.info(f"Removed Node.js installation at {NODE_DIR}")
    else:
        logging.info(f"No local Node.js installation found at {NODE_DIR}")


def run_server(script_path, args=None, cwd=None):
    """
    Run a Node.js script using the local or system Node.js.

    Parameters:
        script_path: str — path to the .js file to run
        args: list[str] — optional arguments to pass to the script
        cwd: str — optional working directory (defaults to NODE_DIR)

    Returns:
        subprocess.Popen — the running server process
    """
    found, node_path = is_node_installed()
    if not found:
        node_path = install_node(ask=True)
        if not node_path:
            raise RuntimeError("Node.js is not installed and installation was declined.")

    cmd = [node_path, script_path] + (args or [])
    work_dir = cwd or NODE_DIR

    logging.info(f"Starting server: {' '.join(cmd)}")
    process = subprocess.Popen(
        cmd,
        cwd=work_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    logging.info(f"Server started with PID {process.pid}")
    return process
