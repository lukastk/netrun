# netrun-ui

Visual editor for netrun flow configurations.

## Installation

```bash
pip install netrun-ui
```

## Usage

```bash
# Open native window (default)
netrun-ui

# Start server for browser access
netrun-ui --server

# Specify working directory
netrun-ui -C /path/to/project

# Custom port
netrun-ui --port 8080
```

### Development Mode

If you have the source code and Node.js installed:

```bash
# Development mode with hot reload
netrun-ui --dev

# Development server mode (for browser)
netrun-ui --dev --server
```

## Building from Source

1. Clone the repository
2. Install Node.js dependencies: `npm install`
3. Install the package: `pip install -e .` (this auto-builds the frontend)

## CLI Options

```
netrun-ui [OPTIONS]

Options:
  -s, --server          Run in server mode (browser) instead of native window
  -d, --dev             Development mode: use Vite dev server (requires Node.js)
  -p, --port PORT       Backend server port (default: 8000)
  --frontend-port PORT  Frontend dev server port, only used with --dev (default: 5173)
  -C, --working-dir DIR Working directory for file explorer (default: current directory)
  --width WIDTH         Window width in app mode (default: 1400)
  --height HEIGHT       Window height in app mode (default: 900)
```
