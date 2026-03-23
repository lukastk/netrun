"""CLI entry point for netrun-dashboard."""

import argparse


def main():
    parser = argparse.ArgumentParser(description="netrun-dashboard server")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=18400, help="Port to listen on (default: 18400)")
    args = parser.parse_args()

    import uvicorn
    from server.main import app

    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
