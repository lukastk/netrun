"""FastAPI backend for netrun-dashboard.

Provides a registry API for ObserveServers to register/deregister,
and serves the built SvelteKit SPA as static files.
"""

import logging
import subprocess
import shutil
import time
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

logger = logging.getLogger(__name__)

PACKAGE_DIR = Path(__file__).parent.parent
STATIC_DIR = Path(__file__).parent / "static"
BUILD_DIR = PACKAGE_DIR / "build"


def _ensure_static() -> None:
    """Build the frontend and copy to server/static/ if missing."""
    if STATIC_DIR.is_dir():
        return

    if not (PACKAGE_DIR / "package.json").is_file():
        logger.warning("No package.json found — cannot build frontend")
        return

    logger.info("Building frontend (npm run build)...")
    try:
        subprocess.run(
            ["npm", "run", "build"],
            cwd=str(PACKAGE_DIR),
            check=True,
            capture_output=True,
        )
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        logger.warning("Frontend build failed: %s", e)
        return

    if BUILD_DIR.is_dir():
        shutil.copytree(BUILD_DIR, STATIC_DIR)
        logger.info("Frontend built and copied to server/static/")
    else:
        logger.warning("Build directory not found after npm run build")

# Heartbeat thresholds (seconds)
UNHEALTHY_TIMEOUT = 30
PRUNE_TIMEOUT = 90


class RegisterRequest(BaseModel):
    name: str
    url: str


class HeartbeatRequest(BaseModel):
    url: str


class DeregisterRequest(BaseModel):
    url: str


class _NetEntry:
    __slots__ = ("name", "url", "registered_at", "last_heartbeat")

    def __init__(self, name: str, url: str):
        self.name = name
        self.url = url
        now = time.time()
        self.registered_at = now
        self.last_heartbeat = now

    def to_dict(self) -> dict:
        now = time.time()
        age = now - self.last_heartbeat
        return {
            "name": self.name,
            "url": self.url,
            "last_heartbeat": self.last_heartbeat,
            "healthy": age < UNHEALTHY_TIMEOUT,
        }


# In-memory registry keyed by URL
_registry: dict[str, _NetEntry] = {}


_ensure_static()


def create_app() -> FastAPI:
    app = FastAPI(title="netrun-dashboard", version="0.1.0")

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # --- Registry API ---

    @app.post("/api/register")
    def register(req: RegisterRequest):
        entry = _registry.get(req.url)
        if entry is not None:
            entry.name = req.name
            entry.last_heartbeat = time.time()
        else:
            _registry[req.url] = _NetEntry(req.name, req.url)
        return {"ok": True}

    @app.post("/api/heartbeat")
    def heartbeat(req: HeartbeatRequest):
        entry = _registry.get(req.url)
        if entry is not None:
            entry.last_heartbeat = time.time()
            return {"ok": True}
        return JSONResponse({"ok": False, "message": "not registered"}, status_code=404)

    @app.post("/api/deregister")
    def deregister(req: DeregisterRequest):
        removed = _registry.pop(req.url, None)
        return {"ok": removed is not None}

    @app.get("/api/nets")
    def list_nets():
        now = time.time()
        # Prune dead entries
        to_remove = [url for url, e in _registry.items() if now - e.last_heartbeat > PRUNE_TIMEOUT]
        for url in to_remove:
            del _registry[url]
        return [e.to_dict() for e in _registry.values()]

    # --- Static file serving (SPA) ---

    if STATIC_DIR.is_dir():
        @app.get("/api/{rest:path}")
        def api_fallback():
            return JSONResponse({"error": "not found"}, status_code=404)

        app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")

    return app


app = create_app()
