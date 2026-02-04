"""FastAPI application for netrun-ui backend."""
import os
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .routes import files, factories, actions

app = FastAPI(
    title="netrun-ui API",
    description="Backend API for the netrun visual editor",
    version="0.1.0",
)

# Configure CORS for development
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",  # Vite dev server
        "http://localhost:4173",  # Vite preview
        "http://127.0.0.1:5173",
        "http://127.0.0.1:4173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routers
app.include_router(files.router, prefix="/api/files", tags=["files"])
app.include_router(factories.router, prefix="/api/factories", tags=["factories"])
app.include_router(actions.router, prefix="/api/actions", tags=["actions"])


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


def get_static_dir() -> Path | None:
    """Get the static files directory if it exists."""
    # Check for static files in the package directory
    package_dir = Path(__file__).parent
    static_dir = package_dir / "static"

    if static_dir.exists() and (static_dir / "index.html").exists():
        return static_dir

    return None


def setup_static_files(app: FastAPI) -> bool:
    """
    Set up static file serving if built frontend exists.
    Returns True if static files are configured, False otherwise.
    """
    static_dir = get_static_dir()

    if static_dir is None:
        return False

    # Serve static assets (js, css, images, etc.)
    # Mount at /_app to match SvelteKit's default asset path
    app_assets = static_dir / "_app"
    if app_assets.exists():
        app.mount("/_app", StaticFiles(directory=str(app_assets)), name="app_assets")

    # Serve other static files (favicon, etc.)
    @app.get("/favicon.png")
    async def favicon():
        favicon_path = static_dir / "favicon.png"
        if favicon_path.exists():
            return FileResponse(favicon_path)
        return {"error": "not found"}

    # Catch-all route for SPA - must be registered last
    @app.get("/{path:path}")
    async def serve_spa(request: Request, path: str):
        # Don't intercept API routes
        if path.startswith("api/"):
            return {"error": "not found"}

        # Try to serve the exact file first
        file_path = static_dir / path
        if file_path.is_file():
            return FileResponse(file_path)

        # Fall back to index.html for SPA routing
        return FileResponse(static_dir / "index.html")

    return True


# Check if we should serve static files
_static_files_enabled = setup_static_files(app)


def has_static_files() -> bool:
    """Check if static files are being served."""
    return _static_files_enabled
