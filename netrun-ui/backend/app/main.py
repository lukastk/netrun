"""FastAPI application for netrun-ui backend."""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

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

# Include routers
app.include_router(files.router, prefix="/api/files", tags=["files"])
app.include_router(factories.router, prefix="/api/factories", tags=["factories"])
app.include_router(actions.router, prefix="/api/actions", tags=["actions"])


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}
