import os

from app.api.search import router as search_router
from app.api.upload import router as upload_router
from app.db.database import engine, base
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


# --------------------------- FastAPI Application Setup ---------------------------


# CORS configuration for the separate Streamlit frontend.
default_cors_origins = [
    "http://localhost:8501",
    "http://127.0.0.1:8501",
]

configured_cors_origins = os.getenv("CORS_ALLOW_ORIGINS")
allowed_cors_origins = (
    [origin.strip() for origin in configured_cors_origins.split(",") if origin.strip()]
    if configured_cors_origins
    else default_cors_origins
)


# intialize FastAPI app
app = FastAPI(
    title = "Data Processing Pipeline API",
    description = "Backend service for data ingestion and preprocessing.",
    version = "1.0.0"
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(search_router)
app.include_router(upload_router)
base.metadata.create_all(bind=engine)  # Create database tables based on models

@app.get("/")
def root(): 
    return {"message": "backend service is running", "docs": "/docs"}