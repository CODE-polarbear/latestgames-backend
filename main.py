import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

app = FastAPI(title="LatestGames API")

# Permissive CORS for demo; tighten in prod
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from games_api import router as games_router
app.include_router(games_router)

@app.get("/health")
def health():
    return {"status":"ok"}