"""FastAPI app — same API as Go (v1 health + records), SQLite; v2 versioned records."""
from fastapi import FastAPI

from app.api.v1.router import router as v1_router
from app.api.v2.router import router as v2_router

app = FastAPI()
app.include_router(v1_router)
app.include_router(v2_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000)
