"""FastAPI app — same API as Go (v1 health + records), SQLite; v2 versioned records."""
from pathlib import Path

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from api import v1, v2

app = FastAPI()
app.include_router(v1.router)
app.include_router(v2.router)

# Optional demo UI (remove demo/ folder + these 3 lines to drop it)
_demo_dir = Path(__file__).resolve().parent / "demo"
if _demo_dir.is_dir():
    app.mount("/demo", StaticFiles(directory=str(_demo_dir), html=True), name="demo")


@app.get("/")
async def root() -> JSONResponse:
    return JSONResponse(content={"message": "Welcome to the Rainbow Time Travel API!"})


@app.exception_handler(Exception)
async def global_exception_handler(_request: Request, exc: Exception) -> JSONResponse:
    if hasattr(exc, "status_code") and hasattr(exc, "detail"):
        status_code = getattr(exc, "status_code", 500)
        detail = str(getattr(exc, "detail", "internal error"))
    else:
        status_code, detail = 500, "internal error"
    return JSONResponse(status_code=status_code, content={"error": detail})


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
